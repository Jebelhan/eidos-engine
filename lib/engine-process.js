/**
 * Python 状态引擎进程管理。
 *
 * 职责：探测运行时 → spawn → ready 握手 → NDJSON 请求/响应配对 → 超时与退出处理。
 * 不实现业务逻辑，只做通信与生命周期。
 */

import { spawn } from 'node:child_process';
import { createRequire } from 'node:module';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { dirname } from 'node:path';
import { LineDecoder, encodeMessage, nextRequestId } from './protocol.js';
import { resolvePython } from './runtime.js';

const __dirname = dirname(fileURLToPath(import.meta.url));
const require = createRequire(import.meta.url);

export class EngineProcess {
  /**
   * @param {object} options
   * @param {string} [options.pythonPath] 显式 Python 路径
   * @param {string} [options.mainPath] Python 入口，默认 ../engine/python/main.py
   * @param {string} [options.cwd]
   * @param {Record<string,string>} [options.env]
   * @param {number} [options.readyTimeoutMs]
   * @param {(line:string)=>void} [options.log] stderr 行回调
   */
  constructor(options = {}) {
    this.options = options;
    this.mainPath = options.mainPath ?? join(__dirname, '..', 'engine', 'python', 'main.py');
    this.readyTimeoutMs = options.readyTimeoutMs ?? 15000;
    this.log = options.log ?? (() => {});

    this.child = null;
    this.state = 'idle'; // idle | starting | ready | stopping | stopped | failed
    this.pythonInfo = null;
    this.pending = new Map();
    this.stderrTail = [];
    this.exitInfo = null;
    this.readyWaiters = [];
    this.decoder = new LineDecoder();
    this.exitHooks = new Set();
  }

  /** 启动并等待 ready。可重复调用，已是 ready 时直接返回。 */
  async start() {
    if (this.state === 'ready') return this.pythonInfo;
    if (this.state === 'starting') {
      return new Promise((resolve, reject) => this.readyWaiters.push({ resolve, reject }));
    }
    this.state = 'starting';

    const python = await resolvePython({ explicitPath: this.options.pythonPath });
    this.pythonInfo = python;

    const child = spawn(python.path, [this.mainPath], {
      cwd: this.options.cwd ?? dirname(this.mainPath),
      env: {
        ...process.env,
        ...(this.options.env ?? {}),
        PYTHONUNBUFFERED: '1',
        PYTHONIOENCODING: 'utf-8',
      },
      stdio: ['pipe', 'pipe', 'pipe'],
      windowsHide: true,
    });
    this.child = child;

    child.stdout.setEncoding('utf8');
    child.stderr.setEncoding('utf8');

    child.stdout.on('data', (chunk) => {
      for (const item of this.decoder.push(chunk)) {
        if (item.ok) this.handleMessage(item.message);
        else this.log(`[bridge] invalid JSON from python: ${item.error}`);
      }
    });

    child.stderr.on('data', (chunk) => {
      for (const line of String(chunk).split(/\r?\n/)) {
        if (line.trim() === '') continue;
        this.stderrTail.push(line);
        if (this.stderrTail.length > 200) this.stderrTail.shift();
        this.log(line);
      }
    });

    child.on('error', (error) => this.fail(error));

    child.on('exit', (code, signal) => {
      this.exitInfo = { code, signal };
      this.state = 'stopped';
      for (const [, waiter] of this.pending) {
        clearTimeout(waiter.timer);
        waiter.reject(new Error(`engine exited (code=${code}, signal=${signal ?? 'none'})`));
      }
      this.pending.clear();
      const error = new Error(`engine exited (code=${code}, signal=${signal ?? 'none'})`);
      for (const waiter of this.readyWaiters.splice(0)) waiter.reject(error);
      for (const hook of this.exitHooks) hook(this.exitInfo);
    });

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.fail(new Error(`engine ready timeout after ${this.readyTimeoutMs}ms`));
        reject(new Error(`engine ready timeout after ${this.readyTimeoutMs}ms`));
      }, this.readyTimeoutMs);
      this.readyWaiters.push({
        resolve: (info) => {
          clearTimeout(timer);
          resolve(info);
        },
        reject: (error) => {
          clearTimeout(timer);
          reject(error);
        },
      });
    });
  }

  handleMessage(message) {
    if (message && message.type === 'ready' && this.state === 'starting') {
      this.state = 'ready';
      for (const waiter of this.readyWaiters.splice(0)) waiter.resolve(this.pythonInfo);
      return;
    }
    if (message && typeof message.request_id === 'string') {
      const waiter = this.pending.get(message.request_id);
      if (!waiter) {
        this.log(`[bridge] response for unknown/expired request ${message.request_id}`);
        return;
      }
      this.pending.delete(message.request_id);
      clearTimeout(waiter.timer);
      if (message.status === 'success') waiter.resolve(message.data ?? null);
      else waiter.reject(Object.assign(new Error(message.error?.message ?? 'engine error'), { code: message.error?.code ?? 'ENGINE_ERROR', data: message.error?.data }));
      return;
    }
    this.log(`[bridge] ignored message: ${JSON.stringify(message).slice(0, 200)}`);
  }

  fail(error) {
    if (this.state === 'ready' || this.state === 'starting') this.state = 'failed';
    for (const waiter of this.readyWaiters.splice(0)) waiter.reject(error);
    for (const [, waiter] of this.pending) {
      clearTimeout(waiter.timer);
      waiter.reject(error);
    }
    this.pending.clear();
    try {
      this.child?.kill();
    } catch {
      /* ignore */
    }
  }

  /**
   * 发送请求，按 request_id 等待响应。
   * 超时后请求从挂起表移除，迟到的响应会被忽略并记录。
   */
  request(operation, payload = {}, options = {}) {
    if (this.state !== 'ready') return Promise.reject(new Error(`engine not ready (state=${this.state})`));
    const requestId = options.requestId ?? nextRequestId(operation);
    const timeoutMs = options.timeoutMs ?? 5000;
    const message = { request_id: requestId, type: operation, payload, protocol_version: '1.0' };

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(requestId);
        reject(Object.assign(new Error(`engine request timeout: ${operation} (${timeoutMs}ms)`), { code: 'ENGINE_TIMEOUT', requestId }));
      }, timeoutMs);
      this.pending.set(requestId, { resolve, reject, timer, operation });
      try {
        this.child.stdin.write(encodeMessage(message));
      } catch (error) {
        clearTimeout(timer);
        this.pending.delete(requestId);
        reject(error);
      }
    });
  }

  /** 正常关闭：先请求 shutdown，再等退出，超时强杀。 */
  async stop(graceMs = 5000) {
    if (this.state === 'ready') {
      this.state = 'stopping';
      try {
        await this.request('shutdown', {}, { timeoutMs: Math.min(graceMs, 3000) });
      } catch {
        /* 进程可能已经退出 */
      }
    }
    const child = this.child;
    if (!child || child.exitCode !== null) {
      this.state = 'stopped';
      return;
    }
    await new Promise((resolve) => {
      const timer = setTimeout(() => {
        try {
          child.kill();
        } catch {
          /* ignore */
        }
        resolve();
      }, graceMs);
      child.once('exit', () => {
        clearTimeout(timer);
        resolve();
      });
    });
    this.state = 'stopped';
  }

  onExit(hook) {
    this.exitHooks.add(hook);
    return () => this.exitHooks.delete(hook);
  }
}
