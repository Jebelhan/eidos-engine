/**
 * Python 运行时探测。
 *
 * 顺序：显式配置 → DSH 自带运行时（workspace dependencies）→ 系统 Python。
 * 每一级都做实际版本与依赖验证，不只看文件是否存在。
 */

import { execFile } from 'node:child_process';
import { promises as fs } from 'node:fs';
import { homedir } from 'node:os';
import { join } from 'node:path';
import { promisify } from 'node:util';

const run = promisify(execFile);
const MIN_MINOR = 10;

function dshHome() {
  return process.env.DSH_HOME && process.env.DSH_HOME.trim() !== ''
    ? process.env.DSH_HOME
    : join(homedir(), '.dsh');
}

/** 列出 DSH 自带运行时里的 Python 可执行文件。 */
async function dshRuntimeCandidates() {
  const root = join(dshHome(), 'dsh-runtimes');
  const out = [];
  let entries = [];
  try {
    entries = await fs.readdir(root, { withFileTypes: true });
  } catch {
    return out;
  }
  for (const entry of entries) {
    if (!entry.isDirectory()) continue;
    const base = join(root, entry.name, 'dependencies', 'python');
    for (const name of process.platform === 'win32' ? ['python.exe'] : ['python3', 'python']) {
      out.push(join(base, name));
    }
  }
  return out;
}

/** 验证一个候选：能跑、版本达标、标准库可用。 */
async function verify(candidate) {
  try {
    const { stdout } = await run(
      candidate,
      ['-c', 'import sys,json,sqlite3;print("%d.%d" % sys.version_info[:2])'],
      { timeout: 8000, windowsHide: true },
    );
    const [major, minor] = stdout.trim().split('.').map(Number);
    if (!(major === 3 && minor >= MIN_MINOR)) {
      return { ok: false, reason: `版本不满足 >=3.${MIN_MINOR}（实际 ${stdout.trim()}）` };
    }
    return { ok: true, version: stdout.trim() };
  } catch (error) {
    return { ok: false, reason: error instanceof Error ? error.message : String(error) };
  }
}

/**
 * 探测可用 Python。
 * @param {{ explicitPath?: string }} [options]
 * @returns {Promise<{ path: string, version: string, source: string, attempts: Array<{path:string,reason?:string}> }>}
 */
export async function resolvePython(options = {}) {
  const attempts = [];
  const candidates = [];

  if (options.explicitPath) candidates.push({ path: options.explicitPath, source: 'config' });
  if (process.env.DSH_PYTHON) candidates.push({ path: process.env.DSH_PYTHON, source: 'env' });
  for (const p of await dshRuntimeCandidates()) candidates.push({ path: p, source: 'dsh-runtime' });
  for (const p of process.platform === 'win32' ? ['py', 'python', 'python3'] : ['python3', 'python']) {
    candidates.push({ path: p, source: 'system' });
  }

  for (const candidate of candidates) {
    const result = await verify(candidate.path);
    if (result.ok) {
      return { path: candidate.path, version: result.version, source: candidate.source, attempts };
    }
    attempts.push({ path: candidate.path, source: candidate.source, reason: result.reason });
  }

  const detail = attempts.map((a) => `${a.path} (${a.source}): ${a.reason}`).join('; ');
  throw new Error(`未找到可用 Python（需要 3.${MIN_MINOR}+）。尝试过：${detail || '无候选'}`);
}
