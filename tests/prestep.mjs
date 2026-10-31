/**
 * P4 pre-step 集成测试：真实记忆链路。
 *   1. 每轮把用户消息作为事件创建候选 Experience；
 *   2. 注入的是真实数据库统计，不是模拟计数；
 *   3. 相同消息重复不重复计数；
 *   4. 多会话隔离；
 *   5. reject 不注入；引擎不可用回退原决策。
 */

import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { apply } from '../lib/index.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const dataDir = mkdtempSync(join(tmpdir(), 'dsh-p4-pre-'));

function makeFakeCtx() {
  const handlers = {};
  const effects = [];
  return {
    logger: { info: () => {} },
    effect(fn) {
      effects.push(fn);
      return () => {};
    },
    on(name, fn) {
      handlers[name] = fn;
      return () => delete handlers[name];
    },
    get handlers() {
      return handlers;
    },
    get effects() {
      return effects;
    },
  };
}

const userMessage = (id, text) => ({
  id,
  role: 'user',
  content: [{ type: 'text', text }],
  source: { kind: 'user' },
});

const turnPayload = (sessionId, cwd) => ({
  agent: { session: { id: sessionId, header: { cwd } } },
  messages: [],
  turn: 1,
  step: 1,
  signal: new AbortController().signal,
});

const enterWith = (messages) => async () => ({ kind: 'enter', messages });
const enter = enterWith([]);

async function main() {
  console.log('P4 pre-step test\n');
  const ctx = makeFakeCtx();
  const handle = apply(ctx, { dataDir });
  const dispose = ctx.effects[0]();

  try {
    for (let i = 0; i < 100 && handle.engine.state !== 'ready'; i += 1) await sleep(50);
    assert.equal(handle.engine.state, 'ready', 'engine ready');
    const preStep = ctx.handlers['agent/pre-step'];
    assert.equal(typeof preStep, 'function', 'pre-step registered');
    console.log('  ✓ pre-step 已注册，引擎就绪');

    const p1 = await preStep(turnPayload('session-a', 'C:/ws/a'), enterWith([userMessage('msg-1', '你好')]));
    assert.equal(p1.messages.length, 3, 'original user message + memory summary + behavior guidance');
    assert.equal(p1.messages[0].id, 'msg-1', 'original message preserved first');
    assert.equal(p1.messages[1].role, 'user', 'injected summary is a user message');
    const t1 = p1.messages[1].content[0].text;
    assert.ok(t1.includes('记忆：本会话经历 1'), `first: ${t1}`);
    assert.ok(t1.includes('待核验 1'), `first: ${t1}`);
    assert.ok(p1.messages[2].content[0].text.includes('DSH行为指导'), 'behavior guidance injected');
    console.log(`  ✓ 第 1 轮注入真实计数: ${t1}`);

    const p2 = await preStep(turnPayload('session-a', 'C:/ws/a'), enterWith([userMessage('msg-2', '再来一句')]));
    const t2 = p2.messages[1].content[0].text;
    assert.ok(t2.includes('记忆：本会话经历 2'), `second: ${t2}`);
    console.log(`  ✓ 第 2 轮计数增长: ${t2}`);

    const p3 = await preStep(turnPayload('session-a', 'C:/ws/a'), enterWith([userMessage('msg-2', '再来一句')]));
    const t3 = p3.messages[1].content[0].text;
    assert.ok(t3.includes('记忆：本会话经历 2'), `dup: ${t3}`);
    console.log(`  ✓ 相同消息去重，计数不重复: ${t3}`);

    const p4 = await preStep(turnPayload('session-b', 'C:/ws/b'), enterWith([userMessage('msg-3', '另一个会话')]));
    const t4 = p4.messages[1].content[0].text;
    assert.ok(t4.includes('记忆：本会话经历 1'), `session-b: ${t4}`);
    console.log(`  ✓ 多会话隔离: ${t4}`);

    const rejected = await preStep(turnPayload('session-a', 'C:/ws/a'), async () => ({ kind: 'reject' }));
    assert.equal(rejected.kind, 'reject');
    console.log('  ✓ reject 决策保持原样');

    await handle.engine.stop();
    const down = await preStep(turnPayload('session-a', 'C:/ws/a'), enter);
    assert.equal(down.kind, 'enter');
    assert.equal(down.messages.length, 0, 'no injection when engine down');
    console.log('  ✓ 引擎不可用时回退原决策');

    console.log('\nPASS: P4 pre-step');
  } finally {
    await dispose().catch(() => {});
    for (let i = 0; i < 5; i += 1) {
      try {
        rmSync(dataDir, { recursive: true, force: true });
        break;
      } catch {
        await sleep(120);
      }
    }
  }
}

main().catch((error) => {
  console.error('\nFAIL:', error);
  process.exitCode = 1;
});
