/**
 * P9 协议级测试：情绪证据接入（真实引擎 + mock 模型）。
 *   1. verified 经历 → 队列 pending_eval
 *   2. 模型评估 → 提交情绪事件 → evaluated
 *   3. 重复评估不重复提交
 *   4. uncertain 经历 → awaiting_verification，不评估
 *   5. uncertain → verified 升级后进入评估
 *   6. 模型输出非法 → retry_wait
 */

import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { apply } from '../lib/index.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const dataDir = mkdtempSync(join(tmpdir(), 'dsh-p9-'));

let modelText = JSON.stringify({
  event_type: 'positive',
  confidence: 0.7,
  emotion_candidates: { p: 0.4, a: 0.2, d: 0.1 },
  reason: 'mock positive',
});

const mockLlm = {
  async *stream() {
    yield { type: 'text-delta', text: modelText };
  },
};

function makeFakeCtx() {
  const handlers = {};
  const effects = [];
  return {
    logger: { info: () => {} },
    get(name) {
      return name === 'llm' ? mockLlm : undefined;
    },
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

async function createExp(engine, i) {
  return engine.request('memory_create_experience', {
    workspace_id: 'ws-p9',
    session_id: 's-p9',
    event_ids: [`p9-ev-${i}`],
    experience_type: 'conversation_turn',
    occurred_at: Date.now(),
    description: `p9 event ${i}`,
  });
}

async function main() {
  console.log(`P9 node test (dataDir=${dataDir})\n`);
  const ctx = makeFakeCtx();
  const handle = apply(ctx, { dataDir, emotionEvalIntervalSeconds: 9999 });
  const dispose = ctx.effects[0]();

  try {
    for (let i = 0; i < 100 && handle.engine.state !== 'ready'; i += 1) await sleep(50);
    assert.equal(handle.engine.state, 'ready');
    const engine = handle.engine;

    // 1. verified → pending_eval
    const exp1 = await createExp(engine, 1);
    await engine.request('memory_verify_experience', {
      experience_id: exp1.experience_id,
      target_status: 'verified',
      reason: 'p9 test',
    });
    await engine.request('outbox_dispatch', { limit: 20 }, { timeoutMs: 15000 });
    const q1 = await engine.request('emotion_queue_pending', {});
    check('verified 经历入队 pending_eval', () => {
      assert.equal(q1.evidences.length, 1);
      assert.equal(q1.evidences[0].queue_status, 'pending_eval');
      assert.equal(q1.evidences[0].canonical_experience_id, exp1.experience_id);
    });

    // 2. 模型评估 → 提交
    const eval1 = await handle.runEmotionEval();
    check('模型评估并提交情绪事件', () => {
      assert.equal(eval1.scanned, 1);
      assert.equal(eval1.submitted, 1);
    });
    const q2 = await engine.request('emotion_queue_pending', {});
    check('队列标记 evaluated', () => assert.equal(q2.evidences.length, 0));

    const detail = await engine.request('emotion_get', { experience_id: exp1.experience_id, workspace_id: 'ws-p9' });
    check('情绪事件已写入', () => {
      assert.equal(detail.events.length, 1);
      assert.equal(detail.events[0].event_type, 'positive');
      assert.equal(detail.events[0].status, 'processed');
    });

    // 3. 重复评估不重复
    const eval2 = await handle.runEmotionEval();
    check('重复评估不重复提交', () => assert.equal(eval2.scanned, 0));

    // 4. uncertain → awaiting
    const exp2 = await createExp(engine, 2);
    await engine.request('memory_verify_experience', {
      experience_id: exp2.experience_id,
      target_status: 'uncertain',
      reason: 'p9 uncertain',
    });
    await engine.request('outbox_dispatch', { limit: 20 }, { timeoutMs: 15000 });
    const q3 = await engine.request('emotion_queue_pending', {});
    const ws3 = await engine.request('emotion_queue_workspace', { workspace_id: 'ws-p9' });
    check('uncertain 经历只入队不评估', () => {
      assert.equal(q3.evidences.length, 0);
      const row = ws3.evidences.find((e) => e.canonical_experience_id === exp2.experience_id);
      assert.ok(row, 'evidence exists');
      assert.equal(row.queue_status, 'awaiting_verification');
    });

    // 5. uncertain → verified 升级
    await engine.request('memory_verify_experience', {
      experience_id: exp2.experience_id,
      target_status: 'verified',
      reason: 'p9 upgrade',
    });
    await engine.request('outbox_dispatch', { limit: 20 }, { timeoutMs: 15000 });
    const q4 = await engine.request('emotion_queue_pending', {});
    check('uncertain→verified 后进入评估队列', () => assert.equal(q4.evidences.length, 1));

    // 6. 模型输出非法 → retry_wait
    modelText = 'not json';
    const eval3 = await handle.runEmotionEval();
    check('模型输出非法进入重试', () => assert.equal(eval3.failed, 1));
    const ws2 = await engine.request('emotion_queue_workspace', { workspace_id: 'ws-p9' });
    const row2 = ws2.evidences.find((e) => e.canonical_experience_id === exp2.experience_id);
    check('队列为 retry_wait', () => assert.equal(row2.queue_status, 'retry_wait'));

    // 恢复模型输出
    modelText = JSON.stringify({ event_type: 'neutral', confidence: 0.4, reason: 'ok' });

    console.log(`\nPASS: ${passed} checks`);
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

let passed = 0;
const check = (name, fn) => {
  fn();
  passed += 1;
  console.log(`  ✓ ${name}`);
};

main().catch((error) => {
  console.error('\nFAIL:', error);
  process.exitCode = 1;
});
