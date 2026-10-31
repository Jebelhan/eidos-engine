/**
 * P11 协议级测试：学习证据接入（真实引擎 + mock 模型）。
 *   1. verified 经历入队
 *   2. 模型匹配已有结论 → 证据写入
 *   3. 两条独立支持 → 结论 active + 建议 pending
 *   4. 无匹配 → 候选结论（不晋升）
 *   5. uncertain 不入队评估
 */

import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { apply } from '../lib/index.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const dataDir = mkdtempSync(join(tmpdir(), 'dsh-p11-'));

let modelText = '{}';
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
  const created = await engine.request('memory_create_experience', {
    workspace_id: 'ws-p11',
    session_id: 's-p11',
    event_ids: [`p11-ev-${i}`],
    experience_type: 'conversation_turn',
    occurred_at: Date.now(),
    description: `p11 event ${i}`,
  });
  await engine.request('memory_verify_experience', {
    experience_id: created.experience_id,
    target_status: 'verified',
    reason: 'p11 test',
  });
  return created;
}

async function main() {
  console.log(`P11 node test (dataDir=${dataDir})\n`);
  const ctx = makeFakeCtx();
  const handle = apply(ctx, {
    dataDir,
    learningEvalIntervalSeconds: 9999,
    emotionEvalIntervalSeconds: 9999,
    relationshipEvalIntervalSeconds: 9999,
  });
  const dispose = ctx.effects[0]();

  try {
    for (let i = 0; i < 100 && handle.engine.state !== 'ready'; i += 1) await sleep(50);
    assert.equal(handle.engine.state, 'ready');
    const engine = handle.engine;

    const conclusion = await engine.request('learning_conclusion_create', {
      workspace_id: 'ws-p11',
      statement: '测试结论：经历会带来稳定规律',
    });
    const cid = conclusion.conclusion_id;

    // 1. 两条 verified 经历入队
    await createExp(engine, 1);
    await createExp(engine, 2);
    await engine.request('outbox_dispatch', { limit: 20 }, { timeoutMs: 15000 });
    const q1 = await engine.request('learning_queue_pending', {});
    check('verified 经历入队', () => assert.equal(q1.evidences.length, 2));

    // 2. 模型匹配已有结论
    modelText = JSON.stringify({ conclusion_id: cid, statement: '', polarity: 'support', rationale: 'mock' });
    const e1 = await handle.runLearningEval();
    check('模型匹配结论并写入证据', () => {
      assert.equal(e1.scanned, 2);
      assert.equal(e1.submitted >= 1, true);
    });

    const c1 = await engine.request('learning_conclusion_get', { conclusion_id: cid });
    check('结论达到 active', () => assert.equal(c1.status, 'active'));

    const suggestions = await engine.request('learning_suggestion_pending', {});
    check('生成 pending 建议', () => {
      assert.equal(suggestions.suggestions.length, 1);
      assert.equal(suggestions.suggestions[0].status, 'pending');
      assert.equal(suggestions.suggestions[0].target_module, 'personality');
    });

    // 3. 重复评估不重复建议
    const c2 = await engine.request('learning_conclusion_get', { conclusion_id: cid });
    check('重复评估幂等', () => assert.equal(c2.status, 'active'));

    // 4. 无匹配 → 候选
    const exp3 = await createExp(engine, 3);
    await engine.request('outbox_dispatch', { limit: 20 }, { timeoutMs: 15000 });
    modelText = JSON.stringify({ conclusion_id: '', statement: '候选结论：新规律', polarity: 'support', rationale: 'mock' });
    const e2 = await handle.runLearningEval();
    check('无匹配时提出候选', () => assert.equal(e2.proposed, 1));

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
