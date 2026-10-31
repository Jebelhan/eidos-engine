/**
 * P14-D1 协议级测试：学习建议确认自动路由。
 *   1. 建议 → 人格领取 → request pending
 *   2. 新经历经学习评估 → 证据写入 → 自动 confirm（人格独立核验）
 *   3. 2 条独立经历 → active + 实例
 */

import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { apply } from '../lib/index.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const dataDir = mkdtempSync(join(tmpdir(), 'dsh-p14d1-'));
const WS = 'ws-p14d1';

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

async function createVerified(engine, i) {
  const created = await engine.request('memory_create_experience', {
    workspace_id: WS,
    session_id: 's-p14d1',
    event_ids: [`p14d1-ev-${i}`],
    experience_type: 'conversation_turn',
    occurred_at: Date.now(),
    description: `p14d1 event ${i}`,
  });
  await engine.request('memory_verify_experience', {
    experience_id: created.experience_id,
    target_status: 'verified',
    reason: 'p14d1 test',
  });
  return created;
}

async function dispatch(engine) {
  await engine.request('outbox_dispatch', { limit: 50 }, { timeoutMs: 20000 });
}

async function main() {
  console.log(`P14-D1 node test (dataDir=${dataDir})\n`);
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

    // 1. 结论 + 两条来源证据 → 建议 → 人格领取
    const conclusion = await engine.request('learning_conclusion_create', { workspace_id: WS, statement: 'p14d1 结论' });
    const cid = conclusion.conclusion_id;
    const origin = [await createVerified(engine, 1), await createVerified(engine, 2)];
    await dispatch(engine);
    for (const exp of origin) {
      await engine.request('learning_submit_evidence', {
        evidence_id: `learn:${WS}:${exp.experience_id}`,
        conclusion_id: cid,
        polarity: 'support',
        rationale: 'origin',
      });
    }
    const pending = await engine.request('learning_suggestion_pending', {});
    check('建议生成', () => assert.equal(pending.suggestions.length, 1));
    await dispatch(engine);
    const consume = await engine.request('personality_request_create', {
      suggestion_id: pending.suggestions[0].suggestion_id,
      conclusion_id: cid,
    });
    const rid = consume.influence_request_id;
    check('request pending', async () => {});
    const req0 = await engine.request('personality_request_get', { influence_request_id: rid });
    check('request pending', () => assert.equal(req0.status, 'pending'));

    // 2. 新经历 → 学习评估 → 证据 → 自动 confirm
    modelText = JSON.stringify({ conclusion_id: cid, statement: '', polarity: 'support', rationale: 'confirm' });
    const e3 = await createVerified(engine, 3);
    await dispatch(engine);
    const eval1 = await handle.runLearningEval();
    console.log("  · eval1:", JSON.stringify(eval1));
    check('学习评估提交证据', () => assert.ok(eval1.submitted >= 1));

    const req1 = await engine.request('personality_request_get', { influence_request_id: rid });
    check('自动确认 1/2', () => {
      assert.equal(req1.status, 'confirming');
    });

    // 3. 第二条独立经历 → active
    const e4 = await createVerified(engine, 4);
    await dispatch(engine);
    await handle.runLearningEval();
    const req2 = await engine.request('personality_request_get', { influence_request_id: rid });
    check('自动确认 2/2 → active', () => assert.equal(req2.status, 'active'));
    const summary = await engine.request('personality_summary', {});
    check('实例进池', () => assert.equal(summary.instances.length, 1));

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
