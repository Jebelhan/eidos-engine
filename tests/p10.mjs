/**
 * P10 协议级测试：关系证据接入（真实引擎 + mock 模型）。
 *   1. verified 经历 → 关系队列 pending_eval
 *   2. 模型评估 → 提交 TrustEvidence → 队列 evaluated
 *   3. Trust 计算（R2 公式）
 *   4. 幂等：重复评估不重复
 *   5. uncertain 只入队等待
 *   6. 无对象候选 → 跳过（不建证据）
 */

import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { apply } from '../lib/index.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const dataDir = mkdtempSync(join(tmpdir(), 'dsh-p10-'));

let modelText = JSON.stringify({
  counterpart: { identity_type: 'counterpart', identity_key: 'test:p10-counterpart', display_name: '测试对象' },
  trust_signal: 1.0,
  evidence_strength: 'medium',
  reason: 'mock',
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
    workspace_id: 'ws-p10',
    session_id: 's-p10',
    event_ids: [`p10-ev-${i}`],
    experience_type: 'conversation_turn',
    occurred_at: Date.now(),
    description: `p10 event ${i}`,
  });
}

async function main() {
  console.log(`P10 node test (dataDir=${dataDir})\n`);
  const ctx = makeFakeCtx();
  const handle = apply(ctx, {
    dataDir,
    relationshipEvalIntervalSeconds: 9999,
    emotionEvalIntervalSeconds: 9999,
  });
  const dispose = ctx.effects[0]();

  try {
    for (let i = 0; i < 100 && handle.engine.state !== 'ready'; i += 1) await sleep(50);
    assert.equal(handle.engine.state, 'ready');
    const engine = handle.engine;

    // 1. verified → 关系队列
    const exp1 = await createExp(engine, 1);
    await engine.request('memory_verify_experience', {
      experience_id: exp1.experience_id,
      target_status: 'verified',
      reason: 'p10 test',
    });
    await engine.request('outbox_dispatch', { limit: 20 }, { timeoutMs: 15000 });
    const q1 = await engine.request('relationship_queue_pending', {});
    check('verified 经历入队 pending_eval', () => {
      assert.equal(q1.evidences.length, 1);
      assert.equal(q1.evidences[0].queue_status, 'pending_eval');
    });

    // 2. 模型评估 → 提交证据
    const e1 = await handle.runRelationshipEval();
    check('模型评估并提交证据', () => {
      assert.equal(e1.scanned, 1);
      assert.equal(e1.submitted, 1);
    });
    const q2 = await engine.request('relationship_queue_pending', {});
    check('队列标记 evaluated', () => assert.equal(q2.evidences.length, 0));

    // 3. Trust 计算：prior=2, W_e=min(0.5,1)=0.5, S=1 → 60
    const ws = await engine.request('relationship_queue_workspace', { workspace_id: 'ws-p10' });
    const evaluated = ws.evidences.find((e) => e.canonical_experience_id === exp1.experience_id);
    check('队列记录 evidence_ref', () => assert.ok(evaluated.evidence_ref));
    const evidence = await engine.request('relationship_evidence_get', { evidence_id: evaluated.evidence_ref });
    const trust = await engine.request('relationship_get_trust', { relationship_id: evidence.relationship_id });
    check('Trust 按 R2 公式计算', () => {
      assert.ok(Math.abs(trust.trust_value - 60) < 0.01, `trust=${trust.trust_value}`);
      assert.ok(trust.confidence > 0);
    });
    check('证据来源事件已记录', () => assert.deepEqual(evidence.source_event_ids, ['p10-ev-1']));

    // 4. 重复提交幂等
    const detail = await engine.request('relationship_submit_from_eval', {
      evidence_id: evaluated.evidence_id,
      counterpart: { identity_type: 'counterpart', identity_key: 'test:p10-counterpart' },
      trust_signal: 1.0,
      evidence_strength: 'medium',
    });
    check('重复提交幂等', () => assert.equal(detail.deduplicated, true));

    // 5. 重复评估不重复
    const e2 = await handle.runRelationshipEval();
    check('重复评估不重复提交', () => assert.equal(e2.scanned, 0));

    // 5. uncertain 只入队
    const exp2 = await createExp(engine, 2);
    await engine.request('memory_verify_experience', {
      experience_id: exp2.experience_id,
      target_status: 'uncertain',
      reason: 'p10 uncertain',
    });
    await engine.request('outbox_dispatch', { limit: 20 }, { timeoutMs: 15000 });
    const q3 = await engine.request('relationship_queue_pending', {});
    check('uncertain 不进入评估队列', () => assert.equal(q3.evidences.length, 0));

    // 6. 无对象候选 → 跳过
    const exp3 = await createExp(engine, 3);
    await engine.request('memory_verify_experience', {
      experience_id: exp3.experience_id,
      target_status: 'verified',
      reason: 'p10 no counterpart',
    });
    await engine.request('outbox_dispatch', { limit: 20 }, { timeoutMs: 15000 });
    modelText = JSON.stringify({ counterpart: null, trust_signal: 0, evidence_strength: 'weak', reason: 'no counterpart' });
    const e3 = await handle.runRelationshipEval();
    check('无对象候选时跳过', () => {
      assert.equal(e3.scanned, 1);
      assert.equal(e3.submitted, 0);
    });

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
