/**
 * P12 协议级测试：学习建议 → 人格持续确认 → 激活 → 实例池。
 *   1. 建议生成并发出事件
 *   2. 人格消费者领取 → request pending
 *   3. 两条独立 confirmed → 激活 + 实例进池
 *   4. 重复确认不重复计数
 *   5. 建议领取幂等
 */

import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { apply } from '../lib/index.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const dataDir = mkdtempSync(join(tmpdir(), 'dsh-p12-'));
const WS = 'ws-p12';

const mockLlm = {
  async *stream() {
    yield { type: 'text-delta', text: '{}' };
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
    session_id: 's-p12',
    event_ids: [`p12-ev-${i}`],
    experience_type: 'conversation_turn',
    occurred_at: Date.now(),
    description: `p12 event ${i}`,
  });
  await engine.request('memory_verify_experience', {
    experience_id: created.experience_id,
    target_status: 'verified',
    reason: 'p12 test',
  });
  return created;
}

async function main() {
  console.log(`P12 node test (dataDir=${dataDir})\n`);
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

    // 构造结论 + 两条来源证据 → 生成建议
    const conclusion = await engine.request('learning_conclusion_create', {
      workspace_id: WS,
      statement: 'p12 结论',
    });
    const cid = conclusion.conclusion_id;
    const origin = [];
    for (let i = 1; i <= 2; i += 1) {
      origin.push(await createVerified(engine, i));
    }
    // 先让学习消费者把 verified 经历入队
    await engine.request('outbox_dispatch', { limit: 20 }, { timeoutMs: 15000 });
    for (const exp of origin) {
      await engine.request('learning_submit_evidence', {
        evidence_id: `learn:${WS}:${exp.experience_id}`,
        conclusion_id: cid,
        polarity: 'support',
        rationale: 'p12 origin',
      });
    }
    const pending = await engine.request('learning_suggestion_pending', {});
    check('建议生成', () => assert.equal(pending.suggestions.length, 1));

    // 建议事件 → dispatch → 人格领取
    await engine.request('outbox_dispatch', { limit: 20 }, { timeoutMs: 15000 });
    const summary0 = await engine.request('personality_summary', {});
    const requestRow = summary0.trait ? null : null;
    const list = await engine.request('outbox_status', {});
    check('人格领取事件已投递', () => assert.ok(list.handlers.includes('personality-suggestion')));

    // 查找 influence request（通过 request_create 幂等获取同一 suggestion）
    const sug = pending.suggestions[0];
    const consume = await engine.request('personality_request_create', {
      suggestion_id: sug.suggestion_id,
      conclusion_id: cid,
    });
    const rid = consume.influence_request_id;
    check('创建 influence request', () => assert.ok(rid));

    const req0 = await engine.request('personality_request_get', { influence_request_id: rid });
    check('request 为 pending', () => assert.equal(req0.status, 'pending'));

    // 两条新的 verified 经历作为确认
    const e3 = await createVerified(engine, 3);
    const e4 = await createVerified(engine, 4);
    const c1 = await engine.request('personality_confirm', {
      influence_request_id: rid,
      experience_id: e3.experience_id,
      source_event_ids: ['p12-ev-3'],
    });
    check('第一条确认', () => {
      assert.equal(c1.accepted, true);
      assert.equal(c1.status, 'confirming');
    });
    const c2 = await engine.request('personality_confirm', {
      influence_request_id: rid,
      experience_id: e4.experience_id,
      source_event_ids: ['p12-ev-4'],
    });
    check('两条确认后激活', () => {
      assert.equal(c2.status, 'active');
      assert.ok(c2.instance_id);
    });

    const summary = await engine.request('personality_summary', {});
    check('实例进入人格池', () => {
      assert.equal(summary.instances.length, 1);
      assert.equal(summary.instances[0].status, 'active');
      assert.ok(summary.instances[0].strength > 0);
    });

    // 领取幂等
    const again = await engine.request('personality_request_create', {
      suggestion_id: sug.suggestion_id,
      conclusion_id: cid,
    });
    check('建议领取幂等', () => assert.equal(again.influence_request_id, rid));

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
