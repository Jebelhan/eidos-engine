/**
 * P13 协议级测试：行为指导接口 + pre-step 注入。
 *   1. submit observation（current/situational）
 *   2. get behavior guidance（Trait 映射 + 情境 + 降级）
 *   3. pre-step 注入行为指导，原消息保留
 *   4. 行为异常时回退原 decision
 */

import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { apply } from '../lib/index.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const dataDir = mkdtempSync(join(tmpdir(), 'dsh-p13-'));
const WS = 'C:/p13-ws';

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

const userMessage = (id, text) => ({
  id,
  role: 'user',
  content: [{ type: 'text', text }],
  source: { kind: 'user' },
});

const turnPayload = () => ({
  agent: { session: { id: 'session-p13', header: { cwd: WS } } },
  messages: [],
  turn: 1,
  step: 1,
  signal: new AbortController().signal,
});

const enterWith = (messages) => async () => ({ kind: 'enter', messages });

async function main() {
  console.log(`P13 node test (dataDir=${dataDir})\n`);
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

    // 1. 写观察（current + situational）
    const obs1 = await engine.request('behavior_submit', {
      session_id: 'session-p13',
      workspace_id: WS,
      observation_type: 'interaction_observation',
      target_layer: 'current',
      observation_payload: {
        preferred: [{ guidance_id: 'state:current', guidance_type: 'interaction_style', description: '当前状态指导' }],
      },
    });
    check('写入 current 状态', () => assert.equal(obs1.applied, true));
    const obs2 = await engine.request('behavior_submit', {
      session_id: 'session-p13',
      workspace_id: WS,
      observation_type: 'task_requirement',
      target_layer: 'situational',
      observation_payload: { description: '本轮优先处理用户明确提出的问题' },
      ttl: 3600_000,
    });
    check('写入 situational 状态', () => assert.equal(obs2.target_layer, 'situational'));

    // 2. 读指导
    const guidance = await engine.request('behavior_guidance', { session_id: 'session-p13', workspace_id: WS });
    check('返回 BehaviorGuidance', () => {
      assert.equal(guidance.session_id, 'session-p13');
      assert.ok(Array.isArray(guidance.preferred));
      assert.ok(Array.isArray(guidance.temporary_overrides));
      assert.equal(guidance.temporary_overrides.length, 1);
      assert.ok(guidance.hard_constraints_note.includes('硬约束'));
    });
    check('情绪可用时不降级', () => {
      assert.equal(guidance.degraded, false);
      assert.equal(guidance.degradation_reasons.includes('emotion_summary_unavailable'), false);
    });

    // 3. pre-step 注入
    const preStep = ctx.handlers['agent/pre-step'];
    const d1 = await preStep(turnPayload(), enterWith([userMessage('m1', '你好')]));
    check('pre-step 注入记忆摘要与行为指导', () => {
      assert.equal(d1.messages.length, 3, `messages=${d1.messages.length}`);
      const texts = d1.messages.map((m) => m.content[0].text);
      assert.ok(texts.some((t) => t.includes('DSH状态')));
      assert.ok(texts.some((t) => t.includes('DSH行为指导')));
    });

    // 4. 行为异常回退
    await handle.engine.stop();
    const d2 = await preStep(turnPayload(), enterWith([userMessage('m2', 'hi')]));
    check('引擎停止时回退原 decision', () => {
      assert.equal(d2.kind, 'enter');
      assert.equal(d2.messages.length, 1);
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
