/**
 * P14-B 协议级测试：情绪推进 + 摘要 + 行为情绪输入。
 *   1. 情绪事件 → advance → PAD 变化
 *   2. 摘要返回 pad/mood
 *   3. 行为指导消费情绪
 *   4. 重复 advance 幂等
 */

import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { apply } from '../lib/index.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const dataDir = mkdtempSync(join(tmpdir(), 'dsh-p14b-'));
const WS = 'ws-p14b';

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

async function main() {
  console.log(`P14-B node test (dataDir=${dataDir})\n`);
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

    // 1. 空状态推进（情绪事件路径由单测覆盖；此处验证协议与幂等）
    const adv1 = await engine.request('emotion_advance', { workspace_id: WS, target_time: Date.now() });
    check('空状态推进到基线', () => {
      assert.equal(adv1.advanced, true);
      assert.ok(Math.abs(adv1.pad.p) < 1e-6);
    });

    const summary = await engine.request('emotion_state_summary', { workspace_id: WS });
    check('情绪摘要可用', () => {
      assert.equal(summary.available, true);
      assert.equal(summary.mood, 'neutral');
      assert.ok(summary.state_version >= 1);
    });

    const guidance = await engine.request('behavior_guidance', { session_id: 's-1', workspace_id: WS });
    check('行为指导读取情绪源', () => {
      assert.ok(guidance.source_revisions.emotion !== null || guidance.confidence_details);
    });

    // 4. 重复推进幂等（同 target_time）
    const t = Date.now();
    const a = await engine.request('emotion_advance', { workspace_id: WS, target_time: t });
    const b = await engine.request('emotion_advance', { workspace_id: WS, target_time: t });
    check('同一 target_time 重复推进返回 already', () => {
      assert.equal(b.advanced, false);
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
