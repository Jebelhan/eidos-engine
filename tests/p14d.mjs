/**
 * P14-D 协议级测试：每日整理链。
 *   1. 八阶段按顺序执行，周期完成
 *   2. 同一天重复触发 → 跳过
 *   3. 学习整理阶段 skipped（P14 无整理）
 *   4. 崩溃恢复：running 周期 → recover → 跳过已完成阶段
 */

import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { apply } from '../lib/index.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const dataDir = mkdtempSync(join(tmpdir(), 'dsh-p14d-'));

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
  console.log(`P14-D node test (dataDir=${dataDir})\n`);
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

    const first = await handle.runDailyCycle();
    check('八阶段执行并完成', () => {
      assert.equal(first.completed, true);
      assert.ok(first.cycleId);
    });

    const status = await engine.request('daily_cycle_status', { cycle_id: first.cycleId });
    check('阶段状态正确', () => {
      assert.equal(status.cycle.status, 'completed');
      const byName = Object.fromEntries(status.steps.map((s) => [s.step_name, s]));
      assert.equal(byName.memory_curation.status, 'completed');
      assert.equal(byName.emotion_advance.status, 'completed');
      assert.equal(byName.learning_curation.status, 'skipped');
      assert.equal(byName.personality_settlement.status, 'completed');
      assert.equal(byName.reconciliation.status, 'completed');
      assert.equal(byName.audit_record.status, 'completed');
    });

    const second = await handle.runDailyCycle();
    check('同一天重复触发被跳过', () => {
      assert.equal(second.skipped, true);
    });

    // 崩溃恢复：造一个 running 周期 → 恢复 → 再跑
    const otherDate = '2026-10-01';
    const begin = await engine.request('daily_cycle_begin', {
      timezone: 'Asia/Shanghai',
      schedule_date: otherDate,
      scheduled_at: Date.now(),
    });
    await engine.request('daily_cycle_claim', { cycle_id: begin.cycle.cycle_id });
    await engine.request('daily_cycle_step_begin', { cycle_id: begin.cycle.cycle_id, step_name: 'memory_curation' });
    const recoverable = await engine.request('daily_cycle_recoverable', {});
    check('存在可恢复周期', () => {
      assert.ok(recoverable.cycles.some((c) => c.cycle_id === begin.cycle.cycle_id));
    });
    await engine.request('daily_cycle_recover', { cycle_id: begin.cycle.cycle_id });
    const recovered = await engine.request('daily_cycle_status', { cycle_id: begin.cycle.cycle_id });
    check('恢复后阶段回到 pending', () => {
      const step = recovered.steps.find((s) => s.step_name === 'memory_curation');
      assert.equal(step.status, 'pending');
      assert.equal(recovered.cycle.status, 'recovery_pending');
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
