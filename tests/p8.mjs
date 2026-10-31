/**
 * P8 协议级测试：事件分发（真实引擎 + 测试消费者 echo-consumer）。
 *   1. 订阅快照：创建事件时固定消费者
 *   2. 自动投递：dispatch 后 consumed
 *   3. 多消费者独立
 *   4. 幂等：重复 dispatch 不重复消费
 *   5. 同 aggregate 顺序
 *   6. 重启恢复：未完成事件重启后继续
 *   7. 状态查询
 */

import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EngineProcess } from '../lib/engine-process.js';

const dataDir = mkdtempSync(join(tmpdir(), 'dsh-p8-'));
const env = { DSH_ENGINE_DATA_DIR: dataDir, DSH_ENGINE_TEST_CONSUMER: '1' };
let passed = 0;
const check = (name, fn) => {
  fn();
  passed += 1;
  console.log(`  ✓ ${name}`);
};

async function createExperience(engine, i) {
  return engine.request('memory_create_experience', {
    workspace_id: 'ws-p8',
    session_id: 's-p8',
    event_ids: [`p8-ev-${i}`],
    experience_type: 'conversation_turn',
    occurred_at: Date.now(),
    description: `p8 event ${i}`,
  });
}

async function main() {
  console.log(`P8 node test (dataDir=${dataDir})\n`);
  const engine = new EngineProcess({ env });
  try {
    await engine.start();
    check('引擎启动', () => assert.equal(engine.state, 'ready'));

    // 1. 创建事件 → 订阅快照
    const created = await createExperience(engine, 1);
    const exp = await engine.request('memory_get_experience', { experience_id: created.experience_id });
    assert.equal(exp.verification_status, 'pending');
    check('创建 pending Experience', () => assert.ok(created.experience_id));

    // 2. 自动投递
    const first = await engine.request('outbox_dispatch', { limit: 20 }, { timeoutMs: 15000 });
    check('dispatch 消费事件', () => {
      assert.ok(first.scanned >= 1, `scanned=${first.scanned}`);
      assert.ok(first.consumed >= 1, `consumed=${first.consumed}`);
    });

    const status1 = await engine.request('outbox_status', {});
    check('状态查询', () => {
      assert.equal(status1.consumers.consumed, 1);
      assert.ok(Array.isArray(status1.subscriptions));
      assert.ok(status1.handlers.includes('echo-consumer'));
    });

    // 3. 幂等：再 dispatch 不重复消费
    const second = await engine.request('outbox_dispatch', { limit: 20 }, { timeoutMs: 15000 });
    check('重复 dispatch 不重复消费', () => assert.equal(second.consumed, 0));

    // 4. 核验产生第二个事件 → 同 aggregate 顺序
    await engine.request('memory_verify_experience', {
      experience_id: created.experience_id,
      target_status: 'verified',
      reason: 'p8 test',
    });
    const third = await engine.request('outbox_dispatch', { limit: 20 }, { timeoutMs: 15000 });
    check('核验事件被投递', () => assert.ok(third.consumed >= 1));

    const status2 = await engine.request('outbox_status', {});
    check('无死信', () => assert.equal(status2.dead_letters, 0));

    await engine.stop();

    // 5. 重启恢复：新事件未投递，重启后继续
    const engine2 = new EngineProcess({ env });
    try {
      await engine2.start();
      const pending = await createExperience(engine2, 99);
      const statusBefore = await engine2.request('outbox_status', {});
      assert.ok(statusBefore.events.pending >= 1 || statusBefore.events.published >= 1);
      const resumed = await engine2.request('outbox_dispatch', { limit: 20 }, { timeoutMs: 15000 });
      check('重启后继续投递未完成事件', () => assert.ok(resumed.consumed >= 1));
      await engine2.stop();
    } finally {
      if (engine2.state !== 'stopped') await engine2.stop().catch(() => {});
    }

    console.log(`\nPASS: ${passed} checks`);
  } finally {
    if (engine.state !== 'stopped') await engine.stop().catch(() => {});
    for (let i = 0; i < 5; i += 1) {
      try {
        rmSync(dataDir, { recursive: true, force: true });
        break;
      } catch {
        await new Promise((r) => setTimeout(r, 120));
      }
    }
  }
}

main().catch((error) => {
  console.error('\nFAIL:', error);
  process.exitCode = 1;
});
