/**
 * P5 验收测试：Experience 创建—核验闭环（协议级）。
 *   1. 列出 pending
 *   2. pending → verified / invalid
 *   3. 非法转换被拒绝
 *   4. 同状态重复核验幂等
 *   5. 重启后状态与事件持久化
 */

import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EngineProcess } from '../lib/engine-process.js';

const dataDir = mkdtempSync(join(tmpdir(), 'dsh-p5-node-'));
let passed = 0;
const check = (name, fn) => {
  fn();
  passed += 1;
  console.log(`  ✓ ${name}`);
};

const env = { DSH_ENGINE_DATA_DIR: dataDir };

async function create(engine, i) {
  return engine.request('memory_create_experience', {
    workspace_id: 'ws-p5',
    session_id: 's-p5',
    event_ids: [`p5-ev-${i}`],
    experience_type: 'conversation_turn',
    occurred_at: Date.now(),
    description: `p5 event ${i}`,
  });
}

async function main() {
  console.log(`P5 node test (dataDir=${dataDir})\n`);
  const engine = new EngineProcess({ env });
  try {
    await engine.start();
    const a = await create(engine, 1);
    const b = await create(engine, 2);

    const pending1 = await engine.request('memory_list_pending', { workspace_id: 'ws-p5' });
    check('列出待核验经历', () => assert.equal(pending1.count, 2));

    const verified = await engine.request('memory_verify_experience', {
      experience_id: a.experience_id,
      target_status: 'verified',
      reason: 'test verified',
    });
    check('pending → verified', () => {
      assert.equal(verified.changed, true);
      assert.equal(verified.status, 'verified');
    });

    const summary1 = await engine.request('memory_summary', { workspace_id: 'ws-p5', session_id: 's-p5' });
    check('摘要反映核验结果', () => {
      assert.equal(summary1.verified, 1);
      assert.equal(summary1.pending, 1);
    });

    const invalid = await engine.request('memory_verify_experience', {
      experience_id: b.experience_id,
      target_status: 'invalid',
      reason: 'test invalid',
    });
    check('pending → invalid', () => assert.equal(invalid.status, 'invalid'));

    let illegal = null;
    try {
      await engine.request('memory_verify_experience', {
        experience_id: a.experience_id,
        target_status: 'uncertain',
      });
    } catch (error) {
      illegal = error;
    }
    check('非法转换被拒绝', () => {
      assert.ok(illegal, 'expected error');
      assert.match(String(illegal.message), /illegal transition/);
    });

    const again = await engine.request('memory_verify_experience', {
      experience_id: a.experience_id,
      target_status: 'verified',
    });
    check('同状态重复核验幂等', () => {
      assert.equal(again.changed, false);
      assert.equal(again.already, true);
    });

    const events = await engine.request('db_status', {});
    check('schema 就绪', () => assert.ok(events.schema_version >= 3));

    await engine.stop();

    // 重启：状态与事件仍在。
    const engine2 = new EngineProcess({ env });
    try {
      await engine2.start();
      const summary2 = await engine2.request('memory_summary', { workspace_id: 'ws-p5', session_id: 's-p5' });
      check('重启后核验状态保持', () => {
        assert.equal(summary2.verified, 1);
        assert.equal(summary2.invalid, 1);
      });
      const pendingAfter = await engine2.request('memory_list_pending', { workspace_id: 'ws-p5' });
      check('重启后无待核验', () => assert.equal(pendingAfter.count, 0));
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
