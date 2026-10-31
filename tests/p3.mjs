/**
 * P3 验收测试：走真实协议，验证基础设施。
 *   1. 引擎启动时建库、迁移到 schema v1；
 *   2. db_status 报告表结构；
 *   3. self_test 覆盖幂等 / Audit / Outbox；
 *   4. 重启后数据库与迁移记录持久保留、不重复迁移；
 *   5. 数据目录可配置。
 */

import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EngineProcess } from '../lib/engine-process.js';

const dataDir = mkdtempSync(join(tmpdir(), 'dsh-p3-node-'));
let passed = 0;
const check = (name, fn) => {
  fn();
  passed += 1;
  console.log(`  ✓ ${name}`);
};

async function main() {
  console.log(`P3 node test (dataDir=${dataDir})\n`);

  const engine = new EngineProcess({ env: { DSH_ENGINE_DATA_DIR: dataDir } });
  try {
    await engine.start();
    check('引擎启动', () => assert.equal(engine.state, 'ready'));

    const status = await engine.request('get_status', {});
    check('schema 已迁移到当前版本', () => assert.ok(status.schema_version >= 2));

    const dbStatus = await engine.request('db_status', {});
    check('db_status 报告表结构', () => {
      for (const table of ['request_dedup', 'audit_records', 'outbox_events', 'event_consumer_records']) {
        assert.ok(dbStatus.tables.includes(table), `missing table: ${table}`);
      }
      assert.ok(dbStatus.schema_version >= 2);
      assert.equal(dbStatus.integrity, 'ok');
    });

    const selfTest = await engine.request('self_test', {}, { timeoutMs: 15000 });
    check('self_test 全部通过', () => {
      assert.equal(selfTest.ok, true, JSON.stringify(selfTest.checks.filter((c) => !c.ok)));
      assert.ok(selfTest.checks.length >= 7);
    });
    for (const c of selfTest.checks) console.log(`    - ${c.name}: ${c.ok ? 'ok' : 'FAIL'}`);

    await engine.stop();
    check('关闭引擎', () => assert.equal(engine.state, 'stopped'));

    // 重启：同一数据目录，迁移不应重复执行，数据保留。
    const engine2 = new EngineProcess({ env: { DSH_ENGINE_DATA_DIR: dataDir } });
    try {
      await engine2.start();
      const db2 = await engine2.request('db_status', {});
      check('重启后 schema 不变', () => assert.ok(db2.schema_version >= 2));
      check('迁移记录未重复', () => assert.equal(db2.migrations.length, dbStatus.migrations.length));
      const status2 = await engine2.request('get_status', {});
      check('重启后引擎就绪', () => assert.ok(status2.schema_version >= 2));
      await engine2.stop();
    } finally {
      if (engine2.state !== 'stopped') await engine2.stop().catch(() => {});
    }

    console.log(`\nPASS: ${passed} checks`);
  } finally {
    if (engine.state !== 'stopped') await engine.stop().catch(() => {});
    // Windows 下 SQLite WAL 句柄可能晚一拍释放；清理失败不算测试失败。
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
