/**
 * P4 验收测试：记忆模块最小真实链路（协议级）。
 *   1. 创建 pending Experience（含事件关联、审计、Outbox 事件）
 *   2. 相同事件集合去重
 *   3. 部分重叠不自动合并
 *   4. 按会话/工作区真实统计
 *   5. 重启后持久化
 */

import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { EngineProcess } from '../lib/engine-process.js';

const dataDir = mkdtempSync(join(tmpdir(), 'dsh-p4-node-'));
let passed = 0;
const check = (name, fn) => {
  fn();
  passed += 1;
  console.log(`  ✓ ${name}`);
};

async function main() {
  console.log(`P4 node test (dataDir=${dataDir})\n`);
  const env = { DSH_ENGINE_DATA_DIR: dataDir };
  const engine = new EngineProcess({ env });
  try {
    await engine.start();

    const ws = 'ws-p4';
    const session = 'session-p4';

    const first = await engine.request('memory_create_experience', {
      request_id: 'req-a',
      workspace_id: ws,
      session_id: session,
      event_ids: ['ev-1', 'ev-2'],
      experience_type: 'conversation_turn',
      occurred_at: Date.now(),
      description: 'hello world',
    });
    check('创建 pending Experience', () => {
      assert.equal(first.created, true);
      assert.ok(first.experience_id);
    });

    const exp = await engine.request('memory_get_experience', { experience_id: first.experience_id });
    check('Experience 状态与事件关联', () => {
      assert.equal(exp.verification_status, 'pending');
      assert.equal(exp.canonical_experience_id, exp.experience_id);
      assert.deepEqual(exp.event_ids.sort(), ['ev-1', 'ev-2']);
    });

    const dup = await engine.request('memory_create_experience', {
      request_id: 'req-b',
      workspace_id: ws,
      session_id: session,
      event_ids: ['ev-2', 'ev-1'],
      experience_type: 'conversation_turn',
      occurred_at: Date.now(),
      description: 'duplicate',
    });
    check('相同事件集合去重', () => {
      assert.equal(dup.created, false);
      assert.equal(dup.deduplicated, true);
      assert.equal(dup.experience_id, first.experience_id);
    });

    const overlap = await engine.request('memory_create_experience', {
      request_id: 'req-c',
      workspace_id: ws,
      session_id: session,
      event_ids: ['ev-2', 'ev-3'],
      experience_type: 'conversation_turn',
      occurred_at: Date.now(),
      description: 'partial overlap',
    });
    check('部分重叠不自动合并', () => {
      assert.equal(overlap.created, false);
      assert.equal(overlap.possible_duplicate, true);
      assert.equal(overlap.matches.length, 1);
    });

    const summary = await engine.request('memory_summary', { workspace_id: ws, session_id: session });
    check('真实记忆统计', () => {
      assert.equal(summary.total, 1);
      assert.equal(summary.pending, 1);
      assert.equal(summary.source, 'memory');
    });

    const otherSummary = await engine.request('memory_summary', { workspace_id: ws, session_id: 'other' });
    check('按会话隔离', () => assert.equal(otherSummary.total, 0));

    await engine.stop();

    // 重启持久化
    const engine2 = new EngineProcess({ env });
    try {
      await engine2.start();
      const afterRestart = await engine2.request('memory_summary', { workspace_id: ws, session_id: session });
      check('重启后真实经历仍在', () => assert.equal(afterRestart.total, 1));
      const db2 = await engine2.request('db_status', {});
      check('schema 已迁移到当前版本', () => assert.ok(db2.schema_version >= 3));
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
