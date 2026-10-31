/**
 * P7-B 证据读取测试（mock sessionPersistence，不触真实宿主）。
 *   1. 单条/多条用户消息读取
 *   2. assistant/tool/system 过滤
 *   3. event_id 精确匹配
 *   4. workspace 隔离
 *   5. 会话缺失 / 打开失败降级
 *   6. 缺失事件记录
 *   7. 凭证脱敏
 *   8. 单条/总长度限制
 */

import assert from 'node:assert/strict';
import { collectEvidence, redactSecrets } from '../lib/evidence.js';

const userEvent = (id, text) => ({
  type: 'user/message',
  data: { id, role: 'user', content: [{ type: 'text', text }], source: { kind: 'user' } },
});
const assistantEvent = (id, text) => ({
  type: 'assistant/message',
  data: { id, role: 'assistant', content: [{ type: 'text', text }] },
});
const toolEvent = (id, text) => ({
  type: 'tool/result',
  data: { id, role: 'tool', content: [{ type: 'text', text }] },
});

function makeCtx(events, options = {}) {
  const sp = {
    async stat() {
      if (options.statFail) throw new Error('missing session');
      return { header: { cwd: options.cwd ?? 'C:/ws' } };
    },
    async open() {
      if (options.openFail) throw new Error('open failed');
      return {
        async read() {
          return { events };
        },
        async close() {},
      };
    },
  };
  return { get: (name) => (name === 'sessionPersistence' ? sp : undefined) };
}

const experience = (overrides = {}) => ({
  experience_id: 'exp-1',
  workspace_id: 'C:/ws',
  session_id: 'session-1',
  description: 'test',
  event_ids: ['ev-1'],
  ...overrides,
});

let passed = 0;
const check = (name, fn) => {
  fn();
  passed += 1;
  console.log(`  ✓ ${name}`);
};

async function main() {
  console.log('P7-B evidence test\n');

  const ctx = makeCtx([userEvent('ev-1', '今天修好了数据库'), assistantEvent('ev-1', '好的'), toolEvent('ev-9', 'tool output')]);
  const one = await collectEvidence(ctx, experience());
  check('单条用户消息读取', () => {
    assert.equal(one.ok, true);
    assert.equal(one.evidence.length, 1);
    assert.equal(one.evidence[0].text, '今天修好了数据库');
    assert.equal(one.missing.length, 0);
  });

  const multiCtx = makeCtx([userEvent('ev-1', '第一条'), userEvent('ev-2', '第二条'), userEvent('ev-3', '无关')]);
  const multi = await collectEvidence(multiCtx, experience({ event_ids: ['ev-1', 'ev-2'] }));
  check('多条关联事件读取', () => {
    assert.equal(multi.evidence.length, 2);
    assert.deepEqual(multi.evidence.map((e) => e.event_id), ['ev-1', 'ev-2']);
  });

  const filtered = await collectEvidence(multiCtx, experience({ event_ids: ['ev-1', 'ev-2'] }));
  check('assistant/tool 事件被过滤', () => {
    assert.ok(filtered.evidence.every((e) => e.text !== 'tool output'));
  });

  const exact = await collectEvidence(multiCtx, experience({ event_ids: ['ev-2'] }));
  check('event_id 精确匹配（不读无关事件）', () => {
    assert.equal(exact.evidence.length, 1);
    assert.equal(exact.evidence[0].event_id, 'ev-2');
  });

  const mismatch = await collectEvidence(makeCtx([userEvent('ev-1', 'x')], { cwd: 'C:/other' }), experience());
  check('workspace 不匹配拒绝读取', () => assert.deepEqual(mismatch, { ok: false, reason: 'workspace-mismatch' }));

  const noSession = await collectEvidence(ctx, experience({ session_id: null }));
  check('缺 session_id 降级', () => assert.equal(noSession.ok, false));

  const notFound = await collectEvidence(makeCtx([], { statFail: true }), experience());
  check('会话不存在降级', () => assert.equal(notFound.reason, 'session-not-found'));

  const openFail = await collectEvidence(makeCtx([], { openFail: true }), experience());
  check('打开失败降级', () => assert.equal(openFail.reason, 'open-failed'));

  const missing = await collectEvidence(makeCtx([userEvent('ev-1', '只有一条')]), experience({ event_ids: ['ev-1', 'ev-missing'] }));
  check('缺失事件记录 missing', () => {
    assert.equal(missing.ok, true);
    assert.deepEqual(missing.missing, ['ev-missing']);
  });

  check('凭证脱敏', () => {
    const redacted = redactSecrets('key=sk-abcdefghijklmnopqrstuvwxyz123456 token=abc123 password=hunter2');
    assert.ok(!redacted.includes('sk-abcdefghijklmnopqrstuvwxyz123456'));
    assert.ok(!redacted.includes('abc123'));
    assert.ok(!redacted.includes('hunter2'));
    assert.ok(redacted.includes('[REDACTED]'));
  });

  const longText = '字'.repeat(3000);
  const truncated = await collectEvidence(makeCtx([userEvent('ev-1', longText)]), experience(), { maxPerItem: 2000, maxTotal: 8000 });
  check('单条长度限制 2000', () => {
    assert.equal(truncated.evidence[0].chars, 2000);
    assert.equal(truncated.evidence[0].text.length, 2000);
  });

  const manyEvents = Array.from({ length: 10 }, (_, i) => userEvent(`ev-${i}`, 'x'.repeat(1500)));
  const total = await collectEvidence(
    makeCtx(manyEvents),
    experience({ event_ids: manyEvents.map((e) => e.data.id) }),
    { maxPerItem: 2000, maxTotal: 8000 },
  );
  check('总长度限制 8000', () => {
    assert.ok(total.totalChars <= 8000, `total=${total.totalChars}`);
    assert.ok(total.evidence.length < 10);
  });

  console.log(`\nPASS: ${passed} checks`);
}

main().catch((error) => {
  console.error('\nFAIL:', error);
  process.exitCode = 1;
});
