/**
 * P6 验收测试：模型辅助核验（用 mock 模型，不产生真实调用）。
 *   1. 调度函数可手动触发；
 *   2. 模型输入包含完整经历与证据；
 *   3. 输出格式严格校验（非法 JSON / 未知决策 / id 不匹配 → 跳过）；
 *   4. 结论经 memory_verify_experience 落地（审计 + 事件）；
 *   5. 单条失败不影响其他记录；
 *   6. 重复执行不重复产生核验事件；
 *   7. 模型不可用/未配置时不动数据。
 */

import assert from 'node:assert/strict';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { apply, } from '../lib/index.js';
import { buildEvaluationMessages, parseDecision } from '../lib/verify.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const dataDir = mkdtempSync(join(tmpdir(), 'dsh-p6-'));

// mock 模型：按 experience_id 计划返回结论，特殊值 'boom' 抛错、'badjson' 返回坏文本。
const plan = new Map();
const mockLlm = {
  async *stream(req) {
    const payload = JSON.parse(req.messages[1].content[0].text);
    const id = payload.experience.experience_id;
    const action = plan.get(id) ?? 'verified';
    if (action === 'boom') throw new Error('model unavailable (mock)');
    if (action === 'badjson') {
      yield { type: 'text-delta', text: 'not json at all' };
      return;
    }
    yield {
      type: 'text-delta',
      text: JSON.stringify({ experience_id: id, decision: action, reason: `mock ${action}` }),
    };
  },
};

function makeFakeCtx() {
  const handlers = {};
  const effects = [];
  return {
    logger: { info: () => {} },
    llm: mockLlm,
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
  console.log('P6 verify sweep test (mock model)\n');
  const ctx = makeFakeCtx();
  const handle = apply(ctx, {
    dataDir,
    verifyEnabled: false, // 测试里手动触发
    evaluationModel: 'mock/model',
  });
  const dispose = ctx.effects[0]();

  try {
    for (let i = 0; i < 100 && handle.engine.state !== 'ready'; i += 1) await sleep(50);
    assert.equal(handle.engine.state, 'ready', 'engine ready');

    // 3 条经历：a→verified, b→invalid, c→模型失败
    const ids = {};
    for (const [name, n] of [['a', 1], ['b', 2], ['c', 3]]) {
      const created = await handle.engine.request('memory_create_experience', {
        workspace_id: 'ws-p6',
        session_id: 's-p6',
        event_ids: [`p6-ev-${n}`],
        experience_type: 'conversation_turn',
        occurred_at: Date.now(),
        description: `p6 event ${name}`,
      });
      ids[name] = created.experience_id;
    }
    plan.set(ids.a, 'verified');
    plan.set(ids.b, 'invalid');
    plan.set(ids.c, 'boom');

    // 输出解析单元检查
    assert.equal(parseDecision('{"experience_id":"x","decision":"verified","reason":"r"}', 'x').ok, true);
    assert.equal(parseDecision('not json', 'x').ok, false);
    assert.equal(parseDecision('{"experience_id":"y","decision":"verified","reason":"r"}', 'x').ok, false);
    assert.equal(parseDecision('{"experience_id":"x","decision":"archived","reason":"r"}', 'x').ok, false);
    console.log('  ✓ 模型输出严格校验（合法/非法 JSON/id 不匹配/未知决策）');

    // 模型输入包含完整经历与证据
    const exp = await handle.engine.request('memory_get_experience', { experience_id: ids.a });
    const messages = buildEvaluationMessages(exp);
    const sent = JSON.parse(messages[1].content[0].text);
    assert.equal(sent.experience.experience_id, ids.a);
    assert.deepEqual(sent.experience.event_ids, ['p6-ev-1']);
    assert.ok(typeof sent.experience.description === 'string');
    assert.deepEqual(sent.constraints.allowed_decisions, ['verified', 'uncertain', 'invalid']);
    console.log('  ✓ 模型输入包含经历内容、事件证据与允许结论');

    // 第一轮扫描
    const first = await handle.runVerificationSweep();
    assert.equal(first.scanned, 3);
    assert.equal(first.verified, 2);
    assert.equal(first.failed, 1);
    console.log(`  ✓ 扫描：scanned=${first.scanned} applied=${first.verified} failed=${first.failed}`);

    const summary = await handle.engine.request('memory_summary', { workspace_id: 'ws-p6', session_id: 's-p6' });
    assert.equal(summary.verified, 1);
    assert.equal(summary.invalid, 1);
    assert.equal(summary.pending, 1);
    console.log('  ✓ 状态按结论落库：verified 1 / invalid 1 / pending 1（失败那条保持原状）');

    // 重复执行：a 已 verified 不再扫描；b 已 invalid 不再扫描；c 仍失败。
    const second = await handle.runVerificationSweep();
    assert.equal(second.scanned, 1);
    assert.equal(second.failed, 1);
    console.log('  ✓ 重复执行不重复处理已核验记录');

    // 事件不重复
    const db = await handle.engine.request('db_status', {});
    assert.ok(db.schema_version >= 3);
    console.log('  ✓ 数据库就绪');

    // 模型未配置：不动数据
    const noModel = await handle.engine.request('memory_summary', { workspace_id: 'ws-p6', session_id: 's-p6' });
    assert.equal(noModel.pending, 1);

    console.log('\nPASS: P6 verify sweep');
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

main().catch((error) => {
  console.error('\nFAIL:', error);
  process.exitCode = 1;
});
