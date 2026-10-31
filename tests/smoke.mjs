/**
 * P1 冒烟测试：直接驱动构建产物 lib/，验证
 *   1. Python 运行时探测
 *   2. 子进程启动与 ready 握手
 *   3. NDJSON 请求/响应配对
 *   4. 非法消息不崩溃
 *   5. 请求超时
 *   6. 优雅关闭
 *
 * 不依赖 DSH 宿主；宿主真实安装另见 README。
 */

import assert from 'node:assert/strict';
import { EngineProcess } from '../lib/engine-process.js';

const log = (line) => console.log(`  [engine] ${line}`);
let passed = 0;
const check = (name, fn) => {
  fn();
  passed += 1;
  console.log(`  ✓ ${name}`);
};

async function main() {
  console.log('P1 smoke test\n');
  const engine = new EngineProcess({ log });

  try {
    console.log('1) 启动与 ready 握手');
    await engine.start();
    assert.equal(engine.state, 'ready');
    assert.ok(engine.pythonInfo?.path, 'python path recorded');
    console.log(`  ✓ ready (python ${engine.pythonInfo.version} ← ${engine.pythonInfo.source})`);

    const pong = await engine.request('ping', {});
    check('ping 返回 pong', () => assert.equal(pong.pong, true));

    const status = await engine.request('get_status', {});
    check('get_status 返回引擎信息', () => {
      assert.equal(status.engine, 'dsh-engine');
      assert.equal(status.mode, 'infrastructure');
      assert.ok(status.python.startsWith('3.'));
    });

    const summary = await engine.request('get_summary', { seq: 7, session_id: 's1', workspace_id: 'w1' });
    check('get_summary 返回确定性摘要', () => {
      assert.equal(summary.marker, 'P2_TEST_SUMMARY');
      assert.equal(summary.seq, 7);
      assert.ok(summary.text.includes('#7'));
      assert.ok(summary.text.includes('s1'));
    });

    // 非法 JSON：直接写一行坏数据，随后 ping 仍应正常。
    engine.child.stdin.write('this is not json\n');
    await new Promise((r) => setTimeout(r, 120));
    const afterBad = await engine.request('ping', {});
    check('非法 JSON 不崩溃，后续请求正常', () => assert.equal(afterBad.pong, true));

    // 未知操作：返回结构化错误。
    let unknownError = null;
    try {
      await engine.request('no_such_operation', {});
    } catch (error) {
      unknownError = error;
    }
    check('未知操作返回结构化错误', () => {
      assert.ok(unknownError, 'expected an error');
      assert.equal(unknownError.code, 'UNKNOWN_OPERATION');
    });

    // 请求超时：slow_ping 睡 250ms，客户端 50ms 超时，确定性触发。
    let timeoutError = null;
    try {
      await engine.request('slow_ping', { seconds: 0.25 }, { timeoutMs: 50 });
    } catch (error) {
      timeoutError = error;
    }
    check('请求超时被标记', () => {
      assert.ok(timeoutError, 'expected timeout');
      assert.equal(timeoutError.code, 'ENGINE_TIMEOUT');
    });
    // 等慢请求在引擎侧自然结束，避免关闭时出现迟到响应干扰。
    await new Promise((r) => setTimeout(r, 300));

    console.log('2) 优雅关闭');
    await engine.stop();
    assert.equal(engine.state, 'stopped');
    console.log('  ✓ stopped');

    console.log(`\nPASS: ${passed} checks`);
  } finally {
    if (engine.state !== 'stopped') await engine.stop().catch(() => {});
  }
}

main().catch((error) => {
  console.error('\nFAIL:', error);
  process.exitCode = 1;
});
