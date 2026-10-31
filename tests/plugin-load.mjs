/**
 * 插件加载测试：用最小 fake ctx 验证 index.js 的 apply 契约。
 *
 * 验证点：
 *   1. 导出形态符合 Cordis Host 插件（apply(ctx, config)）；
 *   2. apply 返回生命周期句柄；
 *   3. 引擎在 effect 内启动并达到 ready；
 *   4. 卸载（dispose）能停止子进程。
 *
 * 真实宿主安装需要 plugin_manager，本环境无法执行，见 README。
 */

import assert from 'node:assert/strict';
import { apply } from '../lib/index.js';

const logs = [];

function makeFakeCtx() {
  const effects = [];
  return {
    logger: { info: (line) => logs.push(line) },
    effect(fn) {
      effects.push(fn);
      return () => {};
    },
    get effects() {
      return effects;
    },
  };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function main() {
  console.log('plugin load test\n');
  const ctx = makeFakeCtx();

  assert.equal(typeof apply, 'function', 'apply must be exported');
  const handle = apply(ctx, {});
  assert.ok(handle && handle.engine, 'apply must return engine handle');
  assert.ok(typeof handle.dispose === 'function', 'apply must return dispose');
  assert.equal(ctx.effects.length, 1, 'one effect registered');
  console.log('  ✓ 导出形态与 effect 注册正确');

  // fake ctx 不会自动执行 effect，这里手动执行拿到 dispose。
  const dispose = ctx.effects[0]();
  assert.equal(typeof dispose, 'function', 'effect must return disposer');
  console.log('  ✓ effect disposer 可用');

  // 等引擎 ready
  for (let i = 0; i < 100 && handle.engine.state !== 'ready'; i += 1) await sleep(50);
  assert.equal(handle.engine.state, 'ready', `engine ready (state=${handle.engine.state})`);
  console.log(`  ✓ 引擎在 effect 内启动就绪 (python ${handle.engine.pythonInfo.version})`);

  await dispose();
  assert.equal(handle.engine.state, 'stopped', 'engine stopped after dispose');
  console.log('  ✓ dispose 停止子进程');

  console.log('\nPASS: plugin apply contract');
}

main().catch((error) => {
  console.error('\nFAIL:', error);
  process.exitCode = 1;
});
