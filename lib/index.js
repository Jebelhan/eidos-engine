/**
 * dsh-engine 插件入口。
 *
 * P1：启动 Python 状态引擎、就绪握手、卸载时优雅关闭。
 * P2：注册 agent/pre-step，每轮向引擎取一段摘要并注入模型上下文。
 *
 * 摘要为 P2 确定性测试数据，不代表任何真实业务状态。
 *
 * 说明：子进程管理使用 node:child_process。宿主另有 ctx.subprocess 统一服务，
 * 但其精确调用方式需要在真实 profile 中实测后才能替换，本阶段不猜测其 API。
 */

import { EngineProcess } from './engine-process.js';
import { createSummaryUserMessage } from './summary.js';
import { runVerificationSweep } from './verify.js';
import { collectEvidence } from './evidence.js';
import { runEmotionEval } from './emotion-eval.js';
import { runRelationshipEval } from './relationship-eval.js';
import { runLearningEval } from './learning-eval.js';
import { runDailyCycle, nextDailyRunMs, localScheduleDate } from './daily-cycle.js';
import { appendFile } from 'node:fs/promises';
import { createHash } from 'node:crypto';
import { homedir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const PLUGIN_ROOT = dirname(fileURLToPath(import.meta.url));
const DSH_HOME = process.env.DSH_HOME || join(homedir(), '.dsh');

/** 摘要/经历请求超时：任何超时都跳过注入，不影响正常回复。 */
const SUMMARY_TIMEOUT_MS = 1500;
const EXPERIENCE_TIMEOUT_MS = 2000;

/** 解析 "provider/model" 形式的评估模型路由。 */
function parseModelRoute(value) {
  if (typeof value !== 'string') return null;
  const idx = value.indexOf('/');
  if (idx <= 0 || idx === value.length - 1) return null;
  return { provider: value.slice(0, idx), model: value.slice(idx + 1) };
}

/** 从消息内容块中提取纯文本。 */
function extractText(message) {
  const blocks = message && Array.isArray(message.content) ? message.content : [];
  return blocks
    .filter((block) => block && block.type === 'text' && typeof block.text === 'string')
    .map((block) => block.text)
    .join('\n');
}

/** P2 验证用 trace：每轮注入写一行，便于在宿主之外确认 pre-step 实际生效。 */
async function traceInjection(line) {
  const path = process.env.DSH_ENGINE_TRACE || join(PLUGIN_ROOT, '..', '.trace.log');
  try {
    await appendFile(path, `${new Date().toISOString()} ${line}\n`, 'utf8');
  } catch {
    /* trace 失败不影响业务 */
  }
}

/** 日志输出：优先用宿主 logger，缺失时退回 stderr。 */
function makeLogger(ctx) {
  const logger = ctx && typeof ctx === 'object' ? ctx.logger : undefined;
  return (line) => {
    try {
      if (logger && typeof logger.info === 'function') logger.info(line);
      else process.stderr.write(`[dsh-engine] ${line}\n`);
    } catch {
      process.stderr.write(`[dsh-engine] ${line}\n`);
    }
  };
}

/**
 * 插件入口。
 * @param {import('@deepseek-ai/cordis').Context} ctx
 * @param {{ dataDir?: string, pythonPath?: string }} config
 */
/**
 * 判断宿主 llm 服务是否可用。
 * 用 cordis 的 ctx.get('llm') 读取服务，不依赖 inject 声明；任何异常按不可用处理。
 */
function llmAvailable(ctx) {
  try {
    const llm = ctx && typeof ctx.get === 'function' ? ctx.get('llm') : ctx?.llm;
    return Boolean(llm && typeof llm.stream === 'function');
  } catch {
    return false;
  }
}

/** 取 llm 服务值（供 verify.js 使用）。 */
export function resolveLlm(ctx) {
  try {
    return ctx && typeof ctx.get === 'function' ? ctx.get('llm') : ctx?.llm;
  } catch {
    return undefined;
  }
}

/** 行为指导摘要（≤300 tokens，软指导，不是强制命令）。 */
function formatBehaviorGuidance(guidance) {
  const lines = [];
  if (Array.isArray(guidance.preferred) && guidance.preferred.length > 0) {
    lines.push('当前倾向：');
    for (const item of guidance.preferred.slice(0, 5)) lines.push(`- ${item.description}`);
  }
  if (Array.isArray(guidance.temporary_overrides) && guidance.temporary_overrides.length > 0) {
    lines.push('当前情境：');
    for (const item of guidance.temporary_overrides.slice(0, 3)) lines.push(`- ${item.description}`);
  }
  if (Array.isArray(guidance.conflicts) && guidance.conflicts.length > 0) {
    lines.push('注意：存在长期倾向与当前情境的冲突，请结合用户当前意图判断。');
  }
  if (guidance.degraded) lines.push('（部分输入缺失，指导不可完全确定）');
  lines.push('以上为软指导，不是强制命令；硬约束由宿主守卫独立执行。');
  const text = `[EIDOS行为指导]\n${lines.join('\n')}`;
  return text.length > 1200 ? `${text.slice(0, 1200)}…` : text;
}

export function apply(ctx, config = {}) {
  const log = makeLogger(ctx);
  const dataDir = config.dataDir && String(config.dataDir).trim() !== '' ? String(config.dataDir) : join(DSH_HOME, 'dsh-engine');
  const engine = new EngineProcess({
    pythonPath: config.pythonPath || '',
    env: {
      DSH_ENGINE_DATA_DIR: dataDir,
      // P8-C 验证用测试消费者（echo-consumer），默认关闭。
      ...(config.testConsumer ? { DSH_ENGINE_TEST_CONSUMER: '1' } : {}),
    },
    log,
  });

  // 启动标记：确认宿主加载的代码版本，并记录模型服务可用性。
  void traceInjection(`boot marker=p6-build-3 llm=${llmAvailable(ctx) ? 'available' : 'unavailable'}`);

  let disposed = false;
  /** P6 核验调度定时器。 */
  let verifyTimer = null;

  /** P6 核验配置：默认每日一次，启动后延迟触发。 */
  const verifyConfig = {
    enabled: config.verifyEnabled !== false,
    intervalMs: Math.max(60_000, (Number(config.verifyIntervalHours) || 24) * 3_600_000),
    startupDelayMs: Math.max(5_000, (Number(config.verifyOnStartupDelaySeconds) || 60) * 1000),
    modelRoute: parseModelRoute(config.evaluationModel || 'deepseek-account/deepseek-flash'),
    timeoutMs: Number(config.verifyTimeoutMs) || 60_000,
    /** P7-B：是否把原始用户消息（脱敏+限长后）送入模型。隐私边界已确认（2026-10-31），默认开启。 */
    sendRawEvidence: config.sendRawEvidence !== false,
  };

  const runVerify = async () => {
    const result = await runVerificationSweep(ctx, engine, {
      modelRoute: verifyConfig.modelRoute,
      log,
      trace: traceInjection,
      timeoutMs: verifyConfig.timeoutMs,
      collectEvidence: verifyConfig.sendRawEvidence
        ? (experience) => collectEvidence(ctx, experience, { trace: traceInjection })
        : undefined,
    });
    await traceInjection(
      `verify-sweep scanned=${result.scanned} applied=${result.verified} skipped=${result.skipped} failed=${result.failed}`,
    );
    return result;
  };

  // ---------- P9：情绪证据评估（Node 模型 + Python 提交） ----------
  const emotionEvalIntervalMs = Math.max(10_000, (Number(config.emotionEvalIntervalSeconds) || 30) * 1000);
  let emotionTimer = null;

  const runEmotion = async () => {
    const result = await runEmotionEval(ctx, engine, {
      modelRoute: verifyConfig.modelRoute,
      log,
      trace: traceInjection,
      timeoutMs: verifyConfig.timeoutMs,
    });
    if (result.scanned > 0) {
      await traceInjection(
        `emotion-eval scanned=${result.scanned} submitted=${result.submitted} failed=${result.failed}`,
      );
    }
    return result;
  };

  const scheduleEmotionEval = (delayMs) => {
    if (disposed) return;
    emotionTimer = setTimeout(async () => {
      emotionTimer = null;
      try {
        if (engine.state === 'ready') await runEmotion();
      } catch (error) {
        log(`emotion eval failed: ${error instanceof Error ? error.message : String(error)}`);
      }
      scheduleEmotionEval(emotionEvalIntervalMs);
    }, delayMs);
    emotionTimer.unref?.();
  };

  // ---------- P10：关系证据评估（Node 模型 + Python 提交） ----------
  const relationshipEvalIntervalMs = Math.max(10_000, (Number(config.relationshipEvalIntervalSeconds) || 30) * 1000);
  let relationshipTimer = null;

  const runRelationship = async () => {
    const result = await runRelationshipEval(ctx, engine, {
      modelRoute: verifyConfig.modelRoute,
      log,
      trace: traceInjection,
      timeoutMs: verifyConfig.timeoutMs,
    });
    if (result.scanned > 0) {
      await traceInjection(
        `relationship-eval scanned=${result.scanned} submitted=${result.submitted} failed=${result.failed}`,
      );
    }
    return result;
  };

  const scheduleRelationshipEval = (delayMs) => {
    if (disposed) return;
    relationshipTimer = setTimeout(async () => {
      relationshipTimer = null;
      try {
        if (engine.state === 'ready') await runRelationship();
      } catch (error) {
        log(`relationship eval failed: ${error instanceof Error ? error.message : String(error)}`);
      }
      scheduleRelationshipEval(relationshipEvalIntervalMs);
    }, delayMs);
    relationshipTimer.unref?.();
  };

  // ---------- P14-D：每日整理链（Node 驱动八阶段，Python 只记账） ----------
  const dailyCycleTimeZone = String(config.dailyCycleTimeZone || 'Asia/Shanghai');
  let dailyTimer = null;

  const runDaily = async () => {
    await traceInjection(`daily-cycle start`);
    const result = await runDailyCycle(engine, {
      verify: runVerify,
      learningEval: runLearning,
      dispatch: runDispatch,
      log,
      trace: traceInjection,
      timeZone: dailyCycleTimeZone,
    });
    if (result && result.completed) {
      await traceInjection('daily-cycle completed');
    }
    return result;
  };

  const scheduleDaily = (delayMs) => {
    if (disposed) return;
    traceInjection(`daily-cycle scheduled delay=${delayMs}`);
    dailyTimer = setTimeout(async () => {
      dailyTimer = null;
      try {
        if (engine.state === 'ready') await runDaily();
      } catch (error) {
        log(`daily cycle failed: ${error instanceof Error ? error.message : String(error)}`);
      }
      scheduleDaily(Math.max(60_000, nextDailyRunMs() - Date.now()));
    }, delayMs);
    dailyTimer.unref?.();
  };

  // ---------- P11：学习证据评估（Node 模型 + Python 提交） ----------
  const learningEvalIntervalMs = Math.max(10_000, (Number(config.learningEvalIntervalSeconds) || 30) * 1000);
  let learningTimer = null;

  const runLearning = async () => {
    const result = await runLearningEval(ctx, engine, {
      modelRoute: verifyConfig.modelRoute,
      log,
      trace: traceInjection,
      timeoutMs: verifyConfig.timeoutMs,
    });
    if (result.scanned > 0) {
      await traceInjection(
        `learning-eval scanned=${result.scanned} submitted=${result.submitted} proposed=${result.proposed} failed=${result.failed}`,
      );
    }
    return result;
  };

  const scheduleLearningEval = (delayMs) => {
    if (disposed) return;
    learningTimer = setTimeout(async () => {
      learningTimer = null;
      try {
        if (engine.state === 'ready') await runLearning();
      } catch (error) {
        log(`learning eval failed: ${error instanceof Error ? error.message : String(error)}`);
      }
      scheduleLearningEval(learningEvalIntervalMs);
    }, delayMs);
    learningTimer.unref?.();
  };

  // ---------- P8-B：Outbox 投递调度（Node 定时触发，禁止重叠） ----------
  const dispatchIntervalMs = Math.max(3_000, (Number(config.dispatchIntervalSeconds) || 5) * 1000);
  let dispatchTimer = null;
  let dispatchBusy = false;

  const runDispatch = async () => {
    dispatchBusy = true;
    try {
      const stats = await engine.request('outbox_dispatch', { limit: 20 }, { timeoutMs: 15_000 });
      // 只在实际发生投递动作时记录，避免空转刷屏。
      if (stats.consumed > 0 || stats.failed > 0 || stats.dead > 0 || stats.completed > 0) {
        await traceInjection(
          `dispatch scanned=${stats.scanned} published=${stats.published} consumed=${stats.consumed} failed=${stats.failed} dead=${stats.dead} completed=${stats.completed}`,
        );
      }
      return stats;
    } finally {
      dispatchBusy = false;
    }
  };

  const scheduleDispatch = (delayMs) => {
    if (disposed) return;
    dispatchTimer = setTimeout(async () => {
      dispatchTimer = null;
      if (dispatchBusy) {
        // 上一轮未结束：跳过本轮，不重叠。
        scheduleDispatch(dispatchIntervalMs);
        return;
      }
      try {
        if (engine.state === 'ready') await runDispatch();
      } catch (error) {
        log(`dispatch failed: ${error instanceof Error ? error.message : String(error)}`);
      }
      scheduleDispatch(dispatchIntervalMs);
    }, delayMs);
    dispatchTimer.unref?.();
  };

  const scheduleVerify = (delayMs) => {
    if (!verifyConfig.enabled || disposed) return;
    verifyTimer = setTimeout(async () => {
      verifyTimer = null;
      try {
        const result = await runVerify();
        log(
          `verify sweep: scanned=${result.scanned} applied=${result.verified} skipped=${result.skipped} failed=${result.failed}`,
        );
      } catch (error) {
        log(`verify sweep failed: ${error instanceof Error ? error.message : String(error)}`);
      }
      scheduleVerify(verifyConfig.intervalMs);
    }, delayMs);
    verifyTimer.unref?.();
  };

  /** 每轮：把用户消息作为原始事件创建候选 Experience，再取真实记忆摘要注入。 */
  const onPreStep = async (payload, next) => {
    const decision = await next();
    if (!decision || decision.kind !== 'enter') return decision;

    try {
      const session = payload?.agent?.session;
      const sessionId = session?.id ? String(session.id) : 'unknown';
      // Session 的 cwd 在创建元数据 header 上（session.header.cwd）。
      const workspaceId = session?.header?.cwd
        ? String(session.header.cwd)
        : session?.cwd
          ? String(session.cwd)
          : 'unknown';

      // 只把真正的用户消息当作原始事件；注入的摘要（source.kind=dsh-engine）不算。
      const userMessages = (decision.messages ?? []).filter(
        (m) => m && m.role === 'user' && m.source && m.source.kind === 'user',
      );
      const eventIds = userMessages.map((m) => m.id).filter((id) => typeof id === 'string' && id !== '');

      // 1. 真实经历：本轮用户消息事件 → 候选 Experience（pending）。
      if (eventIds.length > 0) {
        const sorted = [...eventIds].sort();
        // 幂等键由事件集合决定：同一批消息重放时返回原 experience。
        const requestId = `exp_${createHash('sha1').update(sorted.join('|')).digest('hex').slice(0, 16)}`;
        await engine.request(
          'memory_create_experience',
          {
            workspace_id: workspaceId,
            session_id: sessionId,
            event_ids: sorted,
            experience_type: 'conversation_turn',
            occurred_at: Date.now(),
            description: extractText(userMessages[0]).slice(0, 200),
          },
          { requestId, timeoutMs: EXPERIENCE_TIMEOUT_MS },
        );
      }

      // 2. 真实记忆摘要（来自持久化数据库，不是模拟计数）。
      const summary = await engine.request(
        'memory_summary',
        { workspace_id: workspaceId, session_id: sessionId },
        { timeoutMs: SUMMARY_TIMEOUT_MS },
      );
      const at = new Date(summary.generated_at).toLocaleString('zh-CN', { hour12: false });
      const text =
        `[EIDOS状态] 记忆：本会话经历 ${summary.total}` +
        `（待核验 ${summary.pending} / 已核验 ${summary.verified} / 不确定 ${summary.uncertain}）` +
        ` | workspace=${workspaceId} | at=${at}`;

      const message = createSummaryUserMessage(text, { summary: `memory summary total=${summary.total}` });
      await traceInjection(`inject memory total=${summary.total} pending=${summary.pending} session=${sessionId}`);

      // 3. P13 行为指导（临时验证通道；P14 协调器正式接管）。
      let behaviorMessage = null;
      try {
        const guidance = await engine.request(
          'behavior_guidance',
          { session_id: sessionId, workspace_id: workspaceId },
          { timeoutMs: 1200 },
        );
        const guidanceText = formatBehaviorGuidance(guidance);
        if (guidanceText) {
          behaviorMessage = createSummaryUserMessage(guidanceText, { summary: 'behavior guidance' });
          await traceInjection(
            `inject behavior preferred=${guidance.preferred.length} overrides=${guidance.temporary_overrides.length} degraded=${guidance.degraded}`,
          );
        }
      } catch (error) {
        // 行为模块异常回退，不影响原 decision。
        log(`pre-step behavior guidance skipped: ${error instanceof Error ? error.message : String(error)}`);
      }

      return {
        ...decision,
        messages: behaviorMessage ? [...decision.messages, message, behaviorMessage] : [...decision.messages, message],
      };
    } catch (error) {
      log(`pre-step memory summary skipped: ${error instanceof Error ? error.message : String(error)}`);
      return decision;
    }
  };

  const dispose = () => {
    disposed = true;
    if (verifyTimer !== null) {
      clearTimeout(verifyTimer);
      verifyTimer = null;
    }
    if (dispatchTimer !== null) {
      clearTimeout(dispatchTimer);
      dispatchTimer = null;
    }
    if (emotionTimer !== null) {
      clearTimeout(emotionTimer);
      emotionTimer = null;
    }
    if (relationshipTimer !== null) {
      clearTimeout(relationshipTimer);
      relationshipTimer = null;
    }
    if (learningTimer !== null) {
      clearTimeout(learningTimer);
      learningTimer = null;
    }
    if (dailyTimer !== null) {
      clearTimeout(dailyTimer);
      dailyTimer = null;
    }
    return engine.stop();
  };

  engine.onExit((info) => {
    if (disposed) return;
    log(`engine exited unexpectedly (code=${info.code}, signal=${info.signal ?? 'none'})`);
  });

  const register = () => {
    // 引擎启动
    engine
      .start()
      .then(() => {
        log('engine ready');
        traceInjection(
          `plugin-start llm=${llmAvailable(ctx) ? 'available' : 'unavailable'} model=${
            verifyConfig.modelRoute ? `${verifyConfig.modelRoute.provider}/${verifyConfig.modelRoute.model}` : 'none'
          } verifyEnabled=${verifyConfig.enabled}`,
        );
        log(
          `verify: llm=${llmAvailable(ctx) ? 'available' : 'unavailable'} model=${
            verifyConfig.modelRoute ? `${verifyConfig.modelRoute.provider}/${verifyConfig.modelRoute.model}` : 'none'
          }`,
        );
        scheduleVerify(verifyConfig.startupDelayMs);
        scheduleDispatch(3_000);
        scheduleEmotionEval(60_000);
        scheduleRelationshipEval(90_000);
        scheduleLearningEval(120_000);
        // 启动补跑：今天 03:00 已过且今日周期未完成 → 立即跑一次；否则调度到下一个 03:00。
        const today = localScheduleDate(new Date(), dailyCycleTimeZone);
        engine
          .request('daily_cycle_recoverable', {}, { timeoutMs: 5000 })
          .then(async ({ cycles }) => {
            // 跨天未完成的周期：不补跑更早周期，标记 failed 留审计。
            for (const stale of cycles ?? []) {
              if (stale.schedule_date === today) continue;
              await engine
                .request('daily_cycle_abandon', { cycle_id: stale.cycle_id, reason: 'stale_cycle_after_crash' }, { timeoutMs: 5000 })
                .catch(() => {});
              await traceInjection(`daily-cycle abandon stale cycle=${stale.cycle_id} date=${stale.schedule_date}`);
            }
            return engine.request(
              'daily_cycle_begin',
              { timezone: dailyCycleTimeZone, schedule_date: today, scheduled_at: Date.now() },
              { timeoutMs: 5000 },
            );
          })
          .then((begin) => {
            const now = Date.now();
            const next = nextDailyRunMs(now, 3, dailyCycleTimeZone);
            const todayAt3 = next - 24 * 3600 * 1000;
            if (begin.created) {
              scheduleDaily(todayAt3 < now ? 1_000 : next - now);
            } else if (begin.cycle.status === 'completed' || begin.cycle.status === 'failed') {
              scheduleDaily(Math.max(60_000, next - now));
            } else {
              // 今日周期未完成（pending/running/recovery_pending）→ 立即补跑。
              scheduleDaily(1_000);
            }
          })
          .catch(() => scheduleDaily(60_000));
      })
      .catch((error) => log(`engine start failed: ${error instanceof Error ? error.message : String(error)}`));

    // pre-step 注册（宿主 ctx 才有 on；测试用的 fake ctx 可能没有）
    let offPreStep;
    if (ctx && typeof ctx.on === 'function') {
      offPreStep = ctx.on('agent/pre-step', onPreStep);
    }

    return () => {
      try {
        offPreStep?.();
      } catch {
        /* ignore */
      }
      return dispose();
    };
  };

  if (ctx && typeof ctx.effect === 'function') {
    ctx.effect(register);
  } else {
    // 无 ctx.effect（冒烟测试直接调用 apply）时，调用方负责 stop。
    register();
  }

  return {
    engine,
    dispose,
    runVerificationSweep: runVerify,
    runDispatch,
    runEmotionEval: runEmotion,
    runRelationshipEval: runRelationship,
    runLearningEval: runLearning,
    runDailyCycle: runDaily,
  };
}

export default apply;
