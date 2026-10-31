/**
 * P14-D：每日整理链执行器（Node 驱动，Python 只记账）。
 *
 * 八阶段：记忆整理 → 情绪推进 → 学习整理 → 学习评估 → 人格建议投递 → 人格结算 → 对账 → Audit。
 * 阶段幂等：每阶段稳定 request_id = daily_cycle:{cycle_id}:{step_name}（业务幂等由各模块保证）。
 * 失败：阶段失败即停止后续，周期进入 failed；启动恢复时跳过已完成阶段。
 */

import { createHash } from 'node:crypto';

export const DAILY_STEP_ORDER = [
  'memory_curation',
  'emotion_advance',
  'learning_curation',
  'learning_evaluation',
  'personality_suggest_delivery',
  'personality_settlement',
  'reconciliation',
  'audit_record',
];

export function localScheduleDate(date = new Date(), timeZone = 'Asia/Shanghai') {
  // 用 Intl 按 IANA 时区取本地日期（YYYY-MM-DD）。
  const parts = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(date);
  const get = (type) => parts.find((p) => p.type === type)?.value ?? '00';
  return `${get('year')}-${get('month')}-${get('day')}`;
}

export function nextDailyRunMs(now = Date.now(), hour = 3, timeZone = 'Asia/Shanghai') {
  // 简化：按 IANA 时区计算下一次本地 hour:00。
  const offsetMinutes = (date) => {
    const dtf = new Intl.DateTimeFormat('en-US', {
      timeZone,
      hour12: false,
      year: 'numeric',
      month: '2-digit',
      day: '2-digit',
      hour: '2-digit',
      minute: '2-digit',
      second: '2-digit',
    });
    const parts = dtf.formatToParts(date);
    const get = (t) => Number(parts.find((p) => p.type === t)?.value ?? 0);
    const asUtc = Date.UTC(get('year'), get('month') - 1, get('day'), get('hour') === 24 ? 0 : get('hour'), get('minute'), get('second'));
    return Math.round((asUtc - date.getTime()) / 60000);
  };
  const base = new Date(now);
  const offset = offsetMinutes(base);
  // 目标本地 hour:00 → UTC 毫秒
  const localDate = localScheduleDate(base, timeZone).split('-').map(Number);
  let target = Date.UTC(localDate[0], localDate[1] - 1, localDate[2], hour, 0, 0) - offset * 60000;
  if (target <= now) target += 24 * 3600 * 1000;
  return target;
}

export async function runDailyCycle(engine, options = {}) {
  const log = options.log ?? (() => {});
  const trace = options.trace ?? (() => {});
  const timeZone = options.timeZone ?? 'Asia/Shanghai';
  const verify = options.verify;
  const learningEval = options.learningEval;
  const dispatch = options.dispatch;
  const requestTimeout = options.requestTimeout ?? 10000;

  const scheduleDate = localScheduleDate(new Date(), timeZone);
  const begin = await engine.request(
    'daily_cycle_begin',
    { timezone: timeZone, schedule_date: scheduleDate, scheduled_at: Date.now() },
    { timeoutMs: requestTimeout },
  );
  const cycleId = begin.cycle.cycle_id;

  // 已有周期：已完成/失败 → 跳过；running/recovery_pending → 恢复后重试。
  if (!begin.created) {
    const status = await engine.request('daily_cycle_status', { cycle_id: cycleId }, { timeoutMs: requestTimeout });
    if (status.cycle.status === 'completed') {
      trace(`daily-cycle skip completed cycle=${cycleId}`);
      return { cycleId, skipped: true, reason: 'completed' };
    }
    if (status.cycle.status === 'failed') {
      trace(`daily-cycle skip failed cycle=${cycleId}`);
      return { cycleId, skipped: true, reason: 'failed' };
    }
    if (status.cycle.status === 'running' || status.cycle.status === 'recovery_pending') {
      await engine.request('daily_cycle_recover', { cycle_id: cycleId }, { timeoutMs: requestTimeout });
    }
  }

  const claimed = await engine.request('daily_cycle_claim', { cycle_id: cycleId }, { timeoutMs: requestTimeout });
  if (!claimed.claimed) {
    trace(`daily-cycle claim conflict cycle=${cycleId}`);
    return { cycleId, skipped: true, reason: 'claimed_elsewhere' };
  }

  const now = Date.now();
  const runStep = async (stepName) => {
    const status = await engine.request('daily_cycle_status', { cycle_id: cycleId }, { timeoutMs: requestTimeout });
    const step = status.steps.find((s) => s.step_name === stepName);
    if (step && step.status === 'completed') return 'already_completed';
    await engine.request('daily_cycle_step_begin', { cycle_id: cycleId, step_name: stepName }, { timeoutMs: requestTimeout });
    try {
      if (stepName === 'memory_curation') {
        const result = await verify();
        await engine.request('daily_cycle_step_complete', {
          cycle_id: cycleId,
          step_name: stepName,
          detail: { scanned: result?.scanned ?? 0, applied: (result?.verified ?? 0) + (result?.uncertain ?? 0) },
        }, { timeoutMs: requestTimeout });
        return 'completed';
      }
      if (stepName === 'emotion_advance') {
        const workspaces = await engine.request('memory_workspaces', {}, { timeoutMs: requestTimeout });
        const advanced = [];
        for (const workspace of workspaces.workspaces ?? []) {
          const result = await engine.request(
            'emotion_advance',
            { workspace_id: workspace, target_time: now },
            { timeoutMs: requestTimeout },
          );
          if (result.advanced) advanced.push(workspace);
        }
        await engine.request('daily_cycle_step_complete', {
          cycle_id: cycleId,
          step_name: stepName,
          detail: { advanced: advanced.length, workspaces: advanced },
        }, { timeoutMs: requestTimeout });
        return 'completed';
      }
      if (stepName === 'learning_curation') {
        await engine.request('daily_cycle_step_skip', { cycle_id: cycleId, step_name: stepName, reason: 'no_curation_in_p14' }, { timeoutMs: requestTimeout });
        return 'skipped';
      }
      if (stepName === 'learning_evaluation') {
        const result = await learningEval();
        await engine.request('daily_cycle_step_complete', {
          cycle_id: cycleId,
          step_name: stepName,
          detail: { scanned: result?.scanned ?? 0, submitted: result?.submitted ?? 0, proposed: result?.proposed ?? 0 },
        }, { timeoutMs: requestTimeout });
        return 'completed';
      }
      if (stepName === 'personality_suggest_delivery') {
        const result = await dispatch();
        await engine.request('daily_cycle_step_complete', {
          cycle_id: cycleId,
          step_name: stepName,
          detail: { consumed: result?.consumed ?? 0 },
        }, { timeoutMs: requestTimeout });
        return 'completed';
      }
      if (stepName === 'personality_settlement') {
        // P14-E2：先衰减、后结算；两个稳定 request_id 分别幂等。
        const decayRequestId = `daily_cycle:${cycleId}:personality_pressure_decay`;
        const settleRequestId = `daily_cycle:${cycleId}:personality_settlement`;
        const decayResult = await engine.request(
          'personality_decay',
          { as_of: now, cycle_id: cycleId },
          { requestId: decayRequestId, timeoutMs: requestTimeout },
        );
        const result = await engine.request(
          'personality_settle',
          { now },
          { requestId: settleRequestId, timeoutMs: requestTimeout },
        );
        await engine.request('daily_cycle_step_complete', {
          cycle_id: cycleId,
          step_name: stepName,
          detail: {
            decay: {
              status: decayResult.status,
              decayed_instances: decayResult.decayed_instances ?? 0,
              total_decay_amount: decayResult.total_decay_amount ?? 0,
            },
            settled: result.settled ?? false,
            reason: result.reason ?? null,
            actual_change: result.actual_change ?? 0,
          },
        }, { timeoutMs: requestTimeout });
        return 'completed';
      }
      if (stepName === 'reconciliation') {
        const reconciliation = await engine.request(
          'reconcile_outbox',
          { run_type: 'incremental' },
          { requestId: `daily_cycle:${cycleId}:outbox_reconciliation`, timeoutMs: 30000 },
        );
        const outbox = await engine.request('outbox_status', {}, { timeoutMs: requestTimeout });
        await engine.request('daily_cycle_step_complete', {
          cycle_id: cycleId,
          step_name: stepName,
          detail: {
            reconciliation: {
              run_id: reconciliation.run_id,
              checked_events: reconciliation.checked_events,
              issue_count: reconciliation.issue_count,
              p0: reconciliation.p0_count,
              p1: reconciliation.p1_count,
              p2: reconciliation.p2_count,
            },
            outbox,
          },
        }, { timeoutMs: requestTimeout });
        return 'completed';
      }
      // audit_record：由 daily_cycle_complete 统一写 Audit（同事务）。
      await engine.request('daily_cycle_step_complete', { cycle_id: cycleId, step_name: stepName, detail: { deferred_to: 'daily_cycle_complete' } }, { timeoutMs: requestTimeout });
      return 'completed';
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      await engine.request('daily_cycle_step_fail', {
        cycle_id: cycleId,
        step_name: stepName,
        error: message,
      }, { timeoutMs: requestTimeout }).catch(() => {});
      trace(`daily-cycle step failed step=${stepName} error=${message}`);
      log(`daily cycle step failed: ${stepName}: ${message}`);
      return 'failed';
    }
  };

  for (const stepName of DAILY_STEP_ORDER) {
    const result = await runStep(stepName);
    if (result === 'failed') {
      return { cycleId, completed: false, failedStep: stepName };
    }
    if (stepName === 'audit_record') break;
  }

  await engine.request('daily_cycle_complete', {
    cycle_id: cycleId,
    detail: { schedule_date: scheduleDate, timezone: timeZone, finished_at: Date.now() },
  }, { timeoutMs: requestTimeout });
  trace(`daily-cycle completed cycle=${cycleId} date=${scheduleDate}`);
  return { cycleId, completed: true };
}

export function stepKey(cycleId, stepName) {
  return `daily_cycle:${cycleId}:${stepName}`;
}

export function stableId(prefix, ...parts) {
  return createHash('sha1').update([prefix, ...parts].join('|')).digest('hex').slice(0, 24);
}
