/**
 * P6：模型辅助核验。
 *
 * 职责边界：
 *   Node    —— 调度、取待核验经历、调模型、校验输出、回调记忆模块核验；
 *   模型    —— 只给结论（不允许写库、不允许自定义状态）；
 *   Python  —— 状态机、幂等、Audit、Outbox（唯一写入口）。
 *
 * 防过期：评估时携带读取时的 verification_status，核验写入时校验。
 */

import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const PLUGIN_ROOT = join(fileURLToPath(import.meta.url), '..');

/** 允许的核验结论（与记忆 V1.1.2 状态机一致）。 */
export const ALLOWED_DECISIONS = new Set(['verified', 'uncertain', 'invalid']);

/** 评估提示词版本：核验审计里记录，便于追溯。 */
export const PROMPT_VERSION = 'p6-eval-v1';

function readPrompt() {
  try {
    return readFileSync(join(PLUGIN_ROOT, 'prompts', 'verify-experience.md'), 'utf8');
  } catch {
    return FALLBACK_PROMPT;
  }
}

const FALLBACK_PROMPT = `你是 DSH 记忆模块的核验助手。判断一条经历是否满足核验标准，只输出 JSON。`;

/**
 * 构造模型输入：经历内容 + 可用证据 + 判定标准。
 * 只给记忆模块允许提供的内容，不夹带其他模块数据。
 */
export function buildEvaluationMessages(experience, options = {}) {
  const system = readPrompt();
  const evidence = Array.isArray(options.evidence) ? options.evidence : null;
  const payload = {
    experience: {
      experience_id: experience.experience_id,
      experience_type: experience.experience_type,
      description: experience.description,
      occurred_at: experience.occurred_at,
      created_at: experience.created_at,
      verification_status: experience.verification_status,
      event_ids: experience.event_ids,
      workspace_id: experience.workspace_id,
    },
    evidence: evidence === null ? null : evidence,
    constraints: {
      allowed_decisions: ['verified', 'uncertain', 'invalid'],
      notes: '只能依据提供的经历内容与证据判断；证据不足时输出 uncertain，不要臆造。',
    },
  };
  return [
    { role: 'system', content: [{ type: 'text', text: system }] },
    { role: 'user', content: [{ type: 'text', text: JSON.stringify(payload, null, 2) }] },
  ];
}

/**
 * 严格解析模型输出。
 * 返回 { ok: true, decision, reason } 或 { ok: false, error }。
 */
export function parseDecision(rawText, expectedExperienceId) {
  if (typeof rawText !== 'string' || rawText.trim() === '') {
    return { ok: false, error: 'empty model output' };
  }
  let text = rawText.trim();
  // 容忍 ```json 围栏
  const fence = text.match(/```(?:json)?\s*([\s\S]*?)```/);
  if (fence) text = fence[1].trim();
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch (error) {
    return { ok: false, error: `invalid JSON: ${error instanceof Error ? error.message : String(error)}` };
  }
  if (!parsed || typeof parsed !== 'object') return { ok: false, error: 'output must be a JSON object' };
  if (parsed.experience_id !== expectedExperienceId) {
    return { ok: false, error: `experience_id mismatch: ${parsed.experience_id}` };
  }
  if (!ALLOWED_DECISIONS.has(parsed.decision)) {
    return { ok: false, error: `invalid decision: ${parsed.decision}` };
  }
  if (typeof parsed.reason !== 'string' || parsed.reason.trim() === '') {
    return { ok: false, error: 'missing reason' };
  }
  return { ok: true, decision: parsed.decision, reason: parsed.reason.trim().slice(0, 500) };
}

/**
 * 调模型拿结论。ctx.llm.stream 是宿主唯一受支持的模型调用路径。
 * 返回 { text, usage, elapsedMs }；usage 为宿主返回的用量对象（字段因适配器而异）。
 */
async function evaluateWithModel(ctx, modelRoute, messages, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  const started = Date.now();
  let stream;
  try {
    // 用 cordis 的 ctx.get('llm') 读取服务，不依赖 inject 声明。
    const llm = ctx && typeof ctx.get === 'function' ? ctx.get('llm') : ctx?.llm;
    if (!llm || typeof llm.stream !== 'function') {
      throw new Error('llm service unavailable');
    }
    stream = llm.stream({
      provider: modelRoute.provider,
      model: modelRoute.model,
      messages,
      signal: controller.signal,
    });
  } catch (error) {
    clearTimeout(timer);
    throw new Error(`llm service unavailable: ${error instanceof Error ? error.message : String(error)}`);
  }
  let text = '';
  let usage = null;
  try {
    for await (const chunk of stream) {
      if (!chunk || typeof chunk !== 'object') continue;
      if (chunk.type === 'text-delta' && typeof chunk.text === 'string') text += chunk.text;
      else if (chunk.type === 'usage') usage = chunk;
      else if (chunk.type === 'finish' && chunk.usage) usage = chunk.usage;
    }
    return { text, usage, elapsedMs: Date.now() - started };
  } finally {
    clearTimeout(timer);
  }
}

function decisionRequestId(experienceId, expectedStatus) {
  // 同一经历 + 同一读取状态 = 同一评估请求；重跑幂等。
  return `verify_${createHash('sha1').update(`${experienceId}|${expectedStatus}`).digest('hex').slice(0, 20)}`;
}

/**
 * 跑一轮核验扫描。
 * @returns {{ scanned: number, verified: number, skipped: number, failed: number, details: Array }}
 */
export async function runVerificationSweep(ctx, engine, options = {}) {
  const log = options.log ?? (() => {});
  const trace = options.trace ?? (() => {});
  const limit = options.limit ?? 5;
  const modelRoute = options.modelRoute;
  const timeoutMs = options.timeoutMs ?? 60000;
  const result = { scanned: 0, verified: 0, skipped: 0, failed: 0, details: [] };

  if (!modelRoute || !modelRoute.provider || !modelRoute.model) {
    log('verify sweep skipped: evaluation model not configured');
    return result;
  }
  // 模型服务不可用时直接跳过整轮，不逐条空转。
  try {
    const llm = ctx && typeof ctx.get === 'function' ? ctx.get('llm') : ctx?.llm;
    if (!llm || typeof llm.stream !== 'function') {
      log('verify sweep skipped: llm service unavailable');
      return result;
    }
  } catch {
    log('verify sweep skipped: llm service unavailable');
    return result;
  }
  if (engine.state !== 'ready') {
    log(`verify sweep skipped: engine state=${engine.state}`);
    return result;
  }

  const pending = await engine.request('memory_list_pending', { limit }, { timeoutMs: 5000 });
  const items = pending.experiences ?? [];
  result.scanned = items.length;

  for (const item of items) {
    const expectedStatus = item.verification_status;
    try {
      // 完整经历 + 事件关联作为评估输入。
      const experience = await engine.request(
        'memory_get_experience',
        { experience_id: item.experience_id },
        { timeoutMs: 5000 },
      );

      // P7-B：可选读取原始用户消息（读取不复制，失败回退 description-only）。
      let evidence = null;
      if (typeof options.collectEvidence === 'function') {
        try {
          const collected = await options.collectEvidence(experience);
          if (collected && collected.ok) evidence = collected.evidence;
          else trace(`evidence skipped exp=${item.experience_id} reason=${collected?.reason ?? 'unknown'}`);
        } catch (error) {
          trace(`evidence failed exp=${item.experience_id} error=${error instanceof Error ? error.message : String(error)}`);
        }
      }

      const messages = buildEvaluationMessages(experience, { evidence });
      trace(
        `eval-input exp=${item.experience_id} status=${expectedStatus} events=${(experience.event_ids ?? []).join(',')} desc=${String(experience.description ?? '').slice(0, 160)} evidence=${evidence ? evidence.length : 0}`,
      );
      const { text: raw, usage, elapsedMs } = await evaluateWithModel(ctx, modelRoute, messages, timeoutMs);
      trace(
        `eval-output exp=${item.experience_id} ms=${elapsedMs} usage=${JSON.stringify(usage ?? null)} raw=${String(raw).slice(0, 400)}`,
      );
      const parsed = parseDecision(raw, item.experience_id);
      if (!parsed.ok) {
        result.skipped += 1;
        result.details.push({ experience_id: item.experience_id, outcome: 'invalid_output', error: parsed.error });
        trace(`eval-invalid exp=${item.experience_id} error=${parsed.error}`);
        log(`verify skipped ${item.experience_id}: ${parsed.error}`);
        continue;
      }

      const applied = await engine.request(
        'memory_verify_experience',
        {
          experience_id: item.experience_id,
          target_status: parsed.decision,
          reason: `[${PROMPT_VERSION}] ${parsed.reason}`,
          expected_status: expectedStatus,
        },
        { requestId: decisionRequestId(item.experience_id, expectedStatus), timeoutMs: 5000 },
      );
      result.verified += 1;
      result.details.push({
        experience_id: item.experience_id,
        outcome: 'applied',
        decision: parsed.decision,
        changed: applied.changed,
        already: applied.already ?? false,
      });
      trace(`eval-applied exp=${item.experience_id} decision=${parsed.decision} changed=${applied.changed} already=${applied.already ?? false} reason=${parsed.reason}`);
      log(`verify ${item.experience_id}: ${parsed.decision} (changed=${applied.changed})`);
    } catch (error) {
      result.failed += 1;
      const message = error instanceof Error ? error.message : String(error);
      result.details.push({ experience_id: item.experience_id, outcome: 'failed', error: message });
      log(`verify failed ${item.experience_id}: ${message}`);
      // 单条失败不影响其余记录。
    }
  }
  return result;
}
