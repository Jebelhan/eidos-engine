/**
 * P9：情绪证据的模型评估（Node 侧）。
 *
 * 职责：取接入层队列 → 调模型输出四分类 + confidence + PAD 候选 → 调 emotion_submit。
 * Python 只负责队列与情绪事件存储；模型调用不进入 Python。
 */

import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const PLUGIN_ROOT = join(fileURLToPath(import.meta.url), '..');
const ALLOWED = new Set(['positive', 'negative', 'mixed', 'neutral']);
export const PROMPT_VERSION = 'p9-emotion-v1';

const FALLBACK_PROMPT = '判断一段经历对当时情绪的影响，输出 JSON。';

function readPrompt() {
  try {
    return readFileSync(join(PLUGIN_ROOT, 'prompts', 'emotion-eval.md'), 'utf8');
  } catch {
    return FALLBACK_PROMPT;
  }
}

export function buildEmotionMessages(experience) {
  const system = readPrompt();
  const payload = {
    experience: {
      experience_id: experience.experience_id,
      experience_type: experience.experience_type,
      description: experience.description,
      occurred_at: experience.occurred_at,
    },
    constraints: {
      allowed_event_types: ['positive', 'negative', 'mixed', 'neutral'],
      pad_range: 'P、A、D 各取 -1 到 1',
    },
  };
  return [
    { role: 'system', content: [{ type: 'text', text: system }] },
    { role: 'user', content: [{ type: 'text', text: JSON.stringify(payload, null, 2) }] },
  ];
}

/** 严格解析情绪评估输出。 */
export function parseEmotionDecision(rawText) {
  if (typeof rawText !== 'string' || rawText.trim() === '') return { ok: false, error: 'empty output' };
  let text = rawText.trim();
  const fence = text.match(/```(?:json)?\s*([\s\S]*?)```/);
  if (fence) text = fence[1].trim();
  let parsed;
  try {
    parsed = JSON.parse(text);
  } catch (error) {
    return { ok: false, error: `invalid JSON: ${error instanceof Error ? error.message : String(error)}` };
  }
  if (!parsed || typeof parsed !== 'object') return { ok: false, error: 'output must be object' };
  if (!ALLOWED.has(parsed.event_type)) return { ok: false, error: `invalid event_type: ${parsed.event_type}` };
  const confidence = Number(parsed.confidence);
  if (!Number.isFinite(confidence) || confidence < 0 || confidence > 1) {
    return { ok: false, error: `invalid confidence: ${parsed.confidence}` };
  }
  const pad = parsed.emotion_candidates;
  const candidates = {};
  if (pad && typeof pad === 'object') {
    for (const axis of ['p', 'a', 'd']) {
      const value = Number(pad[axis]);
      if (Number.isFinite(value)) candidates[axis] = Math.max(-1, Math.min(1, value));
    }
  }
  return {
    ok: true,
    decision: {
      event_type: parsed.event_type,
      confidence,
      emotion_candidates: Object.keys(candidates).length > 0 ? candidates : null,
      reason: typeof parsed.reason === 'string' ? parsed.reason.slice(0, 300) : '',
    },
  };
}

async function callModel(ctx, modelRoute, messages, timeoutMs) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const llm = ctx && typeof ctx.get === 'function' ? ctx.get('llm') : ctx?.llm;
    if (!llm || typeof llm.stream !== 'function') throw new Error('llm service unavailable');
    let text = '';
    for await (const chunk of llm.stream({
      provider: modelRoute.provider,
      model: modelRoute.model,
      messages,
      signal: controller.signal,
    })) {
      if (chunk && chunk.type === 'text-delta' && typeof chunk.text === 'string') text += chunk.text;
    }
    return text;
  } finally {
    clearTimeout(timer);
  }
}

function submitRequestId(evidenceId) {
  return `emo_${createHash('sha1').update(evidenceId).digest('hex').slice(0, 20)}`;
}

/**
 * 跑一轮情绪证据评估。
 * @returns {{ scanned: number, submitted: number, failed: number, details: Array }}
 */
export async function runEmotionEval(ctx, engine, options = {}) {
  const log = options.log ?? (() => {});
  const trace = options.trace ?? (() => {});
  const limit = options.limit ?? 5;
  const modelRoute = options.modelRoute;
  const timeoutMs = options.timeoutMs ?? 60000;
  const result = { scanned: 0, submitted: 0, failed: 0, details: [] };

  if (!modelRoute || !modelRoute.provider || !modelRoute.model) {
    log('emotion eval skipped: model not configured');
    return result;
  }
  if (engine.state !== 'ready') {
    log(`emotion eval skipped: engine state=${engine.state}`);
    return result;
  }

  const pending = await engine.request('emotion_queue_pending', { limit }, { timeoutMs: 5000 });
  const items = pending.evidences ?? [];
  result.scanned = items.length;

  for (const item of items) {
    try {
      const experience = await engine.request(
        'memory_get_experience',
        { experience_id: item.canonical_experience_id },
        { timeoutMs: 5000 },
      );
      const messages = buildEmotionMessages(experience);
      const raw = await callModel(ctx, modelRoute, messages, timeoutMs);
      const parsed = parseEmotionDecision(raw);
      if (!parsed.ok) {
        await engine.request(
          'emotion_queue_fail',
          { evidence_id: item.evidence_id, error: parsed.error },
          { timeoutMs: 5000 },
        );
        result.failed += 1;
        result.details.push({ evidence_id: item.evidence_id, outcome: 'invalid_output', error: parsed.error });
        trace(`emotion-eval invalid evidence=${item.evidence_id} error=${parsed.error}`);
        continue;
      }

      await engine.request(
        'emotion_submit',
        {
          evidence_id: item.evidence_id,
          event_type: parsed.decision.event_type,
          confidence: parsed.decision.confidence,
          emotion_candidates: parsed.decision.emotion_candidates,
          description: String(experience.description ?? '').slice(0, 200),
          reason: `[${PROMPT_VERSION}] ${parsed.decision.reason}`,
        },
        { requestId: submitRequestId(item.evidence_id), timeoutMs: 5000 },
      );
      result.submitted += 1;
      result.details.push({ evidence_id: item.evidence_id, outcome: 'submitted', event_type: parsed.decision.event_type });
      trace(`emotion-eval submitted evidence=${item.evidence_id} type=${parsed.decision.event_type} conf=${parsed.decision.confidence}`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      await engine
        .request('emotion_queue_fail', { evidence_id: item.evidence_id, error: message }, { timeoutMs: 5000 })
        .catch(() => {});
      result.failed += 1;
      result.details.push({ evidence_id: item.evidence_id, outcome: 'failed', error: message });
      trace(`emotion-eval failed evidence=${item.evidence_id} error=${message}`);
      log(`emotion eval failed ${item.evidence_id}: ${message}`);
    }
  }
  return result;
}
