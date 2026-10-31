/**
 * P10：关系证据的模型评估（Node 侧）。
 *
 * 职责：取关系接入队列 → 模型判断"经历涉及的关系对象 + 信任信号 + 证据强度" → 调关系模块。
 * 模型只输出候选：不决定权重、不决定身份合法性（身份由关系模块校验）。
 */

import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const PLUGIN_ROOT = join(fileURLToPath(import.meta.url), '..');
const ALLOWED_STRENGTHS = new Set(['weak', 'medium', 'strong', 'critical']);
const ALLOWED_IDENTITY_TYPES = new Set(['counterpart']);
const STABLE_IDENTITY_KEY = /^[a-z][a-z0-9_]{1,31}:[A-Za-z0-9_.@-]{1,128}$/;
export const PROMPT_VERSION = 'p10-relationship-v1';

const FALLBACK_PROMPT = '判断经历涉及的关系对象与信任影响，输出 JSON。';

function readPrompt() {
  try {
    return readFileSync(join(PLUGIN_ROOT, 'prompts', 'relationship-eval.md'), 'utf8');
  } catch {
    return FALLBACK_PROMPT;
  }
}

export function buildRelationshipMessages(experience) {
  const system = readPrompt();
  const payload = {
    experience: {
      experience_id: experience.experience_id,
      experience_type: experience.experience_type,
      description: experience.description,
      occurred_at: experience.occurred_at,
    },
    constraints: {
      principal: '当前用户（dsh_local:local_principal）是关系主体，不是 Counterpart',
      allowed_identity_types: ['counterpart'],
      allowed_strengths: ['weak', 'medium', 'strong', 'critical'],
      trust_signal_range: [-1, 1],
    },
  };
  return [
    { role: 'system', content: [{ type: 'text', text: system }] },
    { role: 'user', content: [{ type: 'text', text: JSON.stringify(payload, null, 2) }] },
  ];
}

/** 严格解析关系评估输出。 */
export function parseRelationshipDecision(rawText) {
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

  const signal = Number(parsed.trust_signal);
  if (!Number.isFinite(signal) || signal < -1 || signal > 1) {
    return { ok: false, error: `invalid trust_signal: ${parsed.trust_signal}` };
  }
  if (!ALLOWED_STRENGTHS.has(parsed.evidence_strength)) {
    return { ok: false, error: `invalid evidence_strength: ${parsed.evidence_strength}` };
  }

  let counterpart = null;
  if (parsed.counterpart && typeof parsed.counterpart === 'object') {
    const identityType = parsed.counterpart.identity_type;
    const identityKey = parsed.counterpart.identity_key;
    if (!ALLOWED_IDENTITY_TYPES.has(identityType)) {
      return { ok: false, error: `invalid identity_type: ${identityType}` };
    }
    if (typeof identityKey !== 'string' || !STABLE_IDENTITY_KEY.test(identityKey.trim())) {
      // 身份键必须是稳定标识（namespace:key），display_name 不得充当身份键。
      return { ok: false, error: `unstable identity_key (need namespace:key): ${identityKey}` };
    }
    counterpart = {
      identity_type: identityType,
      identity_key: identityKey.trim(),
      display_name: typeof parsed.counterpart.display_name === 'string'
        ? parsed.counterpart.display_name.slice(0, 120)
        : '',
    };
  }

  return {
    ok: true,
    decision: {
      counterpart,
      trust_signal: signal,
      evidence_strength: parsed.evidence_strength,
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
  return `rel_${createHash('sha1').update(evidenceId).digest('hex').slice(0, 20)}`;
}

/** 跑一轮关系证据评估。 */
export async function runRelationshipEval(ctx, engine, options = {}) {
  const log = options.log ?? (() => {});
  const trace = options.trace ?? (() => {});
  const limit = options.limit ?? 5;
  const modelRoute = options.modelRoute;
  const timeoutMs = options.timeoutMs ?? 60000;
  const result = { scanned: 0, submitted: 0, failed: 0, details: [] };

  if (!modelRoute || !modelRoute.provider || !modelRoute.model) {
    log('relationship eval skipped: model not configured');
    return result;
  }
  if (engine.state !== 'ready') {
    log(`relationship eval skipped: engine state=${engine.state}`);
    return result;
  }

  const pending = await engine.request('relationship_queue_pending', { limit }, { timeoutMs: 5000 });
  const items = pending.evidences ?? [];
  result.scanned = items.length;

  for (const item of items) {
    try {
      const experience = await engine.request(
        'memory_get_experience',
        { experience_id: item.canonical_experience_id },
        { timeoutMs: 5000 },
      );
      const messages = buildRelationshipMessages(experience);
      const raw = await callModel(ctx, modelRoute, messages, timeoutMs);
      const parsed = parseRelationshipDecision(raw);
      if (!parsed.ok) {
        await engine.request(
          'relationship_queue_fail',
          { evidence_id: item.evidence_id, error: parsed.error },
          { timeoutMs: 5000 },
        );
        result.failed += 1;
        result.details.push({ evidence_id: item.evidence_id, outcome: 'invalid_output', error: parsed.error });
        trace(`relationship-eval invalid evidence=${item.evidence_id} error=${parsed.error}`);
        continue;
      }

      const applied = await engine.request(
        'relationship_submit_from_eval',
        {
          evidence_id: item.evidence_id,
          counterpart: parsed.decision.counterpart,
          trust_signal: parsed.decision.trust_signal,
          evidence_strength: parsed.decision.evidence_strength,
          reason: `[${PROMPT_VERSION}] ${parsed.decision.reason}`,
        },
        { requestId: submitRequestId(item.evidence_id), timeoutMs: 5000 },
      );
      if (applied.skipped) {
        result.details.push({ evidence_id: item.evidence_id, outcome: 'skipped', reason: applied.reason });
        trace(`relationship-eval skipped evidence=${item.evidence_id} reason=${applied.reason}`);
      } else {
        result.submitted += 1;
        result.details.push({ evidence_id: item.evidence_id, outcome: 'submitted' });
        trace(`relationship-eval submitted evidence=${item.evidence_id} strength=${parsed.decision.evidence_strength} signal=${parsed.decision.trust_signal}`);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      await engine
        .request('relationship_queue_fail', { evidence_id: item.evidence_id, error: message }, { timeoutMs: 5000 })
        .catch(() => {});
      result.failed += 1;
      result.details.push({ evidence_id: item.evidence_id, outcome: 'failed', error: message });
      trace(`relationship-eval failed evidence=${item.evidence_id} error=${message}`);
      log(`relationship eval failed ${item.evidence_id}: ${message}`);
    }
  }
  return result;
}
