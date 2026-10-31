/**
 * P11：学习证据的模型评估（Node 侧）。
 *
 * 职责：取学习队列 → 模型判断经历支持/反驳哪个已有结论，或提出候选结论 → 调学习模块。
 * 模型不写库、不决定结论状态、不生成建议。
 */

import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';

const PLUGIN_ROOT = join(fileURLToPath(import.meta.url), '..');
const ALLOWED_POLARITY = new Set(['support', 'against']);
export const PROMPT_VERSION = 'p11-learning-v1';

const FALLBACK_PROMPT = '判断经历支持或反驳哪个结论，输出 JSON。';

function readPrompt() {
  try {
    return readFileSync(join(PLUGIN_ROOT, 'prompts', 'learning-eval.md'), 'utf8');
  } catch {
    return FALLBACK_PROMPT;
  }
}

export function buildLearningMessages(experience, conclusions) {
  const system = readPrompt();
  const payload = {
    experience: {
      experience_id: experience.experience_id,
      experience_type: experience.experience_type,
      description: experience.description,
      occurred_at: experience.occurred_at,
    },
    conclusions: (conclusions ?? []).map((c) => ({
      conclusion_id: c.conclusion_id,
      statement: c.statement,
      status: c.status,
    })),
    constraints: {
      allowed_polarity: ['support', 'against'],
      note: '只能匹配已有结论或提出新候选；不得编造 conclusion_id。',
    },
  };
  return [
    { role: 'system', content: [{ type: 'text', text: system }] },
    { role: 'user', content: [{ type: 'text', text: JSON.stringify(payload, null, 2) }] },
  ];
}

export function parseLearningDecision(rawText, knownConclusionIds = new Set()) {
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

  const conclusionId = typeof parsed.conclusion_id === 'string' ? parsed.conclusion_id.trim() : '';
  const statement = typeof parsed.statement === 'string' ? parsed.statement.trim() : '';
  const polarity = parsed.polarity;
  if (!ALLOWED_POLARITY.has(polarity)) return { ok: false, error: `invalid polarity: ${polarity}` };
  if (conclusionId !== '' && !knownConclusionIds.has(conclusionId)) {
    return { ok: false, error: `unknown conclusion_id: ${conclusionId}` };
  }
  if (conclusionId === '' && statement === '') {
    return { ok: false, error: 'missing conclusion_id and statement' };
  }
  return {
    ok: true,
    decision: {
      conclusion_id: conclusionId || null,
      statement,
      polarity,
      rationale: typeof parsed.rationale === 'string' ? parsed.rationale.slice(0, 300) : '',
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
  return `learn_${createHash('sha1').update(evidenceId).digest('hex').slice(0, 20)}`;
}

export async function runLearningEval(ctx, engine, options = {}) {
  const log = options.log ?? (() => {});
  const trace = options.trace ?? (() => {});
  const limit = options.limit ?? 5;
  const modelRoute = options.modelRoute;
  const timeoutMs = options.timeoutMs ?? 60000;
  const result = { scanned: 0, submitted: 0, proposed: 0, failed: 0, details: [] };

  if (!modelRoute || !modelRoute.provider || !modelRoute.model) {
    log('learning eval skipped: model not configured');
    return result;
  }
  if (engine.state !== 'ready') {
    log(`learning eval skipped: engine state=${engine.state}`);
    return result;
  }

  const pending = await engine.request('learning_queue_pending', { limit }, { timeoutMs: 5000 });
  const items = pending.evidences ?? [];
  result.scanned = items.length;

  for (const item of items) {
    try {
      const experience = await engine.request(
        'memory_get_experience',
        { experience_id: item.canonical_experience_id },
        { timeoutMs: 5000 },
      );
      const list = await engine.request(
        'learning_conclusions_list',
        { workspace_id: item.workspace_id, limit: 20 },
        { timeoutMs: 5000 },
      );
      const conclusions = list.conclusions ?? [];
      const known = new Set(conclusions.map((c) => c.conclusion_id));
      const messages = buildLearningMessages(experience, conclusions);
      const raw = await callModel(ctx, modelRoute, messages, timeoutMs);
      const parsed = parseLearningDecision(raw, known);
      if (!parsed.ok) {
        await engine.request(
          'learning_queue_fail',
          { evidence_id: item.evidence_id, error: parsed.error },
          { timeoutMs: 5000 },
        );
        result.failed += 1;
        result.details.push({ evidence_id: item.evidence_id, outcome: 'invalid_output', error: parsed.error });
        trace(`learning-eval invalid evidence=${item.evidence_id} error=${parsed.error}`);
        continue;
      }

      const applied = await engine.request(
        'learning_submit_evidence',
        {
          evidence_id: item.evidence_id,
          conclusion_id: parsed.decision.conclusion_id,
          statement: parsed.decision.statement,
          polarity: parsed.decision.polarity,
          rationale: `[${PROMPT_VERSION}] ${parsed.decision.rationale}`,
        },
        { requestId: submitRequestId(item.evidence_id), timeoutMs: 5000 },
      );
      if (applied.proposed) {
        result.proposed += 1;
        result.details.push({ evidence_id: item.evidence_id, outcome: 'proposed' });
        trace(`learning-eval proposed evidence=${item.evidence_id} proposal=${applied.proposal_id}`);
      } else {
        result.submitted += 1;
        result.details.push({ evidence_id: item.evidence_id, outcome: 'submitted' });
        trace(`learning-eval submitted evidence=${item.evidence_id} conclusion=${parsed.decision.conclusion_id} polarity=${parsed.decision.polarity}`);
      }
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      await engine
        .request('learning_queue_fail', { evidence_id: item.evidence_id, error: message }, { timeoutMs: 5000 })
        .catch(() => {});
      result.failed += 1;
      result.details.push({ evidence_id: item.evidence_id, outcome: 'failed', error: message });
      trace(`learning-eval failed evidence=${item.evidence_id} error=${message}`);
      log(`learning eval failed ${item.evidence_id}: ${message}`);
    }
  }
  return result;
}
