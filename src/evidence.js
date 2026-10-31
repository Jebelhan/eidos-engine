/**
 * P7-B：原始事件读取与核验上下文组装。
 *
 * 原则：读取，不复制。
 * - 原始事件只从宿主 sessionPersistence 只读接口取；
 * - 不写入 SQLite，不写常规日志，trace 只记 ID/长度/结果；
 * - 只取事件关联的用户消息，过滤 assistant/tool/system；
 * - 读取前校验 workspace 归属，不匹配拒绝；
 * - 任何失败都回退 description-only（P6 模式），不改变 Experience 状态。
 */

const SENSITIVE_PATTERNS = [
  /-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----/g,
  /sk-[A-Za-z0-9]{16,}/g,
  /ghp_[A-Za-z0-9]{20,}/g,
  /AKIA[0-9A-Z]{16}/g,
  /Bearer\s+[A-Za-z0-9._-]{16,}/g,
  /(?:password|passwd|pwd)\s*[:=]\s*\S+/gi,
  /(?:api[_-]?key|token|secret)\s*[:=]\s*\S+/gi,
];

/** 过滤明显的凭证信息。 */
export function redactSecrets(text) {
  let out = String(text ?? '');
  for (const pattern of SENSITIVE_PATTERNS) out = out.replace(pattern, '[REDACTED]');
  return out;
}

function extractText(content) {
  if (!Array.isArray(content)) return '';
  return content
    .filter((block) => block && block.type === 'text' && typeof block.text === 'string')
    .map((block) => block.text)
    .join('\n');
}

/**
 * 读取一条 Experience 关联的原始用户消息。
 * @returns {{ ok: true, evidence: Array<{event_id:string,text:string,chars:number}>, missing: string[], totalChars: number }
 *          | { ok: false, reason: string }}
 */
export async function collectEvidence(ctx, experience, options = {}) {
  const trace = options.trace ?? (() => {});
  const maxPerItem = options.maxPerItem ?? 2000;
  const maxTotal = options.maxTotal ?? 8000;

  const eventIds = Array.isArray(experience?.event_ids) ? experience.event_ids : [];
  if (eventIds.length === 0) return { ok: false, reason: 'no-event-ids' };
  if (!experience.session_id) return { ok: false, reason: 'missing-session-id' };

  let persistence;
  try {
    persistence = ctx && typeof ctx.get === 'function' ? ctx.get('sessionPersistence') : undefined;
  } catch {
    persistence = undefined;
  }
  if (!persistence || typeof persistence.open !== 'function') {
    return { ok: false, reason: 'no-session-persistence' };
  }

  // workspace 归属校验：会话的 header.cwd 必须与 Experience 的 workspace_id 一致。
  let stat;
  try {
    stat = await persistence.stat(experience.session_id);
  } catch {
    return { ok: false, reason: 'session-not-found' };
  }
  const cwd = stat?.header?.cwd;
  if (cwd !== undefined && experience.workspace_id !== undefined && String(cwd) !== String(experience.workspace_id)) {
    return { ok: false, reason: 'workspace-mismatch' };
  }

  let handle;
  try {
    handle = await persistence.open(experience.session_id, 'read');
  } catch {
    return { ok: false, reason: 'open-failed' };
  }

  try {
    const { events } = await handle.read();
    const wanted = new Set(eventIds);
    const evidence = [];
    let totalChars = 0;
    for (const event of events ?? []) {
      if (!event || event.type !== 'user/message') continue;
      const message = event.data;
      if (!message || !wanted.has(message.id)) continue;
      const text = redactSecrets(extractText(message.content)).trim();
      if (text === '') continue;
      const chars = Math.min(text.length, maxPerItem);
      if (totalChars + chars > maxTotal) continue;
      evidence.push({ event_id: message.id, text: text.slice(0, chars), chars });
      totalChars += chars;
    }
    const foundIds = new Set(evidence.map((item) => item.event_id));
    const missing = [...wanted].filter((id) => !foundIds.has(id));
    trace(`evidence read events=${evidence.length} missing=${missing.length} chars=${totalChars}`);
    return { ok: true, evidence, missing, totalChars };
  } catch {
    return { ok: false, reason: 'read-failed' };
  } finally {
    try {
      await handle.close();
    } catch {
      /* 读取失败关闭异常不影响结果 */
    }
  }
}
