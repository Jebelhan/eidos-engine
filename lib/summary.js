/**
 * 摘要消息构造。
 *
 * 引擎返回文本摘要，这里包成模型可见的 user 消息。
 * 手工构造而非依赖宿主包：插件是 link 安装，从插件目录解析不到宿主 node_modules，
 * 结构取自 @deepseek-ai/dsh-llm 的 createUserMessage（{...input, id, role:'user'} + 冻结）。
 */

import { randomUUID } from 'node:crypto';

export const SUMMARY_SOURCE_KIND = 'dsh-engine';
export const SUMMARY_FORM = 'notice';
const SUMMARY_SUMMARY_MAX = 120;

export function boundSummary(summary) {
  const text = String(summary ?? '');
  return text.length <= SUMMARY_SUMMARY_MAX ? text : `${text.slice(0, SUMMARY_SUMMARY_MAX - 1)}…`;
}

function deepFreeze(value) {
  if (value && typeof value === 'object' && !Object.isFrozen(value)) {
    for (const key of Object.keys(value)) deepFreeze(value[key]);
    Object.freeze(value);
  }
  return value;
}

/** 构造一条注入用的 user 消息（结构对齐 dsh-llm 的 UserMessage）。 */
export function createSummaryUserMessage(text, meta = {}) {
  return deepFreeze(
    structuredClone({
      id: randomUUID(),
      role: 'user',
      content: [{ type: 'text', text: String(text) }],
      source: {
        kind: SUMMARY_SOURCE_KIND,
        form: SUMMARY_FORM,
        summary: boundSummary(meta.summary ?? 'DSH engine state summary'),
      },
    }),
  );
}
