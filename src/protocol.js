/**
 * NDJSON 行协议。一行一条 JSON 消息，换行符是边界。
 * 只做编解码与缓冲，不关心业务。
 */

const MAX_LINE_BYTES = 8 * 1024 * 1024;

/** 把消息编码为一行（自动补换行）。 */
export function encodeMessage(message) {
  const line = JSON.stringify(message);
  if (Buffer.byteLength(line, 'utf8') > MAX_LINE_BYTES) {
    throw new Error(`NDJSON message too large: ${Buffer.byteLength(line, 'utf8')} bytes`);
  }
  return line + '\n';
}

/**
 * 行分帧器：push(chunk) 返回本次可解析的消息数组。
 * 非法 JSON 不抛出，作为 error 条目返回，调用方决定记录还是丢弃。
 */
export class LineDecoder {
  constructor() {
    this.buffer = '';
  }

  push(chunk) {
    this.buffer += chunk;
    const out = [];
    let idx;
    while ((idx = this.buffer.indexOf('\n')) >= 0) {
      const line = this.buffer.slice(0, idx).replace(/\r$/, '');
      this.buffer = this.buffer.slice(idx + 1);
      if (line.trim() === '') continue;
      try {
        out.push({ ok: true, message: JSON.parse(line) });
      } catch (error) {
        out.push({ ok: false, line, error: error instanceof Error ? error.message : String(error) });
      }
    }
    return out;
  }

  flush() {
    const rest = this.buffer.trim();
    this.buffer = '';
    if (rest === '') return [];
    try {
      return [{ ok: true, message: JSON.parse(rest) }];
    } catch (error) {
      return [{ ok: false, line: rest, error: error instanceof Error ? error.message : String(error) }];
    }
  }
}

let counter = 0;
/** 生成稳定的请求 ID（同一进程内单调递增，避免依赖随机数）。 */
export function nextRequestId(prefix = 'req') {
  counter += 1;
  return `${prefix}_${Date.now().toString(36)}_${counter.toString(36)}`;
}
