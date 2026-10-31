"""统一请求幂等（request_dedup）。

语义（系统集成层定稿）：
- 同一 (request_id, module_id) 的重复请求：payload 相同返回原结果，不同则冲突；
- 业务写入与幂等记录在同一事务内完成；
- 不同模块之间的 request_id 互不占用。
"""

import hashlib
import json
import time


class IdempotencyConflict(Exception):
    """同一 request_id 的 payload 发生变化。"""


def canonical_payload(payload):
    """稳定序列化：键排序、紧凑分隔符，保证同一 payload 得到同一 hash。"""
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def payload_hash(payload):
    return hashlib.sha256(canonical_payload(payload).encode("utf-8")).hexdigest()


class RequestDedup:
    def __init__(self, db):
        self.db = db

    def run(self, module_id, request_id, operation_type, payload, fn):
        """
        幂等执行 fn(conn)。fn 内的业务写入与幂等记录处于同一事务。
        - 已执行且参数相同：返回原结果，不执行 fn；
        - 已执行但参数不同：抛 IdempotencyConflict；
        - 未执行：执行 fn 并记录结果。
        """
        digest = payload_hash(payload)
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT payload_hash, request_status, result_reference FROM request_dedup"
                " WHERE request_id = ? AND module_id = ?",
                (request_id, module_id),
            ).fetchone()
            if row is not None:
                if row["payload_hash"] != digest:
                    raise IdempotencyConflict(
                        "request_id %s already used by module %s with different payload" % (request_id, module_id)
                    )
                return json.loads(row["result_reference"]) if row["result_reference"] else None

            result = fn(conn)
            now = int(time.time() * 1000)
            conn.execute(
                "INSERT INTO request_dedup"
                " (request_id, module_id, operation_type, payload_hash, request_status,"
                "  result_reference, created_at, completed_at)"
                " VALUES (?, ?, ?, ?, 'completed', ?, ?, ?)",
                (
                    request_id,
                    module_id,
                    operation_type,
                    digest,
                    json.dumps(result, ensure_ascii=False, sort_keys=True),
                    now,
                    now,
                ),
            )
            return result

    def get(self, module_id, request_id):
        row = self.db.conn.execute(
            "SELECT * FROM request_dedup WHERE request_id = ? AND module_id = ?",
            (request_id, module_id),
        ).fetchone()
        return dict(row) if row else None
