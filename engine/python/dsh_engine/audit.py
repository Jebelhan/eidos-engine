"""统一审计。只增不改：应用层不提供更新/删除接口，数据库触发器兜底。"""

import json
import time
import uuid


class AuditLog:
    def __init__(self, db):
        self.db = db

    def write(
        self,
        conn,
        module_id,
        operation,
        target_type,
        target_id,
        *,
        before=None,
        after=None,
        source_refs=None,
        request_id=None,
        logical_version=None,
    ):
        """写入一条审计。调用方负责把它放进自己的业务事务。"""
        audit_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO audit_records"
            " (audit_id, module_id, operation, target_type, target_id,"
            "  before_snapshot, after_snapshot, source_refs, request_id, logical_version, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                audit_id,
                module_id,
                operation,
                target_type,
                str(target_id),
                None if before is None else json.dumps(before, ensure_ascii=False, sort_keys=True),
                None if after is None else json.dumps(after, ensure_ascii=False, sort_keys=True),
                None if source_refs is None else json.dumps(source_refs, ensure_ascii=False),
                request_id,
                logical_version,
                int(time.time() * 1000),
            ),
        )
        return audit_id

    def query(self, *, target_type=None, target_id=None, module_id=None, limit=50):
        clauses = []
        params = []
        if target_type is not None:
            clauses.append("target_type = ?")
            params.append(target_type)
        if target_id is not None:
            clauses.append("target_id = ?")
            params.append(str(target_id))
        if module_id is not None:
            clauses.append("module_id = ?")
            params.append(module_id)
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = self.db.conn.execute(
            "SELECT * FROM audit_records" + where + " ORDER BY created_at DESC LIMIT ?",
            (*params, limit),
        ).fetchall()
        return [dict(row) for row in rows]
