"""E4-C1：Experience 变更消费基础设施（操作级状态 + 待复核队列）。

职责：
- 操作级幂等：UNIQUE(operation_id, consumer_module) + mapping_version 校验；
- 待复核队列：证据引用 + 原因 + 状态，不复制业务正文；
- 状态变更写统一 Audit。

不执行业务证据迁移（由各模块内部操作完成）。
"""

import json
import time
import uuid

MODULE_ID = "experience-consumer"
REVIEW_REASONS = (
    "split_without_event_trace",
    "ambiguous_event_mapping",
    "multi_event_cross_split",
    "invalid_experience_reference",
    "canonical_resolution_error",
    "revised_evidence_uncertain",
    "consumer_validation_failed",
)
REVIEW_STATUSES = ("pending", "processing", "resolved", "rejected", "ignored")
STALE_PROCESSING_MS = 600_000  # 10 分钟：processing 超过此时长视为崩溃残留


class ExperienceChangeConsumer:
    def __init__(self, db, audit):
        self.db = db
        self.audit = audit

    # ---------- 操作级幂等 ----------

    def begin(self, *, operation_id, consumer_module, operation_type, mapping_version, conn=None):
        """开始一次操作级消费；已完成且版本一致 → skip；版本冲突 → conflict。"""
        if conn is not None:
            return self._begin(conn, operation_id=operation_id, consumer_module=consumer_module,
                               operation_type=operation_type, mapping_version=mapping_version)
        with self.db.transaction() as tx:
            return self._begin(tx, operation_id=operation_id, consumer_module=consumer_module,
                               operation_type=operation_type, mapping_version=mapping_version)

    def _begin(self, conn, *, operation_id, consumer_module, operation_type, mapping_version):
        now = int(time.time() * 1000)
        if True:
            row = conn.execute(
                "SELECT * FROM experience_change_consumer_operations"
                " WHERE operation_id = ? AND consumer_module = ?",
                (operation_id, consumer_module),
            ).fetchone()
            if row is not None:
                if int(row["mapping_version"]) != int(mapping_version):
                    return {"skip": False, "conflict": True,
                            "reason": "mapping_version_mismatch",
                            "current_version": int(row["mapping_version"])}
                if row["consumer_status"] == "completed":
                    return {"skip": True, "reason": "already_completed"}
                stale = (
                    row["consumer_status"] == "processing"
                    and row["started_at"] is not None
                    and now - int(row["started_at"]) > STALE_PROCESSING_MS
                )
                if stale:
                    self.audit.write(
                        conn, MODULE_ID, "experience_change.stale_recovery",
                        "experience_change_consumer_operation",
                        "%s:%s" % (operation_id, consumer_module),
                        after={"operation_id": operation_id, "consumer_module": consumer_module,
                               "attempt": int(row["attempt_count"]) + 1},
                    )
                conn.execute(
                    "UPDATE experience_change_consumer_operations SET consumer_status = 'processing',"
                    " attempt_count = attempt_count + 1, last_error = NULL, started_at = COALESCE(started_at, ?)"
                    " WHERE operation_id = ? AND consumer_module = ?",
                    (now, operation_id, consumer_module),
                )
                return {"skip": False, "attempt": int(row["attempt_count"]) + 1}
            conn.execute(
                "INSERT INTO experience_change_consumer_operations(operation_id, consumer_module,"
                " operation_type, mapping_version, consumer_status, started_at, attempt_count)"
                " VALUES (?, ?, ?, ?, 'processing', ?, 1)",
                (operation_id, consumer_module, operation_type, int(mapping_version), now),
            )
            return {"skip": False, "attempt": 1}

    def complete(self, *, operation_id, consumer_module, result=None, conn=None):
        if conn is not None:
            return self._complete(conn, operation_id=operation_id, consumer_module=consumer_module,
                                  result=result)
        with self.db.transaction() as tx:
            return self._complete(tx, operation_id=operation_id, consumer_module=consumer_module,
                                  result=result)

    def _complete(self, conn, *, operation_id, consumer_module, result=None):
        if True:
            conn.execute(
                "UPDATE experience_change_consumer_operations SET consumer_status = 'completed',"
                " completed_at = ?, last_error = NULL WHERE operation_id = ? AND consumer_module = ?",
                (int(time.time() * 1000), operation_id, consumer_module),
            )
            self.audit.write(
                conn, MODULE_ID, "experience_change.consumer_completed",
                "experience_change_consumer_operation",
                "%s:%s" % (operation_id, consumer_module),
                after={"operation_id": operation_id, "consumer_module": consumer_module,
                       "result": result},
            )
        return {"completed": True}

    def fail(self, *, operation_id, consumer_module, error, conn=None):
        if conn is not None:
            return self._fail(conn, operation_id=operation_id, consumer_module=consumer_module,
                              error=error)
        with self.db.transaction() as tx:
            return self._fail(tx, operation_id=operation_id, consumer_module=consumer_module,
                              error=error)

    def _fail(self, conn, *, operation_id, consumer_module, error):
        if True:
            conn.execute(
                "UPDATE experience_change_consumer_operations SET consumer_status = 'failed',"
                " last_error = ? WHERE operation_id = ? AND consumer_module = ?",
                (str(error)[:500], operation_id, consumer_module),
            )
            self.audit.write(
                conn, MODULE_ID, "experience_change.consumer_failed",
                "experience_change_consumer_operation",
                "%s:%s" % (operation_id, consumer_module),
                after={"operation_id": operation_id, "consumer_module": consumer_module,
                       "error": str(error)[:500]},
            )
        return {"failed": True}

    def operation_status(self, operation_id):
        rows = self.db.conn.execute(
            "SELECT * FROM experience_change_consumer_operations WHERE operation_id = ?",
            (operation_id,),
        ).fetchall()
        if not rows:
            return None
        now = int(time.time() * 1000)
        statuses = {}
        for row in rows:
            status = row["consumer_status"]
            if (status == "processing" and row["started_at"] is not None
                    and now - int(row["started_at"]) > STALE_PROCESSING_MS):
                status = "stale"
            statuses[row["consumer_module"]] = status
        if all(status == "completed" for status in statuses.values()):
            overall = "completed"
        elif any(status == "failed" for status in statuses.values()):
            overall = "partial"
        elif any(status == "processing" for status in statuses.values()):
            overall = "processing"
        else:
            overall = "pending"
        return {"operation_id": operation_id, "overall": overall, "consumers": statuses}

    # ---------- 待复核队列 ----------

    def enqueue_review(self, *, workspace_id, operation_id, consumer_module, evidence_ref,
                       reason, source_experience_id=None, target_experience_ids=None,
                       source_event_id=None, conn=None):
        if reason not in REVIEW_REASONS:
            raise ValueError("unknown review reason: %s" % reason)
        if conn is not None:
            return self._enqueue(conn, workspace_id=workspace_id, operation_id=operation_id,
                                 consumer_module=consumer_module, evidence_ref=evidence_ref,
                                 reason=reason, source_experience_id=source_experience_id,
                                 target_experience_ids=target_experience_ids,
                                 source_event_id=source_event_id)
        with self.db.transaction() as tx:
            return self._enqueue(tx, workspace_id=workspace_id, operation_id=operation_id,
                                 consumer_module=consumer_module, evidence_ref=evidence_ref,
                                 reason=reason, source_experience_id=source_experience_id,
                                 target_experience_ids=target_experience_ids,
                                 source_event_id=source_event_id)

    def _enqueue(self, conn, *, workspace_id, operation_id, consumer_module, evidence_ref,
                 reason, source_experience_id=None, target_experience_ids=None,
                 source_event_id=None):
        now = int(time.time() * 1000)
        if True:
            existing = conn.execute(
                "SELECT review_id, review_status FROM evidence_review_queue"
                " WHERE operation_id = ? AND consumer_module = ? AND evidence_ref = ?",
                (operation_id, consumer_module, evidence_ref),
            ).fetchone()
            if existing is not None:
                conn.execute(
                    "UPDATE evidence_review_queue SET updated_at = ? WHERE review_id = ?",
                    (now, existing["review_id"]),
                )
                return {"review_id": existing["review_id"], "deduplicated": True,
                        "review_status": existing["review_status"]}
            review_id = str(uuid.uuid4())
            conn.execute(
                "INSERT INTO evidence_review_queue(review_id, workspace_id, operation_id,"
                " consumer_module, evidence_ref, source_experience_id, target_experience_ids,"
                " source_event_id, review_reason, review_status, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
                (
                    review_id, workspace_id, operation_id, consumer_module, evidence_ref,
                    source_experience_id,
                    json.dumps(target_experience_ids, ensure_ascii=False) if target_experience_ids else None,
                    source_event_id, reason, now, now,
                ),
            )
            self.audit.write(
                conn, MODULE_ID, "evidence_review.enqueue", "evidence_review", review_id,
                after={"operation_id": operation_id, "consumer_module": consumer_module,
                       "evidence_ref": evidence_ref, "reason": reason},
            )
            return {"review_id": review_id, "deduplicated": False, "review_status": "pending"}

    def resolve_review(self, *, review_id, review_status, actor, reason, resolution_ref=None):
        if review_status not in REVIEW_STATUSES:
            raise ValueError("unknown review status: %s" % review_status)
        actor = (actor or "").strip()
        reason = (reason or "").strip()
        if not actor:
            raise ValueError("actor is required")
        if not reason:
            raise ValueError("reason is required")
        if review_status == "resolved" and not (resolution_ref or "").strip():
            raise ValueError("resolved requires resolution_ref")
        now = int(time.time() * 1000)
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM evidence_review_queue WHERE review_id = ?", (review_id,)
            ).fetchone()
            if row is None:
                raise KeyError("review not found: %s" % review_id)
            conn.execute(
                "UPDATE evidence_review_queue SET review_status = ?, updated_at = ?,"
                " resolved_at = ?, resolution_ref = ? WHERE review_id = ?",
                (review_status, now,
                 now if review_status in ("resolved", "rejected", "ignored") else None,
                 resolution_ref, review_id),
            )
            self.audit.write(
                conn, MODULE_ID, "evidence_review.%s" % review_status, "evidence_review",
                review_id,
                before={"review_status": row["review_status"]},
                after={"review_status": review_status, "actor": actor, "reason": reason,
                       "resolution_ref": resolution_ref},
            )
        return {"review_id": review_id, "review_status": review_status}

    def list_reviews(self, filters=None, limit=100):
        sql = "SELECT * FROM evidence_review_queue WHERE 1=1"
        params = []
        filters = filters or {}
        for field in ("review_status", "review_reason", "consumer_module", "workspace_id"):
            if filters.get(field):
                sql += " AND %s = ?" % field
                params.append(filters[field])
        sql += " ORDER BY created_at DESC LIMIT ?"
        params.append(int(limit))
        return [dict(row) for row in self.db.conn.execute(sql, params).fetchall()]
