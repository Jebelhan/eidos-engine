"""P10：关系接入层队列（系统集成层，不属于关系模块契约）。

状态机（与 P9 同构）：
    awaiting_verification → pending_eval（经历被 verified）
    pending_eval → retry_wait → evaluated / dead
"""

import json
import time

ACCESS_MODULE = "relationship-access"
MAX_ATTEMPTS = 3
BACKOFF_MS = [30_000, 120_000, 600_000]


class RelationshipQueue:
    def __init__(self, db, audit):
        self.db = db
        self.audit = audit

    def upsert_from_event(self, conn, *, workspace_id, canonical_experience_id, source_experience_id, source_status):
        if source_status not in ("uncertain", "verified"):
            raise ValueError("source_status must be uncertain/verified")
        now = int(time.time() * 1000)
        row = conn.execute(
            "SELECT * FROM relationship_pending_evidence WHERE workspace_id = ? AND canonical_experience_id = ?",
            (workspace_id, canonical_experience_id),
        ).fetchone()

        if row is None:
            queue_status = "pending_eval" if source_status == "verified" else "awaiting_verification"
            evidence_id = "rel:%s:%s" % (workspace_id, canonical_experience_id)
            conn.execute(
                "INSERT INTO relationship_pending_evidence"
                " (evidence_id, workspace_id, canonical_experience_id, source_experience_id,"
                "  source_status, queue_status, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (evidence_id, workspace_id, canonical_experience_id, source_experience_id, source_status, queue_status, now, now),
            )
            self.audit.write(
                conn, ACCESS_MODULE, "relationship_queue.enqueue", "relationship_evidence", evidence_id,
                after={"queue_status": queue_status, "source_status": source_status},
            )
            return {"action": "enqueued", "queue_status": queue_status}

        current = row["queue_status"]
        if source_status == "verified" and current == "awaiting_verification":
            conn.execute(
                "UPDATE relationship_pending_evidence SET source_status = 'verified',"
                " queue_status = 'pending_eval', source_experience_id = ?, updated_at = ?"
                " WHERE evidence_id = ?",
                (source_experience_id, now, row["evidence_id"]),
            )
            self.audit.write(
                conn, ACCESS_MODULE, "relationship_queue.promote", "relationship_evidence", row["evidence_id"],
                before={"queue_status": current}, after={"queue_status": "pending_eval"},
            )
            return {"action": "promoted", "queue_status": "pending_eval"}

        return {"action": "unchanged", "queue_status": current}

    def pending_eval(self, limit=5, now_ms=None):
        now = int(now_ms if now_ms is not None else time.time() * 1000)
        rows = self.db.conn.execute(
            "SELECT * FROM relationship_pending_evidence"
            " WHERE queue_status = 'pending_eval'"
            "    OR (queue_status = 'retry_wait' AND (next_retry_at IS NULL OR next_retry_at <= ?))"
            " ORDER BY created_at LIMIT ?",
            (now, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def mark_evaluated(self, evidence_id, evidence_ref, candidate=None):
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE relationship_pending_evidence SET queue_status = 'evaluated', evidence_ref = ?,"
                " candidate = ?, last_error = NULL, next_retry_at = NULL, updated_at = ? WHERE evidence_id = ?",
                (
                    evidence_ref,
                    json.dumps(candidate, ensure_ascii=False) if candidate is not None else None,
                    int(time.time() * 1000),
                    evidence_id,
                ),
            )
            self.audit.write(
                conn, ACCESS_MODULE, "relationship_queue.evaluated", "relationship_evidence", evidence_id,
                after={"evidence_ref": evidence_ref, "candidate": candidate},
            )

    def mark_failed(self, evidence_id, error, now_ms=None):
        now = int(now_ms if now_ms is not None else time.time() * 1000)
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT attempts FROM relationship_pending_evidence WHERE evidence_id = ?", (evidence_id,)
            ).fetchone()
            if row is None:
                return None
            attempts = int(row["attempts"]) + 1
            if attempts >= MAX_ATTEMPTS:
                conn.execute(
                    "UPDATE relationship_pending_evidence SET queue_status = 'dead', attempts = ?,"
                    " last_error = ?, next_retry_at = NULL, updated_at = ? WHERE evidence_id = ?",
                    (attempts, str(error), now, evidence_id),
                )
                status = "dead"
            else:
                backoff = BACKOFF_MS[min(attempts - 1, len(BACKOFF_MS) - 1)]
                conn.execute(
                    "UPDATE relationship_pending_evidence SET queue_status = 'retry_wait', attempts = ?,"
                    " last_error = ?, next_retry_at = ?, updated_at = ? WHERE evidence_id = ?",
                    (attempts, str(error), now + backoff, now, evidence_id),
                )
                status = "retry_wait"
            self.audit.write(
                conn, ACCESS_MODULE, "relationship_queue.failed", "relationship_evidence", evidence_id,
                after={"queue_status": status, "attempts": attempts, "error": str(error)},
            )
            return status

    def get(self, evidence_id):
        row = self.db.conn.execute(
            "SELECT * FROM relationship_pending_evidence WHERE evidence_id = ?", (evidence_id,)
        ).fetchone()
        return dict(row) if row else None

    def pending_for_workspace(self, workspace_id, limit=20):
        rows = self.db.conn.execute(
            "SELECT * FROM relationship_pending_evidence WHERE workspace_id = ? ORDER BY created_at LIMIT ?",
            (workspace_id, limit),
        ).fetchall()
        return [dict(row) for row in rows]
