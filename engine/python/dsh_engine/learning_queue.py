"""P11：学习证据接入层队列（系统集成层，不属于学习模块契约）。"""

import time

ACCESS_MODULE = "learning-access"
MAX_ATTEMPTS = 3
BACKOFF_MS = [30_000, 120_000, 600_000]


class LearningQueue:
    def __init__(self, db, audit):
        self.db = db
        self.audit = audit

    def upsert_from_event(self, conn, *, workspace_id, canonical_experience_id, source_experience_id, source_status):
        if source_status not in ("uncertain", "verified"):
            raise ValueError("source_status must be uncertain/verified")
        now = int(time.time() * 1000)
        row = conn.execute(
            "SELECT * FROM learning_evidence_queue WHERE workspace_id = ? AND canonical_experience_id = ?",
            (workspace_id, canonical_experience_id),
        ).fetchone()
        if row is None:
            queue_status = "pending_eval" if source_status == "verified" else "awaiting_verification"
            evidence_id = "learn:%s:%s" % (workspace_id, canonical_experience_id)
            conn.execute(
                "INSERT INTO learning_evidence_queue(evidence_id, workspace_id, canonical_experience_id,"
                " source_experience_id, source_status, queue_status, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (evidence_id, workspace_id, canonical_experience_id, source_experience_id, source_status,
                 queue_status, now, now),
            )
            self.audit.write(
                conn, ACCESS_MODULE, "learning_queue.enqueue", "learning_evidence", evidence_id,
                after={"queue_status": queue_status, "source_status": source_status},
            )
            return {"action": "enqueued", "queue_status": queue_status}

        current = row["queue_status"]
        if source_status == "verified" and current == "awaiting_verification":
            conn.execute(
                "UPDATE learning_evidence_queue SET source_status = 'verified',"
                " queue_status = 'pending_eval', source_experience_id = ?, updated_at = ?"
                " WHERE evidence_id = ?",
                (source_experience_id, now, row["evidence_id"]),
            )
            self.audit.write(
                conn, ACCESS_MODULE, "learning_queue.promote", "learning_evidence", row["evidence_id"],
                before={"queue_status": current}, after={"queue_status": "pending_eval"},
            )
            return {"action": "promoted", "queue_status": "pending_eval"}

        return {"action": "unchanged", "queue_status": current}

    def pending_eval(self, limit=5, now_ms=None):
        now = int(now_ms if now_ms is not None else time.time() * 1000)
        rows = self.db.conn.execute(
            "SELECT * FROM learning_evidence_queue"
            " WHERE queue_status = 'pending_eval'"
            "    OR (queue_status = 'retry_wait' AND (next_retry_at IS NULL OR next_retry_at <= ?))"
            " ORDER BY created_at LIMIT ?",
            (now, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    def mark_evaluated(self, evidence_id, proposal_ref=None):
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE learning_evidence_queue SET queue_status = 'evaluated', proposal_ref = ?,"
                " last_error = NULL, next_retry_at = NULL, updated_at = ? WHERE evidence_id = ?",
                (proposal_ref, int(time.time() * 1000), evidence_id),
            )
            self.audit.write(
                conn, ACCESS_MODULE, "learning_queue.evaluated", "learning_evidence", evidence_id,
                after={"proposal_ref": proposal_ref},
            )

    def mark_failed(self, evidence_id, error, now_ms=None):
        now = int(now_ms if now_ms is not None else time.time() * 1000)
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT attempts FROM learning_evidence_queue WHERE evidence_id = ?", (evidence_id,)
            ).fetchone()
            if row is None:
                return None
            attempts = int(row["attempts"]) + 1
            if attempts >= MAX_ATTEMPTS:
                conn.execute(
                    "UPDATE learning_evidence_queue SET queue_status = 'dead', attempts = ?,"
                    " last_error = ?, next_retry_at = NULL, updated_at = ? WHERE evidence_id = ?",
                    (attempts, str(error), now, evidence_id),
                )
                status = "dead"
            else:
                backoff = BACKOFF_MS[min(attempts - 1, len(BACKOFF_MS) - 1)]
                conn.execute(
                    "UPDATE learning_evidence_queue SET queue_status = 'retry_wait', attempts = ?,"
                    " last_error = ?, next_retry_at = ?, updated_at = ? WHERE evidence_id = ?",
                    (attempts, str(error), now + backoff, now, evidence_id),
                )
                status = "retry_wait"
            self.audit.write(
                conn, ACCESS_MODULE, "learning_queue.failed", "learning_evidence", evidence_id,
                after={"queue_status": status, "attempts": attempts, "error": str(error)},
            )
            return status

    def get(self, evidence_id):
        row = self.db.conn.execute(
            "SELECT * FROM learning_evidence_queue WHERE evidence_id = ?", (evidence_id,)
        ).fetchone()
        return dict(row) if row else None

    def pending_for_workspace(self, workspace_id, limit=20):
        rows = self.db.conn.execute(
            "SELECT * FROM learning_evidence_queue WHERE workspace_id = ? ORDER BY created_at LIMIT ?",
            (workspace_id, limit),
        ).fetchall()
        return [dict(row) for row in rows]
