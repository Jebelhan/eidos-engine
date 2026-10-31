"""P14-D2：每日整理任务账本（lifecycle 管理，不执行业务）。

状态机：pending → running → completed；running → failed → recovery_pending → running。
阶段幂等：稳定 request_id = daily_cycle:{cycle_id}:{step_name}（业务侧幂等由各模块保证）。
"""

import json
import time
import uuid

MODULE_ID = "daily-cycle"
STEP_ORDER = {
    "memory_curation": 1,
    "emotion_advance": 2,
    "learning_curation": 3,
    "learning_evaluation": 4,
    "personality_suggest_delivery": 5,
    "personality_settlement": 6,
    "reconciliation": 7,
    "audit_record": 8,
}
MAX_ATTEMPTS = 3  # 首次 1 次 + 重试 2 次


class DailyCycleService:
    def __init__(self, db, audit):
        self.db = db
        self.audit = audit

    def ensure_cycle(self, *, timezone, schedule_date, scheduled_at):
        """同一时区同一天只允许一个周期；重复请求返回现有周期。"""
        now = int(time.time() * 1000)
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM daily_cycle WHERE timezone = ? AND schedule_date = ?",
                (timezone, schedule_date),
            ).fetchone()
            if row is not None:
                return {"created": False, "cycle": dict(row)}
            cycle_id = str(uuid.uuid4())
            run_id = "%s:%s" % (timezone, schedule_date)
            conn.execute(
                "INSERT INTO daily_cycle(cycle_id, run_id, timezone, schedule_date, scheduled_at,"
                " status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)",
                (cycle_id, run_id, timezone, schedule_date, int(scheduled_at), now, now),
            )
            for step_name, order in STEP_ORDER.items():
                conn.execute(
                    "INSERT INTO daily_cycle_steps(cycle_id, step_name, step_order, status)"
                    " VALUES (?, ?, ?, 'pending')",
                    (cycle_id, step_name, order),
                )
            self.audit.write(
                conn, MODULE_ID, "daily_cycle.create", "daily_cycle", cycle_id,
                after={"run_id": run_id, "timezone": timezone, "schedule_date": schedule_date},
            )
            return {"created": True, "cycle": dict(conn.execute(
                "SELECT * FROM daily_cycle WHERE cycle_id = ?", (cycle_id,)
            ).fetchone())}

    def claim(self, cycle_id):
        """条件更新认领执行权（防并发）。"""
        with self.db.transaction() as conn:
            cursor = conn.execute(
                "UPDATE daily_cycle SET status = 'running', started_at = COALESCE(started_at, ?),"
                " updated_at = ? WHERE cycle_id = ? AND status IN ('pending', 'recovery_pending')",
                (int(time.time() * 1000), int(time.time() * 1000), cycle_id),
            )
            return cursor.rowcount == 1

    def step_begin(self, cycle_id, step_name):
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE daily_cycle_steps SET status = 'running', started_at = ?,"
                " attempts = attempts + 1 WHERE cycle_id = ? AND step_name = ?",
                (int(time.time() * 1000), cycle_id, step_name),
            )
            conn.execute(
                "UPDATE daily_cycle SET current_step = ?, updated_at = ? WHERE cycle_id = ?",
                (step_name, int(time.time() * 1000), cycle_id),
            )

    def step_complete(self, cycle_id, step_name, detail=None):
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE daily_cycle_steps SET status = 'completed', completed_at = ?, last_error = NULL"
                " WHERE cycle_id = ? AND step_name = ?",
                (int(time.time() * 1000), cycle_id, step_name),
            )
            if detail is not None:
                self.audit.write(
                    conn, MODULE_ID, "daily_cycle.step", "daily_cycle_step",
                    "%s:%s" % (cycle_id, step_name),
                    after={"status": "completed", "detail": detail},
                )

    def step_skip(self, cycle_id, step_name, reason):
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE daily_cycle_steps SET status = 'skipped', completed_at = ?, last_error = ?"
                " WHERE cycle_id = ? AND step_name = ?",
                (int(time.time() * 1000), reason, cycle_id, step_name),
            )

    def step_fail(self, cycle_id, step_name, error):
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT attempts FROM daily_cycle_steps WHERE cycle_id = ? AND step_name = ?",
                (cycle_id, step_name),
            ).fetchone()
            attempts = int(row["attempts"]) if row else 0
            conn.execute(
                "UPDATE daily_cycle_steps SET status = 'failed', last_error = ? WHERE cycle_id = ?"
                " AND step_name = ?",
                (str(error)[:500], cycle_id, step_name),
            )
            if attempts >= MAX_ATTEMPTS:
                conn.execute(
                    "UPDATE daily_cycle SET status = 'failed', last_error = ?, updated_at = ?"
                    " WHERE cycle_id = ?",
                    (str(error)[:500], int(time.time() * 1000), cycle_id),
                )
            self.audit.write(
                conn, MODULE_ID, "daily_cycle.step_failed", "daily_cycle_step",
                "%s:%s" % (cycle_id, step_name),
                after={"attempts": attempts, "error": str(error)[:500]},
            )
            return {"attempts": attempts, "exhausted": attempts >= MAX_ATTEMPTS}

    def complete(self, cycle_id, audit_detail):
        """Audit 与周期完成同事务提交。"""
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE daily_cycle SET status = 'completed', completed_at = ?, current_step = NULL,"
                " updated_at = ? WHERE cycle_id = ?",
                (int(time.time() * 1000), int(time.time() * 1000), cycle_id),
            )
            self.audit.write(
                conn, MODULE_ID, "daily_cycle.complete", "daily_cycle", cycle_id,
                after=audit_detail,
            )

    def recover(self, cycle_id):
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE daily_cycle SET status = 'recovery_pending', updated_at = ? WHERE cycle_id = ?",
                (int(time.time() * 1000), cycle_id),
            )
            # 阶段 running 恢复为 pending（保留 attempts），已完成的保持。
            conn.execute(
                "UPDATE daily_cycle_steps SET status = 'pending'"
                " WHERE cycle_id = ? AND status = 'running'",
                (cycle_id,),
            )
            self.audit.write(
                conn, MODULE_ID, "daily_cycle.recover", "daily_cycle", cycle_id,
                after={"status": "recovery_pending"},
            )

    def status(self, cycle_id):
        cycle = self.db.conn.execute(
            "SELECT * FROM daily_cycle WHERE cycle_id = ?", (cycle_id,)
        ).fetchone()
        if cycle is None:
            return None
        steps = self.db.conn.execute(
            "SELECT * FROM daily_cycle_steps WHERE cycle_id = ? ORDER BY step_order", (cycle_id,)
        ).fetchall()
        return {"cycle": dict(cycle), "steps": [dict(s) for s in steps]}

    def find(self, timezone, schedule_date):
        row = self.db.conn.execute(
            "SELECT * FROM daily_cycle WHERE timezone = ? AND schedule_date = ?",
            (timezone, schedule_date),
        ).fetchone()
        return dict(row) if row else None

    def abandon(self, cycle_id, reason):
        """跨天未完成的周期：标记 failed（不自动补跑更早周期），留审计。"""
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE daily_cycle SET status = 'failed', last_error = ?, updated_at = ?"
                " WHERE cycle_id = ? AND status IN ('running', 'recovery_pending', 'pending')",
                (str(reason)[:500], int(time.time() * 1000), cycle_id),
            )
            self.audit.write(
                conn, MODULE_ID, "daily_cycle.abandon", "daily_cycle", cycle_id,
                after={"status": "failed", "reason": str(reason)[:500]},
            )

    def recoverable(self):
        rows = self.db.conn.execute(
            "SELECT * FROM daily_cycle WHERE status IN ('running', 'recovery_pending')"
            " ORDER BY created_at",
        ).fetchall()
        return [dict(r) for r in rows]
