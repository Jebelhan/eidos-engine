"""P8-B：事件分发器（单线程）。

约束（已确认）：
- 同一 aggregate 严格顺序：前一事件未完成，后续事件不处理；
- 跨 aggregate 允许交错，不并发；
- 每个消费者独立幂等，失败不影响其他消费者；
- 消费成功/失败与 Audit 同事务；跳过与空转不写 Audit；
- 失败退避 30s / 2min / 10min，超过上限进死信，不自动重试；
- 历史事件回放必须显式调用 replay（记录 actor / reason）。
"""

import json
import time
import uuid

MAX_ATTEMPTS = 3
BACKOFF_MS = [30_000, 120_000, 600_000]
DISPATCHER_MODULE = "dispatcher"


class Dispatcher:
    def __init__(self, db, audit, outbox):
        self.db = db
        self.audit = audit
        self.outbox = outbox
        self.handlers = {}

    # ---------- 注册 ----------

    def register_handler(self, consumer_module, handler):
        self.handlers[consumer_module] = handler

    # ---------- 投递 ----------

    def dispatch(self, limit=20, now_ms=None):
        """处理一批事件。返回统计。"""
        now = int(now_ms if now_ms is not None else time.time() * 1000)
        stats = {"scanned": 0, "published": 0, "consumed": 0, "failed": 0, "skipped": 0, "dead": 0, "completed": 0}

        # 同一 aggregate 只取最早的一条未完成事件，保证严格顺序。
        # 只选有消费者记录的事件：无预期消费者的事件（历史/未订阅）不阻塞后续事件。
        rows = self.db.conn.execute(
            "SELECT event_id, aggregate_id FROM outbox_events"
            " WHERE status IN ('pending', 'published', 'failed') AND dead_letter_at IS NULL"
            "   AND EXISTS (SELECT 1 FROM event_consumer_records r WHERE r.event_id = outbox_events.event_id)"
            " ORDER BY aggregate_id, created_at"
        ).fetchall()
        per_aggregate = {}
        for row in rows:
            per_aggregate.setdefault(row["aggregate_id"], row["event_id"])
        candidates = list(per_aggregate.values())[:limit]
        stats["scanned"] = len(candidates)

        for event_id in candidates:
            self._dispatch_one(event_id, now, stats)
        return stats

    def _dispatch_one(self, event_id, now, stats):
        event = self.outbox.get(event_id)
        if event is None:
            return

        records = self.db.conn.execute(
            "SELECT * FROM event_consumer_records WHERE event_id = ? ORDER BY consumer_module",
            (event_id,),
        ).fetchall()
        if not records:
            # 无预期消费者（历史事件或未订阅）：保持原状（pending），不发布、不自动完成。
            stats["skipped"] += 1
            return

        if event["status"] == "pending":
            self.outbox.publish(event_id)
            stats["published"] += 1

        for record in records:
            if record["consumer_status"] == "consumed":
                continue
            if record["dead_letter_at"] is not None:
                continue
            if record["next_retry_at"] is not None and record["next_retry_at"] > now:
                continue
            handler = self.handlers.get(record["consumer_module"])
            if handler is None:
                self._fail(event_id, record, "handler-not-registered: %s" % record["consumer_module"], now, stats)
                continue
            self._consume(event_id, record, handler, now, stats)

        self._refresh_event_status(event_id, stats)

    def _consume(self, event_id, record, handler, now, stats):
        event_row = self.db.conn.execute(
            "SELECT payload, event_type FROM outbox_events WHERE event_id = ?", (event_id,)
        ).fetchone()
        payload = json.loads(event_row["payload"])
        # 消费者需要知道事件类型（生命周期消费者按类型分派）。
        payload["__event_type"] = event_row["event_type"]
        consumer = record["consumer_module"]
        try:
            with self.db.transaction() as conn:
                current = conn.execute(
                    "SELECT * FROM event_consumer_records WHERE event_id = ? AND consumer_module = ?",
                    (event_id, consumer),
                ).fetchone()
                if current is None or current["consumer_status"] == "consumed" or current["dead_letter_at"] is not None:
                    return
                attempt = int(current["attempt_count"]) + 1
                # 业务副作用与消费成功记录同事务。
                result = handler(conn, payload)
                conn.execute(
                    "UPDATE event_consumer_records SET consumer_status = 'consumed', consumed_at = ?,"
                    " attempt_count = ?, last_error = NULL, next_retry_at = NULL"
                    " WHERE event_id = ? AND consumer_module = ?",
                    (int(time.time() * 1000), attempt, event_id, consumer),
                )
                self.audit.write(
                    conn,
                    DISPATCHER_MODULE,
                    "outbox.consume",
                    "event",
                    event_id,
                    after={"consumer_module": consumer, "status": "consumed", "attempt": attempt, "result": result},
                )
            stats["consumed"] += 1
        except Exception as error:  # noqa: BLE001 - 单个消费者失败不能影响整批
            self._fail(event_id, record, "%s: %s" % (type(error).__name__, error), now, stats)

    def _fail(self, event_id, record, error_text, now, stats):
        consumer = record["consumer_module"]
        try:
            with self.db.transaction() as conn:
                current = conn.execute(
                    "SELECT * FROM event_consumer_records WHERE event_id = ? AND consumer_module = ?",
                    (event_id, consumer),
                ).fetchone()
                if current is None or current["consumer_status"] == "consumed":
                    return
                attempt = int(current["attempt_count"]) + 1
                if attempt >= MAX_ATTEMPTS:
                    conn.execute(
                        "UPDATE event_consumer_records SET consumer_status = 'failed', attempt_count = ?,"
                        " last_error = ?, next_retry_at = NULL, dead_letter_at = ?, dead_letter_reason = ?"
                        " WHERE event_id = ? AND consumer_module = ?",
                        (attempt, error_text, int(time.time() * 1000), error_text, event_id, consumer),
                    )
                    stats["dead"] += 1
                else:
                    backoff = BACKOFF_MS[min(attempt - 1, len(BACKOFF_MS) - 1)]
                    conn.execute(
                        "UPDATE event_consumer_records SET consumer_status = 'failed', attempt_count = ?,"
                        " last_error = ?, next_retry_at = ?"
                        " WHERE event_id = ? AND consumer_module = ?",
                        (attempt, error_text, now + backoff, event_id, consumer),
                    )
                self.audit.write(
                    conn,
                    DISPATCHER_MODULE,
                    "outbox.consume",
                    "event",
                    event_id,
                    after={"consumer_module": consumer, "status": "failed", "attempt": attempt, "error": error_text},
                )
            stats["failed"] += 1
        except Exception:  # noqa: BLE001 - 审计/记录失败也不能打崩调度
            pass

    def _refresh_event_status(self, event_id, stats):
        with self.db.transaction() as conn:
            consumers = conn.execute(
                "SELECT consumer_status, dead_letter_at FROM event_consumer_records WHERE event_id = ?",
                (event_id,),
            ).fetchall()
            if not consumers:
                return
            pending = [c for c in consumers if c["consumer_status"] != "consumed"]
            dead = [c for c in consumers if c["dead_letter_at"] is not None]
            if dead:
                conn.execute(
                    "UPDATE outbox_events SET status = 'failed', dead_letter_at = ?,"
                    " dead_letter_reason = ? WHERE event_id = ?",
                    (int(time.time() * 1000), "consumer dead letter", event_id),
                )
            elif not pending:
                if conn.execute("SELECT status FROM outbox_events WHERE event_id = ?", (event_id,)).fetchone()["status"] != "consumed":
                    conn.execute("UPDATE outbox_events SET status = 'consumed' WHERE event_id = ?", (event_id,))
                    stats["completed"] += 1

    # ---------- 人工回放（不自动追历史） ----------

    def replay(self, event_id, consumer_module, actor, reason):
        """人工授权回放：重置指定消费者记录。必须记录操作人/原因/目标，仍遵守幂等。"""
        if not actor or not reason:
            raise ValueError("replay requires actor and reason")
        with self.db.transaction() as conn:
            record = conn.execute(
                "SELECT * FROM event_consumer_records WHERE event_id = ? AND consumer_module = ?",
                (event_id, consumer_module),
            ).fetchone()
            if record is None:
                raise KeyError("no consumer record for %s/%s" % (event_id, consumer_module))
            conn.execute(
                "UPDATE event_consumer_records SET consumer_status = 'pending', attempt_count = 0,"
                " next_retry_at = NULL, dead_letter_at = NULL, dead_letter_reason = NULL, last_error = NULL"
                " WHERE event_id = ? AND consumer_module = ?",
                (event_id, consumer_module),
            )
            conn.execute(
                "UPDATE outbox_events SET status = 'published', dead_letter_at = NULL,"
                " dead_letter_reason = NULL WHERE event_id = ?",
                (event_id,),
            )
            self.audit.write(
                conn,
                DISPATCHER_MODULE,
                "outbox.replay",
                "event",
                event_id,
                after={"consumer_module": consumer_module, "actor": actor, "reason": reason},
            )
        return {"replayed": True, "event_id": event_id, "consumer_module": consumer_module}

    # ---------- 状态 ----------

    def status(self):
        rows = self.db.conn.execute(
            "SELECT status, COUNT(*) AS n FROM outbox_events GROUP BY status"
        ).fetchall()
        consumers = self.db.conn.execute(
            "SELECT consumer_status, COUNT(*) AS n FROM event_consumer_records GROUP BY consumer_status"
        ).fetchall()
        dead = self.db.conn.execute(
            "SELECT COUNT(*) AS n FROM event_consumer_records WHERE dead_letter_at IS NOT NULL"
        ).fetchone()["n"]
        return {
            "events": {row["status"]: row["n"] for row in rows},
            "consumers": {row["consumer_status"]: row["n"] for row in consumers},
            "dead_letters": dead,
            "subscriptions": self.outbox.subscriptions(),
            "handlers": sorted(self.handlers.keys()),
        }
