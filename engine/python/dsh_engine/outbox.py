"""统一 Outbox。

状态机（系统集成层定稿）：
    pending → published → consumed
    pending/published → failed（可重试）

多消费者：event_consumer_records 按 (event_id, consumer_module) 独立记录；
只有所有已注册消费者都 consumed，事件才整体标记 consumed。
消费失败不影响其他消费者已完成的记录。
"""

import json
import time
import uuid


class Outbox:
    def __init__(self, db):
        self.db = db

    # ---------- 生产 ----------

    def append(
        self,
        conn,
        *,
        event_type,
        producer_module,
        aggregate_type,
        aggregate_id,
        payload,
        logical_version=None,
        correlation_id=None,
        causation_id=None,
        expected_consumers=None,
    ):
        """写事件。调用方应把本调用放进自己的业务事务（同事务写入）。

        expected_consumers: 预期消费者列表。省略时从订阅表快照当时有效的订阅者
        （P8-B：事件创建时固定消费者集合，不再由业务生产方手动决定）。
        """
        event_id = str(uuid.uuid4())
        conn.execute(
            "INSERT INTO outbox_events"
            " (event_id, event_type, producer_module, aggregate_type, aggregate_id, payload,"
            "  logical_version, correlation_id, causation_id, created_at, status)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')",
            (
                event_id,
                event_type,
                producer_module,
                aggregate_type,
                str(aggregate_id),
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
                logical_version,
                correlation_id,
                causation_id,
                int(time.time() * 1000),
            ),
        )
        if expected_consumers is None:
            expected_consumers = self.snapshot_subscribers(conn, event_type)
        for consumer in expected_consumers or ():
            conn.execute(
                "INSERT INTO event_consumer_records(event_id, consumer_module, consumer_status)"
                " VALUES (?, ?, 'pending')",
                (event_id, consumer),
            )
        return event_id

    # ---------- 订阅（P8-B） ----------

    def snapshot_subscribers(self, conn, event_type):
        """返回当前启用中的订阅者快照。"""
        rows = conn.execute(
            "SELECT consumer_module FROM event_subscriptions WHERE event_type = ? AND enabled = 1"
            " ORDER BY created_at",
            (event_type,),
        ).fetchall()
        return [row["consumer_module"] for row in rows]

    def register_subscription(self, event_type, consumer_module, enabled=True):
        """注册订阅。只影响此后的新事件（历史事件已快照，不追）。"""
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO event_subscriptions(event_type, consumer_module, enabled, created_at)"
                " VALUES (?, ?, ?, ?)"
                " ON CONFLICT(event_type, consumer_module) DO UPDATE SET enabled = excluded.enabled",
                (event_type, consumer_module, 1 if enabled else 0, int(time.time() * 1000)),
            )

    def set_subscription_enabled(self, event_type, consumer_module, enabled):
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE event_subscriptions SET enabled = ? WHERE event_type = ? AND consumer_module = ?",
                (1 if enabled else 0, event_type, consumer_module),
            )

    def subscriptions(self):
        rows = self.db.conn.execute(
            "SELECT * FROM event_subscriptions ORDER BY event_type, consumer_module"
        ).fetchall()
        return [dict(row) for row in rows]

    # ---------- 发布 ----------

    def publish(self, event_id):
        """pending/failed → published。已 published/consumed 时幂等返回。"""
        with self.db.transaction() as conn:
            row = conn.execute("SELECT status FROM outbox_events WHERE event_id = ?", (event_id,)).fetchone()
            if row is None:
                raise KeyError("unknown event: %s" % event_id)
            if row["status"] in ("published", "consumed"):
                return row["status"]
            conn.execute(
                "UPDATE outbox_events SET status = 'published', published_at = ? WHERE event_id = ?",
                (int(time.time() * 1000), event_id),
            )
            return "published"

    def fail_publish(self, event_id, error, retry_delay_ms=0):
        """发布失败：failed，记录尝试次数与错误。"""
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE outbox_events SET status = 'failed',"
                " attempt_count = attempt_count + 1, last_error = ?, next_retry_at = ?"
                " WHERE event_id = ?",
                (str(error), int(time.time() * 1000) + retry_delay_ms, event_id),
            )
            return "failed"

    # ---------- 消费 ----------

    def consume(self, event_id, consumer_module, fn):
        """
        幂等消费。fn(conn, payload) 返回结果。
        - 同一消费者重复消费：返回原结果，不重复执行 fn；
        - 所有已注册消费者完成后，事件整体标记 consumed。
        """
        with self.db.transaction() as conn:
            event = conn.execute("SELECT payload, status FROM outbox_events WHERE event_id = ?", (event_id,)).fetchone()
            if event is None:
                raise KeyError("unknown event: %s" % event_id)
            record = conn.execute(
                "SELECT consumer_status FROM event_consumer_records WHERE event_id = ? AND consumer_module = ?",
                (event_id, consumer_module),
            ).fetchone()
            if record is not None and record["consumer_status"] == "consumed":
                return json.loads(event["payload"])

            result = fn(conn, json.loads(event["payload"]))
            conn.execute(
                "INSERT INTO event_consumer_records"
                " (event_id, consumer_module, consumer_status, attempt_count, consumed_at)"
                " VALUES (?, ?, 'consumed', 0, ?)"
                " ON CONFLICT(event_id, consumer_module) DO UPDATE SET"
                " consumer_status = 'consumed', consumed_at = excluded.consumed_at, last_error = NULL",
                (event_id, consumer_module, int(time.time() * 1000)),
            )
            pending = conn.execute(
                "SELECT COUNT(*) AS n FROM event_consumer_records"
                " WHERE event_id = ? AND consumer_status != 'consumed'",
                (event_id,),
            ).fetchone()["n"]
            if pending == 0:
                conn.execute("UPDATE outbox_events SET status = 'consumed' WHERE event_id = ?", (event_id,))
            return result

    def consume_fail(self, event_id, consumer_module, error):
        """消费者失败：记录该消费者失败，不标记事件整体 consumed。"""
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO event_consumer_records"
                " (event_id, consumer_module, consumer_status, attempt_count, last_error)"
                " VALUES (?, ?, 'failed', 1, ?)"
                " ON CONFLICT(event_id, consumer_module) DO UPDATE SET"
                " consumer_status = 'failed', attempt_count = attempt_count + 1, last_error = excluded.last_error",
                (event_id, consumer_module, str(error)),
            )
            return "failed"

    # ---------- 查询 ----------

    def get(self, event_id):
        row = self.db.conn.execute("SELECT * FROM outbox_events WHERE event_id = ?", (event_id,)).fetchone()
        return dict(row) if row else None

    def consumers(self, event_id):
        rows = self.db.conn.execute(
            "SELECT * FROM event_consumer_records WHERE event_id = ? ORDER BY consumer_module",
            (event_id,),
        ).fetchall()
        return [dict(row) for row in rows]

    def pending_events(self, limit=50):
        rows = self.db.conn.execute(
            "SELECT * FROM outbox_events WHERE status IN ('pending', 'failed') ORDER BY created_at LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(row) for row in rows]
