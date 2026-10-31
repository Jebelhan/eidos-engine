"""P8-B 分发器测试：订阅快照、多消费者、失败隔离、重试、死信、顺序、恢复、审计。"""

import json
import os
import shutil
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from dsh_engine.audit import AuditLog  # noqa: E402
from dsh_engine.db import Database  # noqa: E402
from dsh_engine.dispatcher import Dispatcher  # noqa: E402
from dsh_engine.outbox import Outbox  # noqa: E402

BASE = 1_700_000_000_000


class DispatcherTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dsh-p8-")
        self.db = Database(os.path.join(self.tmp, "test.sqlite"))
        self.db.migrate()
        self.audit = AuditLog(self.db)
        self.outbox = Outbox(self.db)
        self.dispatcher = Dispatcher(self.db, self.audit, self.outbox)
        self.calls = []

        def echo(conn, payload):
            self.calls.append(("echo", payload))
            return {"seen": True}

        self.dispatcher.register_handler("echo", echo)

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def append(self, event_type="experience.created", aggregate="agg-1", payload=None, consumers=None):
        with self.db.transaction() as conn:
            return self.outbox.append(
                conn,
                event_type=event_type,
                producer_module="test",
                aggregate_type="experience",
                aggregate_id=aggregate,
                payload=payload or {"v": 1},
                expected_consumers=consumers,
            )

    # ---------- 订阅快照 ----------

    def test_subscription_snapshot_at_creation(self):
        self.outbox.register_subscription("experience.created", "echo")
        eid = self.append()
        records = self.outbox.consumers(eid)
        self.assertEqual([r["consumer_module"] for r in records], ["echo"])

    def test_new_subscription_does_not_touch_history(self):
        first = self.append()
        self.outbox.register_subscription("experience.created", "echo")
        second = self.append()
        self.assertEqual(self.outbox.consumers(first), [])
        self.assertEqual([r["consumer_module"] for r in self.outbox.consumers(second)], ["echo"])

    # ---------- 投递 ----------

    def test_auto_dispatch_consumes(self):
        self.outbox.register_subscription("experience.created", "echo")
        eid = self.append()
        stats = self.dispatcher.dispatch(now_ms=BASE)
        self.assertEqual(stats["consumed"], 1)
        self.assertEqual(self.outbox.get(eid)["status"], "consumed")
        self.assertEqual(len(self.calls), 1)

    def test_consume_idempotent(self):
        self.outbox.register_subscription("experience.created", "echo")
        self.append()
        self.dispatcher.dispatch(now_ms=BASE)
        stats = self.dispatcher.dispatch(now_ms=BASE)
        self.assertEqual(stats["consumed"], 0)
        self.assertEqual(len(self.calls), 1)

    def test_two_consumers_independent(self):
        self.outbox.register_subscription("experience.created", "a")
        self.outbox.register_subscription("experience.created", "b")
        self.dispatcher.register_handler("a", lambda c, p: {"a": True})
        self.dispatcher.register_handler("b", lambda c, p: {"b": True})
        eid = self.append()
        stats = self.dispatcher.dispatch(now_ms=BASE)
        self.assertEqual(stats["consumed"], 2)
        self.assertEqual(self.outbox.get(eid)["status"], "consumed")

    def test_consumer_failure_isolated(self):
        self.outbox.register_subscription("experience.created", "good")
        self.outbox.register_subscription("experience.created", "bad")

        def fail(conn, payload):
            raise RuntimeError("boom")

        self.dispatcher.register_handler("good", lambda c, p: {"ok": True})
        self.dispatcher.register_handler("bad", fail)
        eid = self.append()
        stats = self.dispatcher.dispatch(now_ms=BASE)
        self.assertEqual(stats["consumed"], 1)
        self.assertEqual(stats["failed"], 1)
        records = {r["consumer_module"]: r for r in self.outbox.consumers(eid)}
        self.assertEqual(records["good"]["consumer_status"], "consumed")
        self.assertEqual(records["bad"]["consumer_status"], "failed")
        self.assertIsNone(records["bad"]["dead_letter_at"])

    def test_retry_backoff(self):
        self.outbox.register_subscription("experience.created", "flaky")
        attempts = {"n": 0}

        def flaky(conn, payload):
            attempts["n"] += 1
            if attempts["n"] < 2:
                raise RuntimeError("transient")
            return {"ok": True}

        self.dispatcher.register_handler("flaky", flaky)
        eid = self.append()
        self.dispatcher.dispatch(now_ms=BASE)  # 失败，退避 30s
        record = self.outbox.consumers(eid)[0]
        self.assertEqual(record["consumer_status"], "failed")
        self.assertEqual(record["next_retry_at"], BASE + 30_000)
        # 未到时间：跳过
        stats = self.dispatcher.dispatch(now_ms=BASE + 1_000)
        self.assertEqual(stats["consumed"], 0)
        # 到期：成功
        stats = self.dispatcher.dispatch(now_ms=BASE + 31_000)
        self.assertEqual(stats["consumed"], 1)
        self.assertEqual(self.outbox.get(eid)["status"], "consumed")

    def test_dead_letter_after_max_attempts(self):
        self.outbox.register_subscription("experience.created", "always-fail")
        self.dispatcher.register_handler("always-fail", lambda c, p: (_ for _ in ()).throw(RuntimeError("nope")))
        eid = self.append()
        self.dispatcher.dispatch(now_ms=BASE)
        self.dispatcher.dispatch(now_ms=BASE + 30_000)
        self.dispatcher.dispatch(now_ms=BASE + 30_000 + 120_000)
        record = self.outbox.consumers(eid)[0]
        self.assertIsNotNone(record["dead_letter_at"])
        # 死信后不再自动重试
        stats = self.dispatcher.dispatch(now_ms=BASE + 10_000_000)
        self.assertEqual(stats["consumed"], 0)

    def test_aggregate_order_strict(self):
        self.outbox.register_subscription("experience.created", "echo")
        self.outbox.register_subscription("experience.verified", "echo")
        first = self.append("experience.created", "agg-1")
        second = self.append("experience.verified", "agg-1")
        stats = self.dispatcher.dispatch(now_ms=BASE)
        # 同 aggregate 只处理最早一条
        self.assertEqual(stats["scanned"], 1)
        self.assertEqual(self.outbox.get(first)["status"], "consumed")
        self.assertEqual(self.outbox.get(second)["status"], "pending")
        self.dispatcher.dispatch(now_ms=BASE)
        self.assertEqual(self.outbox.get(second)["status"], "consumed")

    def test_no_consumer_skipped(self):
        eid = self.append()
        stats = self.dispatcher.dispatch(now_ms=BASE)
        # 无预期消费者的事件不进入候选，也不阻塞后续事件。
        self.assertEqual(stats["skipped"], 0)
        self.assertEqual(self.outbox.get(eid)["status"], "pending")

    def test_no_consumer_does_not_block_aggregate(self):
        self.outbox.register_subscription("experience.created", "echo")
        self.outbox.register_subscription("experience.verified", "echo")
        first = self.append("experience.created", "agg-1")
        second = self.append("experience.verified", "agg-1")
        self.dispatcher.dispatch(now_ms=BASE)
        self.assertEqual(self.outbox.get(first)["status"], "consumed")
        self.dispatcher.dispatch(now_ms=BASE)
        self.assertEqual(self.outbox.get(second)["status"], "consumed")

    # ---------- 审计 ----------

    def test_audit_per_attempt(self):
        self.outbox.register_subscription("experience.created", "echo")
        self.append()
        self.dispatcher.dispatch(now_ms=BASE)
        rows = self.audit.query(module_id="dispatcher", limit=10)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["operation"], "outbox.consume")
        self.assertIn("consumed", rows[0]["after_snapshot"])

    # ---------- 恢复与回放 ----------

    def test_restart_resumes(self):
        self.outbox.register_subscription("experience.created", "echo")
        eid = self.append()
        self.db.close()
        self.db = Database(os.path.join(self.tmp, "test.sqlite"))
        self.db.migrate()
        self.audit = AuditLog(self.db)
        self.outbox = Outbox(self.db)
        self.dispatcher = Dispatcher(self.db, self.audit, self.outbox)
        self.dispatcher.register_handler("echo", lambda c, p: {"ok": True})
        stats = self.dispatcher.dispatch(now_ms=BASE)
        self.assertEqual(stats["consumed"], 1)
        self.assertEqual(self.outbox.get(eid)["status"], "consumed")

    def test_replay_requires_actor_reason(self):
        self.outbox.register_subscription("experience.created", "echo")
        eid = self.append()
        with self.assertRaises(ValueError):
            self.dispatcher.replay(eid, "echo", "", "reason")
        with self.assertRaises(ValueError):
            self.dispatcher.replay(eid, "echo", "actor", "")

    def test_replay_resets_record(self):
        self.outbox.register_subscription("experience.created", "echo")
        eid = self.append()
        self.dispatcher.dispatch(now_ms=BASE)
        self.dispatcher.replay(eid, "echo", "operator", "manual recheck")
        record = self.outbox.consumers(eid)[0]
        self.assertEqual(record["consumer_status"], "pending")
        stats = self.dispatcher.dispatch(now_ms=BASE)
        self.assertEqual(stats["consumed"], 1)
        rows = self.audit.query(module_id="dispatcher", limit=10)
        self.assertTrue(any("outbox.replay" == r["operation"] for r in rows))


if __name__ == "__main__":
    unittest.main(verbosity=2)
