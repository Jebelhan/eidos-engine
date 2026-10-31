"""P3 基础设施测试：迁移、持久化、事务回滚、幂等、审计、Outbox。

直接跑：python tests/test_infra.py
"""

import os
import shutil
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from dsh_engine.audit import AuditLog  # noqa: E402
from dsh_engine.db import Database  # noqa: E402
from dsh_engine.idempotency import IdempotencyConflict, RequestDedup  # noqa: E402
from dsh_engine.migrations import MIGRATIONS  # noqa: E402
from dsh_engine.outbox import Outbox  # noqa: E402

LATEST_SCHEMA = MIGRATIONS[-1][0]


class InfraTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dsh-p3-")
        self.path = os.path.join(self.tmp, "test.sqlite")
        self.db = Database(self.path)
        self.applied = self.db.migrate()

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    # ---------- migration ----------

    def test_migrate_creates_schema(self):
        self.assertEqual(self.db.schema_version(), LATEST_SCHEMA)
        self.assertIn("initial infrastructure schema", [m["name"] for m in self.db.migration_records()])
        for table in ("request_dedup", "audit_records", "outbox_events", "event_consumer_records"):
            self.assertIn(table, self.db.tables())

    def test_migrate_is_idempotent(self):
        # setUp 已执行首次迁移；再次迁移不应重复执行。
        self.assertEqual(self.db.migrate(), [])
        self.assertEqual(self.db.schema_version(), LATEST_SCHEMA)

    def test_persist_across_restart(self):
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO request_dedup(request_id, module_id, operation_type, payload_hash,"
                " request_status, result_reference, created_at, completed_at)"
                " VALUES('r-persist', 'm', 'op', 'h', 'completed', '{}', 1, 1)"
            )
        self.db.close()
        reopened = Database(self.path)
        try:
            self.assertEqual(reopened.schema_version(), LATEST_SCHEMA)
            row = reopened.conn.execute("SELECT request_id FROM request_dedup").fetchone()
            self.assertEqual(row["request_id"], "r-persist")
        finally:
            reopened.close()
            self.db = reopened  # tearDown 关闭

    # ---------- idempotency ----------

    def test_dedup_replay_returns_original(self):
        dedup = RequestDedup(self.db)
        calls = {"n": 0}

        def work(conn):
            calls["n"] += 1
            return {"value": 1, "n": calls["n"]}

        first = dedup.run("m", "req-1", "op", {"a": 1}, work)
        second = dedup.run("m", "req-1", "op", {"a": 1}, work)
        self.assertEqual(first, second)
        self.assertEqual(calls["n"], 1)

    def test_dedup_conflict(self):
        dedup = RequestDedup(self.db)
        dedup.run("m", "req-2", "op", {"a": 1}, lambda conn: {"ok": True})
        with self.assertRaises(IdempotencyConflict):
            dedup.run("m", "req-2", "op", {"a": 2}, lambda conn: {"ok": False})

    def test_dedup_module_boundary(self):
        dedup = RequestDedup(self.db)
        a = dedup.run("module-a", "req-3", "op", {"x": 1}, lambda conn: {"owner": "a"})
        b = dedup.run("module-b", "req-3", "op", {"x": 1}, lambda conn: {"owner": "b"})
        self.assertEqual(a, {"owner": "a"})
        self.assertEqual(b, {"owner": "b"})

    def test_transaction_rollback_removes_dedup_record(self):
        dedup = RequestDedup(self.db)

        def failing(conn):
            conn.execute("INSERT INTO request_dedup(request_id, module_id, operation_type, payload_hash,"
                         " request_status, created_at) VALUES('r-roll', 'm', 'op', 'h', 'completed', 1)")
            raise RuntimeError("boom")

        with self.assertRaises(RuntimeError):
            dedup.run("m", "req-roll", "op", {"a": 1}, failing)
        self.assertIsNone(dedup.get("m", "req-roll"))
        self.assertEqual(
            self.db.conn.execute("SELECT COUNT(*) AS n FROM request_dedup WHERE request_id='r-roll'").fetchone()["n"], 0
        )

    # ---------- audit ----------

    def test_audit_append_only(self):
        audit = AuditLog(self.db)
        with self.db.transaction() as conn:
            audit_id = audit.write(conn, "m", "op", "record", "target-1", after={"v": 1})
        self.assertEqual(len(audit.query(target_type="record", target_id="target-1")), 1)
        with self.assertRaises(Exception):
            with self.db.transaction() as conn:
                conn.execute("UPDATE audit_records SET operation='x' WHERE audit_id=?", (audit_id,))
        with self.assertRaises(Exception):
            with self.db.transaction() as conn:
                conn.execute("DELETE FROM audit_records WHERE audit_id=?", (audit_id,))

    # ---------- outbox ----------

    def test_outbox_full_lifecycle(self):
        outbox = Outbox(self.db)
        seen = []
        with self.db.transaction() as conn:
            eid = outbox.append(
                conn,
                event_type="test.event",
                producer_module="m",
                aggregate_type="agg",
                aggregate_id="a1",
                payload={"v": 1},
                expected_consumers=["c1", "c2"],
            )
        self.assertEqual(outbox.get(eid)["status"], "pending")
        outbox.publish(eid)
        outbox.consume(eid, "c1", lambda conn, payload: seen.append(("c1", payload)))
        self.assertEqual(outbox.get(eid)["status"], "published")
        outbox.consume(eid, "c2", lambda conn, payload: seen.append(("c2", payload)))
        self.assertEqual(outbox.get(eid)["status"], "consumed")
        self.assertEqual([s[0] for s in seen], ["c1", "c2"])

    def test_outbox_consume_idempotent(self):
        outbox = Outbox(self.db)
        seen = []
        with self.db.transaction() as conn:
            eid = outbox.append(
                conn, event_type="e", producer_module="m", aggregate_type="a", aggregate_id="1", payload={}
            )
        outbox.publish(eid)
        outbox.consume(eid, "c1", lambda conn, payload: seen.append(1))
        outbox.consume(eid, "c1", lambda conn, payload: seen.append(2))
        self.assertEqual(seen, [1])

    def test_outbox_retry_from_failed(self):
        outbox = Outbox(self.db)
        with self.db.transaction() as conn:
            eid = outbox.append(
                conn, event_type="e", producer_module="m", aggregate_type="a", aggregate_id="1", payload={}
            )
        outbox.fail_publish(eid, "simulated")
        self.assertEqual(outbox.get(eid)["status"], "failed")
        self.assertEqual(outbox.get(eid)["attempt_count"], 1)
        outbox.publish(eid)
        self.assertEqual(outbox.get(eid)["status"], "published")

    def test_outbox_consumer_fail_then_recover(self):
        outbox = Outbox(self.db)
        with self.db.transaction() as conn:
            eid = outbox.append(
                conn, event_type="e", producer_module="m", aggregate_type="a", aggregate_id="1", payload={"v": 1}
            )
        outbox.publish(eid)
        outbox.consume_fail(eid, "c1", "boom")
        self.assertEqual(outbox.get(eid)["status"], "published")
        self.assertEqual(outbox.consumers(eid)[0]["consumer_status"], "failed")
        outbox.consume(eid, "c1", lambda conn, payload: {"ok": True})
        self.assertEqual(outbox.get(eid)["status"], "consumed")


if __name__ == "__main__":
    unittest.main(verbosity=2)
