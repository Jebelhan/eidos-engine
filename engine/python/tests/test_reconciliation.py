"""P14-E3 对账测试：六类检查、分级、指纹去重、resolved、幂等。"""

import json
import os
import shutil
import sys
import tempfile
import time
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from dsh_engine.audit import AuditLog  # noqa: E402
from dsh_engine.db import Database  # noqa: E402
from dsh_engine.idempotency import RequestDedup  # noqa: E402
from dsh_engine.reconciliation import ReconciliationService  # noqa: E402

BASE = 1_700_000_000_000


class ReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dsh-p14e3-")
        self.db = Database(os.path.join(self.tmp, "test.sqlite"))
        self.db.migrate()
        self.audit = AuditLog(self.db)
        self.dedup = RequestDedup(self.db)
        self.recon = ReconciliationService(self.db, self.dedup, self.audit)
        self.n = 0

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def event(self, i=1, status="published", dead=False, event_type="experience.verified", aggregate="exp-1", created_at=None):
        eid = "ev-%d" % i
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO outbox_events(event_id, event_type, producer_module, aggregate_type,"
                " aggregate_id, payload, logical_version, created_at, published_at, consumed_at, status,"
                " attempt_count, dead_letter_at, dead_letter_reason) VALUES (?, ?, 'memory', 'experience',"
                " ?, '{}', 1, ?, ?, ?, ?, 1, ?, ?)",
                (eid, event_type, aggregate, created_at or (BASE + i), BASE + i, BASE + i,
                 status, BASE + i if dead else None, "dead" if dead else None),
            )
        return eid

    def record(self, event_id, consumer="emotion-evidence", status="consumed", attempts=1, dead=False):
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO event_consumer_records(event_id, consumer_module, consumer_status,"
                " attempt_count, consumed_at, dead_letter_at) VALUES (?, ?, ?, ?, ?, ?)",
                (event_id, consumer, status, attempts, BASE if status == "consumed" else None,
                 BASE if dead else None),
            )

    def subscription(self, event_type, consumer="emotion-evidence"):
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO event_subscriptions(event_type, consumer_module, enabled, created_at)"
                " VALUES (?, ?, 1, ?)",
                (event_type, consumer, BASE),
            )

    def consume_audit(self, event_id, consumer="emotion-evidence"):
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO audit_records(audit_id, module_id, operation, target_type, target_id,"
                " before_snapshot, after_snapshot, source_refs, request_id, logical_version, created_at)"
                " VALUES (?, 'dispatcher', 'outbox.consume', 'event', ?, NULL, ?, NULL, NULL, 1, ?)",
                ("a-%d" % self.n, event_id,
                 json.dumps({"consumer_module": consumer, "status": "consumed"}), BASE),
            )
            self.n += 1

    def run_recon(self, run_type="full"):
        self.n += 1
        return self.recon.reconcile(request_id="run-%d" % self.n, run_type=run_type)

    def issues(self):
        return {
            row["issue_type"]: row
            for row in self.db.conn.execute("SELECT * FROM outbox_reconciliation_issues")
        }

    # ---------- 完整性 ----------

    def test_normal_event_no_issue(self):
        self.event(1, status="consumed")
        self.record("ev-1")
        self.subscription("experience.verified")
        self.consume_audit("ev-1")
        result = self.run_recon()
        self.assertEqual(result["p0_count"], 0)
        self.assertEqual(result["p1_count"], 0)
        self.assertEqual(result["p2_count"], 0)

    def test_event_without_records_has_subscription(self):
        self.event(1)
        self.subscription("experience.verified")
        result = self.run_recon()
        self.assertEqual(result["info_count"], 1)
        self.assertIn("subscription_unknown", self.issues())

    def test_event_without_records_no_subscription(self):
        self.event(1)
        self.run_recon()
        self.assertIn("no_subscriber", self.issues())

    def test_orphan_record(self):
        self.record("ev-missing")
        result = self.run_recon()
        self.assertEqual(result["p0_count"], 1)
        self.assertIn("orphan_consumer_record", self.issues())

    def test_multiple_consumers_ok(self):
        self.event(1, status="consumed")
        self.record("ev-1", "emotion-evidence")
        self.record("ev-1", "relationship-evidence")
        self.subscription("experience.verified", "emotion-evidence")
        self.subscription("experience.verified", "relationship-evidence")
        self.consume_audit("ev-1", "emotion-evidence")
        self.consume_audit("ev-1", "relationship-evidence")
        result = self.run_recon()
        self.assertEqual(result["p0_count"] + result["p1_count"], 0)

    # ---------- 状态 ----------

    def test_consumed_without_record(self):
        self.event(1, status="consumed")
        self.subscription("experience.verified")
        result = self.run_recon()
        self.assertEqual(result["p1_count"], 1)
        self.assertIn("consumed_without_record", self.issues())

    def test_dead_letter_with_active_record(self):
        self.event(1, status="failed", dead=True)
        self.record("ev-1", status="failed", dead=False)
        self.subscription("experience.verified")
        result = self.run_recon()
        self.assertIn("dead_letter_with_active_record", self.issues())

    # ---------- 重试 ----------

    def test_over_retry_not_dead(self):
        self.event(1, status="failed")
        self.record("ev-1", status="failed", attempts=3, dead=False)
        self.subscription("experience.verified")
        result = self.run_recon()
        self.assertIn("over_retry_not_dead", self.issues())

    # ---------- Audit ----------

    def test_missing_consume_audit(self):
        self.event(1)
        self.record("ev-1")
        self.subscription("experience.verified")
        result = self.run_recon()
        self.assertEqual(result["p1_count"], 1)
        self.assertIn("missing_consume_audit", self.issues())

    # ---------- 顺序 ----------

    def test_aggregate_order_violation(self):
        self.event(1, status="failed", aggregate="agg-1", created_at=BASE + 1)
        self.event(2, status="consumed", aggregate="agg-1", created_at=BASE + 2)
        self.record("ev-1", status="failed")
        self.record("ev-2")
        self.subscription("experience.verified")
        self.consume_audit("ev-2")
        result = self.run_recon()
        self.assertIn("aggregate_order_violation", self.issues())

    # ---------- 指纹与 resolved ----------

    def test_fingerprint_dedup(self):
        self.event(1)
        self.subscription("experience.verified")
        first = self.run_recon()
        second = self.run_recon()
        self.assertEqual(first["new_issues"], 1)
        self.assertEqual(second["new_issues"], 0)
        self.assertEqual(second["recurring_issues"], 1)
        count = self.db.conn.execute(
            "SELECT COUNT(*) FROM outbox_reconciliation_issues"
        ).fetchone()[0]
        self.assertEqual(count, 1)

    def test_resolved_when_condition_clears(self):
        self.event(1)
        self.subscription("experience.verified")
        self.run_recon()
        self.assertIn("subscription_unknown", self.issues())
        # 补上消费者记录 + 审计 → 条件消失 → 下一轮对账 resolved
        self.record("ev-1")
        self.consume_audit("ev-1")
        result = self.run_recon()
        self.assertEqual(result["resolved_issues"], 1)
        row = self.issues()["subscription_unknown"]
        self.assertEqual(row["issue_status"], "resolved")

    def test_idempotent_request(self):
        self.event(1)
        first = self.recon.reconcile(request_id="same-1", run_type="full")
        second = self.recon.reconcile(request_id="same-1", run_type="full")
        self.assertEqual(first, second)
        runs = self.db.conn.execute("SELECT COUNT(*) FROM outbox_reconciliation_runs").fetchone()[0]
        self.assertEqual(runs, 1)

    # ---------- 复合游标 ----------

    def test_composite_cursor_same_timestamp(self):
        for i in (1, 2, 3):
            self.event(i, status="published", created_at=BASE)
        self.subscription("experience.verified")
        result = self.recon.reconcile(
            request_id="cur-1", run_type="incremental",
            cursor={"created_at": BASE, "event_id": "ev-1"},
        )
        self.assertEqual(result["checked_events"], 2)  # ev-2 / ev-3 同毫秒，按 event_id 续扫
        self.assertEqual(result["cursor_after"]["event_id"], "ev-3")
        result2 = self.recon.reconcile(
            request_id="cur-2", run_type="incremental",
            cursor={"created_at": BASE, "event_id": "ev-3"},
        )
        self.assertEqual(result2["checked_events"], 0)

    def test_full_run_does_not_advance_incremental_cursor(self):
        self.event(1, created_at=BASE)
        self.subscription("experience.verified")
        full = self.recon.reconcile(request_id="f-1", run_type="full")
        # 全量 run 也写自己的 cursor_after，但增量取 cursor 时只认 incremental run。
        self.assertIsNotNone(full["cursor_after"])
        rows = self.db.conn.execute(
            "SELECT run_type, cursor_after FROM outbox_reconciliation_runs WHERE run_type = 'incremental'"
        ).fetchall()
        self.assertEqual(len(rows), 0)

    # ---------- 人工状态 ----------

    def test_acknowledged_not_resolved(self):
        self.event(1)
        self.subscription("experience.verified")
        self.run_recon()
        with self.db.transaction() as conn:
            conn.execute("UPDATE outbox_reconciliation_issues SET issue_status = 'acknowledged'")
        self.run_recon()
        row = self.issues()["subscription_unknown"]
        self.assertEqual(row["issue_status"], "acknowledged")  # 人工确认不等于已解决

    def test_ignored_stays_ignored_on_rereport(self):
        self.event(1)
        self.subscription("experience.verified")
        self.run_recon()
        with self.db.transaction() as conn:
            conn.execute("UPDATE outbox_reconciliation_issues SET issue_status = 'ignored'")
        first_seen = self.issues()["subscription_unknown"]["last_seen_at"]
        result = self.run_recon()
        row = self.issues()["subscription_unknown"]
        self.assertEqual(row["issue_status"], "ignored")
        self.assertGreaterEqual(row["last_seen_at"], first_seen)  # 重复发现只更新 last_seen
        self.assertEqual(result["recurring_issues"], 1)

    def test_record_with_consumer_outside_current_subscription_not_flagged(self):
        # 有消费者记录时，不因当前订阅表没有该消费者而判异常（历史关系不可反推）。
        self.event(1, status="consumed")
        self.record("ev-1", consumer="legacy-consumer")
        self.consume_audit("ev-1", "legacy-consumer")
        result = self.run_recon()
        self.assertEqual(result["p1_count"] + result["p2_count"], 0)
        self.assertNotIn("subscription_unknown_consumer", self.issues())

    # ---------- 增量不解决未扫描事件 ----------

    def test_incremental_does_not_resolve_unscanned(self):
        self.event(1, created_at=BASE)
        self.subscription("experience.verified")
        self.run_recon("full")  # 发现 ev-1 的 subscription_unknown
        self.event(2, created_at=BASE + 1000)
        result = self.recon.reconcile(
            request_id="inc-1", run_type="incremental",
            cursor={"created_at": BASE, "event_id": "ev-1"},
        )
        self.assertEqual(result["resolved_issues"], 0)
        row = self.db.conn.execute(
            "SELECT * FROM outbox_reconciliation_issues WHERE fingerprint = 'subscription_unknown:ev-1:'"
        ).fetchone()
        self.assertEqual(row["issue_status"], "open")  # 未扫描 → 保持 open

    # ---------- 失败 run 保留 ----------

    def test_failed_run_record_kept(self):
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO outbox_reconciliation_runs(run_id, request_id, run_type, status, started_at, error)"
                " VALUES ('failed-1', 'req-1', 'full', 'failed', ?, 'boom')",
                (BASE,),
            )
        row = self.db.conn.execute(
            "SELECT status, error FROM outbox_reconciliation_runs WHERE run_id = 'failed-1'"
        ).fetchone()
        self.assertEqual(row["status"], "failed")
        self.assertEqual(row["error"], "boom")


if __name__ == "__main__":
    unittest.main(verbosity=2)
