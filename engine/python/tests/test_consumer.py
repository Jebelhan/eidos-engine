"""E4-C1 消费基础设施测试：操作级幂等、待复核队列、审计。"""

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
from dsh_engine.experience_consumer import ExperienceChangeConsumer  # noqa: E402


class ConsumerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dsh-e4c1-")
        self.db = Database(os.path.join(self.tmp, "test.sqlite"))
        self.db.migrate()
        self.audit = AuditLog(self.db)
        self.consumer = ExperienceChangeConsumer(self.db, self.audit)
        self.n = 0

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def begin(self, operation_id="op-1", module="emotion", version=1):
        return self.consumer.begin(
            operation_id=operation_id, consumer_module=module,
            operation_type="merge", mapping_version=version,
        )

    # ---------- 操作级幂等 ----------

    def test_begin_first(self):
        result = self.begin()
        self.assertFalse(result["skip"])
        self.assertEqual(result["attempt"], 1)

    def test_begin_completed_skips(self):
        self.begin()
        self.consumer.complete(operation_id="op-1", consumer_module="emotion")
        result = self.begin()
        self.assertTrue(result["skip"])
        self.assertEqual(result["reason"], "already_completed")

    def test_version_conflict(self):
        self.begin(version=1)
        result = self.begin(version=2)
        self.assertTrue(result["conflict"])
        self.assertEqual(result["reason"], "mapping_version_mismatch")

    def test_fail_and_retry(self):
        self.begin()
        self.consumer.fail(operation_id="op-1", consumer_module="emotion", error="boom")
        status = self.consumer.operation_status("op-1")
        self.assertEqual(status["overall"], "partial")
        result = self.begin()
        self.assertFalse(result["skip"])
        self.assertEqual(result["attempt"], 2)

    def test_operation_status_all_completed(self):
        self.begin(module="emotion")
        self.begin(module="relationship")
        self.consumer.complete(operation_id="op-1", consumer_module="emotion")
        self.consumer.complete(operation_id="op-1", consumer_module="relationship")
        status = self.consumer.operation_status("op-1")
        self.assertEqual(status["overall"], "completed")
        self.assertEqual(status["consumers"]["emotion"], "completed")

    def test_stale_processing_detected_and_retry(self):
        self.begin()
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE experience_change_consumer_operations SET started_at = ?",
                (int(time.time() * 1000) - 20 * 60 * 1000,),
            )
        status = self.consumer.operation_status("op-1")
        self.assertEqual(status["consumers"]["emotion"], "stale")
        result = self.begin()
        self.assertFalse(result["skip"])
        self.assertEqual(result["attempt"], 2)
        audits = self.db.conn.execute(
            "SELECT COUNT(*) FROM audit_records WHERE operation = 'experience_change.stale_recovery'"
        ).fetchone()[0]
        self.assertEqual(audits, 1)

    # ---------- 待复核队列 ----------

    def test_enqueue(self):
        result = self.consumer.enqueue_review(
            workspace_id="ws-1", operation_id="op-1", consumer_module="learning",
            evidence_ref="ev-1", reason="split_without_event_trace",
        )
        self.assertFalse(result["deduplicated"])
        self.assertEqual(result["review_status"], "pending")

    def test_enqueue_dedup(self):
        first = self.consumer.enqueue_review(
            workspace_id="ws-1", operation_id="op-1", consumer_module="learning",
            evidence_ref="ev-1", reason="split_without_event_trace",
        )
        second = self.consumer.enqueue_review(
            workspace_id="ws-1", operation_id="op-1", consumer_module="learning",
            evidence_ref="ev-1", reason="split_without_event_trace",
        )
        self.assertEqual(first["review_id"], second["review_id"])
        self.assertTrue(second["deduplicated"])
        count = self.db.conn.execute("SELECT COUNT(*) FROM evidence_review_queue").fetchone()[0]
        self.assertEqual(count, 1)

    def test_enqueue_invalid_reason(self):
        with self.assertRaises(ValueError):
            self.consumer.enqueue_review(
                workspace_id="ws-1", operation_id="op-1", consumer_module="learning",
                evidence_ref="ev-1", reason="made_up_reason",
            )

    def test_resolve_requires_actor_reason(self):
        review = self.consumer.enqueue_review(
            workspace_id="ws-1", operation_id="op-1", consumer_module="learning",
            evidence_ref="ev-1", reason="split_without_event_trace",
        )
        with self.assertRaises(ValueError):
            self.consumer.resolve_review(review_id=review["review_id"], review_status="resolved",
                                         actor="", reason="x", resolution_ref="r")
        with self.assertRaises(ValueError):
            self.consumer.resolve_review(review_id=review["review_id"], review_status="resolved",
                                         actor="tester", reason="", resolution_ref="r")

    def test_resolved_requires_ref(self):
        review = self.consumer.enqueue_review(
            workspace_id="ws-1", operation_id="op-1", consumer_module="learning",
            evidence_ref="ev-1", reason="split_without_event_trace",
        )
        with self.assertRaises(ValueError):
            self.consumer.resolve_review(review_id=review["review_id"], review_status="resolved",
                                         actor="tester", reason="done")

    def test_resolve_writes_audit(self):
        review = self.consumer.enqueue_review(
            workspace_id="ws-1", operation_id="op-1", consumer_module="learning",
            evidence_ref="ev-1", reason="split_without_event_trace",
        )
        self.consumer.resolve_review(review_id=review["review_id"], review_status="resolved",
                                     actor="tester", reason="manual merge", resolution_ref="m-1")
        row = self.db.conn.execute(
            "SELECT review_status, resolution_ref FROM evidence_review_queue WHERE review_id = ?",
            (review["review_id"],),
        ).fetchone()
        self.assertEqual(row["review_status"], "resolved")
        self.assertEqual(row["resolution_ref"], "m-1")
        audits = self.db.conn.execute(
            "SELECT * FROM audit_records WHERE operation = 'evidence_review.resolved'"
        ).fetchall()
        self.assertEqual(len(audits), 1)

    def test_list_reviews_filters(self):
        self.consumer.enqueue_review(
            workspace_id="ws-1", operation_id="op-1", consumer_module="learning",
            evidence_ref="ev-1", reason="split_without_event_trace",
        )
        self.consumer.enqueue_review(
            workspace_id="ws-1", operation_id="op-2", consumer_module="personality",
            evidence_ref="ev-2", reason="ambiguous_event_mapping",
        )
        rows = self.consumer.list_reviews({"review_status": "pending"})
        self.assertEqual(len(rows), 2)
        only_learning = self.consumer.list_reviews({"consumer_module": "learning"})
        self.assertEqual(len(only_learning), 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
