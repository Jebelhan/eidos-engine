"""P9 情绪接入测试：队列状态机、升级、提交、幂等、修正、失败重试。"""

import os
import shutil
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from dsh_engine.audit import AuditLog  # noqa: E402
from dsh_engine.db import Database  # noqa: E402
from dsh_engine.emotion import EmotionService  # noqa: E402
from dsh_engine.emotion_queue import EmotionQueue  # noqa: E402
from dsh_engine.idempotency import RequestDedup  # noqa: E402

BASE = 1_700_000_000_000


class EmotionTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dsh-p9-")
        self.db = Database(os.path.join(self.tmp, "test.sqlite"))
        self.db.migrate()
        self.audit = AuditLog(self.db)
        self.dedup = RequestDedup(self.db)
        self.emotion = EmotionService(self.db, self.dedup, self.audit)
        self.queue = EmotionQueue(self.db, self.audit)
        self.n = 0

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def submit(self, canonical, event_type="positive", confidence=0.7):
        self.n += 1
        return self.emotion.submit_emotion_event(
            request_id="emo-%d" % self.n,
            workspace_id="ws-1",
            source_experience_id=canonical,
            source_type="experience",
            event_type=event_type,
            description="test",
            confidence=confidence,
            emotion_candidates={"p": 0.3, "a": 0.1, "d": 0.0},
        )

    def enqueue(self, canonical, status):
        with self.db.transaction() as conn:
            return self.queue.upsert_from_event(
                conn,
                workspace_id="ws-1",
                canonical_experience_id=canonical,
                source_experience_id="exp-" + canonical,
                source_status=status,
            )

    # ---------- 队列 ----------

    def test_uncertain_goes_awaiting(self):
        result = self.enqueue("c-1", "uncertain")
        self.assertEqual(result["queue_status"], "awaiting_verification")
        self.assertEqual(self.queue.get("ws-1:c-1")["queue_status"], "awaiting_verification")

    def test_verified_goes_pending_eval(self):
        result = self.enqueue("c-2", "verified")
        self.assertEqual(result["queue_status"], "pending_eval")

    def test_uncertain_then_verified_promotes(self):
        self.enqueue("c-3", "uncertain")
        result = self.enqueue("c-3", "verified")
        self.assertEqual(result["action"], "promoted")
        self.assertEqual(self.queue.get("ws-1:c-3")["queue_status"], "pending_eval")

    def test_verified_then_uncertain_does_not_downgrade(self):
        self.enqueue("c-4", "verified")
        result = self.enqueue("c-4", "uncertain")
        self.assertEqual(result["action"], "unchanged")
        self.assertEqual(self.queue.get("ws-1:c-4")["queue_status"], "pending_eval")

    def test_awaiting_not_in_pending_eval(self):
        self.enqueue("c-5", "uncertain")
        self.assertEqual(self.queue.pending_eval(limit=10), [])

    # ---------- 提交与幂等 ----------

    def test_submit_processed(self):
        result = self.submit("c-6")
        self.assertTrue(result["created"])
        event = self.emotion.get_event(result["emotion_event_id"])
        self.assertEqual(event["status"], "processed")
        self.assertEqual(event["event_type"], "positive")

    def test_submit_idempotent(self):
        first = self.submit("c-7")
        self.n += 1
        second = self.emotion.submit_emotion_event(
            request_id="emo-dup",
            workspace_id="ws-1",
            source_experience_id="c-7",
            event_type="positive",
            description="again",
            confidence=0.9,
        )
        self.assertEqual(first["emotion_event_id"], second["emotion_event_id"])
        self.assertEqual(len(self.emotion.find_by_experience("ws-1", "c-7")), 1)

    def test_invalid_event_type_rejected(self):
        with self.assertRaises(ValueError):
            self.submit("c-8", event_type="happy")

    # ---------- 修正 ----------

    def test_revise_changes_event_type(self):
        result = self.submit("c-9", event_type="positive")
        self.n += 1
        revised = self.emotion.revise_emotion_event(
            request_id="rev-1",
            workspace_id="ws-1",
            emotion_event_id=result["emotion_event_id"],
            correction={"event_type": "mixed", "confidence": 0.6},
            reason="re-evaluation",
        )
        self.assertTrue(revised["revised"])
        event = self.emotion.get_event(result["emotion_event_id"])
        self.assertEqual(event["event_type"], "mixed")
        self.assertEqual(event["status"], "revised")
        self.assertEqual(len(self.emotion.find_by_experience("ws-1", "c-9")), 1)

    def test_revise_requires_reason(self):
        result = self.submit("c-10")
        with self.assertRaises(ValueError):
            self.emotion.revise_emotion_event(
                request_id="rev-2",
                workspace_id="ws-1",
                emotion_event_id=result["emotion_event_id"],
                correction={"event_type": "mixed"},
                reason="",
            )

    # ---------- 失败重试 ----------

    def test_mark_failed_retry_and_dead(self):
        self.enqueue("c-11", "verified")
        evidence_id = "ws-1:c-11"
        self.assertEqual(self.queue.mark_failed(evidence_id, "model down", now_ms=BASE), "retry_wait")
        self.assertEqual(self.queue.mark_failed(evidence_id, "model down", now_ms=BASE), "retry_wait")
        self.assertEqual(self.queue.mark_failed(evidence_id, "model down", now_ms=BASE), "dead")
        self.assertEqual(self.queue.pending_eval(now_ms=BASE + 10_000_000), [])

    def test_mark_evaluated(self):
        self.enqueue("c-12", "verified")
        evidence_id = "ws-1:c-12"
        self.queue.mark_evaluated(evidence_id, "emotion-1")
        row = self.queue.get(evidence_id)
        self.assertEqual(row["queue_status"], "evaluated")
        self.assertEqual(row["emotion_event_id"], "emotion-1")

    def test_audit_written(self):
        self.enqueue("c-13", "verified")
        rows = self.audit.query(module_id="emotion-access", limit=10)
        self.assertTrue(any(r["operation"] == "emotion_queue.enqueue" for r in rows))


if __name__ == "__main__":
    unittest.main(verbosity=2)
