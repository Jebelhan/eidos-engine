"""P14-B 情绪推进测试：基线锚定、低通、幂等、摘要、行为接通。"""

import os
import shutil
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from dsh_engine.audit import AuditLog  # noqa: E402
from dsh_engine.behavior import BehaviorService  # noqa: E402
from dsh_engine.db import Database  # noqa: E402
from dsh_engine.emotion import EmotionService  # noqa: E402
from dsh_engine.idempotency import RequestDedup  # noqa: E402
from dsh_engine.memory import MemoryService  # noqa: E402
from dsh_engine.outbox import Outbox  # noqa: E402
from dsh_engine.personality import PersonalityService  # noqa: E402

BASE = 1_700_000_000_000


class EmotionAdvanceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dsh-p14b-")
        self.db = Database(os.path.join(self.tmp, "test.sqlite"))
        self.db.migrate()
        self.audit = AuditLog(self.db)
        self.dedup = RequestDedup(self.db)
        self.outbox = Outbox(self.db)
        self.memory = MemoryService(self.db, self.dedup, self.audit, self.outbox)
        self.emotion = EmotionService(self.db, self.dedup, self.audit)
        self.personality = PersonalityService(self.db, self.dedup, self.audit, self.memory)
        self.personality.ensure_traits()
        self.behavior = BehaviorService(self.db, self.dedup, self.audit, self.personality, self.emotion)
        self.n = 0

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def add_event(self, i, event_type="positive", confidence=0.8, created_at=None, candidates=None):
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO emotion_events(emotion_event_id, workspace_id, source_experience_id,"
                " source_type, event_type, event_slot, description, confidence, emotion_candidates,"
                " status, created_at) VALUES (?, 'ws-1', ?, 'experience', ?, 0, 'test', ?, ?, 'processed', ?)",
                (
                    "emo-%d" % i, "exp-%d" % i, event_type, confidence,
                    __import__("json").dumps(candidates) if candidates else None,
                    created_at if created_at is not None else BASE - 3_600_000,
                ),
            )

    def advance(self, target=BASE, workspace="ws-1"):
        self.n += 1
        return self.emotion.advance_emotion_state(
            request_id="adv-%d" % self.n, workspace_id=workspace, target_time=target
        )

    # ---------- 推进 ----------

    def test_no_events_returns_to_baseline(self):
        result = self.advance()
        self.assertTrue(result["advanced"])
        self.assertAlmostEqual(result["pad"]["p"], 0.0, places=6)

    def test_positive_event_moves_pad(self):
        self.add_event(1, "positive", 0.8, created_at=BASE - 3_600_000)
        result = self.advance()
        self.assertGreater(result["pad"]["p"], 0.0)

    def test_negative_event_moves_pad_negative(self):
        self.add_event(1, "negative", 0.8, created_at=BASE - 3_600_000)
        result = self.advance()
        self.assertLess(result["pad"]["p"], 0.0)

    def test_already_advanced(self):
        self.advance()
        second = self.advance(target=BASE - 1000)
        self.assertFalse(second["advanced"])

    def test_idempotent(self):
        self.add_event(1)
        first = self.emotion.advance_emotion_state(
            request_id="same-1", workspace_id="ws-1", target_time=BASE
        )
        second = self.emotion.advance_emotion_state(
            request_id="same-1", workspace_id="ws-1", target_time=BASE
        )
        self.assertEqual(first, second)

    def test_workspace_isolated(self):
        self.add_event(1)
        other = self.advance(workspace="ws-2")
        self.assertAlmostEqual(other["pad"]["p"], 0.0, places=6)

    def test_checkpoint_written(self):
        self.add_event(1)
        self.advance()
        count = self.db.conn.execute("SELECT COUNT(*) AS n FROM emotion_checkpoints").fetchone()["n"]
        self.assertEqual(count, 1)

    # ---------- 摘要 ----------

    def test_summary_available(self):
        self.add_event(1, "positive", 0.9)
        self.advance()
        summary = self.emotion.get_emotion_summary("ws-1")
        self.assertTrue(summary["available"])
        self.assertGreater(summary["pad_base"]["p"], 0.0)
        self.assertEqual(summary["events_count"], 1)

    def test_behavior_consumes_emotion(self):
        self.add_event(1, "positive", 0.9)
        self.advance()
        guidance = self.behavior.get_behavior_guidance("s-1", "ws-1")
        ids = [g["guidance_id"] for g in guidance["preferred"]]
        self.assertIn("emotion:positive", ids)
        self.assertFalse(guidance["degraded"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
