"""E4-E Split 消费链路测试：事件映射迁移、待复核、幂等。"""

import json
import os
import shutil
import sys
import tempfile
import time
import unittest
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

TMP = tempfile.mkdtemp(prefix="dsh-e4e-")
os.environ["DSH_ENGINE_DATA_DIR"] = TMP
os.environ["DSH_ENGINE_TEST_CONSUMER"] = "1"

import main  # noqa: E402

WS = "ws-e4e"
BASE = 1_700_000_000_000


class SplitConsumerTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        main.ENGINE.init()
        cls.eng = main.ENGINE
        cls.db = main.ENGINE.db

    @classmethod
    def tearDownClass(cls):
        cls.db.close()
        shutil.rmtree(TMP, ignore_errors=True)

    def setUp(self):
        self.n = 0
        with self.db.transaction() as conn:
            for table in (
                "emotion_events", "trust_evidence", "trust_evidence_source_events",
                "relationship", "counterpart", "conclusion_evidence", "conclusions",
                "learning_influence_confirmations", "learning_influence_requests",
                "experience_change_consumer_operations", "experience_split_mappings",
                "experience_changes", "outbox_events", "event_consumer_records",
                "evidence_review_queue",
            ):
                conn.execute("DELETE FROM %s" % table)

    def exp(self, events):
        self.n += 1
        created = self.eng.memory.create_experience(
            request_id="e-%s" % uuid.uuid4().hex, workspace_id=WS, session_id="s", subject_id=None,
            event_ids=events, experience_type="conversation_turn",
            occurred_at=int(time.time() * 1000), description="d",
        )
        experience_id = created["experience_id"]
        self.eng.memory.verify_experience(
            request_id="v-%s" % uuid.uuid4().hex, experience_id=experience_id,
            target_status="verified", reason="t",
        )
        return experience_id

    def add_emotion(self, experience_id, source_event, slot):
        event_id = "emo-%s" % uuid.uuid4().hex[:8]
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO emotion_events(emotion_event_id, workspace_id, source_experience_id,"
                " source_event_id, source_type, event_type, event_slot, description, confidence,"
                " status, created_at) VALUES (?, ?, ?, ?, 'experience', 'positive', ?, 't', 0.8,"
                " 'processed', ?)",
                (event_id, WS, experience_id, source_event, slot, BASE),
            )
        return event_id

    def add_learning(self, experience_id):
        conclusion = self.eng.learning.create_conclusion(
            request_id="lc-%s" % uuid.uuid4().hex, workspace_id=WS, statement="c")
        evidence_id = "le-%s" % uuid.uuid4().hex[:8]
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO conclusion_evidence(evidence_id, conclusion_id, experience_id,"
                " polarity, created_at) VALUES (?, ?, ?, 'support', ?)",
                (evidence_id, conclusion["conclusion_id"], experience_id, BASE),
            )
        return evidence_id

    def add_confirmation(self, experience_id):
        request_id = "ir-%s" % uuid.uuid4().hex[:8]
        confirmation_id = "cf-%s" % uuid.uuid4().hex[:8]
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO learning_influence_requests(influence_request_id, suggestion_id,"
                " conclusion_id, target_trait_id, direction, raw_strength, confidence, source_weight,"
                " status, created_at, window_expires_at, updated_at) VALUES (?, ?, 'c-1', 't-1', 1,"
                " 0.5, 0.5, 1.0, 'pending', ?, ?, ?)",
                (request_id, "s-%s" % uuid.uuid4().hex[:8], BASE, BASE + 86_400_000, BASE),
            )
            conn.execute(
                "INSERT INTO learning_influence_confirmations(confirmation_id, influence_request_id,"
                " experience_id, independence_key, occurred_at, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (confirmation_id, request_id, experience_id, "ck-%s" % uuid.uuid4().hex[:8], BASE, BASE),
            )
        return confirmation_id

    def dispatch_all(self, rounds=5):
        for _ in range(rounds):
            self.eng.dispatcher.dispatch(limit=20)

    def split(self, source, specs):
        self.n += 1
        return self.eng.memory.split_experience(
            request_id="sp-%s" % uuid.uuid4().hex, source_experience_id=source,
            new_experience_specs=specs, expected_change_version=0,
        )

    def test_split_emotion_mapping(self):
        ev1 = "ev-a-%s" % uuid.uuid4().hex[:8]
        ev2 = "ev-b-%s" % uuid.uuid4().hex[:8]
        a = self.exp([ev1, ev2])
        emo1 = self.add_emotion(a, ev1, 1)
        emo2 = self.add_emotion(a, ev2, 2)
        result = self.split(a, [
            {"event_ids": [ev1], "description": "p1"},
            {"event_ids": [ev2], "description": "p2"},
        ])
        self.dispatch_all()
        targets = {
            row["event_id"]: row["target_experience_id"]
            for row in self.db.conn.execute(
                "SELECT event_id, target_experience_id FROM experience_split_mappings"
                " WHERE operation_id = ?", (result["operation_id"],))
        }
        row1 = self.db.conn.execute(
            "SELECT source_experience_id FROM emotion_events WHERE emotion_event_id = ?", (emo1,)).fetchone()
        row2 = self.db.conn.execute(
            "SELECT source_experience_id FROM emotion_events WHERE emotion_event_id = ?", (emo2,)).fetchone()
        self.assertEqual(row1["source_experience_id"], targets[ev1])
        self.assertEqual(row2["source_experience_id"], targets[ev2])
        self.assertNotEqual(row1["source_experience_id"], row2["source_experience_id"])

    def test_split_learning_and_personality_to_review(self):
        ev1 = "ev-a-%s" % uuid.uuid4().hex[:8]
        ev2 = "ev-b-%s" % uuid.uuid4().hex[:8]
        a = self.exp([ev1, ev2])
        evidence_id = self.add_learning(a)
        confirmation_id = self.add_confirmation(a)
        result = self.split(a, [{"event_ids": [ev1]}, {"event_ids": [ev2]}])
        self.dispatch_all()
        reviews = self.db.conn.execute(
            "SELECT * FROM evidence_review_queue WHERE operation_id = ?", (result["operation_id"],)
        ).fetchall()
        refs = {row["evidence_ref"] for row in reviews}
        self.assertIn(evidence_id, refs)
        self.assertIn(confirmation_id, refs)
        for row in reviews:
            self.assertEqual(row["review_status"], "pending")
            self.assertEqual(row["review_reason"], "split_without_event_trace")

    def test_split_operation_completed(self):
        ev1 = "ev-a-%s" % uuid.uuid4().hex[:8]
        ev2 = "ev-b-%s" % uuid.uuid4().hex[:8]
        a = self.exp([ev1, ev2])
        result = self.split(a, [{"event_ids": [ev1]}, {"event_ids": [ev2]}])
        self.dispatch_all()
        statuses = {row["consumer_module"]: row["consumer_status"] for row in self.db.conn.execute(
            "SELECT * FROM experience_change_consumer_operations WHERE operation_id = ?",
            (result["operation_id"],),
        )}
        for module in ("emotion", "relationship", "learning", "personality"):
            self.assertEqual(statuses.get(module), "completed", module)

    def test_repeat_dispatch_idempotent(self):
        ev1 = "ev-a-%s" % uuid.uuid4().hex[:8]
        ev2 = "ev-b-%s" % uuid.uuid4().hex[:8]
        a = self.exp([ev1, ev2])
        self.split(a, [{"event_ids": [ev1]}, {"event_ids": [ev2]}])
        self.dispatch_all()
        self.dispatch_all()
        count = self.db.conn.execute("SELECT COUNT(*) FROM evidence_review_queue").fetchone()[0]
        self.assertEqual(count, 0)  # 无学习/人格证据 → 无待复核


if __name__ == "__main__":
    unittest.main(verbosity=2)
