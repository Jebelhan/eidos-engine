"""E4-F Revised 消费链路测试：最小消费、待复核、激活保护。"""

import os
import shutil
import sys
import tempfile
import time
import unittest
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

TMP = tempfile.mkdtemp(prefix="dsh-e4f-")
os.environ["DSH_ENGINE_DATA_DIR"] = TMP
os.environ["DSH_ENGINE_TEST_CONSUMER"] = "1"

import main  # noqa: E402

WS = "ws-e4f"
BASE = 1_700_000_000_000


class ReviseConsumerTest(unittest.TestCase):
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
                "conclusion_evidence", "conclusions", "learning_influence_confirmations",
                "learning_influence_requests", "experience_change_consumer_operations",
                "outbox_events", "event_consumer_records", "evidence_review_queue",
            ):
                conn.execute("DELETE FROM %s" % table)

    def exp(self):
        self.n += 1
        created = self.eng.memory.create_experience(
            request_id="e-%s" % uuid.uuid4().hex, workspace_id=WS, session_id="s", subject_id=None,
            event_ids=["ev-%s" % uuid.uuid4().hex], experience_type="conversation_turn",
            occurred_at=int(time.time() * 1000), description="d",
        )
        experience_id = created["experience_id"]
        self.eng.memory.verify_experience(
            request_id="v-%s" % uuid.uuid4().hex, experience_id=experience_id,
            target_status="verified", reason="t",
        )
        return experience_id

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

    def add_confirmation(self, experience_id, status="pending"):
        request_id = "ir-%s" % uuid.uuid4().hex[:8]
        confirmation_id = "cf-%s" % uuid.uuid4().hex[:8]
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO learning_influence_requests(influence_request_id, suggestion_id,"
                " conclusion_id, target_trait_id, direction, raw_strength, confidence, source_weight,"
                " status, created_at, window_expires_at, updated_at) VALUES (?, ?, 'c-1', 't-1', 1,"
                " 0.5, 0.5, 1.0, ?, ?, ?, ?)",
                (request_id, "s-%s" % uuid.uuid4().hex[:8], status, BASE, BASE + 86_400_000, BASE),
            )
            conn.execute(
                "INSERT INTO learning_influence_confirmations(confirmation_id, influence_request_id,"
                " experience_id, independence_key, occurred_at, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (confirmation_id, request_id, experience_id, "ck-%s" % uuid.uuid4().hex[:8], BASE, BASE),
            )
        return confirmation_id

    def dispatch_all(self, rounds=10):
        for _ in range(rounds):
            self.eng.dispatcher.dispatch(limit=20)

    def revise(self, experience_id):
        return self.eng.memory.revise_experience(
            request_id="rv-%s" % uuid.uuid4().hex, experience_id=experience_id,
            correction={"description": "revised"}, expected_change_version=0,
        )

    def test_revise_consumers_complete(self):
        exp = self.exp()
        result = self.revise(exp)
        self.dispatch_all()
        statuses = {row["consumer_module"]: row["consumer_status"] for row in self.db.conn.execute(
            "SELECT * FROM experience_change_consumer_operations WHERE operation_id = ?",
            (result["operation_id"],))}
        for module in ("emotion", "relationship", "learning", "personality"):
            self.assertEqual(statuses.get(module), "completed", module)

    def test_learning_evidence_queued(self):
        exp = self.exp()
        evidence_id = self.add_learning(exp)
        result = self.revise(exp)
        self.dispatch_all()
        rows = self.db.conn.execute(
            "SELECT * FROM evidence_review_queue WHERE operation_id = ?", (result["operation_id"],)
        ).fetchall()
        refs = {row["evidence_ref"] for row in rows}
        self.assertIn(evidence_id, refs)

    def test_personality_unactivated_queued(self):
        exp = self.exp()
        confirmation_id = self.add_confirmation(exp, status="pending")
        result = self.revise(exp)
        self.dispatch_all()
        rows = self.db.conn.execute(
            "SELECT * FROM evidence_review_queue WHERE operation_id = ?", (result["operation_id"],)
        ).fetchall()
        refs = {row["evidence_ref"] for row in rows}
        self.assertIn(confirmation_id, refs)

    def test_activated_confirmation_not_queued(self):
        exp = self.exp()
        self.add_confirmation(exp, status="active")
        result = self.revise(exp)
        self.dispatch_all()
        count = self.db.conn.execute(
            "SELECT COUNT(*) FROM evidence_review_queue WHERE operation_id = ?", (result["operation_id"],)
        ).fetchone()[0]
        self.assertEqual(count, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
