"""E4-D Merge 消费链路测试：四模块重关联、操作级幂等、部分失败。"""

import json
import os
import shutil
import sys
import tempfile
import time
import uuid
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

TMP = tempfile.mkdtemp(prefix="dsh-e4d-")
os.environ["DSH_ENGINE_DATA_DIR"] = TMP
os.environ["DSH_ENGINE_TEST_CONSUMER"] = "1"

import main  # noqa: E402

WS = "ws-e4d"
BASE = 1_700_000_000_000


class MergeConsumerTest(unittest.TestCase):
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
                "experience_change_consumer_operations", "outbox_events",
                "event_consumer_records",
            ):
                conn.execute("DELETE FROM %s" % table)

    def exp(self, i, status="verified"):
        self.n += 1
        created = self.eng.memory.create_experience(
            request_id="e-%s" % uuid.uuid4().hex, workspace_id=WS, session_id="s", subject_id=None,
            event_ids=["ev-%d-%d-%s" % (i, self.n, uuid.uuid4().hex)], experience_type="conversation_turn",
            occurred_at=int(time.time() * 1000), description="d",
        )
        experience_id = created["experience_id"]
        if status != "pending":
            self.eng.memory.verify_experience(
                request_id="v-%s" % uuid.uuid4().hex, experience_id=experience_id,
                target_status=status, reason="t",
            )
        return experience_id

    def add_emotion(self, experience_id):
        self.n += 1
        i = self.n
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO emotion_events(emotion_event_id, workspace_id, source_experience_id,"
                " source_event_id, source_type, event_type, event_slot, description, confidence,"
                " status, created_at) VALUES (?, ?, ?, ?, 'experience', 'positive', ?, 't', 0.8,"
                " 'processed', ?)",
                ("emo-%d" % i, WS, experience_id, "cev-%d" % i, i, BASE),
            )
        return "emo-%d" % i

    def add_trust(self, experience_id):
        self.n += 1
        i = self.n
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO counterpart(counterpart_id, dsh_instance_id, counterpart_type,"
                " identity_namespace, identity_key, display_name, identity_status, created_at, updated_at)"
                " VALUES (?, 'test-instance', 'user', 'user', ?, '', 'active', ?, ?)",
                ("cp-%d" % i, "k-%d" % i, BASE, BASE),
            )
            conn.execute(
                "INSERT INTO relationship(relationship_id, dsh_instance_id, counterpart_id, workspace_id,"
                " status, trust_value, trust_confidence, revision, created_at, updated_at)"
                " VALUES (?, 'test-instance', ?, ?, 'active', 0, 0, 0, ?, ?)",
                ("rel-%d" % i, "cp-%d" % i, WS, BASE, BASE),
            )
            conn.execute(
                "INSERT INTO trust_evidence(evidence_id, workspace_id, relationship_id,"
                " source_experience_id, event_slot, trust_signal, evidence_weight, evidence_status,"
                " revision, evidence_key, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, 1.0, 0.5, 'active', 0, ?, ?, ?)",
                ("te-%d" % i, WS, "rel-%d" % i, experience_id, "slot-%d" % i, "ek-%d" % i, BASE, BASE),
            )
            conn.execute(
                "INSERT INTO trust_evidence_source_events(evidence_id, source_event_id, created_at)"
                " VALUES (?, ?, ?)",
                ("te-%d" % i, "cev-%d" % i, BASE),
            )
        return "te-%d" % i

    def add_learning(self, experience_id):
        self.n += 1
        i = self.n
        conclusion = self.eng.learning.create_conclusion(
            request_id="lc-%s" % uuid.uuid4().hex, workspace_id=WS, statement="c")
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO conclusion_evidence(evidence_id, conclusion_id, experience_id,"
                " polarity, created_at) VALUES (?, ?, ?, 'support', ?)",
                ("le-%d" % i, conclusion["conclusion_id"], experience_id, BASE),
            )
        return "le-%d" % i
        return conclusion["conclusion_id"]

    def add_confirmation(self, experience_id):
        self.n += 1
        i = self.n
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO learning_influence_requests(influence_request_id, suggestion_id,"
                " conclusion_id, target_trait_id, direction, raw_strength, confidence, source_weight,"
                " status, created_at, window_expires_at, updated_at) VALUES (?, ?, 'c-1', 't-1', 1,"
                " 0.5, 0.5, 1.0, 'pending', ?, ?, ?)",
                ("ir-%d" % i, "s-%d" % i, BASE, BASE + 86_400_000, BASE),
            )
            conn.execute(
                "INSERT INTO learning_influence_confirmations(confirmation_id, influence_request_id,"
                " experience_id, independence_key, occurred_at, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                ("cf-%d" % i, "ir-%d" % i, experience_id, "ck-%d" % i, BASE, BASE),
            )
        return "cf-%d" % i

    def dispatch_all(self, rounds=5):
        for _ in range(rounds):
            self.eng.dispatcher.dispatch(limit=20)

    def merge(self, sources, canonical, versions):
        self.n += 1
        return self.eng.memory.merge_experiences(
            request_id="m-%s" % uuid.uuid4().hex, source_experience_ids=sources,
            canonical_experience_id=canonical, expected_change_versions=versions,
        )

    # ---------- 主链路 ----------

    def test_single_source_merge_full_chain(self):
        a = self.exp(1)
        b = self.exp(2)
        emo_id = self.add_emotion(a)
        trust_id = self.add_trust(a)
        learn_id = self.add_learning(a)
        conf_id = self.add_confirmation(a)
        result = self.merge([a], b, {a: 0, b: 0})
        self.dispatch_all()

        emo = self.db.conn.execute("SELECT source_experience_id FROM emotion_events WHERE emotion_event_id = ?", (emo_id,)).fetchone()
        self.assertEqual(emo["source_experience_id"], b)
        trust = self.db.conn.execute("SELECT source_experience_id FROM trust_evidence WHERE evidence_id = ?", (trust_id,)).fetchone()
        self.assertEqual(trust["source_experience_id"], b)
        learn = self.db.conn.execute("SELECT experience_id FROM conclusion_evidence WHERE evidence_id = ?", (learn_id,)).fetchone()
        self.assertEqual(learn["experience_id"], b)
        conf = self.db.conn.execute("SELECT experience_id FROM learning_influence_confirmations WHERE confirmation_id = ?", (conf_id,)).fetchone()
        self.assertEqual(conf["experience_id"], b)

        statuses = {row["consumer_module"]: row["consumer_status"] for row in self.db.conn.execute(
            "SELECT * FROM experience_change_consumer_operations WHERE operation_id = ?",
            (result["operation_id"],),
        )}
        for module in ("emotion", "relationship", "learning", "personality"):
            self.assertEqual(statuses.get(module), "completed", module)

    def test_multi_source_merge(self):
        a = self.exp(1)
        b = self.exp(2)
        c = self.exp(3)
        self.add_emotion(a)
        self.add_emotion(b)
        result = self.merge([a, b], c, {a: 0, b: 0, c: 0})
        self.dispatch_all()
        rows = self.db.conn.execute("SELECT source_experience_id AS experience_id FROM emotion_events ORDER BY emotion_event_id").fetchall()
        self.assertEqual([r["experience_id"] for r in rows], [c, c])
        statuses = {row["consumer_module"]: row["consumer_status"] for row in self.db.conn.execute(
            "SELECT * FROM experience_change_consumer_operations WHERE operation_id = ?",
            (result["operation_id"],),
        )}
        self.assertEqual(statuses.get("emotion"), "completed")

    def test_repeat_dispatch_idempotent(self):
        a = self.exp(1)
        b = self.exp(2)
        self.add_emotion(a)
        self.merge([a], b, {a: 0, b: 0})
        self.dispatch_all()
        self.dispatch_all()
        count = self.db.conn.execute("SELECT COUNT(*) FROM emotion_events").fetchone()[0]
        self.assertEqual(count, 1)

    def test_partial_failure_then_recovery(self):
        a = self.exp(1)
        b = self.exp(2)
        emo_id = self.add_emotion(a)
        original = self.eng.learning.relocate_conclusion_evidence_for_merge

        def boom(*args, **kwargs):
            raise RuntimeError("injected failure")

        self.eng.learning.relocate_conclusion_evidence_for_merge = boom
        try:
            result = self.merge([a], b, {a: 0, b: 0})
            self.dispatch_all(rounds=10)
        finally:
            self.eng.learning.relocate_conclusion_evidence_for_merge = original

        emo = self.db.conn.execute(
            "SELECT source_experience_id FROM emotion_events WHERE emotion_event_id = ?", (emo_id,)).fetchone()
        self.assertEqual(emo["source_experience_id"], b)  # 情绪成功
        failed = self.db.conn.execute(
            "SELECT COUNT(*) FROM event_consumer_records WHERE consumer_module = 'experience-merge-learning'"
            " AND consumer_status = 'failed'").fetchone()[0]
        self.assertGreaterEqual(failed, 1)  # 学习失败

        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE event_consumer_records SET next_retry_at = 0 WHERE consumer_module ="
                " 'experience-merge-learning' AND consumer_status = 'failed'")
        self.dispatch_all()
        statuses = {row["consumer_module"]: row["consumer_status"] for row in self.db.conn.execute(
            "SELECT * FROM experience_change_consumer_operations WHERE operation_id = ?",
            (result["operation_id"],))}
        self.assertEqual(statuses.get("learning"), "completed")

    def test_conflict_kept_on_learning(self):
        a = self.exp(1)
        b = self.exp(2)
        cid = self.add_learning(a)
        self.n += 1
        conflict_id = "le-%d" % self.n
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO conclusion_evidence(evidence_id, conclusion_id, experience_id,"
                " polarity, created_at) VALUES (?, ?, ?, 'support', ?)",
                (conflict_id, cid, b, BASE),
            )
        self.merge([a], b, {a: 0, b: 0})
        self.dispatch_all()
        # 冲突：原证据保留，不删除、不迁移
        count = self.db.conn.execute("SELECT COUNT(*) FROM conclusion_evidence").fetchone()[0]
        self.assertEqual(count, 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
