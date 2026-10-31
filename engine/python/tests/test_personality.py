"""P12 人格接入测试：建议领取、持续确认、激活、窗口、失效。"""

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
from dsh_engine.learning import LearningService  # noqa: E402
from dsh_engine.memory import MemoryService  # noqa: E402
from dsh_engine.outbox import Outbox  # noqa: E402
from dsh_engine.personality import PersonalityService  # noqa: E402


class PersonalityTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dsh-p12-")
        self.db = Database(os.path.join(self.tmp, "test.sqlite"))
        self.db.migrate()
        self.audit = AuditLog(self.db)
        self.dedup = RequestDedup(self.db)
        self.outbox = Outbox(self.db)
        self.memory = MemoryService(self.db, self.dedup, self.audit, self.outbox)
        self.learning = LearningService(self.db, self.dedup, self.audit, self.memory, self.outbox)
        self.personality = PersonalityService(self.db, self.dedup, self.audit, self.memory)
        self.n = 0
        self.conclusion_id = self._make_conclusion()
        self.suggestion_id = "sug-1"

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _make_conclusion(self):
        conclusion = self.learning.create_conclusion(
            request_id="c-1", workspace_id="ws-1", statement="测试结论"
        )
        return conclusion["conclusion_id"]

    def make_experience(self, i, status="verified", independence_key=None, occurred_at=None):
        exp_id = "exp-%d" % i
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO memory_experience(experience_id, workspace_id, session_id, experience_type,"
                " occurred_at, created_at, description, verification_status, canonical_experience_id,"
                " independence_key, correction_status) VALUES (?, 'ws-1', 's-1', 'conversation_turn',"
                " ?, ?, 'test', ?, ?, ?, 'normal')",
                (exp_id, occurred_at if occurred_at is not None else int(time.time() * 1000) + 1000 + i, 1000 + i, status, exp_id, independence_key or ("k%d" % i)),
            )
            conn.execute(
                "INSERT INTO memory_experience_event_links(link_id, experience_id, event_id, relation_type, created_at)"
                " VALUES (?, ?, ?, 'source', 1)",
                ("link-%d" % i, exp_id, "ev-%d" % i),
            )
        return exp_id

    def handle_suggestion(self):
        self.n += 1
        return self.personality.handle_learning_suggestion(
            request_id="h-%d" % self.n,
            suggestion={"suggestion_id": self.suggestion_id, "confidence": 0.5},
            conclusion_id=self.conclusion_id,
        )

    def confirm(self, rid, exp_id):
        self.n += 1
        return self.personality.confirm_learning_influence(
            request_id="conf-%d" % self.n,
            influence_request_id=rid,
            experience_id=exp_id,
            source_event_ids=["ev-%s" % exp_id.split("-")[-1]],
        )

    # ---------- 建议领取 ----------

    def test_handle_suggestion_creates_request(self):
        result = self.handle_suggestion()
        self.assertTrue(result["created"])
        request = self.personality.get_request(result["influence_request_id"])
        self.assertEqual(request["status"], "pending")
        self.assertGreater(request["window_expires_at"], request["created_at"])

    def test_suggestion_consumption_idempotent(self):
        first = self.handle_suggestion()
        second = self.handle_suggestion()
        self.assertEqual(first["influence_request_id"], second["influence_request_id"])
        self.assertFalse(second["created"])

    # ---------- 确认与激活 ----------

    def test_two_confirmations_activate(self):
        request = self.handle_suggestion()
        rid = request["influence_request_id"]
        e1 = self.make_experience(1)
        e2 = self.make_experience(2)
        r1 = self.confirm(rid, e1)
        self.assertEqual(r1["status"], "confirming")
        self.assertEqual(r1["confirmed_count"], 1)
        r2 = self.confirm(rid, e2)
        self.assertEqual(r2["status"], "active")
        summary = self.personality.get_summary()
        self.assertEqual(len(summary["instances"]), 1)
        self.assertEqual(summary["instances"][0]["status"], "active")
        self.assertGreater(summary["instances"][0]["strength"], 0)

    def test_duplicate_independence_not_counted(self):
        request = self.handle_suggestion()
        rid = request["influence_request_id"]
        e1 = self.make_experience(1, independence_key="same-key")
        e2 = self.make_experience(2, independence_key="same-key")
        self.confirm(rid, e1)
        result = self.confirm(rid, e2)
        self.assertEqual(result["reason"], "duplicate_confirmation")
        self.assertEqual(result["confirmed_count"], 1)

    def test_unverified_rejected(self):
        request = self.handle_suggestion()
        rid = request["influence_request_id"]
        exp = self.make_experience(1, status="pending")
        result = self.confirm(rid, exp)
        self.assertFalse(result["accepted"])

    def test_origin_evidence_not_used(self):
        # 建议来源结论的证据不能再作为确认
        exp = self.make_experience(1, occurred_at=1000)
        self.n += 1
        self.learning.add_conclusion_evidence(
            request_id="e-%d" % self.n,
            workspace_id="ws-1",
            conclusion_id=self.conclusion_id,
            experience_id=exp,
            source_event_ids=["ev-1"],
            polarity="support",
        )
        request = self.handle_suggestion()
        result = self.confirm(request["influence_request_id"], exp)
        self.assertEqual(result["reason"], "origin_evidence")

    def test_window_expired_dropped(self):
        request = self.handle_suggestion()
        rid = request["influence_request_id"]
        # 手动把窗口改到过去
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE learning_influence_requests SET window_expires_at = 1 WHERE influence_request_id = ?",
                (rid,),
            )
        exp = self.make_experience(1)
        result = self.confirm(rid, exp)
        self.assertEqual(result["status"], "dropped")

    def test_invalidate_before_activation_revokes(self):
        request = self.handle_suggestion()
        rid = request["influence_request_id"]
        exp = self.make_experience(1)
        self.confirm(rid, exp)
        with self.db.transaction() as conn:
            self.personality.revoke_confirmations_for_experience(conn, experience_id=exp)
        count = self.db.conn.execute(
            "SELECT COUNT(*) AS n FROM learning_influence_confirmations"
            " WHERE influence_request_id = ? AND revoked_at IS NULL",
            (rid,),
        ).fetchone()["n"]
        self.assertEqual(count, 0)

    def test_activated_not_rolled_back(self):
        request = self.handle_suggestion()
        rid = request["influence_request_id"]
        e1 = self.make_experience(1)
        e2 = self.make_experience(2)
        self.confirm(rid, e1)
        self.confirm(rid, e2)
        with self.db.transaction() as conn:
            self.personality.revoke_confirmations_for_experience(conn, experience_id=e1)
        # 已激活：不回滚，确认保留
        count = self.db.conn.execute(
            "SELECT COUNT(*) AS n FROM learning_influence_confirmations"
            " WHERE influence_request_id = ? AND revoked_at IS NULL",
            (rid,),
        ).fetchone()["n"]
        self.assertEqual(count, 2)
        request_row = self.personality.get_request(rid)
        self.assertEqual(request_row["status"], "active")


if __name__ == "__main__":
    unittest.main(verbosity=2)
