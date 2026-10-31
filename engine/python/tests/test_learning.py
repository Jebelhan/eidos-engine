"""P11 学习接入测试：证据准入、独立计数、建议阈值、撤销、幂等。"""

import os
import shutil
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from dsh_engine.audit import AuditLog  # noqa: E402
from dsh_engine.db import Database  # noqa: E402
from dsh_engine.idempotency import RequestDedup  # noqa: E402
from dsh_engine.learning import LearningService  # noqa: E402
from dsh_engine.memory import MemoryService  # noqa: E402
from dsh_engine.outbox import Outbox  # noqa: E402


class LearningTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dsh-p11-")
        self.db = Database(os.path.join(self.tmp, "test.sqlite"))
        self.db.migrate()
        self.audit = AuditLog(self.db)
        self.dedup = RequestDedup(self.db)
        self.outbox = Outbox(self.db)
        self.memory = MemoryService(self.db, self.dedup, self.audit, self.outbox)
        self.learning = LearningService(self.db, self.dedup, self.audit, self.memory)
        self.n = 0
        self.exp_ids = []

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def make_experience(self, i, status="verified"):
        exp_id = "exp-%d" % i
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO memory_experience(experience_id, workspace_id, session_id, experience_type,"
                " occurred_at, created_at, description, verification_status, canonical_experience_id,"
                " independence_key, correction_status) VALUES (?, 'ws-1', 's-1', 'conversation_turn',"
                " ?, ?, 'test', ?, ?, ?, 'normal')",
                (exp_id, 1000 + i, 1000 + i, "verified" if status == "verified" else status, exp_id, "k%d" % i),
            )
            conn.execute(
                "INSERT INTO memory_experience_event_links(link_id, experience_id, event_id, relation_type, created_at)"
                " VALUES (?, ?, ?, 'source', 1)",
                ("link-%d" % i, exp_id, "ev-%d" % i),
            )
        return {"experience_id": exp_id, "event_id": "ev-%d" % i}

    def make_conclusion(self):
        self.n += 1
        return self.learning.create_conclusion(
            request_id="c-%d" % self.n, workspace_id="ws-1", statement="对方在契约审查中表现专业"
        )

    def add_evidence(self, exp, conclusion_id, polarity="support"):
        self.n += 1
        return self.learning.add_conclusion_evidence(
            request_id="ev-%d" % self.n,
            workspace_id="ws-1",
            conclusion_id=conclusion_id,
            experience_id=exp["experience_id"],
            source_event_ids=[exp["event_id"]],
            polarity=polarity,
            rationale="test",
        )

    # ---------- 证据准入 ----------

    def test_add_evidence_requires_verified(self):
        exp = self.make_experience(1, status="pending")
        conclusion = self.make_conclusion()
        with self.assertRaises(ValueError):
            self.add_evidence(exp, conclusion["conclusion_id"])

    def test_add_evidence_rejects_invalid(self):
        exp = self.make_experience(1, status="invalid")
        conclusion = self.make_conclusion()
        with self.assertRaises(ValueError):
            self.add_evidence(exp, conclusion["conclusion_id"])

    def test_add_evidence_rejects_unknown_event(self):
        exp = self.make_experience(1)
        conclusion = self.make_conclusion()
        with self.assertRaises(ValueError):
            self.learning.add_conclusion_evidence(
                request_id="bad-ev",
                workspace_id="ws-1",
                conclusion_id=conclusion["conclusion_id"],
                experience_id=exp["experience_id"],
                source_event_ids=["fake-event"],
                polarity="support",
            )

    # ---------- 评估与建议 ----------

    def test_suggestion_requires_two_independent_support(self):
        conclusion = self.make_conclusion()
        cid = conclusion["conclusion_id"]
        e1 = self.make_experience(1)
        self.add_evidence(e1, cid)
        self.n += 1
        out = self.learning.evaluate_conclusion(request_id="eval-%d" % self.n, conclusion_id=cid)
        self.assertEqual(out["status"], "candidate")
        self.assertIsNone(out["suggestion"])

        e2 = self.make_experience(2)
        self.add_evidence(e2, cid)
        self.n += 1
        out = self.learning.evaluate_conclusion(request_id="eval-%d" % self.n, conclusion_id=cid)
        self.assertEqual(out["status"], "active")
        self.assertIsNotNone(out["suggestion"])
        suggestion = out["suggestion"]
        self.assertEqual(suggestion["status"], "pending")

    def test_child_suggestion_unique_per_window(self):
        conclusion = self.make_conclusion()
        cid = conclusion["conclusion_id"]
        for i in (1, 2):
            self.add_evidence(self.make_experience(i), cid)
        self.n += 1
        first = self.learning.evaluate_conclusion(request_id="eval-%d" % self.n, conclusion_id=cid)
        self.n += 1
        second = self.learning.evaluate_conclusion(request_id="eval-%d" % self.n, conclusion_id=cid)
        self.assertEqual(first["suggestion"]["suggestion_id"], second["suggestion"]["suggestion_id"])

    def test_against_blocks_suggestion(self):
        conclusion = self.make_conclusion()
        cid = conclusion["conclusion_id"]
        self.add_evidence(self.make_experience(1), cid, "support")
        self.add_evidence(self.make_experience(2), cid, "against")
        self.n += 1
        out = self.learning.evaluate_conclusion(request_id="eval-%d" % self.n, conclusion_id=cid)
        self.assertEqual(out["status"], "candidate")
        self.assertIsNone(out["suggestion"])

    def test_revoke_evidence(self):
        conclusion = self.make_conclusion()
        cid = conclusion["conclusion_id"]
        exp = self.make_experience(1)
        self.add_evidence(exp, cid)
        with self.db.transaction() as conn:
            self.learning.revoke_evidence_for_experience(conn, experience_id=exp["experience_id"])
        count = self.db.conn.execute(
            "SELECT COUNT(*) AS n FROM conclusion_evidence WHERE conclusion_id = ? AND revoked_at IS NULL",
            (cid,),
        ).fetchone()["n"]
        self.assertEqual(count, 0)

    def test_propose_conclusion_not_formal(self):
        proposal = self.learning.propose_conclusion(
            workspace_id="ws-1", statement="候选", source_experience_id="exp-x"
        )
        self.assertTrue(proposal["proposal_id"])
        self.assertEqual(self.learning.pending_suggestions(), [])
        self.assertIsNone(self.learning.get_conclusion(proposal["proposal_id"]))

    def test_revoke_suggestion(self):
        conclusion = self.make_conclusion()
        cid = conclusion["conclusion_id"]
        for i in (1, 2):
            self.add_evidence(self.make_experience(i), cid)
        self.n += 1
        out = self.learning.evaluate_conclusion(request_id="eval-%d" % self.n, conclusion_id=cid)
        sid = out["suggestion"]["suggestion_id"]
        self.n += 1
        self.learning.revoke_suggestion(request_id="rev-%d" % self.n, suggestion_id=sid, reason="test")
        self.assertEqual(self.learning.pending_suggestions(), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
