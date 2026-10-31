"""P4 记忆模块测试：Experience 创建、事件关联、去重、摘要、隔离、审计、Outbox。"""

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
from dsh_engine.memory import MemoryService  # noqa: E402
from dsh_engine.outbox import Outbox  # noqa: E402


class MemoryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dsh-p4-")
        self.db = Database(os.path.join(self.tmp, "test.sqlite"))
        self.db.migrate()
        self.dedup = RequestDedup(self.db)
        self.audit = AuditLog(self.db)
        self.outbox = Outbox(self.db)
        self.memory = MemoryService(self.db, self.dedup, self.audit, self.outbox)
        self.n = 0

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def create(self, *, workspace="ws-1", session="s-1", events=None, description="test turn"):
        self.n += 1
        return self.memory.create_experience(
            request_id="req-%d" % self.n,
            workspace_id=workspace,
            session_id=session,
            event_ids=events if events is not None else ["ev-%d" % self.n],
            experience_type="conversation_turn",
            occurred_at=1000 + self.n,
            subject_id=None,
            description=description,
        )

    # ---------- 创建 ----------

    def test_create_pending_with_canonical_and_key(self):
        result = self.create()
        self.assertTrue(result["created"])
        exp = self.memory.get_experience(result["experience_id"])
        self.assertEqual(exp["verification_status"], "pending")
        self.assertEqual(exp["canonical_experience_id"], exp["experience_id"])
        self.assertTrue(exp["independence_key"])
        self.assertEqual(exp["event_ids"], ["ev-1"])

    def test_event_links_unique(self):
        result = self.create(events=["e-a", "e-b", "e-a"])
        exp = self.memory.get_experience(result["experience_id"])
        self.assertEqual(sorted(exp["event_ids"]), ["e-a", "e-b"])

    def test_duplicate_event_set_returns_existing(self):
        first = self.create(events=["e-1", "e-2"])
        second = self.memory.create_experience(
            request_id="req-dup",
            workspace_id="ws-1",
            session_id="s-1",
            event_ids=["e-2", "e-1"],
            experience_type="conversation_turn",
            occurred_at=9999,
            subject_id=None,
            description="dup",
        )
        self.assertFalse(second["created"])
        self.assertTrue(second.get("deduplicated"))
        self.assertEqual(second["experience_id"], first["experience_id"])
        total = self.db.conn.execute("SELECT COUNT(*) AS n FROM memory_experience").fetchone()["n"]
        self.assertEqual(total, 1)

    def test_partial_overlap_is_not_merged(self):
        self.create(events=["e-1", "e-2"])
        result = self.create(events=["e-2", "e-3"])
        self.assertFalse(result["created"])
        self.assertTrue(result.get("possible_duplicate"))
        self.assertEqual(len(result["matches"]), 1)
        self.assertEqual(self.db.conn.execute("SELECT COUNT(*) AS n FROM memory_experience").fetchone()["n"], 1)

    def test_idempotent_on_request_id(self):
        first = self.create(events=["e-x"])
        # 相同 request_id + 相同 payload 返回原结果（不重复建）
        second = self.memory.create_experience(
            request_id="req-1",
            workspace_id="ws-1",
            session_id="s-1",
            event_ids=["e-x"],
            experience_type="conversation_turn",
            occurred_at=1001,
            subject_id=None,
            description="ignored for dedup",
        )
        self.assertEqual(first["experience_id"], second["experience_id"])

    def test_same_event_set_across_sessions(self):
        """session_id 只是创建上下文：同一事件集合从别的会话重放，返回同一经历。"""
        first = self.create(session="s-a", events=["e-same"])
        second = self.memory.create_experience(
            request_id="cross-session",
            workspace_id="ws-1",
            session_id="s-b",
            event_ids=["e-same"],
            experience_type="conversation_turn",
            occurred_at=2,
            subject_id=None,
            description="same event, other session",
        )
        self.assertEqual(first["experience_id"], second["experience_id"])
        exp = self.memory.get_experience(first["experience_id"])
        self.assertEqual(exp["session_id"], "s-a", "创建上下文不被后来的会话覆盖")

    def test_workspace_session_composite_index(self):
        rows = self.db.conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='memory_experience_ws_session_idx'"
        ).fetchall()
        self.assertEqual(len(rows), 1)

    # ---------- 摘要与隔离 ----------

    def test_summary_counts(self):
        self.create(session="s-1", events=["e1"])
        self.create(session="s-1", events=["e2"])
        summary = self.memory.summary("ws-1", "s-1")
        self.assertEqual(summary["total"], 2)
        self.assertEqual(summary["pending"], 2)
        self.assertEqual(summary["verified"], 0)
        self.assertEqual(summary["source"], "memory")

    def test_summary_isolated_by_workspace_and_session(self):
        self.create(workspace="ws-1", session="s-1", events=["e1"])
        self.create(workspace="ws-1", session="s-2", events=["e2"])
        self.create(workspace="ws-2", session="s-1", events=["e3"])
        self.assertEqual(self.memory.summary("ws-1", "s-1")["total"], 1)
        self.assertEqual(self.memory.summary("ws-1", "s-2")["total"], 1)
        self.assertEqual(self.memory.summary("ws-2", "s-1")["total"], 1)
        self.assertEqual(self.memory.summary("ws-1")["total"], 2)

    # ---------- 审计与事件 ----------

    def test_audit_written(self):
        result = self.create()
        rows = self.audit.query(target_type="experience", target_id=result["experience_id"])
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["operation"], "experience.create")

    def test_outbox_event_written(self):
        result = self.create()
        rows = self.db.conn.execute(
            "SELECT * FROM outbox_events WHERE aggregate_id = ?", (result["experience_id"],)
        ).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["event_type"], "experience.created")
        self.assertEqual(rows[0]["status"], "pending")
        payload = rows[0]["payload"]
        self.assertIn(result["experience_id"], payload)

    def test_transaction_consistency(self):
        # 创建失败（空事件）时不留 experience / link / outbox。
        with self.assertRaises(ValueError):
            self.memory.create_experience(
                request_id="req-empty",
                workspace_id="ws-1",
                session_id="s-1",
                event_ids=[],
                experience_type="conversation_turn",
                occurred_at=1,
                subject_id=None,
                description="empty",
            )
        self.assertEqual(self.db.conn.execute("SELECT COUNT(*) AS n FROM memory_experience").fetchone()["n"], 0)
        self.assertEqual(self.db.conn.execute("SELECT COUNT(*) AS n FROM outbox_events").fetchone()["n"], 0)

    # ---------- 核验（P5 创建—核验闭环） ----------

    def verify(self, exp_id, status, request_id=None):
        self.n += 1
        return self.memory.verify_experience(
            request_id=request_id or "vreq-%d" % self.n,
            experience_id=exp_id,
            target_status=status,
        )

    def test_verify_pending_to_verified(self):
        exp = self.create()
        result = self.verify(exp["experience_id"], "verified")
        self.assertTrue(result["changed"])
        self.assertEqual(self.memory.get_experience(exp["experience_id"])["verification_status"], "verified")
        summary = self.memory.summary("ws-1", "s-1")
        self.assertEqual(summary["verified"], 1)
        self.assertEqual(summary["pending"], 0)
        audits = self.audit.query(target_type="experience", target_id=exp["experience_id"])
        self.assertTrue(any(a["operation"] == "experience.verify" for a in audits))
        events = [
            row["event_type"]
            for row in self.db.conn.execute(
                "SELECT event_type FROM outbox_events WHERE aggregate_id = ?", (exp["experience_id"],)
            )
        ]
        self.assertIn("experience.verified", events)

    def test_uncertain_then_verified(self):
        exp = self.create()
        self.verify(exp["experience_id"], "uncertain")
        self.assertEqual(self.memory.get_experience(exp["experience_id"])["verification_status"], "uncertain")
        self.verify(exp["experience_id"], "verified")
        self.assertEqual(self.memory.get_experience(exp["experience_id"])["verification_status"], "verified")

    def test_invalid_is_terminal(self):
        exp = self.create()
        self.verify(exp["experience_id"], "invalid")
        with self.assertRaises(ValueError):
            self.verify(exp["experience_id"], "verified")

    def test_illegal_verified_to_uncertain(self):
        exp = self.create()
        self.verify(exp["experience_id"], "verified")
        with self.assertRaises(ValueError):
            self.verify(exp["experience_id"], "uncertain")

    def test_verify_idempotent_same_request(self):
        exp = self.create()
        first = self.verify(exp["experience_id"], "verified", request_id="v-same")
        second = self.memory.verify_experience(
            request_id="v-same", experience_id=exp["experience_id"], target_status="verified"
        )
        self.assertEqual(first, second)
        self.assertEqual(
            self.db.conn.execute(
                "SELECT COUNT(*) AS n FROM outbox_events WHERE event_type = 'experience.verified'"
            ).fetchone()["n"],
            1,
        )

    def test_verify_already_in_target_status(self):
        exp = self.create()
        self.verify(exp["experience_id"], "verified")
        again = self.verify(exp["experience_id"], "verified")
        self.assertEqual(again["changed"], False)
        self.assertTrue(again["already"])

    def test_list_pending(self):
        self.create(events=["p1"])
        self.create(events=["p2"])
        done = self.create(events=["p3"])
        self.verify(done["experience_id"], "verified")
        pending = self.memory.list_pending("ws-1")
        self.assertEqual(len(pending), 2)
        self.assertTrue(all(item["verification_status"] == "pending" for item in pending))

    def test_check_experience_valid(self):
        exp = self.create(events=["chk-1", "chk-2"])
        result = self.memory.check_experience(
            experience_id=exp["experience_id"], source_event_ids=["chk-1"], workspace_id="ws-1"
        )
        self.assertTrue(result["valid"])
        self.assertEqual(result["canonical_experience_id"], exp["experience_id"])
        self.assertTrue(result["independence_key"])
        self.assertEqual(result["source_events"], ["chk-1", "chk-2"])

    def test_check_experience_workspace_mismatch(self):
        exp = self.create()
        result = self.memory.check_experience(
            experience_id=exp["experience_id"], source_event_ids=["ev-1"], workspace_id="ws-other"
        )
        self.assertFalse(result["valid"])
        self.assertEqual(result["reason"], "workspace_mismatch")

    def test_check_experience_rejects_invalid(self):
        exp = self.create()
        self.verify(exp["experience_id"], "invalid")
        result = self.memory.check_experience(
            experience_id=exp["experience_id"], source_event_ids=["ev-1"], workspace_id="ws-1"
        )
        self.assertFalse(result["valid"])
        self.assertEqual(result["reason"], "status_invalid")

    def test_check_experience_source_events_mismatch(self):
        exp = self.create(events=["chk-1"])
        result = self.memory.check_experience(
            experience_id=exp["experience_id"], source_event_ids=["not-linked"], workspace_id="ws-1"
        )
        self.assertFalse(result["valid"])
        self.assertEqual(result["reason"], "source_events_mismatch")

    def test_stale_evaluation_rejected(self):
        """评估开始后状态已变：旧结论必须被拒绝。"""
        exp = self.create()
        with self.assertRaises(ValueError) as ctx:
            self.memory.verify_experience(
                request_id="stale-1",
                experience_id=exp["experience_id"],
                target_status="invalid",
                expected_status="verified",
            )
        self.assertIn("stale evaluation", str(ctx.exception))
        self.assertEqual(self.memory.get_experience(exp["experience_id"])["verification_status"], "pending")

    def test_expected_status_match_succeeds(self):
        exp = self.create()
        result = self.memory.verify_experience(
            request_id="fresh-1",
            experience_id=exp["experience_id"],
            target_status="verified",
            expected_status="pending",
        )
        self.assertTrue(result["changed"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
