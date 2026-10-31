"""P10 关系接入测试：证据身份、幂等、乐观锁、Trust 计算、merge/split、状态机。"""

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
from dsh_engine.relationship import RelationshipService, make_evidence_key  # noqa: E402


class RelationshipTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dsh-p10-")
        self.db = Database(os.path.join(self.tmp, "test.sqlite"))
        self.db.migrate()
        self.audit = AuditLog(self.db)
        self.dedup = RequestDedup(self.db)
        self.svc = RelationshipService(self.db, self.dedup, self.audit)
        self.n = 0

        # 构造关系：counterpart + relationship（身份键必须是稳定 namespace:key）
        res = self.svc.create_counterpart(
            request_id="cp-1", identity_type="counterpart", identity_key="qq:c-1", display_name="测试对象"
        )
        self.counterpart_id = res["counterpart_id"]
        rel = self.svc.ensure_relationship(workspace_id="ws-1", counterpart_id=self.counterpart_id)
        self.relationship_id = rel["relationship_id"]

        # 构造一条记忆经历（occurred_at 用于 rank）
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO memory_experience(experience_id, workspace_id, session_id, experience_type,"
                " occurred_at, created_at, description, verification_status, canonical_experience_id,"
                " independence_key, correction_status) VALUES (?, 'ws-1', 's-1', 'conversation_turn',"
                " 1000, 1000, 'test', 'verified', ?, 'k1', 'normal')",
                ("exp-1", "exp-1"),
            )

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def submit(self, *, events=None, signal=0.5, strength="medium", experience="exp-1", slot="0", request_id=None):
        self.n += 1
        rel = self.svc.get_trust(self.relationship_id)
        return self.svc.submit_trust_evidence(
            request_id=request_id or "ev-%d" % self.n,
            relationship_id=self.relationship_id,
            source_experience_id=experience,
            source_event_ids=events or ["e1"],
            event_slot=slot,
            trust_signal=signal,
            evidence_strength=strength,
            expected_revision=rel["revision"],
        )

    # ---------- 身份 ----------

    def test_resolve_counterpart(self):
        res = self.svc.resolve_counterpart("counterpart", "qq:c-1")
        self.assertTrue(res["found"])
        self.assertEqual(res["counterpart_id"], self.counterpart_id)

    def test_unstable_identity_key_rejected(self):
        with self.assertRaises(ValueError):
            self.svc.resolve_counterpart("counterpart", "显示名样本")
        with self.assertRaises(ValueError):
            self.svc.create_counterpart(
                request_id="cp-bad", identity_type="counterpart", identity_key="显示名样本", display_name="显示名样本"
            )

    def test_same_display_name_does_not_merge(self):
        a = self.svc.create_counterpart(
            request_id="cp-a", identity_type="counterpart", identity_key="qq:alice", display_name="同名"
        )
        b = self.svc.create_counterpart(
            request_id="cp-b", identity_type="counterpart", identity_key="qq:bob", display_name="同名"
        )
        self.assertNotEqual(a["counterpart_id"], b["counterpart_id"])

    def test_rename_updates_display_only(self):
        renamed = self.svc.update_display_name(counterpart_id=self.counterpart_id, display_name="新名字")
        self.assertEqual(renamed["display_name"], "新名字")
        row = self.db.conn.execute(
            "SELECT identity_key, display_name FROM counterpart WHERE counterpart_id = ?",
            (self.counterpart_id,),
        ).fetchone()
        self.assertEqual(row["identity_key"], "qq:c-1")
        self.assertEqual(row["display_name"], "新名字")
        # 改名后仍能按原身份键解析到同一 counterpart
        self.assertTrue(self.svc.resolve_counterpart("counterpart", "qq:c-1")["found"])

    def test_invalid_identity_type_rejected(self):
        with self.assertRaises(ValueError):
            self.svc.resolve_counterpart("dsh_local", "local_principal")

    # ---------- 证据身份 ----------

    def test_evidence_key_stable(self):
        key1 = make_evidence_key("ws-1", "rel-1", "0", ["e2", "e1", "e1"])
        key2 = make_evidence_key("ws-1", "rel-1", "0", ["e1", "e2"])
        self.assertEqual(key1, key2)
        key3 = make_evidence_key("ws-1", "rel-1", "1", ["e1", "e2"])
        self.assertNotEqual(key1, key3)

    def test_submit_and_trust(self):
        result = self.submit(signal=1.0)
        self.assertTrue(result["created"])
        trust = self.svc.get_trust(self.relationship_id)
        # prior=2.0, W_e=min(0.5,1)=0.5, S=1 → (1.0 + 0.5)/2.5 = 60
        self.assertAlmostEqual(trust["trust_value"], 60.0, places=3)
        self.assertGreater(trust["confidence"], 0)

    def test_negative_signal_lowers_trust(self):
        self.submit(signal=-0.5)
        trust = self.svc.get_trust(self.relationship_id)
        self.assertLess(trust["trust_value"], 50.0)

    def test_idempotent_same_evidence(self):
        first = self.submit(events=["e1"])
        trust = self.svc.get_trust(self.relationship_id)
        # 不同 request_id、同事件集合 → 命中原证据
        second = self.svc.submit_trust_evidence(
            request_id="dup-1",
            relationship_id=self.relationship_id,
            source_experience_id="exp-1",
            source_event_ids=["e1"],
            trust_signal=0.5,
            evidence_strength="medium",
            expected_revision=trust["revision"],
        )
        self.assertEqual(first["evidence_id"], second["evidence_id"])

    def test_revision_conflict(self):
        self.submit()
        with self.assertRaises(ValueError):
            self.svc.submit_trust_evidence(
                request_id="stale-1",
                relationship_id=self.relationship_id,
                source_experience_id="exp-1",
                source_event_ids=["e2"],
                trust_signal=0.5,
                evidence_strength="medium",
                expected_revision=0,
            )

    def test_source_events_frozen(self):
        result = self.submit(events=["e1"])
        with self.assertRaises(Exception):
            with self.db.transaction() as conn:
                conn.execute(
                    "DELETE FROM trust_evidence_source_events WHERE evidence_id = ?",
                    (result["evidence_id"],),
                )

    # ---------- merge / split ----------

    def test_merge_keeps_evidence_key(self):
        result = self.submit(events=["e1"])
        before = self.db.conn.execute(
            "SELECT evidence_key FROM trust_evidence WHERE evidence_id = ?", (result["evidence_id"],)
        ).fetchone()["evidence_key"]
        with self.db.transaction() as conn:
            self.svc.relocate_for_merge(conn, canonical_experience_id="exp-merged", merged_from=["exp-1"])
        row = self.db.conn.execute(
            "SELECT evidence_key, source_experience_id FROM trust_evidence WHERE evidence_id = ?",
            (result["evidence_id"],),
        ).fetchone()
        self.assertEqual(row["evidence_key"], before)
        self.assertEqual(row["source_experience_id"], "exp-merged")

    def test_split_full_ownership_relocates(self):
        result = self.submit(events=["e1", "e2"])
        with self.db.transaction() as conn:
            out = self.svc.relocate_for_split(
                conn,
                split_experience_id="exp-1",
                new_experiences=[{"experience_id": "exp-b", "event_ids": ["e1", "e2"]}],
            )
        self.assertEqual(out["relocated"], 1)
        row = self.db.conn.execute(
            "SELECT source_experience_id, evidence_status FROM trust_evidence WHERE evidence_id = ?",
            (result["evidence_id"],),
        ).fetchone()
        self.assertEqual(row["source_experience_id"], "exp-b")
        self.assertEqual(row["evidence_status"], "active")

    def test_split_partial_ownership_goes_pending(self):
        result = self.submit(events=["e1", "e2"])
        with self.db.transaction() as conn:
            out = self.svc.relocate_for_split(
                conn,
                split_experience_id="exp-1",
                new_experiences=[
                    {"experience_id": "exp-b", "event_ids": ["e1"]},
                    {"experience_id": "exp-c", "event_ids": ["e3"]},
                ],
            )
        self.assertEqual(out["pending_reassessment"], 1)
        row = self.db.conn.execute(
            "SELECT evidence_status FROM trust_evidence WHERE evidence_id = ?", (result["evidence_id"],)
        ).fetchone()
        self.assertEqual(row["evidence_status"], "pending_reassessment")

    def test_split_ambiguous_goes_pending(self):
        result = self.submit(events=["e1", "e2"])
        with self.db.transaction() as conn:
            out = self.svc.relocate_for_split(
                conn,
                split_experience_id="exp-1",
                new_experiences=[
                    {"experience_id": "exp-b", "event_ids": ["e1", "e2"]},
                    {"experience_id": "exp-c", "event_ids": ["e1", "e2"]},
                ],
            )
        self.assertEqual(out["pending_reassessment"], 1)

    def test_invalidate(self):
        result = self.submit()
        with self.db.transaction() as conn:
            self.svc.invalidate_for_experience(conn, canonical_experience_id="exp-1")
        row = self.db.conn.execute(
            "SELECT evidence_status FROM trust_evidence WHERE evidence_id = ?", (result["evidence_id"],)
        ).fetchone()
        self.assertEqual(row["evidence_status"], "invalidated")
        trust = self.svc.get_trust(self.relationship_id)
        self.assertAlmostEqual(trust["trust_value"], 50.0, places=3)


if __name__ == "__main__":
    unittest.main(verbosity=2)
