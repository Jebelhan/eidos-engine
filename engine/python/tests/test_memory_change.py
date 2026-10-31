"""MEM-CHANGE-E4-01：Experience 修正/合并/拆分 与 canonical 解析测试。"""

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

WS = "ws-e4"
WS_B = "ws-other"


class MemoryChangeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dsh-e4-")
        self.db = Database(os.path.join(self.tmp, "test.sqlite"))
        self.db.migrate()
        self.audit = AuditLog(self.db)
        self.dedup = RequestDedup(self.db)
        self.outbox = Outbox(self.db)
        self.memory = MemoryService(self.db, self.dedup, self.audit, self.outbox)
        self.n = 0

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def create(self, events, status="verified", workspace=WS):
        self.n += 1
        created = self.memory.create_experience(
            request_id="create-%d" % self.n,
            workspace_id=workspace,
            session_id="s-1",
            subject_id=None,
            event_ids=events,
            experience_type="conversation_turn",
            occurred_at=1_700_000_000_000,
            description="desc %d" % self.n,
        )
        experience_id = created["experience_id"]
        if status != "pending":
            self.memory.verify_experience(
                request_id="verify-%d" % self.n, experience_id=experience_id,
                target_status=status, reason="test",
            )
        return experience_id

    def resolve(self, experience_id, workspace=WS):
        return self.memory.resolve_canonical_experience(experience_id, workspace)

    def version(self, experience_id):
        return self.db.conn.execute(
            "SELECT change_version FROM memory_experience WHERE experience_id = ?", (experience_id,)
        ).fetchone()["change_version"]

    def merge(self, sources, canonical, versions):
        self.n += 1
        return self.memory.merge_experiences(
            request_id="merge-%d" % self.n,
            source_experience_ids=sources,
            canonical_experience_id=canonical,
            expected_change_versions=versions,
        )

    def revise(self, experience_id, correction, version):
        self.n += 1
        return self.memory.revise_experience(
            request_id="revise-%d" % self.n,
            experience_id=experience_id,
            correction=correction,
            expected_change_version=version,
        )

    def split(self, source_id, specs, version):
        self.n += 1
        return self.memory.split_experience(
            request_id="split-%d" % self.n,
            source_experience_id=source_id,
            new_experience_specs=specs,
            expected_change_version=version,
        )

    # ---------- canonical 解析 ----------

    def test_resolve_ok(self):
        exp = self.create(["e-1"])
        result = self.resolve(exp)
        self.assertEqual(result["resolution_status"], "ok")
        self.assertEqual(result["canonical_experience_id"], exp)

    def test_resolve_split_terminal(self):
        exp = self.create(["e-1"])
        with self.db.transaction() as conn:
            conn.execute("UPDATE memory_experience SET verification_status = 'split' WHERE experience_id = ?", (exp,))
        result = self.resolve(exp)
        self.assertEqual(result["resolution_status"], "split_terminal")

    def test_resolve_invalid_terminal(self):
        exp = self.create(["e-1"], status="invalid")
        result = self.resolve(exp)
        self.assertEqual(result["resolution_status"], "invalid_terminal")

    def test_resolve_broken_chain(self):
        exp = self.create(["e-1"])
        with self.db.transaction() as conn:
            conn.execute("UPDATE memory_experience SET canonical_experience_id = 'missing' WHERE experience_id = ?", (exp,))
        result = self.resolve(exp)
        self.assertEqual(result["resolution_status"], "broken_chain")

    def test_resolve_cross_workspace(self):
        a = self.create(["e-1"])
        b = self.create(["e-2"], workspace=WS_B)
        with self.db.transaction() as conn:
            conn.execute("UPDATE memory_experience SET canonical_experience_id = ? WHERE experience_id = ?", (b, a))
        result = self.resolve(a, WS)
        self.assertEqual(result["resolution_status"], "cross_workspace")

    def test_resolve_cycle(self):
        a = self.create(["e-1"])
        b = self.create(["e-2"])
        with self.db.transaction() as conn:
            conn.execute("UPDATE memory_experience SET canonical_experience_id = ? WHERE experience_id = ?", (b, a))
            conn.execute("UPDATE memory_experience SET canonical_experience_id = ? WHERE experience_id = ?", (a, b))
        result = self.resolve(a)
        self.assertEqual(result["resolution_status"], "cycle_detected")

    # ---------- revise ----------

    def test_revise_ok(self):
        exp = self.create(["e-1"])
        result = self.revise(exp, {"description": "new desc"}, 0)
        self.assertTrue(result["revised"])
        self.assertEqual(self.version(exp), 1)
        row = self.db.conn.execute("SELECT description FROM memory_experience WHERE experience_id = ?", (exp,)).fetchone()
        self.assertEqual(row["description"], "new desc")
        change = self.memory.get_experience_change(result["operation_id"], WS)
        self.assertEqual(change["operation_type"], "revise")
        events = [r["event_type"] for r in self.db.conn.execute("SELECT event_type FROM outbox_events")]
        self.assertIn("experience.revised", events)

    def test_revise_rejects_non_whitelist(self):
        exp = self.create(["e-1"])
        with self.assertRaises(ValueError):
            self.revise(exp, {"verification_status": "invalid"}, 0)

    def test_revise_version_conflict(self):
        exp = self.create(["e-1"])
        with self.assertRaises(ValueError):
            self.revise(exp, {"description": "x"}, 5)

    def test_revise_merged_alias_rejected(self):
        a = self.create(["e-a"])
        b = self.create(["e-b"])
        self.merge([a], b, {a: 0, b: 0})
        with self.assertRaises(ValueError):
            self.revise(a, {"description": "x"}, 1)

    # ---------- merge ----------

    def test_merge_basic(self):
        a = self.create(["e-a"])
        b = self.create(["e-b"])
        result = self.merge([a], b, {a: 0, b: 0})
        self.assertTrue(result["merged"])
        row = self.db.conn.execute("SELECT canonical_experience_id FROM memory_experience WHERE experience_id = ?", (a,)).fetchone()
        self.assertEqual(row["canonical_experience_id"], b)
        self.assertEqual(self.resolve(a)["canonical_experience_id"], b)
        self.assertEqual(self.version(b), 1)
        events = [r["event_type"] for r in self.db.conn.execute("SELECT event_type FROM outbox_events")]
        self.assertIn("experience.merged", events)

    def test_merge_multilevel(self):
        a = self.create(["e-a"])
        b = self.create(["e-b"])
        c = self.create(["e-c"])
        self.merge([a], b, {a: 0, b: 0})
        self.merge([b], c, {b: 1, c: 0})
        result = self.resolve(a)
        self.assertEqual(result["resolution_status"], "ok")
        self.assertEqual(result["canonical_experience_id"], c)
        self.assertEqual(result["chain"], [a, b, c])

    def test_merge_cycle_rejected(self):
        a = self.create(["e-a"])
        b = self.create(["e-b"])
        self.merge([a], b, {a: 0, b: 0})  # a→b
        with self.assertRaises(ValueError):
            self.merge([b], a, {b: 1, a: 1})  # 目标 a 非权威

    def test_merge_alias_source_rejected(self):
        a = self.create(["e-a"])
        b = self.create(["e-b"])
        c = self.create(["e-c"])
        self.merge([a], b, {a: 0, b: 0})
        with self.assertRaises(ValueError):
            self.merge([a], c, {a: 1, c: 0})  # a 已是别名

    def test_merge_terminal_status_rejected(self):
        a = self.create(["e-a"], status="invalid")
        b = self.create(["e-b"])
        with self.assertRaises(ValueError):
            self.merge([a], b, {a: 1, b: 0})

    def test_merge_version_conflict(self):
        a = self.create(["e-a"])
        b = self.create(["e-b"])
        with self.assertRaises(ValueError):
            self.merge([a], b, {a: 3, b: 0})

    def test_merge_idempotent(self):
        a = self.create(["e-a"])
        b = self.create(["e-b"])
        first = self.memory.merge_experiences(
            request_id="merge-fixed", source_experience_ids=[a],
            canonical_experience_id=b, expected_change_versions={a: 0, b: 0},
        )
        second = self.memory.merge_experiences(
            request_id="merge-fixed", source_experience_ids=[a],
            canonical_experience_id=b, expected_change_versions={a: 0, b: 0},
        )
        self.assertEqual(first, second)

    # ---------- split ----------

    def test_split_two(self):
        exp = self.create(["ev-1", "ev-2"])
        result = self.split(exp, [
            {"event_ids": ["ev-1"], "description": "p1"},
            {"event_ids": ["ev-2"], "description": "p2"},
        ], 0)
        self.assertTrue(result["split"])
        self.assertEqual(len(result["target_experience_ids"]), 2)
        row = self.db.conn.execute(
            "SELECT verification_status, canonical_experience_id FROM memory_experience WHERE experience_id = ?", (exp,)
        ).fetchone()
        self.assertEqual(row["verification_status"], "split")
        self.assertEqual(row["canonical_experience_id"], exp)
        mappings = self.memory.get_experience_split_mapping(result["operation_id"], WS)
        self.assertEqual(len(mappings), 2)
        events = [r["event_type"] for r in self.db.conn.execute("SELECT event_type FROM outbox_events")]
        self.assertIn("experience.split", events)

    def test_split_three(self):
        exp = self.create(["ev-1", "ev-2", "ev-3"])
        result = self.split(exp, [
            {"event_ids": ["ev-1"]}, {"event_ids": ["ev-2"]}, {"event_ids": ["ev-3"]},
        ], 0)
        self.assertEqual(len(result["target_experience_ids"]), 3)

    def test_split_duplicate_event_rejected(self):
        exp = self.create(["ev-1", "ev-2"])
        with self.assertRaises(ValueError):
            self.split(exp, [{"event_ids": ["ev-1"]}, {"event_ids": ["ev-1"]}], 0)

    def test_split_foreign_event_rejected(self):
        exp = self.create(["ev-1", "ev-2"])
        with self.assertRaises(ValueError):
            self.split(exp, [{"event_ids": ["ev-1"]}, {"event_ids": ["ev-9"]}], 0)

    def test_split_incomplete_rejected(self):
        exp = self.create(["ev-1", "ev-2"])
        with self.assertRaises(ValueError):
            self.split(exp, [{"event_ids": ["ev-1"]}, {"event_ids": []}], 0)

    def test_split_merged_alias_rejected(self):
        a = self.create(["e-a"])
        b = self.create(["e-b"])
        self.merge([a], b, {a: 0, b: 0})
        with self.assertRaises(ValueError):
            self.split(a, [{"event_ids": ["e-a"]}, {"event_ids": ["e-b"]}], 1)

    def test_split_twice_rejected(self):
        exp = self.create(["ev-1", "ev-2"])
        self.split(exp, [{"event_ids": ["ev-1"]}, {"event_ids": ["ev-2"]}], 0)
        with self.assertRaises(ValueError):
            self.split(exp, [{"event_ids": ["ev-1"]}, {"event_ids": ["ev-2"]}], 1)


if __name__ == "__main__":
    unittest.main(verbosity=2)
