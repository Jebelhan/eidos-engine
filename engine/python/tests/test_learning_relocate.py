"""E4-C2-2 学习证据重关联测试：冲突保留、canonical 去重、异常身份。"""

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

WS = "ws-c22"


class LearningRelocateTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dsh-c22-")
        self.db = Database(os.path.join(self.tmp, "test.sqlite"))
        self.db.migrate()
        self.audit = AuditLog(self.db)
        self.dedup = RequestDedup(self.db)
        self.outbox = Outbox(self.db)
        self.memory = MemoryService(self.db, self.dedup, self.audit, self.outbox)
        self.learning = LearningService(self.db, self.dedup, self.audit, self.memory, self.outbox)
        self.n = 0
        self.conclusion_id = self.learning.create_conclusion(
            request_id="c-1", workspace_id=WS, statement="c22 结论"
        )["conclusion_id"]

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def exp(self, i, status="verified"):
        self.n += 1
        created = self.memory.create_experience(
            request_id="e-%d" % self.n, workspace_id=WS, session_id="s", subject_id=None,
            event_ids=["ev-%d" % i], experience_type="conversation_turn",
            occurred_at=int(time.time() * 1000), description="d",
        )
        experience_id = created["experience_id"]
        if status != "pending":
            self.memory.verify_experience(
                request_id="v-%d" % self.n, experience_id=experience_id,
                target_status=status, reason="t",
            )
        return experience_id

    def add_evidence(self, experience_id, polarity="support"):
        self.n += 1
        return self.learning.add_conclusion_evidence(
            request_id="ev-%d" % self.n, workspace_id=WS,
            conclusion_id=self.conclusion_id, experience_id=experience_id,
            source_event_ids=[], polarity=polarity,
        )

    def relocate(self, source, canonical, version=1):
        self.n += 1
        return self.learning.relocate_conclusion_evidence_for_merge(
            request_id="rel-%d" % self.n, operation_id="op-%d" % self.n,
            workspace_id=WS, source_experience_id=source,
            canonical_experience_id=canonical, mapping_version=version,
        )

    def evidence_count(self):
        return self.db.conn.execute("SELECT COUNT(*) FROM conclusion_evidence").fetchone()[0]

    # ---------- 迁移 ----------

    def test_relocate_no_conflict(self):
        a = self.exp(1)
        c = self.exp(2)
        self.add_evidence(a)
        result = self.relocate(a, c)
        self.assertEqual(result["relocated"], 1)
        row = self.db.conn.execute(
            "SELECT experience_id FROM conclusion_evidence WHERE conclusion_id = ?", (self.conclusion_id,)
        ).fetchone()
        self.assertEqual(row["experience_id"], c)

    def test_relocate_conflict_keeps_original(self):
        a = self.exp(1)
        c = self.exp(2)
        self.add_evidence(a)
        self.add_evidence(c)  # 目标键已占用
        result = self.relocate(a, c)
        self.assertEqual(result["relocated"], 0)
        self.assertEqual(len(result["conflict_kept"]), 1)
        self.assertEqual(self.evidence_count(), 2)  # 不删
        audits = self.db.conn.execute(
            "SELECT COUNT(*) FROM audit_records WHERE operation = 'conclusion_evidence.conflict_kept'"
        ).fetchone()[0]
        self.assertEqual(audits, 1)

    def test_relocate_conflict_with_revoked_target(self):
        a = self.exp(1)
        c = self.exp(2)
        self.add_evidence(a)
        kept = self.add_evidence(c)
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE conclusion_evidence SET revoked_at = ? WHERE evidence_id = ?",
                (int(time.time() * 1000), kept["evidence_id"]),
            )
        result = self.relocate(a, c)
        self.assertEqual(result["relocated"], 0)  # 已撤销的目标仍占唯一键
        self.assertEqual(len(result["conflict_kept"]), 1)

    def test_relocate_idempotent(self):
        a = self.exp(1)
        c = self.exp(2)
        self.add_evidence(a)
        self.relocate(a, c)
        second = self.relocate(a, c)
        self.assertEqual(second["relocated"], 0)

    # ---------- canonical 计数 ----------

    def test_canonical_dedup_counts_once(self):
        a = self.exp(1)
        c = self.exp(2)
        self.add_evidence(a)
        self.add_evidence(c)
        # 合并 A→C 但冲突保留（两条记录都在）
        self.relocate(a, c)
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE memory_experience SET canonical_experience_id = ? WHERE experience_id = ?",
                (c, a),
            )
        result = self.learning.evaluate_conclusion(request_id="eval-1", conclusion_id=self.conclusion_id)
        self.assertEqual(result["support"], 1)  # 同一真实经历只计一次

    def test_split_terminal_not_counted(self):
        a = self.exp(1)
        self.add_evidence(a)
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE memory_experience SET verification_status = 'split' WHERE experience_id = ?",
                (a,),
            )
        result = self.learning.evaluate_conclusion(request_id="eval-2", conclusion_id=self.conclusion_id)
        self.assertEqual(result["support"], 0)

    def test_anomalous_chain_not_counted(self):
        a = self.exp(1)
        self.add_evidence(a)
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE memory_experience SET canonical_experience_id = 'missing' WHERE experience_id = ?",
                (a,),
            )
        result = self.learning.evaluate_conclusion(request_id="eval-3", conclusion_id=self.conclusion_id)
        self.assertEqual(result["support"], 0)
        self.assertEqual(len(result["anomalies"]), 1)
        self.assertEqual(result["anomalies"][0]["resolution_status"], "broken_chain")


if __name__ == "__main__":
    unittest.main(verbosity=2)
