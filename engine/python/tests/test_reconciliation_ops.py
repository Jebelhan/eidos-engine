"""P14-E3 人工处理接口协议级测试：actor/reason 校验、Audit、幂等、事务。"""

import json
import os
import shutil
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

TMP = tempfile.mkdtemp(prefix="dsh-e3ops-")
os.environ["DSH_ENGINE_DATA_DIR"] = TMP

import main  # noqa: E402


class IssueOpsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        main.ENGINE.init()
        cls.db = main.ENGINE.db

    @classmethod
    def tearDownClass(cls):
        cls.db.close()
        shutil.rmtree(TMP, ignore_errors=True)

    def setUp(self):
        self.issue_id = "i-%s" % self._testMethodName
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO outbox_reconciliation_issues(issue_id, run_id, fingerprint, issue_type,"
                " severity, event_id, description, detected_at, last_seen_at, issue_status)"
                " VALUES (?, 'r-1', ?, 'x', 'P2', 'ev-1', 't', 1, 1, 'open')",
                (self.issue_id, "fp-%s" % self._testMethodName),
            )

    def _audits(self, operation):
        return self.db.conn.execute(
            "SELECT * FROM audit_records WHERE operation = ? AND target_id = ?",
            (operation, self.issue_id),
        ).fetchall()

    def _status(self):
        return self.db.conn.execute(
            "SELECT issue_status FROM outbox_reconciliation_issues WHERE issue_id = ?",
            (self.issue_id,),
        ).fetchone()["issue_status"]

    def test_requires_actor(self):
        with self.assertRaises(ValueError):
            main.op_acknowledge_issue("req-1", {"issue_id": self.issue_id, "reason": "known"})

    def test_requires_reason(self):
        with self.assertRaises(ValueError):
            main.op_acknowledge_issue("req-2", {"issue_id": self.issue_id, "actor": "tester"})

    def test_acknowledge_writes_audit(self):
        response = main.op_acknowledge_issue(
            "req-3", {"issue_id": self.issue_id, "actor": "tester", "reason": "known"}
        )
        self.assertEqual(response["status"], "success")
        self.assertEqual(self._status(), "acknowledged")
        audits = self._audits("issue.acknowledge")
        self.assertEqual(len(audits), 1)
        before = json.loads(audits[0]["before_snapshot"])
        after = json.loads(audits[0]["after_snapshot"])
        self.assertEqual(before["issue_status"], "open")
        self.assertEqual(after["issue_status"], "acknowledged")
        self.assertEqual(after["actor"], "tester")
        self.assertEqual(after["reason"], "known")

    def test_repeat_acknowledge_idempotent(self):
        main.op_acknowledge_issue("req-4", {"issue_id": self.issue_id, "actor": "tester", "reason": "known"})
        second = main.op_acknowledge_issue(
            "req-5", {"issue_id": self.issue_id, "actor": "tester", "reason": "known"}
        )
        self.assertTrue(second["data"]["already"])
        self.assertEqual(len(self._audits("issue.acknowledge")), 1)  # 幂等不重复写 Audit

    def test_ignore_writes_audit(self):
        response = main.op_ignore_issue(
            "req-6", {"issue_id": self.issue_id, "actor": "tester", "reason": "noise"}
        )
        self.assertEqual(response["data"]["issue_status"], "ignored")
        self.assertEqual(self._status(), "ignored")
        self.assertEqual(len(self._audits("issue.ignore")), 1)

    def test_unknown_issue_rejected(self):
        with self.assertRaises(KeyError):
            main.op_acknowledge_issue("req-7", {"issue_id": "missing", "actor": "tester", "reason": "x"})


if __name__ == "__main__":
    unittest.main(verbosity=2)
