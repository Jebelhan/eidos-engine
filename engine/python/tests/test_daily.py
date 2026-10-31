"""P14-D 每日周期账本测试：创建幂等、认领、阶段、失败、恢复、完成。"""

import os
import shutil
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from dsh_engine.audit import AuditLog  # noqa: E402
from dsh_engine.daily import DailyCycleService  # noqa: E402
from dsh_engine.db import Database  # noqa: E402


class DailyCycleTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dsh-p14d-")
        self.db = Database(os.path.join(self.tmp, "test.sqlite"))
        self.db.migrate()
        self.audit = AuditLog(self.db)
        self.daily = DailyCycleService(self.db, self.audit)

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def begin(self, date="2026-10-31"):
        return self.daily.ensure_cycle(
            timezone="Asia/Shanghai", schedule_date=date, scheduled_at=1793440000000
        )

    def test_ensure_cycle_idempotent(self):
        first = self.begin()
        second = self.begin()
        self.assertTrue(first["created"])
        self.assertFalse(second["created"])
        self.assertEqual(first["cycle"]["cycle_id"], second["cycle"]["cycle_id"])

    def test_claim_once(self):
        cycle = self.begin()["cycle"]
        self.assertTrue(self.daily.claim(cycle["cycle_id"]))
        self.assertFalse(self.daily.claim(cycle["cycle_id"]))

    def test_steps_created(self):
        cycle = self.begin()["cycle"]
        status = self.daily.status(cycle["cycle_id"])
        self.assertEqual(len(status["steps"]), 8)
        self.assertEqual(status["steps"][0]["step_name"], "memory_curation")

    def test_step_lifecycle(self):
        cycle_id = self.begin()["cycle"]["cycle_id"]
        self.daily.step_begin(cycle_id, "memory_curation")
        self.daily.step_complete(cycle_id, "memory_curation")
        status = self.daily.status(cycle_id)
        step = next(s for s in status["steps"] if s["step_name"] == "memory_curation")
        self.assertEqual(step["status"], "completed")
        self.assertEqual(step["attempts"], 1)

    def test_fail_exhausts(self):
        cycle_id = self.begin()["cycle"]["cycle_id"]
        self.daily.claim(cycle_id)
        for _ in range(3):
            self.daily.step_begin(cycle_id, "memory_curation")
            result = self.daily.step_fail(cycle_id, "memory_curation", "boom")
        self.assertTrue(result["exhausted"])
        cycle = self.daily.find("Asia/Shanghai", "2026-10-31")
        self.assertEqual(cycle["status"], "failed")

    def test_recover_resets_running_step(self):
        cycle_id = self.begin()["cycle"]["cycle_id"]
        self.daily.claim(cycle_id)
        self.daily.step_begin(cycle_id, "memory_curation")
        self.daily.recover(cycle_id)
        status = self.daily.status(cycle_id)
        step = next(s for s in status["steps"] if s["step_name"] == "memory_curation")
        self.assertEqual(step["status"], "pending")
        self.assertEqual(status["cycle"]["status"], "recovery_pending")

    def test_complete(self):
        cycle_id = self.begin()["cycle"]["cycle_id"]
        self.daily.claim(cycle_id)
        self.daily.complete(cycle_id, {"ok": True})
        cycle = self.daily.find("Asia/Shanghai", "2026-10-31")
        self.assertEqual(cycle["status"], "completed")
        self.assertIsNotNone(cycle["completed_at"])

    def test_same_day_unique(self):
        self.begin("2026-10-31")
        other = self.begin("2026-10-31")
        self.assertFalse(other["created"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
