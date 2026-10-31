"""P14-C 人格结算测试：屈服、边界、分摊、幂等、方向交替暂缓。"""

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
from dsh_engine.personality import PersonalityService  # noqa: E402

BASE = 1_700_000_000_000


class SettlementTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dsh-p14c-")
        self.db = Database(os.path.join(self.tmp, "test.sqlite"))
        self.db.migrate()
        self.audit = AuditLog(self.db)
        self.dedup = RequestDedup(self.db)
        self.outbox = Outbox(self.db)
        self.memory = MemoryService(self.db, self.dedup, self.audit, self.outbox)
        self.personality = PersonalityService(self.db, self.dedup, self.audit, self.memory)
        self.trait = self.personality.ensure_trait()
        self.trait_id = self.trait["trait_id"]
        self.n = 0

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def add_instance(self, direction=1, remaining=0.3, eligible_at=BASE):
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO influence_instances(instance_id, trait_id, direction, raw_strength,"
                " confidence, source_weight, strength, remaining_amount, settled_amount, source_kind,"
                " source_refs, eligible_at, status, created_at)"
                " VALUES (?, ?, ?, 0.5, 0.5, 1.0, ?, ?, 0, 'test', '{}', ?, 'active', ?)",
                ("inst-%d" % (self.n + 1), self.trait_id, direction, remaining * 2, remaining,
                 eligible_at, BASE),
            )
            self.n += 1

    def settle(self, now=BASE + 1000):
        self.n += 1
        return self.personality.settle(request_id="settle-%d" % self.n, now=now)

    def set_trait(self, value, **kwargs):
        with self.db.transaction() as conn:
            fields = ", ".join("%s = ?" % k for k in kwargs)
            conn.execute(
                "UPDATE personality_traits SET value = ?%s WHERE trait_id = ?" % (", " + fields if fields else ""),
                (value, *kwargs.values(), self.trait_id),
            )

    # ---------- 屈服 ----------

    def test_release_moves_value_up(self):
        self.add_instance(1, 0.3)
        result = self.settle()
        self.assertTrue(result["settled"])
        self.assertGreater(result["new_value"], result["old_value"])
        self.assertEqual = None  # noqa
        self.assertGreater(result["actual_change"], 0)

    def test_no_instances_no_change(self):
        result = self.settle()
        self.assertFalse(result["settled"])
        self.assertEqual(result["reason"], "below_threshold")

    def test_below_threshold_no_change(self):
        self.add_instance(1, 0.05)
        result = self.settle()
        self.assertFalse(result["settled"])

    def test_negative_release_moves_value_down(self):
        self.add_instance(-1, 0.3)
        result = self.settle()
        self.assertTrue(result["settled"])
        self.assertLess(result["new_value"], result["old_value"])

    # ---------- 边界 ----------

    def test_boundary_caps_release(self):
        self.set_trait(0.99)
        self.add_instance(1, 0.5)
        result = self.settle()
        self.assertTrue(result["settled"])
        self.assertLessEqual(result["new_value"], 1.0)
        self.assertAlmostEqual(result["new_value"], 0.99 + result["actual_change"], places=9)

    def test_dead_zone_no_change(self):
        self.set_trait(1.0)
        self.add_instance(1, 0.5)
        result = self.settle()
        self.assertFalse(result["settled"])
        self.assertEqual(result["reason"], "boundary_reached")

    # ---------- 分摊 ----------

    def test_allocation_sum_equals_release(self):
        self.add_instance(1, 0.2)
        self.add_instance(1, 0.3)
        result = self.settle()
        alloc_sum = sum(a["share"] for a in result["allocations"])
        self.assertAlmostEqual(alloc_sum, result["actual_change"], places=9)
        for alloc in result["allocations"]:
            self.assertGreaterEqual(alloc["share"], 0)

    def test_remaining_monotonic(self):
        self.add_instance(1, 0.4)
        before = self.db.conn.execute(
            "SELECT remaining_amount FROM influence_instances"
        ).fetchone()["remaining_amount"]
        self.settle()
        after = self.db.conn.execute(
            "SELECT remaining_amount FROM influence_instances"
        ).fetchone()["remaining_amount"]
        self.assertLessEqual(after, before)

    # ---------- 幂等 ----------

    def test_idempotent(self):
        self.add_instance(1, 0.3)
        first = self.personality.settle(request_id="same-1", now=BASE + 1000)
        second = self.personality.settle(request_id="same-1", now=BASE + 1000)
        self.assertEqual(first, second)
        count = self.db.conn.execute("SELECT COUNT(*) AS n FROM personality_settlements").fetchone()["n"]
        self.assertEqual(count, 1)

    # ---------- 方向交替 ----------

    def test_direction_alternation_hold(self):
        self.add_instance(1, 0.5)
        first = self.settle()
        self.assertTrue(first["settled"])
        self.assertEqual(first["selected_direction"], 1)
        # 清掉正压力，只留负压力，下一周期方向相反 → 强制暂缓
        with self.db.transaction() as conn:
            conn.execute("UPDATE influence_instances SET remaining_amount = 0, status = 'settled'")
        self.add_instance(-1, 0.5)
        second = self.settle()
        self.assertFalse(second["settled"])
        self.assertEqual(second["reason"], "direction_alternation_hold")
        # 再下一周期（hold 已清）应正常结算
        third = self.settle()
        self.assertTrue(third["settled"])
        self.assertEqual(third["selected_direction"], -1)

    # ---------- 记录 ----------

    def test_settlement_recorded(self):
        self.add_instance(1, 0.3)
        self.settle()
        row = self.db.conn.execute("SELECT * FROM personality_settlements").fetchone()
        self.assertEqual(row["selected_direction"], 1)
        self.assertGreater(row["release"], 0)
        self.assertIn("instance_id", row["instance_allocations"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
