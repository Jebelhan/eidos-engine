"""P14-E2 压力衰减测试：公式、宽限期、epsilon、新证据刷新、结算联动、幂等。"""

import math
import os
import shutil
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from dsh_engine.audit import AuditLog  # noqa: E402
from dsh_engine.db import Database  # noqa: E402
from dsh_engine.idempotency import IdempotencyConflict, RequestDedup  # noqa: E402
from dsh_engine.memory import MemoryService  # noqa: E402
from dsh_engine.outbox import Outbox  # noqa: E402
from dsh_engine.personality import PersonalityService  # noqa: E402
from dsh_engine.personality_decay import PressureDecayService  # noqa: E402

BASE = 1_700_000_000_000
DAY = 86_400_000
HALF = 180.0
LN2 = math.log(2.0)


class DecayTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dsh-p14e2-")
        self.db = Database(os.path.join(self.tmp, "test.sqlite"))
        self.db.migrate()
        self.audit = AuditLog(self.db)
        self.dedup = RequestDedup(self.db)
        self.outbox = Outbox(self.db)
        self.memory = MemoryService(self.db, self.dedup, self.audit, self.outbox)
        self.personality = PersonalityService(self.db, self.dedup, self.audit, self.memory)
        self.trait = self.personality.ensure_trait()
        self.decay = PressureDecayService(self.db, self.dedup, self.audit)
        self.n = 0

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def add_instance(self, remaining, direction=1, created_at=BASE):
        self.n += 1
        iid = "inst-%d" % self.n
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO influence_instances(instance_id, trait_id, direction, raw_strength,"
                " confidence, source_weight, strength, remaining_amount, settled_amount, source_kind,"
                " source_refs, eligible_at, status, created_at, lock_version)"
                " VALUES (?, ?, ?, 0.5, 0.5, 1.0, ?, ?, 0, 'test', '{}', ?, 'active', ?, 0)",
                (iid, self.trait["trait_id"], direction, remaining, remaining, created_at, created_at),
            )
        return iid

    def decay_now(self, as_of):
        self.n += 1
        return self.decay.decay(request_id="decay-%d" % self.n, as_of=as_of)

    # ---------- 公式 ----------

    def test_grace_period_no_decay(self):
        self.add_instance(0.3)
        result = self.decay_now(BASE + 30 * DAY)
        self.assertEqual(result["decayed_instances"], 0)

    def test_just_past_grace_small_decay(self):
        self.add_instance(0.3)
        result = self.decay_now(BASE + 31 * DAY)
        self.assertEqual(result["decayed_instances"], 1)
        expected = 0.3 * math.exp(-LN2 / HALF * 1)
        self.assertAlmostEqual(result["total_decay_amount"], 0.3 - expected, places=9)

    def test_half_life(self):
        self.add_instance(0.3)
        result = self.decay_now(BASE + (30 + 180) * DAY)
        remaining = 0.3 - result["total_decay_amount"]
        self.assertAlmostEqual(remaining, 0.15, places=9)

    def test_two_half_lives(self):
        self.add_instance(0.3)
        result = self.decay_now(BASE + (30 + 360) * DAY)
        remaining = 0.3 - result["total_decay_amount"]
        self.assertAlmostEqual(remaining, 0.075, places=9)

    def test_zero_remaining_skipped(self):
        self.add_instance(0.0)
        result = self.decay_now(BASE + 400 * DAY)
        self.assertEqual(result["processed_instances"], 0)

    def test_epsilon_zeroes_small_pressure(self):
        self.add_instance(1e-9)
        result = self.decay_now(BASE + 400 * DAY)
        self.assertEqual(result["decayed_instances"], 1)  # epsilon_zero：记录一次归零衰减
        row = self.db.conn.execute(
            "SELECT remaining_amount FROM influence_instances"
        ).fetchone()
        self.assertEqual(row["remaining_amount"], 0.0)

    def test_instances_independent(self):
        self.add_instance(0.3)
        self.add_instance(0.1)
        result = self.decay_now(BASE + (30 + 180) * DAY)
        self.assertEqual(result["decayed_instances"], 2)
        rows = self.db.conn.execute(
            "SELECT remaining_amount FROM influence_instances ORDER BY instance_id"
        ).fetchall()
        self.assertAlmostEqual(rows[0]["remaining_amount"], 0.15, places=9)
        self.assertAlmostEqual(rows[1]["remaining_amount"], 0.05, places=9)

    def test_directions_independent(self):
        self.add_instance(0.3, direction=1)
        self.add_instance(0.3, direction=-1)
        result = self.decay_now(BASE + (30 + 180) * DAY)
        self.assertEqual(result["decayed_instances"], 2)

    # ---------- 幂等 ----------

    def test_idempotent_replay(self):
        self.add_instance(0.3)
        first = self.decay.decay(request_id="same-1", as_of=BASE + 200 * DAY)
        second = self.decay.decay(request_id="same-1", as_of=BASE + 200 * DAY)
        self.assertEqual(first, second)
        count = self.db.conn.execute(
            "SELECT COUNT(*) AS n FROM personality_pressure_decay_events"
        ).fetchone()["n"]
        self.assertEqual(count, 1)

    def test_conflicting_params_rejected(self):
        self.add_instance(0.3)
        self.decay.decay(request_id="same-2", as_of=BASE + 200 * DAY)
        with self.assertRaises(IdempotencyConflict):
            self.decay.decay(request_id="same-2", as_of=BASE + 300 * DAY)

    def test_event_unique_per_instance_request(self):
        iid = self.add_instance(0.3)
        self.decay.decay(request_id="same-3", as_of=BASE + 200 * DAY)
        with self.assertRaises(Exception):
            self.db.conn.execute(
                "INSERT INTO personality_pressure_decay_events(event_id, instance_id, request_id,"
                " decay_before, decay_amount, decay_after, applied_at) VALUES ('dup', ?, 'same-3', 0.3, 0.1, 0.2, 1)",
                (iid,),
            )

    # ---------- 结算联动 ----------

    def test_settle_uses_decayed_remaining(self):
        iid = self.add_instance(0.5)
        self.decay_now(BASE + (30 + 180) * DAY)
        result = self.personality.settle(request_id="settle-1", now=BASE + (30 + 180) * DAY)
        self.assertTrue(result["settled"])
        # 结算后剩余压力应为 0.15 - actual_change（衰减后的值，不是 0.3）
        remaining = self.db.conn.execute(
            "SELECT remaining_amount, settled_amount FROM influence_instances WHERE instance_id = ?", (iid,)
        ).fetchone()
        self.assertAlmostEqual(remaining["remaining_amount"] + remaining["settled_amount"], 0.25, places=9)

    def test_below_threshold_after_decay(self):
        self.add_instance(0.25)
        self.decay_now(BASE + (30 + 360) * DAY)  # 0.25 → 0.0625 < 阈值 0.2
        result = self.personality.settle(request_id="settle-2", now=BASE + (30 + 360) * DAY)
        self.assertFalse(result["settled"])
        self.assertEqual(result["reason"], "below_threshold")

    # ---------- 新证据刷新 ----------

    def _make_evidence(self, conclusion_id, exp_id, occurred_at):
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO conclusion_evidence(evidence_id, conclusion_id, experience_id, polarity,"
                " created_at) VALUES (?, ?, ?, 'support', ?)",
                ("ev-%s" % exp_id, conclusion_id, exp_id, occurred_at),
            )

    def test_refresh_same_conclusion(self):
        instance_id = self.add_instance(0.3)
        conclusion_id = "c-1"
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO learning_influence_requests(influence_request_id, suggestion_id, conclusion_id,"
                " target_trait_id, direction, raw_strength, confidence, source_weight, status, created_at,"
                " window_expires_at, updated_at) VALUES ('r-1', 's-1', ?, ?, 1, 0.5, 0.5, 1.0, 'active',"
                " ?, ?, ?)",
                (conclusion_id, self.trait["trait_id"], BASE, BASE + 30 * DAY, BASE),
            )
            conn.execute(
                "UPDATE influence_instances SET source_refs = ? WHERE instance_id = ?",
                ('{"influence_request_id": "r-1"}', instance_id),
            )
            refreshed = self.decay.refresh_anchor_for_conclusion(
                conn,
                conclusion_id=conclusion_id,
                evidence_id="ev-1",
                experience_id="exp-1",
                occurred_at=BASE + DAY,
                independence_key="k1",
                accepted_at=BASE + 2 * DAY,
            )
        self.assertEqual(refreshed, [instance_id])
        state = self.db.conn.execute(
            "SELECT anchor_at FROM personality_pressure_decay_state WHERE instance_id = ?", (instance_id,)
        ).fetchone()
        self.assertEqual(state["anchor_at"], BASE + 2 * DAY)

    def test_refresh_other_conclusion_not_touched(self):
        instance_id = self.add_instance(0.3)
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO learning_influence_requests(influence_request_id, suggestion_id, conclusion_id,"
                " target_trait_id, direction, raw_strength, confidence, source_weight, status, created_at,"
                " window_expires_at, updated_at) VALUES ('r-2', 's-2', 'c-a', ?, 1, 0.5, 0.5, 1.0, 'active',"
                " ?, ?, ?)",
                (self.trait["trait_id"], BASE, BASE + 30 * DAY, BASE),
            )
            conn.execute(
                "UPDATE influence_instances SET source_refs = ? WHERE instance_id = ?",
                ('{"influence_request_id": "r-2"}', instance_id),
            )
            refreshed = self.decay.refresh_anchor_for_conclusion(
                conn,
                conclusion_id="c-b",
                evidence_id="ev-2",
                experience_id="exp-2",
                occurred_at=BASE + DAY,
                independence_key="k2",
                accepted_at=BASE + 2 * DAY,
            )
        self.assertEqual(refreshed, [])

    def test_refresh_old_evidence_rejected(self):
        instance_id = self.add_instance(0.3)
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO learning_influence_requests(influence_request_id, suggestion_id, conclusion_id,"
                " target_trait_id, direction, raw_strength, confidence, source_weight, status, created_at,"
                " window_expires_at, updated_at) VALUES ('r-3', 's-3', 'c-c', ?, 1, 0.5, 0.5, 1.0, 'active',"
                " ?, ?, ?)",
                (self.trait["trait_id"], BASE, BASE + 30 * DAY, BASE),
            )
            conn.execute(
                "UPDATE influence_instances SET source_refs = ? WHERE instance_id = ?",
                ('{"influence_request_id": "r-3"}', instance_id),
            )
            refreshed = self.decay.refresh_anchor_for_conclusion(
                conn,
                conclusion_id="c-c",
                evidence_id="ev-3",
                experience_id="exp-3",
                occurred_at=BASE - DAY,  # 实例之前发生的旧经历
                independence_key="k3",
                accepted_at=BASE + 2 * DAY,
            )
        self.assertEqual(refreshed, [])

    def test_refresh_duplicate_evidence_skipped(self):
        instance_id = self.add_instance(0.3)
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO learning_influence_requests(influence_request_id, suggestion_id, conclusion_id,"
                " target_trait_id, direction, raw_strength, confidence, source_weight, status, created_at,"
                " window_expires_at, updated_at) VALUES ('r-4', 's-4', 'c-d', ?, 1, 0.5, 0.5, 1.0, 'active',"
                " ?, ?, ?)",
                (self.trait["trait_id"], BASE, BASE + 30 * DAY, BASE),
            )
            conn.execute(
                "UPDATE influence_instances SET source_refs = ? WHERE instance_id = ?",
                ('{"influence_request_id": "r-4"}', instance_id),
            )
            first = self.decay.refresh_anchor_for_conclusion(
                conn, conclusion_id="c-d", evidence_id="ev-4", experience_id="exp-4",
                occurred_at=BASE + DAY, independence_key="k4", accepted_at=BASE + 2 * DAY,
            )
            second = self.decay.refresh_anchor_for_conclusion(
                conn, conclusion_id="c-d", evidence_id="ev-4", experience_id="exp-4",
                occurred_at=BASE + DAY, independence_key="k4", accepted_at=BASE + 3 * DAY,
            )
        self.assertEqual(first, [instance_id])
        self.assertEqual(second, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
