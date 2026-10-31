"""P14-E1 停机恢复测试：情绪补算、人格结算幂等、Daily Cycle 恢复。"""

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
from dsh_engine.emotion import EmotionService  # noqa: E402
from dsh_engine.idempotency import RequestDedup  # noqa: E402
from dsh_engine.memory import MemoryService  # noqa: E402
from dsh_engine.outbox import Outbox  # noqa: E402
from dsh_engine.personality import PersonalityService  # noqa: E402

BASE = 1_700_000_000_000
DAY = 86_400_000


class RecoveryTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dsh-p14e1-")
        self.db = Database(os.path.join(self.tmp, "test.sqlite"))
        self.db.migrate()
        self.audit = AuditLog(self.db)
        self.dedup = RequestDedup(self.db)
        self.outbox = Outbox(self.db)
        self.memory = MemoryService(self.db, self.dedup, self.audit, self.outbox)
        self.emotion = EmotionService(self.db, self.dedup, self.audit)
        self.personality = PersonalityService(self.db, self.dedup, self.audit, self.memory)
        self.daily = DailyCycleService(self.db, self.audit)
        self.n = 0

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def add_event(self, i, created_at, workspace="ws-1"):
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO emotion_events(emotion_event_id, workspace_id, source_experience_id,"
                " source_type, event_type, event_slot, description, confidence, emotion_candidates,"
                " status, created_at) VALUES (?, ?, ?, 'experience', 'positive', 0, 't', 0.8, ?, 'processed', ?)",
                ("emo-%d-%s" % (i, workspace), workspace, "exp-%d" % i,
                 '{"p": 0.5, "a": 0.2, "d": 0.1}', created_at),
            )

    def advance(self, target, workspace="ws-1"):
        self.n += 1
        return self.emotion.advance_emotion_state(
            request_id="adv-%d" % self.n, workspace_id=workspace, target_time=target
        )

    # ---------- 情绪补算 ----------

    def test_multi_day_catchup_is_segmented(self):
        # 停机 3 天：一次调用推进到 target，内部按 1h 步长分段（pad 不应一步跳到 target）。
        self.add_event(1, BASE - 3_600_000)
        result = self.advance(BASE + 3 * DAY)
        self.assertTrue(result["advanced"])
        self.assertLess(result["pad"]["p"], result["target"]["p"] + 1e-9)
        # 推进后的值应明显低于目标（低通在 72 步内仍不完全收敛）。
        self.assertGreater(result["pad"]["p"], 0.0)

    def test_restart_does_not_repeat(self):
        self.add_event(1, BASE - 3_600_000)
        first = self.advance(BASE + DAY)
        # 模拟重启（新服务实例，同一个库）
        emotion2 = EmotionService(self.db, self.dedup, self.audit)
        second = emotion2.advance_emotion_state(
            request_id="adv-restart", workspace_id="ws-1", target_time=BASE + DAY
        )
        self.assertFalse(second["advanced"])
        self.assertEqual(first["pad"], second["pad"])

    def test_workspaces_do_not_share(self):
        self.add_event(1, BASE - 3_600_000, workspace="ws-1")
        a = self.advance(BASE + DAY, workspace="ws-1")
        b = self.advance(BASE + DAY, workspace="ws-2")
        self.assertGreater(a["pad"]["p"], 0.0)
        self.assertAlmostEqual(b["pad"]["p"], 0.0, places=6)

    def test_checkpoint_written_once_per_advance(self):
        self.advance(BASE + DAY)
        self.advance(BASE + 2 * DAY)
        count = self.db.conn.execute("SELECT COUNT(*) AS n FROM emotion_checkpoints").fetchone()["n"]
        self.assertEqual(count, 2)

    # ---------- 人格补算 ----------

    def test_settle_not_repeated_on_restart(self):
        trait = self.personality.ensure_trait()
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO influence_instances(instance_id, trait_id, direction, raw_strength,"
                " confidence, source_weight, strength, remaining_amount, settled_amount, source_kind,"
                " source_refs, eligible_at, status, created_at)"
                " VALUES ('i1', ?, 1, 0.5, 0.5, 1.0, 0.3, 0.3, 0, 'test', '{}', ?, 'active', ?)",
                (trait["trait_id"], BASE, BASE),
            )
        first = self.personality.settle(request_id="cycle-1:settlement", now=BASE + DAY)
        self.assertTrue(first["settled"])
        # 重启后同一 cycle 的稳定 request_id 重放 → 不重复消耗
        second = self.personality.settle(request_id="cycle-1:settlement", now=BASE + DAY)
        self.assertEqual(first, second)
        remaining = self.db.conn.execute(
            "SELECT remaining_amount FROM influence_instances WHERE instance_id = 'i1'"
        ).fetchone()["remaining_amount"]
        self.assertAlmostEqual(remaining, 0.3 - first["actual_change"], places=9)

    def test_settle_records_consistent(self):
        trait = self.personality.ensure_trait()
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO influence_instances(instance_id, trait_id, direction, raw_strength,"
                " confidence, source_weight, strength, remaining_amount, settled_amount, source_kind,"
                " source_refs, eligible_at, status, created_at)"
                " VALUES ('i2', ?, 1, 0.5, 0.5, 1.0, 0.3, 0.3, 0, 'test', '{}', ?, 'active', ?)",
                (trait["trait_id"], BASE, BASE),
            )
        result = self.personality.settle(request_id="cycle-2:settlement", now=BASE + DAY)
        record = self.db.conn.execute("SELECT * FROM personality_settlements").fetchone()
        self.assertAlmostEqual(record["actual_change"], result["actual_change"], places=9)
        instance = self.db.conn.execute(
            "SELECT remaining_amount, settled_amount FROM influence_instances WHERE instance_id = 'i2'"
        ).fetchone()
        self.assertAlmostEqual(
            instance["remaining_amount"] + instance["settled_amount"], 0.3, places=9
        )

    # ---------- Daily Cycle 恢复 ----------

    def test_recover_keeps_attempts(self):
        cycle = self.daily.ensure_cycle(
            timezone="Asia/Shanghai", schedule_date="2026-10-31", scheduled_at=BASE
        )["cycle"]
        cycle_id = cycle["cycle_id"]
        self.daily.claim(cycle_id)
        self.daily.step_begin(cycle_id, "memory_curation")  # 崩溃时停在 running
        self.daily.recover(cycle_id)
        status = self.daily.status(cycle_id)
        step = next(s for s in status["steps"] if s["step_name"] == "memory_curation")
        self.assertEqual(step["status"], "pending")
        self.assertEqual(step["attempts"], 1)  # 保留尝试记录
        self.assertEqual(status["cycle"]["status"], "recovery_pending")

    def test_completed_steps_skipped_after_recover(self):
        cycle = self.daily.ensure_cycle(
            timezone="Asia/Shanghai", schedule_date="2026-10-31", scheduled_at=BASE
        )["cycle"]
        cycle_id = cycle["cycle_id"]
        self.daily.claim(cycle_id)
        self.daily.step_begin(cycle_id, "memory_curation")
        self.daily.step_complete(cycle_id, "memory_curation")
        self.daily.recover(cycle_id)
        status = self.daily.status(cycle_id)
        step = next(s for s in status["steps"] if s["step_name"] == "memory_curation")
        self.assertEqual(step["status"], "completed")  # 已完成不重置

    def test_abandon_stale_cycle(self):
        cycle = self.daily.ensure_cycle(
            timezone="Asia/Shanghai", schedule_date="2026-10-30", scheduled_at=BASE
        )["cycle"]
        cycle_id = cycle["cycle_id"]
        self.daily.claim(cycle_id)
        self.daily.abandon(cycle_id, "stale_cycle_after_crash")
        status = self.daily.status(cycle_id)
        self.assertEqual(status["cycle"]["status"], "failed")

    def test_double_claim_rejected(self):
        cycle = self.daily.ensure_cycle(
            timezone="Asia/Shanghai", schedule_date="2026-10-31", scheduled_at=BASE
        )["cycle"]
        cycle_id = cycle["cycle_id"]
        self.assertTrue(self.daily.claim(cycle_id))
        self.assertFalse(self.daily.claim(cycle_id))


if __name__ == "__main__":
    unittest.main(verbosity=2)
