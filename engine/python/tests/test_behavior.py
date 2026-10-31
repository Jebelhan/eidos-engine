"""P13 行为模块测试：Trait 映射阈值、TTL、幂等、冲突、降级、隔离。"""

import os
import shutil
import sys
import tempfile
import unittest
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from dsh_engine.audit import AuditLog  # noqa: E402
from dsh_engine.behavior import BehaviorService  # noqa: E402
from dsh_engine.db import Database  # noqa: E402
from dsh_engine.idempotency import RequestDedup  # noqa: E402
from dsh_engine.memory import MemoryService  # noqa: E402
from dsh_engine.outbox import Outbox  # noqa: E402
from dsh_engine.personality import PersonalityService  # noqa: E402


class BehaviorTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dsh-p13-")
        self.db = Database(os.path.join(self.tmp, "test.sqlite"))
        self.db.migrate()
        self.audit = AuditLog(self.db)
        self.dedup = RequestDedup(self.db)
        self.outbox = Outbox(self.db)
        self.memory = MemoryService(self.db, self.dedup, self.audit, self.outbox)
        self.personality = PersonalityService(self.db, self.dedup, self.audit, self.memory)
        self.personality.ensure_traits()
        self.behavior = BehaviorService(self.db, self.dedup, self.audit, self.personality)
        self.n = 0

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def set_trait(self, name, value):
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE personality_traits SET value = ?, state_version = state_version + 1 WHERE name = ?",
                (value, name),
            )

    def submit(self, layer="current", ttl=None, payload=None, session="s-1", workspace="ws-1"):
        self.n += 1
        return self.behavior.submit_behavior_observation(
            request_id="obs-%d" % self.n,
            session_id=session,
            workspace_id=workspace,
            observation_type="interaction_observation",
            target_layer=layer,
            observation_payload=payload or {"preferred": [{"guidance_id": "state:1", "guidance_type": "interaction_style", "description": "当前状态指导"}]},
            ttl=ttl,
        )

    # ---------- Trait 映射 ----------

    def test_high_trait_generates_guidance(self):
        self.set_trait("openness", 0.8)
        guidance = self.behavior.get_behavior_guidance("s-1", "ws-1")
        ids = [g["guidance_id"] for g in guidance["preferred"]]
        self.assertIn("trait:openness:high", ids)

    def test_low_trait_generates_guidance(self):
        self.set_trait("openness", 0.2)
        guidance = self.behavior.get_behavior_guidance("s-1", "ws-1")
        ids = [g["guidance_id"] for g in guidance["preferred"]]
        self.assertIn("trait:openness:low", ids)

    def test_middle_trait_no_guidance(self):
        self.set_trait("openness", 0.5)
        guidance = self.behavior.get_behavior_guidance("s-1", "ws-1")
        ids = [g["guidance_id"] for g in guidance["preferred"]]
        self.assertNotIn("trait:openness:high", ids)
        self.assertNotIn("trait:openness:low", ids)

    def test_unmapped_trait_not_degraded(self):
        # openness 正常中间值，不是 degraded；degraded 只因情绪摘要缺失
        guidance = self.behavior.get_behavior_guidance("s-1", "ws-1")
        self.assertNotIn("unmapped_trait", guidance["degradation_reasons"])

    # ---------- TTL ----------

    def test_valid_situational_guidance(self):
        self.submit("situational", ttl=3600_000)
        guidance = self.behavior.get_behavior_guidance("s-1", "ws-1")
        self.assertEqual(len(guidance["temporary_overrides"]), 1)

    def test_expired_situational_guidance_hidden(self):
        self.submit("situational", ttl=1)
        import time
        time.sleep(0.01)
        guidance = self.behavior.get_behavior_guidance("s-1", "ws-1")
        self.assertEqual(len(guidance["temporary_overrides"]), 0)

    # ---------- 幂等 ----------

    def test_same_request_id_same_params_idempotent(self):
        first = self.behavior.submit_behavior_observation(
            request_id="same-1", session_id="s-1", workspace_id="ws-1",
            observation_type="interaction_observation", target_layer="current",
            observation_payload={"v": 1},
        )
        second = self.behavior.submit_behavior_observation(
            request_id="same-1", session_id="s-1", workspace_id="ws-1",
            observation_type="interaction_observation", target_layer="current",
            observation_payload={"v": 1},
        )
        # 幂等重放返回原结果
        self.assertEqual(first, second)
        count = self.db.conn.execute(
            "SELECT COUNT(*) AS n FROM behavior_observation_log"
        ).fetchone()["n"]
        self.assertEqual(count, 1)

    def test_workspace_isolation(self):
        self.submit("current", payload={"preferred": [{"guidance_id": "ws-1:1", "guidance_type": "x", "description": "ws1"}]})
        other = self.submit("current", payload={"preferred": [{"guidance_id": "ws-2:1", "guidance_type": "x", "description": "ws2"}]}, workspace="ws-2")
        self.assertNotEqual(other["state_id"], "x")
        g1 = self.behavior.get_behavior_guidance("s-1", "ws-1")
        ids = [g["guidance_id"] for g in g1["preferred"]]
        self.assertIn("ws-1:1", ids)
        self.assertNotIn("ws-2:1", ids)

    # ---------- 校验 ----------

    def test_situational_requires_ttl(self):
        with self.assertRaises(ValueError):
            self.submit("situational", ttl=None)

    def test_current_rejects_ttl(self):
        with self.assertRaises(ValueError):
            self.submit("current", ttl=3600)

    # ---------- 降级与置信度 ----------

    def test_degraded_when_emotion_missing(self):
        guidance = self.behavior.get_behavior_guidance("s-1", "ws-1")
        self.assertTrue(guidance["degraded"])
        self.assertIn("emotion_summary_unavailable", guidance["degradation_reasons"])
        self.assertLessEqual(guidance["confidence"], 0.5)

    def test_hard_constraints_note(self):
        guidance = self.behavior.get_behavior_guidance("s-1", "ws-1")
        self.assertEqual(guidance["hard_constraints_ref"], [])
        self.assertIn("不代表不存在硬约束", guidance["hard_constraints_note"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
