"""E4-C2-3 人格确认重解析测试：canonical 重解析、去重、激活保护、幂等。"""

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
from dsh_engine.memory import MemoryService  # noqa: E402
from dsh_engine.outbox import Outbox  # noqa: E402
from dsh_engine.personality import PersonalityService  # noqa: E402

WS = "ws-c23"
BASE = 1_700_000_000_000


class PersonalityRelocateTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dsh-c23-")
        self.db = Database(os.path.join(self.tmp, "test.sqlite"))
        self.db.migrate()
        self.audit = AuditLog(self.db)
        self.dedup = RequestDedup(self.db)
        self.outbox = Outbox(self.db)
        self.memory = MemoryService(self.db, self.dedup, self.audit, self.outbox)
        self.personality = PersonalityService(self.db, self.dedup, self.audit, self.memory)
        self.trait = self.personality.ensure_trait()
        self.n = 0

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def experience(self, i, workspace=WS):
        self.n += 1
        created = self.memory.create_experience(
            request_id="e-%d" % self.n, workspace_id=workspace, session_id="s", subject_id=None,
            event_ids=["ev-%d-%d" % (i, self.n)], experience_type="conversation_turn",
            occurred_at=int(time.time() * 1000), description="d",
        )
        return created["experience_id"]

    def request(self, status="pending", workspace=WS):
        self.n += 1
        request_id = "req-%d" % self.n
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO learning_influence_requests(influence_request_id, suggestion_id,"
                " conclusion_id, target_trait_id, direction, raw_strength, confidence, source_weight,"
                " status, created_at, window_expires_at, updated_at) VALUES (?, ?, 'c-1', ?, 1,"
                " 0.5, 0.5, 1.0, ?, ?, ?, ?)",
                (request_id, "s-%d" % self.n, self.trait["trait_id"], status, BASE,
                 BASE + 30 * 86_400_000, BASE),
            )
        return request_id

    def confirmation(self, request_id, experience_id, key, created=BASE, workspace=WS):
        self.n += 1
        confirmation_id = "conf-%d" % self.n
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO learning_influence_confirmations(confirmation_id, influence_request_id,"
                " experience_id, independence_key, occurred_at, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (confirmation_id, request_id, experience_id, key, created, created),
            )
        return confirmation_id

    def relocate(self, source, canonical, workspace=WS):
        self.n += 1
        return self.personality.relocate_personality_confirmations_for_merge(
            request_id="rel-%d" % self.n, operation_id="op-%d" % self.n,
            workspace_id=workspace, source_experience_id=source,
            canonical_experience_id=canonical, mapping_version=1,
        )

    def freeze_merge(self, source, canonical):
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE memory_experience SET canonical_experience_id = ? WHERE experience_id = ?",
                (canonical, source),
            )

    def active_count(self, request_id):
        return self.db.conn.execute(
            "SELECT COUNT(*) FROM learning_influence_confirmations"
            " WHERE influence_request_id = ? AND revoked_at IS NULL",
            (request_id,),
        ).fetchone()[0]

    # ---------- 重解析 ----------

    def test_merge_relocates(self):
        a = self.experience(1)
        c = self.experience(2)
        request_id = self.request()
        confirmation_id = self.confirmation(request_id, a, "k-a")
        self.freeze_merge(a, c)
        result = self.relocate(a, c)
        self.assertEqual(result["relocated"], 1)
        row = self.db.conn.execute(
            "SELECT experience_id FROM learning_influence_confirmations WHERE confirmation_id = ?",
            (confirmation_id,),
        ).fetchone()
        self.assertEqual(row["experience_id"], c)

    def test_dedup_same_canonical(self):
        a = self.experience(1)
        c = self.experience(2)
        request_id = self.request()
        first = self.confirmation(request_id, a, "k-a", created=BASE + 1)
        second = self.confirmation(request_id, c, "k-b", created=BASE + 2)
        self.freeze_merge(a, c)
        result = self.relocate(a, c)
        self.assertEqual(len(result["revoked"]), 1)
        self.assertEqual(self.active_count(request_id), 1)

    def test_earliest_kept(self):
        a = self.experience(1)
        c = self.experience(2)
        request_id = self.request()
        early = self.confirmation(request_id, a, "k-a", created=BASE + 1)
        late = self.confirmation(request_id, c, "k-b", created=BASE + 2)
        self.freeze_merge(a, c)
        self.relocate(a, c)
        row = self.db.conn.execute(
            "SELECT confirmation_id FROM learning_influence_confirmations"
            " WHERE influence_request_id = ? AND revoked_at IS NULL",
            (request_id,),
        ).fetchone()
        self.assertEqual(row["confirmation_id"], early)

    def test_revoked_not_restored(self):
        a = self.experience(1)
        c = self.experience(2)
        request_id = self.request()
        confirmation_id = self.confirmation(request_id, a, "k-a")
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE learning_influence_confirmations SET revoked_at = ? WHERE confirmation_id = ?",
                (BASE, confirmation_id),
            )
        result = self.relocate(a, c)
        self.assertEqual(result["relocated"], 0)
        row = self.db.conn.execute(
            "SELECT revoked_at FROM learning_influence_confirmations WHERE confirmation_id = ?",
            (confirmation_id,),
        ).fetchone()
        self.assertIsNotNone(row["revoked_at"])

    def test_active_request_not_touched(self):
        a = self.experience(1)
        c = self.experience(2)
        request_id = self.request(status="active")
        confirmation_id = self.confirmation(request_id, a, "k-a")
        result = self.relocate(a, c)
        self.assertEqual(result["relocated"], 0)
        self.assertIn(confirmation_id, result["skipped"])
        row = self.db.conn.execute(
            "SELECT experience_id FROM learning_influence_confirmations WHERE confirmation_id = ?",
            (confirmation_id,),
        ).fetchone()
        self.assertEqual(row["experience_id"], a)

    def test_anomalous_chain_skipped(self):
        a = self.experience(1)
        request_id = self.request()
        confirmation_id = self.confirmation(request_id, a, "k-a")
        with self.db.transaction() as conn:
            conn.execute(
                "UPDATE memory_experience SET canonical_experience_id = 'missing' WHERE experience_id = ?",
                (a,),
            )
        result = self.relocate(a, "missing")
        self.assertEqual(result["relocated"], 0)
        self.assertIn(confirmation_id, result["skipped"])

    def test_idempotent(self):
        a = self.experience(1)
        c = self.experience(2)
        request_id = self.request()
        self.confirmation(request_id, a, "k-a")
        self.freeze_merge(a, c)
        self.relocate(a, c)
        second = self.relocate(a, c)
        self.assertEqual(second["relocated"], 0)

    def test_workspace_isolation(self):
        a = self.experience(1, workspace="ws-other")
        c = self.experience(2)
        request_id = self.request()
        self.confirmation(request_id, a, "k-a", workspace="ws-other")
        result = self.relocate(a, c, workspace=WS)
        self.assertEqual(result["relocated"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
