"""E4-C2-1 情绪事件重关联测试：merge/split、幂等、不复制、不改 PAD。"""

import os
import shutil
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.dirname(HERE))

from dsh_engine.audit import AuditLog  # noqa: E402
from dsh_engine.db import Database  # noqa: E402
from dsh_engine.emotion import EmotionService  # noqa: E402
from dsh_engine.idempotency import RequestDedup  # noqa: E402

BASE = 1_700_000_000_000


class EmotionRelocateTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="dsh-e4c2-")
        self.db = Database(os.path.join(self.tmp, "test.sqlite"))
        self.db.migrate()
        self.audit = AuditLog(self.db)
        self.dedup = RequestDedup(self.db)
        self.emotion = EmotionService(self.db, self.dedup, self.audit)
        self.n = 0

    def tearDown(self):
        self.db.close()
        shutil.rmtree(self.tmp, ignore_errors=True)

    def add_event(self, i, source_experience="exp-1", source_event=None, workspace="ws-1"):
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO emotion_events(emotion_event_id, workspace_id, source_experience_id,"
                " source_event_id, source_type, event_type, event_slot, description, confidence,"
                " status, created_at) VALUES (?, ?, ?, ?, 'experience', 'positive', ?, 't', 0.8,"
                " 'processed', ?)",
                ("emo-%d" % i, workspace, source_experience, source_event or ("ev-%d" % i), i - 1, BASE),
            )

    def relocate(self, *, op_type, source, canonical=None, targets=None, workspace="ws-1", version=1):
        self.n += 1
        return self.emotion.relocate_emotion_events_for_experience_change(
            request_id="rel-%d" % self.n,
            operation_id="op-%d" % self.n,
            operation_type=op_type,
            workspace_id=workspace,
            source_experience_id=source,
            mapping_version=version,
            canonical_experience_id=canonical,
            event_targets=targets,
        )

    def count(self):
        return self.db.conn.execute("SELECT COUNT(*) FROM emotion_events").fetchone()[0]

    def source_of(self, emotion_event_id):
        return self.db.conn.execute(
            "SELECT source_experience_id FROM emotion_events WHERE emotion_event_id = ?",
            (emotion_event_id,),
        ).fetchone()["source_experience_id"]

    # ---------- merge ----------

    def test_merge_relocates(self):
        self.add_event(1)
        self.add_event(2)
        result = self.relocate(op_type="merge", source="exp-1", canonical="exp-c")
        self.assertEqual(result["relocated"], 2)
        self.assertEqual(self.source_of("emo-1"), "exp-c")
        self.assertEqual(self.source_of("emo-2"), "exp-c")
        self.assertEqual(self.count(), 2)  # 不复制

    def test_merge_idempotent(self):
        self.add_event(1)
        self.relocate(op_type="merge", source="exp-1", canonical="exp-c")
        second = self.relocate(op_type="merge", source="exp-1", canonical="exp-c")
        self.assertEqual(second["relocated"], 0)

    def test_merge_workspace_isolated(self):
        self.add_event(1, workspace="ws-2")
        result = self.relocate(op_type="merge", source="exp-1", canonical="exp-c", workspace="ws-1")
        self.assertEqual(result["relocated"], 0)

    # ---------- split ----------

    def test_split_relocates_by_event(self):
        self.add_event(1, source_event="ev-a")
        self.add_event(2, source_event="ev-b")
        result = self.relocate(
            op_type="split", source="exp-1",
            targets={"ev-a": "new-1", "ev-b": "new-2"},
        )
        self.assertEqual(result["relocated"], 2)
        self.assertEqual(result["unresolved"], [])
        self.assertEqual(self.source_of("emo-1"), "new-1")
        self.assertEqual(self.source_of("emo-2"), "new-2")

    def test_split_missing_mapping_unresolved(self):
        self.add_event(1, source_event="ev-a")
        self.add_event(2, source_event="ev-z")
        result = self.relocate(op_type="split", source="exp-1", targets={"ev-a": "new-1"})
        self.assertEqual(result["relocated"], 1)
        self.assertEqual(result["unresolved"], ["emo-2"])
        self.assertEqual(self.source_of("emo-2"), "exp-1")  # 未动

    def test_split_idempotent(self):
        self.add_event(1, source_event="ev-a")
        self.relocate(op_type="split", source="exp-1", targets={"ev-a": "new-1"})
        second = self.relocate(op_type="split", source="exp-1", targets={"ev-a": "new-1"})
        self.assertEqual(second["relocated"], 0)

    # ---------- PAD 不被修改 ----------

    def test_pad_not_touched(self):
        self.add_event(1)
        before = self.db.conn.execute("SELECT COUNT(*) FROM emotion_state").fetchone()[0]
        self.relocate(op_type="merge", source="exp-1", canonical="exp-c")
        after = self.db.conn.execute("SELECT COUNT(*) FROM emotion_state").fetchone()[0]
        self.assertEqual(before, after)  # 不写 PAD 状态


if __name__ == "__main__":
    unittest.main(verbosity=2)
