"""P9：情绪事件最小实现（V1.3 契约的提交/修正部分）。

范围：
- submit_emotion_event：写 emotion_events + emotion_audit，幂等（统一 request_dedup）。
- revise_emotion_event：修正判断结果（event_type/confidence/candidates），status=revised。
- 不实现：advance_emotion_state、offset、心情投影、rebuild（P9 之外）。
"""

import json
import math
import time
import uuid
from datetime import datetime

# P14-B 校准参数（P15 校准）：基线权重、事件时间常数、低通时间常数、规范步长。
BASELINE_PAD = {"p": 0.0, "a": 0.0, "d": 0.0}
BASELINE_WEIGHT = 2.0
EVENT_TIME_CONSTANT_MS = 86_400_000  # 24h
LOWPASS_TAU_MS = 21_600_000          # 6h
NORM_STEP_MS = 3_600_000             # 1h
WEIGHT_EPSILON = 0.01

MODULE_ID = "emotion"
ALLOWED_EVENT_TYPES = ("positive", "negative", "mixed", "neutral")


class EmotionService:
    def __init__(self, db, dedup, audit):
        self.db = db
        self.dedup = dedup
        self.audit = audit

    def submit_emotion_event(
        self,
        *,
        request_id,
        workspace_id,
        source_experience_id,
        source_event_id=None,
        source_type="experience",
        event_type,
        event_slot=0,
        description="",
        confidence=0.0,
        emotion_candidates=None,
    ):
        if event_type not in ALLOWED_EVENT_TYPES:
            raise ValueError("event_type must be one of %r" % (ALLOWED_EVENT_TYPES,))
        payload = {
            "workspace_id": workspace_id,
            "source_experience_id": source_experience_id,
            "event_type": event_type,
            "event_slot": int(event_slot),
        }

        def work(conn):
            existing = conn.execute(
                "SELECT * FROM emotion_events WHERE workspace_id = ? AND source_experience_id = ?"
                " AND event_type = ? AND event_slot = ?",
                (workspace_id, source_experience_id, event_type, int(event_slot)),
            ).fetchone()
            if existing is not None:
                return {"created": False, "emotion_event_id": existing["emotion_event_id"], "deduplicated": True}

            event_id = str(uuid.uuid4())
            now = int(time.time() * 1000)
            conn.execute(
                "INSERT INTO emotion_events"
                " (emotion_event_id, workspace_id, source_experience_id, source_event_id, source_type,"
                "  event_type, event_slot, description, confidence, emotion_candidates, status, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'processed', ?)",
                (
                    event_id,
                    workspace_id,
                    source_experience_id,
                    source_event_id,
                    source_type,
                    event_type,
                    int(event_slot),
                    description,
                    float(confidence),
                    json.dumps(emotion_candidates, ensure_ascii=False) if emotion_candidates is not None else None,
                    now,
                ),
            )
            self.audit.write(
                conn,
                MODULE_ID,
                "emotion.submit",
                "emotion_event",
                event_id,
                after={"event_type": event_type, "confidence": float(confidence), "source_experience_id": source_experience_id},
                request_id=request_id,
                logical_version=0,
            )
            return {"created": True, "emotion_event_id": event_id}

        return self.dedup.run(MODULE_ID, request_id, "submit_emotion_event", payload, work)

    def revise_emotion_event(self, *, request_id, workspace_id, emotion_event_id, correction, reason):
        """修正判断结果。correction 结构（P9 最小实现，待契约确认）：
        {event_type?, confidence?, emotion_candidates?}
        """
        if not reason:
            raise ValueError("revise requires reason")
        correction = correction or {}
        if "event_type" in correction and correction["event_type"] not in ALLOWED_EVENT_TYPES:
            raise ValueError("invalid event_type in correction")

        def work(conn):
            row = conn.execute(
                "SELECT * FROM emotion_events WHERE emotion_event_id = ? AND workspace_id = ?",
                (emotion_event_id, workspace_id),
            ).fetchone()
            if row is None:
                raise KeyError("emotion event not found: %s" % emotion_event_id)
            before = {"event_type": row["event_type"], "confidence": row["confidence"]}
            after = {
                "event_type": correction.get("event_type", row["event_type"]),
                "confidence": correction.get("confidence", row["confidence"]),
                "reason": reason,
            }
            conn.execute(
                "UPDATE emotion_events SET event_type = ?, confidence = ?,"
                " emotion_candidates = COALESCE(?, emotion_candidates), status = 'revised', revised_at = ?"
                " WHERE emotion_event_id = ?",
                (
                    after["event_type"],
                    after["confidence"],
                    json.dumps(correction["emotion_candidates"], ensure_ascii=False)
                    if correction.get("emotion_candidates") is not None
                    else None,
                    int(time.time() * 1000),
                    emotion_event_id,
                ),
            )
            self.audit.write(
                conn,
                MODULE_ID,
                "emotion.revise",
                "emotion_event",
                emotion_event_id,
                before=before,
                after=after,
                request_id=request_id,
                logical_version=1,
            )
            return {"revised": True, "emotion_event_id": emotion_event_id}

        return self.dedup.run(
            MODULE_ID,
            request_id,
            "revise_emotion_event",
            {"emotion_event_id": emotion_event_id, "workspace_id": workspace_id},
            work,
        )

    def get_event(self, emotion_event_id):
        row = self.db.conn.execute(
            "SELECT * FROM emotion_events WHERE emotion_event_id = ?", (emotion_event_id,)
        ).fetchone()
        return dict(row) if row else None

    def find_by_experience(self, workspace_id, canonical_experience_id):
        rows = self.db.conn.execute(
            "SELECT * FROM emotion_events WHERE workspace_id = ? AND source_experience_id = ?"
            " ORDER BY created_at",
            (workspace_id, canonical_experience_id),
        ).fetchall()
        return [dict(row) for row in rows]

    # ---------- P14-B：推进（唯一 PAD 写入路径，V1.3 §二） ----------

    def _default_pad(self, event_type):
        if event_type == "positive":
            return {"p": 0.5, "a": 0.2, "d": 0.1}
        if event_type == "negative":
            return {"p": -0.5, "a": 0.2, "d": -0.1}
        return {"p": 0.0, "a": 0.0, "d": 0.0}

    def advance_emotion_state(self, *, request_id, workspace_id, target_time):
        """按 V1.3：基线锚定质心 + 时间常数低通，分段推进到 target_time。"""
        target_time = int(target_time)

        def work(conn):
            now = int(time.time() * 1000)
            state = conn.execute(
                "SELECT * FROM emotion_state WHERE workspace_id = ?", (workspace_id,)
            ).fetchone()
            if state is None:
                pad_prev = dict(BASELINE_PAD)
                last = target_time - NORM_STEP_MS
                version = 0
            else:
                pad_prev = json.loads(state["pad_base"])
                last = int(state["last_advanced_at"] or (target_time - NORM_STEP_MS))
                version = int(state["state_version"])

            if target_time <= last:
                return {"advanced": False, "reason": "already_advanced", "pad": pad_prev, "state_version": version}

            events = conn.execute(
                "SELECT * FROM emotion_events WHERE workspace_id = ?"
                " AND status IN ('processed', 'revised') AND created_at <= ?",
                (workspace_id, target_time),
            ).fetchall()

            total_weight = 0.0
            acc = {"p": 0.0, "a": 0.0, "d": 0.0}
            used = 0
            for event in events:
                if event["emotion_candidates"]:
                    pad_i = json.loads(event["emotion_candidates"])
                else:
                    pad_i = self._default_pad(event["event_type"])
                w0 = float(event["confidence"] or 0.5)
                dt = target_time - int(event["created_at"])
                weight = w0 * (2.0 ** (-dt / EVENT_TIME_CONSTANT_MS))
                if weight < WEIGHT_EPSILON:
                    continue
                used += 1
                total_weight += weight
                for axis in ("p", "a", "d"):
                    acc[axis] += weight * float(pad_i.get(axis, 0.0))

            denom = BASELINE_WEIGHT + total_weight
            target = {
                axis: (BASELINE_WEIGHT * BASELINE_PAD[axis] + acc[axis]) / denom for axis in ("p", "a", "d")
            }

            # 分段低通（规范步长），从 last 推进到 target_time。
            pad = dict(pad_prev)
            cursor = last
            while cursor < target_time:
                dt = min(NORM_STEP_MS, target_time - cursor)
                alpha = 1.0 - math.exp(-dt / LOWPASS_TAU_MS)
                for axis in ("p", "a", "d"):
                    pad[axis] = pad[axis] + alpha * (target[axis] - pad[axis])
                cursor += dt
            pad = {axis: max(-1.0, min(1.0, pad[axis])) for axis in pad}

            if state is None:
                conn.execute(
                    "INSERT INTO emotion_state(workspace_id, pad_base, baseline_snapshot, config_version,"
                    " state_version, last_advanced_at, updated_at) VALUES (?, ?, ?, 1, 1, ?, ?)",
                    (workspace_id, json.dumps(pad, sort_keys=True),
                     json.dumps(BASELINE_PAD, sort_keys=True), target_time, now),
                )
            else:
                conn.execute(
                    "UPDATE emotion_state SET pad_base = ?, baseline_snapshot = ?,"
                    " state_version = state_version + 1, last_advanced_at = ?, updated_at = ?"
                    " WHERE workspace_id = ?",
                    (json.dumps(pad, sort_keys=True), json.dumps(BASELINE_PAD, sort_keys=True),
                     target_time, now, workspace_id),
                )
            conn.execute(
                "INSERT INTO emotion_checkpoints(checkpoint_id, workspace_id, pad_snapshot, advanced_to,"
                " event_cursor, event_revision, config_version, step_size, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)",
                (str(uuid.uuid4()), workspace_id, json.dumps(pad, sort_keys=True), target_time,
                 str(used), used, NORM_STEP_MS, now),
            )
            self.audit.write(
                conn, MODULE_ID, "emotion.advance", "emotion_state", workspace_id,
                after={
                    "pad": pad,
                    "target": target,
                    "events_used": used,
                    "advanced_to": target_time,
                    "state_version": version + 1,
                },
                request_id=request_id,
            )
            return {
                "advanced": True,
                "pad": pad,
                "target": target,
                "events_used": used,
                "advanced_to": target_time,
                "state_version": version + 1,
            }

        return self.dedup.run(
            MODULE_ID,
            request_id,
            "advance_emotion_state",
            {"workspace_id": workspace_id, "target_time": target_time},
            work,
        )

    def relocate_emotion_events_for_experience_change(self, *, request_id, operation_id,
                                                      operation_type, workspace_id,
                                                      source_experience_id, mapping_version,
                                                      canonical_experience_id=None,
                                                      event_targets=None, conn=None):
        """E4-C2-1：merge → canonical 重关联；split → 按事件映射重关联。不复制、不改 PAD。"""
        if operation_type not in ("merge", "split"):
            raise ValueError("operation_type must be merge/split")
        if operation_type == "merge" and not canonical_experience_id:
            raise ValueError("merge requires canonical_experience_id")
        if operation_type == "split" and not event_targets:
            raise ValueError("split requires event_targets")

        def work(conn):
            if operation_type == "merge":
                cursor = conn.execute(
                    "UPDATE emotion_events SET source_experience_id = ?"
                    " WHERE workspace_id = ? AND source_experience_id = ?",
                    (canonical_experience_id, workspace_id, source_experience_id),
                )
                moved = cursor.rowcount
                self.audit.write(
                    conn, MODULE_ID, "emotion_events.relocate", "emotion_events",
                    source_experience_id,
                    after={"operation_id": operation_id, "operation_type": "merge",
                           "canonical_experience_id": canonical_experience_id,
                           "relocated": moved, "mapping_version": mapping_version},
                )
                return {"relocated": moved, "unresolved": []}

            rows = conn.execute(
                "SELECT * FROM emotion_events WHERE workspace_id = ? AND source_experience_id = ?",
                (workspace_id, source_experience_id),
            ).fetchall()
            moved = 0
            unresolved = []
            for event in rows:
                target = (event_targets or {}).get(event["source_event_id"])
                if not target:
                    unresolved.append(event["emotion_event_id"])
                    continue
                conn.execute(
                    "UPDATE emotion_events SET source_experience_id = ? WHERE emotion_event_id = ?",
                    (target, event["emotion_event_id"]),
                )
                moved += 1
            self.audit.write(
                conn, MODULE_ID, "emotion_events.relocate", "emotion_events",
                source_experience_id,
                after={"operation_id": operation_id, "operation_type": "split",
                       "relocated": moved, "unresolved": len(unresolved),
                       "mapping_version": mapping_version},
            )
            return {"relocated": moved, "unresolved": unresolved}

        if conn is not None:
            return work(conn)
        return self.dedup.run(
            MODULE_ID, request_id, "relocate_emotion_events",
            {"operation_id": operation_id, "operation_type": operation_type,
             "source": source_experience_id, "canonical": canonical_experience_id},
            work,
        )

    def get_emotion_summary(self, workspace_id):
        """只读摘要（PAD_render + 心情投影），供行为模块消费。"""
        now = int(time.time() * 1000)
        state = self.db.conn.execute(
            "SELECT * FROM emotion_state WHERE workspace_id = ?", (workspace_id,)
        ).fetchone()
        pad = json.loads(state["pad_base"]) if state else dict(BASELINE_PAD)
        hour = datetime.now().hour
        clock = 1.0 - abs(hour - 12) / 12.0
        pad_render = {"p": pad["p"], "a": 0.7 * pad["a"] + 0.3 * clock, "d": pad["d"]}
        if pad["p"] > 0.2:
            mood = "positive"
        elif pad["p"] < -0.2:
            mood = "negative"
        else:
            mood = "neutral"
        events_count = self.db.conn.execute(
            "SELECT COUNT(*) AS n FROM emotion_events WHERE workspace_id = ?"
            " AND status IN ('processed', 'revised')",
            (workspace_id,),
        ).fetchone()["n"]
        return {
            "workspace_id": workspace_id,
            "pad_base": pad,
            "pad_render": pad_render,
            "mood": mood,
            "state_version": int(state["state_version"]) if state else 0,
            "last_advanced_at": int(state["last_advanced_at"]) if state and state["last_advanced_at"] else None,
            "events_count": events_count,
            "generated_at": now,
            "available": True,
        }
