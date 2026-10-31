"""P13：行为模块接入（V1.2 最小实现）。

职责：管理当前/情境状态（session 级），把人格 traits、情绪、关系、行为状态整理成 BehaviorGuidance。
不决策、不调模型、不推理、不写人格/情绪/关系表。

Trait 映射（P13-A 批准）：值 >=0.65 高倾向，<=0.35 低倾向，中间区间不生成；未映射 trait 不标 degraded。
hard_constraints_ref 为空不代表不存在硬约束（宿主守卫独立执行）。
"""

import json
import time
import uuid

MODULE_ID = "behavior"
TRAIT_THRESHOLD_HIGH = 0.65
TRAIT_THRESHOLD_LOW = 0.35

# 人格 Trait 名称 → 映射规则（guidance_type, 高倾向描述, 低倾向描述）
TRAIT_MAPPINGS = {
    "openness": ("interaction_style", "探索式、愿意展开新话题", "收敛式、偏好熟悉话题"),
    "conscientiousness": ("task_strategy", "结构化、先列步骤", "灵活、边做边调整"),
    "extraversion": ("initiative", "主动发起、多给回应", "按需回应、不抢话"),
}
TRAIT_NAMES = tuple(TRAIT_MAPPINGS.keys())


class BehaviorService:
    def __init__(self, db, dedup, audit, personality, emotion=None):
        self.db = db
        self.dedup = dedup
        self.audit = audit
        self.personality = personality
        self.emotion = emotion

    # ---------- 写接口 ----------

    def submit_behavior_observation(
        self,
        *,
        request_id,
        session_id,
        workspace_id,
        observation_type,
        target_layer,
        observation_payload,
        ttl=None,
    ):
        if target_layer not in ("current", "situational"):
            raise ValueError("target_layer must be current/situational")
        if target_layer == "situational" and not ttl:
            raise ValueError("situational observation requires ttl")
        if target_layer == "current" and ttl:
            raise ValueError("current observation must not carry ttl")
        payload = {
            "session_id": session_id,
            "workspace_id": workspace_id,
            "observation_type": observation_type,
            "target_layer": target_layer,
            "observation_payload": observation_payload,
            "ttl": ttl,
        }

        def work(conn):
            now = int(time.time() * 1000)
            if target_layer == "current":
                row = conn.execute(
                    "SELECT * FROM behavior_current_state WHERE workspace_id = ? AND session_id = ?",
                    (workspace_id, session_id),
                ).fetchone()
                if row is None:
                    state_id = str(uuid.uuid4())
                    conn.execute(
                        "INSERT INTO behavior_current_state(id, session_id, workspace_id, state_payload,"
                        " revision, lock_version, created_at, updated_at) VALUES (?, ?, ?, ?, 1, 1, ?, ?)",
                        (state_id, session_id, workspace_id,
                         json.dumps(observation_payload, ensure_ascii=False, sort_keys=True), now, now),
                    )
                else:
                    state_id = row["id"]
                    conn.execute(
                        "UPDATE behavior_current_state SET state_payload = ?, revision = revision + 1,"
                        " lock_version = lock_version + 1, updated_at = ? WHERE id = ?",
                        (json.dumps(observation_payload, ensure_ascii=False, sort_keys=True), now, state_id),
                    )
            else:
                # 新情境替代旧情境：旧 active → cancelled。
                conn.execute(
                    "UPDATE behavior_situational_state SET status = 'cancelled'"
                    " WHERE workspace_id = ? AND session_id = ? AND status = 'active'",
                    (workspace_id, session_id),
                )
                state_id = str(uuid.uuid4())
                conn.execute(
                    "INSERT INTO behavior_situational_state(id, session_id, workspace_id, state_payload,"
                    " revision, lock_version, status, created_at, expires_at)"
                    " VALUES (?, ?, ?, ?, 1, 1, 'active', ?, ?)",
                    (state_id, session_id, workspace_id,
                     json.dumps(observation_payload, ensure_ascii=False, sort_keys=True), now, now + int(ttl)),
                )
            conn.execute(
                "INSERT INTO behavior_observation_log(id, request_id, session_id, workspace_id,"
                " observation_type, target_layer, observation_payload, result_status, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, 'applied', ?)",
                (str(uuid.uuid4()), request_id, session_id, workspace_id, observation_type,
                 target_layer, json.dumps(observation_payload, ensure_ascii=False, sort_keys=True), now),
            )
            self.audit.write(
                conn, MODULE_ID, "behavior.observation", "behavior_state", state_id,
                after={"target_layer": target_layer, "observation_type": observation_type},
                request_id=request_id,
            )
            return {"applied": True, "state_id": state_id, "target_layer": target_layer}

        return self.dedup.run(MODULE_ID, request_id, "submit_behavior_observation", payload, work)

    # ---------- 读接口 ----------

    def get_behavior_guidance(self, session_id, workspace_id):
        now = int(time.time() * 1000)
        timestamp = now
        preferred = []
        conflicts = []
        temporary_overrides = []
        degradation_reasons = []
        source_revisions = {}
        degraded = False

        # 1. 人格 traits
        traits = {}
        trait_revision = None
        try:
            rows = self.personality.db.conn.execute(
                "SELECT * FROM personality_traits WHERE name IN (%s)" % ",".join("?" for _ in TRAIT_NAMES),
                TRAIT_NAMES,
            ).fetchall()
            for row in rows:
                traits[row["name"]] = row["value"]
                trait_revision = max(trait_revision or 0, row["state_version"])
            if not rows:
                degraded = True
                degradation_reasons.append("personality_traits_missing")
            else:
                source_revisions["personality"] = trait_revision or 0
        except Exception:
            degraded = True
            degradation_reasons.append("personality_read_failed")

        # 2. 情绪摘要（P14-B 起可用；缺失时降级）
        emotion_guidance = []
        try:
            if self.emotion is None:
                raise RuntimeError("emotion module unavailable")
            summary = self.emotion.get_emotion_summary(workspace_id)
            source_revisions["emotion"] = summary["state_version"]
            pad = summary["pad_render"]
            if pad["p"] > 0.01:
                emotion_guidance.append({
                    "guidance_id": "emotion:positive",
                    "guidance_type": "emotional_response",
                    "description": "当前情绪偏正向，回应可以更自然、更有温度",
                    "reason_refs": ["emotion_state:%s" % workspace_id],
                })
            elif pad["p"] < -0.01:
                emotion_guidance.append({
                    "guidance_id": "emotion:negative",
                    "guidance_type": "emotional_response",
                    "description": "当前情绪偏负向，回应宜克制、避免过度展开",
                    "reason_refs": ["emotion_state:%s" % workspace_id],
                })
        except Exception:
            source_revisions["emotion"] = None
            degradation_reasons.append("emotion_summary_unavailable")
            degraded = True

        # 3. 关系只读
        relationship_revision = None
        try:
            row = self.personality.db.conn.execute(
                "SELECT revision FROM relationship WHERE workspace_id = ? ORDER BY updated_at DESC LIMIT 1",
                (workspace_id,),
            ).fetchone()
            if row is not None:
                relationship_revision = row["revision"]
                source_revisions["relationship"] = relationship_revision
            else:
                degradation_reasons.append("relationship_missing")
        except Exception:
            degradation_reasons.append("relationship_read_failed")

        # 4. Trait 映射（长期倾向）
        for name, value in traits.items():
            mapping = TRAIT_MAPPINGS.get(name)
            if mapping is None:
                continue
            guidance_type, high_desc, low_desc = mapping
            if value >= TRAIT_THRESHOLD_HIGH:
                preferred.append({
                    "guidance_id": "trait:%s:high" % name,
                    "guidance_type": guidance_type,
                    "description": high_desc,
                    "reason_refs": ["personality_trait:%s" % name],
                })
            elif value <= TRAIT_THRESHOLD_LOW:
                preferred.append({
                    "guidance_id": "trait:%s:low" % name,
                    "guidance_type": guidance_type,
                    "description": low_desc,
                    "reason_refs": ["personality_trait:%s" % name],
                })

        # 情绪影响（当前情绪 → 情感回应倾向）
        preferred.extend(emotion_guidance)

        # 5. 当前状态
        try:
            row = self.personality.db.conn.execute(
                "SELECT * FROM behavior_current_state WHERE workspace_id = ? AND session_id = ?",
                (workspace_id, session_id),
            ).fetchone()
            if row is not None:
                source_revisions["behavior_current"] = row["revision"]
                payload = json.loads(row["state_payload"])
                if isinstance(payload, dict):
                    for item in payload.get("preferred", []):
                        preferred.append(item)
                    conflicts.extend(payload.get("conflicts", []))
        except Exception:
            degradation_reasons.append("behavior_current_read_failed")

        # 6. 情境状态（TTL 读取端检查）
        try:
            row = self.personality.db.conn.execute(
                "SELECT * FROM behavior_situational_state WHERE workspace_id = ? AND session_id = ?"
                " AND status = 'active' AND expires_at > ? ORDER BY created_at DESC LIMIT 1",
                (workspace_id, session_id, now),
            ).fetchone()
            if row is not None:
                source_revisions["behavior_situational"] = row["revision"]
                payload = json.loads(row["state_payload"])
                if isinstance(payload, dict):
                    temporary_overrides.append({
                        "guidance_id": "situational:%s" % row["id"],
                        "guidance_type": "situational_override",
                        "description": payload.get("description", ""),
                        "expires_at": row["expires_at"],
                    })
        except Exception:
            degradation_reasons.append("behavior_situational_read_failed")

        # 7. 冲突：长期倾向与当前状态不一致（payload 中显式声明）
        # 8. confidence（输入可靠性四分量简化）
        coverage = min(len([k for k in ("personality", "relationship", "behavior_current", "behavior_situational")
                            if source_revisions.get(k) is not None]) / 4.0, 1.0)
        version_alignment = 1.0
        conflict_penalty = max(0.0, 1.0 - 0.5 * len(conflicts))
        freshness = 1.0
        confidence = round(0.25 * coverage + 0.25 * version_alignment + 0.25 * conflict_penalty + 0.25 * freshness, 4)
        if degraded:
            confidence = min(confidence, 0.5)

        return {
            "session_id": session_id,
            "workspace_id": workspace_id,
            "generated_at": timestamp,
            "source_revisions": source_revisions,
            "preferred": preferred,
            "alternative": [],
            "soft_prohibited": [],
            "hard_constraints_ref": [],
            "hard_constraints_note": "hard_constraints_ref 为空不代表不存在硬约束；宿主守卫独立执行。",
            "conflicts": conflicts,
            "temporary_overrides": temporary_overrides,
            "confidence": confidence,
            "confidence_details": {
                "input_coverage": round(coverage, 4),
                "version_alignment": version_alignment,
                "conflict_penalty": round(conflict_penalty, 4),
                "freshness": freshness,
            },
            "degraded": degraded,
            "degradation_reasons": degradation_reasons,
        }
