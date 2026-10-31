"""P10：关系模块接入实现（R2 数据模型与聚合公式）。

范围：
- Counterpart 解析/创建（P10 只接受可明确解析的身份）。
- Relationship 获取（按 dsh_instance + counterpart 唯一）。
- TrustEvidence 提交（幂等、乐观锁、来源事件冻结、证据身份 = 事件集合哈希）。
- Trust 读取与重算（R2 §三 公式，参数用默认常量）。
- merge/split 重定位（完整事件集合归属判定）。

不改变 R2 的聚合公式、Confidence 公式、权重映射。
"""

import hashlib
import json
import math
import time
import uuid

MODULE_ID = "relationship"

# R2 §三 权重映射（封版，不修改）。
STRENGTH_WEIGHT = {"weak": 0.25, "medium": 0.5, "strong": 0.75, "critical": 1.0}

# R2 未给出默认值的校准参数（P10 用默认常量，待契约方校准）。
PRIOR_WEIGHT = 2.0
EXPERIENCE_WEIGHT_CAP = 1.0
TRUST_RANK_DECAY = 0.05
CONFIDENCE_SCALE = 2.0

# P10 只接受可明确解析的身份类型。Principal（dsh_local:local_principal）不是 Counterpart。
ALLOWED_IDENTITY_TYPES = ("counterpart",)

# 身份键格式：namespace:key（稳定标识），display_name 不得充当身份键（2026-10-31 决策）。
import re

STABLE_IDENTITY_KEY = re.compile(r"^[a-z][a-z0-9_]{1,31}:[A-Za-z0-9_.@-]{1,128}$")


def is_stable_identity_key(identity_key):
    return isinstance(identity_key, str) and bool(STABLE_IDENTITY_KEY.match(identity_key))


def make_evidence_key(workspace_id, relationship_id, event_slot, source_event_ids):
    """证据稳定身份：规范化 JSON + SHA-256（契约 §8.1）。"""
    payload = {
        "v": 1,
        "workspace_id": workspace_id,
        "relationship_id": relationship_id,
        "event_slot": str(event_slot),
        "source_event_ids": sorted(set(str(e) for e in source_event_ids)),
    }
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class RelationshipService:
    def __init__(self, db, dedup, audit):
        self.db = db
        self.dedup = dedup
        self.audit = audit

    # ---------- 身份 ----------

    def instance_id(self):
        row = self.db.conn.execute("SELECT value FROM system_identity WHERE id = 'dsh_instance'").fetchone()
        return row["value"] if row else "unknown"

    def resolve_counterpart(self, identity_type, identity_key):
        if identity_type not in ALLOWED_IDENTITY_TYPES:
            raise ValueError("identity_type not allowed: %r" % (identity_type,))
        if not is_stable_identity_key(identity_key):
            raise ValueError("identity_key must be a stable 'namespace:key', got %r" % (identity_key,))
        row = self.db.conn.execute(
            "SELECT * FROM counterpart WHERE dsh_instance_id = ? AND counterpart_type = ?"
            " AND identity_namespace = ? AND identity_key = ? AND identity_status = 'active'",
            (self.instance_id(), identity_type, identity_type, identity_key),
        ).fetchone()
        if row is None:
            return {"found": False}
        return {"found": True, "counterpart_id": row["counterpart_id"], "status": row["identity_status"]}

    def create_counterpart(self, *, request_id, identity_type, identity_key, display_name=""):
        if identity_type not in ALLOWED_IDENTITY_TYPES:
            raise ValueError("identity_type not allowed: %r" % (identity_type,))
        if not is_stable_identity_key(identity_key):
            raise ValueError("identity_key must be a stable 'namespace:key', got %r" % (identity_key,))

        def work(conn):
            instance = self.instance_id()
            existing = conn.execute(
                "SELECT counterpart_id FROM counterpart WHERE dsh_instance_id = ?"
                " AND counterpart_type = ? AND identity_namespace = ? AND identity_key = ?",
                (instance, identity_type, identity_type, identity_key),
            ).fetchone()
            if existing is not None:
                return {"created": False, "counterpart_id": existing["counterpart_id"]}
            counterpart_id = str(uuid.uuid4())
            now = int(time.time() * 1000)
            conn.execute(
                "INSERT INTO counterpart"
                " (counterpart_id, dsh_instance_id, counterpart_type, identity_namespace, identity_key,"
                "  display_name, aliases, identity_status, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?, NULL, 'active', ?, ?)",
                (counterpart_id, instance, identity_type, identity_type, identity_key, display_name, now, now),
            )
            self.audit.write(
                conn, MODULE_ID, "counterpart.create", "counterpart", counterpart_id,
                after={"identity_type": identity_type, "identity_key": identity_key},
                request_id=request_id,
            )
            return {"created": True, "counterpart_id": counterpart_id}

        return self.dedup.run(
            MODULE_ID, request_id, "create_counterpart",
            {"identity_type": identity_type, "identity_key": identity_key}, work,
        )

    def update_display_name(self, *, counterpart_id, display_name):
        """改名只更新显示名，不创建新身份、不影响身份键。"""
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT display_name FROM counterpart WHERE counterpart_id = ?", (counterpart_id,)
            ).fetchone()
            if row is None:
                raise KeyError("counterpart not found: %s" % counterpart_id)
            conn.execute(
                "UPDATE counterpart SET display_name = ?, updated_at = ? WHERE counterpart_id = ?",
                (display_name, int(time.time() * 1000), counterpart_id),
            )
            self.audit.write(
                conn, MODULE_ID, "counterpart.update_display_name", "counterpart", counterpart_id,
                before={"display_name": row["display_name"]}, after={"display_name": display_name},
            )
        return {"counterpart_id": counterpart_id, "display_name": display_name}

    def ensure_relationship(self, *, workspace_id, counterpart_id):
        """取得（或创建）该 counterpart 在本 workspace 的 relationship。"""
        instance = self.instance_id()
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT * FROM relationship WHERE dsh_instance_id = ? AND counterpart_id = ?",
                (instance, counterpart_id),
            ).fetchone()
            if row is not None:
                return dict(row)
            relationship_id = str(uuid.uuid4())
            now = int(time.time() * 1000)
            conn.execute(
                "INSERT INTO relationship"
                " (relationship_id, dsh_instance_id, counterpart_id, workspace_id, status,"
                "  trust_value, trust_confidence, revision, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, 'active', 50, 0, 0, ?, ?)",
                (relationship_id, instance, counterpart_id, workspace_id, now, now),
            )
            self.audit.write(
                conn, MODULE_ID, "relationship.create", "relationship", relationship_id,
                after={"counterpart_id": counterpart_id, "workspace_id": workspace_id},
            )
            return dict(conn.execute(
                "SELECT * FROM relationship WHERE relationship_id = ?", (relationship_id,)
            ).fetchone())

    # ---------- 证据 ----------

    def submit_trust_evidence(
        self,
        *,
        request_id,
        relationship_id,
        source_experience_id,
        source_event_ids,
        event_slot="0",
        trust_signal,
        evidence_strength,
        expected_revision,
    ):
        if evidence_strength not in STRENGTH_WEIGHT:
            raise ValueError("evidence_strength must be weak/medium/strong/critical")
        trust_signal = float(trust_signal)
        if not (-1.0 <= trust_signal <= 1.0):
            raise ValueError("trust_signal must be within [-1, 1]")
        events = sorted(set(str(e) for e in source_event_ids))
        if not events:
            raise ValueError("source_event_ids is required")

        def work(conn):
            rel = conn.execute(
                "SELECT * FROM relationship WHERE relationship_id = ?", (relationship_id,)
            ).fetchone()
            if rel is None:
                raise KeyError("relationship not found: %s" % relationship_id)
            if int(rel["revision"]) != int(expected_revision):
                raise ValueError("revision conflict: expected %s, current %s" % (expected_revision, rel["revision"]))

            key = make_evidence_key(rel["workspace_id"], relationship_id, event_slot, events)
            existing = conn.execute(
                "SELECT * FROM trust_evidence WHERE workspace_id = ? AND relationship_id = ? AND evidence_key = ?",
                (rel["workspace_id"], relationship_id, key),
            ).fetchone()
            if existing is not None:
                return {"created": False, "deduplicated": True, "evidence_id": existing["evidence_id"]}

            evidence_id = str(uuid.uuid4())
            now = int(time.time() * 1000)
            conn.execute(
                "INSERT INTO trust_evidence"
                " (evidence_id, workspace_id, relationship_id, source_experience_id, event_slot,"
                "  trust_signal, evidence_weight, evidence_status, revision, evidence_key, created_at, updated_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, 'active', 0, ?, ?, ?)",
                (
                    evidence_id, rel["workspace_id"], relationship_id, source_experience_id, str(event_slot),
                    trust_signal, STRENGTH_WEIGHT[evidence_strength], key, now, now,
                ),
            )
            for event_id in events:
                conn.execute(
                    "INSERT INTO trust_evidence_source_events(evidence_id, source_event_id, created_at)"
                    " VALUES (?, ?, ?)",
                    (evidence_id, event_id, now),
                )
            self._recompute_trust(conn, relationship_id)
            self.audit.write(
                conn, MODULE_ID, "trust_evidence.submit", "trust_evidence", evidence_id,
                after={"relationship_id": relationship_id, "trust_signal": trust_signal,
                       "evidence_strength": evidence_strength},
                request_id=request_id,
            )
            return {"created": True, "evidence_id": evidence_id}

        return self.dedup.run(
            MODULE_ID, request_id, "submit_trust_evidence",
            {"relationship_id": relationship_id, "source_experience_id": source_experience_id,
             "source_event_ids": events, "event_slot": str(event_slot),
             "trust_signal": trust_signal, "evidence_strength": evidence_strength},
            work,
        )

    # ---------- Trust 计算（R2 §三） ----------

    def _recompute_trust(self, conn, relationship_id):
        rel = conn.execute(
            "SELECT * FROM relationship WHERE relationship_id = ?", (relationship_id,)
        ).fetchone()
        evidences = conn.execute(
            "SELECT * FROM trust_evidence WHERE relationship_id = ? AND evidence_status = 'active'",
            (relationship_id,),
        ).fetchall()

        # 按 canonical 分组聚合：N_e / S_e / W_e
        groups = {}
        for ev in evidences:
            groups.setdefault(ev["source_experience_id"], []).append(ev)

        scored = []
        for canonical, group in groups.items():
            n_e = sum(ev["evidence_weight"] * ev["trust_signal"] for ev in group)
            s_e = 0 if abs(n_e) < 1e-12 else (1 if n_e > 0 else -1)
            w_e = min(abs(n_e), EXPERIENCE_WEIGHT_CAP)
            if w_e == 0:
                continue
            occurred_at = self._occurred_at(conn, canonical)
            scored.append((occurred_at, canonical, s_e, w_e))

        # rank 按 occurred_at 降序，相同则 canonical ID 升序
        scored.sort(key=lambda item: (-item[0], item[1]))
        total = PRIOR_WEIGHT
        weighted = PRIOR_WEIGHT * 0.5
        for rank, (_, _, s_e, w_e) in enumerate(scored):
            w_eff = w_e / (1 + TRUST_RANK_DECAY * rank)
            total += w_eff
            weighted += w_eff * (s_e + 1) / 2

        trust_value = 100.0 * weighted / total if total > 0 else 50.0
        confidence = 1 - math.exp(-len(groups) / CONFIDENCE_SCALE) if groups else 0.0
        conn.execute(
            "UPDATE relationship SET trust_value = ?, trust_confidence = ?,"
            " revision = revision + 1, updated_at = ? WHERE relationship_id = ?",
            (round(trust_value, 6), round(confidence, 6), int(time.time() * 1000), relationship_id),
        )

    def _occurred_at(self, conn, experience_id):
        row = conn.execute(
            "SELECT occurred_at FROM memory_experience WHERE experience_id = ?", (experience_id,)
        ).fetchone()
        return int(row["occurred_at"]) if row else 0

    def get_trust(self, relationship_id):
        row = self.db.conn.execute(
            "SELECT relationship_id, trust_value, trust_confidence, revision, status, workspace_id"
            " FROM relationship WHERE relationship_id = ?",
            (relationship_id,),
        ).fetchone()
        if row is None:
            return None
        return {
            "relationship_id": row["relationship_id"],
            "trust_value": row["trust_value"],
            "confidence": row["trust_confidence"],
            "revision": row["revision"],
            "status": row["status"],
            "workspace_id": row["workspace_id"],
        }

    # ---------- merge / split（B5，事务由调用方提供） ----------

    def relocate_for_merge(self, conn, *, canonical_experience_id, merged_from):
        """merge：事件集合不变，只更新证据当前关联。"""
        rows = conn.execute(
            "SELECT evidence_id FROM trust_evidence WHERE source_experience_id IN ("
            + ",".join("?" for _ in merged_from)
            + ")",
            tuple(merged_from),
        ).fetchall()
        for row in rows:
            conn.execute(
                "UPDATE trust_evidence SET source_experience_id = ?, updated_at = ? WHERE evidence_id = ?",
                (canonical_experience_id, int(time.time() * 1000), row["evidence_id"]),
            )
            self.audit.write(
                conn, MODULE_ID, "trust_evidence.relocate", "trust_evidence", row["evidence_id"],
                after={"canonical_experience_id": canonical_experience_id, "reason": "merge"},
            )
        return {"relocated": len(rows)}

    def relocate_for_split(self, conn, *, split_experience_id, new_experiences):
        """
        split：完整事件集合归属判定。
        new_experiences: [{experience_id, event_ids}]
        - E ⊆ events(Ni) 且唯一 → 更新关联
        - 否则 → pending_reassessment
        """
        evidence_rows = conn.execute(
            "SELECT * FROM trust_evidence WHERE source_experience_id = ? AND evidence_status = 'active'",
            (split_experience_id,),
        ).fetchall()
        relocated, pending = 0, 0
        relationships = set()
        for ev in evidence_rows:
            events = {
                r["source_event_id"]
                for r in conn.execute(
                    "SELECT source_event_id FROM trust_evidence_source_events WHERE evidence_id = ?",
                    (ev["evidence_id"],),
                )
            }
            candidates = [
                item for item in new_experiences
                if events and events.issubset(set(item["event_ids"]))
            ]
            if len(candidates) == 1:
                conn.execute(
                    "UPDATE trust_evidence SET source_experience_id = ?, updated_at = ? WHERE evidence_id = ?",
                    (candidates[0]["experience_id"], int(time.time() * 1000), ev["evidence_id"]),
                )
                self.audit.write(
                    conn, MODULE_ID, "trust_evidence.relocate", "trust_evidence", ev["evidence_id"],
                    after={"canonical_experience_id": candidates[0]["experience_id"], "reason": "split"},
                )
                relocated += 1
            else:
                conn.execute(
                    "UPDATE trust_evidence SET evidence_status = 'pending_reassessment', updated_at = ?"
                    " WHERE evidence_id = ?",
                    (int(time.time() * 1000), ev["evidence_id"]),
                )
                self.audit.write(
                    conn, MODULE_ID, "trust_evidence.pending_reassessment", "trust_evidence",
                    ev["evidence_id"],
                    after={"reason": "split: incomplete or ambiguous ownership"},
                )
                pending += 1
            relationships.add(ev["relationship_id"])
        for relationship_id in relationships:
            self._recompute_trust(conn, relationship_id)
        return {"relocated": relocated, "pending_reassessment": pending}

    def invalidate_for_experience(self, conn, *, canonical_experience_id):
        """experience.invalidated：相关证据转 invalidated 并重算。"""
        rows = conn.execute(
            "SELECT DISTINCT relationship_id FROM trust_evidence WHERE source_experience_id = ?"
            " AND evidence_status = 'active'",
            (canonical_experience_id,),
        ).fetchall()
        conn.execute(
            "UPDATE trust_evidence SET evidence_status = 'invalidated', updated_at = ?"
            " WHERE source_experience_id = ? AND evidence_status = 'active'",
            (int(time.time() * 1000), canonical_experience_id),
        )
        for row in rows:
            self._recompute_trust(conn, row["relationship_id"])
            self.audit.write(
                conn, MODULE_ID, "trust_evidence.invalidate", "relationship", row["relationship_id"],
                after={"canonical_experience_id": canonical_experience_id},
            )
        return {"invalidated": len(rows)}
