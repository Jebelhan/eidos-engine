"""P12：人格模块接入最小实现（V1.2 链路中的建议 → 请求 → 确认 → 激活 → 实例池）。

范围：
- Suggestions 领取（suggestion_consumption 幂等）；
- LearningInfluenceRequest（pending → active / dropped）；
- 确认证据：内部调记忆模块 check_experience，取系统 canonical/independence_key；
- 达标（2 条独立 verified 确认，30 天窗口）→ activate → 生成实例进池；
- 证据失效：激活前撤销确认重算；激活后不回滚（记录审计）。

不实现：结算/衰减/边界释放（P14）、行为模块、影子模式。
"""

import json
import math
import time
import uuid

MODULE_ID = "personality"
MIN_CONFIRMATIONS = 2
CONFIRMATION_WINDOW_MS = 30 * 24 * 3600 * 1000
DEFAULT_TRAIT = "openness"
MAX_CHANGE = 0.05        # 单周期最大变化（待 P15 校准）
RELEASE_SCALE = 0.5      # tanh 尺度（待 P15 校准）


class PersonalityService:
    def __init__(self, db, dedup, audit, memory):
        self.db = db
        self.dedup = dedup
        self.audit = audit
        self.memory = memory

    # ---------- Trait ----------

    def ensure_trait(self, name=DEFAULT_TRAIT):
        row = self.db.conn.execute(
            "SELECT * FROM personality_traits WHERE name = ?", (name,)
        ).fetchone()
        if row is not None:
            return dict(row)
        with self.db.transaction() as conn:
            now = int(time.time() * 1000)
            trait_id = str(uuid.uuid4())
            conn.execute(
                "INSERT INTO personality_traits(trait_id, name, value, direction, stability, resistance,"
                " base_yield_threshold, frozen, created_at, updated_at)"
                " VALUES (?, ?, 0.5, 0, 0.5, 1.0, 0.2, 0, ?, ?)",
                (trait_id, name, now, now),
            )
            self.audit.write(
                conn, MODULE_ID, "trait.create", "trait", trait_id, after={"name": name},
            )
            return dict(conn.execute(
                "SELECT * FROM personality_traits WHERE trait_id = ?", (trait_id,)
            ).fetchone())

    def ensure_traits(self, names=("openness", "conscientiousness", "extraversion")):
        """确保行为模块需要的三条 trait 存在（P13）。"""
        return [self.ensure_trait(name) for name in names]

    def get_trait(self, name=DEFAULT_TRAIT):
        row = self.db.conn.execute(
            "SELECT * FROM personality_traits WHERE name = ?", (name,)
        ).fetchone()
        return dict(row) if row else None

    # ---------- 建议领取 ----------

    def handle_learning_suggestion(self, *, request_id, suggestion, conclusion_id, target_trait_id=None):
        """Suggestion → LearningInfluenceRequest(pending)。幂等：同一 suggestion 只处理一次。"""
        # trait 先在事务外准备好，避免在 dedup 事务内嵌套事务。
        trait = self.ensure_trait()

        def work(conn):
            suggestion_id = suggestion.get("suggestion_id")
            consumed = conn.execute(
                "SELECT suggestion_id FROM suggestion_consumption"
                " WHERE suggestion_id = ? AND target_module = 'personality'",
                (suggestion_id,),
            ).fetchone()
            if consumed is not None:
                existing = conn.execute(
                    "SELECT * FROM learning_influence_requests WHERE suggestion_id = ?", (suggestion_id,)
                ).fetchone()
                return {"created": False, "influence_request_id": existing["influence_request_id"] if existing else None}

            trait = self.ensure_trait()
            now = int(time.time() * 1000)
            influence_request_id = str(uuid.uuid4())
            conn.execute(
                "INSERT INTO learning_influence_requests"
                " (influence_request_id, suggestion_id, conclusion_id, target_trait_id, direction,"
                "  raw_strength, confidence, source_weight, status, created_at, window_expires_at, updated_at)"
                " VALUES (?, ?, ?, ?, 1, 0.05, ?, 1.0, 'pending', ?, ?, ?)",
                (
                    influence_request_id,
                    suggestion_id,
                    conclusion_id,
                    target_trait_id or trait["trait_id"],
                    float(suggestion.get("confidence") or 0.5),
                    now,
                    now + CONFIRMATION_WINDOW_MS,
                    now,
                ),
            )
            conn.execute(
                "INSERT INTO suggestion_consumption(suggestion_id, target_module, request_id, result,"
                " processed_at) VALUES (?, 'personality', ?, 'pending_confirmations', ?)",
                (suggestion_id, influence_request_id, now),
            )
            self.audit.write(
                conn, MODULE_ID, "learning_influence.request", "learning_influence_request",
                influence_request_id,
                after={"suggestion_id": suggestion_id, "conclusion_id": conclusion_id, "status": "pending"},
                request_id=request_id,
            )
            return {"created": True, "influence_request_id": influence_request_id}

        return self.dedup.run(
            MODULE_ID, request_id, "handle_learning_suggestion",
            {"suggestion_id": suggestion.get("suggestion_id"), "conclusion_id": conclusion_id},
            work,
        )

    # ---------- 确认证据 ----------

    def confirm_learning_influence(self, *, request_id, influence_request_id, experience_id, source_event_ids):
        def work(conn):
            req = conn.execute(
                "SELECT * FROM learning_influence_requests WHERE influence_request_id = ?",
                (influence_request_id,),
            ).fetchone()
            if req is None:
                raise KeyError("influence request not found: %s" % influence_request_id)
            now = int(time.time() * 1000)
            if req["status"] not in ("pending", "confirming"):
                return {"accepted": False, "status": req["status"], "reason": "not_pending"}
            if now > req["window_expires_at"]:
                conn.execute(
                    "UPDATE learning_influence_requests SET status = 'dropped', updated_at = ?"
                    " WHERE influence_request_id = ?",
                    (now, influence_request_id),
                )
                self.audit.write(
                    conn, MODULE_ID, "learning_influence.expired", "learning_influence_request",
                    influence_request_id, after={"status": "dropped"},
                )
                return {"accepted": False, "status": "dropped", "reason": "window_expired"}

            check = self.memory.check_experience(
                experience_id=experience_id,
                source_event_ids=source_event_ids or [],
                workspace_id=req["workspace_id"] if "workspace_id" in req.keys() else self._workspace_for(conn, req),
            )
            if not check["valid"] or check["verification_status"] != "verified":
                return {"accepted": False, "status": req["status"], "reason": check["reason"]}
            canonical = check["canonical_experience_id"]
            independence_key = check["independence_key"]
            if not independence_key:
                return {"accepted": False, "status": req["status"], "reason": "missing_independence_key"}

            # 时间语义：确认资格按经历实际发生时间（occurred_at）判定，不用证据入库时间。
            experienced_at = self._occurred_at(conn, canonical)

            # 产生建议的原始证据不得再次作为确认依据（建议创建前发生的结论证据）。
            conclusion = conn.execute(
                "SELECT * FROM conclusions WHERE conclusion_id = ?", (req["conclusion_id"],)
            ).fetchone()
            if conclusion is not None:
                origin_evidence = conn.execute(
                    "SELECT ce.experience_id FROM conclusion_evidence ce"
                    " JOIN memory_experience me ON me.experience_id = ce.experience_id"
                    " WHERE ce.conclusion_id = ? AND me.occurred_at < ?",
                    (req["conclusion_id"], req["created_at"]),
                ).fetchall()
                if any(str(r["experience_id"]) == str(canonical) for r in origin_evidence):
                    return {"accepted": False, "status": req["status"], "reason": "origin_evidence"}

            # 确认必须是建议创建之后发生的真实经历；旧经历不能补作确认。
            if experienced_at < int(req["created_at"]):
                return {"accepted": False, "status": req["status"], "reason": "not_new_since_suggestion"}

            existing = conn.execute(
                "SELECT confirmation_id, revoked_at FROM learning_influence_confirmations"
                " WHERE influence_request_id = ? AND independence_key = ?",
                (influence_request_id, independence_key),
            ).fetchone()
            if existing is not None:
                if existing["revoked_at"] is None:
                    return {"accepted": False, "status": req["status"], "reason": "duplicate_confirmation",
                            "confirmed_count": self._confirmed_count(conn, influence_request_id)}
                conn.execute(
                    "UPDATE learning_influence_confirmations SET revoked_at = NULL, experience_id = ?,"
                    " occurred_at = ? WHERE confirmation_id = ?",
                    (canonical, self._occurred_at(conn, canonical), existing["confirmation_id"]),
                )
            else:
                conn.execute(
                    "INSERT INTO learning_influence_confirmations(confirmation_id, influence_request_id,"
                    " experience_id, independence_key, occurred_at, created_at)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (str(uuid.uuid4()), influence_request_id, canonical, independence_key,
                     self._occurred_at(conn, canonical), now),
                )
            conn.execute(
                "UPDATE learning_influence_requests SET status = 'confirming', updated_at = ?"
                " WHERE influence_request_id = ?",
                (now, influence_request_id),
            )
            count = self._confirmed_count(conn, influence_request_id)
            self.audit.write(
                conn, MODULE_ID, "learning_influence.confirm", "learning_influence_request",
                influence_request_id, after={"confirmed_count": count, "experience_id": canonical},
                request_id=request_id,
            )
            if count >= MIN_CONFIRMATIONS:
                return self._activate(conn, req, now, request_id)
            return {"accepted": True, "status": "confirming", "confirmed_count": count,
                    "required_count": MIN_CONFIRMATIONS}

        return self.dedup.run(
            MODULE_ID, request_id, "confirm_learning_influence",
            {"influence_request_id": influence_request_id, "experience_id": experience_id}, work,
        )

    def _workspace_for(self, conn, req):
        conclusion = conn.execute(
            "SELECT workspace_id FROM conclusions WHERE conclusion_id = ?", (req["conclusion_id"],)
        ).fetchone()
        return conclusion["workspace_id"] if conclusion else "unknown"

    def _occurred_at(self, conn, experience_id):
        row = conn.execute(
            "SELECT occurred_at FROM memory_experience WHERE experience_id = ?", (experience_id,)
        ).fetchone()
        return int(row["occurred_at"]) if row else int(time.time() * 1000)

    def _confirmed_count(self, conn, influence_request_id):
        return conn.execute(
            "SELECT COUNT(*) AS n FROM learning_influence_confirmations"
            " WHERE influence_request_id = ? AND revoked_at IS NULL",
            (influence_request_id,),
        ).fetchone()["n"]

    def _activate(self, conn, req, now, request_id):
        instance_id = str(uuid.uuid4())
        strength = req["raw_strength"] * req["confidence"] * req["source_weight"]  # 只算一次
        conn.execute(
            "INSERT INTO influence_instances(instance_id, trait_id, direction, raw_strength, confidence,"
            " source_weight, strength, remaining_amount, settled_amount, source_kind, source_refs,"
            " eligible_at, status, created_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 'learning_suggestion', ?, ?, 'active', ?)",
            (
                instance_id, req["target_trait_id"], 1, req["raw_strength"], req["confidence"],
                req["source_weight"], strength, strength,
                json.dumps({"influence_request_id": req["influence_request_id"]}), now, now,
            ),
        )
        conn.execute(
            "UPDATE learning_influence_requests SET status = 'active', activated_at = ?, updated_at = ?"
            " WHERE influence_request_id = ?",
            (now, now, req["influence_request_id"]),
        )
        self.audit.write(
            conn, MODULE_ID, "learning_influence.activate", "learning_influence_request",
            req["influence_request_id"],
            after={"instance_id": instance_id, "strength": strength, "status": "active"},
            request_id=request_id,
        )
        return {"accepted": True, "status": "active", "confirmed_count": MIN_CONFIRMATIONS,
                "required_count": MIN_CONFIRMATIONS, "instance_id": instance_id}

    # ---------- 失效处理 ----------

    def revoke_confirmations_for_experience(self, conn, *, experience_id):
        """pending 时证据失效：撤销确认；已激活的只记录审计，不回滚人格。"""
        rows = conn.execute(
            "SELECT c.confirmation_id, c.influence_request_id, r.status"
            " FROM learning_influence_confirmations c JOIN learning_influence_requests r"
            " ON r.influence_request_id = c.influence_request_id"
            " WHERE c.experience_id = ? AND c.revoked_at IS NULL",
            (experience_id,),
        ).fetchall()
        now = int(time.time() * 1000)
        for row in rows:
            if row["status"] == "active":
                self.audit.write(
                    conn, MODULE_ID, "learning_influence.post_activation_invalidated",
                    "learning_influence_request", row["influence_request_id"],
                    after={"experience_id": experience_id, "rollback": False},
                )
                continue
            conn.execute(
                "UPDATE learning_influence_confirmations SET revoked_at = ? WHERE confirmation_id = ?",
                (now, row["confirmation_id"]),
            )
            self.audit.write(
                conn, MODULE_ID, "learning_influence.confirmation_revoked",
                "learning_influence_request", row["influence_request_id"],
                after={"experience_id": experience_id},
            )
        return {"revoked": len(rows)}

    # ---------- 读取 ----------

    def relocate_personality_confirmations_for_merge(self, *, request_id, operation_id, workspace_id,
                                                     source_experience_id, canonical_experience_id,
                                                     mapping_version, conn=None):
        """E4-C2-3：未激活确认记录 canonical 重解析；同一 request 同 canonical 只保留最早一条。"""
        def work(conn):
            rows = conn.execute(
                "SELECT c.*, r.status AS request_status FROM learning_influence_confirmations c"
                " JOIN learning_influence_requests r ON r.influence_request_id = c.influence_request_id"
                " WHERE c.experience_id = ? AND c.revoked_at IS NULL",
                (source_experience_id,),
            ).fetchall()
            relocated = 0
            revoked = []
            skipped = []
            affected_requests = set()
            for row in rows:
                if row["request_status"] == "active":
                    skipped.append(row["confirmation_id"])  # 已激活：不碰
                    continue
                resolved = self.memory.resolve_canonical_experience(
                    row["experience_id"], workspace_id)
                if resolved["resolution_status"] != "ok":
                    skipped.append(row["confirmation_id"])  # 异常链：不迁移
                    continue
                target = resolved["canonical_experience_id"]
                conn.execute(
                    "UPDATE learning_influence_confirmations SET experience_id = ?"
                    " WHERE confirmation_id = ?",
                    (target, row["confirmation_id"]),
                )
                relocated += 1
                affected_requests.add(row["influence_request_id"])

            now = int(time.time() * 1000)
            for influence_request_id in affected_requests:
                confirmations = conn.execute(
                    "SELECT * FROM learning_influence_confirmations"
                    " WHERE influence_request_id = ? AND revoked_at IS NULL"
                    " ORDER BY created_at ASC, confirmation_id ASC",
                    (influence_request_id,),
                ).fetchall()
                seen = set()
                for confirmation in confirmations:
                    key = confirmation["experience_id"]
                    if key in seen:
                        conn.execute(
                            "UPDATE learning_influence_confirmations SET revoked_at = ?"
                            " WHERE confirmation_id = ?",
                            (now, confirmation["confirmation_id"]),
                        )
                        revoked.append(confirmation["confirmation_id"])
                        self.audit.write(
                            conn, MODULE_ID, "learning_influence.confirmation_dedup",
                            "learning_influence_confirmation", confirmation["confirmation_id"],
                            after={"operation_id": operation_id,
                                   "influence_request_id": influence_request_id,
                                   "experience_id": key, "reason": "merged_same_experience"},
                        )
                    else:
                        seen.add(key)
            self.audit.write(
                conn, MODULE_ID, "learning_influence.relocate", "learning_influence_requests",
                source_experience_id,
                after={"operation_id": operation_id, "relocated": relocated,
                       "revoked": len(revoked), "skipped": len(skipped),
                       "mapping_version": mapping_version},
            )
            return {"relocated": relocated, "revoked": revoked, "skipped": skipped}

        if conn is not None:
            return work(conn)
        return self.dedup.run(
            MODULE_ID, request_id, "relocate_confirmations",
            {"operation_id": operation_id, "source": source_experience_id,
             "canonical": canonical_experience_id}, work,
        )

    def get_request(self, influence_request_id):
        row = self.db.conn.execute(
            "SELECT * FROM learning_influence_requests WHERE influence_request_id = ?",
            (influence_request_id,),
        ).fetchone()
        return dict(row) if row else None

    def instances(self, trait_id=None):
        sql = "SELECT * FROM influence_instances"
        params = ()
        if trait_id:
            sql += " WHERE trait_id = ?"
            params = (trait_id,)
        return [dict(r) for r in self.db.conn.execute(sql + " ORDER BY created_at", params)]

    def get_summary(self):
        trait = self.get_trait()
        instances = self.instances(trait["trait_id"] if trait else None)
        return {
            "trait": trait,
            "instances": instances,
            "state_version": trait["state_version"] if trait else 0,
        }

    # ---------- P14-C：结算（V1.2 §五） ----------

    def settle(self, *, request_id, trait_name=DEFAULT_TRAIT, now=None, trigger="settlement.natural.daily"):
        """每日自然结算：正负压力分别判断 → 屈服 → 边界优先 → 分摊 → 记账。

        P14-C 不实现衰减事件（remaining 只减不增，衰减留 P14-E/P15）。
        """
        def work(conn):
            now_ms = int(now if now is not None else time.time() * 1000)
            trait = conn.execute(
                "SELECT * FROM personality_traits WHERE name = ?", (trait_name,)
            ).fetchone()
            if trait is None:
                raise KeyError("trait not found: %s" % trait_name)

            rows = conn.execute(
                "SELECT * FROM influence_instances WHERE trait_id = ?"
                " AND status IN ('active', 'partially_settled') AND eligible_at <= ?",
                (trait["trait_id"], now_ms),
            ).fetchall()
            positive = [r for r in rows if r["direction"] == 1 and r["remaining_amount"] > 0]
            negative = [r for r in rows if r["direction"] == -1 and r["remaining_amount"] > 0]
            pressure_pos = sum(r["remaining_amount"] for r in positive)
            pressure_neg = sum(r["remaining_amount"] for r in negative)
            threshold = float(trait["base_yield_threshold"]) * float(trait["resistance"])

            def record_no_change(reason, direction=None):
                conn.execute(
                    "INSERT INTO personality_settlements(settlement_id, trait_id, trigger, old_value,"
                    " new_value, actual_change, selected_direction, positive_pressure, negative_pressure,"
                    " threshold, resistance, release, instance_allocations, config_version, created_at)"
                    " VALUES (?, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, 0, ?, 1, ?)",
                    (str(uuid.uuid4()), trait["trait_id"], trigger, trait["value"], trait["value"],
                     direction, pressure_pos, pressure_neg, threshold, trait["resistance"],
                     json.dumps({"reason": reason}), now_ms),
                )
                return {"settled": False, "reason": reason, "positive_pressure": pressure_pos,
                        "negative_pressure": pressure_neg, "threshold": threshold}

            # 方向交替暂缓：上周期触发过 hold → 本周期暂缓一次。
            if trait["settlement_hold"]:
                conn.execute(
                    "UPDATE personality_traits SET settlement_hold = 0, last_settled_at = ? WHERE trait_id = ?",
                    (now_ms, trait["trait_id"]),
                )
                return record_no_change("direction_hold")

            direction = None
            if pressure_pos > threshold and pressure_neg > threshold:
                if abs(pressure_pos - pressure_neg) < 0.05:
                    conn.execute(
                        "UPDATE personality_traits SET settlement_hold = 1 WHERE trait_id = ?",
                        (trait["trait_id"],),
                    )
                    return record_no_change("both_directions_ambiguous")
                direction = 1 if pressure_pos > pressure_neg else -1
            elif pressure_pos > threshold:
                direction = 1
            elif pressure_neg > threshold:
                direction = -1

            if direction is None:
                return record_no_change("below_threshold")

            # 连续两周期方向交替 → 强制暂缓至少一个周期。
            last_direction = trait["last_settled_direction"]
            if last_direction is not None and last_direction != direction:
                conn.execute(
                    "UPDATE personality_traits SET last_settled_direction = NULL, last_settled_at = ?"
                    " WHERE trait_id = ?",
                    (now_ms, trait["trait_id"]),
                )
                return record_no_change("direction_alternation_hold", direction)

            selected_pressure = pressure_pos if direction == 1 else pressure_neg
            release = min(MAX_CHANGE * math.tanh((selected_pressure - threshold) / RELEASE_SCALE), selected_pressure)
            old_value = float(trait["value"])
            boundary_remaining = (1.0 - old_value) if direction == 1 else old_value
            actual_release = min(release, boundary_remaining)

            if actual_release <= 1e-9:
                return record_no_change("boundary_reached", direction)

            selected_instances = positive if direction == 1 else negative
            total = sum(r["remaining_amount"] for r in selected_instances)
            allocations = []
            for row in selected_instances:
                share = actual_release * row["remaining_amount"] / total
                allocations.append({"instance_id": row["instance_id"], "share": share})
            # 余差补给最大实例，保证 Σshare = actual_release。
            residual = actual_release - sum(a["share"] for a in allocations)
            if allocations:
                allocations[0]["share"] += residual
            for allocation in allocations:
                row = conn.execute(
                    "SELECT * FROM influence_instances WHERE instance_id = ?", (allocation["instance_id"],)
                ).fetchone()
                new_remaining = max(0.0, float(row["remaining_amount"]) - allocation["share"])
                new_settled = float(row["settled_amount"]) + allocation["share"]
                status = "settled" if new_remaining <= 1e-9 else "partially_settled"
                cursor = conn.execute(
                    "UPDATE influence_instances SET remaining_amount = ?, settled_amount = ?, status = ?,"
                    " lock_version = lock_version + 1 WHERE instance_id = ? AND lock_version = ?",
                    (new_remaining, new_settled, status, row["instance_id"], int(row["lock_version"])),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError("lock_version conflict: %s" % row["instance_id"])

            new_value = max(0.0, min(1.0, old_value + direction * actual_release))
            conn.execute(
                "UPDATE personality_traits SET value = ?, state_version = state_version + 1,"
                " last_settled_direction = ?, last_settled_at = ?, settlement_hold = 0, updated_at = ?"
                " WHERE trait_id = ?",
                (new_value, direction, now_ms, now_ms, trait["trait_id"]),
            )
            conn.execute(
                "INSERT INTO personality_settlements(settlement_id, trait_id, trigger, old_value,"
                " new_value, actual_change, selected_direction, positive_pressure, negative_pressure,"
                " threshold, resistance, release, instance_allocations, config_version, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)",
                (str(uuid.uuid4()), trait["trait_id"], trigger, old_value, new_value, actual_release,
                 direction, pressure_pos, pressure_neg, threshold, trait["resistance"], actual_release,
                 json.dumps(allocations), now_ms),
            )
            self.audit.write(
                conn, MODULE_ID, "personality.settle", "trait", trait["trait_id"],
                before={"value": old_value},
                after={
                    "trigger": trigger,
                    "new_value": new_value,
                    "actual_change": actual_release,
                    "selected_direction": direction,
                    "positive_pressure": pressure_pos,
                    "negative_pressure": pressure_neg,
                    "threshold": threshold,
                    "resistance": trait["resistance"],
                    "instance_allocations": allocations,
                    "config_version": 1,
                },
                request_id=request_id,
            )
            return {
                "settled": True,
                "old_value": old_value,
                "new_value": new_value,
                "actual_change": actual_release,
                "selected_direction": direction,
                "positive_pressure": pressure_pos,
                "negative_pressure": pressure_neg,
                "threshold": threshold,
                "allocations": allocations,
                "state_version": int(trait["state_version"]) + 1,
            }

        return self.dedup.run(
            MODULE_ID, request_id, "settle", {"trait_name": trait_name, "now": now}, work
        )
