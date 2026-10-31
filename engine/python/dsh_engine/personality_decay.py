"""P14-E2：人格压力衰减（只改实例池剩余压力，不碰 Trait）。

指数衰减：R_new = R_old × exp(−ln2/H × active_days)
active_days = (as_of − max(anchor + G, last_decay_at)) / 1 天
宽限期内不衰减；增量计算以当前 remaining 为基础；衰减只减不增。
"""

import json
import math
import time
import uuid

MODULE_ID = "personality"
DAY_MS = 86_400_000
DECAY_GRACE_DAYS = 30.0
DECAY_HALF_LIFE_DAYS = 180.0
DECAY_EPSILON = 1e-6
DECAY_CONFIG_VERSION = "p14e2-v1"


class PressureDecayService:
    def __init__(self, db, dedup, audit):
        self.db = db
        self.dedup = dedup
        self.audit = audit

    def _ensure_state(self, conn, instance):
        state = conn.execute(
            "SELECT * FROM personality_pressure_decay_state WHERE instance_id = ?",
            (instance["instance_id"],),
        ).fetchone()
        if state is not None:
            return state
        now = int(time.time() * 1000)
        anchor = int(instance["created_at"])
        conn.execute(
            "INSERT INTO personality_pressure_decay_state(instance_id, anchor_at, last_decay_at,"
            " revision, created_at, updated_at) VALUES (?, ?, ?, 0, ?, ?)",
            (instance["instance_id"], anchor, anchor, now, now),
        )
        return conn.execute(
            "SELECT * FROM personality_pressure_decay_state WHERE instance_id = ?",
            (instance["instance_id"],),
        ).fetchone()

    def decay(self, *, request_id, as_of, cycle_id=None):
        as_of = int(as_of)

        def work(conn):
            instances = conn.execute(
                "SELECT * FROM influence_instances WHERE status IN ('active', 'partially_settled')"
                " AND remaining_amount > 0 AND eligible_at <= ?",
                (as_of,),
            ).fetchall()
            processed = 0
            decayed = 0
            skipped = 0
            total_decay = 0.0
            event_ids = []
            rate = math.log(2.0) / DECAY_HALF_LIFE_DAYS

            for instance in instances:
                processed += 1
                state = self._ensure_state(conn, instance)
                anchor = int(state["anchor_at"])
                last = int(state["last_decay_at"])
                active_start = max(anchor + int(DECAY_GRACE_DAYS * DAY_MS), last)
                if as_of <= active_start:
                    skipped += 1
                    continue
                active_days = (as_of - active_start) / DAY_MS
                r_old = float(instance["remaining_amount"])
                r_new = r_old * math.exp(-rate * active_days)
                amount = r_old - r_new
                reason = "exponential"
                if amount < DECAY_EPSILON:
                    if r_new <= DECAY_EPSILON:
                        amount = r_old
                        r_new = 0.0
                        reason = "epsilon_zero"
                    else:
                        skipped += 1
                        continue

                cursor = conn.execute(
                    "UPDATE influence_instances SET remaining_amount = ?,"
                    " lock_version = lock_version + 1 WHERE instance_id = ? AND lock_version = ?",
                    (r_new, instance["instance_id"], int(instance["lock_version"])),
                )
                if cursor.rowcount != 1:
                    raise RuntimeError("lock_version conflict: %s" % instance["instance_id"])
                now = int(time.time() * 1000)
                conn.execute(
                    "UPDATE personality_pressure_decay_state SET anchor_at = ?, last_decay_at = ?,"
                    " revision = revision + 1, updated_at = ? WHERE instance_id = ?",
                    (anchor, as_of, now, instance["instance_id"]),
                )
                event_id = str(uuid.uuid4())
                conn.execute(
                    "INSERT INTO personality_pressure_decay_events(event_id, instance_id, request_id,"
                    " cycle_id, decay_before, decay_amount, decay_after, anchor_at, last_decay_at,"
                    " applied_at, active_days, half_life_days, grace_period_days, config_version,"
                    " lock_version_before, lock_version_after, reason)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        event_id, instance["instance_id"], request_id, cycle_id, r_old, amount, r_new,
                        anchor, as_of, now, active_days, DECAY_HALF_LIFE_DAYS, DECAY_GRACE_DAYS,
                        DECAY_CONFIG_VERSION, int(instance["lock_version"]), int(instance["lock_version"]) + 1,
                        reason,
                    ),
                )
                self.audit.write(
                    conn, MODULE_ID, "personality_pressure_decay", "influence_instance",
                    instance["instance_id"],
                    before={"remaining_amount": r_old},
                    after={
                        "decay_amount": amount,
                        "remaining_amount": r_new,
                        "anchor_at": anchor,
                        "applied_at": now,
                        "active_days": active_days,
                        "config_version": DECAY_CONFIG_VERSION,
                        "cycle_id": cycle_id,
                        "reason": reason,
                    },
                    request_id=request_id,
                )
                decayed += 1
                total_decay += amount
                event_ids.append(event_id)

            status = "completed" if decayed > 0 else "no_eligible_instances"
            return {
                "status": status,
                "request_id": request_id,
                "as_of": as_of,
                "processed_instances": processed,
                "decayed_instances": decayed,
                "total_decay_amount": total_decay,
                "skipped_instances": skipped,
                "state_revision": 0,
                "event_ids": event_ids,
            }

        return self.dedup.run(
            MODULE_ID,
            request_id,
            "decay_pressure",
            {"as_of": as_of, "cycle_id": cycle_id},
            work,
        )

    def refresh_anchor_for_conclusion(self, conn, *, conclusion_id, evidence_id, experience_id,
                                      occurred_at, independence_key, accepted_at):
        """新证据刷新：同结论实例的衰减锚点（只重置未来，不恢复已衰减压力）。"""
        instances = conn.execute(
            "SELECT i.* FROM influence_instances i"
            " WHERE i.source_refs IS NOT NULL AND EXISTS ("
            "   SELECT 1 FROM learning_influence_requests r"
            "   WHERE r.conclusion_id = ? AND i.source_refs LIKE '%' || r.influence_request_id || '%'"
            " )",
            (conclusion_id,),
        ).fetchall()
        refreshed = []
        for instance in instances:
            state = self._ensure_state(conn, instance)
            if occurred_at < int(instance["created_at"]):
                continue
            # 同一证据只刷新一次（事件账本留痕）。
            already = conn.execute(
                "SELECT 1 FROM personality_pressure_decay_events"
                " WHERE instance_id = ? AND request_id = ? AND reason = 'evidence_refresh'",
                (instance["instance_id"], "refresh:%s" % evidence_id),
            ).fetchone()
            if already is not None:
                continue
            now = int(time.time() * 1000)
            conn.execute(
                "UPDATE personality_pressure_decay_state SET anchor_at = ?, last_decay_at = ?,"
                " revision = revision + 1, updated_at = ? WHERE instance_id = ?",
                (int(accepted_at), int(accepted_at), now, instance["instance_id"]),
            )
            conn.execute(
                "INSERT INTO personality_pressure_decay_events(event_id, instance_id, request_id,"
                " cycle_id, decay_before, decay_amount, decay_after, anchor_at, last_decay_at,"
                " applied_at, active_days, half_life_days, grace_period_days, config_version,"
                " lock_version_before, lock_version_after, reason)"
                " VALUES (?, ?, ?, NULL, ?, 0, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, 'evidence_refresh')",
                (
                    str(uuid.uuid4()), instance["instance_id"], "refresh:%s" % evidence_id,
                    float(instance["remaining_amount"]), float(instance["remaining_amount"]),
                    int(accepted_at), int(accepted_at), now, DECAY_HALF_LIFE_DAYS,
                    DECAY_GRACE_DAYS, DECAY_CONFIG_VERSION, int(instance["lock_version"]),
                    int(instance["lock_version"]),
                ),
            )
            self.audit.write(
                conn, MODULE_ID, "personality_decay.anchor_refresh", "influence_instance",
                instance["instance_id"],
                after={
                    "evidence_id": evidence_id,
                    "experience_id": experience_id,
                    "accepted_at": accepted_at,
                    "anchor_at": accepted_at,
                },
            )
            refreshed.append(instance["instance_id"])
        return refreshed
