"""P11：学习模块接入最小实现（V1.3.1）。

范围：
- 结论（conclusions）与候选结论（proposed_conclusions）分表；
- 证据写入前调用记忆模块核验（check_experience），不接受自造身份；
- 结论评估（独立 support 计数）；
- 建议（suggestions）生成：至少 2 条独立 verified 支持证据，进 pending 队列；
- 证据撤销（experience.invalidated）。

不实现：候选聚类/晋升、人格消费、影子模式、租约、权重校准。
"""

import json
import time
import uuid

MODULE_ID = "learning"
MIN_INDEPENDENT_SUPPORT = 2
DEFAULT_TARGET_MODULE = "personality"
SUGGESTION_TYPE = "personality_influence"


class LearningService:
    def __init__(self, db, dedup, audit, memory, outbox=None):
        self.db = db
        self.dedup = dedup
        self.audit = audit
        self.memory = memory
        self.outbox = outbox

    # ---------- 结论 ----------

    def create_conclusion(self, *, request_id, workspace_id, statement):
        """创建正式结论（P11 工程接口；生产聚类留给后续阶段）。"""

        def work(conn):
            conclusion_id = str(uuid.uuid4())
            now = int(time.time() * 1000)
            conn.execute(
                "INSERT INTO conclusions(conclusion_id, workspace_id, statement, status, created_at, updated_at)"
                " VALUES (?, ?, ?, 'candidate', ?, ?)",
                (conclusion_id, workspace_id, statement, now, now),
            )
            self.audit.write(
                conn, MODULE_ID, "conclusion.create", "conclusion", conclusion_id,
                after={"statement": statement}, request_id=request_id,
            )
            return {"created": True, "conclusion_id": conclusion_id}

        return self.dedup.run(
            MODULE_ID, request_id, "create_conclusion",
            {"workspace_id": workspace_id, "statement": statement}, work,
        )

    def propose_conclusion(self, *, workspace_id, statement, source_experience_id):
        """候选结论：不参与正式评估与建议生成。"""
        with self.db.transaction() as conn:
            proposal_id = str(uuid.uuid4())
            now = int(time.time() * 1000)
            conn.execute(
                "INSERT INTO proposed_conclusions(proposal_id, workspace_id, statement,"
                " source_experience_id, status, created_at)"
                " VALUES (?, ?, ?, ?, 'proposed', ?)",
                (proposal_id, workspace_id, statement, source_experience_id, now),
            )
            self.audit.write(
                conn, MODULE_ID, "proposed_conclusion.create", "proposed_conclusion", proposal_id,
                after={"statement": statement},
            )
            return {"proposal_id": proposal_id}

    def get_conclusion(self, conclusion_id):
        row = self.db.conn.execute(
            "SELECT * FROM conclusions WHERE conclusion_id = ?", (conclusion_id,)
        ).fetchone()
        return dict(row) if row else None

    # ---------- 证据 ----------

    def add_conclusion_evidence(
        self,
        *,
        request_id,
        workspace_id,
        conclusion_id,
        experience_id,
        source_event_ids,
        polarity,
        evidence_type="",
        rationale="",
    ):
        if polarity not in ("support", "against"):
            raise ValueError("polarity must be support/against")
        check = self.memory.check_experience(
            experience_id=experience_id,
            source_event_ids=source_event_ids or [],
            workspace_id=workspace_id,
        )
        if not check["valid"]:
            raise ValueError("experience check failed: %s" % check["reason"])
        if check["verification_status"] != "verified":
            raise ValueError("only verified experiences can become learning evidence")

        canonical = check["canonical_experience_id"]

        def work(conn):
            row = conn.execute(
                "SELECT conclusion_id FROM conclusions WHERE conclusion_id = ?", (conclusion_id,)
            ).fetchone()
            if row is None:
                raise KeyError("conclusion not found: %s" % conclusion_id)
            existing = conn.execute(
                "SELECT evidence_id FROM conclusion_evidence WHERE conclusion_id = ?"
                " AND experience_id = ? AND polarity = ?",
                (conclusion_id, canonical, polarity),
            ).fetchone()
            if existing is not None:
                return {"created": False, "deduplicated": True, "evidence_id": existing["evidence_id"]}
            evidence_id = str(uuid.uuid4())
            now = int(time.time() * 1000)
            conn.execute(
                "INSERT INTO conclusion_evidence(evidence_id, conclusion_id, experience_id,"
                " polarity, evidence_type, rationale, revoked_at, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?, NULL, ?)",
                (evidence_id, conclusion_id, canonical, polarity, evidence_type, rationale, now),
            )
            self.audit.write(
                conn, MODULE_ID, "conclusion_evidence.add", "conclusion_evidence", evidence_id,
                after={"conclusion_id": conclusion_id, "polarity": polarity},
                request_id=request_id,
            )
            return {"created": True, "evidence_id": evidence_id}

        return self.dedup.run(
            MODULE_ID, request_id, "add_conclusion_evidence",
            {"conclusion_id": conclusion_id, "experience_id": canonical, "polarity": polarity},
            work,
        )

    def revoke_evidence_for_experience(self, conn, *, experience_id):
        """experience.invalidated：撤销相关证据并重评（事务由调用方提供）。"""
        rows = conn.execute(
            "SELECT DISTINCT conclusion_id FROM conclusion_evidence"
            " WHERE experience_id = ? AND revoked_at IS NULL",
            (experience_id,),
        ).fetchall()
        conn.execute(
            "UPDATE conclusion_evidence SET revoked_at = ? WHERE experience_id = ? AND revoked_at IS NULL",
            (int(time.time() * 1000), experience_id),
        )
        for row in rows:
            self.audit.write(
                conn, MODULE_ID, "conclusion_evidence.revoke", "conclusion_evidence",
                row["conclusion_id"], after={"experience_id": experience_id},
            )
        return {"revoked": len(rows)}

    # ---------- 评估与建议 ----------

    def _count_independent(self, conn, conclusion_id, polarity, workspace_id):
        """按 canonical 去重计数独立来源；终态不计；异常链不猜身份。"""
        rows = conn.execute(
            "SELECT experience_id FROM conclusion_evidence WHERE conclusion_id = ?"
            " AND polarity = ? AND revoked_at IS NULL",
            (conclusion_id, polarity),
        ).fetchall()
        keys = set()
        anomalies = []
        for row in rows:
            resolved = self.memory.resolve_canonical_experience(row["experience_id"], workspace_id)
            status = resolved["resolution_status"]
            if status == "ok":
                keys.add(resolved["canonical_experience_id"])
            elif status in ("split_terminal", "invalid_terminal"):
                continue
            else:
                anomalies.append({"experience_id": row["experience_id"], "resolution_status": status})
        return len(keys), anomalies

    def relocate_conclusion_evidence_for_merge(self, *, request_id, operation_id, workspace_id,
                                               source_experience_id, canonical_experience_id,
                                               mapping_version, conn=None):
        """E4-C2-2：merge 学习证据重关联；唯一键冲突时保留原记录（不撤销、不删除）。"""
        def work(conn):
            rows = conn.execute(
                "SELECT * FROM conclusion_evidence WHERE experience_id = ? AND revoked_at IS NULL",
                (source_experience_id,),
            ).fetchall()
            relocated = 0
            kept = []
            for row in rows:
                existing = conn.execute(
                    "SELECT evidence_id FROM conclusion_evidence WHERE conclusion_id = ?"
                    " AND experience_id = ? AND polarity = ?",
                    (row["conclusion_id"], canonical_experience_id, row["polarity"]),
                ).fetchone()
                if existing is None:
                    conn.execute(
                        "UPDATE conclusion_evidence SET experience_id = ? WHERE evidence_id = ?",
                        (canonical_experience_id, row["evidence_id"]),
                    )
                    relocated += 1
                else:
                    kept.append(row["evidence_id"])
                    self.audit.write(
                        conn, MODULE_ID, "conclusion_evidence.conflict_kept", "conclusion_evidence",
                        row["evidence_id"],
                        after={"operation_id": operation_id,
                               "target_experience_id": canonical_experience_id,
                               "reason": "unique_conflict_kept"},
                    )
            self.audit.write(
                conn, MODULE_ID, "conclusion_evidence.relocate", "conclusion_evidence",
                source_experience_id,
                after={"operation_id": operation_id, "relocated": relocated,
                       "conflict_kept": len(kept), "mapping_version": mapping_version},
            )
            return {"relocated": relocated, "conflict_kept": kept}

        if conn is not None:
            return work(conn)
        return self.dedup.run(
            MODULE_ID, request_id, "relocate_conclusion_evidence",
            {"operation_id": operation_id, "source": source_experience_id,
             "canonical": canonical_experience_id}, work,
        )

    def evaluate_conclusion(self, *, request_id, conclusion_id, min_support=MIN_INDEPENDENT_SUPPORT):
        def work(conn):
            row = conn.execute(
                "SELECT * FROM conclusions WHERE conclusion_id = ?", (conclusion_id,)
            ).fetchone()
            if row is None:
                raise KeyError("conclusion not found: %s" % conclusion_id)
            support, support_anomalies = self._count_independent(
                conn, conclusion_id, "support", row["workspace_id"])
            against, against_anomalies = self._count_independent(
                conn, conclusion_id, "against", row["workspace_id"])
            anomalies = support_anomalies + against_anomalies
            if row["status"] == "candidate" and support >= min_support and support > against:
                now = int(time.time() * 1000)
                conn.execute(
                    "UPDATE conclusions SET status = 'active', updated_at = ? WHERE conclusion_id = ?",
                    (now, conclusion_id),
                )
                suggestion = self._ensure_suggestion(conn, row, support)
                self.audit.write(
                    conn, MODULE_ID, "conclusion.evaluate", "conclusion", conclusion_id,
                    after={"status": "active", "support": support, "against": against, "anomalies": anomalies},
                    request_id=request_id,
                )
                return {"status": "active", "support": support, "against": against,
                    "suggestion": suggestion, "anomalies": anomalies}
            if row["status"] == "active":
                # 已生效结论重复评估：幂等返回同一窗口内的建议。
                suggestion = self._ensure_suggestion(conn, row, support)
                return {"status": "active", "support": support, "against": against,
                    "suggestion": suggestion, "anomalies": anomalies}
            return {"status": row["status"], "support": support, "against": against,
                    "suggestion": None, "anomalies": anomalies}

        return self.dedup.run(
            MODULE_ID, request_id, "evaluate_conclusion", {"conclusion_id": conclusion_id}, work
        )

    def _ensure_suggestion(self, conn, conclusion_row, support):
        cooldown_window = time.strftime("%Y-%m-%d")
        payload = {
            "conclusion_id": conclusion_row["conclusion_id"],
            "statement": conclusion_row["statement"],
            "support_count": support,
        }
        normalized = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        dedup_key = "%s|%s|%s|%s|%s" % (
            DEFAULT_TARGET_MODULE, SUGGESTION_TYPE, conclusion_row["conclusion_id"], normalized, cooldown_window,
        )
        existing = conn.execute(
            "SELECT suggestion_id, status FROM suggestions WHERE dedup_key = ?", (dedup_key,)
        ).fetchone()
        if existing is not None:
            return {"suggestion_id": existing["suggestion_id"], "status": existing["status"], "deduplicated": True}
        suggestion_id = str(uuid.uuid4())
        now = int(time.time() * 1000)
        conn.execute(
            "INSERT INTO suggestions(suggestion_id, target_module, suggestion_type, source_conclusion_id,"
            " payload, dedup_key, status, created_at, updated_at)"
            " VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?)",
            (suggestion_id, DEFAULT_TARGET_MODULE, SUGGESTION_TYPE, conclusion_row["conclusion_id"],
             normalized, dedup_key, now, now),
        )
        conn.execute(
            "INSERT INTO suggestion_conclusion_links(suggestion_id, conclusion_id, created_at)"
            " VALUES (?, ?, ?)",
            (suggestion_id, conclusion_row["conclusion_id"], now),
        )
        self.audit.write(
            conn, MODULE_ID, "suggestion.create", "suggestion", suggestion_id,
            after={"target_module": DEFAULT_TARGET_MODULE, "conclusion_id": conclusion_row["conclusion_id"]},
        )
        # P12：建议事件供人格模块领取（Suggestions 仍是唯一入口）。
        if self.outbox is not None:
            self.outbox.append(
                conn,
                event_type="learning.suggestion.created",
                producer_module=MODULE_ID,
                aggregate_type="suggestion",
                aggregate_id=suggestion_id,
                payload={
                    "suggestion_id": suggestion_id,
                    "conclusion_id": conclusion_row["conclusion_id"],
                    "target_module": DEFAULT_TARGET_MODULE,
                },
            )
        return {"suggestion_id": suggestion_id, "status": "pending", "deduplicated": False}

    def revoke_suggestion(self, *, request_id, suggestion_id, reason):
        def work(conn):
            row = conn.execute(
                "SELECT status FROM suggestions WHERE suggestion_id = ?", (suggestion_id,)
            ).fetchone()
            if row is None:
                raise KeyError("suggestion not found: %s" % suggestion_id)
            conn.execute(
                "UPDATE suggestions SET status = 'revoked', updated_at = ? WHERE suggestion_id = ?",
                (int(time.time() * 1000), suggestion_id),
            )
            self.audit.write(
                conn, MODULE_ID, "suggestion.revoke", "suggestion", suggestion_id,
                before={"status": row["status"]}, after={"status": "revoked", "reason": reason},
                request_id=request_id,
            )
            return {"revoked": True, "suggestion_id": suggestion_id}

        return self.dedup.run(
            MODULE_ID, request_id, "revoke_suggestion",
            {"suggestion_id": suggestion_id, "reason": reason}, work,
        )

    def pending_suggestions(self, limit=20):
        rows = self.db.conn.execute(
            "SELECT * FROM suggestions WHERE status = 'pending' ORDER BY created_at LIMIT ?", (limit,)
        ).fetchall()
        return [dict(row) for row in rows]
