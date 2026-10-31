"""记忆模块最小真实链路（P4）。

范围：Experience 创建、原始事件关联、Audit、Outbox 事件、按会话摘要。
不做：模型核验、canonical 归一、拆分、independence_key 归一、重要记忆与索引。

契约依据：记忆模块 V1.1.2（pending 状态、canonical 初始指向自身、
同一事件集合去重、部分重叠不自动合并、不复制证据）。
"""

import json
import time
import uuid

# session_id 是 P4 扩展字段：只记录创建上下文用于按会话统计，不参与权威规则。
MODULE_ID = "memory"


class MemoryService:
    def __init__(self, db, dedup, audit, outbox):
        self.db = db
        self.dedup = dedup
        self.audit = audit
        self.outbox = outbox

    # ---------- Experience ----------

    def create_experience(
        self,
        *,
        request_id,
        workspace_id,
        session_id,
        event_ids,
        experience_type,
        occurred_at,
        subject_id,
        description,
    ):
        # 幂等 payload 只包含去重依据。session_id 是创建上下文元数据，
        # 不参与幂等/业务判断（同一事件集合从不同会话重放应返回同一经历）。
        payload = {
            "workspace_id": workspace_id,
            "event_ids": sorted(set(event_ids)),
            "experience_type": experience_type,
        }

        def work(conn):
            normalized = sorted(set(event_ids))
            if not normalized:
                raise ValueError("create_experience requires at least one event_id")

            # 1. 完全相同的 source 事件集合 → 返回已有经验（不重复建）。
            exact = self._find_by_event_set(conn, normalized)
            if exact is not None:
                return {"created": False, "deduplicated": True, "experience_id": exact}

            # 2. 部分重叠 → possible_duplicate，不自动合并。
            overlaps = self._find_overlaps(conn, normalized)
            if overlaps:
                return {
                    "created": False,
                    "possible_duplicate": True,
                    "matches": overlaps,
                }

            now = int(time.time() * 1000)
            experience_id = str(uuid.uuid4())
            independence_key = str(uuid.uuid4())  # P4 占位：一条经历一个 key，归一规则待契约
            conn.execute(
                "INSERT INTO memory_experience"
                " (experience_id, workspace_id, session_id, subject_id, experience_type,"
                "  occurred_at, created_at, description, verification_status,"
                "  canonical_experience_id, independence_key, correction_status)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?, ?, 'normal')",
                (
                    experience_id,
                    workspace_id,
                    session_id,
                    subject_id,
                    experience_type,
                    occurred_at,
                    now,
                    description,
                    experience_id,
                    independence_key,
                ),
            )
            for event_id in normalized:
                conn.execute(
                    "INSERT INTO memory_experience_event_links"
                    " (link_id, experience_id, event_id, relation_type, created_at)"
                    " VALUES (?, ?, ?, 'source', ?)",
                    (str(uuid.uuid4()), experience_id, event_id, now),
                )

            self.audit.write(
                conn,
                MODULE_ID,
                "experience.create",
                "experience",
                experience_id,
                after={
                    "workspace_id": workspace_id,
                    "session_id": session_id,
                    "event_ids": normalized,
                    "verification_status": "pending",
                },
                request_id=request_id,
                logical_version=0,
            )

            self.outbox.append(
                conn,
                event_type="experience.created",
                producer_module=MODULE_ID,
                aggregate_type="experience",
                aggregate_id=experience_id,
                payload={
                    "source_experience_id": experience_id,
                    "canonical_experience_id": experience_id,
                    "event_revision": 0,
                    "occurred_at": occurred_at,
                    "created_at": now,
                    "workspace_id": workspace_id,
                    "session_id": session_id,
                },
                logical_version=0,
            )
            return {"created": True, "experience_id": experience_id}

        return self.dedup.run(MODULE_ID, request_id, "create_experience", payload, work)

    def get_experience(self, experience_id):
        row = self.db.conn.execute(
            "SELECT * FROM memory_experience WHERE experience_id = ?", (experience_id,)
        ).fetchone()
        if row is None:
            return None
        data = dict(row)
        links = self.db.conn.execute(
            "SELECT event_id, relation_type FROM memory_experience_event_links"
            " WHERE experience_id = ? ORDER BY created_at",
            (experience_id,),
        ).fetchall()
        data["event_ids"] = [link["event_id"] for link in links]
        return data

    def summary(self, workspace_id, session_id=None):
        """按会话/工作区的真实经历统计。"""
        clauses = ["workspace_id = ?"]
        params = [workspace_id]
        if session_id is not None:
            clauses.append("session_id = ?")
            params.append(session_id)
        where = " AND ".join(clauses)
        rows = self.db.conn.execute(
            "SELECT verification_status, COUNT(*) AS n FROM memory_experience"
            " WHERE " + where + " GROUP BY verification_status",
            params,
        ).fetchall()
        counts = {row["verification_status"]: row["n"] for row in rows}
        total = sum(counts.values())
        return {
            "source": "memory",
            "workspace_id": workspace_id,
            "session_id": session_id,
            "total": total,
            "pending": counts.get("pending", 0),
            "verified": counts.get("verified", 0),
            "uncertain": counts.get("uncertain", 0),
            "invalid": counts.get("invalid", 0),
            "generated_at": int(time.time() * 1000),
        }

    # ---------- 核验（P5：创建—核验闭环） ----------

    # 记忆 V1.1.2 合法转换（P5 只覆盖 pending/uncertain 的核验方向）。
    _VERIFY_TRANSITIONS = {
        "pending": {"verified", "uncertain", "invalid"},
        "uncertain": {"verified", "invalid"},
    }

    _VERIFY_EVENTS = {
        "verified": "experience.verified",
        "uncertain": "experience.uncertain",
        "invalid": "experience.invalidated",
    }

    def verify_experience(self, *, request_id, experience_id, target_status, reason=None, expected_status=None):
        """执行一次核验状态转换。核验结论由调用方给出。

        expected_status：评估开始时读取到的状态。若评估期间状态已变化，
        拒绝过期结论（防旧结果覆盖新状态）。
        """
        if target_status not in ("verified", "uncertain", "invalid"):
            raise ValueError("target_status must be verified/uncertain/invalid")

        def work(conn):
            row = conn.execute(
                "SELECT verification_status, canonical_experience_id, workspace_id, session_id,"
                " independence_key FROM memory_experience WHERE experience_id = ?",
                (experience_id,),
            ).fetchone()
            if row is None:
                raise KeyError("experience not found: %s" % experience_id)

            current = row["verification_status"]
            if current == target_status:
                return {"changed": False, "already": True, "status": current}

            if expected_status is not None and current != expected_status:
                raise ValueError(
                    "stale evaluation: expected_status=%s, current=%s" % (expected_status, current)
                )

            allowed = self._VERIFY_TRANSITIONS.get(current, set())
            if target_status not in allowed:
                raise ValueError("illegal transition: %s -> %s" % (current, target_status))

            now = int(time.time() * 1000)
            conn.execute(
                "UPDATE memory_experience SET verification_status = ? WHERE experience_id = ?",
                (target_status, experience_id),
            )
            self.audit.write(
                conn,
                MODULE_ID,
                "experience.verify",
                "experience",
                experience_id,
                before={"verification_status": current},
                after={"verification_status": target_status, "reason": reason},
                request_id=request_id,
                logical_version=1,
            )
            self.outbox.append(
                conn,
                event_type=self._VERIFY_EVENTS[target_status],
                producer_module=MODULE_ID,
                aggregate_type="experience",
                aggregate_id=experience_id,
                payload={
                    "source_experience_id": experience_id,
                    "canonical_experience_id": row["canonical_experience_id"],
                    "verification_status": target_status,
                    "previous_status": current,
                    "independence_key": row["independence_key"],
                    "workspace_id": row["workspace_id"],
                    "session_id": row["session_id"],
                    "occurred_at": now,
                },
                logical_version=1,
            )
            return {"changed": True, "status": target_status, "previous": current}

        payload = {"experience_id": experience_id, "target_status": target_status}
        return self.dedup.run(MODULE_ID, request_id, "verify_experience", payload, work)

    def check_experience(self, *, experience_id, source_event_ids, workspace_id):
        """只读核验（学习模块证据准入用）。不写库、不写审计、不改状态。"""
        empty = {
            "valid": False,
            "canonical_experience_id": None,
            "independence_key": None,
            "source_events": [],
            "verification_status": None,
        }
        row = self.db.conn.execute(
            "SELECT * FROM memory_experience WHERE experience_id = ?", (experience_id,)
        ).fetchone()
        if row is None:
            return dict(empty, reason="not_found")
        if str(row["workspace_id"]) != str(workspace_id):
            return dict(empty, reason="workspace_mismatch", verification_status=row["verification_status"])
        status = row["verification_status"]
        if status in ("invalid", "archived", "split"):
            return dict(
                empty,
                reason="status_%s" % status,
                canonical_experience_id=row["canonical_experience_id"],
                verification_status=status,
            )
        links = {
            r["event_id"]
            for r in self.db.conn.execute(
                "SELECT event_id FROM memory_experience_event_links"
                " WHERE experience_id = ? AND relation_type = 'source'",
                (experience_id,),
            )
        }
        wanted = set(str(e) for e in (source_event_ids or []))
        if wanted and not wanted.issubset(links):
            return dict(
                empty,
                reason="source_events_mismatch",
                canonical_experience_id=row["canonical_experience_id"],
                verification_status=status,
            )
        return {
            "valid": True,
            "reason": "ok",
            "canonical_experience_id": row["canonical_experience_id"],
            "independence_key": row["independence_key"],
            "source_events": sorted(links),
            "verification_status": status,
        }

    def list_pending(self, workspace_id=None, limit=50):
        """列出待核验（pending/uncertain）的经历，供每日核验使用。"""
        clauses = ["verification_status IN ('pending', 'uncertain')"]
        params = []
        if workspace_id is not None:
            clauses.append("workspace_id = ?")
            params.append(workspace_id)
        rows = self.db.conn.execute(
            "SELECT experience_id, workspace_id, session_id, experience_type, description,"
            " occurred_at, verification_status FROM memory_experience WHERE "
            + " AND ".join(clauses)
            + " ORDER BY occurred_at LIMIT ?",
            (*params, limit),
        ).fetchall()
        return [dict(row) for row in rows]

    # ---------- 内部 ----------

    def _find_by_event_set(self, conn, event_ids):
        """找到 source 事件集合与给定集合完全一致的 experience。"""
        placeholders = ",".join("?" for _ in event_ids)
        row = conn.execute(
            "SELECT experience_id FROM memory_experience_event_links"
            " WHERE relation_type = 'source' AND event_id IN (" + placeholders + ")"
            " GROUP BY experience_id"
            " HAVING COUNT(DISTINCT event_id) = ?"
            " AND COUNT(DISTINCT event_id) ="
            "   (SELECT COUNT(DISTINCT event_id) FROM memory_experience_event_links"
            "    WHERE experience_id = memory_experience_event_links.experience_id"
            "    AND relation_type = 'source')",
            (*event_ids, len(event_ids)),
        ).fetchone()
        return row["experience_id"] if row else None

    def _find_overlaps(self, conn, event_ids):
        """找与给定事件集合有部分重叠的 experience（不自动合并）。"""
        placeholders = ",".join("?" for _ in event_ids)
        rows = conn.execute(
            "SELECT experience_id, COUNT(*) AS overlap_count FROM memory_experience_event_links"
            " WHERE relation_type = 'source' AND event_id IN (" + placeholders + ")"
            " GROUP BY experience_id",
            (*event_ids,),
        ).fetchall()
        out = []
        for row in rows:
            total = conn.execute(
                "SELECT COUNT(*) AS n FROM memory_experience_event_links"
                " WHERE experience_id = ? AND relation_type = 'source'",
                (row["experience_id"],),
            ).fetchone()["n"]
            # 部分重叠：命中的事件不足以完整覆盖新集合，或已有经历还有额外事件。
            if row["overlap_count"] < len(event_ids) or total > row["overlap_count"]:
                out.append(
                    {
                        "experience_id": row["experience_id"],
                        "overlap_count": row["overlap_count"],
                        "existing_event_count": total,
                    }
                )
        return out

    # ---------- MEM-CHANGE-E4-01 V1.2：修正 / 合并 / 拆分 ----------

    REVISABLE_FIELDS = ("description",)
    MERGE_STATUSES = ("pending", "verified", "uncertain")
    TERMINAL_STATUSES = ("split", "invalid", "archived")

    def resolve_canonical_experience(self, experience_id, workspace_id):
        """解析最终 canonical：正常终点 / split_terminal / invalid_terminal / 异常链。"""
        chain = []
        visited = set()
        current = experience_id
        while True:
            if current in visited:
                return {"requested_experience_id": experience_id, "canonical_experience_id": None,
                        "resolution_status": "cycle_detected", "chain": chain, "error_reason": "cycle"}
            visited.add(current)
            chain.append(current)
            row = self.db.conn.execute(
                "SELECT experience_id, workspace_id, canonical_experience_id, verification_status"
                " FROM memory_experience WHERE experience_id = ?",
                (current,),
            ).fetchone()
            if row is None:
                return {"requested_experience_id": experience_id, "canonical_experience_id": None,
                        "resolution_status": "broken_chain", "chain": chain, "error_reason": "missing"}
            if row["workspace_id"] != workspace_id:
                return {"requested_experience_id": experience_id, "canonical_experience_id": None,
                        "resolution_status": "cross_workspace", "chain": chain, "error_reason": "workspace"}
            if row["canonical_experience_id"] == current:
                status = row["verification_status"]
                if status == "split":
                    return {"requested_experience_id": experience_id, "canonical_experience_id": current,
                            "resolution_status": "split_terminal", "chain": chain, "error_reason": None}
                if status in ("invalid", "archived"):
                    return {"requested_experience_id": experience_id, "canonical_experience_id": current,
                            "resolution_status": "invalid_terminal", "chain": chain, "error_reason": None}
                return {"requested_experience_id": experience_id, "canonical_experience_id": current,
                        "resolution_status": "ok", "chain": chain, "error_reason": None}
            if row["verification_status"] in self.TERMINAL_STATUSES:
                return {"requested_experience_id": experience_id, "canonical_experience_id": None,
                        "resolution_status": "corrupt_chain", "chain": chain,
                        "error_reason": "terminal_in_chain"}
            current = row["canonical_experience_id"]

    def _change_record(self, conn, *, operation_id, workspace_id, operation_type, request_id,
                       sources, targets, version_snapshot, mapping_version, actor):
        conn.execute(
            "INSERT INTO experience_changes(operation_id, workspace_id, operation_type, request_id,"
            " source_experience_ids, target_experience_ids, version_snapshot, mapping_version,"
            " status, actor, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'completed', ?, ?)",
            (operation_id, workspace_id, operation_type, request_id,
             json.dumps(sources, ensure_ascii=False), json.dumps(targets, ensure_ascii=False),
             json.dumps(version_snapshot, ensure_ascii=False), mapping_version, actor,
             int(time.time() * 1000)),
        )

    def revise_experience(self, *, request_id, experience_id, correction, expected_change_version, actor=None):
        """修正内容（白名单字段），不改变身份与 canonical。"""
        unknown = set(correction) - set(self.REVISABLE_FIELDS)
        if unknown:
            raise ValueError("fields not revisable: %s" % sorted(unknown))
        if not correction:
            raise ValueError("correction is empty")

        def work(conn):
            row = conn.execute(
                "SELECT * FROM memory_experience WHERE experience_id = ?", (experience_id,)
            ).fetchone()
            if row is None:
                raise KeyError("experience not found: %s" % experience_id)
            if row["canonical_experience_id"] != experience_id:
                raise ValueError("cannot revise merged alias")
            if int(row["change_version"]) != int(expected_change_version):
                raise ValueError("change_version conflict")
            sets = ", ".join("%s = ?" % field for field in correction)
            cursor = conn.execute(
                "UPDATE memory_experience SET %s, change_version = change_version + 1"
                " WHERE experience_id = ? AND change_version = ?" % sets,
                (*correction.values(), experience_id, int(expected_change_version)),
            )
            if cursor.rowcount != 1:
                raise ValueError("change_version conflict")
            operation_id = str(uuid.uuid4())
            now = int(time.time() * 1000)
            previous = int(row["change_version"])
            self._change_record(
                conn, operation_id=operation_id, workspace_id=row["workspace_id"],
                operation_type="revise", request_id=request_id, sources=[experience_id],
                targets=[experience_id],
                version_snapshot={experience_id: {"before": previous, "after": previous + 1}},
                mapping_version=previous + 1, actor=actor,
            )
            self.audit.write(
                conn, MODULE_ID, "experience.revise", "experience", experience_id,
                before={"description": row["description"]}, after=correction,
                request_id=request_id,
            )
            self.outbox.append(
                conn, event_type="experience.revised", producer_module=MODULE_ID,
                aggregate_type="experience", aggregate_id=experience_id,
                payload={
                    "operation_id": operation_id,
                    "experience_id": experience_id,
                    "workspace_id": row["workspace_id"],
                    "previous_change_version": previous,
                    "change_version": previous + 1,
                    "mapping_version": previous + 1,
                    "correction_fields": sorted(correction.keys()),
                    "occurred_at": now,
                },
            )
            return {"revised": True, "operation_id": operation_id, "change_version": previous + 1}

        return self.dedup.run(
            MODULE_ID, request_id, "revise_experience",
            {"experience_id": experience_id, "correction": correction,
             "expected_change_version": expected_change_version}, work,
        )

    def merge_experiences(self, *, request_id, source_experience_ids, canonical_experience_id,
                          expected_change_versions, actor=None):
        """合并：source 全部成为 canonical 的别名；多级链允许；写入时防环。"""
        sources = list(source_experience_ids)
        if len(set(sources)) != len(sources):
            raise ValueError("duplicate source_experience_ids")
        if canonical_experience_id in sources:
            raise ValueError("canonical_experience_id must not be in sources")

        def work(conn):
            rows = {}
            for experience_id in sources + [canonical_experience_id]:
                row = conn.execute(
                    "SELECT * FROM memory_experience WHERE experience_id = ?", (experience_id,)
                ).fetchone()
                if row is None:
                    raise KeyError("experience not found: %s" % experience_id)
                rows[experience_id] = row
            workspace_id = rows[canonical_experience_id]["workspace_id"]
            for experience_id in list(rows):
                if rows[experience_id]["workspace_id"] != workspace_id:
                    raise ValueError("cross-workspace merge rejected")
                if rows[experience_id]["canonical_experience_id"] != experience_id:
                    raise ValueError("merge source/target must be authoritative (canonical == self)")
                if rows[experience_id]["verification_status"] not in self.MERGE_STATUSES:
                    raise ValueError("merge status not allowed: %s" % rows[experience_id]["verification_status"])
                expected = int(expected_change_versions.get(experience_id, -1))
                if int(rows[experience_id]["change_version"]) != expected:
                    raise ValueError("change_version conflict: %s" % experience_id)
            # 防环：target 的链不得包含任一 source。
            target_chain = self.resolve_canonical_experience(canonical_experience_id, workspace_id)
            if target_chain["resolution_status"] != "ok":
                raise ValueError("target canonical chain invalid: %s" % target_chain["resolution_status"])
            if set(target_chain["chain"]) & set(sources):
                raise ValueError("merge would create a cycle")
            operation_id = str(uuid.uuid4())
            now = int(time.time() * 1000)
            versions = {experience_id: int(row["change_version"]) for experience_id, row in rows.items()}
            for source_id in sources:
                cursor = conn.execute(
                    "UPDATE memory_experience SET canonical_experience_id = ?,"
                    " change_version = change_version + 1 WHERE experience_id = ? AND change_version = ?",
                    (canonical_experience_id, source_id, versions[source_id]),
                )
                if cursor.rowcount != 1:
                    raise ValueError("change_version conflict: %s" % source_id)
                self.outbox.append(
                    conn, event_type="experience.merged", producer_module=MODULE_ID,
                    aggregate_type="experience", aggregate_id=source_id,
                    payload={
                        "operation_id": operation_id,
                        "source_experience_id": source_id,
                        "canonical_experience_id": canonical_experience_id,
                        "workspace_id": workspace_id,
                        "previous_change_version": versions[source_id],
                        "change_version": versions[source_id] + 1,
                        "mapping_version": versions[source_id] + 1,
                    },
                )
            cursor = conn.execute(
                "UPDATE memory_experience SET change_version = change_version + 1"
                " WHERE experience_id = ? AND change_version = ?",
                (canonical_experience_id, versions[canonical_experience_id]),
            )
            if cursor.rowcount != 1:
                raise ValueError("change_version conflict: canonical")
            self._change_record(
                conn, operation_id=operation_id, workspace_id=workspace_id,
                operation_type="merge", request_id=request_id, sources=sources,
                targets=[canonical_experience_id],
                version_snapshot={eid: {"before": versions[eid], "after": versions[eid] + 1} for eid in versions},
                mapping_version=max(versions.values()) + 1, actor=actor,
            )
            self.audit.write(
                conn, MODULE_ID, "experience.merge", "experience", canonical_experience_id,
                before={"canonical_experience_id": canonical_experience_id},
                after={"operation_id": operation_id, "sources": sources,
                       "canonical_experience_id": canonical_experience_id},
                request_id=request_id,
            )
            return {
                "merged": True,
                "operation_id": operation_id,
                "canonical_experience_id": canonical_experience_id,
                "sources": sources,
                "change_versions": {eid: v + 1 for eid, v in versions.items()},
            }

        return self.dedup.run(
            MODULE_ID, request_id, "merge_experiences",
            {"sources": sources, "canonical": canonical_experience_id,
             "expected": expected_change_versions}, work,
        )

    def split_experience(self, *, request_id, source_experience_id, new_experience_specs,
                         expected_change_version, actor=None):
        """拆分：事件级重分配；原记录 split 终态；不复制证据。"""
        if len(new_experience_specs) < 2:
            raise ValueError("split requires at least two new experiences")

        def work(conn):
            source = conn.execute(
                "SELECT * FROM memory_experience WHERE experience_id = ?", (source_experience_id,)
            ).fetchone()
            if source is None:
                raise KeyError("experience not found: %s" % source_experience_id)
            if source["canonical_experience_id"] != source_experience_id:
                raise ValueError("merged alias cannot be split")
            if source["verification_status"] not in ("verified", "uncertain"):
                raise ValueError("split status not allowed: %s" % source["verification_status"])
            if int(source["change_version"]) != int(expected_change_version):
                raise ValueError("change_version conflict")
            workspace_id = source["workspace_id"]

            source_events = {
                row["event_id"] for row in conn.execute(
                    "SELECT event_id FROM memory_experience_event_links"
                    " WHERE experience_id = ? AND relation_type = 'source'",
                    (source_experience_id,),
                )
            }
            assigned = []
            for spec in new_experience_specs:
                event_ids = list(spec.get("event_ids") or [])
                if not event_ids:
                    raise ValueError("each new experience requires event_ids")
                assigned.extend(event_ids)
            if len(assigned) != len(set(assigned)):
                raise ValueError("event_id assigned more than once")
            unknown = set(assigned) - source_events
            if unknown:
                raise ValueError("event_ids not owned by source: %s" % sorted(unknown))
            missing = source_events - set(assigned)
            if missing:
                raise ValueError("source events not fully assigned: %s" % sorted(missing))

            now = int(time.time() * 1000)
            operation_id = str(uuid.uuid4())
            mapping_version = int(source["change_version"]) + 1
            new_ids = []
            for spec in new_experience_specs:
                new_id = str(uuid.uuid4())
                new_ids.append(new_id)
                conn.execute(
                    "INSERT INTO memory_experience(experience_id, workspace_id, session_id, subject_id,"
                    " experience_type, occurred_at, created_at, description, verification_status,"
                    " canonical_experience_id, independence_key, correction_status, change_version)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'normal', 0)",
                    (
                        new_id, workspace_id, spec.get("session_id", source["session_id"]),
                        spec.get("subject_id", source["subject_id"]),
                        spec.get("experience_type", source["experience_type"]),
                        int(spec.get("occurred_at", source["occurred_at"])), now,
                        spec.get("description", source["description"]),
                        spec.get("verification_status", source["verification_status"]),
                        new_id, str(uuid.uuid4()),
                    ),
                )
                for event_id in spec["event_ids"]:
                    conn.execute(
                        "INSERT INTO memory_experience_event_links(link_id, experience_id, event_id,"
                        " relation_type, created_at) VALUES (?, ?, ?, 'source', ?)",
                        (str(uuid.uuid4()), new_id, event_id, now),
                    )
                    conn.execute(
                        "INSERT INTO experience_split_mappings(mapping_id, operation_id,"
                        " source_experience_id, target_experience_id, event_id, mapping_version, created_at)"
                        " VALUES (?, ?, ?, ?, ?, ?, ?)",
                        (str(uuid.uuid4()), operation_id, source_experience_id, new_id,
                         event_id, mapping_version, now),
                    )
            # 迁移记忆模块自身的 source 事件链接。
            conn.execute(
                "DELETE FROM memory_experience_event_links WHERE experience_id = ? AND relation_type = 'source'",
                (source_experience_id,),
            )
            cursor = conn.execute(
                "UPDATE memory_experience SET verification_status = 'split',"
                " change_version = change_version + 1 WHERE experience_id = ? AND change_version = ?",
                (source_experience_id, int(expected_change_version)),
            )
            if cursor.rowcount != 1:
                raise ValueError("change_version conflict")
            self._change_record(
                conn, operation_id=operation_id, workspace_id=workspace_id,
                operation_type="split", request_id=request_id, sources=[source_experience_id],
                targets=new_ids,
                version_snapshot={source_experience_id: {"before": int(expected_change_version),
                                                         "after": int(expected_change_version) + 1}},
                mapping_version=mapping_version, actor=actor,
            )
            self.audit.write(
                conn, MODULE_ID, "experience.split", "experience", source_experience_id,
                before={"verification_status": source["verification_status"]},
                after={"operation_id": operation_id, "targets": new_ids,
                       "mapping_version": mapping_version},
                request_id=request_id,
            )
            self.outbox.append(
                conn, event_type="experience.split", producer_module=MODULE_ID,
                aggregate_type="experience", aggregate_id=source_experience_id,
                payload={
                    "operation_id": operation_id,
                    "source_experience_id": source_experience_id,
                    "target_experience_ids": new_ids,
                    "workspace_id": workspace_id,
                    "previous_change_version": int(expected_change_version),
                    "change_version": int(expected_change_version) + 1,
                    "mapping_version": mapping_version,
                    "mapping_ref": operation_id,
                },
            )
            return {
                "split": True,
                "operation_id": operation_id,
                "source_experience_id": source_experience_id,
                "target_experience_ids": new_ids,
                "mapping_version": mapping_version,
            }

        return self.dedup.run(
            MODULE_ID, request_id, "split_experience",
            {"source": source_experience_id, "specs": new_experience_specs,
             "expected": expected_change_version}, work,
        )

    def get_experience_change(self, operation_id, workspace_id):
        row = self.db.conn.execute(
            "SELECT * FROM experience_changes WHERE operation_id = ? AND workspace_id = ?",
            (operation_id, workspace_id),
        ).fetchone()
        return dict(row) if row else None

    def get_experience_split_mapping(self, operation_id, workspace_id):
        rows = self.db.conn.execute(
            "SELECT m.* FROM experience_split_mappings m"
            " JOIN experience_changes c ON c.operation_id = m.operation_id"
            " WHERE m.operation_id = ? AND c.workspace_id = ? ORDER BY m.event_id",
            (operation_id, workspace_id),
        ).fetchall()
        return [dict(row) for row in rows]
