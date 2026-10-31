"""P14-E3：Outbox 对账（只读业务数据，只写对账账本）。

六类检查：完整性、状态一致性、Audit 一致性、重试/dead-letter、aggregate 顺序、订阅关系。
不修改 Outbox 事件、消费者记录、业务数据；不自动 replay；不补造记录。
"""

import json
import time
import uuid

MODULE_ID = "reconciliation"
MAX_ATTEMPTS = 3
SEVERITY_ORDER = ("P0", "P1", "P2", "INFO")


def _severity_rank(severity):
    try:
        return SEVERITY_ORDER.index(severity)
    except ValueError:
        return len(SEVERITY_ORDER)


class ReconciliationService:
    def __init__(self, db, dedup, audit):
        self.db = db
        self.dedup = dedup
        self.audit = audit

    # ---------- 主流程 ----------

    def reconcile(self, *, request_id, run_type="incremental", cursor=None, limit=500):
        if run_type not in ("incremental", "full"):
            raise ValueError("run_type must be incremental/full")

        def work(conn):
            started = int(time.time() * 1000)
            run_id = str(uuid.uuid4())
            conn.execute(
                "INSERT INTO outbox_reconciliation_runs(run_id, request_id, run_type, status, started_at)"
                " VALUES (?, ?, ?, 'running', ?)",
                (run_id, request_id, run_type, started),
            )
            issues = []
            cursor_before = cursor
            if run_type == "incremental" and cursor:
                rows = conn.execute(
                    "SELECT * FROM outbox_events WHERE (created_at > ?) OR (created_at = ? AND event_id > ?)"
                    " ORDER BY created_at, event_id LIMIT ?",
                    (int(cursor.get("created_at") or 0), int(cursor.get("created_at") or 0),
                     cursor.get("event_id") or "", limit),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM outbox_events ORDER BY created_at, event_id LIMIT ?", (limit,)
                ).fetchall()

            records = conn.execute("SELECT * FROM event_consumer_records").fetchall()
            subs = self._subscriptions(conn)
            audit_keys = self._consume_audits(conn)
            records_by_event = {}
            for record in records:
                records_by_event.setdefault(record["event_id"], []).append(record)

            event_ids = {row["event_id"] for row in rows}
            for event in rows:
                self._check_event(event, records_by_event.get(event["event_id"], []), subs, issues)
            # 孤儿记录（引用不存在事件）
            for record in records:
                if record["event_id"] not in event_ids:
                    exists = conn.execute(
                        "SELECT 1 FROM outbox_events WHERE event_id = ?", (record["event_id"],)
                    ).fetchone()
                    if exists is None:
                        issues.append(self._issue(
                            "orphan_consumer_record", "P0", event_id=record["event_id"],
                            consumer=record["consumer_module"],
                            description="消费者记录引用的 Outbox 事件不存在",
                            evidence={"consumer_status": record["consumer_status"]},
                        ))
            # Audit 一致性
            for record in records:
                if record["consumer_status"] != "consumed":
                    continue
                if (record["event_id"], record["consumer_module"]) not in audit_keys:
                    issues.append(self._issue(
                        "missing_consume_audit", "P1", event_id=record["event_id"],
                        consumer=record["consumer_module"],
                        description="消费成功记录缺少对应消费 Audit",
                    ))
            # aggregate 顺序
            self._check_aggregate_order(rows, records_by_event, issues)

            # 写异常（指纹去重）
            new_issues = 0
            recurring = 0
            for issue in issues:
                existing = conn.execute(
                    "SELECT * FROM outbox_reconciliation_issues WHERE fingerprint = ?",
                    (issue["fingerprint"],),
                ).fetchone()
                if existing is None:
                    conn.execute(
                        "INSERT INTO outbox_reconciliation_issues(issue_id, run_id, fingerprint, issue_type,"
                        " severity, event_id, consumer_module, aggregate_id, description, evidence,"
                        " detected_at, last_seen_at, issue_status)"
                        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'open')",
                        (str(uuid.uuid4()), run_id, issue["fingerprint"], issue["issue_type"],
                         issue["severity"], issue.get("event_id"), issue.get("consumer"),
                         issue.get("aggregate_id"), issue["description"],
                         json.dumps(issue.get("evidence") or {}, ensure_ascii=False, sort_keys=True),
                         started, started),
                    )
                    new_issues += 1
                else:
                    recurring += 1
                    conn.execute(
                        "UPDATE outbox_reconciliation_issues SET last_seen_at = ? WHERE issue_id = ?",
                        (started, existing["issue_id"]),
                    )
                    # resolved 后再次出现 → 重新打开；acknowledged/ignored 保持原状态。
                    if existing["issue_status"] == "resolved":
                        conn.execute(
                            "UPDATE outbox_reconciliation_issues SET issue_status = 'open', resolved_at = NULL"
                            " WHERE issue_id = ?",
                            (existing["issue_id"],),
                        )
            # 已解决：仅当本轮实际重新检查过该事件、且未再发现异常时才可 resolved。
            # 增量对账未扫描的旧事件保持原状态，由全量对账负责完整复核。
            current_fingerprints = {issue["fingerprint"] for issue in issues}
            checked_event_ids = {row["event_id"] for row in rows}
            resolved = 0
            for row in conn.execute(
                "SELECT * FROM outbox_reconciliation_issues WHERE issue_status IN ('open', 'acknowledged')"
            ).fetchall():
                if row["fingerprint"] in current_fingerprints:
                    continue
                if row["event_id"] not in checked_event_ids:
                    continue
                if self._still_valid(conn, row):
                    continue
                conn.execute(
                    "UPDATE outbox_reconciliation_issues SET issue_status = 'resolved', resolved_at = ?"
                    " WHERE issue_id = ?",
                    (started, row["issue_id"]),
                )
                resolved += 1

            counts = {sev: sum(1 for issue in issues if issue["severity"] == sev) for sev in SEVERITY_ORDER}
            issue_count = len(issues) - counts["INFO"]
            cursor_after = None
            if rows:
                last = rows[-1]
                cursor_after = {"created_at": last["created_at"], "event_id": last["event_id"]}
            conn.execute(
                "UPDATE outbox_reconciliation_runs SET status = 'completed', completed_at = ?,"
                " checked_events = ?, checked_consumers = ?, issue_count = ?, p0_count = ?, p1_count = ?,"
                " p2_count = ?, info_count = ?, new_issues = ?, recurring_issues = ?, resolved_issues = ?,"
                " cursor_before = ?, cursor_after = ? WHERE run_id = ?",
                (
                    int(time.time() * 1000), len(rows), len(records), issue_count, counts["P0"],
                    counts["P1"], counts["P2"], counts["INFO"], new_issues, recurring, resolved,
                    json.dumps(cursor_before) if cursor_before else None,
                    json.dumps(cursor_after) if cursor_after else None, run_id,
                ),
            )
            self.audit.write(
                conn, MODULE_ID, "outbox.reconcile", "reconciliation_run", run_id,
                after={
                    "run_type": run_type,
                    "checked_events": len(rows),
                    "checked_consumers": len(records),
                    "issue_count": issue_count,
                    "p0": counts["P0"],
                    "p1": counts["P1"],
                    "p2": counts["P2"],
                    "info": counts["INFO"],
                    "new_issues": new_issues,
                    "recurring_issues": recurring,
                    "resolved_issues": resolved,
                },
                request_id=request_id,
            )
            return {
                "run_id": run_id,
                "status": "completed",
                "run_type": run_type,
                "checked_events": len(rows),
                "checked_consumers": len(records),
                "issue_count": issue_count,
                "p0_count": counts["P0"],
                "p1_count": counts["P1"],
                "p2_count": counts["P2"],
                "info_count": counts["INFO"],
                "new_issues": new_issues,
                "recurring_issues": recurring,
                "resolved_issues": resolved,
                "cursor_before": cursor_before,
                "cursor_after": cursor_after,
                "started_at": started,
                "completed_at": int(time.time() * 1000),
            }

        return self.dedup.run(MODULE_ID, request_id, "reconcile_outbox",
                              {"run_type": run_type, "cursor": cursor}, work)

    # ---------- 检查 ----------

    def _subscriptions(self, conn):
        subs = {}
        for row in conn.execute("SELECT * FROM event_subscriptions WHERE enabled = 1"):
            subs.setdefault(row["event_type"], set()).add(row["consumer_module"])
        return subs

    def _consume_audits(self, conn):
        keys = set()
        for row in conn.execute(
            "SELECT target_id, after_snapshot FROM audit_records"
            " WHERE operation = 'outbox.consume' AND target_type = 'event'"
        ):
            try:
                after = json.loads(row["after_snapshot"]) if row["after_snapshot"] else {}
            except (TypeError, ValueError):
                after = {}
            consumer = after.get("consumer_module")
            if consumer:
                keys.add((row["target_id"], consumer))
        return keys

    def _issue(self, issue_type, severity, *, event_id=None, consumer=None, aggregate_id=None,
               description="", evidence=None):
        if issue_type in ("aggregate_order_violation",):
            fingerprint = "%s:%s:%s" % (issue_type, aggregate_id, event_id)
        elif issue_type == "no_subscriber":
            fingerprint = "%s:%s" % (issue_type, event_id)
        else:
            fingerprint = "%s:%s:%s" % (issue_type, event_id, consumer or "")
        return {
            "issue_type": issue_type,
            "severity": severity,
            "event_id": event_id,
            "consumer": consumer,
            "aggregate_id": aggregate_id,
            "description": description,
            "evidence": evidence or {},
            "fingerprint": fingerprint,
        }

    def _check_event(self, event, records, subs, issues):
        event_type = event["event_type"]
        consumers = subs.get(event_type)
        has_records = bool(records)
        dead = event["dead_letter_at"] is not None

        if not has_records:
            if event["status"] == "consumed":
                issues.append(self._issue(
                    "consumed_without_record", "P1", event_id=event["event_id"],
                    description="事件标记 consumed 但无任何消费成功记录",
                ))
                return
            if consumers:
                # 当前有订阅，但无法证明事件创建时存在订阅关系 → 历史不可确定，不判失败。
                issues.append(self._issue(
                    "subscription_unknown", "INFO", event_id=event["event_id"],
                    description="历史事件无消费者记录，且无法还原创建时订阅关系",
                    evidence={"event_type": event_type, "status": event["status"]},
                ))
            else:
                issues.append(self._issue(
                    "no_subscriber", "INFO", event_id=event["event_id"],
                    description="事件无订阅者，无需投递",
                    evidence={"event_type": event_type},
                ))
            return

        if event["status"] == "published":
            pending = [r for r in records if r["consumer_status"] != "consumed" and r["dead_letter_at"] is None]
            if pending:
                issues.append(self._issue(
                    "published_with_pending_record", "P1", event_id=event["event_id"],
                    consumer=pending[0]["consumer_module"],
                    description="事件已发布但仍有未完成消费者记录",
                    evidence={"pending": [r["consumer_module"] for r in pending]},
                ))
        if event["status"] == "consumed" and not any(r["consumer_status"] == "consumed" for r in records):
            issues.append(self._issue(
                "consumed_without_record", "P1", event_id=event["event_id"],
                description="事件标记 consumed 但无任何消费成功记录",
            ))
        if dead:
            active = [r for r in records if r["consumer_status"] != "consumed" and r["dead_letter_at"] is None]
            if active:
                issues.append(self._issue(
                    "dead_letter_with_active_record", "P1", event_id=event["event_id"],
                    consumer=active[0]["consumer_module"],
                    description="事件已进入死信，但仍有未死信的消费者记录",
                ))
        for record in records:
            if record["attempt_count"] >= MAX_ATTEMPTS and record["dead_letter_at"] is None:
                issues.append(self._issue(
                    "over_retry_not_dead", "P1", event_id=event["event_id"],
                    consumer=record["consumer_module"],
                    description="重试次数已达上限但未进入死信",
                    evidence={"attempt_count": record["attempt_count"]},
                ))
            if record["dead_letter_at"] is not None and record["attempt_count"] < MAX_ATTEMPTS:
                issues.append(self._issue(
                    "early_dead_letter", "P2", event_id=event["event_id"],
                    consumer=record["consumer_module"],
                    description="未达重试上限却进入死信",
                    evidence={"attempt_count": record["attempt_count"]},
                ))

    def _check_aggregate_order(self, rows, records_by_event, issues):
        by_aggregate = {}
        for row in rows:
            key = (row["aggregate_type"], row["aggregate_id"])
            by_aggregate.setdefault(key, []).append(row)
        for key, events in by_aggregate.items():
            ordered = sorted(events, key=lambda e: (e["created_at"], e["event_id"]))
            for index, later in enumerate(ordered):
                later_records = records_by_event.get(later["event_id"], [])
                later_consumed = any(r["consumer_status"] == "consumed" for r in later_records)
                if not later_consumed:
                    continue
                for earlier in ordered[:index]:
                    earlier_records = records_by_event.get(earlier["event_id"], [])
                    if not earlier_records:
                        continue
                    earlier_done = any(r["consumer_status"] == "consumed" for r in earlier_records)
                    earlier_dead = earlier["dead_letter_at"] is not None
                    if earlier_done or earlier_dead:
                        continue
                    issues.append(self._issue(
                        "aggregate_order_violation", "P1", event_id=later["event_id"],
                        aggregate_id=later["aggregate_id"],
                        description="后续事件已完成，但同 aggregate 前序事件未完成",
                        evidence={"earlier_event_id": earlier["event_id"]},
                    ))
                    break

    def _still_valid(self, conn, row):
        """异常条件是否仍然成立：事件不存在时保持；存在时由本轮是否再次发现决定。"""
        event = conn.execute(
            "SELECT * FROM outbox_events WHERE event_id = ?", (row["event_id"],)
        ).fetchone()
        return event is None
