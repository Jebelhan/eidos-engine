#!/usr/bin/env python3
"""DSH 状态引擎。

P3：基础设施层 —— SQLite、Migration、统一 request_dedup、统一 Audit、统一 Outbox。
不含六大模块业务逻辑。

stdout 只输出 NDJSON 协议消息；诊断信息一律写 stderr。
"""

import json
import os
import sqlite3
import sys
import time
import uuid
from datetime import datetime

try:
    from dsh_engine.audit import AuditLog
    from dsh_engine.behavior import BehaviorService
    from dsh_engine.daily import DailyCycleService
    from dsh_engine.experience_consumer import ExperienceChangeConsumer
    from dsh_engine.db import Database
    from dsh_engine.dispatcher import Dispatcher
    from dsh_engine.emotion import EmotionService
    from dsh_engine.emotion_queue import EmotionQueue
    from dsh_engine.idempotency import IdempotencyConflict, RequestDedup
    from dsh_engine.learning import LearningService
    from dsh_engine.learning_queue import LearningQueue
    from dsh_engine.memory import MemoryService
    from dsh_engine.outbox import Outbox
    from dsh_engine.personality import PersonalityService
    from dsh_engine.personality_decay import PressureDecayService
    from dsh_engine.reconciliation import ReconciliationService
    from dsh_engine.relationship import RelationshipService
    from dsh_engine.relationship_queue import RelationshipQueue
except ImportError:  # 直接以脚本方式运行且未安装包时，把自身目录加入 sys.path
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from dsh_engine.audit import AuditLog
    from dsh_engine.behavior import BehaviorService
    from dsh_engine.daily import DailyCycleService
    from dsh_engine.experience_consumer import ExperienceChangeConsumer
    from dsh_engine.db import Database
    from dsh_engine.dispatcher import Dispatcher
    from dsh_engine.emotion import EmotionService
    from dsh_engine.emotion_queue import EmotionQueue
    from dsh_engine.idempotency import IdempotencyConflict, RequestDedup
    from dsh_engine.learning import LearningService
    from dsh_engine.learning_queue import LearningQueue
    from dsh_engine.memory import MemoryService
    from dsh_engine.outbox import Outbox
    from dsh_engine.personality import PersonalityService
    from dsh_engine.personality_decay import PressureDecayService
    from dsh_engine.reconciliation import ReconciliationService
    from dsh_engine.relationship import RelationshipService
    from dsh_engine.relationship_queue import RelationshipQueue

PROTOCOL_VERSION = "1.0"
ENGINE_NAME = "dsh-engine"

# Windows 下默认 stdout 编码可能不是 UTF-8，协议行必须固定 UTF-8。
try:
    sys.stdin.reconfigure(encoding="utf-8")
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except AttributeError:  # pragma: no cover - 旧 Python 无 reconfigure
    pass


class Engine:
    """进程级状态：数据库与基础设施组件。"""

    def __init__(self):
        self.data_dir = os.environ.get("DSH_ENGINE_DATA_DIR") or os.path.join(
            os.path.expanduser("~"), ".dsh", "dsh-engine"
        )
        self.db = None
        self.dedup = None
        self.audit = None
        self.outbox = None
        self.dispatcher = None
        self.db_error = None
        self.migrations_applied = []

    @property
    def db_path(self):
        return os.path.join(self.data_dir, "dsh_engine.sqlite")

    def init(self):
        try:
            os.makedirs(self.data_dir, exist_ok=True)
            self.db = Database(self.db_path)
            self.migrations_applied = self.db.migrate()
            self.dedup = RequestDedup(self.db)
            self.audit = AuditLog(self.db)
            self.outbox = Outbox(self.db)
            self.memory = MemoryService(self.db, self.dedup, self.audit, self.outbox)
            self.dispatcher = Dispatcher(self.db, self.audit, self.outbox)
            self.emotion = EmotionService(self.db, self.dedup, self.audit)
            self.emotion_queue = EmotionQueue(self.db, self.audit)
            self.relationship = RelationshipService(self.db, self.dedup, self.audit)
            self.relationship_queue = RelationshipQueue(self.db, self.audit)
            self.learning = LearningService(self.db, self.dedup, self.audit, self.memory, self.outbox)
            self.learning_queue = LearningQueue(self.db, self.audit)
            self.personality = PersonalityService(self.db, self.dedup, self.audit, self.memory)
            self.behavior = BehaviorService(self.db, self.dedup, self.audit, self.personality, self.emotion)
            self.daily = DailyCycleService(self.db, self.audit)
            self.decay = PressureDecayService(self.db, self.dedup, self.audit)
            self.reconciliation = ReconciliationService(self.db, self.dedup, self.audit)
            self.experience_consumer = ExperienceChangeConsumer(self.db, self.audit)
            self.personality.ensure_traits()
            self._register_emotion_consumer()
            self._register_relationship_consumer()
            self._register_learning_consumer()
            self._register_personality_consumer()
            self._register_experience_merge_consumers()
            self._register_experience_split_consumers()
            self._register_experience_revise_consumers()
            self._register_test_consumer()
            self.db_error = None
        except Exception as error:  # noqa: BLE001 - 数据库不可用时引擎降级但不崩
            self.db_error = "%s: %s" % (type(error).__name__, error)

    def _register_emotion_consumer(self):
        """P9：情绪证据消费者。只入队（awaiting/pending_eval），不提交情绪事件。"""

        def emotion_handler(conn, payload):
            verification_status = payload.get("verification_status")
            source_status = "verified" if verification_status == "verified" else "uncertain"
            return self.emotion_queue.upsert_from_event(
                conn,
                workspace_id=payload.get("workspace_id") or "unknown",
                canonical_experience_id=payload.get("canonical_experience_id") or payload.get("source_experience_id"),
                source_experience_id=payload.get("source_experience_id"),
                source_status=source_status,
            )

        self.dispatcher.register_handler("emotion-evidence", emotion_handler)
        for event_type in ("experience.verified", "experience.uncertain"):
            self.outbox.register_subscription(event_type, "emotion-evidence")

    def _register_relationship_consumer(self):
        """P10：关系证据消费者 + 生命周期消费者（merge/split/invalidated）。"""

        def evidence_handler(conn, payload):
            verification_status = payload.get("verification_status")
            source_status = "verified" if verification_status == "verified" else "uncertain"
            return self.relationship_queue.upsert_from_event(
                conn,
                workspace_id=payload.get("workspace_id") or "unknown",
                canonical_experience_id=payload.get("canonical_experience_id") or payload.get("source_experience_id"),
                source_experience_id=payload.get("source_experience_id"),
                source_status=source_status,
            )

        def lifecycle_handler(conn, payload):
            event_type = payload.get("__event_type") or ""
            if event_type == "experience.invalidated":
                return self.relationship.invalidate_for_experience(
                    conn,
                    canonical_experience_id=payload.get("source_experience_id")
                    or payload.get("canonical_experience_id"),
                )
            if event_type == "experience.merged":
                merged_from = payload.get("merged_from") or []
                if merged_from:
                    return self.relationship.relocate_for_merge(
                        conn,
                        canonical_experience_id=payload.get("canonical_experience_id") or payload.get("source_experience_id"),
                        merged_from=merged_from,
                    )
            if event_type == "experience.split":
                new_experiences = payload.get("new_experiences") or []
                if new_experiences:
                    return self.relationship.relocate_for_split(
                        conn,
                        split_experience_id=payload.get("source_experience_id"),
                        new_experiences=new_experiences,
                    )
            return {"skipped": True}

        self.dispatcher.register_handler("relationship-evidence", evidence_handler)
        self.dispatcher.register_handler("relationship-lifecycle", lifecycle_handler)
        for event_type in ("experience.verified", "experience.uncertain"):
            self.outbox.register_subscription(event_type, "relationship-evidence")
        for event_type in ("experience.merged", "experience.split", "experience.invalidated"):
            self.outbox.register_subscription(event_type, "relationship-lifecycle")

    def _register_learning_consumer(self):
        """P11：学习证据消费者 + invalidated 撤销。"""

        def evidence_handler(conn, payload):
            verification_status = payload.get("verification_status")
            source_status = "verified" if verification_status == "verified" else "uncertain"
            return self.learning_queue.upsert_from_event(
                conn,
                workspace_id=payload.get("workspace_id") or "unknown",
                canonical_experience_id=payload.get("canonical_experience_id") or payload.get("source_experience_id"),
                source_experience_id=payload.get("source_experience_id"),
                source_status=source_status,
            )

        def lifecycle_handler(conn, payload):
            if payload.get("__event_type") == "experience.invalidated":
                return self.learning.revoke_evidence_for_experience(
                    conn,
                    experience_id=payload.get("source_experience_id")
                    or payload.get("canonical_experience_id"),
                )
            return {"skipped": True}

        self.dispatcher.register_handler("learning-evidence", evidence_handler)
        self.dispatcher.register_handler("learning-lifecycle", lifecycle_handler)
        for event_type in ("experience.verified", "experience.uncertain"):
            self.outbox.register_subscription(event_type, "learning-evidence")
        self.outbox.register_subscription("experience.invalidated", "learning-lifecycle")

    def _register_personality_consumer(self):
        """P12：人格领取学习建议（Suggestions 唯一入口）；确认证据由协调器路由（P14）。"""

        def suggestion_handler(conn, payload):
            return self.personality.handle_learning_suggestion(
                request_id="personality-consume-%s" % payload.get("suggestion_id"),
                suggestion={"suggestion_id": payload.get("suggestion_id"), "confidence": 0.5},
                conclusion_id=payload.get("conclusion_id"),
            )

        self.dispatcher.register_handler("personality-suggestion", suggestion_handler)
        self.outbox.register_subscription("learning.suggestion.created", "personality-suggestion")

    def _register_experience_merge_consumers(self):
        """E4-D：experience.merged → 四模块重关联（操作级覆盖全部 source）。"""
        consumer = self.experience_consumer

        def canonical_of(conn, source, workspace_id):
            resolved = self.memory.resolve_canonical_experience(source, workspace_id)
            if resolved["resolution_status"] != "ok":
                raise RuntimeError("canonical resolution failed: %s (%s)"
                                   % (resolved["resolution_status"], source))
            return resolved["canonical_experience_id"]

        def handler_for(module, apply_fn):
            def handler(conn, payload):
                operation_id = payload.get("operation_id")
                workspace_id = payload.get("workspace_id") or "unknown"
                mapping_version = int(payload.get("mapping_version") or 0)
                begin = consumer.begin(
                    operation_id=operation_id, consumer_module=module,
                    operation_type="merge", mapping_version=mapping_version, conn=conn,
                )
                if begin.get("skip"):
                    return {"skipped": True, "reason": begin.get("reason")}
                if begin.get("conflict"):
                    raise RuntimeError("mapping_version conflict")
                change = self.memory.get_experience_change(operation_id, workspace_id)
                if change is None:
                    raise RuntimeError("experience change not found: %s" % operation_id)
                sources = json.loads(change["source_experience_ids"] or "[]")
                results = []
                for source in sources:
                    canonical = canonical_of(conn, source, workspace_id)
                    results.append(apply_fn(conn, source, workspace_id, mapping_version, canonical))
                consumer.complete(
                    operation_id=operation_id, consumer_module=module,
                    result={"sources": results}, conn=conn,
                )
                return {"module": module, "sources": len(sources), "results": results}
            return handler

        def apply_emotion(conn, source, workspace_id, mapping_version, canonical):
            return self.emotion.relocate_emotion_events_for_experience_change(
                request_id=None, operation_id=None, operation_type="merge",
                workspace_id=workspace_id, source_experience_id=source,
                mapping_version=mapping_version, canonical_experience_id=canonical, conn=conn,
            )

        def apply_relationship(conn, source, workspace_id, mapping_version, canonical):
            return self.relationship.relocate_for_merge(
                conn, canonical_experience_id=canonical, merged_from=[source],
            )

        def apply_learning(conn, source, workspace_id, mapping_version, canonical):
            return self.learning.relocate_conclusion_evidence_for_merge(
                request_id=None, operation_id=None, workspace_id=workspace_id,
                source_experience_id=source, canonical_experience_id=canonical,
                mapping_version=mapping_version, conn=conn,
            )

        def apply_personality(conn, source, workspace_id, mapping_version, canonical):
            return self.personality.relocate_personality_confirmations_for_merge(
                request_id=None, operation_id=None, workspace_id=workspace_id,
                source_experience_id=source, canonical_experience_id=canonical,
                mapping_version=mapping_version, conn=conn,
            )

        for module, fn in (
            ("emotion", apply_emotion),
            ("relationship", apply_relationship),
            ("learning", apply_learning),
            ("personality", apply_personality),
        ):
            self.dispatcher.register_handler("experience-merge-%s" % module, handler_for(module, fn))
            self.outbox.register_subscription("experience.merged", "experience-merge-%s" % module)

    def _register_experience_split_consumers(self):
        """E4-E：experience.split → 情绪/关系按事件映射迁移；学习/人格进待复核。"""
        consumer = self.experience_consumer

        def event_targets(operation_id, workspace_id):
            rows = self.memory.get_experience_split_mapping(operation_id, workspace_id)
            targets = {}
            for row in rows:
                targets[row["event_id"]] = row["target_experience_id"]
            return targets

        def handler_for(module, apply_fn):
            def handler(conn, payload):
                operation_id = payload.get("operation_id")
                workspace_id = payload.get("workspace_id") or "unknown"
                mapping_version = int(payload.get("mapping_version") or 0)
                begin = consumer.begin(
                    operation_id=operation_id, consumer_module=module,
                    operation_type="split", mapping_version=mapping_version, conn=conn,
                )
                if begin.get("skip"):
                    return {"skipped": True, "reason": begin.get("reason")}
                if begin.get("conflict"):
                    raise RuntimeError("mapping_version conflict")
                source = payload.get("source_experience_id")
                result = apply_fn(conn, source, workspace_id, operation_id, payload)
                consumer.complete(
                    operation_id=operation_id, consumer_module=module, result=result, conn=conn,
                )
                return {"module": module, "result": result}
            return handler

        def apply_emotion(conn, source, workspace_id, operation_id, payload):
            targets = event_targets(operation_id, workspace_id)
            return self.emotion.relocate_emotion_events_for_experience_change(
                request_id=None, operation_id=operation_id, operation_type="split",
                workspace_id=workspace_id, source_experience_id=source,
                mapping_version=int(payload.get("mapping_version") or 0),
                event_targets=targets, conn=conn,
            )

        def apply_relationship(conn, source, workspace_id, operation_id, payload):
            rows = self.memory.get_experience_split_mapping(operation_id, workspace_id)
            grouped = {}
            for row in rows:
                grouped.setdefault(row["target_experience_id"], []).append(row["event_id"])
            new_experiences = [
                {"experience_id": experience_id, "event_ids": event_ids}
                for experience_id, event_ids in grouped.items()
            ]
            return self.relationship.relocate_for_split(
                conn, split_experience_id=source, new_experiences=new_experiences,
            )

        def apply_learning(conn, source, workspace_id, operation_id, payload):
            rows = conn.execute(
                "SELECT evidence_id FROM conclusion_evidence"
                " WHERE experience_id = ? AND revoked_at IS NULL",
                (source,),
            ).fetchall()
            queued = []
            for row in rows:
                result = consumer.enqueue_review(
                    workspace_id=workspace_id, operation_id=operation_id,
                    consumer_module="learning", evidence_ref=row["evidence_id"],
                    reason="split_without_event_trace", source_experience_id=source, conn=conn,
                )
                queued.append(result["review_id"])
            return {"queued": queued}

        def apply_personality(conn, source, workspace_id, operation_id, payload):
            rows = conn.execute(
                "SELECT c.confirmation_id FROM learning_influence_confirmations c"
                " JOIN learning_influence_requests r ON r.influence_request_id = c.influence_request_id"
                " WHERE c.experience_id = ? AND c.revoked_at IS NULL AND r.status != 'active'",
                (source,),
            ).fetchall()
            queued = []
            for row in rows:
                result = consumer.enqueue_review(
                    workspace_id=workspace_id, operation_id=operation_id,
                    consumer_module="personality", evidence_ref=row["confirmation_id"],
                    reason="split_without_event_trace", source_experience_id=source, conn=conn,
                )
                queued.append(result["review_id"])
            return {"queued": queued}

        for module, fn in (
            ("emotion", apply_emotion),
            ("relationship", apply_relationship),
            ("learning", apply_learning),
            ("personality", apply_personality),
        ):
            self.dispatcher.register_handler("experience-split-%s" % module, handler_for(module, fn))
            self.outbox.register_subscription("experience.split", "experience-split-%s" % module)

    def _register_experience_revise_consumers(self):
        """E4-F：experience.revised 最小消费（情绪/关系不改；学习/人格进待复核）。"""
        consumer = self.experience_consumer

        def handler_for(module, apply_fn):
            def handler(conn, payload):
                operation_id = payload.get("operation_id")
                workspace_id = payload.get("workspace_id") or "unknown"
                mapping_version = int(payload.get("mapping_version") or 0)
                begin = consumer.begin(
                    operation_id=operation_id, consumer_module=module,
                    operation_type="revise", mapping_version=mapping_version, conn=conn,
                )
                if begin.get("skip"):
                    return {"skipped": True, "reason": begin.get("reason")}
                if begin.get("conflict"):
                    raise RuntimeError("mapping_version conflict")
                result = apply_fn(conn, payload, workspace_id, operation_id)
                consumer.complete(
                    operation_id=operation_id, consumer_module=module, result=result, conn=conn,
                )
                return {"module": module, "result": result}
            return handler

        def apply_noop(conn, payload, workspace_id, operation_id):
            return {"action": "noop"}

        def apply_learning(conn, payload, workspace_id, operation_id):
            experience_id = payload.get("experience_id")
            rows = conn.execute(
                "SELECT evidence_id FROM conclusion_evidence"
                " WHERE experience_id = ? AND revoked_at IS NULL",
                (experience_id,),
            ).fetchall()
            queued = []
            for row in rows:
                review = consumer.enqueue_review(
                    workspace_id=workspace_id, operation_id=operation_id,
                    consumer_module="learning", evidence_ref=row["evidence_id"],
                    reason="revised_evidence_uncertain",
                    source_experience_id=experience_id, conn=conn,
                )
                queued.append(review["review_id"])
            return {"queued": queued}

        def apply_personality(conn, payload, workspace_id, operation_id):
            experience_id = payload.get("experience_id")
            rows = conn.execute(
                "SELECT c.confirmation_id FROM learning_influence_confirmations c"
                " JOIN learning_influence_requests r ON r.influence_request_id = c.influence_request_id"
                " WHERE c.experience_id = ? AND c.revoked_at IS NULL AND r.status != 'active'",
                (experience_id,),
            ).fetchall()
            queued = []
            for row in rows:
                review = consumer.enqueue_review(
                    workspace_id=workspace_id, operation_id=operation_id,
                    consumer_module="personality", evidence_ref=row["confirmation_id"],
                    reason="revised_evidence_uncertain",
                    source_experience_id=experience_id, conn=conn,
                )
                queued.append(review["review_id"])
            return {"queued": queued}

        for module, fn in (
            ("emotion", apply_noop),
            ("relationship", apply_noop),
            ("learning", apply_learning),
            ("personality", apply_personality),
        ):
            self.dispatcher.register_handler("experience-revise-%s" % module, handler_for(module, fn))
            self.outbox.register_subscription("experience.revised", "experience-revise-%s" % module)

    def _register_test_consumer(self):
        """P8-C 验证用测试消费者：仅当环境变量开启时注册。"""
        if os.environ.get("DSH_ENGINE_TEST_CONSUMER") != "1":
            return

        def echo_handler(conn, payload):
            return {"echo": True, "keys": sorted(payload.keys()) if isinstance(payload, dict) else []}

        self.dispatcher.register_handler("echo-consumer", echo_handler)
        for event_type in ("experience.created", "experience.verified", "experience.uncertain", "experience.invalidated"):
            self.outbox.register_subscription(event_type, "echo-consumer")

    def close(self):
        if self.db is not None:
            self.db.close()
            self.db = None

    def status(self):
        base = {
            "engine": ENGINE_NAME,
            "mode": "infrastructure",
            "protocol": PROTOCOL_VERSION,
            "python": "%d.%d.%d" % sys.version_info[:3],
            "sqlite": sqlite3.sqlite_version,
            "pid": os.getpid(),
            "data_dir": self.data_dir,
            "schema_version": self.db.schema_version() if self.db else 0,
            "db_error": self.db_error,
        }
        return base


ENGINE = Engine()


def send(message):
    sys.stdout.write(json.dumps(message, ensure_ascii=False) + "\n")
    sys.stdout.flush()


def log(text):
    sys.stderr.write("%s\n" % text)
    sys.stderr.flush()


def ok(request_id, data):
    return {"request_id": request_id, "status": "success", "data": data}


def fail(request_id, code, message):
    return {
        "request_id": request_id,
        "status": "error",
        "error": {"code": code, "message": message},
    }


def _require_db():
    if ENGINE.db is None:
        raise RuntimeError("database unavailable: %s" % (ENGINE.db_error or "not initialized"))
    return ENGINE.db


def op_db_status(request_id):
    db = _require_db()
    return ok(
        request_id,
        {
            "path": db.path,
            "schema_version": db.schema_version(),
            "tables": db.tables(),
            "migrations": db.migration_records(),
            "integrity": db.integrity_check(),
        },
    )


def op_self_test(request_id):
    """P3 验收自检：幂等 / Audit / Outbox。返回逐项结果。"""
    db = _require_db()
    dedup, audit, outbox = ENGINE.dedup, ENGINE.audit, ENGINE.outbox
    checks = []
    tag = uuid.uuid4().hex[:8]

    # 1. 幂等：同参重放返回原结果，业务只执行一次。
    calls = {"n": 0}

    def work(conn):
        calls["n"] += 1
        return {"value": 42, "call": calls["n"]}

    rid = "selftest-dedup-%s" % tag
    first = dedup.run("selftest", rid, "op", {"a": 1}, work)
    second = dedup.run("selftest", rid, "op", {"a": 1}, work)
    checks.append(
        {"name": "dedup_replay_returns_original", "ok": first == second and calls["n"] == 1, "result": second}
    )

    # 2. 幂等冲突：同 request_id 异参。
    conflicted = False
    try:
        dedup.run("selftest", rid, "op", {"a": 2}, work)
    except IdempotencyConflict:
        conflicted = True
    checks.append({"name": "dedup_conflict_on_payload_change", "ok": conflicted})

    # 3. 模块边界：不同模块同 request_id 不冲突。
    other = dedup.run("selftest-other", rid, "op", {"a": 1}, work)
    checks.append({"name": "dedup_module_boundary", "ok": other == {"value": 42, "call": 2}})

    # 4. Audit：写入成功，UPDATE/DELETE 被拒绝。
    with db.transaction() as conn:
        audit_id = audit.write(conn, "selftest", "selftest.write", "record", "r-%s" % tag, after={"v": 1})
    guard = False
    try:
        with db.transaction() as conn:
            conn.execute("UPDATE audit_records SET operation = 'tampered' WHERE audit_id = ?", (audit_id,))
    except sqlite3.Error:
        guard = True
    checks.append({"name": "audit_append_only", "ok": guard})

    # 5. Outbox：pending → published → 多消费者 consumed。
    seen = []
    with db.transaction() as conn:
        event_id = outbox.append(
            conn,
            event_type="selftest.event",
            producer_module="selftest",
            aggregate_type="test",
            aggregate_id="agg-%s" % tag,
            payload={"v": 1},
            expected_consumers=["consumer-a", "consumer-b"],
        )
    before_status = outbox.get(event_id)["status"]
    outbox.publish(event_id)
    published_status = outbox.get(event_id)["status"]
    outbox.consume(event_id, "consumer-a", lambda c, p: seen.append(("a", p)))
    after_a = outbox.get(event_id)["status"]
    outbox.consume(event_id, "consumer-b", lambda c, p: seen.append(("b", p)))
    after_b = outbox.get(event_id)["status"]
    checks.append(
        {
            "name": "outbox_lifecycle",
            "ok": before_status == "pending"
            and published_status == "published"
            and after_a == "published"
            and after_b == "consumed"
            and len(seen) == 2,
            "detail": {"before": before_status, "published": published_status, "after_a": after_a, "after_b": after_b},
        }
    )

    # 6. 消费幂等：重复消费不重复执行。
    count_before = len(seen)
    outbox.consume(event_id, "consumer-a", lambda c, p: seen.append(("a2", p)))
    checks.append({"name": "outbox_consume_idempotent", "ok": len(seen) == count_before})

    # 7. 发布失败可重试。
    with db.transaction() as conn:
        failed_event = outbox.append(
            conn,
            event_type="selftest.fail",
            producer_module="selftest",
            aggregate_type="test",
            aggregate_id="agg2-%s" % tag,
            payload={"v": 2},
        )
    outbox.fail_publish(failed_event, "simulated publish failure")
    failed_status = outbox.get(failed_event)["status"]
    outbox.publish(failed_event)
    retried_status = outbox.get(failed_event)["status"]
    checks.append(
        {
            "name": "outbox_retry_from_failed",
            "ok": failed_status == "failed" and retried_status == "published",
            "detail": {"failed": failed_status, "retried": retried_status},
        }
    )

    return ok(
        request_id,
        {
            "ok": all(check["ok"] for check in checks),
            "checks": checks,
            "schema_version": db.schema_version(),
        },
    )


def op_get_summary(request_id, payload):
    # P2：确定性测试摘要。P3 起附带数据库与幂等可用性信息，仍不接业务模块。
    seq = int(payload.get("seq") or 0)
    session_id = payload.get("session_id") or "unknown"
    workspace_id = payload.get("workspace_id") or "unknown"
    now = datetime.now().astimezone()
    db_ok = ENGINE.db is not None and ENGINE.db_error is None
    text = "[DSH状态] P2测试摘要 #{seq} | session={s} | workspace={w} | at={t} | 引擎在线 | 数据库={db}".format(
        seq=seq,
        s=session_id,
        w=workspace_id,
        t=now.strftime("%Y-%m-%d %H:%M:%S"),
        db="ready" if db_ok else "degraded",
    )
    return ok(
        request_id,
        {
            "marker": "P2_TEST_SUMMARY",
            "seq": seq,
            "generated_at": int(now.timestamp() * 1000),
            "session_id": session_id,
            "workspace_id": workspace_id,
            "db_ok": db_ok,
            "text": text,
        },
    )


def op_memory_create_experience(request_id, payload):
    if ENGINE.memory is None:
        raise RuntimeError("memory module unavailable")
    result = ENGINE.memory.create_experience(
        request_id=request_id or str(uuid.uuid4()),
        workspace_id=payload.get("workspace_id") or "unknown",
        session_id=payload.get("session_id"),
        event_ids=payload.get("event_ids") or [],
        experience_type=payload.get("experience_type") or "interaction",
        occurred_at=int(payload.get("occurred_at") or int(time.time() * 1000)),
        subject_id=payload.get("subject_id"),
        description=payload.get("description") or "",
    )
    return ok(request_id, result)


def op_memory_get_experience(request_id, payload):
    if ENGINE.memory is None:
        raise RuntimeError("memory module unavailable")
    data = ENGINE.memory.get_experience(payload.get("experience_id"))
    if data is None:
        return fail(request_id, "NOT_FOUND", "experience not found: %r" % payload.get("experience_id"))
    return ok(request_id, data)


def op_memory_summary(request_id, payload):
    if ENGINE.memory is None:
        raise RuntimeError("memory module unavailable")
    data = ENGINE.memory.summary(payload.get("workspace_id") or "unknown", payload.get("session_id"))
    return ok(request_id, data)


def op_memory_verify_experience(request_id, payload):
    if ENGINE.memory is None:
        raise RuntimeError("memory module unavailable")
    result = ENGINE.memory.verify_experience(
        request_id=request_id or str(uuid.uuid4()),
        experience_id=payload.get("experience_id"),
        target_status=payload.get("target_status"),
        reason=payload.get("reason"),
        expected_status=payload.get("expected_status"),
    )
    return ok(request_id, result)


def op_memory_list_pending(request_id, payload):
    if ENGINE.memory is None:
        raise RuntimeError("memory module unavailable")
    rows = ENGINE.memory.list_pending(payload.get("workspace_id"), int(payload.get("limit") or 50))
    return ok(request_id, {"experiences": rows, "count": len(rows)})


def op_outbox_dispatch(request_id, payload):
    if ENGINE.dispatcher is None:
        raise RuntimeError("dispatcher unavailable")
    stats = ENGINE.dispatcher.dispatch(limit=int(payload.get("limit") or 20))
    return ok(request_id, stats)


def op_outbox_status(request_id, payload):
    if ENGINE.dispatcher is None:
        raise RuntimeError("dispatcher unavailable")
    return ok(request_id, ENGINE.dispatcher.status())


def op_outbox_replay(request_id, payload):
    if ENGINE.dispatcher is None:
        raise RuntimeError("dispatcher unavailable")
    result = ENGINE.dispatcher.replay(
        payload.get("event_id"),
        payload.get("consumer_module"),
        payload.get("actor"),
        payload.get("reason"),
    )
    return ok(request_id, result)


def op_emotion_submit(request_id, payload):
    evidence = ENGINE.emotion_queue.get(payload.get("evidence_id"))
    if evidence is None:
        raise KeyError("evidence not found: %r" % payload.get("evidence_id"))
    workspace_id = evidence["workspace_id"]
    canonical = evidence["canonical_experience_id"]
    existing = ENGINE.emotion.find_by_experience(workspace_id, canonical)
    rid = request_id or str(uuid.uuid4())

    if existing:
        emotion_event_id = existing[0]["emotion_event_id"]
        result = ENGINE.emotion.revise_emotion_event(
            request_id=rid,
            workspace_id=workspace_id,
            emotion_event_id=emotion_event_id,
            correction={
                "event_type": payload.get("event_type"),
                "confidence": float(payload.get("confidence") or 0.0),
                "emotion_candidates": payload.get("emotion_candidates"),
            },
            reason=payload.get("reason") or "model re-evaluation",
        )
    else:
        result = ENGINE.emotion.submit_emotion_event(
            request_id=rid,
            workspace_id=workspace_id,
            source_experience_id=canonical,
            source_event_id=evidence["source_experience_id"],
            source_type="experience",
            event_type=payload.get("event_type"),
            event_slot=0,
            description=payload.get("description") or "",
            confidence=float(payload.get("confidence") or 0.0),
            emotion_candidates=payload.get("emotion_candidates"),
        )
        emotion_event_id = result["emotion_event_id"]

    ENGINE.emotion_queue.mark_evaluated(evidence["evidence_id"], emotion_event_id)
    return ok(request_id, dict(result, emotion_event_id=emotion_event_id))


def op_emotion_queue_pending(request_id, payload):
    rows = ENGINE.emotion_queue.pending_eval(limit=int(payload.get("limit") or 5))
    return ok(request_id, {"evidences": rows})


def op_emotion_queue_fail(request_id, payload):
    status = ENGINE.emotion_queue.mark_failed(payload.get("evidence_id"), payload.get("error") or "unknown")
    return ok(request_id, {"queue_status": status})


def op_emotion_get(request_id, payload):
    if ENGINE.emotion is None:
        raise RuntimeError("emotion module unavailable")
    workspace_id = payload.get("workspace_id") or "unknown"
    events = ENGINE.emotion.find_by_experience(workspace_id, payload.get("experience_id"))
    return ok(request_id, {"events": events})


def op_emotion_queue_workspace(request_id, payload):
    rows = ENGINE.emotion_queue.pending_for_workspace(
        payload.get("workspace_id") or "unknown", int(payload.get("limit") or 20)
    )
    return ok(request_id, {"evidences": rows})


def op_relationship_submit_from_eval(request_id, payload):
    """Node 评估结果 → 关系模块（对象候选 + 信号 + 强度），不决定权重。"""
    evidence = ENGINE.relationship_queue.get(payload.get("evidence_id"))
    if evidence is None:
        raise KeyError("relationship evidence not found: %r" % payload.get("evidence_id"))
    counterpart = payload.get("counterpart") or {}
    identity_key = counterpart.get("identity_key")
    if not identity_key:
        # 缺稳定身份键：只保留待解析候选，不创建正式 Counterpart/Relationship/Evidence。
        ENGINE.relationship_queue.mark_evaluated(
            evidence["evidence_id"],
            None,
            candidate={
                "display_name": counterpart.get("display_name") or "",
                "identity_type": counterpart.get("identity_type") or "counterpart",
                "reason": "identity_key missing (needs stable resolution)",
            },
        )
        return ok(request_id, {"skipped": True, "reason": "no stable identity key"})

    identity_type = counterpart.get("identity_type") or "counterpart"
    resolved = ENGINE.relationship.resolve_counterpart(identity_type, identity_key)
    if resolved["found"]:
        counterpart_id = resolved["counterpart_id"]
    else:
        created = ENGINE.relationship.create_counterpart(
            request_id=(request_id or str(uuid.uuid4())) + ":cp",
            identity_type=identity_type,
            identity_key=identity_key,
            display_name=counterpart.get("display_name") or "",
        )
        counterpart_id = created["counterpart_id"]

    rel = ENGINE.relationship.ensure_relationship(
        workspace_id=evidence["workspace_id"], counterpart_id=counterpart_id
    )
    experience = ENGINE.memory.get_experience(evidence["canonical_experience_id"])
    event_ids = experience["event_ids"] if experience else []
    if not event_ids:
        ENGINE.relationship_queue.mark_failed(evidence["evidence_id"], "experience has no source events")
        return ok(request_id, {"skipped": True, "reason": "no source events"})

    result = ENGINE.relationship.submit_trust_evidence(
        request_id=request_id or str(uuid.uuid4()),
        relationship_id=rel["relationship_id"],
        source_experience_id=evidence["canonical_experience_id"],
        source_event_ids=event_ids,
        event_slot="0",
        trust_signal=float(payload.get("trust_signal") or 0.0),
        evidence_strength=payload.get("evidence_strength") or "weak",
        expected_revision=rel["revision"],
    )
    ENGINE.relationship_queue.mark_evaluated(evidence["evidence_id"], result["evidence_id"])
    return ok(request_id, dict(result, relationship_id=rel["relationship_id"], counterpart_id=counterpart_id))


def op_relationship_queue_pending(request_id, payload):
    rows = ENGINE.relationship_queue.pending_eval(limit=int(payload.get("limit") or 5))
    return ok(request_id, {"evidences": rows})


def op_relationship_queue_fail(request_id, payload):
    status = ENGINE.relationship_queue.mark_failed(
        payload.get("evidence_id"), payload.get("error") or "unknown"
    )
    return ok(request_id, {"queue_status": status})


def op_relationship_get_trust(request_id, payload):
    trust = ENGINE.relationship.get_trust(payload.get("relationship_id"))
    if trust is None:
        return fail(request_id, "NOT_FOUND", "relationship not found: %r" % payload.get("relationship_id"))
    return ok(request_id, trust)


def op_relationship_queue_workspace(request_id, payload):
    rows = ENGINE.relationship_queue.pending_for_workspace(
        payload.get("workspace_id") or "unknown", int(payload.get("limit") or 20)
    )
    return ok(request_id, {"evidences": rows})


def op_relationship_evidence_get(request_id, payload):
    row = ENGINE.db.conn.execute(
        "SELECT * FROM trust_evidence WHERE evidence_id = ?", (payload.get("evidence_id"),)
    ).fetchone()
    if row is None:
        return fail(request_id, "NOT_FOUND", "trust evidence not found: %r" % payload.get("evidence_id"))
    data = dict(row)
    data["source_event_ids"] = [
        r["source_event_id"]
        for r in ENGINE.db.conn.execute(
            "SELECT source_event_id FROM trust_evidence_source_events WHERE evidence_id = ?",
            (payload.get("evidence_id"),),
        )
    ]
    return ok(request_id, data)


def op_memory_experience_check(request_id, payload):
    if ENGINE.memory is None:
        raise RuntimeError("memory module unavailable")
    result = ENGINE.memory.check_experience(
        experience_id=payload.get("experience_id"),
        source_event_ids=payload.get("source_event_ids") or [],
        workspace_id=payload.get("workspace_id") or "unknown",
    )
    return ok(request_id, result)


def op_learning_submit_evidence(request_id, payload):
    evidence = ENGINE.learning_queue.get(payload.get("evidence_id"))
    if evidence is None:
        raise KeyError("learning evidence not found: %r" % payload.get("evidence_id"))
    workspace_id = evidence["workspace_id"]
    canonical = evidence["canonical_experience_id"]
    conclusion_id = payload.get("conclusion_id")
    rid = request_id or str(uuid.uuid4())

    if not conclusion_id:
        proposal = ENGINE.learning.propose_conclusion(
            workspace_id=workspace_id,
            statement=payload.get("statement") or "",
            source_experience_id=canonical,
        )
        ENGINE.learning_queue.mark_evaluated(evidence["evidence_id"], proposal["proposal_id"])
        return ok(request_id, {"proposed": True, **proposal})

    experience = ENGINE.memory.get_experience(canonical)
    event_ids = experience["event_ids"] if experience else []
    added = ENGINE.learning.add_conclusion_evidence(
        request_id=rid + ":ev",
        workspace_id=workspace_id,
        conclusion_id=conclusion_id,
        experience_id=canonical,
        source_event_ids=event_ids,
        polarity=payload.get("polarity") or "support",
        evidence_type=payload.get("evidence_type") or "",
        rationale=payload.get("rationale") or "",
    )
    evaluation = ENGINE.learning.evaluate_conclusion(request_id=rid + ":eval", conclusion_id=conclusion_id)
    # P14-D1：确认自动路由——该结论下 pending 的人格请求，用当前经历尝试确认。
    # 学习模块只发现机会；人格模块独立核验身份、独立性与窗口；来源证据在 confirm 内被拒绝。
    confirmations = []
    if added.get("created"):
        rows = ENGINE.db.conn.execute(
            "SELECT influence_request_id FROM learning_influence_requests"
            " WHERE conclusion_id = ? AND status IN ('pending', 'confirming')",
            (conclusion_id,),
        ).fetchall()
        for row in rows:
            try:
                confirmations.append(ENGINE.personality.confirm_learning_influence(
                    request_id="auto_confirm:%s:%s" % (evidence["evidence_id"], row["influence_request_id"]),
                    influence_request_id=row["influence_request_id"],
                    experience_id=canonical,
                    source_event_ids=event_ids,
                ))
            except Exception as error:  # noqa: BLE001 - 单条确认失败不阻塞证据
                log("auto confirm failed: %s" % error)
    # P14-E2：新证据刷新同结论实例的衰减锚点（只重置未来，不恢复已衰减压力）。
    refreshed = []
    if added.get("created"):
        experience = ENGINE.memory.get_experience(canonical)
        if experience is not None:
            with ENGINE.db.transaction() as conn:
                refreshed = ENGINE.decay.refresh_anchor_for_conclusion(
                    conn,
                    conclusion_id=conclusion_id,
                    evidence_id=added["evidence_id"],
                    experience_id=canonical,
                    occurred_at=int(experience["occurred_at"]),
                    independence_key=experience["independence_key"],
                    accepted_at=int(time.time() * 1000),
                )
    ENGINE.learning_queue.mark_evaluated(evidence["evidence_id"])
    return ok(request_id, {"evidence": added, "evaluation": evaluation, "confirmations": confirmations, "refreshed_instances": refreshed})


def op_learning_queue_pending(request_id, payload):
    rows = ENGINE.learning_queue.pending_eval(limit=int(payload.get("limit") or 5))
    return ok(request_id, {"evidences": rows})


def op_learning_queue_fail(request_id, payload):
    status = ENGINE.learning_queue.mark_failed(payload.get("evidence_id"), payload.get("error") or "unknown")
    return ok(request_id, {"queue_status": status})


def op_learning_queue_workspace(request_id, payload):
    rows = ENGINE.learning_queue.pending_for_workspace(
        payload.get("workspace_id") or "unknown", int(payload.get("limit") or 20)
    )
    return ok(request_id, {"evidences": rows})


def op_learning_conclusion_create(request_id, payload):
    result = ENGINE.learning.create_conclusion(
        request_id=request_id or str(uuid.uuid4()),
        workspace_id=payload.get("workspace_id") or "unknown",
        statement=payload.get("statement") or "",
    )
    return ok(request_id, result)


def op_learning_conclusion_get(request_id, payload):
    conclusion = ENGINE.learning.get_conclusion(payload.get("conclusion_id"))
    if conclusion is None:
        return fail(request_id, "NOT_FOUND", "conclusion not found: %r" % payload.get("conclusion_id"))
    return ok(request_id, conclusion)


def op_learning_conclusions_list(request_id, payload):
    rows = ENGINE.db.conn.execute(
        "SELECT conclusion_id, workspace_id, statement, status FROM conclusions"
        " WHERE workspace_id = ? ORDER BY created_at DESC LIMIT ?",
        (payload.get("workspace_id") or "unknown", int(payload.get("limit") or 20)),
    ).fetchall()
    return ok(request_id, {"conclusions": [dict(r) for r in rows]})


def op_learning_suggestion_pending(request_id, payload):
    rows = ENGINE.learning.pending_suggestions(limit=int(payload.get("limit") or 20))
    return ok(request_id, {"suggestions": rows})


def op_learning_suggestion_revoke(request_id, payload):
    result = ENGINE.learning.revoke_suggestion(
        request_id=request_id or str(uuid.uuid4()),
        suggestion_id=payload.get("suggestion_id"),
        reason=payload.get("reason") or "unspecified",
    )
    return ok(request_id, result)


def op_personality_confirm(request_id, payload):
    result = ENGINE.personality.confirm_learning_influence(
        request_id=request_id or str(uuid.uuid4()),
        influence_request_id=payload.get("influence_request_id"),
        experience_id=payload.get("experience_id"),
        source_event_ids=payload.get("source_event_ids") or [],
    )
    return ok(request_id, result)


def op_personality_request_get(request_id, payload):
    request = ENGINE.personality.get_request(payload.get("influence_request_id"))
    if request is None:
        return fail(request_id, "NOT_FOUND", "influence request not found")
    return ok(request_id, request)


def op_personality_summary(request_id, payload):
    return ok(request_id, ENGINE.personality.get_summary())


def op_personality_request_create(request_id, payload):
    """工程接口：直接构造 suggestion 并交给人格领取（测试/协调器用）。"""
    suggestion = {
        "suggestion_id": payload.get("suggestion_id") or str(uuid.uuid4()),
        "confidence": float(payload.get("confidence") or 0.5),
    }
    result = ENGINE.personality.handle_learning_suggestion(
        request_id=request_id or str(uuid.uuid4()),
        suggestion=suggestion,
        conclusion_id=payload.get("conclusion_id") or "unknown",
    )
    return ok(request_id, result)


def op_behavior_submit(request_id, payload):
    if ENGINE.behavior is None:
        raise RuntimeError("behavior module unavailable")
    result = ENGINE.behavior.submit_behavior_observation(
        request_id=request_id or str(uuid.uuid4()),
        session_id=payload.get("session_id") or "unknown",
        workspace_id=payload.get("workspace_id") or "unknown",
        observation_type=payload.get("observation_type") or "interaction_observation",
        target_layer=payload.get("target_layer") or "current",
        observation_payload=payload.get("observation_payload") or {},
        ttl=payload.get("ttl"),
    )
    return ok(request_id, result)


def op_behavior_guidance(request_id, payload):
    if ENGINE.behavior is None:
        raise RuntimeError("behavior module unavailable")
    guidance = ENGINE.behavior.get_behavior_guidance(
        payload.get("session_id") or "unknown",
        payload.get("workspace_id") or "unknown",
    )
    return ok(request_id, guidance)


def op_emotion_advance(request_id, payload):
    if ENGINE.emotion is None:
        raise RuntimeError("emotion module unavailable")
    result = ENGINE.emotion.advance_emotion_state(
        request_id=request_id or str(uuid.uuid4()),
        workspace_id=payload.get("workspace_id") or "unknown",
        target_time=int(payload.get("target_time") or int(time.time() * 1000)),
    )
    return ok(request_id, result)


def op_emotion_state_summary(request_id, payload):
    if ENGINE.emotion is None:
        raise RuntimeError("emotion module unavailable")
    return ok(request_id, ENGINE.emotion.get_emotion_summary(payload.get("workspace_id") or "unknown"))


def op_personality_decay(request_id, payload):
    if ENGINE.decay is None:
        raise RuntimeError("decay module unavailable")
    result = ENGINE.decay.decay(
        request_id=request_id or str(uuid.uuid4()),
        as_of=int(payload.get("as_of") or int(time.time() * 1000)),
        cycle_id=payload.get("cycle_id"),
    )
    return ok(request_id, result)


def op_personality_settle(request_id, payload):
    if ENGINE.personality is None:
        raise RuntimeError("personality module unavailable")
    result = ENGINE.personality.settle(
        request_id=request_id or str(uuid.uuid4()),
        trait_name=payload.get("trait_name") or "openness",
        now=int(payload.get("now") or int(time.time() * 1000)),
    )
    return ok(request_id, result)


def op_memory_workspaces(request_id, payload):
    rows = ENGINE.db.conn.execute(
        "SELECT DISTINCT workspace_id FROM memory_experience ORDER BY workspace_id"
    ).fetchall()
    return ok(request_id, {"workspaces": [r["workspace_id"] for r in rows]})


def op_daily_cycle_begin(request_id, payload):
    result = ENGINE.daily.ensure_cycle(
        timezone=payload.get("timezone") or "Asia/Shanghai",
        schedule_date=payload.get("schedule_date") or time.strftime("%Y-%m-%d"),
        scheduled_at=int(payload.get("scheduled_at") or int(time.time() * 1000)),
    )
    return ok(request_id, result)


def op_daily_cycle_claim(request_id, payload):
    return ok(request_id, {"claimed": ENGINE.daily.claim(payload.get("cycle_id"))})


def op_daily_cycle_step_begin(request_id, payload):
    ENGINE.daily.step_begin(payload.get("cycle_id"), payload.get("step_name"))
    return ok(request_id, {"ok": True})


def op_daily_cycle_step_complete(request_id, payload):
    ENGINE.daily.step_complete(payload.get("cycle_id"), payload.get("step_name"), payload.get("detail"))
    return ok(request_id, {"ok": True})


def op_daily_cycle_step_skip(request_id, payload):
    ENGINE.daily.step_skip(payload.get("cycle_id"), payload.get("step_name"), payload.get("reason") or "skipped")
    return ok(request_id, {"ok": True})


def op_daily_cycle_step_fail(request_id, payload):
    result = ENGINE.daily.step_fail(payload.get("cycle_id"), payload.get("step_name"), payload.get("error") or "unknown")
    return ok(request_id, result)


def op_daily_cycle_complete(request_id, payload):
    ENGINE.daily.complete(payload.get("cycle_id"), payload.get("detail") or {})
    return ok(request_id, {"ok": True})


def op_daily_cycle_status(request_id, payload):
    status = ENGINE.daily.status(payload.get("cycle_id"))
    if status is None:
        return fail(request_id, "NOT_FOUND", "cycle not found")
    return ok(request_id, status)


def op_daily_cycle_recover(request_id, payload):
    ENGINE.daily.recover(payload.get("cycle_id"))
    return ok(request_id, {"ok": True})


def op_daily_cycle_recoverable(request_id, payload):
    return ok(request_id, {"cycles": ENGINE.daily.recoverable()})


def op_daily_cycle_abandon(request_id, payload):
    ENGINE.daily.abandon(payload.get("cycle_id"), payload.get("reason") or "stale_cycle")
    return ok(request_id, {"ok": True})


def op_reconcile_outbox(request_id, payload):
    if ENGINE.reconciliation is None:
        raise RuntimeError("reconciliation module unavailable")
    run_type = payload.get("run_type") or "incremental"
    cursor = payload.get("cursor")
    if cursor is None:
        row = ENGINE.db.conn.execute(
            "SELECT cursor_after FROM outbox_reconciliation_runs WHERE status = 'completed'"
            " AND run_type = ? ORDER BY completed_at DESC LIMIT 1",
            (run_type,),
        ).fetchone()
        if row is not None and row["cursor_after"]:
            cursor = json.loads(row["cursor_after"])
    result = ENGINE.reconciliation.reconcile(
        request_id=request_id or str(uuid.uuid4()),
        run_type=run_type,
        cursor=cursor,
    )
    return ok(request_id, result)


def op_reconciliation_report(request_id, payload):
    row = ENGINE.db.conn.execute(
        "SELECT * FROM outbox_reconciliation_runs WHERE run_id = ?", (payload.get("run_id"),)
    ).fetchone()
    if row is None:
        return fail(request_id, "NOT_FOUND", "reconciliation run not found")
    return ok(request_id, dict(row))


def op_reconciliation_issues(request_id, payload):
    sql = "SELECT * FROM outbox_reconciliation_issues WHERE 1=1"
    params = []
    if payload.get("severity"):
        sql += " AND severity = ?"
        params.append(payload["severity"])
    if payload.get("issue_status"):
        sql += " AND issue_status = ?"
        params.append(payload["issue_status"])
    sql += " ORDER BY detected_at DESC LIMIT ?"
    params.append(int(payload.get("limit") or 100))
    rows = ENGINE.db.conn.execute(sql, params).fetchall()
    return ok(request_id, {"issues": [dict(r) for r in rows]})


def _issue_transition(request_id, payload, new_status, action):
    issue_id = payload.get("issue_id")
    actor = (payload.get("actor") or "").strip()
    reason = (payload.get("reason") or "").strip()
    if not actor:
        raise ValueError("actor is required")
    if not reason:
        raise ValueError("reason is required")
    with ENGINE.db.transaction() as conn:
        row = conn.execute(
            "SELECT * FROM outbox_reconciliation_issues WHERE issue_id = ?", (issue_id,)
        ).fetchone()
        if row is None:
            raise KeyError("issue not found: %s" % issue_id)
        if row["issue_status"] == new_status:
            # 重复操作幂等：不再写第二条 Audit。
            return {"ok": True, "issue_id": issue_id, "issue_status": new_status, "already": True}
        conn.execute(
            "UPDATE outbox_reconciliation_issues SET issue_status = ?, resolution_ref = ?,"
            " resolved_at = CASE WHEN ? IN ('resolved') THEN ? ELSE resolved_at END WHERE issue_id = ?",
            (new_status, "%s:%s" % (actor, reason), new_status, int(time.time() * 1000), issue_id),
        )
        ENGINE.audit.write(
            conn, "reconciliation", action, "reconciliation_issue", issue_id,
            before={"issue_status": row["issue_status"]},
            after={"issue_status": new_status, "actor": actor, "reason": reason},
            request_id=request_id,
        )
    return {"ok": True, "issue_id": issue_id, "issue_status": new_status}


def op_acknowledge_issue(request_id, payload):
    return ok(request_id, _issue_transition(request_id, payload, "acknowledged", "issue.acknowledge"))


def op_ignore_issue(request_id, payload):
    return ok(request_id, _issue_transition(request_id, payload, "ignored", "issue.ignore"))


def op_memory_revise(request_id, payload):
    result = ENGINE.memory.revise_experience(
        request_id=request_id or str(uuid.uuid4()),
        experience_id=payload.get("experience_id"),
        correction=payload.get("correction") or {},
        expected_change_version=int(payload.get("expected_change_version") or 0),
        actor=payload.get("actor"),
    )
    return ok(request_id, result)


def op_memory_merge(request_id, payload):
    result = ENGINE.memory.merge_experiences(
        request_id=request_id or str(uuid.uuid4()),
        source_experience_ids=payload.get("source_experience_ids") or [],
        canonical_experience_id=payload.get("canonical_experience_id"),
        expected_change_versions=payload.get("expected_change_versions") or {},
        actor=payload.get("actor"),
    )
    return ok(request_id, result)


def op_memory_split(request_id, payload):
    result = ENGINE.memory.split_experience(
        request_id=request_id or str(uuid.uuid4()),
        source_experience_id=payload.get("source_experience_id"),
        new_experience_specs=payload.get("new_experience_specs") or [],
        expected_change_version=int(payload.get("expected_change_version") or 0),
        actor=payload.get("actor"),
    )
    return ok(request_id, result)


def op_memory_resolve_canonical(request_id, payload):
    result = ENGINE.memory.resolve_canonical_experience(
        payload.get("experience_id"), payload.get("workspace_id") or "unknown"
    )
    return ok(request_id, result)


def op_memory_change_get(request_id, payload):
    change = ENGINE.memory.get_experience_change(
        payload.get("operation_id"), payload.get("workspace_id") or "unknown"
    )
    if change is None:
        return fail(request_id, "NOT_FOUND", "experience change not found")
    return ok(request_id, change)


def op_memory_split_mapping_get(request_id, payload):
    rows = ENGINE.memory.get_experience_split_mapping(
        payload.get("operation_id"), payload.get("workspace_id") or "unknown"
    )
    return ok(request_id, {"mappings": rows})


def op_change_consumer_begin(request_id, payload):
    result = ENGINE.experience_consumer.begin(
        operation_id=payload.get("operation_id"),
        consumer_module=payload.get("consumer_module"),
        operation_type=payload.get("operation_type") or "revise",
        mapping_version=int(payload.get("mapping_version") or 0),
    )
    return ok(request_id, result)


def op_change_consumer_complete(request_id, payload):
    result = ENGINE.experience_consumer.complete(
        operation_id=payload.get("operation_id"),
        consumer_module=payload.get("consumer_module"),
        result=payload.get("result"),
    )
    return ok(request_id, result)


def op_change_consumer_fail(request_id, payload):
    result = ENGINE.experience_consumer.fail(
        operation_id=payload.get("operation_id"),
        consumer_module=payload.get("consumer_module"),
        error=payload.get("error") or "unknown",
    )
    return ok(request_id, result)


def op_change_operation_status(request_id, payload):
    status = ENGINE.experience_consumer.operation_status(payload.get("operation_id"))
    if status is None:
        return fail(request_id, "NOT_FOUND", "operation not found")
    return ok(request_id, status)


def op_review_enqueue(request_id, payload):
    result = ENGINE.experience_consumer.enqueue_review(
        workspace_id=payload.get("workspace_id") or "unknown",
        operation_id=payload.get("operation_id"),
        consumer_module=payload.get("consumer_module") or "unknown",
        evidence_ref=payload.get("evidence_ref") or "unknown",
        reason=payload.get("reason") or "consumer_validation_failed",
        source_experience_id=payload.get("source_experience_id"),
        target_experience_ids=payload.get("target_experience_ids"),
        source_event_id=payload.get("source_event_id"),
    )
    return ok(request_id, result)


def op_review_resolve(request_id, payload):
    result = ENGINE.experience_consumer.resolve_review(
        review_id=payload.get("review_id"),
        review_status=payload.get("review_status") or "resolved",
        actor=payload.get("actor"),
        reason=payload.get("reason"),
        resolution_ref=payload.get("resolution_ref"),
    )
    return ok(request_id, result)


def op_review_list(request_id, payload):
    rows = ENGINE.experience_consumer.list_reviews(filters=payload.get("filters") or {},
                                                   limit=int(payload.get("limit") or 100))
    return ok(request_id, {"reviews": rows})


def op_emotion_relocate(request_id, payload):
    if ENGINE.emotion is None:
        raise RuntimeError("emotion module unavailable")
    result = ENGINE.emotion.relocate_emotion_events_for_experience_change(
        request_id=request_id or str(uuid.uuid4()),
        operation_id=payload.get("operation_id"),
        operation_type=payload.get("operation_type") or "merge",
        workspace_id=payload.get("workspace_id") or "unknown",
        source_experience_id=payload.get("source_experience_id"),
        mapping_version=int(payload.get("mapping_version") or 0),
        canonical_experience_id=payload.get("canonical_experience_id"),
        event_targets=payload.get("event_targets"),
    )
    return ok(request_id, result)


def op_learning_relocate(request_id, payload):
    if ENGINE.learning is None:
        raise RuntimeError("learning module unavailable")
    result = ENGINE.learning.relocate_conclusion_evidence_for_merge(
        request_id=request_id or str(uuid.uuid4()),
        operation_id=payload.get("operation_id"),
        workspace_id=payload.get("workspace_id") or "unknown",
        source_experience_id=payload.get("source_experience_id"),
        canonical_experience_id=payload.get("canonical_experience_id"),
        mapping_version=int(payload.get("mapping_version") or 0),
    )
    return ok(request_id, result)


def op_personality_relocate(request_id, payload):
    if ENGINE.personality is None:
        raise RuntimeError("personality module unavailable")
    result = ENGINE.personality.relocate_personality_confirmations_for_merge(
        request_id=request_id or str(uuid.uuid4()),
        operation_id=payload.get("operation_id"),
        workspace_id=payload.get("workspace_id") or "unknown",
        source_experience_id=payload.get("source_experience_id"),
        canonical_experience_id=payload.get("canonical_experience_id"),
        mapping_version=int(payload.get("mapping_version") or 0),
    )
    return ok(request_id, result)


def handle(message):
    """返回 (response, should_exit)。"""
    if not isinstance(message, dict):
        return fail(None, "BAD_MESSAGE", "message must be a JSON object"), False
    request_id = message.get("request_id")
    operation = message.get("type")
    payload = message.get("payload") or {}

    if operation == "ping":
        return ok(request_id, {"pong": True, "engine": ENGINE_NAME}), False
    if operation == "slow_ping":
        # 仅用于桥接层超时路径的确定性测试。
        import time

        time.sleep(float(payload.get("seconds", 0.2)))
        return ok(request_id, {"pong": True, "slow": True}), False
    if operation in ("hello", "get_status"):
        return ok(request_id, ENGINE.status()), False
    if operation == "get_summary":
        return op_get_summary(request_id, payload), False
    if operation == "memory_create_experience":
        return op_memory_create_experience(request_id, payload), False
    if operation == "memory_get_experience":
        return op_memory_get_experience(request_id, payload), False
    if operation == "memory_summary":
        return op_memory_summary(request_id, payload), False
    if operation == "memory_verify_experience":
        return op_memory_verify_experience(request_id, payload), False
    if operation == "memory_list_pending":
        return op_memory_list_pending(request_id, payload), False
    if operation == "db_status":
        return op_db_status(request_id), False
    if operation == "self_test":
        return op_self_test(request_id), False
    if operation == "outbox_dispatch":
        return op_outbox_dispatch(request_id, payload), False
    if operation == "outbox_status":
        return op_outbox_status(request_id, payload), False
    if operation == "outbox_replay":
        return op_outbox_replay(request_id, payload), False
    if operation == "emotion_submit":
        return op_emotion_submit(request_id, payload), False
    if operation == "emotion_queue_pending":
        return op_emotion_queue_pending(request_id, payload), False
    if operation == "emotion_queue_fail":
        return op_emotion_queue_fail(request_id, payload), False
    if operation == "emotion_relocate":
        return op_emotion_relocate(request_id, payload), False
    if operation == "emotion_get":
        return op_emotion_get(request_id, payload), False
    if operation == "emotion_queue_workspace":
        return op_emotion_queue_workspace(request_id, payload), False
    if operation == "emotion_advance":
        return op_emotion_advance(request_id, payload), False
    if operation == "emotion_state_summary":
        return op_emotion_state_summary(request_id, payload), False
    if operation == "relationship_submit_from_eval":
        return op_relationship_submit_from_eval(request_id, payload), False
    if operation == "relationship_queue_pending":
        return op_relationship_queue_pending(request_id, payload), False
    if operation == "relationship_queue_fail":
        return op_relationship_queue_fail(request_id, payload), False
    if operation == "relationship_queue_workspace":
        return op_relationship_queue_workspace(request_id, payload), False
    if operation == "relationship_get_trust":
        return op_relationship_get_trust(request_id, payload), False
    if operation == "relationship_evidence_get":
        return op_relationship_evidence_get(request_id, payload), False
    if operation == "change_consumer_begin":
        return op_change_consumer_begin(request_id, payload), False
    if operation == "change_consumer_complete":
        return op_change_consumer_complete(request_id, payload), False
    if operation == "change_consumer_fail":
        return op_change_consumer_fail(request_id, payload), False
    if operation == "change_operation_status":
        return op_change_operation_status(request_id, payload), False
    if operation == "review_enqueue":
        return op_review_enqueue(request_id, payload), False
    if operation == "review_resolve":
        return op_review_resolve(request_id, payload), False
    if operation == "review_list":
        return op_review_list(request_id, payload), False
    if operation == "memory_revise":
        return op_memory_revise(request_id, payload), False
    if operation == "memory_merge":
        return op_memory_merge(request_id, payload), False
    if operation == "memory_split":
        return op_memory_split(request_id, payload), False
    if operation == "memory_resolve_canonical":
        return op_memory_resolve_canonical(request_id, payload), False
    if operation == "memory_change_get":
        return op_memory_change_get(request_id, payload), False
    if operation == "memory_split_mapping_get":
        return op_memory_split_mapping_get(request_id, payload), False
    if operation == "memory_experience_check":
        return op_memory_experience_check(request_id, payload), False
    if operation == "learning_submit_evidence":
        return op_learning_submit_evidence(request_id, payload), False
    if operation == "learning_queue_pending":
        return op_learning_queue_pending(request_id, payload), False
    if operation == "learning_queue_fail":
        return op_learning_queue_fail(request_id, payload), False
    if operation == "learning_queue_workspace":
        return op_learning_queue_workspace(request_id, payload), False
    if operation == "learning_conclusion_create":
        return op_learning_conclusion_create(request_id, payload), False
    if operation == "learning_conclusion_get":
        return op_learning_conclusion_get(request_id, payload), False
    if operation == "learning_relocate":
        return op_learning_relocate(request_id, payload), False
    if operation == "learning_conclusions_list":
        return op_learning_conclusions_list(request_id, payload), False
    if operation == "learning_suggestion_pending":
        return op_learning_suggestion_pending(request_id, payload), False
    if operation == "learning_suggestion_revoke":
        return op_learning_suggestion_revoke(request_id, payload), False
    if operation == "personality_confirm":
        return op_personality_confirm(request_id, payload), False
    if operation == "personality_relocate":
        return op_personality_relocate(request_id, payload), False
    if operation == "personality_request_get":
        return op_personality_request_get(request_id, payload), False
    if operation == "personality_summary":
        return op_personality_summary(request_id, payload), False
    if operation == "personality_request_create":
        return op_personality_request_create(request_id, payload), False
    if operation == "behavior_submit":
        return op_behavior_submit(request_id, payload), False
    if operation == "behavior_guidance":
        return op_behavior_guidance(request_id, payload), False
    if operation == "personality_decay":
        return op_personality_decay(request_id, payload), False
    if operation == "personality_settle":
        return op_personality_settle(request_id, payload), False
    if operation == "memory_workspaces":
        return op_memory_workspaces(request_id, payload), False
    if operation == "reconcile_outbox":
        return op_reconcile_outbox(request_id, payload), False
    if operation == "reconciliation_report":
        return op_reconciliation_report(request_id, payload), False
    if operation == "reconciliation_issues":
        return op_reconciliation_issues(request_id, payload), False
    if operation == "acknowledge_issue":
        return op_acknowledge_issue(request_id, payload), False
    if operation == "ignore_issue":
        return op_ignore_issue(request_id, payload), False
    if operation == "daily_cycle_begin":
        return op_daily_cycle_begin(request_id, payload), False
    if operation == "daily_cycle_claim":
        return op_daily_cycle_claim(request_id, payload), False
    if operation == "daily_cycle_step_begin":
        return op_daily_cycle_step_begin(request_id, payload), False
    if operation == "daily_cycle_step_complete":
        return op_daily_cycle_step_complete(request_id, payload), False
    if operation == "daily_cycle_step_skip":
        return op_daily_cycle_step_skip(request_id, payload), False
    if operation == "daily_cycle_step_fail":
        return op_daily_cycle_step_fail(request_id, payload), False
    if operation == "daily_cycle_complete":
        return op_daily_cycle_complete(request_id, payload), False
    if operation == "daily_cycle_status":
        return op_daily_cycle_status(request_id, payload), False
    if operation == "daily_cycle_recover":
        return op_daily_cycle_recover(request_id, payload), False
    if operation == "daily_cycle_recoverable":
        return op_daily_cycle_recoverable(request_id, payload), False
    if operation == "daily_cycle_abandon":
        return op_daily_cycle_abandon(request_id, payload), False
    if operation == "shutdown":
        log("shutdown requested")
        return ok(request_id, {"bye": True}), True
    return fail(request_id, "UNKNOWN_OPERATION", "unknown operation: %r" % (operation,)), False


def main():
    ENGINE.init()
    if ENGINE.db_error:
        log("database init failed: %s" % ENGINE.db_error)
    ready = {"type": "ready"}
    ready.update(ENGINE.status())
    send(ready)
    log("ready (python %s, schema v%d)" % (sys.version.split()[0], ENGINE.status()["schema_version"]))

    try:
        for raw in sys.stdin:
            line = raw.strip()
            if not line:
                continue
            try:
                message = json.loads(line)
            except ValueError as error:
                send(fail(None, "BAD_JSON", str(error)))
                continue
            try:
                response, should_exit = handle(message)
            except Exception as error:  # noqa: BLE001 - 协议层兜底，不允许单条消息打崩进程
                send(
                    fail(
                        message.get("request_id") if isinstance(message, dict) else None,
                        "INTERNAL",
                        str(error),
                    )
                )
                continue
            send(response)
            if should_exit:
                break
    finally:
        ENGINE.close()
    log("bye")


if __name__ == "__main__":
    main()
