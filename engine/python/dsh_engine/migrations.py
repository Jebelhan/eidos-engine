"""版本化迁移。

约定：
- 每个迁移一个整数版本，只增不改；
- 迁移体只写 schema，不写业务数据；
- 已发布的迁移不得修改；需要修正时新增版本。
"""


def _m0001_initial(conn):
    statements = [
        # 统一请求幂等。同一 request_id 在同一 module 内唯一。
        """
        CREATE TABLE IF NOT EXISTS request_dedup (
            request_id      TEXT    NOT NULL,
            module_id       TEXT    NOT NULL,
            operation_type  TEXT    NOT NULL,
            payload_hash    TEXT    NOT NULL,
            request_status  TEXT    NOT NULL DEFAULT 'completed'
                            CHECK (request_status IN ('processing', 'completed', 'failed')),
            result_reference TEXT,
            created_at      INTEGER NOT NULL,
            completed_at    INTEGER,
            expires_at      INTEGER,
            PRIMARY KEY (request_id, module_id)
        )
        """,
        # 统一审计：只增不改（触发器兜底，应用层也不提供更新接口）。
        """
        CREATE TABLE IF NOT EXISTS audit_records (
            audit_id        TEXT    PRIMARY KEY,
            module_id       TEXT    NOT NULL,
            operation       TEXT    NOT NULL,
            target_type     TEXT    NOT NULL,
            target_id       TEXT    NOT NULL,
            before_snapshot TEXT,
            after_snapshot  TEXT,
            source_refs     TEXT,
            request_id      TEXT,
            logical_version INTEGER,
            created_at      INTEGER NOT NULL
        )
        """,
        """
        CREATE TRIGGER IF NOT EXISTS audit_records_no_update
        BEFORE UPDATE ON audit_records
        BEGIN SELECT RAISE(ABORT, 'audit_records is append-only'); END
        """,
        """
        CREATE TRIGGER IF NOT EXISTS audit_records_no_delete
        BEFORE DELETE ON audit_records
        BEGIN SELECT RAISE(ABORT, 'audit_records is append-only'); END
        """,
        # 统一 Outbox：pending → published → consumed；failed 可重试。
        """
        CREATE TABLE IF NOT EXISTS outbox_events (
            event_id         TEXT    PRIMARY KEY,
            event_type       TEXT    NOT NULL,
            producer_module  TEXT    NOT NULL,
            aggregate_type   TEXT    NOT NULL,
            aggregate_id     TEXT    NOT NULL,
            payload          TEXT    NOT NULL,
            logical_version  INTEGER,
            correlation_id   TEXT,
            causation_id     TEXT,
            created_at       INTEGER NOT NULL,
            published_at     INTEGER,
            consumed_at      INTEGER,
            status           TEXT    NOT NULL DEFAULT 'pending'
                             CHECK (status IN ('pending', 'published', 'consumed', 'failed')),
            attempt_count    INTEGER NOT NULL DEFAULT 0,
            next_retry_at    INTEGER,
            last_error       TEXT
        )
        """,
        # 多消费者独立记录：一个事件对所有已注册消费者的完成状态。
        """
        CREATE TABLE IF NOT EXISTS event_consumer_records (
            event_id        TEXT    NOT NULL,
            consumer_module TEXT    NOT NULL,
            consumer_status TEXT    NOT NULL DEFAULT 'pending'
                            CHECK (consumer_status IN ('pending', 'consumed', 'failed')),
            attempt_count   INTEGER NOT NULL DEFAULT 0,
            last_error      TEXT,
            consumed_at     INTEGER,
            PRIMARY KEY (event_id, consumer_module)
        )
        """,
        "CREATE INDEX IF NOT EXISTS outbox_events_status_idx ON outbox_events(status, created_at)",
        "CREATE INDEX IF NOT EXISTS audit_records_target_idx ON audit_records(target_type, target_id)",
    ]
    for sql in statements:
        conn.execute(sql)


def _m0002_memory(conn):
    statements = [
        # P4：记忆模块最小真实链路（Experience + 原始事件关联）。
        # 字段遵循记忆 V1.1.2；session_id 为 P4 扩展字段（记录创建上下文，不参与权威规则），
        # 用于按会话统计摘要，已向契约方报告。
        """
        CREATE TABLE IF NOT EXISTS memory_experience (
            experience_id          TEXT    PRIMARY KEY,
            workspace_id           TEXT    NOT NULL,
            session_id             TEXT,
            subject_id             TEXT,
            experience_type        TEXT    NOT NULL,
            occurred_at            INTEGER NOT NULL,
            created_at             INTEGER NOT NULL,
            description            TEXT    NOT NULL,
            verification_status    TEXT    NOT NULL DEFAULT 'pending'
                                   CHECK (verification_status IN
                                          ('pending', 'verified', 'uncertain', 'invalid', 'archived', 'split')),
            canonical_experience_id TEXT   NOT NULL,
            independence_key       TEXT    NOT NULL,
            correction_status      TEXT    NOT NULL DEFAULT 'normal',
            archived_at            INTEGER
        )
        """,
        "CREATE INDEX IF NOT EXISTS memory_experience_ws_idx ON memory_experience(workspace_id, verification_status)",
        "CREATE INDEX IF NOT EXISTS memory_experience_session_idx ON memory_experience(session_id)",
        """
        CREATE TABLE IF NOT EXISTS memory_experience_event_links (
            link_id        TEXT    PRIMARY KEY,
            experience_id  TEXT    NOT NULL,
            event_id       TEXT    NOT NULL,
            relation_type  TEXT    NOT NULL CHECK (relation_type IN ('source', 'mention', 'correction')),
            created_at     INTEGER NOT NULL,
            UNIQUE (experience_id, event_id, relation_type)
        )
        """,
        "CREATE INDEX IF NOT EXISTS memory_links_event_idx ON memory_experience_event_links(event_id)",
        "CREATE INDEX IF NOT EXISTS memory_links_exp_idx ON memory_experience_event_links(experience_id)",
    ]
    for sql in statements:
        conn.execute(sql)


def _m0003_session_index(conn):
    """session_id 边界固化：只做创建上下文，统计时 workspace+session 双重限定。

    复合索引服务该查询；session_id 单独索引保留（按会话排查用）。
    """
    conn.execute(
        "CREATE INDEX IF NOT EXISTS memory_experience_ws_session_idx"
        " ON memory_experience(workspace_id, session_id)"
    )


def _m0004_dispatcher(conn):
    """P8-B：事件订阅、消费者快照、死信标识。"""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS event_subscriptions ("
        " event_type TEXT NOT NULL,"
        " consumer_module TEXT NOT NULL,"
        " enabled INTEGER NOT NULL DEFAULT 1,"
        " created_at INTEGER NOT NULL,"
        " PRIMARY KEY (event_type, consumer_module))"
    )
    # 事件创建时快照订阅者，形成不可变预期消费者集合（写入 event_consumer_records）。
    conn.execute("ALTER TABLE outbox_events ADD COLUMN dead_letter_at INTEGER")
    conn.execute("ALTER TABLE outbox_events ADD COLUMN dead_letter_reason TEXT")
    conn.execute("ALTER TABLE event_consumer_records ADD COLUMN dead_letter_at INTEGER")
    conn.execute("ALTER TABLE event_consumer_records ADD COLUMN dead_letter_reason TEXT")
    conn.execute("ALTER TABLE event_consumer_records ADD COLUMN next_retry_at INTEGER")


def _m0005_emotion(conn):
    """P9：情绪接入层队列 + 情绪事件最小表（V1.3 契约）。

    emotion_pending_evidence 属于系统集成层（协调两系统），不是情绪模块契约表。
    """
    conn.execute(
        "CREATE TABLE IF NOT EXISTS emotion_pending_evidence ("
        " evidence_id TEXT PRIMARY KEY,"
        " workspace_id TEXT NOT NULL,"
        " canonical_experience_id TEXT NOT NULL,"
        " source_experience_id TEXT NOT NULL,"
        " source_status TEXT NOT NULL"
        "   CHECK (source_status IN ('uncertain', 'verified')),"
        " queue_status TEXT NOT NULL"
        "   CHECK (queue_status IN"
        "     ('awaiting_verification', 'pending_eval', 'retry_wait', 'evaluated', 'dead')),"
        " attempts INTEGER NOT NULL DEFAULT 0,"
        " next_retry_at INTEGER,"
        " last_error TEXT,"
        " emotion_event_id TEXT,"
        " created_at INTEGER NOT NULL,"
        " updated_at INTEGER NOT NULL,"
        " UNIQUE (workspace_id, canonical_experience_id))"
    )
    # 情绪事件最小表（V1.3 §三）；P9 只实现提交/修正，不实现 PAD/offset/推进。
    conn.execute(
        "CREATE TABLE IF NOT EXISTS emotion_events ("
        " emotion_event_id TEXT PRIMARY KEY,"
        " workspace_id TEXT NOT NULL,"
        " source_experience_id TEXT NOT NULL,"
        " source_event_id TEXT,"
        " source_type TEXT NOT NULL,"
        " event_type TEXT NOT NULL"
        "   CHECK (event_type IN ('positive', 'negative', 'mixed', 'neutral')),"
        " event_slot INTEGER NOT NULL DEFAULT 0,"
        " description TEXT,"
        " confidence REAL NOT NULL,"
        " emotion_candidates TEXT,"
        " status TEXT NOT NULL DEFAULT 'processed'"
        "   CHECK (status IN ('pending', 'processed', 'revised', 'expired', 'invalid')),"
        " created_at INTEGER NOT NULL,"
        " revised_at INTEGER,"
        " UNIQUE (workspace_id, source_experience_id, event_type, event_slot))"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS emotion_audit ("
        " audit_id TEXT PRIMARY KEY,"
        " operation TEXT NOT NULL,"
        " target_id TEXT NOT NULL,"
        " before_snapshot TEXT,"
        " after_snapshot TEXT,"
        " request_id TEXT,"
        " created_at INTEGER NOT NULL)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS emotion_pending_queue_idx"
        " ON emotion_pending_evidence(queue_status, next_retry_at)"
    )


def _m0006_relationship(conn):
    """P10：关系模块接入（R2 数据模型 + 接入层队列 + 证据身份）。"""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS system_identity ("
        " id TEXT PRIMARY KEY,"
        " value TEXT NOT NULL,"
        " created_at INTEGER NOT NULL)"
    )
    conn.execute(
        "INSERT OR IGNORE INTO system_identity(id, value, created_at)"
        " VALUES ('dsh_instance', lower(hex(randomblob(16))), CAST(strftime('%s','now') AS INTEGER) * 1000)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS counterpart ("
        " counterpart_id TEXT PRIMARY KEY,"
        " dsh_instance_id TEXT NOT NULL,"
        " counterpart_type TEXT NOT NULL,"
        " identity_namespace TEXT NOT NULL,"
        " identity_key TEXT NOT NULL,"
        " display_name TEXT,"
        " aliases TEXT,"
        " identity_status TEXT NOT NULL DEFAULT 'active'"
        "   CHECK (identity_status IN ('active', 'merged', 'invalid')),"
        " created_at INTEGER NOT NULL,"
        " updated_at INTEGER NOT NULL,"
        " UNIQUE (dsh_instance_id, counterpart_type, identity_namespace, identity_key))"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS relationship ("
        " relationship_id TEXT PRIMARY KEY,"
        " dsh_instance_id TEXT NOT NULL,"
        " counterpart_id TEXT NOT NULL,"
        " workspace_id TEXT NOT NULL,"
        " status TEXT NOT NULL DEFAULT 'active'"
        "   CHECK (status IN ('active', 'dormant', 'ended', 'blocked')),"
        " trust_value REAL NOT NULL DEFAULT 50,"
        " trust_confidence REAL NOT NULL DEFAULT 0,"
        " revision INTEGER NOT NULL DEFAULT 0,"
        " created_at INTEGER NOT NULL,"
        " updated_at INTEGER NOT NULL,"
        " UNIQUE (dsh_instance_id, counterpart_id))"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS trust_evidence ("
        " evidence_id TEXT PRIMARY KEY,"
        " workspace_id TEXT NOT NULL,"
        " relationship_id TEXT NOT NULL,"
        " source_experience_id TEXT NOT NULL,"
        " event_slot TEXT NOT NULL DEFAULT '0',"
        " trust_signal REAL NOT NULL CHECK (trust_signal BETWEEN -1 AND 1),"
        " evidence_weight REAL NOT NULL CHECK (evidence_weight > 0 AND evidence_weight <= 1),"
        " evidence_status TEXT NOT NULL DEFAULT 'active'"
        "   CHECK (evidence_status IN ('active', 'invalidated', 'pending_reassessment')),"
        " revision INTEGER NOT NULL DEFAULT 0,"
        " evidence_key TEXT NOT NULL,"
        " created_at INTEGER NOT NULL,"
        " updated_at INTEGER NOT NULL,"
        " UNIQUE (workspace_id, relationship_id, evidence_key))"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS trust_evidence_source_events ("
        " evidence_id TEXT NOT NULL,"
        " source_event_id TEXT NOT NULL,"
        " created_at INTEGER NOT NULL,"
        " PRIMARY KEY (evidence_id, source_event_id))"
    )
    # 来源事件集合创建后冻结：禁止后续更新与删除（含级联删除路径）。
    conn.execute(
        "CREATE TRIGGER IF NOT EXISTS trust_evidence_source_events_no_update"
        " BEFORE UPDATE ON trust_evidence_source_events"
        " BEGIN SELECT RAISE(ABORT, 'trust_evidence_source_events is immutable'); END"
    )
    conn.execute(
        "CREATE TRIGGER IF NOT EXISTS trust_evidence_source_events_no_delete"
        " BEFORE DELETE ON trust_evidence_source_events"
        " BEGIN SELECT RAISE(ABORT, 'trust_evidence_source_events is immutable'); END"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS relationship_pending_evidence ("
        " evidence_id TEXT PRIMARY KEY,"
        " workspace_id TEXT NOT NULL,"
        " canonical_experience_id TEXT NOT NULL,"
        " source_experience_id TEXT NOT NULL,"
        " source_status TEXT NOT NULL"
        "   CHECK (source_status IN ('uncertain', 'verified')),"
        " queue_status TEXT NOT NULL"
        "   CHECK (queue_status IN"
        "     ('awaiting_verification', 'pending_eval', 'retry_wait', 'evaluated', 'dead')),"
        " attempts INTEGER NOT NULL DEFAULT 0,"
        " next_retry_at INTEGER,"
        " last_error TEXT,"
        " evidence_ref TEXT,"
        " created_at INTEGER NOT NULL,"
        " updated_at INTEGER NOT NULL,"
        " UNIQUE (workspace_id, canonical_experience_id))"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS relationship_pending_queue_idx"
        " ON relationship_pending_evidence(queue_status, next_retry_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS trust_evidence_rel_idx ON trust_evidence(relationship_id, evidence_status)"
    )


def _m0007_identity_constraints(conn):
    """P10 身份约束补丁：待解析候选列 + 清理非稳定身份样本（2026-10-31 决策）。"""
    conn.execute("ALTER TABLE relationship_pending_evidence ADD COLUMN candidate TEXT")
    # 显示名充当身份键的记录不作为正式稳定身份样本，不纳入正式 Trust 统计。
    conn.execute(
        "UPDATE counterpart SET identity_status = 'invalid',"
        " updated_at = CAST(strftime('%s','now') AS INTEGER) * 1000"
        " WHERE identity_key NOT LIKE '%:%'"
    )
    conn.execute(
        "UPDATE relationship SET status = 'ended',"
        " updated_at = CAST(strftime('%s','now') AS INTEGER) * 1000"
        " WHERE counterpart_id IN (SELECT counterpart_id FROM counterpart WHERE identity_status = 'invalid')"
    )
    conn.execute(
        "UPDATE trust_evidence SET evidence_status = 'invalidated',"
        " updated_at = CAST(strftime('%s','now') AS INTEGER) * 1000"
        " WHERE relationship_id IN (SELECT relationship_id FROM relationship WHERE status = 'ended')"
    )


def _m0008_learning(conn):
    """P11：学习模块接入（V1.3.1 最小实现：证据 → 结论 → 建议）。"""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS proposed_conclusions ("
        " proposal_id TEXT PRIMARY KEY,"
        " workspace_id TEXT NOT NULL,"
        " statement TEXT NOT NULL,"
        " source_experience_id TEXT NOT NULL,"
        " status TEXT NOT NULL DEFAULT 'proposed'"
        "   CHECK (status IN ('proposed', 'accepted', 'rejected')),"
        " created_at INTEGER NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS conclusions ("
        " conclusion_id TEXT PRIMARY KEY,"
        " workspace_id TEXT NOT NULL,"
        " statement TEXT NOT NULL,"
        " status TEXT NOT NULL DEFAULT 'candidate'"
        "   CHECK (status IN ('candidate', 'active', 'suspended', 'invalid',"
        "                    'expired', 'superseded', 'split', 'merged')),"
        " created_at INTEGER NOT NULL,"
        " updated_at INTEGER NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS conclusion_evidence ("
        " evidence_id TEXT PRIMARY KEY,"
        " conclusion_id TEXT NOT NULL,"
        " experience_id TEXT NOT NULL,"
        " polarity TEXT NOT NULL CHECK (polarity IN ('support', 'against')),"
        " evidence_type TEXT,"
        " rationale TEXT,"
        " revoked_at INTEGER,"
        " created_at INTEGER NOT NULL,"
        " UNIQUE (conclusion_id, experience_id, polarity))"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS suggestions ("
        " suggestion_id TEXT PRIMARY KEY,"
        " target_module TEXT NOT NULL,"
        " suggestion_type TEXT NOT NULL,"
        " source_conclusion_id TEXT NOT NULL,"
        " payload TEXT NOT NULL,"
        " dedup_key TEXT NOT NULL UNIQUE,"
        " status TEXT NOT NULL DEFAULT 'pending'"
        "   CHECK (status IN ('pending', 'processing', 'accepted', 'rejected',"
        "                    'suspended', 'revoked', 'expired', 'failed')),"
        " created_at INTEGER NOT NULL,"
        " updated_at INTEGER NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS suggestion_conclusion_links ("
        " suggestion_id TEXT NOT NULL,"
        " conclusion_id TEXT NOT NULL,"
        " created_at INTEGER NOT NULL,"
        " PRIMARY KEY (suggestion_id, conclusion_id))"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS learning_evidence_queue ("
        " evidence_id TEXT PRIMARY KEY,"
        " workspace_id TEXT NOT NULL,"
        " canonical_experience_id TEXT NOT NULL,"
        " source_experience_id TEXT NOT NULL,"
        " source_status TEXT NOT NULL"
        "   CHECK (source_status IN ('uncertain', 'verified')),"
        " queue_status TEXT NOT NULL"
        "   CHECK (queue_status IN"
        "     ('awaiting_verification', 'pending_eval', 'retry_wait', 'evaluated', 'dead')),"
        " attempts INTEGER NOT NULL DEFAULT 0,"
        " next_retry_at INTEGER,"
        " last_error TEXT,"
        " proposal_ref TEXT,"
        " created_at INTEGER NOT NULL,"
        " updated_at INTEGER NOT NULL,"
        " UNIQUE (workspace_id, canonical_experience_id))"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS learning_queue_idx"
        " ON learning_evidence_queue(queue_status, next_retry_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS conclusion_evidence_idx"
        " ON conclusion_evidence(conclusion_id, polarity, revoked_at)"
    )


def _m0009_personality(conn):
    """P12：人格接入最小实现（Trait 基线 + 影响实例池 + 学习影响请求与确认）。"""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS personality_traits ("
        " trait_id TEXT PRIMARY KEY,"
        " name TEXT NOT NULL UNIQUE,"
        " value REAL NOT NULL DEFAULT 0.5 CHECK (value >= 0 AND value <= 1),"
        " direction INTEGER NOT NULL DEFAULT 0 CHECK (direction IN (-1, 0, 1)),"
        " stability REAL NOT NULL DEFAULT 0.5,"
        " resistance REAL NOT NULL DEFAULT 1.0,"
        " base_yield_threshold REAL NOT NULL DEFAULT 0.2,"
        " frozen INTEGER NOT NULL DEFAULT 0,"
        " boundary_reason TEXT,"
        " state_version INTEGER NOT NULL DEFAULT 0,"
        " created_at INTEGER NOT NULL,"
        " updated_at INTEGER NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS influence_instances ("
        " instance_id TEXT PRIMARY KEY,"
        " trait_id TEXT NOT NULL,"
        " direction INTEGER NOT NULL CHECK (direction IN (-1, 1)),"
        " raw_strength REAL NOT NULL,"
        " confidence REAL NOT NULL,"
        " source_weight REAL NOT NULL,"
        " strength REAL NOT NULL,"
        " remaining_amount REAL NOT NULL,"
        " settled_amount REAL NOT NULL DEFAULT 0,"
        " source_kind TEXT NOT NULL,"
        " source_refs TEXT,"
        " eligible_at INTEGER NOT NULL,"
        " status TEXT NOT NULL DEFAULT 'active'"
        "   CHECK (status IN ('active', 'partially_settled', 'settled', 'revoked', 'archived')),"
        " created_at INTEGER NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS learning_influence_requests ("
        " influence_request_id TEXT PRIMARY KEY,"
        " suggestion_id TEXT NOT NULL UNIQUE,"
        " conclusion_id TEXT NOT NULL,"
        " target_trait_id TEXT NOT NULL,"
        " direction INTEGER NOT NULL CHECK (direction IN (-1, 1)),"
        " raw_strength REAL NOT NULL,"
        " confidence REAL NOT NULL,"
        " source_weight REAL NOT NULL,"
        " status TEXT NOT NULL DEFAULT 'pending'"
        "   CHECK (status IN ('pending', 'confirming', 'eligible', 'active',"
        "                    'dropped', 'rejected', 'revoked', 'expired')),"
        " created_at INTEGER NOT NULL,"
        " window_expires_at INTEGER NOT NULL,"
        " activated_at INTEGER,"
        " updated_at INTEGER NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS learning_influence_confirmations ("
        " confirmation_id TEXT PRIMARY KEY,"
        " influence_request_id TEXT NOT NULL,"
        " experience_id TEXT NOT NULL,"
        " independence_key TEXT NOT NULL,"
        " occurred_at INTEGER NOT NULL,"
        " created_at INTEGER NOT NULL,"
        " revoked_at INTEGER,"
        " UNIQUE (influence_request_id, independence_key))"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS suggestion_consumption ("
        " suggestion_id TEXT NOT NULL,"
        " target_module TEXT NOT NULL,"
        " request_id TEXT,"
        " result TEXT,"
        " audit_id TEXT,"
        " processed_at INTEGER NOT NULL,"
        " PRIMARY KEY (suggestion_id, target_module))"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS influence_instances_pool_idx"
        " ON influence_instances(trait_id, status, direction)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS influence_requests_status_idx"
        " ON learning_influence_requests(status, window_expires_at)"
    )


def _m0010_behavior(conn):
    """P13：行为模块接入（当前状态 / 情境状态 / 观察日志）。"""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS behavior_current_state ("
        " id TEXT PRIMARY KEY,"
        " session_id TEXT NOT NULL,"
        " workspace_id TEXT NOT NULL,"
        " state_payload TEXT NOT NULL,"
        " revision INTEGER NOT NULL DEFAULT 0,"
        " lock_version INTEGER NOT NULL DEFAULT 0,"
        " created_at INTEGER NOT NULL,"
        " updated_at INTEGER NOT NULL,"
        " UNIQUE (workspace_id, session_id))"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS behavior_situational_state ("
        " id TEXT PRIMARY KEY,"
        " session_id TEXT NOT NULL,"
        " workspace_id TEXT NOT NULL,"
        " state_payload TEXT NOT NULL,"
        " revision INTEGER NOT NULL DEFAULT 0,"
        " lock_version INTEGER NOT NULL DEFAULT 0,"
        " status TEXT NOT NULL DEFAULT 'active'"
        "   CHECK (status IN ('active', 'expired', 'cancelled')),"
        " created_at INTEGER NOT NULL,"
        " expires_at INTEGER NOT NULL)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_behavior_situational_scope"
        " ON behavior_situational_state(workspace_id, session_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_behavior_situational_expiry"
        " ON behavior_situational_state(expires_at)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS behavior_observation_log ("
        " id TEXT PRIMARY KEY,"
        " request_id TEXT,"
        " session_id TEXT NOT NULL,"
        " workspace_id TEXT NOT NULL,"
        " observation_type TEXT NOT NULL,"
        " target_layer TEXT NOT NULL CHECK (target_layer IN ('current', 'situational')),"
        " observation_payload TEXT NOT NULL,"
        " result_status TEXT NOT NULL,"
        " created_at INTEGER NOT NULL)"
    )


def _m0011_emotion_state(conn):
    """P14-B：情绪状态（PAD_base / 检查点）。"""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS emotion_state ("
        " workspace_id TEXT PRIMARY KEY,"
        " pad_base TEXT NOT NULL,"
        " baseline_snapshot TEXT,"
        " config_version INTEGER NOT NULL DEFAULT 1,"
        " state_version INTEGER NOT NULL DEFAULT 0,"
        " last_advanced_at INTEGER,"
        " updated_at INTEGER NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS emotion_checkpoints ("
        " checkpoint_id TEXT PRIMARY KEY,"
        " workspace_id TEXT NOT NULL,"
        " pad_snapshot TEXT NOT NULL,"
        " advanced_to INTEGER NOT NULL,"
        " event_cursor TEXT,"
        " event_revision INTEGER,"
        " config_version INTEGER,"
        " step_size INTEGER,"
        " created_at INTEGER NOT NULL)"
    )


def _m0012_personality_settlement(conn):
    """P14-C：人格结算所需字段（方向交替检测）与结算记录。"""
    conn.execute("ALTER TABLE personality_traits ADD COLUMN last_settled_direction INTEGER")
    conn.execute("ALTER TABLE personality_traits ADD COLUMN last_settled_at INTEGER")
    conn.execute("ALTER TABLE personality_traits ADD COLUMN settlement_hold INTEGER NOT NULL DEFAULT 0")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS personality_settlements ("
        " settlement_id TEXT PRIMARY KEY,"
        " trait_id TEXT NOT NULL,"
        " trigger TEXT NOT NULL,"
        " old_value REAL NOT NULL,"
        " new_value REAL NOT NULL,"
        " actual_change REAL NOT NULL,"
        " selected_direction INTEGER,"
        " positive_pressure REAL NOT NULL,"
        " negative_pressure REAL NOT NULL,"
        " threshold REAL NOT NULL,"
        " resistance REAL NOT NULL,"
        " release REAL NOT NULL,"
        " instance_allocations TEXT,"
        " config_version INTEGER,"
        " created_at INTEGER NOT NULL)"
    )


def _m0013_daily_cycle(conn):
    """P14-D2：每日整理任务账本（周期 + 阶段 + Workspace 明细）。"""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS daily_cycle ("
        " cycle_id TEXT PRIMARY KEY,"
        " run_id TEXT NOT NULL UNIQUE,"
        " timezone TEXT NOT NULL,"
        " schedule_date TEXT NOT NULL,"
        " scheduled_at INTEGER NOT NULL,"
        " started_at INTEGER,"
        " completed_at INTEGER,"
        " status TEXT NOT NULL DEFAULT 'pending'"
        "   CHECK (status IN ('pending', 'running', 'completed', 'failed', 'recovery_pending')),"
        " current_step TEXT,"
        " last_error TEXT,"
        " created_at INTEGER NOT NULL,"
        " updated_at INTEGER NOT NULL,"
        " UNIQUE (timezone, schedule_date))"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS daily_cycle_steps ("
        " cycle_id TEXT NOT NULL,"
        " step_name TEXT NOT NULL,"
        " step_order INTEGER NOT NULL,"
        " status TEXT NOT NULL DEFAULT 'pending'"
        "   CHECK (status IN ('pending', 'running', 'completed', 'failed', 'skipped')),"
        " attempts INTEGER NOT NULL DEFAULT 0,"
        " last_error TEXT,"
        " started_at INTEGER,"
        " completed_at INTEGER,"
        " PRIMARY KEY (cycle_id, step_name))"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS daily_cycle_workspace_steps ("
        " cycle_id TEXT NOT NULL,"
        " step_name TEXT NOT NULL,"
        " workspace_id TEXT NOT NULL,"
        " status TEXT NOT NULL DEFAULT 'pending'"
        "   CHECK (status IN ('pending', 'running', 'completed', 'failed', 'skipped')),"
        " attempts INTEGER NOT NULL DEFAULT 0,"
        " last_error TEXT,"
        " updated_at INTEGER NOT NULL,"
        " PRIMARY KEY (cycle_id, step_name, workspace_id))"
    )


def _m0014_pressure_decay(conn):
    """P14-E2：人格压力衰减（实例版本 + 衰减状态 + 只增不改的事件账本）。"""
    conn.execute("ALTER TABLE influence_instances ADD COLUMN lock_version INTEGER NOT NULL DEFAULT 0")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS personality_pressure_decay_state ("
        " instance_id TEXT PRIMARY KEY,"
        " anchor_at INTEGER NOT NULL,"
        " last_decay_at INTEGER NOT NULL,"
        " revision INTEGER NOT NULL DEFAULT 0,"
        " created_at INTEGER NOT NULL,"
        " updated_at INTEGER NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS personality_pressure_decay_events ("
        " event_id TEXT PRIMARY KEY,"
        " instance_id TEXT NOT NULL,"
        " request_id TEXT,"
        " cycle_id TEXT,"
        " decay_before REAL NOT NULL,"
        " decay_amount REAL NOT NULL,"
        " decay_after REAL NOT NULL,"
        " anchor_at INTEGER,"
        " last_decay_at INTEGER,"
        " applied_at INTEGER NOT NULL,"
        " active_days REAL,"
        " half_life_days REAL,"
        " grace_period_days REAL,"
        " config_version TEXT,"
        " lock_version_before INTEGER,"
        " lock_version_after INTEGER,"
        " reason TEXT,"
        " UNIQUE (instance_id, request_id))"
    )


def _m0015_outbox_reconciliation(conn):
    """P14-E3：Outbox 对账（任务 + 异常）。只读业务数据，不修改投递状态。"""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS outbox_reconciliation_runs ("
        " run_id TEXT PRIMARY KEY,"
        " request_id TEXT,"
        " run_type TEXT NOT NULL CHECK (run_type IN ('incremental', 'full')),"
        " status TEXT NOT NULL CHECK (status IN ('running', 'completed', 'failed')),"
        " started_at INTEGER NOT NULL,"
        " completed_at INTEGER,"
        " checked_events INTEGER NOT NULL DEFAULT 0,"
        " checked_consumers INTEGER NOT NULL DEFAULT 0,"
        " issue_count INTEGER NOT NULL DEFAULT 0,"
        " p0_count INTEGER NOT NULL DEFAULT 0,"
        " p1_count INTEGER NOT NULL DEFAULT 0,"
        " p2_count INTEGER NOT NULL DEFAULT 0,"
        " info_count INTEGER NOT NULL DEFAULT 0,"
        " new_issues INTEGER NOT NULL DEFAULT 0,"
        " recurring_issues INTEGER NOT NULL DEFAULT 0,"
        " resolved_issues INTEGER NOT NULL DEFAULT 0,"
        " cursor_before TEXT,"
        " cursor_after TEXT,"
        " error TEXT)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS outbox_reconciliation_issues ("
        " issue_id TEXT PRIMARY KEY,"
        " run_id TEXT,"
        " fingerprint TEXT NOT NULL UNIQUE,"
        " issue_type TEXT NOT NULL,"
        " severity TEXT NOT NULL CHECK (severity IN ('P0', 'P1', 'P2', 'INFO')),"
        " event_id TEXT,"
        " consumer_module TEXT,"
        " aggregate_id TEXT,"
        " description TEXT NOT NULL,"
        " evidence TEXT,"
        " detected_at INTEGER NOT NULL,"
        " last_seen_at INTEGER,"
        " issue_status TEXT NOT NULL DEFAULT 'open'"
        "   CHECK (issue_status IN ('open', 'acknowledged', 'resolved', 'ignored')),"
        " resolved_at INTEGER,"
        " resolution_ref TEXT)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS reconciliation_issues_status_idx"
        " ON outbox_reconciliation_issues(issue_status, severity)"
    )


def _m0016_experience_changes(conn):
    """MEM-CHANGE-E4-01 V1.2：Experience 修正/合并/拆分（变更版本 + 操作记录 + 拆分映射）。"""
    conn.execute("ALTER TABLE memory_experience ADD COLUMN change_version INTEGER NOT NULL DEFAULT 0")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS experience_changes ("
        " operation_id TEXT PRIMARY KEY,"
        " workspace_id TEXT NOT NULL,"
        " operation_type TEXT NOT NULL CHECK (operation_type IN ('revise', 'merge', 'split')),"
        " request_id TEXT,"
        " source_experience_ids TEXT NOT NULL,"
        " target_experience_ids TEXT NOT NULL,"
        " version_snapshot TEXT,"
        " mapping_version INTEGER NOT NULL DEFAULT 0,"
        " status TEXT NOT NULL DEFAULT 'completed' CHECK (status IN ('completed')),"
        " actor TEXT,"
        " created_at INTEGER NOT NULL)"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS experience_split_mappings ("
        " mapping_id TEXT PRIMARY KEY,"
        " operation_id TEXT NOT NULL,"
        " source_experience_id TEXT NOT NULL,"
        " target_experience_id TEXT NOT NULL,"
        " event_id TEXT NOT NULL,"
        " mapping_version INTEGER NOT NULL DEFAULT 0,"
        " created_at INTEGER NOT NULL,"
        " UNIQUE (operation_id, event_id))"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS split_mapping_source_idx"
        " ON experience_split_mappings(source_experience_id, operation_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS split_mapping_target_idx"
        " ON experience_split_mappings(target_experience_id, operation_id)"
    )


def _m0017_experience_consumers(conn):
    """E4-C1：Experience 变更消费基础设施（操作级状态 + 待复核队列）。"""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS experience_change_consumer_operations ("
        " operation_id TEXT NOT NULL,"
        " consumer_module TEXT NOT NULL,"
        " operation_type TEXT NOT NULL CHECK (operation_type IN ('revise', 'merge', 'split')),"
        " mapping_version INTEGER NOT NULL DEFAULT 0,"
        " consumer_status TEXT NOT NULL DEFAULT 'pending'"
        "   CHECK (consumer_status IN ('pending', 'processing', 'completed', 'failed', 'partial')),"
        " started_at INTEGER,"
        " completed_at INTEGER,"
        " last_error TEXT,"
        " attempt_count INTEGER NOT NULL DEFAULT 0,"
        " PRIMARY KEY (operation_id, consumer_module))"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS evidence_review_queue ("
        " review_id TEXT PRIMARY KEY,"
        " workspace_id TEXT NOT NULL,"
        " operation_id TEXT,"
        " consumer_module TEXT NOT NULL,"
        " evidence_ref TEXT NOT NULL,"
        " source_experience_id TEXT,"
        " target_experience_ids TEXT,"
        " source_event_id TEXT,"
        " review_reason TEXT NOT NULL"
        "   CHECK (review_reason IN ('split_without_event_trace', 'ambiguous_event_mapping',"
        "     'multi_event_cross_split', 'invalid_experience_reference',"
        "     'canonical_resolution_error', 'revised_evidence_uncertain',"
        "     'consumer_validation_failed')),"
        " review_status TEXT NOT NULL DEFAULT 'pending'"
        "   CHECK (review_status IN ('pending', 'processing', 'resolved', 'rejected', 'ignored')),"
        " created_at INTEGER NOT NULL,"
        " updated_at INTEGER NOT NULL,"
        " resolved_at INTEGER,"
        " resolution_ref TEXT,"
        " UNIQUE (operation_id, consumer_module, evidence_ref))"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS evidence_review_status_idx"
        " ON evidence_review_queue(review_status, review_reason)"
    )


MIGRATIONS = [
    (1, "initial infrastructure schema", _m0001_initial),
    (2, "memory experience and event links", _m0002_memory),
    (3, "memory workspace+session index", _m0003_session_index),
    (4, "outbox subscriptions and dead letters", _m0004_dispatcher),
    (5, "emotion evidence queue and events", _m0005_emotion),
    (6, "relationship evidence and trust", _m0006_relationship),
    (7, "identity constraints and candidate column", _m0007_identity_constraints),
    (8, "learning evidence, conclusions, suggestions", _m0008_learning),
    (9, "personality influence and confirmations", _m0009_personality),
    (10, "behavior state and observations", _m0010_behavior),
    (11, "emotion PAD state and checkpoints", _m0011_emotion_state),
    (12, "personality settlement fields and records", _m0012_personality_settlement),
    (13, "daily cycle ledger", _m0013_daily_cycle),
    (14, "personality pressure decay state and events", _m0014_pressure_decay),
    (15, "outbox reconciliation runs and issues", _m0015_outbox_reconciliation),
    (16, "experience changes and split mappings", _m0016_experience_changes),
    (17, "experience change consumer operations and review queue", _m0017_experience_consumers),
]
