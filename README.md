# EIDOS 状态引擎（dsh-engine）

**EIDOS = Evolving Identity & Development Operating System**（原 DSH，2026-10-31 更名）。

DSH 状态引擎 —— Cordis 宿主插件 + Python 状态引擎，已从 P1 骨架发展为完整的经历驱动系统。

核心链路：经历 → 核验 → 情绪 / 关系 / 学习 / 人格 / 行为 → 每日协调与对账 → Experience 修正/合并/拆分全链路。

> 代码标识符（包名 `dsh-engine`、目录、环境变量、数据库路径）暂未改名，对外文案统一为 EIDOS。

## 快速开始

**前置**：Cordis 宿主（DeepSeek Harness 桌面版）、Node.js ≥ 20、Python 3.12。

**安装**（link 到宿主 profile）：

```bash
cd /path/to/dsh-engine
node scripts/build.mjs
# 在宿主 profile 的 plugins 目录里创建指向本仓库的链接，
# 或在宿主插件设置里加载本目录（cordis.patch.yml 已定义插件入口）。
```

**配置项**（插件 config）：

| 键 | 默认 | 说明 |
|---|---|---|
| `dataDir` | `~/.dsh/dsh-engine` | SQLite 数据目录 |
| `pythonPath` | 自动探测 | Python 3.12 路径 |
| `verifyEnabled` | `true` | 是否启用经历核验 |
| `verifyIntervalHours` | `24` | 核验扫描间隔 |
| `evaluationModel` | `deepseek-account/deepseek-flash` | 模型路由 |
| `sendRawEvidence` | `true` | 是否向模型发送脱敏证据 |
| `dailyCycleTimeZone` | `Asia/Shanghai` | 每日整理链时区（03:00 触发） |
| `dispatchIntervalSeconds` | `5` | Outbox 投递间隔 |
| `testConsumer` | `false` | 测试消费者（调试用） |

**数据**：`<dataDir>/dsh_engine.sqlite`（WAL），首次启动自动建库并迁移。

**开发**：

```bash
node scripts/build.mjs      # 构建 lib/
bash tests/run-all.sh       # 全量回归（Node + Python）
```

---

## 架构

## 目录

```
dsh-engine/
├── package.json          # 插件包元数据（main → lib/index.js，dsh.bundle.patch）
├── cordis.patch.yml      # 插件行：id=dsh-engine，config: dataDir/pythonPath
├── icon.svg / locale/    # 插件管理器显示元数据
├── src/                  # 源码（ESM，无外部依赖）
│   ├── index.js          # 插件入口：apply(ctx, config)，effect 托管生命周期
│   ├── engine-process.js # 子进程 + NDJSON 请求/响应对 + ready + 超时 + 退出
│   ├── protocol.js       # 行编解码、非法 JSON 隔离、request_id 生成
│   └── runtime.js        # Python 探测：配置 → DSH 自带运行时 → 系统 Python
├── lib/                  # 构建产物（node scripts/build.mjs 生成，宿主加载这里）
├── engine/python/main.py # 引擎骨架：ready/ping/get_status/slow_ping/shutdown
├── scripts/build.mjs     # 构建：src/ → lib/
└── tests/
    ├── smoke.mjs         # 端到端：探测 → 启动 → 握手 → 通信 → 超时 → 关闭
    └── plugin-load.mjs   # 插件 apply 契约：导出形态、effect、dispose
```

## 构建

```bash
node scripts/build.mjs
```

把 `src/` 逐文件复制到 `lib/`。宿主实际加载的是 `lib/index.js`，所以每次改完 `src/` 都要重新构建。

## 测试

```bash
node tests/smoke.mjs        # 5 项：ready / ping / status / 非法JSON / 超时 / 关闭
node tests/plugin-load.mjs  # 插件 apply 契约
```

冒烟测试不依赖 DSH 宿主，直接驱动 `lib/`；Python 优先使用 DSH 自带运行时（`$DSH_HOME/dsh-runtimes/*/dependencies/python`）。

## 安装到 DSH

本插件是 Cordis bundle，通过宿主 `plugin_manager` 安装：

1. 在 DSH 会话中调用 `plugin_manager`，`action: install_bundle`，`target` 指向本目录绝对路径；
2. 安装后确认插件行 `dsh-engine` 为启用状态；
3. 插件启动时会自动探测 Python、拉起子进程并完成 ready 握手。

> 注意：当前开发环境没有 `plugin_manager` 工具，安装步骤尚未在真实 profile 中验证。

## 配置

`cordis.patch.yml` 中的 `config`：

| 字段 | 默认 | 说明 |
|---|---|---|
| `dataDir` | 空 | 数据目录，空则 `$DSH_HOME/dsh-engine` |
| `pythonPath` | 空 | 显式 Python 路径，空则自动探测 |
| `verifyEnabled` | true | 是否启用模型辅助核验 |
| `verifyIntervalHours` | 24 | 核验扫描间隔（小时，最小 1） |
| `sendRawEvidence` | true | P7-B：是否把原始用户消息（脱敏+限长）送入模型；隐私已确认（2026-10-31） |
| `verifyOnStartupDelaySeconds` | 60 | 启动后首次扫描延迟（秒，最小 5） |
| `evaluationModel` | `deepseek-account/deepseek-flash` | 评估模型路由 `provider/model` |
| `verifyTimeoutMs` | 60000 | 单条评估超时 |
| `dispatchIntervalSeconds` | 5 | P8 投递间隔（秒，最小 3） |
| `testConsumer` | false | P8-C 验证用 echo-consumer，默认关闭 |

## P6 模型辅助核验（已实现）

- 调度在 Node：定时 `memory_list_pending` → `memory_get_experience`（含事件关联）→ `ctx.llm.stream` 调模型 → 严格校验输出 → `memory_verify_experience`。
- 模型只给结论（verified/uncertain/invalid），不写库；Python 仍是唯一写入口。
- 防过期：评估携带读取时的 `verification_status`，写入时校验，状态已变则拒绝（stale evaluation）。
- 单条失败/输出非法/模型超时：跳过该条，保持原状态，不影响其他记录。
- 幂等：同一经历 + 同一读取状态的核验 request_id 固定，重跑不重复发事件。
- 提示词：`prompts/verify-experience.md`（随构建进入 `lib/prompts/`）。

## P3 基础设施（已实现）

- **SQLite**：`$DSH_HOME/dsh-engine/dsh_engine.sqlite`，WAL、外键、busy_timeout、单写者。
- **Migration**：版本化（`schema_migrations`），每个迁移独立事务，失败回滚不记录；当前 `v1`。
- **request_dedup**：`(request_id, module_id)` 唯一；同参重放返回原结果、异参冲突、模块隔离；业务写入与幂等记录同事务。
- **audit_records**：只增不改（数据库触发器拒绝 UPDATE/DELETE）；记录操作主体/对象/前后快照/request_id/logical_version。
- **outbox_events**：`pending → published → consumed`，`failed` 可重试；`event_consumer_records` 按 `(event_id, consumer_module)` 独立记录，全部消费者完成才整体 consumed。
- 协议新增：`db_status`（库路径/版本/表/迁移/完整性）与 `self_test`（幂等/Audit/Outbox 自检）。

## P4 记忆模块（已实现，最小链路）

- `memory_experience` / `memory_experience_event_links`（migration v2）。
- `memory_create_experience`：pending 状态、canonical 初始指向自身、independence_key 占位、request_dedup 幂等、Audit、Outbox `experience.created` 同事务写入。
- 去重：完全相同的 source 事件集合返回已有经历；部分重叠返回 `possible_duplicate`，不自动合并。
- `memory_summary`：按 workspace/session 的真实统计（pending/verified/uncertain/invalid）。
- pre-step 改为真实链路：本轮用户消息 → 候选 Experience → 真实记忆摘要注入。
- `session_id`（migration v2/v3，已确认的工程元数据扩展）：
  - 只记录创建上下文，**不是**当前会话归属，创建后不改；
  - `workspace_id` 仍是权威归属；`session_id` 不参与核验/合并/拆分/去重/独立来源/人格影响；
  - 统计必须 workspace + session 双重限定；复合索引 `memory_experience_ws_session_idx`；
  - 历史记录允许 NULL，不推测回填；
  - 已加入幂等前置：`session_id` 不在幂等 payload 中，同一事件集合跨会话重放返回同一经历。

## P5 记忆核验（已实现，创建—核验闭环）

- `memory_verify_experience`：`pending → verified / uncertain / invalid`，`uncertain → verified / invalid`；`verified` 后不能再转 `uncertain`；`invalid` 终态。
- 每次转换：状态更新 + `experience.verify` 审计 + Outbox（`experience.verified` / `experience.uncertain` / `experience.invalidated`）同事务。
- 幂等：同 request_id 重放返回原结果；同状态重复核验返回 `already: true`，不重复发事件。
- `memory_list_pending`：按 workspace 列出 pending/uncertain，供每日核验使用。
- P5 范围外：每日调度与自动核验结论（模型评估接入在 P6）。

## E4-D/E/F 消费链路（已实现，2026-10-31 真实宿主验证通过，E4 封版）

- Merge：`experience.merged` → 四消费者；操作级覆盖全部 source；同事务；部分失败重试恢复。
- Split：情绪按事件映射迁移、关系 `relocate_for_split`、学习/人格入待复核。
- Revised：最小消费（情绪/关系 noop、学习/人格入待复核、已激活确认不入队）。
- 真实宿主：merge 四模块 completed、事件 consumed。
- 协议：`change_consumer_begin/complete/fail`、`change_operation_status`、`review_enqueue/resolve/list`。

## E4-C1 消费基础设施（已实现，2026-10-31 真实库验证通过）

- migration v17：`experience_change_consumer_operations`、`evidence_review_queue`。
- `ExperienceChangeConsumer`：begin（操作级幂等 + mapping_version 冲突）/ complete / fail / operation_status；review enqueue（去重）/ resolve（actor+reason，resolved 需 ref）/ list。
- 真实库：schema 17；begin→complete→completed；review 入队 pending。

## MEM-CHANGE-E4-01 Experience 变更（已实现，2026-10-31 真实库验证通过）

- migration v16：`memory_experience.change_version`、`experience_changes`、`experience_split_mappings`。
- 操作：`memory_revise`（白名单/版本/别名拒绝）、`memory_merge`（权威/防环/多级链）、`memory_split`（事件级重分配/终态/映射）。
- `memory_resolve_canonical`：ok / split_terminal / invalid_terminal / broken_chain / cross_workspace / cycle_detected / corrupt_chain。
- 读取：`memory_change_get`、`memory_split_mapping_get`（workspace 校验）。
- 真实库：schema 16，canonical 解析 ok。

## P14-E3 Outbox 对账（已实现，2026-10-31 真实库首轮全量通过）

- migration v15：`outbox_reconciliation_runs`、`outbox_reconciliation_issues`（指纹唯一）。
- `reconcile_outbox`：六类检查 + 四级分级 + 指纹去重 + resolved 复核；不修改 Outbox/消费者/业务数据。
- 历史无订阅快照 → `subscription_unknown`（INFO）；死信用 `dead_letter_at` 判定。
- 每日链第七阶段接入增量对账；全量/手动独立运行。
- 真实库：117 事件 → 29 条 `subscription_unknown`（INFO），P0/P1/P2=0。

## P14-E2 人格压力衰减（已实现，2026-10-31 真实库验证通过）

- migration v14：`influence_instances.lock_version`、`personality_pressure_decay_state`、`personality_pressure_decay_events`（只增不改，UNIQUE(instance_id, request_id)）。
- `personality_decay`（`decay_personality_pressure`）：指数衰减（宽限 30 天、半衰期 180 天、epsilon 1e-6），增量计算，锁版本校验，单事务。
- 新证据刷新：同结论实例锚点按接受时间重置（只重置未来，不恢复已衰减压力），刷新记录含 evidence_id。
- 每日链：人格结算阶段内先衰减后结算，两个稳定 request_id；结算写入路径维护 lock_version。
- 真实库：schema 14，实例宽限期内 → `no_eligible_instances`。

## P14-E1 停机恢复（已实现，2026-10-31 真实宿主验证通过）

- 情绪补算：按 1h 规范步长分段推进（72 步验证），重复调用 `already_advanced`。
- 人格补算：每日周期结算稳定 request_id，重放不重复消耗。
- Daily Cycle 恢复：启动扫 recoverable，非今天未完成周期标 `failed`（`stale_cycle_after_crash`），今天恢复补跑；阶段 attempts 保留、已完成不重置、双认领拒绝。
- 真机：正常停机、阶段中断、多次中断三组验证通过。

## P14-C 人格结算（已实现，2026-10-31 真实库验证通过）

- migration v12：方向/暂缓字段、`personality_settlements` 结算记录。
- `personality_settle`（`settle`）：正负压力分别判断 → 屈服 → 边界优先 → 分摊 → 记账；方向交替/双方向模糊暂缓。
- 真实库：压力 0.025 < 阈值 0.2 → `below_threshold`，trait 不变。
- 参数（max_change 0.05 / scale 0.5 / base_yield_threshold 0.2）待 P15 校准；衰减与停机补算留 P14-E。

## P14-B 情绪推进（已实现，2026-10-31 真实宿主验证通过）

- migration v11：`emotion_state`（PAD_base / 基线快照 / 版本）、`emotion_checkpoints`。
- `emotion_advance`（`advance_emotion_state`）：V1.3 基线锚定质心 + 时间常数低通，分段推进，唯一 PAD 写入路径，幂等。
- `emotion_state_summary`（`get_emotion_summary`）：PAD_render（钟点因子）+ 心情投影。
- 行为模块接通情绪输入：PAD 偏正/偏负 → `emotional_response` 软指导。
- 真实效果：宿主行为指导出现「当前情绪偏正向，回应可以更自然、更有温度」，降级消失。
- 参数（基线权重 2.0 / 事件 24h / 低通 6h / 步长 1h / 阈值 0.01）待 P15 校准。

## P13 行为接入（已实现，2026-10-31 真实宿主验证通过）

- migration v10：`behavior_current_state`（session 级）、`behavior_situational_state`（TTL）、`behavior_observation_log`。
- 接口：`behavior_submit`（`submit_behavior_observation`，幂等/层校验）、`behavior_guidance`（`get_behavior_guidance`）。
- Trait 映射：openness/conscientiousness/extraversion，>=0.65 高倾向 / <=0.35 低倾向 / 中间不生成；未映射 trait 不标 degraded。
- `hard_constraints_ref` 为空 + `hard_constraints_note`（不代表不存在硬约束，宿主守卫独立执行）。
- pre-step 复用 P2 接入点注入 ≤300 tokens 软指导；行为模块异常回退原 decision。
- 降级：情绪摘要暂不可用（P9 未实现 PAD 摘要）→ degraded + confidence ≤ 0.5，P14 接通。

## P12 人格接入（已实现，2026-10-31 真实库验证通过）

- migration v9：`personality_traits`、`influence_instances`（实例池）、`learning_influence_requests`、`learning_influence_confirmations`、`suggestion_consumption`。
- 链路：`learning.suggestion.created` 事件 → 人格领取（`suggestion_consumption` 幂等）→ request(pending) → `confirm_learning_influence` ×2 → `activate` → 实例（`source_kind=learning_suggestion`）进池。
- 确认：独立 verified 经历、`independence_key` 由记忆模块核验提供、来源证据不复用；窗口 30 天、阈值 2 条；超期 `dropped`；激活前撤销重算；激活后失效不回滚。
- 不触发结算（P14）；不修改 Trait/实例池/结算规则。
- 协议：`personality_confirm` / `personality_request_get` / `personality_request_create` / `personality_summary`。

## P11 学习接入（已实现，2026-10-31 真实宿主验证通过）

- 记忆模块补 `memory_experience_check`（只读核验：workspace 隔离、排除 invalid/archived/split、来源事件校验、返回系统 independence_key）。
- migration v8：`proposed_conclusions`（候选，不参与评估）、`conclusions`、`conclusion_evidence`、`suggestions`、`suggestion_conclusion_links`、`learning_evidence_queue`。
- 接入链路：verified → 学习队列 → Node 模型（匹配已有结论或提出候选）→ Python 学习模块（证据准入核验 → 评估 → 建议 pending）。
- 建议阈值：至少 2 条独立 verified 支持、且支持 > 反驳；`pending` 进 Suggestions 队列，不接人格消费（P12）。
- 候选结论：模型提 `proposed_conclusion`，只记录、不晋升、不生成建议（P11 不做聚类/晋升）。
- 协议：`learning_submit_evidence` / `learning_queue_pending|fail|workspace` / `learning_conclusion_create|get|list` / `learning_suggestion_pending|revoke`。

## P10 关系接入（已实现，2026-10-31 真实宿主验证通过）

- migration v6：`counterpart`、`relationship`、`trust_evidence`、`trust_evidence_source_events`（不可变触发器）、`relationship_pending_evidence`、`system_identity`。
- 接入链路：verified → 关系队列 → Node 模型（对象候选 + trust_signal + 权重等级候选）→ Python 关系模块（counterpart 解析/创建 → 证据提交 → Trust 重算）。
- 证据身份：`evidence_key = sha256(规范化 JSON{workspace_id, relationship_id, event_slot, source_event_ids})`；canonical 仅当前关联；来源事件集合冻结（触发器拒绝变更）。
- 三态证据状态：`active`（参与聚合）/ `invalidated` / `pending_reassessment`。
- 权重：R2 §三 映射（weak 0.25 / medium 0.5 / strong 0.75 / critical 1.0）。
- Trust：R2 §三 公式；merge/split 完整事件集合归属判定；invalidated 失效并重算。
- 协议：`relationship_submit_from_eval` / `relationship_queue_pending|fail|workspace` / `relationship_get_trust` / `relationship_evidence_get`。
- 限定：只接受可明确解析的 `counterpart` 身份（用户自身 Principal 不当 Counterpart）；**身份键必须是 `namespace:key` 稳定标识**，`display_name` 仅展示、不得充当身份键；缺稳定身份时只保留待解析候选（队列 `candidate` 列）。
- 身份约束补丁（2026-10-31）：重名不合并；改名只更新 display_name；显示名历史样本已 invalidated 不纳入 Trust。merge/split 消费者已注册，生产操作未实现。

## P9 情绪接入（已实现，2026-10-31 真实宿主验证通过）

- 接入层队列 `emotion_pending_evidence`（migration v5）：`awaiting_verification → pending_eval → retry_wait/evaluated/dead`。
- 消费者 `emotion-evidence` 订阅 `experience.verified` / `experience.uncertain`：verified 入队评估，uncertain 只等待核验（不提交情绪事件）。
- Node 评估：四分类（positive/negative/mixed/neutral）+ confidence + PAD 候选 → `emotion_submit`；模型调用只在 Node。
- Python 情绪模块：`submit_emotion_event` / `revise_emotion_event` 最小实现（V1.3 契约，幂等 + 审计）。
- 防重复：接入层 `(workspace_id, canonical_experience_id)` 唯一；event_type 变化走 `revise` 不新建。
- 模型失败：队列退避重试（30s/2min/10min），3 次后 dead；不提交不完整事件。
- 协议：`emotion_submit` / `emotion_queue_pending` / `emotion_queue_fail` / `emotion_get` / `emotion_queue_workspace`。
- 不做：`advance_emotion_state`（留 P14）、offset、心情投影、rebuild。

## P8 事件分发（已实现，2026-10-31 真实宿主验证通过）

- 订阅表 `event_subscriptions`（migration v4）：注册/启停；**事件创建时快照订阅者**，历史事件不追。
- Dispatcher（Python 单线程）：Node 每 5s 调 `outbox_dispatch`；禁止重叠。
- 顺序：同一 aggregate 严格顺序（前一事件未完成，后续等待）；跨 aggregate 交错；不并发。
- 多消费者独立：一个消费者失败不影响其他；失败退避 30s / 2min / 10min，3 次后进死信，不自动重试。
- 审计：每次实际消费尝试（成功/失败）写 `audit_records`；跳过与空转不写。
- 历史回放：`outbox_replay` 必须提供 actor / reason 并写审计；不自动追历史。
- 事务边界：消费者业务副作用与 consumed 记录同事务（同库内）。
- 协议：`outbox_dispatch` / `outbox_status` / `outbox_replay`。
- 测试消费者：`echo-consumer`（`testConsumer` 配置开启，仅验证用）。

## P7-B 核验上下文增强（已实现，2026-10-31 真实接入验证通过）

- `collectEvidence`：经 `ctx.get('sessionPersistence')` 只读打开会话，按 event_ids 取 `user/message` 事件正文（`open(id,'read')` → `handle.read()` → `handle.close()`）。
- 只读关联用户消息；assistant/tool/system 过滤；workspace 归属校验（`header.cwd == experience.workspace_id`，不匹配拒绝）。
- 脱敏：私钥块、`sk-`/`ghp_`/`AKIA`、Bearer、password/token/secret 赋值。
- 限长：单条 2000、单次 8000；只在本批次内存使用，不落库；trace 只记 ID/长度/结果，不记原文。
- 任何失败（无 session_id、会话缺失、打开失败、workspace 不匹配、脱敏后为空）→ 回退 description-only，不改变 Experience 状态。
- 隐私边界：用户已授权（2026-10-31），`sendRawEvidence` 默认开启。
- 真实效果：描述截断导致的错判（P4 验收条目 uncertain）在读取原文后判为 verified。

## P6 模型辅助核验（已实现，封版 2026-10-31）

- 调度在 Node：定时 `memory_list_pending` → `memory_get_experience`（含事件关联）→ 调模型 → 严格校验输出 → `memory_verify_experience`。
- 模型只给结论（verified/uncertain/invalid），不写库；Python 仍是唯一写入口。
- 防过期：评估携带读取时的 `verification_status`，写入时校验，状态已变则拒绝（stale evaluation）。
- 单条失败/输出非法/模型超时：跳过该条，保持原状态，不影响其他记录。
- 幂等：同一经历 + 同一读取状态的核验 request_id 固定，重跑不重复发事件。
- 提示词：`prompts/verify-experience.md`（随构建进入 `lib/prompts/`）。
- **宿主模型接入方式**：`ctx.get('llm')`。`export const inject = ['llm']` 在宿主中未生效（ctx.llm 不可用），`ctx.get` 是 cordis 提供的免 inject 读取接口。
- **真实调用记录**（2026-10-31）：5/5 成功，模型 `deepseek-account/deepseek-flash`，输入 2550 / 输出 1095 / 合计 4157 tokens，约 9s；唯一有实质内容的经历判 verified，4 条测试语判 uncertain。
- **已知限制（P7 待讨论）**：评估输入只有 `description`（创建时截断 200 字），模型无法读取原始事件正文，核验偏保守。方向：经原始事件读取接口取上下文，不复制证据、不改状态机；需先确认事件权威存储与读取权限。
- **策略**：证据不足保留 uncertain，不允许为提高 verified 比例降低标准。

## 测试

```bash
node tests/smoke.mjs        # 桥接层 6 项
node tests/prestep.mjs      # P4 pre-step 真实记忆链路 7 项
node tests/plugin-load.mjs  # 插件契约 4 项
node tests/p3.mjs           # 基础设施协议级 8 项（含重启持久化）
node tests/p4.mjs           # 记忆模块协议级 8 项
node tests/p5.mjs           # 核验闭环 9 项（含重启）
python engine/python/tests/test_infra.py   # 基础设施 12 项（含事务回滚）
python engine/python/tests/test_memory.py  # 记忆模块 19 项（含核验）
bash tests/run-all.sh       # 全量回归
```

## 已知限制

1. **未接入 `ctx.subprocess`**：现用 `node:child_process`，功能已验证；宿主 API 待实测替换。
2. **Outbox 尚无后台投递器**：`publish/consume` 是接口，真实投递循环未实现（P4 接入模块时补）。
3. **消费者订阅表未实现**：`expected_consumers` 由生产方在 append 时显式登记，尚未做 event_type → consumers 的注册表。
4. **日志分级未完成**：stdout 保持纯协议、诊断走 stderr；文件级运行/错误日志分级延后。
5. **数据库备份/恢复未实现**（工程方案有，P3 范围未包含）。
