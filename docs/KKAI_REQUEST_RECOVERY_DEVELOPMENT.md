# KKAI 请求恢复开发规格

> 2026-11-05 修正：流式工具/JSON 交付、首轮 600 秒和 NewAPI 930 秒整流截止的旧要求，已由 [流式修正开发规格](KKAI_REQUEST_RECOVERY_STREAMING_CORRECTION_DEVELOPMENT.md) 覆盖。本文件其余部分保留为既有实现与验证记录；后续开发以修正规格为准。

- 日期：2026-11-04
- 状态：三角色审查、本轮本地实现与定向验证已完成，已发现的代码阻断全部修复；源码锚点以文档基线为准，真实客户端兼容性、性能与生产效果尚未验证。
- 上位决策：[最终方案](KKAI_REQUEST_RECOVERY_PLAN.md)。本文不得改变其中 R01–R14、真实流式或 NewAPI/Sub2 职责划分。
- Sub2 基线：`release/v0.2.18-kkai` / `e760d4c5722c5ec0922750a85e17b31ff04c4a75`。
- NewAPI 对接基线：`production/kkrich` / `17b0445bf4bb201db7513cf6efcf7929e1cd527c`。这些是本地源码版本，不代表当前生产版本。
- 范围：受管 NewAPI → Sub2 OpenAI HTTP Responses/Chat；`stream=true/false`、普通文本、工具、opaque reasoning、严格结构各有单独能力门。原生 WS、其他入口不自动继承。
- 当前已按用户授权完成本轮本地代码实施；受管开关默认关闭，未提交、生产构建、部署或配置启用。

## 1. 实施包与依赖

| 包 | 子系统 | 必须先完成的契约 | 交付验收 |
| --- | --- | --- | --- |
| A | Sub2 统一恢复协调 | 真实 dispatch 计数、三层 context、失败分类、冷却/原号/切号 | 绑定 500、首输出超时、429 不因旧资格门提前结束 |
| B | Sub2 逻辑输出器 | A 的 typed attempt 结果；内部 v1 控制；唯一对外终态 | 普通正文实时、失败后可见重生、Responses/Chat 聚合一致 |
| C | NewAPI 受管接入 | 双向能力握手；在 accumulator 之前消费控制 | 失败轮计量隔离、只结算最终 logical request 一次 |
| D | Sub2 alias/历史 | 可信 attribution；A/B 的 winner 决议；原子持久化 | 下轮 logical ID 到真实 winner，跨用户/过期不可越权 |
| E | 联合验收 | A–D 全部实现，按入口独立开关 | SDK、故障注入、TTFT/资源/工具/结构门槛通过 |

先开发 A、B 的无 alias 简单路径和 C 的握手/状态机，再以 D 开放 Responses 可见重生；缺可信归因或 alias 不可发布时，不能打开需要跨请求身份保持的能力。客户端断连、逻辑期限和有效限额终态始终独立于上游瞬时故障。

## 2. 单一协议与权责

只有 Sub2 协调器决定下一次真实调用；NewAPI 只负责受管协议验证、格式转换与既有终端用户结算。Sub2 的 User/API Key 仅是既有框架鉴权；本项目不新增 Sub2 终端用户账本。多次上游调用的费用可由运营者承担，但不得自动重复有副作用的外部工具调用。

内部控制协议固定 `kkai.recovery.v1`，仅可信受管 channel 双向握手 `X-KKAI-Recovery-Protocol: 1` 后启用。五种 kind 为 `attempt_begin`、`attempt_abandoned`、`synthetic_begin`、`synthetic_end`、`terminal`。完整字段、合法时序及单条 SSE `data:` 序列化以第 4 节为唯一规范；第 6–7 节只能引用，不另造不同 schema。终端自带同名头须清除；上游冒充保留控制 type 须拒绝。无握手不能泄露内部控制或声明输出后恢复；受管握手缺失/版本错误是协议错误，不静默降级。

正常正文一到即交付；不能为控制帧单独提前 Flush HTTP 头。首个业务提交前可有界保存控制元数据，并在原本响应头提交点前按控制→业务顺序写出。对仍可交付的流，`terminal` 只选择最终 attempt 的 outcome/usage，并紧邻且先于该入口的最终公开结果；Responses 成功/限额/失败对应各自终态事件，Chat 成功对应完整 finish/usage/[DONE] 序列，最终失败对应 error 事件。客户端已断开时只做有界内部清算，不再要求发送控制或业务帧。

终态首次携带的 winner 正文/工具，Sub2 先正规化为同 attempt 常规业务事件并按前缀去重，再发送内部 `terminal` 与公开终态。NewAPI 受管 v1 禁止从含旧失败片段的复合最终 `response.output` 回填 winner 文本计量；winner 没有文本就保持没有文本，usage 未报告按现有未知/估算规则处理。展示转录、winner 恢复历史、winner 用量三份投影不得混用。

## 3. 恢复编排与预算

本章保留审查时的缺陷锚点并记录落地边界；实际符号与测试见第 10 节。仅覆盖 NewAPI 受管的 Sub2 OpenAI 文本 HTTP `/v1/responses`、`/v1/chat/completions`，含其实际选中的原生和转换分支。Sub2 的 User/API Key 是现有运营框架实体；终端用户和收费在 NewAPI，不为此增加 Sub2 客户账本。普通文本 delta 保持实时交付；中途失败由逻辑输出器发可见重生成分段，协调器仍实际调用上游。不能把输出字节数、已计上游成本或一次 200/EOF 当成停止恢复或成功的依据。

### 3.1 单一 owner、状态与现有入口

- **现有** `backend/internal/handler/openai_gateway_handler.go` 的 Responses handler 在约 405 行调用 `beginOpenAIResponsesRequestBudget`，约 622 行装 `WithOpenAIModelDispatchBudget`，约 698–1145 行自己选号、重试、排除、终结；`backend/internal/handler/openai_chat_completions.go:23` 的 `ChatCompletions` 是独立 handler，目前未装同一 900 秒/11 次预算。`backend/internal/service/openai_plugin_transport.go:81` 的 `doOpenAIUpstream` 在插件/HTTP 上游发送前调用 `takeOpenAIModelDispatch`；这才是共享调用额度的实际扣点。
- **已落地**的两入口共享 `beginOpenAIRequestBudget`、`WithOpenAIModelDispatchBudget` 与 `takeOpenAIModelDispatch`；handler 保留请求局部的同号计数、暂排时点、硬排账号和最后结果，`OpenAIRecoveryStreamWriter` 持有逻辑 ID、事件序与唯一外部终态。未新增单独的 `OpenAIRecoveryState` 类型或第二个恢复平台；实际决策留在现有 handler 循环中。
- 决策仍遵守 `retry_same_account | switch_account | rebuild_continuation | wait_until_eligible | finish | stop` 语义，但没有为这些动作新造仅一处使用的枚举。一次 attempt 只进入一次恢复决策；`finish` 只接受完整或合法限额终态。选不出账号前区分暂时冷却、永久不可用、硬绑定与策略拒绝，不能统一翻译成池耗尽。
- NewAPI 从请求入口认定受管 Sub2 是唯一恢复 owner；NewAPI 的外层 relay retry 不给同一逻辑请求重新发放 11 次/900 秒。`X-Sub2-Retry-Status: exhausted` 仅是尚可发响应头时的兼容提示，不能承担流中 ownership。独立入口（原生 WS、Anthropic/Grok 等）不因调用公共函数就自动启用此契约。
- 受管链路的 NewAPI HTTP 总时限、响应头等待及 scanner 不可沿用会抢断恢复的 300 秒空闲窗口。采用从原请求入口起算的 930 秒固定传输上限，其中 Sub2 的模型逻辑预算仍严格为 900 秒，额外 30 秒只给网络和终态交付；收到数据、重试或切号都不刷新绝对期限。受管请求直接继承原始客户端取消，不沿用非受管请求“首响应后再屏蔽取消并 drain”的策略，以免 Sub2 在用户取消后继续新 dispatch。已有用量清算保持其独立、有界的既有语义。

### 3.2 三层 context 与资源生命期

1. `downstreamCtx` 保留原始 `c.Request.Context()`，只代表客户断连/取消。启动时保存原引用，不在循环中用 attempt context 永久替换 `c.Request`。`failoverClientGone`（`handler/failover_loop.go:337`）须检查原始下游取消，不再把一次上游超时误判为 499。
2. `logicalCtx` 从 `downstreamCtx` 派生入口起算的绝对 deadline：`entryAt + min(configuredHardBudget, 900s)`，并随下游取消而终止。`WithOpenAIModelDispatchBudget(logicalCtx)` 恰好执行一次；`openAIModelDispatchBudget.entryCtx` 必须绑定此层，绝不可绑定首次 600 秒 attempt context。`beginOpenAIResponsesRequestBudget` 改为两入口可复用的逻辑初始化；Chat 入口显式调用，不能只把函数改名。
3. `attemptCtx` 从 `logicalCtx` 派生每轮独立超时与 cause，保留逻辑共享 budget value。首次调度按下节设期限；后续轮只受剩余绝对时限、必要交付余量、首输出计时约束，不每轮重发 900 秒。上下游读流、429 probe/租约、body reader 均归本轮，失败时先关闭 body、释放账号和出口 lease/探针，再在 `logicalCtx` 上等待和重选。`context.Cause(attemptCtx)` 或等价 typed cause 区分 `attempt_timeout`、`first_output_timeout`、`logical_deadline`、`client_cancel`；确定分类后才走现有 cancellation/499 路径。
4. `service/gateway_usage_billing.go` 的 `detachUpstreamContext` 现用 `WithoutCancel` 但重新应用原 deadline；`detachStreamUpstreamContext` 则去掉取消与 deadline。改造时将“原 attempt 已发出后的有界清算”和“能否发下一次模型请求”分开，清算可以有限延续，任何新 dispatch 必须由 `budget.entryCtx` 检查原 logical cancel/deadline。流中的上游 reader 仍必须受 attempt 关闭控制，不能因 detach 留下永久后台生成。

### 3.3 11 次真实调用、900 秒绝对期限与首轮保留

- 复用 `service/openai_retry_budget.go` 的 `maxOpenAIModelDispatches=11`、`WithOpenAIModelDispatchBudget`、`takeOpenAIModelDispatch` 的互斥原子扣点。选号、限流准入、冷却等待、纯本地参数转换均不扣；同号、切号、真正发送的兼容修复、插件处理或 HTTP 模型请求都扣一次。扣点在插件/HTTP transport 入口，失败在发送前但已进入该入口时保守计一次，不允许重试时重建 budget。超过 11 次返回 typed `dispatch_limit`，不记成账号坏。
- 复用 `config.GatewayConfig.EffectiveOpenAIRequestBudgetSeconds()`，默认且上限 900 秒。从用户进入 handler 记录 `entryAt`，包括读 body、排队、选号、等待、上游生成和终态交付；上游或内部重试不刷新 deadline。用户取消或 deadline 到达立刻禁止新 dispatch；既有用量清算仅在现有有界机制内收尾。
- **初值（待实测调参）** `firstAttemptMax=600s`、`recoveryReserveTarget=300s`、`finalDeliveryReserve=15s`。在真正首 dispatch 前重新算 `remaining = max(0, logicalDeadline-now)`；`reserve = min(300s, max(15s, remaining-120s))`；`firstCap = max(0, min(600s, remaining-reserve))`。首次 attempt deadline 为 `now+firstCap`，不晚于 logical deadline。正常 900 秒即约 600 秒首轮与含最终交付余量的约 300 秒恢复；若入口/排队已耗时，先缩短首轮；不足时 reserve 可降到 15 秒，`firstCap==0` 则不 dispatch。15 秒只用于停止发起新轮和完成本地终态，并非允许突破 900 秒；若最终输出本身需要更多时间，仍以绝对 deadline 为准，不能伪造成功。首轮 600 秒可能提前中断原本稍后会成功的生成，必须测量取消/延迟再调整。
- 每次**真实** dispatch 创建 `physicalDispatchStartedAt`，先算 `effectiveTextLimit=120s`，仅当 `configuredTextFirstOutput>0` 且小于 120 秒时取该配置值；可用首个文本输出 deadline=`min(physicalDispatchStartedAt+effectiveTextLimit, attemptDeadline, logicalDeadline)`；reasoning、心跳、SSE 结构事件不算可用文本。兼容修复在同一个 service `Forward` 调用里再次实际发送也必须获得新 startedAt、fresh parser/body、fresh 首输出 guard。现有 `service/openai_gateway_forward.go:1053–1076`、`openai_gateway_passthrough.go:372–383` 的 guard 从整个 Forward `startTime` 起算，需逐 dispatch 改造；Chat 原生/转换分支（`openai_gateway_chat_completions.go`、`openai_gateway_chat_completions_raw.go`）同样验证。`service/openai_text_output_deadline.go` 现强制文本 120 秒上限，保留其语义，不让 high effort 的 600 秒旧配置放宽文本限额。
- 后续 attempt 使用剩余 `logicalDeadline-now-finalDeliveryReserve` 判断能否**启动**新调用，不能给每轮反复扣留 300 秒。真正已发出的成功轮依逻辑 deadline 完成；余量不足但尚未到期时可以明确停止 `insufficient_recovery_headroom`，须记录剩余时间，不把它冒充“无账号”。所有等待使用 `logicalCtx` 可取消 timer，不忙轮询；`Retry-After` 和共享冷却的较晚时点优先于本地退避。

### 3.4 清除旧资格门、保留必要保护

- **现有要删的独立 300 秒语义及全部调用点**：`config/config.go` 的 `DefaultOpenAIRetryBudgetSeconds`、`GatewayConfig.OpenAIRetryBudgetSeconds`/`EffectiveOpenAIRetryBudgetSeconds`、默认值与校验按 §3.5.4 分步退场；`handler/openai_gateway_handler.go` 的 `beginOpenAIResponsesRequestBudget` 中 `WithOpenAIRetryBudgetDeadline`、各处 `OpenAIRetryBudgetExpired`（约 703/868/998/1095/1106）、`ErrOpenAIRetryBudgetExhausted` 分支及 `handleOpenAIRetryBudgetExhausted`；`service/openai_retry_budget.go` 的该 deadline value/check 和 `takeOpenAIModelDispatch` 第 95 行门；`service/openai_gateway_forward.go:1054` 与 `openai_gateway_passthrough.go:372` 的内部兼容循环门。删除或更新所有引用/测试，不能仅把 config 设 0，因为现有 0 会回到默认 300；`deploy/config.example.yaml` 对旧键说明须删除/迁移，不令旧值悄悄继续限制恢复。
- **不删除** `takeOpenAIModelDispatch` 的 11 次互斥检查、逻辑 hard deadline、第一输出120秒、账号实际冷却/429 probe、永久停用与安全拒绝、针对本地空转的利润 veto 有界保护。旧 `maxOpenAIFirstOutputTimeoutSwitches=1` / `openAIFirstOutputFailoverExhausted` 与 `ShouldStopOpenAIOAuth429Failover` 的累计三次捷径不再对目标两入口作为 logical request 终止条件；后者 Grok/WS 等未纳入入口保持旧语义，须按 caller 精准调整。`maxAccountSwitches` 只能防止未发请求的选号空转，不得在真实 dispatch 仍有预算且存在可行恢复账号时成为另一个更小调用上限。
- 清理旧 `retry_budget_exhausted`/`request_budget_exhausted`/`model_dispatch_budget_exhausted` 混淆：只保留符合真实条件的 `logical_deadline`、`dispatch_limit`、`no_viable_account` 等 stop reason；原有监控字段的迁移在受管链路以新增维度兼容观察，不悄悄把旧 502 指标含义改成成功。

### 3.5 错误决策与配置

#### 3.5.1 统一分类与动作表

`UpstreamFailoverError` 和有类型的 attempt outcome 是输入，不使用最终 HTTP 状态单独决策。分类顺序：先原始客户端取消/逻辑 deadline；再可接受成功/合法限额终态；再本地策略和确定参数错误；最后上游限流/瞬时故障与可行账号，**仅在需要下一次发送时**检查 11 次额度。第 11 次调用若成功仍须 `finish`，不能先按计数终止。以下仅指目标两入口；每轮均记录 attempt number、账号、上游原码、已公开文本、决策、下次准入时间、剩余调用/时间和最终 stop reason，不记录 prompt、工具正文、密文或凭据。

| 信号 | 决策与终止规则 |
| --- | --- |
| 原始客户取消 / logical deadline / 失败后拟发第 12 次 dispatch | `stop` 为 `client_cancel` / `logical_deadline` / `dispatch_limit`，分别处理，均不记作上游账号故障。已开始的用量收尾按既有有界清算。 |
| 完整成功或协议承认的 Responses `incomplete:max_output_tokens`、Chat `length/tool_calls` | `finish` 并保留真实状态/原因；不能把合法限额结束转成失败重试，也不能把截断工具参数交付为可执行调用。 |
| 429、5xx/overload、HTTP2/流读取失败、首个可用文本超时 | 产生 recoverable attempt outcome；无论先前是否输出，都先交逻辑 writer 隔离中间终态，再按同号/换号/等待流程。读流失败不能仅凭 `context canceled` 字符串判断为客户端取消。 |
| 账号永久失效、不可恢复凭据/出口或该账号不支持模型 | 硬排该账号；有合法备选才切号，绑定 continuation 先按 §3.5.2 的历史条件处理；无可行路径才 `no_viable_account`。 |
| 安全/内容策略拒绝、无效 NewAPI 归因、请求格式确定错误、不可修复 400 | `stop` 原因具体化，不换号规避，也不反复原号请求。兼容修复仅 §3.5.3 中单次白名单例外。 |
| 暂时全部冷却、共享 probe 暂不可用 | 对每个合法账号计算 `eligibleAt(account)=max(该账号 Retry-After、cooldown、probe nextEligibleAt、本请求退避)`，再取 `min_account eligibleAt(account)` 等待；到点重新完整执行 `select -> recheck -> admit`。超过逻辑期限/交付余量才按真实原因停止，不把暂时不可选写成永久无账号；共享状态故障不绕过探针并发门。 |

#### 3.5.2 同号优先、暂排冷却与 bound continuation

- **现有缺陷** `handler/failover_loop.go:101–105` 的 `openAIAccountRetryBeforeFailoverAllowed` 仅因 `HasOpenAIModelDispatchBudget` 就禁用原号重试。Responses 首次500后 `handler/openai_gateway_handler.go:1116` 把 bound account 加到 `failedAccountIDs`，selector 带 `previousResponseCanMove=false` 再查同一账号，最终得到 “previous response account is unavailable” 而未发第二次模型调用。取消此预算即禁同号分支，保持账号硬绑定。
- **拟改**可恢复的瞬时失败先在同号上最多**额外两次**（初次＋2），每次都回到 `RecheckOpenAIAccountSchedulable` / `AdmitOpenAI429Selection` 和共享 probe；不能绕过当前冷却、账号 disabled 或 lease。Retry-After/冷却长于可用剩余时可先选其他**合法可移植**账号；若 previous_response_id 硬绑定且完整历史不足，等待原号最早准入直到预算不足，明确 `bound_account_temporarily_unavailable` / `continuation_history_missing`，不得排原号后假切号。局部同号次数在 logical state 中按 bound turn/account 计，不因选号循环重置；其他账号同样最多两次，但总调用上限仍 11。

受管 v1 恢复链路的本次明确策略覆盖旧 `pool_mode_retry_count=0` 同号禁重试设置：对可恢复瞬时故障仍允许最多额外两次有界同号尝试，因为终端用户已要求完成恢复流程。只在该受管链路覆盖次数，不改账号配置或其他入口；账号永久 disabled、无效凭据、安全/策略拒绝、冷却和 probe 准入仍严格生效。部署前在管理员配置与文档中显式标出此语义变化，避免旧 0 被误以为继续禁止该链路重试。
- `failedAccountIDs` 只存永久不可用或本请求绝不再试的硬排；暂时429/5xx/首输出超时使用请求局部 `managedAccountRetryAt` 与既有共享冷却，时点到后重新准入。未绑定的普通 sticky session 可换号；`previous_response_id` 是硬账号/出口绑定，跨号仅在完整、可移植历史重建并经目标账号能力确认后执行。现有调度器保持绑定校验；alias 能力用 `OpenAIRecoveryAliasWinner.Snapshot`、`RebuildOpenAIRecoveryContinuation` 和 `openAIManagedContinuationRouteMatches` 明确判断，禁止以 `response->account` 映射当完整历史。旧链缺失继续合法原号恢复，session-blocked 已失效且无快照时明确停止，不删除 ID 伪造新会话。
- `handler/no_account_error.go:223` 的 `waitForOpenAI429Selection` 仅初选号、无排除、未输出且最多等 3 秒；`handler/openai_429_admission.go:30` 的 `handleOpenAIDeferredSelection` 可直接结束流。目标两入口改由协调器在**每轮**选号/准入时接管冷却，等待后再完整 `select -> recheck -> admit`；失败 attempt 的 body、lease/probe 必须先释放。`service/openai_429_recovery.go` / repository 共享 429 probe 与 2/4/8/15 秒恢复机制保留，不能为延长等待绕过全局保护。
- 同号次数上限是优先次序，不保证硬打 3 次：若账号被证实永久不可用、Retry-After 已超逻辑期限、客户端取消或安全拒绝则不发请求；只有冷却还未到不代表永久失败。切号不继承失败轮半截工具参数或不透明 reasoning。

#### 3.5.3 参数兼容与 29＋9 分类

- 29 条瞬时样本属于上面恢复编排（14 首输出超时、8 上游500/502、3 HTTP2、3流内429、1 overload）；9 条确定性样本单独验收（8 reasoning effort、1 continuation 账号兼容），不得宣称 38 条全部会成功。`service/openai_gateway_upstream_errors.go:340` 现判 429/5xx 等可 failover，同时对 cyber policy/context-window 等作例外；保留这些防绕过边界。`service/openai_responses_rejected_field_retry.go:114` 现有白名单兼容修复继续单次且去重；每次真正再次发送都扣统一 dispatch，不得反复修同一字段。
- 对 `none/minimal` 采用**确定的配置优先**：先按入站原始请求与已选实际 upstream model 读取现有 group `ReasoningEffortMappings`（`service/openai_reasoning_effort_policy.go:338,502`，支持 exact/prefix/suffix/deny），仅配置明确匹配且目标 effort 在该模型已验证支持列表时进行一次映射；无配置或无可证实目标档位，则保留原字段、返回准确兼容 400，并记录 `reasoning_effort_unsupported` 与脱敏 `requested/effective/model`，不自动兜底成 low、不删字段、不换模型。`minimal -> none` 现由 `service/openai_gateway_forward.go:400–402` 对 `reasoning.effort` 无条件执行；改为仅在目标上游明确支持 none 时执行，否则服从已配置映射或保留原值。注意 Chat flat `reasoning_effort` 与 Responses nested `reasoning.effort` 都要覆盖，不把 ceiling downgrade 当作实际模型能力证明。已有模型元数据若不完整，先不猜；确需增能力表时限定精确 upstream model/协议/账号类型并经请求样本验证后再配置。兼容动作日志只含字段名、源/目标档位和模型，不含请求正文。

兼容验证分两阶段：选号前保留原始入站 body、requested effort 与 group policy，不把预先改写的 body 当下轮输入；选号后依据实际 upstream model、账号类型和协议能力生成该 attempt 独立 body，并验证映射目标。每次同号重试或切号从原始 body 重建并重新校验，逻辑级去重只限制重复的兼容修复。现有 `applyOpenAIReasoningEffortPolicyForRequest` 在选号前运行、按 `body.model` 匹配，开发时必须拆开这两个阶段，不能沿用当前调用位置来宣称完成真实模型能力门。
- 上游给出明确 unsupported effort 的 400 时，只有既有明确映射尚未应用且符合上项支持证据，才可**至多一次**修复重发；若本轮已按映射发过、错误未具体指向 effort、属于权限/格式/策略拒绝，则不重发。映射状态在 logical request 中持久化，重试/换号不能无限重置；不同账号模型能力不一致时重新验证目的档位，不能拿 A 的报错改写 B 的请求。该 400 若无安全修复，终止为 `reasoning_effort_unsupported`，不笼统记为 failover exhausted。
- `previous_response_id` 的 400 `incompatible account` 不是把上一 ID 原样切给别的账号即可修复；先检查 alias、真实 owner/egress 和完整历史，依 §3.5.2 同号或合法重建，缺历史以 `continuation_history_missing` 终止/等待，不删除 ID 假装新对话。

#### 3.5.4 配置演进与可验收的最小检查

- **现有键保留并澄清** `gateway.openai_request_budget_seconds`（0 默认900，支持60–900）与 `gateway.openai_first_output_timeout_seconds`（文本值被120硬上限截断）。**拟废弃** `gateway.openai_retry_budget_seconds`：首个兼容版本仍解析显式旧键，但对目标受管入口完全忽略它的资格限制，启动时一次性告警并指出需从部署配置删除；不能让旧值 300 悄悄生效，也不能在旧配置未迁移时突然拒绝启动。更新 `deploy/config.example.yaml` 并同步部署配置后，单独移除旧 `GatewayConfig` 字段、默认值及校验；不得在本次同时移除解析字段却声称仍能检测显式旧键。非目标入口若曾依赖该 value，先按调用点确认并保留其原语义，不将新策略全局扩展。`firstAttemptMax=600s`、`recoveryReserveTarget=300s`、`finalDeliveryReserve=15s` 首期为目标入口内部有界常量；需要运营调参时再增配置及校验，不恢复旧 retry-window 键。实际阶段性启用需要 NewAPI 内部协议能力握手，不允许旧客户端收到未协商控制帧。
- **现有精准测试文件**：`backend/internal/service/openai_retry_budget_test.go`（11次并发/取消/旧门断言需更新）、`backend/internal/handler/failover_loop_test.go`（当前 “prefers fresh account” 应反转为有界同号）、`backend/internal/handler/openai_gateway_first_output_timeout_test.go`（当前单切号门应替换）、`backend/internal/service/openai_text_first_output_timeout_test.go`、`backend/internal/handler/no_account_error_test.go`、`backend/internal/handler/openai_429_admission_test.go`、`backend/internal/service/openai_reasoning_effort_policy_test.go`、`backend/internal/service/openai_responses_rejected_field_retry_test.go`、`backend/internal/service/openai_gateway_upstream_errors_test.go`、`backend/internal/config/config_test.go`。Chat 路径现有 `backend/internal/service/openai_gateway_chat_completions_test.go`；原生 Chat cancellation 专项在 `backend/internal/service/openai_gateway_chat_completions_cancellation_test.go`，带 `unit` build tag。
- 审查阶段曾列出的测试名是需求标签，不是当前执行记录。已执行的代表性断言包括 `TestOpenAIManagedAttemptPreservesRecoveryBudget`、`TestOpenAIManagedChatStopsAtSharedDispatchLimit`、`TestOpenAIManagedChatRecoversAfterVisibleText`、`TestOpenAIManagedResponsesRecoversAndRebuildsContinuation`、`TestOpenAIManagedLegacyContinuationDoesNotDropMissingHistory`、`TestOpenAIManagedFirstOutputStartsAtEachPhysicalDispatch` 及 writer/alias/schema/config 专项；精确命令、结果与剩余验收门见第 10 节。

## 4. 真流式交付与内部控制

### 4.1 交付边界

本章节规定实现边界，不证明任何入口已经通过客户端或性能验收。普通文本继续真流式：首个真实文本 delta 立即交付，不等待本轮成功终态，不恢复历史的整段普通文本缓存。恢复协调器只隐藏中间 attempt 的失败终态和内部控制，不能撤回已发送字节。

每个 logical request 有一个 `logical_id`、一个逻辑期限、一个共享真实模型调用计数。每次实际上游网络 dispatch 递增 `attempt`；同号重试、切号、明确的内部兼容修复都消耗同一计数。`attempt` 不能用 handler 重试圈数或选号次数替代。entry context 绑定 logical context；attempt context 只负责本次网络调用，超时后释放并重新调度。

Sub2 的适配器只返回 typed attempt result；协调器决定 `retry_same_account`、`switch_account`、`rebuild_continuation`、`wait_until_eligible`、`finish` 或 `stop`。服务不得先把中间 `response.failed`、`error`、`[DONE]` 或 Chat `finish_reason` 写成逻辑终态。

### 4.2 受管内部控制 v1

内部控制只对明确配置为可信的 NewAPI→Sub2 channel 开启。请求与响应均使用：

```http
X-KKAI-Recovery-Protocol: 1
```

NewAPI 清除终端用户传入的同名头，只按受管渠道配置设置；Sub2 必须同时验证现有受信调用身份，不能仅凭头值授权。响应回显用于确认协议生效，NewAPI 消费并移除该内部头。未启用该能力的旧渠道不发送控制事件，也不宣称支持本文的输出后恢复；已启用 v1 的渠道遇到缺失/未知版本回显时按协议失败处理，不能静默改成半支持模式。控制事件只在 Sub2 源边界由网关生成；上游原样伪造 `type=kkai.recovery.v1` 的事件必须被拒绝/记录为协议污染，不能被当作内部控制。

控制事件是标准 SSE `data:` JSON，且只在已完成握手的受管 channel 出现：

```json
{
  "type":"kkai.recovery.v1",
  "kind":"attempt_begin",
  "logical_id":"resp_lr_opaque",
  "seq":1,
  "attempt":1
}
```

`kind` 取值固定为：`attempt_begin`、`attempt_abandoned`、`synthetic_begin`、`synthetic_end`、`terminal`。所有事件必须携带不透明 `logical_id`、单调递增的控制 `seq` 和真实 dispatch `attempt`；不得携带 prompt、完整参数、密文或凭据。`terminal` 额外携带必填 `usage_format`（`responses` 或 `chat`）、`usage_reported` 与 `usage`：

```json
{
  "type":"kkai.recovery.v1",
  "kind":"terminal",
  "logical_id":"resp_lr_opaque",
  "seq":9,
  "attempt":3,
  "outcome":"completed",
  "usage_format":"responses",
  "usage_reported":true,
  "usage":{"input_tokens":100,"output_tokens":20,"total_tokens":120}
}
```

以上数字仅为协议示例。线上每个控制对象序列化为单条物理 `data: <JSON>\n\n`。`outcome` 只取 `completed`、`incomplete`、`failed`、`cancelled`。`terminal.usage` 是带 `usage_format` 判别的**单一 tagged-union 协议**：`responses` 要求原生 `input_tokens/output_tokens/total_tokens`，`chat` 要求原生 `prompt_tokens/completion_tokens/total_tokens`；可选的 token details、`billing_usage`、`usage_semantic`、`usage_source`、缓存字段按对应原生格式完整保留。`usage_reported=false` 时 usage 必须为 null；true 时必须为完整非负对象，显式全 0 仍视为已报告。NewAPI 按判别字段校验必需字段并映射到既有 `dto.Usage`，不能把 Chat 当 Responses 解析成 0，也不能用非零合并抹掉 winner 的显式 0。格式错误属于协议失败，不进行静默估算。此对象只选择最终 attempt 的计量事实，不代替公开终态。

`synthetic_begin` / `synthetic_end` 包围网关生成的可见重生说明；说明不计入模型文本 token、usage 或工具统计。NewAPI 必须在正文/usage/tool accumulator 之前消费控制事件，并按 attempt 隔离暂态计量；最终只提交 winner 或明确最终失败的事实。

控制状态机固定如下：

- `seq` 从 1 起每帧加 1，独立于公开 Responses 的 `sequence_number`；NewAPI 拒绝重复、缺口、logical_id 变化、非法 kind 和时序。
- `attempt_begin` 与真实 dispatch 的统一记账点绑定，在该 attempt 首个业务事件前到达。首个业务提交前仅把少量控制对象保留在有界逻辑队列，不能为了 control 提前 Flush HTTP 头；在原有头提交点决定本次头后，同批按 control→business 顺序写出。此队列跨 attempt 保留但设确定容量：最多 11 个 begin、11 个 abandoned、1 个 terminal；若已有可见输出，最多再有 22 个 synthetic begin/end，总上限 45 个控制对象。该上限覆盖第 11 轮废弃后最终失败；前业务阶段保守上限 23 个，不扣留普通文本；超限按协议错误收尾。已有业务提交后，控制可即时发送。`attempt_abandoned` 明确废弃当前 attempt 的计量候选，不删除已显示文本，也不把它立即结算为另一个用户请求。
- `synthetic_begin/end` 仅包说明的新增 delta，不嵌套，必须成对；包含旧模型文本的 text-done/item-done 快照不能整体当作说明帧。中断片段本身仍是已生成模型文本，内部统计是否纳入最终费用遵循 NewAPI 既有政策。
- 对仍可交付的已握手 SSE，`terminal` 只出现一次，紧邻且先于最终公开结果；Responses 按 completed/incomplete/failed 真实终态，Chat 成功按完整 finish/usage/[DONE] 序列、最终失败按 `event: error` + `data: {"error":...}` 序列。控制 outcome 与公开结果必须一致，不能为失败伪造 finish 或 [DONE]。没有真实 dispatch 时仅允许 `attempt=0` 的最终失败控制。客户端取消/断连后不再发送 terminal 或公开帧，只记录内部取消与有界清算；未提交 SSE 头的 stream=false/前输出错误保留普通 JSON。
- 最后一个 attempt 已 abandoned、但预算耗尽或无合法路径继续 dispatch 时，允许以该最后 attempt 编号发送 `terminal` 的 `failed/cancelled`；此时必须 `usage_reported=false` 且 `usage=null`，不能恢复废弃轮暂态计量。abandoned 后除 synthetic 区间及旧展示项的 text/part/item 闭合事件外，不允许新模型增量；terminal 后只能出现与 outcome 对应的公开结束序列。
- EOF 发生在 synthetic 区间中、terminal 后没有与 outcome 匹配的公开结果、未知版本或任意状态机错误，均为协议未完成，不能计作正常成功；不泄露内部帧，也不跳过既有失败结算。
- 控制帧不增加 received model event、TTFT 或工具计数。最终可见 output 是展示转录，不能作为 winner-only 文本 fallback 或下一轮恢复历史；NewAPI 保留按 attempt 的独立计量投影。

terminal-only 输出不增加新的控制 kind：若 winner 的标准终态首次携带完整正文/工具，逻辑输出器先将未交付的部分正规化为同 attempt 的正常 item/delta/done，再发内部 terminal 与公开终态。已发送部分只允许匹配同 attempt 的精确前缀后补缺，不能重复或悄悄覆盖。NewAPI 在受管 v1 中禁止从混合最终 `response.output` 再提取全文计费 fallback；winner 没有文本就保持没有文本，未知 usage 仍按既有未知/估算规则处理，不从旧展示段或 marker 补数。

### 4.3 Responses 逻辑生命周期

Responses 对外只发送一次 logical `response.created`，之后所有事件的 response ID、output/content index 和 sequence 由类型化映射维护；不得用字符串替换 JSON。后续 attempt 的原生 `response.created` 只作为内部输入。

公开 `response.created` 的 model、service_tier、created_at 和已交付头部元数据属于首轮对外展示事实，不因换号重写已发送字节；最终 winner 的实际模型、层级、创建时间和用量另存于 attempt 计量/历史投影，账单按 winner 事实。未提交头部前可按实际结果确定；头部已提交后不能伪称 winner 元数据早已交付。

一个 item ID 永不跨 attempt 复用；output index 按对外交付顺序连续分配，content index 限定于该 item。延迟的工具/opaque item 不能先占一个未公开索引、再让后续文本跳过它。公开 sequence 由唯一 writer 递增，禁止 keepalive、恢复说明和上游 reader 并发写出导致重排。

普通文本失败后：

1. 隐藏中间 attempt 的 `response.failed`/`error`；按 output index 与 content index 逐一闭合旧文本及 refusal parts，再关闭对应 item。默认使用“展示段闭合”序列：各 text/refusal-done、content-part-done 仅补尚未关闭的层级；此 `status=completed` 仅表示展示 item 已闭合，不表示该 attempt 成功，控制事件与日志仍为 `attempt_abandoned`。
2. 已有可见内容时，发出独立的 assistant message 中断说明 item，使用新 item ID/output index；通过 `synthetic_begin`/`synthetic_end` 仅包说明 delta，排除其计量。闭合说明 item 后，新答案使用另一个新 item 继续正常 delta。
3. 只发一次最终逻辑终态。最终 output 必须反映已实际交付的旧片段、中断说明和新结果，不能把旧片段伪装成 winner，也不能在最终对象中删除客户端已经看到的内容。

展示段闭合的具体顺序是：旧 part 的 text/refusal-done（实际已交付前缀）→ content-part-done，逐 part 完成后 output-item-done；然后新建说明 item/content part → `synthetic_begin` → 说明 text-delta → `synthetic_end` → 说明 text-done/content-part-done/output-item-done。所有事件填齐对应协议要求的索引、ID、文本/part 和 annotations/logprobs 等字段；不得补造未交付的模型后缀。之后新建答案 item/content part 并流式输出新答案。必须按 content index 独立记录 text/part 的 done 状态：已经 done 的层级不能再打开或重复 done，只补齐尚未发送的关闭事件。尚无任何已公开可见业务内容时直接重试/切号，不发中断说明；有可见内容时说明始终独立，避免将文本追加进拒绝 part 或已闭合 part。所有说明是 assistant message，不能创建 role=system 的 Responses output message。

`status=incomplete` 的旧 item 是备选兼容序列：状态更直接表达模型 attempt 截断，但某些聚合器可能把它与最终 completed 判为不一致。推荐默认采用上述 completed 展示段闭合序列；completed 描述由网关明确收尾的展示 item，原 attempt 仍是 abandoned。两者都必须验证，不能以“更兼容”的推测代替真实 SDK 结果；未通过时不得启用该入口的可见重生。

最终同时维护三份逻辑上独立的投影：展示转录（含旧片段/说明/winner）、winner 恢复历史（不含失败片段/说明）、各 attempt 计量。可以共享不可变字节，但不能把展示 output 整包回灌为真实 winner 上下文。有界记录已发送文本用于最终聚合不延迟 delta；容量耗尽要明确记录并停止不再可实现的交付，不能悄悄省略已显示项。

### 4.4 Chat 生命周期与多 choice

Chat 保持一个 logical completion ID 与原 choice 关联。普通 content delta 立即转发；中间 attempt 的 finish、usage 和 `[DONE]` 全部隐藏。失败后在每个已交付 choice 内追加清楚的可见重生分隔，再继续该 choice 的新答案；不得伪造新模型 choice 或混合不同 attempt 的 finish。

恢复按整个上游 attempt 协调：所有非空 `finish_reason`、结束 usage chunk 和 `[DONE]` 均先扣留，必须等待所有预期 choice 的最终状态与适配器的正常终止契约，不能仅因 choice 0 已结束就提交终态。失败后丢弃这些尾部帧，所有 choice 一起重生成；仅在已公开 choice 上加分隔，没产生过内容的 choice 不伪造旧答案。只有最终成功或合法限额 attempt 发送各 choice 唯一的 finish、最终 usage 和一次 `[DONE]`；最终失败发送明确 error 事件，不伪造正常完成。`length`/`tool_calls` 保留合法语义；缺终态和读流失败不能伪装 stop。`n>1`、并行 choice、choice 顺序和客户端聚合必须逐项验收；未通过验收的入口不能声称 Chat 全覆盖。

不得将下一 attempt 的 `response.created` 原样喂给仍在对外工作的旧 Responses→Chat state：现有 `resToChatHandleCreated` 会覆盖 state.ID。每 attempt 使用独立适配解析状态，逻辑输出层保留统一 ID/choice，并对输出做有类型的归一化。协议不自带撤回文字：在 Markdown 代码块、纯代码答案等情况下，提示必须仍能辨认为网关分隔；严格机器结构走 5.2。

### 4.5 失败与交付

已发字节不能撤回；下游连接在最终交付中断开时，记录交付失败，不能通过重新调用模型宣称已修复该连接。用户取消停止新 dispatch，但已开始 attempt 的 usage/资源收尾仍须有界。逻辑期限、单次 attempt 超时和客户端取消分别记录。

`stream=false` 只做头部能力协商与内部恢复，不把本节控制 SSE 编进 JSON body；中间 attempt 未对外显示，最终只返回实际 winner 的正常 JSON/usage（含适用的 logical ID 映射），或标准失败 JSON。不上 heartbeat，不提前 200 空白；最终响应头在交付前从实际结果确定。内部把 SSE 聚合成 JSON 的路径也遵守此规则，不能把纯流式控制状态要求强加给普通 JSON 客户端。

NewAPI 的 SSE→JSON 聚合也必须在转换/累计前消费 v1 控制，尤其 `chat_completions_via_responses.go` → `OaiResponsesToChatBufferedStreamHandler`。此路径仅为客户端 `stream=false` 暂存最终 JSON，不改变 `stream=true` 普通文本实时交付；不得把中间 attempt 或 synthetic 提示混入最终结构/计量。

## 5. 工具、结构输出与兼容矩阵

### 5.1 工具和 opaque reasoning

普通正文保持真流式。按**客户端可执行调用**分类，而非只按原始 `item.type`：`function_call`、`custom_tool_call`、转换前后可能出现的 `tool_search_call` 及以后新增的可执行 call，其 arguments/input 与关联 ID 作为局部可执行单元暂存，直到该 attempt 达到可接受终态且调用内容完整、协议校验通过才释放。`responses_client_tools.go` 会把上游 function_call 还原成 `tool_search_call`，必须在转换两侧都遵守此门；多工具批次按整轮协调，失败 attempt 的半段参数、call ID 与工具终态全部丢弃。

`response.incomplete` 因 max tokens 是合法限额终态，但不授权释放截断工具参数。只有最终快照证明调用完整时才可执行；已释放的外部动作依靠执行记录/幂等性处理，恢复费用接受不扩大副作用授权。新 attempt 不重复发送已确认执行的调用。

Opaque reasoning/compaction 密文局部保留至该 attempt 的可接受终态，只释放完整且归属正确的 item；失败 attempt 不将密文公开或写入后续 history。可见 reasoning summary 按普通文本规则处理，已显示 summary 与未公开密文的状态分离。不得通过改 ID、丢密文或把两次 item 拼接伪造等价上下文。

`function_call` 参数按其 JSON/工具契约验证；custom/freeform input 按对应完成事件与工具契约验证，不能一律要求 JSON。完整性可由匹配的 done 事件或 authoritative 终态快照证明。只有结构校验通过也不代表外部动作已经执行：`openAIWSToolCallReplayCollector` 和 call_id 覆盖检查不是执行回执。已经释放但执行未知的调用不能自动换一个新 call_id 再发；等待客户端真实 result/已有执行记录，或按明确工具恢复能力结束。

### 5.2 严格结构输出

`json_object`/`json_schema` 禁止插入自然语言重生标记。首期可对已声明的受管结构模式局部暂存结构单元；提交条件是结构完整、schema 校验通过且 attempt 达到可接受终态。一个 JSON 对象可能就是整份答案，这会增加该类请求的首字延迟，但不代表恢复普通文本整段缓存。

若严格结构必须实时输出并支持重启，只能通过明确协商的客户端 restart/replace 能力；未验证前不得宣称无损重生。结构/工具单元超过有界容量时，按明确本地容量失败处理，不偷偷退回不安全拼接或无界缓冲。

### 5.3 兼容矩阵与实现边界

| 入口/模式 | 首期声明 | 必须验证 |
|---|---|---|
| Responses HTTP，普通文本，受管 v1 | 候选支持 | logical ID/index/sequence、展示段闭合、NewAPI 控制消费、目标 SDK 聚合 |
| Chat HTTP，普通文本，受管 v1 | 候选支持 | 同 ID、每 choice 分隔、全 choice 协调、唯一 finish/usage/[DONE] |
| Responses/Chat `stream=false` | 单独候选 | 普通 JSON、无 SSE 心跳、失败后不提前 200、最终 winner/终态一致 |
| 工具、opaque reasoning/compaction | 单独能力 | 完整调用批次、无重复副作用、失败轮密文隔离、上下文可移植 |
| strict JSON/schema | 结构模式候选 | schema 完整性、结构化客户端重生/重启能力 |
| 原生 Responses WS | 不继承 HTTP 声明 | 每轮状态、客户端 reset、工具/turn-state 逐项验证 |
| WS↔HTTP bridge、Anthropic、图像/视频及其他供应商 | 不继承声明 | 逐入口协议与计量验收 |

以下测试向量是按入口验收要求，本地代码级覆盖与仍未执行的真实客户端/性能验收在第 10 节分别记录：

1. 上游尚未终止而首个模型文本已到，客户端立即收到；没有完整文本 stage。
2. 文本后 429、HTTP/2 RST、overload、首输出/attempt 超时后实际继续 dispatch；旧段清楚闭合、新段实时输出，公共终态只有一次。
3. 增量和最终聚合两种读取结果完全一致；旧展示段已完成的标记不改变 abandoned 的内部结果；sequence/index/ID 无重复或跳占。
4. 多 choice 一半完成后失败，各 choice 同轮恢复；没有提前 finish、第二次 role/ID 切换或重复 DONE。
5. 工具半包、parallel calls、freeform input、限额截断、工具已释放但 result 未知分别处理；模型正文不被工具局部暂存拖到终态。
6. opaque reasoning 有 summary/无 summary、跨号、旧会话携密文、超限各有独立结果；失败轮 cipher 不进入最终 winner。
7. JSON/schema 闭合后再出现失败仍不公开该失败结构；单结构超限不会旁路；stream=false 仍是普通 JSON。
8. 控制伪造/未知版本/seq 重复与缺口/synthetic 中断/缺最终业务终态，均可确定归因；显式零 usage、缺 usage、terminal-only winner 文本不混入旧展示转录。
9. 经 NewAPI raw Responses、Responses→Chat、实际 Codex/SDK/UI 分别验收，不能用仅比较最终 JSON 的测试替代。

本章节的基础仍是 Sub2 `openAIFirstOutputStage` 的必要局部单元/前导保护，不恢复 `OpenAIAtomicStreamFailover`；`openAIWSToolCallReplayCollector` / `combineOpenAIWSReplayItems` 仅为已确认工具上下文的不可变收集。NewAPI 的 `ResponsesStreamEventToChatChunks`、`StreamScannerHandler` 在受管控制消费后接收最终逻辑事件。当前落地符号及未验证门槛见第 10 节；“候选支持”仍不是“已通过客户端验收”。

## 6. alias 与历史存储

### 6.1 可信命名空间与现有实现边界

恢复 alias 和历史快照只对经过 NewAPI attribution 验签的受管请求开启。命名空间必须由以下稳定字段共同派生并做不可逆哈希：Sub2 接入 API key、`NewAPIOrigin`、`NewAPIUserID`、`NewAPITokenID`；客户端自报 `X-Session-Id`、thread/window id 只能作为会话维度，不能单独建立跨请求信任。当前部署只有一个 NewAPI 发行方；`NewAPIOrigin` 是验签接收目标，不是可区分多个共享密钥发行方的租户 ID。

现有可复用符号：

- `backend/internal/server/middleware/newapi_attribution.go:NewAPIAttribution`：成功验签写入 `ctxkey.NewAPIUserID`、`NewAPITokenID`、`NewAPIOrigin`；缺少 attribution 时继续普通流程，伪造/无效签名返回 `400 INVALID_NEWAPI_ATTRIBUTION`。
- `backend/internal/service/openai_cyber_session_block.go:ResolveCyberSessionBlockIdentity`：已有可信组合和哈希方式，可复用其身份来源，不建立 Sub2 用户、租户或账本。
- `backend/internal/service/openai_ws_state_store.go:OpenAIWSStateStore`：已有 response/account、HTTP owner、turn-state、egress 和失效标记；这些接口不是完整历史快照接口。
- `backend/internal/service/openai_ws_forwarder_payload.go` 的 `openAIWSExtractNormalizedInputSequence`、`buildOpenAIWSReplayInputSequenceFromItems`、`buildOpenAIWSCurrentTurnRetryPayload`：用于输入归一化、工具上下文配对和保守跨账号门。
- `backend/internal/service/openai_ws_http_bridge.go:openAIWSToolCallReplayCollector`：`Items` 仅工具上下文，`AllItems` 可保留完成 output item；两者均只能作为快照构建输入，不能把连接内变量当持久化历史。

落地接口位于 Sub2 service/cache 边界：`OpenAIRecoveryAliasCache`、`NewOpenAIRecoveryAliasID`、`PublishOpenAIRecoveryAlias`、`ResolveOpenAIRecoveryAlias`、`DeleteOpenAIRecoveryAlias`。成功终态前原子 create-if-absent 发布；**首个正文前不做同步 Redis 预留**。发布前的 alias 只是不可续接的展示身份，读取 miss 必须拒绝，发布失败不得发送成功终态。快照正文与普通日志分离；当前实现只存规范化纯文本 input 和成功 winner output，不存失败展示片段，不把工具/附件/opaque 内容伪装为可移植历史。别名另存实际 response ID、账号/出口和 turn-state 策略。显示转录仅在当前请求内有界维护，不另落普通日志或持久化诊断库。

### 6.2 Redis key、值、TTL 与容量门

首期 published TTL 沿用现有响应粘性配置 `Gateway.OpenAIWS.StickyResponseIDTTLSeconds`（源码默认 3600 秒，`openAIWSResponseStickyTTL` 非正时回退 1 小时），**从成功终态原子发布时开始**；不得被已经结束的 logical request 900 秒期限裁成 0。published alias 的失效时间不得晚于它引用的 snapshot/parent。发布前的 pending 身份只存在于当前 logical request 内存中，随请求取消/结束释放，无 Redis pending TTL 或首字远程写入。部署前从目标配置核实真实 TTL；若实际响应亲和 TTL 变化，同步校验 alias/快照保持同一有效窗口。

实现使用 `resp_lr_` 加 144 bit 随机十六进制后缀作为对外 alias，保留 `resp_` 响应 ID 形态并使入口可识别；客户端仍不能自行创建有效映射。Redis 使用下列哈希 key，元数据与快照在同一个 hash 内原子写入：

```text
kkai:recovery:v1:alias:{scopeHash}:{aliasHash}
kkai:recovery:v1:alias:{scopeHash}:active
kkai:recovery:v1:alias:global:active
kkai:recovery:v1:alias:global:writes
```

已发布元数据使用版本化 JSON，包含 `version=1`、不可变 `revision=1`、`state=published`、`logical_id`、scope、`created_at/expires_at`、`has_snapshot` 和实际 winner response/account/egress/turn-state 策略。pending/failed 仅保留在逻辑请求内，不写出可解析映射；过期由 Redis TTL 与元数据时间双重验证。普通文本快照使用 `{version,turns,items}` 平铺成功历史，避免父链 TTL 短于新别名；不是展示转录，也不引用失败轮。实际凭据、prompt 正文、encrypted reasoning、附件内容不得放入普通日志；快照存储必须按字段白名单和总大小限制执行。

初始有界策略：单快照最多 8 MiB（借用现有 `openAIFirstOutputStageMaxBytes` 作为保守工程上限，**不**代表已测得完整响应足够），单父链最多 16 MiB/16 轮，每个可信 scope 最多 256 个已发布 alias、64 MiB 快照字节，全局最多 256 MiB 快照字节，最多 32 个并发快照写入。alias 元数据最多 4 KiB，展示转录另受同样 8 MiB 单请求上限。容量在发布时用 Redis 原子准入与 TTL 回收实现跨实例统一约束，不在首字热路径预留；计数回收、进程崩溃、原子发布和真实 Redis `maxmemory`/淘汰策略都必须做故障注入。数值可在发布前依据真实负载测量收紧或调大，但不得省略容量门或暗中无界退化。超过容量、序列化失败、Redis 超时或 CAS 冲突均是确定性本地恢复失败：不得退回不安全拼接，不得把 alias 或代理 ID 发给上游；可保留已有同号恢复或结束并报告真实原因。

容量计数成立的部署硬门：目标 Redis 必须经只读配置证据确认 `maxmemory-policy=noeviction`，并为现有业务、256 MiB 快照、索引及元数据预留足够空间。payload、作用域索引和全局计数是多个 key；允许选择性淘汰计数但保留 payload 的策略会使容量上限失真，不能以“保护 namespace”代替真实 Redis 配置。受管渠道开关默认关闭，未核验此门、实际 TTL 和容量故障注入前不得启用。本轮本地实现没有修改生产 Redis，也未证明该生产前提已满足。

纯文本历史构建保留不可变的原始请求。带 `previous_response_id` 的客户端 input 总是追加到已验证父历史；不能凭内容前缀相同猜测客户端重复了全历史并擅自去重。网关重建的完整 body 只用于当前上游尝试，不反过来冒充原始客户端 input。未知 message/part 字段、工具、附件和 opaque 内容不能被静默丢掉来伪造“完整文本历史”；当前纯文本重建会明确拒绝这些不可证明可移植的快照。

### 6.3 发布、读取和转发不变量

1. 逻辑请求本地生成随机 pending alias，可作为单一对外 response 身份早期公开，不做首字前 Redis 写入；绝不能作为已成功、可续接的 ID 被读取或转发，也不能让终端用户指定映射。
2. 每次真实 attempt 使用独立 attempt 编号和局部解析/usage/tool/image 状态；logical ID、绝对预算、已公开事件索引和取消状态跨 attempt 保留。
3. 只有 winner 达到可接受终态、快照完成校验并成功 `PublishWinner` 后，才允许发送对应终态；发布必须原子完成，且同一 alias 不可被后续轮次覆盖。发布失败时禁止透传代理内部 ID，按既有安全错误/可恢复策略处理。
4. 客户端携带 alias 的下一轮请求，在现有 owner/account/egress 校验前先读取并验证 NewAPI 签名 scope、状态、TTL、revision、当前 winner owner/egress，再将 alias 替换为 `actual_response_id`。现有 `ValidateOpenAIHTTPResponseOwner` 比对的是 Sub2 `subject.UserID`/API key，在当前仅管理员的部署中不能代替终端 NewAPI caller 验权。读取 miss、过期、`pending`、`failed`、scope 不匹配或存储错误时，**绝不**把代理 alias 原样发给上游；可回到可信原号恢复，或返回明确的会话不可用错误。
5. 绑定请求优先同号有界恢复。只有历史快照包含完整可重放 input、前轮成功 output、父链、工具 call/result、指令、附件权限和 opaque 内容可移植性证据时，才允许跨号；只有 tool-only replay、response→account 路由绑定或不透明 item ID 不构成完整快照。 超出 16 轮/16 MiB 父链容量只关闭跨账号重建，不能拒绝仍满足 owner/egress 校验的同号续聊；父链完整性仅在准备跨号时验证。
6. winner snapshot 只包含有效恢复上下文；对外显示的旧截断片段和 synthetic 重生提示只属于展示转录，不能再次注入下一轮 input，也不能估算 winner token。
7. `X-Codex-Turn-State` 采用首期 strip-old-echo 策略：命中 alias 时确定性剥离客户端携带的旧 blob；不主动注入未实际交付给客户端的 winner blob，不恢复主动 STATE 采集/续期/新票据系统。若目标客户端必须依赖 winner state，需专门兼容门和端到端证据。
8. 用户取消、逻辑总时限和单次 attempt 超时分开处理。显式取消后不得启动新 attempt；快照/alias 的取消收尾必须有界完成。

缺少可信 attribution 时，新 alias/跨请求历史/可见 Responses 重生模式整体关闭；保留不改变对外响应身份的既有原号恢复和现有认证。无效签名维持 `INVALID_NEWAPI_ATTRIBUTION`，不能降级为未提供。

首期只承诺后续请求的 `previous_response_id` alias 解析；现有 Sub2 路由没有 `GET/DELETE /responses/:id`，因此不新增 alias 的 retrieve/delete，不向客户端宣称代理 ID 可通用取回或删除。若未来新增，须另定 owner/TTL/删除传递与历史清理契约。

## 7. NewAPI 接入与观测

### 7.1 唯一内部 v1 控制契约

受管 channel 必须在配置中明确启用，并与请求/响应头 `X-KKAI-Recovery-Protocol: 1` 双向协商；未完成握手不得发送内部控制帧。**唯一的 v1 JSON schema、五种 kind、字段合法性与控制时序以 §4.2 为准**，本章不另定义副本。`attempt_begin` 宣布真实 dispatch，`attempt_abandoned` 废弃失败轮计量，`synthetic_begin/end` 仅包 Sub2 生成的可见提示 delta，`terminal` 提交最终 attempt 的用量事实。NewAPI 清除终端传入的协议头，Sub2 拒绝上游伪造的保留 `type`；NewAPI 只在协商成功且 sequence 合法时消费，不向外泄露控制帧。首个业务提交前控制对象只在有界队列中暂存，不为 control 单独 Flush HTTP 头；原有头提交点先定头，同批按 control→business 顺序交付。普通文本首字不等待整轮终态。

### 7.2 入口与 accumulator 前置消费

受管控制消费覆盖 NewAPI 的 raw Responses SSE、raw Chat SSE、Responses→Chat SSE、Chat→Responses SSE，以及客户端 `stream=false` 时的 SSE→JSON 聚合；所有入口在正文/usage/tool/image accumulator 或转换器前识别并消费 v1 控制。既有 raw 入口之外，还须接入 `relay/channel/openai/chat_via_responses.go`、`responses_via_chat.go` 与 `chat_completions_via_responses.go` 的 buffered 分支。

- `relay/channel/openai/relay_responses.go:OaiResponsesStreamHandler`、`relay/channel/openai/relay-openai.go:OaiStreamHandler`：现有 `helper.StreamScannerHandler`/`StreamStatus.RequireTerminal` 保持终态和取消保护；控制帧只改变恢复 attempt 的内部状态。
- `relay/channel/openai/chat_via_responses.go:OaiResponsesToChatStreamHandler`、`responses_via_chat.go:OaiChatToResponsesStreamHandler` 及 `chat_via_responses.go:OaiResponsesToChatBufferedStreamHandler`：控制必须在转换器/手写 scanner 前消费；不得仅改两条 raw handler。
- `relay/helper/stream_scanner.go:copyCodexSSEHeaders`：现有入口会在 scanner 开始时复制 `X-Codex-Turn-State`；一旦 HTTP 头已提交，不能换成 winner 头。必须保留实际交付头的归属事实，下一轮 alias 命中时确定性剥离旧 echo。
- `service/responses_usage.go:ResponsesUsageAccumulator` 与 `relay/channel/openai/responses_tool_usage.go:responsesToolUsage`：扩展为 attempt 局部实例或显式 `ResetAttempt`，不跨失败 attempt 合并。
- Chat 的 `responseTextBuilder`、`lastStreamData`/`secondLastStreamData`、`seenStreamToolCalls`、`streamFunctionCallNames`、`finishedChoices` 等局部状态必须在 attempt 切换时重建；Responses 的 `reportedUsage`、tool/image 计数和文本累计同样重建。
- 两个方向的转换器展示状态（logical ID、已发 role/choice、tool index、公开事件序）跨 attempt 保留；独立的 winner-only meter 在 `attempt_begin` 新建、`attempt_abandoned` 丢弃。`chat_via_responses.go` 与 `responses_via_chat.go` 的 `state.UsageText()` 当前会累计全部展示 delta，受管 v1 的无 usage fallback 必须改用 attempt-local winner 文本，不能 Reset 展示状态破坏客户端生命周期。buffered SSE→JSON 同理。

`attempt_abandoned` 到达时丢弃该 attempt 的暂态 usage、tool/image、文本估算和未完成结构单元；已公开旧文本仍留在展示转录。Sub2 先闭合旧展示 parts/items，再用独立 assistant message 生成网关说明 delta，并用 `synthetic_begin/end` **只包这段新增 delta**。NewAPI 仅透传或格式转换该业务 delta，同时跳过 synthetic 计量，不生成第二份提示。旧 item 可用 `status=completed` 闭合展示生命周期，但其 attempt 仍为 abandoned，逻辑 response 保持 in_progress；具体序列必须通过真实客户端的增量与最终聚合门槛。最终只对 winner attempt 调用现有 usage/终态处理一次。

`terminal.usage` 必须作为最终 attempt 的完整快照（包括显式零值）替换该 attempt 的 usage，而非调用 `MergeUsageNonZero` 保留上一 attempt 的值；`usage_reported=false` 当且仅当 `usage=null`。Sub2 在 terminal 前，把 winner 只在上游终态首次出现、尚未交付的正文正规化为**同 attempt** 的正常 item/delta/done；已交付部分只补未交付后缀，不重复。NewAPI managed-v1 禁止从包含旧失败片段和提示的最终 `response.output` 做全文文本估算回填，也不得让 `responsesToolUsage.observeResponse` 从复合 output 重新计入旧工具/图像；只取 winner 局部 accumulator、同 attempt 正规化 delta/工具事件与 terminal usage。无上游 usage 时遵循现有本地估算/unknown billing 规则，但估算输入必须只含 winner 实际生成内容。最终失败的 NewAPI 结算仍走现有 `BillingSession`、`RefundFailedRequestBilling`、membership billing 和 `BillingOutcomeUnknown` 语义，不新增 Sub2 客户收费或第二套用户账本。

### 7.3 既有终态、保护和头部不变量

不得删除 NewAPI 现有 `response_started`、client cancellation、billing-unknown、pinned/strict、`SkipRetry` 和 `StreamStatus.RequireTerminal` 保护；Sub2 是唯一恢复 owner，NewAPI 不因中间 attempt 重新发放独立 retry budget。`X-Sub2-Retry-Status: exhausted` 继续作为兼容终态提示，不承担 ownership。

标准 Responses 维持单一 logical response 生命周期：旧已发片段不可撤回；新的上游 `response.created` 不直接成为第二个对外 response。适配器必须维护一致的 logical response ID、item/content/output index 和 sequence；当前普通 Chat/Responses→Chat 转换器没有 reset 语义，未协商的重生不能承诺兼容。`StreamScannerHandler` 目前在 callback 前调用 `SetFirstResponseTime` 并增加 `ReceivedResponseCount`，managed-v1 必须在这个位置先分类内部 control，不能让其污染事件数；现有 `FirstResponseTime` 连 `response.created` 也计入，不能冒充首字，须另记 `first_real_text_delta`，排除 control、心跳、synthetic marker 与其他非文本事件。JSON/schema、工具参数、opaque reasoning、音视频/image 等不能用自然语言 marker 替代结构协议；目标客户端未验证前禁止宣称无损恢复。

### 7.4 观测字段与脱敏

沿用 NewAPI request ID、Sub2 attempted events 和现有错误聚合；增加或映射以下结构化字段：`logical_id`、`attempt`、`account_id`（按既有脱敏策略）、`recovery_action`、`failure_phase`、`retryable`、`public_output_started`、`synthetic_emitted`、`tool_commit_boundary`、`history_completeness`、`alias_state`、`remaining_attempts`、`remaining_deadline_ms`、`cooldown_wait_ms`、`final_outcome`、`delivery_status`、`billing_outcome`。禁止记录 prompt、正文参数、encrypted reasoning、附件内容、凭据或完整 control frame。

attempt 计数必须按真实 dispatch；`switch_count` 不能代替调用数。分别记录失败 attempt、最终失败逻辑请求和成功恢复逻辑请求；区分 `alias_pending`、`alias_published`、`alias_read_miss`、`alias_read_error`、`history_incomplete`、`turn_state_stripped`、`capacity_rejected`、`client_cancelled`、`deadline_exhausted` 等原因。Sub2 内部可记录上游重复成本为未知/已观测，不把它转换成 NewAPI 终端收费。

### 7.5 落地符号与职责对照

| 范围 | 既有基础 | 本轮落地 |
| --- | --- | --- |
| attribution/scope | `NewAPIAttribution`; `ctxkey.NewAPIUserID/NewAPITokenID/NewAPIOrigin` | `OpenAIRecoveryScope`；`openAIManagedRecoveryRequested` 验证可信归因 |
| alias/state | `OpenAIWSStateStore`; HTTP owner/account/egress 绑定 | `OpenAIRecoveryAliasCache` 与 service 发布/解析接口；Redis Lua 原子发布及容量门 |
| replay | 既有 continuation 路由约束 | `BuildOpenAIRecoverySnapshot`；`RebuildOpenAIRecoveryContinuation`；原始请求不可变、纯文本白名单 |
| protocol/output | `StreamScannerHandler`；Responses/Chat 原生与转换器 | Sub2 `OpenAIRecoveryStreamWriter`；NewAPI `recoveryV1State`；唯一逻辑终态与最多 45 个控制帧 |
| accounting | `ResponsesUsageAccumulator`；`responsesToolUsage`；`BillingSession` | 各入口按 attempt 重建计量；terminal usage 完整替换；winner-only fallback |
| deadlines/cancel | `takeOpenAIModelDispatch`；HTTP transport/cache；scanner | 每真实 dispatch 首输出 guard；NewAPI `ManagedRecoveryRequestTimeout`/`ManagedRecoveryDeadline` 固定传输期限与直接取消 |
| headers | `copyCodexSSEHeaders`；现有 turn-state echo 防护 | alias 解析/跨号重建时剥离旧 `X-Codex-Turn-State`；不伪造已交付的 winner 头 |
| strict/tool schema | 既有请求工具声明 | `OpenAIRecoveryStructuredFormat`、`ValidateOpenAIRecoveryStructuredOutput`、`ValidateOpenAIRecoveryToolArguments`；禁外部 schema 读取 |

### 7.6 最小验证命令与门槛

本轮实际定向命令和结果集中在第 10 节，避免把审查时的候选测试名当作已执行证据。检查覆盖受管握手、无握手不泄露、attempt reset、winner-only usage、alias 发布/解析、turn-state strip、工具/结构校验、取消与时限；跨仓 fixture 使用真实 Sub2 writer 字节重放至 NewAPI 原生、双向转换和 SSE→JSON 聚合入口。

代码级回归不能代替真实 SDK/Codex 客户端、Redis 实际配置/容量故障、TTFT 和资源分位的发布验收。无 reported usage 时必须只估算 winner 正文；旧失败正文和 synthetic 提示只参与展示，不回填计量或恢复历史。工具搜索调用同样遵守整轮校验与一次提交门。

## 8. 需求追踪

| 需求 | 实现主责 | 阻断验收 |
| --- | --- | --- |
| R01 | C、D | NewAPI 仅一次用户结算；Sub2 不新增终端收费 |
| R02 | B | 普通文本 TTFT 不等整轮完成 |
| R03 | A、B | 输出后 429/HTTP2/overload 有真实下一 dispatch |
| R04 | A | 11 次真实调用、900 秒绝对时限不可重置 |
| R05 | B、C | 中间失败/终态不关闭 logical stream |
| R06 | B、C | Responses/Chat 增量与最终聚合一致 |
| R07 | A、D | 绑定同号恢复与完整历史门 |
| R08 | B、D | 失败轮工具不执行；已执行动作不重复 |
| R09 | B、C | 严格结构有效且不混入 marker |
| R10 | A、B | 限额正常结束保留真实状态 |
| R11 | A | 冷却等待；永久/策略拒绝不被切号绕过 |
| R12 | C、D | 可信 scope 隔离；日志脱敏；容量有界 |
| R13 | A、B | 取消、逻辑期限、单轮超时分别验收 |
| R14 | E | 故障注入、目标 SDK、性能门槛通过后才声明完成 |

## 9. 交付判定

每个入口单独启用，不以一种 SDK 的最终 JSON 正确代替增量解析和工具执行验收。目标客户端不能处理可见重生、alias/turn-state 不闭环、工具可重复执行、JSON/schema 损坏、预算可重置或内存/磁盘无界，任一项出现即阻断该入口上线。性能测量至少分开普通文本首字、工具可执行时间、恢复总耗时、CPU/内存/I/O 和取消率；没有测量不宣称性能最优。

## 10. 本地实施、审查与验证记录

### 10.1 已落地代码

本轮先由恢复架构、流式协议、NewAPI 集成三个角色审查方案和开发规格，修订后分工实施，再交叉检查其他角色的代码。第 3 节的旧行号与“现有/拟新增”保留为基线问题说明，不代表当前源码仍处在未开发状态；当前实现见下表。

| 实施包 | 主要代码 | 本地已实现行为 |
| --- | --- | --- |
| A：Sub2 编排 | `openai_managed_recovery.go`、两种 HTTP handler、`openai_retry_budget.go`、调度器 | 统一 11 次真实发送/900 秒逻辑预算、首轮预留、同号重新准入/切号/冷却等待、取消与超时分类；受管模式忽略旧 retry window |
| B：Sub2 交付 | `openai_recovery_stream_writer.go`、Responses/Chat/raw/passthrough/CC pipeline、first-output guards | 普通文本实时交付、可见重生、唯一终态、每次真实发送独立首输出计时；工具、密文、严格结构局部校验后提交 |
| C：NewAPI | `recovery_v1.go`、`api_request.go`、raw/转换/聚合 handlers、`stream_scanner.go`、HTTP transport/cache | 双向握手、45 控制帧边界、winner-only 计量、显式零 usage、930 秒固定传输上限、用户取消立即传递；受管连接池与普通请求隔离 |
| D：alias/历史 | `openai_recovery_alias.go`、`gateway_cache_openai_recovery_alias.go`、`openai_recovery_history.go` | 可信 scope、成功前原子发布、TTL/容量门；展示转录与成功历史分离，完整纯文本历史才可换号重建 |
| 参数/结构 | `openai_managed_reasoning_effort.go`、`openai_recovery_structured.go` | 选号后按实际模型证据应用显式 effort 映射；JSON/schema 与工具参数校验，禁止外部 schema 读取 |

严格 schema 复用 `github.com/santhosh-tekuri/jsonschema/v6 v6.0.2`，固定到 `go.mod/go.sum`，没有自制不完整的 schema 解释器。NewAPI 新增 `managed_recovery_v1` 渠道字段，默认关闭；此处没有启用任何生产渠道。

### 10.2 追加审查修复

- NewAPI HTTP 总超时、响应头等待和 scanner 不再被旧 300 秒窗口提前终止；deadline 固定到原请求起点，数据与重试不续期。受管请求取消不进入普通请求的延迟 drain。
- 同一 `Forward` 内的兼容重发在真实 dispatch 重新启动首输出计时；保持逻辑与首轮绝对期限。marker observer 期间发生取消也会在发送前再次检查。
- history builder 使用不可变原始 input，重复内容仍按客户端语义追加；不从已重建 body 再次累计全历史。
- alias 先完成结构/工具校验，再原子发布；读取失败、过期、scope 错误不透传代理 ID。选号后和准入刷新后都核对 winner 账号、出口及凭据能力。
- 旧真实 `previous_response_id` 遇到 session-blocked 时也必须有完整可移植 snapshot 才能删除 ID 重建；缺失历史明确返回 `continuation_history_missing`，不把半段上下文当新会话重发。
- `json_object/json_schema` 四种 wire 模式的空 completed 结果在发布 alias 前拒绝；合法完整工具调用与明确 refusal 单独处理，不把拒绝伪装成空 JSON 成功。多 choice 与前后 attempt 的豁免状态相互隔离。
- 第 11 轮输出后废弃再最终失败的最大控制数为 45，双方解析器与 fixture 使用同一边界。
- Responses 同一 message 的多个 content parts 各自维护 text/refusal 与闭合状态；失败时先闭合旧部分，再发独立重生成说明，不能借用前一 part 的 done 标志省略后续闭合，也不能把说明混入 refusal。
- `output_text/refusal.done`、非空 `content_part.added/done`、message `output_item.added/done` 及终态快照统一补齐尚未交付的精确后缀，再按顺序闭合。普通文本仍即时交付；strict JSON 的校验内容、可见内容标志与 winner 历史来自同一轮完整内容，不能因缺 delta 将合法快照误判为空。
- NewAPI refusal 保留原生字段与 content part 边界；partial delta 后 done 补后缀、重复 done 去重。流式展示持续，缺 usage 的计量状态按 attempt 独立重建，只纳入 winner 的实际文本/refusal，排除失败轮和网关提示。
- 质量复核将 Sub2 选号等待限制为可恢复的无账号、出口容量或 429 冷却错误；数据库/调度内部错误立即保留原错误，不借暂排时点等到总期限。流式 writer 每轮重置可见性与 choice，零输出失败轮不重复提示；Chat 可见 reasoning 计入本轮分段；分块 SSE 增量扫描从尾部保留分隔符前缀，不随长帧分块反复全量搜索。
- NewAPI 只在 Sub2 返回 2xx 且双向协议头回显成功后设置恢复 owner；此前的原始上游 429、缺回显、传输失败继续由 NewAPI 既有外层预算处理。受管 Sub2 握手前真实 429 即使全局状态范围移除 429，或对外状态被映射为 400/200，仍按原始上游状态重试；未证实来源的本地 429 不获得此例外。取消、本地策略、固定渠道、跳过重试和预算门保留；握手前 ping 仅为 SSE 注释，可继续保活且不误阻外层重试。只有已握手的 Sub2 承担后续流内恢复，普通渠道的重试政策不变。真实可见 refusal delta 纳入首字指标，不计 synthetic 提示。

### 10.3 已执行的定向验证

以下为实际通过的命令记录，不是要求重新执行的全仓清单。Sub2 命令工作目录为 `/Users/tokk/Documents/Codex/2026-08-09/li/work/sub2api/backend`；本机离线执行部分命令时使用已缓存的 Go 1.27.3 二进制及 `GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off`，未更改仓库工具链配置。

```sh
go test ./internal/service -run '^TestOpenAIRecoveryHistory' -count=1
go test ./internal/service -run '^TestOpenAIManagedReasoningEffort' -count=1
go test ./internal/service -run 'TestOpenAI(Managed|ModelDispatchBudget|RetryBudget)' -count=1
go test ./internal/config -run 'Test(OpenAIRecoveryConfig|LoadDefaultOpenAIResponseTimeouts|LoadOpenAIFirstOutputTimeoutsFromEnv|ValidateOpenAIBudgetCompatibility|EffectiveOpenAIBudgets)' -count=1
go test ./internal/service -run '^TestOpenAIManagedFirstOutput' -count=1
go test ./internal/service -run '^(TestOpenAIModelDispatchBudget|TestOpenAIRetryBudgetDeadline|TestOpenAINativeTextFirstOutput|TestOpenAIForwardFirstOutputTimeoutIncludesResponseHeaderWait|TestOpenAIForwardNonStreamingTextTimeoutSwitchesBeforeCommit|TestOpenAINonStreamingTextRejectsResponseAfterDeadline|TestOpenAIChatTextDeadline|TestNewOpenAIFirstOutputTimeoutErrorRecordsCallerProxyAttribution)' -count=1
go test ./internal/service -run 'TestOpenAIRecoveryStreamWriter' -count=1
go test ./internal/service -run '^(TestOpenAIRecoveryStreamWriterDoesNotMarkInvisibleRetry|TestOpenAIRecoveryStreamWriterParsesSplitSSEDelimiters|TestOpenAIRecoveryStreamWriterResponsesRegeneratesWithOneLogicalTerminal|TestOpenAIRecoveryStreamWriterChatKeepsOnlyWinnerFinishAndUsage|TestOpenAIRecoveryStreamWriterStructuredUnitStaysPrivateUntilValidated)$' -count=1
go test -tags unit ./internal/service -run '^(TestOpenAISilentRefusalDetector|TestOpenAIExplicitRefusalWithoutUsageDoesNotFailOver|TestForwardAsRawChatCompletions_SilentRefusal|TestHandleChatStreamingResponse_SilentRefusal)' -count=1
go test -tags unit ./internal/handler -run '^(TestOpenAIManagedSelectionDoesNotWaitOnInternalError|TestOpenAIManagedRecoveryStopsBeforeDeliveryReserve)$' -count=1 -timeout=90s
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off /Users/tokk/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.27.3.darwin-arm64/bin/go test ./internal/service -run '^TestOpenAIRecovery(StructuredOutput|ToolArguments)$' -count=1 -timeout=60s
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off /Users/tokk/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.27.3.darwin-arm64/bin/go test -tags=unit ./internal/handler -run '^TestOpenAIManagedLegacyContinuationDoesNotDropMissingHistory$' -count=1 -timeout=60s
GOTOOLCHAIN=local GOPROXY=off GOSUMDB=off /Users/tokk/go/pkg/mod/golang.org/toolchain@v0.0.1-go1.27.3.darwin-arm64/bin/go test -tags=unit ./internal/handler -run '^TestOpenAIManaged' -count=1 -timeout=90s
```

上列均 PASS。前序 handler 检查为 6.131 秒，覆盖真实同号两次再切号、11 次发送耗尽、别名续聊与缺历史停止；首输出新增用例无分钟级 sleep。前序 writer 回归为 0.067 秒，覆盖多 content part 以及七类正文入口的补齐/闭合；新增定向检查覆盖零输出失败轮、可见 reasoning 与长帧分隔符。明确 refusal 检查为 0.075 秒；选号内部错误不等待的两项 handler 定向检查为 0.063 秒。alias/cache 的隔离、原子发布、TTL、容量及纯文本 history 定向检查已在前序实现阶段通过。新增 legacy 回归最初因测试夹具未配置异步 cyber 计费依赖而失败；仅在该路由夹具标记审计已记录以隔离无关异步计费后，失败项及全部受管 handler 回归均通过，没有为测试削弱生产校验。

NewAPI 命令工作目录为 `/Users/tokk/Documents/Codex/2026-08-04/s-y-s-s/new-api-kkai`：

```sh
go test ./relay/channel ./relay/channel/openai ./relay/helper ./controller ./service -run 'TestManagedRecovery|TestRecoveryV1|TestOaiResponses|TestOaiChatToResponses|TestOaiStreamHandler|TestOpenAIStreamHandlers|TestResponsesHandlersBillActualToolCalls|TestChatHandlersBill|TestStreamScannerHandler|TestShouldRetryStops|TestGenerateTextOtherInfo|TestDoApiRequest|TestUpstreamPolicy|TestDurableImageRequest|TestHTTPClientCache|TestHTTPTransportPolicy|TestNormalizeHTTPTransportPolicy|TestAutoOneShard|TestFourShardHTTP2|TestForcedHTTP1|TestInvalidateProxyClient|TestResetProxyClientCache|TestCloseIdleConnectionsRedials' -count=1 -timeout=120s
go test ./relay/channel/openai ./service -run '^(TestManagedRecoveryRefusalDoneUsageIsWinnerOnly|TestManagedRecoveryUnreportedUsageEstimatesWinnerOnly|TestResponsesUsageAccumulator)' -count=1 -timeout=90s
go test ./controller -run '^(TestManagedRecoveryPreHandshakeErrorsKeepNewAPIRetry|TestManagedRecoveryOwnerDoesNotReceiveAnotherRelayRetryBudget)$' -count=1 -timeout=90s
go test ./relay/channel -run '^(TestManagedRecoveryProtocolRequiresSignedChannelAndResponseEcho|TestManagedRecoveryContextKeepsOriginalDeadlineAndParentCancellation|TestManagedRecoveryCancelsSub2AfterHeadersWithoutWaitingForPolicyDrain)$' -count=1 -timeout=90s
go test ./relay/channel/openai -run '^(TestRecoveryV1RealTextMetricIncludesVisibleRefusal|TestRecoveryV1RealTextMetricExcludesControlsAndSyntheticText)$' -count=1 -timeout=90s
```

前序五包检查均 PASS，覆盖原生/双向转换/聚合、握手与计量、固定 deadline、真实 HTTP 取消、受管连接池隔离及非受管既有行为。最新 refusal 补全与计量检查的两个包分别 PASS（0.047 秒、0.061 秒），覆盖 partial delta 后 done、重复 done、第二 refusal part，以及失败轮/网关提示排除。质量收敛后的 controller 两项检查 PASS（0.076 秒），含真实上游 429 经状态码映射到 400/200、ping 已提交、普通渠道、未证实来源的本地 429 与握手 owner；channel 三项检查 PASS（0.056 秒），拒绝回复首字两项检查 PASS（0.037 秒）。

转换器收尾命令在 NewAPI `relaykit/` 下执行：

```sh
go test ./relayconvert/internal/oai_responses -count=1 -timeout=90s
go test ./relayconvert -count=1 -timeout=90s
```

分别 PASS（0.011 秒、0.025 秒）。最终跨项目命令在 NewAPI 根目录执行：

```sh
python3 relay/channel/openai/testdata/sub2_recovery/capture.py --sub2 /Users/tokk/Documents/Codex/2026-08-09/li/work/sub2api
go test ./relay/channel/openai -run '^TestManagedRecoveryCapturedSub2' -count=1 -timeout=90s
```

capture 与 replay 均 PASS（首次实际 writer 运行 0.066 秒、NewAPI 回放 0.062 秒；质量修复后按最新 writer 重新捕获及回放再次通过，最终回放 0.060 秒）。capture 通过 Go overlay 运行真实 Sub2 writer，使用合成测试消息，不访问线上流量、不修改 Sub2 源码；结果位于 NewAPI `relay/channel/openai/testdata/sub2_recovery/`。15 个 fixture 与 3 个 producer 源文件的 SHA-256 全部匹配 manifest；最终 writer SHA-256 为 `a01fe3aa05291ac9d7a9e5cd6b334d148b14f9baac3784e5ede5e340286d3c40`。重放覆盖原生 Responses/Chat、双向流转换、Responses SSE→Chat JSON、done-only/partial/multipart refusal 与 item 快照、winner-only 缺 usage 估算、废弃 reasoning 关闭、45 控制帧及耗尽终态。

两仓 `git diff --check` 已通过；已改动/新建 Go 文件及 capture Go 模板的 `gofmt -l` 无输出，新文件空白检查通过。两份文档的 JSON 示例、相对链接和尾部空白检查通过。独立终审复读本轮修复后未发现新的代码阻断。没有运行全仓测试、生产镜像构建、提交、代码推送、部署或生产配置变更。

### 10.3.1 隔离故障与本地 HTTP 验收

本轮只用合成请求、`httptest`、`miniredis` 和独立的本机回环 Redis；没有连接生产服务或向线上用户注入故障。Sub2 在 `backend/` 下执行：

```sh
go test -tags unit ./internal/handler -run '^TestOpenAIManagedResponsesRecoversFromTransientFailures$' -count=1 -timeout=90s
go test -race -tags unit ./internal/handler -run '^TestOpenAIManagedResponsesRecoversFromTransientFailures$' -count=1 -timeout=90s
go test -race -tags unit ./internal/handler -run '^TestOpenAIManaged(ChatRecoversAfterVisibleText|ChatStopsAtSharedDispatchLimit|ResponsesRecoversAndRebuildsContinuation)$' -count=1 -timeout=90s
go test ./internal/repository -run '^TestOpenAIRecoveryAliasCache' -count=1 -timeout=90s
go test ./internal/service -run '^(TestOpenAIRecoveryStreamWriterResponsesRegeneratesWithOneLogicalTerminal|TestOpenAIRecoveryStreamWriterParsesSplitSSEDelimiters|TestOpenAINativeTextFirstOutputCapsEveryEffort|TestOpenAIManagedFirstOutputStartsAtEachPhysicalDispatch)$' -count=1 -timeout=90s
```

全部 PASS（依次 5.139、6.272、7.267、0.124、0.100 秒）。新增故障用例真实调用 Sub2 handler，在首字前和输出后分别注入 429/500，四种情况均按账号 `1,1,1,2` 完成恢复；公开流只含一次成功终态，输出后故障才显示重生成提示，alias 快照仅含胜出轮。

本机已有缓存的 `redis:8.6-alpine` 镜像被临时运行在 `127.0.0.1:64252`，无卷挂载。只在该临时实例上检查 `maxmemory-policy=noeviction`，并执行：

```sh
KKAI_TEST_REDIS_ADDR=127.0.0.1:64252 go test ./internal/repository -run '^TestOpenAIRecoveryAliasRealRedisRoundTrip$' -count=1 -timeout=30s -v
KKAI_TEST_REDIS_DOWN_ADDR=127.0.0.1:64252 go test ./internal/repository -run '^TestOpenAIRecoveryAliasDisconnectedRedisFailsClosed$' -count=1 -timeout=30s -v
```

分别 PASS（0.076、1.058 秒）。前者验证原子发布、冲突不覆盖、读取和删除；后者在停止容器后验证读写失败关闭。临时容器及卷已检查无残留。环境变量门限定为 `127.0.0.1`，默认不连接任何 Redis。

NewAPI 根目录执行：

```sh
go test ./relay/channel/openai -run '^TestManagedRecoveryCapturedSub2StreamsOverLocalHTTPBeforeTerminal$' -count=1 -timeout=30s
go test ./relay/channel/openai -run '^TestManagedRecoveryCapturedSub2' -count=1 -timeout=90s
```

分别 PASS（0.039、0.061 秒）。新增测试以两个本地 HTTP 服务把已捕获的真实 Sub2 writer 流经过实际 `DoApiRequest` 握手和 Responses handler 交给标准库 HTTP 客户端；上游终态由 barrier 扣留时，客户端已经读到首段文本，之后再收到独立重生成提示和胜出答案。内部控制帧不公开，只有一个完成终态，winner usage 为 5。这个用例没有启动完整 NewAPI controller/数据库计费路由，也不是实际 Codex/SDK 客户端。

### 10.4 尚未完成的验收与启用条件

1. 真实目标 SDK/Codex 客户端的增量、最终聚合、alias/turn-state 续聊和工具执行验收尚未完成；fixture 重放不能代替客户端产品行为。
2. 普通文本 TTFT、工具可执行时间、恢复总耗时及 CPU/内存/I/O 分位未实测，600/300/15 秒与容量初值不能宣称已是性能最优。
3. 本地临时真 Redis 已验证 `maxmemory-policy=noeviction`、alias 读写与断连失败关闭；仍未读取生产 Redis 的策略、实际 TTL/可用容量，也未做目标生产 Redis 的故障注入和容量验收。开关默认关闭，缺这些证据不得启用。
4. 只实现有界纯文本快照的跨账号重建；工具/附件/opaque 输入不被伪造为可移植历史。入站直接带 `encrypted_content` 的 Responses 能力门仍保留；旧会话缺历史仅保留合法原号路径，账号/出口能力不满足时明确停止。
5. 不新增代理 alias 的 `GET/DELETE /responses/:id`，不宣称原生 WS、WS bridge、Anthropic、图像/视频或其他供应商已覆盖。

本地代码完成与生产发布是两个阶段。任何发布须另按两仓维护中的 runbook 完成相应验收，本次文档与代码变更不构成上线证据。
