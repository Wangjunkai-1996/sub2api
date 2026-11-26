# Sub2API v0.2.22 合并评审与执行方案

评审日期：2026-11-17（Asia/Shanghai）。更新：2026-11-18（Asia/Shanghai）。状态：方案已落地并完成本地合并；兼容性修正、账务/rollup 集成和 322→324 迁移兼容性已完成定向验证，未构建或部署。

**推荐：以官方稳定 tag v0.2.22 为新 release 基线，使用当前 KKAI HEAD 做受审阅的三方合并，再逐项保留、替换或退休定制。** 官方提交作为第一父，当前定制作为另一父。采用官方完整版本及其测试，不重放全部历史补丁；对共享缓存、迁移、流式输出和计费的语义交叉点人工处理。

这是当前源码证据下的最佳集成路径，但不是“新版本已测试通过”或“可立即上线”的结论。最终执行必须优先证明两份新迁移、新旧实例共享账本，以及共享缓存的真实混跑兼容性；不能因为历史上曾有 fork 版本号就默认保留一层桥接。

2026-11-18 后续修正：移除 KKAI fork 的“选中 Pro OAuth 账号后才审核”路径、`plan_type=pro` 分类器及其专用测试；OpenAI HTTP 入口统一回到官方的账号选择前审核。该修正仅在本地工作区完成，尚未提交、构建或部署。

## 1. 冻结输入与实际规模

| 项目 | 已核实的事实 |
| --- | --- |
| 当前仓库 | `/Users/tokk/Documents/Codex/2026-08-09/li/work/sub2api` |
| 合并后分支 | `release/v0.2.22-kkai` |
| 合并提交 | `b67bc7b190f42fe1ce922e0f2668372392fa0c79`（第一父官方 `b0fa9f3e6eeab1968a53e1289b69daa4bb40ca2b`，第二父 KKAI `855bdb83bc422e4385938846b2b9a64864559a84`） |
| 当前官方基线 | `v0.2.21` → `10ba8f9b4dc2e6023d4a9879ab77b21841babd10` |
| 新官方稳定版 | [v0.2.22](https://github.com/Wei-Shaw/sub2api/releases/tag/v0.2.22)，北京时间 2026-11-17 10:48:12 发布，非预发布 |
| 新官方提交 | `b0fa9f3e6eeab1968a53e1289b69daa4bb40ca2b` |
| 本地定制净差异 | 相对 v0.2.21：564 文件，+84,679 / -11,793 行 |
| 差异构成 | 测试 272 文件；Ent 生成物 36 文件；其余 256 文件（包含 schema、文档等） |
| 官方版本增量 | v0.2.21 → v0.2.22：340 文件，+20,708 / -1,652 行，187 个可达新增提交（含合并提交） |
| 双方改动交集 | 79 个文件；机械合并预演有 16 个冲突文件 |
| 工作区初始状态 | 干净；合并后工作区已清理并保持干净 |

runbook 记录的 2026-11-11 源码为 `4d32760b5`。合并提交的第二父为 `855bdb83b`，保留了图片明确失败证明修复。本轮未访问生产，不据旧发布记录宣称当前线上版本、颜色或健康。

用户明确授权后，已通过 `127.0.0.1:7897` 单独读取官方发布信息并抓取精确 v0.2.22 tag，没有抓取 main、推送代码或切换分支。代码托管同步与未来生产交付分开。

## 2. 三角色评议及取舍

| 角色 | 核心判断与反方挑战 | 最终纳入方案 |
| --- | --- | --- |
| 定制架构评审 | 564 文件不适合手抄；历史存在引入又撤销的功能。自动合并成功的新入口也可能绕过定制守卫。 | 使用三方合并保留净状态；按入口和调用链审阅，兼查无文本冲突区域；保持退休功能删除语义。 |
| 数据与安全评审 | 初看本地与官方 auth snapshot 都叫 28，需核对真实读写者；迁移无文本冲突不代表无需执行数据库变更。 | 默认采用官方 auth v28；仅发现真实 native-28 不完整写者时才增加临时桥接。冻结历史迁移；迁移授权、混跑和回滚证明先于候选启动。 |
| 测试与发布评审 | 新计费队列、快照复用和 rollup 是相互依赖的变化，少量摘取不能代表完整升级；Mac 通过不能证明 Linux socket 行为。 | 保留官方成组实现与测试，增加 KKAI 组合回归；分层验证，正式发布仍按 runbook 门禁。 |

| 备选路径 | 判断 |
| --- | --- |
| **新稳定 tag + 受审阅三方合并 + 最小语义修正** | **首选。** 来源可追溯、减少遗漏、可退休重复补丁，且延续上一轮官方为第一父的集成结构。 |
| 当前 release 直接 pull/merge 官方 main | 不选。混入非固定发布边界，也不符合升级手册从稳定 tag 建立新基线的要求。 |
| 所有定制历史整体 rebase/cherry-pick | 不选。会重走 managed recovery 等已引入又撤销的状态，增加冲突与复活旧功能风险。 |
| 官方源码覆盖，再手工重做全部定制 | 不选。遗漏出口池、历史迁移、生成字段和隐含安全合同的风险高。 |
| 仅摘官方少量修复 | 只适合另行限定范围的紧急热修复，不作为本次完整官方升级。 |
| 因新版本刚发布而无条件等待 | 不作为默认方案。先完成可审阅的本地集成与验证；是否上线取决于兼容性证据，而非仅看发布日期。 |

执行中采用一条强制判定：**官方完整覆盖的行为只保留官方实现；官方部分覆盖的行为只移植缺失的最小守卫；官方没有覆盖且属于 KKAI 业务合同的行为才保留本地实现；历史 SQL 只作为账本证据保留，不等于保留旧运行时。** 每项运行时定制或历史迁移差异都须记录其归类理由及验证证据；退休重复实现时保留对应行为测试。

## 3. 具体合并决策

### 3.1 鉴权缓存：默认采用官方 v28，桥接只作临时兜底

复核 `backend/internal/service/api_key_auth_cache.go` 与 `api_key_auth_cache_impl.go` 的精确 diff 后，官方 v0.2.22 与本地快照的字段、序列化和还原映射已经相同，官方只是增加 `ForceOpenAIUltrafast`。当前 `855bdb83b` 的生产写入仍是 `Version=25, CompletenessVersion=28`；未发现真实生产代码写入“native wire 28、旧完整性语义”的旧作者。

因此最小方案是：采用官方 v28 的读写和完整性逻辑，删除本地 `CompletenessVersion`/bridge 分支中已经被官方覆盖的重复实现，只保留真实业务字段和失效行为测试。执行前必须用实际保留的旧二进制验证：旧 `25/28` 写入→新节点回源重建；新官方 v28 写入→旧节点可读；所有字段（含 KKAI 策略和 `ForceOpenAIUltrafast`）往返；更新/撤销正确失效。

只有在执行阶段发现无法界定的真实 native-28 不完整写者或残留缓存，才临时采用 `CompletenessVersion=29`、wire 28 的桥接，并把它作为有退出期限的兼容补丁，不能把它固化成新的长期架构。不能保留“官方 28 必须拒绝”的失效断言，因为它不符合当前精确源码证据。

新增强制 Ultrafast 开关在混跑和依赖旧版语义回滚期间保持默认 false。**关掉新开关不代表全部价格一致**：客户端原本就可以请求 ultrafast，新版修正 Sol 的 6 倍计价。需单独验收客户端 tier、最终模型、Fast/free-Fast、自定义价格和回滚时的计费差异，明确混跑期间处理方式。

### 3.2 迁移：冻结 322 条，审查新增 2 条

当前 322 个 SQL 的完整文件名、Git blob、原始 SHA-256 与 runner trimmed SHA-256 已保存到 `.codex/upgrade-review-20261117/migration-ledger-baseline.json`。其中含 20 个自有 SQL 和 8 条历史上游文件名映射；旧映射不能恢复成官方原名。

v0.2.22 新增：

- `251_group_force_openai_ultrafast.sql`：默认 false 的分组字段。
- `251_group_usage_rollup_nonblocking_build.sql`：增加 `expired_before`、`building_until`、`build_dirty_from`，替换汇总失效函数和锁协议。

两者均不与当前 SQL 完整路径相撞；同为 251 前缀与已有 `251_group_i18n.sql` 可以共存。runner 按完整文件名识别和字典序执行（`migrations_runner.go:176`、`:203`），不能因编号相同重排旧账本。预期候选集合为 324 条，仍需验证实际执行顺序及前置依赖。

保持旧 322 条字节不变，包括已退休功能的历史清理 SQL 和 `246_channel_reasoning_effort_multipliers.sql` 的旧计费键兼容。新增 migration 必须单独制定并获得执行授权；本轮不执行。`ent.go:133` 启动就会跑迁移，不能先启动生产候选再检查。

门禁：322→324 升级、历史 checksum/applied_at 不变、重复执行无副作用、旧二进制在新 schema/函数下运行、真实业务回滚兼容。正式镜像保留 `-migration-contract`；普通发布遇到 pending、缺文件或未知 checksum 立即停止。

### 3.3 日汇总：官方新协议整体替换旧 fork 补丁

以官方“登记→计算→发布、过期下界与脏区间”整体实现替代本地旧清理预锁/锁序补丁。迁移、触发器、官方 rollup repository、清理与分区逻辑必须成组采用；旧的 `cleanupUsageLogsBatchWithRollupInvalidation`、`dropUsageLogsPartitionWithRollupInvalidation`、`lockGroupUsageRollupState` 及其锁序修补直接列为删除项，不把旧 `FOR UPDATE` 机械塞回新实现。

保留并调整既有死锁用例，证明新协议下写入不被长时间阻塞、迟到更新/删除不丢失、保留期边界正确。增加旧节点清理与新节点汇总/分区删除同时运行的验证。官方 3 秒分区锁等待只是退出机制，不是混跑安全证明。

### 3.4 调度：官方单点加载，注入最小 KKAI 守卫

采用官方 `GetSnapshotValues`、bucket singleflight、完整配置池和按读取时刻过滤限时冷却的结构。直接退休本地 `fallbackLoadSF`、`newSharedFallbackContext`、`loadSchedulableAccountsFallback`、`readFallbackSnapshot` 这一整套重复 singleflight/fallback 实现，不能让两套并行存在。

只在官方 `decodeCachedAccountPayload`/`GetSnapshotValues` 入口接入 KKAI 必需的 metadata projection 标记、EgressMode/Revision/Bindings、凭据脱敏、revision/cooldown 最终准入及 previous_response_id 的 account+route 约束。沿用官方缓存结构，验收其 10 秒直接复用、20 秒宽限和只读共享母本在本地准入规则下的正确性；请求不得原地修改母本或污染另一请求的 SelectedEgress。只为验证发现的缺口补最小逻辑，不提前设计新适配框架。

### 3.5 流式与身份：官方状态机单点实现，补齐本地合同

以官方 v0.2.22 的 keepalive 直发、首输出 staging、Messages `held/emit/releaseHeld`、failed-frame passthrough 作为唯一状态机，删除本地重复的 keepalive/event 分支。只保留有明确业务合同的最小守卫：120 秒 usable-output deadline、11 次总派发预算和取消约束、符合条件的 Messages 输出前读取失败换号、response routing/ID 先绑定后输出及 replay boundary、855 图片失败证据协议。官方已覆盖的“是否写过 keepalive”“是否已进入 SSE”判断不再维护第二套 `Written` 语义；传输错误不伪装成真实上游 failed/error 帧。

应覆盖 keepalive→换号→成功/错误、结构帧后超时、客户端取消、审核拦截和凭据故障。不能将“尚无正文”当成“可任意重放”，也不能将心跳字节当成已交付业务结果。

继续保留 NewAPI HMAC 归属、nonce 防重放、Cyber 用户/会话隔离和租约释放。OpenAI Responses、Messages、Chat Completions 统一在账号选择前执行官方 Prompt Audit；删除 fork 的“选中 Pro OAuth 账号后审核”分支及 `plan_type=pro` 分类。官方保留客户端 Chat `metadata.user_id`，本地是账号 opt-in 的可信 KKAI 身份注入，二者不等价，不退休本地实现。

### 3.6 图片与账务：保留 855b 协议，采用官方批处理并补组合验证

完整保留 `855bdb83b` 的失败证据协议：完整 UTF-8/无重复键 JSON、确定零图片、跨尝试未知结果的粘滞否决、已有图片优先、保留码防伪及错误脱敏。未证明的 OAuth/SSE/已派发结果不能因新 failover 流程获得重试或退款证明。专用图片入口零实际输出零费用；混合请求保留真实 token 费用。

采用官方 UsageBillingLaneCombiner、ApplyBatch、事务去重和失败回退作为一个完整变化。增加旧 Apply 与新 ApplyBatch 同用户/同 request ID 并行、配额临界值、提交结果未知、归档去重以及图片 hold/capture/release 交错的集成覆盖。

退出顺序也是验收点：官方把 BillingCache、UsageRecordWorkerPool 和新增 lane 的 Stop 放入并行组；当前缓存停止后会拒绝入队，部分调用有同步回退，不能仅凭这一点断言资金丢失。先用关停测试证明是否存在未完成结算或缓存同步丢失；只有出现可复现缺陷时，才在 `wire.go` 源定义中修复对应依赖并重新生成 `wire_gen.go`，不预先保留一套本地 shutdown 编排。此项是静态发现的待验证风险，未声称已复现故障。

### 3.7 生成物、工具链与已退休能力

Ent 从合并后的 schema、Wire 从合并后的 provider/cleanup 源定义生成，不能整文件选 ours/theirs。官方 Go 与 Dockerfile 同步升为 1.27.4，应保持一致；jsonschema-go 是同版本从 indirect 转 direct，未发现本次引入新版本。前端 package/lockfile 无版本增量。

新版 tag 内 `backend/cmd/server/VERSION` 仍为 `0.2.21`，不能据它推断镜像版本。正式构建使用 release wrapper 显式传入新版 version 和精确 commit，并核验镜像 `-version`。

保持已退休的暖机、主动 STATE 票据、分组调度覆盖、整段原子流缓冲和 managed text recovery 的删除语义，以及旧输入拒绝/过滤。不从老补丁链重新引入。

## 4. 执行顺序及通过条件

1. 已冻结上述旧 HEAD、新 tag 和官方基线，并在 `release/v0.2.22-kkai` 完成本地合并。
2. 已在新基线上以当前精确 HEAD 完成 `--no-commit --no-ff` 合并，再提交双父 merge commit；auth/迁移/rollup、调度、流式/账务、前端与生成物均按本方案处理。
3. 已运行前端直接相关测试；本机已切换到 Go 1.27.4，后端服务、handler、repository 和 migration 定向组均通过。账务/rollup 集成、322→324 升级、历史 checksum 保持和旧 runner 兼容均已通过；auth v28 新旧节点缓存混跑、旧二进制完整业务兼容和生产回滚仍未完成正式演练。
4. 同时审查 `v0.2.22..候选` 和 `855bdb83b..候选`，确认官方增量没有漏接、必要定制没有丢失、退休功能没有复活。无冲突的 63 个双方改动文件也纳入语义审查。
5. 迁移演练与回滚兼容通过后，单独取得生产迁移执行授权。任何未解的数据兼容问题都阻断启动候选。
6. 完整发布请求下，再按维护手册从精确 commit 在 Mac 用 kkai-mirror-builder 构建，经控制器 plan-stage、10%→50%→100%→complete，每级至少 120 秒，保留真实业务结果、错误归因、即时/延迟状态和回滚证据。release_scope 根据两角色实际执行路径确认；新账务两角色均执行时为 both，不能仅因共享二进制就默认 both。

若官方 v28 缓存与旧版读写、或旧版在新 rollup 协议下的兼容性不能证明，暂停完整升级；此时只能另行收窄为有明确收益和独立依赖边界的热修复，不能把一个删减功能的混合版本标成完整 v0.2.22。

## 5. 合并后建议的定向验证（部分已执行）

这些是第一轮入口，不代表正式发布只需跑这几条。Go 命令工作目录为 `backend/`；执行前核对新旧测试实际名称与 build tags。全套或预计超过五分钟/时长未知的命令，按用户规则先说明准确命令、原因和预估并获批准；集成测试因 Docker 不可用而 skip 不算验证通过。

```sh
go test -tags=unit ./internal/service -run '^Test(APIKeyAuthSnapshot.*|UsageBillingLane_.*|OpenAIUltrafastGateMatchesBilling|InflightEstimate_AccountMappingNoDBAndBoundedMemory)$' -count=1
go test -tags=unit ./internal/service ./internal/handler -run '^Test(OpenAIGatewayHandlerImages_.*|ForwardOpenAIImagesAPIKey_.*|OpenAIImagesProvider.*|OpenAIHTTPStreamReplay.*|OpenAINativeStream.*|OpenAIMessages.*|ForwardAsAnthropic_(PreContent.*|ContentReleasedNeverFailsOver|StreamError.*))$' -count=1
go test -tags=unit ./internal/service ./internal/repository -run '^Test(SchedulerFallback.*|SchedulerCacheSnapshotMemo.*|SchedulerMetadata.*|AccountEgressConfigNamespace.*)$' -count=1
go test -tags=unit ./migrations ./internal/repository -run '^Test(V0218MigrationPreservesKKAILedger|GroupForceOpenAIUltrafastMigration|Migration251AddsExpiredFloorAndNonBlockingBuildMarks|MigrationContractUsesRunnerChecksums|EmbeddedMigrationContractExposesKnownCompatibility)$' -count=1
go test -tags=integration ./internal/repository -run '^Test(UsageBillingRepositoryApplyBatch_.*|UsageBillingLaneCombiner_ConcurrentRequestsOfOneUser|GroupUsageRollupSync.*|GroupUsageRollupRetentionCleanup.*|GroupUsageRollupTriggers.*)$' -count=1
go test ./internal/handler -run 'TestPromptAudit|TestRunSecurityAudit|TestOpenAINonAuditEntryPoints|TestOtherOpenAIEntryPoints|TestOpenAIWebSocketPreserves'
go test ./internal/server/routes -run 'TestEveryGatewayPOSTRouteIsClassifiedForPromptAuditCoverage|TestResponsesWebSocketDoesNotRunPromptAudit'
```

前端从仓库根按本次冲突及功能选择现有 spec，例如：

```sh
pnpm --dir frontend exec vitest run src/views/admin/__tests__/GroupsView.openaiForcedTier.spec.ts src/components/account/__tests__/AccountUsageCell.spec.ts src/components/account/__tests__/EditAccountModal.spec.ts src/components/account/__tests__/EgressPoolSelector.spec.ts src/components/account/__tests__/accountEgressPolicy.spec.ts
```

已补齐本轮发现的最小兼容修正：调度 metadata 序列化 helper、官方 `ListModelAvailabilityCandidates` 回源测试桩、快照宽限期测试边界、Messages 预内容读取错误的不可重放边界，以及图片失败证据/脱敏/透传响应合同。账务/rollup 集成和 322→324 升级兼容已有定向测试并已通过；auth v28 新旧节点缓存混跑、旧二进制完整业务兼容和真实生产回滚路径仍缺少证据，上面现有测试不能冒充这些覆盖。Linux TCP_USER_TIMEOUT 改动需隔离 Linux/amd64 验证，Mac 的通过结果不覆盖该 build-tag 路径。正式发布仍须执行 runbook 要求的修改包、受影响入口和构建验证。

既有验证缺口：`docs/kkai/image-failure-code-review-20261115.md:62` 记录 service 完整 unit 包因 `TestInflightEstimate_AccountMappingNoDBAndBoundedMemory` 内存阈值失败，隔离通过但原因未确认。新版未直接修改该失败测试，不能宣称已经修复，也不能为求绿直接放宽阈值。

## 6. 本轮实测与证据

- `git status --short --branch`、`git rev-parse HEAD`、`git rev-parse 'v0.2.22^{commit}'`：分支与精确对象已确认。
- `git diff --shortstat v0.2.21 v0.2.22`：340 文件，+20,708/-1,652。
- `git diff --shortstat v0.2.21..HEAD`：564 文件，+84,679/-11,793。
- `git merge-tree --write-tree --name-only v0.2.22 855bdb83bc422e4385938846b2b9a64864559a84`：退出码 1，16 个冲突文件；这是预演的正常冲突结果，不是通过或实际合并。仅生成 Git 对象，未改变索引、工作区代码或分支 ref。预演树 `a5ea83825db999d2289f209edb40d2cc8dad2b66` 含冲突标记，不能用于构建。
- Python 标准库只读集合/账本检查：79 个双方改动文件；322 条基线 SQL 已冻结；两条新 SQL 与旧完整路径无碰撞。
- `git diff --check`：通过；报告以 `git diff --no-index --check /dev/null docs/kkai/upstream-v0.2.22-merge-plan-20261117.md` 检查，无空白错误输出（no-index 的新增文件差异返回码 1）。
- 本轮已运行前端 4 个定向 Vitest 文件（177/177 通过）；后端定向组在 Go 1.27.4 下全部通过：service 调度/账务组、service+handler 流式/图片组、service+repository 调度缓存组、migrations+repository 迁移合同组。另对三处首次失败用例完成回归复测并通过，`gofmt -d`、`git diff --check` 和 `git diff --cached --check` 均通过。账务批处理/lane、group usage rollup、322→324 迁移升级、历史 checksum、旧 runner 和 auth/payment schema 集成用例均通过。未运行后端完整 package suite、全套 lint/typecheck/build；未编译镜像、访问生产、迁移或部署。候选功能验证、缓存混跑、旧二进制业务兼容和生产回滚证据仍待执行。
- 2026-11-18 审核路径收敛复测：删除 Pro 专属选中账号审核及分类器后，`go test ./internal/handler -run 'TestPromptAudit|TestRunSecurityAudit|TestOpenAINonAuditEntryPoints|TestOtherOpenAIEntryPoints|TestOpenAIWebSocketPreserves'` 与 `go test ./internal/server/routes -run 'TestEveryGatewayPOSTRouteIsClassifiedForPromptAuditCoverage|TestResponsesWebSocketDoesNotRunPromptAudit'` 均通过；`gofmt`、`git diff --check` 和残留引用检查通过。完整测试套件仍未运行。

原始证据保存在 `.codex/upgrade-review-20261117/`：`upstream-latest.json`、`overlap-files.txt`、`merge-tree-v0222.txt`、`migration-ledger-baseline.json`。

16 个冲突文件如下：

```text
backend/cmd/server/wire_gen.go
backend/internal/handler/openai_gateway_handler.go
backend/internal/repository/dashboard_aggregation_group_usage_test.go
backend/internal/repository/dashboard_aggregation_repo.go
backend/internal/repository/integration_harness_test.go
backend/internal/repository/scheduler_cache.go
backend/internal/repository/usage_cleanup_repo.go
backend/internal/repository/usage_cleanup_repo_test.go
backend/internal/repository/usage_log_repo_group_summary_test.go
backend/internal/service/api_key_auth_cache_impl.go
backend/internal/service/openai_gateway_messages.go
backend/internal/service/openai_gateway_response_handling.go
backend/internal/service/scheduler_snapshot_service.go
frontend/src/components/account/AccountUsageCell.vue
frontend/src/components/account/__tests__/AccountUsageCell.spec.ts
frontend/src/components/account/__tests__/EditAccountModal.spec.ts
```
