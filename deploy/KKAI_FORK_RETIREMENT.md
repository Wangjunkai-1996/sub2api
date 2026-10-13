# KKAI 四项功能退场实施记录

日期：2026-10-13。工作分支：`release/v0.2.15-kkai`。修改前 HEAD：`5c709885b0c92c8c40e8780eb230df50b5c24f2c`；本地官方对照：`v0.2.15`。

**阶段 A 的应用代码已完成退场并发布；阶段 B 的物理数据清理仍需生产执行。** 历史数据库结构仍在，不能将应用功能退场理解成生产数据库已完成清理。

## 1. 实际改动

| 删除项 | 现在的行为 | 保留能力 |
| --- | --- | --- |
| 分组级调度覆盖 | 删除组模型、服务、仓库、认证缓存及 Ent 中的覆盖字段；调度使用全局设置 | 全局基础/高级调度、订阅优先、权重、利润控制、出口池、组编辑并发保护 |
| 整段流式缓存后提交 | 删除 atomic 开关及整段暂存/终态提交分支；首个语义输出即可发送 | 首输出超时、前导事件暂存、安全重放边界、TTFT、usage、取消归因 |
| Codex 五小时自动暖机 | 删除 worker、启动/停止、probe、专用配额分支、身份代数、独占租约、管理 API 和前端控件 | 普通用量显示/刷新、OAuth 刷新、Agent Identity recovery、账号测试、额度重置 |
| 主动 STATE 票据 | 删除采集/续期/注入/store/watchdog/缺票门控、票据出口约束、强制 WS 桥接、UI/API/config | 客户端原生 STATE、跨账号保护、原生 WS/兼容桥会话恢复、HTTP/2 回退 |

同时完成：

- 重新生成 Ent 和 Wire，删除专用测试及失效调用；更新中英文文案和类型。
- 删除普通并发、Live、出口池 Lua 中的暖机独占键；同步所有 KEYS 偏移和负载返回数组解析。保留普通计数、身份容量、FIFO、lease fencing、模式切换和不限并发账号计数。
- 新增 `retired_account_extra.go`：创建、编辑、复制、导入、批量写入过滤退休字段；DTO、导出和审计继续隐藏历史票据和代理私密值，不修改调用方 map。
- 旧 settings/group 输入返回 400 并提示刷新。十个旧功能路由返回 404。
- 检查时发现既有 HTTP/2 响应体回退在收到响应头后提前清零错误计数；移除这次重复清零，仅在正常 EOF 后记录成功。已有回归测试验证连续断流触发 HTTP/1.1 回退。

## 2. 有意保留的兼容保护

过滤这些精确键/前缀：`codex_turn_ticket:*`（覆盖 v1/v2）、`codex_ticket_config`、`codex_ticket_watchdog`、`codex_harvest_proxy_url`、`openai_codex_warmup_policy`、`codex_warmup_policy`、`openai_window_warmup_policy`。

历史设置 `openai_codex_ticket_harvest_proxy_url` 继续按敏感字段审计脱敏。旧 `openai_window_warmup:` 鉴权失败冷却原因的保护仍保留，防止错误恢复可能失效的账号。它们没有 worker、采集、续期、注入或功能开关。

没有删除 Anthropic `intercept_warmup_requests`、正常 WS generate prewarm 或 Codex CLI-only 策略。这些不是此次退休的五小时自动暖机/主动采票功能。

阶段 A 的 `backend/migrations` **338 个已跟踪文件与修改前逐字相同**，当时没有 pending migration。阶段 B 追加 `249_retire_kkai_fork_features.sql` 与 `250_purge_retired_kkai_fork_data.sql`；历史迁移保持原字节。`backend/go.mod`、`backend/go.sum`、`frontend/package.json` 和 `frontend/pnpm-lock.yaml` 均未改变。

## 3. 验证结果及精确命令

下面按实际覆盖范围记录。修正后的失败用例仅在相关范围重跑；`no tests to run` 不作为测试通过证据。未运行全仓测试、全仓 lint、完整前端 typecheck 或发布构建。

### 后端生成与应用检查

工作目录：`backend`。

```sh
go generate ./ent
go generate ./cmd/server

go test ./internal/handler/admin ./internal/handler/dto ./internal/repository ./internal/service ./internal/server/routes ./internal/config -run 'Test(ExportDataExcludesCodexTicketMaterial|ImportData.*|AccountFromServiceShallow|Retired|RetiredForkRoutes|OpenAIHTTP2|HTTPUpstreamOpenAIHTTP2|ObserveOpenAIHTTP2|OpenAIStreamsFirstSemanticOutputBeforeTerminal|OpenAITTFT|OpenAIStream|APIKeyAuthSnapshot|OpenAICodexTurnState|RelayOpenAICodexTurnState|StageOpenAICodexTurnState|QueryUsage|ResetCredit|AgentIdentity|OpenAIAdvancedScheduler|SelectAccountWithScheduler|AccountEgress|Config|OpenAIResponsesStreamFailure)' -timeout 180s

go test ./internal/service ./internal/handler/admin ./internal/handler/dto -run 'Test(.*(CodexTurnState|StagedTurnState|CrossAccount|QueryUsage|ResetCredit|AgentIdentity|APIKeyAuthSnapshot|OpenAIAdvancedScheduler|SelectAccountWithScheduler|StreamReplay|FirstOutput|TTFT|Retired|AcquireAccountSlot|AcquireUserSlot)|ExportData|ImportData|AccountFromServiceShallow)' -timeout 180s

go test ./internal/repository ./internal/handler ./internal/server/routes -run 'Test(.*(HTTP2|LiveLease|LegacyEgress|LegacyLiveAdmission|TransitionMode|RequestBudget|ModelDispatch)|AccountEgress|OpenAIResponsesStreamFailure|RetiredForkRoutes)' -timeout 180s

go test ./internal/repository -run 'Test(HTTPUpstreamOpenAIHTTP2|OpenAIHTTP2|ObserveOpenAIHTTP2|BuildUpstreamTransport|HTTPUpstream.*Profile)' -timeout 90s

go test ./cmd/server ./internal/server ./internal/server/middleware -run 'Test(.*Wire|.*Cleanup|.*Contract|.*Audit|.*Redact)' -timeout 90s

go test ./internal/config -run 'TestLoad(DefaultSchedulingConfig|DefaultOpenAIResponseTimeouts|OpenAIFirstOutputTimeoutsFromEnv|DefaultOpenAIWSConfig)' -timeout 60s
```

结果：生成成功。首段实时输出、前导重试/输出后不重试、原生 STATE、配额/额度重置、旧缓存兼容、过滤/脱敏、旧 API 拒绝、出口池、Live、装配和中间件检查均已获得通过结果。上述扩展 service/admin/DTO 命令首次在新增备份测试失败，补入必填的空 `proxies` 数组后，仅重跑该用例通过：

```sh
go test ./internal/handler/admin -run 'TestExportDataExcludesCodexTicketMaterial' -timeout 60s
```

上述 transport 命令首次发现两个既有 HTTP/2 回退用例失败；修复提前清零后，由列出的 HTTP/2 专项命令重跑通过。其余 Live/出口切换及 handler/routes 在原命令中通过。`internal/server` 的普通构建没有测试文件，API 契约另用下方 `unit` 标签验证。

### 显式 unit 标签检查

```sh
go test -tags unit ./internal/service ./internal/handler ./internal/server -run 'Test(AccountTestService_OpenAI|OpenAIGatewayService_(OpenAIAdvancedScheduler|SelectAccountWithScheduler|Forward_WSv2_(GeneratePrewarm|TurnState)|Prewarm)|.*(RequestBudget|ModelDispatchBudget|FilterCrossAccount)|OpenAIHTTPStreaming|OpenAIStreaming|OpenAIResponsesStreamFailure)' -timeout 180s
go test -tags unit ./internal/handler -run 'TestOpenAIResponsesStreamFailureClientCancellationAttribution' -timeout 90s
go test -tags unit ./internal/server -run '^TestAPIContracts$' -timeout 90s
```

结果：service 通过；handler 中取消归因测试曾依靠已删除的分组覆盖打开高级调度，改为用全局设置启用并在清理时恢复，单独重跑通过。该组合中的其余 handler 选中用例通过。首个组合在 server 未匹配到测试，后用准确的 `TestAPIContracts` 验证通过。

新增的 `TestAccountTestService_OpenAIRetiredTicketsNeverGateOrInject` 在真实账号测试调用边界覆盖：旧账号 opt-in 仍为 true 时，缺票也正常请求；旧 STATE 存在时不主动注入。

### PostgreSQL / Redis 定向集成

本机临时容器：PostgreSQL `18.1-alpine3.23`、Redis `8.4-alpine`。没有访问生产。32 项实际用例通过，含普通组更新的乐观锁、退休列仍存在时的 ORM 读写、旧票据写入过滤、普通账号/批量/extra 更新、Live 计数、FIFO、容量隔离和出口切换。

标准测试入口有两个**原有前置问题**，本轮未修改其业务代码或历史迁移：

1. `proxy_expiry_renewal_integration_test.go` 仍以旧的五参数形式调用当前三参数 `sweepOneExpiredProxy`，导致整个 integration 包不能直接编译。
2. 历史迁移 `248_account_egress_direct_default.sql` 要求已有名为 `sys1-ipv4` 的已验证路由，空库 harness 没有该测试 fixture，迁移会主动拒绝。

本次用临时 Go overlay 隔离第 1 个无关测试文件，并在第 2 个明确错误出现时向临时数据库加入使用文档网段 `192.0.2.248` 的合成代理/身份/路由数据，然后重试原字节迁移。没有修改迁移、跳过迁移或对该地址发请求。此结果证明带历史结构和所需 fixture 的本地读写兼容，**不证明未经准备的空库入口或完整 integration 套件通过**。

```sh
CI=true go test -overlay=/tmp/kkai-retirement-integration-overlay.json -tags integration ./internal/repository -run 'Test(AccountRepoSuite|GroupRepoSuite|ConcurrencyCacheSuite|AccountEgressCacheSuite)/(TestRetired|TestCreate$|TestUpdate$|TestUpdateExtra|TestBulkUpdate|TestLiveLease|TestAccountSlot|TestPerIdentity|TestFIFO|TestConfigVersion|TestConcurrentCache|TestTransitionBridge|TestV2Rollback)|TestGroupRepositoryUpdateRejectsStaleOrdinarySnapshot' -count=1 -v -timeout 180s
```

外层进程设置 240 秒时限，实际包耗时 4.746 秒。日志 `/tmp/kkai-retirement-integration.log`，overlay JSON 指向 `/tmp/kkai-integration-harness.go` 与 `/tmp/kkai-excluded-proxy-test.go`；这三个文件仅为本次本机测试使用，未加入应用。

### 前端

工作目录：`frontend`。

```sh
pnpm exec vitest run src/views/admin/__tests__/SettingsView.spec.ts src/components/account/__tests__/CreateAccountModal.spec.ts src/components/account/__tests__/EditAccountModal.spec.ts src/components/account/__tests__/AccountUsageCell.spec.ts src/views/admin/__tests__/AccountsView.usageWindowsHint.spec.ts src/utils/__tests__/accountUsageRefresh.spec.ts src/i18n/__tests__/localeKeyCompleteness.spec.ts --testTimeout 10000
pnpm exec vitest run src/components/account/__tests__/EditAccountModal.spec.ts --testTimeout 10000

pnpm exec eslint src/api/admin/accounts.ts src/api/admin/settings.ts src/components/account/AccountUsageCell.vue src/components/account/CreateAccountModal.vue src/components/account/EditAccountModal.vue src/i18n/locales/en/admin/accounts.ts src/i18n/locales/en/admin/settings.ts src/i18n/locales/zh/admin/accounts.ts src/i18n/locales/zh/admin/settings.ts src/types/index.ts src/utils/accountUsageRefresh.ts src/views/admin/AccountsView.vue src/views/admin/SettingsView.vue

pnpm exec vue-tsc --noEmit --project /tmp/kkai-retirement-tsconfig.json
```

结果：7 个测试文件累计 252 项通过；第一次有 1 项 Edit 测试残留票据控件断言，移除失效断言后仅重跑该文件的 77 项全部通过。定向 ESLint 通过。

临时 tsconfig 覆盖修改的 9 个运行入口文件、必要的全局声明及其导入依赖，编译选项来自项目配置，类型检查通过。它不代表完整前端模块图或发布构建已验证。既有 router-link / Browserslist 提示没有引入依赖升级。

### 静态检查

工作目录：仓库根目录。`git diff --check` 通过；对 338 个 `backend/migrations` 已跟踪文件逐一与 `git show HEAD:<path>` 比较字节相同；依赖清单和锁文件同样相同。扫描生成 Ent、运行代码、前端和部署示例：退场实现已清除，剩余命中仅为历史迁移、旧输入拒绝、敏感值过滤、测试和明确的历史鉴权冷却保护。

## 4. 原发布门禁与阶段 B

以下保留当时的发布前门禁；实际发布状态见第 5 节、基础设施仓库的操作证据，以及第 6 节的当前只读盘点。

1. 按维护中的 Sub2API runbook 读取当次生产状态和实际 router，确认所有应用角色、worker 和当前回滚版本。当前未连接生产，不能推断槽位/镜像/健康状态。
2. 在旧程序仍可管理配置时，备份并将组调度覆盖归零；关闭原子流式启动配置、暖机/采票总开关及账号 opt-in。确认没有在途采集/暖机发送，并停掉旧 worker。总开关关闭不能代替停止旧 scanner。
3. 新旧版本共存时，不允许旧后台任务继续产生合成请求。先让旧独占 lease 到期；必要时仅处理可证明属于本功能的精确键，禁止清空 Redis。
4. 本地构建唯一镜像标签，按 runbook 上传 sys1 并完成候选健康、直连、有效路由、GPT/OpenAI 错误窗口和回滚检查。正式构建/部署门禁尚未运行。
5. 当前程序不再读取退休 ORM 列，但阶段 A 仍保留历史列、三张暖机表、函数和触发器；旧数据/触发器仍可能有数据库维护副作用。因此不能宣布数据库零写入或物理清理完成。
6. 阶段 B 仅在 active 和 rollback 都兼容无四项功能的 schema 后实施：先移除相关触发器及函数，再移除退休表/列、精确 settings、账号旧私有键、生产 YAML/env 与精确 Redis 键。执行前检查依赖和备份，采用独立追加迁移；绝不改写历史迁移。
7. 不批量恢复 `status=error`、`schedulable=false`、429、Cyber 或人工禁用账号。旧暖机鉴权错误仍需正常凭据验证，不能因删除功能自动解封。

阶段 B 和正式发布均需各自的实际验证证据。本轮未承诺“无任何风险”，已知的标准 integration 入口问题及前端完整构建缺口必须在后续交付中保持可见。

## 5. 2026-10-13 正式发布补充验证

用户明确授权提交、推送和蓝绿上线。发布 scope 为 `both`：文本模型转发与图片角色的账号测试、管理接口都实际使用此次删除的路径。当前基线由已安装控制器动态确认：text/image green，源码 `5c709885b0`；控制器与本地基础设施 commit 均为 `d55e83e2a3f1fded44ba76ba20887b9071813513`。候选和最终状态另存基础设施仓库 `artifacts/sub2api/`，此处不是上线完成声明。

发布门禁补跑的精确命令（Go 工作目录 `backend`）：

```sh
go test -tags unit ./cmd/server ./ent ./ent/account ./ent/group ./ent/migrate ./ent/runtime ./ent/schema ./internal/config ./internal/handler ./internal/handler/admin ./internal/handler/dto ./internal/repository ./internal/server ./internal/server/middleware ./internal/server/routes ./internal/service -timeout 180s
go test -tags unit ./internal/repository -timeout 90s
go test -tags unit ./internal/handler -skip '^(TestOpenAINonstreamHandlerTransportFailover_RequiresUnsentProof|TestOtherOpenAIEntryPointsDoNotRunPromptAudit)$' -timeout 90s
go test -tags unit ./internal/service -skip '^(TestInitialMessageFiltersPreserveSignedLatestAssistantContentBytes|TestLegacySchedulerDecision_PreviousResponseRouting)$' -timeout 300s
go test -tags unit ./internal/service -run '^Test(AccountReadableSnapshot|ListPluginAccounts|Retired)' -timeout 60s
```

前端工作目录 `frontend`：`pnpm run build` 通过，包含 i18n 检查、完整 `vue-tsc -b` 和 Vite 生产构建。未跑整个仓库全部测试或全仓 lint。

- 发布检查修复了 SQL mock 仍含退休 identity-generation 列值、插件字段分类误删三项保留字段的问题；插件快照补充过滤历史退休 extra，测试确认源 map 不变。
- 所有其他修改包单测通过。repository 全包 10.879 秒通过；handler 除两个已证实基线失败外 42.508 秒通过。
- service 第一次因整个包超过 180 秒中断，改为 300 秒后 225.905 秒完成；除两个已证实基线失败外，仅协议矩阵 golden 检查失败（576 个历史错误文案差异）。没有删除这些有效测试或改写 golden。
- 5 类既有失败均在 `git archive 5c709885b0` 的临时本地源码副本复现：OAuth 图片 unsent fixture、图片审核静态断言、历史消息 placeholder 断言、隔离代理 previous-response 断言、协议矩阵 576 个差异。对照只使用本地对象，未访问代码托管。日志 `/tmp/kkai-retirement-baseline-failures.log` 与 `/tmp/kkai-retirement-baseline-routing.log`。这些是已知验证缺口，不是全绿声明。

生产过渡配置已通过旧版正常管理 API 停用（带审计和缓存失效）：暖机 master=false、默认 policy=off；132 个未软删除账号的 policy 改为 off。STATE 三个 master 原本均为 false，启用账号数为零；35 个分组原本全部 inherit、无 overrides。没有修改 soft-deleted 历史账号。备份保存于 sys1 root-only `/var/lib/kkai/sub2api-release/operator-evidence/retirement-20261013T061815Z/disabled-config-before.json`。

停用后有效暖机 lease、global-send permit、普通暖机 Redis 独占键均为零；最近已记录合成发送为 2026-10-12。旧 rollback 的 scanner 可继续做非发送维护，不能为停 scanner 破坏保留旧容器身份的蓝绿门禁；发送通过总开关及所有有效账号策略双重关闭。遗留启动 env 和历史数据库结构留待阶段 B；新程序不再消费它们。生产业务基线已有上游 502、客户端取消和旧账号轮询 404，灰度必须完整保留并归因实际窗口错误。

## 6. 阶段 B 物理清理清单（2026-10-13 只读盘点）

`sys1` 控制器状态为 `complete`、`runtime_gate=passed`。以下只读查询未读取票据、代理或设置值；计数在执行清理前必须重新确认。

| 对象 | 当前盘点 | 清理动作 |
| --- | --- | --- |
| 暖机数据 | `openai_window_warmup_jobs` 198 行、`attempts` 28 行、`runtime` 1 行 | 备份并核对保留要求后，追加迁移删除三张专用表 |
| 暖机数据库逻辑 | 5 个 `trg_openai_window_warmup_*` 触发器、8 个 `openai_window_warmup_*` 函数 | 先删除触发器，再删除表和函数；确认账号写入不再产生暖机记录 |
| 专用列 | `accounts.openai_warmup_identity_generation`；`groups.scheduler_type`、`groups.advanced_scheduler_overrides` | 先改写 `enqueue_group_auth_cache_invalidation()`，移除对分组旧列的引用，再删除三列及相关约束 |
| 专用设置 | 4 个 `openai_codex_ticket_*`、11 个 `openai_window_warmup_*` 键 | 只按核准的精确键删除；采集代理设置属于敏感数据，不记录其值 |
| 账号历史 `extra` | `codex_turn_ticket:gpt-5.6-sol` 99 个、`codex_turn_ticket:gpt-6-astra` 99 个、`openai_codex_warmup_policy` 3142 个 | 包含软删除账号，按退休键白名单清理；保留正常用量和客户端原生 STATE 字段 |
| 启动配置 | 两个当前应用容器仍带 `OPENAI_WINDOW_WARMUP_WORKER_ENABLED` 环境变量 | 在受控部署配置中删除，不直接手改运行容器 |

执行顺序：

1. 确认所有可能承载请求的应用版本都不再依赖这些表、列和触发器；旧版本仍可运行时，不执行破坏性 DDL。核对生产对象、依赖、精确行数和受影响账号范围，取得受限备份并验证可恢复。
2. 单独审查追加迁移。`249` 在短事务中停用五个专用触发器、改写分组鉴权缓存失效函数、删除暖机表/函数/专用列与约束；`250` 在独立事务中清理账号 `extra` 和精确设置键，避免将账号 GIN 更新放在 DDL 表锁事务内。历史迁移文件保持原字节，不能删改。`250` 有 90 秒语句上限，执行前必须在生产规模恢复副本上演练并记录耗时；超限时停止，不扩大超时或手工部分执行。
3. 从受控生产配置移除废弃环境变量，核对 Redis 中仅属于这四项功能的精确键；不得清空数据库、Redis、备份或其他服务配置。历史备份中的票据按既定保留策略到期销毁，避免把备份误当成当前运行数据。
4. 用生产对象清单、精确键计数、账号和分组写入、鉴权缓存失效、文本与图片请求验证结果收尾。确认触发器/表/列已消失，设置和 `extra` 退休键计数为零，且无新增错误。旧请求拒绝、导入过滤、审计脱敏和历史鉴权冷却保护继续保留，以防旧客户端或备份重新写入退休字段。

### 阶段 B 恢复演练与执行边界

2026-10-13 第一轮 `895110988f` 镜像蓝绿发布已 `complete`，绿槽为 active，蓝槽为 `6cd367c0c0` rollback；四个容器均为退场后的代码。绿槽已无废弃 `OPENAI_WINDOW_WARMUP_WORKER_ENABLED`，蓝槽仍带该旧环境变量，需在下一轮受控替换清除。第一轮发布的 10%、50%、100% 错误窗均经人工归因接受，并非自动错误门禁通过；证据保存在基础设施仓库对应 release 的 ignored artifacts 目录。

在 `sys1` 的 root-only `operator-evidence/retire-phase-b-20261013T101155Z/` 内保存生产完整 schema 的 `pg_dump -Fc` 与受影响 7 表的 data-only `pg_dump -Fc`，模式 `0600`，SHA-256 分别为 `f2ba4548230f18db0e3a3300028a84035582f03b0fd346094c00af359c3bec4c` 和 `31b71e2061da0df112378258f32772ed14c2446fa5746e385e17bc6c9c328c69`。数据备份只是此项迁移的受限恢复材料，不是全库 PITR 或生产数据库通用恢复能力。

使用生产相同 PostgreSQL 18.4 镜像，在 `--network none` 的隔离容器中恢复两个归档（data-only 用 `pg_restore --disable-triggers` 处理循环外键）并实际执行 `249`、`250`。恢复时 16,982 个账号、35 个组、187 个暖机 job、28 个 attempt 和 310 个历史 migration；演练后账号/组行数不变，3 表、退休列、198 个票据键、3,141 个暖机策略键、15 个旧 setting 键均消失。两个 SQL 均需以显式单事务执行并在同事务记录 runner 规则的 trimmed SHA-256；不可用默认 autocommit 的 `psql -f`。当前生产迁移仍停在 `248`，不得把演练当成已完成生产清理。

阶段 B 属于独立的破坏性数据迁移，不能用普通无迁移的应用蓝绿流程顺带执行。标准 integration 入口的既有编译错误和空库 fixture 缺口也必须在迁移验证前修好，不能靠临时 overlay 或跳过迁移证明阶段 B 可发布。
