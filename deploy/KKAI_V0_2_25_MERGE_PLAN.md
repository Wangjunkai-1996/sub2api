# Sub2API v0.2.25 合并方案

2026-11-26，北京时间。

**推荐从正式 tag `v0.2.25` 建立 `release/v0.2.25-kkai`，以官方提交为第一父，与 KKAI `e5517b6cb1` 做一次双父合并。** 账务探测接进 KKAI 已有的原子事务，请求头变量和结构化错误采用官方实现。合并后以独立提交纳入本地已验证的退役改动，删除出口池 `shadow` 预演和 7 个冗余函数。这样可以单独审阅或回退清理，同时保留官方升级。[官方发布说明](https://github.com/Wei-Shaw/sub2api/releases/tag/v0.2.25)

## 冻结基线

| 输入 | 证据 |
| --- | --- |
| 最新正式版 | `v0.2.25`，11 月 24 日 11:21 发布。官方 release 与源码归档指向 `348997267d756eab5729bb6b78a275a15d18796a` |
| 共同基线 | 官方 `v0.2.24` 解引用为 `5c2982a81b46e2142d0143d2f83c1ce9c4da1d24` |
| KKAI 源码基线 | `release/v0.2.24-kkai` 为 `e5517b6cb1b1d58d37ef3ed4ef790bc48e638806`，本地 fork 跟踪 ref 相同。11 月 25 日文本发布记录确认该提交包含 AUTH 依赖的账号探测修复。该次图片槽沿用 `507a79ca9a` 制品 |
| 源码核验 | 官方源码归档含 4,795 个文件。将 GitHub 官方比较 diff 独立应用到 `v0.2.24`，得到同一个根树 `5f702a4870654083d8ef85362f4918f63e56a382`，归档 SHA-256 为 `824c09225b2a2474bd3a59137afb2e8791410cec3e20b47640d4e353cd9e60e2` |

官方新增 **15 个提交，91 个文件**，其中 5 个新增、86 个修改。KKAI 的净差异是 **569 个文件**，按重命名前后分别计数是 **577 个路径**。双方一共 **630 个路径**，官方独有 53 个，KKAI 独有 539 个，共同改动 38 个。其中 7 个文本冲突、30 个自动合并、1 个内容相同。离线预演基于归档重建的临时源码树，本地应用仓库尚缺正式 `v0.2.25` Git tag/object。台账记录双方状态、模式、blob 和逐路径处置。[官方版本比较](https://github.com/Wei-Shaw/sub2api/compare/v0.2.24...v0.2.25) · [逐路径清单](kkai-v0.2.25-merge-paths.tsv) · [11 月 25 日发布记录](/Users/tokk/Documents/Codex/2026-08-09/li/work/kkai-infra-warmup/artifacts/sub2api/v0.2.24-kkai-20261125-probe-e5517b6cb1/release-result.md)

## 官方功能与 KKAI 的接口

| 领域 | 合并方案 | 接收条件 |
| --- | --- | --- |
| 账务与分组名称 | 接收 `GET /v1/sub2api/billing?include=group`、`POST /api/v1/admin/accounts/upstream-billing-probe/group-preview`，以及单账号一键或自动同步名称和备注。预览复用正式探测的 URL 校验、禁跳转、凭据和代理归属。自动同步默认关闭，沿用官方按字段校验和 CAS 写回 | 原账务响应的 `schema_version=1`、倍率和缓存语义照常。只读取已认证 Key 所绑定分组，支持语言选择及重复/逗号分隔 `include`。并发关闭、换凭据/出口与旧探测写回时，快照、名称、备注、倍率各自符合当前状态。非法名称或备注单项丢弃，有效探测和倍率仍落库 |
| 账号保存和 AUTH | 官方的 `AccountBillingSettingsUpdate` 接进现有 `AccountConfigurationRepository`，共享同一行锁下的探测字段合并。保留 field mask、组绑定、出口修订、shadow 继承、outbox、状态及运行时 `extra`。保留 11 月 25 日的 `/admin/accounts/:id/probe` 和交互式 `/test` 各自语义 | 真实 `PUT` 一次提交组同步与出口变更，两者一起成功或回滚。省略名称/备注沿用锁内当前值，显式清备注有效。AUTH 的 OAuth 文本探测成功后保留并发 401 的错误和停调状态。账务分组预览独立走官方入口 |
| 请求头变量与 WS | 使用官方 `ApplyHeaderOverrides(ctx, headers)`，接入已认证 API Key 的用户、Key、绑定分组及规范化请求 ID。保存阶段用合并后的账号类型和凭据校验。WS 握手采用连接级作用域，将出站上报身份加入既有兼容键 | 原静态覆写和认证头限制有效。跨用户、Key、分组及出口绑定的 WS 握手各自隔离，包含强制复用、预热与后续轮。共享模型抓取、后台探测、管理员测试无入站代理用户身份。账号模板使用独立出站头名，保留 `X-NewAPI-*` 的 HMAC、nonce 校验及内部剥离，禁止覆写重新引入内部签名字段 |
| 错误处理与运维 | HTTP 透传和 compact SSE 采用官方净化后的结构化错误、管理员透传/自定义文案/跳过监控规则。容量错误在发给客户端的 compact `response.failed` 中映射为 `server_error`。错误详情合并官方 `reported_headers` 与 KKAI 已有的出口和尝试归属 | `invalid_encrypted_content`、上下文超长保留必要字段。保留当前 120 秒首个可用输出上限、11 次总派发预算、已输出后的重放边界及续接隔离。OPS 只展示该次尝试经脱敏的上报头，隐藏模板中的固定文字及账号凭据 |
| 图片与兼容入口 | 吸收 API Key 图片转发使用的新请求头上下文，同时保留 `image_provider_error` 的来源证明、未知结果与已有可交付输出的处理。保留 NewAPI TTFT、逐次用量、实际模型/tier、图片退款幂等 | 真实图片、编辑和 `account-test` 通过。响应终态、实际图片和结算一起核对。错误透传规则、缺失证明与已派发未知时继续采用图片路径现行判断 |
| 构建与依赖 | 接受官方针对 `GO-2026-6278` 固定的 `gorilla/websocket v1.5.4-0.20240701034025-d67f41855da4` 和两个新校验项，保留 KKAI 的 `go.sum` 差异。将源码 `VERSION` 写为 `0.2.25`，现有构建入口继续注入生产版本及精确提交 | 官方 tag 的源码 `VERSION` 与 fork 都是 `0.2.24`，自动合并仍是旧值。验证 Go 实际依赖版本、管理端 Ops WS 握手、网关 WS 及镜像 `/app/sub2api -version`。应用 Dockerfile、前端锁文件和 Ent schema 在官方增量中没有变动 |

## 七个文本冲突的处理

| 文件 | 处理 |
| --- | --- |
| `backend/internal/handler/admin/account_handler.go` | 在 KKAI Create 和 Update 结构体各增加官方 `GroupSyncEnabled *bool`，保留 `EgressMode`、`EgressPool` 及单一 Bulk 声明。核对字段透传及 `/:id/probe` 和分组预览的路由。分组预览按现有受限管理员规则默认拒绝，保持 AUTH 既有权限契约 |
| `backend/internal/repository/account_repo.go` | 用官方强类型更新参数与状态规则。保留 KKAI 已锁定并解码的完整 `currentExtra`，从中读取新增分组同步标记，继续以行内 runtime extra 为准。同步修改 SELECT、Scan 和调用方，保留原有 CAS、禁用联动、代理身份、outbox 和重读行为 |
| `backend/internal/repository/account_repo_grok_managed_extra_test.go` | 保留共享行锁投影及 Grok 管理字段的断言，改用新 `AccountBillingSettingsUpdate`，SQL mock 与实际列顺序一致 |
| `backend/internal/repository/account_repo_opencode_go_usage_test.go` | 统一测试投影，保留 OpenCode Go 身份和代理变更导致快照失效的覆盖 |
| `backend/internal/repository/account_repo_upstream_billing_probe_update_test.go` | 保留 `TestLockAndMergeAccountProbeExtraUsesLockedRuntimeExtra`，将其旧调用一并改为新签名。用锁内数据覆盖显式同步开关、默认关闭、保存 guard、旧快照和行锁契约 |
| `frontend/src/components/account/CreateAccountModal.vue` | API Key 平台增加官方 `upstream_billing_group_sync_enabled`，保留现有出口池字段构造、过期暂停、模型和账务设置。草稿预览只传支持的凭据组合 |
| `frontend/src/components/account/__tests__/EditAccountModal.spec.ts` | 将 `previewUpstreamGroup` 与 KKAI `showErrorMock`、`showSuccessMock` 放入同一次 `vi.hoisted`。保留同步、失败提示、出口及旧账号的断言 |

### 自动合并后的必查调用方

1. **真实更新事务。** `admin_account.go` 的生产分支在 fallback 前从 `UpdateAccountConfiguration` 返回，预演只在 fallback 传入了 `GroupSyncEnabled`。把显式变更整理成一份 `AccountBillingSettingsUpdate` 传给 `AccountConfigurationMutation` 和 fallback。在 `account_configuration_repo.go` 的 `needsExtra` 和行锁调用中消费。更新旧签名与测试桩。在锁内合并当前凭据与新类型后执行模板类型校验，覆盖无 credentials 的类型变更和并发更新。保留名称/备注的字段掩码，避免旧表单覆盖后台新值。[当前事务入口](/Users/tokk/Documents/Codex/2026-08-09/li/work/sub2api/backend/internal/repository/account_configuration_repo.go:518)
2. **其他账号写入。** 核对单账号开关、批量关闭、CRS 导入、复制及通用 `extra` 更新。关闭探测同时关闭倍率和名称同步。换 Key、URL 或代理后，用真实 PostgreSQL 验证迟到的旧探测写回被 CAS 拒绝。出口修订、影子绑定、运行时 extra 和 outbox 走现有事务。前端只提交管理员实际更改的名称和备注。[上游账务探测增量](https://github.com/Wei-Shaw/sub2api/compare/v0.2.24...v0.2.25)
3. **鉴权、作用域和重试。** 扫描所有 `ApplyHeaderOverrides` 调用，验证文本、图片、两轮 WS、后台探测和共享模型抓取。当前 KKAI 在入站剥离 `X-NewAPI-*`，官方出站覆写名单没有同一前缀保护。建议在现有覆写校验与运行时过滤共用的判定中保留该内部命名空间，前端提示与之对齐。覆盖静态值、变量和历史保存的配置。WS 同时核对出口绑定、强制复用、上报身份及每次尝试的 OPS 记录。[当前 WS 兼容键](/Users/tokk/Documents/Codex/2026-08-09/li/work/sub2api/backend/internal/service/openai_ws_pool.go:2743) · [当前 NewAPI 入站归属](/Users/tokk/Documents/Codex/2026-08-09/li/work/sub2api/backend/internal/server/middleware/newapi_attribution.go:94)
4. **错误优先级。** 审阅 API Key 和 OAuth 的 HTTP、SSE、compact 及图片入口。核对确定性请求错误、运营规则命中、真实账号 401/429、Cyber 拦截、首输出前容量切换和已提交流的终态。OPS 跳过监控只影响现行契约定义的最终故障，保留已恢复尝试和账号健康证据。[图片失败契约](/Users/tokk/Documents/Codex/2026-08-09/li/work/sub2api/docs/kkai/image-failure-development-20261115.md)

## Fork 代码取舍

| 决定 | 内容 |
| --- | --- |
| 复用官方的实现 | 请求头覆写切到带 context 的官方入口。结构化错误、脱敏规则和 compact 容量错误沿用官方 formatter。账务探测采用官方 `AccountBillingSettingsUpdate` 和既有 KKAI 锁内持久化，收敛旧的形参和单用途分支 |
| 退休出口池 `shadow` 预演 | 11 月 26 日 16:52（北京时间）只读核对 `sys1`，`account_egress_pool_rollout_mode=enforce`，47 个未软删除账号配置了池模式。代码中 `shadow` 与 `off` 都走 legacy 准入，仅额外计算容量、候选数并输出两条诊断日志。应用及配套基础设施仓库中的运行脚本和告警配置未找到这些日志的消费者。本地已移除预演。库中已有的 `shadow`（含大小写和首尾空格）按 `off` 读取，继续使用 legacy 身份镜像。显式 `enforce` 仍走真实池分配、租约和并发限制。新旧进程能共用原设置，无需改库。[准入入口](../backend/internal/service/account_egress_admission.go) · [旧值兼容测试](../backend/internal/service/account_egress_legacy_admission_test.go) |
| 删除 5 个无调用的 fork 函数 | 删除 `NewAccountEgressCache`、`IsAccountEgressPoolEnforced`、`Account.EffectiveEgressCapacity`、`selectAccountForModelWithExclusionsStickyHit`、`buildCyberSessionBlock`。Wire 实际注入 `ProvideConcurrencyCache`，分配器和 Redis 使用 `AccountEgressPoolConfig.EffectiveCapacity`，调度器在真实选号结果上记录 sticky 命中，会话封锁由服务直接计算、读写。最后两个名字在官方树有调用，KKAI 同名包装在冻结分支和合并预演均无调用。只删除这两个包装。[Wire](../backend/internal/repository/wire.go) · [调度入口](../backend/internal/service/openai_gateway_scheduling.go) · [会话封锁](../backend/internal/service/openai_cyber_session_block.go) |
| 删除 2 个仅供测试调用的函数，保留行为覆盖 | 删除 `AccountEgressLeaseLost` 和 `isOpenAICyberAccountCooldownEligible`。原测试分别改为验证真实出站请求上下文收到的取消原因，以及按指定策略计算的账号资格。另将两个构造历史 Redis 过渡状态的方法及其 Lua、一个读取并发冷却状态的方法移到已有的 `_test.go`。生产分配器继续识别旧 Redis 状态，测试能从旧状态验证准入、排空和恢复。[请求上下文](../backend/internal/service/account_egress_runtime.go) · [Redis 兼容测试](../backend/internal/repository/account_egress_cache_test.go) |
| 仍有调用方 | AUTH 的 OAuth `/:id/probe`、NewAPI 签名归属与 TTFT、出口池和运行时准入、429 恢复、HTTP 失效续接、原生 WS/STATE、真实图片失败与退款协议。维护这些契约以及旧格式兼容 |
| 保留按需启用的网络追踪 | 同一时间核对 4 个应用容器的环境覆盖和配置，均取默认值 `false`。排障手册仍使用 `openai.network_trace` 关联 DNS、TCP、TLS、连接复用和网络首字节耗时。官方 `reported_headers` 记录请求头变量，无法提供这些时间。保留现有开关及配置示例。Spark 账号对母账号凭据和出口的继承也有运行时调用。[网关排障手册](GATEWAY_DIAGNOSTICS.md) · [原生 HTTP 出站入口](../backend/internal/service/openai_plugin_transport.go) |
| 延续历史退役 | 分组级调度覆盖、整段流缓冲、五小时自动暖机、主动 STATE 票据、Traffic Director、managed text recovery。保留旧路由拒绝、导入过滤、历史 SQL 与已登记校验和。[上版取舍](KKAI_V0_2_24_MERGE_PLAN.md) · [退役记录](KKAI_FORK_RETIREMENT.md) |

## 落地顺序与验证

1. 按升级手册取得正式本地 tag，核对 commit、tree 和共同基线。从当前工作区导出清单中标为“本轮清理”的 14 个 Go 文件差异，在新 release 工作树合并冻结的 KKAI 提交。实施时若生产源码基线前移，对新增差异补做预演。以两个父提交为基准审阅最终 diff，逐路径闭合清单，处理自动合并的调用方。先形成可构建的双父提交，再应用清理补丁并单独提交。后端只有相关构造实际改变才需要重新生成 Wire 或 Ent 产物。[升级手册](/Users/tokk/Documents/Codex/runbooks/sub2api-upgrade-and-deployment.md:164)

   ```sh
   export GIT_NO_LAZY_FETCH=1
   repo_root=$(git rev-parse --show-toplevel)
   cleanup_patch=$(mktemp -t sub2api-v025-cleanup)
   awk -F '\t' '$5 ~ /^本轮清理/ {print $1}' deploy/kkai-v0.2.25-merge-paths.tsv |
     xargs git diff --no-ext-diff --no-textconv --no-renames --binary --full-index e5517b6cb1 -- > "$cleanup_patch"
   shasum -a 256 "$cleanup_patch"
   git show-ref --verify refs/tags/v0.2.25
   git rev-parse 'v0.2.25^{commit}' 'v0.2.25^{tree}'
   git merge-base v0.2.25 e5517b6cb1
   git merge-tree --write-tree --name-only --no-messages --merge-base=v0.2.24 v0.2.25 e5517b6cb1
   candidate_dir="${repo_root%/*}/sub2api-v0.2.25-kkai"
   git worktree add -b release/v0.2.25-kkai "$candidate_dir" v0.2.25
   cd "$candidate_dir"
   git merge --no-ff --no-commit e5517b6cb1
   git diff --name-only --diff-filter=U
   # 解决上列冲突、完成调用方适配，验证并提交双父合并后执行
   git apply --check "$cleanup_patch"
   git apply --index "$cleanup_patch"
   git diff --cached --check
   ```

   记录实施结果时，将原工作区的本方案和逐路径清单一同纳入新 release 分支的升级记录。清理补丁仅包含 14 个 Go 文件。

2. 定向验证从真实入口出发。覆盖受改包及直接依赖、前端类型和实际构建。优先补同时保存出口与分组同步、并发探测写回、动态头与签名归属隔离的缺口。PostgreSQL 验 CAS 与锁序，Redis 验冷却、会话和出口的新旧版本兼容。模型计费和 Ultrafast 用固定请求夹具核对。对比 11 月 25 日已复现的 3 个 service 基线失败，分别涉及 Codex 父级缓存键、断连终态和 Grok 搜索规则。[既有基线检查及缺口](/Users/tokk/Documents/Codex/2026-08-09/li/work/kkai-infra-warmup/artifacts/sub2api/v0.2.24-kkai-20261125-probe-e5517b6cb1/release-result.md)

   ```sh
   # 在 backend 执行，以下是首批验证入口
   go test -tags unit ./internal/handler/admin ./internal/repository ./internal/server ./internal/server/routes -run 'Test(AccountProbe|AccountHandlerUpstreamBilling|LockAndMergeAccountProbeExtra|UpdateAccountConfiguration|GatewayRoutesKeyBillingInfo|APIContracts|ScopedAdmin|OpsWSHelpers|MigrationContract|EmbeddedMigrationContract)' -count=1 -timeout=120s
   go test -tags unit ./internal/service ./internal/server/middleware -run 'Test(APIKeyAuthSetsGroupContext|NewAPIAttribution|ApplyHeaderOverrides|HeaderOverride|ValidateHeaderOverrideVariableUsage|OpenAIWSHandshakeCompatibilitySeparatesReportedIdentity|AppendOpsUpstreamError|OpenAIGatewayService_APIKeyPassthrough_|OpenAIStreamMetadataAndKeepaliveBeforeOverloadFailOver|ForwardOpenAIImagesAPIKey_FailureContract)' -count=1 -timeout=120s
   go test -tags unit ./internal/handler -run 'Test(OpenAIResponsesWebSocket_NativeLaterTurnUsageLimitDoesNotReplayFirstMessage|OpenAIGatewayHandlerImages_.*(Failure|UnknownOutcome))' -count=1 -timeout=90s
   go test -tags integration ./internal/repository -run 'Test(ProbeGroupWriteHonorsDisableDuringRequest|AccountUpdatePreservesConcurrentProbeSnapshot|AdminAccountEditPreservesRateSynchronizedAfterLoad|ProbeSnapshotCASProtectsManualRateAfterSyncDisabled)' -count=1 -timeout=120s
   go list -mod=readonly -m github.com/gorilla/websocket

   # 从仓库根执行
   pnpm --dir frontend exec vitest run src/components/account/__tests__/CreateAccountModal.spec.ts src/components/account/__tests__/EditAccountModal.spec.ts src/components/account/__tests__/BulkEditAccountModal.spec.ts src/components/account/__tests__/HeaderOverrideEditor.spec.ts src/components/account/__tests__/credentialsBuilder.spec.ts src/api/__tests__/admin.accounts.upstreamBillingProbe.spec.ts src/views/admin/__tests__/AccountsView.lite.spec.ts src/views/admin/ops/components/__tests__/OpsErrorDetailModal.spec.ts src/views/admin/ops/utils/__tests__/errorDetailResponse.spec.ts
   pnpm --dir frontend typecheck
   ```

3. 核对迁移和构建身份。官方两版 305 份 SQL 逐字一致，预演相对 `e5517b6cb1` 的 325 份 SQL 和迁移契约文件一致。预期本次没有新迁移。保留生产已登记的文件名、历史 checksum 兼容规则和 Docker 降权入口。定稿时验证下列命令及归档身份，发布控制器再用实际候选二进制的 `-migration-contract` 比对线上账本。任何 pending、缺文件或未知 checksum 停在迁移门禁。[迁移门禁](/Users/tokk/Documents/Codex/runbooks/sub2api-upgrade-and-deployment.md:128)

   ```sh
   git diff --exit-code e5517b6cb1 HEAD -- backend/migrations backend/internal/repository/migration_contract.go backend/internal/repository/migrations_runner.go
   git diff --check v0.2.25 HEAD
   git diff --stat v0.2.25 HEAD
   git diff --stat e5517b6cb1 HEAD
   git archive --format=tar HEAD >/dev/null
   ```

4. 正式发布建议 `release_scope=both`。文本角色实际执行透传、compact、WS 和账务变更，图片角色实际执行 API Key 请求头覆写及相关账号测试。按发布手册从精确本地 commit 在 Mac 构建，单次上传 `sys1`，审阅 `plan-stage` 的精确摘要。由有效路由发现 text 和 image 槽，各保留旧 active。按 **10% → 50% → 100% → complete** 晋级，每段至少 120 秒，以文本 SSE `response.completed`、真实有效图片、候选与旧版的完整错误窗口验收。与发布无关的错误按 v1 逐组归因，确认回归按 release ID 回滚。最终核对即时、延迟、清理后状态及回滚准备度。[蓝绿与回滚手册](/Users/tokk/Documents/Codex/runbooks/sub2api-upgrade-and-deployment.md:227)

   **配置兼容。** 旧版会发送受支持覆写项中的模板原文。灰度前可按账号统计已使用模板语法或内部保留头的配置，审阅记录只保留数量与归属。建议灰度沿用静态头，以隔离夹具验证新功能，完成后按需为具体账号开启模板和名称同步。启用时记录对应账号的原配置、名称和备注。日后回滚到旧版时按这些记录恢复账号配置和业务字段。[旧版覆写实现](/Users/tokk/Documents/Codex/2026-08-09/li/work/sub2api/backend/internal/service/account_header_override.go:176)

## 本轮实际验证

- 通过 `git status --short --branch`、`git rev-parse --show-toplevel HEAD 'v0.2.24^{commit}'`、`git for-each-ref` 和 `git merge-base --is-ancestor 507a79ca9a e5517b6cb1` 确认冻结基线。`git show-ref --verify refs/tags/v0.2.25` 确认正式 tag 尚缺。
- 通过 `git read-tree 5c2982a81b46e2142d0143d2f83c1ce9c4da1d24`、`git apply --cached --whitespace=nowarn` 和 `git write-tree` 在临时对象库重建官方树，和源归档逐对象一致。独立 `git diff --no-renames --name-status` 核对 630 路径。
- 在临时对象库以 `git merge-tree --write-tree --name-only --no-messages -z --merge-base=5c2982a81b46e2142d0143d2f83c1ce9c4da1d24 refs/heads/upstream-source e5517b6cb1b1d58d37ef3ed4ef790bc48e638806` 预演，退出码 1 对应上述 7 个冲突。`git diff --exit-code e5517b6cb1 b42908e74ce428b3ee16867cfd0404857bca7401` 在迁移目录、迁移契约、Ent、Wire、Docker 入口及前端锁文件指定路径均为 0。
- 本轮清理补丁的 SHA-256 为 `dccf65e3472b5e2ae34c8f8d9b28408004159e482e9b6d900cf729be135b6fe8`。在三方预演的独立索引上执行 `git apply --cached --check` 并实际应用，得到树 `7de3cb047bd3f05ae9190efefbb77b82d5252b8a`。相对原始预演恰好 14 个文件变化，逐文件与当前清理工作区字节相同。冻结基线的 630 个路径、7 个原始冲突及清单中的 blob 继续作为对照。迁移目录、迁移契约及 Wire 生成文件字节一致。
- `sys1`（`tokk@ovh-sys1-prod`）的 PostgreSQL 只读事务查询了 `settings` 中唯一的 rollout 键及未软删除的池账号数，再按 `com.kkai.role=sub2api` 枚举运行容器并核对网络追踪配置。结果见上表。记录的是当时的配置，仓库外的临时诊断订阅及历史启用次数未核实。
- 以下命令在当前清理工作区完成。103 个顶层 Go 用例通过，handler 5、repository 33、service 65，含子测试共 180 项，无跳过。全量套件跳过。正式合并候选的编译、PostgreSQL 集成验证及蓝绿验收列在第 2 至 4 步。

  ```sh
  # 在仓库根执行，检查结果均为空
  git diff --check
  git diff --name-only -- '*.go' | xargs gofmt -l

  # 在 backend 执行
  GOPROXY=off go test -json -tags unit ./internal/handler ./internal/repository ./internal/service -run '^Test(RecordCyberPolicyIfMarked|CyberSessionBlock|AccountEgress|Legacy(Egress|AdmissionDoesNot)|Transition|ForcePool|OpenAISchedulerStickyHit|OpenAISelectAccountWithLoadAwareness_Sticky|OpenAICyberAccountCooldown|OpenAINetworkTraceNativeFeatureGate)' -count=1 -timeout=90s
  ```

## 正式合并实施结果

官方 tag 已经取得。提交 `348997267d756eab5729bb6b78a275a15d18796a` 的源码树与预演归档一致。双父合并提交为 `de76aa1f2`，随后以 `0c7c5a508` 单独提交出口池预演和冗余函数的清理。完成 7 处原始冲突及账务事务、请求头和 WS 调用方适配。受测源码树为 `e664471e7310947ca132a296a39f5b67a4f138cc`。

7 个 Go 包共 228 个定向顶层测试通过。5 个 PostgreSQL 定向用例通过，覆盖联合保存、回滚和旧代理探测返回冲突。前端 9 个文件共 349 个测试通过，前端生产构建通过。全仓套件跳过。迁移目录、迁移契约和 runner 与原 KKAI 发布源码逐字一致。生产只读检查中，使用请求头变量模板或内部签名头覆写的存量账号均为零。生产镜像、迁移账本、切流和回滚以本版基础设施发布记录为准。
