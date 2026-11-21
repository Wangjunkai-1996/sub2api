# Sub2API v0.2.24 合并方案

评审日期为 2026-11-21。**推荐以官方稳定版 v0.2.24 为第一父，合并已发布的 KKAI v0.2.23 源码，建立 `release/v0.2.24-kkai`。** 这样可以整组引入官方入口校验、OAuth 轮换和协议修复，同时从上次实际交付的 KKAI 净差异出发。两个父提交分别保存来源，今后升级仍有明确的共同基线。

正式对象已按用户授权经 Clash 代理获取，并在本地完成双父合并。合并提交为 `f85ffdf83`，第一父为官方 `v0.2.24`，第二父为 KKAI `v0.2.23`。后续运行时修复提交为 `2ac606251`，fork 退役清理提交为 `d04fd66ba`，方案文档在实现提交之后追加。应用代码和本地 release 分支已变更，迁移文件、数据库和生产环境未变更。文件范围、8 个文本冲突和运行时兼容性已完成核对。[逐文件清单](kkai-v0.2.24-merge-paths.tsv) · [官方发布说明](https://github.com/Wei-Shaw/sub2api/releases/tag/v0.2.24)

## 1. 冻结输入与覆盖范围

| 输入 | 精确标识 |
| --- | --- |
| 共同基线，官方 v0.2.23 | `5af9afb588c4b014b3df9db7d4073af1f8ae01b6` |
| KKAI，`release/v0.2.23-kkai` | `ee05e2a1736d54d6afdde7f1dafb35ae2329b0ee`，含容器降权后的入口脚本权限修正 |
| 官方稳定版 v0.2.24 | `5c2982a81b46e2142d0143d2f83c1ce9c4da1d24`，北京时间 11 月 20 日 17:19 发布 |
| 从官方缓存重建的源码根树 | `0430966c6d4c317d9b4fedce4465f39f0381eed3` |

官方比较给出 **110 个新增提交、190 个变更路径**，含 52 个新增和 138 个修改，+11,853 / −610 行。KKAI 相对共同基线的 Git 常规统计为 **561 个变更文件**，+84,261 / −11,502 行。把 8 笔迁移重命名的旧、新文件名分别计数后，KKAI 有 **569 个路径**。完整并集为 **713**，其中官方独有 144，KKAI 独有 523，双方改动 46。两侧内容完全相同的 3 个路径也计入交集。

离线审查使用已缓存的官方完整递归树和版本比较。190 份补丁逐一应用到共同基线，每份结果均与官方 Git blob 哈希一致。重建后的 4,790 个文件的路径、模式、对象 ID 以及 271 个子目录树 ID 均与清单一致。在临时 Git 对象库指定共同基线运行三方预演，得到 **8 个文本冲突，38 个共同改动路径自动合并或已相同**。附表每行给出三份对象、预演结果和计划处置。范围外新发现的必要调用方应在实施记录补列。[官方版本比较](https://github.com/Wei-Shaw/sub2api/compare/v0.2.23...v0.2.24)

## 2. 官方功能的接收范围

| 业务域 | 随 v0.2.24 接收的变化 | 与 KKAI 一起验收 |
| --- | --- | --- |
| 网关入口与审计 | 请求字段歧义校验、模型白名单、BOM 与 multipart 处理，按协议返回读取错误，文本路由提前限制请求体。同步官方审计扫描、快照和性能调整 | HTTP、WS 首帧与后续帧、透传及兼容桥都先校验再派发。保留可信 NewAPI 归属、签名及 nonce、审核时序和错误脱敏。拒绝的请求无上游派发及计费 |
| 请求解析与性能 | 根字段单遍扫描、已验证的请求体复用、工具 schema、命名空间、图片意图和 cache_control 处理优化 | 接收 `ParseGatewayRequestValidated` 的调用方必须先完成验证。请求体替换、协议改变及重试后重新取得匹配的派生字段，输入字节和共享快照的所有权正确 |
| 身份与账号 | OpenAI OAuth 已轮换凭据的有界持久化与并发保护，ChatGPT OAuth 模型匹配，Codex 文献检索端点的 403 分类，OpenAI 请求超时的故障切换 | PAT、影子账号、Agent Identity、主出口、管理员重新授权与调度缓存一致。文献权限 403 只影响该请求，真实账号访问异常仍按对应策略处理 |
| 模型与协议转换 | Codex ToolSearch、音频能力、VS Code IDE 上下文、Responses 每轮推理强度，stop 序列、Kimi 工具、DeepSeek thinking 续接、Gemini 最后一轮工具结果及 System One 模型映射 | 核对 HTTP、原生 WS、WS 转 HTTP、Messages 和 Chat 的真实入口。保留本轮请求值、最终生效值与实际计费模型 |
| Anthropic、Gemini 与 Antigravity | Claude Code 上下文压缩的 5 分钟缓存 TTL、Claude 5.5 合成路由 thinking 适配、长上下文元数据和账号测试采样参数。Antigravity 429 作用域、按账号转发端点查询额度、Gemini SSE 注释帧 | Claude OAuth / Setup Token 的窗口估算、Claude/Gemini 刷新、Vertex/Bedrock 账号测试覆盖相应平台。KKAI 的 OpenAI 429 恢复按自己账号和轮次处理 |
| 图片、搜索与计费 | 流式图片 usage 压成一行、缺失的缓冲 Responses 输出重建，Grok 搜索使用 xAI 成功次数、图片编辑支持五张并保留输入错误。Exa 提供商和搜索来源块 | 对照上游终态、实际图片和结算记录。保留已确认零输出、未知结果、已有可交付图片三种不同结果，避免重复生成、扣费或退款 |
| 管理端与运维 | 管理员建用户初始化平台限额、频繁手动刷新 token 提醒、批量透传编辑保留模型过滤、模型广场复制 ID、运维 Tokens/s 改按累计耗时计算 | 同步 API 类型及中英文文案。管理员新建和普通注册采用相应默认限额。额度列表、出口编辑、后台冷却设置与新搜索配置可以共存 |

## 3. 八个文本冲突的明确解法

| 冲突文件 | 采用的实现与验收要点 |
| --- | --- |
| `backend/internal/service/oauth_refresh_api.go` | 用官方锁租期预算、有界脱离取消的成功落库、CAS 后重读、token cache 失效和锁释放。将 KKAI 的核心身份比较、管理员并发编辑优先和 Agent Identity 注册用到的接口接入同一套流程。token、账号/组织身份、runtime/task 身份已改变时采用数据库中的新状态。允许安全合并其他字段时，只应用本次轮换实际改变且未被管理员同时更改的字段。删除字段也作相同判定。对外结果取最终持久化的状态 |
| `backend/internal/service/oauth_refresh_api_test.go` | 统一 CAS 测试桩，保留官方的租期不足、可选补全超时、取消后已轮换、落库失败，以及 KKAI 的并发重新授权、代理编辑和身份注册用例。核对 DB、返回凭据、缓存、调度 outbox 四者的一致性 |
| `backend/internal/repository/scheduler_cache.go` | 在 KKAI 完整账号投影的副本上采用官方 `Groups` 精简和 `AccountGroups` 扁平化，保留分组 ID、出口模式/修订/绑定、元数据投影版本及脱敏。序列化不能修改调用方对象。新旧节点交替读写时覆盖缺标记回源、账号冷却和出口最终准入 |
| `backend/internal/service/admin_service.go` | 构造函数同时接入官方 `UserPlatformQuotaRepository` 和 KKAI `EgressService`，分别落到对应字段。走真实管理建用户和出口管理路径验证 |
| `backend/cmd/server/wire_gen.go` | 源构造函数与 provider 确定后，从 `backend` 运行 `go generate ./cmd/server`。核对生成的两项依赖、worker 生命周期以及所有直接构造 `NewAdminService` 的测试调用方 |
| `backend/internal/service/openai_ws_http_bridge.go` | 采用官方从每轮入口捕获并传递 `RequestedReasoningEffort` 的做法，删除桥内的重复计算和赋值，保留生效的 `ReasoningEffort`。沿用官方当轮故障处理，保留 KKAI 对可重放历史、会话归属和出口绑定的额外校验 |
| `backend/internal/service/openai_images_direct_test.go` | 分别保留官方多行 usage 单行发送，以及 KKAI 完整合法 JSON 转换和非法 JSON 拒绝回退的测试。覆盖已派发、已有图、明确零图及最终错误封装 |
| `frontend/src/components/account/AccountUsageCell.vue` | 采用官方刷新键、防陈旧异步写回、未知使用率和窗口费用估算。融合 KKAI 被动读取、显式主动刷新及快照反馈。列表、单元格与批量缓存用同一失效规则，旧账号或旧刷新键的迟到结果不能覆盖当前行 |

## 4. 自动合并仍需人工处理的行为

1. **仓储会留下两个同名方法。** 预演所得 `account_repo.go` 有两个 `UpdateOpenAIOAuthCredentialsIfUnchanged`。保留官方按平台共用的底层 SQL，OpenAI/Grok 各留一个公开入口。核实凭据和 proxy 比较、影子账号限制、事务内 outbox 以及成功后的快照更新。统一服务接口后同步 `openai_agent_identity.go` 的实际调用。
2. **构造函数与版本号。** 两个 scoped-admin 测试文件在双方连 blob 都相同，但各自追加的末尾参数代表不同依赖，要与 API 契约测试一起适配融合后的签名。双方的 `backend/cmd/server/VERSION` 也都写着 `0.2.23`。本次源码显式更新为 `0.2.24`，发布包装器再注入 KKAI 版号和最终提交。
3. **双窗口与刷新。** 后端改用官方的账号 ID 加窗口时长为键，并校验起点和时效，分别计算 5 小时及 7 天统计。保留 KKAI 的既有快照和额度读取语义。批量编辑要分别覆盖勾选和未勾选模型过滤、透传、出口配置。后台主动刷新 OAuth token 与读取额度各走自己的接口。
4. **结算和流式重试。** 官方 408、流式首输出前故障切换和 KKAI 的 120 秒可用首输出上限、11 次总派发预算同时生效，遵守单项更严的次数限制。已有业务输出、取消、已派发且结果未知等情形按原有重放边界处理。核对胜出尝试的 TTFT、请求 ID、实际模型/tier、Grok 搜索和混合 token/图片使用量。按上游终态保留 NewAPI 的 `image_provider_error` 证明、脱敏及账务幂等。[图片协议与用例](../docs/kkai/image-failure-development-20261115.md)
5. **共有 3 个跨版本调用缺口。** `855bdb83b → b67bc7b19` 的 handler 对比显示下表中的旧调用在 v0.2.22 合并时移除。它们独立于新版本的 8 个文本冲突。对当前缺口先留入口回归，再做最小修复。

| 路径 | 当前源码证据 | 在 v0.2.24 的处理 |
| --- | --- | --- |
| Pro OAuth 账号 Cyber 冷却 | 管理设置和调度复核仍在。处理上游事件的入口已失去 `ApplyCyberPolicyAccountCooldown` 调用 | 在符合现行策略的真实上游事件恢复一次写入，覆盖重复事件、有效分组、开关关闭、调度复核和会话级保护 |
| HTTP Responses 失效续接 | 归属检查仍在，失效标记的读、写方法已失去业务调用。这两条调用在 managed recovery 删除之后仍存在 | 在归属校验与派发之间检查失效状态。收到确定的上游失效信号时写标记，按原归属清理。验证跨实例、存储异常、旧会话及并发新绑定。维持现有用户/会话封锁，拒绝已失效续接 |
| WS 出口池跨轮租约 | 旧 handler 使用 `shouldHoldOpenAIWSPoolLease` 管理整条连接的出口租约。当前 handler 仍定义该函数，轮末释放后续轮再申请账号槽。现有 helper 测试不足以证明真实连接行为 | 针对真正取得出口租约的池账号做两轮 WS 入口测试，同时覆盖 legacy 并发槽、连接断开、租约丢失和后续轮故障切换。若其他层已维持同一有效租约，保留实现并移除无用 helper。若入口测试揭示租约提前释放，只补连接级持有/释放逻辑 |

官方已含当轮重试载荷处理和原生后续轮限额时拒绝重放首包的用例。KKAI 在桥接路径额外检查历史是否完整、前一响应 ID 是否连续，并在不可安全重放时结束当轮。对原生 WS、passthrough 与 HTTP 桥分别验证两轮请求，核对安全重试、拒绝重放两种结果及当轮的账号、出口、计费分组和原始模型。

## 5. Fork 功能取舍

| 决定 | 范围与理由 |
| --- | --- |
| 随合并清理 | 删只有定义的旧 `OpenAIAccountControlProxyURL`。刷新和额度查询已经调用 `ResolveOpenAIAccountControlProxyURL`。删除失效的 `docs/CODEX_ACCOUNT_STATE.md` 启用指南以及中英文 README 入口。可直接复核的无调用小函数如 `egressPoolError` 和 `stringValueOrDefault` 可同批清理 |
| 采用官方单一实现 | 删除重复 OAuth 仓储 SQL、WS 桥的 requested effort 重算，以及已经由官方窗口键覆盖的旧单窗口缓存逻辑。两边验证不同业务行为的测试继续保留 |
| 有实际消费者，保留 | NewAPI 在 OpenAI 流式请求发送 `X-Sub2-TTFT`，消费 SSE 注释并写入 `sub2_ttft_ms` 和 `frt`。保留只读额度查询、原生 STATE 与跨账号保护、HTTP/2 回退、真实 429 恢复、HMAC 归属、利润门和图片失败证明。保留出口池、legacy 准入、Live、代理身份验证、绑定与容量限制。[NewAPI 的计时契约](/Users/tokk/Documents/Codex/2026-08-04/s-y-s-s/new-api-kkai/docs/kkai/first-response-timing.md)
| 有条件另案下线 | OpenAI 原生 HTTP 网络追踪默认关闭，运维下线后会失去 DNS/TCP/TLS、连接复用及网络首字节明细。出口池 `shadow` 会失去预演容量、候选数及诊断日志。当前缺少线上配置和运维使用证据，这两项在 v0.2.24 继续兼容。确认没有依赖后单独移除，旧 `shadow` 值按 `off` 解释并保留 legacy 准入 |
| 历史退役 | 分组级调度覆盖、整段流缓存后提交、Codex 五小时自动暖机、主动 STATE 票据、Traffic Director、managed text recovery 均保持退役。保留历史迁移、旧路由 404、导入过滤、敏感字段脱敏和现役会话清理。全局调度、活跃会话状态和正常配额刷新仍是有效业务能力。[退役实施记录](KKAI_FORK_RETIREMENT.md) |

## 6. 实施顺序和验收

**取得正式对象。** 按升级手册使用本地正式 tag。可导入包含 tag 与历史的本地 Git bundle，或通过独立获授权的代码同步经 Clash 代理取得该 tag。已有缓存足以预演源码树。建立 release 分支前核对正式 tag 解引用的 commit、根树、共同基线与第 1 节相符，重新核对当前 KKAI HEAD。下方为实施时从仓库根执行的命令。

```sh
export GIT_NO_LAZY_FETCH=1
git status --short --branch
git rev-parse 'v0.2.24^{commit}' 'v0.2.24^{tree}'
git merge-base v0.2.24 ee05e2a1736d54d6afdde7f1dafb35ae2329b0ee
git switch -c release/v0.2.24-kkai v0.2.24
git merge --no-commit --no-ff ee05e2a1736d54d6afdde7f1dafb35ae2329b0ee
git diff --name-only --diff-filter=U
```

**完成合并。** 先解第 3 节冲突，同步第 4 节调用方，生成 Wire。本版上游没有 Ent schema 增量，沿用当前 KKAI schema 与配套产物。对两边的差异分别审查。逐行核对附表与最终索引，特别检查自动合并的依赖、测试桩和单边入口是否仍能触达。先形成有双父的可构建合并提交，再用小提交分别收口历史入口回归和已明确的清理。提交内容只包含本次工作。

**验证从真实入口开始。** 下列是合并后可直接使用的定向检查，Go 命令在 `backend` 执行，前端命令在仓库根执行。补齐三个入口缺口时，把相应新用例加入测试筛选或直接运行其精确名称。检查运行结果中的匹配用例。跳过或零匹配单独记录。

```sh
go test -tags unit ./internal/pkg/requestmodel ./internal/server/middleware ./internal/server/routes -run 'Test(ConflictingField|RejectConflictingRequestFields|RetiredForkRoutes)'
go test -tags unit ./internal/service ./internal/repository -run 'Test(RefreshIfNeeded_OpenAI|.*AgentIdentity|MarshalSchedulerCacheAccount|SchedulerCacheMetadataProjection|BuildSchedulerMetadataAccount_KeepsSlimGroupMembership|GatewayCacheOpenAICyberAccountCooldown)'
go test -tags unit ./cmd/server ./internal/server ./internal/server/routes ./internal/repository -run 'Test(APIContracts|ScopedAdmin|.*Wire)'
go test -tags unit ./internal/handler ./internal/service -run 'Test(RecordCyberPolicyIfMarked|OpenAICyberAccountCooldown|InvalidateOpenAIHTTPContinuation|OpenAIWSStateStore_HTTPResponseInvalid|OpenAIResponsesWebSocket_NativeLaterTurn|ShouldHoldOpenAIWSPoolLease|ProxyOpenAIWSHTTPBridgeTurn|OpenAI429|OpenAIHTTPStreamReplay)'
go test -tags unit ./internal/service -run 'Test(ForwardAlphaSearch|CodexDirectImagesStream|ForwardOpenAIImagesAPIKey|OpenAIImagesProvider|OpenAIChatTTFTComment|OpenAITTFTComment|OpenAIGatewayServiceForwardImages_APIKeyRequestTimeout)'
```

从仓库根执行前端定向测试。

```sh
pnpm --dir frontend exec vitest run src/utils/__tests__/accountUsageRefresh.spec.ts src/components/account/__tests__/AccountUsageCell.spec.ts src/components/account/__tests__/BulkEditAccountModal.spec.ts src/components/account/__tests__/EgressPoolSelector.spec.ts src/views/admin/__tests__/AccountsView.usageWindowsHint.spec.ts src/views/admin/__tests__/SettingsView.spec.ts
```

随后验证实际受改包及直接依赖，包括官方协议转换、搜索、审计、模型路由和管理端。按发布手册完成修改包单测与受影响前端类型/构建验证。PostgreSQL 验 OAuth 条件写入、outbox 及建用户限额。Redis 验新旧节点双向读写、出口/冷却、续接标记和 429 恢复。离线使用固定夹具核对同一请求的模型、tier、费用、归属与图片失败结果。无须等待 Ultrafast 自然流量。通用长时或全套命令按项目的成本规则处理。

**核账并固定候选。** 官方 v0.2.23、v0.2.24 各有 305 份迁移 SQL，两者的文件名、内容相同。KKAI 有 325 份，保留 20 份自有 SQL、8 对历史重命名及已登记校验。预期候选与 `ee05e2a173` 的全部 325 份文件逐字一致。这轮没有计划内生产迁移。官方增量也未改 Go 依赖文件、前端锁文件或 Dockerfile。保留 KKAI 的 go.sum 项、Docker 入口权限、`-migration-contract` 和健康检查。示例核对命令在合并、修正已提交后运行。

```sh
git diff --exit-code ee05e2a173 HEAD -- ':(glob)backend/migrations/*.sql'
git diff --check v0.2.24 HEAD
git diff --stat v0.2.24 HEAD
git diff --stat ee05e2a173 HEAD
```

正式对象就绪后，使用以下命令独立复核清单。前两份差异应分别列出 190 和 569 个路径。合并预演退出码 1 对应第 3 节的 8 个冲突。附表中 `A`、`M`、`D` 和 `=` 分别表示新增、修改、删除和与共同基线相同，最后三列是各版本的 Git 文件模式及 blob ID，缺失记为 `-`。

```sh
git diff --no-renames --name-only v0.2.23 v0.2.24
git diff --no-renames --name-only v0.2.23 ee05e2a173
git merge-tree --write-tree --name-only --no-messages --merge-base=v0.2.23 v0.2.24 ee05e2a173
```

**生产交付。** 实施并验证后采用 `release_scope=both`。文本角色实际执行入口校验、OAuth 调度和 WS。图片角色实际执行图片转发、usage 编码、Grok 编辑和 account-test。由 Mac 通过既有 `kkai-mirror-builder`、Docker 代理和发布包装器从精确提交构建唯一的 linux/amd64 制品，检查内嵌版本、提交、迁移契约和双端归档摘要。单次上传至 `sys1`，审阅 `plan-stage` 的精确计划。候选启动前由控制器比对实际数据库账本，出现 pending、缺文件或未知 checksum 即停在迁移检查。

按有效 router 分别发现文本和图片 active，保留旧 active 作为回滚对，按控制器原位热重载保持配置 inode。候选健康和直连通过后走 **10% → 50% → 100% → complete**，逐级至少 120 秒。验文本 Responses/SSE 正常终态、图片实际有效结果、image 与 account-test 同槽、候选与稳定侧的完整脱敏错误窗口。已证实无关的错误按 v1 人工审阅契约记账并继续。确认回归则使用该 release ID 回滚。完成以即时和延迟状态、有效路由、受保护容器、回滚就绪及按手册清理后的复核为准。[生产发布手册](/Users/tokk/Documents/Codex/runbooks/sub2api-upgrade-and-deployment.md)

## 7. 实施记录

### 7.1 已落地内容

- 已按授权只从官方 origin 获取 v0.2.24，对象、根树和共同基线与预演记录一致。
- 已建立 release/v0.2.24-kkai，正式双父合并为 f85ffdf83。8 个文本冲突已人工解决，Wire 已生成，版本已更新为 0.2.24。
- 2ac606251 收口 Cyber 账号冷却、HTTP 续接失效围栏、WS 出口租约跨轮复用、OAuth 轮换 outbox 测试、官方夹具适配和前端迟到结果测试。d04fd66ba 清理无调用的旧控制代理函数、出口错误函数、导入默认值函数和已退役的 Codex STATE 启用指南。
- 保留 ResolveOpenAIAccountControlProxyURL、原生 STATE、跨账号保护、真实 429 恢复、TTFT、图片失败证明、网络追踪、出口池和 shadow。迁移 SQL 与原 KKAI v0.2.23 完全一致，未执行数据库迁移。
- 合并路径清单覆盖原始 713 个输入路径。实施阶段新增的回归入口文件为 backend/internal/handler/openai_continuation_egress_test.go，已进入 2ac606251。

### 7.2 验证命令与结果

工作目录为仓库根，Go 命令在 backend 执行。

```sh
env GOPROXY=off GOTOOLCHAIN=local go test -tags unit ./internal/handler ./internal/service ./internal/repository ./internal/handler/admin -run 'Test(RecordCyberPolicyIfMarked|OpenAIResponses_RejectsPreviouslyBlockedContinuationAcrossInstances|OpenAIResponsesWebSocket_PoolLeaseSurvivesTwoBridgeTurns|OpenAIResponsesWebSocket_NativeLaterTurnUsageLimitDoesNotReplayFirstMessage|InvalidateOpenAIHTTPContinuation|OpenAIWSStateStore_HTTPResponseInvalid|.*LeaseLoss|LegacyEgressLeaseLoss|OpenAICyberAccountCooldown|.*AgentIdentity|AdminService_CreateUser|OpenAIOAuthRotationPublishesOneOutboxEvent)$' -count=1
env GOPROXY=off GOTOOLCHAIN=local go test -tags unit ./internal/service ./internal/repository ./internal/handler ./internal/handler/admin -run '^$' -count=1
env GOPROXY=off GOTOOLCHAIN=local go build -o /tmp/sub2api-v0.2.24-server ./cmd/server
git diff --exit-code ee05e2a1736d54d6afdde7f1dafb35ae2329b0ee HEAD -- backend/migrations
git diff --check
```

上述 Go 定向测试、受改包编译和服务构建均通过。部分仓储和管理包的筛选没有匹配测试，随后由 PostgreSQL/Redis 定向集成覆盖。

```sh
env GOPROXY=off GOTOOLCHAIN=local go test -json -timeout 120s -tags integration ./internal/repository -run 'Test(AccountRepoSuite|UserPlatformQuotaRepository_BulkInsertInitial_Idempotent|AccountEgressCacheSuite)$/^(TestOpenAIOAuthRotationPublishesOneOutboxEvent|TestV2RollbackWriterIsolatedFromV3Config|TestFIFOReleaseAndRefreshFencing)$'
pnpm --dir frontend exec vitest run src/utils/__tests__/accountUsageRefresh.spec.ts src/components/account/__tests__/AccountUsageCell.spec.ts src/components/account/__tests__/BulkEditAccountModal.spec.ts src/components/account/__tests__/EgressPoolSelector.spec.ts src/views/admin/__tests__/AccountsView.usageWindowsHint.spec.ts src/views/admin/__tests__/SettingsView.spec.ts
```

PostgreSQL/Redis 定向集成的 6 项实际用例全部通过。前端 6 个文件共 250 项通过。测试输出保留了既有的 jsdom、router-link、语言环境 watch 警告和主动刷新失败日志，它们没有造成失败。

官方新增测试首次有 27 项因旧测试夹具缺少 KKAI 的 StatusActive 提前退出，补齐 3 个夹具文件后按 upstream-recheck-command.txt 定向复跑通过。没有等待 Ultrafast 自然样本。

### 7.3 尚未执行的范围

本次未执行全仓测试、生产镜像构建、镜像上传、蓝绿 stage/promote、代码托管 push 或生产数据库变更。生产交付仍须按本手册第 6 至第 11 节重新发现实际拓扑并完成迁移、候选健康、有效路由、错误窗口和回滚门禁。
