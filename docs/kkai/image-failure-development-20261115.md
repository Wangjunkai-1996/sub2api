# 图片明确失败协议：Sub2API 开发文档

状态：v1.1，主方案负责人已批准，实施与独立审查修正完成，源码冻结，最终修改包验证完成。更新：2026-11-16（Asia/Shanghai）。

依据：NewAPI `docs/kkai/image-failure-contract-20261115/plan.md` v1.0 及协议审查 P-01～P-06。工作目录为 `/Users/tokk/Documents/Codex/2026-08-09/li/work/sub2api`，分支 `release/v0.2.21-kkai`，起点 `4d32760b5834378b6a6b9d4ac10a66f551de86db`。不创建分支/worktree，不做代码托管联网操作，不改 schema、依赖或文本语义。

## 1. 事故实际路由与范围

通过 `sys1`（主机 `ovh-sys1-prod`，用户 `tokk`）只读日志核实：

- 22:06:36.369：`Images request routing request_model=gpt-image-2.6-sunburst upstream_model=gpt-image-2.6-sunburst endpoint=/v1/images/generations account_type=apikey`。
- 请求 `f25adfe6-a717-4a6a-97a0-00137d7541cc`、client request `9d805a73-1954-4d7f-ad44-d7cc724ec02f` 在 22:08:46.849 记录 `account_id=16841,upstream_status=502,switch_count=1`，随后排除唯一账号并耗尽。
- 同账号当前只读 `accounts` 查询为 `type=apikey,platform=openai`。`updated_at` 晚于事故，所以这条当前记录仅作交叉核对，历史路由以前述当时日志为证。未查询或输出 credentials。
- 持久化 `ops_error_logs` 没有对应请求，历史完整响应未恢复；不能证明当时 JSON 已符合本次新增结构条件。回归使用与事故特征相同的受控 fixture。

`ForwardImages` 按 `account.Type` 分到 API-key 与 OAuth。事故属于 `forwardOpenAIImagesAPIKey`，本次新增明确失败来源限于原生 Images JSON HTTP 错误及已有成功 HTTP 状态中的 error envelope。覆盖 generations、JSON/multipart edits、原生非流式多图，以及 JSON keepalive。

OAuth direct、Responses 和 batch 不新增终态证明，也不重构其错误选择、retry 或部分成功结算。它们发生模型请求派发后，本次协议视为“未取得本协议认可的失败证据”；这一保守状态跨账号/路由粘滞，不能被后续 native 明确失败覆盖。已完成图片仍按现有成功/部分成功路径交付结算。

## 2. 内部状态与判定

最小新增内部字段（均不进入客户端请求 JSON 或数据库）：

| 位置 | 字段 | 用途 |
| --- | --- | --- |
| `OpenAIImagesRequest` | `hasUnprovenOutcome bool` | 一个逻辑请求跨账号/分支保留的粘滞否决位；默认 false，只可从 false 变 true |
| `UpstreamFailoverError` | `ImageOutcomeDefinitive bool` | 仅来源层设置，表示本次整个请求已有被认可失败证据且此前无未证实结果；默认 false |
| `UpstreamFailoverError` | `ImageFailureMessage string` | 已脱敏、有界的原始错误原因，供最终出口和 ops 使用；不存 key/raw body 副本 |
| `OpenAIImagesUpstreamError` | `ImageOutcomeDefinitive bool` | 直接返回的 HTTP200 内嵌错误保留同一证明，序列化前仍受既有专用语义和状态限制 |

`hasUnprovenOutcome` 不表示供应商一定完成或未完成，仅表示网关没有可用于整单失败 marker 的证据。它保存在原请求对象，handler 跨账号复用同一对象，不能随错误对象或 batch 的单次副本重置。

状态来源：

1. native `doOpenAIUpstream` 返回错误，只有已有 `sendState.mayFailover(...)` 明确证明未发送才保持原状态。不能把 trace callback 缺失当作未发送。其他 transport/read/limit 错误设置否决位，保持当前禁止重放的返回路径。
2. native HTTP 错误首次读取完整且严格结构成立，才能生成证明；结构不成立设置否决位。确认失败不增加重试资格，也不改变现有账号健康策略。
3. native SSE 不属于新增证明来源，实际派发后设置否决位；已有 SSE 结果/错误写出规则保持不变。
4. OAuth direct/Responses/batch 在真实 `doOpenAIUpstream` 返回后设置否决位；明确的未发送 transport 分支先返回，不设置。凭据获取、建请求、参数验证等派发前返回同样不设置。batch 任一请求一旦派发，该请求对象的否决位保持 true，不依赖 `firstFailure` 或最终被选中的错误。
5. native 直接 HTTP200 error 在写出前检查此前否决位；即使当前 body 完整，也不能覆盖早先未知。final failover 对象的证明在服务层生成时已经合并此前状态。

## 3. 首次读取与严格证明

native HTTP>=400 入口改用已有 `readUpstreamResponseBodyLimited(resp.Body, openAIUpstreamErrorBodyReadLimitForConfig(s.cfg))`。保留原有错误体长度上限，并补 max+1 探测，立即处理 read error；不能先调用会忽略读错的 `readUpstreamErrorBody` 再对 bytes reader 宣称完整。读取失败不通过现有 HTTP failover 路径重新派发已接受的图片请求。

HTTP200 入口继续使用 `ReadUpstreamResponseBody`；它已经传播 read error/超限。证明建立在原始完整 body 上，先证明、再脱敏，不能让脱敏结果改变分类。

共享的图片专用判定须同时满足：

- 整个 body 是合法且完整的 JSON 根对象，尾随空白允许，尾随垃圾拒绝。
- 使用标准库 JSON token visitor 拒绝任何重复键作为证明，避免 gjson 首值与消费端末值语义不一致；不改变普通业务 JSON parser。
- `error` 是对象；`error.type` 和 `error.message` 都是实际 JSON string 且 trim 后非空。不用 `.String()` 隐式转换数值、bool、数组或对象。
- `error.code` 可缺失、null 或未知；不通过自然语言、耗时、502 状态单独推断。
- 无已支持格式的可交付图片输出。复用原生图片计数/输出解析能力，并用回归确认 `data[].b64_json`、`data[].url` 和已有 Responses 图片 result 形状不会被误判；缺少可复用支路时只补最小防误标检测。
- 已有 policy、credential、balance、capacity、request validation 专用错误优先；通用 provider failure 才输出新 marker。

HTTP200 的畸形 error 仍走安全普通失败/未知处理，不能因不符合严格证明被错误地转换成成功；若存在可交付输出，则保留既有成功/部分成功路径。

## 4. 最终错误出口

所有 `handler/openai_images.go` 的耗尽路径（选不到下一账号、次数耗尽、429 停止、已有流字节）统一使用来源证明。共享 `handleFailoverExhausted` 只在显式图片来源证明成立且专用分支全部未接管时改变输出；文本来源默认零值，输出不变。

| 出口 | 本次行为 |
| --- | --- |
| 默认通用最终映射到 HTTP502 | type=`upstream_error`, code=`image_provider_error`，message 为有用脱敏原因 |
| 仅 JSON whitespace keepalive 已提交 HTTP200 | 最后仍追加一个合法 JSON error envelope，允许同一 marker，运维保留 intended status502 |
| 透传保留/映射为502 | 保留规则选择，带 marker；custom message 优先，其次来源安全 message |
| 透传明确映射为其他状态（含显式200） | 保留规则 status/message，不发本协议 marker；即使已有 keepalive200也不得用它掩盖规则语义 |
| policy/credential/balance/capacity/body-limit/model-validation | 原专用分支优先，不覆盖 type/code/status、不改变处罚和重试 |
| SSE 已写实际语义字节/图片结果 | 走已有流序列化，不补第二个普通 JSON；没有本次 HTTP JSON 来源证明时不新增 marker |
| 任意前序未证实派发结果 | 抑制 marker；保留原错误出口及 NewAPI 核对行为 |

优先复用 `handleStreamingAwareErrorWithCode`。默认和透传 helper 均从 `ImageFailureMessage` 取新诊断，不能再次从原始 body 直接提取敏感文本。`MarkOpsUpstreamFinalOutcome` 记录原 provider status 和最终 status，非502规则的 marker 抑制在本次测试/开发验收记录中明确，不能声称现有 NewAPI 对任意映射状态均立即退款。

诊断可用与终态证明独立：`ImageFailureMessage` 非空时，即使 unknown 否决 marker，也将已净化原因发送给 NewAPI 管理员诊断；code 的证明门禁不因此放宽。三个图片 error serializer 用同一 `clientErrorCode` 检查 reserved marker，匹配采用 TrimSpace+EqualFold，与 NewAPI 消费者一致。native/OAuth direct 原样 SSE 中也剥离上游自填的 reserved code；重复字段的 SSE 帧先以标准库 UseNumber 规范化，再剥离并按实际输出计数，不扩展 OAuth 终态判断。HTTP200 同时有 error 与可交付图时移除矛盾 error，保留真实输出及结算。

## 5. 安全 message 与 ops

现有 `sanitizeUpstreamErrorMessage` 只覆盖有限 query 参数，`redactAgentIdentitySensitiveBody` 只针对 Agent Identity；两者不足以直接安全转发 API-key 自由文本。不得把这两点写成已有完整保护。

新增图片专用、可复用的短 message 净化函数，固定最多 **2048 UTF-8 字节**，步骤如下：

1. 在持有本次实际 token、account/base URL 的服务层先移除准确值回显；兼容 JSON escaped 回显。凭据不保存到 failover 对象，不传到 handler。
2. 匹配前将 Unicode 空白变为空格并移除其余控制/Cf 字符，避免重新拼出漏扫的 key/URL。复用现有 query 清理，补齐 Authorization（含 Basic）/Bearer、常见 API-key/token/secret/password/credential JSON/等号/冒号形式、`sk-` 格式、任意scheme URL、地址/IP；配置 base/proxy URL 的 Hostname 也精确移除。
3. 非法 UTF-8、超过64KiB、完整 JSON/HTML 正文回退安全摘要；普通短自然语言保持，最后按 UTF-8 边界限2048字节。空值同样回退 `Image provider returned an error`。不在清理失败时回退原文。
4. HTTP200 分支的原始 provider type/code 仅作为安全短标识的 ops detail；不由 marker 覆盖原 provider status。policy 使用固定安全摘要。
5. 自定义透传文案优先，但仍经过通用格式/URL/控制字符/长度清理；有效 token 的精确清理在来源消息离开服务层前完成。handler 最终规则文案没有活动 token，不能声称能识别操作者自行填写的任意无标签秘密；不为此把 token 放进 failover/持久化对象。
6. native 非failover出口在同步调用期间显式传递活动敏感值，使用同一净化器生成message和安全标识；该路径停用raw body日志，参数原文不外发。OAuth传nil保留既有行为。type/code仅允许128字节以内ASCII标识且净化后不变；对象/数值字段不能通过 `.String()` 变成policy或其他专用终态。

ops 保留被净化的 message 与原 provider status，HTTP200 分支另保留安全 type/code detail；HTTP>=400 的原始 type/code 仍在内存 ResponseBody 供规则匹配，不承诺新增独立持久化。NewAPI 取得的是最终 wire type/code 和安全原因，不能恢复生产者未发送的原始字段。本次新增出口不把 raw response body 或新自由文本直接写系统日志。现有请求/账号 ID 作为关联坐标。只改本次触及的图片诊断边界，不扩展成全服务通用日志重构。

## 6. 测试与验证

跨端 fixture 写在 `backend/internal/service/openai_images_apikey_error_test.go` 和 handler 事故回归：

```json
{"error":{"type":"upstream_error","message":"抱歉，刚才图片生成服务出现错误，这次没能成功生成 BLG 夺冠海报。目前无法提供成品图片。"}}
```

HTTP502 输入允许 code 缺失；最终 wire 为相同 type/message，加 `code=image_provider_error`。NewAPI worker 使用该最终 envelope 验证立即 failed/refunded，一次 POST/一次退款。无需制造130秒延时。

必须覆盖：完整502/HTTP200、code缺失/null、generations/edits、未知类型/无效JSON/尾随垃圾、完整JSON附readErr、limit+1、error夹带图片、前序未知后明确失败、前序明确后未知、派发前失败后明确、单账号耗尽/次数耗尽、passthrough status/custom message、keepalive、专用语义、OAuth无新marker与部分成功。测试统计真实假上游调用次数，不能只检查私有bool。

优先扩展现有 `openai_images_apikey_error_test.go` 与 `handler/openai_images_failover_test.go`。图片专用 sanitizer/parser 可有小型确定性 table test；使用现有 testify，不加依赖或睡眠压力测试。

本机默认 Go1.26.5，模块要求 Go1.27.3；使用已授权的代理下载工具链，禁止 GOPROXY direct 回退触发代码托管 Git。所有 Go 命令在 `backend/`，共同前缀如下：

```sh
env HTTPS_PROXY=http://127.0.0.1:7897 HTTP_PROXY=http://127.0.0.1:7897 ALL_PROXY=http://127.0.0.1:7897 NO_PROXY=localhost,127.0.0.1,::1 GOPROXY=https://proxy.golang.org GOTOOLCHAIN=auto
```

该 env 前缀后分别执行：

```sh
go test -tags=unit ./internal/service -run 'Test(ForwardOpenAIImagesAPIKey_|OpenAIImagesProvider|OpenAIGatewayServiceForwardImages_|ImagesOAuth|CodexDirectImages|ImageStream|OpenAIImagesJSONKeepalive_|OpenAIImagesInsufficientBalance|ForwardOpenAIImagesOAuth_|OpenAIImagesRejectedDriver)' -count=1
go test -tags=unit ./internal/handler -run 'Test(OpenAIGatewayHandlerImages_|OpenAI.*Failover|HandleFailoverExhausted)' -count=1
```

依赖缓存完备后各预计1～3分钟。已通过代理取得 Go1.27.3，无 go.mod/lockfile 改动。正式发布前按全局手册执行修改包完整unit测试（`go test -tags=unit ./internal/service -count=1`、`go test -tags=unit ./internal/handler -count=1`），不以定向筛选冒充包完整通过。

使用 `gofmt` 只格式化修改 Go 文件，`git diff --check` 检查差异。不运行 `go test ./...` 或全前端套件。

已执行结果（共同env前缀同上）：

| 命令 | 结果 |
| --- | --- |
| `go test -tags=unit ./internal/service -count=1` | PASS，269.415s；审查后局部修改再跑上方定向范围 |
| `go test -tags=unit ./internal/handler -count=1` | PASS，64.171s；审查后局部修改再跑上方定向范围 |
| `go test ./internal/service -count=1`（最初无tag全包） | FAIL，180.892s；捕获到未改动的 `TestFilterThinkingBlocksSignedHistorySkipsDecode` allocation18不满足<16。长输出被工具截断，未声称已完整保存失败清单 |
| `go test ./internal/service -run '^TestFilterThinkingBlocksSignedHistorySkipsDecode$' -count=1` | PASS，0.056s；测试、被测函数、底层string-view代码及go.mod/sum与HEAD字节一致；未改阈值，未证明全包瞬态失败原因 |

以上完整 unit-tag 包成功属于审查修正前的历史验证；末轮包结果与最后增量验证见下文，不将隔离通过说成历史基线通过，也不把未执行的集成/登录测试说成通过。全仓套件未运行。

末轮定向 service 范围曾因新增三序列化器表测误取顶层 `code` 失败；实现写 `error.code`，修正后 `go test -tags=unit ./internal/service -run '^TestOpenAIImagesProvider' -count=1` PASS（0.061s），加入 UTF-8 门禁和fixture后最终 `TestForwardOpenAIImagesAPIKey_FailureContract|TestOpenAIImagesProvider` PASS（0.117s）。最终 handler 定向命令 PASS（6.349s）。

源码冻结后的完整包结果：`go test -tags=unit ./internal/handler -count=1` PASS（64.011s）；`go test -tags=unit ./internal/service -count=1` FAIL（268.543s），唯一失败为未改动的 `TestInflightEstimate_AccountMappingNoDBAndBoundedMemory`，HeapAlloc 8483248 超过 8388608 阈值。该测试用 `go test -tags=unit ./internal/service -run '^TestInflightEstimate_AccountMappingNoDBAndBoundedMemory$' -count=1` 隔离运行 PASS（0.178s），测试及被测 billing 文件与 HEAD 字节/hash 一致，不经过图片逻辑；完整失败输出保存在 `docs/kkai/image-failure-validation-20261116/service-unit.log`，隔离输出在 `inflight-isolated.log`。不修改该无关阈值、不把完整 service 包称为通过。`frozen-source.json` 记录源码快照SHA；最终源码在完整包失败后只增加了 `utf8.Valid(body)` 和对应fixture，因此完整包结果不代表逐字最终代码，最终定向命令代表该增量已验证。完整全仓套件和登录E2E未运行。

## 7. 发布与审查边界

release_scope=`image`，原因：新增字段默认false，只有图片入口及图片错误来源设置并消费新协议；共享 handler 的文本错误回归必须证明零值行为不变。不因修改共享 package 而发布 text。

按主方案冻结顺序，NewAPI 后端/前端完整验收收尾后，才进行 Sub2 image 发布；不并发 stage/promote。Mac 构建不可变制品、精确 migration contract、实际安装控制器身份及完整10/50/100/complete门禁由主协调者执行。历史路由证据不能替代发布时有效拓扑发现。

依用户要求，登录和真实账号业务 E2E 未由代理执行（用户接手业务测试，代理未验证）。本地 fixture 不冒充生产真实业务成功；缺样本、schema、身份或未归因失败按既定发布门禁停止。

主方案负责人已确认本开发文档并授权实施。独立代码审查见 `docs/kkai/image-failure-code-review-20261115.md`；两端提交/发布由主协调者执行，本开发阶段未commit、build、stage或promote。`docs/*` 被仓库ignore覆盖，提交时精确 `git add -f` 本开发文档和审查文档，不修改ignore规则。
