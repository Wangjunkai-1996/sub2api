# 图片终态失败协议独立代码审查

审查时间：2026-11-16（Asia/Shanghai）。审查角色：NewAPI 后端实现与资金安全复核代理，独立于 Sub2API 实现者。状态：最终独立代码审查批准。S-01 至 S-10 均已处置；定向图片协议与完整 handler 测试通过。完整 service 包仍有未修改的内存阈值用例失败，保留验证缺口，不宣称全包通过。

审查只读取当前 Sub2API 改动，不改变生产状态，不提交代码；NewAPI 已冻结的生产提交不在本轮修改范围。测试由实现者执行，本文区分静态审查、最终源码包门禁和定向增量验证。

## 范围与结论

检查新 `openai_images_failure.go`、原生 HTTP 图片入口、OAuth 累积未知标志、`OpenAIImagesUpstreamError` 序列化、共享 failover 最终出口及测试。安全诊断与无输出证明是本轮重点；handler 协议复核另由独立子代理完成。

总体方向符合冻结方案：证明只由完整原生 JSON 错误建立，未知结果跨账号粘滞；新 metadata 默认为零值，文本请求维持既有行为；新日志诊断不决定重试或账单。HTTP200 同时包含图片和 error 时保留图片、删除竞争错误字段，继续既有实际输出计数。

以下问题已直接发给实现者并完成静态处置复核；测试通过与发布批准分开记录。

| 编号 | 优先级 | 发现 | 要求的最小修正 |
| --- | --- | --- | --- |
| S-01 | P1 | 先删除所有控制字符会将 `Bearer\nunknown-secret` 合并为 `Bearerunknown-secret`，绕过凭据 scheme 匹配。 | 空白先统一为空格，仅删除其他 control/format 字符；补换行 Bearer fixture。 |
| S-02 | P1 | ASCII 短标识规则仍允许 `api_key:other-secret`、`password:other-secret` 与 `sk-other-secret`，可进入新的 providerDetails 或 clientError type/code。 | 复用现有凭据表达和 key 规则拒绝，精确活动 key 规则继续保留；补 metadata fixture。 |
| S-03 | P1 | 完整 base URL 替换不能删除 `lookup private.example` 中的配置上游裸域名；配置地址可进入新增 wire message。 | 用标准 URL 解析提取配置地址的 Hostname 并一并精确遮盖，覆盖实际上游和代理；不增加通用域名扫描。 |
| S-04 | P1 | `openAIImagesSSEErrorStatus` 未覆盖 `content_filter`，会把该专用语义在 native HTTP502 入口认成通用明确失败。 | proof 额外复用 `isOpenAIImagesContentPolicyRefusal(type+code)` 否决，与 HTTP200 专用处理保持一致。 |
| S-05 | P1 | HTTP200 `error.message` 为对象时，既有 gjson 转换器 `.String()` 会序列化对象；新诊断分支可能将原始 JSON 对象回传，而旧分支是固定摘要。 | 原 message 不是字符串时使用固定摘要；整段 JSON/HTML 响应文本保守回退，保留普通自然语言。 |
| S-06 | P1 | gjson `Get` 只取首个同名字段；重复 `data` 的首值为空、末值含图片时，现有输出计数会漏图并错误建立终态证明。 | 新 proof helper 窄拒绝相关重复键，包括根 error/data/output/response 和 response.output；不改通用 JSON 解析器。 |
| S-07 | P1 | 主协调者另发现：生产者只用大小写敏感匹配抑制 reserved marker，而 NewAPI 消费端采用 TrimSpace + EqualFold。 | 所有三个 serializer 和 native assignment 按同样的归一化规则抑制无证明的 marker 变体，补混合大小写 fixture。 |
| S-08 | P1 | 新 raw SSE 剥离器若仅用 gjson 首值，会漏掉重复 code 的末值 reserved marker。 | 非唯一 JSON 帧用标准库 UseNumber 规范化后剥离；规范化须在用量和输出计数之前；补真实 native/OAuth 流中已有图片加重复错误码 fixture。 |
| S-09 | P1 | HTTP200 type/code 对象经 `.String()` 后包含 content_policy 时，新的 policy fallback 可错误产出明确政策失败并触发下游退款。 | 原始字段非 JSON string 时先清空，重新计算专用分类；补对象包含 policy 的 fixture，确保不产出明确 policy code。 |
| S-10 | P1 | gjson v1.18.0 的 validstring 不验证原始 UTF-8；encoding/json Token 也可替换非法字节，因此“完整合法 JSON”证明可能接受含原始0xff的字符串。 | proof 首门禁增加 `utf8.Valid(body)` 并加入非法 UTF-8 envelope fixture；净化器的安全摘要不能代替来源编码证明。 |

最终复核处置：

- S-01：`unicode.IsSpace` 先转为空格，其他 control/Cf 再删除；换行 Bearer fixture 已加入。
- S-02/S-03：`safeOpenAIImagesFailureIdentifier` 要求格式合法且净化后字节不变；native message、metadata 和同步非failover路径均传入活动 token/base/proxy，标准 URL Hostname 与常见凭据表达已覆盖。凭据不进入 failover 对象。nonfailover 的 raw-body detail/system 出口在该 native 分支停用；OAuth nil 参数保留原有行为。
- S-04：通用证明直接复用 content-policy 别名检查否决。policy 的固定摘要与专用 code 不被通用 marker 覆盖。
- S-05：native HTTP200 只从实际 string message 生成诊断，其余使用固定摘要；净化器对整段 JSON/HTML、非法 UTF-8、超64KiB 输入回退安全摘要，保留普通 `{width}`/`< 2048` 自然语言。
- S-06：采用标准库 JSON Token visitor 检查整个证明输入的重复键，不改变通用业务解析器。此处选择完整唯一性检查，避免只检查根 data 而遗漏 error 内 type/message/code 或 response.output 歧义。重复键只否决来源证明。
- S-07：三个 serializer 共享 `clientErrorCode`，TrimSpace + EqualFold 后只有证明成立且语义状态502才返回规范小写 marker；native fallback 同样抑制大小写变体。三个 writer 的表测均已覆盖，stream builder 的断言路径已纠正为 `error.code`。
- S-08：SSE先用 UseNumber 规范化非唯一对象，再删除 error.code/response.error.code/code 的 reserved 值，之后统计用量与真实图片。raw frame 在写出前替换为净化后的 payload，不向已开始的流追加第二个普通 JSON。已有图片仍保留、计数和账务结果仍依据实际输出。
- S-09：原 type/code 非 string 时清空，并重新执行结构化错误分类；policy 不通过对象序列化或 message 自由文本产生。
- S-10：已加入 `utf8.Valid(body)` 首门禁和非法 UTF-8 envelope fixture；随后定向 Provider 复核通过。完整 service 包在该一行之前执行，不能代表逐字最终源码。

主协调者新增的诊断行为亦已复核：`ImageFailureMessage` 安全且非空时，即便终态证明为false，默认最终出口仍保留该 message。marker仍受独立proof与502门禁控制；已有专用语义及透传自定义文案优先。`UnknownOutcomeRetainsSafeDiagnosticWithoutMarker` 覆盖此增量。

S-06 最小输入：

```json
{"error":{"type":"upstream_error","message":"failed"},"data":[],"data":[{"b64_json":"AA=="}]}
```

此类输入仍是语法有效 JSON，不能用 `gjson.ValidBytes` 代替无歧义的终态证明。若无法确定唯一结构，保留未知结果，不输出可消费的 `image_provider_error`。

## 资金与结果路径

- 原生完整错误证明不增加 POST 次数；明确未发送失败沿用既有安全换号，已派发但未知的失败不因本次标志获得新重试许可。
- OAuth direct/batch/SSE 不提供本协议的明确失败证明，实际派发后设置粘滞未知标志；结果对象仍沿用已有部分成功计费与恢复逻辑。
- 已有图片输出优先；provider 错误不能覆盖已交付结果并让下游错误退款。
- `image_provider_error` 仅在最终语义状态 502 输出，JSON keepalive 已提交 wire 200 仍保留既有合同；operator 显式映射到其他状态时抑制 marker。
- Sub2API 的实际 provider status 与 NewAPI 接收到的 wire status 是两层不同证据；本次不扩宽 NewAPI 的既有消费状态白名单。

## 验证边界

已逐文件核对 10 个 Go 文件：`frozen-source.json` 加上末轮 S-10 两个文件的 `final-image-source-sha256.txt` 构成最终冻结清单，当前全部 SHA-256 匹配。此次审查只更新本文，未改动 Go 或 NewAPI 文件。

最终源码包门禁：`go test -tags=unit ./internal/handler -count=1` PASS 64.011s；`go test -tags=unit ./internal/service -count=1` FAIL 268.543s，唯一失败是未改动的 `TestInflightEstimate_AccountMappingNoDBAndBoundedMemory`（HeapAlloc 8483248 >= 8388608）。隔离命令 PASS 0.178s，billing 测试/被测文件与 HEAD hash 完全一致且不经过图片路径；完整输出保存在 `docs/kkai/image-failure-validation-20261116/service-unit.log`，隔离输出在 `inflight-isolated.log`。因此 service 完整包不能称通过，也不能把隔离结果升级为基线原因结论。

最后增量验证：`go test -tags=unit ./internal/service -run 'TestForwardOpenAIImagesAPIKey_FailureContract|TestOpenAIImagesProvider' -count=1` PASS 0.117s；`git diff --check` PASS。此轮覆盖非法 UTF-8、重复键、大小写 reserved code、三种 serializer、SSE 已有图片和安全 message。handler 在无源码变更的最后轮完整包 PASS 64.011s。全仓套件、真实账号 E2E、构建/发布不属于本审查，均未宣称通过。

首次无tag service全包出现未修改 allocation测试失败，隔离重跑通过，但没有证明完整失败原因；不能将其写成“已确认历史基线问题”。该证据和缺口已在开发文档保留。

全仓套件未由审查者执行，未对生产账号执行真实生成请求，未用本地 fixture 冒充生产业务验收。审查不替代正式构建、候选健康、灰度、错误窗口和回滚门禁。
