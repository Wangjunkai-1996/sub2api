# KKAI 请求恢复流式修正：Sub2 开发规格

日期：2026-11-05。状态：两端本地代码已按本修正落地并做定向验证；本地受管开关对照已验证首文本早于上游终态，真实客户端和线上验收待发布阶段执行。CPU 和生产性能尚未量化。

本文件落实已确认的 NewAPI→Sub2 行为：正常内容实时交付，文本中途可恢复错误按预算重试，实际工具事件公开后或显式 JSON 输出后不再重生成。它覆盖旧版 `KKAI_REQUEST_RECOVERY_DEVELOPMENT.md` 中要求 JSON/工具输出等待成功提交、首轮固定 600 秒及 NewAPI 固定 930 秒取消的要求；旧版关于签名握手、账号调度、11 次真实发送、续聊别名、计量和取消的其余约束继续有效。NewAPI 必须同步按最终出站请求体排除显式 JSON，不能在两端认定不同恢复 owner 时启用渠道。

## 实施点

1. `backend/internal/handler/openai_gateway_handler.go`、`openai_chat_completions.go` 在安装受管 writer/context 之前，使用 `openai_managed_recovery.go` 的请求资格判断。显式 `json_object`/`json_schema` 走普通路径；已收到受管协议头但能力不匹配时明确返回 capability rejection，供 NewAPI 在响应未提交时安全回放。请求仅声明 `tools`/`tool_choice:auto` 不退出受管路径，实际只输出文本时仍能恢复。
2. `backend/internal/service/openai_recovery_stream_writer.go` 保留文本逐帧交付。Chat 的 `delta.tool_calls` 和 Responses 的可执行工具项目，首事件发送前标记“此逻辑请求不可再重试”，然后按标准顺序实时发送增量。当前会直接拒绝的旧式 Chat `delta.function_call` 也须原样实时转发并触发同一边界，不能因声明了 `functions` 就把整轮文本恢复关闭。移除这些工具的 `heldChatFrames`/`heldItems` 成功后释放路径；`CommitAttempt` 不得重复发送。Responses 仍须保持 item ID、output index、最终 output 和流中事件一致。不要顺带把 reasoning、压缩或其他非工具项目改成无条件直通。
3. Responses/Chat handler 在 `AbandonAttempt` 和任何新 dispatch 前检查上述边界。工具已公开后发生错误，只按原控制协议发送一次 `terminal(failed)` 和公共失败终态，不发重生成 marker、不切号、不拼接后续工具。必要时调整底层流读取错误返回，让 handler 统一结束，而非底层自行续接。
4. `openAIManagedAttemptContext` 不再因预留恢复时间而在 600 秒截断仍正常进行的首轮。当前受管 Chat 已在 `beginOpenAIRequestBudget(..., true)` 装入 900 秒硬截止；改为仅限制真正失败后的下一次派发，不中断健康首轮。保留客户端取消、现有首个有效输出/空闲超时、Responses 原有 900 秒请求预算和实际失败后的有界恢复；普通 Chat 的现有期限不变。NewAPI 侧需同步移除受管 930 秒整流硬截止，且不得退回更短的通用超时。
5. Responses 续聊别名仍在可用 logical ID 完成前发布。发布失败必须明确结束为失败，不把无效 ID 报成功，也不为已完成的生成再派发一次。后续显式 JSON 请求虽走普通代理，仍需正确解析既有受管 ID 并遵守账号/出口绑定及历史约束。

## 验收

- 用可控上游和同步屏障验证 Chat/Responses 正常文本、显式 JSON 和首个工具增量均早于上游终态到达客户端；通过检查实际 deadline 配置或可控时钟验证正常长流不受恢复专用 600 秒截断，不真实等待十分钟。
- 文本在首字前与中途遇到 429/暂时性 5xx 后，确实执行同号重试或切号；带自动工具声明但只输出文本也能恢复，终态唯一。
- Chat 和 Responses 各验证一次“工具首帧实时到达，随后失败”；失败后没有第二次 dispatch、重生成提示或重复工具帧，工具结果不被标成成功。
- Chat/Responses 显式 JSON 不安装受管 writer，成功流不等整段结束，失败不拼 JSON；非流式提交前安全重试保留。
- 核对别名成功续聊、发布失败终态、用量与 NewAPI 的 v1 终态顺序。复用 `openai_recovery_stream_writer_test.go` 和受管 handler 的现有夹具；只增加能证明上述行为的定向测试，不扩展罕见格式矩阵。

本地对照仅为可控上游的时序与分配样本，不能外推生产性能。真实客户端验收完成前，不得据此启用生产渠道开关。本文件记录开发工作，提交、构建或部署需独立的发布指令。
