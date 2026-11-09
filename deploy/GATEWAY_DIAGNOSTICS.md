网关轻量诊断

**会话身份状态**

已有 `cyber_policy` 和 `cyber_policy_session_blocked` Ops 错误事件的消息增加 `session_identity_status`。该字段描述封锁键的身份条件，不改变请求准入、封锁键、会话来源或封锁策略，也不新增原始会话 ID 日志。

| 值 | 含义 |
| --- | --- |
| `ready` | 已验证中转身份与有效 `X-Session-Id` 足以构造封锁键 |
| `missing_api_key` | 缺少有效的入口 API Key ID |
| `invalid_request` | 请求上下文不存在 |
| `unverified_relay` | 缺少已验证的中转 origin/user/token |
| `missing_session` | 权威 `X-Session-Id` 缺失或为空白 |
| `invalid_session` | 权威会话 ID 未通过既有字符或长度校验 |

`ready` 不代表封锁开关已开启、用户不在白名单、Redis 可用或写入成功。其他 session 头与正文提示不会改变权威身份来源。状态随事件一起保存，不为每个普通请求新增日志。

**按需网络耗时**

默认关闭。在服务配置中设置并重启：

```yaml
gateway:
  openai_network_trace_enabled: true
```

也可向服务进程传入 `GATEWAY_OPENAI_NETWORK_TRACE_ENABLED=true`。排障结束后恢复为 `false` 并重启。这里的配置开关不会自动更改已部署实例。

启用后，OpenAI 网关每次原生 HTTP 出站调用产生一条 INFO 事件 `openai.network_trace`，仅进入标准日志，不进入 Ops 系统日志数据库。通过已有 `request_id`、`client_request_id`、`account_id` 关联；存在请求派发预算时附带 `dispatch_attempt`。关闭时不安装跟踪回调。

| 字段 | 含义 |
| --- | --- |
| `attempt_ms` | 该次 HTTP 出站调用直到传输适配器返回响应或错误的耗时，可能包含内部解压预读或协议适配处理，不包含调用方之后的流读取 |
| `status_code` | 传输适配器返回响应时的 HTTP 状态 |
| `dns_ms`、`tcp_ms`、`tls_ms` | 已完整观测到的对应阶段耗时 |
| `connection_acquire_ms` | 获取连接耗时，包含新建连接时间，不能当作纯连接池排队时间 |
| `connection_requests`、`connections_reused` | 观察到的连接获取请求及复用次数 |
| `write_to_first_byte_ms` | 请求写完到响应首字节的时间；只在单次、顺序明确的交换中记录 |
| `result` | 固定结果类别：`response`、`http_error`、`transport_error`、`timeout`、`canceled`、`no_response` |

连接复用时可能没有 DNS/TCP/TLS 字段；没有完成的阶段也省略，不填假零值。并发拨号的阶段耗时取活动区间的并集，多次交换累计；阶段之间可能重叠，不能相加作为总耗时。缺失阶段不证明该阶段耗时为零。

覆盖的是经过该网关原生 HTTP 出站入口的调用，不包含插件传输、原生 WebSocket 或其他网关独立入口。首字节可能只是响应头，不能代替模型 TTFT。事件不新增记录主机、IP、URL、代理、请求头、正文或原始错误文本，也不增加重试、改变路由或收紧身份策略。
