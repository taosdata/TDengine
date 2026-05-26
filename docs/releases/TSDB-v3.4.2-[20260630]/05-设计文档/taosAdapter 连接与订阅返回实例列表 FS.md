# taosAdapter 连接与订阅返回实例列表 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-05-15 | 2026-06-30 | 0.1 | Sheyanjie | 初稿，新增 WebSocket 连接与 TMQ 订阅可选返回 adapter 实例列表的功能说明 |
| 2026-05-15 | 2026-05-20 | 1.0 | Sheyanjie | 明确 `list_instances` 返回值为带端口号的实例访问地址 |

## 2. 背景

taosAdapter 进程会通过 TDengine 客户端注册自身实例信息，供外部组件进行服务发现、调度或可观测展示。此前 WebSocket SQL 连接和 WebSocket TMQ 订阅接口只完成连接或订阅建立，不在响应中返回当前可见的 adapter 实例列表。调用方如果需要在建立连接或订阅时同步感知集群中的 adapter 实例，需要额外调用其他能力或维护独立发现逻辑。

本次优化在不改变默认协议行为的前提下，为以下两个 WebSocket 入口增加可选的 `list_instances` 请求字段：

1. SQL WebSocket 连接：`/ws`，action 为 `conn`。
2. TMQ WebSocket 订阅：`/rest/tmq`，action 为 `subscribe`。

当请求显式设置 `list_instances: true` 时，taosAdapter 在成功建立连接或订阅后调用 TDengine 客户端实例查询能力，并在响应中返回 `list_instances` 字段。未设置或设置为 `false` 时响应保持旧行为，不返回该字段。

## 3. 定义

1. **adapter 实例**：通过 TDengine 客户端 `taos_register_instance` 注册的 taosAdapter 运行实例。
2. **实例列表**：通过 `taos_list_instances(filter_type)` 获取到的实例访问地址列表。本功能使用 adapter 注册类型作为过滤条件，只返回 taosAdapter 相关实例；列表中的每个元素必须包含端口号，格式为 `host:port`，例如 `localhost:6041`。
3. **`list_instances` 请求字段**：布尔字段，表示调用方是否希望在本次连接或订阅响应中返回 adapter 实例列表。
4. **`list_instances` 响应字段**：字符串数组字段，仅当请求字段为 `true` 且查询实例列表成功时返回。
5. **旧版本 adapter**：未实现本功能、请求结构中没有 `list_instances` 字段的 taosAdapter 版本。

## 4. 行为说明

### 4.1 SQL WebSocket 连接

SQL WebSocket 连接请求在 `args` 中新增可选字段 `list_instances`。

| 字段 | 类型 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `list_instances` | bool | 否 | `false` | 是否在连接成功响应中返回 adapter 实例列表 |

请求示例：

```json
{
  "action": "conn",
  "args": {
    "req_id": 1,
    "user": "root",
    "password": "taosdata",
    "list_instances": true
  }
}
```

成功响应示例：

```json
{
  "code": 0,
  "message": "",
  "action": "conn",
  "req_id": 1,
  "timing": 3,
  "version": "3.4.2.0",
  "list_instances": [
    "localhost:6041",
    "localhost:6042"
  ]
}
```

默认行为示例：

```json
{
  "action": "conn",
  "req_id": 1,
  "timing": 2,
  "version": "3.4.2.0"
}
```

行为约束：

1. `list_instances` 未传或为 `false` 时，不调用实例列表查询能力，响应中不包含 `list_instances`。
2. `list_instances` 为 `true` 时，连接认证、白名单校验、连接选项设置全部成功后，再查询实例列表。
3. 实例列表查询成功但结果为空时，响应包含空数组：`"list_instances": []`。
4. 实例列表查询失败时，本次连接失败，响应使用现有错误响应结构返回错误码和错误信息，并关闭已创建的 TDengine 连接。

### 4.2 TMQ WebSocket 订阅

TMQ 订阅请求在 `args` 中新增可选字段 `list_instances`。

| 字段 | 类型 | 是否必填 | 默认值 | 说明 |
| --- | --- | --- | --- | --- |
| `list_instances` | bool | 否 | `false` | 是否在订阅成功响应中返回 adapter 实例列表 |

请求示例：

```json
{
  "action": "subscribe",
  "args": {
    "req_id": 1,
    "user": "root",
    "password": "taosdata",
    "group_id": "g1",
    "topics": [
      "topic_meters"
    ],
    "auto_commit": "true",
    "offset_reset": "earliest",
    "snapshot_enable": "true",
    "list_instances": true,
    "config": {}
  }
}
```

成功响应示例：

```json
{
  "code": 0,
  "message": "",
  "action": "subscribe",
  "req_id": 1,
  "timing": 5,
  "version": "3.4.2.0",
  "list_instances": [
    "localhost:6041",
    "localhost:6042"
  ]
}
```

默认行为示例：

```json
{
  "action": "subscribe",
  "req_id": 1,
  "timing": 4,
  "version": "3.4.2.0"
}
```

行为约束：

1. `list_instances` 未传或为 `false` 时，不调用实例列表查询能力，响应中不包含 `list_instances`。
2. `list_instances` 为 `true` 时，在订阅流程持有 TMQ handler 锁后先查询实例列表；查询失败则订阅失败，不创建新的 consumer。
3. 首次订阅和已 `unsubscribe` 后的重新订阅均支持返回 `list_instances`。
4. 当前 handler 已存在未取消订阅的 consumer 时，仍按既有逻辑返回 `tmq should have unsubscribed first`，不因本字段改变。

### 4.3 实例列表查询规则

taosAdapter 使用 TDengine 客户端封装函数查询实例列表：

```text
taos_list_instances(filter_type)
```

`filter_type` 使用当前版本配置中的 adapter 注册类型，等价于 adapter 注册实例时使用的类型。该规则保证返回结果只包含 adapter 相关实例，不混入其他类型的注册实例。

返回列表中的每个元素表示一个可访问的 adapter 实例地址，必须携带端口号：

```text
<host>:<port>
```

其中 `host` 可以是主机名、IP 地址或 `localhost`，`port` 是该 adapter 对外提供服务的端口，例如 `localhost:6041`。客户端不得假设返回值是不带端口的裸实例标识，也不得在缺少端口号时自行补默认端口。

### 4.4 错误码和错误处理

| 场景 | 接口 | 返回行为 |
| --- | --- | --- |
| 请求 JSON 中包含未知字段 `list_instances`，服务端为旧版本 adapter | SQL 连接 / TMQ 订阅 | 旧版本 Go JSON 反序列化忽略未知字段，不报错 |
| 请求未设置 `list_instances` | SQL 连接 / TMQ 订阅 | 保持旧响应结构，不返回 `list_instances` |
| 实例列表查询成功且为空 | SQL 连接 / TMQ 订阅 | 响应返回 `"list_instances": []` |
| 实例列表查询失败 | SQL 连接 | 使用现有连接错误响应返回错误码；非 TDengine 标准错误时为 `0xffff`；连接关闭 |
| 实例列表查询失败 | TMQ 订阅 | 使用现有 TMQ 错误响应返回 `0xffff` 和错误信息；订阅不创建 consumer |

## 5. 性能

1. 默认路径无性能影响：未请求 `list_instances` 时不调用 `taos_list_instances`，响应结构与旧版本一致。
2. 请求 `list_instances: true` 时，每次连接或订阅成功路径额外进行一次本地客户端实例列表查询。该查询只返回实例访问地址列表，数据量与 adapter 实例数量相关，通常较小。
3. SQL WebSocket 连接路径中，实例列表查询发生在连接初始化阶段，不影响后续查询执行性能。
4. TMQ 订阅路径中，实例列表查询发生在订阅初始化阶段，不影响后续 poll、fetch、commit 等消费路径。

## 6. 安全

1. 本功能不新增认证方式，不绕过现有用户密码、TOTP、Bearer Token、白名单和连接选项校验。
2. `list_instances` 返回的是 adapter 注册实例访问地址（`host:port`），不包含用户数据、SQL 内容、topic 数据或凭据信息。
3. 响应是否返回实例列表由调用方显式请求控制。默认不返回，避免旧调用方在日志或中间层中意外暴露新增字段。
4. 请求日志中的密码、TOTP code、Bearer Token 仍按既有逻辑隐藏；新增 `list_instances` 仅作为布尔值记录。

## 7. 兼容性

1. **升级兼容**：`list_instances` 默认值为 `false`，未设置时响应不包含该字段，旧客户端行为不变。
2. **字段命名兼容**：响应字段名固定为 `list_instances`，不使用历史或内部别名 `instances`。
3. **旧服务端兼容**：新客户端向旧版本 adapter 发送 `list_instances` 字段时，旧版本 Go JSON 反序列化默认忽略未知字段，不报错；旧版本响应不包含 `list_instances`。
4. **新客户端兼容旧响应**：新客户端应将缺失的 `list_instances` 字段视为服务端未返回实例列表，不应作为协议错误。
5. **降级兼容**：降级到旧版本 adapter 后，请求中的 `list_instances` 被忽略，响应中不返回该字段；核心连接和订阅能力不受影响。

## 8. 运维

1. 无新增配置项，无需修改 `taos.cfg`。
2. 无新增系统表、SQL 命令或管理命令。
3. 运维或调度组件如需在连接或订阅建立时获取 adapter 实例列表，可在请求中设置 `list_instances: true`。
4. 若响应中没有 `list_instances` 字段，需要按以下顺序判断：
   1. 请求是否未设置 `list_instances: true`。
   2. 服务端是否为旧版本 adapter。
   3. 中间代理或客户端解析逻辑是否丢弃了未知字段。

## 9. 使用场景

1. **客户端服务发现**：客户端在建立 WebSocket SQL 连接后立即获取当前 adapter 实例列表，用于后续连接重试或负载均衡策略。
2. **TMQ 消费者初始化**：消费端在订阅成功时获取 adapter 实例列表，用于记录当前消费连接所在的 adapter 集群视图。
3. **可观测性采集**：监控组件在轻量连接或订阅检查时同步采集 adapter 实例访问地址，辅助排查实例注册、注销或发现异常。
4. **灰度兼容验证**：混部新旧 adapter 版本时，调用方可以通过响应是否包含 `list_instances` 判断当前连接命中的 adapter 是否支持该扩展能力。

## 10. 约束和限制

1. `list_instances` 只在连接或订阅响应中返回，不提供持续推送；实例列表在响应之后可能发生变化。
2. 本功能返回的是 TDengine 客户端实例注册表中的 adapter 实例访问地址，必须包含端口号；不保证包含负载、健康状态或其他元数据。
3. 如果 adapter 实例未正确注册，或者注册已过期，则不会出现在返回列表中。
4. TMQ `subscribe` 在未取消订阅的 consumer 存在时仍不允许重复订阅；该限制不因 `list_instances` 改变。

## 11. 常见错误和排查

| 现象 | 可能原因 | 排查建议 |
| --- | --- | --- |
| 响应中没有 `list_instances` | 请求未设置 `list_instances: true` | 检查 WebSocket 请求 `args` |
| 响应中没有 `list_instances` | 命中旧版本 adapter | 查看响应 `version`，或确认部署版本 |
| 返回 `list_instances: []` | 当前过滤条件下未发现 adapter 注册实例 | 检查 adapter 实例注册逻辑和注册有效期 |
| 连接或订阅返回 `0xffff` | `taos_list_instances` 调用失败或内部错误 | 查看 taosAdapter 日志中的 `list instances error` 或 `taos_list_instances` 调用日志 |
| TMQ 重复订阅失败 | 当前连接已有未取消订阅的 consumer | 先执行 `unsubscribe`，再重新 `subscribe` |

## 12. 可观测性

1. taosAdapter debug 日志会记录 `taos_list_instances` 的调用、返回实例数量、错误信息和耗时。
2. SQL WebSocket 连接日志可看到连接请求中的 `list_instances` 布尔值。
3. TMQ 订阅日志可看到 subscribe 请求中的 `list_instances` 布尔值。
4. taos shell、taos Explorer、TDinsight 不需要修改；是否使用该字段由各客户端或上层组件自行决定。

## 13. 安装和卸载

无。该能力随 taosAdapter 二进制发布，不涉及额外安装、卸载脚本或数据迁移。

## 14. 文档

1. 需要在 taosAdapter WebSocket SQL 连接协议文档中补充 `conn` 请求参数 `list_instances` 和响应字段说明。
2. 需要在 taosAdapter WebSocket TMQ 协议文档中补充 `subscribe` 请求参数 `list_instances` 和响应字段说明。
3. 文档需明确：默认不返回该字段；旧版本 adapter 会忽略请求字段且不返回响应字段。

## 15. 参考文档

1. `controller/ws/ws/conn.go`
2. `controller/ws/tmq/tmq.go`
3. `db/syncinterface/wrapper.go`
4. `controller/ws/ws/conn_test.go`
5. `controller/ws/tmq/tmq_test.go`
6. 提交：`1047a57396f06d022912e77a0aa1cf2b69b0cb55`

## 16. 附录
