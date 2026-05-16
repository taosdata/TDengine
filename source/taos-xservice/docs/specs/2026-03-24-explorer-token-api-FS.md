# Explorer Token API 概要设计说明书（Functional Spec）

# 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-24 | 2026-03-24 | 1.0 | Copilot | 初始版本 |

# 背景

Explorer 的 Web UI 登录流程使用 XOR 时间加密传输密码，这对浏览器客户端是合适的，但对于第三方集成、自动化脚本、CLI 工具等程序化调用场景，需要额外实现 XOR 加密逻辑才能获取令牌，使用门槛较高。

本特性在现有 `/api/-/login` 接口上扩展，支持明文密码认证，并为 Explorer 令牌添加 `xt-` 前缀使其具备自描述性。

# 定义

| 术语 | 定义 |
| --- | --- |
| `xt-` 前缀 | Explorer Token 的固定前缀，格式为 `xt-{uuid-v4}` |
| 明文密码模式 | 请求体中使用 `password` 字段直接传入密码 |
| 加密密码模式 | 请求体中使用 `encrypted_password` 字段传入 XOR 加密后的密码（Web UI 现有行为） |

# 行为说明

## API 接口变更

### `POST /api/-/login`

**请求体**（JSON）：

| 字段 | 类型 | 必填 | 说明 |
| --- | --- | --- | --- |
| `username` | string | 是 | TDengine 用户名 |
| `password` | string | 否 | 明文密码（与 `encrypted_password` 二选一） |
| `encrypted_password` | string | 否 | XOR 加密密码（与 `password` 二选一） |
| `captcha` | string | 否 | 验证码（仅在启用验证码时需要） |

**优先级规则**：当 `password` 和 `encrypted_password` 同时存在时，优先使用 `password`。两者均不存在时返回错误。

**响应格式**：

#### 明文密码模式（简化响应）

```json
{
    "code": 0,
    "token": "xt-4ebd32d8-eb56-443f-a021-e181345421e2",
    "server_version": "3.4.0.13.enterprise"
}
```

#### 加密密码模式（保持现有格式）

```json
{
    "code": 0,
    "column_meta": [["server_version()", "BINARY", 19]],
    "data": [["3.4.0.13.enterprise"]],
    "rows": 1,
    "registered_user": "...",
    "token": "xt-4ebd32d8-eb56-443f-a021-e181345421e2"
}
```

#### 认证失败

```json
{
    "code": 65535,
    "desc": "Authentication failed"
}
```

HTTP 状态码：401 Unauthorized

#### 缺少密码字段

```json
{
    "code": 65535,
    "desc": "Either password or encrypted_password is required"
}
```

HTTP 状态码：401 Unauthorized

### 示例调用

```bash
# 获取 Token
curl -X POST http://localhost:6060/api/-/login \
  -H "Content-Type: application/json" \
  -d '{"username": "root", "password": "taosdata"}'

# 使用 Token 执行 SQL
curl -X POST http://localhost:6060/api/-/rest/sql \
  -H "Authorization: Bearer xt-4ebd32d8-eb56-443f-a021-e181345421e2" \
  -d "SELECT now()"
```

### 错误码列表

| HTTP 状态码 | code | desc | 说明 |
| --- | --- | --- | --- |
| 200 | 0 | - | 认证成功 |
| 401 | 65535 | Authentication failed / Invalid password | 用户名或密码错误 |
| 401 | 65535 | captchaRequired | 需要验证码但未提供 |
| 401 | 65535 | captchaInputError | 验证码错误 |
| 401 | 65535 | Either password or encrypted_password is required | 未提供任何密码字段 |
| 500 | 65535 | (TDengine 内部错误) | 服务端错误 |

## Token 格式变更

所有新生成的 Explorer 会话令牌格式从：

```
550e8400-e29b-41d4-a716-446655440000
```

变更为：

```
xt-550e8400-e29b-41d4-a716-446655440000
```

Token 通过以下方式传递：
1. **Cookie**：`session_id=xt-...`（HttpOnly, SameSite=Lax, 1 小时有效）
2. **Bearer Token**：`Authorization: Bearer xt-...`
3. **JSON 响应体**：`"token": "xt-..."`

## Cookie 行为

无论使用哪种密码模式，成功登录后均会设置 `session_id` HttpOnly cookie。

# 性能

无性能影响。变更仅涉及字符串前缀拼接和条件分支，无额外 I/O 或计算开销。

# 安全

1. 明文密码通过 HTTPS 传输，不在日志中记录。
2. Token 具有 `xt-` 前缀，可被安全扫描工具识别为 Explorer 专用令牌。
3. 认证失败时不泄露用户是否存在的信息。
4. 令牌有效期 1 小时（3600 秒）。

# 兼容性

- `encrypted_password` 字段从必填变为可选（`Option<String>`），但 Web UI 始终发送该字段，因此 Web UI 行为完全不变。
- 旧版不含 `xt-` 前缀的 Token 在其有效期内仍可正常使用。内部 UUID 解析函数会自动剥离前缀。
- 加密密码模式的响应格式新增了 `token` 字段，现有客户端忽略未知字段即可。

# 运维

无。

# 使用场景

## UC-1：第三方系统集成

外部系统（如 Grafana、自定义监控平台）通过 `/api/-/login` 使用明文密码获取 Token，然后使用 Bearer Token 调用 Explorer REST API 执行 SQL 查询。

## UC-2：自动化脚本

运维脚本使用 `curl` 调用登录接口获取 Token，后续通过 Token 执行批量数据操作。

## UC-3：CLI 工具

命令行工具使用用户名密码登录，获取 Token 后缓存，在有效期内复用。

## UC-4：Web UI 正常登录

浏览器用户通过 Web UI 登录，使用 `encrypted_password` 模式，行为与变更前完全一致。

# 约束和限制

约束：
- 生产环境必须启用 HTTPS，否则明文密码在网络传输中不安全。

限制：
- Token 有效期固定为 1 小时，暂不支持自定义。
- 不支持 Token 刷新，过期后需重新登录。

# 常见错误和排查

| 错误现象 | 可能原因 | 排查方法 |
| --- | --- | --- |
| 401 "Authentication failed" | 用户名或密码错误 | 检查 TDengine 用户凭据 |
| 401 "Either password or encrypted_password is required" | 请求体中未包含任何密码字段 | 检查请求 JSON 格式 |
| 500 Internal Server Error | TDengine 服务不可用 | 检查 taosd 进程状态 |
| Token 被拒绝 | Token 已过期（1 小时） | 重新调用登录接口获取新 Token |

# 可观测性

- taos Explorer Web UI：登录行为不变。响应中新增 `token` 字段，但 UI 代码通过 cookie 管理会话，无需额外适配。
- taos shell：不受影响。
- TDinsight：不受影响。

# 安装和卸载

无额外要求。随 taos-explorer 二进制更新即可生效。

# 文档

- 需要更新企业版文档中 Explorer REST API 章节，补充明文密码登录方式和 Token 前缀说明。
- 需要更新官网文档中 Explorer 接口参考。

# 参考文档

- Explorer OAuth Session 模块：`explorer/server/src/oauth/session.rs`
- Explorer Login Handler：`explorer/server/src/main.rs`

# 附录

无。
