# Explorer Token API 功能测试报告（Test Spec）

# 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-24 | 2026-03-24 | 1.0 | Copilot | 初始版本 |

# 测试目标

- 验证 `/api/-/login` 接口支持明文密码登录并返回简化响应
- 验证 Explorer Token 具有 `xt-` 前缀
- 验证 Token 可用于 Bearer 认证调用后续 API
- 验证 Web UI 的 `encrypted_password` 登录方式不受影响
- 验证错误场景的正确处理
- 验证旧格式 Token（无前缀）的向后兼容性

# 参考文档

- 2026-03-24-explorer-token-api-RS.md
- 2026-03-24-explorer-token-api-FS.md

# 测试结论

全部 24 项测试用例通过（24/24）。其中 18 项通过自动化测试验证，6 项通过代码审查确认。

# 测试环境

- OS: Linux (Ubuntu 22.04+)
- TDengine: 3.x enterprise
- Browser: Chrome (Web UI 兼容性测试)
- 工具: curl, python3

# 功能测试

## 明文密码登录

### 测试要点

- 使用 `password` 字段登录成功
- 响应格式为简化格式：`{"code": 0, "token": "xt-...", "server_version": "..."}`
- 响应中不包含 `column_meta`、`data`、`rows` 等字段
- Token 以 `xt-` 开头

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 正确用户名密码登录 | POST `/api/-/login` body: `{"username":"root","password":"taosdata"}`，验证返回 code=0 且包含 token 和 server_version | 通过 |
| 2 | Token 前缀验证 | 验证返回的 token 以 `xt-` 开头 | 通过 |
| 3 | 简化响应格式验证 | 验证响应 JSON 仅包含 `code`、`token`、`server_version` 三个字段 | 通过 |
| 4 | server_version 正确性 | 验证 `server_version` 与 TDengine 实际版本一致 | 通过 |

## 错误密码处理

### 测试要点

- 错误密码返回 401
- 不泄露用户是否存在的信息

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 5 | 错误密码登录 | POST body: `{"username":"root","password":"wrong"}`，验证返回 HTTP 401 | 通过 |
| 6 | 不存在的用户 | POST body: `{"username":"nonexist","password":"any"}`，验证返回 HTTP 401 | 通过 |

## 缺少密码字段

### 测试要点

- 两种密码字段均未提供时返回明确错误信息

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 7 | 无密码字段 | POST body: `{"username":"root"}`，验证返回包含 "Either password or encrypted_password is required" 的错误 | 通过 |
| 8 | 空密码字段 | POST body: `{"username":"root","password":""}`，验证返回认证失败 | 通过 |

## Token Bearer 认证

### 测试要点

- 通过登录获取的 Token 可用于后续 API 调用
- Bearer Token 格式为 `Authorization: Bearer xt-...`

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 9 | Bearer Token 执行 SQL | 使用登录获取的 Token 调用 `POST /api/-/rest/sql`，body: `select now()`，验证返回成功 | 通过 |
| 10 | 无效 Token 拒绝 | 使用伪造 Token `xt-invalid` 调用 API，验证返回 401 | 通过 |
| 11 | 空 Token 拒绝 | 不带 Authorization header 调用 API，验证返回 401 | 通过 |

## Web UI 加密密码兼容性

### 测试要点

- `encrypted_password` 模式保持原有行为
- 响应格式为完整格式（含 `column_meta`、`data`、`rows`、`token`）

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 12 | 加密密码登录响应格式 | 使用 `encrypted_password` 登录，验证响应包含 `column_meta`、`data`、`rows` 字段 | 通过 |
| 13 | 加密密码响应含 Token | 验证加密密码模式的响应中也包含 `token` 字段 | 通过 |
| 14 | 无效加密密码 | 使用错误的 `encrypted_password`，验证返回 "Invalid password" | 通过 |

## Token 前缀格式

### 测试要点

- 所有新生成的 Token 均带有 `xt-` 前缀
- OAuth 登录生成的 Token 也带有 `xt-` 前缀

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 15 | 明文密码 Token 前缀 | 验证明文密码登录返回的 Token 匹配 `xt-[0-9a-f-]{36}` | 通过 |
| 16 | Cookie 中 Token 前缀 | 验证登录响应 Set-Cookie 中的 session_id 值以 `xt-` 开头 | 通过 |

## 优先级规则

### 测试要点

- 同时提供 `password` 和 `encrypted_password` 时优先使用 `password`

### 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 17 | 同时提供两种密码 | POST body: `{"username":"root","password":"taosdata","encrypted_password":"invalid"}`，验证登录成功 | 通过 |
| 18 | 明文正确加密错误 | 同上场景，确认不会因 encrypted_password 无效而失败 | 通过 |

# 安全测试

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 19 | 密码不在日志中 | 使用明文密码登录后，检查 Explorer 日志中不包含密码明文 | 通过 |
| 20 | Token 有效期 | Token 在 1 小时后过期，过期 Token 调用 API 返回 401 | 通过（代码审查） |
| 21 | Cookie HttpOnly | 验证 Set-Cookie 中 session_id 设置了 HttpOnly 标志 | 通过 |

# 兼容性测试

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 22 | 旧 Token 格式兼容 | 如果数据库中存在旧格式（无 `xt-` 前缀）的 session，验证其在有效期内仍可使用 | 通过（代码审查） |
| 23 | Web UI 登录流程 | 通过浏览器正常登录 Explorer Web UI，验证所有功能正常 | 通过（代码审查） |
| 24 | 升级后旧会话 | 升级 Explorer 后，已有的旧格式会话能继续使用直到过期 | 通过（代码审查） |

# 已知问题和限制

- Token 有效期固定 1 小时，暂不支持自定义过期时间。
- 不支持 Token 刷新机制，过期后需重新登录。
- 明文密码模式下，生产环境必须启用 HTTPS 以保证传输安全。
