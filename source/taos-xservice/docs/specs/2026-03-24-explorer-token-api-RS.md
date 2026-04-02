# Explorer Token API 需求规格说明书（Requirement Spec）

# 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-24 | 2026-03-24 | 1.0 | Copilot | 初始版本 |

# 引言

## 术语与缩写名词

| 术语 | 定义 |
| --- | --- |
| Explorer | taos-explorer，TDengine 的 Web 管理界面服务 |
| Token | Explorer 会话令牌，用于 API 认证 |
| Session ID | Explorer 内部的会话标识符，即 Token 的实际值 |
| XOR 加密 | Explorer 登录流程中使用的时间相关异或加密算法 |

## 相关文档资料

- TDengine Explorer 用户手册
- Explorer OAuth 会话管理模块设计文档

## 优先级要求

高优先级。此功能是面向外部集成和自动化场景的基础能力，需尽快交付。

## 版本要求

企业版与开源版均支持。随 taos-explorer 下一版本发布。

# 需求目标

当前 Explorer 的登录接口 (`POST /api/-/login`) 仅支持 Web UI 使用的 XOR 加密密码方式，不便于第三方系统、脚本或 CLI 工具通过编程方式获取访问令牌。

本需求旨在：

1. 为 `/api/-/login` 接口增加明文密码登录方式，使程序化调用方可直接使用 TDengine 用户名和密码获取 Token。
2. 为所有 Explorer 管理令牌添加 `xt-` 前缀，使令牌具备自描述性，可与其他类型的令牌区分。
3. 当使用明文密码登录时，返回简化的 JSON 响应，仅包含 `code`、`token` 和 `server_version`，方便程序化处理。

# 功能需求

| 序号 | 功能类别 | 功能名称 | 功能描述 |
| --- | --- | --- | --- |
| 1 | 认证 | 明文密码登录 | `/api/-/login` 接口新增可选 `password` 字段，支持直接传入明文密码进行认证 |
| 2 | 认证 | 简化响应格式 | 使用明文密码登录时，返回 `{"code":0,"token":"xt-...","server_version":"..."}` |
| 3 | 令牌管理 | Token 前缀 | 所有新生成的 Explorer 会话令牌添加 `xt-` 前缀 |
| 4 | 兼容性 | 旧令牌兼容 | 系统能够识别和使用不带 `xt-` 前缀的旧格式令牌 |
| 5 | 兼容性 | Web UI 兼容 | 使用 `encrypted_password` 的 Web UI 登录流程保持不变 |

# 性能需求

无额外性能要求。令牌生成基于现有会话管理机制，仅增加字符串前缀操作，性能影响可忽略。

# 安全需求

1. 明文密码仅在 HTTPS 传输中使用，生产环境必须启用 TLS。
2. 密码不在服务端日志中记录。
3. 令牌有效期默认 1 小时，通过 HttpOnly cookie 和 Bearer Token 两种方式提供。
4. 认证失败返回 401，不泄露具体错误信息（如用户是否存在）。

# 其他需求

## 兼容性需求

- `encrypted_password` 字段从必填改为可选，但 Web UI 行为不变。
- 旧版 Token（不含 `xt-` 前缀）在会话有效期内仍可使用。

## 接口需求

- 接口路径：`POST /api/-/login`
- Content-Type: `application/json`
- 请求体支持两种认证方式，二选一：
  - `{"username": "root", "password": "taosdata"}` — 明文密码
  - `{"username": "root", "encrypted_password": "..."}` — XOR 加密密码（Web UI 使用）

## 运维需求

无额外运维需求。

## 易用性需求

程序化调用场景下，用户可通过一次 HTTP POST 请求即可获取令牌，无需实现 XOR 加密逻辑。

## 测试需求

- 明文密码登录成功返回 Token
- 错误密码返回 401
- 不提供任何密码字段返回错误提示
- Token 可用于后续 Bearer 认证
- Token 带有 `xt-` 前缀
- Web UI 登录流程不受影响
