# Explorer 登录 CAPTCHA - RS

## 1. 修订记录

| 编写日期 | 版本 | 修订人 | 说明 |
| --- | --- | --- | --- |
| 2026-03-02 | 1.0 | 霍琳贺 | 初版：为 Explorer 登录增加可配置 CAPTCHA，默认关闭 |

## 2. 背景与问题

Explorer 目前支持用户名/密码登录（以及可选的 OAuth 登录）。在部分部署场景下（公网暴露、合规要求或存在爆破风险），需要在登录流程中加入 CAPTCHA（图形验证码）以提升抗自动化攻击能力。

## 3. 需求目标

1. 在 Explorer 登录时支持 CAPTCHA 校验。
2. CAPTCHA 默认不启用，避免影响现有用户体验与兼容性。
3. 当启用 CAPTCHA 时：每次登录都必须完成验证码校验（不做“失败次数阈值触发”）。
4. 前端 CAPTCHA 输入框交互与注册流程保持一致（弹窗、输入框 append 图片、点击刷新）。
5. 复用现有验证码图片 API（注册使用的 `/api/-/captcha`），避免新增接口与重复逻辑。

## 4. 术语

- CAPTCHA：图形验证码。
- Explorer Server：Explorer 后端服务（Rust/actix-web）。
- Explorer UI：Explorer 前端（Vue）。

## 5. 范围

### 5.1 本次开发范围

- 新增配置项控制登录 CAPTCHA 开关。
- 登录接口在开关开启时强制校验验证码。
- 前端登录页在开关开启时弹出验证码输入弹窗，并在登录请求中携带验证码。
- 复用验证码图片生成与校验逻辑（`/api/-/captcha` + 服务端缓存校验）。

### 5.2 非本次开发范围

- 基于“登录失败次数”触发 CAPTCHA（如 N 次失败后才要求）。
- 更换验证码算法/增加滑块验证码/接入第三方验证码服务。
- 账号锁定、IP 黑名单等其他风控策略。

## 6. 用户场景

1. 作为管理员，希望通过配置文件/环境变量开启登录 CAPTCHA，从而降低爆破风险。
2. 作为用户，当管理员开启 CAPTCHA 时，每次登录需要输入验证码；验证码错误会有明确提示并允许刷新重试。
3. 作为开发者/运维，希望验证码 API 与注册流程一致，减少维护成本。

## 7. 功能需求

### 7.1 FR-1 配置项

- 新增配置：`security.login_captcha = true|false`
- 默认值：`false`
- 环境变量：`EXPLORER_SECURITY_LOGIN_CAPTCHA=true|false`
- CLI：`--login-captcha`（由 clap derive 生成）

### 7.2 FR-2 查询登录选项（前端判断是否启用）

- 新增接口：`GET /api/-/login-options`
- 返回示例（成功）：
  - `{ "code": 0, "data": { "captchaEnabled": true }, "msg": null }`

### 7.3 FR-3 获取验证码图片（复用注册 API）

- 复用接口：`GET /api/-/captcha`
- 行为：
  - `phone_email` 必填（空则 400）。
  - 返回 `image/png`。
- 注意：为复用 API，登录流程将“用户名”作为 `phone_email` 参数传递。

### 7.4 FR-4 登录校验

- 登录接口：`POST /api/-/login`
- 当 `security.login_captcha=true`：
  - 请求体必须包含 `captcha` 字段。
  - 服务端使用 key `captcha-<username>` 校验验证码（与注册一致的 key 前缀）。
  - 校验失败：
    - 缺失验证码：401，`desc = "captchaRequired"`
    - 验证码错误/过期：401，`desc = "captchaInputError"`
- 当 `security.login_captcha=false`：
  - 不要求 `captcha` 字段，保持兼容。

## 8. 非功能需求

### 8.1 安全

- CAPTCHA 仅作为“增加攻击成本”的辅助措施，不替代密码强度、限速、锁定等策略。
- 不能在日志中记录用户输入的验证码明文。

### 8.2 性能

- 验证码生成与校验应在可接受延迟内完成（单次请求目标 < 200ms，具体以环境为准）。
- 验证码缓存具备过期机制（当前实现复用已有 5 分钟缓存过期逻辑）。

### 8.3 兼容性

- 默认关闭，不改变现有登录行为。
- 启用后，仅对登录流程增加验证码弹窗与请求字段，不影响 OAuth 流程。
