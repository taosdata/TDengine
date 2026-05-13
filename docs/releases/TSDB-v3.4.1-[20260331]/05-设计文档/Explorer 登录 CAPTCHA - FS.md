# Explorer 登录 CAPTCHA - FS

## 1. 修订记录

| 编写日期 | 版本 | 修订人 | 说明 |
| --- | --- | --- | --- |
| 2026-03-02 | 1.0 | 霍琳贺 | 初版：定义登录 CAPTCHA 的接口、前端交互与配置方式（与实现对齐） |

## 2. 背景

参见 RS：[Explorer 登录 CAPTCHA - RS](https://taosdata.feishu.cn/wiki/O7c6wE91nid595kfSLUcFmkwnle)

## 3. 设计原则

1. 默认不启用，显式配置才启用。
2. 与注册验证码 UI/样式一致。
3. API 复用：验证码图片沿用注册的 `/api/-/captcha`。
4. 服务端强校验：开关启用时，登录请求必须携带验证码且校验通过。

## 4. 定义

- CAPTCHA

## 5. 用户行为

### 5.1 配置

#### 5.1.1 explorer.toml

```toml
[security]
login_captcha = true
```

#### 5.1.2 环境变量

- `EXPLORER_SECURITY_LOGIN_CAPTCHA=true`

#### 5.1.3 CLI

- `--login-captcha`

### 5.2 后端接口（实现对齐）

#### 5.2.1 获取登录选项

- Method: `GET`
- Path: `/api/-/login-options`
- Auth: 无（`noAuth: true`）
- Response（成功）：
```json
{ "code": 0, "data": { "captchaEnabled": true }, "msg": null }
```

- 说明：
  - `captchaEnabled` 由服务端读取 `security.login_captcha`。

#### 5.2.2 获取图形验证码（复用注册 API）

- Method: `GET`
- Path: `/api/-/captcha`
- Query:
  - `phone_email`：必填。为复用注册 API，登录流程将“用户名”传入此参数。
  - `ts`：可选，仅用于缓存 bust（浏览器端）。
- Response:
  - `200 image/png`
  - `400 application/json`：`{ code, desc }`（当 `phone_email` 为空）
- 服务端缓存 key 规则：
  - key = `captcha-<phone_email>`
  - 登录场景下等价于 `captcha-<username>`

#### 5.2.3 登录

- Method: `POST`
- Path: `/api/-/login`
- Request JSON：
```json
{
  "username": "root",
  "encrypted_password": "<time-based-xor>",
  "captcha": "1234"
}
```

- 兼容性：当 `security.login_captcha=false` 时，`captcha` 可省略。
- 失败行为（当启用 CAPTCHA）：
  - 缺少 captcha：HTTP 401，JSON `{ "code": <FAILED>, "desc": "captchaRequired" }`
  - captcha 错误/过期：HTTP 401，JSON `{ "code": <FAILED>, "desc": "captchaInputError" }`
- 注意：验证码校验通过后，会将该 key 从缓存移除（一次性使用）。

### 5.3 前端交互（实现对齐）

#### 5.3.1 登录页行为

1. 页面加载时调用 `/api/-/login-options` 判断是否开启 CAPTCHA。
2. 若开启：用户点击“登录”按钮后弹出与注册一致的验证码弹窗。
3. 弹窗内：
  - 输入框样式类：`captcha-input`
  - 图片容器类：`captcha-img-box`
  - 点击图片刷新：重新请求 `/api/-/captcha?phone_email=<username>&ts=<now>`
1. 用户确认验证码后：
  - 把验证码写入 `dynamicValidateForm.captcha`
  - 继续调用登录 API，并在 body 中携带 `captcha`
1. 若服务端返回 `captchaRequired`/`captchaInputError`：
  - 弹出错误提示（i18n：`login.captchaRequired` / `login.captchaInputError`）
  - 清空已输入验证码并重新打开验证码弹窗

#### 5.3.2 与注册页面一致性

- 登录页复用与注册页相同的弹窗结构（`el-dialog` + append 图片），以及相同的样式片段（`.captcha-input`）。

## 6. 性能

- 验证码生成与校验应在可接受延迟内完成（单次请求目标 < 200ms，具体以环境为准）。
- 验证码缓存具备过期机制（当前实现复用已有 5 分钟缓存过期逻辑）。

## 7. 安全

- CAPTCHA 仅作为“增加攻击成本”的辅助措施，不替代密码强度、限速、锁定等策略。
- 不能在日志中记录用户输入的验证码明文。

## 8. 兼容性

无。

## 9. 运维

- 开启后会显著改变登录体验（每次登录都弹验证码）。
- 建议同时开启 HTTPS、反向代理限速等手段，CAPTCHA 不是唯一的安全策略。

## 10. 使用场景

1. 作为管理员，希望通过配置文件/环境变量开启登录 CAPTCHA，从而降低爆破风险。
2. 作为用户，当管理员开启 CAPTCHA 时，每次登录需要输入验证码；验证码错误会有明确提示并允许刷新重试。
3. 作为开发者/运维，希望验证码 API 与注册流程一致，减少维护成本。

## 11. 约束和限制

- 必须提供可访问的 IDMP `base_url`。
- 账号必须可登录，且目标环境需关闭验证码。
- MCP 客户端需支持 stdio 模式。

## 12. 常见错误和排查

- `captchaRequired`：提示“请先获取并输入图形验证码”。
- `captchaInputError`：提示“图形验证码错误，请重新输入”。

## 13. 可观测性

UI 可见。

## 14. 安装和卸载

无变化。

## 15. 文档

需要修改企业版 Explorer 配置文档。

## 16. 参考文档
