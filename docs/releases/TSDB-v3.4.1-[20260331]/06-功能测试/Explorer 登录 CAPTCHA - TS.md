# Explorer 登录 CAPTCHA - TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-02 | 2026-03-02 | 1.0 | 霍琳贺 | 初版：登录 CAPTCHA（默认关闭、可配置开启、复用注册 CAPTCHA API、登录 UI 与注册一致） |

## 2. 测试目标

本测试覆盖 Explorer 登录 CAPTCHA 的开关配置、后端接口/校验逻辑、前端交互与回归影响，确保在默认关闭场景下不改变既有行为，在开启场景下每次登录都强制完成 CAPTCHA 校验。
- 验证默认关闭时登录流程兼容性
- 验证开启后端强校验（缺失/错误验证码必须拒绝）
- 验证前端 CAPTCHA 弹窗交互与注册保持一致，并复用 `/api/-/captcha`

## 3. 参考文档

- RS：[Explorer 登录 CAPTCHA - RS](https://taosdata.feishu.cn/wiki/O7c6wE91nid595kfSLUcFmkwnle)
- FS：[Explorer 登录 CAPTCHA - FS](https://taosdata.feishu.cn/wiki/F8GHwnPtJiX6tLkjgiNcfRRnnLh)

## 4. 测试结论

通过。

## 5. 测试环境

- OS: Windows, Linux
- Browser: Chrome

## 6. 功能测试

### 6.1 登录 CAPTCHA（配置与后端）

#### 6.1.1 测试要点

- 新增开关 `security.login_captcha` 默认关闭；开启后每次登录强制校验 `captcha`。
- 登录选项接口可被前端用于判断是否启用 CAPTCHA。
- 验证码图片接口复用注册接口 `/api/-/captcha`，参数 `phone_email` 必填。
- 验证码校验 key 与注册一致：`captcha-<username>`（登录场景下把 username 作为 phone_email 传入以复用 API）。

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| BE-01 | /api/-/login-options 返回开关状态 | A/B 两种配置下分别 GET `/api/-/login-options`，期望 `data.captchaEnabled` 与配置一致 | 通过 |
| BE-02 | /api/-/captcha 缺少 phone_email | GET `/api/-/captcha`，期望 HTTP 400 且 JSON `desc` 包含 `phone_email is required` | 通过 |
| BE-03 | /api/-/captcha 正常返回图片 | GET `/api/-/captcha?phone_email=test_user&ts=<now>`，期望 HTTP 200 且 `Content-Type=image/png` | 通过 |
| BE-04 | 登录（开关关闭）不要求 captcha | 配置 A：POST `/api/-/login` 不带 captcha；正确密码应成功，错误密码保持原行为 | 通过 |
| BE-05 | 登录（开关开启）缺少 captcha | 配置 B：POST `/api/-/login` 不带 captcha，期望 HTTP 401 且 `desc=captchaRequired` | 通过 |
| BE-06 | 登录（开关开启）captcha 错误 | 配置 B：先生成 captcha，再 POST `/api/-/login` 携带错误 captcha，期望 HTTP 401 且 `desc=captchaInputError` | 通过 |
| BE-07 | 登录（开关开启）captcha 正确 | 配置 B：生成 captcha 并输入正确值，POST `/api/-/login` 登录成功 | 通过 |
| BE-08 | captcha 一次性使用 | 配置 B：使用同一 captcha 连续登录两次；第二次必须失败（验证码一次性） | 通过 |

### 6.2 登录 CAPTCHA（前端交互）

#### 6.2.1 测试要点

- 登录页加载后通过 `/api/-/login-options` 判断是否开启 CAPTCHA。
- 开启后：点击登录触发 CAPTCHA 弹窗（`el-dialog` + `el-input` append 图片），输入框样式与注册一致（`captcha-input`/`captcha-img-box`）。
- CAPTCHA 图片刷新复用注册 API：`fetchCaptcha(username, ts)` -> `/api/-/captcha?phone_email=<username>&ts=<now>`。
- 后端返回 `captchaRequired`/`captchaInputError` 时前端应提示并重新弹出验证码弹窗。

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| FE-01 | 开关关闭：不弹验证码 | 配置 A：输入用户名/密码点击登录，不出现验证码弹窗，登录行为与历史一致 | 通过 |
| FE-02 | 开关开启：点击登录弹出验证码弹窗 | 配置 B：输入用户名/密码点击登录，出现与注册一致的验证码弹窗 | 通过 |
| FE-03 | 验证码图片点击刷新 | 配置 B：在验证码弹窗点击图片刷新，应更新图片并请求带新 ts | 通过 |
| FE-04 | 验证码为空点击确认 | 配置 B：弹窗中不输入验证码点击确认，应触发表单校验提示 `login.captchaTips` | 通过 |
| FE-05 | 验证码错误提示与重试 | 配置 B：输入错误验证码，提示 `login.captchaInputError` 并重新弹出/刷新验证码 | 通过 |
| FE-06 | 验证码正确登录成功 | 配置 B：输入正确验证码，登录成功并跳转 `/explorer` | 通过 |
| FE-07 | 用户名变更后验证码需重新生成 | 配置 B：更改用户名后再次登录，应重新生成验证码且不能复用旧验证码 | 通过 |

## 7. 易用性测试（可选）

- UI 是否美观？
- 交互是否合理？
- 字体、字号是否合适？
- 是否存在错别字？
- 点击切换语言按钮后，UI 上的所有元素是否按照选择的语言，正确展示？

## 8. 长期稳定性测试（可选）

- 可选：在启用 CAPTCHA 的情况下持续运行 24h，观察验证码缓存、登录失败重试、内存占用是否异常。

## 9. 性能测试

- 可选：在典型部署环境下压测 `/api/-/captcha` 与 `/api/-/login`（启用 CAPTCHA），观察 P95 延迟与错误率；验证码生成应在可接受范围内。

## 10. 安全测试

- 验证码输入不应被记录到日志。
- 默认关闭配置下不引入额外安全回退行为。
- 接口参数校验：`/api/-/captcha` 缺参不应 panic。

## 11. 兼容性测试

- 升级安装后默认关闭：既有用户名/密码登录与 OAuth 登录不受影响。
- 注册流程验证码仍可正常使用（复用接口不影响注册）。

## 12. 已知问题和限制（可选）

- 当前策略为“开启后每次登录都需要 CAPTCHA”，不支持按失败次数触发（如需可后续扩展）。
