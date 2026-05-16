# Explorer TOTP 和 Token 认证 - RS

## 1 引言

### 1.1 术语与缩写名词

- **TOTP**: Time-based One-Time Password，基于时间的一次性密码，由 RFC 6238 定义。
- **2FA**: Two-Factor Authentication，双因素认证，要求用户提供两种不同类型的身份验证因子。
- **MFA**: Multi-Factor Authentication，多因素认证，2FA 的泛化概念。
- **HOTP**: HMAC-based One-Time Password，基于 HMAC 的一次性密码，TOTP 的基础算法，由 RFC 4226 定义。
- **OTP**: One-Time Password，一次性密码的总称。
- **Token**: 令牌，由 TSDB 生成的长期凭证字符串，可替代用户名密码进行身份认证，适用于 API 调用、自动化脚本等无交互场景。
- **TTL**: Time To Live，令牌有效期，以天为单位。
- **认证器应用**: Authenticator App，生成 TOTP 码的客户端应用（如 Google Authenticator、Microsoft Authenticator、Authy 等）。

### 1.2 相关文档资料

- [身份鉴别 FS（飞书）](https://taosdata.feishu.cn/wiki/CXXqwV3Fai36rQkby6zcmBWwnMd) — TSDB 内核的身份鉴别功能规格说明书，本文档的核心依据
- RFC 6238: TOTP: Time-Based One-Time Password Algorithm
- RFC 4226: HOTP: An HMAC-Based One-Time Password Algorithm
- [Explorer OAuth2 SSO 需求规格说明书](../../explorer/docs/RS_OAuth2_SSO.md)

### 1.3 优先级要求

- **优先级**: 高
- **期望交付时间**: TBD
- **业务价值**: 增强 Explorer 账户安全性，满足企业客户对多因素认证的安全合规要求

### 1.4 版本要求

- **开源状态**: 企业版功能
- **发布版本**: TBD
- **支持范围**: 仅企业版支持，社区版不包含此功能

### 1.5 实现约束

1. **认证器兼容性**: TOTP 实现须严格遵循 RFC 6238 标准，确保与主流认证器应用（Google Authenticator、Microsoft Authenticator、Authy 等）兼容。
2. **用户体系依赖**: TOTP 和 Token 均基于 TDengine 现有用户体系，必须先有 TDengine 用户才能绑定 TOTP 或创建 Token。
3. **TSDB 能力封装**: Explorer 不自行实现 TOTP 验证和 Token 管理逻辑，而是通过 SQL 命令和 `TaosBuilder::from_dsn()` 的 DSN 参数（`totp_code`、`bearer_token` 等），封装 TSDB 内核已有能力。
4. **与 OAuth2 SSO 共存**: 当同时启用 OAuth2 SSO 和 TOTP 时，OAuth 登录后，不再需 TOTP 验证。
5. **时间同步**: 服务端须保证系统时钟准确（建议 NTP 同步），TOTP 验证应允许一定的时间窗口偏移以容忍客户端时钟偏差。
6. **Token 权限等同**: Token 认证后的权限与其所属用户的权限完全一致，由 TDengine 原生权限系统管理。
7. **Token 数量限制**: 每用户的 Token 数量受 TSDB 的 `ALLOW_TOKEN_NUM` 参数控制（默认 3 个，含已过期和禁用的），可通过 `ALTER USER <username> ALLOW_TOKEN_NUM <num>` 调整。taosx 自动创建的专用 Token 也计入此限额。Explorer 需在创建时处理超出限额的错误提示。
8. **taosx 任务兼容**: 用户启用 TOTP 后，原有的用户名密码方式将无法直接连接 TSDB，会影响该用户创建的 taosx DataIn 任务。Explorer 需在用户启用 TOTP 时自动为其创建专用 Token，供 taosx 任务和 TOTP 用户会话使用，并加密持久化存储到 Explorer 的 SQLite 数据库中（复用 OAuth2 SSO 的加密存储机制）。创建任务时自动向 `to` DSN 注入 `bearer_token` 参数，利用 DSN 认证优先级（`bearer_token` > `totp_code` > `user:password`）确保任务连接不受 TOTP 限制。

## 2 需求目标

当前 TDengine Explorer 仅依赖用户名和密码进行身份认证，存在以下问题：

1. **安全风险**: 一旦密码泄露，攻击者即可完全控制用户账户
2. **自动化不便**: API 调用、自动化脚本等场景需要硬编码用户名密码，既不安全也不便于管理
3. **功能入口分散**: 当前右上角仅有「修改密码」和「退出」两个选项，缺少统一的个人资料入口

TDengine TSDB 已在内核层面支持两种增强认证机制：

- **TOTP 认证**: 通过 `CREATE/DROP TOTP_SECRET` SQL 命令管理，提供双因素认证能力
- **Token 认证**: 通过 `CREATE/DROP/ALTER TOKEN` SQL 命令管理，提供基于令牌的无密码认证能力

但上述能力目前仅可通过命令行操作，缺少图形化界面支持。

本需求旨在：

1. **统一个人资料入口**: 将右上角的「修改密码」升级为「个人资料（Profile）」页面，包含「基本信息」「TOTP 认证」「Token 认证」三个子页面
2. **增强账户安全**: 通过 TOTP 在用户名密码之外增加第二重验证因子，即使密码泄露也无法直接登录
3. **支持无密码认证**: 通过 Token 机制为 API 调用、自动化脚本、第三方集成等场景提供安全的无密码认证方式
4. **图形化管理**: 为 TSDB 的 TOTP 和 Token 能力提供用户友好的 UI 操作界面
5. **合规要求**: 满足企业客户对多因素认证的安全合规需求（如等保、SOC2 等）
6. **自助式操作**: 用户可自主完成密码修改、TOTP 绑定/解绑和 Token 创建/管理，减少管理员运维负担

## 3 功能需求

### 3.1 页面结构总览

右上角用户图标的下拉菜单从原来的「修改密码 / 退出」改为「个人资料 / 退出」。点击「个人资料」进入 Profile 页面，左侧边栏导航包含三个子页面：

```
┌─────────────────────────────────────────────────────┐
│  Profile                                   │
├──────────┬──────────────────────────────────────────┤
│          │                                          │
│ 基本信息  │  （当前选中子页面的内容区域）                 │
│          │                                          │
│ TOTP 认证 │                                          │
│          │                                          │
│ Token 管理│                                          │
│          │                                          │
├──────────┴──────────────────────────────────────────┤
└─────────────────────────────────────────────────────┘
```

### 3.2 功能列表

| 序号 | 功能模块   | 功能名称           | 功能描述                                                                     |
| ---- | ---------- | ------------------ | ---------------------------------------------------------------------------- |
| 1    | 基本信息   | 修改密码           | 原有修改密码功能迁移至此子页面                                               |
| 2    | 基本信息   | 用户信息展示       | 展示当前用户名、TOTP 启用状态等基本信息                                      |
| 3    | TOTP 认证  | 生成 TOTP 密钥     | 调用 `CREATE TOTP_SECRET FOR USER` 为当前用户生成 TOTP 密钥                  |
| 4    | TOTP 认证  | 展示二维码         | 将 TOTP 密钥生成标准 otpauth:// URI 并渲染为二维码，供认证器应用扫码添加     |
| 5    | TOTP 认证  | 验证绑定           | 用户输入认证器生成的 6 位验证码，验证绑定是否成功                            |
| 6    | TOTP 认证  | TOTP 二次验证      | 登录时 TSDB 连接返回 TOTP 错误码后，提示用户输入 6 位验证码再次认证          |
| 7    | TOTP 认证  | 用户自助解绑       | 用户登录后可自行解绑 TOTP，调用 `DROP TOTP_SECRET FROM USER` 关闭双因素认证  |
| 8    | TOTP 认证  | TOTP 状态展示      | 在「基本信息」子页面展示当前用户的 TOTP 启用状态                             |
| 9    | Token 管理 | 创建 Token         | 调用 `CREATE TOKEN` 为当前用户创建令牌，支持设置名称、启用状态、TTL、备注    |
| 10   | Token 管理 | 查看 Token 列表    | 查询 `information_schema.ins_tokens` 展示当前用户的所有令牌                  |
| 11   | Token 管理 | 修改 Token         | 调用 `ALTER TOKEN` 修改令牌属性（启用/禁用、TTL 等）                         |
| 12   | Token 管理 | 删除 Token         | 调用 `DROP TOKEN` 删除指定令牌                                               |
| 13   | Token 管理 | Token 登录         | 支持用户使用 Token 代替用户名密码登录 Explorer                               |
| 14   | Token 管理 | Token 数量展示     | 在「基本信息」子页面展示当前用户的 Token 数量                                |
| 15   | taosx 兼容 | 自动创建专用 Token | 用户启用 TOTP 时，自动创建 taosx 专用 Token 并加密存储，确保后台任务不受影响 |
| 16   | taosx 兼容 | 隐藏专用 Token     | 前端 Token 列表过滤掉 taosx 自动创建的 Token，用户不可见                     |
| 17   | taosx 兼容 | 会话 Token 化      | TOTP 登录后会话使用 Token 认证，后续 SQL 查询无需 TOTP 码                     |
| 18   | taosx 兼容 | 任务 DSN 注入      | 创建或启动 DataIn 任务时自动向 `to` DSN 注入 `bearer_token`，xnode 优先使用 Token |
| 19   | taosx 兼容 | 连接池清理         | 用户登录时清除连接池缓存，防止旧连接绕过 TOTP 验证                           |

### 3.3 个人资料 — 基本信息

#### 3.3.1 页面入口变更

- 右上角用户图标的下拉菜单从「修改密码 / 退出」改为「个人资料 / 退出」
- 点击「个人资料」进入 Profile 页面，默认展示「基本信息」子页面

#### 3.3.2 用户信息展示

展示当前登录用户的基本信息：

- 用户名
- TOTP 状态（已启用 / 未启用）
- Token 数量（已创建 / 上限）

#### 3.3.3 修改密码

原有的「修改密码」功能迁移至此子页面，交互逻辑保持不变：

- 输入旧密码
- 输入新密码
- 确认新密码
- 点击「保存修改」

### 3.4 个人资料 — TOTP 认证

#### 3.4.1 TOTP 状态展示

进入「TOTP 认证」子页面时，根据用户的 TOTP 启用状态展示不同内容：

- **未启用**: 展示 TOTP 功能说明和「启用 TOTP」按钮
- **已启用**: 展示「TOTP 已启用」状态标识和「关闭 TOTP」按钮

状态通过查询 `information_schema.ins_users` 的 `totp` 字段获取。

#### 3.4.2 TOTP 绑定流程

```
用户进入「个人资料」→「TOTP 认证」子页面
        ↓
点击「启用 TOTP」
        ↓
Explorer 后端执行 CREATE TOTP_SECRET FOR USER <username>
        ↓
返回 TOTP 密钥（Base32 编码）
        ↓
前端生成 otpauth:// URI，渲染为二维码
同时展示密钥明文（供手动输入）
        ↓
用户使用认证器应用扫码 / 手动输入密钥
        ↓
用户输入认证器生成的 6 位验证码
        ↓
Explorer 后端通过 DSN 携带 totp_code 建立 TSDB 连接验证
  DSN: ws://<username>:<password>@<host>:<port>?totp_code=<6位验证码>
        ↓
  [连接成功] → 绑定完成，页面切换为「已启用」状态
  [连接失败] → 提示验证码错误，允许重试
```

**说明**:

- 二维码 URI 格式: `otpauth://totp/TDengine:<username>?secret=<secret>&issuer=TDengine`
- 绑定验证通过尝试建立带 `totp_code` 参数的 TSDB 连接完成，连接成功即证明验证码正确
- 绑定过程中的验证步骤是必要的，确保用户的认证器已正确配置后才正式启用 TOTP
- TOTP 绑定成功后，Explorer 后端需自动为该用户创建 taosx 专用 Token（见 3.7 taosx 任务兼容处理）

#### 3.4.3 TOTP 登录验证流程

```
用户输入用户名 + 密码
        ↓
Explorer 后端通过 DSN ws://user:pass@host:port 建立 TSDB 连接
        ↓
  [连接成功] → 未启用 TOTP，直接进入 Explorer 主界面
  [连接失败] → 检查错误码
        ↓
  [TSDB_CODE_MND_WRONG_TOTP_CODE] → 该用户已启用 TOTP，需要二次验证
        ↓
  前端展示 TOTP 验证码输入页面
        ↓
  用户输入认证器应用中的 6 位验证码
        ↓
  Explorer 后端通过 DSN ws://user:pass@host:port?totp_code=<code> 建立 TSDB 连接
        ↓
    [连接成功] → 登录成功，进入 Explorer 主界面
    [连接失败] → 提示验证码错误，允许重试
  [其他错误码] → 直接向用户展示对应错误信息（如密码错误、账户锁定等）
```

**说明**:

- 不需要预先查询用户的 TOTP 状态，而是直接尝试建立 TSDB 连接，通过返回的错误码 `TSDB_CODE_MND_WRONG_TOTP_CODE` 判断是否需要 TOTP 二次验证
- 携带 TOTP 验证码时，将 `totp_code` 作为 DSN 查询参数传递
- 登录失败锁定由 TSDB 内核的 `FAILED_LOGIN_ATTEMPTS` 和 `PASSWORD_LOCK_TIME` 参数控制，Explorer 不自行实现锁定逻辑

#### 3.4.4 TOTP 解绑流程

```
用户进入「个人资料」→「TOTP 认证」子页面
        ↓
点击「关闭 TOTP」
        ↓
输入当前 TOTP 验证码（确认操作者持有设备）
        ↓
Explorer 后端通过 DSN 携带 totp_code 建立 TSDB 连接验证验证码有效性
        ↓
验证通过后，通过该连接执行 DROP TOTP_SECRET FROM USER <username>
        ↓
解绑完成，页面切换为「未启用」状态
```

#### 3.4.5 恢复码机制

TSDB 层面目前未提供恢复码（Recovery Code）机制。当用户丢失认证器设备时，联系管理员进行 TOTP 强制解绑

### 3.5 个人资料 — Token 管理

#### 3.5.1 Token 列表页面

进入「Token 管理」子页面时，展示当前用户的 Token 列表和「创建 Token」按钮。

```
Explorer 后端执行:
  SELECT * FROM information_schema.ins_tokens
  WHERE user = '<username>'
        ↓
前端展示 Token 列表，包含以下列：
  - 名称（name）
  - 启用状态（enable）— 开关形式，可直接切换
  - 创建时间（create_time）
  - 过期时间（expire_time）— 已过期的标红提示
  - 创建者（provider）
  - 备注（extra_info）
  - 操作（编辑 / 删除）
```

**说明**: Token 列表不展示 Token 字符串本身，仅展示元数据信息。

#### 3.5.2 创建 Token

```
用户点击「创建 Token」按钮
        ↓
弹出创建表单，填写 Token 属性：
  - 名称（必填，唯一标识）
  - 是否立即启用（默认启用）
  - 有效期 TTL（以天为单位，0 表示永不过期）
  - 备注信息（可选）
        ↓
Explorer 后端执行:
  CREATE TOKEN IF NOT EXISTS <name> FROM USER <username>
    ENABLE <0|1> PROVIDER 'explorer' TTL <ttl> EXTRA_INFO '<info>'
        ↓
TSDB 返回生成的 Token 字符串
        ↓
前端弹窗展示 Token（仅展示一次，提供复制按钮，提示用户妥善保存）
        ↓
用户关闭弹窗，刷新列表
```

**说明**:

- `PROVIDER` 字段固定填写 `'explorer'`，标识该 Token 由 Explorer 创建
- Token 字符串仅在创建时展示一次，后续无法再次查看，需提醒用户立即复制保存
- `EXTRA_INFO` 可由用户填写备注，便于后续识别 Token 用途
- 每用户 Token 数量受 `ALLOW_TOKEN_NUM` 限制（默认 3 个，含已过期和禁用的），taosx 自动创建的专用 Token 也计入此限额，可通过 `ALTER USER <username> ALLOW_TOKEN_NUM <num>` 调整。超出限额时 TSDB 会返回错误，Explorer 需展示友好提示（如「Token 数量已达上限，请删除不需要的 Token 后重试」）

#### 3.5.3 修改 Token

```
用户在 Token 列表中点击「编辑」
        ↓
弹出编辑表单，可修改的属性：
  - 启用/禁用状态（ENABLE 0|1）
  - 有效期 TTL（修改后以修改时间起算）
        ↓
Explorer 后端执行:
  ALTER TOKEN <name> ENABLE <0|1> TTL <ttl>
        ↓
修改完成，刷新列表
```

#### 3.5.4 删除 Token

```
用户在 Token 列表中点击「删除」
        ↓
弹出二次确认对话框
        ↓
Explorer 后端执行:
  DROP TOKEN IF EXISTS <name>
        ↓
删除完成，刷新列表
```

#### 3.5.5 Token 登录流程

```
用户访问 Explorer 登录页面
        ↓
选择「Token 登录」方式（登录页面提供切换入口）
        ↓
输入 Token 字符串
        ↓
Explorer 后端通过 DSN ws://host:port?bearer_token=<token_string> 建立 TSDB 连接
        ↓
  [连接成功] → 通过 SELECT current_user() 获取当前用户名
             → 登录成功，进入 Explorer 主界面
  [连接失败] → 提示 Token 无效或已过期
```

**说明**:

- 登录页面需同时支持「用户名密码登录」和「Token 登录」两种方式的切换
- Token 登录成功后，若该用户已启用 TOTP，**不再**需要 TOTP 二次验证（Token 本身即为独立认证凭证）
- Token 登录时客户端不知道用户身份，需在连接成功后通过 `SELECT current_user()` 获取当前用户名
- Token 登录不受 `FAILED_LOGIN_ATTEMPTS`（登录失败锁定）和 `PASSWORD_LIFE_TIME`（密码过期）的影响

### 3.6 登录页面变更

登录页面需支持「用户名密码登录」和「Token 登录」两种方式的切换，详见 3.4.3（TOTP 登录验证流程）和 3.5.5（Token 登录流程）。

### 3.7 taosx 任务兼容处理

#### 3.7.1 问题背景

taosx DataIn 任务通过 DSN 中的用户名密码（`taos://user:pass@host`）连接 TSDB。用户启用 TOTP 后，连接将返回 `TSDB_CODE_MND_WRONG_TOTP_CODE` 错误，导致该用户创建的所有后台任务连接失败。

Token 认证不受 TOTP 影响（DSN 认证优先级：`bearer_token` > `totp_code` > `user:password`），因此需为 taosx 自动创建专用 Token，供后台任务和 TOTP 用户会话使用。

#### 3.7.2 自动创建 taosx 专用 Token

用户启用 TOTP 成功后（3.4.2 绑定流程验证通过），Explorer 后端自动执行：

```
检查是否已存在该用户的 taosx 专用 Token
（查询 ins_tokens 中 name = '__taosx_<username>__' 且 user = '<username>' 的记录）
        ↓
  [SQLite 中已有 Token] → 验证 Token 是否有效（尝试建立连接）
      [有效] → 跳过
      [无效] → 删除旧 Token（TSDB + SQLite），重新创建
  [不存在] → 执行 CREATE TOKEN __taosx_<username>__ FROM USER <username>
               ENABLE 1 TTL 0 EXTRA_INFO '__auto__'
           → 将返回的 Token 字符串使用 AES-256-GCM 加密后存入 Explorer SQLite
```

**命名与标记**:

- Token 名称: `__taosx_<username>__`（双下划线包裹，与用户手动创建的 Token 区分）
- 不设置 `PROVIDER`（避免 TSDB 对特定 Provider 的权限限制）
- `EXTRA_INFO`: `'__auto__'`（额外标记）
- `TTL`: `0`（永不过期）

#### 3.7.3 Token 持久化存储

新建独立的 `taosx_tokens` 表（不扩展 `oauth_users` 表，避免职责耦合）：

```sql
CREATE TABLE IF NOT EXISTS taosx_tokens(
    username text PRIMARY KEY NOT NULL,
    token_name text NOT NULL,
    encrypted_token text NOT NULL,
    created_at timestamp DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp DEFAULT CURRENT_TIMESTAMP
);
```

- `username` 作主键，每用户最多一条 taosx 专用 Token 记录
- Token 字符串使用 AES-256-GCM 加密后存入（复用 `SessionManager.encrypt_token()` / `decrypt_token()`）
- 加密密钥与 OAuth2 SSO 共用（`EXPLORER_SECURITY_ENCRYPTION_KEY`）
- 通过 sqlx migration 框架自动建表，升级无需手工干预

#### 3.7.4 TOTP 用户会话机制

TOTP 用户登录成功后，Explorer 会话中**不存储用户密码**，而是存储 taosx 专用 Token：

```
TOTP 登录成功 → 获取/创建 taosx 专用 Token
             → Session 密码字段存储 "__token__<token_value>"
             → 后续 REST SQL 代理请求使用 build_dsn() 时，
               检测 "__token__" 前缀，自动构建 bearer_token DSN
```

这确保 TOTP 用户登录后的所有 SQL 查询（数据浏览器、SQL 编辑器等）通过 Token 认证，无需每次提供 TOTP 验证码。

#### 3.7.5 任务创建时自动注入 Token

用户通过 Explorer 创建 DataIn 任务时，如果该用户已有 taosx 专用 Token，Explorer 自动在任务的 `to` DSN 中追加 `bearer_token` 参数：

```
原始 to DSN:  taos://user:pass@host:6030/db
注入后 DSN:   taos://user:pass@host:6030/db?bearer_token=<token_value>
```

由于 DSN 认证优先级 `bearer_token > user:password`，xnode 使用注入后的 DSN 时，优先通过 Token 认证，不受 TOTP 限制。原有的 `user:pass` 保留作为兜底（用户未启用 TOTP 时仍可用）。

#### 3.7.6 连接池管理

Explorer 使用 `deadpool` 连接池缓存 TSDB 连接。为防止旧连接绕过 TOTP 验证：

- 用户每次登录时，清除该用户的连接池缓存，强制建立新连接
- 确保 TOTP 状态变更（启用/禁用）后，后续连接使用最新的认证方式

#### 3.7.7 前端 Token 列表过滤

用户在「Token 管理」子页面查看 Token 列表时，前端过滤掉名称匹配 `__taosx_%__` 的记录，使自动创建的 Token 对用户不可见，避免误删。

#### 3.7.8 用户解绑 TOTP 时的处理

用户解绑 TOTP（3.4.4）后，taosx 专用 Token 可继续保留（Token 认证不依赖 TOTP 状态），不影响后台任务运行。

## 4 性能需求

1. **登录性能**: 根据 FS 要求，身份鉴别引入的额外开销（TOTP 验证、前置检查等）导致的登录速度下降控制在 30% 以内
2. **二维码生成**: 绑定流程中二维码生成和展示时间不超过 1s
3. **Token 操作响应时间**: Token 的创建、修改、删除操作响应时间不超过 500ms
4. **Token 列表查询**: Token 列表查询响应时间不超过 1s

## 5 安全需求

1. **密钥传输安全**: TOTP 密钥仅在绑定阶段通过 HTTPS 传输给前端一次，之后不再返回明文密钥
2. **Token 传输安全**: Token 字符串仅在创建时通过 HTTPS 返回一次，之后不再返回明文 Token
3. **二维码安全**: 二维码页面应提示用户「截图或密钥请妥善保管，关闭页面后无法再次查看」
4. **登录失败锁定**: 密码+TOTP 登录的失败锁定由 TSDB 内核的 `FAILED_LOGIN_ATTEMPTS` 和 `PASSWORD_LOCK_TIME` 参数控制，Explorer 不自行实现锁定逻辑，仅展示 TSDB 返回的锁定错误信息
5. **Token 登录不受密码策略影响**: Token 登录不受 `FAILED_LOGIN_ATTEMPTS`（登录失败锁定）和 `PASSWORD_LIFE_TIME`（密码过期）的影响
6. **解绑安全**: 用户自助解绑 TOTP 时须验证当前 TOTP 验证码，防止他人操作
7. **Token 删除安全**: 删除 Token 需二次确认，防止误操作
8. **操作审计**: TOTP 绑定/解绑、Token 创建/修改/删除等关键操作须记录审计日志
9. **时间窗口**: TOTP 验证允许前后各 1 个时间步长（共 3 个窗口，即 ±30 秒）的偏移
10. **Token 生命周期**: 建议用户设置合理的 TTL，避免使用永不过期的 Token；过期 Token 自动失效
11. **Token 数量限制**: 每用户 Token 数量受 `ALLOW_TOKEN_NUM` 参数控制（默认 3 个），防止无限创建

## 6 其他需求

### 6.1 兼容性需求

- **认证器兼容**: 兼容所有支持标准 TOTP（RFC 6238）的认证器应用，包括但不限于 Google Authenticator、Microsoft Authenticator、Authy、1Password 等
- **浏览器兼容**: 支持主流浏览器（Chrome 90+、Firefox 88+、Safari 14+、Edge 90+）
- **现有认证兼容**: 不影响现有的用户名密码认证方式，TOTP 和 Token 均为可选的增强/替代功能
- **OAuth2 SSO 兼容**: 若同时启用 OAuth2 SSO，OAuth 登录成功后不再需要 TOTP 二次验证；Token 登录不受 OAuth2 SSO 配置影响
- **TSDB 版本兼容**: 需 TDengine 企业版支持身份鉴别功能的版本（DSN 支持 `totp_code`、`bearer_token` 参数）；连接旧版本 TSDB 时，TOTP 和 Token 相关功能入口应隐藏或置灰
- **向后兼容**: 根据 FS 要求，从旧版本升级时不需要手工干预，旧版本创建的用户可正常使用

### 6.2 接口需求

#### 复用现有 SQL 代理接口

Explorer 已有通用 SQL 代理接口 `POST /api/-/rest/sql`，该接口会提取当前用户的认证信息，代理执行 SQL 并返回结果。以下 Token 管理和 TOTP 状态查询操作由前端直接调用此接口完成：

| 操作            | 前端通过 `/api/-/rest/sql` 执行的 SQL                                                                                    |
| --------------- | ------------------------------------------------------------------------------------------------------------------------ |
| 查询 TOTP 状态  | `SELECT totp FROM information_schema.ins_users WHERE name = '<username>'`                                                |
| 创建 Token      | `CREATE TOKEN IF NOT EXISTS <name> FROM USER <username> ENABLE <0\|1> PROVIDER 'explorer' TTL <ttl> EXTRA_INFO '<info>'` |
| 修改 Token      | `ALTER TOKEN <name> ENABLE <0\|1> TTL <ttl>`                                                                             |
| 删除 Token      | `DROP TOKEN IF EXISTS <name>`                                                                                            |
| 查询 Token 列表 | `SELECT * FROM information_schema.ins_tokens WHERE \`user\` = '<username>'`                                              |

#### 需要新增/修改的后端接口

| 接口                          | 方法 | 说明                                                        | 底层实现方式                                                                 |
| ----------------------------- | ---- | ----------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `/api/-/login`（修改）        | POST | 现有登录接口，需增加 TOTP 验证码参数                        | DSN 参数 `totp_code`：`ws://user:pass@host?totp_code=<code>`                 |
| `/api/-/login/token`          | POST | 新增 Token 登录接口                                         | DSN 参数 `bearer_token`：`ws://host?bearer_token=<token_string>`             |
| `/api/-/profile/totp/enable`  | POST | 新增 TOTP 绑定接口：不携带 `totp_code` 生成密钥，携带则验证 | 从会话获取凭据，生成密钥或 DSN 携带 `totp_code` 验证                         |
| `/api/-/profile/totp/disable` | POST | 新增 TOTP 解绑接口：携带 `totp_code` 验证后执行 DROP        | 从会话获取凭据，DSN 携带 `totp_code` 验证后执行 `DROP TOTP_SECRET FROM USER` |

**`/api/-/login` 修改说明**:

- 现有登录接口通过 `TaosBuilder::from_dsn("ws://user:pass@host:port")` 建立 TSDB 连接
- 需改为先尝试不带 `totp_code` 的连接
- 若返回 `TSDB_CODE_MND_WRONG_TOTP_CODE`，前端提示用户输入 TOTP 验证码后，携带验证码再次请求
- 后端收到带 TOTP 验证码的请求时，通过 `TaosBuilder::from_dsn("ws://user:pass@host:port?totp_code=<code>")` 建立连接完成认证

**`/api/-/login/token` 说明**:

- 新增接口，接收 Token 字符串
- 通过 `TaosBuilder::from_dsn("ws://host:port?bearer_token=<token_string>")` 建立 TSDB 连接
- 连接成功后通过 `SELECT current_user()` 获取当前用户名
- 建立 Explorer 会话并返回

**`/api/-/profile/totp/enable` 说明**:

- 新增接口，从会话中获取当前用户凭据（用户名 + 密码）
- 不携带 `totp_code` 时：通过 TSDB 连接执行 `CREATE TOTP_SECRET FOR USER <username>`，返回 Base32 密钥
- 携带 `totp_code` 时：通过 `TaosBuilder::from_dsn("ws://user:pass@host:port?totp_code=<code>")` 建立连接验证，成功后自动创建 taosx 专用 Token
- 请求体：`{ totp_code?: string }`

**`/api/-/profile/totp/disable` 说明**:

- 新增接口，从会话中获取当前用户凭据
- 通过 DSN 携带 `totp_code` 建立 TSDB 连接验证验证码有效性
- 验证通过后执行 `DROP TOTP_SECRET FROM USER <username>`
- 请求体：`{ totp_code: string }`

#### TSDB 连接器使用方式

Explorer 后端通过 `taos` Rust crate 的 `TaosBuilder::from_dsn()` 建立 TSDB 连接，不同认证方式通过 DSN 参数区分：

| 认证方式             | DSN 格式                                         | 说明                         |
| -------------------- | ------------------------------------------------ | ---------------------------- |
| 用户名 + 密码        | `ws://user:pass@host:port`                       | 现有方式                     |
| 用户名 + 密码 + TOTP | `ws://user:pass@host:port?totp_code=<6位验证码>` | TOTP 验证码作为 DSN 查询参数 |
| Token                | `ws://host:port?bearer_token=<token_string>`     | Token 作为 DSN 查询参数      |

### 6.3 运维需求

- **无额外配置**: TOTP 和 Token 能力均由 TSDB 内核提供，Explorer 层面无需额外的服务端配置
- **日志记录**: 记录 TOTP 绑定/解绑、Token 创建/修改/删除、认证成功/失败等关键操作的日志

### 6.4 易用性需求

- **绑定引导**: 提供清晰的分步引导，指导用户完成认证器应用的安装和 TOTP 绑定
- **二维码 + 手动输入**: 同时提供二维码扫码和密钥明文手动输入两种绑定方式
- **Token 创建引导**: 创建 Token 时提供各字段的说明和推荐值（如建议设置合理 TTL）
- **一次性展示提醒**: TOTP 密钥和 Token 字符串仅展示一次，需在 UI 上明确提醒用户立即复制保存
- **错误提示**: 认证失败时给出明确提示（如「验证码错误」「Token 无效或已过期」）
- **状态可见**: 用户在安全设置页面可清晰看到 TOTP 启用状态和 Token 列表
- **文档支持**: 提供使用帮助文档，包含 TOTP 常见问题（时间不同步、设备丢失）和 Token 使用场景指引

### 6.5 测试需求

- **单元测试**: TOTP 和 Token 相关后端接口的单元测试覆盖率不低于 90%
- **集成测试**: 验证 Explorer 与 TSDB TOTP/Token SQL 命令的完整交互流程
- **前端测试**: 覆盖 TOTP 绑定/登录验证/解绑和 Token 创建/修改/删除/登录的完整 UI 交互流程
- **安全测试**: 验证暴力破解防护、密钥/Token 传输安全等安全机制
- **兼容性测试**: 使用 Google Authenticator、Microsoft Authenticator 等主流认证器验证 TOTP 兼容性

## 参考文档

### TDengine TSDB 的 TOTP 认证

#### 如何开启 TOTP 认证

1. 为用户生成 TOTP 密钥

```shell
taos> CREATE TOTP_SECRET FOR USER root;
                     totp_secret                      |
=======================================================
 TAL2P4XLCH4S5YMG6C2VRKQPKUFTY5E3VEH2MH2E2EUYTWKXYCLA |
Query OK, 1 row(s) in set (0.007352s)
```

通过执行 SQL：`CREATE TOTP_SECRET FOR USER <username>` 为指定的用户开启 TOTP 认证。SQL 会返回 TOTP 密钥。

2. 在认证器中添加密钥，生成验证码
3. 登录时输入 TOTP 验证码（6位数字）
4. TSDB 会校验 TOTP 验证码

#### 如何解绑？

登录后，执行 SQL

```shell
taos> DROP TOTP_SECRET FROM USER root;
Drop OK, 0 row(s) affected (0.008095s)
```

#### 如何查看某个用户是否开启了 TOTP？

执行SQL

```
taos> select totp from information_schema.ins_users;
 totp |
=======
    0 |
Query OK, 1 row(s) in set (0.005449s)
```

0 表示没有开启TOTP，1 表示开启TOTP。

### TDengine TSDB 的 Token 认证

#### 如何创建 token？

执行 SQL，为用户生成一个 Token

```SQL
CREATE TOKEN [IF NOT EXISTS] <令牌名称> FROM USER <用户名> [<令牌属性>]

<令牌属性> ::= ENABLE { 0 | 1 } |
              PROVIDER <provider name>
              TTL <ttl>
              EXTRA_INFO <extra info>
```

参数说明：

- ENABLE：1 是立即生效，0是创建token但不生效。
- PROVIDER：创建者名称，TSDB 只记录不使用。
- TTL：指定令牌有效期，以天为单位，0表示永不过期；创建 TOKEN 时从创建时起算，修改后以修改时间起算。
- EXTRA_INFO：备注信息，TSDB 只记录不使用，由写入方负责解释。

示例：

```
taos> CREATE TOKEN IF NOT EXISTS m111 FROM USER root ENABLE 1 PROVIDER 'zyyang' TTL 0 EXTRA_INFO 'hello TSDB';
                              token                              |
==================================================================
 lNilHp97SmHEliDdDwTWbhrLFSPAH8SdGuRCOIAJNFXmColiXnzPG9evHyMjvCl |
Query OK, 1 row(s) in set (0.009725s)
```

#### 如何删除 token？

执行 SQL：

```SQL
DROP TOKEN [IF EXISTS] <令牌名称>
```

示例

```shell
taos> DROP TOKEN m111;
Drop OK, 0 row(s) affected (0.006516s)
```

#### 如何修改 token？

执行sql:

```SQL
ALTER TOKEN <令牌名称> [<令牌属性>]
```

示例：

```shell
taos> ALTER TOKEN m111 TTL 1;
Query OK, 0 row(s) affected (0.008563s)
```

#### 如何查看所有 token

执行 SQL：

```SQL
SELECT * FROM information_schema.ins_tokens;
```

或者

```SQL
SHOW TOKENS;
```

示例：

```shell
taos> show tokens;
              name              |            user            |            provider            | enable |       create_time       |       expire_time       |           extra_info           |
=============================================================================================================================================================================================
 m111                           | root                       | zyyang                         |      1 | 2026-03-19 15:19:35.000 | 1970-01-01 08:00:00.000 | hello TSDB                     |
Query OK, 1 row(s) in set (0.007547s)

taos> select * from information_schema.ins_tokens;
              name              |            user            |            provider            | enable |       create_time       |       expire_time       |           extra_info           |
=============================================================================================================================================================================================
 m111                           | root                       | zyyang                         |      1 | 2026-03-19 15:19:35.000 | 1970-01-01 08:00:00.000 | hello TSDB                     |
Query OK, 1 row(s) in set (0.007633s)
```

#### 如何用 Token 认证？

taos shell 可以通过 `-q` 参数指定 token
TDengine TSDB 的连接器也已经支持了 token 认证接口

示例：

```shell
taos -q
Enter token:
```

输入 token 后进入

#### 创建非root用户后需要授权

```
create user aaa pass 'tbase125!';
grant role `SYSDBA` to aaa;
```
