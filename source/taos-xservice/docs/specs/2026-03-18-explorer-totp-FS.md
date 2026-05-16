# Explorer TOTP 和 Token 认证 - FS

# 修订记录

| 编写日期   | 发布日期   | 版本 | 修订人 | 主要修改内容                             |
| ---------- | ---------- | ---- | ------ | ---------------------------------------- |
| 2026-03-20 | 2026-03-20 | 1.0  |        | 初版：基于 RS 整理 TOTP 和 Token 认证 FS |

# 背景

本文档定义 TDengine Explorer 中 TOTP 双因素认证和 Token 认证的产品行为与实现细节。

TDengine TSDB 内核已在身份鉴别 FS 中实现了 TOTP 和 Token 认证能力，Explorer 作为 UI 层需要封装这些能力，提供图形化的管理和登录界面。本文档以 [身份鉴别 FS](https://taosdata.feishu.cn/wiki/CXXqwV3Fai36rQkby6zcmBWwnMd) 和 [Explorer TOTP 和 Token 认证 RS](./RS.md) 为依据。

# 定义

- **TOTP**: Time-based One-Time Password，基于时间和共享密钥生成一次性密码的算法（RFC 6238）。
- **Token**: 由 TSDB 生成的长期凭证字符串，可替代用户名密码进行身份认证。
- **TTL**: Time To Live，令牌有效期，以天为单位。
- **认证器应用**: 生成 TOTP 码的客户端应用（Google Authenticator、Microsoft Authenticator、Authy 等）。
- **SQL 代理**: Explorer 已有的 `POST /api/-/rest/sql` 接口，代理执行 SQL 到 TSDB。

# 范围与设计原则

- Explorer 不自行实现 TOTP 验证和 Token 管理逻辑，而是封装 TSDB 内核已有能力。
- Token CRUD 管理操作通过现有 SQL 代理接口完成。
- TOTP 绑定/解绑、登录流程需要新增/修改后端 API（通过 DSN 参数 `totp_code`/`bearer_token` 建立 TSDB 连接）。
- 安全策略（登录失败锁定、密码过期等）由 TSDB 内核控制，Explorer 仅展示错误信息。
- 以实现代码为准；任何行为变动必须同时更新实现与本文件。

# 行为说明

## 页面结构

### 入口变更

右上角用户图标的下拉菜单从原来的「修改密码 / 退出」改为「个人资料 / 退出」。

### Profile 页面

点击「个人资料」进入 Profile 页面，左侧边栏导航包含三个子页面：

- **基本信息**: 用户信息展示 + 修改密码
- **TOTP 认证**: TOTP 状态查看、绑定、解绑
- **Token 管理**: Token 列表、创建、修改、删除

默认展示「基本信息」子页面。

### 登录页面

登录页面新增「Token 登录」入口，支持「用户名密码登录」和「Token 登录」两种方式的切换。

## 基本信息子页面

### 用户信息展示

展示当前登录用户的基本信息（只读）：

- 用户名
- TOTP 状态（已启用 / 未启用）— 通过 SQL 代理执行 `SELECT totp FROM information_schema.ins_users WHERE name = '<username>'` 获取
- Token 数量 — 通过 SQL 代理执行 `SELECT count(*) FROM information_schema.ins_tokens WHERE \`user\` = '<username>'` 获取

### 修改密码

原有的「修改密码」功能迁移至此子页面，交互逻辑和后端实现保持不变。

表单字段：

- 旧密码（必填）
- 新密码（必填）
- 确认新密码（必填）

提交后调用现有修改密码 API。

## TOTP 认证子页面

### 状态展示

进入子页面时，通过 SQL 代理查询 TOTP 状态：

```sql
SELECT totp FROM information_schema.ins_users WHERE name = '<username>';
```

根据返回值展示不同 UI：

- `totp = 0`（未启用）: 展示 TOTP 功能说明文案 + 「启用 TOTP」按钮
- `totp = 1`（已启用）: 展示「TOTP 已启用」状态标识 + 「关闭 TOTP」按钮

### 绑定流程（后端 API）

TOTP 绑定通过新增的后端接口 `POST /api/-/profile/totp/enable` 统一管理，分两步交互完成。

**第 1 步：生成密钥**

前端调用 `POST /api/-/profile/totp/enable`（不携带 `totp_code`）。

后端从会话中获取当前用户凭据，通过 TSDB 连接执行：

```sql
CREATE TOTP_SECRET FOR USER <username>;
```

TSDB 返回 Base32 编码的密钥字符串，后端将密钥返回给前端。

前端使用返回的密钥生成 `otpauth://` URI 并渲染为二维码：

```
otpauth://totp/TDengine:<username>?secret=<secret>&issuer=TDengine
```

同时展示密钥明文（供无法扫码时手动输入）。

页面提示：「请使用认证器应用扫描二维码或手动输入密钥。密钥仅展示一次，关闭页面后无法再次查看。」

> 二维码生成在前端完成（使用 `qrcode` 等 JS 库），无需后端参与。

**第 2 步：验证绑定**

用户输入认证器生成的 6 位验证码，前端调用 `POST /api/-/profile/totp/enable`（携带 `totp_code`）。

后端从会话中获取当前用户凭据，通过 DSN 携带 `totp_code` 参数建立到 TSDB 的连接来验证：

```
ws://<username>:<password>@<host>:<port>?totp_code=<6位验证码>
```

使用 `TaosBuilder::from_dsn()` 建立连接，若连接成功则说明 TOTP 验证码正确，绑定完成。

- 验证成功：后端自动为该用户创建 taosx 专用 Token（见 taosx 任务兼容处理章节），返回成功，页面切换为「已启用」状态
- 验证失败：返回错误，提示「验证码错误」，允许重试

### 解绑流程（后端 API）

TOTP 解绑通过新增的后端接口 `POST /api/-/profile/totp/disable` 完成。

1. 用户点击「关闭 TOTP」
2. 弹出确认对话框，要求输入当前 TOTP 验证码（确认操作者持有设备）
3. 前端调用 `POST /api/-/profile/totp/disable`，携带 `totp_code`
4. 后端从会话中获取当前用户凭据，先通过 DSN 携带 `totp_code` 建立 TSDB 连接验证验证码有效性
5. 验证通过后，通过该连接执行：

```sql
DROP TOTP_SECRET FROM USER <username>;
```

6. 页面切换为「未启用」状态

### 恢复码机制

TSDB 层面目前未提供恢复码（Recovery Code）机制。当用户丢失认证器设备时，联系管理员进行 TOTP 强制解绑。

### TOTP 登录验证流程（后端实现）

此流程在 Explorer 后端的登录接口 `/api/-/login` 中实现。

**现有行为**:

```
POST /api/-/login { username, encrypted_password }
  → 解密 encrypted_password（TimeBasedXor 加密传输）
  → 通过 DSN ws://user:pass@host:port 建立 TSDB 连接
  → 成功：建立会话，返回 session
  → 失败：返回错误
```

**修改后行为**:

```
POST /api/-/login { username, encrypted_password, totp_code? }
  → 解密 encrypted_password（TimeBasedXor 加密传输）
  → 若请求不含 totp_code:
      通过 DSN ws://user:pass@host:port 建立 TSDB 连接
      → 成功：建立会话，返回 session
      → 失败且错误码 = TSDB_CODE_MND_WRONG_TOTP_CODE:
          返回 HTTP 401，body 中标识 need_totp = true
      → 其他错误：返回对应错误信息
  → 若请求含 totp_code:
      通过 DSN ws://user:pass@host:port?totp_code=<code> 建立 TSDB 连接
      → 成功：建立会话，返回 session
      → 失败：返回对应错误信息
```

**前端交互**:

1. 前端提交 `{ username, encrypted_password }`（密码经 TimeBasedXor 加密传输）
2. 若后端返回 `need_totp = true`，前端展示 TOTP 验证码输入界面
3. 用户输入 6 位验证码后，前端重新提交 `{ username, encrypted_password, totp_code }`
4. 后端通过 DSN 携带 `totp_code` 参数建立 TSDB 连接完成认证

**实现要点**:

- 登录接口的请求体新增可选字段 `totp_code`（明文传输，不经 TimeBasedXor 加密）
- 需要能识别 TSDB 连接器返回的 `TSDB_CODE_MND_WRONG_TOTP_CODE` 错误码
- 登录失败锁定由 TSDB 内核的 `FAILED_LOGIN_ATTEMPTS` 和 `PASSWORD_LOCK_TIME` 参数控制，Explorer 不自行实现

**参考文件**:

- 后端登录逻辑：`explorer/server/src/main.rs`（现有登录相关 handler）
- TSDB 连接器：`TaosBuilder::from_dsn()` 支持 DSN 中 `totp_code` 参数

## Token 管理子页面

### Token 列表

进入子页面时，前端通过 SQL 代理查询当前用户的 Token 列表：

```sql
SELECT * FROM information_schema.ins_tokens WHERE `user` = '<username>';
```

列表展示以下列：

| 列名     | 字段          | 说明                                   |
| -------- | ------------- | -------------------------------------- |
| 名称     | `name`        | Token 唯一标识                         |
| 启用状态 | `enable`      | 开关形式，1=启用 0=禁用，可直接切换    |
| 创建时间 | `create_time` | —                                      |
| 过期时间 | `expire_time` | 已过期的标红提示；永不过期的显示为 `—` |
| 创建者   | `provider`    | —                                      |
| 备注     | `extra_info`  | —                                      |
| 操作     | —             | 编辑 / 删除按钮                        |

**注意**: Token 列表不展示 Token 字符串本身，仅展示元数据。

**启用状态切换**: 用户点击开关时，前端通过 SQL 代理执行：

```sql
ALTER TOKEN <name> ENABLE <0|1>;
```

### 创建 Token

用户点击「创建 Token」按钮，弹出创建表单：

| 字段   | 类型 | 必填 | 默认值 | 说明                      |
| ------ | ---- | ---- | ------ | ------------------------- |
| 名称   | 文本 | 是   | —      | Token 唯一标识            |
| 启用   | 开关 | —    | 启用   | 是否立即生效              |
| 有效期 | 数字 | —    | 0      | TTL（天），0 表示永不过期 |
| 备注   | 文本 | 否   | —      | 用途说明                  |

提交后，前端通过 SQL 代理执行：

```sql
CREATE TOKEN IF NOT EXISTS <name> FROM USER <username>
  ENABLE <0|1> PROVIDER 'explorer' TTL <ttl> EXTRA_INFO '<info>';
```

**成功**：TSDB 返回 Token 字符串。前端弹窗展示 Token，提供复制按钮，提示：「Token 仅展示一次，请立即复制保存。关闭此窗口后无法再次查看。」

**失败处理**：

- Token 数量超出 `ALLOW_TOKEN_NUM` 限制（默认 3 个）：展示「Token 数量已达上限，请删除不需要的 Token 后重试」。注意：taosx 自动创建的专用 Token 也计入 `ALLOW_TOKEN_NUM` 限额，若限额不足可通过 `ALTER USER <username> ALLOW_TOKEN_NUM <num>` 调整
- Token 名称重复：展示「该名称已存在，请使用其他名称」
- 其他错误：展示 TSDB 返回的错误信息

**实现要点**: `PROVIDER` 固定为 `'explorer'`，标识由 Explorer 创建。

### 修改 Token

用户点击列表中的「编辑」按钮，弹出编辑表单：

| 字段   | 可修改 | 说明                            |
| ------ | ------ | ------------------------------- |
| 名称   | 否     | 只读展示                        |
| 启用   | 是     | 开关切换                        |
| 有效期 | 是     | TTL（天），修改后以修改时间起算 |

提交后，前端通过 SQL 代理执行：

```sql
ALTER TOKEN <name> ENABLE <0|1> TTL <ttl>;
```

### 删除 Token

用户点击列表中的「删除」按钮：

1. 弹出确认对话框：「确定要删除 Token "<name>" 吗？删除后使用该 Token 的所有连接将立即失效。」
2. 确认后，前端通过 SQL 代理执行：

```sql
DROP TOKEN IF EXISTS <name>;
```

3. 刷新列表。

### Token 登录流程（后端实现）

新增接口 `POST /api/-/login/token`。

**请求**:

```json
{
  "token": "<token_string>"
}
```

**后端处理**:

```
POST /api/-/login/token { token }
  → 通过 DSN ws://host:port?bearer_token=<token_string> 建立 TSDB 连接
  → 成功：
      通过 SELECT current_user() 查询当前用户名
      建立 Explorer 会话（与密码登录共用会话机制）
      返回 session
  → 失败：返回 HTTP 401，提示 Token 无效或已过期
```

**前端交互**:

1. 用户在登录页切换到「Token 登录」模式
2. 输入 Token 字符串，点击登录
3. 调用 `POST /api/-/login/token`
4. 成功后进入 Explorer 主界面

**安全约束**（来自身份鉴别 FS）:

- Token 登录不需要 TOTP 二次验证
- Token 登录不受 `FAILED_LOGIN_ATTEMPTS` 和 `PASSWORD_LIFE_TIME` 的影响

**参考文件**:

- 后端登录逻辑：`explorer/server/src/main.rs`
- TSDB 连接器：`TaosBuilder::from_dsn()` 支持 DSN 中 `bearer_token` 参数

## taosx 任务兼容处理

### 问题

taosx DataIn 任务通过 DSN 中的用户名密码连接 TSDB。用户启用 TOTP 后，连接会返回 `TSDB_CODE_MND_WRONG_TOTP_CODE` 错误，导致后台任务连接失败。而 Token 认证不受 TOTP 影响。

DSN 认证优先级：`bearer_token` > `totp_code` > `user:password`。当 DSN 中同时存在多种认证参数时，TSDB 按此优先级选择认证方式。

### 方案

Explorer 通过以下三层机制确保 TOTP 用户的任务和会话正常运行：

1. **自动创建 Token**：TOTP 绑定成功后，自动创建 taosx 专用 Token
2. **会话 Token 化**：TOTP 登录后，会话使用 Token 认证代替密码认证
3. **任务 DSN 注入**：创建 DataIn 任务时，自动向 `to` DSN 注入 `bearer_token`

### 自动创建流程

在 TOTP 绑定成功后（3.4.2 验证通过），后端执行以下逻辑：

```rust
// 伪代码
async fn create_taosx_token_if_needed(dsn: &Dsn, username: &str, session_mgr: &SessionManager) -> Result<()> {
    let token_name = format!("__taosx_{}__", username);

    // 1. 检查 SQLite 中是否已有 Token，且是否有效
    if let Some(token) = session_mgr.get_taosx_token(username).await? {
        if verify_token_works(&token).await {
            return Ok(()); // Token 有效，跳过
        }
        // Token 无效（可能旧版带 PROVIDER 限制），删除重建
        drop_token_in_tsdb(dsn, &token_name).await?;
        session_mgr.delete_taosx_token(username).await?;
    }

    // 2. 检查 TSDB 中是否已有同名 Token（可能是旧版），删除后重建
    if token_exists_in_tsdb(dsn, &token_name, username).await {
        drop_token_in_tsdb(dsn, &token_name).await?;
    }

    // 3. 创建 Token（不设 PROVIDER，避免权限限制）
    let result = query_with_dsn(dsn, &format!(
        "CREATE TOKEN IF NOT EXISTS {} FROM USER {} ENABLE 1 TTL 0 EXTRA_INFO '__auto__'",
        token_name, username
    )).await?;

    let token_string = result.get_token_value()?;

    // 4. 加密存储到 SQLite
    session_mgr.store_taosx_token(username, &token_name, &token_string).await?;

    Ok(())
}
```

### Token 命名与标识

| 属性         | 值                     | 说明                            |
| ------------ | ---------------------- | ------------------------------- |
| `name`       | `__taosx_<username>__` | 双下划线包裹，与用户 Token 区分 |
| `EXTRA_INFO` | `'__auto__'`           | 额外标记                        |
| `TTL`        | `0`                    | 永不过期                        |
| `ENABLE`     | `1`                    | 立即生效                        |

### 持久化存储

新建独立的 `taosx_tokens` 表存储 taosx 专用 Token，不扩展 `oauth_users` 表（OAuth 用户映射表与 taosx Token 职责不同，且非 OAuth 用户不在该表中）。

**新增 migration 文件**: `explorer/server/migrations/20260326000001_taosx_tokens.up.sql`

```sql
-- taosx 专用 Token 存储（用户启用 TOTP 时自动创建）
CREATE TABLE IF NOT EXISTS taosx_tokens(
    username text PRIMARY KEY NOT NULL,       -- TDengine 用户名
    token_name text NOT NULL,                 -- TSDB 中的 Token 名称 (__taosx_<username>__)
    encrypted_token text NOT NULL,            -- AES-256-GCM 加密后的 Token 字符串
    created_at timestamp DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp DEFAULT CURRENT_TIMESTAMP
);
```

对应 down 文件: `explorer/server/migrations/20260326000001_taosx_tokens.down.sql`

```sql
DROP TABLE IF EXISTS taosx_tokens;
```

**设计说明**:

- `username` 作主键，每用户最多一条 taosx 专用 Token 记录
- 加密方式复用 `SessionManager` 现有的 `encrypt_token()` / `decrypt_token()` 方法（AES-256-GCM），共用 `EXPLORER_SECURITY_ENCRYPTION_KEY` 加密密钥
- sqlx migration 框架自动执行，版本升级无需手工干预

**写入流程**（TOTP 绑定成功后）:

```
SessionManager.encrypt_token(token_string)
    → INSERT INTO taosx_tokens (username, token_name, encrypted_token) VALUES (?, ?, ?)
```

**读取流程**（TOTP 登录或 taosx 任务连接 TSDB 时）:

```
SELECT encrypted_token FROM taosx_tokens WHERE username = ?
    → SessionManager.decrypt_token(encrypted_token)
    → TaosBuilder::from_dsn("ws://host:port?bearer_token=<decrypted_token>")
```

**参考实现**:

- 加密/解密逻辑：`explorer/server/src/oauth/session.rs` 中的 `encrypt_token` / `decrypt_token` 方法
- AES 工具函数：`explorer/server/src/utils/aes.rs` 中的 `aes_encrypt_base64` / `aes_decrypt_base64`
- 密钥加载：`explorer/server/src/security/mod.rs` 中的 `SecurityConfig::load_encryption_key`
- 数据库池：`SessionManager` 复用 `Storage` 的 `SqlitePool`

### TOTP 用户会话机制

TOTP 用户登录后，会话中**不存储用户密码**，改为存储 taosx 专用 Token：

```
TOTP 登录成功
    → create_taosx_token_if_needed() 获取/创建专用 Token
    → Session 密码字段存储 "__token__<token_value>"
    → build_dsn() 检测 "__token__" 前缀 → dsn.set("bearer_token", token)
```

后续所有 REST SQL 代理请求（数据浏览器、SQL 编辑器等）自动使用 Token 认证，无需每次提供 TOTP 验证码。

### 任务创建与启动时自动注入 Token

用户通过 Explorer 创建或启动 DataIn 任务时，如果该用户已有 taosx 专用 Token，Explorer 自动在任务的 `to` DSN 中追加 `bearer_token` 参数：

```
原始 to DSN:  taos://user:pass@host:6030/db
注入后 DSN:   taos://user:pass@host:6030/db?bearer_token=<token_value>
```

**注入时机**（覆盖所有任务启动路径）：

| 操作           | 函数                  | 注入方式                                      |
| -------------- | --------------------- | --------------------------------------------- |
| 创建任务       | `create_task_inner()` | 修改 `config.to` 后写入 `CREATE XNODE TASK`   |
| 更新任务       | `update_task()`       | 修改 `config.to` 后写入 `ALTER XNODE TASK`    |
| 启动已有任务   | `start_task()`        | 先查询再 `ALTER XNODE TASK` 更新 DSN          |
| 批量启动任务   | `batch_start_tasks()` | 逐个查询再 `ALTER XNODE TASK` 更新 DSN        |
| 导入任务       | `import_task()`       | 走 `create_task_inner()` 路径                 |

**启动已有任务的注入流程**：

```
start_task(task_id)
    → extract_username_from_request() 获取当前用户名
    → session_manager.get_taosx_token(username) 获取 Token
    → SHOW XNODE TASKS WHERE ID = task_id 查询当前 to DSN
    → 如果 bearer_token 已存在且相同，跳过
    → ALTER XNODE TASK {id} FROM '{from}' TO '{new_to_with_bearer_token}'
    → START XNODE TASK {id}
```

实现位置：`explorer/server/src/x_api/tasks.rs`。

注入逻辑：
1. 从请求中提取当前用户名
2. 查询 `SessionManager.get_taosx_token(username)` 获取加密 Token
3. 如果存在，解密后追加到 `to` DSN 的 `bearer_token` 查询参数
4. 对已有任务，先通过 `ALTER XNODE TASK` 更新 DSN，再 `START`
5. `import_task`（任务导入）和 `update_task`（任务更新）同样注入

由于 DSN 认证优先级 `bearer_token > user:password`，注入后 xnode 优先使用 Token 认证，不受 TOTP 限制。原有的 `user:pass` 保留作为兜底。

### 连接池管理

Explorer 使用 `deadpool` 连接池缓存 TSDB 连接。为防止旧连接绕过 TOTP 验证（连接在 TOTP 启用前创建，仍在缓存中可用）：

- 用户每次登录时调用 `clear_pool()` 清除该用户的连接池缓存
- 强制建立新连接，确保 TOTP 状态变更后立即生效

实现位置：`explorer/server/src/main.rs` 的 `login()` handler，在 `query_with_dsn()` 之前调用 `clear_pool(&dsn, auth.username.clone())`。

### 前端过滤

Token 列表查询时，过滤掉自动创建的 Token：

```sql
-- 原查询
SELECT * FROM information_schema.ins_tokens WHERE `user` = '<username>';

-- 改为（前端过滤或 SQL 过滤）
SELECT * FROM information_schema.ins_tokens WHERE `user` = '<username>' AND name NOT LIKE '__taosx_%__';
```

### 生命周期

| 事件               | 行为                                                       |
| ------------------ | ---------------------------------------------------------- |
| 用户启用 TOTP      | 自动创建 taosx Token，加密存入 SQLite                      |
| TOTP 登录          | Session 使用 Token 认证，后续查询自动通过 bearer_token     |
| 创建 DataIn 任务   | 自动向 `to` DSN 注入 `bearer_token`                        |
| 用户解绑 TOTP      | Token 保留不删除（不影响后台任务）                         |
| 用户被删除         | TSDB 自动清理该用户的所有 Token，SQLite 中的记录也应清理   |
| taosx 任务连接失败 | 检查 SQLite 中是否有该用户的 Token，若无则提示用户重新登录 |

# 前端实现要点

## 技术选型

| 需求          | 方案                                      |
| ------------- | ----------------------------------------- |
| 二维码生成    | 前端 JS 库（如 `qrcode.vue` 或 `qrcode`） |
| TOTP URI 构建 | 前端按 `otpauth://` 标准格式拼接          |
| SQL 执行      | 调用现有 `POST /api/-/rest/sql` 代理接口  |
| Token 列表    | Element Plus Table 组件                   |
| 侧边栏导航   | 自定义侧边栏组件（用户头像 + 导航菜单）  |

## 路由

新增 Profile 页面路由：

```
/profile/basic    → 基本信息（默认）
/profile/totp     → TOTP 认证
/profile/tokens   → Token 管理
```

## 状态管理

无需新增 Pinia store，所有数据均通过 SQL 代理实时查询，不做本地缓存。

# 安全

## 已由 TSDB 内核保障

- 登录失败锁定（`FAILED_LOGIN_ATTEMPTS` + `PASSWORD_LOCK_TIME`）
- 密码过期策略（`PASSWORD_LIFE_TIME`）
- Token 数量限制（`ALLOW_TOKEN_NUM`）
- TOTP 密钥的生成、存储和校验

## Explorer 需保障

- TOTP 密钥仅在绑定阶段通过 HTTPS 传输给前端一次，之后不再返回明文
- Token 字符串仅在创建时返回一次，之后不再返回明文
- 解绑 TOTP 时须验证当前验证码
- 删除 Token 需二次确认
- taosx 自动创建的 Token 使用 AES-256-GCM 加密存储在 SQLite 中，与 OAuth2 SSO 共用加密密钥

# 兼容性

- **TSDB 版本兼容**: 需 TDengine 企业版支持身份鉴别功能的版本；连接旧版本 TSDB 时，前端检测到不支持 TOTP/Token 的 SQL 报错后，应隐藏或置灰相关功能入口
- **OAuth2 SSO 兼容**: OAuth 登录后不再需要 TOTP 二次验证；Token 登录不受 OAuth2 SSO 配置影响
- **taosx 任务兼容**: 启用 TOTP 后不影响已有的 DataIn 任务运行（通过自动创建专用 Token + 任务 DSN 注入 `bearer_token` 解决）
- **向后兼容**: 从旧版本升级时不需要手工干预，旧版本创建的用户可正常使用
- **浏览器兼容**: Chrome 90+、Firefox 88+、Safari 14+、Edge 90+

# 运维

- **无额外配置**: TOTP 和 Token 能力均由 TSDB 内核提供，Explorer 层面无需额外的服务端配置项
- **日志**: 后端记录 TOTP/Token 相关操作的日志（绑定、解绑、创建、修改、删除、登录成功/失败）

# 测试需求

## 后端

- `/api/-/login` TOTP 流程：
  - 未启用 TOTP 用户正常登录
  - 已启用 TOTP 用户首次请求返回 `need_totp = true`
  - 携带正确 `totp_code` 登录成功
  - 携带错误 `totp_code` 登录失败
- `/api/-/login/token` 流程：
  - 有效 Token 登录成功，返回正确用户名
  - 无效/过期/禁用 Token 登录失败
  - Token 登录后不触发 TOTP
- taosx 专用 Token 自动管理：
  - 用户启用 TOTP 后，自动创建 `__taosx_<username>__` Token
  - Token 正确加密存入 SQLite
  - 重复启用 TOTP 不重复创建 Token
  - 旧版 Token（带 PROVIDER 限制）自动迁移重建
  - taosx 任务使用该 Token 成功连接 TSDB
- TOTP 登录会话：
  - TOTP 登录后会话使用 Token 认证
  - 数据浏览器、SQL 编辑器等功能正常工作
  - 连接池清理确保 TOTP 状态变更立即生效
- 任务 DSN 注入：
  - 创建 DataIn 任务时 `to` DSN 自动注入 `bearer_token`
  - 导入任务时同样注入
  - 未启用 TOTP 的用户创建任务时不注入（无 taosx token）

## 前端

- Profile 页面三个子页面的渲染和切换
- TOTP 绑定完整流程（生成密钥 → 展示二维码 → 输入验证码 → 绑定成功）
- TOTP 解绑流程（输入验证码 → 解绑成功）
- Token CRUD 完整流程（创建 → 列表展示 → 修改 → 删除）
- Token 创建后一次性展示和复制
- Token 数量超限时的错误提示
- Token 列表中不显示名称匹配 `__taosx_%__` 的自动创建 Token
- 登录页 Token 登录切换和流程

## 兼容性

- Google Authenticator、Microsoft Authenticator 验证 TOTP 兼容性
- 连接不支持 TOTP/Token 的旧版 TSDB 时，功能入口正确隐藏

# 参考文件

- RS 文档：`docs/specs/20260318-explorer-TOTP/RS.md`
- 身份鉴别 FS（飞书）：https://taosdata.feishu.cn/wiki/CXXqwV3Fai36rQkby6zcmBWwnMd
- Explorer 后端入口：`explorer/server/src/main.rs`
- SQL 代理实现：`explorer/server/src/main.rs` 中 `rest_proxy()` 函数
- 认证中间件：`explorer/server/src/oauth/middleware.rs`
- 前端 API 层：`explorer/src/api/`
- OAuth2 SSO FS：`explorer/docs/FS_OAuth2_SSO.md`
