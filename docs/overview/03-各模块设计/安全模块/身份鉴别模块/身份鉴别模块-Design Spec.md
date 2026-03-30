# 身份鉴别模块-Design Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-08 | 2026-01-08 | 1.0 | 程洪泽 | 初稿 |

## 2. 引言

### 2.1 目的

本文档旨在描述 TDengine TSDB 身份鉴别模块的设计与实现，涵盖强口令策略、登录失败锁定、密码生命周期、多因素认证（TOTP、TOKEN）等功能的设计方案。

### 2.2 范围

本文档涵盖身份鉴别模块的以下方面：
- 用户创建和修改的语法扩展
- 口令策略和安全存储
- 用户锁定和解锁机制
- 资源限制和密码安全策略
- TOTP 多因素认证
- TOKEN 认证
- 登录流程和会话管理
- 兼容性考虑

### 2.3 受众

- TDengine 开发人员
- 系统架构师
- 安全审计人员
- 质量保证工程师

## 3. 术语

| 术语 | 定义 |
| --- | --- |
| TOTP | 基于时间的一次性密码算法，用于多因素认证 |
| TOKEN | 访问令牌，用于OAuth 2.0认证 |
| 盐值 | 随机字符串，用于密码散列前与密码拼接，增强安全性 |
| 散列算法 | 单向加密算法，将任意长度输入转换为固定长度输出 |
| 资源限制 | 对用户会话、连接、调用等的限制 |
| 密码生命周期 | 密码的有效期、重用限制等策略 |

## 4. 概述

### 4.1 架构

身份鉴别模块采用分层架构：
1. **语法解析层**：解析 `CREATE USER`、`ALTER USER`等 SQL 语句
2. **业务逻辑层**：实现口令策略、资源限制、认证逻辑
3. **数据存储层**：存储用户信息、TOTP 密钥、TOKEN 等
4. **接口层**：提供 C API 供客户端调用

### 4.2 技术

- **加密算法**：SHA-256 用于密码散列，TOTP 算法用于动态口令
- **随机数生成**：使用操作系统安全随机数生成器
- **数据库存储**：系统表存储用户信息、TOTP 密钥、TOKEN 等
- **网络通信**：TLS 加密传输，SASL 机制保护认证数据

### 4.3 依赖项

- OpenSSL：TLS 支持和加密算法
- 操作系统安全随机数 API
- TDengine 核心数据库引擎

## 5. 设计考虑

### 5.1 假设和限制

1. 企业版支持全部功能，社区版仅支持基本口令信息和用户锁定
2. 向后兼容3.3.8版本，升级无需手工干预
3. 性能影响：登录速度下降控制在30%以内

### 5.2 设计模式和原则

1. **单一职责原则**：每个函数负责单一功能
2. **开闭原则**：通过扩展而非修改现有代码添加新功能
3. **最小权限原则**：用户仅拥有必要权限
4. **防御性编程**：对所有输入进行验证和清理

### 5.3 风险和缓解措施

| 风险 | 缓解措施 |
| --- | --- |
| 密码泄露 | 使用加盐散列存储，不存储明文密码 |
| 重放攻击 | TOTP动态口令一次性有效 |
| 暴力破解 | 登录失败锁定机制 |
| 中间人攻击 | TLS加密传输，SASL机制保护 |

## 6. 详细设计

### 6.1 组件设计

#### 6.1.1 用户管理组件

- 负责用户创建、修改、删除
- 实现口令策略验证
- 管理用户资源限制

#### 6.1.2 认证组件

- 密码认证：用户名+密码验证
- TOTP 认证：动态口令验证
- TOKEN 认证：访问令牌验证

#### 6.1.3 会话管理组件

- 会话创建和销毁
- 会话状态跟踪
- 资源限制执行

#### 6.1.4 安全策略组件

- 密码策略执行
- 登录失败计数和锁定
- 密码生命周期管理

### 6.2 关键数据结构

#### 6.2.1 用户对象（SUserObj）

```c
typedef struct SUserObj {
  union {
    char name[TSDB_USER_LEN];
    char user[TSDB_USER_LEN];        // 用户名
  };

  // 密码历史记录，从新到旧，最新的是当前密码
  int32_t        numOfPasswords;     // 密码历史数量
  SUserPassword* passwords;          // 密码历史数组
  char           salt[TSDB_PASSWORD_SALT_LEN + 1];  // 盐值

  char    acct[TSDB_USER_LEN];       // 账户名
  char    totpsecret[TSDB_TOTP_SECRET_LEN];  // TOTP密钥
  int64_t createdTime;               // 创建时间（毫秒）
  int64_t updateTime;                // 更新时间（毫秒）
  int64_t uid;                       // 用户ID
  int8_t  superUser;                 // 是否超级用户
  int8_t  sysInfo;                   // 是否可查看系统信息
  int8_t  enable;                    // 是否启用
  int8_t  createdb;                  // 是否可创建数据库
  int8_t  changePass;                // 密码修改策略（0=不能修改，1=必须修改，2=可以修改）
  int32_t authVersion;               // 权限版本号
  
  // 资源限制字段
  int32_t sessionPerUser;            // 每用户最大会话数
  int32_t connectTime;               // 会话最大持续时间（分钟）
  int32_t connectIdleTime;           // 会话最大空闲时间（分钟）
  int32_t callPerSession;            // 单会话最大并发调用数
  int32_t vnodePerCall;              // 单调用最大涉及vnode数
  int32_t failedLoginAttempts;       // 允许连续失败登录次数
  int32_t passwordLifeTime;          // 密码有效期（天）
  int32_t passwordReuseTime;         // 密码重用时间（天）
  int32_t passwordReuseMax;          // 密码历史记录次数
  int32_t passwordLockTime;          // 密码锁定时间（分钟）
  int32_t passwordGraceTime;         // 密码过期宽限期（天）
  int32_t inactiveAccountTime;       // 账户不活动锁定时间（天）
  int32_t allowTokenNum;             // 允许的TOKEN数量
  int32_t tokenNum;                  // 当前TOKEN数量
  
  // IP和时间白名单
  SIpWhiteList*      pIpWhiteListDual;  // IP白名单
  SDateTimeWhiteList* pTimeWhiteList;   // 时间白名单
  
  // 权限相关
  SHashObj* roles;       // 角色
  SHashObj* readDbs;     // 读数据库权限
  SHashObj* writeDbs;    // 写数据库权限
  SHashObj* objPrivs;    // 对象权限
  SHashObj* selectTbs;   // SELECT表权限
  SHashObj* insertTbs;   // INSERT表权限
  SHashObj* updateTbs;   // UPDATE表权限
  SHashObj* deleteTbs;   // DELETE表权限
  
  SRWLatch  lock;                    // 读写锁
  int8_t    passEncryptAlgorithm;    // 密码加密算法
} SUserObj;

typedef struct {
  char    pass[TSDB_PASSWORD_LEN];   // 密码散列值
  int32_t setTime;                   // 密码设置时间（秒）
} SUserPassword;
```

#### 6.2.2 TOTP认证

TOTP 密钥直接存储在`SUserObj`结构的`totpsecret`字段中，长度为`TSDB_TOTP_SECRET_LEN`（32字节）。密钥通过函数`taosGenerateTotpSecret()`生成，该函数接收用户提供的种子（totpseed）作为输入：
```c
// 在SUserObj中：
char totpsecret[TSDB_TOTP_SECRET_LEN];  // TOTP密钥

// 生成TOTP密钥的函数
int taosGenerateTotpSecret(const char* seed, int seedLen, 
                          char* secret, int secretLen);
```

**生成流程**：
1. 用户在创建或修改用户时提供`totpseed`（8-255字符）
2. 系统使用`taosGenerateTotpSecret()`基于种子生成32字节的TOTP密钥
3. 密钥存储在用户对象中，用于后续的TOTP验证
**安全性**：
- TOTP密钥不可直接查看，只能通过重新生成的方式更新
- 用户需要使用 Google Authenticator 等软件录入密钥以生成验证码

#### 6.2.3 TOKEN 对象

```c
typedef struct STokenObj {
  char    name[TSDB_TOKEN_NAME_LEN];      // TOKEN名称（唯一标识）
  char    token[TSDB_TOKEN_LEN];          // 令牌字符串（自动生成）
  char    provider[TSDB_TOKEN_PROVIDER_LEN]; // 提供商信息
  char    user[TSDB_USER_LEN];            // 对应用户名
  char    extraInfo[TSDB_TOKEN_EXTRA_INFO_LEN]; // 额外信息（可扩展）
  int8_t  enabled;                        // 是否启用
  int32_t expireTime;                     // 过期时间（秒，0表示永不过期）
  int32_t createdTime;                    // 创建时间（秒）
} STokenObj;
```

**说明**：
- TOKEN 使用`name`字段作为唯一标识符，而不是`token`字符串本身
- `token`字符串是63位随机字符（A-Za-z0-9），由系统自动生成（`TSDB_TOKEN_LEN`=64，包含\0）
- `provider`和`extraInfo`字段用于支持 OAuth 2.0 等扩展场景
- 实际代码中没有使用`auth_code`和`client_id`字段，而是使用更通用的`provider`和`extraInfo`

### 6.3 数据库设计

#### 6.3.1 数据模型

##### 6.3.1.1 用户表（ins_users）

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| user_name | VARCHAR(64) | 用户名，主键 |
| num_of_passwords | INT | 密码历史数量 |
| passwords | BINARY | 密码历史数组（SUserPassword[]） |
| salt | VARCHAR(32) | 盐值（31字节+\0） |
| acct | VARCHAR(64) | 账户名 |
| totp_secret | VARCHAR(32) | TOTP密钥 |
| created_time | TIMESTAMP | 创建时间（毫秒） |
| update_time | TIMESTAMP | 更新时间（毫秒） |
| uid | BIGINT | 用户唯一ID |
| super_user | BOOLEAN | 是否超级用户 |
| sys_info | BOOLEAN | 是否可查看系统信息 |
| enable | BOOLEAN | 是否启用 |
| create_db | BOOLEAN | 是否可创建数据库 |
| change_pass | TINYINT | 密码修改策略（0/1/2） |
| auth_version | INT | 权限版本号 |
| session_per_user | INT | 每用户最大会话数（默认-1） |
| connect_time | INT | 会话最大时间/分钟（默认-1） |
| connect_idle_time | INT | 会话空闲时间/分钟（默认-1） |
| call_per_session | INT | 单会话并发调用数（默认-1） |
| vnode_per_call | INT | 单调用vnode数（默认-1） |
| failed_login_attempts | INT | 失败登录次数（默认3） |
| password_life_time | INT | 密码有效期/天（默认90） |
| password_reuse_time | INT | 密码重用时间/天（默认30） |
| password_reuse_max | INT | 密码历史次数（默认5） |
| password_lock_time | INT | 密码锁定时间/分钟（默认1） |
| password_grace_time | INT | 密码过期宽限期/天（默认7） |
| inactive_account_time | INT | 账户不活动时间/天（默认90） |
| allow_token_num | INT | 允许TOKEN数量（默认3） |
| token_num | INT | 当前TOKEN数量 |
| pass_encrypt_algorithm | TINYINT | 密码加密算法 |
| ... | ... | 其他权限相关字段 |

**说明**：
- 资源限制字段中，`-1`表示UNLIMITED（无限制）
- **重要**：代码中时间类资源限制以秒为单位存储，但在CREATE/ALTER USER语法中以天或分钟为单位指定
- root 用户的默认设置：
  - `session_per_user`, `failed_login_attempts`, `password_life_time`, `password_grace_time`, `inactive_account_time` 都设置为 -1（无限制）
  - `password_lock_time` 设置为 86400秒（1天）
- 普通用户的默认设置见代码中的 `TSDB_USER_*_DEFAULT` 常量

##### 6.3.1.2 TOKEN 表（ins_tokens）

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| name | VARCHAR(32) | TOKEN名称，主键（唯一标识） |
| token | VARCHAR(64) | 令牌字符串（63位随机字符+\0） |
| user_name | VARCHAR(64) | 用户名，外键 |
| provider | VARCHAR(64) | 提供商信息（用于OAuth 2.0等） |
| extra_info | VARCHAR(1024) | 额外信息（可扩展） |
| enabled | BOOLEAN | 是否启用 |
| create_time | INT | 创建时间（秒） |
| expire_time | INT | 过期时间（秒，0表示永不过期） |

**说明**：
- `name`字段是TOKEN的唯一标识符，由用户指定（最长 31 字符，`TSDB_TOKEN_NAME_LEN`=32 包含 \0）
- `token`字符串由系统自动生成，用于实际的认证（63 位随机字符A-Za-z0-9，`TSDB_TOKEN_LEN`=64 包含 \0）
- `expire_time`为0时表示令牌永不过期，否则为绝对时间戳（秒）
- 创建TOKEN时会增加用户的`token_num`计数，需检查是否超过`allow_token_num`限制
- `provider`最长 63 字符（`TSDB_TOKEN_PROVIDER_LEN`=64包含\0）
- `extra_info`最长 1023 字符（`TSDB_TOKEN_EXTRA_INFO_LEN`=1024包含\0）

#### 6.3.2 数据访问层

- 使用 TDengine 的 MNode 插件管理用户数据
- 通过`mndAcquireUser`、`mndReleaseUser`等函数访问用户对象
- 事务性操作确保数据一致性

### 6.4 图表解释

#### 6.4.1 数据流图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"graph TD\n    A[客户端请求] --\u003e B[语法解析]\n    B --\u003e C{认证类型}\n    C --\u003e|密码认证| D[密码验证]\n    C --\u003e|TOTP认证| E[TOTP验证]\n    C --\u003e|TOKEN认证| F[TOKEN验证]\n    D --\u003e G[检查密码策略]\n    E --\u003e H[检查TOTP状态]\n    F --\u003e I[检查TOKEN有效性]\n    G --\u003e J[创建会话]\n    H --\u003e J\n    I --\u003e J\n    J --\u003e K[返回连接句柄]\n","theme":"default","view":"chart"}"/>

#### 6.4.1 消息序列图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"sequenceDiagram\n    participant Client\n    participant taosc\n    participant mnode\n    participant auth\n    \n    Client-\u003e\u003etaosc: taos_connect_totp(user, pass, totp)\n    taosc-\u003e\u003emnode: 认证请求\n    mnode-\u003e\u003eauth: 验证用户密码\n    auth--\u003e\u003emnode: 密码验证结果\n    mnode-\u003e\u003eauth: 验证TOTP\n    auth--\u003e\u003emnode: TOTP验证结果\n    mnode--\u003e\u003etaosc: 认证成功\n    taosc--\u003e\u003eClient: TAOS*连接句柄\n","theme":"default","view":"chart"}"/>

#### 6.4.2 登录流程图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"flowchart TD\n    Start[开始登录] --\u003e Input[输入认证信息]\n    Input --\u003e CheckType{认证类型}\n    \n    CheckType --\u003e|密码| CheckPass[检查密码]\n    CheckPass --\u003e PassValid{密码有效?}\n    PassValid --\u003e|是| CheckTOTPReq{需要TOTP?}\n    PassValid --\u003e|否| Fail[登录失败]\n    \n    CheckTOTPReq --\u003e|是| CheckTOTP[检查TOTP]\n    CheckTOTPReq --\u003e|否| Success[登录成功]\n    \n    CheckTOTP --\u003e TOTPValid{TOTP有效?}\n    TOTPValid --\u003e|是| Success\n    TOTPValid --\u003e|否| Fail\n    \n    CheckType --\u003e|TOKEN| CheckToken[检查TOKEN]\n    CheckToken --\u003e TokenValid{TOKEN有效?}\n    TokenValid --\u003e|是| Success\n    TokenValid --\u003e|否| Fail\n    \n    Fail --\u003e UpdateFailCount[更新失败计数]\n    UpdateFailCount --\u003e CheckLock{达到锁定阈值?}\n    CheckLock --\u003e|是| LockAccount[锁定账户]\n    CheckLock --\u003e|否| End[结束]\n    LockAccount --\u003e End\n    Success --\u003e CreateSession[创建会话]\n    CreateSession --\u003e End\n","theme":"default","view":"chart"}"/>

#### 6.4.3 状态转换图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"stateDiagram-v2\n    [*] --\u003e Active: 创建用户\n    Active --\u003e Locked: 登录失败超限\u003cbr/\u003e或手动锁定\n    Active --\u003e Expired: 密码过期\n    Expired --\u003e Grace: 进入宽限期\n    Grace --\u003e Locked: 宽限期超时\n    Locked --\u003e Active: 解锁或锁定时间到\n    Expired --\u003e Active: 修改密码\n    Grace --\u003e Active: 修改密码\n","theme":"default","view":"chart"}"/>

## 7. 接口规范

### 7.1 API 文档

#### 7.1.1 新增 API

```c
/**
 * @brief 使用TOTP认证连接TDengine
 * @param ip 服务器IP地址
 * @param user 用户名
 * @param pass 密码
 * @param totpcode TOTP验证码
 * @param db 数据库名
 * @param port 端口号
 * @return TAOS* 连接句柄，失败返回NULL
 */
TAOS *taos_connect_totp(const char *ip, const char *user, const char *pass, 
                        const char* totpcode, const char *db, uint16_t port);

/**
 * @brief 使用TOKEN认证连接TDengine
 * @param ip 服务器IP地址
 * @param token 访问令牌
 * @param db 数据库名
 * @param port 端口号
 * @return TAOS* 连接句柄，失败返回NULL
 */
TAOS *taos_connect_token(const char *ip, const char *token, const char *db, uint16_t port);

/**
 * @brief 测试连接认证（不创建会话）
 * @param ip 服务器IP地址
 * @param user 用户名
 * @param pass 密码
 * @param totpcode TOTP验证码
 * @param db 数据库名
 * @param port 端口号
 * @return int 认证结果，0成功，非0错误码
 */
int taos_connect_test(const char *ip, const char *user, const char *pass, 
                      const char* totpcode, const char *db, uint16_t port);

/**
 * @brief 检查连接状态
 * @param taos 连接句柄
 * @return int 连接状态，0正常，非0异常
 */
int taos_check_connection(TAOS* taos);
```

#### 7.1.2 修改的 API

```c
/**
 * @brief 检查操作权限（增加token参数）
 * @param pMnode MNode实例
 * @param user 用户名
 * @param token 访问令牌（可为NULL）
 * @param operType 操作类型
 * @return int32_t 错误码，0成功
 */
int32_t mndCheckOperPrivilege(SMnode *pMnode, const char *user, 
                              const char* token, EOperType operType);

/**
 * @brief 检查修改用户权限（增加token参数）
 */
int32_t mndCheckAlterUserPrivilege(SMnode* pMnode, const char *opUser, 
                                   const char* opToken, SUserObj *pUser, 
                                   SAlterUserReq *pAlter);

/**
 * @brief 检查TOKEN权限
 */
int32_t mndCheckTokenPrivilege(SMnode* pMnode, const char* opUser, 
                               const char* opToken, const char *user, 
                               const char* token);
```

### 7.2 用户界面

#### 7.2.1 SQL 语法扩展

##### 7.2.1.1 CREATE USER 语法

```sql
CREATE USER [IF NOT EXISTS] <用户名> 
PASS <口令>
[TOTPSEED <totpseed>]
[ACCOUNT LOCK | ACCOUNT UNLOCK]
[SYSINFO {0|1}]
[CREATEDB {0|1}]
[CHANGEPASS {0|1|2}]
[TOTP {0|1}]
[SESSION_PER_USER <参数设置>]
[CONNECT_IDLE_TIME <参数设置>]
-- ... 其他资源限制
[HOST <HOST项>{,<HOST项>}]
[NOT_ALLOW_HOST <HOST项>{,<HOST项>}]
[ALLOW_DATETIME <时间项>{,<时间项>}]
[NOT_ALLOW_DATETIME <时间项>{,<时间项>}]
```

##### 7.2.1.2 ALTER USER 语法

```sql
ALTER USER <用户名>
<PASS <口令> |
 TOTPSEED <totpseed> |
 ENABLE {0|1} |
 ACCOUNT LOCK | ACCOUNT UNLOCK |
 SYSINFO {0|1} |
 CREATEDB {0|1} |
 CHANGEPASS {0|1|2} |
 SESSION_PER_USER <参数设置> |
 -- ... 其他修改项
 ADD HOST <HOST项> |
 ADD NOT_ALLOWED_HOST <HOST项> |
 DROP HOST <HOST项> |
 DROP NOT_ALLOWED_HOST <HOST项>>
```

##### 7.2.1.3 TOKEN 管理语法

```sql
-- 创建TOKEN
CREATE TOKEN <name>
  FROM USER <username> 
  [ENABLE {0|1}]           -- 默认1（启用）
  [TTL <seconds>]          -- 过期时间（秒），0或不指定表示永不过期
  [PROVIDER <provider_info>]
  [EXTRA_INFO <extra_info>]
  [IGNORE_EXISTS];         -- 如果已存在则忽略

-- 修改TOKEN
ALTER TOKEN <name> 
  SET ENABLE = {0|1} |     -- 启用/禁用
  SET TTL = <seconds> |    -- 修改过期时间
  SET PROVIDER = <provider_info> |
  SET EXTRA_INFO = <extra_info>;

-- 删除TOKEN
DROP TOKEN <name> [IGNORE_NOT_EXISTS];

-- 查询TOKEN
SELECT * FROM information_schema.ins_tokens 
WHERE name = '<name>' 
   OR user = '<username>';
```

**说明**：
- TOKEN 的名称`<name>`是用户指定的唯一标识符（最长31字符）
- 实际的`token`字符串由系统自动生成（63 位随机字符 A-Za-z0-9），在创建成功后返回
- `provider`（最长63字符）和`extra_info`（最长 1023 字符）用于支持扩展场景，如 OAuth 2.0
- 使用 TOKEN 登录的用户无权修改该 TOKEN 的属性

## 8. 安全考虑

### 8.1 安全要求

#### 8.1.1 口令安全

- 口令长度 8-255 位（`TSDB_PASSWORD_MIN_LEN`=8 到`TSDB_PASSWORD_MAX_LEN`=255）
- 至少包含大写字母、小写字母、数字、特殊字符中的三类
- 特殊字符包括：`! @ # $ % ^ & * ( ) - _ + = [ ] { } : ; > < ? | ~ , .`
- 禁止与用户名相同
- 使用加盐散列存储，不存储明文
**实际实现细节**：
- 盐值长度：`TSDB_PASSWORD_SALT_LEN` (31 字节) + \0
- 密码散列值长度：`TSDB_PASSWORD_LEN` (32 字节)
- 加密函数：`mndEncryptPass(char *pass, const char* salt, int8_t *algo)`
- 生成盐函数：`generateSalt(char *salt, size_t len)`
- 支持多种加密算法，通过`passEncryptAlgorithm`字段标识
- 旧版本（<3.3.8）使用 MD5 且未加盐，通过检查`salt`字段是否全0来判断并兼容

#### 8.1.2 传输安全

- 客户端与服务器通信使用 TLS 加密
- 通过 SASL 机制传输非明文密码信息
- 防止中间人攻击

#### 8.1.3 多因素认证

- TOTP 动态口令一次性有效，防重放攻击
- TOKEN 认证支持 OAuth 2.0 标准
- 认证级别可配置（密码、TOTP、UKEY）

### 8.2 漏洞缓解

#### 8.2.1 暴力破解防护

- 连续登录失败锁定机制
- 锁定时间可配置
- 自动解锁或手动解锁

#### 8.2.2 密码策略

- 密码生命周期管理
- 密码历史记录防止重用
- 密码复杂度强制要求

#### 8.2.3 会话安全

- 会话超时自动断开
- 空闲连接自动清理
- 并发会话数限制

## 9. 性能和可扩展性

### 9.1 性能要求

#### 9.1.1 登录性能

- 登录速度下降控制在 30% 以内
- 支持高并发登录请求
- 认证操作响应时间<100ms

#### 9.1.2 资源消耗

- 内存占用增加<10%
- CPU 使用率增加<5%
- 网络带宽增加可忽略

#### 9.1.3 资源限制默认值

以下是实际代码中定义的默认值（定义在`tdef.h`中）：

| 参数 | 常量名 | 默认值 | 说明 |
| --- | --- | --- | --- |
| SESSION_PER_USER | TSDB_USER_SESSION_PER_USER_DEFAULT | -1 | 无限制 |
| CONNECT_TIME | TSDB_USER_CONNECT_TIME_DEFAULT | -1 | 无限制 |
| CONNECT_IDLE_TIME | TSDB_USER_CONNECT_IDLE_TIME_DEFAULT | -1 | 无限制 |
| CALL_PER_SESSION | TSDB_USER_CALL_PER_SESSION_DEFAULT | -1 | 无限制 |
| VNODE_PER_CALL | TSDB_USER_VNODE_PER_CALL_DEFAULT | -1 | 无限制 |
| FAILED_LOGIN_ATTEMPTS | TSDB_USER_FAILED_LOGIN_ATTEMPTS_DEFAULT | 3 | root用户为-1（无限制） |
| PASSWORD_LIFE_TIME | TSDB_USER_PASSWORD_LIFE_TIME_DEFAULT | 129600 | 90天（代码中以秒存储），root用户为-1 |
| PASSWORD_REUSE_TIME | TSDB_USER_PASSWORD_REUSE_TIME_DEFAULT | 43200 | 30天（代码中以秒存储） |
| PASSWORD_REUSE_MAX | TSDB_USER_PASSWORD_REUSE_MAX_DEFAULT | 5 | 5次 |
| PASSWORD_LOCK_TIME | TSDB_USER_PASSWORD_LOCK_TIME_DEFAULT | 86400 | 1440分钟（代码中以秒存储，对root用户也生效） |
| PASSWORD_GRACE_TIME | TSDB_USER_PASSWORD_GRACE_TIME_DEFAULT | 10080 | 7天（代码中以秒存储），root用户为-1 |
| INACTIVE_ACCOUNT_TIME | TSDB_USER_INACTIVE_ACCOUNT_TIME_DEFAULT | 129600 | 90天（代码中以秒存储），root用户为-1 |
| ALLOW_TOKEN_NUM | TSDB_USER_ALLOW_TOKEN_NUM_DEFAULT | 3 | 3个 |

**特别说明**：
- `-1`表示 UNLIMITED（无限制）
- root 用户的大部分限制项默认设置为-1，以避免被锁定
- `PASSWORD_LOCK_TIME`对所有用户默认都是 86400 秒（1440 分钟，即 1 天）
- 代码中时间类资源限制以秒为单位存储，但 SQL 语法中以天或分钟为单位指定

### 9.2 可扩展性

#### 9.2.1 水平扩展

- 认证服务无状态设计，支持多实例部署
- 用户数据集中存储在 MNode，保证一致性
- 会话信息可分布式存储

#### 9.2.2 垂直扩展

- 模块化设计，各组件可独立升级
- 资源限制参数可动态调整
- 支持插件式扩展新认证方式

## 10. 部署和配置

### 10.1 部署流程

#### 10.1.1 企业版部署

1. 安装TDengine企业版软件包
2. 配置`multiFactorLevel`参数（1-3，分别代表密码、TOTP、UKEY）
3. 启动 TDengine 服务
4. 创建用户并配置认证方式

#### 10.1.2 社区版部署

1. 安装 TDengine 社区版软件包
2. 仅支持基本口令信息和用户锁定功能
3. 启动 TDengine 服务

### 10.2 配置管理

#### 10.2.1 服务端配置

```toml

## 11. 身份鉴别相关配置

enableStrongPassword = 1          # 强制强口令策略，默认1

## 12. multiFactorLevel = 2            # 多因素认证级别（1=密码，2=TOTP，3=UKEY）

                                  # 注：此参数在RS中提及但实际代码未实现
```

**说明**：
- `enableStrongPassword`是 3.3.8 版本已有的配置项
- 资源限制等配置在创建/修改用户时通过 SQL 语句设置，而非服务端配置文件
- TOTP 是否启用由用户级别的`TOTP`属性控制，而非全局配置

#### 12.0.1 客户端配置

```c
// 使用TOTP认证连接
TAOS *taos = taos_connect_totp("127.0.0.1", "user", "password", "123456", NULL, 6030);
if (taos == NULL) {
    printf("Failed to connect: %s\n", taos_errstr(NULL));
}

// 使用TOKEN认证连接（token是63位随机字符）
TAOS *taos = taos_connect_token("127.0.0.1", "Abc123XYZ...xyz789", NULL, 6030);
if (taos == NULL) {
    printf("Failed to connect: %s\n", taos_errstr(NULL));
}

// 检查连接状态（用于连接池管理）
int ret = taos_check_connection(taos);
if (ret != 0) {
    // 连接已断开，需要重新连接
    taos_close(taos);
    taos = taos_connect_totp(...);
}
```

### 12.1 版本控制

#### 12.1.1 向后兼容性

**基本要求**：
- 支持从 3.3.8 版本平滑升级
- 旧版本 MD5 密码自动兼容
- 新增语法不影响现有 SQL 语句
**旧版本用户口令的兼容**：
旧版本数据库创建的用户的口令使用的散列算法是MD5且未进行加盐处理，数据库中也未记录散列算法信息。
1. **判断方法**：通过检查`salt`字段是否全零来判断
  - 如果`salt`全零，则为旧版本用户（MD5，无盐）
  - 否则为新版本用户（使用`passEncryptAlgorithm`指定的算法和盐值）
1. **读取逻辑**：
  ```c
  if (sver < USER_VER_SUPPORT_ADVANCED_SECURITY) {
      // 旧版本数据，读取单个密码
      SDB_GET_BINARY(pRaw, dataPos, pUser->passwords[0].pass, TSDB_PASSWORD_LEN, _OVER)
      pUser->numOfPasswords = 1;
      memset(pUser->salt, 0, sizeof(pUser->salt));  // salt全零标记旧版本
  } else {
      // 新版本数据，读取密码历史和salt
      SDB_GET_INT32(pRaw, dataPos, &pUser->numOfPasswords, _OVER)
      // 读取密码数组和salt
  }
  ```

1. **验证逻辑**：
  - 如果`salt`全零，则按旧版本逻辑进行比对（使用MD5且不加盐）
  - 否则按新版本逻辑进行比对（使用`passEncryptAlgorithm`指定的算法和盐值）
1. **升级逻辑**：
  - 用户修改密码时，使用新版本逻辑进行散列处理，完成升级
  - 生成新的盐值并使用新的加密算法
**版本号管理**：
- `USER_VER_NUMBER`：当前用户表版本号
- `USER_VER_SUPPORT_ADVANCED_SECURITY`：支持高级安全特性的最低版本号

#### 12.1.2 发布说明

- 企业版：支持全部身份鉴别功能
- 社区版：仅支持基本口令信息和用户锁定
- 升级时自动迁移用户数据

#### 12.1.3 回滚策略

- 支持版本回滚到 3.3.8
- 回滚时保留用户基本信息
- 新增功能相关数据将被忽略

## 13. 监控和维护

### 13.1 日志记录和诊断

#### 13.1.1 日志内容

- 用户登录成功/失败记录
- 密码修改操作
- 用户锁定/解锁事件
- TOTP 密钥创建/更新
- TOKEN 创建/删除

#### 13.1.2 诊断工具

```sql
-- 查看用户状态
SHOW USERS;

-- 查看登录失败记录
SELECT * FROM information_schema.ins_login_attempts 
WHERE user = 'username' AND success = 0 
ORDER BY attempt_time DESC;

-- 查看密码过期用户
SELECT user_name, create_time, 
       DATE_ADD(create_time, INTERVAL password_life_time DAY) as expire_date
FROM information_schema.ins_users 
WHERE DATE_ADD(create_time, INTERVAL password_life_time DAY) < NOW();
```

### 13.2 维护

#### 13.2.1 日常维护

- 定期检查密码过期用户
- 监控登录失败记录
- 清理过期 TOKEN
- 审计用户权限变更

#### 13.2.2 故障处理

- 用户无法登录：检查账户状态、密码过期、锁定状态
- TOTP 认证失败：检查时间同步、密钥状态
- TOKEN 认证失败：检查 TOKEN 有效期、启用状态

#### 13.2.3 备份和恢复

- 定期备份用户表、TOTP 表、TOKEN 表
- 支持用户数据导入导出
- 灾难恢复时重建认证数据

## 14. 参考资料

### 14.1 内部文档

1. [身份鉴别模块-Requirement Spec](https://taosdata.feishu.cn/wiki/JKedwGMhui74RokhFZjcHHFxnnh)
2. [身份鉴别模块-Function Spec](https://taosdata.feishu.cn/wiki/WIbGwIzWLitDw6kcumpc43m9nab)
