# 身份鉴别 RS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-09-09 | - | 0.1 | 关胜亮 | 新建 |
| 2025-10-14 | 2025-10-14 | 1.0 | 关胜亮 | 按评审记录修改 |

## 2. 引言

### 2.1 术语与缩写名词

1. TOTP：基于时间和共享密钥生成一次性密码的算法
2. UKey：通过 USB 接口与计算机相连的硬件安全设备，内部通常包含安全芯片或智能卡芯片，可以存储用户的私钥、数字证书等敏感信息，并具备密码运算功能

### 2.2 相关文档资料

JIRA [TS-7231](https://jira.taosdata.com:18080/browse/TS-7231)

### 2.3 优先级要求

高

### 2.4 版本要求

1. 企业版支持
2. 社区版仅支持口令信息、用户锁定

## 3. 需求目标

确保每个用户身份唯一，并强制使用强认证措施
1. 强口令策略，长度≥8位，含大小写字母+数字+特殊字符
2. 登录失败锁定，如连续 5 次错误锁定 30 分钟
3. 密码生命周期
4. 多因素认证

## 4. 功能要求

创建和修改用户时，需要为用户指定用户名、口令、资源限制等信息。调整后的语法大致如下
```bash {wrap}
CREATE USER [IF NOT EXISTS] <用户名> <口令信息> [<锁定子句>][<存储加密密钥>][<资源限制>][<密码过期子句>][<允许HOST子句>][<禁止HOST子句>][<允许时间子句>][<禁止时间子句>][<支持TOKEN数量>]
<口令信息> ::= PASS <口令> 
<锁定子句> ::= 
        ACCOUNT LOCK | 
        ACCOUNT UNLOCK
<数据加密密钥> ::= ENCRYPT BY <密钥>
<资源限制> ::= 
        <资源设置项>{,<资源设置项>} | 
        <资源设置项>{ <资源设置项>}
<资源设置项> ::= 
        SESSION_PER_USER <参数设置> |
        CONNECT_IDLE_TIME <参数设置> |
        CONNECT_TIME <参数设置> |
        CALL_PER_SESSION <参数设置> |
        VNODE_PER_CALL <参数设置> |
        SYSINFO <参数设置> |
        CREATEDB <参数设置> |
        FAILED_LOGIN_ATTEMPTS <参数设置> |
        PASSWORD_LIFE_TIME <参数设置> | 
        PASSWORD_REUSE_TIME <参数设置> |
        PASSWORD_REUSE_MAX <参数设置> |
        PASSWORD_LOCK_TIME <参数设置> |
        PASSWORD_GRACE_TIME <参数设置>|
        INACTIVE_ACCOUNT_TIME<参数设置>
<参数设置> ::=
        <参数值>| 
        UNLIMITED| 
        DEFAULT
<密码过期子句> ::= PASSWORD EXPIRE 
<允许HOST子句> ::= 
        HOST NULL |
        HOST <HOST项>{,<HOST项>}
<禁止HOST子句> ::= 
        NOT_ALLOW_HOST |
        NOT_ALLOW_HOST <HOST项>{,<HOST项>}
<HOST项> ::= 
        <具体HOST>|
        <网段>
<允许时间子句> ::= ALLOW_DATETIME <时间项>{,<时间项>}
<禁止时间子句> ::= NOT_ALLOW_DATETIME <时间项>{,<时间项>}
<时间项> ::= 
        <具体时间段> | 
        <规则时间段>
<具体时间段> ::= <具体日期> <具体时间> TO <具体日期> <具体时间>
<规则时间段> ::= <规则时间标志> <具体时间> TO <规则时间标志> <具体时间> 
<规则时间标志> ::= 
        MON | 
        TUE | 
        WED | 
        THURS | 
        FRI | 
        SAT | 
        SUN
<支持 TOKEN 数量> ::= allow_token_num <参数值>
```

### 4.1 口令信息

1. 口令格式要求
   - 口令禁止与用户名相同
   - 口令长度为 8-255 位
   - 密码至少包含大写字母、小写字母、数字、特殊字符中的三类，特殊字符包括 `! @ # $ % ^ & * ( ) - _ + = [ ] { } : ; > < ? | ~ , .`
2. 散列选项：单向加密算法。它把任意长度的输入（如密码）通过散列算法变换成固定长度的输出（散列值）。关键特性是单向性，即从散列值几乎无法反向推算出原始密码。在存储密码时，不应存储明文密码，而是存储其散列值，这样即使数据库泄露，攻击者也无法直接获取用户密码
3. 加盐选项：在密码散列之前，将一个随机生成的字符串（称为“盐”）与密码拼接起来，然后对整个字符串进行散列。盐值需要和散列值一起存储在数据库中。这样做的好处是，即使两个用户密码相同，由于盐值不同，它们的散列值也会不同，能有效抵御彩虹表攻击
4. 3.3.8 版本的 EnableStrongPassword 选项，默认值应该为 1

### 4.2 口令传输

为确保密码传输的安全，需要提供安全的通信机制。如下要求将在 “[传输安全](https://taosdata.feishu.cn/wiki/ACKtwrzpbi4T62kXk79cC3Ixndb)” 中实现
1. 客户端与服务器的通信需使用 TLS 传输
2. 为了进一步防止中间人攻击，需要在 TLS 认证链路上，通过 SASL 机制，传输非明文密码信息

### 4.3 用户锁定

在不删除用户的情况下，对用户进行锁定和解锁
1. 提供 ACCOUNT LOCK（UNLOCK）语法
2. 3.3.8 版本已经支持的 ENABLE 语法仍然保留

### 4.4 数据加密密钥

为用户生成数据加密密钥
1. 当通过“[安全函数](https://taosdata.feishu.cn/wiki/K6yOwulCHiwXPIk0iv0coTCYnsc)”中的 SM4 加解密函数进行数据加解密时，使用该密钥
2. 该密钥不支持更改

### 4.5 资源限制

#### 4.5.1 会话与连接控制

如下选项的生效，将在 “[传输安全](https://taosdata.feishu.cn/wiki/ACKtwrzpbi4T62kXk79cC3Ixndb)” 中实现，例如定期通过心跳获取在 mnode 中设定的各个参数值，然后执行断开连接操作。
1. SESSION_PER_USER：每个用户的最大并发会话数，默认 5，限制单个用户同时建立的数据库连接数量，防止资源独占
2. CONNECT_TIME：单次会话最大持续时间（分钟），默认 480 分钟，超时后自动断开连接，避免长期空闲会话占用资源
3. CONNECT_IDLE_TIME：会话最大空闲时间（分钟），默认 30 分钟，连接无活动超过该时间后自动断开

#### 4.5.2 资源限制

前两个选项的生效，将在 “[传输安全](https://taosdata.feishu.cn/wiki/ACKtwrzpbi4T62kXk79cC3Ixndb)” 中实现，例如定期通过心跳获取在 mnode 中设定的各个参数值，然后执行断开连接操作。
1. CALL_PER_SESSION ：单会话最大并发子调用数量，默认 10
2. VNODE_PER_CALL：单调用最大涉及 vnode 数量，默认 10
3. SYSINFO：是否能够查看系统信息。`1` 表示可以查看，`0` 表示无权查看。
4. CREATEDB：表示该用户是否能够创建数据库。`1` 表示可以创建，`0` 表示无权创建。

#### 4.5.3 密码安全策略

1. FAILED_LOGIN_ATTEMPTS：允许的连续失败登录次数，默认 3，超过次数后账户将被锁定
2. PASSWORD_LOCK_TIME：密码锁定持续时间（天），默认 1，账户因登录失败被锁定后的解锁等待时间
3. PASSWORD_LIFE_TIME：密码有效期（天），默认 90，密码必须更改的周期
4. PASSWORD_GRACE_TIME：密码过期后的宽限期（天），默认 7，密码过期后允许修改的缓冲时间
5. PASSWORD_REUSE_TIME：密码重用时间（天），默认 30，旧密码不能在此期限内重复使用
6. PASSWORD_REUSE_MAX：密码历史记录次数，默认 5，需要多少次密码更改后才能重复使用旧密码
7. INACTIVE_ACCOUNT_TIME：账户不活动锁定时间（天），默认 90，长期未使用的账户自动锁定

### 4.6 密码过期

设置用户密码为过期状态后，用户需修改密码后才能登录使用。

### 4.7 允许和禁止 HOST

1. 扩充已有白名单功能的语法，能够禁止某些 HOST 的连接
2. 禁止某些 HOST 的连接的功能在 “[传输安全](https://taosdata.feishu.cn/wiki/ACKtwrzpbi4T62kXk79cC3Ixndb)” 中实现

### 4.8 允许和禁止时间

参考语法定义实现，用户不能在禁止时间段登录，已经登录的用户需强制下线。

### 4.9 多因素认证

结合多种认证因素，提升安全性。
1. 密码认证：知识因素，用户知道的秘密
2. TOTP 认证：动态口令，一次性有效，防重放攻击
3. 通信安全：认证数据在传输过程中应受到保护，通过安全机密通信保证
有如下要求
1. 服务端增加配置参数 multiFactorLevel，设置认证的级别，可取值 1-3，分别代表密码、TOTP、UKEY 
2. 增加 taos_get_options() 函数，获取服务端的配置参数，其中就有新增的 multiFactorLevel
3. 在 taos_set_options 函数中，可以设置后续 taos_connect、taos_connect_auth 需要提供的信息
   - totp_code：验证码
   - use_ukey：是否使用 UKEY
4. 调整 shell 程序，集成以上两个函数
5. taosExplorer 也需同步调整

#### 4.9.1 TOTP 认证

TOTP（基于时间的一次性密码）的密钥是安全机制的核心。密钥通过系统表 information_schema.ins_totps 管理。在设计时决定是每个用户一条记录，或者全局一条记录，参照如下 SQL 语句
```bash {wrap}
create totp;
update totp;
drop totp;
show totps;
select * from information_schema.ins_totps;
```

TOTP 通常以 Base32 编码，包含至少 16 字节（约 20 个字符），服务器使用安全的随机数生成器​（如`os.urandom`、`std::random_device` 等安全的随机数库）生成。
1. TOTP 的生成和管理，由系统安全管理员负责。
2. 用户可以通过  Google Authenticator 软件录入 clusterId、密钥，之后即可产生验证码
3. 使用该验证码作为登录信息的一部分

### 4.10 TOKEN 认证

为了支持 oAuth 2.0 认证，允许为用户设置多个访问令牌（TOKEN）。TOKEN 拥有的权限和 对应用户 的权限完全相同。

#### 4.10.1 令牌管理

令牌信息存储在系统表 ins_tokens 表中，包括
1. 令牌字符串
2. 令牌对应的用户名
3. 令牌的创建时间
4. 令牌的有效期
5. 令牌申请时的客户端信息，例如授权代码、客户端标志、其他可扩展的字符串

#### 4.10.2 创建、删除、查看令牌的语法

参考如下语法。taosExplorer 可基于此，实现授权代码和令牌关联。
```sql {wrap}
CREATE token 
        from user <username> 
        auth_code <code> 
        client_id <id> 
        expire_time <time>
        other_info <info>; 

SELECT token from ins_tokens from ins_tokens where auth_code = <auth_code>
DROP token <token_id>
ALTER token set expire_time=<time>
ALTER token set enable=1
```

#### 4.10.3 接口调整

修改已有函数 taos_connect_auth 的行为
1. 将 auth 参数修改为 token，不再按照 MD5 方式处理
2. 在不输入 user 参数时，也可登录

## 5. 性能需求

身份鉴别有可能带来一定的性能开销。在测试过程中，对如下指标进行测试，如果性能不达预期，需优化代码。
1. **登录速度**
   - 测试不同认证方式的登录速度
2. **写入和查询性能**
   - 对会话时间等选项的控制会产生额外的条件判断，测试对基本写入查询的影响

## 6. 安全需求

身份认证相关信息的创建、变更权限，将在“权限管理”相关功能中表述

## 7. 兼容性需求

1. 需向后兼容，自 3.3.8 版本升级至本版本时，不需要手工干预
2. 尽可能不改变现有的已存在语法，不影响写入、查询
3. 不能退回到旧版本
