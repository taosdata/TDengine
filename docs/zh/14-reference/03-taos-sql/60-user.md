---
title: 用户管理
sidebar_label: 用户管理
description: 本节讲述基本的用户管理功能
---

用户管理语法在所有版本中可用，但在 TDengine TSDB 社区版中仅基础功能实际可用，使用高级功能需要 TDengine TSDB 企业版。要想全面了解和使用的用户管理功能，请联系 TDengine TSDB 销售团队。

## 创建用户

```sql
CREATE USER user_name PASS 'password'
  [SYSINFO {1|0}]
  [CREATEDB {1|0}]
  [ENABLE {1|0}]
  [CHANGEPASS {2|1|0}]
  [SESSION_PER_USER {value | DEFAULT | UNLIMITED}]
  [CONNECT_TIME {value | DEFAULT | UNLIMITED}]
  [CONNECT_IDLE_TIME {value | DEFAULT | UNLIMITED}]
  [CALL_PER_SESSION {value | DEFAULT | UNLIMITED}]
  [FAILED_LOGIN_ATTEMPTS {value | DEFAULT | UNLIMITED}]
  [PASSWORD_LOCK_TIME {value | DEFAULT | UNLIMITED}]
  [PASSWORD_LIFE_TIME {value | DEFAULT | UNLIMITED}]
  [PASSWORD_GRACE_TIME {value | DEFAULT | UNLIMITED}]
  [PASSWORD_REUSE_TIME {value | DEFAULT}]
  [PASSWORD_REUSE_MAX {value | DEFAULT}]
  [INACTIVE_ACCOUNT_TIME {value | DEFAULT | UNLIMITED}]
  [ALLOW_TOKEN_NUM {value | DEFAULT | UNLIMITED}]
  [HOST {ip | ip range}]
  [NOT_ALLOW_HOST {ip | ip range}]
  [ALLOW_DATETIME {time range}]
  [NOT_ALLOW_DATETIME {time range}]
```

用户名最长不超过 23 个字节。

密码长度必须为 8 到 255 位，并且至少包含大写字母、小写字母、数字、特殊字符中的三类。特殊字符包括 `! @ # $ % ^ & * ( ) - _ + = [ ] { } : ; > < ? | ~ , .`。可以通过在 taos.cfg 中添加参数 `enableStrongPassword 0` 关闭此强制要求，或者通过如下 SQL 关闭。

```sql
alter all dnodes 'EnableStrongPassword' '0'
```

- `SYSINFO` 表示该用户是否能够查看系统信息。`1` 表示可以查看，`0` 表示无权查看。系统信息包括服务配置、dnode、vnode、存储等信息。缺省值为 `1`。
- `ENABLE` 表示是否启用该用户。`1` 表示启用，`0` 表示未启用，未启用的用户不能登录系统。缺省值为 `1`。
- `CREATEDB` 表示该用户是否能够创建数据库。`1` 表示可以创建，`0` 表示无权创建。缺省值为 `0`。从企业版 v3.3.2.0 开始支持。
- `CHANGEPASS` 表示用户是否能够或必须修改密码。`2` 表示可以修改，`1`表示必须修改，`0`表示不能修改。缺省值为`2`。从企业版 v3.4.0.0 开始支持。
- `SESSION_PER_USER` 限制用户同时建立的数据库连接数量，默认 32，最小 1，设置为 UNLIMITED 则不限制。从企业版 v3.4.0.0 开始支持。
- `CONNECT_TIME` 限制单次会话最大持续时间，单位为分钟，默认 480，最小 1，设置为 UNLIMITED 则不限制。从企业版 v3.4.0.0 开始支持。
- `CONNECT_IDLE_TIME` 允许的会话最大空闲时间，单位为分钟，默认 30，最小 1，设置为 UNLIMITED 则不限制。从企业版 v3.4.0.0 开始支持。
- `CALL_PER_SESSION` 单会话最大并发子调用数量，默认 10，最小 1，设置为 UNLIMITED 则不限制。从企业版 v3.4.0.0 开始支持。
- `FAILED_LOGIN_ATTEMPTS` 允许的连续失败登录次数，超过次数后账户将被锁定，默认 3，最小 1，设置为 UNLIMITED 则不限制。从企业版 v3.4.0.0 开始支持。
- `PASSWORD_LOCK_TIME` 账户因登录失败被锁定后的解锁等待时间，单位分钟，默认 1440，最小 1，设置为 UNLIMITED 则永久锁定。从企业版 v3.4.0.0 开始支持。
- `PASSWORD_LIFE_TIME` 密码有效期，单位天，默认 90，最小 1，设置为 UNLIMITED 则永不过期。从企业版 v3.4.0.0 开始支持。 
- `PASSWORD_GRACE_TIME` 密码过期后的宽限期，密码过期后允许修改的缓冲时间，宽限期内禁止执行除修改密码以外的其他操作，宽限期内如未修改密码则锁定账户，单位天，默认 7，最小 0，设置为 UNLIMITED 则永不锁定。从企业版 v3.4.0.0 开始支持。
- `PASSWORD_REUSE_TIME` 密码重用时间，旧密码失效后不能在此期限内重复使用，单位天，默认 30，最小 0，最大 365。从企业版 v3.4.0.0 开始支持。
- `PASSWORD_REUSE_MAX` 密码历史记录次数，需要多少次密码更改后才能重复使用旧密码。默认 5，最小 0，最大 100。新密码需同时满足 PASSWORD_REUSE_TIME 和 PASSWORD_REUSE_MAX 两项限制。从企业版 v3.4.0.0 开始支持。
- `INACTIVE_ACCOUNT_TIME` 账户不活动锁定时间，长期未使用的账户自动锁定，单位天，默认 90，最小 1，设置为 UNLIMITED 则永不锁定。从企业版 v3.4.0.0 开始支持。
- `ALLOW_TOKEN_NUM` 支持的令牌个数，默认 3，最小 0，设置为 UNLIMITED 则不限制。从企业版 v3.4.0.0 开始支持。
- `HOST` 和 `NOT_ALLOW_HOST` IP 地址白名单和黑名单，可以是单个 IP 地址，如 `192.168.1.1`，也可以是一个地址段，如 `192.168.1.1/24`。当黑白名单同时存在时，只允许在白名单中且不在黑名单中的地址访问。从企业版 v3.4.0.0 开始支持。
- `ALLOW_DATETIME` 和 `NOT_ALLOW_DATETIME` 允许和不允许登录的时间范围，包括日期、起始时间（精确到分钟）、时长（以分钟为单位）三部分，其中日期可以是具体的日期，也可以是 MON、TUE、WED、THU、FRI、SAT、SUN 代表的日期，例如：`2025-12-25 08:00 120`、`TUE 08:00 120`。从企业版 v3.4.0.0 开始支持。

在下面的示例中，我们创建一个密码为 `abc123!@#` 且可以查看系统信息的用户。

```sql
taos> create user test pass 'abc123!@#' sysinfo 1;
Query OK, 0 of 0 rows affected (0.001254s)
```

## 查看用户

可以使用如下命令查看系统中的用户。

```sql
SHOW USERS;
```

以下是示例：

```sql
taos> show users;
       name        | super | enable | sysinfo | createdb |       create_time       | totp |      allowed_host       |   allowed_datetime   |
============================================================================================================================================
 test              |     0 |      1 |       1 |        0 | 2025-12-24 18:56:20.709 |    0 | +127.0.0.1/32, +::1/128 | +ALL                 |
 root              |     1 |      1 |       1 |        1 | 2025-12-24 18:00:43.197 |    0 | +127.0.0.1/32, +::1/128 | +ALL                 |
Query OK, 2 rows in set (0.001657s)
```

或者，可以查询内置系统表 INFORMATION_SCHEMA.INS_USERS 来获取用户信息。

```sql
taos> select * from information_schema.ins_users;
       name        | super | enable | sysinfo | createdb |       create_time       | totp |      allowed_host       |   allowed_datetime   |
============================================================================================================================================
 test              |     0 |      1 |       1 |        0 | 2025-12-24 18:56:20.709 |    0 | +127.0.0.1/32, +::1/128 | +ALL                 |
 root              |     1 |      1 |       1 |        1 | 2025-12-24 18:00:43.197 |    0 | +127.0.0.1/32, +::1/128 | +ALL                 |
Query OK, 2 row(s) in set (0.007383s)
```

## 删除用户

```sql
DROP USER user_name;
```

## 修改用户配置

```sql
ALTER USER user_name alter_user_clause
 
alter_user_clause: {
  [PASS 'password']
  [SYSINFO {1|0}]
  [CREATEDB {1|0}]
  [ENABLE {1|0}]
  [CHANGEPASS {2|1|0}]
  [SESSION_PER_USER {value | DEFAULT | UNLIMITED}]
  [CONNECT_TIME {value | DEFAULT | UNLIMITED}]
  [CONNECT_IDLE_TIME {value | DEFAULT | UNLIMITED}]
  [CALL_PER_SESSION {value | DEFAULT | UNLIMITED}]
  [FAILED_LOGIN_ATTEMPTS {value | DEFAULT | UNLIMITED}]
  [PASSWORD_LOCK_TIME {value | DEFAULT | UNLIMITED}]
  [PASSWORD_LIFE_TIME {value | DEFAULT | UNLIMITED}]
  [PASSWORD_GRACE_TIME {value | DEFAULT | UNLIMITED}]
  [PASSWORD_REUSE_TIME {value | DEFAULT}]
  [PASSWORD_REUSE_MAX {value | DEFAULT}]
  [INACTIVE_ACCOUNT_TIME {value | DEFAULT | UNLIMITED}]
  [ALLOW_TOKEN_NUM {value | DEFAULT | UNLIMITED}]
  [ADD HOST {ip | ip range}]
  [DROP HOST {ip | ip range}]
  [ADD NOT_ALLOW_HOST {ip | ip range}]
  [DROP NOT_ALLOW_HOST {ip | ip range}]
  [ADD ALLOW_DATETIME {time range}]
  [DROP ALLOW_DATETIME {time range}]
  [ADD NOT_ALLOW_DATETIME {time range}]
  [DROP NOT_ALLOW_DATETIME {time range}]
}
```

下面的示例禁用了名为 `test` 的用户。

```sql
taos> alter user test enable 0;
Query OK, 0 of 0 rows affected (0.001160s)
```

## 令牌管理

令牌管理是 TDengine TSDB 企业版功能，从企业版 v3.4.0.0 开始支持。

### 创建令牌

```sql
CREATE TOKEN token_name FROM USER user_name [ENABLE {1|0}] [TTL value] [PROVIDER value] [EXTRA_INFO value]
```

令牌名称最长 31 个字节。

- `ENABLE` 表示是否启用该令牌。`1` 表示启用，`0` 表示未启用，未启用的令牌不能用于登录系统。缺省值为 `1`。
- `TTL` 令牌的有效时长，以天为单位，从创建时起算，默认 `0`，表示永远有效。
- `PROVIDER` 令牌提供者的名称，最长 63 个字节。
- `EXTRA_INFO` 由应用管理的附加信息，最长 1023 字节。

在下面的示例中，我们为用户 test 创建了一个名为 `test_token` 的令牌。注意，由于令牌值比较长，且仅在创建时展示一次，后续无法查询，所以请在 SQL 命令的最后使用 `\G` 以便完整显示。

```sql
taos> create token test_token from user test \G;
*************************** 1.row ***************************
token: BsyjYKxhCMntZ3pHgweCd2uV2C8HoGKn8Mvd49dRRCtzusX0P1mgqRMrG7SzUca
Query OK, 1 row(s) in set (0.003018s)
```

### 查看令牌

可以使用如下命令查看已经创建的令牌，但普通用户仅能查看自己的令牌。

```sql
SHOW TOKENS;
```

以下是示例：

```sql
taos> show tokens;
    name    | user | provider | enable |       create_time       |       expire_time       | extra_info |
=========================================================================================================
 root_token | root |          |      1 | 2025-12-25 10:02:28.000 | 1970-01-01 08:00:00.000 |            |
 test_token | test |          |      1 | 2025-12-25 10:01:47.000 | 1970-01-01 08:00:00.000 |            |
Query OK, 2 row(s) in set (0.003313s)
```

或者，可以查询内置系统表 INFORMATION_SCHEMA.INS_TOKENS 来获取令牌信息。

```sql
taos> select * from information_schema.ins_tokens;
    name    | user | provider | enable |       create_time       |       expire_time       | extra_info |
=========================================================================================================
 root_token | root |          |      1 | 2025-12-25 10:02:28.000 | 1970-01-01 08:00:00.000 |            |
 test_token | test |          |      1 | 2025-12-25 10:01:47.000 | 1970-01-01 08:00:00.000 |            |
Query OK, 2 row(s) in set (0.007438s)
```

### 修改令牌

```sql
ALTER TOKEN token_name [ENABLE {1|0}] [TTL value] [PROVIDER value] [EXTRA_INFO value]
```

当修改令牌的有效时长（TTL）时，新的有效时长从修改时起算。

### 删除令牌

```sql
DROP TOKEN token_name;
```

另外，删除用户时，其令牌会被同时级联删除。