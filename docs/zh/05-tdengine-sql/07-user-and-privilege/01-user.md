---
title: 用户管理
sidebar_label: 用户管理
description: 创建、查看、修改与删除用户，以及 IP 白名单/黑名单、TOTP 与令牌管理
---

用户管理语法在所有版本中可用，但在 TDengine 社区版中仅基础功能实际可用。

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
  [VNODE_PER_CALL {value | DEFAULT | UNLIMITED}]
  [FAILED_LOGIN_ATTEMPTS {value | DEFAULT | UNLIMITED}]
  [PASSWORD_LOCK_TIME {value | DEFAULT | UNLIMITED}]
  [PASSWORD_LIFE_TIME {value | DEFAULT | UNLIMITED}]
  [PASSWORD_GRACE_TIME {value | DEFAULT | UNLIMITED}]
  [PASSWORD_REUSE_TIME {value | DEFAULT}]
  [PASSWORD_REUSE_MAX {value | DEFAULT}]
  [INACTIVE_ACCOUNT_TIME {value | DEFAULT | UNLIMITED}]
  [ALLOW_TOKEN_NUM {value | DEFAULT | UNLIMITED}]
  [SECURITY_LEVEL min_level, max_level]
  [HOST {ip | ip range} [, {ip | ip range}] ...]
  [NOT_ALLOW_HOST {ip | ip range} [, {ip | ip range}] ...]
  [ALLOW_DATETIME {time range} [, {time range}] ...]
  [NOT_ALLOW_DATETIME {time range} [, {time range}] ...]
```

用户名最长不超过 23 个字节。

密码长度必须为 8 到 255，且至少包含大写字母、小写字母、数字、特殊字符中的三类。特殊字符包括 `! @ # $ % ^ & * ( ) - _ + = [ ] { } : ; > < ? | ~ , .`。可在 `taos.cfg` 中设置 `enableStrongPassword 0` 关闭此强制要求，或通过如下 SQL 关闭：

```sql
ALTER ALL DNODES 'EnableStrongPassword' '0';
```

`FAILED_LOGIN_ATTEMPTS` 等选项的默认值与配置参数 `enableAdvancedSecurity` 相关，见下文。可通过如下 SQL 设置其状态：

```sql
-- 默认关闭高级安全功能
ALTER ALL DNODES 'EnableAdvancedSecurity' '0';
-- 默认打开高级安全功能
ALTER ALL DNODES 'EnableAdvancedSecurity' '1';
```

- `SYSINFO`：该用户是否可查看系统信息。`1` 表示可以，`0` 表示无权。系统信息包括服务配置、dnode、vnode、存储等。缺省值为 `1`。
- `ENABLE`：是否启用该用户。`1` 表示启用，`0` 表示未启用；未启用的用户不能登录。缺省值为 `1`。
- `CREATEDB`：是否可创建数据库。`1` 表示可以，`0` 表示无权。缺省值为 `0`。企业版自 `v3.3.2.0` 起支持。
- `CHANGEPASS`：用户是否能够或必须修改密码。`2` 表示可以修改，`1` 表示必须修改，`0` 表示不能修改。缺省值为 `2`。企业版自 `v3.4.0.0` 起支持。
- `SESSION_PER_USER`：限制用户同时建立的数据库连接数量。`enableAdvancedSecurity` 打开时默认 `32`，否则默认 `-1`（`UNLIMITED`）。最小 `1`；设为 `-1` 或 `UNLIMITED` 则不限制。企业版自 `v3.4.0.0` 起支持。
- `CONNECT_TIME`：单次会话最大持续时间，单位为分钟。`enableAdvancedSecurity` 打开时默认 `480`，否则默认 `-1`（`UNLIMITED`）。最小 `1`；设为 `-1` 或 `UNLIMITED` 则不限制。企业版自 `v3.4.0.0` 起支持。
- `CONNECT_IDLE_TIME`：允许的会话最大空闲时间，单位为分钟。`enableAdvancedSecurity` 打开时默认 `30`，否则默认 `-1`（`UNLIMITED`）。最小 `1`；设为 `-1` 或 `UNLIMITED` 则不限制。企业版自 `v3.4.0.0` 起支持。
- `CALL_PER_SESSION`：单会话最大并发子调用数量。`enableAdvancedSecurity` 打开时默认 `128`，否则默认 `-1`（`UNLIMITED`）。最小 `1`；设为 `-1` 或 `UNLIMITED` 则不限制。企业版自 `v3.4.0.0` 起支持。
- `VNODE_PER_CALL`：单次调用可涉及的最大 vnode 数量。默认 `-1`（无限制）。企业版自 `v3.4.0.0` 起支持。
- `FAILED_LOGIN_ATTEMPTS`：允许的连续失败登录次数，超过后账户将被锁定。`enableAdvancedSecurity` 打开时默认 `3`，否则默认 `UNLIMITED`。最小 `1`；设为 `UNLIMITED` 则不限制。企业版自 `v3.4.0.0` 起支持。
- `PASSWORD_LOCK_TIME`：因登录失败被锁定后的解锁等待时间，单位为分钟。`enableAdvancedSecurity` 打开时默认 `1440`，否则默认 `1`。最小 `1`；设为 `UNLIMITED` 则永久锁定。企业版自 `v3.4.0.0` 起支持。
- `PASSWORD_LIFE_TIME`：密码有效期，单位为天。`enableAdvancedSecurity` 打开时默认 `90`，否则默认 `UNLIMITED`。最小 `1`；设为 `UNLIMITED` 则永不过期。企业版自 `v3.4.0.0` 起支持。
- `PASSWORD_GRACE_TIME`：密码过期后的宽限期（单位天）。宽限期内禁止执行除修改密码以外的操作；宽限期内未改密则锁定账户。`enableAdvancedSecurity` 打开时默认 `7`，否则默认 `UNLIMITED`。最小 `0`；设为 `UNLIMITED` 则永不因宽限期锁定。企业版自 `v3.4.0.0` 起支持。
- `PASSWORD_REUSE_TIME`：密码重用时间，旧密码失效后不能在此期限内重复使用，单位为天。`enableAdvancedSecurity` 打开时默认 `30`，否则默认 `0`。最小 `0`，最大 `365`。新密码需同时满足 `PASSWORD_REUSE_TIME` 与 `PASSWORD_REUSE_MAX`。企业版自 `v3.4.0.0` 起支持。
- `PASSWORD_REUSE_MAX`：密码历史记录次数，需经过多少次更改后才能重复使用旧密码。`enableAdvancedSecurity` 打开时默认 `5`，否则默认 `0`。最小 `0`，最大 `100`。新密码需同时满足 `PASSWORD_REUSE_TIME` 与 `PASSWORD_REUSE_MAX`。企业版自 `v3.4.0.0` 起支持。
- `INACTIVE_ACCOUNT_TIME`：账户不活动锁定时间，长期未使用的账户自动锁定，单位为天。`enableAdvancedSecurity` 打开时默认 `90`，否则默认 `UNLIMITED`。最小 `1`；设为 `UNLIMITED` 则永不锁定。企业版自 `v3.4.0.0` 起支持。
- `ALLOW_TOKEN_NUM`：支持的令牌个数。默认 `3`，最小 `0`；设为 `UNLIMITED` 则不限制。企业版自 `v3.4.0.0` 起支持。
- `SECURITY_LEVEL`：用户安全等级范围（`min_level`, `max_level`），用于强制访问控制（MAC）。详见 [强制访问控制（MAC）](./02-grant.md#强制访问控制mac)。企业版支持。
- `HOST` / `NOT_ALLOW_HOST`：IP 地址白名单与黑名单。可为单个 IP（如 `192.168.1.1`），或 [CIDR](https://www.rfc-editor.org/rfc/rfc4632) 地址段（如 `192.168.1.1/24`）。企业版自 `v3.4.0.0` 起支持。须将 `enableWhiteList` 设为 `1` 后才会生效（参数说明见 [taosd](../../12-operations-and-tooling/03-components/01-taosd.md)）。组合语义、增删查示例与注意事项见下文 [IP 白名单与黑名单](#ip-白名单与黑名单)。
- `ALLOW_DATETIME` / `NOT_ALLOW_DATETIME`：允许与不允许登录的时间范围（以服务端时区为准），包含日期、起始时间（精确到分钟）、时长（分钟）三部分。日期可为具体日期，或 `MON`、`TUE`、`WED`、`THU`、`FRI`、`SAT`、`SUN`；例如 `2025-12-25 08:00 120`、`TUE 08:00 120`。企业版自 `v3.4.0.0` 起支持。
  - 若两者均未设置，允许在任何时间登录。
  - 若只设置 `ALLOW_DATETIME`，仅该时间段允许登录。
  - 若只设置 `NOT_ALLOW_DATETIME`，该时间段不允许登录，其它时间允许。
  - 若同时设置，则只能在属于 `ALLOW_DATETIME` 且不属于 `NOT_ALLOW_DATETIME` 的时间段内登录。

以下示例创建一个密码为 `abc123!@#`、可查看系统信息的用户：

```sql
taos> CREATE USER test PASS 'abc123!@#' SYSINFO 1;
Query OK, 0 of 0 rows affected (0.001254s)
```

## 查看用户

```sql
SHOW USERS;
```

**示例**

```sql
taos> SHOW USERS;
 name | super | enable | sysinfo | createdb |       create_time       | totp |      allowed_host       | allowed_datetime |
===========================================================================================================================
 test |     0 |      1 |       1 |        0 | 2025-12-24 18:56:20.709 |    0 | +127.0.0.1/32, +::1/128 | +ALL             |
 root |     1 |      1 |       1 |        1 | 2025-12-24 18:00:43.197 |    0 | +127.0.0.1/32, +::1/128 | +ALL             |
Query OK, 2 rows in set (0.001657s)
```

在 `allowed_host` 中，地址或地址段前缀为 `+` 表示白名单（允许登录），前缀为 `-` 表示黑名单（不允许登录）。`allowed_datetime` 同理。

也可查询内置系统表 `information_schema.ins_users`：

```sql
taos> SELECT * FROM information_schema.ins_users;
 name | super | enable | sysinfo | createdb |       create_time       | totp |      allowed_host       | allowed_datetime |
===========================================================================================================================
 test |     0 |      1 |       1 |        0 | 2025-12-24 18:56:20.709 |    0 | +127.0.0.1/32, +::1/128 | +ALL             |
 root |     1 |      1 |       1 |        1 | 2025-12-24 18:00:43.197 |    0 | +127.0.0.1/32, +::1/128 | +ALL             |
Query OK, 2 row(s) in set (0.007383s)
```

## 删除用户

```sql
DROP USER [IF EXISTS] user_name;
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
  [VNODE_PER_CALL {value | DEFAULT | UNLIMITED}]
  [FAILED_LOGIN_ATTEMPTS {value | DEFAULT | UNLIMITED}]
  [PASSWORD_LOCK_TIME {value | DEFAULT | UNLIMITED}]
  [PASSWORD_LIFE_TIME {value | DEFAULT | UNLIMITED}]
  [PASSWORD_GRACE_TIME {value | DEFAULT | UNLIMITED}]
  [PASSWORD_REUSE_TIME {value | DEFAULT}]
  [PASSWORD_REUSE_MAX {value | DEFAULT}]
  [INACTIVE_ACCOUNT_TIME {value | DEFAULT | UNLIMITED}]
  [ALLOW_TOKEN_NUM {value | DEFAULT | UNLIMITED}]
  [SECURITY_LEVEL min_level, max_level]
  [ADD HOST {ip | ip range} [, {ip | ip range}] ...]
  [DROP HOST {ip | ip range} [, {ip | ip range}] ...]
  [ADD NOT_ALLOW_HOST {ip | ip range} [, {ip | ip range}] ...]
  [DROP NOT_ALLOW_HOST {ip | ip range} [, {ip | ip range}] ...]
  [ADD ALLOW_DATETIME {time range} [, {time range}] ...]
  [DROP ALLOW_DATETIME {time range} [, {time range}] ...]
  [ADD NOT_ALLOW_DATETIME {time range} [, {time range}] ...]
  [DROP NOT_ALLOW_DATETIME {time range} [, {time range}] ...]
}
```

以下示例禁用名为 `test` 的用户：

```sql
taos> ALTER USER test ENABLE 0;
Query OK, 0 of 0 rows affected (0.001160s)
```

修改用户密码（`ALTER USER ... PASS`）后，服务端会在心跳中检测并踢除使用**旧密码**建立的连接；被踢除的连接上后续请求将返回认证失败（`0x80000357`）。执行密码变更操作的连接本身不受影响。**Token 连接不受此机制影响。**

:::note
自企业版 `v3.4.2.1` 起，`ALTER USER ... SYSINFO {0|1}` 会联动修改用户的 `SYSINFO_0` / `SYSINFO_1` 角色；授予高阶系统角色也会联动提升 `SYSINFO` 属性。详见 [权限管理](./02-grant.md#sysinfo-属性与角色的联动)。
:::

## IP 白名单与黑名单

IP 白名单/黑名单限制用户可从哪些地址登录，与 `GRANT` 权限相互独立、分开管理。企业版自 `v3.2.0.0` 起提供白名单能力；`HOST` / `NOT_ALLOW_HOST` 语法自 `v3.4.0.0` 起支持。社区版可执行增删查，但不会对来源 IP 做限制。须将系统配置 `enableWhiteList` 设为 `1` 后黑白名单才会生效（参数说明见 [taosd](../../12-operations-and-tooling/03-components/01-taosd.md)）。

登录判定规则：

- 若既未设置 `HOST` 也未设置 `NOT_ALLOW_HOST`，则允许用户在任何地址登录。**注意**：为保证安全和便于使用，创建用户时若设置了 `HOST`，或两者均未设置，系统会自动将 `127.0.0.1` 和 `::1` 加入 `HOST`。因此上述“任何地址”情形，需通过 `ALTER USER` 删除全部 `HOST` 与 `NOT_ALLOW_HOST` 后才会出现。
- 若只设置 `HOST`，则仅允许从该地址或地址段登录。
- 若只设置 `NOT_ALLOW_HOST`，则不允许从该地址或地址段登录，其它地址允许。
- 若同时设置二者，则只能从属于 `HOST` 且不属于 `NOT_ALLOW_HOST` 的地址登录。

增加 IP 白名单：

```sql
CREATE USER test PASS 'taosdata1' HOST '192.168.1.0/24', '10.0.0.1';
ALTER USER test ADD HOST '192.168.2.0/24';
```

增加 IP 黑名单：

```sql
ALTER USER test ADD NOT_ALLOW_HOST '203.0.113.5/32';
```

查询：

```sql
SELECT name, allowed_host FROM information_schema.ins_users;
SHOW USERS;
```

在 `allowed_host` 中，地址或地址段前缀为 `+` 表示白名单（允许登录），前缀为 `-` 表示黑名单（不允许登录）。

删除：

```sql
ALTER USER test DROP HOST '192.168.2.0/24';
ALTER USER test DROP NOT_ALLOW_HOST '203.0.113.5/32';
```

说明：

- 开源版和企业版都能添加成功，且可以查询到，但是开源版不会对 IP 做任何限制。
- 一次可以添加多个 IP range，服务端会做去重，去重的逻辑是需要 IP range 完全一样。例如：`CREATE USER u_write PASS 'taosdata1' HOST 'iprange1','iprange2'`。
- 默认会把 `127.0.0.1` 添加到白名单列表，且在白名单列表可以查询（用户手册所述场景下亦可能包含 `::1`）。
- 集群的节点 IP 集合会自动添加到白名单列表，但是查询不到。
- `taosAdapter` 和 `taosd` 不在一个机器的时候，需要把 `taosAdapter` 的 IP 手动添加到 `taosd` 白名单列表中。
- 集群情况下，各个节点的 `enableWhiteList` 须一致，或者全为 `false`，或者全为 `true`，要不然集群无法启动。
- 白名单变更生效时间约 1s，不超过 2s。每次变更对收发性能有些微影响（多一次判断，可以忽略），变更完之后影响忽略不计；变更过程中对集群没有影响，对正在访问且 IP 已包含在白名单内的客户端也没有影响。
- 如果添加两个 IP range，例如 `192.168.1.1/16`（假设为 A）与 `192.168.1.1/24`（假设为 B），严格来说 A 包含了 B，但考虑情况太复杂，并不会对 A 和 B 做合并。
- 要删除的时候，必须严格匹配。也就是如果添加的是 `192.168.1.1/24`，要删除也是 `192.168.1.1/24`。
- 只有 `root` 才有权限对其他用户增删 IP 白名单。
- 兼容之前的版本，但是不支持从当前版本回退到之前版本。
- `x.x.x.x/32` 和 `x.x.x.x` 属于同一个 IP range，显示为 `x.x.x.x`。
- 如果客户端拿到的是 `0.0.0.0/0`，说明没有开启白名单。
- 如果白名单发生了改变，客户端会在 heartbeat 里检测到。
- 针对一个 user，添加的 IP 个数上限是 2048。

## TOTP 双因素认证

TOTP 双因素认证为企业版功能，自企业版 `v3.4.0.1` 起支持。

### 创建 / 更新 TOTP 密钥

```sql
CREATE TOTP_SECRET FOR USER user_name;
```

若用户尚未创建 TOTP 密钥，本命令为其创建；若已创建，则更新密钥。无论哪种情况，都会返回新密钥，且仅展示一次，请及时保存。系统会为已创建 TOTP 密钥的用户自动启用 TOTP 双因素认证。

启用后，TDengine 要求 TOTP 验证码长度为 6 位，且每 30 秒更新一次；请按此参数配置验证码生成器，否则客户端可能无法登录。

为用户 `test` 创建 TOTP 密钥的示例：

```sql
taos> CREATE TOTP_SECRET FOR USER test;
                     totp_secret                      |
=======================================================
 ERIRPLZL4ZBFTPT5BNXMVFPR4Z3PTHUWTBTCNZPOHYPYQGTD25XA |
Query OK, 1 row(s) in set (0.002314s)
```

### 删除 TOTP 密钥

```sql
DROP TOTP_SECRET FROM USER user_name;
```

删除用户的 TOTP 密钥后，该用户的 TOTP 双因素认证将被禁用。

示例：

```sql
taos> DROP TOTP_SECRET FROM USER test;
Drop OK, 0 row(s) affected (0.002295s)
```

## 令牌管理

令牌管理为企业版功能，自企业版 `v3.4.0.0` 起支持。

### 创建令牌

```sql
CREATE TOKEN [IF NOT EXISTS] token_name FROM USER user_name [ENABLE {1|0}] [TTL value] [PROVIDER value] [EXTRA_INFO value];
```

令牌名称最长 31 个字节。

- `ENABLE`：是否启用该令牌。`1` 表示启用，`0` 表示未启用；未启用的令牌不能用于登录。缺省值为 `1`。
- `TTL`：令牌有效时长，单位为天，从创建时起算。默认 `0`，表示永久有效。
- `PROVIDER`：令牌提供者名称，最长 63 个字节。
- `EXTRA_INFO`：由应用管理的附加信息，最长 1023 字节。

以下为用户 `test` 创建名为 `test_token` 的令牌。注意：令牌值仅在创建时展示一次，后续无法查询，请及时保存。

```sql
taos> CREATE TOKEN test_token FROM USER test;
                             token                               |
==================================================================
 BsyjYKxhCMntZ3pHgweCd2uV2C8HoGKn8Mvd49dRRCtzusX0P1mgqRMrG7SzUca |
Query OK, 1 row(s) in set (0.003018s)
```

### 查看令牌

可根据权限查看系统中的令牌；权限不足时可能只能看到自己的令牌：

```sql
SHOW TOKENS;
```

**示例**

```sql
taos> SHOW TOKENS;
    name    | user | provider | enable |       create_time       |       expire_time       | extra_info |
=========================================================================================================
 root_token | root |          |      1 | 2025-12-25 10:02:28.000 | 1970-01-01 08:00:00.000 |            |
 test_token | test |          |      1 | 2025-12-25 10:01:47.000 | 1970-01-01 08:00:00.000 |            |
Query OK, 2 row(s) in set (0.003313s)
```

也可查询 `information_schema.ins_tokens`：

```sql
taos> SELECT * FROM information_schema.ins_tokens;
    name    | user | provider | enable |       create_time       |       expire_time       | extra_info |
=========================================================================================================
 root_token | root |          |      1 | 2025-12-25 10:02:28.000 | 1970-01-01 08:00:00.000 |            |
 test_token | test |          |      1 | 2025-12-25 10:01:47.000 | 1970-01-01 08:00:00.000 |            |
Query OK, 2 row(s) in set (0.007438s)
```

### 修改令牌

```sql
ALTER TOKEN token_name [ENABLE {1|0}] [TTL value] [PROVIDER value] [EXTRA_INFO value];
```

修改 `TTL` 时，新的有效时长从修改时刻起算。

### 删除令牌

```sql
DROP TOKEN [IF EXISTS] token_name;
```

删除用户时，其令牌会一并级联删除。
