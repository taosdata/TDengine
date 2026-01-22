---
title: Users
slug: /tdengine-reference/sql-manual/manage-users
---

The user management syntax is available across all versions, but in the TDengine TSDB Community Edition, only basic features are functionally accessible. Advanced functionalities require the TDengine TSDB Enterprise Edition. To learn about and obtain comprehensive user management features, please contact the TDengine sales team.

## Create User

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
  [HOST {ip | ip range} [, {ip | ip range}] ...]
  [NOT_ALLOW_HOST {ip | ip range} [, {ip | ip range}] ...]
  [ALLOW_DATETIME {time range} [, {time range}] ...]
  [NOT_ALLOW_DATETIME {time range} [, {time range}] ...]
```

The username can be up to 23 bytes long.

The password must be between 8 and 255 characters long and include at least three types of characters from the following: uppercase letters, lowercase letters, numbers, and special characters. Special characters include `! @ # $ % ^ & * ( ) - _ + = [ ] { } : ; > < ? | ~ , .`, and this requirement is able to be closed by adding enableStrongPassword 0 in taos.cfg, or by the following SQL:

```sql
alter all dnodes 'EnableStrongPassword' '0'
```

- `SYSINFO` indicates whether the user can view system information. `1` means they can view, `0` means they have no permission to view. System information includes service configuration, dnode, vnode, storage, etc. The default value is `1`.
- `CREATEDB` indicates whether the user can create databases. `1` means they can create databases, `0` means they have no permission to create databases. The default value is `0`. // Supported starting from TDengine Enterprise version 3.3.2.0
- `ENABLE` indicates whether the user is enabled, `1` means enabled, `0` means disabled. A disabled user cannot connect to the database. The default value is `1`.
- `CHANGEPASS` indicate whether the use can or must change password, `2` means can change password, `1` means must change password, `0` means cannot change password. The default value is `2`. Support in Enterprise Edition v3.4.0.0 and above.
- `SESSION_PER_USER` The maximum allowed simulaneous connections of the user. The default value is `32`, with a minimal of `1`, set to `UNLIMITED` disables the restriction. Support in Enterprise Edition v3.4.0.0 and above.
- `CONNECT_TIME` The maximum allowed duration for a single session in minutes. The default value is `480`, with a minimum of `1`, set to `UNLIMITED` disables the restriction. Support in Enterprise Edition v3.4.0.0 and above.
- `CONNECT_IDLE_TIME` The maximum allowed idle duration for a single session in minutes. The default value is `30`, with a minimum of `1`, set to `UNLIMITED` disables the restriction. Support in Enterprise Edition v3.4.0.0 and above.
- `CALL_PER_SESSION` The maximum allowed number of sub-calls per session. The default value is `10`, with a minimum of `1`, set to `UNLIMITED` disables the restriction. Support in Enterprise Edition v3.4.0.0 and above.
- `VNODE_PER_CALL` The maximum number of vnodes that a single call can involve. The default value is `-1`, which means unlimited. Support in Enterprise Edition v3.4.0.0 and above.
- `FAILED_LOGIN_ATTEMPTS` The number of allowed consecutive failed login attempts; the user will be locked after exceeding this limit. The default value is `3`, with a minimum of `1`, set to `UNLIMITED` disables the restriction. Support in Enterprise Edition v3.4.0.0 and above.
- `PASSWORD_LOCK_TIME` The unlock waiting time for the user when locked due to failed login attempts, in minutes. The default value is `1440`, with a minimum of `1`, set to `UNLIMITED` means the user is locked for ever. Support in Enterprise Edition v3.4.0.0 and above.
- `PASSWORD_LIFE_TIME` Password validity period, in days. The default value is `90`, with a minimum of `1`, set to `UNLIMITED` means never expire. Support in Enterprise Edition v3.4.0.0 and above.
- `PASSWORD_GRACE_TIME` The grace period after password expiration, in days. This is a buffer time allowing password changes, during this period, all operations other than password modification are prohibited. If the password is not changed within the grace period, the user will be locked. The default value is `7`, with a minimum of `0`, set to `UNLIMITED` means never lock the user. Support in Enterprise Edition v3.4.0.0 and above.
- `PASSWORD_REUSE_TIME` The duration during which an old password cannot be reused,  in days.  A new password must comply with both the `PASSWORD_REUSE_TIME` and `PASSWORD_REUSE_MAX` restrictions. The default value is `30`, with a maximum of `365` and a minimum of `0`. Support in Enterprise Edition v3.4.0.0 and above.
- `PASSWORD_REUSE_MAX` The number of password changes required before an old password can be reused. A new password must comply with both the `PASSWORD_REUSE_TIME` and `PASSWORD_REUSE_MAX` restrictions. The default value is `5`, with a maximum of `100` and a minimum of `0`. Support in Enterprise Edition v3.4.0.0 and above.
- `INACTIVE_ACCOUNT_TIME` User inactivity lockout period, in days. The default value is `90`, with a minimum of `1`, set to `UNLIMITED` means never lockout the user. Support in Enterprise Edition v3.4.0.0 and above.
- `ALLOW_TOKEN_NUM` The maximum allowed number of tokens. The default value is `3`, with a minimum of `0`, set to `UNLIMITED` disables this restriction. Support in Enterprise Edition v3.4.0.0 and above.
- `HOST` and `NOT_ALLOW_HOST` IP address whitelist and blacklist. Entries can be a single IP address, such as `192.168.1.1`, or a subnet range in [CIDR](https://www.rfc-editor.org/rfc/rfc4632) format, such as `192.168.1.1/24`. Support in Enterprise Edition v3.4.0.0 and above.
  - If neither `HOST` nor `NOT_ALLOW_HOST` is set, the user is allowed to log in from any address. **Note:** For security and convenience, if `HOST` is set or neither `HOST` nor `NOT_ALLOW_HOST` is set during user creation, the system automatically adds `127.0.0.1` and `::1` to `HOST`. Therefore, the scenario described in this section can only occur when all `HOST` and `NOT_ALLOW_HOST` entries are dropped via `ALTER USER`.
  - If only `HOST` is set, the user is allowed to log in from that addresses or subnet ranges, and login from other addresses is not allowed.
  - If only `NOT_ALLOW_HOST` is set, the user is not allowed to log in from that addresses or subnet ranges, but login from other addresses is allowed.
  - If both `HOST` and `NOT_ALLOW_HOST` are set, the user can only log in from addresses that belong to `HOST` and do not belong to `NOT_ALLOW_HOST`. Login from any other address is not allowed.
- `ALLOW_DATETIME` and `NOT_ALLOW_DATETIME` Permitted and prohibited login time ranges based on the server's local time zone. A valid time range consists of three parts: date, start time (accurate to the minute), and duration (in minutes). The date can be a specific date or represented by MON, TUE, WED, THU, FRI, SAT, SUN, for example: `2025-12-25 08:00 120`, `TUE 08:00 120`. Support in Enterprise Edition v3.4.0.0 and above.
  - If neither `ALLOW_DATETIME` nor `NOT_ALLOW_DATETIME` is set, the user is allowed to log in at any time.
  - If only `ALLOW_DATETIME` is set, the user is allowed to log in during that time periods, and login at other times is not allowed.
  - If only `NOT_ALLOW_DATETIME` is set, the user is not allowed to log in during that time periods, but login at other times is allowed.
  - If both `ALLOW_DATETIME` and `NOT_ALLOW_DATETIME` are set, the user can only log in during time periods that belong to `ALLOW_DATETIME` and do not belong to `NOT_ALLOW_DATETIME`. Login at any other time is not allowed.

In the example below, we create a user with the password `abc123!@#` who can view system information.

```sql
taos> create user test pass 'abc123!@#' sysinfo 1;
Query OK, 0 of 0 rows affected (0.001254s)
```

## View Users

You can use the following command to view the users in the system.

```sql
SHOW USERS;
```

Here is an example:

```sql
taos> show users;
       name        | super | enable | sysinfo | createdb |       create_time       | totp |      allowed_host       |   allowed_datetime   |
============================================================================================================================================
 test              |     0 |      1 |       1 |        0 | 2025-12-24 18:56:20.709 |    0 | +127.0.0.1/32, +::1/128 | +ALL                 |
 root              |     1 |      1 |       1 |        1 | 2025-12-24 18:00:43.197 |    0 | +127.0.0.1/32, +::1/128 | +ALL                 |
Query OK, 2 rows in set (0.001657s)
```

Note that in `allowed_host`, if an address or subnet range is prefixed with `+`, it indicates a whitelist entry, allowing login from that address. If prefixed with `-`, it indicates a blacklist entry, disallowing login from that address. The same rule applies to `allowed_datetime`.

Alternatively, you can query the built-in system table INFORMATION_SCHEMA.INS_USERS to get user information.

```sql
taos> select * from information_schema.ins_users;
       name        | super | enable | sysinfo | createdb |       create_time       | totp |      allowed_host       |   allowed_datetime   |
============================================================================================================================================
 test              |     0 |      1 |       1 |        0 | 2025-12-24 18:56:20.709 |    0 | +127.0.0.1/32, +::1/128 | +ALL                 |
 root              |     1 |      1 |       1 |        1 | 2025-12-24 18:00:43.197 |    0 | +127.0.0.1/32, +::1/128 | +ALL                 |
Query OK, 2 row(s) in set (0.007383s)
```

## Delete User

```sql
DROP USER [IF EXISTS] user_name;
```

## Modify User Configuration

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

The following example disables the user named `test`:

```sql
taos> alter user test enable 0;
Query OK, 0 of 0 rows affected (0.001160s)
```

## TOTP Two-Factor Authentication

TOTP Two-Factor Authentication is a feature of TDengine TSDB Enterprise Edition, support in version v3.4.0.1 and above.

### Create/Update TOTP secret

```sql
CREATE TOTP_SECRET FOR USER user_name
```

If the user has not yet created a TOTP secret, this command will create a TOTP secret for the user. If the user has already created a TOTP secret, this command will update the secret for the user. In either case, this command will return the newly created secret, which will only be displayed once, please save it promptly. The system will automatically enable TOTP two-factor authentication for users who have a TOTP secret.

After enabling TOTP two-factor authentication, TDengine TSDB requires the OTP to be 6 digits long and updated every 30 seconds. Please ensure to configure your TOTP generator according to these parameters; otherwise, clients may fail to log in.

For example, we can use the following command to create a TOTP secret for user test.

```sql
taos> create totp_secret for user test;
                     totp_secret                      |
=======================================================
 ERIRPLZL4ZBFTPT5BNXMVFPR4Z3PTHUWTBTCNZPOHYPYQGTD25XA |
Query OK, 1 row(s) in set (0.002314s)
```

### Drop TOTP Secret

```sql
DROP TOTP_SECRET FROM USER user_name
```

This command drops the TOTP secret from the user. After the secret is dropped, the user's TOTP two‑factor authentication will be disabled.

For example, we can use the following command to drop the TOTP key from user test.

```sql
taos> drop totp_secret from user test;
Drop OK, 0 row(s) affected (0.002295s)
```

## Token Management

Token management is a feature of TDengine TSDB Enterprise Edition, support in version v3.4.0.0 and above.

### Create Token

```sql
CREATE TOKEN [IF NOT EXISTS] token_name FROM USER user_name [ENABLE {1|0}] [TTL value] [PROVIDER value] [EXTRA_INFO value]
```

The token_name can be up to 31 bytes long.

- `ENABLE` indicates whether the token is enabled, `1` means enabled, `0` means disabled. A disabled token cannot be used to connect the database. The default value is `1`.
- `TTL` validity period in days, `0` means always valid.
- `PROVIDER` name of the token provider, can be up to 63 bytes long.
- `EXTRA_INFO` Additional information managed by applications, can be up to 1023 bytes long.

In the following example, we create a token named test_token for the user test. Please save the token value promptly as it is only displayed once upon creation—and cannot be queried thereafter.

```sql
taos> create token test_token from user test;
                             token                               |
==================================================================
 BsyjYKxhCMntZ3pHgweCd2uV2C8HoGKn8Mvd49dRRCtzusX0P1mgqRMrG7SzUca |
Query OK, 1 row(s) in set (0.003018s)
```

### View Tokens

You can use the following command to view tokens in the system, but depending on your privilege, you may only see tokens of your own.

```sql
SHOW TOKENS;
```

For example:

```sql
taos> show tokens;
    name    | user | provider | enable |       create_time       |       expire_time       | extra_info |
=========================================================================================================
 root_token | root |          |      1 | 2025-12-25 10:02:28.000 | 1970-01-01 08:00:00.000 |            |
 test_token | test |          |      1 | 2025-12-25 10:01:47.000 | 1970-01-01 08:00:00.000 |            |
Query OK, 2 row(s) in set (0.003313s)
```

Alternatively, you can query the built-in system table INFORMATION_SCHEMA.INS_TOKENS to get user information.

```sql
taos> select * from information_schema.ins_tokens;
    name    | user | provider | enable |       create_time       |       expire_time       | extra_info |
=========================================================================================================
 root_token | root |          |      1 | 2025-12-25 10:02:28.000 | 1970-01-01 08:00:00.000 |            |
 test_token | test |          |      1 | 2025-12-25 10:01:47.000 | 1970-01-01 08:00:00.000 |            |
Query OK, 2 row(s) in set (0.007438s)
```

### Modify Token

```sql
ALTER TOKEN token_name [ENABLE {1|0}] [TTL value] [PROVIDER value] [EXTRA_INFO value]
```

When modify the validity period (TTL), new validity period starts from the time of modification.

### Drop Token

```sql
DROP TOKEN [IF EXISTS] token_name;
```

Note that when drop a user, tokens of the user will be cascade deleted simultaneously.
