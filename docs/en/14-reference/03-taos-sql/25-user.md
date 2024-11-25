---
title: Manage Users
description: This section discusses basic user management functions.
slug: /tdengine-reference/sql-manual/manage-users
---

User and permission management is a feature of the TDengine enterprise edition. This section covers only the basic user management part. For comprehensive permission management functions, please contact the TDengine sales team.

## Create User

```sql
CREATE USER user_name PASS 'password' [SYSINFO {1|0}];
```

The username can be up to 23 bytes long.

The password can be up to 31 bytes long. It can include letters, numbers, and special characters except for single quotes, double quotes, backticks, backslashes, and spaces. The password cannot be an empty string.

`SYSINFO` indicates whether the user can view system information. `1` means they can view it, while `0` means they do not have permission. System information includes service configurations, dnodes, vn, storage, etc. The default value is `1`.

In the following example, we create a user with the password `123456` who can view system information.

```sql
taos> create user test pass '123456' sysinfo 1;
Query OK, 0 of 0 rows affected (0.001254s)
```

## View Users

You can use the following command to view users in the system.

```sql
SHOW USERS;
```

Here is an example:

```sql
taos> show users;
           name           | super | enable | sysinfo | createdb |       create_time      | allowed_host |
=========================================================================================================
 test                     |     0 |      1 |       1 |        0 |2022-08-29 15:10:27.315 | 127.0.0.1    |
 root                     |     1 |      1 |       1 |        1 |2022-08-29 15:03:34.710 | 127.0.0.1    |
Query OK, 2 rows in database (0.001657s)
```

Alternatively, you can query the built-in system table INFORMATION_SCHEMA.INS_USERS to get user information.

```sql
taos> select * from information_schema.ins_users;
           name           | super | enable | sysinfo | createdb |       create_time      | allowed_host |
=========================================================================================================
 test                     |     0 |      1 |       1 |        0 |2022-08-29 15:10:27.315 | 127.0.0.1    |
 root                     |     1 |      1 |       1 |        1 |2022-08-29 15:03:34.710 | 127.0.0.1    |
Query OK, 2 rows in database (0.001953s)
```

## Delete User

```sql
DROP USER user_name;
```

## Modify User Configuration

```sql
ALTER USER user_name alter_user_clause
 
alter_user_clause: {
    PASS 'literal'
  | ENABLE value
  | SYSINFO value
  | CREATEDB value
}
```

- PASS: Modifies the password, followed by the new password.
- ENABLE: Enables or disables the user, where `1` means enabled and `0` means disabled.
- SYSINFO: Allows or prohibits viewing system information, where `1` means allowed and `0` means prohibited.
- CREATEDB: Allows or prohibits database creation, where `1` means allowed and `0` means prohibited.

In the following example, we disable the user named `test`:

```sql
taos> alter user test enable 0;
Query OK, 0 of 0 rows affected (0.001160s)
```

## Authorization Management

Authorization management is only available in the TDengine enterprise edition; please contact the TDengine sales team.
