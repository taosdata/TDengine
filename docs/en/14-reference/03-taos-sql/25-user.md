---
title: Users
slug: /tdengine-reference/sql-manual/manage-users
---

User and permission management is a feature of TDengine Enterprise Edition. This section only discusses the basic user management part. To learn about and obtain comprehensive permission management features, please contact the TDengine sales team.

## Create User

```sql
CREATE USER user_name PASS 'password' [SYSINFO {1|0}] [CREATEDB {1|0}];
```

The username can be up to 23 bytes long.

The password must be between 8 and 255 characters long and include at least three types of characters from the following: uppercase letters, lowercase letters, numbers, and special characters. Special characters include `! @ # $ % ^ & * ( ) - _ + = [ ] { } : ; > < ? | ~ , .`, and this requirement is able to be closed by adding enableStrongPassword 0 in taos.cfg, or by the following SQL:

```sql
alter all dnodes 'EnableStrongPassword' '0'
```

`SYSINFO` indicates whether the user can view system information. `1` means they can view, `0` means they have no permission to view. System information includes service configuration, dnode, vnode, storage, etc. The default value is `1`.

`CREATEDB` indicates whether the user can create databases. `1` means they can create databases, `0` means they have no permission to create databases. The default value is `0`. // Supported starting from TDengine Enterprise version 3.3.2.0

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

- PASS: Change the password, followed by the new password
- ENABLE: Enable or disable the user, `1` means enable, `0` means disable
- SYSINFO: Allow or prohibit viewing system information, `1` means allow, `0` means prohibit
- CREATEDB: Allow or prohibit creating databases, `1` means allow, `0` means prohibit. // Supported starting from TDengine Enterprise version 3.3.2.0

The following example disables the user named `test`:

```sql
taos> alter user test enable 0;
Query OK, 0 of 0 rows affected (0.001160s)
```

## Authorization Management

Authorization management is only available in the TDengine Enterprise Edition, please contact the TDengine sales team.
