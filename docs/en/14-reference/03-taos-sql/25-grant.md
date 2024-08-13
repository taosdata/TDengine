---
title: User and Access Control
sidebar_label: Access Control
description: This document describes how to manage users and permissions in TDengine.
---

User and Access control is a distingguished feature of TDengine enterprise edition. In this section, only the most fundamental functionalities of user and access control are demonstrated. To get the full knowledge of user and access control, please contact the TDengine team.

## Create a User

```sql
CREATE USER user_name PASS 'password' [SYSINFO {1|0}];
```

This statement creates a user account.

The maximum length of user_name is 23 bytes.

The maximum length of password is 31 bytes. The password can include leters, digits, and special characters excluding single quotation marks, double quotation marks, backticks, backslashes, and spaces. The password cannot be empty.

`SYSINFO` indicates whether the user is allowed to view system information. `1` means allowed, `0` means not allowed. System information includes server configuration, dnode, vnode, storage. The default value is `1`.

For example, we can create a user whose password is `123456` and is able to view system information.

```sql
taos> create user test pass '123456' sysinfo 1;
Query OK, 0 of 0 rows affected (0.001254s)
```

## View Users

To show the users in the system, please use 

```sql
SHOW USERS;
```

This is an example:

```sql
taos> show users;
           name           | super | enable | sysinfo | createdb |       create_time      | allowed_host |
=========================================================================================================
 test                     |     0 |      1 |       1 |        0 |2022-08-29 15:10:27.315 | 127.0.0.1    |
 root                     |     1 |      1 |       1 |        1 |2022-08-29 15:03:34.710 | 127.0.0.1    |
Query OK, 2 rows in database (0.001657s)
```

Alternatively, you can get the user information by querying a built-in table, INFORMATION_SCHEMA.INS_USERS. For example:

```sql
taos> select * from information_schema.ins_users;
           name           | super | enable | sysinfo | createdb |       create_time      | allowed_host |
=========================================================================================================
 test                     |     0 |      1 |       1 |        0 |2022-08-29 15:10:27.315 | 127.0.0.1    |
 root                     |     1 |      1 |       1 |        1 |2022-08-29 15:03:34.710 | 127.0.0.1    |
Query OK, 2 rows in database (0.001953s)
```

## Delete a User

```sql
DROP USER user_name;
```

## Modify User Information

```sql
ALTER USER user_name alter_user_clause

alter_user_clause: {
    PASS 'literal'
  | ENABLE value
  | SYSINFO value
  | CREATEDB value
}
```

- PASS: Modify the user password.
- ENABLE: Specify whether the user is enabled or disabled. 1 indicates enabled and 0 indicates disabled.
- SYSINFO: Specify whether the user can query system information. 1 indicates that the user can query system information and 0 indicates that the user cannot query system information.
- CREATEDB: Specify whether the user can create databases. 1 indicates that the user can create databases and 0 indicates that the user cannot create databases.

For example, you can use below command to disable user `test`:

```sql
taos> alter user test enable 0;
Query OK, 0 of 0 rows affected (0.001160s)
```


## Grant Permissions

Permission control is only available in TDengine Enterprise, please contact TDengine sales team.