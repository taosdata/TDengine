---
title: 用户管理
sidebar_label: 用户管理
description: 本节讲述基本的用户管理功能
---

用户和权限管理是 TDengine 企业版的功能，本节只讲述基本的用户管理部分。要想了解和获取全面的权限管理功能，请联系 TDengine 销售团队。

## 创建用户

```sql
CREATE USER user_name PASS 'password' [SYSINFO {1|0}];
```

用户名最长不超过 23 个字节。

密码最长不超过 31 个字节。密码可以包含字母、数字以及除单引号、双引号、反引号、反斜杠和空格以外的特殊字符，密码不能为空字符串。

`SYSINFO` 表示该用户是否能够查看系统信息。`1` 表示可以查看，`0` 表示无权查看。系统信息包括服务配置、dnode、vnode、存储等信息。缺省值为 `1`。

在下面的示例中，我们创建一个密码为 `123456` 且可以查看系统信息的用户。 

```sql
taos> create user test pass '123456' sysinfo 1;
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
           name           | super | enable | sysinfo | createdb |       create_time      | allowed_host |
=========================================================================================================
 test                     |     0 |      1 |       1 |        0 |2022-08-29 15:10:27.315 | 127.0.0.1    |
 root                     |     1 |      1 |       1 |        1 |2022-08-29 15:03:34.710 | 127.0.0.1    |
Query OK, 2 rows in database (0.001657s)
```

或者，可以查询内置系统表 INFORMATION_SCHEMA.INS_USERS 来获取用户信息。

```sql
taos> select * from information_schema.ins_users;
           name           | super | enable | sysinfo | createdb |       create_time      | allowed_host |
=========================================================================================================
 test                     |     0 |      1 |       1 |        0 |2022-08-29 15:10:27.315 | 127.0.0.1    |
 root                     |     1 |      1 |       1 |        1 |2022-08-29 15:03:34.710 | 127.0.0.1    |
Query OK, 2 rows in database (0.001953s)
```

## 删除用户

```sql
DROP USER user_name;
```

## 修改用户配置

```sql
ALTER USER user_name alter_user_clause
 
alter_user_clause: {
    PASS 'literal'
  | ENABLE value
  | SYSINFO value
  | CREATEDB value
}
```

- PASS: 修改密码，后跟新密码
- ENABLE: 启用或禁用该用户，`1` 表示启用，`0` 表示禁用
- SYSINFO: 允许或禁止查看系统信息，`1` 表示允许，`0` 表示禁止
- CREATEDB: 允许或禁止创建数据库，`1` 表示允许，`0` 表示禁止

下面的示例禁用了名为 `test` 的用户:

```sql
taos> alter user test enable 0;
Query OK, 0 of 0 rows affected (0.001160s)
```

## 授权管理

授权管理仅在 TDengine 企业版中可用，请联系 TDengine 销售团队。