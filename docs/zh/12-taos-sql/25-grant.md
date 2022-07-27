---
sidebar_label: 权限管理
title: 权限管理
---

本节讲述如何在 TDengine 中进行权限管理的相关操作。

## 创建用户

```sql
CREATE USER use_name PASS password;
```

创建用户。

use_name最长为23字节。

password最长为128字节，合法字符包括"a-zA-Z0-9!?$%^&*()_–+={[}]:;@~#|<,>.?/"，不可以出现单双引号、撇号、反斜杠和空格，且不可以为空。

## 删除用户

```sql
DROP USER user_name;
```

## 修改用户信息

```sql
ALTER USER user_name alter_user_clause
 
alter_user_clause: {
    PASS 'literal'
  | ENABLE value
  | SYSINFO value
}
```

- PASS：修改用户密码。
- ENABLE：修改用户是否启用。1表示启用此用户，0表示禁用此用户。
- SYSINFO：修改用户是否可查看系统信息。1表示可以查看系统信息，0表示不可以查看系统信息。


## 授权

```sql
GRANT privileges ON priv_level TO user_name
 
privileges : {
    ALL
  | priv_type [, priv_type] ...
}
 
priv_type : {
    READ
  | WRITE
}
 
priv_level : {
    dbname.*
  | *.*
}
```

对用户授权。

授权级别支持到DATABASE，权限有READ和WRITE两种。

TDengine 有超级用户和普通用户两类用户。超级用户缺省创建为root，拥有所有权限。使用超级用户创建出来的用户为普通用户。在未授权的情况下，普通用户可以创建DATABASE，并拥有自己创建的DATABASE的所有权限，包括删除数据库、修改数据库、查询时序数据和写入时序数据。超级用户可以给普通用户授予其他DATABASE的读写权限，使其可以在此DATABASE上读写数据，但不能对其进行删除和修改数据库的操作。

对于非DATABASE的对象，如USER、DNODE、UDF、QNODE等，普通用户只有读权限（一般为SHOW命令），不能创建和修改。

## 撤销授权

```sql
REVOKE privileges ON priv_level FROM user_name
 
privileges : {
    ALL
  | priv_type [, priv_type] ...
}
 
priv_type : {
    READ
  | WRITE
}
 
priv_level : {
    dbname.*
  | *.*
}

```

收回对用户的授权。