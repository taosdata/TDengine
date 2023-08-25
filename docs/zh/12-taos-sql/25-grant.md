---
sidebar_label: 权限管理
title: 权限管理
description: 企业版中才具有的权限管理功能
---

本节讲述如何在 TDengine 中进行权限管理的相关操作。权限管理是 TDengine 企业版的特有功能，欲试用 TDengine 企业版请联系 TDengine 销售或市场团队。

TDengine 中的权限管理分为用户管理、数据库授权管理以及消息订阅授权管理。

当 TDengine 安装并部署成功后，系统中内置有 "root" 用户。持有默认 "root" 用户密码的系统管理员应该第一时间修改 root 用户的密码，并根据业务需要创建普通用户并为这些用户授予适当的权限。在未授权的情况下，普通用户可以创建 DATABASE，并拥有自己创建的 DATABASE 的所有权限，包括删除数据库、修改数据库、查询时序数据和写入时序数据。超级用户可以给普通用户授予其他（即非该用户所创建的） DATABASE 的读写权限，使其可以在这些 DATABASE 上读写数据，但不能对其进行删除和修改数据库的操作。超级用户或者 topic 的创建者也可以给其它用户授予对某个 topic 的订阅权限。

## 用户管理

用户管理涉及用户的整个生命周期，从创建用户、对用户进行授权、撤销对用户的授权、查看用户信息、直到删除用户。

### 创建用户

创建用户的操作只能由 root 用户进行，语法如下

```sql
CREATE USER user_name PASS 'password' [SYSINFO {1\|0}]; 
```

说明：

-   user_name 最长为 23 字节。
-   password 最长为 128 字节，合法字符包括"a-zA-Z0-9!?\$%\^&\*()_–+={[}]:;@\~\#\|\<,\>.?/"，不可以出现单双引号、撇号、反斜杠和空格，且不可以为空。
-   SYSINFO 表示用户是否可以查看系统信息。1 表示可以查看，0 表示不可以查看。系统信息包括服务端配置信息、服务端各种节点信息（如 DNODE、QNODE等）、存储相关的信息等。默认为可以查看系统信息。

示例：创建密码为123456且可以查看系统信息的用户 test

```
SQL taos\> create user test pass '123456' sysinfo 1; Query OK, 0 of 0 rows affected (0.001254s)
```

### 查看用户

查看系统中的用户信息请使用 show users 命令，示例如下

```sql
show users;
```

也可以通过查询系统表 `INFORMATION_SCHEMA.INS_USERS` 获取系统中的用户信息，示例如下

```sql
select * from information_schema.ins_users;  
```

### 删除用户

删除用户请使用

```sql
DROP USER user_name; 
```

### 修改用户信息

修改用户信息的命令如下

```sql
ALTER USER user_name alter_user_clause   alter_user_clause: {  PASS 'literal'  \| ENABLE value  \| SYSINFO value } 
```

说明：

-   PASS：修改用户密码。
-   ENABLE：修改用户是否启用。1 表示启用此用户，0 表示禁用此用户。
-   SYSINFO：修改用户是否可查看系统信息。1 表示可以查看系统信息，0 表示不可以查看系统信息。

示例：禁用 test 用户

```sql
alter user test enable 0; Query OK, 0 of 0 rows affected (0.001160s) 
```

```sql
CREATE USER use_name PASS 'password' [SYSINFO {1|0}];
```

## 访问控制

在 TDengine 企业版中，系统管理员可以根据业务和数据安全的需要控制任意一个用户对每一个数据库、订阅甚至表级别的访问。

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

### 数据库权限


TDengine 有超级用户和普通用户两类用户。超级用户缺省创建为root，拥有所有权限。使用超级用户创建出来的用户为普通用户。在未授权的情况下，普通用户可以创建 DATABASE，并拥有自己创建的 DATABASE 的所有权限，包括删除数据库、修改数据库、查询时序数据和写入时序数据。超级用户可以给普通用户授予其他 DATABASE 的读写权限，使其可以在此 DATABASE 上读写数据，但不能对其进行删除和修改数据库的操作。

对于非DATABASE的对象，如USER、DNODE、UDF、QNODE等，普通用户只有读权限（一般为SHOW命令），不能创建和修改。

对数据库的访问权限包含读和写两种权限，它们可以被分别授予，也可以被同时授予。

补充说明

-   priv_level 格式中 "." 之前为数据库名称， "." 之后为表名称
-   "dbname.\*" 意思是名为 "dbname" 的数据库中的所有表
-   "\*.\*" 意思是所有数据库名中的所有表

**下表中总结了数据库权限的各种组合**

对 root 用户和普通用户的权限的说明如下表

| 用户     | 描述                               | 权限说明                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
|----------|------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 超级用户 | 只有 root 是超级用户               |  DB 外部 所有操作权限，例如user、dnode、udf、qnode等的CRUD DB 权限，包括 创建 删除 更新，例如修改 Option，移动 Vgruop等 读 写 Enable/Disable 用户                                                                                                                                                                                                                                                                                                                                     |
| 普通用户 | 除 root 以外的其它用户均为普通用户 | 在可读的 DB 中，普通用户可以进行读操作 select describe show subscribe 在可写 DB 的内部，用户可以进行写操作： 创建、删除、修改 超级表 创建、删除、修改 子表 创建、删除、修改 topic 写入数据 被限制系统信息时，不可进行如下操作 show dnode、mnode、vgroups、qnode、snode 修改用户包括自身密码 show db时只能看到自己的db，并且不能看到vgroups、副本、cache等信息 无论是否被限制系统信息，都可以 管理 udf 可以创建 DB 自己创建的 DB 具备所有权限 非自己创建的 DB ，参照读、写列表中的权限 |

### 消息订阅授权

任意用户都可以在自己拥有读权限的数据库上创建 topic。超级用户 root 可以在任意数据库上创建 topic。每个 topic 的订阅权限都可以被独立授权给任何用户，不管该用户是否拥有该数据库的访问权限。删除 topic 只能由 root 用户或者该 topic 的创建者进行。topic 只能由超级用户、topic的创建者或者被显式授予 subscribe 权限的用户订阅。

授予订阅权限的语法如下：

```sql
GRANT privileges ON priv_level TO user_name  privileges : {  ALL  | priv_type [, priv_type] ... }   priv_type : {  SUBSCRIBE }   priv_level : {  topic_name }
```

### 基于标签的授权（表级授权）

从 TDengine 3.0.5.0 开始，我们支持按标签授权某个超级表中部分特定的子表。具体的 SQL 语法如下。

```sql
GRANT privileges ON priv_level [WITH tag_condition] TO user_name
 
privileges : {
    ALL
  | SUBSCRIBE
  | priv_type [, priv_type] ...
}
 
priv_type : {
    READ
  | WRITE
}
 
priv_level : {
    dbname.tbname
  | dbname.*
  | *.*
  | topic_name
}

REVOKE privileges ON priv_level [WITH tag_condition] FROM user_name

privileges : {
    ALL
  | priv_type [, priv_type] ...
}
 
priv_type : {
    READ
  | WRITE
}
 
priv_level : {
    dbname.tbname
  | dbname.*
  | *.*
}
```

上面 SQL 的语义为：

- 用户可以通过 dbname.tbname 来为指定的表（包括超级表和普通表）授予或回收其读写权限，不支持直接对子表授予或回收权限。
- 用户可以通过 dbname.tbname 和 WITH 子句来为符合条件的所有子表授予或回收其读写权限。使用 WITH 子句时，权限级别必须为超级表。

**表级权限和数据库权限的关系**

下表列出了在不同的数据库授权和表级授权的组合下产生的实际权限。

|                |**表无授权**       | **表读授权** | **表读授权有标签条件** | **表写授权** | **表写授权有标签条件** |
| -------------- | ---------------- | -------- | ---------- | ------ | ----------- | 
| **数据库无授权**  | 无授权          | 对此表有读权限，对数据库下的其他表无权限   |  对此表符合标签权限的子表有读权限，对数据库下的其他表无权限       | 对此表有写权限，对数据库下的其他表无权限      | 对此表符合标签权限的子表有写权限，对数据库下的其他表无权限      | 
| **数据库读授权**  | 对所有表有读权限 | 对所有表有读权限     | 对此表符合标签权限的子表有读权限，对数据库下的其他表有读权限       | 对此表有写权限，对所有表有读权限    | 对此表符合标签权限的子表有写权限，所有表有读权限 | 
| **数据库写授权**  | 对所有表有写权限 | 对此表有读权限，对所有表有写权限    | 对此表符合标签权限的子表有读权限，对所有表有写权限      | 对所有表有写权限     | 对此表符合标签权限的子表有写权限，数据库下的其他表有写权限        | 

### 查看用户授权

使用下面的命令可以显示一个用户所拥有的授权：

```sql
show user privileges 
```
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

### 撤销授权

1.  撤销数据库访问的授权

```sql
REVOKE privileges ON priv_level FROM user_name   privileges : {  ALL  \| priv_type [, priv_type] ... }   priv_type : {  READ  \| WRITE }   priv_level : {  dbname.\*  \| \*.\* }  
```

2.  撤销数据订阅的授权

```sql
REVOKE privileges ON priv_level FROM user_name   privileges : {  ALL  \| priv_type [, priv_type] ... }   priv_type : {  SUBSCRIBE }   priv_level : {  topi_name }
