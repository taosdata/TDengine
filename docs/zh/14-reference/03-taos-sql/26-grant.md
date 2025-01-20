---
toc_max_heading_level: 4
title: 权限管理
---

TDengine 中的权限管理分为[用户管理](../user)、数据库授权管理以及消息订阅授权管理，本节重点说明数据库授权和订阅授权。

## 数据库访问授权

系统管理员可以根据业务需要对系统中的每个用户针对每个数据库进行特定的授权，以防止业务数据被不恰当的用户读取或修改。对某个用户进行数据库访问授权的语法如下：

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
    dbname.tbname
  | dbname.*
  | *.*
}
```

对数据库的访问权限包含读和写两种权限，它们可以被分别授予，也可以被同时授予。

说明

-   priv_level 格式中 "." 之前为数据库名称， "." 之后为表名称，意思为表级别的授权控制。如果 "." 之后为 "\*" ，意为 "." 前所指定的数据库中的所有表
-   "dbname.\*" 意思是名为 "dbname" 的数据库中的所有表
-   "\*.\*" 意思是所有数据库名中的所有表

### 数据库权限说明

对 root 用户和普通用户的权限的说明如下表

| 用户     | 描述                               | 权限说明                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| -------- | ---------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 超级用户 | 只有 root 是超级用户               | DB 外部 所有操作权限，例如user、dnode、udf、qnode等的CRUD DB 权限，包括 创建 删除 更新，例如修改 Option，移动 Vgruop等 读 写 Enable/Disable 用户                                                                                                                                                                                                                                                                                                                                      |
| 普通用户 | 除 root 以外的其它用户均为普通用户 | 在可读的 DB 中，普通用户可以进行读操作 select describe show subscribe 在可写 DB 的内部，用户可以进行写操作： 创建、删除、修改 超级表 创建、删除、修改 子表 创建、删除、修改 topic 写入数据 被限制系统信息时，不可进行如下操作 show dnode、mnode、vgroups、qnode、snode 修改用户包括自身密码 show db时只能看到自己的db，并且不能看到vgroups、副本、cache等信息 无论是否被限制系统信息，都可以 管理 udf 可以创建 DB 自己创建的 DB 具备所有权限 非自己创建的 DB ，参照读、写列表中的权限 |

## 消息订阅授权

任意用户都可以在自己拥有读权限的数据库上创建 topic。超级用户 root 可以在任意数据库上创建 topic。每个 topic 的订阅权限都可以被独立授权给任何用户，不管该用户是否拥有该数据库的访问权限。删除 topic 只能由 root 用户或者该 topic 的创建者进行。topic 只能由超级用户、topic的创建者或者被显式授予 subscribe 权限的用户订阅。

具体的 SQL 语法如下：

```sql
GRANT SUBSCRIBE ON topic_name TO user_name

REVOKE SUBSCRIBE ON topic_name FROM user_name
```

## 基于标签的授权（表级授权）

从 TDengine 3.0.5.0 开始，我们支持按标签授权某个超级表中部分特定的子表。具体的 SQL 语法如下。

```sql
GRANT privileges ON priv_level [WITH tag_condition] TO user_name
 
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

## 表级权限和数据库权限的关系

下表列出了在不同的数据库授权和表级授权的组合下产生的实际权限。

|                  | **表无授权**     | **表读授权**                             | **表读授权有标签条件**                                       | **表写授权**                             | **表写授权有标签条件**                                     |
| ---------------- | ---------------- | ---------------------------------------- | ------------------------------------------------------------ | ---------------------------------------- | ---------------------------------------------------------- |
| **数据库无授权** | 无授权           | 对此表有读权限，对数据库下的其他表无权限 | 对此表符合标签权限的子表有读权限，对数据库下的其他表无权限   | 对此表有写权限，对数据库下的其他表无权限 | 对此表符合标签权限的子表有写权限，对数据库下的其他表无权限 |
| **数据库读授权** | 对所有表有读权限 | 对所有表有读权限                         | 对此表符合标签权限的子表有读权限，对数据库下的其他表有读权限 | 对此表有写权限，对所有表有读权限         | 对此表符合标签权限的子表有写权限，所有表有读权限           |
| **数据库写授权** | 对所有表有写权限 | 对此表有读权限，对所有表有写权限         | 对此表符合标签权限的子表有读权限，对所有表有写权限           | 对所有表有写权限                         | 对此表符合标签权限的子表有写权限，数据库下的其他表有写权限 |


## 查看用户授权

使用下面的命令可以显示一个用户所拥有的授权：

```sql
show user privileges 
```

## 撤销授权

1.  撤销数据库访问的授权

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
    dbname.tbname
  | dbname.*
  | *.*
}
```

2.  撤销数据订阅的授权

```sql
REVOKE privileges ON priv_level FROM user_name

privileges : {
    ALL
  | priv_type [, priv_type] ...
}

priv_type : {
    SUBSCRIBE
}

priv_level : {
    topic_name
}
```
