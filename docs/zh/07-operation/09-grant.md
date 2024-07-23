---
sidebar_label: 权限管理
title: 权限管理
toc_max_heading_level: 4
---

TDengine 支持对系统资源、库、表、视图和主题的访问权限控制。root 用户可以为每个用户针对不同的资源设置不同的访问权限。

## 资源管理

仅 root 用户可以管理用户、节点、vnode、qnode、snode 等系统信息，包括查询、新增、删除和修改。

## 授权

### 库和表的授权

在 TDengine 中，库和表的权限分为 read （读）和 write （写）两种。这些权限可以单独授予，也可以同时授予用户。

- read 权限：拥有 read 权限的用户仅能查询库或表中的数据，而无法对数据进行修改或删除。这种权限适用于需要访问数据但不需要对数据进行写入操作的场景，如数据分析师、报表生成器等。
- write 权限：拥有 write 权限的用户既可以查询库或表中的数据，也可以向库或表中写入数据。这种权限适用于需要对数据进行写入操作的场景，如数据采集器、数据处理器等。

对某个用户进行库和表访问授权的语法如下。

```sql
grant privileges on resources [with tag_filter] to user_name
privileges: {
 all,
 | priv_type [, priv_type] …
}
priv_type：{
 read
 | write
}
resources ：{
 dbname.tbname
 | dbname.*
 | *.*
}
```

相关参数说明如下。
- resources ：可以访问的库或表。. 之前为数据库名称，. 之后为表名称。dbname.tbname 的意思是名为 dbname 的数据库中的 tbname 表必须为普通表或超级表。dbname.* 的意思是名为 dbname 的数据库中的所有表。*.* 的意思是所有数据库中的所有表。
- tag_f ilter：超级表的过滤条件。

上述 SQL 既可以授权一个库、所有库，也可以授权一个库下的普通表或超级表，还可以通过 dbname.tbname 和 with 子句的组合授权符合过滤条件的一张超级表下的所有子表。
如下 SQL 将数据库 power 的 read 权限授权给用户 test。
```sql
grant read on power to test
```

如下 SQL 将数据库 power 下超级表 meters 的全部权限授权给用户 test。
```sql
grant all on power.meters to test
```

如下 SQL 将超级表 meters 离标签值 groupId 等于 1 的子表的 write 权限授权给用户test。
```sql
grant all on power.meters with groupId=1 to test
```

如果用户被授予了数据库的写权限，那么用户对这个数据库下的所有表都有读和写的权限。但如果一个数据库只有读的权限或甚至读的权限都没有，表的授权会让用户能读或写部分表，详细的授权组合见参考手册。

### 视图授权

在 TDengine 中，视图（view）的权限分为 read、write 和 alter 3 种。它们决定了用户对视图的访问和操作权限。以下是关于视图权限的具体使用规则。
- 视图的创建者和 root 用户默认具备所有权限。这意味着视图的创建者和 root 用户可以查询、写入和修改视图。
- 对其他用户进行授权和回收权限可以通过 grant 和 revoke 语句进行。这些操作只能由 root 用户执行。
- 视图权限需要单独授权和回收，通过 db.* 进行的授权和回收不包含视图权限。
- 视图可以嵌套定义和使用，对视图权限的校验也是递归进行的。

为了方便视图的分享和使用，TDengine 引入了视图有效用户（即视图的创建用户）的概念。被授权用户可以使用视图有效用户的库、表及嵌套视图的读写权限。当视图被replace 后，有效用户也会被更新。

视图操作和权限要求的详细对应关系请见参考手册。

视图授权语法如下。
```sql
grant privileges on [db_name.]view_name to user_name
privileges: {
 all,
 | priv_type [, priv_type] ...
}
priv_type: {
 read
 | write
 alter
}
```

在数据库 power 下将视图 view_name 的读权限授权给用户 test，SQL 如下。
```sql
grant read on power.view_name to test
```

在数据库 power 库下将视图 view_name 的全部权限授权给用户 test，SQL 如下。
```sql
grant all on power.view_name to test
```

### 消息订阅授权

消息订阅是 TDengine 独具匠心的设计。为了保障用户订阅信息的安全性，TDengine 可针对消息订阅进行授权。在使用消息订阅授权功能前，用户需要了解它的如下特殊使用规则。
- 任意用户在拥有读权限的数据库上都可以创建主题。root 用户具有在任意数据库上创建主题的权限。
- 每个主题的订阅权限可以独立授权给任何用户，无论其是否具备该数据库的访问权限。
- 删除主题的操作只有 root 用户或该主题的创建者可以执行。
- 只有超级用户、主题的创建者或被显式授权订阅权限的用户才能订阅主题。这些权限设置既保障了数据库的安全性，又保证了用户在有限范围内的灵活操作。

消息订阅授权的 SQL 语法如下。
```sql
grant privileges on priv_level to user_name 
privileges : { 
 all 
 | priv_type [, priv_type] ...
} 
priv_type : { 
 subscribe
} 
priv_level : { 
 topic_name
}
```

将名为 topic_name 的主题授权给用户 test，SQL 如下。
```sql
grant subscribe on topic_name to test
```

## 查看授权

当企业拥有多个数据库用户时，使用如下命令可以查询具体一个用户所拥有的所有授权，SQL 如下。
```sql
show user privileges
```

## 撤销授权

由于数据库访问、数据订阅和视图的特性不同，针对具体授权的撤销语法也略有差异。下面列出具体的撤销授权对应不同授权对象的语法。
撤销数据库访问授权的 SQL 如下。
```sql
revoke privileges on priv_level [with tag_condition] from user_name
privileges : {
 all
 | priv_type [, priv_type] ...
}
priv_type : {
 read
 | write
}
priv_level : {
 dbname.tbname
 | dbname.*
 | *.*
}
```

撤销视图授权的 SQL 如下。
```sql
revoke privileges on [db_name.]view_name from user_name
privileges: {
 all,
 | priv_type [, priv_type] ...
}
priv_type: {
 read
 | write
 | alter
}
```

撤销数据订阅授权的 SQL 如下。
```sql
revoke privileges on priv_level from user_name 
privileges : {
    all 
 | priv_type [, priv_type] ...
} 
priv_type : { 
 subscribe
} 
priv_level : { 
 topic_name
}
```

撤销用户 test 对于数据库 power 的所有授权的 SQL 如下。
```sql
revoke all on power from test
```

撤销用户 test 对于数据库 power 的视图 view_name 的读授权的 SQL 如下。
```sql
revoke read on power.view_name from test
```

撤销用户 test 对于消息订阅 topic_name 的 subscribe 授权的 SQL 如下。
```sql
revoke subscribe on topic_name from test
```