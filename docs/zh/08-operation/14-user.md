---
sidebar_label: 用户和权限
title: 用户和权限管理
toc_max_heading_level: 4
---

TDengine TSDB 默认仅配置了一个 root 用户，该用户拥有最高权限。TDengine TSDB 支持对库、表、视图和主题等系统资源的访问权限控制。root 用户可以为每个用户针对不同的资源设置不同的访问权限。本节介绍 TDengine TSDB 中的用户和权限管理。用户和权限管理是 TDengine TSDB Enterprise 特有功能。

从 TDengine 3.4.0.0 版本开始，引入了基于角色的访问控制（RBAC，Role-Based Access Control）机制。RBAC 通过将权限与角色关联，再将角色授予用户，实现更灵活、更精细的权限管理。系统支持三权分立的安全架构，包含数据库管理员（SYSDBA）、数据库安全员（SYSSEC）和数据库审计员（SYSAUDIT）三种系统管理角色。

## 用户管理

### 创建用户

创建用户的操作只能由具有 CREATE USER 权限的用户进行，语法如下。

```sql
create user user_name pass 'password' [sysinfo {1|0}] [createdb {1|0}]
```

相关参数说明如下。

- user_name：用户名最长不超过 23 个字节。
- password：密码长度必须为 8 到 255 个字节。密码至少包含大写字母、小写字母、数字、特殊字符中的三类。特殊字符包括 `! @ # $ % ^ & * ( ) - _ + = [ ] { } : ; > < ? | ~ , .`（始自 v3.3.5.0），可以通过在 taos.cfg 中添加参数 `enableStrongPassword 0` 关闭此强制要求，或者通过如下 SQL 关闭（始自 v3.3.6.0）。

```sql
alter all dnodes 'EnableStrongPassword' '0'
```

- sysinfo：用户是否可以查看系统信息。1 表示可以查看，0 表示不可以查看。系统信息包括服务端配置信息、服务端各种节点信息，如 dnode、查询节点（qnode）等，以及与存储相关的信息等。默认为可以查看系统信息。
- createdb：用户是否可以创建数据库。1 表示可以创建，0 表示不可以创建。缺省值为 0。从企业版 v3.3.2.0 开始支持。

如下 SQL 可以创建密码为 `abc123!@#` 且可以查看系统信息的用户 test。

```sql
create user test pass 'abc123!@#' sysinfo 1
```

### 查看用户

查看系统中的用户信息可使用如下 SQL。

```sql
show users;
```

也可以通过查询系统表 information_schema.ins_users 获取系统中的用户信息，示例如下。

```sql
select * from information_schema.ins_users;
```

### 修改用户信息

修改用户信息的 SQL 如下。

```sql
alter user user_name alter_user_clause 
alter_user_clause: { 
 pass 'literal' 
 | enable value 
 | sysinfo value
 | createdb value
}
```

相关参数说明如下。

- pass：修改用户密码。
- enable：是否启用用户。1 表示启用此用户，0 表示禁用此用户。
- sysinfo：用户是否可查看系统信息。1 表示可以查看系统信息，0 表示不可以查看系统信息
- createdb：用户是否可创建数据库。1 表示可以创建数据库，0 表示不可以创建数据库。从企业版 v3.3.2.0 开始支持。

如下 SQL 禁用 test 用户。

```sql
alter user test enable 0
```

### 删除用户

删除用户的 SQL 如下。

```sql
drop user user_name
```

## 角色管理

从 TDengine 3.4.0.0 版本开始，支持基于角色的访问控制（RBAC）。通过角色，可以将一组权限打包，然后统一授予用户，简化权限管理。

### 创建角色

创建角色的语法如下。

```sql
create role [if not exists] role_name;
```

示例：创建名为 developer 的角色。

```sql
create role developer;
```

### 查看角色

查看系统中所有角色的 SQL 如下。

```sql
show roles;
```

也可以通过查询系统表获取角色信息。

```sql
select * from information_schema.ins_roles;
```

### 查看角色权限

查看角色拥有的权限可使用如下 SQL。

```sql
show role privileges;
```

也可以通过查询系统表获取角色权限信息。

```sql
select * from information_schema.ins_role_privileges;
```

### 删除角色

删除角色的 SQL 如下。

```sql
drop role [if exists] role_name;
```

### 锁定/解锁角色

锁定角色后，拥有该角色的用户将无法使用角色对应的权限。

```sql
lock role role_name;
unlock role role_name;
```

### 将角色授予用户

将角色授予用户的语法如下。

```sql
grant role role_name to user_name;
```

示例：将角色 developer 授予用户 test。

```sql
grant role developer to test;
```

### 从用户撤销角色

从用户撤销角色的语法如下。

```sql
revoke role role_name from user_name;
```

示例：从用户 test 撤销角色 developer。

```sql
revoke role developer from test;
```

## 系统管理角色

TDengine 企业版支持三权分立的安全架构，包含三种系统管理角色：数据库管理员（SYSDBA）、数据库安全员（SYSSEC）和数据库审计员（SYSAUDIT）。在首次安装后，这些角色均授予给系统默认用户 root。

**重要规则：**
1. 不允许将上述三种角色中任意两个同时授予同一个用户，以确保三权分立的核心安全架构。
2. 系统中允许存在多个拥有 SYSDBA/SYSSEC/SYSAUDIT 角色的用户。
3. 系统管理角色的权限范围不可更改。

### 数据库管理员（SYSDBA）

数据库管理员是权限最高的系统管理员角色，负责数据库的日常运维和系统管理。具备除数据库安全员、数据库审计员之外的所有系统权限。负责创建用户和角色，但不能执行授予 SYSDBA 之外的系统权限管理，不能执行与审计数据库相关的操作。

授予/撤销 SYSDBA 角色：

```sql
grant role `SYSDBA` to user_name;
revoke role `SYSDBA` from user_name;
```

### 数据库安全员（SYSSEC）

数据库安全员的主要职责是制定并应用安全策略，强化系统安全机制。主要负责：
1. 用户与角色授权管理：授予/撤销除 SYSDBA/SYSAUDIT 之外的权限。
2. 权限与对象授权：可以将除 SYSDBA/SYSAUDIT 之外的系统权限或数据库对象（表、视图等）的权限授予其他用户或角色。

SYSSEC 拥有的权限包括：
- 授予/撤销 SYSSEC 权限
- 修改和查看安全参数（ALTER/SHOW SECURITY VARIABLE）
- TOTP 管理（CREATE/SHOW/DROP/UPDATE TOTP）
- 设置用户安全信息（SET USER SECURITY INFO）
- 读取安全相关的系统信息（READ INFORMATION_SCHEMA SECURITY）

授予/撤销 SYSSEC 角色：

```sql
grant role `SYSSEC` to user_name;
revoke role `SYSSEC` from user_name;
```

### 数据库审计员（SYSAUDIT）

数据库审计员是负责独立审计监督的关键角色，其核心职责是监控和审查数据库操作，确保所有行为合规。审计员不能查看业务数据，仅负责 SYSAUDIT 权限的授予与撤销。

SYSAUDIT 拥有的权限包括：
- 授予/撤销 SYSAUDIT 权限
- 修改和查看审计参数（ALTER/SHOW AUDIT VARIABLE）
- 审计数据库管理（CREATE/DROP/ALTER/READ/WRITE AUDIT DATABASE）
- 读取审计相关的系统信息（READ INFORMATION_SCHEMA AUDIT）

授予/撤销 SYSAUDIT 角色：

```sql
grant role `SYSAUDIT` to user_name;
revoke role `SYSAUDIT` from user_name;
```

## 系统内置角色

除了三种系统管理角色外，TDengine 还提供以下系统内置角色，这些角色的权限范围不可更改。

### SYSAUDIT_LOG 角色

用于在审计库中建表、写入数据，但不能删表、修改表或删除数据。该角色不能与 SYSDBA/SYSSEC/SYSAUDIT 角色同时授予某一个用户。

### SYSINFO_0 角色

对应原有 `sysinfo=0` 的权限分类，为保持兼容性改造为角色。主要权限包括：
- 读取基础系统信息（READ INFORMATION_SCHEMA BASIC）
- 读取基础性能信息（READ PERFORMANCE_SCHEMA BASIC）
- SHOW DATABASES 等基础查询权限
- 不具有修改自身密码的权限

可以将该角色直接授予用户或其他角色：

```sql
grant role `SYSINFO_0` to user_name;
```

### SYSINFO_1 角色

对应原有 `sysinfo=1` 的权限分类，为保持兼容性改造为角色。主要权限包括：
- 修改自身密码（ALTER SELF PASS）
- 读取特权系统信息（READ INFORMATION_SCHEMA PRIVILEGED）
- 读取特权性能信息（READ PERFORMANCE_SCHEMA PRIVILEGED）
- SHOW USERS、SHOW CLUSTER 等查询权限

可以将该角色直接授予用户或其他角色：

```sql
grant role `SYSINFO_1` to user_name;
```


分别对应老版本中的数据库 READ 和 WRITE 权限。在升级时，会将老版本对应的权限自动赋予用户。这两个角色也不能直接授予用户或角色，因为其作用对象是具体的数据库。

## 权限管理

TDengine 3.4.0.0 版本开始支持更细粒度的权限控制。权限可以授予用户或角色。

### 系统权限

系统权限用于控制用户对系统级操作的访问。授予系统权限的语法如下。

```sql
grant privileges to {user_name | role_name};
revoke privileges from {user_name | role_name};

privileges : {
    ALL [PRIVILEGES]
  | priv_type [, priv_type] ...
}
```

支持的系统权限类型（priv_type）包括：

| 权限类别 | 权限类型 | 说明 |
| --- | --- | --- |
| 数据库管理 | CREATE DATABASE | 创建数据库 |
| | BALANCE VGROUP | 均衡 VGROUP |
| | BALANCE VGROUP LEADER | 均衡 VGROUP LEADER |
| | MERGE VGROUP | 合并 VGROUP |
| | REDISTRIBUTE VGROUP | 重分配 VGROUP |
| | SPLIT VGROUP | 分裂 VGROUP |
| 函数权限 | CREATE FUNCTION | 创建函数 |
| | DROP FUNCTION | 删除函数 |
| | SHOW FUNCTIONS | 查看函数 |
| 挂载权限 | CREATE MOUNT | 创建挂载 |
| | DROP MOUNT | 删除挂载 |
| | SHOW MOUNTS | 查看挂载 |
| 用户管理 | CREATE USER | 创建用户 |
| | DROP USER | 删除用户 |
| | SET USER SECURITY INFO | 设置用户安全信息 |
| | SET USER AUDIT INFO | 设置用户审计信息 |
| | SET USER BASIC INFO | 设置用户基本信息 |
| | LOCK USER | 锁定用户 |
| | UNLOCK USER | 解锁用户 |
| | SHOW USERS | 查看用户 |
| 角色管理 | CREATE ROLE | 创建角色 |
| | DROP ROLE | 删除角色 |
| | SHOW ROLES | 查看角色 |
| 令牌权限 | CREATE TOKEN | 创建令牌 |
| | DROP TOKEN | 删除令牌 |
| | ALTER TOKEN | 修改令牌 |
| | SHOW TOKENS | 查看令牌 |
| 节点管理 | CREATE NODE | 创建节点 |
| | DROP NODE | 删除节点 |
| | SHOW NODES | 查看节点 |
| 系统参数 | ALTER SECURITY VARIABLE | 修改安全参数 |
| | ALTER AUDIT VARIABLE | 修改审计参数 |
| | ALTER SYSTEM VARIABLE | 修改系统参数 |
| | ALTER DEBUG VARIABLE | 修改调试参数 |
| | SHOW SECURITY VARIABLE | 查看安全参数 |
| | SHOW AUDIT VARIABLE | 查看审计参数 |
| | SHOW SYSTEM VARIABLE | 查看系统参数 |
| | SHOW DEBUG VARIABLE | 查看调试参数 |
| 密钥/密码 | UPDATE KEY | 更新密钥 |
| | CREATE TOTP | 创建 TOTP |
| | DROP TOTP | 删除 TOTP |
| | ALTER PASS | 修改密码 |
| | ALTER SELF PASS | 修改自身密码 |
| 审计管理 | CREATE AUDIT DATABASE | 创建审计数据库 |
| | DROP AUDIT DATABASE | 删除审计数据库 |
| | ALTER AUDIT DATABASE | 修改审计数据库 |
| | USE AUDIT DATABASE | 使用审计数据库 |
| 系统管理 | SHOW TRANSACTIONS | 查看事务 |
| | KILL TRANSACTION | 终止事务 |
| | SHOW CONNECTIONS | 查看连接 |
| | KILL CONNECTION | 终止连接 |
| | SHOW QUERIES | 查看查询 |
| | KILL QUERY | 终止查询 |
| | SHOW GRANTS | 查看授权 |
| | SHOW CLUSTER | 查看集群 |
| | SHOW APPS | 查看应用 |

示例：授予用户 test 创建数据库的权限。

```sql
grant create database to test;
```

示例：授予角色 developer 创建表和查看用户的权限。

```sql
grant create table, show users to developer;
```

### 对象权限

对象权限用于控制用户对数据库对象（库、表、视图等）的访问。

#### 库和表的授权

在 TDengine 中，库和表的权限包括多种类型。从 3.4.0.0 版本开始，支持更细粒度的权限控制。

对某个用户或角色进行库和表访问授权的语法如下。

```sql
grant privileges on priv_level [with filter] to {user_name | role_name};
privileges: {
    ALL [PRIVILEGES]
  | priv_type [, priv_type] ...
}
priv_type: {
    -- 3.3.x.y 旧版本语法，3.4.0.0 及以上版本不再支持
    READ 
  | WRITE
    -- 3.4.0.0 新增的细粒度权限
  | ALTER DATABASE | DROP DATABASE | USE DATABASE | FLUSH DATABASE
  | COMPACT DATABASE | TRIM DATABASE | ROLLUP DATABASE | SCAN DATABASE
  | SSMIGRATE DATABASE | SHOW DATABASES
  | SHOW VNODES | SHOW VGROUPS | SHOW COMPACTS | SHOW RETENTIONS
  | SHOW SCANS | SHOW SSMIGRATES
    -- 表权限
  | CREATE TABLE | DROP TABLE | ALTER TABLE | SHOW TABLES | SHOW CREATE TABLE
  | SELECT [TABLE] | INSERT [TABLE] | DELETE [TABLE]
    -- 索引权限
  | CREATE INDEX | DROP INDEX | SHOW INDEXES
    -- RSMA 权限
  | CREATE RSMA | DROP RSMA | ALTER RSMA | SHOW RSMAS | SHOW CREATE RSMA
    -- TSMA 权限
  | CREATE TSMA | DROP TSMA | SHOW TSMAS
    -- 视图权限
  | CREATE VIEW | DROP VIEW | SHOW VIEWS | SELECT VIEW
    -- 订阅权限
  | CREATE TOPIC | DROP TOPIC | SHOW TOPICS | SHOW CONSUMERS | SHOW SUBSCRIPTIONS
    -- 流计算权限
  | CREATE STREAM | DROP STREAM | SHOW STREAMS | START STREAM | STOP STREAM | RECALCULATE STREAM
}
priv_level: {
    *                    -- 所有数据库（3.4.0.0 新增）
  | *.*                  -- 所有数据库的所有对象
  | dbname               -- 指定数据库（3.4.0.0 新增）
  | dbname.*             -- 指定数据库的所有对象
  | dbname.objname       -- 指定数据库的指定对象
}
```

相关参数说明如下。

- priv_level：可以访问的库或表。`.` 之前为数据库名称，`.` 之后为表名称。`dbname`.`tbname` 的意思是名为 dbname 的数据库中的 tbname 表。`dbname.*` 的意思是名为 dbname 的数据库中的所有表。`*.*` 的意思是所有数据库中的所有表。
- filter：表的过滤条件。

上述 SQL 既可以授权一个库、所有库，也可以授权一个库下的普通表或超级表，还可以通过 `dbname.tbname` 和 `with` 子句的组合授权符合过滤条件的一张超级表下的所有子表。

如下 SQL 将数据库 power 的查询权限授权给用户 test。

```sql
grant read on power.* to test; // 3.3.x.y 语法
// 下述 4 种语法为 3.4.x.y 新增， 并且在功能上等价
grant select on power.* to test;
grant select on table power.* to test;
grant select table on power.* to test;
grant select table on table power.* to test;
```

如下 SQL 将数据库 power 的使用权限授权给用户 test（3.4.0.0 新语法）。

```sql
// 下述 2 种语法在功能上等价
grant use database on database power to test;
grant use on database power to test;
```

如下 SQL 将数据库 power 下超级表 meters 的全部表权限授权给用户 test。

```sql
grant all on power.meters to test;
```

如下 SQL 将超级表 meters 标签值 groupId 等于 1 的子表的全部表权限授权给用户 test。

```sql
grant all on power.meters with groupId=1 to test;
```

3.3.x.y 版本，如果用户被授予了数据库的写权限，那么用户对这个数据库下的所有表都有读和写的权限。但如果一个数据库只有读的权限或甚至读的权限都没有，表的授权会让用户能读或写部分表，详细的授权组合见参考手册。
3.4.0.0 版本开始，不再有数据库的读/写权限，而是细化为数据库及其内部对象的权限。

#### 列级权限（3.4.0.0 新增）

从 3.4.0.0 版本开始，支持列级权限控制，可以限制用户只能访问表中的特定列。

```sql
grant select(col1, col2),insert(ts,col0) on dbname.tbname to user_name;
grant insert(ts,col1, col2) on dbname.tbname to user_name;
```

#### 行级权限（3.4.0.0 新增）

从 3.4.0.0 版本开始，支持基于行的权限控制。与 3.3.x.y 版本的 tag filter 相同，行权限通过 row filter 指定，并且 tag filter 和 row_filter 可以组合使用。

```sql
grant select on dbname.tbname with row_filter to user_name;
```

### 视图授权

在 TDengine 中，视图（view）的权限分为 read、write 和 alter 3 种。它们决定了用户对视图的访问和操作权限。以下是关于视图权限的具体使用规则。

- 视图的创建者和 root 用户默认具备所有权限。这意味着视图的创建者和 root 用户可以查询、写入和修改视图。
- 对其他用户进行授权和回收权限可以通过 grant 和 revoke 语句进行。这些操作只能由具有相应权限的用户执行。
- 视图权限需要单独授权和回收，通过 db.* 进行的授权和回收不包含视图权限。
- 视图可以嵌套定义和使用，对视图权限的校验也是递归进行的。

为了方便视图的分享和使用，TDengine TSDB 引入了视图有效用户（即视图的创建用户）的概念。被授权用户可以使用视图有效用户的库、表及嵌套视图的读写权限。当视图被 replace 后，有效用户也会被更新。

视图操作和权限要求的详细对应关系请见参考手册。

视图授权语法如下。

```sql
grant privileges on [db_name.]view_name to {user_name | role_name}
privileges: {
    ALL [PRIVILEGES]
  | priv_type [, priv_type] ...
}
priv_type: {
    READ
  | WRITE
  | ALTER
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

消息订阅是 TDengine TSDB 独具匠心的设计。为了保障用户订阅信息的安全性，TDengine TSDB 可针对消息订阅进行授权。在使用消息订阅授权功能前，用户需要了解它的如下特殊使用规则。

- 任意用户在拥有读权限的数据库上都可以创建主题。root 用户及具有 CREATE TOPIC 权限的用户可以在对应的数据库上创建主题。
- 每个主题的订阅权限可以独立授权给任何用户，无论其是否具备该数据库的访问权限。
- 删除主题的操作只有 root 用户、具有 DROP TOPIC 权限的用户或该主题的创建者可以执行。
- 只有超级用户、主题的创建者或被显式授权订阅权限的用户才能订阅主题。这些权限设置既保障了数据库的安全性，又保证了用户在有限范围内的灵活操作。

消息订阅授权的 SQL 语法如下。

```sql
grant privileges on priv_level to {user_name | role_name}
privileges : {
    ALL [PRIVILEGES]
  | priv_type [, priv_type] ...
}
priv_type : {
    SUBSCRIBE
}
priv_level : {
    topic_name
}
```

将名为 topic_name 的主题授权给用户 test，SQL 如下。

```sql
grant subscribe on topic_name to test
```

### 查看授权

当企业拥有多个数据库用户时，使用如下命令可以查询具体一个用户所拥有的所有授权。

查看用户权限：

```sql
show user privileges
```

也可以通过查询系统表获取用户权限信息：

```sql
select * from information_schema.ins_user_privileges;
```

查看角色权限（3.4.0.0 新增）：

```sql
show role privileges
```

也可以通过查询系统表获取角色权限信息：

```sql
select * from information_schema.ins_role_privileges;
```

### 撤销授权

由于数据库访问、数据订阅和视图的特性不同，针对具体授权的撤销语法也略有差异。下面列出具体的撤销授权对应不同授权对象的语法。

撤销系统权限的 SQL 如下（3.4.0.0 新增）。

```sql
revoke privileges from {user_name | role_name}
privileges : {
    ALL [PRIVILEGES]
  | priv_type [, priv_type] ...
}
```

撤销数据库访问授权的 SQL 如下。

```sql
revoke privileges on priv_level [with tag_condition] from {user_name | role_name}
privileges : {
    ALL [PRIVILEGES]
  | priv_type [, priv_type] ...
}
priv_type : {
    READ
  | WRITE
  | -- 3.4.0.0 新增的细粒度权限类型
}
priv_level : {
    dbname.tbname
  | dbname.*
  | *.*
}
```

撤销视图授权的 SQL 如下。

```sql
revoke privileges on [db_name.]view_name from {user_name | role_name}
privileges: {
    ALL [PRIVILEGES]
  | priv_type [, priv_type] ...
}
priv_type: {
    READ
  | WRITE
  | ALTER
}
```

撤销数据订阅授权的 SQL 如下。

```sql
revoke privileges on priv_level from {user_name | role_name}
privileges : {
    ALL [PRIVILEGES]
  | priv_type [, priv_type] ...
}
priv_type : {
    SUBSCRIBE
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

### 所有者转移（3.4.0.0 新增）

从 3.4.0.0 版本开始，支持将数据库对象的所有者转移给其他用户或角色。

```sql
alter obj_type obj_name owner to {user_name | role_name}
obj_type : {
    DATABASE
  | TABLE      -- 只支持超级表
  | INDEX
  | RSMA
  | ...
}
```

示例：将数据库 power 的所有者转移给用户 admin。

```sql
alter database power owner to admin
```

## 兼容性说明

- TDengine 3.4.0.0 版本的 RBAC 功能与之前版本的权限管理兼容。旧版本的 `sysinfo` 和 `createdb` 参数仍然有效，系统会自动将其映射到对应的角色权限。
- 从低版本升级至 3.4.0.0 及以上版本时，原有的用户权限会自动迁移到新的权限体系中。
- 升级后不支持降级，升级过程不支持滚动升级，需要停机升级。

## 最佳实践

1. **使用角色简化权限管理**：建议创建几个常见的角色（如开发者、运维人员、数据分析师等），将权限授予角色，再将角色授予用户，而不是逐条为每个用户授予权限。

2. **遵循最小权限原则**：只授予用户完成工作所需的最小权限集合。

3. **定期审计权限**：定期使用 `show user privileges` 和 `show role privileges` 检查用户和角色的权限配置。

4. **三权分立架构**：在对安全性要求较高的场景，建议将 SYSDBA、SYSSEC、SYSAUDIT 角色分配给不同的用户，实现职责分离。
