---
sidebar_label: 用户和权限
title: 用户和权限管理
toc_max_heading_level: 4
---

TDengine TSDB 默认仅配置了一个 root 用户，该用户拥有最高权限。TDengine TSDB 支持对系统资源、库、表、视图和主题的访问权限控制。本节介绍 TDengine 中的用户和权限管理。

:::info
用户和权限管理是 TDengine Enterprise 特有功能。
:::

## 版本对比

| 特性 | 3.3.x.y | 3.4.0.0+ |
|------|---------|----------|
| 基础用户管理 | ✓ | ✓ |
| RBAC 角色管理 | ✗ | ✓ |
| 三权分立（SYSDBA/SYSSEC/SYSAUDIT） | ✗ | ✓ |
| 细粒度权限 | ✗ | ✓ |
| 审计库权限 | ✗ | ✓ |
| 表权限 | ✓ | ✓ |
| 行权限 | ✗ | ✓ |
| 列权限 | ✗ | ✓ |
---



## 用户管理

### 创建用户

创建用户的语法如下。

```sql
create user user_name pass'password' [sysinfo {1|0}] [createdb {1|0}]
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
alter user test enable 0;
```

### 删除用户

删除用户的 SQL 如下。

```sql
drop user user_name;
```

## 权限管理 - 3.3.x.y 版本

仅 root 用户可以管理用户、节点、vnode、qnode、snode 等系统信息，包括查询、新增、删除和修改。

### 库和表的授权

在 TDengine TSDB 中，库和表的权限分为 read 和 write 两种。这些权限可以单独授予，也可以同时授予用户。

- read 权限：拥有 read 权限的用户仅能查询库或表中的数据，而无法对数据进行修改或删除。这种权限适用于需要访问数据但不需要对数据进行写入操作的场景，如数据分析师、报表生成器等。
- write 权限：拥有 write 权限的用户可以向库或表中写入数据。这种权限适用于需要对数据进行写入操作的场景，如数据采集器、数据处理器等。如果只拥有 write 权限而没有 read 权限，则只能写入数据但不能查询数据。

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

- resources：可以访问的库或表。`.` 之前为数据库名称，`.` 之后为表名称。`dbname.tbname` 的意思是名为 dbname 的数据库中的 tbname 表必须为普通表或超级表。`dbname.*` 的意思是名为 dbname 的数据库中的所有表。`*.*` 的意思是所有数据库中的所有表。
- tag_filter：超级表的过滤条件。

上述 SQL 既可以授权一个库、所有库，也可以授权一个库下的普通表或超级表，还可以通过 `dbname.tbname` 和 `with` 子句的组合授权符合过滤条件的一张超级表下的所有子表。

如下 SQL 将数据库 power 的 read 权限授权给用户 test。

```sql
grant read on power to test;
```

如下 SQL 将数据库 power 下超级表 meters 的全部权限授权给用户 test。

```sql
grant all on power.meters to test;
```

如下 SQL 将超级表 meters 离标签值 groupId 等于 1 的子表的 write 权限授权给用户 test。

```sql
grant all on power.meters with groupId=1 to test;
```

如果用户被授予了数据库的写权限，那么用户对这个数据库下的所有表都有读和写的权限。但如果一个数据库只有读的权限或甚至读的权限都没有，表的授权会让用户能读或写部分表，详细的授权组合见参考手册。

### 视图授权

在 TDengine TSDB 中，视图（view）的权限分为 read、write 和 alter 3 种。它们决定了用户对视图的访问和操作权限。以下是关于视图权限的具体使用规则。

- 视图的创建者和 root 用户默认具备所有权限。这意味着视图的创建者和 root 用户可以查询、写入和修改视图。
- 对其他用户进行授权和回收权限可以通过 grant 和 revoke 语句进行。这些操作只能由 root 用户执行。
- 视图权限需要单独授权和回收，通过 db.* 进行的授权和回收不包含视图权限。
- 视图可以嵌套定义和使用，对视图权限的校验也是递归进行的。

为了方便视图的分享和使用，TDengine TSDB 引入了视图有效用户（即视图的创建用户）的概念。被授权用户可以使用视图有效用户的库、表及嵌套视图的读写权限。当视图被 replace 后，有效用户也会被更新。

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
grant read on power.view_name to test;
```

在数据库 power 库下将视图 view_name 的全部权限授权给用户 test，SQL 如下。

```sql
grant all on power.view_name to test;
```

### 消息订阅授权

消息订阅是 TDengine TSDB 独具匠心的设计。为了保障用户订阅信息的安全性，TDengine TSDB 可针对消息订阅进行授权。在使用消息订阅授权功能前，用户需要了解它的如下特殊使用规则。

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
grant subscribe on topic_name to test;
```

### 查看授权

当企业拥有多个数据库用户时，使用如下命令可以查询具体一个用户所拥有的所有授权，SQL 如下。

```sql
show user privileges
```

### 撤销授权

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
revoke all on power from test;
```

撤销用户 test 对于数据库 power 的视图 view_name 的读授权的 SQL 如下。

```sql
revoke read on power.view_name from test;
```

撤销用户 test 对于消息订阅 topic_name 的 subscribe 授权的 SQL 如下。

```sql
revoke subscribe on topic_name from test;
```

---

## 权限管理 - 3.4.0.0+ 版本

### 三权分立概述

从 3.4.0.0 开始，TDengine 企业版通过基于角色的访问控制（RBAC）实现了严格的三权分立机制，将 root 用户的管理权限拆分为 SYSDBA、SYSSEC 和 SYSAUDIT 三种系统管理权限，从而实现权限的有效隔离和制衡。

| 角色 | 全称 | 职责 |
|------|------|------|
| **SYSDBA** | 数据库管理员 | 数据库日常运维、系统管理、用户角色创建。不能执行与 SYSSEC/SYSAUDIT 相关操作 |
| **SYSSEC** | 数据库安全员 | 用户角色权限授予/撤销、安全策略制定 |
| **SYSAUDIT** | 数据库审计员 | 独立审计监督、审计数据库管理、审计日志查看。不能查看业务数据 |

**关键约束：**

```
❌ 不允许将 SYSDBA/SYSSEC/SYSAUDIT 中任意两个同时授予同一用户
✓ 系统允许多个用户拥有同一系统角色
✓ 系统管理角色权限范围不可通过命令行更改，支持升级自动更新
```

### root 用户与系统角色

**初始状态：** root 用户默认拥有 SYSDBA、SYSSEC、SYSAUDIT 的全部权限

**推荐做法：** 在系统初始配置后，立即分离角色，之后停用 root 进行日常操作

```sql
-- 创建专用管理员
CREATE USER dba_user PASS 'DbaPass123!@#';
CREATE USER sec_user PASS 'SecPass123!@#';
CREATE USER audit_user PASS 'AuditPass123!@#';

-- 分离授权
GRANT ROLE `SYSDBA` TO dba_user;
GRANT ROLE `SYSSEC` TO sec_user;
GRANT ROLE `SYSAUDIT` TO audit_user;
```

### 数据库管理员（SYSDBA）

**职责：**

- 数据库的日常运维、系统管理
- 创建和管理用户、角色
- 管理数据库、表、索引等对象
- 管理节点、流计算、订阅等系统资源

**限制：**

- 不能授予 SYSSEC/SYSAUDIT 权限
- 不能执行与审计数据库相关的操作
- 默认不拥有查看业务数据的权限（但可查看元数据）

### 数据库安全员（SYSSEC）

**职责：**

- 用户与角色权限管理（除 SYSDBA/SYSAUDIT 外）
- 安全参数配置
- TOTP 密钥管理
- 用户安全信息设置

**权限示例：**

```sql
GRANT/REVOKE SYSSEC PRIVILEGE
ALTER SECURITY VARIABLE
CREATE TOTP / DROP TOTP
SET USER SECURITY INFORMATION
READ INFORMATION_SCHEMA SECURITY
```

### 数据库审计员（SYSAUDIT）

**职责：**

- 独立审计监督
- 审计数据库管理
- 审计日志查看
- 审计相关参数配置

**权限示例：**

```sql
GRANT/REVOKE SYSAUDIT PRIVILEGE
ALTER/DROP/USE AUDIT DATABASE
SELECT AUDIT TABLE
SET USER AUDIT INFORMATION
READ INFORMATION_SCHEMA AUDIT
```

### 角色管理

#### 创建角色

```sql
CREATE ROLE [IF NOT EXISTS] role_name;
```

**约束：**

- 创建者需具有 CREATE ROLE 权限
- 角色名长度 1-63 字符
- 角色名不能与已存在用户名重名

#### 删除和查看角色

```sql
-- 删除角色
DROP ROLE [IF EXISTS] role_name;

-- 查看角色列表
SHOW ROLES;
SELECT * FROM information_schema.ins_roles;

-- 查看角色权限
SHOW ROLE PRIVILEGES;
SELECT * FROM information_schema.ins_role_privileges;
```

#### 角色禁用/启用

```sql
LOCK ROLE role_name;
UNLOCK ROLE role_name;
```

#### 角色授予和回收

```sql
GRANT ROLE role_name TO user_name;
REVOKE ROLE role_name FROM user_name;
```

### 系统内置角色

除三大系统管理角色外，TDengine 还提供下述系统内置角色：

| 角色 | 说明 |
|------|------|
| **SYSAUDIT_LOG** | 可在审计库建表、写入数据，但不能删表/改表/删数据。不能与 SYSDBA/SYSSEC/SYSAUDIT 同时授予某一用户 |
| **SYSINFO_0** | 对应 SYSINFO=0 权限，查看基础系统信息 |
| **SYSINFO_1** | 对应 SYSINFO=1 权限，查看更多系统信息，可修改自身密码 |

### 系统权限管理

3.4.0.0+ 新增细粒度系统权限：

```sql
-- 授予系统权限
GRANT privileges TO {user_name | role_name};
-- 撤销系统权限
REVOKE privileges FROM {user_name | role_name};

privileges: {
    ALL [PRIVILEGES]
  | priv_type [, priv_type] ...
}

priv_type: {
    -- 数据库权限
    CREATE DATABASE

    -- 函数权限
  | CREATE FUNCTION | DROP FUNCTION | SHOW FUNCTIONS

    -- 挂载权限
  | CREATE MOUNT | DROP MOUNT | SHOW MOUNTS

    -- 用户权限
  | CREATE USER | DROP USER | ALTER USER
  | SET USER BASIC INFORMATION | SET USER SECURITY INFORMATION | SET USER AUDIT INFORMATION |
  | UNLOCK USER | LOCK USER | SHOW USERS

    -- 令牌权限
  | CREATE TOKEN | DROP TOKEN | ALTER TOKEN | SHOW TOKENS

    -- 角色权限
  | CREATE ROLE | DROP ROLE | SHOW ROLES | LOCK ROLE | UNLOCK ROLE

    -- 密钥权限
  | CREATE TOTP | DROP TOTP | 
  
    -- 密码权限
  | ALTER PASS | ALTER SELF PASS

    -- 节点权限
  | CREATE NODE | DROP NODE | SHOW NODES

    -- 权限授予回收权限
  ｜GRANT PRIVILEGE ｜ REVOKE PRIVILEGE | SHOW PRIVILEGES

    -- 系统参数权限
  | ALTER SECURITY VARIABLE | ALTER AUDIT VARIABLE   | ALTER SYSTEM VARIABLE | ALTER DEBUG VARIABLE
  | SHOW SECURITY VARIABLES | SHOW AUDIT VARIABLES | SHOW SYSTEM VARIABLES | SHOW DEBUG VARIABLES

    -- 系统管理权限
  | READ INFORMATION SCHEMA BASIC | READ INFORMATION SCHEMA PRIVILEGED
  | READ INFORMATION SCHEMA SECURITY | READ INFORMATION SCHEMA AUDIT 
  | READ PERFORMANCE SCHEMA BASIC | READ PERFORMANCE SCHEMA PRIVILEGED
  | SHOW TRANSACTIONS | KILL TRANSACTION
  | SHOW CONNECTIONS | KILL CONNECTION
  | SHOW QUERIES | KILL QUERY
  | SHOW GRANTS | SHOW CLUSTER | SHOW APPS

}
```

### 对象权限管理

3.4.0.0+ 支持更细粒度的对象权限：

```sql
-- 授予对象权限
GRANT privileges ON [priv_obj] priv_level [WITH condition] TO {user_name | role_name}

-- 撤销对象权限
REVOKE privileges ON [priv_obj] priv_level [WITH condition] FROM {user_name | role_name}

-- 权限作用对象
priv_obj: {
    database           -- 数据库
  | table              -- 表(不指定默认为表)
  | view               -- 视图
  | index              -- 索引
  | tsma               -- 窗口预聚集
  | rsma               -- 降采样存储
  | topic              -- 主题
  | stream             -- 流计算
}

priv_level: {
    *                  -- 所有库
  | dbname             -- 指定库
  | *.*                -- 所有库，所有对象
  | dbname.*           -- 指定库，所有对象
  | dbname.objname     -- 指定库，指定对象
}

privileges: {
    ALL [PRIVILEGES]
  | priv_type [, priv_type] ...
}

priv_type: {

    #### 库权限

    ALTER [DATABASE] | DROP [DATABASE] | USE [DATABASE]
    FLUSH [DATABASE] | COMPACT [DATABASE] | TRIM [DATABASE]
    ROLLUP [DATABASE] | SCAN [DATABASE] | SHOW [DATABASES]

    #### 表权限

    CREATE TABLE | DROP TABLE | ALTER TABLE
    SHOW TABLES | SHOW CREATE TABLE
    SELECT TABLE | INSERT TABLE | DELETE TABLE

    #### 视图权限

    CREATE VIEW | DROP VIEW | SHOW VIEWS | SELECT VIEW

    #### 索引权限

    CREATE INDEX | DROP INDEX | SHOW INDEXES

    #### 窗口预聚集权限

    CREATE TSMA | DROP TSMA SHOW TSMAS

    #### 降采样存储权限

    CREATE RSMA | DROP RSMA | ALTER RSMA | SHOW RSMAS | SHOW CREATE RSMA

    #### 订阅权限

    CREATE TOPIC | DROP TOPIC | SHOW TOPICS | SUBSCRIBE
    SHOW CONSUMERS | SHOW SUBSCRIPTIONS

    #### 流计算权限

    CREATE STREAM | DROP STREAM | SHOW STREAMS
    START STREAM | STOP STREAM | RECALC STREAM
}
```

### 审计数据库

3.4.0.0+ 专门支持审计数据库：

**特性：**

- 系统仅允许一个审计库
- 审计库通过 `is_audit` 属性标识（非固定名称）
- 仅 SYSAUDIT 可删除和修改审计库
- 为防止误删库，新增了 allow_drop 属性。审计库默认为 0，普通库默认为 1。删除审计库时，需要将 allow_drop 属性修改为 1。

**权限限制：**

```text
❌ 任何人不允许删除审计表
❌ 任何人不允许修改审计表
❌ 任何人不允许删除审计表中的数据
✓ 仅 SYSAUDIT_LOG 角色可向审计库写入数据
✓ 仅 SYSAUDIT 角色可向查看审计库中的表数据
```

### 所有者（Owner）概念

3.4.0.0+ 明确了对象所有者的权限：

- **所有者**：数据库对象的创建者或被转移所有权的接收者
- **隐含权限**：所有者对该对象拥有无需授权的全量权限
- **管理权**：可修改对象结构、删除对象

---

## 权限查看

```sql
-- 查看用户权限（3.3.x.y+）
SHOW USER PRIVILEGES
SELECT * FROM information_schema.ins_user_privileges

-- 查看角色权限（3.4.0.0+）
SHOW ROLE PRIVILEGES
SELECT * FROM information_schema.ins_role_privileges
```

---

## 最佳实践

### 3.3.x.y 版本

1. 使用 root 创建业务用户，按最小权限原则授权
2. 只读应用仅授予 READ 权限
3. 写入应用仅授予 WRITE 权限
4. 利用标签过滤限制用户访问特定子表

### 3.4.0.0+ 版本

1. **立即分离三权**：初始化后，将 SYSDBA/SYSSEC/SYSAUDIT 分配给不同用户
2. **禁用 root 日常操作**：配置完成后，不再使用 root 进行日常运维
3. **使用角色简化权限**：创建通用角色，授权给用户

**示例 - 创建只读分析角色：**

```sql
CREATE ROLE analyst_role;
GRANT SHOW,SELECT ON power.* TO analyst_role;
GRANT SHOW,USE on database power TO analyst_role;
GRANT ROLE analyst_role TO analyst_user;
```

**示例 - 创建数据写入角色：**

```sql
CREATE ROLE writer_role;
GRANT INSERT ON power.* TO writer_role;
GRANT SHOW,USE,CREATE TABLE ON database power TO writer_role;
GRANT ROLE writer_role TO writer_user;
```

**示例 - 安全审计配置：**

```sql
-- 创建审计库
CREATE DATABASE audit_db KEEP 36500d IS_AUDIT 1 ENCRYPT_ALGORITHM 'SM4-CBC' WAL_LEVEL 2;

-- 创建审计员
CREATE USER audit_user PASS 'AuditPass123!@#';
GRANT ROLE `SYSAUDIT` TO audit_user;

-- 创建审计日志角色（用于应用写入）
CREATE ROLE audit_logger;
GRANT ROLE `SYSAUDIT_LOG` TO audit_logger;
```

---

## 兼容性与升级

| 特性 | 3.3.x.y | 3.4.0.0+ |
|------|---------|----------|
| CREATE/ALTER/DROP USER | ✓ | ✓ |
| GRANT/REVOKE READ/WRITE | ✓ | ✗ |
| 视图/订阅权限 | ✓ | ✓ |
| 角色管理 | ✗ | ✓ |
| 三权分立 | ✗ | ✓ |
| 细粒度权限 | ✗ | ✓ |
| 审计数据库 | ✗ | ✓ |

**升级说明：**

- ✓ 支持从低版本停机后自动升级到 3.4.0.0+
- ✗ 不支持滚动升级
- ✗ 升级后无法降级
