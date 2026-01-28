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

```text
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

-- 权限作用对象（不指定默认为表）
priv_obj: {
    database           -- 数据库
  | table              -- 表
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

column_list: {
    columnName [,columnName] ...
}

priv_type: {

    #### 库权限(database)

    ALTER [DATABASE] | DROP [DATABASE] | USE [DATABASE] | FLUSH [DATABASE] 
    | COMPACT [DATABASE] | TRIM [DATABASE] | ROLLUP [DATABASE] | SCAN [DATABASE]
    | SSMIGRATE DATABASE | SHOW [DATABASES] 
    | CREATE TABLE | CREATE VIEW | CREATE TOPIC | CREATE STREAM

    #### 表权限(table)

    DROP [TABLE] | ALTER [TABLE] | SHOW CREATE [TABLE] | SHOW [TABLES]
    | SELECT [TABLE] | INSERT [TABLE] | DELETE [TABLE]
    | CREATE INDEX | CREATE TSMA | CREATE RSMA
    
    #### 列权限(table)

    SELECT (column_list) | INSERT (column_list) 

    #### 视图权限(view)

    DROP [VIEW] | ALTER [VIEW] | SHOW [VIEWS] | SELECT VIEW

    #### 索引权限(index)

    DROP [INDEX] | SHOW [INDEXES] | SHOW CREATE [INDEX]

    #### 窗口预聚集权限(tsma)

    DROP [TSMA] | SHOW [TSMAS] | SHOW CREATE [TSMA]

    #### 降采样存储权限(rsma)

    DROP [RSMA] | ALTER [RSMA] | SHOW [RSMAS] | SHOW CREATE [RSMA]

    #### 主题权限(topic)

    DROP [TOPIC] | SHOW [TOPICS] | SHOW CREATE [TOPIC] | SUBSCRIBE [TOPIC]
    | SHOW CONSUMERS | SHOW SUBSCRIPTIONS

    #### 流计算权限(stream)

    DROP [STREAM] | SHOW [STREAMS] | SHOW CREATE [STREAM]
    | START [STREAM] | STOP [STREAM] | RECALCULATE [STREAM]
}
```

#### 数据库权限

数据库权限用于控制用户对数据库的访问和操作。数据库权限可以在不同级别应用。

**权限应用级别：**

- `*`：所有数据库
- `dbname`：指定数据库

**常用权限组合：**

| 权限组合 | 说明 | 使用场景 |
|---------|------|---------|
| USE | 使用（访问）数据库 | 数据库基本访问 |
| ALTER | 修改数据库参数 | 数据库配置调整 |
| DROP | 删除数据库 | 数据库清理、卸载 |
| CREATE TABLE | 创建表 | 表结构创建 |
| CREATE VIEW | 创建视图 | 视图创建 |
| CREATE TOPIC | 创建主题 | 主题创建 |
| CREATE STREAM | 创建流 | 流计算创建 |
| SHOW | 查看数据库信息 | 列出数据库中的对象 |
| FLUSH | 刷新数据库 | 强制持久化数据 |
| COMPACT | 压缩数据库 | 数据库维护、优化 |

**示例 - 数据库权限授权：**

```sql
-- 用户 developer 可以使用 power 数据库
GRANT USE ON DATABASE power TO developer;

-- 用户可以访问所有数据库
GRANT USE ON DATABASE * TO analyst;

-- 用户可以在 power 数据库创建表和视图
GRANT CREATE TABLE, CREATE VIEW ON DATABASE power TO creator;

-- 用户可以修改数据库配置
GRANT ALTER ON DATABASE power TO dba_user;

-- 用户可以查看数据库中的对象列表
GRANT SHOW ON DATABASE power TO viewer;

-- 用户对数据库有完整管理权限
GRANT ALL ON DATABASE power TO admin_user;

-- 撤回用户的数据库权限
REVOKE ALL ON DATABASE power FROM developer;
```

**数据库权限特殊说明：**

- **所有者概念**：数据库创建者默认拥有该数据库的全部权限

#### 表权限

表权限用于控制用户对表的访问和操作。表权限可以在不同级别应用：

**权限应用级别：**

- `*.*`：所有数据库的所有表
- `dbname.*`：指定数据库的所有表
- `dbname.tbname`：指定数据库的指定表

**常用权限组合：**

| 权限组合 | 说明 | 使用场景 |
|---------|------|---------|
| SELECT | 查询表数据 | 数据分析、报表查询 |
| INSERT | 写入表数据 | 数据采集、实时写入 |
| SELECT, INSERT | 读写数据 | 数据处理、ETL 操作 |
| SELECT, INSERT, DELETE | 完整操作 | 数据维护、数据清理 |
| ALTER, DROP | 修改表结构 | 表结构管理、维护 |

**示例 - 表权限授权：**

```sql
-- 用户只能查询 power 库的 meters 表
GRANT SELECT ON power.meters TO analyst;

-- 用户可以向 power 库的所有表写入数据
GRANT INSERT ON power.* TO collector;

-- 用户对 power 库的 devices 表有 SELECT,INSERT,DELETE 操作权限
GRANT SELECT, INSERT, DELETE ON power.devices TO operator;

-- 用户对 power 库的 devices 表有完整操作权限
GRANT ALL ON power.devices to operator;

-- 用户可以修改 power 库中所有表的结构
GRANT ALTER ON power.* TO dba_user;

-- 用户可以创建表和索引
GRANT CREATE TABLE, CREATE INDEX ON power TO dba_user;
```

**表权限策略及优先级：**

- 子表权限 > 超级表权限。如果没有子表权限，子表继承超级表的权限。
- 显式指定表名的权限 > 隐含的通配符 * 权限。
- 表的 owner 拥有该表的完整操作权限，子表继承超级表的 owner。

#### 行权限

行权限用于限制用户只能访问表中满足特定条件的行数据。通过 `WITH` 子句指定行过滤条件。

**语法：**

```sql
GRANT SELECT ON table_name WITH condition TO user_name;
REVOKE SELECT ON table_name FROM user_name;
REVOKE ALL ON table_name FROM user_name;
```

**条件规则：**

- 条件适用于超级表或普通表
- 不能指定子表的条件
- 同一表只能设置一条条件规则
- 多个条件可使用 `AND/OR` 组合
- 可以与 tag 子表条件组合使用
- 可以与列权限组合使用

**示例 - 按数据源分行权限：**

```sql
-- 用户 u1 只能查看来自传感器 sensor_001 的数据
GRANT SELECT ON power.meters WITH source='sensor_001' TO u1;

-- 用户 u2 只能查看温度大于 30°C 的数据
GRANT SELECT ON power.meters WITH temperature > 30 TO u2;

-- 用户 u3 可查看/写入/删除时间范围内的数据
GRANT SELECT, INSERT, DELETE ON power.meters WITH ts >= '2024-01-01' AND ts < '2024-02-01' TO u3;
```

#### 列权限

列权限用于限制用户只能访问表中的特定列，只支持在 `SELECT` 或 `INSERT` 权限中指定列。

**语法：**

```sql
GRANT SELECT (col1, col2, ...) ON table_name TO user_name;
GRANT INSERT (col1, col2, ...) ON table_name TO user_name;
REVOKE SELECT,INSERT ON table_name FROM user_name;
REVOKE ALL ON table_name FROM user_name;
```

**列权限规则：**

- 只适用于 `SELECT` 和 `INSERT` 操作
- 只能指定超级表或普通表，不能指定子表
- 同一表同一操作只能设置一条规则
- 可配合行权限一起使用

**示例 - 按列分权限：**

```sql
-- 用户 analyst 只能查看功率和时间戳列
GRANT SELECT (ts, power) ON power.meters TO analyst;

-- 用户 writer 只能向温度列写入数据
GRANT INSERT (ts, temperature) ON power.meters TO writer;

-- 用户 limited_user 只能查看设备 ID 和状态列
GRANT SELECT (device_id, status) ON power.meters TO limited_user;
```

**示例 - 结合行权限和列权限：**

```sql
-- 用户只能查看特定时间范围内的功率和状态列
GRANT SELECT (ts, power, status) ON power.meters WITH ts >= '2024-01-01' TO analyst;

-- 用户只能向特定来源的传感器操作温度数据
GRANT SELECT, INSERT (ts, temperature), DELETE ON power.meters WITH source='sensor_001' TO collector;
```

**行/列权限优先级：**

- 更新时间靠后的规则生效
- 相同更新时间，用户权限优先于角色权限
- 用户和角色不同类型的权限取并集

### 视图权限

视图权限用于控制用户对视图的访问和操作。视图权限需要单独授权，数据库权限不包含视图权限。

**权限应用级别：**

- `[dbname.]view_name`：指定视图（指定数据库或不指定）

**常用权限组合：**

| 权限 | 说明 | 使用场景 |
|------|------|---------|
| SELECT [VIEW] | 查询视图数据 | 数据分析、报表查询 |
| DROP [VIEW] | 删除视图 | 视图清理和维护 |
| ALTER [VIEW] | 修改视图定义 | 视图结构调整 |
| SHOW [VIEWS] | 查看视图列表 | 查看系统中已有的视图 |
| SHOW CREATE [VIEW] | 查看视图定义 | 了解视图的创建语句 |

**示例 - 视图权限授权：**

```sql
-- 用户 analyst 可以查询 power 库中的视图 meter_stats
GRANT SELECT ON VIEW power.meter_stats TO analyst;

-- 用户可以修改视图定义
GRANT ALTER ON VIEW power.meter_stats TO maintainer;

-- 用户可以查看视图列表和定义
GRANT SHOW, SHOW CREATE ON VIEW power.meter_stats TO viewer;

-- 用户对视图有完整操作权限
GRANT ALL ON VIEW power.meter_stats TO admin_user;

-- 撤回用户的针对该视图的所有权限
REVOKE ALL ON VIEW power.meter_stats FROM analyst;
```

**视图权限特殊说明：**

- **创建权限**：视图创建权通过 `CREATE VIEW` 数据库权限控制
- **所有者权限**：视图创建者默认拥有该视图的全部权限，可使用嵌套视图（视图有效用户概念）
- **嵌套视图**：被授权用户可使用视图有效用户的库、表及嵌套视图的读写权限
- **权限继承**：视图权限需单独授权，通过 `dbname.*` 进行的授权不包含视图权限

**权限优先级：**

- 显式指定视图名的权限 > 通配符权限

### 主题权限

主题权限用于控制用户对消息主题的访问和操作。TDengine 支持对主题进行细粒度权限控制。

**权限应用级别：**

- `*.*`：所有主题
- `dbname.topicname`：指定数据库的指定主题

**常用权限组合：**

| 权限 | 说明 | 使用场景 |
|------|------|---------|
| SUBSCRIBE | 订阅主题 | 数据消费者订阅消息流 |
| SHOW TOPICS | 查看主题列表 | 查看系统中已有的主题 |
| SHOW CREATE TOPIC | 查看主题定义 | 了解主题的创建语句 |
| DROP TOPIC | 删除主题 | 主题清理和维护 |
| SHOW CONSUMERS | 查看消费者 | 监控订阅状态 |
| SHOW SUBSCRIPTIONS | 查看订阅信息 | 了解订阅关系 |

**示例 - 主题权限授权：**

```sql
-- 用户 consumer1 可以订阅 power 数据库中的 device_events 主题
GRANT SUBSCRIBE ON power.device_events TO consumer1;

-- 用户 consumer2 可以订阅所有数据库中的所有主题
GRANT SUBSCRIBE ON *.* TO consumer2;

-- 用户可以查看主题信息
GRANT SHOW ON TOPIC power.* TO viewer;

-- 用户可以查看主题定义和消费者信息
GRANT SHOW CREATE, SHOW CONSUMERS ON TOPIC power.device_events TO inspector;

-- 用户对主题有完整管理权限（仅数据库管理员）
GRANT ALL ON TOPIC power.device_events TO admin_user;

-- 撤回 inspector 拥有的所有主题权限
REVOKE ALL ON TOPIC power.device_events FROM inspector;
```

**主题权限特殊说明：**

- **创建权限**：主题创建权通过 `CREATE TOPIC` 数据库权限控制，任意用户在拥有 `CREATE TOPIC` 权限的数据库上都可以创建主题
- **删除权限**：仅主题创建者和拥有 `DROP TOPIC` 权限的用户可以删除主题
- **订阅权限**：通过 `SUBSCRIBE` 权限独立控制，用户即使没有数据库访问权限，也可被授予订阅权限
- **消费者管理**：拥有 `SHOW CONSUMERS` 权限可查看订阅该主题的消费者信息

**权限优先级：**

- 显式指定主题名的权限 > 通配符 `*` 权限
- 更新时间靠后的规则生效
- 相同更新时间，用户权限优先于角色权限

### 流计算权限

流计算权限用于控制用户对流（Stream）的访问和操作。

**权限应用级别：**

- `*.*`：所有流
- `dbname.*`：指定数据库的所有流
- `dbname.stream_name`：指定数据库的指定流

**常用权限组合：**

| 权限 | 说明 | 使用场景 |
|------|------|---------|
| SHOW [STREAMS] | 查看流列表 | 查看系统中已有的流 |
| SHOW CREATE [STREAM] | 查看流定义 | 了解流的创建语句和配置 |
| DROP [STREAM] | 删除流 | 流清理和维护 |
| START [STREAM] | 启动流 | 启动数据处理 |
| STOP [STREAM] | 停止流 | 暂停数据处理 |
| RECALCULATE [STREAM] | 重新计算流 | 流数据重新处理 |

**示例 - 流计算权限授权：**

```sql
-- 用户 processor 可以查看 power 库中的所有流
GRANT SHOW ON STREAM power.* TO processor;

-- 用户可以启动和停止特定流
GRANT START, STOP ON STREAM power.realtime_agg TO operator;

-- 用户可以查看流定义和操作流
GRANT SHOW CREATE, START, STOP ON STREAM power.* TO manager;

-- 用户对流有完整管理权限
GRANT ALL ON STREAM power.realtime_agg TO admin_user;

-- 撤回用户的启动权限
REVOKE START ON STREAM power.realtime_agg FROM operator;

-- 撤回用户针对该流的所有权限
REVOKE ALL ON STREAM power.realtime_agg FROM operator;
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
- **管理权限**：可修改对象结构、删除对象

---

## 权限查看

```sql
-- 查看用户权限（3.3.x.y+）
SHOW USER PRIVILEGES
SELECT * FROM information_schema.ins_user_privileges

taos> show user privileges;
 user_name |    priv_type        |  priv_scope | db_name | table_name | condition |  notes | columns |        update_time         |
===================================================================================================================================
 u1        | CREATE DATABASE     | CLUSTER     |         |            |           |        |         |                            |
 u1        | SUBSCRIBE           | TOPIC       | d0      | topic1     |           |        |         |                            |
 u1        | USE DATABASE        | DATABASE    | d0      |            |           |        |         |                            |
 u1        | CREATE TABLE        | DATABASE    | d0      |            |           |        |         |                            |
 u1        | ALTER               | VIEW        | d0      | v1         |           |        |         |                            |
 u1        | SELECT VIEW         | VIEW        | d0      | v1         |           |        |         |                            |
 u1        | SELECT              | TABLE       | d0      | stb0       |           |        | ts,c0   | 2026-01-28 14:39:56.960258 |
 u1        | INSERT              | TABLE       | d0      | stb0       |           |        | ts,c0   | 2026-01-28 14:39:56.977788 |
 u2        | CREATE DATABASE     | CLUSTER     |         |            |           |        |         |                            |

-- 查看角色权限（3.4.0.0+）
SHOW ROLE PRIVILEGES
SELECT * FROM information_schema.ins_role_privileges
```

```text
taos> show role privileges;
 role_name      |    priv_type        |  priv_scope | db_name | table_name | condition |  notes | columns |     update_time       |
 ===================================================================================================================================
 SYSSEC         | SHOW CREATE         | TABLE       | \*      | \*         |           |        |         |                       |
 SYSSEC         | SHOW                | VIEW        | \*      | \*         |           |        |         |                       |
 SYSSEC         | SHOW CREATE         | VIEW        | \*      | \*         |           |        |         |                       |
 SYSSEC         | SHOW                | TSMA        | \*      | \*         |           |        |         |                       |
 SYSSEC         | SHOW CREATE         | TSMA        | \*      | \*         |           |        |         |                       |
 SYSAUDIT_LOG   | USE AUDIT DATABASE  | CLUSTER     |         |            |           |        |         |                       |
 SYSAUDIT_LOG   | CREATE AUDIT TABLE  | CLUSTER     |         |            |           |        |         |                       |
 SYSAUDIT_LOG   | INSERT AUDIT TABLE  | CLUSTER     |         |            |           |        |         |                       |
```

---

## 最佳实践

### 3.3.x.y 版本

1. 使用 root 创建业务用户，按最小权限原则授权
2. 只读应用仅授予 READ 权限
3. 写入应用仅授予 WRITE 权限
4. 利用标签过滤限制用户访问特定子表

### 3.4.0.0+ 版本

1. **立即分离三权限**：初始化后，将 SYSDBA/SYSSEC/SYSAUDIT 分配给不同用户
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
