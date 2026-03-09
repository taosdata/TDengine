---
toc_max_heading_level: 4
title: 权限管理
---

TDengine TSDB 中的权限管理分为 [用户管理](../user)、数据库授权管理以及消息订阅授权管理，本节重点说明数据库授权和订阅授权。
授权管理仅在 TDengine TSDB 企业版中可用，请联系 TDengine TSDB 销售团队。授权语法在 3.3.x.y 及之前的社区版可用，但不起作用，在 3.4.0.0 及后续版本，授权语法执行报错。
3.4.0.0 开始，TDengine 企业版通过基于角色的访问控制（RBAC）实现了三权分立机制，权限部分改动较大，部分语法不再兼容。本文后续部分，会分别说明。

## 版本对比

| 特性 | 3.3.x.y- | 3.4.0.0+ |
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

## 权限管理 - 3.3.x.y 及之前版本

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

- priv_level 格式中 "." 之前为数据库名称，"." 之后为表名称，意思为表级别的授权控制。如果 "." 之后为 "\*"，意为 "." 前所指定的数据库中的所有表
- "dbname.\*" 意思是名为 "dbname" 的数据库中的所有表
- "\*.\*" 意思是所有数据库名中的所有表

### 数据库权限说明

对 root 用户和普通用户的权限的说明如下表

| 用户     | 描述                               | 权限说明                        |
| -------- | --------------------------------- | -- |
| 超级用户 | 只有 root 是超级用户               |<br/>DB 外部：所有操作权限，例如 user、dnode、udf、qnode 等的 CRUD <br/>DB 权限：包括创建、删除、修改 Option、移动 Vgruop、读、写、Enable/Disable 用户  |
| 普通用户 | 除 root 以外的其它用户均为普通用户 | <br/>在可读的 DB 中：普通用户可以进行读操作 select、describe、show、subscribe <br/>在可写 DB 的内部，用户可以进行写操作，创建、删除、修改超级表，创建、删除、修改子表，创建、删除、修改 topic。写入数据 <br/>被限制系统信息时，不可进行如下操作 show dnode、mnode、vgroups、qnode、snode、修改用户包括自身密码、`show db` 时只能看到自己的 db，并且不能看到 vgroups、副本、cache 等信息 <br/>无论是否被限制系统信息，都可以管理 udf，可以创建 DB、自己创建的 DB 具备所有权限、非自己创建的 DB，参照读、写列表中的权限 |

## 消息订阅授权

任意用户都可以在自己拥有读权限的数据库上创建 topic。超级用户 root 可以在任意数据库上创建 topic。每个 topic 的订阅权限都可以被独立授权给任何用户，不管该用户是否拥有该数据库的访问权限。删除 topic 只能由 root 用户或者该 topic 的创建者进行。topic 只能由超级用户、topic 的创建者或者被显式授予 subscribe 权限的用户订阅。

具体的 SQL 语法如下：

```sql
GRANT SUBSCRIBE ON topic_name TO user_name

REVOKE SUBSCRIBE ON topic_name FROM user_name
```

## 基于标签的授权（表级授权）

从 v3.0.5.0 开始，我们支持按标签授权某个超级表中部分特定的子表。具体的 SQL 语法如下。

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

|                | **表无授权**   | **表读授权**                        | **表读授权有标签条件**                              | **表写授权**                        | **表写授权有标签条件**   |
| -------------- | ------------- | --------------------------------- | ------------------------------------------------- | ---------------------------------- | -------------------- |
| **数据库无授权** | 无授权         | 对此表有读权限，对数据库下的其他表无权限 | 对此表符合标签权限的子表有读权限，对数据库下的其他表无权限  | 对此表有写权限，对数据库下的其他表无权限 | 对此表符合标签权限的子表有写权限，对数据库下的其他表无权限 |
| **数据库读授权** | 对所有表有读权限 | 对所有表有读权限                    | 对此表符合标签权限的子表有读权限，对数据库下的其他表有读权限 | 对此表有写权限，对所有表有读权限        | 对此表符合标签权限的子表有写权限，所有表有读权限           |
| **数据库写授权** | 对所有表有写权限 | 对此表有读权限，对所有表有写权限       | 对此表符合标签权限的子表有读权限，对所有表有写权限         | 对所有表有写权限                      | 对此表符合标签权限的子表有写权限，数据库下的其他表有写权限 |

## 查看用户授权

使用下面的命令可以显示一个用户所拥有的授权：

```sql
show user privileges 
```

## 撤销授权

1. 撤销数据库访问的授权

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

2. 撤销数据订阅的授权

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

---

## 权限管理 - 3.4.0.0 及后续版本

### 三权分立概述

从 3.4.0.0 开始，TDengine 企业版通过基于角色的访问控制（RBAC）实现了三权分立机制，将 root 用户的管理权限拆分为 SYSDBA、SYSSEC 和 SYSAUDIT 三种系统管理权限，从而实现权限的有效隔离和制衡。

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
  priv_type [, priv_type] ...
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
  | SET USER BASIC INFORMATION | SET USER SECURITY INFORMATION | SET USER AUDIT INFORMATION
  | UNLOCK USER | LOCK USER | SHOW USERS | SHOW USERS SECURITY INFORMATION

    -- 令牌权限
  | CREATE TOKEN | DROP TOKEN | ALTER TOKEN | SHOW TOKENS

    -- 角色权限
  | CREATE ROLE | DROP ROLE | SHOW ROLES | LOCK ROLE | UNLOCK ROLE

    -- 密钥权限
  | CREATE TOTP | DROP TOTP
  
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
  | READ INFORMATION_SCHEMA BASIC | READ INFORMATION_SCHEMA PRIVILEGED
  | READ INFORMATION_SCHEMA SECURITY | READ INFORMATION_SCHEMA AUDIT 
  | READ PERFORMANCE_SCHEMA BASIC | READ PERFORMANCE_SCHEMA PRIVILEGED
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
    ALTER | DROP
  | SELECT [(column_list)] | INSERT [(column_list)] | DELETE
  | CREATE TABLE | CREATE VIEW | CREATE INDEX | CREATE TSMA | CREATE RSMA | CREATE TOPIC | CREATE STREAM
  | USE | SHOW | SHOW CREATE
  | FLUSH | COMPACT | TRIM | ROLLUP | SCAN | SSMIGRATE
  | SUBSCRIBE | SHOW CONSUMERS | SHOW SUBSCRIPTIONS
  | START | STOP | RECALCULATE
}
```

#### 对象类型与权限类型对应关系

不同的对象类型支持的权限类型不同，具体对应关系如下：

| 权限类型 | database | table | view | index | tsma | rsma | topic | stream |
|---------|:--------:|:-----:|:----:|:-----:|:----:|:----:|:-----:|:------:|
| ALTER | ✓ | ✓ | ✓ | | | ✓ | | |
| DROP | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| SELECT [(column_list)] | | ✓ | ✓ | | | | | |
| INSERT [(column_list)] | | ✓ | | | | | | |
| DELETE | | ✓ | | | | | | |
| CREATE TABLE | ✓ | | | | | | | |
| CREATE VIEW | ✓ | | | | | | | |
| CREATE INDEX | | ✓ | | | | | | |
| CREATE TSMA | | ✓ | | | | | | |
| CREATE RSMA | | ✓ | | | | | | |
| CREATE TOPIC | ✓ | | | | | | | |
| CREATE STREAM | ✓ | | | | | | | |
| USE | ✓ | | | | | | | |
| SHOW | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| SHOW CREATE | ✓ | ✓ | ✓ | | | ✓ | | |
| FLUSH | ✓ | | | | | | | |
| COMPACT | ✓ | | | | | | | |
| TRIM | ✓ | | | | | | | |
| ROLLUP | ✓ | | | | | | | |
| SCAN | ✓ | | | | | | | |
| SSMIGRATE | ✓ | | | | | | | |
| SUBSCRIBE | | | | | | | ✓ | |
| SHOW CONSUMERS | | | | | | | ✓ | |
| SHOW SUBSCRIPTIONS | | | | | | | ✓ | |
| START | | | | | | | | ✓ |
| STOP | | | | | | | | ✓ |
| RECALCULATE | | | | | | | | ✓ |

**说明：**

- 使用 `GRANT` 授权时，需要通过 `ON [priv_obj]` 指定对象类型，系统会自动校验该权限是否适用于指定的对象类型。
- `[(column_list)]` 表示可选的列名列表，用于实现列级权限控制。`view` 只支持 `SELECT`，不支持指定列名列表。
- 同一表相同类型的权限只能设置一条规则。
- 撤销权限时，精确匹配 priv_level，不支持递归撤销。例如，`revoke select on d0.* from u1` 只撤销 `d0.*` 对应的权限，不撤销 `d0.t1` 对应的权限。

#### 用户权限和角色权限说明

- 大多数情况下，用户权限和角色权限叠加生效，即取并集。
- 针对行/列权限，只取一条规则，即不取并集也不取交集。如果用户和角色均设置了某一类型的行/列权限，优先取更新时间靠后的，更新时间相同则优先取用户权限。

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

-- 用户可以通过 show databases 命令查看数据库对象 power
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
REVOKE SELECT ON table_name FROM user_name; // revoke 时无论是否指定 condition，均会撤销对应表的 select 权限。
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
- 同一表相同类型的操作只能设置一条规则
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

-- 用户可以查看 power 库的所有主题信息
GRANT SHOW ON TOPIC power.* TO viewer;

-- 用户可以查看主题定义和消费者信息
GRANT SHOW CREATE, SHOW CONSUMERS ON TOPIC power.device_events TO inspector;

-- 用户对主题有完整管理权限
GRANT ALL ON TOPIC power.device_events TO admin_user;

-- 撤回 inspector 拥有的所有主题权限
REVOKE ALL ON TOPIC power.device_events FROM inspector;
```

**主题权限特殊说明：**

- **创建权限**：主题创建权通过 `CREATE TOPIC` 数据库权限控制，任意用户在拥有 `CREATE TOPIC` 权限的数据库上都可以创建主题
- **删除权限**：仅主题创建者和拥有 `DROP TOPIC` 权限的用户可以删除主题
- **消费者管理**：拥有 `SHOW CONSUMERS` 权限可查看订阅该主题的消费者信息

**权限优先级：**

- 显式指定主题名的权限 > 通配符 `*` 权限

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
-- 查看用户权限（3.4.0.0+）
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
 SYSSEC         | SHOW CREATE         | TABLE       |  *      |  *         |           |        |         |                       |
 SYSSEC         | SHOW                | VIEW        |  *      |  *         |           |        |         |                       |
 SYSSEC         | SHOW CREATE         | VIEW        |  *      |  *         |           |        |         |                       |
 SYSSEC         | SHOW                | TSMA        |  *      |  *         |           |        |         |                       |
 SYSSEC         | SHOW CREATE         | TSMA        |  *      |  *         |           |        |         |                       |
 SYSAUDIT_LOG   | USE AUDIT DATABASE  | CLUSTER     |         |            |           |        |         |                       |
 SYSAUDIT_LOG   | CREATE AUDIT TABLE  | CLUSTER     |         |            |           |        |         |                       |
 SYSAUDIT_LOG   | INSERT AUDIT TABLE  | CLUSTER     |         |            |           |        |         |                       |
```

---

## 最佳实践

### 3.3.x.y- 版本

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

| 特性 | 3.3.x.y- | 3.4.0.0+ |
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
