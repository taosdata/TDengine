# 访问控制 RS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-09-10 | - | 0.1 | 关胜亮 | 新建 |
| 2025-10-14 | 2025-10-14 | 1.0 | 关胜亮 | 按评审记录修改 |

## 2. 引言

### 2.1 术语与缩写名词

### 2.2 相关文档资料

JIRA [TS-7232](https://jira.taosdata.com:18080/browse/TS-7232)

### 2.3 优先级要求

高

### 2.4 版本要求

1. 企业版支持
2. 社区版不支持

## 3. 需求目标

实施最小权限原则，严格管理用户对数据库的访问权限。
1. 基于角色的访问控制，系统管理员、安全管理员、审计员
2. 表、列级权限：需增加子表、普通表、列的权限控制
3. 读写权限细化：仅 read/write，需补充 alter、insert、delete等操作权限
4. 权限定期审查（如每季度一次）
5. 实现操作系统和数据库系统特权用户的权限分离
6. 系统管理员用户名允许修改
7. Root 禁止访问私有数据

## 4. 功能需求

### 4.1 权限列表

#### 4.1.1 库权限

对于非系统库（具体指 information_schema，performance_schema，审计数据库），有如下权限
1. CREATE DATABASE
2. ALTER DATABASE
3. DROP DATABASE
4. USE DATABASE
5. COMPACT DATABASE
6. TRIM DATABASE
7. FLUSH DATABASE
8. SHOW DATABASES
9. SHOW VNODES
10. SHOW VGROUPS
11. BALANCE VGROUP
12. REDISTRIBUTE VGROUP

#### 4.1.2 表权限

如下权限，包括普通表、超级表、子表、虚拟表
1. CREATE TABLE
2. DROP TABLE
3. ALTER TABLE
4. SHOW TABLES
5. SHOW CREATE TABLE
6. READ TABLE
7. READ TABLE BY TAG
8. WRITE TABLE
9. DELETE TABLE

#### 4.1.3 列权限

对给定表的给定列的读写权限控制
1. READ COLUMN DATA
2. WRITE COLUMN DATA

#### 4.1.4 行权限

对给定表的给定列的数据范围的权限控制，指定时间戳范围
1. WRITE ROW DATA
2. READ ROW DATA

#### 4.1.5 自定义函数权限

1. CREATE FUNCTION
2. DROP FUNCTION
3. SHOW FUNCTIONS

#### 4.1.6 索引权限

1. CREATE INDEX
2. DROP INDEX
3. SHOW INDEXES

#### 4.1.7 SMA 权限

1. CREATE RSMA
2. DROP RSMA
3. SHOW RSMA
4. CREATE TSMA
5. DROP TSMA
6. SHOW TSMA

#### 4.1.8 视图权限

1. CREATE VIEW
2. DROP VIEW
3. SHOW VIEW
4. READ VIEW

#### 4.1.9 角色权限

1. CREATE ROLE
2. DROP ROLE
3. SHOW ROLE

#### 4.1.10 用户权限

1. CREATE USER
2. DROP USER
3. SET USER SAFE INFO：创建、修改用户时，可以设置安全相关信息
4. SET USER AUDIT INFO：创建、修改用户时，可以设置审计相关信息
5. SET USER BASIC INFO
6. ENABLE USER
7. DISABLE USER
8. SHOW USERS

#### 4.1.11 令牌权限

1. CREATE TOKEN
2. DROP TOKEN
3. ALTER TOKEN
4. SHOW TOKEN

#### 4.1.12 密码权限

1. ALTER PASS：修改其他用户密码
2. ALTER SELF PASS：修改用户自己的密码

#### 4.1.13 权限授予回收的权限

授予和回收权限时，将自己拥有的对象的部分或全部访问权限授予其他用户。
1. GRANT PRIVLIEGE
2. REVOKE PRIVILEGE
3. GRANT SYSDBA PRIVILEGE
4. REVOKE SYSDBA PRIVILEGE
5. GRANT SYSSSO PRIVILEGE
6. REVOKE SYSSSO PRIVILEGE
7. GRANT SYSAUDITOR PRIVILEGE
8. REVOKE SYSAUDITOR PRIVILEGE

#### 4.1.14 节点管理权限

1. CREATE DNODE
2. DROP DNODE
3. SHOW DNODES
4. CREATE MNODE
5. DROP MNODE
6. SHOW MNODES
7. CREATE QNODE
8. DROP QNODE
9. SHOW QNODES
10. CREATE SNODE
11. DROP SNODE
12. SHOW SNODES
13. CREATE BNODE
14. DROP BNODE
15. SHOW BNODES

#### 4.1.15 系统参数调整权限

1. ALTER SAFE VARIABLE：修改安全参数
2. ALTER AUDIT VARIABLE：修改审计参数
3. ALTER SYSTEM VARIABLE：修改系统参数
4. ALTER DEBUG VARIABLE：修改调试参数
5. SHOW SAFE VARIABLE：查看安全参数
6. SHOW AUDIT VARIABLE：查看审计参数
7. SHOW SYSTEM VARIABLE：查看系统参数
8. SHOW DEBUG VARIABLE：查看调试参数

#### 4.1.16 密钥权限

1. UPDATE KEY：更新 SVR_KEY、DB_KEY
2. CREATE TOTP
3. SHOW TOTP
4. DROP TOTP
5. UPDATE TOTP

#### 4.1.17 订阅权限

1. CREATE TOPIC
2. DROP TOPIC
3. SHOW TOPICS
4. SHOW CONSUMERS
5. SHOW SUBSCRIPTIONS

#### 4.1.18 流计算权限

1. CREATE STREAM
2. DROP STREAM
3. SHOW STREAM

#### 4.1.19 系统管理权限

1. SHOW TRANS
2. KILL TRANS
3. SHOW CONNECTIONS
4. KILL CONNECTION
5. SHOW QUERIES
6. KILL QUERY

#### 4.1.20 系统信息查看权限

1. USE INFORMATION_SCHEMA
2. USE PERFORMANCE_SCHEMA
3. READ INFORMATION_SCHEMA LIMIT：只能看到表 ins_databases、ins_functions ins_indexes、ins_stables、ins_tables、ins_tags、ins_columns、ins_configs、ins_topics、ins_subscriptions、ins_streams、ins_stream_tasks、ins_views、ins_compacts、ins_compact_details、ins_tsmas，部分表只能看到可见字段，参照 [TD-18525](https://jira.taosdata.com:18080/browse/TD-18525)
4. READ INFORMATION_SCHEMA SAFE 
5. READ INFORMATION_SCHEMA AUDIT 
6. READ INFORMATION_SCHEMA BASIC
7. READ PERFORMANCE_SCHEMA LIMIT：只能看到表 perf_connections perf_queries/perf_consumers perf_trans、perf_apps
8. READ PERFORMANCE_SCHEMA BASIC
9. SHOW GRTANTS
10. SHOW CLUSTER
11. SHOW APPS

#### 4.1.21 审计管理权限

1. CREATE AUDIT DATABASE
2. DROP AUDIT DATABASE
3. ALTER AUDIT DATABASE
4. READ AUDIT DATABASE
5. WRITE AUDIT DATABASE

### 4.2 角色管理

#### 4.2.1 创建角色

```sql {wrap}
CREATE ROLE [IF NOT EXISTS] <角色名>;
```

1. 创建者必须具有 CREATE ROLE 数据库权限；
2. 角色名的长度为 8-63 个字符；
3. 角色名不允许和系统已存在的用户名重名；

#### 4.2.2 删除角色

```sql {wrap}
DROP ROLE [IF EXISTS] <角色名>;
```

#### 4.2.3 查看角色

```sql {wrap}
SHOW ROLES
SELECT * from Information_scheam.ins_roles;
```

#### 4.2.4 角色启用与禁用

```sql {wrap}
LOCK/UNLOCK ROLE
```

### 4.3 权限分配

#### 4.3.1 数据库权限分配

通过 GRANT 给角色或者用户授予数据库权限，为保证兼容性，允许使用 DATABASE 关键字
```sql {wrap}
GRANT <DATABASE> privileges ON priv_obj [WITH condition] TO <user_name / role name>
```

#### 4.3.2 数据库权限回收

```sql {wrap}
REVOKE <DATABASE> privileges FROM <user_name / role name>
```

#### 4.3.3 查看数据库权限

```sql {wrap}
show privileges 
show user privileges 
select * from ins_privileges;
```

#### 4.3.4 角色权限分配

通过 GRANT 给用户分配角色
```sql {wrap}
GRANT ROLE role_name TO <user_name>
```

#### 4.3.5 角色权限回收

```sql {wrap}
REVOKE ROLE role_name FROM <user_name>
```

### 4.4 系统管理角色

系统管理角色分为数据库管理员、数据库安全员、数据库审计员三种类型。在首次安装后，这些角色均授予给系统默认用户 root，之后由 SYSDBA 授予角色给其他用户。当 root 用户不再担任系统管理角色后，可以被删除。

#### 4.4.1 数据库管理员

数据库管理员是权限最高的系统管理员角色，负责数据库的日常运维和系统管理。角色名称为 SYSDBA。具备除 数据库安全员、数据库审计员之外的所有系统权限

#### 4.4.2 数据库安全员

数据库安全员的主要职责是制定并应用安全策略，强化系统安全机制。角色名称为 SYSSSO。拥有的权限包括：
1. GRANT SYSSSO PRIVILEGE
2. REVOKE SYSSSO PRIVILEGE
3. ALTER SAFE VARIABLE：修改安全参数
4. SHOW SAFE VARIABLE：查看安全参数
5. UPDATE KEY：更新 SVR_KEY、DB_KEY
6. CREATE TOTP
7. SHOW TOTP
8. DROP TOTP
9. UPDATE TOTP
10. SET USER SAFE INFO：创建、修改用户时，可以设置安全相关信息
11. READ INFORMATION_SCHEMA SAFE

#### 4.4.3 数据库审计员

数据库安全员是负责独立审计监督的关键角色，其核心职责是监控和审查数据库操作，确保所有行为合规，但不能查看业务数据。角色名称为 SYSAUDITOR。拥有的权限包括：
1. GRANT SYSAUDITOR PRIVILEGE
2. REVOKE SYSAUDITOR PRIVILEGE
3. ALTER AUDIT VARIABLE
4. SHOW AUDIT VARIABLE
5. CREATE AUDIT DATABASE
6. DROP AUDIT DATABASE
7. ALTER AUDIT DATABASE
8. READ AUDIT DATABASE
9. WRITE AUDIT DATABASE
10. READ INFORMATION_SCHEMA AUDIT
其他说明：给数据库增加名为“审计”的选项

### 4.5 系统内置角色

系统内置角色的权限范围不可更改。

#### 4.5.1 SYSINFO_1 角色

SYSINFO=1 的权限分类在 3.3.8 版本之前一直具备，为保持兼容性，改造为角色。部分权限如下，参见 [权限管理旧版 FS 文档](https://taosdata.feishu.cn/wiki/CFyuwFGCKimeXpkOHT2cTSAGn4M)。
1. ALTER SELF PASS
2. CREATE DATABASE
3. READ INFORMATION_SCHEMA BASIC
4. READ PERFORMANCE_SCHEMA BASIC
5. SHOW USERS、SHOW CLUSTER 等权限

#### 4.5.2 SYSINFO_0 角色

SYSINFO=0 的权限分类在 3.3.8 版本之前一直具备，为保持兼容性，改造为角色。部分权限如下，参见“[权限管理旧版 FS 文档](https://taosdata.feishu.cn/wiki/CFyuwFGCKimeXpkOHT2cTSAGn4M)”。
1. 不设置 ALTER SELF PASS 的权限
2. 不设置 CREATE DATABASE 的权限
3. READ INFORMATION_SCHEMA LIMIT
4. READ PERFORMANCE_SCHEMA LIMIT
5. SHOW DATABASES 等权限

#### 4.5.3 SYSDBO 角色

数据库所有者权限在 3.3.8 版本之前一直具备，为保持兼容性，改造为角色。部分权限如下，参见“[权限管理旧版 FS 文档](https://taosdata.feishu.cn/wiki/CFyuwFGCKimeXpkOHT2cTSAGn4M)”。
1. 不设置 DB 外部的相关权限
2. DB 内部的权限，按和老版本的兼容性给与默认值(DB Owner）
3. 对自己拥有的数据库内部的、表、视图、索引、流等，具有所有的对象权限并可以授出与回收
在用户创建完数据库之后，会在刚创建的数据库上，被赋予 SYSDBO  角色对应的权限

#### 4.5.4 SYSDBW 角色

参照老版本中的 DB WRITE 权限

#### 4.5.5 SYSDBR 角色

参照老版本中的 DB READ 权限

## 5. 性能需求

访问控制可能带来一定的性能开销。在测试过程中，对如下指标进行测试，如果性能不达预期，需优化代码。
1. **写入性能**：
   - 访问控制实现影响数据写入性能，写入延迟增加不得超过 100%。
2. **查询性能**：
   - 访问控制实现影响数据查询性能，查询延迟增加不得超过 100%。

## 6. 安全需求

权限相关信息需加密存储，已经在“[存储安全需求文档](https://taosdata.feishu.cn/wiki/UYAqwU3GqiBsjCkT6BccKqLmnGh)”中描述

## 7. 兼容性需求

1. 为保证创建用户语法的兼容性，在“[身份鉴别需求文档](https://taosdata.feishu.cn/wiki/GZNPwH62SiiRtQkQHTvcM73YnDh)”中，创建用户支持 sysinfo 和 createdb 两个选项，当指定选项时，自动赋予角色和权限给用户
2. Grant 和 Revoke 的语法变化很大，但仍考虑语法解析时进行特殊处理，保证语法兼容性
3. 在存储时，需向后兼容，自 3.3.8 版本升级至本版本时，不需要手工干预
4. 使用新版本后，不能退回到旧版本
