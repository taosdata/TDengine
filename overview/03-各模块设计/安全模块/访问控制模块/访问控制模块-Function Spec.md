# 访问控制模块-Function Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-10-28 | 2025-11-20 | 1.0 | 徐开礼 | 新建 |
| 2025-12-08 | 2025-12-16 | 1.1 | 程洪泽 | 修订及完善内容 |

## 2. 背景

实施最小权限原则，严格管理用户对数据库的访问权限。
1. 基于角色的访问控制（RBAC），系统管理员、安全管理员、审计员
2. 强制访问控制（MAC）
3. 表、列级权限：需增加子表、普通表、列的权限控制（可仅用于演示）
4. 读写权限细化：仅 read/write，需补充 alter、insert、delete等操作权限
5. 权限定期审查（如每季度一次）
6. 实现操作系统和数据库系统特权用户的权限分离
7. 系统管理员用户名允许修改
8. Root 禁止访问私有数据
JIRA: [TS-7232](https://jira.taosdata.com:18080/browse/TS-7232)

## 3. 定义

### 3.1 系统权限

- 面向 TDengine 集群全局的权限，作用于集群级资源与操作（如创建数据库、集群启动停止、全局参数配置、超级用户管理等），不针对集群内具体的数据库、表等数据对象。

### 3.2 对象权限

- 面向 TDengine 集群内具体对象的权限，可作用于单个或多个数据库、超级表、子表、视图等对象，控制对该对象的特定操作（如查询、插入、更新、删除、授权等）。

### 3.3 超级用户 root

- TDengine 数据库的超级用户，在 TDengine 初次启动时默认创建，拥有 SYSDBA/SYSSEC/SYSAUDIT 3 个系统角色的所有权限。
<callout emoji="bulb" background-color="light-orange" border-color="light-orange">
在 TDengine 3.4.x.y 及后续支持 RBAC 的企业版中，在系统初始配置将`数据库管理员（SYSDBA）`、`数据库安全员（SYSSEC）` 和`数据库审计员（SYSAUDIT）` 3 个系统角色赋予不同的用户后，建议由 SYSDBA 锁定 root 用户，以减少安全风险。为防止单点风险，系统中拥有 SYSDBA 角色的用户需要 2 名及以上，否则，不允许锁定 root。
</callout>

### 3.4 数据库管理员 SYSDBA

- [数据库管理员](https://taosdata.feishu.cn/docx/Wcqwd8yyrob5SCxorAncKJmYnBc#doxcnfRE3RRR1UqFVTh2ckSgiBb)是一种权限最高的系统内置管理员角色，负责数据库的日常运维和系统管理。具备除`数据库安全员`、`数据库审计员`之外的所有系统权限。

### 3.5 数据库安全员 SYSSEC

- [数据库安全员](https://taosdata.feishu.cn/docx/Wcqwd8yyrob5SCxorAncKJmYnBc#doxcncv3t0u2v9O6IOK9iJiyfJb)是一种系统内置角色，主要职责是制定并应用安全策略，强化系统安全机制。

### 3.6 数据库审计员 SYSAUDIT

- [数据库审计员](https://taosdata.feishu.cn/docx/Wcqwd8yyrob5SCxorAncKJmYnBc#doxcnzsKcvUhNBxyL20I9mAiQYb)是一种系统内置的负责独立审计监督的关键角色，核心职责是监控和审查数据库操作，确保所有行为合规，但不能查看业务数据。

### 3.7 所有者 Owner

- 所有者（Owner）是指对特定数据库对象（如表、索引、视图、函数等）拥有最高控制权和管理权的用户（或角色），是数据库对象的 “创建者或被转移所有权的接收者”，对该对象拥有隐含的、无需额外授权的全量权限，包括但不限于修改对象结构、删除对象、向其他用户授予 / 撤回权限等。

## 4. 行为说明

- PostgreSQL/Oracle/MySQL/OpenGauss 等主流数据库的权限管理功能各不相同，其中 OpenGauss [三权分立](https://gitcode.com/opengauss/docs/blob/6.0.0/content/zh/docs/DatabaseAdministrationGuide/%E4%B8%89%E6%9D%83%E5%88%86%E7%AB%8B.md)实现更加严格，该 FS 更加倾向于 OpenGauss 的风格。 

### 4.1 权限和角色

1. 权限可分为系统权限和对象权限。
2. 基于 RBAC，可以将权限逐项授予某一角色或从某一角色撤回。
3. 为便于运维，系统内置常用角色，默认授予相应的权限。
4. 如果有特殊要求，可由 SYSDBA 或拥有`创建角色`权限的用户创建新的角色。

### 4.2 权限列表

- 目前，共计 129 种权限。

#### 4.2.1 库权限

- 对于非系统库（具体指 information_schema，performance_schema，审计数据库），有如下权限
<callout emoji="bulb" background-color="light-orange" border-color="light-orange">
vnode/vgroup 是创建数据库的过程中自动创建的，因此将其归类于`库（DB）权限`
</callout>


| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | CREATE DATABASE | **是** | SYSDBA | ALTER USER user_name CREATEDB 0/1; -- 保留。兼容 3.4.x.y 之前的版本 GRANT `CREATE DATABASE` TO user_name/role_name; REVOKE `CREATE DATABASE` FROM user_name/role_name; |  |
| 2 | ALTER DATABASE |  | SYSDBA owner |  |
| 3 | DROP DATABASE |  | SYSDBA owner |  |
| 4 | USE DATABASE |  | SYSDBA owner |  |
| 5 | FLUSH DATABASE |  | SYSDBA owner |  |
| 6 | COMPACT DATABASE |  | SYSDBA owner | COMPACT VGROUP 操作检查 COMPACT DATABASE 权限 |
| 7 | TRIM DATABASE |  | SYSDBA owner |  |
| 8 | ROLLUP DATABASE |  | SYSDBA owner | ROLLUP VGROUP 操作检查 ROLLUP DATABASE 权限 |
| 9 | SCAN DATABASE |  | SYSDBA owner | SCAN VGROUP 操作检查 SCAN DATABASE 权限 |
| 10 | SSMIGRATE DATABASE |  | SYSDBA owner |  |
| 11 | BALANCE VGROUP | 是 | SYSDBA 且不允许授予其他人或角色 |  | 理论上也应该是对象权限。为了功能正常执行（BALANCE VGROUP 操作作用于多个 DB），设置为系统权限。 |
| 12 | BALANCE VGROUP LEADER | 是 | SYSDBA 且不允许授予其他人或角色 |
| 13 | MERGE VGROUP | 是 | SYSDBA 且不允许授予其他人或角色 |
| 14 | REDISTRIBUTE VGROUP | 是 | SYSDBA 且不允许授予其他人或角色 |
| 15 | SPLIT VGROUP | 是 | SYSDBA 且不允许授予其他人或角色 |
| 16 | SHOW DATABASES |  |  | 拥有 `所有 DB`或 `该 DB` 的 show databases 权限，返回；否则不返回。 |
| 17 | SHOW VNODES |  |  | 拥有 `所有 DB`或 `该 DB` 的 show vnodes 权限，返回；否则不返回。 |
| 18 | SHOW VGROUPS |  |  | 拥有 `所有 DB`或 `该 DB` 的 show vgroups 权限，返回；否则不返回。 |
| 19 | SHOW COMPACTS |  |  | 拥有 `所有 DB`或 `该 DB` 的 show compacts 权限，返回；否则不返回。 |
| 20 | SHOW RETENTIONS |  |  | 同上 |
| 21 | SHOW SCANS |  |  | 同上 |
| 22 | SHOW SSMIGRATES |  |  | 同上 |

#### 4.2.2 表权限

- 如下权限，包括普通表、超级表、子表、虚拟表 4 种类型的表，暂不针对表的类型区分权限。

| 序号 | 权限名称(privilege name) | 系统权限 | grant/revoke 语句 | 备注 1）以下操作，均需要先拥有 USE DATABASE 的权限。 2）如果执行下述 grant 操作时，没有 dbname 或 * 的 USE DATABASE 权限时，会报错，提示用户先授予 USE DATABASE 权限：Cannot grant CREATE TABLE privilege since lack of USE DATABASE privilege on {dbname} or *" |
| --- | --- | --- | --- | --- |
| 1 | CREATE TABLE |  | GRANT `{privilege name}` ON dbname TO user_name/role_name; GRANT `{privilege name}` ON * TO user_name/role_name; REVOKE `{privilege name}` ON db FROM user_name/role_name; REVOKE `{privilege name}` ON * FROM user_name/role_name; | 是否拥有某个或所有 DB 的 CREATE TABLE 权限 |
| 2 | DROP TABLE |  |  |
| 3 | ALTER TABLE |  |  |
| 4 | SHOW TABLES |  |  |
| 5 | SHOW CREATE TABLE |  |  |
| 6 | READ TABLE |  | equiv. to SELECT TABLE |
| 7 | WRITE TABLE |  | equiv. to INSERT TABLE |
| 8 | UPDATE TABLE |  | 预留。TDengine 暂无 UPDATE 语句，而是通过 INSERT 语句自动实现 UPDATE。 |
| 9 | DELETE TABLE |  |  |

#### 4.2.3 列权限

- 对给定表的给定列的读写权限控制(可指定列是否脱敏展示)
```cpp {wrap}
1）grant select(c0,c1) on d0.t1 to u1;         => revoke select(c0,c1) on d0.t1 from u1;
2）grant select(c0,mask(c1)) on d0.t1 to u1;   => revoke select(c0,c1) on d0.t1 from u1;

1）2）语句中，针对 c1，同时存在 非 mask 和 mask， 这种条件是存在冲突的，在执行时，向用户返回错误。
```

```cpp {wrap}
READ COLUMN DATA： 即表的 READ TABLE 权限 + column
WRITE COLUMN DATA：即表的 WRITE TABLE 权限 + column
```


| 序号 | 权限名称(privilege name) | 系统权限 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- |
| 1 | ~~READ COLUMN DATA~~ |  | 前提：拥有 USE DATABASE 权限，表的 READ(SELECT) 权限。不存在提示用户先授权：Cannot grant {privilege name} since lack of {privilege name} on priv_level。 实现：如果查询的列无权限，报错返回。 |
| 2 | ~~WRITE COLUMN DATA~~ |  | 前提：拥有 USE DATABASE 权限，表的 WRITE(INSERT) 权限。不存在提示用户先授权。 实现：如果写入的列无权限，报错返回。 |

#### 4.2.4 行权限

- 对给定表的给定行的数据范围的权限控制，指定时间戳范围。
```cpp {wrap}
3.3.x.y 的版本，with tag_condition 语句，支持指定普通列/tag 列，因此，已经支持 "行权限" 控制。
3.4.x.y 版本沿用该语句，将语法中的  tag_condition 修改为 col_condition，支持同时指定普通列/tag 列。只允许存在一条带"行或tag" 条件的权限记录。
```


| 序号 | 权限名称(privilege name) | 系统权限 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- |
| 1 | ~~READ ROW DATA~~ |  | 前提：拥有 USE DATABASE 权限，表的 READ(SELECT) 权限。不存在提示用户先授权：Cannot grant {privilege name} since lack of {privilege name} on priv_level。 实现：在读取时，类似 tag_condition，将时间范围作为过滤条件添加到查询语句上。 |
| 2 | ~~WRITE ROW DATA~~ |  | 前提：拥有 USE DATABASE 权限，表的 WRITE(INSERT) 权限。不存在提示用户先授权。 实现：在写入时，如果 ts 不在范围，报错返回。 |

#### 4.2.5 自定义函数权限

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | CREATE FUNCTION | 是 | SYSDBA |  |
| 2 | DROP FUNCTION | 是 | SYSDBA |  |
| 3 | SHOW FUNCTIONS | 是 | SYSDBA SYSSEC SYSAUDIT SYSINFO_0 SYSINFO_1 |  |

#### 4.2.6 索引权限

| 序号 | 权限名称(privilege name) | 系统权限 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- |
| 1 | CREATE INDEX |  | 创建 INDEX，需要表的 CREATE INDEX 权限，以及对应列的 SELECT 权限。 |
| 2 | DROP INDEX |  |  |
| 3 | SHOW INDEXES |  |  |

#### 4.2.7 SMA 权限

| 序号 | 权限名称(privilege name) | 系统权限 | grant/revoke 语句 | 备注 前提：USE DATABASE |
| --- | --- | --- | --- | --- |
| 1 | CREATE RSMA |  | GRANT `{privilege name}` ON *.* TO user_name/role_name; GRANT `{privilege name}` ON dbname.* TO user_name/role_name; GRANT `{privilege name}` ON dbname.tbname TO user_name/role_name; REVOKE `{privilege name}` ON *.* FROM user_name/role_name; REVOKE `{privilege name}` ON dbname.* FROM user_name/role_name; REVOKE `{privilege name}` ON dbname.tbname FROM user_name/role_name; | 前提：USE DATABASE + TABLE(超级表) READ/WRITE 权限。 |
| 2 | DROP RSMA |  |  |  |
| 3 | ALTER RSMA |  |  |  |
| 4 | SHOW RSMAS |  |  |  |
| 5 | SHOW CREATE RSMA |  |  |  |
| 6 | CREATE TSMA |  |  | 前提：USE DATABASE + TABLE(超级表) READ/WRITE 权限 |
| 7 | DROP TSMA |  |  |  |
| 8 | SHOW TSMAS |  |  |  |

#### 4.2.8 视图权限

| 序号 | 权限名称(privilege name) | 系统权限 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- |
| 1 | CREATE VIEW |  | 前提：USE DATABASE + TABLE 或对应 COLUMN/ROW 的 READ 权限 |
| 2 | DROP VIEW |  |  |
| 3 | SHOW VIEWS |  |  |
| 4 | READ VIEW |  |  |

#### 4.2.9 挂载权限

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | CREATE MOUNT | 是 | SYSDBA |  |
| 2 | DROP MOUNT | 是 | SYSDBA |  |
| 3 | SHOW MOUNTS | 是 | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 |  |

#### 4.2.10 角色权限

SYSDBA 拥有 CREATE/DROP ROLE 权限，SYSDBA/SYSSEC/SYSAUDIT 均拥有 SHOW ROLES 权限。

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | CREATE ROLE | 是 | SYSDBA |  |
| 2 | DROP ROLE | 是 | SYSDBA |  |
| 3 | SHOW ROLES | 是 | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 |  |

#### 4.2.11 用户权限

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | CREATE USER | 是 | SYSDBA |  |
| 2 | DROP USER | 是 | SYSDBA |  |
| 3 | SET USER SECURITY INFO | 是 | SYSSEC | 创建、修改用户时，可以设置安全相关信息 |
| 4 | SET USER AUDIT INFO | 是 | SYSAUDIT | 创建、修改用户时，可以设置审计相关信息 |
| 5 | SET USER BASIC INFO | 是 | SYSDBA | 创建、修改用户时，可以设置与安全/审计无关的基础信息 |
| 6 | ALTER USER | 是 | SYSDBA |  |
| 7 | UNLOCK USER | 是 | SYSSEC |  |
| 8 | LOCK USER | 是 | SYSSEC |  |
| 9 | SHOW USERS | 是 | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 |  |

#### 4.2.12 令牌权限

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | [CREATE TOKEN](https://taosdata.feishu.cn/wiki/CXXqwV3Fai36rQkby6zcmBWwnMd#BCk5dKEoKo9KttxWjUGcjcNWnyf) | 是 | SYSSEC OWNER |  |
| 2 | DROP TOKEN | 是 | SYSSEC OWNER |  |
| 3 | ALTER TOKEN | 是 | SYSSEC OWNER |  |
| 4 | SHOW TOKENS | 是 | SYSDBA SYSSEC SYSAUDIT OWNER | systable 目前列只有 sysInfo true/false 两种取值，可以扩展以支持 SYSAUDIT 进行查看某些列。 - SYSAUDIT 可以看到： - 令牌ID - 所属用户 - 创建时间 - 最后使用时间 - 状态（活跃/禁用） - 权限范围 SYSAUDIT 不应看到： - 令牌具体密钥值 - 加密的令牌内容 - 签名密钥材料 - OWNER 只能查看自己的 TOKEN |

#### 4.2.13 密钥权限

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | UPDATE KEY | 是 | SYSDBA | 更新 SVR_KEY、DB_KEY |
| 2 | [CREATE TOTP](https://taosdata.feishu.cn/wiki/CXXqwV3Fai36rQkby6zcmBWwnMd#P7hEdjsOoo9IMyxlg4JcjPo7ne6) | 是 | SYSSEC OWNER |  |
| 3 | DROP TOTP | 是 | SYSSEC OWNER |  |
| 4 | UPDATE TOTP | 是 | SYSSEC OWNER |  |
| ~~5~~ | ~~SHOW TOTPS~~ | ~~是~~ | ~~SYSDBA~~ ~~SYSSEC~~ ~~SYSAUDIT~~ ~~OWNER~~ |  |

#### 4.2.14 密码权限

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | ALTER PASS | 是 | ROOT SYSDBA |  |
| 2 | ALTER SELF PASS | 是 | OWNER |  |

#### 4.2.15 权限授予回收的权限

- 授予和回收权限时，将自己拥有的对象的部分或全部访问权限授予其他用户，或从其他用户回收已授予的权限。
- GRANT 暂不支持 WITH GRANT OPTION。因此，列表中的权限，只能由 SYSDBA/SYSSEC/SYSAUDIT 和 Owner 授予/撤回 。

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | GRANT PRIVILEGE | 是 | SYSSEC OWNER |
| 2 | REVOKE PRIVILEGE | 是 | SYSSEC OWNER |
| 3 | GRANT SYSDBA PRIVILEGE | 是 | SYSDBA |
| 4 | REVOKE SYSDBA PRIVILEGE | 是 | SYSDBA |
| 5 | GRANT SYSSEC PRIVILEGE | 是 | SYSSEC |
| 6 | REVOKE SYSSEC PRIVILEGE | 是 | SYSSEC |
| 7 | GRANT SYSAUDIT PRIVILEGE | 是 | SYSAUDIT |
| 8 | REVOKE SYSAUDIT PRIVILEGE | 是 | SYSAUDIT |

#### 4.2.16 节点管理权限

| 序号 | 权限名称(privilege name) | 系统权限 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- |
| ~~1~~ | ~~CREATE DNODE~~ | 是 |  |
| ~~2~~ | ~~DROP DNODE~~ | 是 |  |
| ~~3~~ | ~~SHOW DNODES~~ | 是 |  |
| ~~4~~ | ~~CREATE MNODE~~ | 是 |  |
| ~~5~~ | ~~DROP MNODE~~ | 是 |  |
| ~~6~~ | ~~SHOW MNODES~~ | 是 |  |
| ~~7~~ | ~~CREATE QNODE~~ | 是 |  |
| ~~8~~ | ~~DROP QNODE~~ | 是 |  |
| ~~9~~ | ~~SHOW QNODES~~ | 是 |  |
| ~~10~~ | ~~CREATE SNODE~~ | 是 |  |
| ~~11~~ | ~~DROP SNODE~~ | 是 |  |
| ~~12~~ | ~~SHOW SNODES~~ | 是 |  |
| ~~13~~ | ~~CREATE BNODE~~ | 是 |  |
| ~~14~~ | ~~DROP BNODE~~ | 是 |  |
| ~~15~~ | ~~SHOW BNODES~~ | 是 |  |

#### 4.2.17 系统参数调整权限

| 序号 | 权限名称(privilege name) | 系统权限 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- |
| 1 | ALTER SECURITY VARIABLE | 是 | 修改安全参数 |
| 2 | ALTER AUDIT VARIABLE | 是 | 修改审计参数 |
| 3 | ALTER SYSTEM VARIABLE | 是 | 修改系统参数 |
| 4 | ALTER DEBUG VARIABLE | 是 | 修改调试参数 |
| 5 | SHOW SECURITY VARIABLE | 是 | 查看安全参数 |
| 6 | SHOW AUDIT VARIABLE | 是 | 查看审计参数 |
| 7 | SHOW SYSTEM VARIABLE | 是 | 查看系统参数 |
| 8 | SHOW DEBUG VARIABLE | 是 | 查看调试参数 |

#### 4.2.18 订阅权限

| 序号 | 权限名称(privilege name) | 系统权限 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- |
| 1 | CREATE TOPIC |  | GRANT privileges ON * TO {user_name|role_name}; GRANT privileges ON dbname TO {user_name|role_name}; REVOKE privileges FROM * {user_name|role_name}; REVOKE privileges FROM dbname {user_name|role_name}; | 前提：表权限 |
| 2 | DROP TOPIC |  |  |
| 3 | SHOW TOPICS |  |  |
| 4 | SHOW CONSUMERS |  |  |
| 5 | SHOW SUBSCRIPTIONS |  |  |

#### 4.2.19 流计算权限

| 序号 | 权限名称(privilege name) | 系统权限 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- |
| 1 | CREATE STREAM |  | GRANT privileges ON * TO {user_name|role_name}; GRANT privileges ON dbname TO {user_name|role_name}; REVOKE privileges FROM * {user_name|role_name}; REVOKE privileges FROM dbname {user_name|role_name}; |  |
| 2 | DROP STREAM |  |  |
| 3 | SHOW STREAMS |  |  |
| 4 | START STREAM |  |  |
| 5 | STOP STREAM |  |  |
| 6 | RECALC STREAM |  |  |

#### 4.2.20 系统管理权限

| 序号 | 权限名称(privilege name) | 系统权限 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- |
| 1 | SHOW TRANS |  |
| 2 | KILL TRANS |  |
| 3 | SHOW CONNECTIONS |  |
| 4 | KILL CONNECTION |  |
| 5 | SHOW QUERIES |  |
| 6 | KILL QUERY |  |

#### 4.2.21 系统信息查看权限

| 序号 | 权限名称(privilege name) | 系统权限 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- |
| 1 | USE INFORMATION_SCHEMA |  |
| 2 | USE PERFORMANCE_SCHEMA |  |
| 3 | READ INFORMATION_SCHEMA LIMIT | 只能看到受限系统表，ins_databases、ins_functions ins_indexes、ins_stables、ins_tables、ins_tags、ins_columns、ins_configs、ins_topics、ins_subscriptions、ins_streams、ins_stream_tasks、ins_views、ins_compacts、ins_compact_details、ins_tsmas，部分表只能看到可见字段，参照 [TD-18525](https://jira.taosdata.com:18080/browse/TD-18525) |
| 4 | READ INFORMATION_SCHEMA SECURITY | 安全相关的表, ins_users, ins_user_privileges, ins_tokens, ins_roles, ins_role_prvileges(TODO) |
| 5 | READ INFORMATION_SCHEMA AUDIT | 审计相关的系统表, ins_audits(TODO) |
| 6 | READ INFORMATION_SCHEMA PLAIN | 非 security/audit，并且不受 sysInfo 限制的表/字段。 |
| 7 | READ PERFORMANCE_SCHEMA LIMIT | 只能看到受限系统表，perf_connections perf_queries/perf_consumers perf_trans、perf_apps |
| 8 | READ PERFORMANCE_SCHEMA BASIC |  |
| 9 | SHOW GRANTS | 授权相关的系统表，涉及命令：show grants/show grants full/show grants logs/show cluster machines, .etc |
| 10 | SHOW CLUSTER |  |
| 11 | SHOW APPS |  |

#### 4.2.22 审计管理权限

- 审计库由 SYSDBA 或拥有 CREATE AUDIT DATABASE 权限的用户创建。
- 数据库增加 AUDIT 参数指定是否为审计库：0 非审计库(未指定时默认值) ， 1 审计库。
- TDengine 中，限制只能有一个审计库。
```cpp {wrap}
create database if not exists d0 audit 1; -- 指定数据库为审计库
alter database d0 audit 1;                -- 修改数据库为审计库(为了兼容老版本审计库)
```

- TDengine 中，预置 `审计日志记录` SYSAUDIT_LOG 角色，可以在 audit 库中建表、写入数据，但不能删表/修改表/删除数据。SYSAUDIT_LOG 角色不能与 SYSDBA/SYSSEC/SYSAUDIT 角色同时授予某一个人。

| 序号 | 权限名称(privilege name) | 系统权限 | 系统角色 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | CREATE AUDIT DATABASE | 是 | SYSDBA |
| 2 | DROP AUDIT DATABASE | 是 | SYSDBA |
| 3 | ALTER AUDIT DATABASE | 是 | SYSDBA |
| 4 | USE AUDIT DATABASE | 是 | SYSAUDIT SYSAUDIT_LOG |
| 5 | CREATE TABLE(4.2.2 表权限) |  | SYSAUDIT_LOG |  |
| 6 | ~~DROP TABLE(4.2.2 表权限)~~ |  | ~~SYSDBA~~ |  |
| 7 | ~~ALTER TABLE(4.2.2 表权限)~~ |  | ~~SYSDBA~~ |  |
| 8 | SHOW TABLES(4.2.2 表权限) |  | SYSAUDIT SYSAUDIT_LOG |  |
| 9 | SHOW CREATE TABLE(4.2.2 表权限) |  | SYSAUDIT SYSAUDIT_LOG |  |
| 10 | READ TABLE(4.2.2 表权限) |  | SYSAUDIT | 只有 SYSAUDIT 拥有该权限。 |
| 11 | WRITE TABLE(4.2.2 表权限) |  | SYSAUDIT_LOG | 只有拥有 SYSAUDIT_LOG 角色的用户可以向审计库中的表写入数据。 |
| 12 | UPDATE TABLE(4.2.2 表权限) |  | N/A |  | TDengine 暂不支持 update 语句 |
| 13 | DELETE TABLE(4.2.2 表权限) |  | N/A |  | 任何人不允许删除审计表中的数据 |

##### 4.2.22.1 taosKeeper

- 目前，taosKeeper 中，有配置项，可指定 `用户名`和`审计库名`，默认分别为 root 和 audit。
- 3.4.x.y 及后续版本对应的 taosKeeper，不允许使用 root 账户写入审计库。需要手动创建审计日志写入用户，为其赋予 SYSAUDIT_LOG 角色，并为其生成  [TOKEN ](https://taosdata.feishu.cn/wiki/CXXqwV3Fai36rQkby6zcmBWwnMd#BCk5dKEoKo9KttxWjUGcjcNWnyf)。
```sql {wrap}
CREATE USER audit_logger PASS 'xxxxxxxxxx'; -- audit_logger 为示例用户名，实际使用时建议更换。
GRANT ROLE SYSAUDIT_LOG TO audit_logger;
```

- TDengine 的审计日志模块，通过查询获取 `审计库的名称`和 `拥有 SYSAUDIT_LOG 角色的用户名及其TOKEN`，结合审计日志信息一同发往 taosKeeper 进行后续处理。

### 4.3 角色管理

角色管理操作只能由`具有 SYSDBA 角色`或`具有下述角色权限管理`的用户执行。SYSSEC/SYSAUDIT 不允许拥有角色管理权限。

#### 4.3.1 创建角色

```sql {wrap}
CREATE ROLE [IF NOT EXISTS] <角色名>;
```

1. 创建者必须具有 CREATE ROLE 数据库权限；
2. 角色名的长度为 8-63 个字符；
3. 角色名不允许和系统已存在的用户名重名；

#### 4.3.2 删除角色

```sql {wrap}
DROP ROLE [IF EXISTS] <角色名>;
```

#### 4.3.3 查看角色

```sql {wrap}
SHOW ROLES;
SELECT * from Information_scheam.ins_roles;

SHOW ROLE PRIVILEGES;
SELECT * from Information_scheam.ins_role_privileges;
```

#### 4.3.4 角色启用与禁用

```sql {wrap}
LOCK/UNLOCK ROLE
```

### 4.4 权限语法

#### 4.4.1 权限`分配/回收` 

##### 4.4.1.1 系统权限

```sql {wrap}
-- 系统权限
-- 3.3.x.y 已支持，3.4.x.y 兼容
CREATE USER user_name PASS 'password' [SYSINFO {1|0}] [CREATEDB {1|0}]; 
ALTER USER user_name alter_user_clause                               
alter_user_clause: {
    PASS 'literal'
  | ENABLE value
  | SYSINFO value
  | CREATEDB value
}

-- 3.4.x.y 新增
GRANT privileges TO {user_name|role_name};         -- QA: [with grant option]
REVOKE privileges FROM {user_name|role_name};      -- QA: [restrict|cascade]
privileges : {
    ALL [PRIVILEGES] 
    | priv_type [, priv_type] ...
}

priv_type : {
    -- 数据库管理
      CREATE DATABASE
    | BALANCE VGROUP | BALANCE VGROUP LEADER 
    | MERGE VGROUP | REDISTRIBUTE VGROUP | SPLIT VGROUP 
    
    -- 函数权限
    | CREATE FUNCTION | DROP FUNCTION | SHOW FUNCTIONS
    
    -- 挂载权限
    | CREATE MOUNT | DROP MOUNT | SHOW MOUNTS
  
    -- 用户管理
    | CREATE USER | DROP USER | SET USER SECURITY INFO | SET USER AUDIT INFO
    | SET USER BASIC INFO | ENABLE USER | DISABLE USER | SHOW USERS
    
    -- 角色管理
    | CREATE ROLE | DROP ROLE | SHOW ROLES
    
    -- 令牌权限
    | CREATE TOKEN | DROP TOKEN | ALTER TOKEN | SHOW TOKENS
    
    -- 节点管理
    | CREATE NODE | DROP NODE | SHOW NODES
    
    -- 系统参数
    | ALTER SECURITY VARIABLE | ALTER AUDIT VARIABLE | ALTER SYSTEM VARIABLE | ALTER DEBUG VARIABLE
    | SHOW SECURITY VARIABLE | SHOW AUDIT VARIABLE | SHOW SYSTEM VARIABLE | SHOW DEBUG VARIABLE
    
    -- 密钥/密码相关
    | UPDATE KEY | CREATE TOTP | SHOW TOTP | DROP TOTP | UPDATE TOTP
    | ALTER PASS | ALTER SELF PASS
    
    -- 审计管理
    | CREATE AUDIT DATABASE | DROP AUDIT DATABASE | ALTER AUDIT DATABASE 
    | USE AUDIT DATABASE
    
    -- 系统管理
    | SHOW TRANS | KILL TRANS | SHOW CONNECTIONS | KILL CONNECTION | SHOW QUERIES | KILL QUERY
    | SHOW GRANTS | SHOW CLUSTER | SHOW APPS
}
```

##### 4.4.1.2 对象权限

```sql {wrap}
-- 对象权限
GRANT privileges ON priv_level [WITH tag_condition ] TO {user_name|role_name};
REVOKE privileges ON priv_level [WITH tag_condition] FROM {user_name|role_name};

privileges : {
    ALL                           -- 3.3.x.y 已支持，3.4.x.y 兼容
  | ALL PRIVILEGES                -- 3.4.x.y 新增
  | priv_type [, priv_type] ...
}

priv_type : {
    READ                          -- 3.3.x.y 已支持，3.4.x.y 兼容(仅支持表/视图，不再支持 DB)
  | WRITE                         -- 3.3.x.y 已支持，3.4.x.y 兼容(仅支持表/视图，不再支持 DB)
  | {privilege_name}              -- 3.4.x.y 新增，privilege_name 取值参照 4.2
}

privilege_name: {
  -- 库权限
  ALTER DATABASE | DROP DATABASE | USE DATABASE | FLUSH DATABASE| COMPACT DATABASE 
  | TRIM DATABASE | ROLLUP DATABASE | SCAN DATABASE| SSMIGRATE DATABASE | SHOW DATABASES
  | SHOW VNODES | SHOW VGROUPS | SHOW COMPACTS | SHOW RETENTIONS | SHOW SCANS | SHOW SSMIGRATES
  
  -- 表权限
  | CREATE TABLE | DROP TABLE | ALTER TABLE | SHOW TABLES| SHOW CREATE TABLE 
  | READ [TABLE][(col,...)] | WRITE [TABLE][(col,...)]| DELETE TABLE
  
  -- 列权限 // 简化/减少权限类型
  | READ COLUMN DATA | WRITE COLUMN DATA
  
  -- 行权限 // 简化/减少权限类型
  | READ ROW DATA | WRITE ROW DATA
  
  -- 索引权限
  | CREATE INDEX | DROP INDEX | SHOW INDEXES
  
  -- RSMA 权限
  | CREATE RSMA | DROP RSMA | ALTER RSMA | SHOW RSMAS | SHOW CREATE RSMA
  
  -- TSMA 权限
  | CREATE TSMA | DROP TSMA | SHOW TSMAS
  
  -- 视图权限
  | CREATE VIEW | DROP VIEW | SHOW VIEWS | READ [VIEW]
  
  -- 订阅权限
  | CREATE TOPIC | DROP TOPIC | SHOW TOPICS | SHOW CONSUMERS | SHOW SUBSCRIPTIONS

  -- 流计算权限
  | CREATE STREAM | DROP STREAM | SHOW STREAMS | START STREAM | STOP STREAM | RECALC STREAM
}

priv_level: {
    *                             -- 所有数据库             3.4.x.y 新增 
    | *.*                         -- 所有数据库的所有对象     3.3.x.y 已支持，3.4.x.y 兼容
    | dbname                      -- 指定数据库             3.4.x.y 新增   
    | dbname.*                    -- 指定数据库的所有对象     3.3.x.y 已支持，3.4.x.y 兼容
    | dbname.objname              -- 指定数据库的指定对象     3.3.x.y 已支持，3.4.x.y 扩展
    | objname                     -- 当前数据库的指定对象     3.4.x.y 新增
    | col_spec                    -- 列规格                 3.4.x.y 新增
    | row_spec                    -- 行规格                 3.4.x.y 新增
}

col_spec: {//与主流数据库语法不一致: grant select(c0,c1) on t1 to u1;
    dbname.tbname(col_name [, col_name] ...)
    | tbname(col_name [, col_name] ...)
}

row_spec: {
    dbname.tbname BETWEEN start_time AND end_time
    | tbname BETWEEN start_time AND end_time
}
```

#### 4.4.2 查看数据库权限

```sql {wrap}
show user privileges            -- 查看用户权限，3.3.x.y 已支持
show role privileges            -- 查看角色权限，3.4.x.y 新增
select * from ins_user_privileges;   -- 查看用户权限，3.3.x.y 已支持
select * from ins_role_privileges;   -- 查看角色权限，3.4.x.y 新增
```

#### 4.4.3 角色权限分配

通过 GRANT 给用户分配角色
```sql {wrap}
GRANT ROLE role_name TO <user_name>；    -- QA: [WITH GRANT OPTION]
```

#### 4.4.4 角色权限回收

```sql {wrap}
REVOKE ROLE role_name FROM <user_name>;  -- QA: [CASCADE|RESTRICT]
```

#### 4.4.5 所有者转移

```sql {wrap}
ALTER obj_type obj_name OWNER TO <user_name|role_name>;
obj_type : {
    DATABASE
  | TABLE -- 只支持超级表
  | INDEX
  | RSMA
  | ...
}
```

### 4.5 系统管理角色

系统管理角色分为`数据库管理员 SYSDBA`、`数据库安全员 SYSSEC`、`数据库审计员 SYSAUDIT` 三种类型。在首次安装后，这些角色均授予给系统默认用户 root，之后由 SYSDBA 授予上述 3 种系统角色给其他用户。当 root 用户不再担任系统管理角色后，可以被删除。
1. 不允许将上述角色中任意 2 个同时授予同一个用户，否则，会破坏`三权分立`的核心安全架构。
2. 系统中允许存在 2 个及以上的用户拥有 SYSDBA/SYSSEC/SYSAUDIT 角色。
3. 系统管理角色的权限范围不可更改。系统升级时，新增权限需要默认赋给系统管理角色。

#### 4.5.1 数据库管理员

##### 4.5.1.1 概述

`数据库管理员` 是权限最高的系统管理员角色，负责数据库的日常运维和系统管理，角色名称为 `SYSDBA`。具备除 `数据库安全员、数据库审计员`之外的所有系统权限。负责创建用户和角色，但是，不能执行授予 SYSDBA 之外的授权管理，不能执行与 AUDIT DATABASE 相关的操作。

##### 4.5.1.2 权限

1. GRANT SYSDBA PRIVILEGE
2. REVOKE SYSDBA PRIVILEGE

#### 4.5.2 数据库安全员

##### 4.5.2.1 概述

`数据库安全员` 的主要职责是制定并应用安全策略，强化系统安全机制，角色名称为 `SYSSEC`。负责：
1）用户与角色授权管理
授予/撤销除 SYSDBA/SYSAUDIT 之外的权限。
2）权限与对象授权
SYSSEC 可以将任何除 SYSDBA/SYSAUDIT 之外的系统权限或数据库对象（表、视图、模式等）的权限授予其他用户或角色。

##### 4.5.2.2 权限

1. GRANT SYSSEC PRIVILEGE
2. REVOKE SYSSEC PRIVILEGE
3. ALTER SECURITY VARIABLE：修改安全参数
4. SHOW SECURITY VARIABLE：查看安全参数
5. UPDATE KEY：更新 SVR_KEY、DB_KEY
6. CREATE TOTP
7. SHOW TOTP
8. DROP TOTP
9. UPDATE TOTP
10. SET USER SECURITY INFO：创建、修改用户时，可以设置安全相关信息
11. READ INFORMATION_SCHEMA SECURITY

#### 4.5.3 数据库审计员

##### 4.5.3.1 概述

数据库审计员是负责独立审计监督的关键角色，其核心职责是监控和审查数据库操作，确保所有行为合规，但不能查看业务数据，角色名称为 SYSAUDIT。负责且仅负责 SYSAUDIT 权限的授予与撤销。

##### 4.5.3.2 权限

1. GRANT SYSAUDIT PRIVILEGE
2. REVOKE SYSAUDIT PRIVILEGE
3. ALTER AUDIT VARIABLE
4. SHOW AUDIT VARIABLE
5. CREATE AUDIT DATABASE
6. DROP AUDIT DATABASE
7. ALTER AUDIT DATABASE
8. READ AUDIT DATABASE
9. WRITE AUDIT DATABASE
10. READ INFORMATION_SCHEMA AUDIT
<callout emoji="bulb" background-color="light-orange" border-color="light-orange">
给数据库增加名为“审计”的选项，系统中，只允许存在一个审计库。
</callout>

### 4.6 系统内置角色

系统内置角色的权限范围不可更改。

#### 4.6.1 [SYSAUDIT_LOG](https://taosdata.feishu.cn/docx/Wcqwd8yyrob5SCxorAncKJmYnBc#doxcnpukrBAE93BJJXnrIXEo5Ge) 角色

用于在 audit 库中建表、写入数据，但不能删表/修改表/删除数据。该角色不能与 SYSDBA/SYSSEC/SYSAUDIT 角色同时授予某一个人。

#### 4.6.2 SYSINFO_0 角色

SYSINFO=0 的权限分类在 3.3.8 版本之前一直具备，为保持兼容性，改造为角色。部分权限如下，参见“[权限管理旧版 FS 文档](https://taosdata.feishu.cn/wiki/CFyuwFGCKimeXpkOHT2cTSAGn4M)”。
1. 不设置 ALTER SELF PASS 的权限
2. READ INFORMATION_SCHEMA LIMIT
3. READ PERFORMANCE_SCHEMA LIMIT
4. SHOW DATABASES 等权限
5. 可以将该角色直接授予用户或角色。

#### 4.6.3 SYSINFO_1 角色

SYSINFO=1 的权限分类在 3.3.8 版本之前一直具备，为保持兼容性，改造为角色。部分权限如下，参见 [权限管理旧版 FS 文档](https://taosdata.feishu.cn/wiki/CFyuwFGCKimeXpkOHT2cTSAGn4M)。
1. ALTER SELF PASS
2. READ INFORMATION_SCHEMA BASIC
3. READ PERFORMANCE_SCHEMA BASIC
4. SHOW USERS、SHOW CLUSTER 等权限
5. 可以将该角色直接授予用户或角色。

#### 4.6.4 SYSDBO 角色

数据库所有者权限在 3.3.8 版本之前一直具备，为保持兼容性，改造为角色。部分权限如下，参见“[权限管理旧版 FS 文档](https://taosdata.feishu.cn/wiki/CFyuwFGCKimeXpkOHT2cTSAGn4M)”。
1. 不设置 DB 外部的相关权限
2. DB 内部的权限，按和老版本的兼容性给与默认值(DB Owner）
3. 对自己拥有的数据库内部的、表、视图、索引、流等，具有所有的对象权限并可以授出与回收
4. 在用户创建完数据库之后，会在刚创建的数据库上，被赋予 SYSDBO  角色对应的权限。
5. 不能将该角色直接授予用户或角色，因为其作用对象是具体的数据库。

#### 4.6.5 SYSDBR 角色

参照老版本中的 DB READ 权限。
1. 在升级时，将老版本 DB READ 对应的权限，自动赋予用户。
2. 不能将该角色直接授予用户或角色，因为其作用对象是具体的数据库。

#### 4.6.6 SYSDBW 角色

参照老版本中的 DB WRITE 权限。
1. 在升级时，将老版本 DB WRITE 对应的权限，自动赋予用户。
2. 不能将该角色直接授予用户或角色，因为其作用对象是具体的数据库。

## 5. 性能

- 访问过程增加了校验，耗时会增加。根据不同的操作类型，增加幅度不应该超过 (20%-100%]。
- 通过最佳实践，使大多数权限检查操作在最短路径结束，以减少耗时。

## 6. 兼容性

- 支持从低版本停机后，自动升级至 3.4.0.0 及以上的版本；不支持滚动升级。升级后，无法再降级。

## 7. 运维

### 7.1 root 用户管理

1. TDengine 3.4.x.y 及后续版本遵循 RBAC 原则，在初始配置 DB 时，针对 root 用户的操作建议参考：[root 用户管理](https://taosdata.feishu.cn/docx/Wcqwd8yyrob5SCxorAncKJmYnBc#doxcnzEtDn3c6rmRBtAkrNaR61M)。  

### 7.2 最佳实践

1. TDengine 3.4.x.y 及后续版本，权限粒度区分比较细。运维人员如果针对每个用户逐条授予权限，则工作非常繁琐。一般建议创建几个常见的角色，将权限授予角色，再将角色授予用户。

## 8. 使用场景

- 针对数据库访问进行权限检查的操作。

## 9. 约束和限制

- 仅企业版支持，社区版不支持。

## 10. 常见错误和排查

- 用户操作失败，错误码对照表

| Error code | description | note |
| --- | --- | --- |
|  |  |  |

## 11. 可观测性

- 用户和角色拥有的权限可通过 show privileges 等命令查看。

## 12. 安装和卸载

- 无特殊要求

## 13. 文档

- 需要修改官网文档

## 14. 参考

- [访问控制 RS](https://taosdata.feishu.cn/wiki/Y12Ywd797ieHBBkVZsqcpsRgnAg)
- [权限管理-Function Spec ](https://taosdata.feishu.cn/wiki/CFyuwFGCKimeXpkOHT2cTSAGn4M)
- [20251014 访问控制需求评审记录](https://taosdata.feishu.cn/wiki/HfJswUGqfiGUh2kldHzcuFoznAb)

## 15. 附录

### 15.1 关联 FS 评审提出的需求 

1. 2025-10-15 16:40 审计评审时，simon 等提出的问题记录:
```sql {wrap}
系统升级或启动时，默认创建 3 个系统用户：root 作为 DBA，sso, auditor。                -- DONE
可以创建一个 user，赋予 auditor DB write 权限，并为其创建 token。用于写 audit 记录。  -- DONE
```

### 15.2 PG/MySQL/Oracle 等 DBMS 的 RBAC 

#### 15.2.1 概述

| DBMS | 核心理念 | 是否有强制三权分立模式？ | 超级用户权限限制 | 不可绕过审计 | 评价 |
| --- | --- | --- | --- | --- | --- |
| GaussDB | 安全优先，强制分离 | 是​​（核心特性） | 强​​（初始化后可剥离） | 是​​（AUDITADMIN独占） | 最严格，从设计上强制 |
| Oracle | 企业级特性，可选分离 | 部分（需购买AVDF等组件） | 中（依赖配置和流程） | 是​​（ Unified Auditing + 外部工具） | 功能强大，但依赖配置和额外采购 |
| PG 社区版 | 灵活性与扩展性 | 否​​（社区版） | 弱​​（超级用户无所不能） | 否​​（超级用户可控制所有审计） | 最弱，完全依赖管理规范 |
| MySQL社区版 | 简单易用 | 否​​ | 弱​​（超级用户无所不能） | 否​​（超级用户可控制所有审计） | 最弱，完全依赖管理规范 |
| TDengine 3.[0-3].x.y | 效率优先，部分权限 | 否 | 弱（超级用户无所不能） | 否​​（超级用户可控制所有审计） | 弱，无 RBAC |
| TDengine 3.4+ | 效率优先，安全并重 | 是（企业版） | 中（依赖配置和流程） | 是（SYSAUDIT独占) | 功能完善，依赖配置 |

#### 15.2.2 SYSDBA/SYSSEC/SYSAUDIT 关联

| 特性 | PostgreSQL | Oracle | OpenGauss |
| --- | --- | --- | --- |
| 最高权限角色 | SUPERUSER | SYSDBA | SYSDBA |
| 安全管理员角色 | 无内置专用角色 | 如 SYSMAN、DBA，或自定义角色 | SYSSEC |
| 审计管理员角色 | 无内置专用角色 | AUDIT_ADMIN | SYSAUDIT |
| SYSDBA 授予权 | 现有 SUPERUSER | 现有 SYSDBA (WITH ADMIN OPTION) | 现有 SYSDBA |
| SYSSEC 授予权 | 不适用 | SYSDBA | SYSDBA |
| SYSAUDIT 授予权 | 不适用 | SYSDBA (最佳实践) | SYSDBA |
| SYSAUDIT 自我复制 | 不适用 | 技术上允许 (WITH ADMIN OPTION) | 明确禁止 |
| 角色数量限制 | 无明确限制 | 无明确限制 | 无明确限制 |
| 优点 | - 简单直接：模型简单，易于理解和实现。 - 决策高效：没有复杂的制衡，管理任务可以快速完成。 - 高可用：可以创建多个 `SUPERUSER` 以防止单点故障。 | - 灵活性高：可以根据组织架构灵活地委托管理权限，满足复杂需求。 - 功能强大：丰富的权限和选项可以构建精细化的权限体系。 - 可扩展性好：可以根据业务增长需要，灵活增加管理员数量。 | - 强大的权力制衡：这是其最核心的优点。`SYSDBA` 有权任命所有人，但其所有行为都被 `SYSAUDIT` 独立且强制地审计。 - 职责分离明确：从技术上强制实现了三权分立，角色之间无法越权。 - 高可信度与可审计性：审计轨迹非常清晰，所有权限的源头都是 `SYSDBA` 的明确操作。 - 符合安全合规：满足对分权和强制审计有严格要求的场景。 |
| 缺点 | - 无内置制衡：这是最大的风险。任何一个 `SUPERUSER` 都可以做任何事，且没有任何内置机制来审计或阻止其滥用权限。 - 权限扩散风险高：由于没有数量限制和内置制衡，容易因管理疏忽而创建过多超级用户，极大地扩大攻击面。 - 审计依赖性：审计功能的有效性完全依赖于 `SUPERUSER` 的诚实。 | - 权限扩散风险：`WITH ADMIN OPTION` 的使用可能导致权限意外扩散，如果控制不当，会严重违反最小权限原则。 - 依赖最佳实践：模型的安全性严重依赖于管理员的经验和纪律，缺乏技术强制力。 - 审计链条复杂：如果权限被多次转授，追溯权限的最终来源会变得复杂。 | - 灵活性较低：管理上不够灵活，审计团队无法自主增加成员。 - 单点依赖：权限体系的扩展严重依赖于初始的 `SYSDBA`。 - 仍需管理规范：虽然技术上有制衡，但如果 `SYSDBA` 随意任命大量管理员，虽然行为可被审计，但仍会带来管理混乱和风险。因此，必须通过外部管理制度来约束其数量。 |

### 15.3 PG/MySQL/Oracle 等 DBMS 权限`授予/撤回` 语法

#### 15.3.1 PG

| grant/revoke 语句 | 特点 |
| --- | --- |
| ##### 系统权限 SUPERUSER 超级用户权限（所有操作权限） `ALTER ROLE role_name SUPERUSER/NOSUPERUSER;` CREATEDB 创建数据库权限 `ALTER ROLE role_name CREATEDB/NOCREATEDB;` CREATEROLE 创建 / 管理角色权限 `ALTER ROLE role_name CREATEROLE/NOCREATEROLE;` INHERIT 继承其他角色权限 `ALTER ROLE role_name INHERIT/NOINHERIT;` LOGIN 允许登录数据库 `ALTER ROLE role_name LOGIN/NOLOGIN;` REPLICATION 复制权限（用于流复制） `ALTER ROLE role_name REPLICATION/NOREPLICATION;` BYPASSRLS 绕过行级安全策略 `ALTER ROLE role_name BYPASSRLS/NOBYPASSRLS;` ##### 通用对象 ###### **Database** -- 授予数据库连接、创建对象等权限 GRANT CONNECT, CREATE, TEMPORARY ON DATABASE db_name TO role_name; -- 授予所有权限（需超级用户） GRANT ALL PRIVILEGES ON DATABASE db_name TO role_name; REVOKE CONNECT, CREATE ON DATABASE db_name FROM role_name; REVOKE ALL PRIVILEGES ON DATABASE db_name FROM role_name; ###### **Table** -- 基础权限（SELECT/INSERT/UPDATE/DELETE等） GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE table_name TO role_name; -- 列级权限 GRANT UPDATE (col1, col2) ON TABLE table_name TO role_name; -- 所有权限 GRANT ALL PRIVILEGES ON TABLE table_name TO role_name; REVOKE SELECT, INSERT ON TABLE table_name FROM role_name; REVOKE ALL PRIVILEGES ON TABLE table_name FROM role_name; ###### **View** GRANT SELECT, INSERT ON VIEW view_name TO role_name; GRANT ALL PRIVILEGES ON VIEW view_name TO role_name; REVOKE SELECT ON VIEW view_name FROM role_name; REVOKE ALL PRIVILEGES ON VIEW view_name FROM role_name; ##### **特有对象** ###### **Schema** GRANT USAGE, CREATE ON SCHEMA schema_name TO role_name; -- USAGE允许访问模式内对象 REVOKE USAGE ON SCHEMA schema_name FROM role_name; ###### **Sequence** GRANT USAGE, SELECT, UPDATE ON SEQUENCE seq_name TO role_name; REVOKE USAGE ON SEQUENCE seq_name FROM role_name; ###### **Function** GRANT EXECUTE ON FUNCTION func_name(param_types) TO role_name; REVOKE EXECUTE ON FUNCTION func_name(param_types) FROM role_name; ##### 批量授权 CREATE ROLE admin_role WITH CREATEDB CREATEROLE LOGIN; -- 创建角色并授予多个系统权限 GRANT admin_role TO user_name; -- 将角色权限授予用户 REVOKE admin_role FROM user_name; -- 撤回角色权限 | 1. 系统权限通用 alter role 语法实现。 1. 对象权限通过 grant/revoke 实现。 1. 语法非常标准，除 批量角色权限管理grant/revoke role 外，grant/revoke 均基于对象类型关键词(e.g. DATABASE, TABLE, VIEW)。 |

#### 15.3.2 MySQL

| grant/revoke 语句 | 特点 |
| --- | --- |
| ##### 系统权限 （通过 *.* 表示所有库） **ALL PRIVILEGES 所有权限（全局） ** GRANT ALL PRIVILEGES ON **.** TO 'user'@'host' WITH GRANT OPTION; REVOKE ALL PRIVILEGES ON **.** FROM 'user'@'host'; **CREATE USER 创建用户权限 ** GRANT CREATE USER ON **.** TO 'user'@'host'; REVOKE CREATE USER ON **.** FROM 'user'@'host'; **DROP USER 删除用户权限 ** GRANT DROP USER ON **.** TO 'user'@'host'; REVOKE DROP USER ON **.** FROM 'user'@'host'; **CREATE DATABASE 创建数据库权限 ** GRANT CREATE ON **.** TO 'user'@'host'; REVOKE CREATE ON **.** FROM 'user'@'host'; **DROP DATABASE 删除数据库权限 ** GRANT DROP ON **.** TO 'user'@'host'; REVOKE DROP ON **.** FROM 'user'@'host'; **SHUTDOWN 关闭数据库权限 ** GRANT SHUTDOWN ON **.** TO 'user'@'host'; REVOKE SHUTDOWN ON **.** FROM 'user'@'host'; **RELOAD 重新加载配置权限（如 FLUSH） ** GRANT RELOAD ON **.** TO 'user'@'host'; REVOKE RELOAD ON **.** FROM 'user'@'host'; **PROCESS 查看所有进程权限 ** GRANT PROCESS ON **.** TO 'user'@'host'; REVOKE PROCESS ON **.** FROM 'user'@'host'; **SUPER 超级权限（如 KILL 任意进程、修改全局变量） ** GRANT SUPER ON **.** TO 'user'@'host'; REVOKE SUPER ON **.** FROM 'user'@'host'; ##### 通用对象 ###### **Database** -- 数据库级权限（SELECT/INSERT等作用于库内所有表） GRANT SELECT, INSERT ON db_name.* TO 'user'@'host'; -- 管理权限（创建库、授权等） GRANT CREATE, ALTER ON db_name*.** TO 'user'@'host'; GRANT ALL PRIVILEGES ON db_name.* TO 'user'@'host' WITH GRANT OPTION; -- 允许转授权 REVOKE SELECT ON db_name.* FROM 'user'@'host'; REVOKE ALL PRIVILEGES ON db_name.* FROM 'user'@'host'; ###### **Table** GRANT SELECT, UPDATE (col1) ON db_name.table_name TO 'user'@'host'; -- 列级权限 GRANT ALL PRIVILEGES ON db_name.table_name TO 'user'@'host'; REVOKE UPDATE (col1) ON db_name.table_name FROM 'user'@'host'; REVOKE ALL PRIVILEGES ON db_name.table_name FROM 'user'@'host'; ###### **View** GRANT SELECT, INSERT ON db_name.view_name TO 'user'@'host'; -- 需视图可更新 REVOKE SELECT ON db_name.view_name FROM 'user'@'host'; ##### **特有对象** ###### **存储过程 / 函数（Procedure/Function）** GRANT EXECUTE ON PROCEDURE db_name.proc_name TO 'user'@'host'; GRANT EXECUTE ON FUNCTION db_name.func_name TO 'user'@'host'; REVOKE EXECUTE ON PROCEDURE db_name.proc_name FROM 'user'@'host'; ###### **事件（Event）** GRANT EVENT ON db_name.* TO 'user'@'host'; -- 事件调度权限 REVOKE EVENT ON db_name.* FROM 'user'@'host'; |  |

#### 15.3.3 Oracle

| grant/revoke 语句 | 特点 |
| --- | --- |
| ##### 系统权限 **DBA 数据库管理员权限（所有系统权限） ** GRANT DBA TO user_name; REVOKE DBA FROM user_name; **CREATE SESSION 连接数据库权限 ** GRANT CREATE SESSION TO user_name; REVOKE CREATE SESSION FROM user_name; **CREATE DATABASE 创建数据库权限（仅初始化时可用） ** GRANT CREATE DATABASE TO user_name; REVOKE CREATE DATABASE FROM user_name; **CREATE TABLE 创建表权限（需指定表空间） ** GRANT CREATE TABLE TO user_name; REVOKE CREATE TABLE FROM user_name; **CREATE VIEW 创建视图权限 ** GRANT CREATE VIEW TO user_name; REVOKE CREATE VIEW FROM user_name; **CREATE PROCEDURE 创建存储过程权限 ** GRANT CREATE PROCEDURE TO user_name; REVOKE CREATE PROCEDURE FROM user_name; **DROP ANY TABLE 删除任意用户的表权限 ** GRANT DROP ANY TABLE TO user_name; REVOKE DROP ANY TABLE FROM user_name; **ALTER ANY TABLE 修改任意用户的表权限 ** GRANT ALTER ANY TABLE TO user_name; REVOKE ALTER ANY TABLE FROM user_name; **SELECT ANY TABLE 查询任意用户的表权限 ** GRANT SELECT ANY TABLE TO user_name; REVOKE SELECT ANY TABLE FROM user_name; **CREATE USER 创建用户权限 ** GRANT CREATE USER TO user_name; REVOKE CREATE USER FROM user_name; **DROP USER 删除用户权限 ** GRANT DROP USER TO user_name; REVOKE DROP USER FROM user_name; ##### 通用对象 ###### **Database（通过角色或系统权限控制）** -- 系统权限（作用于整个数据库） GRANT CREATE SESSION TO user_name; -- 连接数据库 GRANT CREATE TABLE TO user_name; -- 创建表 REVOKE CREATE SESSION FROM user_name; ###### **Table** GRANT SELECT, INSERT, UPDATE (col1) ON schema_name.table_name TO user_name; -- 列级权限 GRANT ALL PRIVILEGES ON schema_name.table_name TO user_name WITH GRANT OPTION; REVOKE UPDATE (col1) ON schema_name.table_name FROM user_name; REVOKE ALL PRIVILEGES ON schema_name.table_name FROM user_name; ###### **View** GRANT SELECT ON schema_name.view_name TO user_name; GRANT INSERT ON schema_name.view_name TO user_name; -- 需视图可更新 REVOKE SELECT ON schema_name.view_name FROM user_name; ##### **特有对象** ###### **Sequence** GRANT SELECT, ALTER ON schema_name.seq_name TO user_name; REVOKE SELECT ON schema_name.seq_name FROM user_name; ###### **Procedure** GRANT EXECUTE ON schema_name.proc_name TO user_name; REVOKE EXECUTE ON schema_name.proc_name FROM user_name; ###### **Synonym** GRANT CREATE SYNONYM TO user_name; -- 创建同义词权限 GRANT SELECT ON schema_name.synonym_name TO user_name; -- 访问同义词 | **Oracle 没有 “独立数据库” 的概念，而是通过 “表空间（Tablespace）” 和 “用户（Schema）” 实现资源隔离**，用户的所有对象（表、视图等）默认存储在自己的 Schema 下，且权限控制本质是 “用户对对象的操作权限” 而非 “对某个数据库的权限”（这与 MySQL、PG 的 “多数据库” 架构有本质区别） |

#### 15.3.4 TiDB

| grant/revoke 语句 | 特点 |
| --- | --- |
| ##### 系统权限 兼容 MySQL ##### 通用对象 ###### **Database** GRANT SELECT, INSERT ON db_name.* TO 'user'@'host'; GRANT ALL PRIVILEGES ON db_name.* TO 'user'@'host' WITH GRANT OPTION; REVOKE SELECT ON db_name.* FROM 'user'@'host'; ###### **Table** GRANT UPDATE (col1) ON db_name.table_name TO 'user'@'host'; REVOKE UPDATE (col1) ON db_name.table_name FROM 'user'@'host'; ##### **特有对象** ###### **TiFlash 副本权限** GRANT FLASHBACK ON db_name.* TO 'user'@'host'; -- 闪回权限 REVOKE FLASHBACK ON db_name.* FROM 'user'@'host'; |  |

#### 15.3.5 OceanBase

| grant/revoke 语句 | 特点 |
| --- | --- |
| ##### 系统权限 分兼容模式，MySQL 或 Oracle ##### 通用对象 ###### **Database/Table/View** GRANT SELECT ON db_name.table_name TO 'user'@'host'; REVOKE SELECT ON db_name.table_name FROM 'user'@'host'; GRANT SELECT ON schema_name.table_name TO user_name; REVOKE SELECT ON schema_name.table_name FROM user_name; ##### **特有对象** ###### **资源池（Resource Pool）** GRANT MANAGE RESOURCE POOL TO user_name; -- 管理资源池权限 |  |

#### 15.3.6 达梦

| grant/revoke 语句 | 特点 |
| --- | --- |
| ##### 系统权限 兼容 Oracle ##### 通用对象 ###### **Database** GRANT CREATE SESSION TO user_name; -- 连接权限 GRANT DBA TO user_name; -- 管理员权限 REVOKE CREATE SESSION FROM user_name; ###### **Table** GRANT SELECT, INSERT ON schema_name.table_name TO user_name; GRANT UPDATE (col1) ON schema_name.table_name TO user_name; REVOKE INSERT ON schema_name.table_name FROM user_name; ##### **特有对象** ###### **外部表（External Table）** GRANT SELECT ON schema_name.ext_table_name TO user_name; ###### **包（Package）** GRANT EXECUTE ON schema_name.package_name TO user_name; |  |

#### 15.3.7 TDengine 3.[0-3].x.y

| grant/revoke 语句 | 特点 |
| --- | --- |
| ##### 系统权限 ```sql {wrap} CREATE USER user_name PASS 'password' [SYSINFO {1|0}] [CREATEDB {1|0}]; ALTER USER user_name alter_user_clause alter_user_clause: { PASS 'literal' | ENABLE value | SYSINFO value | CREATEDB value } ``` ##### 通用对象 ###### **Database/Table** ```sql {wrap} GRANT privileges ON priv_level [WITH tag_condition] TO user_name; REVOKE privileges ON priv_level [WITH tag_condition] FROM user_name; privileges : { ALL | priv_type [, priv_type] ... } priv_type : { READ | WRITE } priv_level : { dbname.tbname | dbname.* | *.* } ``` ##### **特有对象** ###### **消息订阅** ```sql {wrap} GRANT SUBSCRIBE ON topic_name TO user_name REVOKE SUBSCRIBE ON topic_name FROM user_name ``` | 1. 系统权限目前的 SYSINFO, CREATEDB 2 种，操作对象为 user，语法类似 PG。 1. DB/Table/Topic 等对象权限，语法类似 Oracle。 |

### 15.4 OpenGuass 针对初始超级用户和 SYSADMIN, CREATEROLE, AUDITADMIN 的处理
