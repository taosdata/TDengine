# 访问控制 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-10-28 | - | 0.1 | 徐开礼 | 基于 [RS](https://taosdata.feishu.cn/wiki/Y12Ywd797ieHBBkVZsqcpsRgnAg) 撰写初稿 |
| 2025-11-03 | - | 0.2 | 徐开礼 | 基于 FS 评审进行修改 [审计管理权限](https://taosdata.feishu.cn/wiki/SxFfwi3p0iTPZxkO5pRco9BmnKd#Ba70duG7Bo3AGSx3tlTcCAPXn3d) |
| 2025-11-04 | - | 0.3 | 徐开礼 | 基于 FS 评审进行修改 |
| 2025-11-19 | 2025-11-20 | 1.0 | 关胜亮 | 修订格式 |
| 2026-03-05 | 2026-03-05 | 1.1 | 徐开礼 | [4.7 兼容 3.3 版本语法](https://taosdata.feishu.cn/wiki/SxFfwi3p0iTPZxkO5pRco9BmnKd#M9J1dAhWhoiT74xFJ63cfPS7nqd) |

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
在 TDengine 3.4.x.y 及后续支持 RBAC 的企业版中，在系统初始配置将`数据库管理员（SYSDBA）`、`数据库安全员（SYSSEC）` 和`数据库审计员（SYSAUDIT）` 3 个系统角色赋予不同的用户后，建议由 SYSDBA 锁定 root 用户，以减少安全风险。系统中拥有 SYSDBA 角色的用户需要 1 名及以上，否则，不允许锁定 root。
</callout>

### 3.4 数据库管理员 SYSDBA

- [数据库管理员](https://taosdata.feishu.cn/wiki/SxFfwi3p0iTPZxkO5pRco9BmnKd#S5YLdty7RoMjU5xSMj0c5SLBnDc)是一种权限最高的系统内置管理员角色，负责数据库的日常运维和系统管理。具备除`数据库安全员`、`数据库审计员`之外的所有系统权限。

### 3.5 数据库安全员 SYSSEC

- [数据库安全员](https://taosdata.feishu.cn/wiki/SxFfwi3p0iTPZxkO5pRco9BmnKd#CnnRd0X7QoH27HxBWGwc7O6KnRc)是一种系统内置角色，主要职责是制定并应用安全策略，强化系统安全机制。

### 3.6 数据库审计员 SYSAUDIT

- [数据库审计员](https://taosdata.feishu.cn/wiki/SxFfwi3p0iTPZxkO5pRco9BmnKd#D1D5d21Cao8ukux00YqcrAZBnAe)是一种系统内置的负责独立审计监督的关键角色，核心职责是监控和审查数据库操作，确保所有行为合规，但不能查看业务数据。

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
| 2 | ALTER [DATABASE] |  | SYSDBA owner |  |
| 3 | DROP [DATABASE] |  | SYSDBA | owner 可以删除自己创建的 db，不需要额外的权限(例外：audit db 需要 drop audit db 权限)。 针对 [TS-7279](https://jira.taosdata.com:18080/browse/TS-7279)，需要云服务创建的用户，具有 DROP DATABASE 的权限。 |
| ~~4~~ | ~~DROP OWNED DATABASE~~ |  | ~~SYSDBA~~ |  |
| 5 | USE [DATABASE] |  | SYSDBA owner |  |
| 6 | FLUSH [DATABASE] |  | SYSDBA owner |  |
| 7 | COMPACT [DATABASE] |  | SYSDBA owner | COMPACT VGROUP 操作检查 COMPACT DATABASE 权限 |
| 8 | TRIM [DATABASE] |  | SYSDBA owner |  |
| 9 | ROLLUP [DATABASE] |  | SYSDBA owner | ROLLUP VGROUP 操作检查 ROLLUP DATABASE 权限 |
| 10 | SCAN [DATABASE] |  | SYSDBA owner | SCAN VGROUP 操作检查 SCAN DATABASE 权限 |
| 11 | SSMIGRATE [DATABASE] |  | SYSDBA owner |  |
| 12 | BALANCE VGROUP | 是 | SYSDBA 且不允许授予其他人或角色 | 理论上也应该是对象权限。为了功能正常执行（BALANCE VGROUP 操作作用于多个 DB），设置为系统权限。 |
| 13 | BALANCE VGROUP LEADER | 是 | SYSDBA 且不允许授予其他人或角色 |
| 14 | MERGE VGROUP | 是 | SYSDBA 且不允许授予其他人或角色 |
| 15 | REDISTRIBUTE VGROUP | 是 | SYSDBA 且不允许授予其他人或角色 |
| 16 | SPLIT VGROUP | 是 | SYSDBA 且不允许授予其他人或角色 |
| 17 | SHOW [DATABASES] |  |  | 拥有 `所有 DB`或 `该 DB` 的 show databases 权限以及 db owner，返回；否则不返回。 |
| 18 | SHOW VNODES |  |  | 拥有 `所有 DB`或 `该 DB` 的 show vnodes 权限以及 db owner，返回；否则不返回。 |
| 19 | SHOW VGROUPS |  |  | 拥有 `所有 DB`或 `该 DB` 的 show vgroups 权限以及 db owner，返回；否则不返回。 |
| 20 | SHOW COMPACTS |  |  | 拥有 `所有 DB`或 `该 DB` 的 show compacts 权限以及 db owner，返回；否则不返回。 |
| 21 | SHOW RETENTIONS |  |  | 同上 |
| 22 | SHOW SCANS |  |  | 同上 |
| 23 | SHOW SSMIGRATES |  |  | 同上 |

#### 4.2.2 表权限

- 如下权限，包括 `普通表、超级表、子表、虚拟超级表、虚拟子表` 等类型的表，暂不针对表的类型区分权限。
- 子表继承超级表的权限。
- 目前，只有 select table，insert table，delete table，支持在 grant 时指定超级表 + with col_tag condition。其他场景，暂不支持，需要明确指定超级表，或者子表，或者子表继承超级表权限。

| 序号 | 权限名称(privilege name) **注：括号后边的内容，表示在grant/revoke 语句时不需要包含，例如，grant drop on d0.stb0 to u1 或 grant drop on table d0.* to u1; ** | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 1）以下操作，均需要先拥有 USE DATABASE 的权限。 2）如果执行下述 grant 操作时，没有 dbname 或 * 的 USE DATABASE 权限时，会报错，提示用户先授予 USE DATABASE 权限：Cannot grant CREATE TABLE privilege since lack of USE DATABASE privilege on {dbname} or *" |
| --- | --- | --- | --- | --- | --- |
| 1 | CREATE TABLE 1) create stable 2) create table 3) create vtable |  | SYSDBA db owner | GRANT `{privilege name}` ON DATABASE dbname TO user_name/role_name; GRANT `{privilege name}` ON DATABASE * TO user_name/role_name; REVOKE `{privilege name}` ON DATABASE dbname FROM user_name/role_name; REVOKE `{privilege name}` ON DATABASE * FROM user_name/role_name; | USE DATABASE + 是否拥有某个或所有 DB 的 CREATE TABLE 权限 |
| 2 | DROP [TABLE] drop stable/drop table drop table // child table |  | SYSDBA tb owner | USE DATABASE + 是否拥有某个或所有 DB 的 DROP TABLE 权限 1) grant 不支持 WITH col_tag_condition。需要明确指定超级表，或者子表，或者子表继承超级表权限。实际使用中，不建议指定大量子表，否则，元数据占用空间过大。 2) all 权限时，忽略 col_tag_condition。 3) drop table with 语句暂不开放，只限 root 用户使用。 |
| 3 | ALTER [TABLE] alter stable alter child table set tags alter ntable |  | SYSDBA tb owner | USE DATABASE + 是否拥有某个或所有 DB 的 ALTER TABLE 权限 1) grant 不支持 WITH col_tag_condition，需要明确指定超级表，或者子表，或者子表继承超级表权限。实际使用中，不建议指定大量子表，否则，元数据占用空间过大。 2) all 权限时，忽略 col_tag_condition。 |
| 4 | SHOW [TABLES] 1) show stables 2) show tables（暂不支持） |  | SYSDBA SYSSEC SYSAUDIT tb owner | Can't show tables created by others at default https://jira.taosdata.com:18080/browse/TS-6667 USE DATABASE + SHOW TABLES 1) grant 不支持 WITH col_tag_condition，需要明确指定超级表，或者子表，或者子表继承超级表权限。实际使用中，不建议指定大量子表，否则，元数据占用空间过大。 2) all 权限时，忽略 col_tag_condition。 |
| 5 | SHOW CREATE [TABLE] |  | SYSDBA SYSSEC SYSAUDIT tb owner | Can't desc tables created by others at default USE DATABASE + SHOW CREATE TABLE 1) grant 不支持 WITH col_tag_condition，需要明确指定超级表，或者子表，或者子表继承超级表权限。实际使用中，不建议指定大量子表，否则，元数据占用空间过大。 2) all 权限时，忽略 col_tag_condition。 |
| 6 | SELECT AUDIT TABLE |  | SYSAUDIT，不可授予其他人或角色 | 1) grant 支持超级表 WITH col_tag_condition，只支持直接指定超级表和普通表，或者子表继承超级表权限，不支持子表名。 ```cpp {wrap} 不支持子表名的原因： 如果设置某个或一些子表，例如， ctb0 with ts?。如果只查子表，没有问题。如果查询超级表时，要查询所有拥有权限的子表，并且对查询结果进行过滤，实现复杂度高，收益不大。 因此，针对 select/insert/delete，只支持 stb with col_tag_condition ``` 2）revoke 时，要检查依赖的 topic/stream/rsma/tsma 等。 |
| 7 | INSERT AUDIT TABLE |  | SYSAUDIT_LOG，不可授予其他人或角色 | 1) grant 支持超级表 WITH tag_condition，只支持直接指定超级表和普通表，或者子表继承超级表权限，不支持子表名。 2）revoke 时，要检查依赖的 rsma 等。 |
| 8 | UPDATE [TABLE] |  | 不支持 | 预留。TDengine 暂无 UPDATE 语句，而是通过 INSERT 语句自动实现 UPDATE。 |
| 9 | DELETE [TABLE] |  | table owner | 1) grant 支持超级表 WITH tag_condition，只支持直接指定超级表和普通表，或者子表继承超级表权限，不支持子表名。 |

#### 4.2.3 列权限

- 对给定表的给定列的读写权限控制(可指定列是否脱敏展示)
```sql {wrap}
行+列+tag 权限规则，
2.0） 只适用于 select/insert。
2.1） 只能指定超级表或者普通表，不可以指定子表。
2.2） 下述规则，一个超级表或者普通表，只能指定一条规则。
2.3） 如果权限对象为所有表，即 *，则不能指定 with 条件，不能指定 cols。
2.4） 支持在 cols 中，设置 mask 标记。mask 标记，只针对 select 语句生效。
以 stb0 为例:
// -- 可以查所有列
grant select on stb0; // 可以查所有超级表，包括子表。
grant select on stb0 with ts=100; // 可以查某些子表
grant select on stb0 with t0=100; // 可以查某些行
grant select on stb0 with t0=100 and ts=100; // 可以查某些子表的某些行
// -- 只能查某些列
grant select(c0,c1) on stb0; // 可以查所有超级表，包括子表, 只能查某些列
grant select(c0,c1) on stb0 with ts=100; // 可以查某些子表, 只能查某些列
grant select(c0,c1) on stb0 with t0=100; // 可以查某些行, 只能查某些列
grant select(c0,c1) on stb0 with t0=100 and ts=100; // 可以查某些子表的某些行, 只能查某些列
// -- select 中可以存储 mask 标记
grant select(c0,mask(c1)) on d0.t1 to u1;
2.5） 要想修改权限，需要把原来的规则 revoke。
// revoke select on stb0，即可以收回上述任一条规则。只需要指定权限类型 select 和 权限对象 stb0，其他的条件，不需要指定，指定了也会忽略，只会根据 权限类型 select 和 权限对象 stb0 执行。
2.6） 如果还有查询所有表(*)的通配符规则，e.g. grant select on d0.* to u1，则更细粒度权限对象的规则优先，粗粒度对象规则靠后。针对 stb0，细粒度的 stb0 的规则生效；针对 stb1，因为没有细粒度的 stb1 规则，所以粗粒度的通配符 * 规则生效。
2.7）如果用户和角色同时指定了 stb0 的权限，规则如下：
2.7.1）优先取用户的 stb0 权限；
2.7.2）系统角色的权限不可更新，因此，不存在 stb0 的 select 问题。
2.7.3）普通角色，如果存在多条 stb0 的权限，则更新时间靠后的生效。
总结：1）用户显式精准的细粒度规则 > 角色精准的细粒度规则 > 用户通配符规则 > 角色通配符规则；2）多个普通角色如果存在多条相同规则，更新时间靠后的生效。
```

#### 4.2.4 行权限

- 对给定表的给定行的数据范围的权限控制，指定时间戳范围。
```cpp {wrap}
参照列权限的描述，行权限通过 with col_tag condition，与 tag 条件一同指定，天然支持行权限功能。
```

#### 4.2.5 自定义函数权限

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | CREATE FUNCTION | 是 | SYSDBA |  |
| 2 | DROP FUNCTION | 是 | SYSDBA |  |
| 3 | SHOW FUNCTIONS | 是 | SYSDBA SYSSEC SYSAUDIT SYSINFO_0 SYSINFO_1 |  |

#### 4.2.6 索引权限

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | CREATE INDEX |  | SYSDBA | GRANT `{privilege name}` ON TABLE *.* TO user_name/role_name; GRANT `{privilege name}` ON TABLE dbname.* TO user_name/role_name; GRANT `{privilege name}` ON TABLE dbname.tbname TO user_name/role_name; REVOKE `{privilege name}` ON TABLE *.* FROM user_name/role_name; REVOKE `{privilege name}` ON TABLE dbname.* FROM user_name/role_name; REVOKE `{privilege name}` ON TABLE dbname.tbname FROM user_name/role_name; | 创建 INDEX，需要 USE DATABASE + 表的 CREATE INDEX 权限 + 对应列或表的 SELECT 权限。 |
| 2 | DROP [INDEX] |  | SYSDBA owner | USE DATABASE(or db owner) + DROP INDEX(or index owner) |
| 3 | SHOW [INDEXES] |  | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 SYSINFO_0 owner | 基础信息一般不限制查看，前提是权限级别不能太低。 |

#### 4.2.7 SMA 权限

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 前提：USE DATABASE |
| --- | --- | --- | --- | --- | --- |
| 1 | CREATE RSMA |  | SYSDBA | GRANT `{privilege name}` ON TABLE *.* TO user_name/role_name; GRANT `{privilege name}` ON TABLE dbname.* TO user_name/role_name; GRANT `{privilege name}` ON TABLE dbname.tbname TO user_name/role_name; REVOKE `{privilege name}` ON TABLE *.* FROM user_name/role_name; REVOKE `{privilege name}` ON TABLE dbname.* FROM user_name/role_name; REVOKE `{privilege name}` ON TABLE dbname.tbname FROM user_name/role_name; | 前提：USE DATABASE + TABLE(超级表) SELECT/INSERT 权限。 |
| 2 | DROP [RSMA] |  | SYSDBA owner | USE DATABASE + DROP RSMA 权限 |
| 3 | ALTER [RSMA] |  | SYSDBA owner |  |
| 4 | SHOW [RSMAS] |  | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 SYSINFO_0 owner | 拥有 所有 rsma 或 该 rsma 的 show 权限，返回；否则不返回。 |
| 5 | SHOW CREATE [RSMA] |  | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 SYSINFO_0 owner | USE DATABASE + SHOW CREATE RSMA or owner |
| 6 | CREATE TSMA |  | SYSDBA | GRANT `{privilege name}` ON TABLE *.* TO user_name/role_name; GRANT `{privilege name}` ON TABLE dbname.* TO user_name/role_name; GRANT `{privilege name}` ON TABLE dbname.tbname TO user_name/role_name; REVOKE `{privilege name}` ON TABLE *.* FROM user_name/role_name; REVOKE `{privilege name}` ON TABLE dbname.* FROM user_name/role_name; REVOKE `{privilege name}` ON TABLE dbname.tbname FROM user_name/role_name; | 检查 5 种权限：USE DATABASE + TABLE(超级表) SELECT TABLE/CREATE STREAM/CREATE TSMA + CREATE TABLE on db 权限 |
| 7 | DROP [TSMA] |  | SYSDBA owner | USE DATABASE + DROP TSMA 权限 |
| 8 | SHOW [TSMAS] |  | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 SYSINFO_0 owner | 拥有 所有 tsma 或 该 tsma 的 show 权限，返回；否则不返回。 |

#### 4.2.8 视图权限

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | CREATE VIEW |  | SYSDBA | GRANT `{privilege name}` ON DATABASE *.* TO user_name/role_name; GRANT `{privilege name}` ON DATABASE dbname.* TO user_name/role_name; GRANT `{privilege name}` ON DATABASE dbname.tbname TO user_name/role_name; REVOKE `{privilege name}` ON DATABASE *.* FROM user_name/role_name; REVOKE `{privilege name}` ON DATABASE dbname.* FROM user_name/role_name; REVOKE `{privilege name}` ON DATABASE dbname.tbname FROM user_name/role_name; | 前提：USE DATABASE + TABLE 或对应 COLUMN/ROW 的 READ 权限 |
| 2 | DROP [VIEW] |  | SYSDBA owner |  |
| 3 | ALTER [VIEW] |  | SYSDBA owner |  |
| 4 | SHOW [VIEWS] |  | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 SYSINFO_0 owner |  |
| 5 | SELECT [VIEW] |  | owner | 在创建时检查了基础表的 select 权限，执行时暂沿用 3.3.x.y 的逻辑，暂未检查，后续完善。 |

#### 4.2.9 挂载权限

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | CREATE MOUNT | 是 | SYSDBA |  |
| 2 | DROP MOUNT | 是 | SYSDBA | DROP MOUNT |
| 3 | SHOW MOUNTS | 是 | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 |  |

#### 4.2.10 角色权限

SYSDBA 拥有 CREATE/DROP ROLE 权限，SYSDBA/SYSSEC/SYSAUDIT/SYSINFO_1 均拥有 SHOW ROLES 权限。

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | CREATE ROLE | 是 | SYSDBA |  |
| 2 | DROP ROLE | 是 | SYSDBA | owner 如果没有 drop role 权限，则不能删除自己创建的 role |
| 3 | SHOW ROLES | 是 | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 |  |
| 4 | UNLOCK ROLE | ~~是~~ | SYSDBA SYSSEC |  |  |
| 5 | LOCK ROLE | ~~是~~ | SYSDBA SYSSEC |  | SYSDBA/SYSSEC/SYSAUDIT/SYSAUDIT_LOG 不允许被 lock, SYSINFO_1 和 SYSINFO_0 允许被 lock |

#### 4.2.11 用户权限

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | CREATE USER | 是 | SYSDBA |  |
| 2 | DROP USER | 是 | SYSDBA | owner 如果没有 drop user 权限，也不能删除自己创建的 user |
| 3 | ALTER USER | 是 | SYSDBA SYSSEC SYSAUDIT |  |
| 4 | SET USER SECURITY INFORMATION | 是 | SYSSEC | 修改用户时，可以设置安全相关信息 ```cpp Sysinfo => Totpseed 任何人可以修改 FailedLoginAttempts PasswordLifeTime PasswordReuseTime PasswordReuseMax PasswordLockTime PasswordGraceTime InactiveAccountTime numIpRanges numDropIpRanges numTimeRanges numDropTimeRanges ``` |
| 5 | SET USER AUDIT INFORMATION | 是 | SYSAUDIT | 修改用户时，可以设置审计相关信息 ```cpp // 暂无 ``` |
| 6 | SET USER BASIC INFORMATION | 是 | SYSDBA | 修改用户时，可以设置与安全/审计无关的基础信息 ```cpp Createdb SessionPerUser ConnectTime ConnectIdleTime CallPerSession VnodePerCall AllowTokenNum ``` |
| 7 | UNLOCK USER | 是 | SYSDBA SYSSEC |  |
| 8 | LOCK USER | 是 | SYSDBA SYSSEC | 确保系统中至少有一个有效的 SYSDBA，可以是 root，也可以是普通用户 |
| 9 | SHOW USERS | 是 | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 | 可以执行 show users 和 show users full 命令 |
| 10 | SHOW USERS SECURITY INFORMATION | 是 | SYSDBA SYSSEC SYSAUDIT |  | 拥有该权限，show users full 命令的 security 相关的字段展示明文，否则脱敏展示为 `*` |

#### 4.2.12 令牌权限

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | [CREATE TOKEN](https://taosdata.feishu.cn/wiki/CXXqwV3Fai36rQkby6zcmBWwnMd#BCk5dKEoKo9KttxWjUGcjcNWnyf) | 是 | SYSSEC owner |  |
| 2 | DROP TOKEN | 是 | SYSSEC owner |  |
| 3 | ALTER TOKEN | 是 | SYSSEC owner |  |
| 4 | SHOW TOKENS | 是 | SYSDBA SYSSEC SYSAUDIT owner | TODO: systable 目前列只有 sysInfo true/false 两种取值，可以扩展以支持 SYSAUDIT 进行查看某些列。 - SYSAUDIT 可以看到： - 令牌ID - 所属用户 - 创建时间 - 最后使用时间 - 状态（活跃/禁用） - 权限范围 - SYSAUDIT 不应看到： - 令牌具体密钥值 - 加密的令牌内容 - 签名密钥材料 - owner 只能查看自己的 TOKEN |

#### 4.2.13 密钥权限

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | UPDATE KEY(暂不支持) | 是 | SYSDBA | 更新 SVR_KEY、DB_KEY |
| 2 | [CREATE TOTP](https://taosdata.feishu.cn/wiki/CXXqwV3Fai36rQkby6zcmBWwnMd#P7hEdjsOoo9IMyxlg4JcjPo7ne6) | 是 | SYSSEC owner |  |
| 3 | DROP TOTP | 是 | SYSSEC owner |  |

#### 4.2.14 密码权限

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | ALTER PASS | 是 | SYSDBA |  |
| 2 | ALTER SELF PASS | 是 | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 SYSINFO_0 |  |

#### 4.2.15 权限授予回收的权限

- 授予和回收权限时，将自己拥有的对象的部分或全部访问权限授予其他用户，或从其他用户回收已授予的权限。
- GRANT 暂不支持 WITH GRANT OPTION。因此，列表中的权限，只能由 SYSDBA/SYSSEC/SYSAUDIT 和 Owner 授予/撤回 。

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | GRANT PRIVILEGE | 是 | SYSSEC |
| 2 | REVOKE PRIVILEGE | 是 | SYSSEC |
| 3 | SHOW PRIVILEGES | 是 | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 |  |
| 4 | GRANT SYSDBA PRIVILEGE | 是 | SYSDBA 不支持授予或撤回 |
| 5 | REVOKE SYSDBA PRIVILEGE | 是 | SYSDBA 不支持授予或撤回 |
| 6 | GRANT SYSSEC PRIVILEGE | 是 | SYSSEC 不支持授予或撤回 |
| 7 | REVOKE SYSSEC PRIVILEGE | 是 | SYSSEC 不支持授予或撤回 |
| 8 | GRANT SYSAUDIT PRIVILEGE | 是 | SYSAUDIT 不支持授予或撤回 |
| 9 | REVOKE SYSAUDIT PRIVILEGE | 是 | SYSAUDIT 不支持授予或撤回 |

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
| ~~16~~ | ~~CREATE ANODE~~ | 是 |  |
| ~~17~~ | ~~DROP ANODE~~ | 是 |  |
| ~~18~~ | ~~SHOW ANODES~~ | 是 | 不受权限控制，任何人均可查看。 |
| 19 | ~~CREATE XNODE~~ | 是 |  |
| 20 | ~~DROP XNODE~~ | 是 |  |
| 21 | ~~SHOW XNODES~~ | 是 | 不受权限控制，任何人均可查看。 |

#### 4.2.17 系统参数调整权限

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | ALTER SECURITY VARIABLE | 是 | SYSSEC | 修改安全参数 |
| 2 | ALTER AUDIT VARIABLE | 是 | SYSAUDIT | 修改审计参数 |
| 3 | ALTER SYSTEM VARIABLE | 是 | SYSDBA | 修改系统参数 |
| 4 | ALTER DEBUG VARIABLE | 是 | SYSDBA | 修改调试参数 |
| 5 | SHOW SECURITY VARIABLES | 是 | sysinfo = 1 暂沿用原控制逻辑 | 查看安全参数 |
| 6 | SHOW AUDIT VARIABLES | 是 | sysinfo = 1 暂沿用原控制逻辑 | 查看审计参数 |
| 7 | SHOW SYSTEM VARIABLES | 是 | sysinfo = 1 暂沿用原控制逻辑 | 查看系统参数 |
| 8 | SHOW DEBUG VARIABLES | 是 | sysinfo = 1 暂沿用原控制逻辑 | 查看调试参数 |

#### 4.2.18 订阅权限

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | CREATE TOPIC |  | SYSDBA | GRANT privileges ON DATABASE * TO {user_name|role_name}; GRANT privileges ON DATABASE dbname TO {user_name|role_name}; REVOKE privileges ON DATABASE * FROM {user_name|role_name}; REVOKE privileges ON DATABASE dbname FROM {user_name|role_name}; | USE DATABASE + select table + create topic |
| 2 | DROP [TOPIC] |  | SYSDBA owner | USE DATABASE + drop topic |
| 3 | SHOW [TOPICS] |  | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 SYSINFO_0 owner | 拥有 所有 topic 或 该 topic 的 show 权限，返回；否则不返回。 |
| 4 | SHOW CONSUMERS |  | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 SYSINFO_0 db owner + topic owner + subscribe owner |  |
| 5 | SHOW SUBSCRIPTIONS |  | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 SYSINFO_0 db owner + topic owner + subscribe owner(仅返回自已的订单) |  |
| 6 | SUBSCRIBE |  | db owner + topic owner | grant subscribe on topic dbName.topicName to user_name/role_name; | 旧的语法不再兼容 前提：拥有 topic 所在 DB 的 USE DATABASE 权限 |

#### 4.2.19 流计算权限


| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | CREATE STREAM |  | SYSDBA |  | 检查 4 种权限：USE DATABASE + CREATE STREAM(fetch tables) + CREATE TABLE on db 权限 |
| 2 | DROP [STREAM] |  | SYSDBA owner | USE DATABASE + DROP STREAM |
| 3 | SHOW [STREAMS] |  | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 SYSINFO_0 owner | 拥有所有 stream 或 该 stream 的 show 权限，返回；否则不返回。 |
| 4 | START [STREAM] |  | SYSDBA owner | USE DATABASE + START STREAM |
| 5 | STOP [STREAM] |  | SYSDBA owner | USE DATABASE + STOP STREAM |
| 6 | RECALCULATE [STREAM] |  | SYSDBA owner | USE DATABASE + RECALCULATE STREAM |

#### 4.2.20 系统管理权限

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | SHOW TRANSACTIONS | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 SYSINFO_0 |  |
| 2 | KILL TRANSACTION | SYSDBA |  |
| 3 | SHOW CONNECTIONS | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 SYSINFO_0 |  |
| 4 | KILL CONNECTION | SYSDBA |  |
| 5 | SHOW QUERIES | YSDBA SYSSEC SYSAUDIT SYSINFO_1 SYSINFO_0 |  |
| 6 | KILL QUERY | SYSDBA |  |

#### 4.2.21 系统信息查看权限

| 序号 | 权限名称(privilege name) | 系统权限 | 默认拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | ~~USE INFORMATION SCHEMA~~ | N/A | 直接使用 grant use on database information_schema to u1; |
| 2 | ~~USE PERFORMANCE SCHEMA~~ | N/A | 直接使用 grant use on database performance_schema to u1; |
| 3 | READ INFORMATION SCHEMA BASIC | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 | 只能看到受限系统表，ins_databases、ins_functions ins_indexes、ins_stables、ins_tables、ins_tags、ins_columns、ins_configs、ins_topics、ins_subscriptions、ins_streams、ins_stream_tasks、ins_views、ins_compacts、ins_compact_details、ins_tsmas，部分表只能看到可见字段，参照 [TD-18525](https://jira.taosdata.com:18080/browse/TD-18525) |
| 4 | READ INFORMATION SCHEMA SECURITY | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 | 安全相关的表, ins_users, ins_user_privileges, ins_tokens, ins_roles, ins_role_prvileges |
| 5 | READ INFORMATION SCHEMA AUDIT | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 | 审计相关的系统表: 暂无 |
| 6 | READ INFORMATION SCHEMA PRIVILEGED | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 SYSINFO_0 | 非 security/audit，并且不受 sysInfo 限制的表/字段。 |
| 7 | READ PERFORMANCE SCHEMA BASIC | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 | 只能看到受限系统表，perf_connections, perf_queries, perf_consumers, perf_trans, perf_apps |
| 8 | READ PERFORMANCE SCHEMA PRIVILEGED | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 SYSINFO_0 |  |
| 9 | SHOW GRANTS | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 | 授权相关的系统表，涉及命令： - show grants: 未限制，因为 taos shell 登录时使用. - show grants full/show grants logs/show cluster machines 进行了限制 |
| 10 | SHOW CLUSTER | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 |  |
| 11 | SHOW APPS | SYSDBA SYSSEC SYSAUDIT SYSINFO_1 SYSINFO_0 |  |

#### 4.2.22 审计管理权限

- 审计库由 SYSDBA ~~或拥有 CREATE AUDIT DATABASE 权限的用户~~创建。
- 数据库增加 AUDIT 参数指定是否为审计库：0 非审计库(未指定时默认值) ， 1 审计库。
- TDengine 中，限制只能有一个审计库。与老版本不同的是，audit 名字不再固定，而是通过库的属性 is_audit 标识，老版本中原有判断 audit 库的逻辑要更新。
```cpp {wrap}
create encrypt_key "xxxxxxxxxxxxxx";
create database if not exists d0 is_audit 1 encrypt_algorithm 'SM4-CBC'; -- 指定数据库为审计库
alter database d0 audit 1;                -- 修改数据库为审计库(为了兼容老版本审计库)，暂不支持，因为无关修改为加密库。
```

- TDengine 中，预置 `审计日志记录` SYSAUDIT_LOG 角色，可以在 audit 库中建表、写入数据，但不能删表/修改表/删除数据。SYSAUDIT_LOG 角色不能与 SYSDBA/SYSSEC/SYSAUDIT 角色同时授予某一个人。
- 在技术实现上，新增 AUDIT DATABASE 的 DROP/ALTER/USE 权限，TABLE 级别的权限与普通库相同。

| 序号 | 权限名称(privilege name) | 系统权限 | 拥有者 | grant/revoke 语句 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | ~~CREATE AUDIT DATABASE~~ 1）创建审计库没有安全风险。为便于运维，不单独设置 CREATE AUDIT DATABASE 权限，拥有建库权限的用户均可以创建审计库。 2）但即使是创建者，也不能 USE AUDIT DB，不能 select/insert 数据。 | N/A | N/A |
| 2 | DROP AUDIT DATABASE | 是 | 仅 SYSAUDIT，不可授予其他人 |
| 3 | ALTER AUDIT DATABASE | 是 | 仅 SYSAUDIT，不可授予其他人 |
| 4 | USE AUDIT DATABASE | 是 | 仅 SYSAUDIT/SYSAUDIT_LOG，不可授予其他人 |
| 5 | CREATE AUDIT TABLE |  | SYSAUDIT_LOG，不可授予其他人 |  |
| 6 | DROP TABLE(4.2.2 表权限) |  | N/A | 任何人不允许删除审计表 |
| 7 | ALTER TABLE(4.2.2 表权限) |  | N/A | 任何人不允许修改审计表 |
| 8 | SHOW TABLES(4.2.2 表权限) |  | SYSAUDIT SYSAUDIT_LOG |  |
| 9 | SHOW CREATE TABLE(4.2.2 表权限) |  | SYSAUDIT SYSAUDIT_LOG |  |
| 10 | SELECT AUDIT TABLE |  | SYSAUDIT，不可授予其他人 | 只有 SYSAUDIT 拥有该权限。 |
| 11 | INSERT AUDIT TABLE |  | SYSAUDIT_LOG，不可授予其他人 | 1) 只有拥有 SYSAUDIT_LOG 角色的用户可以向审计库中的表写入数据。 2) 如果要判断是否为审计库，常规做法是判断 SDbCfgInfo 中 isAudit 是否为 1 2.1） 目前 buildInsertCatalogReq 中没有，可以增加。 缺点：多了缓存请求，有可能影响写入性能。 优点：逻辑直接，最简单。 以下为降低性能影响的办法： 2.2） 首先判断 audit =开关； 2.3） 或者在 catalog 中，一直缓存 catalog dbFName，写入时每次返回，在写入时直接判断。 缺点：需要专门针对 audit 库专门增加一套缓存更新逻辑。 2.4）或者在 STableMeta 中增加 isAudit 标记，标记表是否为 audit 库的表。 缺点：在 stable/normal table 中，增加了一个 bit 的标志位。 优点：直接包含在取表的逻辑中，使用简单。 2.5) STableMeta 中，增加标识，但是不存储。在返回 meta 时，通过 db 或者 vnode 中的 is_audit 标识，给 STableMeta 设置标记位。 QA： 是否可以不考虑内存数据结构的兼容性。 union { uint8_t flags; struct { uint8_t virtualStb:1; uint8_t isAudit:1; uint8_t reserve:6; }; }; |
| 12 | *UPDATE TABLE(4.2.2 表权限)* |  | N/A |  | TDengine 暂不支持 update 语句，但是相同时间戳的会更新(如果有必要，可在写入时处理，禁止审计库更新，不便于在写入前进行检查)。 |
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
2. 角色名的长度为 1-63 个字符；
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
    | CREATE USER | DROP USER | ALTER USER| SET USER BASIC INFORMATION 
    | SET USER SECURITY INFORMATION | SET USER AUDIT INFORMATION| UNLOCK USER | LOCK USER | SHOW USERS
    | SHOW USERS SECURITY INFORMATION

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

##### 4.4.1.2 对象权限

```sql {wrap}
-- 授予对象权限
GRANT privileges ON [priv_obj] priv_level [WITH condition] TO {user_name | role_name}

-- 撤销对象权限
REVOKE privileges ON [priv_obj] priv_level [WITH condition] FROM {user_name | role_name}

-- 权限作用对象（不指定默认为表）
priv_obj: {
    database            -- 数据库
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

    ALTER | DROP | USE | FLUSH 
    | COMPACT | TRIM | ROLLUP | SCAN
    | SSMIGRATE | SHOW 
    | CREATE TABLE | CREATE VIEW | CREATE TOPIC | CREATE STREAM

    #### 表权限(table)

    DROP | ALTER | SHOW CREATE | SHOW| SELECT | INSERT | DELETE| CREATE INDEX | CREATE TSMA | CREATE RSMA
    
    #### 列权限(table)

    SELECT (column_list) | INSERT (column_list) 

    #### 视图权限(view)

    DROP | ALTER | SHOW | SELECT

    #### 索引权限(index)

    DROP | SHOW | SHOW CREATE

    #### 窗口预聚集权限(tsma)

    DROP | SHOW | SHOW CREATE

    #### 降采样存储权限(rsma)

    DROP | ALTER | SHOW | SHOW CREATE

    #### 主题权限(topic)

    DROP | SHOW | SHOW CREATE | SUBSCRIBE
    | SHOW CONSUMERS | SHOW SUBSCRIPTIONS

    #### 流计算权限(stream)

    DROP | SHOW | SHOW CREATE| START | STOP | RECALCULATE
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

#### 4.6.1 [SYSAUDIT_LOG](https://taosdata.feishu.cn/wiki/SxFfwi3p0iTPZxkO5pRco9BmnKd#Ba70duG7Bo3AGSx3tlTcCAPXn3d) 角色

用于在 audit 库中建表、写入数据，但不能删表/修改表/删除数据。该角色不能与 SYSDBA/SYSSEC/SYSAUDIT 角色同时授予某一个人。

#### 4.6.2 SYSINFO_0 角色

SYSINFO=0 的权限分类在 3.3.8 版本之前一直具备，为保持兼容性，改造为角色。部分权限如下，参见“[权限管理旧版 FS 文档](https://taosdata.feishu.cn/wiki/CFyuwFGCKimeXpkOHT2cTSAGn4M)”。
1. 不设置 ALTER SELF PASS 的权限
2. READ INFORMATION_SCHEMA BASIC
3. READ PERFORMANCE_SCHEMA BASIC
4. SHOW DATABASES 等权限
5. 可以将该角色直接授予用户。

#### 4.6.3 SYSINFO_1 角色

SYSINFO=1 的权限分类在 3.3.8 版本之前一直具备，为保持兼容性，改造为角色。部分权限如下，参见 [权限管理旧版 FS 文档](https://taosdata.feishu.cn/wiki/CFyuwFGCKimeXpkOHT2cTSAGn4M)。
1. ALTER SELF PASS
2. READ INFORMATION_SCHEMA PRIVILEGED
3. READ PERFORMANCE_SCHEMA PRIVILEGED
4. SHOW USERS、SHOW CLUSTER 等权限
5. 可以将该角色直接授予用户。

#### 4.6.4 SYSDBO 角色

数据库所有者权限在 3.3.8 版本之前一直具备，为保持兼容性，改造为角色。部分权限如下，参见“[权限管理旧版 FS 文档](https://taosdata.feishu.cn/wiki/CFyuwFGCKimeXpkOHT2cTSAGn4M)”。
1. 不设置 DB 外部的相关权限
2. DB 内部的权限，按和老版本的兼容性给与默认值(DB Owner）
3. 对自己拥有的数据库内部的、表、视图、索引、流等，具有所有的对象权限并可以授出与回收
4. 在用户创建完数据库之后，会在刚创建的数据库上，被赋予 SYSDBO  角色对应的权限。
5. 不能将该角色直接授予用户，因为其作用对象是具体的数据库。

#### 4.6.5 SYSDBR 角色

参照老版本中的 DB READ 权限。
1. 在升级时，将老版本 DB READ 对应的权限，自动赋予用户。
2. 不能将该角色直接授予用户，因为其作用对象是具体的数据库。

#### 4.6.6 SYSDBW 角色

参照老版本中的 DB WRITE 权限。
1. 在升级时，将老版本 DB WRITE 对应的权限，自动赋予用户。
2. 不能将该角色直接授予用户，因为其作用对象是具体的数据库。

### 4.7 兼容 3.3 版本语法

- 需求： 。
<callout emoji="bulb" background-color="light-orange" border-color="light-orange">
1）兼容 3.3 版本的语法，是指在语法层面上将 3.3 版本的语法由客户端语法解析器自动转换为 3.4 版本对应的权限，而不是退回到 3.3 版本的权限。
2）该问题的目标是尽可能的兼容旧语法，以提升老用户升级后的使用体验，新用户仍推荐使用 3.4 版本的新语法。
</callout>

#### 4.7.1 基本逻辑

1. 新增参数： enableGrantLegacySyntax
```plaintext
enableGrantLegacySyntax：
设置为 1 时，兼容 3.3 版本的权限授予与撤回语法，默认值为 1。
设置为 0 时，不兼容 3.3 版本的权限授予与撤回语法。
```

1. enableGrantLegacySyntax 为 1 时，grant all/read/write on <object>.* 等语法的行为与 3.3 保持一致。
      enableGrantLegacySyntax 为 0 时， grant all/read/write on <object>.* 等语法的行为在支持 3.4.0.0 的标准语法基础上，也尽量保持与 3.3 一致，以减少差异，提升用户体验。
1. subscribe/view 兼容旧语法，无论 enableGrantLegacySyntax 是否开启。 
2. 云服务版，为保持兼容性，新创建用户默认授予角色 SYSINFO_0，非云服务版本，新创建用户默认授予角色 SYSINFO_1。该行为，不受 enableGrantLegacySyntax 控制。

#### 4.7.2 语法对照表

- 规则总结及语法对照表
```sql {wrap}
1）权限作用对象 
1.1) 不指定对象类型时，db 与 db.* 等价。enableGrantLegacySyntax 为 1 扩展至所有对象(db/table/view/index/rsma/tsma/topic/stream)，enableGrantLegacySyntax 为 0 时仅扩展 table/view 对象（暂不支持 index,rsma,tsma 等依附于表的对象）； enableGrantLegacySyntax 为 1 扩展至所有对象，唯一的目标就是兼容 3.3 版本的权限，提供老用户升级后的使用体验。
1.2）不指定对象类型时，无论 enableGrantLegacySyntax 取 0 还是 1，当二级对象不为通配符时，例如，db.objName，在 grant 时，按照 table/view/topic 的顺序依次查找，只要查找到一个，则停止查找，如果均未查到，则报错；在 revoke 时，如果查找到对象，则只收回对应的对象的权限，如果任何对象均未找到，则收回 table/view/topic 对应对象名称的所有权限。
2）权限类型
2.1）all/read/write，基于 1) 中的对象类型扩展为具体的权限；read/write 也支持基于显式指定对象类型时扩展为具体权限。
2.2）alter/drop/show/show_create 作为通用权限也支持对象扩展；
2.3）subscribe 仅作用于 topic，所以也不报错。
2.4）其他权限暂不支持，报错。
```


| 对象 | 3.3.8 | 3.4.0 | 3.4.1 enableGrantLegacySyntax=1 | 3.4.1 enableGrantLegacySyntax=0 | 备注 |
| --- | --- | --- | --- | --- | --- |
| DB 权限 grant all on db to u1; grant read on db to u1; grant write on db to u1; | 报错 语法不支持 | - 转换为 DB 及 DB 下所有对象的权限 grant all on database db to u1; grant all on table db.* to u1; grant all on view db.* to u1; grant all on topic db.* to u1; grant all on stream db.* to u1; grant all on rsma db.* to u1; grant all on tsma db.* to u1; - read/write 转换为对应的权限 | 同左 但仅限于 table/view 对象，不包含其他对象。 | - grant <priv> on <db> to <user> 语法，是为了兼容 3.3 语法，因此，<priv> 支持 all/read/write/alter 权限，其他类型的权限报错。 - 非标准语法，不建议使用。 |
| DB 权限 grant all on db.* to u1; | 表权限 db 下所有表的所有权限 | 同上 | 同左 但仅限于 table/view 对象，不包含其他对象。 |  |
| DB 权限 grant read on db.* to u1; grant write on db.* to u1; | 报错 read/write 不支持 | 同上 | 同左 但仅限于 table/view 对象，不包含其他对象。 |  |
| DB 权限 grant alter on db to u1; 不报错，但是没任何作用 | 语法不支持 | 转换为 DB 及 DB 下所有对象的 alter 权限 | 同左 | 非标准语法，不建议使用。 |
| DB 权限 grant alter on db.* to u1; 不报错，但是没任何作用 | 表权限 db 下所有表的alter权限 | 同上 | 同左 但仅限于 table/view 对象，不包含其他对象。 |  |
| 不支持，报错： grant <priv> on db to u1; priv 取 all/read/write/alter 之外的其他值，例如，drop。 | 报错 对象类型不支持 | 为了语义的一致性，支持 drop/show/show_create/subscribe，其他类型报错 | 同左 但仅限于 table/view 对象。 | 非标准语法，不建议使用。 |
| 不支持，报错 | 表权限 grant <priv> on db.* to u1; priv 取 all/read/write/alter 之外的其他值，例如，drop | 扩展为table/view权限 | 同左 |  |
| 表权限 grant all on db.stb0 to u1; grant alter on db.stb0 to u1; | 同左 | 按照 table/view/topic 的顺序自动匹配 | 同左 |  |
| 表权限 grant read on db.stb0 to u1; grant write on db.stb0 to u1; | 报错 read/write 不支持 | 按照 table/view/topic 的顺序自动匹配 | 同左 |  |
| 视图权限 grant all on db.view0 to u1; grant alter on db.view0 to u1; | 报错，需指定 view grant all/alter on view db.view0 to u1; | 按照 table/view/topic 的顺序自动匹配 | 同左 |  |
| 视图权限 grant read on db.view0 to u1; grant write on db.view0 to u1; | 报错 read/write 不支持 | 按照 table/view/topic 的顺序自动匹配 | 同左 |  |
| topic | 主题权限 grant subscribe on topic1 to u1; | 报错，需要指定 topic 和 db grant subscribe on topic db.topic1 to u1; | 转换 grant subscribe on topic db.topic1 to u1; | 同左 |  |

## 5. 性能

- 访问过程增加了校验，耗时会增加。根据不同的操作类型，增加幅度不应该超过 (20%-100%]。
- 通过最佳实践，使大多数权限检查操作在最短路径结束，以减少耗时。

## 6. 兼容性

- 支持从低版本停机后，自动升级至 3.4.0.0 及以上的版本；不支持滚动升级。升级后，无法再降级。
- 升级至 3.4.0.0+ 版本后，在老版本设置的用户权限，会由数据库自动转换为 3.4.0.0+ 版本的用户权限，理论上，不需要人工干预，升级后可通过 `show users` 查看用户角色，通过  `show user privileges` 查看用户权限，通过 `show role privileges `查看角色权限。因为 3.4.0.0+ 版本的权限种类较细，不能 100% 保证升级后没有任何权限问题。如果遇到了，请联系运维或研发人员处理。
- 3.4.0.0-3.4.0.X 版本的权限整体改动较大，对语法进行了统一，部分语法不再兼容，新的语法请参考 [官网用户手册](https://docs.taosdata.com/next/reference/taos-sql/grant/)。 
- 自 3.4.0.Y 版本起，[兼容 3.3 版本语法](https://taosdata.feishu.cn/wiki/SxFfwi3p0iTPZxkO5pRco9BmnKd#M9J1dAhWhoiT74xFJ63cfPS7nqd)。

## 7. 运维

### 7.1 root 用户管理

1. TDengine 3.4.x.y 及后续版本遵循 RBAC 原则，在初始配置 DB 时，针对 root 用户的操作建议参考：[root 用户管理](https://taosdata.feishu.cn/docx/KcYEd5xNKoQE2oxHpVfcOWpNnlf#share-OHRudg8vuoufgExlhiscmTfNncf)。  

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
