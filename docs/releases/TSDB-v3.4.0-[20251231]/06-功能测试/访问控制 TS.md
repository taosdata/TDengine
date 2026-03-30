# 访问控制 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-22 | 2026-01-25 | 1.0 | 徐开礼 | 初稿 |

## 2. 测试目标

本测试规范用于验证 TDengine [访问控制](https://jira.taosdata.com:18080/browse/TS-7232) 功能正确性、完整性、安全性与兼容性，确保产品行为与设计目标一致。主要测试目标包括：
- 角色管理
- 权限授予与撤回
- 权限控制：`数据库权限、表权限、行权限、列权限、自定义函数权限、索引权限、SMA 权限、视图权限、挂载权限、角色权限、用户权限、令牌权限、密码权限、节点管理权限、系统参数调整权限、订阅权限、流计算权限、系统管理权限、系统信息查看权限、审计管理权限`
- 资源清理：删除`库/超级表/视图/主题`时，自动清理用户权限。
- 版本兼容：老版本(3.3.x.y 及以下)升级、未来版本(3.4.x.y 及以上)升级。3.4.x.y 不支持降级至 3.3.x.y。

## 3. 参考文档

[访问控制 RS](https://taosdata.feishu.cn/wiki/Y12Ywd797ieHBBkVZsqcpsRgnAg)
[访问控制 FS](https://taosdata.feishu.cn/wiki/SxFfwi3p0iTPZxkO5pRco9BmnKd)

## 4. 测试结论

1. 核心功能符合预期。
2. 未开启子表/行/列权限条件下，对查询性能影响不大；写入因为增加了验证流程，会有小幅下降(< 10%)。

## 5. 测试环境

-  OS： Linux

## 6. 功能测试

- 本文档中，root 指超级用户，u1/u2/u3 等指普通用户。

### 6.1 角色管理功能

#### 6.1.1 测试要点

- 创建/删除角色，为用户添加/删除角色。
- 角色命名规范，系统角色冲突检测。
- 查看内置系统角色及其权限。

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 查看内置角色 | 1）系统内置 SYSDBA/SYSSEC/SYSAUDIT/SYSAUDIT_LOG/SYSINFO_1/SYSINFO_0 6 个角色，包括 3.3.x.y 升级至 3.4.x.y 的场景； | 通过 |
| 2 | 查看内置角色权限 | 1）系统内置角色默认拥有的权限，与产品设计一致。 | 通过 |
| 3 | 创建角色 | 1）命名符合角色命名规范可正常创建(例如，最大长度为 63 个字符，不能以 sys 为前缀)，命名不符合规范给出错误提示； 2）角色名称不能与已有角色和用户同名； 3）系统中角色数量上限是 200 个(包含系统内置角色)。 注：系统中用户数量上限是 2000 个(包含系统内置用户)。 | 通过 |
| 4 | 删除角色 | 1）系统内置角色不可删除； 2）普通角色可以删除，同时删除用户的该角色； | 通过 |
| 5 | 为用户添加角色 | 1）可以为普通用户添加任何一个独立的角色； 2）SYSDBA/SYSSEC/SYSAUDIT/SYSAUDIT_LOG 的任何两个不能同时授予同一个普通用户(root 用户内置 SYSDBA/SYSSEC/SYSAUDIT，且不可删除)； 3）用户拥有的角色上限是 32 个； 4）不支持为 root 用户添加内置系统角色； 5）不支持为角色添加角色； | 通过 |
| 6 | 为用户删除角色 | 1）普通用户的角色可以删除； 2）root 用户的 `内置系统角色`不可以删除； 3）删除角色时，要保证系统中至少有一个有效用户(enable 状态，未锁定)拥有 SYSDBA 角色。 | 通过 |

### 6.2 权限控制

#### 6.2.1 测试要点

- `数据库权限、表权限、行权限、列权限、自定义函数权限、索引权限、SMA 权限、视图权限、挂载权限、角色权限、用户权限、令牌权限、密码权限、节点管理权限、系统参数权限、订阅权限、流计算权限、系统管理权限、系统信息权限、审计管理权限` 的权限，均与产品设计行为一致。

#### 6.2.2 用例列表

##### 6.2.2.1 数据库权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | CREATE DATABASE | 1）拥有 create database 权限的用户可以创建数据库，否则不可以创建； 2）alter user u1 createdb 1 与 grant create database to u1 等价; 3）alter user u1 createdb 0 与 revoke create database from u1 等价; | 通过 |
| 2 | ALTER DATABASE | 1）拥有 alter database 权限的用户和 db owner 可以修改普通数据库的属性； | 通过 |
| 3 | DROP DATABASE | 1）拥有 drop database 权限的用户和 db owner 可以删除普通数据； 2）删除审计数据库，需要 DROP AUDIT DB 权限。 | 通过 |
| 4 | USE DATABASE | 1）拥有 use database 权限的用户和 db owner 可以 use 普通数据库； | 通过 |
| 5 | FLUSH DATABASE | 1）拥有 flush database 权限的用户和 db owner 可以执行该操作； | 通过 |
| 6 | COMPACT DATABASE | 1）拥有 compact database 权限的用户和 db owner 可以执行该操作； | 通过 |
| 7 | TRIM DATABASE | 1）拥有 trim database 权限的用户和 db owner 可以执行该操作； | 通过 |
| 8 | ROLLUP DATABASE | 1）拥有 rollup database 权限的用户和 db owner 可以执行该操作； | 通过 |
| 9 | SCAN DATABASE | 1）拥有 scan database 权限的用户和 db owner 可以执行该操作； | 通过 |
| 10 | SSMIGRATE DATABASE | 1）拥有 ssmigrate database 权限的用户和 db owner 可以执行该操作； | 通过 |
| 11 | BALANCE VGROUP | 1）拥有 balance vgroup 权限的用户 可以执行该操作； | 通过 |
| 12 | BALANCE VGROUP LEADER | 1）拥有 balance vgroup leader 权限的用户可以执行该操作； | 通过 |
| 13 | MERGE VGROUP | 1）拥有 merge vgroup 权限的用户可以执行该操作； | 通过 |
| 14 | REDISTRIBUTE VGROUP | 1）拥有 redistribute vgroup 权限的用户可以执行该操作； | 通过 |
| 15 | SPLIT VGROUP | 1）拥有 split vgroup 权限的用户可以执行该操作； | 通过 |
| 16 | SHOW DATABASES | 1）拥有 `所有 DB`或 `该 DB` 的 show databases 权限以及 db owner，返回；否则不返回。 | 通过 |
| 17 | SHOW VNODES | 1）拥有 `所有 DB`或 `该 DB` 的 show vnodes 权限以及 db owner，返回；否则不返回。 | 通过 |
| 18 | SHOW VGROUPS | 1）拥有 `所有 DB`或 `该 DB` 的 show vgroups 权限以及 db owner，返回；否则不返回。 | 通过 |
| 19 | SHOW COMPACTS | 1）拥有 `所有 DB`或 `该 DB` 的 show compacts 权限以及 db owner，返回；否则不返回。 | 通过 |
| 20 | SHOW RETENTIONS | 1）拥有 `所有 DB`或 `该 DB` 的 show retentions 权限以及 db owner，返回；否则不返回。 | 通过 |
| 21 | SHOW SCANS | 1）拥有 `所有 DB`或 `该 DB` 的 show scans 权限以及 db owner，返回；否则不返回。 | 通过 |
| 22 | SHOW SSMIGRATES | 1）拥有 `所有 DB`或 `该 DB` 的 show ssmigrates 权限以及 db owner，返回；否则不返回。 | 通过 |

##### 6.2.2.2 表权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | CREATE TABLE | 1) 拥有`某个或所有数据库的建表权限` 的用户 或者 `db owner` ，可以创建超级表/子表/普通表/虚拟表；否则，创建失败； | 通过 |
| 2 | DROP TABLE | 1）拥有 `某个或所有表的 drop 权限` 或者 table owner，可以 drop table。 | 通过 |
| 3 | ALTER TABLE | 1）拥有 `某个或所有表的 alter 权限` 或者 table owner，可以 alter table。 | 通过 |
| 4 | SHOW TABLES | 1) 拥有 `某个或所有表的 show tables 权限` 或者 table owner，可以在 show stables 显示。 注：show tables：暂未进行表权限控制。 | 通过 |
| 5 | SHOW CREATE TABLE | 1) 拥有 `某个或所有表的 show show table 权限` 或者 table owner，可以正常执行；否则报错。 | 通过 |
| 6 | SELECT TABLE | 1）拥有 `某个或所有表的 select 权限` 或者 table owner，可以正常执行；否则报错。 | 通过 |
| 7 | INSERT TABLE | 1）拥有 `某个或所有表的 insert 权限` 或者 table owner，可以正常执行；否则报错。 | 通过 |
| 9 | DELETE TABLE | 1）拥有 `某个或所有表的 delete 权限` 或者 table owner，可以正常执行；否则报错。 | 通过 |

##### 6.2.2.3 行权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 查询行权限 | 1）查询范围拥有行权限，可正常返回数据；否则，不返回数据。 | 通过 |
| 2 | 写入行权限 | 1）写入范围拥有行权限，可正常写入；否则，写入报错。 | 通过 |
| 3 | 删除行权限 | 1）删除范围拥有行权限，可正常删除；否则，不会删除，不报错。 | 通过 |

##### 6.2.2.4 列权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 查询列权限 | 1）查询列拥有列权限，可正常返回数据；否则，查询报错； | 通过 |
| 2 | 写入列权限 | 1）写入列拥有列权限，可正常写入；否则，写入报错； | 通过 |

##### 6.2.2.5 自定义函数权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | CREATE FUNCTION | 1）拥有 create function 权限，可以正常执行；否则报错。 | 通过 |
| 2 | DROP FUNCTION | 1）拥有 drop function 权限，可以正常执行；否则报错。 | 通过 |
| 3 | SHOW FUNCTIONS | 1）拥有 show functions 权限，可以正常执行；否则报错。 | 通过 |

##### 6.2.2.6 索引权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | CREATE INDEX | 1）拥有 create index 权限，同时拥有表的 select，use database 权限，可以正常执行；否则报错。 | 通过 |
| 2 | DROP INDEX | 1）拥有 drop index 权限 或者 index owner，同时拥有 use database 权限，可以正常执行；否则报错。 | 通过 |
| 3 | SHOW INDEXES | 1）拥有 show indexes 权限 或者 index owner，可以正常展示，否则，不展示。 | 通过 |

##### 6.2.2.7 SMA 权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | CREATE RSMA | 1）拥有 create rsma 权限，同时拥有 use database + stable 的 select/insert 权限，可以正常执行；否则报错。 | 通过 |
| 2 | DROP RSMA | 1）拥有 drop rsma 权限 或者 rsma owner，同时拥有 use database 权限，可以正常执行；否则报错。 | 通过 |
| 3 | ALTER RSMA | 1）拥有 alter rsma 权限 或者 rsma owner，同时拥有 use database 权限，可以正常执行；否则报错。 | 通过 |
| 4 | SHOW RSMAS | 1）拥有 所有 rsma 或该 rsma 的 show 权限，或者 rsma owner，返回；否则不返回。 | 通过 |
| 5 | SHOW CREATE RSMA | 1）拥有 所有 rsma 或该 rsma 的 show create 权限，或者 rsma owner，正常执行；否则报错。 | 通过 |
| 6 | CREATE TSMA | 1）拥有 use database + stable(超级表) SELECT TABLE/CREATE STREAM/CREATE TSMA + CREATE TABLE on database 权限，可以正常执行；否则报错。 | 通过 |
| 7 | DROP TSMA | 1）拥有 use database，以及 drop tsma 权限，或者 tsma owner，可以正常执行；否则报错。 | 通过 |
| 8 | SHOW TSMAS | 1）拥有 所有 tsma 或 该 tsma 的 show 权限，或者 tsma owner，返回；否则不返回。 | 通过 |

##### 6.2.2.8 视图权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | CREATE VIEW | 1）拥有 create view + use database 权限，可以正常执行；否则报错。 | 通过 |
| 2 | DROP VIEW | 1）拥有 drop view 权限或者 view owner，同时拥有 use database 权限，可以正常执行；否则报错。 | 通过 |
| 3 | ALTER VIEW | 1）拥有 alter view 权限或者 view owner，同时拥有 use database 权限，可以正常执行；否则报错。 | 通过 |
| 4 | SHOW VIEWS | 1）拥有 show views 权限或者 view owner 返回；否则不返回。 | 通过 |
| 5 | SELECT VIEW | 1）拥有 select view 权限 或者 view owner 正常执行，否则报错。 | 通过 |

##### 6.2.2.9 挂载权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | CREATE MOUNT | 1）拥有 create mount 权限，可以正常执行，否则报错。 | 通过 |
| 2 | DROP MOUNT | 1）拥有 drop mount 权限，可以正常执行，否则报错。 | 通过 |
| 3 | SHOW MOUNTS | 1）拥有 show mounts 权限，可以正常执行，否则报错。 | 通过 |

##### 6.2.2.10 角色权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | CREATE ROLE | 1）拥有 create role 权限，可以正常执行，否则报错。 | 通过 |
| 2 | DROP ROLE | 1）拥有 drop role 权限，可以正常执行，否则报错。 | 通过 |
| 3 | SHOW ROLES | 1）拥有 show roles 权限，可以正常执行，否则报错。 | 通过 |
| 4 | UNLOCK ROLE | 1）拥有 unlock role 权限，可以正常执行，否则报错。 | 通过 |
| 5 | LOCK ROLE | 1）拥有 lock role 权限，可以正常执行，否则报错。 | 通过 |

##### 6.2.2.11 用户权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | CREATE USER | 1）拥有 create user 权限，可以正常执行，否则报错。 | 通过 |
| 2 | DROP USER | 1）拥有 drop user 权限，可以正常执行，否则报错。 | 通过 |
| 3 | ALTER USER | 1）拥有 alter user 权限，可以正常执行，否则报错。 | 通过 |
| 4 | SET USER SECURITY INFORMATION | 1）拥有 alter user + 该权限，才可以修改用户安全信息时，否则报错。 | 通过 |
| 5 | SET USER AUDIT INFORMATION | 1）拥有 alter user + 该权限，才可以修改用户审计信息时，否则报错。 | N/A 用户暂不包含审计信息。 |
| 6 | SET USER BASIC INFORMATION | 1）拥有 alter user + 该权限，才可以修改用户基本信息时，否则报错。 | 通过 |
| 7 | UNLOCK USER | 1）拥有 unlock user 权限，可以正常执行，否则报错。 | 通过 |
| 8 | LOCK USER | 1）拥有 lock user 权限，可以正常执行，否则报错。 2）确保系统中至少有一个有效的 SYSDBA，可以是 root，也可以是普通用户，否则报错。 | 通过 |
| 9 | SHOW USERS | 1）拥有 show users 权限，可以正常执行，否则报错。 | 通过 |

##### 6.2.2.12 令牌权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | CREATE TOKEN | 1）拥有 create token 权限或者用户自己，可以正常执行，否则报错。 | 通过 |
| 2 | DROP TOKEN | 1）拥有 drop token 权限或者用户自己，可以正常执行，否则报错。 | 通过 |
| 3 | ALTER TOKEN | 1）拥有 alter token 权限或者用户自己，可以正常执行，否则报错。 | 通过 |
| 4 | SHOW TOKENS | 1）拥有 show tokens 权限 或者用户自己，可以正常返回，否则不返回，但是不报错。 | 通过 |

##### 6.2.2.13 密钥权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | CREATE TOTP | 1）拥有 create totp 权限或者用户自己，可以正常执行，否则报错。 | 通过 |
| 2 | DROP TOTP | 1）拥有 drop totp 权限或者用户自己，可以正常执行，否则报错。 | 通过 |

##### 6.2.2.14 密码权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | ALTER PASS | 1）拥有 alter pass 权限，可以修改其他用户的权限，否则报错。 | 通过 |
| 2 | ALTER SELF PASS | 1）拥有 alter self pass 权限，可以修改自己的权限，否则报错。 | 通过 |

##### 6.2.2.15 权限授予回收的权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | GRANT PRIVILEGE | 1）拥有 grant privilege 权限才可以执行 grant 操作，否则报错。 | 通过 |
| 2 | REVOKE PRIVILEGE | 1）拥有 revoke privilege 权限才可以执行 revoke 操作，否则报错。 | 通过 |
| 3 | SHOW PRIVILEGES | 1）拥有 show privileges 权限才可以正常执行 show user/role privileges 操作，否则报错。 | 通过 |
| 4 | GRANT SYSDBA PRIVILEGE | SYSDBA 拥有，不支持授予或撤回 | 通过 |
| 5 | REVOKE SYSDBA PRIVILEGE | SYSDBA 拥有，不支持授予或撤回 | 通过 |
| 6 | GRANT SYSSEC PRIVILEGE | SYSSEC 拥有，不支持授予或撤回 | 通过 |
| 7 | REVOKE SYSSEC PRIVILEGE | SYSSEC 拥有，不支持授予或撤回 | 通过 |
| 8 | GRANT SYSAUDIT PRIVILEGE | SYSAUDIT 拥有，不支持授予或撤回 | 通过 |
| 9 | REVOKE SYSAUDIT PRIVILEGE | SYSAUDIT 拥有，不支持授予或撤回 | 通过 |

##### 6.2.2.16 节点管理权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | CREATE NODE | 1）拥有该权限，可以执行下述操作，否则报错： CREATE DNODE/MNODE/QNODE/SNODE/BNODE | 通过 |
| 2 | DROP NODE | 1）拥有该权限，可以执行下述操作，否则报错： DROP DNODE/MNODE/QNODE/SNODE/BNODE | 通过 |
| 3 | SHOW NODES | 1）拥有该权限，可以执行下述操作，否则报错： SHOW DNODES/MNODES/QNODES/SNODES/BNODES | 通过 |

##### 6.2.2.17 系统参数权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | ALTER SECURITY VARIABLE | 1）拥有该权限，可以修改安全参数，否则报错。 | 通过 |
| 2 | ALTER AUDIT VARIABLE | 1）拥有该权限，可以修改审计参数，否则报错。 | 通过 |
| 3 | ALTER SYSTEM VARIABLE | 1）拥有该权限，可以修改普通系统参数，否则报错。 | 通过 |
| 4 | ALTER DEBUG VARIABLE | 1）拥有该权限，可以修改调试参数，否则报错。 | 通过 |
| 5 | SHOW SECURITY VARIABLES | 1）sysinfo 为 1，可以正常返回。 注：展示暂沿用原逻辑。 | 通过 |
| 6 | SHOW AUDIT VARIABLES | 1）sysinfo 为 1，可以正常返回。 注：展示暂沿用原逻辑。 | 通过 |
| 7 | SHOW SYSTEM VARIABLES | 1）sysinfo 为 1，可以正常返回。 注：展示暂沿用原逻辑。 | 通过 |
| 8 | SHOW DEBUG VARIABLES | 1）sysinfo 为 1，可以正常返回。 注：展示暂沿用原逻辑。 | 通过 |

##### 6.2.2.18 订阅权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | CREATE TOPIC | 1）拥有 create topic 权限，可正常执行；否则报错。 | 通过 |
| 2 | DROP TOPIC | 1）拥有 drop topic 权限 或者 topic owner，可正常执行；否则报错。 | 通过 |
| 3 | SHOW TOPICS | 1）拥有 show topics 权限 或者 topic owner，可正常返回；否则不返回。 | 通过 |
| 4 | SHOW CONSUMERS | 1）拥有 show consumers 权限 或者 topic owner 或 subscribe owner，可正常返回；否则不返回。 | 通过 |
| 5 | SHOW SUBSCRIPTIONS | 1）拥有 show subscriptions 权限 或者 topic owner 或者 subscribe owner(仅返回自己的订阅)，可正常返回；否则不返回。 | 通过 |
| 6 | SUBSCRIBE | 1）拥有该权限 或者 topic owner，可正常执行；否则报错。 | 通过 |

##### 6.2.2.19 流计算权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | CREATE STREAM | 1）拥有触发库 use database 权限，触发表的 select 权限；流执行库的 use database 权限 和 create stream 权限；流结果存储库的 use database 和 create table 权限，可正常执行；否则报错。 | 通过 |
| 2 | DROP STREAM | 1）拥有 drop stream 权限 或者 stream owner，同时拥有 use database 权限，可正常执行；否则报错。 | 通过 |
| 3 | SHOW STREAMS | 1）拥有 show streams 权限 或者 stream owner，可正常返回，否则不返回。 | 通过 |
| 4 | START STREAM | 1）拥有 start stream 权限 或者 stream owner，同时拥有 use database 权限，可正常执行；否则报错。 | 通过 |
| 5 | STOP STREAM | 1）拥有 stop stream 权限 或者 stream owner，同时拥有 use database 权限，可正常执行；否则报错。 | 通过 |
| 6 | RECALCULATE STREAM | 1）拥有 recalculate stream 权限 或者 stream owner，同时拥有 use database 权限，可正常执行；否则报错。 | 通过 |

##### 6.2.2.20 系统管理权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | SHOW TRANSACTIONS | 1）拥有 show transanctions 权限，可正常返回，否则报错。 | 通过 |
| 2 | KILL TRANSACTION | 1）拥有 kill transaction 权限，可正常执行，否则报错。 | 通过 |
| 3 | SHOW CONNECTIONS | 1）拥有 show connections 权限，可正常返回，否则报错。 | 通过 |
| 4 | KILL CONNECTION | 1）拥有 kill connection 权限，可正常执行，否则报错。 | 通过 |
| 5 | SHOW QUERIES | 1）拥有 show queries 权限，可正常返回，否则报错。 | 通过 |
| 6 | KILL QUERY | 1）拥有 kill query 权限，可正常执行，否则报错。 | 通过 |

##### 6.2.2.21 系统信息权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | USE INFORMATION_SCHEMA | 1）拥有 information_schema 或者所有 db 的 use 的权限，可正常执行；否则，报错。 | 通过 |
| 2 | USE PERFORMANCE_SCHEMA | 1）拥有 performance_schema 或者所有 db 的 use 的权限，可正常执行；否则，报错。 | 通过 |
| 3 | READ INFORMATION_SCHEMA BASIC | 1）拥有 information schema use 权限，且拥有该权限，show information_schema.tables，可以展示基础信息表，否则，不返回，但也不报错。 | 通过 |
| 4 | READ INFORMATION_SCHEMA SECURITY | 1）拥有 information schema use 权限，且拥有该权限，show information_schema.tables，可以展示安全信息表，否则，不返回，但也不报错。 | 通过 |
| 5 | READ INFORMATION_SCHEMA AUDIT | 1）拥有 information schema use 权限，且拥有该权限，show information_schema.tables，可以展示审计信息表，否则，不返回，但也不报错。 | 通过 |
| 6 | READ INFORMATION_SCHEMA PRIVILEGED | 1）拥有 information schema use 权限，且拥有该权限，show information_schema.tables，可以展示高阶信息表，否则，不返回，但也不报错。 | 通过 |
| 7 | READ PERFORMANCE_SCHEMA BASIC | 1）拥有 performance schema use 权限，且拥有该权限，show performance_schema.tables，可以展示基础信息表，否则，不返回，但也不报错。 | 通过 |
| 8 | READ PERFORMANCE_SCHEMA PRIVILEGED | 1）拥有 performance schema use 权限，且拥有该权限，show performance_schema.tables，可以展示高阶信息表，否则，不返回，但也不报错。 | 通过 |
| 9 | SHOW GRANTS | 拥有 show grants 权限，可正常执行，否则报错。 | 通过 |
| 10 | SHOW CLUSTER | 拥有 show cluster 权限，可正常执行，否则报错。 | 通过 |
| 11 | SHOW APPS | 拥有 show apps 权限，可正常执行，否则报错。 | 通过 |

##### 6.2.2.22 审计管理权限

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | DROP AUDIT DATABASE | 1）拥有 SYSAUDIT 角色，并且审计库的 allow_drop 属性为 1 时，才可以删除审计库；否则报错。 | 通过 |
| 2 | ALTER AUDIT DATABASE | 1）拥有 SYSAUDIT 角色，才可以修改审计库；否则报错。 | 通过 |
| 3 | USE AUDIT DATABASE | 1）拥有 use audit database 权限时，才可以 use 审计库；否则报错。2）use audit database 权限，默认授予的 SYSAUDIT 和 SYSAUDIT_LOG 角色。 3）use audit database 权限，不可以授予其他人或其他角色。 | 通过 |
| 4 | CREATE AUDIT TABLE | 1）只有拥有 SYSAUDIT_LOG 角色的用户，才可以在审计中创建表。 2）create audit table 权限，默认被授予的 SYSAUDIT_LOG 角色。 3）create audit table 权限，不可以被授予其他人或其他角色。 | 通过 |
| 5 | DROP TABLE | 1）审计库中的表，不可以被 drop。 | 通过 |
| 6 | ALTER TABLE | 1）审计库中的表，不可以被 alter。 | 通过 |
| 7 | SHOW TABLES | 1）拥有 show tables 权限，同时拥有 use audit database 权限，可以查看，否则报错。 | 通过 |
| 8 | SHOW CREATE TABLE | 1）拥有 show create table 权限，同时拥有 use audit database 权限，可以查看，否则报错。 | 通过 |
| 9 | SELECT AUDIT TABLE | 1）只有拥有 SYSAUDIT 角色的用户，才可以查询审计库中的表数据。 2）select audit table 权限，默认被授予的 SYSAUDIT 角色。 3）select audit table 权限，不可以被授予其他人或其他角色。 | 通过 |
| 10 | INSERT AUDIT TABLE | 1）只有拥有 SYSAUDIT_LOG 角色的用户，才可以向审计库中的表写入数据。 2）insert audit table 权限，默认被授予的 SYSAUDIT_LOG 角色。 3）insert audit table 权限，不可以被授予其他人或其他角色。 | 通过 |
| 12 | DELETE TABLE | 1）审计库中的表的数据，不可以被 delete。 | 通过 |

### 6.3 资源清理

#### 6.3.1 测试要点

- 数据库、超级表、视图、主题等删除时，自动清理用户对应的权限。
- 普通表和子表，因不经过 mnode 及数量较大，暂不支持自动清理，由用户手工清理。

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 删除 db | 1）用户和角色中，对应 db 的权限都被删除。 | 通过 |
| 2 | 删除 stb | 1）用户和角色中，对应 stb 的权限都被删除。 | 通过 |
| 3 | 删除 view | 1）用户和角色中，对应 view 的权限都被删除。 | 通过 |
| 4 | 删除 topic | 1）用户和角色中，对应 topic 的权限都被删除。 | 通过 |

### 6.4 版本升级

#### 6.4.1 测试要点

- 3.3.x.y 升级至 3.4.x.y 时，无兼容性问题，可自动将 3.3.x.y 的 库/表/视图/主题权限升级至 3.4.x.y。
- 3.4.x.y 版本升级至后续版本时，如果系统角色权限类别发生变化，可自动升级系统角色的权限。普通角色和用户的权限，需要人工处理。

#### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 老版本升级 | 3.3.x.y 升级至 3.4.x.y： 1）可正常启动，原有用户及其基本属性均自动升级； 2）系统内置角色添加成功，且对应的权限符合预期； 3）root 用户授予 SYSDBA/SYSSEC/SYSAUDIT 角色。 4）普通用户 sysinfo 属性为 1，授予 SYSINFO_1 角色。 5）普通用户 sysinfo 属性为 0，授予 SYSINFO_0 角色。 6）root 和 普通用户都添加了 uid 属性并赋值； 7）3.3.x.y 的用户的权限(db/table/view/topic)均升级至 3.4.x.y； 8）db 增加了 ownerId 属性，并被赋值为 createUser 用户新生成的 uid。 | 通过 |
| 2 | 新版本升级 | 3.4.x.y 升级至 3.4.x.y： 1）可正常启动； 2）如果系统角色拥有的权限发生变化，可以自动更新，不需要人工干预。 | 通过 |
| 3 | 兼容性 | 1）alter user u1 createdb 1 与 grant create database to u1 等价 2）alter user u1 createdb 0 与 revoke create database from u1 等价 | 通过 |

## 7. 易用性测试

- grant/revoke 语法，基于主流数据库产品的 SQL 语法进行扩展。常用的表权限控制 select/insert/insert 语法是一致的，针对一些特有的权限类别引入了新的关键词。

## 8. 长期稳定性测试（可选）

## 9. 性能测试

## 10. 安全测试

- 用户的行为均通过具体的权限控制，从根本上防止用户越权和数据非预期的访问。

## 11. 兼容性测试

- 3.3.x.y 版本可直接升级至 3.4.x.y 版本，但不支持滚动升级。
- 因权限语法和逻辑整体改动较大，升级至 3.4.x.y 版本后，原有的授权语法无法做到兼容，需使用新的语法。
- 为减少 3.3.x.y 升级至 3.4.x.y 后，因权限改动对用户的影响。在内核层面，已经尽可能的对老版本的权限进行自动升级，但是，不能 100% 保证用户升级后不做任何权限改动。

## 12. 已知问题和限制（可选）

这里用于记录产品使用上的一些限制，包括不支持的场景等，以及在发版时没有解决的minor issues.
- 主要使用场景的权限已使用新的权限进行控制，但因权限种类较多，部分权限仍沿用老版本的权限控制，后续，会根据实际需求完全迁移至新的权限控制逻辑。
