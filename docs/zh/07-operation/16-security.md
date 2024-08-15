---
sidebar_label: 更多安全策略
title: 更多安全策略
toc_max_heading_level: 4
---

除了传统的用户和权限管理之外，TDengine 还有其他的安全策略，例如 IP 白名单、审计日志、数据加密等，这些都是 TDengine Enterprise 特有功能，其中白名单功能在 3.2.0.0 版本首次发布，审计日志在 3.1.1.0 版本中首次发布，数据库加密在 3.3.0.0 中首次发布，建议使用最新版本。

## IP 白名单

IP 白名单是一种网络安全技术，它使 IT 管理员能够控制“谁”可以访问系统和资源，提升数据库的访问安全性，避免外部的恶意攻击。IP 白名单通过创建可信的 IP 地址列表，将它们作为唯一标识符分配给用户，并且只允许这些 IP 地址访问目标服务器。请注意，用户权限与 IP 白名单是不相关的，两者分开管理。下面是配置 IP 白名单的具体方法。

增加 IP 白名单的 SQL 如下。
```sql
create user test pass password [sysinfo value] [host host_name1[,host_name2]] 
alter user test add host host_name1
```

查询 IP 白名单的 SQL 如下。
```sql
SELECT TEST, ALLOWED_HOST FROM INS_USERS;
SHOW USERS;
```

删除 IP 白名单的命令如下。
```sql
ALTER USER TEST DROP HOST HOST_NAME1
```

## 审计日志

TDengine 先对用户操作进行记录和管理，然后将这些作为审计日志发送给taosKeeper，再由 taosKeeper 保存至任意 TDengine 集群。管理员可通过审计日志进行安全监控、历史追溯。TDengine 的审计日志功能开启和关闭操作非常简单，只须修改TDengine 的配置文件后重启服务。审计日志的配置说明如下。

### taosd 配置

审计日志由数据库服务 taosd 产生，其相应参数要配置在 taos.cfg 配置文件中，详细参数如下表。

| 参数名称       | 参数含义                                                  |
|:-------------:|:--------------------------------------------------------:|
|audit | 是否打开审计日志，默认值为 0。1 为开启，0 为关闭 |
|monitorFqdn | 接收审计日志的 taosKeeper 所在服务器的 FQDN | 
|monitorPort | 接收审计日志的 taosKeeper 服务所用端口 |
|monitorCompaction | 上报数据时是否进行压缩 |

### taosKeeper 配置

在 taosKeeper 的配置文件 keeper.toml 中配置与审计日志有关的配置参数，如下表所示

| 参数名称       | 参数含义                                                  |
|:-------------:|:--------------------------------------------------------:|
|auditDB | 用于存放审计日志的数据库的名字，默认值为 "audit" ，taosKeeper 在收到上报的审计日志后会判断该数据库是否存在，如果不存在会自动创建它 |

### 数据格式

上报的审计日志格式如下

```json
{
    "ts": timestamp,
    "cluster_id": string,
    "user": string,
    "operation": string,
    "db": string,
    "resource": string,
    "client_add": string,
    "details": string
}
```

### 表结构

taosKeeper 会依据上报的审计数据在相应的数据库中自动建立超级表用于存储数据。该超级表的定义如下

```sql
CREATE STABLE operations(ts timestamp, details VARCHAR(64000)， User VARCHAR(25), Operation VARCHAR(20), db VARCHAR(65), resource VARCHAR(193), client_add(25)) TAGS (clusterID VARCHAR(64) );
```

其中：
1. db为操作涉及的database，resource为操作涉及的资源。
2. User 和 Operation 为数据列，表示哪个用户在该对象上进行了什么操作
3. timestamp 为时间戳列，表示操作发生时的时间
4. details 为该操作的一些补充细节，在大多数操作下是所执行的操作的SQL语句。
5. client_add为客户端地址，包括ip和端口

### 操作列表

目前审计日志中所记录的操作列表以及每个操作中各字段的含义如下表（注：因为每个操作的实加者即 user 字段、时间戳字段和client_add在所有操作中的含义相同，下表不包含）

| 操作        | Operation | DB | Resource | Details |
| ----------------| ----------| ---------| ---------| --------|
| create database | createDB  | db name  | NULL     | SQL |
| alter database  | alterDB   | db name  | NULL     | SQL |
| drop database   | dropDB    | db name  | NULL     | SQL |
| create stable   | createStb | db name  | stable name | SQL |
| alter stable    | alterStb  | db name  | stable name | SQL |
| drop stable     | dropStb   | db name  | stable name | SQL |
| create user     | createUser | NULL |  被创建的用户名 | 用户属性参数,  (password除外) |
| alter user      | alterUser | NULL | 被修改的用户名 | 修改密码操作记录的是被修改的参数和新值 (password除外) ；其他操作记录SQL |
| drop user       | dropUser | NULL | 被删除的用户名 | SQL |
| create topic    | createTopic | topic 所在 DB | 创建的 topic 名字 | SQL |
| drop topic      | cropTopic | topic 所在 DB | 删除的 topic 名字 | SQL |
| create dnode    | createDnode | NULL | IP:Port 或 FQDN:Port | SQL |
| drop dnode      | dropDnode | NULL | dnodeId | SQL |
| alter dnode     | alterDnode | NULL | dnodeId | SQL |
| create mnode    | createMnode | NULL | dnodeId | SQL |
| drop mnode      | dropMnode | NULL | dnodeId | SQL |
| create qnode    | createQnode | NULL | dnodeId | SQL |
| drop qnode      | dropQnode | NULL | dnodeId | SQL |
| login           | login  | NULL | NULL | appName |
| create stream   | createStream | NULL | 所创建的 strem 名 | SQL |
| drop stream     | dropStream | NULL | 所删除的 stream 名 | SQL |
| grant privileges| grantPrivileges | NULL | 所授予的用户 | SQL | 
| remove privileges | revokePrivileges | NULL | 被收回权限的用户 | SQL | 
| compact database| compact | database name  | NULL | SQL |
| balance vgroup leader | balanceVgroupLead | NULL | NULL | SQL |
| restore dnode | restoreDnode | NULL | dnodeId | SQL |
| restribute vgroup | restributeVgroup | NULL | vgroupId | SQL |
| balance vgroup | balanceVgroup | NULL | vgroupId | SQL |
| create table | createTable | db name | NULL | table name |
| drop table | dropTable | db name | NULL | table name |

### 查看审计日志

在 taosd 和 taosKeeper 都正确配置并启动之后，随着系统的不断运行，系统中的各种操作（如上表所示）会被实时记录并上报，用户可以登录 taosExplorer，点击 “系统管理”→“审计” 页面，即可查看审计日志; 也可以在 TDengine CLI 中直接查询相应的库和表。

## 数据加密

TDengine 支持透明数据加密（Transparent Data Encryption，TDE），通过对静态数据文件进行加密，阻止可能的攻击者绕过数据库直接从文件系统读取敏感信息。数据库的访问程序是完全无感知的，应用程序不需要做任何修改和编译，就能够直接应用到加密后的数据库，支持国标 SM4 等加密算法。在透明加密中，数据库密钥管理、数据库加密范围是两个最重要的话题。TDengine 采用机器码对数据库密钥进行加密处理，保存在本地而不是第三方管理器中。当数据文件被拷贝到其他机器后，由于机器码发生变化，无法获得数据库密钥，自然无法访问数据文件。TDengine 对所有数据文件进行加密，包括预写日志文件、元数据文件和时序数据文件。加密后，数据压缩率不变，写入性能和查询性能仅有轻微下降。

### 配置密钥

密钥配置分离线设置和在线设置两种方式。

方式一，离线设置。通过离线设置可为每个节点分别配置密钥，命令如下。
```shell
taosd -y {encryptKey}
```

方式二，在线设置。当集群所有节点都在线时，可以使用 SQL 配置密钥，SQL 如下。
```sql
create encrypt_key {encryptKey};
```
在线设置方式要求所有已经加入集群的节点都没有使用过离线设置方式生成密钥，否则在线设置方式会失败，在线设置密钥成功的同时也自动加载和使用了密钥。

### 创建加密数据库

TDengine 支持通过 SQL 创建加密数据库，SQL 如下。
```sql
create database [if not exists] db_name [database_options]
database_options:
 database_option ...
database_option: {
 encrypt_algorithm {'none' |'sm4'}
}
```

主要参数说明如下。
encrypt_algorithm：指定数据采用的加密算法。默认是 none，即不采用加密。sm4 表示采用 SM4 加密算法

### 查看加密配置

用户可通过查询系统数据库 ins_databases 获取数据库当前加密配置，SQL 如下。
```sql
select name, `encrypt_algorithm` from ins_databases;
              name              | encrypt_algorithm |
=====================================================
 power1                         | none              |
 power                          | sm4               |
```

### 查看节点密钥状态

通过以下的SQL命令参看节点密钥状态：

```sql
show encryptions;

select * from information_schema.ins_encryptions;
  dnode_id   |           key_status           |
===============================================
           1 | loaded                         |
           2 | unset                          |
           3 | unknown                        |
```
key_status 有三种取值：
- 当节点未设置密钥时，状态列显示 unset。
- 当密钥被检验成功并且加载后，状态列显示 loaded.
- 当节点未启动，key的状态无法被探知时，状态列显示 unknown

### 更新密钥配置

当节点的硬件配置发生变更时，需要通过以下命令更新密钥，与离线配置密钥的命令相同：

```shell
taosd -y  {encryptKey}
```
更新密钥配置，需要先停止 taosd，并且使用完全相同的密钥，也即密钥在数据库创建后不能修改。