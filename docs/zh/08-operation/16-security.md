---
sidebar_label: 更多安全策略
title: 更多安全策略
toc_max_heading_level: 4
---

除了传统的用户和权限管理之外，TDengine TSDB 还有其他的安全策略，例如 IP 白名单、审计日志、数据加密等，这些都是 TDengine TSDB Enterprise 特有功能，其中白名单功能在 3.2.0.0 版本首次发布，审计日志在 3.1.1.0 版本中首次发布，数据库加密在 3.3.0.0 中首次发布，建议使用最新版本。

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

说明

- 开源版和企业版本都能添加成功，且可以查询到，但是开源版本不会对 IP 做任何限制。
- `create user u_write pass 'taosdata1' host 'iprange1','iprange2'`，可以一次添加多个 ip range，服务端会做去重，去重的逻辑是需要 ip range 完全一样
- 默认会把 `127.0.0.1` 添加到白名单列表，且在白名单列表可以查询
- 集群的节点 IP 集合会自动添加到白名单列表，但是查询不到。
- taosadaper 和 taosd 不在一个机器的时候，需要把 taosadaper IP 手动添加到 taosd 白名单列表中
- 集群情况下，各个节点 enableWhiteList 成一样，或者全为 false，或者全为 true，要不然集群无法启动
- 白名单变更生效时间 1s，不超过 2s，每次变更对收发性能有些微影响（多一次判断，可以忽略），变更完之后、影响忽略不计，变更过程中对集群没有影响，对正在访问客户端也没有影响（假设这些客户端的 IP 包含在 white list 内）
- 如果添加两个 ip range，192.168.1.1/16(假设为 A)，192.168.1.1/24(假设为 B)，严格来说，A 包含了 B，但是考虑情况太复杂，并不会对 A 和 B 做合并
- 要删除的时候，必须严格匹配。也就是如果添加的是 192.168.1.1/24，要删除也是 192.168.1.1/24
- 只有 root 才有权限对其他用户增删 ip white list
- 兼容之前的版本，但是不支持从当前版本回退到之前版本
- x.x.x.x/32 和 x.x.x.x 属于同一个 iprange，显示为 x.x.x.x
- 如果客户端拿到的 0.0.0.0/0，说明没有开启白名单
- 如果白名单发生了改变，客户端会在 heartbeat 里检测到。
- 针对一个 user，添加的 IP 个数上限是 2048

## 审计日志

TDengine TSDB 先对用户操作进行记录和管理，然后将这些作为审计日志发送给 taosKeeper，再由 taosKeeper 保存至任意 TDengine TSDB 集群。管理员可通过审计日志进行安全监控、历史追溯。TDengine TSDB 的审计日志功能开启和关闭操作非常简单，只须修改 TDengine TSDB 的配置文件后重启服务。审计日志的配置说明如下。

### taosd 配置

审计日志由数据库服务 taosd 产生，其相应参数要配置在 taos.cfg 配置文件中，详细参数如下表。

| 参数名称       | 参数含义                                                  |
|:-------------:|:--------------------------------------------------------:|
|audit       | 是否打开审计日志，1 为开启，0 为关闭，默认值为 0。 |
|monitorFqdn | 接收审计日志的 taosKeeper 所在服务器的 FQDN |
|monitorPort | 接收审计日志的 taosKeeper 服务所用端口 |
|monitorCompaction | 上报数据时是否进行压缩 |

### taosKeeper 配置

在 taosKeeper 的配置文件 keeper.toml 中配置与审计日志有关的配置参数，如下表所示

| 参数名称       | 参数含义                                                  |
|:-------------:|:--------------------------------------------------------:|
|auditDB | 用于存放审计日志的数据库的名字，默认值为 "audit"，taosKeeper 在收到上报的审计日志后会判断该数据库是否存在，如果不存在会自动创建 |

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

taosKeeper 会依据上报的审计数据在相应的数据库中自动建立超级表用于存储数据。该超级表的定义如下：

```sql
create stable operations (ts timestamp, user_name varchar(25), operation varchar(20), db varchar(65), resource varchar(193), client_address varchar(64), details varchar(50000)) tags (cluster_id varchar(64))
```

其中：

1. db 为操作涉及的 database，resource 为操作涉及的资源。
2. user_name 和 operation 为数据列，表示哪个用户在该对象上进行了什么操作。
3. ts 为时间戳列，表示操作发生时的时间。
4. details 为该操作的一些补充细节，在大多数操作下是所执行的操作的 SQL 语句。
5. client_address 为客户端地址，包括 ip 和端口。

### 操作列表

目前审计日志中所记录的操作列表以及每个操作中各字段的含义（因为每个操作的施加者，即 user、client_add、时间戳字段在所有操作中的含义相同，下表不再描述）

| 操作        | Operation | DB | Resource | Details |
| ----------------| ----------| ---------| ---------| --------|
| create database | createDB  | db name  | NULL     | SQL |
| alter database  | alterDB   | db name  | NULL     | SQL |
| drop database   | dropDB    | db name  | NULL     | SQL |
| create stable   | createStb | db name  | stable name | SQL |
| alter stable    | alterStb  | db name  | stable name | SQL |
| drop stable     | dropStb   | db name  | stable name | SQL |
| create user     | createUser | NULL |  被创建的用户名 | 用户属性参数， (password 除外) |
| alter user      | alterUser | NULL | 被修改的用户名 | 修改密码记录被修改的参数和新值 (password 除外)，其他操作记录 SQL |
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
| create stream   | createStream | NULL | 所创建的 stream 名 | SQL |
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

在 taosd 和 taosKeeper 都正确配置并启动之后，随着系统的不断运行，系统中的各种操作（如上表所示）会被实时记录并上报，用户可以登录 taosExplorer，点击**系统管理**→**审计**页面，即可查看审计日志; 也可以在 TDengine TSDB CLI 中直接查询相应的库和表。

## 存储安全

TDengine TSDB 支持透明数据加密（Transparent Data Encryption，TDE），通过对静态数据文件进行加密，阻止可能的攻击者绕过数据库直接从文件系统读取敏感信息。数据库的访问程序是完全无感知的，应用程序不需要做任何修改和编译，就能够直接应用到加密后的数据库。存储安全特性支持国密 SM4 和 AES 等加密算法，采用分级密钥管理机制，并提供完善的密钥备份恢复功能。

### 密钥体系

TDengine TSDB 采用分级密钥管理体系，包括以下密钥类型：

- **SVR_KEY（服务器主密钥）**：用于加密数据库主密钥和系统级信息，与机器硬件绑定，防止跨机移植
- **DB_KEY（数据库主密钥）**：用于加密各类派生密钥
- **CFG_KEY（配置加密密钥）**：专用于加密配置文件，一旦生成不可更改
- **META_KEY（元数据加密密钥）**：用于加密元数据文件，一旦生成不可更改
- **DATA_KEY（时序数据加密密钥）**：用于加密时序数据文件和相关日志，一旦生成不可更改

所有密钥使用机器码绑定技术，当数据文件被拷贝到其他机器后，由于机器码发生变化，无法获得密钥，自然无法访问数据文件。加密后，数据压缩率不变，写入性能和查询性能小幅下降。

**注意**：存储安全功能需要获取机器码，在某些虚拟化环境（如某些容器环境）中可能无法使用。

### 生成密钥

使用 `taosk` 工具生成密钥，基本语法如下：

```shell
taosk -c /etc/taos \
  --set-cfg-algorithm sm4 \
  --set-meta-algorithm sm4 \
  --encrypt-server [svr_key] \
  --encrypt-database [db_key] \
  --encrypt-config \
  --encrypt-metadata \
  --encrypt-data [data_key]
```

主要参数说明：

- `-c`：指定配置文件路径，默认 `/etc/taos`
- `--set-cfg-algorithm`：设置配置文件加密算法（sm4 或 aes），默认 sm4
- `--set-meta-algorithm`：设置元数据加密算法（sm4 或 aes），默认 sm4
- `--encrypt-server`：启用服务器加密，可选择性指定 SVR_KEY，不指定则自动生成
- `--encrypt-database`：启用数据库加密，可选择性指定 DB_KEY，不指定则自动生成
- `--encrypt-config`：启用配置文件加密，自动生成 CFG_KEY
- `--encrypt-metadata`：启用元数据加密，自动生成 META_KEY
- `--encrypt-data`：启用数据文件加密，可选择性指定 DATA_KEY，不指定则自动生成

示例：

```shell
# 生成所有密钥，使用默认 SM4 算法
taosk -c /etc/taos \
  --encrypt-server \
  --encrypt-database \
  --encrypt-config \
  --encrypt-metadata \
  --encrypt-data

# 指定密钥并使用不同算法
taosk -c /etc/taos \
  --set-cfg-algorithm aes \
  --set-meta-algorithm sm4 \
  --encrypt-server mysvr123 \
  --encrypt-database mydb4567 \
  --encrypt-config \
  --encrypt-metadata \
  --encrypt-data oldkey123
```

密钥生成后会保存在以下位置：

- `{dataDir}/dnode/config/master.bin`：存储 SVR_KEY 和 DB_KEY
- `{dataDir}/dnode/config/derived.bin`：存储 CFG_KEY、META_KEY 和 DATA_KEY

### 创建加密数据库

TDengine TSDB 支持在创建数据库时指定加密算法，SQL 如下：

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [database_options]
database_options:
  database_option ...
database_option: {
  ENCRYPT_ALGORITHM {'none' | 'SM4-CBC' | 'AES-128-CBC'}
}
```

参数说明：

- `encrypt_algorithm`：指定数据采用的加密算法。默认是 none，即不采用加密。SM4-CBC 表示采用 SM4-CBC 加密算法，AES-128-CBC 表示采用 AES-128-CBC 加密算法

示例：

```sql
-- 创建使用 SM4 加密的数据库
CREATE DATABASE db1 ENCRYPT_ALGORITHM 'SM4-CBC';

-- 创建使用 AES 加密的数据库
CREATE DATABASE db2 ENCRYPT_ALGORITHM 'AES-128-CBC';

-- 创建不加密的数据库
CREATE DATABASE db3;
```

**注意**：

- 数据库的 ENCRYPT_ALGORITHM 在创建后不能修改
- 创建加密数据库前必须先使用 taosk 生成包含 DATA_KEY 的密钥

### 查看加密状态

#### 查看系统加密状态

通过系统表查看整体加密状态：

```sql
SELECT * FROM information_schema.ins_encrypt_status;
         encrypt_scope          |           algorithm            |       status       |
=======================================================================================
 config                         | AES-128-CBC                    | enabled            |
 metadata                       | AES-128-CBC                    | enabled            |
 data                           | SM4-CBC:SM4                    | enabled            |
```

字段说明：

- `encrypt_scope`：加密范围（config、metadata、data）
- `algorithm`：使用的加密算法
- `status`：加密状态（enabled 或 disabled）

#### 查看数据库加密配置

通过系统表查看各数据库的加密算法：

```sql
SELECT name,`encrypt_algorithm` FROM information_schema.ins_databases;
              name              | encrypt_algorithm  |
======================================================
 information_schema             | NULL               |
 performance_schema             | NULL               |
 db2                            | AES-128-CBC        |
 db1                            | SM4-CBC            |
```

### 更新密钥

可以通过 taosk 工具或 SQL 命令更新 SVR_KEY 和 DB_KEY（其他密钥一旦生成不可更改）。

#### 使用 taosk 更新

```shell
# 停止 taosd
systemctl stop taosd

# 更新密钥
taosk -c /etc/taos --update-svrkey new_svr_key --update-dbkey new_db_key

# 启动 taosd
systemctl start taosd
```

#### 使用 SQL 更新

在 taosd 运行时，可通过 SQL 更新密钥（需要管理员权限）：

```sql
-- 更新 SVR_KEY
ALTER SYSTEM SET SVR_KEY 'new_svr_key';

-- 更新 DB_KEY
ALTER SYSTEM SET DB_KEY 'new_db_key';
```

### 密钥备份与恢复

#### 备份密钥

使用 taosk 创建便携式备份（不包含机器码绑定，可在其他机器恢复）：

```shell
taosk -c /etc/taos --backup --svr-key your_svr_key
```

备份文件会生成在 `{dataDir}/dnode/config/` 目录下，文件名格式为 `master.bin.backup.{timestamp}`。

**注意**：备份时需要提供正确的 SVR_KEY 进行验证。

#### 恢复密钥

在新机器上从备份恢复密钥：

```shell
taosk -c /etc/taos \
  --restore \
  --machine-code /path/to/backup_file \
  --svr-key your_svr_key
```

恢复操作会将密钥绑定到当前机器的机器码。

### 密钥到期策略

可以通过 SQL 设置密钥到期时间和策略（需要管理员权限）：

```sql
ALTER SYSTEM SET KEY_EXPIRATION 90 DAYS STRATEGY 'ALARM';
```

策略选项：

- `ALARM`：密钥到期时会在日志中输出告警信息。

### 配置文件行为变更

启用存储安全后，TDengine TSDB 的配置管理方式发生以下变化：

1. **配置仅首次启动有效**：系统初次启动后，后续修改 taos.cfg 文件不会生效
2. **通过 SQL 修改配置**：所有配置修改必须通过 SQL 命令执行，需要管理员权限

修改配置示例：

```sql
ALTER DNODE 1 'debugFlag' '143';
```

### 透明加密范围

启用存储安全后，TDengine TSDB 会对以下文件进行透明加密：

1. **配置文件加密**（需要 CFG_KEY）：
   - dnode.info、dnode.json
   - mnode.json、raft_config.json、raft_store.json
   - vnodes.json、vnode.json 等

2. **元数据文件加密**（需要 META_KEY）：
   - mnode 的 SDB
   - snode 的 checkpoint 文件

3. **数据文件加密**（需要 DATA_KEY）：
   - TSDB 数据文件
   - WAL 预写日志文件
   - STT 文件
   - TDB、BSE 等索引文件

所有配置文件加密后会在开头包含明文标识头（"tdEncrypt"），用于标记文件已加密，避免重复加密。

### 版本兼容性

- 从不支持存储安全的版本升级到新版本，可以正常运行
- 历史版本的加密数据库可以通过指定 DATA_KEY 进行兼容
- 启用存储安全后，不能回退到不支持存储安全的历史版本
