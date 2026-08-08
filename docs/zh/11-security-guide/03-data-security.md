---
sidebar_label: 数据安全
title: 数据安全
description: TDengine TSDB Enterprise IP 白名单、安全删除与透明数据加密（TDE）
toc_max_heading_level: 4
---

除了传统的用户和权限管理之外，TDengine 还有其他的安全策略，例如 IP 白名单、审计日志、数据加密等，这些都是 TDengine TSDB Enterprise 特有功能。白名单功能在 `v3.2.0.0` 首次发布，审计日志在 `v3.1.1.0` 首次发布，数据库加密在 `v3.3.0.0` 首次发布，建议使用最新版本。另可通过数据库 / 语句选项启用安全删除（`SECURE_DELETE`）。审计能力见 [审计与合规](./05-audit-and-compliance.md)；本节说明 IP 白名单、安全删除与存储加密。

数据库 DDL 中的 `ENCRYPT_ALGORITHM`、`IS_AUDIT`、`SECURE_DELETE`、`SECURITY_LEVEL` 等选项，语法入口见 [数据库](../05-tdengine-sql/02-ddl/01-database.md)。其中 `SECURITY_LEVEL`（MAC）详见 [权限管理](../05-tdengine-sql/07-user-and-privilege/02-grant.md#强制访问控制mac)；`IS_AUDIT` 约束见 [审计与合规](./05-audit-and-compliance.md)；`SECURE_DELETE` 见下文 [安全删除](#安全删除)。

## IP 白名单

IP 白名单是一种网络安全技术，它使 IT 管理员能够控制“谁”可以访问系统和资源，提升数据库的访问安全性，避免外部的恶意攻击。IP 白名单通过创建可信的 IP 地址列表，将它们作为唯一标识符分配给用户，并且只允许这些 IP 地址访问目标服务器。请注意，用户权限与 IP 白名单是不相关的，两者分开管理。完整 `HOST` / `NOT_ALLOW_HOST` 语法与行为见 [用户管理](../05-tdengine-sql/07-user-and-privilege/01-user.md)。须将 `enableWhiteList` 设为 `1` 后黑白名单才会生效（参数说明见 [taosd](../12-operations-and-tooling/03-components/01-taosd.md)）。下面是配置 IP 白名单的具体方法。

增加 IP 白名单的 SQL 如下：

```sql
CREATE USER test PASS 'taosdata1' HOST '192.168.1.0/24', '10.0.0.1';
ALTER USER test ADD HOST '192.168.2.0/24';
```

查询 IP 白名单的 SQL 如下：

```sql
SELECT name, allowed_host FROM information_schema.ins_users;
SHOW USERS;
```

删除 IP 白名单的命令如下：

```sql
ALTER USER test DROP HOST '192.168.2.0/24';
```

说明：

- 开源版和企业版都能添加成功，且可以查询到，但是开源版不会对 IP 做任何限制。
- 一次可以添加多个 IP range，服务端会做去重，去重的逻辑是需要 IP range 完全一样。例如：`CREATE USER u_write PASS 'taosdata1' HOST 'iprange1','iprange2'`。
- 默认会把 `127.0.0.1` 添加到白名单列表，且在白名单列表可以查询（用户手册所述场景下亦可能包含 `::1`）。
- 集群的节点 IP 集合会自动添加到白名单列表，但是查询不到。
- `taosAdapter` 和 `taosd` 不在一个机器的时候，需要把 `taosAdapter` 的 IP 手动添加到 `taosd` 白名单列表中。
- 集群情况下，各个节点的 `enableWhiteList` 须一致，或者全为 `false`，或者全为 `true`，要不然集群无法启动。
- 白名单变更生效时间约 1s，不超过 2s。每次变更对收发性能有些微影响（多一次判断，可以忽略），变更完之后影响忽略不计；变更过程中对集群没有影响，对正在访问且 IP 已包含在白名单内的客户端也没有影响。
- 如果添加两个 IP range，例如 `192.168.1.1/16`（假设为 A）与 `192.168.1.1/24`（假设为 B），严格来说 A 包含了 B，但考虑情况太复杂，并不会对 A 和 B 做合并。
- 要删除的时候，必须严格匹配。也就是如果添加的是 `192.168.1.1/24`，要删除也是 `192.168.1.1/24`。
- 只有 `root` 才有权限对其他用户增删 IP 白名单。
- 兼容之前的版本，但是不支持从当前版本回退到之前版本。
- `x.x.x.x/32` 和 `x.x.x.x` 属于同一个 IP range，显示为 `x.x.x.x`。
- 如果客户端拿到的是 `0.0.0.0/0`，说明没有开启白名单。
- 如果白名单发生了改变，客户端会在 heartbeat 里检测到。
- 针对一个 user，添加的 IP 个数上限是 2048。

## 安全删除

数据库选项 `SECURE_DELETE`（取值 `0` / `1`，默认 `0`）控制删除路径是否在写入删除标记之外，对落盘数据块做物理覆写。DDL 语法见 [数据库 · SECURE_DELETE](../05-tdengine-sql/02-ddl/01-database.md#secure_delete)；单次删除也可在语句末尾加 `SECURE_DELETE` 关键字，见 [数据删除](../05-tdengine-sql/03-data-write/02-delete.md)。

- **关闭（`0`）**：仅写入删除标记；查询不再返回已删数据，但对应文件块在后续压缩/回收前仍可能残留在磁盘上。
- **开启（`1`）**：在删除标记之外，对 DATA / STT 等落盘文件中命中 `(表, 时间区间)` 的数据块执行文件级覆写（secure erase），降低通过文件系统直接读取已删内容的风险。

行为要点：

- 生效条件为库级 `SECURE_DELETE=1`、表/超级表元数据中的安全删除标志，或语句级 `DELETE ... SECURE_DELETE` 三者之一（实现上按位或合并）。
- 物理覆写在删除标记写入之后执行；覆写失败会记服务端日志，查询语义仍以删除标记为准（已删数据不会因覆写失败而重新可见）。
- 当前实现面向新版 TSDB 文件格式；旧格式文件会跳过文件级覆写，依赖后续压缩等路径回收。
- 多副本场景下，文件级覆写在 Raft leader 上执行；follower 通过 WAL 重放逻辑删除，不会自动复现同一套物理覆写操作。
- WAL 中仍可能保留删除前的原始写入记录，直至检查点后的 WAL 裁剪；OS 页缓存、SSD 磨损均衡等也可能使物理介质上短期仍可见旧内容。本能力不是硬件级 Secure Erase / Sanitize，也不等同于“静态加密 + 销毁密钥”。
- 开启后删除路径 I/O 与耗时会增加，请按业务对残留数据清除的要求与性能开销权衡。
- 与 TDE 互补：TDE 降低静态文件被直接解读的风险；安全删除侧重删除后对残留块的覆写。二者均不构成特定法规或认证符合性声明。
- 全局参数 `secureEraseMode`（默认 `0`）控制整块可直接覆写时的填充方式：`0` 为零填充，`1` 为随机字节；部分重叠块为保证就地写回始终零填充。详见 [taosd · secureEraseMode](../12-operations-and-tooling/03-components/01-taosd.md#secureerasemode)。

示例：

```sql
CREATE DATABASE db SECURE_DELETE 1;
ALTER DATABASE db SECURE_DELETE 1;
DELETE FROM meters WHERE ts < '2021-10-01 10:40:00.100' SECURE_DELETE;
```

## 存储安全

TDengine 支持透明数据加密（Transparent Data Encryption，TDE），通过对静态数据文件进行加密，阻止可能的攻击者绕过数据库直接从文件系统读取敏感信息。数据库的访问程序是完全无感知的，应用程序不需要做任何修改和编译，就能够直接应用到加密后的数据库，支持国标 SM4 等加密算法。在透明加密中，数据库密钥管理、数据库加密范围是两个最重要的话题。TDengine 采用机器码对数据库密钥进行加密处理，保存在本地而不是第三方管理器中。当数据文件被拷贝到其他机器后，由于机器码发生变化，无法获得数据库密钥，自然无法访问数据文件。TDengine 对所有数据文件进行加密，包括预写日志文件、元数据文件和时序数据文件。加密后，数据压缩率不变，写入性能和查询性能仅有轻微下降。

### 密钥层级

TDengine 使用分层密钥管理体系：

- **SVR_KEY（服务端主密钥）**：用于加密数据库主密钥和系统级信息，并绑定机器硬件以阻止跨机器直接迁移。
- **DB_KEY（数据库主密钥）**：用于加密各类派生密钥。
- **CFG_KEY（配置加密密钥）**：用于加密配置文件，生成后不可更改。
- **META_KEY（元数据加密密钥）**：用于加密元数据文件，生成后不可更改。
- **DATA_KEY（时序数据加密密钥）**：用于加密时序数据文件及相关日志，生成后不可更改。

:::note
存储安全功能需要获取机器码，某些虚拟化环境（如部分容器环境）可能无法提供机器码。
:::

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
- `-d`：指定数据目录（`dataDir`），默认从配置文件读取
- `--set-cfg-algorithm`：设置配置文件加密算法（`sm4` 或 `aes`），默认 `sm4`
- `--set-meta-algorithm`：设置元数据加密算法（`sm4` 或 `aes`），默认 `sm4`
- `--encrypt-server`：启用服务器加密，可选择性指定 `SVR_KEY`，不指定则自动生成
- `--encrypt-database`：启用数据库加密，可选择性指定 `DB_KEY`，不指定则自动生成
- `--encrypt-config`：启用配置文件加密，自动生成 `CFG_KEY`
- `--encrypt-metadata`：启用元数据加密，自动生成 `META_KEY`
- `--encrypt-data`：启用数据文件加密，可选择性指定 `DATA_KEY`，不指定则自动生成

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

- `{dataDir}/dnode/config/master.bin`：存储 `SVR_KEY` 和 `DB_KEY`
- `{dataDir}/dnode/config/derived.bin`：存储 `CFG_KEY`、`META_KEY` 和 `DATA_KEY`

### 查看加密配置文件

使用 `taosk` 工具可以查看加密的配置文件内容。

其中 `-d` 用于指定 TDengine 的数据目录（即 `dataDir`，例如 `/var/lib/taos`），工具会从该目录加载解密所需的密钥。如果已经通过 `-c` 指定了配置目录，且对应配置文件中包含正确的 `dataDir`，则可以省略 `-d`。

```shell
taosk -d /var/lib/taos --view-config /path/to/encrypted_config.json
```

该命令会自动从数据目录加载密钥，解密并显示配置文件内容。

### 编辑加密配置文件

使用 `taosk` 工具可以直接编辑加密的配置文件：

```shell
taosk -d /var/lib/taos --edit-file /path/to/encrypted_config.json
```

该命令会：

1. 从数据目录加载 `CFG_KEY`
2. 解密配置文件到临时文件（权限 `0600`）
3. 使用系统编辑器（`$EDITOR` 或 `vi`）打开文件
4. 通过 SHA-256 哈希检测文件变化
5. 如有修改，自动重新加密并写回原文件
6. 清理临时文件

**注意**：

- 编辑前必须先生成包含 `CFG_KEY` 的密钥（使用 `--encrypt-config` 选项）
- 可通过 `EDITOR` 环境变量指定编辑器，如 `EDITOR=nano taosk -d /var/lib/taos --edit-file /path/to/encrypted_config.json`
- 如果退出编辑器时未保存，文件不会被修改

### 查看加密状态

#### 查看系统加密状态

通过系统表查看整体加密状态：

```sql
SELECT * FROM information_schema.ins_encrypt_status;
```

示例输出：

```text
         encrypt_scope          |           algorithm            |       status       |
=======================================================================================
 config                         | AES-128-CBC                    | enabled            |
 metadata                       | AES-128-CBC                    | enabled            |
 data                           | SM4-CBC:SM4                    | enabled            |
```

字段说明：

- `encrypt_scope`：加密范围（`config`、`metadata`、`data`）
- `algorithm`：使用的加密算法
- `status`：加密状态（`enabled` 或 `disabled`）

### 更新密钥

可以通过 `taosk` 工具或 SQL 命令更新 `SVR_KEY` 和 `DB_KEY`（其他密钥一旦生成不可更改）。

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

在 `taosd` 运行时，可通过 SQL 更新密钥（需要管理员权限）：

```sql
-- 更新 SVR_KEY
ALTER SYSTEM SET SVR_KEY 'new_svr_key';

-- 更新 DB_KEY
ALTER SYSTEM SET DB_KEY 'new_db_key';
```

### 密钥备份与恢复

#### 备份密钥

使用 `taosk` 创建便携式备份（不包含机器码绑定，可在其他机器恢复）：

```shell
taosk -c /etc/taos --backup --svr-key your_svr_key
```

备份文件会生成在 `{dataDir}/dnode/config/` 目录下，文件名格式为 `master.bin.backup.{timestamp}`。

**注意**：备份时需要提供正确的 `SVR_KEY` 进行验证。

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

启用存储安全后，TDengine 的配置管理方式发生以下变化：

1. **配置仅首次启动有效**：系统初次启动后，后续修改 `taos.cfg` 文件不会生效。
2. **通过 SQL 修改配置**：所有配置修改必须通过 SQL 命令执行，需要管理员权限。

修改配置示例：

```sql
ALTER DNODE 1 'debugFlag' '143';
```

### 透明加密范围

启用存储安全后，TDengine 会对以下文件进行透明加密：

1. **配置文件加密**（需要 `CFG_KEY`）：
   - `dnode.info`、`dnode.json`
   - `mnode.json`、`raft_config.json`、`raft_store.json`
   - `vnodes.json`、`vnode.json` 等

2. **元数据文件加密**（需要 `META_KEY`）：
   - mnode 的 SDB
   - snode 的 checkpoint 文件

3. **数据文件加密**（需要 `DATA_KEY`）：
   - TSDB 数据文件
   - WAL 预写日志文件
   - STT 文件
   - TDB、BSE 等索引文件

所有配置文件加密后会在开头包含明文标识头（`tdEncrypt`），用于标记文件已加密，避免重复加密。

### 版本兼容性

- 从不支持存储安全的版本升级到新版本，可以正常运行。
- 历史版本的加密数据库可以通过指定 `DATA_KEY` 进行兼容。
- 启用存储安全后，不能回退到不支持存储安全的历史版本。

### 查看加密算法

你可以查看所有内置可用加密算法：

```sql
SHOW ENCRYPT_ALGORITHMS;
```

示例输出：

```text
id      |          algorithm_id          |              name              |              desc              |              type              |             source             |         ossl_algr_name         |
1 | SM4-CBC                        | SM4                            | SM4 symmetric encryption       | Symmetric Ciphers CBC mode     | build-in                       | SM4-CBC:SM4                    |
2 | AES-128-CBC                    | AES                            | AES symmetric encryption       | Symmetric Ciphers CBC mode     | build-in                       | AES-128-CBC                    |
```

字段说明：

1. `id`：算法的数字标识，内置算法从 1 开始，自定义算法从 101 开始。
2. `algorithm_id`：算法的全局唯一标识。
3. `name`：算法名称。
4. `desc`：算法的描述。
5. `type`：算法类型，包括：Symmetric Ciphers CBC mode（对称加密算法 CBC 模式，用于数据库加密）、Asymmetric Ciphers（非对称加密算法）、Digests（散列算法）。
6. `source`：算法来源，包括：`built-in`（内置算法）、`customized`（用户自定义算法）。示例输出中内置算法可能显示为 `build-in`，与 `built-in` 同义，以实际查询结果为准。
7. `ossl_algr_name`：算法在 OpenSSL 中的名称。如果是内置算法则是在 default provider 中的名称，可以参考 [OSSL_PROVIDER-default](https://docs.openssl.org/master/man7/OSSL_PROVIDER-default/)；如果是自定义算法，则是你在程序中自定义的名称。

### 添加自定义算法

你可以添加自己的自定义算法：

```sql
CREATE ENCRYPT_ALGR 'vigenere' ALGR_NAME 'vigenere' DESC 'my custom algr'
  ALGR_TYPE 'Symmetric_Ciphers_CBC_mode' OSSL_ALGR_NAME 'vigenere';
```

用户自定义算法需按接口开发一个 so 库，`taosd` 启动时会加载这个 so 库，加载后即可使用自定义算法。在这个 so 库中可以包含多个算法，算法有自己的命名，通过 `CREATE ENCRYPT_ALGR` 中的 `ossl_algr_name` 字段指定。自定义算法接口采用 OpenSSL 的实现，遵循 OpenSSL 的接口定义，参考 [OpenSSL provider](https://docs.openssl.org/master/man7/provider/)。参数 `encryptExtDir` 指定自定义算法库 so 文件的路径，目前只支持加载单个文件。

### 删除自定义算法

你可以删除自己的自定义算法：

```sql
DROP ENCRYPT_ALGR 'vigenere';
```

删除一个自定义算法前，必须保证这个算法没有被使用，比如必须提前删除使用该算法的 database。

内置算法（`source` 为 `build-in`，如 `SM4-CBC`、`AES-128-CBC` 等）不允许删除，执行删除会返回错误。

### 创建加密数据库

TDengine 支持通过 SQL 创建加密数据库，SQL 如下：

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [database_options]

database_options:
  database_option ...

database_option: {
  ENCRYPT_ALGORITHM {'none' | 'sm4' | ...}
}
```

主要参数说明如下：

- `ENCRYPT_ALGORITHM`：指定数据采用的加密算法。默认是 `none`，即不采用加密。如果要设置加密数据，则需指定 `SHOW ENCRYPT_ALGORITHMS` 中类型为 Symmetric Ciphers CBC mode 的 `algorithm_id`。更多 DDL 说明见 [数据库](../05-tdengine-sql/02-ddl/01-database.md)。

示例：

```sql
-- 使用 SM4 加密
CREATE DATABASE db1 ENCRYPT_ALGORITHM 'SM4-CBC';

-- 使用 AES 加密
CREATE DATABASE db2 ENCRYPT_ALGORITHM 'AES-128-CBC';

-- 不加密
CREATE DATABASE db3;
```

**注意**

- 数据库的 `ENCRYPT_ALGORITHM` 创建后不能修改。
- 创建加密数据库前，必须先使用 `taosk --encrypt-data` 生成包含 `DATA_KEY` 的密钥。

### 查看加密配置

你可通过查询系统库 `information_schema.ins_databases` 获取数据库当前加密配置：

```sql
SELECT name, `encrypt_algorithm` FROM information_schema.ins_databases;
```

示例输出：

```text
              name              | encrypt_algorithm |
=====================================================
 power1                         | none              |
 power                          | sm4               |
```

### 加密用户密码

默认情况下，用户的密码会以 MD5 的形式进行存储。可以通过参数 `encryptPassAlgorithm` 将用户密码进行加密储存。`encryptPassAlgorithm` 默认是未设置的状态，在未设置时，不对用户密码进行加密，也即只以 MD5 的形式存储。当 `encryptPassAlgorithm` 设置为 `sm4` 时（目前只支持 `sm4` 加密算法），对用户密码进行加密存储。设置 `encryptPassAlgorithm` 参数前，同样按照前面的步骤配置密钥。
