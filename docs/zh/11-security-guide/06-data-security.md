---
sidebar_label: 静态数据保护
title: 静态数据保护
description: 透明数据加密（TDE）、密钥管理与安全删除（SECURE_DELETE）
toc_max_heading_level: 4
---

本节说明企业版**静态数据保护**：透明数据加密（TDE）与安全删除（`SECURE_DELETE`）。二者互补——TDE 降低磁盘/文件被直接解读的风险；安全删除侧重删除后对残留数据块的物理覆写。文档不声称特定外部法规或认证符合性结论。

## 版本与能力演进

| 版本 | 能力 |
|------|------|
| `v3.3.0.0` | 企业版首次提供库级数据加密（`ENCRYPT_ALGORITHM`）与相关集群密钥机制 |
| `v3.3.7.0` | 用户密码落盘可额外加密（与数据密钥相关，见下文） |
| `v3.4.0.0` | 分级密钥 + `taosk` 全量透明加密（配置 / 元数据 / 时序数据）；加密算法扩展与自定义算法 |
| `v3.4.1.0` | `SECURE_DELETE` 与全局参数 `secureEraseMode` |
| `v3.4.2.0` | `encryptScope` 增加 `query_spill`（查询落盘临时文件加密） |

建议使用最新企业版。数据库 DDL 中的 `ENCRYPT_ALGORITHM`、`IS_AUDIT`、`SECURE_DELETE`、`SECURITY_LEVEL` 等选项，语法入口见 [数据库](../05-tdengine-sql/02-ddl/01-database.md)。其中 `SECURITY_LEVEL`（MAC）详见 [权限管理](../05-tdengine-sql/07-user-and-privilege/02-grant.md#强制访问控制mac)；`IS_AUDIT` 约束见 [审计与合规](./07-audit-and-compliance.md)。IP 访问控制见 [用户管理 · IP 白名单与黑名单](../05-tdengine-sql/07-user-and-privilege/01-user.md#ip-白名单与黑名单)，不在本节展开。

:::note
**v3.3 与 v3.4+ 的关系**：`v3.3` 以库级 `ENCRYPT_ALGORITHM`（如 `'sm4'`）及集群侧加密密钥为主；`v3.4.0.0` 起推荐以 `taosk` 生成分级密钥，再用 `ENCRYPT_ALGORITHM 'SM4-CBC'` / `'AES-128-CBC'` 等算法 ID 建加密库。下文以 **v3.4+ / `taosk`** 为主路径；升级与兼容见 [版本兼容性](#version-compatibility)。
:::

## 1. 存储安全（TDE）

TDengine 支持透明数据加密（Transparent Data Encryption，TDE）：对静态数据文件加密，降低攻击者绕过数据库、直接从文件系统读取敏感信息的风险。应用程序对加密无感知，无需修改业务代码。内置支持国密 SM4、AES 等对称算法（CBC 模式）。

数据库可选的对称算法包括 `SM4-CBC`、`AES-128-CBC`、`AES-256-CBC` 和对称类型的自定义算法，全部内置算法及其用途见 [加密算法管理](#encrypt-algorithm-management)。

密钥管理采用**机器码绑定**：密钥经机器码保护后保存在本地，而非第三方 KMS。数据文件拷贝到其他机器后，因机器码变化无法解出密钥，从而无法解读文件。加密范围覆盖预写日志、元数据与时序数据文件；加密后压缩率不变，写入与查询性能通常仅有轻微下降。

:::note
存储安全依赖机器码。某些虚拟化 / 容器环境可能无法提供机器码，部署前请验证。
:::

### 1.1 推荐启用流程（v3.4+）

1. 停止业务写入窗口（离线生成密钥时建议停止 `taosd`）。
2. 使用 `taosk` 生成分级密钥（至少包含 `DATA_KEY` 才能建加密库）。
3. 启动 `taosd`，创建加密数据库：`ENCRYPT_ALGORITHM 'SM4-CBC'`（或 `'AES-128-CBC'` 等）。
4. 用 `SHOW ENCRYPT_STATUS` / `ins_encrypt_status` 与 `ins_databases.encrypt_algorithm` 校验。

```shell
taosk -c /etc/taos \
  --encrypt-server \
  --encrypt-database \
  --encrypt-config \
  --encrypt-metadata \
  --encrypt-data
```

```sql
CREATE DATABASE secure_db
  ENCRYPT_ALGORITHM 'SM4-CBC';

SELECT *
FROM information_schema.ins_encrypt_status;

SELECT
  name,
  `encrypt_algorithm`
FROM information_schema.ins_databases;
```

审计库在 `v3.4.0.0` 起要求 `ENCRYPT_ALGORITHM` 不得为 `none`，详见 [审计与合规 · 创建审计库](./07-audit-and-compliance.md)。

### 1.2 密钥层级

| 密钥 | 作用 | 可否更新 |
|------|------|----------|
| `SVR_KEY`（服务端主密钥） | 加密数据库主密钥与系统级信息，并绑定机器硬件 | 可更新 |
| `DB_KEY`（数据库主密钥） | 加密各类派生密钥 | 可更新 |
| `CFG_KEY`（配置加密密钥） | 加密配置类文件 | 生成后不可更改 |
| `META_KEY`（元数据加密密钥） | 加密元数据文件 | 生成后不可更改 |
| `DATA_KEY`（时序数据加密密钥） | 加密时序数据文件及相关日志 | 生成后不可更改 |

依赖关系可理解为：`SVR_KEY` → `DB_KEY` →（`CFG_KEY` / `META_KEY` / `DATA_KEY`）。

### 1.3 生成密钥

使用企业版 `taosk` 工具生成密钥：

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

主要参数：

| 参数 | 说明 |
|------|------|
| `-c` | 配置文件目录，默认 `/etc/taos` |
| `-d` | 数据目录（`dataDir`）；若 `-c` 对应配置已含正确 `dataDir` 可省略 |
| `--set-cfg-algorithm` | 配置文件加密算法：`sm4`、`aes`（AES-128）或 `aes256`（AES-256），默认 `sm4` |
| `--set-meta-algorithm` | 元数据加密算法：`sm4`、`aes`（AES-128）或 `aes256`（AES-256），默认 `sm4` |
| `--set-digest-algorithm` | 加密文件完整性校验使用的摘要算法：`sm3` 或 `sha256`，默认 `sm3` |
| `--encrypt-server` | 启用服务端主密钥；可指定 `SVR_KEY`，省略则自动生成 |
| `--encrypt-database` | 启用数据库主密钥；可指定 `DB_KEY`，省略则自动生成 |
| `--encrypt-config` | 启用配置文件加密，自动生成 `CFG_KEY` |
| `--encrypt-metadata` | 启用元数据加密，自动生成 `META_KEY` |
| `--encrypt-data` | 启用数据文件加密；可指定 `DATA_KEY`，省略则自动生成 |

示例：

```shell
# 生成全部密钥（默认 SM4）
taosk -c /etc/taos \
  --encrypt-server \
  --encrypt-database \
  --encrypt-config \
  --encrypt-metadata \
  --encrypt-data

# 指定密钥并混用算法
taosk -c /etc/taos \
  --set-cfg-algorithm aes \
  --set-meta-algorithm sm4 \
  --encrypt-server mysvr123 \
  --encrypt-database mydb4567 \
  --encrypt-config \
  --encrypt-metadata \
  --encrypt-data oldkey123
```

密钥文件位置：

- `{dataDir}/dnode/config/master.bin`：`SVR_KEY`、`DB_KEY`
- `{dataDir}/dnode/config/derived.bin`：`CFG_KEY`、`META_KEY`、`DATA_KEY`

### 1.4 查看与编辑加密配置文件

```shell
# 查看（自动加载密钥并解密显示）
taosk -d /var/lib/taos --view-config /path/to/encrypted_config.json

# 编辑（解密 → 编辑器 → 变更则重新加密写回）
taosk -d /var/lib/taos --edit-file /path/to/encrypted_config.json
```

编辑流程要点：从数据目录加载 `CFG_KEY`；解密到权限 `0600` 的临时文件；使用 `$EDITOR` 或 `vi`；以 SHA-256 检测变更；写回后清理临时文件。编辑前须已生成 `CFG_KEY`（`--encrypt-config`）。可用 `EDITOR=nano` 指定编辑器；未保存退出则不改文件。

### 1.5 查看加密状态

```sql
SHOW ENCRYPT_STATUS;

-- 等价
SELECT *
FROM information_schema.ins_encrypt_status;
```

示例输出：

```text
         encrypt_scope          |           algorithm            |       status       |
=======================================================================================
 config                         | AES-128-CBC                    | enabled            |
 metadata                       | AES-128-CBC                    | enabled            |
 data                           | SM4-CBC:SM4                    | enabled            |
```

| 字段 | 说明 |
|------|------|
| `encrypt_scope` | 范围：`config` / `metadata` / `data` |
| `algorithm` | 所用算法 |
| `status` | `enabled` 或 `disabled` |

系统表字段说明见 [INS_ENCRYPT_STATUS](../05-tdengine-sql/09-system-info/01-meta.md#ins_encrypt_status)；另有历史密钥状态查询 [SHOW ENCRYPTIONS](../05-tdengine-sql/09-system-info/03-show.md#show-encryptions)。

### 1.6 更新密钥

仅 `SVR_KEY`、`DB_KEY` 可更新；`CFG_KEY` / `META_KEY` / `DATA_KEY` 生成后不可更改。

**离线（`taosk`）**：

```shell
systemctl stop taosd
taosk -c /etc/taos --update-svrkey new_svr_key --update-dbkey new_db_key
systemctl start taosd
```

**在线（SQL，需管理员权限）**：

```sql
ALTER SYSTEM SET SVR_KEY 'new_svr_key';

ALTER SYSTEM SET DB_KEY 'new_db_key';
```

### 1.7 密钥备份与恢复

备份生成**不绑定机器码**的便携副本，便于迁机；恢复时再绑定当前机器码。

```shell
# 备份（须提供正确 SVR_KEY 校验）
taosk -c /etc/taos --backup --svr-key your_svr_key
# 生成于 {dataDir}/dnode/config/master.bin.backup.{timestamp}

# 在新机器恢复
taosk -c /etc/taos \
  --restore \
  --machine-code /path/to/backup_file \
  --svr-key your_svr_key
```

备份文件不绑定机器码，拿到备份文件和 `SVR_KEY` 即可恢复密钥。可以在备份时再用 SM2 或 RSA 公钥加密备份文件，恢复时须同时持有对应的私钥，降低备份文件在传输和存放过程中泄露的风险：

```shell
# 使用 SM2 公钥加密备份文件
taosk -c /etc/taos --backup --svr-key your_svr_key \
  --backup-algorithm sm2 \
  --public-key /path/to/sm2-public.pem

# 使用对应私钥恢复
taosk -c /etc/taos --restore \
  --machine-code /path/to/backup_file \
  --svr-key your_svr_key \
  --private-key /path/to/sm2-private.pem
```

| 参数 | 说明 |
|------|------|
| `--backup-algorithm` | 备份文件的加密算法：`sm2` 或 `rsa`。省略时备份文件不做额外加密 |
| `--public-key` | PEM 格式公钥文件。使用 `rsa` 时，密钥长度不得小于 2048 位 |
| `--private-key` | 恢复时使用的 PEM 格式私钥文件，须与备份时的公钥配对 |

SM2 密钥对可以用支持国密的 OpenSSL（3.0 及以上）生成：

```shell
openssl genpkey -algorithm SM2 -out sm2-private.pem
openssl pkey -in sm2-private.pem -pubout -out sm2-public.pem
```

### 1.8 密钥到期策略

```sql
ALTER SYSTEM SET KEY_EXPIRATION 90 DAYS STRATEGY 'ALARM';
```

当前策略选项：`ALARM`——到期时在日志中输出告警。

### 1.9 配置文件行为变更

启用配置加密（`CFG_KEY`）后：

1. **配置文件多在首次启动生效**：后续直接改 `taos.cfg` 往往不再生效。
2. **运行期改配走 SQL**：例如 `ALTER DNODE 1 'debugFlag' '143';`（需相应权限）。

可用 `taosk --view-config` / `--edit-file` 查看或修改已加密配置文件。

### 1.10 透明加密范围

| 范围 | 所需密钥 | 典型对象 |
|------|----------|----------|
| 配置文件 | `CFG_KEY` | `dnode.info` / `dnode.json`、`mnode.json`、`raft_*.json`、`vnodes.json` / `vnode.json` 等 |
| 元数据 | `META_KEY` | mnode SDB、snode checkpoint 等 |
| 数据文件 | `DATA_KEY` | TSDB、WAL、STT、TDB / BSE 等索引文件 |

已加密文件开头可含明文标识头 `tdEncrypt`，用于识别并避免重复加密。

#### 1.10.1 与 `encryptAlgorithm` / `encryptScope` 的关系

`taos.cfg` 中仍保留企业版参数 [encryptAlgorithm](../12-operations-and-tooling/03-components/01-taosd.md#encryptalgorithm)、[encryptScope](../12-operations-and-tooling/03-components/01-taosd.md#encryptscope)（`v3.3.0.0` 引入），用于声明算法与加密范围组合（如 `tsdb`、`vnode_wal`、`sdb`、`mnode_wal`、`all`）。`v3.4.2.0` 起 `encryptScope` 增加 `query_spill`，用于查询因内存不足而落盘的临时文件加密。

**v3.4+ 推荐以 `taosk` 分级密钥 + 库级 `ENCRYPT_ALGORITHM` 为主路径**；`encryptAlgorithm` / `encryptScope` 为兼容与范围声明参数，二者勿混为两套互不等价的“主流程”。自定义算法 so 路径见 [encryptExtDir](../12-operations-and-tooling/03-components/01-taosd.md#encryptextdir)。

### 1.11 版本兼容性 {#version-compatibility}

- 从不支持存储安全的版本升级到支持版本，一般可正常运行。
- 历史版本加密数据库可通过指定 `DATA_KEY` 等方式兼容迁入（以实际升级说明为准）。
- 启用存储安全后，**不能回退**到不支持存储安全的历史版本。
- `v3.3` 时代的 `CREATE ENCRYPT_KEY` / `taosd -y` 等单密钥流程仅作兼容理解；新部署请使用 `taosk`。

### 1.12 加密算法管理 {#encrypt-algorithm-management}

TDengine 企业版内置国密算法和国际标准算法，覆盖对称加密、摘要和非对称加密三类，并支持按需接入自定义算法。算法可以通过 taosExplorer 的可视化界面管理，也可以通过 SQL 管理。

#### 1.12.1 内置算法

| `id` | `algorithm_id` | 类型 | 遵循标准 | 密钥或摘要长度 | 主要用途 |
|------|----------------|------|----------|----------------|----------|
| 1 | `SM4-CBC` | 对称加密 | GB/T 32907-2016 | 128 位密钥 | 数据库、配置文件、元数据和用户口令加密 |
| 2 | `AES-128-CBC` | 对称加密 | FIPS 197 | 128 位密钥 | 数据库、配置文件和元数据加密 |
| 7 | `AES-256-CBC` | 对称加密 | FIPS 197 | 256 位密钥 | 数据库、配置文件和元数据加密 |
| 3 | `SM3` | 摘要 | GB/T 32905-2016 | 256 位摘要 | 加密文件完整性校验、国密 TLS、SQL 函数 `SM3()` |
| 4 | `SHA-256` | 摘要 | FIPS 180-4 | 256 位摘要 | 加密文件完整性校验、TLS、SQL 函数 `SHA2()` |
| 5 | `SM2` | 非对称加密 | GB/T 32918-2016 | 256 位密钥 | 国密 TLS 证书、密钥备份文件加密 |
| 6 | `RSA` | 非对称加密 | RFC 8017（PKCS #1） | 2048 位及以上 | TLS 证书、密钥备份文件加密 |

说明：

- RSA 密钥长度小于 2048 位时拒绝使用。
- 内置算法由系统维护，不能修改或删除。

#### 1.12.2 算法用途与配置

| 用途 | 可选算法 | 配置方式 |
|------|----------|----------|
| 时序数据加密 | `SM4-CBC`、`AES-128-CBC`、`AES-256-CBC`，或对称类型的自定义算法 | 建库时指定 `ENCRYPT_ALGORITHM`，见下文 1.13 节 |
| 配置文件、元数据加密 | `sm4`、`aes`、`aes256` | `taosk --set-cfg-algorithm`、`--set-meta-algorithm`，见 1.3 节 |
| 用户口令加密存储 | `SM4` | 生成 `DATA_KEY` 后自动启用，见下文 1.14 节 |
| 加密文件完整性校验 | `sm3`、`sha256` | `taosk --set-digest-algorithm`，见 1.3 节 |
| 传输加密 | SM2 证书配合 SM4 / SM3 套件；RSA / ECC 证书配合 AES / SHA-256 套件 | `taos.cfg` 中的 TLS 参数，见 [全链路传输安全 · 国密 TLS](./02-full-trace-transport.md#gm-tls) |
| 密钥备份文件加密 | `sm2`、`rsa` | `taosk --backup --backup-algorithm`，见 1.7 节 |
| SQL 中的加解密与摘要 | SM4、AES、SM3、SHA-2 | `SM4_ENCRYPT`、`SM4_DECRYPT`、`AES_ENCRYPT`、`AES_DECRYPT`、`SM3`、`SHA2` 函数，见 [内置函数](../05-tdengine-sql/04-data-query/03-function.md) |

**加密文件完整性校验**：`taosk` 加密配置文件、元数据和密钥文件时，会按 `--set-digest-algorithm` 指定的算法计算明文摘要，写入加密文件头。`taosd` 和 `taosk` 读取文件时先解密再校验摘要，摘要不一致说明文件被篡改或损坏，此时拒绝加载该文件，并在日志中输出 `encrypted file digest mismatch` 及文件路径。不含摘要的加密文件读取时跳过校验；更新密钥（见 1.6 节）后会补写摘要。

#### 1.12.3 通过 taosExplorer 管理加密算法 {#encrypt-algorithm-explorer}

taosExplorer 提供 **加密算法** 管理页面，可在浏览器中查看全部算法，添加和删除自定义算法，无需编写 SQL。

**权限要求**

- 查看算法列表：`SYSINFO` 属性为 `1` 的用户。
- 添加、删除自定义算法：拥有 `SYSSEC` 角色的用户；未启用三权分立时为 `root`。

**查看算法**

1. 登录 taosExplorer，在左侧菜单中选择 **系统管理** > **安全** > **加密算法**。
2. 页面以列表展示全部算法，列包括 **ID**、**算法标识**、**名称**、**类型**、**来源**、**OpenSSL 名称**、**描述** 和 **引用数**。**引用数** 表示当前使用该算法的数据库数量。
3. 可以按 **类型**（对称加密、摘要、非对称加密）和 **来源**（内置、自定义）筛选，也可以在搜索框中按算法标识或名称搜索。
4. 点击算法所在行，右侧显示算法详情，包括引用该算法的数据库列表。

**添加自定义算法**

1. 先将实现自定义算法的 OpenSSL provider 动态库放到 [`encryptExtDir`](../12-operations-and-tooling/03-components/01-taosd.md#encryptextdir) 指定的路径，并在所有 dnode 上重启 `taosd` 使其加载，详见下文 1.12.5 节。
2. 在 **加密算法** 页面点击 **+ 添加算法**，填写表单：

    | 表单项 | 是否必填 | 说明 | 对应 SQL 子句 |
    |--------|----------|------|---------------|
    | 算法标识 | 是 | 全局唯一，建库时在 `ENCRYPT_ALGORITHM` 中使用 | `CREATE ENCRYPT_ALGR 'id'` |
    | 名称 | 是 | 算法名称 | `ALGR_NAME` |
    | 类型 | 是 | 对称加密（CBC 模式）、摘要或非对称加密 | `ALGR_TYPE` |
    | OpenSSL 名称 | 是 | provider 中注册的算法名 | `OSSL_ALGR_NAME` |
    | 描述 | 否 | 便于识别的说明 | `DESC` |

3. 点击 **检测**，taosExplorer 在各 dnode 上检查 provider 是否已加载、能否按 **OpenSSL 名称** 找到该算法，并对类型为对称加密的算法做一次加解密自检。全部 dnode 检测通过后，**提交** 按钮才可用。
4. 点击 **提交**，算法出现在列表中，**来源** 为 **自定义**，即可在建库时使用。

**删除自定义算法**

在列表中找到该算法，点击 **删除** 并确认。**引用数** 大于 0 时不能删除，页面会列出引用该算法的数据库，需先删除这些数据库。内置算法不显示 **删除** 按钮。

添加和删除操作会写入审计日志，操作名分别为 `createEncryptAlgr` 和 `dropEncryptAlgr`，见 [审计与合规](./07-audit-and-compliance.md)。

#### 1.12.4 通过 SQL 管理加密算法

查看内置与自定义算法：

```sql
SHOW ENCRYPT_ALGORITHMS;

-- 更完整字段见 information_schema.ins_encrypt_algorithms
```

示例输出：

```text
id | algorithm_id | name    | desc                     | type                        | source   | ossl_algr_name |
1  | SM4-CBC      | SM4     | SM4 symmetric encryption | Symmetric Ciphers CBC mode  | build-in | SM4-CBC:SM4    |
2  | AES-128-CBC  | AES     | AES symmetric encryption | Symmetric Ciphers CBC mode  | build-in | AES-128-CBC    |
3  | SM3          | SM3     | SM3 digests              | Digests                     | build-in | SM3            |
4  | SHA-256      | SHA-256 | SHA2 digests             | Digests                     | build-in | SHA-256        |
5  | SM2          | SM2     | SM2 Asymmetric Cipher    | Asymmetric Ciphers          | build-in | SM2            |
6  | RSA          | RSA     | RSA Asymmetric Cipher    | Asymmetric Ciphers          | build-in | RSA            |
7  | AES-256-CBC  | AES     | AES symmetric encryption | Symmetric Ciphers CBC mode  | build-in | AES-256-CBC    |
```

| 字段 | 说明 |
|------|------|
| `id` | 数字标识；内置从 1 起，自定义从 101 起 |
| `algorithm_id` | 全局唯一标识（建库时使用） |
| `name` / `desc` | 名称与描述 |
| `type` | 如 Symmetric Ciphers CBC mode、Asymmetric Ciphers、Digests |
| `source` | `built-in` / `customized`（界面也可能显示 `build-in`，以查询结果为准） |
| `ossl_algr_name` | OpenSSL / 自定义 provider 中的名称 |

只查看某一类算法：

```sql
SELECT
  algorithm_id,
  name,
  `desc`
FROM information_schema.ins_encrypt_algorithms
WHERE type = 'Digests';
```

添加自定义算法：

```sql
CREATE ENCRYPT_ALGR 'vigenere' ALGR_NAME 'vigenere' DESC 'my custom algr' ALGR_TYPE 'Symmetric_Ciphers_CBC_mode' OSSL_ALGR_NAME 'vigenere';
```

`ALGR_TYPE` 的取值为 `Symmetric_Ciphers_CBC_mode`、`Digests` 或 `Asymmetric_Ciphers`。目前只有对称类型的自定义算法可以用于 `ENCRYPT_ALGORITHM` 建库。

删除自定义算法（须先无引用，例如已 `DROP` 使用该算法的数据库）：

```sql
DROP ENCRYPT_ALGR 'vigenere';
```

内置算法不允许删除。

#### 1.12.5 接入自定义算法

自定义算法需按 OpenSSL provider 接口实现动态库（Linux 上为 `.so`，Windows 上为 `.dll`），由 `taosd` 启动时加载。完整流程如下：

1. 按 OpenSSL provider 接口实现算法，参考 [OpenSSL provider](https://docs.openssl.org/master/man7/provider/)、[OSSL_PROVIDER-default](https://docs.openssl.org/master/man7/OSSL_PROVIDER-default/)。同一个动态库可以包含多个算法，各算法通过 `OSSL_ALGR_NAME` 区分。
2. 将动态库复制到集群中所有 dnode 的同一路径下，并在每个 dnode 的 `taos.cfg` 中用 [`encryptExtDir`](../12-operations-and-tooling/03-components/01-taosd.md#encryptextdir) 指定该路径（当前仅支持加载单个文件）。
3. 逐个重启 `taosd`，使其加载动态库。
4. 通过 taosExplorer 或 `CREATE ENCRYPT_ALGR` 登记算法。
5. 用该算法建库，写入并查询少量数据，确认加解密正常。

:::warning
使用自定义算法加密的数据库，在所有 dnode 上都依赖该动态库。删除、替换动态库或修改 `encryptExtDir` 前，须先确认没有数据库引用该算法，否则 `taosd` 无法读取这些数据库的数据。
:::

### 1.13 创建加密数据库

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [database_options]

database_option: {
  ENCRYPT_ALGORITHM {'none' | 'SM4-CBC' | 'AES-128-CBC' | 'AES-256-CBC' | ...}
}
```

- `ENCRYPT_ALGORITHM`：默认 `none`。加密时须使用 `SHOW ENCRYPT_ALGORITHMS` 中类型为 Symmetric Ciphers CBC mode 的 `algorithm_id`。完整 DDL 见 [数据库](../05-tdengine-sql/02-ddl/01-database.md)。
- **创建后不可修改**加密算法。
- 创建加密库前须已通过 `taosk --encrypt-data` 生成 `DATA_KEY`。

```sql
CREATE DATABASE db1
  ENCRYPT_ALGORITHM 'SM4-CBC';

CREATE DATABASE db2
  ENCRYPT_ALGORITHM 'AES-128-CBC';

CREATE DATABASE db3
  ENCRYPT_ALGORITHM 'AES-256-CBC';

CREATE DATABASE db4;  -- 不加密
```

查看库级配置：

```sql
SELECT
  name,
  `encrypt_algorithm`
FROM information_schema.ins_databases;
```

展示值可能因版本显示为 `SM4-CBC`、`sm4` 等形式，以实际查询为准。

### 1.14 加密用户密码

用户口令默认先经摘要（如 MD5 / 与 SCRAM 相关的哈希流程）写入元数据。当集群已加载 **`DATA_KEY`**（`taosk --encrypt-data`，或兼容路径下等价的数据加密密钥）时，服务端在落盘前会对口令摘要再做 **SM4** 保护，并写入盐值；校验登录时在 `DATA_KEY` 可用且该用户口令已标记为加密存储的前提下再解密比对。这样可降低元数据文件泄露时直接还原口令材料的风险。

启用要点：

1. 先按上文完成密钥生成，确保 `DATA_KEY` 已存在并可被 `taosd` 加载（可用 `SHOW ENCRYPT_STATUS` / `ins_encrypt_status` 确认 `data` 范围为 `enabled`）。
2. 之后新建用户或修改密码时，新口令会按加密存储路径落盘；已存在、未加密存储的旧用户口令不会自动回填，需改密或重建用户后才会带上落盘加密。
3. `encryptPassAlgorithm`：`taos.cfg` / [taosd](../12-operations-and-tooling/03-components/01-taosd.md#encryptpassalgorithm) 仍可能列出该参数（`v3.3.7.0` 引入，取值如 `sm4`），属早期“口令落盘加密开关 + 单密钥”文档口径。`v3.4+` 新部署以是否已生成并加载 `DATA_KEY` 为准；勿再把它与 `taosk` 分级密钥理解成两套互不等价、必须同时配置的主流程。升级环境若仍依赖旧参数与 `CREATE ENCRYPT_KEY`，以实现与发行说明为准。

## 2. 安全删除 {#安全删除}

数据库选项 `SECURE_DELETE`（取值 `0` / `1`，默认 `0`）控制删除路径是否在写入删除标记之外，对落盘数据块做物理覆写。DDL 语法见 [数据库 · SECURE_DELETE](../05-tdengine-sql/02-ddl/01-database.md#secure_delete)；单次删除也可在语句末尾加 `SECURE_DELETE` 关键字，见 [数据删除](../05-tdengine-sql/03-data-write/02-delete.md)。表 / 超级表也可设置同名选项（与库级、语句级按位或合并生效）。

- **关闭（`0`）**：仅写入删除标记；查询不再返回已删数据，但对应文件块在后续压缩/回收前仍可能残留在磁盘上。
- **开启（`1`）**：在删除标记之外，对 DATA / STT 等落盘文件中命中 `(表, 时间区间)` 的数据块执行文件级覆写（secure erase），降低通过文件系统直接读取已删内容的风险。

行为要点：

- 生效条件为库级 `SECURE_DELETE=1`、表/超级表元数据中的安全删除标志，或语句级 `DELETE ... SECURE_DELETE` 三者之一（实现上按位或合并）。
- 物理覆写在删除标记写入之后执行；覆写失败会记服务端日志，查询语义仍以删除标记为准（已删数据不会因覆写失败而重新可见）。
- 当前实现面向新版 TSDB 文件格式；旧格式文件会跳过文件级覆写，依赖后续压缩等路径回收。
- 多副本场景下，文件级覆写在 Raft Leader 上执行；Follower 通过 WAL 重放逻辑删除，不会自动复现同一套物理覆写操作。
- WAL 中仍可能保留删除前的原始写入记录，直至检查点后的 WAL 裁剪；OS 页缓存、SSD 磨损均衡等也可能使物理介质上短期仍可见旧内容。本能力不是硬件级 Secure Erase / Sanitize，也不等同于“静态加密 + 销毁密钥”。
- 开启后删除路径 I/O 与耗时会增加，请按业务对残留数据清除的要求与性能开销权衡。
- 与 TDE 互补：TDE 降低静态文件被直接解读的风险；安全删除侧重删除后对残留块的覆写。
- 全局参数 `secureEraseMode`（默认 `0`）控制整块可直接覆写时的填充方式：`0` 为零填充，`1` 为随机字节；部分重叠块为保证就地写回始终零填充。详见 [taosd · secureEraseMode](../12-operations-and-tooling/03-components/01-taosd.md#secureerasemode)。

示例：

```sql
-- 库级
CREATE DATABASE db
  SECURE_DELETE 1;

ALTER DATABASE db SECURE_DELETE 1;

-- 超级表 / 表级（与库级、语句级按位或）
CREATE STABLE meters (
  ts      TIMESTAMP,
  current FLOAT,
  voltage INT
) TAGS (
  location VARCHAR(64)
)
  SECURE_DELETE 1;

ALTER STABLE meters SECURE_DELETE 1;

-- 语句级
DELETE FROM meters WHERE ts < '2021-10-01 10:40:00.100' SECURE_DELETE;
```

## 3. 相关查阅

| 主题 | 文档 |
|------|------|
| IP 白名单 / 黑名单 | [用户管理 · IP 白名单与黑名单](../05-tdengine-sql/07-user-and-privilege/01-user.md#ip-白名单与黑名单)；全链路说明见 [全链路认证 · IP 访问控制](./01-full-trace-auth.md#ip-访问控制) |
| 库 / 表 DDL | [数据库](../05-tdengine-sql/02-ddl/01-database.md)、[数据删除](../05-tdengine-sql/03-data-write/02-delete.md) |
| SHOW / 系统表 | [SHOW](../05-tdengine-sql/09-system-info/03-show.md)、[元数据表](../05-tdengine-sql/09-system-info/01-meta.md) |
| taosd 加密相关参数 | [taosd](../12-operations-and-tooling/03-components/01-taosd.md)（`encryptAlgorithm`、`encryptScope`、`encryptExtDir`、`encryptPassAlgorithm`、`secureEraseMode` 等） |
| 国密 TLS 传输 | [全链路传输安全 · 国密 TLS](./02-full-trace-transport.md#gm-tls) |
| 全链路中的落盘可靠 | [全链路高可靠 · TDE](./03-full-trace-reliability.md) |
| 加固清单 | [安全加固建议](./08-security-hardening.md) |
