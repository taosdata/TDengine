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
CREATE DATABASE secure_db ENCRYPT_ALGORITHM 'SM4-CBC';
SELECT * FROM information_schema.ins_encrypt_status;
SELECT name, `encrypt_algorithm` FROM information_schema.ins_databases;
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
| `--set-cfg-algorithm` | 配置文件加密算法：`sm4` 或 `aes`，默认 `sm4` |
| `--set-meta-algorithm` | 元数据加密算法：`sm4` 或 `aes`，默认 `sm4` |
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

### 1.12 加密算法管理

查看内置与自定义算法：

```sql
SHOW ENCRYPT_ALGORITHMS;
-- 更完整字段见 information_schema.ins_encrypt_algorithms
```

示例输出：

```text
id | algorithm_id | name | desc                        | type                        | source   | ossl_algr_name |
1  | SM4-CBC      | SM4  | SM4 symmetric encryption    | Symmetric Ciphers CBC mode  | build-in | SM4-CBC:SM4    |
2  | AES-128-CBC  | AES  | AES symmetric encryption    | Symmetric Ciphers CBC mode  | build-in | AES-128-CBC    |
```

| 字段 | 说明 |
|------|------|
| `id` | 数字标识；内置从 1 起，自定义从 101 起 |
| `algorithm_id` | 全局唯一标识（建库时使用） |
| `name` / `desc` | 名称与描述 |
| `type` | 如 Symmetric Ciphers CBC mode、Asymmetric Ciphers、Digests |
| `source` | `built-in` / `customized`（界面也可能显示 `build-in`，以查询结果为准） |
| `ossl_algr_name` | OpenSSL / 自定义 provider 中的名称 |

添加自定义算法：

```sql
CREATE ENCRYPT_ALGR 'vigenere' ALGR_NAME 'vigenere' DESC 'my custom algr'
  ALGR_TYPE 'Symmetric_Ciphers_CBC_mode' OSSL_ALGR_NAME 'vigenere';
```

自定义算法需按 OpenSSL provider 接口实现 so，由 `taosd` 启动时加载；`encryptExtDir` 指定 so 路径（当前仅支持加载单个文件）。同一 so 可包含多个算法，通过 `OSSL_ALGR_NAME` 对应。参考 [OpenSSL provider](https://docs.openssl.org/master/man7/provider/)、[OSSL_PROVIDER-default](https://docs.openssl.org/master/man7/OSSL_PROVIDER-default/)。

删除自定义算法（须先无引用，例如已 `DROP` 使用该算法的数据库）：

```sql
DROP ENCRYPT_ALGR 'vigenere';
```

内置算法不允许删除。

### 1.13 创建加密数据库

```sql
CREATE DATABASE [IF NOT EXISTS] db_name [database_options]

database_option: {
  ENCRYPT_ALGORITHM {'none' | 'SM4-CBC' | 'AES-128-CBC' | ...}
}
```

- `ENCRYPT_ALGORITHM`：默认 `none`。加密时须使用 `SHOW ENCRYPT_ALGORITHMS` 中类型为 Symmetric Ciphers CBC mode 的 `algorithm_id`。完整 DDL 见 [数据库](../05-tdengine-sql/02-ddl/01-database.md)。
- **创建后不可修改**加密算法。
- 创建加密库前须已通过 `taosk --encrypt-data` 生成 `DATA_KEY`。

```sql
CREATE DATABASE db1 ENCRYPT_ALGORITHM 'SM4-CBC';
CREATE DATABASE db2 ENCRYPT_ALGORITHM 'AES-128-CBC';
CREATE DATABASE db3;   -- 不加密
```

查看库级配置：

```sql
SELECT name, `encrypt_algorithm` FROM information_schema.ins_databases;
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
CREATE DATABASE db SECURE_DELETE 1;
ALTER DATABASE db SECURE_DELETE 1;

-- 超级表 / 表级（与库级、语句级按位或）
CREATE STABLE meters (
  ts TIMESTAMP, current FLOAT, voltage INT
) TAGS (location VARCHAR(64)) SECURE_DELETE 1;
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
| 全链路中的落盘可靠 | [全链路高可靠 · TDE](./03-full-trace-reliability.md) |
| 加固清单 | [安全加固建议](./08-security-hardening.md) |
