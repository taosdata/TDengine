# 存储安全模块-Function Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-10-15 | 2025-11-19 | 1.0 | 鲍之骁 | 新建 |
| 2025-12-08 | 2025-12-15 | 1.1 | 程洪泽 | 修订及完善内容 |

## 2. 背景

在数据安全日益重要的背景下，尤其是在金融、政府和医疗等行业，静态数据（Encryption at Rest）的保护已成为合规要求的核心。TDengine 作为时序数据库，存储大量敏感时序数据、配置和元数据，面临潜在泄露风险。该特性源于 JIRA TS-7230 的需求，旨在通过引入透明数据加密和密钥管理机制，实现重要数据的加密存储，支持国密算法，从而提升系统的机密性和完整性。目标是让用户无需手动干预即可自动加密文件，同时提供密钥生命周期管理，确保数据在存储层的安全，同时最小化性能影响。
**JIRA**：[TS-7230](https://jira.taosdata.com:18080/browse/TS-7230)

## 3. 定义

### 3.1 密钥体系

| 密钥名称 | 缩写 | 用途 | 特性 | 生成方式 | 存储方式 | 加密范围 |
| --- | --- | --- | --- | --- | --- | --- |
| 服务器主密钥 | SVR_KEY | 用于加密数据库主密钥和系统级信息 | 与机器硬件绑定，防止跨机移植 | 用户指定或系统自动生成 | 使用机器码加密后存储在encrypt.bin中 | 系统级信息加密 |
| 数据库主密钥 | DB_KEY | 用于加密数据库密钥 | 支持动态修改，用于密钥轮换 | 从SVR_KEY派生或用户指定 | 使用SVR_KEY加密后存储 | 数据库密钥保护 |
| 配置加密密钥 | CFG_KEY | 专用于加密配置文件和非数据文件 | 一旦生成不可更改，确保配置文件完整性 | 从SVR_KEY派生 | 使用SVR_KEY加密后存储 | dnode、mnode、vnode、snode的配置文件 |
| 元数据加密密钥 | META_KEY | 用于加密元数据文件，如权限、用户、密码相关信息 | 一旦生成不可更改 | 从SVR_KEY派生 | 使用SVR_KEY加密后存储 | mnode的SDB、WAL文件和snode的checkpoint文件 |
| 时序数据加密密钥 | DATA_KEY | 用于加密时序数据文件和相关日志 | 一旦生成不可更改，支持历史版本兼容 | 从SVR_KEY派生或用户指定（兼容历史版本） | 使用SVR_KEY加密后存储 | TSDB、WAL、STT、TDB、BSE等数据文件 |

### 3.2 核心概念

| 概念名称 | 英文名称 | 定义 | 特点 | 优势 | 应用场景 |
| --- | --- | --- | --- | --- | --- |
| 透明加密 | Transparent Data Encryption (TDE) | 数据库系统在存储层自动处理的加解密过程 | 对应用程序完全透明，无需修改业务代码 | 简化安全管理，降低应用开发复杂度 | 数据文件、配置文件、元数据文件的自动加密 |
| 国密算法 | Chinese National Cryptographic Algorithms | 中国国家密码管理局批准的密码算法体系 | 符合国家密码标准，支持国产化需求 | 满足国内安全合规要求 | 金融、政府、医疗等敏感行业的数据加密 |
| 密钥派生函数 | Key Derivation Function (KDF) | 从主密钥派生出其他密钥的密码学函数 | 使用盐值和用途标识确保密钥唯一性 | 实现密钥分级管理，增强安全性 | 从SVR_KEY派生CFG_KEY、META_KEY、DATA_KEY |

### 3.3 系统组件

| 组件名称 | 类型 | 功能描述 | 接口形式 | 权限要求 | 关键特性 |
| --- | --- | --- | --- | --- | --- |
| taosk | 命令行工具 | TDengine 安全密钥工具，用于密钥生成、管理、备份、恢复、查看 | 命令行接口，支持批量操作 | 需要管理员权限执行敏感操作 | 支持国密算法、密钥生命周期管理、备份恢复功能 |
| encrypt.bin | 密钥文件 | 存储加密密钥的文件，包含所有加密密钥的加密形式 | 二进制文件格式 | 文件系统权限控制 | 使用机器码加密防止跨节点复制，支持备份导出 |
| 机器码 | 硬件标识 | 基于 CPU ID、主板序列号、MAC 地址等硬件信息生成的唯一标识 | 系统调用获取 | 需要硬件访问权限 | 确保唯一性和不可伪造性，用于密钥绑定 |
| 文件加密标识 | 文件头部 | 加密文件开头的明文标识，用于快速识别文件加密状态 | 固定格式：魔法数字+算法标识+版本号 | 无特殊权限要求 | 支持快速检测、算法升级和版本兼容 |

## 4. 密钥生成与管理

本特性引入了密钥管理和加密机制，影响配置、部署和 SQL 操作。加密过程透明，用户通过 taosk 工具和 SQL 交互。以下详细说明变化。

### 4.1 密钥生成与管理

本章节介绍如何使用taosk程序生成密钥。
**示例：**
```plaintext {wrap}
#生成密钥
taosk 
    -c /etc/taos 
    --set-algorithm sm4 
    --encrypt-server svr_key
    --encrypt-database db_key 
    --encrypt-config 
    --encrypt-metadata 
    --encrypt-data data_key
```

**参数说明：**
1. -c: 指定配置文件路径，默认 /etc/taos。
2. --set-algorithm: 设置加密算法（sm4/sm3/sm2），默认 sm4。
3. encrypt-server: 设置 SVR_KEY。
4. Encrypt-database: 设置 DB_KEY。
5. --encrypt-config/metadata/data: 分别启用配置/元数据/时序数据加密，默认关闭,其中 data_key 可以由用户指定，方便兼容历史版本中已经加密的数据库。
6. 如果不设置SVR_KEY/DB_KEY，使用默认规则安全生成。
密钥存储在 encrypt.bin。

### 4.2 密钥变更与查看

为避免重复加密数据文件带来的巨额性能损耗，密钥仅支持修改 svr_key 以及 db_key。
**通过命令更改：**
```plaintext {wrap}
taosk --update-svrkey new_svr_key --update-dbkey new_db_key
```

**通过SQL (需要管理员权限）：**
```sql {wrap}
ALTER SYSTEM SET SVR_KEY 'new_svr_key'; 
ALTER SYSTEM SET DB_KEY 'new_db_key';
```

**查看：**
```plaintext {wrap}
SHOW SYSTEM KEYS;
  name      |      value           |     last modified
=============================================================
 SVR_KEY    | "this is a password" | 2025-10-16 16:41:32.001
 DB_KEY     | "this is a password" | 2025-10-14 16:41:32.002
 CFG_KEY    | "this is a password" | 2025-08-16 16:41:32.001
 META_KEY   | "this is a password" | 2025-07-16 16:41:32.001
 DATA_KEY   | "this is a password" | 2025-02-16 16:41:32.005

```

### 4.3 密钥到期设置

**通过**** ****SQL设置密钥到期策略：**
```plaintext {wrap}
ALTER SYSTEM SET KEY_EXPIRATION 90 DAYS STRATEGY 'ALARM';
```

**策略：**
1. 'ALARM'（发送告警到监控接口，写入TDinsight）
2. 'QUERY'（停止数据查询服务，暂不实现）
3. 'INSERT'（停止数据写入服务，暂不实现）
**参数说明：**
1. KEY_EXPIRATION：设置密钥到期时间
2. STRATEGY：设置密钥到期后策略，默认为告警。

### 4.4 密钥分发和备份

#### 4.4.1 密钥分发

通过安全可靠的内部加密通信机制（SASL），由 mnode 分发至其他经过认证的 dnode 节点。

#### 4.4.2 密钥备份

输入原节点机器码、SVR_KEY，生成不包含机器码加密信息的 encrypt.bin 文件。
**示例：**
```plaintext {wrap}
taosk --backup --machine-code orig_code --svr-key svr_key
```

#### 4.4.3 密钥恢复

通过 taosk 程序，基于备份的密钥文件，当输入目标节点机器码、SVR_KEY 时，为该节点生成可用的秘钥文件。
**示例：**
```plaintext
taosk --restore --machine-code target_code --svr-key old_key
```

## 5. 透明加密

在透明加密中，加解密过程由数据库管理系统自动完成，用户不可见。

### 5.1 配置文件加密

#### 5.1.1 配置文件透明加密

集群启动时，密钥文件存在（dnode/config/encrypt.bin）且包含 CFG_KEY 时，对 dnode、mnode、vnode、snode 文件夹中的所有非数据文件进行加密，具体指：
1. dnode：包括 dnode.info  dnode.json
2. mnode：包括 mnode.json、raft_config.json、raft_store.json
3. vnode：包括 vnodes.json、vnode.json、raft_config.json、raft_store.json 等所有非数据文件
4. snode：包括 snode.json

#### 5.1.2 配置文件行为变更

**简化 taos.cfg 文件内容：**
taos.cfg 仅保留必要的配置参数，例如 firstEp、secondEp、fqdn、serverPort、debugFlag、dataDir。
**防止配置文件被随意篡改：**
taos.cfg 中的配置仅在系统初次启动时有效，后续修改 taos.cfg 文件中的配置参数不会影响数据库行为，所有配置参数修改都需要拥有管理员权限的用户通过 SQL 修改。
**示例：**
```plaintext
ALTER DNODE 1 'debugFlag' '143';
```

### 5.2 元数据文件加密

集群启动时，密钥文件存在（dnode/config/encrypt.bin）且包含 META_KEY 时，对 snode、mnode 中的所有数据文件进行加密，具体包括：
1. mnode： SDB 和 WAL 文件
2. snode：checkpoint 等文件

### 5.3 数据文件加密

集群启动时，密钥文件存在（dnode/config/encrypt.bin）且包含 DATA_KEY 后对数据文件进行加密，具体规则如下：
1. 创建数据库时，通过指定 ENCRYPT_ALGORITHM 选项开启，不支持修改
2. 加密范围包括 TSDB、WAL、STT、TDB、BSE 等文件

## 6. 加密状态查看

### 6.1 查看当前系统加密状态

增加系统表 ins_encrypt_status，各列定义如下
1. 加密范围：元数据加密、配置文件加密、时序数据文件加密
2. 加密算法：名称
3. 加密状态：开启、关闭
**示例：**
```sql
SELECT * FROM information_schema.ins_encrypt_status;
 encrypt_scope  | algorithm |  status
=========================================
 config         |    sm2    | enabled
 metadata       |    sm3    | disabled
 data           |    sm4    | enabled

```

### 6.2 查看特定数据库的加密状态

系统表 ins_databases 的 "encrypt_algorithm" 可查看各个数据库的加密算法配置。
```plaintext
select name, encrypt_algorithm from ins_databases;
              name              | encrypt_algorithm |
=====================================================
 power1                         | none              |
 power                          | sm4               |
```

## 7. taosd 行为

当前章节用于描述 taosd 在初始化或升级时涉及存储安全的行为。

### 7.1 新增标志位定义

为防止重复加密并提升状态管理效率，引入以下标志位。这些标志位存储在 dnode 和 database 配置中，支持持久化存储和实时查询。

#### 7.1.1 db_encrypt_status（数据库加密状态）

- **0：未知** - 表示数据库加密状态不确定，通常出现在升级场景中。
- **1：未加密** - 数据库文件处于明文状态，可安全执行加密操作。
- **2：加密完成** - 数据库文件已全部加密，不可重复执行加密操作，以避免性能开销和数据一致性风险。

#### 7.1.2 dnode_encrypt_status（数据节点加密状态）

- **0：未加密** - dnode 下的配置文件和元数据文件处于明文状态。
- **1：加密完成** - dnode 下的所有相关文件已加密完成。

#### 7.1.3 文件加密标识

此外，为精确标识单个文件是否已加密，在加密文件开头添加固定明文头部（魔法数字序列"tdEncrypt"），后跟算法标识（如 SM4 的整数编码）和版本号。该明文头部不包含敏感信息，仅用于快速检测文件类型和加密状态，支持兼容性扩展（如多算法切换）。
此外加密过程中，采用原子文件替换策略：将加密内容写入临时文件，完成后原子重命名替换原文件，并安全删除旧文件残留。这确保操作的原子性，防止中断导致的数据损坏。

### 7.2 集群初始化

集群初始化过程从密钥配置开始，确保所有节点安全启动并启用透明加密。以下为完整流程：
1. **配置主密钥**
   - 在主节点执行 taosk 密钥管理工具，指定 SVR_KEY（服务器主密钥）和 DB_KEY（数据库主密钥）。
   - 工具基于主密钥自动派生 CFG_KEY（配置文件加密密钥）、META_KEY（元数据加密密钥）和 DATA_KEY（时序数据加密密钥）。
   - 生成的密钥文件以加密形式保存在每个节点的 dnode/config/encrypt.bin 中，使用机器码绑定防止跨节点泄露。
2. **自动分发密钥**
   - mnode 服务启动后，通过 SASL 加密通信通道将密钥安全分发至所有 dnode 节点。
   - 每个 dnode 接收密钥后，使用本地机器码对密钥进行二次加密存储，防止密钥被随意移植。
3. **创建加密的配置文件**
   - 使用默认加密算法（如 SM4）对 dnode、mnode、vnode 和 snode 中的配置文件（如 dnode.json、mnode.json、raft_config.json）进行加密。
   - 在每个加密文件开头插入明文头部标识，用于后续快速校验。
   - 加密完成后，统一设置所有 dnode 的 `dnode_encrypt_status` = 1（加密完成）。
4. **创建数据库**
   - 创建数据库时，可通过 ENCRYPT_ALGORITHM 参数指定加密算法（如 SM4）；若未指定，则不进行加密。
   - 新数据库的文件（如 TSDB、WAL、STT）在创建时立即加密，并在文件开头添加明文头部标识。
   - 数据库加密完成后，立即标记 `db_encrypt_status`= 2（加密完成）。
   - 对于非加密数据库，默认标记 `db_encrypt_status` = 1（未加密），以便后续按需启用。

### 7.3 对现有非加密集群进行加密处理

针对运行中的非加密集群，提供集群停止后的加密服务。设计的主要目的是为了尽可能的避免重复的加密操作。
1. **停止当前集群的服务**
2. **配置主密钥**
   - 使用 taosk 工具在主节点生成密钥文件，保存至 dnode/config/encrypt.bin。
   - 通过 mnode 分发密钥至所有 dnode，确保每个节点本地加密存储。
3. **重启当前集群的服务，并对未加密文件加密**
   - 服务重启后，taosd 自动扫描并加密未处理文件。
   - **加密配置文件**
      - 先检查 `dnode_encrypt_status`：若为 0（未加密）且指定了加密算法，则启动加密流程；反之，则跳过加密流程以防重复加密。
      - 遍历 dnode、mnode、vnode 和 snode 中的配置文件（如 dnode.info、mnode.json、raft_config.json）。
      - 对于每个文件，读取开头字节检查明文头部：若已存在标识，则跳过；否则，创建临时文件（e.g., filename.tmp），使用 CFG_KEY 加密内容并插入明文头部，完成后原子重命名替换原文件，并安全删除旧文件残留。
      - 所有文件加密完成后，设置 `dnode_encrypt_status` = 1（加密完成）。
   - **加密元数据文件与数据文件**
      - 先检查数据库的 db_encrypt_status：若为 1（未加密），则启动加密；若为 2（加密完成），则跳过。
      - 对于元数据文件（如 mnode 的 SDB 和 WAL、snode 的 checkpoint）：创建临时文件，使用 META_KEY 分块加密），并在临时文件开头添加明文头部，完成后原子替换原文件，并删除旧文件。
      - 对于数据文件（如 TSDB、WAL、STT）：针对每个数据库，创建临时文件，使用 DATA_KEY 进行背景加密，完成后原子替换，并删除旧文件。
      - 完成后更新 `db_encrypt_status` 为  2（加密完成）。

### 7.4 历史版本升级

针对从旧版本升级的集群，提供兼容方法，主要处理原有密钥机制的兼容性。
1. **密钥兼容配置**
   - 使用 taosk 工具指定 DATA_KEY 为历史版本的密钥（旧单密钥），以桥接新分级密钥结构。
   - 自动派生其他密钥（CFG_KEY、META_KEY），并更新 encrypt.bin 文件。
2. **已加密数据库添加加密标识**
   - taosd 扫描数据库配置：若 `encrypt_algorithm` 已设置为加密（如 'sm4'），则进入迁移模式。
   - 对于每个已经加密过的数据文件，为其添加明文头部。
   - 迁移过程中设置 `db_encrypt_status` = 0（未知），完成后更新为 2（加密完成）。
   - 若文件已部分兼容（旧加密无头部），则创建临时文件，仅添加头部而不重复加密，完成后原子替换，并删除旧文件，以优化性能。
   - 完成后统一标记 `db_encrypt_status` = 2（加密完成）。
3. **未加密数据库添加未加密标识**
   - taosd 扫描数据库配置：若 `encrypt_algorithm` 已设置为 none。
   - 设置 `db_encrypt_status` = 1(未加密)。

## 8. 性能

存储加密在保证数据文件安全性的同时，也会带来一定的性能影响，不同的加密算法对性能的影响各有不同，用户需要根据自己的需求来决定是否进行加密以及加密算法的选择。在测试过程中，对如下指标进行测试，如果性能不达预期，需优化代码。
1. **启动性能**
   - 加密实现不应显著影响 TDengine 启动性能。
2. **写入性能**：
   - 加密实现影响数据写入性能，写入延迟增加不得超过 100%。
3. **查询性能**：
   - 加密实现影响数据查询性能，查询延迟增加不得超过 100%。
4. **资源消耗**：
   - 加密功能的内存占用需可控。

## 9. 安全

存储安全的设计采用分级密钥和机器绑定，确保存储文件的安全性，其中主要包含以下内容：
1. SVR_KEY用机器码加密，防止密钥移植。
2. 密钥分发通过SASL，避免明文传输。
3. 设置密钥到期策略，防止密钥过期风险。
4. 支持国密算法符合本土安全标准。

## 10. 兼容性

1. 未启用存储安全的历史版本可以正确的升级到启用存储安全的版本。
2. 支持存储安全但是没有开启存储安全功能的集群，可以正确回退到历史版本。
3. 支持存储安全并且开启存储安全功能的集群，不可以回退到不支持存储安全功能的历史版本。

## 11. 运维

无。

## 12. 使用场景

TDengine 存储加密适用于合规存储、敏感配置保护、权限数据安全、隐私保护、灾备恢复、密钥管理及高安全集群。

## 13. 约束和限制

1. 对于无法获取机器码的环境，比如虚拟化的环境，无法使用存储安全功能。
2. 加密算法选择受限于支持的国密算法（SM2、SM3、SM4）。
3. 密钥一旦生成（CFG_KEY、META_KEY、DATA_KEY）不可更改。
4. 已加密的数据库不支持修改加密算法。

## 14. 常见错误和排查

| 错误提示 | 排查方法 |
| --- | --- |
| Invalid machine code | 错误的机器码，确认encrypt.bin是否从直接从其他机器拷贝，使用taosk --restore恢复。 |
| get machine code failed | 获取机器码失败；存储安全功能和机器码密切相关，为了防止随意拷贝密钥，对于无法获取机器码的环境，不允许使用存储安全功能。 |
| Decryption failed | 集群启动时，发现密钥不匹配，确认密钥是否正确。 |
| Key expiration | 密钥已经过期，请及时更新密钥。 |

## 15. 可观测性

TDinsight：配置密钥到期告警策略，监控接口推送通知。

## 16. 安装和卸载

无。

## 17. 文档

需要修改企业版文档和社区版文档增加存储安全相关内容。

## 18. 参考文档

1. [存储安全 RS](https://taosdata.feishu.cn/wiki/UYAqwU3GqiBsjCkT6BccKqLmnGh)
2. [数据加密-Functional Spec](https://taosdata.feishu.cn/wiki/UUmIweZ5AiZyVMkgreQcvwZ4nFf)

## 19. 附录
