# 存储安全模块-Design Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-12-23 | 2025-12-25 | 1.0 | 程洪泽 | 新建 |

## 2. 引言

### 2.1 目的

本文档旨在详细描述 TDengine 数据库系统中存储安全模块的设计与实现。该模块负责为配置文件、元数据文件和数据文件提供透明的加密/解密功能，确保敏感数据在存储时的安全性。

### 2.2 范围

本文档涵盖以下内容：
- 存储安全模块的整体架构设计
- 加密文件格式和数据结构
- 支持的加密算法
- 原子文件操作机制
- 密钥管理和状态机
- 配置文件的透明加密/解密
- 模块接口和调用关系

### 2.3 受众

- TDengine 开发人员
- 系统架构师
- 安全审计人员
- 技术支持工程师

## 3. 术语

| 术语 | 定义 |
| --- | --- |
| CFG_KEY | 配置文件加密密钥，用于加密配置文件 |
| DB_KEY | 数据库主密钥，用于加密数据库文件 |
| META_KEY | 元数据加密密钥，用于加密元数据文件 |
| DATA_KEY | 数据加密密钥，用于加密数据文件 |
| 原子文件操作 | 通过临时文件和重命名确保文件操作的原子性 |
| CBC 模式 | 密码块链接模式，一种对称加密操作模式 |
| SM4 | 国密 SM4 对称加密算法 |
| AES | 高级加密标准 |

## 4. 概述

### 4.1 架构

存储安全模块采用分层架构设计：
<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"graph TB\n    subgraph \"应用层\"\n        A1[配置文件读写]\n        A2[数据文件读写]\n        A3[元数据文件读写]\n    end\n    \n    subgraph \"接口层\"\n        B1[taosWriteCfgFile]\n        B2[taosReadCfgFile]\n        B3[taosWriteEncryptFileHeader]\n        B4[taosReadEncryptFileHeader]\n    end\n    \n    subgraph \"核心层\"\n        C1[加密/解密引擎]\n        C2[文件操作管理]\n        C3[密钥管理]\n        C4[原子操作控制]\n    end\n    \n    subgraph \"算法层\"\n        D1[SM4-CBC]\n        D2[AES-128-CBC]\n        D3[内置加密库]\n    end\n    \n    subgraph \"存储层\"\n        E1[加密文件格式]\n        E2[文件系统]\n    end\n    \n    A1 --\u003e B1\n    A2 --\u003e B3\n    A3 --\u003e B3\n    B1 --\u003e C1\n    B2 --\u003e C1\n    B3 --\u003e C2\n    B4 --\u003e C2\n    C1 --\u003e D1\n    C1 --\u003e D2\n    C1 --\u003e D3\n    C2 --\u003e E1\n    C3 --\u003e E1\n    E1 --\u003e E2\n","theme":"default","view":"chart"}"/>

### 4.2 技术

- **编程语言**: C
- **加密算法**: SM4-CBC, AES-128-CBC
- **文件格式**: 自定义加密文件头 + 加密数据
- **原子操作**: 临时文件 + 重命名机制
- **密钥管理**: 多级密钥体系（SVR_KEY, DB_KEY, CFG_KEY, META_KEY, DATA_KEY）

### 4.3 依赖项

- `crypt.h`: 加密算法接口
- `os.h`: 操作系统抽象层
- `tdef.h`: 类型和常量定义
- `tglobal.h`: 全局配置和状态

## 5. 设计考虑

### 5.1 假设和限制

1. **向后兼容性**: 支持未加密文件的透明升级
2. **性能影响**: 加密/解密操作对 I/O 性能有可接受的影响
3. **密钥安全**: 密钥在内存中以明文形式存在，依赖操作系统安全机制
4. **文件大小**: 加密后文件大小会增加（16字节对齐的填充）

### 5.2 设计模式和原则

1. **透明加密**: 上层应用无需关心文件是否加密
2. **原子操作**: 所有文件操作保证原子性，避免部分写入
3. **错误恢复**: 操作失败时自动清理临时文件
4. **状态检查**: 提供快速的文件加密状态检查接口

### 5.3 风险和缓解措施

| 风险 | 影响 | 缓解措施 |
| --- | --- | --- |
| 密钥泄露 | 数据完全暴露 | 定期轮换密钥，使用硬件安全模块 |
| 加密算法漏洞 | 数据可能被解密 | 支持算法升级，使用标准加密算法 |
| 性能瓶颈 | 系统吞吐量下降 | 优化加密实现，支持硬件加速 |
| 文件损坏 | 数据丢失 | 原子操作保证完整性，定期备份 |

## 6. 详细设计

### 6.1 组件设计

#### 6.1.1 文件加密头组件

```c
typedef struct {
    char    magic[TD_ENCRYPT_MAGIC_LEN];  // Magic number "tdEncrypt"
    int32_t algorithm;                    // 加密算法标识
    int32_t version;                      // 文件格式版本
    int32_t dataLen;                      // 加密数据长度
    char    reserved[32];                 // 保留字段
} STdEncryptFileHeader;
```

**功能**:
- 标识加密文件
- 记录加密算法和版本
- 存储加密数据长度
- 提供扩展空间

#### 6.1.2 配置文件加密组件

**核心函数**:
- `taosWriteCfgFile()`: 写入配置文件（自动加密）
- `taosReadCfgFile()`: 读取配置文件（自动解密）
- `taosEncryptExistingCfgFiles()`: 加密现有配置文件
**特性**:
- 根据 `tsCfgKey` 是否为空决定是否加密
- 支持透明升级（未加密 -> 加密）
- 原子文件操作保证数据一致性

#### 6.1.3 通用文件加密组件

**核心函数**:
- `taosWriteEncryptFileHeader()`: 写入加密文件头
- `taosReadEncryptFileHeader()`: 读取加密文件头
- `taosIsEncryptedFile()`: 快速检查文件是否加密

#### 6.1.4 密钥管理组件

**状态管理**:
```c
enum {
    ENCRYPT_KEY_STAT_UNKNOWN = 0,
    ENCRYPT_KEY_STAT_UNSET,
    ENCRYPT_KEY_STAT_SET,
    ENCRYPT_KEY_STAT_LOADED,
    ENCRYPT_KEY_STAT_NOT_EXIST
};
```

**等待机制**:
- `taosWaitCfgKeyLoaded()`: 等待密钥加载（30秒超时）
- 轮询检查密钥状态（100ms间隔）

### 6.2 关键数据结构

#### 6.2.1 加密选项结构

```c
typedef struct SCryptOpts {
    int32_t len;                    // 数据长度
    char*   source;                 // 源数据
    char*   result;                 // 结果数据
    int32_t unitLen;                // 加密单元长度（16字节）
    char    key[ENCRYPT_KEY_LEN + 1]; // 加密密钥
    const char* pOsslAlgrName;      // 算法名称
} SCryptOpts;
```

#### 6.2.2 加密数据描述结构

```c
typedef struct SEncryptData {
    char encryptAlgrName[TSDB_ENCRYPT_ALGR_NAME_LEN]; // 算法名称
    char encryptKey[ENCRYPT_KEY_LEN + 1];             // 加密密钥
} SEncryptData;
```

#### 6.2.3 加密状态枚举

```c
// 数据库加密状态
typedef enum {
    TD_DB_ENCRYPT_STATUS_UNKNOWN = 0,   // 未知状态
    TD_DB_ENCRYPT_STATUS_PLAIN = 1,     // 未加密
    TD_DB_ENCRYPT_STATUS_ENCRYPTED = 2  // 已加密
} ETdDbEncryptStatus;

// DNode 加密状态
typedef enum {
    TD_DNODE_ENCRYPT_STATUS_PLAIN = 0,     // 未加密
    TD_DNODE_ENCRYPT_STATUS_ENCRYPTED = 1  // 已加密
} ETdDnodeEncryptStatus;
```

### 6.3 数据库设计（如适用）

#### 6.3.1 数据模型

存储安全模块不直接管理数据库表，但影响以下文件的存储格式：
1. **配置文件**: `dnode.json`, `mnode.json`, `vnode.json`, `snode.json`
2. **Raft 状态文件**: `raft_config.json`, `raft_store.json`
3. **元数据文件**: `current.json`, `vnodes.json`
4. **数据文件**: TSDB 数据文件（通过 `SEncryptData` 结构传递加密参数）

#### 6.3.2 数据访问层

模块通过以下方式与文件系统交互：
1. **原子写入**:
  ```plaintext
   - 创建临时文件: filepath.tmp
   - 写入数据（加密或明文）
   - 同步到磁盘 (fsync)
   - 重命名为目标文件
   - 删除旧文件（如果存在）
  ```

1. **透明读取**:
  ```plaintext
   - 检查文件头 magic number
   - 如果加密: 读取头 -> 读取加密数据 -> 解密
   - 如果明文: 直接读取
  ```

### 6.4 图表解释

#### 6.4.1 数据流图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"flowchart TD\n    Start[开始文件操作] --\u003e CheckKey{密钥已加载?}\n    CheckKey --\u003e|是| CheckEncrypt{文件需加密?}\n    CheckKey --\u003e|否| WaitKey[等待密钥加载]\n    WaitKey --\u003e Timeout{超时?}\n    Timeout --\u003e|是| Error[返回超时错误]\n    Timeout --\u003e|否| WaitKey\n    \n    CheckEncrypt --\u003e|是| Encrypt[加密数据]\n    CheckEncrypt --\u003e|否| WritePlain[写入明文]\n    \n    Encrypt --\u003e CreateTemp[创建临时文件]\n    WritePlain --\u003e CreateTemp\n    \n    CreateTemp --\u003e WriteHeader[写入文件头]\n    WriteHeader --\u003e WriteData[写入数据]\n    WriteData --\u003e Fsync[同步到磁盘]\n    Fsync --\u003e Rename[重命名为目标文件]\n    Rename --\u003e Success[操作成功]\n    \n    Error --\u003e End[结束]\n    Success --\u003e End\n","theme":"default","view":"chart"}"/>

#### 6.4.2 消息序列图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"sequenceDiagram\n    participant App as 应用程序\n    participant TEncrypt as 存储安全模块\n    participant Crypt as 加密引擎\n    participant FS as 文件系统\n    \n    App-\u003e\u003eTEncrypt: taosWriteCfgFile(filepath, data, len)\n    TEncrypt-\u003e\u003eTEncrypt: 检查 tsCfgKey\n    alt tsCfgKey 为空\n        TEncrypt-\u003e\u003eFS: 创建临时文件\n        TEncrypt-\u003e\u003eFS: 写入明文数据\n    else tsCfgKey 非空\n        TEncrypt-\u003e\u003eCrypt: 加密数据 (SM4-CBC)\n        Crypt--\u003e\u003eTEncrypt: 返回加密数据\n        TEncrypt-\u003e\u003eFS: 创建临时文件\n        TEncrypt-\u003e\u003eFS: 写入加密文件头\n        TEncrypt-\u003e\u003eFS: 写入加密数据\n    end\n    TEncrypt-\u003e\u003eFS: fsync 同步\n    TEncrypt-\u003e\u003eFS: 重命名为目标文件\n    TEncrypt--\u003e\u003eApp: 返回成功\n","theme":"default","view":"chart"}"/>

#### 6.4.3 流程图：配置文件读取

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"flowchart TD\n    Start[taosReadCfgFile] --\u003e CheckExist{文件存在?}\n    CheckExist --\u003e|否| ReturnError[返回文件不存在错误]\n    CheckExist --\u003e|是| CheckEncrypted{文件已加密?}\n    \n    CheckEncrypted --\u003e|否| ReadPlain[读取明文文件]\n    ReadPlain --\u003e AllocBuf[分配缓冲区]\n    AllocBuf --\u003e ReadData[读取文件内容]\n    ReadData --\u003e NullTerm[添加空终止符]\n    NullTerm --\u003e ReturnData[返回数据]\n    \n    CheckEncrypted --\u003e|是| ReadHeader[读取加密文件头]\n    ReadHeader --\u003e VerifyMagic{Magic 正确?}\n    VerifyMagic --\u003e|否| ReturnCorrupt[返回文件损坏错误]\n    VerifyMagic --\u003e|是| CheckKey{CFG_KEY 可用?}\n    CheckKey --\u003e|否| ReturnNoKey[返回无密钥错误]\n    CheckKey --\u003e|是| ReadEncrypted[读取加密数据]\n    ReadEncrypted --\u003e Decrypt[解密数据 SM4-CBC]\n    Decrypt --\u003e ReturnData\n    \n    ReturnError --\u003e End[结束]\n    ReturnCorrupt --\u003e End\n    ReturnNoKey --\u003e End\n    ReturnData --\u003e End\n","theme":"default","view":"chart"}"/>

#### 6.4.4 状态转换图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"stateDiagram-v2\n    [*] --\u003e UNKNOWN: 初始状态\n    UNKNOWN --\u003e PLAIN: 检测到未加密文件\n    UNKNOWN --\u003e ENCRYPTED: 检测到加密文件\n    PLAIN --\u003e ENCRYPTED: 执行加密操作\n    ENCRYPTED --\u003e PLAIN: 解密操作（理论上不推荐）\n    \n    state ENCRYPTED {\n        [*] --\u003e KEY_LOADED: 密钥已加载\n        KEY_LOADED --\u003e DECRYPTING: 开始解密\n        DECRYPTING --\u003e DECRYPTED: 解密成功\n        DECRYPTING --\u003e DECRYPT_FAILED: 解密失败\n        DECRYPTED --\u003e [*]: 返回数据\n        DECRYPT_FAILED --\u003e [*]: 返回错误\n    }\n","theme":"default","view":"chart"}"/>

## 7. 接口规范

### 7.1 API 文档

#### 7.1.1 核心 API

##### 7.1.1.1 `taosWriteEncryptFileHeader`

```c
/**
 * @brief 写入加密文件头（使用原子文件替换）
 * 
 * @param filepath 目标文件路径
 * @param algorithm 加密算法标识（如 TSDB_ENCRYPT_ALGO_SM4）
 * @param data 数据缓冲区（调用者应预先加密数据，可为NULL）
 * @param dataLen 数据长度（0表示空文件）
 * @return 0成功，错误码失败
 */
int32_t taosWriteEncryptFileHeader(const char *filepath, int32_t algorithm, 
                                   const void *data, int32_t dataLen);
```

##### 7.1.1.2 `taosReadEncryptFileHeader`

```c
/**
 * @brief 读取加密文件头
 * 
 * @param filepath 文件路径
 * @param header 输出参数，头数据
 * @return 0成功，错误码失败
 */
int32_t taosReadEncryptFileHeader(const char *filepath, STdEncryptFileHeader *header);
```

##### 7.1.1.3 `taosIsEncryptedFile`

```c
/**
 * @brief 快速检查文件是否加密
 * 
 * @param filepath 文件路径
 * @param algorithm 输出参数，算法标识（可为NULL）
 * @return true文件已加密，false未加密或错误
 */
bool taosIsEncryptedFile(const char *filepath, int32_t *algorithm);
```

##### 7.1.1.4 `taosWriteCfgFile`

```c
/**
 * @brief 写入配置文件（支持透明加密）
 * 
 * 根据 tsCfgKey 是否为空决定是否加密：
 * - 空：写入明文文件
 * - 非空：使用 SM4-CBC 加密后写入
 * 
 * @param filepath 目标文件路径
 * @param data 数据缓冲区
 * @param dataLen 数据长度
 * @return 0成功，错误码失败
 */
int32_t taosWriteCfgFile(const char *filepath, const void *data, int32_t dataLen);
```

##### 7.1.1.5 `taosReadCfgFile`

```c
/**
 * @brief 读取配置文件（支持透明解密）
 * 
 * 自动检测文件是否加密：
 * - 加密：读取头 -> 读取加密数据 -> 解密
 * - 明文：直接读取
 * 
 * @param filepath 文件路径
 * @param data 输出参数，数据缓冲区（调用者需释放）
 * @param dataLen 输出参数，数据长度
 * @return 0成功，错误码失败
 */
int32_t taosReadCfgFile(const char *filepath, char **data, int32_t *dataLen);
```

##### 7.1.1.6 `taosEncryptExistingCfgFiles`

```c
/**
 * @brief 加密现有配置文件
 * 
 * 扫描常见配置文件位置并加密所有明文文件。
 * 在密钥从 mnode 加载后调用，确保所有敏感配置文件都加密。
 * 
 * @param dataDir 数据目录路径（tsDataDir）
 * @return 0成功，错误码失败
 */
int32_t taosEncryptExistingCfgFiles(const char *dataDir);
```

##### 7.1.1.7 `taosWaitCfgKeyLoaded`

```c
/**
 * @brief 等待加密密钥加载（带超时）
 * 
 * 轮询检查加密密钥状态，直到：
 * - 密钥成功加载（返回0）
 * - 超时发生（返回 TSDB_CODE_TIMEOUT_ERROR）
 * 
 * 超时时间：TD_ENCRYPT_KEY_WAIT_TIMEOUT_MS（30秒）
 * 检查间隔：100ms
 * 
 * @return 0密钥加载成功，TSDB_CODE_TIMEOUT_ERROR超时
 */
int32_t taosWaitCfgKeyLoaded(void);
```

#### 7.1.2 辅助函数

##### 7.1.2.1 `taosGetEncryptAlgoName`

```c
/**
 * @brief 获取加密算法名称
 * 
 * @param algorithm 算法ID
 * @return 算法名称字符串
 */
static inline const char *taosGetEncryptAlgoName(int32_t algorithm) {
  if (algorithm >= 0 && algorithm < TD_ENCRYPT_ALGO_NAME_MAX) {
    return TD_ENCRYPT_ALGO_NAMES[algorithm];
  }
  return "UNKNOWN";
}
```

### 7.2 用户界面（如适用）

存储安全模块不直接提供用户界面，但通过以下方式与系统交互：
1. **配置文件管理**: 通过 `taosWriteCfgFile` 和 `taosReadCfgFile` 透明处理加密/解密
2. **命令行工具**: 系统管理员可通过命令行工具管理加密密钥和状态
3. **日志输出**: 加密操作状态通过日志系统输出（uInfo, uDebug, uError）

## 8. 安全考虑

### 8.1 安全要求

#### 8.1.1 数据加密

- **配置文件加密**: 所有敏感配置文件（dnode.json, mnode.json等）必须支持加密存储
- **数据文件加密**: 数据库文件支持透明加密，保护用户数据
- **密钥分离**: 使用多级密钥体系（CFG_KEY, DB_KEY, META_KEY, DATA_KEY）实现密钥分离

#### 8.1.2 密钥管理

- **密钥存储**: 加密密钥从 mnode 加载，不在配置文件中明文存储
- **密钥轮换**: 支持密钥定期轮换机制
- **密钥生命周期**: 密钥状态管理（加载、未加载、不存在、禁用）

#### 8.1.3 访问控制

- **文件权限**: 加密文件设置适当的文件系统权限
- **进程隔离**: 密钥仅在需要加密/解密的进程内存中存在
- **内存安全**: 加密完成后及时清理内存中的明文数据

### 8.2 漏洞缓解

#### 8.2.1 侧信道攻击防护

- **恒定时间操作**: 加密/解密操作实现为恒定时间，防止时序攻击
- **内存清理**: 使用 `memset` 清理敏感内存区域

#### 8.2.2 文件完整性保护

- **原子操作**: 所有文件操作使用原子替换，防止部分写入
- **完整性校验**: 加密文件头包含 magic number 和版本校验
- **错误恢复**: 操作失败时自动清理临时文件

#### 8.2.3 密钥泄露防护

- **内存加密**: 考虑未来支持内存加密技术
- **硬件安全模块**: 支持与 HSM 集成进行密钥管理
- **密钥分割**: 支持密钥分割存储，需要多个组件才能恢复完整密钥

## 9. 性能和可扩展性

### 9.1 性能要求

#### 9.1.1 加密性能

- **算法效率**: SM4-CBC 和 AES-128-CBC 提供良好的性能平衡
- **硬件加速**: 支持 CPU 加密指令集加速（如 AES-NI）
- **批量处理**: 支持大文件分块加密，减少内存占用

#### 9.1.2 I/O 性能影响

- **透明加密**: 加密/解密对上层应用透明，性能影响可控
- **原子操作开销**: 临时文件创建和重命名操作增加少量开销
- **填充开销**: CBC 模式需要16字节对齐，增加约 0-15 字节填充

#### 9.1.3 密钥加载性能

- **异步加载**: 密钥从 mnode 异步加载，不阻塞系统启动
- **超时机制**: 30秒超时防止系统启动卡死
- **状态缓存**: 密钥状态缓存，避免重复检查

### 9.2 可扩展性

#### 9.2.1 算法扩展

- **插件架构**: 加密算法支持动态扩展
- **算法协商**: 支持客户端-服务器算法协商
- **向后兼容**: 新算法保持与旧版本兼容

#### 9.2.2 密钥体系扩展

- **多租户支持**: 支持多租户密钥隔离
- **密钥版本化**: 支持密钥版本管理，便于轮换
- **外部 KMS 集成**: 支持与外部密钥管理服务集成

#### 9.2.3 文件类型扩展

- **通用接口**: `taosWriteEncryptFileHeader` 和 `taosReadEncryptFileHeader` 提供通用加密文件支持
- **格式扩展**: 加密文件头保留字段支持未来格式扩展
- **类型检测**: 自动检测文件加密状态，支持混合环境

## 10. 部署和配置

### 10.1 配置管理

#### 10.1.1 加密开关

```bash

## 11. 启用配置文件加密

tsCfgKey = "your-encryption-key"

## 12. 禁用配置文件加密（空字符串）

tsCfgKey = ""
```

#### 12.0.1 超时配置

```c
// 密钥加载超时时间（毫秒）
#define TD_ENCRYPT_KEY_WAIT_TIMEOUT_MS 30000

// 密钥状态检查间隔（毫秒）
const int32_t checkIntervalMs = 100;
```

#### 12.0.2 算法配置

```c
// 默认加密算法
#define TSDB_ENCRYPT_ALGO_SM4 1

// 算法名称映射
static const char *TD_ENCRYPT_ALGO_NAMES[] = {
    "NONE",         // 0: ENCRYPT_ALGO_NONE
    "SM4-CBC:SM4",  // 1: ENCRYPT_ALGO_SM4
    "AES-128-CBC",  // 2: AES symmetric encryption
    "SM3",          // 3: ENCRYPT_ALGO_SM3
    "SHA-256",      // 4: SHA-256 digest
    "SM2"           // 5: ENCRYPT_ALGO_SM2
};
```

### 12.1 版本控制

#### 12.1.1 向后兼容性

- **文件版本**: 加密文件头包含版本号，支持多版本解析
- **算法兼容**: 新版本支持旧算法解密
- **降级保护**: 加密文件不支持降级到无加密版本

#### 12.1.2 升级策略

1. **数据备份**: 升级前完整备份所有配置文件
2. **渐进升级**: 支持部分文件加密，部分文件明文的混合状态
3. **回滚计划**: 准备加密失败时的回滚方案

#### 12.1.3 发布管理

- **版本标识**: 加密模块版本独立标识
- **变更日志**: 详细记录加密相关变更
- **测试验证**: 发布前全面测试加密功能

## 13. 监控和维护

### 13.1 监控

#### 13.1.1 健康检查

- **密钥状态**: 监控 `tsEncryptKeysStatus` 状态变化
- **文件加密状态**: 定期检查关键配置文件加密状态
- **性能指标**: 监控加密/解密操作性能和资源使用

#### 13.1.2 告警机制

- **密钥加载失败**: 密钥加载超时或失败告警
- **加密操作失败**: 文件加密/解密失败告警
- **完整性告警**: 加密文件完整性校验失败告警

## 14. 参考资料

1. [存储安全模块-Requirement Spec](https://taosdata.feishu.cn/wiki/B5Syws2OciC4iek5ZijcIVzonGe)
2. [存储安全模块-Function Spec](https://taosdata.feishu.cn/wiki/Kp7awktpjiINIIkcI6XcniuLn7c)
