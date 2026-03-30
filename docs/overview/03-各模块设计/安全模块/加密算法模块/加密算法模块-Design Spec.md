# 加密算法模块-Design Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-12-19 | 2025-12-22 | 1.0 | 程洪泽 | 新建 |

## 2. 引言

### 2.1 目的

本文档旨在定义加密算法模块的设计实现方案，为"存储安全"和"身份鉴别"功能提供统一的加密算法支持框架。通过集成国密算法和标准安全算法，并支持自定义算法扩展，构建安全、高效、可扩展的加密服务体系。

### 2.2 范围

本文档涵盖以下内容：
1. 加密算法模块的整体架构设计
2. 算法管理框架的设计与实现
3. 内置算法（国密和国标算法）的集成方案
4. 自定义算法扩展机制
5. 数据库加密功能的设计
6. 性能、安全、兼容性等非功能性设计

### 2.3 受众

1. 系统架构师：了解加密模块的整体设计架构
2. 开发工程师：实现加密算法功能的具体开发人员
3. 测试工程师：验证加密功能正确性的测试人员
4. 运维工程师：部署和维护加密模块的运维人员
5. 安全审计人员：评估加密模块安全性的专业人员

## 3. 术语

| 术语/缩写 | 全称/解释 | 备注 |
| --- | --- | --- |
| SM2 | 国密非对称加密算法，基于椭圆曲线密码学 | GB/T 35275-2017 |
| SM3 | 国密哈希算法，输出256位哈希值 | GB/T 32905-2016 |
| SM4 | 国密对称加密算法，分组长度为 128 位 | GB/T 32907-2016 |
| SM9 | 国密标识密码算法，基于身份标识的密码系统 | 暂未实现 |
| RSA | Rivest-Shamir-Adleman 非对称加密算法 | PKCS#1 标准 |
| AES | Advanced Encryption Standard 对称加密算法 | FIPS 197标准 |
| ECC | Elliptic Curve Cryptography 椭圆曲线密码学 | 暂未实现 |
| SHA | Secure Hash Algorithm 安全哈希算法 | FIPS 180-4 标准 |
| UKEY | 硬件加密设备，用于存储密钥和进行加密运算 |  |
| DLL | Dynamic Link Library 动态链接库 |  |
| OpenSSL | 开源的 SSL/TLS工具包 | 本项目基于 OpenSSL 3.0+ 版本 |
| EVP | Envelope，OpenSSL 提供的高级加密接口 | 屏蔽底层算法差异，提供一致 API |
| Provider | OpenSSL 3.0 引入的模块化架构 | 支持算法动态加载 |
| CBC 模式 | Cipher Block Chaining 密码块链接模式 | 分组密码的工作模式之一，需要初始化向量 |

## 4. 概述

### 4.1 架构

加密算法模块采用分层架构设计，整体架构如下：
<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"graph TB\n    subgraph \"应用层\"\n        A1[存储安全模块]\n        A2[身份鉴别模块]\n        A3[SQL接口]\n    end\n    \n    subgraph \"服务层\"\n        B1[加密算法管理服务]\n        B2[算法调用服务]\n        B3[密钥管理服务]\n    end\n    \n    subgraph \"核心层\"\n        C1[算法管理器]\n        C3[算法注册表]\n    end\n    \n    subgraph \"实现层\"\n        D1[内置算法实现]\n        D2[自定义算法实现]\n        D3[OpenSSL适配器]\n    end\n    \n    subgraph \"基础设施\"\n        E1[配置管理]\n        E2[日志系统]\n    end\n    \n    A1 --\u003e B1\n    A2 --\u003e B1\n    A3 --\u003e B1\n    B1 --\u003e C1\n    B2 --\u003e C3\n    C1 --\u003e C3\n    C3 --\u003e D1\n    C3 --\u003e D2\n    D1 --\u003e D3\n    D2 --\u003e D3\n    D3 --\u003e E1\n    D3 --\u003e E2\n","theme":"default","view":"chart"}"/>

**架构说明**：
1. 应用层：提供 SQL 接口和模块接口，供上层应用调用
2. 服务层：封装加密算法相关服务，包括算法管理、调用和密钥管理
3. 核心层：实现算法管理核心逻辑，包括算法注册和发现等
4. 实现层：具体算法实现，包括内置算法和自定义算法
5. 基础设施：提供配置、日志、监控等基础支持

### 4.2 技术

| 技术栈 | 版本/规格 | 用途说明 |
| --- | --- | --- |
| OpenSSL | 3.0+ | 加密算法基础库，提供 EVP 接口 |
| C 语言 | C11 标准 | 核心模块开发语言 |
| SQL | 系统 SQL 语法 | 算法管理接口 |
| Provider 架构 | OpenSSL 3.0 特性 | 支持算法动态加载和扩展 |
| EVP 接口 | OpenSSL 高级接口 | 统一算法调用方式 |
| JSON | 轻量级数据交换格式 | 算法配置信息存储 |

### 4.3 依赖项

| 依赖项 | 版本要求 | 用途说明 | 许可证要求 |
| --- | --- | --- | --- |
| OpenSSL | ≥3.0.0 | 加密算法基础库 | Apache 2.0 |
| 数据库系统 | 主系统 | 算法信息存储 | - |

## 5. 设计考虑

### 5.1 假设和限制

**假设**：
1. 系统运行在支持 OpenSSL 3.0+ 的环境中
2. 系统管理员具备基本的加密知识
3. 自定义算法开发者熟悉 OpenSSL Provider 开发
**限制**：
1. 自定义算法仅支持对称加密 CBC 模式
2. 算法 ID 范围：内置算法 1-100，自定义算法 101-1000
3. 单个算法实例并发操作有限制
4. 非对称加密算法性能较低，不适合大数据量加密
5. 部分算法（ECC、SM9）暂未实现

### 5.2 设计模式和原则

| 设计模式 | 应用场景 | 实现说明 |
| --- | --- | --- |
| 策略模式 | 算法选择和执行 | 不同算法实现统一的加密/解密接口 |
| 注册表模式 | 算法管理 | 维护算法注册表，支持动态添加/删除算法 |
| 适配器模式 | OpenSSL 接口适配 | 将不同算法的差异封装在适配器中 |

**设计原则**：
1. 开闭原则：支持算法扩展，无需修改现有代码
2. 单一职责：每个模块只负责一个功能
3. 接口隔离：定义清晰的算法接口，避免接口臃肿
4. 依赖倒置：高层模块不依赖低层模块，都依赖抽象接口

### 5.3 风险和缓解措施

| 风险类别 | 风险描述 | 缓解措施 |
| --- | --- | --- |
| 安全风险 | 算法实现存在漏洞 | 1. 使用经过安全审计的OpenSSL库 2. 定期更新算法库 3. 安全代码审查 |
| 性能风险 | 加密操作影响系统性能 | 1. 算法性能基准测试 2. 硬件加速支持 3. 批量处理优化 |
| 兼容性风险 | 新旧版本算法不兼容 | 1. 版本兼容性测试 2. 数据迁移工具 3. 并行支持新旧算法 |
| 扩展性风险 | 自定义算法接口变化 | 1. 稳定的接口规范 2. 版本管理 3. 向后兼容保证 |
| 运维风险 | 算法故障影响业务 | 1. 健康检查机制 2. 故障自动恢复 3. 详细的监控和告警 |

## 6. 详细设计

### 6.1 组件设计

#### 6.1.1 算法管理器

TDengine TSDB 以 MNode 作为算法管理器，一方面负责管理系统内所有加密算法的生命周期，另一方面提供算法的注册、查找、加载和卸载等功能。除此之外，MNode 自带的多副本功能也为算法管理器提供了高可用保障。
职责：
- 管理所有加密算法的生命周期
- 处理算法的注册、查找、加载、卸载
- 维护算法注册表
- 提供算法查询接口
关键接口：
```c
int32_t mndInitEncryptAlgr(SMnode *pMnode);

void mndCleanupEncryptAlgr(SMnode *pMnode);

SEncryptAlgrObj *mndAcquireEncryptAlgrById(SMnode *pMnode, int64_t id);

SEncryptAlgrObj *mndAcquireEncryptAlgrByAId(SMnode *pMnode, char* algorithm_id);

void mndReleaseEncryptAlgr(SMnode *pMnode, SEncryptAlgrObj *pObj);

void mndGetEncryptOsslAlgrNameById(SMnode *pMnode, int64_t id, char* out);
```

#### 6.1.2 算法注册表

算法注册表负责存储系统中所有加密算法的元数据信息，包括内置算法和自定义算法。注册表提供算法信息的快速查找和持久化支持，确保系统能够高效地管理和使用各种加密算法。TDengine TSDB 采用 MNode 内置的 SDB 表作为算法注册表的存储介质。
**职责**：
- 存储算法元数据信息
- 提供算法信息的快速查找
- 支持算法信息的持久化
**数据结构**：
```c
SSdbTable table = {
    .sdbType = SDB_ENCRYPT_ALGORITHMS,
    .keyType = SDB_KEY_INT32,
    .upgradeFp = (SdbUpgradeFp)mndUpgradeBuiltinEncryptAlgr,
    .encodeFp = (SdbEncodeFp)mndEncryptAlgrActionEncode,
    .decodeFp = (SdbDecodeFp)mndEncryptAlgrActionDecode,
    .insertFp = (SdbInsertFp)mndEncryptAlgrActionInsert,
    .updateFp = (SdbUpdateFp)mndEncryptAlgrActionUpdate,
    .deleteFp = (SdbDeleteFp)mndEncryptAlgrActionDelete,
};
```

### 6.2 关键数据结构

#### 6.2.1 加密操作选项（SCryptOpts）

```c
#define ENCRYPT_KEY_LEN 16
typedef struct SCryptOpts {
    int32_t len;                    // 数据长度
    char*   source;                 // 源数据
    char*   result;                 // 结果数据
    int32_t unitLen;                // 单位长度
    char    key[ENCRYPT_KEY_LEN + 1]; // 加密密钥
    char*   pOsslAlgrName;          // OpenSSL算法名称
} SCryptOpts;
```

#### 6.2.2 算法类型宏定义

```c
#define ENCRYPT_ALGR_TYPE__SYMMETRIC_CIPHERS  1
#define ENCRYPT_ALGR_TYPE__DIGEST             2
#define ENCRYPT_ALGR_TYPE__ASYMMETRIC_CIPHERS 3
```

#### 6.2.3 算法来源宏定义

```c
#define ENCRYPT_ALGR_SOURCE_BUILTIN    1
#define ENCRYPT_ALGR_SOURCE_CUSTOMIZED 2
```

### 6.3 数据库设计

系统表 `ins_encrypt_algorithems` 设计：
```c
static const SSysDbTableSchema userEncryptAlgrSchema[] = {
    {.name = "id", .bytes = 4, .type = TSDB_DATA_TYPE_INT, .sysInfo = false},
    {.name = "algorithm_id", .bytes =  TSDB_ENCRYPT_ALGR_NAME_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "name", .bytes =  TSDB_ENCRYPT_ALGR_NAME_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "desc", .bytes = TSDB_ENCRYPT_ALGR_DESC_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "type", .bytes = 200, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "source", .bytes = 200, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
    {.name = "ossl_algr_name", .bytes = TSDB_ENCRYPT_ALGR_NAME_LEN + VARSTR_HEADER_SIZE, .type = TSDB_DATA_TYPE_VARCHAR, .sysInfo = false},
};
```

### 6.4 图表说明

#### 6.4.1 数据流图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"graph LR\n    A[应用程序] --\u003e B[加密服务接口]\n    B --\u003e C{算法选择}\n    C --\u003e|对称加密| D[对称加密处理器]\n    C --\u003e|非对称加密| E[非对称加密处理器]\n    C --\u003e|散列算法| F[散列算法处理器]\n  \n    D --\u003e G[OpenSSL EVP接口]\n    E --\u003e G\n    F --\u003e G\n  \n    G --\u003e H[算法实现]\n    H --\u003e I[内置算法]\n    H --\u003e J[自定义算法]\n  \n    I --\u003e K[SM4/AES等]\n    J --\u003e L[用户实现算法]\n  \n    G --\u003e M[密钥管理器]\n    M --\u003e N[密钥存储]\n  \n    subgraph \"监控和日志\"\n        O[性能监控]\n        P[安全审计]\n        Q[操作日志]\n    end\n  \n    B --\u003e O\n    G --\u003e P\n    D --\u003e Q\n    E --\u003e Q\n    F --\u003e Q\n","theme":"default","view":"chart"}"/>

#### 6.4.2 消息序列图（算法调用流程）

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"sequenceDiagram\n    participant App as 应用程序\n    participant Service as 加密服务\n    participant Manager as 算法管理器\n    participant Factory as 算法工厂\n    participant OpenSSL as OpenSSL\n  \n    App-\u003e\u003eService: 加密请求(algorithm_id, data)\n    Service-\u003e\u003eManager: 查询算法信息(algorithm_id)\n    Manager--\u003e\u003eService: 返回算法信息\n    Service-\u003e\u003eFactory: 创建算法实例(algorithm_id)\n    Factory--\u003e\u003eService: 返回算法实例\n    Service-\u003e\u003eOpenSSL: 执行加密操作(data, key)\n    OpenSSL--\u003e\u003eService: 返回加密结果\n    Service--\u003e\u003eApp: 返回加密数据\n    Service-\u003e\u003eFactory: 归还算法实例\n","theme":"default","view":"chart"}"/>

#### 6.4.3 流程图（对称加密执行步骤）

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"flowchart TD\n    A[开始加密操作] --\u003e B[获取算法ID和数据]\n    B --\u003e C[查询算法管理器获取算法信息]\n    C --\u003e D{算法是否存在且可用?}\n    D --\u003e|是| E[从算法工厂获取算法实例]\n    D --\u003e|否| F[返回错误: 算法不存在]\n    E --\u003e G[初始化加密上下文]\n    G --\u003e H[设置密钥和初始化向量]\n    H --\u003e I[执行加密操作]\n    I --\u003e J[处理加密结果]\n    J --\u003e K[清理加密上下文]\n    K --\u003e L[归还算法实例到工厂]\n    L --\u003e M[返回加密结果]\n    F --\u003e N[结束并返回错误]\n    M --\u003e O[结束并返回成功]\n","theme":"default","view":"chart"}"/>

## 7. 接口规范

### 7.1 SQL 接口

#### 7.1.1 show encrypt_algorithms

功能描述：显示系统中所有可用的加密算法，包括内置算法和用户自定义算法。
语法：
```sql
show encrypt_algorithms;
```

输出字段说明：

| 字段名 | 类型 | 说明 |
| --- | --- | --- |
| id | INT | 算法的数字标识符（1-100 为内置算法，101-1000 为自定义算法） |
| algorithm_id | VARCHAR(64) | 算法的全局唯一标识符，如"SM4-CBC" |
| name | VARCHAR(64) | 算法显示名称 |
| desc | VARCHAR(256) | 算法描述 |
| type | VARCHAR(32) | 算法类型：Symmetric Ciphers CBC mode / Asymmetric Cipher / Digests |
| source | VARCHAR(16) | 算法来源：build-in / customized |
| ossl_algr_name | VARCHAR(128) | OpenSSL 算法名称 |

示例输出：
```sql
 | id  | algorithm_id | name     | desc               | type                       | source     | ossl_algr_name |
 | --- | ------------ | -------- | ------------------ | -------------------------- | ---------- | -------------- |
 | 1   | SM4-CBC      | SM4      | SM4对称加密算法    | Symmetric Ciphers CBC mode | build-in   | SM4-CBC:SM4    |
 | 2   | AES-128-CBC  | AES      | AES对称加密算法    | Symmetric Ciphers CBC mode | build-in   | AES-128-CBC    |
 | 3   | SM2          | SM2      | SM2非对称加密算法  | Asymmetric Cipher          | build-in   | SM2            |
 | 4   | SM3          | SM3      | SM3散列算法        | Digests                    | build-in   | SM3            |
 | 5   | SHA-256      | SHA-256  | SHA-256散列算法    | Digests                    | build-in   | SHA-256        |
 | 6   | RSA          | RSA      | RSA非对称加密算法  | Asymmetric Cipher          | build-in   | RSA            |
 | 101 | vigenere     | vigenere | 自定义维吉尼亚密码 | Symmetric Ciphers CBC mode | customized | vigenere       |
```

#### 7.1.2 create encrypt_algr

功能描述：创建自定义加密算法记录。
语法：
```sql
create encrypt_algr 'algorithm_id' 
name 'algorithm_name' 
desc 'algorithm_description' 
type 'algorithm_type' 
ossl_algr_name 'openssl_algorithm_name';
```

参数说明：
- `algorithm_id`：算法标识符，必须唯一，范围 101-1000
- `algorithm_name`：算法显示名称
- `algorithm_description`：算法详细描述
- `algorithm_type`：算法类型，当前仅支持 `Symmetric_Ciphers_CBC_mode`
- `openssl_algorithm_name`：OpenSSL provider 中的算法名称
约束条件：
1. `algorithm_id` 必须在 101-1000 范围内
2. `algorithm_id` 不能与现有算法重复
3. 对应的.so文件必须已放置在 `encryptExtDir` 配置目录
4. 算法必须已在 OpenSSL provider 中正确注册

#### 7.1.3 drop encrypt_algr

功能描述：删除自定义加密算法。
语法：
```sql
drop encrypt_algr 'algorithm_id';
```

约束条件：
1. 算法必须未被任何数据库使用
2. 必须先删除使用该算法的所有数据库
3. 系统内置算法不可删除

#### 7.1.4 create database（加密扩展）

功能描述：创建数据库时指定加密算法。
语法扩展：
```sql
CREATE DATABASE [IF NOT EXISTS] db_name [database_options]

database_options:
    database_option ...

database_option: {
    ENCRYPT_ALGORITHM {'none' | algorithm_id}
}
```

参数说明：
- `ENCRYPT_ALGORITHM`：数据库加密算法
  - `'none'`：不加密（默认值）
  - `algorithm_id`：`show encrypt_algorithms`中的算法标识
约束条件：
1. 仅支持类型为 `Symmetric Ciphers CBC mode` 的算法
2. 算法必须存在于 `show encrypt_algorithms` 列表中
3. 加密算法一旦设置，不可修改（需重建数据库）

### 7.2 C 语言 API 接口

#### 7.2.1 算法管理接口

```c
int32_t mndInitEncryptAlgr(SMnode *pMnode);

void mndCleanupEncryptAlgr(SMnode *pMnode);

SEncryptAlgrObj *mndAcquireEncryptAlgrById(SMnode *pMnode, int64_t id);

SEncryptAlgrObj *mndAcquireEncryptAlgrByAId(SMnode *pMnode, char* algorithm_id);

void mndReleaseEncryptAlgr(SMnode *pMnode, SEncryptAlgrObj *pObj);

void mndGetEncryptOsslAlgrNameById(SMnode *pMnode, int64_t id, char* out);
```

#### 7.2.2 加密操作接口

```c
int32_t CBC_Decrypt(SCryptOpts* opts);

int32_t CBC_Encrypt(SCryptOpts* opts);

int32_t Builtin_CBC_Encrypt(SCryptOpts* opts);

int32_t Builtin_CBC_Decrypt(SCryptOpts* opts);
```

#### 7.2.3 错误码定义

| 错误码 | 错误描述 | 可能的出错场景或者可能的原因 | 建议用户采取的措施 |
| --- | --- | --- | --- |
| 0x8000042E | Failed to load encryption provider | 加载失败 | 确认 encryptExtDir 是否配置正确 |
| 0x800004E0 | Encrypt algorithm not exists in list | 不存在 | 确认操作是否正确 |
| 0x800004E1 | Invalid encryption algorithm type, support Symmetric_Ciphers_CBC_mode, Digests, Asymmetric_Ciphers now | 不存在 | 确认操作是否正确 |
| 0x800004E2 | Encryption algorithm already exists, please keep algorithm_id unique | 已存在 | 确认操作是否正确 |
| 0x800004E3 | Encryption algorithm type not match | 不存在 | 确认操作是否正确 |
| 0x800004E4 | Invalid encryption algorithm format | 输入算法 id 为空 | 确认操作是否正确 |
| 0x800004E5 | Encryption algorithm in use | 仍然在使用 | 删除所有使用这个算法的对象 |

### 7.3 配置接口

#### 7.3.1 配置文件格式

```plaintext
encryptExtDir  /path/to/custom/algorithms
```

## 8. 安全可控考虑

#### 8.0.1 算法安全性

安全标准符合性：
1. 国密算法：符合国家密码管理局标准
  - SM2：GB/T 35275-2017
  - SM3：GB/T 32905-2016
  - SM4：GB/T 32907-2016
1. 国际标准算法：符合国际安全标准
  - AES：FIPS 197标准
  - RSA：PKCS#1标准
  - SHA：FIPS 180-4标准
算法安全要求：
1. 禁止不安全算法：不实现 MD5、SHA-1、DES、3DES、RC4、RSA-1024 及以下等已知不安全算法
2. 密钥长度要求：
  - 对称加密算法：≥128 位
  - 非对称加密算法：≥2048 位（RSA）或 256 位（ECC/SM2）
1. 算法安全审计：所有集成的算法需无已知安全漏洞，提供算法安全证明或权威机构认证链接

#### 8.0.2 密钥管理安全

参考文档（TODO）。

#### 8.0.3 访问控制

参考文档（TODO）。

#### 8.0.4 安全更新机制

OpenSSL漏洞管理：
1. 漏洞跟踪：定期关注 OpenSSL 安全公告（[https://www.openssl.org/news/vulnerabilities.html](https://www.openssl.org/news/vulnerabilities.html%EF%BC%89)[）](https://www.openssl.org/news/vulnerabilities.html%EF%BC%89)
2. 及时更新：发现安全漏洞时及时应用 OpenSSL 安全补丁
3. 兼容性测试：更新后进行全面兼容性测试
算法淘汰机制：
1. 安全评估：定期评估算法安全性
2. 风险标记：发现安全风险时标记算法为"不安全"
3. 迁移支持：提供数据重加密工具，支持算法迁移

### 8.1 代码可控性

#### 8.1.1 自研代码控制

核心模块自研：
1. 算法管理框架：完全自研，确保代码透明度和可控性
2. 接口适配层：自研 OpenSSL 适配器，隔离第三方库变化
3. 密钥管理模块：自研密钥生命周期管理，确保密钥安全
代码质量保障：
1. 代码审查：所有加密相关代码需经过安全代码审查
2. 静态分析：使用静态代码分析工具检测安全漏洞
3. 动态测试：进行模糊测试和渗透测试

#### 8.1.2 第三方库安全审查

详见文档（TODO）。

## 9. 性能和可扩展性

参考[加密算法模块-Function Spec](https://taosdata.feishu.cn/wiki/TrHRwsHhcipXw7khCBWcpXNmnRh)和[加密算法模块-Requirement Spec](https://taosdata.feishu.cn/wiki/PKguwahaKi75oNk9w3Fcg062ndZ)。

## 10. 部署和配置

### 10.1 部署

本功能与 TDengine TSDB 一并发布，无需特殊部署。

### 10.2 配置

参考[加密算法模块-Design Spec](https://taosdata.feishu.cn/wiki/Hr88wlTKDi1AHKkBeotcPE9wntf)的配置接口章节。

## 11. 监控和维护

无。

### 11.1 日志记录和诊断

无。

## 12. 参考资料

1. [加密算法模块-Requirement Spec](https://taosdata.feishu.cn/wiki/PKguwahaKi75oNk9w3Fcg062ndZ)
2. [加密算法模块-Function Spec](https://taosdata.feishu.cn/wiki/TrHRwsHhcipXw7khCBWcpXNmnRh)
