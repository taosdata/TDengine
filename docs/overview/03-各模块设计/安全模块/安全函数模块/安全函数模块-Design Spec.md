# 安全函数模块-Design Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-12-22 | 2025-12-24 | 1.0 | 程洪泽 | 新建 |

## 2. 引言

### 2.1 **目的**

本文档旨在定义安全函数模块的设计方案，该模块提供数据库层面的数据安全处理能力，包括加密、哈希、脱敏和编码四类函数。通过本设计文档，开发团队可以理解模块的架构、组件设计、接口规范和安全考虑，确保实现符合功能规格和需求规格的要求。

### 2.2 **范围**

本文档涵盖安全函数模块的整体架构设计、组件设计、接口规范、安全考虑、性能要求、部署配置和监控维护等方面。具体包括：
1. 加密函数：SM4_ENCRYPT/SM4_DECRYPT, AES_ENCRYPT/AES_DECRYPT
2. 哈希函数：MD5, SHA1/SHA, SHA2
3. 脱敏函数：MASK_FULL, MASK_PARTIAL, MASK_NONE
4. 编码函数：FROM_BASE64, TO_BASE64
5. 版本支持：企业版支持所有函数，社区版支持除加密函数外的其他函数

### 2.3 **受众**

1. 开发人员：理解模块架构和实现细节
2. 架构师：评审设计方案和技术选型
3. 测试人员：了解功能边界和测试要点
4. 运维人员：掌握部署配置和监控维护方法
5. 项目经理：跟踪设计进度和风险点

## 3. 术语

| 术语/缩写 | 全称/解释 |
| --- | --- |
| SM4 | 国密 SM4 对称加密算法，分组长度和密钥长度均为 128 位 |
| AES | 高级加密标准（Advanced Encryption Standard），支持 128/192/256 位密钥 |
| MD5 | 消息摘要算法第 5 版（Message-Digest Algorithm 5），输出 32 位十六进制字符串 |
| SHA | 安全散列算法（Secure Hash Algorithm），包括 SHA1（40 位）、SHA2 系列（224/256/384/512 位） |
| Base64 | Base64 编码标准，用于二进制数据的文本化表示 |
| DDM | 动态数据脱敏（Dynamic Data Masking） |
| KMS | 密钥管理系统（Key Management System） |
| CBC | 密码分组链接模式（Cipher Block Chaining），AES 加密的工作模式 |
| PKCS7 | 公钥密码学标准第 7 号，定义填充方案 |
| UTF-8 | Unicode 转换格式 8 位，支持多字节字符编码 |

## 4. 概述

### 4.1 **架构**

安全函数模块采用插件式架构，作为数据库的扩展模块集成。整体架构分为三层：
<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"graph TB\n    subgraph \"应用层（SQL接口）\"\n        A[SQL查询] --\u003e B[数据库解析器]\n        B --\u003e C[安全函数插件]\n    end\n    \n    subgraph \"函数调度层\"\n        C --\u003e D[函数调度器]\n        D --\u003e E1[加密函数模块]\n        D --\u003e E2[哈希函数模块]\n        D --\u003e E3[脱敏函数模块]\n        D --\u003e E4[编码函数模块]\n    end\n    \n    subgraph \"算法实现层\"\n        E1 --\u003e F1[国密算法 SM4]\n        E1 --\u003e F2[国际算法 AES]\n        E2 --\u003e F3[基础算法库 OpenSSL]\n        E3 --\u003e F4[脱敏算法]\n        E4 --\u003e F5[编码算法 Base64]\n    end\n    \n    F1 --\u003e G[结果返回用户]\n    F2 --\u003e G\n    F3 --\u003e G\n    F4 --\u003e G\n    F5 --\u003e G\n    \n    style A fill:#e1f5fe\n    style B fill:#e1f5fe\n    style C fill:#e1f5fe\n    style D fill:#f3e5f5\n    style E1 fill:#f3e5f5\n    style E2 fill:#f3e5f5\n    style E3 fill:#f3e5f5\n    style E4 fill:#f3e5f5\n    style F1 fill:#e8f5e8\n    style F2 fill:#e8f5e8\n    style F3 fill:#e8f5e8\n    style F4 fill:#e8f5e8\n    style F5 fill:#e8f5e8\n    style G fill:#fff3e0\n","theme":"default","view":"chart"}"/>

- **应用层**：提供标准SQL函数接口，支持SELECT、INSERT等语句调用
- **函数调度层**：按函数类别分发请求，处理参数验证和类型转换
- **算法实现层**：具体算法的实现，包括国密算法、国际标准算法和基础算法库

### 4.2 **技术**

- **编程语言**：C/C++（性能关键部分），部分辅助功能使用Python
- **加密库**：OpenSSL 1.1.1+（支持AES、SHA系列算法）
- **国密算法**：自主实现SM4算法，确保代码可控性
- **编译工具**：CMake 3.10+，GCC 7.0+ 或 Clang 5.0+
- **数据库接口**：MySQL插件API或类似数据库扩展接口
- **测试框架**：Google Test（单元测试），sysbench（性能测试）

### 4.3 **依赖项**

- https://github.com/kokke/tiny-AES-C
- C++标准库（C++11或更高版本）

## 5. 设计考虑

### 5.1 **假设和限制**

- **假设**：
   - 数据库使用 UTF-8 编码，确保多字节字符正确处理
   - 操作系统提供可靠的随机数生成器（/dev/urandom 或类似）
   - 硬件支持 64 位运算和必要的加密指令集（如 AES-NI）
   - 系统管理员具备基本的安全配置知识
- **限制**：
   - 加密函数最大数据大小：16MB（超过需分块处理）
   - 哈希函数最大数据大小：1GB
   - Base64 函数最大数据大小：16MB
   - 单次加密最大记录数：1000 条
   - 最大并发连接数：1000 连接
   - 单个函数调用最大内存：1GB
   - 总内存使用限制：系统内存的 50%

### 5.2 **风险和缓解措施**

- **风险**** ****1：算法实现漏洞**
  - **缓解**：核心算法（SM4）自主实现，经过严格安全审计；国际算法使用成熟开源库（OpenSSL）
  - **验证**：通过标准测试向量验证算法正确性
- **风险**** ****2：密钥泄露**
  - **缓解**：密钥在数据库中加密存储，使用数据库主密钥保护；支持外部KMS集成
  - **监控**：记录所有密钥使用操作，异常访问实时告警
- **风险**** ****3：性能瓶颈**
  - **缓解**：支持批量处理、硬件加速（AES-NI）、结果缓存
  - **优化**：性能测试覆盖典型场景，设置性能阈值告警
- **风险**** ****4：第三方库许可证冲突**
  - **缓解**：严格评估第三方库许可证，确保与项目许可证兼容
  - **控制**：建立第三方依赖管理机制，记录所有依赖及其许可证
- **风险**** ****5：升级兼容性问题**
  - **缓解**：保持函数接口向后兼容，提供数据迁移工具
  - **测试**：升级前进行全面兼容性测试

## 6. 详细设计

### 6.1 加密函数接口

#### 6.1.1 SM4

```java
/**
 * @brief 使用 SM4 算法加密数据
 *
 * @param key    加密密钥
 * @param keylen 密钥长度
 * @param pBuf   输入/输出缓冲区。输入时包含明文，输出时包含密文。缓冲区大小必须足够容纳加密后的数据（参见
 * tsm4_encrypt_len）
 * @param len    输入数据的长度
 * @return int32_t 加密后数据的长度，如果失败则返回负值
 */
int32_t taosSm4Encrypt(uint8_t* key, int32_t keylen, uint8_t* pBuf, int32_t len);

/**
 * @brief 使用 SM4 算法解密数据
 *
 * @param key    解密密钥
 * @param keylen 密钥长度
 * @param pBuf   输入/输出缓冲区。输入时包含密文，输出时包含明文
 * @param len    输入密文数据的长度
 * @return int32_t 解密后数据的长度，如果失败则返回负值
 */
int32_t taosSm4Decrypt(uint8_t* key, int32_t keylen, uint8_t* pBuf, int32_t len);

/**
 * @brief 计算 SM4 加密所需的缓冲区长度
 *
 * @param len 输入数据的原始长度
 * @return uint32_t 加密后所需的最大缓冲区长度（通常包含填充）
 */
uint32_t tsm4_encrypt_len(int32_t len);
```

#### 6.1.2 AES

```cpp
/**
 * @brief 使用 AES 算法加密数据
 *
 * @param key    加密密钥
 * @param keylen 密钥长度
 * @param pBuf   输入/输出缓冲区。输入时包含明文，输出时包含密文。缓冲区大小必须足够容纳加密后的数据（参见
 * taes_encrypt_len）
 * @param len    输入数据的长度
 * @param iv     初始化向量 (Initialization Vector)，如果模式不需要 IV 可为 NULL
 * @return int32_t 加密后数据的长度，如果失败则返回负值
 */
int32_t taosAesEncrypt(uint8_t* key, int32_t keylen, uint8_t* pBuf, int32_t len, const uint8_t* iv);

/**
 * @brief 使用 AES 算法解密数据
 *
 * @param key    解密密钥
 * @param keylen 密钥长度
 * @param pBuf   输入/输出缓冲区。输入时包含密文，输出时包含明文
 * @param len    输入密文数据的长度
 * @param iv     初始化向量 (Initialization Vector)，必须与加密时使用的 IV 一致
 * @return int32_t 解密后数据的长度，如果失败则返回负值
 */
int32_t taosAesDecrypt(uint8_t* key, int32_t keylen, uint8_t* pBuf, int32_t len, const uint8_t* iv);

/**
 * @brief 计算 AES 加密所需的缓冲区长度
 *
 * @param len 输入数据的原始长度
 * @return uint32_t 加密后所需的最大缓冲区长度（通常包含填充）
 */
uint32_t taes_encrypt_len(int32_t len);
```

### 6.2 哈希函数接口

```java
/**
 * @brief 计算给定缓冲区的 SHA1 哈希值，并将结果以十六进制字符串形式写回该缓冲区。
 *
 * 该函数首先计算输入数据的 SHA1 摘要（20字节），然后将其格式化为
 * 40个字符的十六进制字符串（例如 "a94a8fe5..."），并覆盖写入到原始缓冲区 `pBuf` 中。
 *
 * @param pBuf 指向要计算哈希的数据缓冲区的指针。该缓冲区必须足够大，
 *             至少能容纳 41 个字节（40个十六进制字符 + 1个空终止符），
 *             以便存储生成的哈希字符串。
 * @param len  输入数据的长度（字节数）。
 *
 * @return int32_t 返回写入缓冲区的字符数（通常为 40），如果出错则返回负值。
 *
 * @note 此函数会修改 `pBuf` 的内容。调用者需确保 `pBuf` 有足够的空间
 *       来存放输出的哈希字符串，否则会导致缓冲区溢出。
 */
int32_t taosCreateSHA1Hash(char *pBuf, int32_t len);
```

```java
/**
 * @brief 创建 SHA-2 系列哈希值 (SHA224, SHA256, SHA384, SHA512)
 *
 * 该函数根据指定的摘要大小（digestSize），计算输入缓冲区的 SHA-2 哈希值，
 * 并将结果以十六进制字符串的形式写回输入缓冲区 `pBuf`。
 *
 * @param pBuf       输入/输出缓冲区。
 *                   输入时：包含需要计算哈希的原始数据。
 *                   输出时：包含计算出的哈希值的十六进制字符串表示（以 null 结尾）。
 *                   注意：调用者必须确保 `pBuf` 有足够的空间来存储生成的哈希字符串
 *                   (即 digestSize / 4 + 1 字节)。
 * @param len        输入数据的长度（字节数）。
 * @param digestSize 目标哈希摘要的位长度 (例如：224, 256, 384, 512)。
 *
 * @return int32_t   写入缓冲区的字符数（不包括 null 终止符）。
 *                   如果 digestSize 不支持，则返回 0。
 */
int32_t taosCreateSHA2Hash(char *pBuf, int32_t len, uint32_t digestSize);
```

### 6.3 脱敏函数接口

```java
/**
 * @brief 对输入数据进行全掩码处理（全部隐藏）。
 *
 * @param pInput 输入参数数组，包含待掩码的数据。
 * @param inputNum 输入参数的数量。
 * @param pOutput 输出参数，用于存储掩码后的数据。
 * @return int32_t 执行结果状态码，0 表示成功，非 0 表示失败。
 */
int32_t maskFullFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);

/**
 * @brief 对输入数据进行部分掩码处理（隐藏部分内容）。
 *
 * @param pInput 输入参数数组，包含待掩码的数据。
 * @param inputNum 输入参数的数量。
 * @param pOutput 输出参数，用于存储掩码后的数据。
 * @return int32_t 执行结果状态码，0 表示成功，非 0 表示失败。
 */
int32_t maskPartialFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);

/**
 * @brief 不对输入数据进行掩码处理（保持原样）。
 *
 * @param pInput 输入参数数组。
 * @param inputNum 输入参数的数量。
 * @param pOutput 输出参数，用于存储原始数据。
 * @return int32_t 执行结果状态码，0 表示成功，非 0 表示失败。
 */
int32_t maskNoneFunction(SScalarParam *pInput, int32_t inputNum, SScalarParam *pOutput);
```

### 6.4 编码函数接口

```java
/**
 * @brief Base64 编码函数
 *
 * 将输入的二进制数据进行 Base64 编码。
 *
 * @param out     输出缓冲区，用于存储编码后的 Base64 字符串。调用者需确保缓冲区足够大。
 * @param input   输入缓冲区，包含待编码的原始二进制数据。
 * @param in_len  输入数据的长度（字节数）。
 * @param out_len 输出缓冲区的最大容量（字节数），用于防止溢出。
 */
void tbase64_encode(uint8_t *out, const uint8_t *input, size_t in_len, VarDataLenT out_len);

/**
 * @brief Base64 解码函数
 *
 * 将输入的 Base64 字符串解码为原始二进制数据。
 *
 * @param out     输出缓冲区，用于存储解码后的二进制数据。
 * @param input   输入缓冲区，包含待解码的 Base64 字符串。
 * @param in_len  输入 Base64 字符串的长度。
 * @param out_len 输入时表示输出缓冲区的最大容量；输出时存储实际解码后的数据长度。
 *
 * @return int32_t 成功返回 0，失败返回非 0 错误码。
 */
int32_t tbase64_decode(uint8_t *out, const uint8_t *input, size_t in_len, VarDataLenT *out_len);

/**
 * @brief 计算 Base64 编码后的长度
 *
 * 根据输入数据的长度，计算编码所需的缓冲区大小（通常包含结尾的空字符或填充）。
 *
 * @param in_len 输入数据的长度。
 * @return uint32_t 编码后所需的缓冲区长度。
 */
uint32_t tbase64_encode_len(size_t in_len);

/**
 * @brief 计算 Base64 解码后的最大长度
 *
 * 根据 Base64 字符串的长度，估算解码后所需的缓冲区大小。
 *
 * @param in_len 输入 Base64 字符串的长度。
 * @return uint32_t 解码后所需的最大缓冲区长度。
 */
uint32_t tbase64_decode_len(size_t in_len);
```

## 7. 接口规范

详细 SQL 接口变更参考[安全函数模块-Function Spec](https://taosdata.feishu.cn/wiki/FVb4wvjgUi3qHWkf0VmcAPNgnUf).

## 8. 安全可控考虑

### 8.1 **安全要求**

- **数据加密**：敏感数据在存储和传输过程中必须加密
  - SM4 算法符合国密标准（GB/T 32907-2016）
  - AES 算法符合国际标准（FIPS PUB 197）
  - 密钥在数据库中加密存储，使用数据库主密钥保护
- **用户认证**：函数调用需验证用户权限
  - 加密函数需要 ENCRYPT 权限
  - 脱敏函数需要 MASK 权限
  - 所有函数需要 EXECUTE 基础权限
- **授权控制**：基于角色的访问控制
  - 管理员：可查看原始数据（MASK_NONE）
  - 普通用户：看到脱敏数据
  - 审计员：可查看审计日志但不可修改数据
- **输入验证**：所有函数参数严格验证
  - 类型检查：确保参数类型符合要求
  - 边界检查：防止缓冲区溢出
  - 长度检查：防止超长输入攻击
1. **概述如何缓解漏洞以及如何处理敏感数据**
  - **代码可控性**：
    - 核心算法（SM4）自主实现，确保无后门
    - 第三方库（OpenSSL）严格评估和审计
    - 代码变更记录详细日志，支持版本回滚
  - **漏洞缓解**：
    - 定期安全扫描和代码审计
    - 及时更新第三方库安全补丁
    - 安全测试包括边界条件和异常输入
  - **敏感数据处理**：
    - 内存中的敏感数据及时清零
    - 密钥材料不写入日志文件
    - 错误信息不泄露敏感数据细节
  - **供应链安全**：
    - 建立第三方依赖管理机制
    - 记录所有依赖库的许可证和版本
    - 确保依赖来源可信，版本可控

## 9. 性能和可扩展性（如适用）

### 9.1 **性能要求**

- **响应时间**：
  - 加密操作：P50 < 100ms，P95 < 200ms，P99 < 500ms
  - 哈希操作：P50 < 50ms，P95 < 100ms，P99 < 200ms
  - 脱敏操作：P50 < 10ms，P95 < 20ms，P99 < 50ms
  - 编码操作：P50 < 20ms，P95 < 50ms，P99 < 100ms
- **吞吐量**：
  - 加密函数：单核 > 1000 OPS（1KB数据）
  - 哈希函数：单核 > 5000 OPS（1KB数据）
  - 脱敏函数：单核 > 10000 OPS
- **资源使用**：
  - CPU使用率：峰值 < 70%
  - 内存使用：单个函数 < 2GB，总计 < 系统内存50%
  - 磁盘IO：根据硬件配置优化

### 9.2 **可扩展性**

- **水平扩展**：
  - 函数调用无状态，支持多实例部署
  - 数据库连接池优化，支持高并发
  - 监控指标集中收集，支持集群部署
- **垂直扩展**：
  - 支持硬件加速（Intel AES-NI）
  - 算法实现优化，充分利用多核CPU
  - 内存使用优化，支持大数据处理
- **扩展策略**：
  - 批量处理：支持批量加密/解密操作
  - 异步处理：非实时场景支持异步操作
  - 缓存机制：热点数据加密结果缓存

## 10. 部署和配置

本特性随 TDengine TSDB 发布，无单独部署或配置需求。

## 11. 监控和维护

无。

## 12. 参考资料

1. [安全函数模块-Requirement Spec](https://taosdata.feishu.cn/wiki/ZJaMwJsMRifWtRkehypc9CRVnQc)
2. [安全函数模块-Function Spec](https://taosdata.feishu.cn/wiki/FVb4wvjgUi3qHWkf0VmcAPNgnUf)
