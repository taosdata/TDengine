# 加密算法模块-Function Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-10-28 | 2025-11-20 | 1.0 | 陈东明 | 新建 |
| 2025-12-08 | 2025-12-12 | 1.1 | 程洪泽 | 修订及完善内容 |

## 2. 背景

随着数据安全要求的不断提高，数据库系统需要提供更强大、更灵活的加密能力。当前系统仅支持有限的加密算法，无法满足多样化的安全需求。为提升系统的安全性和合规性，需要集成业界标准的加密算法，并支持用户自定义算法。
- **关联 JIRA**：[TS-7270](https://jira.taosdata.com:18080/browse/TS-7270)

## 3. 定义

| **术语** | **英文** | **定义** | **备注** |
| --- | --- | --- | --- |
| **OpenSSL** | OpenSSL | 开源的 SSL/TLS 工具包，包含完整的加密算法库和密码学工具 | 本项目基于 OpenSSL 3.0+ 版本 |
| **EVP** | Envelope | OpenSSL 提供的高级加密接口，提供统一的算法调用方式 | 屏蔽底层算法差异，提供一致 API |
| **Provider** | Provider | OpenSSL 3.0 引入的模块化架构，支持算法动态加载 | 包括 default、legacy、fips 等 provider |
| **国密算法** | Chinese Commercial Cryptography | 中国国家密码管理局发布的商用密码算法 | 包括 SM2、SM3、SM4 等 |
| **CBC模式** | Cipher Block Chaining | 分组密码的工作模式之一，每个明文块先与前一个密文块进行异或操作，再进行加密 | 提供更好的安全性，但需要初始化向量(IV) |

## 4. 行为说明

### 4.1 算法管理功能

#### 4.1.1 show encrypt_algorithms

**功能描述**：显示系统中所有可用的加密算法，包括内置算法和用户自定义算法。
**语法**：
```sql
show encrypt_algorithms;
```

**输出示例**：
```plaintext {wrap}
show encrypt_algorithms;
 id | algorithm_id | name       |              desc        |              type          |
=========================================================================================
  1 | SM4-CBC      | SM4        | SM4 symmetric encryption | Symmetric Ciphers CBC mode | 
  2 | AES-128-CBC  | AES        | AES symmetric encryption | Symmetric Ciphers CBC mode | 
101 | vigenere     | vigenere   | my custom algr           | Symmetric Ciphers CBC mode |
```

**字段说明**：
1. **id**：算法的数字标识
  - 范围：正整数
  - 内置算法：1-100
  - 自定义算法：101-1000
1. **algorithm_id**：算法的全局唯一标识
  - 格式：字符串，支持字母、数字、连字符
  - 命名规范：`算法名称-模式`（如 SM4-CBC、AES-128-CBC）
  - 唯一性：全系统唯一，不可重复
1. **name**：算法名称
  - 格式：英文字符串
  - 长度限制：最大 64 字符
1. **desc**：算法的描述
  - 内容：算法功能、特点、适用场景
  - 长度限制：最大 256 字符
1. **type**：算法类型，包括：
  - **Symmetric Ciphers CBC mode**：对称加密算法 CBC 模式
    - 用途：数据库数据加密
    - 特点：分组加密，需要初始化向量
  - Asymmetric Cipher：非对称加密算法
    - 用途：密钥交换、数字签名
    - 特点：公钥加密，私钥解密
  - Digests：散列算法
    - 用途：数据完整性校验、数字签名
    - 特点：单向不可逆
1. **source**：算法来源，包括：
   - **build-in**：内置算法
   - **customized**：用户自定义算法
2. **ossl_algr_name**：算法在 OpenSSL 中的名称
  - 内置算法：OpenSSL 默认 provider 中的算法名称
  - 自定义算法：用户 provider 中注册的算法名称
  - 参考：[https://docs.openssl.org/master/man7/OSSL_PROVIDER-default/](https://docs.openssl.org/master/man7/OSSL_PROVIDER-default/)

### 4.2 内置算法详情

#### 4.2.1 对称加密算法（CBC模式）

| 算法 | 密钥长度 | 块大小 | 安全等级 | 性能等级 | 标准 | 适用场景 |
| --- | --- | --- | --- | --- | --- | --- |
| SM4-CBC | 128位 | 128位 | 高（国密） | 高 | GB/T 32907-2016 | 金融、政务等高安全场景 |
| AES-128-CBC | 128位 | 128位 | 高 | 高 | FIPS 197 | 国际通用，兼容性好 |

**技术特性**：
- 工作模式：CBC（Cipher Block Chaining）
- 填充方式：PKCS#7
- 初始化向量：16字节，随机生成
- 密钥管理：系统自动管理，用户无需关心

#### 4.2.2 非对称加密算法

| 算法 | 密钥长度 | 安全等级 | 性能等级 | 标准 | 主要用途 |
| --- | --- | --- | --- | --- | --- |
| SM2 | 256位 | 高（国密） | 中 | GB/T 32918-2016 | 数字签名、密钥交换 |
| RSA | 2048位 | 高 | 低 | PKCS#1 | 兼容现有系统 |

#### 4.2.3 散列算法

| 算法 | 输出长度 | 安全等级 | 性能等级 | 标准 | 主要用途 |
| --- | --- | --- | --- | --- | --- |
| SM3 | 256位 | 高（国密） | 高 | GB/T 32905-2016 | 数字签名、完整性校验 |
| SHA-256 | 256位 | 高 | 高 | FIPS 180-4 | 国际通用，兼容性好 |

#### 4.2.4 未实现算法说明

1. ECC 算法（椭圆曲线密码学）
  - 状态：暂未实现
  - 原因：OpenSSL 中对应 EVP_KEM-EC 类别，当前需求不明确
  - 计划：后续版本根据需求开发
  - 参考：[https://docs.openssl.org/master/man7/EVP_KEM-EC/](https://docs.openssl.org/master/man7/EVP_KEM-EC/)
1. SM9 算法（基于身份的密码）
  - 状态：暂未实现
  - 原因：OpenSSL 未原生支持，需要额外开发
  - 计划：评估需求后决定开发计划

### 4.3 自定义算法管理

#### 4.3.1 配置参数

`**encryptExtDir**`：自定义算法库加载路径
- 类型：字符串
- 默认值：空（不加载自定义算法）
- 格式：绝对路径，指向包含算法库的目录
- 示例：`/opt/taos/encrypt/`
- 限制：当前仅支持加载单个.so文件

#### 4.3.2 create encrypt_algr

**功能描述**：创建自定义加密算法记录。
**语法**：
```sql
CREATE ENCRYPT_ALGR <algorithm_id>
       NAME <algorithm_name>
       DESC <algorithm_description>
       TYPE <algorithm_type>
       OSSL_ALGR_NAME <openssl_algorithm_name>;
```

**参数说明**：
- `algorithm_id`：算法标识符，必须唯一
- `algorithm_name`：算法显示名称
- `algorithm_description`：算法详细描述
- `algorithm_type`：算法类型，当前仅支持 `Symmetric_Ciphers_CBC_mode`
- `openssl_algorithm_name`：OpenSSL provider中的算法名称
**示例**：
```sql
CREATE ENCRYPT_ALGR 'vigenere' 
       NAME 'vigenere' 
       DESC 'my custom algr' 
       TYPE 'Symmetric_Ciphers_CBC_mode' 
       OSSL_ALGR_NAME 'vigenere';
```

**约束条件**：
1. `algorithm_id` 必须在 101-1000 范围内
2. `algorithm_id` 不能与现有算法重复
3. 对应的 .so 文件必须已放置在 `encryptExtDir` 目录
4. 算法必须已在 OpenSSL provider 中正确注册

#### 4.3.3 drop encrypt_algr

**功能描述**：删除自定义加密算法。
**语法**：
```sql
DROP ENCRYPT_ALGR <algorithm_id>;
```

**示例**：
```sql
DROP ENCRYPT_ALGR 'vigenere';
```

**约束条件**：
1. 算法必须未被任何数据库使用
2. 必须先删除使用该算法的所有数据库
3. 系统内置算法不可删除

#### 4.3.4 自定义算法开发接口

**开发框架**：基于 OpenSSL 3.0+ Provider 架构
**核心接口**：
```c
// Provider管理接口static OSSL_FUNC_provider_query_operation_fn custom_prov_operation;
static OSSL_FUNC_provider_get_params_fn custom_prov_get_params;
static OSSL_FUNC_provider_get_reason_strings_fn custom_prov_get_reason_strings;

// 对称加密算法接口static OSSL_FUNC_cipher_newctx_fn custom_newctx;
static OSSL_FUNC_cipher_encrypt_init_fn custom_encrypt_init;
static OSSL_FUNC_cipher_decrypt_init_fn custom_decrypt_init;
static OSSL_FUNC_cipher_update_fn custom_update;
static OSSL_FUNC_cipher_final_fn custom_final;
static OSSL_FUNC_cipher_dupctx_fn custom_dupctx;
static OSSL_FUNC_cipher_freectx_fn custom_freectx;
static OSSL_FUNC_cipher_get_params_fn custom_get_params;
static OSSL_FUNC_cipher_gettable_params_fn custom_gettable_params;
static OSSL_FUNC_cipher_set_ctx_params_fn custom_set_ctx_params;
static OSSL_FUNC_cipher_get_ctx_params_fn custom_get_ctx_params;
static OSSL_FUNC_cipher_settable_ctx_params_fn custom_settable_ctx_params;
static OSSL_FUNC_cipher_gettable_ctx_params_fn custom_gettable_ctx_params;
```

**开发指南**：
1. 参考 OpenSSL官方文档：[https://docs.openssl.org/master/man7/provider/](https://docs.openssl.org/master/man7/provider/)
2. 示例项目：[https://github.com/provider-corner/vigenere](https://github.com/provider-corner/vigenere)
3. 编译要求：与系统 OpenSSL 版本一致
4. 依赖库：OpenSSL 3.0+ 开发库
**限制**：
1. 仅支持对称加密算法开发
2. 必须实现完整的CBC模式接口
3. 算法性能需满足基本要求（不低于SM4的50%）

### 4.4 数据库加密功能

#### 4.4.1 create database

**功能描述**：创建数据库时指定加密算法。
**语法扩展**：
```sql
CREATE DATABASE [IF NOT EXISTS] db_name [database_options]

database_options:
    database_option ...

database_option: {
    ENCRYPT_ALGORITHM {'none' | algorithm_id}
}
```

**参数说明**：
- `ENCRYPT_ALGORITHM`：数据库加密算法
  - `'none'`：不加密（默认值）
  - `algorithm_id`：`show encrypt_algorithms`中的算法标识
**示例**：
```sql
-- 创建使用SM4加密的数据库
CREATE DATABASE secure_db 
    ENCRYPT_ALGORITHM 'SM4-CBC'
    BUFFER 16 
    CACHESIZE 1;

-- 创建使用自定义算法加密的数据库
CREATE DATABASE custom_db 
    ENCRYPT_ALGORITHM 'vigenere'
    DURATION 10d;
```

**约束条件**：
1. 仅支持类型为 `Symmetric Ciphers CBC mode` 的算法
2. 算法必须存在于 `show encrypt_algorithms` 列表中
3. 加密算法一旦设置，不可修改（需重建数据库）

#### 4.4.2 show create database

**功能描述**：显示数据库创建语句，包含加密算法信息。
示例输出：
```sql
show create database secure_db\G;

*************************** 1.row ***************************
       Database: secure_db
Create Database: CREATE DATABASE `secure_db` 
BUFFER 16 
CACHESIZE 1 
CACHEMODEL 'none' 
COMP 2 
DURATION 10d 
WAL_FSYNC_PERIOD 3000 
MAXROWS 4096 
MINROWS 100 
STT_TRIGGER 2 
KEEP 365d,365d,365d 
PAGES 256 
PAGESIZE 4 
PRECISION 'ms' 
REPLICA 3 
WAL_LEVEL 1 
VGROUPS 1 
SINGLE_STABLE 0 
TABLE_PREFIX 0 
TABLE_SUFFIX 0 
TSDB_PAGESIZE 4 
WAL_RETENTION_PERIOD 3600 
WAL_RETENTION_SIZE 0 
KEEP_TIME_OFFSET 0 
ENCRYPT_ALGORITHM 'SM4-CBC' 
SS_CHUNKPAGES 131072 
SS_KEEPLOCAL 525600m 
SS_COMPACT 1 
COMPACT_INTERVAL 0d 
COMPACT_TIME_RANGE 0d,0d 
COMPACT_TIME_OFFSET 0h
```

### 4.5 算法调用接口

其他模块调用加密算法时，使用 OpenSSL 的 EVP 接口调用算法。EVP 接口说明参看https://docs.openssl.org/master/man7/evp/ 。
例如，对称加密的调用大致如下：
```sql
EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();

EVP_CIPHER *cipher = EVP_CIPHER_fetch(NULL, osslAlgrName, NULL);

EVP_EncryptInit(ctx, cipher, key, IV);

EVP_EncryptUpdate(ctx, opts->result, &outlen, opts->source, opts->len);

EVP_EncryptFinal(ctx, &opts->result[outlen], &tmplen);

```

其中调用不同的算法需要制定 osslAlgrName，即 show encrypt_algorithms 中的 ossl_algr_name。
1. SM2 的调用如下：https://jishuzhan.net/article/1723131346328489985
2. RSA 的调用如下：https://blog.csdn.net/yizhiniu_xuyw/article/details/114371606
散列算法的调用如下：
```cpp
if(!(mdctx = EVP_MD_CTX_create())) goto err;
/* 创建缓冲区来存储收到消息的MAC */
if(!(sigtmp = OPENSSL_malloc(sizeof(unsigned char) * EVP_PKEY_size(key)))) goto err;
sigtmplen = EVP_PKEY_size(key);
/* 计算收到消息的MAC */
if(1 != EVP_DigestSignInit(mdctx, NULL, EVP_sha256(), NULL, key)) goto err;
if(1 != EVP_DigestSignUpdate(mdctx, msg, strlen(msg))) goto err;
if(1 != EVP_DigestSignFinal(mdctx, sigtmp, &sigtmplen)) goto err;
```

这部分代码参考自：http://gmssl.org/docs/evp-api.html
每种类型的算法需要根据使用模块的设计进行细节调整和二次封装。例如 Symmetric Ciphers CBC mode，为了简化设计，将 key 和 IV 设计成相同的值，并且二次封装成如下方法：
```plaintext
int32_t Symmetric_Ciphers_CBC_Encrypt(SCryptOpts *opts)
int32_t Symmetric_Ciphers_CBC_Decrypt(SCryptOpts *opts)

typedef struct SCryptOpts {
  int32_t len;
  char*   source;
  char*   result;
  int32_t unitLen;
  char    key[ENCRYPT_KEY_LEN + 1];
  char*   pOsslAlgrName;
} SCryptOpts;
```

这些细节的设计和二次封装需各使用模块自行实现。

## 5. 性能

在使用加密算法的功能中，性能会比不使用加密算法的情况下，要有性能损失。依据使用的算法的不同，损失也会不同。
算法采用OpenSSL的实现，各算法性能可参看OpenSSL的benchmark:
https://openssl-library.org/performance/
因为数据库采用了对称加密的CBC模式，所以对数据写入、数据查询都会有影响。本次改动重新实现了 SM4 算法，并且增加了 AES 算法。新的 SM4 算法实现的性能，与旧 SM4 算法持平。新的 AES 算法对性能的影响，也与旧 SM4 算法持平。

## 6. 安全

### 6.1 漏洞管理

1. OpenSSL 漏洞跟踪：定期关注 OpenSSL 安全公告
  - 漏洞公告：[https://www.openssl.org/news/vulnerabilities.html](https://www.openssl.org/news/vulnerabilities.html)
  - 安全更新：及时应用OpenSSL安全补丁
1. 算法淘汰机制：发现安全漏洞时及时标记并淘汰受影响算法
2. 应急响应：制定加密算法安全事件应急响应流程

### 6.2 安全审计

1. 操作日志：记录所有加密算法管理操作
2. 访问控制：基于角色的算法访问权限控制
3. 安全事件：监控异常加密操作行为
4. 合规检查：定期检查算法使用合规性

## 7. 兼容性

### 7.1 旧 SM4 算法兼容

放弃原有的 SM4 算法，采用 OpenSSL 中的算法。在实现时，测试 2 种算法是否兼容，如果则替换旧算法，如果不兼容则保留旧算法。

### 7.2 旧加密库兼容

在现有实现中 SM4 算法采用整形数字 1 表示，为保持与现有实现兼容，show encrypt_algorithms中，SM4 的 id 需保持为 1，不能改为其他值。

### 7.3 升级以及后续添加内置算法

添加 upgrade 机制，在现有集群的基础上，升级到新版本，可添加内置算法到 mnode 中。另外，利用 upgrade 机制，后期也可以继续添加新算法。

## 8. 运维

### 8.1 监控告警

1. 操作日志：记录算法管理操作
2. 错误日志：记录加密操作错误
3. 审计日志：记录安全相关事件
4. 性能日志：记录加密性能数据

### 8.2 配置管理

#### 8.2.1 配置文件

参考 4.3.1 配置参数部分。

## 9. 使用场景

### 9.1 典型应用场景

下表展示了不同行业对加密算法的典型需求及推荐配置：

| 行业/场景 | 需求特点 | 推荐配置 |
| --- | --- | --- |
| 金融行业 | 1. 高安全性要求 2. 国密算法合规要求 3. 审计追踪需求 | 1. 数据库加密：SM4-CBC 2. 数字签名：SM2 + SM3 3. 密钥管理：硬件加密模块 |
| 政府机构 | 1. 数据保密性要求高 2. 国产化要求 3. 分级保护 | 1. 核心数据：SM4-CBC 2. 普通数据：AES-128-CBC 3. 身份认证：SM2数字证书 |
| 互联网企业 | 1. 高性能要求 2. 国际标准兼容 3. 灵活扩展 | 1. 数据加密：AES-128-CBC 2. 传输加密：TLS 1.3 3. 自定义算法：业务特定需求 |

### 9.2 最佳实践

下表总结了加密算法使用的最佳实践：

| 实践类别 | 具体原则/实践 |
| --- | --- |
| 算法选择原则 | 1. 安全性满足业务需求 2. 性能影响可接受 3. 符合合规要求 |
| 密钥管理实践 | 1. 定期轮换加密密钥 2. 分离存储密钥和数据 3. 实施最小权限原则 |
| 性能优化实践 | 1. 批量处理加密数据 2. 启用硬件加速 3. 监控性能指标 |

## 10. 约束和限制

### 10.1 技术限制

1. 算法类型限制：自定义算法仅支持对称加密 CBC 模式
2. 性能限制：非对称加密算法性能较低，不适合大数据量加密
3. 兼容限制：部分旧硬件可能不支持某些算法硬件加速

### 10.2 使用限制

1. 算法数量：最多支持 1000 个算法（内置 100+ 自定义 900）
2. 并发限制：单个算法实例并发加密操作有限制
3. 数据大小：单次加密操作支持最大数据长度受内存限制

### 10.3 平台限制

1. 操作系统：依赖 OpenSSL 3.0+，部分旧系统需要升级
2. 硬件要求：部分算法需要特定 CPU 指令集支持
3. 内存要求：加密操作需要额外内存缓冲区

## 11. 常见错误和排查

| Encrypt algorithm not exists in list" | 指定的算法不存在，重新选择 |
| --- | --- |
| Invalid encryption algorithm type | 在指定的类型中选择 |
| Encryption algorithm already exists | 算法 id 重复 |
| Encryption algorithm in use | 算法在使用中，不能删除 |

## 12. 可观测性

加载路径为手工配置，在配置错误时，会导致taosd无法启动，在日志中添加明确日志输出，说明加载路径错误。

## 13. 安装和卸载

无

## 14. 文档

在文档中，添加如下文档：
1.增加 show encrypt_algorithms
2.增加 create encrypt_algr
3.增加 drop encrypt_algr
4.修改 create db, show create database 的加密字段的描述
5.自定义算法实现接口的 OpenSSL 说明文档的链接
6.增加配置项目 encryptExtDir 的相关说明

## 15. 参考文档

[加密算法 RS](https://taosdata.feishu.cn/wiki/HUQCwzSS7iRrGVkiyV8c93o0n1b)

## 16. 附录

无
