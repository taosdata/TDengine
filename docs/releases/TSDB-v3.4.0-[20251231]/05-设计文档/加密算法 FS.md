# 加密算法 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-10-28 | - | 0.1 | 陈东明 | 新建 |
| 2025-11-20 | 2025-11-20 | 1.0 | 陈东明 | 发布 |

## 2. 背景

1. 集成加密算法，并且提供用户可自定义算法的机制
2. JIRA [TS-7270](https://jira.taosdata.com:18080/browse/TS-7270)

## 3. 定义

1. OpenSSL: 开源的 ssl 实现，其中包含一个独立的加密库，提供多种加密算法，并且提供用户自定义算法的接口。
2. EVP：OpenSSL提供的调用加密算法的接口。

## 4. 行为说明

### 4.1 show encrypt_algorithms;

```plaintext
show encrypt_algorithms;
     id      |          algorithm_id          |              name              |              desc              |              type              |             source             |         ossl_algr_name         |
====================================================================================================================================================================================================================
           1 | SM4-CBC                        | SM4                            | SM4 symmetric encryption       | Symmetric Ciphers CBC mode     | build-in                       | SM4-CBC:SM4                    |
           2 | AES-128-CBC                    | AES                            | AES symmetric encryption       | Symmetric Ciphers CBC mode     | build-in                       | AES-128-CBC                    |
         101 | vigenere                       | vigenere                       | my custom algr                 | Symmetric Ciphers CBC mode     | customized                     | vigenere                       |
```

**字段说明**
1. id：算法的数字标识，内置算法从1开始，自定义算法从101开始
2. algorithm_id：算法的全局唯一标识
3. name：算法名称
4. desc：算法的描述
5. type：算法类型，包括：
   - Symmetric Ciphers CBC mode：对称加密算法CBC模式，用于数据库加密
   - Asymmetric Cipher：非对称加密算法
   - Digests：散列算法
6. source：算法来源，包括：
   - build-in:内置算法
   - customized：用户自定义算法
7. ossl_algr_name：算法在OpenSSL中的名称，如果是内置算法则是在default provider中的名称，可以参看 https://docs.openssl.org/master/man7/OSSL_PROVIDER-default/ , 如果自定义算法，则是用户在程序中自定义

### 4.2 新增的算法

以上算法参看 https://docs.openssl.org/master/man7/OSSL_PROVIDER-default/ 

| algorithm_id | name | desc | type | ossl_algr_name |  |
| --- | --- | --- | --- | --- | --- |
| SM4-CBC | SM4 | SM4 symmetric encryption | Symmetric Ciphers CBC mode | SM4-CBC:SM4 | https://docs.openssl.org/master/man7/EVP_CIPHER-SM4/ |
| AES-128-CBC | AES | AES symmetric encryption | Symmetric Ciphers CBC mode | AES-128-CBC | https://docs.openssl.org/master/man7/EVP_ASYM_CIPHER-SM2/ |
| SM2 | SM2 | SM2 Asymmetric Cipher | Asymmetric Cipher |  | https://docs.openssl.org/master/man7/EVP_ASYM_CIPHER-SM2/ |
| SM3 | SM3 | SM3 digests | Digests | SM3 | https://docs.openssl.org/master/man7/EVP_MD-SM3/ |
| SHA-256 | SHA-256 | SHA2 digests | Digests | SHA-256 | https://docs.openssl.org/master/man7/EVP_SIGNATURE-RSA/ |
| RSA | RSA | RSA Asymmetric Cipher | Asymmetric Cipher |  | https://docs.openssl.org/master/man7/EVP_ASYM_CIPHER-RSA/ |

未实现如下算法：
1. 需求中的 ECC 算法，应该是 Asymmetric Key Encapsulation 算法，参看https://docs.openssl.org/master/man7/EVP_KEM-EC/，无对应类别，所以本次未实现，后续需要再开发
2. 需求中的 SM9，是基于身份的密码，无对应类别，并且在 OpenSSL 中未实现，后续看时机需要再开发

### 4.3 加载路径

新增配置参数 encryptExtDir，指定自定义算法库 so 文件的路径。目前只支持加载单个文件。

### 4.4 create encrypt_algr

```sql
create encrypt_algr 'vigenere' name 'vigenere' desc 'my custom algr' type 'Symmetric_Ciphers_CBC_mode' ossl_algr_name 'vigenere';
```

type 字段的取值
1. Symmetric_Ciphers_CBC_mode：对称加密算法CBC模式，用于数据库加密

### 4.5 自定义算法接口

用户自定义算法，用户需按照接口开发一个 so 库，taosd 启动时会加载这个 so 库，so 库被加载后，用户自定义算法即可被使用。在这个 so 库中，用户可以包含多个算法，算法有自己的命名，通过 create encrypt_algr 中的 ossl_algr_name 字段指定。
自定义算法接口采用 OpenSSL 的实现，遵循 OpenSSL 的接口定义。Open SSL 的接口定义参看 https://docs.openssl.org/master/man7/provider/ 。
在 Github 中，有 OpenSSL 的自定义算法实现，例如：https://github.com/provider-corner，其中项目https://github.com/provider-corner/vigenere 实现了一个最简单的加密算法。在这个项目中实现如下接口即可：
```sql
static OSSL_FUNC_provider_query_operation_fn vigenere_prov_operation;
static OSSL_FUNC_provider_get_params_fn vigenere_prov_get_params;
static OSSL_FUNC_provider_get_reason_strings_fn vigenere_prov_get_reason_strings;

static OSSL_FUNC_cipher_newctx_fn vigenere_newctx;
static OSSL_FUNC_cipher_encrypt_init_fn vigenere_encrypt_init;
static OSSL_FUNC_cipher_decrypt_init_fn vigenere_decrypt_init;
static OSSL_FUNC_cipher_update_fn vigenere_update;
static OSSL_FUNC_cipher_final_fn vigenere_final;
static OSSL_FUNC_cipher_dupctx_fn vigenere_dupctx;
static OSSL_FUNC_cipher_freectx_fn vigenere_freectx;
static OSSL_FUNC_cipher_get_params_fn vigenere_get_params;
static OSSL_FUNC_cipher_gettable_params_fn vigenere_gettable_params;
static OSSL_FUNC_cipher_set_ctx_params_fn vigenere_set_ctx_params;
static OSSL_FUNC_cipher_get_ctx_params_fn vigenere_get_ctx_params;
static OSSL_FUNC_cipher_settable_ctx_params_fn vigenere_settable_ctx_params;
static OSSL_FUNC_cipher_gettable_ctx_params_fn vigenere_gettable_ctx_params;
```

用户无法定制非对称加密算法，因为无统一的调用方法。

### 4.6 其他模块调用加密算法

其他模块调用加密算法时，使用 OpenSSL 的 EVP 接口调用算法。EVP 接口说明参看https://docs.openssl.org/master/man7/evp/ 。
例如，对称加密的调用大致如下：
```sql
EVP_CIPHER_CTX *ctx = EVP_CIPHER_CTX_new();

EVP_CIPHER *cipher = EVP_CIPHER_fetch(NULL, osslAlgrName, NULL);

EVP_EncryptInit(ctx, cipher, key, IV);

EVP_EncryptUpdate(ctx, opts->result, &outlen, opts->source, opts->len);

EVP_EncryptFinal(ctx, &opts->result[outlen], &tmplen);

```

其中调用不同的算法需要制定osslAlgrName，即show encrypt_algorithms中的ossl_algr_name。
1. SM2的调用如下：https://jishuzhan.net/article/1723131346328489985
2. RSA的调用如下：https://blog.csdn.net/yizhiniu_xuyw/article/details/114371606
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

### 4.7 Drop encrypt_algr

```plaintext
drop encrypt_algr 'vigenere';
```

删除一个自定义算法前，必须保证这个算法没有被使用，比如必须提前删除使用该算法的 database。
其他使用算法的模块，必须添加同样的检查逻辑在删除算法的逻辑中。

### 4.8 create db

```plaintext
CREATE DATABASE [IF NOT EXISTS] db_name [database_options]
 
database_options:
    database_option ...
 
database_option: {
   ENCRYPT_ALGORITHM {'none' | 'sm4'}
}

```

ENCRYPT_ALGORITHM: 指定数据采用的加密算法。默认是 none，即不采用加密。如果要设置加密数据，则需指定 show encrypt_algorithms 中 algorithm_id，并且类型为 Symmetric Ciphers CBC mode。

### 4.9 show create database

```markdown
show create database power2\G;
*************************** 1.row ***************************
       Database: power2
Create Database: CREATE DATABASE `power2` BUFFER 16 CACHESIZE 1 CACHEMODEL 'none' COMP 2 DURATION 10d WAL_FSYNC_PERIOD 3000 MAXROWS 4096 MINROWS 100 STT_TRIGGER 2 KEEP 365d,365d,365d PAGES 256 PAGESIZE 4 PRECISION 'ms' REPLICA 3 WAL_LEVEL 1 VGROUPS 1 SINGLE_STABLE 0 TABLE_PREFIX 0 TABLE_SUFFIX 0 TSDB_PAGESIZE 4 WAL_RETENTION_PERIOD 3600 WAL_RETENTION_SIZE 0 KEEP_TIME_OFFSET 0 ENCRYPT_ALGORITHM 'vigenere' SS_CHUNKPAGES 131072 SS_KEEPLOCAL 525600m SS_COMPACT 1 COMPACT_INTERVAL 0d COMPACT_TIME_RANGE 0d,0d COMPACT_TIME_OFFSET 0h
```

ENCRYPT_ALGORITHM：显示指定的加密算法，show encrypt_algorithms 中 algorithm_id

## 5. 性能

在使用加密算法的功能中，性能会比不使用加密算法的情况下，要有性能损失。依据使用的算法的不同，损失也会不同。
算法采用OpenSSL的实现，各算法性能可参看OpenSSL的benchmark:
https://openssl-library.org/performance/
因为数据库采用了对称加密的CBC模式，所以对数据写入、数据查询都会有影响。本次改动重新实现了 SM4 算法，并且增加了 AES 算法。新的 SM4 算法实现的性能，与旧 SM4 算法持平。新的 AES 算法对性能的影响，也与旧 SM4 算法持平。

## 6. 安全

内置算法采用 OpenSSL 的实现，在及时更新的情况下，可修补安全漏洞。
OpenSSL的公布的漏洞修复列表如下：
https://openssl-library.org/news/vulnerabilities/index.html

## 7. 兼容性

### 7.1 旧 SM4 算法兼容

放弃原有的 SM4 算法，采用 OpenSSL 中的算法。在实现时，测试 2 种算法是否兼容，如果则替换旧算法，如果不兼容则保留旧算法。

### 7.2 旧加密库兼容

在现有实现中 SM4 算法采用整形数字 1 表示，为保持与现有实现兼容，show encrypt_algorithms中，SM4 的 id 需保持为 1，不能改为其他值。

### 7.3 升级以及后续添加内置算法

添加 upgrade 机制，在现有集群的基础上，升级到新版本，可添加内置算法到 mnode 中。另外，利用 upgrade 机制，后期也可以继续添加新算法。

## 8. 运维

无

## 9. 使用场景

### 9.1 列出所有内置可使用算法

使用 show encrypt_algorithms 可以展示所有算法

### 9.2 添加、列出自定义算法

通过 create encrypt_algr 可以增加一种新的加密算法，并且在对应的功能中使用，例如添加一种对称加密的 CBC 模式，可以在 create db 时使用。
使用 show encrypt_algorithms 可以展示自定义算法

## 10. 约束和限制

无

## 11. 常见错误和排查

| Encrypt algorithm not exists in list" | 指定的算法不存在，重新选择 |
| --- | --- |
| Invalid encryption algorithm type | 在指定的类型中选择 |
| Encryption algorithm already exists | 算法id重复 |
| Encryption algorithm in use | 算法在使用中，不能删除 |

## 12. 可观测性

加载路径为手工配置，在配置错误时，会导致taosd无法启动，在日志中添加明确日志输出，说明加载路径错误。

## 13. 安装和卸载

无

## 14. 文档

在文档中，添加如下文档：
1.增加show encrypt_algorithms
2.增加create encrypt_algr
3.增加drop encrypt_algr
4.修改 create db, show create database的加密字段的描述
5.自定义算法实现接口的OpenSSL说明文档的链接
6.增加配置项目encryptExtDir的相关说明

## 15. 参考文档

[加密算法 RS](https://taosdata.feishu.cn/wiki/HUQCwzSS7iRrGVkiyV8c93o0n1b)

## 16. 附录

无
