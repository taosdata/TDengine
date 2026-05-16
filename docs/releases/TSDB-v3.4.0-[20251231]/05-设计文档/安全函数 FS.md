# 安全函数 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-10-14 | - | 0.1 | 金明磊 | 初稿 |
| 2025-11-19 | 2025-11-19 | 1.0 | 关胜亮 | 修订格式 |

## 2. 背景

安全函数的需求来自于：[安全函数 RS](https://taosdata.feishu.cn/wiki/K6yOwulCHiwXPIk0iv0coTCYnsc)，包含四类功能：加密函数，编码函数，哈希函数，脱敏函数。其中
1. BASE64 编解码函数也可以归类于字符串函数
2. 数据脱敏和去标识化能够实现有用信息的安全交付，使用数据脱敏函数可以使敏感数据得到变形处理
3. 数据加解密函数可以使数据加密后存储、并解密。
JIRA: [TS-7235](https://jira.taosdata.com:18080/browse/TS-7235)

## 3. 定义

### 3.1 适用数据类型

函数中的适用数据类型，表示参数只接受说明中描述的数据类型，其他的数据类型会导致函数报错。

### 3.2 多字节安全

字符串函数的多字节安全（multibyte safe）指的是在处理多字节字符（比如中文字符）的时候，会把该字符当成一个整体来看待，而不是按照字节划分并按照多个个体来看待。
1. 多字节字符在匹配子串的时候，有一个中文字符 `你` 的十六进制表示为 `E4 BD A0`，此时假设有一个字符串的十六进制表示为 `E4 BD` ，该字符串并不会被认为是该中文字符的子串。
2. 在计算字符串的字符长度（CHAR_LENGTH）的时候，会把多字节字符 `你` 的长度算做 1，而非 3。
当前实现，只保证使用 UTF-8 编码的字符串的多字节安全。

### 3.3 不同数据类型比较规则

因为涉及到不同数据类型比较时的类型转换，在此定义比较规则。
1. 如果有任意参数为 NULL ，返回 NULL。
2. 如果输入参数都是同一类型，按照该类型比较，返回值也是该类型。
3. 数值类型和字符串类型比较，按照数值类型进行比较，返回值是字符串类型。
4. 如果输入参数包含 VARBINARY 类型，那么其余的参数必须都是 VARCHAR 或 VARBINARY 类型，否则会报错，此时按照 VARBINARY 类型进行比较，返回值为 VARBINARY 类型。

## 4. 行为说明

下面分四部分说明函数行为：加密函数，编码函数，哈希函数，脱敏函数。分别对应于 RS 中的这四类函数。

### 4.1 加密函数

#### 4.1.1 SM4_ENCRYPT

```plaintext
SM4_ENCRYPT(str, key_str)
```

**功能说明**：使用 SM4 算法对数据进行加密
**返回结果类型**：字符串。
**适用数据类型**：字符串。
**使用说明**：
1. 支持在 INSERT 和 SELECT 语句中使用
2. key_str 为密钥
3. 仅企业版支持
**举例**：
```sql
-> INSERT INTO t VALUES (1, SM4_ENCRYPT('abcd'，'mykeystring'));
```

#### 4.1.2 SM4_DECRYPT

```plaintext
SM4_DECRYPT(crypt_str, key_str)
```

**功能说明**：使用 SM4 算法对数据进行解密
**返回结果类型**：字符串。
**适用数据类型**：字符串。
**使用说明**：
1. key_str 为密钥
2. 仅企业版支持
**举例**：
```sql
-> SELECT ts，SM4_DECRYPT(data，'mykeystring') from t;
```

#### 4.1.3 AES_ENCRYPT

```plaintext
AES_ENCRYPT(str, key_str[, init_vector])
```

**功能说明**：在 AES-128-CBC 模式下，把字符串加密为密文。
**返回结果类型**：字符串。
**适用数据类型**：字符串。
**使用说明**：
1. key_str 为密钥，init_vector 为初始化向量。
**举例**：
```sql {wrap}
-> SELECT AES_DECRYPT(AES_ENCRYPT('text',SHA2('My secret passphrase',512)),SHA2('My secret passphrase',512)) as str;
             str       |
========================
                'text' |
```

#### 4.1.4 AES_DECRYPT

```plaintext
AES_DECRYPT(crypt_str, key_str[, init_vector])
```

**功能说明**：把字符串密文解密为明文。
**返回结果类型**：字符串。
**适用数据类型**：字符串。
**使用说明**：
1. key_str 为密钥，init_vector 为初始化向量。
**举例**：
```sql {wrap}
-> SELECT AES_DECRYPT(AES_ENCRYPT('text',SHA2('My secret passphrase',512)),SHA2('My secret passphrase',512)) as str;
             str       |
========================
                'text' |
```

### 4.2 哈希函数

#### 4.2.1 MD5

```plaintext
MD5(str)
```

**功能说明**：计算字符串的 MD5 128 位校验和。
**返回结果类型**：字符串。
**适用数据类型**：字符串。
**使用说明**：
1. 如果输入为 NULL，则返回 NULL。
**举例**：
```sql
-> SELECT MD5('testing');
    MD5('testing')                     |
=======================================
    'ae2b1fca515949e5d54fb22b8ed95575' |
```

#### 4.2.2 SHA1 / SHA

```plaintext
SHA1(str)
```

**功能说明**：计算 SHA1 160 位校验和，具体可参考 RFC 3174。
**返回结果类型**：字符串。
**适用数据类型**：字符串。
**使用说明**：
1. SHA 与 SHA1 是同义词。
**举例**：
```sql
-> SELECT SHA1('abc');
    SHA1('abc')                                |
================================================
    'a9993e364706816aba3e25717850c26c9cd0d89d' |
```

#### 4.2.3 SHA2

```plaintext
SHA2(str, hash_length)
```

**功能说明**：计算 SHA2 系列的哈希函数（SHA-224, SHA-256, SHA-384, and SHA-512）。
**返回结果类型**：字符串。
**适用数据类型**：字符串。
**使用说明**：
1. 当 hash_length 为 0 时，作为 256 处理。
**举例**：
```sql
-> SELECT SHA2('abc', 224);
    SHA2('abc', 224)                                           |
===============================================================
    '23097d223405d8228642a477bda255b32aadbce4bda0b3f7e36c9da7' |
```

### 4.3 脱敏函数

#### 4.3.1 MASK_FULL

```plaintext
MASK_FULL(column_name, replacement_value)
```

**功能说明**：将目标数据进行全脱敏处理。
**返回结果类型**：字符串。
**适用数据类型**：字符串。
**举例**：
```sql
-> SELECT MASK_FULL(name, 'CONFIDENTIAL') AS masked_name FROM customers;
    masked_name    |
===================
    'CONFIDENTIAL' |
```

#### 4.3.2 MASK_PARTIAL

```plaintext
MASK_PARTIAL(column_name, visible_prefix_length, visible_suffix_length, mask_char)
```

**功能说明**：将目标数据进行部分脱敏处理。
**返回结果类型**：字符串。
**适用数据类型**：字符串。
**举例**：
```sql
-> SELECT MASK_PARTIAL(phone, 3, 4, '*') AS masked_phone FROM customers;
    masked_phone  |
===================
    '***2011****' |
```

#### 4.3.3 MASK_NONE

```plaintext
MASK_NONE(column_name)
```

**功能说明**：将目标数据进行空脱敏处理。
**返回结果类型**：字符串。
**适用数据类型**：字符串。
**举例**：
```sql
-> SELECT MASK_NONE(salary) AS salary FROM customers;
    salary        |
===================
    '2011'        |
```

### 4.4 编码函数

#### 4.4.1 FROM_BASE64

```plaintext
FROM_BASE64(str)
```

**功能说明**：解码 base64 编码的字符串。
**返回结果类型**：字符串。
**适用数据类型**：BINARY, VARCHAR 字符串。
**使用说明**：
1. 如果输入为 NULL，或非法的 base64 编码，则返回 NULL。
**举例**：
```sql
-> SELECT TO_BASE64('abc'), FROM_BASE64(TO_BASE64('abc'));
    TO_BASE64('abc')    |   FROM_BASE64(TO_BASE64('abc'))  |
===========================================================
    'JWJj'              | 'abc'                            |
```

#### 4.4.2 TO_BASE64

```plaintext
TO_BASE64(str)
```

**功能说明**：用 base64 编码字符串。
**返回结果类型**：字符串。
**适用数据类型**：BINARY, VARCHAR 字符串。
**使用说明**：
1. 如果输入为 NULL，则返回 NULL。
**举例**：
```sql
-> SELECT TO_BASE64('abc'), FROM_BASE64(TO_BASE64('abc'));
    TO_BASE64('abc')    |   FROM_BASE64(TO_BASE64('abc'))  |
===========================================================
    'JWJj'              | 'abc'                            |
```

### 4.5 特殊说明：

1. 加解密函数仅在企业版支持(目前的加解密功能只在企业版本具备)
2. 其他函数在企业版和社区版都支持
3. Select/insert 语句支持：脱敏函数只支持 select，其它函数支持 select 和 insert 语句。

## 5. 安全

1. SM4 的密钥保存在数据库中，已经进行加密。
2. 脱敏函数需要和动态数据脱敏策略结合，实现基于角色的、精细化的访问控制，本期不实现，但留下接口。

## 6. 性能

加解密、编码函数对数据写入和查询有一定影响，在测试阶段，给出典型输出的写入和查询性能。如果写入延迟和查询延迟增加超过 100%，需优化代码。

## 7. 兼容性

非存储层修改，不涉及兼容性问题。

## 8. 运维

不涉及。

## 9. 使用场景

不涉及。

## 10. 约束和限制

不涉及。

## 11. 常见错误和排查

不涉及。

## 12. 可观测性

不涉及。

## 13. 安装和卸载

不涉及。

## 14. 文档

1. 需要修改官网文档
2. 行为说明章节中的内容，需要更新到官网文档的 **SQL**** ****手册-函数**部分。

## 15. 参考文档

需求：[安全函数 RS](https://taosdata.feishu.cn/wiki/K6yOwulCHiwXPIk0iv0coTCYnsc)
JIRA: [TS-7235](https://jira.taosdata.com:18080/browse/TS-7235)
函数行为可参考：
1. https://dev.mysql.com/doc/refman/9.0/en/encryption-functions.html
2. https://dev.mysql.com/doc/refman/9.0/en/data-masking-component-functions.html
3. https://dev.mysql.com/doc/refman/8.4/en/string-functions.html#function_from-base64
4. https://dev.mysql.com/doc/refman/8.4/en/string-functions.html#function_to-base64
5. https://mariadb.com/docs/server/reference/sql-functions/secondary-functions/encryption-hashing-and-compression-functions

## 16. 附录

无。
