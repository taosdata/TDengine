# 安全函数 RS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-09-10 | - | 0.1 | 关胜亮 | 新建 |
| 2025-10-14 | 2025-10-14 | 1.0 | 关胜亮 | 按评审记录修改 |

## 2. 引言

### 2.1 术语与缩写名词

### 2.2 相关文档资料

JIRA [TS-7235](https://jira.taosdata.com:18080/browse/TS-7235)

### 2.3 优先级要求

高

### 2.4 版本要求

1. 加解密函数仅在企业版支持
2. 其他函数在企业版和社区版都支持

## 3. 需求目标

1. 数据脱敏函数：敏感数据变形处理
2. 数据加密函数：数据加密后存储、解密

## 4. 功能需求

### 4.1 加密函数

#### 4.1.1 SM4 加密函数

1. 语法：`SM4_ENCRYPT(str)`
2. 说明：
   - 使用 SM4 算法对数据进行加密
   - 创建用户时，如果指定了“存储加密密钥”，则会自动生成适配算法的密钥
   - 支持在 INSERT 和 SELECT 语句中使用
3. 示例
```sql {wrap}
SELECT SM4_ENCRYPT('abcd');
INSERT INTO t VALUES (1, SM4_ENCRYPT('abcd'));
```

#### 4.1.2 SM4 解密函数

1. 语法：`SM4_DECRYPT(str)`
2. 说明
   - 使用 SM4 算法对数据进行解密
   - 创建用户时，如果指定了“存储加密密钥”，则会自动生成适配算法的密钥
   - 支持在 INSERT 和 SELECT 语句中使用
3. 示例
```sql {wrap}
SELECT ts，SM4_DECRYPT(data) from t;
```

#### 4.1.3 AES 加密函数

1. 语法：`AES_ENCRYPT(str, key_str[, init_vector])`
2. 说明：
   - 使用密钥 `key_str`（和可选的初始化向量 `init_vector`）对明文 `str`进行加密，返回二进制字符串
   - 支持在 INSERT 和 SELECT 语句中使用，参见 MySQL 的语法实现。

#### 4.1.4 AES 解密函数

1. 语法：`AES_DECRYPT(crypt_str, key_str[, init_vector])`
2. 说明：
   - `AES_DECRYPT`使用相同的密钥 `key_str`（和初始化向量 `init_vector`）对密文 `crypt_str`进行解密，返回原始明文字符串。
   - 支持在 INSERT 和 SELECT 语句中使用，参见 MySQL 的语法实现。

### 4.2 编码函数

#### 4.2.1 Base64 编码函数

1. 语法：`TO_BASE64(str)`
2. 说明：
   - 将字符串或二进制数据转换为 Base64 编码的字符串。
   - 支持在 INSERT 和 SELECT 语句中使用，具体要求参见 MySQL 的语法实现。
3. 示例
```sql {wrap}
SELECT TO_BASE64('abcd');
INSERT INTO t VALUES (1, TO_BASE64('abcd'));
```

#### 4.2.2 Base64 解码函数

1. 语法：`FROM_BASE64(str)`
2. 说明：
   - 将 Base64 编码的字符串解码回原始的二进制数据或字符串。
   - 支持在 INSERT 和 SELECT 语句中使用，具体要求参见 MySQL 的语法实现。
3. 示例
```sql {wrap}
SELECT ts，FROM_BASE64(data) from t;
```

### 4.3 哈希函数

#### 4.3.1 MD5 函数

1. 语法：`MD5(str)`
2. 说明：
   - 计算字符串 `str`的 MD5 哈希值，返回一个 32 位的十六进制字符串。MD5 已经不安全，但为功能全面性考虑，仍需提供。
   - 支持在 INSERT 和 SELECT 语句中使用，具体要求参见 MySQL 的语法实现。
3. 示例
```sql {wrap}
SELECT MD5('Hello World');
```

#### 4.3.2 SHA1 函数

1. 语法：`SHA1(str)`
2. 说明：
   - 是一种生成160位（20字节）哈希值的加密哈希函数，其结果通常表示为40位的十六进制字符串。
   - 支持在 INSERT 和 SELECT 语句中使用，具体要求参见 MySQL 的语法实现。
3. 示例
```sql {wrap}
SELECT SHA1('Hello World');
```

#### 4.3.3 SHA2 函数

1. 语法：`SHA2(str)`
2. 说明：
   - 计算字符串 `str`的哈希值，支持 224、256、384、512 位。
   - 推荐使用 SHA-256。
   - 支持在 INSERT 和 SELECT 语句中使用，具体要求参见 MySQL 的语法实现。
3. 示例
```sql {wrap}
SELECT SHA2('Hello World');
```

### 4.4 脱敏函数

#### 4.4.1 MASK_FULL

1. 语法：`MASK_FULL(column_name, replacement_value)`
2. 说明​：将指定列的所有敏感数据替换为一个无意义的、统一的固定值。
3. 示例：
```sql {wrap}
SELECT MASK_FULL(name, 'CONFIDENTIAL') AS masked_name FROM users;
```

#### 4.4.2 MASK_PARTIAL

1. 语法：`MASK_PARTIAL(column_name, visible_prefix_length, visible_suffix_length, mask_char)`
2. 说明​：对数据进行部分遮蔽​，通常保留数据的部分前缀和/或后缀，中间用指定字符（如 `*`）填充。
3. 示例：
```sql {wrap}
SELECT MASK_PARTIAL(phone, 3, 4, '*') AS masked_phone FROM customers;
```

#### 4.4.3 MASK_NONE

1. 语法：`MASK_NONE(column_name)`
2. 说明​：不对数据做任何脱敏处理，直接返回原始值。通常用于白名单或测试场景。
3. 示例：
```sql {wrap}
SELECT MASK_NONE(salary) FROM sensitive_table;
```

## 5. 性能需求

加解密、编码函数对数据写入和查询有一定影响，在测试阶段，给出典型输出的写入和查询性能。如果写入延迟和查询延迟增加超过 100%，需优化代码。

## 6. 安全需求

1. SM4 的密钥保存在数据库中，已经进行加密。
2. 脱密函数需要和动态数据脱敏策略结合，实现基于角色的、精细化的访问控制，本期不实现，但留下接口。

## 7. 兼容性需求

1. 不涉及兼容性问题。
