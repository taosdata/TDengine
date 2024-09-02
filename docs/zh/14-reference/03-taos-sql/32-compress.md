---
title: 可配置压缩算法
sidebar_label: 可配置压缩
description: 可配置压缩算法
---

从 TDengine 3.3.0.0 版本开始，TDengine 提供了更高级的压缩功能，用户可以在建表时针对每一列配置是否进行压缩、以及使用的压缩算法和压缩级别。

## 压缩术语定义

### 压缩等级

- 一级压缩：对数据进行编码，本质也是一种压缩
- 二级压缩：在编码的基础上对数据块进行压缩

### 压缩级别

在本文中特指二级压缩算法内部的级别，比如zstd，至少8个level可选，每个level 下都有不同表现，本质是压缩率、压缩速度、解压速度之间的 tradeoff，为了避免选择困难，特简化定义为如下三种级别：

- high：压缩率最高，压缩速度和解压速度相对最差。
- low：压缩速度和解压速度最好，压缩率相对最低。
- medium：兼顾压缩率、压缩速度和解压速度。

### 压缩算法列表

- 编码算法列表（一级压缩):simple8b, bit-packing,delta-i, delta-d, disabled  

- 压缩算法列表(二级压缩): lz4、zlib、zstd、tsz、xz、disabled

- 各个数据类型的默认压缩算法列表和适用范围

| 数据类型 |   可选编码算法      |  编码算法默认值 | 可选压缩算法|压缩算法默认值| 压缩等级默认值|
| :-----------:|:----------:|:-------:|:-------:|:----------:|:----:|
|  tinyint/untinyint/smallint/usmallint/int/uint | simple8b| simple8b | lz4/zlib/zstd/xz| lz4 | medium|
|   bigint/ubigint/timestamp   |  simple8b/delta-i    | delta-i |lz4/zlib/zstd/xz | lz4| medium|
|float/double | delta-d|delta-d |lz4/zlib/zstd/xz/tsz|lz4| medium|
|binary/nchar| disabled| disabled|lz4/zlib/zstd/xz| lz4| medium|
|bool| bit-packing| bit-packing| lz4/zlib/zstd/xz| lz4| medium|

## SQL 语法

### 建表时指定压缩

```sql
CREATE [dbname.]tabname (colName colType [ENCODE 'encode_type'] [COMPRESS 'compress_type' [LEVEL 'level'], [, other cerate_definition]...])
```

**参数说明**

- tabname：超级表或者普通表名称
- encode_type: 一级压缩，具体参数见上面列表
- compress_type: 二级压缩,具体参数见上面列表
- level: 特指二级压缩的级别，默认值为medium, 支持简写为 'h'/'l'/'m'

**功能说明**

- 创建表的时候指定列的压缩方式

### 更改列的压缩方式

```sql
ALTER TABLE [db_name.]tabName MODIFY COLUMN colName [ENCODE 'ecode_type'] [COMPRESS 'compress_type'] [LEVEL "high"]

```

**参数说明**

- tabName: 表名，可以为超级表、普通表
- colName: 待更改压缩算法的列, 只能为普通列

**功能说明**

- 更改列的压缩方式

### 查看列的压缩方式

```sql
DESCRIBE [dbname.]tabName
```

**功能说明**

- 显示列的基本信息，包括类型、压缩方式

## 兼容性

- 完全兼容已经存在的数据
- 从更低版本升级到 3.3.0.0 后不能回退
