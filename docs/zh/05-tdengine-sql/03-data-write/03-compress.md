---
title: 压缩配置
sidebar_label: 压缩配置
description: 按列配置数据编码与压缩算法
---

TDengine 支持细粒度的压缩配置。建表时可针对每一列指定是否压缩，以及使用的编码算法、压缩算法和压缩级别。

## 压缩术语

### 压缩等级

- 一级压缩：对数据进行编码，本质上也是一种压缩。
- 二级压缩：在编码结果基础上，对数据块再进行压缩。

### 压缩级别

本文中的压缩级别特指二级压缩算法内部的级别。例如 `zstd` 至少提供 8 个 `level` 可选，不同级别在压缩率、压缩速度和解压速度之间权衡不同。为简化选择，TDengine 将其归纳为以下三种级别：

- `high`：压缩率最高，压缩速度和解压速度相对最差。
- `low`：压缩速度和解压速度最好，压缩率相对最低。
- `medium`：兼顾压缩率、压缩速度和解压速度。

## 算法与默认值

- 编码算法（一级压缩）：`simple8b`、`bit-packing`、`delta-i`、`delta-d`、`disabled`、`bss`（byte-stream-split）
- 压缩算法（二级压缩）：`lz4`、`zlib`、`zstd`、`tsz`、`xz`、`disabled`

各数据类型可用的算法及默认值如下：

| 数据类型 | 可选编码算法 | 编码默认值 | 可选压缩算法 | 压缩默认值 | 压缩级别默认值 |
| --- | --- | --- | --- | --- | --- |
| `INT` / `UINT` | `disabled` / `simple8b` | `simple8b` | `lz4` / `zlib` / `zstd` / `xz` | `lz4` | `medium` |
| `TINYINT` / `UTINYINT` / `SMALLINT` / `USMALLINT` | `disabled` / `simple8b` | `simple8b` | `lz4` / `zlib` / `zstd` / `xz` | `zlib` | `medium` |
| `BIGINT` / `UBIGINT` | `disabled` / `simple8b` / `delta-i` | `simple8b` | `lz4` / `zlib` / `zstd` / `xz` | `lz4` | `medium` |
| `TIMESTAMP` | `disabled` / `delta-i` | `delta-i` | `lz4` / `zlib` / `zstd` / `xz` | `lz4` | `medium` |
| `FLOAT` / `DOUBLE` | `disabled` / `delta-d` / `bss` | `bss` | `lz4` / `zlib` / `zstd` / `xz` / `tsz` | `lz4` | `medium` |
| `BINARY` / `NCHAR` | `disabled` | `disabled` | `lz4` / `zlib` / `zstd` / `xz` | `zstd` | `medium` |
| `BOOL` | `disabled` / `bit-packing` | `bit-packing` | `lz4` / `zlib` / `zstd` / `xz` | `zstd` | `medium` |
| `DECIMAL` | `disabled` | `disabled` | `lz4` / `zlib` / `zstd` / `xz` | `zstd` | `medium` |

其中，`tsz` 仅适用于 `FLOAT` 和 `DOUBLE`。

## 语法

### 建表时指定压缩

```sql
CREATE TABLE [db_name.]tb_name (
    col_name col_type
        [ENCODE 'encode_type']
        [COMPRESS 'compress_type']
        [LEVEL 'level']
    [, ...]
);
```

超级表语法中同样支持在列定义上指定 `ENCODE`、`COMPRESS` 和 `LEVEL`，参见 [超级表](../02-ddl/03-stable.md#创建超级表)。

**参数说明**

- `tb_name`：普通表或超级表名称。
- `encode_type`：一级压缩（编码）算法，取值见上文算法列表。
- `compress_type`：二级压缩算法，取值见上文算法列表。
- `level`：二级压缩级别，默认值为 `medium`，也支持简写为 `'h'`、`'l'`、`'m'`。

### 修改列的压缩方式

```sql
ALTER TABLE [db_name.]tb_name MODIFY COLUMN col_name
    [ENCODE 'encode_type']
    [COMPRESS 'compress_type']
    [LEVEL 'level'];
```

**参数说明**

- `tb_name`：表名，可以是超级表或普通表。
- `col_name`：待修改压缩配置的列，只能是普通列。

### 查看列的压缩方式

```sql
DESCRIBE [db_name.]tb_name;
```

`DESCRIBE` 会显示列的基本信息，包括类型和压缩配置。

## 兼容性

- 完全兼容升级前已存在的数据。
- 从更低版本升级到 `v3.3.0.0` 及之后版本后，不能再回退到更低版本。
