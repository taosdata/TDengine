---
sidebar_label: Data Compression
title: Data Compression
description: Configure per-column encoding and compression algorithms
---

Starting from version 3.3.0.0, TDengine provides more advanced compression features, allowing users to configure whether to compress each column at the time of table creation, as well as the compression algorithm and compression level used.

## Compression Terminology

### Compression Stages

- First-level compression: Encoding the data, which is essentially a form of compression.
- Second-level compression: Compressing data blocks on top of encoding.

### Compression Levels

In this document, it specifically refers to the internal levels of the second-level compression algorithms, such as zstd, with at least 8 levels available, each level having different performances. Essentially, it's a tradeoff between compression ratio, compression speed, and decompression speed. To avoid difficulty in choosing, it is simplified into the following three levels:

- `high`: Highest compression ratio, relatively worst compression and decompression speeds.
- `low`: Best compression and decompression speeds, relatively lowest compression ratio.
- `medium`: Balances compression ratio, compression speed, and decompression speed.

## Algorithms and Defaults

- Encoding algorithms (first-level compression): `simple8b`, `bit-packing`, `delta-i`, `delta-d`, `disabled`, `bss` (byte-stream-split)
- Compression algorithms (second-level compression): `lz4`, `zlib`, `zstd`, `tsz`, `xz`, `disabled`

Available algorithms and defaults for each data type:

| Data Type | Available Encoding Algorithms | Default Encoding Algorithm | Available Compression Algorithms | Default Compression Algorithm | Default Compression Level |
| --- | --- | --- | --- | --- | --- |
| `INT` / `UINT` | `disabled` / `simple8b` | `simple8b` | `lz4` / `zlib` / `zstd` / `xz` | `lz4` | `medium` |
| `TINYINT` / `UTINYINT` / `SMALLINT` / `USMALLINT` | `disabled` / `simple8b` | `simple8b` | `lz4` / `zlib` / `zstd` / `xz` | `zlib` | `medium` |
| `BIGINT` / `UBIGINT` | `disabled` / `simple8b` / `delta-i` | `simple8b` | `lz4` / `zlib` / `zstd` / `xz` | `lz4` | `medium` |
| `TIMESTAMP` | `disabled` / `delta-i` | `delta-i` | `lz4` / `zlib` / `zstd` / `xz` | `lz4` | `medium` |
| `FLOAT` / `DOUBLE` | `disabled` / `delta-d` / `bss` | `bss` | `lz4` / `zlib` / `zstd` / `xz` / `tsz` | `lz4` | `medium` |
| `BINARY` / `NCHAR` | `disabled` | `disabled` | `lz4` / `zlib` / `zstd` / `xz` | `zstd` | `medium` |
| `BOOL` | `disabled` / `bit-packing` | `bit-packing` | `lz4` / `zlib` / `zstd` / `xz` | `zstd` | `medium` |
| `DECIMAL` | `disabled` | `disabled` | `lz4` / `zlib` / `zstd` / `xz` | `zstd` | `medium` |

`tsz` applies only to `FLOAT` and `DOUBLE`.

## SQL Syntax

### Specifying Compression When Creating Tables

```sql
CREATE TABLE [db_name.]tb_name (
    col_name col_type
        [ENCODE 'encode_type']
        [COMPRESS 'compress_type']
        [LEVEL 'level']
    [, ...]
);
```

Supertable column definitions also support `ENCODE`, `COMPRESS`, and `LEVEL`; see [Create a Supertable](../02-ddl/03-stable.md#create-a-supertable).

**Parameter Description**

- `tb_name`: Name of the basic table or supertable
- `encode_type`: First-level compression (encoding); see the list above
- `compress_type`: Second-level compression; see the list above
- `level`: Second-level compression level; default is `medium`; also supports abbreviations `'h'` / `'l'` / `'m'`

### Changing the Compression Method of a Column

```sql
ALTER TABLE [db_name.]tb_name MODIFY COLUMN col_name
    [ENCODE 'encode_type']
    [COMPRESS 'compress_type']
    [LEVEL 'level'];
```

**Parameter Description**

- `tb_name`: Table name; can be a supertable or a basic table
- `col_name`: Column whose compression settings will change; can only be a normal column

### Viewing the Compression Method of a Column

```sql
DESCRIBE [db_name.]tb_name;
```

`DESCRIBE` shows basic column information, including type and compression settings.

## Compatibility

- Fully compatible with existing data
- Cannot revert to a lower version after upgrading to 3.3.0.0
