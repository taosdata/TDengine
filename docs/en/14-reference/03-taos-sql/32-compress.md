---
title: Manage Data Compression
description: Configurable compression algorithms
slug: /tdengine-reference/sql-manual/manage-data-compression
---

Starting from TDengine version 3.3.0.0, TDengine provides advanced compression features, allowing users to configure whether to compress each column, the compression algorithm used, and the compression level at the time of table creation.

## Compression Terminology Definition

### Compression Level

- **Level 1 Compression**: Data is encoded, which is essentially a form of compression.
- **Level 2 Compression**: Compresses data blocks based on encoding.

### Compression Levels

In this article, the term specifically refers to the levels within the secondary compression algorithms, such as zstd, where at least 8 levels are available. Each level has different performance characteristics, essentially balancing compression ratio, compression speed, and decompression speed. To simplify selection, we define three levels:

- **High**: Highest compression ratio, but relatively poorer compression and decompression speeds.
- **Low**: Best compression and decompression speeds, but relatively lower compression ratio.
- **Medium**: Balances compression ratio, compression speed, and decompression speed.

### Compression Algorithm List

- **Encoding Algorithms List (Level 1 Compression)**: simple8b, bit-packing, delta-i, delta-d, disabled.

- **Compression Algorithms List (Level 2 Compression)**: lz4, zlib, zstd, tsz, xz, disabled.

- Default compression algorithms and applicable ranges for various data types:

| Data Type |   Optional Encoding Algorithms      |  Default Encoding Algorithm | Optional Compression Algorithms | Default Compression Algorithm | Default Compression Level |
| :-----------:|:----------:|:-------:|:-------:|:----------:|:----:|
|  tinyint/untinyint/smallint/usmallint/int/uint | simple8b| simple8b | lz4/zlib/zstd/xz| lz4 | medium|
|   bigint/ubigint/timestamp   |  simple8b/delta-i    | delta-i | lz4/zlib/zstd/xz | lz4| medium|
| float/double | delta-d|delta-d | lz4/zlib/zstd/xz/tsz|lz4| medium|
| binary/nchar| disabled| disabled| lz4/zlib/zstd/xz| lz4| medium|
| bool| bit-packing| bit-packing| lz4/zlib/zstd/xz| lz4| medium|

## SQL Syntax

### Specify Compression When Creating a Table

```sql
CREATE [dbname.]tabname (colName colType [ENCODE 'encode_type'] [COMPRESS 'compress_type' [LEVEL 'level'], [, other cerate_definition]...])
```

**Parameter Description**

- `tabname`: Name of the supertable or basic table.
- `encode_type`: Level 1 compression, see the list above for specific parameters.
- `compress_type`: Level 2 compression, see the list above for specific parameters.
- `level`: Specifically refers to the level of the secondary compression, default value is medium, supports shorthand 'h'/'l'/'m'.

**Function Description**

- Specify the compression method for columns when creating the table.

### Change Column Compression Method

```sql
ALTER TABLE [db_name.]tabName MODIFY COLUMN colName [ENCODE 'ecode_type'] [COMPRESS 'compress_type'] [LEVEL "high"];
```

**Parameter Description**

- `tabName`: Table name, can be a supertable or basic table.
- `colName`: The column whose compression algorithm is to be changed; can only be an ordinary column.

**Function Description**

- Change the compression method for a column.

### View Column Compression Method

```sql
DESCRIBE [dbname.]tabName;
```

**Function Description**

- Displays basic information about the column, including type and compression method.

## Compatibility

- Fully compatible with existing data.
- Cannot revert back after upgrading from a lower version to 3.3.0.0.
