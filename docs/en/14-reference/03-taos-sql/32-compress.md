---
title: Data Compression
slug: /tdengine-reference/sql-manual/manage-data-compression
---

Starting from version 3.3.0.0, TDengine provides more advanced compression features, allowing users to configure whether to compress each column at the time of table creation, as well as the compression algorithm and compression level used.

## Compression Terminology Definition

### Compression Stages

- First-level compression: Encoding the data, which is essentially a form of compression.
- Second-level compression: Compressing data blocks on top of encoding.

### Compression Levels

In this document, it specifically refers to the internal levels of the second-level compression algorithms, such as zstd, with at least 8 levels available, each level having different performances. Essentially, it's a tradeoff between compression ratio, compression speed, and decompression speed. To avoid difficulty in choosing, it is simplified into the following three levels:

- high: Highest compression ratio, relatively worst compression and decompression speeds.
- low: Best compression and decompression speeds, relatively lowest compression ratio.
- medium: Balances compression ratio, compression speed, and decompression speed.

### List of Compression Algorithms

- Encoding algorithms list (First-level compression): simple8b, bit-packing, delta-i, delta-d, disabled, bss(byte-stream-split) 

- Compression algorithms list (Second-level compression): lz4, zlib, zstd, tsz, xz, disabled

- Default compression algorithms list and applicable range for each data type

| Data Type                            | Available Encoding Algorithms | Default Encoding Algorithm | Available Compression Algorithms | Default Compression Algorithm | Default Compression Level |
| :----------------------------------- | :---------------------------- | :------------------------- | :------------------------------- | :---------------------------- | :------------------------ |
| int/uint                             | disabled/simple8b             | simple8b                   | lz4/zlib/zstd/xz                 | lz4                           | medium                    |
| tinyint/untinyint/smallint/usmallint | disabled/simple8b             | simple8b                   | lz4/zlib/zstd/xz                 | zlib                          | medium                    |
| bigint/ubigint/timestamp             | disabled/simple8b/delta-i     | delta-i                    | lz4/zlib/zstd/xz                 | lz4                           | medium                    |
| float/double                         | disabled/delta-d/bss          | delta-d                    | lz4/zlib/zstd/xz/tsz             | lz4                           | medium                    |
| binary/nchar                         | disabled                      | disabled                   | lz4/zlib/zstd/xz                 | zstd                          | medium                    |
| bool                                 | disabled/bit-packing          | bit-packing                | lz4/zlib/zstd/xz                 | zstd                          | medium                    |
| decimal                              | disabled                      | disabled                   | lz4/zlib/zstd/xz                 | zstd                          | medium                    |

## SQL Syntax

### Specifying Compression When Creating Tables

```sql
CREATE [dbname.]tabname (colName colType [ENCODE 'encode_type'] [COMPRESS 'compress_type' [LEVEL 'level'], [, other create_definition]...])
```

Parameter Description:

- tabname: Name of the supertable or basic table
- encode_type: First-level compression, see the list above
- compress_type: Second-level compression, see the list above
- level: Specifically refers to the level of second-level compression, default is medium, supports abbreviation as 'h'/'l'/'m'

Function Description:

- Specify the compression method for columns when creating a table

### Changing the Compression Method of a Column

```sql
ALTER TABLE [db_name.]tabName MODIFY COLUMN colName [ENCODE 'ecode_type'] [COMPRESS 'compress_type'] [LEVEL "high"]

```

Parameter Description:

- tabName: Table name, can be a supertable or a basic table
- colName: Column for which the compression algorithm is to be changed, can only be a normal column

Function Description:

- Change the compression method of a column

### Viewing the Compression Method of a Column

```sql
DESCRIBE [dbname.]tabName
```

Function Description:

- Displays basic information of the column, including type and compression method

## Compatibility

- Fully compatible with existing data
- Cannot revert to a lower version after upgrading to 3.3.0.0
