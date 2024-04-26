
---

title: Configurable Storage Compression
description: Configurable column storage compression method
---

# Configurable Storage Compression

## Compression Terminology Definition

### Compression Level Definition

- Level 1 Compression: Encoding the data, which is essentially a form of compression
- Level 2 Compression: Compressing data blocks.

### Compression Algorithm Level

In this article, it specifically refers to the level within the secondary compression algorithm, such as zstd, at least 8 levels can be selected, each level has different performance, essentially it is a tradeoff between compression ratio, compression speed, and decompression speed. To avoid the difficulty of choice, it is simplified and defined as the following three levels:

- high: The highest compression ratio, the worst compression speed and decompression speed.
- low: The best compression speed and decompression speed, the lowest compression ratio.
- medium: Balancing compression ratio, compression speed, and decompression speed.

### Compression Algorithm List

- Encoding algorithm list (Level 1 compression): simple8b, bit-packing, delta-i, delta-d, disabled  

- Compression algorithm list (Level 2 compression): lz4, zlib, zstd, tsz, xz, disabled

- Default compression algorithm list and applicable range for each data type

| Data Type |   Optional Encoding Algorithm      |  Default Encoding Algorithm  | Optional Compression Algorithm|Default Compression Algorithm| Default Compression Level|  
| :-----------:|:----------:|:-------:|:-------:|:----------:|:----:|
  tinyint/untinyint/smallint/usmallint/int/uint | simple8b| simple8b | lz4/zlib/zstd/xz| lz4 | medium|
|   bigint/ubigint/timestamp   |  simple8b/delta-i    | delta-i |lz4/zlib/zstd/xz | lz4| medium|
|float/double | delta-d|delta-d |lz4/zlib/zstd/xz/tsz|tsz| medium|
|binary/nchar| disabled| disabled|lz4/zlib/zstd/xz| lz4| medium|
|bool| bit-packing| bit-packing| lz4/zlib/zstd/xz| lz4| medium|

Note: For floating point types, if configured as tsz, its precision is determined by the global configuration of taosd. If configured as tsz, but the lossy compression flag is not configured, lz4 is used for compression by default.

## SQL 语法

### Specify the compression method when creating a table

```
CREATE [dbname.]tabname (colName colType [ENCODE 'encode_type'] [COMPRESS 'compress_type' [LEVEL 'level'], [, other cerate_definition]...])
```

**Parameter Description**

- tabname: Super table or ordinary table name
- encode_type: Level 1 compression, specific parameters see the above list
- compress_type: Level 2 compression, specific parameters see the above list
- level: Specifically refers to the level of secondary compression, the default value is medium, supports abbreviation as 'h'/'l'/'m'

**Function Description**

- Specify the compression method for the column when creating a table

### Change the compression method of the column

```
ALTER TABLE [db_name.]tabName MODIFY COLUMN colName [ENCODE 'ecode_type'] [COMPRESS 'compress_type'] [LEVEL "high"]
```

**Parameter Description**

- tabName: Table name, can be a super table or an ordinary table
- colName: The column to change the compression algorithm, can only be a normal column

**Function Description**

- Change the compression method of the column

### View the compression method of the column

```
DESCRIBE [dbname.]tabName
```

**Function Description**

- Display basic information of the column, including type and compression method

## Compatibility

- Fully compatible with existing data
- Does not support rollback
