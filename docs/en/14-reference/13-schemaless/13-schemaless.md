---
title: Schemaless Writing
description: This document describes how to use the schemaless write component of TDengine.
---

In IoT applications, data is collected for many purposes such as intelligent control, business analysis, device monitoring and so on. Due to changes in business or functional requirements or changes in device hardware, the application logic and even the data collected may change. Schemaless writing automatically creates storage structures for your data as it is being written to TDengine, so that you do not need to create supertables in advance. When necessary, schemaless writing will automatically add the required columns to ensure that the data written by the user is stored correctly.

The schemaless writing method creates super tables and their corresponding subtables. These are completely indistinguishable from the super tables and subtables created directly via SQL. You can write data directly to them via SQL statements. Note that the names of tables created by schemaless writing are based on fixed mapping rules for tag values, so they are not explicitly ideographic and they lack readability.

Note: Schemaless writing creates tables automatically. Creating tables manually is not supported with schemaless writing.

## Schemaless Writing Line Protocol

TDengine's schemaless writing line protocol supports InfluxDB's Line Protocol, OpenTSDB's telnet line protocol, and OpenTSDB's JSON format protocol. However, when using these three protocols, you need to specify in the API the standard of the parsing protocol to be used for the input content.

For the standard writing protocols of InfluxDB and OpenTSDB, please refer to the documentation of each protocol. The following is a description of TDengine's extended protocol, based on InfluxDB's line protocol first. They allow users to control the (super table) schema more granularly.

With the following formatting conventions, schemaless writing uses a single string to express a data row (multiple rows can be passed into the writing API at once to enable bulk writing).

```json
measurement,tag_set field_set timestamp
```

where:

- measurement will be used as the data table name. It will be separated from tag_set by a comma.
- `tag_set` will be used as tags, with format like `<tag_key>=<tag_value>,<tag_key>=<tag_value>` Enter a space between `tag_set` and `field_set`.
- `field_set`will be used as data columns, with format like `<field_key>=<field_value>,<field_key>=<field_value>` Enter a space between `field_set` and `timestamp`.
- `timestamp` is the primary key timestamp corresponding to this row of data

All data in tag_set is automatically converted to the NCHAR data type and does not require double quotes (").

In the schemaless writing data line protocol, each data item in the field_set needs to be described with its data type. Let's explain in detail:

- If there are English double quotes on both sides, it indicates the VARCHAR type. For example, `"abc"`.
- If there are double quotes on both sides and a L/l prefix, it means NCHAR type. For example, `L"error message"`.
- If there are double quotes on both sides and a G/g prefix, it means GEOMETRY type. For example `G"Point(4.343 89.342)"`.
- If there are double quotes on both sides and a B/b prefix, it means VARBINARY type. Hexadecimal start with \x or string can be used in double quotes. For example `B"\x98f46e"` `B"hello"`.
- Spaces, equals sign (=), comma (,), double quote ("), and backslash (\\) need to be escaped with a backslash (\\) in front. (All refer to the ASCII character). The rules are as follows:

| **Serial number** | **Element**    | **Escape characters**   |
| -------- | ----------- | -----------------------------       |
| 1        | Measurement     | Comma, Space                    |
| 2        | Tag key         | Comma, Equals Sign, Space       |
| 3        | Tag value       | Comma, Equals Sign, Space       |
| 4        | Field key       | Comma, Equals Sign, Space       |
| 5        | Field value     | Double quote, Backslash         |

With two contiguous backslashes, the first is interpreted as an escape character. Examples of backslash escape rules are as follows:

| **Serial number**      | **Backslashes**    | **Interpreted as**            |
| --------               | -----------        | ----------------------------- |
| 1                      | \                  | \                             |
| 2                      | \\\\               | \                             |
| 3                      | \\\\\\             | \\\\                          |
| 4                      | \\\\\\\\           | \\\\                          |
| 5                      | \\\\\\\\\\         | \\\\\\                        |
| 6                      | \\\\\\\\\\\\       | \\\\\\                        |

- Numeric types will be distinguished from data types by the suffix.

| **Serial number** | **Postfix** | **Mapping type**              | **Size (bytes)** |
| ----------------- | ----------- | ----------------------------- | ---------------- |
| 1                 | None or f64 | double                        | 8                |
| 2                 | f32         | float                         | 4                |
| 3                 | i8/u8       | TinyInt/UTinyInt              | 1                |
| 4                 | i16/u16     | SmallInt/USmallInt            | 2                |
| 5                 | i32/u32     | Int/UInt                      | 4                |
| 6                 | i64/i/u64/u | BigInt/BigInt/UBigInt/UBigInt | 8                |

- `t`, `T`, `true`, `True`, `TRUE`, `f`, `F`, `false`, and `False` will be handled directly as BOOL types.

For example, the following string indicates that the one row of data is written to the st supertable with the t1 tag as "3" (NCHAR), the t2 tag as "4" (NCHAR), and the t3 tag as "t3" (NCHAR); the c1 column is 3 (BIGINT), the c2 column is false (BOOL), the c3 column is "passit" (BINARY), the c4 column is 4 (DOUBLE), and the primary key timestamp is 1626006833639000000.

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4f64 1626006833639000000
```

Note that if the wrong case is used when describing the data type suffix, or if the wrong data type is specified for the data, it may cause an error message and cause the data to fail to be written.

## Main processing logic for schemaless writing

Schemaless writes process row data according to the following principles.

1. You can use the following rules to generate the subtable names: first, combine the measurement name and the key and value of the label into the next string:

   ```json
   "measurement,tag_key1=tag_value1,tag_key2=tag_value2"
   ```

   :::tip
   Note that tag_key1, tag_key2 are not the original order of the tags entered by the user but the result of using the tag names in ascending order of the strings. Therefore, tag_key1 is not the first tag entered in the line protocol.
   The string's MD5 hash value "md5_val" is calculated after the ranking is completed. The calculation result is then combined with the string to generate the table name: "t_md5_val". "t\_" is a fixed prefix that every table generated by this mapping relationship has.
   :::

   If you do not want to use an automatically generated table name, there are two ways to specify sub table names(the first one has a higher priority).
   
   1. You can configure smlAutoChildTableNameDelimiter in taos.cfg(except for `@ # space \r \t \n`).
      1. For example, `smlAutoChildTableNameDelimiter=tname`. You can insert `st,t0=cpul,t1=4 c1=3 1626006833639000000` and the table name will be cpu1-4.
   
   2. You can configure smlChildTableName in taos.cfg to specify table names.
      2. For example, `smlChildTableName=tname`. You can insert `st,tname=cpul,t1=4 c1=3 1626006833639000000` and the cpu1 table will be automatically created. Note that if multiple rows have the same tname but different tag_set values, the tag_set of the first row is used to create the table and the others are ignored.

2. If the super table obtained by parsing the line protocol does not exist, this super table is created.
   **Important:** Manually creating supertables for schemaless writing is not supported. Schemaless writing creates appropriate supertables automatically.

3. If the subtable obtained by the parse line protocol does not exist, Schemaless creates the sub-table according to the subtable name determined in steps 1 or 2.

4. If the specified tag or regular column in the data row does not exist, the corresponding tag or regular column is added to the super table (only incremental).

5. If there are some tag columns or regular columns in the super table that are not specified to take values in a data row, then the values of these columns are set to NULL.

6. For BINARY or NCHAR columns, if the length of the value provided in a data row exceeds the column type limit, the maximum length of characters allowed to be stored in the column is automatically increased (only incremented and not decremented) to ensure complete preservation of the data.

7. Errors encountered throughout the processing will interrupt the writing process and return an error code.

8. It is assumed that the order of field_set in a supertable is consistent, meaning that the first record contains all fields and subsequent records store fields in the same order. If the order is not consistent, set smlDataFormat in taos.cfg to false. Otherwise, data will be written out of order and a database error will occur.
   Note: TDengine 3.0.3.0 and later automatically detect whether order is consistent. This parameter is no longer used.
9. Due to the fact that SQL table names do not support period (.), schemaless has also processed period (.). If there is a period (.) in the table name automatically created by schemaless, it will be automatically replaced with an underscore (\_). If you manually specify a sub table name, if there is a dot (.) in the sub table name, it will also be converted to an underscore (\_)
10. Taos.cfg adds the configuration of smlTsDefaultName (with a string value), which only works on the client side. After configuration, the time column name of the schemaless automatic table creation can be set through this configuration. If not configured, defaults to _ts.
11. Super table name or child table name are case sensitive.
:::tip
All processing logic of schemaless will still follow TDengine's underlying restrictions on data structures, such as the total length of each row of data cannot exceed 48 KB(64 KB since version 3.0.5.0) and the total length of a tag value cannot exceed 16 KB. See [TDengine SQL Boundary Limits](../../taos-sql/limit) for specific constraints in this area.
:::

## Time resolution recognition

Three specified modes are supported in the schemaless writing process, as follows:

| **Serial** | **Value**           | **Description**        |
| ---------- | ------------------- | ---------------------- |
| 1          | SML_LINE_PROTOCOL   | InfluxDB Line Protocol |
| 2          | SML_TELNET_PROTOCOL | OpenTSDB file protocol |
| 3          | SML_JSON_PROTOCOL   | OpenTSDB JSON protocol |

In InfluxDB line protocol mode, you must specify the precision of the input timestamp. Valid precisions are described in the following table.

| **No.** | **Precision**                     | **Description**       |
| ------- | --------------------------------- | --------------------- |
| 1       | TSDB_SML_TIMESTAMP_NOT_CONFIGURED | Not defined (invalid) |
| 2       | TSDB_SML_TIMESTAMP_HOURS          | Hours                 |
| 3       | TSDB_SML_TIMESTAMP_MINUTES        | Minutes               |
| 4       | TSDB_SML_TIMESTAMP_SECONDS        | Seconds               |
| 5       | TSDB_SML_TIMESTAMP_MILLI_SECONDS  | Milliseconds          |
| 6       | TSDB_SML_TIMESTAMP_MICRO_SECONDS  | Microseconds          |
| 7       | TSDB_SML_TIMESTAMP_NANO_SECONDS   | Nanoseconds           |

In OpenTSDB file and JSON protocol modes, the precision of the timestamp is determined from its length in the standard OpenTSDB manner. User input is ignored.

## Data Model Mapping

This section describes how data in InfluxDB line protocol is mapped to a schema. The data measurement in each line is mapped to a supertable name. The tag name in tag_set is the tag name in the schema, and the name in field_set is the column name in the schema. The following example shows how data is mapped:

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4f64 1626006833639000000
```

This row is mapped to a supertable: `st` contains three NCHAR tags: t1, t2, and t3. Five columns are created: ts (timestamp), c1 (bigint), c3 (binary), c2 (bool), and c4 (bigint). The following SQL statement is generated:

```json
create stable st (_ts timestamp, c1 bigint, c2 bool, c3 binary(6), c4 bigint) tags(t1 nchar(1), t2 nchar(1), t3 nchar(2))
```

## Processing Schema Changes

This section describes the impact on the schema caused by different data being written.

If you use line protocol to write to a specific tag field and then later change the field type, a schema error will occur. This triggers an error on the write API. This is shown as follows:

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4    1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4i   1626006833640000000
```

The first row defines c4 as a double. However, in the second row, the suffix indicates that the value of c4 is a bigint. This causes schemaless writing to throw an error.

An error also occurs if data input into a binary column exceeds the defined length of the column.

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c5="pass"     1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c5="passit"   1626006833640000000
```

The first row defines c5 as a binary(4). but the second row writes 6 bytes to it. This means that the length of the binary column must be expanded to contain the data.

```json
st,t1=3,t2=4,t3=t3 c1=3i64               1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c6="passit"   1626006833640000000
```

The preceding data includes a new entry, c6, with type binary(6). When this occurs, a new column c6 with type binary(6) is added automatically.

## Write Integrity

TDengine guarantees the idempotency of data writes. This means that you can repeatedly call the API to perform write operations with bad data. However, TDengine does not guarantee the atomicity of multi-row writes. In a multi-row write, some data may be written successfully and other data unsuccessfully.

## Error Codes

The TSDB_CODE_TSC_LINE_SYNTAX_ERROR indicates an error in the schemaless writing component.
This error occurs when writing text. For other errors, schemaless writing uses the standard TDengine error codes
found in taos_errstr.
