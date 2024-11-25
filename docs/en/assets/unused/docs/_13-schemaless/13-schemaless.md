---
title: Schemaless Writes
description: 'The Schemaless writing method allows for automatic creation of storage structures corresponding to data, eliminating the need to pre-create super tables/sub-tables.'
---

In IoT applications, it is common to collect a significant amount of data for smart control, business analysis, and device monitoring. Due to version upgrades in application logic or hardware adjustments in the devices, the data collection items may frequently change. To facilitate data recording in such situations, TDengine provides a Schemaless writing method that eliminates the need to pre-create super tables or sub-tables, allowing the data writing interface to automatically create storage structures corresponding to the data. Additionally, when necessary, Schemaless will automatically add required data columns to ensure that the user’s written data can be correctly stored.

Super tables and their corresponding sub-tables established through the schemaless writing method are identical to those created directly via SQL. You can also directly write data into them using SQL statements. However, it is important to note that the table names created through the schemaless writing method are generated based on the tag values according to a fixed mapping rule, making them less readable.

**Note:** Schemaless writing will automatically create tables, eliminating the need for manual creation. Manually creating tables may result in unknown errors.

## Schemaless Write Line Protocol

TDengine’s schemaless write line protocol is compatible with InfluxDB's line protocol, OpenTSDB's telnet line protocol, and OpenTSDB's JSON format protocol. However, when using these three protocols, it is necessary to specify the parsing protocol standard in the API.

For standard write protocols of InfluxDB and OpenTSDB, please refer to their respective documentation. Below, we first introduce the extended protocol content based on InfluxDB's line protocol, allowing users to control the (super table) schema in a more granular manner.

Schemaless uses a string to express a data row (multiple rows can be sent to the write API at once), with the following format:

```json
measurement,tag_set field_set timestamp
```

Where:

- **measurement** will serve as the table name, separated from the tag_set by a comma.
- **tag_set** serves as the tag data, formatted as `<tag_key>=<tag_value>,<tag_key>=<tag_value>`, allowing multiple tag data to be separated by commas. It is separated from field_set by a space.
- **field_set** serves as the ordinary column data, formatted as `<field_key>=<field_value>,<field_key>=<field_value>`, also separated by commas. It is separated from the timestamp by a space.
- **timestamp** is the primary key timestamp for this row of data.
- Schemaless writing does not support writing to tables with a secondary key column.

All data in the tag_set is automatically converted to the nchar data type and does not require double quotes (").

In the schemaless write data row protocol, each data item in the field_set needs to describe its data type. Specifically:

- If it is enclosed in double quotes, it represents a VARCHAR type, for example, `"abc"`.
- If it is enclosed in double quotes and prefixed with L or l, it represents an NCHAR type, for example, `L"error message"`.
- If it is enclosed in double quotes and prefixed with G or g, it represents a GEOMETRY type, for example, `G"Point(4.343 89.342)"`.
- If it is enclosed in double quotes and prefixed with B or b, it represents a VARBINARY type, with the contents possibly starting with \x for hexadecimal or strings, for example, `B"\x98f46e"` or `B"hello"`.
- Spaces, equal signs (=), commas (,), double quotes ("), and backslashes (\) need to be escaped with a backslash (\). The specific escape rules are as follows:

| **Index** | **Field**       | **Characters to Escape**             |
|-----------|-----------------|-------------------------------------|
| 1         | Super Table Name | Comma, space                        |
| 2         | Tag Name        | Comma, equal sign, space            |
| 3         | Tag Value       | Comma, equal sign, space            |
| 4         | Column Name     | Comma, equal sign, space            |
| 5         | Column Value    | Double quote, backslash             |

Two consecutive backslashes escape the first one; if there is only one backslash, it does not need to be escaped. Examples of backslash escaping rules are as follows:

| **Index** | **Backslash** | **Escapes to**                   |
|-----------|---------------|----------------------------------|
| 1         | \             | \                                |
| 2         | \\\\          | \                                |
| 3         | \\\\\\        | \\\\                             |
| 4         | \\\\\\\\      | \\\\                             |
| 5         | \\\\\\\\\\    | \\\\\\                           |
| 6         | \\\\\\\\\\\\  | \\\\\\                           |

- Numeric types will be distinguished by suffixes:

| **Index** | **Suffix** | **Mapped Type**               | **Size (Bytes)** |
|-----------|------------|-------------------------------|------------------|
| 1         | None or f64| double                        | 8                |
| 2         | f32        | float                         | 4                |
| 3         | i8/u8      | TinyInt/UTinyInt             | 1                |
| 4         | i16/u16    | SmallInt/USmallInt           | 2                |
| 5         | i32/u32    | Int/UInt                     | 4                |
| 6         | i64/i/u64/u| BigInt/BigInt/UBigInt/UBigInt | 8                |

- t, T, true, True, TRUE, f, F, false, False will be directly treated as BOOL type.

For example, the following data row indicates writing a data sub-table under the super table named `st`, with tag `t1` as "3" (NCHAR), tag `t2` as "4" (NCHAR), and tag `t3` as "t3" (NCHAR), writing column `c1` as 3 (BIGINT), column `c2` as false (BOOL), column `c3` as "passit" (BINARY), column `c4` as 4 (DOUBLE), with a primary key timestamp of 1626006833639000000.

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4f64 1626006833639000000
```

It is important to note that if the casing of the data type suffix is incorrect, or if the data type specified for the data is incorrect, an error may occur, leading to data write failures.

## Main Processing Logic of Schemaless Writes

Schemaless writes handle row data according to the following principles:

1. The sub-table name will be generated using the following rules: first, the measurement name and tag key-value pairs will be combined into a string:

   ```json
   "measurement,tag_key1=tag_value1,tag_key2=tag_value2"
   ```

   :::tip
   It is important to note that here, `tag_key1`, `tag_key2` are not in the original order input by the user but are the result of sorting the tag names in ascending order as strings. Therefore, `tag_key1` is not necessarily the first tag input in the row protocol.
   After sorting, the MD5 hash value "md5_val" of this string is calculated. The calculated result is then combined with the string to generate the table name: “t_md5_val”. The “t_” is a fixed prefix; every automatically generated table via this mapping will have this prefix.
   :::tip

   If you do not want to use the automatically generated table name, there are two ways to specify the sub-table name (the first method has higher priority):
   1. Configure the `smlAutoChildTableNameDelimiter` parameter in `taos.cfg` to specify it (excluding `@ # space newline tab`).
      1. For example, configure `smlAutoChildTableNameDelimiter=-`. When inserting data like `st,t0=cpu1,t1=4 c1=3 1626006833639000000`, the created table name will be `cpu1-4`.
   2. Configure the `smlChildTableName` parameter in `taos.cfg` to specify it.
      1. For example, configure `smlChildTableName=tname`. When inserting data like `st,tname=cpu1,t1=4 c1=3 1626006833639000000`, the created table name will be `cpu1`. Note that if multiple rows of data have the same `tname`, but the subsequent tag sets differ, only the first row's tag set will be used for automatic table creation; the others will be ignored.

2. If the super table obtained from parsing the row protocol does not exist, it will create this super table (it is not recommended to manually create super tables; otherwise, data insertion may behave abnormally).
3. If the sub-table obtained from parsing the row protocol does not exist, Schemaless will create the sub-table according to the name determined in steps 1 or 2.
4. If the tag or ordinary column specified in the data row does not exist, the corresponding tag or ordinary column will be added to the super table (only additions, no deletions).
5. If there are tag columns or ordinary columns in the super table that were not specified in a given data row, the values of those columns will be set to NULL for that row.
6. For BINARY or NCHAR columns, if the length of the provided value exceeds the column type's limit, the maximum character length that the column can store will be automatically increased (only additions).
7. Any errors encountered during this process will interrupt the write operation and return an error code.
8. To improve write efficiency, it is assumed by default that the order of `field_set` within the same super table is the same (the first piece of data includes all fields, and subsequent data follows this order). If the order differs, the `smlDataFormat` parameter should be configured to false; otherwise, data will be inserted in the same order, which could lead to inconsistencies in stored data. From version 3.0.3.0 onwards, automatic order detection will be implemented, and this configuration will be deprecated.
9. Since SQL table names do not support dots (.), the schemaless write also processes dots. If the automatically generated table name contains a dot (.), it will be replaced with an underscore (_). If a sub-table name is manually specified and contains a dot (.), it will also be converted to an underscore (_).
10. The `taos.cfg` file adds the `smlTsDefaultName` configuration (value as string) which takes effect only on the client side. After configuring this, the name of the timestamp column for automatically generated schemaless tables can be set using this configuration. If not configured, it defaults to `_ts`.
11. The names of super tables or sub-tables created through schemaless writing are case-sensitive.

:::tip
All processing logic of schemaless writing still adheres to the underlying limitations imposed by TDengine on data structures, such as the total length of each row of data not exceeding 48KB (increased to 64KB starting from version 3.0.5.0) and the total length of tag values not exceeding 16KB. For specific limitations and constraints, please refer to [TDengine SQL Boundary Limits](../../taos-sql/limit).

:::

## Time Resolution Identification

During the schemaless write process, three specified modes are supported, as follows:

| **Index** | **Value**              | **Description**                    |
|-----------|------------------------|------------------------------------|
| 1         | SML_LINE_PROTOCOL      | InfluxDB line protocol             |
| 2         | SML_TELNET_PROTOCOL    | OpenTSDB text line protocol        |
| 3         | SML_JSON_PROTOCOL      | JSON format protocol               |

In the SML_LINE_PROTOCOL parsing mode, users need to specify the time resolution of the input timestamp. The available time resolutions are as follows:

| **Index** | **Time Resolution Definition**            | **Meaning**        |
|-----------|-------------------------------------------|---------------------|
| 1         | TSDB_SML_TIMESTAMP_NOT_CONFIGURED        | Undefined (invalid) |
| 2         | TSDB_SML_TIMESTAMP_HOURS                 | Hour                |
| 3         | TSDB_SML_TIMESTAMP_MINUTES               | Minute              |
| 4         | TSDB_SML_TIMESTAMP_SECONDS                | Second              |
| 5         | TSDB_SML_TIMESTAMP_MILLI_SECONDS          | Millisecond         |
| 6         | TSDB_SML_TIMESTAMP_MICRO_SECONDS          | Microsecond         |
| 7         | TSDB_SML_TIMESTAMP_NANO_SECONDS           | Nanosecond          |

In SML_TELNET_PROTOCOL and SML_JSON_PROTOCOL modes, the timestamp's length determines its precision (the same as the standard operation method of OpenTSDB), at which point the user-specified time resolution will be ignored.

## Data Mode Mapping Rules

This section explains how data in InfluxDB line protocol maps to structured data. Each data measurement in the protocol maps to a super table name. The tag names in tag_set map to tag names in the data schema, and the field names in field_set map to column names. For example, consider the following data:

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4f64 1626006833639000000
```

This row of data maps to generate a super table: `st`, which contains three tags of type NCHAR: `t1`, `t2`, and `t3`. It also contains five data columns: `ts` (timestamp), `c1` (BIGINT), `c3` (BINARY), `c2` (BOOL), and `c4` (BIGINT). The mapping would result in the following SQL statement:

```json
create stable st (_ts timestamp, c1 bigint, c2 bool, c3 binary(6), c4 bigint) tags(t1 nchar(1), t2 nchar(1), t3 nchar(2))
```

## Handling Data Mode Changes

This section explains how changes in data mode are affected when writing different row data.

When writing a clearly defined field type with the line protocol, subsequent changes to that field's type definition will result in a clear data mode error, triggering the write API to report an error. As shown below:

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4    1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4i   1626006833640000000
```

In the first row, the data type mapping defines column `c4` as Double, but in the second row, the numeric suffix indicates that the column is BigInt, thus triggering a parsing error for the schemaless write.

If the previous row protocol declares the column as binary, and later rows require a longer binary length, this will trigger a change in the super table mode.

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c5="pass"     1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c5="passit"   1626006833640000000
```

In the first row, the row protocol parsing declares column `c5` as a binary(4) field; the second row will extract column `c5` as binary but with a width of 6. This requires increasing the width of the binary type to accommodate the new string length.

```json
st,t1=3,t2=4,t3=t3 c1=3i64               1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c6="passit"   1626006833640000000
```

The second row adds a column `c6` of type binary(6) compared to the first row. In this case, a column `c6` will be automatically added with a type of binary(6).

## Write Integrity

TDengine provides idempotence guarantees for data writes, meaning you can repeatedly call the API for erroneous data write operations. However, it does not guarantee the atomicity of multi-row data writes. In a batch writing process, some data may succeed while other data may fail.

## Error Codes

If there are errors in the data itself during the schemaless write process, the application will receive the error message TSDB_CODE_TSC_LINE_SYNTAX_ERROR, indicating that the error occurred in the written text. Other error codes are consistent with the original system, and specific error reasons can be obtained through `taos_errstr`.
