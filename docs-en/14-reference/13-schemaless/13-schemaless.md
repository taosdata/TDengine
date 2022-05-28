---
title: Schemaless Writing
description: "The Schemaless write method eliminates the need to create super tables/sub tables in advance and automatically creates the storage structure corresponding to the data as it is written to the interface."
---

In IoT applications, many data items are often collected for intelligent control, business analysis, device monitoring, etc. Due to the version upgrades of the application logic, or the hardware adjustment of the devices themselves, the data collection items may change frequently. To facilitate the data logging work in such cases, TDengine starting from version 2.2.0.0 provides a series of interfaces to the schemaless writing method, which eliminate the need to create super tables and subtables in advance by automatically creating the storage structure corresponding to the data as the data is written to the interface. And when necessary, schemaless writing will automatically add the required columns to ensure that the data written by the user is stored correctly.

The schemaless writing method creates super tables and their corresponding subtables completely indistinguishable from the super tables and subtables created directly via SQL. You can write data directly to them via SQL statements. Note that the names of tables created by schemaless writing are based on fixed mapping rules for tag values, so they are not explicitly ideographic and lack readability.

## Schemaless Writing Line Protocol

TDengine's schemaless writing line protocol supports InfluxDB's Line Protocol, OpenTSDB's telnet line protocol, and OpenTSDB's JSON format protocol. However, when using these three protocols, you need to specify in the API the standard of the parsing protocol to be used for the input content.

For the standard writing protocols of InfluxDB and OpenTSDB, please refer to the documentation of each protocol. The following is a description of TDengine's extended protocol, based on InfluxDB's line protocol first. They allow users to control the (super table) schema more granularly.

With the following formatting conventions, schemaless writing uses a single string to express a data row (multiple rows can be passed into the writing API at once to enable bulk writing).

```json
measurement,tag_set field_set timestamp
```

where :

- measurement will be used as the data table name. It will be separated from tag_set by a comma.
- tag_set will be used as tag data in the format `<tag_key>=<tag_value>,<tag_key>=<tag_value>`, i.e. multiple tags' data can be separated by a comma. It is separated from field_set by space.
- field_set will be used as normal column data in the format of `<field_key>=<field_value>,<field_key>=<field_value>`, again using a comma to separate multiple normal columns of data. It is separated from the timestamp by a space.
- The timestamp is the primary key corresponding to the data in this row.

All data in tag_set is automatically converted to the NCHAR data type and does not require double quotes (").

In the schemaless writing data line protocol, each data item in the field_set needs to be described with its data type. Let's explain in detail:

- If there are English double quotes on both sides, it indicates the BINARY(32) type. For example, `"abc"`.
- If there are double quotes on both sides and an L prefix, it means NCHAR(32) type. For example, `L"error message"`.
- Spaces, equal signs (=), commas (,), and double quotes (") need to be escaped with a backslash (\\) in front. (All refer to the ASCII character)
- Numeric types will be distinguished from data types by the suffix.

| **Serial number** | **Postfix** | **Mapping type** | **Size (bytes)** |
| -------- | -------- | ------------ | -------------- |
| 1 | none or f64 | double | 8 |
| 2 | f32 | float | 4 |
| 3 | i8 | TinyInt | 1 |
| 4 | i16 | SmallInt | 2 |
| 5 | i32 | Int | 4 |
| 6 | i64 or i | Bigint | 8 |

- `t`, `T`, `true`, `True`, `TRUE`, `f`, `F`, `false`, and `False` will be handled directly as BOOL types.

For example, the following data rows indicate that the t1 label is "3" (NCHAR), the t2 label is "4" (NCHAR), and the t3 label is "t3" to the super table named `st` labeled "t3" (NCHAR), write c1 column as 3 (BIGINT), c2 column as false (BOOL), c3 column is "passit" (BINARY), c4 column is 4 (DOUBLE), and the primary key timestamp is 1626006833639000000 in one row.

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4f64 1626006833639000000
```

Note that if the wrong case is used when describing the data type suffix, or if the wrong data type is specified for the data, it may cause an error message and cause the data to fail to be written.

## Main processing logic for schemaless writing

Schemaless writes process row data according to the following principles.

1. You can use the following rules to generate the subtable names: first, combine the measurement name and the key and value of the label  into the next string:

```json
"measurement,tag_key1=tag_value1,tag_key2=tag_value2"
```

Note that tag_key1, tag_key2 are not the original order of the tags entered by the user but the result of using the tag names in ascending order of the strings. Therefore, tag_key1 is not the first tag entered in the line protocol.
The string's MD5 hash value "md5_val" is calculated after the ranking is completed. The calculation result is then combined with the string to generate the table name: "t_md5_val". "t*" is a fixed prefix that every table generated by this mapping relationship has.

2. If the super table obtained by parsing the line protocol does not exist, this super table is created.
If the subtable obtained by the parse line protocol does not exist, Schemaless creates the sub-table according to the subtable name determined in steps 1 or 2.
4. If the specified tag or regular column in the data row does not exist, the corresponding tag or regular column is added to the super table (only incremental).
5. If there are some tag columns or regular columns in the super table that are not specified to take values in a data row, then the values of these columns are set to NULL.
6. For BINARY or NCHAR columns, if the length of the value provided in a data row exceeds the column type limit, the maximum length of characters allowed to be stored in the column is automatically increased (only incremented and not decremented) to ensure complete preservation of the data.
7. If the specified data subtable already exists, and the specified tag column takes a value different from the saved value this time, the value in the latest data row overwrites the old tag column take value.
8. Errors encountered throughout the processing will interrupt the writing process and return an error code.

:::tip
All processing logic of schemaless will still follow TDengine's underlying restrictions on data structures, such as the total length of each row of data cannot exceed
16k bytes. See [TAOS SQL Boundary Limits](/taos-sql/limit) for specific constraints in this area.
:::

## Time resolution recognition

Three specified modes are supported in the schemaless writing process, as follows:

| **Serial** | **Value** | **Description** |
| -------- | ------------------- | ------------------------------- |
| 1 | SML_LINE_PROTOCOL | InfluxDB Line Protocol |
| 2 | SML_TELNET_PROTOCOL | OpenTSDB Text Line Protocol | | 2 | SML_TELNET_PROTOCOL | OpenTSDB Text Line Protocol
| 3 | SML_JSON_PROTOCOL | JSON protocol format |

In the SML_LINE_PROTOCOL parsing mode, the user is required to specify the time resolution of the input timestamp. The available time resolutions are shown in the following table.

| **Serial Number** | **Time Resolution Definition** | **Meaning** |
| -------- | --------------------------------- | -------------- |
| 1 | TSDB_SML_TIMESTAMP_NOT_CONFIGURED | Not defined (invalid) |
| 2 | TSDB_SML_TIMESTAMP_HOURS | hour |
| 3 | TSDB_SML_TIMESTAMP_MINUTES | MINUTES
| 4 | TSDB_SML_TIMESTAMP_SECONDS | SECONDS
| 5 | TSDB_SML_TIMESTAMP_MILLI_SECONDS | milliseconds
| 6 | TSDB_SML_TIMESTAMP_MICRO_SECONDS | microseconds
| 7 | TSDB_SML_TIMESTAMP_NANO_SECONDS | nanoseconds |

In SML_TELNET_PROTOCOL and SML_JSON_PROTOCOL modes, the time precision is determined based on the length of the timestamp (in the same way as the OpenTSDB standard operation), and the user-specified time resolution is ignored at this point.

## Data schema mapping rules

This section describes how data for line protocols are mapped to data with a schema. The data measurement in each line protocol is mapped to
The tag name in tag_set is the name of the tag in the data schema, and the name in field_set is the column's name. The following data is used as an example to illustrate the mapping rules.

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4f64 1626006833639000000
```

The row data mapping generates a super table: `st`, which contains three labels of type NCHAR: t1, t2, t3. Five data columns are ts (timestamp), c1 (bigint), c3 (binary), c2 (bool), c4 (bigint). The mapping becomes the following SQL statement.

```json
create stable st (_ts timestamp, c1 bigint, c2 bool, c3 binary(6), c4 bigint) tags(t1 nchar(1), t2 nchar(1), t3 nchar(2))
```

## Data schema change handling

This section describes the impact on the data schema for different line protocol data writing cases.

When writing to an explicitly identified field type using the line protocol, subsequent changes to the field's type definition will result in an explicit data schema error, i.e., will trigger a write API report error. As shown below, the

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4 1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4i 1626006833640000000
```

The data type mapping in the first row defines column c4 as DOUBLE, but the data in the second row is declared as BIGINT by the numeric suffix, which triggers a parsing error with schemaless writing.

If the line protocol before the column declares the data column as BINARY, the subsequent one requires a longer binary length, which triggers a super table schema change.

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c5="pass" 1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c5="passit" 1626006833640000000
```

The first line of the line protocol parsing will declare column c5 is a BINARY(4) field, the second line data write will extract column c5 is still a BINARY column. Still, its width is 6, then you need to increase the width of the BINARY field to be able to accommodate the new string.

```json
st,t1=3,t2=4,t3=t3 c1=3i64 1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c6="passit" 1626006833640000000
```

The second line of data has an additional column c6 of type BINARY(6) compared to the first row. Then a column c6 of type BINARY(6) is automatically added at this point.

## Write integrity

TDengine provides idempotency guarantees for data writing, i.e., you can repeatedly call the API to write data with errors. However, it does not give atomicity guarantees for writing multiple rows of data. During the process of writing numerous rows of data in one batch, some data will be written successfully, and some data will fail.

## Error code

If it is an error in the data itself during the schemaless writing process, the application will get `TSDB_CODE_TSC_LINE_SYNTAX_ERROR` error message, which indicates that the error occurred in writing. The other error codes are consistent with the TDengine and can be obtained via the `taos_errstr()` to get the specific cause of the error.
