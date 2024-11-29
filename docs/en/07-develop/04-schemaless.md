---
title: Ingesting Data in Schemaless Mode
sidebar_label: Schemaless Ingestion
slug: /developer-guide/schemaless-ingestion
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

In Internet of Things (IoT) applications, it is often necessary to collect a large number of data points to achieve various functionalities such as automated management, business analysis, and device monitoring. However, due to reasons like version upgrades of application logic and adjustments in the hardware of devices, the data collection items may change frequently. To address this challenge, TDengine provides a schemaless writing mode aimed at simplifying the data recording process.

By using schemaless writing, users do not need to pre-create supertables or subtables, as TDengine automatically creates the corresponding storage structure based on the actual data written. Additionally, when necessary, schemaless writing can also automatically add required data columns or tag columns to ensure that the data written by users can be stored correctly.

It is worth noting that supertables and their corresponding subtables created through schemaless writing are functionally indistinguishable from those created directly via SQL. Users can still use SQL to write data into these tables directly. However, the table names generated through schemaless writing are based on the tag values and are created according to a fixed mapping rule, which may result in names that lack readability and are not easy to understand.

**When using schemaless writing, tables are automatically created, and there is no need to manually create tables. Manual table creation may result in unknown errors.**

## Schemaless Writing Line Protocol

TDengine's schemaless writing line protocol is compatible with InfluxDB's line protocol, OpenTSDB's telnet line protocol, and OpenTSDB's JSON format protocol. For the standard writing protocols of InfluxDB and OpenTSDB, please refer to their respective official documentation.

The following introduces the protocol based on InfluxDB's line protocol, along with the extensions made by TDengine. This protocol allows users to control (supertable) schemas in a more granular way. A string can express a data row, and multiple rows can be passed to the writing API at once as multiple strings. The format is specified as follows.

```text
measurement,tag_set field_set timestamp
```

The parameters are described as follows:

- measurement is the table name, separated from tag_set by a comma.
- tag_set has the format `<tag_key>=<tag_value>, <tag_key>=<tag_value>`, indicating the tag column data, separated by commas and space from field_set.
- field_set has the format `<field_key>=<field_value>, <field_key>=<field_value>`, indicating the ordinary columns, also separated by commas and space from the timestamp.
- timestamp is the primary key timestamp corresponding to this data row.
- Schemaless writing does not support writing data to tables with a second primary key column.

All data in tag_set is automatically converted to the nchar data type and does not require double quotes.
In the schemaless writing data row protocol, each data item in field_set needs to describe its own data type with specific requirements as follows:

- If surrounded by double quotes, it indicates varchar type, e.g., "abc".
- If surrounded by double quotes and prefixed with L or l, it indicates nchar type, e.g., L" error message ".
- If surrounded by double quotes and prefixed with G or g, it indicates geometry type, e.g., G"Point(4.343 89.342)".
- If surrounded by double quotes and prefixed with B or b, it indicates varbinary type, where the quoted string can start with \x for hexadecimal or be a regular string, e.g., B"\x98f46e" and B"hello".
- For characters like spaces, equals sign (=), commas (,), double quotes ("), and backslashes (\), the preceding backslash (\) must be used for escaping (all are in English half-width symbols). The domain escaping rules for the schemaless writing protocol are shown in the following table.

| **No.** | **Domain**   | **Need to Escape Characters**   |
| -------- | -------- | ---------------- |
| 1        | Supertable Name | Comma, space       |
| 2        | Tag Name   | Comma, equals sign, space |
| 3        | Tag Value   | Comma, equals sign, space |
| 4        | Column Name     | Comma, equals sign, space |
| 5        | Column Value     | Double quotes, backslash   |

If two consecutive backslashes are used, the first backslash acts as an escape character; if only one backslash is used, it does not need to be escaped. The backslash escape rules for the schemaless writing protocol are shown in the following table.

| **No.** | **Backslash**   | **Escaped as** |
| -------- | ------------ | ---------- |
| 1        | \            | \          |
| 2        | \\\\         | \          |
| 3        | \\\\\\       | \\\\       |
| 4        | \\\\\\\\     | \\\\       |
| 5        | \\\\\\\\\\   | \\\\\\     |
| 6        | \\\\\\\\\\\\ | \\\\\\     |

Numerical types will be distinguished by suffixes. The numerical type escape rules for the schemaless writing protocol are shown in the following table.

| **No.** | **Suffix**    | **Mapped Type**                  | **Size (bytes)** |
| -------- | ----------- | ----------------------------- | -------------- |
| 1        | None or f64    | double                        | 8              |
| 2        | f32         | float                         | 4              |
| 3        | i8/u8       | TinyInt/UTinyInt              | 1              |
| 4        | i16/u16     | SmallInt/USmallInt            | 2              |
| 5        | i32/u32     | Int/UInt                      | 4              |
| 6        | i64/i/u64/u | BigInt/BigInt/UBigInt/UBigInt | 8              |

- t, T, true, True, TRUE, f, F, false, False will be treated directly as BOOL type.

For example, the following data row represents: writing to a subtable under the supertable named `st`, with tags t1 as "3" (NCHAR), t2 as "4" (NCHAR), and t3 as "t3" (NCHAR), writing column c1 as 3 (BIGINT), column c2 as false (BOOL), column c3 as "passit" (BINARY), and column c4 as 4 (DOUBLE), with a primary key timestamp of 1626006833639000000.

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4f64 1626006833639000000
```

It is important to note that errors in case sensitivity when specifying data type suffixes, or incorrect specifications for data types, can lead to error messages and cause data writing to fail.

TDengine provides idempotency guarantees for data writing, meaning you can repeatedly call the API to write erroneous data. However, it does not provide atomicity guarantees for writing multiple rows of data. This means that during a batch write process, some data may be successfully written while other data may fail.

## Schemaless Writing Processing Rules

Schemaless writing processes row data according to the following principles:

1. The following rules will be used to generate subtable names: first, combine the measurement name and the tag key and value into the following string.

   ```json
   "measurement,tag_key1=tag_value1,tag_key2=tag_value2"
   ```

   - It should be noted that tag_key1, tag_key2 here are not in the original order of user-input tags, but are arranged in alphabetical order based on tag names. Therefore, tag_key1 is not the first tag input in the row protocol.
   After arrangement, the MD5 hash value "md5_val" of the string is calculated. Then, the result is combined with the string to generate the table name: "t_md5_val". The prefix "t_" is fixed, and every table generated by this mapping relationship has this prefix.

   - If you do not want to use the automatically generated table name, there are two ways to specify subtable names (the first one has a higher priority).
    1. Specify it by configuring the `smlAutoChildTableNameDelimiter` parameter in `taos.cfg` (excluding `@ # space carriage return newline tab`).
        1. For example: if configured `smlAutoChildTableNameDelimiter=-` and the data inserted is `st,t0=cpu1,t1=4 c1=3 1626006833639000000`, the created table name will be `cpu1-4`.
    2. Specify it by configuring the `smlChildTableName` parameter in `taos.cfg`.
        1. For example: if configured `smlChildTableName=tname` and the data inserted is `st,tname=cpu1,t1=4 c1=3 1626006833639000000`, the created table name will be `cpu1`. Note that if the `tname` is the same for multiple rows of data but with different tag sets, only the first row with the automatically created table will use the specified tag set; the others will be ignored.

2. If the supertable obtained from parsing the row protocol does not exist, it will be created (it is not recommended to manually create supertables; otherwise, data insertion may be abnormal).
3. If the subtable obtained from parsing the row protocol does not exist, Schemaless will create the subtable according to the names determined in steps 1 or 2.
4. If the tag columns or ordinary columns specified in the data row do not exist, the corresponding tag columns or ordinary columns will be added to the supertable (only additions are allowed).
5. If some tag columns or ordinary columns exist in the supertable but are not specified with values in a data row, those columns will be set to NULL in that row.
6. For BINARY or NCHAR columns, if the provided value's length exceeds the column type limit, the allowable character length of that column will be automatically increased (only increments are allowed) to ensure complete data storage.
7. Any errors encountered during the entire process will interrupt the writing process and return an error code.
8. To improve writing efficiency, it is assumed by default that the order of field_set in the same supertable is the same (the first data contains all fields, and the subsequent data follows this order). If the order differs, you need to configure the parameter `smlDataFormat` as false; otherwise, data will be written in the same order, and the data in the library will be abnormal. Starting from version 3.0.3.0, automatic detection of order consistency is performed, and this configuration is deprecated.
9. Since SQL table names do not support dots (.), schemaless writing also handles dots (.). If the table name generated by schemaless writing contains a dot (.), it will be automatically replaced with an underscore (_). If a subtable name is specified manually, dots (.) in the name will also be converted to underscores (_).
10. The `taos.cfg` configuration file has added the `smlTsDefaultName` configuration (value as a string), which only takes effect on the client side. After configuration, the timestamp column name of the automatically created supertable can be set through this configuration. If not configured, it defaults to `_ts`.
11. The names of supertables or subtables created through schemaless writing are case-sensitive.
12. Schemaless writing still adheres to TDengine's underlying limitations on data structures, such as the total length of each row of data not exceeding 48KB (64KB from version 3.0.5.0), and the total length of tag values not exceeding 16KB.

## Time Resolution Identification

Schemaless writing supports three specified modes, as shown in the table below:

| **No.** | **Value**              | **Description**                        |
| -------- | ------------------- | ----------------------------- |
| 1        | SML_LINE_PROTOCOL   | InfluxDB Line Protocol      |
| 2        | SML_TELNET_PROTOCOL | OpenTSDB Telnet Protocol             |
| 3        | SML_JSON_PROTOCOL   | JSON Format Protocol                   |

In SML_LINE_PROTOCOL parsing mode, users need to specify the time resolution of the input timestamps. Available time resolutions are as follows:

| **No.** | **Time Resolution Definition**                | **Meaning**       |
| -------- | --------------------------------- | -------------- |
| 1        | TSDB_SML_TIMESTAMP_NOT_CONFIGURED | Undefined (Invalid) |
| 2        | TSDB_SML_TIMESTAMP_HOURS          | Hours           |
| 3        | TSDB_SML_TIMESTAMP_MINUTES        | Minutes         |
| 4        | TSDB_SML_TIMESTAMP_SECONDS        | Seconds         |
| 5        | TSDB_SML_TIMESTAMP_MILLI_SECONDS  | Milliseconds     |
| 6        | TSDB_SML_TIMESTAMP_MICRO_SECONDS  | Microseconds     |
| 7        | TSDB_SML_TIMESTAMP_NANO_SECONDS   | Nanoseconds      |

In SML_TELNET_PROTOCOL and SML_JSON_PROTOCOL modes, the time precision is determined by the length of the timestamps (this is the same as the standard operation method for OpenTSDB), and the user-specified time resolution will be ignored.

## Data Mode Mapping Rules

The data from InfluxDB line protocol will be mapped to a structured format, where the measurement maps to the supertable name, the tag names in tag_set map to the tag names in the data structure, and the names in field_set map to column names. For example, the following data:

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4f64 1626006833639000000
```

This data row generates a supertable named `st`, which includes three nchar-type tags: `t1`, `t2`, `t3`, and five data columns: `ts` (timestamp), `c1` (bigint), `c3` (binary), `c2` (bool), `c4` (bigint). It maps to the following SQL statement:

```json
create stable st (_ts timestamp, c1 bigint, c2 bool, c3 binary(6), c4 bigint) tags(t1 nchar(1), t2 nchar(1), t3 nchar(2))
```

## Data Mode Change Handling

This section explains the impact on data mode under different row data writing conditions.

When writing with a clearly identified field type using row protocol, changing the field type definition later will result in a clear data mode error, triggering the writing API to report an error. As shown below,

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4    1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c3="passit",c2=false,c4=4i   1626006833640000000
```

The first row maps the data type of column `c4` to Double, but the second row specifies this column as BigInt through the numerical suffix, triggering a parsing error in schemaless writing.

If a row protocol indicates that a column is binary, but subsequent rows require a longer binary length, this will trigger a change in the supertable mode.

```json
st,t1=3,t2=4,t3=t3 c1=3i64,c5="pass"     1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c5="passit"   1626006833640000000
```

In the first row, the row protocol parsing declares `c5` as a binary(4) field, and the second row will extract `c5` as a binary column, but its width will be increased to accommodate the new string width.

```json
st,t1=3,t2=4,t3=t3 c1=3i64               1626006833639000000
st,t1=3,t2=4,t3=t3 c1=3i64,c6="passit"   1626006833640000000
```

In the second row, compared to the first, an additional column `c6` of type binary(6) has been added. Therefore, a new column `c6` of type binary(6) will be automatically added.

## Example of Schemaless Writing

Using the smart meter as an example, here are code samples demonstrating how various language connectors use the schemaless writing interface to write data, covering three protocols: InfluxDB's line protocol, OpenTSDB's TELNET line protocol, and OpenTSDB's JSON format protocol.

:::note

- Since the automatic table creation rules for schemaless writing differ from those in previous SQL example sections, please ensure that the `meters`, `metric_telnet`, and `metric_json` tables do not exist before running the code samples.
- The TELNET line protocol and JSON format protocol of OpenTSDB only support a single data column, so other examples have been used.

:::

### Websocket Connection

<Tabs defaultValue="java" groupId="lang">
<TabItem value="java" label="Java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/SchemalessWsTest.java:schemaless}}
```

Execute schemaless writing with reqId, the last parameter reqId can be used for request tracing.

```java
writer.write(lineDemo, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS, 1L);
```

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/schemaless_ws.py}}
```

</TabItem>

<TabItem label="Go" value="go">

```go
{{#include docs/examples/go/schemaless/ws/main.go}}
```

</TabItem>

<TabItem label="Rust" value="rust">

```rust
{{#include docs/examples/rust/restexample/examples/schemaless.rs}}
```

</TabItem>

<TabItem label="Node.js" value="node">

```js
{{#include docs/examples/node/websocketexample/line_example.js}}
```

</TabItem>

<TabItem label="C#" value="csharp">

```csharp
{{#include docs/examples/csharp/wssml/Program.cs:main}}
```

</TabItem>

<TabItem label="C" value="c">

```c
{{#include docs/examples/c-ws/sml_insert_demo.c:schemaless}}
```

</TabItem>

<TabItem label="REST API" value="rest">

Not supported

</TabItem>
</Tabs>

### Native Connection

<Tabs defaultValue="java" groupId="lang">
<TabItem label="Java" value="java">

```java
{{#include docs/examples/java/src/main/java/com/taos/example/SchemalessJniTest.java:schemaless}}
```

Execute schemaless writing with reqId, the last parameter reqId can be used for request tracing.

```java
writer.write(lineDemo, SchemalessProtocolType.LINE, SchemalessTimestampType.NANO_SECONDS, 1L);
```

</TabItem>

<TabItem label="Python" value="python">

```python
{{#include docs/examples/python/schemaless_native.py}}
```

</TabItem>

<TabItem label="Go" value="go">

```go
{{#include docs/examples/go/schemaless/native/main.go}}
```

</TabItem>

<TabItem label="Rust" value="rust">

```rust
{{#include docs/examples/rust/nativeexample/examples/schemaless.rs}}
```

</TabItem>

<TabItem label="Node.js" value="node">

Not supported

</TabItem>

<TabItem label="C#" value="csharp">

```csharp
{{#include docs/examples/csharp/nativesml/Program.cs:main}}
```

</TabItem>

<TabItem label="C" value="c">

```c
{{#include docs/examples/c/sml_insert_demo.c:schemaless}}
```

</TabItem>

<TabItem label="REST API" value="rest">

Not supported

</TabItem>
</Tabs>

## Querying Written Data

Running the code samples from the previous section will automatically create tables in the power database. We can query the data through the taos shell or application. Below are examples of querying supertables and the meters table data using the taos shell.

```shell
taos> show power.stables;
          stable_name           |
=================================
 meter_current                  |
 stb0_0                         |
 meters                         |
Query OK, 3 row(s) in set (0.002527s)



taos> select * from power.meters limit 1 \G;
*************************** 1.row ***************************
     _ts: 2021-07-11 20:33:53.639
 current: 10.300000199999999
 voltage: 219
   phase: 0.310000000000000
 groupid: 2
location: California.SanFrancisco
Query OK, 1 row(s) in set (0.004501s)
```
