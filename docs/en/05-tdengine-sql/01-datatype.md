---
sidebar_label: Data Types and Precision
title: Data Types and Precision
description: Supported data types, time units, timestamps, literals, and constants in TDengine
---

This page describes the data types, time units, timestamp rules, and constant forms available in TDengine SQL.

## Data Type Overview

In TDengine, the following data types can be used for normal tables, child tables, and supertables. Some types are restricted to specific positions; see the sections below.

| Type | Storage | Description |
| --- | --- | --- |
| `TIMESTAMP` | 8 bytes | Timestamp. Default precision is milliseconds; can be set to microseconds or nanoseconds when creating a database. |
| `BOOL` | 1 byte | Boolean. |
| `TINYINT` | 1 byte | Signed single-byte integer, range `[-128, 127]`. |
| `TINYINT UNSIGNED` | 1 byte | Unsigned single-byte integer, range `[0, 255]`. |
| `SMALLINT` | 2 bytes | Signed short integer, range `[-32768, 32767]`. |
| `SMALLINT UNSIGNED` | 2 bytes | Unsigned short integer, range `[0, 65535]`. |
| `INT` | 4 bytes | Signed integer, range `[-2^31, 2^31-1]`. |
| `INT UNSIGNED` | 4 bytes | Unsigned integer, range `[0, 2^32-1]`. |
| `BIGINT` | 8 bytes | Signed long integer, range `[-2^63, 2^63-1]`. |
| `BIGINT UNSIGNED` | 8 bytes | Unsigned long integer, range `[0, 2^64-1]`. |
| `FLOAT` | 4 bytes | Single-precision float, about 6–7 significant digits, range about `[-3.4E38, 3.4E38]`. |
| `DOUBLE` | 8 bytes | Double-precision float, about 15–16 significant digits, range about `[-1.7E308, 1.7E308]`. |
| `BINARY` | Custom | Single-byte string; recommended for ASCII printable characters only. |
| `VARCHAR` | Custom | Alias for `BINARY`. |
| `NCHAR` | Custom | Multi-byte string; suitable for Chinese and other multi-byte characters. |
| `VARBINARY` | Custom | Variable-length binary data. |
| `GEOMETRY` | Custom | Geometry type; supports 2D `POINT`, `LINESTRING`, and `POLYGON`. |
| `DECIMAL` | 8 or 16 bytes | High-precision numeric type; range depends on `precision` and `scale`. |
| `BLOB` | Up to 4 MB | Large-object binary data. |
| `JSON` | Custom | JSON tag type; can only be used for tag columns. |

## General Limits

- Maximum row length is 48 KB (64 KB starting from version 3.0.5.0). Each `BINARY`, `NCHAR`, `GEOMETRY`, or `VARBINARY` column also occupies an extra 2 bytes of storage.
- Maximum length of `BINARY`, `VARBINARY`, and `GEOMETRY` data columns is 65,517 bytes; for tag columns it is 16,382 bytes.
- Maximum length of a single `BLOB` column value is 4,194,304 bytes.
- `JSON` can only be used for tag columns. If a JSON tag is used, there can be only one tag column.
- `DECIMAL` is supported only for ordinary columns; tag columns are not supported yet.

For naming limits on databases, tables, columns, and tags, see [Names & Limits](./11-appendix/02-limit.md).

## Time Units

Wherever a time duration is required in TDengine SQL (time arithmetic, INTERVAL, EVERY, SLIDING, etc.), a single-character suffix denotes the unit. Supported time units, from smallest to largest:

| Unit | Meaning | Notes |
| :---: | --- | --- |
| `b` | Nanoseconds | Smallest precision unit; meaningful only when database precision is nanoseconds. |
| `u` | Microseconds | Meaningful only when database precision is microseconds or nanoseconds. |
| `a` | Milliseconds | Default database precision. |
| `s` | Seconds | |
| `m` | Minutes | |
| `h` | Hours | |
| `d` | Days | |
| `w` | Weeks | Fixed at 7 days. |
| `n` | Natural month | Calendar unit; only allowed in `INTERVAL` windows, not in time arithmetic, `EVERY`, `SURROUND`, etc. |
| `q` | Natural quarter | Calendar unit; equivalent to 3 natural months; only allowed in `INTERVAL` windows, not in time arithmetic, `EVERY`, `SURROUND`, etc. |
| `y` | Natural year | Calendar unit; only allowed in `INTERVAL` windows, not in time arithmetic, `EVERY`, `SURROUND`, etc. |

Unit letters are case-insensitive (e.g., `1S` equals `1s`).
For full timezone and natural-unit semantics, see [Timezone and Natural Time Units](./10-time/01-timezone.md).

## Timestamp

The timestamp is the primary key of time-series data in TDengine. Creating tables, writing data, and querying history usually require specifying a timestamp.

- Time string format is `YYYY-MM-DD HH:mm:ss.MS`; default resolution is milliseconds, e.g. `2017-08-12 18:25:58.128`.
- `NOW` is the client current time. When writing, if the timestamp is `NOW`, the submitting client’s current time is used.
- A timestamp may also be a long integer representing elapsed time since UTC `1970-01-01 00:00:00`. The unit of the integer follows database precision: milliseconds, microseconds, or nanoseconds.
- Time expressions support addition and subtraction, e.g. `NOW - 2h` means 2 hours before now. See [Time Units](#time-units).

Default timestamp precision is milliseconds. When creating a database, set microseconds or nanoseconds with `PRECISION`.

```sql
CREATE DATABASE db_name PRECISION 'ns';
```

For full `PRECISION` details, see [Create Database](./02-ddl/01-database.md).

## String, Binary, and Spatial Types

### BINARY and VARCHAR

`BINARY` stores single-byte strings; `VARCHAR` is an alias for `BINARY`. Store only ASCII printable characters in `BINARY`/`VARCHAR`; use `NCHAR` for Chinese and other multi-byte characters. Forcing Chinese into `BINARY` may sometimes read/write, but without charset information it easily causes garbled or corrupted data.

Specify a length when using `BINARY` or `VARCHAR`, e.g. `BINARY(20)` stores up to 20 single-byte characters. Strings are enclosed in single quotes; a single quote inside a string can be escaped as `\'`.

### NCHAR

`NCHAR` stores strings that include multi-byte characters (e.g. Chinese). Each `NCHAR` character occupies 4 bytes. Specify character length when using it, e.g. `NCHAR(10)` stores up to 10 `NCHAR` characters. Writing a string longer than declared returns an error.

### VARBINARY

`VARBINARY` stores variable-length binary data. It can be written via SQL or schemaless (convert to a string starting with `\x`), or bound directly as binary via `STMT`. Query display returns hexadecimal starting with `\x`.

### GEOMETRY

`GEOMETRY` stores 2D geometry objects and supports `POINT`, `LINESTRING`, and `POLYGON`. Length calculation:

| Syntax | Min length | Max length | Growth per coordinate set |
| --- | --- | --- | --- |
| `POINT(1.0 1.0)` | 21 | 21 | None |
| `LINESTRING(1.0 1.0, 2.0 2.0)` | `9+2*16` | `9+4094*16` | `+16` |
| `POLYGON((1.0 1.0, 2.0 2.0, 1.0 1.0))` | `13+3*16` | `13+4094*16` | `+16` |

## High-Precision Numeric Type DECIMAL

The `DECIMAL` data type stores high-precision numbers. Definition syntax: `DECIMAL(18, 2)`, `DECIMAL(38, 10)`, where `precision` is the maximum number of significant digits and `scale` is the maximum number of fractional digits. For example, `DECIMAL(8, 4)` represents `[-9999.9999, 9999.9999]`.

When defining `DECIMAL`, `precision` is in `[1, 38]` and `scale` is in `[0, precision]`. `scale` 0 means integers only. You may omit `scale` (defaults to 0); `DECIMAL(18)` equals `DECIMAL(18, 0)`.

When `precision` ≤ 18, storage is 8 bytes (`DECIMAL64`); when `precision` is in `(18, 38]`, storage is 16 bytes (`DECIMAL`). In SQL, write numeric values directly. Values exceeding the type’s maximum raise `DECIMAL_OVERFLOW`; values within range but with more fractional digits than `scale` are rounded. For example, type `DECIMAL(10, 2)` with value `10.987` stores `10.99`.

`DECIMAL` supports SQL and `STMT2` writes; schemaless writes are not supported yet.

Operations between integer types and `DECIMAL` convert the integer to `DECIMAL` first. Operations between `DECIMAL` and `DOUBLE`, `FLOAT`, `VARCHAR`, or `NCHAR` convert to `DOUBLE` first.

When querying `DECIMAL` expressions, if an intermediate result exceeds the current type’s maximum, a `DECIMAL OVERFLOW` error is returned.

## Large Object Type BLOB

`BLOB` stores larger binary data, maximum length 4,194,304 bytes. Write via SQL or `STMT2`, or as a string starting with `\x`.

When queried via the shell, `BLOB` is shown as a hexadecimal string starting with `\x`.

Limits:

- `BLOB` is allowed only in ordinary data columns, and at most one `BLOB` column.
- Conditional filtering on `BLOB` columns is not supported.

Other limits:

- Not supported in virtual tables, stream computing, and related features.

## JSON Tags

`JSON` can only be used for tag columns. If a JSON tag is used, there can be only one tag column.

### Syntax

1. Create a JSON tag.

   ```sql
   CREATE STABLE s1 (ts TIMESTAMP, v1 INT) TAGS (info JSON);

   CREATE TABLE s1_1 USING s1 TAGS ('{"k1": "v1"}');
   ```

2. Use the JSON value operator `->`.

   ```sql
   SELECT * FROM s1 WHERE info->'k1' = 'v1';

   SELECT info->'k1' FROM s1;
   ```

3. Use `CONTAINS` to test whether a JSON key exists.

   ```sql
   SELECT * FROM s1 WHERE info CONTAINS 'k2';

   SELECT * FROM s1 WHERE info CONTAINS 'k1';
   ```

### Supported Operations

- In `WHERE`, `MATCH`, `NMATCH`, `BETWEEN ... AND`, `LIKE`, `AND`, `OR`, `IS NULL`, and `IS NOT NULL` are supported; `IN` is not.

  ```sql
  SELECT * FROM s1 WHERE info->'k1' MATCH 'v*';

  SELECT * FROM s1 WHERE info->'k1' LIKE 'v%' AND info CONTAINS 'k2';

  SELECT * FROM s1 WHERE info IS NULL;

  SELECT * FROM s1 WHERE info->'k1' IS NOT NULL;
  ```

- JSON tags can appear in `GROUP BY`, `ORDER BY`, `JOIN`, `UNION ALL`, and subqueries, e.g. `GROUP BY info->'key'`.
- `DISTINCT` is supported.
- Full overwrite of JSON tag values is supported.
- Renaming JSON tags is supported.
- Adding/dropping JSON tags or changing JSON tag column width is not supported.

### Other Constraints

1. JSON key length cannot exceed 256 bytes and must be printable ASCII; total JSON string length cannot exceed 4096 bytes.
2. Input may be empty (`""`, `"\t"`, `" "`, or `NULL`) or an object; non-empty strings, booleans, and arrays are not allowed.
3. An object may be `{}`. If the object is `{}`, the whole JSON string is treated as empty. A key may be `""`; if so, that key-value pair is ignored.
4. Values may be numbers (int/double), strings, bool, or null; arrays and nesting are not supported yet.
5. If two identical keys appear, the first takes effect.
6. Escape sequences in JSON strings are not supported yet.
7. Querying a missing key returns NULL.
8. When a JSON tag is a subquery result, the outer query cannot continue to parse that JSON string.

The following are not supported:

```sql
SELECT jtag->'key' FROM (SELECT jtag FROM stable);

SELECT jtag->'key' FROM (SELECT jtag FROM stable) WHERE jtag->'key' > 0;
```

## Constants

TDengine supports the following constant forms.

- Integer literals: e.g. `123`, `+123`, `-123`, type `BIGINT`. Values outside the `BIGINT` range are truncated to `BIGINT`.
- Floating-point literals: e.g. `123.45`, type `DOUBLE`.
- Scientific-notation literals: e.g. `1.2E3`, type `DOUBLE`.
- String literals: e.g. `'abc'` or `"abc"`, type `BINARY`, length is the actual character count. Use `\'` for single quotes and `\"` for double quotes inside the string.
- Timestamp literals: e.g. `TIMESTAMP '2017-08-12 18:25:58.128'`, type `TIMESTAMP`. The string must match `YYYY-MM-DD HH:mm:ss.MS`; resolution follows the current database.
- Boolean literals: `TRUE` or `FALSE`, type `BOOL`.
- Null literals: empty string, tab, space, or `NULL`; usable for any type.

Numeric types in SQL are judged as integer or floating-point by decimal point or scientific notation. Watch for overflow: `9999999999999999999` overflows the long-integer upper bound, while `9999999999999999999.0` is a valid floating-point number.
