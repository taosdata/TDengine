---
title: Data Types
slug: /tdengine-reference/sql-manual/data-types
---

## Timestamp

Using TDengine, the most important aspect is the timestamp. When creating and inserting records, or querying historical records, specifying the timestamp is necessary. The rules for timestamps are as follows:

- The time format is `YYYY-MM-DD HH:mm:ss.MS`, with the default time resolution being milliseconds. For example: `2017-08-12 18:25:58.128`
- The internal function NOW represents the current time of the client
- When inserting records, if the timestamp is NOW, the current time of the client submitting the record is used
- Epoch Time: The timestamp can also be a long integer, representing the number of milliseconds since UTC time 1970-01-01 00:00:00. Accordingly, if the time precision of the Database is set to "microseconds", the meaning of the long integer format timestamp corresponds to the number of microseconds since UTC time 1970-01-01 00:00:00; the logic for nanoseconds precision is similar.
- Time can be added or subtracted, such as NOW-2h, which indicates pushing the query time forward by 2 hours (the last 2 hours). The time unit after the number can be b (nanoseconds), u (microseconds), a (milliseconds), s (seconds), m (minutes), h (hours), d (days), w (weeks). For example `SELECT * FROM t1 WHERE ts > NOW-2w AND ts <= NOW-1w`, represents querying data for a whole week two weeks ago. When specifying the time window (Interval) for Down Sampling operations, the time unit can also use n (natural month) and y (natural year).

TDengine's default timestamp precision is milliseconds, but microseconds and nanoseconds are also supported by passing the `PRECISION` parameter during `CREATE DATABASE`.

```sql
CREATE DATABASE db_name PRECISION 'ns';
```

## Data Types

In TDengine, the following data types can be used in the data model of basic tables.

| #    |     **Type**      | **Bytes** | **Description**                                              |
| ---- | :---------------: | --------- | ------------------------------------------------------------ |
| 1    |     TIMESTAMP     | 8         | Timestamp. Default precision is milliseconds, supports microseconds and nanoseconds, see the previous section for details. |
| 2    |        INT        | 4         | Integer, range [-2^31, 2^31-1]                               |
| 3    |   INT UNSIGNED    | 4         | Unsigned integer, [0, 2^32-1]                                |
| 4    |      BIGINT       | 8         | Long integer, range [-2^63, 2^63-1]                          |
| 5    |  BIGINT UNSIGNED  | 8         | Long integer, range [0, 2^64-1]                              |
| 6    |       FLOAT       | 4         | Float, precision 6-7 digits, range [-3.4E38, 3.4E38]         |
| 7    |      DOUBLE       | 8         | Double precision float, precision 15-16 digits, range [-1.7E308, 1.7E308] |
| 8    |      BINARY       | Custom    | Records single-byte strings, recommended for handling ASCII visible characters only, multi-byte characters such as Chinese should use NCHAR |
| 9    |     SMALLINT      | 2         | Short integer, range [-32768, 32767]                         |
| 10   | SMALLINT UNSIGNED | 2         | Unsigned short integer, range [0, 65535]                     |
| 11   |      TINYINT      | 1         | Single-byte integer, range [-128, 127]                       |
| 12   | TINYINT UNSIGNED  | 1         | Unsigned single-byte integer, range [0, 255]                 |
| 13   |       BOOL        | 1         | Boolean, {true, false}                                       |
| 14   |       NCHAR       | Custom    | Records strings including multi-byte characters, such as Chinese characters. Each NCHAR character occupies 4 bytes of storage space. Strings are enclosed in single quotes, and single quotes within the string are escaped with `\'`. NCHAR usage must specify the string size, a column of type NCHAR(10) indicates that this column can store up to 10 NCHAR characters. If the user's string length exceeds the declared length, an error will be reported. |
| 15   |       JSON        |           | JSON data type, only Tags can be in JSON format              |
| 16   |      VARCHAR      | Custom    | Alias for BINARY type                                        |
| 17   |     GEOMETRY      | Custom    | Geometry type, supported starting from version 3.1.0.0       |
| 18   |     VARBINARY     | Custom    | Variable-length binary data, supported starting from version 3.1.1.0 |
| 19  |      DECIMAL      |  8 or 16  | High-precision numeric type. The range of values depends on the precision and scale specified in the type. Supported starting from version 3.3.6. See the description below. |

:::note

- The maximum length of each row in a table cannot exceed 48KB (64KB starting from version 3.0.5.0) (Note: Each BINARY/NCHAR/GEOMETRY/VARBINARY type column will also occupy an additional 2 bytes of storage space).
- Although the BINARY type supports byte-type binary characters at the storage level, different programming languages do not guarantee consistent handling of binary data. Therefore, it is recommended to store only ASCII visible characters in the BINARY type and avoid storing invisible characters. Multibyte data, such as Chinese characters, should be saved using the NCHAR type. If Chinese characters are forcibly saved using the BINARY type, although they can sometimes be read and written normally, they do not carry character set information, which can easily lead to data corruption or even data damage.
- Theoretically, the BINARY type can be up to 16,374 bytes long (from version 3.0.5.0, data columns are 65,517 bytes, label columns are 16,382 bytes). BINARY only supports string input, which must be enclosed in single quotes. When used, the size must be specified, such as BINARY(20) defines a string up to 20 single-byte characters long, with each character occupying 1 byte of storage space, totaling a fixed 20 bytes of space. If the user's string exceeds 20 bytes, an error will be reported. For single quotes within the string, they can be represented by the escape character backslash followed by a single quote, i.e., `\'`.
- GEOMETRY type data columns have a maximum length of 65,517 bytes, and label columns have a maximum length of 16,382 bytes. Supports 2D subtypes of POINT, LINESTRING, and POLYGON data. The length calculation method is shown in the following table:

    | #    | **Syntax**                           | **Minimum Length** | **Maximum Length** | **Increment per Coordinate Set** |
    | ---- | ------------------------------------ | ------------------ | ------------------ | -------------------------------- |
    | 1    | POINT(1.0 1.0)                       | 21                 | 21                 | None                             |
    | 2    | LINESTRING(1.0 1.0, 2.0 2.0)         | 9+2*16             | 9+4094*16          | +16                              |
    | 3    | POLYGON((1.0 1.0, 2.0 2.0, 1.0 1.0)) | 13+3*16            | 13+4094*16         | +16                              |

- In SQL statements, the type of numerical values will be determined based on the presence of a decimal point or the use of scientific notation, so care must be taken to avoid type overflow. For example, 9999999999999999999 will be considered to exceed the upper boundary of long integers and overflow, while 9999999999999999999.0 will be considered a valid floating point number.
- VARBINARY is a data type for storing binary data, with a maximum length of 65,517 bytes for data columns and 16,382 bytes for label columns. Binary data can be written via SQL or schemaless methods (needs to be converted to a string starting with \x), or through stmt methods (can use binary directly). Displayed as hexadecimal starting with \x.

:::

### DECIMAL Data Type

The `DECIMAL` data type is used for high-precision numeric storage and is supported starting from version 3.3.6. The definition syntax is: `DECIMAL(18, 2)`, `DECIMAL(38, 10)`, where two parameters must be specified: `precision` and `scale`. `Precision` refers to the maximum number of significant digits supported, and `scale` refers to the maximum number of decimal places. For example, `DECIMAL(8, 4)` represents a range of `[-9999.9999, 9999.9999]`. When defining the `DECIMAL` data type, the range of `precision` is `[1, 38]`, and the range of `scale` is `[0, precision]`. If `scale` is 0, it represents integers only. You can also omit `scale`, in which case it defaults to 0. For example, `DECIMAL(18)` is equivalent to `DECIMAL(18, 0)`.

When the `precision` value is less than or equal to 18, 8 bytes of storage (DECIMAL64) are used internally. When the `precision` is in the range `(18, 38]`, 16 bytes of storage (DECIMAL) are used. When writing `DECIMAL` type data in SQL, numeric values can be written directly. If the value exceeds the maximum representable value for the type, a `DECIMAL_OVERFLOW` error will be reported. If the value does not exceed the maximum representable value but the number of decimal places exceeds the `scale`, it will be automatically rounded. For example, if the type is defined as `DECIMAL(10, 2)` and the value `10.987` is written, the actual stored value will be `10.99`.

The `DECIMAL` type only supports regular columns and does not currently support tag columns. The `DECIMAL` type supports SQL-based writes only and does not currently support `stmt` or schemaless writes.

When performing operations between integer types and the `DECIMAL` type, the integer type is converted to the `DECIMAL` type before the calculation. When the `DECIMAL` type is involved in calculations with `DOUBLE`, `FLOAT`, `VARCHAR`, or `NCHAR` types, it is converted to `DOUBLE` type for computation.

When querying `DECIMAL` type expressions, if the intermediate result of the calculation exceeds the maximum value that the current type can represent, a `DECIMAL_OVERFLOW` error is reported.

## Constants

TDengine supports multiple types of constants, details as shown in the table below:

| #    |                     **Syntax**                     | **Type**  | **Description**                                              |
| ---- | :------------------------------------------------: | --------- | ------------------------------------------------------------ |
| 1    |                   [\{+ \| -}]123                   | BIGINT    | The literal type of integer values is always BIGINT. If the user input exceeds the range of BIGINT, TDengine truncates the value as BIGINT. |
| 2    |                       123.45                       | DOUBLE    | The literal type of floating-point values is always DOUBLE. TDengine determines whether the value is an integer or floating point based on the presence of a decimal point or the use of scientific notation. |
| 3    |                       1.2E3                        | DOUBLE    | The literal type for scientific notation is DOUBLE.          |
| 4    |                       'abc'                        | BINARY    | Content enclosed in single quotes is a string literal, whose type is BINARY. The size of BINARY is the actual number of characters. For single quotes within the string, they can be represented by the escape character backslash followed by a single quote, i.e., `\'`. |
| 5    |                       "abc"                        | BINARY    | Content enclosed in double quotes is a string literal, whose type is BINARY. The size of BINARY is the actual number of characters. For double quotes within the string, they can be represented by the escape character backslash followed by a single quote, i.e., `\"`. |
| 6    |        TIMESTAMP \{'literal' \| "literal"}         | TIMESTAMP | The TIMESTAMP keyword indicates that the following string literal should be interpreted as a TIMESTAMP type. The string must meet the YYYY-MM-DD HH:mm:ss.MS format, with the time resolution being that of the current database. |
| 7    |                  \{TRUE \| FALSE}                  | BOOL      | Boolean type literal.                                        |
| 8    | \{'' \| "" \| '\t' \| "\t" \| ' ' \| " " \| NULL } | --        | Null value literal. Can be used for any type.                |

:::note

- TDengine determines whether a numeric type is an integer or a floating-point based on the presence of a decimal point or the use of scientific notation. Therefore, be aware of potential type overflow when using it. For example, 9999999999999999999 is considered to exceed the upper boundary of a long integer and will overflow, while 9999999999999999999.0 is considered a valid floating-point number.

:::
