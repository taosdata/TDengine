---
title: Data Types
description: 'TDengine supported data types: Timestamp, Float, JSON type, etc.'
slug: /tdengine-reference/sql-manual/data-types
---

## Timestamp

In TDengine, the most important aspect is the timestamp. You need to specify the timestamp when creating and inserting records, as well as when querying historical records. The rules for timestamps are as follows:

- The timestamp format is `YYYY-MM-DD HH:mm:ss.MS`, with a default resolution of milliseconds. For example: `2017-08-12 18:25:58.128`.
- The internal function NOW represents the current time of the client.
- When inserting records, if the timestamp is NOW, the current time of the client submitting the record is used.
- Epoch Time: The timestamp can also be a long integer representing the number of milliseconds since UTC time 1970-01-01 00:00:00. Accordingly, if the time precision of the database is set to "microseconds", the long integer timestamp corresponds to the number of microseconds since UTC time 1970-01-01 00:00:00; the logic is the same for nanoseconds.
- You can add or subtract time, for example, NOW-2h indicates that the query time is moved back 2 hours (the last 2 hours). The time unit after the number can be b (nanoseconds), u (microseconds), a (milliseconds), s (seconds), m (minutes), h (hours), d (days), w (weeks). For example, `SELECT * FROM t1 WHERE ts > NOW-2w AND ts <= NOW-1w` indicates querying data from exactly one week two weeks ago. When specifying the time window (Interval) for down sampling, time units can also be n (natural month) and y (natural year).

The default timestamp precision in TDengine is milliseconds, but it also supports microseconds and nanoseconds by passing the `PRECISION` parameter when `CREATE DATABASE` is executed.

```sql
CREATE DATABASE db_name PRECISION 'ns';
```

## Data Types

In TDengine, the following data types can be used in the data model of a table.

| #   |     **Type**      | **Bytes** | **Description**                                                                                                                                                                                                                                                                                                          |
| --- | :---------------: | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   |     TIMESTAMP     | 8         | Timestamp. The default precision is milliseconds, and it can support microseconds and nanoseconds, detailed explanation can be found in the previous section.                                                                                                                                                            |
| 2   |        INT        | 4         | Integer, range [-2^31, 2^31-1]                                                                                                                                                                                                                                                                                          |
| 3   |   INT UNSIGNED    | 4         | Unsigned integer, range [0, 2^32-1]                                                                                                                                                                                                                                                                                     |
| 4   |      BIGINT       | 8         | Long integer, range [-2^63, 2^63-1]                                                                                                                                                                                                                                                                                    |
| 5   |  BIGINT UNSIGNED  | 8         | Long integer, range [0, 2^64-1]                                                                                                                                                                                                                                                                                        |
| 6   |       FLOAT       | 4         | Floating-point number, significant digits 6-7, range [-3.4E38, 3.4E38]                                                                                                                                                                                                                                                   |
| 7   |      DOUBLE       | 8         | Double precision floating-point number, significant digits 15-16, range [-1.7E308, 1.7E308]                                                                                                                                                                                                                             |
| 8   |      BINARY       | Custom    | Records single-byte strings, recommended for handling ASCII visible characters only; multi-byte characters such as Chinese must use NCHAR.                                                                                                                                                                                |
| 9   |     SMALLINT      | 2         | Short integer, range [-32768, 32767]                                                                                                                                                                                                                                                                                    |
| 10  | SMALLINT UNSIGNED | 2         | Unsigned short integer, range [0, 65535]                                                                                                                                                                                                                                                                                 |
| 11  |      TINYINT      | 1         | Single-byte integer, range [-128, 127]                                                                                                                                                                                                                                                                                  |
| 12  | TINYINT UNSIGNED  | 1         | Unsigned single-byte integer, range [0, 255]                                                                                                                                                                                                                                                                           |
| 13  |       BOOL        | 1         | Boolean type, {true, false}                                                                                                                                                                                                                                                                                             |
| 14  |       NCHAR       | Custom    | Records strings containing multi-byte characters, such as Chinese characters. Each NCHAR character occupies 4 bytes of storage space. Strings should be enclosed in single quotes, and single quotes within strings should be escaped with `\'`. The size must be specified when using NCHAR; for example, NCHAR(10) indicates that this column can store up to 10 NCHAR characters. An error will occur if the user string length exceeds the declared length. |
| 15  |       JSON        |           | JSON data type, only tags can be in JSON format                                                                                                                                                                                                                                                                         |
| 16  |      VARCHAR      | Custom    | Alias for BINARY                                                                                                                                                                                                                                                                                                         |
| 17  |      GEOMETRY     | Custom    | Geometry type                                                                                                                                                                                                                                                                                                             |
| 18  |      VARBINARY     | Custom    | Variable-length binary data                                                                                                                                                                                                                                                                                              |

:::note

- The length of each row in a table cannot exceed 48KB (in version 3.0.5.0 and later, this limit is 64KB). (Note: Each BINARY/NCHAR/GEOMETRY/VARBINARY type column will also occupy an additional 2 bytes of storage space).
- Although the BINARY type supports byte-based binary characters at the storage level, the way different programming languages handle binary data may not guarantee consistency. Therefore, it is recommended to only store ASCII visible characters in the BINARY type and avoid storing invisible characters. Multi-byte data, such as Chinese characters, should be stored using the NCHAR type. If you forcefully use the BINARY type to store Chinese characters, it may sometimes read and write correctly, but it lacks character set information, making it prone to data garbling or even corruption.
- Theoretically, the BINARY type can be up to 16,374 bytes long (65,517 bytes for data columns and 16,382 bytes for tag columns starting from version 3.0.5.0). The BINARY type only supports string input, and the strings must be enclosed in single quotes. You must specify the size, for example, BINARY(20) defines the maximum length of a single-byte character string as 20 characters, occupying a total of 20 bytes of space. If the user string exceeds 20 bytes, an error will occur. For single quotes within the string, use the escape character backslash plus single quote, i.e., `\'`.
- The GEOMETRY type data column has a maximum length of 65,517 bytes, and the maximum length for tag columns is 16,382 bytes. It supports 2D POINT, LINESTRING, and POLYGON subtype data. The length calculation is as follows:

    | # | **Syntax**                               | **Min Length** | **Max Length**   | **Each Coordinate Length Growth** |
    |---|--------------------------------------|----------|------------|--------------|
    | 1 | POINT(1.0 1.0)                       | 21       | 21         | None            |
    | 2 | LINESTRING(1.0 1.0, 2.0 2.0)         | 9+2*16   | 9+4094*16  | +16          |
    | 3 | POLYGON((1.0 1.0, 2.0 2.0, 1.0 1.0)) | 13+3*16  | 13+4094*16 | +16          |

- In SQL statements, the numerical type will be determined based on the presence of a decimal point or scientific notation. Therefore, be cautious of type overflow when using values. For example, 9999999999999999999 will be considered an overflow exceeding the upper limit of long integers, while 9999999999999999999.0 will be considered a valid floating-point number.
- VARBINARY is a data type for storing binary data, with a maximum length of 65,517 bytes and a maximum length for tag columns of 16,382 bytes. It can be written using SQL or schemaless methods (it needs to be converted to a string starting with \x for writing), or it can be written using the stmt method (binary can be used directly). Displayed in hexadecimal format starting with \x.

:::

## Constants

TDengine supports multiple types of constants, detailed in the table below:

| #   |                     **Syntax**                      | **Type**  | **Description**                                                                                                                                              |
| --- | :-----------------------------------------------: | --------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| 1   |                   [\{+ \| -}]123                   | BIGINT    | The literal type of integer values is BIGINT. If the user input exceeds the range of BIGINT, TDengine truncates the value according to BIGINT.                                          |
| 2   |                      123.45                       | DOUBLE    | The literal type of floating-point values is DOUBLE. TDengine determines whether the numerical type is integer or floating-point based on the presence of a decimal point or scientific notation.                            |
| 3   |                       1.2E3                       | DOUBLE    | The literal type of scientific notation is DOUBLE.                                                                                                           |
| 4   |                       'abc'                       | BINARY    | Content enclosed in single quotes is a string literal of type BINARY, and the size of BINARY is the actual number of characters. For single quotes within the string, use the escape character backslash plus single quote, i.e., `\'`. |
| 5   |                       "abc"                       | BINARY    | Content enclosed in double quotes is a string literal of type BINARY, and the size of BINARY is the actual number of characters. For double quotes within the string, use the escape character backslash plus double quote, i.e., `\"`. |
| 6   |        TIMESTAMP \{'literal' \| "literal"}         | TIMESTAMP | The TIMESTAMP keyword indicates that the following string literal needs to be interpreted as TIMESTAMP type. The string must meet the format YYYY-MM-DD HH:mm:ss.MS, with the time resolution as the current database's time resolution. |
| 7   |                  \{TRUE \| FALSE}                  | BOOL      | Boolean type literal.                                                                                                                                       |
| 8   | \{'' \| "" \| '\t' \| "\t" \| ' ' \| " " \| NULL } | --        | Null value literal. Can be used for any type.                                                                                                               |

:::note

- TDengine determines whether the numerical type is integer or floating-point based on the presence of a decimal point or scientific notation. Therefore, be cautious of type overflow when using values. For example, 9999999999999999999 will be considered an overflow exceeding the upper limit of long integers, while 9999999999999999999.0 will be considered a valid floating-point number.

:::
