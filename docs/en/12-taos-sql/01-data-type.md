---
title: Data Types
description: "TDengine supports a variety of data types including timestamp, float, JSON and many others."
---

## TIMESTAMP

When using TDengine to store and query data, the most important part of the data is timestamp. Timestamp must be specified when creating and inserting data rows. Timestamp must follow the rules below:

- The format must be `YYYY-MM-DD HH:mm:ss.MS`, the default time precision is millisecond (ms), for example `2017-08-12 18:25:58.128`
- Internal function `now` can be used to get the current timestamp on the client side
- The current timestamp of the client side is applied when `now` is used to insert data
- Epoch Time：timestamp can also be a long integer number, which means the number of seconds, milliseconds or nanoseconds, depending on the time precision, from 1970-01-01 00:00:00.000 (UTC/GMT)
- Add/subtract operations can be carried out on timestamps. For example `now-2h` means 2 hours prior to the time at which query is executed. The units of time in operations can be b(nanosecond), u(microsecond), a(millisecond), s(second), m(minute), h(hour), d(day), or w(week). So `select * from t1 where ts > now-2w and ts <= now-1w` means the data between two weeks ago and one week ago. The time unit can also be n (calendar month) or y (calendar year) when specifying the time window for down sampling operations.

Time precision in TDengine can be set by the `PRECISION` parameter when executing `CREATE DATABASE`. The default time precision is millisecond. In the statement below, the precision is set to nanonseconds.

```sql
CREATE DATABASE db_name PRECISION 'ns';
```

## Data Types

In TDengine, the data types below can be used when specifying a column or tag.

| #   | **type**  | **Bytes** | **Description** |
| --- | :-------: | --------- | ------------------------- |
| 1   | TIMESTAMP | 8         | Default precision is millisecond, microsecond and nanosecond are also supported  |
| 2   |    INT    | 4         | Integer, the value range is [-2^31, 2^31-1]  |
| 3   |INT UNSIGNED|4         | Unsigned integer, the value range is [0, 2^31-1] |
| 4   |  BIGINT   | 8         | Long integer, the value range is [-2^63, 2^63-1] |
| 5   | BIGINT UNSIGNED | 8   | Unsigned long integer, the value range is [0, 2^63-1] |
| 6   |   FLOAT   | 4         | Floating point number, the effective number of digits is 6-7, the value range is [-3.4E38, 3.4E38]  |
| 7   |  DOUBLE   | 8         | Double precision floating point number, the effective number of digits is 15-16, the value range is [-1.7E308, 1.7E308]  |
| 8   |  BINARY   | User Defined | Single-byte string for ASCII visible characters. Length must be specified when defining a column or tag of binary type. The string length can be up to  16374 bytes. The string value must be quoted with single quotes. The literal single quote inside the string must be preceded with back slash like `\'` |
| 9   | SMALLINT  | 2         | Short integer, the value range is [-32768, 32767] |
| 10  | SMALLINT UNSIGNED | 2 | Unsigned short integer, the value range is [0, 32767] |
| 11  |  TINYINT  | 1         | Single-byte integer, the value range is [-128, 127] |
| 12  | TINYINT UNSIGNED | 1  | Unsigned single-byte integer, the value range is [0, 127] |
| 13  |   BOOL    | 1         | Bool, the value range is {true, false}   |
| 14  | NCHAR     | User Defined| Multi-Byte string that can include multi byte characters like Chinese characters. Each character of NCHAR type consumes 4 bytes storage. The string value should be quoted with single quotes. Literal single quote inside the string must be preceded with backslash, like `\’`. The length must be specified when defining a column or tag of NCHAR type, for example nchar(10) means it can store at most 10 characters of nchar type and will consume fixed storage of 40 bytes. An error will be reported if the string value exceeds the length defined.   |
| 15  |   JSON    |           | JSON type can only be used on tags. A tag of json type is excluded with any other tags of any other type |
| 16  | VARCHAR   | User Defined| Alias of BINARY type |

:::note
- TDengine is case insensitive and treats any characters in the sql command as lower case by default, case sensitive strings must be quoted with single quotes.
- Only ASCII visible characters are suggested to be used in a column or tag of BINARY type. Multi-byte characters must be stored in NCHAR type.
- Numeric values in SQL statements will be determined as integer or float type according to whether there is decimal point or whether scientific notation is used, so attention must be paid to avoid overflow. For example, 9999999999999999999 will be considered as overflow because it exceeds the upper limit of long integer, but 9999999999999999999.0 will be considered as a legal float number.

:::

## Constants
TDengine supports constants of multiple data type.

| #   | **Syntax**  | **Type** | **Description**    |
| --- | :-------: | --------- | -------------------------------------- |
| 1 | [{+ \| -}]123 | BIGINT | Numeric constants are treated as BIGINT type. The value will be truncated if it exceeds the range of BIGINT type. |
| 2 | 123.45 | DOUBLE | Floating number constants are treated as DOUBLE type. TDengine determines whether it's a floating number based on if decimal point or scientific notation is used. |
| 3 | 1.2E3 | DOUBLE | Constants in scientific notation are treated ad DOUBLE type. |
| 4 | 'abc' | BINARY | String constants enclosed by single quotes are treated as BINARY type. Its size is determined as the acutal length. Single quote itself can be included by preceding backslash, i.e. `\'`, in a string constant. |
| 5 | "abc" | BINARY | String constants enclosed by double quotes are treated as BINARY type. Its size is determined as the acutal length. Double quote itself can be included by preceding backslash, i.e. `\"`, in a string constant. |
| 6 | TIMESTAMP {'literal' \| "literal"} | TIMESTAMP | A string constant following `TIMESTAMP` keyword is treated as TIMESTAMP type. The string should be in the format of "YYYY-MM-DD HH:mm:ss.MS". Its time precision is same as that of the current database being used.  |
| 7 | {TRUE \| FALSE} | BOOL | BOOL type contant.  |
| 8 | {'' \| "" \| '\t' \| "\t" \| ' ' \| " " \| NULL } | -- | NULL constant, it can be used for any type.|

:::note
- TDengine determines whether it's a floating number based on if decimal point or scientific notation is used. So whether the value is determined as overflow depends on both the value and the determined type. For example, 9999999999999999999 is determined as overflow because it exceeds the upper limit of BIGINT type, while 9999999999999999999.0 is considered as a valid floating number because it is within the range of DOUBLE type.

:::
