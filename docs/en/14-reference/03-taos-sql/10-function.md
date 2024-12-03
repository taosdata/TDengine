---
title: Functions
description: List of functions supported by TDengine
slug: /tdengine-reference/sql-manual/functions
---

## Single-row Functions

Single-row functions return one result row for each row in the query result.

### Mathematical Functions

#### ABS

```sql
ABS(expr)
```

**Function Description**: Returns the absolute value of the specified field.

**Return Result Type**: Matches the original data type of the specified field.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**: Can only be used with regular columns, selection, or projection functions; cannot be used with aggregation functions.

#### ACOS

```sql
ACOS(expr)
```

**Function Description**: Returns the arccosine of the specified field.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**: Can only be used with regular columns, selection, or projection functions; cannot be used with aggregation functions.

#### ASIN

```sql
ASIN(expr)
```

**Function Description**: Returns the arcsine of the specified field.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**: Can only be used with regular columns, selection, or projection functions; cannot be used with aggregation functions.

#### ATAN

```sql
ATAN(expr)
```

**Function Description**: Returns the arctangent of the specified field.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**: Can only be used with regular columns, selection, or projection functions; cannot be used with aggregation functions.

#### CEIL

```sql
CEIL(expr)
```

**Function Description**: Returns the smallest integer greater than or equal to the specified field.

**Return Result Type**: Matches the original data type of the specified field.

**Applicable Data Types**: Numeric types.

**Applicable For**: Tables and supertables.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Usage Description**: Can only be used with regular columns, selection, or projection functions; cannot be used with aggregation functions.

#### COS

```sql
COS(expr)
```

**Function Description**: Returns the cosine of the specified field.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**: Can only be used with regular columns, selection, or projection functions; cannot be used with aggregation functions.

#### FLOOR

```sql
FLOOR(expr)
```

**Function Description**: Returns the largest integer less than or equal to the specified field. Other usage details are similar to those of the CEIL function.

#### LOG

```sql
LOG(expr1[, expr2])
```

**Function Description**: Returns the logarithm of expr1 to the base expr2. If the expr2 parameter is omitted, it returns the natural logarithm of the specified field.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**: Can only be used with regular columns, selection, or projection functions; cannot be used with aggregation functions.

#### POW

```sql
POW(expr1, expr2)
```

**Function Description**: Returns expr1 raised to the power of expr2.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**: Can only be used with regular columns, selection, or projection functions; cannot be used with aggregation functions.

#### ROUND

```sql
ROUND(expr[, digits])
```

**Function Description**: Returns the rounded value of the specified field.

**Return Result Type**: Matches the original data type of the specified field.

**Applicable Data Types**:

- `expr`: Numeric types.
- `digits`: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**:

- If either `expr` or `digits` is NULL, it returns NULL.
- If `digits` is specified, it retains `digits` decimal places, with a default of 0.
- If the input value is of INTEGER type, regardless of the value of `digits`, it will only return INTEGER type without retaining decimals.
- A positive `digits` indicates rounding to that number of decimal places. If the number of decimal places is less than `digits`, no rounding occurs; it simply returns the value.
- A negative `digits` indicates truncating decimals, rounding to the left of the decimal point `digits` places. If the number of places to the left of the decimal point is less than `digits`, it returns 0.
- Since DECIMAL type is not currently supported, this function uses DOUBLE and FLOAT to represent results with decimals, but DOUBLE and FLOAT have a precision limit, making this function potentially meaningless when the number of digits is too high.
- Can only be used with regular columns, selection, or projection functions; cannot be used with aggregation functions.

**Example**:

```sql
taos> select round(8888.88);
      round(8888.88)       |
============================
      8889.000000000000000 |

taos> select round(8888.88,-1);
     round(8888.88,-1)     |
============================
      8890.000000000000000 |
```

#### SIN

```sql
SIN(expr)
```

**Function Description**: Returns the sine of the specified field.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**: Can only be used with regular columns, selection, or projection functions; cannot be used with aggregation functions.

#### SQRT

```sql
SQRT(expr)
```

**Function Description**: Returns the square root of the specified field.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**: Can only be used with regular columns, selection, or projection functions; cannot be used with aggregation functions.

#### TAN

```sql
TAN(expr)
```

**Function Description**: Returns the tangent of the specified field.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**: Can only be used with regular columns, selection, or projection functions; cannot be used with aggregation functions.

#### PI

```sql
PI()
```

**Function Description**: Returns the value of π (pi).

**Return Result Type**: DOUBLE.

**Applicable Data Types**: None.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**:

- π ≈ 3.141592653589793.
- Can only be used with regular columns, selection, or projection functions; cannot be used with aggregation functions.

**Example**:

```sql
taos> select pi();
           pi()            |
============================
         3.141592653589793 |
```

#### TRUNCATE

```sql
TRUNCATE(expr, digits)
```

**Function Description**: Returns the truncated value of the specified field to a specified number of digits.

**Return Result Type**: Matches the original data type of the `expr` field.

**Applicable Data Types**:

- `expr`: Numeric types.
- `digits`: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**:

- If either `expr` or `digits` is NULL, it returns NULL.
- Truncation is direct without rounding.
- A positive `digits` indicates truncation to that number of decimal places. If the number of decimal places is less than `digits`, no truncation occurs; it simply returns the value.
- A `digits` of zero indicates discarding decimal places.
- A negative `digits` indicates discarding decimal places and setting the digits to the left of the decimal point to 0. If the number of places to the left is less than `digits`, it returns 0.
- Since DECIMAL type is not currently supported, this function uses DOUBLE and FLOAT to represent results with decimals, but DOUBLE and FLOAT have a precision limit, making this function potentially meaningless when the number of digits is too high.
- Can only be used with regular columns, selection, or projection functions; cannot be used with aggregation functions.

**Example**:

```sql
taos> select truncate(8888.88, 0);
 truncate(8888.88, 0)    |
============================
    8888.000000000000000 |

taos> select truncate(8888.88, -1);
 truncate(8888.88, -1)   |
============================
    8880.000000000000000 |
```

#### EXP

```sql
EXP(expr)
```

**Function Description**: Returns e (the base of the natural logarithm) raised to the specified power.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**:

- If `expr` is NULL, it returns NULL.
- Can only be used with regular columns, selection, or projection functions; cannot be used with aggregation functions.

**Example**:

```sql
taos> select exp(2);
          exp(2)           |
============================
         7.389056098930650 |
```

#### LN

```sql
LN(expr)
```

**Function Description**: Returns the natural logarithm of the specified parameter.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**:

- If `expr` is NULL, it returns NULL.
- If `expr` is less than or equal to 0, it returns NULL.

**Example**:

```sql
taos> select ln(10);
          ln(10)           |
============================
         2.302585092994046 |
```

#### MOD

```sql
MOD(expr1, expr2)
```

**Function Description**: Calculates the result of expr1 % expr2.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**:

- If `expr2` is 0, it returns NULL.
- If either `expr1` or `expr2` is NULL, it returns NULL.
- Can only be used with regular columns, selection, or projection functions; cannot be used with aggregation functions.

**Example**:

```sql
taos> select mod(10,3);
         mod(10,3)         |
============================
         1.000000000000000 |

taos> select mod(1,0);
         mod(1,0)          |
============================
 NULL                      |
```

#### RAND

```sql
RAND([seed])
```

**Function Description**: Returns a random number uniformly distributed between 0 and 1.

**Return Result Type**: DOUBLE.

**Applicable Data Types**:

- `seed`: INTEGER.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**:

- If a `seed` value is specified, the specified `seed` is used as a random seed to ensure the generated random number sequence is deterministic.
- Can only be used with regular columns, selection, or projection functions; cannot be used with aggregation functions.

**Example**:

```sql
taos> select rand();
          rand()           |
============================
         0.202092426923147 |

taos> select rand();
          rand()           |
============================
         0.131537788143166 |

taos> select rand(1);
          rand(1)          |
============================
         0.000007826369259 |

taos> select rand(1);
          rand(1)          |
============================
         0.000007826369259 |
```

#### SIGN

```sql
SIGN(expr)
```

**Function Description**: Returns the sign of the specified parameter.

**Return Result Type**: Matches the original data type of the specified field.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**:

- If `expr` is negative, it returns -1.
- If `expr` is positive, it returns 1.
- If `expr` is 0, it returns 0.
- If `expr` is NULL, it returns NULL.

**Example**:

```sql
taos> select sign(-1);
       sign(-1)        |
========================
                    -1 |

taos> select sign(1);
        sign(1)        |
========================
                     1 |

taos> select sign(0);
        sign(0)        |
========================
                     0 |
```

#### DEGREES

```sql
DEGREES(expr)
```

**Function Description**: Converts the specified parameter from radians to degrees.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**:

- If `expr` is NULL, it returns NULL.
- degree = radian * 180 / π.

**Example**:

```sql
taos> select degrees(PI());
       degrees(pi())       |
============================
       180.000000000000000 |
```

#### RADIANS

```sql
RADIANS(expr)
```

**Function Description**: Converts the specified parameter from degrees to radians.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**:

- If `expr` is NULL, it returns NULL.
- radian = degree * π / 180.

**Example**:

```sql
taos> select radians(180);
       radians(180)        |
============================
         3.141592653589793 |
```

### String Functions

String functions take string-type input parameters and return results of either numeric or string types.

#### CHAR_LENGTH

```sql
CHAR_LENGTH(expr)
```

**Function Description**: Returns the length of the string in terms of character count.

**Return Result Type**: BIGINT.

**Applicable Data Types**: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**:

- Unlike the `LENGTH()` function, which counts bytes, `CHAR_LENGTH()` counts multibyte characters (like Chinese characters) as one character. For example, `CHAR_LENGTH('你好') = 2` while `LENGTH('你好') = 6`.
- If `expr` is NULL, it returns NULL.

**Example**:

```sql
taos> select char_length('Hello world');
 char_length('Hello world') |
=============================
                         11 |

taos> select char_length('你好 世界');
      char_length('你好 世界') |
===============================
                            5 |
```

#### CONCAT

```sql
CONCAT(expr1, expr2 [, expr] ...)
```

**Function Description**: String concatenation function.

**Return Result Type**: If all parameters are of VARCHAR type, the result type is VARCHAR. If any parameter includes NCHAR type, the result type is NCHAR. If any parameter contains NULL, the output is NULL.

**Applicable Data Types**: VARCHAR, NCHAR. The minimum number of parameters is 2, and the maximum is 8.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

#### CONCAT_WS

```sql
CONCAT_WS(separator_expr, expr1, expr2 [, expr] ...)
```

**Function Description**: String concatenation function with a separator.

**Return Result Type**: If all parameters are of VARCHAR type, the result type is VARCHAR. If any parameter includes NCHAR type, the result type is NCHAR. If any parameter contains NULL, the output is NULL.

**Applicable Data Types**: VARCHAR, NCHAR. The minimum number of parameters is 3, and the maximum is 9.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

#### LENGTH

```sql
LENGTH(expr)
```

**Function Description**: Returns the length in bytes.

**Return Result Type**: BIGINT.

**Applicable Data Types**: VARCHAR, NCHAR, VARBINARY.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

#### LOWER

```sql
LOWER(expr)
```

**Function Description**: Converts the string parameter value to all lowercase letters.

**Return Result Type**: Matches the original type of the input field.

**Applicable Data Types**: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

#### LTRIM

```sql
LTRIM(expr)
```

**Function Description**: Returns the string after removing leading spaces.

**Return Result Type**: Matches the original type of the input field.

**Applicable Data Types**: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

#### RTRIM

```sql
RTRIM(expr)
```

**Function Description**: Returns the string after removing trailing spaces.

**Return Result Type**: Matches the original type of the input field.

**Applicable Data Types**: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

#### TRIM

```sql
TRIM([{LEADING | TRAILING | BOTH} [remstr] FROM] expr)
TRIM([remstr FROM] expr)
```

**Function Description**: Returns the string `expr` with all prefixes or suffixes of `remstr` removed.

**Return Result Type**: Matches the original type of the input field `expr`.

**Applicable Data Types**:

- remstr: VARCHAR, NCHAR.
- expr: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**:

- The first optional variable [LEADING | BOTH | TRAILING] specifies which side of the string to trim:
  - LEADING removes the specified characters from the start of the string.
  - TRAILING removes the specified characters from the end of the string.
  - BOTH (default) removes the specified characters from both the start and end of the string.
- The second optional variable [remstr] specifies which characters to trim:
  - If `remstr` is not specified, spaces are trimmed by default.
  - `remstr` can specify multiple characters, e.g., `trim('ab' from 'abacd')` would yield 'acd'.
- If `expr` is NULL, it returns NULL.
- This function is multi-byte safe.

**Example**:

```sql
taos> select trim('        a         ');
 trim('        a         ') |
=============================
 a                          |

taos> select trim(leading from '        a         ');
 trim(leading from '        a         ') |
==========================================
 a                                       |


taos> select trim(leading 'b' from 'bbbbbbbba         ');
 trim(leading 'b' from 'bbbbbbbba         ') |
==============================================
 a                                           |

taos> select trim(both 'b' from 'bbbbbabbbbbb');
 trim(both 'b' from 'bbbbbabbbbbb') |
=====================================
 a                                  |
```

#### SUBSTRING/SUBSTR

```sql
SUBSTRING/SUBSTR(expr, pos [, len])
SUBSTRING/SUBSTR(expr FROM pos [FOR len])
```

**Function Description**: Returns the substring of string `expr` starting at position `pos`. If `len` is specified, it returns the substring starting at `pos` with length `len`.

**Return Result Type**: Matches the original type of the input field `expr`.

**Applicable Data Types**:

- `expr`: VARCHAR, NCHAR.
- `pos`: Integer type.
- `len`: Integer type.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**:

- If `pos` is a positive number, the result is the substring starting from `pos` from left to right.
- If `pos` is a negative number, the result is the substring starting from `pos` from right to left.
- Any parameter being NULL returns NULL.
- This function is multi-byte safe.
- If `len` is less than 1, it returns an empty string.
- `pos` is 1-based, if `pos` is 0, it returns an empty string.
- If `pos` + `len` exceeds `len(expr)`, it returns the substring from `pos` to the end of the string, equivalent to executing `substring(expr, pos)`.

**Example**:

```sql
taos> select substring('tdengine', 0);
 substring('tdengine', 0) |
===========================

taos> select substring('tdengine', 3);
 substring('tdengine', 3) |
===========================
 engine                   |

taos> select substring('tdengine', 3,3);
 substring('tdengine', 3,3) |
=============================
 eng                        |

taos> select substring('tdengine', -3,3);
 substring('tdengine', -3,3) |
==============================
 ine                         |

taos> select substring('tdengine', -3,-3);
 substring('tdengine', -3,-3) |
===============================
```

#### SUBSTRING_INDEX

```sql
SUBSTRING_INDEX(expr, delim, count)
```

**Function Description**: Returns the substring of `expr` cut at the position of the specified occurrence of the delimiter.

**Return Result Type**: Matches the original type of the input field `expr`.

**Applicable Data Types**:

- `expr`: VARCHAR, NCHAR.
- `delim`: VARCHAR, NCHAR.
- `count`: INTEGER.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**:

- If `count` is a positive number, the result is the substring to the left of the `count`-th occurrence of `delim`.
- If `count` is a negative number, the result is the substring to the right of the absolute value of `count`-th occurrence of `delim`.
- Any parameter being NULL returns NULL.
- This function is multi-byte safe.

**Example**:

```sql
taos> select substring_index('www.tdengine.com','.',2);
 substring_index('www.tdengine.com','.',2) |
============================================
 www.taosdata                              |

taos> select substring_index('www.tdengine.com','.',-2);
 substring_index('www.tdengine.com','.',-2) |
=============================================
 tdengine.com                               |
```

#### UPPER

```sql
UPPER(expr)
```

**Function Description**: Converts the string parameter value to all uppercase letters.

**Return Result Type**: Matches the original type of the input field.

**Applicable Data Types**: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

#### CHAR

```sql
CHAR(expr1 [, expr2] [, expr3] ...)
```

**Function Description**: Treats input parameters as integers and returns the corresponding ASCII characters.

**Return Result Type**: VARCHAR.

**Applicable Data Types**: Integer types, VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable For**: Tables and supertables.

**Usage Description**:

- Input values exceeding 255 will be converted into multi-byte results, e.g., `CHAR(256)` equals `CHAR(1,0)`, and `CHAR(256 * 256)` equals `CHAR(1,0,0)`.
- NULL values in input parameters are ignored.
- String-type input parameters will be processed as numeric types.
- If the corresponding character for the input parameter is a non-printable character, the return value may include the character, but it might not be displayable.

```text
taos> select char(77);
 char(77) |
===========
 M        |

taos> select char(77,77);
 char(77,77) |
==============
 MM          |

taos> select char(77 * 256 + 77);
 char(77 * 256 + 77) |
======================
 MM                  |

taos> select char(77,NULL,77);
 char(77,null,77) |
===================
 MM               | M          |

```

#### ASCII

```sql
ASCII(expr)
```

**Function Description**: Returns the ASCII code of the first character of the string.

**Return Data Type**: BIGINT.

**Applicable Data Types**: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- If `expr` is NULL, it returns NULL.
- If the first character of `expr` is a multi-byte character, only the ASCII code corresponding to the value of the first byte of that character is returned.

**Example**:

```sql
taos> select ascii('testascii');
 ascii('testascii') |
===================== 
                116 |
```

#### POSITION

```sql
POSITION(expr1 IN expr2)
```

**Function Description**: Calculates the position of the string `expr1` within the string `expr2`.

**Return Data Type**: BIGINT.

**Applicable Data Types**:

- `expr1`: VARCHAR, NCHAR.
- `expr2`: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- If either `expr1` or `expr2` is NULL, it returns NULL.
- If `expr1` does not exist in `expr2`, it returns 0.
- If `expr1` is an empty string, it is considered a successful match in `expr2`, returning 1.
- The position returned is 1-based.
- This function is multi-byte safe.

**Example**:

```sql
taos> select position('a' in 'cba');
 position('a' in 'cba') |
========================= 
                      3 |

taos> select position('' in 'cba');
 position('' in 'cba') |
======================== 
                     1 |

taos> select position('d' in 'cba');
 position('d' in 'cba') |
========================= 
                      0 |
```

#### REPLACE

```sql
REPLACE(expr, from_str, to_str)
```

**Function Description**: Replaces all occurrences of `from_str` in the string with `to_str`.

**Return Data Type**: Same as the original type of the input field `expr`.

**Applicable Data Types**:

- `expr`: VARCHAR, NCHAR.
- `from_str`: VARCHAR, NCHAR.
- `to_str`: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- This function is case-sensitive.
- If any parameter is NULL, it returns NULL.
- This function is multi-byte safe.

**Example**:

```sql
taos> select replace('aabbccAABBCC', 'AA', 'DD');
 replace('aabbccAABBCC', 'AA', 'DD') |
====================================== 
 aabbccDDBBCC                        |
```

#### REPEAT

```sql
REPEAT(expr, count)
```

**Function Description**: Returns a string formed by repeating the input string the specified number of times.

**Return Data Type**: Same as the original type of the input field `expr`.

**Applicable Data Types**:

- `expr`: VARCHAR, NCHAR.
- `count`: INTEGER.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- If `count \< 1`, it returns an empty string.
- If either `expr` or `count` is NULL, it returns NULL.

**Example**:

```sql
taos> select repeat('abc',5);
      repeat('abc',5)      |
============================ 
 abcabcabcabcabc           |
            
taos> select repeat('abc',-1);
 repeat('abc',-1) |
=================== 
                  |
```

### Conversion Functions

Conversion functions convert values from one data type to another.

#### CAST

```sql
CAST(expr AS type_name)
```

**Function Description**: Data type conversion function that returns the result of converting `expr` to the specified type `type_name`.

**Return Data Type**: The type specified in CAST (type_name).

**Applicable Data Types**: The type of the input parameter `expr` can be any type except JSON and VARBINARY. If `type_name` is VARBINARY, `expr` must be of VARCHAR type.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- Unsupported type conversions will result in an error.
- For supported types, values that cannot be converted correctly will be output according to the conversion function. Potential scenarios include:
  1) Invalid character situations when converting string types to numeric types, e.g., "a" may convert to 0 but will not trigger an error.
  2) When converting to numeric types, if the numeric value exceeds the range that `type_name` can represent, it will overflow but will not trigger an error.
  3) When converting to string types, if the converted length exceeds the specified length in `type_name`, it will truncate but will not trigger an error.

#### TO_ISO8601

```sql
TO_ISO8601(expr [, timezone])
```

**Function Description**: Converts a UNIX timestamp into the ISO8601 standard date-time format, with optional time zone information. The timezone parameter allows users to specify any time zone for the output. If omitted, the current client's system time zone is used.

**Return Data Type**: VARCHAR.

**Applicable Data Types**: INTEGER, TIMESTAMP.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- The timezone parameter accepts formats: [z/Z, +/-hhmm, +/-hh, +/-hh:mm]. For example, TO_ISO8601(1, "+00:00").
- The precision of the input timestamp is determined by the precision of the queried table; if no table is specified, the precision is milliseconds.

#### TO_JSON

```sql
TO_JSON(str_literal)
```

**Function Description**: Converts a string constant into JSON type.

**Return Data Type**: JSON.

**Applicable Data Types**: JSON strings in the form of '\{ "literal" : literal }'. '\{}' indicates a null value. Keys must be string literals, and values can be numeric literals, string literals, boolean literals, or null literals. Escape characters are not supported in str_literal.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable To**: Tables and supertables.

#### TO_UNIXTIMESTAMP

```sql
TO_UNIXTIMESTAMP(expr [, return_timestamp])

return_timestamp: {
    0
  | 1
}
```

**Function Description**: Converts a date-time formatted string into a UNIX timestamp.

**Return Data Type**: BIGINT, TIMESTAMP.

**Applicable Data Types**: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- The input date-time string must conform to the ISO8601/RFC3339 standard; unconvertible string formats will return NULL.
- The precision of the returned timestamp matches the current DATABASE setting.
- The return_timestamp parameter specifies whether the function returns a timestamp type. Setting it to 1 returns TIMESTAMP, setting it to 0 returns BIGINT. If not specified, it defaults to returning BIGINT.

#### TO_CHAR

```sql
TO_CHAR(ts, format_str_literal)
```

**Function Description**: Converts a timestamp type to a string according to a specified format.

**Version**: ver-3.2.2.0

**Return Data Type**: VARCHAR

**Applicable Data Types**: TIMESTAMP

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable To**: Tables and supertables.

**Supported Formats**:

| **Format**            | **Description**                                  | **Example**                  |
| --------------------- | ------------------------------------------------ | ----------------------------- |
| AM,am,PM,pm           | Morning and afternoon without dot separation     | 07:00:00am                   |
| A.M.,a.m.,P.M.,p.m.   | Morning and afternoon with dot separation        | 07:00:00a.m.                 |
| YYYY,yyyy             | Year, 4 or more digits                          | 2023-10-10                   |
| YYY,yyy               | Year, last 3 digits                             | 023-10-10                    |
| YY,yy                 | Year, last 2 digits                             | 23-10-10                     |
| Y,y                   | Year, last digit                                | 3-10-10                      |
| MONTH                 | Month, all uppercase                             | 2023-JANUARY-01              |
| Month                 | Month, first letter uppercase                    | 2023-January-01              |
| month                 | Month, all lowercase                             | 2023-january-01              |
| MM,mm                 | Month, numeric 01-12                            | 2023-01-01                   |
| DD,dd                 | Day of the month, 01-31                        |                             |
| DAY                   | Day of the week, all uppercase                   | MONDAY                       |
| Day                   | Day of the week, first character uppercase       | Monday                       |
| day                   | Day of the week, all lowercase                   | monday                       |
| DY                    | Day of the week, abbreviation, all uppercase     | MON                          |
| Dy                    | Day of the week, abbreviation, first character uppercase | Mon                          |
| dy                    | Day of the week, abbreviation, all lowercase      | mon                          |
| DDD                   | Day of the year, 001-366                        |                             |
| D,d                   | Day of the week, numeric, 1-7, Sunday(1) to Saturday(7) |                             |
| HH24,hh24             | Hour, 00-23                                    | 2023-01-30 23:59:59          |
| hh12,HH12, hh, HH     | Hour, 01-12                                    | 2023-01-30 12:59:59PM        |
| MI,mi                 | Minute, 00-59                                   |                             |
| SS,ss                 | Second, 00-59                                   |                             |
| MS,ms                 | Millisecond, 000-999                            |                             |
| US,us                 | Microsecond, 000000-999999                      |                             |
| NS,ns                 | Nanosecond, 000000000-999999999                |                             |
| TZH,tzh               | Timezone hour                                   | 2023-01-30 11:59:59PM +08    |

**Usage Instructions**:

- Formats like `Month`, `Day` are left-aligned, with spaces added to the right; e.g., `2023-OCTOBER  -01`, `2023-SEPTEMBER-01`. September has no space as it has the most letters.
- When using `ms`, `us`, `ns`, the output format differs only in precision. For example, for `ts` as `1697182085123`, `ms` outputs `123`, `us` outputs `123000`, `ns` outputs `123000000`.
- Content in the time format that cannot match the rules will output directly. To specify some parts of the format string to not convert, double quotes can be used, e.g., `to_char(ts, 'yyyy-mm-dd "is formatted by yyyy-mm-dd"')`. To output double quotes, add a backslash before the double quote, e.g., `to_char(ts, '\"yyyy-mm-dd\"')` will output `"2023-10-10"`.
- Output formats that are numeric, like `YYYY`, `DD`, have the same meaning in uppercase and lowercase; `yyyy` and `YYYY` can be interchanged.
- It is recommended to include timezone information in the time format; if not included, the default output timezone is that configured on the server or client.
- The precision of the input timestamp is determined by the precision of the queried table; if no table is specified, the precision is milliseconds.

#### TO_TIMESTAMP

```sql
TO_TIMESTAMP(ts_str_literal, format_str_literal)
```

**Function Description**: Converts a string into a timestamp according to a specified format.

**Version**: ver-3.2.2.0

**Return Data Type**: TIMESTAMP

**Applicable Data Types**: VARCHAR

**Nested Subquery Support**: Applicable to both inner and outer queries

**Applicable To**: Tables and supertables

**Supported Formats**: Same as `to_char`

**Usage Instructions**:

- If both `ms`, `us`, `ns` are specified, the output timestamp includes the sum of these three fields. For example, `to_timestamp('2023-10-10 10:10:10.123.000456.000000789', 'yyyy-mm-dd hh:mi:ss.ms.us.ns')` outputs `2023-10-10 10:10:10.123456789` as the corresponding timestamp.
- Formats like `MONTH`, `MON`, `DAY`, `DY`, and others where output is numeric, the uppercase and lowercase meaning is the same; e.g., `to_timestamp('2023-JANUARY-01', 'YYYY-month-dd')`, `month` can be replaced with `MONTH` or `Month`.
- If the same field is specified multiple times, the earlier specification will be overridden. For example, `to_timestamp('2023-22-10-10', 'yyyy-yy-MM-dd')` outputs the year as `2022`.
- To avoid converting with unexpected time zones, it is recommended to carry timezone information in the time, e.g., '2023-10-10 10:10:10+08'; if no timezone is specified, the default is that of the server or client.
- If the full time is not specified, the default time value is `1970-01-01 00:00:00` in the specified or default timezone, and unspecified parts will use corresponding parts from that default value. Currently, formats specifying only year and day without month (like 'yyyy-mm-DDD') are not supported; 'yyyy-mm-DD' is supported.
- If the format string includes `AM`, `PM`, etc., the hour must be in 12-hour format, ranging from 01-12.
- The `to_timestamp` conversion has a certain error tolerance; if the format string and timestamp string do not fully correspond, it can sometimes convert, e.g., `to_timestamp('200101/2', 'yyyyMM1/dd')`, where the extra 1 in the format string is discarded. Extra space characters (spaces, tabs, etc.) in the format string will also be ignored automatically, e.g., `to_timestamp('  23 年 - 1 月 - 01 日  ', 'yy 年-MM月-dd日')` can be successfully converted. Although fields like `MM` require two digits (zero-padded if one digit), a single digit can also successfully convert in `to_timestamp`.
- The precision of the output timestamp matches that of the queried table; if the table is not specified, the precision is milliseconds. For example, `select to_timestamp('2023-08-1 10:10:10.123456789', 'yyyy-mm-dd hh:mi:ss.ns')` will truncate the microseconds and nanoseconds. If a nanosecond table is specified, truncation will not occur, e.g., `select to_timestamp('2023-08-1 10:10:10.123456789', 'yyyy-mm-dd hh:mi:ss.ns') from db_ns.table_ns limit 1`.

### Time and Date Functions

Time and date functions operate on the timestamp type.

All functions returning the current time, such as NOW, TODAY, and TIMEZONE, will only be computed once per SQL statement, regardless of how many times they appear.

#### NOW

```sql
NOW()
```

**Function Description**: Returns the current system time of the client.

**Return Data Type**: TIMESTAMP.

**Applicable Fields**: In WHERE or INSERT statements, it can only act on TIMESTAMP type fields.

**Applicable To**: Tables and supertables.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Usage Instructions**:

- Supports time addition and subtraction, e.g., NOW() + 1s; the supported time units are as follows: b (nanoseconds), u (microseconds), a (milliseconds), s (seconds), m (minutes), h (hours), d (days), w (weeks).
- The precision of the returned timestamp matches the current DATABASE setting.

#### TIMEDIFF

```sql
TIMEDIFF(expr1, expr2 [, time_unit])
```

**Function Description**: Returns the result of the timestamp `expr1` - `expr2`, which may be negative, and approximates to the precision specified by the time unit `time_unit`.

**Return Data Type**: BIGINT.

**Applicable Data Types**:

- `expr1`: BIGINT, TIMESTAMP type representing UNIX timestamps, or VARCHAR, NCHAR types in date-time format.
- `expr2`: BIGINT, TIMESTAMP type representing UNIX timestamps, or VARCHAR, NCHAR types in date-time format.
- `time_unit`: See usage instructions.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- Supported time units for `time_unit` are as follows: 1b (nanoseconds), 1u (microseconds), 1a (milliseconds), 1s (seconds), 1m (minutes), 1h (hours), 1d (days), 1w (weeks).
- If the time unit `time_unit` is not specified, the precision of the returned time difference matches the current DATABASE setting.
- Input containing strings that do not conform to time date formats will return NULL.
- If `expr1` or `expr2` is NULL, it returns NULL.
- If `time_unit` is NULL, it is equivalent to not specifying a time unit.
- The precision of the input timestamp is determined by the precision of the queried table; if the table is not specified, the precision is milliseconds.

**Example**:

```sql
taos> select timediff('2022-01-01 08:00:00', '2022-01-01 08:00:01',1s);
 timediff('2022-01-01 08:00:00', '2022-01-01 08:00:01',1s) |
============================================================ 
                                                        -1 |

taos> select timediff('2022-01-01 08:00:01', '2022-01-01 08:00:00',1s);
 timediff('2022-01-01 08:00:01', '2022-01-01 08:00:00',1s) |
============================================================ 
                                                         1 |
```

#### TIMETRUNCATE

```sql
TIMETRUNCATE(expr, time_unit [, use_current_timezone])
```

**Function Description**: Truncates the timestamp according to the specified time unit `time_unit`.

**Return Data Type**: TIMESTAMP.

**Applicable Fields**: BIGINT, TIMESTAMP types representing UNIX timestamps, or VARCHAR, NCHAR types in date-time format.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- Supported time units for `time_unit` are as follows: 1b (nanoseconds), 1u (microseconds), 1a (milliseconds), 1s (seconds), 1m (minutes), 1h (hours), 1d (days), 1w (weeks).
- The precision of the returned timestamp matches the current DATABASE setting.
- The precision of the input timestamp is determined by the precision of the queried table; if the table is not specified, the precision is milliseconds.
- Input containing strings that do not conform to time date formats will return NULL.
- When truncating timestamps to a day (1d) or week (1w), you can specify whether to truncate based on the current time zone by setting the `use_current_timezone` parameter.
  Value 0 indicates truncation using the UTC time zone, while value 1 indicates truncation using the current time zone.
  For example, if the client-configured time zone is UTC+0800, `TIMETRUNCATE('2020-01-01 23:00:00', 1d, 0)` will return '2020-01-01 08:00:00' in Eastern Eight Zone time.
  However, when using `TIMETRUNCATE('2020-01-01 23:00:00', 1d, 1)`, the result will be '2020-01-01 00:00:00' in Eastern Eight Zone time.
  If `use_current_timezone` is not specified, its default value is 1.
- When truncating to a week (1w), the calculation of `TIMETRUNCATE` is based on the UNIX timestamp (1970-01-01 00:00:00 UTC). The UNIX timestamp starts on a Thursday, so all truncated dates are Thursdays.

#### TIMEZONE

```sql
TIMEZONE()
```

**Function Description**: Returns the current time zone information of the client.

**Return Data Type**: VARCHAR.

**Applicable Fields**: None.

**Applicable To**: Tables and supertables.

#### TODAY

```sql
TODAY()
```

**Function Description**: Returns the client's system time at midnight today.

**Return Data Type**: TIMESTAMP.

**Applicable Fields**: In WHERE or INSERT statements, it can only act on TIMESTAMP type fields.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- Supports time addition and subtraction, e.g., TODAY() + 1s; the supported time units are: b (nanoseconds), u (microseconds), a (milliseconds), s (seconds), m (minutes), h (hours), d (days), w (weeks).
- The precision of the returned timestamp matches the current DATABASE setting.

#### WEEK

```sql
WEEK(expr [, mode])
```

**Function Description**: Returns the week number of the input date.

**Return Data Type**: BIGINT.

**Applicable Data Types**:

- `expr`: BIGINT, TIMESTAMP type representing UNIX timestamps, or VARCHAR, NCHAR types in date-time format.
- `mode`: An integer between 0 - 7.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- If `expr` is NULL, it returns NULL.
- The precision of the input timestamp is determined by the precision of the queried table; if the table is not specified, the precision is milliseconds.
- The `mode` specifies whether the week starts on Sunday or Monday and the range of return values from 1 - 53 or 0 - 53. The following table describes the different modes and their corresponding calculation methods:

| Mode | First Day of the Week | Return Value Range | Calculation Method for Week 1         |
| ---- | ---------------------- | ------------------- | ------------------------------------- |
| 0    | Sunday                 | 0 - 53              | The first week that includes Sunday is Week 1 |
| 1    | Monday                 | 0 - 53              | The first week with four or more days is Week 1 |
| 2    | Sunday                 | 1 - 53              | The first week that includes Sunday is Week 1 |
| 3    | Monday                 | 1 - 53              | The first week with four or more days is Week 1 |
| 4    | Sunday                 | 0 - 53              | The first week with four or more days is Week 1 |
| 5    | Monday                 | 0 - 53              | The first week that includes Monday is Week 1 |
| 6    | Sunday                 | 1 - 53              | The first week with four or more days is Week 1 |
| 7    | Monday                 | 1 - 53              | The first week that includes Monday is Week 1 |

**Example**:

```sql
taos> select week('2000-01-01',0);
 week('2000-01-01',0)  |
======================== 
                     0 |

taos> select week('2000-01-01',1);
 week('2000-01-01',1)  |
======================== 
                     0 |

taos> select week('2000-01-01',2);
 week('2000-01-01',2)  |
======================== 
                    52 |

taos> select week('2000-01-01',3);
 week('2000-01-01',3)  |
======================== 
                    52 |
```

#### WEEKOFYEAR

```sql
WEEKOFYEAR(expr)
```

**Function Description**: Returns the week number of the input date.

**Return Data Type**: BIGINT.

**Applicable Data Types**: BIGINT, TIMESTAMP type representing UNIX timestamps, or VARCHAR, NCHAR types in date-time format.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- Equivalent to `WEEK(expr, 3)`, where the first day of the week is Monday, and the return value range is 1 - 53, with the first week having four or more days as Week 1.
- If `expr` is NULL, it returns NULL.
- The precision of the input timestamp is determined by the precision of the queried table; if the table is not specified, the precision is milliseconds.

**Example**:

```sql
taos> select weekofyear('2000-01-01');
 weekofyear('2000-01-01') |
=========================== 
                       52 |
```

#### WEEKDAY

```sql
WEEKDAY(expr)
```

**Function Description**: Returns the day of the week for the input date.

**Return Data Type**: BIGINT.

**Applicable Data Types**: BIGINT, TIMESTAMP type representing UNIX timestamps, or VARCHAR, NCHAR types in date-time format.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- Return values: 0 represents Monday, 1 represents Tuesday, ..., 6 represents Sunday.
- If `expr` is NULL, it returns NULL.
- The precision of the input timestamp is determined by the precision of the queried table; if the table is not specified, the precision is milliseconds.

**Example**:

```sql
taos> select weekday('2000-01-01');
 weekday('2000-01-01') |
======================== 
                     5 |
```

#### DAYOFWEEK

```sql
DAYOFWEEK(expr)
```

**Function Description**: Returns the day of the week for the input date.

**Return Data Type**: BIGINT.

**Applicable Data Types**: BIGINT, TIMESTAMP type representing UNIX timestamps, or VARCHAR, NCHAR types in date-time format.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- Return values: 1 represents Sunday, 2 represents Monday, ..., 7 represents Saturday.
- If `expr` is NULL, it returns NULL.
- The precision of the input timestamp is determined by the precision of the queried table; if the table is not specified, the precision is milliseconds.

**Example**:

```sql
taos> select dayofweek('2000-01-01');
 dayofweek('2000-01-01') |
========================== 
                       7 |
```

## Aggregate Functions

Aggregate functions return a single result row for each grouping of the query result set. Grouping can be specified using the GROUP BY clause or window partition clause; if none are specified, the entire result set is treated as a single group.

TDengine supports aggregate queries on data and provides the following aggregate functions.

### APERCENTILE

```sql
APERCENTILE(expr, p [, algo_type])
```

**Function Description**: Calculates the approximate percentile of values in a specified column of a table/supertable, similar to the PERCENTILE function but returns approximate results.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Applicable To**: Tables and supertables.

**Notes**:

- The value of `p` is in the range [0,100]; when `p` is 0, it is equivalent to MIN, and when `p` is 100, it is equivalent to MAX.
- `algo_type` can take values "default" or "t-digest". If "default" is input, the function uses a histogram-based algorithm for calculation. If "t-digest" is input, the t-digest algorithm calculates the approximate percentile. If `algo_type` is not specified, "default" is used.
- The approximate result of the "t-digest" algorithm is sensitive to the order of input data; different input order results in slight errors for supertable queries.

### AVG

```sql
AVG(expr)
```

**Function Description**: Calculates the average of the specified field.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Applicable To**: Tables and supertables.

### COUNT

```sql
COUNT({* | expr})
```

**Function Description**: Counts the number of records for the specified field.

**Return Data Type**: BIGINT.

**Applicable Data Types**: All types of fields.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- You can use an asterisk (\*) to substitute a specific field, returning the total record count when using an asterisk (\*).
- If the counted field is a specific column, it returns the number of non-NULL records in that column.

### ELAPSED

```sql
ELAPSED(ts_primary_key [, time_unit])
```

**Function Description**: The elapsed function expresses the length of time covered by continuous records over a statistical period; used in conjunction with the `twa` function, it can calculate the area under the statistical curve. When the INTERVAL clause is specified for the window, it returns the time range covered by data for each window within the specified time range; if there is no INTERVAL clause, it returns the time range covered by data for the entire specified time range. Note that the value returned by ELAPSED is not the absolute value of the time range but rather the absolute value divided by the number of units obtained from the `time_unit`.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: TIMESTAMP.

**Applicable To**: Tables, supertables, outer queries of nested queries.

**Notes**:

- The `ts_primary_key` parameter must be the first column of the table, which is a TIMESTAMP type primary key column.
- Returns in the time unit specified by the `time_unit` parameter, with the minimum being the database time resolution. If the `time_unit` parameter is not specified, the database time resolution is used as the unit. Supported time units are: 1b (nanoseconds), 1u (microseconds), 1a (milliseconds), 1s (seconds), 1m (minutes), 1h (hours), 1d (days), 1w (weeks).
- Can be used in combination with `interval`, returning the time difference of timestamps for each time window. Note that, except for the first and last time windows, the timestamp differences of the middle windows are the length of the window.
- The order by `asc`/`desc` does not affect the calculation results.
- For supertables, it must be used with the `group by tbname` clause and cannot be used directly.
- For basic tables, it is not supported to combine with the `group by` clause.
- For nested queries, it is only valid when the inner query outputs an implicit timestamp column. For example, `select elapsed(ts) from (select diff(value) from sub1)` will output the implicit timestamp column from the inner query; this is the primary key column and can be used as the first parameter of the elapsed function. Conversely, for example, `select elapsed(ts) from (select * from sub1)` will have lost the meaning of the primary key column when outputting `ts` to the outer layer, making it invalid to use the elapsed function. Moreover, since the elapsed function is strongly dependent on the timeline, a structure like `select elapsed(ts) from (select diff(value) from st group by tbname)` will return a calculable result but has no practical significance, and such usage will be restricted in the future.
- It is not supported to be mixed with the `leastsquares`, `diff`, `derivative`, `top`, `bottom`, `last_row`, `interp`, and other functions.

### LEASTSQUARES

```sql
LEASTSQUARES(expr, start_val, step_val)
```

**Function Description**: Calculates the fitting linear equation of the values in a column of a table. `start_val` is the initial value of the independent variable, and `step_val` is the step value of the independent variable.

**Return Data Type**: String expression (slope, intercept).

**Applicable Data Types**: `expr` must be numeric types.

**Applicable To**: Tables.

### SPREAD

```sql
SPREAD(expr)
```

**Function Description**: Calculates the difference between the maximum and minimum values of a column in a table.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: INTEGER, TIMESTAMP.

**Applicable To**: Tables and supertables.

### STDDEV/STDDEV_POP

```sql
STDDEV/STDDEV_POP(expr)
```

**Function Description**: Calculates the population standard deviation of a column in a table.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Applicable To**: Tables and supertables.

**Example**:

```sql
taos> select id from test_stddev;
     id      |
============== 
           1 | 
           2 | 
           3 | 
           4 | 
           5 |

taos> select stddev_pop(id) from test_stddev;
      stddev_pop(id)       |
============================ 
         1.414213562373095 |
```

### VAR_POP

```sql
VAR_POP(expr)
```

**Function Description**: Calculates the population variance of a column in a table.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Applicable To**: Tables and supertables.

**Example**:

```sql
taos> select id from test_var;
     id      |
============== 
           3 | 
           1 | 
           2 | 
           4 | 
           5 |

taos> select var_pop(id) from test_var;
        var_pop(id)        |
============================ 
         2.000000000000000 |
```

### SUM

```sql
SUM(expr)
```

**Function Description**: Calculates the sum of a column in a table/supertable.

**Return Data Type**: DOUBLE, BIGINT.

**Applicable Data Types**: Numeric types.

**Applicable To**: Tables and supertables.

### HYPERLOGLOG

```sql
HYPERLOGLOG(expr)
```

**Function Description**:

- Uses the hyperloglog algorithm to return the cardinality of a column. This algorithm significantly reduces memory usage when dealing with large data volumes; the returned cardinality is an estimate, with a standard error of 0.81%.
- For smaller datasets, this algorithm may not be very accurate; you can use `select count(data) from (select unique(col) as data from table)` to achieve this.

**Return Data Type**: INTEGER.

**Applicable Data Types**: Any type.

**Applicable To**: Tables and supertables.

### HISTOGRAM

```sql
HISTOGRAM(expr, bin_type, bin_description, normalized)
```

**Function Description**: Calculates the distribution of data according to user-specified intervals.

**Return Data Type**: If the normalization parameter `normalized` is set to 1, the return type is DOUBLE; otherwise, it is BIGINT.

**Applicable Data Types**: Numeric fields.

**Applicable To**: Tables and supertables.

**Detailed Description**:

- `bin_type` specifies the type of bins; valid inputs are "user_input", "linear_bin", "log_bin".
- `bin_description` describes how to generate the bin intervals, and for the three types of bins, the following formats (all in JSON format strings) apply:
  - "user_input": "[1, 3, 5, 7]"
    User specifies the exact values of the bins.
  - "linear_bin": "\{"start": 0.0, "width": 5.0, "count": 5, "infinity": true}"
    "start" is the starting point of the data, "width" is the offset for each bin, "count" is the total number of bins, "infinity" indicates whether to add (-inf, inf) as the interval start and end points, generating intervals of [-inf, 0.0, 5.0, 10.0, 15.0, 20.0, +inf].
  - "log_bin": "\{"start":1.0, "factor": 2.0, "count": 5, "infinity": true}"
    "start" is the starting point of the data, "factor" is the factor for exponential growth, "count" is the total number of bins, "infinity" indicates whether to add (-inf, inf) as the interval start and end points, generating intervals of [-inf, 1.0, 2.0, 4.0, 8.0, 16.0, +inf].
- The `normalized` parameter specifies whether to normalize the returned result to a range between 0 and 1. Valid inputs are 0 and 1.

### PERCENTILE

```sql
PERCENTILE(expr, p [, p1] ... )
```

**Function Description**: Calculates the percentiles of values in a column of a table.

**Return Data Type**: The minimum number of parameters for this function is 2, and the maximum is 11. It can return up to 10 percentile values at once. When the number of parameters is 2, it returns one percentile, which is of type DOUBLE; when the number of parameters exceeds 2, it returns a type of VARCHAR in the format of a JSON array containing multiple return values.

**Applicable Fields**: Numeric types.

**Applicable To**: Tables.

**Usage Instructions**:

- The value `p` is in the range 0≤*P*≤100; a value of 0 is equivalent to MIN, and a value of 100 is equivalent to MAX.
- When calculating multiple percentiles for the same column simultaneously, it is advisable to use a single PERCENTILE function with multiple parameters; this significantly reduces query response time.
  For instance, using the query `SELECT percentile(col, 90, 95, 99) FROM table` performs better than `SELECT percentile(col, 90), percentile(col, 95), percentile(col, 99) from table`.

## Selection Functions

Selection functions return one or more result rows based on semantics. Users can specify the output of the `ts` column or other columns (including `tbname` and tag columns) simultaneously, making it easier to know from which data row the selected values originate.

### BOTTOM

```sql
BOTTOM(expr, k)
```

**Function Description**: Returns the smallest *k* non-NULL values from a specified column in a table/supertable. If multiple records have the same value and selecting all would exceed the *k* limit, the system will randomly select the required number from the same value.

**Return Data Type**: Same as the applied field.

**Applicable Data Types**: Numeric types.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- The value of *k* must be in the range 1≤*k*≤100;
- The system simultaneously returns the associated timestamp column for that record;
- Note: The BOTTOM function does not support the FILL clause.

### FIRST

```sql
FIRST(expr)
```

**Function Description**: Returns the first non-NULL value written in a specified column in a table/supertable.

**Return Data Type**: Same as the applied field.

**Applicable Data Types**: All fields.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- If you want to return the first non-NULL value for each column, you can use FIRST(\*); when querying a supertable, if `multiResultFunctionStarReturnTags` is set to 0 (default), FIRST(\*) only returns ordinary columns of the supertable; if set to 1, it returns both ordinary columns and tag columns.
- If all values in a column of the result set are NULL, the return result for that column is also NULL;
- If all columns in the result set are NULL, no results will be returned.
- When querying a table with a composite primary key, if there are multiple records with the minimum timestamp, only the data corresponding to the minimum of the composite primary key is returned.

### INTERP

```sql
INTERP(expr [, ignore_null_values])
```

**Function Description**: Returns the recorded value or interpolated value of the specified column at a specified time section. The `ignore_null_values` parameter can be 0 or 1, where 1 indicates to ignore NULL values; the default value is 0.

**Return Data Type**: Same as the field type.

**Applicable Data Types**: Numeric types.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- INTERP is used to obtain the recorded value of a specified column at a specified time section; if no matching row data exists at that time section, interpolation will be performed based on the settings of the FILL parameter.
- The input data for INTERP is the specified column data; filtering can be done through conditional statements (WHERE clause) on the original column data, and if no filtering condition is specified, all data is input.
- INTERP must be used in conjunction with the RANGE, EVERY, and FILL keywords.
- The output time range for INTERP is specified according to the RANGE(timestamp1, timestamp2) field, where timestamp1 \<= timestamp2 must be satisfied. Here, `timestamp1` is the starting value of the output time range; if `timestamp1` meets the interpolation conditions, it will be the first record output. `timestamp2` is the ending value of the output time range; the last record output must not exceed `timestamp2`.
- INTERP determines the number of output records in the output time range based on the EVERY(time_unit) field, where time_unit can take time unit values: 1a (milliseconds), 1s (seconds), 1m (minutes), 1h (hours), 1d (days), 1w (weeks). For example, EVERY(500a) will perform interpolation every 500 milliseconds for the specified data.
- INTERP determines how to perform interpolation at each moment that meets the output condition based on the FILL field. Please refer to the [FILL Clause](../time-series-extensions/#fill-clause) for how to use the FILL clause.
- INTERP can use the `RANGE` field to specify interpolation for a single time point by only providing a unique timestamp; in this case, the EVERY field can be omitted. For example: `SELECT INTERP(col) FROM tb RANGE('2023-01-01 00:00:00') FILL(linear)`.
- When applied to supertables, INTERP will interpolate based on the data from all the subtables of that supertable sorted by the primary key column; it can also be used with `PARTITION BY tbname` to force the result down to a single timeline.
- INTERP can be used with the pseudo-column _irowts to return the timestamp corresponding to the interpolation point (supported from version 3.0.2.0 onwards).
- INTERP can also be used with the pseudo-column _isfilled to show whether the returned result is from the original record or produced by the interpolation algorithm (supported from version 3.0.3.0 onwards).
- When querying a table with a composite primary key, if there are multiple records with the same timestamp, only the data corresponding to the minimum composite primary key will participate in the calculation.
- `INTERP` support NEAR fill mode. When `FILL(NEAR)` is used, the nearest value to the interpolation point is used to fill the missing value. If there are multiple values with the same distance to the interpolation point, previous row is used. NEAR fill is not supported in stream computation. For example, `SELECT _irowts,INTERP(current) FROM test.meters RANGE('2017-07-22 00:00:00','2017-07-24 12:25:00') EVERY(1h) FILL(NEAR)`(supported from version 3.3.4.9 onwards).
- Psedo column `_irowts_origin` can be used along with `INTERP` only when using NEAR/NEXT/PREV fill mode, `_irowts_origin` supported from version 3.3.4.9 onwards.
- `INTERP` RANGE clause support INTERVAL extension(supported from version 3.3.4.9 onwards), like `RANGE('2023-01-01 00:00:00', 1d)`. The second parameter is the interval length, and the unit cannot use y(year), n(month). The interval length must be an integer, with no quotes, the value can't be 0. The interval length is used to restrict the search range from the time point specified. For example, `SELECT _irowts,INTERP(current) FROM test.meters RANGE('2017-07-22 00:00:00', 1d) FILL(NEAR, 1)`. The query will return the interpolation result of the current column within the range of 1 day from the time point '2017-07-22 00:00:00'. If there is no data within the range, the specified value in FILL will be used. Only FILL PREV/NEXT/NEAR is supported in this case. It's illegal to use `EVERY` clause and NOT specify values in FILL clause in this case. None data-point range clause with INTERVAL extension is not supported currently, like `RANGE('2017-07-22 00:00:00', '2017-07-22 12:00:00', 1h)` is not supported.

### LAST

```sql
LAST(expr)
```

**Function Description**: Returns the last non-NULL value written in a specified column in a table/supertable.

**Return Data Type**: Same as the applied field.

**Applicable Data Types**: All fields.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- If you want to return the last (maximum timestamp) non-NULL value for each column, you can use LAST(\*); when querying a supertable, if `multiResultFunctionStarReturnTags` is set to 0 (default), LAST(\*) only returns ordinary columns of the supertable; if set to 1, it returns both ordinary columns and tag columns.
- If all values in a column of the result set are NULL, the return result for that column is also NULL; if all columns in the result set are NULL, no results will be returned.
- When used for supertables, if there are multiple records with the same maximum timestamp, one will be randomly returned from among them, and there is no guarantee that the data selected will be consistent across multiple runs.
- When querying a table with a composite primary key, if there are multiple records with the maximum timestamp, only the data corresponding to the maximum of the composite primary key is returned.

### LAST_ROW

```sql
LAST_ROW(expr)
```

**Function Description**: Returns the last record of a table/supertable.

**Return Data Type**: Same as the applied field.

**Applicable Data Types**: All fields.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- If you want to return the last record (maximum timestamp) for each column, you can use LAST_ROW(\*); when querying a supertable, if `multiResultFunctionStarReturnTags` is set to 0 (default), LAST_ROW(\*) only returns ordinary columns of the supertable; if set to 1, it returns both ordinary columns and tag columns.
- When used for supertables, if there are multiple records with the same maximum timestamp, one will be randomly returned from among them, and there is no guarantee that the data selected will be consistent across multiple runs.
- Cannot be used with INTERVAL.
- Similar to the LAST function, for queries on tables with composite primary keys, if there are multiple records with the maximum timestamp, only the data corresponding to the maximum of the composite primary key is returned.

### MAX

```sql
MAX(expr)
```

**Function Description**: Returns the maximum value of a column in a table/supertable.

**Return Data Type**: Same as the applied field.

**Applicable Data Types**: Numeric types, VARCHAR, NCHAR.

**Applicable To**: Tables and supertables.

**Usage Instructions**: The `MAX` function can accept strings as input parameters, returning the largest string value.

### MIN

```sql
MIN(expr)
```

**Function Description**: Returns the minimum value of a column in a table/supertable.

**Return Data Type**: Same as the applied field.

**Applicable Data Types**: Numeric types, VARCHAR, NCHAR.

**Applicable To**: Tables and supertables.

**Usage Instructions**: The `MIN` function can accept strings as input parameters, returning the smallest string value.

### MODE

```sql
MODE(expr)
```

**Function Description**: Returns the most frequent value; if there are multiple values with the same frequency, one of them will be randomly output.

**Return Data Type**: Consistent with the input data type.

**Applicable Data Types**: All types of fields.

**Applicable To**: Tables and supertables.

### SAMPLE

```sql
SAMPLE(expr, k)
```

**Function Description**: Retrieves k sampling values from the data. The valid input range for parameter k is 1≤ k ≤ 1000.

**Return Data Type**: Same as the original data type.

**Applicable Data Types**: All types of fields.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable To**: Tables and supertables.

### TAIL

```sql
TAIL(expr, k [, offset_rows])
```

**Function Description**: Returns k consecutive records after skipping the last `offset_val` records, not ignoring NULL values. The `offset_val` can be omitted, in which case the last k records are returned. When an `offset_val` is input, this function is equivalent to `order by ts desc LIMIT k OFFSET offset_val`.

**Parameter Range**: k: [1,100] offset_val: [0,100].

**Return Data Type**: Same as the applied field.

**Applicable Data Types**: Any type except the timestamp primary key column.

**Applicable To**: Tables, supertables.

### TOP

```sql
TOP(expr, k)
```

**Function Description**: Returns the largest *k* non-NULL values from a specified column in a table/supertable. If multiple records have the same value, and selecting all would exceed the *k* limit, the system will randomly select the required number from the same value.

**Return Data Type**: Same as the applied field.

**Applicable Data Types**: Numeric types.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- The value of *k* must be in the range 1≤*k*≤100;
- The system simultaneously returns the associated timestamp column for that record;
- Note: The TOP function does not support the FILL clause.

### UNIQUE

```sql
UNIQUE(expr)
```

**Function Description**: Returns the value of the data that appears first in the column. This function is similar to DISTINCT. When querying a table with a composite primary key, if there are multiple records with the minimum timestamp, only the data corresponding to the minimum composite primary key is returned.

**Return Data Type**: Same as the applied field.

**Applicable Data Types**: All types of fields.

**Applicable To**: Tables and supertables.

## Time Series Specific Functions

Time series-specific functions are tailored by TDengine to meet the query scenarios of time series data. In general databases, achieving similar functionality usually requires complex query syntax and is inefficient. TDengine has built in these functions to minimize user operational costs.

### CSUM

```sql
CSUM(expr)
```

**Function Description**: Cumulative sum, ignoring NULL values.

**Return Data Type**: If the input column is an integer type, the return type is long (int64_t); if it is a floating point number, the return type is double precision (Double). If it is an unsigned integer type, the return type is unsigned long (uint64_t).

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- Does not support operations like +, -, \*, /; for example, `csum(col1) + csum(col2)`.
- Can only be used with aggregation functions. This function can be applied to both basic tables and supertables.

### DERIVATIVE

```sql
DERIVATIVE(expr, time_interval, ignore_negative)

ignore_negative: {
    0
  | 1
}
```

**Function Description**: Calculates the rate of change of the values in a specified column of a table. The length of the unit time interval can be specified using the `time_interval` parameter, with a minimum of 1 second (1s); the `ignore_negative` parameter can be set to either 0 or 1, where 1 indicates that negative values should be ignored. For queries on tables with composite primary keys, if multiple records exist with the same timestamp, only the data corresponding to the minimum composite primary key will participate in the calculation.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- It can be used together with related selection columns. For example: `select _rowts, DERIVATIVE() from`.

### DIFF

```sql
DIFF(expr [, ignore_option])

ignore_option: {
    0
  | 1
  | 2
  | 3
}
```

**Function Description**: Calculates the difference between the current column's effective value and the previous row's effective value in a specified column in a table. The `ignore_option` can take values 0|1|2|3, which can be omitted, with the default value being 0.

- `0` indicates not ignoring negative values or NULL values.
- `1` indicates treating negative values as NULL values.
- `2` indicates not ignoring negative values but ignoring NULL values.
- `3` indicates ignoring both negative values and NULL values.
- For queries on tables with composite primary keys, if there are multiple records with the same timestamp, only the data corresponding to the minimum composite primary key will participate in the calculation.

**Return Data Type**: For boolean, timestamp, and integer types, returns int_64; for floating types, returns double. If the `diff` result overflows, it will return the value after overflow.

**Applicable Data Types**: Numeric types, timestamps, and boolean types.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- `diff` calculates the difference between the current specific column and the most recent valid data of the same column. The most recent valid data of the same column is defined as the nearest non-null value with a smaller timestamp in that same column.
- For numeric types, the `diff` result is the corresponding arithmetic difference; for timestamp types, the difference is calculated according to the database's timestamp precision; for boolean types, true is treated as 1, and false is treated as 0.
- If the current row data is NULL or no valid data for the same column is found, the `diff` result will be NULL.
- When ignoring negative values (when `ignore_option` is set to 1 or 3), if the `diff` result is negative, the result is set to NULL, which will then be filtered according to the NULL filtering rules.
- If the `diff` result overflows, whether the result should be "ignored negative values" depends on whether the logical operation result is positive or negative; for example, 9223372036854775800 - (-9223372036854775806) exceeds the BIGINT range; the `diff` result will show the overflow value -10 but will not be treated as a negative value to ignore.
- A single statement can use one or multiple `diff`, and each `diff` can specify the same or different `ignore_option`. When multiple `diff` exist in a single statement, only if all `diff` results for a certain row are NULL, and all `ignore_option` are set to ignore NULL values, that row will be removed from the result set.
- It can be selected to use with related columns. For example: `select _rowts, DIFF() from`.
- When there is no composite primary key, if different subtables have records with the same timestamp, it will prompt "Duplicate timestamps not allowed".
- When using composite primary keys, the combination of timestamps and primary keys may be the same for different subtables. Which row is used depends on which row is found first, meaning that in this case, the results of multiple `diff()` runs may differ.

### IRATE

```sql
IRATE(expr)
```

**Function Description**: Calculates the instantaneous growth rate. It uses the last two sample values in a time interval to compute the instantaneous growth rate; if these two values show a decreasing trend, only the last value is taken for the calculation instead of using the difference between the two values. For queries on tables with composite primary keys, if there are multiple records with the same timestamp, only the data corresponding to the minimum composite primary key will participate in the calculation.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Applicable To**: Tables and supertables.

### MAVG

```sql
MAVG(expr, k)
```

**Function Description**: Calculates the moving average of k consecutive values. If the number of input rows is less than k, there will be no output.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- Does not support operations like +, -, \*, /; for example, `mavg(col1, k1) + mavg(col2, k1)`.
- Can only be used with ordinary columns and selection functions, not with aggregation functions.

### STATECOUNT

```sql
STATECOUNT(expr, oper, val)
```

**Function Description**: Returns the count of consecutive records that meet a certain condition, with the result appended as a new column for each row. The condition is determined by the parameters; if the condition is true, it adds 1; if false, it resets to -1; if the data is NULL, it skips that record.

**Parameter Range**:

- `oper`: "LT" (less than), "GT" (greater than), "LE" (less than or equal to), "GE" (greater than or equal to), "NE" (not equal), "EQ" (equal), case insensitive.
- `val`: Numeric type.

**Return Data Type**: INTEGER.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Not supported in subqueries.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- Cannot be used in conjunction with window operations, such as `interval/state_window/session_window`.

### STATEDURATION

```sql
STATEDURATION(expr, oper, val, unit)
```

**Function Description**: Returns the length of time for which a continuous record meets a certain condition, with the result appended as a new column for each row. The condition is determined by the parameters; if the condition is true, it adds the time length between two records (the length for the first record that meets the condition is recorded as 0); if false, it resets to -1; if the data is NULL, it skips that record.

**Parameter Range**:

- `oper`: 'LT' (less than), 'GT' (greater than), 'LE' (less than or equal to), 'GE' (greater than or equal to), 'NE' (not equal), 'EQ' (equal), case insensitive, but needs to be enclosed in single quotes.
- `val`: Numeric type.
- `unit`: The unit of time length; valid time unit values are: 1b (nanoseconds), 1u (microseconds), 1a (milliseconds), 1s (seconds), 1m (minutes), 1h (hours), 1d (days), 1w (weeks). If omitted, defaults to the current database precision.

**Return Data Type**: INTEGER.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Not supported in subqueries.

**Applicable To**: Tables and supertables.

**Usage Instructions**:

- Cannot be used in conjunction with window operations, such as `interval/state_window/session_window`.

### TWA

```sql
TWA(expr)
```

**Function Description**: Time-weighted average function. Calculates the time-weighted average of a specified column over a period in a table. For queries on tables with composite primary keys, if there are multiple records with the same timestamp, only the data corresponding to the minimum composite primary key will participate in the calculation.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Applicable To**: Tables and supertables.

## System Information Functions

### DATABASE

```sql
SELECT DATABASE();
```

**Description**: Returns the currently logged-in database. If no default database is specified during login, and the USE command has not been used to switch databases, it returns NULL.

### CLIENT_VERSION

```sql
SELECT CLIENT_VERSION();
```

**Description**: Returns the client version.

### SERVER_VERSION

```sql
SELECT SERVER_VERSION();
```

**Description**: Returns the server version.

### SERVER_STATUS

```sql
SELECT SERVER_STATUS();
```

**Description**: Checks whether all dnodes in the server are online; if so, it returns success; otherwise, it returns a connection error. To query the status of the cluster, it is recommended to use `SHOW CLUSTER ALIVE;`, which behaves differently from `SELECT SERVER_STATUS();`. If some nodes in the cluster are unavailable, it does not return an error but rather different status codes; for more details, refer to [SHOW CLUSTER ALIVE](../show-commands/#show-cluster-alive).

### CURRENT_USER

```sql
SELECT CURRENT_USER();
```

**Description**: Retrieves the current user.

## Geometry Functions

### Geometry Input Functions

#### ST_GeomFromText

```sql
ST_GeomFromText(VARCHAR WKT expr)
```

**Function Description**: Creates geometry data from a specified geometric value based on Well-Known Text (WKT) representation.

**Return Data Type**: GEOMETRY.

**Applicable Data Types**: VARCHAR.

**Applicable Table Types**: Basic tables and supertables.

**Usage Instructions**: The input can be one of the WKT strings, such as point (POINT), linestring (LINESTRING), polygon (POLYGON), multipoint (MULTIPOINT), multilinestring (MULTILINESTRING), multipolygon (MULTIPOLYGON), or geometry collection (GEOMETRYCOLLECTION). The output is the GEOMETRY data type defined in binary string form.

### Geometry Output Functions

#### ST_AsText

```sql
ST_AsText(GEOMETRY geom)
```

**Function Description**: Returns the specified Well-Known Text (WKT) representation from geometry data.

**Return Data Type**: VARCHAR.

**Applicable Data Types**: GEOMETRY.

**Applicable Table Types**: Basic tables and supertables.

**Usage Instructions**: The output can be one of the WKT strings, such as point (POINT), linestring (LINESTRING), polygon (POLYGON), multipoint (MULTIPOINT), multilinestring (MULTILINESTRING), multipolygon (MULTIPOLYGON), or geometry collection (GEOMETRYCOLLECTION).

### Geometry Relationship Functions

#### ST_Intersects

```sql
ST_Intersects(GEOMETRY geomA, GEOMETRY geomB)
```

**Function Description**: Compares two geometric objects and returns true if they intersect.

**Return Data Type**: BOOL.

**Applicable Data Types**: GEOMETRY, GEOMETRY.

**Applicable Table Types**: Basic tables and supertables.

**Usage Instructions**: If either of the two geometric objects shares a point, they intersect.

#### ST_Equals

```sql
ST_Equals(GEOMETRY geomA, GEOMETRY geomB)
```

**Function Description**: Returns TRUE if the given geometric objects are "spatially equal".

**Return Data Type**: BOOL.

**Applicable Data Types**: GEOMETRY, GEOMETRY.

**Applicable Table Types**: Basic tables and supertables.

**Usage Instructions**: "Spatially equal" means ST_Contains(A, B) = true and ST_Contains(B, A) = true, and the order of points may differ, but they represent the same geometric structure.

#### ST_Touches

```sql
ST_Touches(GEOMETRY geomA, GEOMETRY geomB)
```

**Function Description**: Returns TRUE if A and B intersect, but their interiors do not intersect.

**Return Data Type**: BOOL.

**Applicable Data Types**: GEOMETRY, GEOMETRY.

**Applicable Table Types**: Basic tables and supertables.

**Usage Instructions**: A and B share at least one common point, and these common points lie within at least one boundary. For point/point inputs, the relationship is always FALSE since points do not have boundaries.

#### ST_Covers

```sql
ST_Covers(GEOMETRY geomA, GEOMETRY geomB)
```

**Function Description**: Returns TRUE if every point in B is located inside (intersects the interior or boundary) the geometry A.

**Return Data Type**: BOOL.

**Applicable Data Types**: GEOMETRY, GEOMETRY.

**Applicable Table Types**: Basic tables and supertables.

**Usage Instructions**: A contains B means that no point in B is outside A.

#### ST_Contains

```sql
ST_Contains(GEOMETRY geomA, GEOMETRY geomB)
```

**Function Description**: Returns TRUE if A contains B, meaning that if geometry A contains geometry B.

**Return Data Type**: BOOL.

**Applicable Data Types**: GEOMETRY, GEOMETRY.

**Applicable Table Types**: Basic tables and supertables.

**Usage Instructions**: A contains B if and only if all points of B are located inside A (i.e., either inside or on the boundary) and A and B share at least one common point in their interiors.

#### ST_ContainsProperly

```sql
ST_ContainsProperly(GEOMETRY geomA, GEOMETRY geomB)
```

**Function Description**: Returns TRUE if every point in B is located inside A.

**Return Data Type**: BOOL.

**Applicable Data Types**: GEOMETRY, GEOMETRY.

**Applicable Table Types**: Basic tables and supertables.

**Usage Instructions**: None of the points in B are located on the boundary or outside A.
