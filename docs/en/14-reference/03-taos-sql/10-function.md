---
title: Functions
slug: /tdengine-reference/sql-manual/functions
---

## Single Row Functions

Single row functions return a result row for each row in the query results.

### Mathematical Functions

#### ABS

```sql
ABS(expr)
```

**Function Description**: Gets the absolute value of the specified field.

**Return Type**: Consistent with the original data type of the specified field.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**: Can only be used with normal columns, selection, and projection functions, not with aggregation functions.

#### ACOS

```sql
ACOS(expr)
```

**Function Description**: Gets the arccosine of the specified field.

**Return Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**: Can only be used with normal columns, selection, and projection functions, not with aggregation functions.

#### ASIN

```sql
ASIN(expr)
```

**Function Description**: Gets the arcsine of the specified field.

**Return Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**: Can only be used with normal columns, selection, and projection functions, not with aggregation functions.

#### ATAN

```sql
ATAN(expr)
```

**Function Description**: Gets the arctangent of the specified field.

**Return Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**: Can only be used with normal columns, selection, and projection functions, not with aggregation functions.

#### CEIL

```sql
CEIL(expr)
```

**Function Description**: Gets the ceiling of the specified field.

**Return Type**: Consistent with the original data type of the specified field.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Usage Notes**: Can only be used with normal columns, selection, and projection functions, not with aggregation functions.

#### COS

```sql
COS(expr)
```

**Function Description**: Gets the cosine of the specified field.

**Return Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**: Can only be used with normal columns, selection, and projection functions, not with aggregation functions.

#### FLOOR

```sql
FLOOR(expr)
```

**Function Description**: Gets the floor of the specified field.
 Other usage notes see [CEIL](#ceil) function description.

#### GREATEST

```sql
GREATEST(expr1, expr2[, expr]...)
```

**Function Description**: Get the maximum value of all input parameters. The minimum number of parameters for this function is 2.

**Version**: ver-3.3.6.0

**Return Type**: Refer to the comparison rules. The comparison type is the final return type.

**Applicable Data Types**:

- Numeric types: timestamp, bool, integer and floating point types
- Strings types: nchar and varchar types.

**Comparison rules**: The following rules describe the conversion method of the comparison operation:

- If any parameter is NULL, the comparison result is NULL.
- If all parameters in the comparison operation are string types, compare them as string types
- If all parameters are numeric types, compare them as numeric types.
- If there are both string types and numeric types in the parameters, according to the `compareAsStrInGreatest` configuration item, they are uniformly compared as strings or numeric values. By default, they are compared as strings.
- In all cases, when different types are compared, the comparison type will choose the type with a larger range for comparison. For example, when comparing integer types, if there is a BIGINT type, BIGINT will definitely be selected as the comparison type.

**Related configuration items**: Client configuration, compareAsStrInGreatest is 1, which means that both string types and numeric types are converted to string comparisons, and 0 means that they are converted to numeric types. The default is 1.

#### LEAST

```sql
LEAST(expr1, expr2[, expr]...)
```

**Function Description**: Get the minimum value of all input parameters. The rest of the description is the same as the [GREATEST](#greatest) function.

#### LOG

```sql
LOG(expr1[, expr2])
```

**Function Description**: Gets the logarithm of expr1 to the base expr2. If the expr2 parameter is omitted, it returns the natural logarithm of the specified field.

**Return Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**: Can only be used with normal columns, selection, and projection functions, not with aggregation functions.

#### POW

```sql
POW(expr1, expr2)
```

**Function Description**: Gets the power of expr1 raised to the exponent expr2.

**Return Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**: Can only be used with regular columns, selection (Selection), projection (Projection) functions, and cannot be used with aggregation (Aggregation) functions.

#### ROUND

```sql
ROUND(expr[, digits])
```

**Function Description**: Obtains the rounded result of the specified field.

**Return Result Type**: Consistent with the original data type of the specified field.

**Applicable Data Types**:

- `expr`: Numeric type.
- `digits`: Numeric type.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- If `expr` or `digits` is NULL, returns NULL.
- If `digits` is specified, it retains `digits` decimal places, default is 0.
- If the input value is of INTEGER type, regardless of the value of `digits`, it will only return INTEGER type, without retaining decimals.
- `digits` greater than zero means operating on the decimal places, rounding to `digits` decimal places. If the number of decimal places is less than `digits`, no rounding operation is performed, and it is returned directly.
- `digits` less than zero means discarding the decimal places and rounding the number to the left of the decimal point by `digits` places. If the number of places to the left of the decimal point is less than `digits`, returns 0.
- Since the DECIMAL type is not yet supported, this function will use DOUBLE and FLOAT to represent results containing decimals, but DOUBLE and FLOAT have precision limits, and using this function may be meaningless when there are too many digits.
- Can only be used with regular columns, selection (Selection), projection (Projection) functions, and cannot be used with aggregation (Aggregation) functions.
- `digits` is supported from version 3.3.3.0.

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

**Function Description**: Obtains the sine result of the specified field.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**: Can only be used with regular columns, selection (Selection), projection (Projection) functions, and cannot be used with aggregation (Aggregation) functions.

#### SQRT

```sql
SQRT(expr)
```

**Function Description**: Obtains the square root of the specified field.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**: Can only be used with regular columns, selection (Selection), projection (Projection) functions, and cannot be used with aggregation (Aggregation) functions.

#### TAN

```sql
TAN(expr)
```

**Function Description**: Obtains the tangent result of the specified field.

**Version**: ver-3.3.3.0

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**: Can only be used with regular columns, selection (Selection), projection (Projection) functions, and cannot be used with aggregation (Aggregation) functions.

#### PI

```sql
PI()
```

**Function Description**: Returns the value of π (pi).

**Return Result Type**: DOUBLE.

**Applicable Data Types**: None.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- π ≈ 3.141592653589793.
- Can only be used with regular columns, selection (Selection), projection (Projection) functions, and cannot be used with aggregation (Aggregation) functions.

**Example**:

```sql
taos> select pi();
           pi()            |
============================
         3.141592653589793 |
```

##### TRUNCATE

```sql
TRUNCATE(expr, digits)
```

**Function Description**: Gets the truncated value of the specified field to the specified number of digits.

**Version**: ver-3.3.3.0

**Return Type**: Consistent with the original data type of the `expr` field.

**Applicable Data Types**:

- `expr`: Numeric type.
- `digits`: Numeric type.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- If `expr` or `digits` is NULL, return NULL.
- Truncation is done directly to the specified number of digits without rounding.
- `digits` greater than zero means operating on the decimal places, truncating to `digits` decimal places. If the number of decimal places is less than `digits`, no truncation is performed, and the value is returned directly.
- `digits` equal to zero means dropping the decimal places.
- `digits` less than zero means dropping the decimal places and zeroing the positions to the left of the decimal point up to `digits`. If the number of positions to the left of the decimal point is less than `digits`, return 0.
- Since the DECIMAL type is not yet supported, this function uses DOUBLE and FLOAT to represent results containing decimals, but DOUBLE and FLOAT have precision limits, and using this function may be meaningless when the number of digits is too large.
- Can only be used with regular columns, selection, and projection functions, not with aggregation functions.

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

**Function Description**: Returns the value of e (the base of natural logarithms) raised to the specified power.

**Version**: ver-3.3.3.0

**Return Type**: DOUBLE.

**Applicable Data Types**: Numeric type.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- If `expr` is NULL, return NULL.
- Can only be used with regular columns, selection, and projection functions, not with aggregation functions.

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

**Version**: ver-3.3.3.0

**Return Type**: DOUBLE.

**Applicable Data Types**: Numeric type.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- If `expr` is NULL, return NULL.
- If `expr` is less than or equal to 0, return NULL.
- Can only be used with regular columns, selection, and projection functions, not with aggregation functions.

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

**Version**: ver-3.3.3.0

**Return Type**: DOUBLE.

**Applicable Data Types**: Numeric type.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- Returns NULL if `expr2` is 0.
- Returns NULL if `expr1` or `expr2` is NULL.
- Can only be used with regular columns, selection (Selection), and projection (Projection) functions, not with aggregation (Aggregation) functions.

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

**Function Description**: Returns a uniformly distributed random number from 0 to 1.

**Version**: ver-3.3.3.0

**Return Result Type**: DOUBLE.

**Applicable Data Types**:

- `seed`: INTEGER.

**Nested Subquery Support**: Applicable to inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- If a `seed` value is specified, it will use the specified `seed` as the random seed to ensure the generated random number sequence is deterministic.
- Can only be used with regular columns, selection (Selection), and projection (Projection) functions, not with aggregation (Aggregation) functions.

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

**Version**: ver-3.3.3.0

**Return Result Type**: Consistent with the original data type of the specified field.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- Returns -1 if `expr` is negative,
- Returns 1 if `expr` is positive,
- Returns 0 if `expr` is 0,
- Returns NULL if `expr` is NULL,
- Can only be used with regular columns, selection (Selection), and projection (Projection) functions, not with aggregation (Aggregation) functions.

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

**Function Description**: Calculates the value of the specified parameter converted from radians to degrees.

**Version**: ver-3.3.3.0

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage**:

- If `expr` is NULL, it returns NULL.
- degree = radian * 180 / π.
- Can only be used with regular columns, selection (Selection), and projection (Projection) functions, not with aggregation (Aggregation) functions.

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

**Function Description**: Calculates the value of the specified parameter converted from degrees to radians.

**Version**: ver-3.3.3.0

**Return Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage**:

- If `expr` is NULL, it returns NULL.
- radian = degree * π / 180.
- Can only be used with regular columns, selection (Selection), and projection (Projection) functions, not with aggregation (Aggregation) functions.

**Example**:

```sql
taos> select radians(180);
       radians(180)        |
============================
         3.141592653589793 |
```

#### CRC32

```sql
CRC32(expr)
```

**Function Description**: Returns the unsigned 32-bit integer that represents the Cyclic Redundancy Check (CRC).

**Return Type**: INT UNSIGNED.

**Applicable Data Types**: Suitable for any type.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- If `expr` is NULL, it returns NULL.
- if `expr` is the empty string, it returns 0.
- if `expr` is a non string, it is interpreted as a string.
- This function is multibyte safe.

**Example**:

```sql
taos> select crc32(NULL);
 crc32(null) |
==============
 NULL        |

taos> select crc32("");
  crc32("")  |
==============
           0 |

taos> select crc32(123);
 crc32(123)  |
==============
  2286445522 |

taos> select crc32(123.456);
 crc32(123.456) |
=================
      844093190 |

taos> select crc32(TO_TIMESTAMP("2000-01-01", "yyyy-mm-dd hh24:mi:ss"));
 crc32(to_timestamp("2000-01-01", "yyyy-mm-dd hh24:mi:ss")) |
=============================================================
                                                 2274736693 |

taos> select crc32("This is a string");
 crc32("This is a string") |
============================
                 141976383 |

taos> select crc32("这是一个字符串");
 crc32("这是一个字符串") |
========================
            1902862441 |

taos> select crc32(col_name) from ins_columns limit 10;
 crc32(col_name) |
==================
      3208210256 |
      3292663675 |
      3081158046 |
      1063017838 |
      2063623452 |
      3996452140 |
      2559042119 |
      3485334036 |
      3208210256 |
      3292663675 |
```

### String Functions

The input parameters for string functions are of string type, and the return results are of numeric type or string type.

#### CHAR_LENGTH

```sql
CHAR_LENGTH(expr)
```

**Function Description**: String length counted in characters.

**Return Type**: BIGINT.

**Applicable Data Types**: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage**:

- Unlike the `LENGTH()` function, for multibyte characters, such as Chinese characters, the `CHAR_LENGTH()` function counts them as one character, length 1, while `LENGTH()` calculates their byte count, length 3. For example, `CHAR_LENGTH('你好') = 2`, `LENGTH('你好') = 6`.
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
CONCAT(expr1, expr2 [, expr] ... )
```

**Function Description**: String concatenation function.

**Return Type**: If all parameters are of VARCHAR type, the result type is VARCHAR. If parameters include NCHAR type, the result type is NCHAR. If parameters include NULL values, the output is NULL.

**Applicable Data Types**: VARCHAR, NCHAR. The function requires a minimum of 2 parameters and a maximum of 8 parameters.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

#### CONCAT_WS

```sql
CONCAT_WS(separator_expr, expr1, expr2 [, expr] ... )
```

**Function Description**: String concatenation function with a separator.

**Return Type**: If all parameters are of VARCHAR type, the result type is VARCHAR. If parameters include NCHAR type, the result type is NCHAR. If parameters include NULL values, the output is NULL.

**Applicable Data Types**: VARCHAR, NCHAR. The function requires a minimum of 3 parameters and a maximum of 9 parameters.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

#### LENGTH

```sql
LENGTH(expr)
```

**Function Description**: Length in bytes.

**Return Result Type**: BIGINT.

**Applicable Data Types**: VARCHAR, NCHAR, VARBINARY.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

#### LOWER

```sql
LOWER(expr)
```

**Function Description**: Converts the string argument value to all lowercase letters.

**Return Result Type**: Same as the original type of the input field.

**Applicable Data Types**: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

#### LTRIM

```sql
LTRIM(expr)
```

**Function Description**: Returns the string after removing left-side spaces.

**Return Result Type**: Same as the original type of the input field.

**Applicable Data Types**: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

#### RTRIM

```sql
RTRIM(expr)
```

**Function Description**: Returns the string after removing right-side spaces.

**Return Result Type**: Same as the original type of the input field.

**Applicable Data Types**: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

#### TRIM

```sql
TRIM([{LEADING | TRAILING | BOTH} [remstr] FROM] expr)
TRIM([remstr FROM] expr)
```

**Function Description**: Returns the string expr with all prefixes or suffixes of remstr removed.

**Version**: ver-3.3.3.0

**Return Result Type**: Same as the original type of the input field expr.

**Applicable Data Types**:

- remstr: VARCHAR, NCHAR.
- expr: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- The first optional variable [LEADING | BOTH | TRAILING] specifies which side of the string to trim:
  - LEADING removes specified characters from the beginning of the string.
  - TRAILING removes specified characters from the end of the string.
  - BOTH (default) removes specified characters from both the beginning and the end of the string.
- The second optional variable [remstr] specifies the string to be trimmed:
  - If remstr is not specified, spaces are trimmed by default.
  - remstr can specify multiple characters, such as trim('ab' from 'abacd'), where 'ab' is treated as a whole to be trimmed, resulting in the trimmed result 'acd'.
- If expr is NULL, returns NULL.
- This function is multibyte safe.

**Examples**:

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

**Function Description**: Returns a substring of string `expr` starting at position `pos`. If `len` is specified, it returns the substring starting at position `pos` with length `len`.

**Return Result Type**: Same as the original type of the input field `expr`.

**Applicable Data Types**:

- `expr`: VARCHAR, NCHAR.
- `pos`: Integer type.
- `len`: Integer type.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- If `pos` is positive, the result is the substring of `expr` starting from the left to the right from position `pos`.
- If `pos` is negative, the result is the substring of `expr` starting from the right to the left from position `pos`.
- If any argument is NULL, returns NULL.
- This function is multi-byte safe.
- If `len` is less than 1, returns an empty string.
- `pos` is 1-based; if `pos` is 0, returns an empty string.
- If `pos` + `len` exceeds `len(expr)`, returns the substring from `pos` to the end of the string, equivalent to executing `substring(expr, pos)`.
- Function `SUBSTRING` is equal to `SUBSTR`, supported from ver-3.3.3.0.
- Syntax `SUBSTRING/SUBSTR(expr FROM pos [FOR len])` is supported from ver-3.3.3.0.

**Examples**:

```sql
taos> select substring('tdengine', 0);
 substring('tdengine', 0) |
===========================
                          |

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
                              |
```

#### SUBSTRING_INDEX

```sql
SUBSTRING_INDEX(expr, delim, count)
```

**Function Description**: Returns a substring of `expr` cut at the position where the delimiter appears the specified number of times.

**Version**: ver-3.3.3.0

**Return Result Type**: Same as the original type of the input field `expr`.

**Applicable Data Types**:

- `expr`: VARCHAR, NCHAR.
- `delim`: VARCHAR, NCHAR.
- `count`: INTEGER.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- If `count` is positive, the result is the substring of `expr` from the left to the right up to the position where `delim` appears for the `count` time.
- If `count` is negative, the result is the substring of `expr` from the right to the left up to the position where `delim` appears for the absolute value of `count`.
- If any argument is NULL, returns NULL.
- This function is multi-byte safe.

**Examples**:

```sql
taos> select substring_index('www.tdengine.com','.',2);
 substring_index('www.tdengine.com','.',2) |
============================================
 www.tdengine                              |

taos> select substring_index('www.tdengine.com','.',-2);
 substring_index('www.tdengine.com','.',-2) |
=============================================
 tdengine.com                               |
```

#### UPPER

```sql
UPPER(expr)
```

**Function Description**: Converts the string argument value to all uppercase letters.

**Return Result Type**: Same as the original type of the input field.

**Applicable Data Types**: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

#### CHAR

```sql
CHAR(expr1 [, expr2] [, expr3] ...)
```

**Function Description**: Treats the input parameters as integers and returns the characters corresponding to these integers in ASCII encoding.

**Version**: ver-3.3.3.0

**Return Result Type**: VARCHAR.

**Applicable Data Types**: Integer types, VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- Values exceeding 255 will be converted into multi-byte results, such as `CHAR(256)` equivalent to `CHAR(1,0)`, `CHAR(256 * 256)` equivalent to `CHAR(1,0,0)`.
- NULL values in input parameters will be skipped.
- If the input parameters are of string type, they will be converted to numeric type for processing.
- If the character corresponding to the input parameter is a non-printable character, the return value will still contain the character corresponding to that parameter, but it may not be displayed.
- This function can have at most 2^31 - 1 input parameters.

**Examples**:

```sql
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
 MM               |
```

#### ASCII

```sql
ASCII(expr)
```

**Function Description**: Returns the ASCII code of the first character of the string.

**Version**: ver-3.3.3.0

**Return Result Data Type**: BIGINT.

**Applicable Data Types**: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- If `expr` is NULL, return NULL.
- If the first character of `expr` is a multi-byte character, only the ASCII code of the first byte of that character will be returned.

**Examples**:

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

**Function Description**: Calculates the position of string `expr1` in string `expr2`.

**Version**: ver-3.3.3.0

**Return Result Type**: BIGINT.

**Applicable Data Types**:

- `expr1`: VARCHAR, NCHAR.
- `expr2`: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- If `expr1` or `expr2` is NULL, return NULL.
- If `expr1` does not exist in `expr2`, return 0.
- If `expr1` is an empty string, it is considered to always successfully match in `expr2`, returning 1.
- The returned position is 1-based.
- This function is multi-byte safe.

**Examples**:

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

**Version**: ver-3.3.3.0

**Return Type**: Same as the original type of the input field `expr`.

**Applicable Data Types**:

- `expr`: VARCHAR, NCHAR.
- `from_str`: VARCHAR, NCHAR.
- `to_str`: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- This function is case-sensitive.
- If any argument is NULL, returns NULL.
- This function is multibyte safe.

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

**Function Description**: Returns a string that repeats the string `expr` a specified number of times.

**Version**: ver-3.3.3.0

**Return Type**: Same as the original type of the input field `expr`.

**Applicable Data Types**:

- `expr`: VARCHAR, NCHAR.
- `count`: INTEGER.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- If `count < 1`, returns an empty string.
- If `expr` or `count` is NULL, returns NULL.

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

#### TO_BASE64

```sql
TO_BASE64(expr)
```

**Function Description**: Returns the base64 encoding of the string `expr`s.

**Return Type**: VARCHAR.

**Applicable Data Types**:

- `expr`: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- If `expr` is NULL, returns NULL.

**Example**:

```sql
taos> select to_base64("");
 to_base64("") |
================
               |

taos> select to_base64("Hello, world!");
 to_base64("Hello, world!") |
=============================
 SGVsbG8sIHdvcmxkIQ==       |

taos> select to_base64("你好 世界");
 to_base64("你好 世界")      |
==============================
 5L2g5aW9IOS4lueVjA==        |
```

### Conversion Functions

Conversion functions convert values from one data type to another.

#### CAST

```sql
CAST(expr AS type_name)
```

**Function Description**: Data type conversion function, returns the result of converting `expr` to the type specified by `type_name`.

**Return Type**: The type specified in CAST (`type_name`).

**Applicable Data Types**: The type of input parameter `expr` can be any type except JSON and VARBINARY. If `type_name` is VARBINARY, then `expr` must be of VARCHAR type.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- Unsupported type conversions will result in an error.
- For supported types, if some values cannot be correctly converted, the output of the conversion function will prevail. Current possible scenarios include:
        1) Invalid character situations when converting string types to numeric types, e.g., "a" might convert to 0, but will not throw an error.
        2) When converting to numeric types, if the value exceeds the range that `type_name` can represent, it will overflow, but will not throw an error.
        3) When converting to string types, if the converted length exceeds the length specified in `type_name`, it will be truncated, but will not throw an error.
- The DECIMAL type does not support conversion to or from JSON, VARBINARY, or GEOMETRY types.

#### TO_ISO8601

```sql
TO_ISO8601(expr [, timezone])
```

**Function Description**: Converts a timestamp into the ISO8601 standard date and time format, with additional timezone information. The `timezone` parameter allows users to specify any timezone information for the output. If the `timezone` parameter is omitted, the output will include the current client system's timezone information.

**Return Data Type**: VARCHAR type.

**Applicable Data Types**: INTEGER, TIMESTAMP.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- The `timezone` parameter accepts timezone formats: [z/Z, +/-hhmm, +/-hh, +/-hh:mm]. For example, TO_ISO8601(1, "+00:00").
- The precision of the input timestamp is determined by the precision of the table queried, if no table is specified, the precision is milliseconds.

#### TO_JSON

```sql
TO_JSON(str_literal)
```

**Function Description**: Converts a string literal to JSON type.

**Return Data Type**: JSON.

**Applicable Data Types**: JSON strings, in the form '\{ "literal" : literal }'. '\{}' represents a null value. Keys must be string literals, and values can be numeric literals, string literals, boolean literals, or null literals. Escape characters are not supported in str_literal.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

#### TO_UNIXTIMESTAMP

```sql
TO_UNIXTIMESTAMP(expr [, return_timestamp])

return_timestamp: {
    0
  | 1
}
```

**Function Description**: Converts a datetime format string into a timestamp.

**Return Data Type**: BIGINT, TIMESTAMP.

**Applicable Fields**: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- The input datetime string must conform to the ISO8601/RFC3339 standards, and formats that cannot be converted will return NULL.
- The precision of the returned timestamp is consistent with the time precision setting of the current DATABASE.
- return_timestamp specifies whether the function's return value is of TIMESTAMP type; setting it to 1 returns TIMESTAMP type, setting it to 0 returns BIGINT type. If not specified, it defaults to BIGINT type.

#### TO_CHAR

```sql
TO_CHAR(ts, format_str_literal)
```

**Function Description**: Converts a timestamp type to a string according to the specified format

**Version**: ver-3.2.2.0

**Return Data Type**: VARCHAR

**Applicable Fields**: TIMESTAMP

**Nested Subquery Support**: Applicable to both inner and outer queries

**Applicable to**: Tables and supertables

Supported Formats:

| **Format**            | **Description**                           | **Example**               |
| ------------------- | ----------------------------------------- | ------------------------- |
| AM,am,PM,pm         | AM/PM without dots                        | 07:00:00am                |
| A.M.,a.m.,P.M.,p.m. | AM/PM with dots                           | 07:00:00a.m.              |
| YYYY,yyyy           | Year, 4 or more digits                    | 2023-10-10                |
| YYY,yyy             | Year, last 3 digits                       | 023-10-10                 |
| YY,yy               | Year, last 2 digits                       | 23-10-10                  |
| Y,y                 | Year, last digit                          | 3-10-10                   |
| MONTH               | Month, uppercase                          | 2023-JANUARY-01           |
| Month               | Month, first letter uppercase             | 2023-January-01           |
| month               | Month, lowercase                          | 2023-january-01           |
| MON                 | Month, abbreviation, uppercase (three characters) | JAN, SEP              |
| Mon                 | Month, abbreviation, first letter uppercase | Jan, Sep                |
| mon                 | Month, abbreviation, lowercase            | jan, sep                  |
| MM,mm               | Month, numeric 01-12                      | 2023-01-01                |
| DD,dd               | Day of the month, 01-31                   |                           |
| DAY                 | Day of the week, uppercase                | MONDAY                    |
| Day                 | Day of the week, first letter uppercase   | Monday                    |
| day                 | Day of the week, lowercase                | monday                    |
| DY                  | Day of the week, abbreviation, uppercase  | MON                       |
| Dy                  | Day of the week, abbreviation, first letter uppercase | Mon             |
| dy                  | Day of the week, abbreviation, lowercase  | mon                       |
| DDD                 | Day of the year, 001-366                  |                           |
| D,d                 | Day of the week, numeric, 1-7, Sunday(1) to Saturday(7) |               |
| HH24,hh24           | Hour, 00-23                               | 2023-01-30 23:59:59       |
| hh12,HH12, hh, HH   | Hour, 01-12                               | 2023-01-30 12:59:59PM     |
| MI,mi               | Minute, 00-59                             |                           |
| SS,ss               | Second, 00-59                             |                           |
| MS,ms               | Millisecond, 000-999                      |                           |
| US,us               | Microsecond, 000000-999999                |                           |
| NS,ns               | Nanosecond, 000000000-999999999           |                           |
| TZH,tzh             | Time zone hours                           | 2023-01-30 11:59:59PM +08 |

**Usage Instructions**:

- The output format for `Month`, `Day`, etc., is left-aligned with spaces added to the right, such as `2023-OCTOBER  -01`, `2023-SEPTEMBER-01`. September has the longest number of letters among the months, so there is no space for September. Weeks are similar.
- When using `ms`, `us`, `ns`, the output of the above three formats only differs in precision, for example, if ts is `1697182085123`, the output for `ms` is `123`, for `us` is `123000`, and for `ns` is `123000000`.
- Content in the time format that does not match the rules will be output directly. If you want to specify parts of the format string that can match rules not to be converted, you can use double quotes, like `to_char(ts, 'yyyy-mm-dd "is formatted by yyyy-mm-dd"')`. If you want to output double quotes, then add a backslash before the double quotes, like `to_char(ts, '\"yyyy-mm-dd\"')` will output `"2023-10-10"`.
- Formats that output numbers, such as `YYYY`, `DD`, uppercase and lowercase have the same meaning, i.e., `yyyy` and `YYYY` are interchangeable.
- It is recommended to include timezone information in the time format; if not included, the default output timezone is the timezone configured by the server or client.
- The precision of the input timestamp is determined by the precision of the table queried; if no table is specified, then the precision is milliseconds.

#### TO_TIMESTAMP

```sql
TO_TIMESTAMP(ts_str_literal, format_str_literal)
```

**Function Description**: Converts a string to a timestamp according to the specified format.

**Version**: ver-3.2.2.0

**Return Result Data Type**: TIMESTAMP

**Applicable Fields**: VARCHAR

**Nested Subquery Support**: Applicable to inner and outer queries

**Applicable to**: Tables and supertables

**Supported Formats**: Same as `to_char`

**Usage Instructions**:

- If `ms`, `us`, `ns` are specified at the same time, then the resulting timestamp includes the sum of these three fields. For example, `to_timestamp('2023-10-10 10:10:10.123.000456.000000789', 'yyyy-mm-dd hh:mi:ss.ms.us.ns')` outputs the timestamp corresponding to `2023-10-10 10:10:10.123456789`.
- `MONTH`, `MON`, `DAY`, `DY` and other formats that output numbers have the same meaning in uppercase and lowercase, such as `to_timestamp('2023-JANUARY-01', 'YYYY-month-dd')`, `month` can be replaced with `MONTH` or `Month`.
- If the same field is specified multiple times, the earlier specification will be overridden. For example, `to_timestamp('2023-22-10-10', 'yyyy-yy-MM-dd')`, the output year is `2022`.
- To avoid using an unintended timezone during conversion, it is recommended to carry timezone information in the time, for example, '2023-10-10 10:10:10+08'; if no timezone is specified, the default timezone is the one specified by the server or client.
- If a complete time is not specified, then the default time value is `1970-01-01 00:00:00` in the specified or default timezone, and the unspecified parts use the corresponding parts of this default value. Formats that only specify the year and day without specifying the month and day, like 'yyyy-mm-DDD', are not supported, but 'yyyy-mm-DD' is supported.
- If the format string contains `AM`, `PM`, etc., then the hour must be in 12-hour format, ranging from 01-12.
- `to_timestamp` conversion has a certain tolerance mechanism; even when the format string and timestamp string do not completely correspond, conversion is sometimes possible, like: `to_timestamp('200101/2', 'yyyyMM1/dd')`, the extra 1 in the format string will be discarded. Extra whitespace characters (spaces, tabs, etc.) in the format string and timestamp string will also be automatically ignored. Although fields like `MM` require two digits (with a leading zero if only one digit), in `to_timestamp`, a single digit can also be successfully converted.
- The precision of the output timestamp is the same as the precision of the queried table; if no table is specified, then the output precision is milliseconds. For example, `select to_timestamp('2023-08-1 10:10:10.123456789', 'yyyy-mm-dd hh:mi:ss.ns')` will truncate microseconds and nanoseconds. If a nanosecond table is specified, truncation will not occur, like `select to_timestamp('2023-08-1 10:10:10.123456789', 'yyyy-mm-dd hh:mi:ss.ns') from db_ns.table_ns limit 1`.

### Time and Date Functions

Time and date functions operate on timestamp types.

All functions that return the current time, such as NOW, TODAY, and TIMEZONE, are calculated only once in a SQL statement, no matter how many times they appear.

#### NOW

```sql
NOW()
```

**Function Description**: Returns the current system time of the client.

**Return Result Data Type**: TIMESTAMP.

**Applicable Fields**: When used in WHERE or INSERT statements, it can only be applied to fields of TIMESTAMP type.

**Applicable to**: Tables and supertables.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Usage Instructions**:

- Supports time addition and subtraction operations, such as NOW() + 1s. Supported time units include:
        b(nanoseconds), u(microseconds), a(milliseconds), s(seconds), m(minutes), h(hours), d(days), w(weeks).
- The precision of the returned timestamp is consistent with the time precision set in the current DATABASE.

#### TIMEDIFF

```sql
TIMEDIFF(expr1, expr2 [, time_unit])
```

**Function Description**: Returns the result of the timestamp `expr1` - `expr2`, which may be negative, and approximated to the precision specified by the `time_unit`.

**Return Result Type**: BIGINT.

**Applicable Data Types**:

- `expr1`: BIGINT, TIMESTAMP types representing timestamps, or VARCHAR, NCHAR types in ISO8601/RFC3339 standard date-time format.
- `expr2`: BIGINT, TIMESTAMP types representing timestamps, or VARCHAR, NCHAR types in ISO8601/RFC3339 standard date-time format.
- `time_unit`: See usage instructions.
- `timediff` return the absolute value of the difference between timestamp `expr1` and `expr2` before ver-3.3.3.0.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- Supported time units `time_unit` include: 1b(nanoseconds), 1u(microseconds), 1a(milliseconds), 1s(seconds), 1m(minutes), 1h(hours), 1d(days), 1w(weeks).
- If the time unit `time_unit` is not specified, the precision of the returned time difference is consistent with the time precision set in the current DATABASE.
- Returns NULL if the input contains strings that do not conform to the date-time format.
- Returns NULL if `expr1` or `expr2` is NULL.
- If `time_unit` is NULL, it is equivalent to the time unit not being specified.
- The precision of the input timestamp is determined by the precision of the table being queried; if no table is specified, the precision is milliseconds.

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

use_current_timezone: {
    0
  | 1
}
```

**Function Description**: Truncates the timestamp according to the specified time unit `time_unit`.

**Return Result Data Type**: TIMESTAMP.

**Applicable Fields**: BIGINT, TIMESTAMP types representing timestamps, or VARCHAR, NCHAR types in ISO8601/RFC3339 standard date-time format.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- Supported time units `time_unit` include:
          1b(nanoseconds), 1u(microseconds), 1a(milliseconds), 1s(seconds), 1m(minutes), 1h(hours), 1d(days), 1w(weeks).
- The precision of the returned timestamp is consistent with the time precision set in the current DATABASE.
- The precision of the input timestamp is determined by the precision of the table being queried; if no table is specified, the precision is milliseconds.
- Returns NULL if the input contains strings that do not conform to the date-time format.
- When using 1d/1w as the time unit to truncate timestamps, the `use_current_timezone` parameter can be set to specify whether to truncate based on the current timezone.
  A value of 0 means truncation using the UTC timezone, and a value of 1 means truncation using the current timezone.
  For example, if the client's configured timezone is UTC+0800, then TIMETRUNCATE('2020-01-01 23:00:00', 1d, 0) returns the East Eight Zone time '2020-01-01 08:00:00'.
  Using TIMETRUNCATE('2020-01-01 23:00:00', 1d, 1) returns the East Eight Zone time '2020-01-01 00:00:00'.
  When `use_current_timezone` is not specified, the default value is 1.
- When truncating the time value to a week (1w), the calculation of timetruncate is based on the Unix timestamp (January 1, 1970, 00:00:00 UTC). Since the Unix timestamp starts on a Thursday,
  all truncated dates are Thursdays.

#### TIMEZONE

```sql
TIMEZONE()
```

**Function Description**: Returns the current timezone information of the client.

**Return Data Type**: VARCHAR.

**Applicable Fields**: None

**Applicable to**: Tables and supertables.

#### TODAY

```sql
TODAY()
```

**Function Description**: Returns the system time at midnight of the current day for the client.

**Return Data Type**: TIMESTAMP.

**Applicable Fields**: Can only be used with TIMESTAMP type fields when used in WHERE or INSERT statements.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- Supports time addition and subtraction operations, such as TODAY() + 1s. Supported time units include:
                b(nanoseconds), u(microseconds), a(milliseconds), s(seconds), m(minutes), h(hours), d(days), w(weeks).
- The precision of the returned timestamp is consistent with the time precision set for the current DATABASE.

#### WEEK

```sql
WEEK(expr [, mode])
```

**Function Description**: Returns the week number of the input date.

**Version**: ver-3.3.3.0

**Return Result Type**: BIGINT.

**Applicable Data Types**:

- `expr`: BIGINT, TIMESTAMP type representing a timestamp, or VARCHAR, NCHAR type conforming to ISO8601/RFC3339 date and time standards.
- `mode`: An integer between 0 - 7.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- If `expr` is NULL, returns NULL.
- The precision of the input timestamp is determined by the precision of the table queried; if no table is specified, the precision is milliseconds.
- `mode` is used to specify whether the week starts on Sunday or Monday, and whether the return value range is 1 - 53 or 0 - 53. The following table details the calculation methods corresponding to different modes:

| Mode | First Day of the Week | Return Value Range  | Calculation Method for the 1st Week         |
| ---- | --------------------- | ------------------- | ------------------------------------------ |
| 0    | Sunday                | 0 - 53              | The first week containing Sunday is week 1 |
| 1    | Monday                | 0 - 53              | The first week containing at least four days is week 1 |
| 2    | Sunday                | 1 - 53              | The first week containing Sunday is week 1 |
| 3    | Monday                | 1 - 53              | The first week containing at least four days is week 1 |
| 4    | Sunday                | 0 - 53              | The first week containing at least four days is week 1 |
| 5    | Monday                | 0 - 53              | The first week containing Monday is week 1 |
| 6    | Sunday                | 1 - 53              | The first week containing at least four days is week 1 |
| 7    | Monday                | 1 - 53              | The first week containing Monday is week 1 |

- When the return value range is 0 - 53, dates before the 1st week are considered week 0.
- When the return value range is 1 - 53, dates before the 1st week are considered the last week of the previous year.
- For example, with `2000-01-01`,
  - In `mode=0`, the return value is `0` because the first Sunday of that year is `2000-01-02`, making `2000-01-02` the start of week 1, thus `2000-01-01` is week 0, returning 0.
  - In `mode=1`, the return value is `0` because the week containing `2000-01-01` only has two days, `2000-01-01 (Saturday)` and `2000-01-02 (Sunday)`, making `2000-01-03` the start of the first week, thus `2000-01-01` is week 0, returning 0.
  - In `mode=2`, the return value is `52` because `2000-01-02` starts week 1, and with the return value range being 1-53, `2000-01-01` is considered the last week of the previous year, i.e., the 52nd week of 1999, returning 52.

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

**Version**: ver-3.3.3.0

**Return Type**: BIGINT.

**Applicable Data Types**: BIGINT, TIMESTAMP types representing timestamps, or VARCHAR, NCHAR types in ISO8601/RFC3339 date-time format.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage**:

- Equivalent to `WEEK(expr, 3)`, where the first day of the week is Monday, and the return value ranges from 1 to 53, with the first week containing four or more days being week 1.
- If `expr` is NULL, returns NULL.
- The precision of the input timestamp is determined by the precision of the table queried; if no table is specified, the precision is milliseconds.

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

**Function Description**: Returns the weekday of the input date.

**Version**: ver-3.3.3.0

**Return Type**: BIGINT.

**Applicable Data Types**: BIGINT, TIMESTAMP types representing timestamps, or VARCHAR, NCHAR types in ISO8601/RFC3339 date-time format.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage**:

- Return value 0 represents Monday, 1 represents Tuesday ... 6 represents Sunday.
- If `expr` is NULL, returns NULL.
- The precision of the input timestamp is determined by the precision of the table queried; if no table is specified, the precision is milliseconds.

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

**Function Description**: Returns the weekday of the input date.

**Version**: ver-3.3.3.0

**Return Type**: BIGINT.

**Applicable Data Types**: BIGINT, TIMESTAMP types representing timestamps, or VARCHAR, NCHAR types in ISO8601/RFC3339 date-time format.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage**:

- Return value 1 represents Sunday, 2 represents Monday ... 7 represents Saturday.
- If `expr` is NULL, returns NULL.
- The precision of the input timestamp is determined by the precision of the table queried; if no table is specified, the precision is milliseconds.

**Example**:

```sql
taos> select dayofweek('2000-01-01');
 dayofweek('2000-01-01') |
==========================
                       7 |
```

## Aggregate Functions

Aggregate functions return a single result row for each group of the result set of a query. Groups can be specified by a GROUP BY or window partition clause; if none is specified, the entire result set is considered a single group.

TDengine supports aggregate queries on data. The following aggregate functions are provided.

### APERCENTILE

```sql
APERCENTILE(expr, p [, algo_type])

algo_type: {
    "default"
  | "t-digest"
}
```

**Function Description**: Calculates the approximate percentile ranks of values in a specified column of a table/supertable, similar to the PERCENTILE function but returns an approximate result.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

**Description**:

- The range of p is [0,100], where 0 is equivalent to MIN and 100 is equivalent to MAX.
- algo_type can be "default" or "t-digest". When the input is "default", the function uses a histogram-based algorithm for calculation. When the input is "t-digest", it uses the t-digest algorithm to calculate the approximate percentile. If algo_type is not specified, the "default" algorithm is used.
- The approximate result of the "t-digest" algorithm is sensitive to the order of input data, and different input orders may result in slight discrepancies in supertable queries.

### AVG

```sql
AVG(expr)
```

**Function Description**: Calculates the average value of the specified field.

**Return Data Type**: DOUBLE, DECIMAL.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

**Description**: When the input type is DECIMAL, the output type is also DECIMAL. The precision and scale of the output conform to the rules described in the data type section. The result type is obtained by dividing the SUM type by UINT64. If the SUM result causes a DECIMAL type overflow, a DECIMAL OVERFLOW error is reported.

### COUNT

```sql
COUNT({* | expr})
```

**Function Description**: Counts the number of record rows for the specified field.

**Return Data Type**: BIGINT.

**Applicable Data Types**: All field types.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- An asterisk (*) can be used to replace a specific field, using an asterisk (*) returns the total number of records.
- If the counting field is a specific column, it returns the number of non-NULL value records in that column.

### ELAPSED

```sql
ELAPSED(ts_primary_key [, time_unit])
```

**Function Description**: The elapsed function expresses the continuous duration within the statistical period, and when used in conjunction with the twa function, it can calculate the area under the statistical curve. When specifying a window with the INTERVAL clause, it calculates the time range covered by data in each window within the given time range; if there is no INTERVAL clause, it returns the time range covered by data for the entire given time range. Note that ELAPSED does not return the absolute value of the time range, but the number of units obtained by dividing the absolute value by the time_unit. Stream computing only supports this function in FORCE_WINDOW_CLOSE mode.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: TIMESTAMP.

**Applicable to**: Tables, supertables, outer queries of nested queries

**Notes**:

- The ts_primary_key parameter can only be the first column of the table, i.e., the TIMESTAMP type primary key column.
- Returns according to the time unit specified by the time_unit parameter, with the minimum being the time resolution of the database. If the time_unit parameter is not specified, the time resolution of the database is used as the time unit. Supported time units time_unit include:
          1b (nanosecond), 1u (microsecond), 1a (millisecond), 1s (second), 1m (minute), 1h (hour), 1d (day), 1w (week).
- Can be used in combination with interval, returning the timestamp difference for each time window. It is important to note that, except for the first and last time windows, the timestamp differences for the middle windows are all the length of the window.
- order by asc/desc does not affect the calculation of the difference.
- For supertables, it needs to be used in combination with the group by tbname clause, and cannot be used directly.
- For regular tables, it is not supported in combination with the group by clause.
- For nested queries, it is only valid when the inner query outputs an implicit timestamp column. For example, the statement select elapsed(ts) from (select diff(value) from sub1), the diff function causes the inner query to output an implicit timestamp column, which is the primary key column and can be used as the first parameter of the elapsed function. Conversely, for example, the statement select elapsed(ts) from (select * from sub1), the ts column output to the outer layer no longer has the meaning of the primary key column and cannot use the elapsed function. Additionally, as a function strongly dependent on the timeline, forms like select elapsed(ts) from (select diff(value) from st group by tbname) although will return a calculation result, it has no practical significance, and such usage will also be restricted in the future.
- Not supported in combination with leastsquares, diff, derivative, top, bottom, last_row, interp, and other functions.

### LEASTSQUARES

```sql
LEASTSQUARES(expr, start_val, step_val)
```

**Function Description**: Calculates the linear equation of the values of a column in the table. start_val is the initial value of the independent variable, step_val is the step value of the independent variable.

**Return Data Type**: String expression (slope, intercept).

**Applicable Data Types**: expr must be a numeric type.

**Applicable to**: Tables.

### SPREAD

```sql
SPREAD(expr)
```

**Function Description**: Calculates the difference between the maximum and minimum values of a column in the table.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: INTEGER, TIMESTAMP.

**Applicable to**: Tables and supertables.

### STDDEV/STDDEV_POP

```sql
STDDEV/STDDEV_POP(expr)
```

**Function Description**: Calculates the population standard deviation of a column in the table.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

**Description**:

- Function `STDDEV_POP` equals `STDDEV` and is supported from ver-3.3.3.0.

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

**Version**: ver-3.3.3.0

**Return Data Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

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

**Return Data Type**: DOUBLE, BIGINT,DECIMAL.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

**Description**: When the input type is DECIMAL, the output type is DECIMAL(38, scale), where precision is the maximum value currently supported, and scale is the scale of the input type. If the SUM result overflows, a DECIMAL OVERFLOW error is reported.

### HYPERLOGLOG

```sql
HYPERLOGLOG(expr)
```

**Function Description**:

- Uses the hyperloglog algorithm to return the cardinality of a column. This algorithm significantly reduces memory usage with large data volumes, providing an estimated cardinality with a standard error of 0.81%.
- For smaller data volumes, this algorithm may not be very accurate. Alternatively, use `select count(data) from (select unique(col) as data from table)`.

**Return Result Type**: INTEGER.

**Applicable Data Types**: Any type.

**Applicable to**: Tables and supertables.

### HISTOGRAM

```sql
HISTOGRAM(expr, bin_type, bin_description, normalized)
```

**Function Description**: Statistics of data distribution according to user-specified intervals.

**Return Result Type**: If the normalized parameter is set to 1, the result type is DOUBLE, otherwise it is BIGINT.

**Applicable Data Types**: Numeric fields.

**Applicable to**: Tables and supertables.

**Detailed Description**:

- bin_type: User-specified bucket type, valid inputs are "user_input", "linear_bin", "log_bin".
- bin_description: Describes how to generate bucket intervals, for the three types of buckets, the descriptions are as follows (all in JSON format strings):
  - "user_input": "[1, 3, 5, 7]"
       User specifies the exact values for bins.

  - "linear_bin": "\{"start": 0.0, "width": 5.0, "count": 5, "infinity": true}"
       "start" indicates the starting point of data, "width" indicates the offset for each bin, "count" is the total number of bins, "infinity" indicates whether to add (-inf, inf) as the interval start and end points,
       generating intervals as [-inf, 0.0, 5.0, 10.0, 15.0, 20.0, +inf].

  - "log_bin": "\{"start":1.0, "factor": 2.0, "count": 5, "infinity": true}"
       "start" indicates the starting point of data, "factor" indicates the exponential growth factor, "count" is the total number of bins, "infinity" indicates whether to add (-inf, inf) as the interval start and end points,
       generating intervals as [-inf, 1.0, 2.0, 4.0, 8.0, 16.0, +inf].
- normalized: Whether to normalize the results to between 0 and 1. Valid inputs are 0 and 1.

### PERCENTILE

```sql
PERCENTILE(expr, p [, p1] ... )
```

**Function Description**: Calculates the percentile values for a column in a table.

**Return Data Type**: The function requires a minimum of 2 parameters and can accept up to 11 parameters. It can return up to 10 percentile values at once. When the number of parameters is 2, it returns one percentile as a DOUBLE. When the number of parameters is more than 2, it returns a VARCHAR type, formatted as a JSON array containing multiple return values.

**Applicable Fields**: Numeric types.

**Applicable to**: Tables.

**Usage Instructions**:

- *P* values range from 0≤*P*≤100, where P=0 is equivalent to MIN and P=100 is equivalent to MAX;
- When calculating multiple percentiles for the same column, it is recommended to use one PERCENTILE function with multiple parameters to significantly reduce the response time of the query.
  For example, using the query SELECT percentile(col, 90, 95, 99) FROM table performs better than SELECT percentile(col, 90), percentile(col, 95), percentile(col, 99) from table.

## Selection Functions

Selection functions choose one or more rows from the query result set based on semantics. Users can specify the output of the ts column or other columns (including tbname and tag columns), making it easy to know which data row the selected values originate from.

### BOTTOM

```sql
BOTTOM(expr, k)
```

**Function Description**: Calculates the smallest *k* non-NULL values of a column in a table/supertable. If multiple data entries have the same value and exceed the limit of k entries, the system randomly selects the required number of entries from those with the same value.

**Return Data Type**: Same as the applied field.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- *k* value range is 1≤*k*≤100;
- The system also returns the associated timestamp column;
- Limitation: BOTTOM function does not support the FILL clause.

### FIRST

```sql
FIRST(expr)
```

**Function Description**: Calculates the first non-NULL value written in a column of a table/supertable.

**Return Data Type**: Same as the applied field.

**Applicable Data Types**: All fields.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- To return the first non-NULL value of each column (smallest timestamp), use FIRST(\*); when querying a supertable, and if multiResultFunctionStarReturnTags is set to 0 (default), FIRST(\*) only returns the normal columns of the supertable; if set to 1, it returns both the normal and tag columns of the supertable.
- If all values in a column in the result set are NULL, the return for that column is also NULL;
- If all columns in the result set are NULL, no results are returned.
- For tables with composite primary keys, if there are multiple entries with the smallest timestamp, only the data with the smallest composite primary key is returned.

### LAST

```sql
LAST(expr)
```

**Function Description**: Returns the last non-NULL value written in a column of a table/supertable.

**Return Data Type**: Same as the applied field.

**Applicable Data Types**: All fields.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- To return the last (timestamp largest) non-NULL value of each column, you can use LAST(\*); when querying a supertable, and if multiResultFunctionStarReturnTags is set to 0 (default), LAST(\*) only returns the normal columns of the supertable; if set to 1, it returns both the normal and tag columns of the supertable.
- If all values in a column in the result set are NULL, the return result for that column is also NULL; if all columns in the result set are NULL, no result is returned.
- When used with supertables, if there are multiple rows with the same timestamp and it is the largest, one will be randomly returned, and it is not guaranteed that the same row will be selected in multiple runs.
- For tables with composite primary keys, if there are multiple records with the maximum timestamp, only the data with the largest corresponding composite primary key is returned.

### LAST_ROW

```sql
LAST_ROW(expr)
```

**Function Description**: Returns the last record of a table/supertable.

**Return Data Type**: Same as the applied field.

**Applicable Data Types**: All fields.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- To return the last record (timestamp largest) of each column, you can use LAST_ROW(\*); when querying a supertable, and if multiResultFunctionStarReturnTags is set to 0 (default), LAST_ROW(\*) only returns the normal columns of the supertable; if set to 1, it returns both the normal and tag columns of the supertable.
- When used with supertables, if there are multiple rows with the same timestamp and it is the largest, one will be randomly returned, and it is not guaranteed that the same row will be selected in multiple runs.
- Cannot be used with INTERVAL.
- Similar to the LAST function, for tables with composite primary keys, if there are multiple records with the maximum timestamp, only the data with the largest corresponding composite primary key is returned.

### MAX

```sql
MAX(expr)
```

**Function Description**: Calculates the maximum value of a column in a table/supertable.

**Return Data Type**: Same as the applied field.

**Applicable Data Types**: Numeric types, VARCHAR, NCHAR.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- The max function can accept strings as input parameters, and when the input parameter is a string type, it returns the largest string value(supported from ver-3.3.3.0, function `max` only accept numeric parameter before ver-3.3.3.0).

### MIN

```sql
MIN(expr)
```

**Function Description**: Calculates the minimum value of a column in a table/supertable.

**Return Data Type**: Same as the applied field.

**Applicable Data Types**: Numeric types, VARCHAR, NCHAR.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- The min function can accept strings as input parameters, and when the input parameter is a string type, it returns the largest string value(supported from ver-3.3.3.0, function `min` only accept numeric parameter before ver-3.3.3.0).

### MODE

```sql
MODE(expr)
```

**Function Description**: Returns the most frequently occurring value, if there are multiple values with the same highest frequency, it randomly outputs one of them.

**Return Data Type**: Consistent with the input data type.

**Applicable Data Types**: All field types.

**Applicable to**: Tables and supertables.

### SAMPLE

```sql
SAMPLE(expr, k)
```

**Function Description**: Gets k sample values of the data. The valid input range for parameter k is 1 ≤ k ≤ 1000.

**Return Result Type**: Same as the original data type.

**Applicable Data Types**: All field types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

### TAIL

```sql
TAIL(expr, k [, offset_rows])
```

**Function Description**: Returns the last k records after skipping the last offset_val records, not ignoring NULL values. offset_val can be omitted. In this case, it returns the last k records. When offset_val is provided, the function is equivalent to `order by ts desc LIMIT k OFFSET offset_val`.

**Parameter Range**: k: [1,100] offset_val: [0,100].

**Return Data Type**: Same as the applied field.

**Applicable Data Types**: Suitable for any type except the time primary key column.

**Applicable to**: Tables, supertables.

### TOP

```sql
TOP(expr, k)
```

**Function Description**: Calculates the top k largest non-NULL values of a column in a table/supertable. If multiple data entries have the same value and including all would exceed the limit of k, the system will randomly select the required number from those with the same value.

**Return Data Type**: Same as the field of the application.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- *k* value range is 1≤*k*≤100;
- The system also returns the timestamp column associated with the record;
- Limitation: TOP function does not support the FILL clause.

### UNIQUE

```sql
UNIQUE(expr)
```

**Function Description**: Returns the deduplicated values of the column. This function is similar to distinct. For the same data, it returns the one with the smallest timestamp. For queries on tables with composite primary keys, if there are multiple records with the smallest timestamp, only the data with the smallest composite primary key is returned.

**Return Data Type**: Same as the field of the application.

**Applicable Data Types**: All types of fields.

**Applicable to**: Tables and supertables.

### COLS

```sql
COLS (func(expr), output_expr1, [, output_expr2] ... )
```

**Function Description**: On the data row where the execution result of function func(expr) is located, execute the expression output_expr1, [, output_expr2], return its result, and the result of func (expr) is not output.

**Return Data Type**: Returns multiple columns of data, and the data type of each column is the type of the result returned by the corresponding expression.

**Applicable Data Types**: All type fields.

**Applicable to**: Tables and Super Tables.

**Usage Instructions**:

- Func function type: must be a single-line selection function (output result is a single-line selection function, for example, last is a single-line selection function, but top is a multi-line selection function).
- Mainly used to obtain the associated columns of multiple selection function results in a single SQL query. For example: select cols(max(c0), ts), cols(max(c1), ts) from ... can be used to get the different ts values of the maximum values of columns c0 and c1.
- The result of the parameter func is not returned. If you need to output the result of func, you can add additional output columns, such as: select first(ts), cols(first(ts), c1) from ..
- When there is only one column in the output, you can set an alias for the function. For example, you can do it like this: "select cols(first (ts), c1) as c11 from ...".
- Output one or more columns, and you can set an alias for each output column of the function. For example, you can do it like this: "select (first (ts), c1 as c11, c2 as c22) from ...".

## Time-Series Specific Functions

Time-Series specific functions are tailor-made by TDengine to meet the query scenarios of time-series data. In general databases, implementing similar functionalities usually requires complex query syntax and is inefficient. TDengine has built these functionalities into functions, greatly reducing the user's cost of use.

### CSUM

```sql
CSUM(expr)
```

**Function Description**: Cumulative sum, ignoring NULL values.

**Return Result Type**: If the input column is an integer type, the return value is long integer (int64_t), for floating-point numbers, the return value is double precision floating point (Double). For unsigned integer types, the return value is unsigned long integer (uint64_t).

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- Does not support +, -, *, / operations, such as csum(col1) + csum(col2).
- Can only be used with aggregation functions. This function can be applied to both basic tables and supertables.

### DERIVATIVE

```sql
DERIVATIVE(expr, time_interval, ignore_negative)

ignore_negative: {
    0
  | 1
}
```

**Function Description**: Calculates the rate of change per unit of a column in the table. The length of the unit time interval can be specified by the time_interval parameter, which can be as short as 1 second (1s); the value of the ignore_negative parameter can be 0 or 1, where 1 means to ignore negative values. For queries on tables with composite primary keys, if there are multiple records with the same timestamp, only the data with the smallest composite primary key is involved in the calculation.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- Can be used with the columns associated with the selection. For example: select _rowts, DERIVATIVE(col1, 1s, 1) from tb1.

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

**Function Description**: Calculates the difference between a specific column in the table and the current column's previous valid value. ignore_option can be 0|1|2|3, and can be omitted, defaulting to 0.

- `0` means do not ignore (diff result) negative values and do not ignore null values
- `1` means treat (diff result) negative values as null values
- `2` means do not ignore (diff result) negative values but ignore null values
- `3` means ignore (diff result) negative values and ignore null values
- For queries on tables with composite primary keys, if there are multiple records with the same timestamp, only the data with the smallest composite primary key is involved in the calculation.

**Return Data Type**: For bool, timestamp, and integer value types, returns int_64; for floating-point types, returns double; if the diff result overflows, it returns the overflowed value.

**Applicable Data Types**: Numeric types, timestamp, and bool types.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- diff calculates the difference between the specific column of the current row and the previous valid data of the same column, where the previous valid data refers to the nearest non-null value in the same column with a smaller timestamp.
- For numeric types, the diff result is the corresponding arithmetic difference; for timestamp types, the difference is calculated based on the timestamp precision of the database; for bool types, true is considered as 1, and false as 0
- If the current row data is null or no previous valid data is found in the same column, the diff result is null
- When ignoring negative values (ignore_option set to 1 or 3), if the diff result is negative, the result is set to null, then filtered according to the null value filtering rules
- When the diff result overflows, whether the result is `a negative value to be ignored` depends on whether the logical operation result is positive or negative, for example, the value of 9223372036854775800 - (-9223372036854775806) exceeds the range of BIGINT, the diff result will show the overflow value -10, but it will not be ignored as a negative value
- A single statement can use one or multiple diffs, and each diff can specify the same or different ignore_option; when there is more than one diff in a single statement, only when all diff results of a row are null and all ignore_options are set to ignore null values, the row is excluded from the result set
- Can be used with associated columns. For example: select _rowts, DIFF() from.
- When there is no composite primary key, if different subtables have data with the same timestamp, a "Duplicate timestamps not allowed" message will be displayed
- When using composite primary keys, the timestamp and composite primary key combinations of different subtables may be the same, which row is used depends on which one is found first, meaning that the results of running diff() multiple times in this situation may vary.

### INTERP

```sql
INTERP(expr [, ignore_null_values])

ignore_null_values: {
    0
  | 1
}
```

**Function Description**: Returns the record value or interpolated value of a specified column at a specified time slice. The ignore_null_values parameter can be 0 or 1, where 1 means to ignore NULL values, default is 0.

**Return Data Type**: Same as the field type.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

Usage Instructions:

- INTERP is used to obtain the record value of a specified column at the specified time slice. It has a dedicated syntax (interp_clause) when used. For syntax introduction, see [reference link](../query-data/#interp).
- When there is no row data that meets the conditions at the specified time slice, the INTERP function will interpolate according to the settings of the [FILL](../time-series-extensions/#fill-clause) parameter.
- When INTERP is applied to a supertable, it will sort all the subtable data under that supertable by primary key column and perform interpolation calculations, and can also be used with PARTITION BY tbname to force the results to a single timeline.
- When using INTERP with FILL PREV/NEXT/NEAR modes, its behavior differs from window queries. If data exists at the slice, no FILL operation will be performed, even if the current value is NULL.
- INTERP can be used with the pseudocolumn _irowts to return the timestamp corresponding to the interpolation point (supported from version 3.0.2.0).
- INTERP can be used with the pseudocolumn _isfilled to display whether the return result is from the original record or generated by the interpolation algorithm (supported from version 3.0.3.0).
- INTERP can only use the pseudocolumn `_irowts_origin` when using FILL PREV/NEXT/NEAR modes. `_irowts_origin` is supported from version 3.3.4.9.
- For queries on tables with composite primary keys, if there are data with the same timestamp, only the data with the smallest composite primary key participates in the calculation.

### IRATE

```sql
IRATE(expr)
```

**Function Description**: Calculates the instantaneous growth rate. It uses the last two sample data points in the time interval to calculate the instantaneous growth rate; if these two values are decreasing, then only the last value is used for the calculation, rather than the difference between the two. For queries on tables with composite primary keys, if there are multiple data points with the same timestamp, only the data corresponding to the smallest composite primary key is used in the calculation.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

### MAVG

```sql
MAVG(expr, k)
```

**Function Description**: Calculates the moving average of consecutive k values. If the number of input rows is less than k, no result is output. The valid input range for parameter k is 1 ≤ k ≤ 1000.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- Does not support +, -, *, / operations, such as mavg(col1, k1) + mavg(col2, k1);
- Can only be used with regular columns, selection, and projection functions, not with aggregation functions;

### STATECOUNT

```sql
STATECOUNT(expr, oper, val)
```

**Function Description**: Returns the number of consecutive records that meet a certain condition, with the result appended as a new column to each row. The condition is calculated based on the parameters, adding 1 if the condition is true, resetting to -1 if false, and skipping the data if it is NULL.

**Parameter Range**:

- oper: "LT" (less than), "GT" (greater than), "LE" (less than or equal to), "GE" (greater than or equal to), "NE" (not equal to), "EQ" (equal to), case insensitive.
- val: Numeric

**Return Result Type**: INTEGER.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Not applicable to subqueries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- Cannot be used with window operations, such as interval/state_window/session_window.

### STATEDURATION

```sql
STATEDURATION(expr, oper, val, unit)
```

**Function Description**: Returns the duration of time for consecutive records that meet a certain condition, with the result appended as a new column to each row. The condition is calculated based on the parameters, adding the time length between two records if the condition is true (the time length of the first record meeting the condition is counted as 0), resetting to -1 if false, and skipping the data if it is NULL.

**Parameter Range**:

- oper: `'LT'` (less than), `'GT'` (greater than), `'LE'` (less than or equal to), `'GE'` (greater than or equal to), `'NE'` (not equal to), `'EQ'` (equal to), case insensitive, but must be enclosed in `''`.
- val: Numeric
- unit: Time unit of the duration, possible values: 1b (nanoseconds), 1u (microseconds), 1a (milliseconds), 1s (seconds), 1m (minutes), 1h (hours), 1d (days), 1w (weeks). If omitted, defaults to the current database precision.

**Return Result Type**: INTEGER.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Not applicable to subqueries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- Cannot be used with window operations, such as interval/state_window/session_window.

### TWA

```sql
TWA(expr)
```

**Function Description**: Time-weighted average function. Calculates the time-weighted average of a column in a table over a period of time. For queries on tables with composite primary keys, if there are multiple data points with the same timestamp, only the data corresponding to the smallest composite primary key is used in the calculation. Stream computing supports this function only in FORCE_WINDOW_CLOSE mode.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

## System Information Functions

### DATABASE

```sql
SELECT DATABASE();
```

**Description**: Returns the currently logged-in database. If no default database was specified at login and the USE command has not been used to switch databases, it returns NULL.

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

**Description**: Checks if all dnodes on the server are online; if so, it returns success, otherwise, it returns an error that the connection could not be established. To check the status of the cluster, it is recommended to use `SHOW CLUSTER ALIVE;`, which, unlike `SELECT SERVER_STATUS();`, does not return an error when some nodes in the cluster are unavailable, but instead returns different status codes, see: [SHOW CLUSTER ALIVE](../show-commands/#show-cluster-alive)

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

**Return Type**: GEOMETRY

**Applicable Data Types**: VARCHAR

**Applicable Table Types**: Basic tables and supertables

**Usage Instructions**: The input can be one of the WKT strings, such as POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRYCOLLECTION. The output is the GEOMETRY data type defined in binary string form.

### Geometry Output Functions

#### ST_AsText

```sql
ST_AsText(GEOMETRY geom)
```

**Function Description**: Returns the specified Well-Known Text (WKT) representation from geometry data.

**Return Type**: VARCHAR

**Applicable Data Types**: GEOMETRY

**Applicable Table Types**: Basic tables and supertables

**Usage Instructions**: The output can be one of the WKT strings, such as POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRYCOLLECTION.

### Geometry Relationship Functions

#### ST_Intersects

```sql
ST_Intersects(GEOMETRY geomA, GEOMETRY geomB)
```

**Function Description**: Compares two geometry objects and returns true if they intersect.

**Return Type**: BOOL

**Applicable Data Types**: GEOMETRY, GEOMETRY

**Applicable Table Types**: Basic tables and supertables

**Usage Instructions**: If the two geometry objects share any point, they intersect.

#### ST_Equals

```sql
ST_Equals(GEOMETRY geomA, GEOMETRY geomB)
```

**Function Description**: Returns TRUE if the given geometry objects are "spatially equal".

**Return Type**: BOOL

**Applicable Data Types**: GEOMETRY, GEOMETRY

**Applicable Table Types**: Basic tables and supertables

**Usage Instructions**: "Spatially equal" means that ST_Contains(A,B) = true and ST_Contains(B,A) = true, and the order of points may differ but represent the same geometric structure.

#### ST_Touches

```sql
ST_Touches(GEOMETRY geomA, GEOMETRY geomB)
```

**Function Description**: Returns TRUE if A and B intersect, but their interiors do not intersect.

**Return Type**: BOOL

**Applicable Data Types**: GEOMETRY, GEOMETRY

**Applicable Table Types**: Basic tables and supertables

**Usage Instructions**: A and B have at least one common point, and these common points are located on at least one boundary. For point/point input, the relationship is always FALSE, because points have no boundaries.

#### ST_Covers

```sql
ST_Covers(GEOMETRY geomA, GEOMETRY geomB)
```

**Function Description**: Returns TRUE if every point in B is inside the geometric shape A (intersecting with the interior or boundary).

**Return Type**: BOOL

**Applicable Data Types**: GEOMETRY, GEOMETRY

**Applicable Table Types**: Basic tables and supertables

**Usage Instructions**: A contains B means that no points in B are outside of A (on the outside).

#### ST_Contains

```sql
ST_Contains(GEOMETRY geomA, GEOMETRY geomB)
```

**Function Description**: Returns TRUE if geometric shape A contains geometric shape B.

**Return Type**: BOOL

**Applicable Data Types**: GEOMETRY, GEOMETRY

**Applicable Table Types**: Basic tables and supertables

**Usage Instructions**: A contains B if and only if all points of B are inside A (i.e., located inside or on the boundary) (or equivalently, no points of B are outside A), and the interiors of A and B have at least one point in common.

#### ST_ContainsProperly

```sql
ST_ContainsProperly(GEOMETRY geomA, GEOMETRY geomB)
```

**Function Description**: Returns TRUE if every point of B is inside A.

**Return Type**: BOOL

**Applicable Data Types**: GEOMETRY, GEOMETRY

**Applicable Table Types**: Basic tables and supertables

**Usage Instructions**: No points of B are on the boundary or outside of A.
