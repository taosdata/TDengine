---
title: Functions
sidebar_label: Functions
description: This document describes the standard SQL functions available in TDengine.
toc_max_heading_level: 4
---

## Scalar Functions

Scalar functions return one result for each row.

### Mathematical Functions

#### ABS

```sql
ABS(expr)
```

**Description**: The absolute value of a specific field.

**Return value type**: Same as the field being used

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

#### ACOS

```sql
ACOS(expr)
```

**Description**: The arc cosine of a specific field.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

#### ASIN

```sql
ASIN(expr)
```

**Description**: The arc sine of a specific field.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.


#### ATAN

```sql
ATAN(expr)
```

**Description**: The arc tangent of a specific field.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.


#### CEIL

```sql
CEIL(expr)
```

**Description**: The rounded up value of a specific field

**Return value type**: Same as the field being used

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

#### COS

```sql
COS(expr)
```

**Description**: The cosine of a specific field.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

#### FLOOR

```sql
FLOOR(expr)
```

**Description**: The rounded down value of a specific field
 **More explanations**: The restrictions are same as those of the `CEIL` function.

#### LOG

```sql
LOG(expr [, base])
```

**Description**: The logarithm of a specific field with `base` as the radix. If you do not enter a base, the natural logarithm of the field is returned.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.


#### POW

```sql
POW(expr, power)
```

**Description**: The power of a specific field with `power` as the exponent.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.


#### ROUND

```sql
ROUND(expr[, digits])
```

**Description**: The rounded value of a specific field.

**Return value type**: Same as the `expr` field being used.

**Applicable data types**: 
- `expr`: Numeric.
- `digits`: Numeric.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: 
- if `expr` or `digits` is NULL, return NULL。
- Rounds the `expr` to `digits` decimal places, `digits` defaults to 0 if not specified.
- If the input value is of INTEGER type, it will always return an INTEGER, regardless of the value of `digits`, and no decimal places will be retained.
- A `digits` value greater than zero indicates that the function will round the result to `digits` decimal places. If the number of decimal places is less than `digits`, no rounding is performed, and the result is returned directly.
- A `digits` value less than zero indicates that the function will drop the decimal places and round the number to the left of the decimal point by `digits` places. If the number of digits to the left of the decimal point is less than `digits`, the result will be 0.
- Since DECIMAL type is not supported at the moment, this function will return results in DOUBLE or FLOAT for numbers with decimal places. However, DOUBLE and FLOAT have precision limits, so when the number of digits is too large, using this function may not yield meaningful results.
- This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

**examples**: 
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

**Description**: The sine of a specific field.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

#### SQRT

```sql
SQRT(expr)
```

**Description**: The square root of a specific field.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

#### TAN

```sql
TAN(expr)
```

**Description**: The tangent of a specific field.

**Return value type**: Double

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

#### PI
```sql
PI()
```

**Description**: Returns the value of π.

**Return value type**: DOUBLE.

**Applicable data types**: None.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: 
- π ≈ 3.141592653589793.
- This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

**Examples**: 
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

**Description**: Returns the number `expr`, truncated to `digits` decimal places.

**Return value type**: Same as the `expr` field being used.

**Applicable data types**: 
- `expr`: Numeric.
- `digits`: Numeric.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: 
- If `expr` or `digits` is NULL, the function returns NULL.
- Truncation is performed directly based on the specified number of digits, without rounding.
- A `digits` value greater than zero indicates that the function will truncate the result to `digits` decimal places. If the number of decimal places is less than `digits`, no truncation is performed, and the result is returned as is.
- A `digits` value equal to zero indicates that the decimal places will be dropped.
- A `digits` value less than zero indicates that the decimal places will be dropped, and the digits to the left of the decimal point, up to `digits` positions, will be replaced with `0`. If the number of digits to the left of the decimal point is less than `digits`, the result will be 0.
- Since DECIMAL type is not supported at the moment, this function will use DOUBLE and FLOAT to represent results with decimal places. However, DOUBLE and FLOAT have precision limits, so using this function with a large number of digits may not yield meaningful results.
- This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

**Examples**: 
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
**Description**: Returns the value of e (the base of natural logarithms) raised to the power of `expr`.

**Return value type**: DOUBLE.

**Applicable data types**: Numeric.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: 
- if `expr` is NULL, the function returns NULL.
- This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

**Examples**: 
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

**Description**: Returns the natural logarithm of `expr`.

**Return value type**: DOUBLE.

**Applicable data types**: Numeric.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: 
- if `expr` is NULL, the function returns NULL.
- if `expr` less than or equal to 0, the function returns NULL.
- This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

**Examples**: 
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

**Description**: Modulo operation. Returns the remainder of `epxr1` divided by `expr2`.

**Return value type**: DOUBLE.

**Applicable data types**: Numeric.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: 
- if `expr2` is 0, the function returns NULL.
- if `expr` or `expr2` is NULL, the function returns NULL.
- This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

**Examples**: 
``` sql
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

**Description**: Returns a random floating-point value `v` in the range `0 <= v < 1.0`.

**Return value type**: DOUBLE.

**Applicable data types**: 
- `seed`: INTEGER.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: 
- If an integer argument `seed` is specified, it is used as the seed value. With a seed, `RAND(seed)` returns the same value each time
- This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

**Examples**: 
``` sql
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

**Description**: Returns the sign of the argument.

**Return value type**: Same as the `expr` field being used.

**Applicable data types**: Numeric.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: 
- if `expr` is negative, returns -1.
- if `expr` is positive, returns 1.
- if `expr` is 0, returns 0.
- if `expr` is NULL, returns NULL.
- This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

**Examples**: 
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

**Description**: Returns the argument `expr`, converted from radians to degrees.

**Return value type**: DOUBLE.

**Applicable data types**: Numeric.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: 
- if `expr` is NULL, the function returns NULL.
- degree = radian * 180 / π。
- This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

**Examples**: 
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

**Description**: Returns the argument `expr`, converted from degrees to radians.

**Return value type**: DOUBLE.

**Applicable data types**: Numeric.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Usage**: 
- if `expr` is NULL, the function returns NULL.
- radian = degree * π / 180.
- This function can only be used on data columns. It can be used with selection and projection functions but not with aggregation functions.

**Examples**: 
```sql
taos> select radians(180);
       radians(180)        |
============================
         3.141592653589793 |
```

### Concatenation Functions

Concatenation functions take strings as input and produce string or numeric values as output.

#### CHAR_LENGTH

```sql
CHAR_LENGTH(expr)
```

**Description**: The length in number of characters of a string

**Return value type**: Bigint

**Applicable data types**: VARCHAR and NCHAR

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**:
- Unlike the LENGTH() function, the CHAR_LENGTH() function treats multi-byte characters, such as Chinese characters, as a single character with a length of 1, whereas LENGTH() calculates the byte count, resulting in a length of 3. For example, CHAR_LENGTH('你好') = 2 and LENGTH('你好') = 6.
- If expr is NULL, the function returns NULL.

**Examples**: 
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

**Description**: The concatenation result of two or more strings

**Return value type**: If the concatenated strings are VARCHARs, the result is a VARCHAR. If the concatenated strings are NCHARs, the result is an NCHAR. If an input value is null, the result is null.

**Applicable data types**: VARCHAR and NCHAR You can concatenate between 2 and 8 strings.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables


#### CONCAT_WS

```sql
CONCAT_WS(separator_expr, expr1, expr2 [, expr] ...)
```

**Description**: The concatenation result of two or more strings with separator

**Return value type**: If the concatenated strings are VARCHARs, the result is a VARCHAR. If the concatenated strings are NCHARs, the result is an NCHAR. If an input value is null, the result is null.

**Applicable data types**: VARCHAR and NCHAR You can concatenate between 3 and 9 strings.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables


#### LENGTH

```sql
LENGTH(expr)
```

**Description**: The length in bytes

**Return value type**: Bigint

**Applicable data types**: VARCHAR and NCHAR and VARBINARY 

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables


#### LOWER

```sql
LOWER(expr)
```

**Description**: Convert the input string to lower case

**Return value type**: Same as input

**Applicable data types**: VARCHAR and NCHAR

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables


#### LTRIM

```sql
LTRIM(expr)
```

**Description**: Remove the left leading blanks of a string

**Return value type**: Same as input

**Applicable data types**: VARCHAR and NCHAR

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables


#### RTRIM

```sql
LTRIM(expr)
```

**Description**: Remove the right tailing blanks of a string

**Return value type**: Same as input

**Applicable data types**: VARCHAR and NCHAR

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

#### TRIM
```sql
TRIM([{LEADING | TRAILING | BOTH} [remstr] FROM] expr)
TRIM([remstr FROM] expr)
```

**Description**: Returns the string `expr` with all `remstr` prefixes or suffixes removed.

**Return value type**: Same as the `expr` field being used.

**Applicable data types**: 
- remstr: VARCHAR,NCHAR.
- epxr: VARCHAR,NCHAR.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**: 
- The first optional variable [LEADING | BOTH | TRAILING] specifies which side of the string to trim:
    - LEADING will remove the specified characters from the beginning of the string.
    - TRAILING will remove the specified characters from the end of the string.
    - BOTH (the default) will remove the specified characters from both the beginning and the end of the string.
- The second optional variable [remstr] specifies the string to be trimmed:
    - If `remstr` is not specified, spaces will be removed by default.
    - `remstr` can contain multiple characters. For example, `trim('ab' from 'abacd')` will treat 'ab' as a whole and trim it, resulting in 'acd'.
- If `expr` is NULL, the function returns NULL.
- This function is multibyte-safe.

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
**Description**: The forms without a `len` argument return a substring from string `expr` starting at position `pos`. The forms with a `len` argument return a substring `len` characters long from string `expr`, starting at position `pos`.

**Return value type**: Same as the `expr` field being used.

**Applicable data types**: 
- `expr`: VARCHAR,NCHAR.
- `pos`: INTEGER.
- `len`: INTEGER.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**: 
- If `pos` is positive, the result is the substring of `expr` starting at the `pos` position, counting from left to right.
- If `pos` is negative, the result is the substring of `expr` starting at the `pos` position, counting from right to left.
- If any argument is NULL, the function returns NULL.
- This function is multibyte-safe.
- If `len` is less than 1, the function returns an empty string.
- `pos` is 1-based, and if `pos` is 0, the function returns an empty string.
- If `pos` + `len` exceeds the length of `expr`, the result is the substring starting from `pos` to the end of the string, equivalent to executing `substring(expr, pos)`.

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

**Description**: Returns the substring from string `expr` before `count` occurrences of the delimiter `delim`.

**Return value type**: Same as the `expr` field being used.

**Applicable data types**: 
- `expr`: VARCHAR,NCHAR.
- `delim`: VARCHAR, NCHAR.
- `count`: INTEGER.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**: 
- If `count` is positive, everything to the left of the final delimiter (counting from the left) is returned.
- If `count` is negative, everything to the right of the final delimiter (counting from the right) is returned
- If any argument is NULL, the function returns NULL.
- This function is multibyte-safe.

**Examples**: 
```sql
taos> select substring_index('www.taosdata.com','.',2);
 substring_index('www.taosdata.com','.',2) |
============================================
 www.taosdata                              |

taos> select substring_index('www.taosdata.com','.',-2);
 substring_index('www.taosdata.com','.',-2) |
=============================================
 taosdata.com                               |
```

#### UPPER

```sql
UPPER(expr)
```

**Description**: Convert the input string to upper case

**Return value type**: Same as input

**Applicable data types**: VARCHAR and NCHAR

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: table, STable

#### CHAR
```sql
CHAR(expr1 [, expr2] [, epxr3] ...)
```

**Description**: Interprets each argument `expr` as an integer and returns a string consisting of the characters given by the code values of those integers.

**Return value type**: VARCHAR.

**Applicable data types**: INTEGER,VARCHAR,NCHAR.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**: 
- Arguments larger than 255 are converted into multiple result bytes. For example, CHAR(256) is equivalent to CHAR(1,0), and CHAR(256*256) is equivalent to CHAR(1,0,0):
- NULL values are skipped.
- If the input parameter is of string type, it will be converted to a numeric type for processing.
- If the input parameter corresponds to non-printable characters, the return value will still include those characters, but they may not be visible.

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

**Description**: Returns the numeric value of the leftmost character of the string `expr`.

**Return value type**: BIGINT.

**Applicable data types**: VARCHAR, NCHAR.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**: 
- If `expr` is NULL, the function returns NULL.
- If `expr` is an empty string, the function returns 0.
- If the first character of expr is a multibyte character, only the ASCII code corresponding to the first byte of that character will be returned.

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

**Description**: Returns the position of the first occurrence of substring `expr1` in string `expr2`

**Return value type**: BIGINT.

**Applicable data types**: 
- `expr1`: VARCHAR, NCHAR.
- `expr2`: VARCHAR, NCHAR.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**: 
- If `expr1` or `expr2` is NULL, the function returns NULL.
- If `expr2` is not found in `expr1`, the function returns 0.
- If `expr2` is an empty string, it is considered to always match successfully in `expr1`, and the function returns 1.
- The returned position is 1-based.
- This function is multibyte-safe.

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
**Description**: Returns the string `expr` with all occurrences of the string `from_str` replaced by the string `to_str`.

**Return value type**: Same as the `expr` field being used.

**Applicable data types**: 
- `expr`: VARCHAR, NCHAR.
- `from_str`: VARCHAR, NCHAR.
- `to_str`: VARCHAR, NCHAR.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**: 
- This function is case-sensitive.
- If any argument is NULL, the function returns NULL.
- This function is multibyte-safe.

**Examples**: 
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
**Description**: Returns a string consisting of the string `expr` repeated `count` times.

**Return value type**: Same as the `expr` field being used.

**Applicable data types**: 
- `expr`:  VARCHAR,NCHAR.
- `count`: INTEGER.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**: 
- If `count` is less than 1, returns an empty string.
- if `expr` or `count` is NULL, the function returns NULL.

**Examples**: 
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

Conversion functions change the data type of a value.

#### CAST

```sql
CAST(expr AS type_name)
```

**Description**: Convert the input data `expr` into the type specified by `type_name`.

**Return value type**: The type specified by parameter `type_name`

**Applicable data types**: All data types except JSON and VARBINARY. If type_name is VARBINARY, expr can only be VARCHAR.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**:

- Error will be reported for unsupported type casting
- Some values of some supported data types may not be casted, below are known issues:
        1. Some strings cannot be converted to numeric values. For example, the string `a` may be converted to `0`. However, this does not produce an error.
        2. If a converted numeric value is larger than the maximum size for the specified type, an overflow will occur. However, this does not produce an error.
        3. If a converted string value is larger than the maximum size for the specified type, the output value will be truncated. However, this does not produce an error.

#### TO_ISO8601

```sql
TO_ISO8601(expr [, timezone])
```

**Description**: The ISO8601 date/time format converted from a UNIX timestamp, plus the timezone. You can specify any time zone with the timezone parameter. If you do not enter this parameter, the time zone on the client is used.

**Return value type**: VARCHAR

**Applicable data types**: Integers and timestamps

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**:

- You can specify a time zone in the following format: [z/Z, +/-hhmm, +/-hh, +/-hh:mm]. For example, TO_ISO8601(1, "+00:00").
- The precision of the input timestamp will be recognized automatically according to the precision of the table used, milliseconds will be used if no table is specified.


#### TO_JSON

```sql
TO_JSON(str_literal)
```

**Description**: Converts a string into JSON.

**Return value type**: JSON

**Applicable data types**: JSON strings in the form `{"literal": literal}`. `{}` indicates a null value. The key must be a string literal. The value can be a numeric literal, string literal, Boolean literal, or null literal. str_literal cannot include escape characters.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: table, STable


#### TO_UNIXTIMESTAMP

```sql
TO_UNIXTIMESTAMP(expr [, return_timestamp])

return_timestamp: {
    0
  | 1
}
```

**Description**: UNIX timestamp converted from a string of date/time format

**Return value type**: BIGINT, TIMESTAMP

**Applicable column types**: VARCHAR and NCHAR

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**:

- The input string must be compatible with ISO8601/RFC3339 standard, NULL will be returned if the string can't be converted
- The precision of the returned timestamp is same as the precision set for the current data base in use
- return_timestamp indicates whether the returned value type is TIMESTAMP or not. If this parameter set to 1, function will return TIMESTAMP type. Otherwise function will return BIGINT type. If parameter is omitted, default return value type is BIGINT.

#### TO_CHAR

```sql
TO_CHAR(ts, format_str_literal)
```

**Description**: Convert a ts column to string as the format specified

**Version**: Since ver-3.2.2.0

**Return value type**: VARCHAR

**Applicable column types**: TIMESTAMP

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Supported Formats**

| **Format**          | **Comment**                                    | **example**               |
| ------------------- | ---------------------------------------------- | ------------------------- |
| AM,am,PM,pm         | Meridiem indicator(without periods)            | 07:00:00am                |
| A.M.,a.m.,P.M.,p.m. | Meridiem indicator(with periods)               | 07:00:00a.m.              |
| YYYY,yyyy           | year, 4 or more digits                         | 2023-10-10                |
| YYY,yyy             | year, last 3 digits                            | 023-10-10                 |
| YY,yy               | year, last 2 digits                            | 23-10-10                  |
| Y,y                 | year, last digit                               | 3-10-10                   |
| MONTH               | full uppercase of month                        | 2023-JANUARY-01           |
| Month               | full capitalized month                         | 2023-January-01           |
| month               | full lowercase of month                        | 2023-january-01           |
| MON                 | abbreviated uppercase of month(3 char)         | JAN, SEP                  |
| Mon                 | abbreviated capitalized month                  | Jan, Sep                  |
| mon                 | abbreviated lowercase of month                 | jan, sep                  |
| MM,mm               | month number 01-12                             | 2023-01-01                |
| DD,dd               | month day, 01-31                               |                           |
| DAY                 | full uppercase of week day                     | MONDAY                    |
| Day                 | full capitalized week day                      | Monday                    |
| day                 | full lowercase of week day                     | monday                    |
| DY                  | abbreviated uppercase of week day              | MON                       |
| Dy                  | abbreviated capitalized week day               | Mon                       |
| dy                  | abbreviated lowercase of week day              | mon                       |
| DDD                 | year day, 001-366                              |                           |
| D,d                 | week day number, 1-7, Sunday(1) to Saturday(7) |                           |
| HH24,hh24           | hour of day, 00-23                             | 2023-01-30 23:59:59       |
| hh12,HH12, hh, HH   | hour of day, 01-12                             | 2023-01-30 12:59:59PM     |
| MI,mi               | minute, 00-59                                  |                           |
| SS,ss               | second, 00-59                                  |                           |
| MS,ms               | milli second, 000-999                          |                           |
| US,us               | micro second, 000000-999999                    |                           |
| NS,ns               | nano second, 000000000-999999999               |                           |
| TZH,tzh             | time zone hour                                 | 2023-01-30 11:59:59PM +08 |

**More explanations**:
- The output format of `Month`, `Day` are left aligined, like`2023-OCTOBER  -01`, `2023-SEPTEMBER-01`, `September` is the longest, no paddings. Week days are slimilar.
- When `ms`,`us`,`ns` are used in `to_char`, like `to_char(ts, 'yyyy-mm-dd hh:mi:ss.ms.us.ns')`, The time of `ms`,`us`,`ns` corresponds to the same fraction seconds. When ts is `1697182085123`, the output of `ms` is `123`, `us` is `123000`, `ns` is `123000000`.
- If we want to output some characters of format without converting, surround it with double quotes. `to_char(ts, 'yyyy-mm-dd "is formated by yyyy-mm-dd"')`. If want to output double quotes, add a back slash before double quote, like `to_char(ts, '\"yyyy-mm-dd\"')` will output `"2023-10-10"`.
- For formats that output digits, the uppercase and lowercase formats are the same.
- It's recommended to put time zone in the format, if not, the default time zone will be that in server or client.
- The precision of the input timestamp will be recognized automatically according to the precision of the table used, milliseconds will be used if no table is specified.

#### TO_TIMESTAMP

```sql
TO_TIMESTAMP(ts_str_literal, format_str_literal)
```

**Description**: Convert a formated timestamp string to a timestamp

**Version**: Since ver-3.2.2.0

**Return value type**: TIMESTAMP

**Applicable column types**: VARCHAR

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**Supported Formats**: The same as `TO_CHAR`.

**More explanations**:
- When `ms`, `us`, `ns` are used in `to_timestamp`, if multi of them are specified, the results are accumulated. For example, `to_timestamp('2023-10-10 10:10:10.123.000456.000000789', 'yyyy-mm-dd hh:mi:ss.ms.us.ns')` will output the timestamp of `2023-10-10 10:10:10.123456789`.
- The uppercase or lowercase of `MONTH`, `MON`, `DAY`, `DY` and formtas that output digits have same effect when used in `to_timestamp`, like `to_timestamp('2023-JANUARY-01', 'YYYY-month-dd')`, `month` can be replaced by `MONTH`, or `month`. The cases are ignored.
- If multi times are specified for one component, the previous will be overwritten. Like `to_timestamp('2023-22-10-10', 'yyyy-yy-MM-dd')`, the output year will be `2022`.
- To avoid unexpected time zone used during the convertion, it's recommended to put time zone in the ts string, e.g. '2023-10-10 10:10:10+08'. If time zone not specified, default will be that in server or client.
- The default timestamp if some components are not specified will be: `1970-01-01 00:00:00` with the timezone specified or default to local timezone. Only `DDD` is specified without `DD` is not supported currently, e.g. format 'yyyy-mm-ddd' is not supported, but 'yyyy-mm-dd' is supported.
- If `AM` or `PM` is specified in formats, the Hour must between `1-12`.
- In some cases, `to_timestamp` can convert correctly even the format and the timestamp string are not totally matched. Like `to_timetamp('200101/2', 'yyyyMM1/dd')`, the digit `1` in format string are ignored, and the output timestsamp is `2001-01-02 00:00:00`. Spaces and tabs in formats and tiemstamp string are also ignored automatically.
- The precision of the output timestamp will be the same as the table in SELECT stmt, millisecond will be used if no table is specified. The output of `select to_timestamp('2023-08-1 10:10:10.123456789', 'yyyy-mm-dd hh:mi:ss.ns')` will be truncated to millisecond precision. If a nano precision table is specified, no truncation will be applied. Like `select to_timestamp('2023-08-1 10:10:10.123456789', 'yyyy-mm-dd hh:mi:ss.ns') from db_ns.table_ns limit 1`.


### Time and Date Functions

These functions perform operations on times and dates.

All functions that return the current time, such as `NOW`, `TODAY`, and `TIMEZONE`, are calculated only once per statement even if they appear multiple times.

#### NOW

```sql
NOW()
```

**Description**: The current time of the client side system

**Return value type**: TIMESTAMP

**Applicable column types**: TIMESTAMP only

**Applicable table types**: standard tables and supertables

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**More explanations**:

- Add and Subtract operation can be performed, for example NOW() + 1s, the time unit can be:
        b(nanosecond), u(microsecond), a(millisecond)), s(second), m(minute), h(hour), d(day), w(week)
- The precision of the returned timestamp is same as the precision set for the current data base in use


#### TIMEDIFF

```sql
TIMEDIFF(expr1, expr2 [, time_unit])
```

**Description**: Returns `expr1 − expr2`, and rounded to the time unit specified by `time_unit`

**Return value type**: BIGINT

**Applicable column types**: 
- `expr1`: UNIX-style timestamps in BIGINT and TIMESTAMP format and other timestamps in VARCHAR and NCHAR format.
- `expr2`: UNIX-style timestamps in BIGINT and TIMESTAMP format and other timestamps in VARCHAR and NCHAR format
- `time_unit`: See "More explanations".

**Applicable table types**: standard tables and supertables

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**More explanations**:
- Time unit specified by `time_unit` can be:
          1b (nanoseconds), 1u (microseconds), 1a (milliseconds), 1s (seconds), 1m (minutes), 1h (hours), 1d (days), or 1w (weeks)
- If the time unit `time_unit` is not specified, the precision of the returned time difference will be consistent with the current DATABASE time precision setting.
- If the input contains a string that does not conform to the date-time format, the function returns NULL.
- If `expr1` or `expr2` is NULL, the function returns NULL.
- If `time_unit` is NULL, it is equivalent to not specifying a time unit.
- The precision of the input timestamp is determined by the table being queried. If no table is specified, the precision defaults to milliseconds.

**Examples**:
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

**Description**: Truncate the input timestamp with unit specified by `time_unit`

**Return value type**: TIMESTAMP

**Applicable column types**: UNIX-style timestamps in BIGINT and TIMESTAMP format and other timestamps in VARCHAR and NCHAR format

**Applicable table types**: standard tables and supertables

**More explanations**:
- Time unit specified by `time_unit` can be:
          1b (nanoseconds), 1u (microseconds), 1a (milliseconds), 1s (seconds), 1m (minutes), 1h (hours), 1d (days), or 1w (weeks)
- The precision of the returned timestamp is same as the precision set for the current data base in use
- The precision of the input timestamp will be recognized automatically according to the precision of the table used, milliseconds will be used if no table is specified.
- If the input data is not formatted as a timestamp, the returned value is null.
- When using 1d/1w as the time unit to truncate timestamp, you can specify whether to truncate based on the current time zone by setting the use_current_timezone parameter.
  Value 0 indicates truncation using the UTC time zone, value 1 indicates truncation using the current time zone.
  For example, if the time zone configured by the Client is UTC + 0800, TIMETRUNCATE ('2020-01-01 23:00:00', 1d, 0) returns the result of '2020-01-01 08:00:00'.
  When using TIMETRUNCATE ('2020-01-01 23:00:00', 1d, 1), the result is 2020-01-01 00:00:00 '.
  When use_current_timezone is not specified, use_current_timezone defaults to 1.
- When truncating a time value to the week (1w), weeks are determined using the Unix epoch (1970-01-01T00:00:00Z UTC). The Unix epoch was on a Thursday, so all calculated weeks begin on Thursday.

#### TIMEZONE

```sql
TIMEZONE()
```

**Description**: The timezone of the client side system

**Applicable data types**: VARCHAR

**Applicable column types**: None

**Applicable table types**: standard tables and supertables


#### TODAY

```sql
TODAY()
```

**Description**: The timestamp of 00:00:00 of the client side system

**Return value type**: TIMESTAMP

**Applicable column types**: TIMESTAMP only

**Applicable table types**: standard tables and supertables

**More explanations**:

- Add and Subtract operation can be performed, for example TODAY() + 1s, the time unit can be:
                b(nanosecond), u(microsecond), a(millisecond)), s(second), m(minute), h(hour), d(day), w(week)
- The precision of the returned timestamp is same as the precision set for the current data base in use

#### WEEK
```sql
WEEK(expr [, mode])
```
**Description**: This function returns the week number for `expr`.

**Return value type**: BIGINT.

**Applicable column types**: 
- `expr`: UNIX-style timestamps in BIGINT and TIMESTAMP format and other timestamps in VARCHAR and NCHAR format.
- `mode`: INTEGERS in the range of 0-7.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**: 
- If `expr` is NULL, the function returns NULL.
- The precision of the input timestamp is determined by the table being queried. If no table is specified, the precision defaults to milliseconds.
- The following table describes how the `mode` argument works.

|Mode	| First day of week	 | Range	 | Week 1 is the first week …    |
|-------|------------------|-------|-------------------------------|
|0	| Sunday	          | 0-53	 | with a Sunday in this year    |
|1	| Monday	          | 0-53	 | with 4 or more days this year |
|2	| Sunday	          | 1-53	 | with a Sunday in this year    |
|3	| Monday	          | 1-53	 | with 4 or more days this year |
|4	| Sunday	          | 0-53	 | with 4 or more days this year |
|5	| Monday	          | 0-53	 | with a Monday in this year    |
|6	| Sunday	          | 1-53	 | with 4 or more days this year |
|7	| Monday	          | 1-53	 | with a Monday in this year    |

- When the return value range is 0 - 53, dates before the 1st week are considered as week 0.
- When the return value range is 1 - 53, dates before the 1st week are considered as the last week of the previous year.
- Taking `2000-01-01` as an example:
    - In `mode=0`, the return value is `0` because the first Sunday of that year is `2000-01-02`, and week 1 starts from `2000-01-02`. Thus, `2000-01-01` falls in week 0, and the function returns 0.
    - In `mode=1`, the return value is also `0` because the week containing `2000-01-01` only has two days, `2000-01-01 (Saturday)` and `2000-01-02 (Sunday)`. Week 1 starts from `2000-01-03`, so `2000-01-01` is part of week 0, and the function returns 0.
    - In `mode=2`, the return value is `52` because week 1 starts from `2000-01-02`, and the return value range is 1-53. Therefore, `2000-01-01` is considered part of the last week of the previous year, which is week 52 of 1999, and the function returns 52.

**Examples**: 
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
**Description**: Returns the calendar week of the `expr` as a number.

**Return value type**: BIGINT.

**Applicable column types**: UNIX-style timestamps in BIGINT and TIMESTAMP format and other timestamps in VARCHAR and NCHAR format.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**: 
- WEEKOFYEAR() is a compatibility function that is equivalent to WEEK(expr,3).
- If `expr` is NULL, the function returns NULL.
- The precision of the input timestamp is determined by the table being queried. If no table is specified, the precision defaults to milliseconds.

**Examples**: 
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
**Description**: Returns the weekday index for `expr`.

**Return value type**: BIGINT.

**Applicable column types**: UNIX-style timestamps in BIGINT and TIMESTAMP format and other timestamps in VARCHAR and NCHAR format.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**: 
- Return value 0 = Monday, 1 = Tuesday, … 6 = Sunday.
- If `expr` is NULL, the function returns NULL.
- The precision of the input timestamp is determined by the table being queried. If no table is specified, the precision defaults to milliseconds.

**Examples**: 
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
**Description**: Returns the weekday index for `expr`.

**Return value type**: BIGINT.

**Applicable column types**: UNIX-style timestamps in BIGINT and TIMESTAMP format and other timestamps in VARCHAR and NCHAR format.

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**: 
- Return value 1 = Sunday, 2 = Monday, …, 7 = Saturday.
- If `expr` is NULL, the function returns NULL.
- The precision of the input timestamp is determined by the table being queried. If no table is specified, the precision defaults to milliseconds.

**Examples**: 
```sql
taos> select dayofweek('2000-01-01');
 dayofweek('2000-01-01') |
==========================
                       7 |
```

## Aggregate Functions

Aggregate functions return one row per group. You can use windows or GROUP BY to group data. Otherwise, the entire query is considered a single group.

TDengine supports the following aggregate functions:

### APERCENTILE

```sql
APERCENTILE(expr, p [, algo_type])

algo_type: {
    "default"
  | "t-digest"
}
```

**Description**: Similar to `PERCENTILE`, but a simulated result is returned

**Return value type**: DOUBLE

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**Explanations**:
- _p_ is in range [0,100], when _p_ is 0, the result is same as using function MIN; when _p_ is 100, the result is same as function MAX.
- `algo_type` can only be input as `default` or `t-digest` Enter `default` to use a histogram-based algorithm. Enter `t-digest` to use the t-digest algorithm to calculate the approximation of the quantile. `default` is used by default.
- The approximation result of `t-digest` algorithm is sensitive to input data order. For example, when querying STable with different input data order there might be minor differences in calculated results.

### AVG

```sql
AVG(expr)
```

**Description**: The average value of the specified fields.

**Return value type**: DOUBLE

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables


### COUNT

```sql
COUNT({* | expr})
```

**Description**: The number of records in the specified fields.

**Return value type**: BIGINT

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**More explanation**:

- Wildcard (\*) is used to represent all columns.
If you input a specific column, the number of non-null values in the column is returned.


### ELAPSED

```sql
ELAPSED(ts_primary_key [, time_unit])
```

**Description**: `elapsed` function can be used to calculate the continuous time length in which there is valid data. If it's used with `INTERVAL` clause, the returned result is the calculated time length within each time window. If it's used without `INTERVAL` clause, the returned result is the calculated time length within the specified time range. Please be noted that the return value of `elapsed` is the number of `time_unit` in the calculated time length.

**Return value type**: Double if the input value is not NULL;

**Return value type**: TIMESTAMP

**Applicable tables**: table, STable, outer in nested query

**Explanations**:
- `ts_primary_key` parameter can only be the first column of a table, i.e. timestamp primary key.
- The minimum value of `time_unit` is the time precision of the database. If `time_unit` is not specified, the time precision of the database is used as the default time unit. Time unit specified by `time_unit` can be:
          1b (nanoseconds), 1u (microseconds), 1a (milliseconds), 1s (seconds), 1m (minutes), 1h (hours), 1d (days), or 1w (weeks)
- It can be used with `INTERVAL` to get the time valid time length of each time window. Please be noted that the return value is same as the time window for all time windows except for the first and the last time window.
- `order by asc/desc` has no effect on the result.
- `group by tbname` must be used together when `elapsed` is used against a STable.
- `group by` must NOT be used together when `elapsed` is used against a table or sub table.
- When used in nested query, it's only applicable when the inner query outputs an implicit timestamp column as the primary key. For example, `select elapsed(ts) from (select diff(value) from sub1)` is legal usage while `select elapsed(ts) from (select * from sub1)` is not. In addition, because elapsed has a strict dependency on the timeline, a statement like `select elapsed(ts) from (select diff(value) from st group by tbname) will return a meaningless result.
- It can't be used with `leastsquares`, `diff`, `derivative`, `top`, `bottom`, `last_row`, `interp`.


### LEASTSQUARES

```sql
LEASTSQUARES(expr, start_val, step_val)
```

**Description**: The linear regression function of a specified column, `start_val` is the initial value and `step_val` is the step value.

**Return value type**: A string in the format of "(slope, intercept)"

**Applicable data types**: Numeric

**Applicable table types**: table only


### SPREAD

```sql
SPREAD(expr)
```

**Description**: The difference between the max and the min of a specific column

**Return value type**: DOUBLE

**Applicable data types**: Integers and timestamps

**Applicable table types**: standard tables and supertables


### STDDEV/STDDEV_POP

```sql
STDDEV/STDDEV_POP(expr)
```

**Description**: Population standard deviation of a specific column in a table or STable

**Return value type**: DOUBLE

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**Examples**:
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

**Description**: Population standard variance of a specific column in a table or STable

**Return value type**: DOUBLE

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**Examples**:
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

**Description**: The sum of a specific column in a table or STable

**Return value type**: DOUBLE or BIGINT

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables


### HYPERLOGLOG

```sql
HYPERLOGLOG(expr)
```

**Description**:
  The cardinal number of a specific column is returned by using hyperloglog algorithm. The benefit of using hyperloglog algorithm is that the memory usage is under control when the data volume is huge.
  However, when the data volume is very small, the result may be not accurate, it's recommended to use `select count(data) from (select unique(col) as data from table)` in this case.

**Return value type**: Integer

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables


### HISTOGRAM

```sql
HISTOGRAM(expr, bin_type, bin_description, normalized)
```

**Description**: Returns count of data points in user-specified ranges.

**Return value type** If normalized is set to 1, a DOUBLE is returned; otherwise a BIGINT is returned

**Applicable data types**: Numeric

**Applicable table types**: table, STable

**Explanations**:
- bin_type: parameter to indicate the bucket type, valid inputs are: "user_input", "linear_bin", "log_bin".
- bin_description: parameter to describe how to generate buckets can be in the following JSON formats for each bin_type respectively:
    - "user_input": "[1, 3, 5, 7]":
       User specified bin values.

    - "linear_bin": "&lcub;"start": 0.0, "width": 5.0, "count": 5, "infinity": true&rcub;"
       "start" - bin starting point.       "width" - bin offset.       "count" - number of bins generated.       "infinity" - whether to add (-inf, inf) as start/end point in generated set of bins.
       The above "linear_bin" descriptor generates a set of bins: [-inf, 0.0, 5.0, 10.0, 15.0, 20.0, +inf].

    - "log_bin": "&lcub;"start":1.0, "factor": 2.0, "count": 5, "infinity": true&rcub;"
       "start" - bin starting point.       "factor" - exponential factor of bin offset.       "count" - number of bins generated.       "infinity" - whether to add (-inf, inf) as start/end point in generated range of bins.
       The above "linear_bin" descriptor generates a set of bins: [-inf, 1.0, 2.0, 4.0, 8.0, 16.0, +inf].
- normalized: setting to 1/0 to turn on/off result normalization. Valid values are 0 or 1.


### PERCENTILE

```sql
PERCENTILE(expr, p [, p1] ...)
```

**Description**: The value whose rank in a specific column matches the specified percentage. If such a value matching the specified percentage doesn't exist in the column, an interpolation value will be returned.

**Return value type**: This function takes 2 minimum and 11 maximum parameters, and it can simultaneously return 10 percentiles at most. If 2 parameters are given, a single percentile is returned and the value type is DOUBLE.
                       If more than 2 parameters are given, the return value type is a VARCHAR string, the format of which is a JSON ARRAY containing all return values.

**Applicable column types**: Numeric

**Applicable table types**: table only

**More explanations**:

- _p_ is in range [0,100], when _p_ is 0, the result is same as using function MIN; when _p_ is 100, the result is same as function MAX.
- When calculating multiple percentiles of a specific column, a single PERCENTILE function with multiple parameters is advised, as this can largely reduce the query response time.
  For example, using SELECT percentile(col, 90, 95, 99) FROM table will perform better than SELECT percentile(col, 90), percentile(col, 95), percentile(col, 99) from table.

## Selection Functions

Selection functions return one or more results depending. You can specify the timestamp column, tbname pseudocolumn, or tag columns to show which rows contain the selected value.

### BOTTOM

```sql
BOTTOM(expr, k)
```

**Description**: The least _k_ values of a specific column in a table or STable. If a value has multiple occurrences in the column but counting all of them in will exceed the upper limit _k_, then a part of them will be returned randomly.

**Return value type**:Same as the data type of the column being operated upon

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**More explanation**:

- _k_ must be in range [1,100]
- The timestamp associated with the selected values are returned too
- Can't be used with `FILL`

### FIRST

```sql
FIRST(expr)
```

**Description**: The first non-null value of a specific column in a table or STable

**Return value type**:Same as the data type of the column being operated upon

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**More explanation**:

- FIRST(\*) can be used to get the first non-null value of all columns; When querying a super table and multiResultFunctionStarReturnTags is set to 0 (default), FIRST(\*) only returns columns of super table; When set to 1, returns columns and tags of the super table.
- NULL will be returned if all the values of the specified column are all NULL
- A result will NOT be returned if all the columns in the result set are all NULL
- For a table with composite primary key, the data with the smallest primary key value is returned.

### INTERP

```sql
INTERP(expr [, ignore_null_values])

ignore_null_values: {
    0
  | 1
}
```

**Description**: The value that matches the specified timestamp range is returned, if existing; or an interpolation value is returned. The value of `ignore_null_values` can be 0 or 1, 1 means null values are ignored. The default value of this parameter is 0.


**Return value type**: Same as the column being operated upon

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**More explanations**

- `INTERP` is used to get the value that matches the specified time slice from a column. If no such value exists an interpolation value will be returned based on `FILL` parameter.
- The input data of `INTERP` is the value of the specified column and a `where` clause can be used to filter the original data. If no `where` condition is specified then all original data is the input.
- `INTERP` must be used along with `RANGE`, `EVERY`, `FILL` keywords.
- The output time range of `INTERP` is specified by `RANGE(timestamp1,timestamp2)` parameter, with timestamp1 &lt;= timestamp2. timestamp1 is the starting point of the output time range. timestamp2 is the ending point of the output time range.
- The number of rows in the result set of `INTERP` is determined by the parameter `EVERY(time_unit)`. Starting from timestamp1, one interpolation is performed for every time interval specified `time_unit` parameter. The parameter `time_unit` must be an integer, with no quotes, with a time unit of: a(millisecond)), s(second), m(minute), h(hour), d(day), or w(week). For example, `EVERY(500a)` will interpolate every 500 milliseconds.
- Interpolation is performed based on `FILL` parameter. For more information about FILL clause, see [FILL Clause](../distinguished/#fill-clause).
- When only one timestamp value is specified in `RANGE` clause, `INTERP` is used to generate interpolation at this point in time. In this case, `EVERY` clause can be omitted. For example, SELECT INTERP(col) FROM tb RANGE('2023-01-01 00:00:00') FILL(linear).
- `INTERP` can be applied to supertable by interpolating primary key sorted data of all its childtables. It can also be used with `partition by tbname` when applied to supertable to generate interpolation on each single timeline.
- Pseudocolumn `_irowts` can be used along with `INTERP` to return the timestamps associated with interpolation points(support after version 3.0.2.0).
- Pseudocolumn `_isfilled` can be used along with `INTERP` to indicate whether the results are original records or data points generated by interpolation algorithm(support after version 3.0.3.0).
- For a table with composite primary key, onley the data with the smallest primary key value is used to generate interpolation.

**Example**

- We use the smart meters example used in this documentation to illustrate how to use the INTERP function.
- We want to downsample every 1 hour and use a linear fill for missing values. Note the order in which the "partition by" clause and the "range", "every" and "fill" parameters are used.

```sql
SELECT _irowts,INTERP(current) FROM test.meters PARTITION BY TBNAME RANGE('2017-07-22 00:00:00','2017-07-24 12:25:00') EVERY(1h) FILL(LINEAR)
```

### LAST

```sql
LAST(expr)
```

**Description**: The last non-NULL value of a specific column in a table or STable

**Return value type**:Same as the data type of the column being operated upon

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**More explanation**:

- LAST(\*) can be used to get the last non-NULL value of all columns; When querying a super table and multiResultFunctionStarReturnTags is set to 0 (default), LAST(\*) only returns columns of super table; When set to 1, returns columns and tags of the super table.
- If the values of a column in the result set are all NULL, NULL is returned for that column; if all columns in the result are all NULL, no result will be returned.
- When it's used on a STable, if there are multiple values with the timestamp in the result set, one of them will be returned randomly and it's not guaranteed that the same value is returned if the same query is run multiple times.
- For a table with composite primary key, the data with the largest primary key value is returned.


### LAST_ROW

```sql
LAST_ROW(expr)
```

**Description**: The last row of a table or STable

**Return value type**:Same as the data type of the column being operated upon

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**More explanations**:

- LAST_ROW(\*) can be used to get the last value of all columns; When querying a super table and multiResultFunctionStarReturnTags is set to 0 (default), LAST_ROW(\*) only returns columns of super table; When set to 1, returns columns and tags of the super table.
- When it's used on a STable, if there are multiple values with the timestamp in the result set, one of them will be returned randomly and it's not guaranteed that the same value is returned if the same query is run multiple times.
- Can't be used with `INTERVAL`.
- Like `LAST`, the data with the largest primary key value is returned for a table with composite primary key.

### MAX

```sql
MAX(expr)
```

**Description**: The maximum value of a specific column of a table or STable

**Return value type**:Same as the data type of the column being operated upon

**Applicable data types**: Numeric, VARCHAR，NCHAR.

**Applicable table types**: standard tables and supertables

**More explanations**: MAX() may take a string argument; in such cases, it returns the maximum string value.

### MIN

```sql
MIN(expr)
```

**Description**: The minimum value of a specific column in a table or STable

**Return value type**:Same as the data type of the column being operated upon

**Applicable data types**: Numeric, VARCHAR，NCHAR.

**Applicable table types**: standard tables and supertables

**More explanations**: MIN() may take a string argument; in such cases, it returns the minimum string value.

### MODE

```sql
MODE(expr)
```

**Description**:The value which has the highest frequency of occurrence. One random value is returned if there are multiple values which have highest frequency of occurrence.

**Return value type**: Same as the input data

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables


### SAMPLE

```sql
SAMPLE(expr, k)
```

**Description**: _k_ sampling values of a specific column. The applicable range of _k_ is [1,1000].

**Return value type**: Same as the column being operated

**Applicable data types**: Any data type

**Applicable nested query**: Inner query and Outer query

**Applicable table types**: standard tables and supertables


### TAIL

```sql
TAIL(expr, k, offset_val)
```

**Description**: The next _k_ rows are returned after skipping the last `offset_val` rows, NULL values are not ignored. `offset_val` is optional parameter. When it's not specified, the last _k_ rows are returned. When `offset_val` is used, the effect is same as `order by ts desc LIMIT k OFFSET offset_val`.

**Parameter value range**: k: [1,100] offset_val: [0,100]

**Return value type**:Same as the data type of the column being operated upon

**Applicable data types**: Any data type except for timestamp, i.e. the primary key

**Applicable table types**: standard tables and supertables


### TOP

```sql
TOP(expr, k)
```

**Description**: The greatest _k_ values of a specific column in a table or STable. If a value has multiple occurrences in the column but counting all of them in will exceed the upper limit _k_, then a part of them will be returned randomly.

**Return value type**:Same as the data type of the column being operated upon

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**More explanation**:

- _k_ must be in range [1,100]
- The timestamp associated with the selected values are returned too
- Can't be used with `FILL`

### UNIQUE

```sql
UNIQUE(expr)
```

**Description**: The values that occur the first time in the specified column. The effect is similar to `distinct` keyword. For a table with composite primary key, only the data with the smallest primary key value is returned.

**Return value type**:Same as the data type of the column being operated upon

**Applicable column types**: Any data types

**Applicable table types**: table, STable


## Time-Series Extensions

TDengine includes extensions to standard SQL that are intended specifically for time-series use cases. The functions enabled by these extensions require complex queries to implement in general-purpose databases. By offering them as built-in extensions, TDengine reduces user workload.

### CSUM

```sql
CSUM(expr)
```

**Description**: The cumulative sum of each row for a specific column, NULL value will be discard.

**Return value type**: Long integer for integers; Double for floating points. uint64_t for unsigned integers

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**:

- Arithmetic operation can't be performed on the result of `csum` function
- Can only be used with aggregate functions This function can be used with supertables and standard tables.


### DERIVATIVE

```sql
DERIVATIVE(expr, time_inerval, ignore_negative)

ignore_negative: {
    0
  | 1
}
```

**Description**: The derivative of a specific column. The time rage can be specified by parameter `time_interval`, the minimum allowed time range is 1 second (1s); the value of `ignore_negative` can be 0 or 1, 1 means negative values are ignored. For tables with composite primary key, the data with the smallest primary key value is used to calculate the derivative.

**Return value type**: DOUBLE

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables

**More explanation**:

- It can be used together with a selected column. For example: select \_rowts, DERIVATIVE() from.

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

**Description**: The difference of each row with its previous row for a specific column. `ignore_option` takes the value of 0|1|2|3, the default value is 0 if it's not specified. 
- `0` means that negative values ​​(diff results) are not ignored and null values ​​are not ignored
- `1` means that negative values ​​(diff results) are treated as null values
- `2` means that negative values ​​(diff results) are not ignored but null values ​​are ignored
- `3` means that negative values ​​(diff results) are ignored and null values ​​are ignored
- For tables with composite primary key, the data with the smallest primary key value is used to calculate the difference.

**Return value type**: `bool`, `timestamp` and `integer` value type all return `int_64`, `float` type returns `double`; if the diff result overflows, it is returned as overflow.

**Applicable data types**: Numeric type, timestamp and bool type.

**Applicable table types**: standard tables and supertables

**More explanation**:

- diff is to calculate the difference of a specific column in current row and the **first valid data before the row**. The **first valid data before the row** refers to the most adjacent non-null value of same column with smaller timestamp.
- The diff result of numeric type is the corresponding arithmatic difference; the timestamp is calculated based on the timestamp precision of the database; when calculating diff, `true` is treated as 1 and `false` is treated as 0
- If the data of current row is NULL or can't find the **first valid data before the current row**, the diff result is NULL
- When ignoring negative values ​​(ignore_option is set to 1 or 3), if the diff result is negative, the result is set to null, and then filtered according to the null value filtering rule
- When the diff result has an overflow, whether to ignore the negative value depends on the result of the logical operation is positive or negative. For example, the value of 9223372036854775800 - (-9223372036854775806) exceeds the range of BIGINT, and the diff result will display the overflow value -10, but it will not be ignored as a negative value
- Single or multiple diffs can be used in a single statement, and for each diff you can specify same or different `ignore_option`. When there are multiple diffs in a single statement, when and only when all the diff results are NULL for a row and each diff's `ignore_option` is specified as ignoring NULL, the output of this row will be removed from the result set.
- Can be used with the selected associated columns. For example: `select _rowts, DIFF()`.
- When there is not composite primary key, if there are the same timestamps across different subtables, it will prompt "Duplicate timestamps not allowed"
- When using with composite primary key, there may be same combination of timestamp and complete primary key across sub-tables, which row will be used depends on which row is found first, that means the result of running diff() multiple times may be different in such a case

### IRATE

```sql
IRATE(expr)
```

**Description**: instantaneous rate on a specific column. The last two samples in the specified time range are used to calculate instantaneous rate. If the last sample value is smaller, then only the last sample value is used instead of the difference between the last two sample values. For tables with composite primary key, the data with the smallest primary key value is used to calculate the rate.

**Return value type**: DOUBLE

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables


### MAVG

```sql
MAVG(expr, k)
```

**Description**: The moving average of continuous _k_ values of a specific column. If the number of input rows is less than _k_, nothing is returned. The applicable range of _k_ is [1,1000].

**Return value type**: DOUBLE

**Applicable data types**: Numeric

**Nested query**: It can be used in both the outer query and inner query in a nested query.

**Applicable table types**: standard tables and supertables

**More explanations**:

- Arithmetic operation can't be performed on the result of `MAVG`.
- Can only be used with data columns, can't be used with tags. - Can't be used with aggregate functions.


### STATECOUNT

```sql
STATECOUNT(expr, oper, val)
```

**Description**: The number of continuous rows satisfying the specified conditions for a specific column. The result is shown as an extra column for each row. If the specified condition is evaluated as true, the number is increased by 1; otherwise the number is reset to -1. If the input value is NULL, then the corresponding row is skipped.

**Applicable parameter values**:

- oper : Can be one of `'LT'` (lower than), `'GT'` (greater than), `'LE'` (lower than or equal to), `'GE'` (greater than or equal to), `'NE'` (not equal to), `'EQ'` (equal to), the value is case insensitive, the value must be in quotes.
- val: Numeric types

**Return value type**: Integer

**Applicable data types**: Numeric

**Applicable nested query**: Outer query only

**Applicable table types**: standard tables and supertables

**More explanations**:

- Can't be used with window operation, like interval/state_window/session_window


### STATEDURATION

```sql
STATEDURATION(expr, oper, val, unit)
```

**Description**: The length of time range in which all rows satisfy the specified condition for a specific column. The result is shown as an extra column for each row. The length for the first row that satisfies the condition is 0. Next, if the condition is evaluated as true for a row, the time interval between current row and its previous row is added up to the time range; otherwise the time range length is reset to -1. If the value of the column is NULL, the corresponding row is skipped.

**Applicable parameter values**:

- oper : Can be one of `'LT'` (lower than), `'GT'` (greater than), `'LE'` (lower than or equal to), `'GE'` (greater than or equal to), `'NE'` (not equal to), `'EQ'` (equal to), the value is case insensitive, the value must be in quotes.
- val: Numeric types
- unit: The unit of time interval. Enter one of the following options: 1b (nanoseconds), 1u (microseconds), 1a (milliseconds), 1s (seconds), 1m (minutes), 1h (hours), 1d (days), or 1w (weeks) If you do not enter a unit of time, the precision of the current database is used by default.

**Return value type**: Integer

**Applicable data types**: Numeric

**Applicable nested query**: Outer query only

**Applicable table types**: standard tables and supertables

**More explanations**:

- Can't be used with window operation, like interval/state_window/session_window


### TWA

```sql
TWA(expr)
```

**Description**: Time weighted average on a specific column within a time range. For tables with composite primary key, the data with the smallest primary key value is used to calculate the average.

**Return value type**: DOUBLE

**Applicable data types**: Numeric

**Applicable table types**: standard tables and supertables



## System Information Functions

### DATABASE

```sql
SELECT DATABASE();
```

**Description**: The current database. If no database is specified upon logging in and no database is specified with `USE` after login, NULL will be returned by `select database()`.


### CLIENT_VERSION

```sql
SELECT CLIENT_VERSION();
```

**Description**: The client version.

### SERVER_VERSION

```sql
SELECT SERVER_VERSION();
```

**Description**: The server version.

### SERVER_STATUS

```sql
SELECT SERVER_STATUS();
```

**Description**: The server status. When checking the status of a cluster, the recommended way is to use `SHOW CLUSTER ALIVE;`. Unlike `SELECT SERVER_STATUS();`, it does not return an error when some nodes in the cluster are unavailable; instead, it returns different status codes. Plese check [SHOW CLUSTER ALIVE](https://docs.tdengine.com/reference/taos-sql/show/#show-cluster-alive) for details.

### CURRENT_USER

```sql
SELECT CURRENT_USER();
```

**Description**: get current user.


## Geometry Functions

### Geometry Input Functions

Geometry input functions create geometry data from WTK.

#### ST_GeomFromText

```sql
ST_GeomFromText(VARCHAR WKT expr)
```

**Description**: Return a specified GEOMETRY value from Well-Known Text representation (WKT).

**Return value type**: GEOMETRY

**Applicable data types**: VARCHAR

**Applicable table types**: standard tables and supertables

**Explanations**: 
- The input can be one of WTK string, like POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRYCOLLECTION.
- The output is a GEOMETRY data type, internal defined as binary string.

### Geometry Output Functions

Geometry output functions convert geometry data into WTK.

#### ST_AsText

```sql
ST_AsText(GEOMETRY geom)
```

**Description**: Return a specified Well-Known Text representation (WKT) value from GEOMETRY data.

**Return value type**: VARCHAR

**Applicable data types**: GEOMETRY

**Applicable table types**: standard tables and supertables

**Explanations**: 
- The output can be one of WTK string, like POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRYCOLLECTION.

### Geometry Relationships Functions

Geometry relationships functions determine spatial relationships between geometries.

#### ST_Intersects

```sql
ST_Intersects(GEOMETRY geomA, GEOMETRY geomB)
```

**Description**: Compares two geometries and returns true if they intersect.

**Return value type**: BOOL

**Applicable data types**: GEOMETRY, GEOMETRY

**Applicable table types**: standard tables and supertables

**Explanations**: 
- Geometries intersect if they have any point in common.


#### ST_Equals

```sql
ST_Equals(GEOMETRY geomA, GEOMETRY geomB)
```

**Description**: Returns TRUE if the given geometries are "spatially equal".

**Return value type**: BOOL

**Applicable data types**: GEOMETRY, GEOMETRY

**Applicable table types**: standard tables and supertables

**Explanations**: 
- 'Spatially equal' means ST_Contains(A,B) = true and ST_Contains(B,A) = true, and the ordering of points can be different but represent the same geometry structure.


#### ST_Touches

```sql
ST_Touches(GEOMETRY geomA, GEOMETRY geomB)
```

**Description**: Returns TRUE if A and B intersect, but their interiors do not intersect.

**Return value type**: BOOL

**Applicable data types**: GEOMETRY, GEOMETRY

**Applicable table types**: standard tables and supertables

**Explanations**: 
- A and B have at least one point in common, and the common points lie in at least one boundary.
- For Point/Point inputs the relationship is always FALSE, since points do not have a boundary.


#### ST_Covers

```sql
ST_Covers(GEOMETRY geomA, GEOMETRY geomB)
```

**Description**: Returns TRUE if every point in Geometry B lies inside (intersects the interior or boundary of) Geometry A.

**Return value type**: BOOL

**Applicable data types**: GEOMETRY, GEOMETRY

**Applicable table types**: standard tables and supertables

**Explanations**: 
- A covers B means no point of B lies outside (in the exterior of) A.


#### ST_Contains

```sql
ST_Contains(GEOMETRY geomA, GEOMETRY geomB)
```

**Description**: Returns TRUE if geometry A contains geometry B.

**Return value type**: BOOL

**Applicable data types**: GEOMETRY, GEOMETRY

**Applicable table types**: standard tables and supertables

**Explanations**: 
- A contains B if and only if all points of B lie inside (i.e. in the interior or boundary of) A (or equivalently, no points of B lie in the exterior of A), and the interiors of A and B have at least one point in common.


#### ST_ContainsProperly

```sql
ST_ContainsProperly(GEOMETRY geomA, GEOMETRY geomB)
```

**Description**: Returns TRUE if every point of B lies inside A.

**Return value type**: BOOL

**Applicable data types**: GEOMETRY, GEOMETRY

**Applicable table types**: standard tables and supertables

**Explanations**: 
- There is no point of B that lies on the boundary of A or in the exterior of A.
