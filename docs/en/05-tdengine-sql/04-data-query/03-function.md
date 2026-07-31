---
title: Functions
toc_max_heading_level: 4
---

## Function Classification

### Three Types of Functions: Scalar, Aggregate, and Set

Functions are divided into three categories based on the **input-to-output row relationship**, which is the primary classification dimension:

1. **Scalar Functions**: Each input row produces one output row (1 → 1). Each row's calculation depends only on the current row's input values.

2. **Aggregate Functions**: Multiple input rows produce a single result value (N → 1). The result can only be produced after seeing all input rows.

3. **Set Functions**: Multiple input rows produce one or more result rows (N → M, where M ≥ 1). The number of output rows is determined by the function semantics, parameters, or data. M=1 is a degenerate case (e.g., `TOP(col,1)`); the difference from aggregate functions is that aggregate functions always output exactly 1 row while set functions may output more than 1. The difference from scalar functions is that each output row of a set function may depend on multiple input rows (e.g., neighboring row context), whereas scalar functions depend only on the current row.

### Pipeline Function Description

Beyond the three primary categories, some functions also have an **orthogonal attribute — Pipeline**:

> **Pipeline Functions**: Each output row of the function maintains a correspondence with one input row, so scalar expressions or regular columns on the same output row can continue to use that input row's timestamp and other column values. A pipeline function itself may directly return a value from the corresponding input row, or compute a result from that input row, neighboring rows, cumulative state, sliding windows, or candidate sets.

The pipeline attribute can appear in both aggregate and set function categories:

- **Pipeline Aggregate Functions** (N→1): FIRST, LAST, LAST_ROW, MAX, MIN, MODE
- **Pipeline Set Functions** (N→M): LAG, LEAD, FILL_FORWARD, CSUM, MAVG, STATECOUNT, STATEDURATION, DIFF, DERIVATIVE, TOP, BOTTOM, SAMPLE, TAIL, UNIQUE, COLS

#### Core Characteristics

The essential characteristic that distinguishes pipeline functions from ordinary aggregate functions is that **they execute before scalar functions, their output serves as input for co-located scalar/regular columns, and there is a row-correspondence relationship between output and input** — each output row corresponds to a specific row in the original input, and that row's timestamp and other column values can be used in subsequent scalar operations.

Pipeline functions are divided into two types based on their computation mode:

| Mode | Description | Representative Functions |
|------|-------------|--------------------------|
| **Row-by-Row Transformation** | Scans input row by row in order; each row is processed by the function to produce a corresponding output row; computation may depend on context (neighboring rows, cumulative state, sliding window, etc.), but output rows maintain positional correspondence with input rows | LAG, LEAD, FILL_FORWARD, CSUM, MAVG, STATECOUNT, STATEDURATION, DIFF, DERIVATIVE |
| **Subset Selection** | Selects a subset of original rows from the full input based on sorting, random sampling, or positional criteria; each selected row is still a complete original row with unchanged timestamp and column values | FIRST, LAST, LAST_ROW, MAX, MIN, MODE, TOP, BOTTOM, SAMPLE, TAIL, UNIQUE |

#### Execution Order When Coexisting with Other Functions

When multiple functions appear together in the same SELECT, execution proceeds in two phases:

##### Phase 1: Non-scalar functions compute independently and in parallel

Pipeline functions and other aggregate/set functions each **independently and in parallel** process the same input sequence without interfering with each other. Note that scalar functions nested within other non-scalar functions also execute in this phase.

##### Phase 2: Scalar function post-processing

Co-located scalar functions do not participate in Phase 1. Instead, after Phase 1 pipeline functions have determined the output row set, scalar functions **evaluate independently on each output row**, effectively post-processing the results of the previous phase. The original column values referenced by scalar expressions are taken from the input row corresponding to the current output row. When multiple pipeline functions are present, the constraints and behavior are described in "Uniqueness Constraint When Scalar and Pipeline Functions Coexist."

##### Complete Example

```sql
SELECT abs(voltage), CSUM(current), LAG(voltage, 1) FROM meters;
```

Assume 4 input rows:

| ts | voltage | current |
|----|---------|---------|
| t1 | 220     | 1.0     |
| t2 | 215     | 1.5     |
| t3 | 225     | 2.0     |
| t4 | 210     | 1.2     |

**Phase 1**: `CSUM(current)` and `LAG(voltage, 1)` each independently scan the same input, both outputting N=4 rows. Both are row-by-row transformation type; the i-th output row comes from the same input row, so they are concatenated by timestamp alignment:

| ts | CSUM(current) | LAG(voltage,1) | voltage (original column, for scalar use) |
|----|--------------|----------------|-------------------------------------------|
| t1 | 1.0          | NULL           | 220 |
| t2 | 2.5          | 220            | 215 |
| t3 | 4.5          | 215            | 225 |
| t4 | 5.7          | 225            | 210 |

**Phase 2**: `abs(voltage)` takes the voltage value on each output row and computes the absolute value (voltage values are all positive here, so the result is unchanged — shown for illustration only):

| ts | abs(voltage) | CSUM(current) | LAG(voltage,1) |
|----|-------------|--------------|----------------|
| t1 | 220         | 1.0          | NULL           |
| t2 | 215         | 2.5          | 220            |
| t3 | 225         | 4.5          | 215            |
| t4 | 210         | 5.7          | 225            |

## Function Usage Rules

### Nesting Rules

1. **Scalar functions can nest with any type of function** as long as parameter requirements are satisfied:
   - Scalar functions can be used as arguments to aggregate, set, or other scalar functions (scalar as inner layer): `SUM(abs(voltage))`
   - Results of aggregate, set, or other scalar functions can be used as arguments to scalar functions (scalar as outer layer): `abs(SUM(voltage))`, `ROUND(AVG(voltage), 2)`, `abs(TOP(voltage, 1))`
2. **Aggregate and set functions cannot nest with each other**: Aggregate functions and set functions cannot be directly or indirectly nested with each other, with no exceptions.

### Co-location Rules

1. **Scalar ↔ Scalar**: Can coexist.
2. **Aggregate ↔ Aggregate** (aggregate co-location rule): Can coexist; pipeline aggregate functions and non-pipeline aggregate functions can also be mixed. Valid: `SELECT MAX(voltage), SUM(current), COUNT(*) FROM meters`
3. **Set ↔ Set** (set co-location rule): Can coexist when output row counts are equal; not allowed when row counts differ.
4. **Scalar ↔ Aggregate** (scalar-aggregate co-location rule): Scalar expressions need a pipeline aggregate function to provide a row anchor.
   - **SELECT contains a pipeline aggregate function**: Scalars can coexist with it; non-pipeline aggregate functions in the same SELECT can also coexist via the aggregate co-location rule. Valid: `SELECT abs(voltage), MAX(voltage) FROM meters`, `SELECT abs(voltage), MAX(voltage), SUM(current) FROM meters`
   - **SELECT contains only non-pipeline aggregate functions (no pipeline aggregate functions)**: Scalars cannot coexist — there is no row anchor. Invalid: `SELECT abs(voltage), SUM(voltage) FROM meters`
5. **Scalar ↔ Set** (scalar-set co-location rule), two cases:
   - **Scalar ↔ Pipeline Set Function**: Can coexist; row count does not need to be considered. Valid: `SELECT abs(voltage), TOP(voltage, 5) FROM meters`, `SELECT voltage + 1, DIFF(voltage) FROM meters`
   - **Scalar ↔ Non-pipeline Set Function (HISTOGRAM, etc.)**: **Cannot coexist**. Invalid: `SELECT abs(voltage), HISTOGRAM(voltage, 'linear_bin', '...', 0) FROM meters`
6. **Aggregate ↔ Set** (aggregate-set co-location rule): Can coexist when output row counts are equal; not allowed when row counts differ.

### Uniqueness Constraint When Scalar and Pipeline Functions Coexist

When pipeline functions are present and the SELECT clause also contains scalar expressions, pipeline functions that **select a specific subset** from the input (FIRST, LAST, LAST_ROW, MAX, MIN, MODE, TOP, BOTTOM, SAMPLE, TAIL, UNIQUE) may have at most **one** such function; pipeline set functions that perform **row-by-row transformation** on input rows (LAG, LEAD, FILL_FORWARD, CSUM, MAVG, STATECOUNT, STATEDURATION, DIFF, DERIVATIVE) are not subject to this constraint, but multiple set functions coexisting must still satisfy the equal output row count requirement.

> - Valid: `SELECT abs(voltage), MAX(voltage) FROM meters` — only one subset-selection pipeline function, row context is unambiguous
> - Invalid: `SELECT abs(voltage), MAX(voltage), MIN(current) FROM meters` — two subset-selection pipeline functions, each anchoring a different row, scalar row context is ambiguous
> - Invalid: `SELECT abs(voltage), TOP(voltage,5), TOP(current,5) FROM meters` — two subset-selection pipeline functions, each selecting different sets of original rows
> - Valid: `SELECT voltage + 1, LAG(voltage, 1), CSUM(current) FROM meters` — all are row-by-row transformation pipeline set functions, not subject to the uniqueness constraint (but must satisfy the set co-location rule: LAG and CSUM both output N rows)

## Mathematical Functions

### ABS

```sql
ABS(expr)
```

**Function Classification**: Scalar function.

**Function Description**: Gets the absolute value of the specified field.

**Return Type**: Consistent with the original data type of the specified field.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**: Can only be used with normal columns, selection, and projection functions, not with aggregation functions.

### ACOS

```sql
ACOS(expr)
```

**Function Classification**: Scalar function.

**Function Description**: Gets the arccosine of the specified field.

**Return Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**: Can only be used with normal columns, selection, and projection functions, not with aggregation functions.

### ASIN

```sql
ASIN(expr)
```

**Function Classification**: Scalar function.

**Function Description**: Gets the arcsine of the specified field.

**Return Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**: Can only be used with normal columns, selection, and projection functions, not with aggregation functions.

### ATAN

```sql
ATAN(expr)
```

**Function Classification**: Scalar function.

**Function Description**: Gets the arctangent of the specified field.

**Return Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**: Can only be used with normal columns, selection, and projection functions, not with aggregation functions.

### CEIL

```sql
CEIL(expr)
```

**Function Classification**: Scalar function.

**Function Description**: Gets the ceiling of the specified field.

**Return Type**: Consistent with the original data type of the specified field.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Usage Notes**: Can only be used with normal columns, selection, and projection functions, not with aggregation functions.

### CORR

```sql
CORR(expr1, expr2)
```

**Function Classification**: Aggregate function.

**Function Description**: Calculates the Pearson correlation coefficient between two columns of data. This value reflects the strength and direction of the linear relationship between two sequences, and the return value is between -1 and 1.

**Version**: v3.3.8.0

**Return Data Type**: DOUBLE.

**Applicable Data Types**:

- `expr1`: Numeric types.
- `expr2`: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- If `expr1` or `expr2` is NULL, returns NULL.

**Example**:

```sql
taos> select k, j from test_corr;
      k      |      j      |
============================
           1 |           2 |
           2 |           3 |
           3 |           5 |
           4 |           7 |
           5 |           8 |

taos> select corr(k, j) from test_corr;
         corr(k,j)         |
============================
         0.992277876713668 |
```

### COS

```sql
COS(expr)
```

**Function Classification**: Scalar function.

**Function Description**: Gets the cosine of the specified field.

**Return Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**: Can only be used with normal columns, selection, and projection functions, not with aggregation functions.

### FLOOR

```sql
FLOOR(expr)
```

**Function Classification**: Scalar function.

**Function Description**: Gets the floor of the specified field.
 Other usage notes see [CEIL](#ceil) function description.

### GREATEST

```sql
GREATEST(expr1, expr2[, expr]...)
```

**Function Classification**: Scalar function.

**Function Description**: Get the maximum value of all input parameters. The minimum number of parameters for this function is 2.

**Version**: ver-3.3.6.0

**Return Type**: Refer to the comparison rules. The comparison type is the final return type.

**Applicable Data Types**:

- Numeric types: timestamp, bool, integer and floating point types
- Strings types: nchar and varchar types.

**Comparison rules**: The following rules describe the conversion method of the comparison operation:

- If any parameter is NULL, the comparison result is NULL. (See `ignoreNullInGreatest` below to skip NULL arguments instead.)
- If all parameters in the comparison operation are string types, compare them as string types
- If all parameters are numeric types, compare them as numeric types.
- If there are both string types and numeric types in the parameters, according to the `compareAsStrInGreatest` configuration item, they are uniformly compared as strings or numeric values. By default, they are compared as strings.
- In all cases, when different types are compared, the comparison type will choose the type with a larger range for comparison. For example, when comparing integer types, if there is a BIGINT type, BIGINT will definitely be selected as the comparison type.

**Related configuration items**:

- `compareAsStrInGreatest` (client configuration): `1` means that when both string types and numeric types are present they are uniformly compared as strings; `0` means they are uniformly compared as numeric values. The default is `1`.
- `ignoreNullInGreatest` (client configuration, available since ver-3.4.2.0): `0` (default) keeps the MySQL-compatible behavior — any NULL argument makes the result NULL. `1` skips NULL arguments and compares only the non-NULL values; if every argument is NULL, the result is still NULL. This option is orthogonal to `compareAsStrInGreatest`: it only controls NULL handling, the comparison rules above for non-NULL values are unchanged.

### LEAST

```sql
LEAST(expr1, expr2[, expr]...)
```

**Function Classification**: Scalar function.

**Function Description**: Get the minimum value of all input parameters. The rest of the description is the same as the [GREATEST](#greatest) function.

### LOG

```sql
LOG(expr1[, expr2])
```

**Function Classification**: Scalar function.

**Function Description**: Gets the logarithm of expr1 to the base expr2. If the expr2 parameter is omitted, it returns the natural logarithm of the specified field.

**Return Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**: Can only be used with normal columns, selection, and projection functions, not with aggregation functions.

### POW

```sql
POW(expr1, expr2)
```

**Function Classification**: Scalar function.

**Function Description**: Gets the power of expr1 raised to the exponent expr2.

**Return Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**: Can only be used with regular columns, selection (Selection), projection (Projection) functions, and cannot be used with aggregation (Aggregation) functions.

### ROUND

```sql
ROUND(expr[, digits])
```

**Function Classification**: Scalar function.

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

### SIN

```sql
SIN(expr)
```

**Function Classification**: Scalar function.

**Function Description**: Obtains the sine result of the specified field.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**: Can only be used with regular columns, selection (Selection), projection (Projection) functions, and cannot be used with aggregation (Aggregation) functions.

### SQRT

```sql
SQRT(expr)
```

**Function Classification**: Scalar function.

**Function Description**: Obtains the square root of the specified field.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**: Can only be used with regular columns, selection (Selection), projection (Projection) functions, and cannot be used with aggregation (Aggregation) functions.

### TAN

```sql
TAN(expr)
```

**Function Classification**: Scalar function.

**Function Description**: Obtains the tangent result of the specified field.

**Version**: ver-3.3.3.0

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**: Can only be used with regular columns, selection (Selection), projection (Projection) functions, and cannot be used with aggregation (Aggregation) functions.

### PI

```sql
PI()
```

**Function Classification**: Scalar function.

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

### TRUNCATE

```sql
TRUNCATE(expr, digits)
```

**Function Classification**: Scalar function.

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

### EXP

```sql
EXP(expr)
```

**Function Classification**: Scalar function.

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

### LN

```sql
LN(expr)
```

**Function Classification**: Scalar function.

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

### MOD

```sql
MOD(expr1, expr2)
```

**Function Classification**: Scalar function.

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

### RAND

```sql
RAND([seed])
```

**Function Classification**: Scalar function.

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

### SIGN

```sql
SIGN(expr)
```

**Function Classification**: Scalar function.

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

### DEGREES

```sql
DEGREES(expr)
```

**Function Classification**: Scalar function.

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

### RADIANS

```sql
RADIANS(expr)
```

**Function Classification**: Scalar function.

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

## String Functions

The input parameters for string functions are of string type, and the return results are of numeric type or string type.

### CHAR_LENGTH

```sql
CHAR_LENGTH(expr)
```

**Function Classification**: Scalar function.

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

### CONCAT

```sql
CONCAT(expr1, expr2 [, expr] ... )
```

**Function Classification**: Scalar function.

**Function Description**: String concatenation function.

**Return Type**: If all parameters are of VARCHAR type, the result type is VARCHAR. If parameters include NCHAR type, the result type is NCHAR. If parameters include NULL values, the output is NULL.

**Applicable Data Types**: VARCHAR, NCHAR. The function requires a minimum of 2 parameters and a maximum of 8 parameters.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

### CONCAT_WS

```sql
CONCAT_WS(separator_expr, expr1, expr2 [, expr] ... )
```

**Function Classification**: Scalar function.

**Function Description**: String concatenation function with a separator.

**Return Type**: If all parameters are of VARCHAR type, the result type is VARCHAR. If parameters include NCHAR type, the result type is NCHAR. If parameters include NULL values, the output is NULL.

**Applicable Data Types**: VARCHAR, NCHAR. The function requires a minimum of 3 parameters and a maximum of 9 parameters.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

### FIND_IN_SET

```sql
FIND_IN_SET(expr1, expr2[, expr3])
```

**Function Classification**: Scalar function.

**Function Description**: Split `expr2` into a list of strings using `expr3` as the separator, then return the index of `expr1` in the list, return 0 if not exist.  `expr3` cannot be NULL or empty string, if not provided, the default is `,`.

**Return Type**: BIGINT. If `expr1` or `expr2` is NULL, then return NULL.

**Applicable Data Types**: VARCHAR, NCHAR. The function requires a minimum of 2 parameters and a maximum of 3 parameters.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

### LENGTH

```sql
LENGTH(expr)
```

**Function Classification**: Scalar function.

**Function Description**: Length in bytes.

**Return Result Type**: BIGINT.

**Applicable Data Types**: VARCHAR, NCHAR, VARBINARY.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

### LIKE_IN_SET

```sql
LIKE_IN_SET(expr1, expr2[, expr3])
```

**Function Classification**: Scalar function.

**Function Description**: Split `expr2` into a list of strings using `expr3` as the separator, then match `expr1` with the items using the semantics of the `LIKE` operator, return the index of the first matched item, return 0 if there's no match.  `expr3` cannot be NULL or empty string, if not provided, the default is `,`.

**Return Type**: BIGINT. If `expr1` or `expr2` is NULL, then return NULL.

**Applicable Data Types**: VARCHAR, NCHAR. The function requires a minimum of 2 parameters and a maximum of 3 parameters.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

### LOWER

```sql
LOWER(expr)
```

**Function Classification**: Scalar function.

**Function Description**: Converts the string argument value to all lowercase letters.

**Return Result Type**: Same as the original type of the input field.

**Applicable Data Types**: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

### LTRIM

```sql
LTRIM(expr)
```

**Function Classification**: Scalar function.

**Function Description**: Returns the string after removing left-side spaces.

**Return Result Type**: Same as the original type of the input field.

**Applicable Data Types**: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

### REGEXP_EXTRACT

```sql
REGEXP_EXTRACT(expr, pattern [, group_idx])
```

**Function Classification**: Scalar function.

**Function Description**: Applies the POSIX extended regular expression `pattern` to `expr` and returns the substring matched by capture group `group_idx`. Returns NULL when there is no match or when `expr` or `pattern` is NULL.

**Return Type**: Same as `expr` (VARCHAR or NCHAR).

**Applicable Data Types**: `expr`: VARCHAR, NCHAR. `pattern`: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage**:

- If omitted, `group_idx` defaults to `1`.
- If provided as a non-`NULL` value, `group_idx` must be a non-negative integer constant. `0` returns the entire match; `1` returns the first capture group, `2` the second, and so on. The maximum value is 512.
- If `group_idx` is SQL `NULL`, the function returns `NULL`.
- Returns NULL if `group_idx` exceeds the number of capture groups in `pattern`, or if the addressed group did not participate in the match.
- `pattern` must be provided as a constant literal or parameter placeholder; it cannot reference a column or be computed from other expressions.

**Example**:

```sql
taos> SELECT REGEXP_EXTRACT('2026-04-22', '([0-9]{4})-([0-9]{2})-([0-9]{2})', 1);
 regexp_extract('2026-04-22', '([0-9]{4})-([0-9]{2})-([0-9]{2})', 1) |
=======================================================================
 2026                                                                  |

taos> SELECT REGEXP_EXTRACT('2026-04-22', '([0-9]{4})-([0-9]{2})-([0-9]{2})', 0);
 regexp_extract('2026-04-22', '([0-9]{4})-([0-9]{2})-([0-9]{2})', 0) |
=======================================================================
 2026-04-22                                                            |

taos> SELECT REGEXP_EXTRACT('no-digits-here', '[0-9]+', 1);
 regexp_extract('no-digits-here', '[0-9]+', 1) |
===============================================
 NULL                                          |
```

### REGEXP_IN_SET

```sql
REGEXP_IN_SET(expr1, expr2[, expr3])
```

**Function Classification**: Scalar function.

**Function Description**: Split `expr2` into a list of strings using `expr3` as the separator, then using `expr1` as a regular expression to match the items, return the index of the first matched item, return 0 if there's no match.  `expr3` cannot be NULL or empty string, if not provided, the default is `,`.

**Return Type**: BIGINT. If `expr1` or `expr2` is NULL, then return NULL.

**Applicable Data Types**: VARCHAR, NCHAR. The function requires a minimum of 2 parameters and a maximum of 3 parameters.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

### RTRIM

```sql
RTRIM(expr)
```

**Function Classification**: Scalar function.

**Function Description**: Returns the string after removing right-side spaces.

**Return Result Type**: Same as the original type of the input field.

**Applicable Data Types**: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

### TRIM

```sql
TRIM([{LEADING | TRAILING | BOTH} [remstr] FROM] expr)
TRIM([remstr FROM] expr)
```

**Function Classification**: Scalar function.

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

### SUBSTRING/SUBSTR

```sql
SUBSTRING/SUBSTR(expr, pos [, len])
SUBSTRING/SUBSTR(expr FROM pos [FOR len])
```

**Function Classification**: Scalar function.

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

### SUBSTRING_INDEX

```sql
SUBSTRING_INDEX(expr, delim, count)
```

**Function Classification**: Scalar function.

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

### UPPER

```sql
UPPER(expr)
```

**Function Classification**: Scalar function.

**Function Description**: Converts the string argument value to all uppercase letters.

**Return Result Type**: Same as the original type of the input field.

**Applicable Data Types**: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

### CHAR

```sql
CHAR(expr1 [, expr2] [, expr3] ...)
```

**Function Classification**: Scalar function.

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

### ASCII

```sql
ASCII(expr)
```

**Function Classification**: Scalar function.

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

### POSITION

```sql
POSITION(expr1 IN expr2)
```

**Function Classification**: Scalar function.

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

### REPLACE

```sql
REPLACE(expr, from_str, to_str)
```

**Function Classification**: Scalar function.

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

### REPEAT

```sql
REPEAT(expr, count)
```

**Function Classification**: Scalar function.

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

## Data Security and Codec Functions

### Encoding Functions

#### TO_BASE64

```sql
TO_BASE64(expr)
```

**Function Classification**: Scalar function.

**Function Description**: Returns the base64 encoding of `expr`. For non-string types, the value is first converted to its string representation before encoding.

**Return Type**: VARCHAR.

**Applicable Data Types**:

- `expr`: BOOL, numeric types, TIMESTAMP, VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- If `expr` is NULL, returns NULL.
- If the base64-encoded result exceeds the maximum VARCHAR length, an error is returned.
- BOOL type: TRUE is encoded as the string `'1'`, FALSE as `'0'`.
- TIMESTAMP type: always formatted in UTC as `yyyy-mm-dd hh24:mi:ss.{precision}+00` (where precision is ms/us/ns depending on the column) before encoding. This ensures results are timezone-independent and preserve full precision.

**Example**:

```sql
taos> select to_base64(NULL);
 to_base64(null) |
==================
 NULL            |

taos> select to_base64("");
 to_base64("") |
================
               |

taos> select to_base64(14324);
 to_base64(14324) |
====================
 MTQzMjQ=         |

taos> select to_base64("14324");
 to_base64("14324") |
====================
 MTQzMjQ=         |

taos> select to_base64("Hello, world!");
 to_base64("Hello, world!") |
=============================
 SGVsbG8sIHdvcmxkIQ==       |

taos> select to_base64("你好 世界");
 to_base64("你好 世界")      |
==============================
 5L2g5aW9IOS4lueVjA==        |
```

#### FROM_BASE64

```sql
FROM_BASE64(expr)
```

**Function Classification**: Scalar function.

**Function Description**: Decode the base64 encoded string `expr`.

**Return Type**: VARCHAR.

**Applicable Data Types**:

- `expr`: VARCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- If `expr` is NULL, returns NULL.

**Example**:

```sql
taos> select from_base64("SGVsbG8sIHdvcmxkIQ==");
 from_base64("SGVsbG8sIHdvcmxkIQ==") |
======================================
 Hello, world!                       |
Query OK, 1 row(s) in set (0.000786s)
```

### Hashing Functions

#### MD5

```sql
MD5(expr)
```

**Function Classification**: Scalar function.

**Function Description**: Calculates an MD5 128-bit checksum for the string `expr`.

**Return Type**: VARCHAR.

**Applicable Data Types**:

- `expr`: VARCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- If `expr` is NULL, returns NULL.

**Example**:

```sql
taos> select md5('mytext')\G;
*************************** 1.row ***************************
md5('mytext'): 947ef8c8db156a568d5974d71f7638f4
Query OK, 1 row(s) in set (0.000522s)

taos> insert into db.tb values(now, md5('mytext'));
Insert OK, 1 row(s) affected (0.005111s)
```

#### SHA1 / SHA

```sql
SHA1(expr)
```

**Function Classification**: Scalar function.

**Function Description**: Calculates an SHA-1 160-bit checksum for the string `expr`, as described in RFC 3174 (Secure Hash Algorithm).

**Return Type**: VARCHAR.

**Applicable Data Types**:

- `expr`: VARCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- If `expr` is NULL, returns NULL.
- SHA() is synonymous with SHA1()

**Example**:

```sql
taos> select sha('mytext')\G;
*************************** 1.row ***************************
sha('mytext'): 65d922aad93c7e165ed888a2ab85befe9841fd39
Query OK, 1 row(s) in set (0.000658s)
```

#### SHA2

```sql
SHA2(expr, hash_length)
```

**Function Classification**: Scalar function.

**Function Description**: Calculates the SHA-2 family of hash functions (SHA-224, SHA-256, SHA-384, and SHA-512).

**Return Type**: VARCHAR.

**Applicable Data Types**:

- `expr`: VARCHAR.
- `hash_length`: 224, 256, 384, 512

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- If `expr` is NULL, returns NULL.

**Example**:

```sql
taos> select sha2('mytext', 224)\G;
*************************** 1.row ***************************
sha2('mytext', 224): 576e8f2cf59ebc59dd7659c48916f162ae0cf35937563999d5a7800e
Query OK, 1 row(s) in set (0.000569s)
```

#### CRC32

```sql
CRC32(expr)
```

**Function Classification**: Scalar function.

**Function Description**: Returns the unsigned 32-bit integer that represents the Cyclic Redundancy Check (CRC).

**Return Type**: INT UNSIGNED.

**Applicable Data Types**: Suitable for any type.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- If `expr` is NULL, it returns NULL.
- If `expr` is the empty string, it returns 0.
- If `expr` is a non string, it is interpreted as a string.
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

### Data Masking Functions

TDengine supports two approaches to data masking, each suited to different use cases:

| Approach | Description | Typical Usage |
|----------|-------------|---------------|
| **Masking functions** (this section) | Explicitly called by the user in a SQL query to transform a given expression before returning results. Available to any user with query privileges; masking logic is determined by the query itself. | `SELECT MASK_FULL(phone, '*') FROM t;` |
| **Grant-based column masking** (`GRANT MASK(col)`) | An administrator binds a masking policy to a column via `GRANT`. The masking is applied transparently for the specified user — the system automatically replaces the real value with `'*'` without requiring the user to modify their queries. **Enterprise Edition only.** | `GRANT SELECT (MASK(phone)) ON db.t TO user1;` |

For detailed syntax and behavior of grant-based column masking, see [GRANT — Column Permissions](../07-user-and-privilege/02-grant.md#column-permissions).

#### MASK_FULL

```sql
MASK_FULL(str, replace_value)
```

**Function Classification**: Scalar function.

**Function Description**: Mask the string `str` fully with the string `replace_value`.

**Return Type**: VARCHAR.

**Applicable Data Types**:

- `str`: VARCHAR.
- `replace_value`: string.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Example**:

```sql
taos> SELECT MASK_FULL('mytext', 'CONFIDENTIAL');
 mask_full('mytext', 'CONFIDENTIAL') |
======================================
 CONFIDENTIAL                        |
Query OK, 1 row(s) in set (0.002790s)
```

#### MASK_PARTIAL

```sql
MASK_PARTIAL(str, prefix_length, suffix_length, mask_char)
```

**Function Classification**: Scalar function.

**Function Description**: Mask the string `str` partially with the character `mask_char`.

**Return Type**: VARCHAR.

**Applicable Data Types**:

- `str`: VARCHAR.
- `prefix_length`: The number of characters to mask from the beginning of the string.
- `suffix_length`: The number of characters to mask from the end of the string.
- `mask_char`: The masking character.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Example**:

```sql
taos> SELECT MASK_partial('mytext', 1, 2, '*');
 mask_partial('mytext', 1, 2, '*') |
====================================
 *yte**                            |
Query OK, 1 row(s) in set (0.002787s)
```

#### MASK_NONE

```sql
MASK_NONE(str)
```

**Function Classification**: Scalar function.

**Function Description**: Null masking for testing only.

**Return Type**: VARCHAR.

**Applicable Data Types**:

- `str`: VARCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Example**:

```sql
taos> SELECT MASK_NONE('mytext');
 mask_none('mytext') |
======================
 mytext              |
Query OK, 1 row(s) in set (0.001474s)
```

### Encryption Functions

#### SM4_ENCRYPT

```sql
SM4_ENCRYPT(str, key_str)
```

**Function Classification**: Scalar function.

**Function Description**: Encrypts the string `str` using the key string `key_str`, and returns the encrypted output with SM4.

**Return Type**: VARCHAR.

**Applicable Data Types**:

- `str`: VARCHAR.
- `key_str`: The key string.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- Can be used in both select and inserting clauses.
- Only supported by the Enterprise edition.

**Example**:

```sql
taos> SELECT sm4_decrypt(sm4_encrypt('mytext', 'mykeystring'), 'mykeystring');
 sm4_decrypt(sm4_encrypt('mytext', 'mykeystring'), 'mykeystring') |
===================================================================
 mytext                                                           |
Query OK, 1 row(s) in set (0.003432s)
```

#### SM4_DECRYPT

```sql
SM4_DECRYPT(str, key_str)
```

**Function Classification**: Scalar function.

**Function Description**: Decrypts the string `str` using the key string `key_str`, and returns the decrypted output with SM4.

**Return Type**: VARCHAR.

**Applicable Data Types**:

- `expr`: VARCHAR.
- `key_str`: The key string.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- Can be used in both select and inserting clauses.
- Only supported by the Enterprise edition.

**Example**:

See `sm4_encrypt`.

#### AES_ENCRYPT

```sql
AES_ENCRYPT(str, key_str[, init_vector])
```

**Function Classification**: Scalar function.

**Function Description**: Encrypts the string `str` using the key string `key_str`, and returns the encrypted output with AES-128-CBC or AES-128-ECB.

**Return Type**: VARCHAR.

**Applicable Data Types**:

- `str`: VARCHAR.
- `key_str`: The key string.
- `init_vector`: The initialization vector.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- Can be used in both select and inserting clauses.

**Example**:

```sql
taos> SELECT aes_decrypt(aes_encrypt('mytext', 'mykeystring'), 'mykeystring');
 aes_decrypt(aes_encrypt('mytext', 'mykeystring'), 'mykeystring') |
===================================================================
 mytext                                                           |
Query OK, 1 row(s) in set (0.000514s)
```

#### AES_DECRYPT

```sql
AES_DECRYPT(str, key_str[, init_vector])
```

**Function Classification**: Scalar function.

**Function Description**: Decrypts the string `str` using the key string `key_str`, and returns the decrypted output with AES-128-CBC or AES-128-ECB.

**Return Type**: VARCHAR.

**Applicable Data Types**:

- `str`: VARCHAR.
- `key_str`: The key string.
- `init_vector`: The initialization vector.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- Can be used in both select and inserting clauses.

**Example**:

See `aes_encrypt`.

## Conversion Functions

Conversion functions convert values from one data type to another.

### CAST

```sql
CAST(expr AS type_name)
```

**Function Classification**: Scalar function.

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

### TO_ISO8601

```sql
TO_ISO8601(expr [, timezone])
```

**Function Classification**: Scalar function.

**Function Description**: Converts a timestamp into the ISO8601 standard date and time format, with timezone information. The optional `timezone` parameter allows users to specify the output timezone. If omitted, it uses the current connection timezone first; if not set, it uses the client timezone; if still unavailable, it falls back to the system default timezone.

**Return Data Type**: VARCHAR type.

**Applicable Data Types**: INTEGER, TIMESTAMP.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- The `timezone` parameter accepts the formats described in [Supported Timezone Formats](../10-time/01-timezone.md#supported-timezone-formats). **Only `TO_ISO8601` interprets offsets in ISO 8601 convention** (`local = UTC + offset`, i.e. `'+08:00'` = east-8 = Beijing time); `'+0800'`, `'UTC+8'`, `'UTC+0800'`, and `'UTC+08:00'` all behave identically. See [ISO 8601 sign convention](../10-time/01-timezone.md#to_iso8601).
- If `timezone` is omitted, it uses the current connection timezone.
- For IANA timezone input, the output offset is DST-aware for the target timestamp.
- The precision of the input timestamp is determined by the precision of the table queried, if no table is specified, the precision is milliseconds.

### TO_JSON

```sql
TO_JSON(str_literal)
```

**Function Classification**: Scalar function.

**Function Description**: Converts a string literal to JSON type.

**Return Data Type**: JSON.

**Applicable Data Types**: JSON strings, in the form '\{ "literal" : literal }'. '\{}' represents a null value. Keys must be string literals, and values can be numeric literals, string literals, boolean literals, or null literals. Escape characters are not supported in str_literal.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

### TO_UNIXTIMESTAMP

```sql
TO_UNIXTIMESTAMP(expr [, return_timestamp])

return_timestamp: {
    0
  | 1
}
```

**Function Classification**: Scalar function.

**Function Description**: Converts a datetime format string into a timestamp.

**Return Data Type**: BIGINT, TIMESTAMP.

**Applicable Fields**: VARCHAR, NCHAR.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- The input datetime string must conform to the ISO8601/RFC3339 standards, and formats that cannot be converted will return NULL.
- The precision of the returned timestamp is consistent with the time precision setting of the current DATABASE.
- return_timestamp specifies whether the function's return value is of TIMESTAMP type; setting it to 1 returns TIMESTAMP type, setting it to 0 returns BIGINT type. If not specified, it defaults to BIGINT type.
- An input string without a timezone is parsed in the current connection timezone. If a non-connection timezone is needed, timezone information can be carried in the input datetime string, in which case the timezone it carries takes precedence.

### TO_CHAR

```sql
TO_CHAR(ts, format_str_literal [, timezone])
```

**Function Classification**: Scalar function.

**Function Description**: Converts a timestamp type to a string according to the specified format.

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
| TZ,tz               | Time zone hours and minutes               | 2023-01-30 11:59:59PM +08:00 |

**Usage Instructions**:

- The output format for `Month`, `Day`, etc., is left-aligned with spaces added to the right, such as `2023-OCTOBER  -01`, `2023-SEPTEMBER-01`. September has the longest number of letters among the months, so there is no space for September. Weeks are similar.
- When using `ms`, `us`, `ns`, the output of the above three formats only differs in precision, for example, if ts is `1697182085123`, the output for `ms` is `123`, for `us` is `123000`, and for `ns` is `123000000`.
- Content in the time format that does not match the rules will be output directly. If you want to specify parts of the format string that can match rules not to be converted, you can use double quotes, like `to_char(ts, 'yyyy-mm-dd "is formatted by yyyy-mm-dd"')`. If you want to output double quotes, then add a backslash before the double quotes, like `to_char(ts, '\"yyyy-mm-dd\"')` will output `"2023-10-10"`.
- Formats that output numbers, such as `YYYY`, `DD`, uppercase and lowercase have the same meaning, i.e., `yyyy` and `YYYY` are interchangeable.
- If `timezone` is provided, the accepted formats are described in [Supported Timezone Formats](../10-time/01-timezone.md#supported-timezone-formats).
- If `timezone` is omitted, it uses the current connection timezone.
- For IANA timezone input, the output offset is DST-aware for the target timestamp.
- The precision of the input timestamp is determined by the precision of the table queried; if no table is specified, then the precision is milliseconds.

### TO_TIMESTAMP

```sql
TO_TIMESTAMP(ts_str_literal, format_str_literal)
```

**Function Classification**: Scalar function.

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
- If no timezone is specified, the connection-level timezone is used. If a non-connection timezone is needed, timezone information can be carried in the input datetime string, for example, '2023-10-10 10:10:10+08'.
- If a complete time is not specified, then the default time value is `1970-01-01 00:00:00` in the specified or default timezone, and the unspecified parts use the corresponding parts of this default value. Formats that only specify the year and day without specifying the month and day, like 'yyyy-mm-DDD', are not supported, but 'yyyy-mm-DD' is supported.
- If the format string contains `AM`, `PM`, etc., then the hour must be in 12-hour format, ranging from 01-12.
- `to_timestamp` conversion has a certain tolerance mechanism; even when the format string and timestamp string do not completely correspond, conversion is sometimes possible, like: `to_timestamp('200101/2', 'yyyyMM1/dd')`, the extra 1 in the format string will be discarded. Extra whitespace characters (spaces, tabs, etc.) in the format string and timestamp string will also be automatically ignored. Although fields like `MM` require two digits (with a leading zero if only one digit), in `to_timestamp`, a single digit can also be successfully converted.
- The precision of the output timestamp is the same as the precision of the queried table; if no table is specified, then the output precision is milliseconds. For example, `select to_timestamp('2023-08-1 10:10:10.123456789', 'yyyy-mm-dd hh:mi:ss.ns')` will truncate microseconds and nanoseconds. If a nanosecond table is specified, truncation will not occur, like `select to_timestamp('2023-08-1 10:10:10.123456789', 'yyyy-mm-dd hh:mi:ss.ns') from db_ns.table_ns limit 1`.

## Time and Date Functions

Time and date functions operate on timestamp types.

All functions that return the current time, such as NOW, TODAY, and TIMEZONE, are calculated only once in a SQL statement, no matter how many times they appear.

### NOW

```sql
NOW()
```

**Function Classification**: Scalar function.

**Function Description**: Returns the current system time of the client.

**Return Result Data Type**: TIMESTAMP.

**Applicable Fields**: When used in WHERE or INSERT statements, it can only be applied to fields of TIMESTAMP type.

**Applicable to**: Tables and supertables.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Usage Instructions**:

- Supports time addition and subtraction operations, such as NOW() + 1s. Supported time units are listed in [Time Units](../01-datatype.md#time-units) (milliseconds through weeks only).
- The precision of the returned timestamp is consistent with the time precision set in the current DATABASE.
- `NOW()` and `NOW` both follow the current connection timezone set by `SET TIMEZONE`.
- When using fixed-offset values with `SET TIMEZONE`, the sign is counterintuitive: `SET TIMEZONE '+08:00'` displays time 8 hours **behind** UTC, not ahead. Use `SET TIMEZONE 'Asia/Shanghai'` to get Beijing time reliably.
- To verify which timezone a connection is using, run `SELECT TIMEZONE()`. To see the current time with timezone offset, run `SELECT TO_ISO8601(NOW())`.

### TIMEDIFF

```sql
TIMEDIFF(expr1, expr2 [, time_unit])
```

**Function Classification**: Scalar function.

**Function Description**: Returns the result of the timestamp `expr1` - `expr2`, which may be negative, and approximated to the time unit specified by the `time_unit`.

**Return Result Type**: BIGINT.

**Applicable Data Types**:

- `expr1`: BIGINT, TIMESTAMP types representing timestamps, or VARCHAR, NCHAR types in ISO8601/RFC3339 standard date-time format.
- `expr2`: BIGINT, TIMESTAMP types representing timestamps, or VARCHAR, NCHAR types in ISO8601/RFC3339 standard date-time format.
- `time_unit`: See usage instructions.
- `timediff` returns the absolute value of the difference between timestamp `expr1` and `expr2` before ver-3.3.3.0.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- Returns NULL if `expr1` or `expr2` is NULL.
- Returns NULL if the input contains strings that do not conform to any date-time format.
- The precision of the input timestamp is determined by the precision of the table being queried; if no table is specified, the precision is milliseconds.
- The time unit of the returned value is specified by the `time_unit` parameter, with the minimum being the time resolution of the database. If the `time_unit` parameter is not specified, the time resolution of the database is used as the time unit. Supported time units are listed in [Time Units](../01-datatype.md#time-units).
- If `time_unit` is NULL, it is equivalent to the time unit not being specified.

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

### TIMETRUNCATE

```sql
TIMETRUNCATE(expr, time_unit [, timezone_or_flag])
```

**Function Classification**: Scalar function.

**Function Description**: Truncates the timestamp according to the specified time unit `time_unit`.

**Return Result Data Type**: TIMESTAMP.

**Applicable Fields**: BIGINT, TIMESTAMP types representing timestamps, or VARCHAR, NCHAR types in ISO8601/RFC3339 standard date-time format.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- Supported time units are listed in [Time Units](../01-datatype.md#time-units). For natural calendar truncation, `1n`, `1q`, and `1y` are supported.
- The precision of the returned timestamp is consistent with the time precision set in the current DATABASE.
- The precision of the input timestamp is determined by the precision of the table being queried; if no table is specified, the precision is milliseconds.
- Returns NULL if the input contains strings that do not conform to the date-time format.
- The third parameter supports both integer flags and timezone strings.
  - Integer `0`: truncates on fixed boundaries on the UTC timeline. For example, `1d` aligns to UTC `00:00`, and `1w` aligns to UTC week boundaries. The returned timestamp is still displayed in the current connection timezone.
  - Integer `1`: truncates on local calendar boundaries in the current connection timezone.
    - String timezone: accepts the formats described in [Supported Timezone Formats](../10-time/01-timezone.md#supported-timezone-formats).
- When the third parameter is omitted, it uses the current connection timezone.
- For `1w`, week alignment uses `firstDayOfWeek`. For `firstDayOfWeek` initialization and platform differences, see [firstDayOfWeek](../../12-operations-and-tooling/03-components/02-taosc.md#region-related).
- `GMT` / `GMT±...` and ambiguous abbreviations (for example `CST`) are rejected.

### TIMEZONE

```sql
TIMEZONE()
```

**Function Classification**: Scalar function.

**Function Description**: Returns the single effective timezone string for the current connection. It prefers the connection-level setting; if none is set, it falls back to the client-global timezone snapshotted when the connection was created, and then to the system default timezone.

**Return Data Type**: VARCHAR.

**Applicable Fields**: None

**Applicable to**: Tables and supertables.

### TODAY

```sql
TODAY()
```

**Function Classification**: Scalar function.

**Function Description**: Returns the system time at midnight of the current day for the client.

**Return Data Type**: TIMESTAMP.

**Applicable Fields**: Can only be used with TIMESTAMP type fields when used in WHERE or INSERT statements.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- Supports time addition and subtraction operations, such as TODAY() + 1s. Supported time units are listed in [Time Units](../01-datatype.md#time-units) (milliseconds through weeks only).
- The precision of the returned timestamp is consistent with the time precision set for the current DATABASE.
- Timezone resolution uses the current connection timezone first; if not set, it uses the client timezone; if still unavailable, it falls back to the system default timezone.

### WEEK

```sql
WEEK(expr [, mode])
```

**Function Classification**: Scalar function.

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
- For a BIGINT or TIMESTAMP input representing a fixed instant, the local calendar date is determined in the current connection timezone before computing; for a string input, a carried timezone only determines the corresponding absolute instant, while a timezone-less string is parsed in the connection timezone and the result is timezone-independent. Regardless of whether the input carries a timezone, the local calendar date is always determined in the connection-level timezone before computing.

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

### WEEKOFYEAR

```sql
WEEKOFYEAR(expr)
```

**Function Classification**: Scalar function.

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
- For a BIGINT or TIMESTAMP input representing a fixed instant, the local calendar date is determined in the current connection timezone before computing; for a string input, a carried timezone only determines the corresponding absolute instant, while a timezone-less string is parsed in the connection timezone and the result is timezone-independent. Regardless of whether the input carries a timezone, the local calendar date is always determined in the connection-level timezone before computing.

**Example**:

```sql
taos> select weekofyear('2000-01-01');
 weekofyear('2000-01-01') |
===========================
                       52 |
```

### WEEKDAY

```sql
WEEKDAY(expr)
```

**Function Classification**: Scalar function.

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
- For a BIGINT or TIMESTAMP input representing a fixed instant, the local calendar date is determined in the current connection timezone before computing; for a string input, a carried timezone only determines the corresponding absolute instant, while a timezone-less string is parsed in the connection timezone and the result is timezone-independent. Regardless of whether the input carries a timezone, the local calendar date is always determined in the connection-level timezone before computing.

**Example**:

```sql
taos> select weekday('2000-01-01');
 weekday('2000-01-01') |
========================
                     5 |
```

### DAYOFWEEK

```sql
DAYOFWEEK(expr)
```

**Function Classification**: Scalar function.

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
- For a BIGINT or TIMESTAMP input representing a fixed instant, the local calendar date is determined in the current connection timezone before computing; for a string input, a carried timezone only determines the corresponding absolute instant, while a timezone-less string is parsed in the connection timezone and the result is timezone-independent. Regardless of whether the input carries a timezone, the local calendar date is always determined in the connection-level timezone before computing.

**Example**:

```sql
taos> select dayofweek('2000-01-01');
 dayofweek('2000-01-01') |
==========================
                       7 |
```

### DATE

```sql
DATE(expr)
```

**Function Classification**: Scalar function.

**Function Description**: Returns date of the input time expression.

**Version**: ver-3.3.8.0

**Return Type**: VARCHAR.

**Applicable Data Types**: BIGINT, TIMESTAMP types representing timestamps, or VARCHAR, NCHAR types in ISO8601/RFC3339 date-time format.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage**:

- The return value is of `yyyy-mm-dd` format.
- If `expr` is NULL, returns NULL.
- If `expr` is of type VARCHAR or NCHAR but does not conform to the ISO8601/RFC3339 standard, returns NULL.
- The precision of the input timestamp is determined by the precision of the table queried; if no table is specified, the precision is milliseconds.
- If the input string carries a timezone, that timezone determines the corresponding absolute instant; otherwise it is parsed in the connection-level timezone. Regardless of whether the input carries a timezone, the returned date is always computed using the connection-level timezone.

**Example**:

(note: the following statements are executed in the UTC+0800 timezone, and the precision is milliseconds)

```sql
taos> select date(946656000000);
       date(946656000000)       |
=================================
 2000-01-01                     |

taos> select date('2000-01-01 12:00:00.000');
 date('2000-01-01 12:00:00.000') |
==================================
 2000-01-01                      |
```

## Statistical Aggregate Functions

Aggregate functions return a single result row for each group of the result set of a query. Groups can be specified by a GROUP BY or window partition clause; if none is specified, the entire result set is considered a single group.

TDengine supports aggregate queries on data. The following aggregate functions are provided.

### DISTINCT Aggregation

Aggregate functions support the `DISTINCT` keyword to deduplicate values before performing the aggregation. Syntax:

```sql
AGG_FUNC(DISTINCT expr)
```

**Supported aggregate functions**: `COUNT`, `SUM`, `AVG`, `MIN`, `MAX`.

**Applicable data types**: `expr` can be a column or expression; data type must satisfy the requirements of the corresponding aggregate function.

**Applicable to**: Tables and supertables.

**Usage notes**:

- `COUNT(DISTINCT expr)` returns the number of distinct non-NULL values.
- `SUM(DISTINCT expr)` computes the sum of distinct values.
- Can be combined with `GROUP BY`, `PARTITION BY`, and window clauses.
- Supports `INTERVAL` windows — deduplication is performed independently within each time window.
- A single query may mix DISTINCT and non-DISTINCT aggregates, for example:

  ```sql
  SELECT COUNT(DISTINCT voltage), AVG(current) FROM meters;
  ```

- When the deduplicated data exceeds the memory threshold (`pqSortMemThreshold`, default 16 MB), the engine automatically switches to a sort-based deduplication mode using disk-backed buffers. No user intervention is required.

**Limitations**:

- Only `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` support DISTINCT; using it with other functions returns error code `0x26B4` ("Function does not support DISTINCT").

**Examples**:

```sql
-- Count distinct voltage values
SELECT COUNT(DISTINCT voltage) FROM meters;

-- Distinct count per 10-minute window
SELECT _wstart, COUNT(DISTINCT voltage) FROM meters INTERVAL(10m);

-- Sum of distinct voltages grouped by location
SELECT location, SUM(DISTINCT voltage) FROM meters GROUP BY location;
```

### Basic Numeric Aggregation

#### AVG

```sql
AVG(expr)
```

**Function Classification**: Aggregate function.

**Function Description**: Calculates the average value of the specified field.

**Return Data Type**: DOUBLE, DECIMAL.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

**Description**: When the input type is DECIMAL, the output type is also DECIMAL. The precision and scale of the output conform to the rules described in the data type section. The result type is obtained by dividing the SUM type by UINT64. If the SUM result causes a DECIMAL type overflow, a DECIMAL OVERFLOW error is reported.

#### COUNT

```sql
COUNT({* | expr})
```

**Function Classification**: Aggregate function.

**Function Description**: Counts the number of record rows for the specified field.

**Return Data Type**: BIGINT.

**Applicable Data Types**: All field types.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- An asterisk (*) can be used to replace a specific field, using an asterisk (*) returns the total number of records.
- If the counting field is a specific column, it returns the number of non-NULL value records in that column.

#### SUM

```sql
SUM(expr)
```

**Function Classification**: Aggregate function.

**Function Description**: Calculates the sum of a column in a table/supertable.

**Return Data Type**: DOUBLE, BIGINT,DECIMAL.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

**Description**: When the input type is DECIMAL, the output type is DECIMAL(38, scale), where precision is the maximum value currently supported, and scale is the scale of the input type. If the SUM result overflows, a DECIMAL OVERFLOW error is reported.

#### LEASTSQUARES

```sql
LEASTSQUARES(expr, start_val, step_val)
```

**Function Classification**: Aggregate function.

**Function Description**: Calculates the linear equation of the values of a column in the table. start_val is the initial value of the independent variable, step_val is the step value of the independent variable.

**Return Data Type**: String expression (slope, intercept).

**Applicable Data Types**: expr must be a numeric type.

**Applicable to**: Tables.

### Dispersion Measures

#### STDDEV/STDDEV_POP/STD

```sql
STDDEV/STDDEV_POP/STD(expr)
```

**Function Classification**: Aggregate function.

**Function Description**: Calculates the population standard deviation of a column in the table.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

**Description**:

- Function `STDDEV_POP` equals `STDDEV` and is supported from ver-3.3.3.0.
- Function `STD` equals `STDDEV` and is supported from ver-3.3.8.0.

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

#### STDDEV_SAMP

```sql
STDDEV_SAMP
```

**Function Classification**: Aggregate function.

**Function Description**: Calculates the sample standard deviation of a column in the table.

**Version**: ver-3.3.8.0

**Return Data Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

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

taos> select stddev_samp(id) from test_stddev;
      stddev_samp(id)       |
============================
         1.58113883008419   |
```

#### VARIANCE/VAR_POP

```sql
VARIANCE/VAR_POP(expr)
```

**Function Classification**: Aggregate function.

**Function Description**: Calculates the population variance of a column in a table.

**Version**: ver-3.3.3.0

**Return Data Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

**Description**:

- Function `VARIANCE` equals `VAR_POP` and is supported from ver-3.3.8.0.

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

#### VAR_SAMP

```sql
VAR_SAMP(expr)
```

**Function Classification**: Aggregate function.

**Function Description**: Calculates the sample variance of a column in a table.

**Version**: ver-3.3.8.0

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

taos> select var_samp(id) from test_var;
        var_samp(id)        |
============================
         2.500000000000000 |
```

#### SPREAD

```sql
SPREAD(expr)
```

**Function Classification**: Aggregate function.

**Function Description**: Calculates the difference between the maximum and minimum values of a column in the table.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: INTEGER, TIMESTAMP.

**Applicable to**: Tables and supertables.

### Percentile and Cardinality Estimation

#### APERCENTILE

```sql
APERCENTILE(expr, p [, algo_type])

algo_type: {
    "default"
  | "t-digest"
}
```

**Function Classification**: Aggregate function.

**Function Description**: Calculates the approximate percentile ranks of values in a specified column of a table/supertable, similar to the PERCENTILE function but returns an approximate result.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

**Description**:

- The range of p is [0,100], where 0 is equivalent to MIN and 100 is equivalent to MAX.
- algo_type can be "default" or "t-digest". When the input is "default", the function uses a histogram-based algorithm for calculation. When the input is "t-digest", it uses the t-digest algorithm to calculate the approximate percentile. If algo_type is not specified, the "default" algorithm is used.
- The approximate result of the "t-digest" algorithm is sensitive to the order of input data, and different input orders may result in slight discrepancies in supertable queries.

#### HYPERLOGLOG

```sql
HYPERLOGLOG(expr)
```

**Function Classification**: Aggregate function.

**Function Description**:

- Uses the hyperloglog algorithm to return the cardinality of a column. This algorithm significantly reduces memory usage with large data volumes, providing an estimated cardinality with a standard error of 0.81%.
- For smaller data volumes, this algorithm may not be very accurate. Alternatively, use `select count(data) from (select unique(col) as data from table)`.

**Return Result Type**: INTEGER.

**Applicable Data Types**: Any type.

**Applicable to**: Tables and supertables.

#### PERCENTILE

```sql
PERCENTILE(expr, p [, p1] ... )
```

**Function Classification**: Aggregate function.

**Function Description**: Calculates the percentile values for a column in a table.

**Return Data Type**: The function requires a minimum of 2 parameters and can accept up to 11 parameters. It can return up to 10 percentile values at once. When the number of parameters is 2, it returns one percentile as a DOUBLE. When the number of parameters is more than 2, it returns a VARCHAR type, formatted as a JSON array containing multiple return values.

**Applicable Fields**: Numeric types.

**Applicable to**: Tables.

**Usage Instructions**:

- The PERCENTILE function is not applicable to virtual table.
- *P* values range from 0≤*P*≤100, where P=0 is equivalent to MIN and P=100 is equivalent to MAX;
- When calculating multiple percentiles for the same column, it is recommended to use one PERCENTILE function with multiple parameters to significantly reduce the response time of the query.
  For example, using the query SELECT percentile(col, 90, 95, 99) FROM table performs better than SELECT percentile(col, 90), percentile(col, 95), percentile(col, 99) from table.

### String Aggregation

#### GROUP_CONCAT

```sql
GROUP_CONCAT(expr)
```

**Function Classification**: Aggregate function.

**Function Description**: Concatenate the non-null fields of a table.

**Version**: ver-3.3.8.0

**Return Data Type**: VARCHAR.

**Applicable Data Types**: String types.

**Applicable to**: Tables and supertables.

**Example**:

```sql
taos> select str1, str2 from test_var;
     id      |      id      |
=============================
          a1 |       b1     |
          a2 |       b2     |
          a3 |       b3     |

taos> select group_concat(str1, str2, ':') from test_var;
         group_concat(str1, str2, ':')   |
==========================================
         a1b1:a2b2:a3b3                  |
```

### Distribution Statistics

#### HISTOGRAM

```sql
HISTOGRAM(expr, bin_type, bin_description, normalized)
```

**Function Classification**: Set function.

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

## Comparison Functions

### IF

```sql
IF(expr1, expr2, expr3)
```

**Function Classification**: Scalar function.

**Function Description**: If expr1 is true, return expr2, otherwise return expr3.

**Return Data Type**: Depends on the contexts.

**Applicable Fields**: expressions.

**Usage Instructions**:

- Similar to the CASE expressions.

**Example**:

```sql
taos> SELECT IF(1>2,2,3);
      if(1>2,2,3)      |
========================
                     3 |
```

### IFNULL

```sql
IFNULL(expr1, expr2)
```

**Function Classification**: Scalar function.

**Function Description**: If expr1 is not null, return expr1, otherwise return expr2.

**Return Data Type**: Depends on the contexts.

**Applicable Fields**: expressions.

**Example**:

```sql
taos> SELECT IFNULL(1,0);
      ifnull(1,0)      |
========================
                    1 |
```

### NVL

**Function Classification**: Scalar function.

`NVL` is a synonym for [IFNULL](#ifnull).

### NULLIF

```sql
NULLIF(expr1, expr2)
```

**Function Classification**: Scalar function.

**Function Description**: If expr1  = expr2, return NULL, otherwise return expr1.

**Return Data Type**: Depends on the contexts.

**Applicable Fields**: expressions.

**Example**:

```sql
taos> SELECT NULLIF(1,1);
      nullif(1,1)      |
========================
 NULL                  |
```

### NVL2

```sql
NVL2(expr1, expr2, expr3)
```

**Function Classification**: Scalar function.

**Function Description**: If expr1 is not null, return expr2, otherwise return expr1.

**Return Data Type**: Depends on the contexts.

**Applicable Fields**: expressions.

**Example**:

```sql
taos> SELECT NVL2(NULL,1,2);
========================
                     2 |
```

## Selection Functions

Selection functions choose one or more rows from the query result set based on semantics. Users can specify the output of the ts column or other columns (including tbname and tag columns), making it easy to know which data row the selected values originate from.

### BOTTOM

```sql
BOTTOM(expr, k)
```

**Function Classification**: Pipeline set function.

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

**Function Classification**: Pipeline aggregate function.

**Function Description**: Calculates the first non-NULL value written in a column of a table/supertable.

**Return Data Type**: Same as the applied field.

**Applicable Data Types**: All fields.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- To return the first non-NULL value of each column (smallest timestamp), use FIRST(\*); when querying a supertable, and if multiResultFunctionStarReturnTags is set to 0 (default), FIRST(\*) only returns the normal columns of the supertable; if set to 1, it returns both the normal and tag columns of the supertable.
- If all values in a column in the result set are NULL, the return for that column is also NULL;
- If all columns in the result set are NULL, no results are returned.
- For tables with composite primary keys, if there are multiple entries with the smallest timestamp, only the data with the smallest composite primary key is returned.
- With a degraded timeline, NULL time rows are skipped during time-value comparison; the row with the smallest non-NULL time is returned. If all time values are NULL, an empty result is returned. When no TIMESTAMP column exists, the first row in input order is returned.

### LAST

```sql
LAST(expr)
```

**Function Classification**: Pipeline aggregate function.

**Function Description**: Returns the last non-NULL value written in a column of a table/supertable.

**Return Data Type**: Same as the applied field.

**Applicable Data Types**: All fields.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- To return the last (timestamp largest) non-NULL value of each column, you can use LAST(\*); when querying a supertable, and if multiResultFunctionStarReturnTags is set to 0 (default), LAST(\*) only returns the normal columns of the supertable; if set to 1, it returns both the normal and tag columns of the supertable.
- If all values in a column in the result set are NULL, the return result for that column is also NULL; if all columns in the result set are NULL, no result is returned.
- When used with supertables, if there are multiple rows with the same timestamp and it is the largest, one will be randomly returned, and it is not guaranteed that the same row will be selected in multiple runs.
- For tables with composite primary keys, if there are multiple records with the maximum timestamp, only the data with the largest corresponding composite primary key is returned.
- With a degraded timeline, NULL time rows are skipped; the row with the largest non-NULL time is returned. If all time values are NULL, an empty result is returned. When no TIMESTAMP column exists, the last row in input order is returned.

### LAST_ROW

```sql
LAST_ROW(expr)
```

**Function Classification**: Pipeline aggregate function.

**Function Description**: Returns the last record of a table/supertable.

**Return Data Type**: Same as the applied field.

**Applicable Data Types**: All fields.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- To return the last record (timestamp largest) of each column, you can use LAST_ROW(\*); when querying a supertable, and if multiResultFunctionStarReturnTags is set to 0 (default), LAST_ROW(\*) only returns the normal columns of the supertable; if set to 1, it returns both the normal and tag columns of the supertable.
- When used with supertables, if there are multiple rows with the same timestamp and it is the largest, one will be randomly returned, and it is not guaranteed that the same row will be selected in multiple runs.
- Similar to the LAST function, for tables with composite primary keys, if there are multiple records with the maximum timestamp, only the data with the largest corresponding composite primary key is returned.
- With a degraded timeline, NULL time rows are skipped; the row with the largest non-NULL time is returned. Behavior is consistent with LAST. When no TIMESTAMP column exists, the last row in input order is returned.

### MAX

```sql
MAX(expr)
```

**Function Classification**: Pipeline aggregate function.

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

**Function Classification**: Pipeline aggregate function.

**Function Description**: Calculates the minimum value of a column in a table/supertable.

**Return Data Type**: Same as the applied field.

**Applicable Data Types**: Numeric types, VARCHAR, NCHAR.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- The min function can accept strings as input parameters, and when the input parameter is a string type, it returns the smallest string value (supported from ver-3.3.3.0; before ver-3.3.3.0, `min` only accepted numeric parameters).

### MODE

```sql
MODE(expr)
```

**Function Classification**: Pipeline aggregate function.

**Function Description**: Returns the most frequently occurring value, if there are multiple values with the same highest frequency, it randomly outputs one of them.

**Return Data Type**: Consistent with the input data type.

**Applicable Data Types**: All field types.

**Applicable to**: Tables and supertables.

### SAMPLE

```sql
SAMPLE(expr, k)
```

**Function Classification**: Pipeline set function.

**Function Description**: Gets k sample values of the data. The valid input range for parameter k is 1 ≤ k ≤ 1000.

**Return Result Type**: Same as the original data type.

**Applicable Data Types**: All field types.

**Nested Subquery Support**: Applicable to both inner and outer queries.

**Applicable to**: Tables and supertables.

### TAIL

```sql
TAIL(expr, k [, offset_rows])
```

**Function Classification**: Pipeline set function.

**Function Description**: Returns the last k records after skipping the last offset_val records, not ignoring NULL values. offset_val can be omitted. In this case, it returns the last k records. When offset_val is provided, the function is equivalent to `order by ts desc LIMIT k OFFSET offset_val`.

**Parameter Range**: k: [1,100] offset_val: [0,100].

**Return Data Type**: Same as the applied field.

**Applicable Data Types**: Suitable for any type except the time primary key column.

**Applicable to**: Tables, supertables.

### TOP

```sql
TOP(expr, k)
```

**Function Classification**: Pipeline set function.

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

**Function Classification**: Pipeline set function.

**Function Description**: Returns the deduplicated values of the column. This function is similar to distinct. For the same data, it returns the one with the smallest timestamp. For queries on tables with composite primary keys, if there are multiple records with the smallest timestamp, only the data with the smallest composite primary key is returned.

**Return Data Type**: Same as the field of the application.

**Applicable Data Types**: All types of fields.

**Applicable to**: Tables and supertables.

### COLS

```sql
COLS (func(expr), output_expr1, [, output_expr2] ... )
```

**Function Classification**: Pipeline set function.

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

## Sequential Analysis Functions

Sequential analysis functions are tailor-made by TDengine to meet the query scenarios of time-series data. In general databases, implementing similar functionalities usually requires complex query syntax and is inefficient. TDengine has built these functionalities into functions, greatly reducing the user's cost of use.

### CSUM

```sql
CSUM(expr)
```

**Function Classification**: Pipeline set function.

**Function Description**: Cumulative sum, ignoring NULL values.

**Return Result Type**: If the input column is an integer type, the return value is long integer (int64_t), for floating-point numbers, the return value is double precision floating point (Double). For unsigned integer types, the return value is unsigned long integer (uint64_t).

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- Does not support +, -, *, / operations, such as csum(col1) + csum(col2).
- Can only be used with aggregation functions. This function can be applied to both basic tables and supertables.
- With a degraded timeline, csum does not depend on time values and computes by row order. Still reports errors for real (non-NULL) duplicate timestamps; NULL time rows do not trigger duplicate timestamp checks.

### DERIVATIVE

```sql
DERIVATIVE(expr, time_interval, ignore_negative)

ignore_negative: {
    0
  | 1
}
```

**Function Classification**: Pipeline set function.

**Function Description**: Calculates the rate of change per unit of a column in the table. The length of the unit time interval can be specified by the time_interval parameter, which can be as short as 1 second (1s); the value of the ignore_negative parameter can be 0 or 1, where 1 means to ignore negative values. For queries on tables with composite primary keys, if there are multiple records with the same timestamp, only the data with the smallest composite primary key is involved in the calculation.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- Can be used with the columns associated with the selection. For example: select _rowts, DERIVATIVE(col1, 1s, 1) from tb1.
- With a degraded timeline, duplicate timestamp rows output NULL (avoiding division by zero) without updating the previous value; NULL time rows also output NULL. Output row count is always N-1.
- With a disordered degraded timeline, derivative can still execute but time differences alternate between positive and negative, producing physically meaningless slope values. Use `ORDER BY` in the subquery to ensure ordering.

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

**Function Classification**: Pipeline set function.

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
- With a degraded timeline, duplicate timestamp rows output NULL without updating the previous value (the next non-duplicate row uses the last valid row for calculation); NULL time rows also output NULL. Output row count is always N-1.

### FILL_FORWARD

```sql
FILL_FORWARD(expr)
```

**Function Classification**: Pipeline set function.

**Function Description**: Replace the nulls with the previous nonnull value, and keep it null if all previous ones are null.

**Return Result Type**: Same as the input type.

**Applicable Data Types**: Numeric types, String types.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- Support +, -, *, / operations, such as fill_forward(col1) + fill_forward(col2);

**Example**:

```sql
taos> select _rowts,f1,f2,fill_forward(f1),fill_forward(f2),fill_forward(f1)*fill_forward(f2) from db.tb;
         _rowts          |     f1      |       f2       |   fill_forward(f1)   |   fill_forward(f2)     | fill_forward(f1)*fill_forward(f2) |
============================================================================================================================================
 2025-12-02 01:01:01.000 |           1 |       2.000000 |                    1 |               2.000000 |                          2.000000 |
 2025-12-02 01:01:02.000 |        NULL |       4.000000 |                    1 |               4.000000 |                          4.000000 |
 2025-12-02 01:01:03.000 |           5 |           NULL |                    5 |               4.000000 |                         20.000000 |
 2025-12-02 01:01:04.000 |           7 |       8.000000 |                    7 |               8.000000 |                         56.000000 |
Query OK, 4 row(s) in set (56.155269s)
```

### MAVG

```sql
MAVG(expr, k)
```

**Function Classification**: Pipeline set function.

**Function Description**: Calculates the moving average of consecutive k values. If the number of input rows is less than k, no result is output. The valid input range for parameter k is 1 ≤ k ≤ 1000.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Applicable to inner and outer queries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- Does not support +, -, *, / operations, such as mavg(col1, k1) + mavg(col2, k1);
- Can only be used with regular columns, selection, and projection functions, not with aggregation functions;
- When used with a window clause, `MAVG` is calculated only from samples inside the current window and does not continue state across windows.

### STATECOUNT

```sql
STATECOUNT(expr, oper, val)
```

**Function Classification**: Pipeline set function.

**Function Description**: Returns the number of consecutive records that meet a certain condition, with the result appended as a new column to each row. The condition is calculated based on the parameters, adding 1 if the condition is true, resetting to -1 if false, and skipping the data if it is NULL.

**Parameter Range**:

- oper: "LT" (less than), "GT" (greater than), "LE" (less than or equal to), "GE" (greater than or equal to), "NE" (not equal to), "EQ" (equal to), case insensitive.
- val: Numeric

**Return Result Type**: INTEGER.

**Applicable Data Types**: Numeric types.

**Nested Subquery Support**: Not applicable to subqueries.

**Applicable to**: Tables and supertables.

**Usage Notes**:

- When used with a window clause, `STATECOUNT` counts consecutive records only inside the current window and does not accumulate across windows.

### STATEDURATION

```sql
STATEDURATION(expr, oper, val, unit)
```

**Function Classification**: Pipeline set function.

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

- When used with a window clause, `STATEDURATION` measures continuous duration only inside the current window and does not accumulate across windows.
- With a degraded timeline, NULL time rows output -1 (do not participate in time difference calculation).
- With a disordered degraded timeline, stateduration can still execute but time differences may be negative, causing negative or jumping duration values. Use `ORDER BY` in the subquery to ensure ordering.

### LAG

```sql
LAG(expr, offset[, default_val])
```

**Function Classification**: Pipeline set function.

**Function Description**: Returns the value of `expr` from the row that is `offset` rows before the current row.

**Return Data Type**: Same as the data type of `expr`.

**Applicable Data Types**: All data types.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- `offset` must be an integer greater than 0.
- `default_val` is optional. It is returned when the target row does not exist; if omitted, `NULL` is returned.
- `default_val` must be type-compatible with `expr`.
- `LAG` is evaluated on the row order of the input result set; you can use `ORDER BY` to change the evaluation order.
- It can be used together with `_rowts`, `tbname`, tag columns, and also in subqueries and `PARTITION BY` scenarios.
- When used with a window clause, `LAG` is evaluated only within the current window in window-local row order and does not carry state across windows.
- `LAG` can also be used as a SQL standard window function together with an `OVER` clause, in which case the parameter rules differ slightly (`offset` may be 0). See [Window Functions](./09-window-function.md#value-window-functions).

### LEAD

```sql
LEAD(expr, offset[, default_val])
```

**Function Classification**: Pipeline set function.

**Function Description**: Returns the value of `expr` from the row that is `offset` rows after the current row.

**Return Data Type**: Same as the data type of `expr`.

**Applicable Data Types**: All data types.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- `offset` must be an integer greater than 0.
- `default_val` is optional. It is returned when the target row does not exist; if omitted, `NULL` is returned.
- `default_val` must be type-compatible with `expr`.
- `LEAD` is evaluated on the row order of the input result set; you can use `ORDER BY` to change the evaluation order.
- It can be used together with `_rowts`, `tbname`, tag columns, and also in subqueries and `PARTITION BY` scenarios.
- When used with a window clause, `LEAD` is evaluated only within the current window in window-local row order and does not read rows from the next window.
- `LEAD` can also be used as a SQL standard window function together with an `OVER` clause, in which case the parameter rules differ slightly (`offset` may be 0). See [Window Functions](./09-window-function.md#value-window-functions).

## Time-Series Special Aggregate Functions

Time-series special aggregate functions are aggregate functions specifically designed for time-series data scenarios in TDengine.

### ELAPSED

```sql
ELAPSED(ts_primary_key [, time_unit])
```

**Function Classification**: Aggregate function.

**Function Description**: The elapsed function expresses the continuous duration within the statistical period, and when used in conjunction with the twa function, it can calculate the area under the statistical curve. When specifying a window with the INTERVAL clause, it calculates the time range covered by data in each window within the given time range; if there is no INTERVAL clause, it returns the time range covered by data for the entire given time range. Note that ELAPSED does not return the absolute value of the time range, but the number of units obtained by dividing the absolute value by the time_unit. Stream computing only supports this function in FORCE_WINDOW_CLOSE mode.

**Return Result Type**: DOUBLE.

**Applicable Data Types**: TIMESTAMP.

**Applicable to**: Tables, supertables, outer queries of nested queries

**Notes**:

- The `ts_primary_key` parameter can be the primary key column or a regular TIMESTAMP column from a subquery output. When the subquery does not output a primary key column, elapsed can use the degraded timeline column (the first TIMESTAMP column in the output schema).
- elapsed can specify a TIMESTAMP column different from the current effective timeline. In this case, elapsed computes the time span using the specified column without affecting other functions' timeline selection in the same query.
- The time unit of the returned value is specified by the `time_unit` parameter, with the minimum being the time resolution of the database. If the `time_unit` parameter is not specified, the time resolution of the database is used as the time unit. Supported time units `time_unit` include: 1b (nanosecond), 1u (microsecond), 1a (millisecond), 1s (second), 1m (minute), 1h (hour), 1d (day), 1w (week).
- Can be used in combination with interval, returning the timestamp difference for each time window. It is important to note that, except for the first and last time windows, the timestamp differences for the middle windows are all the length of the window.
- order by asc/desc does not affect the calculation of the difference.
- For supertables, it needs to be used in combination with the group by tbname clause, and cannot be used directly.
- For regular tables, it is not supported in combination with the group by clause.
- For nested queries, elapsed can use any TIMESTAMP column output by the inner query. If no TIMESTAMP column is output, elapsed cannot be used.
- With a disordered degraded timeline, elapsed results depend on the time difference between the first and last input rows, which may not equal the actual time span. Use `ORDER BY` in the subquery to ensure ordering.
- Not supported in combination with leastsquares, diff, derivative, top, bottom, last_row, interp, and other functions.

### IRATE

```sql
IRATE(expr)
```

**Function Classification**: Aggregate function.

**Function Description**: Calculates the instantaneous growth rate. It uses the last two sample data points in the time interval to calculate the instantaneous growth rate; if these two values are decreasing, then only the last value is used for the calculation, rather than the difference between the two. For queries on tables with composite primary keys, if there are multiple data points with the same timestamp, only the data corresponding to the smallest composite primary key is used in the calculation.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- With a degraded timeline, duplicate timestamp points are skipped; if all timestamps are identical, returns 0. NULL time rows are skipped.
- irate internally selects the two points with the largest time values, so results remain meaningful even with disordered input.

### TWA

```sql
TWA(expr)
```

**Function Classification**: Aggregate function.

**Function Description**: Time-weighted average function. Calculates the time-weighted average of a column in a table over a period of time. For queries on tables with composite primary keys, if there are multiple data points with the same timestamp, only the data corresponding to the smallest composite primary key is used in the calculation. Stream computing supports this function only in FORCE_WINDOW_CLOSE mode.

**Return Data Type**: DOUBLE.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

**Usage Instructions**:

- With a degraded timeline, duplicate timestamp points are skipped (zero area contribution); NULL time rows are skipped for weighting calculation.
- With a disordered degraded timeline, twa can still execute but negative time differences produce negative weights, causing the weighted average to deviate from the true mean. Use `ORDER BY` in the subquery to ensure ordering.

## Interpolation Functions

Interpolation functions are used to return interpolated values at specified time slices.

### INTERP

```sql
INTERP(expr [, ignore_null_values])

ignore_null_values: {
    0
  | 1
}
```

**Function Classification**: Set function.

**Function Description**: Returns the record value or interpolated value of a specified column at a specified time slice. The ignore_null_values parameter can be 0 or 1, where 1 means to ignore NULL values, default is 0. When ignore_null_values is set to 1, other NULL value samples will be ignored during interpolation.

**Return Data Type**: Same as the field type.

**Applicable Data Types**: Numeric types.

**Applicable to**: Tables and supertables.

Usage Instructions:

- INTERP is used to obtain the record value of a specified column at the specified time slice. It has a dedicated syntax (interp_clause) when used. For syntax introduction, see [reference link](./01-query.md#interp).
- When there is no row data that meets the conditions at the specified time slice, the INTERP function will interpolate according to the settings of the [FILL clause](./01-query.md#fill-clause).
- When INTERP is applied to a supertable, it will sort all the subtable data under that supertable by primary key column and perform interpolation calculations, and can also be used with PARTITION BY tbname to force the results to a single timeline.
- When using INTERP with FILL PREV/NEXT/NEAR modes, its behavior differs from window queries: the `ignore_null_values` parameter affects the search for adjacent valid data. If the parameter is set to ignore NULL data, adjacent NULL data will not be used for interpolation. Instead, the search will continue until a non-NULL value is found.
- INTERP can be used with the pseudocolumn _irowts to return the timestamp corresponding to the interpolation point (supported from version 3.0.2.0).
- INTERP can be used with the pseudocolumn _isfilled to display whether the return result is from the original record or generated by the interpolation algorithm (supported from version 3.0.3.0).
- INTERP can only use the pseudocolumn `_irowts_origin` when using FILL PREV/NEXT/NEAR modes. `_irowts_origin` is supported from version 3.3.4.9.
- For queries on tables with composite primary keys, if there are data with the same timestamp, only the data with the smallest composite primary key participates in the calculation.
- With a degraded timeline, NULL time rows are skipped; interpolation uses non-NULL time rows only. With a disordered degraded timeline, interp can still execute but adjacent points have non-contiguous time values, making linear interpolation results physically meaningless. Use `ORDER BY` in the subquery to ensure ordering.

## System and Metadata Functions

### DATABASE

```sql
SELECT DATABASE();
```

**Function Classification**: Scalar function.

**Description**: Returns the currently logged-in database. If no default database was specified at login and the USE command has not been used to switch databases, it returns NULL.

### CLIENT_VERSION

```sql
SELECT CLIENT_VERSION();
```

**Function Classification**: Scalar function.

**Description**: Returns the client version.

### SERVER_VERSION

```sql
SELECT SERVER_VERSION();
```

**Function Classification**: Scalar function.

**Description**: Returns the server version.

### SERVER_STATUS

```sql
SELECT SERVER_STATUS();
```

**Function Classification**: Scalar function.

**Description**: Checks if all dnodes on the server are online; if so, it returns success, otherwise, it returns an error that the connection could not be established. To check the status of the cluster, it is recommended to use `SHOW CLUSTER ALIVE;`, which, unlike `SELECT SERVER_STATUS();`, does not return an error when some nodes in the cluster are unavailable, but instead returns different status codes, see: [SHOW CLUSTER ALIVE](../09-system-info/03-show.md#show-cluster-alive)

### CURRENT_USER

```sql
SELECT CURRENT_USER();
```

**Function Classification**: Scalar function.

**Description**: Retrieves the current user.

### SLEEP

```sql
SELECT SLEEP(seconds);
```

**Function Classification**: Scalar function.

**Description**: Pauses execution for the specified number of seconds. When used in a table query, `SLEEP` is evaluated once per row (MySQL-compatible); total wait time equals the sum of each row's duration.

**Parameters**:

- `seconds`: DOUBLE - Number of seconds to sleep (supports fractional values like 0.5); negative or NULL values skip the sleep and return 0

**Return value**: INT - Returns 0 on success or for negative/NULL arguments

**Examples**:

```sql
-- Sleep for 2 seconds
SELECT SLEEP(2);

-- Sleep for 500 milliseconds
SELECT SLEEP(0.5);

-- Negative argument returns 0 immediately
SELECT SLEEP(-1);

-- NULL argument returns 0 immediately
SELECT SLEEP(NULL);

-- Used with a table query: SLEEP is evaluated once per row (MySQL-compatible);
-- total wait time equals the sum of each row's duration
SELECT SLEEP(1), col1 FROM table1;
```

## Geometry Functions

### Geometry Input Functions

#### ST_GeomFromText

```sql
ST_GeomFromText(VARCHAR WKT expr)
```

**Function Classification**: Scalar function.

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

**Function Classification**: Scalar function.

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

**Function Classification**: Scalar function.

**Function Description**: Compares two geometry objects and returns true if they intersect.

**Return Type**: BOOL

**Applicable Data Types**: GEOMETRY, GEOMETRY

**Applicable Table Types**: Basic tables and supertables

**Usage Instructions**: If the two geometry objects share any point, they intersect.

#### ST_Equals

```sql
ST_Equals(GEOMETRY geomA, GEOMETRY geomB)
```

**Function Classification**: Scalar function.

**Function Description**: Returns TRUE if the given geometry objects are "spatially equal".

**Return Type**: BOOL

**Applicable Data Types**: GEOMETRY, GEOMETRY

**Applicable Table Types**: Basic tables and supertables

**Usage Instructions**: "Spatially equal" means that ST_Contains(A,B) = true and ST_Contains(B,A) = true, and the order of points may differ but represent the same geometric structure.

#### ST_Touches

```sql
ST_Touches(GEOMETRY geomA, GEOMETRY geomB)
```

**Function Classification**: Scalar function.

**Function Description**: Returns TRUE if A and B intersect, but their interiors do not intersect.

**Return Type**: BOOL

**Applicable Data Types**: GEOMETRY, GEOMETRY

**Applicable Table Types**: Basic tables and supertables

**Usage Instructions**: A and B have at least one common point, and these common points are located on at least one boundary. For point/point input, the relationship is always FALSE, because points have no boundaries.

#### ST_Covers

```sql
ST_Covers(GEOMETRY geomA, GEOMETRY geomB)
```

**Function Classification**: Scalar function.

**Function Description**: Returns TRUE if every point in B is inside the geometric shape A (intersecting with the interior or boundary).

**Return Type**: BOOL

**Applicable Data Types**: GEOMETRY, GEOMETRY

**Applicable Table Types**: Basic tables and supertables

**Usage Instructions**: A contains B means that no points in B are outside of A (on the outside).

#### ST_Contains

```sql
ST_Contains(GEOMETRY geomA, GEOMETRY geomB)
```

**Function Classification**: Scalar function.

**Function Description**: Returns TRUE if geometric shape A contains geometric shape B.

**Return Type**: BOOL

**Applicable Data Types**: GEOMETRY, GEOMETRY

**Applicable Table Types**: Basic tables and supertables

**Usage Instructions**: A contains B if and only if all points of B are inside A (i.e., located inside or on the boundary) (or equivalently, no points of B are outside A), and the interiors of A and B have at least one point in common.

#### ST_ContainsProperly

```sql
ST_ContainsProperly(GEOMETRY geomA, GEOMETRY geomB)
```

**Function Classification**: Scalar function.

**Function Description**: Returns TRUE if every point of B is inside A.

**Return Type**: BOOL

**Applicable Data Types**: GEOMETRY, GEOMETRY

**Applicable Table Types**: Basic tables and supertables

**Usage Instructions**: No points of B are on the boundary or outside of A.
