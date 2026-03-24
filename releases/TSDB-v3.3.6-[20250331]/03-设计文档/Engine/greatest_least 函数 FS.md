# greatest/least 函数 FS

## 1. 背景

[TS-5607](https://jira.taosdata.com:18080/browse/TS-5607) 

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025-03-04 | 1.0 | 任新胜 |  |

## 3. 定义

无

## 4. 行为说明

#### 4.0.1 greatest

```plaintext
GREATEST(expr1, expr2[, expr]...)
```

**功能说明**：获得输入的所有参数中的最大值。该函数最小参数个数为 2 个。
**返回结果类型**：参考比较规则，比较类型即为最终返回类型。
**适用数据类型**：
- 数值类型：包括 bool 型，整型和浮点型
- nchar 和 varchar 类型。
**比较规则：**以下规则描述了比较操作的转换方式：
- 如果有任何一个参数为 NULL，则比较结果为 NULL。
- 如果比较操作中的所有参数都是字符串类型，按照字符串类型比较
- 如果所有参数都是数值类型，则将它们作为数值类型进行比较。
- TIMESTAMP 类型也是数值类型，当和 TIMESTAMP 参与比较的类型都是整数类型时，按照 TIMESTAMP 进行比较；
- 如果参数中既有字符串类型，也有数值类型，根据 compareAsStrInGreatest 配置项，统一作为字符串或者数值进行比较。默认按照字符串比较。
- 在所有情况下，不同类型比较，比较类型会选择范围更大的类型进行比较，例如作为整数类型比较时，如果存在 BIGINT 类型，必定会选择 BIGINT 作为比较类型。
**相关配置项：**客户端配置，compareAsStrInGreatest 为 1 表示同时存在字符串类型和数值类型统一转为字符串比较，为 0 表示统一转为数值类型比较。默认为 1。

#### 4.0.2 least

```plaintext
LEAST(expr1, expr2[, expr]...)
```

**功能说明**：获得输入的所有参数中的最小值，其余部分同 `greatest` 函数。

## 5. 性能

无

## 6. 兼容性

无兼容性问题

## 7. 运维

无

## 8. 使用场景

多列比较求最大/最小值的场景

## 9. 约束和限制

greatest/least 支持数据类型为：
- 数值类型：包括 bool 型，整型和浮点型
- 支持 nchar 和 varchar 类型
- 不支持上述之外的其他类型

## 10. 常见错误和排查

无

## 11. 可观测性

无

## 12. 安装和卸载

无

## 13. 文档

官方文档操作符这里需要更新
Greatest 和 least 函数说明需要同步

## 14. 参考文档

### 14.1 [TDengine 支持 Mysql 函数](https://taosdata.feishu.cn/wiki/P8Y0w9S13icde2kFZj7c4DUQnIb)

https://docs.taosdata.com/reference/taos-sql/operators/#%E6%AF%94%E8%BE%83%E8%BF%90%E7%AE%97%E7%AC%A6

### 14.2 Mysql 行为参考

greatest 取最大值的比较规则和 ">" 操作比较符的规则不同，（字符串和数值比较，"129" 与 100 比较）结果相反，这里是否借鉴值得商榷。
https://dev.mysql.com/doc/refman/8.4/en/comparison-operators.html#function_least
```c
mysql> select greatest(name, age), name, age, name > age  from d1;
+---------------------+------+------+------------+
| greatest(name, age) | name | age  | name > age |
+---------------------+------+------+------------+
| 190                 | 190  |  180 |          1 |
| 29                  | 29   |   29 |          0 |
| 19                  | 19   |  100 |          0 |
+---------------------+------+------+------------+
3 rows in set (0.00 sec)
```

#### 14.2.1 Mysql greatest/least 规则说明

[`LEAST(`](https://dev.mysql.com/doc/refman/8.4/en/comparison-operators.html#function_least)[`***value1***`](https://dev.mysql.com/doc/refman/8.4/en/comparison-operators.html#function_least)[`,`](https://dev.mysql.com/doc/refman/8.4/en/comparison-operators.html#function_least)[`***value2***`](https://dev.mysql.com/doc/refman/8.4/en/comparison-operators.html#function_least)[`,...)`](https://dev.mysql.com/doc/refman/8.4/en/comparison-operators.html#function_least)
With two or more arguments, returns the smallest (minimum-valued) argument. The arguments are compared using the following rules:
- If any argument is `NULL`, the result is `NULL`. No comparison is needed.
- If all arguments are integer-valued, they are compared as integers.
- If at least one argument is double precision, they are compared as double-precision values. Otherwise, if at least one argument is a [`DECIMAL`](https://dev.mysql.com/doc/refman/8.4/en/fixed-point-types.html) value, they are compared as [`DECIMAL`](https://dev.mysql.com/doc/refman/8.4/en/fixed-point-types.html) values.
- If the arguments comprise a mix of numbers and strings, they are compared as strings.
- If any argument is a nonbinary (character) string, the arguments are compared as nonbinary strings.
- In all other cases, the arguments are compared as binary strings.
The return type of [`LEAST()`](https://dev.mysql.com/doc/refman/8.4/en/comparison-operators.html#function_least) is the aggregated type of the comparison argument types.

#### 14.2.2 比较操作符中的类型转换

https://dev.mysql.com/doc/refman/8.4/en/type-conversion.html
The following rules describe how conversion occurs for comparison operations:
- If one or both arguments are `NULL`, the result of the comparison is `NULL`, except for the `NULL`-safe [`<=>`](https://dev.mysql.com/doc/refman/8.4/en/comparison-operators.html#operator_equal-to) equality comparison operator. For `NULL <=> NULL`, the result is true. No conversion is needed.
- If both arguments in a comparison operation are strings, they are compared as strings.
- If both arguments are integers, they are compared as integers.
- Hexadecimal values are treated as binary strings if not compared to a number.
- If one of the arguments is a [`TIMESTAMP`](https://dev.mysql.com/doc/refman/8.4/en/datetime.html) or [`DATETIME`](https://dev.mysql.com/doc/refman/8.4/en/datetime.html) column and the other argument is a constant, the constant is converted to a timestamp before the comparison is performed. This is done to be more ODBC-friendly. This is not done for the arguments to [`IN()`](https://dev.mysql.com/doc/refman/8.4/en/comparison-operators.html#operator_in). To be safe, always use complete datetime, date, or time strings when doing comparisons. For example, to achieve best results when using [`BETWEEN`](https://dev.mysql.com/doc/refman/8.4/en/comparison-operators.html#operator_between) with date or time values, use [`CAST()`](https://dev.mysql.com/doc/refman/8.4/en/cast-functions.html#function_cast) to explicitly convert the values to the desired data type.
- A single-row subquery from a table or tables is not considered a constant. For example, if a subquery returns an integer to be compared to a [`DATETIME`](https://dev.mysql.com/doc/refman/8.4/en/datetime.html) value, the comparison is done as two integers. The integer is not converted to a temporal value. To compare the operands as [`DATETIME`](https://dev.mysql.com/doc/refman/8.4/en/datetime.html) values, use [`CAST()`](https://dev.mysql.com/doc/refman/8.4/en/cast-functions.html#function_cast) to explicitly convert the subquery value to [`DATETIME`](https://dev.mysql.com/doc/refman/8.4/en/datetime.html).
- If one of the arguments is a decimal value, comparison depends on the other argument. The arguments are compared as decimal values if the other argument is a decimal or integer value, or as floating-point values if the other argument is a floating-point value.
- In all other cases, the arguments are compared as floating-point (double-precision) numbers. For example, a comparison of string and numeric operands takes place as a comparison of floating-point numbers.

### 14.3 Postgres: 不支持字符串的列同数值列比较

```plaintext
postgres=# select Greatest(Name, Number2) from Product;
ERROR:  GREATEST types character varying and integer cannot be matched
LINE 1: select Greatest(Name, Number2) from Product;
                              ^
postgres=# select Greatest('19', 100) from Product;
 greatest
----------
      100
      100
      100
(3 rows)

postgres=# select Greatest('19', 100);
 greatest
----------
      100
(1 row)

postgres=# select Name > Number2 from Product;
ERROR:  operator does not exist: character varying > integer
LINE 1: select Name > Number2 from Product;
                    ^
HINT:  No operator matches the given name and argument types. You might need to add explicit type casts.
postgres=# select Number1 > Number2 from Product;
 ?column?
----------
 t
 t

(3 rows)
```

## 15. 附录

无
