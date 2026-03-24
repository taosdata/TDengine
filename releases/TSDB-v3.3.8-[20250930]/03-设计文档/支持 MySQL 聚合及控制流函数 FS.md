# 支持 MySQL 聚合及控制流函数 FS

## 1. 背景

补全一些常见的函数，并兼容 MySQL，Hive 的一些函数，具体如下：
控制流函数
If Ifnull nullif nvl (a synonym for ifnull) nvl2   
比较算子：
Isnull Isnotnull coalesce
聚合函数：
std stddev_samp variance var_samp group_concat
更多背景情况可以参考：
[TS-6111 [产品] 支持 MySQL 的条件函数](https%3A%2F%2Fjira.taosdata.com%3A18080%2Fbrowse%2FTS-6111)
[TS-6112 [产品] 支持 MySQL 的聚合函数](https%3A%2F%2Fjira.taosdata.com%3A18080%2Fbrowse%2FTS-6112)
修改原有标差，方差实现，保证浮点计算精度，避免数值溢出；
为了保证实现的完整性，增加计划外的 nvl2 函数；
三个计划外的位函数由于时间关系暂不实现：bit_and, bit_or, bit_xor

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/9/11 | 0.1 | 金明垒 | 增加各函数的行为说明 |
|  |  |  |  |

## 3. 定义

#### 3.0.1 适用数据类型

函数中的适用数据类型表示参数只接受说明中描述的数据类型，其他的数据类型会导致函数报错。

#### 3.0.2 多字节安全

字符串函数的多字节安全（multibyte safe）指的是在处理多字节字符（比如中文字符）的时候，会把该字符当成一个整体来看待，而不是按照字节划分，将其划分成多个个体来看待。
比如多字节字符在匹配子串的时候，有一个中文字符 `你` 的十六进制表示为 `E4 BD A0`，此时假设有一个字符串的十六进制表示为 `E4 BD` ，该字符串并不会被认为是该中文字符的子串。
以及在计算字符串的字符长度（CHAR_LENGTH）的时候，会把多字节字符 `你` 的长度算做 1，而非 3。
目前只保证使用 UTF-8 编码的字符串的多字节安全

#### 3.0.3 不同数据类型比较规则

因为涉及到不同数据类型比较时的类型转换，在此定义比较规则。
- 函数中涉及的比较规则和比较运算符的规则相同。
- 如果有任意参数为 NULL ，返回 NULL。
- 如果输入参数都是同一类型，按照该类型比较，返回值也是该类型。
- 数值类型和字符串类型比较，按照数值类型进行比较，返回值是字符串类型。
- 如果输入参数包含 VARBINARY 类型，那么其余的参数必须都是 VARCHAR 或 VARBINARY 类型，否则会报错，此时按照 VARBINARY 类型进行比较，返回值为 VARBINARY 类型。

## 4. 行为说明

下面分三部分说明函数行为：控制流函数，比较算子，聚合函数。

#### 4.0.1 控制流函数

##### 4.0.1.1 if

```plaintext
IF(expr1, expr2, expr3)
```

**功能说明**：如果 expr1 为真，返回 expr2，否则返回 expr3。
**返回结果类型**：依赖于使用的上下文。
**适用数据类型**：表达式。
**使用说明**：
1. 类似于 CASE 表达式。
**举例**：
```sql
-> SELECT IF(1>2,2,3);
      if(1>2,2,3)      |
========================
                     3 |
```

##### 4.0.1.2 ifnull/nvl

```plaintext
IFNULL(expr1, expr2)
```

**功能说明**：如果 expr1 非空真，返回 expr1，否则返回 expr2。
**返回结果类型**：依赖于使用的上下文。
**适用数据类型**：表达式。
**使用说明**：
1. nvl 与 ifnull 功能一样。
**举例**：
```sql
-> SELECT IFNULL(1,0); 
      ifnull(1,0)      |
========================
                     1 |
```

##### 4.0.1.3 nullif

```plaintext
NULLIF(expr1, expr2)
```

**功能说明**：如果 expr1 = expr2，返回 NULL，否则返回 expr1。
**返回结果类型**：依赖于使用的上下文。
**适用数据类型**：表达式。
**举例**：
```sql
-> SELECT NULLIF(1,1);
      nullif(1,1)      |
========================
 NULL                  |
```

##### 4.0.1.4 nvl2

```plaintext
NVL2(expr1, expr2, expr3)
```

**功能说明**：如果 expr1 非空值，返回 expr2，否则返回 expr1。
**返回结果类型**：依赖于使用的上下文。
**适用数据类型**：表达式。
**举例**：
```sql
-> SELECT NVL2(NULL,1,2);
    nvl2(null,1,2)     |
========================
                     2 |
```

#### 4.0.2 比较算子

比较算子有三个，见下表：

| # | 名称 | 适用类型 | 描述 |
| --- | --- | --- | --- |
| 1 | ISNULL | 所有类型 | 是否为空值 |
| 2 | ISNOTNULL | 所有类型 | 是否为非空值 |
| 3 | COALESCE | 所有类型 | 返回第一个非空值 |

#### 4.0.3 聚合函数

##### 4.0.3.1 std

```plaintext
STD(expr)
```

**功能说明**：统计表中某列的总体标准差；与 stddev_pop 保持一致。
**返回结果类型**：DOUBLE。
**适用数据类型**：数值类型。
**使用说明**：
1. 若 `expr` 为 NULL，返回 NULL。
2. <equation>\sigma = \sqrt{\frac{1}{N} \sum_{i=1}^{N} (x_i - \bar{x})^2}</equation>
3. 为保证浮点计算精度，避免数值溢出，实现时使用递推公式。
**举例**：
```sql
-> select id from test_stddev;
        +------+
        | id   |
        +------+
        |    1 |
        |    2 |
        |    3 |
        |    4 |
        |    5 |
        +------+
-> select std(id) from test_stddev;
        +--------------------+
        | std(id)     |
        +--------------------+
        | 1.4142135623730951 |
        +--------------------+
```

##### 4.0.3.2 variance

```plaintext
VARIANCE(expr)
```

**功能说明**：统计表中某列的总体方差；与 var_pop 保持一致。
**返回结果类型**：DOUBLE。
**适用数据类型**：数值类型。
**使用说明**：
1. 若 `expr` 为 NULL，返回 NULL。
2. <equation>\sigma^2 = \frac{1}{N} \sum_{i=1}^{N} (x_i - \bar{x})^2</equation>。
3. 为保证浮点计算精度，避免数值溢出，实现时使用递推公式。
**举例**：
```sql
-> select id from test_var;
        +------+
        | id   |
        +------+
        |    1 |
        |    2 |
        |    3 |
        |    4 |
        |    5 |
        +------+
-> select variance(id) from test_var;
        +-------------+
        | variance(id) |
        +-------------+
        |           2 |
        +-------------+
```

##### 4.0.3.3 stddev_samp

```plaintext
stddev_samp(expr)
```

**功能说明**：统计表中某列的样本标准差。
**返回结果类型**：DOUBLE。
**适用数据类型**：数值类型。
**使用说明**：
1. 若 `expr` 为 NULL，返回 NULL。
**举例**：
```sql
-> select id from test_var;
        +------+
        | id   |
        +------+
        |    1 |
        |    2 |
        |    3 |
        |    4 |
        |    5 |
        +------+
-> select stddev_samp(id) from test_var;
        +------------------+
        | stddev_samp(id)  |
        +------------------+
        | 1.58113883008419 |
        +------------------+
```

##### 4.0.3.4 var_samp

```plaintext
var_samp(expr)
```

**功能说明**：统计表中某列的样本方差。
**返回结果类型**：DOUBLE。
**适用数据类型**：数值类型。
**使用说明**：
1. 若 `expr` 为 NULL，返回 NULL。
**举例**：
```sql
-> select id from test_var;
        +------+
        | id   |
        +------+
        |    1 |
        |    2 |
        |    3 |
        |    4 |
        |    5 |
        +------+
-> select var_samp(id) from test_var;
        +-------------------+
        | var_samp(id)      |
        +-------------------+
        | 2.500000000000000 |
        +-------------------+
```

##### 4.0.3.5 group_concat

```plaintext
group_concat(expr..., separator)
```

**功能说明**：将多个字段值连接为一个字符串。
**返回结果类型**：VARCHAR。
**适用数据类型**：字符串类型。
**使用说明**：
1. 若 `expr` 为 NULL，返回 NULL。
**举例**：
```sql
-> select str1, str2 from test_var;
     id      |      id      |
=============================
          a1 |       b1     |
          a2 |       b2     |
          a3 |       b3     |
          
-> select group_concat(str1, str2, ':') from test_var;
         group_concat(str1, str2, ':')   |
==========================================
         a1b1:a2b2:a3b3                  |
```

## 5. 性能

无。

## 6. 兼容性

函数行为变动及兼容性影响说明如下：
1. 修改原有标差，方差实现，保证浮点计算精度，避免数值溢出；
2. group_concat 语法较为复杂，目前函数框架暂无法支持，仅支持分隔符。

## 7. 运维

无。

## 8. 使用场景

具体使用方法已在行为说明章节中每个函数的**举例**部分写明。

## 9. 约束和限制

具体每个函数的约束和限制见行为说明章节中**使用说明**部分以及**适用数据类型**部分。

## 10. 常见错误和排查

无。

## 11. 可观测性

无。

## 12. 安装和卸载

无。

## 13. 文档

需要修改官网文档
行为说明章节中的内容需要更新到官网文档的**SQL手册-函数**部分

## 14. 参考文档

标差，方差重新实现：

TS-7115

函数行为可参考：
https://dev.mysql.com/doc/refman/9.0/en/aggregate-functions.html#function_std
https://dev.mysql.com/doc/refman/9.0/en/comparison-operators.html#function_isnull
https://dev.mysql.com/doc/refman/9.0/en/flow-control-functions.html#function_if

## 15. 附录

无。
