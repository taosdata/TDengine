# 时序数据查询模块-Function Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-01-10 | 2025-01-10 | 1.0 | 潘魏 | 第一次安可送测 |
| 2025-11-26 | 2025-11-26 | 1.1 | 廖浩均 | 重构文档 |

## 2. 背景

时序数据查询引擎作为软件系统的核心组件，用于高效处理需求文档中明确规定的各类时序数据查询与实时分析需求。查询引擎通过深度优化的查询处理架构，能够支持从简单的点查询到复杂的时间窗口聚合分析，同时保证在数据持续流入场景下的低延迟响应。
在接口设计方面，引擎提供了完整的 C/C++ 语言 API 接口层，这些接口严格遵循软件产品的架构规范，通过定义函数原型、数据结构和错误处理机制，为上层应用提供稳定可靠的数据访问能力。API 设计充分考虑了时序数据的特点，提供了专门的时间序列操作函数、窗口聚合接口和流式处理支持。
时序数据查询引擎具备以下关键特性：支持 SQL-92 查询语法并针对时序场景进行扩展，提供丰富的时间序列处理函数库高效执行数据降采样、时间窗口计算、异常检测等复杂操作。通过精心设计的内存管理和查询优化策略，引擎能够在保证查询性能的同时，有效控制资源消耗。
通过 C/C++ API，外部系统可以方便地集成时序数据查询能力，实现数据的实时监控、趋势分析和决策支持。这种设计既保证了引擎的高性能，又为软件产品的集成部署提供了充分的灵活性。

## 3. 定义

1. **结构化查询语言（Structured Query Language，SQL）：**是一种用于管理和操作关系型数据库的标准编程语言。 它允许用户存储、更新、删除、搜索和检索数据库中的数据。 SQL 被广泛应用于各种数据中心应用程序中，是 ISO 和 ANSI 等标准化机构认可的国际标准。
2. **SQL-92**：《Information Systems - Database Language - SQL (includes ANSI X3.168-1989) (formerly ANSI X3.135-1992(R1998))》，ANSI 在 1992 年发布的数据库 SQL 语言标准。
3. **关联查询驱动表**：驱动关联查询进行的表，在 Left Join 系列中左表为驱动表，在 Right Join 系列中右表为驱动表。
4. **主键列**：本文中提到的主键列都指的是数据库内第一列时间列，又称为主键时间列，与复合主键新增的主键列无关。
5. **等值条件**：通过等号（=）运算符指定的运算条件。
6. **连接条件**：连接条件是指进行表关联所指定的条件，TDengine 支持的所有关联查询都需要指定连接条件，连接条件通常（Inner Join 和 Window Join 例外）只出现在 ON 之后。根据语义，Inner Join 中出现在 WHERE 之后的条件也可以视作连接条件，而 Window Join 是通过 WINDOW_OFFSET 来指定连接条件。
7. **过滤条件**：过滤条件是指对表或表关联的结果进行过滤的条件，通过 WHERE 子句进行指定。
8. **关联查询主连接条件**：作为一款时序数据库，TDengine 所有的关联查询都围绕主键时戳列进行，因此要求除 ASOF/Window Join 外的所有关联查询都必须含有主键列的等值连接条件，而按照顺序首次出现在连接条件中的主键列等值连接条件将会被作为主连接条件。ASOF Join 的主连接条件可以包含非等值的连接条件，而 Window Join 的主连接条件则是通过 WINDOW_OFFSET 来指定。
9. **关联查询分组条件**：时序数据库特色的 ASOF/Window Join （其他 Join 不支持）支持对关联查询的输入数据进行分组，然后每个分组内进行关联操作。分组只对关联查询的输入进行，输出结果将不包含分组信息。ASOF/Window Join 中出现在 ON 之后的等值条件（ASOF 的主连接条件除外）将被作为分组条件。
10. **内存池：**预先分配并管理一大块连续内存的技术，用于高效地处理大量、频繁的小内存请求。它通过一次性向操作系统申请大块内存，并在内部自行分割和回收，避免了频繁的系统调用和内存碎片问题。
11. **分配的内存大小（MS）**：在通过各种内存分配接口从系统分配内存时指定的要分配的内存大小，其值由应用具体指定。
12. **实际分配的内存大小（AMS）**：通过各种内存分配接口从系统分配的实际内存大小，因为内存管理器的实现原因，其值有可能会大于应用指定的分配大小。
13. **实际使用的内存大小（UMS）**：实际使用的内存大小是应用真正从系统获得的物理内存大小，因为物理内存的分配是在实际使用时才会分配，因此 UMS 会小于或等于 AMS，并且根据使用情况的不同其差值可能会存在显著差异。
14. **系统可用内存大小（SAMS）：**某一时刻系统中可以使用的物理内存大小（不含 SWAP）。

## 4. 行为说明

时序数据查询引擎提供了一套强大的数据访问接口，其核心设计遵循广泛兼容的 SQL-92 标准语法规范，同时针对时序数据的独特特征进行了深度优化与功能扩展。
查询引擎不仅完整支持 SELECT、WHERE、GROUP BY、ORDER BY 等查询子句，还专门设计了面向时序场景的特殊语法和函数，使得用户能够以熟悉的 SQL 语句高效处理时间序列数据。
主要的查询语句的语法规则如下：
1. 标准SQL语法兼容性，完全支持 SQL-92 标准的核心语法，包括但不限于：
   - 数据查询（SELECT）
   - 条件过滤（WHERE）
   - 数据分组与聚合（GROUP BY, HAVING）
   - 结果排序（ORDER BY）
   - 多表连接（JOIN）
2. 时序数据专用扩展语法
   - 时间窗口函数：提供INTERVAL子句进行固定窗口、滑动窗口的数据切分
   - 时间序列填充：支持FILL子句对缺失时间点进行插值处理
   - 时间区间查询：使用TIME_RANGE(start_time, end_time)函数快速查询特定时间段
   - 降采样查询：通过SAMPLE 子句实现数据降采样
   - 时间序列分组：支持PARTITION BY对时间序列进行逻辑分组
3. 时序专用函数库
   - 时间计算函数：如NOW(), DIFF(), TIMETRUNCATE()等
   - 时序聚合函数：除标准聚合函数外，还提供：
      - FIRST() - 获取时间窗口内第一个值
      - LAST() - 获取时间窗口内最后一个值
      - INTERVAL() - 时间窗口聚合
      - MAVG() - 移动平均计算

### 4.1 语法定义

查询 SQL 语句主要的语法规则定义如下：
```sql {wrap}
SELECT {DATABASE() | CLIENT_VERSION() | SERVER_VERSION() | SERVER_STATUS() | NOW() | TODAY() | TIMEZONE() | CURRENT_USER() | USER() }

SELECT [hints] [DISTINCT] [TAGS] select_list
    from_clause
    [WHERE condition]
    [partition_by_clause]
    [interp_clause]
    [window_clause]
    [group_by_clause]
    [order_by_clasue]
    [SLIMIT limit_val [SOFFSET offset_val]]
    [LIMIT limit_val [OFFSET offset_val]]
    [>> export_file]

hints: /*+ [hint([hint_param_list])] [hint([hint_param_list])] */

hint:
    BATCH_SCAN | NO_BATCH_SCAN | SORT_FOR_GROUP | PARTITION_FIRST | PARA_TABLES_SORT | SMALLDATA_TS_SORT

select_list:
    select_expr [, select_expr] ...

select_expr: {
    *
  | query_name.*
  | [schema_name.] {table_name | view_name} .*
  | t_alias.*
  | expr [[AS] c_alias]
}

from_clause: {
    table_reference [, table_reference] ...
  | table_reference join_clause [, join_clause] ...
}

table_reference:
    table_expr t_alias

table_expr: {
    table_name
  | view_name
  | ( subquery )
}

join_clause:
    [INNER|LEFT|RIGHT|FULL] [OUTER|SEMI|ANTI|ASOF|WINDOW] JOIN table_reference [ON condition] [WINDOW_OFFSET(start_offset, end_offset)] [JLIMIT jlimit_num]

window_clause: {
    SESSION(ts_col, tol_val)
  | STATE_WINDOW(col)
  | INTERVAL(interval_val [, interval_offset]) [SLIDING (sliding_val)] [WATERMARK(watermark_val)] [FILL(fill_mod_and_val)]
  | EVENT_WINDOW START WITH start_trigger_condition END WITH end_trigger_condition
  | COUNT_WINDOW(count_val[, sliding_val])

interp_clause:
      RANGE(ts_val [, ts_val]) EVERY(every_val) FILL(fill_mod_and_val)

partition_by_clause:
    PARTITION BY partition_by_expr [, partition_by_expr] ...

partition_by_expr:
    {expr | position | c_alias}

group_by_clause:
    GROUP BY group_by_expr [, group_by_expr] ... HAVING condition
                                                    
group_by_expr:
    {expr | position | c_alias}

order_by_clasue:
    ORDER BY order_expr [, order_expr] ...

order_expr:
    {expr | position | c_alias} [DESC | ASC] [NULLS FIRST | NULLS LAST]
    
```

### 4.2 时序数据查询

#### 4.2.1 函数说明

##### 4.2.1.1 缓存函数

###### 4.2.1.1.1 LAST

```sql
LAST(expr)
```

**功能说明**：统计表/超级表中某列的值最后写入的非 NULL 值。
**返回数据类型**：同应用的字段。
**适用数据类型**：所有字段。
**适用于**：表和超级表。
**使用说明**:
1. 如果要返回各个列的最后（时间戳最大）一个非 NULL 值，可以使用 LAST(*)；查询超级表，且multiResultFunctionStarReturnTags设置为 0 (默认值) 时，LAST(*)只返回超级表的普通列；设置为 1 时，返回超级表的普通列和标签列。
2. 如果结果集中的某列全部为 NULL 值，则该列的返回结果也是 NULL；如果结果集中所有列全部为 NULL 值，则不返回结果。
3. 在用于超级表时，时间戳完全一样且同为最大的数据行可能有多个，那么会从中随机返回一条，而并不保证多次运行所挑选的数据行必然一致。
4. 对于存在复合主键的表的查询，若最大时间戳的数据有多条，则只有对应的复合主键最大的数据被返回。

###### 4.2.1.1.2 LAST_ROW

```sql
LAST_ROW(expr)
```

**功能说明**：返回表/超级表的最后一条记录。
**返回数据类型**：同应用的字段。
**适用数据类型**：所有字段。
**适用于**：表和超级表。
**使用说明**：
1. 如果要返回各个列的最后一条记录（时间戳最大），可以使用 LAST_ROW(*)；查询超级表，且multiResultFunctionStarReturnTags设置为 0 (默认值) 时，LAST_ROW(*)只返回超级表的普通列；设置为 1 时，返回超级表的普通列和标签列。
2. 在用于超级表时，时间戳完全一样且同为最大的数据行可能有多个，那么会从中随机返回一条，而并不保证多次运行所挑选的数据行必然一致。
3. 不能与 INTERVAL 一起使用。
4. 与 LAST 函数一样，对于存在复合主键的表的查询，若最大时间戳的数据有多条，则只有对应的复合主键最大的数据被返回。

##### 4.2.1.2 插值函数

###### 4.2.1.2.1 INTERP

```sql
INTERP(expr [, ignore_null_values])

ignore_null_values: {
    0
  | 1
}
```

**功能说明**：返回指定时间截面指定列的记录值或插值。ignore_null_values 参数的值可以是 0 或 1，为 1 时表示忽略 NULL 值, 缺省值为 0。
**返回数据类型**：同字段类型。
**适用数据类型**：数值类型。
**适用于**：表和超级表。
**使用说明**
1. INTERP 用于在指定时间断面获取指定列的记录值，如果该时间断面不存在符合条件的行数据，那么会根据 FILL 参数的设定进行插值。
2. INTERP 的输入数据为指定列的数据，可以通过条件语句（where 子句）来对原始列数据进行过滤，如果没有指定过滤条件则输入为全部数据。
3. INTERP SQL 查询需要同时与 RANGE，EVERY 和 FILL 关键字一起使用；流计算不能使用 RANGE，需要 EVERY 和 FILL 关键字一起使用。
4. INTERP 的输出时间范围根据 RANGE(timestamp1, timestamp2) 字段来指定，需满足 timestamp1 <= timestamp2。其中 timestamp1 为输出时间范围的起始值，即如果 timestamp1 时刻符合插值条件则 timestamp1 为输出的第一条记录，timestamp2 为输出时间范围的结束值，即输出的最后一条记录的 timestamp 不能大于 timestamp2。
5. INTERP 根据 EVERY(time_unit) 字段来确定输出时间范围内的结果条数，即从 timestamp1 开始每隔固定长度的时间（time_unit 值）进行插值，time_unit 可取值时间单位：1a(毫秒)，1s(秒)，1m(分)，1h(小时)，1d(天)，1w(周)。例如 EVERY(500a) 将对于指定数据每500毫秒间隔进行一次插值.
6. INTERP 根据 FILL 字段来决定在每个符合输出条件的时刻如何进行插值。关于 FILL 子句如何使用请参考 [FILL 子句](https://docs.taosdata.com/reference/taos-sql/distinguished/#fill-%E5%AD%90%E5%8F%A5)
7. INTERP 可以在 RANGE 字段中只指定唯一的时间戳对单个时间点进行插值，在这种情况下，EVERY 字段可以省略。例如：SELECT INTERP(col) FROM tb RANGE('2023-01-01 00:00:00') FILL(linear).
8. INTERP 作用于超级表时, 会将该超级表下的所有子表数据按照主键列排序后进行插值计算，也可以搭配 PARTITION BY tbname 使用，将结果强制规约到单个时间线。
9. INTERP 可以与伪列 _irowts 一起使用，返回插值点所对应的时间戳(3.0.2.0 版本以后支持)。
10. INTERP 可以与伪列 _isfilled 一起使用，显示返回结果是否为原始记录或插值算法产生的数据(3.0.3.0 版本以后支持)。
11. INTERP 对于带复合主键的表的查询，若存在相同时间戳的数据，则只有对应的复合主键最小的数据参与运算。

##### 4.2.1.3 数值函数

时序数据函数是时序数据库特有的与时序时间处理紧密相关的一类函数。

###### 4.2.1.3.1 CSUM

```sql
CSUM(expr)
```

**功能说明**：累加和（Cumulative sum），忽略 NULL 值。
**返回结果类型**： 输入列如果是整数类型返回值为长整型 （int64_t），浮点数返回值为双精度浮点数（Double）。无符号整数类型返回值为无符号长整型（uint64_t）。
**适用数据类型**：数值类型。
**嵌套子查询支持**： 适用于内层查询和外层查询。
**适用于**：表和超级表。
**使用说明**：
- 不支持 +、-、*、/ 运算，如 csum(col1) + csum(col2)。
- 只能与聚合（Aggregation）函数一起使用。 该函数可以应用在普通表和超级表上。

###### 4.2.1.3.2 DERIVATIVE

```sql
DERIVATIVE(expr, time_interval, ignore_negative)

ignore_negative: {
    0
  | 1
}
```

**功能说明**：统计表中某列数值的单位变化率。其中单位时间区间的长度可以通过 time_interval 参数指定，最小可以是 1 秒（1s）；ignore_negative 参数的值可以是 0 或 1，为 1 时表示忽略负值。对于存在复合主键的表的查询，若时间戳相同的数据存在多条，则只有对应的复合主键最小的数据参与运算。
**返回数据类型**：DOUBLE。
**适用数据类型**：数值类型。
**适用于**：表和超级表。
**使用说明**:
- 可以与选择相关联的列一起使用。 例如: select _rowts, DERIVATIVE() from。

###### 4.2.1.3.3 DIFF

```sql
DIFF(expr [, ignore_option])

ignore_option: {
    0
  | 1
  | 2
  | 3
}
```

**功能说明**：统计表中特定列与之前行的当前列有效值之差。 ignore_option 取值为 0|1|2|3 , 可以不填，默认值为 0.
- `0` 表示不忽略(diff结果)负值不忽略 null 值
- `1` 表示(diff结果)负值作为 null 值
- `2` 表示不忽略(diff结果)负值但忽略 null 值
- `3` 表示忽略(diff结果)负值且忽略 null 值
- 对于存在复合主键的表的查询，若时间戳相同的数据存在多条，则只有对应的复合主键最小的数据参与运算。
**返回数据类型**：bool、时间戳及整型数值类型均返回 int_64，浮点类型返回 double, 若 diff 结果溢出则返回溢出后的值。
**适用数据类型**：数值类型、时间戳和 bool 类型。
**适用于**：表和超级表。
**使用说明**:
1. diff 是计算本行特定列与同列的前一个有效数据的差值，同列的前一个有效数据：指的是同一列中时间戳较小的最临近的非空值。
2. 数值类型 diff 结果为对应的算术差值；时间戳类型根据数据库的时间戳精度进行差值计算；bool 类型计算差值时 true 视为 1， false 视为 0
3. 如当前行数据为 null 或者没有找到同列前一个有效数据时，diff 结果为 null
4. 忽略负值时（ ignore_option 设置为 1 或 3 ），如果 diff 结果为负值，则结果设置为 null，然后根据 null 值过滤规则进行过滤
5. 当 diff 结果发生溢出时，结果是否是`应该忽略的负值`取决于逻辑运算结果是正数还是负数，例如 9223372036854775800 - (-9223372036854775806) 的值超出 BIGINT 的范围 ，diff 结果会显示溢出值 -10，但并不会被作为负值忽略
6. 单个语句中可以使用单个或者多个 diff，并且每个 diff 可以指定相同或不同的 ignore_option ，当单个语句中存在多个 diff 时当且仅当某行所有 diff 的结果都为 null ，并且 ignore_option 都设置为忽略 null 值，该行才从结果集中剔除
7. 可以选择与相关联的列一起使用。 例如: select _rowts, DIFF() from。
8. 当没有复合主键时，如果不同的子表有相同时间戳的数据，会提示 "Duplicate timestamps not allowed"
9. 当使用复合主键时，不同子表的时间戳和主键组合可能相同，使用哪一行取决于先找到哪一行，这意味着在这种情况下多次运行 diff() 的结果可能会不同。

###### 4.2.1.3.4 IRATE

```sql
IRATE(expr)
```

**功能说明**：计算瞬时增长率。使用时间区间中最后两个样本数据来计算瞬时增长速率；如果这两个值呈递减关系，那么只取最后一个数用于计算，而不是使用二者差值。对于存在复合主键的表的查询，若时间戳相同的数据存在多条，则只有对应的复合主键最小的数据参与运算。
**返回数据类型**：DOUBLE。
**适用数据类型**：数值类型。
**适用于**：表和超级表。

###### 4.2.1.3.5 MAVG

```sql
MAVG(expr, k)
```

**功能说明**： 计算连续 k 个值的移动平均数（moving average）。如果输入行数小于 k，则无结果输出。参数 k 的合法输入范围是 1≤ k ≤ 1000。
**返回结果类型**： DOUBLE。
**适用数据类型**： 数值类型。
**嵌套子查询支持**： 适用于内层查询和外层查询。
**适用于**：表和超级表。
**使用说明**：
1. 不支持 +、-、*、/ 运算，如 mavg(col1, k1) + mavg(col2, k1);
2. 只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用；

###### 4.2.1.3.6 STATECOUNT

```sql
STATECOUNT(expr, oper, val)
```

**功能说明**：返回满足某个条件的累计连续记录的个数。计算规则：如果条件满足则累计计数加 1，条件不满足则重置计数为-1，如果 expr 数据为 NULL，则输出为 NULL。
**参数范围**：
1. oper : "LT" (小于)、"GT"（大于）、"LE"（小于等于）、"GE"（大于等于）、"NE"（不等于）、"EQ"（等于），不区分大小写。
2. val : 数值型
**返回结果类型**：INTEGER。
**适用数据类型**：数值类型。
**嵌套子查询支持**：不支持应用在子查询上。
**适用于**：表和超级表。
**使用说明**：
- 不能和窗口操作一起使用，例如 interval/state_window/session_window。

###### 4.2.1.3.7 STATEDURATION

```sql
STATEDURATION(expr, oper, val, unit)
```

**功能说明**：返回满足某个条件的累计连续记录的时间长度。计算规则：如果条件满足则累计时间加上当前记录与前一条记录之间的时间长度，第一个满足条件的记录时间长度记为 0；如果条件不满足则重置累计时间长度为-1，如果 expr 数据为 NULL，则输出为 NULL。
**参数范围**：
1. oper : `'LT'` (小于)、`'GT'`（大于）、`'LE'`（小于等于）、`'GE'`（大于等于）、`'NE'`（不等于）、`'EQ'`（等于），不区分大小写，但需要用`''`包括。
2. val : 数值型
3. unit : 输出的累计时间长度的单位，可取值时间单位： 1b(纳秒), 1u(微秒)，1a(毫秒)，1s(秒)，1m(分)，1h(小时)，1d(天), 1w(周)。如果省略，默认为当前数据库精度。
**返回结果类型**：INTEGER。
**适用数据类型**：数值类型。
**嵌套子查询支持**：不支持应用在子查询上。
**适用于**：表和超级表。
**使用说明**：
- 不能和窗口操作一起使用，例如 interval/state_window/session_window。

###### 4.2.1.3.8 TWA

```sql
TWA(expr)
```

**功能说明**：统计表中某列在一段时间内的时间加权平均值。对于存在复合主键的表的查询，若时间戳相同的数据存在多条，则只有对应的复合主键最小的数据参与运算。流计算仅在 FORCE_WINDOW_CLOSE 模式下支持该函数。
**返回数据类型**：DOUBLE。
**适用数据类型**：数值类型。
**适用于**：表和超级表。

##### 4.2.1.4 聚合函数

###### 4.2.1.4.1 APERCENTILE

```sql
APERCENTILE(expr, p [, algo_type])

algo_type: {
    "default"
  | "t-digest"
}
```

**功能说明**：统计表/超级表中指定列的值的近似百分比分位数，与 PERCENTILE 函数相似，但是返回近似结果。
**返回数据类型**： DOUBLE。
**适用数据类型**：数值类型。
**适用于**：表和超级表。
**说明**：
1. p 值范围是 [0,100]，当为0时等同于 MIN，为 100 时等同于 MAX。
2. algo_type 取值为 "default" 或 "t-digest"。 输入为 "default" 时函数使用基于直方图算法进行计算。输入为 "t-digest" 时使用 t-digest 算法计算分位数的近似结果。如果不指定 algo_type 则使用 "default" 算法。
3. "t-digest" 算法的近似结果对于输入数据顺序敏感，对超级表查询时不同的输入排序结果可能会有微小的误差。

###### 4.2.1.4.2 ELAPSED

```sql
ELAPSED(ts_primary_key [, time_unit])
```

**功能说明**：elapsed 函数表达了统计周期内连续的时间长度，和 twa 函数配合使用可以计算统计曲线下的面积。在通过 INTERVAL 子句指定窗口的情况下，统计在给定时间范围内的每个窗口内有数据覆盖的时间范围；如果没有 INTERVAL 子句，则返回整个给定时间范围内的有数据覆盖的时间范围。注意，ELAPSED 返回的并不是时间范围的绝对值，而是绝对值除以 time_unit 所得到的单位个数。流计算仅在 FORCE_WINDOW_CLOSE 模式下支持该函数。
**返回结果类型**：DOUBLE。
**适用数据类型**：TIMESTAMP。
**适用于**: 表，超级表，嵌套查询的外层查询
**说明**：
1. ts_primary_key 参数只能是表的第一列，即 TIMESTAMP 类型的主键列。
2. 按 time_unit 参数指定的时间单位返回，最小是数据库的时间分辨率。time_unit 参数未指定时，以数据库的时间分辨率为时间单位。支持的时间单位 time_unit 如下： 1b(纳秒), 1u(微秒)，1a(毫秒)，1s(秒)，1m(分)，1h(小时)，1d(天), 1w(周)。
3. 可以和 interval 组合使用，返回每个时间窗口的时间戳差值。需要特别注意的是，除第一个时间窗口和最后一个时间窗口外，中间窗口的时间戳差值均为窗口长度。
4. order by asc/desc 不影响差值的计算结果。
5. 对于超级表，需要和 group by tbname 子句组合使用，不可以直接使用。
6. 对于普通表，不支持和 group by 子句组合使用。
7. 对于嵌套查询，仅当内层查询会输出隐式时间戳列时有效。例如 select elapsed(ts) from (select diff(value) from sub1) 语句，diff 函数会让内层查询输出隐式时间戳列，此为主键列，可以用于 elapsed 函数的第一个参数。相反，例如 select elapsed(ts) from (select * from sub1) 语句，ts 列输出到外层时已经没有了主键列的含义，无法使用 elapsed 函数。此外，elapsed 函数作为一个与时间线强依赖的函数，形如 select elapsed(ts) from (select diff(value) from st group by tbname)尽 管会返回一条计算结果，但并无实际意义，这种用法后续也将被限制。
8. 不支持与 leastsquares、diff、derivative、top、bottom、last_row、interp 等函数混合使用。

###### 4.2.1.4.3 LEASTSQUARES

```sql
LEASTSQUARES(expr, start_val, step_val)
```

**功能说明**：统计表中某列的值的拟合直线方程。start_val 是自变量初始值，step_val 是自变量的步长值。
**返回数据类型**：字符串表达式（斜率, 截距）。
**适用数据类型**：expr 必须是数值类型。
**适用于**：表。

###### 4.2.1.4.4 SPREAD

```sql
SPREAD(expr)
```

**功能说明**：统计表中某列的最大值和最小值之差。
**返回数据类型**：DOUBLE。
**适用数据类型**：INTEGER, TIMESTAMP。
**适用于**：表和超级表。

#### 4.2.2 关联查询

##### 4.2.2.1 Left ASOF Join

**含义**
左不完全匹配连接 - 不同于其他传统 Join 的完全匹配模式，ASOF Join 允许以指定的匹配模式进行不完全匹配，即按照主键时间戳最接近的方式进行匹配。
**语法**
```sql {wrap}
SELECT ... FROM table_name1 LEFT ASOF JOIN table_name2 [ON ...] [JLIMIT jlimit_num] [WHERE ...] [...]
```

**结果**集
左表中每一行数据与右表中符合连接条件的按主键列排序后时间戳最接近的最多 jlimit_num 条数据或空数据（NULL）的笛卡尔积集合。
**适用范围**
支持超级表、普通表、子表间 Left ASOF Join。
**说明**
1. 只支持表间 ASOF Join，不支持子查询间 ASOF Join；
2. ON 子句中支持指定主键列或主键列的 timetruncate 函数运算（不支持其他标量运算及函数）后的单个匹配规则（主连接条件），支持的运算符及其含义如下：

  | 运算符 | 含义 |
| --- | --- |
| > | 匹配右表中主键时间戳小于左表主键时间戳且时间戳最接近的数据行 |
| >= | 匹配右表中主键时间戳小于等于左表主键时间戳且时间戳最接近的数据行 |
| = | 匹配右表中主键时间戳等于左表主键时间戳的行 |
| < | 匹配右表中主键时间戳大于左表主键时间戳且时间戳最接近的数据行 |
| <= | 匹配右表中主键时间戳大于等于左表主键时间戳且时间戳最接近的数据行 |

1. 如果不含 ON 子句或 ON 子句中未指定主键列的匹配规则，则默认主键匹配规则运算符是 “>=”， 即右表中主键时戳小于等于左表主键时戳的行数据。不支持多个主连接条件；
2. ON 子句中还可以指定除主键列外的 TAG、普通列（不支持标量函数及运算）之间的等值条件用于分组计算，除此之外不支持其他类型的条件；
3. 所有 ON 条件间只支持 AND 运算；
4. JLIMIT 用于指定单行匹配结果的最大行数，可选，未指定时默认值为1，即左表每行数据最多从右表中获得一行匹配结果。JLIMIT 取值范围为 [0, 1024]。符合匹配条件的 jlimit_num 条数据不要求时间戳相同，当右表中不存在满足条件的 jlimit_num 条数据时，返回的结果行数可能小于 jlimit_num；当右表中存在符合条件的多于 jlimit_num 条数据时，如果时间戳相同将随机返回 jlimit_num 条数据。
**示例**

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.ts, b.ts FROM tba1 a LEFT ASOF JOIN tba2 b ON a.ts = b.ts | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:04 </column> <column width="50"> 2023-11-17 16:29:00 NULL 2023-11-17 16:29:03 NULL </column> </grid> |
| SELECT a.ts, b.ts FROM tba1 a LEFT ASOF JOIN tba2 b ON a.ts <= b.ts | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:04 </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:03 2023-11-17 16:29:03 2023-11-17 16:29:05 </column> </grid> |
| SELECT a.ts, b.ts FROM tba1 a LEFT ASOF JOIN tba2 b ON a.ts > b.ts JLIMIT 2 | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:03 2023-11-17 16:29:04 2023-11-17 16:29:04 </column> <column width="50"> NULL 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:01 2023-11-17 16:29:03 </column> </grid> |
| SELECT a.ts, b.ts FROM tba1 a LEFT ASOF JOIN tba2 b | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:04 </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:03 </column> </grid> |
| SELECT a.ts, b.ts FROM tba1 a LEFT ASOF JOIN tba2 b JLIMIT 2 | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:03 2023-11-17 16:29:04 2023-11-17 16:29:04 </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:01 2023-11-17 16:29:03 </column> </grid> |

##### 4.2.2.2 Left Window Join

**含义**
左窗口连接 - 根据左表中每一行的主键时间戳和窗口边界构造窗口并据此进行窗口连接，支持窗口内进行投影、标量和聚合操作。
**语法**
```sql {wrap}
SELECT ... FROM table_name1 LEFT WINDOW JOIN table_name2 [ON ...] WINDOW_OFFSET(start_offset, end_offset) [JLIMIT jlimit_num] [WHERE ...] [...]
```

**结果集**
左表中每一行数据与右表中基于左表主键时戳列和 WINDOW_OFFSET 划分的窗口内的至多 jlimit_num 条数据或空数据（NULL）的笛卡尔积集合 或 
左表中每一行数据与右表中基于左表主键时戳列和 WINDOW_OFFSET 划分的窗口内的至多 jlimit_num 条数据的聚合结果或空数据（NULL）组成的行数据集合。
**适用范围**
支持超级表、普通表、子表间 Left Window Join。
**说明**
1. 只支持表间 Window Join，不支持子查询间 Window Join；
2. ON 子句可选，只支持指定除主键列外的 TAG、普通列（不支持标量函数及运算）之间的等值条件用于分组计算，所有条件间只支持 AND 运算；
3. WINDOW_OFFSET 用于指定窗口的左右边界相对于左表主键时间戳的偏移量，支持自带时间单位的形式，例如：WINDOW_OFFSET(-1a， 1a)，表示每个窗口是 [左表主键时间戳 - 1毫秒，左表主键时间戳 + 1毫秒] ，左右边界均为闭区间。数字后面的时间单位可以是 b（纳秒）、u（微秒）、a（毫秒）、s（秒）、m（分）、h（小时）、d（天）、w（周），不支持自然月（n）、自然年（y），支持的最小时间单位为数据库精度，左右表所在数据库精度需保持一致。
4. JLIMIT 用于指定单个窗口内的最大匹配行数，可选，未指定时默认获取每个窗口内的所有匹配行。JLIMIT 取值范围为 [0, 1024]，当右表中不存在满足条件的 jlimit_num 条数据时，返回的结果行数可能小于 jlimit_num；当右表中存在超过 jlimit_num 条满足条件的数据时，优先返回窗口内主键时间戳最小的 jlimit_num 条数据。
5. SQL 语句中不能含其他 GROUP BY/PARTITION BY/窗口查询；
6. 支持在 WHERE 子句中进行标量过滤，支持在 HAVING 子句中针对每个窗口进行聚合函数过滤（不支持标量过滤），不支持 SLIMIT，不支持各种窗口伪列；
**示例**

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.ts, b.ts FROM tba1 a LEFT WINDOW JOIN tba2 b WINDOW_OFFSET(-1s, 1s) | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:04 2023-11-17 16:29:04 </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:03 2023-11-17 16:29:03 2023-11-17 16:29:05 </column> </grid> |
| SELECT a.ts, b.ts FROM tba1 a LEFT WINDOW JOIN tba2 b WINDOW_OFFSET(-1s, 1s) JLIMIT 1 | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:04 </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:03 </column> </grid> |
| SELECT a.ts, count(b.*) FROM tba1 a LEFT WINDOW JOIN tba2 b WINDOW_OFFSET(-1s, 1s) | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:04 </column> <column width="50"> 2 2 1 2 </column> </grid> |
| SELECT a.ts, count(b.*) FROM tba1 a LEFT WINDOW JOIN tba2 b ON a.col1 = b.col1 WINDOW_OFFSET(-1s, 1s) | <grid cols="2"> <column width="50"> 2023-11-17 16:29:02 2023-11-17 16:29:04 2023-11-17 16:29:00 2023-11-17 16:29:03 </column> <column width="50"> 1 1 0（NULL） 0（NULL） </column> </grid> |
| SELECT a.ts, count(b.*) FROM tba1 a LEFT WINDOW JOIN tba2 b WINDOW_OFFSET(-1s, 1s) HAVING(count(b.*) > 1) | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:04 </column> <column width="50"> 2 2 2 </column> </grid> |

##### 4.2.2.3 Right ASOF Join

**含义**
右不完全匹配连接 - 不同于其他传统 Join 的完全匹配模式，ASOF Join 允许以指定的匹配模式进行不完全匹配，即按照主键时间戳最接近的方式进行匹配。
**语法**
```sql {wrap}
SELECT ... FROM table_name1 RIGHT ASOF JOIN table_name2 [ON ...] [JLIMIT jlimit_num] [WHERE ...] [...]
```

**结果集**
右表中每一行数据与左表中符合连接条件的按主键列排序后时间最接近的最多 jlimit_num 条数据或空数据（NULL）的笛卡尔积集合。
**适用范围**
支持超级表、普通表、子表间 Right ASOF Join。
**说明**
1. 只支持表间 ASOF Join，不支持子查询间 ASOF Join；
2. ON 子句中支持指定主键列或主键列的 timetruncate 函数运算（不支持其他标量运算及函数）后的单个匹配规则（主连接条件），支持的运算符及其含义如下：

  | 运算符 | 含义 |
| --- | --- |
| > | 匹配左表中主键时间戳大于右表主键时间戳且时间戳最接近的数据行 |
| >= | 匹配左表中主键时间戳大于等于右表主键时间戳且时间戳最接近的数据行 |
| = | 匹配左表中主键时间戳等于右表主键时间戳的行 |
| < | 匹配左表中主键时间戳小于右表主键时间戳且时间戳最接近的数据行 |
| <= | 匹配左表中主键时间戳小于等于右表主键时间戳且时间戳最接近的数据行 |

1. 如果不含 ON 子句或 ON 子句中未指定主键列的匹配规则，则默认主键匹配规则运算符是 “<=”， 即匹配左表中主键时间戳小于等于右表主键时间戳的行数据。不支持多个主连接条件。
2. ON 子句中还可以指定除主键列外的 TAG、普通列（不支持标量函数及运算）之间的等值条件用于分组计算，除此之外不支持其他类型的条件；
3. 所有 ON 条件间只支持 AND 运算。
4. JLIMIT 用于指定单行匹配结果的最大行数，可选，未指定时默认值为1，即右表每行数据最多从左表中获得一行匹配结果。JLIMIT 取值范围为 [0, 1024]。符合匹配条件的 jlimit_num 条数据不要求时间戳相同，当左表中不存在满足条件的 jlimit_num 条数据时，返回的结果行数可能小于 jlimit_num。
**示例**

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.ts, b.ts FROM tba1 a RIGHT ASOF JOIN tba2 b ON a.ts = b.ts | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 NULL 2023-11-17 16:29:03 NULL </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:05 </column> </grid> |
| SELECT a.ts, b.ts FROM tba1 a RIGHT ASOF JOIN tba2 b ON a.ts <= b.ts | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:00 2023-11-17 16:29:03 2023-11-17 16:29:04 </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:05 </column> </grid> |
| SELECT a.ts, b.ts FROM tba1 a RIGHT ASOF JOIN tba2 b ON a.ts > b.ts JLIMIT 2 | <grid cols="2"> <column width="50"> 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:04 NULL </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:05 </column> </grid> |

##### 4.2.2.4 Right Window Join

**含义**
右窗口连接 - 根据右表每一行的主键时间戳和窗口边界构造窗口并据此进行窗口连接，支持窗口内进行投影、标量和聚合操作。
**语法**
```sql {wrap}
SELECT ... FROM table_name1 RIGHT WINDOW JOIN table_name2 [ON ...] WINDOW_OFFSET(start_offset, end_offset) [JLIMIT jlimit_num] [WHERE ...] [...]
```

**结果集**
右表中每一行数据与左表中基于右表主键时戳列和 WINDOW_OFFSET 划分的窗口内的至多 jlimit_num 条数据或空数据（NULL）的笛卡尔积集合 或 
右表中每一行数据与左表中基于右表主键时戳列和 WINDOW_OFFSET 划分的窗口内的至多 jlimit_num 条数据的聚合结果或空数据（NULL）组成的行数据集合。
**适用范围**
支持超级表、普通表、子表间 Right Window Join。
**说明**
1. 只支持表间 Window Join，不支持子查询间 Window Join；
2. ON 子句可选，只支持指定除主键列外的 TAG、普通列（不支持标量函数及运算）之间的等值条件用于分组计算，所有条件间只支持 AND 运算；
3. WINDOW_OFFSET 用于指定窗口的左右边界相对于右表主键时间戳的偏移量，支持自带时间单位的形式，例如：WINDOW_OFFSET(-1a， 1a)，则表示每个窗口是 [右表主键时间戳 - 1毫秒，右表主键时间戳 + 1毫秒] ，左右边界均为闭区间。数字后面的时间单位可以是 b（纳秒）、u（微秒）、a（毫秒）、s（秒）、m（分）、h（小时）、d（天）、w（周），不支持自然月、自然年，最小支持的时间单位为数据库精度，左右表所在数据库精度需保持一致。
4. JLIMIT 用于指定单个窗口内的最大匹配行数，可选，未指定时默认获取每个窗口内的所有匹配行。JLIMIT 取值范围为 [0, 1024]，当左表中不存在满足条件的 jlimit_num 条数据时，返回的结果行数可能小于 jlimit_num；当左表中存在超过 jlimit_num 条满足条件的数据时，优先返回窗口内主键时间戳最小的 jlimit_num 条数据。
5. SQL 语句中不能含其他 GROUP BY/PARTITION BY/窗口查询；
6. 支持在 WHERE 子句中进行标量过滤，支持在 HAVING 子句中针对窗口进行聚合函数过滤（不支持标量过滤），不支持 SLIMIT；
**示例**

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.ts, b.ts FROM tba1 a RIGHT WINDOW JOIN tba2 b WINDOW_OFFSET(-1s, 1s) | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:02 2023-11-17 16:29:03 2023-11-17 16:29:04 2023-11-17 16:29:04 </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:03 2023-11-17 16:29:03 2023-11-17 16:29:05 </column> </grid> |
| SELECT a.ts, b.ts FROM tba1 a RIGHT WINDOW JOIN tba2 b WINDOW_OFFSET(-1s, 1s) JLIMIT 1 | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:00 2023-11-17 16:29:02 2023-11-17 16:29:04 </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:05 </column> </grid> |
| SELECT b.ts, count(a.*) FROM tba1 a RIGHT WINDOW JOIN tba2 b WINDOW_OFFSET(-1s, 1s) | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:05 </column> <column width="50"> 1 2 3 1 </column> </grid> |
| SELECT b.ts, count(a.*) FROM tba1 a RIGHT WINDOW JOIN tba2 b ON a.col1 = b.col1 WINDOW_OFFSET(-1s, 1s) | <grid cols="2"> <column width="50"> 2023-11-17 16:29:01 2023-11-17 16:29:03 2023-11-17 16:29:00 2023-11-17 16:29:05 </column> <column width="50"> 1 1 0（NULL） 0（NULL） </column> </grid> |

#### 4.2.3 分片查询

##### 4.2.3.1 PARTITION BY

PARTITION BY 子句是用来支持数据切片查询，用于根据 part_list 对数据进行切分，在每个切分的分片中可以进行各种计算。
PARTITION BY 与 GROUP BY 基本含义相似，都是按照指定列表进行数据分组然后进行计算，不同点在于 PARTITION BY 没有 GROUP BY 子句的 SELECT 列表的各种限制，组内可以进行任意运算（常量、聚合、标量、表达式等），因此在使用上 PARTITION BY 完全兼容 GROUP BY，所有使用 GROUP BY 子句的地方都可以替换为 PARTITION BY, 需要注意的是在没有聚合查询时两者的查询结果可能存在差异。
因为 PARTITION BY 没有返回一行聚合数据的要求，因此还可以支持在分组切片后的各种窗口运算，所有需要分组进行的窗口运算都只能使用 PARTITION BY 子句。

#### 4.2.4 窗口查询

TDengine 支持按时间窗口切分方式进行聚合结果查询，比如温度传感器每秒采集一次数据，但需查询每隔 10 分钟的温度平均值。这种场景下可以使用窗口子句来获得需要的查询结果。窗口子句用于针对查询的数据集合按照窗口切分成为查询子集并进行聚合，窗口包含时间窗口（time window）、状态窗口（status window）、会话窗口（session window）、事件窗口（event window）、计数窗口（count window）五种窗口。其中时间窗口又可划分为滑动时间窗口和翻转时间窗口。
窗口子句语法如下：
```sql
window_clause: {
    SESSION(ts_col, tol_val)
  | STATE_WINDOW(col)
  | INTERVAL(interval_val [, interval_offset]) [SLIDING (sliding_val)] [FILL(fill_mod_and_val)]
  | EVENT_WINDOW START WITH start_trigger_condition END WITH end_trigger_condition
  | COUNT_WINDOW(count_val[, sliding_val])
}
```

其中，interval_val 和 sliding_val 都表示时间段，interval_offset 表示窗口偏移量，interval_offset 必须小于 interval_val，语法上支持三种方式，举例说明如下:
1. INTERVAL(1s, 500a) SLIDING(1s), 自带时间单位的形式，其中的时间单位是单字符表示, 分别为: a (毫秒), b (纳秒), d (天), h (小时), m (分钟), n (月), s (秒), u (微秒), w (周), y (年).
2. INTERVAL(1000, 500) SLIDING(1000), 不带时间单位的形式，将使用查询库的时间精度作为默认时间单位，当存在多个库时默认采用精度更高的库.
3. INTERVAL('1s', '500a') SLIDING('1s'), 自带时间单位的字符串形式，字符串内部不能有任何空格等其它字符.

##### 4.2.4.1 时间窗口

INTERVAL 子句用于产生相等时间周期的窗口，SLIDING 用以指定窗口向前滑动的时间。每次执行的查询是一个时间窗口，时间窗口随着时间流动向前滑动。查询过滤、聚合等操作按照每个时间窗口为独立的单位执行。
INTERVAL 和 SLIDING 子句需要配合聚合或选择函数来使用。例如下面的 SQL 语句非法：
```plaintext
SELECT * FROM temp_tb_1 INTERVAL(1m);
```

SLIDING 向前滑动的时间不能超过一个窗口的时间范围。以下语句非法：
```plaintext
SELECT COUNT(*) FROM temp_tb_1 INTERVAL(1m) SLIDING(2m);
```

INTERVAL 子句允许使用 AUTO 关键字来指定窗口偏移量，此时如果 WHERE 条件给定了明确可应用的起始时间限制，则会自动计算所需偏移量，使得从该时间点切分时间窗口；否则不生效，即：仍以 0 作为偏移量。以下是简单示例说明：
```sql
-- 有起始时间限制，从 '2018-10-03 14:38:05' 切分时间窗口
SELECT COUNT(*) FROM meters WHERE _rowts >= '2018-10-03 14:38:05' INTERVAL (1m, AUTO);

-- 无起始时间限制，不生效，仍以 0 为偏移量
SELECT COUNT(*) FROM meters WHERE _rowts < '2018-10-03 15:00:00' INTERVAL (1m, AUTO);

-- 起始时间限制不明确，不生效，仍以 0 为偏移量
SELECT COUNT(*) FROM meters WHERE _rowts - voltage > 1000000;
```

使用时间窗口需要注意：
1. 聚合时间段的窗口宽度由关键词 INTERVAL 指定，最短时间间隔 10 毫秒（10a）；并且支持偏移 offset（偏移必须小于间隔），也即时间窗口划分与“UTC 时刻 0”相比的偏移量。SLIDING 语句用于指定聚合时间段的前向增量，也即每次窗口向前滑动的时长。
2. 使用 AUTO 作为窗口偏移量时，如果 WHERE 时间条件比较复杂，比如多个 AND/OR/IN 互相组合，那么 AUTO 可能不生效，这种情况可以通过手动指定窗口偏移量进行解决。
3. 使用 AUTO 作为窗口偏移量时，如果窗口宽度的单位是 d (天), n (月), w (周), y (年)，比如: INTERVAL(1d, AUTO), INTERVAL(3w, AUTO)，此时 TSMA 优化无法生效。如果目标表上手动创建了TSMA，语句会报错退出；这种情况下，可以显式指定 Hint SKIP_TSMA 或者不使用 AUTO 作为窗口偏移量。

##### 4.2.4.2 状态窗口

将具有相同的状态量数值的时间区间划分为一个状态窗口，状态量数值发生变化后前一个窗口关闭并同时产生一个新的窗口。使用 STATE_WINDOW 的参数来确定状态窗口划分列，例如：
```plaintext
SELECT COUNT(*), FIRST(ts), status FROM temp_tb_1 STATE_WINDOW(status);
```

仅查询返回 status 为 2 时的状态窗口的信息：
```plaintext
SELECT * FROM (SELECT COUNT(*) AS cnt, FIRST(ts) AS fst, status FROM temp_tb_1 STATE_WINDOW(status)) t WHERE status = 2;
```

TDengine 还支持将 CASE 表达式用在状态量，可以表达某个状态的开始是由满足某个条件而触发，这个状态的结束是由另外一个条件满足而触发的语义。例如，智能电表的电压正常范围是 205V 到 235V，那么可以通过监控电压来判断电路是否正常。
```plaintext
SELECT tbname, _wstart, CASE WHEN voltage >= 205 and voltage <= 235 THEN 1 ELSE 0 END status FROM meters PARTITION BY tbname STATE_WINDOW(CASE WHEN voltage >= 205 and voltage <= 235 THEN 1 ELSE 0 END);
```

##### 4.2.4.3 会话窗口

会话窗口根据记录的主键时间戳的值来划分窗口，只有连续时间戳的间隔小于等于指定的窗口间隔才会被划分为同一个窗口，超出窗口间隔的时间戳会被划分到新的窗口中。
例如，下面的语句会将时间戳间隔小于等于 10s 的记录划归为同一个窗口：
```plaintext

SELECT COUNT(*), FIRST(ts) FROM temp_tb_1 SESSION(ts, 10s);
```

##### 4.2.4.4 事件窗口

事件窗口根据用户指定的开始条件和结束条件来划定窗口，当 start_trigger_condition 满足时则窗口开始，直到 end_trigger_condition 满足时窗口关闭。start_trigger_condition 和 end_trigger_condition 可以是任意 TDengine 支持的条件表达式，且可以包含不同的列。
事件窗口可以仅包含一条数据，即当一条数据同时满足 start_trigger_condition 和 end_trigger_condition，且当前不在一个窗口内时，这条数据自己构成了一个窗口。
事件窗口无法关闭时，不构成一个窗口，不会被输出。即有数据满足 start_trigger_condition，此时窗口打开，但后续数据都不能满足 end_trigger_condition，这个窗口无法被关闭，这部分数据不够成一个窗口，不会被输出。
如果直接在超级表上进行事件窗口查询，TDengine 会将超级表的数据汇总成一条时间线，然后进行事件窗口的计算。 如果需要对子查询的结果集进行事件窗口查询，那么子查询的结果集需要满足按时间线输出的要求，且可以输出有效的时间戳列。
示例：
```sql
select _wstart, _wend, count(*) from t event_window start with c1 > 0 end with c2 < 10 
```

##### 4.2.4.5 计数窗口

计数窗口按固定的数据行数来划分窗口。默认将数据按时间戳排序，再按照用户指定的每个窗口内的记录计数（count_val）将数据划分为多个窗口。count_val 表示每个计数窗口包含的最大数据行数，总数据行数不能整除count_val 时，最后一个窗口的行数会小于 count_val。sliding_val 是常量，表示窗口滑动的数量，类似于 interval的 SLIDING。
示例，每 4 条记录划分为一个窗口：
```sql
select _wstart, _wend, count(*) from t count_window(4);
```

##### 4.2.4.6 时间戳伪列

窗口聚合查询结果中，如果 SQL 语句中没有指定输出查询结果中的时间戳列，那么最终结果中不会自动包含窗口的时间列信息。如果需要在结果中输出聚合结果所对应的时间窗口信息，需要在 SELECT 子句中使用时间戳相关的伪列: 时间窗口起始时间 (_WSTART), 时间窗口结束时间 (_WEND), 时间窗口持续时间 (_WDURATION), 以及查询整体窗口相关的伪列: 查询窗口起始时间(_QSTART) 和查询窗口结束时间(_QEND)。需要注意的是时间窗口起始时间和结束时间均是闭区间，时间窗口持续时间是数据当前时间分辨率下的数值。例如，如果当前数据库的时间分辨率是毫秒，那么结果中 500 就表示当前时间窗口的持续时间是 500毫秒 (500 ms)。
示例
智能电表的建表语句如下：
```plaintext
CREATE TABLE meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS (location BINARY(64), groupId INT);
```

针对智能电表采集的数据，以 10 分钟为一个阶段，计算过去 24 小时的电流数据的平均值、最大值、电流的中位数。如果没有计算值，用前一个非 NULL 值填充。使用的查询语句如下：
```plaintext
SELECT _WSTART, _WEND, AVG(current), MAX(current), APERCENTILE(current, 50) FROM meters
  WHERE ts>=NOW-1d and ts<=now
  INTERVAL(10m)
  FILL(PREV);
```

### 4.3 通用数据查询

#### 4.3.1 语法说明

##### 4.3.1.1 Hints

Hints 是用户控制单个语句查询优化的一种手段，当 Hint 不适用于当前的查询语句时会被自动忽略，具体说明如下：
- Hints 语法以`/*+`开始，终于`*/`，前后可有空格。
- Hints 语法只能跟随在 SELECT 关键字后。
- 每个 Hints 可以包含多个 Hint，Hint 间以空格分开，当多个 Hint 冲突或相同时以先出现的为准。
- 当 Hints 中某个 Hint 出现错误时，错误出现之前的有效 Hint 仍然有效，当前及之后的 Hint 被忽略。
- hint_param_list 是每个 Hint 的参数，根据每个 Hint 的不同而不同。
支持的 Hints 列表如下：
举例：
```sql
SELECT /*+ BATCH_SCAN() */ a.ts FROM stable1 a, stable2 b where a.tag0 = b.tag0 and a.ts = b.ts;
SELECT /*+ SORT_FOR_GROUP() */ count(*), c1 FROM stable1 PARTITION BY c1;
SELECT /*+ PARTITION_FIRST() */ count(*), c1 FROM stable1 PARTITION BY c1;
SELECT /*+ PARA_TABLES_SORT() */ * from stable1 order by ts;
SELECT /*+ SMALLDATA_TS_SORT() */ * from stable1 order by ts;
```

##### 4.3.1.2 结果去重

`DISTINCT` 关键字可以对结果集中的一列或多列进行去重，去除的列既可以是标签列也可以是数据列。
对标签列去重：
```sql
SELECT DISTINCT tag_name [, tag_name ...] FROM stb_name;
```

对数据列去重：
```sql
SELECT DISTINCT col_name [, col_name ...] FROM tb_name;
```

##### 4.3.1.3 标签查询

当查询的列只有标签列时，`TAGS` 关键字可以指定返回所有子表的标签列。每个子表只返回一行标签列。
返回所有子表的标签列：
```sql
SELECT TAGS tag_name [, tag_name ...] FROM stb_name
```

##### 4.3.1.4 窗口子句

1. 窗口子句位于数据切分子句之后，不可以和 GROUP BY 子句一起使用。
2. 窗口子句将数据按窗口进行切分，对每个窗口进行 SELECT 列表中的表达式的计算，SELECT 列表中的表达式只能包含：
   - 常量。
   - _wstart伪列、_wend伪列和_wduration伪列。
   - 聚集函数（包括选择函数和可以由参数确定输出行数的时序特有函数）。
   - 包含上面表达式的表达式。
   - 且至少包含一个聚集函数。

##### 4.3.1.5 FILL 子句

FILL 子句需要与窗口子句或 INTERP 子句一同使用，用于指定某一窗口区间数据缺失的情况下的填充模式。填充模式包括以下几种：
1. 不进行填充：NONE（默认填充模式）。
2. VALUE 填充：固定值填充，此时需要指定填充的数值。例如：FILL(VALUE, 1.23)。这里需要注意，最终填充的值受由相应列的类型决定，如 FILL(VALUE, 1.23)，相应列为 INT 类型，则填充值为 1, 若查询列表中有多列需要 FILL, 则需要给每一个 FILL 列指定 VALUE, 如 `SELECT _wstart, min(c1), max(c1) FROM ... FILL(VALUE, 0, 0)`, 注意, SELECT 表达式中只有包含普通列时才需要指定 FILL VALUE, 如 `_wstart`, `_wstart+1a`, `now`, `1+1` 以及使用 partition by 时的 partition key (如 tbname)都不需要指定 VALUE, 如 `timediff(last(ts), _wstart)` 则需要指定VALUE。
3. PREV 填充：使用前一个非 NULL 值填充数据。例如：FILL(PREV)。
4. NULL 填充：使用 NULL 填充数据。例如：FILL(NULL)。
5. LINEAR 填充：根据前后距离最近的非 NULL 值做线性插值填充。例如：FILL(LINEAR)。
6. NEXT 填充：使用下一个非 NULL 值填充数据。例如：FILL(NEXT)。
以上填充模式中，除了 NONE 模式默认不填充值之外，其他模式在查询的整个时间范围内如果没有数据 FILL 子句将被忽略，即不产生填充数据，查询结果为空。这种行为在部分模式（PREV、NEXT、LINEAR）下具有合理性，因为在这些模式下没有数据意味着无法产生填充数值。而对另外一些模式（NULL、VALUE）来说，理论上是可以产生填充数值的，至于需不需要输出填充数值，取决于应用的需求。所以为了满足这类需要强制填充数据或 NULL 的应用的需求，同时不破坏现有填充模式的行为兼容性，从 3.0.3.0 版本开始，增加了两种新的填充模式：
1. NULL_F: 强制填充 NULL 值
2. VALUE_F: 强制填充 VALUE 值
NULL, NULL_F, VALUE, VALUE_F 这几种填充模式针对不同场景区别如下：
1. INTERVAL 子句： NULL_F, VALUE_F 为强制填充模式；NULL, VALUE 为非强制模式。在这种模式下下各自的语义与名称相符
2. 流计算中的 INTERVAL 子句：NULL_F 与 NULL 行为相同，均为非强制模式；VALUE_F 与 VALUE 行为相同，均为非强制模式。即流计算中的 INTERVAL 没有强制模式
3. INTERP 子句：NULL 与 NULL_F 行为相同，均为强制模式；VALUE 与 VALUE_F 行为相同，均为强制模式。即 INTERP 中没有非强制模式。

##### 4.3.1.6 伪列

伪列的行为表现与普通数据列相似但其并不实际存储在表中。可以查询伪列，但不能对其做插入、更新和删除的操作。伪列有点像没有参数的函数。TDengine 支持的伪列包括：
1. **TBNAME** 可以视为超级表中一个特殊的标签，代表子表的表名。
2. **_QSTART/_QEND**
_qstart 和_qend 表示用户输入的查询时间范围，即 WHERE 子句中主键时间戳条件所限定的时间范围。
1. **_WSTART/_WEND/_WDURATION** _wstart 伪列、_wend 伪列和_wduration 伪列 _wstart 表示窗口起始时间戳，_wend 表示窗口结束时间戳，_wduration 表示窗口持续时长。
这三个伪列只能用于时间窗口的窗口切分查询之中，且要在窗口切分子句之后出现。
1. **_c0/_ROWTS**
TDengine 中，所有表的第一列都必须是时间戳类型，且为其主键，_rowts 伪列和_c0 伪列均代表了此列的值。相比实际的主键时间戳列，使用伪列更加灵活，语义也更加标准。例如，可以和 max\min 等函数一起使用。
```sql
select _rowts, max(current) from meters;
```

1. **_IROWTS**
_irowts 伪列只能与 interp 函数一起使用，用于返回 interp 函数插值结果对应的时间戳列。
```sql
select _irowts, interp(current) from meters range('2020-01-01 10:00:00', '2020-01-01 10:30:00') every(1s) fill(linear);
```

##### 4.3.1.7 GROUP BY

如果在语句中同时指定了 GROUP BY 子句，那么 SELECT 列表只能包含如下表达式：
1. 常量
2. 聚集函数
3. 与 GROUP BY 后表达式相同的表达式。
4. 包含前面表达式的表达式
GROUP BY 子句对每行数据按 GROUP BY 后的表达式的值进行分组，并为每个组返回一行汇总信息。
GROUP BY 子句中可以通过指定表或视图的列名来按照表或视图中的任何列分组，这些列不需要出现在 SELECT 列表中。
GROUP BY 子句中可以使用位置语法，位置标识为正整数，从 1 开始，表示使用 SELECT 列表的第几个表达式进行分组。
GROUP BY 子句中可以使用结果集列名，表示使用 SELECT 列表的指定表达式进行分组。
GROUP BY 子句中在使用位置语法和结果集列名进行分组时，其对应的 SELECT 列表中的表达式不能是聚集函数。
该子句对行进行分组，但不保证结果集的顺序。若要对分组进行排序，请使用 ORDER BY 子句

##### 4.3.1.8 ORDER BY

ORDER BY 子句对结果集排序。如果没有指定 ORDER BY，无法保证同一语句多次查询的结果集返回顺序一致。
ORDER BY 后可以使用位置语法，位置标识为正整数，从 1 开始，表示使用 SELECT 列表的第几个表达式进行排序。
ASC 表示升序，DESC 表示降序。
NULLS 语法用来指定 NULL 值在排序中输出的位置。NULLS LAST 是升序的默认值，NULLS FIRST 是降序的默认值。

##### 4.3.1.9 LIMIT

LIMIT 控制输出条数，OFFSET 指定从第几条之后开始输出。LIMIT/OFFSET 对结果集的执行顺序在 ORDER BY 之后。LIMIT 5 OFFSET 2 可以简写为 LIMIT 2, 5，都输出第 3 行到第 7 行数据。
在有 PARTITION BY/GROUP BY 子句时，LIMIT 控制的是每个切分的分片中的输出，而不是总的结果集输出。

##### 4.3.1.10 SLIMIT

SLIMIT 和 PARTITION BY/GROUP BY 子句一起使用，用来控制输出的分片的数量。SLIMIT 5 SOFFSET 2 可以简写为 SLIMIT 2, 5，都表示输出第 3 个到第 7 个分片。
需要注意，如果有 ORDER BY 子句，则输出只有一个分片。

#### 4.3.2 运算符说明

##### 4.3.2.1 算术运算符

##### 4.3.2.2 位运算符

##### 4.3.2.3 JSON 运算符

`->` 运算符可以对 JSON 类型的列按键取值。`->` 左侧是列标识符，右侧是键的字符串常量，如 `col->'name'`，返回键 `'name'` 的值。

##### 4.3.2.4 集合运算符

集合运算符将两个查询的结果合并为一个结果。包含集合运算符的查询称之为复合查询。复合查询中每条查询的选择列表中的相应表达式在数量上必须匹配，且结果类型以第一条查询为准，后续查询的结果类型必须可转换到第一条查询的结果类型，转换规则同 CAST 函数。
TDengine 支持 `UNION ALL` 和 `UNION` 操作符。UNION ALL 将查询返回的结果集合并返回，并不去重。UNION 将查询返回的结果集合并并去重后返回。在同一个 SQL 语句中，集合操作符最多支持 100 个。

##### 4.3.2.5 比较运算符

LIKE 条件使用通配符字符串进行匹配检查，规则如下：
1. '%'（百分号）匹配 0 到任意个字符；'_'（下划线）匹配单个任意 ASCII 字符。
2. 如果希望匹配字符串中原本就带有的 _（下划线）字符，那么可以在通配符字符串中写作 _，即加一个反斜线来进行转义。
3. 通配符字符串最长不能超过 100 字节。不建议使用太长的通配符字符串，否则将有可能严重影响 LIKE 操作的执行性能。
MATCH 条件和 NMATCH 条件使用正则表达式进行匹配，规则如下：
1. 支持符合 POSIX 规范的正则表达式，具体规范内容可参见[Regular Expressions](https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/V1_chap09.html)
2. MATCH 和正则表达式匹配时, 返回 TURE. NMATCH 和正则表达式不匹配时, 返回 TRUE.
3. 只能针对子表名（即 tbname）、字符串类型的标签值进行正则表达式过滤，不支持普通列的过滤。
4. 正则匹配字符串长度不能超过 128 字节。可以通过参数 maxRegexStringLen 设置和调整最大允许的正则匹配字符串，该参数是客户端配置参数，需要重启客户端才能生效

##### 4.3.2.6 逻辑运算符

TDengine 在计算逻辑条件时，会进行短路径优化，即对于 AND，第一个条件为 FALSE，则不再计算第二个条件，直接返回 FALSE；对于 OR，第一个条件为 TRUE，则不再计算第二个条件，直接返回 TRUE。

##### 4.3.2.7 CASE 表达式

###### 4.3.2.7.1 语法

```plaintext
CASE value WHEN compare_value THEN result [WHEN compare_value THEN result ...] [ELSE result] END
CASE WHEN condition THEN result [WHEN condition THEN result ...] [ELSE result] END
```

###### 4.3.2.7.2 说明

TDengine 通过 CASE 表达式让用户可以在 SQL 语句中使用 IF ... THEN ... ELSE 逻辑。
第一种 CASE 语法返回第一个 value 等于 compare_value 的 result，如果没有 compare_value 符合，则返回 ELSE 之后的 result，如果没有 ELSE 部分，则返回 NULL。
第二种语法返回第一个 condition 为真的 result。 如果没有 condition 符合，则返回 ELSE 之后的 result，如果没有 ELSE 部分，则返回 NULL。
CASE 表达式的返回类型为第一个 WHEN THEN 部分的 result 类型，其余 WHEN THEN 部分和 ELSE 部分，result 类型都需要可以向其转换，否则 TDengine 会报错。

###### 4.3.2.7.3 示例

某设备有三个状态码，显示其状态，语句如下：
```sql
SELECT CASE dev_status WHEN 1 THEN 'Running' WHEN 2 THEN 'Warning' WHEN 3 THEN 'Downtime' ELSE 'Unknown' END FROM dev_table;
```

统计智能电表的电压平均值，当电压小于 200 或大于 250 时认为是统计有误，修正其值为 220，语句如下：
```sql
SELECT AVG(CASE WHEN voltage < 200 or voltage > 250 THEN 220 ELSE voltage END) FROM meters;
```

#### 4.3.3 函数说明

##### 4.3.3.1 数学函数

###### 4.3.3.1.1 ABS

```sql
ABS(expr)
```

**功能说明**：获得指定字段的绝对值。
**返回结果类型**：与指定字段的原始数据类型一致。
**适用数据类型**：数值类型。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。
**使用说明**：只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。

###### 4.3.3.1.2 ACOS

```sql
ACOS(expr)
```

**功能说明**：获得指定字段的反余弦结果。
**返回结果类型**：DOUBLE。
**适用数据类型**：数值类型。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。
**使用说明**：只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。

###### 4.3.3.1.3 ASIN

```sql
ASIN(expr)
```

**功能说明**：获得指定字段的反正弦结果。
**返回结果类型**：DOUBLE。
**适用数据类型**：数值类型。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。
**使用说明**：只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。

###### 4.3.3.1.4 ATAN

```sql
ATAN(expr)
```

**功能说明**：获得指定字段的反正切结果。
**返回结果类型**：DOUBLE。
**适用数据类型**：数值类型。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。
**使用说明**：只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。

###### 4.3.3.1.5 CEIL

```sql
CEIL(expr)
```

**功能说明**：获得指定字段的向上取整数的结果。
**返回结果类型**：与指定字段的原始数据类型一致。
**适用数据类型**：数值类型。
**适用于**: 表和超级表。
**嵌套子查询支持**：适用于内层查询和外层查询。
**使用说明**: 只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。

###### 4.3.3.1.6 COS

```sql
COS(expr)
```

**功能说明**：获得指定字段的余弦结果。
**返回结果类型**：DOUBLE。
**适用数据类型**：数值类型。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。
**使用说明**：只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。

###### 4.3.3.1.7 FLOOR

```sql
FLOOR(expr)
```

**功能说明**：获得指定字段的向下取整数的结果。 其他使用说明参见 CEIL 函数描述。

###### 4.3.3.1.8 LOG

```sql
LOG(expr1[, expr2])
```

**功能说明**：获得 expr1 对于底数 expr2 的对数。如果 expr2 参数省略，则返回指定字段的自然对数值。
**返回结果类型**：DOUBLE。
**适用数据类型**：数值类型。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。
**使用说明**：只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。

###### 4.3.3.1.9 POW

```sql
POW(expr1, expr2)
```

**功能说明**：获得 expr1 的指数为 expr2 的幂。
**返回结果类型**：DOUBLE。
**适用数据类型**：数值类型。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。
**使用说明**：只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。

###### 4.3.3.1.10 ROUND

```sql
ROUND(expr)
```

**功能说明**：获得指定字段的四舍五入的结果。
**返回结果类型**：与指定字段的原始数据类型一致。
**适用数据类型**：
- `expr`：数值类型。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。
**使用说明**：
1. 若 `expr` 为 NULL，返回 NULL。
2. 由于暂未支持 DECIMAL 类型，所以该函数会用 DOUBLE 和 FLOAT 来表示包含小数的结果，但是 DOUBLE 和 FLOAT 是有精度上限的，当位数太多时使用该函数可能没有意义。
3. 只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。
**举例**：
```sql
taos> select round(8888.88);
      round(8888.88)       |
============================
      8889.000000000000000 |

```

###### 4.3.3.1.11 SIN

```sql
SIN(expr)
```

**功能说明**：获得指定字段的正弦结果。
**返回结果类型**：DOUBLE。
**适用数据类型**：数值类型。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。
**使用说明**：只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。

###### 4.3.3.1.12 SQRT

```sql
SQRT(expr)
```

**功能说明**：获得指定字段的平方根。
**返回结果类型**：DOUBLE。
**适用数据类型**：数值类型。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。
**使用说明**：只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。

###### 4.3.3.1.13 TAN

```sql
TAN(expr)
```

**功能说明**：获得指定字段的正切结果。
**返回结果类型**：DOUBLE。
**适用数据类型**：数值类型。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。
**使用说明**：只能与普通列，选择（Selection）、投影（Projection）函数一起使用，不能与聚合（Aggregation）函数一起使用。

##### 4.3.3.2 字符串函数

###### 4.3.3.2.1 CHAR_LENGTH

```sql
CHAR_LENGTH(expr)
```

**功能说明**：以字符计数的字符串长度。
**返回结果类型**：BIGINT。
**适用数据类型**：VARCHAR, NCHAR。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。
**使用说明**：
1. 与 `LENGTH()` 函数不同在于，对于多字节字符，比如中文字符， `CHAR_LENGTH()` 函数会将其算做一个字符，长度为 1，而 `LENGTH()` 会计算其字节数，长度为 3。比如 `CHAR_LENGTH('你好') = 2`， `LENGTH('你好') = 6`。
2. 如果 `expr` 为 NULL，返回 NULL。
**举例**：
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

###### 4.3.3.2.2 CONCAT

```sql
CONCAT(expr1, expr2 [, expr] ... )
```

**功能说明**：字符串连接函数。
**返回结果类型**：如果所有参数均为 VARCHAR 类型，则结果类型为 VARCHAR。如果参数包含 NCHAR 类型，则结果类型为 NCHAR。如果参数包含 NULL 值，则输出值为 NULL。
**适用数据类型**：VARCHAR, NCHAR。 该函数最小参数个数为 2 个，最大参数个数为 8 个。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。

###### 4.3.3.2.3 CONCAT_WS

```sql
CONCAT_WS(separator_expr, expr1, expr2 [, expr] ...)
```

**功能说明**：带分隔符的字符串连接函数。
**返回结果类型**：如果所有参数均为 VARCHAR 类型，则结果类型为 VARCHAR。如果参数包含 NCHAR 类型，则结果类型为NCHAR。如果参数包含NULL值，则输出值为 NULL。
**适用数据类型**：VARCHAR, NCHAR。该函数最小参数个数为 3 个，最大参数个数为 9 个。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。

###### 4.3.3.2.4 LENGTH

```sql
LENGTH(expr)
```

**功能说明**：以字节计数的长度。
**返回结果类型**：BIGINT。
**适用数据类型**：VARCHAR, NCHAR, VARBINARY。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。

###### 4.3.3.2.5 LOWER

```sql
LOWER(expr)
```

**功能说明**：将字符串参数值转换为全小写字母。
**返回结果类型**：与输入字段的原始类型相同。
**适用数据类型**：VARCHAR, NCHAR。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。

###### 4.3.3.2.6 LTRIM

```sql
LTRIM(expr)
```

**功能说明**：返回清除左边空格后的字符串。
**返回结果类型**：与输入字段的原始类型相同。
**适用数据类型**：VARCHAR, NCHAR。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。

###### 4.3.3.2.7 RTRIM

```sql
RTRIM(expr)
```

**功能说明**：返回清除右边空格后的字符串。
**返回结果类型**：与输入字段的原始类型相同。
**适用数据类型**：VARCHAR, NCHAR。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。

###### 4.3.3.2.8 SUBSTR

```sql
SUBSTR(expr, pos [, len])
```

**功能说明**：返回字符串 `expr` 在 `pos` 位置开始的子串，若指定了 `len` ，则返回在 `pos` 位置开始，长度为 `len` 的子串。
**返回结果类型**：与输入字段 `expr` 的原始类型相同。
**适用数据类型**：
1. `expr`：VARCHAR,NCHAR。
2. `pos`：整数类型。
3. `len`：整数类型。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。
**使用说明**：
1. 若 `pos` 为正数，则返回的结果为 `expr` 从左到右开始数 `pos` 位置开始的右侧的子串。
2. 若 `pos` 为负数，则返回的结果为 `expr` 从右到左开始数 `pos` 位置开始的右侧的子串。
3. 任意参数为 NULL，返回 NULL。
4. 该函数是多字节安全的。
5. 若 `len` 小于 1，返回空串。
6. `pos` 是 1-base 的，若 `pos` 为 0，返回空串。
7. 若 `pos` + `len` 大于 `len(expr)`，返回从 `pos` 开始到字符串结尾的子串，等同于执行 `substr(expr, pos)`。
**举例**：
```sql
taos> select substr('tdengine', 0);
 substr('tdengine', 0) |
===========================
                          |

taos> select substr('tdengine', 3);
 substr('tdengine', 3) |
===========================
 engine                   |

taos> select substr('tdengine', 3,3);
 substr('tdengine', 3,3) |
=============================
 eng                        |

taos> select substr('tdengine', -3,3);
 substr('tdengine', -3,3) |
==============================
 ine                         |

taos> select substr('tdengine', -3,-3);
 substr('tdengine', -3,-3) |
===============================
                              |
```

###### 4.3.3.2.9 UPPER

```sql
UPPER(expr)
```

**功能说明**：将字符串参数值转换为全大写字母。
**返回结果类型**：与输入字段的原始类型相同。
**适用数据类型**：VARCHAR, NCHAR。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。

##### 4.3.3.3 转换函数

###### 4.3.3.3.1 CAST

```sql
CAST(expr AS type_name)
```

**功能说明**：数据类型转换函数，返回 expr 转换为 type_name 指定的类型后的结果。
**返回结果类型**：CAST 中指定的类型（type_name)。
**适用数据类型**：输入参数 expr 的类型可以是除 JSON 和 VARBINARY 外的所有类型。如果 type_name 为 VARBINARY，则 expr 只能是 VARCHAR 类型。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。
**使用说明**：
1. 对于不能支持的类型转换会直接报错。
2. 对于类型支持但某些值无法正确转换的情况，对应的转换后的值以转换函数输出为准。目前可能遇到的几种情况： 
   - 字符串类型转换数值类型时可能出现的无效字符情况，例如 "a" 可能转为 0，但不会报错。 
   - 转换到数值类型时，数值大于 type_name 可表示的范围时，则会溢出，但不会报错。
   - 转换到字符串类型时，如果转换后长度超过 type_name 中指定的长度，则会截断，但不会报错。

###### 4.3.3.3.2 TO_ISO8601

```sql
TO_ISO8601(expr [, timezone])
```

**功能说明**：将时间戳转换成为 ISO8601 标准的日期时间格式，并附加时区信息。timezone 参数允许用户为输出结果指定附带任意时区信息。如果 timezone 参数省略，输出结果则附带当前客户端的系统时区信息。
**返回结果数据类型**：VARCHAR 类型。
**适用数据类型**：INTEGER, TIMESTAMP。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。
**使用说明**：
1. timezone 参数允许输入的时区格式为: [z/Z, +/-hhmm, +/-hh, +/-hh:mm]。例如，TO_ISO8601(1, "+00:00")。
2. 输入时间戳的精度由所查询表的精度确定, 若未指定表, 则精度为毫秒.

###### 4.3.3.3.3 TO_JSON

```sql
TO_JSON(str_literal)
```

**功能说明**: 将字符串常量转换为 JSON 类型。
**返回结果数据类型**: JSON。
**适用数据类型**: JSON 字符串，形如 '{ "literal" : literal }'。'{}'表示空值。键必须为字符串字面量，值可以为数值字面量、字符串字面量、布尔字面量或空值字面量。str_literal中不支持转义符。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。

###### 4.3.3.3.4 TO_UNIXTIMESTAMP

```sql
TO_UNIXTIMESTAMP(expr [, return_timestamp])

return_timestamp: {
    0
  | 1
}
```

**功能说明**：将日期时间格式的字符串转换成为时间戳。
**返回结果数据类型**：BIGINT, TIMESTAMP。
**应用字段**：VARCHAR, NCHAR。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**：表和超级表。
**使用说明**：
1. 输入的日期时间字符串须符合 ISO8601/RFC3339 标准，无法转换的字符串格式将返回 NULL。
2. 返回的时间戳精度与当前 DATABASE 设置的时间精度一致。
3. return_timestamp 指定函数返回值是否为时间戳类型，设置为1时返回 TIMESTAMP 类型，设置为0时返回 BIGINT 类型。如不指定缺省返回 BIGINT 类型。

###### 4.3.3.3.5 TO_TIMESTAMP

```sql
TO_TIMESTAMP(ts_str_literal, format_str_literal)
```

**功能说明**: 将字符串按照指定格式转化为时间戳.
**版本**: ver-3.2.2.0
**返回结果数据类型**: TIMESTAMP
**应用字段**: VARCHAR
**嵌套子查询支持**: 适用于内层查询和外层查询
**适用于**: 表和超级表
**支持的格式**: 与`to_char`相同
**使用说明**:
1. 若`ms`, `us`, `ns`同时指定, 那么结果时间戳包含上述三个字段的和. 如 `to_timestamp('2023-10-10 10:10:10.123.000456.000000789', 'yyyy-mm-dd hh:mi:ss.ms.us.ns')` 输出为 `2023-10-10 10:10:10.123456789`对应的时间戳.
2. `MONTH`, `MON`, `DAY`, `DY` 以及其他输出为数字的格式的大小写意义相同, 如 `to_timestamp('2023-JANUARY-01', 'YYYY-month-dd')`, `month`可以被替换为`MONTH` 或者`Month`.

##### 4.3.3.4 时间和日期函数

###### 4.3.3.4.1 NOW

```sql
NOW()
```

**功能说明**：返回客户端当前系统时间。
**返回结果数据类型**：TIMESTAMP。
**应用字段**：在 WHERE 或 INSERT 语句中使用时只能作用于 TIMESTAMP 类型的字段。
**适用于**：表和超级表。
**嵌套子查询支持**：适用于内层查询和外层查询。
**使用说明**：
1. 支持时间加减操作，如 NOW() + 1s, 支持的时间单位如下： b(纳秒)、u(微秒)、a(毫秒)、s(秒)、m(分)、h(小时)、d(天)、w(周)。
2. 返回的时间戳精度与当前 DATABASE 设置的时间精度一致。

###### 4.3.3.4.2 TIMEDIFF

```sql
TIMEDIFF(expr1, expr2 [, time_unit])
```

**功能说明**：返回时间戳 `expr1` - `expr2` 的结果绝对值，并近似到时间单位 `time_unit` 指定的精度。
**返回结果类型**：BIGINT。
**适用数据类型**：
1. `expr1`：表示时间戳的 BIGINT, TIMESTAMP 类型，或符合 ISO8601/RFC3339 标准的日期时间格式的 VARCHAR, NCHAR 类型。
2. `expr2`：表示时间戳的 BIGINT, TIMESTAMP 类型，或符合 ISO8601/RFC3339 标准的日期时间格式的 VARCHAR, NCHAR 类型。
3. `time_unit`：见使用说明。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**: 表和超级表。
**使用说明**：
1. 支持的时间单位 `time_unit` 如下： 1b(纳秒), 1u(微秒)，1a(毫秒)，1s(秒)，1m(分)，1h(小时)，1d(天), 1w(周)。
2. 如果时间单位 `time_unit` 未指定， 返回的时间差值精度与当前 DATABASE 设置的时间精度一致。
3. 输入包含不符合时间日期格式的字符串则返回 NULL。
4. `expr1` 或 `expr2` 为 NULL，返回 NULL。
5. `time_unit` 为 NULL，等同于未指定时间单位。
6. 输入时间戳的精度由所查询表的精度确定, 若未指定表, 则精度为毫秒.
**举例**：
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

###### 4.3.3.4.3 TIMETRUNCATE

```sql
TIMETRUNCATE(expr, time_unit [, use_current_timezone])

use_current_timezone: {
    0
  | 1
}
```

**功能说明**：将时间戳按照指定时间单位 time_unit 进行截断。
**返回结果数据类型**：TIMESTAMP。
**应用字段**：表示时间戳的 BIGINT, TIMESTAMP 类型，或符合 ISO8601/RFC3339 标准的日期时间格式的 VARCHAR, NCHAR 类型。
**适用于**：表和超级表。
**使用说明**：
1. 支持的时间单位 time_unit 如下：1b(纳秒), 1u(微秒)，1a(毫秒)，1s(秒)，1m(分)，1h(小时)，1d(天), 1w(周)。
2. 返回的时间戳精度与当前 DATABASE 设置的时间精度一致。
3. 输入时间戳的精度由所查询表的精度确定, 若未指定表, 则精度为毫秒.
4. 输入包含不符合时间日期格式的字符串则返回 NULL。
5. 当使用 1d/1w 作为时间单位对时间戳进行截断时， 可通过设置 use_current_timezone 参数指定是否根据当前时区进行截断处理。 值 0 表示使用 UTC 时区进行截断，值 1 表示使用当前时区进行截断。 例如客户端所配置时区为 UTC+0800, 则 TIMETRUNCATE('2020-01-01 23:00:00', 1d, 0) 返回结果为东八区时间 '2020-01-01 08:00:00'。 而使用 TIMETRUNCATE('2020-01-01 23:00:00', 1d, 1) 时，返回结果为东八区时间 '2020-01-01 00:00:00'。 当不指定 use_current_timezone 时，use_current_timezone 默认值为 1 。
6. 当将时间值截断到一周（1w）时，timetruncate 的计算是基于 Unix 时间戳（1970年1月1日00:00:00 UTC）进行的。Unix 时间戳始于星期四， 因此所有截断后的日期都是星期四。

###### 4.3.3.4.4 TIMEZONE

```sql
TIMEZONE()
```

**功能说明**：返回客户端当前时区信息。
**返回结果数据类型**：VARCHAR。
**应用字段**：无
**适用于**：表和超级表。

###### 4.3.3.4.5 TODAY

```sql
TODAY()
```

**功能说明**：返回客户端当日零时的系统时间。
**返回结果数据类型**：TIMESTAMP。
**应用字段**：在 WHERE 或 INSERT 语句中使用时只能作用于 TIMESTAMP 类型的字段。
**适用于**：表和超级表。
**使用说明**：
1. 支持时间加减操作，如 TODAY() + 1s, 支持的时间单位如下：b(纳秒)，u(微秒)，a(毫秒)，s(秒)，m(分)，h(小时)，d(天)，w(周)。
2. 返回的时间戳精度与当前 DATABASE 设置的时间精度一致。

##### 4.3.3.5 聚合函数

###### 4.3.3.5.1 AVG

```sql
AVG(expr)
```

**功能说明**：统计指定字段的平均值。
**返回数据类型**：DOUBLE。
**适用数据类型**：数值类型。
**适用于**：表和超级表。

###### 4.3.3.5.2 COUNT

```sql
COUNT({* | expr})
```

**功能说明**：统计指定字段的记录行数。
**返回数据类型**：BIGINT。
**适用数据类型**：全部类型字段。
**适用于**：表和超级表。
**使用说明**:
1. 可以使用星号(*)来替代具体的字段，使用星号(*)返回全部记录数量。
2. 如果统计字段是具体的列，则返回该列中非 NULL 值的记录数量。

###### 4.3.3.5.3 STDDEV

```sql
STDDEV(expr)
```

**功能说明**：统计表中某列的总体标准差。
**返回数据类型**：DOUBLE。
**适用数据类型**：数值类型。
**适用于**：表和超级表。
**举例**：
```sql
taos> select id from test_stddev;
     id      |
==============
           1 |
           2 |
           3 |
           4 |
           5 |

taos> select stddev(id) from test_stddev;
      stddev(id)       |
============================
         1.414213562373095 |
```

###### 4.3.3.5.4 SUM

```sql
SUM(expr)
```

**功能说明**：统计表/超级表中某列的和。
**返回数据类型**：DOUBLE, BIGINT。
**适用数据类型**：数值类型。
**适用于**：表和超级表。

###### 4.3.3.5.5 HYPERLOGLOG

```sql
HYPERLOGLOG(expr)
```

**功能说明**：
1. 采用 hyperloglog 算法，返回某列的基数。该算法在数据量很大的情况下，可以明显降低内存的占用，求出来的基数是个估算值，标准误差（标准误差是多次实验，每次的平均数的标准差，不是与真实结果的误差）为 0.81%。
2. 在数据量较少的时候该算法不是很准确，可以使用 select count(data) from (select unique(col) as data from table) 的方法。
**返回结果类型**：INTEGER。
**适用数据类型**：任何类型。
**适用于**：表和超级表。

###### 4.3.3.5.6 HISTOGRAM

```sql
HISTOGRAM(expr，bin_type, bin_description, normalized)
```

**功能说明**：统计数据按照用户指定区间的分布。
**返回结果类型**：如归一化参数 normalized 设置为 1，返回结果为 DOUBLE 类型，否则为 BIGINT 类型。
**适用数据类型**：数值型字段。
**适用于**: 表和超级表。
**详细说明**：
1. bin_type 用户指定的分桶类型, 有效输入类型为 "user_input“, ”linear_bin", "log_bin"。
2. bin_description 描述如何生成分桶区间，针对三种桶类型，分别为以下描述格式(均为 JSON 格式字符串)：
   - "user_input": "[1, 3, 5, 7]" 用户指定 bin 的具体数值。
   - "linear_bin": "{"start": 0.0, "width": 5.0, "count": 5, "infinity": true}" "start" 表示数据起始点，"width" 表示每次 bin 偏移量, "count" 为 bin 的总数，"infinity" 表示是否添加（-inf, inf）作为区间起点和终点， 生成区间为[-inf, 0.0, 5.0, 10.0, 15.0, 20.0, +inf]。
   - "log_bin": "{"start":1.0, "factor": 2.0, "count": 5, "infinity": true}" "start" 表示数据起始点，"factor" 表示按指数递增的因子，"count" 为 bin 的总数，"infinity" 表示是否添加（-inf, inf）作为区间起点和终点， 生成区间为[-inf, 1.0, 2.0, 4.0, 8.0, 16.0, +inf]。
3. normalized 是否将返回结果归一化到 0~1 之间 。有效输入为 0 和 1。

###### 4.3.3.5.7 PERCENTILE

```sql
PERCENTILE(expr, p [, p1] ... )
```

**功能说明**：统计表中某列的值百分比分位数。
**返回数据类型**： 该函数最小参数个数为 2 个，最大参数个数为 11 个。可以最多同时返回 10 个百分比分位数。当参数个数为 2 时，返回一个分位数， 类型为 DOUBLE，当参数个数大于 2 时，返回类型为 VARCHAR, 格式为包含多个返回值的 JSON 数组。
**应用字段**：数值类型。
**适用于**：表。
**使用说明**：
1. P 值取值范围 0≤P≤100，为 0 的时候等同于 MIN，为 100 的时候等同于 MAX;
2. 同时计算针对同一列的多个分位数时，建议使用一个 PERCENTIL E函数和多个参数的方式，能很大程度上降低查询的响应时间。 比如，使用查询SELECT percentile(col, 90, 95, 99) FROM table, 性能会优于 SELECT percentile(col, 90), percentile(col, 95), percentile(col, 99) from table。

##### 4.3.3.6 选择函数

选择函数是 TDengine 扩展支持的一类时序数据库函数，可以在一列数据中获取选择结果的同时输出其他列的值。

###### 4.3.3.6.1 BOTTOM

```sql
BOTTOM(expr, k)
```

**功能说明**：统计表/超级表中某列的值最小 *k* 个非 NULL 值。如果多条数据取值一样，全部取用又会超出 k 条限制时，系统会从相同值中随机选取符合要求的数量返回。
**返回数据类型**：同应用的字段。
**适用数据类型**：数值类型。
**适用于**：表和超级表。
**使用说明**:
1. k 值取值范围 1≤k≤100；
2. 系统同时返回该记录关联的时间戳列；
3. 限制：BOTTOM 函数不支持 FILL 子句。

###### 4.3.3.6.2 FIRST

```sql
FIRST(expr)
```

**功能说明**：统计表/超级表中某列的值最先写入的非 NULL 值。
**返回数据类型**：同应用的字段。
**适用数据类型**：所有字段。
**适用于**：表和超级表。
**使用说明**:
1. 如果要返回各个列的首个（时间戳最小）非 NULL 值，可以使用 FIRST(*)；查询超级表，且multiResultFunctionStarReturnTags设置为 0 (默认值) 时，FIRST(*) 只返回超级表的普通列；设置为 1 时，返回超级表的普通列和标签列。
2. 如果结果集中的某列全部为 NULL 值，则该列的返回结果也是 NULL；
3. 如果结果集中所有列全部为 NULL 值，则不返回结果。
4. 对于存在复合主键的表的查询，若最小时间戳的数据有多条，则只有对应的复合主键最小的数据被返回。

###### 4.3.3.6.3 MAX

```sql
MAX(expr)
```

**功能说明**：统计表/超级表中某列的值最大值。
**返回数据类型**：同应用的字段。
**适用数据类型**：数值类型。
**适用于**：表和超级表。

###### 4.3.3.6.4 MIN

```sql
MIN(expr)
```

**功能说明**：统计表/超级表中某列的值最小值。
**返回数据类型**：同应用的字段。
**适用数据类型**：数值类型。
**适用于**：表和超级表。

###### 4.3.3.6.5 MODE

```sql
MODE(expr)
```

**功能说明**：返回出现频率最高的值，若存在多个频率相同的最高值，则随机输出其中某个值。
**返回数据类型**：与输入数据类型一致。
**适用数据类型**：全部类型字段。
**适用于**：表和超级表。

###### 4.3.3.6.6 SAMPLE

```sql
SAMPLE(expr, k)
```

**功能说明**：获取数据的 k 个采样值。参数 k 的合法输入范围是 1≤ k ≤ 1000。
**返回结果类型**：同原始数据类型。
**适用数据类型**：全部类型字段。
**嵌套子查询支持**：适用于内层查询和外层查询。
**适用于**：表和超级表。

###### 4.3.3.6.7 TAIL

```sql
TAIL(expr, k [, offset_rows])
```

**功能说明**：返回跳过最后 offset_val 个，然后取连续 k 个记录，不忽略 NULL 值。offset_val 可以不输入。此时返回最后的 k 个记录。当有 offset_val 输入的情况下，该函数功能等效于 `order by ts desc LIMIT k OFFSET offset_val`。
**参数范围**：k: [1,100] offset_val: [0,100]。
**返回数据类型**：同应用的字段。
**适用数据类型**：适合于除时间主键列外的任何类型。
**适用于**：表、超级表。

###### 4.3.3.6.8 TOP

```sql
TOP(expr, k)
```

**功能说明**：统计表/超级表中某列的值最大 k 个非 NULL 值。如果多条数据取值一样，全部取用又会超出 k 条限制时，系统会从相同值中随机选取符合要求的数量返回。
**返回数据类型**：同应用的字段。
**适用数据类型**：数值类型。
**适用于**：表和超级表。
**使用说明**:
1. k* *值取值范围 1≤k≤100；
2. 系统同时返回该记录关联的时间戳列；
3. 限制：TOP 函数不支持 FILL 子句。

###### 4.3.3.6.9 UNIQUE

```sql
UNIQUE(expr)
```

**功能说明**：返回该列数据去重后的值。该函数功能与 distinct 相似。对于相同的数据，返回时间戳最小的一条，对于存在复合主键的表的查询，若最小时间戳的数据有多条，则只有对应的复合主键最小的数据被返回。
**返回数据类型**：同应用的字段。
**适用数据类型**：全部类型字段。
**适用于**: 表和超级表。

##### 4.3.3.7 系统或会话信息函数

###### 4.3.3.7.1 DATABASE

```sql
SELECT DATABASE();
```

**说明**：返回当前登录的数据库。如果登录的时候没有指定默认数据库，且没有使用USE命令切换数据库，则返回NULL。

###### 4.3.3.7.2 CLIENT_VERSION

```sql
SELECT CLIENT_VERSION();
```

**说明**：返回客户端版本。

###### 4.3.3.7.3 SERVER_VERSION

```sql
SELECT SERVER_VERSION();
```

**说明**：返回服务端版本。

###### 4.3.3.7.4 SERVER_STATUS

```sql
SELECT SERVER_STATUS();
```

**说明**：检测服务端是否所有 dnode 都在线，如果是则返回成功，否则返回无法建立连接的错误。如果想要查询集群的状态，推荐使用 `SHOW CLUSTER ALIVE;`, 与 `SELECT SERVER_STATUS();` 不同，当集群中的部分节点不可用时，它不会返回错误，而是返回不同的状态码。

###### 4.3.3.7.5 CURRENT_USER

```sql
SELECT CURRENT_USER();
```

**说明**：获取当前登录用户。

##### 4.3.3.8 Geometry 函数

###### 4.3.3.8.1 ST_GeomFromText

```sql
ST_GeomFromText(VARCHAR WKT expr)
```

**功能说明**：根据 Well-Known Text (WKT) 表示从指定的几何值创建几何数据。
**返回值类型**：GEOMETRY
**适用数据类型**：VARCHAR
**适用表类型**：标准表和超表
**使用说明**：输入可以是 WKT 字符串之一，例如点（POINT）、线串（LINESTRING）、多边形（POLYGON）、多点集（MULTIPOINT）、多线串（MULTILINESTRING）、多多边形（MULTIPOLYGON）、几何集合（GEOMETRYCOLLECTION）。输出是以二进制字符串形式定义的 GEOMETRY 数据类型。

###### 4.3.3.8.2 ST_AsText

```sql
ST_AsText(GEOMETRY geom)
```

**功能说明**：从几何数据中返回指定的 Well-Known Text (WKT) 表示。
**返回值类型**：VARCHAR
**适用数据类型**：GEOMETRY
**适用表类型**：标准表和超表
**使用说明**：输出可以是 WKT 字符串之一，例如点（POINT）、线串（LINESTRING）、多边形（POLYGON）、多点集（MULTIPOINT）、多线串（MULTILINESTRING）、多多边形（MULTIPOLYGON）、几何集合（GEOMETRYCOLLECTION）。

###### 4.3.3.8.3 ST_Intersects

```sql
ST_Intersects(GEOMETRY geomA, GEOMETRY geomB)
```

**功能说明**：比较两个几何对象，并在它们相交时返回 true。
**返回值类型**：BOOL
**适用数据类型**：GEOMETRY，GEOMETRY
**适用表类型**：标准表和超表
**使用说明**：如果两个几何对象有任何一个共享点，则它们相交。

###### 4.3.3.8.4 ST_Equals

```sql
ST_Equals(GEOMETRY geomA, GEOMETRY geomB)
```

**功能说明**：如果给定的几何对象是"空间相等"的，则返回 TRUE。
**返回值类型**：BOOL
**适用数据类型**：GEOMETRY，GEOMETRY
**适用表类型**：标准表和超表
**使用说明**："空间相等"意味着 ST_Contains(A,B) = true 和 ST_Contains(B,A) = true，并且点的顺序可能不同，但表示相同的几何结构。

###### 4.3.3.8.5 ST_Touches

```sql
ST_Touches(GEOMETRY geomA, GEOMETRY geomB)
```

**功能说明**：如果 A 和 B 相交，但它们的内部不相交，则返回 TRUE。
**返回值类型**：BOOL
**适用数据类型**：GEOMETRY，GEOMETRY
**适用表类型**：标准表和超表
**使用说明**：A 和 B 至少有一个公共点，并且这些公共点位于至少一个边界中。对于点/点输入，关系始终为 FALSE，因为点没有边界。

###### 4.3.3.8.6 ST_Covers

```sql
ST_Covers(GEOMETRY geomA, GEOMETRY geomB)
```

**功能说明**：如果 B 中的每个点都位于几何形状 A 内部（与内部或边界相交），则返回 TRUE。
**返回值类型**：BOOL
**适用数据类型**：GEOMETRY，GEOMETRY
**适用表类型**：标准表和超表
**使用说明**：A 包含 B 意味着 B 中的没有点位于 A 的外部（在外部）。

###### 4.3.3.8.7 ST_Contains

```sql
ST_Contains(GEOMETRY geomA, GEOMETRY geomB)
```

**功能说明**：如果 A 包含 B，描述：如果几何形状 A 包含几何形状 B，则返回 TRUE。
**返回值类型**：BOOL
**适用数据类型**：GEOMETRY，GEOMETRY
**适用表类型**：标准表和超表
**使用说明**：A 包含 B 当且仅当 B 的所有点位于 A 的内部（即位于内部或边界上）（或等效地，B 的没有点位于 A 的外部），并且 A 和 B 的内部至少有一个公共点。

###### 4.3.3.8.8 ST_ContainsProperly

```sql
ST_ContainsProperly(GEOMETRY geomA, GEOMETRY geomB)
```

**功能说明**：如果 B 的每个点都位于 A 内部，则返回 TRUE。
**返回值类型**：BOOL
**适用数据类型**：GEOMETRY，GEOMETRY
**适用表类型**：标准表和超表
**使用说明**：B 的没有点位于 A 的边界或外部。

#### 4.3.4 关联查询

在之前的版本中 TDengine 只支持内连接，为了更好的支持客户需求，自 3.3.0.0 版本起 TDengine 开始支持更为广泛的 JOIN 类型，这其中既包括传统数据库中的 LEFT JOIN、RIGHT JOIN、FULL JOIN、SEMI JOIN、ANTI-SEMI JOIN，也包括时序库中特色的 ASOF JOIN、WINDOW JOIN。JOIN 操作可以在子表、普通表、超级表以及子查询间进行。
所有支持的 Join 组合如下表所示：

|  | None | Semi | Anti-Semi | ASOF | Window |
| --- | --- | --- | --- | --- | --- |
| Inner | 支持 | / | / | / | / |
| Left | 支持 | 支持 | 支持 | 支持 | 支持 |
| Right | 支持 | 支持 | 支持 | 支持 | 支持 |
| Full | 支持 | / | / | / | / |

*注：上表中之所以存在一些不支持的 Join 组合，主要原因是这些 Join 类型（Semi、Anti-Semi、ASOF、Window）是有明确的单向性的，其查询结果也跟这种单向性紧密相关，因此 Inner Join 或 Full Join 与他们的组合将产生矛盾性。以 Semi Join 为例，通常表达的是操作符 IN 的语义，查询结果也通常只有源表的数据，当同时存在反向查询时，这种查询结果将失去意义。因此对于这些组合来说 TDengine 不提供显式支持，如果需要可以通过 SQL 语句的组合来实现，例如通过 UNION 语句获得更精确的双向语义。*
自 3.3.0.0 版本起 TDengine 还将支持主键列进行 timetruncate 后进行关联查询。通过这个功能增强，可以将一系列的相关时间戳转换为同一个时间戳，进而在关联查询时可以实现按时间区间 Join 的目的。

##### 4.3.4.1 Inner Join

**含义**
内连接 - 只有左右表中同时符合连接条件的数据才会被返回，可以视为两个表符合连接条件的数据的交集。
**语法**
```sql
SELECT ... FROM table_name1 [INNER] JOIN table_name2 [ON ...] [WHERE ...] [...]
或
SELECT ... FROM table_name1, table_name2 WHERE ... [...]
```

**结果集**
符合连接条件的左右表行数据的笛卡尔积集合。
**适用范围**
支持超级表、普通表、子表、子查询间 Inner Join。
**说明**
- 对于第一种语法，INNER 关键字可选， ON 和/或 WHERE 中可以指定主连接条件和其他连接条件，WHERE 中还可以指定过滤条件，ON/WHERE 两者至少指定一个；
- 对于第二种语法，可以在 WHERE 中指定主连接条件、其他连接条件、过滤条件；
- 对超级表进行 Inner Join 时，与主连接条件 AND 关系的 Tag 列等值条件将作为类似分组条件使用，因此输出结果不能保持有序。
**示例**

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.col1, b.col1 FROM tba1 a JOIN tba2 b ON a.ts = b.ts | <grid cols="2"> <column width="50"> 1 4 </column> <column width="50"> 2 5 </column> </grid> |
| SELECT a.col1, b.col1 FROM sta a JOIN sta b ON a.ts = b.ts AND a.ts < '2023-11-17 16:29:02' | <grid cols="2"> <column width="50"> 1 1 2 2 3 </column> <column width="50"> 1 2 1 2 3 </column> </grid> |

##### 4.3.4.2 Left Outer Join

**含义**
左（外）连接 - 既包含左右表同时符合连接条件的数据集合，也包括左表中不符合连接条件的数据集合。
**语法**
```sql
SELECT ... FROM table_name1 LEFT [OUTER] JOIN table_name2 ON ... [WHERE ...] [...]
```

**结果集**
Inner Join 的结果集 + 左表中不符合连接条件的行和右表的空数据（NULL）组成的行数据集合。
**适用范围**
支持超级表、普通表、子表、子查询间 Left Join。
**说明**
- OUTER 关键字可选；
**示例**

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.col1, b.col1 FROM sta a LEFT JOIN sta b ON a.ts = b.ts AND a.ts < '2023-11-17 16:29:02' AND b.ts < '2023-11-17 16:29:01' | <grid cols="2"> <column width="50"> 1 1 2 2 3 3 4 5 5 7 </column> <column width="50"> 1 2 1 2 NULL NULL NULL NULL NULL NULL </column> </grid> |
| SELECT a.col1, b.col1 FROM sta a LEFT JOIN sta b ON a.ts = b.ts WHERE a.ts < '2023-11-17 16:29:02' AND b.ts < '2023-11-17 16:29:01' ORDER BY a.col1, b.col1; | <grid cols="2"> <column width="50"> 1 1 2 2 </column> <column width="50"> 1 2 1 2 </column> </grid> |

##### 4.3.4.3 Left Semi Join

**含义**
左半连接 - 通常表达的是 IN/EXISTS 的含义，即对左表任意一条数据来说，只有当右表中存在任一符合连接条件的数据时才返回左表行数据。
**语法**
```sql
SELECT ... FROM table_name1 LEFT SEMI JOIN table_name2 ON ... [WHERE ...] [...]
```

**结果集**
左表中符合连接条件的行和右表任一符合连接条件的行组成的行数据集合。
**适用范围**
支持超级表、普通表、子表、子查询间 Left Semi Join。
**示例**

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.ts, b.ts FROM sta a LEFT SEMI JOIN sta b ON a.ts = b.ts AND a.ts < '2023-11-17 16:29:02' | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:00 2023-11-17 16:29:01 </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:00 2023-11-17 16:29:01 </column> </grid> |

##### 4.3.4.4 Left Anti-Semi Join

**含义**
左反连接 - 同左半连接的逻辑正好相反，通常表达的是 NOT IN/NOT EXISTS 的含义，即对左表任意一条数据来说，只有当右表中不存在任何符合连接条件的数据时才返回左表行数据。
**语法**
```sql
SELECT ... FROM table_name1 LEFT ANTI JOIN table_name2 ON ... [WHERE ...] [...]
```

**结果集**
左表中不符合连接条件的行和右表的空数据（NULL）组成的行数据集合。
**适用范围**
支持超级表、普通表、子表、子查询间 Left Anti-Semi Join。
**示例**

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.ts, b.ts FROM tba1 a LEFT ANTI JOIN tba2 b ON a.ts = b.ts | <grid cols="2"> <column width="50"> 2023-11-17 16:29:02 2023-11-17 16:29:04 </column> <column width="50"> NULL NULL </column> </grid> |

##### 4.3.4.5 Right Outer Join

**含义**
右（外）连接 - 既包含左右表同时符合连接条件的数据集合，也包括右表中不符合连接条件的数据集合。
**语法**
```sql
SELECT ... FROM table_name1 RIGHT [OUTER] JOIN table_name2 ON ... [WHERE ...] [...]
```

**结果集**
Inner Join 的结果集 + 右表中不符合连接条件的行和左表的空数据（NULL）组成的行数据集合。
**适用范围**
支持超级表、普通表、子表、子查询间 Right Join。
**说明**
- OUTER 关键字可选；
**示例**

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.col1, b.col1 FROM sta a RIGHT JOIN sta b ON a.ts = b.ts and b.ts < '2023-11-17 16:29:02' AND a.ts < '2023-11-17 16:29:01' | <grid cols="2"> <column width="50"> 1 2 1 2 NULL NULL NULL NULL NULL NULL </column> <column width="50"> 1 1 2 2 3 3 4 5 5 7 </column> </grid> |

##### 4.3.4.6 Right Semi Join

**含义**
右半连接 - 通常表达的是 IN/EXISTS 的含义，即只有当左表中存在任一符合连接条件的数据时才返回右表行数据。
**语法**
```sql
SELECT ... FROM table_name1 RIGHT SEMI JOIN table_name2 ON ... [WHERE ...] [...]
```

**结果集**
右表中符合连接条件的行和左表任一符合连接条件的行组成的行数据集合。
**适用范围**
支持超级表、普通表、子表、子查询间 Right Semi Join。
**示例**

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.ts, b.ts FROM sta a RIGHT SEMI JOIN sta b ON a.ts = b.ts AND b.ts < '2023-11-17 16:29:02' | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:00 2023-11-17 16:29:01 </column> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:00 2023-11-17 16:29:01 </column> </grid> |

##### 4.3.4.7 Right Anti-Semi Join

**含义**
右反连接 - 同右半连接的逻辑正好相反，通常表达的是 NOT IN/NOT EXISTS 的含义，即只有当左表中不存在任何符合连接条件的数据时才返回右表行数据。
**语法**
```sql
SELECT ... FROM table_name1 RIGHT ANTI JOIN table_name2 ON ... [WHERE ...] [...]
```

**结果集**
右表中不符合连接条件的行和左表的空数据（NULL）组成的行数据集合。
**适用范围**
支持超级表、普通表、子表、子查询间 Right Anti-Semi Join。
**示例**

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.ts, b.ts FROM tba1 a RIGHT ANTI JOIN tba2 b ON a.ts = b.ts | <grid cols="2"> <column width="50"> NULL NULL </column> <column width="50"> 2023-11-17 16:29:01 2023-11-17 16:29:05 </column> </grid> |

##### 4.3.4.8 Full Outer Join

**含义**
全（外）连接 - 既包含左右表同时符合连接条件的数据集合，也包括左右表中不符合连接条件的数据集合。
**语法**
```sql
SELECT ... FROM table_name1 FULL [OUTER] JOIN table_name2 ON ... [WHERE ...] [...]
```

**结果集**
Inner Join 的结果集 + 左表中不符合连接条件的行加上右表的空数据组成的行数据集合 + 右表中不符合连接条件的行加上左表的空数据组成的行数据集合。
**适用范围**
支持超级表、普通表、子表、子查询间 Full Outer Join。
**说明**
- OUTER 关键字可选；
**示例**

| SQL 语句 | 查询结果 |
| --- | --- |
| SELECT a.ts, b.ts FROM tba1 a FULL JOIN tba2 b ON a.ts = b.ts AND a.ts < '2023-11-17 16:29:03' AND b.ts < '2023-11-17 16:29:03' | <grid cols="2"> <column width="50"> 2023-11-17 16:29:00 2023-11-17 16:29:02 NULL 2023-11-17 16:29:03 2023-11-17 16:29:04 NULL NULL </column> <column width="50"> 2023-11-17 16:29:00 NULL 2023-11-17 16:29:01 NULL NULL 2023-11-17 16:29:03 2023-11-17 16:29:05 </column> </grid> |

普通表与普通表之间的 JOIN 操作：
```sql
SELECT *
FROM temp_tb_1 t1, pressure_tb_1 t2
WHERE t1.ts = t2.ts
```

超级表与超级表之间的 LEFT JOIN 操作：
```sql
SELECT *
FROM temp_stable t1 LEFT JOIN temp_stable t2
ON t1.ts = t2.ts AND t1.deviceid = t2.deviceid AND t1.status=0;
```

子表与超级表之间的 LEFT ASOF JOIN 操作：
```sql
SELECT *
FROM temp_ctable t1 LEFT ASOF JOIN temp_stable t2
ON t1.ts = t2.ts AND t1.deviceid = t2.deviceid;
```

#### 4.3.5 嵌套查询

“嵌套查询”又称为“子查询”，也即在一条 SQL 语句中，“内层查询”的计算结果可以作为“外层查询”的计算对象来使用。
TDengine 的查询引擎支持在 FROM 子句中使用非关联子查询（“非关联”的意思是，子查询不会用到父查询中的参数）。也即在普通 SELECT 语句的 tb_name_list 位置，用一个独立的 SELECT 语句来代替（这一 SELECT 语句被包含在英文圆括号内），于是完整的嵌套查询 SQL 语句形如：
```plaintext
SELECT ... FROM (SELECT ... FROM ...) ...;
```

信息
1. 内层查询的返回结果将作为“临时表”供外层查询使用，此临时表建议起别名，以便于外层查询中方便引用。
2. 外层查询支持直接通过列名引用内层查询的列或伪列。
3. 内层查询支持的功能特性与非嵌套的查询语句能力是一致的。

#### 4.3.6 UNION 子句

```plaintext
SELECT ...
UNION [ALL] SELECT ...
[UNION [ALL] SELECT ...]
```

TDengine 支持 UNION/UNION ALL 操作符。也就是说，如果多个 SELECT 子句返回结果集的结构完全相同（列名、列类型、列数、顺序），那么可以通过 UNION/UNION ALL 把这些结果集合并到一起。

#### 4.3.7 视图

视图（View）本质上是一个存储在数据库中的查询语句。视图（非物化视图）本身不包含数据，只有在从视图读取数据时才动态执行视图所指定的查询语句。我们在创建视图时指定一个名称，然后可以像使用普通表一样对其进行查询等操作。视图的使用需遵循以下规则：
1. 视图可以嵌套定义和使用，视图与创建时指定的或当前数据库绑定使用。
2. 在同一个数据库内，视图名称不允许重名，视图名跟表名也推荐不重名（不强制）。当出现视图与表名重名时，写入、查询、授权、回收权限等操作优先使用同名表。

##### 4.3.7.1 创建（更新）视图

```plaintext
CREATE [ OR REPLACE ] VIEW [db_name.]view_name AS query
```

说明：
1. 创建视图时可以指定视图绑定的数据库名（*db_name*），未明确指定时默认为当前连接绑定的数据库；
2. 查询语句（*query*）中推荐指定数据库名，支持跨库视图，未指定时默认为与视图绑定的数据库(有可能非当前连接指定的数据库)；

##### 4.3.7.2 查看视图

1. 查看某个数据库下的所有视图
```sql
SHOW [db_name.]VIEWS;
```

1. 查看视图的创建语句
```sql
SHOW CREATE VIEW [db_name.]view_name;
```

1. 查看视图列信息
```sql
DESCRIBE [db_name.]view_name;
```

1. 查看所有视图信息
```sql
SELECT ... FROM information_schema.ins_views;
```

##### 4.3.7.3 修改视图

```plaintext
ALTER VIEW [ IF EXISTS ] view_name ALTER [ COLUMN ] column_name SET DEFAULT expression
ALTER VIEW [ IF EXISTS ] view_name ALTER [ COLUMN ] column_name DROP DEFAULT
```

##### 4.3.7.4 删除视图

```sql
DROP VIEW [IF EXISTS] [db_name.]view_name;
```

**SQL 示例**
1. 建表
```sql
CREATE TABLE tb1 (ts TIMESTAMP, col1 INT, col2 FLOAT, col3 BINARY(50));
```

1. 查询
查询 tb1 刚过去的一个小时的所有记录：
```sql
SELECT * FROM tb1 WHERE ts >= NOW - 1h;
```

查询表 tb1 从 2018-06-01 08:00:00.000 到 2018-06-02 08:00:00.000 时间范围，并且 col3 的字符串是'nny'结尾的记录，结果按照时间戳降序：
```sql {wrap}
SELECT * FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' AND ts <= '2018-06-02 08:00:00.000' AND col3 LIKE '%nny' ORDER BY ts DESC;
```

查询 col1 与 col2 的和，并取名 complex, 时间大于 2018-06-01 08:00:00.000, col2 大于 1.2，结果输出仅仅 10 条记录，从第 5 条开始：
```sql {wrap}
SELECT (col1 + col2) AS 'complex' FROM tb1 WHERE ts > '2018-06-01 08:00:00.000' AND col2 > 1.2 LIMIT 10 OFFSET 5;
```

查询过去 10 分钟的记录，col2 的值大于 3.14，并且将结果输出到文件 `/home/testoutput.csv`：
```sql {wrap}
SELECT COUNT(*) FROM tb1 WHERE ts >= NOW - 10m AND col2 > 3.14 >> /home/testoutput.csv;
```

1. 创建视图
```sql
CREATE VIEW view1 AS SELECT _wstart, count(*) FROM table1 INTERVAL(1d);
CREATE VIEW view2 AS SELECT ts, col2 FROM table1;
CREATE VIEW view3 AS SELECT * from view1;
```

1. 查看视图
```sql
SHOW VIEWS；
```

1. 视图查询
```sql
SELECT * from view1;
```

### 4.4 查询内存管控

查询分配的内存不会超过内存使用上限；能够按照TASK、QUERY、全局统计查询内存分配、释放、占用情况；
在实时可用内存数不足以支持某些查询完成时返回查询内存耗尽错误；超过单个查询内存使用上限的查询直接返回查询内存到达使用上限错误；通过及时释放已经执行完成或出错的 TASK 内存来降低并发内存占用；

#### 4.4.1 **控制参数**

| 配置项 | 类型 | 含义 | 适用范围 | 值域范围 | 默认值 | 动态更新 |
| --- | --- | --- | --- | --- | --- | --- |
| queryUseMemoryPool | bool | 查询是否使用内存池管理内存，默认打开使用本功能，可根据需要关闭 | 服务端 | false：关闭 true：打开 | true（打开） | 不支持 |
| memPoolFullFunc | bool | 是否启用全功能内存池，当启用时会记录统计内存使用信息，但同时会显著降低内存接口性能，因此可以只在需要调试问题时开启（**不对外暴露**） | 服务端 | false：关闭 true：打开 | false（关闭） | 不支持 |
| minReservedMemorySize | INT32 | 最小预留的系统可用内存数量，单位：MB | 服务端 | [1024-1000000000] | 无（根据可以内存自行预留） | 不支持 |
| ~~queryBufferPoolSize~~ | ~~INT32~~ | ~~单个 dnode 中可以使用的总查询内存上限，单位：MB~~ | ~~服务端~~ | ~~[0-1000000000]~~ | ~~0（不启用）~~ | ~~不支持~~ |
| singleQueryMaxMemorySize | INT32 | 单个查询在单个节点(dnode)上可以使用的内存上限，单位：MB | 服务端 | [0-1000000000] | 0（无上限） | 不支持 |
| queryNoFetchTimeoutSec | INT32 | 查询中当应用长时间不 FETCH 数据时的超时时间，从最后一次响应起当超过该时间时自动清除查询任务 | 服务端 | [60, 1000000000] | 3600*5 | 支持 |


#### 4.4.2 单个查询内存限制模式

用户可以通过配置项 singleQueryMaxMemorySize 来指定单个查询在单个节点上可以使用的内存上限（AMS），当某个查询在某个节点上执行时内存消耗超过该内存上限时，直接返回查询内存到达上限错误；若未配置该配置项，该功能不启动，即单个查询内存使用无上限。
<callout emoji="bulb" background-color="light-orange" border-color="light-orange">
因为实际分配的内存大小(AMS) 与实际使用的内存大小(UMS)之间可能存在差异，因此可能出现实际使用内存未达上限而因为分配内存达到上限的场景。
在全功能内存池模式下，采用非精确统计模式以便获取更好的内存使用性能。
</callout>

#### 4.4.3 内存预留上限模式

当用户配置了 minReservedMemorySize 时，服务端将根据该配置预留内存；当用户未配置 minReservedMemorySize 时，将自动预留系统物理内存总量的 20% 且不小于 1G 大小的内存（SRMS）。除预留内存外，剩余的可用内存都可以被查询使用。在这种模式下，查询内存池会始终动态保留该预留，也就意味着查询可用内存数将跟随系统中可用内存的升降而自动更新，但是无法保证预留操作更新的实时性。
<callout emoji="bulb" background-color="light-orange" border-color="light-orange">
说明：
使用内存预留上限模式可以简化用户配置，同时可以保证物理内存更好的使用效率，但是因为无法保证内存预留和释放的实时性，因此如果遇到系统中任意应用急速分配物理内存场景仍有可能导致内存耗尽问题（OOM）。因此在这种模式下可以根据安全性的需要设置 singleQueryMaxMemorySize 大小，其值越大出现 OOM 的概率越低。
</callout>


#### 4.4.4 模式对比

| 对比项 | 内存预留上限模式 | 不使用内存管控模式 |
| --- | --- | --- |
| 上限值 | 浮动值 | 无 |
| 内存利用率 | 中 | 高 |
| 安全性（不 OOM） | 中 | 低 |
| 可能造成 OOM 的场景 | - 急速分配及写入物理内存 - minReservedMemorySize 过小 | - 系统内存耗尽 |
| 规避 OOM 手段 | - 配置合理大小的 minReservedMemorySize - 配置足够的 SWAP 空间 - 配置足够的物理内存 | - 配置足够的 SWAP 空间 - 配置足够的物理内存 |

说明：
- 单个查询内存限制模式可以同其他两种模式混合使用；
- 默认配置下，默认模式为内存预留上限模式；
- 当 taosd 启动时，系统可用内存大小低于 5G 或预留模式下预留后低于 4G 时自动不使用查询内存管控功能，即采用不使用内存管控模式；

#### 4.4.5 查询淘汰策略

查询淘汰是指当内存使用达到或即将达到上限时，通过自动驱使部分查询失败来释放内存，最终达到内存不超过上限的目的。当前使用内存预留上限模式：
- 如果任一时刻新分配内存的请求将导致系统可用内存数量低于预留大小时，将自动淘汰当前尝试分配内存的查询；
- 如果任一时刻系统可用内存大小（SAMS）小于预留大小时，将自动淘汰部分当前进行中的查询，直至系统可用内存大小大于预留大小；

## 5. 性能

查询引擎按照默认策略进行性能优化，在存在多种可行实现方式时提供用户使用 Hint 的方式进行性能优化。

## 6. 安全

查询引擎的安全性功能需求，作为查询引擎运行的整体保障和支撑，其操作过程对用户透明。安全性功能说明涵盖以下内容：
1. 身份验证与授权 (Authentication and Authorization)：登录身份确认后，授权模块（基于角色的访问控制 RBAC ）根据预定义的策略检查用户权限，确保其仅能执行被允许的查询类型（SELECT, SHOW）以及访问特定的数据库对象（表、视图、列）。
2. 最小权限原则 (Principle of Least Privilege, POLP)：数据库查询引擎及其关联的服务账户必须严格遵循最小权限原则。引擎在操作系统层面或数据库内部，应仅被授予执行其基本功能所需的最低限度权限。
3. 传输过程数据加密(Data Encryption In Transit)：见通信部分。
4. 安全会话管理 (Secure Session Management)：见通信部分。
5. 日志记录、监控与审计追踪 (Logging, Monitoring, and Audit Trails)：查询引擎记录详尽的审计日志（Audit  Logs），涵盖所有数据库连接尝试、敏感数据访问以及潜在的安全异常事件（如异常大量的查询、非工作时间访问）。
6. 安全错误处理与信息泄露预防 (Secure Error Handling and Information Leakage Prevention)：以安全的方式处理和呈现错误信息。不包含敏感的系统内部信息、数据库连接字符串、服务器路径或底层架构细节。
7. 资源限制与拒绝服务保护 (Resource Limiting and DoS Protection)：针对单个查询的执行时间（Query Timeouts）、内存消耗控制、以及并发连接数限制。

## 7. 兼容性

1. 大部分功能兼容 2.x 版本，对于原有功能设计不合理的情况进行修正，必要时增加配置来保持兼容性。
2. SQL-92 适用的查询功能需要支持 SQL-92 语法。

## 8. 运维

无。

## 9. 使用场景

查询场景。

## 10. 约束和限制

### 10.1 分组与窗口查询限制

1. 分组或窗口总个数不超过 1000 万个。

### 10.2 关联查询限制

#### 10.2.1 输入时间线限制

1. 目前所有 Join 都要求输入数据含有效的主键时间线，所有表查询都可以满足，子查询需要注意输出数据是否含有效的主键时间线。

#### 10.2.2 连接条件限制

1. 除 ASOF 和 Window Join 之外，其他 Join 的连接条件中必须含主键列的主连接条件； 且
2. 主连接条件与其他连接条件间只支持 AND 运算；
3. 作为主连接条件的主键列只支持 timetruncate 函数运算（不支持其他函数和标量运算），作为其他连接条件时无限制；

#### 10.2.3 分组条件限制

1. 只支持除主键列外的 TAG、普通列的等值条件；
2. 不支持标量运算；
3. 支持多个分组条件，条件间只支持 AND 运算；

#### 10.2.4 查询结果顺序限制

1. 普通表、子表、子查询且无分组条件无排序的场景下，查询结果会按照驱动表的主键列顺序输出；
2. 超级表查询、Full Join或有分组条件无排序的场景下，查询结果没有固定的输出顺序；
因此，在有排序需求且输出无固定顺序的场景下，需要进行排序操作。部分依赖时间线的函数可能会因为没有有效的时间线输出而无法执行。

#### 10.2.5 嵌套 Join 与多表 Join 限制

1. 目前除 Inner Join 支持嵌套与多表 Join 外，其他类型的 Join 暂不支持嵌套与多表 Join。

### 10.3 嵌套查询限制

1. 如果内层查询的结果数据未提供主键时间戳，那么计算过程隐式依赖主键时间戳的函数或子句在外层会无法正常工作。例如：INTERP, DERIVATIVE, IRATE, LAST_ROW, FIRST, LAST, TWA, STATEDURATION, TAIL, UNIQUE。
2. 计算过程需要两遍扫描的函数，在外层查询中无法正常工作。此类函数包括：PERCENTILE。

## 11. 常见错误和排查

1. 语法错误
   - 常见提示：syntax error near xxx
   - 错误排查：检查提示位置的语法拼写错误
2. 表不存在错误
   - 常见提示：Table does not exist: xxx
   - 错误排查：检查查询的表在对应的库中是否存在
3. 系统底层错误
   - 常见提示：System error
   - 错误排查：检查系统资源是否出现不可用情况
4. 触发查询限制错误
   - 常见提示：Too many groups/time window in query
   - 错误排查：检查语句中分组或窗口个数是否过大
5. 功能未支持错误
   - 常见提示：Not supported join on condition
   - 错误排查：当前语句功能暂不支持，检查是否可以通过其他方式进行查询

## 12. 可观测性

通过 SQL 语句检查查询的执行计划，并在记录查询中关键算子的执行时间开销信息。
```sql
explain analysis_expr sql_statement

analysis_expr: 
    [analyze] [verbose true]
```

通过 `verbose true` 可以展示更加详细的执行计划中算子的信息，例如：扫描算子升序/降序扫描情况，扫描次数，返回结果列的数量和宽度等信息。
通过 `analyze` 选项，在执行计划中展示关键执行步骤中每个算子的执行时间开销，并于用户能够直接检查查询过程中主要的耗时操作。

```sql
show queries;
```

通过该语句能够获得当前正在执行的 SQL 语句以及 SQL 语句执行时候的动态，包括：发起人、来源地址、登录用户、执行时间等信息，便于管理人员了解当前开销最大的查询请求。

## 13. 安装和卸载

无。

## 14. 文档

需要在官网文档中添加修改 [10.3.6 数据查询、10.3.9 函数、10.3.10 特色查询、10.3.13 运算符、10.3.28 关联查询、10.3.30 视图]。

## 15. 参考文档

无

## 16. 附录

无
