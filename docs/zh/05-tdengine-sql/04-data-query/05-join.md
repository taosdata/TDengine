---
sidebar_label: 关联查询
title: 关联查询
description: 关联查询（JOIN）概念、类型、语法与限制
---

## JOIN 概念

### 驱动表

驱动关联查询的表。在 Left Join 系列中，左表为驱动表；在 Right Join 系列中，右表为驱动表。

### 连接条件

连接条件指定左右输入的哪些行可以匹配。需要按条件匹配的 JOIN 必须显式指定连接条件；内连接也可写成 `FROM a, b WHERE 条件`，对于普通行级条件，这与 `FROM a INNER JOIN b ON 条件` 的匹配结果相同。`WINDOW JOIN` 使用 `WINDOW_OFFSET` 指定时间窗口。

无条件连接使用显式 `CROSS JOIN` 或逗号语法，两者都返回左右输入的笛卡尔积。`ASOF JOIN` 是可省略 `ON` 的例外：省略时使用默认时间匹配条件；需要指定其他受支持的时间匹配条件或分组条件时，可以显式写出 `ON` 子句。

JOIN 的连接条件须能作为布尔表达式求值。以下类型不能作为连接的等值匹配键：`JSON` 整体值、`BLOB` 和 `MEDIUMBLOB`。聚合和窗口函数不能直接写在 `ON` 中，但子查询已计算出的结果列可以参与连接。

`ASOF JOIN` 的主连接条件基于主键时间戳，可使用支持的比较运算符或 `TIMETRUNCATE` 表达式；标签和普通列只能通过等值条件指定分组。具体限制见下文“Left/Right ASOF Join”。`WINDOW JOIN` 的时间范围通过 `WINDOW_OFFSET` 指定，其他条件限制见对应章节。

### 分组条件

时序场景中的 `ASOF JOIN` / `WINDOW JOIN` 支持对输入数据分组，再在每个分组内做关联。分组只限定参与匹配的行，不等同于 `GROUP BY` 聚合。`ASOF JOIN` / `WINDOW JOIN` 中出现在 `ON` 之后的等值条件（`ASOF JOIN` 的主连接条件除外）将作为分组条件。

### 主键时间线

时间线用于确定时间相关运算所依据的时间列及顺序。普通 JOIN 不强制要求主键时间戳列作为连接条件；子查询未输出时间列时，也可以按业务列关联。

时间线仍用于后续的时间线函数和窗口查询。JOIN 结果不保证按时间有序，也不一定具备这些运算所需的时间线。需要继续做时间相关运算时，应保留所需时间列，并遵循 [基础查询的时间线规则](./01-query.md#时间线)；普通结果排序使用 `ORDER BY`。

## 普通 JOIN 的条件支持

自 `v3.4.3.0` 起，普通 JOIN 不再要求连接条件必须包含主时间戳等值比较。可以按业务列、表达式、范围或组合布尔条件关联；ASOF JOIN 和 WINDOW JOIN 仍遵循各自的时间匹配规则。

| 条件类型 | 示例 | 说明 |
| --- | --- | --- |
| 业务列等值 | `a.groupId = b.groupId` | 不需要额外添加 `a.ts = b.ts` |
| 单侧表达式等值 | `a.voltage + 1 = b.voltage` | 每个表达式只引用对应输入，结果类型须兼容 |
| 混合两侧的等式 | `a.voltage + b.groupId = b.voltage` | 按完整条件判断匹配 |
| 范围条件 | `a.voltage < b.voltage` | 可以不包含等值条件 |
| 组合布尔条件 | `a.voltage = b.voltage OR a.current > b.current` | 按完整条件匹配，保留 SQL 三值逻辑 |
| 单侧或常量条件 | `a.voltage > 220`、`TRUE`、`FALSE` | 条件为 TRUE 才匹配，外连接仍保留未匹配行 |

以下示例使用智能电表数据：超级表 `meters` 包含 `ts TIMESTAMP`、`current FLOAT`、`voltage INT`、`phase FLOAT` 列及 `groupId INT`、`location BINARY(64)` 标签，`d1001`、`d1002` 是其子表。执行前需建立这些表并写入数据。不指定 `ORDER BY` 时不保证结果顺序。

### 示例

保留原有按时刻关联的写法，并在时间相等的行中比较电压：

```sql
SELECT a.ts, a.voltage, b.voltage
FROM d1001 a JOIN d1002 b
ON a.ts = b.ts AND a.voltage > b.voltage;
```

附加条件可以包含 `OR`。以下左连接保留 `d1001` 的全部行；只有满足时间及日期条件时，才填充右侧列：

```sql
SELECT a.ts, b.ts, b.voltage
FROM d1001 a LEFT JOIN d1002 b
ON a.ts = b.ts
AND (a.ts < '2025-01-01 00:00:00.000'
     OR a.ts >= '2025-01-02 00:00:00.000');
```

**按时间粒度关联**

将时间戳截断到秒后匹配，返回同一秒内且左侧电流大于右侧电流的全部行对：

```sql
SELECT a.ts, b.ts, a.current, b.current
FROM d1001 a JOIN d1002 b
ON TIMETRUNCATE(a.ts, 1s) = TIMETRUNCATE(b.ts, 1s)
AND a.current > b.current;
```

**按业务列与表达式关联**

自 `v3.4.3.0` 起，可以只按业务列匹配，不必附加主时间戳等值条件。以下查询关联同组、同地点的记录，再比较电压；匹配行不必来自同一时刻：

```sql
SELECT a.ts, b.ts, a.voltage, b.voltage
FROM meters a JOIN meters b
ON a.groupId = b.groupId AND a.location = b.location
AND a.voltage > b.voltage;
```

表达式可以分别引用各自输入，也可以在完整条件中同时引用两侧：

```sql
-- 左侧电压加 1 后等于右侧电压
SELECT a.ts, b.ts, a.voltage, b.voltage
FROM d1001 a LEFT JOIN d1002 b
ON a.voltage + 1 = b.voltage;

-- 显式转换后比较
SELECT a.ts, b.ts
FROM d1001 a JOIN d1002 b
ON CAST(a.voltage AS BIGINT) = CAST(b.voltage AS BIGINT);

-- 混合引用两侧的等式
SELECT a.ts, b.ts
FROM meters a JOIN meters b
ON a.voltage + b.groupId = b.voltage;
```

**按范围与组合布尔条件关联**

以下查询不包含等值条件，分别按电压范围和完整布尔条件匹配：

```sql
SELECT a.ts, b.ts, a.voltage, b.voltage
FROM d1001 a JOIN d1002 b
ON a.voltage BETWEEN b.voltage - 5 AND b.voltage + 5;

SELECT a.ts, b.ts
FROM d1001 a JOIN d1002 b
ON a.voltage < b.voltage OR a.current > b.current;
```

**`INNER JOIN` 中的 `ON` 与 `WHERE`**

对于以下普通行级条件，两种写法返回相同的行对：

```sql
SELECT a.ts, b.ts
FROM d1001 a INNER JOIN d1002 b
ON a.voltage = b.voltage AND a.current > 10;

SELECT a.ts, b.ts
FROM d1001 a, d1002 b
WHERE a.voltage = b.voltage AND a.current > 10;
```

这种等价性不适用于外连接。下面第一条保留所有左侧行，右侧没有符合条件的行时补 NULL；第二条会过滤掉这些未匹配行：

```sql
SELECT a.ts, b.ts, b.voltage
FROM d1001 a LEFT JOIN d1002 b
ON a.ts = b.ts AND b.voltage > 220;

SELECT a.ts, b.ts, b.voltage
FROM d1001 a LEFT JOIN d1002 b
ON a.ts = b.ts
WHERE b.voltage > 220;
```

**单侧条件、常量条件与无条件连接**

单侧 ON 条件也按行对判断。下面第一条查询中，左侧电压大于 220 的行与所有右侧行匹配；没有匹配的左侧行补 NULL。第二条没有任何匹配，所有左侧行均保留，右侧列均为 NULL：

```sql
SELECT a.ts, b.ts
FROM d1001 a LEFT JOIN d1002 b ON a.voltage > 220;

SELECT a.ts, b.ts
FROM d1001 a LEFT JOIN d1002 b ON FALSE;
```

无条件连接使用 `CROSS JOIN` 或逗号语法；两条查询都返回完整笛卡尔积：

```sql
SELECT a.ts, b.ts FROM d1001 a CROSS JOIN d1002 b;
SELECT a.ts, b.ts FROM d1001 a, d1002 b;
```

普通 `JOIN` 仍须指定 `ON`；`CROSS JOIN` 不接受 `ON`，需要过滤时使用 `WHERE`。

**不输出时间列的子查询关联**

以下两个聚合子查询都未输出时间列，仍可按 `groupId` 关联。普通 JOIN 能执行，不代表其结果可直接用于后续时间线函数或窗口查询：

```sql
SELECT a.groupId, a.avg_voltage, b.avg_current
FROM (SELECT groupId, AVG(voltage) AS avg_voltage
      FROM meters GROUP BY groupId) a
JOIN (SELECT groupId, AVG(current) AS avg_current
      FROM meters GROUP BY groupId) b
ON a.groupId = b.groupId;
```

条件放宽不解除现有类型、输入来源和嵌套连接限制。ASOF/WINDOW 示例及其专用规则见后续对应章节。

## JOIN 算法与 Hint

自 `v3.4.3.0` 起，支持以下普通 JOIN 算法选择及 `JOIN_ALGO` Hint，其中 Theta 用于新增的范围和通用布尔连接条件。

通常由优化器选择算法，无需指定 Hint。算法选择不改变连接类型、完整条件及重复行语义，也不保证没有 `ORDER BY` 时的输出顺序。

| 算法 | 适用条件 | 主要开销 |
| --- | --- | --- |
| Merge | 受支持的时间戳等值匹配表达式，输入满足所需顺序 | 必要时补排序；子查询自身 ORDER BY/LIMIT 语义保持不变 |
| Hash | 可提取类型兼容的等值 key，包括受支持的表达式 | 构建侧内存、key 计算及重复 key 候选匹配 |
| Theta | 无可用等值 key 的范围或通用布尔条件，以及无条件连接 | 逐对检查输入行，大数据量时耗时和内存开销可能较高 |

先过滤不需要的数据，可以减少 JOIN 的输入和匹配开销。需要调优时，通过 `EXPLAIN` 查看算法，再对代表性数据比较执行效果。

在 `SELECT` 后使用 `/*+ JOIN_ALGO(算法) */`，算法参数为 `HASH`、`MERGE` 或 `THETA`：

```sql
EXPLAIN SELECT /*+ JOIN_ALGO(HASH) */ a.ts, b.ts
FROM meters a JOIN meters b ON a.groupId = b.groupId;

EXPLAIN SELECT /*+ JOIN_ALGO(MERGE) */ a.ts, b.ts
FROM meters a JOIN meters b ON a.ts = b.ts;

EXPLAIN SELECT /*+ JOIN_ALGO(THETA) */ a.ts, b.ts
FROM meters a JOIN meters b ON a.voltage < b.voltage;
```

- Hint 是算法偏好。Merge 不适用时可回退 Hash；无法使用等值 key 时可回退 Theta。以 EXPLAIN 中的实际算法为准，不能仅凭 Hint 判断执行路径。
- 已有无参数写法 `HASH_JOIN` 对应 `JOIN_ALGO(HASH)`；自 `v3.4.3.0` 起新增的无参数简写 `MERGE_JOIN` 对应 `JOIN_ALGO(MERGE)`。建议新 SQL 使用统一的 `JOIN_ALGO` 写法。
- ASOF/WINDOW 保持各自的时间匹配规则，不因指定算法 Hint 而改变。

通用 Hint 格式和其他选项见 [基础查询的 Hints](./01-query.md#hints)。

## 语法说明

后续章节会同时介绍 Left/Right Join 系列，因此 Outer、Semi、Anti-Semi、ASOF、Window 等说明中采用类似“左/右”的写法。写在 `/` 前面的适用于 Left Join，写在 `/` 后面的适用于 Right Join。

例如：

- “左/右表”：对 Left Join 指左表，对 Right Join 指右表。
- “右/左表”：对 Left Join 指右表，对 Right Join 指左表。

## JOIN 功能

### INNER JOIN

#### 定义

内连接：返回满足连接条件的左右行组合。一行可以与另一侧的多行匹配，每个匹配行对都会返回。

#### 语法

```sql
SELECT ... FROM table_name1 [INNER] JOIN table_name2 ON ... [WHERE ...] [...]
或
SELECT ... FROM table_name1 CROSS JOIN table_name2 [WHERE ...] [...]
或
SELECT ... FROM table_name1, table_name2 [WHERE ...] [...]
```

#### 结果集

所有满足连接条件的左右行组合。

#### 适用范围

支持超级表、普通表、子表、虚拟表、子查询间 Inner Join。

#### 说明

- 对于第一种语法，`INNER` 关键字可选，必须使用 `ON` 指定连接条件；`WHERE` 用于 Join 输出后的过滤。
- `CROSS JOIN` 和逗号语法表示无条件连接；省略 `WHERE` 时返回完整笛卡尔积，指定 `WHERE` 时在笛卡尔积上过滤。
- Tag 列等值条件可以与其他等值条件组合匹配；需要有序结果时，显式指定 `ORDER BY`。

#### 示例

表 `d1001` 与表 `d1002` 中同时出现电压大于 220V 的时刻及各自的电压值：

```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a JOIN d1002 b ON a.ts = b.ts AND a.voltage > 220 AND b.voltage > 220;
```

### Left/Right Outer Join

#### 定义

左/右（外）连接：返回所有匹配行对，并保留左/右表中没有匹配的行。

#### 语法

```sql
SELECT ... FROM table_name1 LEFT|RIGHT [OUTER] JOIN table_name2 ON ... [WHERE ...] [...]
```

#### 结果集

`INNER JOIN` 的结果集，加上左/右表中没有匹配的行与右/左表空值（`NULL`）组成的行集合。

#### 适用范围

支持超级表、普通表、子表、虚拟表、子查询间 Left/Right Join。

#### 说明

- `OUTER` 关键字可选。

#### 示例

保留 `d1001` 的全部记录；仅当同一时刻双方电压均大于 220V 时显示 `d1002` 的电压，否则右侧为 `NULL`：

```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a LEFT JOIN d1002 b ON a.ts = b.ts AND a.voltage > 220 AND b.voltage > 220;
```

### Left/Right Semi Join

#### 定义

左/右半连接：通常表达 `IN` / `EXISTS` 的语义。对左/右表任意一行，仅当右/左表中存在任一符合连接条件的数据时，才返回该左/右表行。

#### 语法

```sql
SELECT ... FROM table_name1 LEFT|RIGHT SEMI JOIN table_name2 ON ... [WHERE ...] [...]
```

#### 结果集

左/右表中符合连接条件的行，与右/左表任一符合连接条件的行组成的行集合。

#### 适用范围

支持超级表、普通表、子表、虚拟表、子查询间 Left/Right Semi Join。

#### 示例

表 `d1001` 中电压大于 220V，且存在其他电表同一时刻电压也大于 220V 的时间：

```sql
SELECT a.ts FROM d1001 a LEFT SEMI JOIN meters b ON a.ts = b.ts AND a.voltage > 220 AND b.voltage > 220 AND b.tbname != 'd1001';
```

### Left/Right Anti-Semi Join

#### 定义

左/右反连接：与左/右半连接逻辑相反，通常表达 `NOT EXISTS` 的语义。对左/右表任意一行，仅当右/左表中不存在任何符合连接条件的数据时，才返回该左/右表行。

#### 语法

```sql
SELECT ... FROM table_name1 LEFT|RIGHT ANTI JOIN table_name2 ON ... [WHERE ...] [...]
```

#### 结果集

左/右表中没有匹配的行，与右/左表空值（`NULL`）组成的行集合。

#### 适用范围

支持超级表、普通表、子表、虚拟表、子查询间 Left/Right Anti-Semi Join。

#### 示例

表 `d1001` 中电压大于 220V，且不存在其他电表同一时刻电压也大于 220V 的时间：

```sql
SELECT a.ts FROM d1001 a LEFT ANTI JOIN meters b ON a.ts = b.ts AND b.voltage > 220 AND b.tbname != 'd1001' WHERE a.voltage > 220;
```

### Left/Right ASOF Join

#### 定义

左/右不完全匹配连接：不同于传统 Join 的完全匹配，`ASOF JOIN` 允许按指定匹配模式做不完全匹配，即按主键时间戳最接近的方式匹配。

#### 语法

```sql
SELECT ... FROM table_name1 LEFT|RIGHT ASOF JOIN table_name2 [ON ...] [JLIMIT jlimit_num] [WHERE ...] [...]
```

#### 结果集

左/右表中每一行，与右/左表中符合连接条件、按主键列排序后时间戳最接近的至多 `jlimit_num` 条数据（或不存在时的 `NULL`）的笛卡尔积集合。

#### 适用范围

支持超级表、普通表、子表之间的 Left/Right ASOF Join。

#### 说明

- 只支持表之间的 `ASOF JOIN`，不支持子查询之间的 `ASOF JOIN`。
- `ON` 子句中支持指定主键列，或主键列经 `TIMETRUNCATE` 运算后的单个匹配规则（主连接条件）；不支持其他标量运算及函数。下表以 `a.ts 运算符 b.ts` 为例，其中 `a` 为左表、`b` 为右表：

  | 运算符 | Left ASOF 时含义 |
  | ---- | --- |
  | `>`  | 匹配右表中主键时间戳小于左表主键时间戳，且时间戳最接近的数据行 |
  | `>=` | 匹配右表中主键时间戳小于等于左表主键时间戳，且时间戳最接近的数据行 |
  | `=`  | 匹配右表中主键时间戳等于左表主键时间戳的行 |
  | `<`  | 匹配右表中主键时间戳大于左表主键时间戳，且时间戳最接近的数据行 |
  | `<=` | 匹配右表中主键时间戳大于等于左表主键时间戳，且时间戳最接近的数据行 |

  对 Right ASOF，每个右表行从左表中选择满足比较条件且时间最接近的记录。例如 `ON a.ts <= b.ts` 匹配不晚于右表时刻的左表记录。

- 若不含 `ON` 子句，或 `ON` 子句中未指定主键列匹配规则，则默认匹配不晚于驱动行时刻的记录：Left ASOF 等价于 `ON a.ts >= b.ts`，Right ASOF 等价于 `ON a.ts <= b.ts`（`a` 为左表、`b` 为右表）。不支持多个主连接条件。
- `ON` 子句还可指定除主键列外的标签、普通列（不支持标量函数及运算）之间的等值条件，用于分组计算；除此之外不支持其他类型条件。
- 所有 `ON` 条件之间只支持 `AND`。
- `JLIMIT` 用于指定单行匹配结果的最大行数，可选；未指定时默认值为 `1`，即左/右表每行最多从右/左表获得一行匹配结果。`JLIMIT` 取值范围为 `[0, 1024]`；指定 `JLIMIT 0` 时返回空结果。符合条件的至多 `jlimit_num` 条数据不要求时间戳相同；若右/左表中不足 `jlimit_num` 条，返回行数可能更少；若多于 `jlimit_num` 条且时间戳相同，则不保证选中哪些并列记录。

#### 示例

先为 `d1001` 的每行匹配 `d1002` 在同一时刻或此前最近的一条记录，再仅保留双方电压均大于 220V 的结果。若最近记录的电压不符合条件，不会继续查找更早的记录：

```sql
SELECT a.ts, a.voltage, b.ts, b.voltage FROM d1001 a LEFT ASOF JOIN d1002 b ON a.ts >= b.ts WHERE a.voltage > 220 AND b.voltage > 220;
```

### Left/Right Window Join

#### 定义

左/右窗口连接：根据左/右表每一行的主键时间戳和窗口边界构造窗口并据此连接，支持在窗口内做投影、标量和聚合。

#### 语法

```sql
SELECT ... FROM table_name1 LEFT|RIGHT WINDOW JOIN table_name2 [ON ...] WINDOW_OFFSET(start_offset, end_offset) [JLIMIT jlimit_num] [WHERE ...] [...]
```

#### 结果集

以下两种之一：

- 左/右表每一行，与右/左表中按左/右表主键时间戳和 `WINDOW_OFFSET` 划分的窗口内至多 `jlimit_num` 条数据（或不存在时的 `NULL`）的笛卡尔积集合。
- 左/右表每一行，与上述窗口内至多 `jlimit_num` 条数据的聚合结果组成的行集合。没有匹配时，结果由聚合函数决定，例如 `COUNT(b.voltage)` 为 `0`，`AVG(b.voltage)` 为 `NULL`（`b` 为被匹配表）。

#### 适用范围

支持超级表、普通表、子表之间的 Left/Right Window Join。

#### 说明

- 只支持表之间的 `WINDOW JOIN`，不支持子查询之间的 `WINDOW JOIN`。
- `ON` 子句可选，只支持指定除主键列外的标签、普通列（不支持标量函数及运算）之间的等值条件用于分组；所有条件之间只支持 `AND`。
- `WINDOW_OFFSET` 用于指定窗口左右边界相对左/右表主键时间戳的偏移量，支持自带时间单位的写法，例如 `WINDOW_OFFSET(-1a, 1a)`。对 Left Window Join，表示每个窗口为 `[左表主键时间戳 - 1 毫秒, 左表主键时间戳 + 1 毫秒]`，左右边界均为闭区间。数字后的时间单位可为 `b`（纳秒）、`u`（微秒）、`a`（毫秒）、`s`（秒）、`m`（分）、`h`（小时）、`d`（天）、`w`（周）；不支持自然月（`n`）、自然年（`y`）。支持的最小时间单位为数据库精度，左右表所在数据库精度需一致。
- `JLIMIT` 用于指定单个窗口内的最大匹配行数，可选；未指定时默认获取每个窗口内的全部匹配行。`JLIMIT` 取值范围为 `[0, 1024]`；指定 `JLIMIT 0` 时返回空结果。若被匹配一侧不足 `jlimit_num` 条，返回行数可能更少；若超过 `jlimit_num` 条，优先返回窗口内主键时间戳最小的 `jlimit_num` 条。
- SQL 中不能再含其他 `GROUP BY` / `PARTITION BY` / 窗口查询。
- 支持在 `WHERE` 中做标量过滤；支持在 `HAVING` 中针对每个窗口做聚合函数过滤（不支持标量过滤）；不支持 `SLIMIT`；不支持各种窗口伪列。

#### 示例

表 `d1001` 电压大于 220V 时，前后 1 秒区间内表 `d1002` 的电压值：

```sql
SELECT a.ts, a.voltage, b.voltage FROM d1001 a LEFT WINDOW JOIN d1002 b WINDOW_OFFSET(-1s, 1s) WHERE a.voltage > 220;
```

表 `d1001` 电压大于 220V，且前后 1 秒区间内表 `d1002` 的电压平均值也大于 220V 的时间及电压值：

```sql
SELECT a.ts, a.voltage, AVG(b.voltage) FROM d1001 a LEFT WINDOW JOIN d1002 b WINDOW_OFFSET(-1s, 1s) WHERE a.voltage > 220 HAVING (AVG(b.voltage) > 220);
```

### Full Outer Join

#### 定义

全（外）连接：返回所有匹配行对，并保留左右两表中没有匹配的行。

#### 语法

```sql
SELECT ... FROM table_name1 FULL [OUTER] JOIN table_name2 ON ... [WHERE ...] [...]
```

#### 结果集

`INNER JOIN` 的结果集，加上：

- 左表中没有匹配的行与右表空值（`NULL`）组成的行集合；
- 右表中没有匹配的行与左表空值（`NULL`）组成的行集合。

#### 适用范围

支持超级表、普通表、子表、虚拟表、子查询间 Full Outer Join。

#### 说明

- `OUTER` 关键字可选。

#### 示例

表 `d1001` 与表 `d1002` 中记录的全部时刻及电压值：

```sql
SELECT a.ts, a.voltage, b.ts, b.voltage FROM d1001 a FULL JOIN d1002 b ON a.ts = b.ts;
```

## 约束和限制

### 输入时间线限制

- 普通 JOIN 不强制要求主键时间戳列作为连接条件；子查询未输出时间列时仍可按其他列关联，详见 [主键时间线](#主键时间线)。
- 指定 Merge Hint 不改变子查询自身的排序和取行语义。
- 子查询内部的 `ORDER BY` 与 `LIMIT` 仍先决定选取哪些行，JOIN 不改变该语义。
- ASOF/WINDOW 仍要求表输入及主键时间线，详见对应章节。

### 连接条件限制

- 需要按条件匹配的 JOIN 必须显式指定连接条件；内连接也可使用逗号语法并在 `WHERE` 中指定条件。无条件连接使用 `CROSS JOIN` 或不带匹配条件的逗号语法。`ASOF JOIN` 可以省略 `ON`，`WINDOW JOIN` 使用 `WINDOW_OFFSET` 指定时间范围，具体规则见对应章节；
- 普通 Join 支持等值、非等值、单侧、常量和组合布尔条件；
- `ON` 中的表达式须能作为布尔表达式求值；参与比较的值须有兼容的类型。聚合和窗口函数应在子查询中计算后再参与连接。

### 分组条件限制

以下限制适用于 ASOF/WINDOW 的分组匹配：

- 只支持除主键列外的标签、普通列的等值条件。
- 不支持标量运算。
- 支持多个分组条件，条件之间只支持 `AND`。

### 查询结果顺序限制

- 普通 JOIN 不保证结果顺序，包括普通表、子表和子查询之间的 JOIN。需要确定顺序时，应显式指定 `ORDER BY`。

### 嵌套 JOIN 与多表 JOIN 限制

- 目前除 `INNER JOIN` 支持嵌套与多表 Join 外，其他类型的 Join 暂不支持嵌套与多表 Join。
