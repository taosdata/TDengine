# ExternalWindow FS

## 1. 背景

1. [TS-4900](https://jira.taosdata.com:18080/browse/TS-4900)  简述: 在一些场景，需要对窗口内的数据进行聚合操作，但是这个窗口并不是自身时间线的session, state, event等窗口产生的，而是另外的时间线甚至外部提供的文件产生的。详细的分析请看 [基于其他表的窗口进行计算的语法方式](https://taosdata.feishu.cn/docx/K9Ksd4Rtbo6K7IxoCoEcDOeunAn)
2. [TS-5707](https://jira.taosdata.com:18080/browse/TS-5707)  简述：对于事件开始时间（_wstart）、事件结束时间点（_wend），支持增加时间偏移.  例如：
  ```plaintext
  例如增加 time_shilft( 3m, -3m)  表示 事件开始时间后移 3 分钟、事件结束时间点往前移动 3 分钟。
  select _wstart + 3m, _wend - 3m, _wduration , count
  
  from meters
  partitionby tbname
  event_window start with voltage > 800 end with voltage < 8000
  time_shilft( 3m, -3m)
  ```

完整需求 [需求说明：External Window](https://taosdata.feishu.cn/wiki/XGSZwwfVnitr9HkJXkpcf2Ehngh)来自于上面 [TS-4900](https://jira.taosdata.com:18080/browse/TS-4900) ，在实现的同时考虑 [TS-5707](https://jira.taosdata.com:18080/browse/TS-5707)  可以同时实现

## 2. 功能概述

ExternalWindow 的目标是引入一种**基于外部查询定义的 Window 作用域机制**，用于支持在查询中对数据进行**窗口化计算**，其中窗口的定义不依赖于被计算数据集自身的时间线。

### 2.1 设计目标

- 提供一种声明式方式，在查询中引入由外部数据定义的 Window 作用域。
- Window 用于限定计算范围，并提供可供引用的窗口数据。
- 保持语义清晰、可推理，并为未来扩展预留空间。

### 2.2 核心概念与语义模型

#### 2.2.1 External Window 的基本语义

1. External Window 定义一个 **Window Specification**，而非单一时间窗口。
2. 每一个 Window Specification 对应一个由子查询产生的 **Window Relation**。
3. Window Relation 中的每一行表示一个 **Window Instance**。
4. 每一个 Window Instance 表示一个时间闭区间：[start_ts, end_ts]
5. Window Instance 之间可以重叠甚至重复，外部计算根据分组情况应用在分组内的每一个窗口上；重叠重复处理见[这里](https://taosdata.feishu.cn/wiki/D5lDw969liBn6dkrU4fcQ61WnPf#share-K0y6dEOfaokOOGxeok5cjtGNnvg)
6. external window 不要求输入的窗口有序，运算不改变各个窗口的顺序，**输出有序性和输入相同，****乱序相关具体见**[**这里**](https://taosdata.feishu.cn/wiki/D5lDw969liBn6dkrU4fcQ61WnPf#share-DPgWdz8UqoR9tCxwG7McdVAtnge)

#### 2.2.2 时间区间的开闭语义

- 默认，所有 Window Instance 采用 **闭区间**：`ts >= start_ts AND ts <= end_ts`
- 如果需要开区间，对 Window Instance 的起始时间进行 -1/+1  操作，和开区间是同样效果。-1/+1 表示减/加 当前数据库时间精度的1个最小单位。

### 2.3 功能范围

#### 2.3.1 Window 的生成

1. Window 可由任意满足要求的子查询生成。
2. Window 的定义不要求与被计算数据源存在 schema 或主键关联。
3. Window 子查询拥有完整的查询功能。
4. Window 子查询除时间列外，可包含任意属性列及相关表达式，用于在 Window 作用域内参与计算。

#### 2.3.2 External Window 与外层查询的交互

1. 外部查询与 Window 子查询的分组`PARTITION BY`是正交的。分组匹配逻辑见[这里](https://taosdata.feishu.cn/wiki/D5lDw969liBn6dkrU4fcQ61WnPf#share-VYRFdOOtRoixDlx7nA9c5Cvunyb)
2. 当 External Window 的窗口查询时没有数据时，默认不输出该窗口，可通过 fill 子句进行插值；
3. 外部查询可以引用 external window 的选择列，类似于分组常量
4. 支持嵌套使用 External Window。

#### 2.3.3 Window 作用域内的计算

- 支持窗口内的聚合计算
- 支持窗口内的标量计算
- 暂不支持不定行函数（如 `diff`, `interp`）（本期）。

## 3. 语法规范

### 3.1 完整语法

```sql
select_stmt ::=
  SELECT select_list
  FROM from_clause
  [WHERE where_condition]
  [PARTITION BY partition_list]
  [EXTERNAL_WINDOW ((subquery) alias_name) [FILL(fill_mod)]]
  [HAVING having_condition]

```

### 3.2 语法说明

#### 3.2.1 `EXTERNAL_WINDOW` 子句

`EXTERNAL_WINDOW ((subquery) alias_name)` 定义一个 Window Specification。
`subquery`结果集每一行表示一个 Window Instance，必须满足：
- 前两列分别为窗口开始时间和结束时间
- 时间列类型为时间类型

#### 3.2.2 Window Instance

- 每个 Window Instance 表示一个时间区间：`[start_ts, end_ts``]`
- Window Instances 允许有重叠区间
- Window Instances 要求正序（后续考虑放开逆序或者无序）

#### 3.2.3 HAVING 子句

- `HAVING` 子句用于过滤组合 Window Instance
- `HAVING` 在 Window 作用域内计算完成后执行

#### 3.2.4 FILL 子句

- `EXTERNAL_WINDOW` FILL 子句的 fill_mod 暂时只能指定 NONE
- 当有 FILL 子句时，对应窗口未匹配到外层查询数据时，窗口依旧输出，外层查询列引用结果为 `NULL`
- 聚合函数在空输入集上的行为遵循 SQL 标准语义
| 聚合函数 | 结果 |
| --- | --- |
| COUNT(*) | 0 |
| COUNT(expr) | 0 |
| SUM(expr) | NULL |
| AVG(expr) | NULL |
| MIN(expr) | NULL |
| MAX(expr) | NULL |
| FIRST(expr) | NULL |
| LAST(expr) | NULL |

### 3.3 新增辅助函数

#### 3.3.1 lag()

- 语法：lag`(expr, offset[, default_value])`
- 功能：在排序后取前第N行指定列的值
- 参数：
  - column：列名或表达式
  - offset：偏移行数（只允许正值）
  - default_value：可选，偏移行不存在时的默认值

#### 3.3.2 lead()

- 语法：lead`(expr, offset[, default_value])`
- 功能：在排序后取后第N行指定列的值
- 参数：
  - column：列名或表达式
  - offset：偏移行数（只允许正值）
  - default_value：可选，偏移行不存在时的默认值

### 3.4 伪列说明

不增加新标志，使用 _WSTART 和 _WEND 作为窗口开始结束时间
1. _WSTART: (external window time start)
2. _WEND: (external window time end) 
3. _WDURAITON 

## 4. 核心功能特性

### 4.1 内部查询支持

1. 子查询是含约束的普通查询
  - 子查询结果集的前两列必须是输出为timestamp 类型的列或者表达式，用来表示窗口开始时间与结束时间
1. CSV文件子查询：`SELECT * FROM FILE('/path/to/file.csv') aliasName`
  - CSV 文件内容映射为关系型数据集
  - 列类型由系统推断或显式指定
1. 文本串子查询：`SELECT * FROM TEXT(...) aliasName`
  - 文本内容解析为多列数据集
  - 列类型需要显式指定

### 4.2 外部查询功能

- 聚合计算：COUNT, AVG, FIRST, LAST等
- 标量计算
- 列引用机制：通过窗口别名引用内部查询列
  - Window Instance 的属性列/表达式通过 Window 别名进行引用
  - _wstart, _wend 伪列表示窗口的开始结束时间，可以直接引用
  - 引用方式在 `SELECT`,`HAVING`,`order_by_clasue` 中可以使用，在其他子句中无法使用
  - Window Instance 属性列在 Window Scope 内视为常量/分组常量
- External Window 的外部查询暂不支持不定行函数（diff, interp 等）

### 4.3 分组与对齐

#### 4.3.1 内外分组支持

Window Source 与外部查询均可声明 `PARTITION BY`：
- Window Source 的 `PARTITION BY` 用于生成分组独立的 Window Instances
- 外部查询的 `PARTITION BY` 用于限定 Window Scope 内的数据分组

#### 4.3.2 分组关联与对齐

当 Window Source 与外部查询均包含分组字段时：
- 分组条件自动关联
- 等效于自动使用了 groupKey 相等条件（关于 groupKey 的生成及对齐细节见 [分组场景说明](https://taosdata.feishu.cn/wiki/D5lDw969liBn6dkrU4fcQ61WnPf#share-Uc00d1VisotRt9xC7iYcbsECnLh) ）
当 Window Source 与外部查询均只有一个包含分组字段时，见 [分组场景说明](https://taosdata.feishu.cn/wiki/D5lDw969liBn6dkrU4fcQ61WnPf#share-Uc00d1VisotRt9xC7iYcbsECnLh)

### 4.4 嵌套调用支持

嵌套规则与子查询相同，支持范围也相同。即嵌套的SQL查询语句中每层都可使用 external_window，实现多层逻辑过滤。

## 5. 使用场景示例

### 5.1 故障报警场景

1. fault 事件：event = 1
2. alarm 事件：event = 6

#### 5.1.1 故障-告警关联分析

**目标**：分析故障事件后60秒内的告警事件
```sql
select w.ts, w.event, w.val, count(a.*), avg(a.val)
from alarm1 a 
where event = 6 
EXTERNAL_WINDOW (
  (
      select ts, ts + 60s, event, val 
      from fault1
      where event = 1
  )
  w
)
having count(a.*) <= 0；
```

#### 5.1.2 无报警故障窗口输出（match outer）

在 60 秒内收到 alarm 事件的 fault 事件，列出这些 fault  事件和 60 秒内的所有 alarm 事件。注意：没有找到 alarm 事件的 fault 事件，其 a.ts, a.event, a.val 应该显示为 NULL）
```sql {wrap}
select w.ts, w.event, w.val, a.ts, a.event, a.val,
from alarm1 a 
where event = 6 
EXTERNAL_WINDOW ( (
  select ts, ts + 60s, event, val 
  from fault1
  where event = 1 )
  w
)
FILL(NONE)
```

#### 5.1.3 多条件嵌套事件序列（external window 嵌套）

**目标**：筛选"故障后10分钟内未恢复，且无车辆事件，且故障后1分钟内有两次告警"
```sql
SELECT w3.t1, w3.event, w3.val, COUNT(a.*) 
FROM alarm1 a
EXTERNAL_WINDOW (
  (SELECT w2.t1, w2.t1 + 1m, w2.event, w2.val, COUNT(c.*) 
  FROM car1 c
  EXTERNAL_WINDOW (
    (SELECT w1.t1, w1.t2, w1.event, w1.val, COUNT(f2.*)
    FROM fault1 f2 
    WHERE event = 2 
    EXTERNAL_WINDOW (
      (SELECT ts t1, ts + 10m t2, event, val 
      FROM fault1 f1
      WHERE event = 1) w1
    )
    HAVING COUNT(f2.*) <= 0 ) w2
  )
  HAVING COUNT(c.*) <= 0 ) w3
)
HAVING COUNT(a.*) = 2

等效于：
SELECT _wstart, w3.event, w3.val, COUNT(a.*) 
FROM alarm1 a
EXTERNAL_WINDOW (
  (SELECT _wstart, _wstart + 1m, w2.event, w2.val, COUNT(c.*) 
  FROM car1 c
  EXTERNAL_WINDOW (
    (SELECT _wstart, _wend, w1.event, w1.val, COUNT(f2.*)
    FROM fault1 f2 
    WHERE event = 2 
    EXTERNAL_WINDOW (
      (SELECT ts t1, ts + 10m t2, event, val 
      FROM fault1 f1
      WHERE event = 1) w1
    )
    HAVING COUNT(f2.*) <= 0 ) w2
  )
  HAVING COUNT(c.*) <= 0 ) w3
)
HAVING COUNT(a.*) = 2

```

### 5.2 按钮-电梯门场景

找出 button 事件后第一次电梯运动的时间（第一次对应的 door 事件的时间）。其中对于 button 类型为 landing call 的， 要求 door 的 floor 跟 landing floor 一致。对于 button 类型为car call 的，要求 target floor 与 door 的 floor 一致。
- Landing call : event = 1
- Target call: event = 2

#### 5.2.1 按钮-电梯门事件匹配

**目标**：找出按钮事件后第一次对应的电梯门事件
```sql
SELECT w.t1, FIRST(d.ts)
FROM door1 d
WHERE (event = 1 AND targetFloor = d.door) OR (event = 2 AND landingFloor = d.door)
EXTERNAL_WINDOW ( 
  (SELECT ts t1, ts + 10m t2, event, targetFloor, landingFloor
  FROM button1 b) w
)
HAVING COUNT(d.*) > 0
```

#### 5.2.2 事件间窗口统计（lag 函数）

**目标**：统计两次连续告警事件之间发生车辆事件的次数, 最近一次 alarm event 的窗口一直持续到永远。
```sql
SELECT w.t1, COUNT(c.*)
FROM car1 c
EXTERNAL_WINDOW (
      (SELECT ts t1, lag(ts, 1, maxtime()) t2
      FROM alarm1 a) w
)
```

#### 5.2.3 事件间窗口统计 （lag 函数替代）

```sql
SELECT w.t1, COUNT(c.*)
FROM car1 c
EXTERNAL_WINDOW (
       (
          (select ts - diff(ts) t1, ts t2, deviceId
            from alarm1 a
          )
          union
          ( 
            select last(ts) t1, maxtime() t2, deviceId
            from alarm1 a
          )
       ) w
) 
```

### 5.3 动态复权因子计算（分组计算）

**目标**：先计算动态复权因子，再应用于细粒度K线数据
```sql
SELECT ts, price*ratio 
FROM k_1m m
PARTITION BY cmplno
EXTERNAL_WINDOW (
  (
  SELECT ts t1, 
         neighbor(ts, 1, maxtime()) t2, 
         (a/b) * neighbor(a/b, -1, 1) ratio 
  FROM k_day
  PARTITION BY cmplno
  ) w
)
```

### 5.4 事件窗口报警事件统计( event_window )

```sql
select w.ts, w.event, w.val, count(a.*), avg(a.val)
from alarm1 a 
where event = 6 
PARTITION BY device_id
EXTERNAL_WINDOW (
  (
      FROM d001
      PARTITION BY device_id
      EVENT_WINDOW
      START WITH voltage <= 190
      END WITH voltage >= 200
  ) w
)
```

### 5.5 偏移窗口 [TS-5707](https://jira.taosdata.com:18080/browse/TS-5707)  

针对需求：增加 **time_shilft( 3m, -3m)  表示 事件开始时间后移 3 分钟、事件结束时间点往前移动 3 分钟。**
select **_wstart + 3m, _wend - 3m,** _wduration , count
from meters
partitionby tbname
event_window start with voltage > 800 end with voltage < 8000
**time_shilft( 3m, -3m)**
用 external window 等效表达
虽然是同一张表，但是窗口变了，数据读了两遍
```sql
select _wstart, _wend, _wduration, count(*), avg(val)
from meters a 
PARTITION BY tbname
EXTERNAL_WINDOW (
  (
      select _wstart + 3m, _wend - 3m
      from meters
      partitionby tbname
      event_window start with voltage > 800 end with voltage < 8000
  ) w
)
```

## 6. 特殊场景说明

#### 6.0.1 排序要求说明

##### 6.0.1.1 外部窗口乱序如何处理

语法窗口要求正序有序，及按照 _wstart 递增，_wstart 可以有重复。
当输入窗口有序时，external_window 的输出窗口也按照_wstart 正序有序输出。
如果输入窗口在语法阶段能够确定非正序有序，则在语法阶段报错；如果语法阶段未能发现，而在执行阶段检测到输入窗口乱序，则在执行阶段报错。

#### 6.0.2 重叠、重复、非法窗口

运行时发现[非法窗口](https://taosdata.feishu.cn/wiki/D5lDw969liBn6dkrU4fcQ61WnPf#share-FzxGdWNWVoxbt3xl5u3clzoWn2b)报错
有重叠窗口各个窗口独立计算，有重叠窗口指的多个时间窗口交集非空。
开始结束时间完全一致的完全重复的窗口，对于窗口有序的输入，只计算第一次，只输出一个窗口。

#### 6.0.3 分组场景说明

1. Window 窗口无分组，外部查询也无分组，对查询数据应用所有的 external window 进行计算；
2. Window 窗口无分组，外部查询有分组，在外部查询的所有分组上应用所有 window 窗口进行计算。
```sql
SELECT ...
FROM alarm a
PARTITION BY deviceId
EXTERNAL_WINDOW (
  (
      SELECT ts, ts+60s, deviceId FROM fault WHERE event=1
  ) w
)
```

1. Window 窗口有分组，但是外部查询没有分组，外部查询应用在每个分组的所有 external window 窗口上。
```sql
SELECT ...
FROM alarm a
EXTERNAL_WINDOW(
  (
      SELECT ts, ts+60s, deviceId FROM fault WHERE event=1
      PARTITION BY deviceId
  ) w
)
```

1. 内外均有分组时，默认应用分组条件。
   - 关于分组 key 的说明和计算：内部/外部 无论有几个分组条件，都按照分组条件 (字符串拼接求hash，对多个条件有顺序要求 ) 生成 groupKey； extern window 窗口的 groupkey(输入窗口分组key) 称为:  inputGroupKey,  计算窗口 groupkey 称为： calcGroupKey
   - ExternalWindow 的分组条件默认为 calcGroupKey =  inputGroupKey，满足分组条件输出，否则不输出
   - FILL 子句不影响分组是否输出，只影响满足条件的分组内的某个窗口是否输出

## 7. 约束与限制

### 7.1 约束说明

#### 7.1.1 使用场景

1. 未来在考虑，本期不允许在流里面使用，语法报错
2. 未来支持，本期不允许在订阅使用，语法报错
3. Insert into select 中允许使用
4. STMT 支持使用 external_window 语句，不支持绑定 externa_window 参数

#### 7.1.2 Window Source 约束

以下情况应视为语义错误：
1. Subquey 类型的 Window Source 的前两列无法解析为窗口开始时间与结束时间，要求前两列必须为输出类型为 timestamp 类型的表达式
2. 可在解析阶段发现的窗口开始时间大于或等于窗口结束时间（执行期间才发现的时间区间错误以运行时错误返回）

#### 7.1.3 FILL 使用约束

FILL 子句在 External window 之后使用，FILL() 参数暂只支持填充 None，不支持其他填充选项。

#### 7.1.4 WHERE 的作用域约束

以下用法应视为非法：
1. 在 `WHERE` 子句中引用 Window Instance 的属性列

#### 7.1.5 版本约束

1. Lag/lead 函数可能先只能实现前后 1 行的查找
2. 在 Window Scope 内使用不定行函数（如 `diff`, `interp`）报错
3. FILL 在 External window 后， 只支持填充 None，其他选项报错
4. 流计算和订阅中使用 external window 报错

### 7.2 运行时错误

1. 以下为无效 Window Instance 不参与后续 Window Scope 构建与计算。
   - 运行时发现无效窗口：窗口开始时间大于窗口结束时间、窗口起始时间乱序
   - Window Instance 的时间区间为空或者无效（出现 NULL）

### 7.3 性能考虑

- 分组/窗口数量影响关联计算开销
- 嵌套深度增加计算复杂度

## 8. 开发计划

- 本期完成
  - External window 基本功能
  - Lag/lead 函数，简单模式，找到前/后 1 行；（前 N 行可能会有缓存问题，实现时再根据情况考虑，N 行实现本次不保证）
- 不在本次，后续待完成的功能
  - lag/lead 函数增强，支持前/后 N 行
  - 通过文本生成子查询 [ExternalWindow FS](https://taosdata.feishu.cn/wiki/D5lDw969liBn6dkrU4fcQ61WnPf)
  - 通过 CSV 文件生成子查询
  - Window Instances 考虑放开逆序或者无序，优先级低
- 无确定需求，待有需求再考虑开发的能力
  - lag/lead 函数增强，支持找不到值的默认填充
  - FILL 填充支持更多选项
  - External window 和外部查询分组匹配方式增加更多选项
