# 窗口查询 SELECT 列表扩展及 FILL 子句支持 FS

## 1. 修订记录

|编写日期|发布日期|版本|修订人|主要修改内容|
|---|---|---|---|---|
|2026-04-20|2026-04-20|1.0|Joey Sima|初稿|

## 2. 背景

TDengine 中窗口查询（INTERVAL / SESSION / STATE_WINDOW / EVENT_WINDOW / COUNT_WINDOW）的 SELECT 列表此前仅允许：

- 常量。

- `_wstart`、`_wend`、`_wduration` 伪列。

- 聚集函数（包括选择函数和可以由参数确定输出行数的时序特有函数）。

- 包含上面表达式的表达式。


用户在窗口查询中无法直接输出原始数据列、标量表达式、标签、表名等信息，使用不便。同时，csum、diff、derivative、mavg、statecount、stateduration、lag、lead、fill_forward 等 **不定行函数** （给定 N 行输入，输出行数不确定，函数内部维护跨行状态）也被 Parser 直接禁止出现在窗口查询中，且不能与 FILL 子句一起使用。用户在需要 "按窗口重置、逐行计算" 的场景下只能在应用层自行分窗后多次查询，使用不便且性能较差。

**本次改动的目标：**

1. 扩展窗口查询支持的表达式类型，允许原始列、标签列、标量函数、不定行函数在窗口查询中使用。

2. 在 INTERVAL 窗口查询中，当 SELECT 列表包含改动前已支持的函数（聚集函数、选择函数、可确定输出行数的时序特有函数）时，FILL 行为与原先一致；当 SELECT 列表不包含上述函数时，仅允许与 FILL (NONE / NULL / NULL_F / VALUE / VALUE_F) 搭配使用，为空窗口生成填充行。


## 3. 定义

|术语|定义|
|---|---|
|不定行函数|包括 csum、diff、derivative、mavg、statecount、stateduration、lag、lead、fill_forward。在代码中具有 `FUNC_MGT_INDEFINITE_ROWS_FUNC` 标记。输出行数由输入数据决定，不可在编译期确定。|
|窗口查询|使用 INTERVAL / SESSION / STATE_WINDOW / EVENT_WINDOW / COUNT_WINDOW 子句将数据按窗口分组的查询。|
|FILL 子句|在 INTERVAL 查询中，为数据缺失的空窗口指定填充策略。支持 NONE / NULL / NULL_F / VALUE / VALUE_F / PREV / NEXT / LINEAR / NEAR 等模式。|
|窗口内独立计算|每个窗口视为一个独立的计算单元，函数状态（累加器、前值等）在窗口开始时重置，不从上一个窗口继承。|
|强制填充|NULL_F / VALUE_F 模式，即使查询时间范围内完全无数据，也为每个空窗口生成填充行。与之相对，NULL / VALUE 仅在时间范围内存在至少一条数据时才填充。|
|聚合模式|窗口查询的默认执行模式，每个窗口对数据进行聚合后输出 1 行结果。|
|投影模式|窗口查询的扩展执行模式，每个窗口可输出多行结果（具体行数取决于 SELECT 列表中的函数语义），伪列在同一窗口的所有输出行中重复。|
|SCALAR / AGG 关键字|放置在 `SELECT` 与列列表之间的模式指示关键字。`SCALAR` 强制投影模式，`AGG` 显式声明聚合模式。仅在系统无法自动推断模式时生效。|

## 4. 行为说明

本次改动的核心变化是一条规则： **窗口查询的 SELECT 列表不再有额外限制，组内可进行任意运算。**

### 4.1 SELECT 列表规则变化

**改动前：** 窗口查询 SELECT 列表仅允许常量、窗口伪列（`_wstart`、`_wend`、`_wduration`）和聚集函数及其组合表达式。

**改动后：** 以下函数仍不支持窗口查询：unique、tail、interp、forecast。其余函数按是否包含聚集函数（包括选择函数和可以由参数确定输出行数的时序特有函数）分为两种情况：

**包含聚集函数时：** 与改动前一致。

**不包含聚集函数时：** SELECT 列表可进行任意运算，包括但不限于：

- 常量

- 窗口伪列（`_wstart`、`_wend`、`_wduration`）

- 不定行函数（csum、diff、derivative、mavg、statecount、stateduration、lag、lead、fill_forward）

- 原始数据列（如 `ts`、`current`、`voltage`）

- 标签列（如 `location`、`device_type`）

- `tbname`

- 标量函数和运算符（如 `concat(location, '-zone')`、`current * 10`）

- 以上表达式的任意组合和运算


> 约束：
>
> - unique、tail、interp、forecast 不能在窗口查询中使用（与改动前一致）。
>
> - 聚集函数与不定行函数不可在同一 SELECT 列表中混用（详见 4.4 节）。
>
> - 输出行数不同的不定行函数不可在同一 SELECT 列表中混用（如 csum 输出 N 行，diff 输出 N-1 行，不能一起用）。
>

**示例：**

```SQL
-- 标量运算 + 不定行函数
SELECT _wstart, tbname, concat(location, '-zone') AS zone,  
       current * 10 AS current_ma, diff(voltage) FROM meters  
PARTITION BY tbname INTERVAL(1m);

-- 原始数据列 + 不定行函数
SELECT _wstart, ts, current, csum(current) FROM meters  
PARTITION BY tbname INTERVAL(10s);

-- STATE_WINDOW 中输出任意列
SELECT ts, status, current, csum(current) FROM meters  
PARTITION BY tbname STATE_WINDOW(status);

-- 单表查询中直接引用任意列
SELECT _wstart, ts, location, current, voltage 
FROM d1001 INTERVAL(10s);
```

### 4.2 查询模式与 SCALAR / AGG 关键字

窗口查询在 SELECT 列表扩展后存在两种执行模式：

|模式|每个窗口输出行数|说明|
|---|---|---|
|聚合模式（Aggregation）|**1 行**|与改动前一致，对窗口内数据进行聚合，每个窗口输出一行结果。|
|投影模式（Projection）|**≥0 行**（取决于函数语义）|每个窗口可输出多行结果：原始数据列按输入行数（N 行）输出，不定行函数按函数语义输出（如 csum 输出 N 行，diff 输出 N-1 行）。伪列（`_wstart`、`_wend` 等）在同一窗口的所有输出行中重复。|

#### 自动推断规则

系统根据 SELECT 列表中的表达式类型自动推断执行模式：

|SELECT 列表内容|推断结果|
|---|---|
|包含聚集函数（count、sum、avg、first 等）|聚合模式|
|包含原始数据列（`ts`、`current` 等）或标量表达式|投影模式|
|包含不定行函数（csum、diff 等）|投影模式|
|仅包含伪列（`_wstart`、`_wend`）、`tbname`、标签列、常量|**歧义场景**，默认为聚合模式|

歧义场景是指 SELECT 列表中没有任何能区分模式的表达式。此时系统无法判断用户期望每个窗口输出 1 行还是 N 行，默认按聚合模式处理（与改动前行为一致）。

#### SCALAR / AGG 关键字

为解决歧义场景，新增 `SCALAR` 和 `AGG` 关键字，放置在 `SELECT` 与列列表之间：

```SQL
SELECT [SCALAR | AGG] select_list FROM ...
```

- **`SCALAR`**：强制使用投影模式。即使 SELECT 列表仅包含伪列和标签，也按每窗口 N 行输出。
- **`AGG`**：显式声明使用聚合模式。与默认行为一致，用于代码可读性。

**当 SELECT 列表中已有原始数据列或聚集函数时，模式由内容自动决定，关键字不改变行为。** 关键字仅在歧义场景下生效。

**示例：**

```SQL
-- 歧义场景：SELECT 仅有伪列和 tbname。
-- 默认（聚合模式）：每个窗口 1 行
SELECT _wstart, _wend, tbname FROM d1001 INTERVAL(3s);
-- 结果：5 行（5 个窗口各 1 行）

-- SCALAR：强制投影模式，每个窗口 N 行
SELECT SCALAR _wstart, _wend, tbname FROM d1001 INTERVAL(3s);
-- 结果：10 行（10 条输入数据各 1 行，伪列在同一窗口内重复）

-- AGG：显式聚合，与默认一致
SELECT AGG _wstart, _wend, tbname FROM d1001 INTERVAL(3s);
-- 结果：5 行

-- 非歧义场景：有原始数据列 ts → 自动投影模式，关键字无额外效果
SELECT _wstart, ts, tbname FROM d1001 INTERVAL(3s);
SELECT SCALAR _wstart, ts, tbname FROM d1001 INTERVAL(3s);  -- 同上
SELECT AGG _wstart, ts, tbname FROM d1001 INTERVAL(3s);     -- 同上，仍为投影模式
```

### 4.3 FILL 子句行为

SELECT 列表扩展后，INTERVAL 查询中出现了此前不存在的函数组合。FILL 子句的行为按 SELECT 列表中的函数类型分为三种情况：

|场景| FILL 行为                                                                                                                                                       |
|---|---------------------------------------------------------------------------------------------------------------------------------------------------------------|
|SELECT 列表包含聚集函数（包括选择函数、可确定输出行数的时序特有函数），且不包含禁止 FILL 的函数| 与原先完全一致，所有 FILL 模式均支持（NONE / NULL / NULL_F / VALUE / VALUE_F / PREV / NEXT / LINEAR / NEAR）                                                                   |
|SELECT 列表包含禁止 FILL 的函数（top、bottom、histogram、sample、tail）| 不支持任何 FILL 模式（与改动前一致）                                                                                                                                         |
|SELECT 列表不包含上述函数（如仅使用不定行函数、标量表达式、原始列等）| 仅支持 FILL (NONE / NULL / NULL_F / VALUE / VALUE_F)，禁止 FILL (PREV / NEXT / LINEAR / NEAR)，因为每个窗口内可能有多条数据，此时用 FILL PREV/NEXT/LINEAR/NEAR 的语义不明确，不能确定到底是取哪个值，所以禁用 |

**非聚集场景下各 FILL 模式的行为：**

|FILL 模式|有数据的窗口|空窗口（范围内其他窗口有数据）|空窗口（范围内所有窗口均无数据）|
|---|---|---|---|
|NONE|正常计算，输出 N 行|跳过，不输出|跳过，不输出|
|NULL|正常计算，输出 N 行|输出 1 行 NULL|**不输出**|
|NULL_F|正常计算，输出 N 行|输出 1 行 NULL|**输出 1 行 NULL**|
|VALUE, v|正常计算，输出 N 行|输出 1 行 v|**不输出**|
|VALUE_F, v|正常计算，输出 N 行|输出 1 行 v|**输出 1 行 v**|

> 有数据窗口的输出行数取决于函数语义（如 csum 输出 N 行，diff 输出 N-1 行，mavg (val,k) 在窗口行数 < k 时不输出）。空窗口固定输出 0 行（NONE）或 1 行填充值（其他模式）。

**示例：**

```SQL
-- 支持：不定行函数 + FILL(NULL)
SELECT _wstart, csum(current) FROM meters  
WHERE ts >= '2020-01-01 10:00:00' AND ts < '2020-01-01 10:01:00'  
INTERVAL(10s) FILL(NULL);

-- 支持：不定行函数 + FILL(VALUE_F)
SELECT _wstart, csum(current) FROM meters  
WHERE ts >= '2020-01-01 10:00:00' AND ts < '2020-01-01 10:01:00'  
INTERVAL(10s) FILL(VALUE_F, 0);

-- 不支持：不定行函数 + FILL(PREV)
SELECT _wstart, csum(current) FROM meters  
WHERE ts >= '2020-01-01 10:00:00' AND ts < '2020-01-01 10:01:00'  
INTERVAL(10s) FILL(PREV);
-- 错误：Only FILL(NONE/NULL/NULL_F/VALUE/VALUE_F) is supported with indefinite rows function 'csum'

-- 不支持：top/bottom/histogram/sample/tail + 任何 FILL（与改动前一致）
SELECT _wstart, top(current, 3) FROM meters  
WHERE ts >= '2020-01-01 10:00:00' AND ts < '2020-01-01 10:01:00'  
INTERVAL(10s) FILL(NULL);
-- 错误：Fill not allowed for function 'top'
```

### 4.4 仍然禁止的组合

```SQL
-- 不定行函数与聚合函数混用
SELECT csum(current), sum(current) FROM meters INTERVAL(10s);

-- unique / tail 仍然不支持窗口查询
SELECT _wstart, unique(current) FROM meters INTERVAL(10s);

-- GROUP BY 仍然不支持不定行函数
SELECT csum(current) FROM meters GROUP BY status;

-- 输出行数不同的不定行函数混用
SELECT csum(current), diff(current) FROM meters;
-- 错误：Multiple indefinite rows functions with different return rows
```

### 4.5 错误码

|错误码|十六进制|触发条件|错误信息|
|---|---|---|---|
|TSDB_CODE_PAR_NOT_ALLOWED_FUNC|0x264F|unique/tail 在窗口查询中使用；interp/forecast 在窗口查询或 GROUP BY 中使用；不定行函数在 GROUP BY 中使用|`Function 'xxx' is not supported in window query` / `Function 'xxx' is not supported in window query or group query` / `Multiple indefinite rows functions with different return rows`|
|TSDB_CODE_PAR_FILL_NOT_ALLOWED_FUNC|0x2657|top/bottom/histogram/sample/tail 与任何 FILL 搭配；不定行函数与 FILL(PREV/NEXT/LINEAR/NEAR) 搭配|`Fill not allowed for function 'xxx'` / `Only FILL(NONE/NULL/NULL_F/VALUE/VALUE_F) is supported with indefinite rows function 'xxx'`|

## 5. 性能

**写入性能：** 无影响。本改动仅涉及查询路径。

**查询性能：**

- **窗口查询（无 FILL）：** 窗口算子在不定行函数模式下使用 `applyIndefRowsFuncOnWindow` 替代 `applyAggFunctionOnPartialTuples`，每窗口调用次数从 1 次变为 N 次（N = 窗口内行数）。与不使用窗口直接查询不定行函数相比，额外开销仅为窗口边界检测和状态重置，可忽略不计。

- **FILL 查询：** Fill 算子在不定行函数模式下需要 peek 下一行时间戳来判断窗口边界（一次数组索引操作），不引入显著开销。空窗口填充的开销与聚合函数的 FILL 相同。

- **无回归风险：** 不涉及不定行函数模式时（即纯聚合查询），执行路径与原来完全一致，不会产生性能退化。


## 6. 安全

无。本改动不涉及认证、授权、加密或数据访问控制。

## 7. 兼容性

**向后兼容。**

- 此前报错的 SQL 现在可以正常执行（功能扩展，非行为破坏）。

- 此前可以正常执行的 SQL 行为不变。

- 窗口查询 SELECT 列表扩展是纯增量变更：原有允许的表达式仍然允许，新增允许的表达式（原始列、标签列等）此前会报错。

- 新的错误码 `TSDB_CODE_PAR_FILL_NOT_ALLOWED_FUNC`（0x2657）用于精确区分 FILL 模式不支持的场景，此前使用的是 `TSDB_CODE_PAR_NOT_ALLOWED_FUNC`（0x264F）一刀切拒绝。

- 序列化 / 反序列化：逻辑计划和物理计划节点新增 `indefRowsMode` 布尔字段，使用 `tDecodeIsEnd()` 兼容旧版本反序列化（旧版本不包含此字段时默认为 false，行为等同于纯聚合模式）。


## 8. 运维

无。本改动不影响部署方式、监控指标或运维流程。

## 9. 使用场景

### IoT 设备累计流量按小时窗口重置，输出设备信息

用户需要按小时统计每台设备的累计流量（csum），每小时重新开始累加，空窗口填充 0，并同时输出设备名和位置标签。

```SQL
SELECT _wstart, tbname, location, csum(flow) FROM device_data  
WHERE ts >= '2024-01-01' AND ts < '2024-01-02'  
PARTITION BY tbname  
INTERVAL(1h) FILL(VALUE, 0);
```



### 按小时窗口统计设备状态持续时长，输出设备类型

使用 stateduration 按小时窗口统计设备运行持续时长，空窗口强制填充 -1，输出设备类型标签。

```SQL
SELECT _wstart, tbname, device_type, stateduration(status, "EQ", 1, 1s) FROM device_status
  WHERE ts >= '2024-06-01' AND ts < '2024-06-02'
  PARTITION BY tbname
  INTERVAL(1h) FILL(VALUE_F, -1);
```



## 10. 约束和限制

**约束：**

1. 不定行函数不能与聚合函数在同一 SELECT 中混用（如 `SELECT csum(val), sum(val) FROM t INTERVAL(10s)` 报错）。

2. 输出行数不同的不定行函数不能在同一 SELECT 中混用（如 `SELECT csum(val), diff(val) FROM t` 报错，csum 输出 N 行而 diff 输出 N-1 行）。

3. 不定行函数不能在 GROUP BY 中使用。

4. unique、tail、interp、forecast 不支持窗口查询（与改动前一致）。


**限制：**

1. top、bottom、histogram、sample、tail 不能与任何 FILL 模式搭配使用（与改动前一致）。

2. FILL (PREV / NEXT / LINEAR / NEAR) 仅支持 SELECT 列表包含聚集函数（包括选择函数和可以由参数确定输出行数的时序特有函数）时使用。


## 11. 常见错误和排查

|   |   |   |
|---|---|---|
|错误信息|原因|解决方法|
|`Only FILL(NONE/NULL/NULL_F/VALUE/VALUE_F) is supported with indefinite rows function 'xxx'`|使用了 FILL(PREV/NEXT/LINEAR)|改为 FILL(NULL) 或 FILL(VALUE, v)|
|`Function 'xxx' is not supported in group query`|在 GROUP BY 子句中使用不定行函数|改用 PARTITION BY + 窗口查询|
|`Function 'unique' is not supported in window query`|unique/tail 不支持窗口查询|无法在窗口中使用 unique/tail|
|`Function 'interp' is not supported in window query or group query`|interp/forecast 不支持窗口查询和 GROUP BY|使用 RANGE ... EVERY ... FILL 语法替代 interp|
|`Not a single group set function`|不定行函数与聚合函数混用|分开查询或嵌套子查询|
|`Multiple indefinite rows functions with different return rows`|输出行数不同的不定行函数混用（如 csum + diff）|仅混用输出行数相同的不定行函数|



## 12. 可观测性

无。taos shell、taos Explorer、TDinsight 等组件无行为变化。不定行函数的窗口查询结果通过标准查询接口返回，显示方式与现有查询一致。


## 13. 安装和卸载

无特殊要求。本改动随产品版本发布，无需额外的安装/卸载步骤。

## 14. 文档

需要修改官网文档：

1. **特色查询（24-distinguished.md）**：扩展窗口子句 SELECT 列表规则，新增原始数据列、标签列、tbname、标量表达式的描述；新增不定行函数作为允许的函数类型。

2. **函数参考（22-function.md）**：将 statecount、stateduration、lag、lead 的"不支持窗口查询"改为"窗口内独立计算"说明；mavg 新增窗口行为说明。

3. **SELECT 语法（20-select.md）**：在 FILL 子句说明中新增不定行函数的 FILL 模式限制说明。

4. **基础查询（03-query.md）**：窗口查询示例中补充输出 tbname、标签列等的用法。


## 15. 参考文档

- [https://docs.taosdata.com/reference/taos-sql/distinguished/](https://docs.tdengine.com/reference/sql/function/)

- [https://docs.taosdata.com/reference/taos-sql/function/](https://docs.tdengine.com/reference/sql/select/)

- https://docs.taosdata.com/reference/taos-sql/select/#fill-%E5%AD%90%E5%8F%A5


## 16. 附录

无。