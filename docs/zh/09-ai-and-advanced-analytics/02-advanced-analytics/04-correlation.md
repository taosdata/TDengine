---
title: 时间序列相关分析
sidebar_label: 时间序列相关分析
description: 时间序列相关分析
---

TDengine 提供时间序列相关分析能力：皮尔森相关系数 `CORR` 自 `v3.3.8.0` 起可用；动态时间规整 `DTW` / `DTW_PATH` 与滞后互相关 `TLCC` 自 `v3.4.0.0` 起可用。

### CORR

```sql
CORR(expr1, expr2)
```

计算两个时间序列的皮尔森相关系数，该系数反映两个序列的线性相关性。关于 `CORR` 函数的细节请参阅 [内置函数](../../05-tdengine-sql/04-data-query/03-function.md#corr)。

> 说明：`CORR` 函数不需要 TDgpt 支持，可直接使用。

### DTW

基于动态规划方法对两个时间序列通过非线性时域对准（timing alignment）后计算相似度（similarity）。

计算结果值越小表明两个时间序列相似度越高。`DTW` 计算相似度时使用曼哈顿距离（忽略时间维度），暂不支持使用欧氏距离。

#### 语法

```sql
DTW(column1_name, column2_name, option_expr)

option_expr: {"
radius=expr
[,expr2]
"}
```

1. `column1_name` 和 `column2_name`：参与动态时间规整计算的两列时间序列数据。
2. `option_expr`：字符串，输入动态时间规整计算的相关参数。采用逗号分隔的 `K=V` 字符串表示，字符串不需使用单引号、双引号或转义号等符号，不能使用中文及其他宽字符。
3. `radius=2` 表示邻域半径为 2，限制 `DTW` 路径在距离矩阵中搜索相邻 2 个数值；`radius` 有效范围为 `[1, 10]`，超出范围将报错。
4. 不支持白噪声检查，用户不需要设定执行算法。
5. 最大允许输入 10240 行数据进行计算，超过上限会触发 `Analysis failed since too many input rows`（`0x80000446`）错误。

#### 参数说明

| 参数 | 含义 | 默认值 |
| -------- | --- | --- |
| `radius` | 搜索的邻域半径，限制 DTW 路径在距离矩阵中的探索范围。较小的半径计算更快但可能牺牲精度，较大的半径更精确但计算成本更高；有效范围 `[1, 10]` | 1 |

1. 支持数值列输入
2. 返回值为双精度浮点数

#### 示例

```sql
-- 计算 col1 和 col2 两列数据的动态规整结果，不设置邻域矩阵搜索半径
SELECT dtw(col1, col2)
FROM foo;

-- 计算相似度时指定数值邻域矩阵搜索半径为 2
SELECT dtw(col1, col2, 'radius=2')
FROM foo;
```

### DTW_PATH

基于动态规划方法对两个时间序列通过非线性时域对准后计算相似度。
与 `DTW` 不同，`DTW_PATH` 返回计算相似度时使用的两个时间序列数值匹配列表。

#### 语法

```sql
DTW_PATH(column1_name, column2_name, option_expr)

option_expr: {"
radius=expr
[,expr2]
"}
```

使用条件及约束与 `DTW` 相同，区别是 `DTW_PATH` 返回字符串，表示 `column1_name` 和 `column2_name` 两列匹配的序号。

#### 示例

```sql
taos> select col1, col2 from foo;
       col1     |      col2    |
================================
              1 |            1 |
            1.1 |          1.5 |
              1 |          1.3 |
            1.2 |          1.8 |
            1.1 |          1.6 |


taos> select dtw_path(col1, col2,'radius=1') res from foo;

               res              |
=================================
 (0, 0)                         |
 (1, 0)                         |
 (2, 0)                         |
 (3, 1)                         |
 (3, 2)                         |
 (3, 3)                         |
 (4, 4)                         |

-- (0, 0) 表示使用 col1 列第一行与 col2 列第一行计算差值（曼哈顿距离）：1-1=0；(1, 0) 表示使用 col1 列第二行与 col2 列第一行计算差值：1.1-1=0.1；依此类推，将所有差值相加得到结果 1.6
```

### TLCC

返回两个时间序列在不同时间滞后（lag）下的相关性数值，用以评估两个时间序列之间的动态关系。多用于识别一个序列的变化是否会对另一个序列产生延迟影响，以及这种影响的方向和程度。

#### 语法

```sql
TLCC(column1_name, column2_name, option_expr)

option_expr: {"
lag_start=expr,
lag_end=expr
[,expr2]
"}
```

1. `column1_name` 和 `column2_name`：参与滞后互相关计算的两列时间序列数据。
2. `option_expr`：字符串，输入滞后互相关计算的相关参数。采用逗号分隔的 `K=V` 字符串表示，字符串不需使用单引号、双引号或转义号等符号，不能使用中文及其他宽字符。
3. `lag_start` 与 `lag_end` 表示滞后步数范围，须满足 `lag_start <= lag_end`，且 `abs(lag) < 输入行数`。
4. 不支持白噪声检查，用户无需设置执行的算法。
5. 最大允许输入 10240 行数据进行计算，超过上限会触发 `Analysis failed since too many input rows`（`0x80000446`）错误。

#### 参数说明

| 参数 | 含义 | 默认值 |
| --- | --- | --- |
| `lag_start` | 滞后步数的起始值 | -1 |
| `lag_end` | 滞后步数的结束值 | 1 |

1. 只支持数值列输入
2. 返回值为不同滞后步数下两个时间序列的线性相关数值

#### 示例

```sql
-- 计算 col1 和 col2 两列数据在不同滞后步数下的相关性
SELECT tlcc(col1, col2)
FROM foo;

-- 计算 col1 和 col2 两列数据在滞后步数 -10 到 10 下的相关性
SELECT tlcc(col1, col2, 'lag_start=-10, lag_end=10')
FROM foo;
```
