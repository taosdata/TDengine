---
sidebar_label: 窗口预聚集
title: 窗口预聚集
description: 窗口预聚集使用说明
---

为了提高大数据量的聚合函数查询性能，通过创建窗口预聚集 (TSMA Time-Range Small Materialized Aggregates) 对象, 使用固定时间窗口对指定的聚集函数进行预计算，并将计算结果存储下来，查询时通过查询预计算结果以提高查询性能。

## 创建TSMA

```sql
-- 创建基于超级表或普通表的tsma
CREATE TSMA tsma_name ON [dbname.]table_name FUNCTION (func_name(func_param) [, ...] ) INTERVAL(time_duration);
-- 创建基于小窗口tsma的大窗口tsma
CREATE RECURSIVE TSMA tsma_name ON [db_name.]tsma_name1 INTERVAL(time_duration);

time_duration:
    number unit
```

创建 TSMA 时需要指定 TSMA 名字, 表名字, 函数列表以及窗口大小. 当基于一个已经存在的 TSMA 创建新的 TSMA 时, 需要使用 `RECURSIVE` 关键字但不能指定 `FUNCTION()`， 新创建的 TSMA 已有 TSMA 拥有相同的函数列表， 且此种情况下所指定的 INTERVAL 必须为所基于的 TSMA 窗口长度的整数倍。

其中 TSMA 命名规则与表名字类似, 长度最大限制为表名长度限制减去输出表后缀长度, 表名长度限制为193, 输出表后缀为`_tsma_res_stb_`, TSMA 名字最大长度为178.

TSMA只能基于超级表和普通表创建, 不能基于子表创建.

函数列表中只能指定支持的聚集函数(见下文), 并且函数参数必须为1个, 即使当前函数支持多个参数, 函数参数内必须为普通列名, 不能为标签列. 函数列表中完全相同的函数和列会被去重, 如同时创建两个avg(c1), 则只会计算一个输出. TSMA 计算时将会把所有`函数中间结果`都输出到另一张超级表中, 输出超级表还包含了原始表的所有tag列. 函数列表中函数个数最多支持创建表最大列个数(包括tag列)减去 TSMA 计算附加的四列, 分别为`_wstart`, `_wend`, `_wduration`, 以及一个新增tag列 `tbname`, 再减去原始表的tag列数. 若列个数超出限制, 会报`Too many columns`错误. 

由于TSMA输出为一张超级表, 因此输出表的行长度受最大行长度限制, 不同函数的`中间结果`大小各异, 一般都大于原始数据大小, 若输出表的行长度大于最大行长度限制, 将会报`Row length exceeds max length`错误. 此时需要减少函数个数或者将常用的函数进行分组拆分到多个TSMA中.

窗口大小的限制为[1m ~ 1y/12n]. INTERVAL 的单位与查询中INTERVAL子句相同, 如 a (毫秒), b (纳秒), h (小时), m (分钟), s (秒), u (微秒), d (天), w(周), n(月), y(年).

TSMA为库内对象, 但名字全局唯一. 集群内一共可创建TSMA个数受参数`maxTsmaNum`限制, 参数默认值为3, 范围: [0-3]. 注意, 由于TSMA后台计算使用流计算, 因此每创建一条TSMA, 将会创建一条流, 因此能够创建的TSMA条数也受当前已经存在的流条数和最大可创建流条数限制.

## 支持的函数列表
| 函数|  备注 |
|---|---|
|min||
|max||
|sum||
|first||
|last||
|avg||
|count| 若想使用count(*), 则应创建count(ts)函数|
|spread||
|stddev||
|||

## 删除TSMA
```sql
DROP TSMA [db_name.]tsma_name;
```
若存在其他TSMA基于当前被删除TSMA创建, 则删除操作报`Invalid drop base tsma, drop recursive tsma first`错误. 因此需先删除 所有Recursive TSMA.

## TSMA的计算
TSMA的计算结果为与原始表相同库下的一张超级表, 此表用户不可见. 不可删除, 在`DROP TSMA`时自动删除. TSMA的计算是通过流计算完成的, 此过程为后台异步过程, TSMA的计算结果不保证实时性, 但可以保证最终正确性.

TSMA计算时若原始子表内没有数据, 则可能不会创建对应的输出子表, 因此在count查询中, 即使配置了`countAlwaysReturnValue`, 也不会返回该表的结果.

当存在大量历史数据时, 创建TSMA之后, 流计算将会首先计算历史数据, 此期间新创建的TSMA不会被使用. 数据更新删除或者过期数据到来时自动重新计算影响部分数据。 在重新计算期间 TSMA 查询结果不保证实时性。若希望查询实时数据， 可以通过在 SQL 中添加 hint `/*+ skip_tsma() */` 或者关闭参数`querySmaOptimize`从原始数据查询。

## TSMA的使用与限制

客户端配置参数: `querySmaOptimize`, 用于控制查询时是否使用TSMA, `True`为使用, `False`为不使用即从原始数据查询.

客户端配置参数：`maxTsmaCalcDelay`，单位 s，用于控制用户可以接受的 TSMA 计算延迟，若 TSMA 的计算进度与最新时间差距在此范围内， 则该 TSMA 将会被使用， 若超出该范围， 则不使用， 默认值： 600（10 分钟）， 最小值： 600（10 分钟）， 最大值： 86400（1 天）.

客户端配置参数: `tsmaDataDeleteMark`, 单位毫秒, 与流计算参数`deleteMark`一致, 用于控制流计算中间结果的保存时间, 默认值为: 1d, 最小值为1h. 因此那些距最后一条数据时间大于配置参数的历史数据将不保存流计算中间结果, 因此若修改这些时间窗口内的数据, TSMA的计算结果中将不包含更新的结果. 即与查询原始数据结果将不一致.

### 查询时使用TSMA

已在 TSMA 中定义的 agg 函数在大部分查询场景下都可直接使用， 若存在多个可用的 TSMA， 优先使用大窗口的 TSMA， 未闭合窗口通过查询小窗口TSMA或者原始数据计算。 同时也有某些场景不能使用 TSMA(见下文)。 不可用时整个查询将使用原始数据进行计算。 

未指定窗口大小的查询语句默认优先使用包含所有查询聚合函数的最大窗口 TSMA 进行数据的计算。 如`SELECT COUNT(*) FROM stable GROUP BY tbname`将会使用包含count(ts)且窗口最大的TSMA。因此若使用聚合查询频率高时, 应当尽可能创建大窗口的TSMA.

指定窗口大小时即 `INTERVAL` 语句，使用最大的可整除窗口 TSMA。 窗口查询中， `INTERVAL` 的窗口大小， `OFFSET` 以及 `SLIDING` 都影响能使用的 TSMA 窗口大小， 可整 除窗口 TSMA 即 TSMA 窗口大小可被查询语句的 `INTERVAL， OFFSET， SLIDING` 整除的窗口。因此若使用窗口查询较多时, 需要考虑经常查询的窗口大小, 以及 offset, sliding大小来创建TSMA.

例 1. 如 创建 TSMA 窗口大小 `5m` 一条， `10m` 一条， 查询时 `INTERVAL(30m)`， 那么优先使用 `10m` 的 TSMA， 若查询为 `INTERVAL(30m, 10m) SLIDING(5m)`， 那么仅可使用 `5m` 的 TSMA 查询。


### 查询限制

在开启了参数`querySmaOptimize`并且无`skip_tsma()` hint时, 以下查询场景无法使用TSMA:

- 某个TSMA 中定义的 agg 函数不能覆盖当前查询的函数列表时
- 非 `INTERVAL` 的其他窗口，或者 `INTERVAL` 查询窗口大小（包括 `INTERVAL，SLIDING，OFFSET`）不是定义窗口的整数倍，如定义窗口为 2m，查询使用 5 分钟窗口，但若存在 1m 的窗口，则可以使用。
- 查询 `WHERE` 条件中包含任意普通列(非主键时间列)的过滤。
- `PARTITION` 或者 `GROUY BY` 包含任意普通列或其表达式时
- 可以使用其他更快的优化逻辑时， 如last cache优化, 若符合last优化的条件, 则先走last 优化, 无法走last时, 再判断是否可以走tsma优化
- 当前 TSMA 计算进度延迟大于配置参数 `maxTsmaCalcDelay`时

下面是一些例子:

```sql
SELECT agg_func_list [, pesudo_col_list] FROM stable WHERE exprs [GROUP/PARTITION BY [tbname] [, tag_list]] [HAVING ...] [INTERVAL(time_duration, offset) SLIDING(duration)]...;

-- 创建
CREATE TSMA tsma1 ON stable FUNCTION(COUNT(ts), SUM(c1), SUM(c3), MIN(c1), MIN(c3), AVG(c1)) INTERVAL(1m);
-- 查询
SELECT COUNT(*), SUM(c1) + SUM(c3) FROM stable; ---- use tsma1
SELECT COUNT(*), AVG(c1) FROM stable GROUP/PARTITION BY tbname, tag1, tag2;  --- use tsma1
SELECT COUNT(*), MIN(c1) FROM stable INTERVAL(1h);  ---use tsma1
SELECT COUNT(*), MIN(c1), SPREAD(c1) FROM stable INTERVAL(1h); ----- can't use, spread func not defined, although SPREAD can be calculated by MIN and MAX which are defined.
SELECT COUNT(*), MIN(c1) FROM stable INTERVAL(30s); ----- can't use tsma1, time_duration not fit. Normally, query_time_duration should be multple of create_duration.
SELECT COUNT(*), MIN(c1) FROM stable where c2 > 0; ---- can't use tsma1, can't do c2 filtering
SELECT COUNT(*) FROM stable GROUP BY c2; ---- can't use any tsma
SELECT MIN(c3), MIN(c2) FROM stable INTERVAL(1m); ---- can't use tsma1, c2 is not defined in tsma1.

-- Another tsma2 created with INTERVAL(1h) based on tsma1
CREATE RECURSIVE TSMA tsma2 on tsma1 INTERVAL(1h);
SELECT COUNT(*), SUM(c1) FROM stable; ---- use tsma2
SELECT COUNT(*), AVG(c1) FROM stable GROUP/PARTITION BY tbname, tag1, tag2;  --- use tsma2
SELECT COUNT(*), MIN(c1) FROM stable INTERVAL(2h);  ---use tsma2
SELECT COUNT(*), MIN(c1) FROM stable WHERE ts < '2023-01-01 10:10:10' INTERVAL(30m); --use tsma1
SELECT COUNT(*), MIN(c1) + MIN(c3) FROM stable INTERVAL(30m);  ---use tsma1
SELECT COUNT(*), MIN(c1) FROM stable INTERVAL(1h) SLIDING(30m);  ---use tsma1
SELECT COUNT(*), MIN(c1), SPREAD(c1) FROM stable INTERVAL(1h); ----- can't use tsma1 or tsma2, spread func not defined
SELECT COUNT(*), MIN(c1) FROM stable INTERVAL(30s); ----- can't use tsma1 or tsma2, time_duration not fit. Normally, query_time_duration should be multple of create_duration.
SELECT COUNT(*), MIN(c1) FROM stable where c2 > 0; ---- can't use tsma1 or tsam2, can't do c2 filtering
```

### 使用限制

创建TSMA之后, 对原始超级表的操作有以下限制:

- 必须删除该表上的所有TSMA才能删除该表.
- 原始表所有tag列不能删除, 也不能修改tag列名或子表的tag值, 必须先删除TSMA, 才能删除tag列.
- 若某些列被TSMA使用了, 则这些列不能被删除, 必须先删除TSMA. 添加列不受影响, 但是新添加的列不在任何TSMA中, 因此若要计算新增列, 需要新创建其他的TSMA.

## 查看TSMA
```sql
SHOW [db_name.]TSMAS;
SELECT * FROM information_schema.ins_tsma;
```
若创建时指定的较多的函数, 且列名较长, 在显示函数列表时可能会被截断(目前最大支持输出256KB).
