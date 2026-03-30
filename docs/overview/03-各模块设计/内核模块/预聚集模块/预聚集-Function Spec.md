# 预聚集-Function Spec

## 1. 修订记录

| **编写日期** | **发布日期** | **版本** | **修订人** | **主要修改内容** |
| --- | --- | --- | --- | --- |
| 2024-04-25 | 2024-04-25 | 1.0 | 王加明、徐开礼 | 正式发布 |
| 2025-12-31 | 2025-12-31 | 1.1 | 廖浩均 | 重构文档 |

## 2. 背景

在大数据量场景下, 经常需要查询某段时间内的汇总结果, 当历史数据变多或者时间范围变大时, 查询时间也会相应增加. 通过预聚集的方式可以将计算结果提前存储下来, 后续查询可以直接读取聚集结果, 而不需要扫描原始数据, 如当前Block内的SMA (Small Materialized Aggregates)信息.
Block内的SMA信息粒度较小, 若查询时间范围是日,月甚至年时, Block的数量将会很多, 因此TSMA (Time-Range Small Materialized Aggregates)支持用户指定时间窗口进行预聚集. 通过对固定时间窗口内的数据进行预计算， 并将计算结果存储下来， 查询时通过查询预计算结果以提高查询性能。

## 3. 定义

1. **SMA: **Small Materialized Aggregates, 即物化的聚集或者预聚集。包含了一个数据块的预聚集的结果，通常包含最小值、最大值、和、NULL值等信息。
2. **TSMA: **Time-range Small Materialized Aggregates, 即基于时间范围的预聚集。包含了按照时间段范围切分的数据的预聚集结果，也包含诸如最小值、最大值、和、NULL值信息。与 SMA 的区别是包含的数据是按照时间段划分，不是按照数据块为单位生成聚合结果。
3. **源数据表**: 是指创建TSMA时指定的需要计算的超级表或者普通表.
4. **输出表**: 是指TSMA计算的结果存储的表, 与源数据表相对应。
5. **管理节点（Mnode）**：TDengine 集群中负责监控和维护集群的运行状态，负责分布式事务管理、集群元数据（包括用户、数据库、超级表等）的管理，集群权限控制和安全控制的节点。
6. **虚拟节点（virtual node，Vnode）**：TDengine 系统中一种逻辑单元，若干个虚拟节点构成一个数据节点。每个虚拟节点包含缓存空间、磁盘空间、负责管理存储在该虚拟节点的时序数据、具备执行查询处理的消息队列和线程池。每个虚拟节点包含若干个表（子表）的数据。

## 4. 行为说明

### 4.1 基本流程

- 使用`CREATE TSMA`创建 TSMA， SQL 内指定表名称， aggregate 函数， 计算的列以及预计算的窗口大小。由于整个集群可创建的 TSMA 个数有限制（最多 12 个）， 且查询时若两个查询列分别创建在了两条 TSMA 中， 那么将导致这两条 TSMA 都无法使用， 因此建议创建时考虑多业务场景的使用需求， 指定业务中经常使用到的聚合函数以及对应的列。
```sql {wrap}
-- SQL示例
CREATE TSMA tsma1 ON test.meters FUNCTION(avg(c1), avg(c2), max(c1), max(c2), min(c1), min(c2), count(ts)) INTERVAL(5m);
CREATE RECURSIVE TSMA tsma2 ON test.tsma1 INTERVAL(10m);
CREATE RECURSIVE TSMA tsma3 ON test.tsma2 INTERVAL(30m);

CREATE TSMA tsma4 ON test.meters FUNCTION(avg(c1), avg(c2), max(c1), max(c2), min(c1), min(c2)) INTERVAL(7m); -- min, max, count, sum was created on all numeric columns
```

- 创建成功之后， 对原始表的正常查询即可使用 TSMA 预计算的结果。
```sql {wrap}
SELECT avg(c1), avg(c2), min(c1) FROM meters INTERVAL(10m); -- tsma2 with be used
SELECT avg(c1), avg(c2), min(c1), count(*) FROM meters ; -- tsma3 with be used
SELECT avg(c2), avg(c2), min(c1) FROM meters PARTITION BY tbname INTERVAL(10m); -- tsma2 with be used
SELECT avg(c3), avg(c2), min(c1) FROM meters PARTITION BY tbname INTERVAL(1h); -- tsma3 with be used
SELECT avg(c4), max(c3), min(c1) FROM meters WHERE ts >= '2024-01-01 10:00:00' and ts < '2024-01-02 10:00:00' PARTITION BY tbname INTERVAL(1h); -- tsma3 with be used
SELECT avg(c1), avg(c2), min(c1) FROM meters INTERVAL(8m); -- no tsma with be used
SELECT avg(c1), avg(c2), min(c1) FROM meters INTERVAL(1m); -- no tsma with be used
```

### 4.2 SQL 语法

#### 4.2.1 TSMA 操作语法

需要提供以下语法对 TSMA 进行操作，包括创建、删除、查看等操作。
```sql {wrap}
-- 创建基于超级表或普通表的tsma
CREATE TSMA tsma_name ON [dbname].table_name FUNCTION (func_name(func_param) [, ...] ) INTERVAL(time_duration);

-- 创建基于小窗口tsma的大窗口tsma
CREATE RECURSIVE TSMA tsma_name2 ON [db_name.]tsma_name1 INTERVAL(time_duration);

time_duration:
    number unit

-- 删除
DROP TSMA [db_name.]tsma_name;

-- SHOW CREATE
SHOW CREATE TSMA [db_name.]tsma_name;
-- SHOW TSMAS FROM, 或者从information_schema.ins_tsma表中查询
SHOW [db_name.]TSMAS;
SELECT * FROM information_schema.ins_tsma;
```

约束规则：
1. 当基于 TSMA 创建时, 即使用 `RECURSIVE` 关键字， 不需要指定 `FUNCTION（）`创建将与已有 TSMA 相同的函数列表， 且 `INTERVAL` 必须为所基于的TSMA窗口的整数倍。
2. `INTERVAL` 内指定窗口大小。如 1h， 则创建的 TSMA 将会以 1h 为窗口大小计算聚合结果。必须指定单位, 单位格式与查询interval相同.
3. 由于 TSMA 的计算结果将会输出到超级表中， 包括原始超级表的所有tag列, 因此所指定的函数个数`最大值`是`表列数最大值 - 原始所有列 - 4`， 其中 4 列分别为`_wstart`， `_wend`， `_wduration`,  `tbname`。 若列数超出限制， 则会报`Too Many Columns`错误。
4. 在`show tsmas` 语句的结果集中, 列最大长度为256KB, 超出的长度会被截断。

#### 4.2.2 RSMA 操作语法

```sql {wrap}
-- RSMA 表各个存储层级的保存时长，通过 DB KEEP 参数指定，分别为 keep 0,keep 1,keep 2。
CREATE DATABASE [IF NOT EXISTS] db_name KEEP keep0[,keep1[,keep2]]；

-- 创建基于超级表的 RSMA(子表/普通表等其他类型的表不支持)。
CREATE RSMA [IF NOT EXISTS] rsma_name ON [dbname.]table_name FUNCTION([func_name(col_name)[,...]]) INTERVAL(interval1[,interval2]; 

-- 删除 RSMA 
DROP RSMA [IF EXISTS] [db_name.]rsma_name;

-- 修改列的聚合函数(适用于新增列的场景，只允许修改未指定 function 的列。注意：因为未指定聚合函数时，默认聚合函数为 last，在修改聚合函数后，会存在聚合结果的类型不一致。)
ALTER RSMA [IF EXISTS] [db_name.]rsma_name FUNCTION ([func_name(col_name)[,...]])

-- 显示创建语句
SHOW CREATE RSMA [db_name.]rsma_name;

-- 显示所有 RSMA
SHOW [db_name.]RSMAS;
SELECT * FROM information_schema.ins_rsmas;

-- 手动重算 RSMA 在某一时间范围的数据，可指定 vgroups。1）未指定时间范围时，计算 keep 在 2/3 级的所有文件组；2）指定时间范围时，则计算时间范围内包含的整个文件组的。3）如果 rollup 后，未写入新的数据，则不会重复 rollup。4）如果 rollup 时间范围的文件组在 keep 1，则不进行计算，即 rollup 只对 keep 2/3 的文件组生效。
-- 手动重算适用于对不满足多级存储迁移条件的 2/3 级文件组进行计算。需要注意的是：如果需要重算的文件组已经在 s3 上，则重算生成的文件组会重新保存到本地，参照 7.1 节的说明。
ROLLUP DATABASE db_name [start_opt end_opt]
ROLLUP [db_name] VGROUPS IN (vgroup_ids) [start_opt end_opt]

start_opt(A) ::= .   
start_opt(A) ::= START WITH NK_INTEGER(B).
start_opt(A) ::= START WITH NK_STRING(B).
start_opt(A) ::= START WITH TIMESTAMP NK_STRING(B).
end_opt(A) ::= .
end_opt(A) ::= END WITH NK_INTEGER(B).
end_opt(A) ::= END WITH NK_STRING(B).
end_opt(A) ::= END WITH TIMESTAMP NK_STRING(B).


-- 显示所有的 retention 任务。RSMA 计算任务，均通过 retention 线程完成，在 retention 任务中，增加是否为 rsma task 的标识，可区分自动触发和手动触发。
SHOW [db_name.]RETENTIONS; 
SELECT * FROM information_schema.ins_retentions; 
SHOW RETENTION retention_task_id; -- 显示详细信息，包含进度。
KILL RETENTION retention_task_id; -- 终止指定 id 的 RSMA 计算任务。
```


### 4.3 TSMA 聚合支持的函数列表

| **支持函数** | **说明** |
| --- | --- |
| min | 无 |
| max | 无 |
| sum | 无 |
| first | 无 |
| last | 无 |
| avg | 无 |
| count | 若要在查询中使用count(*)函数, 应当创建count(ts)函数 |
| spread | 无 |
| stddev | 无 |
| hyperloglog | 无 |

创建时指定了不支持的函数将会报`Invalid TSMA aggregate function`错误。函数内只支持指定一列作为参数, 即使该函数支持多个参数, 并且所使用列不能为tag列.

### 4.4 创建/删除的限制与约束

#### 4.4.1 TSMA 限制与约束

1. TSMA 在库内名字唯一，名字命名规则与表名相同,  作为数据库内部对象。TSMA 可与超级表名重复（但不建议）。由于tsma创建的流名字与tsma名字相同, 而流为全局对象, 非库下对象, 因此tsma名字在不同库下也不能相同，`TSMA` 名字的长度不能超过 `178 Bytes`.
2. 递归创建时可以使用已有 TSMA 的计算结果， 加快新创建的 TSMA 的计算速度。但创建条件有一定的限制。
3. 限制集群内 TSMA 可创建的个数。添加配置参数`maxTsmaNum`， 默认值：8， 范围： 0 - 12。
4. 仅允许在超级表或普通表上创建或基于其他 TSMA 创建。
5. 基于小窗口创建时窗口大小必须是小窗口大小的整数倍。 如 120s 可以基于 1m 创建， 支持创建的最大窗口为 1y或者12个月， 最小窗口为 1分钟, 1天只能基于1h建立, 不能基于2h,3h等建立, 1月只能基于1天建立, 不能基于2天或者其他天建立。
6. 如果某个表上某一列被 TSMA 使用了， 则该列不允许被删除（ 必须先 DROP TSMA， 再删除列）； 添加列不受影响， 新增的列不会自动计算。若想计算新增列， 需要删除原始 TSMA，重新创建一个。只对新增列创建一个 TSMA 只能解决单独查询该新增列的场景， 与其他列一起查询时新旧 TSMA 都无法使用。
7. 若某张超级表上创建了 TSMA， 那么该表的所有 tag 都无法删除，无法改名字, 所有 tag 列无法修改值， 增加新的 tag 不受影响。 若查询中使用了该新创建的 tag， 则 TSMA 无法使用, 此时会使用原始数据进行计算。
8. 删除 TSMA 时， 若存在其他窗口的 TSMA 基于当前窗口 TSMA 创建，会报错， 需要先删除其他 TSMA。
9. TSMA所创建的流个数也包括在集群整体流个数限制之内.
10. 由于TSMA输出为一张超级表, 因此输出表的行长度受最大行长度限制, 不同函数的`中间结果`大小各异, 一般都大于原始数据大小, 若输出表的行长度大于最大行长度限制, 将会报`Row length exceeds max length`错误.

#### 4.4.2 RSMA 限制与约束

1. 要实现 RSMA 迁移时自动降采样计算，DB 的 [KEEP](https://docs.taosdata.com/reference/taos-sql/database/#keep) 参数需满足 keep0 < keep1 < keep 2。如果未配置多级存储，则不会迁移数据，但仍然会在原存储层级进行降采样存储。
2. RSMA 名称 rsma_name 在集群内唯一。
3. RSMA func_name 取值范围为 min, max, sum, avg, first, last，count/spread/cols/interp 函数均不支持。
4. RSMA 每一个非主键列均可指定一个 func_name，但是只能指定一个，非数值类型的列不能指定 sum/avg 等数值计数函数。FUNCTION 参数可为空。未显式指定 func_name  的列默认函数为 last。
5. 非 Primary Key 复合主键列，func_name 仅可指定 first/last，不指定默认函数为 last。
6. RSMA interval 取值范围为 [0, duration] 之间整数，至少一个 非 0。interval 1 取 0，表示多级存储 level 2 存储层级的数据不进行降采样存储；interval 2  取 0，表示多级存储 level 3 存储层级的数据不进行降采样存储。
7. RSMA 表的 interval 都大于 0 时，需要满足 interval1 < interval2 <= duration， 且必须指定单位，单位格式与查询 [interval ](https://docs.taosdata.com/reference/taos-sql/distinguished/#%E7%AA%97%E5%8F%A3%E5%88%87%E5%88%86%E6%9F%A5%E8%AF%A2)相同。
8. 跨文件边界的 interval 计算，即使不考虑 s3，也会增加系统的复杂性、资源消耗和文件碎片化，降低查询读取效率，并且带来的收益较低。因此，约定 DB duration 参数必须能够被 RSMA 表的 interval 整除。
```sql {wrap}
假设 duration 为 1 天（86400s），interval 为 1 至 3600 之间的数字，则可以做为 interval 的有 82 个（1, 2, 3, 4, 5, 6, 8, 9, 10, 12, 15, 16, 18, 20, 24, 25, 27, 30, 32, 36, 40, 45, 48, 50, 54, 60, 64, 72, 75, 80, 90, 96, 100, 108, 120, 128, 135, 144, 150, 160, 180, 192, 200, 216, 225, 240, 270, 288, 300, 320, 360, 384, 400, 432, 450, 480, 540, 576, 600, 640, 675, 720, 800, 864, 900, 960, 1080, 1152, 1200, 1350, 1440, 1600, 1728, 1800, 1920, 2160, 2400, 2700, 2880, 3200, 3456, 3600），基本上不会对用户的需求造成影响。
```

1. 为保证 interval 1/2 窗口聚合结果相对于原始数据的正确性，约定 interval 2 必须为 interval 1 的整数倍(interval 1/2 均为非 0 正整数时)。这样，可以保证 min/max/sum/first/last 结果的正确性；但是，avg 结果正确性仍然无法保证，还可能有误差。

### 4.5 TSMA 的计算

1. TSMA 的存在形式是用户不可见的表，不可直接删除， 在 DROP TSMA 时删除。该表中存储有按照时间段进行聚合的结果，作为与原始数据表进行关联的（预计算）表。 
2. TSMA 是通过流计算进行实时的计算，并随着原始表的数据的变化进行变化。考虑到流计算与数据写入过程是异步， 写入 TSMA 的预计算表中的计算结果并不是与写入的时序数据同步更新，但可以保证最终的正确性。
3. 当创建 TSMA 时，若存在大量历史数据， 那么在计算历史数据期间，TSMA 将不可用，TSMA的可用性及状态并不影响用户的查询，查询引擎会自行判断是否调用 TSMA 的预计算结果，或者基于原始的时序数据执行查询。
4. 数据更新删除或者过期数据到来时自动重新计算影响部分数据。 在重新计算期间 TSMA 查询结果不保证实时性。若希望查询实时数据， 可以通过在 SQL 中添加 hint `/*+ skip_tsma() */` 或者关闭参数`querySmaOptimize`从原始数据查询。
5. 增加配置参数： `maxTsmaCalcDelay`， 单位`秒（s）`， 用户可以接受的 TSMA 计算延迟， 若 TSMA 的计算进度与最新时间差距在此范围内， 则该 TSMA 将会被使用， 若超出该范围， 则不可用， 默认值： 600（10 分钟）， 最小值： 600（10 分钟）， 最大值： 86400（1 天）。

### 4.6 查询时使用 TSMA

1. 复用客户端现有配置项： `querySmaOptimize`，可通过`alter local`修改， 如开启则在查询时优先使用 TSMA 计算结果， 关闭时则屏蔽任何 TSMA， 只从原始数据进行计算。
2. 已在 TSMA 中定义的 agg 函数在大部分查询场景下都可直接使用， 若存在多个可用的 tsma， 优先使用大窗口的 tsma， 未闭合窗口通过查询小窗口或者原始数据计算。 同时也有某些场景不能使用 tsma。 不可用时整个查询将使用原始数据进行计算。
3. **不可用的场景包括：** 
  - 某个TSMA 中定义的 agg 函数不能覆盖当前查询的函数列表时无法使用当前TSMA。 如下面的 line 9；
  - 非 INTERVAL 的其他窗口， 或者 INTERVAL 查询窗口大小（包括 INTERVAL， SLIDING， OFFSET）不是定义窗口的整数倍, 即无法通过组合多个tsma窗口生成查询的interval时， 如定义窗口为 2m， 查询使用 5 分钟窗口， 但若存在 1m 的窗口， 则可以使用, 注意, 由于时区差异, 查询一天的窗口时, 1h的tsma窗口可用, 2h或其他整小时的窗口不可用, 查询一月的窗口时, 1d的窗口可用, 但是2d或其他整天的窗口不可用。
  - 查询 where 条件中包含任意普通列的过滤。 如 line 11；若仅有针对主键 TS 列的简单过滤时可用， 目前仅支持 between， 比较操作中操作数为常数时间戳的过滤， 如 line 20。
  - PARTITION 或者 GROUY BY 包含任意普通列时不能使用任何 TSMA。
  - 可以使用其他更快的优化逻辑时， 如`last cache`优化, 若符合last优化的条件, 则先走last 优化, 无法走last时, 再判断是否可以走tsma优化。
  - 当前 TSMA 计算进度延迟大于配置参数 `maxTsmaCalcDelay`时。
```sql {wrap}
SELECT agg_func_list [, pesudo_col_list] FROM stable WHERE exprs [GROUP/PARTITION BY [tbname] [, tag_list]] [HAVING ...] [INTERVAL(time_duration, offset) SLIDING(duration)]...;

-- 创建
CREATE TSMA tsma1 on stable FUNCTION(COUNT(*), SUM(c1), SUM(c3), MIN(c1), MIN(c3), AVG(c1)) INTERVAL(1m);
-- 查询
SELECT COUNT(*), SUM(c1) + SUM(c3) FROM stable; ---- use tsma1
SELECT COUNT(*), AVG(c1) FROM stable GROUP/PARTITION BY tbname, tag1, tag2;  --- use tsma1
SELECT COUNT(*), MIN(c1) FROM stable INTERVAL(1h);  ---use tsma1
SELECT COUNT(*), MIN(c1), SPREAD(c1) FROM stable INTERVAL(1h); ----- can't use, spread func not defined, although SPREAD can be calculated by MIN and MAX which are defined.
SELECT COUNT(*), MIN(c1) FROM stable INTERVAL(30s); ----- can't use tsma1, time_duration not fit. Normally, query_time_duration should be multple of create_duration.
SELECT COUNT(*), MIN(c1) FROM stable where c2 > 0; ---- can't use tsma1, can't do c2 filtering
SELECT COUNT(*) FROM stable GROUP BY c2; ---- can't use any tsma
SELECT MIN(c3), MIN(c2) FROM stable INTERVAL(1m); ---- can't use any tsma, c2 is not defined in tsma.

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

1. **查询时 TSMA 的选择**
  - 未指定窗口大小的查询语句默认优先使用包含所有查询聚合函数的最大窗口 TSMA 进行数据的计算。 如`SELECT COUNT(*) FROM stable GROUP BY tbname;`。
  - 指定窗口大小时即 interval 语句，使用最大的可整除窗口 TSMA。 窗口查询中， INTERVAL 的窗口大小， OFFSET 以及 SLIDING 都影响能使用的 TSMA 窗口大小， 可整 除窗口 TSMA 即 TSMA 窗口大小可被查询语句的 INTERVAL， OFFSET， SLIDING 整除的窗口。
例 1。 如 创建 TSMA 窗口大小 5m 一条， 10m 一条， 查询时 INTERVAL（30m）， 那么优先使用 10m 的 TSMA， 若查询为 INTERVAL（30m， 10m） SLIDING（5m）， 那么仅可使用 5m 的 TSMA 查询。
- 在带主键时间列的 WHERE 条件时，若开始和结束时间与窗口不对齐， 那么边界窗口会从其他对齐的小窗口 TSMA 中计算， 若不存在对齐的小窗口 TSMA， 那么直接从原始数据进行计算。
例 2。 如创建了 5m 和 10m 两条 TSMA: tsma1， tsma2， 查询时 INTERVAL（10m）， WHERE 条件为 `ts >= '2024-01-01 10:05:00.000' and ts < '2024-01-01 11:00:00.000'`， 那么时间区间： `['10:05:00.000', '10:10:00.000')`的数据由 tsma1 计算， 剩下部分由 tsma2 计算。
例 3。 若不存在能对齐窗口的 TSMA， 那么这部分数据由原始数据计算， 如还是上述的两条 tsma， 查询为： INTERVAL（10m）， WHERE 条件为： `ts >= '2024-01-01 10:05:00.000' and ts < '2024-01-01 11:04:00.000'`， 那么时间区间： `['10:05:00.000', '10:10:00.000')`的数据由 tsma1 计算， 时间区间： `['10:10:00.000', '11:00:00.000')`由 tsma2 计算。 剩下的`['11:00:00.000', '11:04:00.000')`由原始数据进行计算。
注意： 上述例子中 WHERE 条件的右侧都为开区间， 而 SQL 中的`BETWEEN`的右侧为闭区间， 当 WHERE 的右侧（仅右侧）使用闭区间时， 最右侧的数据一定会使用原始数据进行计算。 即使右侧时间与 TSMA 窗口对齐， 如上述例 2， 若 WHERE 条件右侧为闭区间， 那么时间区间： `['10:05:00.000', '10:10:00.000')`的数据由 tsma1 计算， 时间区间`['10:10:00.000', '11:00:00.000')`的数据由 tsma2 计算。 时刻`'11:00:00.000'`的数据将由原始数据计算。

### 4.7 创建/删除 RSMA 及运行规则

1. RSMA 降采样计算支持自动触发。通过 DB keep (keep0/keep1/keep2) 参数控制，由`低层级`向`高层级`迁移时自动完成降采样存储。
2. RSMA 降采样计算支持手动触发。适用于`降采样存储的数据更新或删除后需要重算` 以及 `已经迁移至目标存储层级但未进行降采样存储`的场景。
```sql {wrap}
1/2/3 存储层级的数据均支持更新和删除，但是更新和删除不会触发重算。因此，level 2/3 层级存在乱序数据写入时，原有聚合过的窗口内可能存在多条数据；此时进行手动重算会基于聚合数据和乱序数据，对于 avg/first/last 等函数再次聚合的结果可能不准确，对于 min/max/sum 等函数无影响。
```

1. 存在 RSMA 时，可以进行表结构修改、表的创建删除，这些操作在计算和重算时延迟生效。
2. RSMA 计算完成后删除原有数据文件，降采样数据与原始数据时间范围不存在重叠。
3. RSMA 和 S3 迁移之间存在依赖关系，在 S3 首次迁移时，需要进行 RSMA 的计算。
4. RSMA 不影响任何查询行为。因为只是更新了数据文件。如果查询范围跨越了存储层级，有可能同时返回原始数据和降采样数据。

### 4.8 RSMA 查询规则

1. 查询时，根据查询的时间范围，自动查询对应区间的数据。
2. 如果某次查询的时间范围覆盖了多个 retention level，则只查询并返回最高 retention level 的数据。
3. 如果未指定查询时间，则默认时间范围为 [INT64_MIN, INT64_MAX]，只查询最高 retention level 的数据。

### 4.9 其他场景

- 若由于 dnode 故障等因素导致查询 TSMA 失败，将重新查询原始数据进行计算。除查询时间稍长以外用户无感知。

## 5. 性能

创建 TSMA 之后， 计算历史数据时或存在删除以及插入过期数据等操作时会占用较多的系统资源用于计算， 计算结束之后无影响。
通过 TSMA 的查询性能高于通过原始数据的查询性能。

## 6. 安全

授权用户才在所属的数据库中创建、删除 TSMA/RSMA。针对目标表具备访问权限的用户才能够对指定（超级）表创建相应 TSMA/RSMA。
用户针对 TSMA/RSMA 创建、删除操作需要记录审计日志，以满足安全审计和核查的需求。
此外，TSMA/RSMA 的安全控制与TDengine现有其他安全机制协同工作，TSMA 运行期间依托于系统提供的安全传输通道确保网络与传输安全，在从服务端传输到客户端的过程中，应与普通查询数据采用相同的加密信道进行保护，防止数据在传输中被窃听。

## 7. 兼容性

无。

## 8. 运维

TSMA 信息可以通过 SQL `SHOW TSMAS`查看， 其中包括了当前库下的所有 TSMA， 返回结果中每行包括了当前 TSMA 的基本信息， 如名字， 输入表， 输出表， 函数列表， 窗口大小等， 以及当前 TSMA 的计算状态， 计算进度等。

## 9. 使用场景

#### 9.0.1 推荐使用的场景

若业务中需要对某张表的某些列经常性的做聚合操作， 可以是 partition/group by tbname， 或者 partition/group by 某些 tag 列， 或者完全没有 partition/group by。 典型 SQL 如：
```sql
SELECT avg(c1), min(c2), max(c3), count(*) from meters;
SELECT avg(c1), min(c2), max(c3), count(*) from meters partition by tbname;
SELECT avg(c1), min(c2), max(c3), count(*) from meters partition by tag1, tag2;
```

或者还希望对 ts 主键列进行时间戳的简单过滤：
```sql
SELECT avg(c1), min(c2), max(c3), count(*) 
FROM meters WHERE ts BETWEEN 1705300039000 AND 1705400039000;

SELECT avg(c1), min(c2), max(c3), count(*) 
FROM meters WHERE ts BETWEEN 1705300039000 AND 1705400039000 PARTITION BY tbname;

SELECT avg(c1), min(c2), max(c3), count(*) 
FROM meters WHERE ts BETWEEN 1705300039000 AND 1705400039000 PARTITION BY tag1, tag2;
```

或者带有窗口查询的需求如：
```sql {wrap}
SELECT avg(c1), min(c2), max(c3), count(*) 
FROM meters WHERE ts BETWEEN 1705300039000 AND 1705400039000 INTERVAL(30m);

SELECT avg(c1), min(c2), max(c3), count(*) 
FROM meters 
WHERE ts BETWEEN 1705300039000 AND 1705400039000 
PARTITION BY tbname INTERVAL(30m);

SELECT avg(c1), min(c2), max(c3), count(*) 
FROM meters 
WHERE ts BETWEEN 1705300039000 AND 1705400039000 
PARTITION BY tag1, tag2 INTERVAL(30m);
```

此时可以创建如下 TSMA:
```sql
CREATE TSMA tsma1 
ON meters 
FUNCTION(avg(c1), min(c2), max(c3), count(*)) 
INTERVAL(30m);
```

若有其他窗口大小的需求， 如 50m， 则可以创建一个最大公约数的窗口大小， 即 30m 和 50m 的最大公约数， 10 分钟的窗口。 这样就可在查询 30m 和 50m 窗口时都可使用 TSMA 的预计算结果。

#### 9.0.2 不推荐使用的场景

若业务 sql 中需要对某些普通非主键 ts 列做过滤， 则不应该对该场景创建 TSMA， 如 sql:
```sql {wrap}
SELECT avg(c1), min(c2), max(c3), count(*) 
FROM meters 
WHERE ts BETWEEN 1705300039000 AND 1705400039000 and c3 > 100;
```

若聚合操作中需要先对列进行计算时也无法通过 TSMA 来提高查询性能，如 sql:
```sql {wrap}
SELECT avg(c1 + 10), min(c2), max(c3), count(*) 
FROM meters 
WHERE ts BETWEEN 1705300039000 AND 1705400039000;
```

若只是简单查询 last， 并且可以使用 Last 缓存的情况下，  不建议创建 TSMA。
若业务查询中， 经常使用窗口查询， 且窗口大小不定或无最小的可整除窗口， 或可整除窗口很小时则建立 TSMA 意义不大。 如经常使用如下业务 sql:
```sql {wrap}
SELECT avg(c1), min(c2), max(c3), count(*) FROM meters WHERE ts BETWEEN 1705300039000 AND 1705400039000 INTERVAL(7m);
SELECT avg(c1), min(c2), max(c3), count(*) FROM meters WHERE ts BETWEEN 1705300039000 AND 1705400039000 INTERVAL(54s);

 --若以上两条语句经常被使用, 那么支持的TSMA最大窗口为6s,即420s和54s的最大公约数
 
SELECT avg(c1), min(c2), max(c3), count(*) FROM meters WHERE ts BETWEEN 1705300039000 AND 1705400039000 INTERVAL(13s, 1s) SLIDING(3s);
 --若以上语句经常需要查询, 那么可使用的TSMA窗口最大为 1s.
```

多表低频场景不建议建立窗口较小的TSMA. 因为窗口较小时, 对数据的聚集效果不明显, 导致数据量依然很大, 查询性能不能得到提升.

## 10. 约束和限制

见 4.3 创建和删除限制以及 4.5 查询时使用 TSMA。归纳几点如下：
- 仅可使用支持的函数列表内的函数创建 TSMA。
- 仅支持基于普通表或者超级表创建， 不支持基于子表创建。
- TSMA 窗口的最大值为 1年或12个月。
- 创建的 TSMA 条数受参数`maxTsmaNum`限制。
- 对已经创建了 TSMA 的列， 将无法 `drop column`， 当前表也无法 drop， 需要先 drop tsma 才能操作。
- 对已经创建了 TSMA 的超级表， 所有 TAG 都无法删除或者修改值。 需要先 DROP 所有 TSMA 才能操作。
- 在删除递归创建的 TSMA 时， 需要先删除大窗口的 TSMA， 再删除小窗口的 TSMA。
- 创建 TSMA 时， 若存在大量历史数据， 那么在计算历史数据期间，查询将自动使用原始数据或者其他计算进度已经满足要求的 TSMA 进行计算。
- TSMA 的计算是通过创建流计算任务完成的，其计算结果不是实时的， 因此在查询时若计算进度大于配置参数`maxTsmaCalcDelay`则无法使用 TSMA ， 查询将会自动使用原始数据进行计算。
- 数据更新删除或者过期数据到来时自动重新计算影响部分数据。 在重新计算期间 TSMA 查询结果不保证实时性。
- 查询时对 SQL 的使用限制见 4.5。

## 11. 常见错误和排查

如何确定当前 SQL 是否使用了 TSMA 进行计算， 可通过`explain verbose true`查看当前执行计划， 若计划中使用扫描表中存在 tsma 的输出表， 即使用了 TSMA。 计划中也包含使用了哪些 TSMA 信息。
可通过`SHOW TSMAS`查看 tsma 创建的窗口大小， 函数列表， 输出超级表的名称， 以及关联的流计算任务。

## 12. 参考文档

无
