# Explain analyze 优化 FS

## 1. 背景

Explain 语句返回一个查询语句的物理执行计划。Explain analyze 语句会实际执行一个查询语句，并在其物理执行计划的基础上，输出实际执行时间、I/O等执行过程中产生的信息。注意，explain analyze 仅记录物理算子执行过程，对于 query 等待、与taosc数据传输阶段的信息并未记录。
当前对 explain 有关的需求可以总结为以下几点：
1. Explain analyze 对算子实际执行时间、起止时间的统计不准确
2. 交付人员希望 explain analyze 算子能显示更多信息帮助定位性能瓶颈
3. Explain analyze 结果可读性增强
4. 官网文档缺少对 explain analyze 语句的介绍
5. Explain 支持增删改语句
6. Explain ratio 功能开发

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2026-02-03 | 0.1 | @张天毅 | 初步创建 |
| 2026-02-05 | 1.0 | @张天毅 | 初版发布 |
| 2026-02-09 | 1.1 | @张天毅 | 完善文档，增加计算细节 |
| 2026-02-10 | 1.2 | @张天毅 | 针对slowest vgroup增加指标，帮助诊断问题 |

## 3. 定义

**算子启动时间（exec_start）**：算子首次开始执行的时刻
**算子等待时间（input_wait_elapsed & ouput_wait_elapsed）**：算子不进行内部计算，而是等待其他算子的累计时间，这里又分两种情况，即等待下游算子输入的时间和等待上游算子输出的时间
**算子处理时间（exec_elapsed）**：算子的实际执行时间

## 4. 现状及问题

### 4.1 算子执行时间

目前 explain analyze 结果中，输出了算子的“起始时间”和“总执行时间”，如
```sql
taos> explain analyze select * from ctb1\G;
*************************** 1.row ***************************
QUERY_PLAN: -> Table Scan on ctb1 (cost=0.000..0.072 ...)
*************************** 2.row ***************************
...
```

且不说目前这俩个值的计算是否准确，通过这两个值并无法得出算子的实际执行时间，因为算子获取下游计算结果或向上游返回计算结果时都可能发生等待，这对性能调优的指导意义还不够大。而且这两个值目前的计算方法也不准确。

### 4.2 算子其他参数

Ratio关键字的采样率功能目前没有实现，但不设置时会输出一个错误值，如
```sql
taos> explain analyze select * from ctb1\G;
*************************** 1.row ***************************
...
*************************** 4.row ***************************
QUERY_PLAN: Ratio: 0.001000
*************************** 5.row ***************************
...
```

## 5. 行为说明

### 5.1 算子执行时间

#### 5.1.1 新增指标

为了方便性能调优，执行时间有关的指标变为以下八个：

| 指标 | 含义 | 诊断价值 | 输出级别 | 计算方法 |
| --- | --- | --- | --- | --- |
| exec_first_row | 算子返回第一条数据的时刻 |  | 普通 | 在 next 方法首次返回时初始化 |
| exec_last_row | 算子最后一次返回数据的时刻 |  | 普通 | 在 next 方法首次返回时初始化，每次返回数据不为空则更新 |
| exec_elapsed | 算子的实际执行时间 | 算子本身的计算开销 | verbose | **同步算子**：在 next 方法开始时开始计时，在 getNextBlockFromDownstream 调用时停止，getNextBlockFromDownstream 返回后再次开始计时，next 方法返回前停止计时 **异步算子（exchange）**：在 next 方法开始时开始计时，开始等待返回数据时（exchangeWait 执行前）暂时停止；接受到返回数据后（exchangeWait 结束后）再次开始计时，next 方法结束时停止 |
| exec_create | 算子的创建时间 | 算子何时被调度 | verbose | 在算子的 create 方法被调用时立即初始化 |
| exec_start | 算子首次开始执行的时刻 | 算子何时被调度 | verbose | 在 next 方法第一次被调用时初始化 |
| exec_times | 算子执行次数 |  | verbose | Next 方法的调用次数 |
| input_wait_elapsed | 等待下游数据的时间 | 下游是否是瓶颈 | verbose | **同步算子**：在 getNextBlockFromDownstream 调用时开始，getNextBlockFromDownstream 返回后停止 **异步算子（exchange）**：exchangeWait 的累计执行时间 |
| output_wait_elapsed | 等待上游消费的时间 | 上游是否是瓶颈 | verbose | next 方法结束时开始，再次调用时结束 |

后五个指标放在verbose级别输出。Exec_elapsed 等三个时间指标单位为毫秒，精确到小数点后三位，等价于精确到微秒。
修改后的格式类似：
```sql
taos> explain analyze select * from ctb1\G;
*************************** 1.row ***************************
QUERY_PLAN: -> Table Scan on ctb1 (cost=exec_first_row..exec_last_row ...)
*************************** 2.row ***************************
...


taos> explain analyze verbose true select * from ctb1\G;
*************************** 1.row ***************************
QUERY_PLAN: -> Table Scan on ctb1 (cost=exec_first_row..exec_last_row ...)
*************************** 2.row ***************************
QUERY_PLAN:       Exec: compute=exec_elapsed create=exec_create start=exec_start times=exec_times input_wait=input_wait_elapsed output_wait=output_wait_elapsed
*************************** 3.row ***************************
...
```

### 5.2 其他算子指标

除去算子执行时间相关指标外，为方便优化性能和判断性能瓶颈，为算子新增几个通用指标，为个别算子新增专属指标。

#### 5.2.1 通用指标（所有算子均包含）

| 指标 | 含义 | 诊断价值 | 输出级别 | 计算方法 | 备注 |
| --- | --- | --- | --- | --- | --- |
| filter_efficiency | 过滤后剩余数据量，反映过滤效率 | 比值越小，过滤越有效 | verbose | Rows_out / rows_in | 放在现有的filter部分内 |

#### 5.2.2 专属指标

##### 5.2.2.1 I/O cost（table scan 下级指标）

##### 5.2.2.2 以下指标输出各个 vgroups 的最大值和平均值（verbose 级别）。

| 指标 | 含义 | 操作（新增/删除/保留？） | 输出级别 | 备注 |
| --- | --- | --- | --- | --- |
| total_blocks | 总blocks数目 | 保留 | 普通 |  |
| load_blocks | data、sst和 buffer 读取 block 数目 | 删除 |  | 拆分 file blocks和buffer blocks |
| file_load_blocks | File 读取 block 数目 | 新增 | verbose | 日志中已统计，包括data file和stt file |
| file_load_elapsed | File 读取耗费时间 | 新增 | verbose | 日志中已统计，包括data file和stt file |
| stt_load_blocks | Stt 读取 block 数目 | 新增 | verbose | 日志中已统计 |
| stt_load_elapsed | Stt 读取耗费时间 | 新增 | verbose | 日志中已统计 |
| mem_load_blocks | Mem 读取 block 数目 | 新增 | verbose | 尚未统计，实现方法与mem_load_elapsed类似 |
| mem_load_elapsed | 生成 mem block 耗费时间 | 新增 | verbose | 日志中已统计 |
| load_block_SMAs -> sma_load_blocks | Sma 读取 block 数目 | 保留 | verbose |  |
| sma_load_elapsed | Sma 读取耗费时间 | 新增 | verbose | 日志中已统计 |
| composed_blocks | 因数据交织合成 blocks 数目 | 新增 | verbose | 日志中已统计 |
| composed_elapsed | 合并交织数据用时 | 新增 | verbose | 日志中已统计 |
| total_rows | Scan 总行数 | 保留 | 普通 |  |
| check_rows | 通过 scan 过滤的行数 | 保留 | 普通 |  |
| max_row_task -> max_row_vgroup_id | ~~输出行数最多的 task id~~ 输出行数最多的 vgroup_id | 删除 |  | vgroup_id 目前没有随 explain_rsp 从服务端返回，需要在 msg 中添加该字段，或通过其他方式获取 |
| total_rows | Scan 总行数 | 删除 |  | 重复输出 |
| ep | endpoint | 删除 |  |  |
| slowest_vgroup_id | 最慢节点的 vgroup_id | 新增 | verbose |  |
| slow_deviation_rate | 最慢节点耗时相对中位数偏离率 | 新增 | verbose | <equation>\frac{max\_time-median\_time}{median\_time} \times 100\% </equation>，反映最慢节点离群程度 |
| cost_ratio | 最慢/最快耗时倍数 | 新增 | verbose | <equation>{max\_time}/{min\_time} </equation>，反映最慢节点离群程度 |
| data_deviation_rate | 最慢节点行数相对中位数偏离率 | 新增 | verbose | <equation>\frac{max\_rows-median\_rows}{median\_rows} \times 100\% </equation>，数据偏离率，区分数据倾斜/节点异常 |

##### 5.2.2.3 Network（Exchange 下级指标）

| 指标 | 含义 | 诊断价值 | 输出级别 | 备注 |
| --- | --- | --- | --- | --- |
| mode | 并发还是顺序请求数据 |  | verbose |  |
| fetch_times | 数据源 fetch 次数 |  | verbose | 输出平均值和最大值 |
| fetch_rows | 数据源获取数据行数 |  | verbose | 输出平均值和最大值 |
| fetch_cost | 数据源 rpc 往返耗时 |  | verbose | 输出平均值和最大值 计算方法：数据源 rpc 收到回复时间 - rpc 发送时间 |

### 5.3 Verbose 级别和可读性

#### 5.3.1 输出格式

当算子分发到多个 vgroups 上执行时，为方便阅读不会将各个 vgroup 上该算子的指标全部输出，而是对每个指标聚合计算平均值和最大值；当算子在单个 vgroup 上执行时，其各项指标可以直接输出，不需要计算平均值等。例如 cost 指标，会输出为`cost=``**avg**``_exec_first_row(``**max**``_exec_first_row)..``**avg**``_exec_last_row(``**max**``_exec_last_row)`，首先输出平均值，之后在括号中输出最大值。一些全局类型的指标除外，例如如 max_row_vgroup_id、算子的固定参数如输入输出列数、列宽等参数，无论算子在单个或多个 vgroups 上执行，都只输出原始值。
精度方面，时间类型的指标默认以“ms”为单位，小数点后保留三位；其他小数类型指标，小数点后保留一位小数；整数类型保留原始值，百分数类型也只保留整数部分。

#### 5.3.2 普通级别输出示例

```sql {wrap}
taos> explain analyze verbose false select * from stb where ts > "2026-02-09"\G;
*************************** 1.row ***************************
QUERY_PLAN: -> Data Exchange 3:1 (cost=0.088..0.122 rows=2 width=16)
*************************** 2.row ***************************
QUERY_PLAN:    -> Projection (cost=0.390(0.422)..0.425(0.486) rows=0.7(1) columns=3 width=16 input_order=asc)
*************************** 3.row ***************************
QUERY_PLAN:       -> Table Scan on stb (cost=0.000(0.000)..0.398(0.402) rows=0.7(1) columns=2 pseudo_columns=1 width=16 order=[asc|1 desc|0] mode=ts_order data_load=data)
*************************** 4.row ***************************
QUERY_PLAN:             I/O cost: total_blocks=0.7(1)
*************************** 5.row ***************************
QUERY_PLAN:                       total_rows=0.7(1) check_rows=0.7(1)
*************************** 6.row ***************************
QUERY_PLAN: Planning Time: 2.295 ms
*************************** 7.row ***************************
QUERY_PLAN: Execution Time: 5.308 ms
Query OK, 8 row(s) in set (0.007122 s)
```

#### 5.3.3 Verbose 级别输出示例

```sql {wrap}
taos> explain analyze verbose true select * from stb where ts > "2026-02-09" and c1 > 10\G;
*************************** 1.row ***************************
QUERY_PLAN: -> Data Exchange 3:1 (cost=0.685..0.817 rows=2 width=16)
*************************** 2.row ***************************
QUERY_PLAN:       Output: columns=3 width=16
*************************** 3.row ***************************
QUERY_PLAN:       Exec: compute=0.106 create=0.088 start=0.099 times=1 input_wait=0.703 output_wait=0.000
*************************** 3.row ***************************
QUERY_PLAN:       Network: mode=concurrent fetch_times=0.7(1) fetch_rows=0.7(1) fetch_cost=0.112(0.120)
*************************** 4.row ***************************
QUERY_PLAN:    -> Projection (cost=0.556(0.466)..0.674(0.588) rows=0.7(1) columns=3 width=16 input_order=asc)
*************************** 5.row ***************************
QUERY_PLAN:          Output: columns=3 width=16 ignore_group_id=true
*************************** 6.row ***************************
QUERY_PLAN:          Merge ResBlocks: True
*************************** 7.row ***************************
QUERY_PLAN:          Exec: compute=0.090(0.092) create=0.476(0.477) start=0.552(0.587) times=1(1) input_wait=0.123(0.099) output_wait=0.144(0.135)
*************************** 8.row ***************************
QUERY_PLAN:       -> Table Scan on stb (cost=0.000(0.000)..0.589(0.672) rows=0.7(1) columns=2 pseudo_columns=1 width=16 order=[asc|1 desc|0] mode=ts_order data_load=data)
*************************** 9.row ***************************
QUERY_PLAN:             Output: columns=3 width=16
*************************** 10.row ***************************
QUERY_PLAN:             Time Range: [1770566400001, 9223372036854775807]
*************************** 11.row ***************************
QUERY_PLAN:             Filter: (condition=`db3`.`stb`.`c1` > 10, efficiency=100%)
*************************** 12.row ***************************
QUERY_PLAN:             Exec: compute=0.610(0.622) create=0.000(0.000) start=0.000(0.001) times=1(1) input_wait=0.000(0.000) output_wait=0.877(0.886)
*************************** 13.row ***************************
QUERY_PLAN:             I/O cost: total_blocks=0.7(1) file_load_blocks=0.7(1) stt_load_blocks=0(0.0) mem_load_blocks=0(0.0) sma_load_blocks=0(0.0) composed_blocks=0(0.0)
*************************** 14.row ***************************
QUERY_PLAN:                       file_load_elapsed=0.356(0.366) stt_load_elapsed=0.000(0.000) mem_load_elapsed=0.000(0.000) sma_load_elapsed=0.000(0.000) composed_elapsed=0.000(0.000)
*************************** 15.row ***************************
QUERY_PLAN:                       total_rows=0.7(1) check_rows=0.7(1) slowest_vgroup_id=0 slow_deviation=0% cost_ratio=20.0 data_deviation=0%
*************************** 17.row ***************************
QUERY_PLAN: Planning Time: 1.018 ms
*************************** 18.row ***************************
QUERY_PLAN: Execution Time: 9.883 ms
Query OK, 18 row(s) in set (0.010122 s)
```

### 5.4 Format 选项（本期不做）

```sql
EXPLAIN [(FORMAT fmt)] [ANALYZE [VERBOSE]] query;
fmt: text | html | json | graphviz
```

参考 [duckdb](https://duckdb.org/docs/stable/dev/profiling#the-format-option) 中的功能，explain 支持多种输出格式，默认为`text`。其中比较特殊的格式是`graphviz`，这种格式的输出可以导入到 [graphviz](http://graphviz.org) 可视化工具中，将 pipeline 打印成一张树状图。图中的节点即算子，连接节点的有向边是数据流转的路径。相比于`text`格式可读性更强，在多节点的场景中，拓扑图可以更直观对比每个节点上的计算量是否平衡，判断数据倾斜问题。

### 5.5 支持 DML 语句

DML 语句存在**不可逆的数变更风险**，不建议在 explain 中支持，因为若 explain 原生支持 DML，用户极易混淆两者的边界，导致生产环境中不可逆的数据错误。主流数据库可以通过事务包裹 explain 语句规避数据风险，我们目前做不到。
建议将 DML 语句中的数据查询部分提取出来，单独分析。

### 5.6 Ratio 功能

暂不支持，文档不会提及

## 6. 性能

额外统计开销应当极低

## 7. 参考文档

1. [mysql docs - explain analyze](https://dev.mysql.com/blog-archive/mysql-explain-analyze/)
2. [postgresql docs - explain](https://www.postgresql.org/docs/current/sql-explain.html)
3. [clickhouse docs - explain](https://clickhouse.com/docs/zh/sql-reference/statements/explain)，[clickhouse docs - sampling query profile](https://clickhouse.com/docs/operations/optimizing-performance/sampling-query-profiler)
4. [duckdb docs - explain_analyze](https://duckdb.org/docs/stable/guides/meta/explain_analyze)，[duckdb docs - format option](https://duckdb.org/docs/stable/dev/profiling#the-format-option)
5. [influxdb docs - explain analyze](https://docs.influxdata.com/influxdb3/core/reference/sql/explain/#explain-analyze)
6. [Ali E-MapReduce - query profile](https://help.aliyun.com/zh/emr/emr-serverless-starrocks/analyze-queries-by-using-query-profile?spm=a2c4g.11186623.0.0.2bfb4ca1QNK3d0)

## 8. 附录

### 8.1 相关jira及问题描述：

TD-25066

<quote-container>
除去table scan之外的其他算子统计的结束时间都是算子结束时的时间, 此时间比较滞后, 不是算子真正执行的时间, 改为与table scan相同的统计逻辑。
</quote-container>


TS-4998

<quote-container>
explain 已经能够打印如下信息
1. 扫描的记录条数
2. 参与计算的记录条数
3. 扫描的 block 数目
4. 读取的 block 数目
还需要增加如下信息：
1. 被更新的记录条数
2. 被删除的记录条数
3. 每个 block 的平均记录条数
4. 存在时间交织的 block 比例
5. 读取 stt 文件的个数、大小
6. 读取 data 文件的个数、大小
</quote-container>


TD-24860

<quote-container>
为了更好的定位一些性能问题，需要补充一些必要信息：算子输入输出数据量、内存占用、磁盘占用等。
</quote-container>

无描述
TD-27429


TD-23037
