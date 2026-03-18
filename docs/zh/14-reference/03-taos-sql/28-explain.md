---
sidebar_label: EXPLAIN
title: EXPLAIN
description: 查看查询执行计划与运行期分析信息
---

`EXPLAIN` 用于查看查询语句的执行计划。它适合在 SQL 调优、慢查询诊断、标签索引命中分析、过滤下推确认、跨节点数据交换排查等场景中使用。

`EXPLAIN ANALYZE` 会在执行计划的基础上补充运行期指标，可用于判断瓶颈究竟在扫描、过滤、排序、网络交换还是跨 vgroup 的数据倾斜。

## 语法

```sql
EXPLAIN [ANALYZE] [VERBOSE {true | false}] query_or_subquery;
```

## 参数说明

### `ANALYZE`

执行语句并返回运行期指标。与普通 `EXPLAIN` 相比，它会额外展示：

- 算子首行返回时间、末行返回时间、输出行数
- 算子执行耗时与等待耗时
- 扫描 I/O 代价
- 跨节点数据交换代价
- 计划时间与执行时间

说明：

- `ANALYZE` 需要实际执行目标查询语句

诊断价值：

- 用于判断慢 SQL 是“计划不好”还是“执行慢”
- 用于识别热点 vgroup、慢节点和数据倾斜
- 用于验证过滤、排序、窗口、聚合等算子的真实代价

### `VERBOSE`

默认值为 `false`。设置为 `true` 后，会显示每个算子的详细属性，例如：

- `Output`
- `Filter`
- `Primary Filter`
- `Tag Index Filter`
- `Time Range`
- `Time Window`
- `Merge ResBlocks`
- `Sort Key`
- `Sort Method`
- `Network`
- `Exec cost`
- `I/O cost`

诊断价值：

- 适合确认优化器是否做了谓词下推、标签索引下推、主键过滤下推
- 适合定位排序键、合并键、网络交换模式、窗口参数等细节

## 返回结果说明

`EXPLAIN` 的返回结果只有一列，列名为 `QUERY_PLAN`。每一行是计划树中的一个节点或一个详细统计信息。

执行计划采用树状结构展示：

- 最上层节点是最终结果的产生位置
- 缩进越深，越接近底层扫描
- `->` 之后是算子名称
- `VERBOSE true` 时，算子下方会追加该算子的详细统计信息

示例：

```text
-> Projection (...)
   -> Aggregate (...)
      -> Table Scan on meters (...)
```

阅读建议：

- 从最上往下看，先判断结果是如何生成的
- 从最下往上看，判断底层扫描、过滤、聚合、排序、交换是如何逐步叠加的
- 如果是分布式查询，重点关注 `Data Exchange`、`Network`、`I/O cost` 和慢 vgroup 信息

## 常见算子

下表列出 `EXPLAIN` 中常见的算子名称及其诊断价值。

| 算子名称 | 含义 | 诊断价值 |
| --- | --- | --- |
| `Aggregate` | 聚合 | 无分组聚合 |
| `Block Dist Scan on ...` | 数据块分布扫描 | 用于块级分布或块级统计场景 |
| `Count` | 计数窗口 | 用于 `COUNT_WINDOW` |
| `Data Exchange N:1` | 多源到单源的数据交换 | 用于观察跨节点拉取成本与网络瓶颈 |
| `Dynamic Query Control for ...` | 动态查询控制算子 | 更多用于复杂计划调度信息和内部执行策略判断 |
| `Event` | 事件窗口 | 用于 `EVENT_WINDOW` |
| `External on Column ...` | 外部窗口相关算子 | 用于外部窗口或外部对齐处理 |
| `Fill` | 填充算子 | 用于缺失值填充 |
| `Group Cache` | 分组缓存算子 | 用于分组缓存和分组复用 |
| `Group Sort` | 分组排序 | 用于分组场景下的排序 |
| `GroupAggregate` | 分组聚合 | 有分组键的聚合 |
| `Indefinite Rows Function` | 返回不定长结果的函数算子 | 用于特殊函数处理 |
| `Inner/Left/Right/Full ... Join` | 关联算子 | 重点关注连接算法、连接条件、主键条件和时间范围 |
| `Interp` | 插值算子 | 用于 `INTERP` 插值 |
| `Interval on Column ...` | 时间窗口算子 | 适用于 `INTERVAL` 查询 |
| `Last Row Scan on ...` | Last Row 缓存扫描 | 用于 `last_row`、`last`查询等场景，适合确认是否进行缓存扫描 |
| `Merge` | 归并 | 用于多个输入流的合并 |
| `Merge Aligned Interval on Column ...` | 对齐窗口归并 | 常见于窗口结果对齐场景 |
| `Merge Interval on Column ...` | 窗口归并算子 | 用于分布式窗口聚合合并 |
| `Partition on Column ...` | 分片算子 | 常见于数据分片，重点关注分片列信息 |
| `Projection` | 投影 | 负责列裁剪、表达式输出、列重排 |
| `Session` | 会话窗口 | 用于 `SESSION` |
| `Sort` | 排序 | 重点关注是否发生显式排序及其排序代价 |
| `StateWindow on Column ...` | 状态窗口 | 用于 `STATE_WINDOW` |
| `System Table Scan on ...` | 系统表扫描 | 用于元数据或系统库查询 |
| `Table Count Scan on ...` | 表计数扫描 | 用于表数量相关查询 |
| `Table Merge Scan on ...` | 多表合并扫描 | 常见于超级表、多子表聚合或排序场景，适合观察多表归并成本 |
| `Table Scan on ...` | 表扫描 | 最常见的底层扫描节点，用于判断扫描顺序、扫描模式、数据加载方式 |
| `Tag Scan on ...` | 标签扫描 | 判断是否只基于标签或元数据就能缩小扫描范围 |
| `Virtual Table Scan on ...` | 虚拟表扫描 | 判断虚拟表查询是否已经在逻辑层裁剪数据 |

## 指标速查

### 1. 算子标题行中的常见指标

这些指标通常出现在 `-> 算子名 (...)` 这一行中。

| 指标 | 含义 | 诊断价值 |
| --- | --- | --- |
| `algo=Merge` / `algo=Hash` | Join 使用的连接算法 | 用于判断连接瓶颈更偏向排序归并还是哈希构建 |
| `asof_op=` | ASOF JOIN 的比较运算符 | 用于确认最近点连接的比较方向 |
| `batch_process_child=` | 是否按批处理子任务 | 用于分析动态计划的执行策略 |
| `batch_scan=` | 是否启用批量扫描 | 用于判断超级表或批量子表场景是否走了批量路径 |
| `blocking=` | 是否为阻塞型算子，`1` 表示需要攒够数据后再输出 | 阻塞型算子更容易带来首包延迟 |
| `columns=` | 算子参与处理或输出的列数 | 列数过多意味着列裁剪不足，可能增加扫描、网络和内存开销 |
| `cost=first..last` | 仅在 `EXPLAIN ANALYZE` 中出现，表示该算子从创建到首行返回、末行返回的耗时，单位为毫秒 | 用于判断首包慢还是全量处理慢；首值大常见于扫描、网络、排序预热，末值大常见于大结果集或重计算 |
| `data_load=data` | 读取原始数据块 | 说明需要真正访问明细数据 |
| `data_load=no` | 不需要加载完整数据块 | 说明该查询主要依赖元数据、索引或统计信息 |
| `data_load=sma` | 读取 SMA/TSMA 结果 | 说明查询可能命中了预计算路径 |
| `functions=` | 当前算子处理的函数数量 | 用于估计聚合、窗口、插值等算子的计算复杂度 |
| `global_group=` | 是否为全局分组缓存 | 用于分析分组缓存的范围 |
| `group_by_uid=` | 是否按 UID 分组 | 常用于内部分组策略判断 |
| `group_join=` | 是否启用分组连接 | 用于复杂连接诊断 |
| `groups=` | 分组键数量 | 用于分析分组维度是否过多 |
| `has_partition=` | 是否包含分区信息 | 用于判断虚拟表或动态查询是否保留了分区属性 |
| `input_order=` | 输入时间序 | 用于判断上游是否已经满足当前算子对顺序的要求 |
| `jlimit=` | 单行匹配的最大连接行数 | 用于分析连接放大是否被限制 |
| `limit=` | 当前算子承接到的 `LIMIT` | 用于确认 `LIMIT` 是否被尽早下推 |
| `mode=grp_order` | 按分组顺序组织扫描 | 说明计划更强调分组输出顺序 |
| `mode=seq_grp_order` | 顺序分组扫描 | 常用于按表或按标签分组并保持顺序的场景 |
| `mode=sort` | 归并或合并时采用排序模式 | 说明当前合并过程依赖排序键 |
| `mode=ts_order` | 按时间序组织扫描 | 常见于普通时间序扫描 |
| `offset=` | 当前算子承接到的 `OFFSET` | 用于判断偏移是否参与了当前层裁剪 |
| `order=[asc\|x desc\|y]` | 扫描时顺序读取与逆序读取的计数 | 用于判断扫描是否主要按升序还是降序进行 |
| `origin_vgroup_num=` | 原始 vgroup 数量 | 用于观察虚拟稳定表查询的并行规模 |
| `output_order=` | 输出时间序 | 用于判断当前算子是否改变了顺序，进而推断后续是否还能避免排序 |
| `partitions=` | 分片键数量 | 用于判断 `PARTITION BY` 的维度规模 |
| `pseudo_columns=` | 伪列数量，例如 `_wstart`、`_wend`、`tbname` 等 | 可用于确认窗口列、表名列等是否被引入执行链路 |
| `rows=` | 该算子输出的结果行数 | 用于判断某层是否放大了数据量，或某个 vgroup 是否输出异常偏大 |
| `seq_win_grp=` | 是否按顺序窗口分组 | 用于窗口连接和复杂窗口的执行策略判断 |
| `slimit=` | 当前算子承接到的 `SLIMIT` | 用于分析分片输出数量是否在早期被控制 |
| `soffset=` | 当前算子承接到的 `SOFFSET` | 用于分析分片偏移是否生效 |
| `src_scan=` | 动态控制下的源扫描位置信息 | 主要供复杂计划与支持排障使用 |
| `uid_slot=` | UID 槽位信息 | 主要供复杂计划与支持排障使用 |
| `vgroup_slot=` | vgroup 槽位信息 | 主要供复杂计划与支持排障使用 |
| `width=` | 单行宽度，单位为字节 | 宽行会放大扫描、排序、网络交换和内存占用；如果标题行与 `Output` 行的 `width` 不同，通常说明算子内部还有中间列或辅助列 |
| `window_offset=(x, y)` | 窗口连接偏移范围 | 用于确认时间窗连接的左右边界 |

说明：

- 在多 vgroup 聚合输出中，`a(b)` 形式表示“平均值（最大值）”
- 对 `rows=` 来说，`b` 可用于快速定位最重的单个执行节点
- 对 `cost=` 来说，首值大多对应首包延迟，末值大多对应总处理时长

### 2. `VERBOSE true` 下的结构与属性指标

| 指标 | 含义 | 诊断价值 |
| --- | --- | --- |
| `Buffers:` | 排序缓冲区大小 | 用于判断排序内存压力 |
| `End Cond: ...` | 事件窗口结束条件 | 用于确认 `EVENT_WINDOW` 的结束触发条件 |
| `fetch_cost=` | 交换阶段 RPC 拉取耗时 | 网络或远端节点压力的直接体现 |
| `fetch_rows=` | 每节点拉取的行数 | 用于判断网络批量大小是否合理 |
| `fetch_times=` | 交换阶段的拉取次数 | 次数过多说明数据被切得过碎或下游反复取数 |
| `Fill Values: ...` | 填充值列表 | 用于确认 `FILL` 或 `INTERP` 的填充值是否正确 |
| `Filter: ... efficiency=xx%` | 过滤效率，仅在 `ANALYZE` 下出现 | 一般越低说明过滤越充分；若过滤条件很强但效率接近 100%，可能没有在预期层级生效 |
| `Filter: conditions=...` | 当前算子的过滤条件 | 用于确认过滤谓词处在哪一层执行 |
| `Join Col Cond: ...` | 非主键列连接条件 | 用于定位列条件是否下推到连接层 |
| `Join Full Cond: ...` | Join 完整条件 | 用于确认最终连接条件表达式 |
| `Join Param: ...` | Join 的附加参数 | 用于分析 ASOF/WINDOW JOIN 的比较符、偏移范围和限制 |
| `Join Prim Cond: ...` | Join 主键条件 | 用于判断主键连接条件是否被单独抽取 |
| `Left Equal Cond:` | 左侧等值列 | 用于分析等值连接键 |
| `Left Table Time Range: ...` | 左表时间范围 | 用于时间条件只命中某一侧 Join 的诊断 |
| `Left/Right Table Time Range: ...` | 两侧表时间范围 | 用于确认 Join 两边都参与了时间裁剪 |
| `loops:` | 排序循环次数 | 循环次数多通常意味着处理批次多或归并轮次多 |
| `Merge Key: ...` | 多路归并的键 | 用于确认多分片结果是按什么键归并的 |
| `Merge ResBlocks: True/False` | 是否需要归并结果块 | `True` 说明当前层存在块级合并，可能带来额外内存与 CPU 开销 |
| `Network: mode=...` | 数据交换方式，`concurrent` 或 `sequence` | 并发拉取通常吞吐更高，顺序拉取更容易出现长尾等待 |
| `Output: columns=... width=...` | 该算子对下游真正输出的列数与行宽 | 用于确认列裁剪是否生效 |
| `Output: Ignore Group Id: true/false` | 该算子输出时是否忽略分组 ID | 如果为 `true`，说明后续阶段不再区分上游分组边界 |
| `Partition Key: partitions=n` | 分片键信息 | 用于确认 `PARTITION BY` 是否生效 |
| `Primary Filter: ...` | 主键过滤条件，通常是时间戳或主键相关过滤 | 说明主键条件已下推到更底层，通常对性能很重要 |
| `Right Equal Cond:` | 右侧等值列 | 用于分析等值连接键 |
| `Right Table Time Range: ...` | 右表时间范围 | 用于时间条件只命中某一侧 Join 的诊断 |
| `Sort Key: ...` | 排序键 | 用于确认是否按预期列排序 |
| `Sort Method: quicksort / merge sort` | 排序算法 | 可用于区分内存内排序和更重的归并排序路径 |
| `Start Cond: ...` | 事件窗口开始条件 | 用于确认 `EVENT_WINDOW` 的起始触发条件 |
| `Tag Index Filter: conditions=...` | 可利用标签索引的过滤条件 | 是判断标签索引是否命中的最直接指标 |
| `Time Range: [start, end]` | 当前算子的时间扫描范围 | 用于确认时间条件是否被成功裁剪 |
| `Time Window: interval=... offset=... sliding=...` | 时间窗口参数 | 用于确认窗口大小、偏移与滑动步长是否符合预期 |
| `Window Count=` | 计数窗口大小 | 用于确认 `COUNT_WINDOW()` 的窗口规模 |
| `Window Sliding=` | 计数窗口滑动步长 | 用于确认 `COUNT_WINDOW()` 的滑动设置 |
| `Window: gap=...` | 会话窗口间隔 | 用于诊断 `SESSION()` 的切分标准 |

### 3. 算子的执行代价指标

`EXPLAIN ANALYZE VERBOSE true` 会在算子下额外展示 `Exec cost` 行。

| 指标 | 含义 | 诊断价值 |
| --- | --- | --- |
| `Exec cost:` | 当前算子的执行代价摘要 | 用于判断当前算子究竟是算得慢、等得久，还是被上游阻塞 |
| `compute=` | 算子自身执行时间，不含等待下游数据时间，单位毫秒 | 该值高，通常说明算子本身计算、聚合、排序或扫描开销大 |
| `create=` | 算子创建时间；多节点时为平均创建时间与最晚创建时间，时区为系统当前时区 | 多个节点创建时间差异大，可能意味着任务分发或调度不均衡 |
| `input_wait=` | 等待下游返回数据的累计时间，单位毫秒 | 高说明瓶颈更可能在子节点或远端扫描 |
| `output_wait=` | 等待上游再次调用的累计时间，单位毫秒 | 高说明当前算子产生结果后，上游消费不及时 |
| `start=` | 算子从创建到第一次被调用的耗时，单位毫秒 | 值大说明算子虽已创建，但较晚才真正进入执行 |
| `times=` | 算子被调用的次数 | 调用次数异常偏多，常意味着上游按很小批次拉取 |

说明：

- 单节点显示为单值，多节点显示为 `平均值(最大值)`
- `create=` 在多节点时也会显示为 `平均时间戳(最晚时间戳)`

### 4. 扫描类算子的 I/O 指标

扫描类算子在 `EXPLAIN ANALYZE VERBOSE true` 下还会展示三行 `I/O cost` 信息。

| 指标 | 含义 | 诊断价值 |
| --- | --- | --- |
| `check_rows=` | 过滤或检查过的行数 | 若远大于 `rows`，说明过滤前扫描量很大 |
| `composed_blocks=` | 组合生成的块数 | 说明扫描结果经过了额外的块拼装 |
| `composed_elapsed=` | 结果块组合耗时 | 值高说明块拼装或重组成本显著 |
| `cost_ratio=` | 最慢节点与最快节点的耗时比值 | 比值越大，倾斜越严重 |
| `data_deviation=` | 最慢节点相对中位节点的数据量偏差 | 用于辅助判断慢节点是算得慢还是拿到的数据就更多 |
| `file_load_blocks=` | 从文件加载的块数 | 值高说明磁盘读取较多 |
| `file_load_elapsed=` | 文件块加载耗时 | 判断磁盘 I/O 压力 |
| `mem_load_blocks=` | 从内存加载的块数 | 值高通常说明缓存命中较好 |
| `mem_load_elapsed=` | 内存块加载耗时 | 内存路径也高时，可能是块数过多或行宽过大 |
| `slow_deviation=` | 最慢节点相对中位节点的耗时偏差 | 偏差大说明存在明显长尾 |
| `slowest_vgroup_id=` | 最慢 vgroup ID，仅多节点时出现 | 直接定位慢节点 |
| `sma_load_blocks=` | 从 SMA/TSMA 加载的块数 | 说明是否命中预聚合数据 |
| `sma_load_elapsed=` | SMA/TSMA 加载耗时 | 用于评估预聚合路径收益 |
| `stt_load_blocks=` | 从 STT 相关结构加载的块数 | 用于分析 STT 路径参与程度 |
| `stt_load_elapsed=` | STT 加载耗时 | 判断 STT 路径代价 |
| `total_blocks=` | 总处理块数 | 反映扫描总体工作量 |

诊断建议：

- `file_load_*` 高：优先检查时间范围、标签过滤和索引命中
- `mem_load_*` 高但仍慢：重点检查结果行宽、排序和聚合复杂度
- `sma_load_*` 为 0：说明未命中预计算路径
- `cost_ratio`、`slow_deviation` 高：优先定位热点 vgroup、数据分布不均或某个节点资源异常，再通过 `data_deviation` 辅助定位

### 5. 计划级汇总指标

`EXPLAIN ANALYZE` 在结果末尾还会给出整个语句的汇总信息。

| 指标 | 含义 | 诊断价值 |
| --- | --- | --- |
| `Execution Time:` | 执行计划实际运行耗时 | 是判断最终端到端耗时的总指标 |
| `Planning Time:` | 生成执行计划耗时 | 计划时间高时，应关注 SQL 复杂度、关联层级和优化器开销 |

## 使用示例

### 1. 查看普通查询的执行计划

```sql
taos> EXPLAIN SELECT ts, current FROM meters WHERE ts >= '2026-01-01 00:00:00' AND ts < '2026-02-01 00:00:00' \G;
*************************** 1.row ***************************
QUERY_PLAN: -> Data Exchange 4:1 (width=12)
*************************** 2.row ***************************
QUERY_PLAN:    -> Projection (columns=2 width=12 input_order=asc)
*************************** 3.row ***************************
QUERY_PLAN:       -> Table Scan on meters (columns=2 width=12 order=[asc|1 desc|0] mode=ts_order data_load=data)
```

### 2. 查看详细计划

```sql
taos> EXPLAIN VERBOSE true SELECT ts, current FROM meters WHERE ts >= '2025-01-01 00:00:00+08:00' AND ts < '2025-01-02 00:00:00+08:00' \G;
*************************** 1.row ***************************
QUERY_PLAN: -> Data Exchange 4:1 (width=12)
*************************** 2.row ***************************
QUERY_PLAN:       Output: columns=2 width=12
*************************** 3.row ***************************
QUERY_PLAN:    -> Projection (columns=2 width=12 input_order=asc)
*************************** 4.row ***************************
QUERY_PLAN:          Output: columns=2 width=12
*************************** 5.row ***************************
QUERY_PLAN:          Output: Ignore Group Id: true
*************************** 6.row ***************************
QUERY_PLAN:          Merge ResBlocks: False
*************************** 7.row ***************************
QUERY_PLAN:       -> Table Scan on meters (columns=2 width=12 order=[asc|1 desc|0] mode=ts_order data_load=data)
*************************** 8.row ***************************
QUERY_PLAN:             Output: columns=2 width=12
*************************** 9.row ***************************
QUERY_PLAN:             Time Range: [1735660800000, 1735747199999]
```

### 3. 查看执行过程信息

```sql
taos> EXPLAIN ANALYZE VERBOSE true SELECT * FROM meters WHERE location = 'Beijing' \G;
*************************** 1.row ***************************
QUERY_PLAN: -> Data Exchange 4:1 (cost=2.358..9.769 rows=80000 width=39)
*************************** 2.row ***************************
QUERY_PLAN:       Output: columns=6 width=39
*************************** 3.row ***************************
QUERY_PLAN:       Network: mode=concurrent fetch_times=2.8(5) fetch_rows=20000.0(40000) fetch_cost=5.389(9.244)
*************************** 4.row ***************************
QUERY_PLAN:       Exec cost: compute=0.649 create=2026-03-13 16:57:39.481277 start=0.010 times=25 input_wait=7.160 output_wait=1.951
*************************** 5.row ***************************
QUERY_PLAN:    -> Projection (cost=2.195(3.248)..7.120(12.106) rows=20000.0(40000) columns=6 width=39 input_order=asc)
*************************** 6.row ***************************
QUERY_PLAN:          Output: columns=6 width=39
*************************** 7.row ***************************
QUERY_PLAN:          Output: Ignore Group Id: true
*************************** 8.row ***************************
QUERY_PLAN:          Merge ResBlocks: False
*************************** 9.row ***************************
QUERY_PLAN:          Exec cost: compute=0.095(0.154) create=2026-03-13 16:57:39.475838(2026-03-13 16:57:39.475887) start=0.021(0.023) times=7.0(13) input_wait=5.271(9.039) output_wait=1.759(2.926)
*************************** 10.row ***************************
QUERY_PLAN:       -> Table Scan on meters (cost=2.132(3.149)..7.125(12.116) rows=20000.0(40000) columns=4 pseudo_columns=2 width=39 order=[asc|1 desc|0] mode=ts_order data_load=data)
*************************** 11.row ***************************
QUERY_PLAN:             Output: columns=6 width=39
*************************** 12.row ***************************
QUERY_PLAN:             Time Range: [-9223372036854775808, 9223372036854775807]
*************************** 13.row ***************************
QUERY_PLAN:             Tag Index Filter: conditions=(`test`.`meters`.`location` = 'Beijing')
*************************** 14.row ***************************
QUERY_PLAN:             Exec cost: compute=5.268(9.033) create=2026-03-13 16:57:39.475829(2026-03-13 16:57:39.475880) start=0.030(0.037) times=7.0(13) input_wait=0.000(0.000) output_wait=1.855(3.053)
*************************** 15.row ***************************
QUERY_PLAN:          -> I/O cost: total_blocks=6.0(12) file_load_blocks=0.8(1) stt_load_blocks=0.0(0) mem_load_blocks=0.0(0) sma_load_blocks=0.0(0) composed_blocks=6.0(12)
*************************** 16.row ***************************
QUERY_PLAN:                file_load_elapsed=0.000(0.000) stt_load_elapsed=0.000(0.000) mem_load_elapsed=0.000(0.000) sma_load_elapsed=0.000(0.000) composed_elapsed=3.578(6.655)
*************************** 17.row ***************************
QUERY_PLAN:                check_rows=20000.0(40000) slowest_vgroup_id=4 slow_deviation=130% cost_ratio=173.7 data_deviation=300%
*************************** 18.row ***************************
QUERY_PLAN: Planning Time: 0.606 ms
*************************** 19.row ***************************
QUERY_PLAN: Execution Time: 24.992 ms

-- 注意：需要手动创建标签索引
```

### 4. 聚合查询

```sql
taos> EXPLAIN ANALYZE VERBOSE true SELECT tbname, count(*), avg(current) FROM meters PARTITION BY tbname \G;
*************************** 1.row ***************************
QUERY_PLAN: -> Data Exchange 4:1 (cost=0.357..0.563 rows=30 width=288)
*************************** 2.row ***************************
QUERY_PLAN:       Output: columns=3 width=288
*************************** 3.row ***************************
QUERY_PLAN:       Network: mode=concurrent fetch_times=1.0(1) fetch_rows=7.5(10) fetch_cost=0.408(0.512)
*************************** 4.row ***************************
QUERY_PLAN:       Exec cost: compute=0.059 create=2026-03-13 16:29:52.978341 start=0.008 times=5 input_wait=0.491 output_wait=0.007
*************************** 5.row ***************************
QUERY_PLAN:    -> Aggregate (cost=5.506(7.403)..5.506(7.403) rows=7.5(10) functions=3 width=288 input_order=unknown )
*************************** 6.row ***************************
QUERY_PLAN:          Output: columns=3 width=288 blocking=1
*************************** 7.row ***************************
QUERY_PLAN:          Merge ResBlocks: True
*************************** 8.row ***************************
QUERY_PLAN:          Exec cost: compute=0.271(0.434) create=2026-03-13 16:29:52.970343(2026-03-13 16:29:52.970453) start=0.802(0.860) times=1.0(1) input_wait=4.433(6.305) output_wait=0.002(0.004)
*************************** 9.row ***************************
QUERY_PLAN:       -> Table Scan on meters (cost=1.190(1.321)..5.414(7.396) rows=75000.0(100000) columns=2 pseudo_columns=1 width=284 order=[asc|1 desc|0] mode=ts_order data_load=sma)
*************************** 10.row ***************************
QUERY_PLAN:             Output: columns=3 width=284
*************************** 11.row ***************************
QUERY_PLAN:             Time Range: [-9223372036854775808, 9223372036854775807]
*************************** 12.row ***************************
QUERY_PLAN:             Partition Key: partitions=1
*************************** 13.row ***************************
QUERY_PLAN:             Exec cost: compute=4.427(6.299) create=2026-03-13 16:29:52.970329(2026-03-13 16:29:52.970427) start=0.816(0.876) times=23.5(31) input_wait=0.000(0.000) output_wait=0.178(0.246)
*************************** 14.row ***************************
QUERY_PLAN:          -> I/O cost: total_blocks=22.5(30) file_load_blocks=0.0(0) stt_load_blocks=0.0(0) mem_load_blocks=22.5(30) sma_load_blocks=22.5(30) composed_blocks=22.5(30)
*************************** 15.row ***************************
QUERY_PLAN:                file_load_elapsed=0.000(0.000) stt_load_elapsed=0.000(0.000) mem_load_elapsed=4.229(6.031) sma_load_elapsed=0.000(0.000) composed_elapsed=4.229(6.031)
*************************** 16.row ***************************
QUERY_PLAN:                check_rows=75000.0(100000) slowest_vgroup_id=4 slow_deviation=41% cost_ratio=3.8 data_deviation=25%
*************************** 17.row ***************************
QUERY_PLAN: Planning Time: 8.821 ms
*************************** 18.row ***************************
QUERY_PLAN: Execution Time: 10.767 ms
```

### 5. 时间窗口查询

```sql
taos> EXPLAIN ANALYZE VERBOSE true SELECT _wstart, _wend, count(*), avg(current) FROM meters INTERVAL(10s) \G;
*************************** 1.row ***************************
QUERY_PLAN: -> Merge Aligned Interval on Column  (cost=0.626..0.626 rows=10 functions=4 width=32 input_order=asc output_order=asc)
*************************** 2.row ***************************
QUERY_PLAN:       Output: columns=4 width=32
*************************** 3.row ***************************
QUERY_PLAN:       Time Window: interval=10s offset=0a sliding=10s
*************************** 4.row ***************************
QUERY_PLAN:       Merge ResBlocks: True
*************************** 5.row ***************************
QUERY_PLAN:       Exec cost: compute=0.028 create=2026-03-13 16:34:44.806501 start=0.018 times=1 input_wait=0.580 output_wait=0.002
*************************** 6.row ***************************
QUERY_PLAN:    -> Merge (cost=0.605..0.605 rows=40 columns=4 width=82 input_order=asc output_order=asc mode=sort)
*************************** 7.row ***************************
QUERY_PLAN:          Sort Method: merge sort  Buffers:20.00 Kb  loops:2
*************************** 8.row ***************************
QUERY_PLAN:          Output: columns=4 width=82
*************************** 9.row ***************************
QUERY_PLAN:          Output: Ignore Group Id: false
*************************** 10.row ***************************
QUERY_PLAN:          Merge Key: _group_id asc,  asc
*************************** 11.row ***************************
QUERY_PLAN:          Exec cost: compute=0.580 create=2026-03-13 16:34:44.806493 start=0.026 times=2 input_wait=0.000 output_wait=0.028
*************************** 12.row ***************************
QUERY_PLAN:       -> Data Exchange 1:1 (cost=0.312..0.312 rows=10 width=82)
*************************** 13.row ***************************
QUERY_PLAN:             Output: columns=4 width=82
*************************** 14.row ***************************
QUERY_PLAN:             Network: mode=concurrent fetch_times=1 fetch_rows=10 fetch_cost=0.248
*************************** 15.row ***************************
QUERY_PLAN:             Exec cost: compute=0.007 create=2026-03-13 16:34:44.806480 start=0.073 times=2 input_wait=0.232 output_wait=0.305
*************************** 16.row ***************************
QUERY_PLAN:          -> Interval on Column ts (cost=5.483..5.483 rows=10 functions=4 width=82 input_order=asc output_order=asc)
*************************** 17.row ***************************
QUERY_PLAN:                Output: columns=4 width=82
*************************** 18.row ***************************
QUERY_PLAN:                Time Range: [-9223372036854775808, 9223372036854775807]
*************************** 19.row ***************************
QUERY_PLAN:                Time Window: interval=10s offset=0a sliding=10s
*************************** 20.row ***************************
QUERY_PLAN:                Merge ResBlocks: False
*************************** 21.row ***************************
QUERY_PLAN:                Exec cost: compute=0.250 create=2026-03-13 16:34:44.800429 start=0.024 times=1 input_wait=5.209 output_wait=0.003
*************************** 22.row ***************************
QUERY_PLAN:             -> Table Scan on meters (cost=0.388..5.478 rows=90000 columns=2 width=12 order=[asc|1 desc|0] mode=ts_order data_load=sma)
*************************** 23.row ***************************
QUERY_PLAN:                   Output: columns=2 width=12
*************************** 24.row ***************************
QUERY_PLAN:                   Time Range: [-9223372036854775808, 9223372036854775807]
*************************** 25.row ***************************
QUERY_PLAN:                   Exec cost: compute=5.204 create=2026-03-13 16:34:44.800423 start=0.031 times=28 input_wait=0.000 output_wait=0.247
*************************** 26.row ***************************
QUERY_PLAN:                -> I/O cost: total_blocks=27 file_load_blocks=0 stt_load_blocks=0 mem_load_blocks=27 sma_load_blocks=0 composed_blocks=27
*************************** 27.row ***************************
QUERY_PLAN:                      file_load_elapsed=0.000 stt_load_elapsed=0.000 mem_load_elapsed=5.141 sma_load_elapsed=0.000 composed_elapsed=5.141
*************************** 28.row ***************************
QUERY_PLAN:                      check_rows=90000
*************************** 29.row ***************************
QUERY_PLAN:       -> Data Exchange 1:1 (cost=0.310..0.310 rows=10 width=82)
*************************** 30.row ***************************
QUERY_PLAN:             Output: columns=4 width=82
*************************** 31.row ***************************
QUERY_PLAN:             Network: mode=concurrent fetch_times=1 fetch_rows=10 fetch_cost=0.251
*************************** 32.row ***************************
QUERY_PLAN:             Exec cost: compute=0.002 create=2026-03-13 16:34:44.806485 start=0.308 times=2 input_wait=0.000 output_wait=0.301
*************************** 33.row ***************************
QUERY_PLAN:          -> Interval on Column ts (cost=5.316..5.316 rows=10 functions=4 width=82 input_order=asc output_order=asc)
*************************** 34.row ***************************
QUERY_PLAN:                Output: columns=4 width=82
*************************** 35.row ***************************
QUERY_PLAN:                Time Range: [-9223372036854775808, 9223372036854775807]
*************************** 36.row ***************************
QUERY_PLAN:                Time Window: interval=10s offset=0a sliding=10s
*************************** 37.row ***************************
QUERY_PLAN:                Merge ResBlocks: False
*************************** 38.row ***************************
QUERY_PLAN:                Exec cost: compute=0.258 create=2026-03-13 16:34:44.800435 start=0.020 times=1 input_wait=5.038 output_wait=0.002
*************************** 39.row ***************************
QUERY_PLAN:             -> Table Scan on meters (cost=0.303..5.308 rows=100000 columns=2 width=12 order=[asc|1 desc|0] mode=ts_order data_load=sma)
*************************** 40.row ***************************
QUERY_PLAN:                   Output: columns=2 width=12
*************************** 41.row ***************************
QUERY_PLAN:                   Time Range: [-9223372036854775808, 9223372036854775807]
*************************** 42.row ***************************
QUERY_PLAN:                   Exec cost: compute=5.034 create=2026-03-13 16:34:44.800432 start=0.023 times=31 input_wait=0.000 output_wait=0.255
*************************** 43.row ***************************
QUERY_PLAN:                -> I/O cost: total_blocks=30 file_load_blocks=0 stt_load_blocks=0 mem_load_blocks=30 sma_load_blocks=0 composed_blocks=30
*************************** 44.row ***************************
QUERY_PLAN:                      file_load_elapsed=0.000 stt_load_elapsed=0.000 mem_load_elapsed=4.978 sma_load_elapsed=0.000 composed_elapsed=4.978
*************************** 45.row ***************************
QUERY_PLAN:                      check_rows=100000
*************************** 46.row ***************************
QUERY_PLAN:       -> Data Exchange 1:1 (cost=0.579..0.579 rows=10 width=82)
*************************** 47.row ***************************
QUERY_PLAN:             Output: columns=4 width=82
*************************** 48.row ***************************
QUERY_PLAN:             Network: mode=concurrent fetch_times=1 fetch_rows=10 fetch_cost=0.504
*************************** 49.row ***************************
QUERY_PLAN:             Exec cost: compute=0.005 create=2026-03-13 16:34:44.806488 start=0.307 times=2 input_wait=0.267 output_wait=0.029
*************************** 50.row ***************************
QUERY_PLAN:          -> Interval on Column ts (cost=4.232..4.232 rows=10 functions=4 width=82 input_order=asc output_order=asc)
*************************** 51.row ***************************
QUERY_PLAN:                Output: columns=4 width=82
*************************** 52.row ***************************
QUERY_PLAN:                Time Range: [-9223372036854775808, 9223372036854775807]
*************************** 53.row ***************************
QUERY_PLAN:                Time Window: interval=10s offset=0a sliding=10s
*************************** 54.row ***************************
QUERY_PLAN:                Merge ResBlocks: False
*************************** 55.row ***************************
QUERY_PLAN:                Exec cost: compute=0.207 create=2026-03-13 16:34:44.800646 start=0.027 times=1 input_wait=3.998 output_wait=0.003
*************************** 56.row ***************************
QUERY_PLAN:             -> Table Scan on meters (cost=0.311..4.224 rows=80000 columns=2 width=12 order=[asc|1 desc|0] mode=ts_order data_load=sma)
*************************** 57.row ***************************
QUERY_PLAN:                   Output: columns=2 width=12
*************************** 58.row ***************************
QUERY_PLAN:                   Time Range: [-9223372036854775808, 9223372036854775807]
*************************** 59.row ***************************
QUERY_PLAN:                   Exec cost: compute=3.996 create=2026-03-13 16:34:44.800642 start=0.032 times=25 input_wait=0.000 output_wait=0.201
*************************** 60.row ***************************
QUERY_PLAN:                -> I/O cost: total_blocks=24 file_load_blocks=0 stt_load_blocks=0 mem_load_blocks=24 sma_load_blocks=0 composed_blocks=24
*************************** 61.row ***************************
QUERY_PLAN:                      file_load_elapsed=0.000 stt_load_elapsed=0.000 mem_load_elapsed=3.922 sma_load_elapsed=0.000 composed_elapsed=3.922
*************************** 62.row ***************************
QUERY_PLAN:                      check_rows=80000
*************************** 63.row ***************************
QUERY_PLAN:       -> Data Exchange 1:1 (cost=0.580..0.580 rows=10 width=82)
*************************** 64.row ***************************
QUERY_PLAN:             Output: columns=4 width=82
*************************** 65.row ***************************
QUERY_PLAN:             Network: mode=concurrent fetch_times=1 fetch_rows=10 fetch_cost=0.262
*************************** 66.row ***************************
QUERY_PLAN:             Exec cost: compute=0.003 create=2026-03-13 16:34:44.806490 start=0.577 times=2 input_wait=0.000 output_wait=0.025
*************************** 67.row ***************************
QUERY_PLAN:          -> Interval on Column ts (cost=3.050..3.050 rows=10 functions=4 width=82 input_order=asc output_order=asc)
*************************** 68.row ***************************
QUERY_PLAN:                Output: columns=4 width=82
*************************** 69.row ***************************
QUERY_PLAN:                Time Range: [-9223372036854775808, 9223372036854775807]
*************************** 70.row ***************************
QUERY_PLAN:                Time Window: interval=10s offset=0a sliding=10s
*************************** 71.row ***************************
QUERY_PLAN:                Merge ResBlocks: False
*************************** 72.row ***************************
QUERY_PLAN:                Exec cost: compute=0.222 create=2026-03-13 16:34:44.800922 start=0.026 times=1 input_wait=2.802 output_wait=0.003
*************************** 73.row ***************************
QUERY_PLAN:             -> Table Scan on meters (cost=0.505..3.037 rows=30000 columns=2 width=12 order=[asc|1 desc|0] mode=ts_order data_load=sma)
*************************** 74.row ***************************
QUERY_PLAN:                   Output: columns=2 width=12
*************************** 75.row ***************************
QUERY_PLAN:                   Time Range: [-9223372036854775808, 9223372036854775807]
*************************** 76.row ***************************
QUERY_PLAN:                   Exec cost: compute=2.801 create=2026-03-13 16:34:44.800918 start=0.030 times=10 input_wait=0.000 output_wait=0.212
*************************** 77.row ***************************
QUERY_PLAN:                -> I/O cost: total_blocks=9 file_load_blocks=0 stt_load_blocks=0 mem_load_blocks=9 sma_load_blocks=0 composed_blocks=9
*************************** 78.row ***************************
QUERY_PLAN:                      file_load_elapsed=0.000 stt_load_elapsed=0.000 mem_load_elapsed=2.756 sma_load_elapsed=0.000 composed_elapsed=2.756
*************************** 79.row ***************************
QUERY_PLAN:                      check_rows=30000
*************************** 80.row ***************************
QUERY_PLAN: Planning Time: 0.484 ms
*************************** 81.row ***************************
QUERY_PLAN: Execution Time: 8.138 m
```

## 诊断建议

### 没有使用标签索引

```text
-> Table Scan on meters (...)
      Time Range: [2026-01-01 00:00:00, 2026-01-31 23:59:59]
      Tag Index Filter: conditions=location='Beijing' and groupid=2
      Primary Filter: ts >= 2026-01-01 00:00:00 and ts < 2026-02-01 00:00:00
```

重要：

- 在扫描诊断中，`Tag Index Filter` 是否出现应优先检查；它直接决定是否能在扫描前缩小子表/分片范围，需手动创建
- 若没有 `Tag Index Filter`，即使写了标签条件，也可能退化为更大范围扫描，导致 `check_rows`、`file_load_blocks` 和整体耗时显著上升

### 查询慢，但扫描不重

重点看：

- `Sort`
- `Group Sort`
- `Merge`
- `Aggregate`
- `Exec cost: compute=...`

如果扫描层 `I/O cost` 不高，而上层 `compute` 很高，通常瓶颈在计算、排序或归并。

### 查询慢，而且扫描量大

重点看：

- `Time Range`
- `Primary Filter`
- `Tag Index Filter`
- `check_rows`
- `file_load_blocks`

如果 `Time Range` 没有缩小、`Tag Index Filter` 未出现、`check_rows` 又很大，往往说明扫描裁剪不充分。

### 计划里出现很多 `Data Exchange`

重点看：

- `Network: fetch_cost`
- `fetch_times`
- `rows=平均值(最大值)`
- `slowest_vgroup_id`

如果交换层代价高，说明瓶颈可能在跨节点传输，而不是本地算子。

### 某些节点特别慢

重点看：

- `slowest_vgroup_id`
- `slow_deviation`
- `cost_ratio`
- `data_deviation`

判断方法：

- `data_deviation` 高：更可能是数据倾斜
- `data_deviation` 不高但 `cost_ratio` 高：更可能是某个节点资源异常或热点竞争

### 过滤条件写了很多，但性能没有改善

重点看：

- `Filter`
- `Primary Filter`
- `Tag Index Filter`
- `Filter: efficiency=...`

如果过滤条件只停留在上层算子，而没有出现在扫描层，通常说明还没有真正做到早过滤。

## 使用建议

- 日常查看计划，优先使用 `EXPLAIN VERBOSE true`
- 排查慢查询，优先使用 `EXPLAIN ANALYZE VERBOSE true`
- 重点观察是否出现 `Tag Index Filter`、`Primary Filter`、`Data Exchange`、`Exec cost` 和 `I/O cost`
- 在多 vgroup 查询中，优先关注所有 `平均值(最大值)` 形式的字段，它们最容易暴露长尾问题
- 当 `rows`、`width`、`fetch_cost` 同时偏大时，通常意味着网络传输压力会非常明显

## 相关文档

- [数据查询](./20-select.md)
- [特色查询](./24-distinguished.md)
- [关联查询](./25-join.md)
- [标签索引](./26-tagindex.md)
