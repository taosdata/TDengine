---
title: EXPLAIN
slug: /tdengine-reference/sql-manual/explain
---

`EXPLAIN` shows the execution plan of a query. It is useful for SQL tuning, slow-query diagnosis, tag-index hit analysis, filter pushdown verification, and troubleshooting cross-node data exchange.

`EXPLAIN ANALYZE` adds runtime metrics on top of the execution plan, helping you determine whether the bottleneck is in scanning, filtering, sorting, network exchange, or data skew across vgroups.

## Syntax

```sql
EXPLAIN [ANALYZE] [VERBOSE {true | false}] query_or_subquery;
```

## Parameters

### `ANALYZE`

Executes the statement and returns runtime metrics. Compared with plain `EXPLAIN`, it additionally shows:

- Time to first row, time to last row, and output row count for each operator
- Operator execution time and wait time
- Scan I/O cost
- Cross-node data exchange cost
- Planning time and execution time

Notes:

- `ANALYZE` requires the target query statement to actually run.

Diagnostic value:

- Distinguishes whether a slow SQL is caused by a poor plan or slow execution
- Identifies hot vgroups, slow nodes, and data skew
- Verifies the real cost of filters, sorts, window operators, aggregations, and more

### `VERBOSE`

The default value is `false`. When set to `true`, detailed properties of each operator are shown, for example:

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

Diagnostic value:

- Confirms whether the optimizer performed predicate pushdown, tag-index pushdown, and primary-key filter pushdown
- Helps locate details such as sort keys, merge keys, network exchange mode, and window parameters

## Result Format

`EXPLAIN` returns a single column named `QUERY_PLAN`. Each row is either a node in the plan tree or a row with detailed statistic information.

The execution plan is displayed as a tree:

- The topmost node is where the final result is produced
- Deeper indentation means closer to the underlying scan
- The operator name appears after `->`
- When `VERBOSE true` is enabled, rows with detailed statistic information are appended below each operator

Example:

```text
-> Projection (...)
   -> Aggregate (...)
      -> Table Scan on meters (...)
```

Reading tips:

- Read from top to bottom to see how the final result is produced
- Read from bottom to top to understand how scan, filter, aggregation, sort, and exchange are layered together
- For distributed queries, focus on `Data Exchange`, `Network`, `I/O cost`, and information about slow vgroups

## Common Operators

The following table lists common operator names that may appear in `EXPLAIN` output and their diagnostic value.

| Operator | Meaning | Diagnostic value |
| --- | --- | --- |
| `Aggregate` | Aggregation | Aggregation without grouping |
| `Block Dist Scan on ...` | Distributed block scan | Used in block-level distribution or block-level statistics scenarios |
| `Count` | Count window | Used for `COUNT_WINDOW` |
| `Data Exchange N:1` | Multi-source to single-source data exchange | Useful for observing cross-node fetch cost and network bottlenecks |
| `Dynamic Query Control for ...` | Dynamic query control operator | Mostly used to inspect complex plan scheduling and internal execution strategy |
| `Event` | Event window | Used for `EVENT_WINDOW` |
| `External on Column ...` | External-window-related operator | Used for external window or external alignment processing |
| `Fill` | Fill operator | Used for filling missing values |
| `Group Cache` | Group cache operator | Used for group caching and group reuse |
| `Group Sort` | Group sort | Used for sorting in grouped scenarios |
| `GroupAggregate` | Grouped aggregation | Aggregation with grouping keys |
| `Indefinite Rows Function` | Function operator with variable-length output | Used for special function processing |
| `Inner/Left/Right/Full ... Join` | Join operator | Focus on the join algorithm, join conditions, primary-key conditions, and time range |
| `Interp` | Interpolation operator | Used for `INTERP` |
| `Interval on Column ...` | Time-window operator | Used in `INTERVAL` queries |
| `Last Row Scan on ...` | Last-row cache scan | Used in `last_row`, `last`, and similar queries; useful for confirming whether the cache scan path is used |
| `Merge` | Merge | Used to merge multiple input streams |
| `Merge Aligned Interval on Column ...` | Aligned-window merge | Common in aligned window result scenarios |
| `Merge Interval on Column ...` | Window merge operator | Used for merging distributed window aggregations |
| `Partition on Column ...` | Partition operator | Common in data partitioning; focus on the partition column information |
| `Projection` | Projection | Responsible for column pruning, expression output, and column reordering |
| `Session` | Session window | Used for `SESSION` |
| `Sort` | Sort | Focus on whether an explicit sort occurs and its cost |
| `StateWindow on Column ...` | State window | Used for `STATE_WINDOW` |
| `System Table Scan on ...` | System table scan | Used for metadata or system database queries |
| `Table Count Scan on ...` | Table count scan | Used in table-count-related queries |
| `Table Merge Scan on ...` | Multi-table merge scan | Common in supertable, multi-subtable aggregation, or sort scenarios; useful for observing merge cost across tables |
| `Table Scan on ...` | Table scan | The most common low-level scan node; used to determine scan order, scan mode, and data loading path |
| `Tag Scan on ...` | Tag scan | Used to determine whether tags or metadata alone can reduce the scan range |
| `Virtual Table Scan on ...` | Virtual table scan | Used to determine whether a virtual-table query has already pruned data at the logical layer |

## Metric Quick Reference

### 1. Common Metrics in Operator Header Rows

These metrics usually appear on the `-> operator (...)` line.

| Metric | Meaning | Diagnostic value |
| --- | --- | --- |
| `algo=Merge` / `algo=Hash` | Join algorithm used by the join operator | Indicates whether the join bottleneck is more likely in sort-merge or hash build |
| `asof_op=` | Comparison operator used by ASOF JOIN | Confirms the comparison direction in nearest-point joins |
| `batch_process_child=` | Whether child tasks are processed in batches | Helps analyze execution strategy in dynamic plans |
| `batch_scan=` | Whether batch scan is enabled | Indicates whether a supertable or batched subtable query uses the batch path |
| `blocking=` | Whether the operator is blocking; `1` means it buffers enough data before producing output | Blocking operators are more likely to increase time-to-first-row latency |
| `columns=` | Number of columns processed or output by the operator | Too many columns may indicate insufficient column pruning, increasing scan, network, and memory overhead |
| `cost=first..last` | Appears only in `EXPLAIN ANALYZE`; time from operator creation to first row and last row, in milliseconds | Helps distinguish slow first response from slow full processing; a large first value often indicates scan, network, or sort warm-up, while a large last value often indicates a large result set or heavy computation |
| `data_load=data` | Reads raw data blocks | Indicates that detailed data must actually be accessed |
| `data_load=no` | Full data blocks do not need to be loaded | Indicates that the query mainly depends on metadata, indexes, or statistics |
| `data_load=sma` | Reads SMA/TSMA results | Indicates that the query may hit a precomputed path |
| `functions=` | Number of functions handled by the operator | Helps estimate computational complexity for aggregation, windowing, interpolation, and similar operators |
| `global_group=` | Whether this is a global group cache | Helps analyze the scope of group caching |
| `group_by_uid=` | Whether grouping is by UID | Commonly used for understanding internal grouping strategy |
| `group_join=` | Whether grouped join is enabled | Used in diagnosing complex joins |
| `groups=` | Number of grouping keys | Helps determine whether grouping dimensions are excessive |
| `has_partition=` | Whether partition information is included | Used to determine whether a virtual table or dynamic query retains partition attributes |
| `input_order=` | Input time order | Indicates whether upstream already satisfies the ordering requirement of the current operator |
| `jlimit=` | Maximum number of joined rows per input row | Used to analyze whether join amplification is constrained |
| `limit=` | `LIMIT` seen by the current operator | Helps confirm whether `LIMIT` has been pushed down early |
| `mode=grp_order` | Scan organized by group order | Indicates that the plan emphasizes grouped output order |
| `mode=seq_grp_order` | Sequential grouped scan | Common in scenarios grouped by table or tag while preserving order |
| `mode=sort` | Merge or combine in sort mode | Indicates that the current merge process depends on sort keys |
| `mode=ts_order` | Scan organized in time order | Common in normal time-series scans |
| `offset=` | `OFFSET` seen by the current operator | Helps determine whether offset trimming participates at this layer |
| `order=[asc\|x desc\|y]` | Counts of forward and reverse scan reads | Indicates whether the scan is mainly ascending or descending |
| `origin_vgroup_num=` | Original number of vgroups | Useful for observing the parallelism of virtual supertable queries |
| `output_order=` | Output time order | Indicates whether the operator changes ordering, which helps infer whether later sorting can still be avoided |
| `partitions=` | Number of partition keys | Helps understand the scale of `PARTITION BY` dimensions |
| `pseudo_columns=` | Number of pseudo-columns such as `_wstart`, `_wend`, `tbname`, and so on | Useful for confirming whether window columns, table name columns, and similar values have entered the execution pipeline |
| `rows=` | Number of rows output by the operator | Useful for spotting where row counts expand or where a vgroup outputs an unusually large amount of data |
| `seq_win_grp=` | Whether sequential window grouping is used | Useful for understanding execution strategy for window joins and complex windows |
| `slimit=` | `SLIMIT` seen by the current operator | Helps determine whether partition output count is constrained early |
| `soffset=` | `SOFFSET` seen by the current operator | Helps determine whether partition offset takes effect |
| `src_scan=` | Source scan position under dynamic control | Mainly used for complex plans and support troubleshooting |
| `uid_slot=` | UID slot information | Mainly used for complex plans and support troubleshooting |
| `vgroup_slot=` | vgroup slot information | Mainly used for complex plans and support troubleshooting |
| `width=` | Row width in bytes | Wide rows amplify scan, sort, network exchange, and memory usage; if the header-row `width` differs from the `Output` row `width`, the operator usually carries intermediate or auxiliary columns internally |
| `window_offset=(x, y)` | Offset range for a window join | Confirms the left and right bounds of a time-window join |

Notes:

- In multi-vgroup aggregated output, `a(b)` means `average(maximum)`
- For `rows=`, `b` can be used to quickly locate the heaviest execution node
- For `cost=`, the first value mostly corresponds to first-response latency, while the last value mostly corresponds to total processing time

### 2. Structural and Attribute Metrics with `VERBOSE true`

| Metric | Meaning | Diagnostic value |
| --- | --- | --- |
| `Buffers:` | Sort buffer size | Used to determine sort memory pressure |
| `End Cond: ...` | Event window end condition | Confirms the end trigger for `EVENT_WINDOW` |
| `fetch_cost=` | RPC fetch time in the exchange stage | A direct indicator of network or remote-node pressure |
| `fetch_rows=` | Rows fetched per node | Helps determine whether network batch size is reasonable |
| `fetch_times=` | Number of fetches in the exchange stage | Too many fetches may mean the data is fragmented too finely or the downstream repeatedly pulls small batches |
| `Fill Values: ...` | Fill value list | Confirms whether fill values for `FILL` or `INTERP` are correct |
| `Filter: ... efficiency=xx%` | Filter efficiency; appears only with `ANALYZE` | Lower usually means filtering is more selective; if the predicate is strong but efficiency is near 100%, it may not be applied at the expected layer |
| `Filter: conditions=...` | Filter condition at the current operator | Shows where a filter predicate is executed |
| `Join Col Cond: ...` | Join condition on non-primary-key columns | Used to see whether column conditions are pushed into the join layer |
| `Join Full Cond: ...` | Full join condition | Confirms the final join condition expression |
| `Join Param: ...` | Additional join parameters | Used to analyze comparison operators, offset ranges, and limits in ASOF/WINDOW JOIN |
| `Join Prim Cond: ...` | Primary-key join condition | Used to determine whether the primary-key join condition is extracted separately |
| `Left Equal Cond:` | Equality columns on the left side | Used to analyze equality join keys |
| `Left Table Time Range: ...` | Time range of the left table | Used to diagnose cases where time predicates only hit one side of a join |
| `Left/Right Table Time Range: ...` | Time ranges of both sides | Confirms that both sides of the join are time-pruned |
| `loops:` | Number of sort loops | Higher values usually indicate more processing batches or more merge rounds |
| `Merge Key: ...` | Keys used for multi-way merge | Confirms which keys are used to merge multi-shard results |
| `Merge ResBlocks: True/False` | Whether result blocks must be merged | `True` means block-level merging exists at this layer and may add memory and CPU overhead |
| `Network: mode=...` | Data exchange mode, `concurrent` or `sequence` | Concurrent fetch usually provides better throughput; sequential fetch is more likely to show tail latency |
| `Output: columns=... width=...` | Number of columns and row width actually emitted downstream | Used to confirm whether column pruning is effective |
| `Output: Ignore Group Id: true/false` | Whether group IDs are ignored in this operator's output | If `true`, later stages no longer distinguish upstream group boundaries |
| `Partition Key: partitions=n` | Partition key information | Confirms whether `PARTITION BY` takes effect |
| `Primary Filter: ...` | Primary-key filter condition, usually timestamp or other primary-key-related predicates | Indicates that primary-key conditions have been pushed down to a lower layer, which is usually critical for performance |
| `Right Equal Cond:` | Equality columns on the right side | Used to analyze equality join keys |
| `Right Table Time Range: ...` | Time range of the right table | Used to diagnose cases where time predicates only hit one side of a join |
| `Sort Key: ...` | Sort key | Confirms whether sorting uses the expected columns |
| `Sort Method: quicksort / merge sort` | Sorting algorithm | Helps distinguish in-memory sorts from heavier merge-sort paths |
| `Start Cond: ...` | Event window start condition | Confirms the start trigger for `EVENT_WINDOW` |
| `Tag Index Filter: conditions=...` | Filter conditions that can use a tag index | The most direct indicator of whether a tag index is hit |
| `Time Range: [start, end]` | Time scan range of the current operator | Confirms whether the time predicate has been pruned successfully |
| `Time Window: interval=... offset=... sliding=...` | Time-window parameters | Confirms whether window size, offset, and sliding step match expectations |
| `Window Count=` | Count-window size | Confirms the window size of `COUNT_WINDOW()` |
| `Window Sliding=` | Count-window sliding step | Confirms the sliding setting of `COUNT_WINDOW()` |
| `Window: gap=...` | Session window gap | Used to diagnose how `SESSION()` is split |

### 3. Operator's Execution-Cost Metrics

`EXPLAIN ANALYZE VERBOSE true` additionally shows an `Exec cost` row under operators.

| Metric | Meaning | Diagnostic value |
| --- | --- | --- |
| `Exec cost:` | Summary of execution cost for the current operator | Helps determine whether the operator is slow because it computes slowly, waits too long, or is blocked by upstream/downstream |
| `compute=` | Time spent inside the operator itself, excluding time waiting on child data, in milliseconds | A high value usually means the operator itself is expensive because of computation, aggregation, sorting, or scanning |
| `create=` | Operator creation time; for multi-node execution this is shown as average creation time and latest creation time, in the current system time zone | Large differences across nodes may indicate uneven task dispatch or scheduling imbalance |
| `input_wait=` | Total time waiting for child data, in milliseconds | A high value usually means the bottleneck is more likely in child nodes or remote scans |
| `output_wait=` | Total time waiting for the parent operator to request more data, in milliseconds | A high value means this operator produces results but the parent consumes them slowly |
| `start=` | Time from creation until the operator is called for the first time, in milliseconds | A large value means the operator was created early but entered execution late |
| `times=` | Number of times the operator is invoked | An unusually large value often means the parent is pulling data in very small batches |

Notes:

- A single-node plan shows a single value; a multi-node plan shows `average(maximum)`
- In multi-node output, `create=` also appears as `average_timestamp(latest_timestamp)`

### 4. I/O Metrics for Scan Operators

In `EXPLAIN ANALYZE VERBOSE true`, scan operators also show three `I/O cost` rows.

| Metric | Meaning | Diagnostic value |
| --- | --- | --- |
| `check_rows=` | Number of rows checked or filtered | If much larger than `rows`, the scan volume before filtering is large |
| `composed_blocks=` | Number of composed result blocks | Indicates that scan results were additionally assembled into blocks |
| `composed_elapsed=` | Time spent composing result blocks | A high value means block assembly or reorganization is expensive |
| `cost_ratio=` | Cost ratio between the slowest node and the fastest node | The larger the ratio, the more severe the skew |
| `data_deviation=` | Data-volume deviation of the slowest node relative to the median node | Helps determine whether a slow node is slow because it computed slowly or simply received more data |
| `file_load_blocks=` | Number of blocks loaded from files | A high value indicates heavier disk reads |
| `file_load_elapsed=` | Time spent loading file blocks | Used to assess disk I/O pressure |
| `mem_load_blocks=` | Number of blocks loaded from memory | A high value usually indicates good cache hit rate |
| `mem_load_elapsed=` | Time spent loading memory blocks | If this is high too, there may be too many blocks or overly wide rows |
| `slow_deviation=` | Cost deviation of the slowest node relative to the median node | A high value indicates obvious tail latency |
| `slowest_vgroup_id=` | ID of the slowest vgroup; appears only for multi-node execution | Directly identifies the slow node |
| `sma_load_blocks=` | Number of blocks loaded from SMA/TSMA | Indicates whether the pre-aggregation path is used |
| `sma_load_elapsed=` | Time spent loading SMA/TSMA data | Used to evaluate the benefit of the pre-aggregation path |
| `stt_load_blocks=` | Number of blocks loaded from STT-related structures | Used to analyze how much the STT path participates |
| `stt_load_elapsed=` | Time spent loading STT data | Used to assess STT-path cost |
| `total_blocks=` | Total number of processed blocks | Reflects overall scan workload |

Diagnostic tips:

- If `file_load_*` is high, first check the time range, tag filters, and index hits
- If `mem_load_*` is high but the query is still slow, focus on result row width, sort cost, and aggregation complexity
- If `sma_load_*` is `0`, the query did not hit the precomputed path
- If `cost_ratio` and `slow_deviation` are high, first locate hot vgroups, uneven data distribution, or abnormal node resources, then use `data_deviation` to narrow it down further

### 5. Plan-Level Summary Metrics

At the end of the output, `EXPLAIN ANALYZE` also shows summary information for the whole statement.

| Metric | Meaning | Diagnostic value |
| --- | --- | --- |
| `Execution Time:` | Actual end-to-end execution time of the plan | The overall metric for final latency |
| `Planning Time:` | Time spent generating the execution plan | If high, focus on SQL complexity, join depth, and optimizer overhead |

## Examples

### 1. Show the execution plan of a normal query

```sql
taos> EXPLAIN SELECT ts, current FROM meters WHERE ts >= '2026-01-01 00:00:00' AND ts < '2026-02-01 00:00:00' \G;
*************************** 1.row ***************************
QUERY_PLAN: -> Data Exchange 4:1 (width=12)
*************************** 2.row ***************************
QUERY_PLAN:    -> Projection (columns=2 width=12 input_order=asc)
*************************** 3.row ***************************
QUERY_PLAN:       -> Table Scan on meters (columns=2 width=12 order=[asc|1 desc|0] mode=ts_order data_load=data)
```

### 2. Show the detailed plan

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

### 3. Show execution-process metrics

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

-- Note: a tag index must be created manually
```

### 4. Aggregation query

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

### 5. Time-window query

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

## Diagnostic Suggestions

### No tag index is used

```text
-> Table Scan on meters (...)
      Time Range: [2026-01-01 00:00:00, 2026-01-31 23:59:59]
      Tag Index Filter: conditions=location='Beijing' and groupid=2
      Primary Filter: ts >= 2026-01-01 00:00:00 and ts < 2026-02-01 00:00:00
```

Important:

- When diagnosing scans, first check whether `Tag Index Filter` appears. It directly determines whether the engine can narrow the subtable or shard range before scanning. This index must be created manually.
- If `Tag Index Filter` does not appear, the query may fall back to a much larger scan even if tag predicates are written, causing `check_rows`, `file_load_blocks`, and total latency to increase significantly.

### The query is slow, but the scan is not heavy

Focus on:

- `Sort`
- `Group Sort`
- `Merge`
- `Aggregate`
- `Exec cost: compute=...`

If scan-layer `I/O cost` is low but upper-layer `compute` is high, the bottleneck is usually in computation, sorting, or merging.

### The query is slow, and the scan volume is large

Focus on:

- `Time Range`
- `Primary Filter`
- `Tag Index Filter`
- `check_rows`
- `file_load_blocks`

If `Time Range` is not narrowed, `Tag Index Filter` does not appear, and `check_rows` is large, scan pruning is usually insufficient.

### The plan contains many `Data Exchange` nodes

Focus on:

- `Network: fetch_cost`
- `fetch_times`
- `rows=average(maximum)`
- `slowest_vgroup_id`

If the exchange layer is expensive, the bottleneck may be cross-node transfer rather than local operators.

### Some nodes are much slower than others

Focus on:

- `slowest_vgroup_id`
- `slow_deviation`
- `cost_ratio`
- `data_deviation`

How to judge:

- If `data_deviation` is high, data skew is more likely
- If `data_deviation` is not high but `cost_ratio` is high, abnormal node resources or hotspot contention is more likely

### Many filter conditions are written, but performance does not improve

Focus on:

- `Filter`
- `Primary Filter`
- `Tag Index Filter`
- `Filter: efficiency=...`

If filter conditions remain only in upper-level operators and do not appear at the scan layer, early filtering usually has not actually happened.

## Recommendations

- For day-to-day plan inspection, start with `EXPLAIN VERBOSE true`
- For slow-query troubleshooting, start with `EXPLAIN ANALYZE VERBOSE true`
- Pay special attention to whether `Tag Index Filter`, `Primary Filter`, `Data Exchange`, `Exec cost`, and `I/O cost` appear
- In multi-vgroup queries, prioritize fields shown as `average(maximum)`, because they are the easiest place to spot long-tail issues
- When `rows`, `width`, and `fetch_cost` are all large, network transfer pressure is usually significant

## Related Documentation

- [Query Data](./20-select.md)
- [Feature Query](./24-distinguished.md)
- [Join Queries](./25-join.md)
- [Tag Indices](./26-tag-index.md)
