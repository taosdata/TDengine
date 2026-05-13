---
sidebar_label: Virtual Table Query Optimization
title: Virtual Table Query Optimization
toc_max_heading_level: 4
---

A Virtual Table is a logical table structure designed by TDengine for Industrial IoT scenarios. A virtual table does not store data itself. Instead, it combines columns from multiple physical source tables through column references, aligning them by timestamp to provide users with a unified query view.

While virtual tables simplify SQL for cross-table queries, the underlying implementation faces significant performance challenges: a naive execution strategy requires full merging of data from all source tables, which is prohibitively expensive. To address this, TDengine introduces several optimization strategies during query plan generation to minimize unnecessary data merging and scanning while preserving query semantics.

This document describes two core optimizations: **Pushdown Aggregation** and **Two-Phase Window Query Splitting**.

## Pushdown Aggregation

:::note
In this section and subsequent sections, the operator names used in execution plan diagrams (such as Agg, ColsMerge, AggA, etc.) are conceptual labels intended to illustrate the optimization approach. They do not correspond to actual operator names in TDengine's EXPLAIN output.
:::

### Background

Querying a virtual table can be abstracted into the following model:

```text
User Query → Virtual Table → [Source Table A, Source Table B, Source Table C, ...]
```

When querying a virtual table, the engine reads data from each source table, sorts and aligns them by timestamp, merges the multiple data streams into a unified result set, and then performs the user-specified aggregation.

Consider the following aggregation query:

```sql
SELECT count(a), sum(b) FROM vtable;
```

Column `a` comes from source table A, and column `b` comes from source table B. Assume that source tables A and B each contain 1 million rows. Under the naive execution path, the engine must first merge the full data from A and B by timestamp into a temporary result set, then perform the aggregation on that result set.

This process incurs a dual performance penalty:

1. **Merge overhead**: Even if the timestamps in A and B are perfectly aligned, the merge operator must perform row-by-row timestamp comparison and column concatenation across 1 million rows — a substantial computational cost in itself.
2. **Loss of SMA information**: TDengine's storage engine maintains SMA (pre-aggregation statistics) on each data block, recording summary values such as count, sum, min, and max for each column within the block. When aggregation is performed directly on a source table, the engine can leverage SMA information to skip row-level scanning and read pre-computed results directly. However, the dynamically generated result set from merging does not carry SMA information from the original data blocks, forcing aggregation functions to fall back to row-by-row computation.

The fundamental conflict is that a virtual table appears as "one table" to the user but is "multiple tables" to the engine. If the execution plan fails to recognize this structural characteristic, it pays unnecessary merge costs to maintain the single-table abstraction while losing access to the storage engine's optimization capabilities.

### Dependency Analysis

Revisiting the query above, `count(a)` requires only the data from column `a`, which exists only in source table A; `sum(b)` requires only the data from column `b`, which exists only in source table B. There is no data dependency between the two aggregation functions — each can independently complete its computation using only its corresponding source table's data.

This leads to the key optimization insight: **If all input parameters of an aggregation function depend on a single source table, the aggregation can be computed directly on that source table without waiting for data merging.** This determination can be made during query plan generation through column dependency analysis.

### Optimization Strategy

Based on the analysis above, TDengine implements Pushdown Aggregation optimization, which splits aggregation functions by source table dependency, enabling each source table to perform its aggregation independently and then combining results by column.

**Execution plan before optimization:**

```text
Agg[count(a), sum(b)]
    └── VirtualTableScan (merge full data from A and B)
            ├── ScanA (1 million rows)
            └── ScanB (1 million rows)
```

The engine scans 1 million rows each from A and B, the merge operator performs row-by-row timestamp comparison and column concatenation to produce a result set of approximately 1 million rows (the actual count depends on timestamp alignment), and then executes count and sum on it. The merged result set carries no SMA information, so aggregation can only proceed row by row.

**Execution plan after optimization:**

```text
ColsMerge (combine by column)
    ├── AggA[count(a)] → ScanA
    └── AggB[sum(b)] → ScanB
```

AggA executes `count(a)` directly on source table A. Since the aggregation operates on original data blocks, the engine can directly leverage SMA information to read pre-computed count values. AggB similarly uses SMA to compute `sum(b)`. Finally, the ColsMerge node combines the two results by column before returning them.

This optimization provides two benefits:

1. **Eliminates merge overhead**: Row-by-row timestamp comparison and column concatenation across 1 million rows is completely bypassed.
2. **Restores SMA acceleration**: Aggregation operates directly on original data blocks, enabling the engine to use block-level pre-aggregation information and reducing aggregation from row-level scanning to block-level reads — potentially achieving orders-of-magnitude performance improvement.

### Multiple Aggregation Functions

When multiple aggregation functions depend on the same source table, they are grouped together:

```sql
SELECT count(a), avg(a), sum(b), max(b) FROM vtable;
```

The optimized execution plan is:

```text
ColsMerge
    ├── AggA[count(a), avg(a)] → ScanA
    └── AggB[sum(b), max(b)] → ScanB
```

Each group only scans a single source table's data, avoiding merge overhead while fully utilizing SMA information.

When an aggregation function's input parameters depend on columns from multiple source tables, the function cannot be pushed down and must remain on the post-merge execution path. However, in typical time-series data scenarios, the vast majority of aggregation functions depend on columns from a single source table, giving this optimization broad coverage.

## Two-Phase Window Query Splitting

### Background

While Pushdown Aggregation is highly effective for plain aggregation queries, it has limitations when facing window queries. Window queries introduce the concept of window boundaries, creating real data dependencies between source tables that prevent simple independent computation.

In TDengine, window queries allow users to partition data into multiple windows by time intervals, state changes, or other criteria, and then perform independent aggregation within each window. For example:

```sql
SELECT _wstart, _wend, avg(b) FROM vtable STATE_WINDOW(a);
```

This query partitions data into windows based on value changes in column `a`, computing the average of column `b` within each window.

The challenge is that column `a` (the state column) resides in source table A, while column `b` (the aggregation column) resides in source table B. Window boundaries are determined by A, but the data to aggregate is in B. A real data dependency exists between the two source tables — the computation range of `avg(b)` depends on the state changes in `a`, and B must know A's window boundaries before it can begin computation.

In time-series data scenarios, state columns are typically sparse — device states are written only when changes occur, resulting in very small data volumes. Measurement columns, on the other hand, are very dense — sensors continuously collect at high frequencies, potentially accumulating millions of rows. Consider a typical scenario: the state column `a` has 50 rows, while the aggregation column `b` has 5 million rows. The naive execution plan is:

```text
WindowAgg[avg(b), STATE_WINDOW(a)]
    └── VirtualTableScan (merge full data from A and B)
            ├── ScanA (50 rows, sparse state changes)
            └── ScanB (5 million rows, dense measurements)
```

Even though the state column has only 50 rows, the naive approach still requires full merging with the 5-million-row aggregation column — an extremely expensive operation.

### Dependency Structure Analysis

Analyzing the dependency relationships in window queries reveals two key structural characteristics:

1. **The dependency is unidirectional**: Window boundaries depend solely on the state column `a` (source table A), and aggregation depends solely on the data column `b` (source table B) plus the window boundaries. B depends on A's window boundaries, but A does not depend on any data from B. The dependency direction is A → B, not A ↔ B.
2. **Data volumes are highly asymmetric**: The state column that determines window boundaries is typically very sparse (tens of rows), while the measurement column requiring aggregation is very dense (millions of rows). The cost of determining boundaries is minimal, while the cost of full merging is almost entirely driven by the dense aggregation column.

These two characteristics point to a clear optimization direction: first determine window boundaries at minimal cost on the sparse state column, then have the dense aggregation column read only data within the window coverage for computation, thereby avoiding both full merging and full scanning.

### Optimization Strategy

Based on the unidirectional dependency characteristic, TDengine splits window queries into two execution phases.

#### Phase 1: Determine Window Boundaries

Execute window partitioning on the source table A where the state column resides. No actual aggregation is performed — only the start and end timestamps of each window are output:

```text
WindowSplit(STATE_WINDOW(a)) → ScanA
Output: [(_wstart₁, _wend₁), (_wstart₂, _wend₂), ...]
```

Since the state column itself is sparse, the scanning cost of this phase is extremely low. The resulting window list is also small, typically containing only a few to a few hundred windows.

#### Phase 2: Independent Aggregation per Source Table

The window boundaries produced in Phase 1 are distributed to source tables that need to perform aggregation. Each source table independently executes range scans and aggregation within the window time ranges:

```text
ExtWindowAgg[avg(b)] → ScanB (scans only data within window ranges)
```

Source table B does not need to understand the window partitioning logic. It only needs to receive a set of `(_wstart, _wend)` intervals and compute `avg(b)` within each interval. The aggregation column no longer requires a full scan — it reads only data within each window's time range, skipping gap data between windows.

#### Complete Execution Plan

The two phases are coordinated by a scheduling node (DynQueryCtrl). The complete execution plan is:

```text
DynQueryCtrl (scheduling node)
    │
    ├── Phase 1: WindowSplit(a) → ScanA
    │       Output: window boundary list
    │
    └── Phase 2 (starts after boundaries are determined):
            ColsMerge
                ├── ExtWindowAgg[avg(b)] → ScanB
                └── ExtWindowAgg[...] → ScanC (if more source tables exist)
```

The scheduling node waits for Phase 1 to complete, obtains the window list, and then triggers the executors for each source table in Phase 2.

### Performance Comparison

Using the typical scenario described above: state column `a` has 50 rows, aggregation column `b` has 5 million rows.

**Before optimization (full merge):**

- ScanA reads 50 rows, ScanB reads 5 million rows
- Window partitioning and aggregation are performed on the merged result of approximately 5 million rows
- Total processing: full merge of 5 million rows plus the sorting overhead of the merge itself

**After optimization (two-phase splitting):**

- Phase 1: ScanA reads 50 rows, producing 50 windows
- Phase 2: ScanB performs range scans based on the 50 window boundaries, reading only data within window coverage
- Total processing: 50-row scan + range aggregation on far fewer than 5 million rows, with merge overhead completely eliminated

### Multiple Source Table Aggregation

When a query involves multiple source tables, window boundaries only need to be determined once, and each source table can execute window aggregation in parallel:

```sql
SELECT _wstart, avg(b), max(c) FROM vtable STATE_WINDOW(a);
```

Where column `a` is in source table A, column `b` is in source table B, and column `c` is in source table C. The optimized execution plan is:

```text
DynQueryCtrl
    ├── Phase 1: WindowSplit(a) → ScanA → Output window boundaries
    └── Phase 2:
            ColsMerge
                ├── ExtWindowAgg[avg(b)] → ScanB
                └── ExtWindowAgg[max(c)] → ScanC
```

The more source tables involved, the greater the advantage of this "determine boundaries once, aggregate in parallel" approach.

### Applicability

The two-phase splitting strategy can be generalized to other types of window queries:

- **Time windows (INTERVAL)**: Window boundaries are fixed time intervals shared naturally by all source tables. Phase 1 can be skipped entirely, proceeding directly to parallel aggregation.
- **Session windows (SESSION)**: Session boundaries are determined by activity intervals in a column, similar to state windows, and equally suitable for two-phase splitting.

The differences between window types are concentrated in Phase 1 (how boundaries are determined). The logic of Phase 2 (independent aggregation by boundary) is universal.

## Summary of Optimization Principles

Both optimizations follow the same design principles:

|  | Pushdown Aggregation | Two-Phase Window Splitting |
| :--- | :--- | :--- |
| Core problem | No data dependencies between aggregation functions; merge unnecessary | Unidirectional dependency between window boundaries and aggregation data; can be processed in phases |
| Optimization approach | Split aggregation functions by source table; execute independently | Determine window boundaries first; then aggregate by boundary independently |
| Common principle | Analyze dependency structure, identify independence, defer data convergence | Analyze dependency structure, identify independence, defer data convergence |

The general principle underlying both optimizations can be stated as: **Perform dependency analysis during query plan generation, identify computation units that can be processed independently, and defer the data convergence point as late as possible so that data completes as much computation as possible before convergence.** The later data converges, the smaller the data volume to process, and the better the engine can leverage underlying storage optimizations such as SMA pre-aggregation.

This approach is fundamentally identical to predicate pushdown, column pruning, and partition pruning in relational databases. The multi-source-table structure of virtual tables provides a prime scenario for applying this principle: boundaries between source tables are naturally clear, dependency relationships are straightforward to analyze, and pushdown benefits are substantial — especially in a time-series database system like TDengine, where the storage layer (including SMA) has been deeply optimized.
