---
name: tsdb-dev-external-window
description: "Use when investigating or modifying TDengine external_window semantics, planner/executor flow, or regression behavior. Chinese trigger words: external_window, external window, externalwindowoperator, test_external, _wstart, _wend, calcWithPartition, extWinSplit"
metadata:
  author: xsren
   version: 1.0.0
   owner_team: engine
compatibility: "TDinternal or TDengine source trees containing community/source, community/test, and community/docs"
---

# tsdb-dev-external-window

## When to Use

Use this skill when the user needs to understand or modify the implementation of `external_window`, especially in these situations:

- Analyze why an `external_window(...)` query returns incorrect rows, missing rows, duplicate rows, wrong ordering, or broken grouping.
- Modify parser, planner, or executor behavior for `external_window`.
- Add new regression coverage for `external_window`, or locate the cause of an existing regression failure.
- Explain the relationship among `_wstart`, `_wend`, window companion columns such as `w.xxx`, outer `PARTITION BY`, and subquery `PARTITION BY/GROUP BY`.
- Investigate issues related to `extWinSplit`, `calcWithPartition`, multi-group output, nested external window, or dynamic windows.

Before activating this skill, confirm that the current workspace contains at least one of these paths:

- `community/source/libs/executor/src/externalwindowoperator.c`
- `community/test/cases/13-TimeSeriesExt/08-ExternalWindow/test_external.py`
- `community/docs/zh/05-basic/03-query.md`

If the workspace does not contain these paths, do not activate this skill. Ask the user for the correct repository or file locations first.

Path convention for the rest of this skill:

- In TDinternal, TDengine source code lives under the `community/` subdirectory.
- Unless a step explicitly says otherwise, all repository-relative paths below should be interpreted relative to `community/`.
- Example: `source/libs/executor/src/externalwindowoperator.c` means `community/source/libs/executor/src/externalwindowoperator.c` in TDinternal.

Preferred trigger keywords:

- `external_window`
- `external window`
- `externalwindowoperator`
- `test_external`
- `_wstart`
- `_wend`
- `calcWithPartition`
- `extWinSplit`

## Input

Required information:

- What kind of problem the user cares about: semantic explanation, implementation debugging, regression fix, test extension, or plan-chain analysis.
- The relevant SQL, or a minimal reproducer query.

Strongly recommended additions:

- Whether the execution path is streaming or non-streaming.
- Whether there is an outer `PARTITION BY`.
- Whether the `external_window` subquery contains `PARTITION BY`, `GROUP BY`, or `INTERVAL`.
- Whether the failing path is aggregate, projection, or indefinite rows.
- The failure symptom: wrong rows, missing rows, wrong window boundaries, ordering errors, filtering errors, confused group ids, unsupported STMT path, and so on.

If the user does not provide SQL but clearly refers to a code change, you can also work directly from:

- A failing function name.
- A failing test name.
- Relevant file paths.

Optional information:

- `EXPLAIN` output.
- Relevant error codes or error text.
- Failing case names or `ans` file names.

## Output

The response should include at least these parts:

1. Semantic judgment
   - Whether the SQL satisfies `external_window` constraints.
   - Whether the failure looks more like a parser, planner, executor, or test-expectation issue.

2. Code location
   - The files and functions that should be inspected first.
   - Which stage owns the issue: syntax, translation, logical plan, physical plan, or execution.

3. Fix or test guidance
   - The smallest likely change point.
   - The regression case that should be added or updated.
   - The test entrypoint that should be run.

4. If the user asks for implementation analysis
   - A call-chain overview for `external_window`.
   - The relationship among trigger-group, calc-group, window matching, and result emission.

## Execution Steps

The investigation order must stay fixed as: `docs/semantics -> parser -> planner -> executor -> regression`.

Do not start by staring only at `externalwindowoperator.c`, because parser or planner constraint issues are easy to misdiagnose as executor bugs.

### 1. Confirm semantics and SQL constraints first

Read these locations first:

- `docs/zh/05-basic/03-query.md`
- `source/libs/parser/inc/sql.y`
- `source/libs/parser/src/parTranslater.c`

Verify these rules before anything else:

- `EXTERNAL_WINDOW((subquery) alias)` is a special window clause.
- The first two columns of the subquery must be `timestamp`.
- If the outer query has no `GROUP BY` or `PARTITION BY`, the subquery cannot introduce its own `GROUP BY` or `PARTITION BY`.
- `WHERE` cannot reference columns exposed by `EXTERNAL_WINDOW`.
- Although `FILL` has a dedicated grammar branch, current behavior should be treated as unsupported according to regression coverage.

Key implementation anchors:

- `checkExternalWindowSubquerySchema()` validates subquery structure.
- Parser grammar lives in `sql.y`, mainly under `twindow_clause_opt` and `external_window_fill_opt`.

### 2. Inspect how the planner lowers semantics into the execution plan

Read these locations first:

- `source/libs/planner/src/planLogicCreater.c`
- `source/libs/planner/src/planPhysiCreater.c`
- `source/libs/planner/src/planSpliter.c`

Focus on these points:

- `createWindowLogicNodeByExternal()` converts outer `PARTITION BY` into `calcWithPartition`.
- The external window logical node requires globally ordered input, and outer partitioning reduces output ordering guarantees to the in-group level.
- The splitter enables `extWinSplit` for external window scenarios.
- `needGroupSort` often works together with `calcWithPartition` and controls output ordering across multiple groups.

If the problem looks like this:

- `EXPLAIN` is missing an external window node.
- Upper-level sort or project nodes bind the wrong columns.
- Partition semantics are already lost in planning.

Stop at the planner layer first. Do not jump straight to the executor.

If the issue adds node fields or changes behavior, also verify these serialization and clone paths:

- `source/libs/nodes/src/nodesCloneFuncs.c`
- `source/libs/nodes/src/nodesCodeFuncs.c`
- `source/libs/nodes/src/nodesMsgFuncs.c`

### 3. Start execution analysis from the operator main path

Primary file:

- `source/libs/executor/src/externalwindowoperator.c`

Core entrypoints:

- `createExternalWindowOperator()`
- `createMergeAlignedExternalWindowOperator()`
- `extWinOpen()` / `extWinNext()`
- `mergeAlignExtWinNext()`

Separate the three execution modes first:

- Scalar projection path: `EEXT_MODE_SCALAR`.
- Aggregate path: `EEXT_MODE_AGG`.
- Indefinite rows path: `EEXT_MODE_INDEFR_FUNC`.

Then inspect four categories of state:

1. Window sets
   - `SExtWinCalcGrpCtx.pWins`
   - In non-streaming mode, windows may be prebuilt by `extWinInitNonStreamWindowDataFromBlock()`.

2. Group contexts
   - Trigger-group: `SExtWinTrigGrpCtx`.
   - Calc-group: `SExtWinCalcGrpCtx`.
   - In multi-group execution, `baseGId` and `groupId` drive switching in `extWinSwitchInitTGrpCtx()` and `extWinSwitchInitCGrpCtx()`.

3. Window matching strategy
   - Single-table, non-overlapping: `extWinGetNoOvlpWin()`.
   - Single-table, overlapping: `extWinGetOvlpWin()`.
   - Multi-table, non-overlapping: `extWinGetMultiTbNoOvlpWin()`.
   - Multi-table, overlapping: `extWinGetMultiTbOvlpWin()`.
   - Merge-aligned mode uses `mergeAlignExtWinGetWinFromTs()` separately.

4. Result output
   - Scalar and indefinite paths emit through `pOutputBlocks` plus `pWinRowIdx`.
   - The aggregate path buffers rows in `resultRows`, then merges them in `extWinAggOutputRes()`.

Fast triage rules:

- Stable syntax errors or reproducible error codes: inspect the parser first.
- Missing plan fields, wrong split strategy, or wrong group sort: inspect the planner first.
- Wrong row counts, wrong content, boundary bugs, or grouping bugs after execution: inspect the executor.
- Only golden outputs differ: compare `ans` files and existing cases first, and decide whether this is an implementation regression or an expected-result change.

### 4. For partition issues, inspect `baseGId/groupId` normalization first

When the symptom is “some partitions have no output”, “partitions bleed into each other”, or “nested external window loses partitions”, inspect these functions first:

- `extWinResolveBaseGroupIdForPartition()`
- `extWinResolveCalcGroupIdForPartition()`
- `extWinResolveBlockIdForPartition()`
- `extWinSwitchInitCtxs()`
- `extWinAssignBlockGrpId()`

Focus on these checks:

- Whether the current block carries only `groupId` and no `baseGId`.
- Whether execution falls into the compatibility branch for “only one trigger-group”.
- Whether enabling `calcWithPartition` still gets skipped by gating logic that assumes `baseGId == groupId`.

### 5. Inspect boundary, ordering, and empty-window issues this way

When the symptom is “off by one at the boundary”, “wrong order”, or “bad empty-window behavior”, inspect these points first:

- Initialization of `tw.skey` and `tw.ekey` inside `extWinInitCGrpCtx()`.
- Call sites that use `getNumOfRowsInTimeWindow(...)`.
- `extWinAggHandleEmptyWins()`
- `mergeAlignExtWinFillEmptyWins()`
- `extWinRebuildWinIdxByFilter()`

Key ideas:

- Executor-side windows are generally treated as half-open intervals `[skey, ekey)`.
- Merge-aligned dynamic windows must also emit `NULL` rows for empty windows to preserve column alignment.
- If filtering changes row counts, `pWinRowIdx` must be rebuilt in sync.
- Subquery window start times must remain non-decreasing, or non-streaming pre-initialization fails immediately.

### 6. Start regression work from the closest case cluster in `test_external.py`

Primary entrypoint:

- `test/cases/13-TimeSeriesExt/08-ExternalWindow/test_external.py`

Look for these groups first:

- `basic_query()`: basic semantics, projection, aggregation, limit, and order.
- `external_window_negative_semantics()`: semantic errors and error messages.
- `fill_external_window_negative()`: negative coverage around `FILL`.
- `vtable_external_window_regression()`: virtual-table and dynamic-window regressions.
- `stmt_external_window_regression()`: STMT restriction regressions.
- The `.ans` files for window boundary, orderby, path, cross-mix, and join scenarios.

Debugging advice:

- Search `test_external.py` for nearby cases using SQL keywords first.
- Then compare expected results in the sibling `ans/*.ans` files.
- If the problem is inside the operator, return to the matching path in `externalwindowoperator.c` and instrument there.

## File Map

All paths in this file map are relative to TDinternal's `community/` subdirectory.

- Syntax entry: `source/libs/parser/inc/sql.y`
- Semantic validation: `source/libs/parser/src/parTranslater.c`
- Logical planning: `source/libs/planner/src/planLogicCreater.c`
- Physical planning: `source/libs/planner/src/planPhysiCreater.c`
- Split strategy: `source/libs/planner/src/planSpliter.c`
- Non-streaming and general executor: `source/libs/executor/src/externalwindowoperator.c`
- Streaming executor: `source/libs/executor/src/streamexternalwindowoperator.c`
- Explain rendering: `source/libs/command/src/explain.c`
- User documentation: `docs/zh/05-basic/03-query.md`
- Regression entrypoint: `test/cases/13-TimeSeriesExt/08-ExternalWindow/test_external.py`

## Common Pitfalls

- Editing only `externalwindowoperator.c` while forgetting parser or planner constraints and node-field synchronization.
- Assuming the executor is broken when the outer query has no partition but the subquery does, even though the parser rejects that shape directly.
- Treating the window end boundary as closed, which produces off-by-one behavior near `ekey`.
- Forgetting to rebuild `pWinRowIdx` after filtering, which misaligns emitted window indexes.
- Misunderstanding the roles of `baseGId` and `groupId` in multi-group flows, then misdiagnosing the issue as a window-matching bug.
- Looking only at the aggregate path for nested or partitioned external window issues, while ignoring output ordering and group switching.
- Adding node fields but forgetting clone, code, or message paths, which breaks cross-node plan transfer or explain output.

## Safety

- Do not infer implementation details from documentation alone. Always read tests and executor code together.
- Do not mix streaming and non-streaming paths. Both have external window implementations.
- Do not adjust `groupId/baseGId` compatibility logic without matching coverage.
- Before changing boundary handling, empty-window filling, or filter-rebuild logic, add or run the corresponding regression coverage.
- If the user provides SQL but no expected result, confirm the intended semantics from the docs and existing `ans` files before proposing a code change.
- If the current workspace does not contain external-window-related source and tests, do not invent paths or implementation details.
- If you are preparing the shared-repository version, avoid absolute machine paths and keep only repository-relative paths.

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-dev-external-window version=1.0.0 author=xsren`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
