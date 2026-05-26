---
name: tsdb-dev-query-engine
description: "Develop, debug, review, and optimize the TDengine query engine. Use when working on query planning, executor behavior, expression evaluation, scan/filter/aggregate/sort pipelines, result correctness, or query-engine performance. Trigger keywords: query engine, planner, executor, operator, expression, scan, filter, aggregation, sort, vnode query, qnode query, TDengine query."
metadata:
  author: wpan
  version: 1.0.0
  owner_team: engine
compatibility: Designed for TDengine internal repositories and query-engine work spanning client, parser, planner, catalog, scheduler, qworker, executor, function, and scalar paths.
---

# tsdb-dev-query-engine

Use this skill as the entry point for TDengine query engine work that is broader than a single specialized subsystem.

## When to Use

Use this skill for **all query-related tasks**, including but not limited to:

- Developing or refactoring query-engine features across client, parser, planner, catalog, scheduler, qworker, executor, function, or scalar paths.
- Diagnosing and locating any query-related defect: incorrect results, crashes, hangs, OOMs, wrong error codes, assertion failures, or regression reports.
- Reviewing query-engine changes for correctness, impact radius, error handling, and test gaps.
- Investigating and optimizing query performance: plan choice, operator pipeline efficiency, memory growth, data movement, or CPU-bound hot paths.
- Understanding end-to-end query flow from SQL parsing to result delivery.
- Any other work that touches or is caused by the query subsystem.

Prefer a more specific skill when one exists for the affected subsystem.

- Use tsdb-dev-hash-join for hash join operator design, debugging, review, and optimization.
- Use this skill when the problem crosses multiple query stages, the affected subsystem is unclear, or no dedicated sub-skill exists.

## Input

Minimum context to collect before acting:

- Task type: feature, bug fix, performance analysis, or code review.
- SQL statement or workload pattern that reproduces the behavior.
- Affected module or suspected stage: client, parser, planner, catalog, scheduler, qworker, executor, function, or scalar.
- Observable symptom: wrong result, crash, hang, OOM, slow query, wrong plan, or code-review concern.

Collect additional context by task type.

### Bug diagnosis

- Reproduction steps and whether the issue is stable or intermittent.
- Schema, relevant data characteristics, and NULL or boundary-value behavior.
- Query plan output if available.
- Error logs, sanitizer output, stack trace, or core information if available.

### Feature or refactor

- Target behavior and non-goals.
- Modules and interfaces that must change.
- Compatibility constraints and required test scope.
- New functionality should be split into the appropriate modules by responsibility boundaries, instead of stacking cross-layer logic in one layer.

### Performance work

- Query shape, data scale, and distribution.
- Current bottleneck evidence such as plan output, profile samples, or timing breakdown.
- Whether the issue is algorithm choice, plan optimization, or implementation efficiency.

### Code review

- Changed files or diff scope.
- Expected behavior and risk area.
- Existing tests that claim to cover the change.

If the SQL, symptom, or affected stage is missing, ask for it before making irreversible changes.

## Output

Return results in a compact structure that makes the next action obvious.

```yaml
task_type: <feature|bug-fix|performance|review>
scope:
  stage: <parser|planner|executor|function|scalar|qcom|client>
  modules:
    - <module or file>
analysis:
  symptom: <what failed>
  root_cause: <confirmed cause or current best hypothesis>
  impact: <affected query types or operators>
actions:
  - <change made or review finding>
validation:
  tests_run:
    - <command or case>
  result: <passed|failed|not-run>
risks:
  - <remaining risk or follow-up>
```

Acceptance criteria:

- The reported root cause or review finding is tied to a concrete query stage and code location.
- Changes fit the existing query-engine architecture instead of bypassing it.
- Validation covers the affected behavior and relevant regression surface.
- Remaining risks, assumptions, and unverified areas are stated explicitly.
- The plan optimizer must remain optional. Disabling any single optimization rule must still keep queries functionally correct and executable.

## Workflow

### 1. Classify the problem at the right layer

- Parser: SQL text to AST or semantic analysis issues.
- Planner: plan shape, operator selection, pushdown, pruning, or rewrite issues.
- Catalog: client-side metadata retrieval, caching, update, and eviction management. Provide both sync and async interfaces externally, and prefer batched mode for internal metadata retrieval.
- Scheduler: physical-plan scheduling and execution orchestration issues.
- Qworker: server-side message handling and query scheduling management.
- Executor: runtime operator lifecycle, block flow, memory ownership, done-state handling.
- Function or scalar: expression evaluation, type rules, or built-in function behavior.
- Client or qcom: query dispatch, result transport, or shared query utilities.

### 2. Diagnose from symptom to root cause

- For wrong results, trace SQL to plan to operator behavior before editing code.
- For crashes or memory issues, inspect ownership, boundary handling, and error paths first.
- For slow queries, decide in order: algorithm choice, plan quality, then operator-level efficiency.
- For reviews, prioritize behavioral regressions, interface mismatches, unchecked errors, and missing tests.
- When fixing crashes or coredumps, the fix is not complete until query result correctness is verified on the fixed code.

### 3. Implement minimal, architecture-consistent changes

- Fix the root cause instead of adding local workarounds.
- Preserve existing interfaces unless the change clearly requires widening scope.
- Propagate error codes and keep cleanup paths complete.
- Re-check all touched comments and assumptions after the change.
- The executor is execution-stage only and must not carry planner-stage responsibilities.
- If execution flow must change dynamically based on runtime data or partial results, implement it via a dynamic query control operator instead of embedding planning logic in executor.
- Executor operators must stay decoupled from upstream/downstream concrete operator types, and must not directly modify internal state of adjacent operators.

### 4. Validate progressively

- Reproduce with the smallest stable case first.
- Run targeted tests for the changed stage or operator.
- Expand to integration or broader query scenarios when the change affects shared planning or execution logic.

## Core Source Areas

> **Build target note:**
> - **Client modules** (parser / planner / catalog / scheduler / client query): compiled into the **taos dynamic library** (`libtaos.so` and `libtaosnative.so` on Linux, equivalent on other platforms) — changes here require rebuilding the client shared library.
> - **Server modules** (qworker / executor): compiled into `taosd` — changes here require rebuilding `taosd`.
> - **Common modules** (function / scalar / qcom): linked by both — changes here require rebuilding both the client shared library and `taosd`.

| Stage | Typical path | Build target |
|---|---|---|
| Parser | source/libs/parser/ | client shared lib |
| Planner | source/libs/planner/ | client shared lib |
| Scheduler | source/libs/scheduler/ | client shared lib |
| Catalog | source/libs/catalog/ | client shared lib |
| Qworker | source/libs/qworker/ | taosd (server) |
| Executor | source/libs/executor/ | taosd (server) |
| Function | source/libs/function/ | both |
| Scalar | source/libs/scalar/ | both |
| Query common | source/libs/qcom/ | both |
| Client query | source/client/src/ | client shared lib |

## Safety

- Do not modify framework-level components (including scheduler and qworker) before fully understanding upstream/downstream impact.
- Do not treat a plan symptom as an operator bug until the plan itself is checked.
- Do not ignore error codes, partial-cleanup paths, or done-state transitions.
- Do not rely on a single happy-path query; validate NULLs, empty inputs, boundary sizes, and complex query shapes when relevant.
- Do not hardcode local source-tree paths, machine-specific assumptions, credentials, or internal-only runtime state into the skill output.
- For destructive repository or environment actions, require explicit user confirmation first.
- Do not force new features into an existing large module. Split by responsibility into appropriate submodules first.
- Do not implement or mix planner/optimizer semantics inside executor.
- Do not let executor operators control adjacent operators via concrete type assumptions or struct-level tight coupling.
- Do not treat coredump or crash fixes as complete without verifying final query result correctness; explain the immediate crash root cause alone is insufficient.

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-dev-query-engine version=0.3.0 author=wpan`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->

