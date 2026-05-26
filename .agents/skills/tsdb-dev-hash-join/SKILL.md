---
name: tsdb-dev-hash-join
description: "Develop, debug, review, and optimize hash join operator in TDengine query engine. Covers inner/left/right/full outer/semi/anti join types. Use when implementing new hash join features, diagnosing incorrect results, fixing crashes or memory issues, tuning performance, or reviewing hash join code. Keywords: hash join, join operator, build phase, probe phase, hash table, join condition, equi-join, semi join, anti join, full outer join, TDengine executor."
metadata:
  author: wpan
  version: 1.0.0
  owner_team: engine
  compatibility:
    os:
      - "linux"
      - "windows"
      - "macos"
---

# tsdb-dev-hash-join

Use this skill to develop, debug, review, and optimize the hash join operator in the TDengine query engine.

## When to use

- Developing new hash join features or extending join type support
- Diagnosing incorrect join results (missing rows, extra rows, wrong values)
- Fixing crashes, memory corruption, or memory leaks in hash join execution
- Tuning hash join performance (reducing memory copies, improving throughput)
- Reviewing hash join related code changes
- Understanding the hash join architecture and execution flow

## Inputs

Collect the following context based on the task type:

### For bug diagnosis

- Symptoms: what is wrong (incorrect results, crash, OOM, hang)
- Join type and subtype: inner / left outer / right outer / full outer / semi / anti
- Join conditions: equality columns, ON conditions, post-filters
- Input data characteristics: row counts, NULL presence, timestamp ordering, data types involved
- Whether the issue is reproducible with unit tests or only in full-pipeline execution
- Core dump or sanitizer output if available

### For development tasks

- Which join type or feature is being added or modified
- Current state of the implementation (what works, what remains)
- Whether plan-level changes are also required

### For code review

- The specific files and functions under review
- Which join types are affected by the change

### For performance tuning

- Query and table schema
- Build table size and probe table size
- Memory usage observations
- Whether the bottleneck is in build phase, probe phase, or result emission

If critical input is missing, ask these clarifying questions first:

1. Which join type and subtype is involved?
2. Is the issue in the operator layer (hashjoin.c / hashjoinoperator.c) or in the plan layer?
3. Can the issue be reproduced with unit tests in joinTests.cpp?
4. What is the approximate data scale (build-side rows, probe-side rows)?

## Workflow

### 1. Identify the problem category

Classify the issue based on symptoms:

- **Incorrect results**: determine which part is wrong
  - Matched rows incorrect: build-probe matching logic error
  - Non-matched rows incorrect (LEFT/RIGHT/FULL/ANTI): NMatch emission logic error
  - Incomplete output: operator did not drain all results before signaling done
  - Wrong row count: duplicate emission or missed rows in linked list traversal
- **Crash or memory corruption**: likely caused by
  - Buffer overflow in page pool or key serialization
  - Use-after-free in hash table or linked list
  - NULL pointer dereference on unexpected input
- **OOM or high memory usage**: typically caused by
  - Large build-side hash table (all rows kept in memory)
  - Page pool growing without bound
- **Performance regression**: typically caused by
  - Suboptimal join algorithm selection (hash join may not be the best choice for the given data characteristics)
  - Missing plan-level optimizations (filter pushdown, condition pushdown, build/probe side selection)
  - Excessive memory copies during result construction
  - Inefficient key serialization for multi-column keys
  - Suboptimal hash table sizing

### 2. Diagnose the issue

Follow the order: symptoms -> join type logic -> data flow -> code path.

#### 2.1 Narrow down by join type

For result correctness issues, identify which logical part produces wrong output. Taking FULL OUTER JOIN as an example:

1. Are matched rows (probe-build pairs) correct?
2. Are non-matched probe rows (NULL-padded build columns) correct?
3. Are non-matched build rows (NULL-padded probe columns) correct?
4. Are rows with NULL keys handled correctly?
5. Does the operator output all rows before signaling completion?

Each join type has a clear execution flow in hashjoin.c. Read the corresponding `h<Type>JoinDo` function to trace the logic step by step.

#### 2.2 Check the three-phase state machine

LEFT, ANTI, and FULL joins use a three-phase probe model:

- **PRE phase**: probe rows before the join time window — emitted as non-matching
- **CUR phase**: probe rows within the time window — matched against the hash table
- **POST phase**: probe rows after the time window — emitted as non-matching

Verify that phase transitions (PRE -> CUR -> POST) happen at the correct row indices, especially when `hasTimeRange` is true.

#### 2.3 Check the two-block output strategy

When `pPreFilter` exists (non-equi ON conditions):

1. Matched rows go to `midBlk` first
2. `pPreFilter` is applied to `midBlk`
3. Surviving rows are merged into `finBlk`
4. If no rows survive and all build rows are exhausted, a NULL-padded probe row is emitted
5. If `midBlk` overflows `finBlk`, `midRemains` is set and blocks are swapped on next call

Verify that the midBlk/finBlk swap logic and threshold checks are correct.

#### 2.4 Reproduce with unit tests

Use joinTests.cpp to reproduce the issue:

- The test framework generates random input data with configurable parameters
- It independently computes expected results using brute-force nested loops
- It supports all join types, condition types, and filter combinations
- Check whether the failing scenario is covered by existing test combinations
- Each test case has two key tunable parameters: **execution count** (how many random rounds to run) and **block size** (rows per data block); these must be adjusted to match the testing goal:
  - Too few iterations or too small a block size → insufficient data volume, low branch coverage, bugs may not be triggered
  - Too many iterations or too large a block size → execution time grows sharply, CI timeout risk
  - Tune upward when targeting edge cases (boundary rows, multi-block spill, large NULL ratio); tune downward for quick smoke checks
- Because the tests contain randomized input data, a single successful run does not prove the implementation is correct; use sufficiently large data volume and enough iterations to improve the chance of exposing edge-case defects.

#### 2.5 Determine the root-cause layer

- **Plan layer**: join type selection, condition pushdown, build/probe assignment
- **Operator framework** (hashjoinoperator.c): initialization, main loop, block management
- **Join implementation** (hashjoin.c): per-join-type matching and emission logic
- **Hash table**: key collision, serialization mismatch, linked list corruption
- **Memory management**: page pool allocation, buffer overflow, leak

#### 2.6 Performance diagnosis priorities

When diagnosing performance issues, follow this priority order:

**Priority 1: Analyze SQL and data characteristics to determine if hash join is optimal.**
Examine the SQL statement, left and right table data characteristics (row counts, cardinality, data distribution), and time ranges. Determine whether hash join is the best algorithm choice for this query, or whether merge join or other approaches would be more efficient given the data properties.

**Priority 2: Analyze SQL and data characteristics for plan-level optimization opportunities.**
Examine whether filters, join conditions, or expressions can be pushed down further in the query plan. Check whether the build/probe side assignment is optimal (smaller table as build side). Evaluate whether time range restrictions can reduce the amount of data entering the join operator.

**Priority 3: Analyze operator-level implementation efficiency.**
Only after confirming that hash join is the right algorithm and the plan is reasonably optimized, proceed to analyze implementation-level performance issues such as memory copy overhead, key serialization efficiency, hash table sizing, and block threshold tuning.

### 3. Apply fixes

#### 3.1 Fix the root cause

- Fix the specific code path identified in diagnosis
- Do not use workarounds that mask the underlying problem

#### 3.2 Validate with unit tests

- Ensure ALL existing unit tests in joinTests.cpp pass
- If unit test results mismatch, first verify whether the expected result in the test is correct:
  - If expected result logic/data is wrong, fix the issue in the unit test framework or test case setup
  - If expected result is correct, locate and fix the problem in product code
- During local debugging, some joinTests.cpp cases may be temporarily commented out, but before final code submission all commented test cases MUST be re-enabled.
- If a test case shows random failures or intermittent failures, do not dismiss it as noise; the root cause MUST be identified and the fix MUST be validated with repeatable tests.
- Validate in stages: run with small data volume first for fast correctness checks, then run with large data volume for stress and coverage verification; empty-data, small-data, and large-data scenarios MUST all pass.
- If the bug was not caught by existing tests, analyze why and update test coverage:
  - Prefer modifying existing test logic to cover the new case
  - Add new test cases only when the scenario is fundamentally different
- For plan-layer issues, add integration test cases

#### 3.3 Build and run tests

`joinTests.cpp` test execution follows standard GoogleTest usage, including full-suite execution, filtered execution, and single-test execution.

Linux build commands:

```bash
cd TDinternal
mkdir -p debug && cd debug

# Standard build with unit tests
cmake .. -DBUILD_SANITIZER=true -DBUILD_TOOLS=true -DGRANT_VALUE=365 -DBUILD_TEST=true
make -j8 && make install
```

Run hash join unit tests (standard gtest style):

```bash
# Run all tests in the binary
./build/bin/joinTests

# Run one test suite
./build/bin/joinTests --gtest_filter=HashJoinTest.*

# Run a single test case
./build/bin/joinTests --gtest_filter=HashJoinTest.FullOuterJoinWithNullKeys
```

Single test example:

```bash
./build/bin/joinTests --gtest_filter=HashJoinTest.LeftAntiJoinBasic
```

Build option reference:

| Option | Purpose |
|--------|---------|
| `-DBUILD_SANITIZER=true` | Enable AddressSanitizer for memory issue detection |
| `-DBUILD_TEST=true` | Compile unit tests |
| `-DGRANT_VALUE=365` | Prevent license expiration during local testing |
| `-DBUILD_TOOLS=true` | Build auxiliary tools |

Windows build commands:

```bat
"C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat" x64
cmake .. -G "NMake Makefiles JOM" -DBUILD_TEST=true -DBUILD_TOOLS=true -DCMAKE_BUILD_TYPE=Debug
jom -j 4
```

## Design Principles

### 1. Memory management

- Memory usage is not limited at the operator level; it is controlled by the global query memory management framework
- The build-side hash table is fully in-memory; no disk spill is planned currently
- Page pool uses 10 MB pages allocated on demand

### 2. Performance requirements

- Minimize memory copy operations in result construction
- Use batch assignment wherever possible instead of row-by-row copying
- Key columns from the probe side are read directly from the current block, not copied redundantly
- Variable-length columns require per-row size calculation; minimize overhead

### 3. Error handling

- Every function that can fail MUST return an error code
- Every call to a function that returns an error code MUST check the return value
- Use `HJ_ERR_RET(c)` for immediate return on error, `HJ_ERR_JRET(c)` for goto-based cleanup

### 4. Unit test coverage

- Every feature, bug fix, and code branch in the join operator MUST be testable and verifiable through unit tests
- All unit tests in joinTests.cpp must be enabled and pass in the final submitted version
- If a bug is discovered that was not caught by existing tests, analyze the test gap first, then fix or extend the test (this is the priority before fixing the bug itself)
- For plan-layer changes, corresponding integration test cases must be added
- Each test case's **execution count** and **block size** are independently configurable; choose values appropriate for the scenario:
  - Execution count controls how many random data rounds are generated; increase for thorough regression, decrease for quick smoke tests
  - Block size controls rows per data block; vary it to exercise single-block, multi-block boundary, and large-batch paths
  - Avoid leaving these at defaults without consideration — values that are too small risk missing edge cases, values that are too large risk unacceptably long CI run times
- Test data scale must be validated progressively: start from empty-data and small-data cases to confirm basic correctness, then expand to large-data cases to verify multi-block, stress, and high-volume behavior.
- Since joinTests.cpp uses random data generation, one successful run is not sufficient evidence that the code is correct; prefer enough test data volume and enough iterations to maximize effective coverage within acceptable execution time.
- Any random or intermittent test failure discovered in joinTests.cpp must be treated as a real defect signal until proven otherwise; find the root cause and verify the fix with repeated test execution.

### 5. Code comments

- All key data structures MUST have comments explaining their purpose, constraints, and limitations
- All functions MUST have comments describing their functionality, parameters, return values, and side effects
- All function parameters MUST have comments when their purpose is not self-evident from the name
- Comments should focus on the "why" and constraints/limitations, not just restating what the code does

### 6. Plan-level optimization

- Push down filters and join conditions as much as possible
- Prefer pushing conditions to the build side to reduce hash table size
- Time range filters should be evaluated before hash insertion

### 7. Code organization

- `hashjoin.c`: per-join-type execution logic (the `h<Type>JoinDo` functions)
- `hashjoinoperator.c`: operator framework (init, main loop, teardown, shared utilities)
- `hashjoin.h`: all type definitions, constants, and function declarations
- `join.h`: shared primitives used by both hash join and merge join

## Code Review

### Highest-risk review targets

- Changes to the probe loop that may break the PRE/CUR/POST phase transitions
- Changes to midBlk/finBlk swap logic (can cause row loss or duplication)
- Changes to hash table insertion that may corrupt linked lists
- Changes to key serialization (NULL handling, composite key layout, variable-length keys)
- Changes affecting the `grpSingleRow` optimization (SEMI/ANTI without ON condition)
- Changes to `hJoinSetDone` that may free resources still in use
- Changes that affect one join type but may have side effects on others
- Missing error code checks on any function call

### Per-join-type review focus

| Join Type | Key Review Points |
|-----------|-------------------|
| INNER | NULL key handling (must skip), no non-match emission |
| LEFT/RIGHT OUTER | PRE/POST phase correctness, NULL-padded row emission for unmatched probe rows |
| SEMI | `grpSingleRow` optimization correctness, at-most-one-row-per-probe guarantee |
| ANTI | Inverted match logic, NULL key probe rows must be emitted as non-matching |
| FULL OUTER | `SFGroupData` bitmap tracking, build-side non-match emission, NULL key handling on both sides |

### Review output format

1. Affected join types and execution paths
2. Risk assessment and potential side effects on other join types
3. Specific code locations and issues found
4. Required test coverage (existing tests sufficient or new tests needed)

## TDengine Hash Join Architecture

### Core source files

| File | Path | Purpose |
|------|------|---------|
| hashjoin.h | `source/libs/executor/inc/hashjoin.h` | Type definitions, constants, function declarations |
| join.h | `source/libs/executor/inc/join.h` | Shared join primitives (EJoinTableType, macros) |
| hashjoin.c | `source/libs/executor/src/hashjoin.c` | Per-join-type execution logic |
| hashjoinoperator.c | `source/libs/executor/src/hashjoinoperator.c` | Operator framework, init, main loop, utilities |
| joinTests.cpp | `source/libs/executor/test/joinTests.cpp` | Unit tests with random data generation and result verification |

### Execution flow

```text
createHashJoinOperatorInfo (init)
  -> hJoinSetBuildAndProbeTable (assign build/probe sides)
  -> hJoinInitTableInfo x2 (key cols, value cols, expressions)
  -> tSimpleHashInit (create hash table)
  -> hJoinSetImplFp (bind joinFp and buildFp)

hJoinMainProcess (called repeatedly by upstream)
  -> buildFp (first call only: build the hash table)
       -> for each build block:
            hJoinFilterTimeRange -> hJoinLaunchEqualExpr -> hJoinAddRowToHash
  -> for each probe block:
       hJoinPrepareStart (set up context, determine phase)
         -> joinFp (hInnerJoinDo / hLeftJoinDo / hSemiJoinDo / hAntiJoinDo / hFullJoinDo)
              -> for each probe row: serialize key, hash lookup, emit results
       -> apply pFinFilter if present
       -> return finBlk to upstream when threshold reached
  -> hJoinSetDone (release resources when probe exhausted)
```

### Join type dispatch

| Join Type | Sub Type | joinFp | buildFp | Hash Value Type |
|-----------|----------|--------|---------|-----------------|
| INNER | any | hInnerJoinDo | hJoinBuildHash | SGroupData |
| LEFT / RIGHT | OUTER | hLeftJoinDo | hJoinBuildHash | SGroupData |
| LEFT / RIGHT | SEMI | hSemiJoinDo | hJoinBuildHash | SGroupData |
| LEFT / RIGHT | ANTI | hAntiJoinDo | hJoinBuildHash | SGroupData |
| FULL | OUTER | hFullJoinDo | hFullJoinBuildHash | SFGroupData |

For RIGHT joins, build and probe sides are swapped so that LEFT join logic applies symmetrically.

See `references/data-structures.md` for detailed struct definitions and `references/architecture.md` for the complete execution flow.

## Output

Use the following fixed output structure:

```yaml
task_type: <development|bug-fix|performance|code-review>
join_types_affected:
  - <INNER|LEFT|RIGHT|FULL|SEMI|ANTI>
problem_analysis:
  category: <incorrect-results|crash|oom|performance|plan-issue|new-feature>
  root_cause_layer: <plan|operator-framework|join-implementation|hash-table|memory-management>
  description: <concise root cause or task description>
code_locations:
  - file: <file path>
    function: <function name>
    issue: <what is wrong or what needs to change>
remediation:
  - <action-1>
  - <action-2>
test_plan:
  unit_tests:
    - <existing test coverage status>
    - <new tests needed if any>
  integration_tests:
    - <needed for plan-layer changes>
  all_tests_pass: <true|false>
open_risks:
  - <risk-1>
```

Acceptance criteria:

- Root-cause layer is explicitly identified
- All affected join types are listed
- Unit test coverage is verified (all tests in joinTests.cpp pass)
- If a bug was found, test gap analysis is included
- No changes break other join types

## Safety

- Do not modify core join logic without understanding the complete execution flow for all affected join types
- Every code change must have corresponding unit test coverage
- Do not ignore error return codes or bypass error checking
- Do not fix a bug in one join type at the cost of breaking another
- All unit tests in joinTests.cpp must pass before submitting; all tests must be enabled in the final version
- When a bug is found, first analyze why existing unit tests did not catch it, then fix the test gap (priority) or add new tests
- Pre-submit test gate: ensure no joinTests.cpp cases remain commented out or otherwise disabled, and run full joinTests without `--gtest_filter` before final submission

High-risk operation confirmation flow:

1. Before modifying shared code (join.h, common functions in hashjoinoperator.c), assess impact on all join types.
2. Before modifying hash table or page pool logic, verify memory safety with AddressSanitizer.
3. Before submitting, ensure all unit tests pass with all test cases enabled.

## References

- See `CHEATSHEET.md` for a quick diagnosis checklist and build command reference.
- See `references/data-structures.md` for detailed struct and enum definitions.
- See `references/architecture.md` for the complete execution flow and memory layout.
- Latest source code: `/local/TDinternal.1/community/source/libs/executor/`
## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-dev-hash-join version=0.1.8 author=wpan`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->

