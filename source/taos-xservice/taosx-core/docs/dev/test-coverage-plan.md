# Unit Test Coverage Improvement Plan (taosx-core)

## Goals
- Raise and sustain unit/integration coverage for `src/**` with fast, deterministic tests.
- Protect critical behaviors (parsing, config validation, task orchestration, sinks/runners) from regressions.
- Make coverage visible in CI and block regressions where practical.

## Baseline & Tooling
- Generate a baseline report: `cargo llvm-cov nextest --workspace --lcov --output-path lcov.info`. Store artifacts in CI.
- Run with relevant feature sets: `cargo llvm-cov nextest`, `cargo llvm-cov nextest --all-features`, and minimal feature combos (e.g., `--no-default-features --features rustls`).
- Add a coverage job in CI that uploads HTML and lcov to artifacts for quick inspection.

## Baseline Coverage Metrics (2025-12-12)
**Overall taosx-core coverage: 57.3%** (21,226/37,041 lines)

### Module Breakdown:
- **legacy**: 18.6% (19/102 lines)
- **migrations**: 28.7% (188/654 lines)
- **tmq**: 38.8% (428/1,103 lines)
- **taoz**: 40.7% (337/827 lines)
- **lib.rs**: 41.1% (223/543 lines) - **Critical priority**
- **s3**: 43.0% (176/409 lines)
- **utils**: 56.3% (2,687/4,771 lines)
- **transform**: 57.1% (301/527 lines)
- **plugins**: 59.6% (16,122/27,047 lines)
- **task_set**: 70.4% (745/1,058 lines)

### Critical Low-Coverage Files:
- `lib.rs`: 0.0% - ConnectorLicense expiry logic
- `migrations/users.rs`: 0.0% - User SQL generation
- `migrations/privileges.rs`: 0.0% - Privilege SQL generation
- `core_metrics.rs`: 38.6% - Atomic counters and metrics

## Progress Updates

### 2025-12-12: Phase 2 Quick Wins Completed
**Coverage after improvements: 58.1%** (21,802/37,544 lines) - **+0.8% gain**

#### Completed:
1. **lib.rs** (ConnectorLicense): 0% → **91.4%** ✅
   - Added 16 unit tests covering all expiry methods
   - Tests: is_expired_day(), expired_days(), is_expired_second(), expired_seconds()
   - Covers boundary cases, negative expiry, and edge timestamps

2. **migrations/privileges.rs**: 0% → **95.8%** ✅
   - Added 8 unit tests for SQL generation
   - Tests: GRANT/REVOKE for database and table privileges
   - Tests: Privilege SQL with conditions, target() method

3. **migrations/users.rs**: 0% → **64.3%** ✅
   - Added 9 unit tests for User SQL generation
   - Tests: Basic users, super users, disabled users, whitelist handling
   - Tests: to_sqls() with various flags, to_sql_drop()

4. **plugins/config/mod.rs**: ~50% → **100.0%** ✅
   - Added 7 comprehensive tests for AdvancedOptions::from_dsn()
   - Tests: All options, no options, partial options, error handling
   - Tests: Zero values, invalid inputs, LogLevel parsing

5. **core_metrics.rs**: 38.6% → **53.3%** ✅
   - Added 14 unit tests for CommonMetrics and CoreMetrics
   - Tests: Metrics creation, message counting, reset functionality
   - Tests: TaskStartTime and LastPersistTime timing functions
   - Tests: CoreMetrics type unwrapping and panic conditions

6. **plugins/expr/mod.rs**: ~50% → **~65%** (estimated) ✅
   - Added 15 comprehensive unit tests for expression evaluation
   - Tests: Logical operations (AND, OR, NOT), arithmetic (*, /)
   - Tests: Null handling, type coercion (float→int, int→string)
   - Tests: Comparisons (==, !=), filter operations, empty batches
   - Tests: Serialization/deserialization with null_if_error

**Phase 2 Summary:**
- **Total new tests:** 69 unit tests
- **Files improved:** 6 critical modules
- **All tests passing:** ✅ 27 expr tests, 29 config/metrics tests

#### Next Steps:
- Phase 3: Runner and sink tests
- Phase 4: Integration and utility tests
- Phase 5: CI quality gates

## Module Map & Coverage Priorities
1) **Core correctness (high priority)**
   - `core_metrics.rs`, `global.rs`: validate atomic counters, license expiry calculations, and feature flags (e.g., agent compression) under boundary dates/times.
   - `migrations/{mod,privileges,users}.rs`: snapshot SQL generation (grant/revoke/drop), privilege/user serialization, and idempotency checks.
2) **Config and expression layer (high priority)**
   - `plugins/config`, `plugins/expr`: serde parsing/validation, default filling, error messages; expression eval against Arrow arrays with mixed types/nulls; property-based tests for round-trip conversions.
3) **Pipelines and runners (high/medium priority)**
   - `plugins/runners/{config,opc,opentsdb,influxdb,pi}`: config validation, endpoint/url parsing, unit-scale encode/decode of sample payloads, retry/backoff parameters, and task lifecycle state transitions (without network I/O; use fakes/stubs).
   - `plugins/source` and `plugins/service`: ensure plugin discovery/registration works and rejects duplicates/unknowns.
4) **Sinks and transforms (medium priority)**
   - `plugins/sink/{flat,lush,ipc_*,point::*}`: format correctness, partition/path derivation, chunking, compression toggles, and backpressure handling with bounded channels.
   - `plugins/transform` (filter/expr/constants): predicate correctness, type coercion, and failure modes.
5) **Types, utils, and peripheral modules (medium/low priority)**
   - `types`, `utils`, `s3`, `taoz`, `tmq`, `stream`, `task_set`: time/encoding helpers, URI parsing, filesystem interactions, concurrency primitives; emphasize determinism via tempdirs and controlled clocks.

## Quick Wins (first wave)
- Add unit tests for `ConnectorLicense` expiry paths (day/second) with fixed timestamps.
- Snapshot tests for privilege/user SQL strings to lock grammar.
- Config parser tests for `plugins/config` and `plugins/runners/config` using minimal and invalid inputs.
- Expression eval table-driven cases in `plugins/expr` (nulls, type mismatch, arithmetic/logic).
- Sink path/partition derivation tests in `plugins/sink/point` and small buffer flush tests in `flat`/`lush`.

## Deeper Coverage (second wave)
- Property-based/fuzz-lite tests for parsers (CSV/JSON/config) to catch corner cases.
- Concurrency tests using bounded channels to ensure send/recv ordering and backpressure in sinks/runners.
- Integration-style tests that wire a minimal runner -> transform -> sink pipeline with faked I/O (no network or filesystem side effects beyond tempdirs).

## Test Data & Fixtures
- Reuse/trim existing fixtures in `tests/` (e.g., CSV samples) to keep tests fast.
- Prefer in-memory data over filesystem/network; where FS is required, use `tempfile` and clean up via RAII.
- Provide helper builders/fakes for runners and sinks to avoid real services (e.g., stub clients implementing the minimal trait surface used in code).

## CI & Quality Gates
- Add a coverage job that runs on PRs; fail when coverage drops beyond a small tolerance from the baseline.
- Surface a short coverage delta summary in PR comments (lines/regions touched in `src/**`).
- Keep a weekly/bi-weekly target (e.g., +5% relative to baseline) until core modules exceed the agreed threshold.

## Work Breakdown (suggested order)
1) Baseline report + coverage job wiring.
2) Quick wins in core/migrations/config/expr.
3) Sinks/runners unit coverage (faked I/O + concurrency cases).
4) Transform/types/utils edge coverage.
5) Light integration pipelines and periodic cleanup (dead tests, flaky detections).

## Definition of Done
- Coverage report available from CI artifacts and checked locally with one command.
- Critical modules (core_metrics, migrations, config/expr, sinks/runners) have deterministic, isolated tests for their public surfaces and main failure paths.
- No flakiness, and new code paths ship with tests by default (enforced via code review checklist).
