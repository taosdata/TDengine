# Problem Statement
Improve unit/integration test coverage for taosx\-core to protect critical functionality from regressions, make coverage visible in CI, and establish quality gates for future development\. The project currently has test coverage infrastructure in place \(cargo\-llvm\-cov, nextest, codecov\) but requires systematic test expansion across core modules\.
# Current State
## Coverage Infrastructure
* Coverage tool: cargo\-llvm\-cov with nextest
* CI workflows: `.github/workflows/cov.yaml` uploads coverage to Codecov
* Make targets: `cargo make test` runs coverage and merges reports \(llvm\-cov\-merged\.lcov\)
* Existing tests: Scattered unit tests in many modules \(70\+ files contain \#\[test\] blocks\)
* Documentation: test\-coverage\-plan\.md outlines strategy
## Module Structure
taosx\-core/src contains:
* Core modules: lib\.rs \(ConnectorLicense\), core\_metrics\.rs, global\.rs, migrations/
* Config/expr layer: plugins/config, plugins/expr
* Pipelines/runners: plugins/runners \(opc, opentsdb, influxdb, pi\)
* Sinks/transforms: plugins/sink \(flat, lush, point\), plugins/transform
* Utilities: types, utils, s3, taoz, tmq, task\_set
## Test Coverage Status
* Some modules have extensive tests \(plugins/transform/parse/json\.rs: 19 tests\)
* Other critical modules have minimal or no tests \(migrations, core\_metrics\)
* Test fixtures exist in taosx\-core/tests/ \(kinghist\-2k\.csv, opc/\)
# Proposed Changes
## Phase 1: Baseline Generation & Analysis \(Week 1\)
### 1\.1 Generate baseline coverage report
* Run: `cargo make test` to generate target/llvm\-cov\-merged\.lcov
* Upload baseline to artifacts for reference
* Parse lcov\.info to identify:
    * Total line coverage %
    * Per\-module coverage breakdown
    * Uncovered critical paths in high\-priority modules
* Document baseline metrics in test\-coverage\-plan\.md
### 1\.2 Prioritize modules by coverage gaps
High priority \(target >80% coverage\):
* src/lib\.rs \(ConnectorLicense expiry logic\)
* src/core\_metrics\.rs \(atomic counters, metrics\)
* src/global\.rs \(global state management\)
* src/migrations/\*\.rs \(SQL generation for privileges/users\)
* src/plugins/config/mod\.rs \(config parsing/validation\)
* src/plugins/expr/mod\.rs \(expression evaluation\)
Medium priority \(target >70% coverage\):
* src/plugins/runners/config\.rs
* src/plugins/sink/point/model\.rs
* src/plugins/sink/flat\.rs, lush\.rs
* src/plugins/transform/parse/\*\.rs
Low priority \(target >50% coverage\):
* src/utils/\*\.rs \(except critical path helpers\)
* src/s3, src/taoz, src/tmq
## Phase 2: Quick Wins \- Core & Config \(Week 2\-3\)
### 2\.1 Add tests for ConnectorLicense \(src/lib\.rs:69\-106\)
Test scenarios:
* `is_expired_day()`: expired licenses \(past dates\), valid licenses, boundary cases
* `expired_days()`: calculate correct duration past expiry
* `is_expired_second()`: second\-precision expiry checks
* `expired_seconds()`: handle negative expiry values, edge timestamps
* Use fixed test timestamps via chrono::DateTime::from\_timestamp
Location: Add \#\[cfg\(test\)\] block to src/lib\.rs
### 2\.2 Add tests for migrations \(src/migrations/\*\.rs\)
Test scenarios:
* src/migrations/privileges\.rs: snapshot SQL generation for grant/revoke/drop
* src/migrations/users\.rs: user SQL generation, serialization
* src/migrations/mod\.rs: migration orchestration, idempotency
* Use insta snapshots for SQL string validation
Location: Expand existing test blocks in each module
### 2\.3 Add tests for config parsing \(src/plugins/config/mod\.rs\)
Test scenarios:
* Valid minimal configs
* Invalid configs \(missing required fields, wrong types\)
* Default value filling
* Error message validation
* Use table\-driven tests with serde\_json
Location: Expand existing 9 tests in src/plugins/config/mod\.rs:152\-313
### 2\.4 Add tests for expression evaluation \(src/plugins/expr/mod\.rs\)
Test scenarios:
* Arithmetic operations with nulls
* Type coercion/mismatches
* Logic operations \(AND, OR, NOT\)
* Function calls \(from functions\.rs\)
* Property\-based tests for round\-trip conversions
Location: Expand existing 13 tests in src/plugins/expr/mod\.rs:350\-592
## Phase 3: Runners & Sinks \(Week 4\-5\)
### 3\.1 Add tests for runner configs \(src/plugins/runners/\)
Test scenarios:
* Config validation for each runner type \(opc, pi, opentsdb, influxdb\)
* Endpoint/URL parsing
* Retry/backoff parameter validation
* Use fakes/stubs to avoid network I/O
Files to enhance:
* src/plugins/runners/config\.rs
* src/plugins/runners/opc/config/\*\.rs
* src/plugins/runners/pi/config\.rs
### 3\.2 Add tests for sinks \(src/plugins/sink/\)
Test scenarios:
* Format correctness \(flat, lush\)
* Path/partition derivation \(point module\)
* Chunking and compression toggles
* Backpressure with bounded channels
* Small buffer flush tests
Files to enhance:
* src/plugins/sink/flat\.rs \(4 existing tests\)
* src/plugins/sink/lush\.rs \(3 existing tests\)
* src/plugins/sink/point/model\.rs \(16 existing tests\)
* src/plugins/sink/point/csv\.rs
### 3\.3 Add tests for transforms \(src/plugins/transform/\)
Test scenarios:
* Filter predicates \(filter/mod\.rs: 5 existing tests\)
* Type coercion in parse modules
* Map operations \(constant, expr, cast\)
* Failure modes and edge cases
Files to enhance:
* src/plugins/transform/parse/json\.rs \(19 existing tests \- ensure coverage\)
* src/plugins/transform/map/\*\.rs
* src/plugins/transform/filter/\*\.rs
## Phase 4: Integration & Utils \(Week 6\)
### 4\.1 Add light integration tests
Test scenarios:
* Minimal runner → transform → sink pipeline
* Use faked I/O \(no network/FS beyond tempfile\)
* Test data flow and error propagation
* Concurrency/channel ordering tests
Location: Create new test files in taosx\-core/tests/integration/
### 4\.2 Add utility tests \(src/utils/\*\.rs\)
Test scenarios:
* Time/encoding helpers \(duration\.rs, timeout\.rs\)
* URI parsing \(dsn\.rs: 6 existing tests\)
* SQL helpers \(sql\.rs: 7 existing tests\)
* Filesystem interactions with tempfile \(files\.rs: 9 existing tests\)
Files to enhance:
* src/utils/mod\.rs \(10 existing tests\)
* src/utils/codec\.rs, breakpoints\.rs, trace\.rs
### 4\.3 Add property\-based tests
Test scenarios:
* Parsers \(CSV/JSON/config\) with arbitrary inputs
* Use proptest or quickcheck
* Focus on input validation and error handling
Location: Add to existing test modules or new proptest/ directory
## Phase 5: CI Quality Gates & Maintenance \(Week 7\-8\)
### 5\.1 Enhance CI coverage job
Changes to \.github/workflows/cov\.yaml:
* Add PR coverage comparison \(fail if coverage drops >2%\)
* Generate HTML report as artifact
* Add PR comment with coverage summary
* Consider codecov PR annotations
### 5\.2 Establish coverage thresholds
* Add cargo\-llvm\-cov html output to Makefile\.toml
* Set per\-module coverage targets
* Update CONTRIBUTING\.md with coverage requirements
### 5\.3 Documentation updates
* Update test\-coverage\-plan\.md with progress
* Add testing guide to taosx\-core/docs/dev/
* Document test fixtures and helpers
### 5\.4 Cleanup & maintenance
* Remove dead tests
* Fix flaky tests
* Establish code review checklist \(tests required for new features\)
# Testing Strategy
## Test Data & Fixtures
* Reuse existing fixtures: taosx\-core/tests/kinghist\-2k\.csv, opc/ samples
* Create minimal test data in\-memory where possible
* Use tempfile for FS tests with RAII cleanup
* Build helper traits/fakes for runners and sinks
## Test Patterns
* Table\-driven tests for parsers and validators
* Snapshot tests \(insta\) for SQL generation
* Property\-based tests for fuzz\-lite coverage
* Integration tests with faked I/O
* Avoid real network/database connections
## Coverage Tool Usage
```warp-runnable-command
# Generate baseline
cargo make test
# View HTML report
cargo llvm-cov --html --open nextest --workspace
# Test with features
cargo llvm-cov nextest --all-features
cargo llvm-cov nextest --no-default-features --features rustls
# Test single module
cargo llvm-cov nextest -p taosx-core --test test_name
```
# Success Metrics
* Overall taosx\-core coverage: >70% line coverage \(from baseline\)
* Core modules \(lib, core\_metrics, global, migrations\): >80% coverage
* Config/expr layers: >75% coverage
* Runners/sinks: >70% coverage
* CI fails on >2% coverage drop in PRs
* Coverage report available in CI artifacts
* Zero flaky tests
* New features require tests \(enforced in code review\)
# Definition of Done
* Baseline coverage report generated and documented
* High\-priority modules have deterministic, isolated tests
* CI coverage job with quality gates enabled
* Documentation updated with testing guide
* Code review checklist includes test requirements
* Regular coverage monitoring established \(weekly/bi\-weekly reviews\)
