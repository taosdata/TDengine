# Manual performance benchmarks

This directory contains benchmarks for explicit runs in a dedicated performance
environment. They reuse the pytest fixtures in `test/conftest.py`, but are outside
the `cases/` directory selected by `test/pytest.ini`. Benchmark filenames omit the
`test_` prefix and `_test.py` suffix so directory-wide collection does not pick them
up. Pass the full benchmark file path to pytest to run one.

Do not register timing thresholds in `test/ci/cases.task`. New system regression
tests belong in `test/cases/` and must be registered there. For this feature,
`cases/09-DataQuerying/04-OrderBy/test_partition_first_state_window.py` provides the
registered result-correctness coverage; planner unit tests cover plan structure.
The benchmark also checks results before measuring, but is not continuous CI
coverage.

## Partition-first state-window benchmark

Install the dependencies described in [the test framework README](../README.md)
and prepare the engine binaries. From the repository root, run:

```bash
cd source/taos-community/test
export TAOS_BIN_PATH="$(git rev-parse --show-toplevel)/debug-dev/build/bin"
export WORK_DIR="$(git rev-parse --show-toplevel)/sim/pf-state-benchmark"
pytest -s benchmarks/partition_first_state_window_performance.py
```

Set `TAOS_BIN_PATH` to the actual build output if it differs. The normal framework
deployment starts and stops the test cluster. To reuse a dedicated, already
deployed cluster, point `WORK_DIR` at its test environment and add
`--skip_deploy --skip_stop`. The benchmark recreates the database named by
`PF_STATE_PERF_DB` (default: `perf_pf_state_window`).

The default tag workload uses 1,000 groups, 20 rows per group, one vgroup, two
warmup rounds and 11 measured rounds. It alternates default and hinted queries
and requires at least 10% lower median latency. Run the regular-column workload
with three vgroups explicitly:

```bash
PF_STATE_PERF_PARTITION_MODE=column \
PF_STATE_PERF_VGROUPS=3 \
PF_STATE_PERF_MIN_GAIN_PCT=-15 \
pytest -s benchmarks/partition_first_state_window_performance.py
```

`PF_STATE_PERF_GROUPS`, `PF_STATE_PERF_ROWS_PER_GROUP`, `PF_STATE_PERF_RUNS`,
`PF_STATE_PERF_PAYLOAD_BYTES`, and `PF_STATE_PERF_MIN_GAIN_PCT` control workload and
acceptance criteria. The column mode defaults to a 15% regression tolerance.
Record the hardware, build configuration, workload parameters and measured
latencies when reporting results; these thresholds are for controlled manual
acceptance, not shared functional CI.
