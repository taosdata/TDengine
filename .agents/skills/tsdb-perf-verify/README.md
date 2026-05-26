# Performance Verification Skill

This skill verifies that performance optimizations achieved the expected improvements.

## What it does

- Loads baseline metrics from previous run
- Re-runs the exact same test scenario
- Compares throughput, latency, and CPU hotspot metrics
- Produces a comparison report and determines if goals are met

## Verified workflow

1. Load baseline: `cat /tmp/baseline_metrics.txt`
2. Restart taosd with optimized build
3. Re-run write and query scripts
4. Compare and save to `${ISSUE_DIR}/results/optimization_report.md`

## Files

- `skill.md`: skill definition and usage guidance
- `config.json`: metadata and triggers

## Next Steps

- If goals met: code review and merge
- If not: return to **tsdb-perf-profiling** and iterate
