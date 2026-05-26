# Performance Scenario Setup Skill

This skill sets up a reproducible performance test scenario for TDengine.

## What it does

- Starts taosd and verifies it is running
- Generates test data using taosBenchmark or taosgen
- Creates write and query scripts under `/tmp/`
- Records baseline performance metrics

## Verified workflow

1. Start taosd: `/root/workspace/TDinternal/debug/build/bin/taosd -c /etc/taos`
2. Generate data: `taosBenchmark -y -d perf_db -t 1000 -n 100000 -T 16`
3. Create `/tmp/perf_write.sh` and `/tmp/perf_query.sh`
4. Run baseline and save to `/tmp/baseline_metrics.txt`

## Files

- `skill.md`: skill definition and usage guidance
- `config.json`: metadata and triggers

## Next Steps

After setup, proceed to **tsdb-perf-profiling**.
