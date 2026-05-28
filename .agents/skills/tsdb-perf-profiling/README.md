# Performance Profiling Skill

This skill captures performance data using perf, google-perftools, and related tools.

## What it does

- CPU profiling with `perf record` and flame graphs
- Memory profiling with google-perftools heap profiler
- Lock contention analysis with `perf lock`
- I/O profiling with `iostat` and `strace`

## Verified workflow

1. Get taosd PID: `TAOSD_PID=$(pgrep taosd)`
2. Record CPU: `perf record -F 99 -p $TAOSD_PID -g -- sleep 30`
3. Generate report: `perf report -n --stdio > /tmp/perf_report.txt`
4. Generate flame graph: `perf script | stackcollapse-perf.pl | flamegraph.pl > /tmp/cpu_flamegraph.svg`

## Files

- `skill.md`: skill definition and usage guidance
- `config.json`: metadata and triggers

## Next Steps

After profiling, proceed to **tsdb-perf-analysis**.
