# Performance Data Analysis Skill

This skill uses AI to analyze profiling data and identify performance bottlenecks.

## What it does

- Reads perf reports, heap reports, and lock reports from `/tmp/`
- Identifies top CPU-consuming functions and call chains
- Analyzes memory allocation patterns and potential leaks
- Produces a structured analysis report

## Verified workflow

1. Read reports: `head -100 /tmp/perf_report.txt`
2. AI identifies hotspot functions (>5% overhead)
3. Analyze call chains and self vs total time
4. Save analysis to `${ISSUE_DIR}/results/perf_analysis_report.md`

## Files

- `skill.md`: skill definition and usage guidance
- `config.json`: metadata and triggers

## Next Steps

After analysis, proceed to **tsdb-perf-code-locate**.
