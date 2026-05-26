# Performance Issue Reproduction Skill

This skill creates a structured workspace for reproducing and analyzing performance issues.

## What it does

- Creates a per-issue directory under `/root/ccdocs/`
- Generates README.md, reproduction script, analysis script, and notes template
- Provides a results directory for storing profiling data

## Verified workflow

1. Set issue name: `ISSUE_NAME="your_issue_name"`
2. Create workspace: `mkdir -p /root/ccdocs/${ISSUE_NAME}/results`
3. Generate `reproduce_issue.sh` and `analyze_performance.sh`
4. Document findings in `notes.md`

## Files

- `skill.md`: skill definition and usage guidance
- `config.json`: metadata and triggers

## Next Steps

After creating the workspace:
1. Customize scripts for the specific issue
2. Run reproduction to confirm the issue
3. Use **tsdb-perf-scenario-setup** → **tsdb-perf-profiling** → **tsdb-perf-analysis** workflow
