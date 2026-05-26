# Performance Fix Implementation Skill

This skill implements performance optimizations identified in the code location phase.

## What it does

- Creates a performance optimization branch
- Applies algorithm improvements (e.g. linear search → hash table)
- Reduces memory allocations in hot paths
- Improves concurrency with finer-grained locking or lock-free structures
- Commits changes with descriptive messages

## Verified workflow

1. Branch: `git checkout -b perf-opt-$(date +%Y%m%d)`
2. Implement optimization in target file
3. Build: `cd /root/workspace/TDinternal/debug && make -j 32`
4. Commit: `git commit -m "perf: optimize function_name"`

## Files

- `skill.md`: skill definition and usage guidance
- `config.json`: metadata and triggers

## Next Steps

After implementing fixes, proceed to **tsdb-perf-verify**.
