# Performance Code Location Skill

This skill locates the exact source code causing performance bottlenecks.

## What it does

- Searches for hotspot functions in TDinternal source code
- Reads and analyzes function implementations
- Checks algorithm complexity, memory operations, data structures, and synchronization
- Produces a code location report with optimization proposals

## Verified workflow

1. Search: `grep -rn "function_name" /root/workspace/TDinternal/source/`
2. Read the function implementation
3. Analyze algorithm complexity and resource usage
4. Estimate impact and implementation priority

## Files

- `skill.md`: skill definition and usage guidance
- `config.json`: metadata and triggers

## Next Steps

After locating code, proceed to **tsdb-perf-fix**.
