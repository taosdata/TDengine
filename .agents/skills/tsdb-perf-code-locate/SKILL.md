---
name: tsdb-perf-code-locate
description: "Combine performance analysis results with source code examination to locate the exact code causing performance issues. Analyze algorithm complexity and resource usage. Keywords: performance, code locate, hotspot, source analysis"
metadata:
  author: beryl
  version: 1.0.0
  owner_team: engine
---

# Performance Code Location

## Quick Start

This skill helps locate the exact source code causing performance bottlenecks identified in the analysis phase.

## Prerequisites

- Performance analysis completed (see tsdb-perf-analysis)
- Hotspot functions identified
- Source code repository available

## Step 1: Locate Hotspot Functions

### Find Function Definition

```bash
# Search for function definition
grep -rn "function_name" /root/workspace/TDinternal/source/ --include="*.c" --include="*.cpp" --include="*.h"

# Use more specific search
grep -rn "^.*function_name\s*(" /root/workspace/TDinternal/source/ --include="*.c" --include="*.cpp"

# Find in specific directories
find /root/workspace/TDinternal/source -name "*.c" -o -name "*.cpp" | xargs grep -l "function_name"
```

### AI Search Strategy

For each hotspot function identified in analysis:

1. Search for function definition
2. Identify the file and line number
3. Read the function implementation
4. Analyze the surrounding context
5. Trace the call chain

## Step 2: Read and Analyze Code

### Read Function Implementation

```bash
# Read specific function (if you know the file)
# Use Read tool to view the file
# Example: Read source/dnode/vnode/src/tsdb/tsdbRead.c
```

### Code Analysis Checklist

When examining hotspot code, AI should check:

#### Algorithm Complexity
- [ ] Loop nesting level (O(n²) or worse?)
- [ ] Recursive calls (tail recursion? depth?)
- [ ] Search algorithms (linear vs binary?)
- [ ] Sorting operations (algorithm choice?)

#### Memory Operations
- [ ] Allocation frequency (in loops?)
- [ ] Allocation size (large allocations?)
- [ ] Memory copies (memcpy, strcpy in hot path?)
- [ ] Buffer reuse (allocate once or repeatedly?)

#### Data Structures
- [ ] Appropriate structure choice (list vs hash vs tree?)
- [ ] Cache locality (struct layout, access patterns?)
- [ ] Unnecessary indirection (pointer chasing?)
- [ ] Data duplication (same data stored multiple times?)

#### Synchronization
- [ ] Lock scope (too broad?)
- [ ] Lock frequency (in tight loops?)
- [ ] Lock ordering (potential deadlock?)
- [ ] Read-write lock usage (readers blocked by writers?)

#### I/O Operations
- [ ] Synchronous I/O in hot path?
- [ ] Small I/O operations (should batch?)
- [ ] Unnecessary fsync calls?
- [ ] Buffering strategy?

## Step 3: Trace Call Chain

### Understand Caller Context

```bash
# Find all callers of the hotspot function
grep -rn "function_name(" /root/workspace/TDinternal/source/ --include="*.c" --include="*.cpp" | grep -v "^.*function_name\s*("

# Find callers in specific files
grep -n "function_name(" /root/workspace/TDinternal/source/path/to/caller.c
```

### Call Chain Analysis

For each hotspot, trace:

1. **Who calls it?** - Identify all callers
2. **How often?** - Is it in a loop? What's the frequency?
3. **With what parameters?** - Are parameters causing inefficiency?
4. **Can it be avoided?** - Is the call necessary?

## Step 4: Identify Optimization Opportunities

### Common Optimization Patterns

#### Pattern 1: Loop Optimization

```c
// BEFORE: O(n²) nested loops
for (int i = 0; i < n; i++) {
    for (int j = 0; j < m; j++) {
        // expensive operation
    }
}

// AFTER: Reduce complexity or move work outside loop
```

#### Pattern 2: Allocation Optimization

```c
// BEFORE: Allocate in loop
for (int i = 0; i < n; i++) {
    char *buf = malloc(size);
    // use buf
    free(buf);
}

// AFTER: Allocate once, reuse
char *buf = malloc(size);
for (int i = 0; i < n; i++) {
    // reuse buf
}
free(buf);
```

#### Pattern 3: String Optimization

```c
// BEFORE: Repeated strlen calls
for (int i = 0; i < n; i++) {
    if (strlen(str) > 10) {  // strlen called n times
        // ...
    }
}

// AFTER: Cache length
int len = strlen(str);
for (int i = 0; i < n; i++) {
    if (len > 10) {
        // ...
    }
}
```

#### Pattern 4: Lock Optimization

```c
// BEFORE: Lock held too long
pthread_mutex_lock(&mutex);
// expensive operation 1
// expensive operation 2
// expensive operation 3
pthread_mutex_unlock(&mutex);

// AFTER: Reduce lock scope
// expensive operation 1 (no lock needed)
pthread_mutex_lock(&mutex);
// only critical section
pthread_mutex_unlock(&mutex);
// expensive operation 2 (no lock needed)
```

## Step 5: Document Findings

### Create Analysis Report

For each hotspot, document:

```
Function: function_name
File: path/to/file.c:line_number
Overhead: X%

Current Implementation:
- [Description of current code]
- Algorithm complexity: O(?)
- Key issues: [List problems]

Root Cause:
- [Explain why it's slow]

Optimization Opportunity:
- [Specific optimization approach]
- Expected complexity: O(?)
- Estimated improvement: X%

Implementation Plan:
1. [Step 1]
2. [Step 2]
3. [Step 3]

Risks:
- [Potential issues with the fix]

Testing:
- [How to verify the fix]
```

## Step 6: Cross-Reference with Performance Data

### Validate Findings

```bash
# Check if the code matches the performance profile
# Example: If perf shows high malloc calls, verify code has allocations in loop

# Look for the specific patterns identified in perf data
# Example: If cache-miss is high, check data structure layout
```

## AI Analysis Workflow

### For Each Hotspot Function:

1. **Locate**: Find the function in source code
2. **Read**: Read the complete function implementation
3. **Analyze**: Identify performance issues
4. **Trace**: Understand the call context
5. **Propose**: Suggest specific optimizations
6. **Estimate**: Predict improvement impact

### Example Analysis

```
Hotspot: tsdbReadRowsFromCache
File: source/dnode/vnode/src/tsdb/tsdbRead.c:245
Overhead: 35%

Analysis:
- Function iterates through cache with O(n) linear search
- Called inside a loop, resulting in O(n²) overall
- Allocates temporary buffer on each call (line 250)
- Uses strcmp for key comparison (line 260)

Root Cause:
- Linear search in hot path
- Repeated allocations
- String comparison overhead

Optimization:
1. Replace linear search with hash table lookup: O(n²) -> O(n)
2. Pre-allocate buffer and reuse: Reduce malloc overhead
3. Use integer key comparison if possible: Faster than strcmp

Expected Improvement: 60-70% reduction in this function's overhead
Overall Impact: ~20-25% total performance improvement
```

## Execution Rules

- Always read the actual source code, don't assume
- Analyze the complete function, not just snippets
- Consider the caller context and frequency
- Look for algorithmic issues first (biggest impact)
- Then look for micro-optimizations
- Validate findings against performance data
- Provide specific line numbers and file paths
- Estimate the impact of each optimization
- Consider implementation complexity vs benefit

## Output

This skill should produce:

1. **Code Location Report**: Exact file:line for each hotspot
2. **Code Analysis**: Detailed analysis of each function
3. **Optimization Proposals**: Specific, actionable fixes
4. **Impact Estimates**: Expected improvement for each fix
5. **Implementation Priority**: Ordered by impact/effort ratio

## Next Steps

After locating and analyzing the code, proceed to:
- **tsdb-perf-fix**: Implement the identified optimizations
## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-perf-code-locate version=0.1.0 author=beryl`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->

