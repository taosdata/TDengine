---
name: tsdb-perf-fix
description: "Implement performance optimizations based on code analysis. Apply algorithm improvements, data structure changes, concurrency optimizations, and caching strategies. Keywords: performance fix, optimization, algorithm, concurrency"
metadata:
  author: beryl
  version: 1.0.0
  owner_team: engine
---

# Performance Fix Implementation

## Quick Start

This skill implements the performance optimizations identified in the code location phase.

## Prerequisites

- Code analysis completed (see tsdb-perf-code-locate)
- Optimization opportunities identified
- Source code ready for modification

## Step 1: Prepare for Changes

### Backup and Branch

```bash
# Create a performance optimization branch
cd /root/workspace/TDinternal
git checkout -b perf-opt-$(date +%Y%m%d)

# Verify current branch
git branch

# Save current state
git stash
```

### Review Optimization Plan

Before implementing, review:
- Target function and file
- Current performance metrics
- Proposed optimization
- Expected improvement
- Potential risks

## Step 2: Implement Optimizations

### Optimization Type 1: Algorithm Improvement

#### Example: Replace Linear Search with Hash Table

```c
// BEFORE: O(n) linear search
for (int i = 0; i < count; i++) {
    if (items[i].key == target_key) {
        return &items[i];
    }
}

// AFTER: O(1) hash table lookup
typedef struct {
    SHashObj *hash;  // hash table
    // ... other fields
} OptimizedStruct;

// Initialize hash table
pStruct->hash = taosHashInit(1024, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BINARY), true, HASH_NO_LOCK);

// Insert items
taosHashPut(pStruct->hash, &item.key, sizeof(item.key), &item, sizeof(item));

// Lookup
Item *pItem = taosHashGet(pStruct->hash, &target_key, sizeof(target_key));
```

#### Example: Reduce Loop Complexity

```c
// BEFORE: O(n²) nested loops
for (int i = 0; i < n; i++) {
    for (int j = 0; j < m; j++) {
        if (array1[i] == array2[j]) {
            // match found
        }
    }
}

// AFTER: O(n) with hash set
SHashObj *set = taosHashInit(n, taosGetDefaultHashFunction(TSDB_DATA_TYPE_INT), true, HASH_NO_LOCK);
for (int i = 0; i < n; i++) {
    taosHashPut(set, &array1[i], sizeof(int), &array1[i], sizeof(int));
}
for (int j = 0; j < m; j++) {
    if (taosHashGet(set, &array2[j], sizeof(int)) != NULL) {
        // match found
    }
}
taosHashCleanup(set);
```

### Optimization Type 2: Memory Optimization

#### Example: Reduce Allocations

```c
// BEFORE: Allocate in loop
for (int i = 0; i < count; i++) {
    char *buffer = taosMemoryMalloc(BUFFER_SIZE);
    // use buffer
    taosMemoryFree(buffer);
}

// AFTER: Allocate once, reuse
char *buffer = taosMemoryMalloc(BUFFER_SIZE);
for (int i = 0; i < count; i++) {
    memset(buffer, 0, BUFFER_SIZE);  // clear if needed
    // reuse buffer
}
taosMemoryFree(buffer);
```

#### Example: Object Pool

```c
// BEFORE: Frequent allocation/deallocation
Object *obj = createObject();
// use obj
destroyObject(obj);

// AFTER: Object pool
typedef struct {
    Object   *objects;
    int       capacity;
    int       used;
    pthread_mutex_t mutex;
} ObjectPool;

Object *poolAcquire(ObjectPool *pool) {
    pthread_mutex_lock(&pool->mutex);
    if (pool->used < pool->capacity) {
        Object *obj = &pool->objects[pool->used++];
        pthread_mutex_unlock(&pool->mutex);
        return obj;
    }
    pthread_mutex_unlock(&pool->mutex);
    return NULL;
}

void poolRelease(ObjectPool *pool, Object *obj) {
    pthread_mutex_lock(&pool->mutex);
    // Reset object state
    pool->used--;
    pthread_mutex_unlock(&pool->mutex);
}
```

### Optimization Type 3: Lock Optimization

#### Example: Reduce Lock Scope

```c
// BEFORE: Lock held too long
pthread_mutex_lock(&mutex);
expensiveOperation1();
criticalSection();
expensiveOperation2();
pthread_mutex_unlock(&mutex);

// AFTER: Minimal lock scope
expensiveOperation1();  // no lock needed
pthread_mutex_lock(&mutex);
criticalSection();
pthread_mutex_unlock(&mutex);
expensiveOperation2();  // no lock needed
```

#### Example: Read-Write Lock

```c
// BEFORE: Mutex blocks all readers
pthread_mutex_lock(&mutex);
readData();
pthread_mutex_unlock(&mutex);

// AFTER: Read-write lock allows concurrent reads
pthread_rwlock_rdlock(&rwlock);
readData();
pthread_rwlock_unlock(&rwlock);

// Writers still exclusive
pthread_rwlock_wrlock(&rwlock);
writeData();
pthread_rwlock_unlock(&rwlock);
```

#### Example: Lock-Free Structure

```c
// BEFORE: Lock-protected counter
pthread_mutex_lock(&mutex);
counter++;
pthread_mutex_unlock(&mutex);

// AFTER: Atomic operation
__atomic_add_fetch(&counter, 1, __ATOMIC_SEQ_CST);
// or
atomic_fetch_add(&counter, 1);
```

### Optimization Type 4: Caching

#### Example: Cache Computed Results

```c
// BEFORE: Recompute every time
int getValue() {
    return expensiveComputation();
}

// AFTER: Cache result
typedef struct {
    int   value;
    bool  cached;
    pthread_mutex_t mutex;
} CachedValue;

int getValue(CachedValue *cache) {
    pthread_mutex_lock(&cache->mutex);
    if (!cache->cached) {
        cache->value = expensiveComputation();
        cache->cached = true;
    }
    int result = cache->value;
    pthread_mutex_unlock(&cache->mutex);
    return result;
}
```

#### Example: Memoization

```c
// BEFORE: Recalculate same inputs
int fibonacci(int n) {
    if (n <= 1) return n;
    return fibonacci(n-1) + fibonacci(n-2);
}

// AFTER: Memoization
int fibonacciMemo(int n, int *memo) {
    if (n <= 1) return n;
    if (memo[n] != -1) return memo[n];
    memo[n] = fibonacciMemo(n-1, memo) + fibonacciMemo(n-2, memo);
    return memo[n];
}
```

### Optimization Type 5: String Optimization

#### Example: Avoid Repeated strlen

```c
// BEFORE: strlen in loop condition
for (int i = 0; i < strlen(str); i++) {
    // process str[i]
}

// AFTER: Cache length
int len = strlen(str);
for (int i = 0; i < len; i++) {
    // process str[i]
}
```

#### Example: Use String View

```c
// BEFORE: Copy strings
char *copy = strdup(original);
processString(copy);
free(copy);

// AFTER: Use pointer/view (if no modification needed)
processString(original);  // no copy
```

### Optimization Type 6: I/O Optimization

#### Example: Batch Operations

```c
// BEFORE: Many small writes
for (int i = 0; i < count; i++) {
    write(fd, &data[i], sizeof(data[i]));
}

// AFTER: Batch write
write(fd, data, count * sizeof(data[0]));
```

#### Example: Async I/O

```c
// BEFORE: Synchronous I/O blocks
read(fd, buffer, size);
processData(buffer);

// AFTER: Async I/O (if supported)
struct aiocb cb;
memset(&cb, 0, sizeof(cb));
cb.aio_fildes = fd;
cb.aio_buf = buffer;
cb.aio_nbytes = size;
aio_read(&cb);

// Do other work while I/O in progress
doOtherWork();

// Wait for completion
aio_suspend(&cb, 1, NULL);
processData(buffer);
```

## Step 3: Add Performance Annotations

### Document Optimizations

```c
// Add comments explaining the optimization
/*
 * Performance optimization: Replaced O(n) linear search with O(1) hash lookup
 * Previous implementation: Linear scan through array
 * New implementation: Hash table with pre-computed keys
 * Expected improvement: 60% reduction in lookup time
 * Benchmark: Before 100ms, After 40ms (on 10K items)
 */
```

## Step 4: Compile and Test

### Build with Optimizations

```bash
# Clean build
cd /root/workspace/TDinternal/debug
rm -rf *

# Rebuild
cmake .. -DBUILD_TOOLS=TRUE && make -j 32

# Check for compilation errors
echo $?
```

### Quick Functionality Test

```bash
# Start taosd
pkill -9 taosd
/root/workspace/TDinternal/debug/build/bin/taosd -c /etc/taos &

# Basic smoke test
taos -s "SHOW DATABASES;"

# Run specific test case
cd /root/workspace/TDinternal/community/test
pytest test_specific_case.py -v
```

## Step 5: Verify No Regressions

### Run Unit Tests

```bash
# Run relevant unit tests
cd /root/workspace/TDinternal/debug/build/bin
./tsdbTest
./queryTest
# ... other relevant tests
```

### Run Integration Tests

```bash
# Run pytest suite
cd /root/workspace/TDinternal/community/test
pytest -v -k "test_related_functionality"
```

## Execution Rules

- Make one optimization at a time
- Compile and test after each change
- Keep changes focused and minimal
- Preserve existing functionality
- Add comments explaining optimizations
- Follow existing code style
- Handle error cases properly
- Avoid premature optimization
- Measure before and after (see tsdb-perf-verify)

## Common Pitfalls to Avoid

### Pitfall 1: Breaking Functionality

```c
// WRONG: Optimization breaks edge cases
if (count > 0) {  // forgot count == 0 case
    // optimized code
}

// RIGHT: Handle all cases
if (count == 0) {
    return NULL;
}
// optimized code
```

### Pitfall 2: Memory Leaks

```c
// WRONG: Forgot to free
char *buf = malloc(size);
if (error) {
    return -1;  // leak!
}
free(buf);

// RIGHT: Free on all paths
char *buf = malloc(size);
if (error) {
    free(buf);
    return -1;
}
free(buf);
```

### Pitfall 3: Race Conditions

```c
// WRONG: Check-then-act race
if (cache->valid) {  // not atomic with next line
    return cache->value;
}

// RIGHT: Atomic check and access
pthread_mutex_lock(&cache->mutex);
if (cache->valid) {
    int value = cache->value;
    pthread_mutex_unlock(&cache->mutex);
    return value;
}
pthread_mutex_unlock(&cache->mutex);
```

## Output

This skill should produce:

1. **Modified source files** with optimizations applied
2. **Compilation success** (no build errors)
3. **Test results** (no regressions)
4. **Git commit** with clear description

## Commit Changes

```bash
# Stage changes
git add <modified_files>

# Commit with descriptive message
git commit -m "perf: optimize function_name

- Replace O(n) linear search with O(1) hash lookup
- Reduce memory allocations in hot path
- Expected improvement: 60% reduction in function overhead

Benchmark results will be added after verification."

# Show commit
git show HEAD
```

## Next Steps

After implementing fixes, proceed to:
- **tsdb-perf-verify**: Re-run performance tests to verify improvements
## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-perf-fix version=0.1.0 author=beryl`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->

