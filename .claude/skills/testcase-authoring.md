# Test Case Authoring Guide

## Description

Guidelines for writing efficient, reliable TDengine test cases. Emphasizes DRY principle: reuse existing helpers, abstract repetitive patterns.

---

## 1. Optimization Principles

### 1.1 Preserve Original Behavior
**Critical**: Performance optimizations must not change the original logic or behavior.

```python
# Original: Single INSERT for atomicity
INSERT INTO tb VALUES (...) tb1 VALUES (...)

# Wrong: Splitting changes atomicity
INSERT INTO tb VALUES (...)
INSERT INTO tb1 VALUES (...)
```

### 1.2 DRY: Don't Repeat Yourself
Before writing a new pattern, check if the framework already provides a helper. Abstract repetitive logic into reusable functions.

### Common Framework Helpers

| Pattern | Helper Method | Location |
|---------|---------------|----------|
| Wait for query rows | `tdSql.checkRowsLoop(expected, sql, retries, wait)` | sql.py |
| Wait for dnodes ready | `clusterCommonCheck.checkDnodes(dnodeNum)` | clusterCommonCheck.py |
| Prepare test database | `tdSql.prepare()` | sql.py |

### Examples

**Good**: Use existing helper
```python
# One line, clear intent, proper timeout handling
tdSql.checkRowsLoop(1, "show mounts", loopCount=30, waitTime=1)
```

**Avoid**: Reinvent the wheel
```python
# 6 lines, manual retry, no explicit timeout error
for i in range(30):
    tdSql.query("show mounts")
    if tdSql.queryRows == 1:
        break
    time.sleep(1)
```

---

## 2. Timeout Handling

### 2.1 Use Framework Helpers (Preferred)

```python
# Wait for specific row count
tdSql.checkRowsLoop(expectedRows=3, sql="show dnodes", loopCount=30, waitTime=1)
```

### 2.2 Custom Loop (When Helper Not Available)

Set `queryTimes=1` to prevent nested timeout multiplication:

```python
def wait_for_custom_condition(self, timeout=100):
    count = 0
    while count < timeout:
        result = tdSql.query("SELECT ...", exit=False, queryTimes=1)
        if condition_met(result):
            return True
        time.sleep(1)
        count += 1
    raise Exception(f"Timeout after {timeout}s")
```

### Why queryTimes=1?

- `tdSql.query()` defaults to 10 internal retries (1s each)
- Outer loop × inner retry = 10× longer timeout than expected

---

## 3. Batch Insert

### Rule
Use batch insert when inserting >100 rows. Choose batch size based on row size to stay under maxSQLLength=4MB limit.

### Batch Size Guidelines

| Table Type | Row Size | Batch Size | SQL Size |
|------------|----------|------------|----------|
| Simple (2-3 cols) | ~50 bytes | 200 | ~10KB |
| Normal (5-10 cols) | ~100 bytes | 100 | ~10KB |
| Wide (>100 cols) | ~2KB+ | 20 | ~40KB |

### Pattern

```python
def insert_data_batch(self, table, rows):
    batch_size = 100
    values = []
    
    for i, row in enumerate(rows):
        values.append(f"({row.ts}, {row.value})")
        if len(values) >= batch_size:
            tdSql.execute(f"INSERT INTO {table} VALUES {','.join(values)}")
            values = []
    
    if values:  # Remaining rows
        tdSql.execute(f"INSERT INTO {table} VALUES {','.join(values)}")
```

### Important: Check for Empty Lists

Always check if values list is non-empty before executing to avoid SQL syntax errors:

```python
# Good: Check before execute
if values_tb and values_tb1:
    tdSql.execute(f"INSERT INTO tb VALUES {','.join(values_tb)} tb1 VALUES {','.join(values_tb1)}")

# Avoid: Empty list causes syntax error
tdSql.execute(f"INSERT INTO tb VALUES {','.join(values_tb)}")  # Error if values_tb is []
```

---

## 4. Code Quality

### 4.1 Abstract Repetitive Patterns

If you find yourself writing the same pattern 3+ times, abstract it:

```python
# Good: Abstracted helper
def insert_with_pattern(self, table, num_rows, pattern_fn):
    """Insert rows with a custom pattern function."""
    batch_size = 100
    for batch_start in range(0, num_rows, batch_size):
        batch_end = min(batch_start + batch_size, num_rows)
        values = [pattern_fn(i) for i in range(batch_start, batch_end)]
        tdSql.execute(f"INSERT INTO {table} VALUES {','.join(values)}")

# Usage
self.insert_with_pattern("tb1", 10000, lambda i: f"({ts + i}, null, null)")
self.insert_with_pattern("tb2", 10000, lambda i: f"({ts + i}, 1, 1)")
```

### 4.2 Use Integer Division

```python
# Good
half = num_rows // 2
if i < half:

# Avoid
if i < num_rows / 2:  # Float result
```

### 4.3 Name Magic Numbers

```python
# Good
TS_BASE = 1626624000000  # 2021-07-18 16:00:00 UTC

# Avoid
f"({1626624000000 + i}, {i})"  # What does this mean?
```

### 4.4 Accurate Comments

```python
# Good
# tb2: non-null numeric/bool columns, null string columns (c7, c8)

# Avoid (misleading)
# tb2: all non-null values  # Actually c7, c8 are null
```

---

## 5. Data Volume Control

### Rule
Parameterize data volume. **Warning: Changing data volume requires updating all related assertions.**

```python
def setup_test_data(self, database, num_random=100):
    """Create test data with controllable volume.
    
    WARNING: If you change num_random, you MUST also update:
    - checkRows() calls
    - checkData() calls that depend on row count
    - Any other assertions based on data volume
    """
    for i in range(num_random):
        generate_row(i)
```

### Important
Changing `num_random` without updating assertions will cause test failures:
```python
# If num_random=50 but assertion still expects 100:
tdSql.checkRows(100)  # FAIL: actual rows = 50
```

---

## Quick Checklist

- [ ] Optimization preserves original behavior?
- [ ] Tested edge cases (empty, single, batch boundary)?
- [ ] Pattern exists in framework? → Use existing helper
- [ ] Same pattern used 3+ times? → Abstract into function
- [ ] Custom wait loop? → `queryTimes=1` + explicit timeout error
- [ ] >100 INSERTs? → Batch insert (size 100/20) + empty check
- [ ] Integer division? → Use `//`
- [ ] Magic numbers? → Named constants
- [ ] Test data volume? → Parameterized (note: changing requires updating assertions)
