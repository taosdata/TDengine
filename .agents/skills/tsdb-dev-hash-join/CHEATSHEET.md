# Hash Join Quick Reference

## Diagnosis Checklist

### Incorrect Results

- [ ] Identify the join type and subtype (inner/left/right/full/semi/anti)
- [ ] Determine which part of the result is wrong:
  - [ ] Matched rows (probe-build pairs)
  - [ ] Non-matched probe rows (LEFT/ANTI/FULL only)
  - [ ] Non-matched build rows (FULL only)
  - [ ] NULL key handling
- [ ] Check the three-phase state machine (PRE/CUR/POST) for outer joins
- [ ] Check midBlk/finBlk logic if pPreFilter is involved
- [ ] Check whether the operator signals done prematurely
- [ ] Reproduce with joinTests.cpp unit tests

### Crash / Memory Corruption

- [ ] Build with `-DBUILD_SANITIZER=true` and reproduce
- [ ] Check page pool bounds (SBufRowInfo offset vs page size)
- [ ] Check hash table linked list integrity (next pointers)
- [ ] Check key buffer sizing for multi-column or variable-length keys
- [ ] Check for use-after-free in hJoinSetDone cleanup

### High Memory Usage

- [ ] Check build-side row count (all rows held in memory)
- [ ] Check page pool growth (10 MB per page)
- [ ] Verify global query memory management is active
- [ ] Consider reducing build-side through plan-level filter pushdown

### Performance Issues

- [ ] Profile build phase vs probe phase time
- [ ] Check for unnecessary memory copies in result construction
- [ ] Check key serialization overhead for multi-column keys
- [ ] Verify hash table sizing (initial capacity = 1.5x estimated build rows)
- [ ] Check block threshold ratio (HJOIN_BLK_THRESHOLD_RATIO = 0.9)

## Key File Locations

```
source/libs/executor/
├── inc/
│   ├── hashjoin.h          # Type definitions, constants, declarations
│   └── join.h              # Shared join primitives
├── src/
│   ├── hashjoin.c          # Per-join-type execution logic
│   └── hashjoinoperator.c  # Operator framework, init, main loop
└── test/
    └── joinTests.cpp       # Unit tests with random data and result verification
```

## Build Commands

### Linux

```bash
cd TDinternal && mkdir -p debug && cd debug

# Full build with sanitizer and tests
cmake .. -DBUILD_SANITIZER=true -DBUILD_TOOLS=true -DGRANT_VALUE=365 -DBUILD_TEST=true
make -j8 && make install

# Build without sanitizer (faster)
cmake .. -DBUILD_TOOLS=true -DGRANT_VALUE=365 -DBUILD_TEST=true
make -j8 && make install

# Minimal build
cmake .. -DGRANT_VALUE=365
make -j8 && make install
```

### Windows

```bat
"C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat" x64
cmake .. -G "NMake Makefiles JOM" -DBUILD_TEST=true -DBUILD_TOOLS=true -DCMAKE_BUILD_TYPE=Debug
jom -j 4
```

## Join Type Behavior Quick Reference

| Behavior | INNER | LEFT/RIGHT OUTER | SEMI | ANTI | FULL OUTER |
|----------|-------|-------------------|------|------|------------|
| Emit matched rows | Yes | Yes | One per probe | No | Yes |
| Emit unmatched probe | No | Yes (NULL build) | No | Yes | Yes (NULL build) |
| Emit unmatched build | No | No | No | No | Yes (NULL probe) |
| PRE/POST phases | No | Yes | No | Yes | Yes |
| Hash value type | SGroupData | SGroupData | SGroupData | SGroupData | SFGroupData |
| NULL key in build | Skip | Skip | Skip | Skip | Emit immediately |
| NULL key in probe | Skip | Emit NMatch | Skip | Emit NMatch | Emit NMatch |
| grpSingleRow opt | No | No | Yes* | Yes* | No |

*Only when pFullOnCond is absent.

## Error Handling Pattern

```c
// Immediate return on error
HJ_ERR_RET(someFunction());

// Goto cleanup on error
HJ_ERR_JRET(someFunction());
// ...
_return:
  // cleanup code
  return code;
```

## Implementation Status

| Component | Status |
|-----------|--------|
| INNER JOIN operator | Complete |
| LEFT/RIGHT OUTER operator | Complete |
| SEMI JOIN operator | Complete |
| ANTI JOIN operator | Complete |
| FULL OUTER JOIN operator | Mostly complete (remaining work in progress) |
| Plan layer (all types) | Not yet implemented |
| Unit tests (joinTests.cpp) | Implemented; not all tests enabled yet |
| Full pipeline integration | Blocked by plan layer |
