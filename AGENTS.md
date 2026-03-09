# AGENTS.md - TDengine Codebase Guide

This document provides essential information for AI coding agents working in the TDengine repository.

## Project Overview

TDengine is an open-source, high-performance, cloud-native time-series database designed for IoT, Connected Cars, and Industrial IoT applications. It is written primarily in C and C++.

## Build Commands

### Prerequisites
- GCC 9.3.1+ and CMake 3.18.0+ required
- For taosAdapter/taosKeeper: Go 1.23+

### Quick Build
```bash
# Full build with tools and contrib
./build.sh

# Equivalent to:
mkdir debug && cd debug
cmake .. -DBUILD_TOOLS=true -DBUILD_CONTRIB=true
make
```

### Build Options
```bash
# Build with tests enabled (required for running unit tests)
cmake .. -DBUILD_TOOLS=true -DBUILD_TEST=true -DBUILD_CONTRIB=true

# Build with taosAdapter
cmake .. -DBUILD_HTTP=false

# Build with taosKeeper
cmake .. -DBUILD_KEEPER=true

# Build with Jemalloc
cmake .. -DJEMALLOC_ENABLED=ON
```

### Installation
```bash
sudo make install
```

## Test Commands

### Unit Tests (Google Test Framework)

```bash
# Run a single unit test
cd debug/build/bin
./osTimeTests

# Run specific test binary (examples)
./heapTest
./arrayTest
./cfgTest
./bloomFilterTest
./utilTests

# Run all unit tests
cd tests/unit-test/
bash test.sh -e 0
```

### System Tests (Python Framework)

```bash
# Run a single system test
cd tests/system-test
python3 ./test.py -f 2-query/avg.py

# Run all system tests
cd tests
./run_all_ci_cases.sh -t python
```

### Legacy Tests (TSIM Framework - Deprecated)

```bash
# Run a single legacy test
cd tests/script
./test.sh -f tsim/db/basic1.sim

# Run all legacy tests
cd tests
./run_all_ci_cases.sh -t legacy
```

### CI Tests (All Types)

```bash
# Run all CI tests on main branch
cd tests
./run_all_ci_cases.sh -b main
```

### Adding New Tests

For unit tests:
1. Create `.cpp` test file in module's `test/` directory
2. Update `CMakeLists.txt` to include the test
3. Use `add_test()` CMake command to register with CTest
4. Rebuild with `-DBUILD_TEST=true`

## Code Style Guidelines

### Formatting (.clang-format)

The project uses clang-format based on Google style with these settings:

- **Indentation**: 2 spaces, no tabs
- **Column Limit**: 120 characters
- **Pointer Alignment**: Right (`int* ptr`, `void* p`)
- **Braces**: Attach style (opening brace on same line)
- **Includes**: Sorted automatically, system includes first
- **Functions**: Short functions allowed on single line

Always run clang-format before committing:
```bash
# Format a file in-place
clang-format --style=file -i <file>

# Check formatting without modifying
clang-format --style=file --dry-run <file>
```

### Naming Conventions

| Type | Convention | Examples |
|------|------------|----------|
| Functions | snake_case (preferred in C), camelCase in some modules | `taos_options`, `createParseContext`, `parseBinaryUInteger` |
| Variables | snake_case | `haystack`, `hlen`, `p_request`, `is_neg` |
| Structs/Types | PascalCase with 'S' prefix | `SRefNode`, `SPatternCompareInfo`, `SRefSet` |
| Macros/Constants | UPPER_SNAKE_CASE | `TSDB_CODE_SUCCESS`, `TSDB_REF_STATE_EMPTY` |
| Test Names | PascalCase | `TEST(utilTest, wchar_pattern_match_test)` |

### Header File Conventions

```c
// Standard header guard pattern
#ifndef _TD_UTIL_TAOS_ERROR_H_
#define _TD_UTIL_TAOS_ERROR_H_

// C++ compatibility
#ifdef __cplusplus
extern "C" {
#endif

// System includes first
#include <stdint.h>

// Local includes
#include "tlog.h"

#ifdef __cplusplus
}
#endif

#endif /*_TD_UTIL_TAOS_ERROR_H_*/
```

### File Headers

All source files must include the AGPL license header:
```c
/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 */
```

## Error Handling

TDengine uses a return-code based error handling system. **No exceptions** are used.

### Error Code Macros

```c
// Success/failure codes
#define TSDB_CODE_SUCCESS                   0
#define TSDB_CODE_FAILED                    -1

// Define custom error codes
#define TAOS_DEF_ERROR_CODE(mod, code) ((int32_t)((0x80000000 | ((mod)<<16) | (code))))

// Thread-local error access
#define terrno    (*taosGetErrno())
#define terrMsg   (taosGetErrMsg())

// Check error status
#define TAOS_SUCCEEDED(err)  ((err) >= 0)
#define TAOS_FAILED(err)     ((err) < 0)
```

### Error Handling Pattern

```c
// Typical pattern - return error code on failure
int32_t someFunction(void* param) {
  if (param == NULL) {
    terrno = TSDB_CODE_INVALID_PARA;
    return terrno;
  }
  
  // Allocate memory
  void* ptr = taosMemoryCalloc(1, sizeof(SMyStruct));
  if (ptr == NULL) {
    return terrno;  // terrno already set by taosMemoryCalloc
  }
  
  return TSDB_CODE_SUCCESS;
}

// Caller pattern
int32_t code = someFunction(param);
if (code != TSDB_CODE_SUCCESS) {
  // Handle error - terrno contains the error code
  qError("failed to execute function, error: %s", terrstr());
  return code;
}
```

## Project Structure

```
TDengine/
├── source/           # Main source code
│   ├── util/         # Utility library
│   ├── common/       # Common components
│   ├── libs/         # Core libraries (parser, executor, etc.)
│   ├── client/       # Client library
│   └── dnode/        # Data node implementation
├── include/          # Public headers
│   ├── util/         # Utility headers (taoserror.h, tutil.h, etc.)
│   └── client/       # Client API headers
├── contrib/          # Third-party dependencies
├── tests/            # Test suites
│   ├── unit-test/    # Unit tests
│   ├── system-test/  # System tests (Python)
│   └── script/       # Legacy TSIM tests
├── tools/            # Tools (taosBenchmark, taosdump, etc.)
├── packaging/        # Packaging scripts
└── cmake/            # CMake configuration
```

## Running TDengine

```bash
# Start as service (Linux)
sudo systemctl start taosd

# Start as service (macOS)
sudo launchctl start com.tdengine.taosd

# Start from build directory (development)
./build/bin/taosd -c test/cfg

# Connect using CLI
taos
# Or from build:
./build/bin/taos -c test/cfg
```

## Contribution Guidelines

1. Fork repository and create branch from `3.0` (development branch), NOT `main`
2. For documentation changes, prefix branch with `docs/`
3. Submit pull requests to the `3.0` branch
4. Sign the CLA at https://cla-assistant.io/taosdata/TDengine

## Important Notes

- The `main` branch is for stable releases - do not submit PRs directly to `main`
- Run `clang-format` before committing code changes
- Unit tests use Google Test framework
- System tests use a custom Python framework
- Legacy TSIM tests are deprecated - prefer Python system tests for new cases
