# AGENTS.md - TDengine Development Guide for Agentic Coding Agents

## Build & Test Commands

### Quick Start
```bash
./build.sh first-try      # First time setup
./build.sh gen             # Generate build files
./build.sh bld             # Build
./build.sh install         # Install
./build.sh start           # Start services
./build.sh test            # Run tests
./build.sh stop            # Stop services
```

### Single Unit Test
```bash
# Build: cd debug && cmake .. -DBUILD_TEST=true -DBUILD_TOOLS=true -DBUILD_CONTRIB=true && make
cd debug/build/bin
./osTimeTests                                    # Run all tests
./osTimeTests --gtest_filter=TestGroup.TestCase  # Run specific test
./osTimeTests --gtest_verbose                    # Verbose output
```

### Single System Test
```bash
cd tests/system-test
python3 ./test.py -f 2-query/avg.py    # Run test file
python3 ./test.py -f 2-query/avg.py -v  # Verbose
timeout 300 python3 ./test.py -f 2-query/avg.py  # Timeout
```

### Test Organization
- **Unit**: `tests/unit-test/` - Google Test framework, `{Module}Tests.cpp` (e.g., `osTimeTests.cpp`)
- **System**: `tests/system-test/` - Python, `{category}/{name}.py` (e.g., `2-query/avg.py`)
- **Legacy**: `tests/script/` - TSIM framework, `.sim` files

### CMake Options
```bash
cmake .. -DBUILD_TOOLS=true -DBUILD_CONTRIB=true -DBUILD_TEST=true  # Full dev
cmake .. -DBUILD_TOOLS=true -DBUILD_CONTRIB=true -DBUILD_TEST=false # Faster
```

---

## Code Style

### Formatting
- **Tool**: `clang-format-14`, run `./tools/scripts/codeFormat.sh`
- **Indent**: 2 spaces, no tabs
- **Line limit**: 120 chars
- **Braces**: Same line as declaration
- **Pointers**: `char *ptr` (right-aligned)

### Naming
- **Functions**: `prefixCamelCase` (e.g., `taosMemoryFree`, `taosInitLog`)
- **Globals**: `ts` prefix + snake_case (e.g., `tsServerPort`)
- **Locals**: snake_case or camelCase (e.g., `int32_t i`, `char *z`)
- **Types**: `S` prefix for structs (e.g., `STimeWindow`)
- **Constants**: ALL_CAPS (e.g., `TSDB_EP_LEN`)
- **Types**: `int32_t`, `uint64_t` from `<stdint.h>`

### Imports
1. System includes: `#include <stdio.h>`
2. Project headers: `#include "tutil.h"`
3. Group logically

### Patterns
```c
// Return code
if (code != 0) return code;

// Goto cleanup
if (code != 0) goto _OVER;
_OVER:
  return code;

// Memory
ptr = taosMemoryCalloc(size, 1);
taosMemoryFree(ptr);

// Function call return value checking (MANDATORY)
int32_t code = someFunction();
if (code != 0) {
  // Handle error - log, cleanup, or return
  return code;
}

// Nested function calls with error handling
int32_t code = function1();
if (code != 0) return code;

code = function2();
if (code != 0) goto _OVER;

// Never ignore return values
someFunction();  // WRONG - return value ignored
int32_t code = someFunction();  // CORRECT - check return value
```

### Return Value Handling (CRITICAL)

**MANDATORY REQUIREMENT**: Every function call MUST have its return value checked and handled appropriately.

#### Error Handling Patterns:

1. **Immediate Return**: For simple error cases
   ```c
   int32_t code = taosHashPut(pHash, key, keyLen, pData, dataLen);
   if (code != 0) return code;
   ```

2. **Goto Cleanup**: For complex functions with resources to clean up
   ```c
   int32_t code = taosMemoryMalloc(&ptr, size);
   if (code != 0) goto _ERROR;
   
   code = processData(ptr);
   if (code != 0) goto _ERROR;
   
   _ERROR:
     if (ptr) taosMemoryFree(ptr);
     return code;
   ```

3. **Error Propagation**: Preserve original error codes
   ```c
   int32_t code = validateInput(pInput);
   if (code != 0) {
     tError("Input validation failed: 0x%x", code);
     return code;  // Preserve original error
   }
   ```

#### Common TDengine Error Codes:
- `TSDB_CODE_SUCCESS` (0) - Success
- `TSDB_CODE_INVALID_PARA` - Invalid parameters
- `TSDB_CODE_OUT_OF_MEMORY` - Memory allocation failed
- `TSDB_CODE_SUCCESS` (0) - Success

**IMPORTANT**: Never use `(void)someFunction()` or ignore return values. Every function call that returns an error code must be checked.

### Memory Safety (CRITICAL)

**FORBIDDEN FUNCTIONS**: The following memory-unsafe functions are strictly PROHIBITED in TDengine codebase:

#### String Functions (NEVER USE):
- `strcpy()` - Use `taosStrncpy()` or `snprintf()` instead
- `strcat()` - Use `taosStrncat()` or `snprintf()` instead  
- `sprintf()` - Use `snprintf()` instead
- `gets()` - Use `fgets()` instead
- `scanf()` - Use `fscanf()` with proper bounds checking

#### Memory Functions (NEVER USE):
- `memcpy()` without size validation - Use proper bounds checking
- `memmove()` without size validation - Use proper bounds checking
- `memset()` without size validation - Use proper bounds checking

#### Format String Functions (NEVER USE):
- Any function taking user input as format string
- `printf(userInput)` - Use `printf("%s", userInput)` instead

#### Required Safe Alternatives:
```c
// CORRECT: Safe string copying
taosStrncpy(dest, src, sizeof(dest) - 1);
dest[sizeof(dest) - 1] = '\0';  // Ensure null termination

// CORRECT: Safe string concatenation
taosStrncat(dest, src, sizeof(dest) - strlen(dest) - 1);

// CORRECT: Safe formatted output
snprintf(buffer, sizeof(buffer), "Value: %d", value);

// CORRECT: Safe user input
fgets(input, sizeof(input), stdin);
input[strcspn(input, "\n")] = '\0';  // Remove newline
```

#### Memory Allocation Safety:
```c
// CORRECT: Always check allocation results
char *ptr = taosMemoryMalloc(size);
if (ptr == NULL) {
  return TSDB_CODE_OUT_OF_MEMORY;
}

// CORRECT: Use calloc for zero initialization
char *ptr = taosMemoryCalloc(count, size);
if (ptr == NULL) {
  return TSDB_CODE_OUT_OF_MEMORY;
}
```

**MANDATORY REQUIREMENT**: All code must pass security scanning for buffer overflows and memory safety violations. Use TDengine's memory wrapper functions (`taosMemory*`, `taosStr*`) instead of standard library equivalents.

---

## Repository Structure

- `source/` - Core code (taosd, client libs, storage)
- `include/` - Public headers
- `tests/` - Test suites (unit, system, integration, performance)
- `tools/` - Dev tools (taosBenchmark, taosdump, TDgpt)
- `examples/` - Sample code (multiple languages)
- `docs/` - Documentation

**Languages**: C/C++ (core), Go 1.23+ (taosAdapter/taosKeeper), Python (tools, analytics)

---

## Development Workflow

### Branches
- `main` - Stable releases (no direct patches)
- `3.0` - Development branch (all changes)
- Feature branches - From `3.0`, prefix `docs/` for docs

### Pre-commit
1. `./build.sh bld` - Build succeeds
2. Run affected unit + system tests
3. `./tools/scripts/codeFormat.sh` - Format code
4. `lsp_diagnostics` - Check changed files

---

## Testing Requirements

- **Indentation**: 2 spaces
- **Braces**: Same line as function declaration
- **Line limit**: 120 characters
- **Pointers**: Right-aligned (`char *ptr`)
