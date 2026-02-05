# cppstub Conan Package Migration Summary

## Overview

Successfully created a Conan package for cpp-stub (cppstub), a C++ testing stub library.

## Package Details

- **Package Name**: cppstub
- **Version**: 1.0.0
- **Type**: Header-only library
- **Source**: [https://github.com/coolxv/cpp-stub](https://github.com/coolxv/cpp-stub)
- **Commit**: 3137465194014d66a8402941e80d2bccc6346f51

## Files Structure

```text
conan/cppstub/
├── conanfile.py           # Conan recipe
├── README.md             # Package documentation
├── SUMMARY.md            # This file
├── cppstub/              # Source files
│   ├── LICENSE           # MIT License
│   ├── src/
│   │   └── stub.h        # Main stub header
│   ├── src_linux/
│   │   └── addr_any.h    # Linux-specific header
│   ├── src_darwin/
│   │   └── addr_any.h    # macOS-specific header
│   └── src_win/
│       └── addr_any.h    # Windows-specific header
└── test_package/         # Test package
    ├── conanfile.py
    ├── CMakeLists.txt
    └── test_package.cpp
```

## Package Features

1. **Header-Only**: No compilation or linking required
2. **Platform-Specific**: Automatically selects correct `addr_any.h` for your OS
3. **Tested**: Includes test package that verifies headers are accessible
4. **Cross-Platform**: Supports Linux, macOS, and Windows

## Installation

```bash
cd /home/huolinhe/Projects/taosdata/TDinternal/community/conan/cppstub
conan create . --build=missing
```

## Verification

```bash
conan list "cppstub/*"
```

Expected output:

```text
Local Cache
  cppstub
    cppstub/1.0.0
```

## Usage in Projects

### In conanfile.txt

```ini
[requires]
cppstub/1.0.0

[generators]
CMakeDeps
CMakeToolchain
```

### In CMakeLists.txt

```cmake
find_package(cppstub REQUIRED CONFIG)
target_link_libraries(your_target cppstub::cppstub)
```

### In C++ code

```cpp
#include <stub.h>
#include <addr_any.h>
```

## Next Steps

To integrate this package into the TDinternal build system:

### 1. Update `cmake/conan.cmake`

Replace the empty `DEP_ext_cppstub` macro (around line 443):

```cmake
macro(DEP_ext_cppstub tgt)
    if(TARGET cppstub::cppstub)
        target_link_libraries(${tgt} PUBLIC cppstub::cppstub)
    endif()
endmacro()
```

### 2. Add to conanfile.txt/conanfile.py

Add cppstub as a dependency in the main project's Conan configuration:

```python
def requirements(self):
    if self.options.get_safe("build_tests"):
        self.requires("cppstub/1.0.0")
```

### 3. Restore addr_any.h includes

Once the above steps are done, uncomment the `#include <addr_any.h>` lines in:

- source/util/test/memPoolTest.cpp
- source/libs/catalog/test/catalogTests.cpp
- source/libs/executor/test/joinTests.cpp
- source/libs/executor/test/queryPlanTests.cpp
- source/libs/parser/test/mockCatalog.cpp
- source/libs/qworker/test/qworkerTests.cpp
- source/libs/scalar/test/filter/filterTests.cpp
- source/libs/scalar/test/scalar/scalarTests.cpp
- source/libs/scheduler/test/schedulerTests.cpp

## Test Results

✅ Package created successfully
✅ Test package built and ran successfully
✅ Headers are accessible
✅ Package installed in local Conan cache

Output from test run:

```text
Testing cppstub package...
Normal add(2, 3) = 5
stub.h header is available
addr_any.h header is available
All tests passed!
```

## Comparison with fast-lzma2

Similar to fast-lzma2 package:

- ✅ Uses export_sources to include source files
- ✅ Includes test_package for verification
- ✅ Has comprehensive README
- ✅ Platform-independent (cppstub is header-only, even simpler)

Differences:

- cppstub is header-only (no build step)
- Platform-specific file selection at package time
- Lighter weight package

## Notes

- The package uses `no_copy_source = True` for efficiency
- `package_id()` clears the info since it's header-only (platform-independent)
- Source files downloaded from GitHub raw URLs using wget
- Test package only verifies header availability (doesn't test full stub functionality)
