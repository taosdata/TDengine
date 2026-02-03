# CMake Integration for Conan Packages

## Overview

This document describes the integration of Conan packages into TDinternal's CMake build system.

## Updated Files

### cmake/conan.cmake

This file has been updated to support the newly created Conan packages:
- **cppstub** (v1.0.0) - Header-only testing stub library
- **fast-lzma2** (v1.0.1) - Already configured

## Changes Made

### 1. Added cppstub Package Discovery

**Location**: Lines 33-34

```cmake
# Testing
if(${BUILD_TEST})
    find_package(GTest REQUIRED)
    find_package(cppstub REQUIRED)  # Header-only stub library for unit tests
endif()
```

This ensures that when `BUILD_TEST` is enabled, CMake will find and load the cppstub Conan package.

### 2. Updated DEP_ext_cppstub Macros

**Location**: Lines 447-463

#### DEP_ext_cppstub (Main Macro)
```cmake
macro(DEP_ext_cppstub tgt)
    # cppstub is now available as a Conan package (header-only library)
    if(TARGET cppstub::cppstub)
        target_link_libraries(${tgt} PUBLIC cppstub::cppstub)
    endif()
endmacro()
```

#### DEP_ext_cppstub_INC (Include-only)
```cmake
macro(DEP_ext_cppstub_INC tgt)
    # Header-only library, handled by target_link_libraries above
    if(TARGET cppstub::cppstub)
        target_link_libraries(${tgt} INTERFACE cppstub::cppstub)
    endif()
endmacro()
```

#### DEP_ext_cppstub_LIB (Library-only)
```cmake
macro(DEP_ext_cppstub_LIB tgt)
    # Header-only library, no libs to link
endmacro()
```

### 3. Updated Documentation Comments

**Location**: Lines 355-358

Added to the list of successfully migrated dependencies:

```cmake
# Successfully migrated to Conan:
# - cppstub (testing stub library) - now available as Conan package
# - fast-lzma2 - now available as Conan package
```

## How It Works

### For Test Targets

When a test target uses `DEP_ext_cppstub(my_test_target)`, the macro will:

1. Check if the `cppstub::cppstub` CMake target exists
2. If it exists, link it to the target using `target_link_libraries`
3. Since it's header-only, this only adds include directories (no actual linking)

### Example Usage in CMakeLists.txt

```cmake
# In source/util/test/CMakeLists.txt
add_executable(memPoolTest "memPoolTest.cpp")
DEP_ext_gtest(memPoolTest)
DEP_ext_cppstub(memPoolTest)  # Now properly links cppstub headers
target_link_libraries(memPoolTest PRIVATE os util common)
```

## Benefits

1. **Automatic Include Paths**: Headers from cppstub are automatically available
2. **Platform-Specific**: Correct `addr_any.h` is selected based on OS
3. **No Manual Configuration**: CMake handles all the details
4. **Conditional**: Only loads when BUILD_TEST is enabled
5. **Safe Fallback**: Uses `if(TARGET ...)` checks to avoid errors

## Backward Compatibility

The macros maintain the same interface as before:
- `DEP_ext_cppstub(target)` - Main dependency injection
- `DEP_ext_cppstub_INC(target)` - Include paths only
- `DEP_ext_cppstub_LIB(target)` - Library linking (no-op for header-only)

Existing code that uses these macros will work without modification.

## Testing the Integration

### 1. Verify Package is Available

```bash
conan list "cppstub/*"
```

Expected output:
```
Local Cache
  cppstub
    cppstub/1.0.0
```

### 2. Configure CMake

```bash
cd build
cmake .. -DBUILD_TEST=true
```

You should see:
```
-- Loading Conan dependencies...
-- Found cppstub: 1.0.0
-- All Conan dependencies loaded successfully
```

### 3. Build a Test Target

```bash
cmake --build . --target memPoolTest
```

The build should succeed without "addr_any.h: No such file or directory" errors.

## Migration Status

### ✅ Completed
- cppstub (header-only)
- fast-lzma2 (compiled library)

### ⏳ Pending
- Other dependencies listed in cmake/conan.cmake comments

## Troubleshooting

### Error: "Could not find cppstub"

**Solution**: Create the cppstub Conan package:
```bash
cd conan/cppstub
conan create . --build=missing
```

### Error: "addr_any.h: No such file or directory"

**Possible causes**:
1. cppstub package not installed
2. `DEP_ext_cppstub` not called for the target
3. `BUILD_TEST` not enabled

**Solution**: Verify all three conditions are met.

### Error: "Target cppstub::cppstub not found"

This is normal if cppstub is not available. The `if(TARGET ...)` check prevents build failures.

## Next Steps

To fully integrate cppstub into the project:

1. ✅ Update cmake/conan.cmake (Done)
2. ⏳ Uncomment `#include <addr_any.h>` in test files
3. ⏳ Test building all test targets
4. ⏳ Add cppstub to CI/CD pipeline

## References

- [cppstub README](cppstub/README.md)
- [cppstub SUMMARY](cppstub/SUMMARY.md)
- [Conan CMakeDeps Generator](https://docs.conan.io/en/latest/reference/conanfile/tools/cmake/cmakedeps.html)
- [CMake target_link_libraries](https://cmake.org/cmake/help/latest/command/target_link_libraries.html)

## Related Files

- `conan/cppstub/conanfile.py` - Conan recipe
- `cmake/conan.cmake` - CMake integration
- `source/util/test/CMakeLists.txt` - Example test target
- `source/util/test/memPoolTest.cpp` - Example test file using addr_any.h
