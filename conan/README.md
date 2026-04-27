# TDinternal Conan Packages

This directory contains Conan package recipes for TDinternal dependencies.

## Available Packages

### 1. fast-lzma2 (v1.0.1)

Fast LZMA2 compression library

- **Type**: Compiled library (static/shared)
- **Build System**: Makefile
- **Location**: `fast-lzma2/`
- **Status**: ✅ Complete and tested

Key Features:

- Optimized LZMA2 compression algorithm
- Support for both static and shared libraries
- Cross-platform (Linux, macOS, Windows)
- x86_64 assembly optimizations

Usage:

```cmake
find_package(fast-lzma2 REQUIRED CONFIG)
target_link_libraries(your_target fast-lzma2::fast-lzma2)
```

### 2. cppstub (v1.0.0)

C++ stub/mock library for unit testing

- **Type**: Header-only library
- **Build System**: None (header-only)
- **Location**: `cppstub/`
- **Status**: ✅ Complete and tested

Key Features:

- Header-only (no compilation needed)
- Platform-specific address manipulation
- Simple stubbing API for unit tests
- Cross-platform support

Usage:

```cmake
find_package(cppstub REQUIRED CONFIG)
target_link_libraries(your_target cppstub::cppstub)
```

## Package Comparison

| Feature | fast-lzma2 | cppstub |
|---------|------------|---------|
| Type | Compiled Library | Header-Only |
| Build System | Makefile | None |
| Platform Specific | Yes (binary) | Yes (headers) |
| Link Required | Yes | No |
| Use Case | Compression | Testing/Mocking |
| Package Size | Medium | Small |

## Installation

Install both packages:

```bash
# Install fast-lzma2
cd fast-lzma2
conan create . --build=missing

# Install cppstub
cd ../cppstub
conan create . --build=missing
```

Verify installation:

```bash
conan list "fast-lzma2/*"
conan list "cppstub/*"
```

## Integration with TDinternal

To use these packages in the TDinternal project:

### 1. Add to CMake configuration

Update `cmake/conan.cmake` to include proper macros:

```cmake
# For fast-lzma2
macro(DEP_ext_fast_lzma2 tgt)
    if(TARGET fast-lzma2::fast-lzma2)
        target_link_libraries(${tgt} PUBLIC fast-lzma2::fast-lzma2)
    endif()
endmacro()

# For cppstub
macro(DEP_ext_cppstub tgt)
    if(TARGET cppstub::cppstub)
        target_link_libraries(${tgt} PUBLIC cppstub::cppstub)
    endif()
endmacro()
```

### 2. Add to Conan dependencies

In your main `conanfile.py` or `conanfile.txt`:

```python
def requirements(self):
    # Production dependency
    self.requires("fast-lzma2/1.0.1")
    
    # Test dependency
    if self.options.get_safe("build_tests"):
        self.requires("cppstub/1.0.0")
```

### 3. Use in your code

```cpp
// For compression
#include "fast-lzma2.h"

// For testing
#include <stub.h>
#include <addr_any.h>
```

## Directory Structure

```text
conan/
├── README.md              # This file
├── fast-lzma2/           # Fast LZMA2 package
│   ├── conanfile.py
│   ├── README.md
│   ├── USAGE.md
│   ├── fast-lzma2/       # Source files
│   └── test_package/
└── cppstub/              # C++ Stub package
    ├── conanfile.py
    ├── README.md
    ├── SUMMARY.md
    ├── cppstub/          # Source files
    └── test_package/
```

## Building Packages

### fast-lzma2

```bash
cd fast-lzma2
conan create . --build=missing

# Build shared library version
conan create . --build=missing -o fast-lzma2/*:shared=True

# Build Debug version
conan create . --build=missing -s build_type=Debug
```

### cppstub

```bash
cd cppstub
conan create . --build=missing

# Note: cppstub is header-only, so build options don't affect it
```

## Testing

Both packages include test_package directories that verify:

- Headers are accessible
- Libraries link correctly (fast-lzma2)
- Basic functionality works

Run tests:

```bash
# Automatically run during conan create
conan create . --build=missing
```

## Migration from External CMake Dependencies

These packages replace the following external dependencies in `cmake/external.cmake`:

- `ext_fast_lzma2` → Now using Conan package `fast-lzma2/1.0.1`
- `ext_cppstub` → Now using Conan package `cppstub/1.0.0`

## Benefits of Conan Migration

1. **Reproducible Builds**: Fixed versions across all environments
2. **Faster Setup**: Pre-built binaries when available
3. **Easier Maintenance**: Centralized package management
4. **Cross-Platform**: Consistent behavior across Linux, macOS, Windows
5. **Dependency Management**: Automatic resolution of transitive dependencies

## Next Steps

Other dependencies that could be migrated to Conan:

- lz4
- cJSON
- googletest (gtest)
- zlib
- xz
- And more...

## Documentation

- [fast-lzma2 README](fast-lzma2/README.md)
- [fast-lzma2 USAGE Guide](fast-lzma2/USAGE.md)
- [cppstub README](cppstub/README.md)
- [cppstub SUMMARY](cppstub/SUMMARY.md)

## Troubleshooting

### Package not found

If Conan can't find the package:

```bash
# Check if package is installed
conan list "package-name/*"

# If not, create it
cd package-name
conan create . --build=missing
```

### Build errors

If you encounter build errors:

```bash
# Clean and rebuild
conan remove "package-name/*" -c
cd package-name
conan create . --build=missing
```

### Header not found

Make sure your CMakeLists.txt includes:

```cmake
find_package(package-name REQUIRED CONFIG)
target_link_libraries(your_target package-name::package-name)
```

## License

Each package maintains its original license:

- **fast-lzma2**: BSD-3-Clause / GPL-2.0 (dual license)
- **cppstub**: MIT

## Contributing

To add a new Conan package:

1. Create a new directory: `mkdir conan/package-name`
2. Create `conanfile.py` with package recipe
3. Add source files or download mechanism
4. Create `test_package/` for verification
5. Document in README.md
6. Test with `conan create . --build=missing`
7. Update this README with the new package

## References

- [Conan Documentation](https://docs.conan.io/)
- [fast-lzma2 Original Repository](https://github.com/conor42/fast-lzma2)
- [cppstub Original Repository](https://github.com/coolxv/cpp-stub)
