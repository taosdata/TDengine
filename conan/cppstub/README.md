# C++ Stub Conan Package

This is a Conan package for cpp-stub, a simple and easy-to-use C++ stub library for unit testing.

## Package Information

- **Name**: cppstub
- **Version**: 1.0.0
- **License**: MIT
- **Source**: [https://github.com/coolxv/cpp-stub](https://github.com/coolxv/cpp-stub)
- **Type**: Header-only library

## Description

cpp-stub is a lightweight C++ library for creating test stubs/mocks. It provides:

- `stub.h` - Main stub functionality
- `addr_any.h` - Platform-specific address manipulation (Linux, macOS, Windows)

## Features

- Header-only library (no linking required)
- Cross-platform support (Linux, macOS, Windows)
- Simple API for function stubbing
- Platform-specific implementations included

## Usage

### Creating the Package

```bash
# Create the package locally
conan create . --build=missing

# Verify installation
conan list "cppstub/*"
```

### Using in Your Project

Add to your `conanfile.txt`:

```ini
[requires]
cppstub/1.0.0

[generators]
CMakeDeps
CMakeToolchain
```

Or in your `conanfile.py`:

```python
def requirements(self):
    self.requires("cppstub/1.0.0")
```

### CMakeLists.txt Example

```cmake
cmake_minimum_required(VERSION 3.15)
project(MyProject CXX)

find_package(cppstub REQUIRED CONFIG)

add_executable(mytest test.cpp)
target_link_libraries(mytest cppstub::cppstub)
```

### Code Example

```cpp
#include <stub.h>
#include <addr_any.h>

// Your test code here
// The stub.h provides functionality for function stubbing
// The addr_any.h provides platform-specific address manipulation
```

## Building from Source

The package exports header files from the cpp-stub repository:

1. Source code is exported from the `cppstub/` directory
2. Headers are packaged:
   - `stub.h` from `src/`
   - `addr_any.h` from platform-specific directory (`src_linux/`, `src_darwin/`, or `src_win/`)
3. No compilation needed (header-only)

## Platform-Specific Files

The package automatically selects the correct `addr_any.h` based on your OS:

- **Linux**: `src_linux/addr_any.h`
- **macOS**: `src_darwin/addr_any.h`
- **Windows**: `src_win/addr_any.h`

## Notes

- This is a header-only library, so no linking is required
- The package ID is cleared (platform-independent) since it's header-only
- Suitable for C++ unit testing and mocking

## License

MIT License - See LICENSE file for details.

## Original Repository

- [GitHub](https://github.com/coolxv/cpp-stub)
- Commit: 3137465194014d66a8402941e80d2bccc6346f51
