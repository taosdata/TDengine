# Fast LZMA2 Conan Package

This is a Conan package for Fast LZMA2 Library v1.0.1, an optimized LZMA2 compression algorithm.

## Package Information

- **Name**: fast-lzma2
- **Version**: 1.0.1
- **License**: BSD-3-Clause, GPL-2.0
- **Source**: [https://github.com/conor42/fast-lzma2](https://github.com/conor42/fast-lzma2)

## Features

- Fast LZMA2 compression and decompression
- Support for both static and shared library builds
- Cross-platform support (Linux, macOS, Windows)
- Built using make (not CMake)

## Build Options

| Option   | Default | Description                          |
|----------|---------|--------------------------------------|
| shared   | False   | Build as shared library              |
| fPIC     | True    | Position independent code (non-Windows) |

## Usage

### Creating the Package

```bash
# Create the package locally
conan create . --build=missing

# Create with specific options
conan create . --build=missing -o fast-lzma2/*:shared=True
```

### Testing the Package

```bash
# Test the package
conan create . --build=missing

# The test_package will automatically run
```

### Using in Your Project

Add to your `conanfile.txt`:

```ini
[requires]
fast-lzma2/1.0.1

[generators]
CMakeDeps
CMakeToolchain
```

Or in your `conanfile.py`:

```python
def requirements(self):
    self.requires("fast-lzma2/1.0.1")
```

### CMakeLists.txt Example

```cmake
cmake_minimum_required(VERSION 3.15)
project(MyProject C)

find_package(fast-lzma2 REQUIRED CONFIG)

add_executable(myapp main.c)
target_link_libraries(myapp fast-lzma2::fast-lzma2)
```

### Code Example

```c
#include <stdio.h>
#include <string.h>
#include "fast-lzma2.h"

int main() {
    const char* data = "Hello, Fast LZMA2!";
    size_t src_size = strlen(data);
    
    // Get compression bound
    size_t max_size = FL2_compressBound(src_size);
    char* compressed = malloc(max_size);
    
    // Compress
    size_t compressed_size = FL2_compress(compressed, max_size, 
                                         data, src_size, 6);
    
    if (FL2_isError(compressed_size)) {
        fprintf(stderr, "Compression error: %s\n", 
                FL2_getErrorName(compressed_size));
        return 1;
    }
    
    // Decompress
    char* decompressed = malloc(src_size);
    size_t decompressed_size = FL2_decompress(decompressed, src_size,
                                              compressed, compressed_size);
    
    if (FL2_isError(decompressed_size)) {
        fprintf(stderr, "Decompression error: %s\n", 
                FL2_getErrorName(decompressed_size));
        return 1;
    }
    
    printf("Success!\n");
    free(compressed);
    free(decompressed);
    return 0;
}
```

## Building from Source

The package uses the original Makefile build system from the fast-lzma2 repository:

1. Source code is exported from the `fast-lzma2/` directory
2. Built using `make` with appropriate flags
3. Installs headers: `fast-lzma2.h`, `fl2_errors.h`
4. Installs library: `libfast-lzma2.a` (static) or `libfast-lzma2.so*` (shared)

## System Dependencies

- GCC or compatible C compiler
- GNU Make
- pthread (on Linux/Unix systems)

## Notes

- The library is compiled with `-O2` optimization by default
- For Debug builds, uses `-O0 -g` instead
- On x86_64 architectures, includes optimized assembly code
- Thread-safe with pthread support on Unix-like systems

## License

This Conan package follows the dual-license of the original fast-lzma2 library:

- BSD-3-Clause
- GPL-2.0

See LICENSE and COPYING files for details.
