# Fast LZMA2 Conan Package Usage Guide

## Quick Start

### 1. Create and Install Package to Local Cache

```bash
# Execute in the current directory (containing conanfile.py)
conan create . --build=missing

# Verify installation
conan list "fast-lzma2/*"
```

### 2. Build with Different Configurations

```bash
# Build shared library version
conan create . --build=missing -o fast-lzma2/*:shared=True

# Build Debug version
conan create . --build=missing -s build_type=Debug

# Build without fPIC (if position independent code is not needed)
conan create . --build=missing -o fast-lzma2/*:fPIC=False
```

### 3. Using in Your Project

#### Method 1: Using conanfile.txt

Create `conanfile.txt`:

```ini
[requires]
fast-lzma2/1.0.1

[generators]
CMakeDeps
CMakeToolchain

[options]
fast-lzma2/*:shared=False
```

Install dependencies:

```bash
conan install . --build=missing
```

#### Method 2: Using conanfile.py

Create `conanfile.py`:

```python
from conan import ConanFile
from conan.tools.cmake import cmake_layout

class MyProjectConan(ConanFile):
    settings = "os", "compiler", "build_type", "arch"
    generators = "CMakeDeps", "CMakeToolchain"

    def requirements(self):
        self.requires("fast-lzma2/1.0.1")
    
    def layout(self):
        cmake_layout(self)
```

Install dependencies:

```bash
conan install . --build=missing
```

### 4. CMakeLists.txt Configuration

```cmake
cmake_minimum_required(VERSION 3.15)
project(MyProject C)

# Use Conan-generated toolchain file
find_package(fast-lzma2 REQUIRED CONFIG)

add_executable(myapp main.c)
target_link_libraries(myapp fast-lzma2::fast-lzma2)
```

### 5. Building the Project

```bash
# Use Conan-generated toolchain
cmake --preset conan-release  # or conan-debug
cmake --build --preset conan-release
```

## Code Examples

### Basic Compression/Decompression

```c
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "fast-lzma2.h"

int main() {
    // Original data
    const char* data = "Hello, Fast LZMA2!";
    size_t src_size = strlen(data);
    
    // 1. Calculate maximum buffer size needed for compression
    size_t max_compressed_size = FL2_compressBound(src_size);
    char* compressed = malloc(max_compressed_size);
    
    // 2. Compress data (compression level: 1-10, recommended: 6)
    size_t compressed_size = FL2_compress(
        compressed, max_compressed_size,
        data, src_size,
        6  // Compression level
    );
    
    // 3. Check for errors
    if (FL2_isError(compressed_size)) {
        fprintf(stderr, "Compression failed: %s\n", 
                FL2_getErrorName(compressed_size));
        free(compressed);
        return 1;
    }
    
    printf("Compression ratio: %.2f%%\n", 
           (compressed_size * 100.0) / src_size);
    
    // 4. Decompress
    char* decompressed = malloc(src_size);
    size_t decompressed_size = FL2_decompress(
        decompressed, src_size,
        compressed, compressed_size
    );
    
    // 5. Check decompression errors
    if (FL2_isError(decompressed_size)) {
        fprintf(stderr, "Decompression failed: %s\n", 
                FL2_getErrorName(decompressed_size));
        free(compressed);
        free(decompressed);
        return 1;
    }
    
    // 6. Verify data
    if (memcmp(data, decompressed, src_size) == 0) {
        printf("Success!\n");
    }
    
    free(compressed);
    free(decompressed);
    return 0;
}
```

### Streaming Compression (For Large Files)

```c
#include <stdio.h>
#include "fast-lzma2.h"

int compress_file(const char* input_path, const char* output_path) {
    FILE* fin = fopen(input_path, "rb");
    FILE* fout = fopen(output_path, "wb");
    if (!fin || !fout) return -1;
    
    // Create compression context
    FL2_CStream* cstream = FL2_createCStream();
    if (!cstream) {
        fclose(fin);
        fclose(fout);
        return -1;
    }
    
    // Initialize compression stream (compression level 6)
    size_t init_result = FL2_initCStream(cstream, 6);
    if (FL2_isError(init_result)) {
        FL2_freeCStream(cstream);
        fclose(fin);
        fclose(fout);
        return -1;
    }
    
    // Read and compress in chunks
    size_t const buf_size = 16 * 1024; // 16KB buffer
    void* in_buffer = malloc(buf_size);
    void* out_buffer = malloc(buf_size);
    
    size_t read_size;
    while ((read_size = fread(in_buffer, 1, buf_size, fin)) > 0) {
        FL2_inBuffer input = { in_buffer, read_size, 0 };
        
        while (input.pos < input.size) {
            FL2_outBuffer output = { out_buffer, buf_size, 0 };
            size_t result = FL2_compressStream(cstream, &output, &input);
            
            if (FL2_isError(result)) {
                goto cleanup;
            }
            
            fwrite(out_buffer, 1, output.pos, fout);
        }
    }
    
    // Finish compression
    FL2_outBuffer output = { out_buffer, buf_size, 0 };
    size_t result = FL2_endStream(cstream, &output);
    if (!FL2_isError(result)) {
        fwrite(out_buffer, 1, output.pos, fout);
    }
    
cleanup:
    free(in_buffer);
    free(out_buffer);
    FL2_freeCStream(cstream);
    fclose(fin);
    fclose(fout);
    return 0;
}
```

### Get Version Information

```c
#include <stdio.h>
#include "fast-lzma2.h"

void print_version() {
    unsigned version = FL2_versionNumber();
    printf("Fast LZMA2 version: %u.%u.%u\n",
           version / 10000,
           (version / 100) % 100,
           version % 100);
}
```

## Compilation Options

### CFLAGS

- `-Wall`: Enable all warnings
- `-O2`: Optimization level 2 (Release mode)
- `-O0 -g`: No optimization + debug info (Debug mode)
- `-pthread`: Enable multi-threading support
- `-fPIC`: Position independent code (for shared libraries)

### Special Defines

- `LZMA2_DEC_OPT`: Automatically defined on x86_64 architecture, enables optimized assembly decompression code
- `FL2_DLL_EXPORT`: Windows DLL export (handled automatically)
- `FL2_DLL_IMPORT`: Windows DLL import (automatically defined when using shared library)

## Performance Tips

1. **Compression Level Selection**:
   - Level 1-3: Fast compression, lower compression ratio
   - Level 4-6: Balanced compression speed and ratio (recommended)
   - Level 7-10: High compression ratio, slower speed

2. **Memory Usage**:
   - Higher compression levels use more memory
   - Streaming API allows controlling memory usage

3. **Multi-threading**:
   - Library uses pthread internally, automatically utilizes multiple cores
   - Multi-threading support is enabled by default during compilation

## Troubleshooting

### Compilation Errors

If you encounter compilation errors:

```bash
# Clean and rebuild
conan remove "fast-lzma2/*" -c
conan create . --build=missing
```

### Linking Errors

Ensure CMakeLists.txt correctly links the library:

```cmake
find_package(fast-lzma2 REQUIRED CONFIG)
target_link_libraries(your_target fast-lzma2::fast-lzma2)
```

On Linux, you may need to explicitly link pthread:

```cmake
find_package(Threads REQUIRED)
target_link_libraries(your_target 
    fast-lzma2::fast-lzma2 
    Threads::Threads)
```

## Advanced Usage

### Export to Local Conan Repository

```bash
# Export package to local
conan export . fast-lzma2/1.0.1@

# Or specify user and channel
conan export . fast-lzma2/1.0.1@mycompany/stable
```

### Upload to Private Conan Server

```bash
# Add remote repository
conan remote add myremote http://my-conan-server.com

# Upload package
conan upload "fast-lzma2/1.0.1" -r myremote --all
```

### Cross-Compilation

```bash
# Build for ARM64
conan create . --build=missing -s arch=armv8

# Build for Android
conan create . --build=missing -pr:h=android-armv8
```

## License

Fast LZMA2 uses dual licensing:

- **BSD-3-Clause**: Suitable for most commercial and open-source projects
- **GPL-2.0**: Suitable for GPL-compatible projects

Please refer to the `LICENSE` and `COPYING` files for detailed information.

## Support and Feedback

- Original project: https://github.com/conor42/fast-lzma2
- Report issues: Please submit issues at the original project repository
