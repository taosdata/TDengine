---
name: tsdb-build-taosgen
description: Assist users with building, compiling, and installing taosgen from source code. Use this skill when users need help with cmake configuration, conan dependency management, compilation errors, platform-specific build issues, or setting up the development environment for taosgen. Trigger for phrases like "build taosgen", "compile taosgen", "cmake error", "conan install failed", "how to install taosgen", "build from source", "编译 taosgen", "构建 taosgen", "安装 taosgen", "编译报错", "编译失败", or when users encounter build-related issues on Linux, macOS, or Windows.
metadata:
  author: Yaming Pei
  version: 1.0.0
  owner_team: engine
---

# tsdb-build-taosgen

Help users build, compile, and install taosgen from source code.

## When to use

Use this skill when users need taosgen source build help, including dependency setup, CMake/Conan configuration, compile/test/install issues, and platform-specific troubleshooting.

Typical trigger keywords:
- `build taosgen`
- `compile taosgen`
- `cmake error`
- `conan install failed`
- `install taosgen from source`

## Input

Collect or confirm the minimum context before giving commands:
- OS and architecture (Linux/macOS, x64/ARM64)
- Compiler/CMake/Conan versions
- Current error logs (full command + first failing lines)
- Build goal (`Debug`/`Release`, local build vs install)

If key details are missing, ask targeted clarifying questions and avoid guessing environment-specific commands.

## Output

Provide results in this structure:
1. Short diagnosis summary
2. Exact commands for the user's platform
3. One verification command (`cmake --build .`, `ctest`, or install check)
4. If unresolved, next-step troubleshooting checklist

## Safety

- Do not run destructive commands (e.g., deleting system files or force-removing toolchains) without explicit user confirmation.
- Before privileged commands (`sudo ...`), clearly state why they are needed.
- Prefer project-local and reversible fixes first.
- Do not request or expose secrets, tokens, or credentials.

## Overview

taosgen is a C++17 project that uses:
- **CMake** (≥3.19) for build configuration
- **Conan** (≥2.19) for dependency management
- **CTest** for testing

Supported platforms:
- Linux (x64, ARM64)
- macOS (x64, ARM64)

## Prerequisites

Before building taosgen, ensure the following are installed:

### Required Tools

| Tool | Minimum Version | Installation |
|------|-----------------|--------------|
| CMake | 3.19+ | [cmake.org](https://cmake.org) |
| Conan | 2.19+ | [conan.io](https://conan.io) |
| C++ Compiler | C++17 support | gcc/g++ or clang |

### Platform-Specific Requirements

**Linux (Ubuntu/Debian):**
```bash
sudo apt-get update
sudo apt-get install -y cmake build-essential python3 python3-pip
pip3 install conan>=2.19
```

**macOS:**
```bash
# Install Xcode Command Line Tools
xcode-select --install

# Or use Homebrew
brew install cmake conan
```

## Standard Build Process

### Quick Build (Linux/macOS)

```bash
# Clone the repository
git clone https://github.com/taosdata/taosgen.git
cd taosgen

# Create build directory
mkdir build && cd build

# Install dependencies with Conan
conan install .. --build=missing --output-folder=./conan --settings=build_type=Release

# Configure with CMake
cmake .. -DCMAKE_BUILD_TYPE=Release

# Build
cmake --build .

# Run tests (optional)
ctest
```

### macOS Special Case

If the compiler doesn't automatically select the SDK:

```bash
cmake .. -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_OSX_SYSROOT=$(xcrun --show-sdk-path) \
  -DCMAKE_TOOLCHAIN_FILE=./conan/conan_toolchain.cmake
```

## Build Options

### Build Types

| Type | Use Case | Command |
|------|----------|---------|
| Release | Production builds | `-DCMAKE_BUILD_TYPE=Release` |
| Debug | Development/debugging | `-DCMAKE_BUILD_TYPE=Debug` |

### Conan Profile Configuration

Conan profiles determine how dependencies are built:

```bash
# Detect default profile (run once)
conan profile detect --force

# View current profile
conan profile show

# List available profiles
conan profile list
```

### Common CMake Options

```bash
# Specify custom install prefix
cmake .. -DCMAKE_INSTALL_PREFIX=/usr/local

# Enable verbose build output
cmake .. -DCMAKE_VERBOSE_MAKEFILE=ON
```

## Troubleshooting

### Issue: "Conan command not found"

**Solution:**
```bash
# Install conan via pip
pip3 install --user "conan>=2.19"

# Add to PATH if needed
export PATH="$HOME/.local/bin:$PATH"
```

### Issue: "CMake version too old"

**Solution:**
```bash
# Ubuntu/Debian - use Kitware's official repository
wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | sudo tee /usr/share/keyrings/kitware-archive-keyring.gpg >/dev/null
echo 'deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ jammy main' | sudo tee /etc/apt/sources.list.d/kitware.list >/dev/null
sudo apt-get update
sudo apt-get install cmake

# macOS
brew install cmake
```

### Issue: "C++17 standard not supported"

**Solution:**
Check compiler version and upgrade if necessary:
```bash
# Check gcc version
gcc --version  # Need 7.0+

# Check clang version
clang --version  # Need 5.0+

# Ubuntu/Debian - install newer gcc
sudo apt-get install gcc-10 g++-10
export CC=gcc-10 CXX=g++-10
```

### Issue: "Conan dependency resolution fails"

**Solution:**
```bash
# Clean conan cache and retry
conan remove "*" -c
conan install .. --build=missing --output-folder=./conan

# Or force rebuild all dependencies
conan install .. --build="*" --output-folder=./conan
```

### Issue: "macOS SDK not found"

**Solution:**
```bash
# Install Xcode Command Line Tools
xcode-select --install

# If already installed, reset path
sudo xcode-select --reset

# Or specify SDK path explicitly
cmake .. -DCMAKE_OSX_SYSROOT=$(xcrun --show-sdk-path)
```

## Testing

### Run All Tests

```bash
cd build
ctest
```

### Run with Verbose Output

```bash
ctest -V
```

### Run Specific Test

```bash
ctest -R <test_name>
```

### Parallel Testing

```bash
ctest -j$(nproc)  # Linux
ctest -j$(sysctl -n hw.ncpu)  # macOS
```

## Installation

After successful build:

```bash
cd build
sudo cmake --install .
```

Or specify custom prefix:
```bash
cmake --install . --prefix /path/to/install
```

## IDE Integration

### VSCode

Install extensions:
- C/C++ Extension Pack
- CMake Tools

Configure CMake Tools to use the conan toolchain:
```json
{
  "cmake.configureArgs": [
    "-DCMAKE_TOOLCHAIN_FILE=${workspaceFolder}/build/conan/conan_toolchain.cmake"
  ]
}
```

### CLion

1. Open project root directory
2. CMake should auto-detect the build configuration
3. If not, manually specify:
   - Build directory: `build`
   - CMake options: `-DCMAKE_TOOLCHAIN_FILE=./conan/conan_toolchain.cmake`

## Workflow for Build Issues

When user reports a build issue:

1. **Identify platform**: Linux/macOS? Architecture?
2. **Check prerequisites**: CMake version? Conan version? Compiler?
3. **Review error message**: Is it conan, cmake, or compile-time?
4. **Suggest solutions**: Use the troubleshooting guide above
5. **Verify fix**: Ask user to try the recommended solution

## Example User Interactions

**"How do I build taosgen?"**
- Provide the Quick Build steps for their platform

**"Conan install failed with missing packages"**
- Suggest `conan install .. --build="*"` to build from source

**"I'm on macOS and cmake can't find the SDK"**
- Provide macOS-specific SDK path solution

**"Build succeeded but tests fail"**
- Check if TDengine is running (required for integration tests)
- Suggest using `TSGEN_ENABLE_TEST=OFF` if just testing the build

## References

- [CMake Documentation](https://cmake.org/documentation/)
- [Conan Documentation](https://docs.conan.io/)
- [taosgen CI/CD](https://github.com/taosdata/taosgen/blob/main/.github/workflows/build.yml)

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-build-taosgen version=1.0.0 author=Yaming Pei`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
