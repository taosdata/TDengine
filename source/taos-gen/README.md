<!-- omit in toc -->
# taosgen

[![GitHub Actions Workflow Status](https://img.shields.io/github/actions/workflow/status/taosdata/taosgen/build.yml)](https://github.com/taosdata/taosgen/actions/workflows/build.yml)
[![codecov](https://codecov.io/gh/taosdata/taosgen/branch/main/graph/badge.svg)](https://app.codecov.io/github/taosdata/taosgen)
![GitHub commit activity](https://img.shields.io/github/commit-activity/m/taosdata/taosgen)
![GitHub License](https://img.shields.io/github/license/taosdata/taosgen)
![GitHub Tag](https://img.shields.io/github/v/tag/taosdata/taosgen?label=latest)
<br />
[![Twitter Follow](https://img.shields.io/twitter/follow/tdenginedb?label=TDengine&style=social)](https://twitter.com/tdenginedb)
[![YouTube Channel](https://img.shields.io/badge/Subscribe_@tdengine--white?logo=youtube&style=social)](https://www.youtube.com/@tdengine)
[![Discord Community](https://img.shields.io/badge/Join_Discord--white?logo=discord&style=social)](https://discord.com/invite/VZdSuUg4pS)
[![LinkedIn](https://img.shields.io/badge/Follow_LinkedIn--white?logo=linkedin&style=social)](https://www.linkedin.com/company/tdengine)
[![StackOverflow](https://img.shields.io/badge/Ask_StackOverflow--white?logo=stackoverflow&style=social&logoColor=orange)](https://stackoverflow.com/questions/tagged/tdengine)

<!-- omit in toc -->
## Table of Contents
- [1. Introduction](#1-introduction)
- [2. Architecture](#2-architecture)
- [3. Documentation](#3-documentation)
- [4. AI Agent Integration](#4-ai-agent-integration)
- [5. Prerequisites](#5-prerequisites)
  - [Platform-Specific Requirements](#platform-specific-requirements)
    - [Linux / macOS](#linux--macos)
    - [Windows](#windows)
- [6. Building](#6-building)
  - [Linux / macOS](#linux--macos-1)
  - [Windows](#windows-1)
    - [Option 1: Using Visual Studio Developer Command Prompt](#option-1-using-visual-studio-developer-command-prompt)
    - [Option 2: Using vcvarsallbat](#option-2-using-vcvarsallbat)
- [7. Testing](#7-testing)
  - [7.1 Run Tests](#71-run-tests)
  - [7.2 Add Test Cases](#72-add-test-cases)
- [8. Packaging](#8-packaging)
- [9. CI/CD](#9-cicd)
- [10. Submitting Issues](#10-submitting-issues)
  - [10.1 Required Information](#101-required-information)
  - [10.2 Additional Information](#102-additional-information)
- [11. Submitting PRs](#11-submitting-prs)
- [12. References](#12-references)
- [13. Appendix](#13-appendix)
  - [13.1 Performance Benchmarks](#131-performance-benchmarks)
- [14. License](#14-license)

## 1. Introduction
`taosgen` is a performance benchmarking tool for time-series data products, supporting data generation and write performance testing. `taosgen` uses "jobs" as the basic unit, which are user-defined sets of operations for specific tasks. Each job contains one or more steps and can be connected to other jobs via dependencies, forming a Directed Acyclic Graph (DAG) execution flow for flexible and efficient task orchestration.

Currently, `taosgen` supports Linux, macOS and Windows systems.


## 2. Architecture

For detailed architecture content, please refer to the design document:

- [Architecture Design](docs/architecture.md)

Quick summary:

- `taosgen` is configuration-driven: CLI/ENV/YAML are merged into runtime job definitions.
- The execution model is DAG-based job scheduling with worker-driven step execution.
- `ActionFactory` maps step `uses` + config to concrete actions (DDL / insert / query / subscribe).
- Insert workloads use a producer-consumer pipeline with bounded queues and pluggable sinks.

For design philosophy, trade-offs, module responsibilities, core sequence diagrams, and optional lifecycle details, read `docs/architecture.md`.


## 3. Documentation
- For usage, refer to the [Reference Manual](https://docs.tdengine.com/tdengine-reference/tools/taosgen/), which covers running, command-line arguments, configuration parameters, and sample configuration files.
- This quick guide is mainly for developers who want to contribute, build, and test the `taosgen` tool. For more information about TDengine, visit the [official documentation](https://docs.tdengine.com/).


## 4. AI Agent Integration

`taosgen` provides AI Skill configurations to help AI agents (such as Claude, Claude Code, Cursor, etc.) assist users through natural language conversations. These skills cover configuration generation, build assistance, and development workflows.

**Skills Location:** `.agent/skills/`

**Available Skills:**

1. **taosgen-config** - Generate benchmark configurations
   - Create taosgen configuration files for TDengine, MQTT, and Kafka through natural language descriptions
   - Automatically validate configurations and provide optimization suggestions
   - Support various data generation methods (random, expression, CSV import)
   - Configure complex job workflows with dependencies

2. **taosgen-build** - Build and compile assistance
   - Guide users through the build process with cmake and conan
   - Troubleshoot common build issues on different platforms
   - Provide IDE integration instructions (VSCode, CLion)
   - Assist with testing and installation

**How to use (taking Claude Code as an example):**

**Option 1: Copy to Claude Code skills directory (Recommended)**
```bash
mkdir -p ~/.claude/skills/
cp -r .agent/skills/taosgen-* ~/.claude/skills/

# Then start Claude Code in your project directory
claude
```

**Option 2: Project-local symlink**
Claude Code recognizes skills from the `.claude/skills/` directory. To use the skill locally in this project:
```bash
# Create symlink in project's .claude directory
mkdir -p .claude/
ln -s ../.agent/skills .claude/

# Start Claude Code
claude
```


**Example conversations with Claude Code:**

```
"Create a taosgen config for testing TDengine with 10,000 devices,
 each reporting temperature and humidity every second for 1 hour"

"Generate an MQTT benchmark configuration to simulate 1000 IoT devices
 publishing to topics with QoS 1"

"Help me create a Kafka load test config with 5M messages and batch processing"
```

**Skill Documentation:**
- [taosgen-config/SKILL.md](.agent/skills/taosgen-config/SKILL.md) - Configuration generator
- [taosgen-config/references/](.agent/skills/taosgen-config/references/) - Configuration reference docs
- [taosgen-build/SKILL.md](.agent/skills/taosgen-build/SKILL.md) - Build assistant


## 5. Prerequisites
First, ensure TDengine is deployed locally. For detailed deployment steps, see [Deploy TDengine](https://docs.tdengine.com/get-started/deploy-from-package/). Make sure both taosd and taosAdapter services are running.

Before installing and using `taosgen`, ensure you meet the following platform-specific prerequisites:

- cmake, version 3.19 or above. See [cmake](https://cmake.org).
- conan, version 2.19 or above. See [conan](https://conan.io).
- Python 3 with `pip`, because Conan 2.x is typically installed and managed through Python.
- A C++17-capable compiler.
- TDengine client headers and libraries available to the build environment.

The bundled Conan dependency set includes `fmt`, `jemalloc`, `mimalloc`, `yaml-cpp`, `luajit`, `nlohmann_json`, compression libraries, `spdlog`, `librdkafka`, and the CSV parser declared in `conanfile.txt`.

### Platform-Specific Requirements

#### Linux / macOS
- GCC/Clang compiler with C++17 support
- Ubuntu/Debian example:
  ```shell
  sudo apt update
  sudo apt install -y build-essential cmake python3 python3-pip git pkg-config
  python3 -m pip install --user "conan>=2.19,<3"
  ~/.local/bin/conan profile detect --force
  ```
- RHEL/CentOS/AlmaLinux/Rocky example:
  ```shell
  sudo yum install -y gcc gcc-c++ make cmake3 python3 python3-pip git pkgconfig
  python3 -m pip install --user "conan>=2.19,<3"
  ~/.local/bin/conan profile detect --force
  ```

#### Windows
- Visual Studio 2019 or above (Visual Studio 2022 recommended)
- Install Conan 2.x and initialize a profile before the first configure:
  ```cmd
  py -m pip install --user "conan>=2.19,<3"
  conan profile detect --force
  ```

## 6. Building
This section provides detailed instructions for building `taosgen` on Linux, macOS or Windows platforms.
Before proceeding, make sure you are in the project root directory.

>**Note: This project is developed and compiled using the C++17 standard. Please ensure your compiler supports C++17.**

The most important setup step is to generate a Conan 2.x profile before the first build:
```shell
conan profile detect --force
```

Build options exposed by CMake include:
- `TSGEN_ENABLE_TEST=ON|OFF`
- `TSGEN_ENABLE_COVERAGE=ON|OFF`
- `TSGEN_BUNDLE_JEMALLOC=ON|OFF`
- `TSGEN_BUNDLE_MIMALLOC=ON|OFF`

Only one allocator option should be enabled at a time.

### Linux / macOS

```shell
mkdir build && cd build
conan install .. --build=missing --output-folder=./conan --settings=build_type=Release
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=./conan/conan_toolchain.cmake
cmake --build . --parallel
```

To enable optional build features during configuration, extend the CMake command, for example:
```shell
cmake .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE=./conan/conan_toolchain.cmake \
  -DTSGEN_ENABLE_TEST=ON \
  -DTSGEN_BUNDLE_MIMALLOC=ON \
  -DTSGEN_BUNDLE_JEMALLOC=OFF
```

On macOS, if your compiler does not automatically select the appropriate default SDK, specify CMAKE_OSX_SYSROOT during configuration:
```shell
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_OSX_SYSROOT=$(xcrun --show-sdk-path) -DCMAKE_TOOLCHAIN_FILE=./conan/conan_toolchain.cmake
```

### Windows

#### Option 1: Using Visual Studio Developer Command Prompt

Open **x64 Native Tools Command Prompt for VS 2022** (or VS 2019) from the Start Menu, then run:

```cmd
mkdir build && cd build
conan install .. --build=missing --output-folder=./conan --settings=build_type=Release --settings=compiler=msvc --settings=compiler.version=193 --settings=compiler.cppstd=17 --settings=compiler.runtime=dynamic
cmake .. -G "Visual Studio 17 2022" -A x64 -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=./conan/conan_toolchain.cmake
cmake --build . --config Release
```

For Visual Studio 2019, change the generator to `"Visual Studio 16 2019"`.

#### Option 2: Using vcvarsall.bat

If you prefer using a regular command prompt, you can use the `vcvarsall.bat` script to set up the environment:

```cmd
"<path_to_vs>\VC\Auxiliary\Build\vcvarsall.bat" x64
mkdir build && cd build
conan install .. --build=missing --output-folder=./conan --settings=build_type=Release --settings=compiler=msvc --settings=compiler.version=193 --settings=compiler.cppstd=17 --settings=compiler.runtime=dynamic
cmake .. -G "Visual Studio 17 2022" -A x64 -DCMAKE_BUILD_TYPE=Release -DCMAKE_TOOLCHAIN_FILE=./conan/conan_toolchain.cmake
cmake --build . --config Release
```

Replace `<path_to_vs>` with the actual Visual Studio installation path, for example:
- `"C:\Program Files\Microsoft Visual Studio\2022\Community\VC\Auxiliary\Build\vcvarsall.bat" x64`
- `"C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\VC\Auxiliary\Build\vcvarsall.bat" x64`

For Visual Studio 2019, also change the generator to `"Visual Studio 16 2019"` and `compiler.version` to `192`.

## 7. Testing

### 7.1 Run Tests
`taosgen` uses ctest as its test framework. Run `ctest` in the build directory to execute all test cases.

On Linux / macOS:
```shell
cd build
ctest --output-on-failure
```

On Windows (MSVC multi-config generator requires `--build-config`):
```cmd
cd build
ctest --build-config Release --output-on-failure
```

### 7.2 Add Test Cases
Test cases are located in the test directories of each submodule.
- To add test cases to an existing test file: name the test functions with the prefix `test_` and call them in the `main` function.
- To add a new test file: write test cases and a `main` function in the file, and add the build configuration in the corresponding `CMakeLists.txt` in the same directory.


## 8. Packaging
`taosgen` ships as a Linux tar.gz package built by `source/taos-gen/packaging/pack_gen_tar.sh`.

### What gets packaged
Only the `taosgen` executable is packaged:
- inner `package.tar.gz`: `bin/taosgen`
- outer tar.gz: `install_gen.sh`, `uninstall_gen.sh`, `package.tar.gz`

### Prerequisites
- Build `taosgen` first. For example:
  ```shell
  cd build
  cmake --build . --target taosgen
  ```
  Or follow the CMake workflow in [6. Building](#6-building) and make sure the final build output contains `bin/taosgen`.
- Install taos-community on the target machine before running `taosgen`, because `taosgen` needs `libtaos.so` at runtime.

### Create the package
From the repository root:
```shell
cd source/taos-gen/packaging
bash ./pack_gen_tar.sh -c ../build -n 3.3.6.0
```

Optional arguments:
- `-m <compat_version>`: compatible version string (default: `3.0.0.0`)
- `-V stable|beta`: package version type (default: `stable`)

`-c` must point to the compile directory that contains `bin/taosgen`.

### Output
Stable builds generate:
```shell
source/taos-gen/release/taosGen-<version>-Linux-<arch>.tar.gz
```

Beta builds generate:
```shell
source/taos-gen/release/taosGen-<version>-beta-Linux-<arch>.tar.gz
```

### Install the package
```shell
cd source/taos-gen/release
mkdir -p install-test && cd install-test

tar xzf ../taosGen-<version>-Linux-<arch>.tar.gz
cd taosGen-<version>
bash ./install_gen.sh -s
```

The installer copies `taosgen` to `/usr/bin/` and warns if `libtaos.so` is not available in common library paths.

### Uninstall
```shell
bash ./uninstall_gen.sh
```

### Two-layer tar layout
The package uses the same two-layer layout as the TDengine community tar packages: the outer tarball contains the install scripts plus an inner `package.tar.gz`, and the installer extracts the inner archive before copying `bin/taosgen` into place.

## 9. CI/CD
- [Build Workflow](https://github.com/taosdata/taosgen/actions/workflows/build.yml)
- [Code Coverage](https://app.codecov.io/github/taosdata/taosgen)


## 10. Submitting Issues
We welcome [GitHub Issues](https://github.com/taosdata/taosgen/issues/new?template=Blank+issue). Please provide the following information to help us diagnose and resolve issues efficiently:

### 10.1 Required Information
- Problem Description:
  Provide a clear and detailed description of the issue.
  Indicate whether the issue is persistent or intermittent.
  If possible, include detailed stack traces or error messages to aid diagnosis.

- taosgen version or Commit ID
- taosgen configuration parameters
- TDengine server version

### 10.2 Additional Information
- Operating System: Specify the OS and its version.
- Steps to Reproduce: Provide instructions to reproduce the issue.
- Environment Configuration: Include any relevant environment settings.
- Logs: Attach any logs that may help diagnose the issue.


## 11. Submitting PRs
We welcome contributions! Please follow these steps when submitting a PR:
1. Fork the project ([how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo)).
2. Create a new branch from `main` with a meaningful name (`git checkout -b my_branch`). Do not modify the `main` branch directly.
3. Make your changes, ensure all unit tests pass, and add new tests to verify your changes.
4. Push your changes to your remote branch (`git push origin my_branch`).
5. Create a Pull Request on GitHub ([how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request)).
6. After submitting the PR, you can find it under [Pull Requests](https://github.com/taosdata/taosgen/pulls). Click the link to view CI status. If it passes, you'll see “All checks have passed”. You can always click “Show all checks” -> “Details” for detailed logs.
7. After CI passes, you can check your PR's test coverage on [codecov](https://app.codecov.io/gh/taosdata/taosgen/pulls).


## 12. References
- [TDengine Official Website](https://www.tdengine.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)


## 13. Appendix
Project source code layout (directories only):
```
<root>
├── cmake
├── conf
└── src
    ├── actions
    │   ├── base
    │   ├── components
    │   │   ├── compressor
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── connector
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── encoding
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── expression
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── formatter
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── garbage_collector
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── generator
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── memory_pool
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   ├── metrics
    │   │   │   ├── inc
    │   │   │   ├── src
    │   │   │   └── test
    │   │   └── reader
    │   │       └── csv
    │   │           ├── inc
    │   │           ├── src
    │   │           └── test
    │   ├── config
    │   │   ├── inc
    │   │   ├── src
    │   │   └── test
    │   └── core
    │       ├── checkpoint
    │       │   ├── inc
    │       │   ├── src
    │       │   └── test
    │       ├── create
    │       │   ├── inc
    │       │   ├── src
    │       │   └── test
    │       ├── insert
    │       │   ├── inc
    │       │   ├── src
    │       │   │   ├── generator
    │       │   │   │   ├── inc
    │       │   │   │   ├── src
    │       │   │   │   └── test
    │       │   │   ├── pipeline
    │       │   │   │   ├── inc
    │       │   │   │   ├── src
    │       │   │   │   └── test
    │       │   │   └── writer
    │       │   │       ├── inc
    │       │   │       ├── src
    │       │   │       └── test
    │       │   └── test
    │       ├── query
    │       │   └── inc
    │       └── subscribe
    │           ├── inc
    │           └── src
    ├── engine
    │   ├── inc
    │   ├── src
    │   └── test
    ├── parameter
    │   ├── conf
    │   ├── inc
    │   ├── src
    │   └── test
    ├── plugins
    │   ├── inc
    │   └── src
    │       ├── kafka
    │       │   ├── inc
    │       │   ├── src
    │       │   └── test
    │       ├── mqtt
    │       │   ├── inc
    │       │   ├── src
    │       │   └── test
    │       └── tdengine
    │           ├── inc
    │           ├── src
    │           └── test
    ├── utils
    │   ├── inc
    │   ├── src
    │   └── test
    └── workflow
        ├── inc
        └── src
```

### 13.1 Performance Benchmarks

- Test environment: Client and server identical

  | Component | Specification |
  |---|---|
  | OS | Ubuntu 20.04.6 LTS |
  | CPU | Intel Xeon E5-2650 v3 @ 2.30GHz (Haswell-EP), dual-socket |
  | Cores/Threads | 20C/40T (10C/20T per socket, Hyper-Threading) |
  | Cache | L3 25MB (cache size: 25600 KB) |
  | Memory | 251 GB |
  | Storage | 447 GB SSD × 2, 1.76 TB SSD |
  | Software | TDengine Enterprise 3.3.8.9 (default) ; FlashMQ v1.24.0 (default); Kafka 2.13-4.1.0 (default) |

- Data model: 1,000,000 sub-tables (meters) with current/voltage/phase; interlace=1.
- Results are indicative; actual throughput depends on network, server settings, message size, and concurrency.
- Units: K = thousand rows/sec, M = million rows/sec.

| Target | Scenario | Baseline | taosgen | Config Summary | Gain |
|---|---|---:|---:|---|---:|
| TDengine | 100M rows, 20 threads | 3.168M rps (taosBenchmark) | 3.534M rps | vgroups=32, stmt2, batch=10k | +11.58% |
| MQTT | 2M rows, 20 threads, single record/message | — | 15.15K rps | qos=0, records_per_message=1 | — |
| MQTT | 100M rows, 20 threads, 500 records/message | — | 3.127M rps | qos=0, records_per_message=500 | Significant |
| Kafka (single thread) | 100M rows, official script | 912.70K rps | 968.93K rps | acks=0, batch tuned | +6.16% |
| Kafka (20-way concurrency) | Official script (20 processes) | 2.772M rps | 4.577M rps | taosgen 20 threads | +65.14% |

Notes:
- MQTT QoS 0 with batching improves throughput; broker limits and payload size have major impact.
- TDengine vs taosBenchmark: under equivalent setup, taosgen shows higher throughput and low framework overhead.
- Kafka vs official tool: taosgen outperforms in single-thread and multi-process scenarios.


## 14. License
[MIT License](./LICENSE)