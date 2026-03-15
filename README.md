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
- [2. Documentation](#2-documentation)
- [3. AI Agent Integration](#3-ai-agent-integration)
- [4. Prerequisites](#4-prerequisites)
- [5. Build](#5-build)
- [6. Testing](#6-testing)
  - [6.1 Run Tests](#61-run-tests)
  - [6.2 Add Test Cases](#62-add-test-cases)
- [7. CI/CD](#7-cicd)
- [8. Submitting Issues](#8-submitting-issues)
  - [8.1 Required Information](#81-required-information)
  - [8.2 Additional Information](#82-additional-information)
- [9. Submitting PRs](#9-submitting-prs)
- [10. References](#10-references)
- [11. Appendix](#11-appendix)
  - [11.1 Performance Benchmarks](#111-performance-benchmarks)
- [12. License](#12-license)

## 1. Introduction
`taosgen` is a performance benchmarking tool for time-series data products, supporting data generation and write performance testing. `taosgen` uses "jobs" as the basic unit, which are user-defined sets of operations for specific tasks. Each job contains one or more steps and can be connected to other jobs via dependencies, forming a Directed Acyclic Graph (DAG) execution flow for flexible and efficient task orchestration.

Currently, `taosgen` supports Linux and macOS systems.

## 2. Documentation
- For usage, refer to the [Reference Manual](https://docs.tdengine.com/tdengine-reference/tools/taosgen/), which covers running, command-line arguments, configuration parameters, and sample configuration files.
- This quick guide is mainly for developers who want to contribute, build, and test the `taosgen` tool. For more information about TDengine, visit the [official documentation](https://docs.tdengine.com/).

## 3. AI Agent Integration

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

## 4. Prerequisites
First, ensure TDengine is deployed locally. For detailed deployment steps, see [Deploy TDengine](https://docs.tdengine.com/get-started/deploy-from-package/). Make sure both taosd and taosAdapter services are running.

Before installing and using `taosgen`, ensure you meet the following platform-specific prerequisites:

- cmake, version 3.19 or above. See [cmake](https://cmake.org).
- conan, version 2.19 or above. See [conan](https://conan.io).

## 5. Build
This section provides detailed instructions for building `taosgen` on Linux or macOS platforms.
Before proceeding, make sure you are in the project root directory.

>**Note: This project is developed and compiled using the C++17 standard. Please ensure your compiler supports C++17.**

```shell
mkdir build && cd build
conan install .. --build=missing --output-folder=./conan --settings=build_type=Release
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build .
```

On macOS, if your compiler does not automatically select the appropriate default SDK, specify CMAKE_OSX_SYSROOT during configuration:
```shell
cmake .. -DCMAKE_BUILD_TYPE=Release -DCMAKE_OSX_SYSROOT=$(xcrun --show-sdk-path) -DCMAKE_TOOLCHAIN_FILE=./conan/conan_toolchain.cmake
```

## 6. Testing

### 6.1 Run Tests
`taosgen` uses ctest as its test framework. Run `ctest` in the build directory to execute all test cases.

### 6.2 Add Test Cases
Test cases are located in the test directories of each submodule.
- To add test cases to an existing test file: name the test functions with the prefix `test_` and call them in the `main` function.
- To add a new test file: write test cases and a `main` function in the file, and add the build configuration in the corresponding `CMakeLists.txt` in the same directory.

## 7. CI/CD
- [Build Workflow](https://github.com/taosdata/tsgen/actions/workflows/build.yml)
- [Code Coverage](https://app.codecov.io/github/taosdata/taosgen)

## 8. Submitting Issues
We welcome [GitHub Issues](https://github.com/taosdata/taosgen/issues/new?template=Blank+issue). Please provide the following information to help us diagnose and resolve issues efficiently:

### 8.1 Required Information
- Problem Description:
  Provide a clear and detailed description of the issue.
  Indicate whether the issue is persistent or intermittent.
  If possible, include detailed stack traces or error messages to aid diagnosis.

- taosgen version or Commit ID
- taosgen configuration parameters
- TDengine server version

### 8.2 Additional Information
- Operating System: Specify the OS and its version.
- Steps to Reproduce: Provide instructions to reproduce the issue.
- Environment Configuration: Include any relevant environment settings.
- Logs: Attach any logs that may help diagnose the issue.

## 9. Submitting PRs
We welcome contributions! Please follow these steps when submitting a PR:
1. Fork the project ([how to fork a repo](https://docs.github.com/en/get-started/quickstart/fork-a-repo)).
2. Create a new branch from `main` with a meaningful name (`git checkout -b my_branch`). Do not modify the `main` branch directly.
3. Make your changes, ensure all unit tests pass, and add new tests to verify your changes.
4. Push your changes to your remote branch (`git push origin my_branch`).
5. Create a Pull Request on GitHub ([how to create a pull request](https://docs.github.com/en/pull-requests/collaborating-with-pull-requests/proposing-changes-to-your-work-with-pull-requests/creating-a-pull-request)).
6. After submitting the PR, you can find it under [Pull Requests](https://github.com/taosdata/taosgen/pulls). Click the link to view CI status. If it passes, you'll see “All checks have passed”. You can always click “Show all checks” -> “Details” for detailed logs.
7. After CI passes, you can check your PR's test coverage on [codecov](https://app.codecov.io/gh/taosdata/taosgen/pulls).

## 10. References
- [TDengine Official Website](https://www.tdengine.com/)
- [TDengine GitHub](https://github.com/taosdata/TDengine)

## 11. Appendix
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

### 11.1 Performance Benchmarks

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

## 12. License
[MIT License](./LICENSE)