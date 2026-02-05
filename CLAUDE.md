# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## About TDengine

TDengine is an open-source time-series database (TSDB) optimized for IoT, Connected Cars, and Industrial IoT. It's a high-performance, cloud-native, AI-powered distributed system written in C with some components in Go.

## Build System

TDengine uses CMake as its build system. The repository includes a convenience wrapper script `build.sh` that simplifies common build operations.

### Common Build Commands

**First-time build (quick start):**
```bash
./build.sh first-try
```

**Standard development workflow:**
```bash
# Generate build system (one-time setup)
./build.sh gen

# Build the project
./build.sh bld

# Install
./build.sh install

# Start services
./build.sh start

# Run tests
./build.sh test
```

**Build with specific options:**
```bash
# Generate with custom cmake options
./build.sh gen -DKEY:type=value

# Build specific target
./build.sh bld --target <target_name>

# List all available targets
./build.sh bld --target help
```

**Build configurations:**
- Default: Debug mode (`TD_CONFIG=Debug`)
- Release mode: `TD_CONFIG=Release ./build.sh gen && ./build.sh bld`

**Direct CMake usage:**
```bash
cmake -B debug -DCMAKE_BUILD_TYPE=Debug -DBUILD_TOOLS=true -DBUILD_TEST=true
cmake --build debug --config Debug -j$(nproc)
```

### Build Options

Key cmake options (see `cmake/options.cmake`):
- `BUILD_TEST`: Build unit tests using googletest
- `BUILD_TOOLS`: Build command-line tools
- `BUILD_KEEPER`: Build taosKeeper
- `BUILD_HTTP`: Build HTTP support
- `WEBSOCKET`: Enable WebSocket support
- `DCOVER=true`: Enable code coverage
- `TAOSADAPTER_GIT_TAG`: Specify taosadapter version/branch
- `TAOSADAPTER_BUILD_OPTIONS`: Go build options for taosadapter

## Repository Architecture

### High-Level Structure

```
TDengine/
├── source/          # Core source code
│   ├── client/      # Client library and connectors
│   ├── dnode/       # Data node implementation
│   │   ├── bnode/   # Business node (query processing)
│   │   ├── mnode/   # Management node (metadata)
│   │   ├── qnode/   # Query node
│   │   ├── snode/   # Stream node
│   │   └── vnode/   # Virtual node (storage and time-series data)
│   ├── libs/        # Core libraries
│   │   ├── catalog/ # Meta data management
│   │   ├── planner/ # Query planner
│   │   ├── executor/# Query execution
│   │   ├── parser/  # SQL parser
│   │   ├── tdb/     # Time-series database engine
│   │   ├── qworker/ # Query worker
│   │   └── ...      # Other core components
│   ├── common/      # Common utilities
│   ├── util/        # Utility functions
│   └── os/          # OS abstraction layer
├── tools/           # Command-line tools and services
│   ├── keeper/      # taosKeeper (log replication tool)
│   ├── shell/       # taos shell (CLI)
│   └── tdgpt/       # AI/ML agent features
├── include/         # Public headers
│   └── client/      # taos.h - client API
├── tests/           # Test suites
├── examples/        # Example code
└── cmake/           # Build configuration
```

### Key Components

**Data Node (dnode):** The core server process that handles data storage and query processing. Contains:
- **mnode**: Manages metadata (databases, tables, users)
- **vnode**: Virtual data nodes that store time-series data in TSDB files
- **qnode**: Handles query processing
- **snode**: Stream processing engine
- **bnode**: Business logic processing

**Core Libraries (libs/):**
- **parser**: Parses TDengine's SQL dialect (TAOS SQL)
- **planner**: Creates query execution plans
- **executor**: Executes query plans
- **catalog**: Manages metadata (tables, databases, schemas)
- **tdb**: Time-series database storage engine
- **index**: Index structures for fast queries
- **qworker**: Distributed query execution
- **function**: Built-in scalar and aggregate functions

**Client (client/):**
- Client library (`libtaos.so`/`.dylib`)
- C API (`include/client/taos.h`)
- Provides native protocol and REST API support

**Tools:**
- **taos**: Command-line shell/CLI
- **taosd**: Core database server daemon
- **taosadapter**: Go-based adapter for REST/gRPC/WebSocket connections
- **taosKeeper**: Log replication and recovery tool
- **taosdump**: Data export/import utility
- **tdgpt**: AI-powered time-series analysis agent

## Testing

TDengine has multiple testing frameworks:

### Unit Tests
Built with googletest framework. Located in `tests/` directory.

**Run specific unit test:**
```bash
cd debug/build/bin
./osTimeTests
```

**Run all unit tests:**
```bash
cd tests/unit-test/
bash test.sh -e 0
```

### System Tests
Python-based integration tests in `tests/pytest/` and `tests/parallel_test/`.

**Prerequisites:**
```bash
pip3 install pandas psutil fabric2 requests faker simplejson toml pexpect tzlocal distro decorator loguru hyperloglog taospy taos-ws-py
```

**Run system tests:**
```bash
cd tests
pytest pytest/  # Run pytest-based tests
```

### Run All Tests via build.sh
```bash
./build.sh test
```

### Coverage Reports
```bash
cd tests
bash setup-lcov.sh -v 1.16 && ./run_local_coverage.sh -b main -c task
```

## Development Workflow

### Code Organization Principles

1. **Node-based Architecture**: TDengine uses a multi-node architecture where different node types (mnode, vnode, qnode, etc.) handle different responsibilities

2. **Separation of Concerns**:
   - `source/libs/` contains reusable libraries with no external dependencies
   - `source/dnode/` orchestrates these libraries into a working system
   - `source/client/` provides the client interface

3. **Storage Engine (TDB)**: Located in `source/libs/tdb/`, handles low-level time-series data storage with compression and indexing

4. **Query Processing Flow**:
   - `parser` parses SQL into abstract syntax tree
   - `planner` creates logical and physical execution plans
   - `executor` runs the plan using `qworker` for distributed execution

### Branch Strategy

- `main`: Stable releases
- `3.0`: Development branch (create feature branches from here)
- Documentation changes: branch name must start with `docs/` to bypass CI tests

### Connector Development

Connector repositories are separate from core:
- [JDBC](https://github.com/taosdata/taos-connector-jdbc)
- [Go](https://github.com/taosdata/driver-go)
- [Python](https://github.com/taosdata/taos-connector-python)
- [Node.js](https://github.com/taosdata/taos-connector-node)
- [C#](https://github.com/taosdata/taos-connector-dotnet)
- [Rust](https://github.com/taosdata/taos-connector-rust)

Examples for each connector are in `docs/examples/`.

## Important Notes

- **No cross-compilation**: TDengine doesn't support cross-compilation environments
- **Go Requirements**: Building taosAdapter or taosKeeper requires Go 1.23+
- **Platform Support**: Server supports Linux/macOS; Windows support is TSDB-Enterprise only (v3.1.0.0+)
- **CPU Support**: X64/ARM64 natively; MIPS64, Alpha64, ARM32, RISC-V planned

## Configuration

Default configuration directory: `/etc/taos/` (Linux) or specified via `-c` flag

For development/testing, use: `-c test/cfg` to avoid conflicts with installed instances.

## Starting Services

**As daemon:**
```bash
./build.sh start    # Linux: systemctl, macOS: launchctl
```

**In foreground (for development):**
```bash
./build/bin/taosd -c test/cfg
```

**Connect CLI:**
```bash
./build/bin/taos -c test/cfg
```



**JDK 17 location:**
please do not set java home
```bash
~/.sdkman/candidates/java/17.0.17-tem/bin/
```

