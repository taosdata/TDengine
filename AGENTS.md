# AGENTS.md

This file provides guidance to AI agents when working with code in this repository.

## Project Overview

TDengine is an open-source, high-performance, cloud-native time-series database designed for IoT, Connected Cars, and Industrial IoT. It is written primarily in C and uses CMake as its build system.

## Code Organization

### Directory Structure

- `/source/` - Main source code directory
  - `/source/client/` - Client library (taos) implementation for connecting to TDengine server.
  - `/source/common/` - Common utilities and data structures shared across modules.
  - `/source/dnode/` - Data node implementation containing core server components.
    - `/source/dnode/bnode/` - Bnode (backup node) implementation.
    - `/source/dnode/mgmt/` - Node management and coordination logic.
    - `/source/dnode/mnode/` - Management node (mnode) for cluster metadata and DDL operations.
    - `/source/dnode/qnode/` - Query node for distributed query processing.
    - `/source/dnode/snode/` - Stream node for stream processing.
    - `/source/dnode/vnode/` - Virtual node (vnode) for data storage and query execution.
    - `/source/dnode/xnode/` - Cross-node communication utilities.
  - `/source/libs/` - Core libraries and modules.
    - `/source/libs/catalog/` - Metadata catalog management.
    - `/source/libs/executor/` - Query execution engine.
    - `/source/libs/function/` - Built-in functions (aggregations, scalar functions).
    - `/source/libs/index/` - Indexing implementations.
    - `/source/libs/nodes/` - Query plan node definitions and utilities.
    - `/source/libs/parser/` - SQL parser and AST definitions.
    - `/source/libs/planner/` - Query optimizer and planner.
    - `/source/libs/qworker/` - Query worker for distributed query execution.
    - `/source/libs/scalar/` - Scalar expression evaluation.
    - `/source/libs/scheduler/` - Query scheduler for coordinating distributed queries.
    - `/source/libs/sync/` - RAFT-based consensus and replication.
    - `/source/libs/tdb/` - Embedded key-value storage engine.
    - `/source/libs/tfs/` - TDengine file system abstraction.
    - `/source/libs/transport/` - RPC and network transport layer.
    - `/source/libs/wal/` - Write-ahead log implementation.
  - `/source/os/` - OS abstraction layer for cross-platform compatibility.
  - `/source/util/` - General utility functions.

- `/include/` - Public header files mirroring the source structure.

- `/tools/` - Auxiliary tools and utilities.
  - `/tools/shell/` - TDengine CLI (taos shell).
  - `/tools/taos-tools/` - taosBenchmark and taosdump utilities.
  - `/tools/tdgpt/` - TDgpt AI agent for time-series analytics.
  - `/tools/keeper/` - taosKeeper monitoring tool.

- `/test/` - New test framework based on Pytest.
  - `/test/cases/` - Test cases organized by feature category.
  - `/test/env/` - TDengine deployment configuration YAML files.
  - `/test/new_test_framework/` - Test framework utilities and common functions.
  - `/test/ci/` - CI test configuration and task files.
  - `/test/templates/` - Test templates and examples.

- `/docs/` - Documentation and examples.
- `/cmake/` - CMake modules and build configuration.
- `/contrib/` - Third-party dependencies.
- `/deps/` - Additional dependencies.

### Source Files

- When creating new source files (`.c`, `.h`), include the standard TDengine copyright header at the top:

```c
/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
```

### Code Style

TDengine uses clang-format for code formatting. The configuration is in `.clang-format` at the repository root. Key style points:

- Based on Google C++ style
- Indent width: 2 spaces
- Column limit: 120 characters
- Braces attach to control statements (no newline before `{`)
- Pointer alignment: Right (e.g., `int *ptr`)
- Use `true`/`false` for booleans, not `1`/`0`

Run clang-format before committing:

```bash
clang-format -i <source_file>
```

## Building

### Prerequisites

**Linux (Ubuntu):**

```bash
sudo apt-get install -y gcc cmake build-essential git libjansson-dev \
  libsnappy-dev liblzma-dev zlib1g-dev pkg-config
```

**macOS:**

```bash
brew install argp-standalone gflags pkgconfig
```

### Build Commands

Standard build:

```bash
mkdir debug && cd debug
cmake .. -DBUILD_TOOLS=true -DBUILD_CONTRIB=true
make
```

Or use the build script:

```bash
./build.sh
```

Build with tests enabled:

```bash
cmake .. -DBUILD_TOOLS=true -DBUILD_TEST=true -DBUILD_CONTRIB=true
make
```

Build with taosAdapter (Go required):

```bash
cmake .. -DBUILD_HTTP=false
make
```

Build with taosKeeper:

```bash
cmake .. -DBUILD_KEEPER=true
make
```

Build with Jemalloc:

```bash
cmake .. -DJEMALLOC_ENABLED=ON
make
```

### Installation

```bash
sudo make install
```

## Testing

TDengine uses a Pytest-based test framework located in the `/test/` directory. The framework supports flexible server deployment, YAML-based configuration, and generates Allure reports automatically.

> **Important:** The legacy `/tests/` directory is deprecated and will be removed. All new test cases MUST be added to the `/test/` directory using the new Pytest framework. Do NOT use the legacy TSIM framework.

### Prerequisites

```bash
cd test
pip3 install -r requirements.txt
```

Or install dependencies manually:

```bash
pip3 install pytest taospy taos-ws-py pandas psutil fabric2 requests faker \
  toml pyyaml allure-pytest pytest-xdist pytest-timeout
```

### Test Directory Structure

```text
test/
├── cases/                # Test cases organized by feature
│   ├── 01-DataTypes/     # Data type tests
│   ├── 02-Databases/     # Database operation tests
│   ├── 03-Tables/        # Table operation tests
│   ├── 06-DataIngestion/ # Data write tests
│   ├── 09-DataQuerying/  # Query tests
│   ├── 17-DataSubscription/ # TMQ subscription tests
│   ├── 18-StreamProcessing/ # Stream processing tests
│   ├── 70-Cluster/       # Cluster tests
│   ├── 82-UnitTest/      # Unit tests
│   └── ...
├── env/                  # Deployment configuration YAML files
├── new_test_framework/   # Framework utilities
│   └── utils/            # Common utilities (tdLog, tdSql, etool, etc.)
├── ci/                   # CI configuration
│   └── cases.task        # CI test task list
└── requirements.txt      # Python dependencies
```

### Running Tests

**Set environment variables (optional):**

```bash
cd test
source ./setenv_build.sh    # Use build binaries
source ./setenv_install.sh  # Use installed binaries
```

**Run a specific test file:**

```bash
cd test
pytest cases/01-DataTypes/test_datatype_bigint.py
```

**Run a specific test case:**

```bash
pytest cases/01-DataTypes/test_datatype_bigint.py::TestBigint::test_bigint_boundary
```

**Run tests with markers:**

```bash
pytest -m cluster
```

**Run with YAML configuration:**

```bash
pytest --yaml_file=ci_default.yaml cases/01-DataTypes/test_datatype_bigint.py
```

**Run with cluster options:**

```bash
pytest -N 3 -M 3 cases/70-Cluster/test_cluster.py  # 3 dnodes, 3 mnodes
```

### Common Pytest Options

- `-N <num>`: Number of dnodes to start in cluster
- `-M <num>`: Number of mnodes to create
- `-R`: Use RESTful connection
- `-A`: Address sanitizer mode
- `--replica <num>`: Set number of replicas
- `--clean`: Clean test environment before deploy
- `--skip_deploy`: Only run tests without starting TDengine
- `--skip_test`: Only deploy without running tests
- `-s`: Disable output capturing (show print statements)
- `--log-level=DEBUG`: Set log level
- `--alluredir=<dir>`: Generate Allure report

### Batch Run Tests

```bash
cd test
./start_run_test_list.sh                        # Run default test list
./start_run_test_list.sh path/to/test_list.txt  # Run custom test list
./stop_run_test_list.sh                         # Stop batch execution
```

Results are saved in `test_logs/` directory.

### Adding New Test Cases

1. **Create a new test file** in the appropriate `/test/cases/` subdirectory. File name must start with `test_`.

2. **Import required modules:**

```python
from new_test_framework.utils import tdLog, tdSql, etool
```

3. **Define test class and methods:**

```python
class TestNewFeature:

    def setup_class(cls):
        tdLog.debug(f"start to execute {__file__}")
        tdSql.prepare(dbname="test", drop=True, replica=cls.replicaVar)

    def test_feature(self):
        """Test feature description

        Detailed description of what this test verifies.

        Since: v3.3.0.0

        Labels: feature, query

        Jira: TD-12345

        History:
            - 2024-01-01 Author Name Created

        """
        tdSql.execute("CREATE DATABASE test_db")
        tdSql.query("SHOW DATABASES")
        tdSql.checkRows(1)
        tdSql.checkData(0, 0, "test_db")
```

4. **Add to CI** by editing `test/ci/cases.task`:

```text
,,y,.,./ci/pytest.sh pytest cases/<category>/test_new_feature.py
```

### Test Utilities

Common utilities available in `new_test_framework.utils`:

- `tdLog`: Logging utilities (`tdLog.debug()`, `tdLog.info()`, `tdLog.error()`)
- `tdSql`: SQL execution and validation
  - `tdSql.execute(sql)`: Execute SQL statement
  - `tdSql.query(sql)`: Execute query and store results
  - `tdSql.checkRows(num)`: Verify row count
  - `tdSql.checkData(row, col, value)`: Verify specific cell value
  - `tdSql.error(sql)`: Verify SQL throws an error
  - `tdSql.prepare(dbname, drop, replica)`: Setup test database
- `etool`: External tools
  - `etool.benchMark(command)`: Run taosBenchmark

### Unit Tests

Unit tests use Google Test framework. Test files are in `test/` subdirectories within source modules (e.g., `/source/os/test/`).

**Run a unit test:**

```bash
cd debug/build/bin
./osTimeTests
```

**Adding unit tests:**

1. Create test files in the appropriate source `test/` directory.
2. Update `CMakeLists.txt` to include the new test.
3. Build with `-DBUILD_TEST=true`.

### Test Reports

The framework generates Allure reports automatically. After test execution:

```bash
allure generate allure-results -o report_dir --clean
allure open report_dir
```

## Running TDengine

### Start the Server

**As a service (Linux):**

```bash
sudo systemctl start taosd
```

**As a service (macOS):**

```bash
sudo launchctl start com.tdengine.taosd
```

**Directly from build directory:**

```bash
./build/bin/taosd -c test/cfg
```

### Connect with CLI

```bash
taos
# Or from build directory:
./build/bin/taos -c test/cfg
```

## Pull Request Guidelines

### Branch Strategy

- `main` branch is for stable releases only - **do not submit PRs directly to main**.
- Submit all PRs to the development branch `3.0`.
- For documentation-only changes, prefix your branch name with `docs/`.

### Before Submitting

1. Ensure the code compiles without warnings (warnings are treated as errors).
2. Run relevant tests and ensure they pass.
3. Format code with clang-format.
4. Sign the [Contributor License Agreement (CLA)](https://cla-assistant.io/taosdata/TDengine).

### PR Process

1. Fork the repository and create a branch from `3.0`.
2. Make your changes and commit.
3. If fixing a bug, reference the GitHub issue number.
4. Create a pull request to merge into the `3.0` branch.

## Related Repositories

TDengine has separate repositories for language connectors:

- [JDBC Connector](https://github.com/taosdata/taos-connector-jdbc)
- [Go Connector](https://github.com/taosdata/driver-go)
- [Python Connector](https://github.com/taosdata/taos-connector-python)
- [Node.js Connector](https://github.com/taosdata/taos-connector-node)
- [C# Connector](https://github.com/taosdata/taos-connector-dotnet)
- [Rust Connector](https://github.com/taosdata/taos-connector-rust)

## Key Concepts

### Architecture Terms

- **dnode**: Data node - a single TDengine server instance.
- **mnode**: Management node - handles cluster metadata, user management, and DDL.
- **vnode**: Virtual node - a data storage and query unit within a dnode.
- **qnode**: Query node - dedicated node for query processing in compute-storage separation mode.
- **snode**: Stream node - handles stream computing tasks.

### Data Model

- **Database**: Contains multiple tables with shared configuration.
- **Super Table (STable)**: Template for creating subtables with the same schema.
- **Subtable**: Table created from a super table with unique tags.
- **Tags**: Metadata attributes for subtables used for filtering and grouping.

## Debugging Tips

1. Check logs in `/var/log/taos/` or the configured log directory.
2. Use `taos -s "SHOW DNODES"` to check cluster status.
3. Enable debug logging by modifying `taos.cfg`:

   ```text
   debugFlag 135
   ```

4. For core dumps, ensure `ulimit -c unlimited` is set before running.

## Documentation

- [Official Documentation](https://docs.tdengine.com)
- [API Reference](https://docs.tdengine.com/reference/)
- [TDgpt Documentation](./tools/tdgpt/README.md)
