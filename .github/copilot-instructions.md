# Copilot Instructions for TDengine

## Build Commands

TDengine uses CMake with an out-of-source build directory called `debug/`.

```bash
# Configure (generates makefiles in debug/)
./build.sh gen

# Build (uses all cores)
./build.sh bld

# Build a specific target
./build.sh bld --target shell
./build.sh bld --target taosd

# Install (requires sudo; stops running services first)
./build.sh install

# Release build
TD_CONFIG=Release ./build.sh gen
TD_CONFIG=Release ./build.sh bld

# Enable/disable features at configure time
./build.sh gen -DBUILD_TEST=true -DBUILD_TOOLS=true -DJEMALLOC_ENABLED=ON
```

Prerequisites: GCC 9.3.1+, CMake 3.18.0+, Go 1.23+ (for taosAdapter/taosKeeper).

## Test Commands

```bash
# Run a single C/C++ unit test (Google Test binary)
cd debug/build/bin && ./osTimeTests

# Run all unit tests
cd tests && bash unit-test/test.sh -e 0

# Run a single Python system test
cd tests/system-test && python3 ./test.py -f 2-query/avg.py

# Run all system tests
cd tests && ./run_all_ci_cases.sh -t python

# Run a single legacy TSIM test
cd tests/script && ./test.sh -f tsim/db/basic1.sim

# Run all CI test cases
cd tests && ./run_all_ci_cases.sh -b main

# CTest (from build dir)
./build.sh test
```

Python test dependencies: `pip3 install taospy taos-ws-py pandas psutil fabric2 requests faker simplejson toml pexpect tzlocal distro decorator loguru hyperloglog`

## Architecture

TDengine is a distributed time-series database written primarily in C. The server process is `taosd`.

### Node types

- **DNode** (`source/dnode/mgmt/`) — the physical daemon process. Each DNode hosts one or more logical nodes.
- **MNode** (`source/dnode/mnode/`) — meta node. Manages cluster metadata (databases, tables, users, vnodes) via an internal state store called SDB, replicated with RAFT.
- **VNode** (`source/dnode/vnode/`) — virtual node. Owns a shard of data. Handles writes (WAL + TSDB storage) and local query execution.
- **QNode** (`source/dnode/qnode/`) — query node. Executes distributed query fragments coordinated by the scheduler.
- **SNode** (`source/dnode/snode/`) — stream node. Runs continuous stream computations.

### Query pipeline

SQL text flows through: **parser** → **planner** → **scheduler** → **executor** (all under `source/libs/`). The planner runs client-side inside `libtaosnative.so`, not in `taosd`.

### Key libraries (`source/libs/`)

| Library | Role |
|---------|------|
| `parser` | SQL parsing and AST construction |
| `planner` | Query plan generation and optimization |
| `executor` | Physical operator execution |
| `scheduler` | Distributed query task coordination |
| `catalog` | Client-side metadata cache |
| `function` | Built-in aggregate/scalar/window functions |
| `scalar` | Scalar expression evaluation and constant folding |
| `nodes` | AST/plan node definitions and serialization |
| `transport` | RPC framework (libuv-based) |
| `sync` | RAFT consensus for replication |
| `wal` | Write-ahead log |
| `tdb` | Embedded B+tree key-value store |
| `tfs` | File system abstraction |
| `index` | Indexing (inverted index, SMA) |
| `new-stream` | Streaming engine |

### Other important directories

- `source/client/` — native C client library (`libtaos.so` / `libtaosnative.so`)
- `source/os/` — OS abstraction layer (threads, files, sockets, atomics)
- `source/util/` — low-level utilities (arrays, hashes, encoding, compression, logging)
- `source/common/` — shared type definitions and message serialization
- `tools/tdgpt/` — AI agent (Python; forecasting, anomaly detection)
- `tools/shell/` — the `taos` CLI
- `include/` — public headers mirroring `source/` structure

## Coding Conventions

### Naming

- **Structs**: `S` prefix + PascalCase — `STableKeyInfo`, `SSDataBlock`, `SRpcMsg`
- **Enums**: `E` prefix + PascalCase — `EOperatorType`, `ENodeType`
- **Functions**: module prefix + camelCase — `mndInitIdx()`, `parInsertStmt()`, `taosArrayPush()`
- **Pointer variables**: `p` prefix — `pPool`, `pData`, `pNode`, `pReq`
- **Macros/constants**: `UPPER_SNAKE_CASE` — `TSDB_CODE_SUCCESS`, `TSDB_TABLE_NAME_LEN`
- **Static (file-local) functions**: same camelCase style, declared `static` at file top

### Error handling

All functions that can fail return `int32_t` error codes. `TSDB_CODE_SUCCESS` (0) indicates success; negative values indicate errors.

```c
int32_t code = someFunction(args);
if (code != TSDB_CODE_SUCCESS) {
  return code;
}
```

Key error patterns:
- `terrno` — thread-local error code (set automatically by allocation helpers)
- After `taosMemoryCalloc`/`taosMemoryMalloc` failure, return `terrno` directly
- Use `goto _OVER` / `goto _err` for multi-step cleanup
- Error codes defined in `include/util/taoserror.h` via `TAOS_DEF_ERROR_CODE(mod, code)`
- Convert error code to string with `tstrerror(code)`

### Memory management

Use TDengine wrappers, not raw `malloc`/`free`:
- `taosMemoryCalloc()`, `taosMemoryMalloc()`, `taosMemoryRealloc()`
- `taosMemoryFree()`, `taosMemoryFreeClear()` (frees and NULLs the pointer)

### Formatting

Configured via `.clang-format` (Google-based):
- 2-space indentation, 120-column limit
- Braces attach to control statements (K&R style)
- Pointer alignment: right (`int32_t *pVar`)
- Tabs: never

### Logging

Module-prefixed log macros: `uError`/`uWarn`/`uInfo`/`uDebug` (util), `qError`/`qWarn` (query), `mError` (mnode), `vError` (vnode), etc.

### MNode table-driven registration

MNode modules follow a consistent pattern — register SDB encode/decode/insert/update/delete callbacks plus message handlers:

```c
int32_t mndInitXxx(SMnode *pMnode) {
  SSdbTable table = {
    .sdbType   = SDB_XXX,
    .keyType   = SDB_KEY_BINARY,
    .encodeFp  = (SdbEncodeFp)mndXxxActionEncode,
    .decodeFp  = (SdbDecodeFp)mndXxxActionDecode,
    .insertFp  = (SdbInsertFp)mndXxxActionInsert,
    .updateFp  = (SdbUpdateFp)mndXxxActionUpdate,
    .deleteFp  = (SdbDeleteFp)mndXxxActionDelete,
  };
  mndSetMsgHandle(pMnode, TDMT_MND_CREATE_XXX, mndProcessCreateXxxReq);
  return sdbSetTable(pMnode->pSdb, table);
}
```

## Testing Conventions

### Unit tests (C++)

Use Google Test. Test binaries are built to `debug/build/bin/`. Add new tests by:
1. Creating a `.cpp` file in the module's `test/` directory
2. Adding it to the module's `CMakeLists.txt` with `add_executable` + `add_test`

### System tests (Python)

Custom framework (not pytest). Each test file implements a class with `init()`, `run()`, and `stop()` methods:

```python
class TDTestCase:
    def init(self, conn, logSql, replicaVar=1):
        tdSql.init(conn.cursor())

    def run(self):
        tdSql.execute("create database db")
        tdSql.query("select * from information_schema.ins_databases")
        tdSql.checkRows(3)

    def stop(self):
        tdSql.close()

tdCases.addLinux(__file__, TDTestCase())
tdCases.addWindows(__file__, TDTestCase())
```

Key test utilities (imported from `tests/pytest/util/`):
- `tdSql` — execute SQL, check results (`query`, `execute`, `checkRows`, `checkData`, `error`)
- `tdLog` — logging (`info`, `debug`, `success`, `exit`)
- `tdDnodes` — manage TDengine server instances for testing

### CI integration

Add new system tests to `tests/parallel_test/cases.task`:
```
,,n,system-test, python3 ./test.py -f 0-others/my_new_test.py
```

### Legacy TSIM tests

TSIM is deprecated. New tests should use the Python system-test framework.

## Warnings Policy

Warnings are treated as errors in CI — all compiler warnings must be resolved before merge.
