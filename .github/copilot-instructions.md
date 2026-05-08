# Copilot Instructions for TDengine

## Build Commands

```bash
# Generate build (Debug, with tests enabled)
./build.sh gen

# Build
./build.sh bld

# Install (required before running tests)
./build.sh install

# Quick alternative (from existing debug/ directory)
cd debug && make -j4 && make install

# Build with specific cmake options
cmake -B debug -DBUILD_TEST=true -DBUILD_TOOLS=true -DBUILD_CONTRIB=true
cmake --build debug -j$(nproc)
```

## Testing

```bash
# Run all unit tests (Google Test based, C++)
cd debug/build/bin && ./osTimeTests  # single unit test binary

# Run a single Python system test
cd tests/system-test && python3 ./test.py -f 2-query/avg.py

# Run a single legacy TSIM test
cd tests/script && ./test.sh -f tsim/db/basic1.sim

# Run all CI cases
cd tests && ./run_all_ci_cases.sh -b main
```

## Architecture

TDengine is a distributed time-series database. The core is written in C with Python-based system tests.

### Source Layout (`source/`)

- **client/** – Client library (libtaos), CLI (`taos`), stmt2 API
- **dnode/** – Data node process (taosd)
  - **mnode/** – Management node (metadata, DDL, cluster coordination)
  - **vnode/** – Virtual node (data storage & query execution)
    - `meta/` – Table metadata storage
    - `tsdb/` – Time-series data engine
    - `tq/` – Message queue (subscription/TMQ)
  - **qnode/** – Query node (distributed query execution)
- **libs/** – Shared libraries used across components
  - **parser/** – SQL parsing & semantic translation (`parTranslater.c` is the main translator)
  - **planner/** – Query plan generation (logic → physi → split → scale-out)
  - **executor/** – Physical plan operators (scan, join, aggregate, etc.)
  - **nodes/** – AST/plan node definitions, clone/serialize/traverse utilities
  - **catalog/** – Metadata cache between client and mnode
  - **transport/** – RPC framework
  - **sync/** – Raft consensus
  - **function/** – Built-in and UDF function implementations
- **common/** – Shared data types, time utilities, message definitions
- **os/** – OS abstraction layer
- **util/** – General utilities (hash, array, queue, etc.)

### Headers (`include/`)

- `include/common/tmsg.h` – All inter-node message types and `ENodeType` enum
- `include/libs/nodes/` – Node type definitions:
  - `nodes.h` – Base node, list macros (FOREACH, WHERE_EACH, etc.)
  - `querynodes.h` – AST expression/statement nodes
  - `plannodes.h` – Logical and physical plan nodes
  - `cmdnodes.h` – DDL command nodes

### Query Pipeline

SQL → Parser (`parAstCreater.c`) → Translator (`parTranslater.c`) → Logic Plan (`planLogicCreater.c`) → Optimizer (`planOptimizer.c`) → Physical Plan (`planPhysiCreater.c`) → Splitter (`planSpliter.c`) → Scale Out (`planScaleOut.c`) → Executor operators

### Tools (`tools/`)

- **tdgpt/** – AI agent for time-series analytics (Python)
- **shell/** – Interactive CLI (taos)
- **taos-tools/** – taosBenchmark, taosdump (submodule)

## Key Conventions

### Error Handling Pattern
Functions return `int32_t` error codes. Use `TSDB_CODE_SUCCESS` (0) for success. Common pattern:
```c
int32_t code = TSDB_CODE_SUCCESS;
// ... operations ...
if (TSDB_CODE_SUCCESS != code) {
  goto _end;
}
```

### Node System
When adding a new node type:
1. Add enum to `ENodeType` in `include/common/tmsg.h`
2. Define struct in the appropriate header (`querynodes.h`, `plannodes.h`, or `cmdnodes.h`)
3. Add clone logic in `nodesCloneFuncs.c` (use `COPY_SCALAR_FIELD`, `CLONE_NODE_FIELD`, etc.)
4. Add serialization in `nodesCodeFuncs.c`
5. Add creation in `nodesUtilFuncs.c`
6. Add name mapping in `nodesCodeFuncs.c` (`nodesNodeName()`)

### Naming Conventions
- Prefix: `t` or `taos` for public APIs, `td` for internal types
- Structs: `S` prefix (e.g., `SLogicNode`, `SSelectStmt`)
- Enums: `E` prefix (e.g., `ENodeType`, `EDataOrderLevel`)
- Pointer params: `p` prefix (e.g., `pNode`, `pCxt`)
- Output params: double-pointer `pp` prefix or single pointer with comment

### Code Style
- Configured via `.clang-format` (Google-based, 2-space indent, 120 column limit)
- Braces on same line (Attach style)
- Pointer alignment: right-aligned (`char *p`)

### Test Integration
To add a new test case to CI, edit `tests/parallel_test/cases.task`:
```
#priority,rerunTimes,Run with Sanitizer,casePath,caseCommand
,,n,system-test, python3 ./test.py -f 2-query/your_test.py
```
