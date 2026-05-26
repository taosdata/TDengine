---
name: tsdb-dev-add-sql-feature
description: "Guide the full-pipeline implementation of new SQL syntax or statement features in TDengine, covering parser (tokenizer, grammar, AST), planner (logical plan, physical plan), executor (operator), and tests. Use when adding a new SQL statement type, new keyword, new clause, or extending existing SQL syntax. Trigger keywords: new SQL, add syntax, parser, grammar, AST node, planner node, executor operator, lemon parser, sql.y, parAstCreater, planLogicCreater, planPhysiCreater."
metadata:
  author: yihao
  version: 1.0.0
  owner_team: engine
  compatibility:
    - repo: TDinternal/community
    - stages: parser → planner → executor → test
---

# tsdb-dev-add-sql-feature

End-to-end guide for implementing a new SQL feature in TDengine. Covers every file that must change from SQL text to runtime execution.

## When to Use

- Adding a new SQL statement type (e.g. `CREATE SOMETHING`, `ALTER SOMETHING`)
- Adding a new clause or keyword to an existing statement
- Extending query syntax (new window type, new join hint, new operator)
- Any work that touches more than one stage of the SQL pipeline

Do NOT use this skill for:
- Bug fixes in a single module (use `tsdb-dev-query-engine` or module-specific skills instead)
- Performance optimization of existing features

## Prerequisites

- TDinternal/community source tree available
- Build environment configured (cmake, gcc/clang)
- Familiarity with TDengine node type system (`ENodeType` in `tmsg.h`)

## Input

| 参数 | 必需 | 默认值 | 说明 |
|------|:----:|--------|------|
| `feature_description` | ✅ | — | SQL 语法描述，如 "ADD CREATE BNODE STATEMENT" |
| `feature_type` | ✅ | — | `new_stmt` / `new_clause` / `new_query_op` / `new_keyword` |
| `affected_modules` | ❌ | auto | 预期影响的模块列表，不提供则自动推断 |

> 若用户未提供必需参数，Agent 应主动询问。

## Pipeline Overview

TDengine SQL processing follows this fixed order:

```
SQL Text → Tokenizer → Grammar (lemon) → AST → Translator → Logical Plan → Physical Plan → Executor
```

Each stage maps to specific source files and must be modified in order.

## Steps

### Phase 1: Parser — Tokenizer & Keywords

**Goal:** Make the tokenizer recognize new keywords.

#### 1.1 Add keyword to keyword table

File: `source/libs/parser/src/parTokenizer.c`

Add entry to `keywordTable[]` array (kept sorted alphabetically):

```c
{"MY_KEYWORD",           TK_MY_KEYWORD},
```

#### 1.2 Define token constant

Token constants are auto-generated from `sql.y` by the lemon parser. Adding the keyword to `sql.y` (next step) automatically generates the `TK_MY_KEYWORD` constant. Do NOT define it manually.

If the keyword is a **hint** token (not a SQL statement keyword), add it manually in:

File: `include/common/ttokendef.h`

```c
#define TK_MY_HINT  616  // use next available number in the 600+ range
```

### Phase 2: Parser — Grammar Rules

**Goal:** Define how tokens combine into a valid syntax structure.

File: `source/libs/parser/inc/sql.y`

This is a lemon parser grammar file. Key patterns:

#### 2.1 New statement type

Add grammar rule at the appropriate location:

```lemon
// In sql.y
cmd ::= CREATE MY_KEYWORD NK_ID my_keyword_options.  {
  pCxt->pRootNode = createCreateMyKeywordStmt(pCxt, &A, B);
}
```

- `cmd` is the top-level rule entry point
- `NK_ID` is an identifier token, `NK_STRING` for strings, `NK_INTEGER` for numbers
- The action block calls an AST creation function

#### 2.2 New clause for existing statement

Add rules that extend existing statement patterns. Find the existing statement in `sql.y` and add alternatives.

#### 2.3 Rebuild the parser

After modifying `sql.y`, the lemon parser must be regenerated:

```bash
cd source/libs/parser/inc
lemon sql.y
```

This generates `taos_lemon_sql.c` and `taos_lemon_sql.h` (the token definitions).

### Phase 3: Parser — AST Node Definition

**Goal:** Define the C struct that represents the parsed statement.

#### 3.1 Add node type enum

File: `include/common/tmsg.h`

Add to `ENodeType` enum in the correct range:

```c
// Statement nodes start at 100
QUERY_NODE_CREATE_MY_KEYWORD_STMT,
```

Rules for enum placement:
- Syntax/expression nodes: range 1–99
- Statement nodes: range 100–399
- Show statement nodes: range 400+
- Logic plan nodes: range 1000+
- Physical plan nodes: range 1100+

#### 3.2 Define the AST struct

Choose the correct header based on statement category:

| Category | Header file | Example struct |
|----------|-------------|----------------|
| DDL (CREATE/DROP/ALTER) | `include/libs/nodes/cmdnodes.h` | `SCreateDatabaseStmt` |
| DML (SELECT/INSERT/DELETE) | `include/libs/nodes/querynodes.h` | `SSelectStmt` |
| Utility (SHOW/DESCRIBE/KILL) | `include/libs/nodes/cmdnodes.h` | `SShowStmt` |

```c
typedef struct SCreateMyKeywordStmt {
  ENodeType type;
  char      name[TSDB_DB_FNAME_LEN];
  bool      ignoreExists;
  // ... fields specific to this statement
} SCreateMyKeywordStmt;
```

#### 3.3 Register node operations

File: `source/libs/nodes/src/nodesCodeFuncs.c`

Add entry to the node-to-code array so the node type has a code string.

File: `source/libs/nodes/src/nodesMsgFuncs.c`

Add serialization/deserialization for the new node type (binary format for client-server transport).

File: `source/libs/nodes/src/nodesCloneFuncs.c`

Add deep-clone support.

File: `source/libs/nodes/src/nodesEqualFuncs.c`

Add equality comparison if needed.

File: `source/libs/nodes/src/nodesToSQLFuncs.c`

Add SQL text generation (for `SHOW CREATE` and explain output) if applicable.

### Phase 4: Parser — AST Creation Function

**Goal:** Implement the function called from grammar action blocks.

File: `source/libs/parser/src/parAstCreater.c`

```c
SNode* createCreateMyKeywordStmt(SAstCreateContext* pCxt, SToken* pName, SNode* pOptions) {
  CHECK_PARSER_STATUS(pCxt);
  SCreateMyKeywordStmt* pStmt = (SCreateMyKeywordStmt*)nodesMakeNode(QUERY_NODE_CREATE_MY_KEYWORD_STMT);
  CHECK_OUT_OF_MEM(pStmt);
  COPY_STRING_FORM_TOKEN(pStmt->name, pName);
  pStmt->pOptions = pOptions;
  return (SNode*)pStmt;
}
```

Declaration goes in:

File: `source/libs/parser/inc/parAst.h`

```c
SNode* createCreateMyKeywordStmt(SAstCreateContext* pCxt, SToken* pName, SNode* pOptions);
```

### Phase 5: Parser — Semantic Validation

**Goal:** Validate the AST (check names, permissions, references).

File: `source/libs/parser/src/parTranslater.c`

Add a case in the translator dispatch:

```c
case QUERY_NODE_CREATE_MY_KEYWORD_STMT:
  code = translateCreateMyKeywordStmt(pCxt, (SCreateMyKeywordStmt*)pNode);
  break;
```

Implement `translateCreateMyKeywordStmt` to:
- Validate identifier names
- Check database/table existence via catalog
- Resolve cross-references
- Build the request message struct

### Phase 6: Planner — Logical Plan

**Goal:** Convert the validated AST into a logical plan node (only needed for query-type features).

File: `source/libs/planner/src/planLogicCreater.c`

Add case in the logic plan creation dispatch:

```c
case QUERY_NODE_CREATE_MY_KEYWORD_STMT:
  return createLogicPlanMyKeyword(pCxt, pSelectStmt);
```

For DDL/DML statements that don't need a query plan, this step may be skipped — the translator directly produces a command message.

### Phase 7: Planner — Physical Plan

**Goal:** Convert logical plan to physical plan with execution details.

File: `source/libs/planner/src/planPhysiCreater.c`

Add case in physical plan creation dispatch:

```c
case QUERY_NODE_LOGIC_PLAN_MY_KEYWORD:
  return createPhysiPlanMyKeyword(pCxt, pLogicNode, pPhyNode);
```

### Phase 8: Executor — Operator Implementation

**Goal:** Implement the runtime execution logic.

For new physical plan nodes, register an operator:

File: `source/libs/executor/src/executor.c` (operator dispatch table)

```c
case QUERY_NODE_PHYSICAL_PLAN_MY_KEYWORD:
  return doCreateMyKeywordOperatorInfo(pTaskInfo, pPhysiNode, pHandle);
```

Implement the operator in a new or existing source file under `source/libs/executor/src/`.

Operator lifecycle:
1. `createXxxOperatorInfo` — initialization, resource allocation
2. Operator main function — called repeatedly, returns data blocks
3. `destroyXxxOperatorInfo` — cleanup

### Phase 9: Client-Server Message

**Goal:** Define the network message for client → server communication.

File: `include/common/tmsg.h`

Define request/response message structs:

```c
typedef struct SCreateMyKeywordReq {
  char name[TSDB_DB_FNAME_LEN];
  bool ignoreExists;
} SCreateMyKeywordReq;

typedef struct SCreateMyKeywordRsp {
  int8_t unused;
} SCreateMyKeywordRsp;
```

Add message type to the enum (`EMsgType`) and implement serialization in `source/libs/transport/`.

Server-side handling goes in `source/dnode/mnode/` (for cluster-level DDL) or `source/dnode/vnode/` (for vnode-level operations).

### Phase 10: Testing

#### 10.1 Parser unit tests

Test SQL parsing produces correct AST:

```bash
# Run parser tests
pytest test/cases/01-DML/ --parser
```

Or add specific test SQL files under `test/cases/`.

#### 10.2 Integration tests

Add `.sql` test files in the test case directory structure:

```
test/cases/
  00-TDengine/
  01-DML/
  02-DDL/       ← DDL statements go here
  ...
```

Each test file contains SQL statements and expected results.

#### 10.3 Build & smoke test

```bash
mkdir -p debug && cd debug
cmake .. -DBUILD_TEST=true -DBUILD_TOOLS=true -DGRANT_VALUE=365
make -j$(nproc) && make install

# Start taosd and run test
taos -s "CREATE MY_KEYWORD test1"
```

## Output

Return results in this structure:

```yaml
feature: <description>
feature_type: <new_stmt|new_clause|new_query_op|new_keyword>
files_modified:
  - path: <file path>
    change: <what was added/modified>
    stage: <tokenizer|grammar|ast|translator|planner|executor|message>
validation:
  parser_test: <passed|failed|not-run>
  integration_test: <passed|failed|not-run>
  build: <passed|failed|not-run>
open_risks:
  - <risk or follow-up>
```

Acceptance criteria:

- SQL syntax is accepted without parse error
- AST node is correctly constructed and validated
- Request message reaches the correct server handler
- End-to-end behavior matches the specification
- Test cases cover normal, boundary, and error scenarios

## Core Source Map

| Stage | Key Files | Build Target |
|-------|-----------|-------------|
| Tokenizer | `source/libs/parser/src/parTokenizer.c` | client shared lib |
| Grammar | `source/libs/parser/inc/sql.y` | client shared lib |
| AST Creation | `source/libs/parser/src/parAstCreater.c`, `source/libs/parser/inc/parAst.h` | client shared lib |
| AST Nodes (DDL) | `include/libs/nodes/cmdnodes.h` | both |
| AST Nodes (DML) | `include/libs/nodes/querynodes.h` | both |
| Node Type Enum | `include/common/tmsg.h` | both |
| Node Ops | `source/libs/nodes/src/nodes*.c` | both |
| Semantic Validation | `source/libs/parser/src/parTranslater.c` | client shared lib |
| Logical Plan | `source/libs/planner/src/planLogicCreater.c` | client shared lib |
| Physical Plan | `source/libs/planner/src/planPhysiCreater.c` | client shared lib |
| Executor | `source/libs/executor/src/` | taosd (server) |
| Messages | `include/common/tmsg.h`, `source/libs/transport/` | both |
| Server Handler | `source/dnode/mnode/` or `source/dnode/vnode/` | taosd (server) |
| Hint Tokens | `include/common/ttokendef.h` | both |

## Safety

- Do not modify `sql.y` without understanding the lemon parser syntax — incorrect grammar rules cause shift/reduce conflicts
- Do not skip the node operation registration (Phase 3.3) — missing registration causes serialization crashes
- Do not add physical plan nodes without corresponding executor operator implementation
- Do not reuse existing `ENodeType` values — each value must be unique
- Do not modify the `QUERY_NODE_SHOW_*` range order — it must align with `sysTableShowAdapter`
- Do not skip `planValidator.c` updates when adding new plan node types
- For destructive operations, require explicit user confirmation before execution
- When adding error codes, follow `tsdb-dev-add-error-code` skill

## Examples

**用户说：** "帮我加一个 CREATE BNODE 语句"

**Agent 行为：**
1. 确认 `BNODE` 关键字已在 `parTokenizer.c` keywordTable 中
2. 在 `sql.y` 中添加 `cmd ::= CREATE BNODE ...` 语法规则
3. 在 `tmsg.h` 中添加 `QUERY_NODE_CREATE_BNODE_STMT` 枚举值
4. 在 `cmdnodes.h` 中定义 `SCreateBnodeStmt` 结构体
5. 在 `parAstCreater.c` 中实现 `createCreateBnodeStmt`
6. 在 `parAst.h` 中声明该函数
7. 在 `parTranslater.c` 中添加语义校验
8. 在 `tmsg.h` 中定义 `SCreateBnodeReq/Rsp`
9. 在 mnode 中实现服务端处理
10. 编写测试用例并验证

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-dev-add-sql-feature version=0.1.0 author=yihao`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
