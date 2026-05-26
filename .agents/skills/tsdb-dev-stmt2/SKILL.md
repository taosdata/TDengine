---
name: tsdb-dev-stmt2
description: "生成 TDengine STMT2 参数绑定代码（C/C++）。适用场景：用户需要使用 taos_stmt2_* API 进行高性能批量写入或参数化查询，涉及超级表、子表、多表批量绑定、同步/异步执行、交织写入等。触发关键词：stmt2、参数绑定、批量写入、交织写入、interlace、taos_stmt2_bind_param、TAOS_STMT2_BIND、TAOS_STMT2_BINDV、parameter binding、batch insert。"
metadata:
  author: Mario Peng
  version: 1.0.0
  owner_team: engine
---

# TDengine STMT2 参数绑定

## When to Use

- 用户需要使用 TDengine C/C++ STMT2 API（`taos_stmt2_*`）进行**参数化 INSERT 或 SELECT**
- 需要对**超级表多子表批量写入**（一次 bind 多张子表）
- 需要**异步执行**（`asyncExecFn` 回调）
- 用户提到 `taos_stmt2_bind_param`、`TAOS_STMT2_BIND`、`TAOS_STMT2_BINDV` 等关键词
- 需要在写入前通过 `taos_stmt2_get_fields` 获取字段元信息

## Prerequisites

- 已安装 TDengine 客户端库（`libtaos.so` / `libtaos.dylib`）并可链接 `-ltaos`
- 头文件 `taos.h` 可用
- TDengine 服务端已启动，连接信息（host/user/password/port）已知

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-dev-stmt2 version=1.1.0 author=Mario Peng`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

## Input

| 参数 | 必需 | 默认值 | 说明 |
|------|:----:|--------|------|
| `language` | ❌ | `C` | 目标语言：`C` 或 `C++` |
| `sql` | ✅ | — | 绑定语句，含 `?` 占位符，如 `INSERT INTO ? USING meters TAGS(?,?) VALUES(?,?,?,?)` 或 `INSERT INTO meters(tbname,ts,current,groupId) VALUES(?,?,?,?)` |
| `table_schema` | ✅ | — | 建表语句（完整 DDL），如 `CREATE STABLE meters (ts TIMESTAMP, current FLOAT, voltage INT) TAGS (groupId INT, location BINARY(24))` |
| `exec_mode` | ❌ | `sync` | 执行模式：`sync`（同步）或 `async`（异步，需提供回调函数） |
| `operation` | ❌ | `insert` | 操作类型：`insert` 或 `select` |
| `sql_style` | ❌ | `new` | SQL 风格：`new`（列名列表，含 tbname）或 `old`（`INSERT INTO ? USING stb TAGS(...)`） |
| `num_subtables` | ❌ | `1` | 子表总数（每次 bind 覆盖的子表数量） |
| `total_rows` | ❌ | `10` | 每张子表的总写入行数 |
| `batch_rows` | ❌ | `total_rows` | 每批次（每次 `taos_stmt2_exec`）写入的行数；若小于 `total_rows` 则循环多次执行 |
| `interlace` | ❌ | `false` | 是否启用交织写入模式（interlace）：`true` 表示每次 bind 依次向各子表写入固定行数再循环，`false` 表示每张子表一次性写完所有行 |

> 若用户未提供 `sql` 或 `table_schema`，Agent 应主动询问。`batch_rows` 与 `total_rows` 共同决定循环次数：`cycles = ceil(total_rows / batch_rows)`。

## Steps

1. **理解需求**
   - 确认 `sql`（绑定语句）、`table_schema`（建表 DDL）、`exec_mode`、`sql_style`
   - 确认 `num_subtables`、`total_rows`、`batch_rows`、`interlace` 参数
   - 若为 `select`，确认 WHERE 条件中的占位参数类型

2. **生成连接与建表代码**
   - `taos_connect(host, user, password, db, port)` 建立连接
   - 用 `taos_query` 执行 `CREATE DATABASE` / `CREATE STABLE`

3. **生成 STMT2 初始化与 Prepare**

   **异步回调函数类型（`__taos_async_fn_t`）：**
   ```c
   // 定义于 taos.h
   typedef void (*__taos_async_fn_t)(void *param, TAOS_RES *res, int code);
   // param : 用户自定义上下文指针（透传给回调）
   // res   : 查询结果句柄（可用于 taos_fetch_row 等；非查询场景可为 NULL）
   // code  : 错误码，0 表示成功，非 0 时用 taos_errstr(res) / taos_stmt2_error(stmt) 获取描述
   ```

   **TAOS_STMT2_OPTION 结构字段说明：**
   ```c
   typedef struct TAOS_STMT2_OPTION {
     int64_t           reqid;               // 请求 ID（0 = 自动生成）
     bool              singleStbInsert;     // true：所有行写入同一超级表（性能优化）
     bool              singleTableBindOnce; // true：每次 bind 只绑定一张子表
     __taos_async_fn_t asyncExecFn;         // 异步执行回调（NULL = 同步模式）
     void             *userdata;            // 传递给 asyncExecFn 的用户上下文
   } TAOS_STMT2_OPTION;
   ```

   ```c
   // 同步模式：asyncExecFn = NULL
   TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};

   // 异步模式：提供回调函数（taos_stmt2_exec 将异步执行并在完成后调用该回调）
   // TAOS_STMT2_OPTION option = {0, true, true, myAsyncExecCb, &myCtx};

   TAOS_STMT2 *stmt2 = taos_stmt2_init(taos, &option);

   // 新风格 SQL（推荐）：
   // INSERT INTO stb(tbname, col1, col2, tag1, tag2) VALUES(?,?,?,?,?)
   // 旧风格 SQL：
   // INSERT INTO ? USING stb TAGS(?,?) VALUES(?,?,?)
   taos_stmt2_prepare(stmt2, sql, 0);
   ```

4. **（可选）获取字段元信息**
   ```c
   int fieldNum = 0;
   TAOS_FIELD_ALL *pFields = NULL;
   taos_stmt2_get_fields(stmt2, &fieldNum, &pFields);
   // 使用 pFields[i].name / .type / .field_type / .bytes
   taos_stmt2_free_fields(stmt2, pFields);
   ```

5. **分配并填充绑定数据**

   **TAOS_STMT2_BIND 结构字段说明：**
   ```c
   typedef struct {
     int     buffer_type;  // TSDB_DATA_TYPE_* 常量
     void   *buffer;       // 数据指针（列式：指向该列所有行的连续内存）
     int32_t *length;      // 各行数据长度数组（定长类型可为 NULL）
     char   *is_null;      // NULL 标记数组（无 NULL 时可为 NULL）
     int     num;          // 行数（tag 绑定时填 0 或 1）
   } TAOS_STMT2_BIND;
   ```

   **TAOS_STMT2_BINDV 结构字段说明：**
   ```c
   typedef struct {
     int               count;      // 子表数量
     char            **tbnames;    // 子表名数组（指针数组，每元素是一个表名字符串）
     TAOS_STMT2_BIND **tags;       // tag 绑定数组（每元素是该子表的 tag bind 数组）
     TAOS_STMT2_BIND **bind_cols;  // 列绑定数组（每元素是该子表的列 bind 数组）
   } TAOS_STMT2_BINDV;
   ```

   **常用数据类型常量（来自 `taos.h`）：**
   | SQL 类型 | 常量 | 值 | C 类型 | 字节 |
   |----------|------|----|--------|------|
   | NULL | `TSDB_DATA_TYPE_NULL` | 0 | — | 1 |
   | BOOL | `TSDB_DATA_TYPE_BOOL` | 1 | `int8_t` | 1 |
   | TINYINT | `TSDB_DATA_TYPE_TINYINT` | 2 | `int8_t` | 1 |
   | SMALLINT | `TSDB_DATA_TYPE_SMALLINT` | 3 | `int16_t` | 2 |
   | INT | `TSDB_DATA_TYPE_INT` | 4 | `int32_t` | 4 |
   | BIGINT | `TSDB_DATA_TYPE_BIGINT` | 5 | `int64_t` | 8 |
   | FLOAT | `TSDB_DATA_TYPE_FLOAT` | 6 | `float` | 4 |
   | DOUBLE | `TSDB_DATA_TYPE_DOUBLE` | 7 | `double` | 8 |
   | VARCHAR/BINARY | `TSDB_DATA_TYPE_BINARY`（=`TSDB_DATA_TYPE_VARCHAR`） | 8 | `char*` | 变长，需提供 `length` |
   | TIMESTAMP | `TSDB_DATA_TYPE_TIMESTAMP` | 9 | `int64_t` | 8，单位由数据库精度决定（ms/us/ns） |
   | NCHAR | `TSDB_DATA_TYPE_NCHAR` | 10 | `char*`（UTF-8） | 变长，需提供 `length` |
   | TINYINT UNSIGNED | `TSDB_DATA_TYPE_UTINYINT` | 11 | `uint8_t` | 1 |
   | SMALLINT UNSIGNED | `TSDB_DATA_TYPE_USMALLINT` | 12 | `uint16_t` | 2 |
   | INT UNSIGNED | `TSDB_DATA_TYPE_UINT` | 13 | `uint32_t` | 4 |
   | BIGINT UNSIGNED | `TSDB_DATA_TYPE_UBIGINT` | 14 | `uint64_t` | 8 |
   | JSON | `TSDB_DATA_TYPE_JSON` | 15 | `char*`（JSON 字符串） | 变长，需提供 `length` |
   | VARBINARY | `TSDB_DATA_TYPE_VARBINARY` | 16 | `char*` | 变长，需提供 `length` |
   | BLOB | `TSDB_DATA_TYPE_BLOB` | 18 | `char*` | 变长，需提供 `length` |
   | GEOMETRY | `TSDB_DATA_TYPE_GEOMETRY` | 20 | `char*`（WKB 格式） | 变长，需提供 `length` |

   > **注意**：变长类型（VARCHAR、NCHAR、VARBINARY、BLOB、GEOMETRY、JSON）必须提供 `length` 数组（`int32_t*`），每个元素为对应行的字节长度；定长类型的 `length` 可传 `NULL`。

6. **调用 bind 与 exec（含批次/交织逻辑）**

   **非交织模式**（`interlace=false`，默认）：每张子表一次性写完全部 `total_rows` 行：
   ```c
   // 每次循环写一批 batch_rows 行，循环 ceil(total_rows/batch_rows) 次
   for (int cycle = 0; cycle < num_cycles; cycle++) {
     // 填充各子表的 bind_cols 数据（batch_rows 行）
     TAOS_STMT2_BINDV bindv = {num_subtables, tbnames, tags, params};
     taos_stmt2_bind_param(stmt2, &bindv, -1);
     int affected = 0;
     taos_stmt2_exec(stmt2, &affected);
   }
   ```

   **交织模式**（`interlace=true`）：依次向每张子表写入 `batch_rows` 行，循环直到完成 `total_rows`：
   ```c
   // 每次循环仅向单张子表写 batch_rows 行，在各子表间轮询
   for (int cycle = 0; cycle < num_cycles; cycle++) {
     for (int t = 0; t < num_subtables; t++) {
       TAOS_STMT2_BINDV bindv = {1, &tbnames[t], &tags[t], &params[t]};
       taos_stmt2_bind_param(stmt2, &bindv, -1);
       taos_stmt2_exec(stmt2, NULL);
     }
   }
   ```

   **异步模式——方式 A：`asyncExecFn`（option 回调，exec 异步）**
   `taos_stmt2_exec` 提交后立即返回，底层完成后调用 `asyncExecFn`；bind 仍是同步的：
   ```c
   // 回调示例
   void myExecCb(void *param, TAOS_RES *res, int code) {
     MyCtx *ctx = (MyCtx *)param;
     if (code != 0) printf("exec error: %s\n", taos_errstr(res));
     tsem_post(&ctx->sem);  // 通知主线程
   }

   MyCtx ctx = {0};
   tsem_init(&ctx.sem, 0, 0);
   TAOS_STMT2_OPTION option = {0, true, true, myExecCb, &ctx};
   TAOS_STMT2 *stmt2 = taos_stmt2_init(taos, &option);
   // ... prepare / bind_param ...
   taos_stmt2_exec(stmt2, NULL);   // 异步提交，立即返回
   tsem_wait(&ctx.sem);            // 等待回调完成
   ```

   **异步模式——方式 B：`taos_stmt2_bind_param_a`（bind 也异步）**
   bind 和 exec 均异步执行，回调在 bind+exec 全部完成后触发（适合高吞吐流水线）：
   ```c
   // 函数签名
   int taos_stmt2_bind_param_a(TAOS_STMT2 *stmt, TAOS_STMT2_BINDV *bindv,
                               int32_t col_idx, __taos_async_fn_t fp, void *param);
   // fp    : 同 __taos_async_fn_t，bind+exec 完成后回调
   // param : 透传给 fp 的用户上下文
   // 返回值：0 = 成功提交异步任务，非 0 = 提交失败（同步返回错误）

   // 回调示例
   void myBindExecCb(void *param, TAOS_RES *res, int code) {
     MyCtx *ctx = (MyCtx *)param;
     if (code != 0) printf("bind+exec error: %s\n", taos_errstr(res));
     tsem_post(&ctx->sem);
   }

   // option 中 asyncExecFn 设为 NULL（方式 B 不依赖 option 回调）
   TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
   TAOS_STMT2 *stmt2 = taos_stmt2_init(taos, &option);
   // ... prepare ...
   MyCtx ctx = {0};
   tsem_init(&ctx.sem, 0, 0);
   taos_stmt2_bind_param_a(stmt2, &bindv, -1, myBindExecCb, &ctx);
   tsem_wait(&ctx.sem);  // 等待 bind+exec 完成
   ```

   > **注意**：`taos_stmt2_bind_param_a` 与 `asyncExecFn` 不可同时使用于同一 stmt；
   > 同一时刻只允许一个异步 bind 任务在执行，重叠调用会返回 `TSDB_CODE_TSC_STMT_API_ERROR`。

   > 第三个参数 `-1` 表示全列绑定；`affected_rows` 为实际插入行数（SELECT 或异步时可传 `NULL`）。

7. **SELECT 场景：取回结果**
   ```c
   TAOS_RES *pRes = taos_stmt2_result(stmt2);
   TAOS_ROW row;
   while ((row = taos_fetch_row(pRes))) {
     // 处理每行数据
   }
   taos_free_result(pRes);
   ```

8. **错误处理**
   - 所有 `taos_stmt2_*` 调用返回非 0 时，用 `taos_stmt2_error(stmt2)` 获取错误描述
   - 示例：
     ```c
     if (code != 0) {
       printf("Error: %s\n", taos_stmt2_error(stmt2));
       taos_stmt2_close(stmt2);
       exit(EXIT_FAILURE);
     }
     ```

9. **资源释放**
   ```c
   taos_stmt2_free_fields(stmt2, pFields);  // 若调用了 get_fields
   taos_stmt2_close(stmt2);
   taos_close(taos);
   taos_cleanup();
   ```

## Output

生成完整可编译的 C/C++ 源文件，包含：
- 连接与建表逻辑
- `prepareBindData` / `freeBindData` 辅助函数（内存分配与释放）
- `insertData` / `queryData` 主函数（含完整错误检查）
- `main` 函数（含连接、建表、插入/查询、关闭）
- 编译命令注释，例如：`// gcc -o demo demo.c -ltaos`

验收标准：
- 编译无错误（`gcc` / `g++` 链接 `-ltaos`）
- 所有 `taos_stmt2_*` 调用均有错误检查
- 所有动态分配内存均有对应 `free`

## Examples

**用户说：** "帮我写一个用 stmt2 批量写入 meters 超级表的 C 代码，10 张子表每张 10 行"

**Agent 行为：**
1. 确认表结构（如未提供，询问列和 tag 定义）
2. 生成完整 C 文件，包含：
   - `CREATE STABLE power.meters (ts TIMESTAMP, current FLOAT, voltage INT, phase FLOAT) TAGS(groupId INT, location BINARY(24))`
   - `prepareBindData`：为 10 张子表分配 `TAOS_STMT2_BIND` 数组
   - `insertData`：`taos_stmt2_init` → `prepare` → `bind_param` → `exec` → `close`
3. 提示编译命令：`gcc -o demo demo.c -ltaos`

---

**用户说：** "用 stmt2 写一个参数化 SELECT，按时间范围查询"

**Agent 行为：**
1. SQL 使用 `SELECT * FROM stb WHERE ts > ? AND ts < ? LIMIT ?`
2. 绑定 3 个参数（2 个 TIMESTAMP + 1 个 INT）
3. 执行后用 `taos_stmt2_result` + `taos_fetch_row` 遍历结果

## Safety

- 禁止在生成代码中硬编码真实密码或 token；示例中用 `"taosdata"` 等占位符
- 禁止生成 `DROP DATABASE`、`DROP TABLE` 等破坏性 DDL，除非用户明确要求且加 `IF EXISTS` 保护
- 生成的内存分配代码必须包含 NULL 检查与对应 `free`，避免内存泄漏
- 不得将用户提供的表名/列名直接拼接进 SQL 字符串（应通过 `?` 占位符绑定或做合法性校验）
