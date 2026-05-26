# tsdb-dev-stmt2

**版本**：1.2.0 · **维护团队**：engine · **作者**：Mario Peng

## 简介

本 Skill 用于快速生成 TDengine **STMT2 参数绑定**的 C/C++ 写入/查询代码。STMT2（`taos_stmt2_*`）是 TDengine 从 3.3.5.0 版本起推荐的高性能批量写入接口，相比旧版 STMT 接口，新增支持：

- 多子表一次 bind 批量写入
- 同步 / 异步执行
- 新式 SQL 语法（`INSERT INTO stb(tbname, col, ..., tag, ...) VALUES(?,?,?,...)`）
- 交织写入（interlace）模式

## 触发关键词

`stmt2`、`参数绑定`、`批量写入`、`交织写入`、`interlace`、`taos_stmt2_bind_param`、`TAOS_STMT2_BIND`、`TAOS_STMT2_BINDV`、`parameter binding`、`batch insert`

## 使用场景

| 场景 | 说明 |
|------|------|
| 超级表多子表批量写入 | 一次 `bind_param` 覆盖多张子表，每张子表多行 |
| 交织写入 | 依次向各子表写固定批次行数，循环至完成总行数 |
| 参数化 SELECT 查询 | WHERE 条件含 `?` 占位符，执行后获取结果集 |
| 异步高吞吐写入 | 通过 `asyncExecFn` 回调实现非阻塞执行 |
| 获取字段元信息 | 调用 `taos_stmt2_get_fields` 验证列/tag 类型 |

## 输入参数

| 参数 | 必需 | 默认值 | 说明 |
|------|:----:|--------|------|
| `sql` | ✅ | — | 绑定语句，含 `?` 占位符 |
| `table_schema` | ✅ | — | 完整建表 DDL（`CREATE STABLE ...`） |
| `language` | ❌ | `C` | 目标语言：`C` 或 `C++` |
| `exec_mode` | ❌ | `sync` | `sync`（同步）或 `async`（异步） |
| `operation` | ❌ | `insert` | `insert` 或 `select` |
| `sql_style` | ❌ | `new` | SQL 风格：`new`（列名列表）或 `old`（`? USING stb TAGS(...)`） |
| `num_subtables` | ❌ | `1` | 子表总数 |
| `total_rows` | ❌ | `10` | 每张子表的总写入行数 |
| `batch_rows` | ❌ | `total_rows` | 每批次写入行数，决定循环次数 |
| `interlace` | ❌ | `false` | 是否交织写入：`true` = 轮询各子表；`false` = 每表一次写完 |

## 核心 API 速查

```c
// 1. 初始化
// option: {reqid, singleStbInsert, singleTableBindOnce, asyncExecFn, userdata}
TAOS_STMT2_OPTION option = {0, true, true, NULL, NULL};
TAOS_STMT2 *stmt2 = taos_stmt2_init(taos, &option);

// 2. 准备 SQL
taos_stmt2_prepare(stmt2, sql, 0);

// 3. （可选）获取字段元信息
int fieldNum = 0;
TAOS_FIELD_ALL *pFields = NULL;
taos_stmt2_get_fields(stmt2, &fieldNum, &pFields);
taos_stmt2_free_fields(stmt2, pFields);

// 4. 绑定参数
// TAOS_STMT2_BIND: {buffer_type, buffer, length, is_null, num}
// TAOS_STMT2_BINDV: {count, tbnames, tags, bind_cols}
TAOS_STMT2_BINDV bindv = {num_subtables, tbnames, tags, params};
taos_stmt2_bind_param(stmt2, &bindv, -1);  // -1 = 全列绑定

// 5. 执行
int affected = 0;
taos_stmt2_exec(stmt2, &affected);

// 6. SELECT 取结果
TAOS_RES *pRes = taos_stmt2_result(stmt2);
TAOS_ROW row;
while ((row = taos_fetch_row(pRes))) { /* 处理行数据 */ }
taos_free_result(pRes);

// 7. 错误处理
if (code != 0) printf("Error: %s\n", taos_stmt2_error(stmt2));

// 8. 释放资源
taos_stmt2_close(stmt2);
taos_close(taos);
taos_cleanup();
```

## 数据类型常量（`taos.h`）

| SQL 类型 | 常量 | C 类型 | 字节 |
|----------|------|--------|------|
| BOOL | `TSDB_DATA_TYPE_BOOL` | `int8_t` | 1 |
| TINYINT | `TSDB_DATA_TYPE_TINYINT` | `int8_t` | 1 |
| SMALLINT | `TSDB_DATA_TYPE_SMALLINT` | `int16_t` | 2 |
| INT | `TSDB_DATA_TYPE_INT` | `int32_t` | 4 |
| BIGINT | `TSDB_DATA_TYPE_BIGINT` | `int64_t` | 8 |
| TINYINT UNSIGNED | `TSDB_DATA_TYPE_UTINYINT` | `uint8_t` | 1 |
| SMALLINT UNSIGNED | `TSDB_DATA_TYPE_USMALLINT` | `uint16_t` | 2 |
| INT UNSIGNED | `TSDB_DATA_TYPE_UINT` | `uint32_t` | 4 |
| BIGINT UNSIGNED | `TSDB_DATA_TYPE_UBIGINT` | `uint64_t` | 8 |
| FLOAT | `TSDB_DATA_TYPE_FLOAT` | `float` | 4 |
| DOUBLE | `TSDB_DATA_TYPE_DOUBLE` | `double` | 8 |
| TIMESTAMP | `TSDB_DATA_TYPE_TIMESTAMP` | `int64_t` | 8 |
| VARCHAR / BINARY | `TSDB_DATA_TYPE_BINARY` | `char*` | 变长 ¹ |
| NCHAR | `TSDB_DATA_TYPE_NCHAR` | `char*` (UTF-8) | 变长 ¹ |
| JSON | `TSDB_DATA_TYPE_JSON` | `char*` | 变长 ¹ |
| VARBINARY | `TSDB_DATA_TYPE_VARBINARY` | `char*` | 变长 ¹ |
| BLOB | `TSDB_DATA_TYPE_BLOB` | `char*` | 变长 ¹ |
| GEOMETRY | `TSDB_DATA_TYPE_GEOMETRY` | `char*` (WKB) | 变长 ¹ |

> ¹ 变长类型必须提供 `int32_t* length` 数组（各行字节长度）；定长类型 `length` 可传 `NULL`。

## 写入模式对比

| 模式 | `interlace` | 循环结构 | 适用场景 |
|------|:-----------:|----------|----------|
| 非交织（默认） | `false` | 外层按批次，内层覆盖所有子表 | 每表行数独立、追求吞吐量 |
| 交织 | `true` | 外层按批次，内层轮询单子表 | 模拟时序数据多设备同步推送 |
| 异步 | 任意 | 同上，exec 不阻塞，靠回调通知 | 超高吞吐、非阻塞写入场景 |

## SQL 风格对比

```c
// 新风格（推荐，3.3.5.0+）：列名列表中含 tbname
"INSERT INTO meters(tbname, ts, current, voltage, groupId, location) VALUES(?,?,?,?,?,?)"

// 旧风格：子表名用 ? 占位，TAGS 单独列出
"INSERT INTO ? USING meters TAGS(?,?) VALUES(?,?,?)"
```

## 目录结构

```
skills/tsdb-dev-stmt2/
├── SKILL.md     # Skill 定义（Agent 执行逻辑）
└── README.md    # 本文件（人类可读说明）
```

## 参考资料

- [TDengine C/C++ 连接器文档](https://docs.tdengine.com/reference/connector/cpp/)
- [stmt2_insert_demo.c](https://github.com/taosdata/TDengine/tree/main/docs/examples/c/stmt2_insert_demo.c)
- TDengine 错误码参考：`taos_stmt2_error(stmt)` 返回字符串描述
