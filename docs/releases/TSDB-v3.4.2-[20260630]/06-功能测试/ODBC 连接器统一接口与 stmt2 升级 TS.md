# ODBC 连接器统一 Native/WebSocket 接口与 stmt2 升级 测试报告

# 1 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-30 | 2026-04-30 | 1.0 | 裴亚明 | 新增 ODBC 连接器统一接口、stmt2 升级、CONN_MODE 支持功能测试 |

# 2 测试目标

本次修改涉及 ODBC 连接器（taos-connector-odbc）的四项核心功能重构与新增：

1. **统一 Native 和 WebSocket 接口**：移除 `taosws` 独立库依赖，Native 和 WebSocket 两种连接模式均通过统一的 `taos.dll` 进行交互，通过 `taos_options(TSDB_OPTION_DRIVER, "websocket"|"native")` 选择模式，共用 `taos_connect(ip, user, pass, db, port)` 建立连接。
2. **参数绑定 stmt v1 升级到 v2**：将 `TAOS_STMT` API 替换为 `TAOS_STMT2` API，包括 `taos_stmt2_init`、`taos_stmt2_prepare`、`taos_stmt2_bind_param`、`taos_stmt2_exec` 等全套接口。
3. **Block 取数解码层重构**：将 `taos_fetch_block` 替换为 `taos_fetch_raw_block`，直接解码原始 block 数据，移除对 `taosws` 解码路径的依赖。
4. **CONN_MODE（BI 模式）支持**：在统一 `taos_connect` 路径后，通过 `taos_set_conn_mode(TAOS_CONN_MODE_BI, 1)` 恢复 BI 模式功能，原 WebSocket 路径通过 URL query `?conn_mode=1` 传递。

# 3 参考文档

- TDengine 统一客户端库 `taos.h`：`taos_options(TSDB_OPTION_DRIVER, ...)`、`taos_connect`、`taos_set_conn_mode`
- TDengine STMT2 API：`taos_stmt2_init`、`taos_stmt2_prepare`、`taos_stmt2_bind_param`、`taos_stmt2_exec`
- TDengine Raw Block API：`taos_fetch_raw_block`

# 4 测试结论

本次统一接口、stmt2 升级和 CONN_MODE 支持功能已完成开发和测试，Native 和 WebSocket 两种连接方式均可正常工作。

**已知限制：**
- `taos_options(TSDB_OPTION_DRIVER, ...)` 为进程级全局设置，首次调用后不可切换模式
- x86（32 位）平台仅支持 WebSocket 模式，不支持 Native 模式
- CONN_MODE 仅支持在连接建立后设置，通过 `taos_set_conn_mode` 调用

# 5 测试环境

- OS：Windows 10（x64、x86）、Ubuntu 24.04
- 编译器：MSVC（Visual Studio 2019/2022）、GCC
- TDengine Server：3.4.1+ Enterprise
- 测试框架：CTest
- 连接方式：WebSocket（通过 taosAdapter）、Native（通过 taos 客户端库）
- DSN 配置：`TAOS_ODBC_DSN`（Native）、`TAOS_ODBC_WS_DSN`（WebSocket）

# 6 功能测试

## 6.1 统一接口 — 驱动初始化

### 6.1.1 测试要点

验证驱动初始化的正确性和线程安全性：
- `taos_options(TSDB_OPTION_DRIVER, "native"|"websocket")` 正确设置驱动模式
- 引入 `DRIVER_INITIALIZING` 中间状态，使用 CAS 原子操作防止多线程竞态
- 并发初始化时，后续线程通过 spin-wait 等待首个线程完成
- 初始化失败后状态回退为 `DRIVER_UNINIT`，允许重试
- 已初始化为某种模式后，不允许切换到另一种模式

### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | Native DSN 连接 | 通过 `DSN=TAOS_ODBC_DSN` 连接，验证驱动以 native 模式初始化并成功建立连接 | Pass |
| 2 | WebSocket DSN 连接 | 通过 `DSN=TAOS_ODBC_WS_DSN` 连接，验证驱动以 websocket 模式初始化并成功建立连接 | Pass |
| 3 | x86 平台 Native 拒绝 | x86 构建下不配置 URL 时，连接返回错误提示 native 模式不支持 | Pass |
| 4 | 多线程连接 | `test_threads` 测试中多线程并发建立连接，验证驱动初始化的线程安全性 | Pass |

## 6.2 统一接口 — URL 参数解析

### 6.2.1 测试要点

验证 WebSocket URL 中的连接参数正确提取为 `taos_connect` 的参数：
- URL 中的 `host`、`port` 提取为 `ip`、`port` 参数
- URL 中的 `user`、`pass` 提取为 `uid`、`pwd` 参数
- URL 中的 `path`（去除前导 `/`）作为数据库名的 fallback
- DSN 或连接字符串中的显式值优先于 URL 中提取的值

### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | URL host/port 解析 | `URL=http://127.0.0.1:6041` 连接成功，`ip` 和 `port` 正确传递给 `taos_connect` | Pass |
| 2 | URL user/pass 解析 | `URL=http://root:taosdata@host:6041` 连接成功，使用 URL 中的用户名和密码 | Pass |
| 3 | URL db 解析 | `URL=http://host:6041/test_db` 连接成功，数据库设为 `test_db` | Pass |
| 4 | DSN 显式值优先 | DSN 中设置 `DB=mydb`，URL 路径为 `/other_db`，连接使用 `mydb` | Pass |
| 5 | WebSocket DSN 完整流程 | `DSN=TAOS_ODBC_WS_DSN` 连接后执行建库、建表、插入、查询、删库全流程 | Pass |

## 6.3 统一接口 — 连接与查询

### 6.3.1 测试要点

验证 Native 和 WebSocket 两种模式下，通过统一的 `taos_connect` 路径建立连接并执行查询的正确性：
- `SQLConnect` 和 `SQLDriverConnect` 两种连接方式均正常工作
- SQL 查询、数据插入、结果集获取功能一致
- `SQLDisconnect` 正常断开连接并释放资源

### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | c_test (Native) | Native DSN 下执行建库、建表、插入、查询、绑定参数全套流程 | Pass |
| 2 | c_test (WebSocket) | WebSocket DSN 下执行建库、建表、插入、查询、绑定参数全套流程 | Pass |
| 3 | odbc_test (Native) | Native DSN 下通过 JSON 用例文件驱动的批量测试 | Pass |
| 4 | odbc_test (WebSocket) | WebSocket DSN 下通过 JSON 用例文件驱动的批量测试 | Pass |
| 5 | api_test (Native) | Native DSN 下 ODBC API 接口测试（SQLGetInfo、SQLGetTypeInfo 等） | Pass |
| 6 | api_test (WebSocket) | WebSocket DSN 下 ODBC API 接口测试 | Pass |
| 7 | edge_cases_test (Native) | Native DSN 下边界条件测试（大数据集、参数数组绑定等） | Pass |
| 8 | edge_cases_test (WebSocket) | WebSocket DSN 下边界条件测试 | Pass |

## 6.4 STMT2 参数绑定升级

### 6.4.1 测试要点

验证参数绑定从 STMT v1（`taos_stmt_*`）升级到 STMT v2（`taos_stmt2_*`）后的功能正确性：
- `taos_stmt2_init` 正确创建 stmt2 句柄
- `taos_stmt2_prepare` 正确解析 SQL 语句
- `taos_stmt2_bind_param` 正确绑定参数（含 BINDV 批量绑定）
- `taos_stmt2_exec` 正确执行语句
- `taos_stmt2_get_fields` 正确获取字段信息
- `taos_stmt2_close` 正确释放资源，无 double-free
- 各数据类型的参数绑定均正确（BOOL、TINYINT、SMALLINT、INT、BIGINT、FLOAT、DOUBLE、VARCHAR、NCHAR、TIMESTAMP 等）

### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | c_test stmt2 绑定 (Native) | Native DSN 下 `SQLBindParameter` + `SQLExecute` 流程，底层使用 stmt2 API | Pass |
| 2 | c_test stmt2 绑定 (WebSocket) | WebSocket DSN 下 stmt2 参数绑定 | Pass |
| 3 | taos_test stmt2 回归 | 直接调用 `taos_stmt2_*` 系列 API 进行 prepare/bind/exec 测试 | Pass |
| 4 | 参数数组绑定 (Native) | `SQLSetStmtAttr(SQL_ATTR_PARAMSET_SIZE)` 批量绑定多行数据写入 | Pass |
| 5 | 各数据类型绑定 | INT、BIGINT、FLOAT、DOUBLE、VARCHAR、NCHAR、TIMESTAMP、BOOL 等类型参数绑定后查询验证 | Pass |
| 6 | NULL 值绑定 | `SQL_NULL_DATA` 长度标识绑定 NULL 值，查询结果 `SQLGetData` 返回 `SQL_NULL_DATA` | Pass |

## 6.5 Raw Block 取数解码

### 6.5.1 测试要点

验证从 `taos_fetch_block` 切换到 `taos_fetch_raw_block` 后，结果集数据的解码正确性：
- 各数据类型在 raw block 中的正确解码（定长类型、变长类型）
- NULL 值的正确识别
- 时间戳精度（毫秒、微秒、纳秒）的正确处理
- `SQLGetData`、`SQLFetch`、`SQLBindCol` 等 ODBC API 在新解码路径下的正确性

### 6.5.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 全类型查询 (Native) | 创建包含所有支持数据类型的表，插入数据后查询验证每列的解码结果 | Pass |
| 2 | 全类型查询 (WebSocket) | 同上，WebSocket 模式 | Pass |
| 3 | SQLBindCol 绑定取数 | 通过 `SQLBindCol` 绑定列缓冲区，`SQLFetch` 后验证缓冲区中的数据正确 | Pass |
| 4 | SQLGetData 逐列取数 | 通过 `SQLGetData` 逐列获取数据，验证返回值和数据内容正确 | Pass |
| 5 | NULL 值解码 | 插入含 NULL 列的数据行，查询验证 `SQL_NULL_DATA` 正确返回 | Pass |
| 6 | 大结果集 | 插入大量数据行，查询验证分 block 取数的正确性和完整性 | Pass |

## 6.6 CONN_MODE（BI 模式）支持

### 6.6.1 测试要点

验证 `CONN_MODE=1`（BI 模式）在统一 `taos_connect` 路径下的正确恢复：
- DSN 中配置 `CONN_MODE=1` 后，连接建立后调用 `taos_set_conn_mode(TAOS_CONN_MODE_BI, 1)`
- 连接字符串中指定 `CONN_MODE=1` 同样生效
- `taos_set_conn_mode` 的返回值正确检查，失败时报错并断开连接
- setup.c 中 DSN 配置对话框的测试连接按钮也正确应用 CONN_MODE
- 启用/禁用 BI 模式后连接仍可正常查询

### 6.6.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | _conn_mode 启用 BI 模式 | `taos_set_conn_mode(TAOS_CONN_MODE_BI, 1)` 调用成功 | Pass |
| 2 | _conn_mode BI 模式查询 | 启用 BI 模式后执行 `select 1` 查询成功 | Pass |
| 3 | _conn_mode 禁用 BI 模式 | `taos_set_conn_mode(TAOS_CONN_MODE_BI, 0)` 调用成功 | Pass |
| 4 | _conn_mode 禁用后查询 | 禁用 BI 模式后执行 `select 1` 查询成功 | Pass |
| 5 | CONN_MODE DSN 配置 | DSN 中 `CONN_MODE=1` 配置被正确读取并应用到连接 | Pass |
| 6 | CONN_MODE 连接字符串 | 连接字符串 `CONN_MODE=1` 被 conn_parser 正确解析并应用 | Pass |

## 6.7 Setup 对话框（Windows DSN 配置）

### 6.7.1 测试要点

验证 Windows ODBC DSN 配置对话框在统一接口下的正确性：
- WebSocket 模式的测试连接按钮正确调用 `conn_init_driver_type` + `taos_connect`
- Native 模式的测试连接按钮正确调用 `conn_init_driver_type` + `taos_connect`
- URL 中的连接参数正确解析并用于测试连接

### 6.7.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | WebSocket 测试连接 | 配置 URL 后点击 Test 按钮，连接成功弹出成功提示 | Pass |
| 2 | Native 测试连接 | 配置 Server 后点击 Test 按钮，连接成功弹出成功提示 | Pass |

## 6.8 taos_helpers 日志包装

### 6.8.1 测试要点

验证新增的 taos API 日志包装函数正确记录调用参数和返回值：
- `CALL_taos_options`：记录 `option` 和 `arg` 参数及返回值
- `CALL_taos_set_conn_mode`：记录 `taos`、`mode`、`value` 参数及返回值

### 6.8.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | taos_options 日志 | 连接时日志中出现 `taos_options(option:...,arg:...)` 记录 | Pass |
| 2 | taos_set_conn_mode 日志 | 设置 CONN_MODE 时日志中出现 `taos_set_conn_mode(taos:...,mode:...,value:...)` 记录 | Pass |

## 6.9 性能优化

### 6.9.1 测试要点

验证 `memcpy` 替换 `memmove` 优化的正确性：
- 在 stmt var bind packing 中，当目标和源区域不重叠时（`dst + n <= src`），使用 `memcpy` 替代 `memmove`
- 重叠时仍使用 `memmove` 确保正确性

### 6.9.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 参数绑定数据打包 | 多列变长参数绑定后数据打包正确，查询验证数据完整性 | Pass |

## 6.10 长期稳定性测试

无。

## 6.11 性能测试

### 6.11.1 测试场景

- 数据模型：超级表 `meters`（`ts/current/voltage/phase`），100 个子表
- 并发模型：10 个写入线程，每线程 10 个子表
- 数据规模：每子表 100,000 行，总计 10,000,000 行
- 批大小：`--batch 10000`

### 6.11.2 结果对比

| 指标 | 优化前 | 优化后 | 变化 |
| --- | ---: | ---: | ---: |
| 建库建表耗时 | 9029.28 ms | 2744.73 ms | -69.6% |
| 写入总耗时 | 13211.29 ms | 8705.13 ms | -34.1% |
| 总吞吐 | 756,928 rows/s | 1,148,749 rows/s | +51.8% |
| 单线程吞吐范围 | 76,137 ~ 80,575 rows/s | 115,578 ~ 139,037 rows/s | 显著提升 |
| 总写入行数 | 10,000,000 | 10,000,000 | 一致 |
| 结果状态 | SUCCESS | SUCCESS | 一致 |

### 6.11.3 结论

在相同数据规模和并发配置下，优化后写入性能显著提升：总吞吐由 75.7 万 rows/s 提升至 114.9 万 rows/s，增幅约 51.8%；总写入耗时由 13.21 s 降至 8.71 s，下降约 34.1%。同时，优化前后均完整写入 1000 万行且结果为 SUCCESS，功能正确性保持一致。

## 6.12 安全性测试

无。

# 7 兼容性测试

| # | 测试场景 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | x64 平台 Native 模式 | x64 构建下 Native DSN 全套测试通过 | Pass |
| 2 | x64 平台 WebSocket 模式 | x64 构建下 WebSocket DSN 全套测试通过 | Pass |
| 3 | x86 平台 WebSocket 模式 | x86 构建下 WebSocket DSN 全套测试通过（Native 不支持） | Pass |
| 4 | Native 与 WebSocket 结果一致性 | 同一套 ODBC 测试用例在 Native 和 WebSocket DSN 下执行，结果一致 | Pass |
| 5 | 多语言连接器兼容 | C、C++、C#、Python、Rust 测试用例在统一接口下均通过 | Pass |
| 6 | 已有功能不受影响 | 全部 CTest 测试用例（含 odbc_test、c_test、api_test、edge_cases_test 等）在统一接口下通过 | Pass |

# 8 已知问题和限制

- `taos_options(TSDB_OPTION_DRIVER, ...)` 是进程级全局设置，首次初始化后不可切换 Native/WebSocket 模式。同一进程中只能使用一种连接模式。
- x86（32 位）平台不支持 Native 模式，仅支持 WebSocket 模式。这是 TDengine 客户端库的限制。
- `CONN_MODE`（BI 模式）通过 `taos_set_conn_mode` 在连接建立后设置，而非连接参数。如果该 API 调用失败，连接会被断开并返回错误。
- ADO 测试中发现 Native 模式下引擎的 STMT2 接口尚未与原 STMT 功能完全对齐，缺少对参数绑定 UPDATE 语句的支持。该问题已提交项目跟踪：https://project.feishu.cn/taosdata_td/feature/detail/6980625942 。

