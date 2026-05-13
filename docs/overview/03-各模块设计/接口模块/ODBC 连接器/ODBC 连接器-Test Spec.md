# ODBC 连接器-Test Spec

## 1. **修订记录**

| **日期** | **版本** | **作者** | **备忘** |
| --- | --- | --- | --- |
| 2024-01-15 | 1.0 | 佘彦杰 | 创建 |
| 2026-01-12 | 1.1 | 王旭 | 完善测试用例 |

## 2. **测试目标**

1. 功能测试：验证 ODBC 连接器的所有 API 功能是否符合 ODBC 3.0 标准规范，确保连接管理、SQL 执行、数据获取、参数绑定等核心功能正常工作。
2. 性能测试：验证 ODBC 连接器在高并发、大数据量场景下的性能表现，确保满足需求规格中定义的性能指标。
3. 安全性测试：验证连接器的安全特性，包括 TLS/SSL 加密、凭证安全存储、敏感信息保护等。
4. 稳定性测试：验证连接器在长时间运行、多线程并发等场景下的稳定性和可靠性。
5. 兼容性测试：验证连接器在不同操作系统、不同编程语言环境下的兼容性。

## 3. **测试范围**

1. 功能测试：
  - 数据源管理（ConfigDSN、ConfigDriver、ConfigTranslator）
  - 连接管理（SQLConnect、SQLDriverConnect、SQLDisconnect）
  - 属性管理（SQLSetConnectAttr、SQLGetConnectAttr、SQLSetEnvAttr、SQLGetEnvAttr、SQLSetStmtAttr、SQLGetStmtAttr）
  - 环境资源管理（SQLAllocHandle、SQLFreeHandle、SQLFreeStmt）
  - 元数据查询（SQLNumResultCols、SQLColumns、SQLTables、SQLPrimaryKeys）
  - 数据获取（SQLFetch、SQLFetchScroll、SQLGetData）
  - 列操作（SQLDescribeCol、SQLColAttribute）
  - 信息获取（SQLGetInfo、SQLGetTypeInfo）
  - 参数操作（SQLBindParameter、SQLDescribeParam、SQLNumParams）
  - 结果集操作（SQLBindCol、SQLMoreResults、SQLRowCount、SQLCloseCursor）
  - 执行语句（SQLPrepare、SQLExecDirect、SQLExecute、SQLEndTran）
  - 错误诊断（SQLGetDiagField、SQLGetDiagRec）
  - Native 和 WebSocket 两种连接方式
1. 性能测试：
  - 并发查询性能（10 个子表并发查询）
  - 大规模子表查询（10000 个子表最新数据查询）
  - 批量写入性能（10000 个子表同时写入）
  - 事件记录写入性能（30 字段数据写入）
  - 参数绑定批量插入性能
1. 安全性测试：
  - TLS/SSL 加密连接
  - 凭证存储安全性
  - 连接字符串安全处理
  - 错误消息安全性
  - 审计日志功能
1. 稳定性测试：
  - 长时间运行稳定性（72 小时压测）
  - 多线程并发稳定性
  - 内存泄漏检测
  - 异常恢复能力
1. 兼容性测试：
  - 操作系统兼容性（Windows、Linux、macOS）
  - 编程语言兼容性（C、Python、Go、Node.js 等）
  - TDengine 版本兼容性（3.3.0.0 及以上）
  - ODBC 标准兼容性

## 4. **测试结论**

1. 功能测试：通过
2. 性能测试：通过
3. 安全性测试：通过
4. 稳定性测试：通过
5. 兼容性测试：通过

## 5. **已知问题和限制**

1. TDengine 不支持事务，SQLEndTran 为桩实现，仅模拟提交操作，不支持回滚。
2. SQLFetchScroll 仅支持 SQL_FETCH_NEXT，不支持随机游标。
3. 暂不支持 TDengine 的数据订阅和无模式写入特性。
4. ConfigDriver 和 ConfigTranslator 为桩实现。
5. SQLBrowseConnect 和 SQLNativeSql 暂不支持。

## 6. **测试环境**

| **系统** | **部署** | **CPU** | **内存** | **硬盘** |
| --- | --- | --- | --- | --- |
| **CentOS 7.9 / Ubuntu 20.04** | TDengine Server | 8 核 | 32GB | 500GB SSD |
| **Windows 10/11** | ODBC 客户端 | 4 核 | 16GB | 256GB SSD |
| **macOS 12+** | ODBC 客户端 | 4 核 | 16GB | 256GB SSD |

## 7. **测试用例**

### 7.1 **功能测试**

| **测试类型** | **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- | --- |
| **连接管理** | SQLConnect | 使用 DSN、用户名、密码建立连接 | 返回 SQL_SUCCESS，连接成功建立 | conformance_test.c: _connect() |
| **连接管理** | SQLDriverConnect | 使用连接字符串建立连接，支持 Native 和 WebSocket 方式 | 返回 SQL_SUCCESS，连接成功建立 | conformance_test.c: _driver_connect() |
| **连接管理** | SQLDisconnect | 断开已建立的数据库连接 | 返回 SQL_SUCCESS，连接正常断开 | api_test.c: do_sql_driver_conns() |
| **连接管理** | 无效 DSN 连接 | 使用不存在的 DSN 尝试连接 | 返回 SQL_ERROR，连接失败 | 01_case.json |
| **连接管理** | 特殊字符连接 | 使用包含特殊字符的用户名/密码连接 | 返回 SQL_SUCCESS，连接成功 | api_test.c: test_sql_conn_special_char() |
| **环境管理** | SQLAllocHandle (ENV) | 分配环境句柄 | 返回 SQL_SUCCESS，环境句柄有效 | api_test.c: test_sql_alloc_env() |
| **环境管理** | SQLAllocHandle (DBC) | 在环境句柄下分配连接句柄 | 返回 SQL_SUCCESS，连接句柄有效 | api_test.c: do_sql_alloc_conn() |
| **环境管理** | SQLAllocHandle (STMT) | 在连接句柄下分配语句句柄 | 返回 SQL_SUCCESS，语句句柄有效 | edge_cases_test.c: create_statement() |
| **环境管理** | SQLFreeHandle | 释放各类句柄 | 返回 SQL_SUCCESS，资源正确释放 | conformance_test.c: test_env() |
| **环境管理** | SQLSetEnvAttr | 设置 ODBC 版本为 3.0 | 返回 SQL_SUCCESS，属性设置成功 | test_threads.c |
| **属性管理** | SQLSetConnectAttr | 设置当前数据库 （SQL_ATTR_CURRENT_CATALOG） | 返回 SQL_SUCCESS，数据库切换成功 | conformance_test.c: test_conn() |
| **属性管理** | SQLGetConnectAttr | 获取当前连接属性值 | 返回 SQL_SUCCESS，属性值正确 | api_test.c: do_conn_get_info() |
| **属性管理** | SQLSetStmtAttr | 设置参数数组大小、绑定类型等 | 返回 SQL_SUCCESS，属性设置成功 | edge_cases_test.c: test_large_dataset() |
| **属性管理** | SQLGetStmtAttr | 获取语句属性当前值 | 返回 SQL_SUCCESS，属性值正确 | conformance_test.c |
| **SQL 执行** | SQLExecDirect | 直接执行 DDL 语句（CREATE/DROP DATABASE/TABLE） | 返回 SQL_SUCCESS，语句执行成功 | conformance_test.c: _exec_direct() |
| **SQL 执行** | SQLExecDirect | 直接执行 DML 语句（INSERT/SELECT） | 返回 SQL_SUCCESS，数据操作成功 | odbc_test.c: test_exec_direct() |
| **SQL 执行** | SQLPrepare | 预编译 SQL 语句 | 返回 SQL_SUCCESS，语句预编译成功 | conformance_test.c: test_prepare() |
| **SQL 执行** | SQLExecute | 执行预编译的 SQL 语句 | 返回 SQL_SUCCESS，语句执行成功 | edge_cases_test.c: test_large_dataset() |
| **SQL 执行** | SQLEndTran | 提交事务（模拟实现） | 返回 SQL_SUCCESS 或 SQL_SUCCESS_WITH_INFO | api_test.c: test_sql_end_tran() |
| **参数绑定** | SQLBindParameter | 绑定单个参数到预编译语句 | 返回 SQL_SUCCESS，参数绑定成功 | conformance_test.c: test_bind_params() |
| **参数绑定** | SQLBindParameter （数组） | 绑定参数数组进行批量插入 | 返回 SQL_SUCCESS，批量数据插入成功 | edge_cases_test.c: test_bind_array_of_params() |
| **参数绑定** | SQLNumParams | 获取预编译语句中的参数数量 | 返回 SQL_SUCCESS，参数数量正确 | conformance_test.c: test_prepare_with_stmt() |
| **参数绑定** | SQLDescribeParam | 获取参数的数据类型、大小等属性 | 返回 SQL_SUCCESS，参数属性正确 | conformance_test.c: test_prepare_with_stmt() |
| **结果集** | SQLFetch | 逐行获取结果集数据 | 返回 SQL_SUCCESS，数据获取正确 | conformance_test.c: select_count() |
| **结果集** | SQLFetchScroll | 按指定方式获取数据行 | 返回 SQL_SUCCESS，支持 SQL_FETCH_NEXT | conformance_test.c |
| **结果集** | SQLGetData | 获取结果集中特定列的数据 | 返回 SQL_SUCCESS，列数据正确 | api_test.c: run_SQLGetData() |
| **结果集** | SQLGetData （分段） | 分段获取大字段数据 | 返回 SQL_SUCCESS_WITH_INFO，数据分段获取成功 | odbc_test.c: _test_case_get_char_partial() |
| **结果集** | SQLBindCol | 绑定结果集列到应用程序变量 | 返回 SQL_SUCCESS，列绑定成功 | conformance_test.c: select_count_with_col_bind() |
| **结果集** | SQLBindCol （数组） | 绑定结果集列到数组变量，批量获取数据 | 返回 SQL_SUCCESS，批量数据获取正确 | conformance_test.c: select_count_with_col_bind_array() |
| **结果集** | SQLNumResultCols | 获取结果集列数 | 返回 SQL_SUCCESS，列数正确 | conformance_test.c: test_case5_with_stmt_1() |
| **结果集** | SQLRowCount | 获取影响的行数 | 返回 SQL_SUCCESS，行数正确 | conformance_test.c |
| **结果集** | SQLCloseCursor | 关闭游标 | 返回 SQL_SUCCESS，游标正常关闭 | conformance_test.c |
| **结果集** | SQLMoreResults | 检查是否有更多结果集 | 返回 SQL_NO_DATA，无更多结果集 | conformance_test.c |
| **列操作** | SQLDescribeCol | 获取列名、数据类型、大小等信息 | 返回 SQL_SUCCESS，列信息正确 | api_test.c: run_SQLDescribeCol() |
| **列操作** | SQLColAttribute | 获取列的各种属性 | 返回 SQL_SUCCESS，属性值正确 | conformance_test.c: test_case5_with_stmt_1() |
| **元数据** | SQLTables | 查询数据库中的表信息 | 返回 SQL_SUCCESS，表列表正确 | conformance_test.c: test_conn_SQL_catalog_functions() |
| **元数据** | SQLColumns | 查询表中的列信息 | 返回 SQL_SUCCESS，列信息正确 | conformance_test.c: test_SQLColumns() |
| **元数据** | SQLPrimaryKeys | 查询表的主键信息 | 返回 SQL_SUCCESS，主键信息正确（ts 列） | conformance_test.c |
| **元数据** | SQLGetTypeInfo | 获取支持的数据类型信息 | 返回 SQL_SUCCESS，类型信息正确 | conformance_test.c: test_SQLGetTypeInfo() |
| **信息获取** | SQLGetInfo | 获取驱动程序和数据源信息 | 返回 SQL_SUCCESS，信息正确 | api_test.c: do_conn_get_info() |
| **错误诊断** | SQLGetDiagRec | 获取错误诊断记录 | 返回 SQL_SUCCESS，诊断信息正确 | api_test.c: test_sql_diag_rec() |
| **错误诊断** | SQLGetDiagField | 获取特定诊断字段 | 返回 SQL_SUCCESS，字段值正确 | api_test.c: test_sql_diag_field() |
| **数据类型** | TIMESTAMP | 时间戳类型的读写操作 | 数据正确读写，精度正确 | odbc_test.c: cmp_timestamp_against_val() |
| **数据类型** | INT/BIGINT | 整数类型的读写操作 | 数据正确读写 | odbc_test.c: cmp_i32_against_val(), cmp_i64_against_val() |
| **数据类型** | FLOAT/DOUBLE | 浮点类型的读写操作 | 数据正确读写，精度在允许范围内 | odbc_test.c: cmp_real_against_val(), cmp_double_against_val() |
| **数据类型** | VARCHAR/NCHAR | 字符串类型的读写操作 | 数据正确读写，字符集正确 | odbc_test.c: cmp_varchar_against_val(), cmp_wvarchar_against_val() |
| **数据类型** | VARBINARY | 二进制类型的读写操作 | 数据正确读写 | odbc_test.c: cmp_varbinary_against_val() |
| **字符集** | UTF-8 | UTF-8 编码的中文字符读写 | 中文字符正确读写 | py3_test.py: test_charsets() |
| **字符集** | NCHAR | NCHAR 类型的中文字符读写 | 中文字符正确读写 | odbc_test.c: test_chars() |
| **超级表** | 创建超级表 | 创建带标签的超级表 | 返回 SQL_SUCCESS，超级表创建成功 | py3_test.py: test_case0() |
| **超级表** | 子表插入 | 使用 USING 语法自动创建子表并插入数据 | 返回 SQL_SUCCESS，数据插入成功 | py3_test.py: test_case0() |
| **语句管理** | SQLFreeStmt (SQL_CLOSE) | 关闭游标但保留语句句柄 | 返回 SQL_SUCCESS，可重用语句句柄 | conformance_test.c |
| **语句管理** | SQLFreeStmt (SQL_UNBIND) | 解除列绑定 | 返回 SQL_SUCCESS，绑定解除成功 | edge_cases_test.c |
| **语句管理** | SQLFreeStmt (SQL_RESET_PARAMS) | 重置参数绑定 | 返回 SQL_SUCCESS，参数重置成功 | conformance_test.c |

### 7.2 **性能测试**

| **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- |
| **并发查询** | 10 个子表并发查询，每个查询 5000 条记录 | 2 秒内完成所有查询 | benchmark/ |
| **大规模子表查询** | 10000 个子表同时查询最新数据 | 1 秒内完成查询 | benchmark/ |
| **大规模子表写入** | 10000 个子表同时写入最新数据 | 1 秒内完成写入 | benchmark/ |
| **事件记录写入** | 30 字段 VQT 形式数据写入 | 每秒写入 1000 条以上 | benchmark/ |
| **批量参数绑定** | 使用参数数组批量插入大量数据 | 插入效率显著高于单条插入 | edge_cases_test.c: test_large_dataset() |
| **大数据集查询** | 查询并遍历大量数据行 | 内存占用稳定，无泄漏 | edge_cases_test.c: test_large_dataset() |

### 7.3 **安全性测试**

| **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- |
| **TLS/SSL 加密** | 使用 WebSocket 方式建立加密连接 | 连接成功，数据传输加密 | 手动测试 |
| **凭证安全** | 验证密码不以明文形式存储 | 配置文件中无明文密码 | 配置检查 |
| **连接字符串安全** | 验证连接字符串中的密码不写入日志 | 日志文件中无敏感信息 | 日志检查 |
| **错误消息安全** | 验证错误消息不包含敏感信息 | 错误消息不暴露内部结构 | api_test.c: test_sql_diag_rec() |
| **超时机制** | 测试查询超时配置 | 超时后连接正常处理 | 手动测试 |

### 7.4 **稳定性测试**

| **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- |
| **长时间运行** | 20 客户端并发执行查询和写入，持续 72 小时 | 无异常、无内存泄漏 | benchmark/ |
| **多线程并发** | 多线程同时使用 ODBC 连接器 | 线程安全，无竞态条件 | test_threads.c |
| **内存泄漏检测** | 使用 Valgrind 检测内存泄漏 | 无内存泄漏报告 | valgrind/ |
| **句柄泄漏** | 反复分配和释放句柄 | 无句柄泄漏 | api_test.c |
| **连接池** | 多次连接断开操作 | 连接池正常工作 | conformance_test.c |
| **异常恢复** | 网络断开后重连 | 能够正常重新建立连接 | 手动测试 |
| **CreateThread 并发** | Windows CreateThread 多线程测试 | 线程安全运行 | test_threads.c: _run_with_arg_CreateThread() |
| **_beginthreadex 并发** | Windows _beginthreadex 多线程测试 | 线程安全运行 | test_threads.c: _run_with_arg__beginthreadex() |
| **pthread_create 并发** | Linux pthread_create 多线程测试 | 线程安全运行 | test_threads.c: _run_with_arg_pthread_create() |

### 7.5 **兼容性测试**

| **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- |
| **Windows 兼容** | Windows 10/11 上运行所有测试用例 | 所有测试通过 | tests/c/, tests/cpp/ |
| **Linux 兼容** | CentOS/Ubuntu 上运行所有测试用例 | 所有测试通过 | tests/c/, tests/cpp/ |
| **macOS 兼容** | macOS 12+ 上运行所有测试用例 | 所有测试通过 | tests/c/, tests/cpp/ |
| **C 语言** | C 语言程序使用 ODBC 连接器 | 功能正常 | tests/c/ |
| **C++ 语言** | C++ 程序使用 ODBC 连接器 | 功能正常 | tests/cpp/ |
| **Python** | pyodbc 库使用 ODBC 连接器 | 功能正常 | tests/python/py3_test.py |
| **Go** | Go 语言使用 ODBC 连接器 | 功能正常 | tests/go/ |
| **Node.js** | Node.js 使用 ODBC 连接器 | 功能正常 | tests/node/ |
| **Rust** | Rust 使用 ODBC 连接器 | 功能正常 | tests/rust/ |
| **R 语言** | R 语言使用 ODBC 连接器 | 功能正常 | tests/R/ |
| **TDengine 3.3.0.0+** | 与 TDengine 3.3.0.0 及以上版本兼容 | 所有功能正常 | 全部测试用例 |
| **Native 连接** | 使用 TDengine C 客户端库连接 | 连接和操作正常 | TAOS_ODBC_DSN 测试 |
| **WebSocket 连接** | 使用 WebSocket 协议连接 | 连接和操作正常 | TAOS_ODBC_WS_DSN 测试 |
| **ODBC 3.0 标准** | 符合 ODBC 3.0 标准规范 | 标准 API 行为正确 | conformance_test.c |

### 7.6 **边界条件和异常测试**

| **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- |
| **NULL 值处理** | 插入和查询 NULL 值 | NULL 值正确处理 | odbc_test.c |
| **空结果集** | 查询返回空结果集 | SQL_NO_DATA 正确返回 | conformance_test.c |
| **无效句柄** | 使用无效句柄调用 API | 返回 SQL_INVALID_HANDLE | api_test.c |
| **缓冲区截断** | 缓冲区小于数据长度 | 返回 SQL_SUCCESS_WITH_INFO，数据正确截断 | api_test.c: run_SQLGetData() |
| **参数数量不匹配** | 绑定参数数量与 SQL 语句不匹配 | 返回适当错误码 | conformance_test.c |
| **数据类型转换** | 不同数据类型之间的转换 | 转换正确或返回错误 | conformance_test.c: test_case5() |
| **大字段数据** | 读取超大 VARCHAR/VARBINARY 数据 | 分段读取成功 | odbc_test.c: _test_case_get_char_partial() |
| **特殊 SQL 语句** | 执行特殊语法的 SQL 语句 | 正确执行或返回适当错误 | odbc_test.cases |

## 8. **测试计划**

1. 测试环境搭建：0.5 人天
  - 部署 TDengine 服务器
  - 配置 ODBC 驱动和 DSN
  - 准备测试客户端环境
1. 功能测试执行：3 人天
  - 连接管理测试：0.5 人天
  - SQL 执行测试：0.5 人天
  - 参数绑定测试：0.5 人天
  - 结果集操作测试：0.5 人天
  - 元数据和信息获取测试：0.5 人天
  - 错误诊断测试：0.5 人天
1. 性能测试执行：1 人天
  - 并发查询性能测试
  - 批量写入性能测试
  - 大数据量测试
1. 安全性和稳定性测试：1 人天
  - 安全特性验证
  - 长时间运行测试
  - 多线程压力测试
1. 兼容性测试：1 人天
  - 多操作系统测试
  - 多编程语言测试
1. 测试总结：0.5 人天
  - 测试报告编写
  - 问题汇总和跟踪

## 9. **风险评估**

| **风险** | **影响** | **缓解措施** |
| --- | --- | --- |
| **TDengine 服务不稳定** | 测试中断 | 准备备用测试环境 |
| **网络问题** | WebSocket 测试失败 | 使用本地环境测试 |
| **内存不足** | 大数据量测试失败 | 分批执行测试，监控资源使用 |
| **多线程竞态条件** | 偶发性测试失败 | 增加测试迭代次数，使用同步机制 |
| **跨平台差异** | 特定平台测试失败 | 针对各平台单独调试 |

## 10. **参考文档**

1. ODBC 连接器-Requirement Spec
2. ODBC 连接器-Function Spec
3. ODBC 连接器-Design Spec
4. Microsoft Open Database Connectivity (ODBC): https://learn.microsoft.com/en-us/sql/odbc/microsoft-open-database-connectivity-odbc
5. ODBC Programmer's Reference: http://msdn.microsoft.com/en-us/library/ms714177.aspx
6. TDengine 官方文档： https://docs.tdengine.com/
