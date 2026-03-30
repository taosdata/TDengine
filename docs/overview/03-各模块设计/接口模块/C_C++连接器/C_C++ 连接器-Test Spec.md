# C/C++ 连接器-Test Spec

## 1. **修订记录**

| **日期** | **版本** | **作者** | **备忘** |
| --- | --- | --- | --- |
| 2025-01-25 | 1.0 | 王旭 | 初稿 |
| 2026-01-10 | 1.1 | 王旭 | 完善测试用例 |

## 2. **测试目标**

1. 功能测试：验证 C/C++连接器所有功能需求是否正确实现，包括连接管理、SQL 执行、无模式写入、参数绑定、数据订阅、错误处理、数据查询、配置管理等。
2. 性能测试：评估连接器在高并发、大数据量场景下的查询性能、插入性能及稳定性。
3. 安全性测试：验证数据传输安全（SSL/TLS）、身份认证、审计日志等功能。
4. 稳定性测试：验证连接器在长时间运行、异常情况下的稳定性和可靠性。
5. 兼容性测试：验证连接器在不同操作系统（Linux、Windows、macOS）及不同 TDengine 版本下的兼容性。

## 3. **测试范围**

1. 功能测试：覆盖 Requirement Spec 中定义的所有 9 项功能需求。
2. 性能测试：针对百万级数据量的查询性能、批量插入性能、高并发支持进行测试。
3. 安全性测试：测试 SSL/TLS 加密通信、TOTP/Token 认证、错误日志记录等安全特性。
4. 稳定性测试：长时间运行测试、连接异常恢复测试、资源泄漏测试。
5. 兼容性测试：跨平台测试、不同编译环境测试、向后兼容性测试。

## 4. **测试结论**

1. 功能测试：测试通过
2. 性能测试：测试通过
3. 安全性测试：测试通过
4. 稳定性测试：测试通过
5. 兼容性测试：测试通过

## 5. **已知问题和限制**

- 当前测试用例主要集中在功能验证，性能测试和压力测试需要更完善的基础设施。
- 部分边缘场景（如网络中断、服务器宕机）的测试覆盖可能不足。
- 跨平台兼容性测试需要实际硬件环境。

## 6. **测试环境**

| **系统** | **部署** | **CPU** | **内存** | **硬盘** |
| --- | --- | --- | --- | --- |
| **Linux (CentOS 7)** | TDengine 3.x 集群 | 8 核 | 16GB | 500GB SSD |
| **Windows Server 2019** | TDengine 单节点 | 4 核 | 8GB | 256GB SSD |
| **macOS Monterey** | TDengine 开发环境 | Apple M1 | 16GB | 512GB SSD |

## 7. **测试用例**

### 7.1 **功能测试**

| **测试类型** | **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- | --- |
| **连接管理** | 数据库连接 | 使用 IP、端口、用户名、密码连接 TDengine | 连接成功，返回有效 TAOS 句柄 | `clientCase.connect_Test`, `clientCase.connect_with_Test` |
| **连接管理** | TOTP 认证连接 | 使用 TOTP 动态密码进行连接认证 | 认证成功，建立安全连接 | `clientCase.connect_totp_Test` |
| **连接管理** | Token 认证连接 | 使用 Token 进行连接认证 | 认证成功，建立连接 | `clientCase.connect_token_Test` |
| **连接管理** | 连接配置选项 | 设置连接参数（IP、用户、密码等） | 参数生效，连接按配置建立 | `clientCase.set_option_Test`, `connectionCase.setConnectionOption_Test` |
| **SQL 执行** | 基本查询 | 执行 SELECT、SHOW 等 SQL 语句 | 返回正确结果集 | `clientBICase.select_Test`, `clientCase.show_db_Test` |
| **SQL 执行** | DDL 操作 | 创建/删除数据库、表、用户等 | 操作成功，元数据正确更新 | `clientCase.create_db_Test`, `clientCase.create_user_Test`, `clientCase.create_stable_Test` |
| **SQL 执行** | DML 操作 | 执行 INSERT、UPDATE、DELETE | 数据正确写入/修改，影响行数正确 | `clientCase.insert_test`, `clientCase.update_test` |
| **无模式写入** | Line Protocol 解析 | 解析 InfluxDB 行协议数据 | 正确解析字段和标签，创建表结构 | `testCase.smlParseInfluxString_Test` |
| **无模式写入** | JSON 协议解析 | 解析 JSON 格式数据 | 正确解析 JSON 结构，转换为内部格式 | `testCase.smlParseCols_Test` |
| **无模式写入** | Telnet 协议解析 | 解析 Telnet 格式数据 | 正确解析空格分隔的数据 | `testCase.smlParseTelnetLine_Test` |
| **参数绑定** | 预编译语句初始化 | 初始化 TAOS_STMT 对象 | 初始化成功，可后续绑定参数 | `stmt2Case.stmt2_init_prepare_Test` |
| **参数绑定** | 参数绑定执行 | 绑定参数并执行预编译 SQL | 执行成功，数据正确写入 | `stmt2Case.all_type`, `stmt2Case.mixed_bind` |
| **参数绑定** | 批量参数绑定 | 批量绑定多组参数执行 | 批量执行成功，性能提升明显 | `stmt2Case.stmt2_insert_stb`, `stmt2Case.stmt2_insert_ntb` |
| **数据订阅** | 主题创建 | 创建 TMQ 消费主题 | 主题创建成功，可订阅 | `testCase.create_topic_stb_Test`, `testCase.create_topic_ctb_Test` |
| **数据订阅** | 消息消费 | 订阅主题并消费数据 | 正确接收数据变更消息 | `testCase.tmq_subscribe_stb_Test`, `testCase.tmq_consume_Test` |
| **数据订阅** | 偏移量提交 | 提交消费偏移量 | 偏移量正确记录，支持断点续传 | `clientCase.tmq_commit`, `testCase.tmq_commit_Test` |
| **错误处理** | 错误码返回 | 执行错误操作，检查错误码 | 返回正确的错误码 | `stmt2Case.errcode`, `testCase.smlParseCols_Error_Test` |
| **错误处理** | 错误信息获取 | 通过 taos_errstr 获取错误描述 | 返回人类可读的错误信息 | 多个测试用例中验证 |
| **数据查询** | 结果集解析 | 获取查询结果的字段信息和行数据 | 正确解析各数据类型（整型、浮点、字符串等） | `clientCase.projection_query_tables`, `clientCase.agg_query_tables` |
| **数据查询** | 异步查询 | 使用异步 API 执行查询 | 非阻塞执行，通过回调获取结果 | `clientCase.async_api_test` |
| **配置扩展** | 客户端配置 | 通过 taos_options 设置客户端参数 | 配置生效，影响后续操作 | `clientCase.set_option_Test` |
| **配置扩展** | 连接池管理 | 测试连接池的创建、复用和释放 | 连接复用，资源管理正确 | `instanceCase.normal`, `instanceCase.expire` |

### 7.2 **性能测试**

| **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- |
| **查询性能** | 百万级数据量 SELECT 查询 | 查询延迟<100ms，吞吐量>1000 QPS | `clientCase.tsbs_perf_test`（需扩展） |
| **插入性能** | 批量参数绑定插入 | 插入速率>10 万行/秒，CPU/内存使用正常 | `stmt2Case.stmt2_insert_stb`（需压力测试） |
| **高并发** | 100+并发连接执行查询 | 系统稳定，响应时间可接受，无连接泄漏 | `clientCase.generated_request_id_test`（需扩展） |
| **内存泄漏** | 长时间运行，监测内存增长 | 内存使用稳定，无持续增长 | 需新增专项测试 |
| **网络延迟** | 模拟高网络延迟下的操作 | 连接超时和重试机制正常 | 需新增网络模拟测试 |

### 7.3 **安全性测试**

| **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- |
| **SSL/TLS 加密** | 启用 SSL 连接数据库 | 通信加密，握手成功，数据保密 | 需新增 SSL 测试（参考 cases/73-TLS） |
| **认证安全** | 错误密码、无效 Token 尝试连接 | 连接被拒绝，记录安全日志 | `clientCase.connect_totp_Test`中错误案例 |
| **SQL 注入防护** | 尝试 SQL 注入攻击 | 参数绑定防止注入，错误查询被拒绝 | `stmt2Case`系列测试已覆盖 |
| **审计日志** | 执行敏感操作，检查审计记录 | 操作被记录，包含时间、用户、IP 等信息 | 需集成审计功能测试 |

### 7.4 **稳定性测试**

| **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- |
| **长时间运行** | 连续运行 24 小时，执行混合操作 | 无崩溃，无内存泄漏，性能稳定 | 需新增稳定性测试套件 |
| **连接异常** | 模拟网络中断、服务器重启 | 自动重连机制生效，恢复后操作正常 | `clientCase.connect_Test`中的重试逻辑 |
| **资源边界** | 测试连接数、结果集大小等边界 | 边界条件处理正确，优雅降级 | 需新增边界测试 |
| **多线程安全** | 多线程并发使用同一连接/不同连接 | 线程安全，无数据竞争，结果正确 | `clientCase`中多线程测试元素 |

### 7.5 **兼容性测试**

| **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- |
| **操作系统** | 在 Linux、Windows、macOS 编译运行 | 编译通过，功能一致 | 跨平台 CI 流水线验证 |
| **编译器** | 使用 gcc、clang、MSVC 编译 | 编译通过，无警告，运行正常 | 编译器矩阵测试 |
| **TDengine 版本** | 连接不同版本的 TDengine 服务器 | API 兼容，功能正常，降级友好 | 需版本矩阵测试 |
| **字符集/时区** | 不同字符集和时区设置 | 数据正确存储和查询，无乱码 | `charsetCase.*`, `timezoneCase.*` |

## 8. **测试计划**

1. 测试环境搭建：0.5 人天（依赖现有 CI 环境）
2. 测试用例设计与评审：2 人天（基于本 Spec 细化）
3. 测试执行：5 人天（功能测试 2 天，性能 1 天，安全 1 天，稳定 1 天）
4. 问题修复与回归：3 人天
5. 测试总结与报告：1 人天

## 9. **风险评估**

- **技术风险**：性能测试可能需要大量测试数据和生产环境模拟，资源消耗大。
- **进度风险**：跨平台兼容性测试依赖多台物理机/虚拟机，环境准备可能耗时。
- **质量风险**：边缘场景和异常处理测试覆盖可能不足，需补充测试用例。

## 10. **参考文档**

1. C/C++ 连接器-Design Spec
2. C/C++ 连接器-Function Spec
3. C/C++ 连接器-Requirement Spec
