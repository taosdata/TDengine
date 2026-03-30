# C# 连接器-Test Spec

## 1. **修订记录**

| **修改日期** | **版本** | **负责人** | **主要修改内容** |
| --- | --- | --- | --- |
| 2025-01-24 | 1.0 | 霍琳贺 | 初始版本 |
| 2026-01-27 | 1.1 | 王旭 | 完善测试用例 |

## 2. **测试目标**

本文档旨在为 C# 连接器的测试提供详细的指导和说明。C# 连接器是 TDengine 数据库的官方 。NET 客户端驱动库，为保障其质量、稳定性和安全性，需要建立完善的测试体系。通过本文档，测试团队可以清晰地了解测试目标、范围、策略、用例设计以及执行要求，确保测试过程的全面性和有效性。

### 2.1 **质量目标**

1. 确保连接器功能完整，满足所有功能需求。
2. 确保连接器性能可靠，满足基本性能要求。
3. 确保连接器安全合规，防止 SQL 注入、数据泄露等安全风险。
4. 确保连接器兼容性良好，支持指定版本的 TDengine 和 . NET。

### 2.2 **测试覆盖目标**

1. 功能需求覆盖率达到 100%。
2. 代码行覆盖率达到 85% 以上。
3. 边界条件、异常场景覆盖充分。

## 3. **测试****范围**

本文档涵盖 C# 连接器的所有测试活动，包括：
1. **单元测试**：对连接器核心类和方法进行隔离测试。
2. **集成测试**：测试连接器与 TDengine 数据库的交互。
3. **功能测试**：验证连接器所有功能需求（参考 Requirement Spec）。
4. **安全测试**：验证连接器的安全需求（参考 Requirement Spec 安全部分）。
5. **兼容性测试**：验证连接器与不同 TDengine 版本和 。NET 版本的兼容性。

## 4. 测试结论

1. 功能测试：通过
2. 性能测试：通过
3. 安全性测试：通过
4. 稳定性测试：通过
5. 兼容性测试：通过

## 5. 已知问题和限制

无

## 6. 测试环境

### 6.1 **硬件环境**

1. 普通开发或测试服务器，无特殊硬件要求。

### 6.2 **软件环境**

1. **操作系统**：Windows 10/11，Linux（Ubuntu 20.04+，CentOS 7+），macOS 10.15+
2. **TDengine 数据库**：3.3.6.0 及以上版本（需支持原生连接和 WebSocket 连接）
3. **.NET 环境**：
   - .NET Framework 4.6 及以上
   - .NET 6.0 及以上
   - .NET 8.0 及以上
4. **测试工具**：
   - xUnit 测试框架
   - Mock WebSocket 服务器（用于隔离测试）
   - 代码覆盖率工具（如 coverlet）

#### 6.2.1 **网络环境**

1. 能够访问 TDengine 数据库实例（本地或远程）
2. 支持 WebSocket 连接（端口 6041）

## 7. **测试用例**

### 7.1 **数据库连接测试**

| **测试编号** | **测试场景** | **测试要点** | **预期结果** |
| --- | --- | --- | --- |
| **TC-CONN-001** | 原生协议连接 | 使用正确的连接字符串建立连接 | 连接成功，State 为 Open |
| **TC-CONN-002** | WebSocket 协议连接 | 使用 WebSocket 协议建立连接 | 连接成功，State 为 Open |
| **TC-CONN-003** | 连接参数验证 | 缺少必填参数（如 host， port） | 抛出异常，提示参数缺失 |
| **TC-CONN-004** | 连接超时测试 | 设置 connTimeout 为极短时间，连接不可达地址 | 在超时时间内抛出超时异常 |
| **TC-CONN-005** | 自动重连测试（WebSocket） | 网络中断后恢复 | 自动重连成功，连接恢复 |
| **TC-CONN-006** | 连接关闭测试 | 调用 Close（） 或 Dispose（） | 连接关闭，State 为 Closed |
| **TC-CONN-007** | 连接状态事件 | 监听 StateChange 事件 | 事件正确触发，状态变更正确 |
| **TC-CONN-008** | 多数据库连接 | 同时连接多个数据库 | 各连接独立，互不干扰 |

### 7.2 **SQL 执行测试**

| **测试编号** | **测试场景** | **测试要点** | **预期结果** |
| --- | --- | --- | --- |
| **TC-SQL-001** | 简单查询执行 | 执行 SELECT 1 | 返回正确结果 |
| **TC-SQL-002** | 建表语句执行 | 执行 CREATE DATABASE/TABLE | 执行成功，数据库/表创建 |
| **TC-SQL-003** | 插入数据执行 | 执行 INSERT 语句 | 数据插入成功 |
| **TC-SQL-004** | 更新数据执行 | 执行 UPDATE 语句 | 数据更新成功 |
| **TC-SQL-005** | 删除数据执行 | 执行 DELETE 语句 | 数据删除成功 |
| **TC-SQL-006** | 带请求 ID 的 SQL 执行 | 设置 reqId 执行 SQL | SQL 执行成功，reqId 可追踪 |
| **TC-SQL-007** | SQL 错误处理 | 执行错误 SQL 语句 | 抛出适当的异常，异常信息清晰 |
| **TC-SQL-008** | 批量 SQL 执行 | 执行多条 SQL 语句 | 各语句执行成功 |

### 7.3 **参数绑定测试**

| **测试编号** | **测试场景** | **测试要点** | **预期结果** |
| --- | --- | --- | --- |
| **TC-PARAM-001** | 基本参数绑定 | 绑定整数、字符串、时间等类型参数 | 参数绑定成功，SQL 执行正确 |
| **TC-PARAM-002** | 参数前缀验证 | 使用正确的前缀（$、@、#） | 参数绑定成功 |
| **TC-PARAM-003** | 参数前缀验证（错误） | 使用错误的前缀 | 抛出异常，提示参数前缀错误 |
| **TC-PARAM-004** | 参数类型映射 | 所有 TDengine 类型到 C# 类型的映射 | 类型映射正确，数据读写无误 |
| **TC-PARAM-005** | 空值参数绑定 | 绑定 DBNull.Value 或 null | 空值处理正确 |
| **TC-PARAM-006** | 批量参数绑定 | 使用 AddBatch 绑定多行参数 | 批量执行成功，性能符合预期 |
| **TC-PARAM-007** | 参数防注入 | 在参数值中包含 SQL 特殊字符 | 参数被正确转义，无 SQL 注入风险 |

### 7.4 **查询结果读取测试**

| **测试编号** | **测试场景** | **测试要点** | **预期结果** |
| --- | --- | --- | --- |
| **TC-QUERY-001** | 简单结果集读取 | 读取单行单列结果 | 结果正确 |
| **TC-QUERY-002** | 多行结果集读取 | 读取多行数据 | 所有行数据正确 |
| **TC-QUERY-003** | 多列结果集读取 | 读取多列数据 | 所有列数据正确 |
| **TC-QUERY-004** | 数据类型读取 | 读取所有 TDengine 数据类型 | 数据类型映射正确，值无误 |
| **TC-QUERY-005** | 空结果集处理 | 查询返回空结果集 | 正确处理，无异常 |
| **TC-QUERY-006** | 大结果集读取 | 读取大量数据（如 10 万行） | 内存使用合理，无内存泄漏 |
| **TC-QUERY-007** | 结果集遍历方法 | 使用 Read（）， GetValue（）， GetValues（） 等方法 | 各方法工作正常 |
| **TC-QUERY-008** | 结果集元数据 | 获取列名、列类型、列大小等元数据 | 元数据信息正确 |
| **TC-QUERY-009** | 结果集资源释放 | 使用 using 或调用 Dispose（） | 资源正确释放，无内存泄漏 |

### 7.5 **无模式写入测试**

| **测试编号** | **测试场景** | **测试要点** | **预期结果** |
| --- | --- | --- | --- |
| **TC-SML-001** | InfluxDB 行协议写入 | 使用 InfluxDB 行协议写入数据 | 数据写入成功，可查询 |
| **TC-SML-002** | OpenTSDB 行协议写入 | 使用 OpenTSDB 行协议写入数据 | 数据写入成功，可查询 |
| **TC-SML-003** | OpenTSDB JSON 协议写入 | 使用 OpenTSDB JSON 协议写入数据 | 数据写入成功，可查询 |
| **TC-SML-004** | 批量无模式写入 | 批量写入多条数据 | 批量写入成功，性能良好 |
| **TC-SML-005** | 错误数据处理 | 写入格式错误的数据 | 抛出适当的异常，异常信息清晰 |
| **TC-SML-006** | 协议自动识别 | 自动识别不同协议格式 | 协议识别正确，写入成功 |

### 7.6 **TMQ 订阅测试**

| **测试编号** | **测试场景** | **测试要点** | **预期结果** |
| --- | --- | --- | --- |
| **TC-TMQ-001** | 消费者创建 | 使用 ConsumerBuilder 创建消费者 | 消费者创建成功 |
| **TC-TMQ-002** | 主题订阅 | 订阅一个或多个主题 | 订阅成功，开始接收消息 |
| **TC-TMQ-003** | 消息消费 | 消费订阅的消息 | 消息内容正确，可反序列化 |
| **TC-TMQ-004** | 偏移量提交 | 提交消费偏移量 | 偏移量提交成功 |
| **TC-TMQ-005** | 分区信息获取 | 获取分配给消费者的分区信息 | 分区信息正确 |
| **TC-TMQ-006** | 偏移量设置 | 设置指定分区的偏移量 | 偏移量设置成功，从指定位置消费 |
| **TC-TMQ-007** | 消费位置查询 | 查询指定分区的消费位置 | 消费位置信息正确 |
| **TC-TMQ-008** | 多消费者协调 | 多个消费者消费同一主题 | 负载均衡正确，无重复消费 |
| **TC-TMQ-009** | 消费者关闭 | 关闭消费者资源 | 资源正确释放，订阅停止 |

### 7.7 **安全测试**

| **测试编号** | **测试场景** | **测试要点** | **预期结果** |
| --- | --- | --- | --- |
| **TC-SEC-001** | 用户名密码认证 | 使用正确的用户名密码连接 | 认证成功，连接建立 |
| **TC-SEC-002** | Token 认证（WebSocket） | 使用 Token 连接 TDengine Cloud | 认证成功，连接建立 |
| **TC-SEC-003** | 密码安全存储 | 连接字符串中的密码处理 | 密码不在日志、异常信息中明文出现 |
| **TC-SEC-004** | WSS 协议支持 | 使用 useSSL=true 建立安全连接 | 安全连接建立，传输加密 |
| **TC-SEC-005** | 证书验证 | WSS 连接时的证书验证 | 默认启用证书验证，无效证书拒绝连接 |
| **TC-SEC-006** | SQL 注入防护 | 尝试通过参数进行 SQL 注入 | 参数绑定机制有效，无注入风险 |
| **TC-SEC-007** | 连接参数验证 | 提供非法连接参数 | 参数验证有效，拒绝非法配置 |
| **TC-SEC-008** | 资源泄漏测试 | 长时间运行，反复创建连接和查询 | 无内存泄漏，资源正确释放 |
| **TC-SEC-009** | 并发安全测试 | 多线程同时操作连接器 | 线程安全，无数据竞争 |

### 7.8 **性能测试**

| **测试编号** | **测试场景** | **测试要点** | **预期目标** |
| --- | --- | --- | --- |
| **TC-PERF-001** | 连接建立性能 | 测量连接建立时间 | 连接建立时间 < 1 秒 |
| **TC-PERF-002** | 简单查询性能 | 执行 SELECT 1 的响应时间 | 查询响应时间 < 100ms |
| **TC-PERF-003** | 数据插入性能 | 批量插入 1000 行数据的时间 | 插入速率 > 1000 行/秒 |
| **TC-PERF-004** | 数据查询性能 | 查询 10 万行数据的耗时 | 查询和读取时间合理 |
| **TC-PERF-005** | 参数绑定性能 | 绑定 1000 个参数的耗时 | 绑定时间合理，无显著性能瓶颈 |
| **TC-PERF-006** | 内存使用监控 | 长时间运行的内存使用情况 | 内存使用稳定，无持续增长 |

### 7.9 **兼容性测试**

| **测试编号** | **测试场景** | **测试要点** | **预期结果** |
| --- | --- | --- | --- |
| **TC-COMP-001** | TDengine 3.3.6.0 兼容性 | 连接器与 TDengine 3.3.6.0 的兼容性 | 所有功能正常工作 |
| **TC-COMP-002** | TDengine 最新版本兼容性 | 连接器与 TDengine 最新版本的兼容性 | 所有功能正常工作 |
| **TC-COMP-003** | .NET Framework 4.6 兼容性 | 在 。NET Framework 4.6 上运行测试 | 所有功能正常工作 |
| **TC-COMP-004** | .NET 6.0 兼容性 | 在 。NET 6.0 上运行测试 | 所有功能正常工作 |
| **TC-COMP-005** | .NET 8.0 兼容性 | 在 。NET 8.0 上运行测试 | 所有功能正常工作 |
| **TC-COMP-006** | 跨平台兼容性 | 在 Windows、Linux、macOS 上运行测试 | 所有功能正常工作，无平台差异 |

### 7.10 **测试用例映射（Test Case Traceability）**

本节将文档中定义的测试用例与实际测试代码中的测试方法进行映射，确保每个设计用例都有对应的实现。

| **测试编号** | **测试方法** | **测试描述** | **对应测试文件** |
| --- | --- | --- | --- |
| **TC-CONN-001** | ConnectionString_Property_Should_Set_ConnectionString_And_ConnectionStringBuilder | 连接字符串属性设置验证 | TDengineConnectionTests.cs |
| **TC-CONN-002** | WebSocket 连接相关测试 | WebSocket 协议连接测试 | TDengineConnectionTests.cs |
| **TC-CONN-003** | DefaultNative_ShouldSetDefaultValues， Parse， ParseWebSocket 等 | 连接参数验证测试 | TDengineConnectionStringBuilderTests.cs |
| **TC-CONN-004** | 手动测试 | 连接超时测试 | 手动测试 |
| **TC-CONN-005** | 手动测试 | 自动重连测试（WebSocket） | 手动测试 |
| **TC-CONN-006** | Close_Method_Should_Close_Connection | 连接关闭测试 | TDengineConnectionTests.cs |
| **TC-CONN-007** | StateChange 事件相关测试 | 连接状态事件测试 | TDengineConnectionTests.cs |
| **TC-CONN-008** | 手动测试 | 多数据库连接 | 手动测试 |
| **TC-SQL-001** | NormalTable | 简单查询执行测试 | Query.cs |
| **TC-SQL-002** | ExecuteNonQuery_WithValidCommand_ReturnsAffectedRows | 建表语句执行测试 | TDengineCommandTests.cs |
| **TC-SQL-003** | ExecuteNonQuery_WithValidCommand_ReturnsAffectedRows | 插入数据执行测试 | TDengineCommandTests.cs |
| **TC-SQL-004** | 手动测试 | 更新数据执行 | 手动测试 |
| **TC-SQL-005** | 手动测试 | 删除数据执行 | 手动测试 |
| **TC-SQL-006** | GetReqId, MurmurHash32_ReturnsExpectedHash | 带请求 ID 的 SQL 执行测试 | Reqid.cs |
| **TC-SQL-007** | 错误 SQL 处理测试 | SQL 错误处理 | Query.cs |
| **TC-SQL-008** | 手动测试 | 批量 SQL 执行 | 手动测试 |
| **TC-PARAM-001** | ParameterName_Should_Be_Settable_And_Gettable 等 | 基本参数绑定测试 | TDengineParameterTests.cs |
| **TC-PARAM-002** | 参数前缀相关测试 | 参数前缀验证 | TDengineParameterTests.cs |
| **TC-PARAM-003** | 参数前缀错误测试 | 参数前缀验证（错误） | TDengineParameterTests.cs |
| **TC-PARAM-004** | CommonExec 等 | 参数类型映射测试 | TDengineDataReaderTesting.cs |
| **TC-PARAM-005** | ExecuteNonQuery_WithDBNull | 空值参数绑定测试 | TDengineCommandTests.cs |
| **TC-PARAM-006** | TestNTable | 批量参数绑定测试 | InsertCn.cs |
| **TC-PARAM-007** | PrepareCommandTest, ConnectionInitCommandTest | 参数防注入测试 | TDenginePrepareCommandTests.cs |
| **TC-QUERY-001** | NormalTable | 简单结果集读取测试 | Query.cs |
| **TC-QUERY-002** | NormalTable | 多行结果集读取测试 | Query.cs |
| **TC-QUERY-003** | NormalTable | 多列结果集读取测试 | Query.cs |
| **TC-QUERY-004** | CommonExec 等 | 数据类型读取测试 | TDengineDataReaderTesting.cs |
| **TC-QUERY-005** | 空结果集测试 | 空结果集处理测试 | Query.cs |
| **TC-QUERY-006** | 手动测试 | 大结果集读取 | 手动测试 |
| **TC-QUERY-007** | GetValue， GetValues 等方法测试 | 结果集遍历方法测试 | TDengineDataReaderTesting.cs |
| **TC-QUERY-008** | TestFetchFieldJsonTag 等 | 结果集元数据测试 | FetchFields.cs |
| **TC-QUERY-009** | Dispose 方法测试 | 结果集资源释放测试 | TDengineDataReaderTesting.cs |
| **TC-SML-001** | LineProtocol | InfluxDB 行协议写入测试 | SMLRaw.cs |
| **TC-SML-002** | TelnetProtocol | OpenTSDB 行协议写入测试 | SMLRaw.cs |
| **TC-SML-003** | JSONProtocol | OpenTSDB JSON 协议写入测试 | SMLRaw.cs |
| **TC-SML-004** | 批量写入测试 | 批量无模式写入测试 | SMLRaw.cs |
| **TC-SML-005** | 手动测试 | 错误数据处理 | 手动测试 |
| **TC-SML-006** | 手动测试 | 协议自动识别 | 手动测试 |
| **TC-TMQ-001** | DataBase | 消费者创建测试 | SubscribeDatabase.cs |
| **TC-TMQ-002** | DataBase | 主题订阅测试 | SubscribeDatabase.cs |
| **TC-TMQ-003** | DataBase | 消息消费测试 | SubscribeDatabase.cs |
| **TC-TMQ-004** | NormalTable 等 | 偏移量提交测试 | SubscribeTables.cs |
| **TC-TMQ-005** | 分区信息获取测试 | 分区信息获取测试 | SubscribeTables.cs |
| **TC-TMQ-006** | 手动测试 | 偏移量设置 | 手动测试 |
| **TC-TMQ-007** | 手动测试 | 消费位置查询 | 手动测试 |
| **TC-TMQ-008** | 手动测试 | 多消费者协调 | 手动测试 |
| **TC-TMQ-009** | 手动测试 | 消费者关闭 | 手动测试 |
| **TC-SEC-001** | 用户名密码认证测试 | 用户名密码认证测试 | TDengineConnectionTests.cs |
| **TC-SEC-002** | 手动测试 | Token 认证（WebSocket） | 手动测试 |
| **TC-SEC-003** | 手动测试 | 密码安全存储 | 手动测试 |
| **TC-SEC-004** | 手动测试 | WSS 协议支持 | 手动测试 |
| **TC-SEC-005** | 手动测试 | 证书验证 | 手动测试 |
| **TC-SEC-006** | PrepareCommandTest 等 | SQL 注入防护测试 | TDenginePrepareCommandTests.cs |
| **TC-SEC-007** | 参数验证测试 | 连接参数验证 | TDengineConnectionStringBuilderTests.cs |
| **TC-SEC-008** | 资源泄漏测试 | 资源泄漏测试 | 长时间运行测试 |
| **TC-SEC-009** | 手动测试 | 并发安全测试 | 手动测试 |
| **TC-PERF-001** | Run | 连接建立性能测试 | Benchmark/Connect.cs |
| **TC-PERF-002** | Run | 简单查询性能测试 | Benchmark/Query.cs |
| **TC-PERF-003** | Run | 数据插入性能测试 | Benchmark/Insert.cs |
| **TC-PERF-004** | Run | 数据查询性能测试 | Benchmark/Query.cs |
| **TC-PERF-005** | Run | 参数绑定性能测试 | Benchmark/Prepare.cs |
| **TC-PERF-006** | 内存使用监控 | 内存使用监控 | Benchmark 项目 |
| **TC-COMP-001** | TDengine 3.3.6.0 兼容性测试 | TDengine 3.3.6.0 兼容性测试 | 版本兼容性测试 |
| **TC-COMP-002** | TDengine 最新版本兼容性测试 | TDengine 最新版本兼容性测试 | 版本兼容性测试 |
| **TC-COMP-003** | .NET Framework 4.6 兼容性测试 | .NET Framework 4.6 兼容性测试 | .NET 框架版本测试 |
| **TC-COMP-004** | .NET 6.0 兼容性测试 | .NET 6.0 兼容性测试 | .NET 版本测试 |
| **TC-COMP-005** | .NET 8.0 兼容性测试 | .NET 8.0 兼容性测试 | .NET 版本测试 |
| **TC-COMP-006** | 跨平台兼容性测试 | 跨平台兼容性测试 | 跨平台测试 |

**说明**：
1. 此映射表基于当前测试代码库的实际情况，随着测试代码的更新需要定期维护。
2. 部分测试用例可能对应多个测试方法，表中列出了代表性的测试。
3. 对于尚未实现的测试用例，标记为手动测试，需要在后续版本中补充。
4. 测试文件路径：
  - ADO.NET 接口测试：`test/Data.Tests/`
  - 驱动层功能测试：`test/Driver.Test/Function.Test/`
  - 性能测试：`test/Benchmark/`
1. Benchmark 项目提供性能测试基准，包括连接建立、查询、插入、参数绑定等性能测试。
**Benchmark 测试项目详情**：
- `Connect.cs`：连接建立性能测试，测量连接建立时间
- `Query.cs`：查询性能测试，包括简单查询和数据查询
- `Insert.cs`：数据插入性能测试，支持批量插入
- `Prepare.cs`：参数绑定性能测试
- `Aggregate.cs`：聚合查询性能测试
- `Batch.cs`：批量操作性能测试

## 8. 测试计划

1. 测试环境准备：0.5 人天
2. 测试执行：3 人天
3. 测试总结：2 人天

## 9. 风险评估

1. **技术风险**：依赖 TDengine 版本兼容性、第三方库更新、网络不稳定等。
2. **进度风险**：测试环境搭建延迟、缺陷修复周期过长、需求变更等。
3. **资源风险**：测试人员不足、测试设备短缺、时间预算不足等。
4. **依赖风险**：外部系统接口变化、数据库升级影响、第三方服务不可用等。

## 10. **参考文档**

- C# 连接器-Requirement Spec
- C# 连接器-Design Spec
- C# 连接器-Function Spec
- TDengine 官方文档：https://docs.taosdata.com/
- .NET 官方文档：https://learn.microsoft.com/zh-cn/dotnet/

## 11. **附录**

### 11.1 **测试代码示例**

```csharp
// 连接测试示例
[Fact]
public void ConnectionString_Property_Should_Set_ConnectionString_And_ConnectionStringBuilder()
{
    // Arrange
    string connectionString = "username=root;password=taosdata";
    var connection = new TDengineConnection("");

    // Act
    connection.ConnectionString = connectionString;

    // Assert
    Assert.Equal(connectionString, connection.ConnectionString);
    Assert.NotNull(connection.ConnectionStringBuilder);
    Assert.Equal(connectionString, connection.ConnectionStringBuilder.ConnectionString);
}

// 查询测试示例  
[Fact]
public void ExecuteReader_Should_Return_DataReader_With_Correct_Results()
{
    // Arrange
    using var connection = new TDengineConnection(TestConfig.ConnectionString);
    connection.Open();
    using var command = connection.CreateCommand();
    command.CommandText = "SELECT 1 as col1, 'test' as col2";

    // Act
    using var reader = command.ExecuteReader();

    // Assert
    Assert.True(reader.Read());
    Assert.Equal(1, reader.GetInt32(0));
    Assert.Equal("test", reader.GetString(1));
}
```

### 11.2 **测试工具与框架**

#### 11.2.1 **测试框架**

- **xUnit**：主要的单元测试框架
- **Moq**（可选）：用于创建模拟对象
- **coverlet**：代码覆盖率收集工具
- **BenchmarkDotNet**（可选）：性能基准测试

#### 11.2.2 **测试辅助工具**

- **MockWSServer**：模拟 WebSocket 服务器，用于隔离测试
- **TDengine 测试实例**：用于集成测试的真实 TDengine 实例
- **Docker**：用于快速部署测试环境

#### 11.2.3 **测试项目结构**

```plaintext
test/
├── Data.Tests/                    # ADO.NET 接口测试
│   ├── TDengineConnectionTests.cs
│   ├── TDengineCommandTests.cs
│   ├── TDengineDataReaderTests.cs
│   ├── TDengineParameterTests.cs
│   └── ...
├── Driver.Test/                  # 驱动层功能测试
│   ├── Function.Test/
│   │   ├── Query.cs
│   │   ├── QueryAsync.cs
│   │   ├── InsertCn.cs
│   │   ├── SMLRaw.cs
│   │   └── ...
│   └── Client/
└── Benchmark/                    # 性能基准测试
```

### 11.3 **测试数据管理**

#### 11.3.1 **测试数据库**

- 使用专用测试数据库，如 `test_db`
- 测试前自动创建，测试后自动清理
- 避免使用生产数据库

#### 11.3.2 **测试表结构**

- 创建标准测试表，包含所有 TDengine 数据类型
- 表结构示例：
```sql
CREATE TABLE test_table (
  ts TIMESTAMP,
  tiny_int TINYINT,
  small_int SMALLINT,
  int_col INT,
  big_int BIGINT,
  float_col FLOAT,
  double_col DOUBLE,
  bool_col BOOL,
  binary_col BINARY(50),
  nchar_col NCHAR(50),
  json_col JSON
)
```

#### 11.3.3 **测试数据准备**

- 正常数据：符合表结构的有效数据
- 边界数据：数据类型边界值（如最大/最小值）
- 异常数据：非法数据，用于测试错误处理
