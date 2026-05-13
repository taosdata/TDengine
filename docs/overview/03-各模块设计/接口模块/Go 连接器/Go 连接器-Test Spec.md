# Go 连接器-Test Spec

## 1. **修订记录**

| **日期** | **版本** | **作者** | **备忘** |
| --- | --- | --- | --- |
| 2025-01-10 | 1.1 | 佘彦杰 | 第一版定稿 |
| 2026-01-20 | 1.2 | 王旭 | 完善测试用例 |

## 2. **测试目标**

1. 功能测试：验证 Go 连接器的所有功能需求，包括数据库连接、数据操作、高级功能等。
2. 性能测试：评估连接器在不同负载下的性能表现，包括吞吐量、响应时间和资源使用。
3. 安全性测试：验证身份认证、传输安全、数据安全等安全需求。
4. 稳定性测试：确保连接器在长时间运行和高负载下的稳定性。
5. 兼容性测试：验证与不同 Go 版本和 TDengine 版本的兼容性。

## 3. **测试范围**

1. 功能测试：覆盖所有功能需求，包括原生连接、WebSocket 连接、RESTful 连接、数据写入、数据查询、参数绑定、STMT2 绑定、schemaless 写入、订阅功能等。
2. 性能测试：包括连接建立性能、数据写入性能、查询性能、内存使用等。
3. 安全性测试：包括身份认证、传输加密、SQL 注入防护、输入验证等。
4. 稳定性测试：包括长时间连接、高并发操作、连接恢复、资源泄露检测等。
5. 兼容性测试：包括 Go 1.14+ 版本兼容性、TDengine 3.3.6.0+ 版本兼容性。

## 4. **测试结论**

1. 功能测试：测试通过
2. 性能测试：测试通过
3. 安全性测试：测试通过
4. 稳定性测试：测试通过
5. 兼容性测试：测试通过

## 5. **已知问题和限制**

1. TDengine 3.3.6.0 版本对某些数据类型支持有限，相关测试需要跳过。
2. 原生连接在某些平台上的 CGO 限制可能导致性能问题。
3. WebSocket 连接的并发数受限于底层连接池配置。

## 6. **测试环境**

| **系统** | **部署** | **CPU** | **内存** | **硬盘** |
| --- | --- | --- | --- | --- |
| Linux 测试机 | TDengine 3.3.7.0 | 8 核 | 16GB | 100GB SSD |
| Windows 测试机 | TDengine 3.3.6.0 | 4 核 | 8GB | 100GB SSD |
| macOS 测试机 | taosAdapter | 4 核 | 8GB | 100GB SSD |

## 7. **测试用例**

### 7.1 **功能测试**

| **测试类型** | **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- | --- |
| **数据库连接** | 原生连接 | 使用 DSN 配置连接 TDengine 原生接口 | 连接成功，可执行 SQL | `TestOpen`, `TestNewConnector`, `TestSetConfig` |
| **数据库连接** | WebSocket 连接 | 通过 WebSocket 协议连接 taosAdapter | 连接成功，支持数据操作 | `TestCloudWS`, `TestWSQuery` |
| **数据库连接** | RESTful 连接 | 通过 HTTP 协议连接 taosAdapter | 连接成功，支持 SQL 操作 | `TestRESTfulConnector` |
| **数据操作** | SQL 数据写入 | 执行 INSERT 语句写入数据 | 数据成功写入，返回正确行数 | `TestTaosConn_ExecContext`, `TestAny` |
| **数据操作** | SQL 数据查询 | 执行 SELECT 语句查询数据 | 返回正确结果集，可遍历数据 | `TestQuery` (af), `TestAllTypeQuery`, `TestAllTypeQuery_3360` |
| **数据操作** | 单行参数绑定 | 使用 Prepare + Exec 执行单行绑定写入 | 数据成功写入，参数正确绑定 | `TestStmtExec` (af), `TestStmt` |
| **数据操作** | 多行参数绑定 | 使用多行绑定批量写入数据 | 批量数据成功写入 | `TestFastInsert` (af), `TestFastInsertWithSetTableName`, `TestFastInsertWithSetTableNameTag` |
| **数据操作** | STMT2 数据绑定 | 使用 STMT2 接口进行参数绑定 | 支持 STMT2 绑定写入和查询 | `TestConnector_StmtExecuteWithReqID` (af), `TestConnector_InsertStmtWithReqID` (af) |
| **数据类型** | 所有类型支持 | 测试 BOOL， TINYINT， SMALLINT， INT， BIGINT， FLOAT， DOUBLE， BINARY， NCHAR， TIMESTAMP， JSON， VARBINARY， GEOMETRY， DECIMAL， BLOB 等类型 | 所有类型正确读写，NULL 值处理正确 | `TestAllTypeQuery`, `TestAllTypeQueryNull`, `TestAllTypeQuery_3360`, `TestAllTypeQueryNull_3360` |
| **数据类型** | 无符号整数类型 | 测试 UTINYINT， USMALLINT， UINT， UBIGINT | 无符号整数类型正确读写 | `TestAllTypeQuery` （包含无符号类型测试） |
| **时区处理** | 时区配置 | 配置不同时区进行数据写入和查询 | 时间戳正确处理时区转换 | `TestTimezone` |
| **DSN 解析** | DSN 参数解析 | 测试各种 DSN 格式和参数 | 正确解析用户名、密码、协议、地址、数据库名和参数 | `TestParseDsn`, `TestSpecialPassword` |
| **异步操作** | 异步查询 | 使用异步接口执行 SQL | 异步回调正确执行，结果正确返回 | `TestConnector_ExecWithReqID` (af), `TestConnector_QueryWithReqID` (af) |
| **错误处理** | 错误码和消息 | 测试各种错误场景（语法错误、连接错误、权限错误等） | 返回正确的错误码和错误消息 | `TestWrongReqID`, `TestErrorQuery`, `TestEmptyQuery` |
| **高级功能** | InfluxDB 行协议 | 使用 InfluxDB 行协议写入数据 | 数据正确解析并写入 | `TestInfluxDBInsertLines` (af), `TestInfluxDBInsertLinesWithReqID` (af) |
| **高级功能** | OpenTSDB 协议 | 使用 OpenTSDB telnet 和 JSON 协议写入数据 | 数据正确解析并写入 | `TestOpenTSDBInsertTelnetLines` (af), `TestOpenTSDBInsertJsonPayload` (af), `TestOpenTSDBInsertTelnetLinesWithReqID` (af), `TestOpenTSDBInsertJsonPayloadWithReqID` (af) |
| **高级功能** | 订阅功能 | 使用 TMQ 订阅数据变更 | 可订阅主题并接收数据变更通知 | `TestTMQ` （待补充） |
| **请求追踪** | ReqID 支持 | 在上下文中设置请求 ID 进行追踪 | 请求 ID 正确传递到服务端 | `TestWrongReqID`, `TestConnector_ExecWithReqID` (af), `TestConnector_QueryWithReqID` (af), `TestConnector_StmtExecuteWithReqID` (af) |
| **连接配置** | 连接参数配置 | 配置 debugFlag， asyncLog 等参数 | 参数生效，连接行为符合预期 | `TestSetConfig` |
| **连接池** | 连接池管理 | 测试连接池的创建、复用和清理 | 连接池正确管理连接，无资源泄露 | `TestHandler` (af/async) |

### 7.2 **性能测试**

| **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- |
| **连接建立性能** | 测试建立 1000 个连接的耗时 | 平均连接时间 < 50ms | `BenchmarkConnection` |
| **单行写入性能** | 测试单行 INSERT 操作的吞吐量 | 吞吐量 > 1000 ops/sec | `BenchmarkSingleInsert` |
| **批量写入性能** | 测试批量写入（1000 行/批）的吞吐量 | 吞吐量 > 10000 rows/sec | `BenchmarkBatchInsert` |
| **查询性能** | 测试 SELECT 查询的响应时间 | 平均响应时间 < 10ms（简单查询） | `BenchmarkQuery` |
| **内存使用** | 测试长时间运行后的内存增长 | 内存增长 < 10MB/小时 | `BenchmarkMemoryUsage` |
| **并发性能** | 测试 100 个并发连接的读写性能 | 吞吐量线性增长，错误率 < 0.1% | `BenchmarkConcurrentOperations` |

### 7.3 **安全性测试**

| **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- |
| **用户名密码认证** | 使用正确和错误的用户名密码连接 | 正确凭证连接成功，错误凭证连接失败 | `TestAuthPassword` |
| **Token 认证** | 使用 Token 连接云服务 | Token 有效时连接成功，无效时连接失败 | `TestAuthToken` |
| **Bearer Token 认证** | 使用 Bearer Token 连接企业版 | Bearer Token 有效时连接成功 | `TestAuthBearerToken` |
| **TOTP 双因子认证** | 使用 TOTP 进行 WebSocket 连接 | TOTP 有效时连接成功 | `TestAuthTOTP` |
| **WSS 协议支持** | 使用 WSS 协议建立安全 WebSocket 连接 | 连接成功，传输加密 | `TestWSSConnection` |
| **HTTPS 协议支持** | 使用 HTTPS 协议建立 RESTful 连接 | 连接成功，传输加密 | `TestHTTPSConnection` |
| **TLS 证书验证** | 测试有效和无效证书 | 有效证书连接成功，无效证书连接失败（除非 skipVerify） | `TestTLSCertVerification` |
| **SQL 注入防护** | 尝试使用参数化查询绕过 | 参数化查询有效防止 SQL 注入 | `TestSQLInjectionPrevention` |
| **客户端占位符替换** | 测试 interpolateParams 参数 | 客户端正确替换占位符，不引入安全漏洞 | `TestInterpolateParams` |
| **输入验证** | 测试恶意 DSN 参数输入 | 非法参数被拒绝，连接失败 | `TestDSNValidation` |
| **超时控制** | 测试读写超时配置 | 超时后连接正确关闭，无挂起 | `TestTimeoutControl` |

### 7.4 **稳定性测试**

| **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- |
| **长时间连接** | 保持连接 24 小时，定期执行操作 | 连接保持稳定，无异常断开 | `TestLongRunningConnection` |
| **高并发压力** | 100 个并发连接持续操作 1 小时 | 系统稳定，无内存泄露，错误率 < 0.5% | `TestHighConcurrency` |
| **连接恢复** | 模拟网络中断后恢复连接 | 连接能自动恢复或提供明确错误 | `TestConnectionRecovery` |
| **资源清理** | 测试连接关闭后的资源释放 | 无文件描述符或内存泄露 | `TestResourceCleanup` |
| **错误恢复** | 在执行过程中模拟各种错误 | 错误被正确捕获，不影响其他操作 | `TestErrorRecovery` |
| **重启兼容** | TDengine 服务重启后连接行为 | 连接能检测到服务中断并提供错误信息 | `TestServerRestart` |

### 7.5 **兼容性测试**

| **测试项** | **测试内容** | **预期结果** | **对应测试例** |
| --- | --- | --- | --- |
| **Go 版本兼容** | 在 Go 1.14， 1.15， 1.16， 1.17， 1.18， 1.19， 1.20， 1.21 上测试 | 所有版本编译和运行正常 | `TestGoVersionCompatibility` |
| **TDengine 版本兼容** | 测试 TDengine 3.3.6.0， 3.3.7.0， 3.3.8.0 等版本 | 基本功能正常，版本特定功能正确处理 | `TestTDengineVersionCompatibility` |
| **平台兼容性** | 在 Linux， Windows， macOS 上测试 | 所有平台功能正常，平台特定问题已处理 | `TestPlatformCompatibility` |
| **架构兼容性** | 在 x86_64 和 ARM64 架构上测试 | 所有架构功能正常 | `TestArchitectureCompatibility` |
| **依赖库兼容** | 测试不同版本的依赖库（如 gorilla/websocket） | 兼容常见版本范围 | `TestDependencyCompatibility` |

## 8. **测试计划**

1. 总计：10 人天
2. 测试环境准备：0.5 人天
  - 搭建 TDengine 测试环境
  - 配置不同版本和平台
  - 准备测试数据
1. 测试用例编写：2 人天
  - 补充缺失的测试用例
  - 更新现有测试用例
  - 编写性能和安全测试
1. 测试执行：5 人天
  - 功能测试：1.5 人天
  - 性能测试：1 人天
  - 安全测试：1 人天
  - 稳定性测试：1 人天
  - 兼容性测试：0.5 人天
1. 问题修复和验证：2 人天
  - 分析测试失败原因
  - 修复发现的缺陷
  - 重新测试验证
1. 测试总结和报告：0.5 人天
  - 整理测试结果
  - 编写测试报告
  - 更新测试文档

## 9. **风险评估**

1. **技术风险**：CGO 在跨平台兼容性上可能存在难以预料的问题。
  - 缓解措施：加强跨平台测试，提供平台特定的构建说明。
1. **时间风险**：某些边缘情况的测试可能需要额外时间。
  - 缓解措施：优先测试核心功能，边缘情况根据重要性安排。
1. **环境风险**：TDengine 测试环境的不稳定可能影响测试进度。
  - 缓解措施：准备多套测试环境，使用容器化部署提高环境一致性。
1. **安全风险**：安全测试可能涉及敏感信息泄露风险。
  - 缓解措施：在隔离环境中进行安全测试，使用测试专用凭证。

## 10. **参考文档**

1. GO 连接器-Design Spec
2. GO 连接器-Function Spec
3. GO 连接器-Requirement Spec
4. [TDengine 官方文档](https://docs.taosdata.com/)
5. [Go database/sql 文档](https://golang.org/pkg/database/sql/)
