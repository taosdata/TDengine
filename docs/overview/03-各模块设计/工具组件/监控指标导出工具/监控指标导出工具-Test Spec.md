# 监控指标导出工具-Test Spec

## 1. **修订记录**

| 日期 | 版本 | 作者 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025-01-08 | 1.0 | 聂敏慧 | 第一版定稿 |
| 2026-01-14 | 1.1 | 王旭 | 完善测试用例 |

## 2. **测试目标**

1. 功能测试：验证 taosKeeper 所有核心功能模块的正确性，包括监控数据接收、存储、查询、系统指标采集等
2. 性能测试：验证 taosKeeper 在高并发场景下的性能和资源消耗
3. 安全性测试：验证 taosKeeper 的安全机制，包括认证、授权、数据加密等
4. 稳定性测试：验证 taosKeeper 在长时间运行和高负载下的稳定性
5. 兼容性测试：验证 taosKeeper 与不同操作系统、TDengine 版本、硬件的兼容性

## 3. **测试范围**

1. 功能测试：覆盖所有 RESTful API 接口、数据存储、系统监控、日志记录、配置管理等功能
2. 性能测试：测试接口响应时间、并发处理能力、内存使用、CPU 占用等性能指标
3. 安全性测试：测试 SSL/TLS 支持、认证授权、输入验证、错误处理等安全特性
4. 稳定性测试：测试长时间运行、高并发压力、异常恢复等稳定性场景
5. 兼容性测试：测试不同操作系统（Linux, Windows, macOS）、不同 TDengine 版本（3.0+）、不同硬件架构的兼容性

## 4. **测试结论**

1. 功能测试：通过
2. 性能测试：通过
3. 安全性测试：通过
4. 稳定性测试：通过
5. 兼容性测试：通过

## 5. **已知问题和限制**

1. 当前测试主要关注单元测试和集成测试，端到端测试需要完整的 TDengine 环境
2. 性能测试需要专用测试环境和负载生成工具
3. 企业版功能（如审计、Zabbix 集成）需要企业版许可证

## 6. **测试环境**

| **系统** | **IP** | **部署** | **CPU** | **内存** | **硬盘** |
| --- | --- | --- | --- | --- | --- |
| CentOS 7.9 | 192.168.1.100 | taosKeeper + TDengine 3.3.0 | 4核 | 8GB | 100GB |
| Ubuntu 20.04 | 192.168.1.101 | taosKeeper 测试客户端 | 2核 | 4GB | 50GB |
| Windows Server 2019 | 192.168.1.102 | 兼容性测试 | 4核 | 8GB | 100GB |

## 7. **测试用例**

### 7.1 **功能测试**

| 测试类型 | **测试项** | 测试内容 | 预期结果 | **对应测试例** |
| --- | --- | --- | --- | --- |
| API 接口测试 | adapter_report 接口 | 测试 taosAdapter 监控数据上报接口 | 1. 正常数据上报成功，返回200 2. 异常数据返回400 3. 无连接时返回500 4. 数据正确存储到 TDengine | TestAdapter2 TestAdapter_handleFunc_NoConnection_Returns500 TestAdapter_handleFunc_ParseError_ReturnsBadRequest TestAdapter_createTable_NoConnection_ReturnsErrNoConnection |
| API 接口测试 | audit 接口 | 测试审计数据上报接口 | 1. 单条审计数据上报 2. 批量审计数据上报 3. 自定义数据库配置 4. Token 认证支持 5. 各种数据类型和边界值处理 | TestAudit TestAuditBatchCustomDB TestAuditV2CustomDBWithToken TestAuditInfo_AffectedRowsAndDurationVariants TestAudit_handleBatchFunc_GetRawDataError_Returns400 |
| API 接口测试 | check_health 接口 | 测试健康检查接口 | 1. 接口返回200状态码 2. 响应中包含版本信息 | TestCheckHealthInit |
| API 接口测试 | 通用指标接口 | 测试通用监控指标上报 | 1. 集群基础信息上报 2. 慢查询记录上报 3. 批量指标处理 4. 表名生成和标签处理 | TestClusterBasic TestGeneralMetric_handleSlowSqlDetailBatch TestGetSubTableName Test_writeTags |
| API 接口测试 | Zabbix 集成接口 | 测试 Zabbix 监控数据导出 | 1. /zabbix/float 接口返回正确格式 2. /zabbix/string 接口返回正确格式 3. 标签排序功能 | TestZabbixInit TestZabbix_sortLabel |
| 数据存储测试 | 数据库连接器 | 测试 TDengine 连接功能 | 1. IPv6 地址支持 2. 连接重试机制 3. Token 认证 4. 特殊字符处理 5. 错误处理和数据日志 | TestIPv6 TestExecuteWithRetry TestNewConnectorWithDbAndToken TestNewConnectorWithSpecialChars Test_logData_Success_LogsJSONTrace |
| 系统监控测试 | 系统指标采集 | 测试 CPU/内存监控 | 1. 正常环境监控 2. CGroup 环境监控 3. 指标值在合理范围内 4. 监控启动和注册 | TestNormalCollectorCpuPercentRange TestNormalCollectorMemPercentRange TestNewNormalCollector TestStart |
| 配置管理测试 | 配置文件解析 | 测试配置加载和解析 | 1. TOML 配置解析 2. 配置文件备份 3. 默认值设置 4. SSL 配置测试 | TestConfig TestBakConfig TestInitSSL_ParseConfigFile TestInitSSL_WithEnvVars |
| 日志系统测试 | 日志记录功能 | 测试日志配置和输出 | 1. 日志级别设置 2. 日志格式化 3. Gin 中间件日志 4. 异常恢复日志 5. 文件 Hook 和错误处理 | TestConfigLog TestTaosLogFormatterFormat TestGinLog_StatusNot200_TriggersErrorPath TestGinRecoverLog_HandlesPanicWithWriter TestFileHookFire_ReturnsFlushError |
| 指标处理测试 | 指标转换和导出 | 测试监控指标处理逻辑 | 1. 数据类型转换 2. Prometheus 指标导出 3. 角色状态转换 4. 状态字符串转换 5. 指标构建和扩展 | Test_i2string Test_i2float TestProcessorCollect_Gauge_SkipsNilAndEmitsMetric Test_getRoleStr Test_getStatusStr TestExpandMetricsFromConfig |
| 工具函数测试 | 工具函数 | 测试公共工具函数 | 1. IP 地址处理 2. 查询 ID 生成 3. 表名处理 4. 数字解析 5. 表名长度限制测试 6. 安全子字符串处理 | TestHandleIp TestGetQidOwn_CounterWraps_ResetsToOne TestAdapter_tableName TestParseUint_NegativeWithinRange_ReturnsZeroNil TestCreateClusterInfoSql TestSafeSubstring |

### 7.2 **性能测试**

| **测试项** | 测试内容 | 预期结果 | **对应测试例** |
| --- | --- | --- | --- |
| 接口响应时间 | 测试各 API 接口在正常负载下的响应时间 | 95% 的请求响应时间 < 100ms | 手动测试 |
| 并发处理能力 | 测试 adapter_report 接口的并发处理能力 | 支持 1000+ 并发连接，无数据丢失 | 手动测试 |
| 内存使用 | 测试长时间运行下的内存增长情况 | 内存使用稳定，无内存泄漏 | 手动测试 |
| CPU 占用 | 测试高负载下的 CPU 使用率 | CPU 使用率 < 70% (4核) | 手动测试 |
| 数据存储性能 | 测试监控数据写入 TDengine 的性能 | 写入延迟 < 50ms，吞吐量 > 1000 records/s | 手动测试 |

### 7.3 **安全性测试**

|  |  |  |  |
| --- | --- | --- | --- |
| **测试项** | 测试内容 | 预期结果 | **对应测试例** |
| SSL/TLS 支持 | 测试 HTTPS 连接和数据加密 | 支持 SSL/TLS，数据传输加密 | TestHttps |
| 输入验证 | 测试异常输入处理 | 恶意输入被正确拒绝，不导致程序崩溃 | TestAdapter_handleFunc_ParseError_ReturnsBadRequest TestAudit_handleFunc_ParseOldAuditError_ReturnsBadRequest |
| 认证授权 | 测试数据库连接认证 | Token 认证和用户名密码认证都正常工作 | TestNewConnectorWithDbAndToken |
| 错误处理 | 测试异常情况下的错误处理 | 错误信息不泄露敏感信息，程序保持稳定 | TestAdapter_handleFunc_NoConnection_Returns500 TestAudit_handleFunc_NoConnection_Returns500 TestGeneralMetric_handleFunc_NoConnection_Returns500 |
| 日志安全 | 测试日志中的敏感信息过滤 | 密码、Token 等敏感信息不在日志中明文记录 | 手动测试 |

### 7.4 **稳定性测试**

| **测试项** | 测试内容 | 预期结果 | **对应测试例** |
| --- | --- | --- | --- |
| 长时间运行 | 连续运行 72 小时，监控资源使用 | 内存、CPU 使用稳定，无崩溃 | 手动测试 |
| 高并发压力 | 持续高并发请求 24 小时 | 服务保持稳定，无请求失败 | 手动测试 |
| 异常恢复 | 模拟数据库连接中断后恢复 | 自动重连成功，数据不丢失 | TestExecuteWithRetry TestConnectorQuery_ErrorPath_NoAuthExit_ReturnsError |
| 配置热更新 | 测试配置文件更新不重启服务 | 配置变更生效，服务不中断 | 手动测试 |
| 数据一致性 | 测试监控数据不丢失、不重复 | 所有上报数据正确存储，无丢失或重复 | TestAdapter2 Test_adapterTableSql |

### 7.5 **兼容性测试**

| **测试项** | 测试内容 | 预期结果 | **对应测试例** |
| --- | --- | --- | --- |
| 操作系统兼容性 | 测试 Linux/Windows/macOS 兼容性 | 在各操作系统上正常安装运行 | 手动测试 |
| TDengine 版本兼容 | 测试不同 TDengine 版本兼容性 | 支持 TDengine 3.0+ 版本 | 手动测试 |
| 硬件架构兼容 | 测试 x86/ARM 架构兼容性 | 在不同 CPU 架构上正常运行 | 手动测试 |
| 容器环境兼容 | 测试 Docker/Kubernetes 环境 | 在容器环境中正常运行 | TestStart (CGroup 支持) |
| 浏览器兼容性 | 测试 Web 接口的浏览器兼容性 | 主流浏览器正常访问接口 | 手动测试 |

## 8. **测试计划**

1. 总计：9 人天
2. 测试环境搭建：0.5 人天
3. 功能测试执行：3 人天
4. 性能测试执行：1 人天
5. 安全性测试执行：0.5 人天
6. 稳定性测试执行：2 人天
7. 兼容性测试执行：1 人天
8. 测试总结和报告：1 人天

## 9. **风险评估**

1. **环境依赖风险**：测试需要完整的 TDengine 环境，环境搭建可能耗时
  - 缓解措施：提前准备测试环境，使用 Docker 容器化环境
1. **性能测试工具风险**：性能测试需要专用工具，可能学习成本高
  - 缓解措施：使用成熟的性能测试工具如 JMeter、wrk
1. **企业版功能测试风险**：部分功能需要企业版许可证
  - 缓解措施：与企业版团队协调，获取测试许可证
1. **兼容性测试覆盖风险**：难以覆盖所有操作系统和硬件组合
  - 缓解措施：选择主流配置进行测试，使用云服务器覆盖不同环境

## 10. **参考文档**

1. 监控指标导出工具-Design Spec
2. 监控指标导出工具-Function Spec
3. 监控指标导出工具-Requirement Spec
4. taosKeeper 用户文档
