# 数据接入适配工具-Test Spec

## 1. **修订记录**

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 2025-01-03 | 1.0 | 霍宏 | 文档第一版定稿 |
| 2025-12-20 | 1.1 | 王旭 | 基于设计文档更新测试用例 |

## 2. **测试目标**

1. 功能测试：验证 taosAdapter 所有接口和插件的功能正确性，包括 RESTful SQL、WebSocket SQL、schemaless 写入、stmt 操作、TMQ 订阅、InfluxDB/OpenTSDB 协议兼容、数据采集代理集成等。
2. 性能测试：验证 taosAdapter 的写入和查询性能达到需求指标（写入 QPS 40w/s，查询 QPS 1000/s），并在高并发场景下保持稳定。
3. 安全性测试：验证身份认证、TLS/SSL 加密、CORS 限制、查询限流、连接白名单等安全机制的有效性。
4. 稳定性测试：验证 taosAdapter 在长时间运行、高负载、异常输入等情况下的稳定性和可靠性，确保无内存泄漏和资源耗尽。
5. 兼容性测试：验证 taosAdapter 与不同 TDengine 版本、操作系统（Linux/Windows/macOS）、Go 版本、客户端库的兼容性。

## 3. **测试范围**

1. 功能测试：覆盖所有主要功能模块，包括控制器层（RESTful、WebSocket）、插件层（InfluxDB、OpenTSDB、collectd、StatsD、Prometheus 等）、数据库访问层（连接池、同步/异步接口）、CGO 封装层。
2. 性能测试：针对 RESTful 和 WebSocket 接口进行写入和查询压测，使用 taosBenchmark 和 JMeter 工具，测量吞吐量、延迟、资源使用率。
3. 安全性测试：测试 Basic 认证、Token 认证、TLS 连接、CORS 配置、查询限流、并发限制、IP 白名单等功能。
4. 稳定性测试：进行 7x24 小时长时间运行测试，模拟高并发、大数据量、网络异常等场景，监控进程稳定性、内存占用、CPU 使用率。
5. 兼容性测试：测试 taosAdapter 与 TDengine 3.3.0.0 及以上版本的兼容性，在不同 Linux 发行版（Ubuntu、CentOS、Debian）、Windows、macOS 上的运行情况。

## 4. **测试结论**

1. 功能测试：通过
2. 性能测试：通过
3. 安全性测试：通过
4. 稳定性测试：通过
5. 兼容性测试：通过

## 5. **已知问题和限制**

1. 单独部署 taosAdapter 需要安装 TDengine 客户端库。
2. 被弃用的接口仅为兼容保留，不添加新功能。
3. 查询结果过大时可能占用较多内存，需使用分块传输。

## 6. **测试环境**

| **系统** | **IP** | **部署** | **CPU** | **内存** | **硬盘** |
| --- | --- | --- | --- | --- | --- |
| CentOS 7.9 | 192.168.1.100 | TDengine 3.3.0.0 + taosAdapter | 8核 | 32GB | 500GB SSD |
| Ubuntu 20.04 | 192.168.1.101 | taosAdapter 独立部署 | 4核 | 16GB | 256GB SSD |
| Windows Server 2019 | 192.168.1.102 | taosAdapter 独立部署 | 4核 | 16GB | 256GB SSD |

## 7. **测试用例**

### 7.1 **功能测试**

| 测试类型 | **测试项** | 测试内容 | 预期结果 | **对应测试例** |
| --- | --- | --- | --- | --- |
| RESTful 接口 | RESTful SQL 执行 | 通过 HTTP POST 发送 SQL 语句，验证查询和写入功能 | 返回正确的 JSON 格式结果，包含 code、column_meta、data、rows 字段 | TestSql, TestRestful |
| RESTful 接口 | 身份认证 | 测试 Basic 认证和自定义 Token 认证 | 认证成功可执行 SQL，认证失败返回 401 | TestAuth |
| RESTful 接口 | 错误处理 | 发送非法 SQL、错误参数等 | 返回对应的错误码和错误描述 | TestErrorHandling |
| RESTful 接口 | 时区参数 | 使用 tz 参数指定时区 | 返回结果的时间戳按指定时区转换 | TestTimezone |
| RESTful 接口 | 请求 ID | 使用 req_id 参数传递请求 ID | 响应中包含相同的 req_id | TestReqId |
| WebSocket 接口 | 连接建立 | WebSocket 握手，发送 conn 请求 | 连接成功，返回版本信息 | TestWsConn |
| WebSocket 接口 | SQL 查询 | 通过 WebSocket 发送 query 请求执行 SQL | 返回查询结果，支持 fetch 分页获取 | TestWsQuery |
| WebSocket 接口 | schemaless 写入 | 通过 WebSocket 发送 insert 请求写入 schemaless 数据 | 写入成功，返回受影响行数 | TestWsSchemaless |
| WebSocket 接口 | stmt 操作 | stmt 初始化、准备、绑定、执行、关闭 | 执行成功，支持参数化查询和批量写入 | TestStmt |
| WebSocket 接口 | TMQ 订阅 | 建立 TMQ 订阅，消费消息 | 正常接收订阅数据 | TestTmq |
| WebSocket 接口 | 二进制协议 | 使用二进制协议进行数据传输 | 与 JSON 协议功能一致，性能更优 | TestBinaryProtocol |
| 插件接口 | InfluxDB 写入 | 通过 /influxdb/v1/write 接口写入 InfluxDB 行协议数据 | 数据成功写入 TDengine | TestInfluxDB |
| 插件接口 | OpenTSDB 写入 | 通过 /opentsdb/put 接口写入 JSON 和 telnet 格式数据 | 数据成功写入 TDengine | TestOpenTSDB |
| 插件接口 | collectd 集成 | collectd 发送数据到 /collectd 端点 | 数据成功写入 TDengine | TestCollectd |
| 插件接口 | StatsD 集成 | StatsD 发送指标到 /statsd 端点 | 指标成功写入 TDengine | TestStatsD |
| 插件接口 | Prometheus remote_write | Prometheus 通过 /prometheus/v1/remote_write 推送指标 | 指标成功写入 TDengine | TestPrometheusWrite |
| 插件接口 | Prometheus remote_read | 通过 /prometheus/v1/remote_read 读取指标 | 返回正确的指标数据 | TestPrometheusRead |
| 插件接口 | node_exporter 集成 | 收集 node_exporter 指标并写入 TDengine | 指标数据正确入库 | TestNodeExporter |
| 配置管理 | 配置文件 | 使用 taosadapter.toml 配置文件 | 配置项正确加载 | TestConfig |
| 配置管理 | 环境变量 | 通过环境变量覆盖配置 | 环境变量优先级高于配置文件 | TestEnvConfig |
| 配置管理 | 命令行参数 | 通过命令行参数设置配置 | 命令行参数优先级最高 | TestCLIConfig |
| 日志系统 | 日志级别 | 设置不同日志级别（trace/debug/info/warning/error） | 输出相应级别的日志 | TestLogLevel |
| 日志系统 | 日志轮转 | 日志文件按时间、大小轮转 | 轮转文件符合配置，无数据丢失 | TestLogRotation |
| 日志系统 | SQL 日志记录 | 启用 SQL 日志记录 | HTTP 请求中的 SQL 被记录到独立日志文件 | TestSqlLog |
| 连接池 | 连接池管理 | 创建连接池，获取/释放连接 | 连接复用，无泄漏 | TestConnectionPool |
| 连接池 | 白名单过滤 | 配置 IP 白名单，非白名单 IP 拒绝连接 | 白名单内 IP 可连接，外 IP 被拒绝 | TestWhitelist |
| 连接池 | 密码修改通知 | 用户密码修改后，连接池自动释放 | 旧连接池释放，新连接使用新密码 | TestPasswordChange |
| 监控检查 | 健康检查接口 | 访问 /ping 接口 | 返回 "pong" 和 200 状态码 | TestPing |
| 监控检查 | 指标暴露接口 | 访问 /metrics 接口 | 返回 Prometheus 格式的指标数据 | TestMetrics |
| 监控检查 | 内存阈值控制 | 内存使用超过阈值时暂停查询/写入 | 返回 503 错误，防止内存耗尽 | TestMemoryThreshold |
| 错误码映射 | HTTP 状态码映射 | 根据 httpCodeServerError 配置映射错误码 | 符合设计文档中的映射规则 | TestHttpCodeMapping |
| 分块传输 | 大结果集分块 | 查询结果集很大时使用 chunked 传输 | 数据完整，内存占用可控 | TestChunkedTransfer |

### 7.2 **性能测试**

| **测试项** | 测试内容 | 预期结果 | **对应测试例** |
| --- | --- | --- | --- |
| RESTful 写入性能 | 使用 taosBenchmark rest 模式，并发 1000，典型电表表结构，100w 子表，每子表写入 100 条随机数据 | QPS ≥ 40w/s | BenchmarkRestful |
| RESTful 查询性能 | 使用 JMeter 向 JDBC rest 请求，select last_row(*) from test.${tbname}，tbname 随机，并发 1000，循环 50 次 | QPS ≥ 1000/s | BenchmarkQuery |
| WebSocket 写入性能 | 通过 WebSocket 批量写入 schemaless 数据，高并发场景 | 吞吐量接近 RESTful 写入性能 | BenchmarkWsWrite |
| WebSocket 查询性能 | 通过 WebSocket 执行查询，高并发场景 | 吞吐量接近 RESTful 查询性能 | BenchmarkWsQuery |
| 连接池性能 | 高并发下连接池获取/释放操作 | 无竞争瓶颈，获取连接时间 < 10ms | BenchmarkConnectionPool |
| 内存占用 | 长时间运行，监控内存增长 | 内存占用稳定，无持续增长 | BenchmarkMemory |
| 启动时间 | taosAdapter 启动到接收请求的时间 | 启动时间 < 5 秒 | BenchmarkStartup |

### 7.3 **安全性测试**

| **测试项** | 测试内容 | 预期结果 | **对应测试例** |
| --- | --- | --- | --- |
| Basic 认证 | 使用正确的用户名密码和错误的凭据 | 正确凭据通过，错误凭据返回 401 | TestBasicAuth |
| Token 认证 | 使用 /rest/login 获取 token，用 token 访问接口 | token 有效期内可访问，过期或无效返回 401 | TestTokenAuth |
| TLS/SSL 加密 | 配置 HTTPS 和 WSS，使用证书连接 | 加密连接成功，数据传输加密 | TestTLS |
| CORS 配置 | 配置允许的源、头、方法，跨域请求 | 符合 CORS 配置的请求被允许，否则被拒绝 | TestCORS |
| 查询限流 | 配置查询限流，超过限制的查询被拒绝 | 限流生效，返回 503 或排队等待 | TestQueryLimiter |
| 并发限制 | 配置最大连接数，超过限制的连接被拒绝 | 并发连接数不超过配置值 | TestConcurrencyLimit |
| IP 白名单 | 配置 IP 白名单，非白名单 IP 无法连接 | 白名单内 IP 可访问，外 IP 被拒绝 | TestIPWhitelist |
| SQL 注入防护 | 尝试 SQL 注入攻击 | 输入被正确转义或拒绝，数据库安全 | TestSQLInjection |
| 路径遍历 | 尝试访问非授权路径 | 返回 404 或 403，无法访问敏感文件 | TestPathTraversal |

### 7.4 **稳定性测试**

| **测试项** | 测试内容 | 预期结果 | **对应测试例** |
| --- | --- | --- | --- |
| 长时间运行 | 连续运行 7 天，持续执行读写操作 | 无崩溃、内存泄漏、性能下降 | TestLongRunning |
| 高负载压力 | 持续高并发写入查询，达到性能极限 | 系统稳定，错误率 < 0.1% | TestStress |
| 网络异常 | 模拟网络闪断、延迟、丢包 | 自动重连，数据不丢失，恢复后正常 | TestNetworkFailure |
| 异常输入 | 发送畸形请求、超大报文、非法字符 | 正确处理异常，不崩溃，返回合适错误 | TestMalformedInput |
| 资源耗尽 | 模拟内存、文件描述符耗尽场景 | 优雅降级，返回 503，不崩溃 | TestResourceExhaustion |
| 配置热更新 | 运行时修改配置文件 | 配置生效，不影响现有连接 | TestHotReload |
| 进程信号 | 发送 SIGTERM、SIGINT 等信号 | 进程优雅关闭，释放资源 | TestProcessSignal |

### 7.5 **兼容性测试**

| **测试项** | 测试内容 | 预期结果 | **对应测试例** |
| --- | --- | --- | --- |
| TDengine 版本 | 与 TDengine 3.3.0.0、3.3.1.x、3.4.x 等版本配合 | 功能正常，无兼容性问题 | TestTDengineVersion |
| 操作系统 | Linux (Ubuntu/CentOS/Debian)、Windows、macOS | 编译、运行正常，功能一致 | TestOSCompatibility |
| Go 版本 | Go 1.23、1.24 等版本 | 编译通过，运行正常 | TestGoVersion |
| 客户端库 | 使用不同版本的 libtaos 客户端库 | 接口调用正常，无符号冲突 | TestClientLibrary |
| 数据采集代理 | Telegraf、collectd、StatsD、icinga2、TCollector 等 | 数据正确写入，无协议解析错误 | TestAgentIntegration |
| 浏览器兼容性 | 使用不同浏览器测试 WebSocket 接口 | WebSocket 连接正常，数据传输正确 | TestBrowserCompatibility |

## 8. **测试计划**

1. 测试环境搭建：0.5 人天（包括 TDengine 部署、taosAdapter 编译、测试工具安装）
2. 测试执行：5 人天
   - 功能测试 2 天
   - 性能测试 1 天
   - 安全性测试 0.5 天
   - 稳定性测试 1 天
   - 兼容性测试 0.5 天
3. 测试总结与报告：0.5 人天

## 9. **风险评估**

1. 性能不达标：若性能测试未达到需求指标，需要优化代码或调整配置。
2. 兼容性问题：新 TDengine 版本可能引入接口变化，需要适配。
3. 安全漏洞：可能存在未发现的安全隐患，需要定期安全审计。
4. 资源竞争：高并发下连接池、锁竞争可能成为瓶颈，需要优化。
5. 内存泄漏：长时间运行可能出现内存泄漏，需要详细内存分析。

## 10. **参考文档**

1. 数据接入适配工具-Design Spec
2. 数据接入适配工具-Function Spec
3. 数据接入适配工具-Requirement Spec
4. TDengine 官方文档：https://docs.taosdata.com/
5. Go 语言官方文档：https://golang.org/doc/
6. Gin Web 框架文档：https://gin-gonic.com/docs/
