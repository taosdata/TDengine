# 2026 Roadmap - 工具

## HighLights

### 2026 Q1

1. 授权服务：中心化授权服务，支持 TSDB、IDMP 独立授权
2. 认证：Explorer 支持 TOTP 认证，连接器、taosX 支持 TOKEN 认证
3. 安全加固：Explorer 明文密码、SQL 注入问题修复，taosX 安全加固，Adapter、连接器安全加固：明文密码、日志信息防信息泄漏，连接器安全开发用户指南等
4. 漏洞扫描和修复：adapter/连接器/taosx 第三方依赖漏洞扫描和修复，Web 端口漏洞扫描和修复，棱镜七彩工具接入 CI
5. taosX：适配 TSDB 权限管理，Windows 适配，扩展 Transform 解析功能，导出导入顺序一致性优化，力控实时库，KingHistorian 数据源优化，MQTT 支持多个 Broker 等

### 2026 Q2

1. 最佳实践：全链路认证，全链路传输安全，全链路高可用
2. XNODE：平滑迁移、可观测性优化，稳定性测试，性能测试，数据源负载均衡，数据源开发指南，Transform 系列文档
3. 连接器和工具：
   - 排查工具和相关优化：连接器上报类型和版本，TDinsight 添加新统计指标
   - 测试工具和报告：订阅测试工具，负载均衡达成情况测试，taosShell/Dump/taosgen 测试
   - 功能补足和性能优化：STMT2 接口支持补足和 STMT2 性能优化，Decimal/BLOB 类型支持补足

### 2026 Q3

1. 最佳实践：全链路高可靠
2. XNODE：嵌套 JSON 解析能力扩展，数据源支持 IOTDB，支持 TMQ 发不到 Kafka，日志和性能指标优
3. Explorer：支持高可用
4. 连接器：兼容 MySQL 协议，其他 STMT2、订阅、集成能力、性能测试等方面的优化

### 2026 Q4

1. 最佳实践：全链路压缩、全链路负载均衡
2. 数据源：支持 modbus 协议、InfluxDB 3.0
3. taosx：逻辑备份和恢复性能优化，类型支持补全
4. 连接器：生态集成扩展，Python、Rust、C# 、ODBC 连接器优化

## Details

<!-- Unsupported block type: 54 -->
