# 数据管道工具-Test Spec

## 1. **修订记录**

| 日期 | 版本 | 作者 | 主要修改内容 |
| --- | --- | --- | --- |
| 2026-02-09 | 1.0 | 聂敏慧 | 文档第一版定稿 |
| 2025-12-25 | 1.1 | 王旭 | 完善测试用例 |

## 2. **测试目标**

1. **功能测试**：验证 taosX 所有功能模块的正确性，包括数据迁移、数据同步、数据备份恢复、数据导入导出、节点管理、任务管理、Agent 管理等。
2. **性能测试**：验证 taosX 在高负载下的性能表现，包括数据迁移速度、数据同步速度、任务分片切换时间等关键指标。
3. **安全性测试**：验证 taosX 的安全机制，包括认证授权、通信加密、数据保护等。
4. **稳定性测试**：验证 taosX 在长时间运行和高并发场景下的稳定性，包括故障恢复、资源泄漏等。
5. **兼容性测试**：验证 taosX 与不同 TDengine 版本、不同操作系统、不同数据源的兼容性。

## 3. **测试范围**

### 3.1 **功能测试**

1. 数据迁移功能（2.x 到 3.x，3.x 到 3.x，实时查询迁移）
2. 数据同步功能（数据库订阅、超级表订阅、查询语句订阅）
3. 双活功能
4. 数据备份与恢复功能
5. 数据导出与导入功能（CSV、PI Archive、OpenTSDB、InfluxDB、Kafka、MQTT 等）
6. 节点管理功能（XNode 添加、删除、状态管理）
7. 任务分片与负载均衡功能
8. 任务管理功能（SQL 创建、启停、删除任务）
9. Agent 管理功能
10. 服务降级功能
11. 旧版本数据迁移功能

### 3.2 **性能测试**

1. 数据迁移性能（1000 万点/秒）
2. 数据同步性能（100 万点/秒）
3. CSV 数据导入性能（100 万点/秒）
4. 任务分片切换时间（<30 秒）
5. MNode 切换恢复时间（<10 秒）

### 3.3 **安全性测试**

1. XNode 与 MNode 认证授权
2. Agent 与 XNode 认证
3. 用户权限控制
4. 传输加密（TLS/SSL）
5. 双向认证（mTLS）
6. DSN 密码保护
7. 共享存储安全

### 3.4 **稳定性测试**

1. 长时间运行稳定性（72 小时以上）
2. 高并发任务稳定性
3. 故障场景恢复（XNode 宕机、MNode 切换、网络分区）
4. 资源泄漏检测

### 3.5 **兼容性测试**

1. TDengine 版本兼容性（2.0.20+、2.2、2.4、2.6、3.0+）
2. 操作系统兼容性（Linux/Windows/macOS/国产 OS）
3. CPU 架构兼容性（x86_64、arm64、龙芯等）
4. 数据源兼容性（各版本 InfluxDB、OpenTSDB、Kafka 等）

## 4. **测试结论**

| 测试类型 | 测试结果 | 备注 |
| --- | --- | --- |
| 功能测试 | 通过 | 所有核心功能验证通过 |
| 性能测试 | 通过 | 满足性能指标要求 |
| 安全性测试 | 通过 | 安全机制完整有效 |
| 稳定性测试 | 通过 | 系统稳定可靠 |
| 兼容性测试 | 通过 | 兼容性良好 |

## 5. **已知问题和限制**

1. 部分数据源插件（如 Oracle、MSSQL）的测试环境搭建较为复杂，需要特定许可证。
2. 高可用架构测试需要多节点 TDengine 集群环境，测试资源要求较高。
3. 性能测试结果受硬件配置、网络环境等因素影响较大。

## 6. **测试环境**

### 6.1 硬件环境

| 系统 | IP | 部署 | CPU | 内存 | 硬盘 | 网络 |
| --- | --- | --- | --- | --- | --- | --- |
| CentOS 7.9 | 192.168.1.101 | TDengine MNode （3 节点） | 8 核 | 32GB | 500GB SSD | 千兆 |
| Ubuntu 20.04 | 192.168.1.102 | XNode 节点 1 | 4 核 | 16GB | 200GB SSD | 千兆 |
| Ubuntu 20.04 | 192.168.1.103 | XNode 节点 2 | 4 核 | 16GB | 200GB SSD | 千兆 |
| Windows Server 2019 | 192.168.1.104 | 客户端测试机 | 4 核 | 8GB | 500GB HDD | 千兆 |
| 共享存储服务器 | 192.168.1.105 | NFS 共享存储 | 4 核 | 16GB | 2TB SSD | 千兆 |

### 6.2 **软件环境**

1. TDengine 版本：3.4.0.0 企业版
2. taosX 版本：1.1.0
3. 数据库：TDengine 2.6.0.40、3.2.2.0、3.4.0.0
4. 数据源：InfluxDB 1.8、Kafka 2.8、MQTT 5.0、MySQL 8.0、PostgreSQL 14

## 7. **测试用例**

### 7.1 **功能测试**

| 测试类型 | 测试项 | 测试内容 | 预期结果 | 对应测试例 |
| --- | --- | --- | --- | --- |
| 数据迁移 | 2.x 到 3.x 历史数据迁移 | 将 TDengine 2.x 数据库数据迁移到 3.x 集群 | 数据完整迁移，无数据丢失 | tests/td2td.rs |
|  | 3.x 到 3.x 历史数据迁移 | 将 TDengine 3.x 数据库数据迁移到另一个 3.x 集群 | 数据完整迁移，保持一致性 | tests/integration/e2e/ |
|  | 实时查询数据迁移 | 按周期查询最新数据并迁移 | 实时数据正确同步 | tests/integration/e2e/ |
|  | 新增常量标签支持 | 迁移过程中新增标签字段 | 目标表包含新增标签 | tests/integration/e2e/ |
|  | 表名变换支持 | 迁移过程中重命名表名 | 目标表名正确变更 | tests/integration/e2e/ |
| 数据同步 | 数据库订阅同步 | 订阅整个数据库到其他集群 | 数据实时同步 | tests/active-active/ |
|  | 超级表订阅同步 | 订阅超级表到其他集群 | 超级表及子表同步 | tests/active-active/ |
|  | 查询语句订阅同步 | 订阅查询结果到其他集群 | 查询结果正确同步 | tests/tmq2td.rs |
| 双活 | 双集群数据同步 | 两个集群间双向数据同步 | 数据最终一致性 | tests/active-active/ |
| 数据备份 | 3.x 数据备份到文件 | 备份数据库到本地文件 | 备份文件可读且完整 | tests/integration/e2e/ |
| 数据恢复 | 备份文件恢复到集群 | 从备份文件恢复数据 | 数据完整恢复 | tests/integration/e2e/ |
| 数据导出 | 查询结果导出 CSV | 导出 TDengine 查询结果 | CSV 文件格式正确 | tests/integration/e2e/ |
| 数据导入 | CSV 导入 TDengine | 导入 CSV 文件到数据库 | 数据正确导入 | tests/integration/e2e/ |
|  | PI Archive 数据导入 | 导入 PI Archive 数据 | PI 数据正确解析导入 | tests/pi/ |
|  | OpenTSDB 数据导入 | 导入 OpenTSDB 数据 | OpenTSDB 数据转换正确 | tests/integration/datasources/ |
|  | InfluxDB 数据导入 | 导入 InfluxDB 数据 | InfluxDB 行协议解析正确 | tests/integration/datasources/ |
|  | Kafka 数据导入 | 导入 Kafka 消息数据 | Kafka 消息正确消费 | tests/kafka/ |
|  | MQTT 数据导入 | 导入 MQTT 消息数据 | MQTT 消息正确订阅 | tests/mqtt/ |
| API 测试 | REST API 基本功能 | 健康检查、任务管理 API | API 响应正确 | tests/api/basic.rs |
|  | REST API 扩展功能 | 文件上传下载、高级任务操作 | 扩展功能正常 | tests/api/extended.rs |
| 节点管理 | 添加 XNode 节点 | 通过 CREATE XNODE 注册节点 | 节点成功注册 | tests/integration/e2e/ |
|  | 删除 XNode 节点 | 通过 DROP XNODE 移除节点 | 节点成功移除 | tests/integration/e2e/ |
|  | 查看 XNode 状态 | SHOW XNODES 查看节点列表 | 节点状态信息正确 | tests/integration/e2e/ |
|  | DRAIN 模式设置 | 设置节点为 DRAIN 模式 | 节点停止接收新任务 | tests/integration/e2e/ |
| 任务分片 | 自动分片功能 | 任务根据数据源自动分片 | 分片逻辑正确 | tests/integration/e2e/ |
|  | 消费者策略分片 | Kafka/TMQ 按分区消费分片 | 分区消费均衡 | tests/kafka/ |
| 负载均衡 | 分片负载均衡 | 分片在不同 XNode 分布 | 负载均衡策略生效 | tests/integration/e2e/ |
|  | 故障自动迁移 | XNode 宕机时分片迁移 | 分片 30 秒内迁移完成 | tests/integration/e2e/ |
| 任务管理 | SQL 创建任务 | CREATE XNODE TASK 创建任务 | 任务成功创建 | tests/integration/e2e/ |
|  | SQL 启停任务 | START/STOP XNODE TASK 启停 | 任务状态正确变更 | tests/integration/e2e/ |
|  | 查看任务分片 | SHOW XNODE TASKS/JOBS | 任务和分片信息正确 | tests/integration/e2e/ |
| Agent 管理 | Agent 创建管理 | CREATE XNODE AGENT 创建 Agent | Agent 成功注册 | tests/integration/e2e/ |
|  | Agent 连接认证 | Agent 与 XNode 认证通信 | 认证机制有效 | tests/tls/ |
| 服务降级 | 只读模式测试 | MNode 不满足 RAFT 时降级 | 只读模式功能正常 | tests/integration/e2e/ |
| 数据迁移 | 旧版本数据迁移 | taosx migrate 迁移 SQLite 数据 | 数据成功迁移到 MNode | tests/tools/ |

### 7.2 **性能测试**

| 测试项 | 测试内容 | 预期结果 | 对应测试例 |
| --- | --- | --- | --- |
| 数据迁移性能 | 1000 万数据点/秒迁移速度 | 达到或超过 1000 万点/秒 | tests/perf_test.rs |
| 数据同步性能 | 100 万点/秒同步速度 | 达到或超过 100 万点/秒 | tests/performance/ |
| CSV 导入性能 | 100 万点/秒导入速度 | 达到或超过 100 万点/秒 | tests/performance/ |
| 任务分片切换时间 | XNode 故障后分片迁移时间 | <30 秒完成迁移 | tests/integration/e2e/ |
| MNode 切换恢复时间 | MNode Leader 切换后恢复 | <10 秒恢复服务 | tests/integration/e2e/ |
| 高并发任务处理 | 同时运行 100 个任务 | 系统稳定，资源使用正常 | tests/performance/ |
| 大数据量处理 | 处理 TB 级别数据迁移 | 无内存泄漏，进度正常 | tests/performance/ |

### 7.3 **安全性测试**

| 测试项 | 测试内容 | 预期结果 | 对应测试例 |
| --- | --- | --- | --- |
| XNode-MNode 认证 | JWT Token 双向认证 | 认证成功，未授权访问拒绝 | tests/tls/ |
| Agent-XNode 认证 | Token 认证与 IP 白名单 | 有效 Token 可连接，无效拒绝 | tests/tls/ |
| 用户权限控制 | 不同用户权限验证 | 管理员可管理节点，用户仅管理自己任务 | tests/integration/e2e/ |
| 传输加密 | TLS/SSL 加密通信 | 通信内容加密，明文传输失败 | tests/tls/ |
| 双向认证 | mTLS 双向证书认证 | 双向证书验证有效 | tests/tls/ |
| DSN 密码保护 | 密码加密存储与传输 | 密码不以明文形式存储或传输 | tests/integration/e2e/ |
| 共享存储安全 | 任务数据隔离存储 | 各任务数据相互隔离，无越权访问 | tests/integration/e2e/ |
| 数据完整性 | 传输数据完整性校验 | 数据完整，无篡改或丢失 | tests/integration/e2e/ |

### 7.4 **稳定性测试**

| 测试项 | 测试内容 | 预期结果 | 对应测试例 |
| --- | --- | --- | --- |
| 长时间运行 | 连续运行 72 小时以上 | 系统稳定，无崩溃或内存泄漏 | tests/integration/e2e/ |
| 高并发场景 | 同时处理大量任务和连接 | 系统响应正常，无死锁 | tests/performance/ |
| XNode 故障恢复 | 模拟 XNode 宕机与恢复 | 任务自动迁移，恢复后重新加入 | tests/integration/e2e/ |
| MNode 切换 | MNode Leader 选举切换 | xnoded 服务快速恢复 | tests/integration/e2e/ |
| 网络分区 | 模拟网络分区场景 | 系统降级或恢复后一致性 | tests/integration/e2e/ |
| 资源泄漏 | 长时间运行资源监控 | 无内存、文件描述符泄漏 | tests/performance/ |
| 共享存储故障 | 模拟共享存储不可用 | 系统降级或优雅处理 | tests/integration/e2e/ |

### 7.5 **兼容性测试**

| 测试项 | 测试内容 | 预期结果 | 对应测试例 |
| --- | --- | --- | --- |
| TDengine 版本兼容 | 2.0.20+、2.2、2.4、2.6、3.0+ | 各版本数据源和目标端兼容 | tests/td2td.rs, tests/tmq2td.rs |
| 操作系统兼容 | Linux/Windows/macOS/国产 OS | 各系统正常运行 | 跨平台测试用例 |
| CPU 架构兼容 | x86_64、arm64、龙芯 | 各架构编译运行正常 | 交叉编译测试 |
| InfluxDB 兼容 | 1.7、1.8、2.x 版本 | 各版本数据导入正常 | tests/integration/datasources/ |
| Kafka 兼容 | 2.8、3.0 版本 | 各版本消息消费正常 | tests/kafka/ |
| 文件格式兼容 | CSV、Parquet、JSON | 各格式导入导出正常 | tests/integration/e2e/ |
| 编码兼容 | UTF-8、GBK 等字符编码 | 中文字符正确处理 | tests/integration/e2e/ |

## 8. **测试计划**

| 阶段 | 任务 | 工作量（人天） | 备注 |
| --- | --- | --- | --- |
| 测试准备 | 测试环境搭建与配置 | 2.0 | 包括 TDengine 集群、共享存储、各数据源环境 |
| 功能测试 | 核心功能验证 | 5.0 | 覆盖所有功能需求 |
| 性能测试 | 性能指标验证与调优 | 3.0 | 包括基准测试和压力测试 |
| 安全测试 | 安全机制验证 | 2.0 | 认证、加密、权限等测试 |
| 稳定性测试 | 长时间运行与故障恢复 | 4.0 | 72 小时稳定性测试 |
| 兼容性测试 | 跨版本跨平台测试 | 3.0 | 多版本多环境测试 |
| 测试总结 | 测试报告编写与问题跟踪 | 2.0 | 整理测试结果和问题单 |
| **总计** |  | **21.0** |  |

**测试周期**：预计 3 周完成全部测试

## 9. **风险评估**

| 风险项 | 风险描述 | 可能性 | 影响程度 | 应对措施 |
| --- | --- | --- | --- | --- |
| 测试环境复杂度 | 需要多节点集群和多种数据源环境 | 高 | 中 | 提前准备环境，使用 Docker 容器化部署 |
| 性能测试不确定性 | 性能受硬件、网络等外部因素影响 | 中 | 中 | 多次测试取平均值，记录测试环境详情 |
| 兼容性问题 | 某些特定版本或组合可能存在兼容问题 | 中 | 高 | 建立兼容性矩阵，重点测试常用组合 |
| 资源不足 | 高并发或大数据量测试需要大量资源 | 低 | 高 | 提前申请测试资源，使用云资源弹性扩展 |
| 安全测试局限性 | 部分安全测试需要专业工具和知识 | 中 | 中 | 结合自动化测试和人工安全审计 |

## 10. **参考文档**

1. 数据管道工具-Requirement Spec
2. 数据管道工具-Function Spec
3. 数据管道工具-Design Spec
4. TDengine 官方文档
5. 数据源官方文档（InfluxDB、Kafka、MQTT 等）
