# TDengine TSDB v3.4.1.0 系统测试计划

### 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-12-16 | 2025-12-16 | 1.0 | 肖波 | 初始版本 |
| 2026-02-05 | 2026-02-05 | 1.1 | 肖波 | 修订一些格式错误 |
| 2026-03-11 | 2026-03-11 | 1.2 | 肖波 | 修订文字 |

### 2. 测试目标

本次系统测试旨在全面验证 TDengine TSDB v3.4.1.0 版本在功能、性能、可靠性及安全性方面是否达到发布标准。具体目标如下：
1. **功能符合性：** 验证存储、查询、集群管理及订阅模块的功能是否完全符合需求规格说明书。
2. **性能达标：** 验证系统在写入吞吐量（2000 万点/秒）、查询延迟及集群水平扩展性（线性扩展≥ 90%）方面是否满足既定指标。
3. **高可用与稳定性：** 验证在节点故障（DNode/MNode/QNode）、网络分区及 7x24 小时高负载下的系统恢复能力与数据一致性。
4. **安全合规：** 确保身份鉴别、访问控制、数据加密及审计功能符合等保三级要求。
5. **环境兼容性：** 验证在国产化硬件（如鲲鹏 ARM64）及不同操作系统（Ubuntu, CentOS, 麒麟）上的运行兼容性。

### 3. 参考文档

本次系统测试将依据以下项目流程文档、核心模块规格说明书、相关工具设计文档以及引用的行业标准执行。

#### 3.1 项目流程与总体方案

- 《[项目测试过程](https://taosdata.feishu.cn/wiki/OMowwCUdSiiBvvkl3IecbyuQnuh)》
- 《[TDengine 总体测试方案](https://taosdata.feishu.cn/wiki/R7azwDCFzi5vWdkVDUucnacSn4c)》

#### 3.2 核心内核模块规格说明书

##### 3.2.1 时序数据存储模块

- 《时序数据存储模块 Requirement Spec》
- 《时序数据存储模块 Function Spec》
- 《时序数据存储模块 Design Spec》

##### 3.2.2 时序数据查询模块

- 《时序数据查询模块 Requirement Spec》
- 《时序数据查询模块 Function Spec》
- 《时序数据查询模块 Design Spec》

##### 3.2.3 集群模块

- 《集群模块 Requirement Spec》
- 《集群模块 Function Spec》
- 《集群模块 Design Spec》

##### 3.2.4 多副本模块

- 《多副本模块 Requirement Spec》
- 《多副本模块 Function Spec》
- 《多副本模块 Design Spec》

##### 3.2.5 时序数据订阅模块

- 《时序数据订阅模块 Requirement Spec》
- 《时序数据订阅模块 Function Spec》
- 《时序数据订阅模块 Design Spec》

##### 3.2.6 缓存模块 (Cache)

- 《缓存模块 Requirement Spec》
- 《缓存模块 Function Spec》
- 《缓存模块 Design Spec》

##### 3.2.7 时序数据流计算模块

- 《时序数据流计算模块 Requirement Spec》
- 《时序数据流计算模块 Function Spec》
- 《时序数据流计算模块 Design Spec》

##### 3.2.8 虚拟表模块 

- 《虚拟表模块 Requirement Spec》
- 《虚拟表模块 Function Spec》
- 《虚拟表模块 Design Spec》

##### 3.2.9 预聚集模块 

- 《预聚集模块 Requirement Spec》
- 《预聚集模块 Function Spec》
- 《预聚集模块 Design Spec》

##### 3.2.10 标签索引模块

- 《标签索引模块 Requirement Spec》
- 《标签索引模块 Function Spec》
- 《标签索引模块 Design Spec》

##### 3.2.11 自定义函数模块 (UDF)

- 《自定义函数模块 Requirement Spec》
- 《自定义函数模块 Function Spec》
- 《自定义函数模块 Design Spec》

##### 3.2.12 授权模块 

- 《授权模块 Requirement Spec》
- 《授权模块 Function Spec》
- 《授权模块 Design Spec》

##### 3.2.13 通信模块 

- 《通信模块 Requirement Spec》
- 《通信模块 Function Spec》
- 《通信模块 Design Spec》

##### 3.2.14 基础库模块

- 《基础库模块 Requirement Spec》
- 《基础库模块 Function Spec》
- 《基础库模块 Design Spec》

##### 3.2.15 跨平台模块

- 《跨平台模块 Requirement Spec》
- 《跨平台模块 Function Spec》
- 《跨平台模块 Design Spec》

##### 3.2.16 身份鉴别模块

- 《身份鉴别模块 Requirement Spec》
- 《身份鉴别模块 Function Spec》
- 《身份鉴别模块 Design Spec》

##### 3.2.17 访问控制模块

- 《访问控制模块 Requirement Spec》
- 《访问控制模块 Function Spec》
- 《访问控制模块 Design Spec》

##### 3.2.18 传输安全模块

- 《传输安全模块 Requirement Spec》
- 《传输安全模块 Function Spec》
- 《传输安全模块 Design Spec》

##### 3.2.19 存储安全模块

- 《存储安全模块 Requirement Spec》
- 《存储安全模块 Function Spec》
- 《存储安全模块 Design Spec》

##### 3.2.20 安全函数模块

- 《安全函数模块 Requirement Spec》
- 《安全函数模块 Function Spec》
- 《安全函数模块 Design Spec》

##### 3.2.21 加密算法模块

- 《加密算法模块 Requirement Spec》
- 《加密算法模块 Function Spec》
- 《加密算法模块 Design Spec》

#### 3.3 相关工具组件规格说明书

##### 3.3.1 数据接入适配工具 (taosAdapter)

- 《数据接入适配工具 Requirement Spec》
- 《数据接入适配工具 Function Spec》
- 《数据接入适配工具 Design Spec》

##### 3.3.2 数据管道工具 (taosX)

- 《数据管道工具 Requirement Spec》
- 《数据管道工具 Function Spec》
- 《数据管道工具 Design Spec》

##### 3.3.3 可视化管理工具 (taosExplorer)

- 《可视化管理工具 Requirement Spec》
- 《可视化管理工具 Function Spec》
- 《可视化管理工具 Design Spec》

##### 3.3.4 命令行工具 (taosShell)

- 《命令行工具 Requirement Spec》
- 《命令行工具 Function Spec》
- 《命令行工具 Design Spec》

##### 3.3.5 数据备份工具 (taosdump)

- 《数据备份工具 Requirement Spec》
- 《数据备份工具 Function Spec》
- 《数据备份工具 Design Spec》

##### 3.3.6 基准测试工具 (taosBenchmark)

- 《基准测试工具 Requirement Spec》
- 《基准测试工具 Function Spec》
- 《基准测试工具 Design Spec》

##### 3.3.7 监控指标导出工具 (taosKeeper)

- 《监控指标导出工具 Requirement Spec》
- 《监控指标导出工具 Function Spec》
- 《监控指标导出工具 Design Spec》

##### 3.3.8 监控可视化工具 (TDinsight)

- 《监控可视化工具 Requirement Spec》
- 《监控可视化工具 Function Spec》
- 《监控可视化工具 Design Spec》

#### 3.4 语言连接器规格说明书

##### 3.4.1 C/C++ 连接器

- 《C/C++ 连接器 Requirement Spec》
- 《C/C++ 连接器 Function Spec》
- 《C/C++ 连接器 Design Spec》

##### 3.4.2 JDBC 连接器

- 《JDBC 连接器 Requirement Spec》
- 《JDBC 连接器 Function Spec》
- 《JDBC 连接器 Design Spec》

##### 3.4.3 ODBC 连接器

- 《ODBC 连接器 Requirement Spec》
- 《ODBC 连接器 Function Spec》
- 《ODBC 连接器 Design Spec》

##### 3.4.4 Python 连接器

- 《Python 连接器 Requirement Spec》
- 《Python 连接器 Function Spec》
- 《Python 连接器 Design Spec》

##### 3.4.5 NodeJS 连接器

- 《NodeJS 连接器 Requirement Spec》
- 《NodeJS 连接器 Function Spec》
- 《NodeJS 连接器 Design Spec》

##### 3.4.6 Go 连接器

- 《Go 连接器 Requirement Spec》
- 《Go 连接器 Function Spec》
- 《Go 连接器 Design Spec》

##### 3.4.7 Rust 连接器

- 《Rust 连接器 Requirement Spec》
- 《Rust 连接器 Function Spec》
- 《Rust 连接器 Design Spec》

##### 3.4.8 C# 连接器

- 《C# 连接器 Requirement Spec》
- 《C# 连接器 Function Spec》
- 《C# 连接器 Design Spec》

#### 3.5 行业标准与规范

- **安全标准：** GB/T 22239-2019 《信息安全技术 网络安全等级保护基本要求》（第三级）
- **数据库标准：** ANSI/ISO/IEC 9075-1992 (SQL-92 Standard)
- **一致性协议：** Raft Consensus Algorithm (Diego Ongaro and John Ousterhout)
- **时间戳标准：** ISO 8601 (Data elements and interchange formats – Information interchange – Representation of dates and times)

### 4. 测试范围

本次测试涵盖 TDengine TSDB 核心内核模块及相关工具组件，具体如下：

#### 4.1 纳入测试范围

本次测试将严格覆盖以下三大类组件及其子模块：
1. **内核模块**
- **核心功能：** 存储、查询、订阅、缓存、流计算、虚拟表、预聚集、标签索引、自定义函数 (UDF)、多副本、集群、授权、通信、基础库、跨平台。
- **安全功能：** 身份鉴别、访问控制、传输安全、存储安全、安全函数、安全审计、加密算法。
1. **相关工具**
- **数据接入与处理：** 数据接入适配工具 (taosAdapter)、数据管道工具 (taosX)。
- **管理与运维：** 可视化管理工具 (taosExplorer)、命令行工具 (taosShell)、数据备份工具 (taosdump)。
- **监控与测试：** 基准测试工具 (taosBenchmark)、监控指标导出工具 (taosKeeper)、监控可视化工具 (TDinsight)。
1. **语言连接器**
- **覆盖语言：** C/C++ 连接器、JDBC 连接器、ODBC 连接器、Python 连接器、NodeJS 连接器、Go 连接器、Rust 连接器、C# 连接器。

#### 4.2 不予测试范围

- 不在软硬件环境列表内的操作系统版本及硬件架构。
- 第三方应用集成的深度业务逻辑（仅测试标准接口适配）。

### 5. 测试策略

根据《总体测试方案》，采用黑盒测试为主，灰盒测试为辅的策略，结合自动化与手工测试。
1. **功能测试:**
  - **方法：** 依据详细设计文档执行正向与逆向用例。
  - **重点：** 覆盖所有 SQL 语法、集群管理命令及异常输入处理。
  - **工具：** `taos CLI`，自动化测试脚本集。
1. **性能测试:**
  - **方法：** 使用基准工具模拟高并发写入与查询，对比基线指标。
  - **场景：** 单机写入吞吐、集群线性扩展性、复杂聚合查询延迟。
  - **工具：** `taosBenchmark taosgen`。
1. **可靠性与容错测试 :**
  - **方法：** 故障注入（Chaos Engineering）。
  - **场景：** 模拟 `kill -9` 进程、断电、拔网线，验证 Raft 选主与数据恢复。
  - **工具：** 自定义 Python 故障注入脚本。
1. **安全性测试:**
  - **方法：** 渗透测试与合规检查。
  - **重点：** 权限隔离、传输加密 (TLS)、静态数据加密、SQL 注入防御。
1. **兼容性测试:**
  - **方法：** 在 ARM64/x86_64 及不同 OS 上部署并运行核心用例。

### 6. 测试资源

#### 6.1 人员职责

| 角色 | 人员 | 职责 |
| --- | --- | --- |
| 测试负责人 | 肖波 | 计划制定、进度管理、报告编写 |
| 功能测试 | 贾靖斌 | 执行内核、工具及连接器的功能用例 |
| 性能测试 | 聂敏慧 | 执行性能基准测试及扩展性测试 |
| 安全测试 | 陈浩然 | 执行安全渗透、权限及加密测试 |

#### 6.2 软硬件环境

- **硬件配置：**
  - **主测试节点 (x86)：** 3台 (16 Core, 64GB RAM) - 用于核心集群测试
  - **扩展节点：** 2台 - 用于水平扩展性验证
  - **独立 QNode：** 1台 - 用于计算存储分离测试
  - **国产化节点：** 1台 (ARM64/鲲鹏) - 用于兼容性测试
- **软件配置：**
  - **OS：** Ubuntu 20.04 LTS (主), CentOS 7.9 (兼容), 麒麟 V10 (国产)
  - **被测软件：** TDengine Server 3.4.0

#### 6.3 测试工具 

本次系统测试将采用一系列内部自研工具和外部通用工具，以确保测试的深度、效率和全面性。

##### 6.3.1 内部自研测试工具

| **工具名称** | **工具类型** | **主要用途** |
| --- | --- | --- |
| **taosBenchmark / taosgen** | 性能测试 | 核心性能基准测试、高并发写入、查询吞吐量及线性扩展性测试。 |
| **taos CLI** | 命令行客户端 | 功能测试、SQL 语法验证、环境冒烟测试及手工操作验证。 |
| **taosX / taosAdapter** | 数据接入组件 | 验证数据管道的接入功能、性能和数据流的稳定性。 |
| **taosExplorer** | 可视化管理 | 验证可视化管理界面的功能正确性、数据展示及易用性。 |
| **taosdump** | 备份/恢复工具 | 验证数据备份与恢复机制的完整性和可靠性。 |
| **taosKeeper** | 监控指标导出 | 验证系统运行指标的导出功能及数据准确性。 |

##### 6.3.2 外部通用及第三方工具

| **工具名称** | **工具类型** | **主要用途** |
| --- | --- | --- |
| **Prometheus + Grafana** | 性能监控 | 实时监控被测系统的 CPU、内存、磁盘 I/O 等资源使用情况，以及关键性能指标（延迟、吞吐）。 |
| **Jira / 飞书项目** | 缺陷管理 | 缺陷的记录、跟踪、分配、状态流转及统计分析。 |
| **Python / Shell Scripts** | 自动化框架 | 驱动自动化测试脚本执行、批量数据生成、环境初始化及故障注入（Chaos Engineering）。 |
| **Wireshark / TCPDump** | 网络协议分析 | 验证内核及连接器的通信协议、数据传输安全（TLS）及网络异常恢复场景。 |

### 7. 进度计划

依据各模块 Test Spec 的时间安排，整体测试周期为 **2026年1月04日 至 2026年3月17日**。

| **阶段** | **时间段** | **主要任务** |
| --- | --- | --- |
| **阶段一** | 2026/4/4 - 4/7 | **内核模块功能测试 (1周)** 覆盖存储、查询、流计算、订阅等核心功能及单机性能验证。 |
| **阶段二** | 2026/4/8 - 4/14 | **安全模块与工具组件测试 (1周)** 执行身份鉴别、加密、审计测试，以及 taosAdapter、taosExplorer 等工具验证。 |
| **阶段三** | 2026/4/15 - 4/21 | **集群高可用与连接器测试 (1周)** 执行节点扩缩容、故障切换、多语言连接器测试。 |
| **阶段四** | 2026/4/22 - 1/28 | **系统集成与验收 (1周)** 端到端业务流程、安全合规验收及报告评审。 |

### 8. 风险评估与应对

| **风险描述** | **影响等级** | **应对措施** |
| --- | --- | --- |
| **集群资源动态操作原子性问题** MNode 故障导致元数据不一致 | 高 | 强化 Raft 协议测试，重点验证故障回滚机制。 |
| **国产化环境兼容性风险** 在鲲鹏/麒麟环境下出现非预期行为 | 中 | 提前准备环境，在测试初期即启动 ARM 架构的冒烟测试。 |
| **性能测试环境差异** 测试环境与生产环境配置不同导致指标失真 | 高 | 严格记录硬件配置作为基准，性能对比必须在同配置下进行。 |
| **测试用例覆盖不足** 未能覆盖特定极端场景 | 中 | 在已有用例基础上，补充针对极端压缩率和冷热迁移的探索性测试。 |

### 9. 缺陷跟踪与管理

| **项目** | **说明** |
| --- | --- |
| **缺陷管理工具** | 飞书项目、Jira或其他项目指定的缺陷管理系统。 |
| **缺陷提交流程** | 发现缺陷 -> 记录缺陷（包括环境、步骤、预期/实际结果、日志）-> 提交给研发人员 -> 研发修复 -> 测试回归验证 -> 关闭。 |
| **缺陷优先级** | **高（紧急）：** 数据丢失、集群不可用、核心功能无法使用； **中（中等）：** 性能不达标、严重功能缺陷； **低（较低）：** 易用性问题、一般功能缺陷； |
| **测试退出标准** | 所有高级别缺陷必须关闭，中级别缺陷关闭率达到 90% 以上，低级别缺陷不影响发布。 |

### 10. 测试通过准则 

**准入标准：** 研发已转测，冒烟测试通过，测试环境搭建完毕。
**准出标准：**
1. **功能：** 所有 高级用例执行通过，功能覆盖率 100%。
2. **缺陷：** 无 高级遗留缺陷，中级缺陷修复率 >90%。
3. **性能：** 关键指标（写入、查询、扩展性）满足《总体测试方案》定义的阈值。
4. **安全：** 无中高危安全漏洞，通过等保三级合规验证。

### 11. 交付物

1. 系统测试计划（本文档）
2. 各模块详细测试报告（功能、性能、安全、可靠性）
3. 自动化测试脚本集
4. 缺陷跟踪清单与分析报告
