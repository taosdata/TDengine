# TDengine 未声明功能及接口检查报告

- 生成时间：`2026-04-22T18:04:49+08:00`
- 检查脚本：`tools/scripts/check_undeclared_features_and_interfaces.py`
- 检查方法：基于仓库内现行规格文档、全链路说明、示例配置和源码入口做清单式一致性复核，不做运行态端口扫描，也不覆盖仓库外部署制品。
- 总体结论：**未发现未声明功能及接口**

## 一、检查摘要

- 检查项总数：6
- 通过项：6
- 待复核项：0

## 一、未声明功能复核

### 一.1 taosX 导出 TDengine 查询结果到 CSV 文件已完成正式声明

- 结果：**通过**
- 检查说明：核对数据管道工具规格、CLI 说明和源码仓库说明，确认历史关注的 CSV 导出能力已被纳入正式功能清单。
- 本项结论：CSV 导出能力已在需求、功能规格和源码说明中形成闭环，当前不属于未声明功能。
- 证据：
- [声明] `docs/overview/03-各模块设计/工具组件/数据管道工具/数据管道工具-Requirement Spec.md:L83` — 需求规格声明 CSV 导出能力；命中内容：`| 6 | 数据导出 | 导出 TDengine 查询结果到 CSV 文件 |  |`
- [声明] `docs/overview/03-各模块设计/工具组件/数据管道工具/数据管道工具-Function Spec.md:L1144` — 功能规格给出 CSV 导出章节；命中内容：`##### 导出查询结果为 CSV 文件`
- [声明] `docs/overview/03-各模块设计/工具组件/数据管道工具/数据管道工具-Function Spec.md:L1149` — 功能规格给出 taosx run 导出 CSV 的命令示例；命中内容：`-t "csv:./test.csv"`
- [实现] `source/taos-xservice/README.md:L29` — 源码仓库 README 声明支持 CSV/Parquet 离线导入导出；命中内容：`- Export or import offline data files, currently support CSV and Parquet.`
- [实现] `source/taos-xservice/src/run.rs:L37` — CLI 源码说明支持 csv DSN；命中内容：`/// ─ CSV: `csv:/path/to/file.csv`.`
### 一.2 taosX TMQ 订阅导出 Kafka 已完成正式声明

- 结果：**通过**
- 检查说明：核对数据管道工具规格与 Kafka sink 组件，确认历史关注的 TMQ 导出 Kafka 能力已转为正式声明能力。
- 本项结论：TMQ 导出 Kafka 能力已在现行规格和源码组件中可追溯，当前不属于未声明功能。
- 证据：
- [声明] `docs/overview/03-各模块设计/工具组件/数据管道工具/数据管道工具-Function Spec.md:L451` — 功能规格给出 tmq_export 任务示例；命中内容：`CREATE XNODE TASK 'tmq_export'`
- [声明] `docs/overview/03-各模块设计/工具组件/数据管道工具/数据管道工具-Function Spec.md:L453` — 功能规格给出 Kafka 目标 DSN；命中内容：`TO 'kafka://broker:9092';`
- [声明] `docs/overview/03-各模块设计/工具组件/数据管道工具/数据管道工具-Function Spec.md:L27` — 功能规格将 Kafka 列为正式数据源/目标能力；命中内容：`- 消息队列：Kafka。`
- [实现] `source/taos-xservice/README.md:L36` — 源码仓库 README 将 Kafka 列为正式支持对象；命中内容：`- Message queue: Kafka.`
- [实现] `source/taos-xservice/crates/sink-kafka/Cargo.toml:L2` — 源码中存在 Kafka sink 组件；命中内容：`name = "sink-kafka"`

## 二、远程调试及连接接口复核

### 二.1 taosd 原生 6030 连接接口存在正式说明

- 结果：**通过**
- 检查说明：核对全链路认证文档与 Explorer 默认配置，确认 taosd 原生连接为正式接口而非隐藏入口。
- 本项结论：taosd 原生 6030 接口在现行文档和默认配置中均有说明，不属于未声明接口。
- 证据：
- [声明] `docs/full-trace/01-全链路认证.md:L9` — 全链路认证文档说明 taosd 原生 6030 接口；命中内容：`TDengine TSDB 的认证体系按 **6 层分层架构** 逐层展开——从用户侧的入口（应用、Web UI、CLI），经接入层的两条路径（WebSocket/REST 与 taosc 私有协议），延伸到数据采集链路、集群内部、存储层，最终到达运维侧的审计与监控接入。本文档按此分层逐一介绍认证机制：`
- [实现] `source/taos-xservice/explorer/server/examples/explorer.toml:L39` — Explorer 示例配置给出 taosd 原生连接 DSN；命中内容：`# cluster_native = "taos://localhost:6030"`
### 二.2 taosAdapter 的对外接口与调试入口均有正式说明

- 结果：**通过**
- 检查说明：核对 taosAdapter 规格、Swagger、示例配置和调试代码，确认 REST/WebSocket/兼容写入/StatsD/pprof 均非隐藏入口。
- 本项结论：taosAdapter 的 REST、WebSocket、兼容写入、StatsD 和 pprof 调试入口均可在规格或源码中追溯，未见隐藏管理接口。
- 证据：
- [声明] `docs/overview/03-各模块设计/工具组件/数据接入适配工具/数据接入适配工具-Function Spec.md:L31` — 功能规格说明 REST SQL 接口；命中内容：`可以使用任何支持 http 协议的客户端通过访问 RESTful 接口地址 `http://<fqdn>:6041/rest/sql` 来写入数据到 TDengine 或从 TDengine 中查询数据`
- [声明] `docs/overview/03-各模块设计/工具组件/数据接入适配工具/数据接入适配工具-Function Spec.md:L306` — 功能规格说明 WebSocket 接口；命中内容：`### 4.2 WebSocket 接口`
- [声明] `docs/overview/03-各模块设计/工具组件/数据接入适配工具/数据接入适配工具-Function Spec.md:L2400` — 功能规格说明 --debug 仅显式开启 pprof；命中内容：`- `--debug`：是否启用调试模式（开启 pprof/pprof）`
- [声明] `docs/full-trace/01-全链路认证.md:L271` — 全链路认证文档说明 taosAdapter 插件兼容写入覆盖 StatsD；命中内容：`外部系统（collectd、StatsD、OpenTSDB）通过 taosAdapter 的协议兼容端点写入时，taosAdapter **不透传来源凭据**，而是用配置文件中声明的独立账户代写。这些账户属于应用入口层的一部分，建议为每个插件创建专用只写账户：`
- [实现] `source/taos-adapter/docs/swagger.yaml:L202` — Swagger 定义 REST SQL 路由；命中内容：`/rest/sql:`
- [实现] `source/taos-adapter/docs/swagger.yaml:L70` — Swagger 定义 InfluxDB 兼容接口；命中内容：`/influxdb/v1/write:`
- [实现] `source/taos-adapter/docs/swagger.yaml:L119` — Swagger 定义 OpenTSDB 兼容接口；命中内容：`/opentsdb/v1/put/json/:db:`
- [实现] `source/taos-adapter/example/config/taosadapter.toml:L183` — 示例配置给出 StatsD 默认端口；命中内容：`port = 6044`
- [实现] `source/taos-adapter/system/main.go:L82` — 源码中 pprof 仅在 debug 开关下注册；命中内容：`pprof.Register(router)`
### 二.3 taosX/XNode 的 REST、gRPC 与内部认证链路均有正式说明

- 结果：**通过**
- 检查说明：核对全链路文档、XNode 规格、示例配置和服务源码，确认 6050/6055、JWT、TLS、Arrow Flight 均为正式受控接口。
- 本项结论：taosX/XNode 的 REST、gRPC/Arrow Flight 和 JWT/TLS 受控通信在现行文档与源码中均可追溯，未见隐藏通信入口。
- 证据：
- [声明] `docs/overview/03-各模块设计/工具组件/数据管道工具/数据管道工具-Requirement Spec.md:L126` — 需求规格说明 XNode/MNode JWT 认证；命中内容：`- XNode 与 MNode 之间使用 JWT Token 进行双向认证`
- [声明] `docs/overview/03-各模块设计/工具组件/数据管道工具/数据管道工具-Requirement Spec.md:L139` — 需求规格说明 gRPC + Arrow Flight 通信；命中内容：`- XNode 与 MNode 之间使用 gRPC + Arrow Flight 通信，必须启用 TLS 加密`
- [声明] `docs/full-trace/01-全链路认证.md:L750` — 全链路认证文档列出 taosX REST 接口；命中内容：`| REST API | HTTP 1.1 | 6050 | 是（HTTPS） | 不加密 | 供 taosExplorer / 管理工具调用 |`
- [声明] `docs/full-trace/01-全链路认证.md:L751` — 全链路认证文档列出 taosX gRPC 接口；命中内容：`| gRPC | HTTP/2 | 6055 | 是（gRPC TLS）| 不加密 | 供 taosxAgent 连接 |`
- [实现] `source/taos-xservice/examples/taosx.toml:L22` — 示例配置给出 taosX REST 默认端口；命中内容：`#listen = "0.0.0.0:6050"`
- [实现] `source/taos-xservice/examples/taosx.toml:L47` — 示例配置给出 taosX gRPC 默认端口；命中内容：`#grpc = "0.0.0.0:6055"`
- [实现] `source/taos-xservice/src/serve/mod.rs:L65` — 服务源码定义 REST 默认端口；命中内容：`const TAOSX_REST_API_DEFAULT_PORT: u16 = 6050;`
- [实现] `source/taos-xservice/src/serve/mod.rs:L66` — 服务源码定义 gRPC 默认端口；命中内容：`const TAOSX_GRPC_DEFAULT_PORT: u16 = 6055;`
- [实现] `source/taos-xservice/src/serve/rpc/mod.rs:L414` — gRPC 源码要求 x-token 进行鉴权；命中内容：`.get("x-token")`
### 二.4 Explorer Web UI/API 与认证机制均有正式说明

- 结果：**通过**
- 检查说明：核对 Explorer 规格、示例配置和服务源码，确认 6060 端口、Basic/OAuth、WebSocket 和 taosX 代理能力均有正式声明。
- 本项结论：Explorer 的 6060 Web 服务、Basic/OAuth、WebSocket 和 taosX 代理能力在文档与源码中均有支撑，未见隐藏管理入口。
- 证据：
- [声明] `docs/overview/03-各模块设计/工具组件/可视化管理工具/可视化管理工具-Requirement Spec.md:L114` — 需求规格要求提供 REST/WebSocket API 文档；命中内容：`| 46 | 编程接口 | API 文档 | 提供 REST API、WebSocket API 完整文档 |`
- [声明] `docs/overview/03-各模块设计/工具组件/可视化管理工具/可视化管理工具-Design Spec.md:L94` — 设计规格说明 OAuth 2.0/OIDC；命中内容：`| **openidconnect** | 3.5.0 | OAuth 2.0/OIDC 标准客户端 |`
- [声明] `docs/overview/03-各模块设计/工具组件/可视化管理工具/可视化管理工具-Design Spec.md:L96` — 设计规格说明 AES-256-GCM；命中内容：`| **aes-gcm** | 0.10 | AES-256-GCM 加密 (会话密码) |`
- [声明] `docs/overview/03-各模块设计/工具组件/可视化管理工具/可视化管理工具-Design Spec.md:L214` — 设计规格说明 Explorer 默认监听 6060；命中内容：`.bind(("0.0.0.0", 6060))?`
- [实现] `source/taos-xservice/explorer/server/examples/explorer.toml:L8` — Explorer 示例配置给出默认监听端口；命中内容：`port = 6060`
- [实现] `source/taos-xservice/explorer/server/examples/explorer.toml:L32` — Explorer 示例配置给出 taosAdapter 默认代理目标；命中内容：`cluster = "http://localhost:6041"`
- [实现] `source/taos-xservice/explorer/server/examples/explorer.toml:L148` — Explorer 示例配置给出 OAuth 回调地址；命中内容：`#redirect_uri = "http://localhost:6060/api/-/oauth/callback"`
- [实现] `source/taos-xservice/explorer/server/src/main.rs:L318` — Explorer 源码定义默认监听端口；命中内容：`const EXPLORER_PORT: u16 = 6060;`
- [实现] `source/taos-xservice/explorer/server/src/main.rs:L519` — Explorer 源码注册 OAuth 路由；命中内容：`"/api/-/oauth/authorize",`

## 三、综合结论

经对现行规格文档、全链路说明、示例配置和源码入口进行交叉复核，脚本覆盖范围内未发现新增未声明功能、未声明接口、隐藏管理入口或未说明的远程调试接口。
此前重点关注的 taosX CSV 导出与 TMQ 导出 Kafka 能力，当前均已纳入正式需求/功能规格与源码组件说明，可按“历史未声明功能已补充声明并纳入正式文档管理”口径出具检查结论。

## 四、执行方式

```bash
cd ~/tsdb
python3 tools/scripts/check_undeclared_features_and_interfaces.py
```
