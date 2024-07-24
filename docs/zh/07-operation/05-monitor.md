---
sidebar_label: 可视化管理
title: 可视化管理工具
toc_max_heading_level: 4
---

## 集群运行监控

为了确保集群稳定运行，TDengine 集成了多种监控指标收集机制，并通 过taosKeeper 进行汇总。taosKeeper负责接收这些数据，并将其写入一个独立的 TDengine 实例中，该实例可以与被监控的 TDengine 集群保持独立。

用户可以使用第三方的监测工具比如 Zabbix 来获取这些保存的系统监测数据，进而将 TDengine 的运行状况无缝集成到现有的 IT 监控系统中。此外，TDengine 还提供了 TDinsight 插件，用户可以通过 Grafana 平台直观地展示和管理这些监控信息，如下图所示。这为用户提供了灵活的监控选项，以满足不同场景下的运维需求。

~[通过监控组件管理监控信息](./grafana.png)

### taosKeeper 的安装与配置

taosKeeper 的配置文件默认位于 `/etc/taos/taoskeeper.toml`。 下面为一个示例配置文件，更多详细信息见参考手册。

```toml
# gin 框架是否启用 debug
debug = false

# 服务监听端口, 默认为 6043
port = 6043

# 日志级别，包含 panic、error、info、debug、trace等
loglevel = "info"

# 程序中使用协程池的大小
gopoolsize = 50000

# 查询 TDengine 监控数据轮询间隔
RotationInterval = "15s"

[tdengine]
host = "127.0.0.1"
port = 6041
username = "root"
password = "taosdata"

# 需要被监控的 taosAdapter
[taosAdapter]
address = ["127.0.0.1:6041"]

[metrics]
# 监控指标前缀
prefix = "taos"

# 集群数据的标识符
cluster = "production"

# 存放监控数据的数据库
database = "log"

# 指定需要监控的普通表
tables = []

# database options for db storing metrics data
[metrics.databaseoptions]
cachemodel = "none"
```

### 基于 TDinsight 的监控

为了简化用户在 TDengine 监控方面的配置工作，TDengine 提供了一个名为 TDinsight 的 Grafana 插件。该插件与 taosKeeper 协同工作，能够实时监控 TDengine 的各项性能指标。

通过集成 Grafana 和 TDengine 数据源插件，TDinsight 能够读取 taosKeeper 收集并存储的监控数据。这使得用户可以在 Grafana 平台上直观地查看 TDengine 集群的状态、节点信息、读写请求以及资源使用情况等关键指标，实现数据的可视化展示。

此外，TDinsight 还具备针对 vnode、dnode 和 mnode 节点的异常状态告警功能，为开发者提供实时的集群运行状态监控，确保 TDengine 集群的稳定性和可靠性。以下是TDinsight 的详细使用说明，以帮助你充分利用这一强大工具。

#### 前置条件

若要顺利使用 TDinsight，应满足如下条件。
- TDengine 已安装并正常运行。
- taosAdapter 已经安装并正常运行。
- taosKeeper 已经安装并正常运行。
- Grafana 已安装并正常运行，以下介绍以 Grafna 10.4.0 为例。

同时记录以下信息。
- taosAdapter 的 RESTful 接口地址，如 http://www.example.com:6041。
- TDengine 集群的认证信息，包括用户名及密码。

#### 导入仪表盘

TDengine 数据源插件已被提交至 Grafana 官网，完成插件的安装和数据源的创建后，可以进行 TDinsight 仪表盘的导入。

在 Grafana 的 Home-Dashboards 页面，点击位于右上角的 New → mport 按钮，即可进入 Dashboard 的导入页面，它支持以下两种导入方式。
- Dashboard ID：18180。
- Dashboard URL：https://grafana.com/grafana/dashboards/18180-tdinsight-for-3-x/

填写以上 Dashboard ID 或 Dashboard URL 以后，点击 Load 按钮，按照向导操作，即可完成导入。导入成功后，Dashboards 列表页面会出现 TDinsight for 3.x 仪盘，点击进入后，就可以看到 TDinsight 中已创建的各个指标的面板，如下图所示：

![TDinsight 界面示例](./tdinsight.png)

**注意** 在 TDinsight 界面左上角的 Log from 下拉列表中可以选择 log 数据库。

## 可视化管理

为方便用户更高效地使用和管理 TDengine，TDengine 3.0 版本推出了一个全新的可视化组件—taosExplorer。这个组件旨在帮助用户在不熟悉 SQL 的情况下，也能轻松管理 TDengine 集群。通过 taosExplorer，用户可以轻松查看 TDengine 的运行状态、浏览数据、配置数据源、实现流计算和数据订阅等功能。此外，用户还可以利用taosExplorer 进行数据的备份、复制和同步操作，以及配置用户的各种访问权限。这些功能极大地简化了数据库的使用过程，提高了用户体验。

本节介绍可视化管理的基本功能。

### 登录

在完成 TDengine 的安装与启动流程之后，用户便可立即开始使用 taosExplorer。该组件默认监听 TCP 端口 6060，用户只须在浏览器中输入 `http://<IP>:6060/login`（其中的IP 是用户自己的地址），便可顺利登录。成功登录集群后，用户会发现在左侧的导航栏中各项功能被清晰地划分为不同的模块。接下来将简单介绍主要模块。

### 运行监控面板

在 Grafana 上安装 TDengine 数据源插件后，即可添加 TDengine 数据源，并导入TDengine 的 Grafana Dashboard: TDengine for 3.x。通过这一操作，用户将能够在不编写任何代码的情况下实现对 TDengine 运行状态的实时监控和告警功能。

### 数据写入

通过创建不同的任务，用户能够以零代码的方式，将来自不同外部数据源的数据导入 TDengine。目前，TDengine 支持的数据源包括 AVEVA PI System、OPC-UA/DA、MQTT、Kafka、InfluxDB、OpenTSDB、TDengine 2、TDengine 3、CSV、AVEVA Historian 等。在任务的配置中，用户还可以添加与 ETL 相关的配置。

在任务列表页中，可以实现任务的启动、停止、编辑、删除、查看任务的活动日志等操作。

### 数据浏览器

通过“数据浏览器”页面，用户无须编写任何代码，便可轻松进行各种数据查询和分析操作。具体而言，可以通过此页面创建或删除数据库、创建或删除超级表及其子表，执行 SQL 并查看执行结果。此外，查询结果将以可视化形式展现，方便用户理解和分析。同时，还可以收藏常用的 SQL，以便日后快速调用。超级管理员还将拥有对数据库的管理权限，以实现更高级别的数据管理和操作。

### 编程

通过“编程”页面，可以看到不同编程语言如何与 TDengine 进行交互，实现写入和查询等基本操作。用户通过复制粘贴，即可完成一个示例工程的创建。目前支持的编程语言包括 Java、Go、Python、Node.js（Javascript）、C#、Rust、R 等。

### 流计算

通过“流计算”页面，用户可以轻松地创建一个流，从而使用 TDengine 提供的强大的流计算能力。更多关于流计算功能的介绍，详见参考手册。

### 数据订阅

通过“数据订阅”页面，用户可以进行创建、管理和共享主题，查看主题的消费者等操作。除此以外，TDengine 还提供了通过 Go、Rust、Python、Java 等编程语言使用数据订阅相关 API 的示例。

### 工具

通过 “工具” 页面，用户可以了解如下 TDengine 周边工具的使用方法。
- TDengine CLI。
- taosBenchmark。
- taosDump。
- TDengine 与 BI 工具的集成，例如 Google Data Studio、Power BI、永洪 BI 等。
- TDengine 与 Grafana、Seeq 的集成。

### 数据管理

“数据管理”页面为用户提供了丰富的管理功能，包括用户管理、备份、数据同步、集群管理、许可证管理以及审计等。

