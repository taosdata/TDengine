# TDengine 文档

TDengine 是一个高效的存储、查询、分析时序大数据的平台，专为物联网、车联网、工业互联网、运维监测等优化而设计。您可以像使用关系型数据库 MySQL 一样来使用它，但建议您在使用前仔细阅读一遍下面的文档，特别是 [数据模型](/architecture) 与 [数据建模](/model)。除本文档之外，欢迎 [下载产品白皮书](https://www.taosdata.com/downloads/TDengine%20White%20Paper.pdf)。

## [TDengine 介绍](/evaluation)

- [TDengine 简介及特色](/evaluation#intro)
- [TDengine 适用场景](/evaluation#scenes)
- [TDengine 性能指标介绍和验证方法](/evaluation#)

## [立即开始](/getting-started)

- [快捷安装](/getting-started#install)：可通过源码、安装包或 Docker 安装，三秒钟搞定
- [轻松启动](/getting-started#start)：使用 systemctl 启停 TDengine
- [命令行程序 TAOS](/getting-started#console)：访问 TDengine 的简便方式
- [极速体验](/getting-started#demo)：运行示例程序，快速体验高效的数据插入、查询
- [支持平台列表](/getting-started#platforms)：TDengine 服务器和客户端支持的平台列表
- [Kubernetes 部署](https://taosdata.github.io/TDengine-Operator/zh/index.html)：TDengine 在 Kubernetes 环境进行部署的详细说明

## [整体架构](/architecture)

- [数据模型](/architecture#model)：关系型数据库模型，但要求每个采集点单独建表
- [集群与基本逻辑单元](/architecture#cluster)：吸取 NoSQL 优点，支持水平扩展，支持高可靠
- [存储模型与数据分区、分片](/architecture#sharding)：标签数据与时序数据完全分离，按 VNode 和时间两个维度对数据切分
- [数据写入与复制流程](/architecture#replication)：先写入 WAL、之后写入缓存，再给应用确认，支持多副本
- [缓存与持久化](/architecture#persistence)：最新数据缓存在内存中，但落盘时采用列式存储、超高压缩比
- [数据查询](/architecture#query)：支持各种函数、时间轴聚合、插值、多表聚合

## [数据建模](/model)

- [创建库](/model#create-db)：为具有相似数据特征的数据采集点创建一个库
- [创建超级表](/model#create-stable)：为同一类型的数据采集点创建一个超级表
- [创建表](/model#create-table)：使用超级表做模板，为每一个具体的数据采集点单独建表

## [TAOS SQL](/taos-sql)

- [支持的数据类型](/taos-sql#data-type)：支持时间戳、整型、浮点型、布尔型、字符型等多种数据类型
- [数据库管理](/taos-sql#management)：添加、删除、查看数据库
- [表管理](/taos-sql#table)：添加、删除、查看、修改表
- [超级表管理](/taos-sql#super-table)：添加、删除、查看、修改超级表
- [标签管理](/taos-sql#tags)：增加、删除、修改标签
- [数据写入](/taos-sql#insert)：支持单表单条、多条、多表多条写入，支持历史数据写入
- [数据查询](/taos-sql#select)：支持时间段、值过滤、排序、嵌套查询、Union、Join、查询结果手动分页等
- [SQL 函数](/taos-sql#functions)：支持各种聚合函数、选择函数、计算函数，如 AVG, MIN, DIFF 等
- [窗口切分聚合](/taos-sql#aggregation)：将表中数据按照时间段等方式进行切割后聚合，降维处理
- [边界限制](/taos-sql#limitation)：库、表、SQL 等边界限制条件
- [UDF](/taos-sql/udf)：用户定义函数的创建和管理方法
- [错误码](/taos-sql/error-code)：TDengine 2.0 错误码以及对应的十进制码

## [高效写入数据](/insert)

- [SQL 写入](/insert#sql)：使用 SQL INSERT 命令向一张或多张表写入单条或多条记录
- [Schemaless 写入](/insert#schemaless)：免于预先建表，将数据直接写入时自动维护元数据结构
- [Prometheus 写入](/insert#prometheus)：配置 Prometheus, 不用任何代码，将数据直接写入
- [Telegraf 写入](/insert#telegraf)：配置 Telegraf, 不用任何代码，将采集数据直接写入
- [collectd 直接写入](/insert#collectd)：配置 collectd，不用任何代码，将采集数据直接写入
- [StatsD 直接写入](/insert#statsd)：配置 StatsD，不用任何代码，将采集数据直接写入
- [EMQX Broker](/insert#emq)：配置 EMQX，不用任何代码，就可将 MQTT 数据直接写入
- [HiveMQ Broker](/insert#hivemq)：配置 HiveMQ，不用任何代码，就可将 MQTT 数据直接写入

## [高效查询数据](/queries)

- [主要查询功能](/queries#queries)：支持各种标准函数，设置过滤条件，时间段查询
- [多表聚合查询](/queries#aggregation)：使用超级表，设置标签过滤条件，进行高效聚合查询
- [降采样查询值](/queries#sampling)：按时间段分段聚合，支持插值

## [高级功能](/advanced-features)

- [连续查询（Continuous Query）](/advanced-features#continuous-query)：基于滑动窗口，定时自动的对数据流进行查询计算
- [数据订阅（Publisher/Subscriber）](/advanced-features#subscribe)：类似典型的消息队列，应用可订阅接收到的最新数据
- [缓存（Cache）](/advanced-features#cache)：每个设备最新的数据都会缓存在内存中，可快速获取

## [连接器](/connector)

- [C/C++ Connector](/connector#c-cpp)：通过 libtaos 客户端的库，连接 TDengine 服务器的主要方法
- [Java Connector(JDBC)](/connector/java)：通过标准的 JDBC API，给 Java 应用提供到 TDengine 的连接
- [Python Connector](/connector#python)：给 Python 应用提供一个连接 TDengine 服务器的驱动
- [RESTful Connector](/connector#restful)：提供一最简单的连接 TDengine 服务器的方式
- [Go Connector](/connector#go)：给 Go 应用提供一个连接 TDengine 服务器的驱动
- [Node.js Connector](/connector#nodejs)：给 Node.js 应用提供一个连接 TDengine 服务器的驱动
- [C# Connector](/connector#csharp)：给 C# 应用提供一个连接 TDengine 服务器的驱动
- [Windows 客户端](https://www.taosdata.com/blog/2019/07/26/514.html)：自行编译 Windows 客户端，Windows 环境的各种连接器都需要它
- [Rust Connector](/connector/rust): Rust 语言下通过 libtaos 客户端或 RESTful 接口，连接 TDengine 服务器。
- [PHP Connector](/connector/php): 给 PHP 应用提供一个连接 TDengine 服务器的驱动，或 RESTful 接口，连接 TDengine 服务器。

## TDengine 组件与工具

- [taosAdapter](/tools/adapter): TDengine 集群和应用之间的 RESTful 接口适配服务。
- [TDinsight](/tools/insight): 监控 TDengine 集群的 Grafana 面板集合。
- [taosTools](/tools/taos-tools): taosTools 是用于 TDengine 的辅助工具软件集合。。
- [taosdump](/tools/taosdump): TDengine 数据备份工具。使用 taosdump 请安装 taosTools。
- [taosBenchmark](/tools/taosbenchmark): TDengine 压力测试工具。

## [与其他工具的连接](/connections)

- [Grafana](/connections#grafana)：获取并可视化保存在 TDengine 的数据
- [IDEA Database](https://www.taosdata.com/blog/2020/08/27/1767.html)：通过 IDEA 数据库管理工具可视化使用 TDengine
- [TDengineGUI](https://github.com/skye0207/TDengineGUI)：基于 Electron 开发的跨平台 TDengine 图形化管理工具
- [DataX](https://www.taosdata.com/blog/2021/10/26/3156.html)：支持 TDengine 和其他数据库之间进行数据迁移的工具

## [TDengine 集群的安装、管理](/cluster)

- [准备工作](/cluster#prepare)：部署环境前的几点注意事项
- [创建第一个节点](/cluster#node-one)：与快捷安装完全一样，非常简单
- [创建后续节点](/cluster#node-other)：配置新节点的 taos.cfg, 在现有集群添加新的节点
- [节点管理](/cluster#management)：增加、删除、查看集群的节点
- [VNode 的高可用性](/cluster#high-availability)：通过多副本的机制来提供 VNode 的高可用性
- [MNode 的管理](/cluster#mnode)：系统自动创建、无需任何人工干预
- [负载均衡](/cluster#load-balancing)：一旦节点个数或负载有变化，自动进行
- [节点离线处理](/cluster#offline)：节点离线超过一定时长，将从集群中剔除
- [Arbitrator](/cluster#arbitrator)：对于偶数个副本的情形，使用它可以防止脑裂（Split-brain）问题

## [TDengine 的运营和维护](/administrator)

- [容量规划](/administrator#planning)：根据场景，估算硬件资源
- [容错和灾备](/administrator#tolerance)：设置正确的 WAL 和数据副本数
- [系统配置](/administrator#config)：端口，缓存大小，文件块大小和其他系统配置
- [用户管理](/administrator#user)：添加、删除 TDengine 用户，修改用户密码
- [数据导入](/administrator#import)：可按脚本文件导入，也可按数据文件导入
- [数据导出](/administrator#export)：从 Shell 按表导出，也可用 taosdump 工具做各种导出
- [系统连接、任务查询管理](/administrator#status)：检查系统现有的连接、查询、流式计算，日志和事件等
- [系统监控](/administrator#monitoring)：系统监控，使用 TDinsight 进行集群监控等
- [性能优化](/administrator#optimize)：对长期运行的系统进行维护优化，保障性能表现
- [文件目录结构](/administrator#directories)：TDengine 数据文件、配置文件等所在目录
- [参数限制与保留关键字](/administrator#keywords)：TDengine 的参数限制与保留关键字列表

## TDengine 的技术设计

- [系统模块](/architecture/taosd)：taosd 的功能和模块划分
- [数据复制](/architecture/replica)：支持实时同步、异步复制，保证系统的高可用性
- [技术博客](https://www.taosdata.com/cn/blog/?categories=3)：更多的技术分析和架构设计文章

## 应用 TDengine 快速搭建 IT 运维系统

- [DevOps](/devops/telegraf)：使用 TDengine + Telegraf + Grafana 快速搭建 IT 运维系统
- [DevOps](/devops/collectd)：使用 TDengine + collectd/StatsD + Grafana 快速搭建 IT 运维系统
- [最佳实践](/devops/immigrate)：OpenTSDB 应用迁移到 TDengine 的最佳实践

## TDengine 与其他数据库的对比测试

- [用 InfluxDB 开源的性能测试工具对比 InfluxDB 和 TDengine](https://www.taosdata.com/blog/2020/01/13/1105.html)
- [TDengine 与 OpenTSDB 对比测试](https://www.taosdata.com/blog/2019/08/21/621.html)
- [TDengine 与 Cassandra 对比测试](https://www.taosdata.com/blog/2019/08/14/573.html)
- [TDengine 与 InfluxDB 对比测试](https://www.taosdata.com/blog/2019/07/19/419.html)
- [TDengine 与 InfluxDB、OpenTSDB、Cassandra、MySQL、ClickHouse 等数据库的对比测试报告](https://www.taosdata.com/downloads/TDengine_Testing_Report_cn.pdf)

## 物联网大数据

- [物联网、工业互联网大数据的特点](https://www.taosdata.com/blog/2019/07/09/105.html)
- [物联网大数据平台应具备的功能和特点](https://www.taosdata.com/blog/2019/07/29/542.html)
- [通用大数据架构为什么不适合处理物联网数据？](https://www.taosdata.com/blog/2019/07/09/107.html)
- [物联网、车联网、工业互联网大数据平台，为什么推荐使用 TDengine？](https://www.taosdata.com/blog/2019/07/09/109.html)

## 培训和 FAQ

- [FAQ：常见问题与答案](/faq)
- [技术公开课：开源、高效的物联网大数据平台，TDengine 内核技术剖析](https://www.taosdata.com/blog/2020/12/25/2126.html)
- [TDengine 视频教程 - 快速上手](https://www.taosdata.com/blog/2020/11/11/1941.html)
- [TDengine 视频教程 - 数据建模](https://www.taosdata.com/blog/2020/11/11/1945.html)
- [TDengine 视频教程 - 集群搭建](https://www.taosdata.com/blog/2020/11/11/1961.html)
- [TDengine 视频教程 - Go Connector](https://www.taosdata.com/blog/2020/11/11/1951.html)
- [TDengine 视频教程 - JDBC Connector](https://www.taosdata.com/blog/2020/11/11/1955.html)
- [TDengine 视频教程 - Node.js Connector](https://www.taosdata.com/blog/2020/11/11/1957.html)
- [TDengine 视频教程 - Python Connector](https://www.taosdata.com/blog/2020/11/11/1963.html)
- [TDengine 视频教程 - RESTful Connector](https://www.taosdata.com/blog/2020/11/11/1965.html)
- [TDengine 视频教程 - “零”代码运维监控](https://www.taosdata.com/blog/2020/11/11/1959.html)
- [应用案例：一些使用实例来解释如何使用 TDengine](https://www.taosdata.com/cn/blog/?categories=4)
