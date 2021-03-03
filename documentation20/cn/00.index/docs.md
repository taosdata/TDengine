# TDengine文档

TDengine是一个高效的存储、查询、分析时序大数据的平台，专为物联网、车联网、工业互联网、运维监测等优化而设计。您可以像使用关系型数据库MySQL一样来使用它，但建议您在使用前仔细阅读一遍下面的文档，特别是 [数据模型](/architecture) 与 [数据建模](/model)。除本文档之外，欢迎 [下载产品白皮书](https://www.taosdata.com/downloads/TDengine%20White%20Paper.pdf)。如需查阅TDengine 1.6 文档，请点击 [这里](https://www.taosdata.com/cn/documentation16/) 访问。

## [TDengine介绍](/evaluation)

* [TDengine 简介及特色](/evaluation#intro)
* [TDengine 适用场景](/evaluation#scenes)
* [TDengine 性能指标介绍和验证方法](/evaluation#)

## [立即开始](/getting-started)

* [快捷安装](/getting-started#install)：可通过源码、安装包或docker安装，三秒钟搞定
* [轻松启动](/getting-started#start)：使用systemctl 启停TDengine
* [命令行程序TAOS](/getting-started#console)：访问TDengine的简便方式
* [极速体验](/getting-started#demo)：运行示例程序，快速体验高效的数据插入、查询
* [支持平台列表](/getting-started#platforms)：TDengine服务器和客户端支持的平台列表

## [整体架构](/architecture)

* [数据模型](/architecture#model)：关系型数据库模型，但要求每个采集点单独建表
* [集群与基本逻辑单元](/architecture#cluster)：吸取NoSQL优点，支持水平扩展，支持高可靠
* [存储模型与数据分区、分片](/architecture#sharding)：标签数据与时序数据完全分离，按vnode和时间两个维度对数据切分
* [数据写入与复制流程](/architecture#replication)：先写入WAL、之后写入缓存，再给应用确认，支持多副本
* [缓存与持久化](/architecture#persistence)：最新数据缓存在内存中，但落盘时采用列式存储、超高压缩比
* [数据查询](/architecture#query)：支持各种函数、时间轴聚合、插值、多表聚合

## [数据建模](/model)

* [创建库](/model#create-db)：为具有相似数据特征的数据采集点创建一个库
* [创建超级表](/model#create-stable)：为同一类型的数据采集点创建一个超级表
* [创建表](/model#create-table)：使用超级表做模板，为每一个具体的数据采集点单独建表

## [高效写入数据](/insert)

* [SQL写入](/insert#sql)：使用SQL insert命令向一张或多张表写入单条或多条记录
* [Prometheus写入](/insert#prometheus)：配置Prometheus, 不用任何代码，将数据直接写入
* [Telegraf写入](/insert#telegraf)：配置Telegraf, 不用任何代码，将采集数据直接写入
* [EMQ X Broker](/insert#emq)：配置EMQ X，不用任何代码，就可将MQTT数据直接写入
* [HiveMQ Broker](/insert#hivemq)：配置HiveMQ，不用任何代码，就可将MQTT数据直接写入

## [高效查询数据](/queries)

* [主要查询功能](/queries#queries)：支持各种标准函数，设置过滤条件，时间段查询
* [多表聚合查询](/queries#aggregation)：使用超级表，设置标签过滤条件，进行高效聚合查询
* [降采样查询值](/queries#sampling)：按时间段分段聚合，支持插值

## [高级功能](/advanced-features)

* [连续查询(Continuous Query)](/advanced-features#continuous-query)：基于滑动窗口，定时自动的对数据流进行查询计算
* [数据订阅(Publisher/Subscriber)](/advanced-features#subscribe)：象典型的消息队列，应用可订阅接收到的最新数据
* [缓存(Cache)](/advanced-features#cache)：每个设备最新的数据都会缓存在内存中，可快速获取
* [报警监测](/advanced-features#alert)：根据配置规则，自动监测超限行为数据，并主动推送

## [连接器](/connector)

* [C/C++ Connector](/connector#c-cpp)：通过libtaos客户端的库，连接TDengine服务器的主要方法
* [Java Connector(JDBC)](/connector/java)：通过标准的JDBC API，给Java应用提供到TDengine的连接
* [Python Connector](/connector#python)：给Python应用提供一个连接TDengine服务器的驱动
* [RESTful Connector](/connector#restful)：提供一最简单的连接TDengine服务器的方式
* [Go Connector](/connector#go)：给Go应用提供一个连接TDengine服务器的驱动
* [Node.js Connector](/connector#nodejs)：给node应用提供一个连接TDengine服务器的驱动
* [C# Connector](/connector#csharp)：给C#应用提供一个连接TDengine服务器的驱动
* [Windows客户端](https://www.taosdata.com/blog/2019/07/26/514.html)：自行编译windows客户端，Windows环境的各种连接器都需要它

## [与其他工具的连接](/connections)

* [Grafana](/connections#grafana)：获取并可视化保存在TDengine的数据
* [Matlab](/connections#matlab)：通过配置Matlab的JDBC数据源访问保存在TDengine的数据
* [R](/connections#r)：通过配置R的JDBC数据源访问保存在TDengine的数据
* [IDEA Database](https://www.taosdata.com/blog/2020/08/27/1767.html)：通过IDEA 数据库管理工具可视化使用 TDengine

## [TDengine集群的安装、管理](/cluster)

* [准备工作](/cluster#prepare)：部署环境前的几点注意事项
* [创建第一个节点](/cluster#node-one)：与快捷安装完全一样，非常简单
* [创建后续节点](/cluster#node-other)：配置新节点的taos.cfg, 在现有集群添加新的节点
* [节点管理](/cluster#management)：增加、删除、查看集群的节点
* [Vnode 的高可用性](/cluster#high-availability)：通过多副本的机制来提供 Vnode 的高可用性
* [Mnode 的管理](/cluster#mnode)：系统自动创建、无需任何人工干预
* [负载均衡](/cluster#load-balancing)：一旦节点个数或负载有变化，自动进行
* [节点离线处理](/cluster#offline)：节点离线超过一定时长，将从集群中剔除
* [Arbitrator](/cluster#arbitrator)：对于偶数个副本的情形，使用它可以防止split brain

## [TDengine的运营和维护](/administrator)

* [容量规划](/administrator#planning)：根据场景，估算硬件资源
* [容错和灾备](/administrator#tolerance)：设置正确的WAL和数据副本数
* [系统配置](/administrator#config)：端口，缓存大小，文件块大小和其他系统配置
* [用户管理](/administrator#user)：添加、删除TDengine用户，修改用户密码
* [数据导入](/administrator#import)：可按脚本文件导入，也可按数据文件导入
* [数据导出](/administrator#export)：从shell按表导出，也可用taosdump工具做各种导出
* [系统监控](/administrator#status)：检查系统现有的连接、查询、流式计算，日志和事件等
* [文件目录结构](/administrator#directories)：TDengine数据文件、配置文件等所在目录
* [参数限制与保留关键字](/administrator#keywords)：TDengine的参数限制与保留关键字列表

## [TAOS SQL](/taos-sql)

* [支持的数据类型](/taos-sql#data-type)：支持时间戳、整型、浮点型、布尔型、字符型等多种数据类型
* [数据库管理](/taos-sql#management)：添加、删除、查看数据库
* [表管理](/taos-sql#table)：添加、删除、查看、修改表
* [超级表管理](/taos-sql#super-table)：添加、删除、查看、修改超级表
* [标签管理](/taos-sql#tags)：增加、删除、修改标签
* [数据写入](/taos-sql#insert)：支持单表单条、多条、多表多条写入，支持历史数据写入
* [数据查询](/taos-sql#select)：支持时间段、值过滤、排序、查询结果手动分页等
* [SQL函数](/taos-sql#functions)：支持各种聚合函数、选择函数、计算函数，如avg, min, diff等
* [时间维度聚合](/taos-sql#aggregation)：将表中数据按照时间段进行切割后聚合，降维处理
* [边界限制](/taos-sql#limitation)：库、表、SQL等边界限制条件
* [错误码](/taos-sql/error-code)：TDengine 2.0 错误码以及对应的十进制码

## TDengine的技术设计

* [系统模块](/architecture/taosd)：taosd的功能和模块划分
* [数据复制](/architecture/replica)：支持实时同步、异步复制，保证系统的High Availibility
* [技术博客](https://www.taosdata.com/cn/blog/?categories=3)：更多的技术分析和架构设计文章

## 常用工具

* [TDengine样例导入工具](https://www.taosdata.com/blog/2020/01/18/1166.html)
* [TDengine性能对比测试工具](https://www.taosdata.com/blog/2020/01/18/1166.html)
* [IDEA数据库管理工具可视化使用TDengine](https://www.taosdata.com/blog/2020/08/27/1767.html)
* [基于eletron开发的跨平台TDengine图形化管理工具](https://github.com/skye0207/TDengineGUI)
* [DataX，支持TDengine的离线数据采集/同步工具](https://github.com/alibaba/DataX)

## TDengine与其他数据库的对比测试

* [用InfluxDB开源的性能测试工具对比InfluxDB和TDengine](https://www.taosdata.com/blog/2020/01/13/1105.html)
* [TDengine与OpenTSDB对比测试](https://www.taosdata.com/blog/2019/08/21/621.html)
* [TDengine与Cassandra对比测试](https://www.taosdata.com/blog/2019/08/14/573.html)
* [TDengine与InfluxDB对比测试](https://www.taosdata.com/blog/2019/07/19/419.html)
* [TDengine与InfluxDB、OpenTSDB、Cassandra、MySQL、ClickHouse等数据库的对比测试报告](https://www.taosdata.com/downloads/TDengine_Testing_Report_cn.pdf)

## 物联网大数据

* [物联网、工业互联网大数据的特点](https://www.taosdata.com/blog/2019/07/09/105.html)
* [物联网大数据平台应具备的功能和特点](https://www.taosdata.com/blog/2019/07/29/542.html)
* [通用大数据架构为什么不适合处理物联网数据？](https://www.taosdata.com/blog/2019/07/09/107.html)
* [物联网、车联网、工业互联网大数据平台，为什么推荐使用TDengine？](https://www.taosdata.com/blog/2019/07/09/109.html)

## 培训和FAQ

* [FAQ：常见问题与答案](/faq)
* [技术公开课：开源、高效的物联网大数据平台，TDengine内核技术剖析](https://www.taosdata.com/blog/2020/12/25/2126.html)
* [TDengine视频教程-快速上手](https://www.taosdata.com/blog/2020/11/11/1941.html)
* [TDengine视频教程-数据建模](https://www.taosdata.com/blog/2020/11/11/1945.html)
* [TDengine视频教程-集群搭建](https://www.taosdata.com/blog/2020/11/11/1961.html)
* [TDengine视频教程-Go Connector](https://www.taosdata.com/blog/2020/11/11/1951.html)
* [TDengine视频教程-JDBC Connector](https://www.taosdata.com/blog/2020/11/11/1955.html)
* [TDengine视频教程-NodeJS Connector](https://www.taosdata.com/blog/2020/11/11/1957.html)
* [TDengine视频教程-Python Connector](https://www.taosdata.com/blog/2020/11/11/1963.html)
* [TDengine视频教程-RESTful Connector](https://www.taosdata.com/blog/2020/11/11/1965.html)
* [TDengine视频教程-“零”代码运维监控](https://www.taosdata.com/blog/2020/11/11/1959.html)
* [应用案例：一些使用实例来解释如何使用TDengine](https://www.taosdata.com/cn/blog/?categories=4)
