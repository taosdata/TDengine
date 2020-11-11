# TDengine文档

TDengine是一个高效的存储、查询、分析时序大数据的平台，专为物联网、车联网、工业互联网、运维监测等优化而设计。您可以像使用关系型数据库MySQL一样来使用它，但建议您在使用前仔细阅读一遍下面的文档，特别是[数据模型](https://www.taosdata.com/cn/documentation20/data-model-and-architecture)与[数据建模](https://www.taosdata.com/cn/documentation20/model)一节。除本文档之外，欢迎[下载产品白皮书](https://www.taosdata.com/downloads/TDengine White Paper.pdf)。如需查阅TDengine 1.6 文档，请点击[这里](https://www.taosdata.com/cn/documentation16/)访问。

## TDengine介绍

- TDengine 简介及特色
- TDengine 适用场景

## [立即开始](https://www.taosdata.com/cn/getting-started)

- [快捷安装](https://www.taosdata.com/cn/documentation20/getting-started/#快捷安装)：可通过源码、安装包或docker安装，三秒钟搞定
- [轻松启动](https://www.taosdata.com/cn/documentation20/getting-started/#轻松启动)：使用systemctl 启停TDengine
- [命令行程序TAOS](https://www.taosdata.com/cn/documentation20/getting-started/#TDengine命令行程序)：访问TDengine的简便方式
- [极速体验](https://www.taosdata.com/cn/documentation20/getting-started/#TDengine-极速体验)：运行示例程序，快速体验高效的数据插入、查询

## [数据模型和整体架构](https://www.taosdata.com/cn/documentation20/architecture)

- [数据模型](https://www.taosdata.com/cn/documentation20/architecture/#数据模型)：关系型数据库模型，但要求每个采集点单独建表
- [集群与基本逻辑单元](https://www.taosdata.com/cn/documentation20/architecture/#集群与基本逻辑单元)：吸取NoSQL优点，支持水平扩展，支持高可靠
- [存储模型与数据分区、分片](https://www.taosdata.com/cn/documentation20/architecture/#存储模型与数据分区、分片)：标签数据与时序数据完全分离，按vnode和时间两个维度对数据切分
- [数据写入与复制流程](https://www.taosdata.com/cn/documentation20/architecture/#数据写入与复制流程)：先写入WAL、之后写入缓存，再给应用确认，支持多副本
- [缓存与持久化](https://www.taosdata.com/cn/documentation20/architecture/#缓存与持久化)：最新数据缓存在内存中，但落盘时采用列式存储、超高压缩比
- [数据查询](https://www.taosdata.com/cn/documentation20/architecture/#数据查询)：支持各种函数、时间轴聚合、插值、多表聚合

## [数据建模](https://www.taosdata.com/cn/documentation20/model)

- [创建库](https://www.taosdata.com/cn/documentation20/model/#创建库)：为具有相似数据特征的数据采集点创建一个库
- [创建超级表](https://www.taosdata.com/cn/documentation20/model/#创建超级表)：为同一类型的数据采集点创建一个超级表
- [创建表](https://www.taosdata.com/cn/documentation20/model/#创建表)：使用超级表做模板，为每一个具体的数据采集点单独建表

## [高效写入数据](https://www.taosdata.com/cn/documentation20/insert)

- [SQL写入](https://www.taosdata.com/cn/documentation20/insert/#SQL写入)：使用SQL insert命令向一张或多张表写入单条或多条记录
- [Telegraf写入](https://www.taosdata.com/cn/documentation20/insert/#Telegraf直接写入)：配置Telegraf, 不用任何代码，将采集数据直接写入
- [Prometheus写入](https://www.taosdata.com/cn/documentation20/insert/#Prometheus直接写入)：配置Prometheus, 不用任何代码，将数据直接写入
- [EMQ X Broker](https://www.taosdata.com/cn/documentation20/insert/#EMQ-X-Broker)：配置EMQ X，不用任何代码，就可将 MQTT 数据直接写入
- [HiveMQ Broker](https://www.taosdata.com/cn/documentation20/insert/#HiveMQ-Broker)：通过 HiveMQ Extension，不用任何代码，就可将 MQTT 数据直接写入

## [高效查询数据](https://www.taosdata.com/cn/documentation20/queries)

- [主要查询功能](https://www.taosdata.com/cn/documentation20/queries/#主要查询功能)：支持各种标准函数，设置过滤条件，时间段查询
- [多表聚合查询](https://www.taosdata.com/cn/documentation20/queries/#多表聚合查询)：使用超级表，设置标签过滤条件，进行高效聚合查询
- [降采样查询值](https://www.taosdata.com/cn/documentation20/queries/#降采样查询、插值)：按时间段分段聚合，支持插值

## [高级功能](https://www.taosdata.com/cn/documentation20/advanced-features)

- [连续查询(Continuous Query)](https://www.taosdata.com/cn/documentation20/advanced-features/#连续查询(Continuous-Query))：基于滑动窗口，定时自动的对数据流进行查询计算
- [数据订阅(Publisher/Subscriber)](https://www.taosdata.com/cn/documentation20/advanced-features/#数据订阅(Publisher/Subscriber))：象典型的消息队列，应用可订阅接收到的最新数据
- [缓存(Cache)](https://www.taosdata.com/cn/documentation20/advanced-features/#缓存(Cache))：每个设备最新的数据都会缓存在内存中，可快速获取
- [报警监测](https://www.taosdata.com/cn/documentation20/advanced-features/#报警监测(Alert))：根据配置规则，自动监测超限行为数据，并主动推送

## [连接器](https://www.taosdata.com/cn/documentation20/connector)

- [C/C++ Connector](https://www.taosdata.com/cn/documentation20/connector/#C/C++-Connector)：通过libtaos客户端的库，连接TDengine服务器的主要方法
- [Java Connector(JDBC)](https://www.taosdata.com/cn/documentation20/connector-java)：通过标准的JDBC API，给Java应用提供到TDengine的连接
- [Python Connector](https://www.taosdata.com/cn/documentation20/connector/#Python-Connector)：给Python应用提供一个连接TDengine服务器的驱动
- [RESTful Connector](https://www.taosdata.com/cn/documentation20/connector/#RESTful-Connector)：提供一最简单的连接TDengine服务器的方式
- [Go Connector](https://www.taosdata.com/cn/documentation20/connector/#Go-Connector)：给Go应用提供一个连接TDengine服务器的驱动
- [Node.js Connector](https://www.taosdata.com/cn/documentation20/connector/#Node.js-Connector)：给node应用提供一个链接TDengine服务器的驱动

## [与其他工具的连接](https://www.taosdata.com/cn/documentation20/connections-with-other-tools)

- [Grafana](https://www.taosdata.com/cn/documentation20/connections-with-other-tools/#Grafana)：获取并可视化保存在TDengine的数据
- [Matlab](https://www.taosdata.com/cn/documentation20/connections-with-other-tools/#Matlab)：通过配置Matlab的JDBC数据源访问保存在TDengine的数据
- [R](https://www.taosdata.com/cn/documentation20/connections-with-other-tools/#R)：通过配置R的JDBC数据源访问保存在TDengine的数据

## [TDengine集群的安装、管理](https://www.taosdata.com/cn/documentation20/cluster)

- [安装](https://www.taosdata.com/cn/documentation20/cluster/#创建第一个节点)：与单节点的安装一样，但要设好配置文件里的参数first
- [节点管理](https://www.taosdata.com/cn/documentation20/cluster/#节点管理)：增加、删除、查看集群的节点
- [mnode的管理](https://www.taosdata.com/cn/documentation20/cluster/#Mnode的高可用)：系统自动创建、无需任何人工干预
- [负载均衡](https://www.taosdata.com/cn/documentation20/cluster/#负载均衡)：一旦节点个数或负载有变化，自动进行
- [节点离线处理](https://www.taosdata.com/cn/documentation20/cluster/#节点离线处理)：节点离线超过一定时长，将从集群中剔除
- [Arbitrator](https://www.taosdata.com/cn/documentation20/cluster/#Arbitrator的使用)：对于偶数个副本的情形，使用它可以防止split brain

## [TDengine的运营和维护](https://www.taosdata.com/cn/documentation20/administrator)

- [容量规划](https://www.taosdata.com/cn/documentation20/administrator/#容量规划)：根据场景，估算硬件资源
- [容错和灾备](https://www.taosdata.com/cn/documentation20/administrator/#容错和灾备)：设置正确的WAL和数据副本数
- [系统配置](https://www.taosdata.com/cn/documentation20/administrator/#服务端配置)：端口，缓存大小，文件块大小和其他系统配置
- [用户管理](https://www.taosdata.com/cn/documentation20/administrator/#用户管理)：添加、删除TDengine用户，修改用户密码
- [数据导入](https://www.taosdata.com/cn/documentation20/administrator/#数据导入)：可按脚本文件导入，也可按数据文件导入
- [数据导出](https://www.taosdata.com/cn/documentation20/administrator/#数据导出)：从shell按表导出，也可用taosdump工具做各种导出
- [系统监控](https://www.taosdata.com/cn/documentation20/administrator/#系统监控)：检查系统现有的连接、查询、流式计算，日志和事件等
- [文件目录结构](https://www.taosdata.com/cn/documentation20/administrator/#文件目录结构)：TDengine数据文件、配置文件等所在目录

## [TAOS SQL](https://www.taosdata.com/cn/documentation20/taos-sql)

- [支持的数据类型](https://www.taosdata.com/cn/documentation20/taos-sql/#支持的数据类型)：支持时间戳、整型、浮点型、布尔型、字符型等多种数据类型
- [数据库管理](https://www.taosdata.com/cn/documentation20/taos-sql/#数据库管理)：添加、删除、查看数据库
- [表管理](https://www.taosdata.com/cn/documentation20/taos-sql/#表管理)：添加、删除、查看、修改表
- [超级表管理](https://www.taosdata.com/cn/documentation20/taos-sql/#超级表STable管理)：添加、删除、查看、修改超级表
- [标签管理](https://www.taosdata.com/cn/documentation20/taos-sql/#超级表-STable-中-TAG-管理)：增加、删除、修改标签
- [数据写入](https://www.taosdata.com/cn/documentation20/taos-sql/#数据写入)：支持单表单条、多条、多表多条写入，支持历史数据写入
- [数据查询](https://www.taosdata.com/cn/documentation20/taos-sql/#数据查询)：支持时间段、值过滤、排序、查询结果手动分页等
- [SQL函数](https://www.taosdata.com/cn/documentation20/taos-sql/#SQL函数)：支持各种聚合函数、选择函数、计算函数，如avg, min, diff等
- [时间维度聚合](https://www.taosdata.com/cn/documentation20/taos-sql/#时间维度聚合)：将表中数据按照时间段进行切割后聚合，降维处理
- [边界线制](https://www.taosdata.com/cn/documentation20/taos-sql/#TAOS-SQL-边界限制)：TAOS SQL的边界限制
- [错误码](https://www.taosdata.com/cn/documentation20/Taos-Error-Code)：TDengine 2.0 错误码以及对应的十进制码

## TDengine的技术设计

- 系统模块：taosd的功能和模块划分
- 数据复制：支持实时同步、异步复制，保证系统的High Availibility
- [技术博客](https://www.taosdata.com/cn/blog/?categories=3)：更多的技术分析和架构设计文章

## 常用工具

- [TDengine样例导入工具](https://www.taosdata.com/blog/2020/01/18/1166.html)
- [TDengine性能对比测试工具](https://www.taosdata.com/blog/2020/01/18/1166.html)

## TDengine与其他数据库的对比测试

- [用InfluxDB开源的性能测试工具对比InfluxDB和TDengine](https://www.taosdata.com/blog/2020/01/13/1105.html)
- [TDengine与OpenTSDB对比测试](https://www.taosdata.com/blog/2019/08/21/621.html)
- [TDengine与Cassandra对比测试](https://www.taosdata.com/blog/2019/08/14/573.html)
- [TDengine与InfluxDB对比测试](https://www.taosdata.com/blog/2019/07/19/419.html)
- [TDengine与InfluxDB、OpenTSDB、Cassandra、MySQL、ClickHouse等数据库的对比测试报告](https://www.taosdata.com/downloads/TDengine_Testing_Report_cn.pdf)

##物联网大数据

- [物联网、工业互联网大数据的特点](https://www.taosdata.com/blog/2019/07/09/105.html)
- [物联网大数据平台应具备的功能和特点](https://www.taosdata.com/blog/2019/07/29/542.html)
- [通用大数据架构为什么不适合处理物联网数据？](https://www.taosdata.com/blog/2019/07/09/107.html)
- [物联网、车联网、工业互联网大数据平台，为什么推荐使用TDengine？](https://www.taosdata.com/blog/2019/07/09/109.html)

## [培训和FAQ](https://www.taosdata.com/cn/faq)

- [FAQ](https://www.taosdata.com/cn/documentation20/faq)：常见问题与答案
- [应用案列](https://www.taosdata.com/cn/blog/?categories=4)：一些使用实例来解释如何使用TDengine