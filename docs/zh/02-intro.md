---
sidebar_label: 产品简介
title: TDengine Cloud 的产品简介
---

TDengine Cloud 是全托管的时序数据处理云服务平台。它是基于开源的时序数据库 TDengine 而开发的。除高性能的时序数据库之外，它还具有缓存、订阅和流计算等系统功能，而且提供了便利而又安全的数据分享、以及众多的企业级服务功能。它可以让物联网、工业互联网、金融、IT 运维监控等领域企业在时序数据的管理上大幅降低人力成本和运营成本。

本章节主要介绍 TDengine Cloud 的主要功能，竞争优势和典型使用案例，让大家对 TDengine Cloud 有个整体的了解。

## 主要功能

TDengine Cloud 的主要功能如下：

1. 数据写入
   - 支持[使用 SQL 插入数据](../programming/insert/)。
   - 支持 [Telegraf](../data-in/telegraf/)。
   - 支持 [Prometheus](../data-in/prometheus/)。
2. 数据输出
   - 支持标准 [SQL](../programming/query/)，包括子查询。
   - 支持通过工具 [taosDump](../data-out/taosdump/) 导出数据。
   - 支持输出数据到 [Prometheus](../data-out/prometheus/)。
   - 支持通过[数据订阅](../data-subscription/)的方式导出数据.
3. 数据浏览器： 可以浏览数据库和各种表，如果您已经登录，还可以直接执行 SQL 查询语句。
4. 可视化
   - 支持 [Grafana](../visual/grafana/)。
   - 支持 Google Data Studio。
   - 支持 Grafana Cloud (稍后发布)
5. [数据订阅](../data-subscription/)： 用户的应用可以订阅一个数据库，一张表或者一组表。使用的 API 跟 Kafka 基本一致，但是您必须设置具体的过滤条件来定义一个主题，然后您可以和 TDengine Cloud 的其他用户或者用户组分享这个主题。
6. [流计算](../stream/)：不仅支持连续查询，TDengine还支持基于事件驱动的流计算，无需安装 Flink/Spark 就可以处理时序数据。
7. 企业版
   - 支持每天备份数据。
   - 支持复制一个数据库到另外一个区域或者另外一个云。
   - 支持对等 VPC。
   - 支持 IP 白名单。
9. 工具
   - 提供一个交互式 [命令行工具 (CLI)](../tools/cli/) 管理和实时查询。
   - 提供一个性能检测工具 [taosBenchmark](../tools/taosbenchmark/) 来测试 TDengine 的性能。
10. 编程
    - 提供各种[连接器](../programming/connector/)，比如 Java，Python，Go，Rust，Node.js 等编程语言。
    - 提供了[REST API](../programming/connector/rest-api/)。

更多细节功能，请阅读整个文档。

## 竞争优势

由于 TDengine Cloud 充分利用了[时序数据特点](https://www.taosdata.com/blog/2019/07/09/105.html)，比如结构化、无需事务、很少删除或更新、写多读少等等，还有它云原生的设计使 TDengine Cloud 区别于其他时序数据云服务，具有以下特点：

- **[极简时序数据平台](https://www.taosdata.com/tdengine/simplified_solution_for_time-series_data_processing)**：全托管的云服务，用户无需担心繁琐的部署、优化、扩容、备份、异地容灾等事务，可全心关注核心业务，减少对DBA的要求，大幅节省人力成本。

   除高性能、具有水平扩展能力的时序数据库外, TDengine 云服务还提供：

   **缓存**：无需部署 Redis，应用就能快速的获得最新数据。

   **数据订阅**：无需部署 Kafka, 当系统接收到新的数据时，应用将立即收到通知。

   **流式计算**：无需部署 Spark/Flink, 应用就能创建连续查询或时间驱动的流计算。

- **[便捷而且安全的数据共享](https://www.taosdata.com/tdengine/cloud/data-sharing)**：TDengine Cloud 既支持将一个库完全开放，设置读或写的权限；也支持通过数据订阅的方式，将库、超级表、一组或一张表、或聚合处理后的数据分享出去。

   **便捷**：如同在线文档一样简单，只需输入对方邮件地址，设置访问权限和访问时长即可实现分享。对方收到邮件，接受邀请后，可即刻访问。

   **安全**：访问权限可以控制到一个运行实例、库或订阅的 topic；对于每个授权的用户，对分享的资源，会生成一个访问用的 token；访问可以设置到期时间。

   便捷而又安全的时序数据共享，让企业各部门或合作伙伴之间快速洞察业务的运营。

- **[安全可靠的企业级服务](https://tdengine.com/tdengine/high-performance-time-series-database/)**：除强大的时序数据管理、共享功能之外，TDengine Cloud 还提供企业运营必需的

   **可靠**：提供数据定时备份、恢复，数据从运行实例到私有云、其他公有云或 Region 的实时复制。

   **安全**：提供基于角色的访问权限控制、IP 白名单、用户行为审计等功能。

   **专业**：提供7*24的专业技术服务，承诺 99.9% 的 Service Level Agreement。

   安全、专业、高效可靠的企业级服务，用户无需再为数据管理发愁，可以聚焦自身的核心业务。

- **[分析能力](https://www.taosdata.com/tdengine/easy_data_analytics)**：通过超级表、存储计算分离、分区分片、预计算和其它技术，TDengine 能够高效地浏览、格式化和访问数据。

- **[核心开源](https://www.taosdata.com/tdengine/open_source_time-series_database)**：TDengine 的核心代码包括集群功能全部在开源协议下公开。全球超过 140k 个运行实例，GitHub Star 20k，且拥有一个活跃的开发者社区。

采用 TDengine Cloud，可将典型的物联网、车联网、工业互联网大数据平台的总拥有成本大幅降低。表现在几个方面：

1. 由于其超强性能，它能将系统所需的计算资源和存储资源大幅降低
2. 因为支持 SQL，能与众多第三方软件无缝集成，学习迁移成本大幅下降
3. 因为是一款极简的时序数据平台，系统复杂度、研发和运营成本大幅降低

## 技术生态

在整个时序大数据平台中，TDengine 扮演的角色如下：

<figure>

![TDengine Database 技术生态图](eco_system.webp)

<center><figcaption>图 1. TDengine 技术生态图</figcaption></center>
</figure>

上图中，左侧是各种数据采集或消息队列，包括 OPC-UA、MQTT、Telegraf、也包括 Kafka，他们的数据将被源源不断的写入到 TDengine。右侧则是可视化、BI 工具、组态软件、应用程序。下侧则是 TDengine 自身提供的命令行程序（CLI）以及可视化管理工具。

## 典型适用场景

作为一个高性能、分布式、支持 SQL 的时序数据库（Database），TDengine 的典型适用场景包括但不限于 IoT、工业互联网、车联网、IT 运维、能源、金融证券等领域。需要指出的是，TDengine 是针对时序数据场景设计的专用数据库和专用大数据处理工具，因其充分利用了时序大数据的特点，它无法用来处理网络爬虫、微博、微信、电商、ERP、CRM 等通用型数据。下面本文将对适用场景做更多详细的分析。
