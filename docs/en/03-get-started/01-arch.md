---
title: 部署架构及说明
sidebar_label: 部署架构
---

本节简要描述 TDengine Enterprise 的部署架构，并对其中的核心组件予以说明。

## 部署架构

下图展示了整个 TDengine 产品生态的部署架构，其所有核心组件都来自于前面章节中讲述过的 TDengine server 安装包以及 taosX 安装包。

![TDengine 产品生态部署架构图](./arch_taos.png)

## 架构详解

对上图中各组件解释如下：

1. TDengine cluster (business) 是指存储业务数据的目标集群，简称为业务集群，它也是监控数据产生的源头，同时还是 taos explorer 可视化工具管理的对象。

2. taosAdapter (cluster) 是为 业务集群提供 RESTful 和 Websocket 访问接口的服务，它既可以是单一实例，也可以是多个实例配合反向代理（如nginx）。

3. taosKeeper 是将监控数据产生的源头产生的监控数据转入存储监控数据的 TDengine 集群的服务。

4. TDengine cluster (monitoring) 是存储监控数据的 TDengine 集群，简称为监控集群，它既可以和业务集群在物理上是同一集群，也可以是不同集群，可以按需选择。如果有多个 TDengine 集群需要监控，建议部署一个独立的集群用于存储监控数据，独立于任何存储业务数据的集群。如果受限于资源环境，且只有一套业务系统集群，则可以复用该集群来存储自己的监控数据。

5. taosAdapter (monitoring) 是为监控集群提供 RESTful 和 Websocket 访问接口的服务。如果监控集群与业务集群是相同集群，则与它们配套的 taosAdapter 可以是相同实例也可以是不同实例。但如果这两个是不同集群，则与它们配套的 taosAdapter 一定是不同实例。同样， taosAdapter 可以是单一实例也可以是配合反向代理的多实例。

6. taosX 是用于在数据源和 TDengine 集群之间传输数据的零代码平台。数据源除了 TDengine 集群以外，还包括一些第三方数据源，如 MQTT, InfluxDB, OpenTSDB, Kafka, Pi, OPC UA, OPC DA

7. Agent 是一个独的可选组件，在从有些数据源接入数据到 TDengine 集群时，它是必须的。

8. taos explroer 是便于使用和管理 TDengine 集群以及各种数据传输任务的可视化工具。

9. Grafana：存储在监控集群中的监控数据可以通过 Grafana 来呈现关键的监控指标。

10. Applications：向业务集群写入业务数据或从中查询业务数据的应用程序。其中 Applications (native connection) 是指采用原生连接的应用，应用直接连接到业务集群。Applications (RESTful) 是指采用 RESTful 接口访问业务集群的应用。 Applications (WebSocket) 是采用 WebSocket 连接的应用。 RESTful 和 WebSocket 访问都需要通过  taosAdapter 。