---
title: 部署架构及说明
sidebar_label: 部署架构
---

## 部署架构

下图展示了整个 TDengine 产品生态的部署架构，其所有核心组件都来自于前面章节中讲述过的 TDengine server 安装包以及 TDengine Pro Tools 安装包。

![TDengine 产品生态部署架构图](./deployment-taos.png)

对上图中各组件解释如下：
1. taosd cluster (target) 是指数据写入的目标集群，它也是监控数据产生的源头，同时还是 taos explorer 可视化工具管理的对象
2. taosAdapter 是为 TDengine 集群提供 RESTful 和 Websocket 访问接口的服务
3. taosKeeper 是将监控数据产生的源头产生的监控数据转入存储监控数据的 TDengine 集群的服务
4. taosd cluster (monitor) 是存储监控数据的 TDengine 集群，它既可以和 taosd cluster (target) 在物理上是同一集群，也可以是不同集群，可以按需选择。例如，如果有多个 TDengine 集群需要监控，建议部署一个独立的集群用于存储监控数据，独立于任何存储业务数据的集群。如果受限于资源环境，且只有一套业务系统集群，则可以复用该集群来存储自己的监控数据。
5. taosX 是用于在数据源和 TDengine 集群之间传输数据的零代码平台
6. taos explroer 是便于使用和管理 TDengine 集群以及各种数据传输任务的可视化工具。