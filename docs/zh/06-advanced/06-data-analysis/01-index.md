---
sidebar_label: TDgpt
title: TDgpt
---

## 概述

TDgpt 是 TDengine Enterprise 中针对时序数据提供高级分析功能的企业级组件，能够独立于 TDengine 主进程部署和运行，不消耗和占用 TDengine 主进程的资源，通过内置接口向 TDengine 提供运行时动态扩展的高级时序数据分析功能。TDgpt 具有服务无状态、功能易扩展、快速弹性部署、应用轻量化、高安全性等特点。
TDgpt 运行在部署于 TDengine 集群中的 Analysis Node (ANode)中。每个 TDengine 集群中可以部署一个或若干个 ANode 节点，不同的 ANode 节点之间不相关，无同步或协同的要求。ANode 注册到 TDengine 集群以后，就可以通过内部接口提供服务。TDgpt 提供的高级时序数据分析服务可分为时序数据异常检测和时序数据预测分析两个类别。

如下是数据分析的技术架构示意图。

<img src="./pic/data-analysis.png" width="560" alt="TDgpt架构图" />

通过注册指令语句，将 ANode 注册到 MNode 中就加入到 TDengine 集群，查询会按需向其请求数据分析服务。请求服务通过 VNode 直接向 ANode 发起，用户则可以通过 SQL 语句直接调用 ANode 提供的服务。

TDgpt 提供的高级数据分析功能分为时序数据异常检测和时序数据预测两大类。
时序数据异常检测的结果采用异常窗口的形式提供，即分析系统自动将连续的异常数据以时间窗口的形式返回，其使用方式与 TDengine 中其他类型的时间窗口类似。特别地，可以将异常时序数据窗口视作为一种特殊的**事件窗口（Event Window）**，因此事件窗口可应用的查询操作均可应用在异常窗口上。如下图所示，分析平台将返回时序数据异常窗口 [10:51:30, 10:54:40] （红色背景部分数据）。

<img src="./pic/anomaly-detection.png" width="560" alt="异常检测" />

