---
sidebar_label: 数据输出
title: 数据输出
description:  TDengine Cloud 的多种数据输出方式。
---

本章主要介绍如何从 TDengine Cloud 中输出数据。除了使用平常的 SQL 查询，用户还可以使用 TDengine Cloud 提供的[数据订阅](../data-subscription)来进行数据订阅，并将存储在 TDengine 的数据分享给其他人。TDengine 还提供了[连接器](../programming/connector)让开发者可以方便得通过应用程序访问存储在 TDengine 的数据。TDengine还提供了一些工具，如[taosdump](../tools/taosdump)，它用于将存储在 TDengine Cloud 中的数据转储为文件，还有另外一个工具 `taosX`，用于将一个 TDengine Cloud 中的数据同步到另一个。此外，也可以通过第三方工具，如Prometheus，将数据写入 TDengine 中。
