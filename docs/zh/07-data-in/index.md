---
sidebar_label: 数据写入
title: 从TDengine云服务里面写入数据
description: 有多种方式可以往 TDengine 里面写入数据。
---

这章主要介绍目前有多种方式往 TDengine 里面写入数据，比如用户可以直接使用 TDengine SQL 往 TDengine Cloud 里面写入数据，也可以通过编程的方式使用 TDengine 提供的[连接器（Connector）](../programming/connector)往TDengine里面写入数据。TDengine 还提供压力测试工具 [taosBenchmark](../tools/taosbenchmark)往TDengine里面写入数据，另外 TDengine 企业版还提供工具 taosX 可以从一个 TDengine Cloud 实例同步数据到另外一个。

此外，通过第三方工具 Telegraf 和 Prometheus，也可以往 TDengine 写入数据。

:::注意
由于权限的限制，有必须首先在云服务的数据浏览器里面创建数据库，然后才能往这个数据库里面写入数据。这个限制是所有写入方式必须首先做的。
:::
