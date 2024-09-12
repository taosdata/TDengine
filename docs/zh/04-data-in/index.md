---
sidebar_label: 数据写入
title: 往 TDengine Cloud 实例里面写入数据
description: 有多种方式可以往 TDengine Cloud 实例里面写入数据。
---

TDengine Cloud 在数据写入方面提供了三种数据写入的方式把外部数据写入到 TDengine Cloud 的实例里面。

第一种是数据源的方式，用户通过简单易用的几步操作，就可以把远程的大量数据源方便的导入到 TDengine Cloud 的实例中。目前支持的数据类型包括 TDengine 订阅，InfluxDB，MQTT，PI，OPC-UA 和OPC-DA。其中 TDengine 订阅包括本地主机上或者云厂商服务器内部按照的 TDengine，以及 TDengine Cloud 的其他实例。

第二种是通过第三方工具或者通过REST接口的无模式支持的第三方协议往 TDengine Cloud 的实例里面写入数据，目前支持的第三方代理工具有 Telegraf 和 Prometheus，第三方协议有 InfluxDB，OpenTSDB JSON 和 OpenTSDB Telnet 协议。

第三种是通过 CSV 文件的方式导入到 TDengine Cloud 实例中的某个数据库的某一张子表。特别注意，CSV文件每次只能上传一个。

除了本章描述的往 TDengine Cloud 实例里面写入数据的三种方式，用户还可以直接使用 TDengine SQL 往 TDengine Cloud 里面写入数据，也可以通过编程的方式使用 TDengine 提供的[连接器（Connector）](../programming/client-libraries)往 TDengine Cloud 里面实例写入数据。TDengine 还提供压力测试工具 [taosBenchmark](../tools/taosbenchmark)往 TDengine Cloud 里面实例写入数据。

:::注意
由于权限的限制，必须首先在云服务的数据浏览器里面创建数据库，然后才能往这个数据库里面写入数据。这个限制是所有写入方式必须首先做的。
:::

主要功能如下所示：

1. [数据源](./ds/): 通过创建数据源把外部的数据写入 TDengine Cloud 的实例。
2. [数据采集代理](./dca/): 通过第三方代理工具和REST接口的无模式。
<!-- 3. [CSV文件](./csv/): 创建、更新或删除用户或者用户组。您还可以创建/编辑/删除自定义角色。 -->

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
