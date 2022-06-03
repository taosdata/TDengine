---
title: 开发指南
---

开发一个应用，如果你准备采用TDengine作为时序数据处理的工具，那么有如下几个事情要做：
1. 确定应用到TDengine的链接方式。无论你使用何种编程语言，你总可以使用REST接口, 但也可以使用每种编程语言独有的连接器方便的进行链接。
2. 根据自己的应用场景，确定数据模型。根据数据特征，决定建立一个还是多个库；分清静态标签、采集量，建立正确的超级表，建立子表。
3. 决定插入数据的方式。TDengine支持使用标准的SQL写入，但同时也支持schemaless模式写入，这样不用手工建表，可以将数据直接写入。
4. 根据业务要求，看需要撰写哪些SQL查询语句。
5. 如果你要基于时序数据做实时的统计分析，包括各种监测看板，那么建议你采用TDengine的连续查询功能，而不用上线Spark, Flink等复杂的流式计算系统。
6. 如果你的应用有模块需要消费插入的数据，希望有新的数据插入时，就能获取通知，那么建议你采用TDengine提供的数据订阅功能，而无需专门部署Kafka或其他消息队列软件。
7. 在很多场景下(如车辆管理)，应用需要获取每个数据采集点的最新状态，那么建议你采用TDengine的cache功能，而不用单独部署Redis等缓存软件。
8. 如果你发现TDengine的函数无法满足你的要求，那么你可以使用用户自定义函数来解决问题。

本部分内容就是按照上述的顺序组织的。为便于理解，TDengine为每个功能为每个支持的编程语言都提供了示例代码。如果你希望深入了解SQL的使用，需要查看[SQL手册](/taos-sql/)。如果想更深入地了解各连接器的使用，请阅读[连接器参考指南](/reference/connector/)。如果还希望想将TDengine与第三方系统集成起来，比如Grafana, 请参考[第三方工具](/third-party/)。

如果在开发过程中遇到任何问题，请点击每个页面下方的["反馈问题"](https://github.com/taosdata/TDengine/issues/new/choose), 在GitHub上直接递交issue。

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
