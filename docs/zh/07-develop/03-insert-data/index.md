---
sidebar_label: 写入数据
title: 写入数据
description: TDengine 的各种写入方式
---

TDengine 支持多种写入协议，包括 SQL，InfluxDB Line 协议, OpenTSDB Telnet 协议，OpenTSDB JSON 格式协议。数据可以单条插入，也可以批量插入，可以插入一个数据采集点的数据，也可以同时插入多个数据采集点的数据。同时，TDengine 支持多线程插入，支持时间乱序数据插入，也支持历史数据插入。InfluxDB Line 协议、OpenTSDB Telnet 协议和 OpenTSDB JSON 格式协议是 TDengine 支持的三种无模式写入协议。使用无模式方式写入无需提前创建超级表和子表，并且引擎能自适用数据对表结构做调整。

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```