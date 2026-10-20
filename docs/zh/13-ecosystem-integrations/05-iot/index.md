---
sidebar_label: 物联网平台
title: 与各种物联网平台集成
description: 将 Node-RED、Ignition、Neuron 等物联网平台与 TDengine 集成
toc_max_heading_level: 4
---

本节说明如何将 Node-RED、Ignition、Neuron 等物联网平台与 TDengine 集成。通过 Neuron 与 eKuiper 的 InfluxDB v1 sink，可将工业设备采集数据写入 TDengine，并配置拼批、加密传输和高可用部署。工业协议零代码接入可参阅 [零代码数据写入](../../08-data-ingest-and-delivery/01-no-code-ingestion/index.md)；数据订阅概念见 [数据订阅](../../06-data-subscription/index.md)。

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
