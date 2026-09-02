---
sidebar_label: 技术内幕
title: 技术内幕
description: TDengine 架构、存储、查询、虚拟表、数据订阅与流式计算等内部设计概览
---

本章从实现视角简要说明 TDengine 的内部设计，涵盖集群架构、存储与查询引擎、虚拟表查询优化、数据订阅（TMQ）、流式计算、缓存、压缩与日志等。产品用法请优先参阅对应专题：[流式计算](../07-stream-processing/index.md)、[数据订阅](../06-data-subscription/index.md)、[TDengine SQL](../05-tdengine-sql/index.md)；本节侧重机制理解，具体参数与语法以专题文档为准。

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
