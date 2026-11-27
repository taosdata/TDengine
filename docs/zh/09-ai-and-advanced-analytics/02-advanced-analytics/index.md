---
sidebar_label: 高级分析
title: 高级分析
description: TDengine 时序预测、异常检测、补值与相关分析
---

> 使用本章描述的时序数据高级分析功能，需要你首先部署 TDgpt 服务，并将服务注册到 TDengine 中，具体操作步骤请参见 [安装部署](../01-tdgpt/02-tutorial.md)。

本章介绍 TDengine 针对时间序列数据的高级分析功能，包括：时间序列数据预测分析、异常数据检测、缺失值补值功能和时间序列相关性分析功能。其中 `CORR` 等内置函数说明见 [内置函数](../../05-tdengine-sql/04-data-query/03-function.md)；Anode 状态可用 [SHOW ANODES](../../05-tdengine-sql/09-system-info/03-show.md#show-anodes) 查询。

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
