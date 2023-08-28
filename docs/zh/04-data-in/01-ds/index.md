---
sidebar_label: 数据源
title: 数据源管理
description: '数据源管理'
---

TDengine Cloud 提供了简单快捷的一站式数据源管理来方便用户从各种数据源导入数据到 TDengine Cloud 实例的数据库里面。目前支持的数据源包括 TDengine，InfluexDB，MQTT，PI 系统, OPC 系统以及 CSV 等数据源。用户只需在数据源页面进行简单的创建步骤就可以实现各种数据源到 TDengine Cloud 实例的数据迁移。

这部分数据写入内容主要分成两个部分，数据源本身的管理和连接代理的管理。除了“TDengine 订阅”和“旧版本 TDengine”这两种数据源以外，其他数据源都需要首先创建相应的连接代理，才能创建数据源本身。

## 数据源

用户点击右上角的**添加数据源**可以添加新的数据源。当创建完成后，用户可以在数据源列表的**操作**列对已创建的数据源进行管理。对每个已创建的数据源，用户都可以进行修改，启动，暂停，刷新和删除等操作。

## 连接代理

用户点击连接代理列表右上角的**创建新的代理**按钮可以添加新的连接代理。按照连接代理的创建步骤指引就可以完成新的连接代理的创建。创建完成以后，用户可以在该连接代理的操作区域编辑该连接代理，比如修改它的名称，刷新生成的代理令牌以及删除该连接代理。

```mdx-code-block
import DocCardList from '@theme/DocCardList';
import {useCurrentSidebarCategory} from '@docusaurus/theme-common';

<DocCardList items={useCurrentSidebarCategory().items}/>
```
