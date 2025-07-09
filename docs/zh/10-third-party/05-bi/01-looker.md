---
sidebar_label: Looker
title: 与 Looker Studio 的集成
toc_max_heading_level: 4
---
Looker Studio，作为 Google 旗下的一个功能强大的报表和商业智能工具，前身名为 Google Data Studio。在 2022 年的 Google Cloud Next 大会上，Google 将其更名为 Looker Studio。这个工具凭借其丰富的数据可视化选项和多样化的数据连接能力，为用户提供了便捷的数据报表生成体验。用户可以根据预设的模板轻松创建数据报表，满足各种数据分析需求。

由于其简单易用的操作界面和庞大的生态系统支持，Looker Studio 在数据分析领域受到众多数据科学家和专业人士的青睐。无论是初学者还是资深分析师，都可以利用 Looker Studio 快速构建美观且实用的数据报表，从而更好地洞察业务趋势、优化决策过程并提升整体运营效率。

## 获取

目前，TDengine 连接器作为 Looker Studio 的合作伙伴连接器（partner connector），已在 Looker Studio 官网上线。用户访问 Looker Studio 的 Data Source 列表时，只须输入“TDengine”进行搜索，便可轻松找到并立即使用 TDengine 连接器。

TDengine 连接器兼容 TDengine Cloud 和 TDengine Server 两种类型的数据源。TDengine Cloud 是涛思数据推出的全托管物联网和工业互联网大数据云服务平台，为用户提供一站式数据存储、处理和分析解决方案；而 TDengine Server 则是用户自行部署的本地版本，支持通过公网访问。以下内容将以 TDengine Cloud 为例进行介绍。

## 使用

在 Looker Studio 中使用 TDengine 连接器的步骤如下。

第 1 步，进入 TDengine 连接器的详情页面后，在 Data Source 下拉列表中选择 TDengine Cloud，然后点击 Next 按钮，即可进入数据源配置页面。在该页面中填写以下信息，然后点击 Connect 按钮。
   - URL 和 TDengine Cloud Token，可以从 TDengine Cloud 的实例列表中获取。
   - 数据库名称和超级表名称。
   - 查询数据的开始时间和结束时间。

第 2 步，Looker Studio 会根据配置自动加载所配置的 TDengine 数据库下的超级表的字段和标签。

第 3 步，点击页面右上角的 Explore 按钮，即查看从 TDengine 数据库中加载的数据。

第 4 步，根据需求，利用 Looker Studio 提供的图表，进行数据可视化的配置。

**注意** 在第一次使用时，请根据页面提示，对 Looker Studio 的 TDengine 连接器进行访问授权。