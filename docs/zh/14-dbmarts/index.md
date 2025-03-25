---
sidebar_label: 数据库集市
title: 数据库集市
description: TDengine Cloud 的数据库集市
---

TDengine Cloud 为所有客户提供一个数据库集市，客户可以查看所有 TDengine Cloud 已经发布的数据库。点击每个数据库的类型 PUBLIC 或者 PRIVATE 下面的开关按钮，可以直接进入该数据库所在的**数据浏览器**页面，访问该数据库。如果数据库类型是 PRIVATE，需要客户输入相应的访问码进入。客户只拥有该数据库的读取权限，对该数据库进行查询操作。

每个客户都可以把自己的数据库发布到这个数据库集市。客户只需要到**数据浏览器**页面，选择要发布的数据库，在弹出的按钮中，单击**管理数据库权限**按钮，进入该数据库的访问控制页面。在这个页面中，点击**数据库集市访问**标签页面，进行简单的几个配置就可以把任何数据库发布到这个数据库集市。

如果想试用 TDgpt 的功能，请点击**时序数据预测分析数据集**公共数据库旁边的开关按钮，可以进入该数据库的数据浏览器，运行下面 SQL。

```SQL
select forecast(val, 'algo=tdtsfm_1') from forecast.electricity_demand_sub;
```

- 数据集详情请参考 [体验 TDgpt](../tdgpt/)
- TDgpt 详情请参考 [TDgpt文档](https://docs.taosdata.com/advanced/TDgpt/introduction/)
