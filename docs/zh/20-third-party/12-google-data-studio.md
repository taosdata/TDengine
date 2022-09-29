---
sidebar_label: Google Data Studio
title: TDengine Google Data Studio Connector
description: 使用 Google Data Studio 存取 TDengine 数据的详细指南
---

Google Data Studio 是一个强大的报表可视化工具，它提供了丰富的数据图表和数据连接，可以非常方便地按照既定模板生成报表。因其简便易用和生态丰富而在数据分析领域得到一众数据科学家的青睐。

Data Studio 可以支持多种数据来源，除了诸如 Google Analytics、Google AdWords、Search Console、BigQuery 等 Google 自己的服务之外，用户也可以直接将离线文件上传至 Google Cloud Storage，或是通过连接器来接入其它数据源。

![01](gds/gds-01.webp)

目前 TDengine 连接器已经发布到 Google Data Studio 应用商店，你可以在 “Connect to Data” 页面下直接搜索 TDengine，将其选作数据源。

![02](gds/gds-02.png.webp)

接下来选择 AUTHORIZE 按钮。

![03](gds/gds-03.png.webp)

设置允许连接自己的账号到外部服务。

![04](gds/gds-04.png.webp)

在接下来的页面选择运行 TDengine REST 服务的 URL，并输入用户名、密码、数据库名称、表名称以及查询时间范围，并点击右上角的 CONNECT 按钮。

![05](gds/gds-05.png.webp)

连接成功后，就可以使用 GDS 方便地进行数据处理并创建报表了。

![06](gds/gds-06.png.webp)

目前的维度和指标规则是：timestamp 类型的字段和 tag 字段会被连接器定义为维度，而其他类型的字段是指标。用户还可以根据自己的需求创建不同的表。

![07](gds/gds-07.png.webp)
![08](gds/gds-08.png.webp)
![09](gds/gds-09.png.webp)
![10](gds/gds-10.png.webp)
![11](gds/gds-11.png.webp)
