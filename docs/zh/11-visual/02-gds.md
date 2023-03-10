---
sidebar_label: Google Data Studio
title: 使用Google Data Studio
description: 使用 Google Data Studio 存取 TDengine 数据的详细指南
---

使用[合作伙伴的连接器](https://datastudio.google.com/data?search=TDengine)，Google Data Studio可以快速访问 TDengine， 并且通过基于网页的报表功能可以快速创建交互式的报表和仪表盘。整个过程不需要任何的代码编写过程。可以分享报表和仪表盘给不同的个人，团队以及全世界，还可以跟其他人员实时协作，另外在任何的网页里面嵌入您的报表。

更多使用 Data Studio 和 TDengine 集成可以参考[GitHub](https://github.com/taosdata/gds-connector/blob/master/README.md)。

## 选择数据源

目前的 [连接器](https://datastudio.google.com/data?search=TDengine) 支持两种不同的数据源：TDengine Server 和 TDengine Cloud。首先选择”TDengine Cloud“类型然后点击”下一步“。

![Data Studio 数据源选择器](./gds/gds_data_source.webp)

## 连接器配置

### 必须的配置

#### URL

<!-- exclude -->
获取实际的URL，请登录[TDengine Cloud](https://cloud.taosdata.com) 后点击左边的”可视化“菜单，然后选择”Google Data Studio“。
<!-- exclude-end -->

#### TDengine Cloud 令牌

<!-- exclude -->
获取实际的令牌，请登录[TDengine Cloud](https://cloud.taosdata.com) 后点击左边的”可视化“菜单，然后选择”Google Data Studio“。

<!-- exclude-end -->

#### 数据库

数据库的名称，该数据库包含您想查询数据和创建报表的的表，可以是一般表，超级表或者子表。

#### 表

您希望查询数据和执行报表的表的名称

**注意** 可以获取的最大记录行数是1000000。

### 可选配置

#### 查询开始时间和结束时间

在页面上面配置您的连接器的两个时间输入框，这两个时间过滤条件是用来过滤大量数据的。时间输入框的格式是”YYYY-MM-DD HH:MM:SS“。
比如：

``` bash
2022-05-12 18:24:15
```

查询结果的开始时间戳是由 `start date` 定义的。加上这个条件，您不会获取到在 `start date` 时间戳之前的数据。

`end time`输入框表明查询结束的时间戳。因此，在结束时间戳之后的数据也获取不到。这些条件是利用 SQL 的 where 语句来实现的。比如：

```SQL
-- select * from table_name where ts >= start_date and ts <= end_date
select * from test.demo where ts >= '2022-05-10 18:24:15' and ts<='2022-05-12 18:24:15'
```

事实上，您可通过一些过滤器来加快报表加载数据的速度。
![TDengine Cloud 配置页面](./gds/gds_cloud_login.webp)

在配置完成以后，点击"CONNECT"按钮，您就会连接上您的具有给定数据库和表的”TDengine Cloud “。

## 创建报表和仪表盘

使用交互式仪表盘和优美报表解锁您的 TDengine 数据能力，更多详情请参考[文档](https://docs.tdengine.com/third-party/google-data-studio/)
