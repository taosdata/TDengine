---
sidebar_label: 永洪 BI
title: 永洪 BI
description: 使用 TDengine 连接永洪 BI
---

# 工具 - 永洪 BI

![永洪 BI use step](./yonghongbi-step-zh.png)

[永洪一站式大数据 BI 平台](https://www.yonghongtech.com/)为各种规模的企业提供灵活易用的全业务链的大数据分析解决方案，让每一位用户都能使用这一平台轻松发掘大数据价值，获取深度洞察力。TDengine 可以通过 JDBC 连接器作为数据源添加到永洪 BI 中。完成数据源配置后，永洪 BI 就能从 TDengine 中读取数据，并提供数据展示、分析和预测等功能。

### 前置条件

1. Yonghong Desktop Basic 已经安装并运行（如果未安装，请到[永洪科技官方下载页面](https://www.yonghongtech.com/cp/desktop/)下载）。
2. TDengine 已经安装并运行，并确保在 TDengine 服务端启动了 taosadapter 服务。

### 安装 JDBC 连接器

从 [maven.org](https://central.sonatype.com/artifact/com.taosdata.jdbc/taos-jdbcdriver/versions) 下载最新的 TDengine JDBC 连接器（目前的版本是 3.2.7），并安装在 BI 工具运行的机器上。

### 配置 TDengine JDBC 数据源

1. 在打开的 Yonghong Desktop BI 工具中点击“添加数据源”，选择 SQL 数据源中的“GENERIC”类型。
2. 点击“选择自定义驱动”，在“驱动管理”对话框中，点击“驱动列表”旁边的“+”，输入名称“MyTDengine”。然后点击“上传文件”按钮上传刚刚下载的 TDengine JDBC 连接器文件"taos-jdbcdriver-3.2.7-dist.jar"，并选择“com.taosdata.jdbc.rs.RestfulDriver”驱动，最后点击“确定”按钮完成驱动添加。
3. 然后请复制下面的内容到“URL”字段：
```
jdbc:TAOS-RS://localhost:6041?user=root&password=taosdata
```
4. 接着在“认证方式”选择“无身份认证”。
5. 在数据源的高级设置中，修改“Quote符号”的值为反引号“`”。
6. 点击“测试连接”，弹出“测试成功”的对话框。点击“保存”按钮，输入“MyTDengine”来保存 TDengine 数据源。

### 创建 TDengine 数据集

1. 在 BI 工具中点击“添加数据源”，展开刚刚创建的数据源，并浏览 TDengine 中的超级表。
2. 您可以将超级表的数据全部加载到 BI 工具中，也可以通过自定义 SQL 语句导入部分数据。
3. 当勾选“数据库内计算”时，BI 工具将不再缓存 TDengine 的时序数据，并在处理查询时将 SQL 请求发送给 TDengine 直接处理。

当导入数据后，BI 工具会自动将数值类型设置为“度量”列，将文本类型设置为“维度”列。而在 TDengine 的超级表中，采用普通列作为数据的度量，采用标签列作为数据的维度，因此您可能需要在创建数据集时更改部分列的属性。TDengine 在支持标准 SQL 的基础之上，还提供了一系列满足时序业务场景需求的特色查询语法，例如数据切分查询、窗口切分查询等，具体参考[TDengine 特色查询功能介绍](https://docs.taosdata.com/taos-sql/distinguished/)。通过使用这些特色查询，当 BI 工具将 SQL 查询发送到 TDengine 数据库时，可以大大提高数据访问速度，减少网络传输带宽。

在 BI 工具中，您可以创建“参数”并在 SQL 语句中使用，通过手动、定时的方式动态执行这些 SQL 语句，即可实现可视化报告的刷新效果。如下 SQL 语句：

```sql
select _wstart ws, count(*) cnt from supertable where tbname=?{metric} and ts >= ?{from} and ts < ?{to} interval(?{interval})
```

可以从 TDengine 实时读取数据，其中：

- `_wstart`：表示时间窗口起始时间。
- `count(*)`：表示时间窗口内的聚合值。
-  `?{interval}`：表示在 SQL 语句中引入名称为 interval 的参数，当 BI 工具查询数据时，会给 interval 参数赋值，如果取值为 1m，则表示按照 1 分钟的时间窗口降采样数据。
- `?{metric}`：该参数用来指定查询的数据表名称，当在 BI 工具中把某个“下拉参数组件”的 ID 也设置为 metric 时，该“下拉参数组件”的被选择项将会和该参数绑定在一起，实现动态选择的效果。
- `?{from}` 和 `?{to}`：这两个参数用来表示查询数据集的时间范围，可以与“文本参数组件”绑定。

可以在 BI 工具的“编辑参数”对话框中修改“参数”的数据类型、数据范围、默认取值，并在“可视化报告”中动态设置这些参数的值。

### 制作可视化报告

1. 在永洪 BI 工具中点击“制作报告”，创建画布。
2. 拖动可视化组件到画布中，例如“表格组件”。
3. 在“数据集”侧边栏中选择待绑定的数据集，将数据列中的“维度”和“度量”按需绑定到“表格组件”。
4. 点击“保存”后，即可查看报告。
5. 更多有关永洪 BI 工具的信息，请查询其[帮助文档](https://www.yonghongtech.com/help/Z-Suite/10.0/ch/)。