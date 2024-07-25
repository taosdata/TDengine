---
title: 与第三方集成
sidebar_label: 与第三方集成
toc_max_heading_level: 4
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

TDengine是一个高性能、可扩展的时序数据库，它支持SQL查询语言，兼容标准的JDBC和ODBC接口，同时还支持RESTful接口。这使得TDengine能够轻松地与各种商业智能软件和工具集成，从而构建一个开放的技术生态系统。本章将简要介绍TDengine 如何与一些流行的第三方工具进行集成。

## Grafana

Grafana是一个广受欢迎的开源可视化和分析工具，旨在帮助用户轻松地查询、可视化和监控存储在不同位置的指标、日志和跟踪信息。通过Grafana，用户可以将时序数据库中的数据以直观的图表形式展示出来，实现数据的可视化分析。

为了方便用户在Grafana中集成TDengine并展示时序数据，TDengine团队提供了一个名为TDengine Datasource的Grafana插件。通过安装此插件，用户无须编写任何代码，即可实现TDengine与Grafana的快速集成。这使得用户能够轻松地将存储在TDengine 中的时序数据以可视化的方式呈现在Grafana的仪表盘中，从而更好地监控和分析数据库性能。

### 前置条件

要让 Grafana 能正常添加 TDengine 数据源，需要以下几方面的准备工作。
1. TDengine 集群已经部署并正常运行；
2. taosAdapter 已经安装并正常运行；
3. Grafana 7.5.0 及以上的版本已经安装并正常运行。

记录以下信息：
1. taosAdapter 的 REST API 地址，例如：http://www.example.com:6041
2. TDengine 集群的认证信息，包括用户名和密码，默认为root/taosdata

### 安装 TDengine Datasource 插件

TDengine Datasource插件安装方式如下。
第1步，在Web浏览器打开Grafana页面，点击页面左上角的3个横条图标，然后点击Connections按钮。
第2步，在弹出页面的搜索栏内搜索TDengine，选择TDengine Datasource。
第3步，点击Install按钮以安装TDengine插件。
安装完成后，进入插件的详情页面，点击右上角的Add new data source按钮，即可进入数据源添加页面。
进入数据源添加页面后，输入RESTful接口地址和TDengine集群的认证信息后，点击Save & test按钮，即可完成TDengine数据源的添加，如下图所示：

![添加 TDengine 数据源](./addsource.png)

### 创建 Dashboard

添加TDengine数据源后，直接点击界面右上角的Build a dashboard按钮，即可创建 Dashboard。在Input Sql文本栏中输入TDengine的查询语句，便可以轻松实现监控指标的可视化，如下图所示：

![Dashboard](./dashboard.png)

在该示例中，查询的是由taosBenchmark --start-timestamp=1704802806146 --database= power --time-step=1000 -y写入的数据，数据库的名称为power，查询的是超级表meters 中记录的voltage，具体的SQL如下。
```sql
select _wstart, last(voltage) from power.meters where ts>=$from and ts<=$to interval(1m)
```

其中，from、to为内置变量，表示从TDengine Datasource插件Dashboard获取的查询范围和时间间隔。Grafana还提供了其他内置全局变量如 `interval、org、user、timeFilter` 等，具体可以参阅Grafana的官方文档的Variables部分。可以通过group by或partition by列名来实现分组展示数据。假设要按照不同的 `groupId` 来分组展示voltage（为了避免线条太多，只查询3个分组），SQL如下。
```sql
select _wstart, groupid, last(voltage) from power.meters where groupid < 4 and ts>=$from and ts<=$to partition by groupid interval(1m) fill(null);
```

然后在上图所示的 Group By Column 文本框中配置要分组的列，此处填 `groupId`。将Group By Fromat设置为 `groupId-{{groupId}}`，展示的legend名字为格式化的列名。配置好后，可如下图所示看到多条曲线。

![按照不同的groupId来分组展示voltage](./dashboard2.png)

在编写和调试SQL时，可以使用Grafana提供的Query Inspector，通过它可以看到SQL的执行结果，十分方便。

为了简化通过Grafana对TDengine实例的运行进行检测，了解它的健康状态，TDengine还提供了一个Grafana Dashboard: TDinsight for 3.x，可在Grafana的官网直接按照此名称搜索并获取。通过Grafana导入后，即可直接使用，省去了用户自己创建Dashboard的麻烦。

## Looker Studio

Looker Studio，作为Google旗下的一个功能强大的报表和商业智能工具，前身名为Google Data Studio。在2022年的Google Cloud Next大会上，Google将其更名为Looker Studio。这个工具凭借其丰富的数据可视化选项和多样化的数据连接能力，为用户提供了便捷的数据报表生成体验。用户可以根据预设的模板轻松创建数据报表，满足各种数据分析需求。

由于其简单易用的操作界面和庞大的生态系统支持，Looker Studio在数据分析领域受到众多数据科学家和专业人士的青睐。无论是初学者还是资深分析师，都可以利用Looker Studio快速构建美观且实用的数据报表，从而更好地洞察业务趋势、优化决策过程并提升整体运营效率。

### 获取

目前，TDengine连接器作为Looker Studio的合作伙伴连接器（partner connector），已在Looker Studio官网上线。用户访问Looker Studio的Data Source列表时，只须输入 “TDengine”进行搜索，便可轻松找到并立即使用TDengine连接器。

TDengine连接器兼容TDengine Cloud和TDengine Server两种类型的数据源。TDengine Cloud是涛思数据推出的全托管物联网和工业互联网大数据云服务平台，为用户提供一站式数据存储、处理和分析解决方案；而TDengine Server则是用户自行部署的本地版本，支持通过公网访问。以下内容将以TDengine Cloud为例进行介绍。

### 使用

在Looker Studio中使用TDengine连接器的步骤如下。

第1步，进入TDengine连接器的详情页面后，在Data Source下拉列表中选择TDengine Cloud，然后点击Next按钮，即可进入数据源配置页面。在该页面中填写以下信息，然后点击Connect按钮。
   - URL和TDengine Cloud Token，可以从TDengine Cloud的实例列表中获取。
   - 数据库名称和超级表名称。
   - 查询数据的开始时间和结束时间。
第2步，Looker Studio会根据配置自动加载所配置的TDengine数据库下的超级表的字段和标签。
第3步，点击页面右上角的Explore按钮，即查看从TDengine数据库中加载的数据。
第4步，根据需求，利用Looker Studio提供的图表，进行数据可视化的配置。

**注意** 在第一次使用时，请根据页面提示，对Looker Studio的TDengine连接器进行访问授权。

## PowerBI

Power BI是由Microsoft提供的一种商业分析工具。通过配置使用ODBC连接器，Power BI可以快速访问TDengine的数据。用户可以将标签数据、原始时序数据或按时间聚合后的时序数据从TDengine导入到Power BI，制作报表或仪表盘，整个过程不需要任何代码编写过程。

### 前置条件

安装完成Power BI Desktop软件并运行（如未安装，请从其官方地址下载最新的Windows操作系统X64版本）。

### 安装 ODBC 驱动

从TDengine官网下载最新的Windows操作系统X64客户端驱动程序，并安装在运行Power BI的机器上。安装成功后可在“ODBC数据源（64位）”管理工具中看到 TAOS_ODBC_DSN”驱动程序。

### 配置ODBC数据源

配置ODBC数据源的操作步骤如下。

第1步，在Windows操作系统的开始菜单中搜索并打开“ODBC数据源（64位）”管理工具。
第2步，点击“用户DSN”选项卡→“添加”按钮，进入“创建新数据源”对话框。
第3步，选择想要添加的数据源后选择“TDengine”，点击“完成”按钮，进入TDengine ODBC数据源配置页面。填写如下必要信息。
  - DSN：数据源名称，必填，比如“MyTDengine”。
  - 连接类型：勾选“WebSocket”复选框。
  - 服务地址：输入“taos://127.0.0.1:6041”。
  - 数据库：表示需要连接的数据库，可选，比如“test”。
  - 用户名：输入用户名，如果不填，默认为“root”。
  - 密码：输入用户密码，如果不填，默认为“taosdata”。
第4步，点击“测试连接”按钮，测试连接情况，如果成功连接，则会提示“成功连接到taos://root:taosdata@127.0.0.1:6041”。
第5步，点击“确定”按钮，即可保存配置并退出。

### 导入TDengine数据到Power BI

将TDengine数据导入Power BI的操作步骤如下。

第1步，打开Power BI并登录后，点击“主页”→“获取数据”→“其他”→“ODBC”→“连接”，添加数据源。
第2步，选择刚才创建的数据源名称，比如“MyTDengine”，点击“确定”按钮。在弹出的“ODBC驱动程序”对话框中，在左侧导航栏中点击“默认或自定义”→“连接”按钮，即可连接到配置好的数据源。进入“导航器”后，可以浏览对应数据库的数据表并加载。
第3步，如果需要输入SQL，则可以点击“高级选项”选项卡，在展开的对话框中输入并加载数据。

为了充分发挥Power BI在分析TDengine中数据方面的优势，用户需要先理解维度、度量、窗口切分查询、数据切分查询、时序和相关性等核心概念，之后通过自定义的SQL导入数据。
- 维度：通常是分类（文本）数据，描述设备、测点、型号等类别信息。在TDengine的超级表中，使用标签列存储数据的维度信息，可以通过形如“select distinct tbname, tag1, tag2 from supertable”的SQL语法快速获得维度信息。
- 度量：可以用于进行计算的定量（数值）字段，常见计算有求和、取平均值和最小值等。如果测点的采集周期为1s，那么一年就有3000多万条记录，把这些数据全部导入Power BI会严重影响其执行效率。在TDengine中，用户可以使用数据切分查询、窗口切分查询等语法，结合与窗口相关的伪列，把降采样后的数据导入Power BI中，具体语法请参阅TDengine官方文档的特色查询功能部分。
- 窗口切分查询：比如温度传感器每秒采集一次数据，但须查询每隔10min的温度平均值，在这种场景下可以使用窗口子句来获得需要的降采样查询结果，对应的SQL形如`select tbname, _wstart date，avg(temperature) temp from table interval(10m)`，其中，_wstart是伪列，表示时间窗口起始时间，10m表示时间窗口的持续时间，avg(temperature)表示时间窗口内的聚合值。
- 数据切分查询：如果需要同时获取很多温度传感器的聚合数值，可对数据进行切分，然后在切分出的数据空间内进行一系列的计算，对应的SQL形如 `partition by part_list`。数据切分子句最常见的用法是在超级表查询中按标签将子表数据进行切分，将每个子表的数据独立出来，形成一条条独立的时间序列，方便针对各种时序场景的统计分析。
- 时序：在绘制曲线或者按照时间聚合数据时，通常需要引入日期表。日期表可以从Excel表格中导入，也可以在TDengine中执行SQL获取，例如 `select _wstart date, count(*) cnt from test.meters where ts between A and B interval(1d) fill(0)`，其中fill字句表示数据缺失情况下的填充模式，伪列_wstart则为要获取的日期列。
- 相关性：告诉数据之间如何关联，如度量和维度可以通过tbname列关联在一起，日期表和度量则可以通过date列关联，配合形成可视化报表。

### 智能电表样例

TDengine采用了一种独特的数据模型，以优化时序数据的存储和查询性能。该模型利用超级表作为模板，为每台设备创建一张独立的表。每张表在设计时考虑了高度的可扩展性，最多可包含4096个数据列和128个标签列。这种设计使得TDengine能够高效地处理大量时序数据，同时保持数据的灵活性和易用性。

以智能电表为例，假设每块电表每秒产生一条记录，那么每天将产生86 400条记录。对于1000块智能电表来说，每年产生的记录将占用大约600GB的存储空间。面对如此庞大的数据量，Power BI等商业智能工具在数据分析和可视化方面发挥着重要作用。

在Power BI中，用户可以将TDengine表中的标签列映射为维度列，以便对数据进行分组和筛选。同时，数据列的聚合结果可以导入为度量列，用于计算关键指标和生成报表。通过这种方式，Power BI能够帮助决策者快速获取所需的信息，深入了解业务运营情况，从而制定更加明智的决策。

根据如下步骤，便可以体验通过Power BI生成时序数据报表的功能。
第1步，使用TDengine的taosBenchMark快速生成1000块智能电表3天的数据，采集频率为1s。
    ```shell
    taosBenchmark-t1000-n259200-S1000-H200-y
    ```
第2步，导入维度数据。在Power BI中导入表的标签列，取名为tags，通过如下SQL获取超级表下所有智能电表的标签数据。
    ```sql
    selectdistincttbnamedevice,groupId,locationfromtest.meters
    ```
第3步，导入度量数据。在Power BI中，按照1小时的时间窗口，导入每块智能电表的电流均值、电压均值、相位均值，取名为data，SQL如下。
    ```sql
    第3步，导入度量数据。在Power BI中，按照1小时的时间窗口，导入每块智能电表的电流均值、电压均值、相位均值，取名为data，SQL如下。
    ```
第4步，导入日期数据。按照1天的时间窗口，获得时序数据的时间范围及数据计数，SQL如下。需要在Power Query编辑器中将date列的格式从“文本”转化为“日期”。
    ```sql
    select_wstartdate,count(*)fromtest.metersinterval(1d)havingcount(*)>0
    ```
第5步，建立维度和度量的关联关系。打开模型视图，建立表tags和data的关联关系，将tbname设置为关联数据列。
第6步，建立日期和度量的关联关系。打开模型视图，建立数据集date和data的关联关系，关联的数据列为date和datatime。
第7步，制作报告。在柱状图、饼图等控件中使用这些数据。

由于TDengine处理时序数据的超强性能，使得用户在数据导入及每日定期刷新数据时，都可以得到非常好的体验。更多有关Power BI视觉效果的构建方法，请参照Power BI的官方文档。

## 永洪 BI

永洪 BI 是一个专为各种规模企业打造的全业务链大数据分析解决方案，旨在帮助用户轻松发掘大数据价值，获取深入的洞察力。该平台以其灵活性和易用性而广受好评，无论企业规模大小，都能从中受益。

为了实现与 TDengine 的高效集成，永洪 BI 提供了 JDBC 连接器。用户只须按照简单的步骤配置数据源，即可将 TDengine 作为数据源添加到永洪BI中。这一过程不仅快速便捷，还能确保数据的准确性和稳定性。

一旦数据源配置完成，永洪BI便能直接从TDengine中读取数据，并利用其强大的数据处理和分析功能，为用户提供丰富的数据展示、分析和预测能力。这意味着用户无须编写复杂的代码或进行烦琐的数据转换工作，即可轻松获取所需的业务洞察。

### 安装永洪 BI

确保永洪 BI 已经安装并运行（如果未安装，请到永洪科技官方下载页面下载）。

### 安装JDBC驱动

从 maven.org 下载 TDengine JDBC 连接器文件 “taos-jdbcdriver-3.2.7-dist.jar”，并安装在永洪 BI 的机器上。

### 配置JDBC数据源

配置JDBC数据源的步骤如下。

第1步，在打开的永洪BI中点击“添加数据源”按钮，选择SQL数据源中的“GENERIC”类型。
第2步，点击“选择自定义驱动”按钮，在“驱动管理”对话框中点击“驱动列表”旁边的“+”，输入名称“MyTDengine”。然后点击“上传文件”按钮，上传刚刚下载的TDengine JDBC连接器文件“taos-jdbcdriver-3.2.7-dist.jar”，并选择“com.taosdata.jdbc.
rs.RestfulDriver”驱动，最后点击“确定”按钮，完成驱动添加步骤。
第3步，复制下面的内容到“URL”字段。
    ```text
    jdbc:TAOS-RS://127.0.0.1:6041?user=root&password=taosdata
    ```
第4步，在“认证方式”中点击“无身份认证”单选按钮。
第5步，在数据源的高级设置中修改“Quote 符号”的值为反引号（`）。
第6步，点击“测试连接”按钮，弹出“测试成功”对话框。点击“保存”按钮，输入“MyTDengine”来保存TDengine数据源。

### 创建TDengine数据集

创建TDengine数据集的步骤如下。

第1步，在永洪BI中点击“添加数据源”按钮，展开刚刚创建的数据源，并浏览TDengine中的超级表。
第2步，可以将超级表的数据全部加载到永洪BI中，也可以通过自定义SQL导入部分数据。
第3步，当勾选“数据库内计算”复选框时，永洪BI将不再缓存TDengine的时序数据，并在处理查询时将SQL请求发送给TDengine直接处理。

当导入数据后，永洪BI会自动将数值类型设置为“度量”列，将文本类型设置为“维度”列。而在TDengine的超级表中，由于将普通列作为数据的度量，将标签列作为数据的维度，因此用户可能需要在创建数据集时更改部分列的属性。TDengine在支持标准SQL的基础之上还提供了一系列满足时序业务场景需求的特色查询语法，例如数据切分查询、窗口切分查询等，具体操作步骤请参阅TDengine的官方文档。通过使用这些特色查询，当永洪BI将SQL查询发送到TDengine时，可以大大提高数据访问速度，减少网络传输带宽。

在永洪BI中，你可以创建“参数”并在SQL中使用，通过手动、定时的方式动态执行这些SQL，即可实现可视化报告的刷新效果。如下SQL可以从TDengine实时读取数据。
```sql
select _wstart ws, count(*) cnt from supertable where tbname=?{metric} and ts = ?{from} and ts < ?{to} interval(?{interval})
```

其中：
1. `_wstart`：表示时间窗口起始时间。
2. `count（*）`：表示时间窗口内的聚合值。
3. `?{interval}`：表示在 SQL 语句中引入名称为 `interval` 的参数，当 BI 工具查询数据时，会给 `interval` 参数赋值，如果取值为 1m，则表示按照 1 分钟的时间窗口降采样数据。
4. `?{metric}`：该参数用来指定查询的数据表名称，当在 BI 工具中把某个“下拉参数组件”的 ID 也设置为 metric 时，该“下拉参数组件”的被选择项将会和该参数绑定在一起，实现动态选择的效果。
5. `?{from}` 和 `？{to}`：这两个参数用来表示查询数据集的时间范围，可以与“文本参数组件”绑定。
您可以在 BI 工具的“编辑参数”对话框中修改“参数”的数据类型、数据范围、默认取值，并在“可视化报告”中动态设置这些参数的值。

### 21.4.5 制作可视化报告

制作可视化报告的步骤如下。

1. 在永洪 BI 工具中点击“制作报告”，创建画布。
2. 拖动可视化组件到画布中，例如“表格组件”。
3. 在“数据集”侧边栏中选择待绑定的数据集，将数据列中的“维度”和“度量”按需绑定到“表格组件”。
4. 点击“保存”后，即可查看报告。
5. 更多有关永洪 BI 工具的信息，请查询永洪科技官方帮助文档。

