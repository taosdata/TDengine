---
title: 与 PowerBI 集成
sidebar_label: PowerBI
toc_max_heading_level: 4
---

Power BI 是由 Microsoft 提供的一种商业分析工具。通过配置使用 ODBC 连接器，Power BI 可以快速访问 TDengine 的数据。用户可以将标签数据、原始时序数据或按时间聚合后的时序数据从 TDengine 导入到 Power BI，制作报表或仪表盘，整个过程不需要任何代码编写过程。

## 前置条件

准备以下环境：
- TDengine 3.3.4.0 以上版本集群已部署并正常运行（企业及社区版均可）。
- taosAdapter 能够正常运行，详细参考 [taosAdapter 参考手册](../../../reference/components/taosadapter)。
- 从 TDengine 官网下载最新的 Windows 操作系统 X64 客户端驱动程序并进行安装，详细参考 [安装 ODBC 驱动](../../../reference/connector/odbc/#安装)。
- 安装完成 Power BI Desktop 软件并运行（如未安装，请从其官方地址下载最新的 Windows 操作系统 32/64 位版本）。

## 配置数据源

**第 1 步**，在 Windows 操作系统的开始菜单中搜索并打开【ODBC 数据源（64 位）】管理工具并进行配置。详细参考 [配置 ODBC 数据源](../../../reference/connector/odbc/#配置数据源)。

**第 2 步**，打开 Power BI 并登录后，点击【主页】->【获取数据】->【其他】->【ODBC】->【连接】，添加数据源。  

**第 3 步**，选择刚才创建的数据源名称，比如【MyTDengine】，如果需要输入 SQL，则可以点击【高级选项】选项卡，在展开的对话框的编辑框中输入 SQL 语句。点击【确定】按钮，即可连接到配置好的数据源。  

**第 4 步**，进入【导航器】后，可以浏览对应数据库的数据表/视图并加载数据。

## 数据分析

### 使用说明

为了充分发挥 Power BI 在分析 TDengine 中 数据方面的优势，用户需要先理解维度、度量、窗口切分查询、数据切分查询、时序和相关性等核心概念，之后通过自定义的 SQL 导入数据。
- 维度：通常是分类（文本）数据，描述设备、测点、型号等类别信息。在 TDengine 的超级表中，使用标签列存储数据的维度信息，可以通过形如 `select distinct tbname, tag1, tag2 from supertable` 的 SQL 语法快速获得维度信息。
- 度量：可以用于进行计算的定量（数值）字段，常见计算有求和、取平均值和最小值等。如果测点的采集周期为 1s，那么一年就有 3000 多万条记录，把这些数据全部导入 Power BI 会严重影响其执行效率。在 TDengine 中，用户可以使用数据切分查询、窗口切分查询等语法，结合与窗口相关的伪列，把降采样后的数据导入 Power BI 中，具体语法请参阅 TDengine 官方文档的特色查询功能部分。
- 窗口切分查询：比如温度传感器每秒采集一次数据，但须查询每隔 10min 的温度平均值，在这种场景下可以使用窗口子句来获得需要的降采样查询结果，对应的 SQL 形如 `select tbname, _wstart date，avg(temperature) temp from table interval(10m)`，其中，`_wstart` 是伪列，表示时间窗口起始时间，10m 表示时间窗口的持续时间，`avg(temperature)` 表示时间窗口内的聚合值。
- 数据切分查询：如果需要同时获取很多温度传感器的聚合数值，可对数据进行切分，然后在切分出的数据空间内进行一系列的计算，对应的 SQL 形如 `partition by part_list`。数据切分子句最常见的用法是在超级表查询中按标签将子表数据进行切分，将每个子表的数据独立出来，形成一条条独立的时间序列，方便针对各种时序场景的统计分析。
- 时序：在绘制曲线或者按照时间聚合数据时，通常需要引入日期表。日期表可以从 Excel 表格中导入，也可以在 TDengine 中执行 SQL 获取，例如 `select _wstart date, count(*) cnt from test.meters where ts between A and B interval(1d) fill(0)`，其中 fill 字句表示数据缺失情况下的填充模式，伪列 _wstart 则为要获取的日期列。
- 相关性：告诉数据之间如何关联，如度量和维度可以通过 tbname 列关联在一起，日期表和度量则可以通过 date 列关联，配合形成可视化报表。

### 智能电表样例

TDengine 采用了一种独特的数据模型，以优化时序数据的存储和查询性能。该模型利用超级表作为模板，为每台设备创建一张独立的表。每张表在设计时考虑了高度的可扩展性，最多可包含 4096 个数据列和 128 个标签列。这种设计使得 TDengine 能够高效地处理大量时序数据，同时保持数据的灵活性和易用性。

以智能电表为例，假设每块电表每秒产生一条记录，那么每天将产生 86400 条记录。对于 1000 块智能电表来说，每年产生的记录将占用大约 600GB 的存储空间。面对如此庞大的数据量，Power BI 等商业智能工具在数据分析和可视化方面发挥着重要作用。

在 Power BI 中，用户可以将 TDengine 表中的标签列映射为维度列，以便对数据进行分组和筛选。同时，数据列的聚合结果可以导入为度量列，用于计算关键指标和生成报表。通过这种方式，Power BI 能够帮助决策者快速获取所需的信息，深入了解业务运营情况，从而制定更加明智的决策。

根据如下步骤，便可以体验通过 Power BI 生成时序数据报表的功能。

**第 1 步**，使用 TDengine 的 taosBenchMark 快速生成 1000 块智能电表 3 天的数据，采集频率为 1s。

```shell
taosBenchmark -t 1000 -n 259200 -S 1000 -y
```

**第 2 步**，导入维度数据。在 Power BI 中导入表的标签列，取名为 tags，通过如下 SQL 获取超级表下所有智能电表的标签数据。

```sql
select distinct tbname device, groupId, location from test.meters
```

**第 3 步**，导入度量数据。在 Power BI 中，按照 1 小时的时间窗口，导入每块智能电表的电流均值、电压均值、相位均值，取名为 data，SQL 如下。

```sql
select tbname, _wstart ws, avg(current), avg(voltage), avg(phase) from test.meters PARTITION by tbname interval(1h)
```

**第 4 步**，导入日期数据。按照 1 天的时间窗口，获得时序数据的时间范围及数据计数，SQL 如下。需要在 Power Query 编辑器中将 date 列的格式从“文本”转化为“日期”。

```sql
select _wstart date, count(*) from test.meters interval(1d) having count(*)>0
```

**第 5 步**，建立维度和度量的关联关系。打开模型视图，建立表 tags 和 data 的关联关系，将 tbname 设置为关联数据列。

**第 6 步**，建立日期和度量的关联关系。打开模型视图，建立数据集 date 和 data 的关联关系，关联的数据列为 date 和 datetime。

**第 7 步**，制作报告。在柱状图、饼图等控件中使用这些数据。  

由于 TDengine 处理时序数据的超强性能，使得用户在数据导入及每日定期刷新数据时，都可以得到非常好的体验。更多有关 Power BI 视觉效果的构建方法，请参照 Power BI 的官方文档。
