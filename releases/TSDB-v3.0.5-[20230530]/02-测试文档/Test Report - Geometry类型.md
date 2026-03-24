# Test Report - Geometry类型

**概述**：
空间数据表示有关物理位置和几何对象形状的信息。 这些对象可能是点位置或更复杂的对象，例如国家/地区/区域、道路或湖泊。当前支持的空间类型名为"geometry", 声明类型需要指定长度，如geometry（20），其中支持写入的子类型为point、linestring、polygon
Point - Geography 数据类型的 Point 类型表示单个位置，其中 *Lat* 表示纬度， *Long* 表示经度。
Linestring - Geography 数据类型的LineString 是一个一维对象，表示一系列点和连接这些点的线段。
Polygon - Geography 数据类型的Polygon是存储为一系列点的二维表面，这些点定义一个外部边界环和零个或多个内部环。
**环境信息：**
192.168.1.61：
CPU: Intel(R) Xeon(R) CPU E5-2650 v3 @ 2.30GHz （2）10核
Mem：DDR4 16GB * 16
Disk:  893GB
192.168.1.35：
CPU: Intel(R) Xeon(R) CPU E5-2630 v2 @ 2.60GHz （2）6核
Mem: DDR3  32 GB * 2
Disk: 2792GB
102.168.1.63：
CPU: Intel(R) Xeon(R) CPU E5-2650 v3 @ 2.30GHz（2）10核
Mem: DDR4 16GB* 16
Disk: 895GB
**测试用例：**
<view type="1">

  > ⚠ 嵌入文件，需在飞书中查看 (token: SPaJbv2dWojG5axoHO2c7vXNnIe)

</view>


**测试结果：**

|  | 结果描述 | Limitation、Bug、待优化项 |
| --- | --- | --- |
| 写入 | 1. Point， Linestring， Polygon三种类型正常值写入成功，column长度设置无效（[TD-24554](https://jira.taosdata.com:18080/browse/TD-24554) ） 1. Point， Linestring， Polygon NULL值写入成功 1. Point， Linestring， Polygon 向Tag列写入成功 1. 在单副本、多副本下写入以上数据成功 | 1. 【Limitation】Schemaless写入暂不支持 [TD-24559](https://jira.taosdata.com:18080/browse/TD-24559) 1. 【Limitation】三种类型中的值边界暂不支持检查 [TD-24591](https://jira.taosdata.com:18080/browse/TD-24591) 1. 【Bug】[TD-24554](https://jira.taosdata.com:18080/browse/TD-24554) - [Geometry类型在column中的长度设置无效](https://jira.taosdata.com:18080/browse/TD-24554) 1. 【优化项】[TD-24476](https://jira.taosdata.com:18080/browse/TD-24476) - [GEOMETRY类型 写入三个或四个值时，自动获取前两个，无exception报错](https://jira.taosdata.com:18080/browse/TD-24476) 1. 【优化项】[TD-24556](https://jira.taosdata.com:18080/browse/TD-24556) - [Geometry类型减小列长度的报错提示中应加入"Geometry"](https://jira.taosdata.com:18080/browse/TD-24556) |
| 查询 | 1. 对Point， Linestring， Polygon三种类型单列、行数据查询成功， Tag查询失败（[TD-24542](https://jira.taosdata.com:18080/browse/TD-24542) ），NULL值查询失败（[TD-24473](https://jira.taosdata.com:18080/browse/TD-24473)） 1. 使用distinct关键查询， 查询失败（[TD-24484](https://jira.taosdata.com:18080/browse/TD-24484) ） 1. 表、超级表join查询成功 1. 窗口函数查询成功 1. 函数查询目前支持的函数包括 聚合：count 选择：last, first, last_row, mode, tail, unique 1. show 命令查询，查询成功，内容显示失败（[TD-24474](https://jira.taosdata.com:18080/browse/TD-24474) [TD-24482](https://jira.taosdata.com:18080/browse/TD-24482)） | 1. 【Bug】[TD-24484](https://jira.taosdata.com:18080/browse/TD-24484) - [distinct 查询GEOMETRY 列发生coredump](https://jira.taosdata.com:18080/browse/TD-24484) 1. 【Bug】[TD-24542](https://jira.taosdata.com:18080/browse/TD-24542) - [GEOMETRY类型写入tag后，查询报错"ParseException: Unknown WKB type 991"](https://jira.taosdata.com:18080/browse/TD-24542) 1. 【Bug】[TD-24474](https://jira.taosdata.com:18080/browse/TD-24474) - [show create stable xxx 显示GEOMETRY类型为null](https://jira.taosdata.com:18080/browse/TD-24474) 1. 【Bug】[TD-24473](https://jira.taosdata.com:18080/browse/TD-24473) - [GEOMETRY类型值为null，出现coredump](https://jira.taosdata.com:18080/browse/TD-24473) 1. 【Bug】[TD-24482](https://jira.taosdata.com:18080/browse/TD-24482) - [带有GEOMETRY类型的超级表，执行desc xxxx 发生crash](https://jira.taosdata.com:18080/browse/TD-24482) |

**测试总结：**
1. Geometry下的三种类型支持列、Tag的普通写入，但还有一些细节问题待处理。如Point的经纬度值边界检查、Point的多个值写入不报错、Column中长度设置无效等
2. Schemaless写入暂不支持，待添加
3. 查询中还有Tag结果显示问题，一些情况下的crash或coredump问题
4. Geometry做为一种独立的数据类型，还需要一些特有函数才能真正被客户使用起来，未来可以按需规划
