



# 高效查询数据

## 主要查询功能

TDengine 采用 SQL 作为查询语言。应用程序可以通过 C/C++, Java, Go, Python 连接器发送 SQL 语句，用户可以通过 TDengine 提供的命令行（Command Line Interface, CLI）工具 TAOS Shell 手动执行 SQL 即席查询（Ad-Hoc Query）。TDengine 支持如下查询功能：

- 单列、多列数据查询
- 标签和数值的多种过滤条件：\>, \<,  =, \<>, like 等
- 聚合结果的分组（Group by）、排序（Order by）、约束输出（Limit/Offset）
- 数值列及聚合结果的四则运算
- 时间戳对齐的连接查询（Join Query: 隐式连接）操作
- 多种聚合/计算函数: count, max, min, avg, sum, twa, stddev, leastsquares, top, bottom, first, last, percentile, apercentile, last_row, spread, diff等

例如：在TAOS Shell中，从表d1001中查询出vlotage > 215的记录，按时间降序排列，仅仅输出2条。
```mysql
taos> select * from d1001 where voltage > 215 order by ts desc limit 2;
           ts            |       current        |   voltage   |        phase         |
======================================================================================
 2018-10-03 14:38:16.800 |             12.30000 |         221 |              0.31000 |
 2018-10-03 14:38:15.000 |             12.60000 |         218 |              0.33000 |
Query OK, 2 row(s) in set (0.001100s)
```
为满足物联网场景的需求，TDengine支持几个特殊的函数，比如twa(时间加权平均)，spread (最大值与最小值的差)，last_row(最后一条记录)等，更多与物联网场景相关的函数将添加进来。TDengine还支持连续查询。

具体的查询语法请看<a href="https://www.taosdata.com/cn/documentation20/taos-sql/">TAOS SQL </a>。

## 多表聚合查询
以温度传感器采集时序数据作为例，示范Stable超级表多表聚合查询的使用。

在这个例子中，对每个温度计都会建立一张表，表名为温度计的ID，温度计读数的时刻记为ts，采集的值记为degree。通过tags给每个采集器打上不同的标签，其中记录温度计的地区和类型，以方便我们后面的查询。所有温度计的采集量都一样，因此我们用STable来定义表结构。

**定义STable表结构并使用它创建子表**
创建STable语句如下：
```mysql
	CREATE TABLE thermometer (ts timestamp, degree double) 
	TAGS(location binary(20), type int)
```
假设有北京，天津和上海三个地区的采集器共4个，温度采集器有3种类型，我们就可以对每个采集器建表如下：
```mysql
	CREATE TABLE therm1 USING thermometer TAGS (’beijing’, 1);
	CREATE TABLE therm2 USING thermometer TAGS (’beijing’, 2);
	CREATE TABLE therm3 USING thermometer TAGS (’tianjin’, 1);
	CREATE TABLE therm4 USING thermometer TAGS (’shanghai’, 3);
```
其中therm1，therm2，therm3，therm4是超级表thermometer四个具体的子表，也即普通的Table。以therm1为例，它表示采集器therm1的数据，表结构完全由thermometer定义，标签location=”beijing”, type=1表示therm1的地区是北京，类型是第1类的温度计。

**写入数据**
注意，写入数据时不能直接对STable操作，而是要对每张子表进行操作。我们分别向四张表therm1，therm2， therm3， therm4写入一条数据，写入语句如下：
```mysql
	INSERT INTO therm1 VALUES (’2018-01-01 00:00:00.000’, 20);
	INSERT INTO therm2 VALUES (’2018-01-01 00:00:00.000’, 21);
	INSERT INTO therm3 VALUES (’2018-01-01 00:00:00.000’, 24);
	INSERT INTO therm4 VALUES (’2018-01-01 00:00:00.000’, 23);
```
**按标签聚合查询**
查询位于北京(beijing)地区的型号为1的温度传感器采样值的数量count(*)、平均温度avg(degree)、最高温度max(degree)、最低温度min(degree)，并将结果按所处地域(location)和传感器类型(type)进行聚合。
```mysql
 	SELECT COUNT(*), AVG(degree), MAX(degree), MIN(degree)
 	FROM thermometer
 	WHERE location=’beijing’ and type = 1
 	GROUP BY location
```
**按时间周期聚合查询**
查询仅位于北京以外地区的温度传感器最近24小时(24h)采样值的数量count(*)、平均温度avg(degree)、最高温度max(degree)和最低温度min(degree)，将采集结果按照10分钟为周期进行聚合，并将结果按所处地域(location)和传感器类型(type)再次进行聚合。
```mysql
	SELECT COUNT(*), AVG(degree), MAX(degree), MIN(degree)
	FROM thermometer
	WHERE name<>’beijing’ and ts>=now-1d
	INTERVAL(10M)
	GROUP BY location, type
```

## 降采样查询、插值

物联网场景里，经常需要通过降采样（down sampling）将采集的数据按时间段进行聚合。TDengine 提供了一个简便的关键词 interval 让按照时间窗口的查询操作变得极为简单。比如，将智能电表 d1001 采集的电流值每10秒钟求和
```mysql
taos> SELECT sum(current) FROM d1001 INTERVAL(10s);
           ts            |       sum(current)        |
======================================================
 2018-10-03 14:38:00.000 |              10.300000191 |
 2018-10-03 14:38:10.000 |              24.900000572 |
Query OK, 2 row(s) in set (0.000883s)
```
降采样操作也适用于超级表，比如：将所有智能电表采集的电流值每秒钟求和
```mysql
taos> SELECT SUM(current) FROM meters INTERVAL(1s);
           ts            |       sum(current)        |
======================================================
 2018-10-03 14:38:04.000 |              10.199999809 |
 2018-10-03 14:38:05.000 |              32.900000572 |
 2018-10-03 14:38:06.000 |              11.500000000 |
 2018-10-03 14:38:15.000 |              12.600000381 |
 2018-10-03 14:38:16.000 |              36.000000000 |
Query OK, 5 row(s) in set (0.001538s)
```
降采样操作也支持时间偏移，比如：将所有智能电表采集的电流值每秒钟求和，但要求每个时间窗口从 500 毫秒开始
```mysql
taos> SELECT SUM(current) FROM meters INTERVAL(1s, 500a);
           ts            |       sum(current)        |
======================================================
 2018-10-03 14:38:04.500 |              11.189999809 |
 2018-10-03 14:38:05.500 |              31.900000572 |
 2018-10-03 14:38:06.500 |              11.600000000 |
 2018-10-03 14:38:15.500 |              12.300000381 |
 2018-10-03 14:38:16.500 |              35.000000000 |
Query OK, 5 row(s) in set (0.001521s)
```

物联网场景里，每个数据采集点采集数据的时间是难同步的，但很多分析算法(比如FFT)需要把采集的数据严格按照时间等间隔的对齐，在很多系统里，需要应用自己写程序来处理，但使用TDengine的降采样操作就轻松解决。如果一个时间间隔里，没有采集的数据，TDengine还提供插值计算的功能。

语法规则细节请见<a href="https://www.taosdata.com/cn/documentation20/taos-sql/">TAOS SQL </a>。

