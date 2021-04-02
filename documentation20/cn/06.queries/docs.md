
# 高效查询数据

## <a class="anchor" id="queries"></a>主要查询功能

TDengine 采用 SQL 作为查询语言。应用程序可以通过 C/C++, Java, Go, Python 连接器发送 SQL 语句，用户可以通过 TDengine 提供的命令行（Command Line Interface, CLI）工具 TAOS Shell 手动执行 SQL 即席查询（Ad-Hoc Query）。TDengine 支持如下查询功能：

- 单列、多列数据查询
- 标签和数值的多种过滤条件：>, <,  =, <>, like 等
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

具体的查询语法请看 [TAOS SQL 的数据查询](https://www.taosdata.com/cn/documentation/taos-sql#select) 章节。

## <a class="anchor" id="aggregation"></a>多表聚合查询

物联网场景中，往往同一个类型的数据采集点有多个。TDengine采用超级表(STable)的概念来描述某一个类型的数据采集点，一张普通的表来描述一个具体的数据采集点。同时TDengine使用标签来描述数据采集点的静态属性，一个具体的数据采集点有具体的标签值。通过指定标签的过滤条件，TDengine提供了一高效的方法将超级表(某一类型的数据采集点)所属的子表进行聚合查询。对普通表的聚合函数以及绝大部分操作都适用于超级表，语法完全一样。  

**示例1**：在TAOS Shell，查找北京所有智能电表采集的电压平均值，并按照location分组
```mysql
taos> SELECT AVG(voltage) FROM meters GROUP BY location;
       avg(voltage)        |            location            |
=============================================================
             222.000000000 | Beijing.Haidian                |
             219.200000000 | Beijing.Chaoyang               |
Query OK, 2 row(s) in set (0.002136s)
```

**示例2**：在TAOS shell, 查找groupId为2的所有智能电表过去24小时的记录条数，电流的最大值

```mysql
taos> SELECT count(*), max(current) FROM meters where groupId = 2 and ts > now - 24h;
     cunt(*)  |    max(current)  |
==================================
            5 |             13.4 |
Query OK, 1 row(s) in set (0.002136s)
```

TDengine仅容许对属于同一个超级表的表之间进行聚合查询，不同超级表之间的聚合查询不支持。在 [TAOS SQL 的数据查询](https://www.taosdata.com/cn/documentation/taos-sql#select) 一章，查询类操作都会注明是否支持超级表。

## <a class="anchor" id="sampling"></a>降采样查询、插值

物联网场景里，经常需要通过降采样（down sampling）将采集的数据按时间段进行聚合。TDengine 提供了一个简便的关键词 interval 让按照时间窗口的查询操作变得极为简单。比如，将智能电表 d1001 采集的电流值每10秒钟求和
```mysql
taos> SELECT sum(current) FROM d1001 INTERVAL(10s);
           ts            |       sum(current)        |
======================================================
 2018-10-03 14:38:00.000 |              10.300000191 |
 2018-10-03 14:38:10.000 |              24.900000572 |
Query OK, 2 row(s) in set (0.000883s)
```
降采样操作也适用于超级表，比如：将北京所有智能电表采集的电流值每秒钟求和
```mysql
taos> SELECT SUM(current) FROM meters where location like "Beijing%" INTERVAL(1s);
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

语法规则细节请见 [TAOS SQL 的时间维度聚合](https://www.taosdata.com/cn/documentation/taos-sql#aggregation) 章节。

