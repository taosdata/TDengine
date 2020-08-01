



# 高效查询数据

## 主要查询功能

TDengine 采用 SQL 作为查询语言。应用程序可以通过 C/C++, Java, Go, Python 连接器发送 SQL 语句，用户可以通过 TDengine 提供的命令行（Command Line Interface, CLI）工具 TAOS Shell 手动执行 SQL 即席查询（Ad-Hoc Query）。TDengine 支持如下查询功能：

- 单列、多列数据查询
- 标签和数值的多种过滤条件：\>, \<,  =, \<>, like 等
- 聚合结果的分组（Group by）、排序（Order by）、约束输出（Limit/Offset）
- 数值列及聚合结果的四则运算
- 时间戳对齐的连接查询（Join Query）操作
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

TDengine对每个数据采集点单独建表，但在实际应用中经常需要对不同的采集点数据进行聚合。为高效的进行聚合操作，TDengine引入超级表（STable）的概念。超级表用来代表一特定类型的数据采集点，它是包含多张表的表集合，集合里每张表的模式（schema）完全一致，但每张表都带有自己的静态标签，标签可以多个，可以随时增加、删除和修改。

应用可通过指定标签的过滤条件，对一个STable下的全部或部分表进行聚合或统计操作，这样大大简化应用的开发。其具体流程如下图所示：

<center> <img src="../assets/stable.png"> </center>

<center> 多表聚合查询原理图  </center>

1：应用将一个查询条件发往系统；2: taosc将超级表的名字发往 Meta Node（管理节点)；3：管理节点将超级表所拥有的 vnode 列表发回 taosc；4：taosc将计算的请求连同标签过滤条件发往这些vnode对应的多个数据节点；5：每个vnode先在内存里查找出自己节点里符合标签过滤条件的表的集合，然后扫描存储的时序数据，完成相应的聚合计算，将结果返回给taosc；6：taosc将多个数据节点返回的结果做最后的聚合，将其返回给应用。

由于TDengine在vnode内将标签数据与时序数据分离存储，通过先在内存里过滤标签数据，将需要扫描的数据集大幅减少，大幅提升聚合计算速度。同时，由于数据分布在多个vnode/dnode，聚合计算操作在多个vnode里并发进行，又进一步提升了聚合的速度。

对普通表的聚合函数以及绝大部分操作都适用于超级表，语法完全一样，细节请看 TAOS SQL。

比如：在TAOS Shell，查找所有智能电表采集的电压平均值，并按照location分组

```mysql
taos> SELECT AVG(voltage) FROM meters GROUP BY location;
       avg(voltage)        |            location            |
=============================================================
             222.000000000 | Beijing.Haidian                |
             219.200000000 | Beijing.Chaoyang               |
Query OK, 2 row(s) in set (0.002136s)
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

物联网场景里，每个数据采集点采集数据的时间是难同步的，但很多分析算法(比如FFT)需要把采集的数据严格按照时间等间隔的对齐，在很多系统里，需要应用自己写程序来处理，但使用TDengine的降采样操作就轻松解决。如果一个时间间隔里，没有采集的数据，TDengine还提供插值计算的功能。

语法规则细节请见<a href="https://www.taosdata.com/cn/documentation20/taos-sql/">TAOS SQL </a>。

