# Efficient Data Querying

## <a class="anchor" id="queries"></a> Main Query Features

TDengine uses SQL as the query language. Applications can send SQL statements through C/C++, Java, Go, Python connectors, and users can manually execute SQL Ad-Hoc Query through the Command Line Interface (CLI) tool TAOS Shell provided by TDengine. TDengine supports the following query functions:

- Single-column and multi-column data query
- Multiple filters for tags and numeric values: >, <, =, < >, like, etc
- Group by, Order by, Limit/Offset of aggregation results
- Four operations for numeric columns and aggregation results
- Time stamp aligned join query (implicit join) operations
- Multiple aggregation/calculation functions: count, max, min, avg, sum, twa, stddev, leastsquares, top, bottom, first, last, percentile, apercentile, last_row, spread, diff, etc

For example, in TAOS shell, the records with vlotage > 215 are queried from table d1001, sorted in descending order by timestamps, and only two records are outputted.

```mysql
taos> select * from d1001 where voltage > 215 order by ts desc limit 2;
           ts            |       current        |   voltage   |        phase         |
======================================================================================
 2018-10-03 14:38:16.800 |             12.30000 |         221 |              0.31000 |
 2018-10-03 14:38:15.000 |             12.60000 |         218 |              0.33000 |
Query OK, 2 row(s) in set (0.001100s)
```

In order to meet the needs of an IoT scenario, TDengine supports several special functions, such as twa (time weighted average), spread (difference between maximum and minimum), last_row (last record), etc. More functions related to IoT scenarios will be added. TDengine also supports continuous queries.

For specific query syntax, please see the [Data Query section of TAOS SQL](https://www.taosdata.com/cn/documentation/taos-sql#select).

## <a class="anchor" id="aggregation"></a> Multi-table Aggregation Query

In an IoT scenario, there are often multiple data collection points in a same type. TDengine uses the concept of STable to describe a certain type of data collection point, and an ordinary table to describe a specific data collection point. At the same time, TDengine uses tags to describe the statical attributes of data collection points. A given data collection point has a specific tag value. By specifying the filters of tags, TDengine provides an efficient method to aggregate and query the sub-tables of STables (data collection points of a certain type). Aggregation functions and most operations on ordinary tables are applicable to STables, and the syntax is exactly the same.

**Example 1**: In TAOS Shell, look up the average voltages collected by all smart meters in Beijing and group them by location

```mysql
taos> SELECT AVG(voltage) FROM meters GROUP BY location;
       avg(voltage)        |            location            |
=============================================================
             222.000000000 | Beijing.Haidian                |
             219.200000000 | Beijing.Chaoyang               |
Query OK, 2 row(s) in set (0.002136s)
```

**Example 2**: In TAOS Shell, look up the number of records with groupId 2 in the past 24 hours, check the maximum current of all smart meters

```mysql
taos> SELECT count(*), max(current) FROM meters where groupId = 2 and ts > now - 24h;
     cunt(*)  |    max(current)  |
==================================
            5 |             13.4 |
Query OK, 1 row(s) in set (0.002136s)
```

TDengine only allows aggregation queries between tables belonging to a same STable, means aggregation queries between different STables are not supported. In the Data Query section of TAOS SQL, query class operations will all be indicated that whether STables are supported.

## <a class="anchor" id="sampling"></a> Down Sampling Query, Interpolation

In a scenario of IoT, it is often necessary to aggregate the collected data by intervals through down sampling. TDengine provides a simple keyword interval, which makes query operations according to time windows extremely simple. For example, the current values collected by smart meter d1001 are summed every 10 seconds.

```mysql
taos> SELECT sum(current) FROM d1001 INTERVAL(10s);
           ts            |       sum(current)        |
======================================================
 2018-10-03 14:38:00.000 |              10.300000191 |
 2018-10-03 14:38:10.000 |              24.900000572 |
Query OK, 2 row(s) in set (0.000883s)
```

The down sampling operation is also applicable to STables, such as summing the current values collected by all smart meters in Beijing every second.

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

The down sampling operation also supports time offset, such as summing the current values collected by all smart meters every second, but requires each time window to start from 500 milliseconds.

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

In a scenario of IoT, it is difficult to synchronize the time stamp of collected data at each point, but many analysis algorithms (such as FFT) need to align the collected data strictly at equal intervals of time. In many systems, it’s required to write their own programs to process, but the down sampling operation of TDengine can be easily solved. If there is no collected data in an interval, TDengine also provides interpolation calculation function.

For details of syntax rules, please refer to the [Time-dimension Aggregation section of TAOS SQL](https://www.taosdata.com/en/documentation/taos-sql#aggregation).