# stmt2性能测试

## 1. 概述

TDengine基线性能测试是产品的重要性能衡量指标，范围涵盖：写入、查询、流计算、订阅、数据同步等多个方面。初期基线性能测试范围暂定为：写入与查询。

## 2. 报告正文

### 2.1 测试方法

#### 2.1.1 写入性能

通过taosBenchmark写入数据，重复5次写入，删除最大和最小RPS结果，剩余3个结果计算平均值

#### 2.1.2 查询性能

通过jimeter基于query via jni并发查询，重复5次查询，删除最大和最小QPS结果，剩余3个结果计算平均值

### 2.2 比对判据

| # | cachemodel | 查询类型 | 测试场景 | 并发数 | Loop | Jmeter连接池最大连接数 | QPS测试结果（rows/s） |  |  |  |  |  |  | 数据对比 |  |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
|  |  |  |  |  |  |  | v3.3.2.0 |  | v3.3.3.0 |  | v3.0 |  |  | v3.0 |  |
|  |  |  |  |  |  |  | taosc | stmt via taosc | taosc | stmt via taosc | taosc | stmt via taosc | stmt2 via taosc | (stmt2-best)/best | (stmt-best)/best |
| W10001 | none | 数据写入 | cachemodel=none | -- | -- | -- | 546483.35 | 6320711.5 | 527595.46 | 6250676.5 | 535929.94 | 6209236.05 | 6038514.89 | (N4-I4)/I4 | (M4-I4)/I4 |
| W10002 | last_row |  | cachemodel=last_row | -- | -- | -- | 511248.35 | 2653059.5 | 498325.82 | 1949387.5 | 488354 | 1939801.64 | 1907508.14 | (N5-I5)/I5 | (M5-I5)/I5 |
| Q10001 | none | 最新值查询 | 通过last_row(current)查询子表最新数据 | 1000 | 50 | 16 | 1389.9100342 | N/A | 1384.6500244 | N/A | 1378.5500488 | N/A | N/A |  |  |
| Q10002 |  |  | 通过last_row(*)查询子表最新数据 | 1000 | 50 | 16 | 1383.0600586 | N/A | 1376.4499512 | N/A | 1367.9899902 | N/A | N/A |  |  |
| Q10003 |  |  | 通过orderby加limit 1方式查询子表最新数据 | 1000 | 50 | 16 | 1403.5400391 | N/A | 1387.6899414 | N/A | 1362.8100586 | N/A | N/A |  |  |
| Q20001 |  | 投影查询 | 查询一个子表中1天的所有数据 | 200 | 50 | 16 | 1486.3299561 | N/A | 1458.1500244 | N/A | 1463.2700195 | N/A | N/A |  |  |
| Q20002 |  |  | 查询一个子表中1天的ts和current数据 | 200 | 50 | 16 | 1511.0600586 | N/A | 1490.1700439 | N/A | 1490.9799805 | N/A | N/A |  |  |
| Q20003 |  |  | 查询一个子表中7天的ts和current数据 | 200 | 50 | 16 | 660.7000122 | N/A | 634.4299927 | N/A | 639.0200195 | N/A | N/A |  |  |
| Q20004 |  |  | 查询某个地区的所有数据 | 100 | 50 | 16 | 79.0199966 | N/A | 76.9599991 | N/A | 77.5199966 | N/A | N/A |  |  |
| Q20005 |  |  | 查询valtage=99的所有数据 | 100 | 50 | 16 | 1095.2199707 | N/A | 810.4299927 | N/A | 823.9899902 | N/A | N/A |  |  |
| Q30001 |  | 函数查询 | 子表一天电流数据的percentile | 500 | 50 | 16 | 674.2299805 | N/A | 673.7199707 | N/A | 183.7700043 | N/A | N/A |  |  |
| Q30002 |  |  | 子表一天电流的apercentile | 500 | 50 | 16 | 1114.8100586 | N/A | 994.0100098 | N/A | 1055.3000488 | N/A | N/A |  |  |
| Q30003 |  | 聚合查询 | 统计子表行数、电流总量、平均电压 | 500 | 50 | 16 | 1092.3900146 | N/A | 1112.4399414 | N/A | 1149.5300293 | N/A | N/A |  |  |
| Q30004 |  | 断面查询 | 查询一天电流数据每10秒的插值计算 | 20 | 50 | 16 | 409.4700012 | N/A | 397.5299988 | N/A | 395.8800049 | N/A | N/A |  |  |
| Q40001 |  | 分组查询 | 通过分组查询加slimit查询10个子表的最新数据 | 500 | 50 | 16 | 1528.2099609 | N/A | 1534.0400391 | N/A | 1556.6600342 | N/A | N/A |  |  |
| Q50001 |  | 嵌套查询 | 嵌套查询 | 50 | 50 | 16 | 711.6500244 | N/A | 720 | N/A | 713.4699707 | N/A | N/A |  |  |
| Q60001 |  | Join查询 | inner join | 50 | 50 | 16 | 313.8999939 | N/A | 320.6600037 | N/A | 310.519989 | N/A | N/A |  |  |
| Q60002 |  |  | left  join | 50 | 50 | 16 | 282.7999878 | N/A | 281.1000061 | N/A | 270.7099915 | N/A | N/A |  |  |
| Q60003 |  |  | right  join | 50 | 50 | 16 | 282.0899963 | N/A | 275.4500122 | N/A | 268.9899902 | N/A | N/A |  |  |
| Q60004 |  |  | full join | 50 | 50 | 16 | 257.019989 | N/A | 243.9799957 | N/A | 239.4600067 | N/A | N/A |  |  |
| Q60005 |  |  | union all | 50 | 50 | 16 | 413.1700134 | N/A | 405.2200012 | N/A | 380.75 | N/A | N/A |  |  |
| Q10001LR | last_row | 最新值查询 | 通过last_row(current)查询子表最新数据 | 1000 | 50 | 16 | 1422.0999756 | N/A | 1416.8000488 | N/A |  | N/A | N/A |  |  |
| Q10002LR |  |  | 通过last_row(*)查询子表最新数据 | 1000 | 50 | 16 | 1429.8900146 | N/A | 1403.2299805 | N/A |  | N/A | N/A |  |  |
| Q10003LR |  |  | 通过orderby加limit 1方式查询子表最新数据 | 1000 | 50 | 16 | 1394.2800293 | N/A | 1382.5200195 | N/A |  | N/A | N/A |  |  |

## 3. 测试场景

### 3.1 数据写入场景

采用taosBenchmark工具，多线程并发向meters超级表下100万张子表拼批逐行写入，主要参数如下：

| 场景 | 列1 | 列2 | 列3 | 标签1 | 标签2 |
| --- | --- | --- | --- | --- | --- |
| 电表场景 | current FLOAT | voltage INT | phase FLOAT | location | groupId |


|  | cachemodel | vgroups | replica | stt_trigger | cachesize | wal_level | buffer | duration |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 建库参数 | none/last_row | 32 | 1 | 2 | 100 | 1 | 256 | 10d |


|  | 子表数 | 子表行数 | 步长 | 列数 | 标签数 | 写入模式 | interlace | thread_count | num_of_records_per_req | thread_bind_vgroup |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 写入参数 | 100W | 2016 | 5分钟 | 3 | 2 | stmt via taosc | 1 | 32 | 1000 | yes |


|  | numOfCommitThreads | compressMsgSize | monitor | audit |
| --- | --- | --- | --- | --- |
| taos.cfg | 4 | -1 | 0 | 0 |

### 3.2 数据查询场景

采用Jmeter工具，多线程并发向单节点TDengine实例发起查询请求，主要测试场景和并发数如下：
| id | cachemodel | 查询类型 | 测试场景 | 并发数 | Loop | json文件 | SQL |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Q10001 | none | 最新值查询 | 通过last_row(current)查询子表最新数据 | 1000 | 50 | query_lastrow_current.jmx | select last_row(current) from test.${tbname} |
| Q10002 |  |  | 通过last_row(*)查询子表最新数据 | 1000 | 50 | query_lastrow_all.jmx | select last_row(*) from test.${tbname} |
| Q10003 |  |  | 通过orderby加limit 1方式查询子表最新数据 | 1000 | 50 | query_orderby_limit1.jmx | select * from test.${tbname} order by ts desc limit 1 |
| Q20001 |  | 投影查询 | 查询一个子表中1天的所有数据 | 200 | 50 | query_for_one_day.jmx | select * from test.${tbname} where ts >= '2022-10-01 00:00:00.000' and ts <= '2022-10-02 00:00:00.000' |
| Q20002 |  |  | 查询一个子表中1天的ts和current数据 | 200 | 50 | query_for_one_day_current.jmx | select ts,current from test.${tbname} where ts >= '2022-10-01 00:00:00.000' and ts <= '2022-10-02 00:00:00.000' |
| Q20003 |  |  | 查询一个子表中7天的ts和current数据 | 200 | 50 | query_for_one_week_current.jmx | select ts,current from test.${tbname} where ts >= '2022-10-01 00:00:00.000' and ts <= '2022-10-08 00:00:00.000' |
| Q20004 |  |  | 查询某个地区的所有数据 | 100 | 50 | query_filter_tags.jmx | select ts, current, voltage from test.meters where location = '${location}' |
| Q20005 |  |  | 查询valtage=99的所有数据 | 100 | 50 | query_filter_column.jmx | select ts, current, voltage from test.meters where voltage=99 |
| Q30001 |  | 函数查询 | 子表一天电流数据的percentile | 500 | 50 | query_percentile.jmx | select percentile(current, 0.2) from test.${tbname} where ts >= '2022-10-01 00:00:00.000' and ts <= '2022-10-02 00:00:00.000' |
| Q30002 |  |  | 子表一天电流的apercentile | 500 | 50 | query_apercentile.jmx | select APERCENTILE(current, 50) from test.${tbname} where ts >= '2022-10-01 00:00:00.000' and ts <= '2022-10-02 00:00:00.000' |
| Q30003 |  | 聚合查询 | 统计子表行数、电流总量、平均电压 | 500 | 50 | query_aggregate.jmx | select tbname, count(*),sum(current), avg(voltage) from test.${tbname} |
| Q30004 |  | 断面查询 | 查询一天电流数据每10秒的插值计算 | 20 | 50 | query_select_interp.jmx | select interp(current) from test.${tbname} range('2022-10-01 00:00:00.000','2022-10-02 00:00:00.000') every(10s) fill(linear) |
| Q40001 |  | 分组查询 | 通过分组查询加slimit查询10个子表的最新数据 | 500 | 50 | query_partitionby_slimit.jmx | select tbname, last(*) from test.meters partition by tbname slimit 10 |
| Q50001 |  | 嵌套查询 | 嵌套查询 | 50 | 50 | query_nesting.jmx | select abs(max_a - min_a ) from (select max(a) max_a,min(b) min_a from ( select last(current) as a, avg(current) as b from test.d1 group by tbname)) |
| Q60001 |  | Join查询 | inner join | 50 | 50 | query_inner_join.jmx | select * from test.d1 as t1 inner join test.d2 as t2 on t1.voltage = t2.voltage and t1.ts = t2.ts |
| Q60002 |  |  | left  join | 50 | 50 | query_left_join.jmx | select * from test.d1 as t1 left join test.d2 as t2 on t1.voltage = t2.voltage and t1.ts = t2.ts and t1.ts >= '2022-10-01 00:00:00.000' and t1.ts < '2022-10-01 00:00:05.000' |
| Q60003 |  |  | right  join | 50 | 50 | query_right_join.jmx | select * from test.d0 as t1 right join test.d1 as t2 on t1.voltage = t2.voltage and t1.ts = t2.ts and t2.ts in ( '2022-10-01 00:00:00.000','2022-10-01 00:00:30.000') |
| Q60004 |  |  | full join | 50 | 50 | query_full_join.jmx | select * from test.d0 as t1 full join test.d1 as t2 on t1.voltage = t2.voltage and t1.ts = t2.ts and t1.ts > '2022-10-01 00:00:00.000' and t1.ts < '2022-10-01 00:00:02.000' |
| Q60005 |  |  | union all | 50 | 50 | query_union_join.jmx | select * from test.d1 union all select * from test.d2 union all select * from test.d3 |
| Q10001LR | last_row | 最新值查询 | 投影查询子表中的最新的current数据 | 1000 | 50 | query_lastrow_current.jmx | select last_row(current) from test.${tbname} |
| Q10002LR |  |  | 投影查询子表中的最新的一行数据 | 1000 | 50 | query_lastrow_all.jmx | select last_row(*) from test.${tbname} |
| Q10003LR |  |  | 通过orderby加limit 1方式查询子表最新数据 | 1000 | 50 | query_orderby_limit1.jmx | select * from test.${tbname} order by ts desc limit 1 |

### 3.3 测试版本

| 版本 | 安装包 |
| --- | --- |
| 3.3.3.0 | /nas/TDengine/smoking/v3.3.3.0/enterprise/TDengine-enterprise-3.3.3.0-Linux-x64.tar.gz |

### 3.4 测试环境

#### 3.4.1 测试机器

| Hostname | 操作系统 | IP | 用途 |
| --- | --- | --- | --- |
| u1-54 | Ubuntu X86 | 192.168.1.54 | 性能测试工具和Jmeter宿主机 |
| u1-43 | Ubuntu X86 | 192.168.1.43 | TDengine部署机器 |

#### 3.4.2 机器资源

|  | 节点数 | CPU | 内存 | 存储 | 网络 |
| --- | --- | --- | --- | --- | --- |
| 节点配置 | 1 | 40Core | 256G | 3块 SSD: /data1: 读写速度约450M/s /data3: 读写速度约300M/s /data4: 读写速度约350M/s | 万兆网 |

环境详细信息，参见文档关于u1-54的相关信息：[机器配置信息](https://taosdata.feishu.cn/wiki/I4hbwscQHibds4kCIYEcOXWmnYd)

## 4. 相关文档

[基线性能测试方法](https://taosdata.feishu.cn/wiki/CqyowSvW6idLYRkQI2kcsnMAnHb)
