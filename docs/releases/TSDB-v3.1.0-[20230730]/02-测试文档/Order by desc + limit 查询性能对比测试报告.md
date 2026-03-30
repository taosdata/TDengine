# Order by desc + limit 查询性能对比测试报告

对比几个版本, 176 机器上测试
taosBenchmark -y -t 1000 -n 100000

SQL 语句：
 SQL1:  sselect count(*) from meters;
 SQL2:   select * from meters order by ts desc limit 20;
 SQL3:   select * from meters order by ts desc;
 SQL4:   select * from meters order by ts asc;

| 版本号 | 时间 | 写入时间 | SQL1(count(*)) | SQL2（limit） | SQL3(DESC) | SQL4(ASC) | 数据库名 |  |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| b5ff9182da | hzcheng 21794 合入前的版本 | 74.72015 | 0.062885s | 2.984985s | 91.119284s | 90.101688s | TEST | insert delay, min: 26.0400ms, avg: 64.8320ms, p90: 77.4690ms, p95: 95.8650ms, p99: 276.1610ms, max: 2765.0510ms |
| fde8eabfd7 | 老周合入后的版本 |  | 0.021336s | 2.984794s | 90.467374s | 90.404220s | TEST |  |
|  |  | 71.879974 | 0.116110s | 2.965877s | 93.950839s | 88.552811s | TEST1 | insert delay, min: 26.4860ms, avg: 63.0750ms, p90: 73.3820ms, p95: 96.3530ms, p99: 308.5060ms, max: 2876.4050ms |
| 45cb478b319c6de8736408c45d7f132ee7227f1e | 老周合入前的版本 |  | 0.016116s | 34.336868s | 81.272266s | 85.084617s | TEST |  |
|  |  | 73.236967 | 0.088765s | 28.882777s | 93.283181s | 91.753000s | TEST2 | insert delay, min: 26.3600ms, avg: 65.4989ms, p90: 75.5180ms, p95: 95.5660ms, p99: 293.3190ms, max: 3233.3430ms |
| 3.0.7.1 | 上周发的 |  | 0.018423s | 27.668833s | 88.147615s | 90.365041s | TEST |  |
|  |  | 72.945337 | 0.077631s | 32.533022s | 96.127970s | 97.878945s | TEST3 | insert delay, min: 25.9060ms, avg: 63.2416ms, p90: 74.0800ms, p95: 93.6430ms, p99: 261.8490ms, max: 2959.2480ms |
| 3.0.5.1 |  |  | 0.021813s | 0.203368s | 65.652584s | 65.120260s | TEST |  |
|  |  | 74.515164 | 0.122292s | 0.105784s | 64.809692s | 64.782254s | TEST4 | insert delay, min: 26.5560ms, avg: 65.3375ms, p90: 74.9980ms, p95: 95.9170ms, p99: 299.4090ms, max: 3018.3130ms |

结论：
     1)    3.0.5.1 版本 和 之后的版本 DESC + LIMIT 查询性能差距巨大
     2)   老周优化后的版本优化效果明显，但仍然没有达到 3.0.5.1 的速度
     3)    SQL3 和 SQL4 的查询，3.0.5.1 也要快一些
     4） 需要查明 3.0.5.1 的快是自身有问题还是后来的版本有问题
