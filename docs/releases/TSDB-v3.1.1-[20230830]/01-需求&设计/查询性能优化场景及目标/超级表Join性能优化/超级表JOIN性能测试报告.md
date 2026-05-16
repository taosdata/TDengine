# 超级表JOIN性能测试报告

## 1. 自测数据

采用两个超级表或单个超级表进行JOIN查询，SQL语句类似为：
*select a.ts, b.ts from sta a, sta b where a.ts=b.ts and a.t0=b.t0;*

| 子表个数 | 子表行数 | 结果条数 | BATCH_SCAN耗时 | BATCH_SCAN缓存 | NO_BATCH_SCAN耗时 | NO_BATCH_SCAN缓存 | 2.6耗时 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 200000000 | 200000000 | 111 | 9137 | 92 | 0 | 102 |
| 100 | 1000000 | 55000000 | 24 | 888 | 24 | 213 | NA |
| 10000 | 10000 | 248940000 | 123 | 2161 | 138 | 1079 | NA |
| 500000 | 1000 | 500116000 | 304 | 13985 | 1492 | 2 | NA |

注：时间单位：秒， 存储单位：MB

## 2. 博创数据

SQL语句：
*SELECT sum( t2.es1 )/ 3600 es1, sum( t2.es2 )/ 3600 es2, sum( t2.es3 )/ 3600 es3, sum( t2.es4 )/ 3600 es4, sum( t2.es5 )/ 3600 es5, sum( t2.es6 )/ 3600 es6, sum( t2.es7 )/ 3600 es7, sum( t2.es8 )/ 3600 es8, sum( t2.es9 )/ 3600 es9, sum( t2.es10 )/ 3600 es10, sum( t2.es11 )/ 3600 es11, sum( t2.es12 )/ 3600 es12, sum( t2.es13 )/ 3600 es13, sum( t2.es13 )/ 3600 es13, sum( t2.es14 )/ 3600 es14, sum( t2.es16 )/ 3600 es16, sum( t2.es17 )/ 3600 es17, sum( t2.es18 )/ 3600 es18, sum( t2.es19 )/ 3600 es19, sum( t2.es20 )/ 3600 es20, sum( t2.es21 )/ 3600 es21, sum( t2.es22 )/ 3600 es22, sum( t2.es1 )/ sum( t2.es0 ) persentes1, sum( t2.es2 )/ sum( t2.es0 ) persentes2, sum( t2.es3 )/ sum( t2.es0 ) persentes3, sum( t2.es4 )/ sum( t2.es0 ) persentes4, sum( t2.es5 )/ sum( t2.es0 ) persentes5, sum( t2.es6 )/ sum( t2.es0 ) persentes6, sum( t2.es7 )/ sum( t2.es0 ) persentes7, sum( t2.es8 )/ sum( t2.es0 ) persentes8, sum( t2.es9 )/ sum( t2.es0 ) persentes9, sum( t2.es10 )/ sum( t2.es0 ) persentes10, sum( t2.es11 )/ sum( t2.es0 ) persentes11, sum( t2.es12 )/ sum( t2.es0 ) persentes12, sum( t2.es13 )/ sum( t2.es0 ) persentes13, sum( t2.es14 )/ sum( t2.es0 ) persentes14, sum( t2.es15 )/ sum( t2.es0 ) persentes15, sum( t2.es16 )/ sum( t2.es0 ) persentes16, sum( t2.es17 )/ sum( t2.es0 ) persentes17, sum( t2.es18 )/ sum( t2.es0 ) persentes18, sum( t2.es19 )/ sum( t2.es0 ) persentes19, sum( t2.es20 )/ sum( t2.es0 ) persentes20, sum( t2.es21 )/ sum( t2.es0 ) persentes21, sum( t2.es22 )/ sum( t2.es0 ) persentes22 FROM report.d_t t1, report.yx_d_t t2 WHERE t1.DAY = t2.DAY AND t1.vin = t2.vin AND t1.sales_status = 400 AND t1.group_path LIKE '2#%'*

| 子表个数 | BATCH_SCAN耗时 | NO_BATCH_SCAN耗时 | 2.6耗时 | 3.0耗时 |
| --- | --- | --- | --- | --- |
| 50318 | 0.253 | 5.153 | 0.527 | 13.657 |

## 3. 测试结论

- 在 Tag 连接条件能有效过滤部分表的场景下，性能超过2.x水平；
- NO_BATCH_SCAN模式能有效降低缓存空间占用，在表数量较少时，NO_BATCH_SCAN模式效率更高；
