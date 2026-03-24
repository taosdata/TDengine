# Partition by + Slimit/Limit性能优化

## 1. 1:测试背景

https://jira.taosdata.com:18080/browse/TD-25365?filter=23428 【partition by tag + slimit性能优化】
https://jira.taosdata.com:18080/browse/TD-25271?filter=23428 【interval + limit性能优化】

## 2. 2:数据准备

数据集1: taosBenchmark默认创建1亿数据，一万子表*一万数据[tag=10个不重复的]
数据集2: taosBenchmark默认创建10亿数据，一百万子表*一千数据[tag=253个不重复的]

## 3. 3:测试场景

### 3.1 测试结论：

a：场景1、场景2: [只测试了数据集1] 3.0 分支在slimit n 场景中，n值比较小时（临界值是子表数/10），查询提升明显，越小越快，超过临界值后，main分支和3.0分支都有快的，都差不多。
b：场景3、场景6: [只测试了数据集1]  3.0 分支在limit n 场景中，n值比较小时（临界值是子表数/2），查询提升明显，越小越快，超过临界值后，3.0分支明显慢于main分支了，这个在开发提测的报告中也有说明。
c：场景4、场景5: [数据集1]  3.0分支和main分支查询时间基本无变化，应该是没有优化，或者我选测的场景没有命中此轮的修改。
    新增了[数据集2，将tag数从10增加到253，子表从1w增加到100w] 3.0分支查询全面优于main分支，n值越小优势越明显。

### 3.2 场景1: Agg + partition by tbname + slimit

```sql
select count(*),tbname from meters partition by tbname slimit 10; 
select count(*),tbname from meters partition by tbname slimit 100; 
select count(*),tbname from meters partition by tbname slimit 1000; 
select count(*),tbname from meters partition by tbname slimit 2000; 
select count(*),tbname from meters partition by tbname slimit 3000; 
select count(*),tbname from meters partition by tbname slimit 4000; 
select count(*),tbname from meters partition by tbname slimit 5000; 
select count(*),tbname from meters partition by tbname slimit 6000; 
select count(*),tbname from meters partition by tbname slimit 7000; 
select count(*),tbname from meters partition by tbname slimit 8000; 
select count(*),tbname from meters partition by tbname slimit 9000; 
select count(*),tbname from meters partition by tbname slimit 10000; 
```


| 查询sql，编号1-12 | main（s） | 3.0（s） | 备注 |
| --- | --- | --- | --- |
| Sql 1 | 0.484 | 0.047 |  |
| Sql 2 | 0.508 | 0.268 |  |
| Sql 3 | 1.071 | 0.982 |  |
| Sql 4 | 1.284 | 1.722 | 分界点 |
| Sql 5 | 1.773 | 2.213 |  |
| Sql 6 | 2.716 | 2.782 |  |
| Sql 7 | 3.407 | 3.310 |  |
| Sql 8 | 2.933 | 4.203 |  |
| Sql 9 | 4.162 | 4.460 |  |
| Sql 10 | 4.787 | 5.024 |  |
| Sql 11 | 5.431 | 5.473 |  |
| Sql 12 | 6.739 | 5.891 |  |

### 3.3 场景2:Agg + partition by tbname + interval + slimit

```sql
select count(*),tbname from meters partition by tbname interval(1a) slimit 10; 
select count(*),tbname from meters partition by tbname interval(1a) slimit 100; 
select count(*),tbname from meters partition by tbname interval(1a) slimit 1000; 
select count(*),tbname from meters partition by tbname interval(1a) slimit 2000; 
select count(*),tbname from meters partition by tbname interval(1a) slimit 3000; 
select count(*),tbname from meters partition by tbname interval(1a) slimit 4000; 
select count(*),tbname from meters partition by tbname interval(1a) slimit 5000; 
select count(*),tbname from meters partition by tbname interval(1a) slimit 6000; 
select count(*),tbname from meters partition by tbname interval(1a) slimit 7000; 
select count(*),tbname from meters partition by tbname interval(1a) slimit 8000; 
select count(*),tbname from meters partition by tbname interval(1a) slimit 9000; 
select count(*),tbname from meters partition by tbname interval(1a) slimit 10000; 
```


| 查询sql，编号1-12 | main（s） | 3.0（s） | 备注 |
| --- | --- | --- | --- |
| Sql 1 | 12.159 | 0.628 |  |
| Sql 2 | 13.701 | 6.262 |  |
| Sql 3 | 31.216 | 29.018 |  |
| Sql 4 | 45.638 | 45.944 | 分界点 |
| Sql 5 | 64.078 | 64.046 |  |
| Sql 6 | 87.872 | 80.387 |  |
| Sql 7 | 99.370 | 97.343 |  |
| Sql 8 | 119.021 | 122.854 |  |
| Sql 9 | 141.793 | 130.211 |  |
| Sql 10 | 159.854 | 151.889 |  |
| Sql 11 | 164.329 | 176.841 |  |
| Sql 12 | 197.210 | 186.019 |  |

### 3.4 场景3:Agg + partition by tbname + interval + limit

```sql
explain analyze verbose true select count(*),tbname from meters partition by tbname interval(1a) limit 10; 
explain analyze verbose true select count(*),tbname from meters partition by tbname interval(1a) limit 100; 
explain analyze verbose true select count(*),tbname from meters partition by tbname interval(1a) limit 1000; 
explain analyze verbose true select count(*),tbname from meters partition by tbname interval(1a) limit 2000; 
explain analyze verbose true select count(*),tbname from meters partition by tbname interval(1a) limit 3000; 
explain analyze verbose true select count(*),tbname from meters partition by tbname interval(1a) limit 4000; 
explain analyze verbose true select count(*),tbname from meters partition by tbname interval(1a) limit 5000; 
explain analyze verbose true select count(*),tbname from meters partition by tbname interval(1a) limit 6000; 
explain analyze verbose true select count(*),tbname from meters partition by tbname interval(1a) limit 7000; 
explain analyze verbose true select count(*),tbname from meters partition by tbname interval(1a) limit 8000; 
explain analyze verbose true select count(*),tbname from meters partition by tbname interval(1a) limit 9000; 
explain analyze verbose true select count(*),tbname from meters partition by tbname interval(1a) limit 10000;
减少打印时间的影响，前面加上explain analyze verbose true  ,下同
```


| 查询sql，编号1-12 | main（s） | 3.0（s） | 备注 |
| --- | --- | --- | --- |
| Sql 1 | 80.045 | 12.267 |  |
| Sql 2 | 80.707 | 12.926 |  |
| Sql 3 | 81.755 | 22.390 |  |
| Sql 4 | 83.759 | 34.802 |  |
| Sql 5 | 84.216 | 46.869 |  |
| Sql 6 | 85.025 | 59.975 |  |
| Sql 7 | 87.634 | 73.299 | 分界点 |
| Sql 8 | 85.518 | 91.667 |  |
| Sql 9 | 93.043 | 124.274 |  |
| Sql 10 | 96.167 | 154.936 |  |
| Sql 11 | 106.610 | 209.335 |  |
| Sql 12 | 118.239 | 223.002 |  |

### 3.5 场景4:Agg + partition by tag + slimit

```sql
select count(*),groupid from meters partition by groupid slimit 10; 
select count(*),groupid from meters partition by groupid slimit 100; 
select count(*),groupid from meters partition by groupid slimit 1000; 
select count(*),groupid from meters partition by groupid slimit 2000; 
select count(*),groupid from meters partition by groupid slimit 3000; 
select count(*),groupid from meters partition by groupid slimit 4000; 
select count(*),groupid from meters partition by groupid slimit 5000; 
select count(*),groupid from meters partition by groupid slimit 6000; 
select count(*),groupid from meters partition by groupid slimit 7000; 
select count(*),groupid from meters partition by groupid slimit 8000; 
select count(*),groupid from meters partition by groupid slimit 9000; 
select count(*),groupid from meters partition by groupid slimit 10000; 
```


|  |
| --- |
| 查询sql，编号1-12 | main（s） | 3.0（s） | 备注 | main（s） | 3.0（s） | 备注 |
| Sql 1 | 0.362 | 0.335 |  | 18.703 | 0.925 |  |
| Sql 2 | 0.365 | 0.325 |  | 18.871 | 5.903 |  |
| Sql 3 | 0.366 | 0.334 |  | 18.850 | 13.993 |  |
| Sql 4 | 0.360 | 0.327 |  | 18.903 | 14.246 |  |
| Sql 5 | 0.360 | 0.327 |  | 18.773 | 14.001 |  |
| Sql 6 | 0.365 | 0.335 |  | 19.050 | 13.842 |  |
| Sql 7 | 0.359 | 0.337 |  | 18.785 | 13.925 |  |
| Sql 8 | 0.361 | 0.321 |  | 18.909 | 13.725 |  |
| Sql 9 | 0.378 | 0.325 |  | 18.869 | 13.461 |  |
| Sql 10 | 0.371 | 0.331 |  | 18.998 | 13.480 |  |
| Sql 11 | 0.361 | 0.327 |  | 18.862 | 13.496 |  |
| Sql 12 | 0.368 | 0.333 |  | 19.248 | 13.883 |  |

### 3.6 场景5:Agg + partition by tag + interval + slimit

```sql
select count(*),groupid from meters partition by groupid interval(1a) slimit 10; 
select count(*),groupid from meters partition by groupid interval(1a) slimit 100; 
select count(*),groupid from meters partition by groupid interval(1a) slimit 1000; 
select count(*),groupid from meters partition by groupid interval(1a) slimit 2000; 
select count(*),groupid from meters partition by groupid interval(1a) slimit 3000; 
select count(*),groupid from meters partition by groupid interval(1a) slimit 4000; 
select count(*),groupid from meters partition by groupid interval(1a) slimit 5000; 
select count(*),groupid from meters partition by groupid interval(1a) slimit 6000; 
select count(*),groupid from meters partition by groupid interval(1a) slimit 7000; 
select count(*),groupid from meters partition by groupid interval(1a) slimit 8000; 
select count(*),groupid from meters partition by groupid interval(1a) slimit 9000; 
select count(*),groupid from meters partition by groupid interval(1a) slimit 10000;  
```


|  |
| --- |
| 查询sql，编号1-12 | main（s） | 3.0（s） | 备注 | main（s） | 3.0（s） | 备注 |
| Sql 1 | 5.664 | 5.466 |  | 35.097 | 1.576 |  |
| Sql 2 | 5.536 | 5.394 |  | 37.644 | 14.206 |  |
| Sql 3 | 5.519 | 5.399 |  | 41.958 | 35.814 |  |
| Sql 4 | 5.609 | 5.502 |  | 42.081 | 36.002 |  |
| Sql 5 | 5.629 | 5.417 |  | 41.878 | 35.728 |  |
| Sql 6 | 5.571 | 5.386 |  | 41.496 | 35.694 |  |
| Sql 7 | 5.577 | 5.426 |  | 41.785 | 35.817 |  |
| Sql 8 | 5.598 | 5.515 |  | 42.137 | 35.735 |  |
| Sql 9 | 5.610 | 5.432 |  | 42.251 | 35.841 |  |
| Sql 10 | 5.614 | 5.450 |  | 41.807 | 35.818 |  |
| Sql 11 | 5.624 | 5.418 |  | 41.830 | 35.543 |  |
| Sql 12 | 5.515 | 5.465 |  | 41.531 | 35.707 |  |

### 3.7 场景6:Agg + partition by tag + interval  + limit

```sql
explain analyze verbose true select count(*),groupid from meters partition by groupid interval(1a) limit 10; 
explain analyze verbose true select count(*),groupid from meters partition by groupid interval(1a) limit 100; 
explain analyze verbose true select count(*),groupid from meters partition by groupid interval(1a) limit 1000; 
explain analyze verbose true select count(*),groupid from meters partition by groupid interval(1a) limit 2000; 
explain analyze verbose true select count(*),groupid from meters partition by groupid interval(1a) limit 3000; 
explain analyze verbose true select count(*),groupid from meters partition by groupid interval(1a) limit 4000; 
explain analyze verbose true select count(*),groupid from meters partition by groupid interval(1a) limit 5000; 
explain analyze verbose true select count(*),groupid from meters partition by groupid interval(1a) limit 6000; 
explain analyze verbose true select count(*),groupid from meters partition by groupid interval(1a) limit 7000; 
explain analyze verbose true select count(*),groupid from meters partition by groupid interval(1a) limit 8000; 
explain analyze verbose true select count(*),groupid from meters partition by groupid interval(1a) limit 9000; 
explain analyze verbose true select count(*),groupid from meters partition by groupid interval(1a) limit 10000; 
```


| 查询sql，编号1-12 | main（s） | 3.0（s） | 备注 |
| --- | --- | --- | --- |
| Sql 1 | 5.594 | 1.793 |  |
| Sql 2 | 5.531 | 1.800 |  |
| Sql 3 | 5.570 | 2.403 |  |
| Sql 4 | 5.561 | 2.922 |  |
| Sql 5 | 5.626 | 3.546 |  |
| Sql 6 | 5.609 | 4.089 |  |
| Sql 7 | 5.650 | 4.701 |  |
| Sql 8 | 5.677 | 5.310 | 分界点 |
| Sql 9 | 5.639 | 5.946 |  |
| Sql 10 | 5.611 | 6.608 |  |
| Sql 11 | 5.597 | 7.717 |  |
| Sql 12 | 5.676 | 7.790 |  |
