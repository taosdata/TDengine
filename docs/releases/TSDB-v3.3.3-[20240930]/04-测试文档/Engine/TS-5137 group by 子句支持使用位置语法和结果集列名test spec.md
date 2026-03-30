# TS-5137 group by 子句支持使用位置语法和结果集列名test spec

## 1. 测试目标

测试需求文档：[group by 子句支持使用位置语法和结果集列名](https://taosdata.feishu.cn/wiki/RDTwwqZCbijla6khtG8c4IAXn7e)
本次测试主要验证以下方面：
- group_epxr/partition_expr是否支持位置语法
- group_epxr/partition_expr是否支持结果集列名
- group_epxr/partition_expr是否支持非聚集函数表达式
- 是否不支持位置语法/结果集列名对应的SELECT列表里面的表达式是聚集函数
- group by/partition by子句是否不支持包含聚集函数
- group by/partition by的性能测试

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024-07-12 | 0.1 | @黄帅 | test spec |
| 2024-07-17 | 0.2 | @黄帅 | 测试用例重构 |

## 3. 测试结论

### 3.1 功能测试结论

- group_epxr/partition_expr**已支持位置语法**
- group_epxr/partition_expr**已支持结果集列名**
- group_epxr/partition_expr**已支持非聚集函数表达式**
- 不支持位置语法/结果集列名对应的SELECT列表里面的表达式是聚集函数的测试**通过**
- group by/partition by子句不支持包含聚集函数的测试**通过**

### 3.2 性能测试结论

#### 3.2.1 已有功能对比测试结论

- 子表数固定 vgroup变化时约60%的情况下的性能提升，40%的情况性能下降
- 子表数变化 vgroup固定时性能均有不同程度的下降
以下是具体数据：
子表数固定为一万 vgroup变化

| 测试用例说明 | 测试用例 | 子表数固定为一万 vgroup变化 | 相比之前版本性能提升 |
| --- | --- | --- | --- |
| vgroup=10 | 51.76% |
| vgroup=20 | 3% |
| vgroup=40 | 72.81% |
| vgroup=10 | -11.02% |
| vgroup=20 | -16.01% |
| vgroup=40 | -4.91% |
| vgroup=10 | 0.25% |
| vgroup=20 | 0.38% |
| vgroup=40 | 5.02% |
| vgroup=10 | 37.59% |
| vgroup=20 | -3.25% |
| vgroup=40 | 60.15% |
| vgroup=10 | -0.68% |
| vgroup=20 | -2.37% |
| vgroup=40 | 2.72% |

子表数变化 vgroup固定为20

| 测试用例说明 | 测试用例 | 子表数变化 vgroup固定为20 | 相比之前版本性能提升 |
| --- | --- | --- | --- |
| 子表数=1k | -243.35% |
| 子表数=2w | -209.35% |
| 子表数=1k | -19.16% |
| 子表数=2w | -11.75% |
| 子表数=1k | -1.47% |
| 子表数=2w | -1.45% |
| 子表数=1k | -144.56% |
| 子表数=2w | -118.47 |
| 子表数=1k | -4.81% |
| 子表数=2w | -4.21% |

#### 3.2.2 新增功能测试结论

| 测试用例说明 | 测试用例 | 子表数固定，vgroup变化 | 总耗时seconds | avg(s) | min(s) | max(s) | p90(s) | p95 (s) | p99 (s) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| vgroup=10 | 346.9580 | 34.491170s | 29.633129s | 38.551113s | 36.863550s | 37.618715s | 38.551113s |
| vgroup=20 | 469.9040 | 46.570614s | 18.660264s | 53.914107s | 53.367036s | 53.614162s | 53.914107s |
| vgroup=40 | 408.9250 | 40.527473s | 22.463589s | 58.285655s | 57.395393s | 57.427582s | 58.285655s |
| vgroup=10 | 358.0520 | 35.690727s | 30.423152s | 40.417400s | 38.415331s | 39.022179s | 40.417400s |
| vgroup=20 | 323.9810 | 31.887577s | 21.169420s | 40.866759s | 33.246224s | 33.655124s | 40.866759s |
| vgroup=40 | 664.8650 | 65.932041s | 58.121706s | 75.852713s | 69.348042s | 75.094094s | 75.852713s |
| vgroup=10 | 1.5000 | 0.049129s | 0.045840s | 0.059583s | 0.049862s | 0.050239s | 0.059583s |
| vgroup=20 | 1.4060 | 0.039973s | 0.038244s | 0.041516s | 0.040801s | 0.040875s | 0.041516s |
| vgroup=40 | 1.450 | 0.044198s | 0.039765s | 0.049649s | 0.045177s | 0.045240s | 0.049649s |
| vgroup=10 | 1.4960 | 0.048388s | 0.045179s | 0.052903s | 0.049593s | 0.052320s | 0.052903s |
| vgroup=20 | 1.3890s | 0.038141s | 0.036261s | 0.040467s | 0.039732s | 0.039841s | 0.040467s |
| vgroup=40 | 1.4310 | 0.041111s | 0.038367s | 0.052216s | 0.042239s | 0.043931s | 0.052216s |
| vgroup=10 | 1.6930 | 0.068534s | 0.062016s | 0.078703s | 0.071849s | 0.072673s | 0.078703s |
| vgroup=20 | 1.5740 | 0.056356s | 0.053361s | 0.070685s | 0.056052s | 0.056154s | 0.070685s |
| vgroup=40 | 1.6640 | 0.064390s | 0.060748s | 0.071371s | 0.063966s | 0.066335s | 0.071371s |
| vgroup=10 | 1.7980 | 0.078410s | 0.072881s | 0.082555s | 0.079264s | 0.081254s | 0.082555s |
| vgroup=20 | 1.675 | 0.066404s | 0.060331s | 0.078881s | 0.070301s | 0.072267s | 0.078881s |
| vgroup=40 | 1.7280 | 0.071689s | 0.067157s | 0.085154s | 0.071247s | 0.074161s | 0.085154s |
| vgroup=10 | 2.5140 | 0.147594s | 0.128774s | 0.191799s | 0.153976s | 0.185997s | 0.191799s |
| vgroup=20 | 2.0530 | 0.100950s | 0.096076s | 0.125701s | 0.107424s | 0.111574s | 0.125701s |
| vgroup=40 | 2.0720 | 0.104875s | 0.085237s | 0.170316s | 0.112465s | 0.115567s | 0.170316s |

## 4. 开发质量报告

结论：增加的语法开发质量是**优**

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数（测试阻塞，无法进行） | 0 |
| 基础测试用例不通过 | 0 |
| BUG总数 | 0 |
| 严重BUG数 | 0 |

## 5. 已知问题和限制

1. having子句不支持别名
2. partition by语句可以使用浮点数但是并不指代位置，测试用例和结果可见9.1.1.24

## 6. 测试资源及环境

### 6.1 功能测试环境

- branch: 3.0
- client info: 3.3.3.0.alpha
- server info: ver:3.3.3.0.alpha
- build:Linux-x64 2024-08-02 10:39:16 +0800
- gitinfo:81e88d97628bb81dbf49b6b0d2e3a79df6a769f1

### 6.2 性能测试环境

#### 6.2.1 尚未支持新功能版本信息

- branch: release/ver-3.3.1.0
- client info: 3.3.1.0
- server info: ver:3.3.1.0
- gitinfo: 6fa1d7f8790ae11d95cd719fcb7b07f4dd52ae15

#### 6.2.2 新功能支持版本信息

- branch: 3.0
- client info: 3.3.3.0.alpha
- server info: ver:3.3.3.0.alpha
- build:Linux-x64 2024-08-02 10:39:16 +0800
- gitinfo:81e88d97628bb81dbf49b6b0d2e3a79df6a769f1

## 7. 测试范围及重点

本次测试主要对需求中提到的场景进行复测及性能数据对比
功能测试：测试目前的数据库是否支持测试目标中提到的需要支持的功能，是否不支持测试目标中提到的不需要支持的功能。
性能测试：taosBenchmark测试在多条数据下查询的性能。

## 8. 测试准备

### 8.1 测试数据

#### 8.1.1 单主键测试数据

其中ts为主键
<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: K91zbTAoYo9dS6x3d0qc4XDmnyc)

</view>


<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: YsSpbrSoHogvxHxyMTNcD5Wbn2e)

</view>


#### 8.1.2 复合主键测试数据

其中ts与id为复合主键
<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: WFvTbSZzwoLQBoxsJaTcoORanoh)

</view>


#### 8.1.3 性能测试数据

使用taosBenchmark的insert文件方式生成数据，其中固定子表数一万的有vgroup为10、20和40三种，均为一亿条数据，使用固定group为20的有子表数为1000和20000两种，均为一亿条数据。

### 8.2 建立测试环境

```sql {wrap}
CREATE DATABASE IF NOT EXISTS ts_5137;
```

#### 8.2.1 单主键数据表

```sql {wrap}
CREATE STABLE IF NOT EXISTS ts_5137.meters (ts timestamp, current float, voltage int, phase float, id int, name varchar(8)) TAGS (location BINARY(64), groupId INT);
```

#### 8.2.2 复合主键数据表

```sql {wrap}
CREATE STABLE IF NOT EXISTS ts_5137.meters_d (ts timestamp, id int PRIMARY KEY, current float, voltage int, phase float, name varchar(8)) TAGS (location BINARY(64), groupId INT);
```

### 8.3 数据写入测试环境

单主键的数据表stmt动态绑定写入
```sql {wrap}
INSERT INTO ts_5137.? USING ts_5137.meters TAGS (?, ?) VALUES (?, ?, ?, ?);
```

<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: EE2VbFHuEomFLYxf2l1c5sacnlf)

</view>

<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: L5FFbm4EOo92qExQRDhcTSYsn5e)

</view>


写入复合主键数据
<view type="2">

  > ⚠ 嵌入文件，需在飞书中查看 (token: YvQ6b8BT0ojG0BxiPTwcWUx6nPb)

</view>


```sql {wrap}
INSERT INTO ts_5137.? USING ts_5137.meters_d TAGS('tttt', 1) VALUES(?, ?, ?, ?, ?, ?)
```

## 9. 测试用例

### 9.1 功能测试用例

| 测试目标 | 测试用例说明 | 测试用例 | 预期是否可以输出 | 实际输出 | 测试结果 |
| --- | --- | --- | --- | --- | --- |
| 1. group by的group_epxr无参调用 | ```sql {wrap} select t1.`current` as `__fcol_0` from `ts_5137`.`meters` as t1 group by; ``` | **报错，不支持** SQL语法错误 | **错误的查询语法** DB error: syntax error near ";" (0.002356s) | **通过** |
| 2. partition by的partition_epxr无参调用 | ```sql {wrap} select t1.`current` as `__fcol_0` from `ts_5137`.`meters` as t1 partition by; ``` | **报错，不支持** SQL语法错误 | **错误的查询语法** DB error: syntax error near ";" (0.000105s) | **通过** |
| 3. group by的group_epxr为单个位置参数且参数小于左边界值 | ```sql {wrap} select t1.`current` as `__fcol_0` from `ts_5137`.`meters` as t1 group by 0; ``` | **报错，不支持** Unknown column '0' in 'group statement' | **错误的查询语法** DB error: ORDER BY / GROUP BY item must be the number of a SELECT-list expression (0.001312s) | **通过** |
| 4. partition by的partition_epxr为单个位置参数且参数小于左边界值 | ```sql {wrap} select t1.`current` as `__fcol_0` from `ts_5137`.`meters` as t1 partition by 0; ``` | **报错，不支持** Unknown column '0' in 'group statement' | **错误的查询语法** DB error: ORDER BY / GROUP BY item must be the number of a SELECT-list expression (0.000672s) | **通过** |
| 5. group by的group_epxr为单个合法位置参数 | ```sql {wrap} select t1.`current` as `__fcol_0` from `ts_5137`.`meters` as t1 group by 1; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 6. partition by的partition_epxr为单个合法位置参数 | ```sql {wrap} select t1.`current` as `__fcol_0` from `ts_5137`.`meters` as t1 partition by 1; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 7. group by的group_epxr为单个合法位置参数且携带having子句 | ```sql {wrap} select t1.`current` as `__fcol_0` from `ts_5137`.`meters` as t1 group by 1 having t1.`current` > 0; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 8. partition by的partition_epxr为单个合法位置参数且携带having子句 | ```sql {wrap} select t1.`current` as `__fcol_0` from `ts_5137`.`meters` as t1 partition by 1 having t1.`current` > 0; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 9. group by的group_epxr为单个位置参数且参数大于右边界值 | ```sql {wrap} select t1.`current` as `__fcol_0` from `ts_5137`.`meters` as t1 group by 10; ``` | **报错，不支持** | **错误的查询语法** DB error: ORDER BY / GROUP BY item must be the number of a SELECT-list expression (0.000349s) | **通过** |
| 10. partition by的partition_epxr为单个位置参数且参数大于右边界值 | ```sql {wrap} select t1.`current` as `__fcol_0` from `ts_5137`.`meters` as t1 partition by 10; ``` | **报错，不支持** | **错误的查询语法** DB error: ORDER BY / GROUP BY item must be the number of a SELECT-list expression (0.000349s) | **通过** |
| 13. group by的group_epxr为多个位置参数且部分参数在边界内，部分在边界外 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, t1.`phase` as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 group by 1, 0; ``` | **报错，不支持** | **错误的查询语法** DB error: ORDER BY / GROUP BY item must be the number of a SELECT-list expression (0.000349s) | **通过** |
| 14. partition by的partition_epxr为多个位置参数且部分参数在边界内，部分在边界外 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, t1.`phase` as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 partition by 1, 0; ``` | **报错，不支持** | **错误的查询语法** DB error: ORDER BY / GROUP BY item must be the number of a SELECT-list expression (0.000349s) | **通过** |
| 15. group by的group_epxr为多个位置参数且参数在边界内 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1` from `ts_5137`.`meters` as t1 group by 1, 2; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 16. partition by的partition_epxr为多个位置参数且参数在边界内 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1` from `ts_5137`.`meters` as t1 partition by 1, 2; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 17. group by的group_epxr为位置参数与别名混合 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1` from `ts_5137`.`meters` as t1 group by 1, __fcol_1; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 18. partition by的partition_epxr为位置参数与别名混合 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1` from `ts_5137`.`meters` as t1 partition by 1, __fcol_1; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 19. group by的group_epxr为位置参数与别名混合且位置参数与列名指向同一字段 | ```sql {wrap} select t1.`current` as `__fcol_0` from `ts_5137`.`meters` as t1 group by 1, __fcol_0; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 20. partition by的partition_epxr为位置参数与别名混合且位置参数与列名指向同一字段 | ```sql {wrap} select t1.`current` as `__fcol_0` from `ts_5137`.`meters` as t1 partition by 1, __fcol_0; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 21. group by查询子句包含interval | ```sql {wrap} select t1.`current` as `__fcol_0` from `ts_5137`.`meters` as t1 group by 1 interval(10m); ``` | **报错，不支持** | **错误的查询语法** DB error: syntax error near "interval(10m);" (0.000085s) | **通过** group by 不支持interval |
| 22. partition by查询子句包含interval | ```sql {wrap} select t1.`current` as `__fcol_0` from `ts_5137`.`meters` as t1 partition by 1 interval(10m); ``` | **报错，不支持** | **错误的查询语法** DB error: No valid function in window query (0.000220s) | **通过** |
| 23. group by的group_epxr位置参数为浮点数 | ```sql {wrap} select t1.`current` as `__fcol_0` from `ts_5137`.`meters` as t1 group by 1.0; ``` | **报错，不支持** | **错误的查询语法** DB error: Not a GROUP BY expression (0.000314s) | **通过** |
| 24. partition by的partition_epxr位置参数为浮点数 | ```sql {wrap} select t1.`current` as `__fcol_0` from `ts_5137`.`meters` as t1 partition by 1.0; ``` | **不报错，支持** | **正常查询无错误** | **通过** 此处的1.0作为常量，没有意义，对结果不做分组 |
| 25. 对有复合主键数据表进行group by查询，group_epxr为复合主键 | ```sql {wrap} select t1.`ts` as t, t1.`id`, count(1) as `__fcol_3` from `ts_5137`.`meters_d` as t1 group by 1, 2; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 26. 对有复合主键数据表进行partition by查询，partition_epxr为复合主键 | ```sql {wrap} select t1.`ts` as t, t1.`id`, count(1) as `__fcol_3` from `ts_5137`.`meters_d` as t1 partition by 1, 2; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 27. group by的group_epxr为查询语句中的列 | ```sql {wrap} select inner_query.t, inner_query.id, count(1) as __fcol_3 from ( select t1.`ts` as t, t1.`current` as __fcol_0, t1.`voltage` as __fcol_1, t1.`phase` as __fcol_2, t1.`id` from `ts_5137`.`meters` as t1 ) as inner_query group by inner_query.t, inner_query.id; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 28. partition by的partition_epxr为查询语句中的列 | ```sql {wrap} select inner_query.t, inner_query.id, count(1) as __fcol_3 from ( select t1.`ts` as t, t1.`current` as __fcol_0, t1.`voltage` as __fcol_1, t1.`phase` as __fcol_2, t1.`id` from `ts_5137`.`meters` as t1 ) as inner_query partition by inner_query.t, inner_query.id; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 29. group by的group_expr的参数未完全包含可以group by的列 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, t1.`phase` as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 group by 1; ``` | **报错，不支持** | **错误的查询语法** DB error: Not a GROUP BY expression (0.000314s) | **通过** |
| 30. partition by的partition_expr的参数未完全包含可以group by的列 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, t1.`phase` as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 partition by 1; ``` | **报错，不支持** | **错误的查询语法** DB error: Not a GROUP BY expression (0.000314s) | **通过** |
| 31. group by在from子句的select语句中 | ```sql {wrap} select sum(`t0`.`__fcol_1`) as `__fcol_2` from ( select `m`.`current` as `__fcol_0`, sum(`m`.`voltage`) as `__fcol_1` from `ts_5137`.`meters` as `m` group by 1 ) as `t0`; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 32. partition by在from子句的select语句中 | ```sql {wrap} select sum(`t0`.`__fcol_1`) as `__fcol_2` from ( select `m`.`current` as `__fcol_0`, sum(`m`.`voltage`) as `__fcol_1` from `ts_5137`.`meters` as `m` partition by 1 ) as `t0`; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 33. group by的group_epxr为查询语句中的列的位置参数 | ```sql {wrap} select inner_query.t, inner_query.id, count(1) as __fcol_3 from ( select t1.`ts` as t, t1.`current` as __fcol_0, t1.`voltage` as __fcol_1, t1.`phase` as __fcol_2, t1.`id` from `ts_5137`.`meters` as t1 ) as inner_query group by 1, 2; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 34. partition by的partition_epxr为查询语句中的列的位置参数 | ```sql {wrap} select inner_query.t, inner_query.id, count(1) as __fcol_3 from ( select t1.`ts` as t, t1.`current` as __fcol_0, t1.`voltage` as __fcol_1, t1.`phase` as __fcol_2, t1.`id` from `ts_5137`.`meters` as t1 ) as inner_query partition by 1, 2; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 35. 两表left outer join连接使用group by位置语法 | ```sql {wrap} select t1.current, sum(t1.voltage) from ( ts_5137.d1001 t1 left outer join ts_5137.d1002 t2 on t1.ts = t2.ts ) group by 1; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 36. 两表left outer join连接使用partition by位置语法 | ```sql {wrap} select t1.current, sum(t1.voltage) from ( d1001 t1 left outer join d1002 t2 on t1.ts = t2.ts ) partition by 1; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 37. selectlist包含聚集函数partition by查询子句包含interval | ```sql {wrap} select t1.`current` as `__fcol_0`, max(voltage) from `ts_5137`.`meters` as t1 partition by 1 interval(10m); ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 1. group by的group_expr的参数为单结果集列名 | ```sql {wrap} select t1.`current` as `__fcol_0` from `ts_5137`.`meters` as t1 group by __fcol_0; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 2. partition by的partition_expr的参数为单结果集列名 | ```sql {wrap} select t1.`current` as `__fcol_0` from `ts_5137`.`meters` as t1 partition by __fcol_0; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 3. group by的group_expr的参数为多结果集列名 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1` from `ts_5137`.`meters` as t1 group by __fcol_0, __fcol_1; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 4. partition by的partition_expr的参数为多结果集列名 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1` from `ts_5137`.`meters` as t1 partition by __fcol_0, __fcol_1; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 5. group by的group_expr的参数为结果集列名与列名混合 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 group by t1.`current`, __fcol_1; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 6. partition by的partition_expr的参数为结果集列名与列名混合 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 partition by t1.`current`, __fcol_1; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 7. group by的group_expr的参数为结果集列名与位置名混合 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`phase` as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 group by __fcol_0, 2; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 8. partition by的partition_expr的参数为结果集列名与位置名混合 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`phase` as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 partition by __fcol_0, 2; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 9. group by的group_expr的参数为结果集列名与别名混合且指向的是同一列 | ```sql {wrap} select t1.`current` as `__fcol_0`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 group by __fcol_0, 1; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 10. partition by的partition_expr的参数为结果集列名与别名混合且指向的是同一列 | ```sql {wrap} select t1.`current` as `__fcol_0`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 partition by __fcol_0, 1; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 1. group by的group_expr的参数为非聚集函数abs() | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, abs(t1.`phase`) as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 group by 1, 2, abs(t1.`phase`); ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 2. partition by的partition_expr的参数为非聚集函数abs() | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, abs(t1.`phase`) as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 partition by 1, 2, abs(t1.`phase`); ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 3. group by的group_expr的参数为非聚集函数round() | ```sql {wrap} select round(t1.`phase`) as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 group by round(t1.`phase`); ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 4. partition by的partition_expr的参数为非聚集函数round() | ```sql {wrap} select round(t1.`phase`) as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 partition by round(t1.`phase`); ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 5. group by的group_expr的参数为指向非聚集函数的位置参数 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, abs(t1.`phase`) as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 group by 1, 2, 3; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 6. partition by的partition_expr的参数为指向非聚集函数的位置参数 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, abs(t1.`phase`) as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 partition by 1, 2, 3; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 7. group by的group_expr的参数为指向非聚集函数的别名参数 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, abs(t1.`phase`) as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 group by 1, 2, `__fcol_2`; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 8. partition by的partition_expr的参数为指向非聚集函数的别名参数 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, abs(t1.`phase`) as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 partition by 1, 2, `__fcol_2`; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 1. group by的group_expr的参数为聚集函数max()的别名 | ```sql {wrap} select max(t1.`phase`) as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 group by `__fcol_2`; ``` | **报错，不支持** | **错误的查询语法** DB error: There mustn't be aggregation (0.000167s) | **通过** |
| 2. partition by的partition_expr的参数为聚集函数max()的别名 | ```sql {wrap} select max(t1.`phase`) as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 partition by `__fcol_2`; ``` | **报错，不支持** | **错误的查询语法** DB error: There mustn't be aggregation (0.000216s) | **通过** |
| 3. group by的group_expr的参数为聚集函数max()的位置参数 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, max(t1.`phase`) as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 group by 1, 2, 3; ``` | **报错，不支持** | **错误的查询语法** DB error: There mustn't be aggregation (0.000335s) | **通过** |
| 4. partition by的partition_expr的参数为聚集函数max()位置参数 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, max(t1.`phase`) as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 partition by 1, 2, 3; ``` | **报错，不支持** | **错误的查询语法** DB error: There mustn't be aggregation (0.000302s) | **通过** |
| 5. group by的group_expr的参数为聚集函数count(1)的别名 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, round(t1.`phase`) as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 group by 1, 2, 3, `__fcol_3`; ``` | **报错，不支持** | **错误的查询语法** DB error: There mustn't be aggregation (0.000364s) | **通过** |
| 6. partition by的partition_expr的参数为聚集函数count(1)的别名 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, round(t1.`phase`) as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 partition by 1, 2, 3, `__fcol_3`; ``` | **报错，不支持** | **错误的查询语法** DB error: There mustn't be aggregation (0.000399s) | **通过** |
| 7. group by的group_expr的参数为聚集函数count(1)的位置参数 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, round(t1.`phase`) as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 group by 1, 2, 3, 4; ``` | **报错，不支持** | **错误的查询语法** DB error: There mustn't be aggregation (0.000239s) | **通过** |
| 8. partition by的partition_expr的参数为聚集函数count(1)位置参数 | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, round(t1.`phase`) as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 partition by 1, 2, 3, 4; ``` | **报错，不支持** | **错误的查询语法** DB error: There mustn't be aggregation (0.000242s) | **通过** |
| 1. group by的group_expr的参数为聚集函数max() | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, max(t1.`phase`) as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 group by 1, 2, max(t1.`phase`); ``` | **报错，不支持** | **错误的查询语法** DB error: There mustn't be aggregation (0.000458s) | **通过** |
| 2. partition by的partition_expr的参数为聚集函数max() | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, max(t1.`phase`) as `__fcol_2`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 partition by 1, 2, max(t1.`phase`); ``` | **报错，不支持** | **错误的查询语法** DB error: There mustn't be aggregation (0.000331s) | **通过** |
| 3. group by的group_expr的参数为聚集函数count(1) | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 group by 1, 2, count(1); ``` | **报错，不支持** | **错误的查询语法** DB error: There mustn't be aggregation (0.000179s) | **通过** |
| 4. partition by的partition_expr的参数为聚集函数count(1) | ```sql {wrap} select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, count(1) as `__fcol_3` from `ts_5137`.`meters` as t1 partition by 1, 2, count(1); ``` | **报错，不支持** | **错误的查询语法** DB error: There mustn't be aggregation (0.000497s) | **通过** |
| 5. group by的group_expr是select子句的列且包含聚集函数 | ```sql {wrap} select subquery.`__fcol_0`, subquery.`__fcol_1`, subquery.`__fcol_2`, count(1) as `__fcol_3` from ( select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, abs(t1.`phase`) as `__fcol_2` from `ts_5137`.`meters` as t1 ) as subquery group by subquery.`__fcol_0`, subquery.`__fcol_1`, subquery.`__fcol_2`; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 6. partition by的partition_expr是select子句的列且包含聚集函数 | ```sql {wrap} select subquery.`__fcol_0`, subquery.`__fcol_1`, subquery.`__fcol_2`, count(1) as `__fcol_3` from ( select t1.`current` as `__fcol_0`, t1.`voltage` as `__fcol_1`, abs(t1.`phase`) as `__fcol_2` from `ts_5137`.`meters` as t1 ) as subquery partition by subquery.`__fcol_0`, subquery.`__fcol_1`, subquery.`__fcol_2`; ``` | **不报错，支持** | **正常查询无错误** | **通过** |

### 9.2 测试专项

| 测试用例说明 | 测试用例 | 预期结果 | 实际结果 | 测试结果 |
| --- | --- | --- | --- | --- |
| 使用 `GROUP BY` 和 `HAVING` | ```sql {wrap} SELECT name, AVG(voltage) as avg_voltage FROM ts_5137.meters GROUP BY name HAVING AVG(voltage) < 10; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 使用 `GROUP BY` 和 `ORDER BY` | ```sql {wrap} SELECT name, SUM(current) as total_current FROM ts_5137.meters GROUP BY name ORDER BY total_current DESC; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 使用 `GROUP BY` 和 `LIMIT` | ```sql {wrap} SELECT name, COUNT(*) as record_count FROM ts_5137.meters GROUP BY name ORDER BY record_count DESC LIMIT 2; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 常规的group by语句 | ```sql {wrap} SELECT groupId, MAX(voltage) AS max_voltage FROM ts_5137.meters GROUP BY groupId; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 带有having子句的查询语句 | ```sql {wrap} SELECT location, groupId, COUNT(*) AS record_count FROM ts_5137.meters GROUP BY location, groupId HAVING COUNT(*) > 10; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 带有avg聚合函数的group by和having的查询语句 | ```sql {wrap} SELECT groupId, AVG(voltage) AS avg_voltage, AVG(current) AS avg_current FROM ts_5137.meters GROUP BY groupId HAVING AVG(voltage) < 220 AND AVG(current) < 10; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| 带有max min聚合函数的group by和having的查询语句，其中having子句使用位置语法 | ```sql {wrap} SELECT location, MAX(current) - MIN(current) AS current_range FROM ts_5137.meters GROUP BY location HAVING 2 < 20; ``` | **不报错，支持** | **正常查询无错误，此处having不支持位置语法，仅仅是因为2<20表达式成立所以语句无错误** | **通过** |
| 带有max min聚合函数的group by和having的查询语句，其中having子句使用别名 | ```sql {wrap} SELECT location, MAX(current) - MIN(current) AS current_range FROM ts_5137.meters GROUP BY location HAVING current_range < 20; ``` | **报错，不支持** | **错误的查询语法** DB error: Invalid column name: current_range (0.002305s) | **通过** having 不支持别名 |
| group by和order by的查询语句 | ```sql {wrap} SELECT location, groupId, SUM(current) AS total_current FROM ts_5137.meters GROUP BY location, groupId ORDER BY total_current DESC; ``` | **不报错，支持** | **正常查询无错误** | **通过** |
| where子句嵌套select语句的查询语句 | ```sql {wrap} SELECT location, COUNT(*) AS count_above_avg FROM ts_5137.d1001 WHERE current > (SELECT AVG(current) FROM ts_5137.d1002) GROUP BY location; ``` | **报错，不支持** | **错误的查询语法** | **通过** where子句不支持嵌套select |
| 带有where子句和interval时间窗口的查询语句 | ```sql {wrap} SELECT groupId, MAX(current) AS max_current_last_hour FROM ts_5137.meters WHERE ts < NOW() GROUP BY groupId INTERVAL(1s); ``` | **报错，不支持** | **错误的查询语法** DB error: syntax error near "interval(1s);" (0.000173s) | **通过** group by 不支持 interval |
| 带有case when和state状态窗口的查询语句 | ```sql {wrap} SELECT tbname, _wstart, CASE WHEN voltage >= 205 and voltage <= 235 THEN 1 ELSE 0 END status FROM meters PARTITION BY tbname STATE_WINDOW(CASE WHEN voltage >= 205 and voltage <= 235 THEN 1 ELSE 0 END); ``` | **不报错，支持** | **正常查询无错误** | **通过** |

### 9.3 性能测试用例

#### 9.3.1 已有功能性能对比测试

原本就支持的查询语句性能测试对比，使用一亿条数据，共一万张表，分布在不同vgroup下

| 测试用例说明 | 测试用例 | 子表数固定为一万 vgroup变化 | 版本 | 总耗时seconds | avg (s) | min(s) | max(s) | p90(s) | p95 (s) | p99 (s) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 对比前版本 | 5.1820 | 0.411037s | 0.306480s | 0.453523s | 0.437712s | 0.446833s | 0.453523s |
| 对比版本 | 2.5000 | 0.147513s | 0.131966s | 0.179808s | 0.158901s | 0.159748s | 0.179808s |
| 对比前版本 | 7.2950 | 0.625586s | 0.530650s | 0.808814s | 0.642234s | 0.792300s | 0.808814s |
| 对比版本 | 7.5070 | 0.630243s | 0.416279s | 0.677324s | 0.647127s | 0.676570s | 0.677324s |
| 对比前版本 | 9.0980 | 0.800882s | 0.565829s | 0.879925 | 0.834833s | 0.846278s | 0.879925s |
| 对比版本 | 2.4730 | 0.144990s | 0.108878s | 0.175720s | 0.151424s | 0.154065s | 0.175720s |
| 对比前版本 | 128.6090 | 12.748588s | 11.799316s | 13.042057s | 13.023451s | 13.038158s | 13.042057s |
| 对比版本 | 142.7820 | 14.102716s | 13.189191s | 14.507415s | 14.259315s | 14.275587s | 14.507415s |
| 对比前版本 | 103.9040 | 10.206441s | 8.642587s | 13.426508s | 11.164040s | 11.580017s | 13.426508s |
| 对比版本 | 120.5370 | 11.646865s | 9.738008s | 14.313581s | 12.022790s | 12.099166s | 14.313581s |
| 对比前版本 | 112.1350 | 10.986183s | 7.695991s | 11.869992s | 11.045542s | 11.257272s | 11.869992s |
| 对比版本 | 117.6420 | 11.574898s | 10.695877s | 19.588892s | 12.056996s | 12.952150s | 19.588892s |
| 对比前版本 | 222.7650 | 21.966617s | 20.647871s | 22.410329s | 22.064854s | 22.202920s | 22.410329s |
| 对比版本 | 222.2170 | 22.017060s | 21.045394s | 22.744014s | 22.291626s | 22.379987s | 22.744014s |
| 对比前版本 | 222.7570 | 21.869719s | 14.200570s | 25.771229s | 23.851369s | 24.300001s | 25.771229s |
| 对比版本 | 221.9150 | 21.690530s | 17.897327s | 27.678127s | 24.349206s | 25.052577s | 27.678127s |
| 对比前版本 | 223.1650 | 21.917582s | 17.731887s | 32.259873s | 22.457907s | 23.148679s | 32.259873s |
| 对比版本 | 211.9690 | 20.804153s | 17.211202s | 30.560554s | 21.255390s | 21.859002s | 30.560554s |
| 对比前版本 | 4.0780 | 0.303764s | 0.266318s | 0.349175s | 0.319126s | 0.328851s | 0.349175s |
| 对比版本 | 2.5450 | 0.151988s | 0.134610s | 0.177433s | 0.156412s | 0.166712s | 0.177433s |
| 对比前版本 | 5.2330 | 0.413100s | 0.297464s | 0.561334s | 0.450740s | 0.463984s | 0.561334s |
| 对比版本 | 5.4030 | 0.430057s | 0.366738s | 0.639382s | 0.472048s | 0.491109s | 0.639382s |
| 对比前版本 | 6.1730 | 0.512527s | 0.397107s | 0.698116s | 0.522531s | 0.655650s | 0.698116s |
| 对比版本 | 2.4600 | 0.143205s | 0.106299s | 0.177733s | 0.143487s | 0.167883s | 0.177733s |
| 对比前版本 | 353.7230 | 35.089073s | 34.008435s | 36.924213s | 36.168144s | 36.206334s | 36.924213s |
| 对比版本 | 356.1410 | 35.463136s | 34.669938s | 35.996727s | 35.858798s | 35.962243s | 35.996727s |
| 对比前版本 | 357.7940 | 35.081146s | 27.666972s | 44.107893s | 40.070260s | 41.617492s | 44.107893s |
| 对比版本 | 366.2770 | 36.008059s | 29.130703s | 55.211645s | 39.643887s | 39.656023s | 55.211645s |
| 对比前版本 | 367.2180 | 36.288647s | 31.711867s | 40.192733s | 39.792821s | 39.941206s | 40.192733s |
| 对比版本 | 357.2320 | 35.203578s | 18.174907s | 40.142719s | 38.432130s | 39.981042s | 40.142719s |



| 测试用例说明 | 测试用例 | 子表数变化vgroup固定20 | 版本 | 总耗时seconds | avg (ms) | min(s) | max(s) | p90(s) | p95 (s) | p99 (s) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 对比前版本 | 2.2840 | 0.125848s | 0.085749s | 0.165532s | 0.156916s | 0.164750s | 0.165532s |
| 对比版本 | 7.8420 | 0.678052s | 0.621025s | 0.859317s | 0.744600s | 0.800925s | 0.859317s |
| 对比前版本 | 2.7690 | 0.173913s | 0.121865s | 0.243750s | 0.201711s | 0.221386s | 0.243750s |
| 对比版本 | 8.5660 | 0.742476s | 0.621624s | 0.937086s | 0.824657s | 0.862139s | 0.937086s |
| 对比前版本 | 89.1040 | 8.700589s | 5.637027s | 10.179829s | 9.486907s | 9.545955s | 10.179829s |
| 对比版本 | 106.1750 | 10.360544s | 9.298488s | 12.023369s | 11.669067s | 11.992311s | 12.023369s |
| 对比前版本 | 128.3990 | 12.445782s | 10.399948s | 15.456532s | 12.127232s | 12.456547s | 15.456532s |
| 对比版本 | 143.4870 | 13.934250s | 11.387063s | 19.454514s | 14.193405s | 17.444224s | 19.454514s |
| 对比前版本 | 217.5480 | 21.198851s | 17.477627s | 23.029941s | 21.929259s | 22.415322s | 23.029941s |
| 对比版本 | 220.7390 | 21.665118s | 17.123551s | 27.401771s | 23.492485s | 26.850747s | 27.401771s |
| 对比前版本 | 219.6100 | 21.567109s | 15.427709s | 28.366817s | 23.259077s | 23.295955s | 28.366817s |
| 对比版本 | 222.7920 | 21.859919s | 18.367643s | 28.677790s | 22.943062s | 24.238451s | 28.677790s |
| 对比前版本 | 2.1680 | 0.114399s | 0.104778s | 0.148426s | 0.142775s | 0.147744s | 0.147744s |
| 对比版本 | 5.3020 | 0.424438s | 0.406400s | 0.617186s | 0.450430s | 0.478275s | 0.617186s |
| 对比前版本 | 2.7120 | 0.168077s | 0.092853s | 0.222867s | 0.170117s | 0.180839s | 0.222867s |
| 对比版本 | 5.9250 | 0.485352s | 0.441941s | 0.598504s | 0.525276s | 0.588435s | 0.588435s |
| 对比前版本 | 397.9840 | 39.077862s | 37.381633s | 49.811794s | 39.472591s | 39.890549s | 49.811794s |
| 对比版本 | 417.1260 | 40.751820s | 37.465664s | 50.432977s | 41.029240s | 41.675467s | 50.432977s |
| 对比前版本 | 341.9300 | 33.638152s | 28.921222s | 52.472214s | 34.650657s | 37.091236s | 37.091236s |
| 对比版本 | 356.3360 | 34.983405s | 22.414251s | 47.456176s | 34.792267s | 38.829631s | 47.456176s |


#### 9.3.2 新增功能性能测试

| 测试用例说明 | 测试用例 | 子表数固定，vgroup变化 | 总耗时seconds | avg(s) | min(s) | max(s) | p90(s) | p95 (s) | p99 (s) |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| vgroup=10 | 346.9580 | 34.491170s | 29.633129s | 38.551113s | 36.863550s | 37.618715s | 38.551113s |
| vgroup=20 | 469.9040 | 46.570614s | 18.660264s | 53.914107s | 53.367036s | 53.614162s | 53.914107s |
| vgroup=40 | 408.9250 | 40.527473s | 22.463589s | 58.285655s | 57.395393s | 57.427582s | 58.285655s |
| vgroup=10 | 358.0520 | 35.690727s | 30.423152s | 40.417400s | 38.415331s | 39.022179s | 40.417400s |
| vgroup=20 | 323.9810 | 31.887577s | 21.169420s | 40.866759s | 33.246224s | 33.655124s | 40.866759s |
| vgroup=40 | 664.8650 | 65.932041s | 58.121706s | 75.852713s | 69.348042s | 75.094094s | 75.852713s |
| vgroup=10 | 1.5000 | 0.049129s | 0.045840s | 0.059583s | 0.049862s | 0.050239s | 0.059583s |
| vgroup=20 | 1.4060 | 0.039973s | 0.038244s | 0.041516s | 0.040801s | 0.040875s | 0.041516s |
| vgroup=40 | 1.450 | 0.044198s | 0.039765s | 0.049649s | 0.045177s | 0.045240s | 0.049649s |
| vgroup=10 | 1.4960 | 0.048388s | 0.045179s | 0.052903s | 0.049593s | 0.052320s | 0.052903s |
| vgroup=20 | 1.3890s | 0.038141s | 0.036261s | 0.040467s | 0.039732s | 0.039841s | 0.040467s |
| vgroup=40 | 1.4310 | 0.041111s | 0.038367s | 0.052216s | 0.042239s | 0.043931s | 0.052216s |
| vgroup=10 | 1.6930 | 0.068534s | 0.062016s | 0.078703s | 0.071849s | 0.072673s | 0.078703s |
| vgroup=20 | 1.5740 | 0.056356s | 0.053361s | 0.070685s | 0.056052s | 0.056154s | 0.070685s |
| vgroup=40 | 1.6640 | 0.064390s | 0.060748s | 0.071371s | 0.063966s | 0.066335s | 0.071371s |
| vgroup=10 | 1.7980 | 0.078410s | 0.072881s | 0.082555s | 0.079264s | 0.081254s | 0.082555s |
| vgroup=20 | 1.675 | 0.066404s | 0.060331s | 0.078881s | 0.070301s | 0.072267s | 0.078881s |
| vgroup=40 | 1.7280 | 0.071689s | 0.067157s | 0.085154s | 0.071247s | 0.074161s | 0.085154s |
| vgroup=10 | 2.5140 | 0.147594s | 0.128774s | 0.191799s | 0.153976s | 0.185997s | 0.191799s |
| vgroup=20 | 2.0530 | 0.100950s | 0.096076s | 0.125701s | 0.107424s | 0.111574s | 0.125701s |
| vgroup=40 | 2.0720 | 0.104875s | 0.085237s | 0.170316s | 0.112465s | 0.115567s | 0.170316s |

## 10. 问题

## 11. 测试计划 

2024-08-02、2024-08-05、2024-08-06、2024-08-07、2024-08-08

## 12. 测试备忘 

## 13. 参考文档

[group by 子句支持使用位置语法和结果集列名](https://taosdata.feishu.cn/wiki/RDTwwqZCbijla6khtG8c4IAXn7e)
