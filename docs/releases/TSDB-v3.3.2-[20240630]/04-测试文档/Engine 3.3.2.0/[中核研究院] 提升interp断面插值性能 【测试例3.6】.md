# [中核研究院] 提升interp断面插值性能 【测试例3.6】

## 1. 测试目标

根据 [TS-4864](https://jira.taosdata.com:18080/browse/TS-4864) 要求，以100并发任务数量查询单个不同测点前1天0点的历史断面值。

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024-06-13 | 0.1 | guoxy | New |
|  |  |  |  |

## 3. 测试范围

以100并发任务数量查询单个不同测点前1天0点的历史断面值。
统计各次查询耗时、CPU占用率、内存占用率、IO占用率。

## 4. 测试结论

在788.4亿数据量情况，100并发的QPS达到2145.923，查询平均耗时在0.039784s，CPU占到62%，内存从12.8G升到13.1G，IO读写忽略不计达标应该问题不大。具体见7-测试场景1。

同时在这个数据量上，进行了扩展测试，可以作为其他PK测试的参考。
1、将字表换成超级表，查询耗时变成s，这个和panwei沟通是合理的，因为要将这几百亿的数据进行排序，所以很慢，也没有客户对超级表进行interp，都是针对子表的interp，所以该项未进行并发查询。具体见7-测试场景1。
2、range的范围从1个时间点扩展到1天-->7天-->1月，
查询耗时从 0.039784s --> 0.270268s --> 1.495802s -->  7.407176s ；
QPS从 2145.923 --> 359.790  --> 65.632  --> 13.343；
CPU从62%升到100%；
内存基本在13.1G不变；
IO读写可以忽略不计。
具体见7-扩展测试场景1、2、3。

## 5. 测试数据

和肖总沟通，构造的测试数据如下
schema：1列ts，3列float，5列bool，10列tag
```sql
taos> describe meters;
             field              |          type          |   length    |        note        |     encode     |    compress    |     level      |
================================================================================================================================================
 ts                             | TIMESTAMP              |           8 |                    | delta-i        | lz4            | medium         |
 boolcol                        | BOOL                   |           1 |                    | bit-packing    | lz4            | medium         |
 boolcol_1                      | BOOL                   |           1 |                    | bit-packing    | lz4            | medium         |
 boolcol_2                      | BOOL                   |           1 |                    | bit-packing    | lz4            | medium         |
 boolcol_3                      | BOOL                   |           1 |                    | bit-packing    | lz4            | medium         |
 boolcol_4                      | BOOL                   |           1 |                    | bit-packing    | lz4            | medium         |
 phase                          | FLOAT                  |           4 |                    | delta-d        | lz4            | medium         |
 phase_1                        | FLOAT                  |           4 |                    | delta-d        | lz4            | medium         |
 phase_2                        | FLOAT                  |           4 |                    | delta-d        | lz4            | medium         |
 groupid                        | TINYINT                |           1 | TAG                | disabled       | disabled       | disabled       |
 groupid_1                      | TINYINT                |           1 | TAG                | disabled       | disabled       | disabled       |
 groupid_2                      | TINYINT                |           1 | TAG                | disabled       | disabled       | disabled       |
 groupid_3                      | TINYINT                |           1 | TAG                | disabled       | disabled       | disabled       |
 groupid_4                      | TINYINT                |           1 | TAG                | disabled       | disabled       | disabled       |
 location                       | VARCHAR                |          16 | TAG                | disabled       | disabled       | disabled       |
 location_1                     | VARCHAR                |          16 | TAG                | disabled       | disabled       | disabled       |
 location_2                     | VARCHAR                |          16 | TAG                | disabled       | disabled       | disabled       |
 location_3                     | VARCHAR                |          16 | TAG                | disabled       | disabled       | disabled       |
 location_4                     | VARCHAR                |          16 | TAG                | disabled       | disabled       | disabled       |
Query OK, 19 row(s) in set (0.001927s)

```

其他数据信息**：**
数据库32个vgroup，1000个子表，每个子表需要1年数据，每秒2.5条记录，因此单表记录是60*60*24*2.5*365=78840000条，超级表一共78840000*1000=78840000000=788.4亿。
```sql

taos> select count(*) from meters;
       count(*)        |
========================
           78840000000 |
Query OK, 1 row(s) in set (5.573409s)
```

测试json：
```sql
{
    "filetype": "insert",
    "cfgdir": "/data/zhonghe/cfg3/cfg1",
    "host": "u1-64",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "connection_pool_size": 8,
    "thread_count": 100,
    "create_table_thread_count": 8,
    "result_file": "./insert_res.txt",
    "confirm_parameter_prompt": "no",
    "insert_interval": 0,
    "interlace_rows": 1000,
    "num_of_records_per_req": 1000,
    "prepared_rand": 10000,
    "chinese": "no",
    "databases": [
        {
            "dbinfo": {
                "name": "zhonghe",
                "drop": "yes",
                "replica": 1,
                "precision": "ms",
                "vgroups": 32,
                "keep": 3650,
                "minRows": 100,
                "maxRows": 4096,
                "cachemodel": "'both'",
                "WAL_LEVEL": 0,
                "comp": 2
            },
            "super_tables": [
                {
                    "name": "meters",
                    "child_table_exists": "no",
                    "childtable_count": 1000,
                    "childtable_prefix": "m",
                    "escape_character": "yes",
                    "auto_create_table": "no",
                    "batch_create_tbl_num": 5,
                    "data_source": "rand",
                    "insert_mode": "stmt",
                    "non_stop_mode": "no",
                    "line_protocol": "line",
                    "insert_rows": 78840000,
                    "childtable_limit": 10,
                    "childtable_offset": 100,
                    "interlace_rows": 0,
                    "insert_interval": 0,
                    "partial_col_num": 0,
                    "disorder_ratio": 0,
                    "disorder_range": 1000,
                    "timestamp_step": 400,
                    "start_timestamp": "2023-07-01 00:00:00.000",
                    "sample_format": "csv",
                    "sample_file": "./sample.csv",
                    "use_sample_ts": "no",
                    "tags_file": "",
                    "columns": [
                        { "type": "BOOL", "name": "boolcol","count": 5, "max": 1, "min": 0 },
                        { "type": "FLOAT", "name": "phase","count": 3, "max": 1, "min": 0 }
                    ],
                    "tags": [
                        {
                            "type": "TINYINT",
                            "name": "groupid",
                            "count": 5,
                            "max": 10,
                            "min": 1
                        },
                        {
                            "name": "location",
                            "type": "BINARY",
                            "count": 5,
                            "len": 16,
                            "values": ["San Francisco", "Los Angles", "San Diego",
                                "San Jose", "Palo Alto", "Campbell", "Mountain View",
                                "Sunnyvale", "Santa Clara", "Cupertino"]
                        }
                    ]
                }
            ]
        }
    ]
}


```


## 6. 测试环境

- OS：Ubuntu 20.04.2 LTS
- Env：

| **硬件环境** | **IP** | 用途 | **CPU** | **内存** | **硬盘** |
| --- | --- | --- | --- | --- | --- |
| **测试服务器** | 192.168.1.64 | taosBenchmark、taosd | Intel(R) Xeon(R) CPU E5-2650 v3 @ 2.30GHz 40核 | 256G | PERC H730 Mini 446G*2 |


## 7. 性能测试结果

#### 7.0.1 测试场景1:

测试sql：前1天0点断面值。
range为一个时间点，即 range('2024-01-01 00:00:00.000','2024-01-01 00:00:00.000')
select interp(phase) from m1 range('2024-01-01 00:00:00.000','2024-01-01 00:00:00.000') every(1d) fill(linear);
```sql {wrap}
taos> select interp(phase) from m1 range('2024-01-01 00:00:00.000','2024-01-01 00:00:00.000') every(1d) fill(linear);
    interp(phase)     |
=======================
            0.5210000 |
Query OK, 1 row(s) in set (0.011969s)
```

并发查询100次，执行50次。

| 平均耗时(s) | p95耗时(s) | p99耗时(s) | QPS | CPU | 内存（G） | IO-read | IO-write | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0.039784s | 0.006335s | 0.012068s | 2145.923 | 62% | 13.1G | 0 | 4096B | 内存基础占用12.8G |

如果将字表换成超级表
查询一次
```sql {wrap}
taos> select interp(phase) from meters range('2024-01-01 00:00:00.000','2024-01-01 00:00:00.000') every(1d) fill(linear);
    interp(phase)     |
=======================
            0.5210000 |
Query OK, 1 row(s) in set (36511.307750s)
```


| 耗时(s) | CPU | 内存（G） | IO-read-max | IO-write-max | 备注 |
| --- | --- | --- | --- | --- | --- |
| 36511.307750s | 74%，后面稳定在7% | 14.9G | 130M | 808M | 内存基础占用12.9G |


#### 7.0.2 扩展测试场景1:

测试sql：前1天0点断面值。range为1天
 range('2024-01-01 00:00:00.000','2024-01-02 00:00:00.000')
select interp(phase) from m1 range('2024-01-01 00:00:00.000','2024-01-02 00:00:00.000') every(1d) fill(linear);
```sql {wrap}
taos>  select interp(phase) from m1 range('2024-01-01 00:00:00.000','2024-01-02 00:00:00.000') every(1d) fill(linear);
    interp(phase)     |
=======================
            0.5210000 |
            0.5210000 |
Query OK, 2 row(s) in set (0.063473s)
```

并发查询100次，执行50次。

| 平均耗时(s) | p95耗时(s) | p99耗时(s) | QPS | CPU | 内存（G） | IO-read | IO-write | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0.270268s | 0.062248s | 0.172218s | 359.790 | 99% | 13.1G | 0 | 2556k | 内存基础占用12.8G |

#### 7.0.3 扩展测试场景2:

测试sql：前1天0点断面值。range为7天
 range('2024-01-01 00:00:00.000','2024-01-07 00:00:00.000')
select interp(phase) from m1 range('2024-01-01 00:00:00.000','2024-01-07 00:00:00.000') every(1d) fill(linear);
```sql {wrap}
taos> select interp(phase) from m1 range('2024-01-01 00:00:00.000','2024-01-07 00:00:00.000') every(1d) fill(linear);
    interp(phase)     |
=======================
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
Query OK, 7 row(s) in set (0.295779s)
```

并发查询100次，执行50次。

| 平均耗时(s) | p95耗时(s) | p99耗时(s) | QPS | CPU | 内存（G） | IO-read | IO-write | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1.495802s | 0.801668s | 0.323113s | 65.632 | 100% | 13.0G | 0 | 1612k | 内存基础占用12.6G |

#### 7.0.4 扩展测试场景3:

测试sql：前1天0点断面值。range为1月
 range('2024-01-01 00:00:00.000','2024-01-31 00:00:00.000')
select interp(phase) from m1 range('2024-01-01 00:00:00.000','2024-01-31 00:00:00.000') every(1d) fill(linear);
```sql {wrap}
taos> select interp(phase) from m1 range('2024-01-01 00:00:00.000','2024-01-31 00:00:00.000') every(1d) fill(linear);
    interp(phase)     |
=======================
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
            0.5210000 |
Query OK, 31 row(s) in set (1.381662s)
```

并发查询100次，执行50次。

| 平均耗时(s) | p95耗时(s) | p99耗时(s) | QPS | CPU | 内存（G） | IO-read | IO-write | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 7.407176s | 1.505851s | 5.614074s | 13.343 | 100% | 13.1G | 0 | 1596k | 内存基础占用12.9G |
