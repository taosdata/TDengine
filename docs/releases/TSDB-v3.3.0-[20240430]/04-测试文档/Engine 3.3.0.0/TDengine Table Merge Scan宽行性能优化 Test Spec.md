# TDengine Table Merge Scan宽行性能优化 Test Spec

## 1. 测试目标

- 验证新的性能优化对原有查询性能无影响
- 验证特定的场景下使用新算法查询性能得到优化，目前无具体优化目标，实际情况看测试结果

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
|  |  |  |  |

## 3. 测试结论

1. 算法触发边界值为256字节
2. 分别在不同数据量级下进行性能测试对比，最小性能提升比率为20.9%，最大性能提升比率为35%，平均提升比率为28.6%
3. 新算法受vgroup数量的影响规则：同数量级的数据，随着vgroup数量的增加（每个vgroup平均数据量的减少）性能下降。当每个vgroup的平均数据量小于5000W（其为估计值，该值受列宽和行数的共同影响）时，新算法比旧算法耗时更长，在5.4场景中性能降低16.9%
4. 新算法对普通列（非ts主键列）性能无提升
5. 新算法基于3亿数据查询过程中对磁盘的最大消耗较3.2.3.0版本有33.3%的减低，但仍然占用90G的本地磁盘

## 4. 开发质量报告

结论：本特性/优化的开发质量是（优，良，一般，差，很差）

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 （测试阻塞，无法进行） | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 0 |
| 严重 Bug 总数 | 0 |

## 5. 已知问题和限制

## 6. 测试资源及环境

   测试平台：Linux x64
   测试资源：

## 7. 测试范围及重点

- 验证新的性能优化对原有查询性能无影响
- 验证特定的场景下使用新算法查询性能得到优化，目前无具体优化目标，实际情况看测试结果

## 8. 测试数据 (Optional)

### 8.1 数据库配置

| 参数名称 | 参数值 |
| --- | --- |
| wal_retention_period | 1 |
| wal_retention_size | 1 |
| cachemodel | both |
| replica | 1 |
| vgroups | 2 |

### 8.2 taosBenchmark对应json文件

?表示变量，根据不同场景需要特殊配置
Json-1
```json
{
    "filetype": "insert",
    "cfgdir": "/etc/taos",
    "host": "127.0.0.1",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "thread_count": 10,
    "result_file": "./case_duration.txt",
    "confirm_parameter_prompt": "no",
    "check_sql": "no",
    "continue_if_fail": "yes",
    "databases": [
        {
            "dbinfo": {
                "name": "test",
                "drop": "yes",
                "vgroups": ?,
                "replica": 1,
                "precision": "ms",
                "cachemodel":"'both'",
                "wal_retention_period":1,
                "wal_retention_size":1
            },
            "super_tables": [
                {
                    "name": "meters",
                    "child_table_exists": "no",
                    "childtable_count": ?,
                    "insert_rows": ?,
                    "childtable_prefix": "ctb",
                    "insert_mode": "taosc",
                    "timestamp_step": 1,
                    "start_timestamp":"2021-04-19 00:00:00.000",
                    "columns": [
                        { "type": "binary",      "name": "bin245", "len": 245},
                        { "type": "binary",      "name": "bin246", "len": 246}
                    ],
                    "tags": [
                        {
                            "type": "int",
                            "name": "groupid",
                            "max": 10,
                            "min": 1
                        }
                    ]
                }
          ]
      }
   ]
}
```

Json-2
```json
{
    "filetype": "insert",
    "cfgdir": "/etc/taos",
    "host": "127.0.0.1",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "thread_count": 10,
    "result_file": "./case_duration.txt",
    "confirm_parameter_prompt": "no",
    "check_sql": "no",
    "continue_if_fail": "yes",
    "databases": [
        {
            "dbinfo": {
                "name": "test",
                "drop": "yes",
                "vgroups": ?,
                "replica": 1,
                "precision": "ms",
                "cachemodel":"'both'",
                "wal_retention_period":1,
                "wal_retention_size":1
            },
            "super_tables": [
                {
                    "name": "meters",
                    "child_table_exists": "no",
                    "childtable_count": ?,
                    "insert_rows": ?,
                    "childtable_prefix": "ctb",
                    "insert_mode": "taosc",
                    "timestamp_step": 1,
                    "start_timestamp":"2021-04-19 00:00:00.000",
                    "columns": [
                        { "type": "int",         "name": "ic",  "max": 100, "min": 0 },
                        { "type": "binary",      "name": "bin23", "len": 23},
                        { "type": "binary",      "name": "bin24", "len": 24},
                        { "type": "binary",      "name": "bin31", "len": 31},
                        { "type": "binary",      "name": "bin32_1", "len": 32},
                        { "type": "binary",      "name": "bin32_2", "len": 32},
                        { "type": "binary",      "name": "bin32_3", "len": 32},
                        { "type": "binary",      "name": "bin32_4", "len": 32},
                        { "type": "binary",      "name": "bin32_5", "len": 32},
                        { "type": "binary",      "name": "bin32_6", "len": 32},
                        { "type": "binary",      "name": "bin32_7", "len": 32},
                        { "type": "binary",      "name": "bin32_8", "len": 32},
                        { "type": "nchar",       "name": "nchar64", "len": 64}
                    ],
                    "tags": [
                        {
                            "type": "int",
                            "name": "groupid",
                            "max": 10,
                            "min": 1
                        }
                    ]
                }
          ]
      }
   ]
}
```

Json-3
```json
{
    "filetype": "insert",
    "cfgdir": "/etc/taos",
    "host": "127.0.0.1",
    "port": 6030,
    "user": "root",
    "password": "taosdata",
    "thread_count": 10,
    "result_file": "./case_duration.txt",
    "confirm_parameter_prompt": "no",
    "check_sql": "no",
    "continue_if_fail": "yes",
    "databases": [
        {
            "dbinfo": {
                "name": "test",
                "drop": "yes",
                "vgroups": 8,
                "replica": 1,
                "precision": "ms",
                "cachemodel":"'both'",
                "wal_retention_period":1,
                "wal_retention_size":1
            },
            "super_tables": [
                {
                    "name": "meters",
                    "child_table_exists": "no",
                    "childtable_count": 1000,
                    "insert_rows": 10000,
                    "childtable_prefix": "ctb",
                    "insert_mode": "taosc",
                    "timestamp_step": 1,
                    "start_timestamp":"2021-04-19 00:00:00.000",
                    "columns": [
                        { "type": "int",         "name": "ic",  "max": 100, "min": 0 },
                        { "type": "binary",      "name": "bin32_8", "len": 1024},
                        { "type": "nchar",       "name": "nchar64", "len": 512, "count": 10}
                    ],
                    "tags": [
                        {
                            "type": "int",
                            "name": "groupid",
                            "max": 10,
                            "min": 1
                        }
                    ]
                }
          ]
      }
   ]
}
```

## 9. 测试用例

### 9.1 查询sql

```sql
select col1, col2, col3 from meters order by ts [desc]
select /*+ smalldata_ts_sort() */ col1, col2, col3 from meters order by ts [desc]
```

### 9.2 测试场景

| 测试场景 | No. | Json | 列 | 表数 | 每表行数 | 总行数 | vgroup数量 | v3.2.3.0 | 定义smalldata_ts_sort() | 未定义smalldata_ts_sort() | 性能提高百分比 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 多列长度为255，不触发新算法 | 1 | Json-1 | select ts, bin245 from meters order by ts | 10000 | 10000 | 1亿 | 1 | N/A | N/A | 201.267971s | N/A |  |
| 多列长度为256，触发新算法 | 2 | Json-1 | select ts, bin246 from meters order by ts | 10000 | 10000 | 1亿 | 1 | N/A | N/A | 127.281282s | N/A |  |
| 3 | Json-2 | 10000 | 10000 | 1亿 | 1 | N/A | 304.662871s 297.353435s 307.201018s --- Avg: 303.072441s | 220.715197s 221.369647s 223.468837s --- Avg: 221.851227s | 26.8% |  |
| 4 | Json-2 | 15000 | 10000 | 1.5亿 | 1 | N/A | 467.990662s 475.450782s 468.333956s --- Avg: 470.5918s | 333.577433s 332.411700s 328.614137s --- Avg: 331.534423s | 29.5% |  |
| 5.1 | Json-2 | 1 | N/A | 1042.046592s 943.099267s 947.199269s --- Avg: 977.448376s | 742.036424s 662.321225s 650.559357s --- Avg: 684.972335 | 29.93% |  |
| 5.2 | Json-2 | 2 | N/A | 843.433494s 797.567576s 740.497191s --- Avg: 793.832753s | 517.118406s 513.610426s 507.468128s --- Avg: 512.73232s | 35.4% |  |
| 5.3 | Json-2 | 4 | N/A | ~~772.853103s~~ 802.768293s ~~838.175588s~~ 821.072963s 781.424934s 806.406002s --- Avg: 802.918048s | ~~501.944717s~~ 703.654588s ~~732.035029s~~ 619.091590s 551.651758s 665.895844s --- Avg: 635.073445s | 20.9% |  |
| 5.4 | Json-2 | 8 | N/A | 551.749851s 597.011911s 505.320071s ~~716.532516s~~ 511.911773s ~~499.228280s~~ 510.750621s 580.571166s --- Avg: 542.885899 | 523.995562s ~~822.533902s~~ 791.806238s 707.351292s 635.456264s ~~492.945486s~~ 542.352104s 607.389886s --- Avg: 634.725224 | -16.9% |  |
| 6 | Json-2 | 10000 | 15000 | 1.5亿 | 1 | N/A | 458.084598s 448.142287s 446.711261s --- Avg: 450.979382s | 320.261731s 316.953524s 319.704557s --- Avg: 318.97327s | 29.3% |  |
| 7 | Json-2 | 10000 | 30000 | 3亿 | 1 | 1509.963632s 1032.409902s | ~~1139.788408s~~ 937.523848s 938.030094s 951.146944s ~~914.821437s~~ --- Avg: 942.233629s | 732.303178s ~~776.765218s~~ 653.518779s ~~648.927183s~~ 651.602644s --- Avg: 679.141534s | 28.9% |  |
| 8 | Json-2 | select ts, ic, bin32_1, bin32_2,bin32_3, bin32_4, bin32_5, bin32_6, bin32_7, bin32_8 from meters order by ts | 10000 | 10000 | 1亿 | 1 | N/A | 343.258881s 330.130236s 330.169819s --- Avg: 334.519645s | 219.940656s 228.534866s 224.120923s --- Avg: 224.198815s | 32.9% |  |
|  | 9 | Json-2 | select ts, ic, nchar64 from meters order by ic | 10000 | 30000 | 3亿 | 1 | N/A | 1138.508862s 1157.861903s 1149.720808s | 1132.444409s 1133.382580s 1139.258584s | 无提升 |  |
|  | 10 | Json-3 | select ts, ic, bin32_8, nchar64,nchar64_1,nchar64_2,nchar64_3,nchar64_4,nchar64_5,nchar64_6 from meters order by ts; | 1000 | 10000 | 1000W | 8 | N/A | 1777.315980s | 1240.089336s | 30.2% |  |

### 9.3 测试数据对比

| 场景ID | 测试用例 | 结论 |
| --- | --- | --- |
| 1、2 | 验证新算法触发边界值为256字节 | 通过 |
| 3、4、5.1 | 表数量增加时，新旧算法的性能对比 | 表数量增加对性能的提升无影响 |
| 3、6、7 | 表行数增加时，新旧算法的性能对比 | 表行数增加对性能的提升无影响 |
| 5.1、5.2、5.3 | vgroup数量增加时，新旧算法的性能对比 | 新算法受每个vgroup的平均数据行数影响较大，同样数据量级，随着vgroup的数量增加，性能提升明显下降，从最高的35%下降到20% |
| 3、8 | 查询列数增加时，新旧算法的性能对比 | 列宽度增加时性能提升效果更好 |
| 7、9 | ts列和普通数据类order by的效果 | 新算法对普通列的order by场景性能无提升 |
| 7 | 基于3.2.3.0和最新的3.0分支对比查询过程，本次磁盘使用量 | 3.2.3.0 查询消耗磁盘：130G 3.0分支查询消耗磁盘：90G |

## 10. 问题(Optional)

这里用于记录需要讨论的问题：
- 暂无

## 11. Jira

## 12. 测试计划 (Optional)

## 13. 测试备忘 (Optional)

## 14. 参考文档 (Optional)

[Table Merge Scan宽行性能优化](https://taosdata.feishu.cn/wiki/D2HiwsVb8ivp63kTQg5cjCA3nzb)

TD-26207
