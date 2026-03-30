# AVEVA Historian Test（新增功能）

## 1. Objectives

- 通过 explorer 验证 AVEVA Historian 数据源的 transformer 功能
- 验证 AVEVA Historian 数据源断点续传功能

## 2. Revision History

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.01.29 | 0.1 | @贾晨阳 |  |
|  |  |  |  |

## 3. Scope

- 测试均在Explorer上进行，不涉及命令行模式
- 对于断点续传功能，分别在使用 agent 和不使用 agent 的条件下进行测试

## 4. 测试结论

本次测试的主要内容：
1. AVEVA Historian 数据源的 transformer 功能，包含extract-split 、filter、mapping，验证通过。
2. AVEVA Historian 数据源 数据迁移功能的断点续传能力，分别在使用/不使用agent的场景下，对同一任务进行多次启停，查看能够生成断点文件，任务总执行时间并无明显增长，数据也未损失，验证通过。

## 5. Limitations and Known Issues

无

## 6. Environment

- OS: Windows, Linux
- Browser: Chrome

## 7. Test Data

目标库中提前建表，超级表stb schema如下：
```sql {wrap}
taos> describe stb;
             field              |          type          |   length    |    note    |
=====================================================================================
 DateTime                       | TIMESTAMP              |           8 |            |
 Value                          | DOUBLE                 |           8 |            |
 vValue                         | VARCHAR                |         128 |            |
 Quality                        | INT                    |           4 |            |
 QualityDetail                  | INT                    |           4 |            |
 wwTagKey                       | INT                    |           4 |            |
 wwResolution                   | INT                    |           4 |            |
 StartDateTime                  | TIMESTAMP              |           8 |            |
 SourceTag                      | VARCHAR                |         128 |            |
 SourceServer                   | VARCHAR                |         128 |            |
 TagName                        | VARCHAR                |         128 | TAG        |
Query OK, 11 row(s) in set (0.001210s)

```


historian中的数据结构（使用 history 视图）：
```json {wrap}
 "parser": {
        "parse": {
            "DateTime": {"as": "timestamp"},
            "OPCQuality": {"as": "int"},
            "PercentGood": {"as": "double"},
            "Quality": {"as": "tinyint"},
            "QualityDetail": {"as": "int"},
            "SourceServer": {"as": "varchar(256)"},
            "SourceTag": {"as": "varchar(256)"},
            "StartDateTime": {"as": "timestamp"},
            "StateTime": {"as": "double"},
            "TagName": {"as": "varchar(256)"},
            "Value": {"as": "double"},
            "vValue": {"as": "varchar(4000)"},
            "wwCycleCount": {"as": "int"},
            "wwEdgeDetection": {"as": "varchar(16)"},
            "wwExpression": {"as": "varchar(4000)"},
            "wwFilter": {"as": "varchar(512)"},
            "wwInterpolationType": {"as": "varchar(20)"},
            "wwMaxStates": {"as": "int"},
            "wwOption": {"as": "varchar(512)"},
            "wwParameters": {"as": "varchar(128)"},
            "wwQualityRule": {"as": "varchar(20)"},
            "wwResolution": {"as": "int"},
            "wwRetrievalMode": {"as": "varchar(16)"},
            "wwRowCount": {"as": "int"},
            "wwStateCalc": {"as": "varchar(20)"},
            "wwTagKey": {"as": "int"},
            "wwTimeDeadband": {"as": "int"},
            "wwTimeStampRule": {"as": "varchar(20)"},
            "wwTimeZone": {"as": "varchar(50)"},
            "wwUnit": {"as": "varchar(512)"},
            "wwValueDeadband": {"as": "double"},
            "wwValueSelector": {"as": "varchar(128)"},
            "wwVersion": {"as": "varchar(30)"}
        }
    },
```

## 8. Test Cases

### 8.1 Functional

在提测时，开发应保证sanity类型的用例全部通过。
| Type | Use Agent ? | Description | Expected Results | Result | Automated | Jira | Memo |
| --- | --- | --- | --- | --- | --- | --- | --- |
| transformer | N | 结果示例：3列value分别为3.140000104904175，52，395.177001953125
配置：
filter: value< 1000 | 过滤生效后结果示例中仅保留value=395.177001953125所在列 | Pass |  |  |  |
|  |  | 结果示例：3列wwTagKey分别为 217,218,227
配置：
filter：wwTagKey >220 | 过滤生效后结果示例中仅保留wwTagKey=227所在列的数据 | Pass |  |  |  |
|  |  | mapping：
Datetime mapping Startdatetime | 结果示例中datetime映射到超级表stb中的Startdatetime列，写入TDengine中的时间列均为historian中Startdatetime列的值 | Pass |  |  |  |
|  |  | mapping:
子表名称配置为 tb_{TagName} | 结果示例中子表名满足tb_{TagName}命名规则，TDengine中创建的子表同样满足命名规则 | Pass |  |  |  |
|  |  | mapping：
value列设置为常量 100 | 结果示例中value列的值为100，TDengine中每个子表下value列的值均为100 | Pass |  |  |  |
|  |  | mapping：
wwResolution列设置为 sum：Value+wwTagKey | 结果示例中wwResolution列的值满足设置，TDengine中写入的数据对应列值也满足设置 | Pass |  |  |  |
| 断点续传 | Y | 启动history表数据迁移后，对任务进行stop操作，删除TDengine中对应表中的数据；重新启动任务。 | 任务完成后，TDengine中只有重新启动任务后写入的数据；stop之前写入的数据不会再次写入 | Pass |  |  |  |
|  | N |  |  | Pass |  |  |  |

### 8.2 Usability

### 8.3 Reliability

| Type | Use Agent ? | Description | Expected Results | Result | Automated | Jira | Memo |
| --- | --- | --- | --- | --- | --- | --- | --- |
|  | Y | 执行history表数据迁移，迁移过程中停止taosx进程再恢复 | 任务总时间和单次完整迁移的时间基本相当，未出现较大的时间差距 | Pass |  |  |  |
|  | Y | 执行history表数据迁移，迁移过程中停止agent进程再恢复 | 任务状态由running切换为waiting，agent恢复后，任务状态恢复为running，累计任务执行时间与不暂停任务的执行时间基本相等 | Pass |  |  |  |

### 8.4 Performance

无

### 8.5 Security

无

### 8.6 Compatibility

historian 数据源为 taosx v1.5.0 新增数据源，不存在兼容性问题。

### 8.7 Localization

无

## 9. Questions

无

## 10. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: historian，taosx

TD-28669



## 11. Schedule

这里用于计划此feature测试的开始和结束时间。
计划开始时间：2024-2-20
计划结束时间：2024-2-26

## 12. Notes

无。

## 13. Summary

见第4章节

## 14. Reference

用户手册文档：[AVEVA Historian Source](https://taosdata.feishu.cn/wiki/R92NwYTvKiL84Gk4qVdcTtGMnjb) 
AVEVA Historian 使用手册：[AVEVA™ Historian 2020.R2.SP1 Research Report](https://taosdata.feishu.cn/wiki/TjYfwPHo0iUr5JkWr3Ic3lhpndc)
