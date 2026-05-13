# taosX 监控 Test

## 1. Objectives

- 验证taosX 监控功能

## 2. Revision History

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.01.16 | 0.1 | @聂敏慧 | Initial Draft |
|  |  |  |  |

## 3. Scope

- taosx进程 , taosx-agent 进程, 各连接器进程的监控
- 数据源包括：TDengine 2.x 数据源，TDengine 3.x 数据源，使用 IPC 传输数据的数据源，包括 InfluxDB，OpenTSDB，OPC，PI，MQTT，Kafka，CSV，Historian
- 各类数据源的监控指标在TDinsight上测试，不涉及命令行

## 4. 测试结论

1. 在 taosx.toml 中打开 monitor 配置后，可以通过 tdinsight 监控 taosx 进程，taosx-agent 进程以及各数据源的任务信息。
- taosx 监控包括系统信息 (CPU cores 和 Total Memory), Uptime,  Restart Times, CPU usage，Memory usage, Running tasks, Failed Tasks 和 Completed Tasks， 测试通过。 
- taosx-agent 监控包括系统信息 (CPU cores 和 Total Memory), Uptime, CPU usage，Memory usage，测试通过。
- TDengine 2 数据源任务的监控包括TDengine 2任务信息列表，Inserted rows Rate 和 Inserted Points Rate，测试通过。
- TDengine 3 数据源任务的监控包括TDengine 3任务信息列表，Inserted rows Rate，Inserted Points Rate 和 Write Raw Fails，测试通过。
- 使用 IPC 传输数据的外部数据源任务的监控包括任务信息列表，Inserted rows Rate，Inserted Points Rate, Processed/Received Batches, Failed Sqls, Connector CPU Percent, Connector Memory Percent, Connector Disk Read Rate, Connector Disk Write Rate, 测试通过
- 使用 IPC 传输数据的内部数据源任务的监控包括任务信息列表，Inserted rows Rate，Inserted Points Rate, Processed/Received Batches, Failed Sqls, 测试通过
1. 性能： 通过 kafka 数据源的数据接入任务进行测试，开启监控对性能无影响

## 5. Limitations and Known Issues

这里用于记录产品使用上的一些限制，包括不支持的场景等，以及在发版时没有解决的minor issues.
1. 当任务运行时间较短时（小于监控周期），最终上报的执行时间与执行时间可能差距较大 （[TD-28694](https://jira.taosdata.com:18080/browse/TD-28694)）

## 6. Environment

- OS: Windows, Linux
- Browser: Chrome

## 7. Test Data

N/A

## 8. Test Cases

### 8.1 Functional

#### 8.1.1 功能测试用例：

1. taosx 进程的监控
2. taosx-agent 进程的监控 
3. 各连接器进程的监控
4. 各任务指标的监控

#### 8.1.2 测试记录：

|  | Description | Expected Results | Result | Jira | Memo |
| --- | --- | --- | --- | --- | --- |
|  | （sanity）taosx 配置的验证（使用正确的配置） | taosx 成功发送数据到 taoskeeper | Pass |  |  |
|  | taosx 配置的验证（配置错误，缺少配置） | 有默认值的使用默认值 配置错误时有错误提示信息 | Pass |  |  |
| taosx 监控数据正确 | Pass |  |  |
| agent 监控数据正确 | Pass |  |  |
| agent 筛选正确，任务筛选正确 | Pass |  |  |
| TDengine2.x 数据源的任务监控数据正确，在选定的时间段显示监控数据正确 | Pass |  |  |
| TDengine3.x 数据源的任务监控数据正确，在选定的时间段显示监控数据正确 | Pass |  |  |
| IPC 传输数据的数据源的任务监控数据正确，在选定的时间段显示监控数据正确 | Pass |  |  |
| taosx 监控数据正确 | Pass |  |  |
| 任务筛选正确 | Pass |  |  |
| TDengine2.x 数据源的任务监控数据正确，在选定的时间段显示监控数据正确 | Pass |  |  |
| TDengine3.x 数据源的任务监控数据正确，在选定的时间段显示监控数据正确 | Pass |  |  |
| IPC 传输数据的数据源的任务监控数据正确，在选定的时间段显示监控数据正确 测试 mqtt 数据源 测试 kafka 数据源 | Pass |  |  |
| taosx 监控数据正确 | Pass |  |  |
| agent 监控数据正确 | Pass |  |  |
| agent 筛选正确，任务筛选正确 | Pass |  |  |
| IPC 传输数据的数据源的任务监控数据正确，在选定的时间段显示监控数据正确 | Pass |  |  |
| taosx 监控数据正确 | Pass |  |  |
| agent 监控数据正确 | Pass |  |  |
| taosx 筛选正确 agent 筛选正确 任务筛选正确 | Pass |  |  |
| IPC 传输数据的数据源的任务监控数据正确，在选定的时间段显示监控数据正确 | Pass |  |  |
|  | taosx 停止 | 所有的监控数据不再更新 | Pass |  |  |
|  | agent 停止或者断开 | tdinsight中没有这个agent的监控数据 | Pass |  |  |

### 8.2 Usability

测试用例包括但不局限于：
- UI 是否美观
- 交互是否合理
- 字体、字号是否合适
- 是否存在错别字
- 坐标或图标的单位是否合理，是否正确标注

### 8.3 Reliability

N/A

### 8.4 Performance

N/A

### 8.5 Security

N/A

### 8.6 Compatibility

N/A

### 8.7 Localization

测试用例包括但不局限于：
- 点击切换语言按钮后，所有元素是否按照选择的语言，正确展示

## 9. Questions

这里用于记录在Review Metting上需要讨论的问题：

## 10. Jira

此feature相关的所有Jira, 标题中包含统一的标签: [taosX监控]
<!-- Unsupported block type: 999 -->

## 11. Schedule

这里用于计划此feature测试的开始和结束时间。

## 12. Notes

## 13. Summary

## 14. Reference

[监控 taosX (taosX, taosKeeper, TDinsight)](https://taosdata.feishu.cn/wiki/JknAwaK6JiuIThkgnlkcWUNvnng)
