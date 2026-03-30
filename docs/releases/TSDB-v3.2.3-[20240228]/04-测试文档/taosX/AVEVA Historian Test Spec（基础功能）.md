# AVEVA Historian Test Spec（基础功能）

## 1. Objectives

- 通过 explorer 验证 AVEVA Historian 数据源的数据接入基础功能

## 2. Revision History

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2023.12.27 | 0.1 | @贾晨阳 |  |
| 2023.01.04 | 1.0 | @贾晨阳 |  |

## 3. Scope

- 测试均在Explorer上进行，不涉及命令行模式
- 本次测试中，数据同步功能（Live视图）在客户环境下验证

## 4. 测试结论

本次测试中，主要针对通过explorer进行AVEVA historian数据源接入的功能测试和网络异常健壮性测试，基础功能测试通过。
本次测试的主要验证内容：
1. History 表中指定历史时间区间的历史数据迁移功能；
2. History/Live 表实时数据同步功能；
3. 在数据同步/数据迁移过程中，对于网络异常和服务异常的任务恢复机制。

## 5. Limitations and Known Issues

这里用于记录产品使用上的一些限制，包括不支持的场景等，以及在发版时没有解决的minor issues.
- 对于用户手册中提到关于 Runtime.dbo.Live 表查询的限制（[AVEVA Historian Source](https://taosdata.feishu.cn/wiki/R92NwYTvKiL84Gk4qVdcTtGMnjb) 4.3.1节）：
   - 查询 Runtime.dbo.Live 表返回的结果集中，DateTime 是**查询时刻**的时间，且每个 Tag 只会有 1 条记录
   - 当 retrieveInterval 大于 数据上报频率 时，不会丢失数据，但会有重复数据
   - 当 retrieveInterval 小于 数据上报频率 时，会丢失数据
   - 当 retrieveInterval 等于 数据上报频率时，Live 表获得的时间戳，和真实数据的时间戳之间，还不一定完全一致
  本次测试中采用如下测试策略：通过将 retrieveInterval 设置为小于live表数据更新频率来确保数据不丢失，不验证时间戳的正确性；同时在企业版使用文档中增加对该参数的配置建议。

## 6. Environment

- OS: Windows, Linux
- Browser: Chrome

## 7. Test Data

N/A

## 8. Test Cases

### 8.1 Functional

在提测时，开发应保证sanity类型的用例全部通过。
| Type | Use Agent ? | Description | Expected Results | Result | Automated | Jira | Memo |
| --- | --- | --- | --- | --- | --- | --- | --- |
| sanity | Y | 历史数据迁移，同步history视图中的数据 | 任务正常完成，成功迁移数据 | Pass |  |  |  |
|  | N | 实时数据同步，同步live视图中的数据 | 任务持续进行不终止，能够持续同步live库中新增的数据 | Pass |  |  |  |
| connection | Y | 配置正确的用户名、密码、IP、端口号 | 连通性校验通过 | Pass |  |  |  |
|  | N |  | 连通性校验通过 | Pass |  |  |  |
|  | Y | 配置异常的用户名/密码/IP/端口号 | 连通性校验不通过，提示相应错误 | Pass |  |  |  |
| 数据迁移（migrate） | Y | 指定单个tag，beginDateTime 和 endDateTime的数据，其他参数采用默认值，执行数据迁移 | 查看下发的任务信息中参数均为默认值，正常完成指定时间段的数据迁移，任务状态为completed，写入TDengine中的数据和SQLServer中查询的结果一致，满足starttime到endtime区间； | Pass |  |  |  |
|  |  | 指定多个tag | 正常完成对应tag的数据迁移，每个tag对应TDengine中的一个子表 | Pass |  |  |  |
|  |  | tag栏使用“*”，配置tb_{tagName}为子表命名规则 | 正常对historian中除sys开头外所有tag的数据进行迁移 | Pass |  |  |  |
|  |  | 配置timewindow分别为1h，1d，1w | 下发的任务信息中timewindow参数值与设置一致 | Pass |  |  |  |
|  |  | 不配置beginDateTime 或 endDateTime | 前端提示必填项未填错误 | Pass |  |  |  |
|  |  | transformer中配置部分列为target | TDengine中只有配置为target的列有值写入，未配置的列值均为null | Pass |  |  |  |
|  |  | 除datetime列外，至少配置一列col和一列tag | 不符合规则的列配置，前端会提示错误 | Pass |  |  |  |
|  |  | 配置tag_list_size = 10,20,30 | 下发的任务信息中tag_list_size参数值与设置一致 | Pass |  |  |  |
| 数据同步（synchronize） | N | 选择history表，指定单个tag，beginDateTime 的数据，其他参数采用默认值，执行数据同步 | 查看下发的任务信息中参数均为默认值，任务状态置为running | Pass |  |  |  |
|  |  | 选择live表，指定单个tag，其他参数采用默认值，执行数据同步 | 查看下发的任务信息中参数均为默认值，任务状态置为running | Pass |  |  |  |
|  |  | 指定多个tag | 正常完成对应tag的数据同步，每个tag对应TDengine中的一个子表 | Pass |  |  |  |
|  |  | 配置retrieveInterval 为10s，1m，1h | 下发的任务信息中timewindow参数值与设置一致，在TDengine中查看对应轮询周期更新一次数据 | Pass |  |  |  |
|  |  | tag栏使用“*”，配置tb_{tagName}为子表命名规则 | 正常对historian中除sys开头外所有tag的数据进行迁移 | Pass |  |  |  |
|  |  | 不配置begindatetime | 前端提示必填项未填错误 | Pass |  |  |  |
|  |  | 配置tolerance = 500ms，10s，1m | 下发的任务信息中tolerance参数值与设置一致 | Pass |  |  |  |
| 异常输入 |  | UI上配置参数值为边界外值、不合法参数 | 前端对参数合法性和合理性进行校验，不满足的输入在对应位置提示错误 | Pass |  |  |  |


### 8.2 Usability

测试用例包括但不局限于：
- UI是否美观
- 交互是否合理
- 字体、字号是否合适
- 是否存在错别字

### 8.3 Reliability

可靠性主要验证在网络存在异常、或是数据源/目标端/agent进程存在异常恢复后，任务能够继续执行。
| Type | Use Agent ? | Description | Expected Results | Result | Automated | Jira | Memo |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 进程/网络异常 | N | 执行live表数据同步任务，同步过程中停止taosx进程再恢复 | 任务保持running，taosx进程恢复后继续同步live表数据 | Pass |  |  |  |
|  | N | 执行live表数据同步任务，同步过程中停止historian进程再恢复 | 任务保持running，historian进程恢复后继续同步live表数据 | Pass |  |  |  |
|  | N | 执行live表数据同步任务，同步过程中停止taosadapter进程再恢复 | 任务状态由running切换为interuptted，taosadapter恢复后，任务状态恢复为running | Pass |  |  |  |
|  | N | 执行live表数据同步任务，同步过程中停止taosd进程再恢复 | 任务保持running，taosd恢复后，TDengine中继续写入数据 | Pass |  |  |  |
|  | Y | 执行history表数据迁移，迁移过程中停止taosx进程再恢复 | 任务总时间和单次完整迁移的时间基本相当，未出现较大的时间差距 | Pass |  |  |  |
|  | Y | 执行history表数据迁移，迁移过程中停止agent进程再恢复 | 任务状态由running切换为waiting，agent恢复后，任务状态恢复为running | Pass |  |  |  |
|  | Y | 执行history表数据迁移，迁移过程中，通过chaosblade模拟agent和historian之间网络丢包100%再恢复 | 任务保持running，网络恢复后继续进行数据迁移，直至迁移完成 | Pass |  |  |  |
|  | Y | 执行history表数据迁移，迁移过程中，通过chaosblade模拟taosx和agent之间网络丢包100%再恢复 | 任务保持running，网络恢复后继续进行数据迁移，直至迁移完成 | Pass |  |  |  |
|  | Y | 执行history表数据迁移，迁移过程中，通过chaosblade模拟taosx和taosadapter之间网络丢包100%再恢复 | 任务保持running，网络恢复后继续进行数据迁移，直至迁移完成 | Pass |  |  |  |
|  | Y | 执行history表数据迁移，迁移过程中，通过chaosblade模拟taosd和taosadapter之间网络丢包100%再恢复 | 任务保持running，网络恢复后继续进行数据迁移，直至迁移完成 | Pass |  |  |  |
| 数据迁移（migrate） | Y | 指定单个tag，beginDateTime 和 endDateTime的数据，其他参数采用默认值，执行数据迁移 | 查看下发的任务信息中参数均为默认值，正常完成指定时间段的数据迁移，任务状态为completed，写入TDengine中的数据和SQLServer中查询的结果一致，满足starttime到endtime区间； |  |  |  |  |
|  |  | 指定多个tag | 正常完成对应tag的数据迁移，每个tag对应TDengine中的一个子表 |  |  |  |  |
|  |  | tag栏使用“*”，配置tb_{tagName}为子表命名规则 | 正常对historian中除sys开头外所有tag的数据进行迁移 |  |  |  |  |
|  |  | 配置timewindow分别为1h，1d，1w | 下发的任务信息中timewindow参数值与设置一致 |  |  |  |  |
|  |  | 不配置beginDateTime 或 endDateTime | 前端提示必填项未填错误 |  |  |  |  |
|  |  | transformer中配置部分列为target | TDengine中只有配置为target的列有值写入，未配置的列值均为null |  |  |  |  |
|  |  | 除datetime列外，至少配置一列col和一列tag | 不符合规则的列配置，前端会提示错误 |  |  |  |  |
| 数据同步（synchronize） | N | 选择history表，指定单个tag，beginDateTime 的数据，其他参数采用默认值，执行数据同步 |  |  |  |  |  |

### 8.4 Performance

在历史数据迁移场景下，影响性能的参数包括：timeWindow（单次查询窗口时间）、tag_list_size（tag组大小）、read_concurrency（读并发数）、batch_size，对性能调优的验证，是否单独写性能调优文档来验证？

### 8.5 Security

暂无。

### 8.6 Compatibility

historian 数据源为 taosx v1.5.0 新增数据源，不存在兼容性问题。

### 8.7 Localization

测试用例包括但不局限于：
- 点击切换语言按钮后，UI上的所有元素是否按照选择的语言，正确展示

## 9. Questions

这里用于记录在Review Metting上需要讨论的问题：
- 

## 10. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: historian

## 11. Schedule

这里用于计划此feature测试的开始和结束时间。

## 12. Notes

- ~~historian的history表中允许同一个tagname存在多条datetime相同但value不同的数据，而datetime在TDengine中对应的主键时间戳是唯一的，这就导致存在historian多条数据同步至TDengine中只有一条的情况。~~
- 连接器通过SQLServer的查询获取结果，查询时间窗口采用**左闭右开区间**的方式，而当每个查询时间窗口的起始点时间戳没有数据时，因为SQLServer的查询特性，会在查询结果中新增一条该时间戳的插值记录，这样写入TDengine中的数据可能存在比historian中对应表的数据多的情况。

## 13. Summary

## 14. Reference

用户手册文档：[AVEVA Historian Source](https://taosdata.feishu.cn/wiki/R92NwYTvKiL84Gk4qVdcTtGMnjb) 
AVEVA Historian 使用手册：[AVEVA™ Historian 2020.R2.SP1 Research Report](https://taosdata.feishu.cn/wiki/TjYfwPHo0iUr5JkWr3Ic3lhpndc)
