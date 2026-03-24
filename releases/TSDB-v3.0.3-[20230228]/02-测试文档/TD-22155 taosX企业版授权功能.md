# TD-22155 taosX企业版授权功能

## 一、测试概述

任务来源：
TD-22155


## 二、测试结论

测试已经完成，所有问题已经闭环。
剩余可优化项：
TD-22760

## 三、测试发现的问题

TD-22732


TD-22488


TD-22489

## 四、测试方案及测试用例

### 1.测试环境

（1）单机场景
采用两台机器分别作为数据源库和目标库。

| 软件项 | IP |
| --- | --- |
| 192.168.1.40 |
| 192.168.1.42 |
| taostest测试框架 | 192.168.1.40 |

（2） 集群场景
为了验证集群中同时存在企业版和社区版实例的场景，需要将集群搭建在不同的环境上，采用docker部署测试，可实现在同一机器分别部署企业版和社区版实例的场景。

|  | 软件项 | IP |
| --- | --- | --- |
| 192.168.1.40（docker1） |
| 192.168.1.40（docker2） |
| 192.168.1.40（docker3） |
| 192.168.1.40（docker4） |
| 192.168.1.40（docker5） |
| 192.168.1.40（docker6） |
|  | taostest测试框架 | 192.168.1.40 |

### 2.测试用例

依据用户手册中的描述：
1. 对于多个 TDengine 实例的任务（如 taosX Replication）需要满足至少一个（读取或写入端）为企业版。
考虑以下测试用例设计方式：
以下源库用source表示，目标库用target表示
测试采用数据同步的任务方式验证

#### （1）From taosd to taosd

| source | target | 工作方式 | 预期结果 | 是否符合预期 |
| --- | --- | --- | --- | --- |
| From native To native | 符合预期 |
| From native To ws | 符合预期 |
| From ws To native | 符合预期 |
| From ws To ws | 符合预期 |
| From native To native | 符合预期 |
| From native To ws | 符合预期 |
| From ws To native | 符合预期 |
| From ws To ws | 符合预期 |
| From native To native | 符合预期 |
| From native To ws | 符合预期 |
| From ws To native | 符合预期 |
| From ws To ws | 符合预期 |
| From native To native | 符合预期 |
| From native To ws | 符合预期 |
| From ws To native | 符合预期 |
| From ws To ws | 符合预期 |
| From native To native | TD-22732 |
| From native To ws | 符合预期 |
| From ws To native | 符合预期 |
| From ws To ws | 符合预期 |

#### （2）From cluster3 To cluster3

| source | target | 工作方式 | 预期结果 | 是否符合预期 |
| --- | --- | --- | --- | --- |
| From native To native | 符合预期 |
| From native To ws | 符合预期 |
| From ws To native | 符合预期 |
| From ws To ws | 符合预期 |
| From native To native | 符合预期 |
| From native To ws | 符合预期 |
| From ws To native | 符合预期 |
| From ws To ws | 符合预期 |
| From native To native | TD-22732 |
| From native To ws | 符合预期 |
| From ws To native | 符合预期 |
| From ws To ws | 符合预期 |
| From native To native | 符合预期 |
| From native To ws | 符合预期 |
| From ws To native | 符合预期 |
| From ws To ws | 符合预期 |

#### （3）From cloud To cluster3

| source | target | 预期结果 | 是否符合预期 |
| --- | --- | --- | --- |
| enterprise | 任务正常执行 | 符合预期 |
| community | 任务正常执行 | 符合预期 |
| enterprise + create user | 任务正常执行 | 符合预期 |

#### （4）From cluster3 To cloud

| source | target | 预期结果 | 是否符合预期 |
| --- | --- | --- | --- |
| enterprise | 任务正常执行 | 符合预期 |
| community | 任务正常执行 | 符合预期 |
| enterprise + create user | 任务正常执行 | 符合预期 |

#### （5）From cloud To cloud

| source | target | 预期结果 | 是否符合预期 |
| --- | --- | --- | --- |
| cloud | cloud | 任务正常执行 | 符合预期 |

#### （6）其他测试场景

| source | target | 预期结果 | 是否符合预期 |
| --- | --- | --- | --- |
| community | local | 任务报错退出 | TD-22488 |
| enterprise | local | 任务正常执行 | 符合预期 |
| local | community | 任务报错退出 | 符合预期 |
| local | enterprise | 任务正常执行 | 符合预期 |
| .csv | 任务正常执行 | 符合预期 |
| .parquet | 任务正常执行 | 符合预期 |
| .csv | 任务报错退出 | TD-22489 |
| .parquet | 任务报错退出 | 符合预期 |
