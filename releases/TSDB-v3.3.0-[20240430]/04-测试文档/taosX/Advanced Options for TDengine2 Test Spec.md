# Advanced Options for TDengine2 Test Spec

## 1. 测试目标

TDengine2 作为 Data in 数据源的一种，需要在使用上保持与其他数据源的一致性。
本次测试中主要针对高级选项（advanced options）中引入的各项参数实现的正确性进行验证：
- 最大读并发数（Read Concurreny）
- 最大写并发数（Write Concurreny）
- 错误记录文件（File to write failed data）

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.3.15 | 0.0 | @贾晨阳 |  |
| 2024.3.15 | 1.0 | @贾晨阳 | 依据组内reivew结果修改 |

## 3. 测试范围

这里用于描述本需求的覆盖范围：
- 验证最大读并发数在配置后是否生效
- 验证最大写并发数在配置后是否生效
- 验证错误记录文件配置后是否生效

## 4. 测试结论

本次测试验证TDengine2 to TDengine3 的advanced options 在配置后下发任务时是否生效，验证结果为最大读并发数、最大写并发数、错误记录文件参数均可正确生效。
遗留测试项：
TD-29168


## 5. 开发质量报告

结论：本特性/优化的开发质量是优

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 0 |
| 严重 Bug 总数 | 0 |

## 6. 已知问题和限制

1. 本次测试只在 explorer 上开展，命令行不开展测试
2. 本次测试主要内容为功能正确性测试，配置参数对于性能的影响在 release 版本发布后单独进行性能调优测试。
3. 由于CPU实际调度策略和软件内部实现的调度策略略有不同，在配置最大读并发数和最大写并发数参数后，实际观测到的新建连接数可能会比配置的值大一些，但不会相差很大，我们认为这并不是软件问题。
1. 
  TD-29170

## 7. 测试环境

- OS: taosx 分别部署在 Windows, Linux 上
- Browser: Chrome

## 8. 测试数据

为了使错误记录文件配置生效并具备可观测性，提前在目标端创建**同名超级表和子表**，但schema与源端超级表不同，以此构造写入失败的场景。
源端schema：2列int型，2列float型
目标端schema：2列int型，2列varchar(32)型

## 9. 测试用例

### 9.1 功能

在提测时，开发应保证 basic 类型的用例全部通过。
|  | Description | Expected Results | result for developer | Result | Jira | Automated | Memo |
| --- | --- | --- | --- | --- | --- | --- | --- |
| basic | 三个参数均不配置，配置好源端和目标端信息后，创建任务 | 下发的任务DSN中：
Read concurrency = 0
Write concurrency = 1
不包含File to write failed data参数 | Pass | Pass |  |  |  |
| basic | 配置File to write failed data参数的路径为/var/log/taos/error.log，目标端提前创建同名超级表和子表
源端schema：2列int型，2列float型
目标端schema：2列int型，2列varchar(32)型 | 下发的任务DSN中：

File to write failed data = /var/log/taos/error.log
指定路径下生成相应文件 | Pass |  |  |  |  |
| Read concurrency & Write concurrency | 配置
Read concurrency = 5
Write concurrency = 5 | 下发的任务DSN中：
Read concurrency = 5
Write concurrency = 5

源taosd中通过show connection 中查看存在websocket链接数
目标taosd中通过show connection查看存在websocket链接数 |  | Pass |  |  |  |
|  | 配置
Read concurrency = 5
Write concurrency = 10 | 下发的任务DSN中：
Read concurrency = 5
Write concurrency = 10

源taosd中通过show connection 中查看存在websocket链接数
目标taosd中通过show connection查看存在websocket链接数 |  | Pass |  |  |  |
|  | 配置
Read concurrency = 5
Write concurrency = 3 | 前端自动将Write concurrency置为5，且步进长度为5 |  |  |  |  |  |
|  | 配置
Read concurrency = 0
Write concurrency = 5（小于CPU核数） | 下发的任务DSN中：
Read concurrency = 0
Write concurrency = 5
源taosd中通过show connection 中查看存在websocket链接数不大于CPU核数

目标taosd中通过show connection查看存在websocket链接数不大于CPU核数 |  | Pass |  |  |  |
|  | 配置
Read Concurreny/Write Concurreny 为负值 | 前端限制无法配置负值 |  | Pass |  |  |  |
| File to write failed data | 不配置File to write failed data，构建目标端schema与源端不同的写入失败场景，运行任务 | 任务报错退出并重启，转为已停止 |  |  | [https://jira.taosdata.com:18080/browse/TD-29170](https://jira.taosdata.com:18080/browse/TD-29170) |  |  |
|  | 在linux上设置不存在的路径为文件保存路径 | 任务持续报错重启，需要提前创建路径 |  | Pass |  |  |  |
|  | 在windows上设置不存在的路径为文件保存路径 | 任务持续报错重启，需要提前创建路径 |  | Pass |  |  |  |
|  | 在linux上设置存在的路径，文件名不存在 | 在指定路径下创建错误文件 |  | Pass |  |  |  |
|  | 在windows上设置存在的路径，文件名不存在 | 在指定路径下创建错误文件 |  | Pass |  |  |  |
|  | linux权限测试：将文件保存路径设置在一个只读的目录下 | 日志中报错 |  | Pass |  |  |  |
|  | windows权限测试：将文件保存路径设置在一个只读的目录下 | 日志中报错 |  | Pass |  |  |  |
|  | 磁盘满测试：将文件保存路径设置在一个较小的分区，运行一段时间将这个分区写满 | 1. 数据同步正常
1. 日志中有提示信息：错误信息无法正常写入 |  |  |  |  |  |

### 9.2 可用性

依据 FS 中对UI界面的说明进行验证。
用户交互说明：
1. 最大读并发数默认值是 0。 √
2. 最大写并发数默认值是 1。√
3. 如果读并发数为 0， 那么写并发数输入框加和减的 step 为  1。√
4. 一旦读并发数改成非 0 值， 写并发的默认值就要和写并发相等，且加和减的 step 要和读并发数相等。

**本次测试中验证UI实现是否与上述描述一致。**

### 9.3 可靠性

无。

### 9.4 性能

无。

### 9.5 安全性

无。

### 9.6 兼容性

无。

### 9.7 本地化

无。


## 10. Jira

TD-29184


TD-29170


## 11. 测试计划 

测试开始及结束时间计划见 [3.3.0.0 开发计划追踪](https://taosdata.feishu.cn/wiki/U4KbwxWBii31aJkNRuYcL43RnAh)

## 12. 测试备忘 

## 13. 参考文档 

- [Data In: Advanced Options for TDengine2](https://taosdata.feishu.cn/wiki/Ck6Vw2ydSiDrnPkXYgOcWt9Xnec)
