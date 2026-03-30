# TDengine 多级存储配置项支持禁止创建新文件组 Test Spec 

## 1. 测试目标

测试需求文档：[TDengine 多级存储配置项支持禁止创建新文件组](https://taosdata.feishu.cn/wiki/S3kdw2FCGiwxI1kxRx0ctASinfd)
本次测试主要验证在多级存储配置中，新增一个配置项disable_create_new_file，让用户可以控制某个挂载点是否禁止（disable_create_new_file=1时禁止，=0和空时不禁止）创建新文件组。

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024-06-17 | 1.0 | guoxy |  |

## 3. 测试结论

测试通过，和该任务相关的测试未发现问题。

通过发现测试，发现其他两个旧问题，后面需要排期修复。
bug1:副本变更会出现OOM，详细参考：[TD-30736](https://jira.taosdata.com:18080/browse/TD-30736?filter=-2)
bug2:redistribute vgroup+compact，会造成compact 一直不结束，详细参考：[TD-30768](https://jira.taosdata.com:18080/browse/TD-30768?filter=-2)

## 4. 开发质量报告

结论：本特性/优化的开发质量是 优（优，良，一般，差，很差）

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 （测试阻塞，无法进行） | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 0 |
| 严重 Bug 总数 | 0 |

## 5. 已知问题和限制

无

## 6. 测试资源及环境

测试平台：Linux x64
测试资源：192.168.1.64
测试版本：V3.3.2.0

## 7. 测试范围和重点

本次测试的重点如下：
1. 配置了disable_create_new_file后服务正常启动
2. 配置了disable_create_new_file后，数据继续写入后，在配置的dataDir中进行检查，disable_create_new_file=0的挂载点可以继续创建新文件组，disable_create_new_file=1的挂载点禁止继续创建新文件组。
3. 验证其它影响的功能，下面的事务进行操作时，compact db、flush db、trim db、replica db、balance vgroup、redistribute vgroup、split vgroup、restore vnode在disable_create_new_file=0的挂载点可以继续创建新文件组，disable_create_new_file=1的挂载点禁止继续创建新文件组。
4. 从不配置disable_create_new_file的版本升级到disable_create_new_file=0时，各项功能不受影响，升级到disable_create_new_file=1时，测试重点c中的相关操作在disable_create_new_file=1的挂载点中禁止继续创建新文件组。
5. 从配置disable_create_new_file=1的版本降级到disable_create_new_file=0和disable_create_new_file为空时，各项功能不受影响，配置的所有的多级存储挂载点均可以继续创建新文件组。

## 8. 测试数据

taosBenchmark指定json进行数据写入。其中cfg要按多级存储进行配置，多级存储的格式中包含path、level、primary、disable_create_new_file。写入数据的时间步长和跨度保证落到3级存储的每级空间内。
```plaintext {wrap}
dataDir [path] <level> <primary> <disable_create_new_file>
path: 挂载点的文件夹路径
level: 介质存储等级，取值为 0，1，2。 0 级存储最新的数据，1 级存储次新的数据，2 级存储最老的数据，省略默认为 0。 各级存储之间的数据流向：0 级存储 -> 1 级存储 -> 2 级存储。 同一存储等级可挂载多个硬盘，同一存储等级上的数据文件分布在该存储等级的所有硬盘上。 
primary: 是否为主挂载点，0（否）或 1（是），省略默认为 1。
disable_create_new_file: 是否禁止创建新文件组，0（不禁止）或 1（禁止），省略默认为 0。取值为 0 时，允许从该挂载点新建文件组；取值为 1 时，落盘时不会从该挂载点新建文件组，但是，已经生成的文件组，仍然会向该挂载点写入数据。
在配置中，只允许一个主挂载点存在（level=0，primary=1），例如采用如下配置方式：
dataDir /mnt/data1 0 1     // 主挂载点(Primary Disk) 默认不禁止创建新文件组
dataDir /mnt/data2 0 0 0   // 挂载点不禁止创建新文件组 
dataDir /mnt/data3 1 0 0   // 挂载点不禁止创建新文件组 
dataDir /mnt/data4 1 0     // 挂载点默认不禁止创建新文件组 
dataDir /mnt/data5 2 0 1   // 挂载点禁止创建新文件组 
dataDir /mnt/data6 2 0     // 挂载点默认不禁止创建新文件组 
```

## 9. 测试用例

| No. | 用例名称 | 用例描述 | 期望结果 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | 验证挂载点配置disable_create_new_file /mnt/data0 0 1 X /mnt/data1 0 0 X /mnt/data2 1 0 X /mnt/data3 2 0 X | 1. X为空，然后启动 1. 创建数据库、超级表、子表、普通表并写入数据 1. 检查data0-3下文件 1. 对该数据库继续进行数据写入 1. Flush db 1. Compact db 1. Trim db 1. Replica db 1. Balance vgroup 1. Redistribute vgroup 1. Split vgroup 1. Restore dnode 1. 重启所有taosd，然后执行4-12。 | 1. taosd启动成功，无异常日志 1. 数据库、超级表、子表、普通表并写入数据完成，count结果=数据插入结果。 1. 统计文件夹大小和文件组个数。 1. 数据继续写入成功 1. 在data0-3下生成新的文件组 1. 同5 1. 会迁移至指定目录，并生成新的文件组 1. 同5（包括 1 to 2 , 1 to 3 , 3 to 1） 1. 同5 1. 同5 1. 同5 1. 同5 1. 重启成功，行为同4-12。 | 通过 | 出现了[TD-30736](https://jira.taosdata.com:18080/browse/TD-30736)和[TD-30768](https://jira.taosdata.com:18080/browse/TD-30768)，但不属于本任务导致的 |
| 2 | 修改用例1，使X=0 | 测试用例同1完全一致 | 测试用例结果同1完全一致 | 通过 |  |
| 3 | 修改用例1，使X=1 | 1. X为1，然后启动 1. 创建数据库、超级表、子表、普通表并写入数据 1. 检查data0-3下文件 1. 对该数据库继续进行数据写入 1. Flush db 1. Compact db 1. Trim db 1. Replica db 1. Balance vgroup 1. Redistribute vgroup 1. Split vgroup 1. Restore dnode 1. 重启所有taosd，然后执行4-12。 | 1. taosd启动成功，无异常日志 1. 已存在的数据库、超级表、子表、普通表加载成功，查询正确。 1. 统计文件夹大小和文件组个数。 1. 数据无法写入 1. 可以执行，但无法生成新的文件组 1. 同5 1. 可以执行，但无法迁移至指定目录，并生成新的文件组 1. 同5（包括 1 to 2 , 1 to 3) , 但是 (3 to 1）不受影响 1. 同5 1. 同5 1. 同5 1. 同5 1. 重启成功，行为同4-12。 | 通过 |  |
| 4 | 验证挂载点配置disable_create_new_file /mnt/data0 0 1 1 /mnt/data1 0 0 1 /mnt/data11 0 0 0 /mnt/data2 1 0 1 /mnt/data21 1 0 0 /mnt/data3 2 0 1 /mnt/data31 2 0 0 | 1. disable_create_new_file混合配置0和1，然后启动 1. 创建数据库、超级表、子表、普通表并写入数据 1. 检查data0-3下文件和data11、data21、data31下文件 1. 对该数据库继续进行数据写入 1. Flush db 1. Compact db 1. Trim db 1. Replica db 1. Balance vgroup 1. Redistribute vgroup 1. Split vgroup 1. Restore dnode 1. 重启所有taosd，然后执行4-12。 | 1. taosd启动成功，无异常日志 1. 已存在的数据库、超级表、子表、普通表加载成功，查询正确。 1. 统计文件夹大小和文件组个数。 1. 数据可以写入 1. 可以执行，但data0-3下无法生成新的文件组，data11、data21、data31下生成新的文件组 1. 同5 1. 可以执行，但无法迁移至data0-3指定目录及生成新的文件组，但可以迁移至data11、data21、data31下指定目录，并生成新的文件组， 1. 同5（包括 1 to 2 , 1 to 3) , 但是 (3 to 1）不受影响 1. 同5 1. 同5 1. 同5 1. 同5 1. 重启成功，行为同4-12。 | 通过 |  |
| 5 | 验证升级场景， 模拟从用例1和2的X=空或者0， 升级到X=1 | 1、升级前配置及用例同用例1（或2） 2、升级后配置及用例同用例3 | 1、升级前用例行为同用例1（或2） 2、升级后用例行为同用例3 | 通过 |  |
| 6 | 验证降级场景， 模拟从用例3的X=1 降级到X=0或者空 | 1、升级前配置及用例同用例3 2、降级后配置及用例同用例1（或2） | 1、升级前用例行为同用例3 2、降级后用例行为同用例1（或2） | 通过 |  |

## 10. 问题

| Id | Title | Commen |
| --- | --- | --- |
|  |  |  |
|  |  |  |

## 11. 测试计划 

2024-06

## 12. 测试备忘 

无

## 13. 参考文档

[TDengine 多级存储配置项支持禁止创建新文件组](https://taosdata.feishu.cn/wiki/S3kdw2FCGiwxI1kxRx0ctASinfd)
