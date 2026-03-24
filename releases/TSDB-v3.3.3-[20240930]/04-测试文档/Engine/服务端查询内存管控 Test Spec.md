# 服务端查询内存管控 Test Spec

## 1. 测试目标

通过内存管控策略保证数据库运行过程中不会因为内存资源不足而发生OOM情况，若内存资源超过预先配置的内存使用策略最大阈值，会返回明确信息提示用户内存不足

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.8.1 | 1.0 | 翟坤 | 创建初版文档 |
| 2024.8.2 | 2.0 | 翟坤 | 线下review，更新文档 |

## 3. 测试范围

内存管控功能对TD所在节点内存不足情况下的监控和报错机制

## 4. 测试结论

待补充

## 5. 开发质量报告

结论：本特性/优化的开发质量是（优，良，一般，差，很差）

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 |  |
| 基础测试用例不通过 |  |
| Bug 总数 |  |
| 严重 Bug 总数 |  |

## 6. 已知问题和限制

暂无

## 7. 测试环境

- OS: Linux
- IP: 待定

## 8. 测试用例

### 8.1 功能

| 类型 | 测试目的 | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 测试配置项singleQueryMaxMemorySize | 验证singleQueryMaxMemorySize默认值为0，内存使用无上限 | 1. 不配置singleQueryMaxMemorySize
1. 执行多表join查询，将系统内存消耗提高90% | 查询不报错 | Y |  | 可适当调整表中数据量 |
|  | 单条查询超过singleQueryMaxMemorySize配置项，报错 | 将singleQueryMaxMemorySize配置为1M，执行一个大数据量join查询，内存消耗超过1M | 查询报错，提示信息可明确提示内存不足 | Y |  | 具体的阈值在测试中调整 |
|  | 单条查询未超过singleQueryMaxMemorySize配置项，执行正常 | 将singleQueryMaxMemorySize配置为1M，执行简单查询，内存消耗未超过1M | 查询正常返回结果 | Y |  |  |
|  | 验证singleQueryMaxMemorySize的异常值，启动taosd报错 | 配置singleQueryMaxMemorySize为以下值，启动taosd
1、1000000001
2、-1
3、1.2 | 启动taosd报错 | Y |  |  |
|  | 验证singleQueryMaxMemorySize的边界值，启动taosd正常 | 配置singleQueryMaxMemorySize为以下值，启动taosd
1、1000000000
2、0 | taosd正常启动 | Y |  |  |
|  | 动态更新singleQueryMaxMemorySize | 1.通过alter all dnodes命令修改其值
2.通过alter dnode命令修改其值
3.不同dnode节点可配置为不同值
4.show dnode x variables命令验证其值 | 修改立刻生效 | N |  |  |
| 测试配置项queryBufferPoolSize | 验证queryBufferPoolSize默认值为0，单节点内存使用无上限 | 1. 不配置queryBufferPoolSize
1. 通过jmeter执行并发查询持续提升系统内存消耗 | 所有查询不报错直到出现OOM | Y |  |  |
|  | 单节点查询消耗内存超过queryBufferPoolSize的阈值，查询报错 | 1. 将singleQueryMaxMemorySize配置为10G
2.通过jmeter执行并发查询将系统资源消耗提升到超过10G | 查询报错，提示信息可明确提示内存不足 | Y |  |  |
|  | 单条查询未超过queryBufferPoolSize配置项，执行正常 | 将singleQueryMaxMemorySize配置为5G，通过大数据量的2张表join查询，使用内存未超过5G | 查询正常返回结果 | Y |  | singleQueryMaxMemorySize值难以提前设计准确，会在测试中调整 |
|  | 验证queryBufferPoolSize的异常值，启动taosd报错 | 配置singleQueryMaxMemorySize为以下值，启动taosd
1、1000000001
2、-1
3、1.2 | 启动taosd报错 | Y |  |  |
|  | 验证queryBufferPoolSize的边界值，启动taosd正常 | 配置singleQueryMaxMemorySize为以下值，启动taosd
1、1000000000
2、0 | taosd正常启动 | Y |  |  |
|  | 动态更新queryBufferPoolSize | 1.通过alter all dnodes命令修改其值
2.通过alter dnode命令修改其值
3.不同dnode节点可配置为不同值 | 修改立刻生效 | N |  |  |
| 测试配置项queryUseMemoryPool | queryUseMemoryPool默认值为true，打开内存管理功能 | 1.使用默认配置
2.按照上面的步骤验证singleQueryMaxMemorySize和queryBufferPoolSize功能 | 功能生效 | Y |  |  |
|  | 配置queryUseMemoryPool值为true，打开内存管理功能 | 1.配置queryUseMemoryPool=true
2.按照上面的步骤验证singleQueryMaxMemorySize和queryBufferPoolSize功能 | 功能生效 | Y |  |  |
|  | 配置queryUseMemoryPool值为false，关闭内存管理功能 | 1.配置queryUseMemoryPool=false
2.按照上面的步骤验证singleQueryMaxMemorySize和queryBufferPoolSize功能 | 1.内存管理功能失效
2.查询报错，提示信息可明确提示内存不足 | Y |  |  |
|  | 验证queryUseMemoryPool的异常值 | 配置queryUseMemoryPool为以下值，启动taosd
1、1
2、0
3、-1 | 启动taosd报错 | Y |  |  |
|  | 验证queryUseMemoryPool的正常值 | 配置queryUseMemoryPool为以下值，启动taosd
1、true、True、TRUE
2、false、False、FALSE | taosd正常启动 | Y |  |  |
|  | 动态更新queryUseMemoryPool失败 | 1.通过alter all dnodes命令修改其值
2.通过alter dnode命令修改其值 | 修改命令返回错误信息 | N |  |  |
| 测试内存上限与预留机制 | 系统可用内存低于512M，taosd启动报错 | 1.不配置queryBufferPoolSize或配置其值为0
2.通过blade工具将系统可用内存控制在400M
3.启动taosd | 启动taosd报错，明确提示内存不足 | N |  |  |
|  | 不配置queryBufferPoolSize或配置其值为0，其他进程快速占用系统内存 | 1.不配置queryBufferPoolSize或配置其值为0
2.通过jmeter持续发送查询请求
3.通过blade工具将内存逐步降低至512M以下 | 1. 在系统内存持续提高的过程中，taosd的查询失败率会逐步提高
2.不会出现OOM | N |  |  |
| 测试错误信息内容 | 单个查询内存到达使用上限 | 触发单个查询内存到达singleQueryMaxMemorySize | 查询返回错误信息：“Query memory upper limit is reached” | Y |  |  |
|  | 所有查询内存耗尽 | 查询使用内存超过queryBufferPoolSize | 查询返回错误信息："Query memory exhausted" | Y |  |  |
|  | 查询可用内存不足 | 内存不足512M，启动taosd | 返回错误信息："Too few available memory for query" | Y |  |  |
| 测试查询淘汰策略 | 总查询内存达到上限值的 85% | 1.单节点集群，通过jmeter多并发查询，将总资源控制在上限值的70%左右，其中一类是占用内存较小的查询语句，一类是占用内存较大的查询语句
2.调整jmeter的并发和sql，将内存使用率逐步提升超过上限值的85% | 1.正在执行的占用内存较大的查询开始报错，提醒内存不足
2.继续提交的大查询立刻报错，提醒内存不足 | N |  | 因为通过jmeter很难精确控制内存使用，整个测试过程预想测试结果可能跟实际无法精确一致，但总体趋势应该差不多，比如先从内存消耗大的sql开始失败，最后内存消耗相对小的开始失败，最后全部查询都失败。中间可能会出现波动情况，需实际测试时在确定行为是否合理，并且绝对不可以出现OOM现象 |
|  | 总查询内存达到上限值的 90% | 1.单节点集群，通过jmeter多并发查询，将总资源控制在上限值的70%左右，其中一类是占用内存较小的查询语句，一类是占用内存较大的查询语句
2.调整jmeter的并发和sql，将内存使用率逐步提升超过上限值的90% | 该场景行为可能无法精确设计，测试过程看具体情况是否合理，但应该也是大查询sql先失败 | N |  |  |
|  | 总查询内存达到上限值的 95% | 1.单节点集群，通过jmeter多并发查询，将总资源控制在上限值的70%左右，其中一类是占用内存较小的查询语句，一类是占用内存较大的查询语句
2.调整jmeter的并发和sql，将内存使用率逐步提升超过上限值的95% | 1.正在执行的所有查询开始报错，提醒内存不足
2.继续提交的大查询立刻报错，提醒内存不足 | N |  |  |
|  | 配置淘汰策略对比验证 | 1.重复上面3个测试过程，通过jmeter在旧版本找到一个可OOM的必现操作
2.新版本重复相同操作 | 配置了查询淘汰策略后，不会出现OOM | N |  | OOM的必现操作可能很难找到，该项测试不一定能实现 |

### 8.2 可靠性

| 类型 | 测试目的 | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 可靠性测试 | 验证在内存长期处于内存上限高危情况下，系统可持续运行 | 1.多节点集群，配置queryBufferPoolSize和singleQueryMaxMemorySize为0
2.通过jmeter多并发查询，将总资源控制在上限值的95%左右，其中一类是占用内存较小的查询语句，一类是占用内存较大的查询语句
3.在此状态下运行8小时 | 1.查询可以报错
2.taosd不会crash或OOM | N |  |  |
|  | 验证taosd受外部应用占用内存影响的情况下，系统可持续运行 | 1.多节点集群，配置queryBufferPoolSize和singleQueryMaxMemorySize为0
2.通过jmeter多并发查询，将总资源控制在上限值的70%左右，其中一类是占用内存较小的查询语句，一类是占用内存较大的查询语句
2.通过脚本调用blade工具，将系统使用率短时提高到90%，在恢复正常
3.在此状态下运行8小时 | 1.查询可以报错
2.taosd不会crash或OOM | N |  |  |
|  | 验证在内存长期处于内存上限高危情况下，恢复内存正常使用，taosd可继续正常运行 | 基于上面测试运行8小时后，停止查询压力后，做基本的创建表、插入数据和查询操作 | 基本的数据库功能工作正常 | N |  |  |

### 8.3 性能

| 类型 | 测试目的 | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 性能测试 | 验证TSBS查询性能受内存管理功能的影响 | 在测试版本上运行tsbs查询，对比其结果和旧版本（v3.3.2.5）的性能差异 | 查询性能下降不会超过5% | N |  | [TSBS daily run results](http://192.168.0.204:3000/d/a625ab90-3f00-4eb6-805a-b0d20621c6a5/important-tsbse680a7-e883bd-e6b58b-e8af95?orgId=1&refresh=15m&from=now-30d&to=now) |
|  | 验证Benchamrk查询性能受内存管理功能的影响 | 在测试版本上运行benchamrk查询，对比其结果和旧版本（v3.3.2.5）的性能差异 | 基本的数据库功能工作正常 | N |  | [Benchamrk daily run Results](http://192.168.0.204:3000/d/f1b5ade6-a6ef-4a35-88cc-45b0a733e993/27bbf174-be64-5c9b-9db6-911cde004f67?orgId=1&refresh=15m&var-scenario=常用场景-小量级) |

## 9. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: Query_Mem_Control

## 10. 风险评估

1. 由于内存管控功能从外部无法设计针对性的测试用例对每个测试点进行case by case的测试验证，本次功能测试策略（不包括最基本的参数配置项测试）：在并发查询高压的前提下，控制节点的内存，确保内存不足时查询会报错，但TDengine不会OOM
2. 测试用例中设计的测试场景，有可能在测试过程中无法精确量化验证，比如*当正在使用的总查询内存达到上限值的 85% 时，将启动淘汰当前 dnode 中内存使用最高的且单个查询使用内存超过总查询内存 10% 的查询*，还不确认是否能获取到每个被停止的查询当时所占用的内存等信息，将会在测试过程中不断摸索和优化测试用例和测试策略

## 11. 参考文档 (Optional)

JIRA：
TD-30268

FS Doc：[服务端查询内存管控](https://taosdata.feishu.cn/wiki/Y5tbwW0bwiQqdfkoYLlcVS36nfc)
