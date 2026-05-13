# 事务健壮性测试 Test Spec

## 1. 测试目标

本次测试主要目标是验证 TDengine 中事务处理的健壮性，目标如下：
- 在高压和异常场景下，事务处理执行正常或报错
- 系统异常恢复后，未完成的事务的能够继续完成或报错
- 测试过程中，无 crash 或系统死锁产生

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024/5/28 | 0.1 | Charles |  |
| 2024/6/4 | 0.2 | Charles | 根据 Wade Review Comments 修改 |
| 2024/6/5 | 0.3 | Charles | 根据Wade Review Comments 修改测试方法、用例格式、描述等 |
| 2024/6/6 | 0.4 | Charles | 根据Wade Review Comments 增加用例测试目标描述 |
|  |  |  |  |

## 3. 测试结论

待测试完成后填写

## 4. 已知问题和限制

无

## 5. 测试资源及环境

测试平台：Linux x64
测试资源：192.168.1.35
测试版本：V3.3.1.0

## 6. 测试范围及方法

### 6.1 测试方法：

测试分为三个并发部分：
通过taosBenchmark持续写入数据做为压力源，通过dnode重启完成且restore数据完成做为判断起点，如果last(ts) 发生变化则持续写入正常
触发随机 dndoe 节点重启异常，重启完成以dnode pid且restore 数据完成为判断标准（restore数据完成时间暂定万行每秒的速率，用当前总写入行数 / 写入速率 计算timeout时间，会根据测试结果实时调整变量值），发送重启状态至主程序
验证事务能正确完成，无死锁产生，无 crash，通过事务表及事务结果判断事务完成状态，同时taosBenchmark持续数据写入正常
三部分测试状态由主程序统一管理，三个状态都正常，进入下一步测试

### 6.2 测试范围：

当前事务优化正在进行中，优化后，kill transaction 将被禁用；show transactions 将展示更多事务信息；所以，本次测试范围不包括 kill transaction 操作。测试重点是节点异常、高压场景下，多种事务操作健壮性的验证。
事务操作包括：

| create-db |  |
| --- | --- |
| alter-db | 副本变更 |
| drop-db |  |
| compact-db |  |
| create-dnode | dnode数量大于等于3 |
| drop-dnode | dnode数量大于3 |
| restore-dnode |  |
| create-stb-index |  |
| drop-index |  |
| create-mnode |  |
| drop-mnode |  |
| create-qnode |  |
| drop-qnode |  |
| create-sma |  |
| drop-sma |  |
| create-tsma |  |
| drop-tsma |  |
| drop-tbs |  |
| create-snode |  |
| drop-snode |  |
| create-stb |  |
| alter-stb |  |
| drop-stb |  |
| create-topic |  |
| drop-topic |  |
| create-user |  |
| alter-user |  |
| drop-user |  |
| split-vgroup |  |
| balance-vgroup |  |
| balance-vg-leader |  |
| stream-create |  |
| stream-pause |  |
| stream-resume |  |
| stream-drop |  |
| create-view |  |
| drop-view |  |

## 7. 测试数据

taosBenchmark Json: 
> ⚠ 嵌入文件，需在飞书中查看 (token: T96wbaNkOoKwyqxlDCjcYxb8njh)

## 8. 测试用例

### 8.1 数据库事务健壮性测试

1. 目标：验证创建数据库、副本变更、compact 数据库，删除数据库事务健壮性
   - 并发创建数据库的事务健壮性
   - 并发进行数据库副本变更的事务健壮性
   - 并发进行数据库 compact 的事务健壮性
   - 并发进行数据库删除操作的事务健壮性
2. 名称：test_db_transaction_robustness
3. 测试前置条件：
   - TDengine企业版安装部署3节点环境完成，带全功能授权
4. 测试步骤描述：
   - 使用taosBenchmark创建单副本数据库并持续写入数据
   - 并发创建10个数据库，vgroup值根据cpu核数 - 16计算最大值，同时随机重启某个dnode
   - 对持续写入的数据库进行从 1 到 2 的副本变更，同时按随机时间间隔（1-60秒）随机重启某个 dnode（在副本变更结束前，持续随机重启dnode节点），直到副本变更完成或出现系统死锁
   - 重复以上步骤进行从 2 到 3 的副本变更直到副本变更完成或出现系统死锁
   - 对数据库进行 compact 操作，在此过程中按随机时间间隔（1-60秒）重启某个随机选择的 dnode（在compact事务结束前，持续随机重启dnode节点），直到 compact 完成或出现系统死锁
   - 并发删除步骤2创建的数据库，按随机时间间隔（1-60秒）随机重启某个 dnode（在数据库删除事务结束前，持续随机重启dnode节点）
   - 检查环境
5. 期望测试结果：
   - taosBenchmark持续写入数据正常
   - 重启dnode正常，创建数据库事务完成，数据库创建完成
   - 重启dnode正常，副本变更事务1->2完成，检查taosBenchmark创建数据库副本数为2
   - 重启dnode正常，副本变更事务2->3完成，检查taosBenchmark创建数据库副本数为3
   - 重启dnode正常，compact事务完成
   - 重启dnode正常，删除数据库事务完成，步骤2创建的所有数据库被删除
   - 集群各节点状态正常，事务表无未完成事务，taosBenchmark持续写入正常，基本查询正常

### 8.2 Mnode、Dnode事务健壮性测试

1. 目标：验证创建、删除mnode、dnode，恢复dnode事务健壮性
   - 创建mnode事务健壮性
   - 删除mnode事务健壮性
   - 创建dnode事务健壮性
   - 删除dnode事务健壮性
   - 恢复dnode事务健壮性
2. 名称：test_mndoe_dnode_transaction_robustness
3. 测试前置条件：
   - TDengine企业版安装部署3节点环境完成，带全功能授权
4. 测试步骤描述：
   - 使用taosBenchmark创建数据库并持续写入数据
   - 创建 mnode 节点直到  mnode 节点数为3，同时随机重启某个dnode
   - 创建 dnode 节点直到 dnode 节点数为5，同时随机重启某个 dnode
   - 随机删除某个 dnode 节点数据触发恢复 dndoe 事务，同时按随机时间间隔（1-60秒）随机重启某个 dnode（在数据恢复结束前，持续随机重启dnode节点），直到数据恢复完成或出现系统死锁
   - 删除dnode节点至3，同时按随机时间间隔（1-60秒）随机重启某个 dnode（在删除dnode结束前，持续随机重启dnode节点），直到删除dnode完成或出现系统死锁
   - 删除mnode节点至1，同时按随机时间间隔（1-60秒）随机重启某个 dnode（在删除mnode结束前，持续随机重启dnode节点），直到删除mnode完成或出现系统死锁
   - 检查环境
5. 期望测试结果：
   - taosBenchmark持续写入数据正常
   - dnode重启正常，创建mnode事务完成，mndoe数量为3
   - dndoe重启正常，创建dnode事务完成，dnode数量为5
   - dnode重启正常，恢复dnode数据事务完成，事务表事务为0
   - dnode重启正常，删除dnode事务完成，dnode数量为3，且均为mndoe
   - dnode重启正常，删除mnode事务完成，mnode数量为1
   - 集群各节点状态正常，事务表无未完成事务，taosBenchmark持续写入正常，基本查询正常

### 8.3 索引事务健壮性测试

1. 目标：验证创建、删除索引事务健壮性
   - 并发创建值类型标签索引事务健壮性
   - 并发删除值类型标签索引事务健壮性
2. 名称：test_index_transaction_robustness
3. 测试前置条件：
   - TDengine企业版安装部署3节点环境完成，带全功能授权
4. 测试步骤描述：
   - 使用taosBenchmark创建数据库并持续写入数据
   - 对值类型标签列并发创建、删除索引，同时随机重启某个dnode；如果当前所有列均存在索引，创建索引等待5秒后重试，重试最大次数为10次；如果当前只存在默认索引，删除索引等待5秒后重试，重试最大次数为10次
   - 重复以上过程100次
   - 检查环境
5. 期望测试结果：
   - taosBenchmark持续写入数据正常
   - dnode重启正常，创建、删除标签事务完成，索引数与预期一致
   - 同step2
   - 集群各节点状态正常，事务表无未完成事务，taosBenchmark持续写入正常，基本查询正常

### 8.4 Qnode、Snode事务健壮性测试（优先级低）

1. 目标：验证创建、删除qnode、snode事务健壮性
   - 名称：test_qnode_snode_transaction_robustneww
2. 测试前置条件：
   - TDengine企业版安装部署3节点环境完成，带全功能授权
3. 测试步骤描述：
   - 使用taosBenchmark创建数据库并持续写入数据
   - 并发创建、删除snode，同时随机重启dnode节点，如果当前snode数量与dnode数量一致，创建snode等待5秒后重试，重试最大次数为10次；如果当前snode数量为0，删除snode等待5庙后重试，重试最大次数为10次
   - 重复step2 100次
   - 并发创建、删除qnode，同时随机重启dnode节点，如果当前qnode数量与dnode数量一致，创建qnode等待5秒后重试，重试最大次数为10次；如果当前qnode数量为0，删除qnode等待5庙后重试，重试最大次数为10次
   - 重复step4 100次
   - 检查环境
4. 期望测试结果：
   - taosBenchmark持续写入数据
   - dnode重启正常，创建、删除snode事务完成，snode数量与预期一致
   - 同step2
   - dnode重启正常，创建、删除qnode事务完成，qnode数量与预期一致
   - 同step4
   - 集群各节点状态正常，事务表无未完成事务，taosBenchmark持续写入正常，基本查询正常

### 8.5 Sma事务健壮性测试

1. 目标：验证创建、删除sma事务健壮性
   - 随机创建sma索引事务健壮性
   - 随机删除sma索引事务健壮性
2. 名称：test_sma_transaction_robustness
3. 测试前置条件：
   - TDengine企业版安装部署3节点环境完成，带全功能授权
4. 测试步骤描述：
   - 使用taosBenchmark创建数据库并持续写入数据
   - 随机对标签列创建、删除 sma 索引，同时随机重启 dnode 节点，如果当前所有列均存在 sma 索引，创建sma 索引等待 5 秒后重试，重试最大次数为 10 次；如果当前只存在默认索引，删除 sma 索引等待 5 秒后重试，重试最大次数为 10 次
   - 重复以上步骤100次
   - 检查环境
5. 期望测试结果：
   - taosBenchmark持续写入数据
   - dnode重启正常，创建、删除sma索引事务完成，sma索引数与预期一致
   - 同step2
   - 集群各节点状态正常，事务表无未完成事务，taosBenchmark持续写入正常，基本查询正常

### 8.6 Tsma事务健壮性测试

1. 目标：验证创建、删除tsma事务健壮性
   - 并发创建tsma事务健壮性
   - 并发删除tsma事务健壮性
   - 并发删除tsma相关子表事务健壮性
2. 名称：test_tsma_transaction_robustness
3. 测试前置条件：
   - TDengine企业版安装部署3节点环境完成，带全功能授权
4. 测试步骤描述：
   - 使用taosBenchmark创建数据库并持续写入数据
   - 并发创建3个tsma，同时随机重启dnode节点
   - 并发删除3个tsma，同时随机重启dnode节点
   - 停止taosBenchmark写入，并发删除10个子表，同时随机重启dnode节点
   - 检查环境
5. 期望测试结果：
   - taosBenchmark持续写入数据
   - dnode重启正常，创建tsma事务完成， tsma对应的超级表last(ts)值变化
   - dnode重启正常，删除tsma事务完成， tsma对应的超级表last(ts)值无变化
   - taosBenchark写入停止，10个子表被删除，dnode重启正常，tsma删除子表事务完成，tsma对应的子表数减少10个
   - 集群各节点状态正常，事务表无未完成事务，taosBenchmark持续写入正常，基本查询正常

### 8.7 超级表事务健壮性测试

1. 目标：验证创建、修改、删除超级表事务健壮性
   - 并发创建超级表事务健壮性
   - 并发修改超级表事务健壮性
   - 并发删除超级表事务健壮性
2. 名称：test_super_table_transaction_robustness
3. 测试前置条件：
   - TDengine企业版安装部署3节点环境完成，带全功能授权
4. 测试步骤描述：
   - 使用taosBenchmark创建数据库并持续写入数据
   - 创建数据库并创建100个超级表，同时按随机时间间隔（1-60秒）随机重启某个 dnode（在创建超级表结束前，持续随机重启dnode节点），直到创建超级表完成或出现系统死锁
   - 随机修改超级表（增加列、删除列、增加标签、删除标签、修改标签名、修改列宽度、修改标签宽度）100次，同时按随机时间间隔（1-60秒）随机重启某个 dnode（在修改超级表结束前，持续随机重启dnode节点），直到修改超级表完成或出现系统死锁
   - 删除100个超级表，同时按随机时间间隔（1-60秒）随机重启某个 dnode（在删除超级表结束前，持续随机重启dnode节点），直到删除超级表完成或出现系统死锁
   - 检查环境
5. 期望测试结果：
   - 使用taosBenchmark创建数据库并持续写入数据
   - dndoe重启正常，创建超级表事务完成，超级表数量为100
   - dnode重启正常，修改超级表事务完成
   - dnode重启正常，删除超级表事务完成，超级表数量为0
   - 集群各节点状态正常，事务表无未完成事务，taosBenchmark持续写入正常，基本查询正常

### 8.8 Topic事务健壮性测试

1. 目标：验证创建、删除topic事务健壮性
   - 并发创建topic事务健壮性
   - 并发删除topic事务健壮性
2. 名称：test_topic_transaction_robustness
3. 测试前置条件：
   - TDengine企业版安装部署3节点环境完成，带全功能授权
4. 测试步骤描述：
   - 使用taosBenchmark创建单副本数据库并持续写入数据
   - 并发创建topic100个，同时按随机时间间隔（1-60秒）随机重启某个 dnode（在创建topic结束前，持续随机重启dnode节点），直到创建topic完成或出现系统死锁
   - 并发删除topic，同时按随机时间间隔（1-60秒）随机重启某个 dnode（在删除topic结束前，持续随机重启dnode节点），直到删除topic完成或出现系统死锁
   - 检查环境
5. 期望测试结果：
   - taosBenchmark持续写入数据正常
   - dnode重启正常，创建topic事务完成，topic数量为100
   - dnode重启正常，删除topic事务完成，topic数量为0
   - 集群各节点状态正常，事务表无未完成事务，taosBenchmark持续写入正常，基本查询正常

### 8.9 User事务健壮性测试

1. 目标：验证创建、修改、删除用户事务健壮性
   - 并发创建用户事务健壮性
   - 并发修改用户事务健壮性
   - 并发删除用户事务健壮性
2. 名称：test_user_transaction_robustness
3. 测试前置条件：
   - TDengine企业版安装部署3节点环境完成，带全功能授权
4. 测试步骤描述：
   - 用taosBenchmark创建数据库并持续写入数据
   - 创建100个user，同时按随机时间间隔（1-60秒）随机重启某个 dnode（在创建user结束前，持续随机重启dnode节点），直到创建user完成或出现系统死锁
   - 随机修改用户密码、权限100次，同时按随机时间间隔（1-60秒）随机重启某个 dnode（在修改user结束前，持续随机重启dnode节点），直到修改user完成或出现系统死锁
   - 删除用户100次，权限100次，同时按随机时间间隔（1-60秒）随机重启某个 dnode（在删除user结束前，持续随机重启dnode节点），直到删除user完成或出现系统死锁
   - 检查环境
5. 期望测试结果：
   - taosBenchmark持续写入数据
   - dnode重启正常，创建user事务完成， user数量为101
   - dnode重启正常，修改user事务完成
   - dnode重启正常，删除user事务完成， user数量为1
   - 集群各节点状态正常，事务表无未完成事务，taosBenchmark持续写入正常，基本查询正常

### 8.10 VGroup事务健壮性测试

1. 目标：验证spilt、balance、balance-vg-leader vgroup事务健壮性
   - 随机split vgroup事务健壮性
   - 随机balance-vg-leader事务健壮性
   - Balance vgroup事务健壮性
2. 名称：test_vgroup_transaction_robustness
3. 测试前置条件：
   - TDengine企业版安装部署3节点环境完成，带全功能授权
4. 测试步骤描述：
   - 使用taosBenchmark创建单副本数据库，并持续写入数据
   - 随机选取vgroup进行split操作，同时按随机时间间隔（1-60秒）随机重启某个 dnode（在split vgroup结束前，持续随机重启dnode节点），直到split vgroup完成或出现系统死锁
   - 随机选取vgroup进行balance-vg-leader操作，同时按随机时间间隔（1-60秒）随机重启某个 dnode（在balance-vg-leader结束前，持续随机重启dnode节点），直到balance-vg-leader完成或出现系统死锁
   - 进行balance-vgroup操作，同时按随机时间间隔（1-60秒）随机重启某个 dnode（在balance-vgroup结束前，持续随机重启dnode节点），直到balance-vgroup完成或出现系统死锁
   - 创建3副本数据库，并持续写入数据，重复步骤2-4
   - 检查环境
5. 期望测试结果：
   - taosBenchmark持续写入数据正常
   - dnode重启正常，split vgroup事务完成，vgroup数量+1
   - dnode重启正常，balance-vg-leader事务完成
   - dnode重启正常，balance-vgroup事务完成
   - 3副本数据库结果与步骤2-4一致
   - 集群各节点状态正常，事务表无未完成事务，taosBenchmark持续写入正常，基本查询正常

### 8.11 Stream事务健壮性测试

1. 目标：验证创建、停止、恢复、删除流计算事务健壮性
   - 并发创建流事务健壮性
   - 并发停止流事务健壮性
   - 并发恢复流事务健壮性
   - 并发删除流事务健壮性
2. 名称：test_stream_transaction_robustness
3. 测试前置条件：
   - TDengine企业版安装部署3节点环境完成，带全功能授权
4. 测试步骤描述：
   - taosBenchmark创建数据库并持续写入数据
   - 并发创建10个流计算，同时随机重启dnode节点
   - 并发停止10个流计算，同时随机重启dnode节点
   - 并发恢复10个流计算，同时随机重启dnode节点
   - 并发删除10个流计算，同时重启dndoe节点
   - 检查环境
5. 期望测试结果：
   - taosBenchmark持续写入数据
   - dnode重启正常，创建流计算事务完成，流计算任务为10个
   - dnode重启正常，停止流计算事务完成，10个流计算任务为停止状态
   - dnode重启正常，恢复流计算事务完成，10个流计算任务为运行状态
   - dnode重启正常，删除流计算事务完成，流计算任务为0
   - 集群各节点状态正常，事务表无未完成事务，taosBenchmark持续写入正常，基本查询正常

### 8.12 View事务健壮性测试

1. 目标：验证创建、删除view事务健壮性
   - 并发创建视图事务健壮性
   - 并发删除视图事务健壮性
2. 名称：test_view_transaction_robustness
3. 测试前置条件：
   - TDengine企业版安装部署3节点环境完成，带全功能授权
4. 测试步骤描述：
   - taosBenchmark创建数据库并持续写入数据
   - 并发创建10个视图，同时随机重启某个dnode节点
   - 并发删除步骤2创建的视图，同时随机重启某个dnode节点
   - 检查环境
5. 期望测试结果：
   - taosBenchmark持续写入数据正常
   - 重启dnode正常，创建视图事务完成，视图数量为10
   - 重启dnode正常，删除视图事务完成，视图数量为0
   - 集群各节点状态正常，事务表无未完成事务，taosBenchmark持续写入正常，基本查询正常

## 9. 问题

| Id | Title | Commen |
| --- | --- | --- |
|  |  |  |

## 10. 测试计划 

2024-06-03 -- ？

## 11. 测试备忘 

## 12. 参考文档 {folded="true"}

[Transaction 改进](https://taosdata.feishu.cn/wiki/JV0jwKoqai89d4kpOXfczSpdnsc)
[当前客户成功面临的挑战和举措](https://taosdata.feishu.cn/wiki/Eit4wdGLciwMzikhkoScvJXtnng)
