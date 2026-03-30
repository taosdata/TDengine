# [Test Report] TD-18572 3.0 企业版supportVnodes支持热更

### 1. 概述：

当前TDengine不支持热更，意味着用户必须重启taosd才能让supportVnodes生效，不满足企业用户的使用场景需要。本次更新将该特性放入企业版，社区版不支持。

### 2. 测试环境：

102.168.1.63：
CPU: Intel(R) Xeon(R) CPU E5-2650 v3 @ 2.30GHz（2）40核
Mem: DDR4 16GB* 16
Disk: 895GB

### 3. 测试用例：

| 测试用例 | 测试步骤 | 测试结果 |
| --- | --- | --- |
| 通过taos.cfg修改vnodes | 1. 修改taos.cfg中supportVnodes参数值 1. 重启taosd 1. 执行“show dnodes;”验证数值与taos.cfg数值一致 | 1. taos.cfg修改完成 1. 重启taosd正常 1. supportvnodes数值一致 |
| 占满已有vnode，创建新的数据库 | 1. 创建3节点集群 1. 修改3个dnode支持的vnode数量分别为11， 11， 10 1. 使用taosBenchmark写入数据，vgroups数量为32，写入数据完成 1. 创建新的数据库，创建失败 | 1. 集群创建完成 1. 修改dnode支持的vnode数量完成 1. 写入数据正常 1. 提示错误“out of dnodes” |
| 增加dndoe支持的vnode数量，创建新的数据库 | 1. 修改dnode3的vnode数量为12，修改后dndoe1支持的vnode为11；dndoe2为11；dndoe3为12 1. 创建新的数据库，创建成功(默认vgroups数量为2) 1. 删除新创建的数据库，并创建新的数据库，指定vgroups为3，“careate database db vgroups 3;” | 1. 修改dnode支持的vnode数量完成 1. 创建新的数据库成功 1. 删除数据库成功，创建包含3个vgroup的数据库失败，提示错误“out of dnodes” |
| Split vgroup占用vnode | 1. 当前dnode支持的vnode有两个可用 1. 执行两次split vgroup xx； 1. 再次执行split vgroup xx； | 1. 集群中2个vnode可用 1. Split vgroup正常 1. 执行提示错误“out of dnodes” |
| 副本变更过程中，创建新的数据库 | 1. 执行“alter database dbxx replica 3； 1. 修改dnode的vnode数量，修改后dndoe1支持的vnode为34；dndoe2为34；dndoe3为34 1. 执行“alter database dbxx replica 3；”， 执行过程中创建新的db 1. 执行“show vnodes；”查看总的vnodes数量，执行“select * from information_schema.ins_vgroups;”并对比，vnode数保持一致 | 1. 执行失败，提示错误“out of dnodes” 1. dnode支持的vnode数修改成功 1. 创建数据库正常，副本变更正常 1. vnode数一致 |

### 4. 总结：

1. supportVnodes数值可通过taos.cfg配置
2. 可通过命令“alter dnode xx ’supportvnodes 信息‘；”进行动态修改, 修改后立即生效
3. Split vgroup， 创建database以及alter database xx replica xx；过程会占用vnode
4. Split vgroup过程中创建新的database，如没有可用的vnode，会提示错误“out of dndoes”
5. 遗留问题 - [TD-26063](https://jira.taosdata.com:18080/browse/TD-26063) [副本变更过程中创建数据库，实际使用vnodes数会超过dnode中support_vnodes数](https://jira.taosdata.com:18080/browse/TD-26063)
