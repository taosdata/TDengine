# 【Test Report】TD-22970 - 使用 RAFT 协议实现成员变更 (vnode)

### 1. 概述：

本次优化点主要涉及两个方面：
1. 利用raft本身的健壮性，解决目前在成员变更中存在的集群高可用问题
2. 之前实现learner后，阻塞写入的时长已经大大缩短，实现基于raft的成员变更后，将遗留的秒级阻塞也基本去除
3. 之前实现learner时，遗留一个问题，在2->3的时候会有单点故障的风险，此风险存在的时间长度取决于要同步的存量数据的多少，本次改动进一步将这个风险的时长缩短到秒级。

### 2. 测试环境：

102.168.1.63：
CPU: Intel(R) Xeon(R) CPU E5-2650 v3 @ 2.30GHz（2）40核
Mem: DDR4 16GB* 16
Disk: 895GB

### 3. 测试用例：

用例1（非写入场景, 1->3）：
1. 搭建三节点测试环境
2. 通过taosBenchmark创建单副本db并写入一亿条数据
3. 在任一节点执行select count（*）统计写入的数据条数
4. 在任一节点随机选择三个子表的数据，导出csv数据文件
5. 执行命令alter database replica 3；
6. 通过show vgroups命令查看每个dnode节点添加vnode的角色变化，vnode节点增加两个，并同时开始数据同步，新增加的两个vnode节点追平数据后，同时成为follower节点；通过show transactions查看3副本转换过程是否完成
7. 3副本转换完成后，停掉任一dnode节点，在另一个dnode节点执行select count（*）命令，数据条数与单副本下的数据条数一致
8. 导出step d中的三个子表的数据，导出csv数据文件，并与step d中的数据文件对比，数据一致
测试结果：
1. 写入一亿数据后，节点1（61）空间占用1.5GB, 节点2（63）空间占用1.5GB, 节点3（63）空间占用752MB
2. 10个vgroup，时间5分15秒
3. 数据对比完全一致
4. 转成3副本后，点1（61）空间占用3.7GB, 节点2（63）空间占用3.7GB, 节点3（63）空间占用3.7GB

用例2（非写入场景，3->1）：
1. 在用例1的基础上执行alter database replica 1；
2. 通过show vgroups命令查看每个dnode节点添加vnode的角色变化，vnode节点变化过程为3->2->1；通过show transactions查看3副本转换过程是否完成
3. 3副本转换完成后，执行select count（*）命令，数据条数与3副本下的数据条数一致
4. 导出用例1的三个子表的数据，导出csv数据文件，并与用例1的数据文件对比，数据一致
测试结果：
1. 10个vgroup，3副本转1副本时间54秒
2. 数据对比完全一致
3. 转成单副本后，点1（61）空间占用1.5GB, 节点2（63）空间占用1.5GB, 节点3（63）空间占用780MB

用例3（非写入场景, 1->3, 服务异常）：
1. 在用例2的基础上执行alter database replica 3；
2. 通过show vgroups命令查看每个dnode节点添加vnode的角色变化，vnode节点增加两个，并同时开始数据同步，新增加的两个vnode节点追平数据后，同时成为follower节点；通过show transactions查看3副本转换过程是否完成
3. 在数据追平过程中，多次启停mnode的taosd服务，vnode变更过程继续并能正常完成
4. 3副本转换完成后，执行select count（*）命令，数据条数与3副本下的数据条数一致
5. 导出用例1的三个子表的数据，导出csv数据文件，并与用例1的数据文件对比，数据一致
测试结果：
1. 10个vgroup，1副本转3副本时间12分37秒（leader节点重启，follower节点重启一次）
2. 数据对比完全一致
3. 转成单副本后，点1（61）空间占用4.1GB, 节点2（63）空间占用3.8GB, 节点3（63）空间占用3.9GB

用例4（非写入场景, 3->1, 服务异常）：
1. 在用例3的基础上执行alter database replica 1；
2. 通过show vgroups命令查看每个dnode节点添加vnode的角色变化，vnode节点变化过程为3->2->1；通过show transactions查看3副本转换过程是否完成
3. 在vnode变更过程中，多次启停mnode的taosd服务，vnode变更过程继续并能正常完成
4. 3副本转换完成后，执行select count（*）命令，数据条数与3副本下的数据条数一致
5. 导出用例1的三个子表的数据，导出csv数据文件，并与用例1的数据文件对比，数据一致
测试结果：
1. 10个vgroup，3副本转1副本时间1分45秒
2. 数据对比完全一致
3. 转成单副本后，点1（61）空间占用1.5GB, 节点2（63）空间占用1.5GB, 节点3（63）空间占用763MB
4. taosd服务异常恢复后，不影响副本转换

用例5（持续写入场景，1->3）：
1. 搭建三节点测试环境
2. 修改taosBenchmark 参数trying_interval 为1秒，通过taosBenchmark创建单副本db持续写入数据，数据总量一亿
3. 数据写入超过2000万时，执行命令alter database replica 3；
4. 通过show vgroups命令查看每个dnode节点添加vnode的角色变化，vnode节点增加两个，并同时开始数据同步，新增加的两个vnode节点追平数据后，同时成为follower节点；通过show transactions查看3副本转换过程是否完成
5. 查看在vnode变更过程中，阻塞数据写入的时间不高于4秒
6. 副本变更过程中同步查询工作正常
7. 3副本转换完成后，停掉任一dnode节点，在另一个dnode节点执行select count（*）命令，在不同dnode上数据条数一致
8. 随机导出三个子表的数据，导出csv数据文件
9. 关闭不同的dnode节点，导出三个子表的数据，两次导出的三个子表的数据文件对比，数据一致
10. 检查taosBenchmark日志文件，阻塞写入时间不高于4秒

用例6（持续写入场景，3->1）：
1. 在用例5的基础上通过taosBenchmark继续写入数据5000w
2. 执行alter database replica 1；
3. 查看在vnode变更过程中，阻塞数据写入的时间不高于4秒
4. 副本变更过程中同步查询工作正常
5. 通过show vgroups命令查看每个dnode节点添加vnode的角色变化，vnode节点变化过程为3->2->1；通过show transactions查看3副本转换过程是否完成
6. 3副本转换完成后，执行select count（*）命令，数据条数与已存在+写入的数据条数一致
7. 执行alter database replica 3；
8. vnode变更完成后，关闭不同的dnode节点，导出三个子表的数据，两次导出的三个子表的数据文件对比，数据一致
9. 检查taosBenchmark日志文件，阻塞写入时间不高于4秒
10. 执行split vgroup操作，工作正常

用例7（写入场景, 1->3, 服务异常）：
1. 在用例6的基础上通过taosBenchmark继续写入数据5000w
2. 执行alter database replica 3；
3. 查看在vnode变更过程中，阻塞数据写入的时间不高于4秒
4. 副本变更过程中同步查询工作正常
5. 通过show vgroups命令查看每个dnode节点添加vnode的角色变化，vnode节点增加两个，并同时开始数据同步，新增加的两个vnode节点追平数据后，同时成为follower节点；通过show transactions查看3副本转换过程是否完成
6. 在数据追平过程中，多次启停mnode的taosd服务，vnode变更过程继续并能正常完成
7. 3副本转换完成后，执行select count（*）命令，数据条数与3副本下的数据条数一致
8. vnode变更完成后，关闭不同的dnode节点，导出三个子表的数据，两次导出的三个子表的数据文件对比，数据一致
9. 检查taosBenchmark日志文件，阻塞写入时间不高于4秒

用例8（写入场景, 3->1, 服务异常）：
1. 在用例7的基础上通过taosBenchmark继续写入数据5000w
2. 执行alter database replica 1；
3. 查看在vnode变更过程中，阻塞数据写入的时间不高于4秒
4. 副本变更过程中同步查询工作正常
5. 通过show vgroups命令查看每个dnode节点添加vnode的角色变化，vnode节点变化过程为3->2->1；通过show transactions查看3副本转换过程是否完成
6. 在vnode变更过程中，多次启停mnode的taosd服务，vnode变更过程继续并能正常完成
7. 3副本转换完成后，执行select count（*）命令，数据条数与3副本下的数据条数一致
8. 执行alter database replica 3；
9. vnode变更完成后，关闭不同的dnode节点，导出三个子表的数据，两次导出的三个子表的数据文件对比，数据一致
10. 检查taosBenchmark日志文件，阻塞写入时间不高于4秒
11. 执行split vgroup操作，工作正常
用例9（稳定性）：
1. 执行split vgroup、restore dnode, redistribute vgroup
2. 持续写入数据的基础上(写到20亿停止写入)反复执行alter replica 1->3; 3->1的变更；
3. 变更结束后执行split vgroup、restore dnode, redistribute vgroup
4. 每5分钟或60秒注入三节点taosd服务重启异常
5. 副本变更结束查询当前数据条数并记录，持续运行24小时
用例10（特殊场景）
1. 四节点集群写入10亿数据后，执行alter replica 3
2. 完成后，执行drop dnode操作
3. 执行add dnode，restore dnode至完成
4. 执行balance vgroup、redistribute vgroup、split vgroup
5. 查看vgroups状态，多次重启taosd服务，保证尽量多的vnode leader不在第一节点
6. 执行alter replica 1至完成, 数据一致
7. 执行balance vgroup、redistribute vgroup、split vgroup

### 4. 总结：

1. 读写过程中，block数据写入的时间间隔为毫秒级
2. 1->3副本变更过程中，第2个vnode变成follower与第3个vnode节点变成follower的时间间隔缩短，降低2->3过程中集群的单点故障问题为秒级
3. Alter vgroup操作与raft 版本修改相关的操作，如split vgroup、drop dnode、restore dnode、create db、balance vgroup、redistribute vgroup混合操作正常
4. 在多节点集群中，3->1的副本变更过程中的删除leader并重新选主的过程正常，能够完成副本变更
5. 稳定性用例持续运行超过48小时
6. 遗留问题：[TD-25793](https://jira.taosdata.com:18080/browse/TD-25793) [副本变更3-1过程中wal缺失导致重启taosd服务失败](https://jira.taosdata.com:18080/browse/TD-25793)；在三节点同时掉电的场景下，有较低概率导致taosd服务重启失败，需手工恢复
