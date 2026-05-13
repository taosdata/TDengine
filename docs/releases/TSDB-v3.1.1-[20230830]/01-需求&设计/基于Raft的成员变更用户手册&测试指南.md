# 基于Raft的成员变更用户手册&测试指南

### 背景

将membership change的实现改成基于Raft 协议。

### 范围

该设计目前只涉及vnode的membership change。
涉及的命令：
ALTER DATABASE power replica 1;
ALTER DATABASE power replica 3; 

### 设计目标

1.利用raft本身的健壮性，解决目前在成员变更中存在的问题
已有jira：TD-22995 [mnode成员变更]cluster status is disorder after successively creating mnode and dropping mnode
2.之前实现learner后，阻塞写入的时长已经大大缩短，实现基于raft的成员变更后，将遗留的秒级阻塞也基本去除
3.之前实现learner时，遗留一个问题，在2->3的时候会有单点故障的风险，此风险存在的时间长度取决于要同步的存量数据的多少，本次改动进一步将这个风险的时长缩短到秒级。

### 总体设计思路

在标准raft中，有2种configuration change方案：
single-server configuration change：一次变更一个server
arbitrary configuration changes：变更任意多个server

本次更改采用了single-server configuration change。

基于single-server configuration change方案，我们成员变更的过程会改变为：
1->2->3
add learner1 --> add learner2 --> wait 2 learners catch up --> (1->2) --> check --> (2->3) --> check

3->2->1
(3->2) --> check --> drop node3 --> (2->1) -- > check --> drop node2

Note: check为mnode给vnode发送的altercomfirm

#### 没有采用arbitrary configuration changes的考量是：

1.两者都需要变化2次，arbitrary configuration changes在我们的场景下，没有优势
arbitrary configuration changes需要经历的过程：C-old -> C-old,new --> C-new
2.arbitrary configuration changes实现复杂，需要2套configuration，即在syncnode中保存两套成员信息，目前我们已经有一套，需要增加一套


#### 两节点单点故障问题

- 与做learner时的1->2->3的区别本次更改后仍然存在2节点阶段，挂一个节点，集群不可用的问题。但是2节点阶段的时长排除了节点追齐数据的时间，缩短为成员变更一个请求处理的时间。
- 如果采用arbitrary configuration changes方案的3->3->1，同样不能解决这种宕机，区别在于只是节点1宕机时，集群才会宕掉。

### 流程

作为arbitrary configuration changes复杂性的另外一个场景，也是single-server configuration change在3->1场景的特殊性，存在removed leader的问题。
arbitrary configuration changes无法避免地要解决这个问题，即在arbitrary configuration changes方案中是必解问题。
在single-server configuration change方案中，可以采用transfer leader的方法避免这个问题。所以3->1的流程为：

 (force node3 become follower, 3->2) --> check --> drop node3 --> (force node2 become follower, 2->1) -- > check --> drop node2

1->3的流程无变化：
add learner1 --> add learner2 --> wait 2 learners catch up --> (1->2) --> check --> (2->3) --> check

这里有个值得注意的变化，有增加了force node become follower这样的操作，但是这个操作是随机的，有可能导致多次执行，这样使得这个过程的时长不是一个固定值。这一点与本次变更前是不一样的，之前的实现中，是确定的只发生一次选举，过程的时长相对是固定的。

### 测试重点

#### 1.其他受影响的操作

alter hash
启动流程（即open vnode的流程）

#### 2.反复变更

反复变更是指 1->3->1->3....，这样的反复执行副本变更，并且中间夹杂着数据写入和节点的重启

#### 3.变更中节点宕机，重启

在成员变更的过程中，将节点宕机或者重启
这部分的正确性，有raft保证，所以不太好测试，就算是测出问题，也是概率性不好复现的问题，如果有问题，那就是实现的有问题，加上混沌测试比较好
宕机、重启测试分类2类：
1.vnode节点重启，导致这个vnode的leader catching, config changing, 中断和继续
2.mnode节点重启，导致事务的中断和重启

#### 4.变更中写入是否受影响

验证在成员变更的过程中，是否影响数据写入

#### 5.变更后的数据同步，snapshot

本次修改的内容，修改了数据同步和snapshot的状态数据，需验证在副本变更完成后，数据同步和snapshot是否正常。

#### 6.稳定性测试

稳定性包括两部分：
1.成员变更功能本身是否稳定
2.本次变更，修改了数据写入的流程，特别是在从单副本（单线程处理）变为多副本（多线程处理），或者多副本变为单副本，修改了线程模型的切换过程，之前是通过open/close vnode来实现，相对简单，新实现里的线程模型切换变得非常复杂，需要重点验证，表现为整体的稳定性
目前我已经自己跑过1小时稳定运行，期望稳定性测试时长至少过3天，改动了基本数据写入流程，要进行充分测试。

#### 7.distruptive server的观察

标准Raft成员变更中，存在distruptive server问题，即被删除的节点，发起选举，导致留在集群中的节点也重新选举，导致集群短时不可用。
这个问题，不影响正确性，只影响可用性。
本次变更，并未按标准raft实现方案，对该问题进行处理，但是从实现上做了些优化，这个问题出现概率和时长应该不会是明显问题，在测试中注意观察是否符合预期。
问题的表现是，在变更中重新选举，导致变更过程变长

#### 8.看不到成员变更的过程

在使用raft成员变更后，变更过程时长非常短，已观察的结果是不会超过2秒。所以通过show vgroup命令来观察变更过程，如果命令的定期执行时间大于1秒，基本上观察不到， 1->2->3的过程，基本上是只能看到1->3，3->1，一下子就完成了

#### 9.删除leader

前面提到删除leader的问题，在测试中需要注意一个问题。现在成员变更的事务实现部分，是固定流程，先删除vgroup中的vnode3，再删除vnode2，保留vnode1，如果vnode1不是leader，会强制vnode1变成leader，再继续。所以这个行为要和前面讲的distruptive server进行区分。
并且这个删除leader本身也是一个功能点，需要进行测试

#### 10.vnode.json

目前成员信息，分别保存在vnode.json和raft_config.json两个文件中。本次变更中，添加learner的机制，仍然利用了现有的机制，故依赖vnode.json中的信息，并会修改vnode.json文件。
但是后面的过程中，为了避免2个文件修改的原子性问题，其他环节都不在使用vnode.json，只更新raft_config.json文件，所以会出现2个文件不一致的情况。这点需要注意。

#### 11.raft_config.json版本问题

本次更改为raft_config.json增加了版本，但是这引起了一个兼容问题，所以增加了兼容处理。这些兼容处理涉及多个操作，包括create db， restore dnode，split vgroup, balance vgroup, redistribute vgroup, drop dnode。需要对这些操作与alter replica做兼容测试，即混合2种操作的测试，比如，create db后做alter replia，alter replica后split vgroup， split vgroup后alter replica等
