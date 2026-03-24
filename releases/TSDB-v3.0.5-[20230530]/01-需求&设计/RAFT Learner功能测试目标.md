# RAFT Learner功能测试目标

### 1. 背景

添加Raft第二个成员时，需要把第一个节点上的数据复制到新添加的第二个节点，这个复制过程结束后，才能形成raft集群，在形成raft集群之前，这个raft组不能进行任何的写入和读取

### 2. 功能目标

修改raft成员时，不受数据复制过程的阻塞。

### 3. 解决的问题列表

- TS-2778 [宁德新能源]一副本改为三副本同步速度太慢（blocked by learner）
  该问大幅缓解，问题中花十几个小时同步16G数据，复制速度慢。加入learner可以保证在长时间复制的过程中，写入不受影响。写入受影响时间从十几个小时缩短成秒级。

- TD-22356 优化副本变更过程副本数1到2写入阻塞时间长问题（blocked by learner）
  问题中vgroup写入将被若干小时，同样该问题将被大幅缓解。写入受影响时间从十几个小时缩短成秒级。

- TD-22953 长稳环境有很多事务未结束（blocked by learner）
  该问题中，第二个mnode为同步创建，第三个mnode为异步创建。该问题将被解决，第二个和第三个采用相同的机制创建，都为异步。
  
- TD-22970 使用 RAFT 协议实现成员变更（blocked by learner）
  - TD-22995 [mnode成员变更]cluster status is disorder after successively creating mnode and dropping mnode(blocked by 成员变更)
  采用RAFT协议实现成员变更，将不再使用关闭vnode再打开vnode的实现方式，在节点变成成员后，节点处于空白状态，由于不使用关闭再打开，故无法复用vnode初始流程来复制数据，需要一种方式启动数据复制，即learner机制。

- TD-18702 3.0 支持 split vgroup（blocked by learner）
  Split vgroup会做vgroup的副本数变更，会导致split vgroup过程长时间阻塞写入

### 4. 功能详细描述

添加第二个成员时，这个成员的类型是learner类型。不再阻塞读写。learner为本次新增加的类型。作为learner，加入raft集群后，不参与投票，只接受数据复制。

与之对应的，原有的节点被称之为voter节点，即参与raft的投票。

因为不参与投票，当数据写入时，判断写入是否成功的条件不包括learner节点。在添加第二个成员的这个场景中，raft组仍然可以看做是单节点的raft组，虽然该raft组已经是2个节点。所以数据仍然可以正常写入第一个节点。第一个节点收到数据后，会把数据复制到第二个节点。

第二个节点加入集群后，就会开始追赶第一个节点，当追赶到一定程度后（目前这个条件是个固定条数条件，即2个节点间的日志条数小于10，以后会优化为一个动态条件判断），会把节点的从learner改为voter。改为voter后参与raft投票，包括数据写入投票和选举投票。

当第一个节点和第二个节点的数据差异比较大时，learner追赶第一个节点的数据的方式会采用snapshot的方式。做完snapshot的同步后，仍然会按照条目条件追赶，直到条件达到，从Learner转换成voter。

### 5. 受影响的集群管理命令

Create mnode on dnode id;
语法不改变。
行为改变：创建的mnode先会被创建为learner，随后变为follower/leader。通过show mnode命令可以看到learner。虽然在变为learner的过程中，不阻塞写入，但是create mnode作为global级别命令，仍然会阻塞其他命令执行。

ALTER DATABASE power replica 3;
语法不改变。
行为改变：第二个副本先会被创建为learner，复制完数据后变为follower/leader。通过show vgroups命令可以看到learner。在数据复制过程中，数据仍然可以写入。

### 6. 目前的限制

在成员变更的过程中，不会全部过程都不会被阻塞，本次修改会让复制过程不会阻塞写入。以alter db replica为例。
单副本 --> leader/learner --> learner追齐--> leader/follower--> leader/2follower -->follower追齐
          |<-----阻塞---------->|  |<--不阻塞--->| |<--------------阻塞------------------>| |<--不阻塞--->|

如果数据量比较大，learner追齐会占比较长的时间。
