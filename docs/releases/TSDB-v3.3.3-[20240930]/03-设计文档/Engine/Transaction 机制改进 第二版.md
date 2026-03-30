# Transaction 机制改进 第二版

## 1. 背景

Kill transaction经常出问题，调试事务问题时，必须要查日志才能确定问题。

## 2. 变更历史


| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/6/14 | 0.1 | 陈东明 |  |
|  |  |  |  |
|  |  |  |  |
|  |  |  |  |

## 3. 定义

系统事务：系统定时发起的一些操作，比如更新集群的 uptime。
用户事务：用户发起的操作

## 4. 行为说明

### 4.1 Kill transaction

#### 4.1.1 优化目标

安全的kill transaction。
1.不能保证安全kill的transaction，在kill时提示错误，即封掉kill transaction操作。
2.梳理现有事务，对可以保证安全kill的transaction，放开kill transaction功能
3.实现一个新的kill transaction方式，改造一个现有事务，可以通过新的方式kill transaction。

遗留问题，执行一半的事务的兼容性问题，客户环境中为什么会不兼容？

#### 4.1.2 为什么要封掉kill transaction

对于有异常错误导致事务执行被卡住的情况，没有办法去结束这个事务，并且把系统恢复到事务执行前，因为如果 存在一个出错的操作，比如节点宕机或者磁盘损坏，这个错误操作之前的所有操作的恢复操作，也就是逆向操作，也是无法执行。所以针对异常错误，唯一的办法是解决异常后，保证事务可以继续执行，重试出错的操作。保证事务可以继续进行，问题不在于事务机制（目前事务机制已存在消息重发机制），而是在于每个具体事务的涉及到的dnode和vnode上的操作，都可以重复操作，并且在异常恢复后，重发消息可以恢复。这一点，需要对每个事务进行排查和测试。

#### 4.1.3 查看事务是否可以kill

在show transactions添加killable字段，指示该事务是否可以执行kill transaction

#### 4.1.4 三种kill transaction的方式

1. 现有的kill transaction
目前kill transaction的实现：强制把所有的action，状态标记为消息发送成功，并且成功收到response。然后事务继续执行,元数据会执行成功，比如创建db的事务，mnode中会成功创建db的对象。按照这个实现，只有那些只发消息的事务，能被安全的kill掉，其他修改mnode中对象的事务，都会导致mnode的元数据，与实际不符合。
举例，2个可以这种kill的事务：
1.balance vgroup leader: 这个事务是让vgroup重新选举，可以被安全kill
2.kill compact: kill compact 这个事务原本就可以重复执行，在vnode层面，这个命令可以反复执行。

1. 新的一种kill transaction的方式
强行删除mnode中事务的数据，这样就相当于kill掉了事务，但是mnode中的元数据和vnode中的数据会出现不完整的情况。比如，强行中断副本变更事务，show vgroups时，会看到变更一般的vgroup，可能处在只有2个副本，并且能处于learner状态。
处于不完整的事务，必须通过再执行执行一遍副本变更事务，通过重新运行事务，覆盖掉之前不完整的状态。
所以才作用这种方式的事务，必须改造成可以反复的以覆盖方式执行的方式。以往的副本变更事务是基于第一次执行的假设下实现的，比如检测到某个vgroup的副本数量不等于1，事务执行就会报错。需要将事务改造成能检测到vgroup不完整，并且可以重复执行，覆盖原有事务。

如果采用这种方式的 kill，遗留下来的元数据的兼容性问题，是否存在？

1. 本次未实现的kill transaction方式
当kill transaction时，回滚所有已经执行的操作。

停止并且回滚一个事务，只能针对没有任何异常的事务。但是目前的kill transations的实现不是”停止并且回滚一个事务“，也就是原子性事务。实现回滚的目前的问题在于：
a.不但是事务机制本身需要回滚功能，也就是mnode中的事务数据需要回滚，目前的事务机制需要给kill命令添加这个回滚操作，需要给事务添加这个机制。
b.mnode发出去的消息也需要回滚，比如createdb事务，给vnode发送createvnode消息，会创建一个vnode，回滚时需要vnode执行反向操作（rollback操作），删除vnode。要实现这一点，需要这个事务，针对每个消息操作，添加一个逆向操作。需要回滚的事务，需要添加rollback操作，这时这个事务可以显示为killable。
总体来说，实现回滚一个事务优先级不高，目前最常见事务的问题，还是在异常的情况下的事务卡住，也就是2.中提到的场景。
另外，回滚操作也可能因异常被卡住，在这种场景下，也只能通过重试，将回滚操作完成。

#### 4.1.5 区分Kill transaction方式

本次实现第二种kill事务，实现了第二种kill之后，某些事务采用第一种，某些采用第二种，需要有种办法指明某个事务采用的方式。
在show transaction中添加killmode字段，该字段有2个值：
Skip: 表示采用第一种方式的事务
Interupt: 表示采用第二种方式的事务

#### 4.1.6 现有事务梳理，确定采用第一种方式的事务

- 系统事务

|  | killable |
| --- | --- |
| create-acct | No |
| update-arbgroup | No |
| create-cluster | No |
| update-uptime | No |
| update-compact-progress | No |
| update-dnode-obj | No |
| update-dnode-obj | No |
| tmq-reb | No |
| stream-task-reset | No |
| stream-task-update | yes |
| update-grant-log | No |

- 用户事务

|  | killable |
| --- | --- |
| kill-compact | yes |
| recover-csm | No |
| clear-csm | No |
| subscribe | No |
| create-db | No |
| alter-db | No |
| drop-db | No |
| create-dnode | No |
| drop-dnode | No |
| create-func | No |
| drop-func | No |
| create-stb-index | No |
| drop-index | No |
| create-mnode | No |
| drop-mnode | No |
| create-qnode | No |
| drop-qnode | No |
| create-sma | No |
| drop-sma | No |
| create-tsma | No |
| drop-tsma | No |
| create-snode | No |
| drop-snode | No |
| create-stb | No |
| alter-stb | No |
| drop-stb | No |
| drop-tbs | No |
| drop-cgroup | No |
| create-topic | No |
| drop-topic | No |
| create-user | No |
| alter-user | No |
| drop-user | No |
| red-vgroup | No |
| split-vgroup | No |
| balance-vgroup | No |
| stream-create | No |
| stream-checkpoint | yes |
| stream-pause | yes |
| stream-resume | yes |
| stream-drop | No |
| update-cluster-active | No |
| compact-db | No |
| restore-dnode | yes |
| balance-vg-leader | yes |
| create-view | No |
| drop-view | No |



#### 4.1.7 改造副本变更事务，采用第二种方式

去掉副本数量的检测，让副本变更事务可以在执行一半的情况下，重复执行。
只涉及1->3 和 3->1，alter db的其他操作未涉及。

#### 4.1.8 后续的任务

1.将不可以kill的事务，采用第二种方式逐个改造
2.实现第三种，rollback机制。逐个改造事务，支持第三种kill。在实现第三种方式后，添加新的kill transaction force执行第一种和第二种方式，将现有的kill transaction让给第三种方式。

### 4.2 查看事务详情

Show transaction {id} 命令，显示指定事务在当前阶段，比如RedoAction阶段或者在CommitAction阶段，所有Action的列表。
Show transaction {id}的价值在于，而已看到一个事务一共有多少步，当前在第几步，每一步的执行时间是多少，有助排除问题所在。

|  | Operation(Operation meaning) | IsOperated | startTime | endTime | Target | result |
| --- | --- | --- | --- | --- | --- | --- |
| redoAction:0 | sdbType:vgroup, sdbStatus:ready （update vgroup） | written:0 |  |  |  |  |
| redoAction:1 | msgType:alter-replica (send alter-replica msg) | sent:0, received:0 | 2024-05-29T07:11:47.454+0000 | 2024-05-29T07:11:47.825+0000 | numOfEps:1 inUse:0 ep:0-localhost:6230 | errCode:0x0(success) |
| redoAction:2 | msgType:create-vnode (send create-vnode msg) | sent:1, received:0 | 2024-05-29T07:11:47.829+0000 | 2024-05-29T07:11:49.231+0000 | numOfEps:1 inUse:0 ep:0-localhost:6130 | errCode:0x111(Action in progress) |

 
存在的问题：
1. Operation不包含具体信息，并且并不好理解。不能显示要写入的vgroup数据是什么，或者发送的alter-replica消息是什么。并且这些信息对用户来说也并不好理解。开发人员用来排障更有用一些。
2. IsOperated是事务状态，不是history，容易导致误解。比如例子中，事务执行到第3步redoAction:2，已经把消息发送出去，所以sent是1，正在等待回复，所以received是0。但是已经执行的步骤2，redoAction:1，sent和recieved仍然0，虽然这个消息已经发送并且成功接收了回复，这是因为事务可能被重新执行，重新执行时action要重新发送这些消息。

不能解决的问题：
当异常出现，一个事务被卡住时，一般是发给dnode的消息没有回复，这时从事务的信息中，无法排除问题，还是需要进入到dnode的日志中，查看问题所在。

本次未添加事务源头SQL，显示事务源头SQL不能在事务机制优化上解决，优化事务机制，只能添加一个SQL字段，这个字段是空的，必须修改每个具体事务，填充上这个字段。另外，用户事务的名称大多能对应上是什么SQL，另外迷惑的可能是系统事务，但是系统事务本身不会有SQL。

## 5. 性能

无

## 6. 兼容性

无。

## 7. 运维

无。

## 8. 使用场景

1.对于不可kill的事务，在遇到外部异常的情况下，在异常恢复后，事务恢复正常执行。
2.对于可以kill的事务，并且采用skip mode的事务，在事务执行的过程中，可以kill掉事务，并且mnode中相关的数据，未出现不完整，不一致的情况
3.第三种场景，来源自TS-4968 3.2.1.0 从1-3扩展副本时遭遇频繁crash
分析了core的堆栈，出现core是个assert，并且这个assert在用户的版本（3.2.1.0）之后有修改，也就是这个core已经在新版本里修复了。但是这个core导致副本变更的事务不能执行完。在升级到3.3.0.3之后，不再出现core，副本变更事务继续执行，成功完成了一个vgroup的副本变更。事务在执行下一个vgroup时，出现了invalid message的报错，这个报错是由于事务是旧版本生成的，旧事务与新版本不兼容。

## 9. 约束和限制

约束：无
限制：无

## 10. 常见错误和排查

## 11. 可观测性

## 12. 安装和卸载

## 13. 文档

修改kill transaction和show transaction部分。

## 14. 参考文档

## 15. 附录

在程序中，针对某个具体的事物，在代码中添加 setTransactionAble()，显示设置某个事务可以被kill
