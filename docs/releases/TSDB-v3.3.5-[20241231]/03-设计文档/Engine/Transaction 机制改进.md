# Transaction 机制改进

## 1. 背景

1. Kill transaction经常出问题，导致元数据与集群实际情况不一致。
2. Show transaction显示的信息不够，调试事务问题时，大多要查日志才能确定问题。

## 2. 变更历史


| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/12/10 | 0.1 | 陈东明 |  |
|  |  |  |  |
|  |  |  |  |
|  |  |  |  |

## 3. 定义

系统事务：系统定时发起的一些操作，比如更新集群的 uptime。
用户事务：用户发起的操作

## 4. 行为说明

### 4.1 添加transaction killable属性

#### 4.1.1 优化目标

将所有事务默认改为不可以被kill，只有killable=true的事务可以被kill。

#### 4.1.2 现有事务梳理

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
| stream-task-update | No |
| update-grant-log | No |

- 用户事务

|  | killable |
| --- | --- |
| kill-compact | No |
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
| stream-checkpoint | No |
| stream-pause | No |
| stream-resume | No |
| stream-drop | No |
| update-cluster-active | No |
| compact-db | No |
| restore-dnode | yes |
| balance-vg-leader | yes |
| create-view | No |
| drop-view | No |

### 

#### 4.1.3 show transactions; 命令中添加killable字段

添加killable字段，指示该事务是否可以执行kill transaction

### 4.2 添加 show transation {id}; 新命令

Show transaction {id} 命令，显示指定事务在当前阶段，比如RedoAction阶段或者在CommitAction阶段，所有Action的列表。
Show transaction {id}的价值在于，而已看到一个事务一共有多少步，当前在第几步，每一步的执行时间是多少，有助于排除问题所在。

| transaction_id | action | obj_type | result | target | detail |
| --- | --- | --- | --- | --- | --- |
| 12 | redoAction:0(msg) | create-vnode(s:1,r:1) | errCode:0x0(success) | ep:0-47d01:6130,(1:0) | startTime:2024-11-14T12:07:01.237, endTime:2024-11-14T12:07:01.567 |
| 12 | redoAction:1(sdb) | vgroup(2) | rawWritten:0 | sdbStatus:ready |  |
| 12 | redoAction:2(msg)<-last | alter-replica(s:1,r:0) | errCode:0x111(Action in pro... | ep:0-47d01:6130,(1:0) | startTime:2024-11-14T12:07:01.237, endTime:2024-11-14T12:07:01.567 |


- **transaction_id:**事务id
- **action:**该列由以下信息组成:
   - 如果事务处在redo阶段，则该列显示为redoAction:xx，如果事务处在commit阶段，则该列显示为comitAction:xx，其中xx表示该action是这个节点的第几个action。
   - 括号中标识表示该action的类型，目前有2种类型，msg，表示该action是在发送一个消息，sdb表示该action在往mnode中写入一条数据。
   - <-last指示该action是上次执行的action
- **obj_type:**该列表示该action的对象类型，如果是msg类型action，则该列显示的是所发送的消息类型，如果是sdb类型，则该列显示写入mnode中的数据的类型，除此以外，该列还包含了额外信息：
   - 如果是msg类型action，还携带了该消息是否发送，并且是否收到消息的response，采用(s:1,r:0)形式表示，s代表发送，r代表response，1代表已经发送或者收到，0代表未发送或者收到
   - 如果是sdb类型，并且是vgroup类型的数据，括号中的数字表示vgroupid
- **result：**该列表示action的操作结果
   - 如果是msg类型action，该列显示的是该action的执行的code，code一般为
      - success，表示操作成功，
      - Action in progress，表示action在操作中
      - xx(某个具体的错误号)，表示action执行完，并且出现错误
   - 如果是sdb类型action，该列显示的是该数据是否已经写入mnode，一般表示为rawWritten:x，x为1时表示写入，x为0时表示未写入
- **target:**该列表示
   - 如果是msg类型action，该列显示了该消息发送目标是谁，以一个dnode ep列表表示，后面(x,x)表示，分别表示一共有几个ep，该action实际使用的是第几个
   - 如果是sdb类型action，该列显示的形式为sdbStatus:ready，一般有ready，creating，deleted等，分别表示修改，创建和删除操作。
- **detail:**该列显示了msg类型消息的详细信息，也即消息具体的发送时间（startTime），和response收到时间(endTime)。

## 5. 性能

无

## 6. 兼容性

无。

## 7. 运维

无。

## 8. 使用场景

## 9. 约束和限制

### 9.1 存在的问题：

1. obj_type不包含具体信息，并且并不好理解。不能显示要写入的vgroup数据是什么，或者发送的alter-replica消息是什么。并且这些信息对用户来说也并不好理解。开发人员用来排障更有用一些。
2. obj_type是事务状态，不是history，容易导致误解。比如例子中，事务执行到第3步redoAction:2，已经把消息发送出去，所以sent是1，正在等待回复，所以received是0。但是已经执行的步骤2，redoAction:1，sent和recieved仍然0，虽然这个消息已经发送并且成功接收了回复，这是因为事务可能被重新执行，重新执行时action要重新发送这些消息。

### 9.2 不能解决的问题：

当异常出现，一个事务被卡住时，一般是发给dnode的消息没有回复，这时从事务的信息中，无法排除问题，还是需要进入到dnode的日志中，查看问题所在。

## 10. 常见错误和排查

## 11. 可观测性

## 12. 安装和卸载

## 13. 文档

修改kill transaction和show transaction部分。

## 14. 参考文档

## 15. 附录

### 15.1 事务框架的支持

为了支持以上行为，现有事务机制会提供一个机制，可以指定某个事务为killable事务。
在程序中，针对某个具体的事物，在代码中添加 setTransactionAble()，显示设置某个事务可以被kill

### 15.2 哪些事务可以被设置为killable

目前kill transaction的实现：强制把所有的action，状态标记为消息发送成功，并且成功收到response。然后事务继续执行,元数据会执行成功，比如创建db的事务，mnode中会成功创建db的对象。按照这个实现，只有那些只发消息的事务，能被安全的kill掉，其他修改mnode中对象的事务，都会导致mnode的元数据，与实际不符合。

### 15.3 为什么要禁用kill transaction

1. 在现有的实现机制下，对于有异常错误导致事务执行被卡住的情况，没有办法去结束这个事务，并且把系统恢复到事务执行前，因为如果 存在一个出错的操作，比如节点宕机或者磁盘损坏，这个错误操作之前的所有操作的恢复操作，也就是逆向操作，也是无法执行。所以针对异常错误，唯一的办法是解决异常后，保证事务可以继续执行，重试出错的操作。保证事务可以继续进行，问题不在于事务机制（目前事务机制已存在消息重发机制），而是在于每个具体事务的涉及到的dnode和vnode上的操作，都可以重复操作，并且在异常恢复后，重发消息可以恢复。这一点，需要对每个事务进行排查和测试。

2. 停止并且回滚一个事务，只能针对没有任何异常的事务。但是目前的kill transations的实现不是”停止并且回滚一个事务“，也就是原子性事务。实现回滚的目前的问题在于：
  a.不但是事务机制本身需要回滚功能，也就是mnode中的事务数据需要回滚，目前的事务机制需要给kill命令添加这个回滚操作，需要给事务添加这个机制。
  b.mnode发出去的消息也需要回滚，比如createdb事务，给vnode发送createvnode消息，会创建一个vnode，回滚时需要vnode执行反向操作（rollback操作），删除vnode。要实现这一点，需要这个事务，针对每个消息操作，添加一个逆向操作。需要回滚的事务，需要添加rollback操作，这时这个事务可以显示为killable。
  总体来说，实现回滚一个事务优先级不高，目前最常见事务的问题，还是在异常的情况下的事务卡住，也就是2.中提到的场景。
  另外，回滚操作也可能因异常被卡住，在这种场景下，也只能通过重试，将回滚操作完成。

### 15.4 本次未实现的优化需求

本次未添加事务源头SQL，显示事务源头SQL不能在事务机制优化上解决，优化事务机制，只能添加一个SQL字段，这个字段是空的，必须修改每个具体事务，填充上这个字段。另外，用户事务的名称大多能对应上是什么SQL，另外迷惑的可能是系统事务，但是系统事务本身不会有SQL。
