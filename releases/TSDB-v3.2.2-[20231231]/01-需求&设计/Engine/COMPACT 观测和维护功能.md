# COMPACT 观测和维护功能

## 1. 问题描述

具体问题描述可参考：
TS-4229

## 2. 功能设计

1. Compact ID
每个 COMPACT 命令如果被接收，返回一个 transaction id 。如果该命令被拒绝，没有 compact ID 返回。
现在的 compact 命令执行后，直接返回，没有任何反馈。为了保证任务的观测和维护，需要服务端返回请求接收情况。如果请求被接收，则需要返回一个 id，供运维人员后续操作和查询。下面是两个返回的例子：
| result | id |
| --- | --- |
| accepted | 161aed0a-7ee1-11ee-b962-0242ac120002 |

| result | id |
| --- | --- |
| rejected | N/A |
| reason | Detailed explanation |

1. 查看系统中正在运行的 compact 操作
```sql
show compacts;

## 3. output

-- dbname  -- compact ID -- start time --
```

1. 查看 compact 进度的
增加 SQL 语句，可以单独查看 compact 任务的进度
```sql
show compact <compact id>;
```

输出格式如下，会展示每个 vnode 中的文件组总数以及完成 compact 的文件组数量，作为粗略的进度来展示。鉴于每个文件组大小会有差异，这两个数值的比例并不能精确代表进度百分比，仅做参考。
vnode 以 <vgroup_id, dnode_id> 二元组来表示。
| vgroup_id | dnode_id | number_fileset | finished | start_time |
| --- | --- | --- | --- | --- |
| 2 | 1 | 5 | 1 | 45239.5 |
| 2 | 2 | 5 | 4 | 45239.5 |
| 2 | 3 | 5 | 3 | 45239.5 |

1. 结束 compact 任务
```sql
kill compact <compact id>;

## 4. example 

 kill compact `161aed0a-7ee1-11ee-b962-0242ac120002`;
```

## 5. 功能实现

### 5.1 解析器改动

1. 增加查看 compact 进度的 SQL 语句支持
2. 增加结束 compact 任务的 SQL 语句支持

### 5.2 消息部分改动

1. compact 返回消息包括请求结果以及任务的 transaction id
2. 增加查看 compact 进度的消息
3. 增加结束 compact 任务的消息 

### 5.3 MNODE 部分改动

1. mnode 处理 compact 请求需要检查 vnode 上是否有 compact 任务，如果有，则拒绝请求，如果没有，则声称一个 transaction id，下发 compact 任务到涉及到的 vnode，然后将 transaction id 返回客户端。
2. mnode 收到查看 compact 任务时，下发子查询到各个 vnode，分别查询相应 id 的 compact 任务进度，汇总后，返回给客户端。
3. mnode 收到结束 compact 任务后，分别向各个 vnode 下发结束 compact 任务。

### 5.4 VNODE 部分改动

1. vnode 建立 compact 任务监控机制
2. 将 compact 按照文件组建立多个子任务，子任务完成后，修改监控变量
3. vnode 支持 compact 状态查询和终止 

## 6. 存在问题

1. 同时发起多个 compact 任务时的情况？
