# Test Report

## 1. 单节点集群的功能测试 （通过）

- 部署单个节点的集群 
  - firstEp localhost:7100
  - fqdn localhost
  - serverPort 7100
- 启动 taosd
- 执行 sql 语句 create database db
- 停止 taosd
- 查看 dnode/dnode.json
```json
{
        "dnodeId":      1,
        "dnodeVer":     "2",
        "clusterId":    "6686674504093644725",
        "dropped":      0,
        "dnodes":       [{
                        "id":   1,
                        "fqdn": "localhost",
                        "port": 7100,
                        "isMnode":      1
                }]
}
```

- 查看 mnode/sync/raft_config.json
```json
...
                        "replicaNum":   1,
                        "myIndex":      0,
                        "nodeInfo":     [{
                                        "nodePort":     7100,
                                        "nodeFqdn":     "localhost",
                                        "nodeId":       "1",
                                        "clusterId":    "0"
                                }]
 ...
```

- 查看 vnode/vnode2/vnode.json
```json
...
"syncCfg.nodeInfo":     [{
                                "nodePort":     "7100",
                                "nodeFqdn":     "localhost",
                                "nodeId":       "1",
                                "clusterId":    "6686674504093644725"
                        }]
... 
```

- 查看 vnode/vnode2/sync/raft_config.json
```json
{
        "RaftCfg":      {
                "SSyncCfg":     {
                        "replicaNum":   1,
                        "myIndex":      0,
                        "nodeInfo":     [{
                                        "nodePort":     7100,
                                        "nodeFqdn":     "localhost",
                                        "nodeId":       "1",
                                        "clusterId":    "6686674504093644725"
                                }]
                },
                "isStandBy":    0,
                "snapshotStrategy":     2,
                "batchSize":    1,
                "lastConfigIndex":      "-1",
                "configIndexCount":     1,
                "configIndexArr":       [{
                                "index":        "-1"
                        }]
        }
}
```

- 增加 dnode/ep.json
```json
{
    "dnodes":   [{
            "id":   1,
            "fqdn": "localhost",
            "port": 7100,
            "new_fqdn": "localhost",
            "new_port": 7200,
            "isMnode":  1
        }]
}
```

- 修改配置文件 taos.cfg
```json
firstEp                localhost:7200
fqdn                   localhost
serverPort             7200
```

- 启动 taosd，查看如下文件，看到 port 从 7100 变为 7200 
  - mnode/sync/raft_config.json
  - vnode/vnode2/sync/raft_config.json
  - vnode/vnode2/vnode.json
- 执行 SQL 语句，看到 dnodes 和 mnodes 的 ep 都发生了相应变化
```json
taos> show dnodes;
     id      |            endpoint            | vnodes | support_vnodes |   status   |       create_time       |              note              |
=================================================================================================================================================
           1 | localhost:7200                 |      2 |           1024 | ready      | 2023-01-16 16:07:41.374 |                                |
Query OK, 1 row(s) in set (0.005328s)

taos> show mnodes;
     id      |            endpoint            |     role     |  status   |       create_time       |
====================================================================================================
           1 | localhost:7200                 | leader       | ready     | 2023-01-16 16:07:41.376 |
Query OK, 1 row(s) in set (0.004669s)

```

- 可以执行创建库、写入数据等操作
- 停止 taosd，查看如下文件
  - dnode/dnode.json 看到 port 从 7100 变为 7200 
  - dnode/ep.json 文件已经被修改为 dnode/ep.json.bak

## 2. 多节点集群的功能测试（通过）

- 部署三个节点的集群
  - firstEp localhost:7100
  - dnode1
    - fqdn localhost
    - serverPort 7100
  - dnode2
    - fqdn localhost
    - serverPort 7200
  - dnode3
    - fqdn localhost
    - serverPort 7300
- 部署三个 mnode，创建三副本的数据库，vgroups 数目四个，用 taosBenchMark 写入 1000 条数据
- 停止集群
- 修改 dnode 
  - firstEp 127.0.0.1:7400
  - dnode1
    - fqdn 127.0.0.1
    - serverPort 7400
  - dnode2
    - fqdn 127.0.0.1
    - serverPort 7500
  - dnode3
    - fqdn 127.0.0.1
    - serverPort 7600
- 重新启动后，查看所有 dnode 的文件
  - mnode/sync/raft_config.json
  - vnode/vnode2/sync/raft_config.json
  - vnode/vnode2/vnode.json
- 查询数据、show dnodes、show mnodes 结果都正常
- 停止集群，查看如下文件
  - dnode/dnode.json
  - dnode/ep.json.bak

## 3. 多节点集群的异常测试

### 3.1 用例一（通过）

- 创建数据库时，手动停止一个节点，让 create-db 的事务处于执行过程中
- 修改集群的 endpoint
- 再次启动集群，查看 create-db 能继续执行

### 3.2 用例二 （通过）

- 修改集群的 endpoint 时，某个 dnode 的新 endpoint 已经在 dnode.json 中存在
- 那么这个 dnode 启动失败

### 3.3 用例三 （未验证）

- 流计算在 vnode 侧存储的是 epset，当 vgroup 移动或者 dnode 的 fqdn 发生变化时，信息发送可能失败，导致流计算无法进行下去，请 @李珲 验证后提 BUG （我这里准备环境不太容易，且这个 BUG 应该是由  @Jicong 修复）可能的步骤如下
- 复现方法一
  - 创建一个流计算，并用 taosBenchMark 写入数据
  - 修改集群的 endpoint
  - 再次启动集群，并继续用 taosBenchMark 写入数据
  - 看到流计算的结果持续输出
- 复现方法二
  - 创建一个流计算，并用 taosBenchMark 写入数据
  - 使用 redistribute vgroup 功能，移动 vgroup
  - 不需要重启集群，写入数据后，观察流计算结果或者数据订阅结果

###
