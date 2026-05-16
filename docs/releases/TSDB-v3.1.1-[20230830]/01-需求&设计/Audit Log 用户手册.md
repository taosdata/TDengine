# Audit Log 用户手册

### 1. 架构及部署

#### 1.1 架构

<diagram type="1"/>


#### 1.2 taosd的部署

taosd的配置（taos.cfg)：
audit（boolean）：是否打开audit （新参数）
monitorFqdn（string）：接收audit的Fqdn，也即taoskeeper的fqdn （现有参数）
monitorPort（int32）：接收audit的port，也即taoskeeper的port （现有参数）
monitorCompaction (bool）：是否压缩上报数据  （现有参数）

### 2. taosKeeper

#### 2.1 上报地址

新api的地址为：/audit
接收数据的格式为json，字段为：

```json
{
    "ts": timestamp,
    "cluster_id": string,
    "user": string,
    "operation": string,
    "db": string,
    "resource": string,
    "client_add": string,
    "details": string
}
```


#### 2.2 数据存储

1. taosKeeper 增加配置项 auditDB，其默认值为 "audit"
2. 表结构
CREATE STABLE operations(ts timestamp, details VARCHAR()， User VARCHAR(25), Operation VARCHAR(20)，db varchar(65), resource varchar(193), client_address(25)) TAGS (clusterID VARCHAR(64) );

| ts (key) | User (column) | Operation (column) | db (column) | resource (column) | client_address | Detail (column) | clusterID (tag) |
| --- | --- | --- | --- | --- | --- | --- | --- |
|  |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |

如果该操作涉及db，db name会被记录到db这个字段，否则为空。resource记录该操作涉及的资源，比如create user， 被操作的user为资源，记录在该字段中。details 是可选信息，根据不同的场景决定是否上报以及上报什么信息在 details 中。

### 3. Operations 

- create database 

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | createDB | dbname1 | n/a | 建库参数 |

- alter database

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | alterDB | dbname1 | n/a | 被修改的参数和新值 |

- drop database

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | dropDB | dbname1 | n/a | N/A |


- alterStb

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | alterStb | dbname1 | stablename1 |  |

- create stable

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | createStb | dbname1 | stablename1 | 超级表 schema |

- dropStb

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | dropStb | dbname1 | stablename1 |  |

- alter user

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | operationUserName1 | alterUser |  | n/a | 被修改的参数和新值（password 不记新值） 例如： sysinfo '1' password xxx |


- create user

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | operationUserName1 | createUser |  | n/a | 其它参数及其值 |

- drop user

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | operationUserName1 | dropUser |  | user | n/a |


- create topic

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | createTopic | DbName1 | topicName1 | 创建 topic 的参数 |

- drop topic

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | dropTopic |  | topicName1 | n/a |


- create dnode

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | createDnode |  | n/a | n/a |

- drop dnode

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | dropDnode |  | dnodeid1 | n/a |


- create mnode

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | createMnode |  | dnodeid1 | n/a |

- drop mnode

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | dropMnode |  | dnodeid1 | n/a |


- login

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | login |  | ip:port | appName 是否有其它信息待确认 |


- create stream

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | createStream |  | streamName | 建流的参数 |

- drop stream

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | dropStream |  | streamName | n/a |

- create qnode

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | createQnode |  | nodeid | n/a |


- drop qnode

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | dropQnode |  | dnodeId | n/a |


- alter dnode

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | alterDnode |  | dnodeId | 修改的参数和新值 |

- GRANT privileges

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | GrantPrivileges |  | targetUserName1 | privilege |

objName:修改DB、table权限时，objName为DBname，修改topic权限时，objName为topicName
- REVOKE privileges

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | RevokePrivileges |  | targetUserName1 | privilege |

objName:修改DB、table权限时，objName为DBname，修改topic权限时，objName为topicName
- flush database (TBD，有可能无法实现）
- compact database 

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | compact | dbName |  | time range (if existing) |


- balance vgroup leader

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | balanceVgroupLead | n/a | n/a | n/a |

- restore dnode/mnode/vnode/qnode

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | restoreDnode |  | nodeid | 具体参数 |

- REDISTRIBUTE VGROUP

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | RedistributeVgroup |  | vgId | n/a |

- BALANCE VGROUP

| ts | User | Operation | db | resource | Detail |
| --- | --- | --- | --- | --- | --- |
|  | user1 | BalanceVgroup | n/a | n/a | n/a |

### 4. 上报时机

在一个操作的所有前置条件检查通过，准备启动事务执行时上报。这种机制能够保证不会漏报，但上报的操作最后不一定成功，有失败的可能性。
如果向 taosKeeper 发送失败，会记录日志，但原始数据丢失，不会再重复发送。
