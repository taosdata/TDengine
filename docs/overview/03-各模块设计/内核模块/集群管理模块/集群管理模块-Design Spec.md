# 集群管理模块-Design Spec

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-01-08 | 2025-01-08 | 1.0 | 陈东明 | 正式发布 |
| 2025-12-30 | 2025-12-31 | 1.1 | 程洪泽 | 重构补充内容 |

## 2. 引言

### 2.1 目的

本文档旨在描述 TDengine 集群管理模块的设计，包括其架构、组件、数据结构和流程设计，为开发、测试和维护提供技术参考。

### 2.2 范围

本文档涵盖 TDengine 集群管理模块的以下方面：
- 集群节点的管理（DNode、MNode、QNode、SNode）
- 虚拟节点组（VGroup）的管理
- 集群配置和部署
- 性能和安全考虑

### 2.3 受众

本文档面向以下人员：
- TDengine 开发人员
- 系统架构师
- 测试工程师
- 技术支持人员

## 3. 术语

- **TDengine**：一种高性能、支持分布式架构的时间序列数据库。
- **DNode**：DNode 是 TDengine 服务器侧执行代码 `taosd` 在物理节点上的一个运行实例。在一个 TDengine 系统中，至少需要一个 DNode 来确保系统的正常运行。每个 DNode 包含零到多个逻辑的虚拟节点（VNode），但管理节点、弹性计算节点和流计算节点各有 0 个或 1 个逻辑实例。
- **MNode**：MNode（管理节点）是 TDengine 集群中的核心逻辑单元，负责监控和维护所有 DNode的运行状态，并在节点之间实现负载均衡。作为元数据（包括用户、数据库、超级表等）的存储和管理中心，MNode 也被称为 MetaNode。
- **QNode**：QNode（计算节点）是 TDengine 集群中负责执行查询计算任务的虚拟逻辑单元，同时也处理基于系统表的 `show` 命令。为了提高查询性能和并行处理能力，集群中可以配置多个 QNode，这些 QNode 在整个集群范围内共享使用。
- **SNode**：流计算节点，负责流式计算任务。
- **VGroup**：虚拟节点组，由多个 VNode 组成的数据存储单元。

## 4. 概述

### 4.1 架构

集群管理模块整体架构包含5个核心组件：
- **mndDnode**：负责Dnode节点的相关管理
- **mndMnode**：负责mnode节点的相关管理
- **mndQnode**：负责Qnode节点的相关管理
- **mndSnode**：负责Snode节点的相关管理
- **mndVgroup**：负责vgroup的相关管理，被mndDnode模块调用
<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"graph TB\n    subgraph \"集群管理模块架构\"\n        SMnode[SMnode主结构] --\u003e SDB[SDB系统数据库]\n        SMnode --\u003e WAL[WAL预写日志]\n        SMnode --\u003e Sync[同步管理]\n        \n        subgraph \"核心管理组件\"\n            MndDnode[mndDnode模块]\n            MndMnode[mndMnode模块]\n            MndQnode[mndQnode模块]\n            MndSnode[mndSnode模块]\n            MndVgroup[mndVgroup模块]\n        end\n        \n        subgraph \"数据存储层\"\n            SDB --\u003e DnodeTable[DNode表]\n            SDB --\u003e MnodeTable[MNode表]\n            SDB --\u003e QnodeTable[QNode表]\n            SDB --\u003e SnodeTable[SNode表]\n            SDB --\u003e VgroupTable[VGroup表]\n        end\n        \n        subgraph \"外部接口\"\n            RPC[RPC通信接口]\n            CLI[CLI命令行接口]\n            API[REST API接口]\n        end\n        \n        SMnode --\u003e MndDnode\n        SMnode --\u003e MndMnode\n        SMnode --\u003e MndQnode\n        SMnode --\u003e MndSnode\n        SMnode --\u003e MndVgroup\n        \n        MndDnode --\u003e DnodeTable\n        MndMnode --\u003e MnodeTable\n        MndQnode --\u003e QnodeTable\n        MndSnode --\u003e SnodeTable\n        MndVgroup --\u003e VgroupTable\n        \n        MndDnode --\u003e RPC\n        MndMnode --\u003e RPC\n        MndQnode --\u003e RPC\n        MndSnode --\u003e RPC\n        MndVgroup --\u003e RPC\n        \n        RPC --\u003e CLI\n        RPC --\u003e API\n    end\n    \n    subgraph \"集群节点\"\n        DNode1[DNode 1]\n        DNode2[DNode 2]\n        DNode3[DNode 3]\n        MNode[MNode]\n        QNode[QNode]\n        SNode[SNode]\n    end\n    \n    RPC --\u003e DNode1\n    RPC --\u003e DNode2\n    RPC --\u003e DNode3\n    RPC --\u003e MNode\n    RPC --\u003e QNode\n    RPC --\u003e SNode\n    \n    Client[客户端] --\u003e CLI\n    Client --\u003e API\n    \n    style SMnode fill:#e1f5fe\n    style MndDnode fill:#f3e5f5\n    style MndMnode fill:#e8f5e8\n    style MndQnode fill:#fff3e0\n    style MndSnode fill:#fce4ec\n    style MndVgroup fill:#e0f2f1\n    style SDB fill:#f5f5f5\n    style RPC fill:#ffecb3\n","theme":"default","view":"chart"}"/>

**架构说明**：
1. **SMnode主结构**：集群管理模块的核心控制结构，管理所有组件和资源
2. **核心管理组件**：5个专门的管理模块，各司其职
3. **数据存储层**：基于SDB的内存数据库，存储各类节点信息
4. **外部接口**：提供RPC、CLI和REST API三种访问方式
5. **集群节点**：实际运行的TDengine节点，通过RPC与集群管理模块通信
6. **客户端**：用户通过CLI或REST API管理集群
**数据流向**：
- 管理指令：客户端 → 外部接口 → 核心管理组件 → 数据存储层
- 状态同步：集群节点 → RPC接口 → 核心管理组件 → 数据存储层
- 配置下发：数据存储层 → 核心管理组件 → RPC接口 → 集群节点

### 4.2 技术

- 编程语言：C
- 通信协议：TDengine内部消息协议
- 数据存储：内存哈希表用于节点信息管理
- 并发控制：多线程同步机制

### 4.3 依赖项

- TDengine核心库
- 网络通信模块
- 日志记录模块
- 配置管理模块

## 5. 设计考虑

### 5.1 假设和限制

- 假设集群节点网络连通性良好
- 假设节点时钟基本同步
- 限制：单个集群支持的节点数量受内存限制

### 5.2 设计模式和原则

- 模块化设计：各节点类型管理分离
- 单一职责原则：每个组件负责特定类型的节点管理
- 依赖倒置原则：高层模块不依赖低层模块，二者都依赖抽象

### 5.3 风险和缓解措施

- **风险**：节点故障导致数据不一致
  - **缓解措施**：实现节点状态监控和自动恢复机制
- **风险**：网络分区导致脑裂
  - **缓解措施**：使用多数派选举和超时机制
- **风险**：配置错误导致集群不可用
  - **缓解措施**：提供配置验证和回滚机制

## 6. 详细设计

### 6.1 组件设计

#### 6.1.1 mndMnode模块

**功能概述**：
- 管理节点（mnode）的创建、删除和状态管理
- 提供mnode选举和同步配置管理
- 处理mnode相关的RPC请求
**核心函数**：
- `mndInitMnode()`: 初始化mnode管理模块
- `mndProcessCreateMnodeReq()`: 处理创建mnode请求
- `mndProcessDropMnodeReq()`: 处理删除mnode请求
- `mndRetrieveMnodes()`: 查询mnode列表
- `mndAcquireMnode()`: 获取mnode对象
- `mndReleaseMnode()`: 释放mnode对象
**关键特性**：
- 支持最多3个mnode节点
- 支持mnode角色管理（voter/learner）
- 提供mnode状态同步机制
- 集成事务处理机制

#### 6.1.2 mndDnode模块

**功能概述**：
- 数据节点（dnode）的创建、删除和配置管理
- 处理dnode状态监控和心跳检测
- 管理dnode间的通信和协调
**核心函数**：
- `mndInitDnode()`: 初始化dnode管理模块
- `mndProcessCreateDnodeReq()`: 处理创建dnode请求
- `mndProcessDropDnodeReq()`: 处理删除dnode请求
- `mndProcessStatusReq()`: 处理dnode状态请求
- `mndRetrieveDnodes()`: 查询dnode列表
- `mndAcquireDnode()`: 获取dnode对象
**关键特性**：
- 支持dnode在线/离线状态管理
- 提供dnode配置验证机制
- 支持加密密钥管理
- 集成审计日志功能

#### 6.1.3 mndQnode模块

**功能概述**：
- 查询节点（qnode）的创建、删除和管理
- 处理查询计算任务的分配和负载均衡
- 管理qnode状态和性能指标
**核心函数**：
- `mndInitQnode()`: 初始化qnode管理模块
- `mndProcessCreateQnodeReq()`: 处理创建qnode请求
- `mndProcessDropQnodeReq()`: 处理删除qnode请求
- `mndRetrieveQnodes()`: 查询qnode列表
**关键特性**：
- 支持查询负载均衡
- 提供qnode性能监控
- 支持动态添加/移除qnode

#### 6.1.4 mndSnode模块

**功能概述**：
- 流计算节点（snode）的创建、删除和管理
- 处理流式计算任务的分配和协调
- 管理snode状态和资源使用
**核心函数**：
- `mndInitSnode()`: 初始化snode管理模块
- `mndProcessCreateSnodeReq()`: 处理创建snode请求
- `mndProcessDropSnodeReq()`: 处理删除snode请求
- `mndRetrieveSnodes()`: 查询snode列表
**关键特性**：
- 支持流式计算任务管理
- 提供snode资源监控
- 支持流式数据处理的负载均衡

#### 6.1.5 mndVgroup模块

**功能概述**：
- 虚拟节点组（vgroup）的管理和迁移
- 处理vgroup的创建、删除和恢复
- 管理vgroup的副本和负载均衡
**核心函数**：
- `mndInitVgroup()`: 初始化vgroup管理模块
- `mndMoveVgroups()`: 迁移vgroup到其他dnode
- `mndRestoreVgroups()`: 恢复vgroup数据
- `mndAcquireVgroup()`: 获取vgroup对象
**关键特性**：
- 支持vgroup的跨节点迁移
- 提供数据恢复机制
- 支持副本管理和故障转移

### 6.2 关键数据结构

#### 6.2.1 DNode数据结构

```c
typedef struct {
  int32_t    id;                    // 节点ID
  int64_t    createdTime;           // 创建时间
  int64_t    updateTime;            // 更新时间
  int64_t    rebootTime;            // 重启时间
  int64_t    lastAccessTime;        // 最后访问时间
  int32_t    accessTimes;           // 访问次数
  int32_t    numOfVnodes;           // vnode数量
  int32_t    numOfOtherNodes;       // 其他节点数量
  int32_t    numOfSupportVnodes;    // 支持的vnode数量
  int32_t    numOfDiskCfg;          // 磁盘配置数量
  float      numOfCores;            // CPU核心数
  int64_t    memTotal;              // 总内存
  int64_t    memAvail;              // 可用内存
  int64_t    memUsed;               // 已用内存
  EDndReason offlineReason;         // 离线原因
  uint32_t   encryptionKeyChksum;   // 加密密钥校验和
  int8_t     encryptionKeyStat;     // 加密密钥状态
  uint16_t   port;                  // 端口号
  char       fqdn[TSDB_FQDN_LEN];   // 完全限定域名
  char       ep[TSDB_EP_LEN];       // 端点地址
  char       machineId[TSDB_MACHINE_ID_LEN + 1];  // 机器ID
} SDnodeObj;
```

**字段说明**：
- `offlineReason`: 离线原因枚举，包括版本不匹配、集群ID不匹配、网络超时等
- `encryptionKeyChksum`: 用于验证加密密钥的一致性
- `machineId`: 机器唯一标识，用于企业版授权验证

#### 6.2.2 MNode数据结构

```c
typedef struct {
  int32_t    id;                    // 节点ID
  int64_t    createdTime;           // 创建时间
  int64_t    updateTime;            // 更新时间
  ESyncState syncState;             // 同步状态
  SyncTerm   syncTerm;              // 同步任期
  bool       syncRestore;           // 同步恢复标志
  int64_t    roleTimeMs;            // 角色时间
  SDnodeObj* pDnode;                // 关联的dnode对象
  int32_t    role;                  // 节点角色（voter/learner）
  SyncIndex  lastIndex;             // 最后索引
} SMnodeObj;
```

**字段说明**：
- `syncState`: 同步状态，包括leader、follower、candidate等
- `role`: 节点角色，TAOS_SYNC_ROLE_VOTER（投票者）或TAOS_SYNC_ROLE_LEARNER（学习者）
- `lastIndex`: 最后应用的日志索引，用于数据一致性

#### 6.2.3 QNode数据结构

```c
typedef struct {
  int32_t    id;                    // 节点ID
  int64_t    createdTime;           // 创建时间
  int64_t    updateTime;            // 更新时间
  SDnodeObj* pDnode;                // 关联的dnode对象
  SQnodeLoad load;                  // 查询负载信息
} SQnodeObj;
```

#### 6.2.4 SNode数据结构

```c
typedef struct {
  int32_t    id;                    // 节点ID
  int64_t    createdTime;           // 创建时间
  int64_t    updateTime;            // 更新时间
  SDnodeObj* pDnode;                // 关联的dnode对象
} SSnodeObj;
```

#### 6.2.5 VGroup数据结构

```c
typedef struct {
  int32_t   vgId;                   // vgroup ID
  int64_t   createdTime;            // 创建时间
  int64_t   updateTime;             // 更新时间
  int32_t   version;                // 版本号
  uint32_t  hashBegin;              // 哈希范围开始
  uint32_t  hashEnd;                // 哈希范围结束
  char      dbName[TSDB_DB_FNAME_LEN];  // 数据库名称
  int64_t   dbUid;                  // 数据库UID
  int64_t   cacheUsage;             // 缓存使用量
  int64_t   numOfTables;            // 表数量
  int64_t   numOfTimeSeries;        // 时间序列数量
  int64_t   totalStorage;           // 总存储量
  int64_t   compStorage;            // 压缩存储量
  int64_t   pointsWritten;          // 写入点数
  int8_t    compact;                // 压缩标志
  int8_t    isTsma;                 // TSMA标志
  int8_t    replica;                // 副本数量
  SVnodeGid vnodeGid[TSDB_MAX_REPLICA + TSDB_MAX_LEARNER_REPLICA];  // vnode信息
  void*     pTsma;                  // TSMA指针
  int32_t   numOfCachedTables;      // 缓存表数量
  int32_t   syncConfChangeVer;      // 同步配置变更版本
  int32_t   mountVgId;              // 挂载vgroup ID
  int64_t   keepVersion;            // 保留版本
  int64_t   keepVersionTime;        // 保留版本时间
} SVgObj;
```

#### 6.2.6 SMnode主结构

```c
typedef struct SMnode {
  int32_t        selfDnodeId;       // 自身dnode ID
  int64_t        clusterId;         // 集群ID
  TdThread       thread;            // 线程句柄
  TdThread       arbThread;         // 仲裁线程
  TdThreadRwlock lock;              // 读写锁
  int32_t        rpcRef;            // RPC引用
  int32_t        syncRef;           // 同步引用
  bool           stopped;           // 停止标志
  bool           restored;          // 恢复标志
  bool           deploy;            // 部署标志
  char          *path;              // 路径
  int64_t        checkTime;         // 检查时间
  SyncIndex      applied;           // 已应用索引
  SSdb          *pSdb;              // 系统数据库
  SArray        *pSteps;            // 步骤数组
  SQHandle      *pQuery;            // 查询句柄
  SHashObj      *infosMeta;         // 信息元数据
  SHashObj      *perfsMeta;         // 性能元数据
  SWal          *pWal;              // 预写日志
  SShowMgmt      showMgmt;          // 显示管理
  SProfileMgmt   profileMgmt;       // 性能分析管理
  STelemMgmt     telemMgmt;         // 遥测管理
  SSyncMgmt      syncMgmt;          // 同步管理
  SEncryptMgmt   encryptMgmt;       // 加密管理
  SGrantInfo     grant;             // 授权信息
  MndMsgFp       msgFp[TDMT_MAX];   // 消息处理函数
  MndMsgFpExt    msgFpExt[TDMT_MAX];// 扩展消息处理函数
  SMsgCb         msgCb;             // 消息回调
  int64_t        ipWhiteVer;        // IP白名单版本
  int64_t        timeWhiteVer;      // 时间白名单版本
  int32_t        version;           // 版本号
} SMnode;
```

### 6.3 数据库设计

#### 6.3.1 数据模型

集群管理模块使用SDB（System Database）作为内部存储系统，管理以下核心对象：

| 对象类型 | SDB类型 | 键类型 | 描述 |
| --- | --- | --- | --- |
| Dnode | SDB_DNODE | SDB_KEY_INT32 | 数据节点信息 |
| Mnode | SDB_MNODE | SDB_KEY_INT32 | 管理节点信息 |
| Qnode | SDB_QNODE | SDB_KEY_INT32 | 查询节点信息 |
| Snode | SDB_SNODE | SDB_KEY_INT32 | 流计算节点信息 |
| Vgroup | SDB_VGROUP | SDB_KEY_INT32 | 虚拟节点组信息 |
| Db | SDB_DB | SDB_KEY_INT64 | 数据库信息 |

**存储特性**：
- 内存哈希表存储，提供快速访问
- WAL（Write-Ahead Logging）保证数据持久性
- 支持事务操作，保证数据一致性
- 提供对象状态管理（READY、CREATING、DROPPING、DROPPED）

#### 6.3.2 数据访问层

**SDB API接口**：
- `sdbAcquire()`: 获取对象（增加引用计数）
- `sdbRelease()`: 释放对象（减少引用计数）
- `sdbFetch()`: 遍历对象
- `sdbInsert()`: 插入对象
- `sdbUpdate()`: 更新对象
- `sdbDelete()`: 删除对象
**事务支持**：
- 支持多步骤事务操作
- 提供redo/undo日志机制
- 支持事务回滚和重试
**数据持久化**：
- 通过WAL保证操作持久性
- 支持检查点（checkpoint）机制
- 提供数据恢复功能

### 6.4 图表解释

#### 6.4.1 集群架构图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"graph TB\n    subgraph \"TDengine集群\"\n        M1[MNode 1\u003cbr/\u003eLeader]\n        M2[MNode 2\u003cbr/\u003eFollower]\n        M3[MNode 3\u003cbr/\u003eLearner]\n        \n        subgraph \"数据节点组\"\n            D1[DNode 1\u003cbr/\u003evnode: 3]\n            D2[DNode 2\u003cbr/\u003evnode: 2]\n            D3[DNode 3\u003cbr/\u003evnode: 3]\n        end\n        \n        subgraph \"查询节点组\"\n            Q1[QNode 1]\n            Q2[QNode 2]\n        end\n        \n        subgraph \"流计算节点组\"\n            S1[SNode 1]\n        end\n        \n        M1 --\u003e D1\n        M1 --\u003e D2\n        M1 --\u003e D3\n        M1 --\u003e Q1\n        M1 --\u003e Q2\n        M1 --\u003e S1\n        \n        M2 -.-\u003e M1\n        M3 -.-\u003e M1\n        \n        D1 --\u003e VG1[VGroup 1]\n        D1 --\u003e VG2[VGroup 2]\n        D2 --\u003e VG2\n        D2 --\u003e VG3[VGroup 3]\n        D3 --\u003e VG1\n        D3 --\u003e VG3\n    end\n    \n    Client[客户端应用] --\u003e M1\n    Client --\u003e Q1\n    Client --\u003e Q2\n","theme":"default","view":"chart"}"/>

#### 6.4.2 创建DNode流程图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"flowchart TD\n    Start[开始创建DNode] --\u003e Validate[验证参数]\n    Validate --\u003e CheckExist{检查DNode是否已存在}\n    CheckExist --\u003e|已存在| Error1[返回错误: DNode已存在]\n    CheckExist --\u003e|不存在| CreateTrans[创建事务]\n    CreateTrans --\u003e BuildObj[构建DNode对象]\n    BuildObj --\u003e Encode[编码为SDB Raw]\n    Encode --\u003e AppendLog[追加到事务日志]\n    AppendLog --\u003e SetStatus[设置状态为CREATING]\n    SetStatus --\u003e PrepareTrans[准备事务]\n    PrepareTrans --\u003e Commit[提交事务]\n    Commit --\u003e Success[返回成功]\n    \n    Error1 --\u003e End[结束]\n    Success --\u003e End\n","theme":"default","view":"chart"}"/>

#### 6.4.3 删除DNode流程图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"flowchart TD\n    Start[开始删除DNode] --\u003e CheckPrivilege[检查权限]\n    CheckPrivilege --\u003e|无权限| Error1[返回权限错误]\n    CheckPrivilege --\u003e|有权限| AcquireDNode[获取DNode对象]\n    \n    AcquireDNode --\u003e|不存在| Error2[返回DNode不存在错误]\n    AcquireDNode --\u003e|存在| CheckMNode[检查是否有MNode]\n    \n    CheckMNode --\u003e|有MNode| CheckMNodeCount[检查MNode数量]\n    CheckMNodeCount --\u003e|≤1| Error3[返回MNode数量不足错误]\n    CheckMNodeCount --\u003e|\u003e1| CheckSelf[检查是否自身节点]\n    \n    CheckSelf --\u003e|是自身节点| Error4[不能删除Leader节点]\n    CheckSelf --\u003e|不是自身节点| CheckVNodes[检查VNode数量]\n    \n    CheckMNode --\u003e|无MNode| CheckVNodes\n    \n    CheckVNodes --\u003e|有VNode| CheckOnline[检查DNode在线状态]\n    CheckVNodes --\u003e|无VNode| CheckOtherNodes[检查其他节点]\n    \n    CheckOnline --\u003e|在线| CheckForce[检查是否强制删除]\n    CheckOnline --\u003e|离线| CheckForce\n    \n    CheckForce --\u003e|强制删除| CheckVNodeState[检查VNode状态]\n    CheckForce --\u003e|非强制删除| Error5[返回DNode在线错误]\n    \n    CheckVNodeState --\u003e|VNode离线| CreateTrans[创建删除事务]\n    CheckVNodeState --\u003e|VNode在线| Error6[返回VNode在线错误]\n    \n    CheckOtherNodes --\u003e|有其他节点| CheckOnline\n    CheckOtherNodes --\u003e|无其他节点| CreateTrans\n    \n    CreateTrans --\u003e BuildRedoLogs[构建Redo日志]\n    BuildRedoLogs --\u003e BuildRedoActions[构建Redo Actions]\n    BuildRedoActions --\u003e BuildCommitLogs[构建Commit日志]\n    BuildCommitLogs --\u003e PrepareTrans[准备事务]\n    PrepareTrans --\u003e Success[返回成功]\n    \n    Error1 --\u003e End[结束]\n    Error2 --\u003e End\n    Error3 --\u003e End\n    Error4 --\u003e End\n    Error5 --\u003e End\n    Error6 --\u003e End\n    Success --\u003e End\n","theme":"default","view":"chart"}"/>

#### 6.4.4 DNode状态监控流程图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"flowchart TD\n    Start[接收状态请求] --\u003e Parse[解析请求]\n    Parse --\u003e CheckCluster[检查集群ID]\n    CheckCluster --\u003e|不匹配| Error1[返回集群ID不匹配错误]\n    CheckCluster --\u003e|匹配| AcquireDNode[获取DNode对象]\n    AcquireDNode --\u003e|不存在| CreateNew[创建新DNode]\n    AcquireDNode --\u003e|存在| CheckOnline[检查在线状态]\n    \n    CreateNew --\u003e CheckOnline\n    \n    CheckOnline --\u003e|离线| UpdateStatus[更新状态为离线]\n    CheckOnline --\u003e|在线| ValidateConfig[验证配置]\n    \n    UpdateStatus --\u003e BuildResponse[构建响应]\n    ValidateConfig --\u003e|配置有效| UpdateInfo[更新节点信息]\n    ValidateConfig --\u003e|配置无效| Error2[返回配置错误]\n    \n    UpdateInfo --\u003e BuildResponse\n    BuildResponse --\u003e SendResponse[发送响应]\n    \n    Error1 --\u003e End[结束]\n    Error2 --\u003e End\n    SendResponse --\u003e End\n","theme":"default","view":"chart"}"/>

#### 6.4.5 创建MNode流程图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"flowchart TD\n    Start[开始创建MNode] --\u003e CheckPrivilege[检查权限]\n    CheckPrivilege --\u003e|无权限| Error1[返回权限错误]\n    CheckPrivilege --\u003e|有权限| CheckExist[检查MNode是否已存在]\n    \n    CheckExist --\u003e|已存在| Error2[返回MNode已存在错误]\n    CheckExist --\u003e|不存在| CheckDNode[检查DNode是否存在]\n    \n    CheckDNode --\u003e|不存在| Error3[返回DNode不存在错误]\n    CheckDNode --\u003e|存在| CheckMNodeCount[检查MNode数量]\n    \n    CheckMNodeCount --\u003e|≥3| Error4[返回MNode数量超限错误]\n    CheckMNodeCount --\u003e|\u003c3| CheckOnline[检查DNode在线状态]\n    \n    CheckOnline --\u003e|离线| Error5[返回DNode离线错误]\n    CheckOnline --\u003e|在线| CreateTrans[创建事务]\n    \n    CreateTrans --\u003e BuildRedoActions[构建Redo Actions]\n    BuildRedoActions --\u003e BuildRedoLogs[构建Redo Logs]\n    BuildRedoLogs --\u003e BuildCommitLogs[构建Commit Logs]\n    BuildCommitLogs --\u003e PrepareTrans[准备事务]\n    PrepareTrans --\u003e Success[返回成功]\n    \n    Error1 --\u003e End[结束]\n    Error2 --\u003e End\n    Error3 --\u003e End\n    Error4 --\u003e End\n    Error5 --\u003e End\n    Success --\u003e End\n","theme":"default","view":"chart"}"/>

#### 6.4.6 删除MNode流程图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"flowchart TD\n    Start[开始删除MNode] --\u003e CheckPrivilege[检查权限]\n    CheckPrivilege --\u003e|无权限| Error1[返回权限错误]\n    CheckPrivilege --\u003e|有权限| AcquireMNode[获取MNode对象]\n    \n    AcquireMNode --\u003e|不存在| Error2[返回MNode不存在错误]\n    AcquireMNode --\u003e|存在| CheckMNodeCount[检查MNode数量]\n    \n    CheckMNodeCount --\u003e|≤1| Error3[返回MNode数量不足错误]\n    CheckMNodeCount --\u003e|\u003e1| CheckRole[检查MNode角色]\n    \n    CheckRole --\u003e|Leader| CheckForce[检查是否强制删除]\n    CheckRole --\u003e|Follower| CheckDNode[检查DNode在线状态]\n    CheckRole --\u003e|Learner| CheckDNode\n    \n    CheckForce --\u003e|强制删除| CheckDNode\n    CheckForce --\u003e|非强制删除| Error4[不能删除Leader节点]\n    \n    CheckDNode --\u003e|离线| Error5[返回DNode离线错误]\n    CheckDNode --\u003e|在线| CheckSyncState[检查同步状态]\n    \n    CheckSyncState --\u003e|同步中| Error6[返回同步状态错误]\n    CheckSyncState --\u003e|已同步| CreateTrans[创建删除事务]\n    \n    CreateTrans --\u003e BuildRedoLogs[构建Redo日志]\n    BuildRedoLogs --\u003e BuildRedoActions[构建Redo Actions]\n    BuildRedoActions --\u003e BuildCommitLogs[构建Commit日志]\n    BuildCommitLogs --\u003e PrepareTrans[准备事务]\n    PrepareTrans --\u003e Success[返回成功]\n    \n    Error1 --\u003e End[结束]\n    Error2 --\u003e End\n    Error3 --\u003e End\n    Error4 --\u003e End\n    Error5 --\u003e End\n    Error6 --\u003e End\n    Success --\u003e End\n","theme":"default","view":"chart"}"/>

**流程说明**：
1. **权限检查**：验证用户是否有删除MNode的权限
2. **MNode存在性检查**：确认要删除的MNode存在
3. **MNode数量检查**：确保集群中至少还有1个MNode（不能删除最后一个MNode）
4. **角色检查**：
  - Leader节点：需要检查是否强制删除，非强制删除不能删除Leader
  - Follower/Learner节点：可以直接删除
1. **DNode在线检查**：确保MNode所在的DNode在线
2. **同步状态检查**：检查MNode的同步状态，确保数据已同步
3. **事务处理**：创建删除事务，包含Redo日志、Redo Actions和Commit日志
4. **事务提交**：准备并提交事务，完成删除操作
**关键注意事项**：
- **Leader节点保护**：默认情况下不能删除Leader节点，除非使用强制删除选项
- **最小MNode数量**：集群中必须至少保留1个MNode，不能全部删除
- **数据一致性**：删除前需要确保MNode数据已同步，避免数据丢失
- **事务完整性**：使用事务机制保证删除操作的原子性和一致性

#### 6.4.7 创建SNode流程图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"flowchart TD\n    Start[开始创建SNode] --\u003e CheckPrivilege[检查权限]\n    CheckPrivilege --\u003e|无权限| Error1[返回权限错误]\n    CheckPrivilege --\u003e|有权限| CheckDNode[检查DNode是否存在]\n    \n    CheckDNode --\u003e|不存在| Error2[返回DNode不存在错误]\n    CheckDNode --\u003e|存在| CheckSNodeExist[检查SNode是否已存在]\n    \n    CheckSNodeExist --\u003e|已存在| Error3[返回SNode已存在错误]\n    CheckSNodeExist --\u003e|不存在| CheckOnline[检查DNode在线状态]\n    \n    CheckOnline --\u003e|离线| Error4[返回DNode离线错误]\n    CheckOnline --\u003e|在线| CreateTrans[创建事务]\n    \n    CreateTrans --\u003e BuildSNodeObj[构建SNode对象]\n    BuildSNodeObj --\u003e Encode[编码为SDB Raw]\n    Encode --\u003e AppendLog[追加到事务日志]\n    AppendLog --\u003e SetStatus[设置状态为CREATING]\n    SetStatus --\u003e BuildRedoAction[构建Redo Action]\n    BuildRedoAction --\u003e BuildCommitLog[构建Commit日志]\n    BuildCommitLog --\u003e PrepareTrans[准备事务]\n    PrepareTrans --\u003e Success[返回成功]\n    \n    Error1 --\u003e End[结束]\n    Error2 --\u003e End\n    Error3 --\u003e End\n    Error4 --\u003e End\n    Success --\u003e End\n","theme":"default","view":"chart"}"/>

**流程说明**：
1. **权限检查**：验证用户是否有创建SNode的权限
2. **DNode存在性检查**：确认目标DNode存在
3. **SNode存在性检查**：检查该DNode上是否已存在SNode
4. **在线状态检查**：确保目标DNode在线
5. **对象构建**：创建SNode对象，包含ID、创建时间等基本信息
6. **编码存储**：将SNode对象编码为SDB Raw格式
7. **事务处理**：创建事务，包含Redo日志、Redo Action和Commit日志
8. **状态设置**：设置SNode状态为CREATING
9. **事务提交**：准备并提交事务，完成创建操作

#### 6.4.8 删除SNode流程图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"flowchart TD\n    Start[开始删除SNode] --\u003e CheckPrivilege[检查权限]\n    CheckPrivilege --\u003e|无权限| Error1[返回权限错误]\n    CheckPrivilege --\u003e|有权限| AcquireSNode[获取SNode对象]\n    \n    AcquireSNode --\u003e|不存在| Error2[返回SNode不存在错误]\n    AcquireSNode --\u003e|存在| CheckDNode[检查DNode在线状态]\n    \n    CheckDNode --\u003e|离线| Error3[返回DNode离线错误]\n    CheckDNode --\u003e|在线| CheckStreamTasks[检查流计算任务]\n    \n    CheckStreamTasks --\u003e|有任务| CheckForce[检查是否强制删除]\n    CheckStreamTasks --\u003e|无任务| CreateTrans[创建删除事务]\n    \n    CheckForce --\u003e|强制删除| CreateTrans\n    CheckForce --\u003e|非强制删除| Error4[返回有流任务错误]\n    \n    CreateTrans --\u003e BuildRedoLogs[构建Redo日志]\n    BuildRedoLogs --\u003e BuildRedoActions[构建Redo Actions]\n    BuildRedoActions --\u003e BuildCommitLogs[构建Commit日志]\n    BuildCommitLogs --\u003e PrepareTrans[准备事务]\n    PrepareTrans --\u003e Success[返回成功]\n    \n    Error1 --\u003e End[结束]\n    Error2 --\u003e End\n    Error3 --\u003e End\n    Error4 --\u003e End\n    Success --\u003e End\n","theme":"default","view":"chart"}"/>

**流程说明**：
1. **权限检查**：验证用户是否有删除SNode的权限
2. **SNode存在性检查**：确认要删除的SNode存在
3. **DNode在线检查**：确保SNode所在的DNode在线
4. **流计算任务检查**：检查SNode上是否有正在运行的流计算任务
5. **强制删除检查**：如果有流计算任务，需要检查是否强制删除
6. **事务处理**：创建删除事务，包含Redo日志、Redo Actions和Commit日志
7. **事务提交**：准备并提交事务，完成删除操作

#### 6.4.9 创建QNode流程图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"flowchart TD\n    Start[开始创建QNode] --\u003e CheckPrivilege[检查权限]\n    CheckPrivilege --\u003e|无权限| Error1[返回权限错误]\n    CheckPrivilege --\u003e|有权限| CheckDNode[检查DNode是否存在]\n    \n    CheckDNode --\u003e|不存在| Error2[返回DNode不存在错误]\n    CheckDNode --\u003e|存在| CheckQNodeExist[检查QNode是否已存在]\n    \n    CheckQNodeExist --\u003e|已存在| Error3[返回QNode已存在错误]\n    CheckQNodeExist --\u003e|不存在| CheckOnline[检查DNode在线状态]\n    \n    CheckOnline --\u003e|离线| Error4[返回DNode离线错误]\n    CheckOnline --\u003e|在线| CheckLoadBalance[检查负载均衡配置]\n    \n    CheckLoadBalance --\u003e|配置无效| Error5[返回负载配置错误]\n    CheckLoadBalance --\u003e|配置有效| CreateTrans[创建事务]\n    \n    CreateTrans --\u003e BuildQNodeObj[构建QNode对象]\n    BuildQNodeObj --\u003e Encode[编码为SDB Raw]\n    Encode --\u003e AppendLog[追加到事务日志]\n    AppendLog --\u003e SetStatus[设置状态为CREATING]\n    SetStatus --\u003e BuildRedoAction[构建Redo Action]\n    BuildRedoAction --\u003e BuildCommitLog[构建Commit日志]\n    BuildCommitLog --\u003e PrepareTrans[准备事务]\n    PrepareTrans --\u003e Success[返回成功]\n    \n    Error1 --\u003e End[结束]\n    Error2 --\u003e End\n    Error3 --\u003e End\n    Error4 --\u003e End\n    Error5 --\u003e End\n    Success --\u003e End\n","theme":"default","view":"chart"}"/>

**流程说明**：
1. **权限检查**：验证用户是否有创建QNode的权限
2. **DNode存在性检查**：确认目标DNode存在
3. **QNode存在性检查**：检查该DNode上是否已存在QNode
4. **在线状态检查**：确保目标DNode在线
5. **负载均衡检查**：检查负载均衡配置是否有效
6. **对象构建**：创建QNode对象，包含ID、创建时间、负载信息等
7. **编码存储**：将QNode对象编码为SDB Raw格式
8. **事务处理**：创建事务，包含Redo日志、Redo Action和Commit日志
9. **状态设置**：设置QNode状态为CREATING
10. **事务提交**：准备并提交事务，完成创建操作
**关键特性**：
- **负载均衡支持**：QNode创建时会检查负载均衡配置，确保查询任务能合理分配
- **性能监控集成**：QNode对象包含负载信息字段，用于后续性能监控
- **动态扩展**：支持动态添加QNode以提高查询处理能力

#### 6.4.10 删除QNode流程图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"flowchart TD\n    Start[开始删除QNode] --\u003e CheckPrivilege[检查权限]\n    CheckPrivilege --\u003e|无权限| Error1[返回权限错误]\n    CheckPrivilege --\u003e|有权限| AcquireQNode[获取QNode对象]\n    \n    AcquireQNode --\u003e|不存在| Error2[返回QNode不存在错误]\n    AcquireQNode --\u003e|存在| CheckDNode[检查DNode在线状态]\n    \n    CheckDNode --\u003e|离线| Error3[返回DNode离线错误]\n    CheckDNode --\u003e|在线| CheckQueryTasks[检查查询任务]\n    \n    CheckQueryTasks --\u003e|有任务| CheckForce[检查是否强制删除]\n    CheckQueryTasks --\u003e|无任务| CreateTrans[创建删除事务]\n    \n    CheckForce --\u003e|强制删除| CreateTrans\n    CheckForce --\u003e|非强制删除| Error4[返回有查询任务错误]\n    \n    CreateTrans --\u003e BuildRedoLogs[构建Redo日志]\n    BuildRedoLogs --\u003e BuildRedoActions[构建Redo Actions]\n    BuildRedoActions --\u003e BuildCommitLogs[构建Commit日志]\n    BuildCommitLogs --\u003e PrepareTrans[准备事务]\n    PrepareTrans --\u003e Success[返回成功]\n    \n    Error1 --\u003e End[结束]\n    Error2 --\u003e End\n    Error3 --\u003e End\n    Error4 --\u003e End\n    Success --\u003e End\n","theme":"default","view":"chart"}"/>

**流程说明**：
1. **权限检查**：验证用户是否有删除QNode的权限
2. **QNode存在性检查**：确认要删除的QNode存在
3. **DNode在线检查**：确保QNode所在的DNode在线
4. **查询任务检查**：检查QNode上是否有正在运行的查询计算任务
5. **强制删除检查**：如果有查询任务，需要检查是否强制删除
6. **事务处理**：创建删除事务，包含Redo日志、Redo Actions和Commit日志
7. **事务提交**：准备并提交事务，完成删除操作
  
**关键注意事项**：
- **查询任务保护**：默认情况下不能删除有查询任务的QNode，除非使用强制删除选项
- **负载重分配**：删除QNode后，系统会自动将查询任务重新分配到其他QNode
- **性能影响**：删除QNode可能会暂时影响查询性能，直到负载重新平衡
- **事务完整性**：使用事务机制保证删除操作的原子性和一致性

#### 6.4.11 QNode操作消息序列图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"\nsequenceDiagram\n    participant C as Client\n    participant M as MNode\n    participant S as SDB\n    participant W as WAL\n    participant D as DNode\n    \n    Note over C,D: 创建QNode操作序列\n    C-\u003e\u003eM: CREATE QNODE请求\n    M-\u003e\u003eM: 验证权限和参数\n    M-\u003e\u003eS: 检查DNode和QNode状态\n    S--\u003e\u003eM: 返回检查结果\n    M-\u003e\u003eM: 创建QNode对象\n    M-\u003e\u003eW: 写入Redo日志\n    M-\u003e\u003eS: 插入QNode对象（CREATING状态）\n    M-\u003e\u003eD: 发送创建QNode请求\n    D--\u003e\u003eM: 创建响应\n    M-\u003e\u003eW: 写入Commit日志\n    M-\u003e\u003eS: 更新QNode状态为READY\n    M--\u003e\u003eC: 返回创建成功\n    \n    Note over C,D: 删除QNode操作序列\n    C-\u003e\u003eM: DROP QNODE请求\n    M-\u003e\u003eM: 验证权限和参数\n    M-\u003e\u003eS: 检查QNode是否存在\n    S--\u003e\u003eM: 返回QNode对象\n    M-\u003e\u003eM: 检查查询任务状态\n    M-\u003e\u003eM: 创建删除事务\n    M-\u003e\u003eW: 写入Redo日志\n    M-\u003e\u003eS: 更新QNode状态为DROPPING\n    M-\u003e\u003eD: 发送删除QNode请求\n    D--\u003e\u003eM: 删除响应\n    M-\u003e\u003eW: 写入Commit日志\n    M-\u003e\u003eS: 更新QNode状态为DROPPED\n    M--\u003e\u003eC: 返回删除成功\n    \n    Note over C,D: QNode负载监控序列\n    D-\u003e\u003eM: QNode负载状态报告\n    M-\u003e\u003eM: 更新QNode负载信息\n    M-\u003e\u003eS: 更新QNode对象负载字段\n    M-\u003e\u003eM: 执行负载均衡决策\n    M-\u003e\u003eD: 发送查询任务重分配指令\n    D--\u003e\u003eM: 重分配确认响应\n","theme":"default","view":"chart"}"/>

**序列说明**：
1. **创建QNode序列**：
  - 客户端发送创建请求
  - MNode验证权限和参数
  - 检查DNode和QNode状态
  - 创建QNode对象并写入日志
  - 通知DNode创建QNode
  - 更新状态并返回结果
    
1. **删除QNode序列**：
  - 客户端发送删除请求
  - MNode验证权限和参数
  - 检查QNode存在性和查询任务状态
  - 创建删除事务并写入日志
  - 通知DNode删除QNode
  - 更新状态并返回结果
    
1. **QNode负载监控序列**：
  - DNode定期报告QNode负载状态
  - MNode更新QNode负载信息
  - 执行负载均衡决策
  - 发送查询任务重分配指令
  - DNode确认重分配完成

#### 6.4.12 节点状态转换图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"stateDiagram-v2\n    [*] --\u003e Creating: 创建节点\n    Creating --\u003e Ready: 创建成功\n    Creating --\u003e Dropping: 创建失败/删除\n    \n    Ready --\u003e Online: 节点上线\n    Online --\u003e Offline: 心跳超时/配置不匹配\n    Offline --\u003e Online: 恢复连接/配置同步\n    \n    Ready --\u003e Dropping: 删除节点\n    Online --\u003e Dropping: 删除节点\n    Offline --\u003e Dropping: 强制删除\n    \n    Dropping --\u003e Dropped: 删除完成\n    Dropped --\u003e [*]: 清理资源\n    \n    note right of Creating\n        节点正在创建中\n        状态: SDB_STATUS_CREATING\n    end note\n    \n    note right of Ready\n        节点已创建但未上线\n        状态: SDB_STATUS_READY\n    end note\n    \n    note right of Online\n        节点在线且正常工作\n        状态: DND_REASON_ONLINE\n    end note\n    \n    note right of Offline\n        节点离线\n        原因: 版本不匹配/网络超时等\n    end note\n    \n    note right of Dropping\n        节点正在删除中\n        状态: SDB_STATUS_DROPPING\n    end note\n    \n    note right of Dropped\n        节点已删除\n        状态: SDB_STATUS_DROPPED\n    end note\n","theme":"default","view":"chart"}"/>

#### 6.4.13 消息序列图 - 创建DNode

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"sequenceDiagram\n    participant C as Client\n    participant M as MNode\n    participant S as SDB\n    participant W as WAL\n    \n    C-\u003e\u003eM: CREATE DNODE请求\n    M-\u003e\u003eM: 验证权限和参数\n    M-\u003e\u003eS: 检查DNode是否已存在\n    S--\u003e\u003eM: 返回检查结果\n    \n    alt DNode已存在\n        M--\u003e\u003eC: 返回错误\n    else DNode不存在\n        M-\u003e\u003eM: 创建事务对象\n        M-\u003e\u003eM: 构建DNode对象\n        M-\u003e\u003eW: 写入Redo日志\n        M-\u003e\u003eS: 插入DNode对象（CREATING状态）\n        M-\u003e\u003eW: 写入Commit日志\n        M-\u003e\u003eS: 更新DNode状态为READY\n        M--\u003e\u003eC: 返回成功\n    end\n","theme":"default","view":"chart"}"/>

#### 6.4.14 VGroup迁移流程图

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"flowchart TD\n    Start[开始VGroup迁移] --\u003e CheckSource[检查源DNode]\n    CheckSource --\u003e|不存在| Error1[返回源DNode不存在错误]\n    CheckSource --\u003e|存在| CheckTarget[检查目标DNode]\n    \n    CheckTarget --\u003e|不存在| Error2[返回目标DNode不存在错误]\n    CheckTarget --\u003e|存在| CheckVGroup[检查VGroup状态]\n    \n    CheckVGroup --\u003e|不存在| Error3[返回VGroup不存在错误]\n    CheckVGroup --\u003e|存在| CheckReplica[检查副本状态]\n    \n    CheckReplica --\u003e|副本异常| Error4[返回副本状态错误]\n    CheckReplica --\u003e|副本正常| CreateTrans[创建迁移事务]\n    \n    CreateTrans --\u003e LockVGroup[锁定VGroup]\n    LockVGroup --\u003e UpdateConfig[更新VGroup配置]\n    UpdateConfig --\u003e NotifyNodes[通知相关节点]\n    NotifyNodes --\u003e WaitSync[等待数据同步]\n    WaitSync --\u003e VerifyData[验证数据一致性]\n    VerifyData --\u003e|验证失败| Rollback[回滚迁移]\n    VerifyData --\u003e|验证成功| Commit[提交迁移]\n    \n    Commit --\u003e ReleaseLock[释放VGroup锁]\n    ReleaseLock --\u003e Success[返回迁移成功]\n    \n    Rollback --\u003e ReleaseLock\n    ReleaseLock --\u003e Error5[返回迁移失败]\n    \n    Error1 --\u003e End[结束]\n    Error2 --\u003e End\n    Error3 --\u003e End\n    Error4 --\u003e End\n    Error5 --\u003e End\n    Success --\u003e End\n","theme":"default","view":"chart"}"/>

## 7. 接口规范

### 7.1 API 文档

集群管理模块提供以下内部API：
- `mndCreateDnode()`: 创建dnode
- `mndDropDnode()`: 删除dnode
- `mndRetrieveDnodes()`: 查询dnode列表
- `mndCreateMnode()`: 创建mnode
- `mndDropMnode()`: 删除mnode
- `mndRetrieveMnodes()`: 查询mnode列表
- `mndCreateQnode()`: 创建qnode
- `mndDropQnode()`: 删除qnode
- `mndRetrieveQnodes()`: 查询qnode列表
- `mndConfigDnode()`: 配置dnode参数

### 7.2 用户界面

通过 TDengine CLI 和 REST API 提供集群管理功能：
- `SHOW DNODES`: 显示所有dnode
- `SHOW MNODES`: 显示所有mnode
- `SHOW QNODES`: 显示所有qnode
- `CREATE DNODE`: 创建dnode
- `DROP DNODE`: 删除dnode

## 8. 安全考虑

### 8.1 安全要求

- 节点间通信使用加密传输
- 配置信息加密存储
- 访问控制：只有授权节点可以加入集群
- 审计日志记录所有管理操作

### 8.2 漏洞缓解

- 定期更新加密密钥
- 实现节点身份验证机制
- 监控异常访问模式
- 提供安全配置指南

## 9. 接口规范

无

## 10. 安全考虑

无

## 11. 性能和可扩展性

除删除dnode、恢复dnode这个2个操作以外的所有操作能够在 20 秒内返回。删除dnode和恢复dnode这2个操作，所需时间与所在节点所保存的数据量成正比。

## 12. 部署和配置

1. 配置管理：
```bash

## 13. firstEp 是每个 dnode 首次启动后连接的第 1 个 dnode

firstEp h1.taosdata.com:6030

## 14. 必须配置为本 dnode 的 FQDN，如果本机只有一个 hostname，可注释或删除如下这行代码

fqdn h1.taosdata.com

## 15. 配置本 dnode 的端口，默认是 6030

serverPort 6030
```

1. 版本控制：
需向后兼容

## 16. 监控和维护

1. 监控：
无
1. 日志记录和诊断：
执行异常和执行成功均要记录包含特定字符串（例如，mnd）的日志，且执行成功要记录审计日志，以便于问题排查及审计。
1. 维护：
无

## 17. 参考资料

1. [集群管理-Function Spec](https://taosdata.feishu.cn/wiki/ADkTw8ow8ivmDqkZsBFcGZamnle)
