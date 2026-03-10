# 建流与部署时序：正常 vs 异常

## 1. 正常情况（流先落 SDB，再被部署）

```mermaid
sequenceDiagram
    participant Client
    participant MND as Mnode(trans)
    participant SDB as SDB
    participant MSTM as Mnode(StreamMgmt)
    participant Snode

    Client->>MND: create stream 请求
    MND->>MND: trans 创建 (prepare)
    MND->>MND: redoAction: vnode-create-stb (x6)
    Note over MND: 所有 redoAction 完成
    MND->>MND: stage → commit, propose(seq:40)
    MND->>MND: 其他节点 apply commit
    MND->>SDB: commitAction:0 (stb)
    MND->>SDB: commitAction:1 (stream 写入)
    Note over MND,SDB: 流已持久化到 SDB
    MND->>MSTM: (建流事务 commit 完成后) post DEPLOY 入队
    MND->>Client: create-stream-rsp

    Snode->>MSTM: stream 心跳
    MSTM->>MSTM: 处理 deploy action
    MSTM->>SDB: mndAcquireStream(streamName)
    SDB-->>MSTM: 返回 stream 对象 (存在)
    MSTM->>MSTM: msmDeployStreamTasks()
    MSTM->>Snode: 部署 / 调度
```

**要点**：DEPLOY 入队发生在 **commitAction 执行之后**（即 stream 已写入 SDB 之后），心跳处理 deploy 时 `mndAcquireStream` 能查到流。

---

## 2. 本次异常情况（DEPLOY 先入队，流尚未落 SDB）

```mermaid
sequenceDiagram
    participant Client
    participant MND as Mnode(trans)
    participant SDB as SDB
    participant MSTM as Mnode(StreamMgmt)
    participant Snode

    Client->>MND: create stream 请求
    MND->>MND: trans 创建 (prepare), propose(seq:39)
    MND->>MND: redoAction: vnode-create-stb (x6)
    Note over MND: 部分/全部 redoAction 完成
    MND->>MSTM: "half completed" 时 post DEPLOY 入队
    Note over MND,MSTM: 此时 stream 尚未写入 SDB
    MND->>MND: redoAction 全部完成 → stage commit
    MND->>MND: propose(seq:40)，等待 apply

    Snode->>MSTM: stream 心跳 (约 230ms 后)
    MSTM->>MSTM: 处理 deploy action
    MSTM->>SDB: mndAcquireStream(streamName)
    SDB-->>MSTM: NULL (stream 不存在)
    MSTM->>MSTM: "stream no longer exists, ignore deploy"
    MSTM->>Snode: 心跳响应 (未部署)

    Note over MND: 约 110ms 后
    MND->>MND: process sync proposal, apply index:40
    MND->>SDB: commitAction:0 (stb)
    MND->>SDB: commitAction:1 (stream 写入)
    Note over MND,SDB: 流此时才落库，但 deploy 已被忽略
    MND->>Client: create-stream-rsp
```

**要点**：DEPLOY 在 **redoAction 阶段 / half completed** 就入队，而 stream 要等到 **commit 被 apply 后** 才写入 SDB。心跳先于 commit 处理了 deploy，此时 SDB 里还没有该流，于是 “no longer exists, ignore deploy”，导致该流本次不会再被部署。
