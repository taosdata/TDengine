# Raft-Arbitrator 协议设计-方案三

## 1. 设计目标

1. 仅考虑 双副本场景
2. 整个集群最多存在一个 Arbitrator Dnode
3. 暂不考虑大部分副本变更。即仅允许 1 replica 转换为 2 replica + arbitrator
4. 暂不考虑多点故障。raft 双副本 + arbitrator 提供 **至多一个节点故障** 的高可用服务
5. 暂不考虑 为 mnode 提供 Arbitrator 服务。以 vnode 为服务对象，理论上 mnode 可作为扩展内容

## 2. 服务定义

[基于 RAFT 协议和 Arbitrator 的双副本解决方案](https://taosdata.feishu.cn/wiki/FPFswxBdsi4zw1kzzECcvuDsnQb) 
<callout emoji="pushpin" background-color="light-orange" border-color="light-orange">
1. **Arbitrator 是 Dnode 上的一种服务**
2. **所在 Donde 不承载 Vnode，不参与具体业务流程，亦不写入用户数据**
3. **仅提供 Raft 双副本故障时的 Assigned_Leader 仲裁服务**
4. **成为 Assigned_Leader 的 Member 可继续处理业务（可降级写）**
</callout>

## 3. 基本原理

@Benguang ：从**服务状态角度**检查是否可成为 Assigned_Leader
1. **若 Leader（非 Assinged_Leader）达成一致，则 两个 Member 达成同步**
2. **仅 以下两种操作可能引起 Member 不再同步**
   - **Member 自身 restart**
   - **或 另一 Member 被指定为 Assigned_Leader**
---

## 4. token

Arbitrator/Member 使用 Token 标记服务状态。Token 在以下情况重新生成
1. 服务重启后
2. Member 由 Assigned_Leader 切换至 Leader 状态

### 4.1 Arbitrator 发送 heartBeatReq{.arbitratorToken, .seqNo}

定期发送
**arbitratorToken**: startTime+random。Arbitrator restart 后重新生成
**seqNo**: heartBeatReq 序号。**为 每个 Member 维护**，单调递增

### 4.2 Member 发送 heartBeatRsp{.arbitratorToken, .seqNo, .token}

Member 回复自身 token

### 4.3 Arbitrator 接收 heartBeatRsp

1. check1: heartBeatRsp.arbitratorToken != arbitratorToken, skip
2. check2: heartBeatRsp.seqNo <= lastRspSeqNo, skip
3. **设置 lastRspSeqNo = heartBeatRsp.seqNo**
4. 若 heartBeatRsp.token != Member[Id].token
   - 若 AssignedLeader.token == Member[Id].token
      - **清空 Assigned_Leader**
   - **设置 Member[Id].token = heartBeatRsp.token**
---

## 5. **MemberState{.token1, .token2, .isSync}**

**token1**：Member1 的状态
**token2**：Member2 的状态
**isSync**：token1 与 token2 是否同步，默认值 **false**
Arbitrator 使用 MemberState 描述 Member 状态, **仅 isSync == true 时可指定 Assigned_Leader**

### 5.1 **MemberState**.**isSync 状态设置为 true**

Arbitrator 向 Member 发送 写入 raft 的消息

### 5.2 **Arbitrator 发送 **writeRaftReq{.token1, .token2}

定期发送
1. 若 存在 Assigned_Leader，skip
2. 若 heartBeat 超时，skip
3. 若 isSync == true，skip
4. 向 Member 发送 writeRaftReq{token1, token2}

### 5.3 Member 回复 writeRaftRsp{.token1, .token2, .errcode}

1. 若自身非 Leader，回复 writeRaftRsp{token1, token2, ENotLeader}
   - 即使为 Assigned_Leader，回复 writeRaftRsp{token1, token2, ENotLeader}
2. 若 AssignedCommitIndex > RaftCommitIndex，回复 writeRaftRsp{token1, token2, ENotSync}
3. 回复 writeRaftRsp{token1, token2, ESuccess}

### 5.4 **Arbitrator 接收 writeRaftRsp**

1. 若 存在 Assigned_Leader，skip
2. 若 writeRaftRsp.errcode == ENotLeader，skip
3. 若 writeRaftRsp.errcode != ESuccess, **设置 MemberState{token1, token2, false}**
4. **设置 MemberState{token1, token2, ****true****}**
---

## 6. Member 节点故障

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"---\ntitle: Group state diagram\n---\nstateDiagram-v2\n    init --\u003e notSync,noAssigned: recv toekn\n    notSync,noAssigned --\u003e isSync,noAssigned: check sync (isSync)\n    notSync,noAssigned --\u003e notSync,noAssigned: check sync (notSync)\n    isSync,noAssigned --\u003e isSync,assigned: send setAssignedReq\n    isSync,assigned --\u003e isSync,noAssigned: recv setAssignedRsp(refuse)\n    isSync,assigned --\u003e notSync,assigned: recv setAssignedRsp(accept)\n    notSync,assigned --\u003e notSync,noAssigned: recv token\n","theme":"default","view":"chart"}"/>

## 7. set Assigned_Leader 流程

**AssignedLeader{.token}**

### 7.1 Arbitrator 发起 **setAssignedLeaderReq{.token}**

若 Arbitrator 连续无法检测到 某 Member（例如 Member 2）heartBeat > k 次：
1. check1:  MemberState{token1, token2, **false**}, skip
2. check2: AssignedLeader.token != Nil 且 AssignedLeader != {token1, Nil}，skip
3. **更新 MemberState{token1, token2, false}**
4. **记录 AssignedLeader{token1}**
5. 向 Member 1 发送 setAssignedLeaderReq{token1}

### 7.2 Member **回复 ****setAssignedLeaderRsp{.token, .isAccept}**

1. check1: token 若与本地不一致，回复 setAssignedLeaderRsp{myToken, false}
2. check2: 若自身 是 Assigned_Leader，回复 setAssignedLeaderRsp{myToken, true}
3. **term++，becomeAssignedLeader**
4. 回复 setAssignedLeaderRsp{myToken, true}

### 7.3 **Arbitrator 接收 setAssignedLeaderRsp**

Aribtrator 收到 setAssignedLeaderRsp
1. check1: setAssignedLeaderRsp.token != AssignedLeader.token，skip
2. 若 .isAccept = true，设置 MemberState{token1, token2, **false**}
3. **更新 AssignedLeader{Nil}**

## 8. unset Assigned_Leader 流程

### 8.1 Member **发起 unSetAssignedLeader**

**自身状态为 Assigned_Leader，达成同步（AssignedCommitIndex == RaftCommitIndex）**
1. **updateToken**
2. **term++**
3. **becomeLeader**

## 9. Arbitrator 节点故障

1. 若不存在 Assigned Leader，以 raft 双副本继续运行
2. 若存在 Assigned Leader，仍可恢复至 raft 双副本
---

## 10. 数据存储

arbitrator 需存储以下数据
```json
{
        "arbId":        "1",
        "groups":       [{
                        "groupId":      "2",
                        "members":      [{
                                        "dnodeId":      "1"
                                }, {
                                        "dnodeId":      "2"
                                }],
                        "assignedLeader":       {
                                "dnodeId":      "0",
                                "token":        ""
                        }
                }, {
                        "groupId":      "3",
                        "members":      [{
                                        "dnodeId":      "1"
                                }, {
                                        "dnodeId":      "2"
                                }],
                        "assignedLeader":       {
                                "dnodeId":      "0",
                                "token":        ""
                        }
                }]
}
```

---

## 11. 数据恢复

### 11.1 Member 数据恢复

保持原始 raft 恢复机制
1. Assigned_Leader 状态下对外 commit 无需达成一致**，但仍向 Follower 同步 log**

### 11.2 Arbitrator 数据恢复

无特殊处理
---

## 12. 恢复期间限速

达成同步之前可能需在 propose 过程 进行限速。
应为一个通用功能，与 Arbitrator 功能不直接相关，暂不考虑
