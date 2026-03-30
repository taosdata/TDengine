# [deprecated]Raft-Arbitrator 协议设计

## 1. 设计目标

1. 仅考虑 双副本场景
2. 整个集群最多存在一个 Arbitrator Dnode
3. 暂不考虑副本变更。即 2 replica + arbitrator 无法与 1 replica 、3 replica 进行转换
4. raft 双副本 + arbitrator 提供 **至多一个节点故障** 的高可用服务

## 2. 服务定义

[基于 RAFT 协议和 Arbitrator 的双副本解决方案](https://taosdata.feishu.cn/wiki/FPFswxBdsi4zw1kzzECcvuDsnQb) 
<callout emoji="pushpin" background-color="light-orange" border-color="light-orange">
1. **Arbitrator 是一种特殊的 Dnode**
2. **不承载 Mnode/Vnode，不参与具体业务流程，亦不写入用户数据**
3. **仅提供 Raft 双副本故障时的 Assigned_Leader 仲裁服务**
4. **成为 Assigned_Leader 的 Member 可继续处理业务（可降级写）**
</callout>

---

---

## 3. 方案一：

@李顺纲：从日志完整角度检查是否可成为 ：从**日志完整角度**检查是否可成为 Assigned_Leader
由 Member 发起 Assigned_Leader 竞选

## 4. 基本原理

**将成为 Assigned_Leader 的 Member 日志不可落后，即持有 Leader/Assigned_Leader committed 的全部日志。**

## 5. 申请 **Assigned_Leader 仲裁的条件**

<callout emoji="exclamation" background-color="light-orange" border-color="light-orange">
**约束1：申请 Assigned_Leader 的 Member ****本地 lastLogTerm**** 的日志不可落后，即持有 ****本地 lastLogTerm 的**** Leader/Assigned_Leader committed 的全部日志。**
</callout>

### 5.1 **syncInLastLogTerm 状态**

**设置 **syncInLastLogTerm = true**：从设置时刻开始若无协议外数据修改，则满足 约束1**

### 5.2 **syncInLastLogTerm 状态设置**

- 无数据启动时，设置 syncInLastLogTerm = false
- Leader/Assigned_Leader，当前 term 日志不会落后。成为该身份时设置 **syncInLastLogTerm = true**
- Follower 接收 AppendEntries 消息时自行判断
  - **若 Follower matchIndex >= Leader commitIndex，且 当前 term 由 Leader 领导**，Follower 拥有全部 committed entries，可认为当前 term 日志不落后。**syncInLastLogTerm = true**
  - **若 Follower matchIndex < Leader commitIndex，或 当前 term 由 Assigned_Leader 领导**，认为 Follower 缺少部分 committed entries。**syncInLastLogTerm = false**

### 5.3 wal 可能在 poweroff 时截断

**服务关闭时将 syncInLastLogTerm 记录至 state 文件中。启动时检查并移除 state 文件**
- 若存在，读取 syncInLastLogTerm 信息
- 若不存在，认为 wal 不可靠，全部 syncInLastLogTerm = false
- [ ] 确认正常关闭可保留全部日志

## 6. Assigned_Leader 仲裁

<callout emoji="exclamation" background-color="light-orange" border-color="light-orange">
**约束2：选定 Assigned_Leader 的 Member ****term****  不可落后，即****申请者 lastLogTerm 大于等于 Arbitrator 所记录的 term**
</callout>

### 6.1 Arbitrator 指定/Member 申请？

主动发起意味着感知状态变化，Arbitrator 感知到的状态变化 可能与 两个 Member 不一致，破坏 “仅在 raft 无法选主时仲裁” 的基本原则。**故选择 Member 发起申请 的方式。**

### 6.2 仲裁过程

1. **arbitratorTerm > SYNC_TERM_INVALID**
   - 初始化条件，保证 Arbitrator 接收过来自 Leader 的消息，term 0 不允许 Assigned_Leader
2. **将被授予节点的 lastLogTerm >= arbitratorTerm**
   - 若 lastLogTerm < arbitratorTerm，则该节点日志落后，不可授予
3. ~~**不 连续授予不同节点 Assigned_Leader**~~**：被 过程2 包含**
  ~~Arbitrator 需判断当前申请是否满足以下中的任一：~~
   - ~~**isAssigned == 1 && leaderId == reqId**~~
  ~~即 Arbitrator 记录 Leader 为 Assigned，但当前申请者 Id 与 记录的 LeaderId 一致~~
   - ~~**isAssigned == 0 **~~~~即 Arbitrator 记录 Leader 非 Assigned~~
1. 若以上判断全部满足，Arbitrator 持久化 {term, leaderId, isAssigned=1}，并回复成功。否则回复失败

## 7. Assigned_Leader 状态变更

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"stateDiagram-v2\n    Follower--\u003eCandidate: timeoutElect\n\n    Candidate--\u003eLeader: requestVotePeer\n    Candidate--\u003eFollower: higherTerm\n    Candidate--\u003eCandidate: timeoutElect\n    Leader--\u003eFollower: higherTerm\n","theme":"default","view":"chart"}"/>

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"stateDiagram-v2\n    Follower--\u003eCandidate: timeoutElect\n    Leader--\u003eFollower: higherTerm\n    Candidate--\u003eFollower: higherTerm\n    Candidate--\u003eCandidate: timeoutElect\n\n    Leader--\u003eFollower: timeoutNoRsp\n    Assigned_Leader--\u003eFollower: matchIndexOK\n\n    Candidate--\u003eLeader: requestVotePeer\n\n    Candidate--\u003eAssigned_Leader: requestVoteArbitrator\n","theme":"default","view":"chart"}"/>

### 7.1 Leader 成为 Follower

触发条件：Leader 长时未收到 Follower 消息， 进入 timeoutNoRsp
1. Leader 将自身标记为 Follower

### 7.2 Follower 成为 Candidate

触发：Follower 长时未收到 Leader 消息，开始 timeoutElect
1. 将自身标记为 Candidate

### 7.3 Candidate 成为 Assigned_Leader

#### 7.3.1 Candidate 发送 requestVoteArbitratorReq{.vgId, .addr, .lastLogTerm, .term}

- 若 syncInLastLogTerm == false，skip
- 向 Arbitrator 发送 requestVoteArbitratorReq{vgId, myAddr, lastLogTerm, term}

#### 7.3.2 Arbitrator 回复 **requestVoteArbitratorRsp{.addr, .term, .isAccpet}**

1. 若 arbitratorTerm == SYNC_TERM_INVALID，回复 requestVoteArbitratorRsp{addr, SYNC_TERM_INVALID, false}
2. 若 requestVoteArbitratorReq.addr not in vgroup{vgId}，回复 requestVoteArbitratorRsp{addr, SYNC_TERM_INVALID, false}
3. 若 requestVoteArbitratorReq.lastLogTerm < arbitratorTerm，回复 requestVoteArbitratorRsp{addr, SYNC_TERM_INVALID, false}
4. 持久化 leaderAddr = addr, isAssignedLeader = 1, term = term
5. 回复 **requestVoteArbitratorRsp{addr, term, true}**

#### 7.3.3 Member 接收  **requestVoteArbitratorRsp**

1. 若 syncInLastLogTerm == false，skip
2. 若 .isAccept == false, skip
3. 若 .addr != myAddr, skip
4. 若 .term < myTerm, skip
5. **becomeAssignedLeader**

### 7.4 Assigned_Leader 成为 Follower

触发：Assigned_Leader 收到 Follower AppendEntriesReply。若 **Follower matchIndex >= Assigned_Leader commitIndex****，**说明 Follower 已经补全日志，此时 Assigned_Leader 可成为 Leader。
1. Assigned_Leader 将自身标记为 Follower

## 8. Arbitrator 节点故障

两个 Member 继续以 双副本 raft 协议运行

## 9. 数据存储

Raft 可保证 Term 与 Leader 的唯一匹配。Arbitrator **无需感知**最新的 commit-id，**仅需保存** ｛vgId， term，leaderAddr，isAssignedLeader｝关系。即可判定 申请者 是否可以成为 Assigned_Leader
数据保存至 Dnode 节点上 /var/lib/taos/dnode/arbitrator.json 文件中
```json
{
        "isArbitrator":      1,
        "vgroups":       [{
                        "vgId":              2,
                        "term":              5,
                        "leaderAddr":        8214722929011720194,
                        "isAssignedLeader":  0
                }, {
                        "vgId":              3,
                        "term":              8,
                        "leaderAddr":        8214722929011720312,
                        "isAssignedLeader":  1
                }]
}

```

## 10. 数据恢复

### 10.1 Member 数据恢复

同 一般 Raft Member 故障恢复，**恢复期间 非故障节点 保持 Assigned_Leader 状态**，以标识当前 Member 间数据不一致。恢复完毕后，进入 [Assigned_Leader 成为 Leader](https://taosdata.feishu.cn/docx/QbI1dGeYKobhTix3wbKc9H8lnbc#XxAGdvB3xoaJW7xMDlQchuqJnUd) 流程

### 10.2 Arbitrator 数据恢复

收到 Leader/Assigned_Leader 的 heartBeatReq，更新本地 保存的 Leader 关系

## 11. 恢复期间限速（TODO）

Follower matchIndex < Assigned_Leader commitIndex 过程需进行限速
---

---

## 12. 方案二：

@Benguang ：从**服务状态角度**检查是否可成为 Assigned_Leader
由 Arbitrator 发起 Assigned_Leader 指定

## 13. 基本原理

1. **若 Leader 达成 agreedOn，则 两个 Member 达成同步**
2. **仅 以下两种操作可能引起 Member 不再同步**
   - **Member 非正常 restart**
   - **另一 Member 被指定 Assigned_Leader**

## 14. token

使用 token (startTime+random) 标识 vnode 数据的可靠性
vnode stop 时将 token 保存至文件，start 时加载至内存，文件不存在（非正常 restart）则重新生成 token

### 14.1 Arbitrator 获取 token

- [ ] Arbitrator 接收 Member 心跳，获取 token 过程的 msg 往来

## 15. **MemberState{.token, .canBeAssigned}**

Arbitrator 使用 MemberState 描述 Member 状态
**token**：标识 Member 的状态。
**canBeAssigned**：标识 当前状态的 Member 能否成为 Assigned_Leader

### 15.1 **MemberState**.**canBeAssigned**

### 15.2 **Arbitrator 定时 发送 **writeRaftReq{.token1, .token2}

1. 若存在 Assigned_Leader, skip
2. 向 Member 发送 writeRaftReq{token1, token2}

### 15.3 Member 接收 **writeRaftReq**

1. 若自身非 Leader，errcode = ENotLeader
   - 即使 Assigned_Leader，errcode = ENotLeader
回复 writeRaftRsp{token1, token2, errcode}

### 15.4 **Arbitrator 接收 **writeRaftRsp{.token1, .token2, .errcode}

1. 若 存在 Assigned_Leader, skip
2. 若 writeRaftRsp.errcode == ENotLeader，skip
3. 若 writeRaftRsp.errcode != ESuccess, 设置 assignedCandidate{token1, false}，assignedCandidate{token2, false}
4. 设置 assignedCandidate{token1, true}，assignedCandidate{token2, true}

### 15.5 **MemberState.canBeAssigned**** ****正确性**

假设 Member1 **实际 MemberState{token1_M, false}**，而 Arbitrator 标识 Member 1 状态为 **MemberState{token1_A, true}** 
token1_M 产生时间必然**不早于** token1_A，记作 token1_M => token1_A
1. 若 token1_M > token1_A
  **由于 token1_A 如非正常重启则不会复用，**[**若接收 setAssignedLeaderReq 时进行 token 比对**](https://taosdata.feishu.cn/docx/QbI1dGeYKobhTix3wbKc9H8lnbc#Vme6dBK1io0ybXxUxHYcO4SrnOd)，则 Arbitrator 持有的标记**无害**。
1. 若 token1_M == token1_A
  自 Arbitrator 收到 token1_A 开始，Member 1 未 restart。仅 Member2 成为 Assigned_Leader 可能导致 MemberState{token1_M, false}。[而设置 assignedCandidate{token1, true}时，不存在 Assigned_Leader。](https://taosdata.feishu.cn/docx/QbI1dGeYKobhTix3wbKc9H8lnbc#SoHnddPeroLI6ixiauxcKKRbnxy)[**若保证 Assigned_Leader -> Leader 时，两个 Member 数据同步(可达成 argreedOn)。**](https://taosdata.feishu.cn/docx/QbI1dGeYKobhTix3wbKc9H8lnbc#XXU2dT4vgoSZOqxqoZ3cYl4enCf)则由于 当前不存在 Assigned_Leader，MemberState{token1_A, true} 正确，假设不成立。
<callout emoji="exclamation" background-color="light-orange" border-color="light-orange">
**Arbitrator MemberState.canBeAssigned**** ****正确性，由以下条件保证：**
1. **token 非正常重启则不会复用**
2. **接收 setAssignedLeaderReq 时进行 token 比对**
3. **Assigned_Leader -> Leader 时，两个 Member 数据已经同步(达成 argreedOn)**
</callout>

## 16. Assigned_Leader 状态变更

<add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"stateDiagram-v2\n    state OriginealRaft {\n        Follower--\u003eCandidate: timeoutElect\n        Leader--\u003eFollower: higherTerm\n        Candidate--\u003eFollower: higherTerm\n        Candidate--\u003eCandidate: timeoutElect\n\n        Candidate--\u003eLeader: requestVotePeer\n    }\n\n    Assigned_Leader--\u003eLeader: unSetAssignedLeader\n    OriginealRaft --\u003eAssigned_Leader: beSetAssignedLeader\n","theme":"default","view":"chart"}"/>

## 17. set Assigned_Leader 流程

### 17.1 Arbitrator 发起 **setAssignedLeaderReq{.token}**

**AssignedLeader{.token, .term}**
若 Arbitrator 连续无法检测到 某 Member （例如 Member 2）心跳不少于 k 次：
1. check1: **AssignedLeader.token != Nil **且 **AssignedLeader != {token1, Nil}**，则 skip
2. check2: **MemberState{token1, false}**, 则 skip
3. 更新 **MemberState{token2, false}**
4. 记录 **AssignedLeader{token1, Nil}**
5. 向 Member 1 发送 **setAssignedLeaderReq{token1}**

### 17.2 Member ** 接收 setAssignedLeaderReq**

1. check1: token 若与本地不一致，回复 **setAssignedLeaderRsp{myToken, false, term}**
2. check2: 若自身 是 Assigned_Leader，回复 **setAssignedLeaderRsp{myToken, true, term}**
3. term++，becomeAssignedLeader
4. 回复 **setAssignedLeaderRsp{myToken, true, term}**

### 17.3 **Arbitrator 接收 ****setAssignedLeaderRsp{.token, .isAccept, .term}**

Aribtrator 收到 **setAssignedLeaderRsp**
1. check1: **setAssignedLeaderRsp.token != AssignedLeader.token**，skip
2. check2: **AssignedLeader.term != Nil && setAssignedLeaderRsp.term< AssignedLeader.term**，skip
3. 更新 **AssignedLeader**
   - 若 .isAccept = true，更新 **AssignedLeader{token, term}**
   - 若 .isAccept = false，更新 **AssignedLeader{Nil, Nil}**

## 18. unset Assigned_Leader 流程 （子方案一，Member 发起）

- **Assigned_Leader -> Leader 时，已经同步**
- **Arbitrator 故障时，Member 无法从 Assigned_Leader 变更为 Leader**

### 18.1 Member ** 发起 ****unSetAssignedLeaderReq{.token, .term}**

达成 agreedOn 且 自身状态为 Assigned_Leader
1. 向 Arbitrator 发送 **unSetAssignedLeaderReq{token, term}**

### 18.2 **Arbitrator 接收 unSetAssignedLeaderReq**

Arbitrator 收到 **unSetAssignedLeaderReq**
1. check1: **unSetAssignedLeaderReq.token != AssignedLeader.token，回复 unSetAssignedLeaderRsp{false, term}**
2. check2: **unSetAssignedLeaderReq.term < AssignedLeader.term，回复 unSetAssignedLeaderRsp{false, AssignedLeader.term}**
3. 清空 **AssignedLeader**
4. 回复 **unSetAssignedLeaderRsp{true, unSetAssignedLeaderReq.term}**

### 18.3 Member **接收 ****unSetAssignedLeaderRsp{.isAccept, .term}**

1. 若自身状态不为 Assigned_Leader，skip
2. 若 **unSetAssignedLeaderRsp.isAccept**** ****== false**，skip
3. 若 **unSetAssignedLeaderRsp.term < term** ，skip
4. term++，becomeLeader

## 19. ~~unset Assigned_Leader 流程 （子方案二，Arbitrator 发起）~~ {folded="true"}

- **Assigned_Leader -> Leader 时，很难从外部保证 已经同步。**
**可能需要特殊的 写入消息，该消息要求 agreedOn，但整体比较别扭**
- **Arbitrator 故障时，Member 无法从 Assigned_Leader 变更为 Leader**

### 19.1 Arbitrator **发起 ****unSetAssignedLeaderReq{.token, .term}**

存在 Assigned_Leader，收到非 Assigned_Leader Member 的 heartBeat
1. 向 Assigned_Leader 发起 **unSetAssignedLeaderReq**

### 19.2 Member 接收 **unSetAssignedLeaderReq**

1. check1: 自身状态不为 **Assigned_Leader，回复 unSetAssignedLeaderRsp(token, term, false)**
2. check2:  **unSetAssignedLeaderReq.token != token**，**回复 unSetAssignedLeaderRsp(token, term, false)**
3. check3:  **unSetAssignedLeaderReq.term !=  term，回复 unSetAssignedLeaderRsp(token, term, false)**
4. term++，becomeLeader
5. **回复 unSetAssignedLeaderRsp(token, term, true)**

### 19.3 Arbitrator 接收 **unSetAssignedLeaderRsp{.token, .term, .isAccept}**

Arbitrator 收到 **unSetAssignedLeaderRsp**
1. check1: **unSetAssignedLeaderRsp.token != AssignedLeader.token**，skip
2. check2: **unSetAssignedLeaderReq.term != AssignedLeader.term**，skip
3. check3：**unSetAssignedLeaderReq.isAccept != true**，skip
4. 清空 **AssignedLeader**

## 20. Arbitrator 节点故障

1. 两个 Member 继续以 双副本 raft 协议运行
2. 保持 Assigned_Leader 状态运行

## 21. 数据存储

```json
{
    "vgroups": [{
                "vgId": 2,
                "members": [{
                           "memberId": 1,
                           "tokens": [{
                                       "token":    1701048119256
                                       "canBeAssigned": 1
                                       "isAssigned": 0
                           },{
                                       "token":    1701049119264
                                       "canBeAssigned": 1
                                       "isAssigned": 1
                           }]
                },{
                           "memberId": 2,
                           "tokens": [{
                                       "token":    1701048119211
                                       "canBeAssigned": 1
                                       "isAssigned": 0
                           },{
                                       "token":    1701049119673
                                       "canBeAssigned": 0
                                       "isAssigned": 0
                           }]
                }]
            }]
}

```

## 22. 数据恢复

### 22.1 Member 数据恢复

同 一般 Raft Member 故障恢复，**恢复期间 非故障节点 保持 Assigned_Leader 状态**，以标识当前 Member 间数据不一致。恢复完毕后，进入 [unset Assigned_Leader 流程](https://taosdata.feishu.cn/docx/QbI1dGeYKobhTix3wbKc9H8lnbc#BvO6dDLGBoUk7wxwYJOc8OmfnnR)

### 22.2 Arbitrator 数据恢复

1. 无数据启动后，不可设置 Arbitrator
2. 无数据启动后，收到两 Member 心跳后可设置 Arbitrator
3. 需要感知是否存在 Assigned_Leader

## 23. 恢复期间限速（TODO）

与 Follower 达成 agreedOn 之前需进行限速
