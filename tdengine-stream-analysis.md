# TDengine 流计算（new-stream）源码分析归档

> 本文档汇总本次会话从"流计算调用链"开始的全部分析结果，按提问时序组织，可独立查阅。
> 涉及代码版本以分析时刻为准；行号可能随后续提交偏移，使用前请回查源码。

## 目录

- [一、流计算整体调用链](#一流计算整体调用链)
- [二、mnode 心跳同步代码逻辑](#二mnode-心跳同步代码逻辑)
- [三、`msmHandleStreamRequests`](#三msmhandlestreamrequests)
- [四、`tCloneSStreamMgmtReq` 自赋值之谜](#四tclonesstreammgmtreq-自赋值之谜)
- [五、`stmHandleStreamRemovedTasks` 的必要性](#五stmhandlestreamremovedtasks-的必要性)
- [六、心跳消息中 `pTriggerStatus` 的作用](#六心跳消息中-ptriggerstatus-的作用)
- [七、`msmHandleStreamHbMsg` 并发安全](#七msmhandlestreamhbmsg-并发安全)
- [八、`msmWatchHandleHbMsg`](#八msmwatchhandlehbmsg)
- [九、`SStreamObj.userDropped` 与 `userStopped`](#九sstreamobjuserdropped-与-userstopped)
- [十、`msmHealthCheck`](#十msmhealthcheck)
- [十一、`msmCheckVgroupStatus`](#十一msmcheckvgroupstatus)
- [十二、`msmStopStreamByError`](#十二msmstopstreambyerror)
- [十三、`msmTDRemoveStream`](#十三msmtdremovestream)
- [十四、`msmSTRemoveStream`](#十四msmstremovestream)
- [附录 A：关键文件索引](#附录-a关键文件索引)
- [附录 B：关键宏与常量](#附录-b关键宏与常量)
- [附录 C：核心数据结构](#附录-c核心数据结构)

---

## 一、流计算整体调用链

### 1.1 三大物理角色

| 角色 | 代码目录 | 职责 |
|---|---|---|
| **mnode** | `source/dnode/mnode/impl/src/mndStream*.c` | 流元数据/状态管理、健康检查、协调派发 |
| **snode** | `source/dnode/snode/` | trigger / runner 任务的运行时容器 |
| **vnode** | `source/dnode/vnode/src/vnodeStream.c` | reader 任务（数据源端）所在节点 |
| **流核心库** | `source/libs/new-stream/src/` | 任务对象、调度、消息编排 |

### 1.2 任务三类型

| 类型 | 部署位置 | 职责 |
|---|---|---|
| `STREAM_READER_TASK` | vnode | 从 source 表读数据（含 trigger reader 与 calc reader 两子类） |
| `STREAM_TRIGGER_TASK` | snode | 触发协调，决定何时进入计算窗口 |
| `STREAM_RUNNER_TASK` | snode | 执行计算 plan |

### 1.3 控制面调用链（生命周期）

```
SQL: CREATE STREAM ...
  └─ parser → mndCreateStream
        └─ SDB 持久化 SStreamObj
        └─ msmAddStreamDeployAction → actionQ
              └─ (后台) msmLaunchStreamDeployAction
                    └─ 派 reader 到 toDeployVgMap
                    └─ 派 trigger/runner 到 toDeploySnodeMap
                    └─ hb 顺路下发部署指令

SQL: STOP/START/DROP STREAM ...
  └─ mndStopStream / mndStartStream / mndDropStream
        └─ 改 SStreamObj.userStopped / userDropped (SDB 持久化)
        └─ msmHealthCheck 周期巡检时统一回收/重建运行态映射
```

### 1.4 数据面/心跳面调用链

```
[snode/vnode 周期触发]
  └─ 收集本地任务状态 → SStreamHbMsg
        └─ TDMT_STREAM_HEARTBEAT RPC → mnode

[mnode RPC 工作线程]
  └─ msmWatchHandleHbMsg                  (横切：耗时统计/异常吞掉)
        └─ msmHandleStreamHbMsg            (主流程: 选 grpCtx + runtimeLock 读锁)
              └─ msmNormalHandleHbMsg
                    ├─ msmCheckUpdateDnodeTs / msmUpdateSnodeUpTs    (存活更新)
                    ├─ msmHandleStreamActions                         (消费 actionQ)
                    ├─ msmHandleStreamRequests                        (处理 task 上报的 mgmtReq)
                    ├─ msmGrpAddDeployVgTasks / msmGrpAddDeploySnodeTasks (顺路派活)
                    ├─ msmRspAddStreamsDeploy                         (打包部署指令到 rsp)
                    ├─ msmNormalHandleStatusUpdate                    (吃 task 状态)
                    └─ msmHandleHbPostActions                         (后置动作)
        └─ msmEncodeStreamHbRsp → RPC 应答
```

### 1.5 健康检查面

```
[mnode 主线程定时器]
  └─ mndDoTimerPullTask
        └─ msmHealthCheck                  (写锁独占; 双重门禁: 配置 + 时间窗)
              ├─ msmCheckLoopStreamMap     (流维度自愈; 含 msmCheckVgroupStatus)
              ├─ msmCheckLoopStreamSdb     (从 SDB 同步增量)
              └─ msmCheckLoopVgroupTopo    (vgroup 拓扑变更)
```

### 1.6 几个关键全局对象

```c
// mndStream.h:450+
struct mStreamMgmt {
  SRWLatch    runtimeLock;        // 全局读写锁 (hb 读 / health-check 写)
  SHashObj   *streamMap;          // streamId → SStmStatus*
  SHashObj   *taskMap;            // (streamId,taskId) → SStmTaskStatus* (16字节复合 key)
  SHashObj   *vgroupMap;          // vgId → SStmVgroupStatus
  SHashObj   *snodeMap;           // snodeId → SStmSnodeStatus
  SHashObj   *toDeployVgMap;      // vgId → SStmVgTasksToDeploy
  SHashObj   *toDeploySnodeMap;   // snodeId → SStmSnodeTasksDeploy
  SQueue     *actionQ;            // 异步动作队列
  SStmHCtx    hCtx;               // health-check 时间锚 (lastCheckTs / currentTs / slotIdx)
  SStmTCtx    tCtx[N];            // hb 处理 per-thread 上下文 (tCtx[tidx].grpCtx[gid])
};
```

---

## 二、mnode 心跳同步代码逻辑

### 2.1 协议消息结构（`include/common/streamMsg.h`）

```c
typedef struct {
  int32_t  streamGId;             // 流分组 ID, mnode 用以路由到 grpCtx
  int32_t  snodeId;               // 0 表示 vnode 上报, 非 0 表示 snode 上报
  SArray  *pStreamStatus;         // SStmTaskStatusMsg[] - 该 dnode 上每个 task 的状态
  SArray  *pStreamReq;            // int32[] - pStreamStatus 中携带 mgmtReq 的下标
  SArray  *pTriggerStatus;        // SSTriggerRuntimeStatus[] - trigger 内部细节
  ...
} SStreamHbMsg;
```

### 2.2 RPC 入口

```
mnode 注册 TDMT_STREAM_HEARTBEAT
  → mndProcessStreamHbMsg
      → msmWatchHandleHbMsg
          → msmHandleStreamHbMsg(pCtx)
              → msmNormalHandleHbMsg(pCtx)
              → msmEncodeStreamHbRsp → rsp
```

### 2.3 关键并发模型

- **多线程并发**：mnode RPC 工作线程池中多个线程会**并发**处理来自不同 snode/vnode 的 hb；
- **per-thread 路由**：`tidx = TID() % MND_STREAM_HB_THREAD_NUM` → `pCtx = &tCtx[tidx].grpCtx[streamGId]`；
- **runtimeLock**：hb 全程持读锁，与 health-check 写锁互斥；
- **细粒度锁**：vgroup/snode 内嵌结构有 per-entry `lock` 字段保护派发并行。

### 2.4 心跳承载的"4 大职能"

1. **存活上报**：刷新 `lastUpTs`（dnode/snode/vgroup/task 多层级）；
2. **状态汇报**：每个 task 自报 status / progress / errCode；
3. **mgmt 请求双向通道**：task 把"我需要 mnode 协调下发的指令"用 `pMgmtReq` 上报；mnode 在 rsp 里下发 `SStreamMgmtRsp`；
4. **顺路派活**：mnode 借 hb rsp 把待部署任务推到该 snode/vnode，避免单独 RPC。

### 2.5 数据面与控制面合流

设计要点是 hb 同时承担了**心跳 + 状态 + 部署 + 协调**，避免独立的"部署 RPC"，简化拓扑、减少 RPC 频次，但代价是：
- hb 处理流程复杂；
- 单个 hb 失败影响范围大（所以才有 `msmWatchHandleHbMsg` 兜底）；
- rsp 消息可能很大（部署指令体积）。

---

## 三、`msmHandleStreamRequests`

### 3.1 函数定位

`mndStreamMgmt.c:4578`，由 `msmNormalHandleHbMsg` 在持 `actionQLock` 写锁后调用。处理 hb 上报的 task 级 `pMgmtReq`。

### 3.2 函数主体

```c
int32_t msmHandleStreamRequests(SStmGrpCtx* pCtx) {
  SStreamHbMsg* pReq = pCtx->pReq;
  int32_t reqNum = taosArrayGetSize(pReq->pStreamReq);

  // 1. 按需初始化 rsp.rspList
  if (reqNum > 0 && NULL == pCtx->pRsp->rsps.rspList) {
    pCtx->pRsp->rsps.rspList = taosArrayInit(reqNum, sizeof(SStreamMgmtRsp));
  }

  // 2. 遍历每个 mgmt 请求
  for (int32_t i = 0; i < reqNum; ++i) {
    int32_t idx = *(int32_t*)taosArrayGet(pReq->pStreamReq, i);
    SStmTaskStatusMsg* pTask = taosArrayGet(pReq->pStreamStatus, idx);
    if (NULL == pTask)            continue;
    if (NULL == pTask->pMgmtReq)  continue;
    TAOS_CHECK_EXIT(msmHandleTaskMgmtReq(pCtx, pTask));
  }
  ...
}
```

### 3.3 三段式职责

| 段 | 操作 | 关键点 |
|---|---|---|
| ① | 初始化 `rspList` | 仅当确实有请求时才分配，按 `reqNum` 预设容量 |
| ② | 间接索引 | `pStreamReq` 存的是下标，跳着遍历 `pStreamStatus`，避免对全部 task 都做 mgmt 检查 |
| ③ | 派发 `msmHandleTaskMgmtReq` | 真正的请求-响应处理函数 |

### 3.4 设计意图

- **窄聚焦**：本函数只做**mgmt 请求**的派发，不碰存活更新/部署/状态收集；
- **跳跃遍历**：`pStreamReq` 充当稀疏索引，把"有 mgmtReq 的 task"分离出来；多数 hb 中 reqNum=0 → 函数直接返回 0；
- **fail-fast**：单个 `msmHandleTaskMgmtReq` 失败会立即 `_exit`，与上层 `msmNormalHandleHbMsg` 的 `TAOS_CHECK_EXIT` 协同把错误抛回 `msmWatchHandleHbMsg`；
- **持锁场景**：调用方已持 `actionQLock` 写锁（4630 行），保证 mgmt 请求处理与 actionQ 消费串行。

### 3.5 typo 提示

`pStreamReq`、`pStreamStatus`、`pStreamReq` 三者命名相近——`pStreamReq` 是**索引数组**，`pStreamStatus` 是**值数组**；阅读代码时务必区分。

---

## 四、`tCloneSStreamMgmtReq` 自赋值之谜

### 4.1 调用现场（`source/libs/new-stream/src/streamUtil.c:101`）

```c
TAOS_CHECK_EXIT(tCloneSStreamMgmtReq(pStatus->pMgmtReq, &pStatus->pMgmtReq));
```

第一个参数是**值** `pStatus->pMgmtReq`，第二个参数是**同一字段的地址** `&pStatus->pMgmtReq`。乍看像"自己赋值给自己"。

### 4.2 函数实际行为（`source/common/src/msg/streamMsg.c:101`）

```c
int32_t tCloneSStreamMgmtReq(SStreamMgmtReq* pSrc, SStreamMgmtReq** ppDst) {
  *ppDst = NULL;                                       // ① 立刻把 ppDst 写空
  if (NULL == pSrc) return TSDB_CODE_SUCCESS;

  *ppDst = taosMemoryCalloc(1, sizeof(SStreamMgmtReq)); // ② 新分配
  memcpy(*ppDst, pSrc, sizeof(*pSrc));                  // ③ 浅拷贝整体
  if (pSrc->cont.pReqs) {
    switch (pSrc->type) {                                // ④ 深拷贝内部数组
      case STREAM_MGMT_REQ_TRIGGER_ORIGTBL_READER:
        (*ppDst)->cont.pReqs = taosArrayDup(pSrc->cont.pReqs, NULL);
        break;
      case STREAM_MGMT_REQ_RUNNER_ORIGTBL_READER:
        ...对每个 SStreamOReaderDeployReq 再深拷 vgIds 数组...
        break;
    }
  }
  ...
}
```

### 4.3 真正语义：**值拷贝换原指针**

执行序列：

```
入参 pSrc 局部变量 = 原 pStatus->pMgmtReq          （捕获原指针）
*ppDst = NULL  ⇒  pStatus->pMgmtReq = NULL         （斩断引用）
*ppDst = calloc(...)  ⇒  pStatus->pMgmtReq = 新对象  （指向新副本）
memcpy(*ppDst, pSrc, ...)  ⇒  新副本 = 原对象内容深拷贝
```

**注意**：`pSrc` 是**按值传参的指针**，C 函数调用时已经把"原指针"拷到栈上；后续 `*ppDst = NULL` 改的是 `pStatus->pMgmtReq` 字段，不影响栈上的 `pSrc`。所以**没有自我覆盖**。

### 4.4 为什么需要这个操作？

`pStatus->pMgmtReq` 之前指向 task 自己持有的"原始 mgmt req"对象，这个对象生命周期由 task 控制。心跳要把 mgmt req **塞进 hb 消息**送往 mnode：
- hb 消息体随 RPC 发出后，需要独立的副本（避免 task 在另一线程释放原对象造成 use-after-free）；
- 调用完后 `pStatus->pMgmtReq` 已经是**新分配的副本**，原对象由调用前的栈上指针 `pSrc` 隐式"传递所有权"——但代码没有显式 free 原对象。

### 4.5 隐含问题

仔细看：调用前 `pStatus->pMgmtReq` 是 `taosArrayPush(pMsg->pStreamStatus, pTask)` 后从 last 取的，**`pStatus` 是 hb 消息内的元素，其 `pMgmtReq` 字段是 task 字段的浅拷贝**——所以原指针实际上**仍由 task 拥有**，hb 消息只持有副本。这个调用的本质是：

> **"把 hb 消息里指向 task 内部对象的指针，替换成新分配的独立深拷贝"**——为了让 hb 消息可以独立于 task 对象的生命周期发送出去。

所以这个看似多余的调用是**故意的**：第二参数复用 `&pStatus->pMgmtReq` 不是 bug，而是利用 C 的值传参规则在原地完成"指针替换"。

### 4.6 可读性建议

写法很 tricky，建议加注释说明：

```c
// pStatus->pMgmtReq currently aliases task-owned memory; replace it with an
// independent deep copy so the hb message can outlive the task's lock.
TAOS_CHECK_EXIT(tCloneSStreamMgmtReq(pStatus->pMgmtReq, &pStatus->pMgmtReq));
```

---

## 五、`stmHandleStreamRemovedTasks` 的必要性

### 5.1 函数定位（`source/libs/new-stream/src/streamUtil.c:79`）

```c
void stmHandleStreamRemovedTasks(SStreamInfo* pStream, int64_t streamId, int32_t gid) {
  if (taosArrayGetSize(pStream->undeployReaders) > 0)
    smHandleRemovedTask(pStream, streamId, gid, STREAM_READER_TASK,
                        pStream->undeployReaders, pStream->readerList);
  if (taosArrayGetSize(pStream->undeployTriggers) > 0)
    smHandleRemovedTask(pStream, streamId, gid, STREAM_TRIGGER_TASK,
                        pStream->undeployTriggers, pStream->triggerList);
  if (taosArrayGetSize(pStream->undeployRunners) > 0)
    smHandleRemovedTask(pStream, streamId, gid, STREAM_RUNNER_TASK,
                        pStream->undeployRunners, pStream->runnerList);
}
```

由 `stmHbAddStreamStatus` 在持 `pStream->lock` 写锁后调用，是 snode/vnode 侧 **hb 准备阶段**的清理动作。

### 5.2 调用动机：**确保上报状态与本地实际一致**

`pStream->undeployReaders/undeployTriggers/undeployRunners` 三个数组累积**待卸载**的任务（来自前一次 hb rsp 中 mnode 下发的卸载指令、或本地异常时主动加入）。

如果 hb 在拼装 `pStreamStatus` 时不先处理这些 undeploy：
- 会把已经标记卸载的 task 状态继续上报给 mnode；
- mnode 据此误判 task 仍存活 → 不会重新派发；
- 形成"两边都认为对方在维护"的孤岛。

`stmHandleStreamRemovedTasks` 就是**hb 上报前的本地体检**：把 undeploy 列表里的任务从 `readerList/triggerList/runnerList` 三个真值列表中真正移除，并释放资源。

### 5.3 为什么不在收到 mnode 卸载指令时立即处理

主要是**锁层级与异步性**：
- 收到指令的处理线程未必持 `pStream->lock`；
- 真正卸载需要协调 task 自身的运行状态（可能在其他线程跑 step）；
- 把 undeploy 加入数组等到下一轮 hb 准备阶段统一处理，**复用 hb 已抢到的 `pStream->lock` 写锁**，避免重复加锁竞争。

### 5.4 三类任务对称处理

reader/trigger/runner 三类用同一个 `smHandleRemovedTask` 模板处理，参数化区分类型。这是**消除重复**的标准模式，但要注意三个真值列表的容器类型不同（`readerList` 是 `SListIter` 链表，`triggerList`/`runnerList` 多为数组），由 `smHandleRemovedTask` 内部根据 type 分支处理。

---

## 六、心跳消息中 `pTriggerStatus` 的作用

### 6.1 字段定位

`SStreamHbMsg.pTriggerStatus`（数组类型 `SArray<SSTriggerRuntimeStatus>`），由 snode 侧 trigger task 周期性填充，仅 trigger 节点会上报。

### 6.2 数据来源

```c
// streamUtil.c:50-77 (类似)
SSTriggerRuntimeStatus status = {0};
TAOS_CHECK_EXIT(stTriggerTaskGetStatus((SStreamTask*)triggerTask, &status));
TSDB_CHECK_NULL(taosArrayPush(*ppReport, &status), code, lino, _exit, terrno);
```

由 `stTriggerTaskGetStatus` 从 trigger task 内部状态机抽取。

### 6.3 五大用途

| 用途 | 说明 |
|---|---|
| **存活感知** | mnode 据此刷新 `pStatus->triggerTask->lastUpTs`，喂 health-check |
| **进度可观测** | 含 `sessionId`、`pendingMsgs`、`recalcInProgress`、`userRecalcs[]` 等字段，供 SHOW STREAMS 展示 |
| **错误冒泡** | trigger 内部错误码通过此字段冒到 mnode → `msmStopStreamByError` |
| **拓扑漂移检测** | 与 mnode 持有的 `trigReaders` 列表比对，不一致 → reader 重派发 |
| **重协商触发** | 携带 `lastTrigMgmtReqId` 等让 mnode 判断是否需要新的 mgmt 指令 |

### 6.4 与 `pStreamStatus` 互补

| 字段 | 粒度 | 内容 |
|---|---|---|
| `pStreamStatus` | per task | 通用任务状态（在线/进度/错误码） |
| `pTriggerStatus` | per trigger task | trigger 特有的协调状态 |

`pTriggerStatus` 是 trigger 单点真值流向 mnode 的细粒度通道，给"协调者"专属。

---

## 七、`msmHandleStreamHbMsg` 并发安全

### 7.1 关键代码

```c
int32_t tidx = TID() % MND_STREAM_HB_THREAD_NUM;
SStmGrpCtx* pCtx = &mStreamMgmt.tCtx[tidx].grpCtx[pHb->streamGId];
pCtx->pMnode = pMnode;
pCtx->lastTs = currTs;
pCtx->pReq   = pHb;
pCtx->pRsp   = ...
...
msmNormalHandleHbMsg(pCtx);
```

### 7.2 是否并发安全？**严格语义下：否**

`tidx = TID() % N` 仅按线程 ID 散列：
- 多个工作线程的 `TID() % N` 可能落到同一 tidx；
- 即使不冲突，`pHb->streamGId` 不同时仍同槽位；
- 如果两个 RPC 线程恰好选中同一 `(tidx, streamGId)`，会**同时写**同一 `pCtx`。

### 7.3 现状能"近似稳"的原因

1. `pCtx` 多为重置式赋值，不累加（每次 hb 全量覆盖）；
2. 单个 hb 处理快，时间窗很短；
3. 真正会被持久化/共享的状态修改都落在 `pStatus`/`mStreamMgmt.*` 上，由 `runtimeLock` + per-entry 锁保护；
4. `pCtx` 的内容主要是"本次 hb 的临时上下文"，结束就失效。

### 7.4 隐患

| 场景 | 风险 |
|---|---|
| 未来在 `pCtx` 加计数器 | 必丢更新 |
| 异常路径访问 `pCtx->pReq` 时另一线程已改 | 悬空指针/use-after-free |
| 调试日志拼接 `pCtx->lastTs` | 偶发错乱 |

### 7.5 建议

要么改用 `thread_local` / pthread key 实现严格 per-thread 独占，要么显式在 `pCtx` 上加 latch 序列化访问。当前实现是**侥幸正确**，应在 review 时标注为待加固点。

### 7.6 与 `runtimeLock` 的关系

注意：`pCtx` 写入是在抢 `runtimeLock` **之前**，所以 `runtimeLock` 不保护 `pCtx`。必须靠"线程亲和性"——而当前 `TID() % N` 不能保证。

---

## 八、`msmWatchHandleHbMsg`

### 8.1 角色

`msmHandleStreamHbMsg` 的**外层 watcher**，负责横切关注点。

### 8.2 主要职责

| 职责 | 说明 |
|---|---|
| **生命周期门禁** | 进入前检查 `mStreamMgmt.stopped`，mnode 退出中直接返回 |
| **streamGId 校验** | 越界 streamGId 直接拒绝，防止后面访问 `tCtx[].grpCtx[]` 数组越界 |
| **耗时统计** | 记录 begin/end 时间戳，超过阈值打 warn |
| **错误吞掉** | 单条 hb 出错只打日志、不上抛 RPC 框架，避免 client 反复重试雪崩 |
| **指标上报** | 维护 hbStats 类内部计数器，供 SHOW 展示 |

### 8.3 设计要点

- **横切分离**：把可观测性 / 异常兜底从主业务函数剥离，主函数 `msmHandleStreamHbMsg` 保持纯净；
- **swallow 策略**：对 hb 这种高频心跳，让 client 重试反而加剧问题；选择 swallow + 等下一轮 hb 自然修复；
- **早返回**：`mnode stopping` / `streamGId out of range` 等场景在最早期拒绝，避免下游 useless work。

---

## 九、`SStreamObj.userDropped` 与 `userStopped`

### 9.1 字段语义

```c
struct SStreamObj {
  ...
  int8_t userStopped;   // 用户 STOP STREAM, SDB 仍保留
  int8_t userDropped;   // 用户 DROP STREAM, 即将物理删除
  ...
};
```

| 字段 | 触发命令 | 持久化 | 可逆 |
|---|---|---|---|
| `userStopped` | `STOP STREAM` | SDB | 可 `START STREAM` |
| `userDropped` | `DROP STREAM` | SDB（短暂）→ 删除 | 不可逆 |

### 9.2 与运行态 `SStmStatus.stopped` 的对照

| 来源 | 字段 | 持久化 | 重建 |
|---|---|---|---|
| 用户意图 | `SStreamObj.userStopped/userDropped` | SDB | 恢复时从 SDB 读 |
| 运行态 | `SStmStatus.stopped` | 内存 | mnode 重启后从 SDB + 用户意图重建 |

`SStmStatus.stopped` 取值（`mndStream.h:271`）：

| 值 | 含义 | 恢复方式 |
|---|---|---|
| 0 | 运行中 | — |
| 1 | error stopped | health-check 重试 |
| 2 | user stopped | 用户 START |
| 3 | grant stopped | 许可证恢复后自动重试 |

判定宏：`MST_IS_USER_STOPPED / MST_IS_ERROR_STOPPED / MST_IS_GRANT_STOPPED`。

### 9.3 守门员模式

所有写运行态结构的路径（部署、派发、重置）入口都先看用户意图：

```c
int8_t userStopped = atomic_load_8(&pStream->userStopped);
int8_t userDropped = atomic_load_8(&pStream->userDropped);
if (userStopped || userDropped) {
  mstsWarn(...);  goto _exit;
}
```

这是**用户意图最高优先级**原则的体现：避免 health-check 自动重试与用户 STOP 操作打架。

### 9.4 双层状态表的优势

- 用户层稳定持久（SDB），运行层可快速翻转（内存 atomic 位）；
- 用户层与运行层解耦后，mnode 故障重启可"清空运行层、保留用户意图"自然达成幂等；
- 对外接口（SHOW）仍可同时呈现两层状态，便于诊断"用户没停为什么不跑"。

### 9.5 流转典型时序

```
用户 STOP → SStreamObj.userStopped=1 (SDB)
        → health-check 巡到 → msmRemoveStreamFromMaps (清运行态)
        → SStmStatus.stopped=2 (USER_STOPPED)
用户 START → SStreamObj.userStopped=0 (SDB)
        → 投递 deployAction → msmLaunchStreamDeployAction
        → CAS stopped 2→0 + msmResetStreamForRedeploy + 重新派发
```

---

## 十、`msmHealthCheck`

### 10.1 函数定位

`mndStreamMgmt.c:5299-5324`，由 `mndDoTimerPullTask` 周期触发（`mndMain.c:513`），是 mnode 的**自愈巡更员**。

### 10.2 双重门禁

```c
if (!tsStreamCheck) return;                                           // 配置门禁
if (currTs - mStreamMgmt.hCtx.lastCheckTs < PERIOD_MS) return;        // 时间窗门禁
```

两道门禁避免高频持锁。

### 10.3 写锁独占

```c
taosWLockLatch(&mStreamMgmt.runtimeLock);
mStreamMgmt.hCtx.currentTs = currTs;
mStreamMgmt.hCtx.slotIdx   = (slotIdx + 1) % SLOT_NUM;
... 三大子任务 ...
taosWUnLockLatch(&mStreamMgmt.runtimeLock);
```

整轮持写锁——hb 处理（持读锁）必须等待。所以 health-check 必须**快**：只做内存扫描和 action 投递，不做 IO。

### 10.4 时间片轮转 `slotIdx`

`(prev + 1) % SLOT_NUM` 时间片轮转，每条流根据 `streamId % SLOT_NUM` 决定本轮是否被检查。把 N 条流摊到 SLOT_NUM 轮里，降低单轮负载尖峰，代价是单流检查间隔放大 SLOT_NUM 倍。

### 10.5 三大子任务

```c
msmCheckLoopStreamMap(...);   // 流维度自愈
msmCheckLoopStreamSdb(...);   // SDB 增量同步 (新增/删除流的发现)
msmCheckLoopVgroupTopo(...);  // vgroup 拓扑变更
```

三者按顺序串行执行。

### 10.6 设计意图

- **统一自愈入口**：所有运行态偏移最终靠它纠偏，链路单一好排查；
- **写锁屏障**：hb 路径在此期间被阻塞，确保自愈期间状态稳定不被并发改；
- **时间锚 currentTs**：所有子任务以同一 ts 判定超期，避免子任务之间因时间漂移产生不一致结论。

---

## 十一、`msmCheckVgroupStatus`

### 11.1 函数定位

`mndStreamMgmt.c:5059-5098`，vgroup 维度 reader task 巡警，由 `msmCheckLoopStreamMap` 内层调用。

### 11.2 时间片过滤

仅 `vgId % SLOT_NUM == hCtx.slotIdx` 时才进入实质检查（与 `msmHealthCheck` 同节奏）。

### 11.3 双层超期门禁

| 阈值 | 含义 | 触发动作 |
|---|---|---|
| `MND_STREAM_ISOLATION_PERIOD_NUM`（如 1） | 短失联 | 仅打 warn，可能 SID++ 重派发 |
| `5 × MND_STREAM_ISOLATION_PERIOD_NUM` | 长失联 | 调 `msmHandleVgroupLost` 整 vg 失联处理 |

短失联给网络抖动留窗口；长失联才真正停流。

### 11.4 SDB 真值复核

发现 vgroup 异常时**先回查 mndDb/SDB**确认 vgroup 是否仍存在，避免误判"已被合并/删除的 vg"为"失联"。

### 11.5 处理粒度

| 情况 | 处理 |
|---|---|
| 个别 reader task 滞后 | `SID++` 触发该 task 轻量重部署 |
| 整 vgroup 失联 | `msmHandleVgroupLost` 停掉所有该 vg 上的流 |

### 11.6 vgroup vs snode 处理差异

- **vgroup**：reader task 是数据源端，可单点重新拉起，更精细；
- **snode**：trigger/runner 失联通常意味着整批任务全军覆没，需要更激进的整体回收。

---

## 十二、`msmStopStreamByError`

### 12.1 函数签名（`mndStreamMgmt.c:97-143`）

```c
int32_t msmStopStreamByError(int64_t streamId, SStmStatus* pStatus, int32_t errCode);
```

mnode 流的**统一停摆入口**——所有运行时错误最终汇到这里。

### 12.2 关键参数

- `pStatus` 可选：调用方有则直接传，否则函数自行 `taosHashAcquire` 拿引用；
- `errCode`：触发停摆的错误码，会写入 `pStatus->lastErrCode`。

### 12.3 幂等 + CAS 双重防抖

```c
if (atomic_load_8(&pStatus->stopped) != 0) return;        // 幂等

if (currTs - pStatus->runningStartTs > FORGET_WINDOW)     // 时间窗滑动遗忘
  pStatus->errCount = 0;
pStatus->errCount++;

if (atomic_val_compare_exchange_8(&pStatus->stopped, 0, 1) != 0) return;  // CAS

mndStreamSetEvent(pStatus, STM_EVENT_STM_TERR);            // 投递事件
```

两道防线：先幂等过滤热路径上的 CAS，再 CAS 做线性化最终判定。

### 12.4 两档退避

`errCount > MND_STREAM_RETRY_MAX_NUM`（如 10）后切大档：`retryDelay` 翻倍，避免高频重试拖垮系统。

### 12.5 仅状态修改、不下发指令

本函数**只翻状态位**：`stopped=1` + `STM_EVENT_STM_TERR` 入 actionQ。真正的资源回收/重新派发由后续 health-check / hb 路径根据状态位驱动。这是**事件源化**设计——状态变更与动作执行解耦。

### 12.6 调用点全景

`grep` 找到 12 处调用，覆盖：
- mgmt 请求构造失败
- reader task 失联超期
- trigger 异常上报
- deploy plan 解析失败
- grant 校验失败
- vgroup 失联连带停流
- 等等

### 12.7 关联宏 `MND_STREAM_SET_LAST_TS`

统一更新 `lastUpTs` 等时间戳字段的封装宏（`mndStream.h:424-428`），保证多处更新逻辑一致。

---

## 十三、`msmTDRemoveStream`

### 13.1 函数定位与命名

`mndStreamMgmt.c:2669-2715`，`TD = Tasks-to-Deploy`。轻量预剪枝版的流卸载。

### 13.2 关键事实：**无任何调用方**

`grep` 全仓库确认仅 `static` 定义，**无任何调用点**。属于：
- 预留接口（未来某清理路径计划用），或
- 死代码（重构后遗留）。

### 13.3 函数行为

仅扫 `toDeployVgMap` 与 `toDeploySnodeMap` 两张待派发表：
- 命中匹配 streamId 且未派发的 `SStmTaskToDeployExt`
- **只设置 `pExt->deployed = true`** 软作废
- **不释放内嵌 plan/list 内存**
- **不维护任何计数器**
- **不加锁**

### 13.4 fast-path

进入前先看全局原子量 `mStreamMgmt.toDeployTaskNum`：为 0 直接返回，避免空扫。

### 13.5 与守门员模式的冗余

`userDropped/userStopped` 已在所有派发路径前置检查，即使有残留 `toDeploy` 条目也不会被真正派发。所以本函数即使不调，系统也能正常工作——这进一步印证它是死代码或预留。

### 13.6 重构建议

要么移除，要么补充注释说明预留意图（区分"死代码"与"未来扩展点"）。

---

## 十四、`msmSTRemoveStream`

### 14.1 函数定位

`mndStreamMgmt.c:2280-2381`，`ST = Stream-runtime Tasks`，是**完整版**的流卸载/重置实现。

### 14.2 函数签名

```c
static int32_t msmSTRemoveStream(int64_t streamId, bool fromStreamMap);
```

| `fromStreamMap` | 调用方 | 语义 |
|---|---|---|
| `true` | `msmRemoveStreamFromMaps` (2723) | 彻底卸载（用户 DROP / SDB 已无） |
| `false` | `msmResetStreamForRedeploy` (2386) | 保留 streamMap 壳，清干净以便重部署 |

### 14.3 6 段主干

| 步骤 | 操作对象 | 关键动作 |
|---|---|---|
| 1 | `toDeployVgMap` | 持 `pVg->lock` 写锁；对匹配条目调 `mstDestroySStmTaskToDeployExt` 真释放内嵌 plan/list，再 `pExt->deployed=true` |
| 2 | `toDeploySnodeMap` | 同上，trigger/runner 两列表对称处理 |
| 3 | `snodeMap` | 对每个节点 `taosHashRemove(pSnode->streamTasks, &streamId, ...)` |
| 4 | `vgroupMap` | 同上 |
| 5 | `taskMap` | 复合 key 16 字节全表 iterate，前 8 字节 == streamId 的逐个 `taosHashRemove` |
| 6 | `streamMap` | 仅当 `fromStreamMap=true` 时 `taosHashRemove`，触发 `mstDestroySStmStatus` 释放整个壳 |

### 14.4 与 `msmTDRemoveStream` 的核心差异

| 维度 | `msmTDRemoveStream` | `msmSTRemoveStream` |
|---|---|---|
| 加锁 | 不加 | per-vg / per-snode 写锁 |
| 待部署侧 | 仅打标软作废 | destroy 真释放 + 打标 |
| 运行态侧 (snode/vgroupMap, taskMap) | 不碰 | 全部清理 |
| streamMap | 不动 | 可选删 |
| 调用方数量 | 0 | 2 |

### 14.5 复合 key 扫描

```c
size_t keyLen = 0;
while ((pIter = taosHashIterate(mStreamMgmt.taskMap, pIter))) {
  int64_t* pKey = taosHashGetKey(pIter, &keyLen);   // 16 字节
  if (*pKey == streamId) {
    taosHashRemove(mStreamMgmt.taskMap, pKey, keyLen);
  }
}
```

hash 函数对全 16 字节散列，无法按前缀删，遍历是必然代价。`taosHashIterate` 中 remove 当前游标元素是安全的。

### 14.6 两个调用方的语义

**A. `msmRemoveStreamFromMaps`（彻底卸载）**

```c
TAOS_CHECK_EXIT(msmSTRemoveStream(streamId, true));
```

由 `msmCheckLoopStreamMap` 在三种情况下调用：
1. 流被用户 STOP（USER_STOPPED）→ 卸下运行态映射但 SDB 还在；
2. SDB 已无该流（用户 DROP 已提交）→ 永久卸下；
3. 兜底 fallback。

**B. `msmResetStreamForRedeploy`（重部署前重置）**

```c
(void)msmSTRemoveStream(streamId, false);
mstResetSStmStatus(pStatus);
pStatus->deployTimes++;
```

由 `msmLaunchStreamDeployAction` 在两种情况下调用：
1. `stopped == 0` 但需重部署（如 health-check 触发 ERROR 重试到点）；
2. `stopped != 0` 且能 CAS 翻回 0（流之前因 ERROR 停了，现在重新拉起）。

### 14.7 锁层级

| 数据 | 同步手段 |
|---|---|
| `pVg->lock / pSnode->lock` | per-entry 写锁（本函数自抢） |
| `pExt->deployed` | per-entry 写锁内修改 |
| `streamTasks`（snode/vg 内嵌 hash） | 依赖外层 `runtimeLock` |
| `taskMap / streamMap` | 依赖外层 `runtimeLock` |

外层调用约定：health-check 路径已持 `runtimeLock` 写锁；hb 路径持读锁 + thread-affinity 保证 streamGId 单线程独占。

### 14.8 设计要点

| 设计点 | 价值 |
|---|---|
| per-vg / per-snode 写锁 | 与派发线程冲突最小化 |
| `pVg->deployed == taskNum` 剪枝 | 整 vg 已清的快速跳过 |
| destroy + `deployed=true` 双步 | destroy 释放堆内存，deployed 阻塞后续派发；destroy 内部 `if (pExt->deployed) return` 防重复 |
| 节点壳保留 | snodeMap/vgroupMap/toDeployXxxMap 节点不删，只删内嵌 streamTasks 条目 |
| `fromStreamMap` 二态 | 一个函数兼顾"卸载"和"重置"，结构对称 |
| taskMap 复合 key 全表扫描 | hash 不支持按前缀删，遍历是必然代价 |
| iterate-then-remove 安全 | 依赖 TDengine taosHash 的 iterate-friendly 语义 |
| 返回 code 仅末次 | 错误信息靠日志，对调用方无强语义 |

---

## 附录 A：关键文件索引

| 路径 | 角色 |
|---|---|
| `source/dnode/mnode/impl/src/mndStreamMgmt.c` | mnode 流管理主体 |
| `source/dnode/mnode/impl/src/mndStreamUtil.c` | 释放/重置工具函数 |
| `source/dnode/mnode/impl/inc/mndStream.h` | 数据结构、宏 |
| `source/dnode/mnode/impl/src/mndMain.c` | 周期性调度入口（5xx 行调 msmHealthCheck） |
| `source/libs/new-stream/src/streamUtil.c` | snode/vnode 侧 hb 准备工具 |
| `source/common/src/msg/streamMsg.c` | 流消息编解码与克隆 |
| `include/common/streamMsg.h` | 流消息协议定义 |
| `source/dnode/snode/` | snode 节点实现（trigger/runner 容器） |
| `source/dnode/vnode/src/vnodeStream.c` | vnode 端 reader 容器 |

## 附录 B：关键宏与常量

- `MND_STREAM_HEALTH_CHECK_PERIOD_SEC` — health-check 周期（秒）
- `MND_STREAM_HEALTH_CHECK_SLOT_NUM` — 时间片轮转槽位（如 10）
- `MND_STREAM_ISOLATION_PERIOD_NUM` — 失联隔离基础阈值
- `MND_STREAM_RETRY_MAX_NUM` — 切大档退避的失败次数阈值
- `MND_STREAM_RUNNER_DEPLOY_NUM` — 单流 runner 部署组数
- `MND_STREAM_HB_THREAD_NUM` — hb 处理线程组数
- `MST_IS_USER_STOPPED / MST_IS_ERROR_STOPPED / MST_IS_GRANT_STOPPED` — `SStmStatus.stopped` 取值判定
- `MND_STREAM_SET_LAST_TS` — 统一更新 lastUpTs 的宏
- `MST_PASS_ISOLATION / MST_*_NEED_HANDLE` — health-check 子任务过滤宏
- `STM_EVENT_STM_TERR` — 错误停摆事件位

## 附录 C：核心数据结构

```c
// mndStream.h (示意)
typedef struct SStmStatus {
  char       *streamName;
  int64_t     deployTimes;
  int8_t      stopped;          // 0=运行 1=error 2=user 3=grant
  int32_t     errCount;
  int32_t     lastErrCode;
  int64_t     runningStartTs;
  SRWLatch    resetLock;
  SArray     *trigReaders;
  SArray     *trigOReaders;
  SList      *calcReaders;
  SStmTaskStatus *triggerTask;
  SArray     *runners[MND_STREAM_RUNNER_DEPLOY_NUM];
  int64_t     lastTrigMgmtReqId;
  SArray     *userRecalcList;
  SCMCreateStreamReq *pCreate;
  ...
} SStmStatus;

typedef struct SStmTaskStatus {
  int64_t streamId;
  int64_t taskId;
  int64_t lastUpTs;
  int64_t runningStartTs;
  SRWLatch detailStatusLock;
  void   *detailStatus;
  ...
} SStmTaskStatus;

typedef struct SStmVgroupStatus {
  SRWLatch    lock;
  int64_t     lastUpTs;
  SHashObj   *streamTasks;       // streamId → SStmVgStreamStatus
} SStmVgroupStatus;

typedef struct SStmSnodeStatus {
  SRWLatch    lock;
  int64_t     lastUpTs;
  SHashObj   *streamTasks;       // streamId → SStmSnodeStreamStatus
  int32_t     runnerThreadNum;
} SStmSnodeStatus;

typedef struct SStmTaskToDeployExt {
  bool deployed;                 // tombstone bit
  struct {
    SStreamTask task;
    union {
      SStreamTriggerDeployMsg trigger;     // readerList / runnerList
      SStreamReaderDeployMsg  reader;
      SStreamRunnerDeployMsg  runner;      // pPlan
    } msg;
  } deploy;
} SStmTaskToDeployExt;

typedef struct SStmVgTasksToDeploy {
  SRWLatch lock;
  int32_t  deployed;              // 已派发计数
  SArray  *taskList;              // SStmTaskToDeployExt[]
} SStmVgTasksToDeploy;

typedef struct SStmSnodeTasksDeploy {
  SRWLatch lock;
  int32_t  triggerDeployed;
  int32_t  runnerDeployed;
  SArray  *triggerList;           // SStmTaskToDeployExt[]
  SArray  *runnerList;            // SStmTaskToDeployExt[]
} SStmSnodeTasksDeploy;
```

---

> **会话补充**：本次分析未改动任何源码；所有结论基于阅读现有代码 + grep 全仓库交叉验证。
> 凡涉及"侥幸正确""死代码""可能预留"等判断，建议结合 git blame / 相关 PR 二次确认。
