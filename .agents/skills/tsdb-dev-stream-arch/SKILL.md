---
name: tsdb-dev-stream-arch
description: Explains stream code & arch with visual diagrams and analogies. Use when explaining how stream code works, or when a starter asks "how does stream work?"
metadata:
  author: Stephen Jin
  version: 1.0.0
  owner_team: engine
---

When explaining the stream codebase & architecture, always include:

1. **Start with an analogy**: Compare the architecture to a real-world system the developer already understands (e.g., a newspaper production line, water treatment plant, or TDengine's own WAL pipeline in reverse).
2. **Draw a diagram**: Use ASCII art to show the flow, structure, or relationships. Always include the 4-layer view: MNode → SNode (Trigger + Runners) → VNodes (Readers) → Destination Table.
3. **Walk through the code**: Explain step-by-step what happens at each stage, referencing actual file paths and function names from the knowledge base below.
4. **Highlight gotchas**: What are common mistakes or misconceptions? (e.g., the Trigger never computes aggregates itself; Readers are pull-based not push-based; the executor pool recycles instances).
5. **Focus on the big three — Reader, Trigger, Runner**: Explain how each works, how they communicate, and the differences between trigger readers and calc readers.

Keep explanations conversational. If a diagram is possible, draw one and explain it well. For complex concepts, use multiple analogies.

---

## Stream Architecture Knowledge Base

Use the following as ground truth when answering questions. Do NOT re-explore the codebase for information already covered here — go straight to explaining.

### Core Concepts

TDengine stream processing is a **distributed, pull-then-push pipeline** with three task types deployed across the cluster:

- **Reader** (on VNodes): Scans WAL (real-time) or TSDB (historical) for raw source data. Pull-based — waits for requests from the Trigger.
- **Trigger** (on SNode, 1 per stream): The orchestrator. Pulls data from Readers, merges it, checks window/trigger conditions, dispatches calculation requests to Runners.
- **Runner** (on SNodes, N parallel): Executes the actual SQL query plan (aggregations, transforms). Writes results to the destination table via the data sink.

The **MNode** is the control plane — it handles CREATE/DROP STREAM, deploys tasks, monitors health via heartbeats, and rebalances on failure.

### Key Source Files

| File | Purpose |
|------|---------|
| `include/libs/new-stream/stream.h` | Reader/Trigger/Runner task structs, macros, constants |
| `include/common/streamMsg.h` | All message types, enums (`EStreamStatus`, `EStreamTaskType`), `SStreamTask` base struct, `SCMCreateStreamReq` |
| `source/libs/new-stream/src/streamMgmt.c` | Per-DNode task registration & lifecycle, `gStreamMgmt` global singleton |
| `source/libs/new-stream/src/streamTriggerTask.c` | Trigger main execution loop (`stTriggerTaskExecute`), window management, dispatch |
| `source/libs/new-stream/src/streamTriggerMerger.c` | Multi-way merge sort of data from multiple Readers |
| `source/libs/new-stream/src/streamRunner.c` | Runner execution (`stRunnerTaskExecute`), executor pool, output/sink |
| `source/libs/new-stream/src/streamReader.c` | VNode-side WAL/TSDB scanning, responds to pull requests |
| `source/libs/new-stream/src/streamHb.c` | Heartbeat protocol (600ms interval) |
| `source/libs/new-stream/src/streamCheckpoint.c` | State checkpoint and recovery |
| `source/libs/new-stream/src/stream.c` | Top-level stream init (`streamInit()`) |
| `source/libs/new-stream/src/streamTimer.c` | Timer management for periodic triggers |
| `source/libs/new-stream/src/dataSinkMgr.c` | Result writing orchestration |
| `source/libs/new-stream/src/dataSinkCache.c` | In-memory result buffering |
| `source/libs/new-stream/src/dataSinkFile.c` | Disk spill for overflow |
| `source/libs/new-stream/src/freeBlockMgr.c` | Memory block pool management |
| `source/dnode/mnode/impl/src/mndStream.c` | MNode: stream DDL, deployment planning, rebalance |
| `source/dnode/vnode/src/vnd/vnodeStream.c` | VNode-side stream integration |

### Key Data Structures

**Base task** (`streamMsg.h`):
```c
typedef struct SStreamTask {
  EStreamTaskType type;     // STREAM_READER_TASK, STREAM_TRIGGER_TASK, STREAM_RUNNER_TASK
  int64_t streamId;
  int64_t taskId;
  int64_t seriousId;        // deploy index
  int64_t flags;
  int32_t nodeId;           // vgroup or snode ID
  int64_t sessionId;        // real-time, historical, or recalculation
  EStreamStatus status;
  int32_t errorCode;
  SStreamMgmtReq* pMgmtReq;
  // ... locking, callbacks, etc.
} SStreamTask;
```

**Reader task** (`stream.h`):
```c
typedef struct SStreamReaderTask {
  SStreamTask task;
  int8_t      triggerReader;  // triggered by Trigger?
  void*       info;           // VNode scan context
} SStreamReaderTask;
```

**Runner task** (`stream.h`):
```c
typedef struct SStreamRunnerTask {
  SStreamTask              task;
  SStreamRunnerTaskExecMgr execMgr;     // executor pool (free/running lists)
  SStreamRunnerTaskOutput  output;      // dest table schema
  const char              *pPlan;       // query plan string
  int32_t                  parallelExecutionNun;
  bool                     topTask;
  bool                     lowLatencyCalc;
  // ... message callbacks, subtable expressions, etc.
} SStreamRunnerTask;
```

**Executor pool** (`stream.h`):
```c
typedef struct SStreamRunnerTaskExecMgr {
  SList*        pFreeExecs;     // idle executors ready for reuse
  SList*        pRunningExecs;  // currently computing
  TdThreadMutex lock;
  int32_t       execBuildNum;   // total built
} SStreamRunnerTaskExecMgr;
```

### Trigger Types

Defined in `EStreamTriggerType` (`streamMsg.h`):

| Type | Fires when... | Config struct |
|------|---------------|---------------|
| `STREAM_TRIGGER_PERIOD` | Fixed time interval elapses | `SPeriodTrigger` (period, offset) |
| `STREAM_TRIGGER_SLIDING` | Sliding/tumbling window boundary crossed | `SSlidingTrigger` (interval, sliding, offset) |
| `STREAM_TRIGGER_SESSION` | Gap between events exceeds threshold | `SSessionTrigger` (slotId, sessionVal) |
| `STREAM_TRIGGER_COUNT` | N rows accumulated | `SCountTrigger` (countVal, sliding) |
| `STREAM_TRIGGER_STATE` | State expression changes | `SStateWinTrigger` (expr, trueForType/Count/Duration) |
| `STREAM_TRIGGER_EVENT` | Start/end conditions met | `SEventTrigger` (startCond, endCond) |

### Task Status State Machine

```
UNDEPLOYED ──deploy──► INIT ──start──► RUNNING
                                          │
                              ┌───────────┼───────────┐
                              ▼           ▼           ▼
                          STOPPED      FAILED     DROPPING
                         (by user)    (error)   (DROP STREAM)
```

Defined as `EStreamStatus` in `streamMsg.h`:
- `STREAM_STATUS_UNDEPLOYED = 0`
- `STREAM_STATUS_INIT = 1`
- `STREAM_STATUS_RUNNING`
- `STREAM_STATUS_STOPPED`
- `STREAM_STATUS_FAILED`
- `STREAM_STATUS_DROPPING`

### Stream Message Types

```c
typedef enum SStreamMsgType {
  STREAM_MSG_START,              // kick off a task
  STREAM_MSG_UNDEPLOY,           // tear down a task
  STREAM_MSG_ORIGTBL_READER_INFO,
  STREAM_MSG_UPDATE_RUNNER,
  STREAM_MSG_USER_RECALC,
  STREAM_MSG_RUNNER_ORIGTBL_READER,
} SStreamMsgType;
```

### Complete Data Path (trace this when explaining "how does stream work?")

```
1. CREATE STREAM SQL
   └─► mndProcessCreateStreamReq()
       ├── Parse SQL → SCMCreateStreamReq (triggerType, plans, output schema)
       ├── mndStreamCreateOutStb() (auto-create dest super table)
       └── Deploy tasks to DNodes via SStmStreamDeploy RPC

2. DNode receives deployment
   └─► smAddTasksToStreamMap()
       ├── Readers  → gStreamMgmt.vgroupMap (keyed by vgId)
       ├── Trigger  → gStreamMgmt.taskMap (keyed by streamId+taskId)
       └── Runners  → gStreamMgmt.taskMap

3. MNode sends STREAM_MSG_START → Trigger activates
   └─► smStartStreamTasks() → stTriggerTaskExecute()  [MAIN LOOP]
       ├── stTriggerTaskGetTimeWindow() — has window closed?
       ├── Send SSTriggerPullRequest to Readers
       ├── Receive data → stVtableMergerNextDataBlock() (k-way merge sort)
       ├── Check trigger condition (period/sliding/session/count/event)
       ├── stTriggerTaskAcquireRequest() — grab idle Runner slot
       └── Dispatch SSTriggerCalcRequest to Runner

4. Reader (on VNode) responds to pull
   └─► tsdReaderOpen() + tsdReaderNext()
       ├── Scan WAL (real-time) or TSDB (historical backfill)
       ├── Apply triggerScanPlan (pre-filter, partition cols, trigger cols)
       └── Send SSDataBlock back to Trigger

5. Runner receives calc request
   └─► stRunnerTaskExecute(pTask, pReq)
       ├── Pop executor from execMgr.pFreeExecs (or build new)
       ├── streamExecuteTask() — run the calcPlan (aggregation/transform)
       ├── stRunnerOutputBlock() — collect results
       ├── dsPutDataBlock() — write to destination table via data sink
       │   ├── dataSinkCache (in-memory buffering)
       │   └── dataSinkFile (disk spill on memory pressure)
       └── (optional) streamDoNotification() for NOTIFY clause

6. Heartbeat cycle (every 600ms)
   └─► streamHbStart()
       ├── stmBuildHbStreamsStatusReq() — collect all local task statuses
       ├── Send SStreamHbMsg to MNode
       └── MNode responds → streamHbProcessRspMsg()
           ├── smDeployStreams() — new deployments
           ├── smUndeployTasks() — tear down
           └── Redeploy on node failure (STREAM_FLAG_REDEPLOY_RUNNER)
```

### Key Constants

```c
#define STREAM_HB_INTERVAL_MS       600       // heartbeat every 600ms
#define STREAM_MAX_GROUP_NUM        5         // max concurrent stream groups
#define STREAM_MAX_THREAD_NUM       5         // max threads per group
#define STREAM_RETURN_ROWS_NUM      4096      // rows per output block
#define STREAM_RETURN_BLOCK_NUM     4096      // max output blocks
#define STREAM_CALC_REQ_MAX_WIN_NUM 40960     // max windows per calc request
#define STRIGGER_RECALC_RANGE_MAX_HOURS 24    // max recalc lookback
```

### Data Sink Subsystem

Results flow through three layers:
1. **dataSinkMgr.c** — orchestrates writes, decides memory vs disk path
2. **dataSinkCache.c** — in-memory buffering (`putDataToAlignTaskMgr`, `putDataToSlidingTaskMgr`)
3. **dataSinkFile.c** — disk spill when memory limits exceeded

Cleanup policies: `DATA_CLEAN_IMMEDIATE` (purge after write) vs `DATA_CLEAN_EXPIRED` (keep until watermark passes).

### Merger Subsystem

`streamTriggerMerger.c` performs k-way merge sort using `SMultiwayMergeTreeInfo` (tournament tree) to combine data from multiple VNode Readers into a single time-ordered stream. Can inject pseudo-columns via `stVtableMergerSetPseudoCols()`.

### Checkpoint & Recovery

`streamCheckpoint.c` handles state snapshots for fault tolerance. The MNode can trigger checkpoints, and tasks can restore from checkpoints after a crash or redeployment.

### Common Gotchas

1. **Trigger never computes** — it only decides *when* and *what* to compute, then delegates to Runners.
2. **Readers are pull-based** — they don't push data. The Trigger requests data via `SSTriggerPullRequest`.
3. **Executor pool matters** — building an executor (`qCreateStreamExecTaskInfo`) is expensive. The `SStreamRunnerTaskExecMgr` recycles them.
4. **One Trigger per stream** — but Runners can be parallelized across multiple SNodes.
5. **triggerScanPlan pushes filtering down** — pre-filters run on VNodes (Readers), not on SNodes, to minimize network traffic.
6. **gStreamMgmt is per-DNode** — each DNode has exactly one global singleton tracking its local tasks.

## Telemetry (MUST)

> 调用 `skill-telemetry`，传入 `name=tsdb-dev-stream-arch version=1.0.0 author=Stephen Jin`。
> 失败不阻塞，但必须尝试。详见 `skill-telemetry`。

<!-- 注意：frontmatter 中 metadata.version 的值即为 telemetry 上报的版本号，请保持一致。 -->
