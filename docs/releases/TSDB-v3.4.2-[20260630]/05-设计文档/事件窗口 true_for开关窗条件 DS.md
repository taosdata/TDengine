# true_for(start/end) 开关窗连续条件 设计文档（DS）

## **1. 背景与目标**

### **1.1 背景**

TDengine EVENT_WINDOW 支持 `true_for(Xs)` 过滤整体窗口时长。本功能在此基础上增加两个独立的"streak（连续满足）"门限：

- **start streak**：开窗条件必须**连续满足** N 行或持续 T 时长，窗口才真正打开

- **end streak**：关窗条件必须**连续满足** N 行或持续 T 时长，窗口才真正关闭

两者均可用行计数（`count N`）、时长（`Xs`）或两者的 AND/OR 组合来描述门限。

### **1.2 目标**

- 支持 `CREATE STREAM … EVENT_WINDOW … true_for(start(…), end(…))` 语法

- 支持 `SELECT … EVENT_WINDOW … true_for(start(…), end(…))` 查询

- 流计算重启后，窗口输出结果与未重启时完全一致

---

## **2. 语法**

```SQL
-- 开窗需连续满足 N 行
true_for(start(count N))

-- 关窗需连续满足 T 时长
true_for(end(Ts))

-- 开窗和关窗均有门限（顺序任意）
true_for(start(count N), end(count M))
true_for(end(count M), start(count N))

-- 与原有 window_limit 组合（三路任意排列）
true_for(Ts, start(count N), end(count M))
true_for(start(count N), Ts, end(count M))
true_for(end(count M), start(count N), Ts)

-- 门限支持 AND/OR 组合
true_for(start(2s AND count 3))
true_for(end(2s OR count 2))
```



**约束**：

- `start(…)` / `end(…)` 在 sub-event 窗口（`START WITH (cond1, cond2, …)`）中禁止使用，解析期报错 `TSDB_CODE_STREAM_INVALID_TRIGGER`

- 三类参数（`window_limit` / `start_limit` / `end_limit`）各自最多出现一次，顺序任意

---

## **3. 语义**

### **3.1 skey 语义**

有 `start(…)` 时，窗口的开始时间戳（skey）为 **streak 首行的时间戳**，而非达到门限的那一行。streak 积累过程中，原本符合开窗条件的行被"预热"，全部包含在窗口内。

```Plain Text
行:    T1(start)  T2(start)  T3(end)
streak=1          streak=2→满足
→ 窗口 skey=T1（首行），包含 T1、T2、T3
```

### **3.2 ekey 语义**

有 `end(…)` 时，窗口的结束时间戳（ekey）为 **streak 首行的时间戳**，而非达到门限的那一行。

```Plain Text
行:    T1(start)  T2(end1)  T3(end2→满足)
→ 窗口 ekey=T2（首行），窗口聚合包含 T1、T2
```

### **3.3 streak 中断**

streak 积累过程中，如果条件行断开（出现一行不满足条件），当前计数**重置为 0**，从下一次满足条件时重新开始。

### **3.4 AND / OR 组合**

- **AND**：时长和行数**同时**满足时 streak 触发

- **OR**：时长和行数**任一**先满足时 streak 触发

---

## **4. 架构概览**

```Plain Text
SQL 文本
  ↓  Parser → AST → Planner（约 20 个文件，从 SQL 文本到物理计划）
SEventWindowPhysiNode.{startTrueForType/Count/Duration, endTrueForType/Count/Duration}
  ↓  ┌──────────────────────┬────────────────────────────┐
     │ 查询路径（SELECT）    │ 流计算路径（CREATE STREAM）  │
     ↓                      ↓
SEventWindowInfo           SSTriggerTask
（streak 状态局部变量）     （streak 状态 + firstVer）
```

---

## **5. 执行层实现**

### **5.1 共用：isTrueForSatisfied()**

所有 streak 判断的核心函数，两条路径（查询、流计算）共用：

```C
bool isTrueForSatisfied(const STrueForInfo *pInfo,
                        TSKEY firstTs, TSKEY currentTs, int32_t count);
```

- 类型为 `COUNT_ONLY`：`count >= pInfo->count`

- 类型为 `DURATION_ONLY`：`currentTs - firstTs >= pInfo->duration`

- 类型为 `AND`：时长和行数同时满足

- 类型为 `OR`：时长或行数任一满足

- `count == 0 && duration == 0`：直接返回 true（向后兼容短路）

---

### **5.2 查询执行层（SELECT EVENT_WINDOW）**

**文件**：`eventwindowoperator.c`

每个 `SEventWindowInfo` 携带 streak 运行时状态（4 个字段）：

```C
int32_t  startCondCount;    // start streak 当前行数
int64_t  startCondFirstTs;  // streak 首行时间戳（INT64_MIN = 无 streak）
int32_t  endCondCount;
int64_t  endCondFirstTs;
```

**开窗逻辑**（`doEventWindowImpl()` 内，窗口为 NULL 时）：

1. 当前行满足 `START WITH` 条件：

    - 若为 streak 首行（count == 0）：记录 `startCondFirstTs = ts[i]`

    - `startCondCount++`

    - 调用 `isTrueForSatisfied()` 检查是否达到门限

- **达到**：以 `startCondFirstTs` 作为窗口 skey 打开新窗口，重置 streak（count=0, firstTs=INT64_MIN）

- **未达到**：继续等待，窗口不开

2. 当前行**不满足** `START WITH` 条件：streak 中断，重置 count=0, firstTs=INT64_MIN

**关窗逻辑**（窗口已开启，处理每一行）：

1. 当前行满足 `END WITH` 条件：

    - 若为 streak 首行（count == 0）：记录 `endCondFirstTs = ts[i]`

    - `endCondCount++`

    - 调用 `isTrueForSatisfied()` 检查是否达到门限

- **达到**：以 `endCondFirstTs` 覆盖 ekey，关闭窗口，重置 streak

- **未达到**：继续等待，窗口保持开启，行继续累积

2. 当前行**不满足** `END WITH` 条件：end streak 中断，重置 count=0, firstTs=INT64_MIN，窗口保持开启

查询路径为同步一次性执行，streak 状态随 operator 生命周期存在，无需持久化。

---

### **5.3 流计算触发层（CREATE STREAM）**

**文件**：`streamTriggerTask.c / .h`

#### **5.3.1 运行时状态（per-group）**

每个 `SSTriggerRealtimeGroup` 在 event-window union 中携带 6 个 streak 状态字段：

```C
int32_t startCondCount;      // start streak 当前行数（0 = 无 streak）
TSKEY   startCondFirstTs;    // streak 首行时间戳（INT64_MIN = 无 streak）
int64_t startCondFirstVer;   // streak 首行所在 batch 的最小 WAL 版本（INT64_MAX = 无 streak）
int32_t endCondCount;
TSKEY   endCondFirstTs;
int64_t endCondFirstVer;
```

初始化时：`startCondFirstVer = endCondFirstVer = INT64_MAX`（sentinel 表示无进行中 streak）。

#### **5.3.2 主处理循环：stRealtimeGroupDoEventCheck()**

流计算的逐行处理入口。每个 batch 处理时，对 `[startIdx, endIdx)` 范围内的每一行执行以下逻辑：

**① 窗口未开启（pWin == NULL）+ 当前行满足 START WITH：**

```Plain Text
if (startTrueForInfo.count > 0 || startTrueForInfo.duration > 0):
    if startCondCount == 0:
        startCondFirstTs  = ts[i]
        startCondFirstVer = stGroupGetBatchMinVer(pGroup, vgId)  // ← WAL 版本记录
    startCondCount++
    if isTrueForSatisfied(startTrueForInfo, startCondFirstTs, ts[i], startCondCount):
        _skey = startCondFirstTs    // 窗口开始时间取 streak 首行
        startCondCount    = 0
        startCondFirstTs  = INT64_MIN
        startCondFirstVer = INT64_MAX
        → 以 _skey 打开新窗口 (pWin->range.skey = _skey)
    else:
        → 等待，窗口不开
else (无 start streak):
    → 直接以 ts[i] 打开窗口（原有行为）
```

**② 窗口未开启 + 当前行不满足 START WITH：**

```Plain Text
startCondCount    = 0
startCondFirstTs  = INT64_MIN
startCondFirstVer = INT64_MAX
```

**③ 窗口已开启（pWin != NULL）：**

每行先更新窗口的 `wrownum` 和 `range.ekey`（带 `TRIGGER_GROUP_UNCLOSED_WINDOW_MASK` 标记，表示窗口未关闭）。

- **当前行满足 END WITH**：

```Plain Text
if (endTrueForInfo.count > 0 || endTrueForInfo.duration > 0):
    if endCondCount == 0:
        endCondFirstTs  = ts[i]
        endCondFirstVer = stGroupGetBatchMinVer(pGroup, vgId)
    endCondCount++
    if isTrueForSatisfied(endTrueForInfo, endCondFirstTs, ts[i], endCondCount):
        _ekey = endCondFirstTs    // 窗口结束时间取 streak 首行
        _closeNow = true
        endCondCount    = 0
        endCondFirstTs  = INT64_MIN
        endCondFirstVer = INT64_MAX
    else:
        → 继续等待，行计入窗口
else:
    → 直接关窗（_ekey = ts[i]，_closeNow = true）
```

- **当前行不满足 END WITH**：end streak 中断，重置 count=0, firstTs=INT64_MIN, firstVer=INT64_MAX，窗口保持开启

- `_closeNow == true` 时：设置 `pWin->range.ekey = _ekey`，发出 WINDOW_CLOSE 通知，pWin = NULL

#### **5.3.3 stGroupGetBatchMinVer()**

```C
static int64_t stGroupGetBatchMinVer(SSTriggerRealtimeGroup *pGroup, int32_t vgId) {
    // 遍历当前 batch 该 vgId 的所有 meta 条目，取最小 WAL 版本
}
```

取 **batch 最小版本**而非单行版本的原因：TDengine WAL 单位为 block，一个 block 内多行共享同一 `ver`，需以最小值确保回退时不跳过同 block 的前序行。

#### 5.3.4 Checkpoint：WAL 回退策略

**核心设计**：进行中的 streak 状态**不写入 checkpoint**。streak 回退被集成进 `doneVer` 的通用推进机制，checkpoint 直接使用已正确维护的 `doneVer`，无需额外处理。

其他流：条件满足 = 立即开窗 = 立即有计算请求，三者同步发生，calcParamPool 天然托管了整个生命周期。

streak：条件满足 ≠ 立即开窗。开窗之前有一段"预热积累期"——条件持续满足但窗口还没开，这段时间既无计算请求，也无任何系统级标记，完全依赖 startCondCount > 0 这个业务状态来感知。

**`doneVer` 推进阶段（`stRealtimeContextCheck()` 末尾）**

`doneVer` 只在所有飞行中计算请求全部完成时（`calcParamPool.size == 0 && nRunningReq == 0`）才被推进，此时 group 状态完全稳定。推进逻辑如下：

```Plain Text
for each vgId, progress in pReaderWalProgress:
    if forwardDoneVer:
        newDoneVer = progress.lastScanVer
        if triggerType == STREAM_TRIGGER_EVENT:
            minStreakVer = INT64_MAX
            for each group in pGroups where group.vgId == vgId:
                if group.startCondCount > 0 and group.startCondFirstVer != INT64_MAX:
                    minStreakVer = min(minStreakVer, group.startCondFirstVer)
                if group.endCondCount > 0 and group.endCondFirstVer != INT64_MAX:
                    minStreakVer = min(minStreakVer, group.endCondFirstVer)
            if minStreakVer != INT64_MAX:
                newDoneVer = min(newDoneVer, minStreakVer - 1)  // 上限：streak 首行之前
        progress.doneVer = newDoneVer
```

**Checkpoint 写入阶段（`stTriggerTaskDoGenCheckpoint()`）**

`doneVer` 已在推进时反映了 streak 回退，checkpoint 直接使用，无需特殊处理：

关键点：

- streak 回退是 `doneVer` 推进的内置约束，而非 checkpoint 时的事后修正

- `doneVer` 只在有进行中 streak 时才被上限限制，无 streak 的 vnode 不受影响

- checkpoint 文件**无新增字节**，checkpoint 体积不变

#### **5.3.5 重启恢复流程**

1. 流任务从 checkpoint 读取各 vnode 的 `doneVer`（已被回退的版本）

2. WAL 从 `doneVer + 1` 开始重放

3. streak 首行重新进入处理循环，`startCondCount` / `endCondCount` 从 0 重新积累

4. 后续行在相同条件下依次满足，最终在与首次处理相同的行上达到 streak 门限

5. 窗口以相同的 skey / ekey 打开或关闭，聚合结果与未重启时完全一致

**重放开销**：有进行中 streak 的 vnode 仅多回放 streak 积累的若干行（通常个位数）。

---

## **6. 关键数据结构**

### **6.1 STrueForInfo（共用）**

```C
typedef struct STrueForInfo {
    ETrueForType trueForType;  // DURATION_ONLY / COUNT_ONLY / AND / OR
    int32_t      count;        // 行计数门限（0 = 不使用）
    int64_t      duration;     // 时长门限，纳秒（0 = 不使用）
} STrueForInfo;
```

### **6.2 SSTriggerRealtimeGroup（event union 部分，流计算）**

```C
// event-window streak 状态（6 字段）
int32_t startCondCount;      // 当前连续满足 start 条件的行数
TSKEY   startCondFirstTs;    // streak 首行时间戳（INT64_MIN = 无 streak）
int64_t startCondFirstVer;   // streak 首行 batch 最小 WAL 版本（INT64_MAX = 无 streak）
int32_t endCondCount;
TSKEY   endCondFirstTs;
int64_t endCondFirstVer;
```

---

## **7. 兼容性**

### **7.1 向后兼容（默认值为 0）**

未指定 `start(…)` / `end(…)` 时，六个字段默认为 0：

- `startTrueForInfo.count == 0 && duration == 0`：streak 判断短路为直接通过

- 行为与原有 EVENT_WINDOW 完全一致

### **7.2 Checkpoint 格式兼容**

Checkpoint format version 保持 v4，无新增字节。旧版本（不含 streak 字段）的 checkpoint 在新代码下正常读取，缺失字段默认 0。

### **7.3 滚动升级**

Stream 消息中新字段采用可选读取，旧版本节点忽略未知字段；新版本节点对缺失字段默认 0。混部期间行为与旧版本一致，完成升级后再创建使用 start/end streak 的流。

---

## **8. 约束与限制**

|约束|说明|
|---|---|
|sub-event 不支持 start/end|`START WITH (cond1, cond2, …)` 窗口中不能指定 `start(…)` / `end(…)`，解析期报错|
|三路参数各最多一次|`window_limit` / `start_limit` / `end_limit` 各最多出现一次|
|count 参数必须为正整数|count ≤ 0 报错 `TSDB_CODE_PAR_INVALID_TRUE_FOR_COUNT`|
|duration 不能为负|报错 `TSDB_CODE_PAR_TRUE_FOR_NEGATIVE`|
|不支持 ALTER STREAM 修改|需删除并重建流|

---

## **9. 改动文件汇总**

|层次|文件|改动内容|
|---|---|---|
|Parser / AST / Planner / Stream 消息|约 20 个文件|语法扩展、AST 节点新字段、序列化/反序列化、计划传递|
|查询执行|`executorInt.h` / `eventwindowoperator.c`|查询侧 streak 积累与判断逻辑|
|流执行|`streamTriggerTask.h` / `streamTriggerTask.c`|运行时 streak 状态、WAL 版本追踪、checkpoint WAL 回退|
|测试|`test_truefor.py`|s1-s11、o1-o9、q1-q5、r1 共 26 个测试用例|



