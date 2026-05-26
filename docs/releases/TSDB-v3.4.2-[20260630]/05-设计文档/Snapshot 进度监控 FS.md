# Snapshot 进度监控 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-05-12 | — | 0.1 | 鲍之骁 | 初稿 |
| 2026-05-13 | 2026-5-14 | 1.0 | 鲍之骁 | 根据 review 意见修改 |

---

## 2. 背景

当 Raft follower 节点因长时间离线或数据严重损坏而无法通过 WAL 回放追上 leader 时，sync 层会触发 snapshot 复制流程（包括 RAW 全量传输和 ROW 增量传输两种方式）。整个过程可能持续数分钟乃至更长时间，但此前没有任何可观测手段：用户无法得知传输进度、是否卡住、卡在哪个文件集。

本特性在 `information_schema` 下新增两张只读系统表：

- `ins_snap_send_vnodes`：每个正在进行 snapshot 发送的 vnode 对应一行，反映 vnode 级整体进度。
- `ins_snap_send_filesets`：对应 vnode 内每个 fileset（时间分片）的发送进度，提供更细粒度的可观测性。

---

## 3. 定义

| 术语 | 说明 |
|------|------|
| **Snapshot 复制** | Raft 机制中当 follower 落后超出 WAL 保留范围时，leader 将当前存储快照（TSDB 文件）整体传输给 follower 的过程 |
| **RAW 传输** | 全量传输模式（`SNAP_DATA_RAW=14`），直接按物理文件块传输，`sver=0` |
| **ROW 传输** | 增量传输模式（`SNAP_DATA_TSDB=2`），按行解码再重新压缩传输，`sver>0` |
| **Fileset** | TSDB 按时间分片的文件组，由一个 `fid`（时间分片 ID）唯一标识，包含 HEAD/DATA/SMA/STT 等类型文件 |
| **STT 文件** | Sorted Timestamp Table，属于 fileset 的 STT level 文件，数量可为多个 |
| **Leader** | Raft 组中负责写入和数据同步的主节点 |
| **snapRestoring** | `SVgObj` 中新增的标志位（`int8_t`），值 1 表示该 vgroup 的 leader 正在进行 snapshot 传输；该字段为临时状态，SDB 编解码时始终置 0，重启后重新采集 |
| **mnode 定时拉取** | mnode 定期（由 `tsSnapSendPullupInterval` 控制，默认 10 s）轮询各 leader dnode 获取实时进度并缓存到内存哈希表 |
| **VGROUP_VER_NUMBER** | SDB vgroup 序列化版本号，因新增 `snapRestoring` 字段，由 2 升为 3 |

---

## 4. 行为说明

### 4.1 新增系统表

#### `ins_snap_send_vnodes`

查询正在进行 snapshot 发送的 vnode 列表及整体进度。

```sql
-- 查看所有正在传输 snapshot 的 vnode
SELECT * FROM information_schema.ins_snap_send_vnodes;

-- 找出已持续超过 10 分钟的传输（可能卡住）
SELECT * FROM information_schema.ins_snap_send_vnodes
WHERE elapsed > '0:10:00';
```

| 列名 | 类型 | 说明 |
|------|------|------|
| `vgroup_id` | INT | vnode 所属 vgroup ID |
| `dnode_id` | INT | leader 所在 dnode ID |
| `total_file_sets` | INT | 本次发送需传输的 fileset 总数 |
| `finished_file_sets` | INT | 已完整传输完成的 fileset 数量 |
| `start_time` | TIMESTAMP | snapshot reader 打开时间（毫秒精度） |
| `elapsed` | VARCHAR(32) | 持续时长，格式 `H:MM:SS`，由 mnode 查询时实时计算 |

示例输出：

```
 vgroup_id | dnode_id | total_file_sets | finished_file_sets |       start_time        | elapsed
-----------+----------+-----------------+--------------------+-------------------------+---------
         2 |        1 |              12 |                  5 | 2026-05-12 10:30:00.000 | 0:03:21
```

---

#### `ins_snap_send_filesets`

查询当前活跃 snapshot 发送中每个 fileset 的传输进度。

```sql
-- 查看所有 fileset 进度（按 vgroup 和 fid 排序）
SELECT * FROM information_schema.ins_snap_send_filesets
ORDER BY vgroup_id, fid;

-- 查看指定 vgroup 的 fileset 详情
SELECT * FROM information_schema.ins_snap_send_filesets
WHERE vgroup_id = 2
ORDER BY fid;
```

| 列名 | 类型 | 说明 |
|------|------|------|
| `vgroup_id` | INT | vnode 所属 vgroup ID |
| `fid` | INT | fileset ID（时间分片 ID） |
| `file_count` | INT | 该 fileset 包含的物理文件总数（HEAD/DATA/SMA/STT 各类型累计，于 reader 打开时统计） |
| `finished_file_count` | INT | 已完整传输的物理文件数（见 §4.4 语义说明） |
| `total_size` | BIGINT | 该 fileset 所有物理文件大小之和（bytes，来自磁盘元数据） |
| `read_size` | BIGINT | 已从磁盘读取并编码发送的字节数（单调递增；RAW path 与 `total_size` 同单位，ROW path 为重压缩后大小） |
| `start_time` | TIMESTAMP | 开始传输该 fileset 的时间（毫秒精度）；reader 尚未开始时为 0 |
| `elapsed` | VARCHAR(32) | 该 fileset 已耗时，格式 `H:MM:SS`；`start_time` 为 0 时显示 `0:00:00` |
| `start_index` | BIGINT | 该 fileset 的起始 version（`sver`） |
| `end_index` | BIGINT | 该 fileset 的结束 version（`ever`） |
| `transfer_type` | VARCHAR(4) | `"raw"`（RAW 全量）或 `"row"`（ROW 增量） |

示例输出：

```
 vgroup_id | fid | file_count | finished_file_count | total_size | read_size |       start_time        | elapsed | start_index | end_index | transfer_type
-----------+-----+------------+---------------------+------------+-----------+-------------------------+---------+-------------+-----------+---------------
         2 |   1 |          5 |                   3 | 1073741824 | 671088640 | 2026-05-12 10:30:00.000 | 0:01:02 |           0 |      9999 | raw
         2 |   2 |          3 |                   0 |  524288000 |         0 | 2026-05-12 10:31:03.000 | 0:00:05 |           0 |      9999 | raw
```

---

### 4.2 新增配置项

| 配置项 | 类型 | 默认值 | 有效范围 | 说明 |
|--------|------|--------|---------|------|
| `tsSnapSendPullupInterval` | int | `10` | `[1, 10000]` | mnode 定时拉取 snapshot 发送进度的间隔秒数。值越小实时性越高，但会增加 mnode 与 dnode 之间的 RPC 频率。 |

配置方式：在 `taos.cfg` 中添加：

```ini
tsSnapSendPullupInterval 10
```

---

### 4.3 数据生命周期

- snapshot 发送开始（leader 侧 tsdb snapshot reader 打开）时，相应行出现在系统表中。
- snapshot 发送结束（reader 正常关闭或异常中止均触发清理）后，对应行从系统表消失（mnode 下次拉取时若 dnode 不再上报则移除缓存）。
- 系统表中的进度数据最多落后当前实际状态 `tsSnapSendPullupInterval` 秒（默认约 10 s）。
- `elapsed` 列由 mnode 查询时实时计算（`now - start_time`），不受采集延迟影响。

---

### 4.4 进度字段语义

**`finished_file_count` 的更新时机因传输模式而异：**

| 传输模式 | 更新方式 |
|---------|---------|
| RAW（全量） | 每完成一个物理文件传输，`finished_file_count` 立即递增，实时反映单文件粒度的完成情况 |
| ROW（增量） | 整个 fileset 的所有文件合并为一个流处理，仅在整个 fileset 传输完成时 `finished_file_count` 才由 0 跳变为 `file_count` |

**`read_size` 与 `total_size` 的可比性：**

| 传输模式 | 说明 |
|---------|------|
| RAW（全量） | 两者均为物理字节，可直接相除计算百分比进度 |
| ROW（增量） | `read_size` 为重压缩后字节，`total_size` 为原始压缩字节，单位不严格对齐；`read_size` 仅作单调增长趋势参考，不适合用于精确进度百分比计算 |

---

### 4.5 访问权限

与其他 `information_schema` 系统表一致：`sysInfo=false`，所有已登录用户均可查询。

---

### 4.6 错误处理

- mnode 向 dnode 发起进度拉取时，若 dnode 返回错误（如节点离线、消息类型不识别），mnode 将忽略本次错误并等待下次定时拉取。
- dnode 侧处理拉取请求时，若内存分配（`taosArrayPush`）失败（OOM），则立即返回 `TSDB_CODE_OUT_OF_MEMORY`，不返回部分数据，mnode 侧将在下次拉取时重试。
- `elapsed` 列在 `start_time` 为 0 或未初始化时显示 `"0:00:00"`，不显示异常大的时间戳。

---

## 5. 性能

- mnode 每 `tsSnapSendPullupInterval` 秒（默认 10 s）向有活跃 snapshot 的 leader dnode 发送一次轻量 RPC 请求（`TDMT_DND_QUERY_SNAP_SEND_PROGRESS`）。通常 snapshot 并发数极少（0～3），额外开销可忽略。
- tsdb 层进度统计在已有的读写锁（`tsdb->snapStatLock`）保护下执行：每次 chunk 读取后对整数字段加写锁更新，开销为一次 rwlock 加锁 + 整数累加，对 snapshot 传输吞吐量无可量测影响。
- 系统表查询时从 mnode 内存哈希表读取，不触发额外 RPC，查询性能与其他内存级系统表一致。

---

## 6. 安全

- 系统表为只读视图，不接受写入操作。
- 进度数据仅包含文件大小、时间戳、版本号等元数据，不含任何用户数据内容。
- 遵循 TDengine 现有 `information_schema` 的访问控制机制。

---

## 7. 兼容性

- `SVgObj` 中新增 `snapRestoring` 字段（`int8_t`），对应将 `VGROUP_VER_NUMBER` 从 2 升为 3。mnode 重启后从 SDB 解码时始终将 `snapRestoring` 置 0（视为临时状态），不影响历史数据读取。
- `mndVgroupActionUpdate()` 中 `snapRestoring` 需从 `pOld` 拷贝至 `pNew`（方向为 `pNew->snapRestoring = pOld->snapRestoring`），以确保 SDB 更新事件不会意外清零内存中的活跃 snapshot 状态。
- 新增消息类型 `TDMT_DND_QUERY_SNAP_SEND_PROGRESS` 及对应 RSP。旧版本 dnode 收到未知消息类型时按协议返回错误，mnode 侧忽略错误并在下次定时任务时重试，不影响集群功能。
- `syncSnapshotRecving()` 已从 `#ifdef BUILD_NO_CALL` 宏保护中移出，使其在所有构建类型下均可链接，与 `syncSnapshotSending()` 保持一致。
- 本特性不修改任何现有 SQL 语法、现有 API 或现有配置项，对现有功能无破坏性影响。

---

## 8. 运维

执行 `restore dnode` 或等待 follower 从 snapshot 恢复时，可通过以下 SQL 实时监控进度：

```sql
-- vnode 级：查看每个正在传输 snapshot 的 vnode 的整体进度
SELECT * FROM information_schema.ins_snap_send_vnodes;

-- fileset 级：查看指定 vgroup 中每个时间分片的文件传输详情
SELECT * FROM information_schema.ins_snap_send_filesets
WHERE vgroup_id = <vgroup_id>
ORDER BY fid;
```

**判断是否卡住：**
- 若 `elapsed` 持续增长但 `read_size` 无变化，说明对应 fileset 的传输已卡住。
- 若 `finished_file_sets` 长时间不增加，结合 `read_size` 变化趋势可判断整体是否停滞。

**leader 切换：** 若 leader 发生切换，新 leader 将重新开始 snapshot 发送，系统表自动反映新进度，旧缓存在下次拉取后被覆盖。

---

## 9. 使用场景

1. **日常运维监控**：用户发现某 vnode 长时间处于 `OFFLINE` 状态，通过查询 `ins_snap_send_vnodes` 确认是否正在 snapshot 恢复，以及大致还需多久完成。

2. **卡死诊断**：通过对比两次查询 `read_size` 是否有增长，判断传输是否卡住；结合 `fid` 定位到具体的时间分片文件，进一步排查该分片数据量异常或磁盘 I/O 问题。

3. **进度估算**：通过 `total_size` 和传输速率（RAW path：`read_size` 增量 / 时间）估算 snapshot 恢复完成时间，辅助制定维护窗口计划。

4. **告警集成**：监控平台通过定期轮询 `ins_snap_send_vnodes`，当 `elapsed` 超过阈值时触发告警，减少人工巡检负担。

5. **restore dnode 进度追踪**：在执行 `restore dnode <id>` 命令后，通过查询这两张系统表观察各 vnode 的 snapshot 同步进度，判断恢复是否完成。

---

## 10. 约束和限制

**约束：**
- 本特性仅统计 leader 侧的发送进度，不反映 follower 写入端的进度状态。
- 只有 TSDB 类型的 vnode 才会产生 snapshot 发送进度数据；META、TQ 等其他存储类型的进度暂不覆盖。
- 进度数据有最多 `tsSnapSendPullupInterval` 秒（默认 10 s）的采集延迟；`elapsed` 列不受此延迟影响（实时计算）。
- TDengine sync 层保证同一 vnode 在任意时刻最多只有一个活跃的 snapshot send（发送串行化），因此每个 tsdb 实例只需一个全局的 `pSnapStat` 指针，不需要引用计数或每-reader 状态。

**限制：**
- `elapsed` 精度为秒，小于 1 秒的传输不体现为有效时长。
- ROW path 下 `read_size` 与 `total_size` 不严格可比，不适合用于精确进度百分比计算，仅供趋势参考。
- ROW path 下 `finished_file_count` 在整个 fileset 完成前始终为 0，无法反映 fileset 内单文件粒度的进度。

---

## 11. 常见错误和排查

| 现象 | 可能原因 | 排查方法 |
|------|----------|----------|
| 系统表始终为空 | 当前无 snapshot 发送；或 mnode 定时任务未启动 | 检查 `SVgObj.snapRestoring` 是否被置 1；查看 mnode 日志中 `snap-send-progress` 关键词 |
| `elapsed` 持续增长但 `read_size` 不变 | snapshot 传输卡住（网络或磁盘 I/O 阻塞） | 检查 dnode 间网络连通性、磁盘 I/O 状态；查看 dnode 日志中 sync 层相关错误 |
| 系统表中行突然消失 | snapshot 发送正常完成，或发送失败被中止 | 查看 sync 日志中 `snapshotSenderStop` 判断是正常结束还是异常中止 |
| ROW path 下 `finished_file_count` 长时间为 0 | 属正常现象：ROW path 仅在整个 fileset 完成后才将 `finished_file_count` 从 0 跳变为 `file_count` | 无需处理，观察 `read_size` 是否单调递增 |
| `elapsed` 显示 `0:00:00` 但 `start_time` 不为空 | reader 已打开但对应 fileset 尚未开始传输（`start_time` 字段为 0） | 正常状态，等待传输开始后 `elapsed` 会自动更新 |

---

## 12. 可观测性

- **taos shell**：直接执行两张系统表的 `SELECT` 查询即可，无需额外工具。
- **TDinsight / taos Explorer**：本期不做专项可视化支持，用户可在 SQL 面板中自行查询；后续可根据需求添加专项监控面板。
- **日志**：mnode 在每次成功拉取到进度数据时输出 `DEBUG` 级别日志（关键词 `snap-send-progress`），便于诊断拉取链路是否正常运行。

---

## 13. 安装和卸载

- 本特性为纯软件功能，随版本正常升级即可生效，不新增外部依赖，不修改安装包文件结构。
- 降级时需注意：旧版本不认识 `VGROUP_VER_NUMBER=3` 的 SDB 数据，降级前应在 mnode 正常状态下进行（参见兼容性章节）。
- `.claude/settings.local.json`（本地 AI 工具配置文件）已加入 `.gitignore`，不随仓库分发。

---

## 14. 文档

- **已完成**：
  - 企业版「运维」文档（`docs/zh/08-operation/04-maintenance.md`）：在「恢复数据节点」章节下新增「监控 Snapshot 发送进度」子章节，提供两张系统表的查询示例。
  - 英文版对应文档（`docs/en/08-operation/04-maintenance.md`）：同步更新。
  - 元数据参考文档（`docs/zh/14-reference/03-taos-sql/22-meta.md`）：新增两张系统表的列定义说明。
  - 英文元数据参考文档（`docs/en/14-reference/03-taos-sql/22-meta.md`）：同步更新。
- **待处理**：文档 PR 需在版本发布前提交并通过 Wade Review & Merge。

---

## 15. 参考文档

---

## 16. 附录

### A. 进度采集数据链路

```
mnode 定时器（每 tsSnapSendPullupInterval 秒）
  └─ 扫描 SDB，找 snapRestoring=1 的 SVgObj
       └─ 向对应 leader dnode 发送 TDMT_DND_QUERY_SNAP_SEND_PROGRESS
            └─ dnode 遍历本机 vnode
                 └─ 调用 vnodeGetSnapSendProgress()
                      └─ 加读锁读取 tsdb->pSnapStat，深拷贝为 SSnapSendVnodeInfo
                 └─ 构建 SDnodeQuerySnapSendProgressRsp 并序列化返回
            └─ mnode 收到 RSP，反序列化后更新内存哈希表（key=vgId）
  └─ 系统表查询时从内存哈希表读取，实时计算 elapsed
```

### B. 关键数据结构

```c
/* tsdb.h — leader tsdb 侧进度结构（内部使用）*/
typedef struct {
  int32_t fid;
  int32_t fileCount;          // 该 fileset 物理文件总数（打开 reader 时统计）
  int32_t finishedFileCount;  // 已完成传输的物理文件数
  int64_t totalSize;          // 磁盘物理文件大小总和（bytes）
  int64_t readSize;           // 已读取并发送的字节数（单调递增）
  int64_t startTime;          // 开始传输时间（ms），未开始时为 0
  int64_t sver;               // 起始 version
  int64_t ever;               // 结束 version
  int8_t  transferType;       // SNAP_DATA_RAW(14) 或 SNAP_DATA_TSDB(2)
} SSnapSendFileSetStat;

typedef struct {
  int32_t             totalFileSets;    // 总 fileset 数
  int32_t             finishedFileSets; // 已完成 fileset 数
  int64_t             startTime;        // reader 打开时间（ms）
  SSnapSendFileSetStat *pFileSetStats;  // 各 fileset 进度数组
} SSnapSendVnodeStat;

/* tmsg.h — 跨进程传输结构（mnode ↔ dnode RPC）*/
typedef struct {
  int32_t fid;
  int32_t fileCount;
  int32_t finishedFileCount;
  int64_t totalSize;
  int64_t readSize;
  int64_t startTime;
  int64_t sver;
  int64_t ever;
  int8_t  transferType;
} SSnapSendFileSetInfo;
```

### C. 新增消息类型

```c
// include/common/tmsgdef.h
TDMT_DND_QUERY_SNAP_SEND_PROGRESS      // mnode → dnode 请求
TDMT_DND_QUERY_SNAP_SEND_PROGRESS_RSP  // dnode → mnode 响应（自动生成，= 请求 + 1）
```

### D. 涉及文件清单

| 文件 | 修改类型 | 内容摘要 |
|------|----------|---------|
| `include/common/systable.h` | 修改 | 新增 `TSDB_INS_TABLE_SNAP_SEND_VNODES`、`TSDB_INS_TABLE_SNAP_SEND_FILESETS` 常量 |
| `source/common/src/systable.c` | 修改 | 新增两张表的 schema（`elapsed` 为 VARCHAR(32)） |
| `include/common/tmsg.h` | 修改 | 新增进度结构体（`SSnapSendFileSetInfo`、`SSnapSendVnodeInfo`、`SDnodeQuerySnapSendProgressRsp`）、`EShowType` 枚举值、`SVnodeLoad.snapshotSending` |
| `include/common/tmsgdef.h` | 修改 | 新增 `TDMT_DND_QUERY_SNAP_SEND_PROGRESS` |
| `source/common/src/msg/tmsg.c` | 修改 | 新增序列化/反序列化/释放函数；`SVnodeLoad` encode/decode 扩展 |
| `include/common/tglobal.h` | 修改 | 声明 `tsSnapSendPullupInterval` |
| `source/common/src/tglobal.c` | 修改 | 注册 `tsSnapSendPullupInterval`（默认 10，范围 1–10000） |
| `source/dnode/mnode/impl/inc/mndDef.h` | 修改 | `SVgObj` 新增 `int8_t snapRestoring` |
| `source/dnode/mnode/impl/src/mndVgroup.c` | 修改 | encode/decode/update `snapRestoring`；update 方向为 `pNew->snapRestoring = pOld->snapRestoring`；`VGROUP_VER_NUMBER` 升为 3 |
| `source/dnode/mnode/impl/src/mndDnode.c` | 修改 | heartbeat 处理中将 `pVload->snapshotSending` 写入 `pVgroup->snapRestoring` |
| `source/dnode/mnode/impl/src/mndSnapSend.c` | **新增** | 定时拉取（`mndSnapSendPullup`）、进度缓存（`gSnapSendMgmt`）、系统表 retrieve 函数、响应处理 |
| `source/dnode/mnode/impl/inc/mndSnapSend.h` | **新增** | 头文件声明；注释使用 `tsSnapSendPullupInterval` |
| `source/dnode/mnode/impl/src/mndMain.c` | 修改 | 注册 mndSnapSend 初始化步骤；注册 `mndSnapSendPullup` 定时任务 |
| `source/dnode/mnode/impl/src/mndShow.c` | 修改 | 路由两张系统表的 retrieve 请求 |
| `source/dnode/vnode/src/inc/tsdb.h` | 修改 | `STsdb` 新增 `pSnapStat`、`snapStatLock`；定义 `SSnapSendFileSetStat`、`SSnapSendVnodeStat` |
| `source/dnode/vnode/src/tsdb/tsdbOpen.c` | 修改 | `tsdbOpen` 初始化 `snapStatLock`；`tsdbClose` 在 wrlock 下释放 `pSnapStat`+`pFileSetStats` 后销毁 rwlock |
| `source/dnode/vnode/src/tsdb/tsdbSnapshot.c` | 修改 | ROW path：reader 打开时填充 `pSnapStat`；每次 chunk 读取后更新 `readSize`；fileset 完成时设置 `finishedFileCount=fileCount` |
| `source/dnode/vnode/src/tsdb/tsdbSnapshotRAW.c` | 修改 | RAW path：同上，`finishedFileCount` 随物理文件完成实时递增 |
| `source/dnode/vnode/src/vnd/vnodeSnapshot.c` | 修改 | `vnodeGetSnapSendProgress()`：加读锁深拷贝进度；文档注释包含完整返回值说明 |
| `source/dnode/vnode/src/vnd/vnodeQuery.c` | 修改 | heartbeat 时调用 `syncSnapshotSending()` 填充 `pLoad->snapshotSending` |
| `source/dnode/vnode/src/inc/vnodeInt.h` | 修改 | 声明 `vnodeGetSnapSendProgress` |
| `source/dnode/mgmt/mgmt_vnode/src/vmHandle.c` | 修改 | `vmProcessDnodeQuerySnapSendProgressReq()`：遍历 vnode 采集进度，OOM 时立即 abort；`TDMT_DND_QUERY_SNAP_SEND_PROGRESS` 注册 |
| `source/dnode/mgmt/mgmt_vnode/src/vmWorker.c` | 修改 | 消息路由至 mgmt 队列 |
| `source/dnode/mgmt/mgmt_mnode/src/mmHandle.c` | 修改 | RSP 路由到 mnode read 队列 |
| `source/libs/sync/src/syncMain.c` | 修改 | `syncSnapshotSending()` 和 `syncSnapshotRecving()` 均移出 `BUILD_NO_CALL` 宏保护，所有构建类型可链接 |
| `tests/system-test/0-others/information_schema.py` | 修改 | 新增 `ins_snap_send_check()`：验证两张表存在、列数和列名正确 |
| `.gitignore` | 修改 | 新增 `.claude/` 排除规则 |
