# 概要设计说明书（Functional Spec）— 线程 CPU 亲和性管理

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-16 | 2026-04-16 | 1.0 | dmchen | 初始版本 |

## 2. 背景

在高并发、混合负载场景下，TDengine 的 taosd 服务进程中存在大量不同职能的线程（管理类、写入类、读取类），它们共享所有 CPU 核心。当写入线程密集 I/O 操作或查询线程大量计算时，线程间的 CPU 竞争和缓存污染（cache pollution）会导致关键管理操作（集群协调、元数据管理等）出现延迟抖动。

本特性通过 CPU 核心亲和性（CPU Core Affinity）机制，将 taosd 进程中的所有线程分为三类（管理、写入、读取），并将每一类线程绑定到独立的 CPU 核心集合上运行，从而实现：

- **隔离关键管理操作**：保障集群协调、元数据管理、网络收发等关键线程的计算资源
- **减少缓存污染**：同类线程共享相同 CPU 核心，提升 L1/L2 缓存命中率
- **灵活配置**：管理员可根据实际负载模式调整各类线程的 CPU 核心分配比例

本特性提供一个主开关（Master Switch）`enableCpuAffinity`，默认关闭，确保升级时无行为变化（向后兼容）。

## 3. 定义

| 术语 | 定义 |
| --- | --- |
| CPU 亲和性（CPU Affinity） | 操作系统级的线程-CPU 核心绑定机制，限制线程只在指定的 CPU 核心上运行 |
| 管理线程（Management Threads） | taosd 中负责集群管理、协调、网络通信、系统维护等非 per-vnode 操作的线程 |
| 写入线程（Write Threads） | taosd 中负责数据写入、落盘、压缩、合并等 per-vnode 写入路径的线程 |
| 读取线程（Read Threads） | taosd 中负责查询执行、结果获取、流读取等 per-vnode 读取路径的线程 |
| CPU 核心集合（CPU Core Set） | 分配给某一类线程的不相交 CPU 核心 ID 集合 |
| 主开关（Master Switch） | `enableCpuAffinity` 配置参数，控制是否启用 CPU 亲和性功能 |

## 4. 行为说明

### 4.1 配置参数

本特性引入 3 个新的 taosd 配置参数，均写入 `taos.cfg`，需要重启生效：

| 参数名 | 类型 | 默认值 | 有效范围 | 说明 |
| --- | --- | --- | --- | --- |
| `enableCpuAffinity` | BOOL | 0（关闭） | 0 或 1 | 主开关。0 = 所有线程自由运行在所有核心；1 = 启用 CPU 亲和性绑定 |
| `managementCpuCores` | INT32 | 1 | 1–256 | 分配给管理线程的 CPU 核心数量 |
| `readCpuCores` | INT32 | 动态计算：`(totalCores - managementCpuCores) / 2` | 1–256 | 分配给读取线程的 CPU 核心数量 |
| `otherCpuCores` | INT32 | 动态计算：剩余核心 | 1–256 | 分配给写入线程的 CPU 核心数量 |

**配置示例：**

```ini
# taos.cfg — 启用 CPU 亲和性，2 核管理，4 核读取，10 核写入
enableCpuAffinity   1
managementCpuCores  2
readCpuCores        4
otherCpuCores       10
```

**在 16 核机器上的效果：**

- 管理线程：2 核（核心 0, 1）
- 写入线程：10 核（核心 2, 3, 4, 5, 6, 7, 8, 9, 10, 11）
- 读取线程：4 核（核心 12, 13, 14, 15）

### 4.2 核心分配算法

核心按顺序从核心 0 开始分配：

1. **管理线程**：核心 [0 .. M-1]，M = `managementCpuCores`
2. **写入线程**：核心 [M .. M+W-1]，W = `otherCpuCores`
3. **读取线程**：核心 [M+W .. total-1]，R = `readCpuCores`

其中：
- W = `otherCpuCores`（直接使用配置值）
- R = `readCpuCores`（直接使用配置值）
- 验证：`managementCpuCores + readCpuCores + otherCpuCores <= totalCores`

### 4.3 SQL 命令

#### SHOW CPU_ALLOCATION

```sql
SHOW CPU_ALLOCATION;
```

返回集群中所有 dnode 的 CPU 分配状态，每个 dnode 返回 3 行：

```
  dnode_id | thread_category  |  cores  |       core_ids             | enabled |
================================================================================
     1     | management       |    2    | 0,1                        | true    |
     1     | write            |   10    | 6,7,8,9,10,11,12,13,14,15  | true    |
     1     | read             |    4    | 2,3,4,5                    | true    |
     2     | management       |    2    | 0,1                        | true    |
     2     | write            |   10    | 6,7,8,9,10,11,12,13,14,15  | true    |
     2     | read             |    4    | 2,3,4,5                    | true    |
```

当主开关关闭时：

```
  dnode_id | thread_category  |  cores  |  core_ids  | enabled |
================================================================
     1     | management       |    0    | -          | false   |
     1     | write            |    0    | -          | false   |
     1     | read             |    0    | -          | false   |
```

#### 通过 information_schema 查询

```sql
SELECT * FROM information_schema.ins_cpu_allocation;
```

返回格式与 `SHOW CPU_ALLOCATION` 完全一致。

#### 查看配置参数

```sql
SHOW DNODE 1 VARIABLES LIKE 'enableCpuAffinity';
SHOW DNODE 1 VARIABLES LIKE 'managementCpuCores';
SHOW DNODE 1 VARIABLES LIKE 'readCpuCores';
SHOW DNODE 1 VARIABLES LIKE 'otherCpuCores';
```

### 4.4 主开关行为

| 场景 | enableCpuAffinity | 行为 |
| --- | --- | --- |
| 默认安装 / 升级 | 0（默认） | 无任何变化，所有线程自由运行 |
| 显式开启 + ≥3 核 | 1 | 启用 CPU 亲和性绑定 |
| 显式开启 + <3 核 | 1 | 自动禁用亲和性，日志输出 WARNING |
| 关闭 + 配置了其他参数 | 0 | `managementCpuCores`/`readCpuCores`/`otherCpuCores` 值保留但不生效 |

### 4.5 出错处理

| 异常场景 | 行为 |
| --- | --- |
| `managementCpuCores + readCpuCores + otherCpuCores > totalCores`（开关开启时） | taosd 拒绝启动，打印明确错误信息 |
| `managementCpuCores` 超出范围 (< 1 或 > 256) | 配置加载时拒绝，taosd 拒绝启动 |
| `readCpuCores` 超出范围 (< 1 或 > 256) | 配置加载时拒绝，taosd 拒绝启动 |
| `otherCpuCores` 超出范围 (< 1 或 > 256) | 配置加载时拒绝，taosd 拒绝启动 |
| `managementCpuCores + readCpuCores + otherCpuCores > totalCores`（开关开启时） | taosd 拒绝启动，打印明确错误信息 |
| CPU 核心数量 < 3（开关开启时） | 自动禁用亲和性，打印 WARNING 日志 |
| 非 Linux 平台 | 亲和性设置为空操作（no-op），日志提示一次 |
| 主开关关闭时 | 不做运行时核心数验证，仅静态范围检查 |

### 4.6 线程分类

**所有 taosd 进程中的线程必须归入以下三类之一，不得遗漏：**

**管理线程（Management）**：dnode-status, dnode-config, dnode-monitor, mnode-timer, transport(trans-accept, trans-svr-work, cli-threads, http-cli-send-thread), queryWorker(mnode), fetchWorker(mnode), statusWorker(mnode), writeWorker(mnode), syncWorker(mnode), timer, log, wal, catalog, scheduler, open-vnodes, close-vnodes 等。

**写入线程（Write）**：vnode-write, vnode-apply, vnode-sync, vnode-sync-rd, vnode-commit, vnode-merge, vnode-compact, vnode-retention, vnode-scan, vnode-rsma, mgmtWorker, mgmtMultiWorker 等 per-vnode 写入路径线程。

**读取线程（Read）**：queryPool, fetchPool, streamReaderPool 等 per-vnode 读取路径线程。

> 注：客户端线程（hb, stmtBind 等）不在管理范围内，仅管理 taosd 服务进程内的线程。

## 5. 性能

- **CPU 亲和性设置仅在线程创建时执行一次**，后续无运行时开销
- **减少缓存污染**：同类线程共享 CPU 核心，L1/L2 缓存命中率提升
- **管理线程保护**：专用核心保障管理操作低延迟，减少集群协调抖动
- **主开关关闭时**：零性能影响，与未引入本特性完全一致

## 6. 安全

无安全影响。CPU 亲和性是操作系统级的线程调度特性，不涉及数据机密性、完整性或可用性。所有配置通过标准 `taos.cfg` 管理。

## 7. 兼容性

- **完全向后兼容**：主开关默认关闭（`enableCpuAffinity=0`），升级不改变任何运行行为
- **配置参数不影响旧功能**：`managementCpuCores`、`readCpuCores` 和 `otherCpuCores` 在主开关关闭时不生效
- **线程数量不变**：本特性仅控制线程在哪些 CPU 核运行，不改变线程数量。现有线程数配置参数（如 `tsNumOfCompactThreads` 等）完全独立

## 8. 运维

- 配置变更需重启 taosd 生效（不支持动态修改）
- 多节点集群中，每个节点独立配置
- 可通过 `SHOW CPU_ALLOCATION` 实时验证配置是否正确生效
- 容器环境中使用 cgroup 感知的 CPU 核心检测，仅使用容器可见的核心

## 9. 使用场景

### 9.1 写入密集型场景

大量数据持续写入，查询较少。将更多核心分配给写入线程：

```ini
enableCpuAffinity   1
managementCpuCores  1
readCpuCores        2
otherCpuCores       5
```

### 9.2 查询密集型场景

数据已稳定，大量并发查询。将更多核心分配给读取线程：

```ini
enableCpuAffinity   1
managementCpuCores  1
readCpuCores        5
otherCpuCores       2
```

### 9.3 混合负载场景

读写均衡。读写核心平均分配：

```ini
enableCpuAffinity   1
managementCpuCores  2
readCpuCores        3
otherCpuCores       3
```

### 9.4 大规模集群管理节点

集群管理操作频繁，需要更多管理核心：

```ini
enableCpuAffinity   1
managementCpuCores  4
readCpuCores        6
otherCpuCores       6
```

### 9.5 容器化部署

在 Docker/K8s 中限制 CPU 数量时，系统自动感知 cgroup 限制的核心数。若容器仅有 2 核，亲和性自动禁用。

### 9.6 安全回滚

生产环境遇到问题时，将 `enableCpuAffinity` 改回 0 并重启即可恢复原始行为，无需修改其他参数。

## 10. 约束和限制

**约束：**
- 系统至少需要 3 个 CPU 核心才能启用 CPU 亲和性
- 配置变更需要重启 taosd 生效
- 仅支持 Linux 平台的实际亲和性绑定；macOS/Windows 为空操作

**限制：**
- 不支持动态调整核心分配（需重启）
- 不支持热插拔 CPU 核心（使用启动时检测的核心数）
- 不管理客户端线程（仅 taosd 服务端进程）
- 总核心数需 ≥ `managementCpuCores + readCpuCores + otherCpuCores`

## 11. 常见错误和排查

| 错误现象 | 可能原因 | 排查方法 |
| --- | --- | --- |
| taosd 启动失败，日志显示 "managementCpuCores exceeds available" | `managementCpuCores` 配置超过 total − 2 | 检查 `taos.cfg`，减小 `managementCpuCores` 值 |
| `SHOW CPU_ALLOCATION` 显示 `enabled=false` 但配置了 `enableCpuAffinity=1` | 系统 CPU 核心 < 3 | 检查 `grep -c processor /proc/cpuinfo` 或容器 CPU 限制 |
| 线程亲和性未生效 | 非 Linux 平台 | 检查操作系统类型，macOS/Windows 不支持实际绑定 |
| 配置修改后无效果 | 未重启 taosd | 重启 taosd 后再检查 |

**OS 级验证方法：**

```bash
# 检查 taosd 所有线程的 CPU 亲和性
for tid in $(ls /proc/$(pidof taosd)/task/); do
    name=$(cat /proc/$(pidof taosd)/task/$tid/comm 2>/dev/null)
    affinity=$(taskset -cp $tid 2>/dev/null | awk -F: '{print $2}')
    echo "$tid $name $affinity"
done
```

## 12. 可观测性

- **taos shell**：支持 `SHOW CPU_ALLOCATION` 命令直接查看分配状态
- **information_schema**：新增 `ins_cpu_allocation` 表，可通过标准 SQL 查询
- **DNODE VARIABLES**：`enableCpuAffinity`、`managementCpuCores`、`readCpuCores`、`otherCpuCores` 均可通过 `SHOW DNODE N VARIABLES LIKE '...'` 查看
- **启动日志**：启用亲和性时，在 INFO 级别日志输出核心分配信息（如 "CPU affinity enabled: mgmt=[0-1], write=[2-5], read=[6-15]"）
- **taosExplorer / TDinsight**：无影响，仅增加了可查询的系统表

## 13. 安装和卸载

无特殊要求。新增的配置参数在默认值下不影响安装和卸载流程。升级安装后，如果 `taos.cfg` 中没有 `enableCpuAffinity`，系统使用默认值 0（关闭），行为与升级前完全一致。

## 14. 文档

- 需要修改企业版文档：配置参数说明、`SHOW CPU_ALLOCATION` 命令说明、`information_schema.ins_cpu_allocation` 表说明
- 需要修改官网文档：
  - `01-taosd.md` — 新增 4 个配置参数说明
  - `08-config-scope.md` — 新增配置参数作用域条目
  - `52-show.md` — 新增 `SHOW CPU_ALLOCATION` 命令说明
  - `50-meta.md` — 新增 `ins_cpu_allocation` 表说明
- 文档 PR 已准备（中英文各 4 个文件）

## 15. 参考文档

- Linux `sched_setaffinity(2)` man page
- POSIX `pthread_setaffinity_np(3)` man page

## 16. 附录

### 附录 A：核心分配算法伪代码

```
function allocateCores(totalCores, mgmtCores, readCores, otherCores):
    if totalCores < 3:
        return DISABLED  // 自动禁用
    
    if mgmtCores + readCores + otherCores > totalCores:
        return ERROR  // 配置无效
    
    mgmt  = cores[0 .. mgmtCores-1]
    write = cores[mgmtCores .. mgmtCores+otherCores-1]
    read  = cores[mgmtCores+otherCores .. mgmtCores+otherCores+readCores-1]
    
    return {mgmt, write, read}
```
