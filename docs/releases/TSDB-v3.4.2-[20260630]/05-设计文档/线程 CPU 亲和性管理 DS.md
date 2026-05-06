# 详细设计说明书（Design Spec）— 线程 CPU 亲和性管理

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-16 | 2026-04-16 | 1.0 | dmchen | 初始版本 |

## 2. 引言

### 2.1 目的

本文档描述 TDengine taosd 线程 CPU 亲和性管理特性的详细技术设计，包括数据结构、算法、接口、模块交互和配置管理。

### 2.2 范围

- 三个新增配置参数的注册与加载
- CPU 核心分配算法的实现
- 线程亲和性绑定的调用点
- `SHOW CPU_ALLOCATION` SQL 命令及 `ins_cpu_allocation` 系统表
- 跨平台兼容性处理

### 2.3 受众

TDengine 内核开发人员、测试工程师、代码审查人员。

## 3. 术语

| 术语 | 定义 |
| --- | --- |
| `cpu_set_t` | Linux POSIX 类型，表示一组 CPU 核心 ID 的位掩码 |
| `sched_setaffinity` | Linux 系统调用，设置进程/线程的 CPU 亲和性 |
| `pthread_setaffinity_np` | POSIX 线程扩展，设置线程级 CPU 亲和性 |
| `sched_getaffinity` | Linux 系统调用，获取当前进程可用的 CPU 核心集合 |
| CFG_SCOPE_SERVER | TDengine 配置参数作用域：仅服务端（taosd） |
| CFG_DYN_NONE | TDengine 配置参数动态性：不支持动态修改，需重启 |
| CFG_CATEGORY_LOCAL | TDengine 配置参数分类：本地节点级别 |
| per-vnode | 每个虚拟节点（vnode）独立拥有的线程/资源 |

## 4. 概述

### 4.1 架构

本特性的架构分为四层：

```
┌─────────────────────────────────────────────────┐
│                 用户层 (SQL)                      │
│   SHOW CPU_ALLOCATION / ins_cpu_allocation       │
├─────────────────────────────────────────────────┤
│                配置层 (taos.cfg)                   │
│   enableCpuAffinity / managementCpuCores /       │
│   readCpuCores / otherCpuCores                    │
├─────────────────────────────────────────────────┤
│              核心分配层 (osSysinfo.c)              │
│   taosInitCpuAllocation() → SCpuAllocStatus       │
├─────────────────────────────────────────────────┤
│              线程绑定层                            │
│   taosSetCpuAffinity() ← 各 worker 线程创建点      │
├─────────────────────────────────────────────────┤
│              操作系统层                            │
│   pthread_setaffinity_np() / sched_getaffinity()  │
└─────────────────────────────────────────────────┘
```

### 4.2 技术

- 语言：C (C11)，TDengine 3.x 代码库
- 线程库：POSIX threads (pthreads)
- 亲和性 API：Linux `sched.h` (`sched_setaffinity`, `sched_getaffinity`)
- 配置系统：TDengine config (`cfgAddBool`, `cfgAddInt32`, `cfgGetItem`)
- SQL 解析：TDengine parser (sql.y / parTranslater.c)
- 系统表：information_schema 扩展

### 4.3 依赖项

| 依赖 | 说明 |
| --- | --- |
| osSysinfo.h / osSysinfo.c | OS 抽象层，新增 CPU 亲和性 API |
| tglobal.h / tglobal.c | 全局配置参数注册与加载 |
| tworker.h / tworker.c | 线程池基础设施，添加亲和性绑定调用 |
| dmHandle.c | dnode 消息处理，新增 SHOW CPU_ALLOCATION 处理器 |
| dmEnv.c | dnode 环境初始化，调用 taosInitCpuAllocation() |
| sql.y / parTranslater.c | SQL 解析器，新增 SHOW CPU_ALLOCATION 语法 |
| sysscanoperator.c | 系统表扫描算子，路由 ins_cpu_allocation 查询 |
| systable.h / systable.c | 系统表定义，新增 ins_cpu_allocation |

## 5. 设计考虑

### 5.1 假设和限制

- 仅 Linux 平台支持实际的 CPU 亲和性绑定；macOS 和 Windows 为 no-op
- CPU 亲和性在线程创建时设置一次，后续不更改
- 配置参数为静态（需重启），不支持动态修改
- 最少需要 3 个 CPU 核心才能启用亲和性
- 容器环境中使用 cgroup 感知的 CPU 检测（`sched_getaffinity`）

### 5.2 设计模式和原则

- **单例模式**：`SCpuAllocStatus` 为全局唯一实例（`gCpuAllocStatus`），启动时初始化一次，之后只读
- **策略模式**：分配算法可独立于配置加载和线程绑定逻辑
- **关注点分离**：配置层（tglobal.c）、算法层（osSysinfo.c）、绑定层（各 worker 调用点）相互独立
- **防御式编程**：主开关关闭时所有亲和性调用为 no-op；<3 核心时自动降级

### 5.3 风险和缓解措施

| 风险 | 影响 | 缓解措施 |
| --- | --- | --- |
| 配置错误导致启动失败 | 服务不可用 | 启动时验证配置，输出明确错误信息 |
| <3 核心系统无法使用 | 功能受限 | 自动降级，打印 WARNING，正常运行 |
| 非 Linux 平台无效果 | 用户误解 | 文档说明，启动日志提示 |
| CPU 热插拔导致不一致 | 配置偏差 | 使用启动时核心数，要求重启刷新 |

## 6. 详细设计

### 6.1 组件设计

#### 6.1.1 配置注册组件 (tglobal.c)

在 `taosAddServerCfg()` 函数中注册 3 个新的配置参数：

```c
// 全局变量声明 (tglobal.h)
extern bool    tsEnableCpuAffinity;     // 默认: false
extern int32_t tsManagementCpuCores;    // 默认: 1
extern int32_t tsReadCpuCores;          // 默认: 动态计算 (totalCores - managementCpuCores) / 2
extern int32_t tsOtherCpuCores;         // 默认: 动态计算（剩余核心）

// 配置注册 (tglobal.c)
cfgAddBool(pCfg, "enableCpuAffinity", tsEnableCpuAffinity,
           CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_LOCAL, CFG_PRIV_SYSTEM);

cfgAddInt32(pCfg, "managementCpuCores", tsManagementCpuCores,
            1, 256, CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_LOCAL, CFG_PRIV_SYSTEM);

cfgAddInt32(pCfg, "readCpuCores", tsReadCpuCores,
            1, 256, CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_LOCAL, CFG_PRIV_SYSTEM);

cfgAddInt32(pCfg, "otherCpuCores", tsOtherCpuCores,
            1, 256, CFG_SCOPE_SERVER, CFG_DYN_NONE, CFG_CATEGORY_LOCAL, CFG_PRIV_SYSTEM);
```

#### 6.1.2 CPU 分配核心组件 (osSysinfo.c)

**核心职责**：初始化 CPU 分配、设置线程亲和性、提供分配状态查询。

### 6.2 关键数据结构

#### 6.2.1 EThreadCategory 枚举

```c
typedef enum {
  THREAD_CAT_MANAGEMENT = 0,  // 管理线程
  THREAD_CAT_WRITE      = 1,  // 写入线程
  THREAD_CAT_READ       = 2,  // 读取线程
  THREAD_CAT_COUNT      = 3   // 分类总数
} EThreadCategory;
```

#### 6.2.2 SCpuCoreSet 结构

```c
#define TAOS_MAX_CPU_CORES 256

typedef struct {
  EThreadCategory category;               // 所属线程分类
#ifdef __linux__
  cpu_set_t       mask;                    // POSIX CPU 集合位掩码
#else
  unsigned long   mask;                    // 非 Linux 平台占位
#endif
  int32_t         coreIds[TAOS_MAX_CPU_CORES]; // 分配的核心 ID 数组
  int32_t         count;                   // 核心数量
} SCpuCoreSet;
```

#### 6.2.3 SCpuAllocStatus 结构

```c
typedef struct {
  bool        enabled;                     // 亲和性是否实际生效
  SCpuCoreSet sets[THREAD_CAT_COUNT];      // 三类线程的核心集合
  int32_t     totalCores;                  // 系统总 CPU 核心数
} SCpuAllocStatus;

// 全局单例
static SCpuAllocStatus gCpuAllocStatus = {0};
```

### 6.3 数据库设计

#### information_schema.ins_cpu_allocation 表

| 列名 | 类型 | 说明 |
| --- | --- | --- |
| dnode_id | INT | Dnode 标识符 |
| thread_category | VARCHAR(16) | 线程分类："management", "write", "read" |
| cores | INT | 分配的核心数量 |
| core_ids | VARCHAR(256) | 核心 ID 列表（逗号分隔，如 "0,1,2"），禁用时为 "-" |
| enabled | BOOL | 此分类的亲和性是否生效 |

**路由**：该表通过 `sysTableFromDnode()` 标记为 dnode 来源表，translator 为所有 dnode 构建 endpoint 列表，planner 为每个 dnode 创建子计划，executor 向每个 dnode 发送 `TDMT_DND_SYSTABLE_RETRIEVE` 请求并汇总结果。

### 6.4 设计图

#### 6.4.1 数据流图

```
                    taos.cfg
                       │
                       ▼
              ┌────────────────┐
              │  cfgLoad()     │  配置加载
              │  taosLoadCfg() │
              └───────┬────────┘
                      │
        ┌─────────────┼─────────────┐
        │             │             │
        ▼             ▼             ▼
  tsEnableCpu    tsManagement   tsReadCpu    tsOtherCpu
  Affinity       CpuCores      Cores        Cores
        │             │             │             │
        └─────────────┼─────────────┘
                      │
                      ▼
          ┌───────────────────────┐
          │ taosInitCpuAllocation │
          │ (osSysinfo.c)         │
          └───────────┬───────────┘
                      │
                      ▼
          ┌───────────────────────┐
          │  gCpuAllocStatus      │  全局分配状态
          │  (SCpuAllocStatus)    │
          └───────────┬───────────┘
                      │
            ┌─────────┼─────────┐
            ▼         ▼         ▼
     各 worker    SHOW CPU     ins_cpu_
     线程创建     ALLOCATION   allocation
     调用点       (dmHandle)   (sysscan)
```

#### 6.4.2 消息序列图 — SHOW CPU_ALLOCATION 查询

```
  taos client          parser          executor         dnode(dmHandle)
      │                  │                │                   │
      │ SHOW CPU_ALLOC   │                │                   │
      ├─────────────────►│                │                   │
      │                  │ translate to   │                   │
      │                  │ SELECT * FROM  │                   │
      │                  │ ins_cpu_alloc  │                   │
      │                  ├───────────────►│                   │
      │                  │                │ TDMT_DND_SYSTABLE │
      │                  │                │ _RETRIEVE         │
      │                  │                ├──────────────────►│
      │                  │                │                   │ read
      │                  │                │                   │ gCpuAllocStatus
      │                  │                │    SSDataBlock    │
      │                  │                │◄──────────────────┤
      │                  │   result rows  │                   │
      │◄─────────────────┼───────────────┤                   │
      │   3 rows         │                │                   │
```

#### 6.4.3 流程图 — taosInitCpuAllocation()

```
            ┌───────────────────┐
            │     START         │
            └───────┬───────────┘
                    │
            ┌───────▼───────────┐
            │ tsEnableCpuAffin  │
            │ ity == false?     │──── YES ──► enabled=false; RETURN OK
            └───────┬───────────┘
                    │ NO
            ┌───────▼───────────┐
            │ sched_getaffinity │
            │ detect totalCores │
            └───────┬───────────┘
                    │
            ┌───────▼───────────┐
            │ totalCores < 3?   │──── YES ──► enabled=false; LOG WARN; RETURN OK
            └───────┬───────────┘
                    │ NO
            ┌───────▼───────────┐
            │ mgmtCores >       │
            │ totalCores - 2?   │──── YES ──► LOG ERROR; RETURN -1
            └───────┬───────────┘
                    │ NO
            ┌───────▼───────────────────┐
            │ remaining = total - mgmt  │
            │ readCount = floor(        │
            │   remaining*ratio/100)    │
            │ readCount = max(1, read)  │
            │ writeCount = remain-read  │
            │ writeCount = max(1, write)│
            └───────┬───────────────────┘
                    │
            ┌───────▼───────────────────┐
            │ overflow?                 │
            │ read+write > remaining?   │──── YES ──► readCount = remaining - writeCount
            └───────┬───────────────────┘
                    │ NO
            ┌───────▼───────────────────┐
            │ Assign core IDs:          │
            │ mgmt  = [0..M-1]          │
            │ write = [M..M+W-1]        │
            │ read  = [M+W..total-1]    │
            └───────┬───────────────────┘
                    │
            ┌───────▼───────────────────┐
            │ Build cpu_set_t masks     │
            │ Store in gCpuAllocStatus  │
            │ enabled = true            │
            │ LOG INFO: allocations     │
            └───────┬───────────────────┘
                    │
            ┌───────▼───────┐
            │    RETURN 0   │
            └───────────────┘
```

#### 6.4.4 状态转换图

```
                ┌─────────┐
                │  UNSET  │  (全局初始状态)
                └────┬────┘
                     │  taosInitCpuAllocation()
           ┌─────────┼─────────┐
           ▼                   ▼
    ┌──────────┐        ┌──────────┐
    │ DISABLED │        │ ENABLED  │
    │          │        │          │
    │ switch=0 │        │ switch=1 │
    │ or <3核  │        │ & ≥3核   │
    └──────────┘        └──────────┘
         │                   │
         │    taosd shutdown │
         └────────┬──────────┘
                  ▼
            ┌──────────┐
            │ SHUTDOWN │
            └──────────┘
```

## 7. 接口规范

### 7.1 API 文档

#### 7.1.1 OS 抽象层 API (osSysinfo.h)

```c
/**
 * 初始化 CPU 分配。在 taosd 启动时，配置加载之后、worker 线程创建之前调用。
 * 计算三类线程的核心集合并存储到全局 gCpuAllocStatus。
 * @return 0 成功，-1 配置无效（taosd 应拒绝启动）
 */
int32_t taosInitCpuAllocation(void);

/**
 * 为当前线程设置 CPU 亲和性。在线程创建后从线程内部调用。
 * @param category 线程分类 (THREAD_CAT_MANAGEMENT/WRITE/READ)
 * @return 0 成功，-1 失败（仅记录日志，不中断程序）
 */
int32_t taosSetCpuAffinity(EThreadCategory category);

/**
 * 获取当前 CPU 分配状态（只读，初始化后不变）。
 * @return 指向全局 SCpuAllocStatus 的指针
 */
const SCpuAllocStatus *taosGetCpuAllocStatus(void);

/**
 * 获取当前进程可用的 CPU 核心集合（Linux: cgroup 感知）。
 * @param cpuset 输出参数
 * @return 可用核心数量
 */
int32_t taosGetAvailableCpuSet(cpu_set_t *cpuset);
```

#### 7.1.2 SQL 接口

| 命令 | 说明 |
| --- | --- |
| `SHOW CPU_ALLOCATION` | 查看集群中所有 dnode 的 CPU 分配状态，每个 dnode 返回 3 行 |
| `SELECT * FROM information_schema.ins_cpu_allocation` | 等价的 SQL 查询形式 |
| `SHOW DNODE N VARIABLES LIKE 'enableCpuAffinity'` | 查看主开关值 |
| `SHOW DNODE N VARIABLES LIKE 'managementCpuCores'` | 查看管理核心数 |
| `SHOW DNODE N VARIABLES LIKE 'readCpuCores'` | 查看读取核心数 |
| `SHOW DNODE N VARIABLES LIKE 'otherCpuCores'` | 查看写入核心数 |

### 7.2 线程绑定调用点

以下是所有调用 `taosSetCpuAffinity()` 的位置：

| 调用位置 | 线程分类 | 线程类型 |
| --- | --- | --- |
| dmWorker.c | THREAD_CAT_MANAGEMENT | dnode-status, dnode-config, dnode-monitor 等 |
| mmWorker.c | THREAD_CAT_MANAGEMENT | mnode workers (query, fetch, write, sync) |
| smWorker.c | THREAD_CAT_MANAGEMENT | snode runner |
| qmWorker.c | THREAD_CAT_MANAGEMENT | qnode workers |
| transSvr.c | THREAD_CAT_MANAGEMENT | trans-accept, trans-svr-work |
| transCli.c | THREAD_CAT_MANAGEMENT | cli-threads |
| thttp.c | THREAD_CAT_MANAGEMENT | http-cli-send-thread |
| vmInt.c | THREAD_CAT_MANAGEMENT | open/close/restore-vnodes, vnode-timer |
| osTimer.c | THREAD_CAT_MANAGEMENT | timer, tmr |
| tlog.c | THREAD_CAT_MANAGEMENT | log, logRotate |
| tcache.c | THREAD_CAT_MANAGEMENT | cacheRefresh |
| walMgmt.c | THREAD_CAT_MANAGEMENT | wal |
| ctgCache.c | THREAD_CAT_MANAGEMENT | catalog |
| tsched.c | THREAD_CAT_MANAGEMENT | scheduler |
| tworker.c | THREAD_CAT_MANAGEMENT/WRITE/READ | 通用 worker pool（根据传入的 category） |
| vmWorker.c | THREAD_CAT_WRITE | mgmtWorker, mgmtMultiWorker, vnode-write/apply/sync |
| vnodeAsync.c | THREAD_CAT_WRITE | vnode-commit, merge, compact, retention, scan |
| smaEnv.c | THREAD_CAT_WRITE | vnode-rsma |
| vmWorker.c | THREAD_CAT_READ | queryPool, fetchPool, streamReaderPool |
| authClient.c | THREAD_CAT_MANAGEMENT | auth-hb (企业版) |
| authServer.c | THREAD_CAT_MANAGEMENT | auth-quota-refresh (企业版) |
| dbRest.c | THREAD_CAT_MANAGEMENT | auth-update (企业版) |

## 8. 安全考虑

### 8.1 安全要求

- CPU 亲和性是操作系统级别的线程调度特性，不涉及数据加密或用户认证
- 配置参数通过 `taos.cfg` 管理，遵循文件系统权限控制
- `SHOW CPU_ALLOCATION` 命令不需要特殊权限（系统状态信息）

### 8.2 漏洞缓解

- 无新增网络接口或用户输入路径
- 配置参数有明确的范围约束（cfgAddInt32 范围检查）
- 不存在缓冲区溢出风险（`TAOS_MAX_CPU_CORES = 256` 硬限制）

## 9. 性能和可扩展性

### 9.1 性能要求

- **零运行时开销**：亲和性在线程创建时设置一次（`pthread_setaffinity_np` 调用一次）
- **启动时间影响**：`taosInitCpuAllocation()` 执行时间 < 1ms
- **查询影响**：`SHOW CPU_ALLOCATION` 直接读取内存中的全局状态，无磁盘 I/O

### 9.2 可扩展性

- 支持最多 256 个 CPU 核心（`TAOS_MAX_CPU_CORES` 宏定义）
- 多节点集群中每个节点独立配置，水平扩展无影响
- 核心分配算法时间复杂度 O(N)，N 为核心数量

## 10. 部署和配置

### 10.1 部署流程

1. 升级 taosd 到包含本特性的版本
2. 编辑 `taos.cfg`，添加新配置参数
3. 重启 taosd
4. 通过 `SHOW CPU_ALLOCATION` 验证配置生效

### 10.2 配置管理

| 参数 | 文件位置 | 默认值 | 动态性 |
| --- | --- | --- | --- |
| enableCpuAffinity | taos.cfg | 0 | 需重启 |
| managementCpuCores | taos.cfg | 1 | 需重启 |
| readCpuCores | taos.cfg | 动态计算 | 需重启 |
| otherCpuCores | taos.cfg | 动态计算 | 需重启 |

### 10.3 版本控制

- **向后兼容**：默认值不改变任何行为
- **升级**：无迁移需要，新参数缺失时使用默认值
- **回滚**：将 `enableCpuAffinity` 设为 0 即可恢复

## 11. 监控和维护

### 11.1 监控

- `SHOW CPU_ALLOCATION` 或 `information_schema.ins_cpu_allocation` 查看当前分配
- `SHOW DNODE N VARIABLES LIKE 'enableCpuAffinity'` 查看开关状态

### 11.2 日志记录和诊断

| 日志级别 | 内容 |
| --- | --- |
| INFO | 启动时 CPU 分配结果："CPU affinity enabled: mgmt=[0-1], write=[2-5], read=[6-15]" |
| WARN | <3 核心自动禁用、线程亲和性设置失败 |
| ERROR | 配置无效（managementCpuCores 超限）导致启动失败 |

### 11.3 维护

- 修改配置需编辑 `taos.cfg` 并重启 taosd
- 容器环境中调整 CPU 限制后需重启 taosd 以重新检测

## 12. 参考资料

1. 概要设计说明书：`specs/003-thread-cpu-affinity/概要设计说明书.md`
2. 功能测试报告：`specs/003-thread-cpu-affinity/功能测试报告.md`
3. 特性规格文档：`specs/003-thread-cpu-affinity/spec.md`
4. 合并前的设计文档：`specs/001-thread-cpu-management/plan.md`, `specs/002-cpu-affinity-switch/plan.md`

### 修改文件清单

| 文件 | 修改类型 | 说明 |
| --- | --- | --- |
| include/os/osSysinfo.h | 新增 | EThreadCategory, SCpuCoreSet, SCpuAllocStatus 定义 |
| source/os/src/osSysinfo.c | 新增 | CPU 分配算法实现 |
| include/common/tglobal.h | 修改 | 新增 3 个全局变量声明 |
| source/common/src/tglobal.c | 修改 | 注册 3 个配置参数 |
| include/common/systable.h | 修改 | 新增 TSDB_INS_TABLE_CPU_ALLOCATION |
| source/common/src/systable.c | 修改 | 注册 ins_cpu_allocation 表定义 |
| include/common/tmsg.h | 修改 | 新增消息类型 |
| source/dnode/mgmt/node_mgmt/src/dmEnv.c | 修改 | 启动时调用 taosInitCpuAllocation() |
| source/dnode/mgmt/mgmt_dnode/src/dmHandle.c | 修改 | SHOW CPU_ALLOCATION 处理器 |
| source/dnode/mgmt/mgmt_dnode/src/dmWorker.c | 修改 | dnode worker 亲和性绑定 |
| source/dnode/mgmt/mgmt_vnode/src/vmInt.c | 修改 | vnode 全局线程亲和性绑定 |
| source/dnode/mgmt/mgmt_vnode/src/vmWorker.c | 修改 | vnode worker 亲和性绑定 |
| source/dnode/mnode/impl/src/mndMain.c | 修改 | mnode timer 亲和性绑定 |
| source/dnode/vnode/src/sma/smaEnv.c | 修改 | RSMA 线程亲和性绑定 |
| source/dnode/vnode/src/vnd/vnodeAsync.c | 修改 | vnode 异步线程亲和性绑定 |
| source/libs/catalog/src/ctgCache.c | 修改 | catalog 线程亲和性绑定 |
| source/libs/executor/src/sysscanoperator.c | 修改 | ins_cpu_allocation 查询路由 |
| source/libs/nodes/src/nodesUtilFuncs.c | 修改 | SHOW 语句节点类型 |
| source/libs/parser/inc/sql.y | 修改 | SHOW CPU_ALLOCATION 语法规则 |
| source/libs/parser/src/parAstParser.c | 修改 | AST 解析支持 |
| source/libs/parser/src/parAuthenticator.c | 修改 | 权限认证 |
| source/libs/parser/src/parTokenizer.c | 修改 | 词法分析（ALLOCATION token） |
| source/libs/parser/src/parTranslater.c | 修改 | SHOW → SELECT 翻译；`sysTableFromDnode()` 增加 ins_cpu_allocation |
| source/libs/planner/src/planPhysiCreater.c | 修改 | 系统表 epSet 复制，支持 ins_cpu_allocation 按 dnode 路由 |
| source/libs/transport/src/thttp.c | 修改 | HTTP 客户端线程亲和性绑定 |
| source/libs/transport/src/transCli.c | 修改 | 传输客户端线程亲和性绑定 |
| source/libs/transport/src/transSvr.c | 修改 | 传输服务端线程亲和性绑定 |
| source/libs/wal/src/walMgmt.c | 修改 | WAL 线程亲和性绑定 |
| source/os/src/osTimer.c | 修改 | 定时器线程亲和性绑定 |
| source/util/src/tcache.c | 修改 | 缓存刷新线程亲和性绑定 |
| source/util/src/tlog.c | 修改 | 日志线程亲和性绑定 |
| source/util/src/tsched.c | 修改 | 调度器线程亲和性绑定 |
| source/util/src/tworker.c | 修改 | 通用 worker pool 亲和性绑定 |
| include/util/tworker.h | 修改 | worker pool 结构体新增 category 字段 |
| enterprise/src/plugins/grant/src/authClient.c | 修改 | auth-hb 线程亲和性绑定 |
| enterprise/src/plugins/grant/src/authServer.c | 修改 | auth-quota-refresh 线程亲和性绑定 |
| enterprise/src/plugins/grant/src/dbRest.c | 修改 | auth-update 线程亲和性绑定 |
| docs/en/14-reference/* (4 files) | 修改 | 英文文档 |
| docs/zh/14-reference/* (4 files) | 修改 | 中文文档 |
| test/cases/34-CpuAffinity/* (8 files) | 新增 | 测试用例 |
| test/ci/cases.task | 修改 | CI 注册 |
