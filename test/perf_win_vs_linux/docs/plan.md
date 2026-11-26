# TDengine TSDB：Windows 与 Linux 同机性能对比测试方案

> 状态：**待 Review**
> 版本：v0.1（2026-11-05）
> 目标读者：测试执行人 + Review 人

---

## 1. 目标与范围

### 1.1 目标

在同一台物理机上（IT 已安装 Windows / Linux 双系统，共用同一块 NVMe），量化 TDengine TSDB 在两个操作系统上的**写入**与**查询**性能差异，并给出可归因的结论。

### 1.2 范围内

- 单机单节点、单副本（`replica 1`）、默认压缩
- 写入吞吐、写入延迟分布、写入稳定性
- 聚合查询、降采样查询、并发查询、全表扫描（统一冷缓存口径）
- OS 侧资源消耗对比（CPU / IO / 内存）

### 1.3 范围外（本方案不覆盖）

- 集群、多副本、跨机网络（本机网卡 100 Mbps，跨机会成为瓶颈）
- 流计算、数据订阅、taosX / taosAdapter
- 数据压缩算法的绝对值优劣（只记录压缩后体积）
- 写入线程数/批大小的敏感性扫描、长时间稳定性、无 WAL 对照——本轮只做固定参数（**8 线程 / 批 5000**）下的两种写入方式对比

---

## 2. 前提：IT 交付后必须先做的环境验收

**不满足则整套测试结果无效。** 每项目标值都要落在报告里。

| # | 检查项 | Linux 命令 | Windows 命令 | 通过标准 |
|---|---|---|---|---|
| P1 | 同一块 NVMe | `lsblk -o NAME,MODEL,SERIAL,SIZE` | `Get-PhysicalDisk \| ft FriendlyName,SerialNumber,Size` | 两端 `SerialNumber` 一致 |
| P2 | 数据分区可用空间 | `df -hT <挂载点>` | `Get-Volume` | 均 ≥ 200 GB 且数值接近（10 亿行实测占 35 GB，需留重建余量） |
| P3 | CPU / 内存 | `lscpu`、`free -h` | `Get-CimInstance Win32_Processor`、`Win32_ComputerSystem` | 核数、内存容量完全一致 |
| P4 | TDengine 版本 | `taosd -V`、`taosBenchmark -V` | 同左 | 均为 **3.4.3.9**，MD5 校验通过 |
| P5 | 时区 / locale | `timedatectl` | `Get-TimeZone` | 均为 UTC 或同一时区 |
| P6 | 磁盘基线（交叉验证） | `dd` 见 §7.3 | 见 §7.3 | 两端顺序读差异 < 15% |

> ⚠️ 若 P1 不满足（两套系统看到不同磁盘），立即停止——结果不可用。
>
> ⚠️ 若 P4 不满足（版本不一致），先统一版本再测。

### 2.1 安装包来源（固定 v3.4.3.9）

两端统一使用 **v3.4.3.9 官方安装包**，不使用源码构建产物。

NAS 目录：`http://192.168.1.131/data/nas/TDengine/3.4/v3.4.3.9/enterprise/`

| 平台 | 文件 | MD5 |
|---|---|---|
| Linux x86_64 | `tdengine-tsdb-enterprise-3.4.3.9-linux-x64.tar.xz` | `ed701ae3b250715ffe74819867b542cb` |
| Windows x64 | `tdengine-tsdb-enterprise-3.4.3.9-windows-x64.exe` | `aaa4264abbdadca615531224138eb009` |

校验清单：同目录 `3.4.3.9.md5`；Windows 包另有 `.sha256`。

**Linux 获取与校验**

```bash
BASE=http://192.168.1.131/data/nas/TDengine/3.4/v3.4.3.9/enterprise
PKG=tdengine-tsdb-enterprise-3.4.3.9-linux-x64.tar.xz
mkdir -p ~/pkg && cd ~/pkg
wget "$BASE/$PKG" && wget "$BASE/3.4.3.9.md5"
md5sum -c --ignore-missing 3.4.3.9.md5        # 应输出: $PKG: OK
tar -xJf "$PKG" && cd tdengine-tsdb-enterprise-3.4.3.9-linux-x64
./install.sh -e no
./start-all.sh
```

**Windows 获取与安装**

```powershell
taosd -V 前的准备：下载并校验
$base='http://192.168.1.131/data/nas/TDengine/3.4/v3.4.3.9/enterprise'
Invoke-WebRequest "$base/tdengine-tsdb-enterprise-3.4.3.9-windows-x64.exe" -OutFile "$env:TEMP\taos-3.4.3.9.exe"
(Get-FileHash "$env:TEMP\taos-3.4.3.9.exe" -Algorithm SHA256).Hash
# 与 $base/tdengine-tsdb-enterprise-3.4.3.9-windows-x64.exe.sha256 对比后再执行安装
```

> ⚠️ `tsdb-ops-install` skill 的 `scripts/install.sh` 里写的包名是 `.tar.gz`，而 NAS 实际是 **`.tar.xz`**，直接调该脚本会报 HTTP 404。按本文命令手装。
> ⚠️ 安装完成后**两端都要** `taosd -V` / `taosBenchmark -V` 确认输出 `3.4.3.9`，不一致立即停止。

---

## 3. 变量控制

### 3.1 必须完全一致（对齐项）

| 类别 | 参数 | 取值 |
|---|---|---|
| 版本 | taosd / taosBenchmark | **固定 v3.4.3.9 官方安装包**（来源见 §2.1） |
| 数据模型 | 表数量 | 10,000 子表 |
| 数据模型 | 每表行数 | 100,000（10,000 表合计 **10 亿行**） |
| 数据模型 | 列 / 标签 / 类型 | 见 §4.1，完全固定 |
| 数据模型 | 时间步长 / 起始时间 | 1000 ms / `2026-01-01T00:00:00.000Z` |
| 数据库 | `vgroups` | 8 |
| 数据库 | `replica` | 1 |
| 数据库 | `precision` | ms |
| 数据库 | `BUFFER` / `PAGES` / `PAGESIZE` | 256 MB / 256 / 4 KB |
| 数据库 | `CACHESIZE` | 1 MB |
| 数据库 | `WAL_LEVEL` | 1（默认值，主场景） |
| 数据库 | `STT_TRIGGER` / `MINROWS` / `MAXROWS` | 1 / 100 / 4096 |
| 客户端 | 连接方式 | `taosc`（Native），`-h 127.0.0.1` |
| 客户端 | 位置 | **各自本机 localhost**（严禁跨机） |
| 客户端 | 线程数 | 固定 **8**，等于 `VGROUPS` 数 |
| taosd | `supportVnodes` | 256 |
| taosd | `monitor` | 0（关闭监控库自身 IO） |
| taosd | 日志 | `numOfLogLines 100000`、`logKeepDays 1` |

### 3.2 平台侧固定项（各自设置一次）

**Linux**

```bash
cpupower frequency-set -g performance          # 或: echo performance > /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor
swapoff -a                                     # 关闭 swap
sysctl -w vm.swappiness=1
echo madvise > /sys/kernel/mm/transparent_hugepage/enabled
ulimit -n 65535
systemctl stop fstrim.timer 2>/dev/null
```

**Windows**（管理员 PowerShell）

```powershell
powercfg /setactive SCHEME_MIN                 # 高性能
powercfg -h off                                # 关闭休眠，消除 hiberfil IO
Set-Service SysMain -StartupType Disabled; Stop-Service SysMain      # SuperFetch
Set-Service WSearch -StartupType Disabled;  Stop-Service WSearch     # 索引服务
Disable-ScheduledTask -TaskName "\Microsoft\Windows\Defrag\ScheduledDefrag" # 碎片整理
fsutil behavior set DisableLastAccess 1        # 关闭最后访问时间更新
# 页文件：固定大小，放在 C:（不在被测数据盘）
# 系统属性 -> 高级 -> 性能 -> 虚拟内存 -> 自定义大小
Get-Service WinDefend -ErrorAction SilentlyContinue   # 若存在，关闭实时保护并记录
```

> 若 IT 安装的是 Server Core 且没有 Defender / SysMain，记录"不存在"即可，不要伪造。

### 3.3 属于「被测变量」，**不做对齐，只做记录**

这些是 OS 差异的组成部分，正是我们要测的东西：

- 文件系统：ext4（或 IT 选择的 xfs）vs NTFS
- IO 提交模型：Linux `epoll/pread` vs Windows `IOCP/同步 IO`
- 内存管理与页缓存行为
- 线程调度与时钟精度
- TCP 栈（loopback 也走协议栈）

---

## 4. 数据模型与数据集

### 4.1 固定 Schema（智能电表模型）

| 列 | 类型 | 生成范围 |
|---|---|---|
| `ts` | TIMESTAMP | 起始 `2026-01-01 00:00:00.000`，步长 1000 ms |
| `current` | FLOAT | 0 ~ 20 |
| `voltage` | INT | 100 ~ 300 |
| `phase` | FLOAT | 0 ~ 1 |

| 标签 | 类型 | 取值 |
|---|---|---|
| `groupid` | INT | 0 ~ 9999（按表顺序生成，一表一值） |
| `location` | BINARY(24) | 固定字符串 |

### 4.2 规模

- 10,000 子表 × 100,000 行 = **10 亿行**
- 单表时间跨度约 27.8 小时；全表覆盖 2026-01-01 00:00 ~ 2026-01-02 03:46
- **实测落盘 35 GB**（NVMe/ext4；原估 3~10 GB 偏小），原始数据量约 20 GB
- **实测单轮写入 6m45s / 253 万 rows/s**（u126-3：Xeon + NVMe + 8 线程）；机械盘端会明显更慢，需按实测重估窗口

### 4.3 数据一致性策略

两端均用**同一份 JSON、相同参数**各自生成数据，**不使用 CSV 样本文件**。
两端随机数不同，数据集不是字节一致，但表数、行数、列类型、取值范围、时间戳完全一致。

> 报告中必须声明“数据集统计等价、非字节一致”；只比较耗时 / QPS，**不得比较查询结果的绝对值**。

---

## 5. 测试场景矩阵

### 5.1 写入（固定参数，只比两种写入方式）

| ID | 场景 | 写入方式 | 固定参数 | 重复 | 说明 |
|---|---|---|---|---|---|
| W1 | 拼 SQL | `taosc` | 8 线程（= `VGROUPS`），`num_of_records_per_req`=5000 | 3 | 默认路径 |
| W2 | STMT2 | `stmt2` | 8 线程（= `VGROUPS`），`num_of_records_per_req`=5000 | 3 | 参数绑定路径 |

> 除 `insert_mode` 外，两份配置**逐字段完全相同**（已脚本比对：51 个字段仅 `insert_mode` 不同）。
> 线程数固定 **8 = `VGROUPS` 数**：让写入线程与 vgroup 一一对应，避免跨 vgroup 争抢，两端负载形状一致。
> 批大小固定 `num_of_records_per_req`=**5000**，两端一致；命令行不要再加 `-r` 覆盖。
> **两种写入方式都要测**：`taosc` 走「拼 INSERT 语句 → 解析 → 规划 → 执行」完整链路；`stmt2` 走参数绑定，跳过 SQL 解析。两端 OS 在这两条路径上的开销构成不同，只测一种结论会片面。
> 两者都必须用 **Native 驱动**（JSON 模式默认，无 `driver` 字段）；出现 `WS` / `REST` 立即停止。

### 5.2 查询（基于 §4 数据集）

| ID | 场景 | 查询形态 | 配置 |
|---|---|---|---|
| Q1 | 子表逐表范围聚合 | `select count(*), avg(current) from xxxx where ts >= ... and ts < ...`（对每张子表各执行一次，聚合结果仅 1 行） | `super_table_query`，1 线程 |
| Q2 | 超表全局聚合 | `select count(*), avg(current) from meters` | `specified_table_query`，1 线程 |
| Q3 | 时间窗口降采样 | `select _wstart, avg(current) from meters interval(1m)` | `specified_table_query`，1 线程 |
| Q4 | 并发查询 | Q2 + Q3 混合，并发 1 / 2 / 4 / **8**（上限 8） | `specified_table_query`，`threads`=N |
| Q5 | 子表全量查询 | `select * from d0`（固定单表、全列、全时间范围） | `specified_table_query`，独立配置 `query_fullscan.json` |
| Q6 | 超表全量查询 | `select * from meters`（1 万子表全部数据，约 10 亿行回传） | `specified_table_query`，独立配置 `query_stable_fullscan.json`，`query_times=1` |

> **所有查询统一冷口径**：每次测量前**重启机器**，冷启动后只跑一次（`query_times=1`），重复 3 个冷启动取中位数。
> 不区分冷/热两组数据——两端口径一致才有可比性；Windows 无法可靠清页缓存，只能靠重启。
> 并发上限 **8**（不测 16）：与 `VGROUPS` 数一致，避免线程数超过 vgroup 数后变为排队而非并行。

> ⚠️ **Q5 只查 `d0` 一张表**。不要用 `super_table_query`——它会把 SQL 对**全部 10,000 张子表**各执行一遍，单轮就是 10 亿行全列回传，耗时不可接受且无法定位瓶颈。
> ⚠️ 表名固定写 `d0`（子表前缀 `d`，编号从 0 开始），两端目标和数据量完全一致；改动 `childtable_prefix` 时这里要同步改。
> ⚠️ Q5 用 `specified_table_query` 可以拿到**单条 SQL 的 QPS 与延迟分位**，不会与其它场景混算。

> ⚠️ `super_table_query` 的 SQL 里**必须包含 `xxxx`** 占位符（会被替换为子表名），否则报 `sql not found mark string 'xxxx'` 且 `succ queries count is zero`。
> ⚠️ `specified_table_query` 与 `super_table_query` **只能出现一个**。

---

## 6. 指标定义

### 6.1 写入指标（来自 taosBenchmark 输出，实测样例）

```
Spent 0.009987 (real 0.009395) seconds to insert rows: 4000 with 2 thread(s)
  into perf_smoke 400520.68 (real 425758.38) records/second
insert delay, min: 1.9450ms, avg: 2.3487ms, p90: 3.4210ms,
              p95: 3.4210ms, p99: 3.4210ms, max: 3.4210ms
```

**STMT2 多出 `stmt delay1/2/3` 三个分阶段耗时字段：**

```
Spent 0.006034 (real 0.002136) seconds to insert rows: 2000 with 4 thread(s)
  into perfdb 331455.09 (real 936329.59) records/second
  stmt delay1=0.00s delay2=0.00s delay3=0.00s
insert delay, min: 1.5660ms, avg: 2.1368ms, p90: 2.7160ms, ...
```

> `stmt delay1/2/3` 分别对应绑定、执行、收尾阶段；两端对比时这三个值比总吞吐更能定位瓶颈在哪一层。

| 指标 | 含义 | 用途 |
|---|---|---|
| `records/second (real)` | 真实写入吞吐 | **主指标** |
| `insert delay p50/p90/p99` | 单请求延迟分位 | 稳定性 |
| `tables/s` | 建表速度 | 元数据路径对比 |
| data 目录体积 | 压缩后大小 | 压缩比 |
| 写入期间 `iops` / `await` | 磁盘压力 | 归因 |

### 6.2 查询指标（实测样例）

**`specified_table_query`：每条 SQL 单独一行**

```
complete query with 1 threads and 5 sql 1 spend 0.021484s QPS: 232.731
  query delay avg: 0.004030s min: 0.002943s max: 0.007061s
             p90: 0.007061s p95: 0.007061s p99: 0.007061s
  SQL command: select count(*), avg(current) from meters
...
Spend 0.0870 second completed total queries: 15, the QPS of all threads: 172.414
```

**`super_table_query`：只给一行汇总**

```
spend 0.021602s using 4 threads complete query 40 times,
  min delay: 0.001129s, avg delay: 0.001591s, p90: 0.002269s,
  p95: 0.002414s, p99: 0.002582s, max: 0.002582s
Spend 0.0230 second completed total queries: 40, the QPS of all threads: 1739.130
```

| 指标 | 含义 | 用途 |
|---|---|---|
| `QPS` | 总吞吐 | **主指标** |
| `avg / p90 / p99 delay` | 查询延迟 | 稳定性 |
| 扫描行数 | 用 `EXPLAIN ANALYZE` 抽取 | 确认两端处理量一致 |

### 6.3 系统指标

| 平台 | 采集方式 | 采集项 |
|---|---|---|
| Linux | `iostat -x 1`、`pidstat -p <taosd> 1`、`/proc/meminfo` | r/s、w/s、rkB/s、wkB/s、await、%util、CPU%、RSS |
| Windows | `Get-Counter`（见 §7.4） | Disk Reads/sec、Disk Writes/sec、Avg. Disk sec/Transfer、% Idle Time、Processor %、Working Set |

---

## 7. 执行流程

### 7.1 每轮固定步骤（两端完全相同）

**写入轮**

```
1. 停止 taosd
2. 删除 dataDir 与 logDir
3. 启动 taosd，轮询直到可连接（不要用 sleep 猜）
4. 启动 OS 指标采集（后台）
5. 运行 taosBenchmark（insert.json 或 insert_stmt2.json）
6. 停止指标采集，保存原始输出
7. 重复 3 次，取中位数
```

**查询轮（冷口径）**

```
1. 重启机器（两端一致）
2. 开机后轮询 taosd 就绪（taos -s "select server_version()"）
3. 启动 OS 指标采集
4. 运行查询配置（query_times=1，只跑一次）
5. 停止采集，保存输出
6. 重复 3 个冷启动，取中位数
```

> ❗ 冷口径下每个数据点算一次重启：5 个查询场景 × 3 次 × 2 个 OS = **30 次重启**，需提前评估窗口。
> ❗ 写入轮不重启，但每轮必须删干净 dataDir，否则上轮数据会影响压缩率与后续查询基线。
> ❗ 重启后必须回到**同一个 OS**（双系统默认启动项需提前设好），否则会测错系统。

### 7.2 建库（两端用同一段 SQL 预建，benchmark 只写数据）

```sql
DROP DATABASE IF EXISTS perfdb;
CREATE DATABASE perfdb
  PRECISION 'ms'
  VGROUPS 8
  REPLICA 1
  DURATION 10d
  KEEP 3650d
  CACHEMODEL 'none'
  CACHESIZE 1
  BUFFER 256
  PAGES 256
  PAGESIZE 4
  STT_TRIGGER 1
  MINROWS 100
  MAXROWS 4096
  WAL_LEVEL 1;
```

> 3.x 已用 `BUFFER` / `PAGES` / `PAGESIZE` / `CACHESIZE` 取代 2.x 的 `CACHE` / `BLOCKS`。
> 写成 `CACHE 256 BLOCKS 6` 会直接报 `syntax error`（已实测）。
> 建完用 `SHOW CREATE DATABASE perfdb` 回读校验，两端结果必须逐字相同。

> 用预建库 + `"drop": "no"` 的方式，避免 taosBenchmark 用默认参数建库导致两端不一致。

### 7.3 磁盘基线（P6 用）

**Linux**

```bash
dd if=/dev/zero of=/data/base.bin bs=1M count=1024 oflag=direct
dd if=/data/base.bin of=/dev/null bs=1M iflag=direct
dd if=/dev/zero of=/data/sync4k.bin bs=4k count=2000 oflag=dsync
```

**Windows**（PowerShell，1 GB 顺序 + 4K 同步）

```powershell
$dir='D:\'; $p="$dir\base.bin"
$fs=[IO.File]::Open($p,'Create','Write','None'); $b=New-Object byte[] (1MB)
$sw=[Diagnostics.Stopwatch]::StartNew()
for($i=0;$i -lt 1024;$i++){ $fs.Write($b,0,$b.Length) }
$fs.Flush($true); $sw.Stop(); $fs.Close()
"SEQ write 1GB: {0:N2}s -> {1:N2} MB/s" -f $sw.Elapsed.TotalSeconds,(1024/$sw.Elapsed.TotalSeconds)
Remove-Item $p -Force
```

> 判定：两端顺序写差异 > 15% 说明磁盘/分区未对齐，**先查 P1/P2 再测**。

### 7.4 OS 指标采集

**Linux**

```bash
iostat -x 1 > iostat.log 2>&1 &
pidstat -p $(pgrep -x taosd) 1 > pidstat.log 2>&1 &
# 结束后 kill %1 %2
```

**Windows**

```powershell
$c = @(
 '\PhysicalDisk(*)\Disk Reads/sec',
 '\PhysicalDisk(*)\Disk Writes/sec',
 '\PhysicalDisk(*)\Avg. Disk sec/Transfer',
 '\PhysicalDisk(*)\% Idle Time',
 '\Processor(_Total)\% Processor Time',
 '\Process(taosd)\Working Set'
)
Get-Counter -Counter $c -SampleInterval 1 -MaxSamples 3600 |
  Export-Counter -Path counters.blg -Force
```

---

## 8. 配置文件

> ⚠️ **`cfgdir` 两端不同**：Linux 为 `/etc/taos`，Windows 通常为 `C:\\TDengine\\cfg`。
> 这是安装路径差异，不属于被测变量，需在各自的 JSON 中分别填写。

### 8.1 写入（拼 SQL，taosc）：`insert.json`

```json
{
  "filetype": "insert",
  "cfgdir": "/etc/taos",
  "host": "127.0.0.1",
  "port": 6030,
  "user": "root",
  "password": "taosdata",
  "thread_count": 8,
  "create_table_thread_count": 8,
  "num_of_records_per_req": 5000,
  "prepared_rand": 10000,
  "confirm_parameter_prompt": "no",
  "continue_if_fail": "no",
  "databases": [
    {
      "dbinfo": {
        "name": "perfdb",
        "drop": "no",
        "vgroups": 8,
        "precision": "ms",
        "replica": 1
      },
      "super_tables": [
        {
          "name": "meters",
          "child_table_exists": "no",
          "childtable_count": 10000,
          "childtable_prefix": "d",
          "auto_create_table": "yes",
          "batch_create_tbl_num": 100,
          "data_source": "rand",
          "insert_mode": "taosc",
          "insert_rows": 100000,
          "interlace_rows": 0,
          "insert_interval": 0,
          "timestamp_step": 1000,
          "start_timestamp": "2026-01-01T00:00:00.000Z",
          "columns": [
            {"type": "FLOAT", "name": "current", "min": 0,   "max": 20},
            {"type": "INT",   "name": "voltage", "min": 100, "max": 300},
            {"type": "FLOAT", "name": "phase",   "min": 0,   "max": 1}
          ],
          "tags": [
            {"type": "INT",    "name": "groupid",  "min": 0, "max": 9999, "gen": "order"},
            {"type": "BINARY", "name": "location", "len": 24, "values": ["Beijing.Chaoyang"]}
          ]
        }
      ]
    }
  ]
}
```

### 8.2 写入（STMT2）：`insert_stmt2.json`

与 `insert.json` **只有一行不同**：`"insert_mode": "taosc"` → 改为 `"stmt2"`。其余字段（表数、行数、线程、批大小、schema）必须逐字一致，否则两种写入方式的结果不可比。

```json
{
  "filetype": "insert",
  "cfgdir": "/etc/taos",
  "host": "127.0.0.1",
  "port": 6030,
  "user": "root",
  "password": "taosdata",
  "thread_count": 8,
  "create_table_thread_count": 8,
  "num_of_records_per_req": 5000,
  "prepared_rand": 10000,
  "confirm_parameter_prompt": "no",
  "continue_if_fail": "no",
  "databases": [
    {
      "dbinfo": {
        "name": "perfdb",
        "drop": "no",
        "vgroups": 8,
        "precision": "ms",
        "replica": 1
      },
      "super_tables": [
        {
          "name": "meters",
          "child_table_exists": "no",
          "childtable_count": 10000,
          "childtable_prefix": "d",
          "auto_create_table": "yes",
          "batch_create_tbl_num": 100,
          "data_source": "rand",
          "insert_mode": "stmt2",
          "insert_rows": 100000,
          "interlace_rows": 0,
          "insert_interval": 0,
          "timestamp_step": 1000,
          "start_timestamp": "2026-01-01T00:00:00.000Z",
          "columns": [
            {"type": "FLOAT", "name": "current", "min": 0, "max": 20},
            {"type": "INT", "name": "voltage", "min": 100, "max": 300},
            {"type": "FLOAT", "name": "phase", "min": 0, "max": 1}
          ],
          "tags": [
            {"type": "INT", "name": "groupid", "min": 0, "max": 9999, "gen": "order"},
            {"type": "BINARY", "name": "location", "len": 24, "values": ["Beijing.Chaoyang"]}
          ]
        }
      ]
    }
  ]
}
```

> 命令行等价写法：`-I stmt2`（`taosc` 为默认值，不传即可）。但 JSON 模式与命令行参数**互斥**，实际执行只用 JSON。
> 两端启动后确认输出里的 `Connect mode is : Native`，出现 `WS` / `REST` 立即停止。

### 8.3 查询（超表级）：`query_spec.json`

```json
{
  "filetype": "query",
  "cfgdir": "/etc/taos",
  "host": "127.0.0.1",
  "port": 6030,
  "user": "root",
  "password": "taosdata",
  "confirm_parameter_prompt": "no",
  "continue_if_fail": "no",
  "databases": "perfdb",
  "query_times": 1,
  "query_mode": "taosc",
  "specified_table_query": {
    "threads": 1,
    "query_interval": 0,
    "sqls": [
      {"sql": "select count(*), avg(current) from meters"},
      {"sql": "select _wstart, avg(current) from meters interval(1m)"},
      {"sql": "select avg(voltage), max(phase) from meters where ts >= '2026-01-01 00:00:00' and ts < '2026-01-01 00:10:00'"}
    ]
  }
}
```

### 8.4 查询（子表级）：`query_super.json`

```json
{
  "filetype": "query",
  "cfgdir": "/etc/taos",
  "host": "127.0.0.1",
  "port": 6030,
  "user": "root",
  "password": "taosdata",
  "confirm_parameter_prompt": "no",
  "continue_if_fail": "no",
  "databases": "perfdb",
  "query_times": 1,
  "query_mode": "taosc",
  "super_table_query": {
    "stblname": "meters",
    "threads": 4,
    "query_interval": 0,
    "sqls": [
      {"sql": "select count(*), avg(current) from xxxx"},
      {"sql": "select last_row(ts), last_row(current) from xxxx"}
    ]
  }
}
```

### 8.5 查询（子表全量）：`query_fullscan.json`

```json
{
  "filetype": "query",
  "cfgdir": "/etc/taos",
  "host": "127.0.0.1",
  "port": 6030,
  "user": "root",
  "password": "taosdata",
  "confirm_parameter_prompt": "no",
  "continue_if_fail": "no",
  "databases": "perfdb",
  "query_times": 1,
  "query_mode": "taosc",
  "specified_table_query": {
    "threads": 1,
    "query_interval": 0,
    "sqls": [
      {"sql": "select * from d0"}
    ]
  }
}
```

> 只针对 `d0`：10 万行 × 全列，测的是**单表全扫 + 序列化 + 结果传输**。
> `query_times=1`：冷口径下只允许跑一次；重复测量靠**重启机器**实现（见 §7.1）。

---

## 9. 结果输出

### 9.1 汇总表模板

| 场景 | Linux | Windows | 比值(W/L) | 备注 |
|---|---|---|---|---|
| W1 拼SQL 写入吞吐 (rows/s) | | | | 主指标 |
| W2 STMT2 写入吞吐 (rows/s) | | | | 主指标 |
| W1 vs W2 吞吐差 (%) | | | | 两种写入路径差异 |
| W1 insert delay p99 (ms) | | | | |
| 压缩后体积 (MB) | | | | |
| Q2 QPS | | | | |
| Q3 QPS | | | | |
| Q4 并发 8 QPS | | | | |
| Q5 单表全量扫描 QPS | | | | 固定 `d0` |

### 9.2 每次运行必须落盘的原始材料

```
results/
  <os>/
    W1_run1/{insert.json, bm_output.txt, iostat.log, taosd.log}
    W1_run2/...
    ...
  fingerprint.txt      # taosd -V / lscpu|CPU 信息 / df / taos.cfg 全文
  disk_baseline.txt    # §7.3 结果
```

`fingerprint.txt` 是结论可信度的前提，缺它就不出报告。

---

## 10. 判定标准与已知风险

### 10.1 判定

| 差异幅度 | 结论表述 |
|---|---|
| < 5% | 无显著差异 |
| 5% ~ 15% | 轻微差异，需看 p99 与 IO 指标确认 |
| > 15% | 显著差异，必须给出归因（CPU / IO / 内存 / fsync） |

### 10.2 风险清单

| # | 风险 | 影响 | 缓解 |
|---|---|---|---|
| R1 | **Windows 无法可靠清页缓存** | 已统一为“重启机器”，不再依赖清缓存 | 已消除：两端均重启，冷口径一致 |
| R2 | 两端文件系统不同（NTFS vs ext4） | 差异含 FS 因素 | 属 OS 一部分，报告中显式声明，不试图消除 |
| R3 | 双系统共用 NVMe，容量切分不同 | 可用空间/几何差异 | P1/P2 校验；报告记录分区大小 |
| R4 | Windows 后台服务（Defender/索引/更新） | 抖动 | §3.2 全部关闭并记录 |
| R5 | 数据集非字节一致 | 查询结果绝对值不可比 | 已接受：只比耗时/QPS，报告声明“统计等价” |
| R6 | `records_per_req` 与 `-r` 混用 | 两端批次不同 | 只用 JSON 的 `num_of_records_per_req`，命令行不传 `-r` |
| R7 | taosd 日志写放大 | 干扰主指标 | 统一 `numOfLogLines` / `logKeepDays` |
| R8 | Q5 页缓存会掩盖磁盘差异（单表仅 10 万行） | 重复跑时退化为纯 CPU/传输测试 | 已采用冷口径：每次重启后只跑一次 |
| R9 | 10 亿行规模下单轮耗时长（20~40 min） | 测试窗口不够时容易半途中断 | 先跑 1/10 规模冒烟验证流程，再上全量；中断后需从清理步骤重跑 |

---

## 11. 交付物

1. 本方案（Review 通过后冻结为 v1.0）
2. `insert.json` / `insert_stmt2.json` / `query_spec.json` / `query_super.json` / `query_fullscan.json`（纳入版本管理）
3. 执行脚本：`run_linux.sh`、`run_windows.ps1`（封装 §7.1 的 8 步）
4. `results/` 全量原始数据 + `fingerprint.txt`
5. 对比报告（含 §9.1 汇总表 + 归因分析）

---

## 12. 决策记录与剩余待确认

### 12.1 已确认（2026-11-06）

| # | 决策 |
|---|---|
| 1 | 数据集 **10 亿行**（10,000 表 × 100,000 行），接受单轮 20~40 min |
| 2 | **不使用 CSV 样本**：两端同参 JSON 生成，报告声明“统计等价、非字节一致” |
| 3 | 查询**统一冷口径**（每轮重启机器），不再区分冷/热 |
| 4 | **不做混合读写**场景（列入 §1.3 范围外） |
| 5 | 并发查询上限 **8**（= `VGROUPS`），不测 16 |
| 6 | 写入只做 **W1 `taosc` + W2 `stmt2`**，线程 8、批 5000、VGROUPS 8 全部固定 |
| 7 | TDengine 固定 **v3.4.3.9** 官方安装包（NAS，见 §2.1） |

### 12.2 剩余待确认（阻塞项）

| # | 问题 | 影响 |
|---|---|---|
| ❓1 | IT 装完双系统后，能否把**默认启动项固定为被测系统**（或提供选择菜单）？ | 冷口径需要 30 次重启；若每次默认进 Windows，测 Linux 时每轮都要人工在开机菜单选一次，无法无人值守 |
| ❓2 | 重启后由谁确认系统回到同一 OS 且 SSH 可达？ | 我只能 SSH 到 Windows；重启后若进 Linux 且未配 SSH，链路直接中断 |
