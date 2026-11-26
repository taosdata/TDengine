# TDengine TSDB 性能测试报告 — Windows

> 生成时间：2026-11-11 13:34:44  
> 测试窗口：2026-11-11 10:42:44 → 2026-11-11 11:12:37  
> 采集脚本：`script/main.py`

## 1. 结论摘要

- 本次未执行写入阶段（`--only queries`）。
- 查询共 5 项：最快 Q2（1.6 s），最慢 Q5（1,471.2 s）。
- 逐项指标见第 4 节，资源占用见第 5 节，口径见第 6 节。

## 2. 测试环境

| 项目 | 取值 |
|---|---|
| 操作系统 | Microsoft Windows Server 2019 Standard |
| 内核/构建 | build 17763 |
| CPU | Intel(R) Core(TM) i7-10700 CPU @ 2.90GHz（16 逻辑核） |
| 内存 | 79.7 GB |
| 数据盘 | WDC WD20EZAZ-00L9GB0 / 1863 GB |
| TDengine 版本 | 3.4.3.9 |
| taosBenchmark | 3.4.3.9 |
| 安装包 | （未安装，复用已装版本） |
| 数据目录 | C:\TDengine\data |
| CPU 调频策略 | 平衡 |
| 开机时长 | boot 2026-11-10 11:32:54 |

## 3. 写入性能

写入共 4 轮：W1（拼接 SQL，`taosc`）与 W2（参数绑定，`stmt2`）各跑冷启动与热缓存。每轮 DROP + CREATE 数据库后写入 10 亿行（10000 子表 × 100000 行，8 线程，每请求 5000 行）。

| 轮次 | 写入方式 | 缓存 | 吞吐 (rows/s) | wall (s) | p90 | p99 | taosd CPU | 客户端 CPU | taosd RSS | 磁盘写 |
|---|---|---|---|---|---|---|---|---|---|---|

## 4. 查询性能

所有查询在最后一份写入数据上执行（单一热数据集），客户端与服务端同机，native 连接。

| 编号 | SQL | 线程 | 执行次数 | 单次均值 | QPS | p90 | p99 | 进程总耗时 |
|---|---|---|---|---|---|---|---|---|
| Q1 | `select * from meters` | 1 | 3 | 95,040.72 ms | 0.0 | 97,849.54 ms | 97,849.54 ms | 285.40 s |
| Q2 | `select count(*) from meters` | 1 | 20 | 72.92 ms | 13.5 | 82.66 ms | 83.78 ms | 1.60 s |
| Q3 | `select last(*),tbname from meters group by tbname` | 1 | 20 | 150.68 ms | 6.6 | 158.58 ms | 159.25 ms | 3.20 s |
| Q4 | `select count(*) from meters interval(1m)` | 1 | 3 | 1,210.40 ms | 0.8 | 1,452.34 ms | 1,452.34 ms | 3.80 s |
| Q5 | `select * from meters` | 8 | 3 | 490,350.27 ms | 0.0 | 616,485.33 ms | 642,846.03 ms | 1,471.20 s |

## 5. 资源占用

| 轮次 | taosd CPU | 客户端 CPU | RSS 均值 | RSS 峰值 | 线程数 | 磁盘 IOPS | 磁盘吞吐 | 采样点 |
|---|---|---|---|---|---|---|---|---|
| Q1 | 1.59 核 | 0.63 核 | 2,116 MB | 2,158 MB | 207 | 6 | 0.0 MB/s | 142 |
| Q2 | — | — | 2,053 MB | 2,053 MB | 207 | — | — | 1 |
| Q3 | 4.54 核 | — | 2,069 MB | 2,069 MB | 207 | 5 | 0.1 MB/s | 2 |
| Q4 | 5.78 核 | 0.00 核 | 2,077 MB | 2,091 MB | 207 | 21 | 0.2 MB/s | 3 |
| Q5 | 9.85 核 | 2.66 核 | 2,721 MB | 3,059 MB | 213 | 5 | 0.0 MB/s | 727 |

## 6. 口径与偏差说明

- **冷启动**：重启 taosd 并清空页缓存（Linux 写入 `/proc/sys/vm/drop_caches`；Windows 无等价机制，仅重启服务，页缓存由系统自行回收）。
- **热缓存**：不做任何清理，紧接冷启动轮次执行，此时上一轮刚写入的数据仍在页缓存中。
- **查询**：仅在最后一份热数据上执行一次，不区分冷热（按需求"只要一份热数据"）。
- **吞吐**：同时给出 bench 自报口径与 real 口径，real 为端到端真实耗时换算。
- **CPU**：Linux 取 `/proc/<pid>/stat` 前后差值除以墙钟；Windows 取 typeperf `Process\% Processor Time` 均值（单核为 100%，可超 100%）换算为核数。两者均 2 s 采样。
- **磁盘**：Linux 取 `/proc/diskstats` 整盘计数（含所有进程）；Windows 取承载数据目录的 `PhysicalDisk` 实例（如 `0 C: F:`），口径基本一致。
- **注意**：Windows 无官方页缓存丢弃接口，冷启动轮次的"冷"程度弱于 Linux，读取侧指标尤其应结合热缓存轮次一并解读。

## 7. 与对照平台的对比

对照平台：**Linux**（TDengine 3.4.3.9，Ubuntu 24.04.5 LTS）。比值 = 本平台 / 对照平台，>1 表示本平台更好。

### 7.2 查询单次均值

| 编号 | SQL | Windows | Linux | 快慢比 |
|---|---|---|---|---|
| Q1 | `select * from meters` | 95,040.72 ms | 92,818.44 ms | 0.98× |
| Q2 | `select count(*) from meters` | 72.92 ms | 55.01 ms | 0.75× |
| Q3 | `select last(*),tbname from meters group by tbname` | 150.68 ms | 96.85 ms | 0.64× |
| Q4 | `select count(*) from meters interval(1m)` | 1,210.40 ms | 1,028.08 ms | 0.85× |
| Q5 | `select * from meters` | 490,350.27 ms | 341,832.40 ms | 0.70× |

> 「快慢比」= 对照平台耗时 / 本平台耗时，>1 表示本平台更快。

