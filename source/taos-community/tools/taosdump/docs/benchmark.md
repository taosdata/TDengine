# taosBackup vs taosdump 性能基准测试报告

**测试日期**：2026-03-29  
**分支**：`feat/taosBackup`  
**版本**：taosBackup 3.4.1.0.alpha / taosdump（系统安装版）

---

## 1. 测试环境

| 项目 | 配置 |
|------|------|
| 服务器 | 12 核 CPU，62 GiB RAM，Linux 5.15.0-130-generic |
| TDengine | 3.4.1.0.alpha.community |
| 计时工具 | `/usr/bin/time -v`（wall / User CPU / Sys CPU） |

**数据集**：智能电表数据，10,000 子表 × 10,000 行 = **1 亿行**，vgroups=8

---

## 2. 备份结果

### 2.1 性能

| 工具 | 格式 | 线程 | 耗时 (wall) | User (s) | Sys (s) | 文件大小 | 吞吐（行/s） |
|------|------|:----:|:-----------:|:--------:|:-------:|:--------:|:------------:|
| taosBackup | binary  | T1 | **1 min 22 s** | 33.6  | 9.6  | 509 MB | 1,225,000 |
| taosBackup | binary  | T8 | **28 s**       | 31.8  | 9.1  | 509 MB | 3,623,000 |
| taosBackup | parquet | T1 | **4 min 12 s** | 202.1 | 12.8 | 548 MB | 397,000 |
| taosBackup | parquet | T8 | **51 s**       | 212.4 | 12.6 | 548 MB | 1,967,000 |
| taosdump   | avro    | T1 | **9 min 51 s** | 342.5 | 33.3 | 1.2 GB | 169,000 |
| taosdump   | avro    | T8 | **1 min 28 s** | 371.6 | 32.1 | 1.2 GB | 1,134,000 |

### 2.2 文件体积

| 工具 | 格式 | 文件大小 | 占比 |
|------|------|---------|------|
| taosdump   | avro     | 1.2 GB | 100%（基准） |
| taosBackup | binary  | 509 MB | 42% |
| taosBackup | parquet | 548 MB | 46% |

---

## 3. 恢复结果（STMT2）

| 工具 | 格式 | 线程 | 耗时 (wall) | User (s) | Sys (s) | 吞吐（行/s） |
|------|------|:----:|:-----------:|:--------:|:-------:|:------------:|
| taosBackup | binary  | T1 | **1 min 3 s**  | 32.8  | 5.1  | 1,582,000 |
| taosBackup | binary  | T8 | **29 s**       | 33.3  | 4.9  | 3,435,000 |
| taosBackup | parquet | T1 | **2 min 32 s** | 114.2 | 10.5 | 659,000 |
| taosBackup | parquet | T8 | **36 s**       | 114.5 | 10.2 | 2,746,000 |
| taosdump   | avro    | T1 | **6 min 3 s**  | 277.2 | 14.9 | 275,000 |
| taosdump   | avro    | T8 | **1 min 17 s** | 381.4 | 51.2 | 1,304,000 |

---

## 4. 分析

### 4.1 taosBackup 快于 taosdump 的根本原因

**瓶颈在序列化**。taosdump 必须将每条记录的每个字段转为文本（SQL INSERT 语句）：1 亿行 × 4 字段 ≈ 4 亿次浮点/整数→字符串转换，T1 备份消耗了 342 s User CPU，这是主要开销。

taosBackup 通过 Raw Block API 直接读取 TDengine 内部的列式压缩块并原样写入，**全程无序列化**。对比 T8 备份的 User CPU：taosBackup binary 仅 31.8 s，taosdump 高达 371.6 s——**taosdump 消耗了 taosBackup 约 11 倍的 CPU**，这些全部花在把每行每列的数值转换为文本字符串上。文件体积因此也缩小到 taosdump SQL 备份的 42%（509 MB vs 1.2 GB），I/O 量同步减少。

### 4.2 T1 差距 7×，T8 缩到 3×

两者瓶颈不同，决定了多线程的收益差异：

| 工具/格式 | T1 备份 | T8 备份 | T8 加速比 | 瓶颈 |
|---|:---:|:---:|:---:|---|
| taosBackup binary  | 82 s  | 28 s | 2.9× | I/O（CPU 很闲，等磁盘） |
| taosBackup parquet | 252 s | 51 s | 4.9× | CPU（Parquet 编码） |
| taosdump SQL       | 591 s | 88 s | 6.7× | CPU（大量文本序列化） |

taosBackup binary T1 已是 I/O 瓶颈，T8 加速有限（2.9×）；taosdump T1 是 CPU 瓶颈，T8 多核并行收益极大（6.7×）。T8 对 taosdump 的"补贴"更多，差距因此从 **7.2× 收窄到 3.1×**。剩余 3× 来自文件体积本就少 57%，是架构差距，加线程无法消除。

> **单线程竞争力**：taosBackup binary T1（82 s）≈ taosdump T8（88 s），单线程匹敌对方 8 线程。

### 4.3 其他值得关注的点

- **taosdump T8 恢复 Sys CPU 异常飙升**：T1 Sys=14.9 s → T8 Sys=51.2 s（增加 3.4×）。8 线程并发解析文本备份文件并拼接 INSERT，大量并发内存分配导致内核态开销膨胀；taosBackup 同场景下 Sys CPU 几乎不变（5.1→4.9 s）。
- **binary T8 的 User CPU 与 T1 几乎相同**（备份 33.6→31.8 s，恢复 32.8→33.3 s）：说明 binary 格式几乎没有计算量，多线程只是加快 I/O 并发，不产生额外 CPU 开销。
- **parquet 恢复快于备份**（T8：36 s vs 51 s）：Parquet 解码比编码计算量更小，方向不对称。
- **binary 备份与恢复近乎对称**（T8：28/29 s，User CPU ~32 s）：两侧瓶颈均是 vnode 读写吞吐，数据路径等长。

---

## 5. 结论

| 对比 | 备份加速比 | 恢复加速比 |
|------|:---:|:---:|
| taosBackup binary T1 vs taosdump T1 | **7.2×** | **5.8×** |
| taosBackup binary T8 vs taosdump T8 | **3.1×** | **2.6×** |
| taosBackup binary T8 vs taosdump T1 | **21×**  | **12.5×** |

1. **速度领先源于架构**：Raw Block 直通，跳过文本序列化，T8 备份 User CPU 仅 31.8 s vs taosdump 371.6 s（相差 11 倍）；备份文件 509 MB vs 1.2 GB，体积减少 57%。
2. **T1 下差距最大（7×）**：taosBackup 已是 I/O 瓶颈，taosdump 仍是 CPU 瓶颈，两者瓶颈类型不同，差距最显著。
3. **T8 缩窄到 3×**：taosdump 多线程并行解决了 CPU 瓶颈（6.7× 加速），而 taosBackup 受 I/O 制约收益有限（2.9× 加速）。剩余 3× 是文件体积更小带来的 I/O 量差距，无法通过加线程消除。
4. **CPU 友好**：binary T8 备份 User CPU 31.8 s，taosdump 371.6 s，相差 11 倍；对在线业务 CPU 资源的竞争影响小得多。
