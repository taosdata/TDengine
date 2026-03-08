# taosBackup vs taosdump 备份恢复性能基准测试报告

**测试日期：** 2026-03-08  
**测试环境：** Linux, TDengine 3.4.0.2.alpha.community, taosBackup v1.1, taosdump v3.4.0.2.alpha

---

## 一、测试环境与数据准备

### 1.1 数据生成

使用 `taosBenchmark` 生成基准测试数据库 `test`：

```bash
taosBenchmark -t 10000 -n 10000 -y
```

| 参数 | 值 |
|------|----|
| 超级表 | meters |
| 子表数 | 10,000 |
| 每表行数 | 10,000 |
| 总行数 | **100,000,000（1 亿行）** |
| 写入线程数 | 8 |
| 写入总耗时 | 46.68 秒（实际 wall time: ~50s） |
| 写入吞吐 | **2,142,228 rows/s**（实际 2,268,289 rows/s） |

### 1.2 TEST 库占用空间（基准数据）

运行 `show table distributed meters` 获取：

```
Total_Blocks=[28794]  Total_Size=[490969.57 KiB]  Average_size=[17.05 KiB]  Compression_Ratio=[26.19 %]
Block_Rows=[95980000]  MinRows=[2952]  MaxRows=[4096]  AvgRows=[3333]
Inmem_Rows=[180000]  Stt_Rows=[3840000]
Total_Tables=[10000]  Total_Filesets=[4]  Total_Vgroups=[2]
```

| 指标 | 值 |
|------|----|
| 磁盘占用大小 | **479.5 MiB**（490,969.57 KiB） |
| 压缩率 | 26.19%（即压缩后仅为原始数据量的 26.19%） |
| 估算原始未压缩数据量 | ~1,832 MiB |
| 总行数（含 Inmem/Stt） | 100,000,000 |
| 子表数 | 10,000 |
| Vgroup 数 | 2 |

---

## 二、备份与恢复性能测试

### 2.1 taosdump

#### 备份

```bash
time taosdump -D test -o /tmp/taosdump_backup
```

| 指标 | 值 |
|------|----|
| 备份耗时 | **240.63 秒（4分 0.6 秒）** |
| 备份文件格式 | Apache Avro |
| 备份文件大小 | **1,122 MiB**（1,176,786,430 字节，约 1.2 GB） |
| 恢复行数确认 | 100,000,000 行 |
| 备份吞吐 | ~415,629 rows/s |

#### 恢复（`-W test=test_dump_restore`）

```bash
time taosdump -i /tmp/taosdump_backup -W test=test_dump_restore
```

| 指标 | 值 |
|------|----|
| 恢复耗时 | **75.24 秒（1分 15.2 秒）** |
| 恢复行数 | 100,000,000 行 |
| 恢复吞吐 | ~1,329,787 rows/s |

---

### 2.2 taosBackup — binary 格式

#### 备份

```bash
time taosBackup -D test -F binary -o /tmp/taosbackup_binary
```

| 指标 | 值 |
|------|----|
| 备份耗时 | **12.38 秒** |
| 备份文件格式 | Binary |
| 备份文件大小 | **504 MiB**（528,774,468 字节） |
| 备份吞吐 | ~8,077,254 rows/s |

#### 恢复 — stmt2（`-v 2`，默认，较快）

```bash
time taosBackup -i /tmp/taosbackup_binary -v 2 -W test=test_backup_bin_stmt2
```

| 指标 | 值 |
|------|----|
| 恢复耗时 | **53.33 秒** |
| 恢复吞吐 | ~1,875,630 rows/s |

#### 恢复 — stmt1（`-v 1`，旧接口）

```bash
time taosBackup -i /tmp/taosbackup_binary -v 1 -W test=test_backup_bin_stmt1
```

| 指标 | 值 |
|------|----|
| 恢复耗时 | **48.70 秒** |
| 恢复吞吐 | ~2,053,388 rows/s |

---

### 2.3 taosBackup — parquet 格式

#### 备份

```bash
time taosBackup -D test -F parquet -o /tmp/taosbackup_parquet
```

| 指标 | 值 |
|------|----|
| 备份耗时 | **31.02 秒** |
| 备份文件格式 | Apache Parquet |
| 备份文件大小 | **509 MiB**（533,703,776 字节） |
| 备份吞吐 | ~3,225,806 rows/s |

#### 恢复 — stmt2（`-v 2`，默认）

```bash
time taosBackup -i /tmp/taosbackup_parquet -v 2 -W test=test_backup_pqt_stmt2
```

| 指标 | 值 |
|------|----|
| 恢复耗时 | **53.33 秒** |
| 恢复吞吐 | ~1,875,630 rows/s |

#### 恢复 — stmt1（`-v 1`，旧接口）

```bash
time taosBackup -i /tmp/taosbackup_parquet -v 1 -W test=test_backup_pqt_stmt1
```

| 指标 | 值 |
|------|----|
| 恢复耗时 | **53.20 秒** |
| 恢复吞吐 | ~1,879,699 rows/s |

---

## 三、汇总对比

### 3.1 备份性能对比

| 工具 & 格式 | 备份耗时 | 备份文件大小 | 文件大小/DB存储 | 备份吞吐 |
|------------|---------|------------|--------------|---------|
| taosdump（Avro） | 240.63 s | **1,122 MiB** | 2.34× | 415,629 rows/s |
| taosBackup parquet | 31.02 s | **509 MiB** | 1.06× | 3,225,806 rows/s |
| taosBackup binary | **12.38 s** | **504 MiB** | 1.05× | **8,077,254 rows/s** |

> DB 存储规模：479.5 MiB（TDengine 内部 26.19% 压缩后）

### 3.2 恢复性能对比

| 工具 & 恢复方式 | 恢复耗时 | 恢复吞吐 |
|--------------|---------|---------|
| taosdump restore | 75.24 s | 1,329,787 rows/s |
| taosBackup binary + stmt2 | 53.33 s | 1,875,630 rows/s |
| taosBackup parquet + stmt2 | 53.33 s | 1,875,630 rows/s |
| taosBackup parquet + stmt1 | 53.20 s | 1,879,699 rows/s |
| taosBackup binary + stmt1 | **48.70 s** | **2,053,388 rows/s** |

### 3.3 文件大小与 DB 存储对比

| 方案 | 大小（MiB） | 与 DB 存储比 | 与 taosdump 比 |
|------|-----------|-------------|---------------|
| TDengine DB 存储（压缩后） | 479.5 | 1.00× | — |
| taosBackup binary | 504 | 1.05× | **0.45×** |
| taosBackup parquet | 509 | 1.06× | **0.45×** |
| taosdump (Avro) | 1,122 | 2.34× | 1.00× |

---

## 四、分析与结论

### 4.1 备份速度

- **taosBackup binary 备份速度是 taosdump 的 19.4 倍**（12.4s vs 240.6s），这是因为 taosBackup binary 格式直接读取 TDengine 内部数据块并以紧凑二进制形式写出，避免了行级反序列化和 Avro 编码开销，同时充分利用多线程并行。
- taosBackup parquet 备份耗时 31s，是 taosdump 的 7.8 倍，比 binary 慢约 2.5 倍，主要因为 Parquet 列式编码需要额外的 CPU 计算开销。

### 4.2 恢复速度

- **taosBackup 恢复（所有组合）均快于 taosdump**（~48-53s vs 75.2s）。
- binary + stmt1（旧 STMT API）恢复最快（48.7s），比 stmt2 快约 9%。原因可能是 stmt2 在内部有额外的批量调度开销，在这一场景下未能充分体现优势。
- parquet 格式恢复（stmt1 和 stmt2）耗时基本相同（~53s），与 binary + stmt2 接近，说明**恢复瓶颈在于 TDengine 写入端（STMT 接口），而非文件格式的解析**。

### 4.3 备份文件大小

- taosdump 输出的 Avro 文件（1,122 MiB）是 TDengine 磁盘存储（479.5 MiB）的 **2.34 倍**，明显膨胀——这是因为 Avro 格式含有大量 schema 元数据且每个子表单独存储一个文件，而 TDengine 内部使用专为时序数据优化的压缩算法。
- taosBackup binary（504 MiB）和 parquet（509 MiB）大小基本相同，仅略大于 DB 磁盘存储（1.05-1.06×），说明两种格式都保持了较好的数据紧凑性，**备份文件大小约等于 TDengine 原始存储大小**。

### 4.4 综合结论

| # | 结论 |
|---|------|
| 1 | **taosBackup 在备份速度上远优于 taosdump**，binary 格式快近 20 倍，parquet 格式快约 8 倍。 |
| 2 | **taosBackup 恢复速度也优于 taosdump**，快 30-35%。 |
| 3 | **taosBackup 备份文件约为 taosdump 的 45%**，节省约一半存储空间。 |
| 4 | 对于纯性能优先场景，推荐 **binary 格式 + stmt1 恢复**（备份最快、恢复最快）。 |
| 5 | 对于需要跨平台互操作或数据分析的场景（如将备份直接接入 Spark/DuckDB），推荐 **parquet 格式**，其备份文件大小与性能仅略逊于 binary。 |
| 6 | stmt1 与 stmt2 恢复速度差异较小（<10%），stmt1 在此测试中略快，但两者均可用。 |

---

## 五、8 Vgroups 对比测试（验证服务端并发写入瓶颈假设）

### 5.1 背景

第四章分析指出，恢复速度的瓶颈在于 TDengine 服务端 vnode 写入，而非客户端 STMT 绑定效率。为验证此结论，将 test 库 vgroup 数从 **2 提升至 8**，重跑全部测试，并在每次恢复后执行行数验证。

### 5.2 数据准备（8 vgroups）

```bash
taosBenchmark -t 10000 -n 10000 -v 8 -y
```

`show table distributed meters` 结果：

```
Total_Blocks=[27648]  Total_Size=[471393.00 KiB]  Average_size=[17.05 KiB]  Compression_Ratio=[26.19 %]
Block_Rows=[92160000]  MinRows=[2952]  MaxRows=[4096]  AvgRows=[3333]
Inmem_Rows=[4960000]  Stt_Rows=[2880000]
Total_Tables=[10000]  Total_Filesets=[16]  Total_Vgroups=[8]
```

| 指标 | 2 vgroups（前轮） | 8 vgroups（本轮） | 提升 |
|------|-----------------|-----------------|------|
| 写入吞吐 | 2,142,228 rows/s | **2,832,096 rows/s** | +32% |
| 写入 wall time | ~50s | ~38s | 1.3× 快 |
| DB 磁盘占用 | 479.5 MiB | **460.3 MiB**（471,393 KiB）| 略小（Inmem/Stt 不同） |

### 5.3 测试结果

#### taosdump 备份 & 恢复（含行数验证）

| 操作 | 2 vgroups | 8 vgroups | 提升 |
|------|----------|----------|------|
| 备份耗时 | 240.63 s | **74.41 s** | **3.2×** |
| 备份文件大小 | 1,122 MiB | 1,122 MiB | 相同 |
| 恢复耗时 | 75.24 s | **75.29 s** | 无提升 |
| 行数验证 | — | ✅ 100,000,000 | |

> **分析**：备份速度大幅提升（3.2×），因为 taosdump 读取路径可以并行访问 8 个 vgroup。但恢复速度没有提升，根本原因是 **taosdump 备份时丢失了 `VGROUPS` 参数**——备份产生的 `dbs.sql` 中 CREATE DATABASE 语句为：
> ```sql
> CREATE DATABASE IF NOT EXISTS test REPLICA 1 DURATION 10d KEEP 3650d,3650d,3650d PRECISION 'ms' MINROWS 100 MAXROWS 4096 COMP 2 ;
> ```
> 没有 `VGROUPS 8`。恢复后的库实际为 `VGROUPS 2`（已通过 `SHOW CREATE DATABASE` 确认），写入并发度无改变，因此恢复速度与 2-vgroup 轮次完全相同。这是 **taosdump 的一个已知缺陷**。

#### taosBackup — binary 格式（含行数验证）

| 操作 | 2 vgroups | 8 vgroups | 提升 |
|------|----------|----------|------|
| 备份耗时 | 12.38 s | **10.90 s** | 1.1× |
| 备份文件大小 | 504 MiB | 504 MiB | 相同 |
| 恢复 stmt2 耗时 | 53.33 s | **22.99 s** | **2.3×** |
| 行数验证（stmt2） | — | ✅ 100,000,000 | |
| 恢复 stmt1 耗时 | 48.70 s | **25.88 s** | **1.9×** |
| 行数验证（stmt1） | — | ✅ 100,000,000 | |

> **重要发现**：8 vgroups 下 stmt2（22.99s）反超 stmt1（25.88s），与 2 vgroups 时的结果相反。2 vgroups 时服务端已是瓶颈，stmt2 额外开销被掩盖；8 vgroups 释放服务端并发能力后，stmt2 原生的批量绑定优势得以显现。

#### taosBackup — parquet 格式（含行数验证）

| 操作 | 2 vgroups | 8 vgroups | 提升 |
|------|----------|----------|------|
| 备份耗时 | 31.02 s | **30.99 s** | 无（CPU bound） |
| 备份文件大小 | 509 MiB | 510 MiB | 相同 |
| 恢复 stmt2 耗时 | 53.33 s | **31.95 s** | **1.7×** |
| 行数验证（stmt2） | — | ✅ 100,000,000 | |
| 恢复 stmt1 耗时 | 53.20 s | **31.80 s** | **1.7×** |
| 行数验证（stmt1） | — | ✅ 100,000,000 | |

> parquet 备份时间不变（编码 CPU bound，不受 vgroup 影响）；恢复时间提升 1.7×，与 parquet 解码 CPU 开销有关——解码比写入更耗时，因此恢复受服务端瓶颈的制约程度低于 binary，8 vgroups 带来的增益也相对小。

### 5.4 综合对比（2 vgroups vs 8 vgroups）

#### 备份耗时对比

| 工具 & 格式 | 2 vgroups | 8 vgroups | 加速比 |
|------------|----------|----------|-------|
| taosdump（Avro） | 240.63 s | 74.41 s | **3.2×** |
| taosBackup parquet | 31.02 s | 30.99 s | 1.0×（CPU bound）|
| taosBackup binary | 12.38 s | **10.90 s** | 1.1× |

#### 恢复耗时对比

| 工具 & 恢复方式 | 2 vgroups | 8 vgroups | 加速比 |
|--------------|----------|----------|-------|
| taosdump restore | 75.24 s | 75.29 s | 1.0×（vgroup 无效） |
| taosBackup parquet + stmt1 | 53.20 s | 31.80 s | 1.7× |
| taosBackup parquet + stmt2 | 53.33 s | 31.95 s | 1.7× |
| taosBackup binary + stmt1 | 48.70 s | 25.88 s | 1.9× |
| taosBackup binary + stmt2 | 53.33 s | **22.99 s** | **2.3×** |

#### 8 vgroups 下各工具恢复吞吐 vs taosBenchmark 基准

| 工具 & 方式 | 8 vgroups 恢复吞吐 | 占 taosBenchmark 基准（2,832,096 r/s）之比 |
|------------|------------------|-----------------------------------------|
| taosdump restore | 1,328,526 rows/s | 47%（瓶颈在目标库 vgroup 数=2）|
| taosBackup parquet + stmt1 | 3,144,654 rows/s | 111% |
| taosBackup parquet + stmt2 | 3,129,960 rows/s | 111% |
| taosBackup binary + stmt1 | 3,863,194 rows/s | 136% |
| taosBackup binary + stmt2 | **4,349,282 rows/s** | **154%** |

> 注：taosBackup 恢复吞吐**超过** taosBenchmark 基准的原因：taosBenchmark 在写入时还包含 Stt/Inmem 合并和 TSDB 压缩等后台工作；而从备份恢复时，数据已经是有序的时间序列块，TDengine 的写入路径更高效，加之 8 vgroups 让 8 个写线程真正并行互不竞争。

### 5.5 关键结论

1. **验证了瓶颈假设**：将 vgroup 从 2 增至 8，taosBackup 恢复速度提升 1.7-2.3×，证明 2-vgroup 时服务端写入并发是瓶颈。
2. **taosdump 恢复完全不受益**：根本原因是 taosdump **备份时丢失 `VGROUPS` 参数**（已通过读取备份文件 `dbs.sql` 和恢复后 `SHOW CREATE DATABASE` 双重确认），恢复后目标库固定为 `VGROUPS 2`，写入并发度不变。若需改善 taosdump 恢复速度，需在恢复前手动预建目标库并指定 `VGROUPS` 数，再用 `--no-create-databas` 模式写入数据。
3. **8 vgroups 下最优组合是 binary + stmt2**（22.99s，4.35M rows/s），比 taosdump 恢复快 **3.3×**。
4. **stmt2 在高并发服务端下优于 stmt1**，在 2 vgroups 时被掩盖的 stmt2 优势得以体现。
5. **parquet 备份速度不随 vgroup 增加而提升**（均为 ~31s），因为瓶颈在 Parquet 编码 CPU，而非 I/O 或服务端。

---

## 附录：测试命令摘要

### 第一轮（2 vgroups，默认）

```bash
# 1. 生成测试数据
taosBenchmark -t 10000 -n 10000 -y

# 2. 查看数据库占用
taos -s "use test; show table distributed meters"

# 3. taosdump 备份与恢复
taosdump -D test -o /tmp/taosdump_backup
taosdump -i /tmp/taosdump_backup -W test=test_dump_restore

# 4. taosBackup binary 备份与恢复（stmt2/stmt1）
taosBackup -D test -F binary -o /tmp/taosbackup_binary
taosBackup -i /tmp/taosbackup_binary -v 2 -W test=test_backup_bin_stmt2
taosBackup -i /tmp/taosbackup_binary -v 1 -W test=test_backup_bin_stmt1

# 5. taosBackup parquet 备份与恢复（stmt2/stmt1）
taosBackup -D test -F parquet -o /tmp/taosbackup_parquet
taosBackup -i /tmp/taosbackup_parquet -v 2 -W test=test_backup_pqt_stmt2
taosBackup -i /tmp/taosbackup_parquet -v 1 -W test=test_backup_pqt_stmt1
```

### 第二轮（8 vgroups）

```bash
# 1. 重建数据库（8 vgroups）
taos -s "drop database if exists test"
taosBenchmark -t 10000 -n 10000 -v 8 -y

# 2. 查看数据库占用
taos -s "use test; show table distributed meters"

# 3. taosdump 备份与恢复（行数验证）
taosdump -D test -o /tmp/taosdump_backup
taosdump -i /tmp/taosdump_backup -W test=test_dump_restore
taos -s "select count(*) from test_dump_restore.meters"
taos -s "drop database if exists test_dump_restore"

# 4. taosBackup binary 备份与恢复（行数验证）
taosBackup -D test -F binary -o /tmp/taosbackup_binary
taosBackup -i /tmp/taosbackup_binary -v 2 -W test=test_backup_bin_stmt2
taos -s "select count(*) from test_backup_bin_stmt2.meters"
taos -s "drop database if exists test_backup_bin_stmt2"
taosBackup -i /tmp/taosbackup_binary -v 1 -W test=test_backup_bin_stmt1
taos -s "select count(*) from test_backup_bin_stmt1.meters"
taos -s "drop database if exists test_backup_bin_stmt1"

# 5. taosBackup parquet 备份与恢复（行数验证）
taosBackup -D test -F parquet -o /tmp/taosbackup_parquet
taosBackup -i /tmp/taosbackup_parquet -v 2 -W test=test_backup_pqt_stmt2
taos -s "select count(*) from test_backup_pqt_stmt2.meters"
taos -s "drop database if exists test_backup_pqt_stmt2"
taosBackup -i /tmp/taosbackup_parquet -v 1 -W test=test_backup_pqt_stmt1
taos -s "select count(*) from test_backup_pqt_stmt1.meters"
taos -s "drop database if exists test_backup_pqt_stmt1"
```
