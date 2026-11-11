# TDengine TSDB 性能测试套件（Windows vs Linux 同机同盘）

一套脚本、一个命令，完成「安装 → 写入 → 查询 → 出报告」全流程。
Windows 与 Linux 共用同一份代码，平台差异在脚本内部自动处理。

## 目录结构

```
perf_win_vs_linux/
├── config/             # taosBenchmark 配置模板
│   ├── insert.json         # W1 写入：拼接 SQL（insert_mode=taosc）
│   ├── insert_stmt2.json   # W2 写入：参数绑定（insert_mode=stmt2）
│   └── Q1.json ~ Q5.json   # 查询场景
├── script/
│   ├── main.py         # 唯一入口：安装 + 写入 + 查询 + 报告
│   ├── installer.py    # 安装包处理（.exe/.deb/.rpm/.tar.gz/.tar.xz）
│   ├── sampler.py      # 跨平台资源采样（Linux /proc，Windows typeperf）
│   ├── benchlog.py     # taosBenchmark 日志解析
│   └── report.py       # Markdown 报告生成
├── report/             # 生成的报告（<tag>.md）
├── results/            # 原始结果（<tag>/result.json、日志、采样 CSV）
└── docs/               # 方案与历史记录
```

## 快速开始

一条命令，传入安装包即可全自动跑完：

Linux：

```bash
python3 script/main.py /path/to/TDengine-server-3.4.3.9-Linux-x64.tar.gz
```

Windows：

```powershell
& 'C:\Program Files\Python312\python.exe' script\main.py C:\pkgs\TDengine-server-3.4.3.9-Windows-x64.exe
```

要求：Python 3.8+；Linux 下需 root（安装与清空页缓存），Windows 下需管理员（安装与重启服务）。

## 阶段与耗时（10 亿行数据集，仅供参考）

| 阶段 | 内容 | Linux | Windows |
|---|---|---|---|
| install | 安装包静默安装并启动 taosd | ~1 min | ~2 min |
| writes | W1/W2 × 冷/热 = 4 轮，每轮 10 亿行 | ~40 min | ~90 min |
| queries | Q1–Q5，单一热数据集 | ~10 min | ~25 min |
| report | 生成 `report/<tag>.md` | 秒级 | 秒级 |

## 查询场景

| 编号 | 场景 | SQL | 执行次数 | 线程 |
|---|---|---|---|---|
| Q1 | 投影查询 | `select * from meters` | 3 | 1 |
| Q2 | 聚合查询 | `select count(*) from meters` | 20 | 1 |
| Q3 | 子表 LAST 查询 | `select last(*),tbname from meters group by tbname` | 20 | 1 |
| Q4 | 窗口查询 | `select count(*) from meters interval(1m)` | 3 | 1 |
| Q5 | 多线程投影查询 | `select * from meters` | 3 | 8（= VGROUPS） |

## 写入场景

共 4 轮，每轮独立 DROP + CREATE 数据库后写入 **10 亿行**（10000 子表 × 100000 行，8 线程，每请求 5000 行）：

| 轮次 | 配置 | 写入方式 | 缓存处理 | Linux | Windows |
|---|---|---|---|---|---|
| `w1_cold` | `insert.json` | `taosc`（拼接 SQL） | 重启 taosd + 清空页缓存 | ~10.5 min | ~23 min |
| `w1_hot` | `insert.json` | `taosc` | 不清理，紧接上一轮 | ~10.5 min | ~23 min |
| `w2_cold` | `insert_stmt2.json` | `stmt2`（参数绑定） | 重启 taosd + 清空页缓存 | ~10.5 min | ~23 min |
| `w2_hot` | `insert_stmt2.json` | `stmt2` | 不清理，紧接上一轮 | ~10.5 min | ~23 min |

### 如何调用

```bash
# 全流程：写入 4 轮 + 查询
python3 script/main.py --skip-install --tag linux_q1

# 只跑写入（4 轮全跑）
python3 script/main.py --skip-install --only writes --tag linux_w

# 只跑热缓存两轮，跳过重启与清页缓存（省一半时间）
python3 script/main.py --skip-install --only writes --skip-cold --tag linux_hot

# 只跑参数绑定（stmt2）两轮
python3 script/main.py --skip-install --only writes --write-modes stmt2 --tag linux_stmt2

# 只跑拼接 SQL（taosc）两轮
python3 script/main.py --skip-install --only writes --write-modes taosc --tag linux_taosc

# 最省时：只补一轮热缓存下的 stmt2
python3 script/main.py --skip-install --only writes --skip-cold --write-modes stmt2 --tag w2_hot
```

参数组合对应的实际轮次：

| 参数 | 执行轮次 |
|---|---|
| 无（默认） | `w1_cold` `w1_hot` `w2_cold` `w2_hot` |
| `--skip-cold` | `w1_hot` `w2_hot` |
| `--write-modes stmt2` | `w2_cold` `w2_hot` |
| `--write-modes taosc` | `w1_cold` `w1_hot` |
| `--skip-cold --write-modes stmt2` | `w2_hot` |

注意事项：

- 每轮都会 **DROP 现有 `perfdb`**，勿在有用数据的实例上运行。
- 冷启动轮次需要 root（Linux）/ 管理员（Windows）：重启 taosd、清页缓存。
- 写入最后一轮留下的数据集，就是后续查询阶段用的那份热数据；单独跑 `--only queries` 时依赖它已存在。
- 写入吞吐以 `real` 端到端口径为准，`result.json` 同时保留 bench 自报口径。

## 常用参数

| 参数 | 说明 |
|---|---|
| `--package` / 位置参数 | 安装包路径 |
| `--skip-install` | 复用已安装版本 |
| `--only {all,writes,queries}` | 只跑某阶段 |
| `--skip-cold` | 写入只跑热缓存轮次 |
| `--write-modes taosc,stmt2` | 写入只跑指定方式（配合 `--only writes`） |
| `--queries Q1,Q5` | 只跑指定查询（冒烟用） |
| `--query-rounds N` | 查询集整体重复 N 轮 |
| `--tag NAME` | 结果目录与报告文件名 |
| `--baseline other/result.json` | 与另一平台结果对比 |
| `--home PATH` | 指定 TDengine 安装目录 |

常见用法：

```bash
# 只重跑查询（写入数据已存在，约 10~25 分钟）
python3 script/main.py --skip-install --only queries --tag linux_q

# 冒烟测试两条查询
python3 script/main.py --skip-install --only queries --queries Q3,Q4 --tag smoke

# 与另一平台对比（先各自跑完，再对基线重生成报告）
python3 script/report.py results/linux_xxx/result.json report/compare.md results/windows_xxx/result.json
```

## 输出

- `results/<tag>/result.json`：全部指标的结构化结果（环境、写入、查询、资源）
- `results/<tag>/<label>.log`：taosBenchmark 原始日志
- `results/<tag>/<label>.samples.csv`：资源采样原始数据
- `report/<tag>.md`：Markdown 报告

## 口径说明

- **冷启动**：重启 taosd 并清空页缓存。Linux 写 `/proc/sys/vm/drop_caches`；Windows 无等价接口，仅重启服务。
- **热缓存**：不做清理，紧接冷启动执行，上一轮数据仍在页缓存。
- **查询**：只在最后一份热数据上跑一次，不区分冷热。
- **CPU**：Linux 取 `/proc/<pid>/stat` 差分；Windows 取 typeperf `Process\% Processor Time`（单核 100%），均换算为核数。
- **吞吐**：同时给出 bench 自报口径与 real 端到端口径。

---

## 分析报告：交给大模型

`script/report.py` 产出的 `report/<tag>.md` 是**机械输出**——只有实测数字（环境、吞吐、延迟、资源），没有归因。跨平台对比结论、根因分析、资源占用评价需要大模型完成。本节即该任务的操作规范。

### 工作流

1. 两端各跑一次：`--tag linux_<ver>` / `--tag windows_<ver>`
2. 补测裸盘基线（见下方"必需输入"），否则无法排除"磁盘能力"这一类假设
3. 把输入文件与文末提示词一起交给大模型

### 必需输入

| 输入 | 来源 | 必需性 |
|---|---|---|
| `results/<tag>/result.json` ×2 | 脚本产出 | 必需 |
| `results/<tag>/*.samples.csv` ×2 | 脚本产出 | 必需（归一化与 IO 粒度分析依赖原始计数） |
| `report/<tag>.md` ×2 | 脚本产出 | 可选，便于核对 |
| 裸盘基线（两分区顺序/随机读写） | 人工补测 | **强烈建议**，缺失则无法排除磁盘能力 |
| 分区几何、文件系统、调频策略 | 人工记录 | 强烈建议 |

裸盘基线参考：

```bash
# 顺序写
dd if=/dev/zero of=/path/to/bigfile bs=1M count=8192 oflag=direct conv=fdatasync
# 顺序读 / 4K 随机读
dd if=/path/to/bigfile of=/dev/null bs=1M iflag=direct
fio --name=r --rw=randread --bs=4k --iodepth=32 --size=1G --filename=/path/to/bigfile
```

### 分析规则（硬性）

1. **先验同源性**：TDengine 版本、taosBenchmark 版本、建库语句、写入配置、SQL 必须两端完全一致；不一致就不出结论。
2. **结论分级**：每条结论标明是「实测」「推导」「假设」还是「未验证」。禁止把相关性表述为因果。
3. **一律归一化**：绝对资源值经常给出**反向**结论。CPU 必须按单位吞吐折算（如「每 1 M rows/s 消耗多少核」或「每轮查询的 CPU·秒」）；磁盘必须换成「每次 IO 的字节数」，只看 IOPS 会误判谁更高效。
4. **先排除后归因**：按「写放大 → 磁盘物理能力 → CPU → 网络/传输」顺序逐个用数据排除，再给出候选机制。**排除项要写明证据**。
5. **识别硬件偏差**：分区内外圈（同盘不同分区速度可差 1.5–1.9×）、CPU 调频策略、页缓存冷热、资源采样粒度。偏差方向要说明，且要判断它是否与观测结果同向。
6. **原始日志交叉验证**：`result.json` 里的解析值必须与 taosBenchmark 日志、采样 CSV 的原始计数对得上，对不上就修解析而不是改结论。
7. **判断瓶颈归属**：区分**传输 / 存储 / 计算**三类瓶颈。归属错了，优化方向就全错。
8. **指标适用性**：被结果集传输锁死的指标（如回传整表的 `select *`）不能用来衡量存储引擎能力，必须在报告中明说。
9. **标注适用范围**：结论只对当前负载、盘型、数据规模成立，不推广为"某系统普遍更慢"。
10. **采样不足必须标注**：单轮仅 1–3 s 的用例可能只拿到 0–2 个采样点，其 CPU/RSS 不得采信。
11. **外推要标估计**：任何按比例折算（如"扣掉硬件优势后差距约 N×"）必须标为粗算并给出前提。
12. **不得编造未测指标**：数据里没有的量，只能写"需要补测"。

### 常见陷阱

| 陷阱 | 错误表现 | 正确做法 |
|---|---|---|
| 只看绝对 CPU | 得出"某平台更省 CPU" | 按单位吞吐折算后再比 |
| 只看 IOPS | IOPS 高的一侧被当成更高效 | 换算每次 IO 字节数 |
| 把全表扫描当存储能力 | 两端几乎相同却解释不出原因 | 指明被传输锁死，不用于评价引擎 |
| 未识别分区几何 | "磁盘更快的一侧反而更慢"成为无解矛盾 | 补裸盘基线，说明偏差方向 |
| 用均值代替尾部 | 掩盖 p99 数量级差异 | 单列 p90/p95/p99 |
| 采样点不足仍给数 | 用 0–2 个点下结论 | 标注不采信或留空 |
| 把假设写成结论 | 读者无法分辨证据强度 | 分级标注 |

### 输出结构

| 章节 | 内容要求 |
|---|---|
| 1. 结论摘要 | 只放一张「指标 / 倍数」表 + 一句归因，不放原理与推导 |
| 2. 测试环境 | 两端并排；突出会造成偏差的差异项 |
| 3. 写入性能 | 吞吐 → 延迟分位 → 根因（先排除再归因） |
| 4. 查询性能 | 时延 + 并行扩展性（单线程 vs 多线程加速比） |
| 5. 资源占用 | **先给"完成相同任务谁更费"的结论**，再给明细 |
| 6. 口径与已知偏差 | 逐条列出，含硬件偏差方向 |
| 7. 复现方式 | 可直接粘贴的命令 |

### 提示词模板

```text
你是 TDengine TSDB 的性能分析师。请严格遵循
source/taos-community/test/perf_win_vs_linux/README.md 中
「分析报告：交给大模型」一节的规则，产出 Windows vs Linux 对比报告。

输入：
- results/linux_<ver>/result.json
- results/windows_<ver>/result.json
- results/*/*.samples.csv
- 裸盘基线：<数据或路径>
- 分区：Windows 在 <分区>，Linux 在 <分区>

要求：
1. 每条结论标注「实测 / 推导 / 假设 / 未验证」
2. CPU 一律按单位吞吐归一化后再比较
3. 磁盘必须换算每次 IO 字节数
4. 先排除写放大、磁盘能力、CPU、传输，再给根因
5. 摘要只放倍数表，不放原理
6. 数据不足以判断的，写"未验证"并说明需要补什么实验
```
