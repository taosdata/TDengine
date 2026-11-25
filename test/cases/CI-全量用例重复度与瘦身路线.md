# CI 全量用例重复度分析与瘦身路线

> 目标：整理 `source/taos-community/test` 下全部用例的重复，缩短 CI 运行时长，**不降低覆盖率**。
> 本文是全量口径的路线图；**实测流水账在 `cases/CI-耗时节约台账.md`**（每次有新批次完成就更新那张表）。
>
> 两篇文档的位置：`source/taos-community/test/cases/`（与用例目录同级，不占用新增一级目录）。

---

## 文档索引

| 文档 | 内容 | 何时看 |
|---|---|---|
| 本文 | 重复度分层方法论（A/B/C/D/E）、工具、全量路线 | 接手下一次瘦身 |
| `CI-耗时节约台账.md` | **每批次实测节约时长**（最关心的一张表） | 汇报 / 验收 |
| `CI-用例合并映射表.md` | 旧文件 → 新文件映射 + 合并进来的测试方法 + 覆盖率评估 | 查“这个文件哪去了” |

> 维护约定：每完成一批合并或缓存改造，在本文件 §8 与台账里同步写一行《实测》数据；口径统一用
> 「跑批内负载校准」（见台账 §摘要的「口径说明」），不要混用轻载与全量跑批的数字。

---

## 0. TL;DR

| 结论 | 数字 |
|---|---|
| 全量跑批机器时间（基线，5 次跑批中位数） | **71.11 h**，31 台 runner ⇒ 墙钟 ≈ **2.3 h** |
| 注册用例条数（`ci/cases.task`） | 1736 → **1453** |
| 每条用例的固定开销 | ≈ 20 s（taosd 部署 + 集群初始化 + python 启动）⇒ 合计 **9.1 h / 13%** |
| **A 层**：完全重复的注册行 | 6 条 |
| **B 层**：被类级条目覆盖的方法级条目 | 0 条 |
| **C 层**：同一文件同一类被拆成多个进程 | 已吸收 **384 条** |
| **D 层**：跳文件合并 | 92 个源文件 → 25 个合并类 |
| **E 层**：外部源建表复用 | !3864，单项仅 ~167 s（对全量 < 0.1%） |
| **当前合计（已实测）** | **1736 → 1453（−16.3%）**；机器时间 **71.11 h → 60.96 h（−10.15 h，−14.3%）**，墙钟 **−19 min/次全量** |
| 覆盖率 | 未下降（§8.3），MR !3864 的 CI 跑了 **1627/1627 全通过** |
| D 层剩余高置信批次 | 18 个批次、47 个文件（按 setup 重量排序，见 §5.7） |
| 更早完成的人工深挖（4 个目录） | 169 → 122 条，21 个源文件合并成 7 个 |

CI 是**吞吐受限**的：最慢用例 985 s，远小于总时长/并发（2.4 h），所以
**省下的机器时间≈省下的墙钟时间**，而不是把瓶颈从并发挪到关键路径。

---

## 1. 观测口径

数据来自 GitLab `rd-public/tsdb` 的 coordinator artifact `results/workload-<job>.json`，
其中 `attempts[].case_id` 是完整命令行、`worker_elapsed_ms` 是该用例在 worker 上的实测耗时。

| job | attempts | runner 数 | 机器时间 |
|---|---:|---:|---:|
| 961047 | 1746 | 31 | 73.0 h |
| 961239 | 1742 | 31 | 73.0 h |
| 960598 / 960768 / 960970 | ≈1730 | 31 | 用于取中位数 |

耗时取多次运行的中位数，避免单次抖动。

> 逐条取 5 次跑批的中位数后求和 = **71.11 h**（1709 条 pytest 注册），这就是后文所有
> 「节约」的**基线**。单次跑批的绝对值受机器繁忙度影响大，**不要拿它与单次实测直接比**（见 §8.1）。

---

## 2. 时间花在哪

```
  0- 20 s :    5 条    0.0 h
 20- 45 s :  312 条    3.1 h   (4.6%)
 45- 90 s :  539 条    9.8 h  (14.4%)
 90-180 s :  373 条   13.2 h  (19.4%)
180-360 s :  235 条   17.1 h  (25.1%)
360-720 s :  150 条   20.0 h  (29.4%)
720+    s :   21 条    4.9 h   (7.1%)
```

- **一半以上的时间在 180 s 以上的用例里**（46.5%），但这些用例本身很少有重复内容，
  它们的成本来自"跑得久"（大量数据 + 大矩阵），不是"跑得多"。
- **317 条用例本身不到 45 s**，其中部署开销占一半以上 —— 这类用例的数量就是浪费。

机器时间最集中的目录：

| 目录 | 条数 | 机器时间 | 占比 |
|---|---:|---:|---:|
| `05-VirtualTables/04-Query/03-ExtSource` | 90 | 8.8 h | 12.9% |
| `09-DataQuerying/19-FederatedQuery` | 105 | 7.9 h | 11.5% |
| `18-StreamProcessing/99-Others` | 78 | 4.6 h | 6.8% |
| `17-DataSubscription/02-Consume` | 116 | 4.5 h | 6.7% |
| `70-Cluster` | 73 | 2.4 h | 3.5% |
| `05-VirtualTables/04-Query/02-CrossDb` | 62 | 2.0 h | 3.0% |
| `09-DataQuerying/08-SubQuery` | 54 | 2.0 h | 3.0% |
| `18-StreamProcessing/02-Stream` | 50 | 1.9 h | 2.8% |
| `21-MetaData` | 45 | 1.6 h | 2.4% |

---

## 3. 重复的四个层次

`test/conftest.py` 的 `before_test_class` 是 `scope="class"` 且 `autouse=True`：
**一次 pytest 进程 = 一次 taosd 部署**。因此"重复"的本质是"同配置被部署了多次"。

| 层 | 定义 | 判据 | 规模 | 是否丢覆盖 |
|---|---|---|---:|---|
| **A** | 同一 `(env, file, node, args)` 被登记了多次 | 字面相同 | 8 条 | 否 |
| **B** | 已登记 `file::Class`，又登记了它的 `::method` | 集合包含 | 0 条 | 否 |
| **C** | 同一 `(file, class, args)` 的方法被拆成 N 个进程 | 部署参数一致 | 217 条 | 否 |
| **D** | 同目录不同文件、同配置、语义重叠 | Jaccard ≥ 0.45 | 94 个文件 | 需逐文件确认 |

A/B/C 三层是**纯注册层**改动，不需要改一行业务代码，也不可能丢用例
（用 §5 的展开比对脚本逐条证明）。

### 3.1 C 层集中在 5 个目录

| 文件 | 类 | 参数 | 条目数 | CI 耗时合计 |
|---|---|---|---:|---:|
| `02-CrossDb/test_vtable_query_cross_db_stb.py` | `...CrossDBStbMode2` | — | 22 | 2950 s |
| `01-SameDb/test_vtable_query_same_db_stb.py` | `...SameDBStbSma` | — | 22 | 1944 s |
| `02-CrossDb/...cross_db_stb.py` | `...CrossDBStbMode2` | `-Q 2` / `-Q 3` | 19 / 19 | 2053 / 2144 s |
| `01-SameDb/...same_db_stb.py` | `...SameDBStbMode2R` | `-Q 2` / `-Q 3` | 19 / 19 | 1308 / 1524 s |
| `18-StreamProcessing/99-Others/test_fs_02_trigger_matrix.py` | `TestFsTriggerMatrix` | — | 14 | 6298 s |
| `09-DataQuerying/19-FederatedQuery/test_fq_*.py` | 18 个组 | — | 52 | — |
| `05-VirtualTables/04-Query/03-ExtSource/test_vtable_query_ext_source_stb.py` | 9 个组 | — | 26 | — |

按目录：`FederatedQuery` −52、`CrossDb` −45、`SameDb` −45、`99-Others` −47、
`03-ExtSource` −27、`05-DataType` −3。

---

## 4. 合并键必须带部署参数（这一层最容易踩坑）

同一条命令行里，**目标之后的参数是部署参数**，不是"重复"：

```
./ci/pytest.sh pytest <file>::<Class>::<method> -Q 2      # queryPolicy=2 的部署
./ci/pytest.sh pytest <file>::<Class>::<method> -Q 3      # queryPolicy=3 的部署
./ci/pytest.sh pytest <file>::<Class>::<method>           # 默认部署
```

出现过的部署参数：`-N`(dnode) `-M`(mnode) `-Q`(queryPolicy) `-R` `-B` `-K` `-C` `-I`
`--replica` `--timeout`，以及命令行前缀 `env FQ_REQUIRED_SERVICES=... FQ_INFLUX_VERSIONS=...`
和 `CASE_TIMEOUT=<n>`。

所以分组键是：

```
(CASE_TIMEOUT 前缀, env 前缀, file, class, 目标后参数, sanitizer, priority, workdir)
```

按这个键分组，**1678 条里有 59 个组的成员超过 1**。如果漏掉参数，会把
`-Q 2`/`-Q 3`/无参三份当成同一份，那才是真的降覆盖。

### 4.1 踩到的坑：`CASE_TIMEOUT` 可以出现在 `env` 之前

真实的命令行有这种形状（`cases.task` 第 895 行）：

```
100,,y,.,CASE_TIMEOUT=1500 env FQ_REQUIRED_SERVICES=oracle ./ci/pytest.sh pytest cases/.../test_fq_09_stability.py::...::test_fq_stab_o01_oracle_repeat_consistency
```

第一版解析器只剥离开头的 `env K=V`，遇到 `CASE_TIMEOUT=` 就停住，于是把它当成了
"目标之后的参数"，重写时**把 `CASE_TIMEOUT=1500` 和 `FQ_REQUIRED_SERVICES=oracle`
一起丢掉** —— 6 条 FQ/ExtSource/WLR 用例的部署条件被悄悄改了。

这个错误是靠"展开比对"脚本抓到的（§5.2）：它把每条注册展开成
`(env, sanitizer, priority, workdir, file, node, args)` 元组，逐个对比改前改后，
一有差异就报出来。**没有这个脚本，这个改动会以"通过但语义变了"的形式合进去。**

修正后的解析器把前缀当成"任意顺序的 `CASE_TIMEOUT=<n>` 与 `env K=V` 序列"，
并且合并时对同组的 `CASE_TIMEOUT` 取**最大值**，装箱预算再按它扣掉 150 s 余量。

---

## 5. 本轮落地与验证

### 5.1 方法

1. `expand()` 把每条注册行展开成 `(env, sanitizer, priority, workdir, file, node, args)`；
2. A 层去重；B 层删除被类级条目覆盖的方法级条目；
3. C 层按 `CASE_TIMEOUT`（默认 1200 s）留余量，把同一组的方法**贪心装箱**到
   每个进程估算 ≤ **1000 s** 的位置 —— 既去掉重复部署，又不制造超时风险；
4. 合并后的行保持"多个 node id 一次 pytest"的形式，pytest 原生支持：
   `pytest file.py::C::m1 file.py::C::m2 ...`；
5. 落盘前用展开比对脚本证明"注册的用例集合完全一致"。

### 5.2 覆盖守恒证明

不用肉眼看 diff，而是把改前/改后的文件都展开成 `(env, sanitizer, priority,
workdir, file, node, args)` 元组，比较**去重后的注册集合**：

```
duplicate registrations in reference: 6 (deduplicated on purpose)
entries: 1689 -> 1475 ; expanded node ids: 1756 -> 1750
MISSING 0 registration(s):
ADDED   0 registration(s):
COVERAGE-NEUTRAL: YES
```

6 条差值全部是 A 层"同一行被登记了两次"的重复，本身就是要去掉的。

### 5.3 本地实测

| 用例 | CI 口径（多个进程） | 合并后（单进程） | 倍数 |
|---|---:|---:|---:|
| `02-CrossDb` `TestVTableQueryCrossDBStbRef`（无参，15 方法） | 1273 s | **459.5 s** `15 passed` | 2.8× |
| `01-SameDb` `TestVTableQuerySameDBStbRef`（无参，15 方法） | 648 s | **232.2 s** `15 passed` | 2.8× |

两次验证都是直接从 `cases.task` 里取出**合并后的整行命令**原样执行，结果全绿，
`no asan errors`。收益不只是省 14 次部署（14×20 s=280 s），还有 14 次 `setup_class`
里重复建库建表的开销。

同一个文件里 `TestVTableQueryCrossDBStbBasic` 本来就是"1 条类级注册跑 13 个方法"，
说明"一个进程串行跑一个类的多个方法"是框架既有的正常形态，不是新引入的行为。

### 5.4 累计（过程记录：本地实测口径，`cases.task` 有效行 1689 → 1453）

| 段 | 改前 | 改后 | 手段 | 实测省时 |
|---|---:|---:|---|---:|
| `09-DataQuerying/19-FederatedQuery` | 114 | 63 | C | — |
| `05-VirtualTables/04-Query/03-ExtSource` | 94 | 68 | C | — |
| `18-StreamProcessing/99-Others` | 78 | 31 | C | — |
| `05-VirtualTables/04-Query/01-SameDb` | 63 | 18 | A+C | −416 s |
| `05-VirtualTables/04-Query/02-CrossDb` | 62 | 20 | A+C | −814 s |
| `05-VirtualTables/04-Query/05-DataType` | 10 | 7 | C | — |
| `21-MetaData` | 45 | 37 | D (txn 9→1) | −199 s |
| `70-Cluster` | 73 | 70 | D (restore 4→1) | −492 s |
| `01-DataTypes` | 20 | 9 | D (datatype 12→1) | −372 s |
| **合计** | **1689** | **1453** | | **−2.3 h/次** |

> 实测省时 = 该批合并后本地实测耗时 与 CI 口径多条合计 的差（不含未本地验证的 C 层目录）。
> `CrossDb`/`SameDb` 两行只是 `TestVTableQuery*StbRef` 这一个类的结果。
>
> 这是**过程中**的逐批记录（各段口径不一致、未做负载校准，合计 −2.3 h 只能当参考）；
> **最终数字以 §8 的跑批实测为准：−10.15 h（−14.3%）**。

### 5.5 D 层第一批：`21-MetaData/test_taosx_txn_*`（9 → 1）

这 9 个文件是同一份代码的复制：同样的 `_find_binary()`（gcc 编 `tmq_taosx_txn.c`）、
同样的 `_run_scenario()`、同样的 `setup_class`/`s0_cleanup`，只有场景清单不同。
每个文件各自部署一次 taosd、各自编译一次 C 程序。合并成
`test_taosx_txn_replication.py`（类 `TestTaosxTxnReplication`，9 个 `test_*` 方法、
51 个 helper）后，部署 1 次、编译 1 次。

覆盖证明用"逐方法存在性"而不是节点集合（D 层文件身份变了）：9 个源文件的
每个 `def` 都在新文件里以 `<tag>_<name>` 形式存在，`missing: 0`。

本地整文件跑通：**9 passed in 746.7 s**，`no asan errors`（CI 口径 9 条合计 946 s）。
这一批省的是 8 次部署 + 8 次 `gcc` 编译 + 8 次建库，收益不如 vtable 那种
"每次 setup 都建几万行数据"的场景大，但它是 D 层最机械、最容易复制的一类。

### 5.6 D 层第二批：`70-Cluster/test_cluster_restore_*`（4 → 1）

这一批才是 D 层真正值钱的样子。四个文件各自 `setup_class` 调
`RestoreBasic.init()`：5 dnode + 3 mnode、1 db（8 vgroups、replica 3）、1 超级表、
100 张子表、**1e6 行**、5 个 qnode——**四个文件建了四遍完全一样的数据**，然后各自只做
一个 `restore dnode/mnode/qnode/vnode`。

合并成 `test_cluster_restore_all.py`（`TestClusterRestoreAll`，4 个 test 方法，
`stop()` 从每个测试末尾移到 `teardown_class`）：

| 项 | 改前（4 条） | 改后（1 条） |
|---|---:|---:|
| 注册数 | 4 | 1 |
| 本地实测 | — | **4 passed in 332.8 s**，`no asan errors` |
| CI 口径合计 | 825 s（vnode 287 + dnode 194 + mnode 174 + qnode 170） | — |

≈ **2.5×**，每次全量少 **~492 s**。原因很直接：省的是一次 1e6 行的建库建表，
而不是一次 20 s 的部署。

### 5.7 D 层的价值模型（纠正前面的乐观估计）

合并两个文件能省的 = **一次部署（≈20 s）+ 一次 `setup_class`**。所以：

| setup 形态 | 每合并一个文件的收益 | 例子 |
|---|---:|---|
| 只设几个类属性 | ~20–25 s | `test_taosx_txn_*`（实测 25 s/文件） |
| 建库建表 + 插数万行 | ~80–120 s | `test_cluster_restore_*`（实测 123 s/文件） |
| 造 1e6 行 | 同上 | 同上 |

按"重的 `setup_class` + 同目录同部署参数 + `setup` 文本相似度 ≥0.85"筛选，
全仓共有 **18 个高置信批次、47 个可合并掉的文件**（不是之前按 Jaccard 粗算的 94 个）。
这笔收益上限约为 47 × 40 s ≈ **0.5 h**，所以 D 层要**优先挑 setup 重的**，
不能按"文件数"排序。

### 5.8 D 层第三批：`01-DataTypes/test_datatype_*`（12 → 1）

12 个类型文件是同一个 5-helper 脚手架（`create_table` / `insert_data` /
`auto_create_table` / `alter_tag_value` / `illegal_input`），只换列类型；对象名本来就是
`mt_<type>` / `st_<type>_N`，所以可以在同一个 `db` 里串行跑。

| 项 | 改前（12 条） | 改后（1 条） |
|---|---:|---:|
| 注册数 | 12 | 1 |
| 本地实测 | — | **12 passed in 62.7 s**，`no asan errors` |
| CI 口径合计 | **435 s**（单条 31–40 s，几乎全是开销） | — |

≈ **7×**：这批用例的 30–40 s 里几乎没有"真活"，合起来等于只付了一次部署。

### 5.9 全量 collect-only 校验（C 层 76 条合并行）

本地逐条跑完整用例覆盖不到 FQ/ExtSource（需外部服务），但可以便宜地验证"节点 id 都对"：
`pytest --collect-only -q <该行的所有 node id>` —— 不部署 taosd，只做收集。

```
merged lines: 76 (total target ids: 356)
failures: 1
  line 735: ids=8 collected=9   <-- 该用例带 @pytest.mark.parametrize("influx_version", ["3.0","1.8"])
```

即 **76 条合并行全部可以正常收集**，唯一的条数差异是一个 parametrize 产生的两个用例。
这一层校验能抓住的错误类型是：拼错的 node id、拼错的 class 名、文件不存在 ——
这些在 CI 上会表现成"整行报错/跳过"，而不是用例失败，最难发现。

### 5.10 另一个坑：`updatecfgDict` 常常写在类里

第一版"同配置"判定只识别**行首**的 `updatecfgDict = {...}`，而仓库里更常见的写法是
类属性：

```python
class TestDeleteStable:
    updatecfgDict = {'tsdbdebugFlag': 131}
```

结果两个**服务端配置不同**的文件（`test_delete_stable.py` 带 `tsdbdebugFlag`，
`test_delete_childtable.py` 不带）被判成"同配置"。改成 AST 遍历后能正确区分。
（C 层的合并键是**同一个文件**，所以不受这个缺陷影响；只有 D 层的跨文件候选受影响。）

---

## 6. 下一阶段路线

### 6.1 D 层：跨文件同配置语义合并

按"同目录 + 同 `updatecfgDict` + 方法名/helper 名 Jaccard"只能粗略缩小范围（47 个批次、
94 个文件、覆盖 12.8 h）——但"覆盖 12.8 h"**不等于"省 12.8 h"**（见 §5.7）。
按"重的 `setup_class` + setup 文本相似度 ≥0.85"重新排序后剩 **18 个高置信批次、47 个文件**：

| 目录 | 文件数 | 合计耗时 | 说明 |
|---|---:|---:|---|
| ~~`21-MetaData/test_taosx_txn_*`~~ | ~~9~~ | ~~946 s~~ | **已完成**，见 §5.5 |
| ~~`70-Cluster/test_cluster_restore_*`~~ | ~~4~~ | ~~825 s~~ | **已完成**，见 §5.6 |
| ~~`01-DataTypes/test_datatype_*`~~ | ~~12~~ | ~~435 s~~ | **已完成**，见 §5.8 |
| `05-VirtualTables/04-Query/04-Misc` | 11 | 1525 s | 已检查：setup 最大相似度仅 0.73，**不是克隆族**，需逐文件看 fixture |
| `11-Functions/03-Selection` / `04-Timeseries` | 21 / 19 | 1940 / 1242 s | 选中函数族同脚手架，但要先确认同名表不冲突 |
| `05-VirtualTables/06-Meta/03-TagRef` | 4×3 | 1659 s | 同一份拓扑的 `-Q` 三变体，按变体各 4→1 |
| `07-DataDeletion` | 2 | 577 s | |
| `42-Xnode` | 4 | 224 s | |
| `04-SuperTables/03-Alter` | 2 | 183 s | |
| `13-TimeSeriesExt/03-TimeWindow` | 2 | 161 s | |

> `05-VirtualTables/04-Query/03-ExtSource` 的 4 个文件虽然 setup 被判定为"重且相似"，
> 但每个接近 900 s，合并会直接撞 `CASE_TIMEOUT`，不能按文件合并（见 §6.2）。

### 6.2 E 层：给"久但不重复"的用例瘦身

`05-VirtualTables/04-Query/03-ExtSource` 的 4 个文件合计 **25509 s（7.1 h，占全量 10%）**，
它们不是"重复"，而是每个方法都重建外部源表 + 大矩阵查询。这类只能逐个看
（就像 `03-Tables` 报告里 §6 提到的 `column_limit` / `100w` 两个用例）。

---

## 7. 风险与边界

- **不要跨部署参数合并**（§4）。`-Q`/`-N`/`-M`/`--replica`、`env` 前缀、`CASE_TIMEOUT` 都算部署参数。
- **不要跨 sanitizer 通道合并**：`,,n,.,pytest ...`（不走 `pytest.sh`、sanitizer=n）不能并入 `,,y,...`。
- **单进程串行会失去"方法级重试"粒度**：合并后重试是整块重跑。对 flaky 用例要留意
  （`maxAttempts` 仍在，只是粒度变粗）。
- **装箱上限**：默认 `CASE_TIMEOUT=1200`，装箱目标取 1000 s；超大组会被拆成多块，
  不会出现"一条 6000 s 的用例"。
- **顺序依赖**：C 层的前提是同一类的方法互不依赖。已用 `Basic` 类（本来就是一条类级注册）
  做了交叉验证；D 层合并时必须逐文件确认 `setup_class` 造的数据能被所有方法共用。
- **CI 侧无副作用**：`cases.task` 的行是 LPT 调度的 key（`case_timing.py` 的
  `result_key = (path, cmd)`）。改了 cmd 历史耗时会对不上，首次跑批这些行按无历史处理，
  只影响下发顺序，不影响正确性；跑过一次后自动恢复。

---

## 8. 收益与覆盖率结算

> **已由两次 CI 实测取代**（此前的 ≈3.4 h / 5.0% 是改动前估算，作废）：
> **71.11 h → 60.96 h，节约 10.15 h（−14.3%），墙钟约 −19 分钟/次全量**；
> 保守口径（只看改动过的 20 个目录）为 **9.06 h（26.2%）**。
> 逐批明细见 **`CI-耗时节约台账.md`**（同目录；每次有新批次完成就在那里更新）。

### 8.1 实测方法（负载校准）

单次跑批的绝对耗时受机器繁忙程度影响很大（同一批命令两次跑可差 ±50%），所以不能拿「一次实测」
直接比「五次中位数」。这里用**跑批内校准**：

- **基线**：`origin/main` 的 `ci/cases.task` 逐条配 5 次全量跑批（960598 / 960768 / 960970 /
  961047 / 961239）`worker_elapsed_ms` 的中位数求和 = **71.11 h**（1709 条 pytest 注册）；
- **负载系数**：MR !3864 的 CI（child 84943）里未被改动的 **1325 条**实测 62.49 h，其历史中位数
  54.53 h → 该次跑批整体慢 **1.146×**；
- **现状**：该次跑批全量实测 69.85 h ÷ 1.146 = **60.96 h**（它已包含 !3862 的合并 + !3864 的
  外部源建表复用，且跑了 1456 条 pytest + 171 条 unit-test，接近全量）。

### 8.2 分层结果

| 口径 | 基线 | 现状 | 节约 |
|---|---:|---:|---:|
| 被吸收的 384 条 → 承接的 76 条 | 16.58 h | 6.43 h | **10.15 h** |
| 改动过的 20 个目录 | 34.60 h | 25.55 h | **9.06 h（26.2%）** |
| 全量 pytest | 71.11 h | 60.96 h | **10.15 h（−14.3%）** |
| 注册条目 | 1736 | 1453 | **−283（−16.3%）** |

按目录看，收益最大的四个：`19-FederatedQuery` 8.43→5.61 h（−2.82 h）、
`03-ExtSource` 9.35→7.49 h（−1.85 h）、`99-Others` 4.63→3.05 h（−1.58 h）、
`21-MetaData` 2.01→1.34 h（−0.68 h）。这些用例的 `setup_class` 动辄 200–400 s（外部源建表、
造数据），所以「少部署几次」比预想的更值钱。

!3864 的「外部源建表复用」单项：同一批 9 条 ExtSource 命令 A/B 归一后只省 **~167 s（6%）**，
对全量贡献 **< 0.1%**；保留它只因为它严格减少重复工作、且有开关 `TD_VTABLE_REUSE_EXTERNAL_ORIGIN=0`
可关，**不是**本次节约的主力。

### 8.3 覆盖率

| 层 | 证明手段 | 结果 |
|---|---|---|
| C | 展开成 `(env, sanitizer, priority, workdir, file, node, args)` 比较去重集合 | `MISSING 0 / ADDED 0` |
| C | 逐行 `pytest --collect-only` | 76 行 / 356 个 node id 全部可解析 |
| D | 逐 `def` 存在性比对 + 本地整文件跑通 | 92 个源文件的测试方法一个没少 |
| 全部 | MR !3862 的 CI | **86/86 通过**（32 个 test-linux 分片 + unit-test + build 全绿） |
| 全部 | MR !3864 的 CI（近全量） | **1627/1627 通过**（pytest 1456 + unit-test 171，0 failed），2 条 flaky 均为未改动用例 |

### 8.4 必须说清的三个"非零"变化

1. 方法级重试粒度变粗（重试变整块重跑，`maxAttempts` 仍为 1）；
2. `FQ / ExtSource / 99-Others / 05-DataType` 四个目录的 C 层合并，本机无法真跑
   （缺 mysql/pg/influx/sqlserver/oracle），**已在 MR !3862 的 CI 里全部通过**；
3. `test_cluster_restore_*` 里每个测试末尾的 `self.basic.stop()` 移到 `teardown_class`
   （`stop()` 只是 `tdSql.close()`）。

## 9. 本轮用的工具（脚本在 /tmp，未入库）

| 脚本 | 作用 |
|---|---|
| `ci_profile2.py` | 解析 `cases.task` + 关联 CI artifact 的 `worker_elapsed_ms` |
| `ci_l1_exact.py` | 部署参数感知的分组（`CASE_TIMEOUT`/`env`/目标后参数） |
| `ci_time.py` | 耗时分布与目录排名 |
| `ci_semantic.py` | 跨文件 Jaccard 聚类，产出 D 层批次清单 |
| `ci_heavy.py` | 按 `setup_class` 重量 + 文本相似度重新排序（§5.7 的口径） |
| `collapse_cases.py` | A 去重 / B 类级包含 / C 装箱；原子落盘 |
| `verify_cases_task.py` | 展开成节点集合比对，证明覆盖不变 |
| `verify_collect.py` | 逐条 `pytest --collect-only`，证明合并行的 node id 都有效 |
| `merge_taosx_txn.py` / `merge_datatypes.py` | 两类 D 层克隆文件的合并生成器 |

本地验证做法：**从 `cases.task` 里取出合并后的整行命令原样执行**
（`LINE=$(grep ... ci/cases.task); ${LINE#,,y,.,}`），而不是手写 node 列表——
这样验证的就是将要上 CI 的那条命令。

> 本地跑 pytest 需要先绕过 FQ runtime 预置：
> `export _TAOS_TEST_LIB_DIR=/root/tsdb/debug/build/lib`（否则 conftest 会去装
> mysql/pg/influx 依赖并失败）。

建议下一步把 `collapse_cases.py` + `verify_cases_task.py` 正式收进
`source/taos-community/test/ci/`（加版权头 + 单测），后续目录就能批量执行而不依赖 `/tmp`。


---

## 10. 收尾结论：整理方向的收益已到顶（2026-11-23）

四条线都做了量化试探，结论如下（口径见台账 §摘要的「负载校准」）：

| 方向 | 试探方法 | 结果 | 结论 |
|---|---|---|---|
| **C 注册层**（同文件同类多入口） | FQ 分类组在 1500 / 1800 / 2400 / 3000 s 预算下装箱；非 FQ 在 950 s 预算下装箱 | **全部 0 组可再并** | 已到顶 |
| **D 文件层**（跨文件克隆） | 全仓扫「同目录 + 同 helper 名 + 同 test 名」 | 再并掉 `05-Plan` 那一对（`test_vtable_plan_scan_prune.py`，2→1，本地 39.8 s 跑完两个方法）；其余 11 组要么配置不同、要么**共享同一个库/输出超级表**（如 `20-UseCase` 的两个三条峡用例都用 `test1.stb_hbny_sx_mint_jzzt2`，合并会互相污染） | 已无可安全合并项 |
| **E 外部源复用** | !3864 的进程内复用（同一进程内跳过重复 `prepare_origin_tables()`） | 9 条 FQ 命令共省 **167 s（6%）** ⇒ `prepare_origin_tables()` 本身只有几秒 | 跨进程复用的上限同样很小 |
| **部署开销** | 1425 条 × ≈20 s | **7.9 h（占全量 13%）** | **唯一的大杠杆**，但需改 `deploy_taos()`/预热，属 CI 基础设施 |

因此本项的定论是：**「用例整理」这条线的天花板就是「每天减少 1 次部署 ≈ 20 s」**，
在保证覆盖不变、不跨部署参数合并的前提下已基本吃干（累计 −8.8 ~ −10.2 h，12.3% ~ 14.3%）。
要继续压 CI 时长，应转向「每进程 ≈20 s 的部署固定开销」本身，而不是继续合并用例。
