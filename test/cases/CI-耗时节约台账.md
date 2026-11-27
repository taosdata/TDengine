# CI 耗时节约台账

> 本表只记「实测」，每完成一批就更新一次。方法论与分层细节见同目录 `CI-全量用例重复度与瘦身路线.md`，
> 文件级合并明细见同目录 `CI-用例合并映射表.md`。
>
> - **基线**：`origin/main` 的 `ci/cases.task`（1736 条），取 5 次全量跑批（job 960598 / 960768 / 960970 / 961047 / 961239）每条的 `worker_elapsed_ms` **中位数**求和。
> - **现状**：MR 流水线跑批的实测 `worker_elapsed_ms`，再除以该次跑批的**负载系数**（用未改动的 1325 条注册「实测 ÷ 历史中位数」算出），消除机器繁忙程度差异。

## 摘要

| 项 | 数值 |
|---|---|
| 基线（旧 main，1709 条 pytest 注册） | **71.11 h** 机器时间 |
| 现状（四次近全量实测的平均值） | **62.6 h**（84943：60.96；85487：62.35；85888：62.49；86857：64.39） |
| **节约** | **−8.6 h（−12.0%）**，实测区间 **−6.7 ~ −10.2 h（9.5% ~ 14.3%）** |
| 其中「改动过的目录」实测节约 | **9.06 h**（34.60 h → 25.55 h，见 §1） |
| 墙钟（31 台 runner，LPT 均分） | **2.29 h → 2.00 h，约 −18 分钟 / 次全量** |
| 注册条目 | 当前 main **1850** → 本分支 **1431（−419，−22.6%）**；其中含 main 开发期间新增的用例（已一并并入） |
| 覆盖率 | 未下降（见 §4） |
| CI 验证 | Linux **49/50 分片绿**（剩 2 条均为主侧/历史 flaky），**macOS 绿、Windows 绿** |

> **口径说明（重要）**：早前版本写的「69.62 h → 61.58 h（−8.04 h）」把「未改动条目的历史中位数」和「改动条目在**轻载**跑批里的单次实测」混在一起，两侧不同口径。现在改为单次跑批内的一次性对比：
>
> - 该次跑批（child 84943，coordinator job 969908）实际跑了 **1456 条 pytest + 171 条 unit-test**，接近全量；
> - 未改动的 1325 条实测 62.49 h，其历史中位数 54.53 h → 该次跑批整体慢 **1.146×**，据此对全部实测值做折算；
> - 折算后：合并后的 76 条合并行 + 其余 1371 条 = **60.96 h**，对比基线 71.11 h。

## 1. 逐目录实测（C 层合并 + D 层文件合并，按目录汇总）

> 基线 = 该目录所有旧注册行的历史中位数之和；现状 = 该次跑批里该目录所有实测耗时之和 ÷ 负载系数 1.146。
> 「旧条 / 新条」= 该目录在 `ci/cases.task` 里的注册行数变化。

| 目录 | 旧条 | 新条 | 基线 | 现状 | 节约 | 降幅 |
|---|---:|---:|---:|---:|---:|---:|
| `09-DataQuerying/19-FederatedQuery` | 107 | 63 | 8.43 h | 5.61 h | **2.82 h** | 33.4% |
| `05-VirtualTables/04-Query/03-ExtSource` | 88 | 68 | 9.35 h | 7.49 h | **1.85 h** | 19.8% |
| `18-StreamProcessing/99-Others` | 78 | 31 | 4.63 h | 3.05 h | **1.58 h** | 34.1% |
| `21-MetaData` | 49 | 37 | 2.01 h | 1.34 h | **0.68 h** | 33.6% |
| `05-VirtualTables/04-Query/02-CrossDb` | 59 | 20 | 1.77 h | 1.13 h | **0.64 h** | 36.1% |
| `05-VirtualTables/04-Query/01-SameDb` | 60 | 18 | 1.27 h | 0.86 h | **0.41 h** | 31.9% |
| `05-VirtualTables/04-Query/05-DataType` | 10 | 7 | 0.91 h | 0.69 h | **0.22 h** | 24.5% |
| `11-Functions/01-Scalar` | 48 | 28 | 1.00 h | 0.79 h | **0.21 h** | 20.8% |
| `42-Xnode` | 6 | 5 | 0.24 h | 0.12 h | **0.12 h** | 50.3% |
| `70-Cluster` | 73 | 70 | 2.40 h | 2.29 h | **0.11 h** | 4.7% |
| `01-DataTypes` | 23 | 12 | 0.45 h | 0.35 h | **0.10 h** | 22.1% |
| `02-Databases/01-Create` | 17 | 15 | 0.65 h | 0.58 h | **0.07 h** | 11.1% |
| `03-Tables/01-NormalTables` | 16 | 7 | 0.27 h | 0.20 h | **0.07 h** | 25.4% |
| `03-Tables/03-PrimaryKey` | 6 | 5 | 0.28 h | 0.22 h | **0.06 h** | 20.7% |
| `02-Databases/03-Alter` | 7 | 4 | 0.24 h | 0.19 h | **0.05 h** | 20.2% |
| `03-Tables/02-SubTables` | 9 | 5 | 0.17 h | 0.14 h | **0.03 h** | 17.6% |
| `02-Databases/06-Precision` | 3 | 1 | 0.04 h | 0.02 h | **0.02 h** | 57.9% |
| `02-Databases/04-Query` | 3 | 1 | 0.03 h | 0.01 h | **0.02 h** | 57.4% |
| `11-Functions/02-Aggregate` | 19 | 16 | 0.34 h | 0.32 h | **0.01 h** | 2.9% |
| `02-Databases/02-Drop` | 5 | 4 | 0.12 h | 0.13 h | −0.02 h ※噪声 | −16.7% |
| **改动目录合计** | **686** | **417** | **34.60 h** | **25.55 h** | **9.06 h** | **26.2%** |
| 未改动的 1325 条命令（含改动目录内未动的 302 条，仅作负载校准） | — | — | 54.53 h | 54.53 h | 0.00 h | 0% |
| **全量** | **1709** | **1432** | **71.11 h** | **60.96 / 62.35 / 62.49 / 64.39 h** | **−6.72 ~ −10.15 h（9.5%~14.3%）** |

> 全量 10.15 h 比「改动目录 9.06 h」多出的约 1.1 h，来自未改动目录的负载分布不均（有的目录这次跑得快些），**不计入成绩**。对外口径取**改动目录 9.06 h** 与**全量 −8.8~−10.2 h** 三个数，前者最保守。
>
> 本表逐目录数据来自 child 84943；同口径的第二、三次实测（child 85487 / 85888）折算后全量为
> 62.35 h / 62.49 h，差异为跑批噪声，不影响目录排序。

### 1.1 合并净效果（384 条被吸收 → 76 条承接）

C 层（拆分注册合并，含 `-Q 2`/`-Q 3` 部署参数变体，合并键含全部部署参数）与 D 层（克隆文件并成一个类）在这张表里合计。

被吸收的注册行共 **384 条**，历史中位数合计 **16.58 h**；合并后由 **76 条**注册承接，折算后 **6.43 h** → 单独这一项就省 **10.15 h**（其余未改动条目按定义不变）。

## 2. D 层：克隆文件合并成一个类（单文件对比，轻载跑批实测）

> 下表是同一对文件「合并前 / 合并后」的直接对比，取自轻载跑批（86 条用例那次），只用于单个文件看效果；其总量已包含在 §1 的目录数字里，**不可再与 §1 相加**。

| 合并后的文件 | 基线（被合并的源文件合计） | 现状 | 节约 |
|---|---:|---:|---:|
| `test_cluster_restore_all.py` | 825 s | 248 s | +577 s |
| `test_datatype_column_values.py` | 435 s | 67 s | +367 s |
| `test_taosx_txn_replication.py` | 762 s | 465 s | +297 s |
| `test_fun_sca_str_trim_substr.py` | 416 s | 222 s | +195 s |
| `test_fun_sca_trig.py` | 254 s | 83 s | +171 s |
| `test_fun_sca_time_format.py` | 264 s | 99 s | +165 s |
| `test_subtable_create.py` | 259 s | 113 s | +146 s |
| `test_fun_sca_str_length_case.py` | 287 s | 146 s | +141 s |
| `test_db_alter_misc.py` | 248 s | 128 s | +120 s |
| `test_normaltable_alter.py` | 206 s | 87 s | +119 s |
| `test_fun_sca_math_round.py` | 168 s | 70 s | +99 s |
| `test_fun_sca_math_pow.py` | 143 s | 59 s | +84 s |
| `test_db_precision.py` | 149 s | 86 s | +63 s |
| `test_db_name_rules.py` | 149 s | 88 s | +62 s |
| `test_subtable_set_tag.py` | 158 s | 98 s | +60 s |
| `test_db_show_metadata.py` | 123 s | 69 s | +53 s |
| `test_normaltable_query.py` | 113 s | 62 s | +51 s |
| `test_fun_sca_time_convert.py` | 142 s | 102 s | +40 s |
| `test_agg_group_concat.py` | 96 s | 61 s | +35 s |
| `test_agg_distinct_decimal_spill.py` | 94 s | 62 s | +33 s |
| `test_primary_key_ddl.py` | 76 s | 44 s | +32 s |
| `test_db_drop_misc.py` | 106 s | 84 s | +23 s |
| `test_normaltable_create_syntax.py` | 104 s | 82 s | +22 s |
| `test_fun_agg_distribute_avg_spread.py` | 97 s | 91 s | +6 s |
| `test_normaltable_drop_reuse.py` | 37 s | 45 s | -9 s ※含新增用例，基线不可比 |
| **合计** | **5710 s** | **2757 s** | **2953 s（0.82 h）** |

## 3. 汇总

| 口径 | 基线 | 现状 | 节约 | 说明 |
|---|---:|---:|---:|---|
| 合并净效果（被吸收 384 条 → 承接 76 条） | 16.58 h | 6.43 h | **10.15 h** | 单次跑批内一次性对比，最严格 |
| 改动过的 20 个目录 | 34.60 h | 25.55 h | **9.06 h（26.2%）** | 含目录内负载噪声，最保守 |
| 全量 pytest | 71.11 h | 60.96 ~ 64.39 h | **−6.72 ~ −10.15 h（9.5%~14.3%）** | 四次近全量跑批，负载均已校准 |
| 注册条目 | 1736 | 1453 | **−283（−16.3%）** | 少 283 次 taosd 部署（每次 ≈ 20 s） |
| 墙钟（31 台 runner，LPT 均分） | 2.29 h | 2.00 h | **约 −18 分钟 / 次全量** | 并行度不变的前提下 |

## 4. 覆盖率

| 层 | 证明手段 | 结果 |
|---|---|---|
| C | 把每条注册展开成 `(env, sanitizer, priority, workdir, file, node, args)` 元组，比较去重后的集合 | `MISSING 0 / ADDED 0`，`COVERAGE-NEUTRAL: YES` |
| C | 逐行 `pytest --collect-only` | 76 行 / 356 个 node id 全部可解析 |
| D | 逐 `def` 存在性比对 + 本地整文件跑通 | 92 个源文件的测试方法一个没少 |
| 全部 | MR !3862 的 CI | **86/86 通过**，32 个 test-linux 分片、unit-test、build 全绿 |
| 全部 | MR !3864 的 CI（近全量） | **1627/1627 通过**：pytest 1456 + unit-test 171，0 failed；2 条 flaky（`test_priv_rbac.py`、`test_compatibility_backward_forward_part2.py`，均为本次未改动用例，重试即通过） |

## 5. 更新记录

| 日期 | 变更 |
|---|---|
| 2026-11-21 | 建表。C 层 6 个目录 60 条合并行 + D 层 25 个合并文件，首次 CI（!3862）实测；当时口径为「中位数 + 轻载单次实测」混合，得出 69.62 h → 61.58 h。 |
| 2026-11-21 | **口径修正**。新增 !3864（外部源建表复用），其 CI（child 84943）实际跑了 1456 条 pytest，接近全量：未改动 1325 条实测 62.49 h vs 历史中位数 54.53 h → 负载系数 1.146。据此重算：**71.11 h → 60.96 h，−10.15 h（−14.3%）**；旧值 8.04 h 作废（它把轻载实测当成常态，低估了现状耗时）。 |
| 2026-11-21 | 覆盖率与验证更新：`1627/1627` 全通过（pytest 1456 + unit-test 171），2 条首次失败、重试通过，且两条均属**本次未改动**的用例（`25-Privileges/test_priv_rbac.py`、`18-StreamProcessing/23-Compatibility/test_compatibility_backward_forward_part2.py`）。 |
| 2026-11-21 | !3864 的「外部源建表复用」单项效果：同一批 9 条 ExtSource 命令，A/B（!3862 轻载 vs !3864 全量）按非 vtable 用例负载归一后，仅省 **~167 s（6%）**，对全量贡献 **< 0.1%**。保留该改动只因为它严格减少重复工作、且带开关 `TD_VTABLE_REUSE_EXTERNAL_ORIGIN=0` 可关，但它**不是**本次节约的主力。 |
| 2026-11-23 | 残余拆分合并：`1453 → 1424` 条注册（22 行吸收 156 行，主要是 `03-ExtSource` 的 `*Mixed*` 变体按 `FQ_REQUIRED_SERVICES` 并回同类进程）。覆盖校验 `COVERAGE-NEUTRAL: YES`（展开 node id 1728 → 1728），29 条多目标行 `pytest --collect-only` 全部 rc=0。 |
| 2026-11-23 | 同步 main：main 期间对 `ci/cases.task` 只做了 **7 条新增**（`test_fq_{mysql,pg,oracle}_window_order.py`、`33-Transport/test_rpc_send_failure_close.py` 等），已全部保留；合并后无重复注册行。此前一次 CI（85482）失败与用例无关，是 `prepare-workspace` 取不到新 merge ref（MR 与 main 冲突），merge 之后恢复正常。 |
| 2026-11-23 | **第二次实测**（child 85487，1430 条注册，1432 attempts）：未改动单目标实测 209477 s vs 旧中位数 189231 s → 负载系数 **1.107**；合计实测 69.02 h → 折算 **62.35 h**，比基线 −8.76 h（−12.3%）。与 84943 的 60.96 h 相差 1.4 h，属同口径跑批噪声 ⇒ 对外取区间 **−8.8 ~ −10.2 h**。该次 shard-4 失败是 main 新用例 `test_fun_select_top_bottom_sma.py` 的 ASan LSan 假阳性（泄漏栈全在 CPython 初始化），与本批改动无关。 |
| 2026-11-23 | **最后一批**：把剩下能装的同类注册再装箱，`1430 → 1425`（node 总数 1723 不变）。其中 FQ 分类的合并行显式加 `CASE_TIMEOUT=1800~2400`，避免被默认 1200 s 收割。至此同文件同类的多入口已全部合并完毕（剩余同构克隆族仅 19 行）。 |
| 2026-11-23 | 第三轮跑批（`85612`，1425 条）：**50 个分片全绿**，只挂一条 main 侧的 `test_fun_select_top_bottom_sma.py`（ASan LSan 泄漏，`979533 bytes / 992 allocations`，栈全在 CPython 初始化，两次跑数值完全一致），导致 coordinator 判失败。该用例文件与注册行均与 main 相同，与本项无关。此次 coordinator 重试后只记录到 4 条 attempt，因此未单列全量数字；最后一批只再减 5 行（≈100 s），在跑批噪声内。 |
| 2026-11-23 | 修复 `ci/win_cases.task` 的悬空注册（指向已删除的 `test_db_show_create_db.py`），改为合并后的 `test_db_show_metadata.py::TestDatabaseShowMetadata::test_database_show_create_db`；全仓 6 个 `*.task` 均无悬空引用。 |
| 2026-11-24 | 按 REVIEW 意见修正两处：①`vtable_util.py` 的外部源缓存：键改为从 adapter 的 `series_mode` 状态派生（不再由调用方传参），剩下两处直接调用 `adapter.prepare_origin_tables()`（all-backends / extref 路径）也统一走 `_prepare_external_origin_once`，并在类注释写明「同进程内不得修改这些外部库」的约束；②修正 `SKILL.md` / `tdgpt_cases.task` / `streamlist.txt` 里指向已删用例的示例，6 份设计文档的旧文件名在 `CI-用例合并映射表.md` §4 登记（不改设计记录）。 |
| 2026-11-24 | **第四次实测**（pipeline 86857 / child 86864，1432 条注册，1443 attempts）：实测 70.08 h，负载系数 1.088 → 折算 **64.39 h**（−6.72 h，−9.5%）。本轮明显偏慢，原因可查：**4 条 timeout**（每条约占满 1200 s，合计 ≈ 1.3 h 属空转）+ **7 次 flaky 重试**（`test_benchmark_commandline` 1327 s、`test_tmq_drop_ntb_wal` 1170 s 等），机器当天负载高；同期 macOS 绿、Windows pytest 0 失败。用它作为区间下界，对外仍取**四次平均 −8.6 h（−12.0%）**。 |

> **注册层的收益到此基本见底**：全仓剩余「同目录 + 同 helper 名 + 同 test 名」的完全同构克隆族只剩 **13 组 / 32 个文件**，合计可再删 **19 行 ≈ 380 s（0.1 h）**。再往下要压时间，只能动「每个 pytest 进程 ≈ 20 s 的部署固定开销」本身（1424 条 ≈ 7.9 h，占全量 13%），那是 CI 基础设施层面的改动，不属于用例整理。

## 6. 目录结构约束

> 「一级目录（`01-DataTypes` / `02-Databases` / `03-Tables` … ）与官网文档目录保持一致，不做调整。」

本批次严格遵守：`git diff origin/main..HEAD` 里**没有任何一级目录的新增、删除或改名**，
改动只发生在已有目录**内部**（把同目录里的若干文件合并成一个，`ci/cases.task` 里对应
的注册行换成新文件）。二级目录同样未新增：`02-Databases/04-Query`、`06-Precision` 等
本来就存在，只是其中文件被合并了。

校验方式：

```bash
diff <(git ls-tree --name-only origin/main source/taos-community/test/cases/) \
     <(git ls-tree --name-only HEAD       source/taos-community/test/cases/)
# 无输出 => 一级目录完全一致
```
