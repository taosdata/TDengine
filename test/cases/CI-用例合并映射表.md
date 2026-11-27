# CI 用例合并映射表

> 旧文件 = `origin/main` 里被删除/改名的文件；新文件 = 承接它的那个文件（同目录，不改目录结构）。
> 时间：基线 = 5 次全量跑批中位数之和；现状 = child 84943 实测 ÷ 负载系数 1.146（单条注册，无均摊误差）。

## 1. 文件映射（94 → 25）

| 新文件 | 老文件数 | 老文件 | 注册 老→新 | 基线 s | 现状 s |
|---|---:|---|---:|---:|---:|
| `01-DataTypes/test_datatype_column_values.py` | 12 | `test_datatype_{bigint,bool,double,float,int,json,nchar,smallint,timestamp,tinyint,uint,varbinary}.py` | 12→1 | 435 | 63 |
| `02-Databases/01-Create/test_db_name_rules.py` | 3 | `test_name_all`, `test_name_len`, `test_op_table_prefix_suffix` | 3→1 | 149 | 72 |
| `02-Databases/02-Drop/test_db_drop_misc.py` | 2 | `test_db_catalog`, `test_db_repeat` | 2→1 | 106 | 57 |
| `02-Databases/03-Alter/test_db_alter_misc.py` | 4 | `test_cache_last_shards`, `test_db_alter_database`, `test_db_alter_maxrows_compact`, `test_db_alter_option_keep` | 4→1 | 248 | 134 |
| `02-Databases/04-Query/test_db_show_metadata.py` | 3 | `test_db_show_create_db`, `test_db_show_create_table`, `test_db_show_vgroup` | 3→1 | 123 | 52 |
| `02-Databases/06-Precision/test_db_precision.py` | 3 | `test_db_precision_{bugs,ns,us}` | 3→1 | 149 | 63 |
| `03-Tables/01-NormalTables/test_normaltable_alter.py` | 7 | `test_normaltable_alter_{1..5}`, `test_normaltable_delete_reuse{1,2}` | 7→1 | 272 | 74 |
| `03-Tables/01-NormalTables/test_normaltable_create_syntax.py` | 2 | `test_normaltable_datatypes`, `test_normaltable_synatx` | 2→1 | 104 | 63 |
| `03-Tables/01-NormalTables/test_normaltable_drop_reuse.py` | 1 | `test_normaltable_query_after_drop` | 1→1 | 37 | 47 ※含新增用例 |
| `03-Tables/01-NormalTables/test_normaltable_query.py` | 3 | `test_normaltable_query_{aggregate,composite,filter}` | 3→1 | 113 | 69 |
| `03-Tables/02-SubTables/test_subtable_create.py` | 4 | `test_subtable_{batchcreate,autocreate,drop,query}` | 4→1 | 259 | 131 |
| `03-Tables/02-SubTables/test_subtable_set_tag.py` | 2 | `test_subtable_{batch_,}set_tag_vals` | 2→1 | 158 | 140 |
| `03-Tables/03-PrimaryKey/test_primary_key_ddl.py` | 2 | `test_primary_key_{create,delete}` | 2→1 | 76 | 37 |
| `11-Functions/01-Scalar/test_fun_sca_math_pow.py` | 3 | `test_fun_sca_{log,pow,sqrt}` | 3→1 | 143 | 63 |
| `11-Functions/01-Scalar/test_fun_sca_math_round.py` | 4 | `test_fun_sca_{abs,ceil,floor,round}` | 4→1 | 168 | 65 |
| `11-Functions/01-Scalar/test_fun_sca_str_length_case.py` | 4 | `test_fun_sca_{char_length,length,lower,upper}` | 4→1 | 287 | 291 |
| `11-Functions/01-Scalar/test_fun_sca_str_trim_substr.py` | 4 | `test_fun_sca_{ltrim,regexp_extract,rtrim,substr}` | 4→1 | 416 | 366 |
| `11-Functions/01-Scalar/test_fun_sca_time_convert.py` | 3 | `test_fun_sca_{now,to_timestamp,today}` | 3→1 | 208 | 95 |
| `11-Functions/01-Scalar/test_fun_sca_time_format.py` | 3 | `test_fun_sca_{timetruncate,to_iso8601,to_unixtimestamp}` | 3→1 | 197 | 117 |
| `11-Functions/01-Scalar/test_fun_sca_trig.py` | 6 | `test_fun_sca_{acos,asin,atan,cos,sin,tan}` | 6→1 | 254 | 71 |
| `11-Functions/02-Aggregate/test_agg_distinct_decimal_spill.py` | 2 | `test_agg_distinct_{decimal,spill}` | 2→1 | 94 | 55 |
| `11-Functions/02-Aggregate/test_agg_group_concat.py` | 2 | `test_agg_gconcat`, `test_agg_smoking` | 2→1 | 96 | 50 |
| `11-Functions/02-Aggregate/test_fun_agg_distribute_avg_spread.py` | 2 | `test_fun_agg_{avg_dist,spread}` | 2→1 | 97 | 100 |
| `21-MetaData/test_taosx_txn_replication.py` | 9 | `test_taosx_txn_{basic,boundary,concurrent_sessions,ddl_matrix,mixed_nontxn,recovery,snapshot,stress_consumer,wal_trim}` | 9→1 | 762 | 423 |
| `70-Cluster/test_cluster_restore_all.py` | 4 | `test_cluster_restore_{dnode,mnode,qnode,vnode}` | 4→1 | 825 | 250 |
| `05-VirtualTables/05-Plan/test_vtable_plan_scan_prune.py` | 2 | `test_vtable_plan_replace_tbname`, `test_vtable_plan_eliminate_virtual_scan`（两个文件类名/方法名完全相同，只有额外的前置数据不同） | 2→1 | 本地 39.8 s / 2 方法 | — |
| **合计** | **96** | 96 个文件 → 26 个 | **96→26** | **5776** | **2951** |

## 2. 老文件明细（96 个旧文件 → 26 个新文件）

> 每个新文件下面按行列出并入它的全部老文件；`-` 表示与上一行相同。

| 新文件 | 老文件数 | 老文件名 |
|---|---:|---|
| `01-DataTypes/test_datatype_column_values.py` | 12 |  |
| - | - | `01-DataTypes/test_datatype_bigint.py` |
| - | - | `01-DataTypes/test_datatype_bool.py` |
| - | - | `01-DataTypes/test_datatype_double.py` |
| - | - | `01-DataTypes/test_datatype_float.py` |
| - | - | `01-DataTypes/test_datatype_int.py` |
| - | - | `01-DataTypes/test_datatype_json.py` |
| - | - | `01-DataTypes/test_datatype_nchar.py` |
| - | - | `01-DataTypes/test_datatype_smallint.py` |
| - | - | `01-DataTypes/test_datatype_timestamp.py` |
| - | - | `01-DataTypes/test_datatype_tinyint.py` |
| - | - | `01-DataTypes/test_datatype_uint.py` |
| - | - | `01-DataTypes/test_datatype_varbinary.py` |
| `02-Databases/01-Create/test_db_name_rules.py` | 3 |  |
| - | - | `02-Databases/01-Create/test_name_all.py` |
| - | - | `02-Databases/01-Create/test_name_len.py` |
| - | - | `02-Databases/01-Create/test_op_table_prefix_suffix.py` |
| `02-Databases/02-Drop/test_db_drop_misc.py` | 2 |  |
| - | - | `02-Databases/02-Drop/test_db_catalog.py` |
| - | - | `02-Databases/02-Drop/test_db_repeat.py` |
| `02-Databases/03-Alter/test_db_alter_misc.py` | 4 |  |
| - | - | `02-Databases/03-Alter/test_cache_last_shards.py` |
| - | - | `02-Databases/03-Alter/test_db_alter_database.py` |
| - | - | `02-Databases/03-Alter/test_db_alter_maxrows_compact.py` |
| - | - | `02-Databases/03-Alter/test_db_alter_option_keep.py` |
| `02-Databases/04-Query/test_db_show_metadata.py` | 3 |  |
| - | - | `02-Databases/04-Query/test_db_show_create_db.py` |
| - | - | `02-Databases/04-Query/test_db_show_create_table.py` |
| - | - | `02-Databases/04-Query/test_db_show_vgroup.py` |
| `02-Databases/06-Precision/test_db_precision.py` | 3 |  |
| - | - | `02-Databases/06-Precision/test_db_precision_bugs.py` |
| - | - | `02-Databases/06-Precision/test_db_precision_ns.py` |
| - | - | `02-Databases/06-Precision/test_db_precision_us.py` |
| `03-Tables/01-NormalTables/test_normaltable_alter.py` | 7 |  |
| - | - | `03-Tables/01-NormalTables/test_normaltable_alter_1.py` |
| - | - | `03-Tables/01-NormalTables/test_normaltable_alter_2.py` |
| - | - | `03-Tables/01-NormalTables/test_normaltable_alter_3.py` |
| - | - | `03-Tables/01-NormalTables/test_normaltable_alter_4.py` |
| - | - | `03-Tables/01-NormalTables/test_normaltable_alter_5.py` |
| - | - | `03-Tables/01-NormalTables/test_normaltable_delete_reuse1.py` |
| - | - | `03-Tables/01-NormalTables/test_normaltable_delete_reuse2.py` |
| `03-Tables/01-NormalTables/test_normaltable_create_syntax.py` | 2 |  |
| - | - | `03-Tables/01-NormalTables/test_normaltable_datatypes.py` |
| - | - | `03-Tables/01-NormalTables/test_normaltable_synatx.py` |
| `03-Tables/01-NormalTables/test_normaltable_drop_reuse.py` | 1 |  |
| - | - | `03-Tables/01-NormalTables/test_normaltable_query_after_drop.py` |
| `03-Tables/01-NormalTables/test_normaltable_query.py` | 3 |  |
| - | - | `03-Tables/01-NormalTables/test_normaltable_query_aggregate.py` |
| - | - | `03-Tables/01-NormalTables/test_normaltable_query_composite.py` |
| - | - | `03-Tables/01-NormalTables/test_normaltable_query_filter.py` |
| `03-Tables/02-SubTables/test_subtable_create.py` | 4 |  |
| - | - | `03-Tables/02-SubTables/test_subtable_autocreate.py` |
| - | - | `03-Tables/02-SubTables/test_subtable_batchcreate.py` |
| - | - | `03-Tables/02-SubTables/test_subtable_drop.py` |
| - | - | `03-Tables/02-SubTables/test_subtable_query.py` |
| `03-Tables/02-SubTables/test_subtable_set_tag.py` | 2 |  |
| - | - | `03-Tables/02-SubTables/test_subtable_batch_set_tag_vals.py` |
| - | - | `03-Tables/02-SubTables/test_subtable_set_tag_vals.py` |
| `03-Tables/03-PrimaryKey/test_primary_key_ddl.py` | 2 |  |
| - | - | `03-Tables/03-PrimaryKey/test_primary_key_create.py` |
| - | - | `03-Tables/03-PrimaryKey/test_primary_key_delete.py` |
| `05-VirtualTables/05-Plan/test_vtable_plan_scan_prune.py` | 2 |  |
| - | - | `05-VirtualTables/05-Plan/test_vtable_plan_eliminate_virtual_scan.py` |
| - | - | `05-VirtualTables/05-Plan/test_vtable_plan_replace_tbname.py` |
| `11-Functions/01-Scalar/test_fun_sca_math_pow.py` | 3 |  |
| - | - | `11-Functions/01-Scalar/test_fun_sca_log.py` |
| - | - | `11-Functions/01-Scalar/test_fun_sca_pow.py` |
| - | - | `11-Functions/01-Scalar/test_fun_sca_sqrt.py` |
| `11-Functions/01-Scalar/test_fun_sca_math_round.py` | 4 |  |
| - | - | `11-Functions/01-Scalar/test_fun_sca_abs.py` |
| - | - | `11-Functions/01-Scalar/test_fun_sca_ceil.py` |
| - | - | `11-Functions/01-Scalar/test_fun_sca_floor.py` |
| - | - | `11-Functions/01-Scalar/test_fun_sca_round.py` |
| `11-Functions/01-Scalar/test_fun_sca_str_length_case.py` | 4 |  |
| - | - | `11-Functions/01-Scalar/test_fun_sca_char_length.py` |
| - | - | `11-Functions/01-Scalar/test_fun_sca_length.py` |
| - | - | `11-Functions/01-Scalar/test_fun_sca_lower.py` |
| - | - | `11-Functions/01-Scalar/test_fun_sca_upper.py` |
| `11-Functions/01-Scalar/test_fun_sca_str_trim_substr.py` | 4 |  |
| - | - | `11-Functions/01-Scalar/test_fun_sca_ltrim.py` |
| - | - | `11-Functions/01-Scalar/test_fun_sca_regexp_extract.py` |
| - | - | `11-Functions/01-Scalar/test_fun_sca_rtrim.py` |
| - | - | `11-Functions/01-Scalar/test_fun_sca_substr.py` |
| `11-Functions/01-Scalar/test_fun_sca_time_convert.py` | 3 |  |
| - | - | `11-Functions/01-Scalar/test_fun_sca_now.py` |
| - | - | `11-Functions/01-Scalar/test_fun_sca_to_timestamp.py` |
| - | - | `11-Functions/01-Scalar/test_fun_sca_today.py` |
| `11-Functions/01-Scalar/test_fun_sca_time_format.py` | 3 |  |
| - | - | `11-Functions/01-Scalar/test_fun_sca_timetruncate.py` |
| - | - | `11-Functions/01-Scalar/test_fun_sca_to_iso8601.py` |
| - | - | `11-Functions/01-Scalar/test_fun_sca_to_unixtimestamp.py` |
| `11-Functions/01-Scalar/test_fun_sca_trig.py` | 6 |  |
| - | - | `11-Functions/01-Scalar/test_fun_sca_acos.py` |
| - | - | `11-Functions/01-Scalar/test_fun_sca_asin.py` |
| - | - | `11-Functions/01-Scalar/test_fun_sca_atan.py` |
| - | - | `11-Functions/01-Scalar/test_fun_sca_cos.py` |
| - | - | `11-Functions/01-Scalar/test_fun_sca_sin.py` |
| - | - | `11-Functions/01-Scalar/test_fun_sca_tan.py` |
| `11-Functions/02-Aggregate/test_agg_distinct_decimal_spill.py` | 2 |  |
| - | - | `11-Functions/02-Aggregate/test_agg_distinct_decimal.py` |
| - | - | `11-Functions/02-Aggregate/test_agg_distinct_spill.py` |
| `11-Functions/02-Aggregate/test_agg_group_concat.py` | 2 |  |
| - | - | `11-Functions/02-Aggregate/test_agg_gconcat.py` |
| - | - | `11-Functions/02-Aggregate/test_agg_smoking.py` |
| `11-Functions/02-Aggregate/test_fun_agg_distribute_avg_spread.py` | 2 |  |
| - | - | `11-Functions/02-Aggregate/test_fun_agg_avg_dist.py` |
| - | - | `11-Functions/02-Aggregate/test_fun_agg_spread.py` |
| `21-MetaData/test_taosx_txn_replication.py` | 9 |  |
| - | - | `21-MetaData/test_taosx_txn_basic.py` |
| - | - | `21-MetaData/test_taosx_txn_boundary.py` |
| - | - | `21-MetaData/test_taosx_txn_concurrent_sessions.py` |
| - | - | `21-MetaData/test_taosx_txn_ddl_matrix.py` |
| - | - | `21-MetaData/test_taosx_txn_mixed_nontxn.py` |
| - | - | `21-MetaData/test_taosx_txn_recovery.py` |
| - | - | `21-MetaData/test_taosx_txn_snapshot.py` |
| - | - | `21-MetaData/test_taosx_txn_stress_consumer.py` |
| - | - | `21-MetaData/test_taosx_txn_wal_trim.py` |
| `70-Cluster/test_cluster_restore_all.py` | 4 |  |
| - | - | `70-Cluster/test_cluster_restore_dnode.py` |
| - | - | `70-Cluster/test_cluster_restore_mnode.py` |
| - | - | `70-Cluster/test_cluster_restore_qnode.py` |
| - | - | `70-Cluster/test_cluster_restore_vnode.py` |
| **合计** | **96** | **26 个新文件** |

## 3. 覆盖率评估

| 检查 | 方法 | 结果 |
|---|---|---|
| 方法级 | 逐个 `def`：旧文件每个 test 方法须在新文件里以原名或 `<来源tag>_原名` 存在 | 旧 **126** = 新 **126**，缺失 **0** |
| 注册元组级 | 展开 `(优先级/重试/sanitizer/workdir, env, 文件, node, 部署参数)` 后比对集合 | `MISSING 0 / ADDED 0`，`COVERAGE-NEUTRAL: YES` |
| node id 级 | `cases.task` 展开后的 node id 总数 | **1728 → 1728** |
| 可执行性 | 29 条多目标合并行 `pytest --collect-only` | 全部 `rc=0` |
| 边界 | 不同部署参数（`-N 3 --replica 3`、`-Q 2/3`、`FQ_REQUIRED_SERVICES`）一律不跨组合并；`sanitizer=n` 的 `,,n` 行不并入 `,,y` 组 | 已遵守 |
| 端到端 | CI：MR !3864 child 84943 / 85487 / 85888 | pytest 全通过（85888 仅剩 main 侧 SMA LSan 与历史 flaky 的 `test_db_create_encrypt`），unit-test 全通过 |

## 4. 仍然引用旧文件名的文档（登记，不改内容）

`docs/overview/03-各模块设计/内核模块/` 下有 6 份**设计/测试例汇编**文档，它们的「脚本名称」列记录的是合并前的文件名。这些是按发布节点冻结的设计记录，为避免改写历史记录，这里**不改这 6 份文档**，只在此登记；查新文件请用本文件 §2 的映射。

| 文档 | 引用到旧文件 | 处数 |
|---|---:|---:|
| `时序数据存储模块/时序数据存储模块-测试例汇编.md` | 33 | 65 |
| `时序数据查询模块/时序数据查询模块-测试例汇编.md` | 24 | 53 |
| `集群管理模块/集群管理模块-测试例汇编.md` | 4 | 8 |
| `基础库模块/基础库模块-Test Spec.md` | 1 | 5 |
| `跨平台模块/跨平台模块-Test Spec.md` | 1 | 5 |
| `多副本管理模块/多副本管理模块-Test Spec.md` | 1 | 1 |
| **合计** | **64** | **137** |

已同步修正的引用：

| 位置 | 处理 |
|---|---|
| `.agents/skills/tsdb-test-pytest-standard/SKILL.md` 的示例命令 | 改为 `cases/01-DataTypes/test_datatype_column_values.py -N 3` |
| `source/taos-community/test/ci/tdgpt_cases.task` 表头示例行 | 同上（该行被注释，不影响执行） |
| `source/taos-community/test/tools/streamlist.txt` 注释行 | 同上（未被任何脚本引用） |
| `source/taos-community/test/ci/test_windows_case_utils.py` | 只是把旧文件名当字符串样例做断言，不涉及真实路径，保留 |
