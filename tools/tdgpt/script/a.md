# TDengine 测试失败分析报告

## 测试环境信息

| 项目 | 内容 |
|------|------|
| 测试时间 | 2025-10-10 至 2025-10-11 |
| 测试平台 | Windows (win32) |
| Python版本 | 3.8.9 |
| 测试框架 | pytest 7.4.0 |
| 超时设置 | 1200秒 (20分钟) |
| 总用例数 | 59个 |
| 失败用例数 | 58个 |

---

## 失败分类汇总

### 一、平台兼容性问题 (Windows平台不支持的功能)

**数量：13个**

这类问题是由于 TDengine 的某些功能在 Windows 平台上不支持导致的。错误码通常为 `0x0316` (-2147482858)。

| 序号 | 用例名 | 错误码 | 具体原因 |
|------|--------|--------|----------|
| 1 | stream_schema.py | 0x0316 | STREAM_OPTIONS 功能在 Windows 不支持 |
| 2 | stream_td37724.py | 0x0316 | _tlocaltime 等时间变量在流中不支持 |
| 3 | test_idmp_privilege.py | 0x0316 | CREATE STREAM 功能在 Windows 不支持 |
| 4 | test_tsma.py | 0x0316 | TSMA (时序物化聚合) 功能不支持 |
| 5 | test_subquery_vtable_change.py | 0x0316 | state_window 流功能不支持 |
| 6 | test_fun_select_cols.py | 0x0316 | cols() + CREATE STREAM 功能不支持 |
| 7 | test_state_window_extend.py | 0x0316 | count_window + state_window 组合不支持 |
| 8 | test_tmqVnodeTransform_stb_removewal.py | 0x0316 | TMQ 流处理功能不支持 |
| 9 | test_tmqConsFromTsdb.py | 事务冲突 | TMQ 消费功能事务冲突 |
| 10 | test_tmqConsFromTsdb1.py | 0x0316 | TMQ 消费功能不支持 |
| 11 | test_vtable_query_cross_db_stb_group.py | 0x0316 | 虚拟表跨库查询功能不支持 |
| 12 | test_wal_level_skip.py | 0x2603 | WAL level 0 时创建的表重启后丢失 |

---

### 二、事务冲突问题 (Conflict transaction not completed)

**数量：23个 (最普遍)**

错误码：`0x03d3` (-2147482669)，表明集群中存在未完成的冲突事务。

| 序号 | 用例名 | 失败操作 |
|------|--------|----------|
| 1 | test_5dnode3mnode_add_1dnode.py | CREATE STABLE |
| 2 | test_5dnode3mnode_sep1_vnode_stop_dnode_modify_meta.py | CREATE STABLE |
| 3 | test_6dnode3mnode_insert_less_data_alter_rep3to1to3.py | ALTER REPLICA |
| 4 | test_restore_dnode.py | CREATE STABLE |
| 5 | test_restore_vnode.py | CREATE STABLE |
| 6 | test_balancex.py | balance vgroup |
| 7 | test_balance_replica_1.py | balance vgroup |
| 8 | test_balance_vgroups_r1.py | balance vgroup |
| 9 | test_db_alter_replica.py | alter database replica |
| 10 | test_redistribute_vgroup_replica1.py | redistribute vgroup |
| 11 | test_redistribute_vgroup_replica2.py | redistribute vgroup |
| 12 | test_redistribute_vgroup_replica3_v3.py | redistribute vgroup |
| 13 | test_arbitrator.py | CREATE STABLE |
| 14 | test_arbitrator_restart.py | CREATE STABLE |
| 15 | test_split_vgroup.py (replica_1) | split vgroup |
| 16 | test_split_vgroup.py (replica_3) | split vgroup |
| 17 | test_split_vgroup_wal.py (replica_1) | split vgroup |
| 18 | test_split_vgroup_wal.py (replica_3) | split vgroup |
| 19 | test_tmqVnodeSplit_ntb_select.py | CREATE TABLE |
| 20 | test_tmqVnodeSplit_stb_select.py (N=2) | CREATE TABLE |
| 21 | test_tmqVnodeSplit_stb_select.py (N=3) | CREATE TABLE |
| 22 | test_tmqVnodeSplit_stb_select_duplicatedata.py | CREATE TABLE |
| 23 | test_tmqVnodeSplit_stb_select_duplicatedata_false.py | CREATE TABLE |

**典型错误信息：**
```
taos.error.ProgrammingError: [0x03d3]: Conflict transaction not completed
```

---

### 三、超时问题 (Timeout - 1200s)

**数量：11个**

测试执行超过 1200 秒（20分钟）超时限制。

| 序号 | 用例名 | 超时位置 | 可能原因 |
|------|--------|----------|----------|
| 1 | test_balance1.py | balance vgroup 命令 | 集群平衡操作卡住 |
| 2 | test_balance_vnode_clean.py | split vgroup | vgroup拆分操作未完成 |
| 3 | test_commandline.py | taosBenchmark -I sml | Windows命令管道问题 |
| 4 | test_query_json.py | taosBenchmark 查询 | 查询执行卡住 |
| 5 | test_query_json_mixed_query.py | taosBenchmark 查询 | 查询执行卡住 |
| 6 | test_subscribeDb.py | event.wait() | 消费者线程阻塞 |
| 7 | test_subscribeDb0.py | event.wait() | 消费者线程阻塞 |
| 8 | test_taosdemo_test_query_with_json.py | taosBenchmark | 查询执行卡住 |
| 9 | test_taosdemo_test_query_with_json_mixed_query.py | taosBenchmark | 查询执行卡住 |
| 10 | test_tmqVnodeTransform_db_removewal.py | 消费等待 | 消费者线程阻塞 |
| 11 | test_vtable_query.py | taos 命令执行 | SQL查询执行卡住 |

---

### 四、集群同步/高可用性问题

**数量：4个**

| 序号 | 用例名 | 错误码 | 错误信息 | 具体原因 |
|------|--------|--------|----------|----------|
| 1 | test_tsdb_snapshot.py | 0x8000090c | Sync leader is unreachable | 停止1个节点后无法选举新Leader |
| 2 | test_vnode_replica3_import.py | - | vgId:2 no leader | vgroup无法选举leader |
| 3 | test_sync_3Replica1VgElect.py | 0x03d3 | Conflict transaction not completed | 同步选举事务未完成 |
| 4 | test_offline_reason.py | - | dnode not ready within 100s | 节点停止后状态未更新 |

**典型错误信息：**
```
ERROR: Call engine failed. error code: 0x8000090c, reason: Sync leader is unreachable
```

---

### 五、环境问题/工具兼容性问题

**数量：3个**

| 序号 | 用例名 | 问题描述 |
|------|--------|----------|
| 1 | test_commandline.py | Windows不支持 `grep`, `wc` 等Linux命令，命令管道语法 `2>&1 \| grep sleep \| wc -l` 导致超时 |
| 2 | test_passwd.py | `jom -f makefile_win64.mak` 编译工具执行失败 |
| 3 | test_taos_shell.py | `taos -h WIN-1PODEDDU3J1` 主机名连接失败 |

---

### 六、时区处理问题

**数量：2个**

| 序号 | 用例名 | 问题描述 |
|------|--------|----------|
| 1 | test_interval_timezone.py | 时间戳结果存在1小时偏移（时区计算错误），预期 `00:00:00` 实际 `01:00:00` |
| 2 | test_dismatch_config.py | 时区偏移量符号错误，预期 `Asia/Shanghai (UTC, +0800)` 实际 `Asia/Shanghai (UTC, -0800)` |

---

### 七、数据一致性问题

**数量：2个**

| 序号 | 用例名 | 问题描述 |
|------|--------|----------|
| 1 | test_stt.py | 数据计数不匹配，实际 2,825,374 vs 预期 2,825,119，差 255 行 |
| 2 | test_delete_check.py | drop table + compact 后 STT 文件未删除，等待 17 分钟后失败 |

---

### 八、数据库内部错误

**数量：1个**

| 序号 | 用例名 | 错误码 | 错误信息 | 具体原因 |
|------|--------|--------|----------|----------|
| 1 | test_stablity_1.py | 0x02ff | Internal error | double边界值查询触发内部错误 |

**典型错误信息：**
```
taos.error.ProgrammingError: [0x02ff]: Internal error
```

---

## 问题根因分析

### 1. 事务冲突问题 (最普遍)

| 项目 | 内容 |
|------|------|
| 影响 | 23个用例 (约39%) |
| 根因 | 集群操作后事务未完成，可能与 Windows 平台的信号处理、进程终止方式有关 |
| 建议 | 增加事务超时重试机制，或延长集群状态检查等待时间 |

### 2. 平台兼容性问题

| 项目 | 内容 |
|------|------|
| 影响 | 13个用例 (约22%) |
| 根因 | Stream 处理、TMQ 等高级功能在 Windows 平台未完全支持 |
| 建议 | 在 Windows 测试环境中跳过这些用例，或增加平台检测逻辑 |

### 3. 超时问题

| 项目 | 内容 |
|------|------|
| 影响 | 11个用例 (约19%) |
| 根因 | `balance vgroup` 等集群操作卡住；`taosBenchmark` 查询执行死锁或无限等待；TMQ 消费者线程阻塞 |
| 建议 | 检查集群同步机制，增加操作进度日志 |

### 4. 集群同步问题

| 项目 | 内容 |
|------|------|
| 影响 | 4个用例 |
| 根因 | 节点故障后 leader 选举机制在 Windows 上工作异常 |
| 建议 | 检查 Windows 平台的同步模块实现 |

---

## 统计总结

| 问题类型 | 数量 | 占比 | 优先级 |
|----------|------|------|--------|
| 🟥 事务冲突问题 | 23 | 39% | 高 (需修复) |
| 🟧 超时问题 | 11 | 19% | 高 (需调查) |
| 🟨 平台兼容性问题 | 13 | 22% | 中 (需文档说明或跳过) |
| 🟥 集群同步问题 | 4 | 7% | 高 (需修复) |
| 🟩 环境问题 | 3 | 5% | 低 (测试框架问题) |
| 🟨 时区处理问题 | 2 | 3% | 中 (需修复) |
| 🟨 数据一致性问题 | 2 | 3% | 中 (需调查) |
| 🟨 数据库内部错误 | 1 | 2% | 中 (需修复) |
| **总计** | **59** | **100%** | |

---

## 建议措施

### 短期 (1-2周)

- [ ] 在 Windows 测试环境中标记平台不支持的用例为跳过 (`@pytest.mark.skipif`)
- [ ] 增加事务冲突检测后的重试机制（当前已重试10次，可考虑增加间隔或总次数）
- [ ] 修复时区偏移量计算符号错误

### 中期 (1个月)

- [ ] 调查 `balance vgroup` 在 Windows 上的阻塞问题
- [ ] 调查 TMQ 消费者线程在 Windows 上的阻塞问题
- [ ] 修复 STT 文件清理机制

### 长期 (3个月)

- [ ] 完善 Windows 平台的集群同步机制
- [ ] 增加 Stream/TMQ 功能在 Windows 上的支持
- [ ] 优化 Windows 平台的信号处理和进程管理

---

## 附录：关键错误码对照表

| 错误码 | 错误信息 | 说明 |
|--------|----------|------|
| 0x0316 | Unsupported feature on this platform | 平台不支持该功能 |
| 0x03d3 | Conflict transaction not completed | 事务冲突未完成 |
| 0x2603 | Table does not exist | 表不存在 |
| 0x02ff | Internal error | 数据库内部错误 |
| 0x8000090c | Sync leader is unreachable | 同步Leader不可达 |

---

*报告生成时间：2025-03-05*
*分析人员：Kimi Code CLI*