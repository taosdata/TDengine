# TDengine 测试失败分析报告 (2026-03-05)

## 测试环境信息

| 项目 | 内容 |
|------|------|
| 测试时间 | 2026-03-05 |
| 测试平台 | Windows (win32) |
| Python版本 | 3.8.9 |
| 测试框架 | pytest 7.4.0 |
| 超时设置 | 1200秒 (20分钟) |
| 总用例数 | 25个 |
| 失败情况 | FAILED: 20个, TIMEOUT: 4个, ERROR: 2个 |

---

## 失败分类汇总

### 一、Python 版本兼容性问题 (语法错误)

**数量：3个**

| 序号 | 用例名 | 问题描述 | 修复建议 |
|------|--------|----------|----------|
| 1 | test_datatype_decimal.py | `list[int]` 语法在 Python 3.8 不支持 | 改为 `List[int]` |
| 2 | test_datatype_decimal64.py | `list[int]` 语法在 Python 3.8 不支持 | 改为 `List[int]` |
| 3 | test_stable_alter_overall.py | `with` 语句括号换行语法在 Python 3.8 不支持 | 改为嵌套 with 语句 |

**典型错误信息：**
```
TypeError: 'type' object is not subscriptable
# 或
SyntaxError: invalid syntax (with 语句 as 后换行)
```

---

### 二、事务冲突问题 (Conflict transaction not completed - 0x03d3)

**数量：9个**

错误码：`0x03d3` (-2147482669)

| 序号 | 用例名 | 失败操作 | 测试场景 |
|------|--------|----------|----------|
| 1 | test_balance1.py | balance vgroup | 4节点集群balance |
| 2 | test_balance_replica_1.py | balance vgroup | 3节点集群balance |
| 3 | test_balance_vnode_clean.py | balance vgroup | 停止/启动dnode后balance |
| 4 | test_balancex.py | balance vgroup | 超时卡住 |
| 5 | test_redistribute_vgroup_replica2.py | CREATE STABLE | 5节点redistribute |
| 6 | test_redistribute_vgroup_replica3_v3.py | CREATE STABLE | 8节点redistribute |
| 7 | test_db_alter_replica.py | alter database replica | 修改数据库副本数 |
| 8 | test_db_wal_level.py | INSERT | WAL level变更后插入 |
| 9 | test_split_vgroup.py | split vgroup | 超时300秒 |

**典型错误信息：**
```
taos.error.ProgrammingError: [0x03d3]: Conflict transaction not completed
```

---

### 三、集群同步/Leader选举问题

**数量：5个**

| 序号 | 用例名 | 错误信息 | 具体原因 |
|------|--------|----------|----------|
| 1 | test_balance_leader.py | vgId:4 no leader within 100s | vgroup无法选举leader |
| 2 | test_vnode_replica3_basic.py | vgId:2 no leader within 100s | 3副本vgroup无法选举leader |
| 3 | test_redistribute_vgroup_replica3_v1_follower.py | vgId:2 no leader within 100s | follower场景leader选举失败 |
| 4 | test_redistribute_vgroup_replica3_v1_leader.py | vgId:2 no leader within 100s | leader场景leader选举失败 |
| 5 | test_balance3.py | Sync leader is restoring | balance后leader恢复中 |

**典型错误信息：**
```
db:d1 vgId:2 no leader within 100s!
# 或
[0x0914]: Sync leader is restoring
```

---

### 四、超时问题 (Timeout)

**数量：4个**

| 序号 | 用例名 | 超时位置 | 超时时间 | 可能原因 |
|------|--------|----------|----------|----------|
| 1 | test_balancex.py | taos_query | 1200s | balance vgroup 卡住 |
| 2 | test_redistribute_vgroup_replica1.py | redistribute vgroup 2 dnode 3 | 1200s | 生成加密密钥卡住 |
| 3 | test_split_vgroup.py (replica_1) | split vgroup transaction | 300s | split操作未完成 |
| 4 | test_split_vgroup.py (replica_3) | split vgroup transaction | 300s | split操作未完成 |

---

### 五、Windows 环境问题/权限问题

**数量：4个**

| 序号 | 用例名 | 错误信息 | 具体原因 |
|------|--------|----------|----------|
| 1 | test_db_retention.py | [WinError 32] The process cannot access the file | .running文件被占用 |
| 2 | test_stable_create_presuf.py | Create taoslog failed:No error | C:\TDengine\cfg\ 目录权限问题 |
| 3 | test_stable_create_rowlength64k_benchmark.py | Create taoslog failed | taosBenchmark无法创建日志 |
| 4 | test_datatype_decimal_last.py | monitorFqdn failed since Invalid configuration value | 配置值无效+代码异常处理缺陷 |

---

### 六、数据一致性问题

**数量：1个**

| 序号 | 用例名 | 问题描述 |
|------|--------|----------|
| 1 | test_stable_drop_delfile.py | drop table + compact 后 STT 文件未删除，等待17分钟后失败 |

**典型错误信息：**
```
check file can not be deleted. file=v2f1851ver1.stt file size=32768
```

---

### 七、功能/配置问题

**数量：1个**

| 序号 | 用例名 | 问题描述 |
|------|--------|----------|
| 1 | test_db_create_encrypt.py | show encrypt_algorithms 返回1行，期望6行；taosk工具未找到 |

---

## 统计总结

| 问题类型 | 数量 | 占比 | 优先级 |
|----------|------|------|--------|
| 🟥 事务冲突问题 (0x03d3) | 9 | 36% | 高 |
| 🟥 集群同步/Leader选举问题 | 5 | 20% | 高 |
| 🟧 超时问题 | 4 | 16% | 高 |
| 🟨 Python版本兼容性问题 | 3 | 12% | 中 |
| 🟨 Windows环境问题 | 4 | 16% | 中 |
| 🟨 数据一致性问题 | 1 | 4% | 中 |
| 🟩 功能/配置问题 | 1 | 4% | 低 |
| **总计** | **27** | **100%** | |

---

## 问题根因分析

### 1. 事务冲突问题 (最普遍)

| 项目 | 内容 |
|------|------|
| 影响 | 9个用例 (36%) |
| 错误码 | 0x03d3 |
| 根因 | 集群操作后事务未完成，balance/redistribute/split等操作与未完成事务冲突 |
| 建议 | 增加事务完成检查，延长重试间隔，添加集群状态稳定检测 |

### 2. 集群同步/Leader选举问题

| 项目 | 内容 |
|------|------|
| 影响 | 5个用例 (20%) |
| 错误信息 | no leader within 100s / Sync leader is restoring |
| 根因 | Windows平台上vgroup leader选举机制异常，Raft选举无法正常完成 |
| 建议 | 检查Windows平台的同步模块实现，增加选举超时诊断日志 |

### 3. 超时问题

| 项目 | 内容 |
|------|------|
| 影响 | 4个用例 (16%) |
| 根因 | split vgroup、redistribute vgroup、balance vgroup 操作卡住 |
| 建议 | 检查这些长时间运行操作的进度反馈机制，排查死锁可能 |

### 4. Python版本兼容性问题

| 项目 | 内容 |
|------|------|
| 影响 | 3个用例 (12%) |
| 根因 | 代码使用了Python 3.9+语法 (`list[int]`、with语句括号换行)，但环境是Python 3.8.9 |
| 建议 | 统一使用 `typing.List` 或升级Python版本到3.10+ |

---

## 建议措施

### 短期 (1-2周)

- [ ] 修复 Python 3.8 兼容性问题（3个用例）
  - `test_datatype_decimal.py` 第266行：`list[int]` → `List[int]`
  - `test_datatype_decimal64.py` 第317行：`list[int]` → `List[int]`
  - `common.py` 第3025行：修复with语句换行语法
- [ ] 检查 Windows 环境 `C:\TDengine\` 目录权限和配置
- [ ] 增加事务冲突后的等待时间和重试机制

### 中期 (1个月)

- [ ] 调查 `balance vgroup` 在 Windows 上的事务冲突问题
- [ ] 修复 vgroup leader 选举在 Windows 上的超时问题
- [ ] 调查 split vgroup 操作在 Windows 上的性能/死锁问题

### 长期 (3个月)

- [ ] 完善 Windows 平台的集群同步机制
- [ ] 优化长时间运行集群操作的稳定性
- [ ] 建立 Windows 平台 CI 测试环境，避免版本兼容性问题

---

## 附录：关键错误码对照表

| 错误码 | 错误信息 | 说明 |
|--------|----------|------|
| 0x03d3 | Conflict transaction not completed | 事务冲突未完成 |
| 0x0914 | Sync leader is restoring | 同步Leader恢复中 |
| 0x2603 | Table does not exist | 表不存在 |
| 0x0388 | Database not exist | 数据库不存在 |
| 0x80000133 | Create taoslog failed | 创建日志失败 |
| N/A | no leader within 100s | Leader选举超时 |

---

*报告生成时间：2026-03-05*
*分析人员：Kimi Code CLI*