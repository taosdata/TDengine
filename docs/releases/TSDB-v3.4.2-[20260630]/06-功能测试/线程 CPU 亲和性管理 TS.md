# 功能测试报告（Test Spec）— 线程 CPU 亲和性管理

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-04-16 | 2026-04-16 | 1.0 | dmchen | 初始版本 |

## 2. 测试目标

- 验证 `enableCpuAffinity` 主开关正确控制 CPU 亲和性的全局启用/禁用行为
- 验证 `managementCpuCores` 参数正确将指定数量的核心分配给管理线程
- 验证 `readCpuCores` 和 `otherCpuCores` 参数正确将指定数量的核心分配给读取和写入线程
- 验证 `SHOW CPU_ALLOCATION` 和 `information_schema.ins_cpu_allocation` 返回正确的分配状态
- 验证边界条件处理（最小核心保证、顺序分配、数据操作兼容性、重启恢复）
- 验证向后兼容性（升级不改变默认行为）

## 3. 参考文档

- 概要设计说明书：`线程 CPU 亲和性管理 FS.md`
- 详细设计说明书：`线程 CPU 亲和性管理 DS.md`

## 4. 测试结论

全部 21 个测试用例通过，覆盖 4 个用户故事和边界条件验证。

| 测试文件 | 用例数 | 通过 | 失败 | 跳过 | 耗时 |
| --- | --- | --- | --- | --- | --- |
| test_cpu_affinity_switch.py | 4 | 4 | 0 | 0 | 5.04s |
| test_cpu_management_cores.py | 3 | 3 | 0 | 0 | 4.80s |
| test_cpu_read_write_ratio.py | 4 | 4 | 0 | 0 | 7.87s |
| test_cpu_show_allocation.py | 6 | 6 | 0 | 0 | 4.54s |
| test_cpu_affinity_edge_cases.py | 4 | 4 | 0 | 0 | 10.29s |
| **合计** | **21** | **21** | **0** | **0** | **32.54s** |

## 5. 测试环境

- **OS**: Ubuntu 22.04.3 LTS (Linux)
- **CPU**: 4 cores（容器环境）
- **TDengine**: v3.4.1.0.alpha.enterprise
- **分支**: 003-thread-cpu-affinity
- **测试框架**: pytest 8.3.5 + TDengine new_test_framework
- **Python**: 3.10.12

## 6. 功能测试

### 6.1 US1: 主开关启用/禁用（enableCpuAffinity）

#### 6.1.1 测试要点

- 主开关关闭（默认值 0）时，所有线程无亲和性限制
- 主开关关闭时，`SHOW CPU_ALLOCATION` 返回 `enabled=false`, `cores=0`, `core_ids="-"`
- 主开关关闭时，`managementCpuCores`、`readCpuCores` 和 `otherCpuCores` 配置值保留但不生效
- 主开关开启时，线程具有受限的 CPU 亲和性掩码
- 默认安装/升级不改变行为（向后兼容）

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| T003 | test_switch_off_default | 部署 taosd（enableCpuAffinity=0），验证 `SHOW CPU_ALLOCATION` 返回 3 行均为 enabled=false, cores=0, core_ids="-"。通过 /proc/\<pid\>/task/\*/status 验证所有线程无受限亲和性掩码。 | PASS |
| T004 | test_switch_on | 部署 taosd（enableCpuAffinity=1, managementCpuCores=1, readCpuCores=1, otherCpuCores=2），验证 `SHOW CPU_ALLOCATION` 返回 3 行均为 enabled=true，cores>0，core_ids 为有效整数列表。通过 /proc 验证线程具有受限亲和性掩码。 | PASS |
| T005 | test_switch_off_preserves_config | 部署 taosd（enableCpuAffinity=0, managementCpuCores=4, readCpuCores=2, otherCpuCores=3），验证 DNODE VARIABLES 中 managementCpuCores=4, readCpuCores=2, otherCpuCores=3。验证 `SHOW CPU_ALLOCATION` 仍为 enabled=false。 | PASS |
| T006 | test_upgrade_no_behavior_change | 部署 taosd（未显式配置 enableCpuAffinity），验证 DNODE VARIABLES 中默认值为 0。验证 `SHOW CPU_ALLOCATION` 为 enabled=false。 | PASS |

### 6.2 US2: 管理线程核心配置（managementCpuCores）

#### 6.2.1 测试要点

- 默认管理核心数 = 1，分配核心 ID = "0"
- 自定义管理核心数 = 2，分配核心 ID = "0,1"
- 剩余核心正确分配给读取和写入线程
- 配置值可通过 DNODE VARIABLES 查询

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| T007 | test_default_management_cores | 部署 taosd（managementCpuCores=1），验证 management 行 cores=1, core_ids="0"。验证 write+read 行的核心数之和 = total-1。 | PASS |
| T008 | test_custom_management_cores | 部署 taosd（managementCpuCores=2），验证 management 行 cores=2, core_ids="0,1"。验证剩余核心 = total-2 分配给 write 和 read。 | PASS |
| T009 | test_management_cores_via_dnode_variables | 查询 `SHOW DNODE 1 VARIABLES LIKE 'managementCpuCores'`，验证返回值 = 1。 | PASS |

### 6.3 US3: 读写核心配置（readCpuCores / otherCpuCores）

#### 6.3.1 测试要点

- readCpuCores=2, otherCpuCores=2：读写核心相等
- readCpuCores=1, otherCpuCores=2：写入线程获得更多核心
- readCpuCores=2, otherCpuCores=1：读取线程获得更多核心
- 验证 managementCpuCores + readCpuCores + otherCpuCores <= totalCores
- 各分类的 core_ids 互不重叠

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| T010 | test_cores_equal | readCpuCores=2, otherCpuCores=2。验证 read 和 write 核心数符合配置。验证总核心 = managementCpuCores + readCpuCores + otherCpuCores。验证 core_ids 互不重叠。 | PASS |
| T011 | test_write_heavy_cores | readCpuCores=1, otherCpuCores=2。验证 write 核心=2，read 核心=1。验证分配符合配置值。 | PASS |
| T012 | test_read_heavy_cores | readCpuCores=2, otherCpuCores=1。验证 read 核心=2，write 核心=1。验证分配符合配置值。 | PASS |
| T013 | test_custom_cores_allocation | readCpuCores=1, otherCpuCores=2, managementCpuCores=1。验证 management+read+write 核心分配符合配置。验证 core_ids 顺序和互不重叠。 | PASS |

### 6.4 US4: CPU 分配可观测性（SHOW CPU_ALLOCATION）

#### 6.4.1 测试要点

- `SHOW CPU_ALLOCATION` 返回所有 dnode 的结果，每个 dnode 3 行、5 列 schema（含 dnode_id）
- `information_schema.ins_cpu_allocation` 返回相同结果
- 单节点集群：3 行，dnode_id=1；多节点集群：N×3 行
- core_ids 在 [0, cpu_count) 范围内，完整覆盖所有核心，分类间无重叠
- 启用时 DNODE VARIABLES 显示 enableCpuAffinity=1
- 禁用时显示 enabled=false

#### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| T014 | test_show_cpu_allocation_schema | 验证 `SHOW CPU_ALLOCATION` 返回 3 行（单节点）。验证 dnode_id=1。验证 thread_category 值为 management, write, read。验证 5 列类型正确。 | PASS |
| T015 | test_information_schema_table | 验证 `SELECT * FROM information_schema.ins_cpu_allocation` 返回与 SHOW 完全一致的结果。 | PASS |
| T016 | test_show_cpu_allocation_core_ids_valid | 解析 core_ids 为整数。验证每个 ID 在 [0, cpu_count) 范围内。验证分类间无重叠。验证并集 = 全部核心。 | PASS |
| T017 | test_show_cpu_allocation_disabled | enableCpuAffinity=0 时，验证 3 行全部 enabled=false, cores=0, core_ids="-"。 | PASS |
| T018a | test_dnode_variables_show_switch_enabled | enableCpuAffinity=1 时，验证 DNODE VARIABLES 返回值为 1。 | PASS |
| T018b | test_dnode_variables_show_switch_disabled | enableCpuAffinity=0 时，验证 DNODE VARIABLES 返回值为 0。 | PASS |

### 6.5 边界条件和验证

#### 6.5.1 测试要点

- 核心 ID 顺序分配（management=[0..M-1], write=[M..M+W-1], read=[M+W..total-1]）
- 三类核心数之和 = 系统 CPU 总数
- 启用亲和性后基本数据操作（建库、建表、写入、查询）正常
- 重启后 CPU 分配恢复，数据不丢失

#### 6.5.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| T019 | test_sequential_core_assignment | 验证 management 得到 [0..M-1]，write 得到 [M..M+W-1]，read 得到 [M+W..total-1]。 | PASS |
| T020 | test_cores_sum_equals_total | 验证 3 行 cores 之和 = 系统 CPU 核心总数。 | PASS |
| T021 | test_basic_data_operations_with_affinity | 启用亲和性后执行 CREATE DATABASE、CREATE TABLE、INSERT、SELECT。验证所有操作成功。 | PASS |
| T022 | test_restart_with_affinity | 启用亲和性后重启 taosd（stop/start）。验证重启后 SHOW CPU_ALLOCATION 返回相同分配。验证重启前写入的数据可查询。 | PASS |

## 7. 易用性测试（可选）

无。本特性为后端配置特性，无 UI 交互。

## 8. 长期稳定性测试（可选）

暂无。建议后续在长稳测试中加入 CPU 亲和性启用状态的测试。

## 9. 性能测试

- CPU 亲和性设置仅在线程创建时执行一次（`pthread_setaffinity_np`），无持续运行时开销
- `SHOW CPU_ALLOCATION` 直接读取内存全局变量，响应时间 < 1ms
- 测试 T021 验证了启用亲和性后基本数据操作（写入 + 查询）正常工作

## 10. 安全测试

- CPU 亲和性是 OS 级别的线程调度特性，不涉及权限提升或数据泄漏
- 配置参数通过 taos.cfg 管理，受文件系统权限保护
- `SHOW CPU_ALLOCATION` 为只读查询，无安全风险

## 11. 兼容性测试

| # | 测试场景 | 测试结果 |
| --- | --- | --- |
| 1 | 升级安装后，未配置 enableCpuAffinity，验证默认值为 0，行为与升级前一致 | PASS（T006 覆盖） |
| 2 | 启用亲和性后，配置 enableCpuAffinity=0 并重启，验证恢复为默认行为 | PASS（通过 T003/T004 组合验证） |
| 3 | 配置 managementCpuCores、readCpuCores 和 otherCpuCores 但主开关关闭，验证参数保留不生效 | PASS（T005 覆盖） |

## 12. 已知问题和限制

- **TDengine 配置持久化**：TDengine v3.4+ 将运行时配置持久化到 `data/dnode/config/local.json`。当同一 pytest 会话中切换 `enableCpuAffinity` 值（如一个测试类设为 0，另一个设为 1）时，持久化的值会覆盖新的 taos.cfg。解决方案：在测试目录下添加了 `conftest.py`，在测试类切换时清理持久化配置目录。
- **平台限制**：macOS 和 Windows 平台不支持实际的 CPU 亲和性绑定，`taosSetCpuAffinity()` 为 no-op。
- **最少核心数**：系统 CPU 核心 < 3 时自动禁用亲和性。
- **动态修改**：不支持运行时动态修改 CPU 亲和性配置，需重启 taosd。

### 测试文件清单

| 文件路径 | 说明 |
| --- | --- |
| community/test/cases/34-CpuAffinity/\_\_init\_\_.py | 包初始化文件 |
| community/test/cases/34-CpuAffinity/conftest.py | 本地 conftest，清理配置持久化 |
| community/test/cases/34-CpuAffinity/cpu_affinity_utils.py | 测试工具函数 |
| community/test/cases/34-CpuAffinity/test_cpu_affinity_switch.py | US1 主开关测试 (4 用例) |
| community/test/cases/34-CpuAffinity/test_cpu_management_cores.py | US2 管理核心测试 (3 用例) |
| community/test/cases/34-CpuAffinity/test_cpu_read_write_ratio.py | US3 读写比例测试 (4 用例) |
| community/test/cases/34-CpuAffinity/test_cpu_show_allocation.py | US4 可观测性测试 (6 用例) |
| community/test/cases/34-CpuAffinity/test_cpu_affinity_edge_cases.py | 边界条件测试 (4 用例) |

### CI 注册

5 个测试文件已注册到 `community/test/ci/cases.task` 的 `# 34-CpuAffinity` 分组下：

```
,,y,.,./ci/pytest.sh pytest cases/34-CpuAffinity/test_cpu_affinity_switch.py
,,y,.,./ci/pytest.sh pytest cases/34-CpuAffinity/test_cpu_management_cores.py
,,y,.,./ci/pytest.sh pytest cases/34-CpuAffinity/test_cpu_read_write_ratio.py
,,y,.,./ci/pytest.sh pytest cases/34-CpuAffinity/test_cpu_show_allocation.py
,,y,.,./ci/pytest.sh pytest cases/34-CpuAffinity/test_cpu_affinity_edge_cases.py
```
