# 数据修复功能 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-03-12 | 2026-3-13 | v3.4.1.0 | @程洪泽 | 测试报告初稿 |

## 2. 测试目标

- 验证 `taosd -r` 本地修复模式的命令行 grammar、帮助信息、合法输入和非法输入报错契约。
- 验证 `meta` repair target 能按新 grammar 触发，并能在真实 vnode 上产生备份目录。
- 验证 `tsdb` repair target 在健康 fileset、显式调度和 block 重建路径下具备可观测行为。
- 验证拆分后的 TSDB force repair 测试文件能够独立进入完整 CI 流程，并在串行执行下稳定运行。

## 3. 参考文档

- PR: https://github.com/taosdata/TDengine/pull/34753
- `docs/zh/14-reference/01-components/01-taosd.md`
- `docs/zh/08-operation/05-maintenance.md`
- `test/cases/80-Components/01-Taosd/test_com_cmdline.py`
- `test/cases/80-Components/01-Taosd/test_meta_force_repair.py`
- `test/cases/80-Components/01-Taosd/test_tsdb_force_repair_metadata.py`
- `test/cases/80-Components/01-Taosd/test_tsdb_force_repair_core_e2e.py`
- `test/cases/80-Components/01-Taosd/test_tsdb_force_repair_stt_e2e.py`
- `test/cases/80-Components/01-Taosd/test_wal_force_repair_e2e.py`
- `test/ci/cases.task`
- `source/common/test/commonTests.cpp`
- `source/os/test/osTimeTests.cpp`
- `source/dnode/vnode/test/tsdbRepairTest.cpp`

## 4. 测试结论

**关键结论如下**：
- repair CLI 的对外 grammar 已明确，帮助信息、合法 target、非法 grammar、legacy 参数移除都有新增自动化覆盖。
- meta force repair 已覆盖参数接受性、真实 vnode 备份目录、默认 `/tmp` 备份根目录，以及备份目录已存在时的异常分支。
- TSDB/WAL repair 用例已从单文件拆分为 `metadata`、`core_e2e`、`stt_e2e` 三个 TSDB 主文件，以及一个独立 `wal_e2e` 文件；`meta force repair` 也已补齐边界场景并接入 `test/ci/cases.task`。
- 拆分后 repair 主路径共 `23` 个 test case，当前串行实测结果为：`meta_force_repair 4 passed`、`metadata 5 passed, 2 skipped`、`core_e2e 8 passed`、`stt_e2e 3 passed`、`wal_e2e 1 passed`。
- 原 `known_limits` 中的真实 `xfail` 已逐条复核并处理：能转正的已迁入主路径；仅剩历史 synthetic 场景被删除，不再占用 CI 路径。
**关键数据如下**：
- repair CLI：`5` 个 test method，覆盖 help、合法 grammar、非法 grammar、legacy 参数移除等契约。
- meta force repair：`4` 个 test method。
  - 参数接受性：`1`
  - 真实 vnode 备份目录：`1`
  - 备份目录已存在异常：`1`
  - 默认 `/tmp` 备份根目录：`1`
- TSDB force repair：`18` 个主路径 test method。
  - `metadata`: `7`
  - `core_e2e`: `8`
  - `stt_e2e`: `3`
- WAL force repair：`1` 个主路径 test method，覆盖真实 WAL 损坏目录 rename/recreate 路径。
- TSDB 历史 synthetic `skip` 场景：`5` 个，已从主路径移除并最终删除 `known_limits` 文件。
- 辅助单元测试：`commonTests`、`osTimeTests` 各新增 1 个；另有 `tsdbRepairTest.cpp` 白盒用例源码

## 5. 测试环境

- OS: Linux 优先
- Browser: N/A
- 数据节点形态: 单 dnode、本地文件系统、可停止并重新启动 `taosd`
- 前置条件
  - 可执行 `taosd`
  - 可访问 vnode 数据目录
  - 可读写临时备份目录，如 `/tmp`
  - Python 新测试框架环境可用
  - 运行方式需在 `test/` 目录下使用 `./ci/pytest.sh pytest cases/...`

## 6. 功能测试

### 6.1 repair CLI 契约

#### 6.1.1 测试要点

- `taosd -r --help` 是否输出 repair 专用帮助。
- `meta`、`tsdb`、`wal` 三类 target 是否接受合法 grammar。
- target grammar 是否正确拒绝未知 file type、重复 key、非法 strategy、缺失 `fileid`、重复 target，以及 `fileid=*` 与显式 `fileid=<fid>` 的冲突。
- `-r`、`--mode force`、`--node-type vnode`、`--repair-target` 是否形成必填组合。
- legacy 参数 `--file-type`、`--vnode-id` 是否已经失效。

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | repair help contract | 校验 `taosd -r --help` 输出 `Usage: taosd -r --mode force --node-type vnode` | 通过 ✅ |
| 2 | valid target grammar | 校验 `wal`、`meta`、`tsdb` 的合法 target、`fileid=*` 以及多 target 组合 | 通过 ✅ |
| 3 | invalid target syntax | 校验未知 file type、重复 key、非法 strategy、缺失 `fileid`、重复 target、wildcard/显式冲突错误信息 | 通过 ✅ |
| 4 | invalid mode and legacy args | 校验缺少 `-r`/`--mode`/`--node-type`/`--repair-target` 以及 legacy 参数被拒绝 | 通过 ✅ |

### 6.2 meta force repair

#### 6.2.1 测试要点

- `meta:vnode=<id>` 新 grammar 是否可用。
- `strategy=from_redo` 是否被接受。
- `--backup-path` 是否接受尾斜杠写法。
- 对真实 vnode 运行 repair 时，是否在外部备份目录生成 `taos_backup_YYYYMMDD/vnodeX/meta/`。

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | meta target syntax | 校验默认策略、显式 `from_redo`、自定义 `--backup-path` 的参数接受性 | 串行实测通过 ✅ |
| 2 | meta backup on real vnode | 创建真实 vnode，停服后执行 meta repair，校验备份目录存在且非空 | 串行实测通过 ✅ |
| 3 | existing backup dir rejected | 预先创建目标备份目录，校验 meta repair 返回非零并提示目录已存在 | 串行实测通过 ✅ |
| 4 | default tmp backup root | 不指定 `--backup-path`，校验备份落在默认 `/tmp/taos_backup_YYYYMMDD/vnodeX/meta/` | 串行实测通过 ✅ |

### 6.3 TSDB force repair 主覆盖

#### 6.3.1 测试要点

- 拆分后的四个修复测试文件是否能独立执行并进入完整 CI 流程。
- 真实 fileset / 真实 `.stt` / 真实 block 的测试夹具是否可构造。
- 健康 fileset 在 repair 后是否保持可读可写。
- repair 是否在 `tsdb open fs` 路径中被真实调度。
- 对真实 missing-head、missing-stt、block 破坏、head size mismatch，以及 `fileid=*` 覆盖多个 fileset 的场景，修复后数据库是否仍可读可写。

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | metadata file split | `test_tsdb_force_repair_metadata.py` 独立覆盖 fixture、noop、dispatch、execution path | 串行实测：`5 passed` ✅ |
| 2 | core e2e split | `test_tsdb_force_repair_core_e2e.py` 独立覆盖 missing-head、multi-fileset old-head、block rebuild、head-only rebuild、drop-invalid-only、多 target、wildcard target、full rebuild | 串行实测：`8 passed`✅ |
| 3 | stt e2e split | `test_tsdb_force_repair_stt_e2e.py` 独立覆盖 missing-stt、backup-root、current.json 更新 | 串行实测：`3 passed`✅ |
| 4 | wal e2e split | `test_wal_force_repair_e2e.py` 独立覆盖真实 WAL 损坏目录 rename/recreate | 串行实测：`1 passed`✅ |
| 5 | core fixture builder | 构造真实 core fileset，校验 vnode、fid、head/data 文件存在 | 已在 `metadata` 文件串行实测通过✅ |
| 6 | multi-fileset fixture builder | 构造一个 vnode 下多个真实 fileset | 已在 `metadata` 文件串行实测通过✅ |
| 7 | stt fixture builder | 构造真实 `.stt` 文件和 entry 计数 | 已在 `metadata` 文件串行实测通过✅ |
| 8 | size-mismatch injector | 校验文件大小破坏辅助函数确实改变文件尺寸 | 已在 `metadata` 文件串行实测通过✅ |
| 9 | healthy fileset noop | 对健康 fileset 执行 repair，校验行数不变且表仍可写 | 已在 `metadata` 文件串行实测通过✅ |
| 10 | missing head remains writable | 删除真实 `.head` 后，校验修复后数据库仍可读写 | 已转正并在 `core_e2e` 串行实测通过✅ |
| 11 | missing old head in multi-fileset | 删除旧 fileset 的 `.head`，校验其余数据仍可写 | 已转正并在 `core_e2e` 串行实测通过✅ |
| 12 | rebuild core from valid blocks | 破坏真实 `.head` 中后段字节，校验 repair 后重启仍可写 | 已在 `core_e2e` 串行实测通过✅ |
| 13 | head-only rebuild recovers real head damage | 破坏真实 `.head`，使用 `head_only_rebuild`，校验 `.data` 保留且修后可写 | 已在 `core_e2e` 串行实测通过✅ |
| 14 | drop-invalid-only does not fix head size mismatch | 扩展 `.head` 制造 size mismatch，使用默认策略，校验 deep repair 未被触发 | 已在 `core_e2e` 串行实测通过✅ |
| 15 | multi-target repairs two filesets | 一次 repair 同时声明两个 `tsdb` target，校验修后数据库仍可写 | 已在 `core_e2e` 串行实测通过✅ |
| 16 | wildcard target repairs all filesets in vnode | 一次 repair 使用 `fileid=*` 覆盖同一 vnode 下多个损坏 fileset，校验修后数据库仍可写 | 已在 `core_e2e` 串行实测通过✅ |
| 17 | full rebuild recovers real head size mismatch | 扩展 `.head` 形成可恢复 size mismatch，校验 `full_rebuild` 后仍可写 | 已转正并在 `core_e2e` 串行实测通过✅ |
| 18 | missing stt remains writable | 删除真实 `.stt`，校验修复后仍可写 | 已转正并在 `stt_e2e` 串行实测通过✅ |
| 19 | missing stt with backup root remains writable | 删除真实 `.stt` 且指定 backup root，校验修复后仍可写 | 已转正并在 `stt_e2e` 串行实测通过✅ |
| 20 | removes missing stt from current.json | 删除真实 `.stt` 后，校验 `current.json` 引用被清理 | 已转正并在 `stt_e2e` 串行实测通过✅ |

### 6.4 TSDB 覆盖演进与限制

#### 6.4.1 测试要点

- 原单文件 `test_tsdb_force_repair.py` 是否已彻底拆分，不再形成单文件长耗时瓶颈。
- 原 `known_limits` 中哪些 case 已经转正，哪些只是历史 synthetic 覆盖遗留。
- 当前还存在哪些未覆盖但值得后续补测的风险面。

#### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | monolithic tsdb repair file | 原单文件 `test_tsdb_force_repair.py` | 已拆分并删除 |
| 2 | known limits file | 历史 `xfail/skip` 收敛文件 | 已清理并删除 |
| 3 | synthetic core cleanup cases | 历史 synthetic `skip` 场景 | 已删除，因已被真实 fileset 覆盖替代 |
| 4 | WAL e2e repair | 真实 WAL 损坏修复回归 | 已补齐，并接入完整 CI 流程 |
| 5 | tsdbRepairTest.cpp | 白盒 TSDB repair 单元测试 | 代码存在，但未接入默认 CMake 构建 |

### 6.5 WAL repair

#### 6.5.1 测试要点

- `wal:vnode=<id>` target grammar 是否可接受。
- `wal` target 是否正确拒绝 `strategy`。
- 代码路径是否在 repair 模式下把 WAL corruption 处理切到显式 repair 触发。

#### 6.5.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | wal target parse | 通过 CLI 用例校验 `wal:vnode=<id>` 接口 | 已有自动化覆盖，当前会话未单独重跑 |
| 2 | wal strategy rejected | 通过 CLI 用例校验 `wal` 不支持 `strategy` | 已有自动化覆盖，当前会话未单独重跑 |
| 3 | wal e2e corruption repair | 真实 WAL 损坏修复回归，验证 `wal.corrupted.*` 目录生成和重建后可写 | 串行实测：`1 passed` |

## 7. 易用性测试（可选）

本 PR 为服务端 CLI/离线修复能力，不涉及 GUI。可从命令行易用性角度关注以下点：
- 新 grammar 是否比 legacy 参数更易理解。
- `--help` 是否能直接给出完整 usage。
- 错误信息是否足够指向用户应补齐或修正的参数。
当前从代码和测试用例看，help 文案和典型错误信息已被显式固化。

## 8. 长期稳定性测试（可选）

本 PR 未新增 soak test 或长期稳定性回归。
建议后续补充：
- 重复执行 repair 后的幂等性验证。
- repair 后多次正常重启的稳定性验证。
- 多 vnode、多 fileset 批量 repair 的长时 IO 行为验证。

## 9. 性能测试

本 PR 无独立性能测试用例。
评估建议：
- `drop_invalid_only` 作为轻量模式，可记录启动耗时基线。
- `head_only_rebuild`、`full_rebuild` 应重点关注大 fileset 场景下的启动耗时和磁盘放大。
- 当前版本不建议仅凭本 PR 将 deep repair 宣传为“高性能恢复能力”。

## 10. 安全测试

无。

## 11. 兼容性测试

测试用例包括但不局限于：
- 正常非 repair 启动路径不应被新接口影响。
- TSDB/WAL repair 用例拆分后，四个主文件应可分别进入完整 CI 流程。
- repair 后正常启动能否继续读写，是当前 TSDB 真实场景的关键兼容性门槛。
当前兼容性判断：
- CLI 兼容性变化已明确并有负向用例覆盖。
- 拆分后的 `metadata/core_e2e/stt_e2e/wal_e2e` 四个主文件均已完成串行实测，未发现主路径失败。
- 兼容性剩余风险主要集中在 `metadata` 的环境相关 `skip` 和未接入构建的白盒测试，而非主路径 repair case 失败。
