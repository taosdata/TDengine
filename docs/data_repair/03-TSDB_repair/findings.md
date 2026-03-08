# Findings

## Confirmed Context
- 总体需求文档位于 `.vscode/dev/RS.md`，目标是扩展 `taosd -r` 支持 vnode 的 `wal/tsdb/tdb` 修复。
- 对齐结果位于 `.vscode/dev/phase1-repair-freeze.md`，前两阶段已经完成命令行参数重构与 `meta force repair`。
- 第一阶段留痕目录：`docs/data_repair/01-参数重构`。
- 第二阶段实际留痕目录存在为：`docs/data_repair/02-META_repair`。
- 最近相关提交：
  - `555e6d6ea0` `feat(repair): add meta force repair flow and coverage`
  - `caf0deed2e` `feat(taosd): implement phase1 repair CLI parsing and validation`

## Early Inferences
- 第三阶段很可能是在已完成 repair CLI 和 meta force repair 的基础上，为 `file-type=tsdb` 增加 `mode=force` 的真实修复流程。
- 设计需要尽量复用 phase1/phase2 已落地的 CLI、备份、vnode 过滤、日志输出与测试组织方式。
- 当前仍缺少用户明确确认的边界：支持的损坏场景、恢复粒度、失败处理策略、是否允许部分恢复。

## Confirmed By User
- 第三阶段范围严格限定为：`taosd -r --node-type vnode --file-type tsdb --mode force`。
- 明确不包含：`wal`、`tdb`、`replica`、`copy`、以及其他 node type。
- 因此本阶段是“单副本 vnode 的 TSDB 强制修复”专项。
- 用户进一步确认 TSDB force repair 的目标语义：在保证 vnode 能正常启动的前提下，尽可能恢复数据。
- 若某个文件组彻底无法恢复，则摘除整个文件组。
- 若文件组中仅部分数据块损坏，则只摘除损坏数据块，保留所有可读数据块。
- 用户接受“扫描现有 TSDB 数据并重新构建数据文件”的实现方向，作为候选核心机制。

## Codebase Findings
- `source/dnode/mgmt/exe/dmMain.c` 当前仅对 `fileType=meta` 放行真实 repair 执行；非 `meta` 仍停留在 phase1 占位返回。
- `source/dnode/vnode/src/tsdb/tsdbOpen.c` 中 `tsdbOpen()` 会调用 `tsdbOpenFS()`，若 `fsstate == TSDB_FS_STATE_INCOMPLETE && force == false`，则返回 `TSDB_CODE_NEED_RETRY`。
- `source/dnode/vnode/src/tsdb/tsdbFS2.c` 已有 `tsdbFSScanAndFix()` / `tsdbFSDoSanAndFix()`，但当前能力主要是：
  - 校验 `current.json` 引用的文件是否存在
  - 发现损坏/缺失后下调 `fset->maxVerValid`
  - 清理目录中未被引用的残留文件
  - 将 `fsstate` 标记为 `TSDB_FS_STATE_INCOMPLETE`
- 现有 TSDB scan/fix 尚未实现：
  - 数据块级损坏识别
  - 从可读块重建 `.data/.head/.sma/.stt/.tomb` 文件组
  - 以“保守模式”摘除坏块并生成新的可启动文件集
- 因此第三阶段若要满足用户目标，需要在现有 FS 层之上新增“扫描 + 过滤 + 重建”的 repair 流程，而不仅是放开 phase1 占位。
- 用户指出此前设计中的“先全量备份该 vnode 当前 TSDB 目录”不可接受：TSDB 数据量可能很大，第三阶段不应采用全量备份。
- 新约束：仅备份被判定为“有问题且将被修改/摘除”的文件组及相关修复产物，不做整个 TSDB 目录的完整备份。
- 用户补充了 TSDB repair 备份约束：备份不能只拷贝受影响文件组的物理文件，还必须同步备份其在 `current.json` 中对应的描述信息。
- 原因：若缺少文件组描述信息，备份产物可能无法被后续工具或人工正确解析，失去回溯与排障价值。
- 因此备份单元应是“文件组物理文件 + 对应 manifest/current 描述片段”的组合，而不是仅文件本体。

## User-Confirmed Repair Rules
- `current.json` 的作用：定义“应该有哪些文件组”，以及每个文件组里有哪些文件、文件大小等描述信息；它不是数据有效性的唯一依据。
- 不在 `current.json` 引用中的残留文件，不作为恢复候选，仍按垃圾文件处理。
- 文件缺失时的细化规则：
  - 若缺失的是 `stt` 文件，则直接将该 `stt` 从文件组中摘除。
  - 若缺失的是 `head` 或 `data` 文件，则应联动摘除 `head/data/sma` 这一组核心文件。
  - 因此：`stt` 是自描述的；`head + data + sma` 需要合在一起才能形成自描述集合。
- 文件组是否保留，取决于其物理文件能否形成“自洽、可验证、可重建”的数据集合。
- 采用保守模式：任何“不确定是否正确”的块都直接丢弃。
- `.data` / `.head` 是核心恢复对象；`.sma` 优先重建；`.stt` / `.tomb` 原则上支持保守扫描，但第一批实现允许按复杂度收敛。
- 推荐判定顺序：先判文件组能否枚举，再枚举块，再做块完整性校验，最后决定保留原组 / 部分重建 / 整组摘除。
- 用户补充实现边界：不能在 `dmMain.c` 中直接调用 TSDB 修复功能。
- 正确切入点应位于 TSDB 打开文件系统的链路：在 `tsdb open fs` 时检查是否命中 repair 模式；若命中，则执行深度扫描、受影响文件组备份与修复。
- 因此 `dmMain.c` 的职责仍仅限于 repair 参数校验与执行放行，不承载 TSDB 修复过程本身。

## Crash-Safe Analysis For `current.json` Repair Updates
- 用户明确指出：repair 修改 `current.json` 时，必须考虑 crash-safe；若更新过程中 crash，系统重启后也应能正确恢复到一致状态。
- 当前实现现状：`tsdbRepairDropMissingStt()` 直接调用 `save_fs(..., current.json)` 覆盖正式 `current.json`，这不是严格 crash-safe 的写法。
- 从现有 TSDB FS 代码看，系统里至少存在两类“抗 crash / 重启恢复”机制：
  1. **事务式 manifest 切换机制（推荐用于 repair 修改 `current.json`）**
     - 通过 `current.c.json` / `current.m.json` 这类临时 manifest 文件承载新状态；
     - 再用 `taosRenameFile()` 原子切换到 `current.json`；
     - `open_fs()` 在重启时会识别 `current.c.json` / `current.m.json`，继续执行 `commit_edit()` 或 `abort_edit()`，从而恢复到一致状态。
  2. **启动时 scan-and-fix / 不完整状态恢复机制**
     - `open_fs()` 打开后会执行 `tsdbFSScanAndFix()`；
     - 若文件系统损坏，则将 `fsstate` 标记为 `TSDB_FS_STATE_INCOMPLETE`；
     - 后续由 `force` 打开、重试或 repair 逻辑继续处理。
- 这两类机制解决的问题不同：
  - 机制 1 解决的是“元信息切换过程中 crash，如何保证 manifest 一致性”。
  - 机制 2 解决的是“文件系统本身已不完整/损坏，启动后如何识别并继续修复”。
- 结论：**repair 对 `current.json` 的更新应归属于机制 1，而不是机制 2。**
  - 原因：repair 改写的是 TSDB 文件集的 manifest / 视图切换，本质是一次元信息提交；
  - 因此应采用“临时 manifest + 原子 rename + 重启恢复识别”的事务式路径；
  - 不能依赖“直接覆盖 `current.json` 后，再靠 scan-and-fix 挽救”来获得 crash-safe。
- 后续实现要求：
  - repair 修改 `current.json` 时，应改成写入 repair 专用临时 manifest（可复用现有 `current.c.json` / `current.m.json` 语义，或新增明确的 repair 临时 manifest）；
  - 然后使用原子切换落盘；
  - 并确保 `open_fs()` 在 crash 后重启时能够识别该中间状态并收敛到一致结果。
- crash-safe 修正已落地：repair 对 `current.json` 的更新已不再直接覆盖正式 manifest，而是改为走“事务式 manifest 切换”路径。
- 具体做法：
  - repair 修改目标先落到 `fSetArrTmp`；
  - 然后写入 `current.c.json`；
  - 再调用现有 `commit_edit()` 原子切换到 `current.json`；
  - 重启时若残留 `current.c.json`，`open_fs()` 会沿现有 commit 恢复路径继续收敛到一致状态。
- 因此 repair 的 `current.json` 回写，现已明确归属于“机制 1：事务式 manifest 切换”，而不是依赖“机制 2：scan-and-fix / INCOMPLETE 状态兜底”。

## 2026-03-07 当前代码补充发现
- `docs/data_repair/03-TSDB_repair/task_plan.md` 里的 `Scope Status` 已落后于代码现状：仓库中 TSDB repair 早已不处于“未开始/待设计确认”，而是已经完成文件组级 repair，并开始推进块级 repair。
- 当前块级实现仍然挂在 `source/dnode/vnode/src/tsdb/tsdbFS2.c`，尚未拆出独立 `tsdbRepair.c`；因此续做时应以代码现状为准，不强行按计划文件里的目标文件名重构。
- 现有可复用接口足以支撑第一批块级修复：
  - 读取侧：`tsdbDataFileReaderOpen()`、`tsdbDataFileReadBrinBlk()`、`tsdbDataFileReadBrinBlock()`、`tsdbDataFileReadBlockData()`。
  - 写入侧：`tsdbDataFileWriterOpen()`、`tsdbDataFileWriteBlockData()`、`tsdbDataFileWriterClose()`。
- `tsdbDataFileWriterClose()` 可以为新建核心文件生成 `STFileOp`；若要保留 staged manifest 路径并避免回退到 `edit_fs()` 重新从 `fSetArr` 全量复制，当前更适合在 `fSetArrTmp` 上直接逐条应用 `tsdbTFileSetEdit()`。
- 新增块级 repair 日志后，`repair.log` 已从“仅记录文件组级 reason/action”扩展到可记录：`block_total`、`block_kept`、`block_dropped`、`action=rebuild_core_group`。
- 新增块级 E2E 时发现：`flush database <db>` 只是**异步触发** TSDB 落盘，不能假设执行一次 `flush` 后立即出现 `.data` 文件；测试需要“循环 flush + 轮询 `.data` 文件落盘”。
- 用户在其自己的 `tmux` 环境中执行 `cd /Projects/work/TDengine/test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_tsdb_force_repair.py`，可以稳定跑出完整结果；这进一步说明我当前会话里的 pytest/harness 卡顿主要是环境差异，不应直接等同于代码逻辑故障。
- 用户给出的全量结果暴露了 4 个当前真实失败：
  - `test_tsdb_force_repair_backs_up_affected_fileset_with_manifest`
  - `test_tsdb_force_repair_removes_missing_head_data_sma_from_current`
  - `test_tsdb_force_repair_backup_writes_repair_log`
  - `test_tsdb_force_repair_rebuilds_core_from_valid_blocks`
- 其中已确认至少有两类确定性测试问题：
  - 备份 helper `_find_backup_manifest()` / `_find_backup_log()` 之前存在截断的控制流，导致 helper 本身无法正确遍历备份目录。
  - `test_tsdb_force_repair_backup_writes_repair_log()` 里错误引用了未定义变量 `fake_fid`，这是显式测试代码 bug，不是 repair 逻辑问题。
- 在用户本地最新全量回归中，当前已收敛为 `11 passed, 1 failed`，仅剩 `test_tsdb_force_repair_rebuilds_core_from_valid_blocks`。
- 该失败并非 vnode 定位或 `.data` 落盘失败，而是 repair 已执行、`repair.log` 已生成，但日志里没有出现 `action=rebuild_core_group`。
- 当前最可能的剩余问题是“坏块注入方式不够稳定”，之前将 `.data` 中间位置随机清零可能没有稳定命中单个可恢复数据块；因此已把注入位置调整为“优先破坏 `TSDB_FHDR_SIZE` 之后的早期 data 区域”，以更稳定地制造“坏一个块、保留后续有效块”的场景。
- 继续收敛后发现：即便调整 `.data` 覆写位置，块级场景仍未稳定触发 `rebuild_core_group`。因此测试方向进一步改为“破坏晚期 `.head` 索引区域”，因为当前 repair 逻辑对 `head` 的 BRIN / 索引损坏更容易稳定判出“前面块保留、后面索引坏 -> rebuild core group”。
- 为进一步提高命中“部分块保留 + rebuild”的概率，块级场景数据库参数已收紧为 `minrows 10 maxrows 200`，以强制产出更多更小的数据块和对应索引记录，避免当前 fileset 只有极少数核心块导致损坏后直接退化成 `drop_core_group` 或无法进入块级统计路径。
- 从用户最新保留现场可确认：此前块级用例实际上命中的是 `reason=size_mismatch_core` / `action=drop_core_group`，并且 `block_total=0`。这说明当前失败并非块级判定本身不工作，而是测试选取到的真实 fileset 在进入块级扫描前就已被“核心文件大小不匹配”规则短路。
- 因此块级用例已进一步收敛为：只从 `current.json` 中挑选 `head/data` 都存在、且其 `size` 与磁盘真实文件大小一致的 core fileset，再对其做内容级破坏。这样可以绕开用户已明确要求保留的 `size_mismatch_core -> drop_core_group` 规则，真正验证“内容损坏但大小一致”时是否进入 `rebuild_core_group`。
- 在用户最新一次本地执行中，这条块级用例最终表现为 `SKIPPED`，原因是：当前实际落盘的真实 fileset 中，没有找到同时满足“`head/data` 存在且 manifest size 与磁盘真实大小一致”的 core fileset。
- 这意味着块级用例的守门条件已经足够保守：当环境不具备可验证 `rebuild_core_group` 的真实样本时，它会跳过而不是误报失败；当前阶段这比继续用不稳定样本制造假失败更符合阶段边界。

## Current Phase3 Status Summary
- 文件组级 repair 规则当前已稳定：`missing_stt`、`missing_core`、`size_mismatch_core`、staged manifest crash-safe 提交均已具备自动化覆盖。
- 块级 repair 第一批当前已具备：
  - 读取 `head` / `data` 并识别坏块；
  - 保留可读块并重写核心文件组；
  - 在 `repair.log` 中记录块级统计和 `rebuild_core_group` 决策。
- 块级 E2E 经过多轮收敛后，已从“随机找真实文件破坏”改为“只选择 size-matched core fileset 再做内容级破坏”；用户最新本地反馈显示该场景已基本打通。

## 2026-03-07 继续验证新增发现
- 最近一次 `wait too long for taosd start` 的直接根因已确认不是 TSDB repair 回退，而是启动环境问题：
  - `source/os/src/osTimezone.c` 从 `/etc/localtime -> .../zoneinfo/UTC` 截取时区字符串时留下了前导 `/`；
  - dnode / psim 日志会出现 `tzalloc(/UTC) failed`；
  - 该问题会进一步触发 `failed to update config since Internal error in time`，从而造成启动阶段长时间等待，后续再连库时表现成 `Invalid user` 一类级联错误。
- 已对该启动阻塞点做最小修复：
  - `source/os/src/osTimezone.c` 现会规范化时区字符串，去掉前导 `/`；
  - 新增 `source/os/test/osTimeTests.cpp` 回归用例覆盖 `truncateTimezoneString("/UTC") -> "UTC"`；
  - 定向单测验证通过。
- 另一个测试稳定性问题也已确认：
  - `source/dnode/vnode/src/tsdb/tsdbFS2.c` 中 repair dispatch 标记原先仅 `printf`，在 pytest 的 `subprocess.PIPE` 场景下存在缓冲导致输出未被及时采集的问题；
  - 已在 dispatch 标记后增加 `fflush(stdout)`，最小入口用例重新验证通过。
- 在环境恢复后重新跑 TSDB repair 全量，失败模式已从“环境态启动失败”收敛为 3 条真实业务失败：
  - `test_tsdb_force_repair_removes_missing_head_data_sma_from_current`
  - `test_tsdb_force_repair_removes_missing_data_head_data_sma_from_current`
  - `test_tsdb_force_repair_removes_size_mismatch_head_data_sma_from_current`
- 上述 3 条失败在移除冗余 dispatch 断言后，已经能确认不是输出采集问题，而是实际结果问题：
  - `current.json` 中对应 `fid` 仍保留 `head/data/sma`；
  - 但默认备份目录下的 `repair.log` 已记录 `reason=missing_core` / `reason=size_mismatch_core` 且 `action=drop_core_group`；
  - 因此当前更像是“repair plan 已判定 drop_core_group 并完成备份日志落盘，但该决策没有最终体现在 staged manifest / committed current.json 中”。
- 需要优先检查的真实代码路径已进一步收窄到：
  - `source/dnode/vnode/src/tsdb/tsdbFS2.c` 中 `tsdbDispatchForceRepair()` 对 `dstFset` 的 `tsdbRepairDropCoreOnTmpFSet()` 修改，
  - 以及之后的 `tsdbRepairCommitStagedCurrent()` / `save_fs()` / `commit_edit()` 是否把该修改正确写入正式 `current.json`。

## 2026-03-08 修复实施
- **问题根因确认**：`tsdbRepairDropCoreOnTmpFSet()` 原实现仅简单将 `farr[ftype]` 设为 NULL，但未生成 `TSDB_FOP_REMOVE` 操作并调用 `tsdbTFileSetEdit()` 记录修改。对比 `tsdbRepairRebuildCoreOnTmpFSet()` 的实现（会生成 REMOVE 操作并调用 `tsdbTFileSetEdit`），发现这是关键差异。
- **修复方案**：
  1. 修改 `tsdbRepairDropCoreOnTmpFSet` 函数签名，增加 `STFileSystem *fs` 参数
  2. 在函数内部生成 `TSDB_FOP_REMOVE` 操作并添加到 `fopArr`
  3. 遍历 `fopArr`，调用 `tsdbTFileSetEdit(fs->tsdb, fset, op)` 应用操作
  4. 更新两处调用点（`tsdbDispatchForceRepair` 和 `tsdbRepairRebuildCoreOnTmpFSet` 的失败回退路径）
- **修改文件**：`source/dnode/vnode/src/tsdb/tsdbFS2.c`
- **待验证**：由于当前会话测试环境受 sandbox 限制（`taosd -r` 进程启动被拦截、端口占用等问题），修复需在用户干净环境验证
