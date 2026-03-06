# Findings: META 强制恢复（Phase2）

## Confirmed Scope
- 仅实现 `taosd -r --mode force --file-type meta ...` 的实际执行链路。
- 不包含 `wal/tsdb` 的联动恢复，也不做统一 repair 编排。
- 实现应视为 `metaGenerateNewMeta()` 调用链的增强版，而非全新独立修复框架。

## Existing Implementation Facts
- `source/dnode/mgmt/exe/dmMain.c` 已完成 repair 参数层解析；`--mode force --file-type meta` 当前可进入 `global.runRepairFlow` 分流。
- 旧 `taosd -r` 行为仍通过全局变量 `generateNewMeta` 触发，在 `source/dnode/vnode/src/meta/metaOpen.c` 中调用 `metaGenerateNewMeta()`。
- `metaGenerateNewMeta()` 当前流程：
  - 基于已打开的旧 meta 构建 `meta_tmp`
  - 扫描 uid 索引与 table.db，重放为新 meta
  - commit/finish 后关闭 `pNewMeta`
  - 将旧 `meta` 重命名为 `meta_bak`
  - 将 `meta_tmp` 重命名为 `meta`
  - 重新打开新 `meta`
- `metaOpen()` 已有一定的目录状态收敛逻辑：根据 `meta` / `meta_tmp` / `meta_bak` 三者是否存在做清理、回切或报错。

## User-Stated Constraints
- 需要考虑恢复过程中宕机的场景。
- 需要考虑 vnode 是否处在“恢复列表”中。
- 需要考虑备份路径。

## Open Design Topics
- repair 模式下是否沿用 `meta_tmp/meta_bak` 目录协议，还是引入带 repair 标识的独立 staging/manifest。
- “恢复列表”需要放在进程内、配置输入，还是 vnode 目录下的持久化状态文件。
- `--backup-path` 与当前本地 `meta_bak` 的职责边界需要明确。

## Newly Confirmed Decisions
- “恢复列表”不需要额外持久化状态；仅由本次命令的 `--vnode-id` 决定。
- 未指定 `--vnode-id` 时，对当前节点下全部 vnode 执行 meta force repair。
- `--backup-path` 如未指定，默认使用 `/tmp`；实际备份根目录格式为 `<backup_path>/taos_backup_YYYYMMDD`。
- 不接受“参数解析后集中串行修复”的方案；修复判断必须内嵌到现有 vnode 并行 open 流程中。
- `dmMain.c` 不需要引入复杂 repair context 传递机制；本次可不考虑独立 repair 上下文问题。
- 测试策略已调整：不把 META force repair 测试继续杂糅到 `test/cases/80-Components/01-Taosd/test_com_cmdline.py`，应新开独立测试文件承载该需求测试。

## Implementation Notes (Session B)
- `dmValidateRepairOption()` 已放开 `meta force repair` 缺省 `--vnode-id` 的场景，语义为“当前 dnode 全量 vnode”。
- `dmMain.c` 不再让 `meta force repair` 走 phase1 占位返回；仅 `wal/tsdb` 等尚未实现类型继续保留原占位提示。
- repair 最小状态通过全局变量下沉到 `metaOpen.c`，避免让 `libvnode` 反向依赖 `taosd` 私有函数。
- `metaOpen()` 已增加按 vnode 判断是否命中本次 `meta force repair` 的分流。
- `metaGenerateNewMeta()` 在本次 force repair 场景下，会先把当前 vnode 的 `meta` 目录递归备份到外部目录，再继续本地 `meta/meta_tmp/meta_bak` 切换流程。
- 外部备份目录格式为 `<backup-root>/taos_backup_YYYYMMDD/vnode<vgId>/meta`，并处理了根目录末尾自带 `/` 的情况，避免生成双斜杠路径。
- 新增独立测试文件 `test/cases/80-Components/01-Taosd/test_meta_force_repair.py`，未继续混入旧 `test_com_cmdline.py`。

## Verification Notes (Session B)
- `python3 -m py_compile test/cases/80-Components/01-Taosd/test_meta_force_repair.py`：通过。
- 手工红阶段验证：`debug/build/bin/taosd -r --node-type vnode --file-type meta --mode force -V` 在改动前返回 `missing '--vnode-id' in repair mode`。
- 手工绿阶段验证：
  - `debug/build/bin/taosd -r --node-type vnode --file-type meta --mode force -V`
  - `debug/build/bin/taosd -r --node-type vnode --file-type meta --mode force --backup-path /tmp/meta-force-repair -V`
  两条命令当前均返回版本信息，退出码为 `0`。
- 分流回归验证：`debug/build/bin/taosd -r --node-type vnode --file-type wal --vnode-id 1 --mode force` 仍返回 phase1 占位提示，说明仅 `meta force repair` 放行至启动链路。
- `pytest` 新文件尚未完成稳定验证；当前继续沿用手工命令验证作为主要证据。
- 进一步验证显示：在补充 `LD_PRELOAD=$(gcc -print-file-name=libasan.so)`、`ASAN_OPTIONS=detect_leaks=0` 与 `LSAN_OPTIONS=detect_leaks=0` 后，`pytest --collect-only` 已可稳定识别 `test_meta_force_repair.py` 中的 2 个用例。
- 但执行 `pytest cases/80-Components/01-Taosd/test_meta_force_repair.py -q` 仍在当前环境卡住，现阶段主要证据仍以手工命令验证为主。
- 已新增“真实 vnode open / 备份目录落盘”测试到 `test_meta_force_repair.py`：测试会创建真实数据库与表、定位实际 vnode 目录、停掉 dnode、以 `-c <cfgDir> -r ...` 启动 repair 进程，并检查 `<backup-root>/taos_backup_YYYYMMDD/vnode<id>/meta` 是否落盘。
- `pytest --collect-only` 已确认该新用例可被识别；实际执行 `pytest -k creates_backup_for_real_vnode` 在当前环境下 90 秒内未结束，被 `timeout` 终止，说明运行态卡顿问题仍在。


## Test Execution Memory
- 对 `test/cases/80-Components/01-Taosd/test_meta_force_repair.py` 的正确执行入口，不应直接使用 `pytest ...`，而应在 `test/` 目录下使用：`./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_meta_force_repair.py`。
- 原因：`./ci/pytest.sh` 会统一注入测试运行所需环境，包括 `BUILD_DIR`、`SIM_DIR`、`ASAN_DIR`，并自动以 `pytest ... -A` 方式启动；这些前置条件直接影响该类用例是否能进入正确的测试环境。
- 后续凡是验证该文件或同类 pytest E2E 用例，默认优先使用 `./ci/pytest.sh pytest <case>` 入口，除非是在做非常明确的底层调试（例如 `--collect-only`、语法检查）。
- 若直接运行 `pytest cases/80-Components/01-Taosd/test_meta_force_repair.py` 出现卡顿、环境不一致或 ASAN/fixture 行为异常，优先视为“入口错误”而非测试本身失败。
- 已按用户确认的正确入口重复验证：`cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_meta_force_repair.py`。
- 在当前 Codex CLI 沙箱环境中，该脚本可成功启动 pytest，并自动转换为 `pytest ... -A` 执行；这进一步证明先前直接调用裸 `pytest` 的验证路径并不可靠。
- 但在当前环境中，`./ci/pytest.sh` 启动后的测试执行仍长时间无后续输出，`sim/asan/psim.info` 也未产生足够线索，说明阻塞仍发生在测试运行早期阶段。
- 同样按正确入口执行 `./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_com_cmdline.py -k repair_cmdline_phase1`，在当前环境中也表现为启动后长时间无结果；因此当前问题更像是该沙箱环境与本地可成功环境之间的运行差异，而不只是 `test_meta_force_repair.py` 单例问题。
- 二进制/库来源审计确认：当前环境中同时存在“仓库构建版”和“安装版” TDengine。
- 仓库构建版路径：`/Projects/work/TDengine/debug/build/bin/taosd`、`/Projects/work/TDengine/debug/build/bin/taos`。
- 安装版路径：`/usr/local/taos/bin/taosd`、`/usr/local/taos/bin/taos`，且 `/usr/bin/taosd` / `/usr/bin/taos` 通过符号链接指向该安装版。
- `test/ci/pytest.sh` 解析出的 `taosd` 来自仓库构建目录（当前为 `./debug/build/bin/taosd`）。
- 但 `test/new_test_framework/utils/before_test.py` 的 `get_taos_bin_path()` 在未显式设置 `TAOS_BIN_PATH` 时，会 fallback 到系统安装路径，因此测试框架存在拿到安装版二进制的风险。
- Python `taos` 驱动的 `taos.cinterface.py` 使用 `ctypes.CDLL("libtaos.so")` 加载客户端库；在当前环境中，`/usr/lib/libtaos.so -> /usr/local/taos/driver/libtaos.so.3.4.0.9.alpha`，说明 Python 客户端默认落到安装版 `libtaos`。
- 因此当前高概率存在“服务端走构建版、客户端/动态库走安装版”的混用风险，这是后续验证环境需要优先统一的根因候选。
- 已尝试通过显式设置 `TAOS_BIN_PATH=/Projects/work/TDengine/debug/build/bin` 和 `LD_LIBRARY_PATH=/Projects/work/TDengine/debug/build/lib:$LD_LIBRARY_PATH` 将测试环境统一到仓库构建版，再使用标准入口执行 `./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_meta_force_repair.py -k creates_backup_for_real_vnode --setup-show`。
- 结果未出现实质性改善：测试仍停在 `SETUP S before_test_session` / `SETUP C add_common_methods` 之后，超时退出。
- 进一步查看运行中的 `pytest` 进程环境时，仅稳定观察到 `ASAN_OPTIONS`，未看到我们注入的 `TAOS_BIN_PATH` / `LD_LIBRARY_PATH` 出现在该 `pytest` 进程环境中；这说明“环境统一到构建版”的尝试尚未真正打通到测试运行进程，或被脚本/子进程链路吞掉。
- 当前可以确认：两套 `taosd/libtaos` 混用是高概率风险，但至少从这轮验证看，它不是唯一根因；还需要继续追踪 `./ci/pytest.sh` -> pytest -> fixture 链路中的环境传递问题。
- 已定位并修复当前环境下测试运行的根因之一：`test/new_test_framework/taostest/util/remote.py` 在 localhost 分支使用 `asyncio.to_thread(subprocess.run(...))`，在当前环境中会卡住；现改为 `asyncio.create_subprocess_shell(...)` 执行本地命令。
- 已定位并修复另一项关键根因：`./ci/pytest.sh` 注入的 `LD_PRELOAD` / ASAN 环境会污染 `Remote.cmd(localhost, ...)` 启动的普通系统命令（如 `cat`、`mkdir`），导致这些命令异常；现对 localhost 子进程显式清理 `LD_PRELOAD` / `ASAN_OPTIONS` / `LSAN_OPTIONS`。
- 已定位并修复第三项关键根因：`taostest` 在 localhost + ASAN 场景下启动 `taosd` 时，`LeakSanitizer` 会直接中止进程；现已在 `test/new_test_framework/taostest/components/taosd.py` 中为该启动路径补充 `detect_leaks=0` 与 `LSAN_OPTIONS=detect_leaks=0`。
- 已调整 localhost 启动策略：即使 `screen` 存在，localhost 场景也不再强制依赖 `screen` 拉起 ASAN `taosd`，而是回退到 `nohup` 启动，避免环境差异放大。
- 最终验证结果：
  - `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_meta_force_repair.py` => `3 passed`
  - `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_com_cmdline.py -k repair_cmdline_phase1` => `1 passed, 1 deselected`
  - 两条验证都通过 `checkAsan.sh`，未发现 ASAN 错误。


## Long-Term Memory
- 后续任何新会话涉及 `test/cases/80-Components/01-Taosd/test_meta_force_repair.py` 或同类 pytest E2E 用例，默认先假设“正确入口”是：`cd test && ./ci/pytest.sh pytest <case>`，而不是直接调用裸 `pytest`。
- 若在 Codex CLI / 沙箱环境中出现“pytest 能收集但长时间无输出”的现象，优先排查测试运行环境，而不要先怀疑业务代码本身。
- 这次明确验证过的高价值排查顺序如下：
  1. 先确认是否走了正确测试入口（`./ci/pytest.sh`）
  2. 再检查是否存在“构建版 + 安装版” TDengine 混用（`debug/build/bin/taosd` vs `/usr/local/taos/bin/taosd`，以及 Python `taos` 默认加载 `/usr/lib/libtaos.so`）
  3. 再检查测试框架 localhost 执行路径是否被 ASAN/LSAN 环境污染
  4. 再检查 localhost 启动 `taosd` 时是否依赖 `screen`、是否实际生成 `taosdlog.0`
  5. 最后再看 `taosd` 自身日志与 `sim/asan/dnode1.asan`
- 这次最终定位出的关键环境问题包括：
  - `Remote.cmd(localhost, ...)` 的实现会卡住
  - `./ci/pytest.sh` 注入的 `LD_PRELOAD` / `ASAN_OPTIONS` / `LSAN_OPTIONS` 会污染普通本地 shell 命令
  - localhost + ASAN 启动 `taosd` 时若不关闭 leak 检测，LSAN 会直接中止进程
  - localhost 启动链路不应盲目依赖 `screen`
  - 当前沙箱环境中还可能出现 `taosd` 绑定端口受限（`EPERM`）的问题，因此必要时应在沙箱外复测
- 后续若再次看到 `taos.error.ConnectionError: [0x000b]: Unable to establish connection`，不要立刻判断为业务缺陷；先检查：
  - `taosd` 进程是否真的起来
  - `sim/dnode1/log/taosdlog.0` 是否存在
  - `sim/asan/dnode1.asan` 是否有 LSAN/ASAN 报错
  - 是否是测试框架环境或沙箱权限问题
- 本任务里，真正用于交付的业务/测试文件与“仅为当前环境验证服务的测试框架适配”应分开看待；如果用户不想提交测试框架修改，不必强行把环境适配补丁带进最终提交。
- 已根据 code review 修复两个业务层问题：
  - `meta force repair` 现在会记录“本次启动内已完成恢复的 vnode”，避免同一 vnode 在后续 `metaOpen()` 中被重复 force repair。
  - 外部备份目录若已存在，不再直接删除覆盖，而是返回 `TSDB_CODE_FS_FILE_ALREADY_EXISTS`，避免同一天内的先前备份被静默抹掉。
- 回归验证通过：
  - `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_meta_force_repair.py` => `3 passed`
  - `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_com_cmdline.py -k repair_cmdline_phase1` => `1 passed, 1 deselected`
- 已按新的边界要求做了一轮收敛重构：`dmMain.c` 不再持有 `tsMetaForceRepair*` 这类 `meta` 专有全局状态，改为只暴露中性的 repair accessor；`metaOpen.c` 通过这些 accessor 自行判断 `meta force repair` 语义、vnode 命中、备份根目录与已消费状态。
- 这轮重构后再次验证通过：
  - `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_meta_force_repair.py` => `3 passed`
  - `cd test && ./ci/pytest.sh pytest cases/80-Components/01-Taosd/test_com_cmdline.py -k repair_cmdline_phase1` => `1 passed, 1 deselected`
