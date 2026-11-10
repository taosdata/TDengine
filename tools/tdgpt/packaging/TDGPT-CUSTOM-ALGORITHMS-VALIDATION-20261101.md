# 用户算法持久化与主 venv 升级验证记录

## 根包初始化与外部配置修复补测

用户根包存在 `custom/__init__.py` 时按普通包加载并执行一次；无该文件时继续使用命名空间包。初始化异常会清理部分导入，记录目录与原因，保留内置算法并允许后续重试。 Linux 实际 Lite 主 venv 的注册器回归 10 项通过，新增根包公共函数、单次初始化及失败重试检查。

控制器保存实际 `--config` 文件路径，传给应用的 `TDGPT_CONF` 与 Linux Gunicorn `-c` 使用同一路径。Windows 原生暂存程序复测使用 D 盘含空格的外部 `custom.py`，故意继承错误配置环境变量，实际自定义算法预测仍为 `[97, 98, 99]`；不操作全局服务。日志为 Windows 测试根目录 `external-config-probe.log`；Linux 为 `import-config-registry.log`。 Python 3.12 定向组合回归 81 项及 18 项 subtest 通过；Ruff 与文档排版检查通过。本次新增代码未重新构建 EXE，完整 EXE 生命周期结果仍对应下节登记的修复。

## 2026-11-03 宿主机服务升级补测

本轮只验收 Lite，Full 环境按用户要求不纳入本次范围。 Linux 产品包沿用前节 `a34191632618` 的 v3.4.2.9.1102； Windows 和 Docker 补充修复提交为 `c24eeb0`，程序依赖未改变。

- Linux x64：在 `192.168.2.104` 的独立 `host-systemd/install` 目录执行真实 `install.sh`，导入离线主 venv `1.0.3` 并注册 `taosanoded.service`。 systemd 启动后执行两次运行中升级，每次重新启动并完成自定义 ETS 预测，三次预测均为 `[97, 98, 99]`。旧 custom 文件迁移、退役及备份记录正确，再次升级保留管理员修改且不恢复已删除文件；模型、模型 venv 和资源标记保留。测试结束后服务停止并禁用，隔离安装及日志保留；原有 `/var/lib/taos/taosanode` 数据未替换。此结果不包含安装失败后的回滚验证，也不是旧正式版本的完整升级矩阵。
- Windows x64：备份旧的残留安装注册表后，在 D 盘含空格目录执行完整 EXE，实际导入离线 venv、注册并启动 LocalSystem 服务。首次发现 WinSW 的脚本路径未引用，含空格目录导致 Python 无法执行服务脚本。对路径添加引号后确认服务与预测恢复；随后从修复模板重建 EXE，完成两次运行中升级。每次启动后 `/status`、`/list` 和自定义 ETS 预测均通过。完整 EXE 卸载后服务移除，自定义算法、管理员修改、模型及资源标记保留，旧注册表记录恢复。该测试在本轮测试安装中构造旧算法目录，不等同于所有旧正式版本升级验收。
- Docker x64 Lite：在产品安装层结束后清理安装过程生成的 Python 字节码目录。重建镜像后先后更换两个容器，持久化算法与数据保留，两次预测均通过。 Docker 报告体积由 371055394 降至 368980940 字节，减少 2074454 字节（约 0.56%）。 gzip 导出文件系统由 335.9 MiB 降至 334.0 MiB。导出扫描确认无字节码目录；按文件类型复核无普通文档文件。通用扫描器仍把 `/usr/bin/test` 可执行文件、文档目录及软链列为失败，原始报告保留，不将该扫描器记为全绿。保留编译器、pip、类型存根和静态库，不涉及依赖裁剪。
- 本地 Python 3.12 Windows 打包回归 14 项通过。

修复后的 EXE 位于 Windows 验证根目录 `windows-lite-fixed/`，SHA-256 为 `4b919c5e4d9b0e606faaeefa4d4b18dd3bbeed4174d1376455d051010b65637a`。新镜像标签为 `tdgpt-lite:test-1102-clean-bytecode`，ID 为 `sha256:6548518b218cf2d0f146b12da4e1a079d046a6033e505e1f001abf784127ce6c`。

原始证据保留于前节同一测试根目录：Linux 的 `host-systemd/install.log`、 `host-systemd/upgrade-1.log`、`host-systemd/upgrade-2.log`、`host-systemd/lifecycle.log`、 `docker-build-fixed.log`、`docker-fixed-probe.log`、`image-fixed-audit.log`、 `image-fixed-file-audit.log`； Windows 的 `host-install.log`、`host-fixed-lifecycle.log`、`exe-host-upgrade.log`、 `exe-host-repeat-upgrade.log`、`exe-host-uninstall.log`、`build-windows-fixed.log`。此前失败证据 `host-lifecycle.log` 和 WinSW 日志也保留，不将首次失败覆盖为通过。

## v3.4.2.9.1102 开发分支整包复测

本轮按用户指定测试版本构建，不发布正式资源、不替换已有现场服务。源码固定为开发分支 `docs/tdgpt-custom-algorithm-release-plan` 的 `a34191632618e1ba29ddd53cc13683baf54f5f6d`，已同步 `origin/main`。复用依赖未变化、前文已验证的 Lite 主 venv `1.0.3`；TSFM 保持 `1.0.0`。

| 检查 | 结果 |
| --- | --- |
| 最新 CI | 父流水线 `68551`、TDgpt 子流水线 `68552` 成功；`test_tdgpt.py` 实际通过，耗时 51.4 秒，worker job `825389`。 |
| Linux x64 Lite 产品包 | 构建成功；真实安装器连续升级三次，迁移、备份记录、退役旧目录通过；管理员修改和删除保留，临时环境变量不影响迁移目标。真实卸载后用户数据保留。 |
| Docker x64 Lite 产品镜像 | 从本轮产品包构建；断网先后创建两个容器，共用四类持久化挂载，两次自定义 ETS 预测均为 `[97, 98, 99]`。错误继承配置被入口覆盖，工作目录正确。 |
| Windows x64 Lite EXE | 原生构建成功；使用其实际暂存程序在 D 盘含空格目录前台运行，迁移、重复迁移、删除保留、配置覆盖和自定义 ETS 预测通过。执行实际卸载器文件清理后自定义算法保留。 |

Windows 未执行完整 EXE 安装、全局服务注册或服务升级，以保留共享测试机现有安装。 Linux 安装测试在一次性容器内复用主 venv，不替代 systemd、完整离线 venv 替换和回滚验收。本轮未重建 Full、ARM64 产品镜像，也未重新验证全部动态模型。

产物摘要：

| 产物 | 字节数 | SHA-256 |
| --- | --- | --- |
| `tdengine-tdgpt-oss-lite-3.4.2.9.1102-linux-x64.tar.xz` | 171440 | `6a30bf4b17931a780149fbfe1cd33b506eb0a03efcd86f754842426f667af470` |
| `tdengine-tdgpt-oss-lite-3.4.2.9.1102-windows-x64.exe` | 2132771 | `34e34a22aaeba25bd96885ccec1ceef96925f4c4187dd52daf29269c3ac4c696` |

Docker 标签为 `tdgpt-lite:test-3.4.2.9.1102-a341916`，镜像 ID 为 `sha256:ac876976b331a2b7c5b5f963b308a4e4e430c9383527950d83d0e048e2c0fb07`， Docker 报告大小为 371055394 字节，gzip 压缩导出文件系统为 335.9 MiB。该值未与同类型旧产品镜像作严格体积比较。产品包内容检查剥离外层产品目录后通过，包含直接 `lib/taosanalytics` 和 `lib/conf.py`，无内嵌 `package.tar.*`、旧 custom 目录和字节码缓存。原始扫描器不剥离外层产品目录，会误报缺少直接程序目录，保留该原始报告。产品镜像扫描仍发现 Python/pip 字节码缓存和基础系统文档软链；不能记为内容扫描全绿，此项属于后续体积清理，不影响上述功能测试结论。

Linux 产物、脚本和日志保留于 `root@192.168.2.104`： `/data/chr/tdgpt-test-3.4.2.9.1102-a341916/`，包含 `build-linux.log`、 `docker-build.log`、`upgrade-test.log`、`docker-probe.log`、`archive-audit.log`、 `archive-normalized-audit.log`、`image-audit.log` 和 `image-file-audit.log`。 Windows 对应目录为 `administrator@192.168.2.74` 的 `D:\cd-test\tdgpt-test-3.4.2.9.1102-a341916\`，包含 `build-windows.log`、 `windows-probe.log` 和 `windows-foreground.log`。

## 2026-11-02 评审修复补充

本节记录 `1e699ee` 之后的评审修复，依赖版本未改变，未重新发布资源包。以下检查使用当前修复代码；旧提交对应的整包结果不能代替本次整包验收。

- Windows 当前 `lib/conf.py` 和历史布局均可推导实际安装目录；服务显式传入当前 `TDGPT_CONF`，避免继承其他安装的配置。覆盖非默认目录及路径含空格的情况。
- Linux 先做只读冲突预检查，再停止旧 worker 与主服务，最后迁移并退役旧目录。停止失败保留旧源与程序；不采用忽略停止失败继续升级的建议。
- 安装迁移忽略临时 `TDGPT_CUSTOM_ALGO_DIR`，使用持久配置；运行时继续支持该变量。
- 不支持硬链接的文件系统使用独占创建复制；发生复制异常清理本次半成品，并发创建的目标不覆盖。迁移前一次报告全部目标冲突。
- Windows 安装器将迁移错误写入安装日志并返回失败；旧配置追加内容使用 ASCII 转义并先编译校验，保留 GBK、UTF-8 BOM 和中文目录。
- Docker 两个入口统一工作目录为安装根目录，并显式导出配置文件路径。
- Linux Lite 主 venv 保留实际生效的 `requirements.in`，可核对 manifest 的输入摘要。

本地 Python 3.10、3.12 的安装迁移、服务、发行包与 Docker 配置回归各 85 项通过，各有 18 项 subtest 通过。Linux 实际主 venv 的迁移和注册器回归 33 项通过； Windows 同组 30 项通过、3 项按平台跳过（两个 Linux shell 用例及符号链接权限）。

原生回归日志保留于 Linux x64 与 Windows 原有验证根目录下的 `review-fixes-20261102/regression.log`。Docker 实际入口检查日志位于 Linux 同目录的 `entrypoint-probe.log`：复用 `tdgpt-lite:review-3123-f02b1f2` 测试镜像，只挂载本次程序库与入口脚本；断网启动，覆盖故意传入的错误 `TDGPT_CONF`，确认工作目录，调用 `/status`、`/list` 和自定义 ETS 预测，结果为 `[97, 98, 99]`。此检查不是最新完整镜像的重新构建，也不代替 Full 镜像验收。

Full 尚无本轮确认的依赖冲突。Linux 构建脚本对 Lite 和 Full 的主 venv 均执行 `pip check`，不是对全部模型 venv 新增检查。Full 使用 `requirements_ess.txt`，还包含 Torch、Transformers、Keras 等依赖，须单独重建并验证，不能以 Lite 通过代替。

### AI 评论逐项核对

以下按 GitLab 评论 ID 记录判断。已逐条回复并解决 22 个可解决的行内讨论；两条汇总评论没有可解决状态。讨论关闭不代表尚未执行的整包验收已完成。

| 评论 ID | 判断与处理 |
| --- | --- |
| `185927` | 不改包名回退。Windows 原生主 venv 已验证 `xgboost-cpu` 分发名，与构建输入一致。 |
| `185928` | 当前 `lib/` 仅包含运行库，未发现被混入的开发文件；不按假设删除有效模块。 |
| `185930` | 采纳可追溯性改进，保留实际 `requirements.in`，摘要继续对应生效输入。 |
| `185931` | 修正 `pip check` 行缩进。 |
| `185932`、`185933` | 两个容器入口均显式传入 `TDGPT_CONF`。默认配置原本通过安装器软链一致，现消除隐含依赖。 |
| `185934` | 用实际 Lite 容器启动和自定义预测验证入口，避免仅增加字符串断言。 |
| `185935` | 未发现功能问题；重复目录扫描属于可选性能优化，本次保留既有裁剪范围。 |
| `185936` | 不成立：`win_release.py` 操作打包暂存目录，不是用户安装现场。 |
| `185937` | 不删除有限值断言：输入为固定测试序列，已有三平台原生算法验证通过。 |
| `185938` | 采纳迁移异常提示改进，安装器记录失败详情并返回失败。 |
| `185939` | 不跳过首次安装：安装前已有 Python 检查，且首次安装也需要准备默认目录。 |
| `185940` | 不忽略停止失败；改为停止前预检查，停止失败保留旧目录并退出。 |
| `185941` | 不静默回退：非法显式配置应阻止启动，符合已约定行为。 |
| `185942` | 保留双向重叠保护，避免用户目录与安装器、模型资源管理范围相互覆盖。 |
| `185943` | 保留可执行管理员配置语义，与服务现有加载方式一致；不能改成只解析字面量。 |
| `185944` | 采纳编码防护：保留原字节，追加 ASCII 转义并编译校验，补 GBK、BOM 与中文目录回归。 |
| `185945` | 解析仅用于启动加载，未发现需要引入缓存及失效机制的性能问题。 |
| `185946`、`185947` | 两个入口统一从安装根目录启动。 |
| `185948` | 参数已实际用于 `FROM`；保留现有构建验证，不增加只镜像实现的字符串测试。 |
| `185949` | 不成立：发行包的 `install.sh` 与 `lib/` 同级，包布局已有回归和实际安装验证。 |
| `185950` 第 1 项 | 不成立：打包前 `clean_release_dir()` 已清理整个发行暂存根目录，不会增量混入旧模块。 |
| `185950` 第 2 项 | 原评论也明确不是问题；只删除旧 custom 占位目录，内置算法仍交付。 |

仍须补齐最新修复后的完整 Windows EXE 安装、升级、卸载，以及 Linux 宿主机服务升级、回滚和完整离线 venv 替换；本轮没有修改已有现场服务。

## 范围与源码

本记录对应 [!3123](https://git.tdengine.net/rd-public/tsdb/-/merge_requests/3123)。主 venv 测试源码提交为 `fecb29b40d51f0059d0c4378ed7414473ba4e6fa`，已包含 `origin/main` 的 `ca9da4fd3546b995aab14fab38700a87a2fa5380`。三平台使用该提交的 TDgpt 源码归档构建，不混入本地未提交代码。后续程序修复与整包验证使用的提交见下文；主 venv 依赖未再次改变。

当前产物仅供测试，未发布至生产下载目录，也未替换现场服务。组长评审并合并后，须从合并后的 main 提交重新构建正式产物，重新记录摘要。

- 程序：配置 `custom_algo_dir`，独立发现 `ad/`、`fc/` 算法，升级前备份和迁移旧目录。
- Lite 主 venv：候选版本 `1.0.3`，Python `3.12.12`；包含 main 新增的 `pycatch22==1.0.1` 及此前增加的 XGBoost、PyOD、Numba、llvmlite。
- XGBoost 使用 `xgboost-cpu==3.0.1`，保持 `import xgboost`，不提供 GPU 功能。
- 主环境实际解析为 `pyarrow==18.1.0`，没有删除或升级到 23。
- TSFM runtime / release 继续使用 `1.0.0`；本次不构建新 TSFM，不宣称修复其旧依赖漏洞。

## 已执行检查

| 环境 | 结果 |
| --- | --- |
| 本地 Python 3.12 | 安装器、Windows 打包、服务入口、归档安全与迁移测试：73 项通过，另有 12 项 subtest 通过 |
| Linux x64，`192.168.2.104` | 主 venv 构建、解压后算法检查通过；迁移、真实注册器与动态模型同步共 24 项通过 |
| Windows x64，`192.168.2.74` | 主 venv 构建、算法检查通过；同组 24 项回归中 22 项通过，2 项平台跳过 |
| Linux ARM64，`192.168.3.91` | 原生 Ubuntu 18.04 / glibc 2.27 环境构建；解压后算法检查及同组 24 项回归全部通过 |
| Docker Linux x64 | 新依赖基础层构建通过；断网容器内 `pip check`、编译器检查和算法检查通过 |
| Linux 产品包 | Lite OSS 测试包构建通过；去除外层产品目录后检查直接 `lib/taosanalytics` 布局、迁移脚本与旧 custom 占位目录排除，均通过 |

算法检查包含 XGBoost 分类和回归、PLS、PyOD IForest、catch22 特征提取、 ETS/CES/Theta 预测及 Cython 代码生成。它不等同于完整的用户 C/C++ 扩展编译、链接验收。构建检查还包括 `pip check`，并在主 venv 中记录 `requirements.lock` 与 manifest。

迁移回归覆盖旧代码备份、重复升级、冲突时拒绝覆盖、相对导入、辅助文件保留、环境变量覆盖、非法路径拒绝、重复名称拒绝、坏模块隔离及实际算法调用。 Linux 测试执行安装器原有清理函数后，确认算法、模型权重及 TSFM manifest 仍保留。 Windows 跳过的两项分别是 Linux shell 清理测试及需要额外权限的符号链接测试。

## 测试主 venv 产物

| 平台 | 文件名 | 字节数 | SHA-256 |
| --- | --- | --- | --- |
| Linux x64 | `tdengine-tdgpt-lite-venv-1.0.3-linux-x64.tar.xz` | 150381740 | `c6afcab5927f995d9a1e1d853dafde84e5ee2355b69661d9b086bec15d40b3ba` |
| Linux ARM64 | `tdengine-tdgpt-lite-venv-1.0.3-linux-arm64.tar.xz` | 136602768 | `62f3d123d4c284ef6f45105a591520b16f7553157290dcc666728519a11ff943` |
| Windows x64 | `tdengine-tdgpt-lite-venv-1.0.3-windows-x64.zip` | 193517791 | `711eaced52b128538d689084deae5bcb5282e801c5af5601cf3e1da1459697e1` |

三平台主 venv 的归档内容扫描通过，未发现裁剪规则禁止的缓存、测试目录或普通文档文件。

产物、锁文件和原始日志保留在各原生验证节点：

- Linux x64 节点：`/data/chr/tdgpt-custom-upgrade-20261101/review-fecb29b/`
- Linux ARM64 节点：`/root/chr/tdgpt-custom-upgrade-20261101/review-fecb29b/`
- Windows：`D:\cd-test\tdgpt-custom-upgrade-20261101\review-fecb29b\`

## 镜像内容检查

Docker x64 测试依赖镜像为 `tdgpt-lite-env-amd64:review-3123-fecb29b`，镜像 ID 为 `sha256:91af41887d8ca20a540f6a1f229132794211528e599f41f200ef0f4714fe1d30`。 Docker 报告大小为 368593114 字节，gzip 压缩导出文件系统为 333.8 MiB。这是当前候选配置的实测值，不是与上一正式发行版本的体积比较。

静态 Dockerfile 检查通过，保留 gcc、libc-dev、Cython、pip、头文件及静态库。通用扫描器原始镜像报告未通过：它把 `/usr/bin/test` 可执行文件和 `/usr/share/doc` 的目录项、链接也计为残留。因此另按文件类型复核镜像： Python 测试目录、字节码缓存和普通文档文件检查通过；基础系统仍有一个 `/usr/share/doc/cpp-x86-64-linux-gnu/README.Bugs` 文档软链。不将原始扫描器报告记为全绿，原始报告与复核日志分别保留为 `image-audit.log` 和 `image-file-audit.log`。

## 旧 TSFM 兼容性

2.104 遗留 TSFM 的 Python 链接指向已删除的历史测试目录。原目录未修复或替换；在 `compat/model_venvs/tsfm` 隔离副本中，仅重建指向现有 Python 3.12.12 runtime 的解释器链接，不重装或升级其中依赖。

使用新主 venv 与新程序执行资源校验：旧 `tsfm-runtime-1.0.0`、Moirai 和 Moment 权重均为 `valid`。再以旧 TSFM 隔离副本运行新 Moirai worker 代码，从本地已有权重执行一次 64 点输入、3 点输出的预测，返回 HTTP 200 和有限数值。使用 Flask 测试客户端，未启动监听端口；模型权重未重新下载。原始结果位于验证目录上一级的 `compat/verify-review.json` 与 `compat/moirai-inference.log`。

这验证了上述运行时与 Moirai 的组合，不代表全部官方模型和用户序列化模型已验收。

## 补充整包验证与 CI 修复

整包测试发现并修复了 Windows 打包器仍读取旧程序目录、服务入口的 `lib` 导入路径、依赖发现路径以及 Gunicorn 23 不支持 `--wsgi-app` 参数的问题。 Linux 与 Docker 最终复测使用已提交源码 `f02b1f27263f633b8a6e736e3915c71b428de4df`，复用上述依赖未变化的主 venv。

- Docker 产品镜像 `tdgpt-lite:review-3123-f02b1f2`：断网启动两个先后创建的容器，复用算法、模型、模型 venv、资源状态四类挂载。两次真实预测均返回三行 `[97, 98, 99]`，算法注册与持久化标记保留。
- Linux 真实安装脚本：在一次性容器内模拟旧算法目录和旧配置，连续安装两次，旧算法迁移、备份及配置更新通过；运行真实卸载脚本后四类用户数据仍保留。此项复用镜像内主 venv，不替代宿主机 systemd 和完整离线 venv 替换验收。
- Windows 在提交 `7348b979a6e1b8736286ab1dcfc366e23f428002` 上原生构建 `tdengine-tdgpt-oss-lite-3.4.3.8-windows-x64.exe` 成功。使用安装器暂存的实际程序文件，在随机本机端口以前台模式启动服务， `/status` 就绪且 `/list` 包含外置自定义算法。未执行 EXE 安装，未修改已有全局服务。
- 失败任务 `816507` 为 Ruff 格式检查，涉及四处导入排序和两处长行；已修复。 Ruff 0.17.3 全目录检查通过，最新任务 `816730` 通过。另修复单元测试中旧目录布局和旧 venv 默认版本的预期；最新 Python 3.10、3.12 单元测试任务 `816728`、`816729` 均通过。

Linux 原始记录位于 `192.168.2.104` 的 `/data/chr/tdgpt-custom-upgrade-20261101/review-f02b1f2/`： `persistence-test.log`、`upgrade-test.log`。 Windows 原始记录位于 `192.168.2.74` 的 `D:\cd-test\tdgpt-custom-upgrade-20261101\review-7348b97\`： `installer-build.log`、`foreground-probe.log`、`foreground.log`。

## 再次升级与路径保护回归

迁移完成并校验新目录、备份与源文件一致后，原子退役旧目录。备份目录保留迁移 JSON 记录，包含文件 SHA-256 与退役状态。默认算法路径改为从 `data_dir` 派生的 `<data_dir>/custom`，Windows 同样使用 `<安装目录>/data/custom`。配置与安装器不单独写死算法路径，显式配置和环境覆盖仍保留。新增回归覆盖以下情形：

- 迁移后修改或删除新目录算法，再次升级保留修改且不恢复已删除文件。
- 目标文件原本已相同，也先保留备份与记录，再退役旧目录。
- 复制中断或备份校验失败时保留旧源；复制中断后可重试。
- 旧目录退役后的清理中断，不会在后续升级重新导入旧算法。
- Windows 拒绝将算法配置到卸载清理的程序目录；默认 `data/custom` 仍可直接使用。
- 仅配置或更改 `data_dir` 时默认路径跟随变化；显式算法路径和环境变量优先级保留。

本地 Python 3.12 的打包、服务入口与安装迁移组合回归共 73 项通过、 12 项 subtest 通过；Linux x64 使用实际主 venv 的迁移与注册器回归 25 项通过， Windows 原生同组回归 23 项通过、2 项平台跳过。TDgpt 全目录 Ruff 0.17.3 检查通过。上述结果包含本节补充修复，整包与镜像实测仍对应前文登记的源码提交，不能视为本次修复后已重新完成完整 EXE 安装或宿主机服务升级验收。

## 合并与正式发布前仍需完成

- Windows Lite 完整 EXE 服务生命周期已完成本轮补测；其他旧正式版本组合仍按实际发版需求验收。
- Linux Lite 宿主机离线安装与服务升级已完成本轮补测；安装失败后的回滚仍未验证。
- Docker ARM64 镜像构建和运行检查；原生 ARM64 主 venv 验证不能替代镜像验收。
- 逐个验证现场实际使用的官方模型和用户模型，记录并恢复原先运行的 worker。
- 正式版本登记后同步发版配置、Apex 清单、下载地址及真实 SHA-256；测试包不得直接当作正式包发布。
