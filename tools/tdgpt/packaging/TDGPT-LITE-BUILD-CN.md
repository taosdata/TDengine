# TDgpt Lite 三平台离线资源构建

Linux 新产物统一使用 `tar.xz`；构建机和安装目标机需提供 `xz`（Debian/Ubuntu 安装 `xz-utils`，RHEL 系安装 `xz`）。Python 资源构建/导入需有 `lzma` 模块。历史 gzip 资源仍兼容，Windows EXE/ZIP 不变。

本文说明 TDgpt Lite 在 Linux x64、Linux ARM64 和 Windows x64 上使用的 Python runtime 与主 venv 构建流程。各架构必须在对应的原生节点构建，不支持交叉构建。

模型资源、共享 TSFM runtime 和 release manifest 的协议见 [`TDGPT-LITE-DESIGN-CN.md`](TDGPT-LITE-DESIGN-CN.md)。

## 目录结构

```text
packaging/
├── README.md                         # 文档总入口
├── TDGPT-LITE-BUILD-CN.md            # 本文：构建流程
├── TDGPT-LITE-DESIGN-CN.md           # 安装与动态资源设计
├── TDGPT-LITE-VALIDATION-CN.md       # 三平台验收流程
├── build_python_runtime_linux.sh     # Linux Python runtime
├── build_linux_offline_assets.sh     # Linux Lite 主 venv
├── build_linux_offline_release.sh    # Linux Lite 组合发布
├── build_windows_lite_venv.py        # Windows Lite 主 venv
├── win_release.py                    # Windows Lite 安装包
├── installer/                        # Inno Setup 模板
├── bin/                              # WinSW、uv 等构建缓存
└── offline-pkgs/                     # TSFM 资源上传入口

../offline-pkgs/
├── build_tsfm_runtime.py             # 共享 TSFM runtime
├── split_model_resource.py            # 全量模型 ZIP 拆分
├── tsfm_model_catalog.json            # 模型身份与命名目录
└── README.md                         # 模型资源构建说明
```

安装包、Python runtime、主 venv、共享 TSFM runtime、模型 ZIP 和 release manifest 是相互独立的发布物。`packaging/` 负责构建和验收，`../offline-pkgs/` 负责模型资源拆包与 runtime 构建；上传由 `packaging/offline-pkgs/upload_tsfm_release.py` 负责。

## 1. 发布物

Linux Python runtime:

```text
tdengine-python-runtime-3.12.12-linux-<x64|arm64>.tar.xz
```

Linux Lite 主 venv：

```text
tdengine-tdgpt-lite-venv-<version>-linux-<x64|arm64>.tar.xz
```

Windows Lite 主 venv：

```text
tdengine-tdgpt-lite-venv-<version>-windows-x64.zip
```

Linux runtime 包以 `bin/`、`lib/` 等目录为归档根目录。Apex 默认将其解压至 `/usr/local/taos-deps/python-runtime`。Linux 和 Windows 主 venv 均根据 `requirements_docker.txt` 构建。

## 2. Linux 构建

### 2.1 构建要求

构建节点需要提供：

- `uv`
- `patchelf >= 0.14.0`
- C 编译器
- SQLite 开发头文件

Debian 或 Ubuntu 可执行：

```bash
apt-get update
apt-get install -y build-essential libsqlite3-dev patchelf
curl -LsSf https://astral.sh/uv/install.sh | sh
```

可通过 `--uv-exe` 或 `--patchelf` 指定不在 `PATH` 中的工具。

### 2.2 构建 Python runtime

```bash
cd source/taos-community/tools/tdgpt

bash packaging/build_python_runtime_linux.sh \
  --arch arm64 \
  --work-dir /data/chr/tdgpt-runtime-build \
  --output-dir /data/chr/dist
```

在 x64 节点使用 `--arch x64`。脚本从 `uv` 管理的 CPython 发行包构建 runtime，携带非 glibc 运行库并设置相对 RPATH。构建结束前会扫描所有 ELF 文件，存在未解析依赖时直接失败。

部分 ARM64 CPython 发行包缺少 `_sqlite3`。出现这种情况时，脚本会使用构建节点的 SQLite 开发包编译该扩展，并在归档前验证 `import sqlite3`。

### 2.3 构建 Lite 主 venv

```bash
bash packaging/build_linux_offline_assets.sh \
  --bundle main \
  --lite \
  --arch arm64 \
  --build-python-home /data/chr/tdgpt-runtime/python-runtime-3.12.12 \
  --work-dir /data/chr/tdgpt-main-venv-build \
  --output-dir /data/chr/dist
```

构建的 venv 默认引用 `/usr/local/taos-deps/python-runtime`。安装时， `install.sh --python-home` 会修复迁移后的 venv 引用和 `taosanode_service.py` 入口。

### 2.4 可选：一次构建 Linux 发布物

日常更新 TDGPT 代码只执行 `script/release.sh -l -e enterprise -v <版本号>`。 Python runtime 仅在 Python 或底层库变更时构建；主 venv 仅在依赖变更时使用 `build_linux_offline_assets.sh --lite --bundle main` 构建。安装测试复用正式依赖包。下面的组合入口会重建全部三个制品，只用于确实需要全套重建的场景。

```bash
bash packaging/build_linux_offline_release.sh \
  --lite \
  --version 3.4.2.0.0713 \
  --edition enterprise \
  --arch arm64 \
  --work-dir /data/chr/tdgpt-release-build \
  --output-dir /data/chr/dist
```

该入口同时生成发布物和 `SHA256SUMS`。安装包与模型资源保持独立，模型不得嵌入 Lite 安装包。

## 3. Windows x64 构建

在 Windows x64 节点准备已解压的 Python 3.12 runtime，然后执行：

```bat
cd source\taos-community\tools\tdgpt

python packaging\build_windows_lite_venv.py ^
  --runtime-dir D:\tdgpt-build\python-runtime ^
  --work-dir D:\tdgpt-build\lite-main-venv ^
  --output-dir D:\tdgpt-build\dist ^
  --version 1.0.2
```

脚本会创建全新 venv、执行 `pip check`、校验关键模块导入，并写入 `requirements.lock` 和 `manifest.json`。ZIP 根路径固定为 `venvs/venv/`。打包前删除缓存、依赖库 test/tests 目录及普通文档/man；保留 .pyi、许可证、 Include/include 下的头文件、静态库和 pip/setuptools/wheel/Cython。清理后执行 pip check 和运行库导入验证，最终再清缓存后生成 ZIP。工作目录必须为空，防止历史 venv 混入发布包。

Windows Lite EXE 使用 `win_release.py` 构建，构建机需要 Python 3.10、3.11 或 3.12 以及 Inno Setup 6。安装包不内置 runtime、主 venv 或模型资源；这些资源通过独立归档导入。静默安装时可使用 `/OFFLINE=<venv-zip>` 和 `/PYTHON_RUNTIME_DIR=<runtime-dir>`，安装目录参数 `/DIR=<path>` 仅首次安装有效。

## 4. 共享 TSFM runtime 和模型资源

### 4.1 构建共享 TSFM runtime

共享 TSFM runtime 由 `../offline-pkgs/build_tsfm_runtime.py` 构建。它是官方 Moirai、Moment、Chronos、TimesFM、Time-MoE 和 TDTSFM 共用的模型 venv；同一平台只需导入一次。

Linux x64 示例：

```bash
python3 ../offline-pkgs/build_tsfm_runtime.py \
  --runtime-dir /data/chr/python-runtime \
  --platform linux \
  --arch x64 \
  --version 1.0.0 \
  --work-dir /data/chr/tsfm-runtime-build \
  --output-dir /data/chr/dist
```

Linux ARM64 使用 `--arch arm64`，Windows x64 使用：

```powershell
python ../offline-pkgs/build_tsfm_runtime.py `
  --runtime-dir D:\tdgpt-build\python-runtime `
  --platform windows `
  --arch x64 `
  --version 1.0.0 `
  --work-dir D:\tdgpt-build\tsfm-runtime `
  --output-dir D:\tdgpt-build\dist
```

脚本默认读取 `requirements_tsfm.txt`，也可以通过 `--requirements` 指定文件。输出文件分别为：

```text
tdengine-tdgpt-tsfm-runtime-1.0.0-linux-x64.tar.xz
tdengine-tdgpt-tsfm-runtime-1.0.0-linux-arm64.tar.xz
tdengine-tdgpt-tsfm-runtime-1.0.0-windows-x64.zip
```

### 4.2 拆分模型资源 ZIP

模型资源不由 Lite 安装包构建入口生成。使用 `../offline-pkgs/split_model_resource.py` 将全量模型 ZIP 拆为单模型 ZIP：

```bash
python3 ../offline-pkgs/split_model_resource.py \
  --source /pkgs/TDengine/deps/tdengine-tdgpt-model-resource-1.0.0.zip \
  --output-dir /pkgs/TDengine/tsfm/1.0.0 \
  --version 1.0.0
```

脚本根据 `tsfm_model_catalog.json` 读取模型身份并生成 Hugging Face 风格的小写文件名，例如：

```text
tdengine-tdgpt-model-moirai-moe-1-0-r-small.zip
tdengine-tdgpt-model-moment-1-base.zip
```

同时生成模型清单，记录模型 ID、版本、归档大小和 SHA256。模型 ZIP 内容保持原样，不加入 TDgpt 私有 manifest 或完成标志。

### 4.3 上传资源

```bash
python3 packaging/offline-pkgs/upload_tsfm_release.py \
  --release-manifest packaging/tdengine-tdgpt-tsfm-release-1.0.0.json
```

上传脚本按清单批量提交 runtime、模型 ZIP 和 release manifest，并轮询异步任务至成功或失败。发布前应先使用 `--dry-run` 检查目标文件名和容器。

## 5. ARM64 兼容性

ARM64 Moirai 使用的 `jaxlib/libjax_common.so` 可能把 `clock_gettime@GLIBC_2.17` 绑定到 `librt.so.1`，但 Kylin V10 的 `librt` 不导出该符号。构建脚本会将依赖修复为 `libc.so.6`。

`patchelf 0.9` 只能修改 `DT_NEEDED`，无法正确更新该 ELF 符号版本引用，因此必须使用 `patchelf >= 0.14.0`。发布通用 `linux-arm64` 资源前，需要分别在 Kylin V10 ARM64 和 Ubuntu ARM64 上验证 JAX 导入及 Moirai 启动。

## 6. 自动化建议

Linux x64、Linux ARM64 和 Windows x64 应分别使用原生构建节点。构建任务输入应包括 TDgpt Git ref、资源版本、安装包版本、架构和可选的软件源地址；输出应统一包含发布物及 SHA256 清单。模型资源通过独立的 TSFM 资源发布流程上传。

## 7. 下一版发布准备与版本决策（待执行）

本期产品暂定 `3.4.3.8`，主 venv 构建入口默认版本更新为 `1.0.3`。本节是发布评审与验收说明，不是已发布制品清单。本期保留 TSFM runtime / release `1.0.0`，不重建 TSFM；其 pyarrow 升级与兼容性问题另行推进，不阻塞本期主 venv 验证。用户算法持久化方案见 [设计文档第 6.1 节](TDGPT-LITE-DESIGN-CN.md#61-用户-python-算法持久化)，产品及已有模型的升级顺序见 [第 7.2.3 节](TDGPT-LITE-DESIGN-CN.md#723-下一版产品与已有动态资源的升级顺序发布方案)。版本号须在发版登记时核对未占用；不能覆盖已有同名制品。

### 7.1 与上一版的源码差异

核对基线为 `ver-3.4.3.3` 及实际验收标签 `validation/tdgpt-3.4.3.3-20261018`，目标为本方案核查时的 main 提交 `ca9da4fd3546b995aab14fab38700a87a2fa5380`。以下为源码声明的变化，两个基线到目标提交的这些 requirements 差异一致；不等于已确认上一版归档内实际安装版本。构建前还须读取上一版制品的 `requirements.lock` 与 manifest 作最终比对。

| 文件 / 消费方 | 基线到目标的变化 | 发布影响 |
| --- | --- | --- |
| `requirements_docker.txt`：Lite 三平台主 venv、Docker 主环境 | 新增 `xgboost==3.0.1`、`pyod==2.0.5`、`numba==0.66.0`、`llvmlite==0.48.0`、`pycatch22==1.0.1`；`requests==2.32.3` 改为 `2.33` | 重建主 venv；重建镜像依赖层，不能只替换程序层 |
| `requirements_tsfm.txt`：共享 TSFM runtime | `pyarrow==18.1.0` 改为 `23.0.1`（!3025） | 独立重建三平台 TSFM runtime，已安装环境须显式升级 |
| `requirements_ess.txt`：Full 依赖入口 | Keras 从 `3.12.0` 改为按 Python 版本选择 `3.15.0` / `3.12.3`；移除这里的 PyOD 声明 | 单独核对 Full 安装和镜像的依赖解析，不能套用 Lite 验收结论 |
| `requirements.txt`：通用依赖入口 | Pillow 从 `12.3.0` 改为 Python < 3.11 用 `12.3.0`、其余用 `13.0.0` | 仅消费该文件的构建受影响，不应据此断言 Lite 主 venv 已安装 Pillow 13 |

本次进一步将 `xgboost==3.0.1` 改为 `xgboost-cpu==3.0.1`，保留相同 `import xgboost` 接口。CPU 发行包的 Linux wheel 使用 manylinux2014，避免普通发行包在新系统上选择 manylinux_2_28 并拉入 NVIDIA NCCL；本期主 venv 不提供 GPU XGBoost，需验证分类、回归训练与推理。Linux 主 venv 新增 manifest 与完整 `requirements.lock`，并保留实际构建使用的 `requirements.in`；manifest 中的 `requirements_sha256` 对应该文件，包含构建脚本对架构的调整。构建阶段执行 `pip check` 及新增算法依赖导入；Windows 同步加强导入验证。Docker 构建也消费同一 requirements 文件。

因此下一版需要发布新主 venv，产品版本变化本身则不是重建 venv 的条件。 `requirements_docker.txt` 同时用于 Linux、Windows Lite 离线 venv，不只用于 Docker。版本范围和传递依赖仍可能解析出不同结果，例如 `mlxtend>=0.23.0`、`pyarrow<19`；新构建需归档完整锁文件，执行依赖一致性检查并解释所有冲突，不能仅凭 pip 安装成功判定可发布。NumPy、scikit-learn、Numba、llvmlite、PyOD 和 XGBoost 须实际导入并运行对应算法，验证 ABI、Python 版本和平台 wheel 是否兼容。

### 7.2 建议版本与发布绑定

| 发布物 | 当前仓库记录 | 下一版建议 | 是否跟随产品安装更新 |
| --- | --- | --- | --- |
| TDgpt 程序 / Docker 产品镜像 | 产品 `3.4.3.3` 历史验收记录 | 暂定 `3.4.3.8`，以实际发布登记为准 | 是 |
| Lite 主 venv | `1.0.2` | 本次构建默认 `1.0.3`，正式发布前确认未占用 | 安装器显式提供新包；Docker 由新镜像提供 |
| 基础 Python runtime | `3.12.12` | 未变更解释器或底层库时复用已验证制品 | 无须因产品升版而更换 |
| 共享 TSFM runtime | release `1.0.0` 对应的 runtime | 本期保留；后续候选 `1.0.1` | 否，使用资源命令显式替换 |
| TSFM release manifest | `1.0.0` | 本期保留；后续候选 `1.0.1` | 否，显式指定新清单 |
| 官方模型 ZIP | 六个已发布权重包 | 内容未变则复用原文件、模型版本和 SHA-256 | 否，无须重复下载权重 |

候选号不是已查询确认的发布库存。主 venv 版本与 TSFM runtime 版本独立，均不使用产品 `3.4.3.8` 代替。构建时显式传入主 venv 的 `--venv-version`（Linux）或 `--version`（Windows），TSFM builder 的 `--version`，不能沿用脚本默认的旧版本后上传覆盖。新清单记录每个平台 runtime 的真实文件名、大小、摘要以及模型的运行时要求；即使只是压缩格式变更也要发新资源版本。

发版须同步 platform-group 发版配置、下载条目和 Apex 组件依赖清单中的版本与 SHA-256，并以新主 venv 验证新程序。`DockerfileTDgpt` 当前引用两个基础镜像的默认 `1.1` 标签，但已支持 `TDGPT_ENV_VERSION` / `TDGPT_LITE_ENV_VERSION` 构建参数；requirements 更新不会自动更新已存在的基础镜像。下一版应先发布新的不可变基础镜像标签，再显式传入构建参数，例如 `--build-arg TDGPT_LITE_ENV_VERSION=<本次已发布基础镜像标签>`；不得遗漏参数而误用旧默认环境，也不得覆盖历史标签。Full 与 Lite、x64 与 ARM64 分别验证。 TSFM 资源不塞进 Lite 主 venv 或 Lite 镜像；Full 预置环境按其现有构建链路另行验证。

### 7.3 pyarrow 的两个环境与 Linux 兼容性阻塞项

Lite 主环境保留 `pyarrow<19`，不删除此依赖，也不因 TSFM 升级而统一到 23。 StatsForecast 的基础依赖链包含 `fugue`、`triad`，后者依赖 pyarrow；最终解析版本须以锁文件为准。该限制用于旧 Linux wheel 兼容，不等于已证明旧版本没有安全问题。TSFM 升级也不能视为主环境的 pyarrow 漏洞已修复；如扫描仍报漏洞，需按实际受影响版本独立评估和处理。

TSFM 独立环境使用 `pyarrow==23.0.1`。当前 `build_tsfm_runtime.py` 仍先显式安装 18.1.0 的 wheel，再安装 `requirements_tsfm.txt`；第二步会尝试升级到 23.0.1，不能仅凭预装步骤就断言最终包仍是 18.1.0。发版前须消除这一矛盾的预装路径，使默认构建和 `--pyarrow-wheel` 指定输入都与最终 requirements 一致，最终检查 `pyarrow.__version__`、锁文件和实际导入结果。仅修改 requirements 或归档名不算完成 runtime 升级。

已核对 [PyPI pyarrow 23.0.1](https://pypi.org/project/pyarrow/23.0.1/#files)： CPython 3.12 的官方 Linux x64、ARM64 glibc wheel 标签为 `manylinux_2_28`， Windows x64 有对应 wheel。现有 glibc 2.27 / Ubuntu 18.04 的支持承诺不能由这些 Linux wheel 满足。在新系统上构建后打包 venv，也不会降低其中 `.so` 的 glibc 要求。

后续 TSFM 升级若保持已有最低系统支持：须构建并验证适用于 glibc 2.27 的 pyarrow 23.0.1 及其 Arrow 依赖，记录源码、构建参数、wheel 摘要和许可证；不能通过改 wheel 标签或沿用旧 jaxlib 的 patchelf 方法宣称兼容。该产物完成前，阻塞通用 Linux 新 TSFM runtime 的发布。如果无法提供兼容构建，应另行评审提高最低系统版本，并在安装前拒绝不支持的机器；不能静默缩减支持范围。Windows wheel 也须原生验证，不代表 Linux 阻塞已解除。不得将保留旧 runtime 标成已完成漏洞修复。

### 7.4 发版验收与交接

每个平台的发布记录须包含程序 SHA、构建输入与镜像 digest、Python 版本、完整依赖锁、资源版本、归档大小和 SHA-256、最低系统要求、验收报告及回滚介质位置。本方案不填写未构建资源的摘要，也不声明已通过三平台运行验证。

本期交付前完成第 1、2、5 项中与保留 TSFM 相应的验证，再完成第 6 项的主 venv 发布；第 3、4 项为后续 TSFM 升级条件。本期不得宣称已完成这些安全修复。

交接清单：

1. 实现设计第 6.1 节的加载器与安装迁移，完成该节的跨平台验收；同步用户手册的自定义算法说明和 Docker Compose 示例，不修改 CI 运维日志来充当产品文档。
2. 在原生 Linux x64、Linux ARM64、Windows x64 构建新主 venv，检查依赖一致性和基本算法；在支持的最低 Linux 系统与 Windows 上验证新安装和从上一版升级。
3. 解决第 7.3 节的 TSFM 构建阻塞，构建三平台 runtime；核对依赖及安全扫描，在最低支持系统加载六个官方模型，执行 readiness 和实际预测/补值请求。
4. 补充 runtime 版本要求校验，验证旧模型权重与新 runtime 配对；验证单模型 `apply` 不会隐式更换 runtime、相同权重摘要不会重下、切换 release 会更新状态，以及离线导入、运行中拒绝替换、失败后的显式回滚。
5. 用上一版的真实安装与数据走完设计第 7.2.3 节的升级/回滚流程；覆盖未安装 TSFM、已安装但未运行、多个模型运行、用户算法附加依赖及动态序列化模型。产品新装不会自动安装 TSFM，旧模型资源也不能被安装器清空。
6. 验收完成后发布制品与不可变清单，更新正式配置和用户升级说明，最后切换消费方。若要改变 CLI 默认 release 或实现 stable 发现机制，另行修改代码并测试；本方案不将第 7.2.1 节规划中的自动发现行为当作已实现。
