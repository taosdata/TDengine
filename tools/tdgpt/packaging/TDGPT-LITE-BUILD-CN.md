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

Python 3.12.16 升级批次的 Lite 主 venv 候选版本为 `1.0.4`，共享 TSFM runtime 候选版本为 `1.0.1`。构建、JSON 元数据、SHA256 和 Apex YAML 必须引用同一批候选产物，不能覆盖已发布的 Lite `1.0.3` 或旧 TSFM 资源；候选版本号不表示已经完成发布验收。验收记录须对应实际构建的 `1.0.1` 归档、包内 manifest、锁文件及 SHA-256，不能仅根据文档版本号判断资源已部署或正式发布。

## 1. 发布物

Linux Python runtime:

```text
tdengine-python-runtime-3.12.16-linux-<x64|arm64>.tar.xz
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
apt-get install -y patchelf
curl -LsSf https://astral.sh/uv/install.sh | sh
```

可通过 `--uv-exe` 或 `--patchelf` 指定不在 `PATH` 中的工具。

构建 `3.12.16` 时使用 `uv >= 0.13.9`。旧版 uv 的索引可能只提供到 `3.12.15`，可在隔离目录升级并通过 `--uv-exe` 使用，避免覆盖构建节点上其他流水线使用的 uv：

```bash
mkdir -p /data/chr/tools/uv-0.13.9
curl -LsSf https://astral.sh/uv/0.13.9/install.sh \
  | UV_INSTALL_DIR=/data/chr/tools/uv-0.13.9 sh
```

### 2.2 构建 Python runtime

```bash
cd source/taos-community/tools/tdgpt

bash packaging/build_python_runtime_linux.sh \
  --arch arm64 \
  --work-dir /data/chr/tdgpt-runtime-build \
  --output-dir /data/chr/dist
```

在 x64 节点使用 `--arch x64`。脚本只从 `uv` 管理的 CPython 发行包构建 runtime；如果索引中没有目标 patch 版本，构建直接失败，不再隐式切换到源码构建。构建结束前会设置相对 RPATH、扫描所有 ELF 文件，存在未解析依赖时直接失败。

脚本使用 uv 提供的 CPython 发行包，不再从源码编译缺失的 `_sqlite3` 扩展；归档前会直接验证 `import sqlite3`，缺失时构建失败。

Linux Lite 与共享 TSFM runtime 的最低支持版本统一为 glibc 2.28（x64 / ARM64），后续版本沿用该基线；glibc 2.27 / Ubuntu 18.04 不再列入支持范围。使用固定基线的 Docker 构建入口，避免构建节点环境差异。Python runtime 入口继续使用 manylinux2014（glibc 2.17）容器下载和校验 uv 发行包，并固定 `uv 0.13.9`；构建容器或个别依赖的较低 glibc 要求不扩大产品支持范围：

```bash
bash packaging/build_python_runtime_linux_docker.sh \
  --python-version 3.12.16 --arch x64
```

入口会检查宿主工作目录和输出目录，并使用实际容器 UID 对绑定挂载执行临时文件写入检查。启用 rootless Docker 或 `userns-remap` 时，需要先为映射后的 UID 配置目录权限；检查失败会在下载 Python 前终止。`--image` 支持 `repository[:tag]`（包括带端口的 registry），脚本将 Python 版本和架构追加到 tag；省略 tag 时自动创建 tag。镜像工具链的固定 uv 版本与底层脚本的最低 uv 版本分别维护。

ARM64 原生节点使用 `--arch arm64`。该入口在本机架构容器内使用 uv 下载对应架构的 CPython 发行包，并复用同一套 ELF 依赖、标准库导入和 SHA256 归档校验。Docker 不替代真实安装验收，构建完成后仍需在 glibc 2.28 节点解压、运行 `ldd`、导入标准库并启动服务。

### 2.3 构建 Lite 主 venv

```bash
bash packaging/build_linux_offline_assets.sh \
  --bundle main \
  --lite \
  --arch arm64 \
  --build-python-home /data/chr/tdgpt-runtime/python-runtime-3.12.16 \
  --work-dir /data/chr/tdgpt-main-venv-build \
  --output-dir /data/chr/dist
```

构建的 venv 默认引用 `/usr/local/taos-deps/python-runtime`。安装时， `install.sh --python-home` 会修复迁移后的 venv 引用和 `taosanode_service.py` 入口。

Linux Lite 构建保留已验证的 PyArrow 18.1.0 CPython 3.12 manylinux2014 wheel 及其 PyPI SHA256，以保持本轮归档输入稳定；该 wheel 可在 glibc 2.28 上运行，不表示产品继续支持 glibc 2.27。wheel 和 requirements 在同一次解析中安装，后续调整依赖须重新验收。TSFM 的 PyArrow 23.0.1 及 Windows 构建不使用此 Linux Lite 选择逻辑。

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
  --python-version 3.12.16 ^
  --work-dir D:\tdgpt-build\lite-main-venv ^
  --output-dir D:\tdgpt-build\dist ^
  --version 1.0.4
```

脚本会创建全新 venv、执行 `pip check`、校验关键模块导入，并写入 `requirements.lock` 和 `manifest.json`。ZIP 根路径固定为 `venvs/venv/`。打包前删除缓存、依赖库 test/tests 目录及普通文档/man；保留 .pyi、许可证、 Include/include 下的头文件、静态库和 pip/setuptools/wheel/Cython。清理后执行 pip check 和运行库导入验证，最终再清缓存后生成 ZIP。工作目录必须为空，防止历史 venv 混入发布包。

Windows Lite EXE 使用 `win_release.py` 构建，构建机需要 Python 3.10、3.11 或 3.12 以及 Inno Setup 6。安装包不内置 runtime、主 venv 或模型资源；这些资源通过独立归档导入。静默安装时可使用 `/OFFLINE=<venv-zip>` 和 `/PYTHON_RUNTIME_DIR=<runtime-dir>`，安装目录参数 `/DIR=<path>` 仅首次安装有效。

## 4. 共享 TSFM runtime 和模型资源

### 4.1 构建共享 TSFM runtime

Linux x64、ARM64 和 Windows x64 均要求 `uv >= 0.13.9`，通过 `--uv` 指定 TSFM builder 使用的可执行文件。uv 不读取 `PIP_INDEX_URL` 或 pip.conf；使用内部源时须显式设置 `UV_DEFAULT_INDEX`（旧变量名 `UV_INDEX_URL` 仍兼容），必要的附加源使用 `UV_INDEX`。源中必须具备目标 Python 和平台所需的 wheel；请勿把含凭据的源地址写入构建日志或版本库。

共享 TSFM runtime 由 `../offline-pkgs/build_tsfm_runtime.py` 构建。它是官方 Moirai、Moment、Chronos、TimesFM、Time-MoE 和 TDTSFM 共用的模型 venv；同一平台只需导入一次。

MOMENT 使用 `--no-deps` 单独安装，避免上游 `momentfm==0.1.4` 将共享依赖降级。因此，当前组合执行 `pip check` 会报告三项依赖声明冲突：MOMENT 要求 `numpy==1.25.2`、`transformers==4.33.3`、`huggingface-hub==0.24.0`，实际共享环境分别使用 `1.26.4`、`4.40.1`、`0.31.4`。这是明确保留的版本组合，不代表 `pip check` 全部通过，也不能忽略其他依赖错误。共享版本变化后须加载真实 MOMENT 权重并验证 `/imputation` 请求，不能仅检查模块导入。

2026-11-19 在 Linux x64 的 Python 3.12.16 TSFM 测试归档解压环境中，使用只读本地权重、禁用网络，完成 MOMENT 模型加载及 Flask 测试客户端的 `/imputation` 调用：32 个时间点中移除 3 个观测值，接口返回 HTTP 200，输出长度为 32、数值全部有限、缺失标记为 3，原有观测值保持不变。该结果验证了上述依赖组合下的单窗口补值路径；不代表 Windows、ARM64、长序列或补值精度已经通过本轮验收。

Linux x64 示例：

```bash
python3 ../offline-pkgs/build_tsfm_runtime.py \
  --runtime-dir /data/chr/python-runtime \
  --python-version 3.12.16 \
  --platform linux \
  --arch x64 \
  --version 1.0.1 \
  --work-dir /data/chr/tsfm-runtime-build \
  --output-dir /data/chr/dist
```

Linux ARM64 使用 `--arch arm64`，Windows x64 使用：

```powershell
python ../offline-pkgs/build_tsfm_runtime.py `
  --runtime-dir D:\tdgpt-build\python-runtime `
  --python-version 3.12.16 `
  --platform windows `
  --arch x64 `
  --version 1.0.1 `
  --work-dir D:\tdgpt-build\tsfm-runtime `
  --output-dir D:\tdgpt-build\dist
```

脚本默认读取 `requirements_tsfm.txt`，也可以通过 `--requirements` 指定文件。输出文件分别为：

```text
tdengine-tdgpt-tsfm-runtime-1.0.1-linux-x64.tar.xz
tdengine-tdgpt-tsfm-runtime-1.0.1-linux-arm64.tar.xz
tdengine-tdgpt-tsfm-runtime-1.0.1-windows-x64.zip
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

本轮发布清单版本为 `1.0.1`。先根据三个平台实际验收的 runtime、复用的模型 ZIP 和各自 SHA256 生成不可变发布清单，再执行上传；构建 runtime 本身不会生成聚合发布清单。仓库中的 `packaging/tdengine-tdgpt-tsfm-release-1.0.0.json` 是历史清单，不能用它发布本轮资源，也不能只改 release 字段后保留旧 runtime 文件名和 SHA256。以下路径应替换为发布工作目录中已核对的清单文件：

```bash
python3 packaging/offline-pkgs/upload_tsfm_release.py \
  --release-manifest /path/to/release/tdengine-tdgpt-tsfm-release-1.0.1.json \
  --dry-run
```

确认 dry-run 的所有资源路径、大小及 SHA256 后，移除 `--dry-run` 执行上传。清单尚未生成、上传或公网下载校验未通过时，不得发布默认使用 `1.0.1` 的产品代码。

上传脚本按清单批量提交 runtime、模型 ZIP 和 release manifest，并轮询异步任务至成功或失败。发布前应先使用 `--dry-run` 检查目标文件名和容器。

## 5. ARM64 兼容性

ARM64 Moirai 使用的 `jaxlib/libjax_common.so` 可能把 `clock_gettime@GLIBC_2.17` 绑定到 `librt.so.1`，但 Kylin V10 的 `librt` 不导出该符号。构建脚本会将依赖修复为 `libc.so.6`。

`patchelf 0.9` 只能修改 `DT_NEEDED`，无法正确更新该 ELF 符号版本引用，因此必须使用 `patchelf >= 0.14.0`。发布通用 `linux-arm64` 资源前，需要分别在 Kylin V10 ARM64 和 Ubuntu ARM64 上验证 JAX 导入及 Moirai 启动。

## 6. 自动化建议

Linux x64、Linux ARM64 和 Windows x64 应分别使用原生构建节点。构建任务输入应包括 TDgpt Git ref、资源版本、安装包版本、架构和可选的软件源地址；输出应统一包含发布物及 SHA256 清单。模型资源通过独立的 TSFM 资源发布流程上传。

## 7. 下一版发布准备与版本决策（待执行）

本轮按实际产品发布登记确定产品版本，主 venv 构建入口默认版本为 `1.0.4`，共享 TSFM runtime 候选版本为 `1.0.1`。本节是发布评审与验收说明，不是已发布制品清单。本轮 Python 3.12.16 升级需重建 Lite 主 venv 和共享 TSFM runtime；已有模型权重可保留，已安装的 TSFM 环境须显式升级。用户算法持久化方案见 [设计文档第 6.1 节](TDGPT-LITE-DESIGN-CN.md#61-用户-python-算法持久化)，产品及已有模型的升级顺序见 [第 7.2.3 节](TDGPT-LITE-DESIGN-CN.md#723-下一版产品与已有动态资源的升级顺序发布方案)。版本号须在发版登记时核对未占用；不能覆盖已有同名制品。

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
| TDgpt 程序 / Docker 产品镜像 | 产品 `3.4.3.3` 历史验收记录 | 以实际发布登记为准 | 是 |
| Lite 主 venv | 已发布 `1.0.3` | 本次构建默认 `1.0.4`，正式发布前确认未占用 | 安装器显式提供新包；Docker 由新镜像提供 |
| 基础 Python runtime | `3.12.16` | Lite、Full 和 TSFM venv 均基于该版本重建 | Python patch 版本变化时不能复用旧 runtime |
| 共享 TSFM runtime | release `1.0.0` 对应的 runtime | 本轮候选 `1.0.1` | 否，使用资源命令显式替换 |
| TSFM release manifest | `1.0.0` | 本轮候选 `1.0.1` | 否，显式指定新清单 |
| 官方模型 ZIP | 六个已发布权重包 | 内容未变则复用原文件、模型版本和 SHA-256 | 否，无须重复下载权重 |

候选号不是已查询确认的发布库存。主 venv 版本与 TSFM runtime 版本独立，均不使用产品版本号代替。构建时显式传入主 venv 的 `--venv-version`（Linux）或 `--version`（Windows），TSFM builder 的 `--version`，不能沿用脚本默认的旧版本后上传覆盖。新清单记录每个平台 runtime 的真实文件名、大小、摘要以及模型的运行时要求；即使只是压缩格式变更也要发新资源版本。

发版须同步 platform-group 发版配置、下载条目和 Apex 组件依赖清单中的版本与 SHA-256，并以新主 venv 验证新程序。本轮 Lite 基础镜像使用 Python `3.12.16`，`DockerfileTDgpt` 的 `TDGPT_LITE_ENV_VERSION` 默认值为 `1.2`，Full 的 `TDGPT_ENV_VERSION` 保留 `1.1`。requirements 更新不会自动更新已存在的基础镜像。发版须先构建并发布两个架构的不可变 Lite 基础镜像 `tdengine/tdgpt-lite-env-amd64:1.2`、`tdengine/tdgpt-lite-env-arm64:1.2`，产品构建显式传入 `--build-arg TDGPT_LITE_ENV_VERSION=1.2`，并验证镜像内 Python 精确版本；不得覆盖历史 `1.1` 标签。Full 与 Lite、x64 与 ARM64 分别验证，Full 不在本轮范围。 TSFM 资源不塞进 Lite 主 venv 或 Lite 镜像；Full 预置环境按其现有构建链路另行验证。

### 7.3 pyarrow 的两个环境与 Linux 支持基线

Lite 主环境保留 `pyarrow<19`，不删除此依赖，也不因 TSFM 升级而统一到 23。StatsForecast 的基础依赖链包含 `fugue`、`triad`，后者依赖 pyarrow；最终解析版本须以锁文件为准。此版本限制不等于已证明旧版本没有安全问题；如扫描仍报漏洞，需按实际受影响版本独立评估和处理。

TSFM 独立环境使用 `pyarrow==23.0.1`。当前 builder 先安装 18.1.0 wheel，再根据 `requirements_tsfm.txt` 升级到 23.0.1；显式 `--pyarrow-wheel` 也只是预装输入，最终版本仍由 requirements 决定。不得将预装 wheel 版本当作最终版本，发版前须检查 `pyarrow.__version__`、锁文件和实际导入结果。

[PyPI pyarrow 23.0.1](https://pypi.org/project/pyarrow/23.0.1/#files) 的 CPython 3.12 官方 Linux x64、ARM64 glibc wheel 标签为 `manylinux_2_28`，Windows x64 有对应 wheel。本轮及后续 Linux Lite、TSFM 统一要求 glibc ≥ 2.28，不再为 glibc 2.27 构建兼容 PyArrow，也不将其列为发版阻塞项。部署前须核对目标系统的 glibc 版本；最终资源须在真实 glibc 2.28 环境执行解释器、依赖导入、算法/模型推理及服务验收。历史 glibc 2.27 测试仅作为历史证据保留，不作为当前支持承诺。

### 7.4 发版验收与交接

每个平台的发布记录须包含程序 SHA、构建输入与镜像 digest、Python 版本、完整依赖锁、资源版本、归档大小和 SHA-256、最低系统要求、验收报告及回滚介质位置。本方案不填写未构建资源的摘要，也不声明已通过三平台运行验证。

本轮同时覆盖 Lite 主 venv `1.0.4` 和共享 TSFM runtime `1.0.1`。以下清单用于逐项确认交付状态，不代表所有条目已完成；已验证结果须绑定实际提交、资源版本和 SHA-256，未完成的验收不能写成通过。只有对应依赖修复和运行验证完成后，才能声明相关安全问题已解决。

交接清单：

1. 实现设计第 6.1 节的加载器与安装迁移，完成该节的跨平台验收；同步用户手册的自定义算法说明和 Docker Compose 示例，不修改 CI 运维日志来充当产品文档。
2. 在原生 Linux x64、Linux ARM64、Windows x64 构建新主 venv，检查依赖一致性和基本算法；在支持的最低 Linux 系统与 Windows 上验证新安装和从上一版升级。
3. 按第 7.3 节的 glibc 2.28 支持基线构建三平台 TSFM runtime；核对依赖及安全扫描，在最低支持系统加载六个官方模型，执行 readiness 和实际预测/补值请求。
4. 补充 runtime 版本要求校验，验证旧模型权重与新 runtime 配对；验证单模型 `apply` 不会隐式更换 runtime、相同权重摘要不会重下、切换 release 会更新状态，以及离线导入、运行中拒绝替换、失败后的显式回滚。
5. 用上一版的真实安装与数据走完设计第 7.2.3 节的升级/回滚流程；覆盖未安装 TSFM、已安装但未运行、多个模型运行、用户算法附加依赖及动态序列化模型。产品新装不会自动安装 TSFM，旧模型资源也不能被安装器清空。
6. 验收完成后发布制品与不可变清单，更新正式配置和用户升级说明，最后切换消费方。本轮代码已将 CLI、HTTP API 及下载事件默认 release 切换为 `1.0.1`；发布产品前必须先核验该清单及其引用资源可下载，不能先发布客户端再补资源。显式指定旧 release 继续用于回滚；第 7.2.1 节规划中的 stable 自动发现仍未实现。
