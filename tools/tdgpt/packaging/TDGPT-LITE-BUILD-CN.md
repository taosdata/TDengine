# TDgpt Lite 三平台离线资源构建

本文说明 TDgpt Lite 在 Linux x64、Linux ARM64 和 Windows x64 上使用的
Python runtime 与主 venv 构建流程。各架构必须在对应的原生节点构建，不支持
交叉构建。

模型资源、共享 TSFM runtime 和 release manifest 的协议见
[`TDGPT-LITE-DESIGN-CN.md`](TDGPT-LITE-DESIGN-CN.md)。

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

安装包、Python runtime、主 venv、共享 TSFM runtime、模型 ZIP 和 release manifest
是相互独立的发布物。`packaging/` 负责构建和验收，`../offline-pkgs/` 负责模型
资源拆包与 runtime 构建；上传由 `packaging/offline-pkgs/upload_tsfm_release.py`
负责。

## 1. 发布物

Linux Python runtime：

```text
tdengine-python-runtime-3.12.12-linux-<x64|arm64>.tar.gz
```

Linux Lite 主 venv：

```text
tdengine-tdgpt-lite-venv-<version>-linux-<x64|arm64>.tar.gz
```

Windows Lite 主 venv：

```text
tdengine-tdgpt-lite-venv-<version>-windows-x64.zip
```

Linux runtime 包以 `bin/`、`lib/` 等目录为归档根目录。Apex 默认将其解压至
`/usr/local/taos-deps/python-runtime`。Linux 和 Windows 主 venv 均根据
`requirements_docker.txt` 构建。

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

在 x64 节点使用 `--arch x64`。脚本从 `uv` 管理的 CPython 发行包构建 runtime，
携带非 glibc 运行库并设置相对 RPATH。构建结束前会扫描所有 ELF 文件，存在未解析
依赖时直接失败。

部分 ARM64 CPython 发行包缺少 `_sqlite3`。出现这种情况时，脚本会使用构建节点的
SQLite 开发包编译该扩展，并在归档前验证 `import sqlite3`。

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

构建的 venv 默认引用 `/usr/local/taos-deps/python-runtime`。安装时，
`install.sh --python-home` 会修复迁移后的 venv 引用和
`taosanode_service.py` 入口。

### 2.4 一次构建 Linux 发布物

```bash
bash packaging/build_linux_offline_release.sh \
  --lite \
  --version 3.4.2.0.0713 \
  --edition enterprise \
  --arch arm64 \
  --work-dir /data/chr/tdgpt-release-build \
  --output-dir /data/chr/dist
```

该入口同时生成发布物和 `SHA256SUMS`。安装包与模型资源保持独立，模型不得嵌入
Lite 安装包。

## 3. Windows x64 构建

在 Windows x64 节点准备已解压的 Python 3.12 runtime，然后执行：

```bat
cd source\taos-community\tools\tdgpt

python packaging\build_windows_lite_venv.py ^
  --runtime-dir D:\tdgpt-build\python-runtime ^
  --work-dir D:\tdgpt-build\lite-main-venv ^
  --output-dir D:\tdgpt-build\dist ^
  --version 1.0.1
```

脚本会创建全新 venv、执行 `pip check`、校验关键模块导入，并写入
`requirements.lock` 和 `manifest.json`。ZIP 根路径固定为 `venvs/venv/`。
工作目录必须为空，防止历史 venv 混入发布包。

Windows Lite EXE 使用 `win_release.py` 构建，构建机需要 Python 3.10、3.11 或
3.12 以及 Inno Setup 6。安装包不内置 runtime、主 venv 或模型资源；这些资源
通过独立归档导入。静默安装时可使用 `/OFFLINE=<venv-zip>` 和
`/PYTHON_RUNTIME_DIR=<runtime-dir>`，安装目录参数 `/DIR=<path>` 仅首次安装有效。

## 4. 共享 TSFM runtime 和模型资源

### 4.1 构建共享 TSFM runtime

共享 TSFM runtime 由 `../offline-pkgs/build_tsfm_runtime.py` 构建。它是官方
Moirai、Moment、Chronos、TimesFM、Time-MoE 和 TDTSFM 共用的模型 venv；同一平台
只需导入一次。

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

脚本默认读取 `requirements_tsfm.txt`，也可以通过 `--requirements` 指定文件。
输出文件分别为：

```text
tdengine-tdgpt-tsfm-runtime-1.0.0-linux-x64.tar.gz
tdengine-tdgpt-tsfm-runtime-1.0.0-linux-arm64.tar.gz
tdengine-tdgpt-tsfm-runtime-1.0.0-windows-x64.zip
```

### 4.2 拆分模型资源 ZIP

模型资源不由 Lite 安装包构建入口生成。使用
`../offline-pkgs/split_model_resource.py` 将全量模型 ZIP 拆为单模型 ZIP：

```bash
python3 ../offline-pkgs/split_model_resource.py \
  --source /pkgs/TDengine/deps/tdengine-tdgpt-model-resource-1.0.0.zip \
  --output-dir /pkgs/TDengine/tsfm/1.0.0 \
  --version 1.0.0
```

脚本根据 `tsfm_model_catalog.json` 读取模型身份并生成 Hugging Face 风格的小写
文件名，例如：

```text
tdengine-tdgpt-model-moirai-moe-1-0-r-small.zip
tdengine-tdgpt-model-moment-1-base.zip
```

同时生成模型清单，记录模型 ID、版本、归档大小和 SHA256。模型 ZIP 内容保持原样，
不加入 TDgpt 私有 manifest 或完成标志。

### 4.3 上传资源

```bash
python3 packaging/offline-pkgs/upload_tsfm_release.py \
  --release-manifest packaging/tdengine-tdgpt-tsfm-release-1.0.0.json
```

上传脚本按清单批量提交 runtime、模型 ZIP 和 release manifest，并轮询异步任务至
成功或失败。发布前应先使用 `--dry-run` 检查目标文件名和容器。

## 5. ARM64 兼容性

ARM64 Moirai 使用的 `jaxlib/libjax_common.so` 可能把
`clock_gettime@GLIBC_2.17` 绑定到 `librt.so.1`，但 Kylin V10 的 `librt` 不导出
该符号。构建脚本会将依赖修复为 `libc.so.6`。

`patchelf 0.9` 只能修改 `DT_NEEDED`，无法正确更新该 ELF 符号版本引用，因此必须
使用 `patchelf >= 0.14.0`。发布通用 `linux-arm64` 资源前，需要分别在 Kylin V10
ARM64 和 Ubuntu ARM64 上验证 JAX 导入及 Moirai 启动。

## 6. 自动化建议

Linux x64、Linux ARM64 和 Windows x64 应分别使用原生构建节点。构建任务输入应
包括 TDgpt Git ref、资源版本、安装包版本、架构和可选的软件源地址；输出应统一
包含发布物及 SHA256 清单。模型资源通过独立的 TSFM 资源发布流程上传。
