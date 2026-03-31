# TDgpt Windows 打包说明

> 语言: [中文](README-CN.md) | [English](README.md)

本目录包含 TDgpt（TDengine Analytics Node）的 Windows 打包流程。主入口是 `win_release.py`，负责整理安装载荷并调用 Inno Setup 生成安装包。

## 适用范围

当前 Windows 交付只保留两条安装路径：

- 基础安装包 + 在线安装
- 基础安装包 + 外部离线 tar 包

旧的 `full-offline` 一体包模式已经删除。

## 主要文件

```text
packaging/
├── win_release.py              # Windows 打包主入口
├── installer/
│   ├── tdgpt.iss               # Inno Setup 模板
│   └── taosanode-service.xml   # WinSW 服务模板
├── bin/
│   ├── WinSW.exe               # 可选，本地缓存的 WinSW 二进制
│   ├── uv.exe                  # 可选，离线资产打包使用的 uv 二进制
│   └── README.md               # 二进制缓存说明
├── README.md
└── README-CN.md

script/
├── install.py                  # Windows 安装逻辑
├── uninstall.py                # Windows 卸载逻辑
└── taosanode_service.py        # 服务与模型统一管理脚本
```

## 打包机预置要求

开始执行打包前，建议先确认下面这些预置条件。

### 一、基础安装包打包前置条件

1. 已安装 Python `3.10`、`3.11` 或 `3.12`，并且可直接从 `PATH` 调用
2. 已安装 Inno Setup 6
3. 当前源码目录完整，至少包含以下文件：
   - `packaging/win_release.py`
   - `packaging/installer/tdgpt.iss`
   - `script/install.py`
   - `script/uninstall.py`
   - `script/taosanode_service.py`
4. 如果希望基础安装包内附带模型归档文件，需要提前准备模型归档目录，例如：
   - `tdtsfm.tar.gz`
   - `timemoe.tar.gz`
   - `moirai.tar.gz`
   - `chronos.tar.gz`
   - `timesfm.tar.gz`
   - `moment-large.tar.gz`

### 二、离线 tar 打包额外前置条件

如果还要额外生成 Windows 离线安装使用的外部 tar 包，还需要准备：

1. 一个主 venv 目录
2. 零个或多个模型 venv 目录
3. 二选一准备 Python runtime 来源：
   - 已准备好的 Python runtime 目录，目录下包含 `python.exe`
   - `packaging/bin/uv.exe`，或者通过 `--uv-exe` 显式指定一个可用的 `uv.exe`
4. 一个可选的 seed package：
   - 用于带入已有离线模型 payload
   - 也可以带入已有模型 venv payload

### 三、可选本地缓存

1. `packaging/bin/WinSW.exe`
   - 用于基础安装包打包时复用本地 WinSW 缓存
   - 如果没有，脚本会尝试从 GitHub 下载
2. `packaging/bin/uv.exe`
   - 用于离线 tar 打包时自动准备 Python runtime
   - 如果已经传入 `--python-runtime-dir`，则不依赖它

### 四、路径与工具说明

- `ISCC.exe` 默认路径为 `C:\Program Files (x86)\Inno Setup 6\ISCC.exe`
- 也可以通过 `--iscc-path` 显式指定
- `win_release.py` 不会把 Python runtime 或虚拟环境直接打进基础安装包
- 当 `build_offline_assets.py` 未显式传入 `--python-runtime-dir` 时，会优先使用 `packaging/bin/uv.exe`

## 目标机器要求

生成后的安装包在运行时有以下要求：

- Windows 10（1803 或更高版本）或 Windows Server 2019+
- 目标机器需要已安装 Microsoft Visual C++ Redistributable x64 14.44 或更高版本；建议安装最新支持的 VC++ 2015-2022 运行库
- 在线首次安装时，目标机器需要在 `PATH` 中提供 Python `3.10` / `3.11` / `3.12`
- 离线安装时，**不要求**系统自带 Python——安装器会通过系统内置的 `tar.exe`（Windows 10 1803+ 自带）从离线 tar 包中自动引导 Python 运行时

## 基础安装包里包含什么

固定包含：

- `cfg/`
- `lib/`
- `resource/`
- `requirements/`
- `bin/`
- `install.py`、`install.bat`
- `uninstall.py`、`uninstall.bat`
- WinSW 可执行文件和 XML
- package metadata

可选包含：

- 通过 `--model-dir` 复制进来的模型归档文件，落在 `<install_dir>\model\`

明确不包含：

- `python/runtime`
- 已解压的虚拟环境
- 已解压的模型目录

## 用法

### 基本命令

```bash
# Community 版本
python packaging/win_release.py -e community -v 3.4.1.0.0325

# Enterprise 版本
python packaging/win_release.py -e enterprise -v 3.4.1.0.0325

# 打包时附带模型归档文件
python packaging/win_release.py -e community -v 3.4.1.0.0325 -m D:\models

# 打包目录中的全部已识别模型归档
python packaging/win_release.py -e community -v 3.4.1.0.0325 -m D:\models -a

# 自定义输出目录
python packaging/win_release.py -e community -v 3.4.1.0.0325 -o D:\tdgpt-release\20260325-r7
```

### 生成外部离线 tar 包

Windows 离线安装所用的外部 tar 包，由 `build_offline_assets.py` 生成。

需要准备的输入：

- 一个主 venv 目录
- 零个或多个模型 venv 目录
- 一个 Python runtime 目录，或者提供 `uv.exe` 让脚本自动准备
- 一个可选的 seed package，用来带入离线模型 payload

典型命令如下：

```bash
python packaging/build_offline_assets.py ^
   --output-file D:\offline-tar\tdengine-tdgpt-offline-assets-3.4.1.0.0325-windows-x64.tar ^
  --seed-package D:\offline-seed\tdgpt-model-seed.tar ^
  --python-runtime-dir C:\TDengine\python311 ^
  --main-venv-dir C:\TDengine\taosanode\venvs\venv ^
  --extra-venv-dir C:\TDengine\taosanode\venvs\moirai_venv ^
  --extra-venv-dir C:\TDengine\taosanode\venvs\chronos_venv ^
  --extra-venv-dir C:\TDengine\taosanode\venvs\timesfm_venv ^
  --extra-venv-dir C:\TDengine\taosanode\venvs\momentfm_venv
```

如果不想提前准备 Python runtime 目录，也可以让脚本通过 `uv.exe` 自动准备：

```bash
python packaging/build_offline_assets.py ^
   --output-file D:\offline-tar\tdengine-tdgpt-offline-assets-3.4.1.0.0325-windows-x64.tar ^
  --main-venv-dir C:\TDengine\taosanode\venvs\venv ^
  --extra-venv-dir C:\TDengine\taosanode\venvs\moirai_venv ^
  --uv-exe packaging\bin\uv.exe ^
  --python-version 3.11
```

建议离线资源包命名为：

```text
tdengine-tdgpt-offline-assets-<version>-windows-x64.tar
```

生成的 tar 包中通常包含：

- `python/runtime/`
- `venvs/venv/`
- `venvs/<extra_venv>/`
- 从 `--seed-package` 带入的模型 payload
- `offline-assets-manifest.txt`

推荐流程：

1. 先用 `win_release.py` 生成基础安装包
2. 再用 `build_offline_assets.py` 生成外部离线 tar 包
3. 两个文件一起交付
4. 用户在 Windows 安装向导里选择 `Offline package`，并选中这个 tar 文件

### 参数说明

| 参数 | 简写 | 说明 |
| --- | --- | --- |
| `--edition` | `-e` | `community` 或 `enterprise` |
| `--version` | `-v` | 安装包版本号，例如 `3.4.1.0.0325` |
| `--model-dir` | `-m` | 可选，指定一个模型归档目录，把归档文件复制进安装包 |
| `--all-models` | `-a` | 配合 `--model-dir` 使用，复制全部已识别模型归档 |
| `--output` | `-o` | 输出目录，默认 `D:\tdgpt-release` |
| `--iscc-path` |  | 自定义 Inno Setup 编译器路径 |
| `--skip-model-check` |  | 兼容保留参数；当前基础安装包已不再强制做模型归档校验 |

### 离线 tar 参数说明

`build_offline_assets.py` 的主要参数如下：

| 参数 | 说明 |
| --- | --- |
| `--output-file` | 输出 tar 文件路径 |
| `--seed-package` | 可选，已有离线模型 payload 的种子 tar，也可以带可选模型 venv payload |
| `--python-runtime-dir` | 已准备好的 Python runtime 目录，目录下需包含 `python.exe` |
| `--main-venv-dir` | 主 taosanode venv，打包后落为 `venvs/venv` |
| `--extra-venv-dir` | 额外模型 venv 目录，可重复传入 |
| `--uv-exe` | 当未传 `--python-runtime-dir` 时，用于自动准备 Python 的 `uv.exe` 路径 |
| `--python-version` | 通过 `uv` 准备的 Python 版本，默认 `3.11` |

### 模型归档说明

如果传入 `--model-dir`，`win_release.py` 会识别并复制这类归档文件：

- `tdtsfm.tar.gz`
- `timemoe.tar.gz`
- `moirai.tar.gz`
- `chronos.tar.gz`
- `timesfm.tar.gz`
- `moment-large.tar.gz`

这些归档文件都是可选的。即使不附带任何模型归档，也可以正常生成基础安装包。

## 输出产物

生成的安装包文件名为：

```text
tdengine-tdgpt-oss-<version>-windows-x64.exe
tdengine-tdgpt-enterprise-<version>-windows-x64.exe
```

## 静默安装与命令行安装

安装器支持完全静默（无人值守）安装，适用于脚本化部署和 All-in-One 打包场景。

### 静默离线安装（不需要系统 Python）

```bat
tdgpt-setup.exe /VERYSILENT /NORESTART /OFFLINE="D:\packages\tdengine-tdgpt-offline-assets-3.4.1.0.0325-windows-x64.tar"
```

| 参数 | 说明 |
| --- | --- |
| `/VERYSILENT` | 完全静默，不显示任何界面 |
| `/SILENT` | 静默安装，仅显示进度条 |
| `/NORESTART` | 安装后不自动重启 |
| `/OFFLINE="路径"` | 指定外部离线 tar 包路径 |
| `/DIR="路径"` | 覆盖安装目录（仅首次安装有效） |

传入 `/OFFLINE` 后，安装器自动使用离线模式。Python 运行时通过系统 `tar.exe` 从 tar 包中引导，无需系统自带 Python。

### 静默在线安装（需要系统 Python）

```bat
tdgpt-setup.exe /VERYSILENT /NORESTART /ONLINE=1
```

静默安装的默认模式是离线模式。若要强制在线模式，请传入 `/ONLINE=1`。对于首次在线安装，目标机仍需在 `PATH` 中提供 Python 3.10–3.12；如果针对所选安装路径没有可用 Python 运行时，安装日志会记录明确的错误信息。

### Python 发现优先级

安装过程中，`install.bat` 按以下顺序查找 Python 解释器：

1. **已有 venv Python** — `<install_dir>\venvs\venv\Scripts\python.exe`（升级场景）
2. **已有 packaged runtime** — `<install_dir>\python\runtime\python.exe`（之前已解压过）
3. **系统 Python** — PATH 中的 `python` 或 `python3`（在线模式）
4. **从离线 tar 引导** — 使用 `tar.exe` 从离线包中只抽取 `python/runtime/` 到临时目录 `_bootstrap`，然后用该 Python 执行 `install.py`

`_bootstrap` 临时目录在安装完成后自动清理。

### All-in-One (AIO) 集成示例

```bat
REM install-all.bat AIO 打包示例
installers\tdgpt-setup.exe /VERYSILENT /NORESTART /OFFLINE="%CD%\offline-packages\tdengine-tdgpt-offline-assets-3.4.1.0.0325-windows-x64.tar"
if errorlevel 1 (
    echo TDgpt 安装失败
    exit /b 1
)
echo TDgpt 安装成功
```

### 检查安装结果

静默安装完成后，可通过以下方式确认：

- 退出码：`0` = 成功，非零 = 失败
- 安装日志：`C:\TDengine\taosanode\log\install.log`
- 服务状态：`sc query Taosanode`

## 安装器行为

当前安装向导行为如下：

- 默认推荐安装来源为 `Offline package`
- 选择离线模式后，如果安装器同目录下存在 `tdengine-tdgpt-offline-assets-<version>-windows-x64.tar`，安装向导会自动带出该路径，用户也可以改选其他 tar 包
- 仍然保留在线安装路径
- Windows 服务固定自动安装，不再作为可选项展示
- 升级安装默认复用现有 `venvs` 和模型文件
- 离线首次安装必须提供一个外部 tar 包
- 离线升级时可以把 tar 路径留空，直接复用现有 runtime 和模型文件
- 支持 `/OFFLINE` 命令行参数，用于静默安装时预设离线包路径

当前批处理包装脚本行为如下：

- `start-taosanode.bat`、`stop-taosanode.bat`、`status-taosanode.bat` 都要求 `<install_dir>\venvs\venv\Scripts\python.exe` 存在
- `start-model.bat`、`stop-model.bat`、`status-model.bat` 同样固定依赖这个主 venv Python
- 这些脚本不会回退到系统 `python`
- `start-model.bat` 在不带参数时默认按 `all` 处理

## 服务与模型命令

安装完成后可使用：

```bat
net start Taosanode
net stop Taosanode
sc query Taosanode

C:\TDengine\taosanode\bin\start-taosanode.bat
C:\TDengine\taosanode\bin\stop-taosanode.bat
C:\TDengine\taosanode\bin\status-taosanode.bat

C:\TDengine\taosanode\bin\start-model.bat
C:\TDengine\taosanode\bin\start-model.bat all
C:\TDengine\taosanode\bin\stop-model.bat all
C:\TDengine\taosanode\bin\status-model.bat
```

如果要直接调用 Python 脚本，应使用主 venv 的 Python：

```bat
C:\TDengine\taosanode\venvs\venv\Scripts\python.exe C:\TDengine\taosanode\bin\taosanode_service.py start
C:\TDengine\taosanode\venvs\venv\Scripts\python.exe C:\TDengine\taosanode\bin\taosanode_service.py model-start all
```

## 日志

当前 Windows 侧关键日志文件包括：

- `<install_dir>\log\install.log`
- `<install_dir>\log\uninstall.log`
- `<install_dir>\log\install-progress.log`
- `<install_dir>\log\taosanode-service.log`
- `<install_dir>\log\taosanode.app.log`
- `<install_dir>\log\model_*.log`

说明：

- 服务管理日志是 `taosanode-service.log`
- `taosanode-service.wrapper.log`、`*.out.log`、`*.err.log` 这类 WinSW wrapper 日志，不属于当前默认日志链路

## 常见问题

### 打包在调用 ISCC 之前失败

- 检查打包机 Python 版本
- 检查版本号格式是否合法
- 检查输出目录是否可被删除并重建

### 找不到 ISCC

- 安装 Inno Setup 6
- 如果 `ISCC.exe` 不在默认路径下，显式传入 `--iscc-path`

### 批处理脚本提示主 Python 环境缺失

- 检查 `<install_dir>\venvs\venv\Scripts\python.exe` 是否存在
- 如果是离线安装，确认离线 tar 已成功导入，必要时重新执行安装
- 如果是在线安装，可重新执行安装以重建主 venv

### 启动命令返回了，但就绪检测未确认成功

- 检查 `<install_dir>\log\taosanode-service.log`
- 检查 `<install_dir>\log\taosanode.app.log`
- 执行 `status-taosanode.bat`
- 执行 `sc query Taosanode`

## 本次审查结论

本 README 已按当前代码实现重新对齐，核对范围包括：

- `packaging/win_release.py`
- `packaging/installer/tdgpt.iss`
- `script/install.py`
- `script/taosanode_service.py`

本次修正的重点有：

- 删除已经下线的 `full-offline` 相关表述
- 明确基础安装包不内置 runtime 和 venv
- 明确离线安装不依赖系统 Python
- 明确批处理脚本固定依赖主 venv Python
- 修正日志文件名，去掉过时的 WinSW wrapper 日志说明
- 明确 Windows 服务是自动安装
- 明确模型归档打包是可选能力，不再是必填
- 新增静默安装文档，包括 `/OFFLINE` 参数说明
- 新增 Python 发现优先级文档
- 新增 AIO 集成示例
- 新增 Windows 10 1803+ 的 `tar.exe` 引导要求说明
