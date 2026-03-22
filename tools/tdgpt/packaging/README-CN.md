# 2. TDGPT Windows 打包脚本

> 📖 **语言**: [中文](README-CN.md) | [English](README.md)

这个目录包含 TDGPT (TDengine Analytics Node) 的 Windows 打包脚本，使用 Inno Setup 创建安装程序。

## 文件说明

```
packaging/
├── win_release.py           # Python 打包脚本（主入口）
└── README.md                # 本文档

script/
├── taosanode_service.py     # 统一的服务管理脚本（跨平台）
└── ...                      # 其他脚本
```

## 统一服务管理脚本

使用 **Python 统一脚本** `taosanode_service.py` 替代原有的 shell/bat 脚本，一套代码跨平台（Linux/Windows）。

### 功能

| 命令 | 说明 |
|------|------|
| `start` | 启动 taosanode 主服务 |
| `stop` | 停止 taosanode 主服务 |
| `status` | 查看服务状态 |
| `model-start [name]` | 启动模型服务（name: tdtsfm, timemoe, chronos, moirai, moment, timesfm, all） |
| `model-stop [name]` | 停止模型服务 |
| `model-status` | 查看模型服务状态 |

### 使用示例

```bash
# 主服务
python taosanode_service.py start
python taosanode_service.py stop
python taosanode_service.py status

# 模型服务
python taosanode_service.py model-start tdtsfm
python taosanode_service.py model-start all
python taosanode_service.py model-stop all
python taosanode_service.py model-status
```

### Windows 批处理包装

为了方便 Windows 用户使用，提供了批处理包装：

```
bin/
├── taosanode_service.py    # 核心 Python 脚本
├── start-taosanode.bat     # 包装: python taosanode_service.py start
├── stop-taosanode.bat      # 包装: python taosanode_service.py stop
├── status-taosanode.bat    # 包装: python taosanode_service.py status
├── start-model.bat         # 包装: python taosanode_service.py model-start
├── stop-model.bat          # 包装: python taosanode_service.py model-stop
└── status-model.bat        # 包装: python taosanode_service.py model-status
```

## 前置要求

1. **Python 3.10 / 3.11 / 3.12** - 需要安装 Python 并添加到 PATH
2. **Microsoft Visual C++ Redistributable x64** - TensorFlow、PyTorch 以及其他原生 Python 依赖所必需
   - 下载地址: https://aka.ms/vc14/vc_redist.x64.exe
3. **Inno Setup 6** - 用于创建安装程序
   - 下载地址: https://jrsoftware.org/isdl.php
   - 安装后确保 `ISCC.exe` 在 PATH 中或在脚本中指定路径

## 使用方法

### 基础用法

```bash
# Community 版本（生产打包，需要模型文件）
python packaging/win_release.py -e community -v 3.4.0.11.0316 -m D:\workspace\models

# Enterprise 版本
python packaging/win_release.py -e enterprise -v 3.4.0.11.0316 -m D:\workspace\models

# 包含所有模型
python packaging/win_release.py -e community -v 3.4.0.11.0316 -m D:\workspace\models -a

# 自定义输出目录
python packaging/win_release.py -e community -v 3.4.0.11.0316 -m D:\workspace\models -o D:\workspace\main\release

# 测试模式（快速验证打包流程，无需模型文件）
python packaging/win_release.py -e community -v 3.4.0.11.0316 --skip-model-check
```

### 模型文件要求

**生产打包（默认）：**
- 必须使用 `-m` 参数指定模型目录
- 模型目录必须包含以下文件：
  - `timemoe.tar.gz` （必需）
  - `tdtsfm.tar.gz` （必需）
- 缺少任何必需文件将导致打包失败

**测试模式（`--skip-model-check`）：**
- 跳过模型验证，无需模型文件
- 仅用于快速测试打包流程
- ⚠️ **不适用于生产环境**

## 打包脚本参数说明

| 参数 | 简写 | 说明 | 必填 |
|------|------|------|------|
| `--edition` | `-e` | 版本类型: enterprise 或 community | 是 |
| `--version` | `-v` | 版本号 (如 3.4.0.11.0316) | 是 |
| `--model-dir` | `-m` | 模型文件目录（生产必需） | 生产必需 |
| `--all-models` | `-a` | 打包所有模型 | 否 |
| `--output` | `-o` | 输出目录 (默认: D:\tdgpt-release) | 否 |
| `--iscc-path` | | Inno Setup 编译器路径 | 否 |
| `--skip-model-check` | | 跳过模型验证（仅测试用） | 否 |

## 安装程序特性

- **默认安装路径**: `C:\TDengine\taosanode`
- **服务管理**: 使用统一的 Python 脚本 `taosanode_service.py`
- **虚拟环境**: 自动创建 Python 虚拟环境
- **环境变量**: 自动添加 `bin` 目录到 PATH
- **日志目录**: `C:\TDengine\taosanode\log`
- **配置保护**: 升级时保留现有配置文件

## 服务管理

安装完成后，可以使用以下命令管理服务：

```bash
# 启动/停止/查看状态
C:\TDengine\taosanode\bin\start-taosanode.bat
C:\TDengine\taosanode\bin\stop-taosanode.bat
C:\TDengine\taosanode\bin\status-taosanode.bat

# 模型服务
C:\TDengine\taosanode\bin\start-model.bat tdtsfm
C:\TDengine\taosanode\bin\start-model.bat
C:\TDengine\taosanode\bin\start-model.bat all
C:\TDengine\taosanode\bin\stop-model.bat all
C:\TDengine\taosanode\bin\status-model.bat
```

或者直接使用 Python 脚本：

```bash
cd C:\TDengine\taosanode
python bin\taosanode_service.py start
python bin\taosanode_service.py model-start all
```

## 与 Linux 打包的差异

| 特性 | Linux | Windows |
|------|-------|---------|
| 服务管理 | Python 统一脚本 | Python 统一脚本 + bat 包装 |
| WSGI 服务器 | gunicorn | waitress |
| 默认路径 | /usr/local/taos/taosanode | C:\TDengine\taosanode |
| 配置文件 | taosanode.config.py | taosanode.config.py |
| 进程管理 | signal / ps | taskkill |

## 注意事项

1. **Python 版本**: 推荐使用 Python 3.9 或更高版本
2. **配置文件**: `taosanode.config.py` 已内置 Windows 路径支持，通过 `on_windows` 变量自动切换
3. **防火墙**: 服务默认使用 6035 端口，需要配置防火墙
4. **依赖安装**: 首次启动时会自动安装 Python 依赖，可能需要联网

## 故障排除

### 服务无法启动

1. 检查 Python 是否正确安装并添加到 PATH
2. 检查日志文件：`C:\TDengine\taosanode\log\taosanode_service_*.log`
3. 手动运行启动脚本查看错误：
   ```bash
   cd C:\TDengine\taosanode
   python bin\taosanode_service.py start
   ```

### 依赖安装失败

1. 确保可以访问 PyPI
2. 手动安装依赖：
   ```bash
   cd C:\TDengine\taosanode
   python -m venv venv
   venv\Scripts\activate.bat
   pip install -r requirements_ess.txt
   ```

### 端口被占用

修改配置文件 `C:\TDengine\taosanode\cfg\taosanode.config.py` 中的 `bind` 设置。

## 参考

- [Inno Setup 文档](https://jrsoftware.org/ishelp/)
- [TDGPT Linux 打包脚本](../script/release.sh)
- [TDGPT Linux 安装脚本](../script/install.sh)
- [TDengine Windows 打包流程](../../../../enterprise/packaging/new_win_release.py)

## 2026-03-17 补充说明

- 安装向导在在线安装模式下新增了 pip 源选择页。
- 默认使用官方 PyPI，不会默认勾选国内镜像。
- 可选预置镜像包括清华镜像和阿里云镜像，也支持手工填写自定义镜像 URL。
- 标准卸载会保留 `cfg`、`model`、`data`、`venv` 目录。
- 只有显式使用 `--remove-model` 时，才会删除 `model` 目录。

## 2026-03-17 目录与安装向导补充

- `requirements*.txt` 现在统一打到 `<安装目录>\requirements\`。
- 所有 Windows venv 现在统一放到 `<安装目录>\venvs\` 下面。
- 安装向导最后阶段新增“安装并注册 Windows 服务”选项，默认勾选。
- 安装完成页会提示以下启动/停止方式：
  - `net start Taosanode`
  - `net stop Taosanode`
  - `<安装目录>\bin\start-taosanode.bat`
  - `<安装目录>\bin\stop-taosanode.bat`
- 非静默安装完成后会自动打开 `<安装目录>\log\install.log`。

### 日志文件说明

- `<安装目录>\log\install.log`：安装全过程输出和安装摘要。
- `<安装目录>\log\uninstall.log`：卸载全过程输出和卸载摘要。
- `<安装目录>\log\taosanode-service.log`：服务注册、服务管理、启停生命周期日志。
- `<安装目录>\log\taosanode-service.wrapper.log`：WinSW wrapper 生命周期日志。
- `<安装目录>\log\taosanode-service.out.log`：Windows 服务 stdout。
- `<安装目录>\log\taosanode-service.err.log`：Windows 服务 stderr。
- `<安装目录>\log\taosanode.app.log`：taosanode 应用日志。

### 标准卸载策略

- 标准卸载默认保留 `cfg`、`data`、`model`、`venvs`、`log`。
- 只有显式执行 `uninstall.py --remove-model` 时，才会删除 `model`。
