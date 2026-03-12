# TDGPT Windows 打包脚本

这个目录包含 TDGPT (TDengine Analytics Node) 的 Windows 打包脚本，使用 Inno Setup 创建安装程序。

## 文件说明

```
packaging/
├── win_release.py           # Python 打包脚本（推荐）
├── windows/
│   ├── tdgpt.iss           # Inno Setup 脚本
│   └── bin/
│       ├── start-taosanode.bat      # 启动服务脚本
│       ├── stop-taosanode.bat       # 停止服务脚本
│       ├── install-service.bat      # 安装 Windows 服务
│       └── uninstall-service.bat    # 卸载 Windows 服务
└── README.md               # 本文档
```

## 前置要求

1. **Python 3.9+** - 需要安装 Python 并添加到 PATH
2. **Inno Setup 6** - 用于创建安装程序
   - 下载地址: https://jrsoftware.org/isdl.php
   - 安装后确保 `ISCC.exe` 在 PATH 中或在脚本中指定路径

## 使用方法

### 方法 1: 使用 Python 脚本（推荐）

```bash
# 基础用法 - Community 版本
python win_release.py -e community -v 3.3.6.0

# Enterprise 版本
python win_release.py -e enterprise -v 3.3.6.0

# 包含模型文件
python win_release.py -e community -v 3.3.6.0 -m D:\models

# 包含所有模型
python win_release.py -e community -v 3.3.6.0 -m D:\models -a

# 自定义输出目录
python win_release.py -e community -v 3.3.6.0 -o D:\release

# 指定 Inno Setup 路径
python win_release.py -e community -v 3.3.6.0 --iscc-path "C:\Program Files (x86)\Inno Setup 6\ISCC.exe"
```

### 方法 2: 手动使用 Inno Setup

1. 准备安装文件目录结构：
```
install/
├── cfg/                    # 配置文件 (taosanode.ini)
├── lib/                    # Python 库 (taosanalytics/)
├── resource/               # 资源文件
├── model/                  # 模型文件 (可选)
├── bin/                    # 批处理脚本
├── requirements.txt        # Python 依赖
└── requirements_ess.txt    # 精简依赖
```

2. 编译安装程序：
```bash
# 使用默认版本号
ISCC.exe windows\tdgpt.iss

# 指定版本号和安装包名称
ISCC.exe /DMyAppVersion="3.3.6.0" /DMyAppInstallName="tdengine-tdgpt-oss-3.3.6.0-Windows-x64" windows\tdgpt.iss
```

## 打包脚本参数说明

### win_release.py 参数

| 参数 | 简写 | 说明 | 必填 |
|------|------|------|------|
| `--edition` | `-e` | 版本类型: enterprise 或 community | 是 |
| `--version` | `-v` | 版本号 (如 3.3.6.0) | 是 |
| `--model-dir` | `-m` | 模型文件目录 | 否 |
| `--all-models` | `-a` | 打包所有模型 | 否 |
| `--output` | `-o` | 输出目录 (默认: D:\tdgpt-release) | 否 |
| `--iscc-path` | | Inno Setup 编译器路径 | 否 |

### Inno Setup 宏定义

| 宏 | 说明 | 默认值 |
|---|------|--------|
| MyAppVersion | 应用程序版本 | 3.3.0.0 |
| MyAppInstallName | 安装包名称 | tdengine-tdgpt-oss-3.3.0.0-Windows-x64 |

## 安装程序特性

- **默认安装路径**: `C:\TDengine\taosanode`
- **服务管理**: 自动创建 Windows 服务 `taosanode`
- **虚拟环境**: 自动创建 Python 虚拟环境
- **环境变量**: 自动添加 `bin` 目录到 PATH
- **日志目录**: `C:\TDengine\taosanode\log`
- **配置保护**: 升级时保留现有配置文件

## 服务管理

安装完成后，可以使用以下命令管理服务：

```bash
# 启动服务
sc start taosanode
net start taosanode

# 停止服务
sc stop taosanode
net stop taosanode

# 删除服务（卸载时自动执行）
sc delete taosanode
```

或者使用提供的批处理脚本：

```bash
# 启动
C:\TDengine\taosanode\bin\start-taosanode.bat

# 停止
C:\TDengine\taosanode\bin\stop-taosanode.bat

# 安装服务
C:\TDengine\taosanode\bin\install-service.bat

# 卸载服务
C:\TDengine\taosanode\bin\uninstall-service.bat
```

## 与 Linux 打包的差异

| 特性 | Linux | Windows |
|------|-------|---------|
| 服务管理 | systemd | Windows Service (sc) |
| WSGI 服务器 | uWSGI | waitress / Flask |
| 进程管理 | systemctl | taskkill / sc |
| 配置文件路径 | /usr/local/taos/taosanode | C:\TDengine\taosanode |
| 日志路径 | /var/log/taos/taosanode | C:\TDengine\taosanode\log |
| 虚拟环境 | /var/lib/taos/taosanode/venv | C:\TDengine\taosanode\venv |

## 注意事项

1. **Python 版本**: 推荐使用 Python 3.9 或更高版本
2. **管理员权限**: 安装服务需要管理员权限
3. **防火墙**: 服务默认使用 6035 端口，需要配置防火墙
4. **依赖安装**: 首次启动时会自动安装 Python 依赖，可能需要联网

## 故障排除

### 服务无法启动

1. 检查 Python 是否正确安装并添加到 PATH
2. 检查日志文件：`C:\TDengine\taosanode\log\taosanode.log`
3. 手动运行启动脚本查看错误：`C:\TDengine\taosanode\bin\start-taosanode.bat`

### 依赖安装失败

1. 确保可以访问 PyPI
2. 手动安装依赖：
```bash
cd C:\TDengine\taosanode
venv\Scripts\activate.bat
pip install -r requirements.txt
```

### 端口被占用

修改配置文件 `C:\TDengine\taosanode\cfg\taosanode.ini` 中的端口设置。

## 参考

- [Inno Setup 文档](https://jrsoftware.org/ishelp/)
- [TDGPT Linux 打包脚本](../script/release.sh)
- [TDengine Windows 打包流程](../../../../enterprise/packaging/new_win_release.py)
