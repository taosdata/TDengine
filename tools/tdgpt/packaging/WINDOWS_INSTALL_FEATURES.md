# TDGPT Windows 安装脚本功能列表

## 📋 项目概述

为 TDGPT 项目创建 Windows 安装脚本，实现与 Linux `install.sh` 功能对等的自动化安装流程。

**实现日期：** 2026-03-13
**参考项目：** IDMP Windows 安装方案
**核心原则：** 与 Linux 行为保持一致

---

## 🏗️ 架构设计

### 三层安装架构

```
用户双击 .exe 安装程序
    ↓
【第一层】Inno Setup (tdgpt.iss)
    - GUI 安装向导
    - 文件复制到 C:\TDengine\taosanode
    - 创建快捷方式
    - 注册表配置（PATH）
    ↓
【第二层】install.bat
    - 检查 Python 环境
    - 调用 install.py
    - 传递安装参数（-o, -a）
    ↓
【第三层】install.py
    - 创建虚拟环境
    - 安装 Python 依赖
    - 提取模型文件
    - 配置服务
    - 生成安装报告
    ↓
安装完成
```

**为什么需要三层？**
- **Inno Setup**：GUI 安装程序，用户体验好，但脚本功能有限
- **install.bat**：简单桥接，检查环境，调用 Python 脚本
- **install.py**：核心逻辑，功能强大，易于维护

---

## 📁 文件结构

### 修改的文件

| 文件 | 路径 | 说明 |
|------|------|------|
| tdgpt.iss | `/packaging/installer/tdgpt.iss` | Inno Setup 脚本，移除 venv 删除，确保目录创建 |
| win_release.py | `/packaging/win_release.py` | 打包脚本，生成 install.bat，复制 install.py/uninstall.py |

### 新创建的文件

| 文件 | 路径 | 行数 | 说明 |
|------|------|------|------|
| install.py | `/script/install.py` | 550+ | 核心安装脚本 |
| uninstall.py | `/script/uninstall.py` | 350+ | 卸载脚本 |
| WINDOWS_INSTALL_IMPLEMENTATION.md | `/packaging/` | - | 实现总结文档 |

---

## ✨ 核心功能

### install.py - 安装脚本

#### 1. 环境检测与验证
- ✅ **Python 版本检查**
  - 检测 Python 3.10/3.11/3.12
  - 尝试多个命令：python, python3, python3.10, python3.11, python3.12
  - 如果不符合要求，提示用户安装并退出

- ✅ **pip 版本验证**
  - 检查 pip 是否可用
  - 验证 pip 版本

- ✅ **磁盘空间检查**
  - 检查安装目录所在磁盘的可用空间
  - 最小要求：20GB
  - 如果空间不足，错误退出

- ✅ **管理员权限检查**
  - 检查是否以管理员权限运行
  - 如果不是，警告用户（不阻止安装）

#### 2. 安装模式选择
- ✅ **在线模式（默认）**
  - 自动创建 Python 虚拟环境
  - 从 PyPI 安装依赖包
  - 支持自定义 pip 镜像源（环境变量 `PIP_INDEX_URL`）
  - 安装所有必需模型的 venv

- ✅ **离线模式（-o 参数）**
  - 跳过已存在的虚拟环境
  - 使用预打包的 wheels 文件（如果提供）
  - 不尝试联网下载依赖
  - 验证必需的 venv 是否存在

- ✅ **模型选择**
  - 默认：仅安装必需模型（tdtsfm、timemoe）
  - 安装向导中可通过 checkbox 选择可选模型（默认勾选 Moirai、MomentFM）
  - 全部模式（-a 参数）：安装所有模型
  - 指定模式（--model 参数）：安装指定模型，可重复使用
    - 示例：`python install.py --model moirai --model moment`

#### 3. 目录结构初始化

**默认安装目录：** `C:\TDengine\taosanode`（可在安装向导中自定义）

```
<安装目录>\                    # 默认 C:\TDengine\taosanode
├── bin\              # 脚本和可执行文件（服务管理脚本）
├── cfg\              # 配置文件（taosanode.config.py）
│                       卸载时保留，不覆盖用户自定义配置
├── lib\              # Python 库（taosanalytics）
├── model\            # 模型文件（tdtsfm、timemoe 等 tar.gz 解压后的目录）
│                       卸载时保留
├── resource\         # 资源文件（SQL 脚本）
├── log\              # 日志目录
│   ├── taosanode.log           # 统一管理日志（服务管理+配置+进程管理+模型管理）
│   ├── taosanode_stdout.log    # 主服务进程输出（仅 Windows）
│   └── model_{name}.log        # 各模型进程输出（如 model_tdtsfm.log）
├── data\             # 数据目录（运行时数据）
│   └── pids\         # PID 文件目录（taosanode 和模型服务的进程 ID）
│                       卸载时保留
├── venv\             # 主虚拟环境（Python 依赖）
│                       卸载时保留
├── timesfm_venv\     # timesfm 虚拟环境（可选）
├── moirai_venv\      # moirai 虚拟环境（可选）
├── chronos_venv\     # chronos 虚拟环境（可选）
└── momentfm_venv\    # momentfm 虚拟环境（可选）
```

**路径说明：**

| 路径 | 默认值 | 说明 | 卸载时保留 |
|------|--------|------|-----------|
| 安装目录 | `C:\TDengine\taosanode` | 可在安装向导中自定义 | - |
| 配置目录 | `<安装目录>\cfg` | taosanode.config.py 等配置文件 | ✅ |
| 日志目录 | `<安装目录>\log` | taosanode.log（统一管理日志）、taosanode_stdout.log、model_{name}.log | ❌ |
| 数据目录 | `<安装目录>\data` | 运行时数据、PID 文件 | ✅ |
| 模型目录 | `<安装目录>\model` | 模型文件（解压后） | ✅ |
| 虚拟环境 | `<安装目录>\venv` | Python 依赖包 | ✅ |
| 脚本目录 | `<安装目录>\bin` | 服务管理脚本 | ❌ |
| 库目录 | `<安装目录>\lib` | taosanalytics Python 库 | ❌ |

**自定义安装目录：**
- 安装向导中可以修改安装路径（默认 `C:\TDengine\taosanode`）
- 所有路径均基于安装目录自动推导，无需手动修改配置文件
- `taosanode.config.py` 通过 `os.path.dirname(__file__)` 自动检测安装位置
- `taosanode_service.py` 通过脚本所在的 `bin\` 目录自动推导安装根目录

#### 4. 虚拟环境管理
- ✅ **主虚拟环境创建**
  - 使用 `python -m venv C:\TDengine\taosanode\venv`
  - 激活虚拟环境
  - 升级 pip：`python -m pip install --upgrade pip`
  - 安装依赖：`pip install -r requirements_ess.txt`
  - 记录安装日志

- ✅ **额外虚拟环境创建（-a 模式）**
  - timesfm_venv：torch 2.3.1+cpu、jax、timesfm、flask 3.0.3
  - moirai_venv：torch 2.3.1+cpu、uni2ts、flask
  - chronos_venv：torch 2.3.1+cpu、chronos-forecasting、flask
  - momentfm_venv：torch 2.3.1+cpu、transformers 4.33.3、momentfm、flask

- ✅ **离线模式处理**
  - 检查 venv 是否已存在
  - 如果存在且离线模式，跳过重新创建
  - 如果不存在，提示用户需要在线模式或手动创建

#### 5. 配置文件处理
- ✅ **配置文件初始化**
  - 由 Inno Setup 复制 `cfg\taosanode.config.py` 到安装目录
  - 如果已存在配置文件，Inno Setup 会保留（onlyifdoesntexist 标志）
  - 不覆盖用户的自定义配置

#### 6. 模型文件处理
- ✅ **模型文件提取**
  - 检查 model 目录下的 tar.gz 文件
  - 提取必需模型：tdtsfm.tar.gz、timemoe.tar.gz
  - 提取可选模型（-a 模式）：chronos.tar.gz、moment-large.tar.gz、moirai.tar.gz、timesfm.tar.gz
  - 使用 Python tarfile 模块

- ✅ **模型验证**
  - 验证模型文件完整性
  - 检查必需模型是否存在
  - 如果缺失，警告用户

#### 7. 服务管理
- ✅ **Windows 服务注册**
  - 调用 `taosanode_service.py install`
  - 使用 WinSW（Windows Service Wrapper）
  - 设置服务为自动启动

- ✅ **服务验证**
  - 检查服务是否成功注册
  - 验证服务状态
  - 如果失败，提供详细错误信息

#### 8. 环境变量配置
- ✅ **PATH 环境变量**
  - 由 Inno Setup 自动添加 `C:\TDengine\taosanode\bin` 到系统 PATH
  - 使用注册表修改
  - 验证 PATH 是否正确设置

#### 9. 安装验证
- ✅ **环境验证**
  - 检查所有目录是否创建成功
  - 验证虚拟环境是否可用

- ✅ **生成安装报告**
  - 记录安装时间、版本、配置
  - 保存到 `C:\TDengine\taosanode\install.log`
  - 显示安装摘要

#### 10. 错误处理与日志
- ✅ **详细日志记录**
  - 记录每个安装步骤
  - 记录错误和警告
  - 保存到 `install.log`

- ✅ **用户交互**
  - 显示安装进度
  - 彩色输出（成功/警告/错误）
  - Windows 10+ 支持 ANSI 颜色

---

### uninstall.py - 卸载脚本

#### 1. 服务停止与卸载
- ✅ **停止服务**
  - 停止主服务：`taosanode_service.py stop`
  - 停止所有模型服务：`taosanode_service.py model-stop all`
  - 强制终止残留进程（仅 taosanode 相关）

- ✅ **卸载服务**
  - 卸载 Windows 服务注册：`taosanode_service.py uninstall`

#### 2. 文件清理
- ✅ **删除程序文件**
  - 删除：bin、lib、resource、log
  - **保留**：cfg（配置）、model（模型）、data（数据）、venv（虚拟环境）

- ✅ **可选删除（通过参数控制）**
  - `--remove-venv`：删除虚拟环境
  - `--remove-data`：删除数据目录
  - `--remove-model`：删除模型目录
  - `--keep-all`：仅停止服务，保留所有文件

#### 3. 环境清理
- ✅ **从 PATH 中移除**
  - 读取系统 PATH 环境变量
  - 移除安装目录
  - 更新注册表

- ✅ **删除快捷方式**
  - 由 Inno Setup 自动处理

#### 4. 卸载报告
- ✅ **生成卸载报告**
  - 记录卸载配置
  - 保存到 `uninstall.log`

- ✅ **显示保留的目录**
  - 提示用户保留了哪些目录
  - 提示如何手动删除（如果需要）

---

## 🎯 与 Linux install.sh 的对应关系

| Linux 功能 | Windows 实现 | 文件 | 说明 |
|-----------|-------------|------|------|
| 参数解析（-d, -a, -o, -h） | argparse | install.py | 支持 -o（离线）、-a（全部模型） |
| Python 版本检查 | check_python_version() | install.py | 检测 3.10/3.11/3.12 |
| pip 版本检查 | check_pip_version() | install.py | 验证 pip 可用性 |
| 磁盘空间检查 | check_disk_space() | install.py | 最小 20GB |
| 目录创建 | create_directories() | install.py | log、data/pids、model |
| 配置文件安装 | Inno Setup | tdgpt.iss | onlyifdoesntexist 标志 |
| 模型文件提取 | extract_models() | install.py | tar.gz 解压 |
| 主虚拟环境创建 | create_venv("venv") | install.py | requirements_ess.txt |
| 额外虚拟环境创建 | install_venvs() | install.py | -a 模式 |
| 服务注册 | install_service() | install.py | 调用 taosanode_service.py |
| 卸载 | uninstall.py | uninstall.py | 保留 cfg、model、data、venv |

---

## 📝 使用方法

### 安装

#### 方式 1：GUI 安装（推荐）
```bash
# 双击 .exe 安装程序
tdengine-tdgpt-oss-1.0.0-Windows-x64.exe

# 安装向导会自动：
# 1. 复制文件到 C:\TDengine\taosanode
# 2. 执行 install.bat
# 3. install.bat 调用 install.py
# 4. 创建 venv、安装依赖、配置服务
```

#### 方式 2：命令行安装（高级用户）
```bash
# 在线模式（默认）
python C:\TDengine\taosanode\install.py

# 离线模式
python C:\TDengine\taosanode\install.py -o

# 安装所有模型
python C:\TDengine\taosanode\install.py -a

# 离线 + 所有模型
python C:\TDengine\taosanode\install.py -o -a

# 自定义 pip 镜像
set PIP_INDEX_URL=https://pypi.tuna.tsinghua.edu.cn/simple
python C:\TDengine\taosanode\install.py
```

### 卸载

#### 方式 1：控制面板卸载（推荐）
```
控制面板 → 程序 → 程序和功能 → TDGPT → 卸载
```

#### 方式 2：命令行卸载（高级用户）
```bash
# 标准卸载（保留 cfg、model、data、venv）
python C:\TDengine\taosanode\uninstall.py

# 删除虚拟环境
python C:\TDengine\taosanode\uninstall.py --remove-venv

# 删除数据目录
python C:\TDengine\taosanode\uninstall.py --remove-data

# 删除模型目录
python C:\TDengine\taosanode\uninstall.py --remove-model

# 完全删除
python C:\TDengine\taosanode\uninstall.py --remove-venv --remove-data --remove-model

# 仅停止服务，保留所有文件
python C:\TDengine\taosanode\uninstall.py --keep-all
```

### 打包

```bash
# 社区版
python packaging/win_release.py -e community -v 1.0.0 -m D:\models

# 企业版
python packaging/win_release.py -e enterprise -v 1.0.0 -m D:\models

# 离线模式打包（包含 wheels）
python packaging/win_release.py -e community -v 1.0.0 -m D:\models --offline

# 包含所有模型
python packaging/win_release.py -e community -v 1.0.0 -m D:\models -a

# 离线 + 所有模型
python packaging/win_release.py -e community -v 1.0.0 -m D:\models --offline -a
```

---

## 🔑 关键设计决策

### 1. 混合方案（Inno Setup + install.bat + install.py）
**原因：**
- Inno Setup：GUI 安装程序，用户体验好
- install.bat：简单桥接，检查环境
- install.py：核心逻辑，功能强大，易于维护

**优点：**
- 兼容性好
- 功能完整
- 易于调试和扩展

### 2. 卸载保留策略
**保留：** cfg、model、data、venv
**删除：** bin、lib、resource、log

**原因：**
- 与 Linux 行为一致
- 保护用户数据和配置
- 方便升级（保留 venv 可加速重装）

### 3. 离线/在线模式
**在线模式：**
- 自动创建 venv
- 从 PyPI 安装依赖
- 适合首次安装

**离线模式（-o）：**
- 跳过已存在的 venv
- 使用预打包 wheels（如果有）
- 适合升级和受限环境

### 4. 模型选择
**默认：** 仅安装必需模型（tdtsfm、timemoe）
**全部模式（-a）：** 安装所有模型

**原因：**
- 节省磁盘空间（每个模型 venv 约 2-3GB）
- 加快安装速度
- 用户可按需安装

### 5. 错误处理策略
**非阻塞警告：**
- 管理员权限检查
- 服务注册失败

**阻塞错误：**
- Python 版本不符
- 磁盘空间不足
- venv 创建失败

**原因：**
- 确保核心功能可用
- 不过度限制用户

---

## 🧪 测试建议

### 1. 基本安装测试
- [ ] 在线模式安装
- [ ] 离线模式安装
- [ ] 全部模型安装
- [ ] 离线 + 全部模型安装

### 2. 环境测试
- [ ] Python 3.10
- [ ] Python 3.11
- [ ] Python 3.12
- [ ] 管理员权限
- [ ] 普通用户权限
- [ ] 不同磁盘空间情况
- [ ] 有/无网络连接

### 3. 升级测试
- [ ] 安装旧版本
- [ ] 运行新版本安装程序
- [ ] 验证配置和数据是否保留
- [ ] 验证 venv 是否正确更新

### 4. 卸载测试
- [ ] 标准卸载
- [ ] 删除 venv
- [ ] 完全删除
- [ ] 验证保留的目录

### 5. 打包测试
- [ ] 社区版打包
- [ ] 企业版打包
- [ ] 离线模式打包
- [ ] 全部模型打包

---

## 📚 相关文档

### 已有文档
- [INSTALL-CN.md](../INSTALL-CN.md) - 中文安装指南
- [packaging/README.md](README.md) - 英文打包文档
- [packaging/README-CN.md](README-CN.md) - 中文打包文档

### 新增文档
- [WINDOWS_INSTALL_IMPLEMENTATION.md](WINDOWS_INSTALL_IMPLEMENTATION.md) - 实现总结文档（本文档的详细版）

### 需要更新的文档
- [ ] INSTALL-CN.md：添加 install.py 和 uninstall.py 的使用说明
- [ ] packaging/README.md：更新打包流程说明
- [ ] packaging/README-CN.md：更新中文打包文档

---

## 🚀 后续改进建议

### 已完成的改进 ✅

#### 1. GUI 安装向导
- ✅ 在 Inno Setup 中添加了离线/在线模式选择页面
- ✅ 添加了模型选择页面（checkbox 多选，默认勾选 Moirai + MomentFM）
- ✅ 用户选择会自动传递给 install.py（-o 和 --model 参数）

#### 2. 进度条显示
- ✅ 在 install.py 中添加了 `print_progress()` 方法
- ✅ 虚拟环境创建过程显示进度条（1/4 到 4/4）
- ✅ 使用彩色进度条（█ 和 ░ 字符）

#### 3. 日志整合
- ✅ 合并为 `taosanode.log`（统一管理日志）+ `taosanode_stdout.log` + `model_{name}.log`
- ✅ 通过 logger name 字段区分日志来源（Config/ProcessManager/TaosanodeService/ModelService）

#### 4. 脚本国际化
- ✅ 所有脚本注释和日志输出改为英文

#### 5. 卸载一致性
- ✅ Linux uninstall.sh 保留 cfg 目录（与 Windows 一致）

### 后续改进
1. **健康检查**：安装后自动运行健康检查脚本
2. **回滚功能**：安装失败时自动回滚
3. **多语言支持**：支持中文和英文安装界面

---

## ⚠️ 已知限制

1. **Python 依赖**：需要用户预先安装 Python 3.10/3.11/3.12
2. **管理员权限**：服务注册需要管理员权限（但不强制）
3. **网络连接**：在线模式需要网络连接（可通过离线模式解决）
4. **磁盘空间**：完整安装需要约 20GB 空间
5. **防火墙**：已移除自动配置功能，需要用户手动配置

---

## 📞 支持

如有问题，请参考：
- 安装日志：`<安装目录>\install.log`
- 卸载日志：`<安装目录>\uninstall.log`
- 服务日志：`<安装目录>\log\taosanode.log`（统一管理日志）
- 主服务输出：`<安装目录>\log\taosanode_stdout.log`（仅 Windows）
- 模型日志：`<安装目录>\log\model_{name}.log`

---

## 📝 更新日志

### 2026-03-16
- ✅ 日志整合：合并为 taosanode.log + taosanode_stdout.log + model_{name}.log
- ✅ 所有脚本注释和日志输出改为英文
- ✅ ISS 安装向导改为 checkbox 多选模型，默认勾选 Moirai + MomentFM
- ✅ install.py 新增 `--model` 参数支持选择性安装模型
- ✅ Linux uninstall.sh 保留 cfg 目录（与 Windows 一致）
- ✅ 支持自定义安装目录（DisableDirPage=no）
- ✅ 补充路径说明文档

### 2026-03-13
- ✅ 创建 install.py 核心安装脚本
- ✅ 创建 uninstall.py 卸载脚本
- ✅ 修改 tdgpt.iss，移除 venv 删除
- ✅ 修改 win_release.py，生成 install.bat
- ✅ 移除防火墙配置功能
- ✅ 创建功能列表文档

---

**文档版本：** 2.0
**最后更新：** 2026-03-16
**维护者：** TDGPT 开发团队

## 2026-03-17 补充说明

- 在线安装模式下，安装向导支持选择 pip 源。
- 默认使用官方 PyPI，不默认勾选国内镜像。
- 可选预置镜像包括清华镜像、阿里云镜像，也支持填写自定义镜像 URL。
- 标准卸载默认保留 `cfg`、`model`、`data`、`venv`。
- 只有显式使用 `--remove-model` 时，才会删除 `model` 目录。

## 2026-03-17 目录、日志与安装向导补充

### 当前 Windows 目录布局

```text
<安装目录>\
├── bin\
├── cfg\
├── data\
├── log\
├── model\
├── requirements\
└── venvs\
    ├── venv\
    ├── timesfm_venv\
    ├── moirai_venv\
    ├── chronos_venv\
    └── momentfm_venv\
```

### 安装向导补充

- 在线安装时支持选择 pip 源，默认仍然是官方 PyPI。
- 完成安装前新增“安装并注册 Windows 服务”选项，默认勾选。
- 安装完成页会提示以下命令：
  - `net start Taosanode`
  - `net stop Taosanode`
  - `<安装目录>\bin\start-taosanode.bat`
  - `<安装目录>\bin\stop-taosanode.bat`
- 非静默安装完成后会自动打开 `<安装目录>\log\install.log`。

### Current log files and purpose

- `<安装目录>\log\install.log`
  - 安装器调用 `install.bat/install.py` 的完整输出和安装摘要。
- `<安装目录>\log\uninstall.log`
  - 卸载器调用 `uninstall.bat/uninstall.py` 的完整输出和卸载摘要。
- `<安装目录>\log\taosanode-service.log`
  - 服务注册、服务管理脚本、主服务启动生命周期日志。
- `<安装目录>\log\taosanode-winsw.wrapper.log`
  - WinSW wrapper lifecycle log.
- `<安装目录>\log\taosanode.app.log`
  - taosanode 应用日志。

### 合并策略说明

- 安装日志已经合并为一个文件：`log\install.log`。
- 卸载日志已经合并为一个文件：`log\uninstall.log`。
- 服务管理日志已经统一写入 `log\taosanode-service.log`。
- WinSW stdout/stderr 已关闭，只保留一个明确命名的 wrapper 日志：`log\taosanode-winsw.wrapper.log`。

### 标准卸载保留策略

- 默认保留：`cfg`、`data`、`model`、`venvs`、`log`。
- 默认删除：`bin`、`lib`、`resource`、`requirements` 和安装脚本。
- `model` 只有在显式执行 `uninstall.py --remove-model` 时才会删除。

## 2026-03-18 补充说明

### 安装器与打包结构调整

- Windows 安装流程继续使用英文界面，但本文档保持中文记录。
- 打包目录结构统一为：
  - requirements 文件放到 `<安装目录>\requirements\`
  - 虚拟环境放到 `<安装目录>\venvs\`
  - 日志统一放到 `<安装目录>\log\`
- `packaging\bin\WinSW.exe` 会打入安装产物，并复制为 `<安装目录>\bin\taosanode-winsw.exe`。
- WinSW 配置文件会生成到 `<安装目录>\bin\taosanode-winsw.xml`。

### Python 依赖安装流程补充

- Windows 主运行环境依赖入口调整为 `requirements_windows_core.txt`。
- TensorFlow CPU 依赖拆分到 `requirements_tensorflow.txt`，用于在线安装时按需追加。
- 在线安装向导支持选择 pip 源：
  - 官方 PyPI
  - 清华源
  - 阿里云源
  - 自定义 URL
- 默认仍然是官方 PyPI，不自动切换国内源。
- 离线 Python 模式下不再单独询问 TensorFlow，因为离线包预期已经包含对应环境内容。

### 模型安装流程补充

- 安装向导中的模型准备方式调整为三种：
  - 暂不安装模型
  - 在线下载所选模型
  - 导入离线模型包
- 在线下载默认勾选的模型调整为：
  - `Moirai Small`
  - `MOMENT Base`
- 在线模型下载支持：
  - 官方 Hugging Face
  - HF Mirror
  - 自定义端点 URL
- Windows 侧统一模型顺序调整为：
  1. `tdtsfm`
  2. `timemoe`
  3. `moirai`
  4. `chronos`
  5. `timesfm`
  6. `moment`
- `TDtsfm v1.0` 当前仍然只能通过离线方式导入。
- `start-model.bat all` / `model-start all` 调整为运行时按模型目录存在性判断，缺失目录直接跳过，不再作为硬错误处理。

### 服务、完成页与日志补充

- 安装向导加入“安装并注册 Taosanode Windows 服务”选项，默认勾选。
- 完成页补充展示：
  - `net start Taosanode`
  - `net stop Taosanode`
  - `start-taosanode.bat`
  - `stop-taosanode.bat`
  - `start-model.bat all`
  - `stop-model.bat all`
  - `status-model.bat`
- 非静默安装完成后会自动打开 `<安装目录>\log\install.log`。
- 安装日志统一写入 `<安装目录>\log\install.log`。
- 卸载日志统一写入 `<安装目录>\log\uninstall.log`。
- Python 服务管理日志统一写入 `<安装目录>\log\taosanode-service.log`。
- WinSW wrapper 日志写入 `<安装目录>\log\taosanode-winsw.wrapper.log`。
- taosanode 主进程输出写入 `<安装目录>\log\taosanode.app.log`。
- 模型运行日志写入 `<安装目录>\log\model_<name>.log`。

### 卸载策略补充

- 标准卸载默认保留：
  - `cfg`
  - `data`
  - `model`
  - `venvs`
  - `log`
- 标准卸载默认删除：
  - `bin`
  - `lib`
  - `resource`
  - `requirements`
  - 安装辅助脚本
- `model` 目录默认保留，只有显式执行 `uninstall.py --remove-model` 才会删除。

## 2026-03-19 补充说明

### 离线导入页与导入逻辑调整

- 离线模型导入页从“每个模型一个文件选择框”调整为“一个可选的离线总包输入框”。
- 该离线总包用于一次性导入多个模型，不再要求用户逐个模型选择压缩包。
- 额外离线包支持格式：
  - `zip`
  - `tar`
  - `tar.gz`
  - `tgz`
- 安装器仍然会自动扫描并导入 `<安装目录>\model\` 下打包时自带的离线模型压缩包。
- 如果用户额外选择了一个离线总包，安装脚本会导入该总包中识别出的全部模型内容。
- 离线总包支持两种布局：
  - 包内已经是各模型目录
  - 包内再嵌套各模型 zip/tar 压缩包
- 本次导入时，只会替换本次实际导入到的模型目录，不会无差别覆盖全部已有模型目录。

### 截断问题修复说明

- 之前离线说明文字被截断，原因是 Inno Setup 自定义文本控件使用了固定高度，超出部分不会自动展开。
- 现在离线导入流程改为标准文件选择页，避免长说明文本因为固定高度被裁切。
- 完成页说明区域也改成自动计算高度，降低长文本被截断的风险。

### 脚本与运行时行为补充

- `install.py` 新增可选参数 `--offline-model-package`。
- `install.py` 会在安装摘要中记录所选离线总包路径。
- `install.py` 继续以 `requirements_windows_core.txt` 作为 Windows 主环境依赖入口。
- 在线安装时，仅为所选在线模型创建额外模型虚拟环境。
- 离线导入模式下，不会在安装阶段额外创建模型专属虚拟环境。
- `taosanode_service.py` 的 `start all` 汇总逻辑已调整为准确统计：
  - 已启动模型
  - 缺失目录而跳过的模型
  - 启动失败的模型
- `taosanode_service.py` 的 `stop all` 汇总逻辑已调整为准确统计：
  - 本次真正停止的模型
  - 原本就未运行的模型
  - 停止失败的模型

### 重新安装语义补充

- `暂不安装模型`
  - 保持现有模型目录不变。
- `在线下载所选模型`
  - 仅刷新本次选择的在线模型。
  - 未选中的模型目录保持不变。
- `导入离线模型包`
  - 导入 `<安装目录>\model` 中自带的离线包。
  - 如用户额外选择离线总包，也会导入该总包中识别出的模型。
  - 只替换本次实际导入的模型目录。

### 2026-03-19 完成的验证

- 重新构建了社区版测试包：
  - `python packaging/win_release.py -e community -v 3.4.0.11.0316 --skip-model-check`
- 验证产物：
  - `D:\tdgpt-e2e-20260319-r5\tdengine-tdgpt-oss-3.4.0.11.0316-Windows-x64.exe`
- 已验证默认安装目录在线安装：
  - `C:\TDengine\taosanode`
- 已验证使用清华 pip 源在线安装依赖。
- 已验证使用 `https://hf-mirror.com` 在线下载模型。
- 已验证默认在线模型选择为：
  - `moirai`
  - `moment`
- 已验证服务注册与控制：
  - 安装服务
  - `net start Taosanode`
  - `net stop Taosanode`
- 已验证 `start-model.bat all` 只启动存在模型目录的模型。
- 已验证默认在线模型运行端口：
  - `moirai` 监听 `127.0.0.1:6039`
  - `moment` 监听 `127.0.0.1:6062`
- 已验证 `stop-model.bat all` 在修复后可以输出准确的停止汇总信息。
