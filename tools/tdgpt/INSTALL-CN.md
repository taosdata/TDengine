# 1. TDGPT 安装和使用指南

> 📖 **语言**: [中文](INSTALL-CN.md) | [English](INSTALL.md)

本指南涵盖 TDGPT 在 Linux 和 Windows 平台上的安装和使用。

## 目录

- [系统要求](#系统要求)
- [Linux 安装](#linux-安装)
- [Windows 安装](#windows-安装)
- [服务管理](#服务管理)
- [配置](#配置)
- [模型管理](#模型管理)
- [故障排查](#故障排查)
- [常见问题](#常见问题)

---

## 系统要求

### 支持的平台

- **Linux**: Ubuntu 18.04+、CentOS 7+ 或其他 Linux 发行版
- **Windows**: Windows Server 2016+ 或 Windows 10/11

### Python 版本

- Python 3.10、3.11 或 3.12（必需）

### 硬件要求

- **CPU**: 建议 4+ 核心
- **内存**: 建议 8GB+
- **磁盘**: 模型和数据需要 20GB+

### 网络端口

- **6035**: 主 taosanode 服务
- **6061**: tdtsfm 模型服务
- **6062**: timemoe 模型服务
- **6063**: chronos 模型服务
- **6064**: moirai 模型服务
- **6065**: timesfm 模型服务
- **6066**: moment 模型服务
- **6067-6070**: 预留模型服务端口

---

## Linux 安装

### 前置条件

1. 安装 Python 3.10+（如果尚未安装）：

```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install python3.10 python3.10-venv python3.10-dev

# CentOS/RHEL
sudo yum install python310 python310-devel
```

2. 确保您有系统级安装的 sudo 权限。

### 安装步骤

1. **解压安装包**：

```bash
tar -xzf tdgpt-linux-x.x.x.tar.gz
cd tdgpt-linux-x.x.x
```

2. **运行安装脚本**：

```bash
# 标准安装
bash install.sh

# 离线安装（如果包含 wheels）
bash install.sh -o

# 自定义安装目录
bash install.sh -d /opt/tdgpt
```

3. **验证安装**：

```bash
# 检查服务状态
python /usr/local/taos/taosanode/bin/taosanode_service.py status

# 检查模型状态
python /usr/local/taos/taosanode/bin/taosanode_service.py model-status
```

### Linux 服务管理

#### 使用 systemd（推荐）

安装脚本会自动创建 systemd 服务。使用以下命令管理：

```bash
# 启动服务
sudo systemctl start taosanode

# 停止服务
sudo systemctl stop taosanode

# 重启服务
sudo systemctl restart taosanode

# 检查状态
sudo systemctl status taosanode

# 启用开机自启
sudo systemctl enable taosanode

# 禁用开机自启
sudo systemctl disable taosanode

# 查看日志
sudo journalctl -u taosanode -f
```

#### 直接使用 Python 脚本

```bash
# 启动服务
python /usr/local/taos/taosanode/bin/taosanode_service.py start

# 停止服务
python /usr/local/taos/taosanode/bin/taosanode_service.py stop

# 检查状态
python /usr/local/taos/taosanode/bin/taosanode_service.py status
```

### Linux 卸载

```bash
# 运行卸载脚本
bash /usr/local/taos/taosanode/uninstall.sh

# 或手动删除
sudo rm -rf /usr/local/taos/taosanode
sudo systemctl disable taosanode
```

---

## Windows 安装

### 前置条件

1. **Python 3.10 / 3.11 / 3.12** - 需要安装 Python 并添加到 PATH
   - 从 [python.org](https://www.python.org/downloads/) 下载
   - 安装时勾选"Add Python to PATH"
   - 验证安装：`python --version`

2. **Microsoft Visual C++ Redistributable x64** - TensorFlow、PyTorch 以及其他原生 Python 依赖所必需
   - 下载地址：[VC++ Redistributable](https://aka.ms/vc14/vc_redist.x64.exe)

3. **管理员权限**：
   - 服务安装需要管理员权限
   - 以管理员身份运行命令提示符

### 安装步骤

1. **解压安装包**：
   - 右键点击安装程序 `.exe` 文件
   - 选择"以管理员身份运行"
   - 按照安装向导进行操作

2. **或手动解压 ZIP 包**：
   - 解压到所需位置（例如 `C:\TDengine\taosanode`）
   - 以管理员身份打开命令提示符
   - 导航到安装目录
   - 运行：`install.bat`

3. **验证安装**：

```batch
python C:\TDengine\taosanode\bin\taosanode_service.py status
```

### Windows 服务管理

#### 选项 1：使用 Windows 服务（推荐）

需要管理员权限。

```batch
# 安装为 Windows 服务
python C:\TDengine\taosanode\bin\taosanode_service.py install-service

# 启动服务
net start Taosanode
# 或：python C:\TDengine\taosanode\bin\taosanode_service.py start-service

# 停止服务
net stop Taosanode
# 或：python C:\TDengine\taosanode\bin\taosanode_service.py stop-service

# 检查服务状态
sc query Taosanode

# 卸载服务
python C:\TDengine\taosanode\bin\taosanode_service.py uninstall-service
```

#### 选项 2：使用批处理脚本（前台模式）

```batch
# 启动服务（前台）
C:\TDengine\taosanode\bin\start-taosanode.bat

# 停止服务
C:\TDengine\taosanode\bin\stop-taosanode.bat

# 检查状态
C:\TDengine\taosanode\bin\status-taosanode.bat
```

#### 选项 3：直接使用 Python 脚本

```batch
# 启动
python C:\TDengine\taosanode\bin\taosanode_service.py start

# 停止
python C:\TDengine\taosanode\bin\taosanode_service.py stop

# 状态
python C:\TDengine\taosanode\bin\taosanode_service.py status
```

### Windows 防火墙配置

如果遇到连接问题，请配置 Windows 防火墙：

```batch
# 自动打开端口（以管理员身份运行）
netsh advfirewall firewall add rule name="TDGPT" dir=in action=allow protocol=TCP localport=6035,6061-6070 profile=any

# 或手动配置：
# 1. 打开 Windows Defender 防火墙（高级安全）
# 2. 点击"入站规则" → "新建规则"
# 3. 选择"端口" → "TCP" → 特定本地端口：6035,6061-6070
# 4. 允许连接
```

### Windows 卸载

```batch
# 运行卸载脚本
C:\TDengine\taosanode\uninstall.bat

# 或手动卸载：
# 1. 卸载 Windows 服务（如果已安装）：
#    python C:\TDengine\taosanode\bin\taosanode_service.py uninstall-service
# 2. 删除安装目录：
#    rmdir /s /q C:\TDengine\taosanode
```

---

## 服务管理

### 常用命令

#### 启动/停止/状态

```bash
# Linux
python /usr/local/taos/taosanode/bin/taosanode_service.py start
python /usr/local/taos/taosanode/bin/taosanode_service.py stop
python /usr/local/taos/taosanode/bin/taosanode_service.py status

# Windows
python C:\TDengine\taosanode\bin\taosanode_service.py start
python C:\TDengine\taosanode\bin\taosanode_service.py stop
python C:\TDengine\taosanode\bin\taosanode_service.py status
```

#### 模型管理

```bash
# 启动特定模型
python <install_dir>/bin/taosanode_service.py model-start tdtsfm

# 启动所有模型
python <install_dir>/bin/taosanode_service.py model-start all

# 停止特定模型
python <install_dir>/bin/taosanode_service.py model-stop tdtsfm

# 停止所有模型
python <install_dir>/bin/taosanode_service.py model-stop all

# 检查模型状态
python <install_dir>/bin/taosanode_service.py model-status
```

---

## 配置

### 配置文件位置

- **Linux**: `/usr/local/taos/taosanode/cfg/taosanode.config.py`
- **Windows**: `C:\TDengine\taosanode\cfg\taosanode.config.py`

### 关键配置参数

#### 服务绑定

```python
# 监听地址和端口
bind = '0.0.0.0:6035'

# 工作进程数
workers = 2
```

#### 日志

```python
# 日志级别：DEBUG、INFO、WARNING、ERROR、CRITICAL
log_level = 'DEBUG'

# 日志文件位置
app_log = '/var/log/taos/taosanode/taosanode.app.log'  # Linux
app_log = 'c:/TDengine/taosanode/log/taosanode.app.log'  # Windows
```

#### 模型配置

```python
# 模型存储目录
model_dir = '/usr/local/taos/taosanode/model/'  # Linux
model_dir = 'c:/TDengine/taosanode/model/'  # Windows

# 模型定义
models = {
    "tdtsfm": {
        "script": "tdtsfm-server.py",
        "port": 6061,
        "required": True,  # 必需
    },
    "timemoe": {
        "script": "timemoe-server.py",
        "port": 6062,
        "required": True,  # 必需
    },
    "chronos": {
        "script": "chronos-server.py",
        "port": 6063,
        "required": False,  # 可选
    },
    # ... 其他模型
}
```

#### Windows Waitress 配置

```python
# Waitress 服务器配置（仅 Windows）
waitress_config = {
    'threads': 4,                    # 工作线程数
    'channel_timeout': 1200,         # 通道超时（秒）
    'connection_limit': 1000,        # 最大连接数
    'cleanup_interval': 30,          # 清理间隔（秒）
    'log_socket_errors': True        # 记录 socket 错误
}
```

### 修改配置

1. 用文本编辑器编辑配置文件
2. 重启服务使更改生效：

```bash
# Linux
sudo systemctl restart taosanode

# Windows
net stop Taosanode
net start Taosanode
```

---

## 模型管理

### 支持的模型

| 模型 | 必需 | 端口 | 描述 |
|------|------|------|------|
| tdtsfm | 是 | 6061 | 时间序列基础模型 |
| timemoe | 是 | 6062 | 时间序列专家混合 |
| chronos | 否 | 6063 | Amazon Chronos |
| moirai | 否 | 6064 | Salesforce Moirai |
| timesfm | 否 | 6065 | Google TimesFM |
| moment | 否 | 6066 | AutonLab MOMENT |

### 启动模型

```bash
# 启动必需模型（tdtsfm、timemoe）
python <install_dir>/bin/taosanode_service.py model-start all

# 启动特定模型
python <install_dir>/bin/taosanode_service.py model-start chronos

# 检查模型状态
python <install_dir>/bin/taosanode_service.py model-status
```

### 停止模型

```bash
# 停止所有模型
python <install_dir>/bin/taosanode_service.py model-stop all

# 停止特定模型
python <install_dir>/bin/taosanode_service.py model-stop chronos
```

---

## 故障排查

### 服务无法启动

**Linux**：

```bash
# 检查 systemd 日志
sudo journalctl -u taosanode -n 50

# 检查 Python 错误
python /usr/local/taos/taosanode/bin/taosanode_service.py start
```

**Windows**：

```batch
# 检查启动日志
type C:\TDengine\taosanode\log\taosanode_startup.log

# 直接运行查看错误
python C:\TDengine\taosanode\bin\taosanode_service.py start
```

### 端口已被占用

```bash
# Linux：查找使用端口 6035 的进程
sudo lsof -i :6035

# Windows：查找使用端口 6035 的进程
netstat -ano | findstr :6035

# 杀死进程
# Linux：sudo kill -9 <PID>
# Windows：taskkill /PID <PID> /F
```

### 模型无法启动

1. 检查模型目录是否存在：

```bash
# Linux
ls -la /usr/local/taos/taosanode/model/

# Windows
dir C:\TDengine\taosanode\model\
```

2. 检查模型日志：

```bash
# Linux
tail -f /var/log/taos/taosanode/taosanode_service_tdtsfm.log

# Windows
type C:\TDengine\taosanode\log\taosanode_service_tdtsfm.log
```

3. 验证 Python 依赖：

```bash
python -m pip list | grep -E "torch|transformers"
```

### 连接被拒绝

1. 检查服务是否运行：

```bash
python <install_dir>/bin/taosanode_service.py status
```

2. 检查防火墙设置：

```bash
# Linux
sudo ufw status
sudo ufw allow 6035/tcp

# Windows
netsh advfirewall firewall show rule name="TDGPT"
```

3. 检查绑定地址：

```bash
# Linux
sudo netstat -tlnp | grep 6035

# Windows
netstat -ano | findstr :6035
```

---

## 常见问题

### Q: 如何检查服务是否运行？

**A**: 使用状态命令：

```bash
python <install_dir>/bin/taosanode_service.py status
```

### Q: 可以运行多个实例吗？

**A**: 不推荐。服务设计为单实例运行。如需要，可通过修改配置文件使用不同的端口。

### Q: 如何查看日志？

**A**:

- **Linux**: `tail -f /var/log/taos/taosanode/taosanode.app.log`
- **Windows**: `type C:\TDengine\taosanode\log\taosanode.app.log`

### Q: 如何更新配置？

**A**: 编辑配置文件并重启服务。更改将在重启后生效。

### Q: 需要重新安装怎么办？

**A**:

1. 卸载当前版本
2. 删除安装目录
3. 运行新安装程序

### Q: 如何启用调试日志？

**A**: 编辑配置文件，设置 `log_level = 'DEBUG'`，然后重启服务。

### Q: 可以使用自定义 Python 虚拟环境吗？

**A**: 可以。修改配置文件中的 `venv_dir` 指向您的虚拟环境。

### Q: 如何备份配置？

**A**: 将配置文件复制到安全位置：

```bash
# Linux
cp /usr/local/taos/taosanode/cfg/taosanode.config.py ~/taosanode.config.backup.py

# Windows
copy C:\TDengine\taosanode\cfg\taosanode.config.py C:\backup\taosanode.config.backup.py
```

---

## 支持

如有问题或疑问，请参考内部文档或联系开发团队。
