---
title: "Anode 管理"
sidebar_label: "Anode 管理"
description: 介绍 Anode 服务启停、模型管理、日志和目录结构
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

本节介绍安装版 TDgpt Anode 的服务启停、模型启停、日志位置、目录结构和基本注册管理方法。

## 服务启停

<Tabs>
<TabItem label="Linux 系统" value="linux-service">

Linux 系统中安装 Anode 以后会自动创建 `taosanoded` 服务，用户可使用 `systemd` 来管理 Anode 服务。

```bash
systemctl start  taosanoded
systemctl stop   taosanoded
systemctl status taosanoded
systemctl restart taosanoded
```

如需查看服务日志：

```bash
journalctl -u taosanoded -f
```

</TabItem>
<TabItem label="Windows 系统" value="windows-service">

Windows 安装完成后会注册 `Taosanode` 服务，推荐优先通过 Windows 服务或安装目录下的批处理脚本管理。

### 推荐方式 1：使用 Windows 服务

```batch
net start Taosanode
net stop Taosanode
sc query Taosanode
```

### 推荐方式 2：使用安装目录下的批处理脚本

```batch
C:\TDengine\taosanode\bin\start-taosanode.bat
C:\TDengine\taosanode\bin\stop-taosanode.bat
C:\TDengine\taosanode\bin\status-taosanode.bat
```

### 高级方式：直接使用 Python 管理脚本

```batch
C:\TDengine\taosanode\venvs\venv\Scripts\python.exe C:\TDengine\taosanode\bin\taosanode_service.py start
C:\TDengine\taosanode\venvs\venv\Scripts\python.exe C:\TDengine\taosanode\bin\taosanode_service.py stop
C:\TDengine\taosanode\venvs\venv\Scripts\python.exe C:\TDengine\taosanode\bin\taosanode_service.py status
```

:::note

Windows 运行包装器现在固定使用安装目录下的主虚拟环境：

- `venvs\venv\Scripts\python.exe`

如果这个主虚拟环境缺失，`start-taosanode.bat`、`stop-taosanode.bat`、`status-taosanode.bat` 会直接报错退出，不再回退到系统 PATH 中的 Python。

:::

</TabItem>
</Tabs>

## 模型服务管理

<Tabs>
<TabItem label="Linux 系统" value="linux-model">

提供时序基础模型服务需要占用较大的内存资源。如果您需要体验时序基础模型服务，可手动执行如下命令：

```bash
# 启动涛思时序数据基础模型
start-model tdtsfm

# 启动 Time-MoE 基础模型
start-model timemoe
```

```bash
# 停止涛思时序数据基础模型
stop-model tdtsfm

# 停止 Time-MoE 基础模型
stop-model timemoe
```

> 上述命令只在安装版本中可用，使用 Docker 镜像和云服务，该命令不可用。更多信息请参考[时序模型服务启动和停止脚本](../dev/tsfm/#时序模型服务启动和停止脚本)。

</TabItem>
<TabItem label="Windows 系统" value="windows-model">

Windows 安装完成后，可以通过安装目录下的脚本管理模型服务：

```batch
C:\TDengine\taosanode\bin\start-model.bat tdtsfm
C:\TDengine\taosanode\bin\stop-model.bat tdtsfm
C:\TDengine\taosanode\bin\status-model.bat
```

启动全部已存在模型目录：

```batch
C:\TDengine\taosanode\bin\start-model.bat all
```

或者直接双击 / 不带参数执行：

```batch
C:\TDengine\taosanode\bin\start-model.bat
```

默认行为是：

- 检查当前安装目录下实际存在的模型目录
- 存在的模型自动启动
- 不存在的模型自动跳过

停止全部模型：

```batch
C:\TDengine\taosanode\bin\stop-model.bat all
```

</TabItem>
</Tabs>

## 目录结构与日志

<Tabs>
<TabItem label="Linux 系统" value="linux-layout">

安装完成后，Linux 版本的 Anode 关键目录如下：

| 目录/文件 | 说明 |
| --- | --- |
| `/usr/local/taos/taosanode/bin` | 可执行文件和脚本目录 |
| `/usr/local/taos/taosanode/lib` | 库文件目录 |
| `/usr/local/taos/taosanode/model` | 模型目录 |
| `/var/log/taos/taosanode/` | 日志目录 |
| `/etc/taos/taosanode.config.py` | 配置文件 |

常见日志包括：

- `/var/log/taos/taosanode/taosanode.app.log`
- `journalctl -u taosanoded` 输出的服务日志

</TabItem>
<TabItem label="Windows 系统" value="windows-layout">

安装完成后，Windows 版本的 Anode 关键目录如下：

| 目录/文件 | 说明 |
| --- | --- |
| `C:\TDengine\taosanode\bin` | 可执行脚本和服务包装脚本 |
| `C:\TDengine\taosanode\cfg` | 配置文件和安装状态文件 |
| `C:\TDengine\taosanode\lib` | Python 业务代码 |
| `C:\TDengine\taosanode\log` | 安装日志、服务日志、模型日志 |
| `C:\TDengine\taosanode\model` | 模型目录 |
| `C:\TDengine\taosanode\python` | 离线导入时落地的 Python runtime 目录 |
| `C:\TDengine\taosanode\venvs` | 主虚拟环境和模型虚拟环境 |

说明：
- `python\runtime` 只会在外部离线 tar 导入后出现
- `bin\start-taosanode.bat`、`bin\stop-taosanode.bat`、`bin\status-taosanode.bat` 以及模型启停脚本都要求 `venvs\venv\Scripts\python.exe` 存在

与本次 Windows 安装链路相关的关键日志包括：

- `C:\TDengine\taosanode\log\install.log`
- `C:\TDengine\taosanode\log\uninstall.log`
- `C:\TDengine\taosanode\log\taosanode-service.log`
- `C:\TDengine\taosanode\log\model_*.log`

与本次实现相关的关键文件包括：

- `C:\TDengine\taosanode\cfg\install-state.json`
- `C:\TDengine\taosanode\cfg\enabled_models.txt`
- `C:\TDengine\taosanode\bin\start-taosanode.bat`
- `C:\TDengine\taosanode\bin\start-model.bat`

</TabItem>
</Tabs>

## 配置文件

<Tabs>
<TabItem label="Linux 系统" value="linux-config">

Linux 配置文件默认位于：

```text
/etc/taos/taosanode.config.py
```

</TabItem>
<TabItem label="Windows 系统" value="windows-config">

Windows 配置文件默认位于：

```text
C:\TDengine\taosanode\cfg\taosanode.config.py
```

Windows 主服务使用 Waitress 运行，常见配置包括：

```python
bind = '0.0.0.0:6035'
workers = 2

waitress_config = {
    'threads': 4,
    'channel_timeout': 1200,
    'connection_limit': 1000,
    'cleanup_interval': 30,
    'log_socket_errors': True,
}
```

</TabItem>
</Tabs>

## Anode 基本操作

无论 Linux 还是 Windows，Anode 启动后都需要注册到 TDengine TSDB 集群中。

### 创建 Anode

```sql
CREATE ANODE {node_url}
```

例如：

```sql
CREATE ANODE '127.0.0.1:6035';
```

### 查看 Anode

```sql
SHOW ANODES;
SHOW ANODES FULL;
```

### 刷新算法列表缓存

```sql
UPDATE ANODE {anode_id};
UPDATE ALL ANODES;
```

### 删除 Anode

```sql
DROP ANODE {anode_id};
```

:::note

`DROP ANODE` 只会把 Anode 从 TDengine TSDB 集群中移除，不会停止本机服务，也不会卸载本机安装。

:::
