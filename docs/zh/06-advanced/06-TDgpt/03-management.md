---
title: "Anode 管理"
sidebar_label: "Anode 管理"
description: 介绍 Anode 管理指令
---

import PkgListV3 from "/components/PkgListV3";

### 启停 Anode 服务

 Linux 系统中安装 Anode 以后会自动创建 `taosanoded` 服务，用户可使用 `systemd` 来管理 Anode 服务，使用如下命令启动/停止/检查 Anode 运行状态。

```bash
systemctl start  taosanoded
systemctl stop   taosanoded
systemctl status taosanoded
```

### 启停时间序列基础模型服务

提供时序基础模型服务需要占用较大的内存资源。避免启动过程中资源不足导致失败，暂不提供自动启动时间序列基础模型的功能。如果您需要体验时序基础模型服务，需要手动执行如下命令

```bash
# 启动涛思时序数据基础模型
start-tdtsfm
# 启动 Time-MoE 基础模型
start-time-moe
```

```bash
# 停止涛思时序数据基础模型
stop-tdtsfm
# 停止 Time-MoE 基础模型
stop-time-moe
```

> 上述命令只在安装版本中可用，使用 Docker 镜像和云服务，该命令不可用。

### 目录及配置文件说明

安装完成后，Anode 主体目录结构如下：

| 目录/文件                              | 说明                                              |
| ---------------------------------- |-------------------------------------------------|
| /usr/local/taos/taosanode/bin      | 可执行文件 (脚本) 目录                                   |
| /usr/local/taos/taosanode/resource | 资源文件目录，链接到文件夹 /var/lib/taos/taosanode/resource/ |
| /usr/local/taos/taosanode/lib      | 库文件目录                                           |
| /usr/local/taos/taosanode/model/   | 模型文件目录，链接到文件夹 /var/lib/taos/taosanode/model     |
| /var/log/taos/taosanode/           | 日志文件目录                                          |
| /etc/taos/taosanode.ini            | 配置文件                                            |

#### 配置说明

配置文件 `taosanode.ini` 默认位于 `/etc/taos/` 目录下。Anode 的服务默认使用 uWSGI 驱动运行，在配置文件中同时具有 Anode 和 uWSGI 的配置信息

具体内容及配置项说明如下：

```ini
[uwsgi]

# Anode RESTful service ip:port
http = 127.0.0.1:6035

# base directory for Anode python files， do NOT modified this
chdir = /usr/local/taos/taosanode/lib

# initialize Anode python file
wsgi-file = /usr/local/taos/taosanode/lib/taos/app.py

# pid file
pidfile = /usr/local/taos/taosanode/taosanode.pid

# conflict with systemctl, so do NOT uncomment this
# daemonize = /var/log/taos/taosanode/taosanode.log

# uWSGI log files
logto = /var/log/taos/taosanode/taosanode.log

# uWSGI monitor port
stats = 127.0.0.1:8387

# python virtual environment directory, used by Anode
virtualenv = /usr/local/taos/taosanode/venv/

[taosanode]
# default taosanode log file
app-log = /var/log/taos/taosanode/taosanode.app.log

# model storage directory
model-dir = /usr/local/taos/taosanode/model/

# default log level
log-level = INFO
```

**提示**
请勿设置 `daemonize` 参数，该参数会导致 uWSGI 与 systemctl 冲突，从而导致 Anode 无法正常启动。

上面的示例配置文件 `taosanode.ini` 只包含了使用 Anode 提供服务的基础配置参数，对于 uWSGI 的其他配置参数的设置及其说明请参考 [uWSGI 官方文档](https://uwsgi-docs-zh.readthedocs.io/zh-cn/latest/Options.html)。

Anode 运行配置主要是以下：

- app-log: Anode 服务运行产生的日志，用户可以调整其到需要的位置
- model-dir: 采用算法针对已经存在的数据集的运行完成生成的模型存储位置
- log-level: app-log 文件的日志级别。可选的配置选项：DEBUG，INFO，CRITICAL，ERROR，WARN

### Anode 基本操作

用户可通过 TDengine TSDB 的命令行工具 taos 进行 Anode 的管理。执行下述命令都需要确保命令行工具 taos 工作正常。

#### 创建 Anode

```sql
CREATE ANODE {node_url}
```

node_url 是提供服务的 Anode 的 IP 和 PORT 组成的字符串，例如：`create anode '127.0.0.1:6035'`。Anode 启动后需要注册到 TDengine TSDB 集群中才能提供服务。不建议将 Anode 同时注册到两个集群中。

#### 查看 Anode

列出集群中所有的数据分析节点，包括其 `FQDN`, `PORT`, `STATUS`等属性。

```sql
SHOW ANODES;

taos> show anodes;
     id      |              url               |    status    |       create_time       |       update_time       |
==================================================================================================================
           1 | 192.168.0.1:6035               | ready        | 2024-11-28 18:44:27.089 | 2024-11-28 18:44:27.089 |
Query OK, 1 row(s) in set (0.037205s)
```

#### 查看可用分析模型

```SQL
SHOW ANODES FULL;

taos> show anodes full;                                                      
     id      |            type            |              algo              | 
============================================================================ 
           1 | anomaly-detection          | grubbs                         | 
           1 | anomaly-detection          | lof                            | 
           1 | anomaly-detection          | shesd                          | 
           1 | anomaly-detection          | ksigma                         | 
           1 | anomaly-detection          | iqr                            | 
           1 | anomaly-detection          | sample_ad_model                | 
           1 | forecast                   | arima                          | 
           1 | forecast                   | holtwinters                    | 
           1 | forecast                   | tdtsfm_1                       | 
           1 | forecast                   | timemoe-fc                     | 
Query OK, 10 row(s) in set (0.028750s)                                       
```

列表中的算法分为两个部分，分别是异常检测算法集合，包含六个算法模型，四个预测算法集。算法模型如下：

| 类型   | 模型名称            | 说明                  |
| ---- | --------------- | ------------------- |
| 异常检测 | grubbs          | 基于数学统计学检测模型         |
| 异常检测 | lof             | 基于密度的检测模型           |
| 异常检测 | shesd           | 季节性 ESD 算法模型          |
| 异常检测 | ksigma          | 数学统计学检测模型           |
| 异常检测 | iqr             | 数学统计学检测模型           |
| 异常检测 | sample_ad_model | 基于自编码器的异常检测示例模型     |
| 预测分析 | arima           | 移动平均自回归预测算法         |
| 预测分析 | holtwinters     | 多次指数平滑预测算法          |
| 预测分析 | tdtsfm_1        | 涛思时序数据基础模型 v1.0 版本  |
| 预测分析 | timemoe-fc      | Time-MoE 时序基础模型的预测能力 |

相关算法的具体介绍和使用说明见后续章节。

#### 刷新分析算法列表缓存

```SQL
UPDATE ANODE {anode_id}
UPDATE ALL ANODES
```

#### 删除 Anode

```sql
DROP ANODE {anode_id}
```

删除 Anode 只是将 Anode 从 TDengine TSDB 集群中移除，管理 Anode 的启停仍然需要使用 `systemctl` 来操作。卸载 Anode 需要使用 `rmtaosanode` 命令。
