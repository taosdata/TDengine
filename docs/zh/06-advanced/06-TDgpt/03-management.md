---
title: "运维管理指南"
sidebar_label: "运维管理指南"
---

### 环境准备
使用 TDgpt 的高级时序数据分析功能需要在 TDengine 集群中安装部署 AI node（Anode）。Anode 运行在 Linux 平台上，并需要 3.10 或以上版本的 Python 环境支持。
> 部署 Anode 需要 TDengine 3.3.6.0 及以后版本，请首先确认搭配 Anode 使用的 TDengine 能够支持 Anode。

可以使用以下的命令在 Ubuntu Linux 上安装 Python 3.10 环境

#### 安装 Python

```shell
sudo apt-get install software-properties-common
sudo add-apt-repository ppa:deadsnakes/ppa
sudo apt update
sudo apt install python3.10
sudo update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.10 2
sudo update-alternatives --config python3
sudo apt install python3.10-venv
sudo apt install python3.10-dev
```

#### 安装 Pip
```shell
curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10
```

####
将 `~/.local/bin` 路径添加到环境变量中 `~/.bashrc or ~/.bash_profile`
```shell
export PATH=$PATH:~/.local/bin
```
至此 Python 环境准备完成，可以进行 taosanode 的安装和部署。

### 安装及卸载
使用 Linux 环境下的安装包 TDengine-anode-3.3.x.x-Linux-x64.tar.gz 可进行 Anode 的安装部署工作，命令如下：

```bash
tar -xzvf TDengine-anode-3.3.6.0-Linux-x64.tar.gz
cd TDengine-anode-3.3.6.0
sudo ./install.sh
```

对于已经安装的 Anode，执行命令 `rmtaosanode` 即可完成卸载。
为了避免影响系统已有的 Python 环境，Anode 使用虚拟环境运行。安装 Anode 会在目录 `/var/lib/taos/taosanode/venv/` 中创建默认的 Python 虚拟环境，Anode 运行所需要的库均安装在该目录下。为了避免反复安装虚拟环境带来的开销，卸载命令 `rmtaosanode` 并不会自动删除该虚拟环境，如果您确认不再需要 Python 的虚拟环境，手动删除该目录即可。

### 启停服务
在 Linux 系统中，安装 Anode 以后会自动创建 `taosanoded` 服务。可以使用 `systemd` 来管理 Anode 服务，使用如下命令启动/停止/检查 Anode。

```bash
systemctl start  taosanoded
systemctl stop   taosanoded
systemctl status taosanoded
```

### 目录及配置说明
安装完成后，Anode 主体目录结构如下：

|目录/文件|说明|
|---------------|------|
|/usr/local/taos/taosanode/bin|可执行文件目录|
|/usr/local/taos/taosanode/resource|资源文件目录，链接到文件夹 /var/lib/taos/taosanode/resource/|
|/usr/local/taos/taosanode/lib|库文件目录|
|/usr/local/taos/taosanode/model/|模型文件目录，链接到文件夹 /var/lib/taos/taosanode/model|
|/var/log/taos/taosanode/|日志文件目录|
|/etc/taos/taosanode.ini|配置文件|

#### 配置说明

Anode 的服务需要使用 uWSGI 驱动驱动运行，因此 Anode 和 uWSGI 的配置信息共同存放在相同的配置文件 `taosanode.ini` 中，该配置文件默认位于 `/etc/taos/` 目录下。
具体内容及配置项说明如下：

```ini
[uwsgi]

# Anode RESTful service ip:port
http = 127.0.0.1:6090

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
- log-level: app-log文件的日志级别


### Anode 基本操作
对于 Anode 的管理，用户需要通过 TDengine 的命令行接口 taos 进行。因此下述介绍的管理命令都需要先打开 taos, 连接到 TDengine 运行实例。 
#### 创建 Anode
```sql 
CREATE ANODE {node_url}
```
node_url 是提供服务的 Anode 的 IP 和 PORT组成的字符串, 例如：`create anode '127.0.0.1:6090'`。Anode 启动后还需要注册到 TDengine 集群中才能提供服务。不建议将 Anode 同时注册到两个集群中。

#### 查看 Anode
列出集群中所有的数据分析节点，包括其 `FQDN`, `PORT`, `STATUS`等属性。
```sql
SHOW ANODES;

taos> show anodes;
     id      |              url               |    status    |       create_time       |       update_time       |
==================================================================================================================
           1 | 192.168.0.1:6090               | ready        | 2024-11-28 18:44:27.089 | 2024-11-28 18:44:27.089 |
Query OK, 1 row(s) in set (0.037205s)

```

#### 查看提供的时序数据分析服务

```SQL
SHOW ANODES FULL;

taos> show anodes full;
     id      |            type            |              algo              |
============================================================================
           1 | anomaly-detection          | shesd                          |
           1 | anomaly-detection          | iqr                            |
           1 | anomaly-detection          | ksigma                         |
           1 | anomaly-detection          | lof                            |
           1 | anomaly-detection          | grubbs                         |
           1 | anomaly-detection          | ad_encoder                     |
           1 | forecast                   | holtwinters                    |
           1 | forecast                   | arima                          |
Query OK, 8 row(s) in set (0.008796s)

```

#### 刷新集群中的分析算法缓存
```SQL
UPDATE ANODE {anode_id}
UPDATE ALL ANODES
```

#### 删除 Anode
```sql
DROP ANODE {anode_id}
```
删除 Anode 只是将 Anode 从 TDengine 集群中删除，管理 Anode 的启停仍然需要使用 `systemctl` 命令。卸载 Anode 则需要使用上面提到的 `rmtaosanode` 命令。
