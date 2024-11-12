---
title: "安装部署"
sidebar_label: "安装部署"
---

### 环境准备
ANode 可以运行在 Linux/Windows/Mac 操作系统之上，要求部署 Anode 的节点安装有 3.10 及以上版本的Python环境，以及相应的 Python 包自动安装组件 Pip。

### 安装及卸载
不同操作系统上安装及部署操作有差异，主要包括安装/卸载操作、安装路径、Anode服务的启停等几个方面。本小节以 Linux 系统为例，说明安装部署的整个流程。使用Linux环境下的安装包 TDengine-enterprise-anode-1.x.x.tar.gz 可进行 ANode 的安装部署工作，使用如下命令：

```bash
tar -xzvf TDengine-enterprise-anode-1.0.0.tar.gz
cd TDengine-enterprise-anode-1.0.0
sudo ./install.sh
```

在安装完成 ANode 之后，执行命令 `rmtaosanode` 即可。
ANode 使用 Python 虚拟环境运行，避免影响安装环境中现有的 Python 库。安装后的默认 Python 虚拟环境目录位于 `/var/lib/taos/taosanode/venv/`。为了避免反复安装虚拟环境带来的开销，卸载 ANode 执行的命令 `rmtaosanode` 并不会自动删除该虚拟环境，如果您确认不需要 Python 的虚拟环境，手动删除即可。

### 启停服务
在 Linux 系统中，安装 ANode 以后可以使用 `systemd` 来管理 ANode 服务。使用如下命令可以启动/停止/检查状态。

```bash
systemctl start  taosanoded
systemctl stop   taosanoded
systemctl status taosanoded
```

### 目录及配置说明
|目录/文件|说明|
|---------------|------|
|/usr/local/taos/taosanode/bin|可执行文件目录|
|/usr/local/taos/taosanode/resource|资源文件目录，链接到文件夹 /var/lib/taos/taosanode/resource/|
|/usr/local/taos/taosanode/lib|库文件目录|
|/var/lib/taos/taosanode/model/|模型文件目录，链接到文件夹 /var/lib/taos/taosanode/model|
|/var/log/taos/taosanode/|日志文件目录|
|/etc/taos/taosanode.ini|配置文件|

#### 配置说明

Anode 提供的服务使用 uWSGI 驱动，因此 ANode 和 uWSGI 的配置信息共同存放在相同的配置文件 `taosanode.ini`，该配置文件默认位于 `/etc/taos/`目录下，其具体内容及说明如下：

```ini
[uwsgi]

# Anode HTTP service ip:port
http = 127.0.0.1:6050

# base directory for Anode python files， do NOT modified this
chdir = /usr/local/taos/taosanode/lib

# initialize Anode python file
wsgi-file = /usr/local/taos/taosanode/lib/taos/app.py

# pid file
pidfile = /usr/local/taos/taosanode/taosanode.pid

# conflict with systemctl, so do NOT uncomment this
# daemonize = /var/log/taos/taosanode/taosanode.log

# log directory
logto = /var/log/taos/taosanode/taosanode.log

# wWSGI monitor port
stats = 127.0.0.1:8387

# python virtual environment directory, used by Anode
virtualenv = /usr/local/taos/taosanode/venv/

[taosanode]
# default app log file
app-log = /var/log/taos/taosanode/taosanode.app.log

# model storage directory
model-dir = /usr/local/taos/taosanode/model/

# default log level
log-level = DEBUG

```

**提示**
请勿设置 `daemonize` 参数，该参数会导致 uWSGI 与 systemctl 冲突，从而无法正常启动。
该配置文件只包含了使用 Anode提供服务的最基础的配置参数，对于 uWSGI 的其他配置参数设置及其含义和说明请参考[uWSGIS官方文档](https://uwsgi-docs-zh.readthedocs.io/zh-cn/latest/Options.html)。
对于 Anode 运行配置主要是以下几个：
- app-log: Anode 服务运行产生的日志，用户可以调整其到需要的位置
- model-dir: 采用算法针对已经存在的数据集的运行完成生成的模型存储位置
- log-level: app-log文件的日志级别


### ANode 基本操作
#### 创建 ANode
```sql 
CREATE ANODE {node_url}
```
node_url 是提供服务的 ANode 的 IP 和 PORT, 例如：`create anode 'http://localhost:6050'`。启动 ANode 以后如果不注册到 TDengine 集群中，则无法提供正常的服务。不建议 ANode 注册到两个或多个集群中。

#### 查看 ANode
列出集群中所有的数据分析节点，包括其 `FQDN`, `PORT`, `STATUS`。
```sql
SHOW ANODES;
```

#### 查看提供的时序数据分析服务

```SQL
SHOW ANODES FULL;
```

#### 刷新集群中的分析算法缓存
```SQL
UPDATE ANODE {node_id}
UPDATE ALL ANODES
```

#### 删除 ANode
```sql
DROP ANODE {anode_id}
```
删除 ANode 只是将 ANode 从 TDengine 集群中删除，管理 ANode 的启停仍然需要使用`systemctl`命令。
