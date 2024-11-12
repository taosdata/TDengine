---
title: "安装部署"
sidebar_label: "安装部署"
---

### 环境准备
ANode 可以运行在Linux/Windows/Mac 操作系统之上，同时要求部署 Anode 的节点安装有 3.10 及以上版本的 Python，以及相应的 Python 包自动安装组件 Pip，同时请确保能够正常连接互联网。

### 安装及卸载
使用安装包 TDengine-enterprise-anode-1.x.x.tar.gz 进行 ANode 的安装部署工作，主要操作流程如下：

```bash
tar -xzvf TDengine-enterprise-anode-1.0.0.tar.gz
cd TDengine-enterprise-anode-1.0.0
sudo ./install.sh
```

在安装完成 ANode 之后，执行命令 `rmtaosanode` 即可。
ANode 使用 Python 虚拟环境运行，避免影响安装环境中现有的 Python 库。安装后的默认 Python 虚拟环境目录位于 `/var/lib/taos/taosanode/venv/`。为了避免反复安装虚拟环境带来的开销，卸载 ANode 执行的命令 `rmtaosanode` 并不会自动删除该虚拟环境，如果您确认不需要 Python 的虚拟环境，手动删除即可。

### 启停服务
安装 ANode 以后，可以使用 `systemctl` 来管理 ANode 的服务。使用如下命令可以启动/停止/检查状态。

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

# Anode http service ip:port
http = 127.0.0.1:6050

# base directory for python files
chdir = /usr/local/taos/taosanode/lib

# initialize python file
wsgi-file = /usr/local/taos/taosanode/lib/taos/app.py

# call module of uWSGI
callable = app

# pid file
pidfile = /usr/local/taos/taosanode/taosanode.pid

# conflict with systemctl, so do NOT uncomment this
# daemonize = /var/log/taos/taosanode/taosanode.log

# log directory
logto = /var/log/taos/taosanode/taosanode.log

# wWSGI monitor port
stats = 127.0.0.1:8387

# python virtual environment directory
virtualenv = /usr/local/taos/taosanode/venv/

[taosanode]
# default app log file
app-log = /var/log/taos/taosanode/taosanode.app.log

# model storage directory
model-dir = /usr/local/taos/taosanode/model/

# default log level
log-level = DEBUG

# draw the query results
draw-result = 0
```

**提示**
请勿设置 `daemonize` 参数，该参数会导致 uWSGI 与 systemctl 冲突，从而无法正常启动。
该配置文件只包含了使用 Anode提供服务的最基础的配置参数，对于 uWSGI 的其他配置参数设置请参考[uWSGIS官方文档](https://uwsgi-docs-zh.readthedocs.io/zh-cn/latest/Options.html)。


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

#### 强制刷新集群中的分析算法缓存
```SQL
UPDATE ANODE {node_id}
UPDATE ALL ANODES
```

#### 删除 ANode
```sql
DROP ANODE {anode_id}
```
删除 ANode 只是将 ANode 从 TDengine 集群中删除，管理 ANode 的启停仍然需要使用`systemctl`命令。
