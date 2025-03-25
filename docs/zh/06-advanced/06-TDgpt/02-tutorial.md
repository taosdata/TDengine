---
title: "安装部署"
sidebar_label: "安装部署"
---

import PkgListV3 from "/components/PkgListV3";


# 通过 Docker 快速体验

本节首先介绍如何通过 Docker 快速体验 TDgpt。

## 启动 TDgpt

如果已经安装了 Docker，首先拉取最新的 TDengine 容器镜像：

```shell
docker pull tdengine/tdengine:latest
```

或者指定版本的容器镜像：

```shell
docker pull tdengine/tdengine:3.3.3.0
```

然后只需执行下面的命令：

```shell
docker run -d -p 6030:6030 -p 6041:6041 -p 6043:6043 -p 6044-6049:6044-6049 -p 6044-6045:6044-6045/udp -p 6060:6060 tdengine/tdengine
```

注意：TDgpt 服务端使用  6090 TCP 端口。TDgpt 是一个无状态时序数据分析智能体，并不会在本地持久化保存数据，仅根据配置可能在本地生成运行日志。

确定该容器已经启动并且在正常运行。

```shell
docker ps
```

进入该容器并执行 `bash`

```shell
docker exec -it <container name> bash
```

然后就可以执行相关的 Linux 命令操作和访问 TDengine。


# 通过云服务快速体验

TDgpt 可以在 TDengine Cloud 上进行快速体验。如果您已经有云服务账号，请在数据库集市里面找到“时序数据预测分析数据集”数据库，点击启用就可以进入这个数据库，然后按照 TDgpt 的 SQL 操作手册来执行语句，比如 `select forecast(val, 'algo=tdtsfm_1') from forecast.electricity_demand;`。

# 通过安装包快速体验

### 环境准备
使用 TDgpt 的高级时序数据分析功能需要在 TDengine 集群中安装部署 AI node（Anode）。Anode 运行在 Linux 平台上，并需要 3.10 或以上版本的 Python 环境支持。
> 部署 Anode 需要 TDengine 3.3.6.0 及以后版本，请首先确认搭配 Anode 使用的 TDengine 能够支持 Anode。

可以使用以下的命令在 Ubuntu Linux 上安装 Python 3.10 环境

### 安装 Python

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

### 安装 PiPy

```shell
curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10
```

将 `~/.local/bin` 路径添加到环境变量中 `~/.bashrc or ~/.bash_profile`
```shell
export PATH=$PATH:~/.local/bin
```
至此 Python 环境准备完成，可以进行 taosanode 的安装和部署。

### 获取安装包
1. 从列表中下载获得 tar.gz 安装包：

   <PkgListV3 type={9}/>

2. 进入到安装包所在目录，使用 tar 解压安装包；
> 请将 `<version>` 替换为下载的安装包版本

```bash
tar -zxvf TDengine-anode-<version>-Linux-x64.tar.gz
```

3. 解压文件后，进入相应子目录，执行其中的 `install.sh` 安装脚本：
   请将 `<version>` 替换为下载的安装包版本

```bash
cd TDengine-anode-<version>
sudo ./install.sh
```

对于已经安装的 Anode，执行命令 `rmtaosanode` 即可完成卸载。
为了避免影响系统已有的 Python 环境，Anode 使用虚拟环境运行。安装 Anode 会在目录 `/var/lib/taos/taosanode/venv/` 中创建默认的 Python 虚拟环境，Anode 运行所需要的库均安装在该目录下。为了避免反复安装虚拟环境带来的开销，卸载命令 `rmtaosanode` 并不会自动删除该虚拟环境，如果您确认不再需要 Python 的虚拟环境，手动删除该目录即可。
