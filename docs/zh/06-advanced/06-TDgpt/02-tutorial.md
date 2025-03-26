---
title: "安装部署"
sidebar_label: "安装部署"
---

import PkgListV3 from "/components/PkgListV3";


本节介绍如何通过 Docker，云服务或安装包来部署 TDgpt

## 通过 Docker 部署

### 镜像版本说明

| 镜像名称                          | 包含模型               |
|-----------------------------------|-----------------------|
| `tdengine/tdengine-tdgpt`         | 涛思时序数据基础模型（TDtsfm v1.0）       |
| `tdengine/tdengine-tdgpt-full`    | 涛思时序数据基础模型（TDtsfm v1.0）+ Time-MoE 时序数据基础模型   |


### 快速启动指南

您需要先安装 Docker，然后通过以下方式获取镜像并启动容器

#### 标准版镜像

拉取最新的 TDgpt 容器镜像：

```shell
docker pull tdengine/tdengine-tdgpt:latest
```

或者指定版本的容器镜像：

```shell
docker pull tdengine/tdengine-tdgpt:3.3.6.0
```

执行下面的命令来启动容器：

```shell
docker run -d -p 6090:6090 -p 5000:5000 tdengine/tdengine-tdgpt:3.3.6.0
```

#### 完整版镜像

拉取最新的 TDgpt 容器镜像：

```shell
docker pull tdengine/tdengine-tdgpt-full:latest
```

或者指定版本的容器镜像：

```shell
docker pull tdengine/tdengine-tdgpt-full:3.3.6.0
```

执行下面的命令来启动容器：

```shell
docker run -d -p 6090:6090 -p 5000:5000 -p 5001:5001 tdengine/tdengine-tdgpt-full:3.3.6.0
```

注意：TDgpt 服务端使用 6090 TCP 端口，5000 和 5001 端口分别标准版模型和完整版模型的服务端口；
TDgpt 是一个无状态时序数据分析智能体，并不会在本地持久化保存数据，仅根据配置可能在本地生成运行日志。

确定该容器已经启动并且在正常运行。

```shell
docker ps
```

进入该容器并执行 `bash`

```shell
docker exec -it <container name> bash
```

然后就可以执行相关的 Linux 命令操作和访问 TDengine。


## 注册云服务使用 TDgpt

TDgpt 可以在 TDengine Cloud 上进行快速体验。如果您已经有云服务账号，登录云服务后，点击屏幕左下侧的 "数据库集市"，请在数据库集市里面找到 **时序数据预测分析数据集** 数据库，点击启用就可以进入这个数据库。然后按照 TDgpt 的 SQL 操作手册来执行语句，例如 `select forecast(val, 'algo=tdtsfm_1') from forecast.electricity_demand;`

## 通过安装包部署 TDgpt

### 环境准备

使用 TDgpt 的高级时序数据分析功能需要在 TDengine 集群中安装部署 Taos AI node（Anode）。Anode 运行在 Linux 平台上, 对部署 Anode 的有一定的环境要求：

- Python: 3.10 或以上版本。
- TDengine：需使用 3.3.6.0 或以上版本。
- C 编译器：因依赖 uWSGI，部署环境需包含 C 编译器。

使用如下命令在 Ubuntu Linux 上安装 Python 3.10 环境。如果您的系统环境中已经有 Python 3.10，请跳过本节，直接查看 [获取安装包](#获取安装包) 部分。

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

#### 安装 PiPy

```shell
curl -sS https://bootstrap.pypa.io/get-pip.py | python3.10
```

将 `~/.local/bin` 路径添加到环境变量中 `~/.bashrc or ~/.bash_profile`。

```shell
export PATH=$PATH:~/.local/bin
```
至此 Python 环境准备完成，可以进行 taosanode 的安装和部署。

#### 安装 C 编译器（按需安装）

```shell 
sudo apt update
sudo apt install build-essential
```

### 获取安装包
1. 从列表中下载获得 tar.gz 安装包

   <PkgListV3 type={9}/>

   安装包中包含两个时序基础模型：涛思时序基础模型（TDtsfm v1.0）和 Time-MoE 时序基础模型。两个基础时序模型启动时候需要一定的内存空间，请确保安装机器至少有 16GiB 可用内存。
  
2. 进入到安装包所在目录，使用 tar 解压安装包；
   
> 请将 `<version>` 替换为下载的安装包版本

```bash
tar -zxvf TDengine-TDgpt-<version>-Linux-x64.tar.gz
```

### 执行安装脚本

解压文件后，进入相应解压文件目录，执行其中 `install.sh` 安装脚本：
请将 `<version>` 替换为下载的安装包版本

```bash
cd TDengine-TDgpt-<version>
./install.sh
```

为了避免影响系统已有的 Python 环境，Anode 使用虚拟环境运行。安装 Anode 会在目录 `/var/lib/taos/taosanode/venv/` 中创建默认的 Python 虚拟环境，Anode 运行所需要的库均安装在该目录下。
> 为了避免反复安装虚拟环境带来的开销，卸载命令 `rmtaosanode` 并不会自动删除该虚拟环境，如果您确认不再需要该 Python 虚拟环境，手动删除该目录即可。

### 激活虚拟环境

为了避免安装操作系统的 Python 环境， TDgpt 安装过程中会自动创建一个虚拟环境，该虚拟环境默认创建的路径在 `/var/lib/taos/taosanode/venv/`。创建完成该虚拟环境，该虚拟环境通过 PiPy 安装了支持 TDgpt 运行所必须的 Python 依赖库。
该虚拟环境不会被卸载脚本 `rmtaosanode` 删除，当您确认不再需要该虚拟环境的时候，需要手动删除该虚拟环境。
后续如果您需要开发自己的算法模型，并能够 TDgpt 正确调用，需要将新的依赖库通过虚拟环境的 Pip 正确地安装。


### 卸载
卸载 TDgpt，执行 `rmtaosanode` 即可。 
