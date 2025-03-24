---
title: "快速体验"
sidebar_label: "快速体验"
---

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

# 通过安装包快速体验

## 获取安装包

1. 从列表中下载获得 tar.gz 安装包：

2. 进入到安装包所在目录，使用 `tar` 解压安装包；
3. 进入到安装包所在目录，先解压文件后，进入子目录，执行其中的 install.sh 安装脚本。

> 请将 `<version>` 替换为下载的安装包版本

```bash
tar -zxvf TDengine-anode-<version>-Linux-x64.tar.gz
```

解压文件后，进入相应子目录，执行其中的 `install.sh` 安装脚本：

```bash
sudo ./install.sh
```

## 安装部署使用

请参考[安装部署指南](./management) 准备环境，并安装部署 TDgpt。

# 通过云服务快速体验

TDgpt 可以在 TDengine Cloud 上进行快速体验。如果您已经有云服务账号，请在数据库集市里面找到“时序数据预测分析数据集”数据库，点击启用就可以进入这个数据库，然后按照 TGgpt 的 SQL 操作手册来执行语句，比如“select forecast(val, 'algo=tdtsfm_1') from forecast.electricity_demand;”。
