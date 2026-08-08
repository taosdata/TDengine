---
sidebar_label: 用 Docker 快速体验
title: 用 Docker 快速体验 TDengine TSDB
description: 使用 Docker 快速体验 TDengine TSDB 的高效写入和查询
---

本页介绍如何使用 Docker 快速启动 TDengine TSDB Enterprise，并完成一次基础体验：启动服务、进入命令行、写入数据和查询数据。如果你不熟悉 Docker，可以使用 [安装包方式快速体验](./02-package.md)。如果你希望为 TDengine 贡献代码，或对内部技术实现感兴趣，请参考 [TDengine GitHub 主页](https://github.com/taosdata/TDengine)。

:::note
从 3.3.7.0 版本开始，TDengine TSDB 的镜像名称调整如下：

- 社区版的镜像名称从 `tdengine/tdengine` 重命名为 `tdengine/tsdb`
- 企业版的镜像名称从 `tdengine/tdengine-ee` 重命名为 `tdengine/tsdb-ee`

:::

## 前提条件

请先确认已经完成以下准备：

1. 本机已经安装 Docker，并且当前用户可以执行 `docker` 命令。
2. 本机可以访问 Docker Hub，或已经从 TDengine 产品下载中心获取离线镜像。

## 启动服务

### 拉取镜像

拉取最新版本的企业版镜像：

```shell
docker pull tdengine/tsdb-ee:latest
```

也可以拉取指定版本，例如：

```bash tsdb-ee
docker pull tdengine/tsdb-ee:{{VERSION}}
```

如果你无法直接访问 Docker Hub，可以前往 TDengine 产品下载中心的 [Docker 镜像下载页面](https://www.taosdata.com/download-center?product=TDengine+TSDB-Enterprise&platform=Docker)，获取 Docker 镜像下载链接。完成下载后，请根据页面中的离线安装提示加载镜像，并修改镜像名称和标签。

### 启动容器

执行下面的命令启动 TDengine 容器：

```shell
docker run -d \
  -v ~/data/taos/dnode/data:/var/lib/taos \
  -v ~/data/taos/dnode/log:/var/log/taos \
  -p 6030:6030 -p 6041:6041 -p 6043:6043 -p 6060:6060 \
  -p 6044-6049:6044-6049 \
  -p 6044-6045:6044-6045/udp \
  -p 6050:6050 -p 6055:6055 \
  --name tdengine-tsdb \
  tdengine/tsdb-ee
```

关于 TDengine 各服务的端口占用情况，请参考运维指南中的 [网络端口要求](../../12-operations-and-tooling/02-operations/01-planning.md#网络端口要求) 章节。

### 查看容器状态

执行下面的命令查看容器运行状态：

```shell
docker ps -f name=tdengine-tsdb
```

查看命令输出中的 `STATUS` 字段。如果该字段显示 `Up ... (healthy)`，说明容器已经启动并正常运行。

### 进入容器

执行下面的命令进入容器：

```shell
docker exec -it tdengine-tsdb bash
```

进入容器后，就可以执行 Linux 命令，并通过 `taos`、`taosBenchmark` 等工具体验 TDengine。

关于使用 Docker 部署 TDengine 的更多详情，请参考运维指南中的 [Docker 部署](../../12-operations-and-tooling/02-operations/03-deployment/02-docker.md) 章节。

import Getstarted from './resource/_get_started.mdx'

<Getstarted />
