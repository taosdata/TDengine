---
sidebar_label: Docker
title: 通过 Docker 快速体验 TDengine
description: 使用 Docker 快速体验 TDengine 的高效写入和查询
---

本节首先介绍如何通过 Docker 快速体验 TDengine，然后介绍如何在 Docker 环境下体验 TDengine 的写入和查询功能。如果你不熟悉 Docker，请使用[安装包的方式快速体验](../../get-started/package/)。如果您希望为 TDengine 贡献代码或对内部技术实现感兴趣，请参考 [TDengine GitHub 主页](https://github.com/taosdata/TDengine)下载源码构建和安装。

## 启动 TDengine

如果已经安装了 Docker，首先拉取最新的 TDengine 容器镜像：

```shell
docker pull tdengine/tdengine:latest
```

或者指定版本的容器镜像：

```shell
docker pull tdengine/tdengine:3.0.1.4
```

然后只需执行下面的命令：

```shell
docker run -d -p 6030:6030 -p 6041:6041 -p 6043-6060:6043-6060 -p 6043-6060:6043-6060/udp tdengine/tdengine
```

注意：TDengine 3.0 服务端仅使用 6030 TCP 端口。6041 为 taosAdapter 所使用提供 REST 服务端口。6043-6049 为 taosAdapter 提供第三方应用接入所使用端口，可根据需要选择是否打开。

如果需要将数据持久化到本机的某一个文件夹，则执行下边的命令：

```shell
docker run -d -v ~/data/taos/dnode/data:/var/lib/taos \
  -v ~/data/taos/dnode/log:/var/log/taos \
  -p 6030:6030 -p 6041:6041 -p 6043-6060:6043-6060 -p 6043-6060:6043-6060/udp tdengine/tdengine
```

:::note

- /var/lib/taos: TDengine 默认数据文件目录。可通过[配置文件]修改位置。你可以修改~/data/taos/dnode/data为你自己的数据目录
- /var/log/taos: TDengine 默认日志文件目录。可通过[配置文件]修改位置。你可以修改~/data/taos/dnode/log为你自己的日志目录
  
:::

确定该容器已经启动并且在正常运行。

```shell
docker ps
```

进入该容器并执行 `bash`

```shell
docker exec -it <container name> bash
```

然后就可以执行相关的 Linux 命令操作和访问 TDengine。

注：Docker 工具自身的下载和使用请参考 [Docker 官网文档](https://docs.docker.com/get-docker/)。

## TDengine 命令行界面

进入容器，执行 `taos`：

```
$ taos

taos>
```

## 快速体验

想要快速体验 TDengine 的写入和查询能力，请参考[快速体验](../use)