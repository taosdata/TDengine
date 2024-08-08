---
sidebar_label: Docker
title: 使用 Docker 快速启动 TDengine
toc_max_heading_level: 4
---

本节简介如何使用 Docker 快速启动 TDengine。

## Docker

1. 测试机器如果已经安装了 Docker，首先拉取最新的 TDengine 容器镜像：
```shell
docker pull tdengine/tdengine:latest

或者指定版本的容器镜像：
```shell
docker pull tdengine/tdengine:3.3.0.0
```

2. 然后只需执行下面的命令：
```shell
docker run -d -p 6030:6030 -p 6041:6041 -p 6043-6049:6043-6049 -p 6043-6049:6043-6049/udp tdengine/tdengine
```

**注意**：TDengine 3.0 服务端仅使用 6030 TCP 端口。6041 为 taosAdapter 所使用提供 REST 服务端口。6043-6049 为 taosAdapter 提供第三方应用接入所使用端口，可根据需要选择是否打开。

如果需要将数据持久化到本机的某一个文件夹，则执行下边的命令：
```shell
docker run -d -v ~/data/taos/dnode/data:/var/lib/taos \
  -v ~/data/taos/dnode/log:/var/log/taos \
  -p 6030:6030 -p 6041:6041 -p 6043-6049:6043-6049 -p 6043-6049:6043-6049/udp tdengine/tdengine
```

3. 确定该容器已经启动并且在正常运行。
```shell
docker ps
```

4. 进入该容器并执行 bash
```shell
docker exec -it <container name bash
```

然后就可以执行相关的 Linux 命令操作和访问 TDengine。

## 故障排查

如果启动 TDengine 服务时出现异常，请查看数据库日志以获取更多信息。你也可以参考 TDengine 的官方文档中的故障排除部分，或者在 TDengine 开源社区中寻求帮助。