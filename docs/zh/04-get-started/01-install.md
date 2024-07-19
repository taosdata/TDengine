---
sidebar_label: 安装与启动
title: TDengine 安装与启动
description: 使用安装包和 Docker 快速体验 TDengine
toc_max_heading_level: 4
---

TDengine 的安装包含括服务端（taosd）、应用驱动（taosc）、用于与第三方系统对接并提供 RESTful 接口的 taosAdapter、命令行程序（CLI，taos）和一些工具软件。

为了适应不同用户的操作系统偏好，TDengine 在 Linux 系统上提供 tar.gz 、 Deb 和 RPM 格式安装包。此外，还支持 apt-get 方式安装，这种方式简便快捷，适合熟悉 Linux 包管理的用户。

除了 Linux 平台以外，TDengine 还支持在 Windows X64 平台和 macOS X64/M1 平台上安装，扩大了其适用性，满足了跨平台的需求。

对于希望进行虚拟化安装的用户，TDengine 同样提供了 Docker 镜像，使得用户可以快速搭建和体验 TDengine 环境，不需要烦琐的手动配置过程。

本节将详细指导如何在 Linux 操作系统中高效地安装和启动 TDengine 3.3.0.0 版本。同时，为了迎合不同用户的多样化需求，本节还将介绍 TDengine 在 Docker 容器中的安装和启动步骤，为用户提供更多灵活性和便利性选项。

##  Linux 系统

访问 TDengine 的官方版本发布页面：https://docs.taosdata.com/releases/tdengine/ ，下载 TDengine 安装包：TDengine-server-3.3.0.0-Linux-x64.tar.gz 。其他类型安装包的安装方法请参考相关文档，TDengine 遵循各种安装包的标准。

1. 进入到安装包所在目录，使用 tar 解压安装包
```shell
tar -zxvf TDengine-server-3.3.0.0-Linux-x64.tar.gz
```

2. 解压文件后，进入相应子目录 TDengine-server-3.3.0.0，执行其中的 install.sh 安装脚本
```shell
sudo ./install.sh
```

3. 安装后，请使用 systemctl 命令来启动 TDengine 的服务进程。
```shell
sudo systemctl start taosd
```

4. 检查服务是否正常工作：
```shell
sudo systemctl status taosd
```

5. 如果服务进程处于活动状态，则 status 指令会显示如下的相关信息：
```shell
Active: active (running)
```

6. 如果后台服务进程处于停止状态，则 status 指令会显示如下的相关信息：
```shell
Active: inactive (dead)
```

如果 TDengine 服务正常工作，那么您可以通过 TDengine 的命令行程序 taos 来访问并体验 TDengine。

如下 systemctl 命令可以帮助你管理 TDengine 服务：
```shell
1. 启动服务进程：sudo systemctl start taosd
2. 停止服务进程：sudo systemctl stop taosd
3. 重启服务进程：sudo systemctl restart taosd
4. 查看服务状态：sudo systemctl status taosd
```

**注意**：
- 当执行 systemctl stop taosd 命令时，TDengine 服务并不会立即终止，而是会等待必要的数据成功落盘，确保数据的完整性。在处理大量数据的情况下，这一过程可能会花费较长的时间。
- 如果操作系统不支持 systemctl，可以通过手动运行 /usr/local/taos/bin/taosd 命令来启动 TDengine 服务。


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