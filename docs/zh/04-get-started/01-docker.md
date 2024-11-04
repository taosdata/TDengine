---
sidebar_label: 用Docker快速体验
title: 用 Docker 快速体验 TDengine
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
docker pull tdengine/tdengine:3.3.3.0
```

然后只需执行下面的命令：

```shell
docker run -d -p 6030:6030 -p 6041:6041 -p 6043:6043 -p 6044-6049:6044-6049 -p 6044-6045:6044-6045/udp -p 6060:6060 tdengine/tdengine
```

注意：TDengine 3.0 服务端仅使用 6030 TCP 端口。6041 为 taosAdapter 所使用提供 REST 服务端口。6043 为 taosKeeper 使用端口。6044-6049 TCP 端口为 taosAdapter 提供第三方应用接入所使用端口，可根据需要选择是否打开。
6044 和 6045 UDP 端口为 statsd 和 collectd 格式写入接口，可根据需要选择是否打开。6060 为 taosExplorer 使用端口。具体端口使用情况请参考[网络端口要求](../../operation/planning#网络端口要求)。

如果需要将数据持久化到本机的某一个文件夹，则执行下边的命令：

```shell
docker run -d -v ~/data/taos/dnode/data:/var/lib/taos \
  -v ~/data/taos/dnode/log:/var/log/taos \
  -p 6030:6030 -p 6041:6041 -p 6043:6043 -p 6044-6049:6044-6049 -p 6044-6045:6044-6045/udp -p 6060:6060 tdengine/tdengine
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

### 体验写入

taosBenchmark 是一个专为测试 TDengine 性能而设计的工具，它能够全面评估TDengine 在写入、查询和订阅等方面的功能表现。该工具能够模拟大量设备产生的数据，并允许用户灵活控制数据库、超级表、标签列的数量和类型、数据列的数量和类型、子表数量、每张子表的数据量、写入数据的时间间隔、工作线程数量以及是否写入乱序数据等策略。

启动 TDengine 的服务，在终端中执行如下命令

```shell
taosBenchmark -y
```

系统将自动在数据库 test 下创建一张名为 meters的超级表。这张超级表将包含 10,000 张子表，表名从 d0 到 d9999，每张表包含 10,000条记录。每条记录包含 ts（时间戳）、current（电流）、voltage（电压）和 phase（相位）4个字段。时间戳范围从“2017-07-14 10:40:00 000” 到 “2017-07-14 10:40:09 999”。每张表还带有 location 和 groupId 两个标签，其中，groupId 设置为 1 到 10，而 location 则设置为 California.Campbell、California.Cupertino 等城市信息。

执行该命令后，系统将迅速完成 1 亿条记录的写入过程。实际所需时间取决于硬件性能，但即便在普通 PC 服务器上，这个过程通常也只需要十几秒。

taosBenchmark 提供了丰富的选项，允许用户自定义测试参数，如表的数目、记录条数等。要查看详细的参数列表，请在终端中输入如下命令
```shell
taosBenchmark --help
```

有关taosBenchmark 的详细使用方法，请参考[taosBenchmark 参考手册](../../reference/tools/taosbenchmark)

### 体验查询

使用上述 taosBenchmark 插入数据后，可以在 TDengine CLI（taos）输入查询命令，体验查询速度。

1. 查询超级表 meters 下的记录总条数
```shell
SELECT COUNT(*) FROM test.meters;
```

2. 查询 1 亿条记录的平均值、最大值、最小值
```shell
SELECT AVG(current), MAX(voltage), MIN(phase) FROM test.meters;
```

3. 查询 location = "California.SanFrancisco" 的记录总条数
```shell
SELECT COUNT(*) FROM test.meters WHERE location = "California.SanFrancisco";
```

4. 查询 groupId = 10 的所有记录的平均值、最大值、最小值
```shell
SELECT AVG(current), MAX(voltage), MIN(phase) FROM test.meters WHERE groupId = 10;
```

5. 对表 d1001 按每 10 秒进行平均值、最大值和最小值聚合统计
```shell
SELECT _wstart, AVG(current), MAX(voltage), MIN(phase) FROM test.d1001 INTERVAL(10s);
```

在上面的查询中，使用系统提供的伪列_wstart 来给出每个窗口的开始时间。
