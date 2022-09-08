---
sidebar_label: Docker
title: 通过 Docker 快速体验 TDengine
description: 使用 Docker 快速体验 TDengine 的高效写入和查询
---

本节首先介绍如何通过 Docker 快速体验 TDengine，然后介绍如何在 Docker 环境下体验 TDengine 的写入和查询功能。如果你不熟悉 Docker，请使用[安装包的方式快速体验](../../get-started/package/)。如果您希望为 TDengine 贡献代码或对内部技术实现感兴趣，请参考 [TDengine GitHub 主页](https://github.com/taosdata/TDengine) 下载源码构建和安装.

## 启动 TDengine

如果已经安装了 docker， 只需执行下面的命令。

```shell
docker run -d -p 6030:6030 -p 6041:6041 -p 6043-6049:6043-6049 -p 6043-6049:6043-6049/udp tdengine/tdengine
```

注意：TDengine 3.0 服务端仅使用 6030 TCP 端口。6041 为 taosAdapter 所使用提供 REST 服务端口。6043-6049 为 taosAdapter 提供第三方应用接入所使用端口，可根据需要选择是否打开。

确定该容器已经启动并且在正常运行

```shell
docker ps
```

进入该容器并执行 bash

```shell
docker exec -it <container name> bash
```

然后就可以执行相关的 Linux 命令操作和访问 TDengine

注: Docker 工具自身的下载和使用请参考 [Docker 官网文档](https://docs.docker.com/get-docker/)。

## 运行 TDengine CLI

进入容器，执行 taos 

```
$ taos

taos> 

```

## 写入数据

可以使用 TDengine 的自带工具 taosBenchmark 快速体验 TDengine 的写入。

进入容器，启动 taosBenchmark：

   ```bash
   $ taosBenchmark
   
   ```

   该命令将在数据库 test 下面自动创建一张超级表 meters，该超级表下有 1 万张表，表名为 "d0" 到 "d9999"，每张表有 1 万条记录，每条记录有 (ts, current, voltage, phase) 四个字段，时间戳从 "2017-07-14 10:40:00 000" 到 "2017-07-14 10:40:09 999"，每张表带有标签 location 和 groupId，groupId 被设置为 1 到 10， location 被设置为 "San Francisco" 或者 "Los Angeles"等城市名称。

   这条命令很快完成 1 亿条记录的插入。具体时间取决于硬件性能。

   taosBenchmark 命令本身带有很多选项，配置表的数目、记录条数等等，您可以设置不同参数进行体验，请执行 `taosBenchmark --help` 详细列出。taosBenchmark 详细使用方法请参照 [taosBenchmark 参考手册](../../reference/taosbenchmark)。

## 体验查询

使用上述 taosBenchmark 插入数据后，可以在 TDengine CLI 输入查询命令，体验查询速度。。

查询超级表下记录总条数：

```sql
taos> select count(*) from test.meters;
```

查询 1 亿条记录的平均值、最大值、最小值等：

```sql
taos> select avg(current), max(voltage), min(phase) from test.meters;
```

查询 location="San Francisco" 的记录总条数：

```sql
taos> select count(*) from test.meters where location="San Francisco";
```

查询 groupId=10 的所有记录的平均值、最大值、最小值等：

```sql
taos> select avg(current), max(voltage), min(phase) from test.meters where groupId=10;
```

对表 d10 按 10s 进行平均值、最大值和最小值聚合统计：

```sql
taos> select avg(current), max(voltage), min(phase) from test.d10 interval(10s);
```

## 其它

更多关于在 Docker 环境下使用 TDengine 的细节，请参考 [在 Docker 下使用 TDengine](../../reference/docker)
