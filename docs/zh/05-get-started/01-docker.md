---
sidebar_label: Docker
title: 通过 Docker 快速体验 TDengine
---

本节首先介绍如何通过 Docker 快速体验 TDengine，然后介绍如何在 Docker 环境下体验 TDengine 的写入和查询功能。

## 启动 TDengine

如果已经安装了 docker， 只需执行下面的命令。

```shell
docker run -d -p 6030-6049:6030-6049 -p 6030-6049:6030-6049/udp tdengine/tdengine
```

确定该容器已经启动并且在正常运行

```shell
docker ps
```

进入该容器并执行 bash

```shell
docker exec -it <container name> bash
```

然后就可以执行相关的 Linux 命令操作和访问 TDengine

:::info

Docker 工具自身的下载请参考 [Docker 官网文档](https://docs.docker.com/get-docker/)。

安装完毕后可以在命令行终端查看 Docker 版本。如果版本号正常输出，则说明 Docker 环境已经安装成功。

```bash
$ docker -v
Docker version 20.10.3, build 48d30b5
```

:::

## 运行 TDengine CLI

有两种方式在 Docker 环境下使用 TDengine CLI (taos) 访问 TDengine. 
- 进入容器后，执行 taos 
- 在宿主机使用容器映射到主机的端口进行访问 `taos -h <hostname> -P <port>`

```
$ taos
Welcome to the TDengine shell from Linux, Client Version:3.0.0.0
Copyright (c) 2022 by TAOS Data, Inc. All rights reserved.

Server is Enterprise   trial Edition, ver:3.0.0.0 and will expire at 2022-09-24 15:29:46.

taos> 

```


## 启动 REST 服务

taosAdapter 是 TDengine 中提供 REST 服务的组件。下面这条命令会在容器中同时启动 `taosd` 和 `taosadapter` 两个服务组件。

```bash
docker run -d --name tdengine-all -p 6030-6049:6030-6049 -p 6030-6049:6030-6049/udp tdengine/tdengine
```

如果想只启动 `taosadapter`：

```bash
docker run -d --name tdengine-taosa -p 6041-6049:6041-6049 -p 6041-6049:6041-6049/udp -e TAOS_FIRST_EP=tdengine-all tdengine/tdengine:3.0.0.0 taosadapter
```

如果想只启动 `taosd`：

```bash
docker run -d --name tdengine-taosd -p 6030-6042:6030-6042 -p 6030-6042:6030-6042/udp -e TAOS_DISABLE_ADAPTER=true tdengine/tdengine:3.0.0.0
```

## 访问 REST 接口

可以在宿主机使用 curl 通过 RESTful 端口访问 Docker 容器内的 TDengine server。

```
curl -L -u root:taosdata -d "show databases" 127.0.0.1:6041/rest/sql
```

输出示例如下：

```
{"code":0,"column_meta":[["name","VARCHAR",64],["create_time","TIMESTAMP",8],["vgroups","SMALLINT",2],["ntables","BIGINT",8],["replica","TINYINT",1],["strict","VARCHAR",4],["duration","VARCHAR",10],["keep","VARCHAR",32],["buffer","INT",4],["pagesize","INT",4],["pages","INT",4],["minrows","INT",4],["maxrows","INT",4],["wal","TINYINT",1],["fsync","INT",4],["comp","TINYINT",1],["cacheModel","VARCHAR",11],["precision","VARCHAR",2],["single_stable","BOOL",1],["status","VARCHAR",10],["retention","VARCHAR",60]],"data":[["information_schema",null,null,14,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,"ready"],["performance_schema",null,null,3,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,"ready"]],"rows":2}
```

这条命令，通过 REST API 访问 TDengine server，这时连接的是从容器映射到主机的 6041 端口。

TDengine REST API 详情请参考[官方文档](/reference/rest-api/)。

## 写入数据

可以使用 TDengine 的自带工具 taosBenchmark 快速体验 TDengine 的写入。

假定启动容器时已经将容器的6030端口映射到了宿主机的6030端口，则可以直接在宿主机命令行启动 taosBenchmark，也可以进入容器后执行：

   ```bash
   $ taosBenchmark
   
   ```

   该命令将在数据库 test 下面自动创建一张超级表 meters，该超级表下有 1 万张表，表名为 "d0" 到 "d9999"，每张表有 1 万条记录，每条记录有 (ts, current, voltage, phase) 四个字段，时间戳从 "2017-07-14 10:40:00 000" 到 "2017-07-14 10:40:09 999"，每张表带有标签 location 和 groupId，groupId 被设置为 1 到 10， location 被设置为 "California.SanFrancisco" 或者 "California.LosAngeles"。

   这条命令很快完成 1 亿条记录的插入。具体时间取决于硬件性能。

   taosBenchmark 命令本身带有很多选项，配置表的数目、记录条数等等，您可以设置不同参数进行体验，请执行 `taosBenchmark --help` 详细列出。taosBenchmark 详细使用方法请参照 [taosBenchmark 参考手册](../reference/taosbenchmark)。

## 体验查询

使用上述 taosBenchmark 插入数据后，可以在 TDengine CLI 输入查询命令，体验查询速度。可以直接在宿主机上也可以进入容器后运行。

查询超级表下记录总条数：

```sql
taos> select count(*) from test.meters;
```

查询 1 亿条记录的平均值、最大值、最小值等：

```sql
taos> select avg(current), max(voltage), min(phase) from test.meters;
```

查询 location="California.SanFrancisco" 的记录总条数：

```sql
taos> select count(*) from test.meters where location="California.SanFrancisco";
```

查询 groupId=10 的所有记录的平均值、最大值、最小值等：

```sql
taos> select avg(current), max(voltage), min(phase) from test.meters where groupId=10;
```

对表 d10 按 10s 进行平均值、最大值和最小值聚合统计：

```sql
taos> select avg(current), max(voltage), min(phase) from test.d10 interval(10s);
```