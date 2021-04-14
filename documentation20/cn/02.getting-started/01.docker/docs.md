# 通过 Docker 快速体验 TDengine

虽然并不推荐在生产环境中通过 Docker 来部署 TDengine 服务，但 Docker 工具能够很好地屏蔽底层操作系统的环境差异，很适合在开发测试或初次体验时用于安装运行 TDengine 的工具集。特别是，借助 Docker，能够比较方便地在 Mac OSX 和 Windows 系统上尝试 TDengine，而无需安装虚拟机或额外租用 Linux 服务器。

下文通过 Step by Step 风格的介绍，讲解如何通过 Docker 快速建立 TDengine 的单节点运行环境，以支持开发和测试。

## 下载 Docker

Docker 工具自身的下载请参考 [Docker官网文档](https://docs.docker.com/get-docker/)。

安装完毕后可以在命令行终端查看 Docker 版本。如果版本号正常输出，则说明 Docker 环境已经安装成功。

```bash
$ docker -v
Docker version 20.10.5, build 55c4c88
```

## 在 Docker 容器中运行 TDengine

1，使用命令拉取 TDengine 镜像，并使它在后台运行。

```bash
$ docker run -d tdengine/tdengine
cdf548465318c6fc2ad97813f89cc60006393392401cae58a27b15ca9171f316
```

- **docker run**：通过 Docker 运行一个容器。
- **-d**：让容器在后台运行。
- **tdengine/tdengine**：拉取的 TDengine 官方发布的应用镜像。
- **cdf548465318c6fc2ad97813f89cc60006393392401cae58a27b15ca9171f316**：这个返回的长字符是容器 ID，我们可以通过容器 ID 来查看对应的容器。

2，确认容器是否已经正确运行。

```bash
$ docker ps
CONTAINER ID   IMAGE               COMMAND   CREATED          STATUS          ···
cdf548465318   tdengine/tdengine   "taosd"   14 minutes ago   Up 14 minutes   ···
```

- **docker ps**：列出所有正在运行状态的容器信息。
- **CONTAINER ID**：容器 ID。
- **IMAGE**：使用的镜像。
- **COMMAND**：启动容器时运行的命令。
- **CREATED**：容器创建时间。
- **STATUS**：容器状态。UP 表示运行中。

3，进入 Docker 容器内，使用 TDengine。

```bash
$ docker exec -it cdf548465318 /bin/bash
root@cdf548465318:~/TDengine-server-2.0.13.0#
```

- **docker exec**：通过 docker exec 命令进入容器，如果退出，容器不会停止。
- **-i**：进入交互模式。
- **-t**：指定一个终端。
- **cdf548465318**：容器 ID，需要根据 docker ps 指令返回的值进行修改。
- **/bin/bash**：载入容器后运行 bash 来进行交互。

4，进入容器后，执行 taos shell 客户端程序。

```bash
$ root@cdf548465318:~/TDengine-server-2.0.13.0# taos

Welcome to the TDengine shell from Linux, Client Version:2.0.13.0
Copyright (c) 2020 by TAOS Data, Inc. All rights reserved.

taos>
```

TDengine 终端成功连接服务端，打印出了欢迎消息和版本信息。如果失败，会有错误信息打印出来。

在 TDengine 终端中，可以通过 SQL 命令来创建/删除数据库、表、超级表等，并可以进行插入和查询操作。具体可以参考 [TAOS SQL 说明文档](https://www.taosdata.com/cn/documentation/taos-sql)。

## 通过 taosdemo 进一步了解 TDengine

1，接上面的步骤，先退出 TDengine 终端程序。

```bash
$ taos> q
root@cdf548465318:~/TDengine-server-2.0.13.0#
```

2，在命令行界面执行 taosdemo。

```bash
$ root@cdf548465318:~/TDengine-server-2.0.13.0# taosdemo
###################################################################
# Server IP:                         localhost:0
# User:                              root
# Password:                          taosdata
# Use metric:                        true
# Datatype of Columns:               int int int int int int int float
# Binary Length(If applicable):      -1
# Number of Columns per record:      3
# Number of Threads:                 10
# Number of Tables:                  10000
# Number of Data per Table:          100000
# Records/Request:                   1000
# Database name:                     test
# Table prefix:                      t
# Delete method:                     0
# Test time:                         2021-04-13 02:05:20
###################################################################
```

回车后，该命令将新建一个数据库 test，并且自动创建一张超级表 meters，并以超级表 meters 为模版创建了 1 万张表，表名从 "t0" 到 "t9999"。每张表有 10 万条记录，每条记录有 f1，f2，f3 三个字段，时间戳 ts 字段从 "2017-07-14 10:40:00 000" 到 "2017-07-14 10:41:39 999"。每张表带有 areaid 和 loc 两个标签 TAG，areaid 被设置为 1 到 10，loc 被设置为 "beijing" 或 "shanghai"。

3，进入 TDengine 终端，查看 taosdemo 生成的数据。

- **进入命令行。**

```bash
$ root@cdf548465318:~/TDengine-server-2.0.13.0# taos

Welcome to the TDengine shell from Linux, Client Version:2.0.13.0
Copyright (c) 2020 by TAOS Data, Inc. All rights reserved.

taos>
```

- **查看数据库。**

```bash
$ taos> show databases;
  name        |      created_time       |   ntables   |   vgroups   |    ···
  test        | 2021-04-13 02:14:15.950 |       10000 |           6 |    ···
  log         | 2021-04-12 09:36:37.549 |           4 |           1 |	   ···

```

- **查看超级表。**

```bash
$ taos> use test;
Database changed.

$ taos> show stables;
       name              |      created_time       | columns |  tags  |   tables    |
=====================================================================================
       meters            | 2021-04-13 02:14:15.955 |       4 |      2 |       10000 |
Query OK, 1 row(s) in set (0.001737s)

```

- **查看表，限制输出十条。**

```bash
$ taos> select * from test.t0 limit 10;
           ts            |     f1      |     f2      |     f3      |
====================================================================
 2017-07-14 02:40:01.000 |           3 |           9 |           0 |
 2017-07-14 02:40:02.000 |           0 |           1 |           2 |
 2017-07-14 02:40:03.000 |           7 |           2 |           3 |
 2017-07-14 02:40:04.000 |           9 |           4 |           5 |
 2017-07-14 02:40:05.000 |           1 |           2 |           5 |
 2017-07-14 02:40:06.000 |           6 |           3 |           2 |
 2017-07-14 02:40:07.000 |           4 |           7 |           8 |
 2017-07-14 02:40:08.000 |           4 |           6 |           6 |
 2017-07-14 02:40:09.000 |           5 |           7 |           7 |
 2017-07-14 02:40:10.000 |           1 |           5 |           0 |
Query OK, 10 row(s) in set (0.003638s)

```

- **查看 t0 表的标签值。**

```bash
$ taos> select areaid, loc from test.t0;
   areaid    |    loc     |
===========================
          10 | shanghai   |
Query OK, 1 row(s) in set (0.002904s)

```

## 停止正在 Docker 中运行的 TDengine 服务

```bash
$ docker stop cdf548465318
cdf548465318
```

- **docker stop**：通过 docker stop 停止指定的正在运行中的 docker 镜像。
- **cdf548465318**：容器 ID，根据 docker ps 指令返回的结果进行修改。

## 编程开发时连接在 Docker 中的 TDengine

从 Docker 之外连接使用在 Docker 容器内运行的 TDengine 服务，有以下两个思路：

1，通过端口映射(-p)，将容器内部开放的网络端口映射到宿主机的指定端口上。通过挂载本地目录(-v)，可以实现宿主机与容器内部的数据同步，防止容器删除后，数据丢失。

```bash
$ docker run -d -v /etc/taos:/etc/taos -p 6041:6041 tdengine/tdengine
526aa188da767ae94b244226a2b2eec2b5f17dd8eff592893d9ec0cd0f3a1ccd

$ curl -u root:taosdata -d 'show databases' 127.0.0.1:6041/rest/sql
{"status":"succ","head":["name","created_time","ntables","vgroups","replica","quorum","days","keep1,keep2,keep(D)","cache(MB)","blocks","minrows","maxrows","wallevel","fsync","comp","precision","status"],"data":[],"rows":0}
```

- 第一条命令，启动一个运行了 TDengine 的 docker 容器，并且将容器的 6041 端口映射到宿主机的 6041 端口上。
- 第二条命令，通过 RESTful 接口访问 TDengine，这时连接的是本机的 6041 端口，可见连接成功。

注意：在这个示例中，出于方便性考虑，只映射了 RESTful 需要的 6041 端口。如果希望以非 RESTful 方式连接 TDengine 服务，则需要映射从 6030 开始的共 11 个端口（完整的端口情况请参见 [TDengine 2.0 端口说明](https://www.taosdata.com/cn/documentation/faq#port)）。在例子中，挂载本地目录也只是处理了配置文件所在的 /etc/taos 目录，而没有挂载数据存储目录。

2，直接通过 exec 命令，进入到 docker 容器中去做开发。也即，把程序代码放在 TDengine 服务端所在的同一个 Docker 容器中，连接容器本地的 TDengine 服务。

```bash
$ docker exec -it 526aa188da /bin/bash
```

