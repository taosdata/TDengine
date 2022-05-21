---
title: 通过 Docker 快速体验 TDengine
---

虽然并不推荐在生产环境中通过 Docker 来部署 TDengine 服务，但 Docker 工具能够很好地屏蔽底层操作系统的环境差异，很适合在开发测试或初次体验时用于安装运行 TDengine 的工具集。特别是，借助 Docker，能够比较方便地在 macOS 和 Windows 系统上尝试 TDengine，而无需安装虚拟机或额外租用 Linux 服务器。另外，从 2.0.14.0 版本开始，TDengine 提供的镜像已经可以同时支持 X86-64、X86、arm64、arm32 平台，像 NAS、树莓派、嵌入式开发板之类可以运行 docker 的非主流计算机也可以基于本文档轻松体验 TDengine。

下文通过 Step by Step 风格的介绍，讲解如何通过 Docker 快速建立 TDengine 的单节点运行环境，以支持开发和测试。

## 下载 Docker

Docker 工具自身的下载请参考 [Docker 官网文档](https://docs.docker.com/get-docker/)。

安装完毕后可以在命令行终端查看 Docker 版本。如果版本号正常输出，则说明 Docker 环境已经安装成功。

```bash
$ docker -v
Docker version 20.10.3, build 48d30b5
```

## 使用 Docker 在容器中运行 TDengine

### 在 Docker 容器中运行 TDengine server

```bash
$ docker run -d -p 6030-6049:6030-6049 -p 6030-6049:6030-6049/udp tdengine/tdengine
526aa188da767ae94b244226a2b2eec2b5f17dd8eff592893d9ec0cd0f3a1ccd
```

这条命令，启动一个运行了 TDengine server 的 docker 容器，并且将容器的 6030 到 6049 端口映射到宿主机的 6030 到 6049 端口上。如果宿主机已经运行了 TDengine server 并占用了相同端口，需要映射容器的端口到不同的未使用端口段。（详情参见 [TDengine 2.0 端口说明](/train-faq/faq#port）。为了支持 TDengine 客户端操作 TDengine server 服务， TCP 和 UDP 端口都需要打开。

- **docker run**：通过 Docker 运行一个容器
- **-d**：让容器在后台运行
- **-p**：指定映射端口。注意：如果不是用端口映射，依然可以进入 Docker 容器内部使用 TDengine 服务或进行应用开发，只是不能对容器外部提供服务
- **tdengine/tdengine**：拉取的 TDengine 官方发布的应用镜像
- **526aa188da767ae94b244226a2b2eec2b5f17dd8eff592893d9ec0cd0f3a1ccd**：这个返回的长字符是容器 ID，我们也可以通过容器 ID 来查看对应的容器

进一步，还可以使用 docker run 命令启动运行 TDengine server 的 docker 容器，并使用 `--name` 命令行参数将容器命名为 `tdengine`，使用 `--hostname` 指定 hostname 为 `tdengine-server`，通过 `-v` 挂载本地目录到容器，实现宿主机与容器内部的数据同步，防止容器删除后，数据丢失。

```bash
docker run -d --name tdengine --hostname="tdengine-server" -v ~/work/taos/log:/var/log/taos -v ~/work/taos/data:/var/lib/taos  -p 6030-6049:6030-6049 -p 6030-6049:6030-6049/udp tdengine/tdengine
```

- **--name tdengine**：设置容器名称，我们可以通过容器名称来访问对应的容器
- **--hostname=tdengine-server**：设置容器内 Linux 系统的 hostname，我们可以通过映射 hostname 和 IP 来解决容器 IP 可能变化的问题。
- **-v**：设置宿主机文件目录映射到容器内目录，避免容器删除后数据丢失。

### 使用 docker ps 命令确认容器是否已经正确运行

```bash
docker ps
```

输出示例如下：

```
CONTAINER ID   IMAGE               COMMAND   CREATED          STATUS          ···
c452519b0f9b   tdengine/tdengine   "taosd"   14 minutes ago   Up 14 minutes   ···
```

- **docker ps**：列出所有正在运行状态的容器信息。
- **CONTAINER ID**：容器 ID。
- **IMAGE**：使用的镜像。
- **COMMAND**：启动容器时运行的命令。
- **CREATED**：容器创建时间。
- **STATUS**：容器状态。UP 表示运行中。

### 通过 docker exec 命令，进入到 docker 容器中去做开发

```bash
$ docker exec -it tdengine /bin/bash
root@tdengine-server:~/TDengine-server-2.4.0.4#
```

- **docker exec**：通过 docker exec 命令进入容器，如果退出，容器不会停止。
- **-i**：进入交互模式。
- **-t**：指定一个终端。
- **tdengine**：容器名称，需要根据 docker ps 指令返回的值进行修改。
- **/bin/bash**：载入容器后运行 bash 来进行交互。

进入容器后，执行 taos shell 客户端程序。

```bash
root@tdengine-server:~/TDengine-server-2.4.0.4# taos

Welcome to the TDengine shell from Linux, Client Version:2.4.0.4
Copyright (c) 2020 by TAOS Data, Inc. All rights reserved.

taos>
```

TDengine 终端成功连接服务端，打印出了欢迎消息和版本信息。如果失败，会有错误信息打印出来。

在 TDengine 终端中，可以通过 SQL 命令来创建/删除数据库、表、超级表等，并可以进行插入和查询操作。具体可以参考 [TAOS SQL 说明文档](/taos-sql/)。

### 在宿主机访问 Docker 容器中的 TDengine server

在使用了 -p 命令行参数映射了正确的端口启动了 TDengine Docker 容器后，就在宿主机使用 taos shell 命令即可访问运行在 Docker 容器中的 TDengine。

```
$ taos

Welcome to the TDengine shell from Linux, Client Version:2.4.0.4
Copyright (c) 2020 by TAOS Data, Inc. All rights reserved.

taos>
```

也可以在宿主机使用 curl 通过 RESTful 端口访问 Docker 容器内的 TDengine server。

```
curl -u root:taosdata -d 'show databases' 127.0.0.1:6041/rest/sql
```

输出示例如下：

```
{"status":"succ","head":["name","created_time","ntables","vgroups","replica","quorum","days","keep0,keep1,keep(D)","cache(MB)","blocks","minrows","maxrows","wallevel","fsync","comp","cachelast","precision","update","status"],"column_meta":[["name",8,32],["created_time",9,8],["ntables",4,4],["vgroups",4,4],["replica",3,2],["quorum",3,2],["days",3,2],["keep0,keep1,keep(D)",8,24],["cache(MB)",4,4],["blocks",4,4],["minrows",4,4],["maxrows",4,4],["wallevel",2,1],["fsync",4,4],["comp",2,1],["cachelast",2,1],["precision",8,3],["update",2,1],["status",8,10]],"data":[["test","2021-08-18 06:01:11.021",10000,4,1,1,10,"3650,3650,3650",16,6,100,4096,1,3000,2,0,"ms",0,"ready"],["log","2021-08-18 05:51:51.065",4,1,1,1,10,"30,30,30",1,3,100,4096,1,3000,2,0,"us",0,"ready"]],"rows":2}
```

这条命令，通过 REST API 访问 TDengine server，这时连接的是本机的 6041 端口，可见连接成功。

TDengine REST API 详情请参考[官方文档](/reference/rest-api/)。

### 使用 Docker 容器运行 TDengine server 和 taosAdapter

在 TDengine 2.4.0.0 之后版本的 Docker 容器，开始提供一个独立运行的组件 taosAdapter，代替之前版本 TDengine 中 taosd 进程中内置的 http server。taosAdapter 支持通过 RESTful 接口对 TDengine server 的数据写入和查询能力，并提供和 InfluxDB/OpenTSDB 兼容的数据摄取接口，允许 InfluxDB/OpenTSDB 应用程序无缝移植到 TDengine。在新版本 Docker 镜像中，默认启用了 taosAdapter，也可以使用 docker run 命令中设置 TAOS_DISABLE_ADAPTER=true 来禁用 taosAdapter；也可以在 docker run 命令中单独使用 taosAdapter，而不运行 taosd 。

注意：如果容器中运行 taosAdapter，需要根据需要映射其他端口，具体端口默认配置和修改方法请参考[taosAdapter 文档](/reference/taosadapter/)。

使用 docker 运行 TDengine 2.4.0.4 版本镜像（taosd + taosAdapter）：

```bash
docker run -d --name tdengine-all -p 6030-6049:6030-6049 -p 6030-6049:6030-6049/udp tdengine/tdengine:2.4.0.4
```

使用 docker 运行 TDengine 2.4.0.4 版本镜像（仅 taosAdapter，需要设置 firstEp 配置项 或 TAOS_FIRST_EP 环境变量）：

```bash
docker run -d --name tdengine-taosa -p 6041-6049:6041-6049 -p 6041-6049:6041-6049/udp -e TAOS_FIRST_EP=tdengine-all tdengine/tdengine:2.4.0.4 taosadapter
```

使用 docker 运行 TDengine 2.4.0.4 版本镜像（仅 taosd）：

```bash
docker run -d --name tdengine-taosd -p 6030-6042:6030-6042 -p 6030-6042:6030-6042/udp -e TAOS_DISABLE_ADAPTER=true tdengine/tdengine:2.4.0.4
```

使用 curl 命令验证 RESTful 接口可以正常工作：

```bash
curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'show databases;' 127.0.0.1:6041/rest/sql
```

输出示例如下：

```
{"status":"succ","head":["name","created_time","ntables","vgroups","replica","quorum","days","keep","cache(MB)","blocks","minrows","maxrows","wallevel","fsync","comp","cachelast","precision","update","status"],"column_meta":[["name",8,32],["created_time",9,8],["ntables",4,4],["vgroups",4,4],["replica",3,2],["quorum",3,2],["days",3,2],["keep",8,24],["cache(MB)",4,4],["blocks",4,4],["minrows",4,4],["maxrows",4,4],["wallevel",2,1],["fsync",4,4],["comp",2,1],["cachelast",2,1],["precision",8,3],["update",2,1],["status",8,10]],"data":[["log","2021-12-28 09:18:55.765",10,1,1,1,10,"30",1,3,100,4096,1,3000,2,0,"us",0,"ready"]],"rows":1}
```

### 应用示例：在宿主机使用 taosBenchmark 写入数据到 Docker 容器中的 TDengine server

1. 在宿主机命令行界面执行 taosBenchmark （曾命名为 taosdemo）写入数据到 Docker 容器中的 TDengine server

   ```bash
   $ taosBenchmark

   taosBenchmark is simulating data generated by power equipments monitoring...

   host:                       127.0.0.1:6030
   user:                       root
   password:                   taosdata
   configDir:
   resultFile:                 ./output.txt
   thread num of insert data:  10
   thread num of create table: 10
   top insert interval:        0
   number of records per req:  30000
   max sql length:             1048576
   database count:             1
   database[0]:
   database[0] name:      test
   drop:                  yes
   replica:               1
   precision:             ms
   super table count:     1
   super table[0]:
       stbName:           meters
       autoCreateTable:   no
       childTblExists:    no
       childTblCount:     10000
       childTblPrefix:    d
       dataSource:        rand
       iface:             taosc
       insertRows:        10000
       interlaceRows:     0
       disorderRange:     1000
       disorderRatio:     0
       maxSqlLen:         1048576
       timeStampStep:     1
       startTimestamp:    2017-07-14 10:40:00.000
       sampleFormat:
       sampleFile:
       tagsFile:
       columnCount:       3
   column[0]:FLOAT column[1]:INT column[2]:FLOAT
       tagCount:            2
           tag[0]:INT tag[1]:BINARY(16)

           Press enter key to continue or Ctrl-C to stop
   ```

   回车后，该命令将在数据库 test 下面自动创建一张超级表 meters，该超级表下有 1 万张表，表名为 "d0" 到 "d9999"，每张表有 1 万条记录，每条记录有 (ts, current, voltage, phase) 四个字段，时间戳从 "2017-07-14 10:40:00 000" 到 "2017-07-14 10:40:09 999"，每张表带有标签 location 和 groupId，groupId 被设置为 1 到 10， location 被设置为 "beijing" 或者 "shanghai"。

   最后共插入 1 亿条记录。

2. 进入 TDengine 终端，查看 taosBenchmark 生成的数据。

   - **进入命令行。**

   ```bash
   $ root@c452519b0f9b:~/TDengine-server-2.4.0.4# taos

   Welcome to the TDengine shell from Linux, Client Version:2.4.0.4
   Copyright (c) 2020 by TAOS Data, Inc. All rights reserved.

   taos>
   ```

   - **查看数据库。**

   ```bash
   $ taos> show databases;
     name        |      created_time       |   ntables   |   vgroups   |    ···
     test        | 2021-08-18 06:01:11.021 |       10000 |           6 |    ···
     log         | 2021-08-18 05:51:51.065 |           4 |           1 |    ···

   ```

   - **查看超级表。**

   ```bash
   $ taos> use test;
   Database changed.

   $ taos> show stables;
                 name              |      created_time       | columns |  tags  |   tables    |
   ============================================================================================
    meters                         | 2021-08-18 06:01:11.116 |       4 |      2 |       10000 |
   Query OK, 1 row(s) in set (0.003259s)

   ```

   - **查看表，限制输出十条。**

   ```bash
   $ taos> select * from test.t0 limit 10;

   DB error: Table does not exist (0.002857s)
   taos> select * from test.d0 limit 10;
              ts            |       current        |   voltage   |        phase         |
   ======================================================================================
    2017-07-14 10:40:00.000 |             10.12072 |         223 |              0.34167 |
    2017-07-14 10:40:00.001 |             10.16103 |         224 |              0.34445 |
    2017-07-14 10:40:00.002 |             10.00204 |         220 |              0.33334 |
    2017-07-14 10:40:00.003 |             10.00030 |         220 |              0.33333 |
    2017-07-14 10:40:00.004 |              9.84029 |         216 |              0.32222 |
    2017-07-14 10:40:00.005 |              9.88028 |         217 |              0.32500 |
    2017-07-14 10:40:00.006 |              9.88110 |         217 |              0.32500 |
    2017-07-14 10:40:00.007 |             10.08137 |         222 |              0.33889 |
    2017-07-14 10:40:00.008 |             10.12063 |         223 |              0.34167 |
    2017-07-14 10:40:00.009 |             10.16086 |         224 |              0.34445 |
   Query OK, 10 row(s) in set (0.016791s)

   ```

   - **查看 d0 表的标签值。**

   ```bash
   $ taos> select groupid, location from test.d0;
      groupid   |     location     |
   =================================
              0 | shanghai         |
   Query OK, 1 row(s) in set (0.003490s)
   ```

### 应用示例：使用数据收集代理软件写入 TDengine

taosAdapter 支持多个数据收集代理软件（如 Telegraf、StatsD、collectd 等），这里仅模拟 StasD 写入数据，在宿主机执行命令如下：

```
echo "foo:1|c" | nc -u -w0 127.0.0.1 6044
```

然后可以使用 taos shell 查询 taosAdapter 自动创建的数据库 statsd 和 超级表 foo 中的内容：

```
taos> show databases;
              name              |      created_time       |   ntables   |   vgroups   | replica | quorum |  days  |           keep           |  cache(MB)  |   blocks    |   minrows   |   maxrows   | wallevel |    fsync    | comp | cachelast | precision | update |   status   |
====================================================================================================================================================================================================================================================================================
 log                            | 2021-12-28 09:18:55.765 |          12 |           1 |       1 |      1 |     10 | 30                       |           1 |           3 |         100 |        4096 |        1 |        3000 |    2 |         0 | us        |      0 | ready      |
 statsd                         | 2021-12-28 09:21:48.841 |           1 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ns        |      2 | ready      |
Query OK, 2 row(s) in set (0.002112s)

taos> use statsd;
Database changed.

taos> show stables;
              name              |      created_time       | columns |  tags  |   tables    |
============================================================================================
 foo                            | 2021-12-28 09:21:48.894 |       2 |      1 |           1 |
Query OK, 1 row(s) in set (0.001160s)

taos> select * from foo;
              ts               |         value         |         metric_type          |
=======================================================================================
 2021-12-28 09:21:48.840820836 |                     1 | counter                      |
Query OK, 1 row(s) in set (0.001639s)

taos>
```

可以看到模拟数据已经被写入到 TDengine 中。

## 停止正在 Docker 中运行的 TDengine 服务

```bash
docker stop tdengine
```

- **docker stop**：通过 docker stop 停止指定的正在运行中的 docker 镜像。
