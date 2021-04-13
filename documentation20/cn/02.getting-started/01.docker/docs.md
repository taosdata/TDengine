# 通过Docker快速体验TDengine

由于目前TDengine2.0的服务器仅能在Linux系统上安装和运行，如果开发者使用的是Mac或Windows系统，可能需要安装虚拟机或远程连接Linux服务器，来使用TDengine。Docker轻巧快速，可以作为一个可行，经济，高效的替代方案。下文准备一步一步介绍，如何通过Docker快速使用TDengine，可用于开发，测试，和备份数据。

&nbsp; 
### 下载Docker

docker的下载可以参考[docker官网文档Get Docker部分](https://docs.docker.com/get-docker/)。

安装成功后可以查看下docker版本。

```bash
$ docker -v
Docker version 20.10.5, build 55c4c88
```

&nbsp; 
&nbsp; 
### 在docker容器中运行TDengine

+ 使用命令拉取tdengine镜像，并使它在后台运行。

```bash
$ docker run -d tdengine/tdengine
cdf548465318c6fc2ad97813f89cc60006393392401cae58a27b15ca9171f316
```

&emsp;&emsp;**run**：和前面的docker命令组合起来，运行一个容器。

&emsp;&emsp;**-d**：让容器在后台运行。

&emsp;&emsp;**tdengine/tdengine**：拉取的TDengine官方发布的应用镜像

&emsp;&emsp;**cdf548465318c6fc2ad97813f89cc60006393392401cae58a27b15ca9171f316**：

&emsp;&emsp;返回的长字符是容器ID，我们可以通过容器ID来查看对应的容器。

&nbsp; 
+ 确认容器是否已经运行起来了。

```bash
$ docker ps
CONTAINER ID   IMAGE               COMMAND   CREATED          STATUS          ···
cdf548465318   tdengine/tdengine   "taosd"   14 minutes ago   Up 14 minutes   ···
```

&emsp;&emsp;**docker ps**：列出所有在运行中的容器信息。

&emsp;&emsp;**CONTAINER ID**：容器ID。

&emsp;&emsp;**IMAGE**：使用的镜像。

&emsp;&emsp;**COMMAND**：启动容器时运行的命令。

&emsp;&emsp;**CREATED**：容器创建时间。

&emsp;&emsp;**STATUS**：容器状态。UP表示运行中。

&nbsp; 
+ 进入docker容器内，使用tdengine。

```bash
$ docker exec -it cdf548465318 /bin/bash
root@cdf548465318:~/TDengine-server-2.0.13.0#
```

&emsp;&emsp;**exec**：通过docker exec命令进入容器，如果退出容器不会停止。

&emsp;&emsp;**-i**：进入交互模式。

&emsp;&emsp;**-t**：指定一个终端。

&emsp;&emsp;**cdf548465318**：容器ID，需要根据具体情况进行修改。

&emsp;&emsp;**/bin/bash**：载入容器后运行bash来进行交互。

&nbsp; 
+ 进入容器后，执行TDengine命令行程序。

```bash
$ root@cdf548465318:~/TDengine-server-2.0.13.0# taos

Welcome to the TDengine shell from Linux, Client Version:2.0.13.0
Copyright (c) 2020 by TAOS Data, Inc. All rights reserved.

taos>
```

TDengine终端连接服务成功，打印出了欢迎消息和版本信息。如果失败，会有错误信息打印出来。

在TDengine终端中，可以通过SQL命令来创建/删除数据库，表，超级表等，并可以进行插入查询操作。具体可以参考[TDengine官网文档的TAOS SQL部分](https://www.taosdata.com/cn/documentation/taos-sql)。

&nbsp; 
&nbsp; 
### 通过taosdemo进一步了解TDengine

+ 接上面的步骤，先退出TDengine命令行程序。

```bash
$ taos> q
root@cdf548465318:~/TDengine-server-2.0.13.0#
```

&nbsp; 
+ 在Linux终端执行taosdemo。

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

回车后，该命令将新建一个数据库test，并且自动创建一张超级表meters，并以超级表meters为模版创建了1万张表，表名从"t0"到"t9999"。每张表有10万条记录，每条记录有f1，f2，f3三个字段，时间戳ts字段从"2017-07-14 10:40:00 000" 到"2017-07-14 10:41:39 999"。每张表带有areaid和loc两个标签TAG， areaid被设置为1到10, loc被设置为"beijing"或者“shanghai"。

&nbsp; 
+ 运行taodemo命令的结果，可以进入TDengine命令行，进行验证。

&emsp;&emsp;**进入命令行。**

```bash
$ root@cdf548465318:~/TDengine-server-2.0.13.0# taos

Welcome to the TDengine shell from Linux, Client Version:2.0.13.0
Copyright (c) 2020 by TAOS Data, Inc. All rights reserved.

taos>
```

&emsp;&emsp;**查看数据库。**

```bash
$ taos> show databases;
  name        |      created_time       |   ntables   |   vgroups   |    ···
  test        | 2021-04-13 02:14:15.950 |       10000 |           6 |    ···
  log         | 2021-04-12 09:36:37.549 |           4 |           1 |	   ···
  
```

&emsp;&emsp;**查看超级表。**

```bash
$ taos> use test;
Database changed.

$ taos> show stables;
       name              |      created_time       | columns |  tags  |   tables    |
=====================================================================================
       meters            | 2021-04-13 02:14:15.955 |       4 |      2 |       10000 |
Query OK, 1 row(s) in set (0.001737s)

```

&emsp;&emsp;**查看表，限制输出十条。**

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

&emsp;&emsp;**查看t0表的标签值。**

```bash
$ taos> select areaid, loc from test.t0;
   areaid    |    loc     |
===========================
          10 | shanghai   |
Query OK, 1 row(s) in set (0.002904s)

```

&nbsp; 
&nbsp; 
### 开发时连接上通过docker启动的TDengine

连接并且使用在docker容器中的启动的TDengine，有以下两个思路：


1. 通过端口映射(-p)，将容器内部开放的网络端口映射到宿主机的指定端口上。通过挂载本地目录(-v)，可以实现宿主机与容器内部的数据同步，防止容器删除后，数据丢失。

```bash
$ docker run -d -v /etc/taos:/etc/taos -p 6041:6041 tdengine/tdengine
526aa188da767ae94b244226a2b2eec2b5f17dd8eff592893d9ec0cd0f3a1ccd

$ curl -u root:taosdata -d 'show databases' 127.0.0.1:6041/rest/sql
{"status":"succ","head":["name","created_time","ntables","vgroups","replica","quorum","days","keep1,keep2,keep(D)","cache(MB)","blocks","minrows","maxrows","wallevel","fsync","comp","precision","status"],"data":[],"rows":0}
```

&emsp;&emsp;第一条命令，启动了一个运行了TDengine的docker容器，并且将容器的6041端口映射到宿主机的6041端口上。

&emsp;&emsp;第二条命令，通过RESTful接口访问TDengine，这时连接的是本机的6041端口，可见连接成功。

&nbsp; 
2. 直接通过exec命令，进入到docker容器中去做开发。

```bash
$ docker exec -it 526aa188da /bin/bash
```

