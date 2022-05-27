---
sidebar_label: TDengine in Docker
title: Deploy TDengine in Docker
---

Even though it's not recommended to deploy TDengine using docker in production system, docker is still very useful in development environment, especially when your host is not Linux. From version 2.0.14.0, the official image of TDengine can support X86-64, X86, arm64, and rm32 .

In this chapter a simple step by step guide of using TDengine in docker is introduced.

## Install Docker

The installation of docker please refer to [Get Docker](https://docs.docker.com/get-docker/).

After docker is installed, you can check whether Docker is installed properly by displaying Docker version.

```bash
$ docker -v
Docker version 20.10.3, build 48d30b5
```

## Launch TDengine in Docker

### Launch TDengine Server

```bash
$ docker run -d -p 6030-6049:6030-6049 -p 6030-6049:6030-6049/udp tdengine/tdengine
526aa188da767ae94b244226a2b2eec2b5f17dd8eff592893d9ec0cd0f3a1ccd
```

In the above command, a docker container is started to run TDengine server, the port range 6030-6049 of the container is mapped to host port range 6030-6049. If port range 6030-6049 has been occupied on the host, please change to an available host port range. Regarding the requirements about ports on the host, please refer to [Port Configuration](/reference/config/#serverport).

- **docker run**: Launch a docker container
- **-d**: the container will run in background mode
- **-p**: port mapping
- **tdengine/tdengine**: The image from which to launch the container
- **526aa188da767ae94b244226a2b2eec2b5f17dd8eff592893d9ec0cd0f3a1ccd**: the container ID if successfully launched.

Furthermore, `--name` can be used with `docker run` to specify name for the container, `--hostname` can be used to specify hostname for the container, `-v` can be used to mount local volumes to the container so that the data generated inside the container can be persisted to disk on the host.

```bash
docker run -d --name tdengine --hostname="tdengine-server" -v ~/work/taos/log:/var/log/taos -v ~/work/taos/data:/var/lib/taos  -p 6030-6049:6030-6049 -p 6030-6049:6030-6049/udp tdengine/tdengine
```

- **--name tdengine**: specify the name of the container, the name can be used to specify the container later
- **--hostname=tdengine-server**: specify the hostname inside the container, the hostname can be used inside the container without worrying the container IP may vary
- **-v**: volume mapping between host and container

### Check the container

```bash
docker ps
```

The output is like below:

```
CONTAINER ID   IMAGE               COMMAND   CREATED          STATUS          ···
c452519b0f9b   tdengine/tdengine   "taosd"   14 minutes ago   Up 14 minutes   ···
```

- **docker ps**: List all the containers
- **CONTAINER ID**: Container ID
- **IMAGE**: The image used for the container
- **COMMAND**: The command used when launching the container
- **CREATED**: When the container was created
- **STATUS**: Status of the container

### Access TDengine inside container

```bash
$ docker exec -it tdengine /bin/bash
root@tdengine-server:~/TDengine-server-2.4.0.4#
```

- **docker exec**: Attach to the container
- **-i**: Interactive mode
- **-t**: Use terminal
- **tdengine**: Container name, up to the output of `docker ps`
- **/bin/bash**: The command to execute once the container is attached

Inside the container, start TDengine CLI `taos`

```bash
root@tdengine-server:~/TDengine-server-2.4.0.4# taos

Welcome to the TDengine shell from Linux, Client Version:2.4.0.4
Copyright (c) 2020 by TAOS Data, Inc. All rights reserved.

taos>
```

The above example is for a successful connection. If `taos` fails to connect to the server side, error information would be shown.

In TDengine CLI, SQL commands can be executed to create/drop databases, tables, STables, and insert or query data. For details please refer to [TAOS SQL](/taos-sql/).

### Access TDengine from host

If `-p` used to map ports properly between host and container, it's also able to access TDengine in container from the host as long as `firstEp` is configured correctly for the client on host.

```
$ taos

Welcome to the TDengine shell from Linux, Client Version:2.4.0.4
Copyright (c) 2020 by TAOS Data, Inc. All rights reserved.

taos>
```

It's also able to access the REST interface provided by TDengine in container from the host.

```
curl -u root:taosdata -d 'show databases' 127.0.0.1:6041/rest/sql
```

Output is like below:

```
{"status":"succ","head":["name","created_time","ntables","vgroups","replica","quorum","days","keep0,keep1,keep(D)","cache(MB)","blocks","minrows","maxrows","wallevel","fsync","comp","cachelast","precision","update","status"],"column_meta":[["name",8,32],["created_time",9,8],["ntables",4,4],["vgroups",4,4],["replica",3,2],["quorum",3,2],["days",3,2],["keep0,keep1,keep(D)",8,24],["cache(MB)",4,4],["blocks",4,4],["minrows",4,4],["maxrows",4,4],["wallevel",2,1],["fsync",4,4],["comp",2,1],["cachelast",2,1],["precision",8,3],["update",2,1],["status",8,10]],"data":[["test","2021-08-18 06:01:11.021",10000,4,1,1,10,"3650,3650,3650",16,6,100,4096,1,3000,2,0,"ms",0,"ready"],["log","2021-08-18 05:51:51.065",4,1,1,1,10,"30,30,30",1,3,100,4096,1,3000,2,0,"us",0,"ready"]],"rows":2}
```

For details of REST API please refer to [REST API]](/reference/rest-api/).

### Run TDengine server and taosAdapter inside container

From version 2.4.0.0, in the TDengine Docker image, `taosAdapter` is enabled by default, but can be disabled using environment variable `TAOS_DISABLE_ADAPTER=true` . `taosAdapter` can also be run alone without `taosd` when launching a container.

For the port mapping of `taosAdapter`, please refer to [taosAdapter](/reference/taosadapter/).

- Run both `taosd` and `taosAdapter` (by default) in docker container:

```bash
docker run -d --name tdengine-all -p 6030-6049:6030-6049 -p 6030-6049:6030-6049/udp tdengine/tdengine:2.4.0.4
```

- Run `taosAdapter` only in docker container, `TAOS_FIRST_EP` environment variable needs to be used to specify the container name in which `taosd` is running:

```bash
docker run -d --name tdengine-taosa -p 6041-6049:6041-6049 -p 6041-6049:6041-6049/udp -e TAOS_FIRST_EP=tdengine-all tdengine/tdengine:2.4.0.4 taosadapter
```

- Run `taosd` only in docker container:

```bash
docker run -d --name tdengine-taosd -p 6030-6042:6030-6042 -p 6030-6042:6030-6042/udp -e TAOS_DISABLE_ADAPTER=true tdengine/tdengine:2.4.0.4
```

- Verify the REST interface:

```bash
curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'show databases;' 127.0.0.1:6041/rest/sql
```

Below is an example output:

```
{"status":"succ","head":["name","created_time","ntables","vgroups","replica","quorum","days","keep","cache(MB)","blocks","minrows","maxrows","wallevel","fsync","comp","cachelast","precision","update","status"],"column_meta":[["name",8,32],["created_time",9,8],["ntables",4,4],["vgroups",4,4],["replica",3,2],["quorum",3,2],["days",3,2],["keep",8,24],["cache(MB)",4,4],["blocks",4,4],["minrows",4,4],["maxrows",4,4],["wallevel",2,1],["fsync",4,4],["comp",2,1],["cachelast",2,1],["precision",8,3],["update",2,1],["status",8,10]],"data":[["log","2021-12-28 09:18:55.765",10,1,1,1,10,"30",1,3,100,4096,1,3000,2,0,"us",0,"ready"]],"rows":1}
```

### Use taosBenchmark on host to access TDengine server in container

1. Run `taosBenchmark`, named as `taosdemo` previously, on the host:

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

   Once the execution is finished, a database `test` is created, a STable `meters` is created in database `test`, 10,000 sub tables are created using `meters` as template, named as "d0" to "d9999", while 10,000 rows are inserted into each table, so totally 100,000,000 rows are inserted.

2. Check the data

   - **Check database**

   ```bash
   $ taos> show databases;
     name        |      created_time       |   ntables   |   vgroups   |    ···
     test        | 2021-08-18 06:01:11.021 |       10000 |           6 |    ···
     log         | 2021-08-18 05:51:51.065 |           4 |           1 |    ···

   ```

   - **Check STable**

   ```bash
   $ taos> use test;
   Database changed.

   $ taos> show stables;
                 name              |      created_time       | columns |  tags  |   tables    |
   ============================================================================================
    meters                         | 2021-08-18 06:01:11.116 |       4 |      2 |       10000 |
   Query OK, 1 row(s) in set (0.003259s)

   ```

   - **Check Tables**

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

   - **Check tag values of table d0**

   ```bash
   $ taos> select groupid, location from test.d0;
      groupid   |     location     |
   =================================
              0 | California.SanDieo         |
   Query OK, 1 row(s) in set (0.003490s)
   ```

### Access TDengine from 3rd party tools

A lot of 3rd party tools can be used to write data into TDengine through `taosAdapter` , for details please refer to [3rd party tools](/third-party/).

There is nothing different from the 3rd party side to access TDengine server inside a container, as long as the end point is specified correctly, the end point should be the FQDN and the mapped port of the host.

## Stop TDengine inside container

```bash
docker stop tdengine
```

- **docker stop**: stop a container
- **tdengine**: container name
