# Quickly Taste TDengine with Docker

While it is not recommended to deploy TDengine services via Docker in a production environment, Docker tools do a good job of shielding the environmental differences in the underlying operating system and are well suited for use in development testing or first-time taste with the toolset for installing and running TDengine. In particular, Docker makes it relatively easy to try TDengine on macOS and Windows systems without having to install a virtual machine or rent an additional Linux server. In addition, starting from version 2.0.14.0, TDengine provides images that support both X86-64, X86, arm64, and arm32 platforms, so non-mainstream computers that can run docker, such as NAS, Raspberry Pi, and embedded development boards, can also easily taste TDengine based on this document.

The following article explains how to quickly build a single-node TDengine runtime environment via Docker to support development and testing through a Step by Step style introduction.

## Docker download

The Docker tools themselves can be downloaded from [Docker official site](https://docs.docker.com/get-docker/).

After installation, you can check the Docker version in the command-line terminal. If the version number is output properly, the Docker environment has been installed successfully.

```bash
$ docker -v
Docker version 20.10.3, build 48d30b5
```

## How to use Docker to run TDengine

### running TDengine server inside Docker

```bash
docker run -d -p 6030-6049:6030-6049 -p 6030-6049:6030-6049/udp tdengine/tdengine
526aa188da767ae94b244226a2b2eec2b5f17dd8eff592893d9ec0cd0f3a1ccd
```

This command starts a docker container with TDengine server running and maps the container's ports from 6030 to 6049 to the host's ports from 6030 to 6049. If the host is already running TDengine server and occupying the same port(s), you need to map the container's port to a different unused port segment. (Please see [TDengine 2.0 Port Description](https://www.taosdata.com/en/documentation/faq#port) for details). In order to support TDengine clients accessing TDengine server services, both TCP and UDP ports need to be open.

- **docker run**: Run a container via Docker
- **-d**: put the container run in the background
- **-p**: specify the port(s) to map. Note: If you do not use port mapping, you can still go inside the Docker container to access TDengine services or develop your application, but you cannot provide services outside the container
- **tdengine/tdengine**: the official TDengine published application image that is pulled
- **526aa188da767ae94b244226a2b2eec2b5f17dd8eff592893d9ec0cd0f3a1ccd**: The long character returned is the container ID, and we can also view the corresponding container by its container ID

Further, you can also use the `docker run` command to start the docker container running TDengine server, and use the `--name` command line parameter to name the container TDengine, use `--hostname` to specify the hostname as TDengine-server, and use `-v` to mount the local directory (-v) to synchronize the data inside the host and the container to prevent data loss after the container is deleted.

```
docker run -d --name tdengine --hostname="tdengine-server" -v ~/work/taos/log:/var/log/taos -v ~/work/taos/data:/var/lib/taos  -p 6030-6041:6030-6041 -p 6030-6041:6030-6041/udp tdengine/tdengine
```

- **--name tdengine**: set the container name, we can access the corresponding container by container name
- **--hostname=tdengine-server**: set the hostname of the Linux system inside the container, we can map the hostname and IP to solve the problem that the container IP may change.
- **-v**: Set the host file directory to be mapped to the inner container directory to avoid data loss after the container is deleted.

### Use the `docker ps` command to verify that the container is running correctly

```bash
docker ps
```

The output could be:

```
CONTAINER ID   IMAGE               COMMAND   CREATED          STATUS          ···
c452519b0f9b   tdengine/tdengine   "taosd"   14 minutes ago   Up 14 minutes   ···
```

- **docker ps**: list all containers in running state.
- **CONTAINER ID**: container ID.
- **IMAGE**: the image used.
- **COMMAND**: the command to run when starting the container.
- **CREATED**: container creation time.
- **STATUS**: container status. UP means running.

### Enter the docker container to do development via the `docker exec` COMMAND

```bash
$ docker exec -it tdengine /bin/bash
root@tdengine-server:~/TDengine-server-2.4.0.4#
```

- **docker exec**: Enter the container by `docker exec` command, if exited, the container will not stop.
- **-i**: use interactive mode.
- **-t**: specify a terminal.
- **tdengine**: container name, needs to be changed according to the value returned by the docker ps command.
- **/bin/bash**: load the container and run bash to interact with it.

After entering the container, execute the taos shell client program.

```bash
root@tdengine-server:~/TDengine-server-2.4.0.4# taos

Welcome to the TDengine shell from Linux, Client Version:2.4.0.4
Copyright (c) 2020 by TAOS Data, Inc. All rights reserved.

taos>
```

The TDengine shell successfully connects to the server and prints out a welcome message and version information. If it fails, an error message is printed.

In the TDengine shell, you can create/delete databases, tables, super tables, etc., and perform insert and query operations via SQL commands. For details, please refer to the [TAOS SQL documentation](https://www.taosdata.com/en/documentation/taos-sql).

### Accessing TDengine server inside Docker container from the host side

After starting the TDengine Docker container with the correct port mapped with the -p command line parameter, you can access the TDengine running inside the Docker container from the host side using the taos shell command.

```
$ taos

Welcome to the TDengine shell from Linux, Client Version:2.4.0.4
Copyright (c) 2020 by TAOS Data, Inc. All rights reserved.

taos>
```

You can also access the TDengine server inside the Docker container using `curl` command from the host side through the RESTful port.

```
curl -u root:taosdata -d 'show databases' 127.0.0.1:6041/rest/sql
```

The output could be:

```
{"status":"succ","head":["name","created_time","ntables","vgroups","replica","quorum","days","keep0,keep1,keep(D)","cache(MB)","blocks","minrows","maxrows","wallevel","fsync","comp","cachelast","precision","update","status"],"column_meta":[["name",8,32],["created_time",9,8],["ntables",4,4],["vgroups",4,4],["replica",3,2],["quorum",3,2],["days",3,2],["keep0,keep1,keep(D)",8,24],["cache(MB)",4,4],["blocks",4,4],["minrows",4,4],["maxrows",4,4],["wallevel",2,1],["fsync",4,4],["comp",2,1],["cachelast",2,1],["precision",8,3],["update",2,1],["status",8,10]],"data":[["test","2021-08-18 06:01:11.021",10000,4,1,1,10,"3650,3650,3650",16,6,100,4096,1,3000,2,0,"ms",0,"ready"],["log","2021-08-18 05:51:51.065",4,1,1,1,10,"30,30,30",1,3,100,4096,1,3000,2,0,"us",0,"ready"]],"rows":2}
```

This command accesses the TDengine server through the RESTful interface, which connects to port 6041 on the local machine, so the connection is successful.

TDengine RESTful interface details can be found in the [official documentation](https://www.taosdata.com/en/documentation/connector#restful).

### Running TDengine server and taosAdapter with a Docker container

Docker containers of TDengine version 2.4.0.0 and later include a component named `taosAdapter`, which supports data writing and querying capabilities to the TDengine server through the RESTful interface and provides the data ingestion interfaces compatible with InfluxDB/OpenTSDB. Allows seamless migration of InfluxDB/OpenTSDB applications to access TDengine.

Note: If taosAdapter is running inside the container, you need to add mapping to other additional ports as needed, please refer to [taosAdapter documentation](https://github.com/taosdata/taosadapter/blob/develop/README.md) for the default port number and modification methods for the specific purpose.

Running TDengine version 2.4.0.4 image with docker.

Start taosAdapter and taosd by default:

```
docker run -d --name tdengine-taosa -p 6030-6049:6030-6049 -p 6030-6049:6030-6049/udp tdengine/tdengine:2.4.0.4
```

Verify that the RESTful interface taosAdapter provides working using the `curl` command.

```
curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'show databases;' 127.0.0.1:6041/rest/sql
```

The output could be:

```
{"status":"succ","head":["name","created_time","ntables","vgroups","replica","quorum","days","keep","cache(MB)","blocks","minrows","maxrows","wallevel","fsync","comp","cachelast","precision","update","status"],"column_meta":[["name",8,32],["created_time",9,8],["ntables",4,4],["vgroups",4,4],["replica",3,2],["quorum",3,2],["days",3,2],["keep",8,24],["cache(MB)",4,4],["blocks",4,4],["minrows",4,4],["maxrows",4,4],["wallevel",2,1],["fsync",4,4],["comp",2,1],["cachelast",2,1],["precision",8,3],["update",2,1],["status",8,10]],"data":[["log","2021-12-28 09:18:55.765",10,1,1,1,10,"30",1,3,100,4096,1,3000,2,0,"us",0,"ready"]],"rows":1}
```

### Application example: write data to TDengine server in Docker container using taosBenchmark on the host

1. execute `taosBenchmark` (was named taosdemo) in the host command line interface to write data to the TDengine server in the Docker container

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

After enter, this command will automatically create a super table `meters` under the database test, there are 10,000 tables under this super table, the table name is "d0" to "d9999", each table has 10,000 records, each record has four fields (ts, current, voltage, phase), the time stamp is from "2017-07-14 10:40:00 000" to "2017-07-14 10:40:09 999", each table has a tag location and groupid, groupid is set from 1 to 10 and location is set to "beijing" or "shanghai".

It takes about a few minutes to execute this command and ends up inserting a total of 100 million records.

2.Go to the TDengine terminal and view the data generated by taosBenchmark.

- **Go to the terminal interface.**

```bash
$ root@c452519b0f9b:~/TDengine-server-2.4.0.4# taos

Welcome to the TDengine shell from Linux, Client Version:2.4.0.4
Copyright (c) 2020 by TAOS Data, Inc. All rights reserved.

taos>
```

- **View the database.**

```bash
$ taos> SHOW DATABASES;
  name        |      created_time       |   ntables   |   vgroups   |    ···
  test        | 2021-08-18 06:01:11.021 |       10000 |           6 |    ···
  log         | 2021-08-18 05:51:51.065 |           4 |           1 |    ···

```

- **View Super Tables.**

```bash
$ taos> USE test;
Database changed.

$ taos> SHOW STABLES;
              name              |      created_time       | columns |  tags  |   tables    |
============================================================================================
 meters                         | 2021-08-18 06:01:11.116 |       4 |      2 |       10000 |
Query OK, 1 row(s) in set (0.003259s)

```

- **View the table and limit the output to 10 entries.**

```bash
taos> SELECT * FROM test.d0 LIMIT 10;
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

- **View the tag values for the d0 table.**

```bash
$ taos> SELECT groupid, location FROM test.d0;
   groupid   |     location     |
=================================
           0 | shanghai         |
Query OK, 1 row(s) in set (0.003490s)

```

### Application Example: use data collection agent to write data into TDengine

taosAdapter supports multiple data collection agents (e.g. Telegraf, StatsD, collectd, etc.), here only demonstrate how StatsD is simulated to write data, and the command is executed from the host side as follows.

```
echo "foo:1|c" | nc -u -w0 127.0.0.1 6044
```

Then you can use the taos shell to query the taosAdapter automatically created database statsd and the contents of the super table foo.

```
taos> SHOW DATABASES;
              name              |      created_time       |   ntables   |   vgroups   | replica | quorum |  days  |           keep           |  cache(MB)  |   blocks    |   minrows   |   maxrows   | wallevel |    fsync    | comp | cachelast | precision | update |   status   |
====================================================================================================================================================================================================================================================================================
 log                            | 2021-12-28 09:18:55.765 |          12 |           1 |       1 |      1 |     10 | 30                       |           1 |           3 |         100 |        4096 |        1 |        3000 |    2 |         0 | us        |      0 | ready      |
 statsd                         | 2021-12-28 09:21:48.841 |           1 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ns        |      2 | ready      |
Query OK, 2 row(s) in set (0.002112s)

taos> USE statsd;
Database changed.

taos> SHOW STABLES;
              name              |      created_time       | columns |  tags  |   tables    |
============================================================================================
 foo                            | 2021-12-28 09:21:48.894 |       2 |      1 |           1 |
Query OK, 1 row(s) in set (0.001160s)

taos> SELECT * FROM foo;
              ts               |         value         |         metric_type          |
=======================================================================================
 2021-12-28 09:21:48.840820836 |                     1 | counter                      |
Query OK, 1 row(s) in set (0.001639s)

taos>
```

You can see that the simulation data has been written to TDengine.

## Stop the TDengine service that is running in Docker

```bash
docker stop tdengine
```

- **docker stop**: Stop the specified running docker image with docker stop.

