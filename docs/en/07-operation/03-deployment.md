---
title: Deployment of TDengine Cluster
sidebar_label: Deployment
toc_heading_level: 4
description: This document describes how to deploy TDengine cluster.
---


TDengine has a native distributed design and provides the ability to scale out. A few nodes can form a TDengine cluster. If you need higher processing power, you just need to add more nodes into the cluster. TDengine uses virtual node technology to virtualize a node into multiple virtual nodes to achieve load balancing. At the same time, TDengine can group virtual nodes on different nodes into virtual node groups, and use the replication mechanism to ensure the high availability of the system. The cluster feature of TDengine is completely open source.

This document describes how to manually deploy a cluster on a host directly and deploy a cluster with Docker, Kubernetes or Helm.

## Manual Deployment
### Prerequisites

1. Step 1

The FQDN of all hosts must be setup properly. For e.g. FQDNs may have to be configured in the /etc/hosts file on each host. You must confirm that each FQDN can be accessed from any other host. For e.g. you can do this by using the `ping` command. If you have a DNS server on your network, contact your network administrator for assistance.

2. Step 2

If any previous version of TDengine has been installed and configured on any host, the installation needs to be removed and the data needs to be cleaned up. To clean up the data, please use `rm -rf /var/lib/taos/\*` assuming the `dataDir` is configured as `/var/lib/taos`.

:::note
FQDN information is written to file. If you have started TDengine without configuring or changing the FQDN, ensure that data is backed up or no longer needed before running the `rm -rf /var/lib\taos/\*` command.
:::

:::note
- The host where the client program runs also needs to be configured properly for FQDN, to make sure all hosts for client or server can be accessed from any other. In other words, the hosts where the client is running are also considered as a part of the cluster.
:::

3. Step 3

- Please ensure that your firewall rules do not block TCP/UDP on ports 6030-6042 on all hosts in the cluster.

4. Step 4

Now it's time to install TDengine on all hosts but without starting `taosd`. Note that the versions on all hosts should be same. If you are prompted to input the existing TDengine cluster, simply press carriage return to ignore the prompt.

5. Step 5

Now each physical node (referred to, hereinafter, as `dnode` which is an abbreviation for "data node") of TDengine needs to be configured properly.

To get the hostname on any host, the command `hostname -f` can be executed.

`ping <FQDN>` command can be executed on each host to check whether any other host is accessible from it. If any host is not accessible, the network configuration, like /etc/hosts or DNS configuration, needs to be checked and revised, to make any two hosts accessible to each other. Hosts that are not accessible to each other cannot form a cluster.

On the physical machine running the application, ping the dnode that is running taosd. If the dnode is not accessible, the application cannot connect to taosd. In this case, verify the DNS and hosts settings on the physical node running the application.

The end point of each dnode is the output hostname and port, such as h1.tdengine.com:6030.

6. Step 6

Modify the TDengine configuration file `/etc/taos/taos.cfg` on each node. Assuming the first dnode of TDengine cluster is "h1.tdengine.com:6030", its `taos.cfg` is configured as following.

```c
// firstEp is the end point to connect to when any dnode starts
firstEp               h1.tdengine.com:6030

// must be configured to the FQDN of the host where the dnode is launched
fqdn                  h1.tdengine.com

// the port used by the dnode, default is 6030
serverPort            6030

```

`firstEp` and `fqdn` must be configured properly. In `taos.cfg` of all dnodes in TDengine cluster, `firstEp` must be configured to point to same address, i.e. the first dnode of the cluster. `fqdn` and `serverPort` compose the address of each node itself. Retain the default values for other parameters.

For all the dnodes in a TDengine cluster, the below parameters must be configured exactly the same, any node whose configuration is different from dnodes already in the cluster can't join the cluster.

| **#** | **Parameter**    | **Definition**                                                                |
| ----- | ---------------- | ----------------------------------------------------------------------------- |
| 1     | statusInterval   | The interval by which dnode reports its status to mnode                       |
| 2     | timezone         | Timezone                                                                      |
| 3     | locale           | System region and encoding                                                    |
| 4     | charset          | Character set                                                                 |
| 5     | ttlChangeOnWrite | Whether the ttl expiration time changes with the table modification operation |

### Start Cluster

The first dnode can be started following the instructions in [Get Started](../../get-started/). Then TDengine CLI `taos` can be launched to execute command `show dnodes`, the output is as following for example:

```
taos> show dnodes;
id | endpoint | vnodes | support_vnodes | status | create_time | note |
============================================================================================================================================
1 | h1.tdengine.com:6030 | 0 | 1024 | ready | 2022-07-16 10:50:42.673 | |
Query OK, 1 rows affected (0.007984s)


```

From the above output, it is shown that the end point of the started dnode is "h1.tdengine.com:6030", which is the `firstEp` of the cluster.

### Add DNODE

There are a few steps necessary to add other dnodes in the cluster.

Second, we can start `taosd` as instructed in [Get Started](../../get-started/).

Then, on the first dnode i.e. h1.tdengine.com in our example, use TDengine CLI `taos` to execute the following command:

```sql
CREATE DNODE "h2.taos.com:6030";
````

This adds the end point of the new dnode (from Step 4) into the end point list of the cluster. In the command "fqdn:port" should be quoted using double quotes. Change `"h2.taos.com:6030"` to the end point of your new dnode.

Then on the first dnode h1.tdengine.com, execute `show dnodes` in `taos`

```sql
SHOW DNODES;
```

to show whether the second dnode has been added in the cluster successfully or not. If the status of the newly added dnode is offline, please check:

- Whether the `taosd` process is running properly or not
- In the log file `taosdlog.0` to see whether the fqdn and port are correct and add the correct end point if not.
The above process can be repeated to add more dnodes in the cluster.

:::tip

Any node that is in the cluster and online can be the firstEp of new nodes.
Nodes use the firstEp parameter only when joining a cluster for the first time. After a node has joined the cluster, it stores the latest mnode in its end point list and no longer makes use of firstEp.

However, firstEp is used by clients that connect to the cluster. For example, if you run TDengine CLI `taos` without arguments, it connects to the firstEp by default.

Two dnodes that are launched without a firstEp value operate independently of each other. It is not possible to add one dnode to the other dnode and form a cluster. It is also not possible to form two independent clusters into a new cluster.

:::

### Show DNODEs

The below command can be executed in TDengine CLI `taos`

```sql
SHOW DNODES;
```

to list all dnodes in the cluster, including ID, end point (fqdn:port), status (ready, offline), number of vnodes, number of free vnodes and so on. We recommend executing this command after adding or removing a dnode.

Below is the example output of this command.

```
taos> show dnodes;
   id   |            endpoint            | vnodes | support_vnodes |   status   |       create_time       |              note              |
============================================================================================================================================
      1 | trd01:6030                     |    100 |           1024 | ready      | 2022-07-15 16:47:47.726 |                                |
Query OK, 1 rows affected (0.006684s)
```

### Show VGROUPs

To utilize system resources efficiently and provide scalability, data sharding is required. The data of each database is divided into multiple shards and stored in multiple vnodes. These vnodes may be located on different dnodes. One way of scaling out is to add more vnodes on dnodes. Each vnode can only be used for a single DB, but one DB can have multiple vnodes. The allocation of vnode is scheduled automatically by mnode based on system resources of the dnodes.

Launch TDengine CLI `taos` and execute below command:

```sql
USE SOME_DATABASE;
SHOW VGROUPS;
```

The example output is below:

```
taos> use db;
Database changed.

taos> show vgroups;
  vgroup_id  |            db_name             |   tables    |  v1_dnode   | v1_status  |  v2_dnode   | v2_status  |  v3_dnode   | v3_status  |    status    |   nfiles    |  file_size  | tsma |
================================================================================================================================================================================================
           2 | db                             |           0 |           1 | leader     |        NULL | NULL       |        NULL | NULL       | NULL         |        NULL |        NULL |    0 |
           3 | db                             |           0 |           1 | leader     |        NULL | NULL       |        NULL | NULL       | NULL         |        NULL |        NULL |    0 |
           4 | db                             |           0 |           1 | leader     |        NULL | NULL       |        NULL | NULL       | NULL         |        NULL |        NULL |    0 |
Query OK, 8 row(s) in set (0.001154s)
```

### Drop DNODE

Before running the TDengine CLI, ensure that the taosd process has been stopped on the dnode that you want to delete.

```sql
DROP DNODE dnodeId;
```

to drop or remove a dnode from the cluster. In the command, you can get `dnodeId` from `show dnodes`.

:::warning

- Once a dnode is dropped, it can't rejoin the cluster. To rejoin, the dnode needs to deployed again after cleaning up the data directory. Before dropping a dnode, the data belonging to the dnode MUST be migrated/backed up according to your data retention, data security or other SOPs.
- Please note that `drop dnode` is different from stopping `taosd` process. `drop dnode` just removes the dnode out of TDengine cluster. Only after a dnode is dropped, can the corresponding `taosd` process be stopped.
- Once a dnode is dropped, other dnodes in the cluster will be notified of the drop and will not accept the request from the dropped dnode.
- dnodeID is allocated automatically and can't be manually modified. dnodeID is generated in ascending order without duplication.

:::

## Docker

This section describes how to start the TDengine service in a container and access it. Users can control the behavior of the service in the container by using environment variables on the docker run command-line or in the docker-compose file.

### Starting TDengine

The TDengine image starts with the HTTP service activated by default, using the following command:

```shell
docker run -d --name tdengine \
-v ~/data/taos/dnode/data:/var/lib/taos \
-v ~/data/taos/dnode/log:/var/log/taos \
-p 6041:6041 tdengine/tdengine
```
:::note

* /var/lib/taos: TDengine's default data file directory. The location can be changed via [configuration file]. And also you can modify ~/data/taos/dnode/data to your any other local emtpy data directory
* /var/log/taos: TDengine's default log file directory. The location can be changed via [configure file]. And also you can modify ~/data/taos/dnode/log to your any other local empty log directory
  

:::

The above command starts a container named "tdengine" and maps the HTTP service port 6041 to the host port 6041. You can verify that the HTTP service provided in this container is available using the following command.

```shell
curl -u root:taosdata -d "show databases" localhost:6041/rest/sql
```

The TDengine client taos can be executed in this container to access TDengine using the following command.

```shell
$ docker exec -it tdengine taos

taos> show databases;
              name              |
=================================
 information_schema             |
 performance_schema             |
Query OK, 2 row(s) in set (0.002843s)
```

The TDengine server running in the container uses the container's hostname to establish a connection. Using TDengine CLI or various client libraries (such as JDBC-JNI) to access the TDengine inside the container from outside the container is more complicated. So the above is the simplest way to access the TDengine service in the container and is suitable for some simple scenarios. Please refer to the next section if you want to access the TDengine service in the container from outside the container using TDengine CLI or various client libraries for complex scenarios.

### Start TDengine on the host network

```shell
docker run -d --name tdengine --network host tdengine/tdengine
```

The above command starts TDengine on the host network and uses the host's FQDN to establish a connection instead of the container's hostname. It is the equivalent of using `systemctl` to start TDengine on the host. If the TDengine client is already installed on the host, you can access it directly with the following command.

```shell
$ taos

taos> show dnodes;
   id   |           end_point            | vnodes | cores  |   status   | role  |       create_time       |      offline reason      |
======================================================================================================================================
      1 | myhost:6030           |      1 |      8 | ready      | any   | 2022-01-17 22:10:32.619 |                          |
Query OK, 1 row(s) in set (0.003233s)
```

### Start TDengine with the specified hostname and port

The `TAOS_FQDN` environment variable or the `fqdn` configuration item in `taos.cfg` allows TDengine to establish a connection at the specified hostname. This approach provides greater flexibility for deployment.

```shell
docker run -d \
   --name tdengine \
   -e TAOS_FQDN=tdengine \
   -p 6030-6049:6030-6049 \
   -p 6030-6049:6030-6049/udp \
   tdengine/tdengine
```

The above command starts a TDengine service in the container, which listens to the hostname tdengine, and maps the container's port segment 6030 to 6049 to the host's port segment 6030 to 6049 (both TCP and UDP ports need to be mapped). If the port segment is already occupied on the host, you can modify the above command to specify a free port segment on the host. If `rpcForceTcp` is set to `1`, you can map only the TCP protocol.

Next, ensure the hostname "tdengine" is resolvable in `/etc/hosts`.

```shell
echo 127.0.0.1 tdengine |sudo tee -a /etc/hosts
```

Finally, the TDengine service can be accessed from the TDengine CLI or any client library with "tdengine" as the server address.

```shell
taos -h tdengine -P 6030
```

If set `TAOS_FQDN` to the same hostname, the effect is the same as "Start TDengine on host network".

### Start TDengine on the specified network

You can also start TDengine on a specific network. Perform the following steps:

1. First, create a docker network named `td-net`

   ```shell
   docker network create td-net
   ```

2. Start TDengine

   Start the TDengine service on the `td-net` network with the following command:

   ```shell
   docker run -d --name tdengine --network td-net \
      -e TAOS_FQDN=tdengine \
      tdengine/tdengine
   ```

3. Start the TDengine client in another container on the same network

   ```shell
   docker run --rm -it --network td-net -e TAOS_FIRST_EP=tdengine tdengine/tdengine taos
   # or
   #docker run --rm -it --network td-net -e tdengine/tdengine taos -h tdengine
   ```

### Launching a client application in a container

If you want to start your application in a container, you need to add the corresponding dependencies on TDengine to the image as well, e.g.

```docker
FROM ubuntu:20.04
RUN apt-get update && apt-get install -y wget
ENV TDENGINE_VERSION=3.0.0.0
RUN wget -c https://tdengine.com/assets-download/3.0/TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz \
   && tar xvf TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz \
   && cd TDengine-client-${TDENGINE_VERSION} \
   && ./install_client.sh \
   && cd ../ \
   && rm -rf TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz TDengine-client-${TDENGINE_VERSION}
## add your application next, eg. go, build it in builder stage, copy the binary to the runtime
#COPY --from=builder /path/to/build/app /usr/bin/
#CMD ["app"]
```

Here is an example GO program:

```go
/*
 * In this test program, we'll create a database and insert 4 records then select out.
 */
package main

import (
    "database/sql"
    "flag"
    "fmt"
    "time"

    _ "github.com/taosdata/driver-go/v3/taosSql"
)

type config struct {
    hostName   string
    serverPort string
    user       string
    password   string
}

var configPara config
var taosDriverName = "taosSql"
var url string

func init() {
    flag.StringVar(&configPara.hostName, "h", "", "The host to connect to TDengine server.")
    flag.StringVar(&configPara.serverPort, "p", "", "The TCP/IP port number to use for the connection to TDengine server.")
    flag.StringVar(&configPara.user, "u", "root", "The TDengine user name to use when connecting to the server.")
    flag.StringVar(&configPara.password, "P", "taosdata", "The password to use when connecting to the server.")
    flag.Parse()
}

func printAllArgs() {
    fmt.Printf("============= args parse result: =============\n")
    fmt.Printf("hostName:             %v\n", configPara.hostName)
    fmt.Printf("serverPort:           %v\n", configPara.serverPort)
    fmt.Printf("usr:                  %v\n", configPara.user)
    fmt.Printf("password:             %v\n", configPara.password)
    fmt.Printf("================================================\n")
}

func main() {
    printAllArgs()

    url = "root:taosdata@/tcp(" + configPara.hostName + ":" + configPara.serverPort + ")/"

    taos, err := sql.Open(taosDriverName, url)
    checkErr(err, "open database error")
    defer taos.Close()

    taos.Exec("create database if not exists test")
    taos.Exec("use test")
    taos.Exec("create table if not exists tb1 (ts timestamp, a int)")
    _, err = taos.Exec("insert into tb1 values(now, 0)(now+1s,1)(now+2s,2)(now+3s,3)")
    checkErr(err, "failed to insert")
    rows, err := taos.Query("select * from tb1")
    checkErr(err, "failed to select")

    defer rows.Close()
    for rows.Next() {
        var r struct {
            ts time.Time
            a  int
        }
        err := rows.Scan(&r.ts, &r.a)
        if err != nil {
            fmt.Println("scan error:\n", err)
            return
        }
        fmt.Println(r.ts, r.a)
    }
}

func checkErr(err error, prompt string) {
    if err != nil {
        fmt.Println("ERROR: %s\n", prompt)
        panic(err)
    }
}
```

Here is the full Dockerfile:

```docker
FROM golang:1.17.6-buster as builder
ENV TDENGINE_VERSION=3.0.0.0
RUN wget -c https://tdengine.com/assets-download/3.0/TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz \
   && tar xvf TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz \
   && cd TDengine-client-${TDENGINE_VERSION} \
   && ./install_client.sh \
   && cd ../ \
   && rm -rf TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz TDengine-client-${TDENGINE_VERSION}
WORKDIR /usr/src/app/
ENV GOPROXY="https://goproxy.io,direct"
COPY ./main.go ./go.mod ./go.sum /usr/src/app/
RUN go env
RUN go mod tidy
RUN go build

FROM ubuntu:20.04
RUN apt-get update && apt-get install -y wget
ENV TDENGINE_VERSION=3.0.0.0
RUN wget -c https://tdengine.com/assets-download/3.0/TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz \
   && tar xvf TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz \
   && cd TDengine-client-${TDENGINE_VERSION} \
   && ./install_client.sh \
   && cd ../ \
   && rm -rf TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz TDengine-client-${TDENGINE_VERSION}

## add your application next, eg. go, build it in builder stage, copy the binary to the runtime
COPY --from=builder /usr/src/app/app /usr/bin/
CMD ["app"]
```

Now that we have `main.go`, `go.mod`, `go.sum`, `app.dockerfile`, we can build the application and start it on the `td-net` network.

```shell
$ docker build -t app -f app.dockerfile
$ docker run --rm --network td-net app -h tdengine -p 6030
============= args parse result: =============
hostName:             tdengine
serverPort:           6030
usr:                  root
password:             taosdata
================================================
2022-01-17 15:56:55.48 +0000 UTC 0
2022-01-17 15:56:56.48 +0000 UTC 1
2022-01-17 15:56:57.48 +0000 UTC 2
2022-01-17 15:56:58.48 +0000 UTC 3
2022-01-17 15:58:01.842 +0000 UTC 0
2022-01-17 15:58:02.842 +0000 UTC 1
2022-01-17 15:58:03.842 +0000 UTC 2
2022-01-17 15:58:04.842 +0000 UTC 3
2022-01-18 01:43:48.029 +0000 UTC 0
2022-01-18 01:43:49.029 +0000 UTC 1
2022-01-18 01:43:50.029 +0000 UTC 2
2022-01-18 01:43:51.029 +0000 UTC 3
```

### Start the TDengine cluster with docker-compose

1. The following docker-compose file starts a TDengine cluster with three nodes.

```yml
version: "3"
services:
  td-1:
    image: tdengine/tdengine:$VERSION
    environment:
      TAOS_FQDN: "td-1"
      TAOS_FIRST_EP: "td-1"
    ports:
      - 6041:6041
      - 6030:6030
    volumes:
      # /var/lib/taos: TDengine's default data file directory. The location can be changed via [configuration file]. you can modify ~/data/taos/dnode1/data to your own data directory
      - ~/data/taos/dnode1/data:/var/lib/taos
      # /var/log/taos: TDengine's default log file directory. The location can be changed via [configure file]. you can modify ~/data/taos/dnode1/log to your own log directory
      - ~/data/taos/dnode1/log:/var/log/taos
  td-2:
    image: tdengine/tdengine:$VERSION
    environment:
      TAOS_FQDN: "td-2"
      TAOS_FIRST_EP: "td-1"
    volumes:
      - ~/data/taos/dnode2/data:/var/lib/taos
      - ~/data/taos/dnode2/log:/var/log/taos
  td-3:
    image: tdengine/tdengine:$VERSION
    environment:
      TAOS_FQDN: "td-3"
      TAOS_FIRST_EP: "td-1"
    volumes:
      - ~/data/taos/dnode3/data:/var/lib/taos
      - ~/data/taos/dnode3/log:/var/log/taos
```

:::note

- The `VERSION` environment variable is used to set the tdengine image tag
- `TAOS_FIRST_EP` must be set on the newly created instance so that it can join the TDengine cluster; if there is a high availability requirement, `TAOS_SECOND_EP` needs to be used at the same time
  

:::

2. Start the cluster

   ```shell
   $ VERSION=3.0.0.0 docker-compose up -d
   Creating network "test_default" with the default driver
   Creating volume "test_taosdata-td1" with default driver
   Creating volume "test_taoslog-td1" with default driver
   Creating volume "test_taosdata-td2" with default driver
   Creating volume "test_taoslog-td2" with default driver
   Creating test_td-1_1       ... done
   Creating test_arbitrator_1 ... done
   Creating test_td-2_1       ... done
   ```

3. Check the status of each node

   ```shell
   $ docker-compose ps
         Name                     Command               State                                                                Ports
   ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   test_arbitrator_1   /usr/bin/entrypoint.sh tar ...   Up      6030/tcp, 6031/tcp, 6032/tcp, 6033/tcp, 6034/tcp, 6035/tcp, 6036/tcp, 6037/tcp, 6038/tcp, 6039/tcp, 6040/tcp, 6041/tcp, 6042/tcp
   test_td-1_1         /usr/bin/entrypoint.sh taosd     Up      6030/tcp, 6031/tcp, 6032/tcp, 6033/tcp, 6034/tcp, 6035/tcp, 6036/tcp, 6037/tcp, 6038/tcp, 6039/tcp, 6040/tcp, 6041/tcp, 6042/tcp
   test_td-2_1         /usr/bin/entrypoint.sh taosd     Up      6030/tcp, 6031/tcp, 6032/tcp, 6033/tcp, 6034/tcp, 6035/tcp, 6036/tcp, 6037/tcp, 6038/tcp, 6039/tcp, 6040/tcp, 6041/tcp, 6042/tcp
   ```

4. Show dnodes via TDengine CLI

```shell
$ docker-compose exec td-1 taos -s "show dnodes"

taos> show dnodes
     id      |            endpoint            | vnodes | support_vnodes |   status   |       create_time       |              note              |
======================================================================================================================================
           1 | td-1:6030                      |      0 |             32 | ready      | 2022-08-19 07:57:29.971 |                                |
           2 | td-2:6030                      |      0 |             32 | ready      | 2022-08-19 07:57:31.415 |                                |
           3 | td-3:6030                      |      0 |             32 | ready      | 2022-08-19 07:57:31.417 |                                |
Query OK, 3 rows in database (0.021262s)

```

### taosAdapter

1. taosAdapter is enabled by default in the TDengine container. If you want to disable it, specify the environment variable `TAOS_DISABLE_ADAPTER=true` at startup

2. At the same time, for flexible deployment, taosAdapter can be started in a separate container

   ```docker
   services:
     # ...
     adapter:
       image: tdengine/tdengine:$VERSION
       command: taosadapter
   ```

   Suppose you want to deploy multiple taosAdapters to improve throughput and provide high availability. In that case, the recommended configuration method uses a reverse proxy such as Nginx to offer a unified access entry. For specific configuration methods, please refer to the official documentation of Nginx. Here is an example:

```yml
version: "3"

networks:
  inter:

services:
  td-1:
    image: tdengine/tdengine:$VERSION
    environment:
      TAOS_FQDN: "td-1"
      TAOS_FIRST_EP: "td-1"
    volumes:
      # /var/lib/taos: TDengine's default data file directory. The location can be changed via [configuration file]. you can modify ~/data/taos/dnode1/data to your own data directory
      - ~/data/taos/dnode1/data:/var/lib/taos
      # /var/log/taos: TDengine's default log file directory. The location can be changed via [configure file]. you can modify ~/data/taos/dnode1/log to your own log directory
      - ~/data/taos/dnode1/log:/var/log/taos
  td-2:
    image: tdengine/tdengine:$VERSION
    environment:
      TAOS_FQDN: "td-2"
      TAOS_FIRST_EP: "td-1"
    volumes:
      - ~/data/taos/dnode2/data:/var/lib/taos
      - ~/data/taos/dnode2/log:/var/log/taos
  adapter:
    image: tdengine/tdengine:$VERSION
    entrypoint: "taosadapter"
    networks:
      - inter
    environment:
      TAOS_FIRST_EP: "td-1"
      TAOS_SECOND_EP: "td-2"
    deploy:
      replicas: 4
  nginx:
    image: nginx
    depends_on:
      - adapter
    networks:
      - inter
    ports:
      - 6041:6041
      - 6044:6044/udp
    command: [
        "sh",
        "-c",
        "while true;
        do curl -s http://adapter:6041/-/ping >/dev/null && break;
        done;
        printf 'server{listen 6041;location /{proxy_pass http://adapter:6041;}}'
        > /etc/nginx/conf.d/rest.conf;
        printf 'stream{server{listen 6044 udp;proxy_pass adapter:6044;}}'
        >> /etc/nginx/nginx.conf;cat /etc/nginx/nginx.conf;
        nginx -g 'daemon off;'",
      ]
```

### Deploy with docker swarm

If you want to deploy a container-based TDengine cluster on multiple hosts, you can use docker swarm. First, to establish a docker swarm cluster on these hosts, please refer to the official docker documentation.

The docker-compose file can refer to the previous section. Here is the command to start TDengine with docker swarm:

```shell
$ VERSION=3.0.0.0 docker stack deploy -c docker-compose.yml taos
Creating network taos_inter
Creating network taos_api
Creating service taos_arbitrator
Creating service taos_td-1
Creating service taos_td-2
Creating service taos_adapter
Creating service taos_nginx
```

Checking status:

```shell
$ docker stack ps taos
ID                  NAME                IMAGE                     NODE                DESIRED STATE       CURRENT STATE                ERROR               PORTS
79ni8temw59n        taos_nginx.1        nginx:latest              TM1701     Running             Running about a minute ago
3e94u72msiyg        taos_adapter.1      tdengine/tdengine:3.0.0.0   TM1702     Running             Running 56 seconds ago
100amjkwzsc6        taos_td-2.1         tdengine/tdengine:3.0.0.0   TM1703     Running             Running about a minute ago
pkjehr2vvaaa        taos_td-1.1         tdengine/tdengine:3.0.0.0   TM1704     Running             Running 2 minutes ago
tpzvgpsr1qkt        taos_arbitrator.1   tdengine/tdengine:3.0.0.0   TM1705     Running             Running 2 minutes ago
rvss3g5yg6fa        taos_adapter.2      tdengine/tdengine:3.0.0.0   TM1706     Running             Running 56 seconds ago
i2augxamfllf        taos_adapter.3      tdengine/tdengine:3.0.0.0   TM1707     Running             Running 56 seconds ago
lmjyhzccpvpg        taos_adapter.4      tdengine/tdengine:3.0.0.0   TM1708     Running             Running 56 seconds ago
$ docker service ls
ID                  NAME                MODE                REPLICAS            IMAGE                     PORTS
561t4lu6nfw6        taos_adapter        replicated          4/4                 tdengine/tdengine:3.0.0.0
3hk5ct3q90sm        taos_arbitrator     replicated          1/1                 tdengine/tdengine:3.0.0.0
d8qr52envqzu        taos_nginx          replicated          1/1                 nginx:latest              *:6041->6041/tcp, *:6044->6044/udp
2isssfvjk747        taos_td-1           replicated          1/1                 tdengine/tdengine:3.0.0.0
9pzw7u02ichv        taos_td-2           replicated          1/1                 tdengine/tdengine:3.0.0.0
```

From the above output, you can see two dnodes, two taosAdapters, and one Nginx reverse proxy service.

Next, we can reduce the number of taosAdapter services.

```shell
$ docker service scale taos_adapter=1
taos_adapter scaled to 1
overall progress: 1 out of 1 tasks
1/1: running   [==================================================>]
verify: Service converged

$ docker service ls -f name=taos_adapter
ID                  NAME                MODE                REPLICAS            IMAGE                     PORTS
561t4lu6nfw6        taos_adapter        replicated          1/1                 tdengine/tdengine:3.0.0.0
```

## Kubernetes

As a time series database for Cloud Native architecture design, TDengine supports Kubernetes deployment. Firstly we introduce how to use YAML files to create a highly available TDengine cluster from scratch step by step for production usage, and highlight the common operations of TDengine in Kubernetes environment.

To meet [high availability](../../tdinternal/high-availability/) requirements, clusters need to meet the following requirements:

- 3 or more dnodes: multiple vnodes in the same vgroup of TDengine are not allowed to be distributed in one dnode at the same time, so if you create a database with 3 replicas, the number of dnodes is greater than or equal to 3
- 3 mnodes: mnode is responsible for the management of the entire TDengine cluster. The default number of mnode in TDengine cluster is only one. If the dnode where the mnode located is dropped, the entire cluster is unavailable.
- Database 3 replicas: The TDengine replica configuration is the database level, so 3 replicas for the database must need three dnodes in the cluster. If any one dnode is offline, does not affect the normal usage of the whole cluster. **If the number of offline** **dnodes** **is 2, then the cluster is not available,** **because** ** the cluster can not complete the election based on RAFT** **.** (Enterprise version: in the disaster recovery scenario, any node data file is damaged, can be restored by pulling up the dnode again)

### Prerequisites

Before deploying TDengine on Kubernetes, perform the following:

- This article applies Kubernetes 1.19 and above
- This article uses the **kubectl** tool to install and deploy, please install the corresponding software in advance
- Kubernetes have been installed and deployed and can access or update the necessary container repositories or other services

You can download the configuration files in this document from [GitHub](https://github.com/taosdata/TDengine-Operator/tree/3.0/src/tdengine).

### Configure the service

Create a service configuration file named `taosd-service.yaml`. Record the value of `metadata.name` (in this example, `taos`) for use in the next step. And then add the ports required by TDengine and record the value of the selector label "app" (in this example, `tdengine`) for use in the next step:

```YAML
---
apiVersion: v1
kind: Service
metadata:
  name: "taosd"
  labels:
    app: "tdengine"
spec:
  ports:
    - name: tcp6030
      protocol: "TCP"
      port: 6030
    - name: tcp6041
      protocol: "TCP"
      port: 6041
  selector:
    app: "tdengine"
```

### Configure the service as StatefulSet

According to Kubernetes instructions for various deployments, we will use StatefulSet as the deployment resource type of TDengine. Create the file `tdengine.yaml `, where replicas defines the number of cluster nodes as 3. The node time zone is China (Asia/Shanghai), and each node is allocated 5G standard storage (refer to the [Storage Classes ](https://kubernetes.io/docs/concepts/storage/storage-classes/)configuration storage class). You can also modify accordingly according to the actual situation.

Please pay special attention to the startupProbe configuration. If dnode's Pod drops for a period of time and then restart, the newly launched dnode Pod will be temporarily unavailable. The reason is the startupProbe configuration is too small, Kubernetes will know that the Pod is in an abnormal state and try to restart it, then the dnode's Pod will restart frequently and never return to the normal status. Refer to [Configure Liveness, Readiness and Startup Probes](https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes/)

```YAML
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: "tdengine"
  labels:
    app: "tdengine"
spec:
  serviceName: "taosd"
  replicas: 3
  updateStrategy:
    type: RollingUpdate
  selector:
    matchLabels:
      app: "tdengine"
  template:
    metadata:
      name: "tdengine"
      labels:
        app: "tdengine"
    spec:
      containers:
        - name: "tdengine"
          image: "tdengine/tdengine:3.0.7.1"
          imagePullPolicy: "IfNotPresent"
          ports:
            - name: tcp6030
              protocol: "TCP"
              containerPort: 6030
            - name: tcp6041
              protocol: "TCP"
              containerPort: 6041
          env:
            # POD_NAME for FQDN config
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            # SERVICE_NAME and NAMESPACE for fqdn resolve
            - name: SERVICE_NAME
              value: "taosd"
            - name: STS_NAME
              value: "tdengine"
            - name: STS_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
            # TZ for timezone settings, we recommend to always set it.
            - name: TZ
              value: "Asia/Shanghai"
            # TAOS_ prefix will configured in taos.cfg, strip prefix and camelCase.
            - name: TAOS_SERVER_PORT
              value: "6030"
            # Must set if you want a cluster.
            - name: TAOS_FIRST_EP
              value: "$(STS_NAME)-0.$(SERVICE_NAME).$(STS_NAMESPACE).svc.cluster.local:$(TAOS_SERVER_PORT)"
            # TAOS_FQND should always be set in k8s env.
            - name: TAOS_FQDN
              value: "$(POD_NAME).$(SERVICE_NAME).$(STS_NAMESPACE).svc.cluster.local"
          volumeMounts:
            - name: taosdata
              mountPath: /var/lib/taos
          startupProbe:
            exec:
              command:
                - taos-check
            failureThreshold: 360
            periodSeconds: 10
          readinessProbe:
            exec:
              command:
                - taos-check
            initialDelaySeconds: 5
            timeoutSeconds: 5000
          livenessProbe:
            exec:
              command:
                - taos-check
            initialDelaySeconds: 15
            periodSeconds: 20
  volumeClaimTemplates:
    - metadata:
        name: taosdata
      spec:
        accessModes:
          - "ReadWriteOnce"
        storageClassName: "standard"
        resources:
          requests:
            storage: "5Gi"
```

### Use kubectl to deploy TDengine

First create the corresponding namespace, and then execute the following command in sequence :

```Bash
kubectl apply -f taosd-service.yaml -n tdengine-test
kubectl apply -f tdengine.yaml -n tdengine-test
```

The above configuration will generate a three-node TDengine cluster, dnode is automatically configured, you can use the **show dnodes** command to view the nodes of the current cluster:

```Bash
kubectl exec -it tdengine-0 -n tdengine-test -- taos -s "show dnodes"
kubectl exec -it tdengine-1 -n tdengine-test -- taos -s "show dnodes"
kubectl exec -it tdengine-2 -n tdengine-test -- taos -s "show dnodes"
```

The output is as follows:

```Bash
taos> show dnodes
     id      | endpoint         | vnodes | support_vnodes |   status   |       create_time       |       reboot_time       |              note              |          active_code           |         c_active_code          |
=============================================================================================================================================================================================================================================
           1 | tdengine-0.ta... |      0 |             16 | ready      | 2023-07-19 17:54:18.552 | 2023-07-19 17:54:18.469 |                                |                                |                                |
           2 | tdengine-1.ta... |      0 |             16 | ready      | 2023-07-19 17:54:37.828 | 2023-07-19 17:54:38.698 |                                |                                |                                |
           3 | tdengine-2.ta... |      0 |             16 | ready      | 2023-07-19 17:55:01.141 | 2023-07-19 17:55:02.039 |                                |                                |                                |
Query OK, 3 row(s) in set (0.001853s)
```

View the current mnode

```Bash
kubectl exec -it tdengine-1 -n tdengine-test -- taos -s "show mnodes\G"
taos> show mnodes\G
*************************** 1.row ***************************
         id: 1
   endpoint: tdengine-0.taosd.tdengine-test.svc.cluster.local:6030
       role: leader
     status: ready
create_time: 2023-07-19 17:54:18.559
reboot_time: 2023-07-19 17:54:19.520
Query OK, 1 row(s) in set (0.001282s)
```

### Create mnode

```Bash
kubectl exec -it tdengine-0 -n tdengine-test -- taos -s "create mnode on dnode 2"
kubectl exec -it tdengine-0 -n tdengine-test -- taos -s "create mnode on dnode 3"
```

View mnode

```Bash
kubectl exec -it tdengine-1 -n tdengine-test -- taos -s "show mnodes\G"

taos> show mnodes\G
*************************** 1.row ***************************
         id: 1
   endpoint: tdengine-0.taosd.tdengine-test.svc.cluster.local:6030
       role: leader
     status: ready
create_time: 2023-07-19 17:54:18.559
reboot_time: 2023-07-20 09:19:36.060
*************************** 2.row ***************************
         id: 2
   endpoint: tdengine-1.taosd.tdengine-test.svc.cluster.local:6030
       role: follower
     status: ready
create_time: 2023-07-20 09:22:05.600
reboot_time: 2023-07-20 09:22:12.838
*************************** 3.row ***************************
         id: 3
   endpoint: tdengine-2.taosd.tdengine-test.svc.cluster.local:6030
       role: follower
     status: ready
create_time: 2023-07-20 09:22:20.042
reboot_time: 2023-07-20 09:22:23.271
Query OK, 3 row(s) in set (0.003108s)
```

### Enable port forwarding

Kubectl port forwarding enables applications to access TDengine clusters running in Kubernetes environments.

```bash
kubectl port-forward -n tdengine-test tdengine-0 6041:6041 &
```

Use **curl** to verify that the TDengine REST API is working on port 6041:

```bash
curl -u root:taosdata -d "show databases" 127.0.0.1:6041/rest/sql
{"code":0,"column_meta":[["name","VARCHAR",64]],"data":[["information_schema"],["performance_schema"],["test"],["test1"]],"rows":4}
```

## Helm

Helm is a package manager for Kubernetes that can provide more capabilities in deploying on Kubernetes.

### Install Helm

```bash
curl -fsSL -o get_helm.sh \
  https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3
chmod +x get_helm.sh
./get_helm.sh

```

Helm uses the kubectl and kubeconfig configurations to perform Kubernetes operations. For more information, see the Rancher configuration for Kubernetes installation.

### Install TDengine Chart

To use TDengine Chart, download it from GitHub:

```bash
wget https://github.com/taosdata/TDengine-Operator/raw/3.0/helm/tdengine-3.0.2.tgz

```

Query the storageclass of your Kubernetes deployment:

```bash
kubectl get storageclass

```

With minikube, the default value is standard.

Use Helm commands to install TDengine:

```bash
helm install tdengine tdengine-3.0.2.tgz \
  --set storage.className=<your storage class name>

```

You can configure a small storage size in minikube to ensure that your deployment does not exceed your available disk space.

```bash
helm install tdengine tdengine-3.0.2.tgz \
  --set storage.className=standard \
  --set storage.dataSize=2Gi \
  --set storage.logSize=10Mi

```

After TDengine is deployed, TDengine Chart outputs information about how to use TDengine:

```bash
export POD_NAME=$(kubectl get pods --namespace default \
  -l "app.kubernetes.io/name=tdengine,app.kubernetes.io/instance=tdengine" \
  -o jsonpath="{.items[0].metadata.name}")
kubectl --namespace default exec $POD_NAME -- taos -s "show dnodes; show mnodes"
kubectl --namespace default exec -it $POD_NAME -- taos

```

You can test the deployment by creating a table:

```bash
kubectl --namespace default exec $POD_NAME -- \
  taos -s "create database test;
    use test;
    create table t1 (ts timestamp, n int);
    insert into t1 values(now, 1)(now + 1s, 2);
    select * from t1;"

```

### Configuring Values

You can configure custom parameters in TDengine with the `values.yaml` file.

Run the `helm show values` command to see all parameters supported by TDengine Chart.

```bash
helm show values tdengine-3.0.2.tgz

```

Save the output of this command as `values.yaml`. Then you can modify this file with your desired values and use it to deploy a TDengine cluster:

```bash
helm install tdengine tdengine-3.0.2.tgz -f values.yaml

```

The parameters are described as follows:

```yaml
# Default values for tdengine.
# This is a YAML-formatted file.
# Declare variables to be passed into helm templates.

replicaCount: 1

image:
  prefix: tdengine/tdengine
  #pullPolicy: Always
  # Overrides the image tag whose default is the chart appVersion.
#  tag: "3.0.2.0"

service:
  # ClusterIP is the default service type, use NodeIP only if you know what you are doing.
  type: ClusterIP
  ports:
    # TCP range required
    tcp: [6030, 6041, 6042, 6043, 6044, 6046, 6047, 6048, 6049, 6060]
    # UDP range
    udp: [6044, 6045]


# Set timezone here, not in taoscfg
timezone: "Asia/Shanghai"

resources:
  # We usually recommend not to specify default resources and to leave this as a conscious
  # choice for the user. This also increases chances charts run on environments with little
  # resources, such as Minikube. If you do want to specify resources, uncomment the following
  # lines, adjust them as necessary, and remove the curly braces after 'resources:'.
  # limits:
  #   cpu: 100m
  #   memory: 128Mi
  # requests:
  #   cpu: 100m
  #   memory: 128Mi

storage:
  # Set storageClassName for pvc. K8s use default storage class if not set.
  #
  className: ""
  dataSize: "100Gi"
  logSize: "10Gi"

nodeSelectors:
  taosd:
    # node selectors

clusterDomainSuffix: ""
# Config settings in taos.cfg file.
#
# The helm/k8s support will use environment variables for taos.cfg,
# converting an upper-snake-cased variable like `TAOS_DEBUG_FLAG`,
# to a camelCase taos config variable `debugFlag`.
#
# See the [Configuration Variables](../../reference/config)
#
# Note:
# 1. firstEp/secondEp: should not be set here, it's auto generated at scale-up.
# 2. serverPort: should not be set, we'll use the default 6030 in many places.
# 3. fqdn: will be auto generated in kubernetes, user should not care about it.
# 4. role: currently role is not supported - every node is able to be mnode and vnode.
#
# Btw, keep quotes "" around the value like below, even the value will be number or not.
taoscfg:
  # Starts as cluster or not, must be 0 or 1.
  #   0: all pods will start as a separate TDengine server
  #   1: pods will start as TDengine server cluster. [default]
  CLUSTER: "1"

  # number of replications, for cluster only
  TAOS_REPLICA: "1"

  #
  # TAOS_NUM_OF_RPC_THREADS: number of threads for RPC
  #TAOS_NUM_OF_RPC_THREADS: "2"


  #
  # TAOS_NUM_OF_COMMIT_THREADS: number of threads to commit cache data
  #TAOS_NUM_OF_COMMIT_THREADS: "4"

  # enable/disable installation / usage report
  #TAOS_TELEMETRY_REPORTING: "1"

  # time interval of system monitor, seconds
  #TAOS_MONITOR_INTERVAL: "30"

  # time interval of dnode status reporting to mnode, seconds, for cluster only
  #TAOS_STATUS_INTERVAL: "1"

  # time interval of heart beat from shell to dnode, seconds
  #TAOS_SHELL_ACTIVITY_TIMER: "3"

  # minimum sliding window time, milli-second
  #TAOS_MIN_SLIDING_TIME: "10"

  # minimum time window, milli-second
  #TAOS_MIN_INTERVAL_TIME: "1"

  # the compressed rpc message, option:
  #  -1 (no compression)
  #   0 (all message compressed),
  # > 0 (rpc message body which larger than this value will be compressed)
  #TAOS_COMPRESS_MSG_SIZE: "-1"

  # max number of connections allowed in dnode
  #TAOS_MAX_SHELL_CONNS: "50000"

  # stop writing logs when the disk size of the log folder is less than this value
  #TAOS_MINIMAL_LOG_DIR_G_B: "0.1"

  # stop writing temporary files when the disk size of the tmp folder is less than this value
  #TAOS_MINIMAL_TMP_DIR_G_B: "0.1"

  # if disk free space is less than this value, taosd service exit directly within startup process
  #TAOS_MINIMAL_DATA_DIR_G_B: "0.1"

  # One mnode is equal to the number of vnode consumed
  #TAOS_MNODE_EQUAL_VNODE_NUM: "4"

  # enbale/disable http service
  #TAOS_HTTP: "1"

  # enable/disable system monitor
  #TAOS_MONITOR: "1"

  # enable/disable async log
  #TAOS_ASYNC_LOG: "1"

  #
  # time of keeping log files, days
  #TAOS_LOG_KEEP_DAYS: "0"

  # The following parameters are used for debug purpose only.
  # debugFlag 8 bits mask: FILE-SCREEN-UNUSED-HeartBeat-DUMP-TRACE_WARN-ERROR
  # 131: output warning and error
  # 135: output debug, warning and error
  # 143: output trace, debug, warning and error to log
  # 199: output debug, warning and error to both screen and file
  # 207: output trace, debug, warning and error to both screen and file
  #
  # debug flag for all log type, take effect when non-zero value\
  #TAOS_DEBUG_FLAG: "143"

  # generate core file when service crash
  #TAOS_ENABLE_CORE_FILE: "1"
```

### Scaling Out

For information about scaling out your deployment, see Kubernetes. Additional Helm-specific is described as follows.

First, obtain the name of the StatefulSet service for your deployment.

```bash
export STS_NAME=$(kubectl get statefulset \
  -l "app.kubernetes.io/name=tdengine" \
  -o jsonpath="{.items[0].metadata.name}")

```

You can scale out your deployment by adding replicas. The following command scales a deployment to three nodes:

```bash
kubectl scale --replicas 3 statefulset/$STS_NAME

```

Run the `show dnodes` and `show mnodes` commands to check whether the scale-out was successful.

### Scaling In

:::warning
Exercise caution when scaling in a cluster.

:::

Determine which dnodes you want to remove and drop them manually.

```bash
kubectl --namespace default exec $POD_NAME -- \
  cat /var/lib/taos/dnode/dnodeEps.json \
  | jq '.dnodeInfos[1:] |map(.dnodeFqdn + ":" + (.dnodePort|tostring)) | .[]' -r
kubectl --namespace default exec $POD_NAME -- taos -s "show dnodes"
kubectl --namespace default exec $POD_NAME -- taos -s 'drop dnode "<you dnode in list>"'

```

### Remove a TDengine Cluster

You can use Helm to remove your cluster:

```bash
helm uninstall tdengine

```

However, Helm does not remove PVCs automatically. After you remove your cluster, manually remove all PVCs.
