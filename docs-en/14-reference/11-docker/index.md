---
title: Deploying TDengine with Docker
Description: "This chapter focuses on starting the TDengine service in a container and accessing it."
---

This chapter describes how to start the TDengine service in a container and access it. Users can control the behavior of the service in the container by using environment variables on the docker run command line or in the docker-compose file.

## Starting TDengine

The TDengine image starts with the HTTP service activated by default, using the following command:

```shell
docker run -d --name tdengine -p 6041:6041 tdengine/tdengine
```

The above command starts a container named "tdengine" and maps the HTTP service end 6041 to the host port 6041. You can verify that the HTTP service provided in this container is available using the following command.

```shell
curl -u root:taosdata -d "show databases" localhost:6041/rest/sql
```

The TDengine client taos can be executed in this container to access TDengine using the following command.

```shell
$ docker exec -it tdengine taos

Welcome to the TDengine shell from Linux, Client Version:2.4.0.0
Copyright (c) 2020 by TAOS Data, Inc.

taos> show databases;
              name | created_time | ntables | vgroups | replica | quorum | days | keep | cache(MB) | blocks | minrows | maxrows | wallevel | fsync | comp | cachelast | precision | update | status | status precision | update | status |
================================================================================================================================== ================================================================================================================================== ================
 log | 2022-01-17 13:57:22.270 | 10 | 1 | 1 | 1 | 10 | 30 | 1 | 3 | 100 | 4096 | 1 | 3000 | 2 | 0 | us | 0 | ready |
Query OK, 1 row(s) in set (0.002843s)
```

The TDengine server running in the container uses the container's hostname to establish a connection. Using TDengine CLI or various connectors (such as JDBC-JNI) to access the TDengine inside the container from outside the container is more complicated. So the above is the simplest way to access the TDengine service in the container and is suitable for some simple scenarios. Please refer to the next section if you want to access the TDengine service in the container from containerized using TDengine CLI or various connectors in some complex scenarios.

## Start TDengine on the host network

```shell
docker run -d --name tdengine --network host tdengine/tdengine
```

The above command starts TDengine on the host network and uses the host's FQDN to establish a connection instead of the container's hostname. It works too, like using `systemctl` to start TDengine on the host. If the TDengine client is already installed on the host, you can access it directly with the following command.

```shell
$ taos

Welcome to the TDengine shell from Linux, Client Version:2.4.0.0
Copyright (c) 2020 by TAOS Data, Inc.

taos> show dnodes;
   id | end_point | vnodes | cores | status | role | create_time | offline reason |
================================================================================================================================== ====
      1 | myhost:6030 | 1 | 8 | ready | any | 2022-01-17 22:10:32.619 | |
Query OK, 1 row(s) in set (0.003233s)
```

## Start TDengine with the specified hostname and port

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

Finally, the TDengine service can be accessed from the taos shell or any connector with "tdengine" as the server address.

```shell
taos -h tdengine -P 6030
```

If set `TAOS_FQDN` to the same hostname, the effect is the same as "Start TDengine on host network".

## Start TDengine on the specified network

You can also start TDengine on a specific network.

1. First, create a docker network named `td-net`

   ```shell
   docker network create td-net
   ``` Create td-net

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
   # docker run --rm -it --network td-net -e tdengine/tdengine taos -h tdengine
   ```

## Launching a client application in a container

If you want to start your application in a container, you need to add the corresponding dependencies on TDengine to the image as well, e.g.

```docker
FROM ubuntu:20.04
RUN apt-get update && apt-get install -y wget
ENV TDENGINE_VERSION=2.4.0.0
RUN wget -c https://www.taosdata.com/assets-download/TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz \
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

    _ "github.com/taosdata/driver-go/v2/taosSql"
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
ENV TDENGINE_VERSION=2.4.0.0
RUN wget -c https://www.taosdata.com/assets-download/TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz \
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
ENV TDENGINE_VERSION=2.4.0.0
RUN wget -c https://www.taosdata.com/assets-download/TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz \
   && tar xvf TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz \
   && cd TDengine-client-${TDENGINE_VERSION} \
   && ./install_client.sh \
   && cd ../ \
   && rm -rf TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz TDengine-client-${TDENGINE_VERSION}

## add your application next, eg. go, build it in builder stage, copy the binary to the runtime
COPY --from=builder /usr/src/app/app /usr/bin/
CMD ["app"]
```

目前我们已经有了 `main.go`, `go.mod`, `go.sum`, `app.dockerfile`， 现在可以构建出这个应用程序并在 `td-net` 网络上启动它

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

## 用 docker-compose 启动 TDengine 集群

1. 如下 docker-compose 文件启动一个 2 副本、2 管理节点、2 数据节点以及 1 个 arbitrator 的 TDengine 集群。

   ```docker
   version: "3"
   services:
     arbitrator:
       image: tdengine/tdengine:$VERSION
       command: tarbitrator
     td-1:
       image: tdengine/tdengine:$VERSION
       environment:
         TAOS_FQDN: "td-1"
         TAOS_FIRST_EP: "td-1"
         TAOS_NUM_OF_MNODES: "2"
         TAOS_REPLICA: "2"
         TAOS_ARBITRATOR: arbitrator:6042
       volumes:
         - taosdata-td1:/var/lib/taos/
         - taoslog-td1:/var/log/taos/
     td-2:
       image: tdengine/tdengine:$VERSION
       environment:
         TAOS_FQDN: "td-2"
         TAOS_FIRST_EP: "td-1"
         TAOS_NUM_OF_MNODES: "2"
         TAOS_REPLICA: "2"
         TAOS_ARBITRATOR: arbitrator:6042
       volumes:
         - taosdata-td2:/var/lib/taos/
         - taoslog-td2:/var/log/taos/
   volumes:
     taosdata-td1:
     taoslog-td1:
     taosdata-td2:
     taoslog-td2:
   ```

  :::note
   - `VERSION` 环境变量被用来设置 tdengine image tag
   - 在新创建的实例上必须设置 `TAOS_FIRST_EP` 以使其能够加入 TDengine 集群；如果有高可用需求，则需要同时使用 `TAOS_SECOND_EP`
   - `TAOS_REPLICA` 用来设置缺省的数据库副本数量，其取值范围为[1,3]
     在双副本环境下，推荐使用 arbitrator, 用 TAOS_ARBITRATOR 来设置
  :::


2. 启动集群

   ```shell
   $ VERSION=2.4.0.0 docker-compose up -d
   Creating network "test_default" with the default driver
   Creating volume "test_taosdata-td1" with default driver
   Creating volume "test_taoslog-td1" with default driver
   Creating volume "test_taosdata-td2" with default driver
   Creating volume "test_taoslog-td2" with default driver
   Creating test_td-1_1       ... done
   Creating test_arbitrator_1 ... done
   Creating test_td-2_1       ... done
   ```

3. 查看节点状态

   ```shell
   $ docker-compose ps
         Name                     Command               State                                                                Ports
   ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
   test_arbitrator_1   /usr/bin/entrypoint.sh tar ...   Up      6030/tcp, 6031/tcp, 6032/tcp, 6033/tcp, 6034/tcp, 6035/tcp, 6036/tcp, 6037/tcp, 6038/tcp, 6039/tcp, 6040/tcp, 6041/tcp, 6042/tcp
   test_td-1_1         /usr/bin/entrypoint.sh taosd     Up      6030/tcp, 6031/tcp, 6032/tcp, 6033/tcp, 6034/tcp, 6035/tcp, 6036/tcp, 6037/tcp, 6038/tcp, 6039/tcp, 6040/tcp, 6041/tcp, 6042/tcp
   test_td-2_1         /usr/bin/entrypoint.sh taosd     Up      6030/tcp, 6031/tcp, 6032/tcp, 6033/tcp, 6034/tcp, 6035/tcp, 6036/tcp, 6037/tcp, 6038/tcp, 6039/tcp, 6040/tcp, 6041/tcp, 6042/tcp
   ```

4. 用 taos shell 查看 dnodes

   ```shell
   $ docker-compose exec td-1 taos -s "show dnodes"

   Welcome to the TDengine shell from Linux, Client Version:2.4.0.0
   Copyright (c) 2020 by TAOS Data, Inc. All rights reserved.

   taos> show dnodes
      id   |           end_point            | vnodes | cores  |   status   | role  |       create_time       |      offline reason      |
   ======================================================================================================================================
         1 | td-1:6030                      |      1 |      8 | ready      | any   | 2022-01-18 02:47:42.871 |                          |
         2 | td-2:6030                      |      0 |      8 | ready      | any   | 2022-01-18 02:47:43.518 |                          |
         0 | arbitrator:6042                |      0 |      0 | ready      | arb   | 2022-01-18 02:47:43.633 | -                        |
   Query OK, 3 row(s) in set (0.000811s)
   ```

## taosAdapter

1. taosAdapter 在 TDengine 容器中默认是启动的。如果想要禁用它，在启动时指定环境变量 `TAOS_DISABLE_ADAPTER=true`

2. 同时为了部署灵活起见，可以在独立的容器中启动 taosAdapter

   ```docker
   services:
     # ...
     adapter:
       image: tdengine/tdengine:$VERSION
       command: taosadapter
   ```

   如果要部署多个 taosAdapter 来提高吞吐量并提供高可用性，推荐配置方式为使用 nginx 等反向代理来提供统一的访问入口。具体配置方法请参考 nginx 的官方文档。如下是示例：

   ```docker
   ersion: "3"

   networks:
     inter:
     api:

   services:
     arbitrator:
       image: tdengine/tdengine:$VERSION
       command: tarbitrator
       networks:
         - inter
     td-1:
       image: tdengine/tdengine:$VERSION
       networks:
         - inter
       environment:
         TAOS_FQDN: "td-1"
         TAOS_FIRST_EP: "td-1"
         TAOS_NUM_OF_MNODES: "2"
         TAOS_REPLICA: "2"
         TAOS_ARBITRATOR: arbitrator:6042
       volumes:
         - taosdata-td1:/var/lib/taos/
         - taoslog-td1:/var/log/taos/
     td-2:
       image: tdengine/tdengine:$VERSION
       networks:
         - inter
       environment:
         TAOS_FQDN: "td-2"
         TAOS_FIRST_EP: "td-1"
         TAOS_NUM_OF_MNODES: "2"
         TAOS_REPLICA: "2"
         TAOS_ARBITRATOR: arbitrator:6042
       volumes:
         - taosdata-td2:/var/lib/taos/
         - taoslog-td2:/var/log/taos/
     adapter:
       image: tdengine/tdengine:$VERSION
       command: taosadapter
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
         - api
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
   volumes:
     taosdata-td1:
     taoslog-td1:
     taosdata-td2:
     taoslog-td2:
   ```

## 使用 docker swarm 部署

如果要想将基于容器的 TDengine 集群部署在多台主机上，可以使用 docker swarm。首先要在这些主机上建立 docke swarm 集群，请参考 docker 官方文档。

docker-compose 文件可以参考上节。下面是使用 docker swarm 启动 TDengine 的命令：

```shell
$ VERSION=2.4.0 docker stack deploy -c docker-compose.yml taos
Creating network taos_inter
Creating network taos_api
Creating service taos_arbitrator
Creating service taos_td-1
Creating service taos_td-2
Creating service taos_adapter
Creating service taos_nginx
```

查看和管理

```shell
$ docker stack ps taos
ID                  NAME                IMAGE                     NODE                DESIRED STATE       CURRENT STATE                ERROR               PORTS
79ni8temw59n        taos_nginx.1        nginx:latest              TM1701     Running             Running about a minute ago
3e94u72msiyg        taos_adapter.1      tdengine/tdengine:2.4.0   TM1702     Running             Running 56 seconds ago
100amjkwzsc6        taos_td-2.1         tdengine/tdengine:2.4.0   TM1703     Running             Running about a minute ago
pkjehr2vvaaa        taos_td-1.1         tdengine/tdengine:2.4.0   TM1704     Running             Running 2 minutes ago
tpzvgpsr1qkt        taos_arbitrator.1   tdengine/tdengine:2.4.0   TM1705     Running             Running 2 minutes ago
rvss3g5yg6fa        taos_adapter.2      tdengine/tdengine:2.4.0   TM1706     Running             Running 56 seconds ago
i2augxamfllf        taos_adapter.3      tdengine/tdengine:2.4.0   TM1707     Running             Running 56 seconds ago
lmjyhzccpvpg        taos_adapter.4      tdengine/tdengine:2.4.0   TM1708     Running             Running 56 seconds ago
$ docker service ls
ID                  NAME                MODE                REPLICAS            IMAGE                     PORTS
561t4lu6nfw6        taos_adapter        replicated          4/4                 tdengine/tdengine:2.4.0
3hk5ct3q90sm        taos_arbitrator     replicated          1/1                 tdengine/tdengine:2.4.0
d8qr52envqzu        taos_nginx          replicated          1/1                 nginx:latest              *:6041->6041/tcp, *:6044->6044/udp
2isssfvjk747        taos_td-1           replicated          1/1                 tdengine/tdengine:2.4.0
9pzw7u02ichv        taos_td-2           replicated          1/1                 tdengine/tdengine:2.4.0
```

从上面的输出可以看到有两个 dnode， 和两个 taosAdapter，以及一个 nginx 反向代理服务。

接下来，我们可以减少 taosAdapter 服务的数量

```shell
$ docker service scale taos_adapter=1
taos_adapter scaled to 1
overall progress: 1 out of 1 tasks
1/1: running   [==================================================>]
verify: Service converged

$ docker service ls -f name=taos_adapter
ID                  NAME                MODE                REPLICAS            IMAGE                     PORTS
561t4lu6nfw6        taos_adapter        replicated          1/1                 tdengine/tdengine:2.4.0
```
