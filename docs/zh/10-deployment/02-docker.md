---
title: 用 Docker 部署 TDengine
sidebar_label: Docker
description: '本章主要介绍如何在容器中启动 TDengine 服务并访问它'
---

本章主要介绍如何在容器中启动 TDengine 服务并访问它。可以在 docker run 命令行中或者 docker-compose 文件中使用环境变量来控制容器中服务的行为。

## 启动 TDengine

TDengine 镜像启动时默认激活 HTTP 服务，使用下列命令

```shell
docker run -d --name tdengine \
-v ~/data/taos/dnode/data:/var/lib/taos \
-v ~/data/taos/dnode/log:/var/log/taos \
-p 6041:6041 tdengine/tdengine
```
:::note

- /var/lib/taos: TDengine 默认数据文件目录。可通过[配置文件]修改位置。你可以修改~/data/taos/dnode/data为你自己的数据目录
- /var/log/taos: TDengine 默认日志文件目录。可通过[配置文件]修改位置。你可以修改~/data/taos/dnode/log为你自己的日志目录

:::

以上命令启动了一个名为“tdengine”的容器，并把其中的 HTTP 服务的端 6041 映射到了主机端口 6041。使用如下命令可以验证该容器中提供的 HTTP 服务是否可用：

```shell
curl -u root:taosdata -d "show databases" localhost:6041/rest/sql
```

使用如下命令可以在该容器中执行 TDengine 的客户端 taos 对 TDengine 进行访问：

```shell
$ docker exec -it tdengine taos

taos> show databases;
              name              |
=================================
 information_schema             |
 performance_schema             |
Query OK, 2 rows in database (0.033802s)
```

因为运行在容器中的 TDengine 服务端使用容器的 hostname 建立连接，使用 TDengine CLI 或者各种连接器（例如 JDBC-JNI）从容器外访问容器内的 TDengine 比较复杂，所以上述方式是访问容器中 TDengine 服务的最简单的方法，适用于一些简单场景。如果在一些复杂场景下想要从容器化使用 TDengine CLI 或者各种连接器访问容器中的 TDengine 服务，请参考下一节。

## 在 host 网络上启动 TDengine

```shell
docker run -d --name tdengine --network host tdengine/tdengine
```

上面的命令在 host 网络上启动 TDengine，并使用主机的 FQDN 建立连接而不是使用容器的 hostname 。这种方式和在主机上使用 `systemctl` 启动 TDengine 效果相同。在主机已安装 TDengine 客户端情况下，可以直接使用下面的命令访问它。

```shell
$ taos

taos> show dnodes;
     id      |            endpoint            | vnodes | support_vnodes |   status   |       create_time       |              note              |
=================================================================================================================================================
           1 | vm98:6030                      |      0 |             32 | ready      | 2022-08-19 14:50:05.337 |                                |
Query OK, 1 rows in database (0.010654s)

```

## 以指定的 hostname 和 port 启动 TDengine

利用 `TAOS_FQDN` 环境变量或者 `taos.cfg` 中的 `fqdn` 配置项可以使 TDengine 在指定的 hostname 上建立连接。这种方式可以为部署提供更大的灵活性。

```shell
docker run -d \
   --name tdengine \
   -e TAOS_FQDN=tdengine \
   -p 6030:6030 \
   -p 6041-6049:6041-6049 \
   -p 6041-6049:6041-6049/udp \
   tdengine/tdengine
```

上面的命令在容器中启动一个 TDengine 服务，其所监听的 hostname 为 tdengine ，并将容器的 6030 端口映射到主机的 6030 端口（TCP，只能映射主机 6030 端口），6041-6049 端口段映射到主机 6041-6049 端口段（tcp 和 udp 都需要映射，如果主机上该端口段已经被占用，可以修改上述命令指定一个主机上空闲的端口段）。

接下来，要确保 "tdengine" 这个 hostname 在 `/etc/hosts` 中可解析。

```shell
echo 127.0.0.1 tdengine |sudo tee -a /etc/hosts
```

最后，可以从 TDengine CLI 或者任意连接器以 "tdengine" 为服务端地址访问 TDengine 服务。

```shell
taos -h tdengine -P 6030
```

如果 `TAOS_FQDN` 被设置为与所在主机名相同，则效果与 “在 host 网络上启动 TDengine” 相同。

## 在指定网络上启动 TDengine

也可以在指定的特定网络上启动 TDengine。下面是详细步骤：

1. 首先，创建一个 docker 网络，命名为 td-net

   ```shell
   docker network create td-net
   ```

2. 启动 TDengine

   以下命令在 td-net 网络上启动 TDengine 服务

   ```shell
   docker run -d --name tdengine --network td-net \
      -e TAOS_FQDN=tdengine \
      tdengine/tdengine
   ```

3. 在同一网络上的另一容器中启动 TDengine 客户端

   ```shell
   docker run --rm -it --network td-net -e TAOS_FIRST_EP=tdengine --entrypoint=taos tdengine/tdengine
   # or
   #docker run --rm -it --network td-net --entrypoint=taos tdengine/tdengine -h tdengine
   ```

## 在容器中启动客户端应用

如果想在容器中启动自己的应用的话，需要将相应的对 TDengine 的依赖也要加入到镜像中，例如：

```docker
FROM ubuntu:20.04
RUN apt-get update && apt-get install -y wget
ENV TDENGINE_VERSION=3.0.0.0
RUN wget -c https://www.tdengine.com/assets-download/3.0/TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz \
   && tar xvf TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz \
   && cd TDengine-client-${TDENGINE_VERSION} \
   && ./install_client.sh \
   && cd ../ \
   && rm -rf TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz TDengine-client-${TDENGINE_VERSION}
## add your application next, eg. go, build it in builder stage, copy the binary to the runtime
#COPY --from=builder /path/to/build/app /usr/bin/
#CMD ["app"]
```

以下是一个 go 应用程序的示例：

* 创建 go mod 项目：

```bash
go mod init app
```

* 创建 main.go：

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

* 更新 go mod

```bash
go mod tidy
```

如下是完整版本的 dockerfile：

```dockerfile
FROM golang:1.19.0-buster as builder
ENV TDENGINE_VERSION=3.0.0.0
RUN wget -c https://www.tdengine.com/assets-download/3.0/TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz \
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
RUN wget -c https://www.tdengine.com/assets-download/3.0/TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz \
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
$ docker build -t app -f app.dockerfile .
$ docker run --rm --network td-net app app -h tdengine -p 6030
============= args parse result: =============
hostName:             tdengine
serverPort:           6030
usr:                  root
password:             taosdata
================================================
2022-08-19 07:43:51.68 +0000 UTC 0
2022-08-19 07:43:52.68 +0000 UTC 1
2022-08-19 07:43:53.68 +0000 UTC 2
2022-08-19 07:43:54.68 +0000 UTC 3
```

## 用 docker-compose 启动 TDengine 集群

1. 如下 docker-compose 文件启动一个 三节点 TDengine 集群。

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
      # /var/lib/taos: TDengine 默认数据文件目录。可通过[配置文件]修改位置。你可以修改~/data/taos/dnode1/data为你自己的数据目录
      - ~/data/taos/dnode1/data:/var/lib/taos
      # /var/log/taos: TDengine 默认日志文件目录。可通过[配置文件]修改位置。你可以修改~/data/taos/dnode1/log为你自己的日志目录
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

* `VERSION` 环境变量被用来设置 tdengine image tag
* 在新创建的实例上必须设置 `TAOS_FIRST_EP` 以使其能够加入 TDengine 集群；如果有高可用需求，则需要同时使用 `TAOS_SECOND_EP`

:::

2. 启动集群

```shell
$ VERSION=3.0.0.0 docker-compose up -d
Creating network "test-docker_default" with the default driver
Creating volume "test-docker_taosdata-td1" with default driver
Creating volume "test-docker_taoslog-td1" with default driver
Creating volume "test-docker_taosdata-td2" with default driver
Creating volume "test-docker_taoslog-td2" with default driver
Creating volume "test-docker_taosdata-td3" with default driver
Creating volume "test-docker_taoslog-td3" with default driver

Creating test-docker_td-3_1 ... done
Creating test-docker_td-1_1 ... done
Creating test-docker_td-2_1 ... done
```

3. 查看节点状态

```shell
   docker-compose ps
       Name                     Command               State   Ports

-------------------------------------------------------------------
test-docker_td-1_1   /tini -- /usr/bin/entrypoi ...   Up
test-docker_td-2_1   /tini -- /usr/bin/entrypoi ...   Up
test-docker_td-3_1   /tini -- /usr/bin/entrypoi ...   Up
```

4. 用 TDengine CLI 查看 dnodes

```shell

$ docker-compose exec td-1 taos -s "show dnodes"

taos> show dnodes

     id      |            endpoint            | vnodes | support_vnodes |   status   |       create_time       |              note              |
=================================================================================================================================================

           1 | td-1:6030                      |      0 |             32 | ready      | 2022-08-19 07:57:29.971 |                                |
           2 | td-2:6030                      |      0 |             32 | ready      | 2022-08-19 07:57:31.415 |                                |
           3 | td-3:6030                      |      0 |             32 | ready      | 2022-08-19 07:57:31.417 |                                |
Query OK, 3 rows in database (0.021262s)

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
      # /var/lib/taos: TDengine 默认数据文件目录。可通过[配置文件]修改位置。你可以修改~/data/taos/dnode1/data为你自己的数据目录
      - ~/data/taos/dnode1/data:/var/lib/taos
      # /var/log/taos: TDengine 默认日志文件目录。可通过[配置文件]修改位置。你可以修改~/data/taos/dnode1/log为你自己的日志目录
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

## 使用 docker swarm 部署

如果要想将基于容器的 TDengine 集群部署在多台主机上，可以使用 docker swarm。首先要在这些主机上建立 docke swarm 集群，请参考 docker 官方文档。

docker-compose 文件可以参考上节。下面是使用 docker swarm 启动 TDengine 的命令：

```shell
$ VERSION=3.0.0.0 docker stack deploy -c docker-compose.yml taos
Creating network taos_inter
Creating service taos_nginx
Creating service taos_td-1
Creating service taos_td-2
Creating service taos_adapter
```

查看和管理

```shell
$ docker stack ps taos
ID             NAME             IMAGE                       NODE      DESIRED STATE   CURRENT STATE                ERROR     PORTS
7m3sbf532bqp   taos_adapter.1   tdengine/tdengine:3.0.0.0   vm98      Running         Running about a minute ago
pj403n6ofmmh   taos_adapter.2   tdengine/tdengine:3.0.0.0   vm98      Running         Running about a minute ago
rxqfwsyk5q1h   taos_adapter.3   tdengine/tdengine:3.0.0.0   vm98      Running         Running about a minute ago
qj40lpxr40oc   taos_adapter.4   tdengine/tdengine:3.0.0.0   vm98      Running         Running about a minute ago
oe3455ulxpze   taos_nginx.1     nginx:latest                vm98      Running         Running about a minute ago
o0tsg70nrrc6   taos_td-1.1      tdengine/tdengine:3.0.0.0   vm98      Running         Running about a minute ago
q5m1oxs589cp   taos_td-2.1      tdengine/tdengine:3.0.0.0   vm98      Running         Running about a minute ago
$ docker service ls
ID             NAME           MODE         REPLICAS   IMAGE                       PORTS
ozuklorgl8bs   taos_adapter   replicated   4/4        tdengine/tdengine:3.0.0.0
crmhdjw6vxw0   taos_nginx     replicated   1/1        nginx:latest                *:6041->6041/tcp, *:6044->6044/udp
o86ngy7csv5n   taos_td-1      replicated   1/1        tdengine/tdengine:3.0.0.0
rma040ny4tb0   taos_td-2      replicated   1/1        tdengine/tdengine:3.0.0.0
```

从上面的输出可以看到有两个 dnode， 和四个 taosAdapter，以及一个 nginx 反向代理服务。

接下来，我们可以减少 taosAdapter 服务的数量

```shell
$ docker service scale taos_adapter=1
taos_adapter scaled to 1
overall progress: 1 out of 1 tasks
1/1: running   [==================================================>]
verify: Service converged

$ docker service ls -f name=taos_adapter
ID             NAME           MODE         REPLICAS   IMAGE                       PORTS
ozuklorgl8bs   taos_adapter   replicated   1/1        tdengine/tdengine:3.0.0.0
```
