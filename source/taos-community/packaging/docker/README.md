# TDengine Docker Image Quick Reference

## What is TDengine?

TDengine is an open-sourced big data platform under [GNU AGPL v3.0](http://www.gnu.org/licenses/agpl-3.0.html), designed and optimized for the Internet of Things (IoT), Connected Cars, Industrial IoT, and IT Infrastructure and Application Monitoring. Besides the 10x faster time-series database, it provides caching, stream computing, message queuing and other functionalities to reduce the complexity and cost of development and operation.

- **10x Faster on Insert/Query Speeds**: Through the innovative design on storage, on a single-core machine, over 20K requests can be processed, millions of data points can be ingested, and over 10 million data points can be retrieved in a second. It is 10 times faster than other databases.

- **1/5 Hardware/Cloud Service Costs**: Compared with typical big data solutions, less than 1/5 of computing resources are required. Via column-based storage and tuned compression algorithms for different data types, less than 1/10 of storage space is needed.

- **Full Stack for Time-Series Data**: By integrating a database with message queuing, caching, and stream computing features together, it is no longer necessary to integrate Kafka/Redis/HBase/Spark or other software. It makes the system architecture much simpler and more robust.

- **Powerful Data Analysis**: Whether it is 10 years or one minute ago, data can be queried just by specifying the time range. Data can be aggregated over time, multiple time streams or both. Ad Hoc queries or analyses can be executed via TDengine shell, Python, R or Matlab.

- **Seamless Integration with Other Tools**: Telegraf, Grafana, Matlab, R, and other tools can be integrated with TDengine without a line of code. MQTT, OPC, Hadoop, Spark, and many others will be integrated soon.

- **Zero Management, No Learning Curve**: It takes only seconds to download, install, and run it successfully; there are no other dependencies. Automatic partitioning on tables or DBs. Standard SQL is used, with C/C++, Python, JDBC, Go and RESTful connectors.

## How to use this image

### Starting TDengine

The TDengine image starts with the HTTP service activated by default, using the following command:

```shell
docker run -d --name tdengine -p 6041:6041 tdengine/tdengine
```

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

The TDengine server running in the container uses the container's hostname to establish a connection. Using TDengine CLI or various connectors (such as JDBC-JNI) to access the TDengine inside the container from outside the container is more complicated. So the above is the simplest way to access the TDengine service in the container and is suitable for some simple scenarios. Please refer to the next section if you want to access the TDengine service in the container from outside the container using TDengine CLI or various connectors for complex scenarios.

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

Finally, the TDengine service can be accessed from the TDengine CLI or any connector with "tdengine" as the server address.

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
RUN wget -c https://www.taosdata.com/assets-download/3.0/TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz \
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
RUN wget -c https://www.taosdata.com/assets-download/3.0/TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz \
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
RUN wget -c https://www.taosdata.com/assets-download/3.0/TDengine-client-${TDENGINE_VERSION}-Linux-x64.tar.gz \
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
    volumes:
      - taosdata-td1:/var/lib/taos/
      - taoslog-td1:/var/log/taos/
  td-2:
    image: tdengine/tdengine:$VERSION
    environment:
      TAOS_FQDN: "td-2"
      TAOS_FIRST_EP: "td-1"
    volumes:
      - taosdata-td2:/var/lib/taos/
      - taoslog-td2:/var/log/taos/
  td-3:
    image: tdengine/tdengine:$VERSION
    environment:
      TAOS_FQDN: "td-3"
      TAOS_FIRST_EP: "td-1"
    volumes:
      - taosdata-td3:/var/lib/taos/
      - taoslog-td3:/var/log/taos/
volumes:
  taosdata-td1:
  taoslog-td1:
  taosdata-td2:
  taoslog-td2:
  taosdata-td3:
  taoslog-td3:
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

## taosAdapter

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
    networks:
      - inter
    environment:
      TAOS_FQDN: "td-1"
      TAOS_FIRST_EP: "td-1"
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
    volumes:
      - taosdata-td2:/var/lib/taos/
      - taoslog-td2:/var/log/taos/
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
volumes:
  taosdata-td1:
  taoslog-td1:
  taosdata-td2:
  taoslog-td2:
```

## Deploy with docker swarm

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
