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

### Start a TDengine instance with RESTful API exposed

Simply, you can use `docker run` to start a TDengine instance and connect it with restful connectors(eg. [JDBC-RESTful](https://www.taosdata.com/cn/documentation/connector/java)).

```bash
docker run -d --name tdengine -p 6041:6041 tdengine/tdengine
```

This command starts a docker container by name `tdengine` with TDengine server running, and maps the container's HTTP port 6041 to the host's port 6041. If you have `curl` in your host, you can list the databases by the command:

```bash
curl -u root:taosdata -d "show databases" localhost:6041/rest/sql
```

You can execute the `taos` shell command in the container:

```bash
$ docker exec -it tdengine taos

Welcome to the TDengine shell from Linux, Client Version:2.4.0.0
Copyright (c) 2020 by TAOS Data, Inc. All rights reserved.

taos> show databases;
              name              |      created_time       |   ntables   |   vgroups   | replica | quorum |  days  |           keep           |  cache(MB)  |   blocks    |   minrows   |   maxrows   | wallevel |    fsync    | comp | cachelast | precision | update |   status   |
====================================================================================================================================================================================================================================================================================
 log                            | 2022-01-17 13:57:22.270 |          10 |           1 |       1 |      1 |     10 | 30                       |           1 |           3 |         100 |        4096 |        1 |        3000 |    2 |         0 | us        |      0 | ready      |
Query OK, 1 row(s) in set (0.002843s)
```

Since TDengine use container hostname to establish connections, it's a bit more complex to use taos shell and native connectors(such as JDBC-JNI) with TDengine container instance. This is the recommended way to expose ports and use TDengine with docker in simple cases. If you want to use taos shell or taosc/connectors smoothly outside the `tdengine` container, see next use cases that match you need.

### Start with host network

```bash
docker run -d --name tdengine --network host tdengine/tdengine
```

Starts container with `host` network will use host's hostname as fqdn instead of container id. It's much like starting natively with `systemd` in host. After installing the client, you can use `taos` shell as normal in host path.

```bash
$ taos

Welcome to the TDengine shell from Linux, Client Version:2.4.0.0
Copyright (c) 2020 by TAOS Data, Inc. All rights reserved.

taos> show dnodes;
   id   |           end_point            | vnodes | cores  |   status   | role  |       create_time       |      offline reason      |
======================================================================================================================================
      1 | host:6030           |      1 |      8 | ready      | any   | 2022-01-17 22:10:32.619 |                          |
Query OK, 1 row(s) in set (0.003233s)
```

### Start with exposed ports and specified hostname

Set the fqdn explicitly will help you to use in other environment or applications. We provide environment variable `TAOS_FQDN` or `fqdn` config option to explicitly set the hostname used by TDengine container instance(s).

Use `TAOS_FQDN` variable within `docker run` command:

```bash
docker run -d \
   --name tdengine \
   -e TAOS_FQDN=tdengine \
   -p 6030-6049:6030-6049 \
   -p 6030-6049:6030-6049/udp \
   tdengine/tdengine
```

This command starts a docker container with TDengine server running and maps the container's TCP ports from 6030 to 6049 to the host's ports from 6030 to 6049 with TCP protocol and UDP ports range 6030-6039 to the host's UDP ports 6030-6039. If the host is already running TDengine server and occupying the same port(s), you need to map the container's port to a different unused port segment. (Please see TDengine 2.0 Port Description for details). In order to support TDengine clients accessing TDengine server services, both TCP and UDP ports need to be exposed by default(unless `rpcForceTcp` is set to `1`).

If you want to use taos shell or native connectors([JDBC-JNI](https://www.taosdata.com/cn/documentation/connector/java), or [driver-go](https://github.com/taosdata/driver-go)), you need to make sure the `TAOS_FQDN` is resolvable at `/etc/hosts` or with custom DNS service.

If you set the `TAOS_FQDN` to host's hostname, it will works as using `hosts` network like previous use case. Otherwise, like in `-e TAOS_FQDN=tdengine`, you can add the hostname record `tdengine` into `/etc/hosts` (use `127.0.0.1` here in host path, if use TDengine client/application in other hosts, you should set the right ip to the host eg. `192.168.10.1`(check the real ip in host with `hostname -i` or `ip route list default`) to make the TDengine endpoint resolvable):

```bash
echo 127.0.0.1 tdengine |sudo tee -a /etc/hosts
```

Then you can use `taos` with the host `tdengine`:

```bash
taos -h tdengine
```

Or develop/test applications with native connectors. As in python:

```python
import taos;
conn = taos.connect(host = "tdengine")
res = conn.query("show databases")
for row in res.fetch_all_into_dict():
   print(row)
```

See the results:

```bash
Python 3.8.10 (default, Nov 26 2021, 20:14:08) 
[GCC 9.3.0] on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> import taos;
>>> conn = taos.connect(host = "tdengine")
>>> res = conn.query("show databases")
>>> for row in res.fetch_all_into_dict():
...    print(row)
... 
{'name': 'log', 'created_time': datetime.datetime(2022, 1, 17, 22, 56, 2, 490000), 'ntables': 11, 'vgroups': 1, 'replica': 1, 'quorum': 1, 'days': 10, 'keep': '30', 'cache(MB)': 1, 'blocks': 3, 'minrows': 100, 'maxrows': 4096, 'wallevel': 1, 'fsync': 3000, 'comp': 2, 'cachelast': 0, 'precision': 'us', 'update': 0, 'status': 'ready'}
```

### Start with specific network

Alternatively, you can use TDengine natively by using specific network.

First, create network for TDengine server and client/application.

```bash
docker network create td-net
```

Start TDengine instance with service name as fqdn (explicitly set with `TAOS_FQDN`):

```bash
docker run -d --name tdengine --network td-net \
   -e TAOS_FQDN=tdengine \
   tdengine/tdengine
```

Start TDengine client in another container with the specific network:

```bash
docker run --rm -it --network td-net -e TAOS_FIRST_EP=tdengine tdengine/tdengine taos
# or
docker run --rm -it --network td-net -e tdengine/tdengine taos -h tdengine
```

When you build your application with docker, you should add the TDengine client in the dockerfile, as based on `ubuntu:20.04` image, install the client like this:

```dockerfile
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

Here is an Go example app:

<!-- code-spell-checker:disable -->
<!-- markdownlint-disable MD010 -->

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

<!-- markdownlint-enable MD010 -->
<!-- code-spell-checker:enable -->

Full version of dockerfile could be:

```dockerfile
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
RUN go env && go mod tidy && go build

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

Suppose you have `main.go`, `go.mod` `go.sum`, `app.dockerfile`, build the app and run it with network `td-net`:

```bash
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

Now you must be much familiar with developing and testing with TDengine, let's see some more complex cases.

### Start with docker-compose with multiple nodes(instances)

Start a 2-replicas-2-mnodes-2-dnodes-1-arbitrator TDengine cluster with `docker-compose` is quite simple. Save the file as `docker-compose.yml`:

```yaml
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

You may notice that:

- We use `VERSION` environment variable to set `tdengine` image tag version once.
- **`TAOS_FIRST_EP`** **MUST** be set to join the newly created instances into an existing TDengine cluster. If you want more instances, use `TAOS_SECOND_EP` in case of HA(High Availability) concerns.
- `TAOS_NUM_OF_MNODES` is for setting number of mnodes for the cluster.
- `TAOS_REPLICA` set the default database replicas, `2` means there're one master and one slave copy of data. The `replica` option should be `1 <= replica <= 3`, and not greater than dnodes number.
- `TAOS_ARBITRATOR` set the arbitrator entrypoint of the cluster for failover/election stuff. It's better to use arbitrator in a two nodes cluster.
- The way to start an arbitrator service is as easy as abc: just add command name `tarbitrator`(which is the binary name of arbitrator daemon) in docker-compose service option: `command: tarbitrator`, and everything is ok now.

Now run `docker-compose up -d` with version specified:

```bash
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

Check the status:

```bash
$ docker-compose ps
      Name                     Command               State                                                                Ports                                                              
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
test_arbitrator_1   /usr/bin/entrypoint.sh tar ...   Up      6030/tcp, 6031/tcp, 6032/tcp, 6033/tcp, 6034/tcp, 6035/tcp, 6036/tcp, 6037/tcp, 6038/tcp, 6039/tcp, 6040/tcp, 6041/tcp, 6042/tcp
test_td-1_1         /usr/bin/entrypoint.sh taosd     Up      6030/tcp, 6031/tcp, 6032/tcp, 6033/tcp, 6034/tcp, 6035/tcp, 6036/tcp, 6037/tcp, 6038/tcp, 6039/tcp, 6040/tcp, 6041/tcp, 6042/tcp
test_td-2_1         /usr/bin/entrypoint.sh taosd     Up      6030/tcp, 6031/tcp, 6032/tcp, 6033/tcp, 6034/tcp, 6035/tcp, 6036/tcp, 6037/tcp, 6038/tcp, 6039/tcp, 6040/tcp, 6041/tcp, 6042/tcp
```

Check dnodes with taos shell:

```bash
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

### Start a TDengine cluster with scaled taosadapter service

In previous use case, you could see the way to start other services built with TDengine(`taosd` as the default command). There's another important service you should know:

> **taosAdapter** is a TDengine’s companion tool and is a bridge/adapter between TDengine cluster and application. It provides an easy-to-use and efficient way to ingest data from data collections agents(like Telegraf, StatsD, CollectD) directly. It also provides InfluxDB/OpenTSDB compatible data ingestion interface to allow InfluxDB/OpenTSDB applications to immigrate to TDengine seamlessly.

`taosadapter` is running inside `tdengine` image by default, you can disable it by `TAOS_DISABLE_ADAPTER=true`. Running `taosadapter` in a separate container is like how `arbitrator` does:

```yaml
services:
  # ...
  adapter:
    image: tdengine/tdengine:$VERSION
    command: taosadapter
```

`taosadapter` could be scaled with docker-compose, so that you can manage the `taosadapter` nodes easily. Here is an example shows 4-`taosadapter` instances in a TDengine cluster(much like previous use cases):

```yaml
version: "3"

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

Start the cluster:

```bash
$ VERSION=2.4.0.0 docker-compose up -d
Creating network "docker_inter" with the default driver
Creating network "docker_api" with the default driver
Creating volume "docker_taosdata-td1" with default driver
Creating volume "docker_taoslog-td1" with default driver
Creating volume "docker_taosdata-td2" with default driver
Creating volume "docker_taoslog-td2" with default driver
Creating docker_td-2_1       ... done
Creating docker_arbitrator_1 ... done
Creating docker_td-1_1       ... done
Creating docker_adapter_1    ... done
Creating docker_adapter_2    ... done
Creating docker_adapter_3    ... done
```

It will start a TDengine cluster with two dnodes and four taosadapter instances, expose ports 6041/tcp and 6044/udp to host.

`6041` is the RESTful API endpoint port, you can verify that the RESTful interface taosAdapter provides working using the `curl` command.

```bash
$ curl -H 'Authorization: Basic cm9vdDp0YW9zZGF0YQ==' -d 'show databases;' 127.0.0.1:6041/rest/sql
{"status":"succ","head":["name","created_time","ntables","vgroups","replica","quorum","days","keep","cache(MB)","blocks","minrows","maxrows","wallevel","fsync","comp","cachelast","precision","update","status"],"column_meta":[["name",8,32],["created_time",9,8],["ntables",4,4],["vgroups",4,4],["replica",3,2],["quorum",3,2],["days",3,2],["keep",8,24],["cache(MB)",4,4],["blocks",4,4],["minrows",4,4],["maxrows",4,4],["wallevel",2,1],["fsync",4,4],["comp",2,1],["cachelast",2,1],["precision",8,3],["update",2,1],["status",8,10]],"data":[["log","2022-01-18 04:37:42.902",16,1,1,1,10,"30",1,3,100,4096,1,3000,2,0,"us",0,"ready"]],"rows":1}
```

If you run curl in batch(here we use [hyperfine](https://github.com/sharkdp/hyperfine) - a command-line benchmarking tool), the requests are balanced into 4 adapter instances.

```bash
hyperfine -m10 'curl -u root:taosdata localhost:6041/rest/sql -d "describe log.log"'
```

View the logs with `docker-compose logs`:

```bash
$ docker-compose logs adapter
# some logs skipped
adapter_2     | 01/18 04:57:44.616529 00000039 TAOS_ADAPTER info "| 200 |     162.185µs |      172.21.0.9 | POST | /rest/sql " model=web sessionID=18
adapter_1     | 01/18 04:57:44.627695 00000039 TAOS_ADAPTER info "| 200 |     145.485µs |      172.21.0.9 | POST | /rest/sql " model=web sessionID=17
adapter_3     | 01/18 04:57:44.639165 00000040 TAOS_ADAPTER info "| 200 |     146.913µs |      172.21.0.9 | POST | /rest/sql " sessionID=17 model=web
adapter_4     | 01/18 04:57:44.650829 00000039 TAOS_ADAPTER info "| 200 |     153.201µs |      172.21.0.9 | POST | /rest/sql " sessionID=17 model=web
adapter_2     | 01/18 04:57:44.662422 00000039 TAOS_ADAPTER info "| 200 |     211.393µs |      172.21.0.9 | POST | /rest/sql " model=web sessionID=19
adapter_1     | 01/18 04:57:44.673426 00000039 TAOS_ADAPTER info "| 200 |     154.714µs |      172.21.0.9 | POST | /rest/sql " model=web sessionID=18
adapter_3     | 01/18 04:57:44.684788 00000040 TAOS_ADAPTER info "| 200 |     131.876µs |      172.21.0.9 | POST | /rest/sql " model=web sessionID=18
adapter_4     | 01/18 04:57:44.696261 00000039 TAOS_ADAPTER info "| 200 |     162.173µs |      172.21.0.9 | POST | /rest/sql " model=web sessionID=18
adapter_2     | 01/18 04:57:44.707414 00000039 TAOS_ADAPTER info "| 200 |     164.419µs |      172.21.0.9 | POST | /rest/sql " model=web sessionID=20
adapter_1     | 01/18 04:57:44.720842 00000039 TAOS_ADAPTER info "| 200 |     179.374µs |      172.21.0.9 | POST | /rest/sql " model=web sessionID=19
adapter_3     | 01/18 04:57:44.732184 00000040 TAOS_ADAPTER info "| 200 |     141.174µs |      172.21.0.9 | POST | /rest/sql " sessionID=19 model=web
adapter_4     | 01/18 04:57:44.744024 00000039 TAOS_ADAPTER info "| 200 |     159.774µs |      172.21.0.9 | POST | /rest/sql " model=web sessionID=19
adapter_2     | 01/18 04:57:44.773732 00000039 TAOS_ADAPTER info "| 200 |     178.993µs |      172.21.0.9 | POST | /rest/sql " model=web sessionID=21
adapter_1     | 01/18 04:57:44.796518 00000039 TAOS_ADAPTER info "| 200 |      238.24µs |      172.21.0.9 | POST | /rest/sql " model=web sessionID=20
adapter_3     | 01/18 04:57:44.810744 00000040 TAOS_ADAPTER info "| 200 |     176.133µs |      172.21.0.9 | POST | /rest/sql " model=web sessionID=20
adapter_4     | 01/18 04:57:44.826395 00000039 TAOS_ADAPTER info "| 200 |     149.215µs |      172.21.0.9 | POST | /rest/sql " model=web sessionID=20
```

`6044/udp` is the [StatsD](https://github.com/statsd/statsd)-compatible port, you can verify this feature with `nc` command(usually provided by `netcat` package).

```bash
echo "foo:1|c" | nc -u -w0 127.0.0.1 6044
```

Check the result in `taos` shell with `docker-compose exec`:

```bash
$ dc exec td-1 taos

Welcome to the TDengine shell from Linux, Client Version:2.4.0.0
Copyright (c) 2020 by TAOS Data, Inc. All rights reserved.

taos> show databases;
              name              |      created_time       |   ntables   |   vgroups   | replica | quorum |  days  |           keep           |  cache(MB)  |   blocks    |   minrows   |   maxrows   | wallevel |    fsync    | comp | cachelast | precision | update |   status   |
====================================================================================================================================================================================================================================================================================
 log                            | 2022-01-18 04:37:42.902 |          17 |           1 |       1 |      1 |     10 | 30                       |           1 |           3 |         100 |        4096 |        1 |        3000 |    2 |         0 | us        |      0 | ready      |
 statsd                         | 2022-01-18 04:45:02.563 |           1 |           1 |       2 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ns        |      2 | ready      |
Query OK, 2 row(s) in set (0.001838s)

taos> select * from statsd.foo;
              ts               |         value         |         metric_type          |
=======================================================================================
 2022-01-18 04:45:02.563422822 |                     1 | counter                      |
Query OK, 1 row(s) in set (0.003854s)
```

Use `docker-compose up -d adapter=1 to reduce the instances to 1

### Deploy TDengine cluster in Docker Swarm with `docker-compose.yml`

If you use docker swarm mode, it will schedule arbitrator/taosd/taosadapter services into different hosts automatically. If you've no experience with k8s/kubernetes, this is the most convenient way to scale out the TDengine cluster with multiple hosts/servers.

Use the `docker-compose.yml` file in previous use case, and deploy with `docker stack` or `docker deploy`:

```bash
$ VERSION=2.4.0 docker stack deploy -c docker-compose.yml taos
Creating network taos_inter
Creating network taos_api
Creating service taos_arbitrator
Creating service taos_td-1
Creating service taos_td-2
Creating service taos_adapter
Creating service taos_nginx
```

Now you've created a TDengine cluster with multiple host servers.

Use `docker service` or `docker stack` to manage the cluster:

<!-- code-spell-checker:disable -->

```bash
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

<!-- code-spell-checker:enable -->

It shows that there are two dnodes, one arbitrator, four taosadapter and one nginx reverse-forward service in this cluster.

You can scale down the taosadapter replicas to `1` by `docker service`:

```bash
$ docker service scale taos_adapter=1
taos_adapter scaled to 1
overall progress: 1 out of 1 tasks 
1/1: running   [==================================================>] 
verify: Service converged

$ docker service ls -f name=taos_adapter
ID                  NAME                MODE                REPLICAS            IMAGE                     PORTS
561t4lu6nfw6        taos_adapter        replicated          1/1                 tdengine/tdengine:2.4.0
```

Now it remains only 1 taosadapter instance in the cluster.

When you want to remove the cluster, just type:

```bash
docker stack rm taos
```

### Environment Variables

When you start `tdengine` image, you can adjust the configuration of TDengine by passing environment variables on the `docker run` command line or in the docker compose file. You can use all of the environment variables that passed to taosd or taosadapter.
