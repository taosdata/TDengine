# TaosKeeper

TDengine Metrics Exporter for Kinds of Collectors, you can obtain the running status of TDengine by performing several simple configurations.

This tool uses TDengine RESTful API, so you could just build it without TDengine client.

## Build

### Get the source codes

```sh
git clone https://github.com/taosdata/TDengine
cd TDengine/tools/keeper
```

### compile

```sh
go mod tidy
go build
```

## Install

If you build the tool by your self, just copy the `taoskeeper` binary to your `PATH`.

```sh
sudo install taoskeeper /usr/bin/
```

## Start

Before start, you should configure some options like database IP, port or the prefix and others for exported metrics.

in `/etc/taos/taoskeeper.toml`.

```toml
# Start with debug middleware for gin
debug = false

# Listen port, default is 6043
port = 6043

# log level
loglevel = "info"

# go pool size
gopoolsize = 50000

# interval for TDengine metrics
RotationInterval = "15s"

[tdengine]
host = "127.0.0.1"
port = 6041
username = "root"
password = "taosdata"

# list of taosAdapter that need to be monitored
[taosAdapter]
address = ["127.0.0.1:6041"]

[metrics]
# metrics prefix in metrics names.
prefix = "taos"

# database for storing metrics data
database = "log"

# export some tables that are not super table
tables = []

[environment]
# Whether running in cgroup.
incgroup = false
```

Now you could run the tool:

```sh
taoskeeper
```

If you use `systemd`, copy the `taoskeeper.service` to `/lib/systemd/system/` and start the service.

```sh
sudo cp taoskeeper.service /lib/systemd/system/
sudo systemctl daemon-reload
sudo systemctl start taoskeeper
```

To start taoskeeper whenever OS rebooted, you should enable the systemd service:

```sh
sudo systemctl enable taoskeeper
```

So if use `systemd`, you'd better install it with these lines all-in-one:

```sh
go mod tidy
go build
sudo install taoskeeper /usr/bin/
sudo cp taoskeeper.service /lib/systemd/system/
sudo systemctl daemon-reload
sudo systemctl start taoskeeper
sudo systemctl enable taoskeeper
```

## Docker

Here is an example to show how to build this tool in docker:

Before building, you should configure `./config/taoskeeper.toml` with proper parameters and edit Dockerfile. Take following as example.

```dockerfile
FROM golang:1.18.2 as builder

WORKDIR /usr/src/taoskeeper
COPY ./ /usr/src/taoskeeper/
ENV GO111MODULE=on \
    GOPROXY=https://goproxy.cn,direct
RUN go mod tidy && go build

FROM alpine:3
RUN mkdir -p /etc/taos
COPY --from=builder /usr/src/taoskeeper/taoskeeper /usr/bin/
COPY ./config/taoskeeper.toml /etc/taos/taoskeeper.toml
EXPOSE 6043
CMD ["taoskeeper"]
```

If you already have taosKeeper binary file, you can build this tool like:

```dockerfile
FROM ubuntu:18.04
RUN mkdir -p /etc/taos
COPY ./taoskeeper /usr/bin/
COPY ./taoskeeper.toml /etc/taos/taoskeeper.toml
EXPOSE 6043
CMD ["taoskeeper"]
```

## Usage (Enterprise Edition)

### Prometheus (by scrape)

It's now act as a prometheus exporter like `node-exporter`.

Here's how to add this in scrape configs of `/etc/prometheus/prometheus.yml`:

```yml
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: "taoskeeper"
    static_configs:
      - targets: [ "taoskeeper:6043" ]
```

Now PromQL query will show the right result, for example, to show disk used percent in an specific host with FQDN regex
match expression:

```promql
taos_dn_disk_used / taos_dn_disk_total {fqdn=~ "tdengine.*"}
```

You can use `docker-compose` with the current `docker-compose.yml` to test the whole stack.

Here is the `docker-compose.yml`:

```yml
version: "3.7"

services:
  tdengine:
    image: tdengine/tdengine
    environment:
      TAOS_FQDN: tdengine
    volumes:
      - taosdata:/var/lib/taos
  taoskeeper:
    build: ./
    depends_on:
      - tdengine
    environment:
      TDENGINE_HOST: tdengine
      TDENGINE_PORT: 6041
    volumes:
      - ./config/taoskeeper.toml:/etc/taos/taoskeeper.toml
    ports:
      - 6043:6043
  prometheus:
    image: prom/prometheus
    volumes:
      - ./prometheus/:/etc/prometheus/
    ports:
      - 9090:9090
volumes:
  taosdata:

```

Start the stack:

```sh
docker-compose up -d
```

Now you point to <http://localhost:9090> (if you have not started a prometheus server by yourself) and query.

For a quick demo with TaosKeeper + Prometheus + Grafana, we provide
a [simple dashboard](https://grafana.com/grafana/dashboards/15164) to monitor TDengine.

### Telegraf

If you are using telegraf to collect metrics, just add inputs like this:

```toml
[[inputs.prometheus]]
  ## An array of urls to scrape metrics from.
  urls = ["http://taoskeeper:6043/metrics"]
```

You can test it with `docker-compose`:

```sh
docker-compose -f docker-compose.yml -f telegraf.yml up -d telegraf taoskeeper
```

Since we have set an stdout file output in `telegraf.conf`:

```toml
[[outputs.file]]
  files = ["stdout"]
```

So you can track with TDengine metrics in standard output with `docker-compose logs`:

```sh
docker-compose -f docker-compose.yml -f telegraf.yml logs -f telegraf
```

### Zabbix

1. Import the zabbix template file `zbx_taos_keeper_templates.xml`.
2. Use the template `TDengine` to create the host and modify the macros `{$TAOSKEEPER_HOST}`
   and `{$COLLECTION_INTERVAL}`.
3. Waiting for monitoring items to be created automatically.

### FAQ

* Error occurred: Connection refused, while taosKeeper was starting

  **Answer**: taoskeeper relies on restful interfaces to query data. Check whether the taosAdapter is running or whether
  the taosAdapter address in taoskeeper.toml is correct.

* Why detection metrics displayed by different TDengine's inconsistent with taoskeeper monitoring?

  **Answer**: If a metric is not created in TDengine, taoskeeper cannot get the corresponding test results.

* Cannot receive log from TDengine server.
  
  **Answer**: Modify `/etc/taos/taos.cfg` file and add parameters like:

  ```cfg
  monitor                  1          // start monitor
  monitorInterval          30         // send log interval (s)
  monitorFqdn              localhost 
  monitorPort              6043       // taosKeeper port
  monitorMaxLogs           100
  ```
