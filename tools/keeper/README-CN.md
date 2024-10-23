# TaosKeeper

taosKeeper 是 TDengine 各项监控指标的导出工具，通过简单的几项配置即可获取 TDengine 的运行状态。并且 taosKeeper 企业版支持多种收集器，可以方便进行监控数据的展示。

taosKeeper 使用 TDengine RESTful 接口，所以不需要安装 TDengine 客户端即可使用。

## 构建

### 获取源码

从 GitHub 克隆源码：

```sh
git clone https://github.com/taosdata/TDengine
cd TDengine/tools/keeper
```

### 编译

taosKeeper 使用 `GO` 语言编写，在构建前需要配置好 `GO` 语言开发环境。

```sh
go mod tidy
go build
```

## 安装

如果是自行构建的项目，仅需要拷贝 `taoskeeper` 文件到你的 `PATH` 中。

```sh
sudo install taoskeeper /usr/bin/
```

## 启动

在启动前，应该做好如下配置：
在 `/etc/taos/taoskeeper.toml` 配置 TDengine 连接参数以及监控指标前缀等其他信息。

```toml
# gin 框架是否启用 debug
debug = false

# 服务监听端口, 默认为 6043
port = 6043

# 日志级别，包含 panic、error、info、debug、trace等
loglevel = "info"

# 程序中使用协程池的大小
gopoolsize = 50000

# 查询 TDengine 监控数据轮询间隔
RotationInterval = "15s"

[tdengine]
host = "127.0.0.1"
port = 6041
username = "root"
password = "taosdata"

# 需要被监控的 taosAdapter
[taosAdapter]
address = ["127.0.0.1:6041"]

[metrics]
# 监控指标前缀
prefix = "taos"

# 存放监控数据的数据库
database = "log"

# 指定需要监控的普通表
tables = []

[environment]
# 是否在容器中运行，影响 taosKeeper 自身的监控数据
incgroup = false
```

现在可以启动服务，输入：

```sh
taoskeeper
```

如果你使用 `systemd`，复制 `taoskeeper.service` 到 `/lib/systemd/system/`，并启动服务。

```sh
sudo cp taoskeeper.service /lib/systemd/system/
sudo systemctl daemon-reload
sudo systemctl start taoskeeper
```

让 taosKeeper 随系统开机自启动。

```sh
sudo systemctl enable taoskeeper
```

如果使用 `systemd`，你可以使用如下命令完成安装

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

如下介绍了如何在 docker 中构建 taosKeeper：

在构建前请配置好 `./config/taoskeeper.toml` 中合适的参数，并编辑 Dockerfile ，示例如下。

```dockerfile
FROM golang:1.18.6-alpine as builder

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

如果已经有 taosKeeper 可执行文件，在配置好 `taoskeeper.toml` 后你可以使用如下方式构建:

```dockerfile
FROM ubuntu:18.04
RUN mkdir -p /etc/taos
COPY ./taoskeeper /usr/bin/
COPY ./taoskeeper.toml /etc/taos/taoskeeper.toml
EXPOSE 6043
CMD ["taoskeeper"]
```

## 使用（企业版）

### Prometheus (by scrape)

taosKeeper 可以像 `node-exporter` 一样向 Prometheus 提供监控指标。\
在 `/etc/prometheus/prometheus.yml` 添加配置：

```yml
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: "taoskeeper"
    static_configs:
      - targets: ["taoskeeper:6043"]
```

现在使用 PromQL 查询即可以显示结果，比如要查看指定主机（通过 FQDN 正则匹配表达式筛选）硬盘使用百分比：

```promql
taos_dn_disk_used / taos_dn_disk_total {fqdn=~ "tdengine.*"}
```

你可以使用 `docker-compose` 测试完整的链路。
`docker-compose.yml`示例：

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

启动：

```sh
docker-compose up -d
```

现在通过访问 <http://localhost:9090> 来查询结果。访问[simple dashboard](https://grafana.com/grafana/dashboards/15164) 来查看TaosKeeper + Prometheus + Grafana 监控 TDengine 的快速启动实例。

### Telegraf

如果使用 telegraf 来收集各个指标，仅需要在配置中增加：

```toml
[[inputs.prometheus]]
## An array of urls to scrape metrics from.
urls = ["http://taoskeeper:6043/metrics"]
```

可以通过 `docker-compose` 来测试

```sh
docker-compose -f docker-compose.yml -f telegraf.yml up -d telegraf taoskeeper
```

由于可以在 `telegraf.conf` 设置日志为标准输出：

```toml
[[outputs.file]]
files = ["stdout"]
```

所以你可以通过 `docker-compose logs` 在标准输出中追踪 TDengine 各项指标。

```sh
docker-compose -f docker-compose.yml -f telegraf.yml logs -f telegraf
```

### Zabbix

1. 导入 zabbix 临时文件 `zbx_taos_keeper_templates.xml`。
2. 使用 `TDengine` 模板来创建主机，修改宏 `{$TAOSKEEPER_HOST}` 和 `{$COLLECTION_INTERVAL}`。
3. 等待并查看到自动创建的条目。

### 常见问题

* 启动报错，显示connection refused

  **解析**：taosKeeper 依赖 restful 接口查询数据，请检查 taosAdapter 是否正常运行或 taoskeeper.toml 中 taosAdapter 地址是否正确。

* taosKeeper 监控不同 TDengine 显示的检测指标数目不一致？

  **解析**：如果 TDengine 中未创建某项指标，taoskeeper 不能获取对应的检测结果。

* 不能接收到 TDengine 的监控日志。

  **解析**: 修改 `/etc/taos/taos.cfg` 文件并增加如下参数：

  ```cfg
  monitor                  1          // 启用 monitor
  monitorInterval          30         // 发送间隔 (s)
  monitorFqdn              localhost  // 接收消息的 FQDN，默认为空
  monitorPort              6043       // 接收消息的端口号
  monitorMaxLogs           100        // 每个监控间隔缓存的最大日志数量
  ```
