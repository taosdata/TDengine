---
sidebar_label: Prometheus
title: Prometheus 
description: 使用 Prometheus 访问 TDengine
---

Prometheus 是一款流行的开源监控告警系统。Prometheus 于2016年加入了 Cloud Native Computing Foundation （云原生云计算基金会，简称 CNCF），成为继 Kubernetes 之后的第二个托管项目，该项目拥有非常活跃的开发人员和用户社区。

## 前置条件

要将 Prometheus 数据写入 TDengine 需要以下几方面的准备工作。
- TDengine 集群已经部署并正常运行
- taosAdapter 已经安装并正常运行。具体细节请参考 [taosAdapter 的使用手册](/reference/taosadapter)
- Prometheus 已经安装。安装 Prometheus 请参考[官方文档](https://prometheus.io/docs/prometheus/latest/installation/)

## 配置 TDengine 为 Prometheus 的第三方数据库

Prometheus 提供了 `remote_write` 和 `remote_read` 接口来利用其它数据库产品作为它的存储引擎。为了让 Prometheus 生态圈的用户能够利用 TDengine 的高效写入和查询，TDengine 也提供了对这两个接口的支持。

通过适当的配置， Prometheus 的数据可以通过 `remote_write` 接口存储到 TDengine 中，也可以通过 `remote_read` 接口来查询存储在 TDengine 中的数据，充分利用 TDengine 对时序数据的高效存储查询性能和集群处理能力。

配置 Prometheus 是通过编辑 Prometheus 配置文件 prometheus.yml （默认位置 /etc/prometheus/prometheus.yml）完成的。

### 配置第三方数据库地址

将其中的 remote_read url 和 remote_write url 指向运行 taosAdapter 服务的服务器域名或 IP 地址，REST 服务端口（taosAdapter 默认使用 6041），以及希望写入 TDengine 的数据库名称，并确保相应的 URL 形式如下：

- remote_read url : `http://<taosAdapter's host>:<REST service port>/prometheus/v1/remote_read/<database name>`
- remote_write url : `http://<taosAdapter's host>:<REST service port>/prometheus/v1/remote_write/<database name>`

### 配置 Basic 验证

- username： <TDengine's username>
- password： <TDengine's password>

### prometheus.yml 文件中 remote_write 和 remote_read 相关部分配置示例

```yaml
remote_write:
  - url: "http://localhost:6041/prometheus/v1/remote_write/prometheus_data"
    basic_auth:
      username: root
      password: taosdata

remote_read:
  - url: "http://localhost:6041/prometheus/v1/remote_read/prometheus_data"
    basic_auth:
      username: root
      password: taosdata
    remote_timeout: 10s
    read_recent: true
```

### 验证方法

重启 Prometheus 后可参考以下示例验证从 Prometheus 向 TDengine 写入数据并能够正确读出。

#### 使用 TDengine CLI 查询写入数据
```
taos> show databases;
              name              |
=================================
 information_schema             |
 performance_schema             |
 prometheus_data                |
Query OK, 3 row(s) in set (0.000585s)

taos> use prometheus_data;
Database changed.

taos> show stables;
              name              |
=================================
 metrics                        |
Query OK, 1 row(s) in set (0.000487s)

taos> select * from metrics limit 10;
              ts               |           value           |             labels             |
=============================================================================================
 2022-04-20 07:21:09.193000000 |               0.000024996 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:14.193000000 |               0.000024996 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:19.193000000 |               0.000024996 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:24.193000000 |               0.000024996 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:29.193000000 |               0.000024996 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:09.193000000 |               0.000054249 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:14.193000000 |               0.000054249 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:19.193000000 |               0.000054249 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:24.193000000 |               0.000054249 | {"__name__":"go_gc_duration... |
 2022-04-20 07:21:29.193000000 |               0.000054249 | {"__name__":"go_gc_duration... |
Query OK, 10 row(s) in set (0.011146s)
```

#### 使用 promql-cli 通过 remote_read 从 TDengine 读取数据

安装 promql-cli

```
 go install github.com/nalbury/promql-cli@latest
```

在 TDengine 和 taosAdapter 服务运行状态对 Prometheus 数据进行查询

```
ubuntu@shuduo-1804 ~ $ promql-cli --host "http://127.0.0.1:9090" "sum(up) by (job)"
JOB           VALUE    TIMESTAMP
prometheus    1        2022-04-20T08:05:26Z
node          1        2022-04-20T08:05:26Z
```

暂停 taosAdapter 服务后对 Prometheus 数据进行查询

```
ubuntu@shuduo-1804 ~ $ sudo systemctl stop taosadapter.service
ubuntu@shuduo-1804 ~ $ promql-cli --host "http://127.0.0.1:9090" "sum(up) by (job)"
VALUE    TIMESTAMP

```

:::note

- TDengine 默认生成的子表名是根据规则生成的唯一 ID 值。
:::

## prometheus with taoskeeper

taoskeeper 提供了 `/metrics` 接口，返回了 Prometheus 格式的监控数据，Prometheus 可以从 taoskeeper 抽取监控数据，实现通过 Prometheus 监控 TDengine 的目的。

### 抽取配置

Prometheus 提供了 `scrape_configs` 配置如何从 endpoint 抽取监控数据，通常只需要修改 `static_configs` 中的 targets 配置为 taoskeeper 的 endpoint 地址，更多配置信息请参考 [Prometheus 配置文档](https://prometheus.io/docs/prometheus/latest/configuration/configuration/#scrape_config)。

```
# A scrape configuration containing exactly one endpoint to scrape:
# Here it's Prometheus itself.
scrape_configs:
  - job_name: "taoskeeper"

    # metrics_path defaults to '/metrics'
    # scheme defaults to 'http'.

    static_configs:
      - targets: ["localhost:6043"]
```

### 监控 dashboard

我们提供了 `TaosKeeper Prometheus Dashboard for 3.x` dashboard，提供了和 TDinsight 类似的监控 dashboard。

在 Grafana Dashboard 菜单点击 `import`，dashboard ID 填写 `18587`，点击 `Load` 按钮即可导入 `TaosKeeper Prometheus Dashboard for 3.x` dashboard。
