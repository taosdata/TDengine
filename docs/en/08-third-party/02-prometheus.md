---
sidebar_label: Prometheus
title: Prometheus for TDengine Cloud
---

Prometheus is a widespread open-source monitoring and alerting system. Prometheus joined the Cloud Native Computing Foundation (CNCF) in 2016 as the second incubated project after Kubernetes, which has a very active developer and user community.

Prometheus provides `remote_write` and `remote_read` interfaces to leverage other database products as its storage engine. To enable users of the Prometheus ecosystem to take advantage of TDengine's efficient writing and querying, TDengine also provides support for these two interfaces.

Prometheus data can be stored in TDengine via the `remote_write` interface with proper configuration. Data stored in TDengine can be queried via the `remote_read` interface, taking full advantage of TDengine's efficient storage query performance and clustering capabilities for time-series data.

## Install Prometheus

Supposed that you use Linux system with architecture amd64:
1. Download
  ```
  wget https://github.com/prometheus/prometheus/releases/download/v2.37.0/prometheus-2.37.0.linux-amd64.tar.gz
  ```
2. Decompress and rename
   ```
   tar xvfz prometheus-*.tar.gz && mv prometheus-2.37.0.linux-amd64 prometheus
   ```  
3. Change to directory prometheus
   ```
   cd prometheus
   ```

Then Prometheus is installed in current directory. For more installation options, please refer to the [official documentation](https://prometheus.io/docs/prometheus/latest/installation/).

## Configuration

Configuring Prometheus is done by editing the Prometheus configuration file `prometheus.yml` (If you followed previous steps, you can find prometheus.xml in current directory).

```yaml
remote_write:
  - url: "<cloud_url>/prometheus/v1/remote_write/prometheus_data?token=<cloud_token>"

remote_read:
  - url: "<cloud_url>/prometheus/v1/remote_read/prometheus_data?token=<cloud_token>"
    remote_timeout: 10s
    read_recent: true
```

<!-- exclude -->
You are expected to replace `<cloud_url>` and `<cloud_token>` with real TDengine cloud URL and token. To obtain the real values, please log in [TDengine Cloud](https://cloud.tdengine.com).
<!-- exclude-end -->

The resulting configuration will collect data about prometheus itself from its own HTTP metrics endpoint, and store data to TDengine Cloud.

## Start Prometheus

```
./prometheus --config.file prometheus.yml
```

Prometheus should start up. It also started a web server at <http://localhost:9090>. If you want to access the web server from a browser which is not running on the same host as Prometheus, please change `localhost` to correct hostname, FQDN or IP address, depending on your network environment.

## Verify Remote Write

Log in TDengine Cloud, click "Explorer" on the left navigation bar. You will see metrics collected by prometheus.

![TDengine prometheus remote_write result](prometheus_data.webp)

## Verify Remote Read

Lets retrieve some metrics from TDengine Cloud via prometheus web server. Browse to <http://localhost:9090/graph> and use the "Graph" tab.

Enter the following expression to graph the per-second rate of chunks being created in the self-scraped Prometheus:

```
rate(prometheus_tsdb_head_chunks_created_total[1m])
```

![TDengine prometheus remote_read](prometheus_read.webp)

