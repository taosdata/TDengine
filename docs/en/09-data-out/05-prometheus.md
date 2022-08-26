---
sidebar_label: Prometheus
title: Prometheus for TDengine Cloud
description: Write data into TDengine from Prometheus.
---

Prometheus is a widespread open-source monitoring and alerting system. Prometheus joined the Cloud Native Computing Foundation (CNCF) in 2016 as the second incubated project after Kubernetes, which has a very active developer and user community.

Prometheus provides `remote_write` and `remote_read` interfaces to leverage other database products as its storage engine. To enable users of the Prometheus ecosystem to take advantage of TDengine's efficient writing and querying, TDengine also provides support for these two interfaces.

Prometheus data can be stored in TDengine via the `remote_write` interface with proper configuration. Data stored in TDengine can be queried via the `remote_read` interface, taking full advantage of TDengine's efficient storage query performance and clustering capabilities for time-series data.

## Install Prometheus

Please refer to [Install Prometheus](../../data-in/prometheus#install-prometheus).

## Configure Prometheus

Please refer to [Configure Prometheus](../../data-in/prometheus/#configure-prometheus).

## Start Prometheus

Please refer to [Start Prometheus](../../data-in/prometheus/#start-prometheus).

## Verify Remote Read

Lets retrieve some metrics from TDengine Cloud via prometheus web server. Browse to <http://localhost:9090/graph> and use the "Graph" tab.

Enter the following expression to graph the per-second rate of chunks being created in the self-scraped Prometheus:

```
rate(prometheus_tsdb_head_chunks_created_total[1m])
```

![TDengine prometheus remote_read](prometheus_read.webp)

