---
title: taosX Monitoring
sidebar_label: taosX Monitoring
toc_max_heading_level: 4
---

## 1. Introduction

This article primarily introduces the configuration of TaosX related to monitoring and the TDinsight panel corresponding to TaosX. TaosX monitoring is similar to TDengine monitoring; both utilize taosKeeper to write the metrics collected by the service into a specified database. Subsequently, the Grafana dashboard is employed for visualization and alerts.

Metrics includes:

1. taosX main process
2. Accessible taosx-agent processes
3. Child processes of taosX or Agent
4. Data-transferring tasks running in taosX service

## 2. Version Compatibility

1. This feature is only available in TDengine Enterprise version 3.2.3.0 or later. If taosX is installed separately, version 1.5.0 or later is required.
2. Grafana plugin [TDengine Datasource v3.5.0](https://grafana.com/grafana/plugins/tdengine-datasource/) or above is required.

## 3. Preparation

Assuming you have installed TDengine and taosAdapter. Then you also need:

1. Start taosKeeper following [Reference Manual/taosKeeper](../../reference/taosKeeper).
2. Start the taosX service according to the configuration in Section 4 of this article.
3. Refer to [Third Party Tool/Grafana](../../third-party/grafana) to deploy Grafana, install the TDengine datasource plugin, and configure the datasource.

## 4. Configuration in taosX

The monitoring-related configuration in the taosX configuration file (default to `/etc/taos/taosx.toml` in Linux, and `C:\TDengine\cfg\taosx.toml` in Windows OS) is as follows:

```toml
[monitor]
#FQDN of taosKeeper service, no default value
#fqdn = "localhost"
#port of taosKeeper service, default 6043
#port = 6043
#how often to send metrics to taosKeeper, default every 10 seconds. Only values from 1 to 10 is valid.
#interval = 10
```

Each configuration also has corresponding command-line options and environment variables. The following table provides an explanation:

| configuration items | Command line options | Environment variables | Meaning                                                              | Range of values | Default values                                                                   |
| ------------------- | -------------------- | --------------------- | -------------------------------------------------------------------- | --------------- | -------------------------------------------------------------------------------- |
| fqdn                | --monitor-fqdn       | MONITOR_FQDN          | taosKeeper service FQDN                                              |                 | no default value, configuring fqdn is equivalent to enabling monitoring function |
| port                | --monitor-port       | MONITOR_PORT          | port for the taosKeeper service                                      |                 | 6043                                                                             |
| interval            | --monitor-interval   | MONITOR_INTERVAL      | the time interval for sending metrics data to taosKeeper, in seconds | 1-10            | 10                                                                               |

## 5. TDinsight for taosX

"TDinsight for taosX" is a Grafana dashboard designed specifically for monitoring taosX. Before use, it is necessary to import this dashboard as followings:

### 5.1 Deploy Dashboard

1. Choose TDengine in the Grafana **"Data sources"** list:
    ![TDEngine Datasource](./pic/monitor-01.jpg)
2. Click **"Dashboards"** tab and import the "TDinsight for taosX" dashboard.
    ![Dashboard](./pic/monitor-02.jpg)
3. View the "TDinsight for taosX" dashboard in Grafana:
    ![monitor rows](./pic/monitor-04.jpg)
    Each row on the board represents specific monitored objects. The top row is dedicated to taosX monitoring, followed by the Agent monitoring row, and finally, the monitoring of various data writing tasks.

    :::note
    1. If you can't see any data after opening this panel, you are likely to need to click the database list in the upper left corner (i.e. the "Log from" drop-down menu) to switch to the database where the monitoring data is located.
    2. The number of Agent rows will be automatically created according to the number of Agent data contained in the database (as shown in the above figure).
    :::

### 5.2 Monitoring Example

#### 5.2.1 taosX monitoring example

![monitor taosx](./pic/monitor-03.png)

#### 5.2.2 Agent Monitoring Example

![monitoring agent](./pic/monitor-09.jpg)

#### 5.2.3 TDengine2 Data Source Monitoring Example

![monitor tdengine2](./pic/monitor-05.png)

:::info
The monitoring panel only displays some monitoring metrics for data writing tasks. There are more comprehensive monitoring metrics on the Explorer page, with specific descriptions for each metric.
:::

#### 5.2.4 TDengine3 Data Source Monitoring Example

![monitor tdengine3](./pic/monitor-06.jpg)

#### 5.2.5 Other data source monitoring examples

![monitor task](./pic/monitor-10.jpg)

## 6. Restrictions

Configurations related to monitoring only take effect when taosX running in server mode.
