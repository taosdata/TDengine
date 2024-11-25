---
title: Monitor Your Cluster
slug: /operations-and-maintenance/monitor-your-cluster
---

import Image from '@theme/IdealImage';
import imgMonitor1 from '../assets/monitor-your-cluster-01.png';
import imgMonitor2 from '../assets/monitor-your-cluster-02.png';
import imgMonitor3 from '../assets/monitor-your-cluster-03.png';
import imgMonitor4 from '../assets/monitor-your-cluster-04.png';
import imgMonitor5 from '../assets/monitor-your-cluster-05.png';
import imgMonitor6 from '../assets/monitor-your-cluster-06.png';
import imgMonitor7 from '../assets/monitor-your-cluster-07.png';
import imgMonitor8 from '../assets/monitor-your-cluster-08.png';

To ensure the stable operation of the cluster, TDengine integrates various monitoring metrics collection mechanisms, which are aggregated through taosKeeper. TaosKeeper is responsible for receiving this data and writing it into a separate TDengine instance, which can operate independently from the monitored TDengine cluster. The two core components of TDengine, taosd (the database engine) and taosX (the data access platform), use the same monitoring architecture to achieve runtime monitoring, but their monitoring metric designs differ.

Regarding how to obtain and use this monitoring data, users can utilize third-party monitoring tools like Zabbix to gather these saved system monitoring data, seamlessly integrating the operational status of TDengine into existing IT monitoring systems. Alternatively, users can use the TDinsight plugin provided by TDengine, which allows them to visually display and manage these monitoring information through the Grafana platform, as shown in the image below. This provides users with flexible monitoring options to meet different operational needs.

<figure>
<Image img={imgMonitor1} alt="Managing monitoring information"/>
<figcaption>Figure 1. Managing monitoring information</figcaption>
</figure>

## Configuring TaosKeeper

Since all monitoring data of TDengine is reported and stored through TaosKeeper, this section first introduces the configuration of TaosKeeper.

The TaosKeeper configuration file is typically located at `/etc/taos/taoskeeper.toml`. Detailed configuration can be found in the [Reference Manual](../../tdengine-reference/components/taoskeeper/#configuration-file). One of the most critical configuration items is `database`, which determines which database in the target system will store the collected monitoring data.

## Monitoring Taosd

### Monitoring Taosd Based on TDinsight

To simplify user configuration for monitoring TDengine, TDengine provides a Grafana plugin called TDinsight. This plugin works in conjunction with TaosKeeper to monitor various performance metrics of TDengine in real-time.

By integrating Grafana with the TDengine data source plugin, TDinsight can read the monitoring data collected by TaosKeeper. This enables users to intuitively view key metrics such as the status of the TDengine cluster, node information, read/write requests, and resource usage on the Grafana platform, allowing for data visualization.

Here are detailed instructions for using TDinsight to help you make the most of this powerful tool.

#### Prerequisites

To successfully use TDinsight, the following conditions must be met:

- TDengine is installed and running normally.
- TaosAdapter is installed and running normally.
- TaosKeeper is installed and running normally.
- Grafana is installed and running normally; the following instructions are based on Grafana version 11.0.0.

Also, record the following information:

- The RESTful interface address of TaosAdapter, such as `http://www.example.com:6041`.
- The authentication information for the TDengine cluster, including the username and password.

#### Importing the Dashboard

The TDengine data source plugin has been submitted to the Grafana official website. For instructions on installing the TDengine data source plugin and configuring the data source, please refer to: [Install Grafana Plugin and Configure Data Source](../../third-party-tools/visualization/grafana/#install-grafana-plugin-and-configure-data-source). After completing the installation of the plugin and creating the data source, you can proceed to import the TDinsight dashboard.

On the Grafana "Home" -> "Dashboards" page, click the "New" -> "Import" button located in the upper right corner to enter the dashboard import page, which supports the following two import methods:

- Dashboard ID: 18180.
- Dashboard URL: [https://grafana.com/grafana/dashboards/18180-tdinsight-for-3-x/](https://grafana.com/grafana/dashboards/18180-tdinsight-for-3-x/)

After filling in the Dashboard ID or Dashboard URL, click the "Load" button and follow the wizard to complete the import. Once the import is successful, the "TDinsight for 3.x" dashboard will appear on the Dashboards list page. Clicking on it will allow you to see the various metrics panels created in TDinsight, as shown in the image below:

<figure>
<Image img={imgMonitor2} alt="TDinsight interface"/>
<figcaption>Figure 2. TDinsight interface</figcaption>
</figure>

:::note

In the TDinsight interface, you can select the `log` database from the "Log from" dropdown list in the upper left corner.

:::

### TDengine V3 Monitoring Data

The TDinsight dashboard data comes from the `log` database (the default database for storing monitoring data, which can be modified in the TaosKeeper configuration file). The "TDinsight for 3.x" dashboard queries the monitoring metrics of taosd and TaosAdapter.

- For taosd monitoring metrics, please refer to [Taosd Monitoring Metrics](../../tdengine-reference/components/taosd/#taosd-monitoring-metrics).
- For TaosAdapter monitoring metrics, please refer to [TaosAdapter Monitoring Metrics](../../tdengine-reference/components/taosadapter/#taosadapter-monitoring-metrics).

## Monitoring TaosX

TaosX is the core component providing zero-code data access capabilities in TDengine, and monitoring it is also essential. The monitoring of TaosX is similar to that of TDengine; metrics collected from the service are written into a specified database through TaosKeeper, and visualized and alarmed using Grafana dashboards. This functionality can monitor the following objects:

1. The taosX process.
2. All running taosx-agent processes.
3. Various connector subprocesses running on either the taosX or taosx-agent side.
4. Various data writing tasks in progress.

### Prerequisites

1. Taosd, TaosAdapter, and TaosKeeper have all been successfully deployed and started.
2. The TaosX service monitoring configuration is correct. For configuration details, refer to the section "Configuring TaosX Monitoring" below. The service must start successfully.  
   :::note
   The taosX included in TDengine Enterprise version 3.2.3.0 or above contains this functionality. If taosX is installed separately, it must be version 1.5.0 or above.
   :::
3. Deploy Grafana, install the TDengine Datasource plugin, and configure the data source. You can refer to: [Install Grafana Plugin and Configure Data Source](../../third-party-tools/visualization/grafana/#install-grafana-plugin-and-configure-data-source).
   :::note
   You need to install Grafana plugin [TDengine Datasource v3.5.0](https://grafana.com/grafana/plugins/tdengine-datasource/) or a higher version.
   :::

### Configuring TaosX Monitoring

The configuration file for TaosX (default is /etc/taos/taosx.toml) contains the following monitor-related configurations:

```toml
[monitor]
# FQDN of the taosKeeper service, no default value
# fqdn = "localhost"
# port of the taosKeeper service, default 6043
# port = 6043
# how often to send metrics to taosKeeper, default every 10 seconds. Only values from 1 to 10 are valid.
# interval = 10
```

Each configuration also has corresponding command-line options and environment variables. The following table explains:

| Configuration File Item | Command-Line Option   | Environment Variable | Meaning                                                  | Value Range | Default Value                          |
| ----------------------- | --------------------- | -------------------- | -------------------------------------------------------- | ----------- | ------------------------------------- |
| fqdn                    | --monitor-fqdn        | MONITOR_FQDN         | FQDN of the TaosKeeper service                           |             | No default value; configuring fqdn enables monitoring |
| port                    | --monitor-port        | MONITOR_PORT         | Port of the TaosKeeper service                           |             | 6043                                  |
| interval                | --monitor-interval    | MONITOR_INTERVAL     | Time interval (in seconds) for TaosX to send metrics to TaosKeeper | 1-10       | 10                                    |

### Monitoring TaosX Based on TDinsight

"TDinsight for TaosX" is a Grafana dashboard specifically created for monitoring TaosX. You need to import this panel before use.

#### Accessing the Panel

1. In the Grafana interface menu, click on "Data sources," and then select the configured TDengine data source.
2. In the data source configuration interface, select the "Dashboard" tab, and then import the "TDinsight for TaosX" panel (import it for the first time). Here is a sample image:

   <figure>
   <Image img={imgMonitor3} alt=""/>
   </figure>

   Each row of this panel represents a monitored object or category. The top row is for taosX monitoring, followed by the agent monitoring row, and finally the monitoring of various data writing tasks.

   :::note

   - If you do not see any data after opening this panel, you may need to click the database list in the upper left corner (i.e., the "Log from" dropdown menu) to switch to the database where the monitoring data is located.
   - The number of agents in the database will automatically create as many agent rows. (as shown in the image above)

   :::

#### Monitoring Examples

1. TaosX Monitoring Example Image

   <figure>
   <Image img={imgMonitor4} alt=""/>
   </figure>

2. Agent Monitoring Example Image

   <figure>
   <Image img={imgMonitor5} alt=""/>
   </figure>

3. TDengine 2 Data Source Monitoring Example Image

   <figure>
   <Image img={imgMonitor6} alt=""/>
   </figure>

   :::info

   The monitoring panel only displays part of the monitoring metrics for data writing tasks. More comprehensive monitoring metrics with detailed explanations for each metric can be found on the Explorer page.

   :::

4. TDengine 3 Data Source Monitoring Example Image

   <figure>
   <Image img={imgMonitor7} alt=""/>
   </figure>

5. Other Data Source Monitoring Example Image

   <figure>
   <Image img={imgMonitor8} alt=""/>
   </figure>

#### Limitations

Monitoring-related configurations will only take effect when taosX is running in server mode.
