# TDinsight - A Monitoring Solution For [TDengine] with [Grafana]

Languages: _English_ _[简体中文](https://www.taosdata.com/cn/documentation/tools/insight)_

TDinsight v3.x is a solution for monitoring TDengine using the builtin native monitoring database and Grafana.

## Compatible with TDengine version
| TDengine grafana plugin version | TDengine version |
| :-----------------------------: | :--------------: |
|              4.0.0              | 3.3.7.0 or later |
|              3.8.0              | 3.3.7.0 or later |
|              3.7.6              | 3.3.7.0 or later |
|              3.7.5              | 3.3.7.0 or later |
|              3.7.4              | 3.3.7.0 or later |
|              3.7.3              | 3.3.7.0 or later |
|              3.7.2              | 3.3.0.0 or later |
|              3.7.1              | 3.3.0.0 or later |
|              3.7.0              | 3.3.0.0 or later |
|              3.6.3              | 3.3.0.0 or later |
|              3.6.2              | 3.3.0.0 or later |
|              3.6.1              | 3.3.0.0 or later |
|              3.6.0              | 3.3.0.0 or later |
|              3.5.2              | 3.3.0.0 or later |
|              3.5.1              | 3.3.0.0 or later |
|              3.5.0              | 3.2.3.0 or later |
|              3.4.0              | 3.2.0.1 or later |
|              3.2.7              | 3.0.0.0 or later |


TDengine writes monitoring data into a special database through [taosKeeper](https://github.com/taosdata/taoskeeper).
The metrics may include the server's CPU, memory, hard disk space, network bandwidth, number of requests, disk
read/write speed, slow queries, other information like important system operations (user login, database creation,
database deletion, etc.), and error alarms. With [Grafana](https://grafana.com/)
and [TDengine Data Source Plugin](https://github.com/taosdata/grafanaplugin/releases), TDinsight can visualize cluster
status, node information, insertion and query requests, resource usage, vnode, dnode, and mnode status, exception alerts
and many other metrics. This is very convenient for developers who want to monitor TDengine cluster status in real-time.

This article will guide users to install the Grafana server and TDengine Data Source Plugin, and deploy the TDinsight
v3.x.

> :exclamation: **Note：** This Dashboard does not support TDengine Cloud Service.

## Requirements

- a single-node TDengine server or a multi-node TDengine cluster and a Grafana server are required. This dashboard
  requires TDengine with the monitoring feature enabled. For detailed configuration, please refer to
  [TDengine monitoring configuration](https://docs.tdengine.com/reference/config/#monitoring-parameters).
- taosAdapter has been instaleld and running, please refer
  to [taosAdapter](https://docs.tdengine.com/reference/taosadapter/).
- taosKeeper has been installed and running, please refer
  to [taosKeeper](https://docs.tdengine.com/reference/taosKeeper/).

## Install Grafana

We recommend using the latest [Grafana](https://grafana.com/) version 9 or 10. You can install Grafana on any
supported operating system by following
the [official Grafana documentation Instructions](https://grafana.com/docs/grafana/latest/installation/) to install
Grafana.

### Installing Grafana on Debian or Ubuntu

For Debian or Ubuntu operating systems, we recommend the Grafana image repository and using the following command to
install from scratch.

```bash
sudo apt-get install -y apt-transport-https
sudo apt-get install -y software-properties-common wget
wget -q -O - https://packages.grafana.com/gpg.key |\
  sudo apt-key add -
echo "deb https://packages.grafana.com/oss/deb stable main" |\
  sudo tee -a /etc/apt/sources.list.d/grafana.list
sudo apt-get update
sudo apt-get install grafana
```

### Install Grafana on CentOS / RHEL

You can install it from its official YUM repository.

```bash
sudo tee /etc/yum.repos.d/grafana.repo << EOF
[grafana]
name=grafana
baseurl=https://packages.grafana.com/oss/rpm
repo_gpgcheck=1
enabled=1
gpgcheck=1
gpgkey=https://packages.grafana.com/gpg.key
sslverify=1
sslcacert=/etc/pki/tls/certs/ca-bundle.crt
EOF
sudo yum install grafana
```

Or install it with RPM package.

```bash
wget https://dl.grafana.com/oss/release/grafana-8.0.0-1.x86_64.rpm
sudo yum install grafana-8.0.0-1.x86_64.rpm
# or
sudo yum install \
  https://dl.grafana.com/oss/release/grafana-8.0.0-1.x86_64.rpm
```

## set up TDinsight v3.x

### Install the TDengine data source plugin

#### Install the latest version of the TDengine Data Source plugin from GitHub

```bash
get_latest_release() {
  curl --silent "https://api.github.com/repos/taosdata/grafanaplugin/releases/latest" |
    grep '"tag_name":' |
    sed -E 's/.*"v([^"]+)".*/\1/'
}
TDENGINE_PLUGIN_VERSION=$(get_latest_release)
sudo grafana-cli \
  --pluginUrl https://github.com/taosdata/grafanaplugin/releases/download/v$TDENGINE_PLUGIN_VERSION/tdengine-datasource-$TDENGINE_PLUGIN_VERSION.zip \
  plugins install tdengine-datasource
```

**NOTE**: The 3.1.6 and earlier version plugins require the following setting in the configuration
file `/etc/grafana/grafana.ini` to enable unsigned plugins.

```ini
[plugins]
allow_loading_unsigned_plugins = tdengine-datasource
```

#### Install TDengine Data Source plugin from Grafana Plugins page

Point to the **Configurations** -> **Plugins**(or "/plugins" url), and search "TDengine".

![install datasource plugin](../../assets/howto-add-plugin.png)

Click "TDengine Datasource", and click "install".

![install datasource plugin](../../assets/howto-add-plugin-install.png)

### Start the Grafana service

```bash
sudo systemctl start grafana-server
sudo systemctl enable grafana-server
```

### Logging into Grafana

Open the default Grafana URL in a web browser: `http://localhost:3000`. The default username/password is `admin`.
Grafana will require a password change after the first login.

### Adding a TDengine Data Source

Point to the **Configurations** -> **Data Sources** menu, and click the **Add data source** button.

![add datasource](../../assets/howto-add-datasource-button.png)

Search for and select **TDengine**.

![add datasource](../../assets/howto-add-datasource-tdengine.png)

**Configure TDengine data source for Grafana version 11.**

![datasource config](../../assets/howto-add-datasource-11v.png)
**Note:**
1. Close the `Load TDengine Alert` button to prevent automatic import of alert rules when adding data sources.
2. When deleting a data source, you need to first click the delete button to clear the imported alarm rules.

**Configure TDengine data source for Grafana with version lower than 11.**

![datasource config](../../assets/howto-add-datasource.png)

Save and test. It will report 'TDengine Data source is working' in normal circumstances.

![datasource testing](../../assets/howto-add-datasource-test.png)

### Importing dashboard

#### Importing dashboard from datasource confining page.

Click **Dashboard** tab in TDengine datasource confining page.

![import dashboard and config](../../assets/import_dashboard-on-datasource.png)

Click the "import" button of "TDinsight for 3.x", and import the dashboard.

### Import dashboard from importing page

Point to **+** / **Create** - **import** (or `/dashboard/import` url).

![import dashboard and config](../../assets/import_dashboard.png)

Input dashboard id `18180` in **Import via grafana.com** and click **Load**.

![import by grafana.com](../../assets/import-dashboard-18180.png)

After importing, TDinsight v3.x dashboard as follows.

![dashboard](../../assets/TDinsight-v3-full.png)

## TDinsight v3.x Dashboard details

The TDinsight dashboard is designed to provide the usage and status of TDengine-related resources,
e.g. [dnodes, mnodes, vnodes](https://www.taosdata.com/cn/documentation/architecture#cluster) and databases.

The details of the metrics are as follows.

### Cluster Status

![tdinsight-mnodes-overview](../../assets/TDinsightV3-1-cluster-status.png)

This section contains the current information and status of the cluster (from left to right, top to bottom).

- **First EP**: the firstEp setting in the current TDengine cluster.
- **Version**: TDengine server version (leader mnode).
- **Expire Time**: Enterprise version expiration time.
- **Used Measuring Points**: The number of measuring points used by the Enterprise Edition.
- **Databases**: The number of databases.
- **Stables & Tables**: The total number of stable and table.
- **Connections**: The number of current connections.
- **DNodes/MNodes/VGroups/VNodes**: Total number of each resource and the number of survivors.
- **Classified Connection Counts**: The current number of active connections, classified by user, application, and IP. 
- **DNodes/MNodes/VGroups/VNodes Alive Percent**: The ratio of the number of alive/total for each resource.
- **Measuring Points Used**: The number of measuring points(no data available in the community version, healthy by
  default).

### DNodes Status

![tdinsight-dnodes-overview](../../assets/TDinsightV3-2-dnodes.png)

- **DNodes Status**: simple table view of `show dnodes`.
- **DNodes Number**: the trends in the number of DNodes.

### MNode Overview

![tdinsight-mnodes-overview](../../assets/TDinsightV3-3-mnodes.png)

1. **MNodes Status**: a simple table view of `show mnodes`.
2. **MNodes Number**: similar to `DNodes Number`, the trends in the number of MNodes.

### Request

![tdinsight-requests](../../assets/TDinsightV3-4-requests.png)

1. **Select Request**: select request in timeseries.
2. **Delete Request**: delete request in timeseries.
3. **Insert Request**: insert request in timeseries.
4. **Inserted Rows**: inserted rows in timeseries.
5. **Slow Sql**: number of slow query requests in timeseries.

### Tables Summary

![tdinsight-Tables-Summary](../../assets/TDinsightV3-5-database.png)

1. **STables**: number of super tables.
2. **Tables**: number of all tables.
3. **Tables**: number of normal tables in timeseries.
4. **Tables Number Foreach VGroups**: number of tables per vgroup.

### DNode Resource Usage

![dnode-usage](../../assets/TDinsightV3-6-dnode-usage.png)

Data node resource usage display with repeated multiple rows for the variable `$fqdn`, one rows per dnode.

1. **Uptime**: the time elapsed since the dnode was created.
2. **Has MNodes?**: whether the current dnode is a mnode.
3. **CPU Cores**: the number of CPU cores.
4. **VNodes Number**: the number of VNodes in the current dnode.
5. **VNodes Masters**: the number of vnodes in the leader role.
6. **Current CPU Usage of taosd**: CPU usage rate of taosd processes.
7. **Current Memory Usage of taosd**: memory usage of taosd processes.
8. **Max Disk Used**: the maximum disk usage rate corresponding to all taosd data directories.
9. **CPU Usage**: Process and system CPU usage.
10. **RAM Usage**: Time series view of RAM usage metrics.
11. **Disk Used**: Disks used at each level of multi-level storage (default is level0).
12. **Disk IO**: Disk IO rate.
13. **Net**: Network IO, the aggregate network IO rate in addition to the local network.
14. **Dnode Memory Usage Metrics**: dnode RPC and apply queue memory usage statistics.
    
### Write Metrics

![write-metrics](../../assets/TDinsightV3-9-wirte-metrics.png)

The relevant write-related metrics statistics, including:

1. **Total Rows and Wal Write Time**：The number of rows written processed by the vnode and the time taken by the vnode to write to the WAL.
2. **Commit Count and Time**：The number of commit requests processed by the vnode and the time taken for these commits.
3. **Blocked Commit Count and Time**：The number of times vnode commits were blocked and the total time spent blocked.
4. **Merge Count and Time**：The number of merge requests processed by the vnode and the time taken for these merges.
5. **Last Cache Commit Count and Time**：The number of times the vnode processed flushing the last cache to disk and the time taken for these operations.
    
### taosAdapter

![taosadapter](../../assets/TDinsightV3-8-taosadaper.png)

Support monitoring taosAdapter rest and websocket request statistics. Including:

1. **Total**: number of total requests.
2. **Successful**: number of total successful requests.
3. **Failed**: number of total failed requests.
4.  **Queries**: number of total queries.
5.  **Writes**: number of total inserts or updates.
6.  **Other**: number of total other requests.

There is also line charts for detail dimensions.

### TDengine Automatic imported alert rules

**Alert rules imported in version 11**

When adding a data source, select 'Load TDengine Alert' and click on the alert menu to display the loaded alarm alerts, as shown below:
![Alert rules](../../assets/alert-rule.png)

**Import Alert Rule Description**

| alert rules                               | description                                                                                                                                                                           |
| :---------------------------------------- | :------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| CPU load of dnode                         | When the CPU load of a node in the cluster remains above 80% for 5 minutes, an alert will be triggered                                                                                |
| Memory usage of dnode                     | When the memory usage of a node in the cluster remains above 60% for 5 minutes, an alert will be triggered                                                                            |
| Disk capacity occupancy of dnode          | When the disk usage of a node in the cluster remains above 60% for 5 minutes, an alert will be triggered                                                                              |
| Cluster authorization expires             | When the authorization time of the cluster is less than 60 days, an alert will be triggered                                                                                           |
| Approaching authorized measurement points | When the user's measurement points exceed 90% of the total measurement points, an alert will be triggered                                                                             |
| dnode offline                             | Check every 30 seconds. When a dnode goes offline in the cluster, an alert will be triggered                                                                                          |
| vnode offline                             | Check every 30 seconds. When a vnode goes offline in the cluster, an alert will be triggered                                                                                          |
| dnode restarted                           | Check every 30 seconds. When a dnode node in the cluster is restarted, an alert will be triggered                                                                                     |
| dnode status                              | Check every 180 seconds. When the number of node status reports in the cluster is less than 3 times, an alert will be triggered                                                       |
| Query the number of concurrent requests   | Check every 60 seconds. When the SQL query is executed more than 100 times, an alert will be triggered                                                                                |
| Number of data deletion requests          | Check every 30 seconds, and an alert will be triggered when SQL is submitted to the database for execution to delete data                                                             |
| Adapter Restful request failed            | Check every 30 seconds, the connector sends database operation instructions to the Adapter through Restful mode. If the execution fails more than 5 times, an alert will be triggered |
| Adapter Websocket request failed          | Check every 30 seconds, the connector sends database operation instructions to the Adapter via Websocket. If the execution fails more than 5 times, an alert will be triggered        |
| Slow query                                | Check every minute, and when a query is found to take more than 300 seconds, an alert will be triggered                                                                               |


## Upgrade

You can update TDengine Grafana datasource plugin and TDinsight for 3.x by re-installation.

## Uninstall

To completely uninstall TDinsight during a manual installation, you need to clean up the following.

1. the TDinsight v3.x Dashboard in Grafana.
2. the Data Source in Grafana.
3. the `tdengine-datasource` plugin in Grafana.

[Grafana]: https://grafana.com

[TDengine]: https://www.taosdata.com
