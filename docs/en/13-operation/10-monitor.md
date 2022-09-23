---
title: TDengine Monitoring
---

After TDengine is started, it automatically writes monitoring data including CPU, memory and disk usage, bandwidth, number of requests, disk I/O speed, slow queries, into a designated database at a predefined interval through taosKeeper. Additionally, some important system operations, like logon, create user, drop database, and alerts and warnings generated in TDengine are written into the `log` database too. A system operator can view the data in `log` database from TDengine CLI or from a web console.

The collection of the monitoring information is enabled by default, but can be disabled by parameter `monitor` in the configuration file. 

## TDinsight 

TDinsight is a complete solution which uses the monitoring database `log` mentioned previously, and Grafana, to monitor a TDengine cluster.

Please refer to [TDinsight Grafana Dashboard](../../reference/tdinsight) to learn more details about using TDinsight to monitor TDengine.

A script `TDinsight.sh` is provided to deploy TDinsight automatically.

Download `TDinsight.sh` with the below command:

```bash
wget https://github.com/taosdata/grafanaplugin/raw/master/dashboards/TDinsight.sh
chmod +x TDinsight.sh
```

Prepare：

1. TDengine Server

   - The URL of REST service：for example `http://localhost:6041` if TDengine is deployed locally
   - User name and password

2. Grafana Alert Notification

You can use below command to setup Grafana alert notification.

An existing Grafana Notification Channel can be specified with parameter `-E`, the notifier uid of the channel can be obtained by `curl -u admin:admin localhost:3000/api/alert-notifications |jq`

     ```bash
     sudo ./TDinsight.sh -a http://localhost:6041 -u root -p taosdata -E <notifier uid>
     ```

Launch `TDinsight.sh` with the command above and restart Grafana, then open Dashboard `http://localhost:3000/d/tdinsight`.

For more use cases and restrictions please refer to [TDinsight](/reference/tdinsight/).
