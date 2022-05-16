---
title: TDengine Monitoring
---

After TDengine is started, a database named `log` for monitoring is created automatically. The information about CPU, memory, disk, bandwidth, number of requests, disk I/O speed, slow query is written into `log` database on the basis of a predefined interval. Besides, some important system operations, like logon, create user, drop database, and alerts and warnings generated in TDengine are written into `log` database too. System operator can view the data in `log` database from TDengine CLI or from a web console.

Collection of the monitoring information is enabled by default, but can be disabled by parameter `monitor` in configuration file. 

## TDinsight 

TDinsight is a total solution which uses the monitor database `log` mentioned previously and Grafana to monitor a TDengine cluster.

From version 2.3.3.0, more monitoring data has been added in the `log` database. Please refer to [TDinsight Grafana Dashboard](https://grafana.com/grafana/dashboards/15167) to learn more details about using TDinsight to monitor TDengine.

A script `TDinsight.sh` is provided to deploy TDinsight in automatic way.

Download `TDinsight.sh` with below command:

```bash
wget https://github.com/taosdata/grafanaplugin/raw/master/dashboards/TDinsight.sh
chmod +x TDinsight.sh
```

Prepare：

1. TDengine Server

   - The URL of REST service：for example `http://localhost:6041` if TDengine is deployed locally
   - User name and password

2. Grafana Alert Notification

There are two ways to setup Grafana alert notification.

- An existing Grafana Notification Channel can be specified with parameter `-E`, the notifier uid of the channel can be obtained by `curl -u admin:admin localhost:3000/api/alert-notifications |jq`

     ```bash
     sudo ./TDinsight.sh -a http://localhost:6041 -u root -p taosdata -E <notifier uid>
     ```

- The AliClund SMS alert built in TDengine data source plugin can be enabled with parameter `-s`, the parameters of this way are as follows:

  - `-I`: AliCloud SMS Key ID
  - `-K`: AliCloud SMS Key Secret
  - `-S`: AliCloud SMS Signature
  - `-C`: SMS notification template
  - `-T`: Input parameters in JSON format for the SMS notification template, for example`{"alarm_level":"%s","time":"%s","name":"%s","content":"%s"}`
  - `-B`: List of mobile numbers to be notified

  Below is an example of the full command using this way.

  ```bash
     sudo ./TDinsight.sh -a http://localhost:6041 -u root -p taosdata -s \
       -I XXXXXXX -K XXXXXXXX -S taosdata -C SMS_1111111 -B 18900000000 \
       -T '{"alarm_level":"%s","time":"%s","name":"%s","content":"%s"}'
  ```

Launch `TDinsight.sh` as above command and restart Grafana, then open Dashboard `http://localhost:3000/d/tdinsight`.

For more use cases and restrictions please refer to [TDinsight](/reference/tdinsight/).
