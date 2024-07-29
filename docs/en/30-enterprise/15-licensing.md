---
toc_max_heading_level: 4
title: License Management
sidebar_label: License Management
---

## Introduction

- TDengine Enterprise requires an active license to operate.
- Version 3.2.3.0 has restructured the license mechanism to a large extent. Therefore, this article takes version 3.2.3.0 as the boundary and explains them separately.


## Before version 3.2.3.0

### License Key and Licensed Items

- The license key contains the values of each licensed item. The licensed item typically includes expiration time, number of time series, storage space, number of database instances, number of users, number of dnodes, and number of CPU cores.
- License keys are generated on a per-cluster basis. If a cluster contains multiple license keys, the highest license level among all keys takes effect.


### Acquiring License Keys

- The license keys are issued by TDengine Team, and customers are required to provide `machine code` or `cluster ID`.
- To obtain the machine code for your system, run the `taosd -k` command. Each machine in a cluster corresponds to one machine code.

```shell
$ taosd -k
machine code: KGQ8Y+haR3iz4lHnX9gHngYl
```

- To obtain the cluster ID for your system, run `show cluster\G;` statement in the TDengine Client. The value of the `id` column is your cluster ID. All machines in a cluster have the same cluster ID.

```shell
taos> show cluster\G;
*************************** 1.row ***************************
         id: 3743835620574542136
       name: 324cbefb-e667-462f-9ebd-d9b0fc753bb3
     uptime: 0
create_time: 2023-08-31 10:37:36.767
    version: trial
expire_time: NULL
Query OK, 1 row(s) in set (0.001976s)
```

### Activating License Keys

1. You can activate your license key by modifying the `taos.cfg` configuration file or by running a SQL statement.
2. If you activate different license keys in `taos.cfg` and through SQL, only the license key activated through SQL takes effect.
3. It is recommended that you activate your license key through SQL. Activating a key by modifying `taos.cfg` is deprecated.
4. After you activate a license key, the key is stored in your cluster and can be viewed with the `show dnodes` statement.
 


#### Configuration File

- To activate your license in `taos.cfg`, add `activeCode <license-key>` to the file. After modifying `taos.cfg`, the license will be activated within 1 minute. You can also restart the dnode containing the leader mnode for your cluster to activate the license immediately.

```shell
activeCode z9sdqG8w67fqBXlHnxWAQezQc/mabvN9N2maa6ksK6JJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2di72ZL6EAo0mcYiPlK2dDdmAt3P46xKs4Q=
```
#### SQL Statement

- To activate your license through SQL, run the `alter dnode` statement in the TDengine Client. You can modify one or all dnodes in your cluster. The license is typically activated within 5 seconds. On clusters with a large number of nodes, the activation process may take longer.
- The following command activates a license on the dnode whose ID is 1. You can run the `show dnodes` statement to find the ID of your dnodes.

```shell
taos> alter dnode 1 'activeCode' 'tP+2soIXpPwxqdKIK2Vz80laXs7Gs9nYN2maa6ksK6JJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2djdUysgjtzivcYiPlK2dDdmABPzBHc7VCc=';
Query OK, 0 row(s) affected (0.003637s) 
```

- The following commands activates a license on all dnodes in the cluster.

```shell
taos> alter all dnodes 'activeCode' 'Wn8j+6KVVRnGIj5StnQ3Zs2XgtVr+h+Vue1VyrZhTL/HeS3wtya3rcYiPlK2dDdmYddxLWSWeUDGIj5StnQ3ZsYiPlK2dDdmrRsroQil5AE=';
```

### Removing License Keys

- You can remove a license key by setting the value of `activeCode` to an empty value or a null value.

```shell
taos> alter dnode 5 'activeCode' '';
taos> alter dnode 5 'activeCode';
taos> alter all dnodes 'activeCode' '';
taos> alter all dnodes 'activeCode';
```
### Viewing License Keys

- You can view the license key by running `show dnodes` statement in the TDengine Client.

```shell
taos> show dnodes\G;
*************************** 1.row ***************************
            id: 1
      endpoint: u3-31:6030
        vnodes: 0
support_vnodes: 17
        status: ready
   create_time: 2023-08-31 10:37:36.765
   reboot_time: 2023-08-31 10:37:36.754
          note: 
   active_code: Wn8j+6KVVRnGIj5StnQ3Zs2XgtVr+h+Vue1VyrZhTL/HeS3wtya3rcYiPlK2dDdmYddxLWSWeUDGIj5StnQ3ZsYiPlK2dDdmrRsroQil5AE=
 c_active_code: 
Query OK, 1 row(s) in set (0.003062s)
```

### Viewing Licensing Information

- You can view the licensing information of your cluster by running the `show grants\G;` statement in the TDengine Client.
- Licensed items between `opc_da` and `mqtt` apply to taosX data source import. Taking `opc_da` as an example, `type` is the data source type, `number` is the upper limit of the data source connections, `speed` is the upper limit of the data source speed, and `expire` is the number of days between 1970-01-01 and the data source expiration time.

```shell
taos> show grants\G;
*************************** 1.row ***************************
    version: trial
expire_time: 2023-10-31 09:37:36
    expired: false
    storage: unlimited
 timeseries: unlimited
  databases: unlimited
      users: unlimited
   accounts: unlimited
     dnodes: unlimited
connections: unlimited
    streams: unlimited
  cpu_cores: unlimited
      speed: unlimited
  querytime: unlimited
     opc_da: {"type":"OPC_DA","number":1,"speed":-1,"expire":"19615"}
     opc_ua: {"type":"OPC_UA","number":1,"speed":-1,"expire":"19615"}
         pi: {"type":"Pi","number":1,"speed":-1,"expire":"19615"}
      kafka: {"type":"Kafka","number":1,"speed":-1,"expire":"19615"}
   influxdb: {"type":"InfluxDB","number":1,"speed":-1,"expire":"19615"}
       mqtt: {"type":"MQTT","number":1,"speed":-1,"expire":"19615"}
Query OK, 1 row(s) in set (0.003706s)
```

## Version 3.2.3.0 and later

### License Key and Licensed Items

- The license key consists of one or multiple licensed items. There are two categories of licensed items: `fundamental items` and `optional items`. The `fundamental items` include expiration time, number of time series, number of dnodes, and number of CPU cores; and the `optional items` include maintenance services, data subscription, stream computing, views, audit logs, multi-level storage, data backup and recovery, CSV Import and import from various data sources, etc.
- You can apply multiple license keys one by one, while each license key can include one or more licensed items. However, the first license key must include the `fundamental items` to make sure basic database functionalities can be used first.


### Acquiring License Keys

- The license keys are issued by TDengine Team, and customers are required to provide `machine code` and `cluster ID`, which can be obtained by running `show cluster machines\G;` statement in the TDengine Client：

```shell
taos> show cluster machines\G;
*************************** 1.row ***************************
     id: 6418372034255504533
machine: AylNik5f3er9a2a9dz08vwW6
Query OK, 1 row(s) in set (0.003007s)
```

### Activating License Keys

- You can activate your license key by running `alter cluster 'activeCode' '${activeCode}';` statement in the TDengine Client, which usually takes effect within 5 seconds.

```shell
taos> alter cluster 'activeCode' 'kv36cnF9GF8Hofj4vUK5XNyDXwbLrKr8dCqLcpsU18HABQ8bxCFgXBxgpcXuqn2znf9gBksqh9c2'; 
Query OK, 0 row(s) affected (0.004947s)
```

### Revoking License Keys

- You can manually revoke a license key by running `alter cluster 'activeCode' 'revoked';` statement in the TDengine client.
- After executing this command, all license keys activated previously will expire after 7 days.

```shell
taos> alter cluster 'activeCode' 'revoked';
Query OK, 0 row(s) affected (0.003184s)
```
### Viewing the License Operation Logs

- You can view the previous license operation logs by running `show grants logs\G;` statement in the TDengine Client.
- In the following output items, multiple records are separated by semicolons, and each record is separated by commas.
- `state` is the license status change log, while each log includes the fields representing `change time/change reason/initial status/current status`.
- `active` is the license key activation log, while each log includes the fields representing `activation time/license key digest`.
- `machine` is machine code information, while each log includes the fields representing `initial online time/dnodeId/machine code/machine code type`.

```shell
taos> show grants logs\G;
*************************** 1.row ***************************
  state: 2024-03-11 10:12:27,init,ungranted,ungranted;2024-03-11 10:26:55,alter,ungranted,granted;2024-03-11 10:30:40,alter,granted,revoked;2024-03-11 10:32:34,alter,revoked,granted;2024-03-11 10:34:24,alter,granted,revoked
 active: 2024-03-11 10:26:55,2Wvuk96zR3YdesRwebfDwXDrFmiTwP;2024-03-11 10:32:34,kv36cnF9GF8Hofj4vUK5XNyDXwbLrK
machine: 2024-03-11 10:32:34,1,AylNik5f3er9a2a9dz08vwW6,3
Query OK, 1 row(s) in set (0.003374s)
```

### Viewing the Licensing Information

- You can view the licensing information of `fundamental items` by running the `show grants\G;` statement in the TDengine Client.

```shell
taos> show grants\G;
*************************** 1.row ***************************
     version: official
 expire_time: 2024-07-01 08:00:00
service_time: 2024-03-11 10:12:25
     expired: false
       state: granted
  timeseries: 0/999
      dnodes: 1/10
   cpu_cores: 12/12
Query OK, 1 row(s) in set (0.003374s)
```

- You can view the licensing information of `optional items` by running the `show grants full;` statement in the TDengine Client.
- Licensed items between `stream` and `view` support setting expiration time(`expire`) and quantity(`limits`. Taking 0/8 as an example, 0 represents the current number of licensed items, and 8 represents the upper limit of licensed items).
- Licensed items between `audit` and `backup_restore` only support setting the expiration time, but does not support setting the quantity.
- Licensed items between `opc_da` and `td3.0` apply to taosX data source import. Taking the `limits` column of `opc_da` as an example, `number` is the upper limit of the data source connections, `speed` is the upper limit of the data source speed, and `expire` is the number of seconds between 1970-01-01 00:00:00 and the data source `expiration time`.

```shell
taos> show grants full;
   grant_name    |    display_name    |        expire        |                                       limits                                        |
====================================================================================================================================================
 stream          | stream             | 2024-03-21 10:12:25  | 0/8                                                                                 |
 subscription    | subscription       | 2024-03-21 10:12:25  | 0/8                                                                                 |
 view            | view               | 2024-03-21 10:12:25  | 0/8                                                                                 |
 audit           | audit              | 2024-03-21 10:12:25  |                                                                                     |
 csv             | csv                | 2024-03-21 10:12:25  |                                                                                     |
 storage         | multi_tier_storage | 2024-03-21 10:12:25  |                                                                                     |
 backup_restore  | backup_restore     | 2024-03-21 10:12:25  |                                                                                     |
 opc_da          | OPC_DA             | 2024-03-21 10:12:25  | {"number":1, "speed":-1, "expire":"1710987145", "expireTime":"2024-03-21 10:12:25"} |
 opc_ua          | OPC_UA             | 2024-03-21 10:12:25  | {"number":1, "speed":-1, "expire":"1710987145", "expireTime":"2024-03-21 10:12:25"} |
 pi              | Pi                 | 2024-03-21 10:12:25  | {"number":1, "speed":-1, "expire":"1710987145", "expireTime":"2024-03-21 10:12:25"} |
 kafka           | Kafka              | 2024-03-21 10:12:25  | {"number":1, "speed":-1, "expire":"1710987145", "expireTime":"2024-03-21 10:12:25"} |
 influxdb        | InfluxDB           | 2024-03-21 10:12:25  | {"number":1, "speed":-1, "expire":"1710987145", "expireTime":"2024-03-21 10:12:25"} |
 mqtt            | MQTT               | 2024-03-21 10:12:25  | {"number":1, "speed":-1, "expire":"1710987145", "expireTime":"2024-03-21 10:12:25"} |
 avevahistorian  | avevaHistorian     | 2024-03-21 10:12:25  | {"number":1, "speed":-1, "expire":"1710987145", "expireTime":"2024-03-21 10:12:25"} |
 opentsdb        | OpenTSDB           | 2024-03-21 10:12:25  | {"number":1, "speed":-1, "expire":"1710987145", "expireTime":"2024-03-21 10:12:25"} |
 td2.6           | TDengine2.6        | 2024-03-21 10:12:25  | {"number":1, "speed":-1, "expire":"1710987145", "expireTime":"2024-03-21 10:12:25"} |
 td3.0           | TDengine3.0        | 2024-03-21 10:12:25  | {"number":1, "speed":-1, "expire":"1710987145", "expireTime":"2024-03-21 10:12:25"} |
Query OK, 17 row(s) in set (0.003513s)
```