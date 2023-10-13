---
toc_max_heading_level: 4
title: License Management
sidebar_label: License Management
---

## Introduction

TDengine Enterprise requires an active license to operate.


## License Key and Authorizations

- The license key contains the values of all authorizations. Authorizations typically include an expiration data, number of nodes, storage space, database instances, users, dnodes, and CPU cores.
- License keys are generated on a per-cluster basis. If a cluster contains multiple license keys, the highest authorization level among all keys takes effect.


## Generating and Distributing License Keys

- The TDengine Team generates license keys based on machine code or cluster ID.
- To obtain the machine code for your system, run the `taosd -k` command. Each machine in a cluster corresponds to one machine code.

```shell
$ taosd -k
machine code: KGQ8Y+haR3iz4lHnX9gHngYl
```

- To obtain the cluster ID for your system, run `show cluster\G;` in the TDengine CLI. The value of the `id` column is your cluster ID. All machines in a cluster have the same cluster ID.

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

## Configuring and Viewing Licenses

1. You can configure your license key by modifying the `taos.cfg` configuration file or by running a SQL statement.
2. If you configure different license keys in `taos.cfg` and through SQL, only the license key configured through SQL takes effect.
3. It is recommended that you configure your license key through SQL. Configuring a key by modifying `taos.cfg` is deprecated.
4. After you configure a license key, the key is stored in your cluster and can be viewed with the `show dnodes` statement.
 


### Configuration File

To activate your license in `taos.cfg`, add `activeCode <license-key>` to the file. After modifying `taos.cfg`, the license will be activated after 5 minutes. You can also restart the dnode containing the leader mnode for your cluster to activate the license immediately.

```shell
activeCode z9sdqG8w67fqBXlHnxWAQezQc/mabvN9N2maa6ksK6JJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2di72ZL6EAo0mcYiPlK2dDdmAt3P46xKs4Q=
```
### SQL Statement

To activate your license through SQL, run the `alter dnode` statement in the TDengine Client. You can modify one or all dnodes in your cluster. The license is typically activated within 5 minutes. On clusters with a large number of nodes, the activation process may take longer.

The following command activates a license on the dnode whose ID is 1. You can run the `show dnodes` statement to find the ID of your dnodes.

```shell
taos> alter dnode 1 'activeCode' 'tP+2soIXpPwxqdKIK2Vz80laXs7Gs9nYN2maa6ksK6JJWl7OxrPZ2ElaXs7Gs9nYSVpezsaz2djdUysgjtzivcYiPlK2dDdmABPzBHc7VCc=';
Query OK, 0 row(s) affected (0.003637s) 
```

The following commands activates a license on all dnodes in the cluster.

```shell
taos> alter all dnodes 'activeCode' 'Wn8j+6KVVRnGIj5StnQ3Zs2XgtVr+h+Vue1VyrZhTL/HeS3wtya3rcYiPlK2dDdmYddxLWSWeUDGIj5StnQ3ZsYiPlK2dDdmrRsroQil5AE=';
```

### Removing a License Key

You can remove a license key by setting the value of `activeCode` to an empty value or a null value.

```shell
taos> alter dnode 5 'activeCode' '';
taos> alter dnode 5 'activeCode';
taos> alter all dnodes 'activeCode' '';
taos> alter all dnodes 'activeCode';
```
### Viewing a License Key

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

### Viewing the Licensing Status

You can check the licensing status of your cluster by running the `show grants\G;` statement from the TDengine Client.

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
