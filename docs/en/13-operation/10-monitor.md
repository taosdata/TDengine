---
title: TDengine Monitoring
description: This document describes how to monitor your TDengine cluster.
---

After TDengine is started, it automatically writes monitoring data including CPU, memory and disk usage, bandwidth, number of requests, disk I/O speed, slow queries, into a designated database at a predefined interval through taosKeeper. Additionally, some important system operations, like logon, create user, drop database, and alerts and warnings generated in TDengine are written into the `log` database too. A system operator can view the data in `log` database from TDengine CLI or from a web console.

The collection of the monitoring information is enabled by default, but can be disabled by parameter `monitor` in the configuration file. 

## TDinsight 

TDinsight is a complete solution which uses the monitoring database `log` mentioned previously, and Grafana, to monitor a TDengine cluster.

A script `TDinsight.sh` is provided to deploy TDinsight automatically.

Download `TDinsight.sh` with the below command:

```bash
wget https://github.com/taosdata/grafanaplugin/raw/master/dashboards/TDinsight.sh
chmod +x TDinsight.sh
```

Prepare:

1. TDengine Server

   - The URL of REST service: for example `http://localhost:6041` if TDengine is deployed locally
   - User name and password

2. Grafana Alert Notification

You can use below command to setup Grafana alert notification.

An existing Grafana Notification Channel can be specified with parameter `-E`, the notifier uid of the channel can be obtained by `curl -u admin:admin localhost:3000/api/alert-notifications |jq`

     ```bash
     ./TDinsight.sh -a http://localhost:6041 -u root -p taosdata -E <notifier uid>
     ```

Launch `TDinsight.sh` with the command above and restart Grafana, then open Dashboard `http://localhost:3000/d/tdinsight`.

## log database

The data of tdinsight dashboard is stored in `log` database (default. You can change it in taoskeeper's config file. For more infrmation, please reference to [taoskeeper document](../../reference/taosKeeper)). The taoskeeper will create log database on taoskeeper startup.

### cluster\_info table

`cluster_info` table contains cluster information records.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|first\_ep|VARCHAR||first ep of cluster|
|first\_ep\_dnode\_id|INT||dnode id or first\_ep|
|version|VARCHAR||tdengine version. such as: 3.0.4.0|
|master\_uptime|FLOAT||days of master's uptime|
|monitor\_interval|INT||monitor interval in second|
|dbs\_total|INT||total number of databases in cluster|
|tbs\_total|BIGINT||total number of tables in cluster|
|stbs\_total|INT||total number of stables in cluster|
|dnodes\_total|INT||total number of dnodes in cluster|
|dnodes\_alive|INT||total number of dnodes in ready state|
|mnodes\_total|INT||total number of  mnodes in cluster|
|mnodes\_alive|INT||total number of  mnodes in ready state|
|vgroups\_total|INT||total number of vgroups in cluster|
|vgroups\_alive|INT||total number of vgroups in ready state|
|vnodes\_total|INT||total number of vnode in cluster|
|vnodes\_alive|INT||total number of vnode in ready state|
|connections\_total|INT||total number of connections to cluster|
|topics\_total|INT||total number of topics in cluster|
|streams\_total|INT||total number of streams in cluster|
|protocol|INT||protocol version|
|cluster\_id|NCHAR|TAG|cluster id|

### d\_info table

`d_info` table contains dnodes information records.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|status|VARCHAR||dnode status|
|dnode\_ep|NCHAR|TAG|dnode endpoint|
|cluster\_id|NCHAR|TAG|cluster id|

### m\_info table 

`m_info` table contains mnode information records.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|role|VARCHAR||the role of mnode. leader or follower|
|mnode\_id|INT|TAG|master node id|
|mnode\_ep|NCHAR|TAG|master node endpoint|
|cluster\_id|NCHAR|TAG|cluster id|

### dnodes\_info table 

`dnodes_info` table contains dnodes information records.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|uptime|FLOAT||dnode uptime in `days`|
|cpu\_engine|FLOAT||cpu usage of tdengine. read from `/proc/<taosd_pid>/stat`|
|cpu\_system|FLOAT||cpu usage of server. read from `/proc/stat`|
|cpu\_cores|FLOAT||cpu cores of server|
|mem\_engine|INT||memory usage of tdengine. read from `/proc/<taosd_pid>/status`|
|mem\_system|INT||available memory on the server in `KB`|
|mem\_total|INT||total memory of server in `KB`|
|disk\_engine|INT|||
|disk\_used|BIGINT||usage of data dir in `bytes`|
|disk\_total|BIGINT||the capacity of data dir in `bytes`|
|net\_in|FLOAT||network throughput rate in byte/s. read from `/proc/net/dev`|
|net\_out|FLOAT||network throughput rate in byte/s. read from `/proc/net/dev`|
|io\_read|FLOAT||io throughput rate in byte/s. read from `/proc/<taosd_pid>/io`|
|io\_write|FLOAT||io throughput rate in byte/s. read from `/proc/<taosd_pid>/io`|
|io\_read\_disk|FLOAT||io throughput rate of disk in byte/s. read from `/proc/<taosd_pid>/io`|
|io\_write\_disk|FLOAT||io throughput rate of disk in byte/s. read from `/proc/<taosd_pid>/io`|
|req\_select|INT||number of select queries received per dnode|
|req\_select\_rate|FLOAT||number of select queries received per dnode divided by monitor interval.|
|req\_insert|INT||number of insert queries received per dnode|
|req\_insert\_success|INT||number of successfully insert queries received per dnode|
|req\_insert\_rate|FLOAT||number of insert queries received per dnode divided by monitor interval|
|req\_insert\_batch|INT||number of batch insertions|
|req\_insert\_batch\_success|INT||number of successful batch insertions|
|req\_insert\_batch\_rate|FLOAT||number of batch insertions divided by monitor interval|
|errors|INT||dnode errors|
|vnodes\_num|INT||number of vnodes per dnode|
|masters|INT||number of master vnodes|
|has\_mnode|INT||if the dnode has mnode|
|has\_qnode|INT||if the dnode has qnode|
|has\_snode|INT||if the dnode has snode|
|has\_bnode|INT||if the dnode has bnode|
|dnode\_id|INT|TAG|dnode id|
|dnode\_ep|NCHAR|TAG|dnode endpoint|
|cluster\_id|NCHAR|TAG|cluster id|

### data\_dir table 

`data_dir` table contains data directory information records. 

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|name|NCHAR||data directory. default is `/var/lib/taos`|
|level|INT||level for multi-level storage|
|avail|BIGINT||available space for data directory in `bytes`|
|used|BIGINT||used space for data directory in `bytes`| 
|total|BIGINT||total space for data directory in `bytes`|
|dnode\_id|INT|TAG|dnode id|
|dnode\_ep|NCHAR|TAG|dnode endpoint|
|cluster\_id|NCHAR|TAG|cluster id|

### log\_dir table

`log_dir` table contains log directory information records.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|name|NCHAR||log directory. default is `/var/log/taos/`|
|avail|BIGINT||available space for log directory in `bytes`|
|used|BIGINT||used space for data directory in `bytes`|
|total|BIGINT||total space for data directory in `bytes`|
|dnode\_id|INT|TAG|dnode id|
|dnode\_ep|NCHAR|TAG|dnode endpoint|
|cluster\_id|NCHAR|TAG|cluster id|

### temp\_dir table

`temp_dir` table contains temp dir information records.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|name|NCHAR||temp directory. default is `/tmp/`|
|avail|BIGINT||available space for temp directory in `bytes`|
|used|BIGINT||used space for temp directory in `bytes`|
|total|BIGINT||total space for temp directory in `bytes`|
|dnode\_id|INT|TAG|dnode id|
|dnode\_ep|NCHAR|TAG|dnode endpoint|
|cluster\_id|NCHAR|TAG|cluster id|

### vgroups\_info table

`vgroups_info` table contains vgroups information records.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|vgroup\_id|INT||vgroup id|
|database\_name|VARCHAR||database for the vgroup|
|tables\_num|BIGINT||number of tables per vgroup|
|status|VARCHAR||status|
|dnode\_id|INT|TAG|dnode id|
|dnode\_ep|NCHAR|TAG|dnode endpoint|
|cluster\_id|NCHAR|TAG|cluster id|

### vnodes\_role table 

`vnodes_role` table contains vnode role information records.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|vnode\_role|VARCHAR||role. leader or follower|
|dnode\_id|INT|TAG|dnode id|
|dnode\_ep|NCHAR|TAG|dnode endpoint|
|cluster\_id|NCHAR|TAG|cluster id|

### log\_summary table

`log_summary` table contains log summary information records.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|error|INT||error count|
|info|INT||info count|
|debug|INT||debug count|
|trace|INT||trace count|
|dnode\_id|INT|TAG|dnode id|
|dnode\_ep|NCHAR|TAG|dnode endpoint|
|cluster\_id|NCHAR|TAG|cluster id|

### grants\_info table 

`grants_info` table contains grants information records.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|expire\_time|BIGINT||time until grants expire in seconds|
|timeseries\_used|BIGINT||timeseries used|
|timeseries\_total|BIGINT||total timeseries|
|dnode\_id|INT|TAG|dnode id|
|dnode\_ep|NCHAR|TAG|dnode endpoint|
|cluster\_id|NCHAR|TAG|cluster id|

### keeper\_monitor table 

`keeper_monitor` table contains keeper monitor information records.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|cpu|FLOAT||cpu usage|
|mem|FLOAT||memory usage|
|identify|NCHAR|TAG||

### taosadapter\_restful\_http\_request\_total table 

`taosadapter_restful_http_request_total` table contains taosadapter rest request information record. The timestamp column of this table is `_ts`.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|\_ts|TIMESTAMP||timestamp|
|gauge|DOUBLE||metric value|
|client\_ip|NCHAR|TAG|client ip|
|endpoint|NCHAR|TAG|taosadpater endpoint|
|request\_method|NCHAR|TAG|request method|
|request\_uri|NCHAR|TAG|request uri|
|status\_code|NCHAR|TAG|status code|

### taosadapter\_restful\_http\_request\_fail table

`taosadapter_restful_http_request_fail` table contains taosadapter failed rest request information record. The timestamp column of this table is `_ts`.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|\_ts|TIMESTAMP||timestamp|
|gauge|DOUBLE||metric value|
|client\_ip|NCHAR|TAG|client ip|
|endpoint|NCHAR|TAG|taosadpater endpoint|
|request\_method|NCHAR|TAG|request method|
|request\_uri|NCHAR|TAG|request uri|
|status\_code|NCHAR|TAG|status code|

### taosadapter\_restful\_http\_request\_in\_flight table 

`taosadapter_restful_http_request_in_flight` table contains taosadapter rest request information record in real time. The timestamp column of this table is `_ts`.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|\_ts|TIMESTAMP||timestamp|
|gauge|DOUBLE||metric value|
|endpoint|NCHAR|TAG|taosadpater endpoint|

### taosadapter\_restful\_http\_request\_summary\_milliseconds table

`taosadapter_restful_http_request_summary_milliseconds` table contains the summary or rest information record. The timestamp column of this table is `_ts`.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|\_ts|TIMESTAMP||timestamp|
|count|DOUBLE|||
|sum|DOUBLE|||
|0.5|DOUBLE|||
|0.9|DOUBLE|||
|0.99|DOUBLE|||
|0.1|DOUBLE|||
|0.2|DOUBLE|||
|endpoint|NCHAR|TAG|taosadpater endpoint|
|request\_method|NCHAR|TAG|request method|
|request\_uri|NCHAR|TAG|request uri|

### taosadapter\_system\_mem\_percent table

`taosadapter_system_mem_percent` table contains taosadapter memory usage information. The timestamp of this table is `_ts`. 

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|\_ts|TIMESTAMP||timestamp|
|gauge|DOUBLE||metric value|
|endpoint|NCHAR|TAG|taosadpater endpoint|

### taosadapter\_system\_cpu\_percent table 

`taosadapter_system_cpu_percent` table contains taosadapter cup usage information. The timestamp of this table is `_ts`.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|\_ts|TIMESTAMP||timestamp|
|gauge|DOUBLE||mertic value|
|endpoint|NCHAR|TAG|taosadpater endpoint|

