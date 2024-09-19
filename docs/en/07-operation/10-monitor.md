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

The data of tdinsight dashboard is stored in `log` database (default. You can change it in taoskeeper's config file. For more infrmation, please reference to [taoskeeper document](../../reference/components/taosKeeper)). The taoskeeper will create log database on taoskeeper startup.

### taosd\_cluster\_basic table

`taosd_cluster_basic` table contains cluster basic information.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|first\_ep|VARCHAR||first ep of cluster|
|first\_ep\_dnode\_id|INT||dnode id or first\_ep|
|cluster_version|VARCHAR||tdengine version. such as: 3.0.4.0|
|cluster\_id|VARCHAR|TAG|cluster id|

### taosd\_cluster\_info table

`taosd_cluster_info` table contains cluster information records.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|cluster\_uptime|DOUBLE||seconds of master's uptime|
|dbs\_total|DOUBLE||total number of databases in cluster|
|tbs\_total|DOUBLE||total number of tables in cluster|
|stbs\_total|DOUBLE||total number of stables in cluster|
|dnodes\_total|DOUBLE||total number of dnodes in cluster|
|dnodes\_alive|DOUBLE||total number of dnodes in ready state|
|mnodes\_total|DOUBLE||total number of  mnodes in cluster|
|mnodes\_alive|DOUBLE||total number of  mnodes in ready state|
|vgroups\_total|DOUBLE||total number of vgroups in cluster|
|vgroups\_alive|DOUBLE||total number of vgroups in ready state|
|vnodes\_total|DOUBLE||total number of vnode in cluster|
|vnodes\_alive|DOUBLE||total number of vnode in ready state|
|connections\_total|DOUBLE||total number of connections to cluster|
|topics\_total|DOUBLE||total number of topics in cluster|
|streams\_total|DOUBLE||total number of streams in cluster|
|grants_expire\_time|DOUBLE||time until grants expire in seconds|
|grants_timeseries\_used|DOUBLE||timeseries used|
|grants_timeseries\_total|DOUBLE||total timeseries|
|cluster\_id|VARCHAR|TAG|cluster id|

### taosd\_vgroups\_info table

`taosd_vgroups_info` table contains vgroups information records.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|tables\_num|DOUBLE||number of tables per vgroup|
|status|DOUBLE||status, value range:unsynced = 0, ready = 1|
|vgroup\_id|VARCHAR|TAG|vgroup id|
|database\_name|VARCHAR|TAG|database for the vgroup|
|cluster\_id|VARCHAR|TAG|cluster id|

### taosd\_dnodes\_info table 

`taosd_dnodes_info` table contains dnodes information records.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|uptime|DOUBLE||dnode uptime in `seconds`|
|cpu\_engine|DOUBLE||cpu usage of tdengine. read from `/proc/<taosd_pid>/stat`|
|cpu\_system|DOUBLE||cpu usage of server. read from `/proc/stat`|
|cpu\_cores|DOUBLE||cpu cores of server|
|mem\_engine|DOUBLE||memory usage of tdengine. read from `/proc/<taosd_pid>/status`|
|mem\_free|DOUBLE||available memory on the server in `KB`|
|mem\_total|DOUBLE||total memory of server in `KB`|
|disk\_used|DOUBLE||usage of data dir in `bytes`|
|disk\_total|DOUBLE||the capacity of data dir in `bytes`|
|system\_net\_in|DOUBLE||network throughput rate in byte/s. read from `/proc/net/dev`|
|system\_net\_out|DOUBLE||network throughput rate in byte/s. read from `/proc/net/dev`|
|io\_read|DOUBLE||io throughput rate in byte/s. read from `/proc/<taosd_pid>/io`|
|io\_write|DOUBLE||io throughput rate in byte/s. read from `/proc/<taosd_pid>/io`|
|io\_read\_disk|DOUBLE||io throughput rate of disk in byte/s. read from `/proc/<taosd_pid>/io`|
|io\_write\_disk|DOUBLE||io throughput rate of disk in byte/s. read from `/proc/<taosd_pid>/io`|
|vnodes\_num|DOUBLE||number of vnodes per dnode|
|masters|DOUBLE||number of master vnodes|
|has\_mnode|DOUBLE||if the dnode has mnode, value range:include=1, not_include=0|
|has\_qnode|DOUBLE||if the dnode has qnode, value range:include=1, not_include=0|
|has\_snode|DOUBLE||if the dnode has snode, value range:include=1, not_include=0|
|has\_bnode|DOUBLE||if the dnode has bnode, value range:include=1, not_include=0|
|error\_log\_count|DOUBLE||error count|
|info\_log\_count|DOUBLE||info count|
|debug\_log\_count|DOUBLE||debug count|
|trace\_log\_count|DOUBLE||trace count|
|dnode\_id|VARCHAR|TAG|dnode id|
|dnode\_ep|VARCHAR|TAG|dnode endpoint|
|cluster\_id|VARCHAR|TAG|cluster id|

### taosd\_dnodes\_status table

`taosd_dnodes_status` table contains dnodes information records.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|status|DOUBLE||dnode status, value range:ready=1，offline =0|
|dnode\_id|VARCHAR|TAG|dnode id|
|dnode\_ep|VARCHAR|TAG|dnode endpoint|
|cluster\_id|VARCHAR|TAG|cluster id|

### taosd\_dnodes\_log\_dir table

`log_dir` table contains log directory information records.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|avail|DOUBLE||available space for log directory in `bytes`|
|used|DOUBLE||used space for data directory in `bytes`|
|total|DOUBLE||total space for data directory in `bytes`|
|name|VARCHAR|TAG|log directory. default is `/var/log/taos/`|
|dnode\_id|VARCHAR|TAG|dnode id|
|dnode\_ep|VARCHAR|TAG|dnode endpoint|
|cluster\_id|VARCHAR|TAG|cluster id|

### taosd\_dnodes\_data\_dir table 

`taosd_dnodes_data_dir` table contains data directory information records. 

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|avail|DOUBLE||available space for data directory in `bytes`|
|used|DOUBLE||used space for data directory in `bytes`| 
|total|DOUBLE||total space for data directory in `bytes`|
|level|VARCHAR|TAG|level for multi-level storage|
|name|VARCHAR|TAG|data directory. default is `/var/lib/taos`|
|dnode\_id|VARCHAR|TAG|dnode id|
|dnode\_ep|VARCHAR|TAG|dnode endpoint|
|cluster\_id|VARCHAR|TAG|cluster id|

### taosd\_mnodes\_info table 

`taosd_mnodes_info` table contains mnode information records.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|role|DOUBLE||the role of mnode. value range:offline = 0,follower = 100,candidate = 101,leader = 102,error = 103,learner = 104|
|mnode\_id|VARCHAR|TAG|master node id|
|mnode\_ep|VARCHAR|TAG|master node endpoint|
|cluster\_id|VARCHAR|TAG|cluster id|

### taosd\_vnodes\_role table 

`taosd_vnodes_role` table contains vnode role information records.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|role|DOUBLE||role. value range:offline = 0,follower = 100,candidate = 101,leader = 102,error = 103,learner = 104|
|vgroup\_id|VARCHAR|TAG|vgroup id|
|database\_name|VARCHAR|TAG|database for the vgroup|
|dnode\_id|VARCHAR|TAG|dnode id|
|cluster\_id|VARCHAR|TAG|cluster id|

### taosd\_sql\_req table

`taosd_sql_req` tables contains taosd sql records.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|count|DOUBLE||sql count|
|result|VARCHAR|TAG|sql execution result，value range: Success, Failed|
|username|VARCHAR|TAG|user name who executed the sql|
|sql\_type|VARCHAR|TAG|sql type，value range:inserted_rows|
|dnode\_id|VARCHAR|TAG|dnode id|
|dnode\_ep|VARCHAR|TAG|dnode endpoint|
|vgroup\_id|VARCHAR|TAG|dnode id|
|cluster\_id|VARCHAR|TAG|cluster id|

### taos\_sql\_req 表

`taos_sql_req` tables contains taos sql records.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|count|DOUBLE||sql count|
|result|VARCHAR|TAG|sql execution result，value range: Success, Failed|
|username|VARCHAR|TAG|user name who executed the sql|
|sql\_type|VARCHAR|TAG|sql type，value range:select, insert，delete|
|cluster\_id|VARCHAR|TAG|cluster id|

### taos\_slow\_sql 表

`taos_slow_sql` ables contains taos slow sql records.

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|count|DOUBLE||sql count|
|result|VARCHAR|TAG|sql execution result，value range: Success, Failed|
|username|VARCHAR|TAG|user name who executed the sql|
|duration|VARCHAR|TAG|sql execution duration，value range:3-10s,10-100s,100-1000s,1000s-|
|cluster\_id|VARCHAR|TAG|cluster id|


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

