---
title: 系统监控
description: 监控 TDengine 的运行状态
---

TDengine 通过 [taosKeeper](../../reference/taosKeeper/) 将服务器的 CPU、内存、硬盘空间、带宽、请求数、磁盘读写速度等信息定时写入指定数据库。TDengine 还将重要的系统操作（比如登录、创建、删除数据库等）日志以及各种错误报警信息进行记录。系统管理员可以从 CLI 直接查看这个数据库，也可以在 WEB 通过图形化界面查看这些监测信息。

这些监测信息的采集缺省是打开的，但可以修改配置文件里的选项 monitor 将其关闭或打开。

## TDinsight - 使用监控数据库 + Grafana 对 TDengine 进行监控的解决方案

我们提供了一个自动化脚本 `TDinsight.sh` 对 TDinsight 进行部署。

下载 `TDinsight.sh`：

```bash
wget https://github.com/taosdata/grafanaplugin/raw/master/dashboards/TDinsight.sh
chmod +x TDinsight.sh
```

准备：

1. TDengine Server 信息：

   - TDengine RESTful 服务：对本地而言，可以是 `http://localhost:6041`，使用参数 `-a`。
   - TDengine 用户名和密码，使用 `-u` `-p` 参数设置。

2. Grafana 告警通知

   - 使用已经存在的 Grafana Notification Channel `uid`，参数 `-E`。该参数可以使用 `curl -u admin:admin localhost:3000/api/alert-notifications |jq` 来获取。

     ```bash
     ./TDinsight.sh -a http://localhost:6041 -u root -p taosdata -E <notifier uid>
     ```

运行程序并重启 Grafana 服务，打开面板：`http://localhost:3000/d/tdinsight`。

## log 库

TDinsight dashboard 数据来源于 log 库（存放监控数据的默认db，可以在 taoskeeper 配置文件中修改，具体参考 [taoskeeper 文档](../../reference/taosKeeper)）。taoskeeper 启动后会自动创建 log 库，并将监控数据写入到该数据库中。

### taosd\_cluster\_basic 表

`taosd_cluster_basic` 表记录集群基础信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|first\_ep|VARCHAR||集群 first ep|
|first\_ep\_dnode\_id|INT||集群 first ep 的 dnode id|
|cluster_version|VARCHAR||tdengine version。例如：3.0.4.0|
|cluster\_id|VARCHAR|TAG|cluster id|

### taosd\_cluster\_info 表

`taosd_cluster_info` 表记录集群信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|cluster_uptime|DOUBLE||当前 master 节点的uptime。单位：秒|
|dbs\_total|DOUBLE||database 总数|
|tbs\_total|DOUBLE||当前集群 table 总数|
|stbs\_total|DOUBLE||当前集群 stable 总数|
|dnodes\_total|DOUBLE||当前集群 dnode 总数|
|dnodes\_alive|DOUBLE||当前集群 dnode 存活总数|
|mnodes\_total|DOUBLE||当前集群 mnode 总数|
|mnodes\_alive|DOUBLE||当前集群 mnode 存活总数|
|vgroups\_total|DOUBLE||当前集群 vgroup 总数|
|vgroups\_alive|DOUBLE||当前集群 vgroup 存活总数|
|vnodes\_total|DOUBLE||当前集群 vnode 总数|
|vnodes\_alive|DOUBLE||当前集群 vnode 存活总数|
|connections\_total|DOUBLE||当前集群连接总数|
|topics\_total|DOUBLE||当前集群 topic 总数|
|streams\_total|DOUBLE||当前集群 stream 总数|
|grants_expire\_time|DOUBLE||认证过期时间，企业版有效，社区版为 DOUBLE 最大值|
|grants_timeseries\_used|DOUBLE||已用测点数|
|grants_timeseries\_total|DOUBLE||总测点数，开源版本为 DOUBLE 最大值|
|cluster\_id|VARCHAR|TAG|cluster id|

### taosd\_vgroups\_info 表

`taosd_vgroups_info` 表记录虚拟节点组信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|tables\_num|DOUBLE||vgroup 中 table 数量|
|status|DOUBLE||vgroup 状态, 取值范围：unsynced = 0, ready = 1|
|vgroup\_id|VARCHAR|TAG|vgroup id|
|database\_name|VARCHAR|TAG|vgroup 所属的 database 名字|
|cluster\_id|VARCHAR|TAG|cluster id|

### taosd\_dnodes\_info 表

`taosd_dnodes_info` 记录 dnode 信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|uptime|DOUBLE||dnode uptime，单位：秒|
|cpu\_engine|DOUBLE||taosd cpu 使用率，从 `/proc/<taosd_pid>/stat` 读取|
|cpu\_system|DOUBLE||服务器 cpu 使用率，从 `/proc/stat` 读取|
|cpu\_cores|DOUBLE||服务器 cpu 核数|
|mem\_engine|DOUBLE||taosd 内存使用率，从 `/proc/<taosd_pid>/status` 读取|
|mem\_free|DOUBLE||服务器可用内存，单位 KB|
|mem\_total|DOUBLE||服务器内存总量，单位 KB|
|disk\_used|DOUBLE||data dir 挂载的磁盘使用量，单位 bytes|
|disk\_total|DOUBLE||data dir 挂载的磁盘总容量，单位 bytes|
|system\_net\_in|DOUBLE||网络吞吐率，从 `/proc/net/dev` 中读取的 received bytes。单位 byte/s|
|system\_net\_out|DOUBLE||网络吞吐率，从 `/proc/net/dev` 中读取的 transmit bytes。单位 byte/s|
|io\_read|DOUBLE||io 吞吐率，从 `/proc/<taosd_pid>/io` 中读取的 rchar 与上次数值计算之后，计算得到速度。单位 byte/s|
|io\_write|DOUBLE||io 吞吐率，从 `/proc/<taosd_pid>/io` 中读取的 wchar 与上次数值计算之后，计算得到速度。单位 byte/s|
|io\_read\_disk|DOUBLE||磁盘 io 吞吐率，从 `/proc/<taosd_pid>/io` 中读取的 read_bytes。单位 byte/s|
|io\_write\_disk|DOUBLE||磁盘 io 吞吐率，从 `/proc/<taosd_pid>/io` 中读取的 write_bytes。单位 byte/s|
|vnodes\_num|DOUBLE||dnode 上 vnodes 数量|
|masters|DOUBLE||dnode 上 master node 数量|
|has\_mnode|DOUBLE||dnode 是否包含 mnode，取值范围：包含=1,不包含=0|
|has\_qnode|DOUBLE||dnode 是否包含 qnode，取值范围：包含=1,不包含=0|
|has\_snode|DOUBLE||dnode 是否包含 snode，取值范围：包含=1,不包含=0|
|has\_bnode|DOUBLE||dnode 是否包含 bnode，取值范围：包含=1,不包含=0|
|error\_log\_count|DOUBLE||error 总数|
|info\_log\_count|DOUBLE||info 总数|
|debug\_log\_count|DOUBLE||debug 总数|
|trace\_log\_count|DOUBLE||trace 总数|
|dnode\_id|VARCHAR|TAG|dnode id|
|dnode\_ep|VARCHAR|TAG|dnode endpoint|
|cluster\_id|VARCHAR|TAG|cluster id|

### taosd\_dnodes\_status 表

`taosd_dnodes_status` 表记录 dnode 状态信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|status|DOUBLE||dnode 状态,取值范围：ready=1，offline =0|
|dnode\_id|VARCHAR|TAG|dnode id|
|dnode\_ep|VARCHAR|TAG|dnode endpoint|
|cluster\_id|VARCHAR|TAG|cluster id|

### taosd\_dnodes\_log\_dir 表

`taosd_dnodes_log_dir` 表记录 log 目录信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|avail|DOUBLE||log 目录可用空间。单位 byte|
|used|DOUBLE||log 目录已使用空间。单位 byte|
|total|DOUBLE||log 目录空间。单位 byte|
|name|VARCHAR|TAG|log 目录名，一般为 `/var/log/taos/`|
|dnode\_id|VARCHAR|TAG|dnode id|
|dnode\_ep|VARCHAR|TAG|dnode endpoint|
|cluster\_id|VARCHAR|TAG|cluster id|

### taosd\_dnodes\_data\_dir 表

`taosd_dnodes_data_dir` 表记录 data 目录信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|avail|DOUBLE||data 目录可用空间。单位 byte|
|used|DOUBLE||data 目录已使用空间。单位 byte|
|total|DOUBLE||data 目录空间。单位 byte|
|level|VARCHAR|TAG|0、1、2 多级存储级别|
|name|VARCHAR|TAG|data 目录，一般为 `/var/lib/taos`|
|dnode\_id|VARCHAR|TAG|dnode id|
|dnode\_ep|VARCHAR|TAG|dnode endpoint|
|cluster\_id|VARCHAR|TAG|cluster id|

### taosd\_mnodes\_info 表

`taosd_mnodes_info` 表记录 mnode 角色信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|role|DOUBLE||mnode 角色， 取值范围：offline = 0,follower = 100,candidate = 101,leader = 102,error = 103,learner = 104|
|mnode\_id|VARCHAR|TAG|master node id|
|mnode\_ep|VARCHAR|TAG|master node endpoint|
|cluster\_id|VARCHAR|TAG|cluster id|

### taosd\_vnodes\_role 表

`taosd_vnodes_role` 表记录虚拟节点角色信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|vnode\_role|DOUBLE||vnode 角色，取值范围：offline = 0,follower = 100,candidate = 101,leader = 102,error = 103,learner = 104|
|vgroup\_id|VARCHAR|TAG|dnode id|
|dnode\_id|VARCHAR|TAG|dnode id|
|database\_name|VARCHAR|TAG|vgroup 所属的 database 名字|
|cluster\_id|VARCHAR|TAG|cluster id|

### taosd\_sql\_req 表

`taosd_sql_req` 记录授权信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|count|DOUBLE||sql 数量|
|result|VARCHAR|TAG|sql的执行结果，取值范围：Success, Failed|
|username|VARCHAR|TAG|执行sql的user name|
|sql\_type|VARCHAR|TAG|sql类型，取值范围：inserted_rows|
|dnode\_id|VARCHAR|TAG|dnode id|
|dnode\_ep|VARCHAR|TAG|dnode endpoint|
|vgroup\_id|VARCHAR|TAG|dnode id|
|cluster\_id|VARCHAR|TAG|cluster id|

### taos\_sql\_req 表

`taos_sql_req` 记录授权信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|count|DOUBLE||sql 数量|
|result|VARCHAR|TAG|sql的执行结果，取值范围：Success, Failed|
|username|VARCHAR|TAG|执行sql的user name|
|sql\_type|VARCHAR|TAG|sql类型，取值范围：select, insert，delete|
|cluster\_id|VARCHAR|TAG|cluster id|

### taos\_slow\_sql 表

`taos_slow_sql` 记录授权信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|count|DOUBLE||sql 数量|
|result|VARCHAR|TAG|sql的执行结果，取值范围：Success, Failed|
|username|VARCHAR|TAG|执行sql的user name|
|duration|VARCHAR|TAG|sql执行耗时，取值范围：3-10s,10-100s,100-1000s,1000s-|
|cluster\_id|VARCHAR|TAG|cluster id|

### keeper\_monitor 表

`keeper_monitor` 记录 taoskeeper 监控数据。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|cpu|DOUBLE||cpu 使用率|
|mem|DOUBLE||内存使用率|
|identify|NCHAR|TAG||

### taosadapter\_restful\_http\_request\_total 表

`taosadapter_restful_http_request_total` 记录 taosadapter rest 请求信息，该表为 schemaless 方式创建的表，时间戳字段名为 `_ts`。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|\_ts|TIMESTAMP||timestamp|
|gauge|DOUBLE||监控指标值|
|client\_ip|NCHAR|TAG|client ip|
|endpoint|NCHAR|TAG|taosadpater endpoint|
|request\_method|NCHAR|TAG|request method|
|request\_uri|NCHAR|TAG|request uri|
|status\_code|NCHAR|TAG|status code|

### taosadapter\_restful\_http\_request\_fail 表

`taosadapter_restful_http_request_fail` 记录 taosadapter rest 请求失败信息，该表为 schemaless 方式创建的表，时间戳字段名为 `_ts`。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|\_ts|TIMESTAMP||timestamp|
|gauge|DOUBLE||监控指标值|
|client\_ip|NCHAR|TAG|client ip|
|endpoint|NCHAR|TAG|taosadpater endpoint|
|request\_method|NCHAR|TAG|request method|
|request\_uri|NCHAR|TAG|request uri|
|status\_code|NCHAR|TAG|status code|

### taosadapter\_restful\_http\_request\_in\_flight 表

`taosadapter_restful_http_request_in_flight` 记录 taosadapter rest 实时请求信息，该表为 schemaless 方式创建的表，时间戳字段名为 `_ts`。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|\_ts|TIMESTAMP||timestamp|
|gauge|DOUBLE||监控指标值|
|endpoint|NCHAR|TAG|taosadpater endpoint|

### taosadapter\_restful\_http\_request\_summary\_milliseconds 表

`taosadapter_restful_http_request_summary_milliseconds` 记录 taosadapter rest 请求汇总信息，该表为 schemaless 方式创建的表，时间戳字段名为 `_ts`。

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

### taosadapter\_system\_mem\_percent 表

`taosadapter_system_mem_percent` 表记录 taosadapter 内存使用情况，该表为 schemaless 方式创建的表，时间戳字段名为 `_ts`。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|\_ts|TIMESTAMP||timestamp|
|gauge|DOUBLE||监控指标值|
|endpoint|NCHAR|TAG|taosadpater endpoint|

### taosadapter\_system\_cpu\_percent 表

`taosadapter_system_cpu_percent` 表记录 taosadapter cpu 使用情况，该表为 schemaless 方式创建的表，时间戳字段名为 `_ts`。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|\_ts|TIMESTAMP||timestamp|
|gauge|DOUBLE||监控指标值|
|endpoint|NCHAR|TAG|taosadpater endpoint|
