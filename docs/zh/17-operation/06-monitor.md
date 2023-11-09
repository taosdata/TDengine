---
title: 系统监控
description: 监控 TDengine 的运行状态
---

TDengine 通过 [taosKeeper](/reference/taosKeeper/) 将服务器的 CPU、内存、硬盘空间、带宽、请求数、磁盘读写速度等信息定时写入指定数据库。TDengine 还将重要的系统操作（比如登录、创建、删除数据库等）日志以及各种错误报警信息进行记录。系统管理员可以从 CLI 直接查看这个数据库，也可以在 WEB 通过图形化界面查看这些监测信息。

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

TDinsight dashboard 数据来源于 log 库（存放监控数据的默认db，可以在 taoskeeper 配置文件中修改，具体参考 [taoskeeper 文档](/reference/taosKeeper)）。taoskeeper 启动后会自动创建 log 库，并将监控数据写入到该数据库中。

### cluster\_info 表

`cluster_info` 表记录集群信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|first\_ep|VARCHAR||集群 first ep|
|first\_ep\_dnode\_id|INT||集群 first ep 的 dnode id|
|version|VARCHAR||tdengine version。例如：3.0.4.0|
|master\_uptime|FLOAT||当前 master 节点的uptime。单位：天|
|monitor\_interval|INT||monitor interval。单位：秒|
|dbs\_total|INT||database 总数|
|tbs\_total|BIGINT||当前集群 table 总数|
|stbs\_total|INT||当前集群 stable 总数|
|dnodes\_total|INT||当前集群 dnode 总数|
|dnodes\_alive|INT||当前集群 dnode 存活总数|
|mnodes\_total|INT||当前集群 mnode 总数|
|mnodes\_alive|INT||当前集群 mnode 存活总数|
|vgroups\_total|INT||当前集群 vgroup 总数|
|vgroups\_alive|INT||当前集群 vgroup 存活总数|
|vnodes\_total|INT||当前集群 vnode 总数|
|vnodes\_alive|INT||当前集群 vnode 存活总数|
|connections\_total|INT||当前集群连接总数|
|topics\_total|INT||当前集群 topic 总数|
|streams\_total|INT||当前集群 stream 总数|
|protocol|INT||协议版本，目前为 1|
|cluster\_id|NCHAR|TAG|cluster id|

### d\_info 表

`d_info` 表记录 dnode 状态信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|status|VARCHAR||dnode 状态|
|dnode\_ep|NCHAR|TAG|dnode endpoint|
|cluster\_id|NCHAR|TAG|cluster id|

### m\_info 表

`m_info` 表记录 mnode 角色信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|role|VARCHAR||mnode 角色， leader 或 follower|
|mnode\_id|INT|TAG|master node id|
|mnode\_ep|NCHAR|TAG|master node endpoint|
|cluster\_id|NCHAR|TAG|cluster id|

### dnodes\_info 表

`dnodes_info` 记录 dnode 信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|uptime|FLOAT||dnode uptime，单位：天|
|cpu\_engine|FLOAT||taosd cpu 使用率，从 `/proc/<taosd_pid>/stat` 读取|
|cpu\_system|FLOAT||服务器 cpu 使用率，从 `/proc/stat` 读取|
|cpu\_cores|FLOAT||服务器 cpu 核数|
|mem\_engine|INT||taosd 内存使用率，从 `/proc/<taosd_pid>/status` 读取|
|mem\_system|INT||服务器可用内存，单位 KB|
|mem\_total|INT||服务器内存总量，单位 KB|
|disk\_engine|INT||单位 bytes|
|disk\_used|BIGINT||data dir 挂载的磁盘使用量，单位 bytes|
|disk\_total|BIGINT||data dir 挂载的磁盘总容量，单位 bytes|
|net\_in|FLOAT||网络吞吐率，从 `/proc/net/dev` 中读取的 received bytes。单位 byte/s|
|net\_out|FLOAT||网络吞吐率，从 `/proc/net/dev` 中读取的 transmit bytes。单位 byte/s|
|io\_read|FLOAT||io 吞吐率，从 `/proc/<taosd_pid>/io` 中读取的 rchar 与上次数值计算之后，计算得到速度。单位 byte/s|
|io\_write|FLOAT||io 吞吐率，从 `/proc/<taosd_pid>/io` 中读取的 wchar 与上次数值计算之后，计算得到速度。单位 byte/s|
|io\_read\_disk|FLOAT||磁盘 io 吞吐率，从 `/proc/<taosd_pid>/io` 中读取的 read_bytes。单位 byte/s|
|io\_write\_disk|FLOAT||磁盘 io 吞吐率，从 `/proc/<taosd_pid>/io` 中读取的 write_bytes。单位 byte/s|
|req\_select|INT||两个间隔内发生的查询请求数目|
|req\_select\_rate|FLOAT||两个间隔内的查询请求速度 = `req_select / monitorInterval`|
|req\_insert|INT||两个间隔内发生的写入请求，包含的单条数据数目|
|req\_insert\_success|INT||两个间隔内发生的处理成功的写入请求，包含的单条数据数目|
|req\_insert\_rate|FLOAT||两个间隔内的单条数据写入速度 = `req_insert / monitorInterval`|
|req\_insert\_batch|INT||两个间隔内发生的写入请求数目|
|req\_insert\_batch\_success|INT||两个间隔内发生的成功的批量写入请求数目|
|req\_insert\_batch\_rate|FLOAT||两个间隔内的写入请求数目的速度 = `req_insert_batch / monitorInterval`|
|errors|INT||两个间隔内的出错的写入请求的数目|
|vnodes\_num|INT||dnode 上 vnodes 数量|
|masters|INT||dnode 上 master node 数量|
|has\_mnode|INT||dnode 是否包含 mnode|
|has\_qnode|INT||dnode 是否包含 qnode|
|has\_snode|INT||dnode 是否包含 snode|
|has\_bnode|INT||dnode 是否包含 bnode|
|dnode\_id|INT|TAG|dnode id|
|dnode\_ep|NCHAR|TAG|dnode endpoint|
|cluster\_id|NCHAR|TAG|cluster id|

### data\_dir 表

`data_dir` 表记录 data 目录信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|name|NCHAR||data 目录，一般为 `/var/lib/taos`|
|level|INT||0、1、2 多级存储级别|
|avail|BIGINT||data 目录可用空间。单位 byte|
|used|BIGINT||data 目录已使用空间。单位 byte|
|total|BIGINT||data 目录空间。单位 byte|
|dnode\_id|INT|TAG|dnode id|
|dnode\_ep|NCHAR|TAG|dnode endpoint|
|cluster\_id|NCHAR|TAG|cluster id|

### log\_dir 表

`log_dir` 表记录 log 目录信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|name|NCHAR||log 目录名，一般为 `/var/log/taos/`|
|avail|BIGINT||log 目录可用空间。单位 byte|
|used|BIGINT||log 目录已使用空间。单位 byte|
|total|BIGINT||log 目录空间。单位 byte|
|dnode\_id|INT|TAG|dnode id|
|dnode\_ep|NCHAR|TAG|dnode endpoint|
|cluster\_id|NCHAR|TAG|cluster id|

### temp\_dir 表

`temp_dir` 表记录 temp 目录信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|name|NCHAR||temp 目录名，一般为 `/tmp/`|
|avail|BIGINT||temp 目录可用空间。单位 byte|
|used|BIGINT||temp 目录已使用空间。单位 byte|
|total|BIGINT||temp 目录空间。单位 byte|
|dnode\_id|INT|TAG|dnode id|
|dnode\_ep|NCHAR|TAG|dnode endpoint|
|cluster\_id|NCHAR|TAG|cluster id|

### vgroups\_info 表

`vgroups_info` 表记录虚拟节点组信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|vgroup\_id|INT||vgroup id|
|database\_name|VARCHAR||vgroup 所属的 database 名字|
|tables\_num|BIGINT||vgroup 中 table 数量|
|status|VARCHAR||vgroup 状态|
|dnode\_id|INT|TAG|dnode id|
|dnode\_ep|NCHAR|TAG|dnode endpoint|
|cluster\_id|NCHAR|TAG|cluster id|

### vnodes\_role 表

`vnodes_role` 表记录虚拟节点角色信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|vnode\_role|VARCHAR||vnode 角色，leader 或 follower|
|dnode\_id|INT|TAG|dnode id|
|dnode\_ep|NCHAR|TAG|dnode endpoint|
|cluster\_id|NCHAR|TAG|cluster id|

### log\_summary 表

`log_summary` 记录日志统计信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|error|INT||error 总数|
|info|INT||info 总数|
|debug|INT||debug 总数|
|trace|INT||trace 总数|
|dnode\_id|INT|TAG|dnode id|
|dnode\_ep|NCHAR|TAG|dnode endpoint|
|cluster\_id|NCHAR|TAG|cluster id|

### grants\_info 表

`grants_info` 记录授权信息。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|expire\_time|BIGINT||认证过期时间，企业版有效，社区版为 bigint 最大值|
|timeseries\_used|BIGINT||已用测点数|
|timeseries\_total|BIGINT||总测点数，开源版本为 bigint 最大值|
|dnode\_id|INT|TAG|dnode id|
|dnode\_ep|NCHAR|TAG|dnode endpoint|
|cluster\_id|NCHAR|TAG|cluster id|

### keeper\_monitor 表

`keeper_monitor` 记录 taoskeeper 监控数据。

|field|type|is\_tag|comment|
|:----|:---|:-----|:------|
|ts|TIMESTAMP||timestamp|
|cpu|FLOAT||cpu 使用率|
|mem|FLOAT||内存使用率|
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
