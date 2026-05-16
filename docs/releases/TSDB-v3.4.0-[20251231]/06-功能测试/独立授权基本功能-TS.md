# 独立授权基本功能-TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2025-11-5 | 0.1 | 彭荣坤 | 张天毅 | 创建 |
| 2025-11-13 | 2025-11-13 | 1.0 | 关胜亮 | 调整目录结构及措辞 |

## 2. 测试目标

测试独立授权的基本功能是否满足需求

## 3. 相关资料

1. [独立授权服务 RS](https://taosdata.feishu.cn/wiki/PGn6wzFwNiZbg3kVMmscaGCDn9d)
2. [独立授权服务 - FS](https://taosdata.feishu.cn/wiki/Jw8IwVcVgiTilakmgcJcSHORn7e)
3. [独立授权服务操作手册](https://taosdata.feishu.cn/wiki/JtsfwvB4Oi178vkQ4rJc68Y3n2d)

## 4. 测试环境

本地环境，macos
配置auth client参数：
1. authReq 1
2. authReqUrl 127.0.0.1:6040
3. authReqInterval      60

## 5. 功能测试

创建两个集群的授权信息：
```sql
create table t1 using grantserver tags(   "1488799824521785139",   true,   "service:2025-12-31;expireDays:2025-12-31;limitTimeSeries:un;limitCpuCores:un;limitDnodes:un;limitVnodes:un;limitStorageSize:un;stream:2025-12-31,un;subscription:2025-12-31,un;view:2025-12-31,un;audit:2025-12-31;storage:2025-12-31;dataSync:2025-12-31;backupRestore:2025-12-31;sharedStorage:2025-12-31;ActiveActive:2025-12-31;DualReplica:2025-12-31;dbEncrypt:2025-12-31;tdgpt:2025-12-31,un;mount:2025-12-31,un;opc_da:2025-12-31,100,1000;opc_ua:2025-12-31,100,1000;pi:2025-12-31,100,1000;kafka:2025-12-31,100,1000;influxdb:2025-12-31,100,1000;mqtt:2025-12-31,100,1000;avevahistorian:2025-12-31,100,1000;opentsdb:2025-12-31,100,1000;td2.6:2025-12-31,100,1000;td3.0:2025-12-31,100,1000;mysql:2025-12-31,100,1000;postgres:2025-12-31,100,1000;oracle:2025-12-31,100,1000;mssql:2025-12-31,100,1000;mongodb:2025-12-31,100,1000;csv:2025-12-31,100,1000;sparkplugb:2025-12-31,100,1000;orc:2025-12-31,100,1000;kinghist:2025-12-31,100,1000;idmpExpireDays:2025-12-31;idmpLimitTsAttributes:un;idmpLimitNonTsAttributes:un;idmpLimitElements:un;idmpLimitServers:un;idmpLimitCpuCores:un;idmpLimitUsers:un;idmpVersionCtrl:2025-12-31;idmpDataForecast:2025-12-31;idmpDataDetect:2025-12-31;idmpDataQuality:2025-12-31;idmpAiChatGen:2025-12-31;" );
create table t2 using grantserver tags(   "1586283241990231131",   true,   "service:2025-12-31;expireDays:2025-12-31;limitTimeSeries:un;limitCpuCores:un;limitDnodes:un;limitVnodes:un;limitStorageSize:un;stream:2025-12-31,un;subscription:2025-12-31,un;view:2025-12-31,un;audit:2025-12-31;storage:2025-12-31;dataSync:2025-12-31;backupRestore:2025-12-31;sharedStorage:2025-12-31;ActiveActive:2025-12-31;DualReplica:2025-12-31;dbEncrypt:2025-12-31;tdgpt:2025-12-31,un;mount:2025-12-31,un;opc_da:2025-12-31,100,1000;opc_ua:2025-12-31,100,1000;pi:2025-12-31,100,1000;kafka:2025-12-31,100,1000;influxdb:2025-12-31,100,1000;mqtt:2025-12-31,100,1000;avevahistorian:2025-12-31,100,1000;opentsdb:2025-12-31,100,1000;td2.6:2025-12-31,100,1000;td3.0:2025-12-31,100,1000;mysql:2025-12-31,100,1000;postgres:2025-12-31,100,1000;oracle:2025-12-31,100,1000;mssql:2025-12-31,100,1000;mongodb:2025-12-31,100,1000;csv:2025-12-31,100,1000;sparkplugb:2025-12-31,100,1000;orc:2025-12-31,100,1000;kinghist:2025-12-31,100,1000;idmpExpireDays:2025-12-31;idmpLimitTsAttributes:un;idmpLimitNonTsAttributes:un;idmpLimitElements:un;idmpLimitServers:un;idmpLimitCpuCores:un;idmpLimitUsers:un;idmpVersionCtrl:2025-12-31;idmpDataForecast:2025-12-31;idmpDataDetect:2025-12-31;idmpDataQuality:2025-12-31;idmpAiChatGen:2025-12-31;" );
```

### 5.1 授权基本功能

1、测试给两个相互独立的集群授权，结果成功给来个那个集群授权一个月
```sql
-- 集群1
taos> show cluster\G;
*************************** 1.row ***************************
         id: 1586283241990231131
       name: 85f3cd00-9005-4821-997b-e58552b6166cd01
     uptime: 4200
create_time: 2025-11-06 17:49:49.475
    version: official
expire_time: 2025-12-06 08:00:00.000
Query OK, 1 row(s) in set (0.008243s)

taos> show grants\G;
*************************** 1.row ***************************
     version: TDengine TSDB-Enterprise official
 expire_time: 2025-12-06 08:00:00
service_time: 2025-12-06 08:00:00
     expired: false
       state: granted
  timeseries: 0/unlimited
      dnodes: 1/unlimited
   cpu_cores: 8/unlimited
      vnodes: 0/unlimited
storage_size: 0.000/unlimited
Query OK, 1 row(s) in set (0.007728s)

-- 集群2
taos> show cluster\G;
*************************** 1.row ***************************
         id: 1488799824521785139
       name: 9ba54f8c-aa44-43e7-9983-1342be6a24bb027
     uptime: 900
create_time: 2025-11-06 19:26:39.940
    version: official
expire_time: 2025-12-06 08:00:00.000
Query OK, 1 row(s) in set (0.002406s)

taos> show grants\G;
*************************** 1.row ***************************
     version: TDengine TSDB-Enterprise official
 expire_time: 2025-12-06 08:00:00
service_time: 2025-12-06 08:00:00
     expired: false
       state: granted
  timeseries: 0/unlimited
      dnodes: 1/unlimited
   cpu_cores: 8/unlimited
      vnodes: 0/unlimited
storage_size: 0.000/unlimited
Query OK, 1 row(s) in set (0.010698s)
```

同时可以在auth_server查询到两个集群的心跳携带的授权信息
```bash
taos> select * from grantserver where cluster_id=1586283241990231131 order by ts desc limit 5;
           ts            |         auth_time          |  auth_status   |  auth_code  |           auth_usage           | auth_updated |          machine_code          |              fqdn              |            first_ep            |       create_time       |         boot_time         |           cluster_id           | enables |           auth_quota           |
===========================================================================================================================================================================================================================================================================================================================================================================
 2025-11-06 19:51:02.878 | 2025-12-06 08:00:00        | granted        |           0 | timeseries:0/un,dnodes:1/un... | false        | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:pengrongkundeMacBook-Pro.... | pengrongkundeMacBook-Pro.lo... | 2025-11-06 17:49:49.475 |                      4200 | 1586283241990231131            | true    | service:2025-12-31;expireDa... |
 2025-11-06 19:50:00.534 | 2025-12-06 08:00:00        | granted        |           0 | timeseries:0/un,dnodes:1/un... | false        | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:pengrongkundeMacBook-Pro.... | pengrongkundeMacBook-Pro.lo... | 2025-11-06 17:49:49.475 |                      4200 | 1586283241990231131            | true    | service:2025-12-31;expireDa... |
 2025-11-06 19:48:58.385 | 2025-12-06 08:00:00        | granted        |           0 | timeseries:0/un,dnodes:1/un... | false        | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:pengrongkundeMacBook-Pro.... | pengrongkundeMacBook-Pro.lo... | 2025-11-06 17:49:49.475 |                      4200 | 1586283241990231131            | true    | service:2025-12-31;expireDa... |
 2025-11-06 19:47:56.163 | 2025-12-06 08:00:00        | granted        |           0 | timeseries:0/un,dnodes:1/un... | false        | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:pengrongkundeMacBook-Pro.... | pengrongkundeMacBook-Pro.lo... | 2025-11-06 17:49:49.475 |                      4200 | 1586283241990231131            | true    | service:2025-12-31;expireDa... |
 2025-11-06 19:46:54.070 | 2025-12-06 08:00:00        | granted        |           0 | timeseries:0/un,dnodes:1/un... | false        | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:pengrongkundeMacBook-Pro.... | pengrongkundeMacBook-Pro.lo... | 2025-11-06 17:49:49.475 |                      4200 | 1586283241990231131            | true    | service:2025-12-31;expireDa... |
Query OK, 5 row(s) in set (0.014772s)

taos> select * from grantserver where cluster_id=1488799824521785139 order by ts desc limit 5;
           ts            |         auth_time          |  auth_status   |  auth_code  |           auth_usage           | auth_updated |          machine_code          |              fqdn              |            first_ep            |       create_time       |         boot_time         |           cluster_id           | enables |           auth_quota           |
===========================================================================================================================================================================================================================================================================================================================================================================
 2025-11-06 19:51:06.056 | 2025-12-06 08:00:00        | granted        |           0 | timeseries:0/un,dnodes:1/un... | false        | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:127.0.0.1:6050               | 127.0.0.1:6050                 | 2025-11-06 19:26:39.940 |                      1200 | 1488799824521785139            | true    | service:2025-12-31;expireDa... |
 2025-11-06 19:50:03.741 | 2025-12-06 08:00:00        | granted        |           0 | timeseries:0/un,dnodes:1/un... | false        | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:127.0.0.1:6050               | 127.0.0.1:6050                 | 2025-11-06 19:26:39.940 |                       900 | 1488799824521785139            | true    | service:2025-12-31;expireDa... |
 2025-11-06 19:49:01.546 | 2025-12-06 08:00:00        | granted        |           0 | timeseries:0/un,dnodes:1/un... | false        | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:127.0.0.1:6050               | 127.0.0.1:6050                 | 2025-11-06 19:26:39.940 |                       900 | 1488799824521785139            | true    | service:2025-12-31;expireDa... |
 2025-11-06 19:47:59.304 | 2025-12-06 08:00:00        | granted        |           0 | timeseries:0/un,dnodes:1/un... | false        | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:127.0.0.1:6050               | 127.0.0.1:6050                 | 2025-11-06 19:26:39.940 |                       900 | 1488799824521785139            | true    | service:2025-12-31;expireDa... |
 2025-11-06 19:46:57.065 | 2025-12-06 08:00:00        | granted        |           0 | timeseries:0/un,dnodes:1/un... | false        | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:127.0.0.1:6050               | 127.0.0.1:6050                 | 2025-11-06 19:26:39.940 |                       900 | 1488799824521785139            | true    | service:2025-12-31;expireDa... |
Query OK, 5 row(s) in set (0.014407s)
```

### 5.2 取消续期功能

新建一个过期的集群，并且在auth_server建立一个enables标签为false的表
```sql
-- auth server
create table t4 using grantserver tags(   "632930818046155171",   false,   "service:2025-12-31;expireDays:2025-12-31;limitTimeSeries:un;limitCpuCores:un;limitDnodes:un;limitVnodes:un;limitStorageSize:un;stream:2025-12-31,un;subscription:2025-12-31,un;view:2025-12-31,un;audit:2025-12-31;storage:2025-12-31;dataSync:2025-12-31;backupRestore:2025-12-31;sharedStorage:2025-12-31;ActiveActive:2025-12-31;DualReplica:2025-12-31;dbEncrypt:2025-12-31;tdgpt:2025-12-31,un;mount:2025-12-31,un;opc_da:2025-12-31,100,1000;opc_ua:2025-12-31,100,1000;pi:2025-12-31,100,1000;kafka:2025-12-31,100,1000;influxdb:2025-12-31,100,1000;mqtt:2025-12-31,100,1000;avevahistorian:2025-12-31,100,1000;opentsdb:2025-12-31,100,1000;td2.6:2025-12-31,100,1000;td3.0:2025-12-31,100,1000;mysql:2025-12-31,100,1000;postgres:2025-12-31,100,1000;oracle:2025-12-31,100,1000;mssql:2025-12-31,100,1000;mongodb:2025-12-31,100,1000;csv:2025-12-31,100,1000;sparkplugb:2025-12-31,100,1000;orc:2025-12-31,100,1000;kinghist:2025-12-31,100,1000;idmpExpireDays:2025-12-31;idmpLimitTsAttributes:un;idmpLimitNonTsAttributes:un;idmpLimitElements:un;idmpLimitServers:un;idmpLimitCpuCores:un;idmpLimitUsers:un;idmpVersionCtrl:2025-12-31;idmpDataForecast:2025-12-31;idmpDataDetect:2025-12-31;idmpDataQuality:2025-12-31;idmpAiChatGen:2025-12-31;" );

-- auth client
taos> show grants\G;
*************************** 1.row ***************************
     version: TDengine TSDB-Enterprise trial
 expire_time: 2025-11-07 09:36:57
service_time: 2025-11-07 09:36:53
     expired: true
       state: ungranted
  timeseries: 0/unlimited
      dnodes: 1/unlimited
   cpu_cores: 8/unlimited
      vnodes: 0/unlimited
storage_size: 0.000/unlimited
Query OK, 1 row(s) in set (0.008375s)
```

查看心跳信息和授权信息，未授权符合预期
```sql
-- 日志报错：
11/07 10:05:53.419702 6201536512 E MND ERROR msg:0x13e811d78, failed to process since Cluster has been disabled by the auth server, app:0x13b014a00 type:auth-check-rsp, QID:0x0:0x50ca25c1025002ad

-- auth server查询心跳信息
taos> select * from t4;
           ts            |         auth_time          |  auth_status   |  auth_code  |           auth_usage           | auth_updated |          machine_code          |              fqdn              |            first_ep            |       create_time       |         boot_time         |
===============================================================================================================================================================================================================================================================================================
 2025-11-07 09:55:01.521 | 2025-11-07 09:36:57        | ungranted      | -2147481547 | timeseries:0/un,dnodes:1/un... | false        | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:127.0.0.1:6060               | 127.0.0.1:6060                 | 2025-11-07 09:36:53.708 |                       600 |
 2025-11-07 09:55:11.861 | 2025-11-07 09:36:57        | ungranted      | -2147481547 | timeseries:0/un,dnodes:1/un... | false        | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:127.0.0.1:6060               | 127.0.0.1:6060                 | 2025-11-07 09:36:53.708 |                       600 |
 2025-11-07 09:55:22.217 | 2025-11-07 09:36:57        | ungranted      | -2147481547 | timeseries:0/un,dnodes:1/un... | false        | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:127.0.0.1:6060               | 127.0.0.1:6060                 | 2025-11-07 09:36:53.708 |                       600 |
 2025-11-07 09:55:32.586 | 2025-11-07 09:36:57        | ungranted      | -2147481547 | timeseries:0/un,dnodes:1/un... | false        | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:127.0.0.1:6060               | 127.0.0.1:6060                 | 2025-11-07 09:36:53.708 |                       600 |
 2025-11-07 09:55:53.290 | 2025-11-07 09:36:57        | ungranted      | -2147481547 | timeseries:0/un,dnodes:1/un... | false        | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:127.0.0.1:6060               | 127.0.0.1:6060                 | 2025-11-07 09:36:53.708 |                       600 |
 2025-11-07 09:56:14.157 | 2025-11-07 09:36:57        | ungranted      | -2147481547 | timeseries:0/un,dnodes:1/un... | false        | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:127.0.0.1:6060               | 127.0.0.1:6060                 | 2025-11-07 09:36:53.708 |                       600 |
 2025-11-07 10:01:17.385 | 2025-11-07 09:36:57        | ungranted      | -2147481547 | timeseries:0/un,dnodes:1/un... | false        | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:127.0.0.1:6060               | 127.0.0.1:6060                 | 2025-11-07 09:36:53.708 |                       600 |
```


### 5.3 更新授权用量功能

将几个basic用量从un改为10，之后可以看到cluster的grants信息变了，测试成功
```sql
alter table auth.t2 set tag ·auth_quota='service:2025-12-31;expireDays:2025-12-31;limitTimeSeries:10;limitCpuCores:10;limitDnodes:10;limitVnodes:10;limitStorageSize:10;stream:2025-12-31,un;subscription:2025-12-31,un;view:2025-12-31,un;audit:2025-12-31;storage:2025-12-31;dataSync:2025-12-31;backupRestore:2025-12-31;sharedStorage:2025-12-31;ActiveActive:2025-12-31;DualReplica:2025-12-31;dbEncrypt:2025-12-31;tdgpt:2025-12-31,un;mount:2025-12-31,un;opc_da:2025-12-31,100,1000;opc_ua:2025-12-31,100,1000;pi:2025-12-31,100,1000;kafka:2025-12-31,100,1000;influxdb:2025-12-31,100,1000;mqtt:2025-12-31,100,1000;avevahistorian:2025-12-31,100,1000;opentsdb:2025-12-31,100,1000;td2.6:2025-12-31,100,1000;td3.0:2025-12-31,100,1000;mysql:2025-12-31,100,1000;postgres:2025-12-31,100,1000;oracle:2025-12-31,100,1000;mssql:2025-12-31,100,1000;mongodb:2025-12-31,100,1000;csv:2025-12-31,100,1000;sparkplugb:2025-12-31,100,1000;orc:2025-12-31,100,1000;kinghist:2025-12-31,100,1000;idmpExpireDays:2025-12-31;idmpLimitTsAttributes:un;idmpLimitNonTsAttributes:un;idmpLimitElements:un;idmpLimitServers:un;idmpLimitCpuCores:un;idmpLimitUsers:un;idmpVersionCtrl:2025-12-31;idmpDataForecast:2025-12-31;idmpDataDetect:2025-12-31;idmpDataQuality:2025-12-31;idmpAiChatGen:2025-12-31;" ';

taos> show grants\G;
*************************** 1.row ***************************
     version: TDengine TSDB-Enterprise official
 expire_time: 2025-12-06 08:00:00
service_time: 2025-12-06 08:00:00
     expired: false
       state: granted
  timeseries: 0/10
      dnodes: 1/10
   cpu_cores: 8/10
      vnodes: 0/10
storage_size: 0.000/10
Query OK, 1 row(s) in set (0.007950s)
```

### 5.4 非过期状态下的自动续期功能

先将auth_client的expireday强行改成now+10天（在15天自动续期的阈值内，但是没有过期），授权成功
```sql
taos> show grants\G;
*************************** 1.row ***************************
     version: TDengine TSDB-Enterprise trial
 expire_time: 2025-11-16 21:57:40
service_time: 2025-11-06 21:57:38
     expired: false
       state: ungranted
  timeseries: 0/unlimited
      dnodes: 1/unlimited
   cpu_cores: 8/unlimited
      vnodes: 0/unlimited
storage_size: 0.000/unlimited

-- 等待一个心跳周期

taos> show grants\G;
*************************** 1.row ***************************
     version: TDengine TSDB-Enterprise official
 expire_time: 2025-12-06 08:00:00
service_time: 2025-12-06 08:00:00
     expired: false
       state: granted
  timeseries: 0/unlimited
      dnodes: 1/unlimited
   cpu_cores: 8/unlimited
      vnodes: 0/unlimited
storage_size: 0.000/unlimited

-- 查看心跳信息，在2025-11-06 22:01:50.137实现授权
taos> select * from t3;
           ts            |         auth_time          |  auth_status   |  auth_code  |           auth_usage           | auth_updated |          machine_code          |              fqdn              |            first_ep            |       create_time       |         boot_time         |
===============================================================================================================================================================================================================================================================================================
 2025-11-06 22:01:19.151 | 2025-11-16 21:57:40        | ungranted      |           0 | timeseries:0/un,dnodes:1/un... | true         | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:127.0.0.1:6060               | 127.0.0.1:6060                 | 2025-11-06 21:57:38.385 |                         0 |
 2025-11-06 22:01:24.314 | 2025-11-16 21:57:40        | ungranted      |           0 | timeseries:0/un,dnodes:1/un... | true         | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:127.0.0.1:6060               | 127.0.0.1:6060                 | 2025-11-06 21:57:38.385 |                         0 |
 2025-11-06 22:01:29.477 | 2025-11-16 21:57:40        | ungranted      |           0 | timeseries:0/un,dnodes:1/un... | true         | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:127.0.0.1:6060               | 127.0.0.1:6060                 | 2025-11-06 21:57:38.385 |                         0 |
 2025-11-06 22:01:34.647 | 2025-11-16 21:57:40        | ungranted      |           0 | timeseries:0/un,dnodes:1/un... | true         | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:127.0.0.1:6060               | 127.0.0.1:6060                 | 2025-11-06 21:57:38.385 |                         0 |
 2025-11-06 22:01:39.818 | 2025-11-16 21:57:40        | ungranted      |           0 | timeseries:0/un,dnodes:1/un... | true         | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:127.0.0.1:6060               | 127.0.0.1:6060                 | 2025-11-06 21:57:38.385 |                         0 |
 2025-11-06 22:01:44.970 | 2025-11-16 21:57:40        | ungranted      |           0 | timeseries:0/un,dnodes:1/un... | true         | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:127.0.0.1:6060               | 127.0.0.1:6060                 | 2025-11-06 21:57:38.385 |                         0 |
 2025-11-06 22:01:50.137 | 2025-11-16 21:57:40        | ungranted      |           0 | timeseries:0/un,dnodes:1/un... | true         | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:127.0.0.1:6060               | 127.0.0.1:6060                 | 2025-11-06 21:57:38.385 |                         0 |
 2025-11-06 22:02:50.635 | 2025-12-06 08:00:00        | granted        |           0 | timeseries:0/un,dnodes:1/un... | false        | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:127.0.0.1:6060               | 127.0.0.1:6060                 | 2025-11-06 21:57:38.385 |                         0 |
 2025-11-06 22:03:52.908 | 2025-12-06 08:00:00        | granted        |           0 | timeseries:0/un,dnodes:1/un... | false        | AQGDaR/mxGqmHeDZPwLNjRmX       | 1:127.0.0.1:6060               | 127.0.0.1:6060                 | 2025-11-06 21:57:38.385 |                         0 |
Query OK, 9 row(s) in set (0.009773s)
```


## 6. 异常情况测试

### 6.1 授权用量限制超出的情况

1. auth_quota超出了auth_server的授权
2. auth_quota内部授权项超出了basicExpireDay

### 6.2 测试auth_server初始化未完成的情况

1. 未建库建表
2. 未创建user
3. 需要被授权的子表标签cluster_id不存在
4. taosadapter未启动

### 6.3 测试auth_server重启的情况

通过

### 6.4 测试auth_quota未按标准填写的情况

通过

## 7. 安全性测试

测试了加密通信的场景，通过
