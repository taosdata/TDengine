---
title: "添加模型服务"
sidebar_label: "添加模型服务"
---

TDgpt 默认已经内置了 Time-MoE 模型的支持功能， 执行 `show anodes full`，可以看到 Time-MoE 的预测服务 `timemoe-fc`，现在只适配了预测服务，所以其后增加了后缀名 fc。
```shell
taos> show anodes full;
     id      |            type            |              algo              |
============================================================================
           1 | anomaly-detection          | grubbs                         |
           1 | anomaly-detection          | lof                            |
           1 | anomaly-detection          | shesd                          |
           1 | anomaly-detection          | ksigma                         |
           1 | anomaly-detection          | iqr                            |
           1 | anomaly-detection          | sample_ad_model                |
           1 | forecast                   | arima                          |
           1 | forecast                   | holtwinters                    |
           1 | forecast                   | tdtsfm_1                       |
           1 | forecast                   | timemoe-fc                     |
```

正确调用 Time-MoE 模型的时间序列数据预测能力，需要您在本地或云端已经部署完成 Time-MoE 服务（需要执行 `./taosanode/lib/taosanalytics/time-moe.py` 的脚本部署 Time-MoE
服务。具体过程请参见 [部署 Time-MoE 服务](./02-deploy-timemoe)）。

修改 `/etc/taos/taosanode.ini` 配置文件中如下部分：

```ini
[tsfm-service]
timemoe-fc = http://192.168.2.90:5001/ds_predict
```

设置正确的 IP 和端口，以及服务地址。

然后重启 taosnode 服务，并更新服务端算法缓存列表 `update all anodes`，之后即可通过 SQL 语句调用 Time-MoE 的时间序列数据预测服务。

```sql
SELECT FORECAST(i32, 'algo=timemoe-fc') 
FROM foo;
```
