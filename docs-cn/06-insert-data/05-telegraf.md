---
sidebar_label: Telegraf 直接写入
---

# Telegraf 直接写入(通过 taosAdapter)

安装 Telegraf 请参考[官方文档](https://portal.influxdata.com/downloads/)。

TDengine 新版本（2.3.0.0+）包含一个 taosAdapter 独立程序，负责接收包括 Telegraf 的多种应用的数据写入。

配置方法，在 /etc/telegraf/telegraf.conf 增加如下文字，其中 database name 请填写希望在 TDengine 保存 Telegraf 数据的数据库名，TDengine server/cluster host、username 和 password 填写 TDengine 实际值：

```
[[outputs.http]]
  url = "http://<TDengine server/cluster host>:6041/influxdb/v1/write?db=<database name>"
  method = "POST"
  timeout = "5s"
  username = "<TDengine's username>"
  password = "<TDengine's password>"
  data_format = "influx"
  influx_max_line_bytes = 250
```

然后重启 telegraf：

```
sudo systemctl start telegraf
```

即可在 TDengine 中查询 metrics 数据库中 Telegraf 写入的数据。

taosAdapter 相关配置参数请参考 taosadapter --help 命令输出以及相关文档。
