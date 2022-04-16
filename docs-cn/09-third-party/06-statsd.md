---
sidebar_label: StatsD
title: StatsD 直接写入
---

安装 StatsD
请参考[官方文档](https://github.com/statsd/statsd)。

TDengine 新版本（2.4.0.0+）包含一个 taosAdapter 独立程序，负责接收包括 StatsD 的多种应用的数据写入。

在 `config.js` 文件中增加如下内容后启动 StatsD，其中 host 和 port 请填写 TDengine 和 taosAdapter 配置的实际值：

```
backends 部分添加 "./backends/repeater"
repeater 部分添加 { host:'<TDengine server/cluster host>', port: <port for StatsD>}
```

示例配置文件：

```
{
port: 8125
, backends: ["./backends/repeater"]
, repeater: [{ host: '127.0.0.1', port: 6044}]
}
```

taosAdapter 相关配置参数请参考 `taosadapter --help` 命令输出以及相关文档。

