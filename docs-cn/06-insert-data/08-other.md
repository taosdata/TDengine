# 其它写入方式

## icinga2 直接写入(通过 taosAdapter)

- 参考链接 `https://icinga.com/docs/icinga-2/latest/doc/14-features/#opentsdb-writer` 使能 opentsdb-writer
- 使能 taosAdapter 配置项 opentsdb_telnet.enable
- 修改配置文件 /etc/icinga2/features-enabled/opentsdb.conf

```
object OpenTsdbWriter "opentsdb" {
  host = "host to taosAdapter"
  port = 6048
}
```

taosAdapter 相关配置参数请参考 taosadapter --help 命令输出以及相关文档。

## TCollector 直接写入(通过 taosAdapter)

TCollector 是一个在客户侧收集本地收集器并发送数据到 OpenTSDB 的进程，taosAdaapter 可以支持接收 TCollector 的数据并写入到 TDengine 中。

使能 taosAdapter 配置项 opentsdb_telnet.enable
修改 TCollector 配置文件，修改 OpenTSDB 宿主机地址为 taosAdapter 被部署的地址，并修改端口号为 taosAdapter 使用的端口（默认 6049）。

taosAdapter 相关配置参数请参考 taosadapter --help 命令输出以及相关文档。

## EMQ Broker 直接写入

MQTT 是流行的物联网数据传输协议，[EMQ](https://github.com/emqx/emqx)是一开源的 MQTT Broker 软件，无需任何代码，只需要在 EMQ Dashboard 里使用“规则”做简单配置，即可将 MQTT 的数据直接写入 TDengine。EMQ X 支持通过 发送到 Web 服务的方式保存数据到 TDEngine，也在企业版上提供原生的 TDEngine 驱动实现直接保存。详细使用方法请参考 [EMQ 官方文档](https://docs.emqx.io/broker/latest/cn/rule/rule-example.html#%E4%BF%9D%E5%AD%98%E6%95%B0%E6%8D%AE%E5%88%B0-tdengine)。

## HiveMQ Broker 直接写入

[HiveMQ](https://www.hivemq.com/) 是一个提供免费个人版和企业版的 MQTT 代理，主要用于企业和新兴的机器到机器 M2M 通讯和内部传输，满足可伸缩性、易管理和安全特性。HiveMQ 提供了开源的插件开发包。可以通过 HiveMQ extension - TDengine 保存数据到 TDengine。详细使用方法请参考 [HiveMQ extension - TDengine 说明文档](https://github.com/huskar-t/hivemq-tdengine-extension/blob/b62a26ecc164a310104df57691691b237e091c89/README.md)。
