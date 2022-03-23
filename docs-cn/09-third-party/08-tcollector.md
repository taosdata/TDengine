---
sidebar_label: TCollector
---

# TCollector 写入

TCollector 是一个在客户侧收集本地收集器并发送数据到 OpenTSDB 的进程，taosAdaapter 可以支持接收 TCollector 的数据并写入到 TDengine 中。

使能 taosAdapter 配置项 opentsdb_telnet.enable
修改 TCollector 配置文件，修改 OpenTSDB 宿主机地址为 taosAdapter 被部署的地址，并修改端口号为 taosAdapter 使用的端口（默认 6049）。

taosAdapter 相关配置参数请参考 taosadapter --help 命令输出以及相关文档。
