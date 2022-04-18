---
sidebar_label: TCollector
title: TCollector 写入
---

TCollector 是 openTSDB 的一部分，它用来采集客户端日志发送给数据库。安装 TCollector 请参考[官方文档](http://opentsdb.net/docs/build/html/user_guide/utilities/tcollector.html#installation-of-tcollector)

TDengine 新版本（2.4.0.0+）包含一个 taosAdapter 独立程序，负责接收包括 TCollector 的多种应用的数据写入。

启动 taosAdapter 的命令为 `systemctl start taosadapter`，可以使用 `systemctl status taosadapter` 检查 taosAdapter 的运行状态。

TCollector 是一个在客户侧收集本地收集器并发送数据到 OpenTSDB 的进程，taosAdapter 可以支持接收 TCollector 的数据并写入到 TDengine 中。

使能 taosAdapter 配置项 `opentsdb_telnet.enable`
修改 TCollector 配置文件，修改 OpenTSDB 宿主机地址为 taosAdapter 被部署的地址，并修改端口号为 taosAdapter 使用的端口（默认 6049）。

taosAdapter 相关配置参数请参考 `taosadapter --help` 命令输出以及相关文档。
