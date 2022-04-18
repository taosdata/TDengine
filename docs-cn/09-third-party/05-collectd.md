---
sidebar_label: collectd
title: collectd 写入
---

collectd 是一款插件式架构的开源监控软件，它可以收集各种来源的指标，如操作系统，应用程序，日志文件和外部设备，并存储此信息或通过网络提供。安装 collectd 请参考[官方文档](https://collectd.org/download.shtml)。

TDengine 新版本（2.4.0.0+）包含一个 taosAdapter 独立程序，可以接收包括 collectd 的多种应用的数据写入。

启动 taosAdapter 的命令为 `systemctl start taosadapter`，可以使用 `systemctl status taosadapter` 检查 taosAdapter 的运行状态。

在 `/etc/collectd/collectd.conf` 文件中增加如下内容，其中 host 和 port 请填写 TDengine 和 taosAdapter 配置的实际值：

```
LoadPlugin network
<Plugin network>
  Server "<TDengine cluster/server host>" "<port for collectd>"
</Plugin>
```

重启 collectd

```
sudo systemctl start collectd
```

taosAdapter 相关配置参数请参考 `taosadapter --help` 命令输出以及相关文档。
