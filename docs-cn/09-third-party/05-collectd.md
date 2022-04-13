---
sidebar_label: collectd
title: collectd 写入
---

安装 collectd，请参考[官方文档](https://collectd.org/download.shtml)。

TDengine 新版本（2.3.0.0+）包含一个 taosAdapter 独立程序，负责接收包括 collectd 的多种应用的数据写入。

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
