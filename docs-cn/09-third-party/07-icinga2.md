---
sidebar_label: icinga2
title: icinga2 写入
---

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