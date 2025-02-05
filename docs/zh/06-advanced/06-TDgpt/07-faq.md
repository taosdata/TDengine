---
title: "常见问题"
sidebar_label: "常见问题"
---

1. 创建 anode 失败

```bash
taos> create anode '127.0.0.1:6090';

DB error: Analysis service can't access[0x80000441] (0.117446s)
```

请检查 anode 服务是否工作正常。

```bash
curl '127.0.0.1:6090'
curl: (7) Failed to connect to 127.0.0.1 port 6090: Connection refused
```

```bash
TDengine© Time Series Data Analytics Platform (ver 1.0.x)
```

