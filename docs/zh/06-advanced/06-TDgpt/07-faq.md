---
title: "常见问题"
sidebar_label: "常见问题"
---

<b>1. 创建 anode 失败，返回指定服务无法访问</b>

```bash
taos> create anode '127.0.0.1:6090';

DB error: Analysis service can't access[0x80000441] (0.117446s)
```

请务必使用 `curl` 命令检查 anode 服务是否正常。`curl '127.0.0.1:6090'` 正常的 anode 服务会返回以下结果。

```bash
TDengine© Time Series Data Analytics Platform (ver 1.0.x)
```

如果出现下面的结果，表示 anode 服务不正常。
```bash
curl: (7) Failed to connect to 127.0.0.1 port 6090: Connection refused
```

如果 anode 服务启动/运行不正常，请检查 uWSGI 的运行日志 `/var/log/taos/taosanode/taosanode.log`，检查其中的错误信息，根据错误信息解决响应的问题。

>请勿使用 systemctl status taosanode 检查 taosanode 是否正常

<b>2. 查询结果不正确</b>
