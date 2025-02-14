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

<b>2. 服务正常，查询过程返回服务不可用</b>
```bash
taos> select _frowts,forecast(current, 'algo=arima, alpha=95, wncheck=0, rows=20') from d1 where ts<='2017-07-14 10:40:09.999';

DB error: Analysis service can't access[0x80000441] (60.195613s)
```
数据分析默认超时时间是 60s，出现这个问题的原因是输入数据分析过程超过默认的最长等待时间，请尝试采用限制数据输入范围的方式将输入数据规模减小或者更换分析算法再次尝试。

<b>3. 返回结果出现非法 JSON 格式错误 (Invalid json format) </b>

从 anode 返回到 TDengine 的分析结果有误，请检查 anode 运行日志 `/var/log/taos/taosanode/taosanode.app.log`，以便于获得具体的错误信息。

