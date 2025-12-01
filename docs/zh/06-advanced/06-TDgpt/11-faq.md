---
title: "常见问题"
sidebar_label: "常见问题"
description: 常见问题
---

### 1. 安装过程中编译 uWSGI 失败，如何处理

TDgpt 安装过程中需要在本地编译 uWSGI，某些环境的 Python（例如：anaconda）安装 uWSGI 会出现冲突导致编译失败，安装流程因此无法继续下去。这种情况下可以尝试在安装过程中忽略 uWSGI 的安装。
由于忽略了 uWSGI 安装，后续启动 taosasnode 服务的时候，需要手动输入命令进行启动 `python3.10 /usr/local/taos/taosanode/lib/taosanalytics/app.py`。执行该命令的时候请确保使用了虚拟环境中的 Python 程序才能加载依赖库。

### 2. 创建 anode 失败，返回指定服务无法访问

```bash
taos> create anode '127.0.0.1:6035';

DB error: Analysis service can't access[0x80000441] (0.117446s)
```

请务必使用 `curl` 命令检查 anode 服务是否正常。执行 `curl '127.0.0.1:6035'` 后，正常的 anode 服务会返回以下结果。

```bash
TDgpt - TDengine© Time Series Data Analytics Platform (ver x.x.x)
```

如果出现下面的结果，表示 anode 服务不正常。

```bash
curl: (7) Failed to connect to 127.0.0.1 port 6035: Connection refused
```

如果 anode 服务启动/运行不正常，请检查 uWSGI 的运行日志 `/var/log/taos/taosanode/taosanode.log`，检查其中的错误信息，根据错误信息解决响应的问题。

> 请勿使用 systemctl status taosanode 检查 taosanode 是否正常

### 3. 服务正常，查询过程返回服务不可用

```bash
taos> select _frowts,forecast(current, 'algo=arima, alpha=95, wncheck=0, rows=20') from d1 where ts<='2017-07-14 10:40:09.999';

DB error: Analysis service can't access[0x80000441] (60.195613s)
```

出现这个问题的原因是输入数据分析过程超过默认的最长等待时间。如果您使用 `uWSGI` 驱动 TDgpt 运行，`uWSGI` 默认单个请求最长等待时间 60s。调整最长等待时间请在配置文件 `taosanode.ini` 中添加配置项：

```ini
# 工作进程在没有请求的情况下自动退出的时间，设置为 0 表示无限等待
harakiri = 0

# 接收到请求后，等待响应的超时时间，默认为 60s。这里修改为 20 分钟
http-timeout = 1200
```

然后在查询 SQL 语句中使用 timeout 参数设置单次请求最大等待时间即可。需要注意 `timeout` 参数最大值为 1200，即单次请求最大执行时间不能够超过 20 分钟。

如果您使用 Python 驱动 TDgpt 运行，在请求的 SQL 语句中调整 timeout 参数即可。

### 4. 返回结果出现非法 JSON 格式错误 (Invalid json format)

从 anode 返回到 TDengine TSDB 的分析结果有误，请检查 anode 运行日志 `/var/log/taos/taosanode/taosanode.app.log` 获得具体的错误信息。

### 5. 如何调整 TDgpt 日志级别以及获得其详细的错误信息

TDgpt 默认日志级别是 debug。调整其日志级别需要更改 TDgpt 配置文件 `/etc/taos/taosanode.ini` 中的 `log-level` 配置项。

```ini
# default log level
log-level = INFO
```

该配置项可选配置包括：DEBUG，INFO，CRITICAL，ERROR，WARN。

对于某些无法直接使用错误码返回的错误信息，请检查日志文件获得准确的错误信息。日志文件位于 `/var/log/taos/taosanode/` 目录。
`taosanode.app.log` 是 TDgpt 产生的日志
`taosanode.log` 是 uWSGI 产生的 web 服务日志
