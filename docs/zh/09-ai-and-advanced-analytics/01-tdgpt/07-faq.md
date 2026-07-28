---
title: 常见问题
sidebar_label: 常见问题
description: TDgpt 安装与调用常见问题
---

### 1. 安装过程中编译 uWSGI 失败，如何处理

`v3.4.1.0` 之前的安装包在本地编译 uWSGI 时，某些 Python 环境（例如 Anaconda）可能冲突导致编译失败，安装流程因此无法继续。可尝试在安装过程中忽略 uWSGI 的安装。

由于忽略了 uWSGI 安装，后续启动 taosanode 服务时，需要手动执行：

```bash
python3.10 /usr/local/taos/taosanode/lib/taosanalytics/app.py
```

执行该命令时请确保使用虚拟环境中的 Python，才能正确加载依赖库。

自 `v3.4.1.0` 起，Linux 环境改用 Gunicorn（Windows 使用 Waitress），不再依赖 uWSGI 编译安装。若升级后仍看到与 uWSGI / `uwsgi` 相关的报错，说明进程或包装脚本仍指向旧入口，应确认 `taosanoded` 使用的是当前安装目录下的 Gunicorn / Waitress 配置（`/etc/taos/taosanode.config.py` 或安装目录 `cfg/taosanode.config.py`），并重启服务。

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

如果 anode 服务启动或运行不正常，请检查服务日志 `/var/log/taos/taosanode/error.log`，根据错误信息排查问题。

> 请勿仅使用 `systemctl status taosanoded` 判断 taosanode 是否可用；应以 `curl` 探测与业务日志为准。

### 3. 服务正常，查询过程返回服务不可用

```bash
taos> select _frowts,forecast(current, 'algo=arima, conf=0.95, wncheck=0, rows=20') from d1 where ts<='2017-07-14 10:40:09.999';

DB error: Analysis service can't access[0x80000441] (60.195613s)
```

常见原因是分析过程超过默认最长等待时间。可在 SQL 中通过 `timeout` 参数延长单次请求等待时间；`timeout` 最大值为 `1200`（秒），即单次请求最长约 20 分钟。

自 `v3.4.1.0` 起，Gunicorn 的超时相关配置在 `/etc/taos/taosanode.config.py`（例如 `timeout`、`keepalive`，默认均为 `1200`）。更早版本若仍使用 uWSGI，则需在当时的 `taosanode.ini` 中调整 `harakiri`、`http-timeout` 等项。

排障时建议同时核对：

- SQL 侧 `timeout` 与 Gunicorn / Waitress 的 `timeout`（或 `channel_timeout`）是否匹配；任一侧过小都会表现为“服务不可用”。
- `bind` 地址与端口是否与 `CREATE ANODE` 注册的 URL 一致（默认 `0.0.0.0:6035`）。
- `/var/log/taos/taosanode/error.log`（Gunicorn）与 `taosanode.app.log`（应用）中是否有 worker 超时、OOM 或模型加载失败记录。

### 4. 返回结果出现非法 JSON 格式错误 (Invalid json format)

从 anode 返回到 TDengine 的分析结果有误，请检查 anode 运行日志 `/var/log/taos/taosanode/taosanode.app.log` 获得具体的错误信息。

### 5. 如何调整 TDgpt 日志级别以及获得其详细的错误信息

TDgpt 默认日志级别是 DEBUG。调整其日志级别需要更改 TDgpt 配置文件 `/etc/taos/taosanode.config.py` 中的 `log_level` 配置项。

```python
# default TDgpt log level
log_level = 'DEBUG'
```

该配置项可选配置包括：`'DEBUG'`，`'INFO'`，`'CRITICAL'`，`'ERROR'`，`'WARN'`。

对于某些无法直接使用错误码返回的错误信息，请检查日志文件获得准确的错误信息。日志文件位于 `/var/log/taos/taosanode/` 目录。

- `taosanode.app.log` 是 TDgpt 产生的日志
- `access.log` 和 `error.log` 是 Gunicorn 产生的 Web 服务日志（Windows 上对应 Waitress 相关日志）

### 6. TDgpt 会返回哪些错误码

参见 [错误码](../../10-developer-guide/09-error-codes.md#tdgpt)。
