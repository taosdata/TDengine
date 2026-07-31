---
title: Frequently Asked Questions
sidebar_label: Frequently Asked Questions
description: Common TDgpt installation and query issues
---

### 1. During the installation process, uWSGI fails to compile

Before `v3.4.1.0`, TDgpt installation packages compiled uWSGI locally. Some Python distributions, such as Anaconda, can cause compilation conflicts. In this case, you can choose not to install uWSGI.

However, this means that you must manually run the `python3.10 /usr/local/taos/taosanode/lib/taosanalytics/app.py` command when starting the taosanode service. Use a virtual Python environment when running this command to ensure that dependencies can be loaded.

Since `v3.4.1.0`, Linux uses Gunicorn and Windows uses Waitress, so uWSGI is no longer installed. If an upgraded installation still reports uWSGI errors, verify that `taosanoded` uses the current `taosanode.config.py` and restart the service.

### 2. Anodes fail to be created because the service cannot be accessed

```bash
taos> create anode '127.0.0.1:6035';

DB error: Analysis service can't access[0x80000441] (0.117446s)
```

First, use curl to check whether the anode is providing services: The output of `curl '127.0.0.1:6035'` should be as follows:

```bash
TDgpt - TDengine© Time Series Data Analytics Platform (ver x.x.x)
```

The following output indicates that the anode is not providing services:

```bash
curl: (7) Failed to connect to 127.0.0.1 port 6035: Connection refused
```

If the Anode is not running, check `/var/log/taos/taosanode/error.log` and `taosanode.app.log`.

Do not rely only on `systemctl status taosanoded`; use `curl` and the application logs to verify availability.

### 3. The service is operational, but queries return that the service is not available

```bash
taos> select _frowts,forecast(current, 'algo=arima, conf=0.95, wncheck=0, rows=20') from d1 where ts<='2017-07-14 10:40:09.999';

DB error: Analysis service can't access[0x80000441] (60.195613s)
```

The request may have exceeded its timeout. The SQL `timeout` parameter accepts up to `1200` seconds. Since `v3.4.1.0`, Gunicorn `timeout` and `keepalive` settings are in `/etc/taos/taosanode.config.py` and default to `1200`. Ensure that SQL and Gunicorn or Waitress timeouts are consistent, that `bind` matches the URL registered by `CREATE ANODE`, and that logs do not report worker timeouts, out-of-memory errors, or model-load failures.

### 4. Illegal json format error is returned

This indicates that the analysis results contain an error. Check the anode operation logs in the `/var/log/taos/taosanode/taosanode.app.log` file to find and resolve any issues.

### 5. How to adjust the TDgpt log level and obtain detailed error information

The default TDgpt log level is `DEBUG`. Change `log_level` in `/etc/taos/taosanode.config.py`.

```python
# default log level
log_level = 'DEBUG'
```

The available options for this configuration item include: DEBUG, INFO, CRITICAL, ERROR, and WARN.

For certain errors that cannot be directly identified via return codes, please check the log files to obtain accurate error details. The log files are located in the `/var/log/taos/taosanode/` directory.

- `taosanode.app.log`: TDgpt application logs.
- `access.log` and `error.log`: Gunicorn web-service logs. Windows uses the corresponding Waitress logs.

### 6. Which error codes does TDgpt return?

See [Error Codes](../../10-developer-guide/09-error-codes.md#tdgpt).
