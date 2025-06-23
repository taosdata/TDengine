---
title: Frequently Asked questions
sidebar_label: Frequently Asked questions
---

### 1. During the installation process, uWSGI fails to compile

The TDgpt installation process compiles uWSGI on your local machine. In certain Python distributions, such as Anaconda, conflicts may occur during compilation. In this case, you can choose not to install uWSGI.
However, this means that you must manually run the `python3.10 /usr/local/taos/taosanode/lib/taosanalytics/app.py` command when starting the taosanode service. Use a virtual Python environment when running this command to ensure that dependencies can be loaded.

### 2. Anodes fail to be created because the service cannot be accessed

```bash
taos> create anode '127.0.0.1:6090';

DB error: Analysis service can't access[0x80000441] (0.117446s)
```

First, use curl to check whether the anode is providing services: The output of `curl '127.0.0.1:6090'` should be as follows:

```bash
TDengine© Time Series Data Analytics Platform (ver 1.0.x)
```

The following output indicates that the anode is not providing services:

```bash
curl: (7) Failed to connect to 127.0.0.1 port 6090: Connection refused
```

If the anode has not started or is not running, check the uWSGI log logs in the `/var/log/taos/taosanode/taosanode.log` file to find and resolve any errors.

Note: Do not use systemctl to check the status of the taosanode service.

### 3. The service is operational, but queries return that the service is not available

```bash
taos> select _frowts,forecast(current, 'algo=arima, alpha=95, wncheck=0, rows=20') from d1 where ts<='2017-07-14 10:40:09.999';

DB error: Analysis service can't access[0x80000441] (60.195613s)
```

The timeout period for the analysis service is 60 seconds. If the analysis process cannot be completed within this period, this error will occur. You can reduce the scope of data being analyzed or try another algorithm to avoid the error.

### 4. Illegal json format error is returned

This indicates that the analysis results contain an error. Check the anode operation logs in the `/var/log/taos/taosanode/taosanode.app.log` file to find and resolve any issues.
