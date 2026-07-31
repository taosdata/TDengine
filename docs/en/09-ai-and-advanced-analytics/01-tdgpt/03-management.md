---
title: Anode Management
sidebar_label: Anode Management
description: Start, configure, and register TDgpt Anodes
---

### Starting the TDgpt Service

The `taosanoded` service is created when you install an anode. You can use systemd to manage this service:

```bash
systemctl start  taosanoded
systemctl stop   taosanoded
systemctl status taosanoded
```

### Starting a Time-Series Foundation Model

Time-series foundation models require significant hardware resources. For this reason, they are not started automatically. To start a time-series foundation model manually, use the following procedure:

```bash
# Start TDtsfm
start-model tdtsfm

# Start Time-MoE
start-model timemoe
```

```bash
# Stop TDtsfm
stop-model tdtsfm

# Stop Time-MoE
stop-model timemoe
```

### Directory and Configuration Information

The directory structure of an anode is described in the following table:

|Directory or File|Description|
|---------------|------|
|/usr/local/taos/taosanode/bin|Directory containing executable files|
|/usr/local/taos/taosanode/resource|Directory containing resource files, linked to `/var/lib/taos/taosanode/resource/`|
|/usr/local/taos/taosanode/lib|Directory containing libraries|
|/usr/local/taos/taosanode/model|Directory containing models, linked to `/var/lib/taos/taosanode/model`|
|/var/log/taos/taosanode/|Log directory|
|/etc/taos/taosanode.config.py|Configuration file|

#### Configuration

Since `v3.4.1.0`, Linux Anodes use Gunicorn and read `/etc/taos/taosanode.config.py`. Earlier versions used uWSGI and `taosanode.ini`.

```python
import multiprocessing

bind = '0.0.0.0:6035'
workers = 2
worker_class = 'sync'
threads = max(multiprocessing.cpu_count() // 4 + 1, 2)
max_requests = 1000
max_requests_jitter = 50
timeout = 1200
keepalive = 1200
accesslog = '/var/log/taos/taosanode/access.log'
errorlog = '/var/log/taos/taosanode/error.log'
loglevel = 'debug'
proc_name = 'tdgpt_taosanode_app'
preload_app = True

app_log = '/var/log/taos/taosanode/taosanode.app.log'
model_dir = '/usr/local/taos/taosanode/model/'
log_level = 'DEBUG'
draw_result = False

tdtsfm_1 = 'http://127.0.0.1:6061/tdtsfm'
timemoe_fc = 'http://127.0.0.1:6062/ds_predict'
```

For additional Gunicorn settings, see the [Gunicorn documentation](https://gunicorn.org/reference/settings/).

The main configuration options for an anode are described as follows:

- `app_log`: Anode application log file.
- `model_dir`: Directory in which models are stored.
- `log_level`: Anode application log level. Valid values are `DEBUG`, `INFO`, `CRITICAL`, `ERROR`, and `WARN`; the default is `DEBUG`.

### Managing Anodes

You manage anodes through the TDengine CLI. The following actions must be performed within the CLI on a client that is connected to your TDengine cluster.

#### Create an Anode

```sql
CREATE ANODE {node_url}
```

The `node_url` parameter determines the IP address and port of the anode. This information will be registered to your TDengine cluster. Do not register a single anode to multiple TDengine clusters.

#### View Anodes

You can run the following command to display the FQDN and status of the anodes in your cluster:

```sql
SHOW ANODES;

taos> show anodes;
     id      |              url               |    status    |       create_time       |       update_time       |
==================================================================================================================
           1 | 192.168.0.1:6035               | ready        | 2024-11-28 18:44:27.089 | 2024-11-28 18:44:27.089 |
Query OK, 1 row(s) in set (0.037205s)

```

#### View Advanced Analytics Services

```sql
SHOW ANODES FULL;

taos> show anodes full;                                                      
     id      |            type            |              algo              | 
============================================================================ 
           1 | anomaly-detection          | grubbs                         | 
           1 | anomaly-detection          | lof                            | 
           1 | anomaly-detection          | shesd                          | 
           1 | anomaly-detection          | ksigma                         | 
           1 | anomaly-detection          | iqr                            | 
           1 | anomaly-detection          | sample_ad_model                | 
           1 | forecast                   | arima                          | 
           1 | forecast                   | holtwinters                    | 
           1 | forecast                   | tdtsfm_1                       | 
           1 | forecast                   | timemoe-fc                     | 
Query OK, 10 row(s) in set (0.028750s)                                       
```

The actual list depends on the models loaded by the Anode. Common built-in models include:

| Type | Name | Description |
|--------|--------|--------------------|
| Anomaly detection | `grubbs` | Statistical model |
| Anomaly detection | `lof` | Density-based model |
| Anomaly detection | `shesd` | Seasonal ESD model |
| Anomaly detection | `ksigma` | Statistical model |
| Anomaly detection | `iqr` | Statistical model |
| Forecasting | `arima` | Autoregressive moving average algorithm |
| Forecasting | `holtwinters` | Exponential smoothing algorithm |
| Forecasting | `tdtsfm_1` | TDtsfm v1.0 |
| Forecasting | `timemoe-fc` | Time-MoE |

These algorithms and models are described in detail in the relevant documentation.

#### Refresh the Algorithm Cache

```sql
UPDATE ANODE {anode_id}
UPDATE ALL ANODES
```

#### Delete an Anode

```sql
DROP ANODE {anode_id}
```

Deleting an anode only removes it from your TDengine cluster. To stop an anode, use systemctl on the machine where the anode is located. To remove an anode, run the `rmtaosanode` command on the machine where the anode is located.

### Windows Service and Model Management

Windows installations register the `Taosanode` service and provide scripts under `C:\TDengine\taosanode\bin`:

| Operation | Command |
| --- | --- |
| Start Anode | `start-taosanode.bat` or `net start Taosanode` |
| Stop Anode | `stop-taosanode.bat` or `net stop Taosanode` |
| Show Anode status | `status-taosanode.bat` or `sc query Taosanode` |
| Start one model | `start-model.bat tdtsfm` |
| Stop one model | `stop-model.bat tdtsfm` |
| Show model status | `status-model.bat` |

The Windows configuration file is `C:\TDengine\taosanode\cfg\taosanode.config.py`, and the service uses Waitress. Logs are stored in `C:\TDengine\taosanode\log`.
