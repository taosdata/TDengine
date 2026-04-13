---
title: taosX Agent Reference
sidebar_label: taosX Agent
---

import Enterprise from '../../assets/resources/_enterprise.mdx';

<Enterprise/>

TDengine taosX Agent is used when TDengine TSDB cannot access a data source directly, for example due to network restrictions. In such cases, you deploy taosX Agent close to the data source or on the same machine as the data source. taosX Agent reads data from the data source and passes it to taosX within your TDengine TSDB cluster.

## System Requirements

It is not necessary to install taosX Agent on a dedicated machine. You can install it on the same machine as the data source or the same machine as TDengine TSDB.

If you choose to install taosX Agent on a dedicated machine, ensure that the machine has at least 1 CPU, 1 GB of memory, and 50 GB of disk space available. Additional hardware resources may be required depending on the amount of data passing through taosX Agent.

## Configuration

When you create a data ingestion task in TDengine TSDB-Explorer or TDengine Cloud, you can choose to deploy taosX Agent. Follow the steps in the wizard to install and configure taosX Agent.

The default configuration file for taosX Agent is located at `/etc/taos/agent.toml` (Linux) or `C:\TDengine\cfg\agent.toml` (Windows).

You must configure the `endpoint` and `token` as prompted by the wizard in TDengine TSDB-Explorer or TDengine Cloud. In addition, if you choose to run multiple instances of taosX Agent on a single machine, you must set the `instanceId` of each instance to a unique value. You can retain the default value for all other parameters.

- `endpoint`: Enter the GRPC service address of `taosX`.
- `token`: Enter the token generated when creating the agent in TDengine TSDB-Explorer or TDengine Cloud.
- `instanceId`: ID of the current taosx-agent service. If multiple instances of taosX Agent are running on the current machine, enter a unique integer.

The following parameters are optional. If you do not specify a value, the default value is retained.

- `ca`: specifies a CA certificate. You can enter the path to a certificate file, such as `/path/to/ca.pem`, or you can enter the certificate content directly. By default, no certificate is specified.
- `compression`: enables compression for data sent between taosX Agent and taosX. Enter `true` or `false`. The default value is `false`.
- `in_memory_cache_capacity`: maximum number of message batches that can be cached in memory. Enter a positive integer. The default value is `64`.
- `keep_online`: keeps taosX Agent running if the taosX service becomes unavailable. Enter `true` or `false`. The default value is `true`.
- `client_port_range.min`: lower bound of the port range on which taosX Agent listens for connections. Enter an integer greater than or equal to 49152. The default value is `49152`.
- `client_port_range.max`: upper bound of the port range on which taosX Agent listens for connections. Enter an integer less than or equal to 65535. The default value is `65535`.
- `log.path`: directory where log files are stored. Enter a path. The default value for Linux is `/var/log/taos` and for Windows is `C:\\TDengine\\log`.
- `log.level`: log level. Enter `error`, `warn`, `info`, `debug`, or `trace`. The default value is `info`.
- `log.compress`: compresses the log files after rolling. Enter `true` or `false`. The default value is `false`.
- `log.rotationCount`: maximum number of log files to retain in the directory. When this value is exceeded, older files are deleted. Enter a positive integer. The default value is `30`.
- `log.rotationSize`: maximum size of the current log file. When this value is exceeded, log rolling is triggered and a new log file is created. Enter a size in bytes, or specify the unit as KB, MB, or GB. The default value is `1GB`.
- `log.reservedDiskSize`: threshold of remaining disk space to stop writing logs. When available disk space is less than this value, taosX Agent stops generating logs. Enter a size in bytes, or specify the unit as KB, MB, or GB. The default value is `1GB`.
- `log.keepDays`: number of days to retain log files. Log files older than this value are deleted automatically. Enter a number of days. The default value is `30`.

The default configuration file is shown as follows.

```toml
# taosX service endpoint
#
#endpoint = "http://localhost:6055"

# !important!
# Uncomment it and copy-paste the token generated in Explorer.
#
#token = ""

# CA certificate file path or content
# Option 1. file
#ca = "/path/to/ca.pem"
# Option 2. content
#ca = """
#-----BEGIN CERTIFICATE-----
#...
#-----END CERTIFICATE-----
#""""

# Server instance id
#
# The instanceId of each instance is unique on the host
#instanceId = 48

# Enable communication data compression between Agent and taosX
#
#compression = true

# In-memory cache capacity
#
#in_memory_cache_capacity = 64

# Keep the agent alive when the taosX service exits or disconnects
keep_online = true

[client_port_range]
# Minimum boundary of listening port of agent, can not less than 49152
#
# min = 49152

# Maximum boundary of listening port of agent, can not greater than 65535
#
# max = 65535

# Log configuration entry
[log]
# All log files are stored in this directory
#
#path = "/var/log/taos" # on linux/macOS
#path = "C:\\TDengine\\log" # on windows

# log filter level
#
#level = "info"

# Compress archived log files or not
#
#compress = false

# The number of log files retained by the current explorer server instance in the `path` directory
#
#rotationCount = 30

# Rotate when the log file reaches this size
#
#rotationSize = "1GB"

# Log downgrade when the remaining disk space reaches this size, only logging `ERROR` level logs
#
#reservedDiskSize = "1GB"

# The number of days log files are retained
#
#keepDays = 30

```

## Start

On Linux, you can start taosX Agent with systemd:

```shell
systemctl start taosx-agent
```

On Windows, open **Services.msc** and start the **taosX Agent** service.

## Troubleshooting

In addition to viewing the log files directly, on Linux you can also use journalctl to view logs:

```shell
journalctl -u taosx-agent [-f]
```
