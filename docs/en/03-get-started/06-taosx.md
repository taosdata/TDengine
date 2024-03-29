---
title: Set Up taosX
sidebar_label: taosX
---

## Introduction

This section describes how to deploy taosX. Once the taosX installation package is installed, taosX is available in the system. For details, please refer to [Installation and Configuration](../install).

## Configuration

taosX supports configuration through a configuration file. On Linux, the default configuration file path is `/etc/taos/taosx.toml`, and on Windows, it is `C:\\TDengine\\config\\taosx.toml`. A complete taosX configuration file looks like the following:


```toml
# plugins home
#plugins_home = "/usr/local/taos/plugins"

# data dir
#data_dir = "/var/lib/taos/taosx"

# logs home
#logs_home = "/var/log/taos"

# log level: off/error/warn/info/debug/trace
#log_level = "info"

# log keep days
#log_keep_days = 30

# number of threads
#jobs = 0

# enable OpenTelemetry tracing and metrics exporter
#otel = false

#[serve]
# listen to ip:port address
#listen = "0.0.0.0:6050"

# GRPC listen address
#grpc = "0.0.0.0:6055"

# database url
#database_url = "sqlite:taosx.db"
```

The `systemd` configuration file for taosX is located at `/etc/systemd/system/taosx.service`. To configure the taosX service, modify the following line in the `taosx.service` file:

```
ExecStart=/usr/bin/taosx serve -v
```

After you modify the `taosx.service` file, restart the taosX service to cause your changes to take effect:

```
systemctl daemon-reload
systemctl restart taosx
```

## Start taosX

On Linux, use `systemd` to start the taosX service:

```shell
systemctl start taosx
```

On Windows, open the **Services** app and start the **taosX** service.

## Troubleshooting

1. Modifying the taosX log level

The default log level for taosX is `info`. To specify a different level, use the following command-line parameters:
- `info`: `taosx serve -v`
- `debug`: `taosx serve -vv`
- `trace`: `taosx serve -vvv`

To specify command-line parameters when taosX is run as a service, see Configuration.

2. Viewing taosX logs

You can use the `journalctl` command to view taosX log files. The following command displays the latest logs:

```
journalctl -u taosx -f
```
