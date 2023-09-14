---
title: Set Up taosX
sidebar_label: taosX
---

## Introduction

This document describes how to install taosX. The taosX installation package includes taosX. For more information, see [Installation Guide](../install/).

## Configuration

You can configure taosX with command-line parameters. To view the supported parameters when taosX is run as a service, run the following command:

```
taosx serve --help
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
