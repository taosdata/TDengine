---
title: Set Up taosX-Agent Reference Guide
sidebar_label: taosX-Agent
toc_max_heading_level: 4
---

## Introduction

This section explains how to deploy the `Agent` (for `taosX`).

## Configuration

The default configuration file for the `Agent` is located at `/etc/taos/agent.toml`, and it includes the following configuration items:

- `endpoint`: Mandatory, the GRPC service address of `taosX`.
- `token`: Mandatory, the Token generated when creating the `Agent` on `Explorer`.
- `compression`: Optional, can be `true` or `false`, default to `false`. If configured to `true`, enable communication data compression between `Agent` and `taosX`.
- `log_level`: Optional, log level, default is `info`. Similar to `taosX`, it supports `error`, `warn`, `info`, `debug`, `trace`.
- `log_keep_days`: Optional, the number of days to keep logs, default is `30` days.

As shown below:

```TOML
endpoint = "grpc://<taosx-ip>:6055"
token = "<token>"
compression = true
log_level = "info"
log_keep_days = 30
```

## Start taos-Agent

On Linux, use the `systemctl` command to start the `taosX` agent:

```bash
systemctl start taosx-agent
```

On Windows, open the **Services** app and start the **taosx-agent** service.

## Troubleshooting

You can view the log file or use the `journalctl` command to view `Agent` log files.

The command to view logs using `journalctl` on Linux is as follows:

```bash
journalctl -u taosx-agent [-f]
```