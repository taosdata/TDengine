---
title: Set Up taos-Agent
sidebar_label: taos-Agent
---

## Introduction

This section explains how to deploy the Agent (for taosX). Once the taosX installation package is installed, the system has the Agent. For details, please refer to [Installation and Configuration](../install).

## Configuration

The default configuration file for the Agent is located at `/etc/taos/agent.toml`, and it includes the following configuration items:

- `endpoint`: Mandatory, the GRPC service address of taosX.
- `token`: Mandatory, the Token generated when creating the Agent on Explorer.
- `log_level`: Optional, log level, default is `info`. Similar to taosX, it supports `error`, `warn`, `info`, `debug`, `trace`.
- `log_keep_days`: Optional, the number of days to keep logs, default is `30` days.

As shown below:

```TOML
endpoint = "grpc://<taosx-ip>:6055"
token = "<token>"
log_level = "info"
log_keep_days = 30
```

## Start taos-Agent

On Linux, use the `systemctl` command to start the taosX agent:

```bash
systemctl start taosx-agent
```

On Windows, open the **Services** app and start the **taosx-agent** service.

## Troubleshooting

You can view the Agent's logs using `journalctl` or by checking the log file `/var/log/taos/agent.log` on Linux or `C:\\TDengine\\log\agent.log` on Windows.

The command to view logs using `journalctl` on Linux is as follows:

```bash
journalctl -u taosx-agent -f
```
