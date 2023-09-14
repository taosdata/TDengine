---
title: Install and Deploy taos-Agent
sidebar_label: taos-Agent
---

## Introduction

This document describes how to install the taosX agent. The taosX agent is installed automatically when you install taosX. For more information, see [Installation Guide](../install/).

## Configuration

The configuration file for the taosX agent is located at `/etc/taos/agent.toml`. The configuration options are described as follows:
- endpoint: (Mandatory) Specify the GRPC endpoint of taosX.
- token: (Mandatory) Specify the token generated for the agent in taosExplorer.
- debug_level: (Optional) Specify the debug level. You can enter `info`, `debug`, or `trace`. The default value is `info`.

The configuration file is described as follows:

```TOML
endpoint = "grpc://<taosx-ip>:6055"
token = "<token>"
log_level = "debug"
```

Log retention settings
You can configure the length of time for which log files are stores by specifying a value for the `TAOSX_LOGS_KEEP_DAYS` environmental variable. The default value is 30 days.

```shell
export TAOSX_LOGS_KEEP_DAYS=7
```

## Start the taosX Agent

On Linux, use the `systemctl` command to start the taosX agent:

```
systemctl start taosx-agent
```

On Windows, open the **Services** app and start the **taosx-agent** service.

## Troubleshooting

You can use the `journalctl` command to view the logs for the taosX agent:

```
journalctl -u taosx-agent -f
```
