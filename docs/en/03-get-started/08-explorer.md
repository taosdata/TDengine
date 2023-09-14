---
title: Install and Deploy taosExplorer
sidebar_label: taosExplorer
---

## Introduction

taosExplorer is a GUI for TDengine Enterprise. With taosExplorer, you can use TDengine features and manage your TDengine deployment and data sources in a convenient Web-based interface. taosExplorer is installed automatically when you install taosX. For more information, see [Installation Guide](../install/).

## Prerequisites

Before you start taosExplorer, ensure that your TDengine cluster is running, taosAdapter is running, and your cluster is connected to taosAdapter. If you want to use data backup, restore, or replication in taosExplorer, ensure that the taosX service and agent are running.

## Configuration

You must configure taosExplorer before running it. The configuration is as follows:

```TOML
listen = "0.0.0.0:6060"
log_level = "info"
cluster = "http://localhost:6041"
x_api = "http://localhost:6050"
```

Description:

-   `listen`: Specify the IP address that taosExplorer uses to provide services.
-   `log_level`: Specify the level of logs to record. You can enter `debug`, `info`, `warn`, `error`, or `fatal`.
-   `cluster`: Specify the location of your taosAdapter instance. 
-   `x_api`: Specify the location of your taosX instance.

## Start taosExplorer

To start taosExplorer, you can run the `taos-explorer` command or use `systemctl` to start the `taosExplorer` service.

```shell
[Unit]
Description=Explorer for TDengine
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart=/usr/bin/taos-explorer
Restart=always

[Install]
WantedBy=multi-user.target
```

## Troubleshooting

1. If you encounter a network connection error when opening taosExplorer in your browser, log in to the machine running taosExplorer and run the `systemctl status taos-explorer.service` command to determine the running status of taosExplorer. If the status is `inactive`, run the `systemctl start taos-explorer.service` command to start taosExplorer.
2. To view taosExplorer logs, run the `journalctl -u taos-explorer` command.
