---
title: 部署 Explorer
sidebar_label: 部署 Explorer
---

## 简介

如果想通过图形化界面使用和管理 TDengine，或者通过图形化界面管理数据接入，则需要部署 taos explorer。在系统中安装了 TDengine Pro Tools 之后，系统中就具备了 taos explorer，细节请参考 [安装与配置](../install)。

## 准备工作

在启动 taosExplorer 之前，请先确认 TDengine 集群已经正确设置并运行（即 taosd 服务），taosAdapter 也已经正确设置和运行并与 TDengine 集群保持连接状态。如果想要使用数据备份和恢复或者数据同步功能，请确保 taosX 服务和 Agent 服务也已经正确设置和运行。

## 配置

在启动 taosExplorer 之前，请确保配置文件中的内容正确。

```TOML
listen = "0.0.0.0:6060"
log_level = "info"
cluster = "http://localhost:6041"
x_api = "http://localhost:6050"
```

说明：

-   listen - taosExplorer 对外提供服务的地址
-   log_level - 日志级别，可选值为 "debug", "info", "warn", "error", "fatal"
-   cluster - TDengine集群的 taosadapter 地址 
-   x_api - taosX 的服务地址

## 启动

然后启动 taosExplorer，可以直接在命令行执行 taos-explorer 或者使用下面的 systemctl 脚本用 systemctl 来启动 taosExplorer 服务

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

## 问题排查

1. 当通过浏览器打开taosExplorer站点遇到“无法访问此网站”的错误信息时，请通过命令行登录taosExplorer所在机器，并使用命令systemctl status taos-explorer.service检查服务的状态，如果返回的状态是inactive，请使用命令systemctl start taos-explorer.service启动服务。
2. 如果需要获取taosExplorer的详细日志，可通过命令journalctl -u taos-explorer
