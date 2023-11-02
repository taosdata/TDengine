---
title: 部署 Agent
sidebar_label: 部署 Agent
---

## 简介

本节讲述如何部署 Agent (for tasoX)。在安装了 taosX 安装包后，系统中就具备了 Agent，细节请参考 [安装与配置](../install)。

## 配置

Agent 默认的配置文件位于`/etc/taos/agent.toml`, 包含以下配置项：
- endpoint: 必填，taosX 的 GRPC endpoint
- token: 必填，在 taosExplorer 上创建 agent 时，产生的token
- debug_level: 非必填，默认为 info, 还支持 debug, trace 等级别

如下所示：

```TOML
endpoint = "grpc://<taosx-ip>:6055"
token = "<token>"
log_level = "debug"
```

日志保存时间设置
日志保存的天数可以通过环境变量进行设置 TAOSX_LOGS_KEEP_DAYS， 默认为 30 天。

```shell
export TAOSX_LOGS_KEEP_DAYS=7
```

## 启动

Linux 系统上 Agent 可以通过 Systemd 命令启动：

```
systemctl start taosx-agent
```

Windows 系统上通过系统管理工具 "Services" 找到 taosx-agent 服务，然后启动它。

## 问题排查

可以通过 journalctl 查看 Agent 的日志

```
journalctl -u taosx-agent -f
```
