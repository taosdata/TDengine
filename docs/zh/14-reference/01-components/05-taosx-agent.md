---
title: taosX-Agent 参考手册
sidebar_label: taosX-Agent
---

本节讲述如何部署 `Agent` (for `taosX`)。使用之前需要安装 TDengine Enterprise 安装包之后，taosX-Agent 用于在部分数据接入场景，如 Pi, OPC UA, OPC DA 等对访问数据源有一定限制或者网络环境特殊的场景下，可以将 taosX-Agent 部署在靠近数据源的环境中甚至与数据源在相同的服务器上，由 taosX-Agent 负责从数据源读取数据并发送给 taosX。

## 配置

`Agent` 默认的配置文件位于 `/etc/taos/agent.toml`, 包含以下配置项：

- `endpoint`: 必填，`taosX` 的 GRPC 服务地址。
- `token`: 必填，在 `Explorer` 上创建 `Agent` 时，产生的 Token。
- `compression`: 非必填，可配置为 `ture` 或 `false`, 默认为 `false`。配置为`true`, 则开启 `Agent` 和 `taosX` 通信数据压缩。
- `log_level`: 非必填，日志级别，默认为 `info`, 同 `taosX` 一样，支持 `error`，`warn`，`info`，`debug`，`trace` 五级。
- `log_keep_days`：非必填，日志保存天数，默认为 `30` 天。

如下所示：

```TOML
endpoint = "grpc://<taosx-ip>:6055"
token = "<token>"
compression = true
log_level = "info"
log_keep_days = 30
```

您不必对配置文件如何设置感到疑惑，阅读并跟随 `Explorer` 中创建 `Agent` 的提示进行操作，您可以对配置文件进行查看、修改和检查。

## 启动

Linux 系统上 `Agent` 可以通过 Systemd 命令启动：

```bash
systemctl start taosx-agent
```

Windows 系统上通过系统管理工具 "Services" 找到 taosx-agent 服务，然后启动它。

## 问题排查

您可以查看日志文件或使用 `journalctl` 命令来查看 `Agent` 的日志。

Linux 下 `journalctl` 查看日志的命令如下：

```bash
journalctl -u taosx-agent [-f]
```
