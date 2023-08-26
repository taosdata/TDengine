---
title: 部署 taosX
sidebar_label: 部署 taosX
---

## 简介

本节讲述如何部署 taosX。在安装了 TDengine Pro Tools 安装包后，系统中就具备了 taosX，细节请参考 [安装与配置](../install)。

## 配置

taosX 仅支持通过命令行参数进行配置。服务模式下，taosX 支持的命令行参数可以通过以下方式查看：

```
taosx serve --help
```

建议通过 Systemd 的方式，启动 taosX 的服务模式，其 Systemd 的配置文件位于：`/etc/systemd/system/taosx.service`. 如需修改 taosX 的启动参数，可以编辑该文件中的以下行：

```
ExecStart=/usr/bin/taosx serve -v
```

修改后，需执行以下命令重启 taosX 服务，使配置生效：

```
systemctl daemon-reload
systemctl restart taosx
```

## 启动

Linux 系统上以 Systemd 的方式启动 taosX 的命令如下：

```shell
systemctl start taosx
```

Windows 系统上，请在 "Services" 系统管理工具中找到 "taosX" 服务，然后点击 "启动这个服务"。

## 问题排查

1. 如何修改 taosX 的日志级别？

taosX 的日志级别是通过命令行参数指定的，默认的日志级别为 Info, 具体参数如下：
- INFO: `taosx serve -v`
- DEBUG: `taosx serve -vv`
- TRACE: `taosx serve -vvv`

Systemd 方式启动时，如何修改命令行参数，请参考“配置”章节。

2. 如何查看 taosX 的日志？

以 Systemd 方式启动时，可通过 journalctl 命令查看日志。以滚动方式，实时查看最新日志的命令如下：

```
journalctl -u taosx -f
```
