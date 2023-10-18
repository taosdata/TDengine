---
title: 安装与配置
sidebar_label: 安装与配置
---

## Linux 平台

TDengine Enterprise 的安装包是一个包含了 TDengine Enterprise 所有核心组件的 all in one 安装包，其命名方式如下：
- `TDengine-Enterprise-<version>-<OS>-<platform>.tar.gz`，例如 `TDengine-Enterprise-3.1.0.3-Linux-x64.tar.gz`

TDengine-Enterprise 安装包中主要包含： 
- `taosd`: 数据库服务端核心组件
- `taosAdapter`：提供 RESTful 接入与 Websocket 连接的代理服务
- `taosKeeper`:  taosd 上报数据的代理服务
- `libtaos.so`：原生连接的客户端 SDK  （C语言接口）
- `libtaosws.so`：WebSocket 连接的客户端 SDK （C语言接口）
- `taosX`：数据接入、同步、备份和恢复的零代码平台
- `taosAgent`：用于一些特定数据源接入时（taosX）的代理服务
- `taosExplorer`：可视化管理工具的服务端
-  数据源接入 SDK：用于连接各种数据源，由 taosX 或 taosAgent 调用

TDengine Enterprise 安装包目前仅支持 Linux 系统。

1. 获取 TDengine serveEnterprise 安装包
2. 进入到安装包所在目录，使用 `tar` 解压安装包；
3. 进入到安装包所在目录，先解压文件后，进入子目录，执行其中的 install.sh 安装脚本。

示例： 请将 `<version>` 替换为下载的安装包版本

```bash
tar -zxvf TDengine-Enterprise-<version>-Linux-x64.tar.gz
```

解压文件后，进入相应子目录，执行其中的 `install.sh` 安装脚本：

```bash
sudo ./install.sh
```

:::info
1. install.sh 安装脚本在执行过程中，缺省会通过命令行交互界面询问一些配置信息，并完成单机运行环境的配置。
2. 如果希望采取无交互安装方式，那么可以运行 `./install.sh -e no`。
3. 运行 `./install.sh -h` 指令可以查看所有参数的详细说明信息。
:::

## Windows 平台

在 Windows 平台上使用 TDengine Enterprise 需要两个安装包，TDengine-server 和 taosX 。

TDengine-server 安装包中包含 taosd, taosAdapter, taosKeeper 组件。 taosX 安装包中包含 taosX, taosExplorer, taos-Agent 组件。下面重点说明 taosX 安装包的安装。TDengine server 的安装类似。

- 下载需要的 taosX 安装包，例如 taosx-1.0.0-Windows-x64-installer.exe，执行安装
- 可使用 uninstall_taosx.exe 进行卸载
- 命令行执行 ```sc start/stop taosx``` 启动/停止 taosx 服务
- 命令行执行 ```sc start/stop taosx-agent``` 启动/停止 taosx-agent 服务
- 命令行执行 ```sc start/stop taos-explorer``` 启动/停止 taosx-agent 服务
- windows 默认安装在```C:\Program Files\taosX```,目录结构如下：
~~~
├── bin
│   ├── taosx.exe
│   ├── taosx-srv.exe
│   ├── taosx-srv.xml
│   ├── taosx-agent.exe
│   ├── taosx-agent-srv.exe
│   ├── taosx-agent-srv.xml
│   ├── taos-explorer.exe
│   ├── taos-explorer-srv.exe
│   └── taos-explorer-srv.xml
├── plugins
│   ├── influxdb
│   │   └── taosx-inflxdb.jar
│   ├── mqtt
│   │   └── taosx-mqtt.exe
│   ├── opc
│   |    └── taosx-opc.exe
│   ├── pi
│   |   └── taosx-pi.exe
│   |   └── taosx-pi-backfill.exe
│   |   └── ...
└── config
│   ├── agent.toml
│   ├── explorer.toml
├── uninstall_taosx.exe
├── uninstall_taosx.dat
~~~
