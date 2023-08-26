---
title: 安装与配置
sidebar_label: 安装与配置
---

## 简介

TDengine Pro 包含两个安装包： 
- `TDengine-server-<version>-<OS>-<platform>.tar.gz`，例如 `TDengine-server-3.1.0.3-Linux-x64.tar.gz`
- `TDengine-pro-tools-<version>-<OS-<platform>.tar.gz`，例如 `TDengine-pro-tools-1.2.0-Linux-x64.tar.gz`

TDengine-server 安装包中主要包含： 
- `taosd`: 数据库服务端核心组件
- `taosAdapter`：提供 RESTful 接入与 Websocket 连接的代理服务
- `taosKeeper`:  taosd 上报数据的代理服务
- `libtaos.so`：原生连接的客户端 SDK  （C语言接口）
- `libtaosws.so`：WebSocket 连接的客户端 SDK （C语言接口）

TDengine-pro-tools 安装包中主要包含：
- `taosX`：数据接入、同步、备份和恢复的零代码平台
- `taosAgent`：用于一些特定数据源接入时（taosX）的代理服务
- `taosExplorer`：可视化管理工具的服务端
- 数据源接入 SDK：用于连接各种数据源，由 taosX 或 taosAgent 调用

## 安装 TDengine Server

### Linux 安装

1. 获取 TDengine server 安装包
2. 进入到安装包所在目录，使用 `tar` 解压安装包；
3. 进入到安装包所在目录，先解压文件后，进入子目录，执行其中的 install.sh 安装脚本。

示例： 请将 `<version>` 替换为下载的安装包版本

```bash
tar -zxvf TDengine-server-<version>-Linux-x64.tar.gz
```

解压文件后，进入相应子目录，执行其中的 `install.sh` 安装脚本：

```bash
sudo ./install.sh
```

:::info
install.sh 安装脚本在执行过程中，会通过命令行交互界面询问一些配置信息。如果希望采取无交互安装方式，那么可以运行 `./install.sh -e no`。运行 `./install.sh -h` 指令可以查看所有参数的详细说明信息。
:::

## 安装 TDengine ProTools

### Linux 安装

下载需要的 taosX 安装包，下文以安装包 `taosx-1.0.0-linux-x64.tar.gz` 为例展示如何安装：

``` bash
# 在任意目录下解压文件
tar -zxf taosx-1.0.0-linux-x64.tar.gz
cd taosx-1.0.0-linux-x64

# 安装
sudo ./install.sh

# 验证
taosx -V 
# taosx 1.0.0-494d280c (built linux-x86_64 2023-06-21 11:06:00 +08:00)
taosx-agent -V 
# taosx-agent 1.0.0-494d280c (built linux-x86_64 2023-06-21 11:06:01 +08:00)

# 卸载
cd /usr/local/taosx
sudo ./uninstall.sh
```

**常见问题:**

1. 安装后系统中增加了哪些文件？
    * /usr/bin: taosx, taosx-agent, taos-explorer
    * /usr/local/taosx/plugins: influxdb, mqtt, opc
    * /etc/systemd/system:taosx.service, taosx-agent.service, taos-explorer.service
    * /usr/local/taosx: uninstall.sh 
    * /etc/taox: agent.toml, explorer.toml

2. taosx -V 提示 "Command not found" 应该如何解决？
    * 检验问题1，保证所有的文件都被复制到对应的目录
    ``` bash
    ls /usr/bin | grep taosx
    ```

### Windows 安装

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
