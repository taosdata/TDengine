---
title: 安装与配置
sidebar_label: 安装与配置
---

## Linux 平台

TDengine Enterprise 的安装包是一个包含了 TDengine Enterprise 所有核心组件的 all in one 安装包，其命名方式如下：

- `TDengine-enterprise-<version>-<OS>-<platform>.tar.gz`，例如 `TDengine-enterprise-3.1.0.3-Linux-x64.tar.gz`

TDengine Enterprise 安装包中主要包含：

- `taosd`: 数据库服务端核心组件
- `taosAdapter`：提供 RESTful 接入与 Websocket 连接的代理服务
- `taosKeeper`:  taosd 上报数据的代理服务
- `libtaos.so`：原生连接的客户端 SDK  （C语言接口）
- `libtaosws.so`：WebSocket 连接的客户端 SDK （C语言接口）
- taosX (`taosx`)：数据接入、同步、备份和恢复的零代码平台
- Explorer (`taos-explorer`)：可视化管理工具的服务端
- 数据源接入 SDK：用于连接各种数据源，由 taosX 调用

安装前依赖项检查：

- 检查是否已安装 `JDK1.8` 或更高版本（通过 Shell 命令 ```java -version``` 检查）

安装步骤如下：

- 获取 TDengine Enterprise 安装包
- 进入到安装包所在目录，使用 `tar` 解压安装包；
- 进入到安装包所在目录，先解压文件后，进入子目录，执行其中的 install.sh 安装脚本。
- 默认安装路径为 /usr/local/taos
- start-all.sh 可以快速在本机启动所有必要的服务
- stop-all.sh 可以快速停止本机上所有与 TDengine Enterprise 有关的服务

示例： 请将 `<version>` 替换为下载的安装包版本

```bash
tar -zxvf TDengine-enterprise-<version>-Linux-x64.tar.gz
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

TDengine Enterprise 的安装包是一个包含了 TDengine Enterprise 所有核心组件的 all in one 安装包，其命名方式如下：

- `TDengine-enterprise-<version>-<OS>-<platform>.tar.gz`，例如 `TDengine-enterprise-3.1.0.3-Windows-x64.tar.gz`

TDengine Enterprise 安装包中主要包含：

- `taosd`: 数据库服务端核心组件
- `taosAdapter`：提供 RESTful 接入与 Websocket 连接的代理服务
- `taosKeeper`:  taosd 上报数据的代理服务
- `libtaos.so`：原生连接的客户端 SDK  （C语言接口）
- `libtaosws.so`：WebSocket 连接的客户端 SDK （C语言接口）
- taosX (`taosx`)：数据接入、同步、备份和恢复的零代码平台
- Explorer (`taos-explorer`)：可视化管理工具的服务端
- 数据源接入 SDK：用于连接各种数据源，由 taosX 调用

安装前依赖项检查：

- 检查是否已安装 `JDK1.8` 或更高版本（通过 CMD 命令 ```java -version``` 检查）
- 检查是否已安装 `Visual C++ 运行时库`（在`控制面板-程序和功能`中查看是否存在 `Microsoft Visual C++ Redistributable` 的条目）, 如果没有安装，可在此下载安装 [VC运行时库](https://learn.microsoft.com/zh-cn/cpp/windows/latest-supported-vc-redist?view=msvc-170)

安装步骤如下：

- 下载需要的版本的 TDengine Enterprise 安装包，例如 TDengine-Enterprise-3.1.1.13-Windows-x64.exe，执行安装
- 可使用 uninstall_TDengine.exe 进行卸载
- 命令行执行 ```sc.exe start/stop taosd``` 启动/停止 taosd 服务
- 命令行执行 ```sc.exe start/stop taosadapter``` 启动/停止 taosadapter 服务
- 命令行执行 ```sc.exe start/stop taoskeeper``` 启动/停止 taoskeeper 服务
- 命令行执行 ```sc.exe start/stop taosx``` 启动/停止 taosx 服务
- 命令行执行 ```sc.exe start/stop taos-explorer``` 启动/停止 taosx-agent 服务