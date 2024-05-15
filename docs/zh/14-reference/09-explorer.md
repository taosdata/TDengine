---
title: 可视化管理
sidebar_label: 可视化管理工具
description: taosExplorer 的使用说明，包括安装、配置、使用等。
---

taos-explorer 为用户提供 TDengine 实例的可视化管理交互界面。

## 安装

无需单独安装，随着 TDengine 服务一起安装，安装完成后，您可以看到 `taos-explorer` 服务。

## 配置

配置文件在 linux 平台上为`/etc/taos/explorer.toml`，windows 平台上为`C:\TDengine\cfg\explorer.toml`。配置内容如下：

``` toml
port = 6060
cluster = "http://localhost:6041"
```

配置文件中只需要关注这两项即可：

- port：explorer 服务端口
- cluster：explorer 连接的 TDengine 实例(taosAdpter)

## 启动&停止

### Linux 系统

如下 `systemctl` 命令可以帮助你管理 taos-explorer 服务：

- 启动服务进程：`systemctl start taos-explorer`

- 停止服务进程：`systemctl stop taos-explorer`

- 重启服务进程：`systemctl restart taos-explorer`

- 查看服务状态：`systemctl status taos-explorer`

### Windows 系统

安装后，可以在拥有管理员权限的 cmd 窗口执行 `sc start taos-explorer` 或在 `C:\TDengine` 目录下，运行 `taos-explorer.exe` 来启动 explorer 服务进程。

## 注册&登录

### 注册流程

安装好，您可以打开浏览器，默认访问`http://ip:6060`来访问 explorer 平台。如果还没有注册过，则首先进入注册界面。输入手机号获取验证码，输入正确的验证码后，即可注册成功。

### 登录

登录时，请使用数据库用户名和密码登录。首次使用，默认的用户名为 `root`，密码为 `taosdata`。登录成功后即可进入`数据浏览器`页面，您可以使用查看数据库、 创建数据库、创建超级表/子表等管理功能。

其他功能页面，如`数据源`等页面，为企业版特有功能，您可以点击查看和简单体验，并不能实际使用。
