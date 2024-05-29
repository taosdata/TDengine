---
title: 可视化管理
sidebar_label: 可视化管理工具
description: taos-explorer 的使用说明，包括安装、配置、使用等。
---

taos-explorer 是一个为用户提供 TDengine 实例的可视化管理交互工具的 web 服务。

## 安装

taos-explorer 无需单独安装，从 TDengine 3.3.0.0 版本开始，它随着 TDengine Server 安装包一起发布，安装完成后，就可以看到 `taos-explorer` 服务。

## 配置

配置文件在 linux 平台上为`/etc/taos/explorer.toml`，配置内容如下：

``` toml
port = 6060
cluster = "http://localhost:6041"
```

配置文件中只需要关注这两项即可：

- port：taos-explorer 对外的服务端口
- cluster：taos-explorer 连接的 TDengine 实例，只支持 websocket 连接，所以该地址为 TDengine 集群中 taosAdapter 服务的地址

## 启动 & 停止

在启动之前，请先确保 TDengine 集群（主要服务是 `taosd` 和 `taosAdapter`）已经启动并在正确运行，并确保 taos-explorer 的配置正确。

### Linux 系统

使用 `systemctl` 命令可以管理 taos-explorer 服务：

- 启动服务进程：`systemctl start taos-explorer`

- 停止服务进程：`systemctl stop taos-explorer`

- 重启服务进程：`systemctl restart taos-explorer`

- 查看服务状态：`systemctl status taos-explorer`

## 注册 & 登录

### 注册流程

安装好，打开浏览器，默认访问`http://ip:6060`来访问 taos-explorer 服务。如果还没有注册过，则首先进入注册界面。输入手机号获取验证码，输入正确的验证码后，即可注册成功。

### 登录

登录时，请使用数据库用户名和密码登录。首次使用，默认的用户名为 `root`，密码为 `taosdata`。登录成功后即可进入`数据浏览器`页面，您可以使用查看数据库、 创建数据库、创建超级表/子表等管理功能。

其他功能页面，如`数据写入-数据源`等页面，为企业版特有功能，您可以点击查看和简单体验，并不能实际使用。
