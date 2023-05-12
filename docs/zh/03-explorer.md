---
sidebar_label: "taosExplorer"
title: "可视化管理"
description: "为了易于企业版用户更容易使用和管理数据库，TDengine 3.0 企业版提供了一个全新的可视化组件 taosExplorer"
---

## 简介

为了易于企业版用户更容易使用和管理数据库，TDengine 3.0 企业版提供了一个全新的可视化组件 taosExplorer。用户能够在其中方便地管理数据库管理系统中中各元素（数据库、超级表、子表）的生命周期，执行查询，监控系统状态，管理用户和授权，完成数据备份和恢复，与其它集群之间进行数据同步，导出数据，管理 topic 和 stream 。

### 配置和启动

1.  taosExplorer 没有独立的安装包，请使用 TDegnine 企业版安装包进行安装。
2.  在启动 taosExplorer 之前，请先确认 TDengine 集群已经正确设置并运行（即 taosd 服务），taosAdapter 也已经正确设置和运行并与 TDengine 集群保持连接状态。如果想要使用数据备份和恢复或者数据同步功能，请确保 taosX 服务也已经正确设置和运行。
3.  在启动 taosExplorer 之前，请确保配置文件中的内容正确]

```TOML
listen = "0.0.0.0:6060"
log_level = "info"
x_api = "http://localhost:6050"
```

说明：

-   listen - taosExplorer 对外提供服务的地址
-   log_level - 日志级别，可选值为 "debug", "info", "warn", "error", "fatal"
-   x_api - taosX 的服务地址

然后启动 taosExplorer，可以直接在命令行执行 taos-explorer 或者使用下面的 systemctl 脚本用 systemctl 来启动 taosExplorer 服务

```TOML
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

### 登录

打开浏览器，输入正确的用户名和密码（注：这里的用户名和密码是所连接的 TDengine 中的用户名和密码）

![登录界面](./login.png)

点击登录按钮进入主界面，主界面功能展示如下：

![主界面示例](./ui.png)

### 仪表盘

taosExplorer 内置了一个简单的仪表盘展示以下集群信息，点击左侧功能列表中的 "Dashboard" 可以启用此功能。

<!-- -   Uptime: 集群自上次重启后迄今的运行时间
-   Create Time: 集群被创建的时间
-   Edition: TDengine 版本，正式企业版 或 试用企业版
-   Expire Time: 企业版的授权到期时间 -->
- 默认的仪表盘回现实对应的文档
- 配置过 grafana 的仪表盘在点击' Dahsboard /面板'时候会跳转到对应的配置地址（该地址来源于 /profile 接口的返回值）

![仪表盘](./dashboard.png)

### Explorer

点击功能列表的 "Explorer"入口，在 Explorer 中可以创建和删除数据库、创建和删除超级表和子表，执行SQL语句，查看SQL语句的执行结果。此外，超级管理员还有对数据库的管理权限，其他用户不提供该功能。

具体权限有：

1.查看（提供数据库/超级表/普通表的基本信息）

2.编辑 (编辑数据库/超级表/普通表的信息)

3.数据库管理权限 （仅限超级管理员，该操作可以给指定用户配置数据库管理权限）

4.删除 （删除数据库/超级表/普通表）

5.追加 （选择对应的数据库/超级表/普通表名称直接追加到右侧sql输入区域，避免了手工输入）

例：图为超级管理员权限，展示所有可操作内容

![数据浏览和管理](./data.png)

每个数据库下只有一个STables和Tables文件夹，分别用来添加超级表和普通表

![管理超级表和普通表](./stable.png)

### 用户管理

点击功能列表中的 "Admin" 入口，可以创建用户、对用户进行访问授权、以及删除用户。还能够对当前所管理的集群中的数据进行备份和恢复。也可以配置一个远程 TDengine 的地址进行数据同步。同时也提供了集群信息和 license 信息以及代理信息以供查看。Admin 菜单只有 root 用户才有权限看到

![用户管理](./admin1.png)

![添加用户](./admin2.png)

### 数据接入

点击功能列表中的 "Data In"，可以配置不同类型的数据源，包括 TDengine, PI, OPC UA，InfluxDB， 将它们的数据接入到当前正在被管理的 TDengine 集群中。目前PI和OPC暂不支持用户自定义选择测点。

1.可以对已有的数据源进行删除和编辑操作，以及状态修改的操作

![数据接入](./in.png)

2.目前可以通过启用代理和常规方式添加数据源，代理方式添加只限于当前用户是  root 的情况下。本期提供五种可添加的数据源类型( MQTT 暂不可用)，如图：

![添加数据源](./add-source.png)
![添加数据源](./add-source1.png)

3.添加PI数据类型如图：有红色星号的为必输项，其他的根据需求填写，点击Submit即可创建一条新的PI

![添加Pi数据源](./pi.png)

4.新增opcua如下。红色星号为必输项，填写对应数据点击Submit即可创建一条线的opcua

![添加 OPCA UA 数据源](./opcua.png)

5.创建opcda如下：

![添加 OPC DA 数据源](./opcda.png)

6.创建InfluxDB如下：

![添加 InfluxDB 数据源](./influxdb.png)

### 管理 topic

包括主题，消费者，共享主题和示例代码

1.主题：该区域可以进行Topic的创建和管理

![创建 Topic](./create-topic.png)

2.消费者：展示消费者信息

![消费者](./consumer.png)

3.共享主题：可进行用户的添加

![共享主题](./share-topic.png)

4.示例代码：以文档形式展现

![示例代码](./sample.png)

### 管理 stream

点击功能列表中的 "Stream" 入口，可以创建和管理 stream。

可通过wizard和sql两种方式进行stream的创建。wizard目前只支持聚合函数，且不支持分组

![流计算](./stream.png)

![创建流计算](./create-stream.png)

### 可视化

展示grafana和google data studio文档

### 编程

展示不同连接器语言的文档

### 工具

展示工具文档

### 数据输出

展示taosdump文档