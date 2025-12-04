---
title: 安装连接代理
sidebar_label: 连接代理
description: 这篇文档主要描述如何为数据写入到 TDengine Cloud 实例创建连接代理。
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

您可以按以下方式安装连接代理。

## 先决条件

- 确保本地计算机与数据源计算机（如 PI Data Archive 和 PI AF Server，可选）位于同一网络或者可以访问 TDengine Cloud。
- 确保本地计算机运行 Linux 或 Windows 操作系统。

## 创建代理

<Tabs>
<TabItem label="Windows" value="windowsagent">

1. 登录 TDengine Cloud 后，在左边菜单打开**数据写入**页面。
2. 在**数据源**选项卡上，单击**连接代理**部分中的**创建新的代理**。
3. 点击**Windows**下载连接代理。
4. 在本地计算机上运行连接代理安装程序，并按照提示操作。
5. 在 TDengine Cloud 中，单击**下一步**。
6. 为您的代理输入一个唯一的名称，然后单击**下一步**以生成一个身份验证令牌。
7. 在本地计算机上，打开 `C:\TDengine\cfg\agent.toml` 文件。
8. 将 TDengine Cloud 中显示的 `endpoint` 和 `token` 的值复制到 `agent.toml` 文件中。
9. 在 TDengine Cloud 中，单击**下一步**，然后单击**完成**。

</TabItem>
<TabItem label="Linux" value="linuxagent">

1. 登录 TDengine Cloud 后，在左边菜单打开**数据写入**页面。
2. 在**数据源**选项卡上，单击**连接代理**部分中的**创建新的代理**。
3. 点击**Linux**下载连接代理。
4. 在本地计算机上解压安装包并运行 `install.sh` 文件。
5. 在 TDengine Cloud 中，单击**下一步**。
6. 为您的代理输入一个唯一的名称，然后单击**下一步**以生成一个身份验证令牌。
7. 在本地计算机上打开 `/etc/taos/agent.toml` 文件。
8. 将 TDengine Cloud 中显示的 `endpoint` 和 `token` 值复制到 `agent.toml` 文件中。
9. 在 TDengine Cloud 中，单击**下一步**，然后单击**完成**。

</TabItem>
</Tabs>

## 启动代理

<Tabs>
<TabItem label="Windows" value="windowsnext">

运行 `sc start taosx-agent` 命令将连接代理作为服务在本地计算机上启动。

</TabItem>

<TabItem label="Linux" value="linuxnext">

运行 `systemctl start taosx-agent` 命令将连接代理作为服务在本地计算机上启动。

</TabItem>

</Tabs>

转到指定的数据源步骤创建数据源。
