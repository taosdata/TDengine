---
title: 安装 taosX-Agent
sidebar_label: 安装 Agent
description: 说明如何安装 taosX-Agent，以便将数据接入 TDengine
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

## 概述

当 taosX 无法直接连接数据源时，可以在数据源所在网络安装 [taosX-Agent](../../12-operations-and-tooling/03-components/07-taosx-agent/index.md)。可将 taosX-Agent 安装在数据源所在主机，或安装在同一网络中且能访问该数据源的其他主机。之后 taosX 即可通过 taosX-Agent 连接数据源。

:::note

- 数据源所在主机不必安装 TDengine。
- 若 taosX 可直接连接数据源，则无需安装 taosX-Agent。

:::

## 创建代理

<Tabs>
<TabItem label="Windows" value="windowsagent">

1. 在浏览器中打开 taosExplorer。
1. 在左侧主菜单中选择 **数据写入**。
1. 打开 **代理** 标签页，点击 **+创建新的代理**。
1. 点击 **Windows** 下载 taosX-Agent。
1. 在本机运行 taosX-Agent 安装程序，并按提示完成安装。
1. 在 taosExplorer 中点击 **下一步**。
1. 为代理输入唯一名称，点击 **下一步** 生成认证 token。
1. 在本机打开 `C:\TDengine\cfg\agent.toml` 文件。
1. 将 taosExplorer 界面中显示的 `endpoint` 与 `token` 值复制到 `agent.toml` 文件中。

   ```toml
   endpoint="http://localhost:6055"
   token="eyJ0eX...BhA"
   ```

1. 在 taosExplorer 中点击 **下一步**。
1. 在本机以管理员身份打开终端，执行以下命令：

   ```shell
   sc start taosx-agent
   ```

1. 在 taosExplorer 中点击 **检查代理是否连接正常**。
1. 若显示 **正常**，点击 **完成**。

</TabItem>

<TabItem label="Linux" value="linuxagent">

1. 在浏览器中打开 taosExplorer。
1. 在左侧主菜单中选择 **数据写入**。
1. 打开 **代理** 标签页，点击 **+创建新的代理**。
1. 点击 **Linux** 下载 taosX-Agent。
1. 在本机运行 taosX-Agent 安装程序，并按提示完成安装。
1. 在 taosExplorer 中点击 **下一步**。
1. 为代理输入唯一名称，点击 **下一步** 生成认证 token。
1. 在本机打开 `/etc/taos/agent.toml` 文件。
1. 将 taosExplorer 界面中显示的 `endpoint` 与 `token` 值复制到 `agent.toml` 文件中。

   ```toml
   endpoint="http://localhost:6055"
   token="eyJ0eX...BhA"
   ```

1. 在 taosExplorer 中点击 **下一步**。
1. 在本机打开终端，执行以下命令：

   ```shell
   sudo systemctl start taosx-agent
   ```

1. 在 taosExplorer 中点击 **检查代理是否连接正常**。
1. 若显示 **正常**，点击 **完成**。

</TabItem>
</Tabs>

之后在创建数据写入任务时，即可选择该代理连接数据源。

## 任务配置与 Agent 生命周期

taosX-Agent 在数据源侧安装并连通后，长期复用同一 Agent 即可。在 taosExplorer 中可以：

- 创建、编辑、启停 Data In 任务（含 OPC DA / OPC UA 等）
- 调整任务级采集参数（如采集间隔）
- 追加点位、更新点位 CSV / 映射

上述操作不要求在 OPC 等服务器主机上重新安装 Agent，也不要求重新生成并下发一套新的“代理安装包”才能改点位。任务通过已配置的 `endpoint` 与 `token` 与中央 taosX 通信。

需要再次改动 Agent 本机的情况包括：更换连接 `endpoint` / `token`、升级 Agent 版本、修改本机 `agent.toml` 或存储转发等本机选项。组件级说明见 [taosX-Agent](../../12-operations-and-tooling/03-components/07-taosx-agent/index.md)；亦可通过 SQL 管理 Agent 与任务，见 [数据接入（Xnode）](../../05-tdengine-sql/08-cluster-management/02-xnode.md)。

## 相关文档

完整配置项说明见 [taosX-Agent](../../12-operations-and-tooling/03-components/07-taosx-agent/index.md)。
