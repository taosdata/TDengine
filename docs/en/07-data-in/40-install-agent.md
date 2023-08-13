---
title: Install Connection Agent
sidebar_label: Connection Agent
description: This document describes how to install the connection agent to ingest data into TDengine.
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

You can install the connection agent to ingest PI System data into TDengine.

## Prerequisites

- Ensure that your local machine is located on the same network as your PI Data Archive and PI AF Server (optional).
- Ensure that your local machine is running Linux or Windows.

## Procedure

<Tabs>
<TabItem label="Windows" value="windowsagent">

1. In TDengine Cloud, open **Data In**.
2. On the **Data Sources** tab, click **Create New Agent** in the **Connection Agents** section.
3. Click **Windows** to download the connection agent.
4. On your local machine, run the connection agent installer and follow the prompts.
5. In TDengine Cloud, click **Next**.
6. Enter a unique name for your agent and click **Next** to generate an authentication token.
7. On your local machine, open the `C:\Program Files\taosX\config\agent.toml` file.
8. Copy the values of `endpoint` and `token` displayed in TDengine Cloud into the `agent.toml` file.
9. In TDengine Cloud, click **Next** and click **Finish**.

</TabItem>

<TabItem label="Linux" value="linuxagent">

1. In TDengine Cloud, open **Data In**.
2. On the **Data Sources** tab, click **Create New Agent** in the **Connection Agents** section.
3. Click **Linux** to download the connection agent.
4. On your local machine, decompress the installation package and run the `install.sh` file.
5. In TDengine Cloud, click **Next**.
6. Enter a unique name for your agent and click **Next** to generate an authentication token.
7. On your local machine, open the `/etc/taos/agent.toml` file.
8. Copy the values of `endpoint` and `token` displayed in TDengine Cloud into the `agent.toml` file.
9. In TDengine Cloud, click **Next** and click **Finish**.

</TabItem>

</Tabs>

## What to Do Next

<Tabs>
<TabItem label="Windows" value="windowsnext">

1. Run the `sc start taosx-agent` command to start the connection agent as a service on your local machine.
2. [Ingest Data from PI System](../pi-system/).

</TabItem>

<TabItem label="Linux" value="linuxnext">

1. Run the `systemctl start taosx-agent` command to start the connection agent as a service on your local machine.
2. [Ingest Data from PI System](../pi-system/).

</TabItem>

</Tabs>