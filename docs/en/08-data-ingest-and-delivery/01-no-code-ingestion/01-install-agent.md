---
title: Install taosX-Agent
sidebar_label: Install Agent
description: This document describes how to install taosX-Agent to ingest data into TDengine.
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import { Enterprise } from '../resources/_resources.mdx';

<Enterprise/>

## Overview

If taosX cannot connect directly to your data source, you can install [taosX-Agent](../../12-operations-and-tooling/03-components/07-taosx-agent/index.md) on the network where your data source is located. You can install taosX-Agent on the machine where your data source is located or on a different machine on the same network that can access the data source. taosX can then connect to your data source through taosX-Agent.

:::note

- It is not necessary to install TDengine on the same machine as the data source.
- If taosX can connect directly to your data source, it is not necessary to install taosX-Agent.

:::

## Create Agent

<Tabs>
<TabItem label="Windows" value="windowsagent">

1. In a web browser, open taosExplorer.
1. From the main menu on the left, select **Data In**.
1. Open the **Agent** tab and click **Create New Agent**.
1. Click **Windows** to download taosX-Agent.
1. On your local machine, run the taosX-Agent installer and follow the prompts.
1. In taosExplorer, click **Next**.
1. Enter a unique name for your agent and click **Next** to generate an authentication token.
1. On your local machine, open the `C:\TDengine\cfg\agent.toml` file.
1. Copy the values of `endpoint` and `token` displayed in taosExplorer into the `agent.toml` file.

   ```toml
   endpoint="http://localhost:6055"
   token="eyJ0eX...BhA"
   ```

1. In taosExplorer, click **Next**.
1. On your local machine, open a terminal as an administrator and run the following command:

   ```shell
   sc start taosx-agent
   ```

1. In taosExplorer, click **Check Agent Connection**.
1. If **Success** is displayed, click **Finish**.

</TabItem>

<TabItem label="Linux" value="linuxagent">

1. In a web browser, open taosExplorer.
1. From the main menu on the left, select **Data In**.
1. Open the **Agent** tab and click **Create New Agent**.
1. Click **Linux** to download taosX-Agent.
1. On your local machine, run the taosX-Agent installer and follow the prompts.
1. In taosExplorer, click **Next**.
1. Enter a unique name for your agent and click **Next** to generate an authentication token.
1. On your local machine, open the `/etc/taos/agent.toml` file.
1. Copy the values of `endpoint` and `token` displayed in taosExplorer into the `agent.toml` file.

   ```toml
   endpoint="http://localhost:6055"
   token="eyJ0eX...BhA"
   ```

1. In taosExplorer, click **Next**.
1. On your local machine, open a terminal and run the following command:

   ```shell
   sudo systemctl start taosx-agent
   ```

1. In taosExplorer, click **Check Agent Connection**.
1. If **Success** is displayed, click **Finish**.

</TabItem>
</Tabs>

When you create data in tasks, you can use this agent to connect to your data source.

## Task Configuration and Agent Lifecycle

After taosX-Agent is installed and connected on the data-source side, reuse the same Agent long term. In taosExplorer you can:

- Create, edit, start, and stop Data In tasks (including OPC DA / OPC UA and others)
- Adjust task-level collection settings (such as collection interval)
- Append points and update point CSV / mapping

These operations do not require reinstalling the Agent on the OPC (or other) server host, and do not require generating and redistributing a new “agent install package” just to change points. Tasks communicate with central taosX using the configured `endpoint` and `token`.

Cases that do require changing the Agent host again include: replacing the connection `endpoint` / `token`, upgrading the Agent, or editing host-local `agent.toml` or store-and-forward options. Component reference: [taosX-Agent](../../12-operations-and-tooling/03-components/07-taosx-agent/index.md). You can also manage agents and tasks with SQL; see [Data Ingestion (Xnode)](../../05-tdengine-sql/08-cluster-management/02-xnode.md).

## Related Documentation

For full configuration options, see [taosX-Agent](../../12-operations-and-tooling/03-components/07-taosx-agent/index.md).
