---
title: Install taosX-Agent
sidebar_label: Install Agent
description: This document describes how to install taosX-Agent to ingest data into TDengine.
---

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import Enterprise from '../../assets/resources/_enterprise.mdx';

<Enterprise/>

If TDengine TSDB cannot connect directly to your data source, you can install [taosX-Agent](../../14-reference/01-components/05-taosx-agent.md) on the network where your data source is located. You can install taosX-Agent on the machine where your data source is located or on a different machine on the same network that can access the data source. TDengine TSDB can then connect to your data source through taosX-Agent.

:::note

- It is not necessary to install TDengine TSDB on the same machine as the data source.
- If TDengine TSDB can connect directly to your data source, it is not necessary to install taosX-Agent.

:::

## Create Agent

<Tabs>
<TabItem label="Windows" value="windowsagent">

1. In a web browser, access TDengine TSDB.
1. From the main menu on the left, select **Data In**.
1. Open the **Agent** tab and click **Create New Agent**.
1. Click **Windows** to download taosX-Agent.
1. On your local machine, run the taosX-Agent installer and follow the prompts.
1. In TDengine TSDB, click **Next**.
1. Enter a unique name for your agent and click **Next** to generate an authentication token.
1. On your local machine, open the `C:\TDengine\cfg\agent.toml` file.
1. Copy the values of `endpoint` and `token` displayed in TDengine TSDB into the `agent.toml` file.

   ```toml
   endpoint="http://localhost:6055"
   token="eyJ0eX...BhA"
   ```

1. In TDengine TSDB, click **Next**.
1. On your local machine, open a terminal as an administrator and run the following command:

   ```shell
   sc start taosx-agent
   ```

1. In TDengine TSDB, click **Check Agent Connection**.
1. If **Success** is displayed, click **Finish**.

</TabItem>

<TabItem label="Linux" value="linuxagent">

1. In a web browser, access TDengine TSDB.
1. From the main menu on the left, select **Data In**.
1. Open the **Agent** tab and click **Create New Agent**.
1. Click **Windows** to download taosX-Agent.
1. On your local machine, run the taosX-Agent installer and follow the prompts.
1. In TDengine TSDB, click **Next**.
1. Enter a unique name for your agent and click **Next** to generate an authentication token.
1. On your local machine, open the `/etc/taos/agent.toml` file.
1. Copy the values of `endpoint` and `token` displayed in TDengine TSDB into the `agent.toml` file.

   ```toml
   endpoint="http://localhost:6055"
   token="eyJ0eX...BhA"
   ```

1. In TDengine TSDB, click **Next**.
1. On your local machine, open a terminal and run the following command:

   ```shell
   sudo systemctl start taosx-agent
   ```

1. In TDengine TSDB, click **Check Agent Connection**.
1. If **Success** is displayed, click **Finish**.

</TabItem>
</Tabs>

When you create data in tasks, you can use this agent to connect to your data source.

For full configuration options, see [taosX-Agent](../../14-reference/01-components/05-taosx-agent.md).
