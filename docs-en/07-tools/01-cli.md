---
title: TDengine Command Line (CLI)
sidebar_label: TDengine CLI
description: Instructions and tips for using the TDengine CLI to connect TDengine Cloud
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

The TDengine command-line interface (hereafter referred to as `TDengine CLI`) is the most simplest way for users to manipulate and interact with TDengine instances.

## Installation

To run TDengine CLI to access TDengine cloud, please install [TDengine client installation package](/download/TDengine-client-2.6.0.2-Linux-x64.tar.gz) first.

<Tabs defaultValue="ConfigOnLinux">
<TabItem value="ConfigOnLinux" label="Config on Linux">

Run this command in your Linux terminal to save your URL and token as variables:

```
export TDENGINE_CLOUD_URL=<URL>
export TDENGINE_CLOUD_TOKEN=<token>
```

</TabItem>
<TabItem value="ConfigOnWindows" label="Config on Windows (coming soon)">

Run this command in your Windows terminal to save your URL and token as variables:

```
set TDENGINE_CLOUD_URL=<URL>
set TDENGINE_CLOUD_TOKEN=<token>
```

</TabItem>
<TabItem value="ConfigOnMac" label="Config on Mac (coming soon)">

Run this command in your Mac terminal to save your URL and token as variables:

```
export TDENGINE_CLOUD_URL=<URL>
export TDENGINE_CLOUD_TOKEN=<token>
```

</TabItem>
</Tabs>

<Tabs defaultValue="ConnectOnLinux">
<TabItem value="ConnectOnLinux" label="Connect on Linux">

To access the TDengine Cloud, you can execute below command from a Linux terminal.

```
taos -R -h $TDENGINE_CLOUD_URL -t $TDENGINE_CLOUD_TOKEN
```

</TabItem>
<TabItem value="ConnectOnWindows" label="Connect on Windows (coming soon)">

To access the TDengine Cloud, you can execute below command from a Windows terminal.

```
taos -R -h %TDENGINE_CLOUD_URL% -t %TDENGINE_CLOUD_TOKEN%
```

</TabItem>
<TabItem value="ConnectOnMac" label="Connect on Mac (coming soon)">

To access the TDengine Cloud, you can execute below command from a Mac terminal.

```
taos -R -h $TDENGINE_CLOUD_URL -t $TDENGINE_CLOUD_TOKEN
```

</TabItem>
</Tabs>

## using TDengine CLI

TDengine CLI will display a welcome message and version information if it successfully connected to the TDengine service. If it fails, TDengine CLI will print an error message. See [FAQ](/train-faq/faq) to solve the problem of terminal connection failure to the server. The TDengine CLI prompts as follows:

```cmd
Welcome to the TDengine shell from Linux, Client Version:2.6.0.2
Copyright (c) 2022 by TAOS Data, Inc. All rights reserved.

taos>
```

After entering the TDengine CLI, you can execute various SQL commands, including inserts, queries, or administrative commands. Please see the [official document](https://docs.tdengine.com/reference/taos-shell#execute-sql-script-file) for more details.

