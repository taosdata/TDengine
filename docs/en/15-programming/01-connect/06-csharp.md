---
sidebar_label: C#
title: Connect with C# Connector
description: Connect to TDengine cloud service using C# connector
---
<!-- exclude -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->
## Create Project

```bash
dotnet new console -o example
```

## Add C# TDengine Driver class lib

```bash
cd example
dotnet add package TDengine.Connector
```

## Config

Run this command in your terminal to save TDengine cloud token as variables:

<Tabs defaultValue="bash">
<TabItem value="bash" label="Bash">

```bash
export TDENGINE_CLOUD_DSN="<DSN>"
```

</TabItem>
<TabItem value="cmd" label="CMD">

```bash
set TDENGINE_CLOUD_DSN="<DSN>"
```

</TabItem>
<TabItem value="powershell" label="Powershell">

```powershell
$env:TDENGINE_CLOUD_DSN="<DSN>"
```

</TabItem>
</Tabs>


<!-- exclude -->
:::note
Replace  <DSN\> with real TDengine cloud DSN. To obtain the real value, please log in [TDengine Cloud](https://cloud.tdengine.com) and click "Connector" and then select "C#".

:::
<!-- exclude-end -->

## Connect


```C#
{{#include docs/examples/csharp/cloud-example/connect/Program.cs}}
```

The client connection is then established. For how to write data and query data, please refer to <https://docs.tdengine.com/cloud/data-in/insert-data/> and <https://docs.tdengine.com/cloud/data-out/query-data/>.

For more details about how to write or query data via REST API, please check [REST API](https://docs.tdengine.com/cloud/programming/connector/rest-api/).