---
sidebar_label: C#
title: Connect with C#
description: This document describes how to connect to TDengine Cloud using the C# client library.
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
vim example.csproj
```

Add following ItemGroup and Task to your project file.

```XML
  <ItemGroup>
    <PackageReference Include="TDengine.Connector" Version="3.1.*" GeneratePathProperty="true" />
  </ItemGroup>
  <Target Name="copyDLLDependency" BeforeTargets="BeforeBuild">
    <ItemGroup>
      <DepDLLFiles Include="$(PkgTDengine_Connector)\runtimes\**\*.*" />
    </ItemGroup>
    <Copy SourceFiles="@(DepDLLFiles)" DestinationFolder="$(OutDir)" />
  </Target>
```

```bash
dotnet add package TDengine.Connector
```

## Config

Run this command in your terminal to save TDengine cloud endpoint and token as variable:

<Tabs defaultValue="bash">
<TabItem value="bash" label="Bash">

```bash
export TDENGINE_CLOUD_ENDPOINT="<cloud_endpoint>"
export TDENGINE_CLOUD_TOKEN="<cloud_token>"
```

</TabItem>
<TabItem value="cmd" label="CMD">

```bash
set TDENGINE_CLOUD_ENDPOINT=<cloud_endpoint>
set TDENGINE_CLOUD_TOKEN=<cloud_token>
```

</TabItem>
<TabItem value="powershell" label="Powershell">

```powershell
$env:TDENGINE_CLOUD_ENDPOINT='<cloud_endpoint>'
$env:TDENGINE_CLOUD_TOKEN='<cloud_token>'
```

</TabItem>
</Tabs>

<!-- exclude -->
:::note IMPORTANT
Replace &lt;cloud_endpoint&gt; and &lt;cloud_token&gt; from the real TDengine cloud DSN like `https://cloud_endpoint?token=cloud_token`. To obtain the real value, please log in [TDengine Cloud](https://cloud.tdengine.com) and click "Programming" on the left menu, then select "C#".

:::
<!-- exclude-end -->

## Connect

``` XML
{{#include docs/examples/csharp/cloud-example/connect/connect.csproj}}
```

```C#
{{#include docs/examples/csharp/cloud-example/connect/Program.cs}}
```

The client connection is then established. For how to write data and query data, please refer to [Insert](https://docs.tdengine.com/cloud/programming/insert/) and [Query](https://docs.tdengine.com/cloud/programming/query/).

For more details about how to write or query data via REST API, please check [REST API](https://docs.tdengine.com/cloud/programming/connect/rest-api/).
