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

## Connect

``` XML
{{#include docs/examples/csharp/cloud-example/connect/connect.csproj}}
```

```C#
{{#include docs/examples/csharp/cloud-example/connect/Program.cs}}
```
<!-- exclude -->
:::note IMPORTANT
Replace `<cloud_endpoint>` and `<cloud_token>` from the real TDengine cloud DSN like `taos://cloud_endpoint:6041?token=cloud_token`. To obtain the real value, please log in [TDengine Cloud](https://cloud.tdengine.com) and click "Programming" on the left menu, then select "C#".

:::
<!-- exclude-end -->

The client connection is then established. For how to write data and query data, please refer to [Insert](https://docs.tdengine.com/cloud/programming/insert/) and [Query](https://docs.tdengine.com/cloud/programming/query/).

For more details about how to write or query data via REST API, please check [REST API](https://docs.tdengine.com/cloud/programming/connector/rest-api/).
