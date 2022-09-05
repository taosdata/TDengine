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

``` XML
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net5.0</TargetFramework>
    <Nullable>enable</Nullable>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="TDengine.Connector" Version="3.0.1" />
  </ItemGroup>

</Project>
```

```C#
//{{#include docs/examples/csharp/cloud-example/Connect.cs}}
```
