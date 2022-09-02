---
sidebar_label: C#
title: TDengine C# Connector
description: Detailed guide for C# Connector
---

 `TDengine.Connector` is the official C# connector for TDengine. C# developers can develop applications to access TDengine instance data.

The source code for `TDengine.Connector` is hosted on [GitHub](https://github.com/taosdata/taos-connector-dotnet/tree/3.0).

## Installation

### Pre-installation

Install the .NET deployment SDK.

### Add TDengine.Connector through Nuget

```bash
dotnet add package TDengine.Connector
```

## Establishing a connection

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

``` C#
{{#include docs/examples/csharp/cloud-example/connect/Program.cs}}
```

## Usage examples

### Basic Insert and Query

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
{{#include docs/examples/csharp/cloud-example/usage/Program.cs}}
```

### STMT Insert

``` XML
<Project Sdk="Microsoft.NET.Sdk">

  <PropertyGroup>
    <OutputType>Exe</OutputType>
    <TargetFramework>net5</TargetFramework>
    <Nullable>enable</Nullable>
  </PropertyGroup>

  <ItemGroup>
    <PackageReference Include="TDengine.Connector" Version="3.0.1" />
  </ItemGroup>

</Project>

```

```C#
{{#include docs/examples/csharp/cloud-example/stmt/Program.cs}}
```

## Important Updates

| TDengine.Connector | Description |
| ------------------------- | ---------------------------------------------------------------- |
| 3.0.1 | Support connect to TDengine cloud service

## API Reference

[API Reference](https://docs.taosdata.com/api/td2.0-connector/)
