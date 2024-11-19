---
sidebar_label: C#
title: 使用 C# 连接器建立连接
description: 使用 C# 连接器建立和 TDengine Cloud 的连接
---
<!-- exclude -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->
## 创建项目

```bash
dotnet new console -o example
```

## Add C# TDengine Driver class lib

```bash
cd example
vim example.csproj
```

增加下面的 ItemGroup 和 Task 配置到您的工程文件中。

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

## 建立连接

``` XML
{{#include docs/examples/csharp/cloud-example/connect/connect.csproj}}
```

```C#
{{#include docs/examples/csharp/cloud-example/connect/Program.cs}}
```
:::note IMPORTANT
替换代码中的 `<cloud_endpoint>` 和 `<token>`， 这两个值可以从 TDengine Cloud 中的实例 `DSN` 获取。

`DSN` 的格式为 `https(<cloud_endpoint>)/?token=<token>`，获取真实 `DSN` 请登录[TDengine Cloud](https://cloud.taosdata.com) 后点击左边的”编程“菜单，然后选择”C#“。
:::
<!-- exclude-end -->
客户端连接建立连接以后，想了解更多写入数据和查询数据的内容，请参考 [写入](https://docs.taosdata.com/cloud/programming/insert/) 和 [查询](https://docs.taosdata.com/cloud/programming/query/)。

想知道更多通过 REST 接口写入数据的详情，请参考[REST 接口](https://docs.taosdata.com/cloud/programming/client-libraries/rest-api/)。
