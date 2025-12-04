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

## 配置

在您的终端里面执行下面的命令设置 TDengine Cloud 的网关地址和令牌为环境变量。

<Tabs defaultValue="bash">
<TabItem value="bash" label="Bash">

```bash
export TDENGINE_CLOUD_ENDPOINT="<cloud_endpoint>"
export TDENGINE_CLOUD_TOKEN="<cloud_token>"
```

</TabItem>
<TabItem value="cmd" label="CMD">

```shell
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
替换 `<cloud_endpoint>` 和 `<cloud_token>` 为 TDengine Cloud 的网关地址和令牌。
获取 TDengine Cloud 的网关地址和令牌，可以登录[TDengine Cloud](https://cloud.taosdata.com) 后点击左边的”编程“菜单，然后选择”C#“。
:::

## 建立连接

``` XML
{{#include docs/examples/csharp/cloud-example/connect/connect.csproj}}
```

```C#
{{#include docs/examples/csharp/cloud-example/connect/Program.cs}}
```

客户端连接建立连接以后，想了解更多写入数据和查询数据的内容，请参考 [写入](https://docs.taosdata.com/cloud/programming/insert/) 和 [查询](https://docs.taosdata.com/cloud/programming/query/)。

想知道更多通过 REST 接口写入数据的详情，请参考[REST 接口](https://docs.taosdata.com/cloud/programming/client-libraries/rest-api/)。
