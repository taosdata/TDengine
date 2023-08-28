---
title: TDengine 命令行(CLI)
sidebar_label: TDengine CLI
description: TDengine CLI 的使用说明和技巧
---

<!-- exclude -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->

TDengine 命令行程序（以下简称 TDengine CLI）是用户操作 TDengine 实例并与之交互的最简洁最常用的方式。

## 安装

运行 TDengine CLI 来访问 TDengine Cloud ，请首先下载和安装最新的 [TDengine 客户端安装包](https://docs.taosdata.com/releases/tdengine/)。

## 配置

<Tabs defaultValue="linux" groupId="sys">
<TabItem value="linux" label="在Linux上面配置">

在您的 Linux 终端里面执行下面的命令设置 TDengine Cloud 的 DSN 为环境变量：

```bash
export TDENGINE_CLOUD_DSN="<DSN>"
```

</TabItem>
<TabItem value="windows" label="Config on Windows (beta)" groupId="sys">

在您的 Windows CMD 里面执行下面的命令设置 TDengine Cloud 的 DSN 为环境变量：

```cmd
set TDENGINE_CLOUD_DSN=<DSN>
```

或者在您的 Windows PowerShell 里面执行下面的命令设置 TDengine Cloud 的 DSN 为环境变量：

```cmd
$env:TDENGINE_CLOUD_DSN='<DSN>'
```

</TabItem>
<TabItem value="mac" label="Config on Mac (beta)" groupId="sys">

在您的 Mac 里面执行下面的命令设置 TDengine Cloud 的 DSN 为环境变量：

```bash
export TDENGINE_CLOUD_DSN="<DSN>"
```

</TabItem>
</Tabs>

<!-- exclude -->
:::note
获取真实的 `DSN` 的值，请登录[TDengine Cloud](https://cloud.taosdata.com) 后点击左边的”工具“菜单，然后选择”TDengine CLI“。
:::
<!-- exclude-end -->

## 建立连接

<Tabs defaultValue="linux" groupId="sys">
<TabItem value="linux" label="在 Linux 上面建立连接">

如果您已经设置了环境变量，您只需要立即执行 `taos` 命令就可以访问 TDengine Cloud 实例。

```
taos
```

如果您没有设置 TDengine Cloud 实例的环境变量，或者您想访问其他 TDengine Cloud 实例，您可以使用下面的命令 `taos -E <DSN>`来执行：

```
taos -E $TDENGINE_CLOUD_DSN
```

</TabItem>
<TabItem value="windows" label="在 Windows 上面建立连接 (测试版本)">

如果您已经设置了环境变量，您只需要立即执行 `taos` 命令就可以访问 TDengine Cloud 实例。

```powershell
taos.exe
```

如果您没有设置 TDengine Cloud 实例的环境变量，或者您想访问其他 TDengine Cloud 实例，您可以使用下面的命令 `taos -E <DSN>`来执行：

```powershell
taos.exe -E $TDENGINE_CLOUD_DSN
```

</TabItem>
<TabItem value="mac" label="在 Mac 上面建立连接 (测试版本)">

如果您已经设置了环境变量，您只需要立即执行 `taos` 命令就可以访问 TDengine Cloud 实例。

```bash
taos
```

如果您没有设置 TDengine Cloud 实例的环境变量，或者您想访问其他 TDengine Cloud 实例，您可以使用下面的命令 `taos -E <DSN>`来执行：

```bash
taos -E $TDENGINE_CLOUD_DSN
```

</TabItem>
</Tabs>

## 使用 TDengine CLI

如果成功连接上 TDengine 服务，TDengine CLI 会显示一个欢迎的消息和版本信息。如果失败了，TDengine CLI 会打印失败消息。TDengine CLI 打印的成功消息如下：

```text
Welcome to the TDengine shell from Linux, Client Version:3.0.0.0
Copyright (c) 2022 by TAOS Data, Inc. All rights reserved.

Successfully connect to cloud.tdengine.com:8085 in restful mode

taos>
```

进入 TDengine CLI 以后，您就可以执行大量的 SQL 命令来进行插入，查询或者进行管理。详情请参考[官方文档](https://docs.taosdata.com/reference/taos-shell#execute-sql-script-file)。
