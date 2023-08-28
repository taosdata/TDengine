---
sidebar_label: REST API
title: REST API
description: 使用 RESTful 接口建立和 TDengine Cloud 的连接
---

<!-- exclude -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->
## 配置

在您的终端里面执行下面的命令来保存 TDengine Cloud 的 URL 和令牌到系统的环境变量里面：

<Tabs defaultValue="bash">
<TabItem value="bash" label="Bash">

```bash
export TDENGINE_CLOUD_TOKEN="<token>"
export TDENGINE_CLOUD_URL="<url>"
```

</TabItem>
<TabItem value="cmd" label="CMD">

```bash
set TDENGINE_CLOUD_TOKEN=<token>
set TDENGINE_CLOUD_URL=<url>
```

</TabItem>
<TabItem value="powershell" label="Powershell">

```powershell
$env:TDENGINE_CLOUD_TOKEN='<token>'
$env:TDENGINE_CLOUD_URL='<url>'
```

</TabItem>
</Tabs>

<!-- exclude -->
:::note
替换 <token\> 和 <url\> 为 TDengine Cloud 的令牌和 URL 。
获取 TDengine Cloud 的令牌和 URL，可以登录[TDengine Cloud](https://cloud.taosdata.com) 后点击左边的”编程“菜单，然后选择”REST API“。
:::
<!-- exclude-end -->
## 使用

TDengine REST API 是使用标准的 HTTP 协议并提供一直简易的方式访问 TDengine 实例。比如下面的命令，通过 URL，令牌和 SQL 命令来组装 HTTP 请求，并使用命令行工具 `curl` 来运行这个命令。

```bash
curl -L \
  -d "select name, ntables, status from information_schema.ins_databases;" \
  $TDENGINE_CLOUD_URL/rest/sql?token=$TDENGINE_CLOUD_TOKEN
```
