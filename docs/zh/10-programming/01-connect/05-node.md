---
sidebar_label: Node.js
title: 使用 Node.js 连接器建立连接
description: 使用 Node.js 连接器建立和 TDengine Cloud 的连接
---
<!-- exclude -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->
## 安装连接器

```bash
npm install @tdengine/rest
```
## 配置

在您的终端里面执行下面的命令设置 TDengine Cloud 令牌为环境变量：

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
获取 TDengine Cloud 的令牌和 URL，可以登录[TDengine Cloud](https://cloud.taosdata.com) 后点击左边的”编程“菜单，然后选择”Node.js“。
:::
<!-- exclude-end -->

## Connect

```javascript
{{#include docs/examples/node/connect.js}}
```

客户端连接建立连接以后，想了解更多写入数据和查询数据的内容，请参考 <https://docs.taosdata.com/cloud/data-in/insert-data/> and <https://docs.taosdata.com/cloud/data-out/query-data/>.

想知道更多通过 REST 接口写入数据的详情，请参考[REST 接口](https://docs.taosdata.com/cloud/programming/connector/rest-api/).
