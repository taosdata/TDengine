---
sidebar_label: OpenTSDB Telnet 协议
title: Schemaless - OpenTSDB Telnet 协议
description: 写入使用 OpenTSDB Telnet 协议的数据
---

<!-- exclude -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->

这个部分我们会介绍如何通过无服务 OpenTSDB Telnet 协议的 REST 接口往 TDengine Cloud 写入数据。


## 配置

在您的终端命令行运行下面的命令来设置 TDengine Cloud 的令牌和URL为环境变量：

<Tabs defaultValue="bash">
<TabItem value="bash" label="Bash">

```bash
export TDENGINE_CLOUD_TOKEN="<token>"
export TDENGINE_CLOUD_URL="<url>"
```

</TabItem>
<TabItem value="cmd" label="CMD">

```bash
set TDENGINE_CLOUD_TOKEN="<token>"
set TDENGINE_CLOUD_URL="<url>"
```

</TabItem>
<TabItem value="powershell" label="Powershell">

```powershell
$env:TDENGINE_CLOUD_TOKEN="<token>"
$env:TDENGINE_CLOUD_URL="<url>"
```

</TabItem>
</Tabs>

## 插入

您可以使用任何支持 HTTP 协议的客户端通过访问 RESTful 的接口地址 `<cloud_url>/opentsdb/v1/put` 往 TDengine 里面写入兼容 OpenTSDB 的数据。访问地址如下：

```text
/opentsdb/v1/put/telnet/<db>?token=<cloud_token>
```

### 写入样例
```bash
curl --request POST "$TDENGINE_CLOUD_URL/opentsdb/v1/put/telnet/<db_name>?token=$TDENGINE_CLOUD_TOKEN" --data-binary "sys  1479496100 1.3E0 host=web01 interface=eth0"
```

## 使用 SQL 查询样例
- `sys` 是超级表名。
- 您可以像这样通过标签过滤数据：`where host="web01"`.
```bash
curl -L -d "select * from <db_name>.sys where host=\"web01\"" $TDENGINE_CLOUD_URL/rest/sql/test?token=$TDENGINE_CLOUD_TOKEN
```
