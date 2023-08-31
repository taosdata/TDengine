---
sidebar_label: OpenTSDB Telnet Protocol
title: Schemaless - OpenTSDB Telnet Protocol
description: Insert data in OpenTSDB Telnet Protocol
---

<!-- exclude -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->

In this section we will explain how to write into TDengine cloud service using schemaless OpenTSDB Telnet protocols over REST interface.

## Config

Run this command in your terminal to save the TDengine cloud token and URL as variables:

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

### Insert

You can use any client that supports the http protocol to access the RESTful interface address `<cloud_url>/opentsdb/v1/put` to write data in OpenTSDB compatible format to TDengine. The EndPoint is as follows:

```text
/opentsdb/v1/put/telnet/<db>?token=<cloud_token>
```

### Insert Example
```bash
curl --request POST "$TDENGINE_CLOUD_URL/opentsdb/v1/put/telnet/<db_name>?token=$TDENGINE_CLOUD_TOKEN" --data-binary "sys  1479496100 1.3E0 host=web01 interface=eth0"
```

## Query Example with SQL
- `sys` is the super table name.
- you can filter data by tag, like:`where host="web01"`.
```bash
curl -L -d "select * from <db_name>.sys where host=\"web01\"" $TDENGINE_CLOUD_URL/rest/sql/test?token=$TDENGINE_CLOUD_TOKEN
```
