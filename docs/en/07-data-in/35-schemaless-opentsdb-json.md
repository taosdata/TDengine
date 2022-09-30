---
sidebar_label: OpenTSDB JSON Protocol
title: Schemaless - OpenTSDB JSON Protocol
description: Insert data in OpenTSDB JSON Protocol
---

<!-- exclude -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->

In this section we will explain how to write into TDengine cloud service using schemaless OpenTSDB JSON protocols over REST interface.

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
/opentsdb/v1/put/json/<db>?token=<cloud_token>
```
### Insert Example
```bash
curl --request POST "$TDENGINE_CLOUD_URL/opentsdb/v1/put/json/<db_name>?token=$TDENGINE_CLOUD_TOKEN" --data-binary "{\"metric\":\"meter_current\",\"timestamp\":1646846400,\"value\":10.3,\"tags\":{\"groupid\":2,\"location\":\"Beijing\",\"id\":\"d1001\"}}"
```
## Query Example with SQL
`meter_current` is the super table name.
you can filter data by tag, like:`where groupid=2`.
```bash
curl -L -d "select * from <db_name>.meter_current where groupid=2" $TDENGINE_CLOUD_URL/rest/sql/test?token=$TDENGINE_CLOUD_TOKEN
```
