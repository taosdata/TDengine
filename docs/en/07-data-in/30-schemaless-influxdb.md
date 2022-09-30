---
sidebar_label: InfluxDB Line Protocol
title: Schemaless - InfluxDB Line Protocol
description: Insert data in Schemaless Line Protocol
---

<!-- exclude -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->

In this section we will explain how to write into TDengine cloud service using schemaless InfluxDB line protocols over REST interface.

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

## Insert

You can use any client that supports the http protocol to access the RESTful interface address `<cloud_url>/influxdb/v1/write` to write data in InfluxDB compatible format to TDengine. The EndPoint is as follows:

```text
/influxdb/v1/write?db=<db_name>&token=<cloud_token>
```

Support InfluxDB query parameters as follows.

+ `db` Specifies the database name used by TDengine
+ `precision` The time precision used by TDengine
    - ns - nanoseconds
    - u - microseconds
    - ms - milliseconds
    - s - seconds
    - m - minutes
    - h - hours

## Insert Example
```bash
curl --request POST "$TDENGINE_CLOUD_URL/influxdb/v1/write?db=<db_name>&token=$TDENGINE_CLOUD_TOKEN&precision=ns" --data-binary "measurement,host=host1 field1=2i,field2=2.0 1577846800001000001"
```
  
## Query Example with SQL
- `measurement` is the super table name.
- you can filter data by tag, like:`where host="host1"`.
```bash
curl -L -d "select * from <db_name>.measurement where host=\"host1\"" $TDENGINE_CLOUD_URL/rest/sql/test?token=$TDENGINE_CLOUD_TOKEN
```
