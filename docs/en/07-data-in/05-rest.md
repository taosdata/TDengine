---
sidebar_label: REST
title: REST
description: Insert data using REST API
---

<!-- exclude -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->

In this section we will explain how to write into TDengine cloud service using REST API.

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

Following command below show how to insert data into the table `d1001` of the database `test` via the command line utility `curl`.

```bash
curl -L \
  -d "INSERT INTO d1001 VALUES (1538548685000, 10.3, 219, 0.31)" \
  $TDENGINE_CLOUD_URL/rest/sql/test?token=$TDENGINE_CLOUD_TOKEN
```

Please refer to [REST-API](https://docs.tdengine.com/reference/rest-api/) for detailed documentation.
