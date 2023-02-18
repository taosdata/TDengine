---
sidebar_label: REST API
title: REST API
description: Connect to TDengine Cloud Service through RESTful API
---

<!-- exclude -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->
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
Replace  <token\> and <url\> with cloud token and URL.
To obtain the value of cloud token and URL, please log in [TDengine Cloud](https://cloud.tdengine.com) and click "Programming" on the left menu, then select "REST API".

:::
<!-- exclude-end -->  
## Usage

The TDengine REST API is based on standard HTTP protocol and provides an easy way to access TDengine. As an example, the code below is to construct an HTTP request with the URL, the token and an SQL command and run it with the command line utility `curl`.

```bash
curl -L \
  -d "select name, ntables, status from information_schema.ins_databases;" \
  $TDENGINE_CLOUD_URL/rest/sql?token=$TDENGINE_CLOUD_TOKEN
```
