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

## Connect

Please copy the code below to your editor and run it.

```bash
curl -L \
  -d "select name, ntables, status from information_schema.ins_databases;" \
  $TDENGINE_CLOUD_URL/rest/sql?token=$TDENGINE_CLOUD_TOKEN
```

