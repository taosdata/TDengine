---
sidebar_label: Node.js
title: Connect with Node.js Connector
description: Connect to TDengine cloud service using Node.JS connector
---
<!-- exclude -->
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<!-- exclude-end -->
## Install Connector

```bash
npm install @tdengine/rest
```
## Config

Run this command in your terminal to save TDengine cloud token as variables:

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
To obtain the value of cloud token and URL, please log in [TDengine Cloud](https://cloud.tdengine.com) and click "Programming" on left menu, then select "Node.js".

:::
<!-- exclude-end -->

## Connect

```javascript
{{#include docs/examples/node/connect.js}}
```

For how to write data and query data, please refer to <https://docs.tdengine.com/cloud/data-in/insert-data/> and <https://docs.tdengine.com/cloud/data-out/query-data/>.

For more details about how to write or query data via REST API, please check [REST API](https://docs.tdengine.com/cloud/programming/connector/rest-api/).
