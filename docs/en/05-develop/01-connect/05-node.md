---
sidebar_label: Node.js
title: Connect with Node.js Connector
pagination_next: develop/insert-data
---

## Install Connector

```bash
npm i td2.0-rest-connector
```
## Config

Run this command in your terminal to save TDengine cloud token as variables:

```bash
export TDENGINE_CLOUD_TOKEN=<token>
export TDENGINE_CLOUD_URL=<url>
```

<!-- exclude -->
:::note
Replace  <token\> and <url\> with cloud token and URL.
To obtain the value of cloud token and URL, please log in [TDengine Cloud](https://cloud.tdengine.com) and click "Connector" and then select "Node.js".

:::
<!-- exclude-end -->

## Connect

```javascript
const { options, connect } = require("td2.0-rest-connector");

async function test() {
  options.url = process.env.TDENGINE_CLOUD_URL;
  options.token = process.env.TDENGINE_CLOUD_TOKEN;
  let conn = connect(options);
}

test();
```

