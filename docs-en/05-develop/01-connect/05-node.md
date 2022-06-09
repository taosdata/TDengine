---
sidebar_label: Node.js
title: Connect with Node.js Connector
---

## Install Connector



## Config

Run this command in your terminal to save TDengine cloud token as variables:

```bash
export TDENGINE_TOKEN=<token>
```


## Connect


```python
const { options, connect } = require("td2.0-rest-connector");

async function test() {
  options.url = "https://cloud.tdengine.com";
  options.token = process.env.TDENGIN_TOKEN;
  let conn = connect(options);
}
test();
```

