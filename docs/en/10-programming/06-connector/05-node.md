---
sidebar_label: Node.JS
title: TDengine Node.JS Connector
description: Detailed guide for Node.JS Connector
---

 `@tdengine/rest` are the official Node.js language connectors for TDengine. Node.js developers can develop applications to access TDengine instance data. `@tdengine/rest` is a **REST connector** that connects to TDengine instances via the REST API.

The source code for the Node.js connectors is located on [GitHub](https://github.com/taosdata/taos-connector-node/tree/3.0).

## Version support

Please refer to [version support list](/reference/connector#version-support)

## Installation steps

### Pre-installation

Install the Node.js development environment
### Install via npm

```bash
npm install @tdengine/rest
```
## Establishing a connection

```javascript
{{#include docs/examples/node/connect.js}}
```

## Usage examples

```javascript
{{#include docs/examples/node/reference_example.js:usage}}
```

## Frequently Asked Questions

1. Using REST connections requires starting taosadapter.

   ```bash
   sudo systemctl start taosadapter
   ```

2. Node.js versions

   `@tdengine/client` supports Node.js v10.9.0 to 10.20.0 and 12.8.0 to 12.9.1.

3. "Unable to establish connection", "Unable to resolve FQDN"

  Usually, the root cause is an incorrect FQDN configuration. You can refer to this section in the [FAQ](https://docs.tdengine.com/2.4/train-faq/faq/#2-how-to-handle-unable-to-establish-connection) to troubleshoot.

## Important update records
| package name         | version | TDengine version    | Description                                                                      |
|----------------------|---------|---------------------|---------------------------------------------------------------------------|
| @tdengine/rest | 3.0.0   | 3.0.0               | Supports TDengine 3.0. Not compatible with TDengine 2.x.                                               |
| td2.0-rest-connector | 1.0.7   | 2.4.x；2.5.x；2.6.x | Removed default port 6041。                                                       |
| td2.0-rest-connector | 1.0.6   | 2.4.x；2.5.x；2.6.x | Fixed affectRows bug with create, insert, update, and alter. |
| td2.0-rest-connector | 1.0.5   | 2.4.x；2.5.x；2.6.x | Support cloud token                                                  |
| td2.0-rest-connector  | 1.0.3  | 2.4.x；2.5.x；2.6.x | Supports connection management, standard queries, system information, error information, and continuous queries          |

## API Reference

[API Reference](https://docs.taosdata.com/api/td2.0-connector/)
