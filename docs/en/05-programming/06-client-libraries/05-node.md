---
sidebar_label: Node.JS
title: TDengine Node.JS Client Library
description: This document describes the TDengine Node.js client library.
---

`@tdengine/websocket` is the official Node.js language client library for TDengine. Node.js developers can develop applications to access TDengine instance data. `@tdengine/websocket` connects to TDengine instances via the WebSocket API.

The source code for the Node.js client library is located on [GitHub](https://github.com/taosdata/taos-connector-node/tree/main).

## Installation steps

### Pre-installation

Install the Node.js development environment

### Install via npm

```bash
npm install @tdengine/websocket
```

## Establishing a connection

```javascript
{{#include docs/examples/node/connect.js}}
```

### Usage examples

```javascript
{{#include docs/examples/node/insert.js}}
```

## Frequently Asked Questions

1. Using REST connections requires starting taosadapter.

   ```bash
   sudo systemctl start taosadapter
   ```

2. "Unable to establish connection", "Unable to resolve FQDN"

   Usually, the root cause is an incorrect FQDN configuration. You can refer to this section in the [FAQ](https://docs.tdengine.com/2.4/train-faq/faq/#2-how-to-handle-unable-to-establish-connection) to troubleshoot.

## Important update records

| version | TDengine version | Description                         |
| ------- | ---------------- | ----------------------------------- |
| 3.1.1   | 3.3.2.0 or later | Optimized data transfer performance |
| 3.1.0   | 3.2.0.0 or later | new version, supports websocket     |
