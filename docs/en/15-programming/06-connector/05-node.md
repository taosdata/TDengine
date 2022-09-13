---
sidebar_label: Node.JS
title: TDengine Node.JS Connector
description: Detailed guide for Node.JS Connector
---

 `td2.0-rest-connector` are the official Node.js language connectors for TDengine. Node.js developers can develop applications to access TDengine instance data. `td2.0-rest-connector` is a **REST connector** that connects to TDengine instances via the REST API. 

The Node.js connector source code is hosted on [GitHub](https://github.com/taosdata/taos-connector-node).

## Installation steps

### Pre-installation

Install the Node.js development environment
### Install via npm

```bash
npm i td2.0-rest-connector
```
## Establishing a connection

```javascript
{{#include docs/examples/node/connect.js}}
```

## Usage examples

```javascript
{{#include docs/examples/node/reference_example.js:usage}}
```

## Important Updates


| td2.0-rest-connector version | Description |
| ------------------------- | ---------------------------------------------------------------- |
| 1.0.5 | Support connect to TDengine cloud service

## API Reference

[API Reference](https://docs.taosdata.com/api/td2.0-connector/)