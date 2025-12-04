---
toc_max_heading_level: 4
sidebar_label: Node.js
title: TDengine Node.js Connector
---

`@tdengine/websocket` 是 TDengine 的官方 Node.js 语言连接器。Node.js 开发人员可以通过它开发可以存取 TDengine 集群数据的应用软件，它通过 WebSocket 接口连接 TDengine 的运行实例。

Node.js 连接器源码托管在 [GitHub](https://github.com/taosdata/taos-connector-node/tree/main)。

## 版本支持

支持 Node.js 14 及以上版本。

## 安装步骤

### 安装前准备

安装 Node.js 开发环境，使用 14 以上版本。[下载链接](https://nodejs.org/en/download/)

### 使用 npm 安装

```bash
npm install @tdengine/websocket
```

## 建立连接

```javascript
{{#include docs/examples/node/connect.js}}
```

## 常见问题

1. 使用 REST 连接需要启动 taosadapter。

   ```bash
   sudo systemctl start taosadapter
   ```

2. "Unable to establish connection"，"Unable to resolve FQDN"

一般都是因为配置 FQDN 不正确。可以参考[如何彻底搞懂 TDengine 的 FQDN](https://www.taosdata.com/blog/2021/07/29/2741.html) 。

## 重要更新记录

| version | TDengine version   | 说明                            |
| ------- | ------------------ | ------------------------------- |
| 3.1.1   | 3.3.2.0 及更高版本 | 优化了数据传输性能              |
| 3.1.0   | 3.2.0.0 及更高版本 | 新版本发布，支持 WebSocket 连接 |

## API 参考

[API 参考](https://docs.taosdata.com/api/td2.0-connector/)
