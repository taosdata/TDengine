---
toc_max_heading_level: 4
sidebar_label: Node.js
title: TDengine Node.js Connector
---

`@tdengine/rest` 是 TDengine 的官方 Node.js 语言连接器。 Node.js 开发人员可以通过它开发可以存取 TDengine 集群数据的应用软件。另外`@tdengine/rest` 是 **REST 连接器**，它通过 REST 接口连接 TDengine 的运行实例。

Node.js 连接器源码托管在 [GitHub](https://github.com/taosdata/taos-connector-node/tree/3.0)。

## 版本支持

请参考[版本支持列表](../#版本支持)

## 安装步骤

### 安装前准备

### 使用 npm 安装

```bash
npm install @tdengine/rest
```

## 建立连接

```javascript
{{#include docs/examples/node/connect.js}}
```

## 更多示例程序

```javascript
{{#include docs/examples/node/reference_example.js:usage}}
```

## 常见问题

1. 使用 REST 连接需要启动 taosadapter。

   ```bash
   sudo systemctl start taosadapter
   ```

2. Node.js 版本

   原生连接器 `@tdengine/client` 目前兼容的 Node.js 版本为：>=v10.20.0 <= v10.9.0 || >=v12.8.0 <= v12.9.1

3. "Unable to establish connection"，"Unable to resolve FQDN"

  一般都是因为配置 FQDN 不正确。 可以参考[如何彻底搞懂 TDengine 的 FQDN](https://www.taosdata.com/blog/2021/07/29/2741.html) 。

## 重要更新记录

### 原生连接器

| package name     | version | TDengine version    | 说明                                                             |
|------------------|---------|---------------------|------------------------------------------------------------------|
| @tdengine/client | 3.0.0   | 3.0.0               | 支持TDengine 3.0 且不与2.x 兼容。                                                          |
| td2.0-connector  | 2.0.12  | 2.4.x；2.5.x；2.6.x | 修复 cursor.close() 报错的 bug。                                 |
| td2.0-connector  | 2.0.11  | 2.4.x；2.5.x；2.6.x | 支持绑定参数、json tag、schemaless 接口等功能。                  |
| td2.0-connector  | 2.0.10  | 2.4.x；2.5.x；2.6.x | 支持连接管理，普通查询、连续查询、获取系统信息、订阅功能等功能。 |
### REST 连接器

| package name         | version | TDengine version    | 说明                                                                      |
|----------------------|---------|---------------------|---------------------------------------------------------------------------|
| @tdengine/rest       | 3.0.0   | 3.0.0               | 支持 TDegnine 3.0，且不与2.x 兼容。                                               |
| td2.0-rest-connector | 1.0.7   | 2.4.x；2.5.x；2.6.x | 移除默认端口 6041。                                                       |
| td2.0-rest-connector | 1.0.6   | 2.4.x；2.5.x；2.6.x | 修复create，insert，update，alter 等SQL 执行返回的 affectRows 错误的bug。 |
| td2.0-rest-connector | 1.0.5   | 2.4.x；2.5.x；2.6.x | 支持云服务 cloud Token；                                                  |
| td2.0-rest-connector | 1.0.3   | 2.4.x；2.5.x；2.6.x | 支持连接管理、普通查询、获取系统信息、错误信息、连续查询等功能。          |

## API 参考

[API 参考](https://docs.taosdata.com/api/td2.0-connector/)
