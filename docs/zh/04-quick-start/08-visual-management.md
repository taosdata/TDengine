---
sidebar_label: 可视化管理
title: 可视化管理
description: 使用 taosExplorer 快速体验 TDengine 可视化管理
toc_max_heading_level: 4
---

taosExplorer 是 TDengine 提供的 Web 可视化管理工具。
相比只在 shell 中执行命令，taosExplorer 更适合用来浏览数据库对象、执行 SQL、查看连接示例，并快速了解可与 TDengine 集成的外部工具。

本章继续使用前几章创建的智能电表数据，带你通过浏览器完成一次最基本的可视化体验：登录 taosExplorer、浏览数据库和表、执行查询，并找到编程语言和可视化工具入口。

## 前提条件

请先确认已经完成以下准备：

1. TDengine 服务已经启动。
2. taosExplorer 服务已经启动，并且浏览器可以访问其端口。
3. 已经创建过 `power` 数据库、`meters` 超级表和 `d1001`、`d1002` 等子表，或已经通过 `taosBenchmark` 生成过示例数据。

如果你使用本快速上手中的 Docker 启动方式，容器已经映射 taosExplorer 默认端口 `6060`。

## 打开 taosExplorer

打开浏览器，访问 taosExplorer 地址。

```text
http://localhost:6060
```

如果 TDengine 部署在远程服务器上，请将 `localhost` 替换为服务器地址，并确认安全组、防火墙或容器端口映射已放通 `6060` 端口。

进入登录页后，输入用户名和密码。默认账号如下：

```text
用户名：root
密码：taosdata
```

登录后，你会进入 taosExplorer 的主界面。

## 浏览数据库和表

进入“数据浏览器”页面，可以查看当前实例中的数据库、超级表、子表和普通表。

![taosExplorer 数据浏览器](./assets/explorer.png)

如果你使用前几章的智能电表示例，可以依次展开：

1. `power` 数据库。
2. `meters` 超级表。
3. `d1001`、`d1002` 等子表。

在这里，你可以直观看到数据库对象之间的层级关系，也可以查看表结构、标签和部分数据。

## 执行 SQL 查询

taosExplorer 也提供 SQL 查询入口。你可以复制下面的 SQL，在页面中执行，查看电表数据。

```sql
SELECT tbname, ts, current, voltage, phase
FROM power.meters
ORDER BY ts DESC
LIMIT 10;
```

返回结果会以表格形式展示。相比 shell，表格界面更适合临时筛选、查看列值和复制查询结果。

如果你使用的是 `taosBenchmark` 生成的默认数据，可以查询 `test.meters`：

```sql
SELECT tbname, ts, current, voltage
FROM test.meters
ORDER BY ts DESC
LIMIT 10;
```

## 查看连接示例和工具入口

taosExplorer 不只用于浏览数据，也提供了一些上手入口：

- 在“编程”页面，可以查看 Java、Go、Python、JavaScript/Node.js、C#、Rust、R 等语言的连接示例。
- 在“工具”页面，可以查看 Grafana、Power BI、Superset、Tableau、Excel 等工具与 TDengine 集成的入口。
- 点击页面右上角的 `?` 图标，可以查看内置帮助和文档入口。

这些入口适合在快速体验后继续连接应用程序、BI 工具或可视化大屏。

## 常见问题

如果浏览器无法打开 taosExplorer，请先检查：

- taosExplorer 服务是否已经启动。
- 访问地址和端口是否正确，默认端口为 `6060`。
- Docker、云主机安全组或本机防火墙是否放通 `6060` 端口。
- taosExplorer 配置中的 `cluster` 是否指向可访问的 taosAdapter 地址，默认是 `http://localhost:6041`。

如果登录后看不到前几章创建的数据，请确认当前连接的是同一个 TDengine 实例，并检查数据库名是否为 `power` 或 `test`。

## 继续阅读

本章只介绍 taosExplorer 的基本使用。更多安装、配置和集成方式，请继续阅读以下文档：

- [taosExplorer 参考手册](../12-operations-and-tooling/03-components/04-explorer.md)：taosExplorer 安装、配置和高级功能。
- [用 Docker 快速体验 TDengine](./01-download-and-install/01-docker.md)：确认 Docker 端口映射和服务启动方式。
- [数据查询](./05-query-and-aggregate.md)：继续在 shell 或 taosExplorer 中执行 SQL 查询。
- [Grafana 集成](./09-grafana-integration.md)：使用 Grafana 创建监控面板。
- [零代码数据写入](./10-no-code-ingestion.md)：通过可视化方式配置数据接入。
