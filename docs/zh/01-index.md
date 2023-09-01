---
title: TDengine 文档
sidebar_label: 文档首页
slug: /
---

TDengine 是一款[开源](https://www.taosdata.com/tdengine/open_source_time-series_database)、[高性能](https://www.taosdata.com/fast)、[云原生](https://www.taosdata.com/tdengine/cloud_native_time-series_database)的<a href="https://www.taosdata.com/" data-internallinksmanager029f6b8e52c="2" title="时序数据库" target="_blank" rel="noopener">时序数据库</a>（<a href="https://www.taosdata.com/time-series-database" data-internallinksmanager029f6b8e52c="9" title="Time Series DataBase" target="_blank" rel="noopener">Time Series Database</a>, <a href="https://www.taosdata.com/tsdb" data-internallinksmanager029f6b8e52c="8" title="TSDB" target="_blank" rel="noopener">TSDB</a>）, 它专为物联网、车联网、工业互联网、金融、IT 运维等场景优化设计。同时它还带有内建的缓存、流式计算、数据订阅等系统功能，能大幅减少系统设计的复杂度，降低研发和运营成本，是一款极简的时序数据处理平台。本文档是 TDengine 的用户手册，主要是介绍 TDengine 的基本概念、安装、使用、功能、开发接口、运营维护、TDengine 内核设计等等，它主要是面向架构师、开发工程师与系统管理员的。

TDengine 充分利用了时序数据的特点，提出了“一个数据采集点一张表”与“超级表”的概念，设计了创新的存储引擎，让数据的写入、查询和存储效率都得到极大的提升。为正确理解并使用 TDengine，无论如何，请您仔细阅读[基本概念](./concept)一章。

如果你是开发工程师，请一定仔细阅读[开发指南](./develop)一章，该部分对数据库连接、建模、插入数据、查询、流式计算、缓存、数据订阅、用户自定义函数等功能都做了详细介绍，并配有各种编程语言的示例代码。大部分情况下，你只要复制粘贴示例代码，针对自己的应用稍作改动，就能跑起来。

我们已经生活在大数据时代，纵向扩展已经无法满足日益增长的业务需求，任何系统都必须具有水平扩展的能力，集群成为大数据以及 Database 系统的不可缺失功能。TDengine 团队不仅实现了集群功能，而且将这一重要核心功能开源。怎么部署、管理和维护 TDengine 集群，请仔细参考[部署集群]一章。

TDengine 采用 SQL 作为查询语言，大大降低学习成本、降低迁移成本，但同时针对时序数据场景，又做了一些扩展，以支持插值、降采样、时间加权平均等操作。[SQL 手册](./taos-sql)一章详细描述了 SQL 语法、详细列出了各种支持的命令和函数。

如果你是系统管理员，关心安装、升级、容错灾备、关心数据导入、导出、配置参数，如何监测 TDengine 是否健康运行，如何提升系统运行的性能，请仔细参考[运维指南](./operation)一章。

如果你对 TDengine 的外围工具、REST API、各种编程语言的连接器（Connector）想做更多详细了解，请看[参考指南](./reference)一章。

最后，作为一个开源软件，欢迎大家的参与。如果发现文档有任何错误、描述不清晰的地方，请在每个页面的最下方，点击“编辑本文档”直接进行修改。

Together, we make a difference!
