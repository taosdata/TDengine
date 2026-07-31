---
sidebar_label: 阅读指南
title: 阅读指南
description: TDengine TSDB 用户手册
slug: /
---

TDengine TSDB（下文简称 TDengine）是一款 [开源](https://www.taosdata.com/tdengine/open_source_time-series_database)、[高性能](https://www.taosdata.com/fast)、[云原生](https://www.taosdata.com/tdengine/cloud_native_time-series_database) 的 [时序数据库](https://www.taosdata.com/)（[Time Series Database](https://www.taosdata.com/time-series-database)，[TSDB](https://www.taosdata.com/tsdb)），专为物联网、车联网、工业互联网、金融、IT 运维等场景优化设计。同时内建缓存、流式计算、数据订阅等系统功能，能大幅降低系统设计复杂度以及研发与运营成本，是一款极简的时序数据处理平台。本文档是其用户手册，介绍基本概念、安装、使用、功能、开发接口、运营维护、内核设计等内容，面向架构师、开发工程师与系统管理员。若对时序数据的基本概念、价值及其业务意义尚不熟悉，请参考 [时序数据基础](../03-core-concepts/index.md)。

TDengine 充分利用时序数据的特点，提出了“一个数据采集点一张表”与“超级表”的概念，并设计了创新的存储引擎，显著提升写入、查询与存储效率。为正确理解并使用它，无论你担任何种角色，请仔细阅读 [基本概念](../04-quick-start/02-basic-concepts.md) 一章。

若你是开发工程师，请仔细阅读 [开发指南](../10-developer-guide/index.md) 一章。该章详细介绍数据库连接、建模、写入、查询、流式计算、缓存、数据订阅、用户自定义函数等功能，并提供多种编程语言的示例代码。多数情况下，复制示例代码并按自身应用稍作修改即可运行。如需进一步了解 REST API 与各语言连接器，请参阅 [连接器参考](../10-developer-guide/08-connectors-reference/index.md) 一章。

在大数据时代，纵向扩展已难以满足持续增长的业务需求，系统普遍需要具备水平扩展能力，集群已成为大数据与数据库系统不可或缺的能力。TDengine 团队不仅实现了集群功能，还将这一核心能力开源。关于如何部署、管理与维护 TDengine 集群，请仔细参阅 [运维指南](../12-operations-and-tooling/02-operations/index.md) 一章。

TDengine 采用 SQL 作为查询语言，可显著降低学习与迁移成本；同时针对时序数据场景进行了扩展，以支持插值、降采样、时间加权平均等操作。[TDengine SQL](../05-tdengine-sql/index.md) 一章详细说明 SQL 语法，并列出支持的命令与函数。

若你是系统管理员，需要了解安装、升级、容错灾备，以及数据导入导出、配置参数、健康监测与性能优化等内容，请仔细参阅 [运维指南](../12-operations-and-tooling/02-operations/index.md) 一章。

若你对数据库内核设计感兴趣，或关注开源实现，建议仔细阅读 [技术内幕](../15-internals/index.md) 一章。该章从分布式架构到存储引擎、查询引擎、数据订阅，再到流式计算引擎均有阐述。建议对照文档阅读 GitHub 上的源代码，深入了解设计与实现，并欢迎加入开源社区贡献代码。

2025 年 7 月，TDengine 团队推出新产品 TDengine IDMP——AI 原生的工业数据管理平台。借助 AI 大模型，可基于采集数据自动感知应用场景、生成面板与报表，并开展实时数据分析。若有数据挖掘需求，或面临海量数据业务洞察方面的挑战，建议 [详细了解 TDengine IDMP 并免费试用](https://www.taosdata.com/idmp)。

TDengine 培训与认证中心是涛思数据官方打造的专业能力平台，通过分层认证体系覆盖时序数据架构设计、集群化运维管理及多语言应用开发三大领域，帮助系统掌握相关技能并获得行业认可的技术认证。

目前面向运维人员与数据分析师，限时免费开放 TCP-BP（时序数据库基础认证专员）认证考试，帮助快速掌握产品架构、SQL 基础、部署运维等核心知识。完成认证后，可获得权威证书，并加入 TDengine 开发者社区，获取专属资料与技术支持。访问 [TDengine 培训与认证中心](https://learn.taosdata.com/) 即可进入专题页面，通过阶梯考试系统化提升技能。

最后，作为开源软件，欢迎参与贡献。若发现文档错误或表述不清，请在每个页面最下方点击“编辑本文档”提交修改。

Together, we make a difference!
