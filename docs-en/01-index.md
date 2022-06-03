---
title: TDengine Documentation
sidebar_label: Documentation Home
slug: /
---

TDengine is a [high-performance](https://tdengine.com/fast), [scalable](https://tdengine.com/scalable) time series database with [SQL support](https://tdengine.com/sql-support). This document is the TDengine user manual. It introduces the basic, as well as novel concepts, in TDengine, and also talks in detail about installation, features, SQL, APIs, operation, maintenance, kernel design and other topics. It’s written mainly for architects, developers and system administrators.

To get a global view about TDengine, like feature list, benchmarks, and competitive advantages, please browse through section [Introduction](./intro).

TDengine greatly improves the efficiency of data ingestion, querying and storage by exploiting the characteristics of time series data, introducing the novel concepts of "one table for one data collection point" and "super table", and designing an innovative storage engine. To understand the new concepts in TDengine and make full use of the features and capabilities of TDengine, please read [“Concepts”](./concept) thoroughly.

If you are a developer, please read the [“Developer Guide”](./develop) carefully. This section introduces the database connection, data modeling, data ingestion, query, continuous query, cache, data subscription, user-defined functions, and other functionality in detail. Sample code is provided for a variety of programming languages. In most cases, you can just copy and paste the sample code, make a few changes to accommodate your application, and it will work.

We live in the era of big data, and scale-up is unable to meet the growing business needs. Any modern data system must have the ability to scale out, and clustering has become an indispensable feature of big data systems. Not only did the TDengine team develop the cluster feature, but also decided to open source this important feature. To learn how to deploy, manage and maintain a TDengine cluster please refer to ["cluster"](./cluster).

TDengine uses ubiquitious SQL as its query language, which greatly reduces learning costs and migration costs. In addition to the standard SQL, TDengine has extensions to better support time series data analysis. These extensions include functions such as roll up, interpolation and time weighted average, among many others. The ["SQL Reference"](./taos-sql) chapter describes the SQL syntax in detail, and lists the various supported commands and functions.

If you are a system administrator who cares about installation, upgrade, fault tolerance, disaster recovery, data import, data export, system configuration, how to monitor whether TDengine is running healthily, and how to improve system performance, please refer to, and thoroughly read the ["Administration"](./operation) section.

If you want to know more about TDengine tools, the REST API, and connectors for various programming languages, please see the ["Reference"](./reference) chapter.

If you are very interested in the internal design of TDengine, please read the chapter ["Inside TDengine”](./tdinternal), which introduces the cluster design, data partitioning, sharding, writing, and reading processes in detail. If you want to study TDengine code or even contribute code, please read this chapter carefully.

TDengine is an open source database, and we would love for you to be a part of TDengine. If you find any errors in the documentation, or see parts where more clarity or elaboration is needed, please click "Edit this page" at the bottom of each page to edit it directly.

Together, we make a difference.
