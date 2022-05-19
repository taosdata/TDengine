---
title: TDengine Documentation
sidebar_label: Documentation Home
slug: /
---

TDengine is a high-performance, scalable time series database with supports SQL. This document is TDengine user manual. It mainly introduces the basic concepts, installation, features, SQL, APIs, operation, maintenance, kernel design, etc. It’s written Mainly for architects, developers and system administrators.

TDengine makes full use of the characteristics of time series data, proposes the concepts of "one table for one data collection point" and "super table", and designs an innovative storage engine, which greatly improves the efficiency of data ingestion, querying and storage. In any case, please read [“basic concepts”](./concept) thoroughly.

If you are a developer, please read [“development guide”](./develop) carefully. This section introduces database connection, data modeling, inserting data, query, continuous query, cache, data subscription, user-defined function, etc. in detail. Sample codes are provided for a variety of programming languages. In most cases, you can just copy and paste the sample code and make a few changes to accommodate your application, and it will work.

We have lived in the era of big data, and scale-up is unable to meet the growing business needs. Any modern data system must have the ability to scale out. Clustering has become an indispensable feature of big data systems. TDengine team has open sourced this important cluster feature. How to deploy, manage and maintain a TDengine cluster? please refer to ["cluster management"](./cluster).

TDengine uses SQL as its query language, which greatly reduces learning costs and migration costs, but at the meantime, it has made some extensions to support time series data scenarios better. The ["SQL Guide"](./taos-sql) chapter describes the SQL syntax in detail, and lists the various supported commands and functions.

If you are a system administrator who cares about installation, upgrade, fault tolerance and disaster recovery, data import, export, system configuration, how to monitor whether TDengine is running healthily, and how to improve system performance, please refer to the ["Operation and Maintenance Guide"](./operation) thoroughly.

If you want to know more about TDengine tools, REST API, and connectors for various programming languages, please see the ["Reference Guide"](./reference) chapter.

If you are very interested in the internal design of TDengine, please read the chapter ["TDengine Inside”](./tdinternal) , which introduces the cluster design, data partitioning, sharding, writing, and reading processes in detail. If you want to study TDengine code or even contribute code, please read this chapter carefully.

As an open source software, you are welcome to be a part of TDengine. If you find any errors in the document, or the description is not clear, please click "Edit this document" at the bottom of each page to edit it directly.

Together, we make a difference.
