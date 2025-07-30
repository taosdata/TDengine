---
title: TDengine Cloud Documentation
sidebar_label: Home
description: This website contains the user manuals for TDengine Cloud, a fully managed cloud service for industrial big data.
slug: /
---
TDengine Cloud, is the fast, elastic, and cost effective time-series data processing service based on the popular open source time-series database, TDengine. With TDengine Cloud, IoT and Big Data developers now have access, on various Cloud providers, to the same robustness, speed and scalability for which TDengine is known. TDengine Cloud delivers a comprehensive time-series platform that allows customers to focus on solving business needs not only by freeing them from operations and maintenance, but also by providing features such as caching, stream processing and PubSub in one integrated platform thus reducing complexity. At the same time, customers can rest assured that ubiquitous third-party tools such as Prometheus, Telegraf, Grafana and MQTT brokers are supported. Naturally, TDengine Cloud supports client libraries for Python, Java, Go, Rust and Node.js thus allowing developers to develop in their language of choice. With SQL support as well as support for schema-less ingestion, TDengine Cloud is adaptable to the needs of all developers. TDengine Cloud also provides additional functions specifically for time series analysis, which makes data analysis and visualization a lot simpler.

This is the documentation structure for TDengine Cloud.

1. The [Introduction](./intro/) provides an overview of the features, capabilities and competitive advantages of TDengine Cloud.
2. The [Concepts](./concept/) section introduces some of the novel ideas that allow TDengine to exploit the characteristics of time series data to increase compute performance and also to make storage very efficient.
3. In the [Data In](./data-in/) section we show you a number of ways for you to get your data into TDengine.
4. The [Developer Guide](./programming/) is a must read if you are developing IoT or Big Data applications for time series data. In this section we introduce the database connection, data modeling, data ingestion, query, stream processing, cache, data subscription, user-defined functions (coming soon), and other functionality in detail. Sample code is provided for a variety of programming languages. In most cases, you can just copy and paste the sample code, make a few changes to accommodate your application, and it will work.
5. [Stream Processing](./stream/) is another extremely useful feature of TDengine Cloud that obviates the need to install external stream processing systems like Kafka or Flink. TDengine's Stream Processing feature allows you to process incoming data streams in real time and push data to tables based on rules that you can define easily.  
6. [Data Subscription](./data-subscription/) is an advanced and useful feature of TDengine. It is similar to asynchronous publish/subscribe where a message published to a topic is immediately received by all subscribers to that topic. TDengine Subscriptions allow you to create event driven applications without having to install an external pub/sub messaging system. In the TDengien Cloud, we provide an easy way to create topic and share your topic in the TDengine Cloud to others through only simple click operations.
7. [Tools](./tools/), TDengine Cloud provides tools for data access, data out and data visualization. With these tools, you can easily and conveniently access the database data of your TDengine instance in TDengine Cloud and perform various queries.
8. [Management](./mgmt/), the management of the resources in the specified TDengine instance.
9. [Instances](./instances/), the management of the instance list of the current organization.
10. [Users](./user-mgmt/), the management of the users in the specified TDengine Cloud organization.
11. [Organizations](./orgs/), the management of the organizaiton list of the specifeid user.
12. [DBMart](./dbmarts/), TDengine Cloud provides the database mart for the user to access the Public/Private database and easy to use these databases.

We are very excited that you have chosen TDengine Cloud to be part of your time series platform and look forward to hearing your feedback and ways in which we can improve and be a small part of your success.
