---
title: TDengine Cloud Service Documentation
sidebar_label: Home
slug: /
---
TDengine Cloud, is the fast, elastic, serverless and cost effective time-series data processing service based on the popular open source time-series database, TDengine. With TDengine Cloud, IoT and Big Data developers now have access, on various Cloud providers, to the same robustness, speed and scalability for which TDengine is known. TDengine Cloud delivers a comprehensive, serverless time-series platform that allows customers to focus on solving business needs not only by freeing them from operations and maintenance, but also by providing features such as caching, stream processing and PubSub in one integrated platform thus reducing complexity. At the same time, customers can rest assured that ubiquitous third-party tools such as Prometheus, Telegraf, Grafana and MQTT brokers are supported. Naturally, TDengine Cloud supports connectors in Python, Java, Go, Rust and Node.js thus allowing developers to develop in their language of choice. With SQL support as well as support for schema-less ingestion, TDengine Cloud is adaptable to the needs of all developers. TDengine Cloud also provides additional functions specifically for time series analysis, which makes data analysis and visualization a lot simpler.

This is the documentation structure for TDengine Cloud.

1. The [Introduction](./intro) provides an overview of the features, capabilities and competitive advantages of TDengine Cloud.

2. The [Concepts](./concept) section introduces some of the novel ideas that allow TDengine to exploit the characteristics of time series data to increase compute performance and also to make storage very efficient.

3. In the [Data In](./data-in) section we show you a number of ways for you to get your data into TDengine.

4. TDengine Cloud believes in giving you extremely easy access to your data and in the [Data Out](./data-out) section we show you a number of ways to get data out of TDengine and into your analysis and visualization applications.

5. The [Visualization](./visual) section shows you how you can visualize the data that you store in TDengine, as well as how you can visualize and monitor the status of your TDengine Cloud instance(s) and databases.

6. [Data Sharing](./data-sharing) is an advanced and useful feature of TDengine Cloud. In this section, we provide an easy way to share your data in the TDengine Cloud to others through only simple click operations.

7. Data [Subscription](./tmq) is an advanced and useful feature of TDengine. It is similar to asynchronous publish/subscribe where a message published to a topic is immediately received by all subscribers to that topic. TDengine Subscriptions allow you to create event driven applications without having to install an external pub/sub messaging system.

8. [Stream Processing](./stream) is another extremely useful feature of TDengine Cloud that obviates the need to install external stream processing systems like Kafka or Flink. TDengine's Stream Processing feature allows you to process incoming data streams in real time and push data to tables based on rules that you can define easily.  

9. TDengine provides sophisticated [Data Replication](./replication) features. You can replicate from Cloud to a private instance and vice versa. You can replicate between Cloud providers regardless of region and you can also replicate between edge instances and Cloud or edge instances and private centralized instances.

10. The [Developer Guide](./programming) is a must read if you are developing IoT or Big Data applications for time series data. In this section we introduce the database connection, data modeling, data ingestion, query, stream processing, cache, data subscription, user-defined functions (coming soon), and other functionality in detail. Sample code is provided for a variety of programming languages. In most cases, you can just copy and paste the sample code, make a few changes to accommodate your application, and it will work.

11. The [TDengine SQL](./taos-sql) section provides comprehensive information about both standard SQL as well as TDengine's extensions for easy time series analysis.

12. The [Tools](./tools) section introduces the Taos CLI which gives you shell access to easily perform ad hoc queries on your instances and databases. Additionally, taosBenchmark is introduced. It is a tool that can help you generate large amounts of data very easily with simple configurations and test the performance of TDengine Cloud.

<!-- 10. Finally, in the [FAQ](./faq) section, we try to preemptively answer questions that we anticipate. Of course, we will continue to add to this section all the time. -->

We are very excited that you have chosen TDengine Cloud to be part of your time series platform and look forward to hearing your feedback and ways in which we can improve and be a small part of your success.
