---
title: TDengine Components
slug: /operations-and-maintenance/tdengine-components
---

import Image from '@theme/IdealImage';
import imgEcosys from '../assets/tdengine-components-01.png';

In the TDengine installation package, in addition to the TDengine database engine taosd, several additional components are provided to facilitate user experience. taosAdapter serves as a bridge between applications and TDengine; taosKeeper is a tool for exporting TDengine monitoring metrics; taosX is a data pipeline tool; taosExplorer is a graphical management tool; taosc is the TDengine client driver. The diagram below shows the topology of the entire TDengine product ecosystem (components taosX, taosX Agent are only available in TDengine Enterprise).

<figure>
<Image img={imgEcosys} alt="TDengine ecosystem"/>
<figcaption>TDengine ecosystem</figcaption>
</figure>

## taosd

In TDengine, taosd is a key daemon and also the core service process. It handles all data-related operations, including data writing, querying, and management. On Linux operating systems, users can conveniently start and stop the taosd process using systemd commands. To view all command-line arguments of taosd, users can execute the taosd -h command.

The logs of the taosd process are stored by default in the /var/log/taos/ directory, facilitating log viewing and management.

TDengine uses a vnode mechanism to segment stored data, with each vnode containing a certain number of data collection points. To provide high availability services, TDengine uses a multi-replica approach to ensure data reliability and persistence. Vnodes on different nodes can form a vgroup to achieve real-time data synchronization. This design not only improves data availability and fault tolerance but also helps achieve load balancing and efficient data processing.

## taosc

taosc is TDengine's client program, providing developers with a set of functions and interfaces to write applications and connect to TDengine, executing various SQL commands. Since taosc is written in C, it can be easily integrated with C/C++ applications.

When interacting with TDengine using other programming languages, reliance on taosc is necessary if using native connections. This is because taosc provides the underlying protocols and data structures required for communication with TDengine, ensuring that applications in different programming languages can interact smoothly with TDengine.

By using taosc, developers can easily build applications that interact with TDengine, implementing functions such as data storage, querying, and management. This design improves the maintainability and scalability of applications while reducing development difficulty, allowing developers to focus on implementing business logic.

## taosAdapter

taosAdapter is a standard component in the TDengine installation package, acting as a bridge and adapter between the TDengine cluster and applications. It supports user access to TDengine services via RESTful interfaces and WebSocket connections, facilitating convenient data access and processing.

taosAdapter can seamlessly connect with various data collection agent tools (such as Telegraf, StatsD, collectd, etc.), thereby importing data into TDengine. Additionally, it provides data writing interfaces compatible with InfluxDB/OpenTSDB, allowing applications originally using InfluxDB/OpenTSDB to be easily ported to TDengine without significant modifications.

Through taosAdapter, users can flexibly integrate TDengine into existing application systems, achieving real-time data storage, querying, and analysis.

taosAdapter provides the following features:

- RESTful interface;
- WebSocket connection;
- Compatible with InfluxDB v1 format writing;
- Compatible with OpenTSDB JSON and Telnet format writing;
- Seamless connection to Telegraf;
- Seamless connection to collectd;
- Seamless connection to StatsD;
- Supports Prometheus remote_read and remote_write.

## taosKeeper

taosKeeper is a monitoring metric export tool newly added in TDengine 3.0, designed to facilitate real-time monitoring of TDengine's operational status and performance metrics. Through simple configuration, TDengine can report its operational status and metrics to taosKeeper. Upon receiving monitoring data, taosKeeper uses the RESTful interface provided by taosAdapter to store this data in TDengine.

An important value of taosKeeper is that it can centralize the monitoring data of multiple or even a batch of TDengine clusters on a unified platform. This enables monitoring software to easily access this data, thereby achieving comprehensive monitoring and real-time analysis of TDengine clusters. Through taosKeeper, users can more conveniently grasp the operational status of TDengine, promptly identify and resolve potential issues, ensuring system stability and efficiency.

## taosExplorer

To simplify the use and management of the database for users, TDengine Enterprise has introduced a new visual component—taosExplorer. This tool provides users with an intuitive interface, making it easy to manage various elements within the database system, such as databases, supertables, subtables, and their lifecycle.

Through taosExplorer, users can execute SQL queries, monitor system status in real-time, manage user permissions, and perform data backup and recovery operations. Additionally, it supports data synchronization with other clusters, data export, and management of topics and stream computing, among other features.

It is worth mentioning that the TSDB-OSS edition and TSDB-Enterprise edition of taosExplorer differ in functionality. The TSDB-Enterprise offers more features and higher levels of technical support to meet the needs of enterprise users. For specific differences and detailed information, users can refer to the official TDengine documentation.

## taosX

As a data pipeline component of TDengine Enterprise, taosX aims to provide users with an easy way to connect to third-party data sources without the need for coding, facilitating convenient data import. Currently, taosX supports numerous mainstream data sources, including AVEVA PI System, AVEVA Historian, OPC-UA/DA, InfluxDB, OpenTSDB, MQTT, Kafka, CSV, TDengine Query, TDengine Data Subscription, MySQL, PostgreSQL, and Oracle, among others.

In practice, users usually do not need to interact directly with taosX. Instead, they can easily access and utilize the powerful features of taosX through the browser user interface provided by taosExplorer. This design simplifies the operation process, lowers the usage threshold, and allows users to focus more on data processing and analysis, thereby improving work efficiency.

## taosX Agent

taosX Agent is an important component of the TDengine Enterprise data pipeline functionality, working in conjunction with taosX to handle external data source import tasks issued by taosX. taosX Agent can initiate connectors or directly fetch data from external data sources, then forward the collected data to taosX for processing.

In edge-cloud collaboration scenarios, taosX Agent is typically deployed at the edge, especially suitable for situations where external data sources cannot be directly accessed via the public network. Deploying taosX Agent at the edge can effectively solve problems related to network restrictions and data transmission delays, ensuring the timeliness and security of data.

## Applications or Third-Party Tools

By integrating with various applications, visualization and BI (Business Intelligence) tools, and data sources, TDengine provides users with flexible and efficient data processing and analysis capabilities to meet business needs in different scenarios. Applications or third-party tools mainly include the following categories.

1. Applications

These applications are responsible for writing business data to the business cluster, querying business data, and subscribing to data. Applications can interact with the business cluster in the following three ways:

- Applications based on taosc: Applications using native connections, directly connected to the business cluster, default port is 6030.
- Applications based on RESTful connections: Applications that access the business cluster using RESTful interfaces, need to connect through taosAdapter, default port is 6041.
- Applications based on WebSocket connections: Applications using WebSocket connections, also need to connect through taosAdapter, default port is 6041.

1. Visualization/BI Tools

TDengine supports seamless integration with numerous visualization and BI tools, such as Grafana, Power BI, and domestically produced visualization and BI tools. Additionally, tools like Grafana can be used to monitor the operational status of the TDengine cluster.

1. Data Sources

TDengine has strong data access capabilities and can connect to various data sources, such as MQTT, OPC-UA/DA, Kafka, AVEVA PI System, AVEVA Historian, etc. This enables TDengine to easily integrate data from different sources, providing users with a comprehensive and unified data view.
