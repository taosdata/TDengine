---
title: TDengine Components
slug: /operations-and-maintenance/tdengine-components
---

import Image from '@theme/IdealImage';
import imgEcosys from '../assets/tdengine-components-01.png';

In the TDengine installation package, in addition to the TDengine database engine (taosd), several additional components are provided to facilitate user usage. The components include:

- **taosAdapter**: Acts as a bridge between applications and TDengine.
- **taosKeeper**: A tool for exporting TDengine monitoring metrics.
- **taosX**: A data pipeline tool.
- **taosExplorer**: A visual management tool.
- **taosc**: The TDengine client driver.

The following diagram shows the topological architecture of the entire TDengine product ecosystem (components taosX and taosX Agent are only available in TDengine Enterprise).

<figure>
<Image img={imgEcosys} alt="TDengine ecosystem"/>
<figcaption>TDengine ecosystem</figcaption>
</figure>

## taosd

In TDengine, **taosd** is a critical daemon process and the core service process. It is responsible for handling all data-related operations, including data writing, querying, and management. On Linux operating systems, users can conveniently start and stop the taosd process using the systemd command. To view all command-line parameters for taosd, users can execute `taosd -h`.

By default, the taosd process logs are stored in the `/var/log/taos/` directory for easy access and management.

TDengine uses a vnode mechanism to partition stored data, with each vnode containing a certain number of data collection points. To provide high availability, TDengine employs a multi-replica method to ensure data reliability and persistence. Vnodes on different nodes can form a vgroup for real-time data synchronization. This design not only improves data availability and fault tolerance but also helps achieve load balancing and efficient data processing.

## taosc

**taosc** is the client program for TDengine, providing developers with a set of functions and interfaces to write applications, connect to TDengine, and execute various SQL commands. Since taosc is written in C, it can be easily integrated with C/C++ applications.

When interacting with TDengine using other programming languages, taosc is also required for native connections. This is because taosc provides the underlying protocols and data structures necessary for communication with TDengine, ensuring smooth interactions between applications in different programming languages.

By using taosc, developers can easily build applications that interact with TDengine to implement functions such as data storage, querying, and management. This design improves the maintainability and scalability of applications while reducing development difficulty, allowing developers to focus on implementing business logic.

## taosAdapter

**taosAdapter** is a standard component in the TDengine installation package, serving as a bridge and adapter between the TDengine cluster and applications. It supports users in accessing TDengine services via RESTful interfaces and WebSocket connections, facilitating convenient data access and processing.

taosAdapter can seamlessly integrate with various data collection agents (such as Telegraf, StatsD, collectd, etc.) to import data into TDengine. Additionally, it provides data writing interfaces compatible with InfluxDB/OpenTSDB, allowing applications originally using InfluxDB/OpenTSDB to be easily migrated to TDengine with minimal modifications.

Through taosAdapter, users can flexibly integrate TDengine into existing application systems, enabling real-time data storage, querying, and analysis.

taosAdapter provides the following features:

- RESTful interface
- WebSocket connection
- Compatibility with InfluxDB v1 format writing
- Compatibility with OpenTSDB JSON and Telnet format writing
- Seamless integration with Telegraf
- Seamless integration with collectd
- Seamless integration with StatsD
- Support for Prometheus `remote_read` and `remote_write`

## taosKeeper

**taosKeeper** is a newly added monitoring metrics export tool in TDengine version 3.0, designed to facilitate real-time monitoring of TDengine's operational status and performance metrics. With simple configuration, TDengine can report its operational status and metrics to taosKeeper. Upon receiving the monitoring data, taosKeeper will utilize the RESTful interface provided by taosAdapter to store this data in TDengine.

One of the key values of taosKeeper is its ability to centrally store monitoring data from multiple TDengine clusters on a unified platform. This allows monitoring software to easily access this data for comprehensive monitoring and real-time analysis of the TDengine clusters. By using taosKeeper, users can more conveniently grasp the operational status of TDengine, promptly detect and resolve potential issues, ensuring system stability and efficiency.

## taosExplorer

To simplify user interaction with the database, TDengine Enterprise introduces a new visualization component—**taosExplorer**. This tool provides users with an intuitive interface for easily managing various elements within the database system, such as databases, supertables, subtables, and their lifecycles.

Through taosExplorer, users can execute SQL queries, monitor system status in real-time, manage user permissions, and perform data backup and recovery operations. Additionally, it supports data synchronization and export between different clusters, as well as managing topics and stream computing functions.

It is worth noting that there are functional differences between the community and enterprise editions of taosExplorer. The enterprise edition offers more features and higher levels of technical support to meet the needs of enterprise users. For specific differences and detailed information, users can refer to the official TDengine documentation.

## taosX

**taosX** serves as the data pipeline component of TDengine Enterprise, aiming to provide users with an easy way to connect to third-party data sources without needing to write code, enabling convenient data import. Currently, taosX supports numerous mainstream data sources, including AVEVA PI System, AVEVA Historian, OPC-UA/DA, InfluxDB, OpenTSDB, MQTT, Kafka, CSV, TDengine 2.x, TDengine 3.x, MySQL, PostgreSQL, and Oracle.

In practical use, users typically do not need to interact directly with taosX. Instead, they can easily access and utilize the powerful features of taosX through the browser user interface provided by taosExplorer. This design simplifies the operational process and lowers the usage threshold, allowing users to focus more on data processing and analysis, thereby improving work efficiency.

## taosX Agent

**taosX Agent** is a vital part of the data pipeline functionality in TDengine Enterprise. It works in conjunction with taosX to receive external data source import tasks issued by taosX. taosX Agent can initiate connectors or directly retrieve data from external data sources, subsequently forwarding the collected data to taosX for processing.

In edge-cloud collaborative scenarios, taosX Agent is typically deployed on the edge, particularly suited for situations where external data sources cannot be accessed directly through the public network. By deploying taosX Agent on the edge, network restrictions and data transmission delays can be effectively addressed, ensuring data timeliness and security.

## Applications or Third-party Tools

By integrating with various applications, visualization, and BI (Business Intelligence) tools, as well as data sources, TDengine provides users with flexible and efficient data processing and analysis capabilities to meet business needs across different scenarios. Applications or third-party tools mainly include the following categories:

1. **Applications**: These applications are responsible for writing, querying business data, and subscribing to data in the business cluster. Applications can interact with the business cluster in three ways:
   - Applications based on taosc: Native connection applications directly connecting to the business cluster, with the default port set to 6030.
   - Applications based on RESTful connections: Applications accessing the business cluster using RESTful interfaces, requiring connection through taosAdapter, with the default port set to 6041.
   - Applications based on WebSocket connections: Applications using WebSocket connections, also requiring connection through taosAdapter, with the default port set to 6041.

2. **Visualization/BI Tools**: TDengine supports seamless integration with various visualization and BI tools, such as Grafana, Power BI, as well as domestic visualization and BI tools. Additionally, users can utilize tools like Grafana to monitor the operational status of the TDengine cluster.

3. **Data Sources**: TDengine possesses powerful data access capabilities, capable of integrating with various data sources, such as MQTT, OPC-UA/DA, Kafka, AVEVA PI System, AVEVA Historian, etc. This enables TDengine to easily consolidate data from different sources, providing users with a comprehensive and unified data view.
