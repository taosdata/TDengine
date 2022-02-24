# TDengine Introduction

## <a class="anchor" id="intro"></a> About TDengine

TDengine is a high-performance, scalable and innovative Big Data platform developed to address the technical and operational challenges of the exponentially growing Internet of Things (IoT) market. It does not rely on any third-party software, nor does it optimize or package any open-source database or stream computing product. Instead, it is developed from scratch and purpose-built for IoT and incorporates the strengths of many traditional relational databases, NoSQL databases, stream computing engines, message queues, and other software. TDengine is optimized for, and hence has unique advantages in processing streaming, time-series, Big Data from billions of sensors, data collectors and devices.

One of the modules of TDengine, which exploits the characteristics of IoT time series data, is the highly optimized storage engine. However, in addition to this, TDengine also provides full-stack functions such as caching, message queuing, subscription, stream computing, among others to reduce the cost and complexity of R&D as well as operations. TDengine essentially provides an efficient, easy-to-use, full-stack technical solution for the processing of IoT and Industrial Internet Big Data. Compared with varied IoT Big Data platforms such as InfluxDB, OpenTSDB, Timescale, Prometheus and Hadoop, TDengine has the following distinct characteristics:

- **Performance improvement over 10 times**: An innovative data storage structure allows every single core to process at least 20,000 requests per second, insert millions of data points every second, and read more than 10 million data points in a second - this is more than 10 times faster than other existing general purpose database.
- **Reduce the cost of hardware or cloud services to 1/5**: Due to its ultra-performance, TDengine consumes less than 1/5 of the computing resources consumed by other common Big Data solutions. Through columnar storage and advanced compression algorithms, the storage consumption is less than 1/10 of other general purpose databases.
- **Full-stack time-series data processing engine**: With integrated database, message queue, cache, stream computing, and other functions, applications do not need to integrate with software such as Kafka/Redis/HBase/Spark/HDFS, thus greatly reducing the complexity and cost of application development, maintenance and operations.
- **Highly Available and Horizontal Scalable**: With a natively distributed architecture and consistency algorithm, via multi-replication and clustering features, TDengine ensures high availability and horizontal scalability to support mission-critical, carrier-grade IoT Big Data applications.
- **Zero operation cost & zero learning cost**: Installing clusters is simple, configurable and quick, with real-time backup built-in. There is no need to split libraries or tables. TDengine supports standard SQL and offers extensions specifically to analyze time-series data. TDengine also supports RESTful, Python/Java/C/C++/C#/Go/Node.js APIs with zero learning cost and reusability.
- **Core is Open Sourced:** The core of TDengine, including the cluster features, is open-sourced. Enterprises are no longer restricted by the database but are supported by a strong ecosystem, stable products and an active developer community. 

With TDengine, the total cost of ownership of typical IoT, Connected Vehicles, Industrial Internet, Energy, Financial, DevOps and other Big Data applications can be greatly reduced. Note that because TDengine makes full use of the characteristics of IoT time-series data and is highly optimized for it, TDengine cannot be used as a general purpose database engine to process general data from web crawlers, microblogs, WeChat, e-commerce, ERP, CRM, and other sources.

![TDengine Technology Ecosystem](../images/eco_system.png)

<center>Figure 1. TDengine Technology Ecosystem</center>

## <a class="anchor" id="scenes"></a>High Level Requirements for the Applicability of TDengine

As an IoT time-series Big Data platform, TDengine is optimal for application scenarios with the requirements described below. Therefore the following sections of this document are mainly aimed at IoT-relevant systems. Other systems, such as CRM, ERP, etc., are beyond the scope of this article.

### Characteristics and Requirements of Data Sources

From the perspective of data sources, designers can analyze the applicability of TDengine in target application systems as follows.

| **Data Source Characteristics and Requirements**         | **Not Applicable** | **Might Be Applicable** | **Very Applicable** | **Description**                                              |
| -------------------------------------------------------- | ------------------ | ----------------------- | ------------------- | :----------------------------------------------------------- |
| A massive amount of total data                           |                    |                         | √                   | TDengine provides excellent scale-out functions in terms of capacity, and has a storage structure with matching high compression ratio to achieve the best storage efficiency in the industry. |
| Data input velocity is extremely high |                  |                         | √                   | TDengine's performance is much higher than other similar products. It can continuously process larger amounts of input data in the same hardware environment, and provides a performance evaluation tool that can easily run in the user environment. |
| A huge number of data sources                            |                    |                         | √                   | TDengine is optimized specifically for a huge number of data sources. It is especially suitable for efficiently ingesting, writing and querying data from billions of data sources. |

### System Architecture Requirements

| **System Architecture Requirements**              | **Not Applicable** | **Might Be Applicable** | **Very Applicable** | **Description**                                              |
| ------------------------------------------------- | ------------------ | ----------------------- | ------------------- | ------------------------------------------------------------ |
| A simple and reliable system architecture |                    |                         | √                   | TDengine's system architecture is very simple and reliable, with its own message queue, cache, stream computing, monitoring and other functions. There is no need to integrate any additional third-party products. |
| Fault-tolerance and high-reliability      |                    |                         | √                   | TDengine has cluster functions to automatically provide high-reliability and high-availability functions such as fault tolerance and disaster recovery. |
| Standardization support                           |                    |                         | √                   | TDengine supports standard SQL and also provides extensions specifically to analyze time-series data. |

### System Function Requirements

| **System Function Requirements**                  | **Not Applicable** | **Might Be Applicable** | **Very Applicable** | **Description**                                              |
| ------------------------------------------------- | ------------------ | ----------------------- | ------------------- | ------------------------------------------------------------ |
| Complete data processing algorithms built-in |                    | √                    |                     | While TDengine implements various general data processing algorithms, industry specific algorithms and special types of processing will need to be implemented at the application level. |
| A large number of crosstab queries             |                    | √                    |                     | This type of processing is better handled by relational database systems but TDengine can work in concert with relational database systems to provide more complete solutions. |

### System Performance Requirements

| **System Performance Requirements**               | **Not Applicable** | **Might Be Applicable** | **Very Applicable** | **Description**                                              |
| ------------------------------------------------- | ------------------ | ----------------------- | ------------------- | ------------------------------------------------------------ |
| Very large total processing capacity     |                    |                      | √                   | TDengine’s cluster functions can easily improve processing capacity via multi-server-coordination. |
| Extremely high-speed data processing           |                    |                      | √                   | TDengine’s storage and data processing are optimized for IoT, and can process data many times faster than similar products. |
| Extremely fast processing of fine-grained data |                    |                      | √                   | TDengine has achieved the same or better performance than other relational and NoSQL data processing systems. |

### System Maintenance Requirements

| **System Maintenance Requirements**               | **Not Applicable** | **Might Be Applicable** | **Very Applicable** | **Description**                                              |
| ------------------------------------------------- | ------------------ | ----------------------- | ------------------- | ------------------------------------------------------------ |
| Native high-reliability          |                    |                      | √                   | TDengine has a very robust, reliable and easily configurable system architecture to simplify routine operation. Human errors and accidents are eliminated to the greatest extent, with a streamlined experience for operators. |
| Minimize learning and maintenance costs |                    |                      | √                   | In addition to being easily configurable, standard SQL support and the Taos shell for ad hoc queries makes maintenance simpler, allows reuse and reduces learning costs.                                        |
| Abundant talent supply               | √                  |                      |                     | Given the above, and given extensive training and professional services provided by TDengine, it is easy to migrate from existing solutions or create a new solution based on TDengine. |
