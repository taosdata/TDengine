# TDengine Introduction

## <a class="anchor" id="intro"></a> About TDengine

TDengine is an innovative Big Data processing product launched by Taos Data in the face of the fast-growing Internet of Things (IoT) Big Data market and technical challenges. It does not rely on any third-party software, nor does it optimize or package any open-source database or stream computing product. Instead, it is a product independently developed after absorbing the advantages of many traditional relational databases, NoSQL databases, stream computing engines, message queues, and other software. TDengine has its own unique Big Data processing advantages in time-series space.

One of the modules of TDengine is the time-series database. However, in addition to this,  to reduce the complexity of research and development and the difficulty of system operation, TDengine also provides functions such as caching, message queuing, subscription, stream computing, etc. TDengine provides a full-stack technical solution for the processing of IoT and Industrial Internet BigData. It is an efficient and easy-to-use IoT Big Data platform. Compared with typical Big Data platforms such as Hadoop, TDengine has the following distinct characteristics:

- **Performance improvement over 10 times**: An innovative data storage structure is defined, with each single core can process at least 20,000 requests per second, insert millions of data points, and read more than 10 million data points, which is more than 10 times faster than other existing general database.
- **Reduce the cost of hardware or cloud services to 1/5**: Due to its ultra-performance, TDengine’s computing resources consumption is less than 1/5 of other common Big Data solutions; through columnar storage and advanced compression algorithms, the storage consumption is less than 1/10 of other general databases.
- **Full-stack time-series data processing engine**: Integrate database, message queue, cache, stream computing, and other functions, and the applications do not need to integrate with software such as Kafka/Redis/HBase/Spark/HDFS, thus greatly reducing the complexity cost of application development and maintenance.
- **Powerful analysis functions**: Data from ten years ago or one second ago, can all be queried based on a specified time range. Data can be aggregated on a timeline or multiple devices. Ad-hoc queries can be made at any time through Shell, Python, R, and MATLAB.
- **Seamless connection with third-party tools**: Integration with Telegraf, Grafana, EMQ, HiveMQ, Prometheus, MATLAB, R, etc. without even one single line of code. OPC, Hadoop, Spark, etc. will be supported in the future, and more BI tools will be seamlessly connected to.
- **Zero operation cost & zero learning cost**: Installing clusters is simple and quick, with real-time backup built-in, and no need to split libraries or tables. Similar to standard SQL, TDengine can support RESTful, Python/Java/C/C++/C#/Go/Node.js, and similar to MySQL with zero learning cost.

With TDengine, the total cost of ownership of typical IoT, Internet of Vehicles, and Industrial Internet Big Data platforms can be greatly reduced. However, it should be pointed out that due to making full use of the characteristics of IoT time-series data, TDengine cannot be used to process general data from web crawlers, microblogs, WeChat, e-commerce, ERP, CRM, and other sources.

![TDengine Technology Ecosystem](page://images/eco_system.png)

<center>Figure 1. TDengine Technology Ecosystem</center>

## <a class="anchor" id="scenes"></a>Overall Scenarios of TDengine

As an IoT Big Data platform, the typical application scenarios of TDengine are mainly presented in the IoT category, with users having a certain amount of data. The following sections of this document are mainly aimed at  IoT-relevant systems. Other systems, such as CRM, ERP, etc., are beyond the scope of this article.

### Characteristics and Requirements of Data Sources

From the perspective of data sources, designers can analyze the applicability of TDengine in target application systems as following.

| **Data Source Characteristics and Requirements**         | **Not Applicable** | **Might Be Applicable** | **Very Applicable** | **Description**                                              |
| -------------------------------------------------------- | ------------------ | ----------------------- | ------------------- | :----------------------------------------------------------- |
| A huge amount of total data                              |                    |                         | √                   | TDengine provides excellent scale-out functions in terms of capacity, and has a storage structure matching high compression ratio to achieve the best storage efficiency in the industry. |
| Data input velocity is occasionally or continuously huge |                    |                         | √                   | TDengine's performance is much higher than other similar products. It can continuously process a large amount of input data in the same hardware environment, and provide a performance evaluation tool that can easily run in the user environment. |
| A huge amount of data sources                            |                    |                         | √                   | TDengine is designed to include optimizations specifically for a huge amount of data sources, such as data writing and querying, which is especially suitable for efficiently processing massive (tens of millions or more) data sources. |

### System Architecture Requirements

| **System Architecture Requirements**              | **Not Applicable** | **Might Be Applicable** | **Very Applicable** | **Description**                                              |
| ------------------------------------------------- | ------------------ | ----------------------- | ------------------- | ------------------------------------------------------------ |
| Require a simple and reliable system architecture |                    |                         | √                   | TDengine's system architecture is very simple and reliable, with its own message queue, cache, stream computing, monitoring and other functions, and no need to integrate any additional third-party products. |
| Require fault-tolerance and high-reliability      |                    |                         | √                   | TDengine has cluster functions to automatically provide high-reliability functions such as fault tolerance and disaster recovery. |
| Standardization specifications                    |                    |                         | √                   | TDengine uses standard SQL language to provide main functions and follow standardization specifications. |

### System Function Requirements

| **System Architecture Requirements**              | **Not Applicable** | **Might Be Applicable** | **Very Applicable** | **Description**                                              |
| ------------------------------------------------- | ------------------ | ----------------------- | ------------------- | ------------------------------------------------------------ |
| Require completed data processing algorithms built-in |                    | √                    |                     | TDengine implements various general data processing algorithms, but has not properly handled all requirements of different industries, so special types of processing shall be processed at the application level. |
| Require a huge amount of crosstab queries             |                    | √                    |                     | This type of processing should be handled more by relational database systems, or TDengine and relational database systems should fit together to implement system functions. |

### System Performance Requirements

| **System Architecture Requirements**              | **Not Applicable** | **Might Be Applicable** | **Very Applicable** | **Description**                                              |
| ------------------------------------------------- | ------------------ | ----------------------- | ------------------- | ------------------------------------------------------------ |
| Require larger total processing capacity     |                    |                      | √                   | TDengine’s cluster functions can easily improve processing capacity via multi-server-cooperating. |
| Require high-speed data processing           |                    |                      | √                   | TDengine’s storage and data processing are designed to be optimized for IoT, can generally improve the processing speed by multiple times than other similar products. |
| Require fast processing of fine-grained data |                    |                      | √                   | TDengine has achieved the same level of performance with relational and NoSQL data processing systems. |

### System Maintenance Requirements

| **System Architecture Requirements**              | **Not Applicable** | **Might Be Applicable** | **Very Applicable** | **Description**                                              |
| ------------------------------------------------- | ------------------ | ----------------------- | ------------------- | ------------------------------------------------------------ |
| Require system with high-reliability         |                    |                      | √                   | TDengine has a very robust and reliable system architecture to implement simple and convenient daily operation with streamlined experiences for operators, thus human errors and accidents are eliminated to the greatest extent. |
| Require controllable operation learning cost |                    |                      | √                   | As above.                                                    |
| Require abundant talent supply               | √                  |                      |                     | As a new-generation product, it’s still difficult to find talents with TDengine experiences from market. However, the learning cost is low. As the vendor, we also provide extensive operation training and counselling services. |
