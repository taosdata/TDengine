---
sidebar_label: Time-Series Data Basics
title: Time-Series Data Basics
description: Fundamentals of time-series data
toc_max_heading_level: 4
---

## What Is Time-Series Data?

Time-series data is a sequence of data points ordered by time. In everyday life, measurements collected by devices and sensors are time-series data, and so are securities trading records. Processing time-series data is nothing new: specialized software has long existed in industrial automation and financial markets—for example, PI System in industry and KDB in finance.

Such data may be produced periodically, quasi-periodically, or by events, at high or low sampling rates. Data is typically sent to servers for aggregation and real-time analysis, supporting system monitoring and alerting as well as market forecasting. Data can also be retained for offline analysis—for example, measuring equipment pace and output over a period to optimize configuration and efficiency; analyzing cost distribution in a production process to reduce cost; or reviewing anomalous values over time together with business context to uncover safety risks and reduce downtime.

Over the past two decades, as communication costs fell and sensing technologies and smart devices spread—especially under IoT and Industry 4.0—industrial and IoT enterprises have deployed sensors at many points to monitor equipment, environments, production lines, and entire systems. From wearables and shared mobility to smart meters and environmental monitors, and from elevators and CNC machines to excavators and industrial lines, enormous volumes of real-time data are generated continuously, and time-series data volumes grow exponentially. Take smart meters as an example: sampling every 15 minutes yields 96 records per day; with more than one billion smart meters in China, that is about 96 billion time-series records per day. A connected vehicle may report to the cloud every 10 to 15 seconds and easily produce about 6,000 records per day; with 200 million connected vehicles, daily volume can reach about 1.2 trillion records or more.

As volumes grow exponentially and analytic and real-time computing demand rises—especially with broader use of AI—traditional time-series tools struggle to keep up. Storing, analyzing, and computing tens of terabytes of time-series data per day in real time has become a major technical challenge. Over the past decade, efficient processing of massive time-series data has drawn strong attention across industry worldwide.

## Ten Characteristics of Time-Series Data

Compared with typical internet application data, time-series data has many distinctive traits. As early as 2017, TDengine founder Jeff Tao systematically summarized ten characteristics of time-series data and its applications:

1. **Data is time-ordered and always carries a timestamp**: Connected devices continuously produce data on a schedule or when triggered by external events. Each record corresponds to a point in time and must include a timestamp; otherwise the value is meaningless.

2. **Data is structured**: Data from IoT and industrial equipment, as well as securities trading data, is usually structured, and most values are numeric. For example, current and voltage from a smart meter can be represented as standard 4-byte floating-point numbers.

3. **Each collection point is a data stream**: Data from one device or one stock is independent of data from another. A device’s data comes only from that device—the source is unique.

4. **Updates and deletes are rare**: In IT or internet applications, records are often modified or deleted; under normal conditions, device or trading data is rarely updated or deleted.

5. **Transactions are less critical**: A single device reading has limited value, and consistency requirements are usually less strict than in traditional RDBMSs. Users care more about trends, so complex transaction mechanisms are typically unnecessary.

6. **Write-heavy, read-light relative to internet apps**: In internet apps, a record is often written once and read many times (for example, an article read by many users). Industrial and IoT device data is mostly read automatically by compute and analytics programs, and only a limited number of times; people usually inspect raw data only when anomalies occur.

7. **Users care about trends over a period**: Bank transactions or social posts often matter record by record; in IoT and industrial time series, adjacent points change little, and users care more about trends over windows such as the past five minutes or one hour than about a single timestamp.

8. **Data has a retention period**: Collected data usually follows time-based retention—one day, one week, one month, one year, or longer. Value often depends on the time range; data outside important windows can be deleted in bulk.

9. **Real-time analytics is required**: Most internet big-data use cases are offline; even when “real-time,” latency tolerance is often high (for example, user profiles can wait a day with little impact). Industrial, IoT, and trading platforms often need tight real-time compute for alerting and monitoring so decisions are not missed.

10. **Traffic is steady and predictable**: Given device count and sampling rate, bandwidth, traffic, storage, and daily growth can be estimated accurately—unlike flash-sale traffic spikes or holiday ticketing surges.

These traits create unique requirements and challenges; an efficient time-series platform should exploit them to improve processing capability.

## Typical Application Scenarios

Time-series data appears in many verticals. The following are representative domains.

1. **Power and energy**: Generation, transmission, distribution, and consumption all produce large volumes of time-series data. In wind power, a single turbine may have hundreds of collection points and a large daily volume; monitoring and analysis are essential for reliable generation. On the consumption side, fast computation on smart-meter current and voltage yields latest total usage and peak/off-peak breakdowns, and helps detect abnormal equipment. Systems may also pull a full year of history for machine learning on usage habits, load forecasting, and energy-saving plans, or pull last month’s time-of-use totals for billing. These are typical energy use cases for time-series data.

2. **Connected vehicles / rail transit**: GPS, speed, fuel use, and fault signals are classic time series and support fleet management and optimization. Collection points per vehicle type range from hundreds to thousands; as connected fleets grow, secure upload, storage, query, and analysis of massive time series become industry pain points. For vehicles themselves, this enables tracking, assisted driving, and fault warning; for surrounding services, it also helps—for example, analyzing station sensor series in a smart metro system to show crowding, temperature, and comfort so passengers can choose better options and operators can manage passenger flow.

3. **Smart manufacturing**: Over the past decade, traditional industry digitized rapidly; collection points per plant grew from thousands to hundreds of thousands or millions. Some remote O&M scenarios face tens of thousands of devices and tens of millions of metrics—all classic time series. Industrial big-data pipelines are often complex. In tobacco manufacturing, for example, industrial protocols vary and units differ by equipment type; as points grow, real-time processing is stressed while high performance, availability, and scalability remain required. A platform that meets storage and analytics needs helps enable more intelligent, automated production.

4. **Smart oilfields**: Smart (digital/intelligent) oilfields use IT and equipment to keep reservoir tomography and dynamic production data up to date, improving development efficiency and economics. Over years of construction, drilling, logging, well testing, and production generate large time-series volumes from dozens of device types on oil, water, and gas wells. Building an intelligent control model centered on a production command center requires real-time processing for tens of thousands of wells, valve groups, heaters, and more—efficient writes and queries, lower storage cost, flexible horizontal scale by business, plus usability and security. Some large projects sync field production data in real time to a headquarters cloud for lake ingestion and unified management through edge–cloud collaboration.

5. **IT operations**: Infrastructure (servers, networks, storage) and applications produce abundant time series. Monitoring reveals availability (online status, response health) and metrics such as CPU, memory, disk, and network utilization; it also covers error logs and security events (intrusion, access control), with alert rules to notify operators—finding issues, preventing failures, and optimizing performance for stable systems.

6. **Finance**: Finance is undergoing data-management change. Market data is classic time series, often retained 5–10 years or even beyond 30 years, sometimes covering full trading history across major venues at TB scale, creating storage and query pressure. Quantitative trading is a showcase for time-series value: reading and analyzing massive market series to react to markets, capture opportunities, and control risk—supporting portfolio management, sentiment monitoring, backtesting, signal simulation, and automated reporting.

## Tools Needed to Process Time-Series Data

An end-to-end time-series platform typically needs these core modules.

1. **Database**: Efficient storage and retrieval. In industrial and IoT scenarios, device data volumes are large. Storage must persist to disk with strong compression to control cost; reads must support efficient real-time and historical queries. Traditional options include relational databases such as MySQL and Oracle, and HBase in the Hadoop ecosystem; purpose-built TSDBs include InfluxDB, OpenTSDB, and Prometheus.

2. **Data subscription**: Many applications need fresh data as soon as it arrives for monitoring and real-time AI or other analysis. For privacy and security, apps should subscribe only to data they are allowed to see. The platform therefore needs subscription capabilities so applications can obtain the latest data.

3. **ETL (Extract, Transform, Load)**: IoT and industrial pipelines often extract, clean, and transform before ingest to ensure quality. Collection systems may disagree on standards—temperature in Celsius or Fahrenheit, different time zones or resolutions—so data must be converted before writing to the database.

4. **Stream computing**: IoT, industrial, and financial apps need fast computation on time-series streams for real-time business needs—for example, computing active and reactive power from smart-meter current and voltage immediately. Platforms often use frameworks such as Apache Spark and Apache Flink.

5. **Cache**: Applications need the latest device or instrument state for display, so a cache provides fast access. At extreme scale without caching, ordinary reads and filters introduce latency that fails real-time requirements. Redis is a common cache choice.

Time-series processing needs multiple modules working together—collection, storage, compute, analytics, visualization, and specialized algorithm libraries. Tool choice depends on business needs and data traits; the right combination unlocks value from diverse time-series workloads.

## Why Purpose-Built Time-Series Tools Are Necessary

As noted in “Ten Characteristics of Time-Series Data,” a strong platform must address those traits; “Tools Needed to Process Time-Series Data” outlined the main modules. In practice, processing massive time series is often a large, complex system.

Earlier, tools proliferated for fast-growing internet data, with the Hadoop ecosystem especially popular. Beyond HDFS, MapReduce, HBase, and Hive, general big-data stacks often add Kafka, Redis, Flink, and NoSQL stores such as MongoDB or Cassandra. Such platforms work well for internet scenarios such as user profiling and sentiment analysis.

When industrial and IoT big data emerged, the industry naturally reused that general stack. Most IoT and telematics platforms still follow similar architectures. The path works, but has shortcomings:

1. **Low development efficiency**: Not a single product—usually at least four modules must be integrated. Many lack standard POSIX or SQL interfaces and have their own toolchains, languages, and configs, raising learning cost. Consistency can suffer as data moves between modules. Open-source components have bugs; even with community help, blockers consume time. Overall, assembling the stack needs a strong team and high labor cost.

2. **Low runtime efficiency**: Many open-source components target unstructured internet data (text, video, images), while IoT collection data is mostly time-series and structured. Using unstructured techniques on structured data wastes storage and compute.

3. **High operations cost**: Kafka, HBase, HDFS, Redis, and others each have their own consoles and ops burden. Traditional DBAs mainly managed MySQL or Oracle; now they must learn, configure, and tune many modules. More modules make troubleshooting harder—for example, when a collected record is missing, it is hard to tell quickly whether Kafka, HBase, Spark, or the application is at fault without correlating logs. Stability is also harder to guarantee.

4. **Slow product delivery and margin pressure**: Low R&D efficiency and high ops cost lengthen time to market and miss opportunities. Open-source components keep evolving; tracking versions needs headcount. Outside top internet firms, mid-sized companies often spend more on general big-data staffing than on buying specialized products or services.

5. **Heavy for small private deployments**: IoT and telematics often require private deployment for security; scale per deployment ranges from hundreds to tens of millions of devices. For small deployments, a general big-data stack is often oversized and poor ROI. Some vendors keep two stacks: a big-data platform for large scale and MySQL-class RDBMS for small scale. As history grows or devices increase, relational limits in performance, ops, and scalability become apparent.

Because of these structural gaps, the fast-growing time-series market long lacked tools that were both simple and efficient. In recent years, specialists entered the space—for example, InfluxData in the United States, whose InfluxDB has strong share in IT monitoring. Open source is active too, such as OpenTSDB on HBase; in China, Alibaba, Baidu, Huawei, and others have related OpenTSDB-based products. TDengine Data released TDengine, an independently developed, open-source TSDB that does not depend on third-party components.

Given huge volumes and distinctive access patterns, time-series processing is technically demanding and needs a specialized platform. Efficient real-time processing helps enterprises monitor operations continuously; analyzing historical series supports better decisions on resource use and production configuration.

## Criteria for Choosing a Time-Series Processing Platform

Enterprises need a suitable time-series big-data platform for massive device and trading data. What capabilities should such a platform provide, and how should it differ from a general big-data stack?

1. **Must be distributed**: Massive industrial and IoT volumes cannot be handled on a single machine. The system must scale horizontally and handle high cardinality efficiently. For smart meters, tags may include device ID, city ID, vendor ID, and model ID; hundreds of cities, millions of devices, plus vendors and models can push cardinality beyond tens of billions. Filtering one device among tens of billions of series is hard—this is the classic high-cardinality problem. Even mid-size projects often exceed 100 million series. Architecture must support business cardinality through a distributed design that scales with growth.

2. **Must be high performance**: “High performance” is relative. Hardware footprints differ, but a good platform should not rely only on throwing hardware at the problem—it should excel per node so fewer resources deliver better results and lower TCO. Without strong storage, read, and analytics performance, a specialized TSDB cannot justify itself versus a general big-data platform.

3. **Must support real-time compute**: Internet big-data use cases such as profiling, recommendations, and sentiment analysis often tolerate batch latency. IoT often needs second-level alerts and decisions on fresh data; without real-time compute, business value drops sharply.

4. **Must be carrier-grade reliable**: Industrial and IoT systems often sit on the production path; downtime can stop lines, cause losses, or hurt end-user service. A smart-meter outage can affect many customers. The system must be highly available, with real-time backup, geo-DR, online software/hardware upgrades, and data-center migration.

5. **Must provide efficient caching**: Many scenarios need the latest device state for alerts and dashboards. The system should efficiently return latest states for all devices or filtered subsets.

6. **Must support real-time stream computing**: Alerts and forecasts often depend on aggregating one or more device streams over time windows, not single samples. Needs vary by scenario, so user-defined functions should be allowed.

7. **Must support data subscription**: As with general platforms, the same data is often used by many apps; the system should notify subscribers of new data. For privacy and security, subscription must be controllable—for example, allowing hourly average power but not raw current and voltage.

8. **Must sustain stable continuous writes**: Connected-device traffic is usually steady and writable capacity can be planned; queries and ad-hoc analysis vary more and can consume unpredictable resources. Enough capacity must be reserved so writes are not starved and data is not lost—in other words, a write-first system.

9. **Must unify real-time and historical access**: Fresh data may be in cache and history on persistent media, possibly tiered by age. The platform should hide storage differences and expose one interface: accessing newly collected data and years-old history should differ mainly by time parameters.

10. **Must support flexible multi-dimensional analysis**: Device data is analyzed by region, model, supplier, operator, and more; dimensions grow with the business and cannot all be predefined. The system needs flexible ways to add dimensions on demand.

11. **Must support ad-hoc analysis and query**: For productivity, provide a CLI or allow SQL via other tools without coding everything. Results should export easily into charts.

12. **Must support downsampling, interpolation, and specialized functions**: Raw rates can be high; analysis often uses downsampled data. Devices are hard to synchronize, so values at a given time often need interpolation (linear, fixed-value, and other strategies). Beyond general stats, industrial internet often needs time-weighted averages, cumulative sums, deltas, and similar functions.

13. **Must offer flexible data-management policies**: Large systems hold many data types—raw and derived—with different rates, retention, replica needs, and access latency. The platform should offer configurable, coexisting policies.

14. **Must be open**: Support standard SQL, provide C/C++, Java, Go, Python, and RESTful APIs, and integrate with Spark, R, Matlab, and similar tools so ML/AI and other apps can extend the platform rather than trapping data in a silo.

15. **Must support heterogeneous environments**: Big-data platforms are built over long periods with mixed server and storage generations; the system should run across diverse hardware tiers.

16. **Must support edge–cloud collaboration**: Flexible mechanisms should upload edge data to the cloud—raw, processed, or filtered only—and allow canceling or adjusting policies so data can be aggregated for centralized decisions.

17. **Needs unified administration**: Visibility into runtime state, cluster/user/resource management, and seamless integration with third-party IT monitoring.

18. **Must support private deployment**: Many enterprises prefer private installs for security, while traditional IT teams are limited—so install, deploy, and operate should stay simple and maintainable.

In short, a time-series big-data platform should be efficient, scalable, real-time, reliable, flexible, open, simple, and easy to operate. Increasingly, enterprises migrate time-series workloads from general big-data stacks or relational databases to purpose-built platforms so massive series can be processed quickly and support sustained growth.

## Time-Series Database Fundamentals

The following articles provide further background on time-series databases.

1. How do TSDBs differ from relational databases, NoSQL, and other general-purpose databases? See [What Is a Time-Series Database?](https://tdengine.com/what-is-a-time-series-database/).

2. What is time-series data, and why is a general big-data architecture a poor fit? See [Characteristics of Time Series Data](https://tdengine.com/characteristics-of-time-series-data/).

3. Interest in TSDBs is high; at least 20 new TSDBs shipped in the past decade. How should you choose? See [How to Choose the Best Time-Series Database](https://tdengine.com/how-to-choose-the-best-time-series-database/).

4. The data model is central; different TSDBs use different models. How do InfluxDB, TDengine, and others compare? See [Data Model Comparison Between Time-Series Databases](https://tdengine.com/data-model-comparison-between-time-series-databases/).

5. High cardinality has long challenged mainstream TSDBs. How does TDengine 3.0 address it? See [High Cardinality in Time Series Data](https://tdengine.com/high-cardinality/).

6. Based on the TSBS standard dataset, TDengine published comparisons with InfluxDB and TimescaleDB on write, query, and disk usage. See:

    - [IoT Performance: InfluxDB and TimescaleDB vs. TDengine](https://tdengine.com/iot-performance-comparison-influxdb-and-timescaledb-vs-tdengine/)
    - [DevOps Performance: InfluxDB and TimescaleDB vs. TDengine](https://tdengine.com/devops-performance-comparison-influxdb-and-timescaledb-vs-tdengine/)
    - [TSBS IoT Performance Report: TDengine, InfluxDB, and TimescaleDB](https://tdengine.com/tsbs-iot-performance-report-tdengine-influxdb-and-timescaledb/)
