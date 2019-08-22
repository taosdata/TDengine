[![TDengine](TDenginelogo.png)](https://www.taosdata.com)

# What is TDengine？

TDengine is an open-sourced big data platform under [GNU AGPL v3.0](http://www.gnu.org/licenses/agpl-3.0.html), designed and optimized for the Internet of Things (IoT), Connected Cars, Industrial IoT, and IT Infrastructure and Application Monitoring. Besides the 10x faster time-series database, it provides caching, stream computing, message queuing and other functionalities to reduce the complexity and cost of development and operation.

- **10x Faster on Insert/Query Speeds**: Through the innovative design on storage, on a single-core machine, over 20K requests can be processed, millions of data points can be ingested, and over 10 million data points can be retrieved in a second. It is 10 times faster than other databases.

- **1/5 Hardware/Cloud Service Costs**: Compared with typical big data solutions, less than 1/5 of computing resources are required. Via column-based storage and tuned compression algorithms for different data types, less than 1/10 of storage space is needed.

- **Full Stack for Time-Series Data**: By integrating a database with message queuing, caching, and stream computing features together, it is no longer necessary to integrate Kafka/Redis/HBase/Spark or other software. It makes the system architecture much simpler and more robust.

- **Powerful Data Analysis**: Whether it is 10 years or one minute ago, data can be queried just by specifying the time range. Data can be aggregated over time, multiple time streams or both. Ad Hoc queries or analyses can be executed via TDengine shell, Python, R or Matlab.

- **Seamless Integration with Other Tools**: Telegraf, Grafana, Matlab, R, and other tools can be integrated with TDengine without a line of code. MQTT, OPC, Hadoop, Spark, and many others will be integrated soon.

- **Zero Management, No Learning Curve**: It takes only seconds to download, install, and run it successfully; there are no other dependencies. Automatic partitioning on tables or DBs. Standard SQL is used, with C/C++, Python, JDBC, Go and RESTful connectors.

# Documentation
For user manual, system design and architecture, engineering blogs, refer to [TDengine Documentation](https://www.taosdata.com/en/documentation/)
 for details. The documentation from our website can also be downloaded locally from *documentation/tdenginedocs-en* or *documentation/tdenginedocs-cn*.

# Building
At the moment, TDengine only supports building and running on Linux systems. You can choose to [install from packages](https://www.taosdata.com/en/getting-started/#Install-from-Package) or from the source code. This quick guide is for installation from the source only.

To build TDengine, use [CMake](https://cmake.org/) 2.8 or higher versions in the project directory. Install CMake for example on Ubuntu:
```
sudo apt-get install -y cmake build-essential
```

To compile and package the JDBC driver source code, you should have a Java jdk-8 or higher and Apache Maven 2.7 or higher installed. 
To install openjdk-8 on Ubuntu:
```
sudo apt-get install openjdk-8-jdk
```
To install Apache Maven on Ubuntu:
```
sudo apt-get install maven
```

Build TDengine:
```cmd

mkdir build && cd build
cmake .. && cmake --build .
```

# Quick Run
To quickly start a TDengine server after building, run the command below in terminal:
```cmd
./build/bin/taosd -c test/cfg
```
In another terminal, use the TDengine shell to connect the server:
```
./build/bin/taos -c test/cfg
```
option "-c test/cfg" specifies the system configuration file directory. 

# Installing
After building successfully, TDengine can be installed by:
```cmd
make install
```
Users can find more information about directories installed on the system in the [directory and files](https://www.taosdata.com/en/documentation/administrator/#Directory-and-Files) section. It should be noted that installing from source code does not configure service management for TDengine.
Users can also choose to [install from packages](https://www.taosdata.com/en/getting-started/#Install-from-Package) for it.

To start the service after installation, in a terminal, use:
```cmd
taosd
```

Then users can use the [TDengine shell](https://www.taosdata.com/en/getting-started/#TDengine-Shell) to connect the TDengine server. In a terminal, use:
```cmd
taos
```

If TDengine shell connects the server successfully, welcome messages and version info are printed. Otherwise, an error message is shown.

# Try TDengine
It is easy to run SQL commands from TDengine shell which is the same as other SQL databases.
```sql
create database db;
use db;
create table t (ts timestamp, a int);
insert into t values ('2019-07-15 00:00:00', 1);
insert into t values ('2019-07-15 01:00:00', 2);
select * from t;
drop database db;
```

# Developing with TDengine
### Official Connectors

TDengine provides abundant developing tools for users to develop on TDengine. Follow the links below to find your desired connectors and relevant documentation.

- [Java](https://www.taosdata.com/en/documentation/connector/#Java-Connector)
- [C/C++](https://www.taosdata.com/en/documentation/connector/#C/C++-Connector)
- [Python](https://www.taosdata.com/en/documentation/connector/#Python-Connector)
- [Go](https://www.taosdata.com/en/documentation/connector/#Go-Connector)
- [RESTful API](https://www.taosdata.com/en/documentation/connector/#RESTful-Connector)
- [Node.js](https://www.taosdata.com/en/documentation/connector/#Node.js-Connector)

### Third Party Connectors

The TDengine community has also kindly built some of their own connectors! Follow the links below to find the source code for them.

- [Rust Connector](https://github.com/taosdata/TDengine/tree/master/tests/examples/rust)
- [.Net Core Connector](https://github.com/maikebing/Maikebing.EntityFrameworkCore.Taos)

# TDengine Roadmap
- Support event-driven stream computing
- Support user defined functions
- Support MQTT connection
- Support OPC connection
- Support Hadoop, Spark connections
- Support Tableau and other BI tools

# Contribute to TDengine

Please follow the [contribution guidelines](CONTRIBUTING.md) to contribute to the project.
