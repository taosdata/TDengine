#Documentation 

TDengine is a highly efficient platform to store, query, and analyze time-series data. It works like a relational database, but you are strongly suggested to read through the following documentation before you experience it.

##Getting Started

- Quick Start: download, install and experience TDengine in a few seconds
- TDengine Shell: command-line interface to access TDengine server
- Major Features: insert/query, aggregation, cache,  pub/sub, continuous query   

## Data Model and Architecture

- Data Model: relational database model, but one table for one device with static tags
- Architecture: Management Module, Data Module, Client Module
- Writing Process: records recieved are written to WAL, cache, then ack is sent back to client
- Data Storage: records are sharded in the time range, and stored column by column 

##TAOS SQL 

- Data Types: support timestamp, int, float, double, binary, nchar, bool, and other types
- Database Management: add, drop, check databases
- Table Management: add, drop, check, alter tables
- Inserting Records: insert one or more records into tables, historical records can be imported
- Data Query: query data with time range and filter conditions, support limit/offset
- SQL Functions: support aggregation, selector, transformation functions
- Downsampling: aggregate data in successive time windows, support interpolation

##STable: Super Table

- What is a Super Table: an innovated way to aggregate tables
- Create a STable: it is like creating a standard table, but with tags defined
- Create a Table via STable: use STable as the template, with tags specified
- Aggregate Tables via STable: group tables together by specifying the tags filter condition
- Create Table Automatically: create tables automatically with a STable as a template
- Management of STables: create/delete/alter super table just like standard tables
- Management of Tags: add/delete/alter tags on super tables or tables  

##Advanced Features

- Continuous Query: query executed by TDengine periodically with a sliding window
- Publisher/Subscriber: subscribe to the newly arrived data like a typical messaging system 
- Caching: the newly arrived data of each device/table will always be cached

##Connector

- C/C++ Connector: primary method to connect to the server through libtaos client library
- Java Connector: driver for connecting to the server from Java applications using the JDBC API
- Python Connector: driver for connecting to the server from Python applications 
- RESTful Connector: a simple way to interact with TDengine via HTTP
- Go Connector: driver for connecting to the server from Go applications
- Node.js Connector: driver for connecting to the server from node applications

##Connections with Other Tools

- Telegraf: pass the collected DevOps metrics to TDengine 
- Grafana: query the data saved in TDengine and visualize them 
- Matlab: access TDengine server from Matlab via JDBC
- R: access TDengine server from R via JDBC 

##Administrator

- Directory and Files: files and directories related with TDengine
- Configuration on Server: customize IP port, cache size, file block size and other settings
- Configuration on Client: customize locale, default user and others   
- User Management: add/delete users, change passwords
- Import Data: import data into TDengine from either script or CSV file
- Export Data: export data either from TDengine shell or from tool taosdump
- Management of Connections, Streams, Queries: check or kill the connections, queries
- System Monitor: collect the system metric, and log important operations

##More on System Architecture

- Storage Design: column-based storage with optimization on time-series data 
- Query Design: an efficient way to query time-series data
- Technical blogs to delve into the inside of TDengine

## More on IoT Big Data

- [Characteristics of IoT Big Data](https://www.taosdata.com/blog/2019/07/09/characteristics-of-iot-big-data/)
- [Why don’t General Big Data Platforms Fit IoT Scenarios?](https://www.taosdata.com/blog/2019/07/09/why-does-the-general-big-data-platform-not-fit-iot-data-processing/)
- [Why TDengine is the Best Choice for IoT Big Data Processing?](https://www.taosdata.com/blog/2019/07/09/why-tdengine-is-the-best-choice-for-iot-big-data-processing/)

##Tutorials & FAQ

- <a href='https://www.taosdata.com/en/faq'>FAQ</a>: a list of frequently asked questions and answers 
- <a href='https://www.taosdata.com/en/blog/?categories=4'>Use cases</a>: a few typical cases to explain how to use TDengine in IoT platform

