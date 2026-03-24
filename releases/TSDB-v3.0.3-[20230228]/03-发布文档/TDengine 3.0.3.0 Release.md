# TDengine 3.0.3.0 Release

## 1. Release Date： 2023/2/28

## 2. Version：3.0.3.0

## 3. User Manuals: [20230228 Release User Manuals](https://taosdata.feishu.cn/wiki/wikcndmzTxC4bpdOyWkYS5QFhsg) 

## 4. Highlights

1. Index can be created on any specified tag, not only first tag 
2. taosX supports rules engine for data transformation
3. taosExplorer, the web based management tool for TDengine, is released with a similar UI/UE as cloud service
4. 3.0 OEM version can be built automatically         

## 5. Improvements

1. Compact out-of-order data to improve query performance
2. Data writing is not blocked by long query
3. Improved stability for 3 replications
4. Significant performance improvement for schemaless writing
5. Enterprise license can be generated or updated based on cluster ID besides machine code
6. Enterprise license can be updated to increase or decrease the number of data collection points
7. Enterprise license can be updated to extend or shrink expiration date

## 6. New Features

### 6.1 taosd/taosc

1. Event window for batch processing
2. Topic subscription can be granted and revoked specifically
3. Index can be created can any tag of any type
4. The result of stream computing can be written into an existing supertable
5. Adapt existing cluster to new FQDN

### 6.2 taosTools

1. vnode can be specified on taosBenchmark CLI 
2. taosBenchmark can write data to a specified range of child tables
3. Legend alias can be set in Grafana plugin
4. Alert can be supported for multi-dimentional data

### 6.3 Connectors

1. Java connector supports TMQ over websocket
2. NodeJS connector supports TMQ over websocket
3. Python connector uses Kafka live API for data subscription

### 6.4 taosX

1. Data source definition and declarative configuration.
2. Transformer definition and expression/rule engine.
3. Implement a simple one-way data streaming format.
4. Simple license control: taosX can only work with taod with enterprise license

### 6.5 taosExplorer

1. Experimental community version, including Dashboard, Data In, Data Out, Explorer, Programming, Tools
2. Viewing topics, streams, administration data, and cluster settings

### 6.6 OEM

1. OEM versions can be released in automatic way

## 7. Development Tasks

taosX: 
TD-21891

taosExplorer: 
TD-21258

<!-- Unsupported block type: 999 -->
