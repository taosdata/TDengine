# TDengine 3.0.5.0 Release

## 1. Release Date： ~2023/5/25

## 2. Version：3.0.5.0

## 3. User Manuals: [3.0.5.0 Release User Manuals](https://taosdata.feishu.cn/wiki/wikcn6xVJzJGe5uhV8Bd3AhlfFf) 

## 4. Highlights

## 5. New Features & Improvements

### 5.1 taosc/taosd

1. System stability & performance
   - Improved system stability under high stress data writing
   - Optimized system performance in some query scenarios
   - Altering database replicas doesn't block writing by introducing RAFT Learner
   - Write driven cache for last() and last_row() to improve the query performance
   - Optimized time cost of creating/dropping database
   - Log long queries by default for easy debugging
   - Controlled meta data cache in taosc library
   - dnode can be restored after its data is totally lost (Enterprise only)
2. System security
   - Privilege control at table level (Enterprise only)
   - License key can be updated using SQL command by "root" (Enterprise only)
3. Stream processing
   - Significantly reduced disk I/O and memory usage
   - Stream can be paused/resumed
4. TMQ
   - Consuming progress can be queried
   - Consumers can perform seek operation
   - Consumers can subscribe supertable with tag filtering
   - Consumers can retrieve meta data based on a topic name
   - Improved performance
5. Others
   - Maximum row length is increased to 64KB
   - interp() can be used for super table
   - Python UDF can support multiple versions with "REPLACE" command
   - Partition by and window clause can be followed by "Having" clause

### 5.2 taosX

1. taosX can support more data sources:  Pi, OPC InfluxDB, MQTT
2. taosExplorer can support more data sources: Pi, OPC InfluxDB, MQTT

### 5.3 Connectors

1. Java/Go/Python/Rust connectors support websocket with full functionalities
2. Java/Go/Python/Rust connectors support retrieving consuming offset
3. Java/Go/Python/Rust connectors support seek operation

## 6. Task List

https://jira.taosdata.com:18090/display/~wxzhang/TDengine+3.0.5.0
