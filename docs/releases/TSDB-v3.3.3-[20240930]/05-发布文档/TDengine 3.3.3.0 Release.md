# TDengine 3.3.3.0 Release

Release Date： 2024/9/30
Version：3.3.3.0
User Manuals: [3.3.3.0 Functional Spec](https://taosdata.feishu.cn/wiki/NoyAwJtXOicJH9kLsFDcMLDfnVc)

## 1. Highlights

1. MongoDB Data Replication to TDengine

## 2. New Features & Improvements

### 2.1 Engine

1. Keyword `level` can be used as column name
2. Performance improvement in querying supertable with `tbname` as filter condition
3. Added some useful fields, like user name, in the system table Information_Schema.ins_subscriptions
4. Alias names can be enclosed using back quotes
5. Azure Blob (Object Storage) can be used with the support of Felxify
6. CSV file can be used to create tables in bunch
7. SQL functions: added some new functions and optimized the behavior of some existing functions
8. The maximum interval of TSMA is enlarged to 1 day
9. Significantly improved the speed of restarting taosd service when the data subscription has been running long time
10. The consumers of `lost` status can be cleaned using SQL command
11. The display precision of `duration` and `keep` parameters of a database was optimized
12. Optimized the retention policy of slow query log files
13. Internal Quality
   - Remove vulnerable code by completing exception handling
   - Make sure all memory allocations are checked and add proper exception handling
   - Replace memory unsafe function with memroy safe function
   - Add return code checking policy in CI to guarantee new code quality
   - Add fault injection mechanism for memory allocation, file I/O and RPC communication

### 2.2 Tools

1. General
   - Unified logs for taosKeeper and taosAdapter
      - ensuring there is a QID to associate all related log entries of a specific request
      - refining log file retention and compression policy
2. taosAdapter 
   - taosAdapter will reject new request when there are piled requests
   - taosAdapter returns affected rows for schemaless writing
3. Monitor
   - Alert template can imported in Grafana 7.5 to 11

### 2.3 Connectors

1. ODBC 
   - 32-bit (new connector)
   - Support View
   - Varbinary data type
   - Geometry data type
2. C/C++ 
   - WebSocket Connector (new connector)
3. JDBC 
   - Supports Mybatis
   - Performance improvement of querying data
4. C#
   - Performance improvement for websocket
5. Python
   - New STMT interface
   - Varbinary data type
   - Geometry data type
6. Go
   - New STMT interface
7. Node.JS
   - Performance improvement for websocket
   - Varbinary data type
   - Geometry data type

### 2.4 taosX/taosExplorer

1. MongoDB (new data source)
   - data replication from Mongo DB to TDengine
2. TDengine
   - Data replication can be performed from a TDengine database to another TDengine database with equal or higher time precision
   - Performance improvement for replicating data from TDengine to TDengine
   - Performance improvement in TDengine Active-Active solution
3. Kafka
   - Performance improvement for replicating data from Kafka to TDengine
   - Group ID can be configured for Kafka data in task
4. RDBMS
   - Resolved the out-of-order issue in replicating data from MySQL/PostgreSQL/SQLServer/OracleDB/MongoDB to TDengine
5. General
   - Plugin mechanism for transformer parser
   - Timeout parameter configuration is unified in multiple data sources
   - Slow query log can be viewed in taosExplorer
   - Favorite SQL is persisted so that it is still available after restarting the browser or reconnecting to taosExplorer
   - Unified in the input format of data source server address and port
   - UI optimization for some legacy data sources
6. Pi: 
   - Updating/Deleting historical data in Pi system can be replicated to TDengine
   - Tag changes in Pi system can be replicated to TDengine
   - Deleting an element in Pi system will trigger automatic deletion of the corresponding table in TDengine
7. OPC
   - OPC data point can be added dynamically using GUI after importing the initial CSV based configuration
   - Polling interval can be configured for OPC task in subscription mode
8. Unified the log format
   - ensuring there is a QID to associate all the related log entries
   - refining log retention and compression policy

## 3. New Platforms

1. MacOS client installation pacakge
2. TDengine can run on 飞腾d2000+麒麟V10
3. TDengine can run on loongArch64+麒麟V10

## 4. Document

1. TDengine Enterprise document in PDF (can be downloaded anytime from internal NAS)

## 5. Attachments：

[3.3.3.0 发版通知](https://taosdata.feishu.cn/wiki/F4T0wClewiI8LtkEPt0cXud9nrb)
[3.3.3.0 中英文Release Notes](https://taosdata.feishu.cn/wiki/Pj5Tw2yeQize7UkepZScXvRCnmb)
[3.3.3.0 冒烟测试报告](https://taosdata.feishu.cn/wiki/G2fLwfrwxiraUtkQSGYcxiU6nFg)
[基线性能测试报告-V3.3.3.0 Release](https://taosdata.feishu.cn/wiki/VKMmwO9IFiEJGZk13l7cjk5pnOc)
[数据同步基线性能测试报告](https://taosdata.feishu.cn/wiki/CSoKwq28niiECSkizt5cSm6snTY)
