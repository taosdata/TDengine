# TDengine 3.2.3.0 Release Notes

## 1. New Features

1. Count Window for stream processing. The starting and ending condition of a stream processing window is determined by the number of rows in the window
2. TDengine monitoring
   - Refactored the metrics collected by taosd service, and refined the Grafana dashboard for monitoring taosd services
   - Collected rich metrics for taosX service, and designed new Grafana dashboard for monitoring resource usage, taosX services, agents, and data in tasks of various data source types
3. New License Mechanism
   - Advanced database features need to be granted, a single license key can grant the fundamental database and/or one or more advanced features
   - Data in features need to be granted, a single license key can grant one or more data source types
4. Data In: Wonderware Historian data can be ingested into TDengine
5. Data Compression between taosX-Agent and taosX to save bandwidth in case they are not in same network

## 2. Improvements

1. Performance improvemnt
   - The cache for last is built when creating super tables and child tables to improve the performance of querying last() or last_row() the first time
   - The cache for last can be utilized when querying both last() and last_row() together in a single SQL statement
   - The performance of querying last_row() when there is a column full of NULL values is improved significantly
   - Performance improvement when transferring data from Pi/OPC to TDengine
   - Performance improvement when transferring data from TDengine 2.6 to TDengine 3.x in case there are millions of tables but each table only has few rows
2. Query optimization
   - Query statement "select count(*)" can filter out empty tables
3. System Operations
   - The impact on data writing when performing "redistribute vgroup" operation is minimized using RAFT learner mechanism
   - Command "restore vnode" can be performed on a single vgroup ID
   - An empty dnode, i.e. without any vnode, can be dropped by force, regardless of online or not
4. Data In Optimizations
   - The initial timestamp can be ignored when writing into TDengine from OPC
   - Advanced options are consolidated and refined for some data source types
   - More options, such as skipRows, delimiter, quoteChar, commentPrefix, have been added for CSV data source
5. Explorer
   - support HTTPS
   - Users can view task related logs
