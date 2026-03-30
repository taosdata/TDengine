# TDengine 3.0.4.0 Release

## 1. Release Date： 2023/3/31

## 2. Version：3.0.4.0

## 3. User Manuals: [3.0.4.0 Release User Manuals](https://taosdata.feishu.cn/wiki/wikcnapama4kqF87vvlK17u8dSf) 

## 4. Highlights

1. Stability Improvement
2. Performance Optimization
3. UDF in Python language
4. Data compact based on specified time range
5. Rebalance workload after restarting dnodes
6. taosX can transfer data from PI/OPC to TDengine
7. taosExplorer version 2 (majorly for data in)

## 5. New Features & Improvements

### 5.1 taosd/taosc

1. Dynamic change of database parameters: stt_trigger and minRows
2. Cleaning WAL based on WAL_RETENTION_PERIOD and WAL_RETENTION_SIZE
3. Performance Optimization for mode+interval
4. Performance Optimization for percentile
5. Rebalance the vgroup leaders after restarting dnode
6. Keep the cluster of remaining dnodes still work after the data of one dnode is broken
7. New metrics: streams_total and topics_total
8. Data compact based on a specified time range

### 5.2 taosTools

1. taosBenchmark supports specified different sample data for subtable
2. taosBenchmark support create table interval
3. taosAdapter support reqID

### 5.3 Connectors

1. PI connector Phase II (integrated with taosX)
2. OPC connector Phase II (integrated with taosX)
3. Java connector support reqID
4. Python connector support reqID

### 5.4 taosX

1. Transformer: data type convertion
2. PI connector integration and transfer data from PI data source to TDengine
3. OPC connector integration and transfer data from OPC to TDengine
4. Typed configration format declaration (for UI / visualization).

### 5.5 taosExplorer

1. taosX data source visualization.
   - PI Data In
   - OPC Data In
   - Data Source configuration wizard
2. Data In task administration.

## 6. Infrastructure Improvement

1. Release script refactor 
  TD-18933

@李珲 Please put the CI improvement plan (like, Alpine, Windows) here

## 7. Task List

<!-- Unsupported block type: 999 -->
