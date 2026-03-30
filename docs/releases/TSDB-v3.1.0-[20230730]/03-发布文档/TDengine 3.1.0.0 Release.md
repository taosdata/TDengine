# TDengine 3.1.0.0 Release

## 1. Release Date: July/31

### 1.1 User Manuals: [3.1.0.0 Release User Manuals](https://taosdata.feishu.cn/wiki/PIMDwnHK4iCBeMk7Sw3ceLLLnye) 

### 1.2 Highlights

1. Storage engine optimization
2. Performance improvement for query
3. Cluster can continue to work after the disk of one node is broken

## 2. New Features/Improvements

### 2.1 taosc/taosd

1. Storage engine optimization, to get the effects below
   - Improve the performance of high cardinal but low writing frequence
   - Compacting data doesn't block data writing
   - Data migration between multiple tier storages doesn't block writing
   - Improve the performance of querying count(*) for out of order data
2. Multiple tier stroage
   - Configurable time for migrating data
3. Query
   - Performance improvement for Join
   - Performance improvement for order by non-primary key + limit
   - Performance and memory usage improvement for stable order by primary key + limit
   - Configuration display optimization (show commands)
4. Stream Processing and TMQ
   - fill_history() refactoring: generate correct result with resource usage under control
   - Consumer can subscribe only meta changes
   - Stream can be paused/resumed with fill_history
   - Vnode can be migrated with existence of stream and TMQ
5. Fault tolerance
   - Cluster (3 replica of at least 3 dnodes) can continue to serve even when the disk of one dnode is totally broken

## 3. Task Link

https://jira.taosdata.com:18090/display/~wxzhang/TDengine+3.1.0.0
