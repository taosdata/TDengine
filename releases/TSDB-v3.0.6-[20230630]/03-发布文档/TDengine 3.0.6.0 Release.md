# TDengine 3.0.6.0 Release 

## 1. Release Date: July/1

## 2. User Manuals: [3.0.6.0 Release User Manuals](https://taosdata.feishu.cn/wiki/MIjnwc5N3iXBR9ktXJHcQn1FnIf) 

## 3. Highlights

1. Cluster can be scaled out using "split vgroup" (Enterprise only)
2. Performance improvement for order by + limit
3. Drop a table automatically based on TTL parameter since its last writing

## 4. New Features & Improvements

### 4.1 taosc/taosd

1. Cluster
   - split vgroup (Enterprise)
2. interp()
   - Memory usage optimization
   - Constant expression can be used in interp fill()
   - Can be applied to single timestamp
3. Query
   - Performance optimization for order by + limit
4. Data Types
   - GeoMetry data type
5. Storage Engine
   - TTL based on the last writing
6. TMQ
   - Internal performance optimization
7. TDengine CLI
   - Show the parameters of subscriptions
   - Show subscription process

### 4.2 Connectors

1. Python connector supports assignment/seek operation for data subscrition over WebSocket
2. Python connector supports STMT writing over websocket
3. Python connector supports schemaless over websocket

## 5. Task List

https://jira.taosdata.com:18090/pages/viewpage.action?pageId=231997918
