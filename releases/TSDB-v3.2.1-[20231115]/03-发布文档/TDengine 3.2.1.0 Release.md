# TDengine 3.2.1.0 Release

## 1. Release Date:  Nov/15

### 1.1 User Manuals: [3.2.1.0 User Manuals](https://taosdata.feishu.cn/wiki/LOV7wUUs3i0Yk1kdKArcvSJxnMh) 

### 1.2 New Features

1. 全面支持BI对接新需求（只显示和操作超级表、性能优化、新函数、新语法等）；
2. ~~RSMA功能~~~~ （行为需要重新设计）~~
3. 视图功能
4. 多级存储故障恢复 
5. S3 支持 类 AWS  (大庆油田）
6. 支持按照写入时间间隔和写入批次返回订阅结果（按照真实写入频率回放数据流）
7. 控制流计算结果的写入速度

## 2. Improvements

1. 性能优化（last/last_row+主键查询、last/last_row+其他函数查询）
2. 流计算中的 State/session window 的性能优化 （增加缓存）
