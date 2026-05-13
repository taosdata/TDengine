# TDengine 3.2.2.0 Release

## 1. Release Date:  12/15

### 1.1 User Manuals: [3.2.2.0 User Manuals](https://taosdata.feishu.cn/wiki/KyMhw7mbVibmmykUujOc36ZnnUh) 

### 1.2 New Features

1. Compact 可观测可操作
2. 流计算支持 event window

## 2. Improvements

1. 性能优化（超级表排序）
2. 提升 split vgroup 的效率
3. 提升 redistribute vnode 的效率
4. S3 
   - 支持断点续传
   - 预读优化
5. Windows 性能优化
6. 整数类型可以写入浮点数
7. 性能优化：Partition by
1. 

## 3. 内部 （ 不发布）

1. Decimal 存储部分
2. 流计算：Checkpoint 从 vnode 调整到 task
3. 流计算：解决三副本下的数据丢失
4. 流计算：多级聚合算子的适应性改造和重构
5. 流计算：snode 增加 backup 服务
6. TSMA
7. Join
