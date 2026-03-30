# 热更新 asynclog 参数

## 1. 相关链接 

TS-4036

## 2. 说明

```sql
-- dnode 1 同步 log
ALTER DNODE 1 'asynclog 0';
-- dnode 1 异步 log
ALTER DNODE 1 'asynclog 1';

-- taos 同步 log
ALTER LOCAL 'asynclog 0';
-- taos 异步 log
ALTER LOCAL 'asynclog 1';
```

调整会立即生效，alter 前已经存储在 asyncBuffer 中的数据不会立即下刷至文件中。
会出现 **短暂日志时间乱序现象**，但不会造成日志丢失。
