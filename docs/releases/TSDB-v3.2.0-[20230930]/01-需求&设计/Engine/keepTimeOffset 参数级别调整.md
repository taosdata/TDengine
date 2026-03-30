# keepTimeOffset 参数级别调整

## 1. 简介

将 keepTimeOffset 配置参数从 Dnode 移动至 DB 中
除配置方式及生效范围外，其他影响与 “Dnode 级别 keepTimeOffset（[keepTimeOffset](https://taosdata.feishu.cn/wiki/G4MPwmj6XiqJtkktnyUcjnDKnEb) ）”一致

## 2. 废弃配置

```c
// taos.cfg
keepTimeOffset 0
// 将被忽略

// SQL: ALTER DNODE
alter dnode 1 'keeptimeoffset 10';
// DB error: Invalid config option (0.006634s)
```

## 3. 新增配置

```c
// SQL: CREATE DATABASE
create database db KEEP_TIME_OFFSET 10;

// SQL: ALTER DATABASE
alter database db KEEP_TIME_OFFSET 0;
```

## 4. 升级行为

1. taos.cfg 中配置的 keepTimeOffset  参数将被忽略
2. KEEP_TIME_OFFSET 参数默认值为 0，原有 DB 升级后自动应用该默认值
3. 按需对原有 DB 调用 'alter database' 以适配原有 cfg 中配置
