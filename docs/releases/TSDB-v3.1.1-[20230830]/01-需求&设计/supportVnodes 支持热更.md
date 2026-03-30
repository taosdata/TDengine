# supportVnodes 支持热更

## 1. 限制

仅企业版可动态调整

## 2. 参数说明

- dnode 级别参数，标识当前 dnode 支持的 vnodes 数目
- 范围：[0-4096]
- 默认值：0，表示 cpuCores * 2
- 实际生效值最小为2

## 3. SQL

```c
// keyValue
alter dnode 1 'supportvnodes 20';

// key value
alter dnode 1 'supportvnodes' '20';
```

## 4. 动态调整影响范围

- 可增大或减小，不会影响当前已分配的 vnodes。影响命令生效后的 vnodes 分配
- 仅影响当前状态，如需持久化操作，需额外手动修改相应 taos.cfg 文件
