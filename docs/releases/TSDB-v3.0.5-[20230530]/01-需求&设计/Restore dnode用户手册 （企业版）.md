# Restore dnode用户手册 （企业版）

### 1. 背景

磁盘故障或数据损坏或人为错误，导致节点重启后其所参与的部分或全部 vgroup 无法加入集群并重建副本，或者以 mnode 的身份无法加入 mnode 集群

### 2. 目标

节点重新加入集群，并且恢复全部数据
并且提供细粒度的命令，让用户恢复部分数据

### 3. 相关问题

TS-2780 3.0 多副本集群下单个节点磁盘损坏后集群和数据的自动恢复功能

TS-3269 [长庆油田]mnode&qnode恢复机制讨论

TD-22365 【集群】丢失一个副本后剩余两副本能够继续工作并自动恢复出一个新的副本

TS-2777 【磁盘损坏】三节点三副本，磁盘损坏后数据无法修复    

TS-3246 长庆油田]ProDB Qnode not found                  

### 4. 新增命令

```sql
restore dnode <dnode_id>；# 恢复dnode上的mnode，所有vnode和qnode
restore mnode on dnode <dnode_id>；# 恢复dnode上的mnode
restore vnode on dnode <dnode_id> ；# 恢复dnode上的所有vnode
restore qnode on dnode <dnode_id>；# 恢复dnode上的qnode
```


### 5. 功能实现

节点恢复过程：
1.从mnode中，读取是否在dnode上存在mnode，如果存在，则生成配置信息，走创建mnode流程恢复mnode
2.从mnode中，读取在该dnode上的所有vgroup，为每个vgroup生成配置信息，为每个vgroup走创建vnode流程恢复vnode
3.从mnode中，读取是否在dnode上存在qnode，如果存在，则生成配置信息，走创建qnode流程恢复qnode

### 6. 功能限制

恢复的前置条件是从空白状态恢复。也就是说：
Restore dnode 是将一个节点从完全空白（也就是mnode、vnode、qnode三个目录都不存在）恢复
Restore mnode是将一个节点，在mnode目录不存在的状态下，恢复
Restore vgroup是将一个节点，在vnodeBe目录不存在的状态下，恢复
Restore qnode是将一个节点，在qnode目录不存在的状态下，恢复
