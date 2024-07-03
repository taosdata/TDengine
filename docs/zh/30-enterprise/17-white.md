---
toc_max_heading_level: 4
title: "IP 白名单"
sidebar_label: "IP 白名单"
---

## 概述

从 TDengine 3.2.0.0 开始，系统管理员可以使用白名单功能来控制每个用户只能从指定的 IP 地址访问 TDengine 服务端，包括原生连接和 RESTful 以及 WebSocket 连接。如果发出连接请求的用户名和 IP 地址的组合不在系统白名单内，连接请求会被拒绝。该功能只在 TDengine Enterprise 中可用。

## 配置

1. 服务端配置：taos.cfg 中添加 enableWhiteList 的全局配置，该配置项在所有 dnode 上必须强一致，如果不一致则 dnode 会启动失败并报错。如果该选项被配置，则企业版只有在白名单内的 (user, IP) 组合能够访问数据库，社区版无影响（所有IP都可访问）。

2. 如果 enableWhiteList = true，则 (root, dnode1) 到 (root,dnoden)会被自动加入白名单 ，即集群中所有 dnode 的 IP 会被自动加入 root 的白名单。

3. 如果一个用户(假定 userA) 可能从 IP1 通过 taosAdapter （假定所在 IP 为 IP2）访问数据库，则首先 （userA, IP2） 要加入白名单，否则 taosAdapter 无法建立与 taosd 的连接。同时 (userA，IP)也要加入白名单，否则 userA 从 IP1 上发出的请求会被拒绝。

4. 系统管理员可以使用如下命令从客户端动态开关白名单功能
   ```sql
   alter all dnodes 'enableWhiteList 1' # 打开白名单功能
   alter all dnodes 'enableWhiteList 0' # 关闭白名单功能
   ```
   如果之前已经开启过白名单，之后关闭，然后再开启，之前添加进去的白名单依旧有效. 也就是说，针对用户的白名单一旦添加进去，一直有效，除非显式删除，这里的开关只是决定是否使用它。

## 权限

只有 root 用户能够修改和查询白名单。非root 用户只能查询白名单。

## 创建白名单

```sql
CREATE USER user_name PASS password [SYSINFO value] [HOST host_name1[,host_name2]]     
```

可以在创建用户时添加一些 IP 或 IP 段到该用户的白名单中。

其中：
- user_name: 是新用户名，如果该用户名已经存在，则命令会失败并报错
- host_nameX：实际 IP 或者IP范围，用子网掩码的方式 

示例：
```sql
CREATAE USER test PASS 'a' HOST "127.0.0.0/24"，"192.168.0.23"
```

## 修改白名单

```sql
ALTER USER user_name ADD HOST host_name1    
```

如果在创建用户时未添加它的白名单，可以在任何时候通过修改用户属性的方式添加白名单 。

其中：
- user_name: 是实际的存在user名，如果不存在，则报错
- host_name1: 实际 IP 或者IP范围，用子网掩码的方式

示例：
```sql
ATLER USER root ADD HOST "127.0.0.0/24"
```

## 删除白名单

```sql
ALTER USER user_name DROP HOST host_name1
```

通过修改用户属性的方式也可以删除白名单中的某个 IP 或 IP range。

其中：
- user_name: 是实际的存在user名，如果不存在，则报错.  
- host_name1: 实际 IP 或者IP范围，用子网掩码的方式表表示

示例：
```sql
alter user root drop host "127.0.0.5"
```

## 删除用户

```sql
drop user <user_name>
```

如果一个用户被删除，则系统中与该用户有关的白名单信息都会被删除

## 错误码

对白名单的操作和使用中可能会出现如下几种常见错误码。

1. TSDB_CODE_MND_USER_HOST_EXIST "Host already exist in ip white list" ， 对一个user, 添加重复IP, 如果添加了一次，第二次会报错. 
2. TSDB_CODE_MND_USER_HOST_NOT_EXIST,      "Host not exist in ip white list， 对一个user, 删除一个不在IP  white list 的IP
3. TSDB_CODE_MND_TOO_MANY_USER_HOST,       "Too many host in ip white list"， 对一个user, 添加的IP数目到了上限，上限是2048
4. TSDB_CODE_MND_USER_LOCAL_HOST_NOT_DROP,  "Host can not be dropped",  尝试删除ip white list 中的127.0.0.1 
5. TSDB_CODE_IP_NOT_IN_WHITE_LIST， "Not allowed to connect"， 不在白名单列表中用户尝试访问

## 补充说明

1. 默认会把各个 taosd 服务所在机器的 IP 地址 添加到白名单列表，且在白名单列表可以查询；这样 root 用户在初始可以在任意一个 taosd 所在服务器上访问集群进行白名单操作。
2. taosadaper 和 taosd 不在一个机器的时候，需要把taosadaper IP手动添加到taosd 白名单列表中。
3. 集群情况下，各个节点 enableWhiteList 成一样，或者全为false,或者全为true, 要不然集群无法启动。
4. 白名单变更生效时间不超过2s 。
5. 如果添加两个ip range,  192.168.1.1/16 (假设为A), 192.168.1.1/24 (假设为B), 虽然A包含了B，但并不会对A和B做合并。只有两次添加的完全相同的两个 ip range 才会被合并在一起。
6. 删除白名单时，必须严格匹配。 也就是如果添加的是192.168.1.1/24, 要删除也是192.168.1.1/24 。
7. 只有 root 用户才能够增删或修改白名单 。
8. x.x.x.x/32 和x.x.x.x 属于同一个iprange, 显示为x.x.x.x 。
9. 如果客户端拿到的 0.0.0.0/0, 说明没有开启白名单。
10. 针对单一用户名，能够添加的白名单上限是 2048 个。
