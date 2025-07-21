---
sidebar_label: 节点管理
title: 节点管理
description: 管理集群节点的 SQL 命令的详细解析
---

组成 TDengine 集群的物理实体是 dnode (data node 的缩写)，它是一个运行在操作系统之上的进程。在 dnode 中可以建立负责时序数据存储的 vnode (virtual node)，在多节点集群环境下当某个数据库的 replica 为 3 时，该数据库中的每个 vgroup 由 3 个 vnode 组成；当数据库的 replica 为 1 时，该数据库中的每个 vgroup 由 1 个 vnode 组成。如果要想配置某个数据库为多副本，则集群中的 dnode 数量至少为 3。在 dnode 还可以创建 mnode (management node)，单个集群中最多可以创建三个 mnode。在 TDengine 3.0.0.0 中为了支持存算分离，引入了一种新的逻辑节点 qnode (query node)，qnode 和 vnode 既可以共存在一个 dnode 中，也可以完全分离在不同的 dnode 上。

## 创建数据节点

```sql
CREATE DNODE {dnode_endpoint | dnode_host_name PORT port_val}
```

其中 `dnode_endpoint` 是形成 `hostname:port`的格式。也可以分开指定 hostname 和 port。

实际操作中推荐先创建 dnode，再启动相应的 dnode 进程，这样该 dnode 就可以立即根据其配置文件中的 firstEP 加入集群。每个 dnode 在加入成功后都会被分配一个 ID。

## 查看数据节点

```sql
SHOW DNODES;
```

可以列出集群中所有的数据节点，所列出的字段有 dnode 的 ID, endpoint, status。

## 删除数据节点

```sql
DROP DNODE dnode_id [force] [unsafe]
```

注意删除 dnode 不等于停止相应的进程。实际中推荐先将一个 dnode 删除之后再停止其所对应的进程。

只有在线节点可以被删除。如果要强制删除离线节点，需要执行强制删除操作，即指定 force 选项。

当节点上存在单副本，并且节点处于离线，如果要强制删除该节点，需要执行非安全删除，即制定 unsafe，并且数据不可再恢复。

## 修改数据节点配置

```sql
ALTER DNODE dnode_id dnode_option

ALTER ALL DNODES dnode_option
```

对于支持动态修改的配置参数，您可以使用 ALTER DNODE 或 ALTER ALL DNODES 语法修改 dnode 中配置参数的值，自 v3.3.4.0 后，修改的配置参数将自动持久化，即便数据库服务重启后仍然生效。

对于一个配置参数是否支持动态修改，请您参考 [taosd 参考手册](https://docs.taosdata.com/reference/components/taosd)

value 是参数的值，需要是字符格式。如修改 dnode 1 的日志输出级别为 debug。

```sql
ALTER DNODE 1 'debugFlag' '143';
```

### 补充说明：
配置参数在 dnode 中被分为全局配置参数与局部配置参数，您可以查看 SHOW VARIABLES 或 SHOW DNODE dnode_id VARIABLES 中的 category 字段来确认配置参数属于全局配置参数还是局部配置参数。
1. 局部配置参数：您可以使用 ALTER DNODE 或 ALTER ALL DNODES 来更新某一个 dnode 或全部 dnodes 的局部配置参数。
2. 全局配置参数：全局配置参数要求各个 dnode 保持一致，所以您只可以使用 ALTER ALL DNODES 来更新全部 dnodes 的全局配置参数。

配置参数是否可以动态修改，有以下三种情况：
1. 支持动态修改，立即生效 
2. 支持动态修改，重启生效
3. 不支持动态修改

对于重启后生效的配置参数，您可以通过 `SHOW VARIABLES` 或 `SHOW DNODE dnode_id VARIABLES` 看到修改后的值，但是需要重启数据库服务才使其生效。

## 添加管理节点

```sql
CREATE MNODE ON DNODE dnode_id
```

系统启动默认在 firstEP 节点上创建一个 MNODE，用户可以使用此语句创建更多的 MNODE 来提高系统可用性。一个集群最多存在三个 MNODE，一个 DNODE 上只能创建一个 MNODE。

## 查看管理节点

```sql
SHOW MNODES;
```

列出集群中所有的管理节点，包括其 ID，所在 DNODE 以及状态。

## 删除管理节点

```sql
DROP MNODE ON DNODE dnode_id;
```

删除 dnode_id 所指定的 DNODE 上的 MNODE。

## 创建查询节点

```sql
CREATE QNODE ON DNODE dnode_id;
```

系统启动默认没有 QNODE，用户可以创建 QNODE 来实现计算和存储的分离。一个 dnode 上只能创建一个 QNODE。一个 dnode 的 `supportVnodes` 参数如果不为 0，同时又在其上创建上 QNODE，则在该 dnode 中既有负责存储管理的 vnode 又有负责查询计算的 qnode，如果还在该 dnode 上创建了 mnode，则一个 dnode 上最多三种逻辑节点都可以存在。但通过配置也可以使其彻底分离。将一个 dnode 的`supportVnodes`配置为 0，可以选择在其上创建 mnode 或者 qnode 中的一种，这样可以实现三种逻辑节点在物理上的彻底分离。

## 查看查询节点

```sql
SHOW QNODES;
```

列出集群中所有查询节点，包括 ID，及所在 dnode

## 删除查询节点

```sql
DROP QNODE ON DNODE dnode_id;
```

删除 ID 为 dnode_id 的 dnode 上的 qnode，但并不会影响该 dnode 的状态。

## 创建订阅节点

```sql
CREATE BNODE ON DNODE dnode_id [PROTOCOL protocol];
```

系统启动默认没有 BNODE，用户可以创建 BNODE 来启动订阅服务。一个 dnode 上只能创建一个 BNODE。protocol 为可选配置项，不提供时，默认为“mqtt”，后续会扩展其它协议。bnode 创建成功后，dnode 会启动子进程 taosmqtt，对外提供订阅服务。

## 查看订阅节点

```sql
SHOW BNODES;
```

列出集群中所有订阅节点，包括 ID，protocol，创建时间，及所在 dnode。

## 删除订阅节点

```sql
DROP BNODE ON DNODE dnode_id;
```

删除 ID 为 dnode_id 的 dnode 上的 bnode，此 dnode 上的 taosmqtt 子进程会退出，停止订阅服务。

## 查询集群状态

```sql
SHOW CLUSTER ALIVE;
```

查询当前集群的状态是否可用，返回值
- 0：不可用
- 1：完全可用
- 2：部分可用（集群中部分节点下线，但其它节点仍可以正常使用） 

## 修改客户端配置

如果将客户端也看作广义的集群的一部分，可以通过如下命令动态修改客户端配置参数。

```sql
ALTER LOCAL local_option
```

您可以使用以上语法更该客户端的配置参数，并且不需要重启客户端，修改后立即生效。

对于一个配置参数是否支持动态修改，请您参考以下页面：[taosc 参考手册](https://docs.taosdata.com/reference/components/taosc/)


## 查看客户端配置

```sql
SHOW LOCAL VARIABLES;
```
