---
title: 双副本方案
sidebar_label: 双副本方案
toc_max_heading_level: 4
---

本节介绍 TDengine 双副本方案的配置与使用。

双活是 TDengine Enterprise 特有功能，在 3.3.0.0 版本中第一次发布，建议使用最新版本。


## 方案特点

1. 最小配置的服务器节点数为 2+1 个，其中两个数据节点，一个仲裁节点
  - 仲裁节点可与其他应用共用，但需与前两个数据节点在同一网段；该节点占用资源少，仅1~2核
  - 如部署3个以上数据节点，无需单独部署仲裁节点
2. 双副本为数据库建库参数，不同数据库可按需选择副本数
3. 支持单副本与双副本之间切换(前提是节点数量满足需求、各节点可用vnode数量/内存/存储空间足够)
4. 支持 TDengine 集群的完整特性，包括：读缓存、数据订阅、流计算等
5. 支持 TDengine 所有语言连接器以及连接方式
6. 不支持双副本与三副本之间的切换
7. 不支持双副本切换为双活，除非另外部署一套实例与当前实例组成双活方案

## 集群配置

双副本要求集群至少配置三个服务器节点(两个数据节点，一个仲裁节点)，基本部署与配置步骤如下：
  1. 确定服务器节点数量、主机名或域名，配置好所有节点的域名解析：DNS 或 /etc/hosts
  2. 各节点分别安装 TDengine 企业版服务端安装包，按需编辑好各节点 taos.cfg
    1. 仲裁节点 SupportVnodes 0
    2. 数据节点 SupportVnodes 按实际需求配置
  3. 启动各节点 taosd 服务 (其他服务可按需启动：taosadapter/taosx/taoskeeper/taos-explorer)
    1. 仲裁节点仅需启动 taosd 服务
  4. 登入taos CLI，将所有节点添加入集群 create dnode xxxx
  5. 创建三个 mnode create mnode on dnode nn

## 数据库创建

创建双副本数据库。DBA按需创建指定的双副本数据库

```sql
create database <dbname> replica 2 vgroups xx buffer xx ...
```

## 修改数据库副本数

创建了一个单副本数据库后，希望改为双副本时，可通过alter命令来实现。反之亦然

```sql
alter database <dbname> replica 2|1
```

## 常见问题

### 1. 创建双副本数据库或修改为双副本时，报错：DB error: Out of dnodes
- 服务器节点数不足：原因是，数据服务器节点数少于两个。
- 解决方案：增加服务器节点数量，满足最低要求。

### 2. 创建双副本数据库或 split vgroup 时，报错：DB error: Vnodes exhausted
- 服务器可用 Vnodes 不足：原因是某些服务器节点可用 Vnodes 数少于建库或 split vgroup 的需求数。
- 解决方案：调整服务器 CPU 数量、SupportVnodes 数量，满足建库要求。
