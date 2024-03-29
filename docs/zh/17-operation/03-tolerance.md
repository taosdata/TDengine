---
title: 容错和灾备
sidebar_label: 容错和灾备
description: TDengine 的容错和灾备功能
---

## 容错

TDengine 支持 **WAL**（Write Ahead Log）机制，实现数据的容错能力，保证数据的高可用。

TDengine 接收到应用的请求数据包时，先将请求的原始数据包写入数据库日志文件，等数据成功写入数据库数据文件后，再删除相应的 WAL。这样保证了 TDengine 能够在断电等因素导致的服务重启时从数据库日志文件中恢复数据，避免数据的丢失。

涉及的系统配置参数有两个：

- wal_level：WAL 级别，1：写 WAL，但不执行 fsync。2：写 WAL，而且执行 fsync。默认值为 1。
- wal_fsync_period：当 wal_level 设置为 2 时，执行 fsync 的周期。设置为 0，表示每次写入，立即执行 fsync。

如果要 100%的保证数据不丢失，需要将 wal_level 设置为 2，wal_fsync_period 设置为 0。这时写入速度将会下降。但如果应用侧启动的写数据的线程数达到一定的数量(超过 50)，那么写入数据的性能也会很不错，只会比 wal_fsync_period 设置为 3000 毫秒下降 30%左右。

## 灾备

TDengine 灾备是通过在异地的两个数据中心中设置两个 TDengine 集群并利用 taosX 的数据复制能力来实现的。假定两个集群为集群 A 和集群 B，其中集群 A 为源集群，承担写入请求并提供查询服务。则在集群 A 所在数据中心中可以配置 taosX 利用 TDengine 提供的数据订阅能力，实时消费集群 A 中新写入的数据，并同步到集群 B。如果发生了灾难导致集群 A 所在数据中心不可用，则可以启用集群 B 作为数据写入和查询的主节点，并在集群 B 所处数据中心中配置 taosX 将数据复制到已经恢复的集群 A 或者新建的集群 C。

利用 taosX 的数据复制能力也可以构造出更复杂的灾备方案。

taosX 只在 TDengine 企业版中提供，关于其具体细节，请联系 business@taosdata.com
