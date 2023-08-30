---
title: 边云协同
sidebar_label: 边云协同
---

## 简介

利用 taosX 这个零代码数据迁移平台以及 TDengine server 的数据订阅特性，可以轻易地实现边云协同功能。在云端（或使用 TDengine server 的集中存储端）部署 taosX，在使用 TDengine server 的边端创建 topic，可以根据需要有选择地将关键数据从边端实时同步到云端。边云协同只支持边端和云端都是 TDengine 3.0 的版本。本节讲述如何使用 taosX 的命令行进行边云协同，您也可以使用 taos-explorer 的可视化界面进行边云协同，具体请参考【可视化管理】(./explorer)。

## 命令行参数

对命令行参数的详细说明请参考【命令行参数】(../cmd)

### 详细参数

针对边云协同的参数说明如下表

参数名称  | 说明                                                             | 默认值                     |
|-----------|------------------------------------------------------------------|----------------------------|
| group.id  | 订阅使用的分组ID                                                 | 若为空则使用 hash 生成一个 |
| client.id | 订阅使用的客户端ID                                               | taosx                      |
| timeout   | 监听数据的超时时间，当设置为 never 表示 taosx 不会停止持续监听 | 500ms                      |
| offset    | 从指定的 offset 开始订阅，格式为 `<vgroup_id>:<offset>`，若有多个 vgroup 则用半角逗号隔开 | 若为空则从 0 开始订阅  |
| token     | 目标源参数。 认证使用参数。                              | 无                                     |

### DSN

**数据源(-f 参数的 DSN)的 object 必须配置为 数据库中已存在的可订阅的 topic**

```shell
taosx run \
  -f 'tmq://root:taosdata@localhost:6030/tp1 ?group.id=taosx1&client.id=taosx&timeout=never&offset=2:10' \
  -t 'taos://root:taosdata@another.com:6030/db2'
```

在以上示例中， `tp1` 是已经在数据库中创建的 topic，该 topic 可以根据边云协同的需要来创建，可以根据需要在 topic 中指定必要的过滤条件，这样只有符合条件的数据才会被实时同步到云端。
