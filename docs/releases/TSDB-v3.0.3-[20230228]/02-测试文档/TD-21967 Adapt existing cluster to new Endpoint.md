# TD-21967 Adapt existing cluster to new Endpoint

## 一、测试概述

任务来源：
TD-21967

用户手册：[Adapt existing cluster to new Endpoint](https://taosdata.feishu.cn/wiki/wikcnV1lzkg5gwqIINkzVsv89Gf) 

## 二、测试结论

功能测试已完成。
剩余部分为stream优化项，以及3副本stream场景。

TD-22313

## 三、测试方案

### 1.测试环境

（1）单机

| 软件项 | IP |
| --- | --- |
| TDengine | 192.168.1.42：6030 |
| taostest测试框架 | 192.168.1.40 |

（2）3节点集群

| 软件项 | IP |
| --- | --- |
| 192.168.1.42:6030 |
| 192.168.1.42:6130 |
| 192.168.1.42:6230 |
| taostest测试框架 | 192.168.1.40 |

（5）5节点集群

| 软件项 | IP |
| --- | --- |
| 192.168.1.42:6030 |
| 192.168.1.42:6130 |
| 192.168.1.42:6230 |
| 192.168.1.42:6330 |
| 192.168.1.42:6430 |
| taostest测试框架 | 192.168.1.40 |

### 2.测试用例

设计思路：主要覆盖ep修改前后，重启启动taos访问、写入、查询、流计算、数据订阅是否正常

#### （1）单机场景

前置条件
FirstEP：u1-42：6030
fqdn：u1-42
serverport：6030

| 编号 | 测试内容 | 预期结果 | 是否符合预期 |
| --- | --- | --- | --- |
| 1 | 1.在/var/lib/taos/dnode目录下新增ep.json文件： 设置 "id": 1, "fqdn": "localhost", "port": 7100, "new_fqdn": "127.0.0.1", "new_port":7030 2.重启taosd服务 | （1）ep.json文件更名为ep.json.bak （2）mnode/sync/raft_config.json 中endpoint值更新为7030 （3）show dnodes命令查看port值变为7030 （4）show mnodes命令查看port值变为7030 （5）创建新的db、stable、table成功 （6）dnode.json文件中port更新为7030 | （1）符合预期 （2）符合预期 （3）符合预期 （4）符合预期 （5）符合预期 （6）符合预期 |
| 2 | 1.启动taosd，先创建一个db 2.通过ep.json文件修改endpoint 3.重启taosd 4.访问之前创建的db | 访问成功，可在db中建库建表、插入数据 | 符合预期 |
| 3 | 1.启动taosd，创建db，在db中写入部分数据，创建订阅该db中数据的topic 2.通过ep.json修改endpoint 3.重启taosd 4.通过topic获取数据 | topic可正常订阅 | 符合预期 |
| 4 | 1.启动taosd，先创建一个db 2.通过ep.json文件修改endport 3.重启taosd | vnode/vnode<Id>/vnode.json 和 vnode/vnode<Id>/sync/raft_config.json文件中的endpoint信息更新正确 | vnode/vnode<Id>/vnode.json 和 vnode/vnode<Id>/sync/raft_config.json文件中的endpoint信息更新正确 |
| 5 | 1.创建一个流计算，同时通过taosBenchmark写入 2.修改endpoint 3.重启taosd 4.通过taosBenchmark继续写入 5.查询流计算结果 | 流计算结果持续更新 |
| 6 | 1.创建一个snode 2.创建一个流计算，同时通过taosBenchmark写入 3.修改endpoint 4.重启taosd 5.通过taosBenchmark继续写入 6.查询流计算结果 | 流计算结果持续更新 |
| 7 | 1.设置ep.json文件中的内容： 将new_fqdn设置为与fqdn值不一致 2.重启taosd | 该节点启动失败 | 符合预期 |
| 8 | 1.设置ep.json文件中的内容： 将new_port设置为与其他节点port值一致 2.重启taosd | 该节点启动失败 | 符合预期 |

#### （2）三节点三副本场景（1mnode）

前置条件
dnode1（mnode）
FirstEP：u1-42：6030
fqdn：u1-42
serverport：6030
dnode2
FirstEP：u1-42：6030
fqdn：u1-42
serverport：6130
dnode3
FirstEP：u1-42：6030
fqdn：u1-42
serverport：6230


| 编号 | 测试内容 | 预期结果 | 是否符合预期 |
| --- | --- | --- | --- |
| 1 | 1.修改dnode1（mnode所在节点）的endpoint为7030 2.重启taosd服务 | （1）ep.json文件更名为ep.json.bak （2）mnode/sync/raft_config.json 中endpoint值更新为7030 （3）show dnodes命令查看port值变为7030 （4）show mnodes命令查看port值变为7030 （5）创建新的db、stable、table成功 | TD-22423 |
| 2 | 1.启动taosd，先创建一个db 2.通过ep.json文件修改endpoint 3.重启taosd 4.访问之前创建的db | 访问成功，可在db中建库建表、插入数据 | 符合预期 |
| 3 | 1.启动taosd，创建db，在db中写入部分数据，创建订阅该db中数据的topic 2.通过ep.json修改mnode所在节点的endpoint 3.重启taosd 4.通过topic进行消费数据 | topic可正常订阅 | 符合预期 |
| 4 | 1.启动taosd，创建db，在db中写入部分数据，创建订阅该db中数据的topic 2.通过ep.json修改非mnode所在节点的endpoint 3.重启taosd 4.通过topic进行消费数据 | topic可正常订阅 | 符合预期 |
| 4 | 1.启动taosd，先创建一个db 2.通过ep.json文件修改vnode所在节点的endport 3.重启taosd | 查看3个dnode存储路径下的vnode/vnode<Id>/vnode.json 和 vnode/vnode<Id>/sync/raft_config.json文件中的endpoint信息更新正确 | 符合预期 |
| 5 | 1.创建一个流计算，同时通过taosBenchmark写入 2.修改endpoint 3.重启taosd 4.通过taosBenchmark继续写入 5.查询流计算结果 | 流计算结果持续更新 |  |
| 6 | 1.在mnode所在的节点上（dnode1）创建一个snode 2.创建一个流计算，同时通过taosBenchmark写入 3.修改snode所在节点endpoint 4.重启taosd 5.通过taosBenchmark继续写入 6.查询流计算结果 | 流计算结果持续更新 |  |
| 7 | 1.在mnode所在的节点上创建一个snode 2.创建一个流计算，同时通过taosBenchmark写入 3.修改非snode所在节点endpoint 4.重启taosd 5.通过taosBenchmark继续写入 6.查询流计算结果 | 流计算结果持续更新 |  |
| 7 | 1.在非mnode所在的节点上创建一个snode 2.创建一个流计算，同时通过taosBenchmark写入 3.修改snode所在节点endpoint 4.重启taosd 5.通过taosBenchmark继续写入 6.查询流计算结果 | 流计算结果持续更新 |  |
| 8 | 1.在非mnode所在的节点上创建一个snode 2.创建一个流计算，同时通过taosBenchmark写入 3.修改非snode所在节点endpoint 4.重启taosd 5.通过taosBenchmark继续写入 6.查询流计算结果 | 流计算结果持续更新 |  |
| 9 | 1.设置ep.json文件中的内容： 将new_fqdn设置为与fqdn值不一致 2.重启taosd | 该节点启动失败 | 符合 |
| 10 | 1.设置ep.json文件中的内容： 将new_port设置为与其他节点port值一致 2.重启taosd | 该节点启动失败 | 符合 |

#### （3）五节点三副本场景（3mnode）

前置条件
dnode1（mnode）
FirstEP：u1-42：6030
fqdn：u1-42
serverport：6030
dnode2（mnode）
FirstEP：u1-42：6030
fqdn：u1-42
serverport：6050
dnode3（mnode）
FirstEP：u1-42：6030
fqdn：u1-42
serverport：6070
dnode4
FirstEP：u1-42：6030
fqdn：u1-42
serverport：6130
dnode5
FirstEP：u1-42：6030
fqdn：u1-42
serverport：6150

| 编号 | 测试内容 | 预期结果 | 是否符合预期 |
| --- | --- | --- | --- |
| 1 | 1.修改dnode1（mnode所在节点）的endpoint为7030 2.重启taosd服务 | （1）ep.json文件更名为ep.json.bak （2）mnode/sync/raft_config.json 中endpoint值更新为7030 （3）show dnodes命令查看port值变为7030 （4）show mnodes命令查看port值变为7030 （5）创建新的db、stable、table成功 | TD-22567 |
| 2 | 1.启动taosd，先创建一个db 2.通过ep.json文件修改endpoint 3.重启taosd 4.访问之前创建的db | 访问成功，可在db中建库建表、插入数据 | 符合 |
| 3 | 1.启动taosd，创建db，在db中写入部分数据，创建订阅该db中数据的topic 2.通过ep.json修改mnode所在节点的endpoint 3.重启taosd 4.通过topic进行消费数据 | topic可正常订阅 | 符合 |
| 4 | 1.启动taosd，创建db，在db中写入部分数据，创建订阅该db中数据的topic 2.通过ep.json修改非mnode所在节点的endpoint 3.重启taosd 4.通过topic进行消费数据 | topic可正常订阅 | 符合 |
| 4 | 1.启动taosd，先创建一个db 2.通过ep.json文件修改vnode所在节点的endport 3.重启taosd | 查看5个dnode存储路径下的vnode/vnode<Id>/vnode.json 和 vnode/vnode<Id>/sync/raft_config.json文件中的endpoint信息更新正确 | 符合 |
| 5 | 1.创建一个流计算，同时通过taosBenchmark写入 2.修改endpoint 3.重启taosd 4.通过taosBenchmark继续写入 5.查询流计算结果 | 流计算结果持续更新 |  |
| 6 | 1.在mnode所在的节点（dnode1）上创建一个snode 2.创建一个流计算，同时通过taosBenchmark写入 3.修改snode所在节点endpoint 4.重启taosd 5.通过taosBenchmark继续写入 6.查询流计算结果 | 流计算结果持续更新 |  |
| 7 | 1.在mnode所在的节点（dnode1）上创建一个snode 2.创建一个流计算，同时通过taosBenchmark写入 3.修改非snode所在节点（dnode4）endpoint 4.重启taosd 5.通过taosBenchmark继续写入 6.查询流计算结果 | 流计算结果持续更新 |  |
| 7 | 1.在非mnode所在的节点（dnode4）上创建一个snode 2.创建一个流计算，同时通过taosBenchmark写入 3.修改snode所在节点endpoint 4.重启taosd 5.通过taosBenchmark继续写入 6.查询流计算结果 | 流计算结果持续更新 |  |
| 8 | 1.在非mnode所在的节点（dnode4）上创建一个snode 2.创建一个流计算，同时通过taosBenchmark写入 3.修改非snode所在节点endpoint 4.重启taosd 5.通过taosBenchmark继续写入 6.查询流计算结果 | 流计算结果持续更新 |  |
| 9 | 1.设置ep.json文件中的内容： 将new_fqdn设置为与fqdn值不一致 2.重启taosd | 该节点启动失败 | 符合预期 |
| 10 | 1.设置ep.json文件中的内容： 将new_port设置为与其他节点port值一致 2.重启taosd | 该节点启动失败 | 符合预期 |
