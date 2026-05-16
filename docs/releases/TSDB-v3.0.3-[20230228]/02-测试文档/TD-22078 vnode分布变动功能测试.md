# TD-22078 vnode分布变动功能测试

## 一、测试概述

任务来源：
TD-22078


## 二、测试结论

优化建议：
TD-22133

三副本部分，等待流计算支持三副本的功能开发完成后，再继续进行测试，本阶段测试工作完成。
遗留测试项：
TD-22313

## 三、测试发现的问题

TD-22260


TD-22303


TD-22305

## 四、测试方案及测试用例

### 1.测试环境

采用1台机器搭建多节点集群

| 软件项 | IP |
| --- | --- |
| 192.168.1.41:6030 |
| 192.168.1.41:6050 |
| 192.168.1.41:6070 |
| …… |
| taostest测试框架 | 192.168.1.40 |

### 2.测试用例

#### REDISTRIBUTE

##### （1）三节点单副本场景

创建DB时，配置vgroups=1，在创建后分配了1个vnode：
sql：create database db vgroups 1

| IP | dnode_id | vgroup_id |
| --- | --- | --- |
| 192.168.1.41:6030 | 1 | 1 |
| 192.168.1.41:6050 | 2 | \ |
| 192.168.1.41:6070 | 3 | \ |

###### 正常测试

| 编号 | 测试内容 | 预期结果 | 是否符合预期 |
| --- | --- | --- | --- |
| 1 | 执行` vgroup 1 dnode 2， 将vnode1 迁移到dnode2下 | 通过show vgroups查看vgroup1所在的dnode变为dnode2； 查询迁移后的数据正确 | 符合 |
| 2 | 在用例1基础上，执行redistribute vgroup 1 dnode 3， 将vnode1 迁移到dnode3下 | 通过show vgroups查看vgroup1所在的dnode2变为dnode3； 查询迁移后的数据正确 | 符合 |
| 3 | 在用例2基础上，执行redistribute vgroup 1 dnode 1， 将vnode1 迁移到dnode1下 | 通过show vgroups查看vgroup1所在的dnode3变为dnode1； 查询迁移后的数据正确 | 符合 |
| 4 | 1.执行redistribute 操作 2.在步骤1的过程中，启动新客户端，执行事务操作（create database等） | 新的事务操作不成功，返回错误 | 符合 |
| 5 | 1.向vnode所在db中写入部分数据 1.执行redistribute 操作 2.在步骤1的过程中，启动新客户端，执行查询语句（select count(*)查询该db中的数据） | 查询获得结果/报错，未出现卡死或者crash | 符合 |
| 6 | 1.通过taosBenchmark向vnode所在db持续写入数据；另通过taosBenchmark对该db中的数据持续进行select * from stb partition by column操作 2.另起终端访问，执行redistribute vgroup 1 dnode 2， 将vnode1 迁移到dnode2下 | 写入流程终止或持续进行，查询流程持续进行或报错；均不出现卡死或者crash现象 | TD-22260 |
| 7 | 1.创建一个查询stable的流计算，通过taosBenchmark写入数据 2.另起终端访问，执行redistribute vgroup 1 dnode 2， 3.流程结束后继续通过taosBenchmark写入数据 3.查看流计算的结果 | 流计算结果持续更新 |
| 8 | 1.创建一个查询stable的流计算，创建db，包含两个vgroup，每个vgroup下都存在部分子表和数据，通过taosBenchmark写入数据。 2.另起终端访问，执行redistribute vgroup 1 dnode 2， 3.流程结束后继续通过taosBenchmark写入数据 3.查看流计算的结果 | 流计算结果持续更新 |
| 9 | 1.创建db，包含两个vgroup，每个vgroup下都存在部分子表和数据，创建一个查询子表的流计算，通过taosBenchmark写入数据。 2.另起终端访问，执行redistribute vgroup 1 dnode 2（该vgroup上有被流计算中被查询的子表数据）， 3.流程结束后继续通过taosBenchmark写入数据 3.查看流计算的结果 | 流计算结果持续更新 |
|  | 1.创建db，包含两个vgroup，每个vgroup下都存在部分子表和数据，创建一个查询子表的流计算，通过taosBenchmark写入数据。 2.另起终端访问，执行redistribute vgroup 1 dnode 2（该vgroup上没有被流计算中被查询的子表数据）， 3.流程结束后继续通过taosBenchmark写入数据 3.查看流计算的结果 | 流计算结果持续更新 | 符合预期 |

###### 异常测试

| 编号 | 测试内容 | 预期结果 | 是否符合预期 |
| --- | --- | --- | --- |
| 1 | 执行sql：redistribute vgroup 1 dnode 2 dnode 3 | 报错，replica数量错误 | 符合 |
| 2 | 执行sql：redistribute vgroup 1 dnode 1 dnode 2 dnode 3 | 报错，replica数量错误 | 符合 |
| 3 | 执行sql：redistribute vgroup 1 dnode 4 | 报错，提示dnode不存在 | 符合 |
| 4 | 1.执行redistribute vgroup 1 dnode 2，将vnode1 迁移到dnode2下 2.迁移过程中，将dnode2离线，再上线 | 迁移过程正常完成，迁移后数据内容和迁移前一致 | 符合 |
| 5 | 1.执行redistribute vgroup 1 dnode 2，将vnode1 迁移到dnode2下 2.迁移过程中，将dnode1离线，再上线 | 迁移过程正常完成，迁移后数据内容和迁移前一致 | 符合 |

##### （2）六节点三副本场景

创建DB时，配置vgroups=1，replica=3：
sql：create database db vgroups 1 replica 3

| IP | dnode_id | vgroup_id |
| --- | --- | --- |
| 192.168.1.41:6030 | 1 | 1 |
| 192.168.1.41:6050 | 2 | 1 |
| 192.168.1.41:6070 | 3 | 1 |
| 192.168.1.41:6130 | 4 | \ |
| 192.168.1.41:6150 | 5 | \ |
| 192.168.1.41:6170 | 6 | \ |

###### 正常测试

| 编号 | 测试内容 | 预期结果 | 是否符合预期 |
| --- | --- | --- | --- |
| 1 | redistribute vgroup 1 dnode 4 dnode 5 dnode 6 将vgroup1的三个vnode迁移到dnode 4,5,6下 | 通过show vgroups查看，vgroup1的三个vnode分布在dnode 4,5,6下 查询迁移后的数据正确 | 符合预期 |
| 2 | 在用例1基础上，执行 redistribute vgroup 1 dnode 1 dnode 2 dnode 3 将vgroup1的三个vnode迁移回dnode 1,2,3下 | 通过show vgroups查看，vgroup1的三个vnode分布在dnode 1,2,3下 查询迁移后的数据正确 | 符合预期 |
| 3 | redistribute vgroup 1 dnode 1 dnode 4 dnode 5 将vgroup1的三个vnode迁移到dnode 1,4,5下 | 通过show vgroups查看，vgroup1的三个vnode分布在dnode 1,4,5下 查询迁移后的数据正确 | 符合预期 |
| 4 | redistribute vgroup 1 dnode 2 dnode 3 dnode 4 将vgroup1的三个vnode迁移到dnode 2,3,4下 | 通过show vgroups查看，vgroup1的三个vnode分布在dnode 2,3,4下 查询迁移后的数据正确 | 符合预期 |
| 5 | 1.通过taosBenchmark向vnode所在db持续写入数据；另通过taosBenchmark对该db中的数据持续进行select * from stb partition by column操作 2.另起终端访问，执行redistribute vgroup 1 dnode 4 dnode 5 dnode 6 | 写入流程终止或持续进行，查询流程持续进行或报错；均不出现卡死或者crash现象 | TD-22260 |
| 6 | 1.创建一个关于db的topic 2.另起终端访问，执行redistribute vgroup 1 dnode 4 dnode 5 dnode 6 3.通过app对已经创建的topic进行消费 | 可正常消费 | 符合预期 |
| 7 | 1.创建一个流计算，通过taosBenchmark写入数据 2.另起终端访问，执行redistribute vgroup 1 dnode 4 dnode 5 dnode 6 3.通过taosBenchmark继续写入数据 3.查看流计算的结果 | 流计算结果正确 |  |

###### 异常测试

| 编号 | 测试内容 | 预期结果 | 是否符合预期 |
| --- | --- | --- | --- |
| 1 | redistribute vgroup 1 dnode 4 | 报错，提示replica数量错误 | 符合预期 |
| 2 | redistribute vgroup 1 dnode 4 dnode 5 | 报错，提示replica数量错误 | 符合预期 |
| 3 | redistribute vgroup 1 dnode 7 dnode 8 dnode 9 | 报错，提示replica数量错误 | 符合预期 |
| 4 | 1.redistribute vgroup 1 dnode 4 dnode 5 dnode 6 将vgroup1的三个vnode迁移到dnode 4,5,6下 2.迁移过程中，将dnode4 下线再上线 | 通过show vgroups查看，vgroup1的三个vnode分布在4,5,6下 迁移后的数据正确 | 符合预期 |
| 5 | 1.redistribute vgroup 1 dnode 4 dnode 5 dnode 6 将vgroup1的三个vnode迁移到dnode 4,5,6下 2.迁移过程中，将dnode4 、dnode5下线再上线 | 通过show vgroups查看，vgroup1的三个vnode分布在4,5,6下 迁移后的数据正确 | 符合预期 |
| 6 | 1.redistribute vgroup 1 dnode 4 dnode 5 dnode 6 将vgroup1的三个vnode迁移到dnode 4,5,6下 2.迁移过程中，将dnode4 、dnode5、dnode6下线再上线 | 通过show vgroups查看，vgroup1的三个vnode分布在4,5,6下 迁移后的数据正确 | 符合预期 |
| 7 | 1.redistribute vgroup 1 dnode 4 dnode 5 dnode 6 将vgroup1的三个vnode迁移到dnode 4,5,6下 2.迁移过程中，将dnode1下线再上线 | 通过show vgroups查看，vgroup1的三个vnode分布在4,5,6下 迁移后的数据正确 | 符合预期 |
| 8 | 1.redistribute vgroup 1 dnode 4 dnode 5 dnode 6 将vgroup1的三个vnode迁移到dnode 4,5,6下 2.迁移过程中，将dnode1、dnode2下线再上线 | 通过show vgroups查看，vgroup1的三个vnode分布在4,5,6下 迁移后的数据正确 | 符合预期 |
| 9 | 1.redistribute vgroup 1 dnode 4 dnode 5 dnode 6 将vgroup1的三个vnode迁移到dnode 4,5,6下 2.迁移过程中，将dnode1、dnode2、dnode3下线再上线 | 通过show vgroups查看，vgroup1的三个vnode分布在4,5,6下 迁移后的数据正确 | 符合预期 |

#### BALANCE VGROUP

##### （1）多节点三副本

创建DB时，配置vgroups=6，replica=3：
sql：create database db vgroups 6 replica 3
通过redistribute将6个vgroup集中分布在dnode 1,2,3三个节点上，如下：

| IP | dnode_id | vgroup_id |
| --- | --- | --- |
| 192.168.1.41:6030 | 1 | 1、2、3、4、5、6 |
| 192.168.1.41:6050 | 2 | 1、2、3、4、5、6 |
| 192.168.1.41:6070 | 3 | 1、2、3、4、5、6 |
| 192.168.1.41:6130 | 4 | \ |
| 192.168.1.41:6150 | 5 | \ |
| 192.168.1.41:6170 | 6 | \ |

###### 正常测试

| 编号 | 测试内容 | 预期结果 | 是否符合预期 |
| --- | --- | --- | --- |
| 1 | 执行balance vgroup命令。 | 通过show dnodes查看，每个dnode下的vnode均为3； | 符合预期 |
| 2 | 在用例1的基础上，向集群中增加一个新的dnode 7 IP：192.168.1.41:6230 执行balance vgroup命令 | 通过show dnodes查看，有部分vnode被balance到新增的dnode 7下，7个dnode下的vnode总数正确且分布平均。 | 符合预期 |
| 3 | 1.通过taosBenchmark向vnode所在db持续写入数据；另通过taosBenchmark对该db中的数据持续进行select * from stb partition by column操作 2.另起终端访问，执行balance vgroups操作 | 写入流程终止或持续进行，查询流程持续进行或报错；均不出现卡死或者crash现象 | TD-22303 TD-22305 |
| 4 | 1.创建一个关于db的topic，并进行一次消费 2.另起终端访问，执行balance vgroups 3.再次消费该topic | 可正常消费 | 符合预期 |
| 5 | 1.创建一个流计算，通过taosBenchmark写入数据 2.另起终端访问，执行balance vgroups 3.流程结束后，继续通过taosBenchmark写入数据 4.查看流计算的结果 | 流计算结果持续更新 |  |

###### 异常测试

| 编号 | 测试内容 | 预期结果 | 是否符合预期 |
| --- | --- | --- | --- |
| 1 | 1.执行balance vgroup命令 2.vnode迁移过程中，将dnode4下线再上线 | vnode迁移正常完成。 通过show dnodes查看，每个dnode下的vnode均为3； 迁移后数据正确。 | 符合预期 |
| 2 | 1.执行balance vgroup命令 2.vnode迁移过程中，将dnode4、dnode5下线再上线 | vnode迁移正常完成。 通过show dnodes查看，每个dnode下的vnode均为3； 迁移后数据正确。 | 符合预期 |
| 3 | 1.执行balance vgroup命令 2.vnode迁移过程中，将dnode4、dnode5、dnode6下线再上线 | vnode迁移正常完成。 通过show dnodes查看，每个dnode下的vnode均为3； 迁移后数据正确。 | 符合预期 |
| 4 | 1.执行balance vgroup命令 2.vnode迁移过程中，将dnode1下线再上线 | vnode迁移正常完成。 通过show dnodes查看，每个dnode下的vnode均为3； 迁移后数据正确。 | 符合预期 |
| 5 | 1.执行balance vgroup命令 2.vnode迁移过程中，将dnode1、dnode2下线再上线 | vnode迁移正常完成。 通过show dnodes查看，每个dnode下的vnode均为3； 迁移后数据正确。 | 符合预期 |
| 6 | 1.执行balance vgroup命令 2.vnode迁移过程中，将dnode1、dnode2、dnode3下线再上线 | vnode迁移正常完成。 通过show dnodes查看，每个dnode下的vnode均为3； 迁移后数据正确。 | 符合预期 |

##### （2）三节点单副本

创建3个DB，分别配置vgroups=1，replica=1：
sql：create database db1 vgroups 1 replica 1
create database db2 vgroups 1 replica 1
create database db3 vgroups 1 replica 1
通过redistribute命令将三个vgroup都分布在dnode1下：

| IP | dnode_id | vgroup_id |
| --- | --- | --- |
| 192.168.1.41:6030（部署mnode） | 1 | 1,2,3 |
| 192.168.1.41:6050 | 2 | \ |
| 192.168.1.41:6070 | 3 | \ |

###### 正常测试

| 编号 | 测试内容 | 预期结果 | 是否符合预期 |
| --- | --- | --- | --- |
| 1 | 执行balance vgroup命令。 | 通过show dnodes查看，vnode会优先平均分布在非mnode所在节点。 | 符合预期 |
| 2 | 1.创建一个关于db的topic 2.另起终端访问，执行balance vgroups 3.通过app对已经创建的topic进行消费 | 可正常消费 | 符合预期 |
| 3 | 1.创建一个流计算，通过taosBenchmark写入数据 2.另起终端访问，执行balance vgroups 3.流程结束后继续通过taosBenchmark写入数据 3.查看流计算的结果 | 流计算结果持续更新 | TD-22313 |

###### 异常测试

| 编号 | 测试内容 | 预期结果 | 是否符合预期 |
| --- | --- | --- | --- |
| 1 | 1.执行balance vgroup命令 2.vnode迁移过程中，将dnode2下线再上线 | vnode迁移正常完成。 通过show dnodes查看，vnode会优先平均分布在非mnode所在节点。 迁移后数据正确。 | 符合预期 |
| 2 | 1.执行balance vgroup命令 2.vnode迁移过程中，将dnode2,3下线再上线 | vnode迁移正常完成。 通过show dnodes查看，vnode会优先平均分布在非mnode所在节点。 迁移后数据正确。 | 符合预期 |
| 3 | 1.执行balance vgroup命令 2.vnode迁移过程中，将dnode1下线再上线 | vnode迁移正常完成。 通过show dnodes查看，vnode会优先平均分布在非mnode所在节点。 迁移后数据正确。 | 符合预期 |
