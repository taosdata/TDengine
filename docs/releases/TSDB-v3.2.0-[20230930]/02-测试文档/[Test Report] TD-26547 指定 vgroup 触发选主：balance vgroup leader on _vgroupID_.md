# [Test Report] TD-26547 指定 vgroup 触发选主：balance vgroup leader on <vgroupID>

### 1. 概述：

在客户场景中，3节点3副本环境下有时会出现数据库vgroup leader分布不均匀的情况，而当前的balance vgroup leader命令会将数据库所有vgroup的leader重新选举一次，存在如下两个问题：
1. 对所有vgroup leader重新选举，存在leader不变的概率，导致vgroup的leader分布依然不均衡，需要再次均衡
2. 对所有vgroup leader重新选举耗时较长
针对以上问题，此任务增加指定vgroup触发选主，通过指定vgroup的方式，减少均衡范围；且耗时减少，可多次选主达到最终的vgroup均衡

### 2. 测试环境：

192.168.1.35：
CPU: Intel(R) Xeon(R) CPU E5-2630 v2 @ 2.60GHz （2）24核
Mem: DDR3  32 GB * 2
Disk: 2792GB

### 3. 测试用例：

| 用例类型 | 用例名称 | 用例描述 | 期望结果 | 测试结果 |
| --- | --- | --- | --- | --- |
| 3节点无数据持续写入场景，对指定vgroup多次选主 | 1. 安装部署3节点3副本集群环境 1. 创建数据库并写入部分数据， vgroup数为10 1. 数据写入完成后，通过命令balance vgroup leader on xx；对指定vgroup多次执行选主 1. 重启部分dnode节点，使vgroup leader分布不均衡 1. 重复步骤3 | 1. 集群安装部署完成 1. 创建数据库、写入数据完成 1. 对指定vgroup多次选主完成，无报错，无crash 1. dnode节点重启完成，vgroup leader分布不均衡 1. 对指定vgroup多次选主完成，无报错，无crash | 1. 安装部署正常 1. 创建数据库，写入数据正常 1. 对指定vgroup多次选主完成，vgroup leader分配均衡 1. dnode节点重启完成，vgroup leader分布不均衡 1. 对指定vgroup多次选主完成，vgroup leader分配均衡 |
| 3节点数据持续写入，对指定vgroup多次选主 | 1. 安装部署3节点3副本集群环境 1. 创建数据库并持续写入部分数据， vgroup数为10 1. 数据写入、查询持续进行中，通过命令balance vgroup leader on xx；对指定vgroup多次执行选主 1. 重启部分dnode节点，使vgroup leader分布不均衡 1. 重复步骤3 | 1. 集群安装部署完成 1. 创建数据库完成， 数据持续写入、查询执行正常 1. 对指定vgroup多次选主完成，无报错，无crash 1. dnode节点重启完成，vgroup leader分布不均衡 1. 对指定vgroup多次选主完成，无报错，无crash | 1. 安装部署正常 1. 创建数据库，写入数据正常，查询执行正常 1. 对指定vgroup多次选主完成，vgroup leader分配均衡 1. dnode节点重启完成，vgroup leader分布不均衡 1. 对指定vgroup多次选主完成，vgroup leader分配均衡 |
| 稳定性用例 | 1. 安装部署3节点3副本集群环境 1. 创建数据库并持续写入部分数据， vgroup数为10 1. 数据写入、查询持续进行中，通过命令balance vgroup leader on xx；对随机vgroup执行选主 1. 重复步骤3，1000次以上 | 1. 集群安装部署完成 1. 创建数据库完成， 数据持续写入、查询执行正常 1. 对指定vgroup选主完成，无报错，无crash 1. 重复1000次结果与步骤3一致 | 1. 安装部署正常 1. 创建数据库，写入数据正常，查询执行正常 1. 对指定vgroup选主完成 1. 1000次随机指定vgroup选主完成 |
| 触发指定vgroup不存在或vgroupID为空 | 1. 安装部署3节点3副本集群环境 1. 创建数据库并写入部分数据， vgroup数为10 1. 执行命令“balance vgroup leader on；” 1. 执行命令“balance vgroup leader on 11” | 1. 集群安装部署完成 1. 创建数据库、写入数据完成 1. 执行命令报错 1. 执行命令报错 | 1. 安装部署正常 1. 创建数据库，写入数据正常 1. 执行报错“syntax error ” 1. 执行报错“ Invalid operation ” |
| 3节点持续写入场景，单节点异常，对指定vgroup多次选主 | 1. 安装部署3节点3副本集群环境 1. 创建数据库并持续写入部分数据， vgroup数为10 1. 停止某一个节点的taosd服务 1. 数据写入、查询持续进行中，通过命令balance vgroup leader on xx；对指定vgroup多次执行选主 | 1. 集群安装部署完成 1. 创建数据库完成， 数据持续写入、查询执行正常 1. 某一个节点的taosd服务停止 1. 对指定vgroup多次选主完成，无报错，无crash | 1. 安装部署正常 1. 创建数据库，写入数据正常 1. taosd服务停止 1. 对指定vgroup多次选主完成，vgroup leader分配均衡show |

### 4. 总结：

1. 通过命令“balance vgroup leader on xx; ” 指定vgroup触发选主执行正常，能够完成vgroup leader均衡
2. 在3节点3副本环境下，在某个节点异常时，指定vgroup触发选主执行正常
3. 在持续写入数据、查询的场景下，1000次随机触发指定vgroup选主执行正常
4. vgroupID参数为空或不正确时，返回相应错误
