# [Test Report] 优化 taosd 上报的 audit 数据

## 1. 背景

TD-26884

## 2. 测试用例

| cases | Test result |
| --- | --- |
| 去除 target1, target2，将操作对象描述在 details 字段中 | pass |
| 加上 client IP，表示该操作的用户所在的IP地址 | pass |
| audit 开关缺省值改为 yes，相应的配置文件模板中出现的应该是 yes | pass |
| cluster ID 仍然要上报 | pass |
| Login details 中的 connType， PID，starttime，sver 去除，只保留app | pass |
| 排查为什么 create db 没有记录 | pass |
| 对于创建和删除子表，在detail中不记录sql，只记录tablename，并且考虑通过配置项来记录，默认打开，对建表性能有要求时关闭 | pass |
| 去掉target1,target2, 改为db和resource | pass |

## 3. 数据上报用例

| Operation | taosc result | rest result | note |
| --- | --- | --- | --- |
| create database | pass | pass |  |
| alter database | pass | pass |  |
| drop database | pass | pass |  |
| alterStb | pass | pass |  |
| create stable | pass | pass |  |
| dropStb | pass | pass |  |
| alter user | pass | pass |  |
| Create user | pass | pass |  |
| drop user | pass | pass |  |
| create topic | pass | pass |  |
| drop topic | pass | pass |  |
| create dnode | pass | pass |  |
| drop dnode | pass | pass |  |
| create mnode | pass | pass |  |
| drop mnode | pass | pass |  |
| login | pass | pass |  |
| create stream | pass | pass |  |
| create qnode | pass | pass |  |
| drop qnode | pass | pass |  |
| alter dnode | pass | pass |  |
| GRANT privileges | pass | pass |  |
| REVOKE privileges | pass | pass |  |
| flush database | fail | fail | By design |
| compact database | pass | pass |  |
| balance vgroup leader | pass | pass |  |
| restore dnode/mnode/vnode/qnode | pass | pass |  |
| REDISTRIBUTE VGROUP | pass | pass |  |
| BALANCE VGROUP | pass | pass |  |
| delete from stable/child table/normal table | fail | fail | By design |

## 4. 发现的问题：

TD-27336


TD-27303


TD-27301


TD-27221


TD-27300
