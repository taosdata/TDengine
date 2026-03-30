# TD-26529:taosd monitor 数据重构和基本观测框架测试报告

## 1. 测试功能简介

 JIRA链接： 
TD-26529

Spec doc：[TDengine 监测](https://taosdata.feishu.cn/wiki/B1W1wfUu8iSefQktLI3cRfeHntd) 

## 2. 测试资源及环境

### 2.1 测试平台

LINUX Ubentu x64

### 2.2 功能测试机器

192.168.0.214
192.168.0.215

### 2.3 性能测试机器

192.168.0.214
192.168.1.51

## 3. 测试报告

| 测试项目 | 测试内容 | 测试结论 | 备注 |
| --- | --- | --- | --- |
| 表taosd_cluster_basic | PASS |  |
| 表taosd_cluster_info | PASS | 仅验证了taosc，未对Websocket和rest链接进行验证，在后续版本中补充测试 |
| 表taosd_vgroups_info | PASS |  |
| 表taosd_dnodes_status_info | PASS |  |
| 表taosd_dnodes_info | PASS | 1. 机器资源相关测试无法做精确性验证，人工根据 taosd 的进程占比来对照，目前没问题 1. info_log_count、trace_log_count和debug_log_count字段数据本次测试未覆盖，在后续版本中补充测试 |
| 表taosd_dnodes_log_dirs | N/A | TDinsight界面未使用，本次测试未覆盖，在后续版本中补充测试 |
| 表taosd_dnodes_data_dirs | PASS |  |
| 表taosd_mnodes_info | PASS | candidate和learner为中间状态，本次测试未覆盖，在后续版本中补充测试 |
| 表taosd_vnodes_info | N/A | TDinsight界面未使用，本次测试未覆盖，在后续版本中补充测试 |
| 表taos_sql_req | PASS | 因为当前版本的统计数据包含了系统服务的request数量，例如监控服务自身的插入和查询操作，测试中无法对request的数量做精确性验证，而已大数量级操作后观察tdinsight中对应count曲线的数量级和趋势进行验证 |
| 表taos_slow_sql | PASS | Cancel状态的统计目前未实现，未做验证 |
| 参数验证： - monitor - monitorInterval - slowLogThreshold | PASS | 不影响核心功能的issue： TD-28829 |
| TDInsight插件测试 | 插件中除taosadapter以外的所有功能 | PASS | TD-28876 |
| 性能测试 | 监控开关状态下的性能对比测试 | Pending | 查询性能对比测试通过 插入性能因插入速度波动性太大，目前无法定性，待后续持续观察 |
| Windows平台兼容性测试 | FAILED | 1. cpu、disk-io、net 三个界面没有结果 TD-28834 |
| 旧版taosd+旧版taosadapter+新版taoskeeper | PASS | 仅测试升级taoskeeper后旧版tdinsight数据可正确展示 |
| 大流量并发场景 |  | Waiting | 没有环境做持续稳定性验证，后续补充 |

## 4. 测试重点及难点

1. 测试项目中有状态值验证的项目，其值是瞬时状态，测试中很难准确的抓取到其状态值进行验证
2. 重新设计的表结构中字段非常多，测试过程需要针对所有字段设计对应的测试用例并进行数据验证，该测试过程许多很难通过脚本自动化验证，问题修复后的回测测试工作量大，效率低
3. 因历史原因，缺少最初的需求和设计文档，当前系统资源监控的部分功能预期目标不明确，测试过程中主观判断为主
4. 测试环境为2个集群，分别有2名测试人员各自进行测试，在验证独立集群统计功能同时验证多集群间的数据统计正确性
5. 系统资源的校验需要通过对比prometheus的采集数据进行图形化对比验证
6. tdinsight区分用户，在测试过程中需要使用非root用户进行测试和数据验证

## 5. 测试用例

### 5.1 测试结果

| 分类 | 测试内容/步骤 | 预期 | 结果 | 备注 |
| --- | --- | --- | --- | --- |
| taosd_cluster_basic表类型 | taosd_cluster_basic为超级表 | P |  |
| tag:cluster_id | 1.数据类型为varchar(50) 2.存储集群的cluster_id值正确 | P |  |
| 列:first_ep | 1.数据类型为varchar(100) 2.配置文件/etc/taos.taos.cfg | P |  |
| 列:version | 1.数据类型为varchar(100) 2.show cluster命令里的version字段 | P |  |
| 列:first_ep_dnode_id | 1.数据类型为int 2.集群 first ep 的 dnodeid | P |  |
| taosd_cluster_info表类型 | taosd_cluster_info为超级表 | p |  |
| tag:cluster_id | 1.数据类型为varchar(50) 2.存储集群的cluster_id值正确 | p |  |
| 列cluster_uptime | 集群重启后的时间，单位为分钟 | p |  |
| 列dbs_total | 1.集群中所有db 的数量总和 2.集群db数量减少时dbs_total数值正确 3.集群db全部删除后dbs_total数值为2，分别为log和audit | p | Show databases |
| 列tbs_total | 1.集群中所有table 的表数量总和 2.集群db的表数量减少时tbs_total数值正确 3.集群表全部删除后tbs_total数值为0 4.集群tbs为10W+，dbs_total数值为0 | p | select count(*) from information_schema.ins_tables where db_name != 'information_schema' and db_name != 'performance_schema'; |
| 列stbs_total | 1.集群中所有stable 的超级表数量总和 2.集群db的超级表数量减少时stbs_total数值正确 3.集群超级表全部删除后stbs_total数值为0 4.集群stb为10W+，dbs_total数值为0 | p | select count(*) from information_schema.ins_stables where db_name != 'information_schema' and db_name != 'performance_schema'; |
| 列dnodes_total | 1.新增1个dnode，统计数量2 2.新增1个dnode，统计数量3 3.drop 1个dnode，统计数量2 4.drop 1个dnode，统计数量1 | p |  |
| 列dnodes_alive | 1.create 3个dnode，统计数量3 2.停止1个taods进程，统计数量2 3.在停止2个taods进程，统计数量1 4.在恢复1个taods进程，统计数量0 | p |  |
| 列mnodes_total | 1.新增1个mnode，统计数量2 2.新增个mnode，统计数量3 3.drop 1个mnode，统计数量2 4.drop 1个mnode，统计数量1 | p |  |
| 列mnodes_alive | 1.新增1个mnode，统计数量2 2.新增个mnode，统计数量3 3.drop 1个mnode，统计数量2 4.drop 1个mnode，统计数量1 | p |  |
| 列vgroups_total | 1.taosd1里db的vgroup配置为2，创建5个db，vgroups_total=10 2.taosd2里，db的vgroup配置为3。创建5个db，vgroups_total=25 3.taosd2上修改db的vgroup为4，vgroups_total=30 4.停止第一个taosd进程，vgroups_total=20 | p |  |
| 列vgroups_alive | 1.kill taosd所在进程，vgroups_alive数量 = vgroups_total - 关闭taosd进程上对应的vgroup数量 2.恢复taosd所在进程，vgroups_alive = vgroups_total | p |  |
| 列vnodes_total | 1.db的vgroup配置为2，创建5个db，vnodes_total=10 2.修改一个db的vgroup为4，vnodes_total=12 4.删除一个vgroup配置为2 的db，vnodes_total=10 | p |  |
| 列vnodes_alive | 1.kill taosd所在进程，vnodes_alive数量 = vgroups_total - 关闭taosd进程上对应的vnode数量 2.恢复taosd所在进程，vnodes_alive = vnodes_total | p |  |
| 列connections_total | 1.创建3个taosc连接，connections_total=3 2.建立1个**原生连接，connections_total=4** 3.建立1个**REST 连接，connections_total=5** 4.建立1个Websocket链接**，connections_total=6** 5.断开2个taosc链接**，connections_total=4** 参考资料：[TDengine Python Connector](https://docs.taosdata.com/connector/python/) 【纪要】**补充connection的类型，Websocket是长连接，rest依赖于adapter的连接池配置数量，主要测试taosc** | p(仅验证了taosc) |  |
| 列topics_total | 1.taosc链接1上创建2个topic topics_total=2 2.taosc链接2上创建3个topic topics_total=5 3.taosc链接2上删除1个topic topics_total=4 4.taosc链接2删除所有topic topics_total=0 参考资料：[数据订阅](https://docs.taosdata.com/taos-sql/tmq/) | p |  |
| 列streams_total | 1.taosc链接1上创建2个stream streams_total=2 2.taosc链接2上创建3个stream streams_total=5 3.taosc链接2上删除1个stream streams_total=4 4.taosc链接2删除所有stream streams_total=0 参考资料：[流式计算](https://docs.taosdata.com/taos-sql/stream/#%E5%88%9B%E5%BB%BA%E6%B5%81%E5%BC%8F%E8%AE%A1%E7%AE%97) | p |  |
| 列grants_expire_time | ./taosGrant_linux64.3230 -k 732768337202661772 --basic 2024-02-24,10000000000000,8,1024 其中basic的第一个参数是超期日期，第二个是测点数 [授权机制优化](https://taosdata.feishu.cn/wiki/OydKwSf1jidC04ki9V2c65NvnKe) | p |  |
| 列grants_timeseries_used | 授权机制正在开发，仅是调用API获取数据吗？ [授权机制优化](https://taosdata.feishu.cn/wiki/OydKwSf1jidC04ki9V2c65NvnKe) | P | select sum(cl) from (select columns-1 as cl from information_schema.ins_tables where type !="SYSTEM_TABLE"); 统计现在已经有的测点数： timeserial数量的计算逻辑是 子表数量*（列数量-1） |
| 列grants_timeseries_total | 授权机制正在开发，仅是调用API获取数据吗？ [授权机制优化](https://taosdata.feishu.cn/wiki/OydKwSf1jidC04ki9V2c65NvnKe) | p |  |
| tag:cluster_id | 1.数据类型为varchar(50) 2.存储集群的cluster_id值正确 | p |  |
| tag:vgroup_id | 1.数据类型为varchar(300) 2.存储集群的vgroup_id值正确 | P schemaless模式，长度动态配置 |  |
| tag:database_name | 1.数据类型为varchar(300) 2.存储集群的database_name值正确 | P schemaless模式，长度动态配置 |  |
| 列tables_num | taosBenchmark插入10w张表，分别计算每个vgroup的tables_num = show vgroups查询每个vgroup的数量 | P | tables select count(tbname) from information_schema.ins_tables where type !="SYSTEM_TABLE"; stables select count(tbname) from information_schema.ins_stables; |
| 列status | 1.创建三副本的db，vgroup状态为1(ready) 2.停止1个dnode，对应的vgroup状态为0(Unsynced) | p |  |
| tag:cluster_id | 1.数据类型为varchar(300) 2.存储集群的cluster_id值正确 | p |  |
| tag:dnode_id | 1.数据类型为varchar(300) 2.存储集群的dnode_id值正确 | p |  |
| tag:dnode_ep | 1.数据类型为varchar(300) 2.存储集群的dnode_ep值正确 | p |  |
| 列status | 启停dnode，查看对应的状态值 | p |  |
| tag:cluster_id | 1.数据类型为varchar(300) 2.存储集群的cluster_id值正确 | P |  |
| tag:dnode_id | 1.数据类型为varchar(300) 2.存储集群的dnode_id值正确 | P |  |
| tag:dnode_ep | 1.数据类型为varchar(300) 2.存储集群的dnode_ep值正确 | P |  |
| 列status | 1.搭建一个三节点集群，所有dnode的状态都是1（ready） 2.关闭其中一个节点的进程，其对应的dnode状态为0（offline） | P |  |
| 列uptime | 集群重启后的时间，单位为天 | P |  |
| 列cpu_engine | 1. 通过跟prometheus的监控图形对比进行验证 | p |  |
| 列cpu_system | 1. 通过跟prometheus的监控图形对比进行验证 | p |  |
| 列cpu_core | 通过命令cat /proc/cpuinfo| grep "processor"| wc -l验证cpu core数量 | p |  |
| 列mem_engine | 1. 通过命令free -h验证总内存 1. 通过跟prometheus的监控图形对比进行验证 | p |  |
| 列mem_system | 1. 通过命令free -h验证总内存 1. 通过跟prometheus的监控图形对比进行验证 | p |  |
| 列mem_total | 1. 通过命令free -h验证总内存 1. 通过跟prometheus的监控图形对比进行验证 | p |  |
| 列disk_used | 1. 通过命令df-h验证 1. 通过跟prometheus的监控图形对比进行验证 | p |  |
| 列disk_total | 1. 通过命令df-h验证 1. 通过跟prometheus的监控图形对比进行验证 | p |  |
| 列net_in | 1. 通过跟prometheus的监控图形对比进行验证 | p |  |
| 列net_out | 1. 通过跟prometheus的监控图形对比进行验证 | p |  |
| 列io_read | 1. 通过跟prometheus的监控图形对比进行验证 | p |  |
| 列io_write | 1. 通过跟prometheus的监控图形对比进行验证 | p |  |
| 列io_read_disk | 1. 通过跟prometheus的监控图形对比进行验证 | p |  |
| 列io_write_disk | 1. 通过跟prometheus的监控图形对比进行验证 | p |  |
| 列vnodes_num | dnode 所在节点的 vnodes 数量 | P | select * from information_schema.ins_vgroups where v1_dnode=1 or v2_dnode=1 or v3_dnode=1; select * from information_schema.ins_vgroups where v1_dnode=2 or v2_dnode=2 or v3_dnode=2; select * from information_schema.ins_vgroups where v1_dnode=3 or v2_dnode=3 or v3_dnode=3; |
| 列masters | 通过select dnode_id,count(status) from information_schema.ins_vnodes where status='leader' partition by dnode_id检查该字段数量 | P | select * from information_schema.ins_vgroups where (v1_dnode=3 and v1_status='leader') or (v2_dnode=3 and v1_status='leader') or (v3_dnode=3 and v1_status='leader'); |
| 列has_mnode | 1.dnode上新建mnode has_mnode=1 2.dnode上删除mnode has_mnode=0 参考：[集群管理](https://docs.taosdata.com/taos-sql/node/) | P | select last(has_mnode),dnode_id from log.taosd_dnodes_info group by dnode_id; |
| 列has_qnode | 1.dnode上新建qnode has_mnode=1 2.dnode上删除qnode has_mnode=0 参考：[集群管理](https://docs.taosdata.com/taos-sql/node/) | p | select last(has_qnode),dnode_id from log.taosd_dnodes_info group by dnode_id; |
| 列has_snode | 1.dnode上新建snode has_mnode=1 2.dnode上删除snode has_mnode=0 参考：[集群管理](https://docs.taosdata.com/taos-sql/node/) | p | select last(has_snode),dnode_id from log.taosd_dnodes_info group by dnode_id; |
| 列has_bnode | 目前实现中没有bnode的概念 | 【纪要】删除该字段 |  |
| errors | 如何触发该error | 【纪要】删除该字段 |  |
| 列error_log_count | 1. 三节点集群分别启动taosd前清空log日志文件 1. 启动一段时间后分别停止taosd，查看日志文件对应ERROR 行数，统计数据应该正确 | p | ERROR msg:0x7f0914008c28, failed to process since Dnode does not exist, app:0x9527 type:status, gtid:0x0:0x42a61e4b9cf00679 |
| 列info_log_count | 1. 三节点集群分别启动taosd前清空log日志文件 1. 启动一段时间后分别停止taosd，查看日志文件对应info行数，统计数据应该正确 | 【纪要】不做验证 |  |
| 列debug_log_count | 由于日志文件中无法通过关键字区分trace和info日志，无法对统计数据的正确性进行验证 | 【纪要】不做验证 |  |
| 列trace_log_count | 由于日志文件中无法通过关键字区分trace和info日志，无法对统计数据的正确性进行验证 | 【纪要】不做验证 |  |
| tag:cluster_id | 1.数据类型为varchar(300) 2.存储集群的cluster_id值正确 | 界面没有 未测试 |  |
| tag:dnode_id | 1.数据类型为varchar(300) 2.存储集群的dnode_id值正确 |  |  |
| tag:dnode_ep | 1.数据类型为varchar(300) 2.存储集群的dnode_ep值正确 |  |  |
| tag:log_dir_name | 1.数据类型为varchar(300) 2.若配置文件没有配置logDir，值为/etc/taos/ 3.若配置文件配置了logDir,值为logDir的值 |  |  |
| 列avail | 日志文件所在磁盘的可用空间容量（byte） |  |  |
| 列used | 日志文件所在磁盘的已用空间容量（byte） |  |  |
| 列total | 日志文件所在磁盘的总空间容量（byte） |  |  |
| tag:cluster_id | 1.数据类型为varchar(300) 2.存储集群的cluster_id值正确 | P |  |
| tag:dnode_id | 1.数据类型为varchar(300) 2.存储集群的dnode_id值正确 | P |  |
| tag:dnode_ep | 1.数据类型为varchar(300) 2.存储集群的dnode_ep值正确 | P |  |
| 列data_dir_name | 1.数据类型为varchar(300) 2.若配置文件没有配置dataDir，值为默认值/var/lib/taos/ 3.若配置文件配置了dataDir,值为dataDir的值 | P |  |
| 列data_dir_level | 配置多级存储，查看每个数据路径对应的目录是否正确 参考资料：[多级存储](https://docs.taosdata.com/tdinternal/arch/#%E5%A4%9A%E7%BA%A7%E5%AD%98%E5%82%A8) | p |  |
| 列avail | 数据文件所在磁盘的可用空间容量（byte） | p |  |
| 列used | 数据文件所在磁盘的已用空间容量（byte） | p |  |
| 列total | 数据文件所在磁盘的总空间容量（byte） | p |  |
| tag:cluster_id | 1.数据类型为varchar(300) 2.存储集群的cluster_id值正确 | P |  |
| tag:mnode_id | 1.数据类型为varchar(300) 2.存储集群的mnode_id值正确 | P |  |
| tag:mnode_ep | 1.数据类型为varchar(300) 2.存储集群的mnode_ep值正确 | P |  |
| 列:role | 1.创建3节点集群，create三个mnode，1个role=102（leader），2个role=100（follower） 2.kill一个follower对应taosd进程，对应的role=0（offline） 3.drop一个follower对应taosd进程，对应的mnode停止上报 3.follower、candidate和learner为中间状态，需要测试过程中看是否能抓取到 4.error的状态还不确定如何从黑盒角度验证 | P |  |
| tag:cluster_id | 1.数据类型为varchar(300) 2.存储集群的cluster_id值正确 | 界面没有 未覆盖 |  |
| tag:mnode_id | 1.数据类型为varchar(300) 2.存储集群的mnode_id值正确 |  |  |
| tag:dnode_id | 1.数据类型为varchar(300) 2.存储集群的dnode_id值正确 |  |  |
| 列:vnode_role | 1.创建3节点集群，create三个mnode，三个role=102（leader） 2.kill一个taosd进程，对应的role=0（offline） 3.follower、candidate和learner为中间状态，需要测试过程中看是否能抓取到 4.error的状态还不确定如何从黑盒角度验证 【纪要】learner在数据量大的场景下验证，优先级低 |  |  |
| tag:cluster_id | 1.数据类型为varchar(300) 2.存储集群的dnode_ep值正确 | p |  |
| tag:vgroup_id | 1.数据类型为varchar(300) 2.存储集群的dnode_ep值正确 | p |  |
| tag:dnode_ep | 1.数据类型为varchar(300) 2.存储集群的dnode_ep值正确 | p |  |
| tag:username | 验证不同user的sql执行过滤功能 | p |  |
| tag:result | 分别验证3个状态： Success, Failed, | p |  |
| 请求数量 | p(数量级别验证) |  |
| 不同taosc进行select或insert操作，查看count数据正确 | p |  |
| select或insert过程中ctrl+c中断，查看对应的result=failed数量增加数量匹配 | p |  |
| Show databases，上报数据 | p |  |
| Use database_name，上报数据 | p |  |
| Show tables，上报数据 | p |  |
| Show stables，上报数据 | p |  |
| Show grants;，上报数据 | p |  |
| Create table t3(ts timestamp, v1 int)，上报数据 | p |  |
| FLUSH DATABASE db_name，上报数据 | p |  |
| SHOW db_name.ALIVE，上报数据 | p |  |
| ALTER TABLE t2 ADD COLUMN v2 int，上报数据 | p |  |
| ALTER TABLE t2 RENAME COLUMN v1 v3，上报数据 | p |  |
| 一般 insert ，上报数据 | p |  |
| 一般 select，上报数据 | p |  |
| Select 1，上报数据 | p |  |
| 一般 delete，上报数据 | p |  |
| Insert into t1 select * from t2，上报数据 | p |  |
| tag:cluster_id | 1.数据类型为varchar(300) 2.存储集群的cluster_id值正确 | p |  |
| tag:username | 1.创建user1，并执行sql超过20s 2.新增慢查询记录的username=user1 | p |  |
| tag:result | 分别验证3个状态： Success, Failed, Cancel | P | Cancel状态现在未实现 |
| tag:duration | 验证一下四类慢查询： 1. 3-10s 1. 10-100s 1. 100-1000s 1. 1000s- 其中边界值因无法控制sql的完成时间，难以对边界值进行校验 【纪要】通过taosbenchmark来验证数据的整体分布情况 | p(验证了前两个区间) | select count(*), `duration` from taos_slow_sql where `count` != 0 group by `duration`; |
|  |  |  |  |
| 服务端配置monitorInterval=2 | 服务端监控数据采集间隔变更为2s | p |  |
| 客户端配置monitorInterval=2 | 客户端监控数据采集间隔变更为2s | p |  |
| 不同节点上monitorInterval配置值不同 （client-server） | 数据采集和tdinsight显示无异常 | P |  |
| 客户端配置慢查询阈值slowLogThreshold | 1. 客户端使用默认slowLogThreshold=3，3s以下的查询不会被记录为慢查询 1. 记录一些3-10s的慢查询，数据库能查到对应数据 1. 修改slowLogThreshold=20，20s以下的查询不会被记录为慢查询 | p |  |
| 多个集群向log库中写数据 | 在db层验证数据正确性 | 【纪要】本期不测试 |  |
| 1. 先启动taoskeeper 1. 后启动taosd | taoskeeper会在log库中正确创建监控所用表格，监控数据写入正确 | p |  |
| 大流量并发场景 | 2套db，24小时连续性数据采集验证，期间利用taosBenchmark和性能测试工具，周期性进行数据的插入和查询操作 |  | p目前没有2套环境运行该测试，只在一套db上运行12+小时 | @翟坤 |
| 异常测试 | 关闭dnode进程 | 统计数据是正确上报和展示 | P |  |
| Windows 平台本次测试覆盖 | 搭建起来看效果 | F | @陈浩然 目前只有四个界面有数据，其余均无数据 |
| 旧版taosd+新版taoskeeper 1. 安装3.1.1.27 release版本 1. 启动taosd、taosadapter和taoskeeper 1. 停止旧版taokeeper后启动最新版本taoskeeper | 旧版的上报机制工作正常，tdinsight显示正确 | p |  |
| 性能测试 | 子表数：1W，子表行数：1W，总行数：1亿，步长：1毫秒，列数：16个，tag数量：8个 | 高频测试场景性能对比，插入和查询性能不允许下降 | 参见第六章节性能测试 | @翟坤 |
| 1. 运行旧版监控工具1小时采集旧数据 1. 停止监控服务，升级db 1. 执行taoskeeper数据迁移命令 1. 重启监控服务 | 1. tdinsight上监控数据显示正确 1. db升级成功 1. 旧表数据不变，生成新表数据 1. tdinsight上监控数据显示正确，新采集数据存储正确 | 【纪要】本期未实现 |  |
| 执行taoskeeper旧表删除命令 | 旧表数据被删除，新表不受影响 | 【纪要】本期未实现 |  |

### 5.2 表taos_sql_req目前统计的请求类型

#### 5.2.1 参考文档

[Sql 类型测试补充说明](https://taosdata.feishu.cn/wiki/YCxYwVN6WiMtjAkckWocLxrNnHg)

#### 5.2.2 测试方案

批量执行相同情况，统计数据会快速提升，依次来验证是否被统计

#### 5.2.3 Request统计范围

| SQL类型 | 对应类型 | 类型id |
| --- | --- | --- |
| query（select）类 | QUERY_NODE_SELECT_STMT | 101 |
| QUERY_NODE_VNODE_MODIFY_STMT | 102 |
| QUERY_NODE_INSERT_STMT | 194 |
| delete类 | QUERY_NODE_DELETE_STMT | 193 |

#### 5.2.4 测试数据

| **SQL** | **类型** |
| --- | --- |
| Show databases | QUERY_NODE_SELECT_STMT |
| Use database_name | QUERY_NODE_SELECT_STMT |
| Show tables | QUERY_NODE_SELECT_STMT |
| Show stables | QUERY_NODE_SELECT_STMT |
| Show grants; | QUERY_NODE_SELECT_STMT |
| Create table t3(ts timestamp, v1 int); | QUERY_NODE_VNODE_MODIFY_STMT |
| FLUSH DATABASE db_name; | QUERY_NODE_VNODE_MODIFY_STMT |
| SHOW db_name.ALIVE; | 产生三种类型： 182 QUERY_NODE_SELECT_STMT 196 |
| ALTER TABLE t2 ADD COLUMN v2 int; | QUERY_NODE_VNODE_MODIFY_STMT |
| ALTER TABLE t2 RENAME COLUMN v1 v3 | QUERY_NODE_VNODE_MODIFY_STMT |
| 一般 insert | QUERY_NODE_VNODE_MODIFY_STMT |
| 一般 select | QUERY_NODE_SELECT_STMT |
| Select 1; | QUERY_NODE_SELECT_STMT |
| 一般 delete | QUERY_NODE_DELETE_STMT |
| Insert into t1 select * from t2; | QUERY_NODE_INSERT_STMT |

## 6. 性能测试

### 6.1 测试结论

1. 插入性能随着插入的次数增多会出现比较大的波动，目前很难从查询结果上为监控性能影响进行定位
2. 新监控框架对查询性能影响小于10%，具体见以下描述：
  - 以3.0版本企业版查询速度为基础数据，三个查询sql在开发分支的查询速度与之对比，最大的差异分别慢约9.21%（毫秒级查询），0.67%和1.43%
  - 监控开发分支在开启和关闭monitor开关后，最大差异为0.95%

| 测试场景 | 测试用例 | 3.0企业版（default） | 3.0企业版-开发分支（monitor=0） | 3.0企业版-开发分支(default) | 3.0企业版-开发分支（开监控:monitorInterval=30s） | 3.0企业版-开发分支（开监控:monitorInterval=2s） | 较3.0企业版（default）速度减低百分比 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| select bc,fc,nch,var from meters limit 100 offset 10000 | Avg: 0.070419s | Avg: 0.070611s | Avg: 0.07047s | Avg: 0.071704s | Avg: 0.076908s | 9.21% |
| select * from meters order by ic, bi, uti limit 100 | Avg: 7.310993s | Avg: 7.359856s | Avg: 7.290598s | Avg: 7.304873s | Avg: 7.3115785s | 0.67% |
| select bc,fc from meters order by bc,fc | Avg: 64.978192s | Avg: 65.905161s | Avg: 64.997732s | Avg: 64.512434s | Avg: 65.389317s | 1.43% |


### 6.2 测试环境

插入性能测试机器：192.168.0.215
查询性能测试机器：192.168.1.51

| IP | MEM | CPU | DISK |
| --- | --- | --- | --- |
| 192.168.0.215 | 64G | Intel(R) Xeon(R) CPU E5-2620 v3 @ 2.40GHz | 450G |
| 192.168.1.51 | 256G | Intel(R) Xeon(R) CPU E5-2650 v3 @ 2.30GHz | 450G |

### 6.3 测试方案

#### 6.3.1 通过性能测试监控工具对性能进行验证

3.0分支在合入监控功能代码后，daily性能数据曲线无明显性能衰退
截止到2.26日，查询曲线无明显连续性大幅波动，详情参见：[benchmark性能监控数据](http://192.168.0.204:3000/d/d0ca4afa-177f-48ab-9606-b907d54b1cbb/a2e978d6-52c5-5d16-bea1-ee9d8c1e8983?orgId=1&refresh=15m&var-scenario=%E5%B8%B8%E7%94%A8%E5%9C%BA%E6%99%AF&var-datasource=c67ffd50-15f6-4c9f-b0b1-7c7e683ff205&var-database=telegraf&var-inter=$__auto_interval_inter&var-server=test216&var-mountpoint=All&var-cpu=All&var-disk=All&var-netif=All&var-irq=All)

#### 6.3.2 通过taosBenchmark进行性能对比测试

##### 6.3.2.1 插入场景&运行命令

插入以下面场景为基础，针对开启和关闭monitor开关做性能对比测试

| 子表数 | 子表行数 | 总行数 | 步长 | 列数 | tag数量 |  |
| --- | --- | --- | --- | --- | --- | --- |
| 1W | 1W | 1亿 | 1毫秒 | 16个 | 8个 |  |

运行命令
```bash
taosBenchmark -f /root/chris/TestNG/perf-test/test_cases/scenarios/rand_common.json
```

##### 6.3.2.2 查询场景&运行命令

| 查询SQL | taosBenchmark命令 | 运行次数 | 并发数量 |
| --- | --- | --- | --- |
| select bc,fc,nch,var from meters limit 100 offset 10000 | taosBenchmark -f /root/chris/TestNG/perf-test/test_cases/query_projection/query5.json | 200 | 10 |
| select * from meters order by ic, bi, uti limit 100 | taosBenchmark -f /root/chris/TestNG/perf-test/test_cases/query_aggregate/query4.json | 50 | 1 |
| select bc,fc from meters order by bc,fc | taosBenchmark -f /root/chris/TestNG/perf-test/test_cases/query_projection/query4.json | 10 | 1 |

### 6.4 性能测试结果

#### 6.4.1 插入测试结果

| 3.0版本企业版（default） | 3.0版本企业版（monitor=0） | 3.0版本企业版（开监控:monitorInterval=30s） | 3.0版本企业版（开监控:monitorInterval=2s） |
| --- | --- | --- | --- |
| 329786.76 (real 344969.03) records/second 251017.66 (real 256767.75) records/second 238821.36 (real 248460.03) records/second 322353.38 (real 338763.47) records/second 237831.73 (real 246408.15) records/second 239412.42 (real 247453.76) records/second 239412.42 (real 247453.76) records/second | 331361.62 (real 344267.52) records/second 230962.43 (real 237404.73) records/second 260922.18 (real 271847.47) records/second 250880.86 (real 264039.01) records/second 317417.62 (real 334603.83) records/second 315757.94 (real 335524.83) records/second 260593.16 (real 268864.72) records/second | 315125.97 (real 330137.70) records/second 316245.19 (real 331214.26) records/second 300362.17 (real 312947.46) records/second 329633.94 (real 345469.92) records/second 250786.76 (real 261218.47) records/second 235815.51 (real 244266.75) records/second 250944.69 (real 260333.37) records/second | 321851.91 (real 335128.13) records/second 239863.90 (real 249078.46) records/second 269201.07 (real 282585.32) records/second 287161.68 (real 295617.84) records/second 311361.63 (real 323968.33) records/second 249411.14 (real 259995.66) records/second 264523.22 (real 275795.73) records/second |

#### 6.4.2 查询测试结果

| 测试用例 | 3.0版本企业版（default） | 3.0版本企业版开发分支（monitor=0） | 3.0企业版开发分支(default) | 3.0企业版开发分支（开监控:monitorInterval=30s） | 3.0企业版开发分支（开监控:monitorInterval=2s） |
| --- | --- | --- | --- | --- | --- |
| 0.072094s 0.070117s 0.069527s 0.069941s | 0.072307s 0.073429s 0.075294s 0.071396s 0.067312s 0.063926s | 0.072370s 0.069760s 0.069386s 0.070382s | 0.071470s 0.071526s 0.074112s 0.071293s 0.070119s | 0.078394s 0.076620s 0.075710s |
| Avg: 0.070419s | Avg: 0.070611s | Avg: 0.0704745s | Avg: 0.071704s | Avg: 0.076908s |
| 7.313381s 7.312314s 7.307284s | 7.353348s 7.357950s 7.368269s | 7.289593s 7.301347s 7.280854s | 7.303357s 7.302677s 7.308584s | 7.299853s 7.303310s 7.298153s 7.344998s |
| Avg: 7.310993 | Avg: 7.359856 | Avg: 7.290598 | Avg: 7.304873 | Avg: 7.3115785 |
| 65.144908s 64.667077s 65.122591s | 66.131344s 65.657012s 65.927128s | 64.388773s 65.376515s 65.065466s 65.016043s 65.141865s | 65.848275s 64.747448s 64.058772s 63.905478s 64.002196s | 64.320218s 66.117576s 65.730156s |
| Avg: 64.978192 | Avg: 65.905161 | Avg: 64.997732 | Avg: 64.5124338 | Avg: 65.389317 |


## 7. 用例评审会议纪要

### 7.1 评审日期

2024.2.21

### 7.2 评审纪要

| NO. | 内容 | 负责人 |
| --- | --- | --- |
| 1 | taosd_cluster_info->cluster_uptime，数据采集颗粒度为分钟 | 东明 |
| 2 | tdinsight插件的uptime时间显示格式改为：天、时、分 | 彦杰 |
| 3 | taosd_cluster_info->connection_total的验证策略： 1. 优先测试taosc的连接数统计 1. 时间足够的情况测试Websocket连接数，注：Websocket是长连接，每新增一个Websocket链接，connection数量加1 1. rest依赖于adapter的连接池配置数量，创建和删除rest链接对connection数量无影响，测试优先级最低 | 翟坤 |
| 4 | taosd_dnodes_info->cpu_engine和cpu_system字段的数据正确性也通过prometheus对比验证 | 翟坤 |
| 5 | taosd_dnodes_info->has_bnode和error字段无用，应删除 | 东明 |
| 6 | taosd_dnodes_info->error_log_count的error触发方法，需要开发协助提供 | 东明 |
| 7 | itaosd_dnodes_info->info_log_count、debug_log_count和trace_log_count字段不用验证 | 翟坤 |
| 8 | taosd_vnodes_info->vnode_role字段的learner状态可在数据量大的场景下验证，但测试优先级低 | 翟坤 |
| 9 | taos_slow_sql->duration:通过taosbenchmark来验证数据的整体分布情况 | 翟坤 |
| 10 | slowLogThreshold参数验证： 1. 生成数据后，修改slowLogThreshold 1. 该项配置为客户端参数 | 翟坤 |
| 11 | monitorInterval参数验证：测试覆盖服务端和客户端的该项配置 | 翟坤 |
| 12 | 兼容性测试->旧版taosd+新版taoskeeper：本期不测试 | 翟坤 |
| 13 | 迁移工具测试：本期不测试 | 翟坤 |
| 14 | 云服务端的测试场景->多个监控服务向一个db写数据：本期不测试 | 翟坤 |
| 15 | 监控服务统计数据受系统服务影响导致数据“失真”的问题作为已知问题，暂不修复 | 翟坤 |
| 16 | request的统计数据仅包含server端的异常数据，不统计c端 | 翟坤 |


##
