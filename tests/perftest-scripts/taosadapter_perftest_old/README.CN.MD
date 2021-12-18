# 注: 目前测试脚本需按测试报告中的环境说明手动部署taosd及taosadapter
- 测试报告: https://jira.taosdata.com:18090/pages/viewpage.action?pageId=127470422
# 1. 单实例集中部署
## 1.1 环境部署: 在192.168.1.85上直接编译develop分支启动taosd即可
## 1.2 脚本运行方法: 在192.168.1.83环境直接运行python3 /home/ubuntu/TDengine/tests/perftest-scripts/taosadapter_perftest/taosadapter_perftest.py即可, 可输出测试报告中3.4.1和3.5.1中的测试结果, 结果会实时打印到同目录下的taosadapter_perftest.log中


# 2. 多实例分离部署
## 2.1 环境部署: 在192.168.1.98上部署taosd, 在192.168.1.83、84、85、86环境将部署taosadapter, 配置对应的/etc/hosts, 并将/etc/taos/taos.cfg中的firstEp指向taosd的地址(vm98:6030)
## 2.2 脚本运行方法: 
- 在192.168.1.93环境根据节点数量需求运行:
- jmeter -n -t opentsdb_insertRows83.jmx
- jmeter -n -t opentsdb_insertRows84.jmx
- jmeter -n -t opentsdb_insertRows85.jmx
- ...
- 需实时监控各节点cpu占用情况, jmeter结果会直接打印到控制台