# TD-25112采用 Learner 机制最小化 split vgroup 阻塞写入的时间测试

## 1. 测试目标

Task：[TD-25112](https://jira.taosdata.com:18080/browse/TD-25112)采用 Learner 机制最小化 split vgroup 阻塞写入的时间测试
分裂任务执行过程中，可持续提供读写服务；期间，可能存在可感知的短暂的写业务中断。测试写中断对业务的影响

### 1.1 测试环境：

192.168.1.176(taosd)主：
CPU: 4核
Mem: 8 GB
Disk: 300GB

### 1.2 测试步骤：

1. 编译启动3节点taosd3.0企业版
2. 编写测试程序测试case分支：case/TD-25112-3.0，文件：splitVgroupByLearner.py
3. 启动线程1，使用benchMark创建db数据库1vgroup，1副本，写入10张表每张1000000条数据
4. 线程1发消息给线程2开始split vgroup后，线程1继续写入1000000
5. 线程2开始进行如下操作：
   - 修改数据库副本为3
   - sleep(5)
   - split vgroup
6. 线程3开始将节点2和节点3，每隔5秒停止和启动1次
7. 线程1写入完毕，测试结束

### 1.3 总结：

测试过程中未发现写入失败的情况， split vgroup对数据写入没有影响
