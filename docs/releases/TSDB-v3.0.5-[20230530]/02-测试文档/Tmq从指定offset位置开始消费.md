# Tmq从指定offset位置开始消费

参考开发手册：[TMQ 行为优化](https://taosdata.feishu.cn/wiki/wikcn74Uarh7C3bMReAwByOFxSd) 

| 行号 | auto.offset.reset 参数 | experimental.snapshot.enable参数 | 首次启动消费 | 是否poll过数据 | tmq_get_topic_assignment | tmq_offset_seek |
| --- | --- | --- | --- | --- | --- | --- |
| 5 | earliest/latest | false | Y | N | earliest时：currentOffset = wal开始版本 latest时：currentOffset = wal结束版本 | 正常seek |
| 6 | earliest/latest | false | Y | Y | 消费到wal，返回当前进度 | 消费到wal，正常seek |
| 7 | earliest/latest | false | N | N | 两种情况 如果存储了commit进度，返回进度 如果没有存储commit 进度，wal开始版本/wal结束版本 | 正常seek |
| 8 | earliest/latest | false | N | Y | 消费到wal，返回当前进度 | 正常seek |

## 1. 测试场景

一、1个vnode，1个consumer 
1、写入10000条记录；
2、reset=earliest 启动消费，poll前获取 offset，进行seek（分别seek到 wal开始，wal结束，wal中间）；poll一次后获取offset，进行seek（分别seek到 wal开始，wal结束，wal中间）；
3、异常场景：进行seek时，分别seek到 wal-1开始，wal+1结束）；

二、2个vnode， 1个consumer

三、1个vnode，2个consumers，没有分配到vnode的consumer进行offsert操作（异常场景）

四、2个vnode， 2个consumers

五、offset设置过程中，发生rebalance。2个vnode，开始2个consumers，再增加1个consumer
