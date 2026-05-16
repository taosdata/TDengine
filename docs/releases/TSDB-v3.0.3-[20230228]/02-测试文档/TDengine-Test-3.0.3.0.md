# TDengine-Test-3.0.3.0

20230228版本测试任务进展：
大部分功能都已经完成，其中有几个报告的结果需要更新。没有完成的测试：
1、compact；
2、tag index：功能已经完成验证，但有bug没有fix；性能测试还没有完成；
3、taosExplorer 跟着开发分步提测走，大约完成了 30%；
4、taosBenchmark两个功能没有启动；
5、“3.0 企业版 show local variables 增加 community gitinfo“ ：这个还没有完成。
6、schemaless性能优化后的测试没有做，下周有人力释放，可以安排启动。

风险：
1、流的大流量长时间测试，需要在长稳测试环境补充；
2、compact功能的稳定；
3、explore 2.24全部提测；

20230228版本测试初步复盘：
1、有些user manual 提交后并不稳定，文档还会随着开发进展变化，测试过程中会出现 内容不对，内容变更等情况，导致测试方案又需要跟着修改，浪费了时间；
2、测试时间还是太短，测试--发现问题--解决问题--回归测试；

20230224 进展记录
> [WARNING] 电子表格读取失败 (token: shtcnOrGVSxr59eKFr0kD5NbjKg_s96azw)
