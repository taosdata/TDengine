# 流计算依据 FS 文档测试完成总结

1. **测试目的**
通过 [流计算新需求与重构 FS](https://taosdata.feishu.cn/wiki/HlKTwwnA2iaF3IkFfY5ctzqknne) 中陈述的功能，对流计算基本功能进行全覆盖测试，目的保障流计算 FS 中声明的功能基本可用。
1. **测试方法**
   - 按 FS 描述逐句验证描述正确性
   - 按 IDMP 产品使用场景测试，流建立在虚拟超级表及虚拟子表上
   - 所有功能都通过写 CASE 完成验证
   - CASE 使用集中式大 CASE 中包括无数小 CASE方式 ，减少 CI 运行时间 
2. **测试内容**
FS 文档中描述内容，本阶段暂不需要发散测试，描述什么测什么完成即可，最基本的测试。
1. **测试输出**
   - 发现 BUG 共计 [39](https://jira.taosdata.com:18080/issues/?jql=project%20in%20(TS%2C%20TD)%2520AND%2520issuetype%2520%253D%2520Bug%2520AND%2520labels%2520%253D%2520stream-dev%2520AND%2520reporter%2520%253D%2520kjduan%2520and%2520createdDate%2520%253E%25272025-08-01%2527%2520and%2520status%2520!%253D%2520CANCELED) 个（从8-10 开始统计）
   - 编写四个大 CASE 共计 **72** 个小 CASE.  
   - 输出测试报告 ，见 [流计算重构 TS](https://taosdata.feishu.cn/wiki/Zkb2wNkHDihARVkGHYEcbNhmnxb)
2. **测试结论**
   - 整体功能不容乐观，很多 FS 中描述基本功能无法通过
   - 最大问题是流计算输出结果不稳定，会被执行时机影响，非幂等操作。
3. **后续工作**
   - 被 BUG 阻塞测试的功能修复后继续完成测试
   - 修复的 BUG 把 CASE 打开
   - 研发无法复现的 BUG 协助复现及处理
