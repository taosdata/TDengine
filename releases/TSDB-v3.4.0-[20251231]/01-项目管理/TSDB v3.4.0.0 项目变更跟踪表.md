# TSDB v3.4.0.0 项目变更跟踪表

## 1. 修订记录

| 更新日期 | 更新人 | 主要修改内容 |
| --- | --- | --- |
| 2025-9-26 | 关胜亮 | 补充项目计划评审后的工作事项 |
| 2025-11-20 | 关胜亮、霍琳贺 | 更新 10-11 月中的变化情况 |
| 2025-12-18 | 关胜亮、霍琳贺 | 更新 11-12 月中的变化情况 |

## 20250926 工作范围变更

### 1.1 变更描述

1. 变更原因：部分任务已经承诺，增补到排期中
2. 变更类型：工作范围变更

### 1.2 变更内容

#### 1.2.1 新增工作项

1. [TS-6477](https://jira.taosdata.com:18080/browse/TS-6477) [交付][山东能源] tmq info 级别日志调整
2. [TD-38148](https://jira.taosdata.com:18080/browse/TD-38148) 降低reader触发与计算数据读取的资源消耗
3. [TD-38139](https://jira.taosdata.com:18080/browse/TD-38139) 流计算支持interp/percentile函数
4. [TD-38132](https://jira.taosdata.com:18080/browse/TD-38132) 增加 皮尔逊相关系数函数
5. [TD-38127](https://jira.taosdata.com:18080/browse/TD-38127) 流计算虚拟表触发计算性能优化
6. [TD-37942](https://jira.taosdata.com:18080/browse/TD-37942) 扩展状态窗口函数的定义
7. [TD-37767](https://jira.taosdata.com:18080/browse/TD-37767) TDgpt 提供关联分析函数和查询方式
8. [TD-37637](https://jira.taosdata.com:18080/browse/TD-37637) 分组计算不支持TDGPT 异常检测窗口
9. [TD-37059](https://jira.taosdata.com:18080/browse/TD-37059) partition by tag 支持字符串运算

#### 1.2.2 移除工作项

移除任务待定

### 1.3 变更影响分析

根据新增任务的工作量，后续调整移除部分优先级较低的工作任务。

## 20251120 工作范围变更

### 1.4 变更描述

1. 变更原因
   - 查询组优先处理 Nevados 客户的流计算性能优化，因此移除部分优先级较低的任务
   - 交付部门根据客户需求，新增的部分任务
2. 变更类型：工作范围变更

### 1.5 变更内容

#### 1.5.1 新增工作项

1. [TS-7596](https://jira.taosdata.com:18080/browse/TS-7596?src=confmacro) [交付][东航私有云] 授权服务可以按节点数或 CPU 数量限制授权总数
2. [TS-7591](https://jira.taosdata.com:18080/browse/TS-7591?src=confmacro) [交付][卡奥斯] 使用 Last 查询虚拟表时走对应子表的缓存
3. [TS-7567](https://jira.taosdata.com:18080/browse/TS-7567?src=confmacro) [交付][长庆油田] 消费端可控的 WAL 保留机制（订阅）
4. [TS-7540](https://jira.taosdata.com:18080/browse/TS-7540?src=confmacro) [售前] compact 命令支持 force 选项
5. [TS-7539](https://jira.taosdata.com:18080/browse/TS-7539?src=confmacro) [交付][长庆油田] 消费端可控的 WAL 保留机制（存储）
6. [TS-7399](https://jira.taosdata.com:18080/browse/TS-7399?src=confmacro) [售前] 提升 event_window 按 tbname 分组查询的效率
7. TS-7477 [海莱德自动化] taosX: 支持 OPC 冗余通信
8. TS-7476 [河北电力新一代调度项目] taosX 归档文件能够自定义文件占用的空间
9. TS-7470 [taosX] CSV数据写入任务，配置CSV地址添加 “保留已处理文件”的选项
10. TS-7467 websocket连接方式支持failover特性backport至3.3.6.*
11. TS-7466 [内部] python连接器补充执行sql超时配置参数
12. TS-7446 [内部测试]taosExplorer面板支持排序
13. TS-7429 [树根互联] taosx legacy_to_taos 支持 ns -> ms 的转存
14. TS-7422 [公共] 在引擎执行前可以按预先制定规则拦截指定模式查询
15. TS-7406 [售前] explorer 登录增加CAPTCHA功能
16. TS-7524 [河北电力新一代调度项目] taosX 归档的 archive 文件读取工具
17. TS-7536 [南网广州电力] DataIn 数据写入任务列表需显示task-id
18. TS-7537 [长江生态环保集团]OpenTSDB导入支持自定义列名标签名
19. TS-7570 [KingHistorian] 支持查询 TagGroupProperties
20. TS-7571 [安徽智质]taosx去掉log中opcda任务的点位打印信息
21. TS-7576 [中冶京诚]explorer web页面点击返回慢
22. TS-7583 [长江生态环保集团] OpenTSDB导入支持自定义子表名
23. TS-7585  [内部测试]taosx opentsdb-id.log 文件过大
24. TS-7586 [长沙卷烟厂]OPC UA数据采集提示异常
25. TS-7592 [EPDC] taosx TMQ 同步时遇表不存在错误可跳过目标库表存在查询
26. TS-7658 [双活] taosX 双活同步实现支持 WAL 保留机制
27. TS-7661 [公共] taosAdapter 拦截规则可监听配置文件修改并生效
28. TS-7667 [上海麦糖] taosx 在 tmq 模式下支持高版本(3.3.6.x) 向低版本(3.1.1.x)实时同步
29. TS-7684 [树根科技] taosX 数据迁移支持额外的 WHERE 条件
30. TS-7690 [海澜智云科技有限公司] taosX 内存回收机制改进

#### 1.5.2 移除工作项

1. [TS-7274](https://jira.taosdata.com:18080/browse/TS-7274?src=confmacro) [交付] 调用订阅服务密码错误返回含义不明确的错误信息“init tscObj failed”
2. [TS-7205](https://jira.taosdata.com:18080/browse/TS-7205?src=confmacro) [售前][陕西中烟] 支持按自然月定时计算
3. [TS-7204](https://jira.taosdata.com:18080/browse/TS-7204) [交付][海澜智云科技有限公司] 社区版在执行企业版专有功能时有报错提醒
4. [TS-7201](https://jira.taosdata.com:18080/browse/TS-7201?src=confmacro) [售前][陕西中烟] 分析产生的新属性，可以作为输入继续进行分析
5. [TS-7134](https://jira.taosdata.com:18080/browse/TS-7134?src=confmacro) [产品] 虚拟表继承
6. [TS-6864](https://jira.taosdata.com:18080/browse/TS-6864) [交付][东方电子] 支持配置多个监控目标地址
7. [TS-6429](https://jira.taosdata.com:18080/browse/TS-6429?src=confmacro) [交付] 禁止删除正在被订阅使用的子表的对应的超级表
8. [TS-6263](https://jira.taosdata.com:18080/browse/TS-6263?src=confmacro) [交付][中国电建集团华东勘测设计研究院] 副本变更不影响数据订阅
9. [TS-7239](https://jira.taosdata.com:18080/browse/TS-7239?src=confmacro) [产品] 联合查询设计文档（通过 Qnode 访问其他数据源）
10. [TS-5960](https://jira.taosdata.com:18080/browse/TS-5960?src=confmacro) [产品] 支持输出最外侧两个窗口的实际起止时间
11. [TS-4996](https://jira.taosdata.com:18080/browse/TS-4996) [交付] Audit 库可以记录客户端 IP 
12. TS-6962 [公共] taosx支持进行数据transformer的导入及导出
13. TS-6893 [公共] taosX支持导入Parquet格式
14. TS-6892 [亿纬锂能] JSON 数据解析能力提升--嵌套数据
15. TS-5973 [河北电力二期]taosx 支持通过transform写入到不同的超级表中
16. TS-5721 [深圳疆海] TD到TD订阅同步，需要支持Transform配置
17. TS-7476 [河北电力新一代调度项目] taosX 归档文件能够自定义文件占用的空间

### 1.6 变更影响分析

新增和移除的任务工作量相差不大，不影响本项目开发进展。

## 20251218 工作范围变更

### 1.7 变更描述

1. 变更原因
   - IDMP 产品新提出的新需求，例如流计算 true_for、Lag 函数等，工作量较大
   - 交付部门根据客户需求，新增的部分任务
2. 变更类型：工作范围变更

### 1.8 变更内容

#### 1.8.1 新增工作项

1. [6487609391](https://project.feishu.cn/taosdata_td/feature/detail/6487609391) 简版 Lag 函数
2. [6491072341](https://project.feishu.cn/taosdata_td/feature/detail/6491072341) 支持流计算批量删除语句
3. [6506048792](https://project.feishu.cn/taosdata_td/feature/detail/6506048792) [IDMP] 流计算的事件窗口仅在满足 true_for 条件时生成窗口开启通知
4. [TS-7018](https://jira.taosdata.com:18080/browse/TS-7018) [IDMP]面板中配置“实时预测”，选择某些算法会报错
5. [TD-38729](https://jira.taosdata.com:18080/browse/TD-38729) 优化：禁止创建虚拟表时源库精度不一致
6. [TD-38615](https://jira.taosdata.com:18080/browse/TD-38615) 优化：新增 walDeleteOnCorruption 参数，启用后备份损坏的 WAL 文件并继续启动数据库
7. [TD-38577](https://jira.taosdata.com:18080/browse/TD-38577) 优化：TDgpt 的数据补全算法支持任意采样间隔
8. [TD-38562](https://jira.taosdata.com:18080/browse/TD-38562) 优化：在数据补全算法中明确白噪音检查失败的错误信息
9. [TD-38533](https://jira.taosdata.com:18080/browse/TD-38533) 优化：改进 taosmqtt 的退出处理逻辑，实现更优雅的停机和资源释放
10. [TD-38501](https://jira.taosdata.com:18080/browse/TD-38501) 优化：支持在流计算中缓存标签等值条件的过滤结果
11. [TD-38450](https://jira.taosdata.com:18080/browse/TD-38450) 优化：不再重试已经超时的 RPC 消息
12. [TD-38256](https://jira.taosdata.com:18080/browse/TD-38256)优化：降低流计算在读取数据时的 CPU 使用比例
13. [TD-38255](https://jira.taosdata.com:18080/browse/TD-38255) 优化：提升存在历史数据的流计算启动速度
14. [TD-38211](https://jira.taosdata.com:18080/browse/TD-38211) 优化：支持修改 RSMA
15. [TD-38163](https://jira.taosdata.com:18080/browse/TD-38163) 优化：改进 mnode 启动时的日志记录和错误码显示
16. [TD-38004](https://jira.taosdata.com:18080/browse/TD-38004) 优化：提升 last_row + composite key 的查询性能
17. [TD-37993](https://jira.taosdata.com:18080/browse/TD-37993) event 窗口支持多个start condition
18. [TD-37309](https://jira.taosdata.com:18080/browse/TD-37309) 流计算支持增删子表和修改子表标签值
19. [TD-37063](https://jira.taosdata.com:18080/browse/TD-37063) 优化：流计算的内存占用
20. [TD-31688](https://jira.taosdata.com:18080/browse/TD-31688) taos-CLI 支持 3.3.5.0 ~ 3.3.8.0 版本中新增的命令词 TAB 补全
21. [6570501686](https://project.feishu.cn/taosdata_td/feature/detail/6570501686) 优化: stmt2 写入时新增布尔类型校验
22. [6551611200](https://project.feishu.cn/taosdata_td/feature/detail/6551611200) 优化：调整 stmt2 的日志，便于问题定位
23. [6497313576](https://project.feishu.cn/taosdata_td/feature/detail/6497313576) 优化：优化 RPC 通信过程中读写锁的使用逻辑

#### 1.8.2 移除工作项

1. [TS-7714](https://jira.taosdata.com:18080/browse/TS-7714?src=confmacro) [华润电力]  3.3.8 show streams 指令文档描述不对
2. [TS-7089](https://jira.taosdata.com:18080/browse/TS-7089?src=confmacro) [交付][深开鸿] blob 类型支持 cast、substr 函数
3. [TS-7088](https://jira.taosdata.com:18080/browse/TS-7088?src=confmacro) Display complete result of "show create database"
4. [TS-6668](https://jira.taosdata.com:18080/browse/TS-6668?src=confmacro) [交付][三峡云化集控] show queries 显示执行进度
5. [TS-6487](https://jira.taosdata.com:18080/browse/TS-6487?src=confmacro) [售前][东方电子CEP] 变化持续时长函数
6. [TS-6486](https://jira.taosdata.com:18080/browse/TS-6486?src=confmacro) [售前][东方电子CEP] 位变化次数函数
7. [TS-6485](https://jira.taosdata.com:18080/browse/TS-6485?src=confmacro) [售前][东方电子CEP] 值变化次数函数
8. [TS-6252](https://jira.taosdata.com:18080/browse/TS-6252?src=confmacro) [交付] Last 查询性能与 numOfVnodeQueryThreads 数量负相关
9. [TS-6194](https://jira.taosdata.com:18080/browse/TS-6194?src=confmacro) [交付][三峡新能源] fill prev 支持填充前一个非 null 值

### 1.9 变更影响分析

新增和移除的任务工作量相差不大，不影响本项目开发进展。
