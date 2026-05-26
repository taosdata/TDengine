# TSDB v3.4.2 项目变更跟踪表

## 1. 修订记录

| 更新日期 | 更新人 | 主要修改内容 |
| --- | --- | --- |
| 2026-4-13 | 关胜亮 | 新建 |
| 2026-5-26 | 关胜亮 | 第二次变更跟踪 |

## 2. 20260413 工作范围变更

### 2.1 变更描述

1. 变更原因
   - 与项目计划文档对比，飞书项目中新增了 56 个工作项（业务 9 个、IDMP 6 个、规划 29 个、海外 2 个、平台 10 个）
   - 业务侧新增主要来自交付和售前部门根据客户需求追加的功能，包括郑煤机、树根科技、赛力斯、三峡、中石油等客户场景
   - IDMP 侧新增来自流计算能力增强（多级子事件、标签列表达式、FFT）和数据备份支持虚拟表
   - 规划侧新增涵盖 Data Out 导出能力（Parquet/Kafka/MQTT）、连接器压缩和高可用、License Center、联邦查询演示版本、ExternalWindow 增强等
   - 海外侧新增 OPC UA Alarm & Events 支持
   - 平台侧新增 CI 流程优化、安全检测、Historian 安装包优化等
2. 变更类型：工作范围变更

### 2.2 变更内容

#### 2.2.1 新增工作项

**业务（9 项）**

1. [交付][郑煤机] trim 操作和 ssmigrate 事务之间冲突
2. [交付][树根科技] restore 命令支持指定 vgroup id 恢复
3. [赛力斯] taosx 支持创建 400+ 个字段的数据写入任务
4. [售前][中石油] 支持不限制国产操作系统和 CPU 的社区版
5. [售前][川威] TSDB Lite 的 Explorer 数据写入选项，仅保留支持的选项：OPC、MQTT 等
6. [社区] 订阅功能开源版本可以修改 topic 数量
7. [售前][上海电气中央研究院] 扩展 taosX 解析功能
8. [河北电力新一代调度项目] explorer 增加 taosx 命令行方式的 -T 参数
9. [交付][三峡] 优化高负载情况下选主行为（可行性方案）

**IDMP（6 项）**

1. [IDMP] 流计算支持多级子事件
2. [IDMP] 流计算的表达式需要支持标签列
3. [IDMP] 支持 FFT
4. [IDMP] 状态窗口需要支持多状态
5. [IDMP] TSDB 默认授权应不因 machine id 变化而 revoke 授权
6. [IDMP] 数据备份支持备份虚拟表和流计算

**规划（29 项）**

1. （子）查询数据来自 CSV 文件
2. external window 和 STMT 一起使用
3. jdbc 元数据订阅需求同步更新：新增修改表 19,20 类型，创建表虚拟子表信息
4. Rust 连接器支持新的 TMQ AlterType 19, 20
5. ExternalWindow FILL 支持
6. taosgen 所有命令行参数，支持环境变量
7. nodejs 支持 app 名称和 ip 设置
8. [测试] Explorer: UI 自动化测试
9. taosx 使用 taos-ui 请使用 submodule
10. [XNODE] Explorer 创建任务收到没有可用 XNODE 时，引导用户到 XNODE 创建页面
11. [产品] Data Out 支持导出到 Parquet
12. [产品] Data Out 支持导出到 Kafka
13. [产品] Data Out 支持导出到 MQTT
14. [产品] TSDB taosX/Explorer 数据导出
15. XNODE: Explorer 支持添加删除 XNODE
16. taosgen 支持查询 TDengine
17. [规划] License Center
18. taos shell 支持以 16 进制显示查询结果
19. ODBC Websocket 支持 stmt2
20. 支持压缩：ODBC 连接器（WS）
21. 支持压缩：Node.JS 连接器（WS）
22. C# 连接器性能压测工具开发
23. [产品] taosgen: CSV 导入功能优化
24. [规划] 联邦查询（演示版本）
25. [安全] 连接器安全开发 - 指南文档
26. [产品] taosx 高可用支持双活
27. c websocket 连接器增加三个函数
28. websocket 连接器增加两个函数
29. [安全] 修复 JDBC sonar 检查的错误和安全问题

**海外（2 项）**

1. OPC UA 支持 Alarm & Events
2. Data In 建超级表，数据列移至标签列自动删除默认标签列

**平台（10 项）**

1. cd：Historian 安装包优化
2. IDMP 用户手册需要支持版本
3. 失败 pr 和定时任务 action，能否推送飞书
4. 云服务前端项目 TDC-UI 的 CICD Github Action 流程包括测试和部署
5. [公共] 非 root 安装仅限企业版客户
6. [TSDB] CI 中添加预检测，避免使用内存不安全的函数
7. [Sail ADV] TDengine Sync Process Reliability Inside Docker
8. [内部] License 发放数据库备份及高可用
9. 改进 CI 用例执行时间
10. 为文档站点生成 single page html

#### 2.2.2 移除工作项

无

### 2.3 变更影响分析

项目计划阶段已为需求变更预留了一定的缓冲任务，新增的 56 个工作项中部分已在 v3.4.1 周期内提前启动或完成。对于剩余工作量，后续将通过适当加班和优先级调整来保障交付进度，整体工期风险可控。

## 3. 20260526 工作范围变更

### 3.1 变更描述

1. 变更原因
   - 与上次进度跟踪对比，飞书项目中新增了 46 个工作项（业务 7 个、IDMP 10 个、规划 19 个、海外 6 个、平台 4 个），移出了 41 个工作项（业务 3 个、IDMP 2 个、规划 30 个、海外 1 个、平台 5 个）
   - 业务侧新增主要来自交付部门根据客户需求追加的功能，包括杨凌美畅 asof join 优化、招商智科 WAL 修复、中石油数据恢复等场景；移出 3 项为分类调整（虚拟表引用不同精度的表移至 IDMP，华为 obs 和 insert into file 已取消）
   - IDMP 侧新增来自虚拟表能力增强（标签同步、引用虚拟表继承）、流计算增强（tag-ref、逐级汇总）、性能优化（大量超级表卡死）和工具支持（taosdump 导出 stream、REGEXP_EXTRACT）
   - 规划侧移出 30 项为范围精简（taosX 负载均衡/高可用系列、压缩工具类等移至后续版本或合并），新增 19 项涵盖安全脱敏、CPU 管理、Windows ASAN、用户手册重组、连接器 decimal 支持等
   - 海外侧新增 6 项主要为 taosgen 数据生成能力增强（schemaless、line protocol）和 Viega Store-and-Forward 支持；移出 1 项（Rolling full-backup 延后）
   - 平台侧新增自动收集日报、星网部署脚本、CD 迁移 GitLab、前端 CI 优化；移出 5 项为范围精简（非 root 安装、SBOM、Docker Sync 等移至后续版本）
2. 变更类型：工作范围变更

### 3.2 变更内容

#### 3.2.1 新增工作项

**业务（7 项）**

1. [交付][杨凌美畅] asof join 查询优化
2. [交付] 提升不停机场景下单 vnode 数据快速恢复能力
3. [交付] 支持监控 restore snapshot 的进度
4. [售前][招商智科] 断电宕机后确保 WAL 可用性（自动修复 WAL 文件）
5. 支持以COUNT_WINDOW(N, 1) 为触发的流进行重算
6. [规划] 数据修复工具支持以 copy 文件的方式恢复数据
7. [售前][一汽] state_window 支持逻辑运算符

**IDMP（10 项）**

1. [IDMP] 支持 REGEXP_EXTRACT 函数（正则表达式提取）
2. [售前][社区] 支持在函数中使用 Distinct 关键字
3. [IDMP] 源子表的标签值修改后能够同步更新虚拟子表的标签值
4. [售前][上海电气中央研究院] 虚拟表支持引用不同数据库精度的表
5. [规划] 虚拟表继承
6. taosdump 支持导出 stream 语句
7. [性能优化] taosd 存在大量的超级表时，taosd 偶发卡死
8. stream 支持tag-ref 和虚拟表引用
9. 目前的子事件的通知消息没有关联父事件和子事件的关系
10. [IDMP] 流计算支持基于超级表标签，实现叶子节点逐级汇总计算

**规划（19 项）**

1. TSDB 用户手册重新组织
2. Python UDF 插件内置编译
3. 减少可执行文件的大小
4. [Windows] 增加 ASAN 编译选项以检查内存写坏问题
5. [安全可靠测评] 列查询结果脱敏展示
6. [规划] 引擎侧 CPU 管理
7. [文档]修改 OPCUA 的用户手册
8. 优化tq 在meta 变更时的处理逻辑，修复超级表订阅 drop table时，meta获取不到的问题，优化tq文件架构
9. taosgen 工业场景模拟数据生成 SKILL
10. taosgen 输出日志的参数 -f 语义模糊，替换成表达能力更强的 -o
11. tsdb 仓库 ODBC 支持 Docker 中编译/本地跑测试用例
12. xnoded 日志优化
13. [Windows] 修改 Database 的 wal 选项默认值为强制刷新
14. [安全相关] 支持通过 taosk 命令修改加密的配置文件
15. 【流计算】虚拟表meta 变更， trigger 逻辑处理
16. taosAdapter 支持 stmt2 查询获取 fields 信息
17. C WebSocket 连接器支持 decimal 数据类型
18. stmt2 查询根据时间戳位数，自动判断精度
19. 0x80000125	Retry needed 错误码让应用重试，不合理，建议优化

**海外（6 项）**

1. [Viega] Feature Request: Add Store-and-Forward Support for taosx-agent OPC UA Ingestion
2. taosgen TDengine schemaless 方式写入支持提前建表
3. taosgen TDengine schemaless 写入支持可选的指定子表列名称
4. taosgen schemaless 行协议支持一定比例 NONE（数据值缺失） 语义
5. taosgen 变长数据类型 varchar/nchar 支持生成随机长度的值
6. taosgen需要能将生成的数据通过line protocol写入到tdengine或influxDB

**平台（4 项）**

1. 自动收集日报并记录到 Gitlab 仓库中
2. [ 星网 ] 配合云平台下发集群的自动化部署脚本
3. cd：cd 流程迁移至 gitlab 仓库
4. 前端 CI 可以支持加上参数支持一个用例失败就结束

#### 3.2.2 移出工作项

**业务（3 项）**

1. [交付][中冶京诚] insert into file 错误信息优化提升（6510958760）
2. [售前][上海电气中央研究院] 虚拟表支持引用不同数据库精度的表（6671971734，移至IDMP）
3. [交付][郑煤机] 共享存储支持配置华为 obs（6918805473）

**IDMP（2 项）**

1. [IDMP] 元数据更新支持事务（演示版本）（6661525203）
2. [IDMP] 流计算的表达式需要支持标签列（6932631879）

**规划（30 项）**

1. [规划] 流计算虚拟表触发计算性能优化（6490982243）
2. [规划] 虚拟表继承（6492554061，移至IDMP）
3. [规划] taosc API 在 stdout 不应有输出（6551339451）
4. taosX 高可用异常测试自动化（6635149921）
5. taosX：PostgreSQL 支持负载均衡（6646214948）
6. [产品] taosx 高可用支持双活（6646286429）
7. taosX: 数据迁移支持负载均衡（6646294822）
8. taosX: Oracle 支持负载均衡（6646341320）
9. taosx 高可用：支持同一任务下多个 agent 节点故障转移（6646475807）
10. taosx: MySQL 支持负载均衡（6646545784）
11. taosx：Agent 支持高可用（6646814636）
12. taosX: TDengine 数据订阅支持负载均衡（6646964092）
13. taosX: MSSQL 支持查询负载均衡（6647002003）
14. [测试] 连接器负载均衡测试（6658950467）
15. [测试] taosx xnode 稳定性测试（6659281143）
16. [规划] taosx 写入性能优化（6659287657）
17. [规划] taosx 性能指标可观测性优化（6659306656）
18. [规划] 流计算进一步降低资源消耗（6659796573）
19. [规划] 流计算多测点场景的性能优化（6659810600）
20. taosx: 新增数据源 开发指南（6659972286）
21. [规划] 流计算多个客户场景的性能提升（6660030137）
22. C# 连接器性能压测工具开发（6662904830）
23. 支持压缩：Node.JS 连接器 (WS)（6663246472）
24. [规划] License Center（6665336277）
25. taosgen 优化CSV文件读入的方式（6666686250）
26. taosgen 支持查询 TDengine（6666995190）
27. [安全] jwt token secret 变为动态发送给 xnoded（6669980852）
28. taosx 使用 taos-ui 请使用 submodule（6793466899）
29. [测试] Explorer: UI 自动化测试（6832951901）
30. taosgen 所有命令行参数，支持环境变量（6922162175）

**海外（1 项）**

1. [Shape Digital] Rolling full-backup（6861580736）

**平台（5 项）**

1. 为文档站点生成 single page html（6437586640）
2. 【售前】统一非root用户和root用户安装后启动taos cli的行为（6668113757）
3. [Sail ADV] TDengine Sync Process Reliability Inside Docker（6747206999）
4. [安全] 为发布版本生成 SBOM 文件（6776375399）
5. [公共] 非root安装仅限企业版客户（6836311627）

### 3.3 变更影响分析

本次变更净减少工作项数量（移出 41 项，新增 46 项），整体工作量变化不大。移出的 30 项规划任务主要为 taosX 负载均衡/高可用系列功能和连接器压缩工具类，已规划至后续版本统一交付。新增任务中多项已处于 Releasing/Done 状态，对当前工期影响可控。
