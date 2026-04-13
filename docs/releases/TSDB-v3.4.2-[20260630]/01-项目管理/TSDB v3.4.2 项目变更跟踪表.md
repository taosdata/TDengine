# TSDB v3.4.2 项目变更跟踪表

## 1. 修订记录

| 更新日期 | 更新人 | 主要修改内容 |
| --- | --- | --- |
| 2026-4-13 | 关胜亮 | 新建 |

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
