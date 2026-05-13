# 2026 Roadmap - 引擎

## HighLights

### 2026 Q1

1. 安全：安全功能开发、安全漏洞修复
2. 存储：数据修复工具、批量标签修改、动态调整数据缓存的 LRU
3. 查询：子查询、外部窗口、ANY/SOME/ALL/EXISTS 运算符、窗口及插值增强、Explain 和 ShowQueries 优化
4. 虚拟表：虚拟表查询性能优化、订阅虚拟表的元数据变更、虚拟表和源表的引用校验 
5. 流计算：按自然周/月/季/年触发、事件和状态窗口触发的 true_for 条件支持持续时间与条数、分组计算性能优化、虚拟超级表触发支持子表增删改

### 2026 Q2

1. 存储：多副本切主和节点恢复优化、数据缓存优化（如强制刷新、多列写入优化、指定列缓存）
2. 查询：联合查询演示版本，放宽窗口查询限制、时间窗口支持周/月/季/年、Interp 性能提升
3. 函数：窗口函数及 OVER 字句
4. 虚拟表：虚拟表继承、虚拟表引用虚拟表、虚拟超级表列名修改
5. 流计算：多个客户场景性能优化、多测点场景性能优化、历史计算性能优化、虚拟表触发性能优化
6. TDgpt：模型生命周期管理、预测性维护、分钟级时区

### 2026 Q3

1. 存储：元数据更新支持事务、提升数据拆分和移动的性能、数据重整不影响写入、磁盘限速
2. 查询：联合查询正式发布、复杂查询性能优化、标量相关子查询、累计窗口、SQL 测试工具
3. 函数：15 个客户需要的函数
4. 虚拟表：通过连续查询订阅虚拟表数据
5. 流计算：可维护性提升
6. 其他：TDgpt 支持 PCA、PLS、聚类分析

### 2026 Q4

1. 存储：全量与增量备份、TEXT 数据类型、不定长字符串数据类型、库名修改、列名修改
2. 查询：关联查询进一步增强、查询并行化、增加可观测指标
3. 函数：支持 MySQL 运算符及函数、UDF 函数框架重构
4. 其他：引擎的 CPU 和内存管控

## Details

| 项目 | 名称 | 迭代周期 | 分类 | 优先级 | Feature链接 |
| --- | --- | --- | --- | --- | --- |
| 「TSDB-20260331」共62个 |  |  |  |  |  |
| 「创新机动组」共5个 |  |  |  |  |  |
| Taos Dev | [安全可靠测评] 整理仓库代码以提高自研率 | TSDB-20260331 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6659850619?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6659850619?node=27331360) |
| Taos Dev | [安全可靠测评] 安全漏洞修复 | TSDB-20260331 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6659822076?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6659822076?node=27331360) |
| Taos Support | [售前] TSDB 适配 risc-v 硬件（外包，内部仅 Review 工作） | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6510735772?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6510735772?node=27331360) |
| Taos Support | [社区] TDgpt restful 驱动支持 Gunicorn | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6484950091?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6484950091?node=27331360) |
| Taos Support | [规划] 数据修复工具 | TSDB-20260331 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6469793274?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6469793274?node=27331360) |
| 「查询组」共24个 |  |  |  |  |  |
| Taos Dev | [交付] Explain analyais 可读性增强，清晰看出语句执行过程 | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6659962841?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6659962841?node=27331360) |
| Taos Dev | [IDMP] 给定的 SQL 集合提供易于定位的明确错误信息 | TSDB-20260331 | IDMP | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6659988199?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6659988199?node=27331360) |
| Taos Dev | [IDMP] 支持 ANY/SOME/ALL/EXISTS 运算符 | TSDB-20260331 | IDMP | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6659773695?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6659773695?node=27331360) |
| Taos Dev | [IDMP] 支持不带 FROM 的标量子查询 | TSDB-20260331 | IDMP | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6641525627?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6641525627?node=27331360) |
| Taos Dev | [规划] 子查询做主键过滤条件时的性能优化 | TSDB-20260331 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6617004723?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6617004723?node=27331360) |
| Taos Support | [交付] taosd 停服后 taosc 重连占用了太高的 cpu | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6598121270?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6598121270?node=27331360) |
| Taos Support | [北美][TASA] 虚拟表支持引用虚拟表 | TSDB-20260331 | IDMP | P1 | [https://project.feishu.cn/taosdata_td/feature/detail/6589380578?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6589380578?node=27331360) |
| Taos Support | [IDMP] 源表的 meta 自动更新到虚拟表和虚拟超级表（折衷方案） | TSDB-20260331 | IDMP | P1 | [https://project.feishu.cn/taosdata_td/feature/detail/6589101088?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6589101088?node=27331360) |
| Taos Dev | [规划] dataOrderLevel 使用及 table merge scan 有序传递 | TSDB-20260331 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6581335366?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6581335366?node=27331360) |
| Taos Support | [交付][深开鸿] blob 类型支持 cast、substr 函数 | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6567926427?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6567926427?node=27331360) |
| Taos Support | [交付][三峡云化集控] show queries 显示执行进度 | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6570714028?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6570714028?node=27331360) |
| Taos Dev | [规划] explain analyze 算子显示的执行时间 | TSDB-20260331 | Plan | P4 | [https://project.feishu.cn/taosdata_td/feature/detail/6548173402?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6548173402?node=27331360) |
| Taos Support | [售前][陕西中烟] 提升虚拟表按批次查询性能 | TSDB-20260331 | IDMP | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6548485194?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6548485194?node=27331360) |
| Taos Support | [规划] 外部窗口 | TSDB-20260331 | IDMP | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6550634959?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6550634959?node=27331360) |
| Taos Dev | [产品] 优化 explain 输出结果 | TSDB-20260331 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6545510969?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6545510969?node=27331360) |
| Taos Support | [北美][Nevados] Support subqueries "IN" clauses | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6539521758?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6539521758?node=27331360) |
| Taos Support | [售前] join/window join 支持基于选择函数结果集进行运算 | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6510828917?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6510828917?node=27331360) |
| Taos Support | [交付][海澜智云] 自动清理无效 sql 信息 | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6512028015?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6512028015?node=27331360) |
| Taos Support | [售前][硕橙科技] In 支持嵌套查询 | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6510267752?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6510267752?node=27331360) |
| Taos Support | [售前][三峡集团] 支持发生状态改变机组的原始数值查询 | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6510828810?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6510828810?node=27331360) |
| Taos Support | [售前][社区] Interval 窗口支持插值时间范围 | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6506145499?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6506145499?node=27331360) |
| Taos Support | [售前][红河卷烟厂] 事件窗口功能增强 | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6507054803?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6507054803?node=27331360) |
| Taos Support | [交付][三峡新能源] fill prev 支持填充前一个非 null 值 | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6506970855?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6506970855?node=27331360) |
| Taos Dev | [规划] 虚拟表查询性能优化 | TSDB-20260331 | IDMP | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6483450778?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6483450778?node=27331360) |
| 「存储组」共20个 |  |  |  |  |  |
| Taos Dev | [IDMP] 元数据更新支持事务（折衷方案） | TSDB-20260331 | IDMP | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6659965197?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6659965197?node=27331360) |
| Taos Dev | [等保四级] 支持敏感数据删除后的强制覆盖 | TSDB-20260331 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6641346408?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6641346408?node=27331360) |
| Taos Dev | [等保四级] 审计信息不经过 taoskeeper 记录 | TSDB-20260331 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6641435300?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6641435300?node=27331360) |
| Taos Dev | [等保四级] root 用户使用默认密码登录后，强制其修改密码 | TSDB-20260331 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6641469804?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6641469804?node=27331360) |
| Taos Dev | [安全可靠测评] 禁止篡改配置文件 | TSDB-20260331 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6640062620?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6640062620?node=27331360) |
| Taos Dev | [安全可靠测评] 支持从旧的加密集群升级到新的版本 | TSDB-20260331 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6640162570?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6640162570?node=27331360) |
| Taos Dev | [安全可靠测评] 完善存储加密功能 | TSDB-20260331 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6640296081?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6640296081?node=27331360) |
| Taos Dev | [安全可靠测评] 列权限生效 | TSDB-20260331 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6640315568?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6640315568?node=27331360) |
| Taos Dev | [安全可靠测评] 权限控制的兼容性处理 | TSDB-20260331 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6640076601?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6640076601?node=27331360) |
| Taos Dev | [安全可靠测评] 支持用户修改权限控制 | TSDB-20260331 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6640208544?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6640208544?node=27331360) |
| Taos Dev | [安全可靠测评] 完善权限控制 | TSDB-20260331 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6640186564?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6640186564?node=27331360) |
| Taos Dev | [安全可靠测评] create totp 时返回结果集 | TSDB-20260331 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6640162509?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6640162509?node=27331360) |
| Taos Dev | [安全可靠测评] 增加 token 相关的通知机制 | TSDB-20260331 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6640223025?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6640223025?node=27331360) |
| Taos Support | [内部] TDlite 授权支持 taosX 部分连接器 | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6628216389?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6628216389?node=27331360) |
| Taos Support | [交付][河北电力] 一次性批量修改多个子表的多个 tag 值功能 | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6594391614?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6594391614?node=27331360) |
| Taos Support | [交付][天合富家] 动态调整 LRU 分片数量以提高 Last 查询性能 | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6568211421?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6568211421?node=27331360) |
| Taos Support | [交付] Audit 库可以记录客户端 IP | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6511301953?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6511301953?node=27331360) |
| Taos Support | [交付][东方电子] 支持配置多个监控目标地址 | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6507093771?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6507093771?node=27331360) |
| Taos Support | [交付][三峡]优化高负载情况下选主行为（尽量完成） | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6507042141?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6507042141?node=27331360) |
| Taos Support | [IDMP] 批量更新、增加和删除虚拟子表的标签和标签值 | TSDB-20260331 | IDMP | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6491345559?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6491345559?node=27331360) |
| 「分析组」共13个 |  |  |  |  |  |
| Taos Dev | [安全可靠测评] 数据订阅支持的 token登录 | TSDB-20260331 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6659792966?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6659792966?node=27331360) |
| Taos Dev | 流计算支持虚拟超级表聚合查询优化 | TSDB-20260331 | IDMP | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6619755141?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6619755141?node=27331360) |
| Taos Support | [售前][一汽红旗] 流计算中能够支持子查询过滤条件 | TSDB-20260331 | IDMP | P1 | [https://project.feishu.cn/taosdata_td/feature/detail/6598056767?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6598056767?node=27331360) |
| Taos Support | [售前][新奥数能] 实现 stmt 查询结果集和 stmt 解耦 | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6597880825?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6597880825?node=27331360) |
| Taos Support | [售前][瑞幸咖啡] 数据订阅支持虚拟表的元数据变更 | TSDB-20260331 | IDMP | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6593807450?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6593807450?node=27331360) |
| Taos Support | [售前][广汽] 流计算事件窗口，满足条件除时长外，还增加记录条数 | TSDB-20260331 | IDMP | P1 | [https://project.feishu.cn/taosdata_td/feature/detail/6589462594?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6589462594?node=27331360) |
| Taos Support | [IDMP] 流计算在源子表/虚拟子表长时间没有新数据写入时，也能提供发送通知的功能 | TSDB-20260331 | IDMP | P1 | [https://project.feishu.cn/taosdata_td/feature/detail/6572489317?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6572489317?node=27331360) |
| Taos Dev | [规划] 流计算多分组批量计算 | TSDB-20260331 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6491136498?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6491136498?node=27331360) |
| Taos Dev | 流计算虚拟超级表触发支持新增、删除子表、子表 tag 值修改、修改列映射关系 | TSDB-20260331 | IDMP | P2 | [https://project.feishu.cn/taosdata_td/feature/detail/6491267649?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6491267649?node=27331360) |
| Taos Support | [售前][陕西中烟] 支持按自然周、月、季、年的定时计算 | TSDB-20260331 | IDMP | P1 | [https://project.feishu.cn/taosdata_td/feature/detail/6490755304?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6490755304?node=27331360) |
| Taos Support | [售前][陕西中烟] 分析产生的新属性，可以作为输入继续进行分析 | TSDB-20260331 | IDMP | P1 | [https://project.feishu.cn/taosdata_td/feature/detail/6490870739?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6490870739?node=27331360) |
| Taos Support | [交付] 调用订阅服务密码错误返回含义不明确的错误信息 | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6490634781?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6490634781?node=27331360) |
| Taos Support | [产品] taos_register_instance 接口使用 firstep 和 secondep | TSDB-20260331 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6487556383?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6487556383?node=27331360) |
| 「TSDB-20260630」共55个 |  |  |  |  |  |
| 「创新机动组」共2个 |  |  |  |  |  |
| Taos Dev | [规划] TDgpt 预测性维护 | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6660040972?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6660040972?node=27331360) |
| Taos Dev | [规划] 日志分析工具 | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6658971933?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6658971933?node=27331360) |
| 「查询组」共12个 |  |  |  |  |  |
| Taos Dev | 支持季度时间单位 | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6474961364?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6474961364?node=27331360) |
| Taos Dev | [规划] 查询中支持按自然周、月、季、年 | TSDB-20260630 | IDMP | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6661700117?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6661700117?node=27331360) |
| Taos Dev | [IDMP] 放宽窗口查询限制（不仅是聚合） | TSDB-20260630 | IDMP | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6659773700?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6659773700?node=27331360) |
| Taos Dev | [性能]相同标量子查询的合并处理 | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6638877516?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6638877516?node=27331360) |
| Taos Dev | [性能]标量子查询结果常量计算为空时的性能优化 | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6638636215?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6638636215?node=27331360) |
| Taos Dev | [IDMP][join] 支持 hash join | TSDB-20260630 | IDMP | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6596178099?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6596178099?node=27331360) |
| Taos Support | [规划] 优化需要 TS 主键列函数的执行条件 | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6536374390?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6536374390?node=27331360) |
| Taos Support | [规划] 子查询涉及主键列排序场景的性能优化 | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6544826545?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6544826545?node=27331360) |
| Taos Support | [售前][Nevados] 大幅放宽关联查询限制 | TSDB-20260630 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6534756767?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6534756767?node=27331360) |
| Taos Support | [售前][南网数研院][南瑞电网] 提升 Interp 查询性能 | TSDB-20260630 | FromTX | P4 | [https://project.feishu.cn/taosdata_td/feature/detail/6514083018?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6514083018?node=27331360) |
| Taos Support | [售前][上科信息] 分组查询 partition by 支持组内排序 | TSDB-20260630 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6510119993?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6510119993?node=27331360) |
| Taos Support | [规划] 虚拟表继承 | TSDB-20260630 | IDMP | P2 | [https://project.feishu.cn/taosdata_td/feature/detail/6492554061?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6492554061?node=27331360) |
| 「存储组」共16个 |  |  |  |  |  |
| Taos Dev | [IDMP] 元数据更新支持事务（演示版本） | TSDB-20260630 | IDMP | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6661525203?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6661525203?node=27331360) |
| Taos Dev | [规划] 完善数据修复工具 | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6661410964?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6661410964?node=27331360) |
| Taos Dev | [规划] 缩短离线节点恢复的时间（不阻塞写入） | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6659897268?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6659897268?node=27331360) |
| Taos Dev | [规划] 缩短多副本切主后集群恢复时间 | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6660003972?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6660003972?node=27331360) |
| Taos Support | [售前][南网 CEP] show local/dnode variables增加一参数列：是否需要重启生效、当前参数未生效 | TSDB-20260630 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6599966995?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6599966995?node=27331360) |
| Taos Support | [交付][天合富家] 增加缓存强制刷新功能 | TSDB-20260630 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6574020760?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6574020760?node=27331360) |
| Taos Support | [IDMP] 删除数据库不加 force 应该告知客户真实原因 | TSDB-20260630 | IDMP | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6572940279?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6572940279?node=27331360) |
| Taos Support | [规划] lastrow 并发查询性能优化 | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6570698058?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6570698058?node=27331360) |
| Taos Dev | [IDMP] 支持修改虚拟超级表列名 | TSDB-20260630 | IDMP | P4 | [https://project.feishu.cn/taosdata_td/feature/detail/6570504710?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6570504710?node=27331360) |
| Taos Support | [售前][神东集团]单副本变三副本支持共享存储 | TSDB-20260630 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6511323203?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6511323203?node=27331360) |
| Taos Support | [交付] show table distribute 格式化显示，便于过滤 | TSDB-20260630 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6506025858?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6506025858?node=27331360) |
| Taos Support | [交付][南网储能-拾贝云] 节点启动过程中应用需要正常使用不报错 | TSDB-20260630 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6506113427?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6506113427?node=27331360) |
| Taos Support | [交付][海澜智云] 社区版在执行企业版专有功能时有报错提醒 | TSDB-20260630 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6507051705?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6507051705?node=27331360) |
| Taos Support | [交付] 支持指定列进行最新数据缓存 | TSDB-20260630 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6491198599?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6491198599?node=27331360) |
| Taos Support | [交付][爱动] 支持分钟级别的时区 | TSDB-20260630 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6491037879?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6491037879?node=27331360) |
| Taos Dev | [交付] 提升开启 Last 缓存时多列场景的写入性能 | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6490743340?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6490743340?node=27331360) |
| 「分析组」共25个 |  |  |  |  |  |
| Taos Dev | [规划] 流计算进一步降低资源消耗 | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6659796573?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6659796573?node=27331360) |
| Taos Dev | [规划] 流计算多测点场景的性能优化 | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6659810600?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6659810600?node=27331360) |
| Taos Dev | [规划] 流计算多个客户场景的性能提升 | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6660030137?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6660030137?node=27331360) |
| Taos Dev | [规划] 流计算支持新增的查询语法（例如 JOIN 等） | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6659850080?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6659850080?node=27331360) |
| Taos Dev | [流计算] vnode 切主reader tablelist 更新逻辑（虚拟表和非虚拟表） | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6616784073?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6616784073?node=27331360) |
| Taos Support | [售前][一汽红旗] 事件窗口的结束条件也能够设置持续时间判断 | TSDB-20260630 | IDMP | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6592836563?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6592836563?node=27331360) |
| Taos Support | [交付][河北电力]优化频繁 use db 导致 mnode read 线程压力过大 | TSDB-20260630 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6589436029?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6589436029?node=27331360) |
| Taos Support | [产品] show streams 支持不指定 dbname | TSDB-20260630 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6579574893?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6579574893?node=27331360) |
| Taos Support | [规划] taosc API 在 stdout 不应有输出 | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6551339451?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6551339451?node=27331360) |
| Taos Dev | [IDMP] 支持窗口函数和 OVER 子句 | TSDB-20260630 | IDMP | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6549502576?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6549502576?node=27331360) |
| Taos Support | [交付][中冶京诚] insert into file 错误信息优化提升 | TSDB-20260630 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6510958760?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6510958760?node=27331360) |
| Taos Support | [三峡集团]需要支持ROW_NUMBER() OVER()函数 | TSDB-20260630 | FromTX | P4 | [https://project.feishu.cn/taosdata_td/feature/detail/6513771567?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6513771567?node=27331360) |
| Taos Support | [交付][拾贝云] Greatest/Least 与 MySQL 对齐，支持忽略 NULL | TSDB-20260630 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6511294180?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6511294180?node=27331360) |
| Taos Support | [交付] 查询函数 Sleep(duration) 用于超时问题模拟 | TSDB-20260630 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6507136288?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6507136288?node=27331360) |
| Taos Support | [售前][陕西中烟] 缺少排名函数，如rank() | TSDB-20260630 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6507156244?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6507156244?node=27331360) |
| Taos Dev | [规划] 流计算支持多分组同时重算 | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6507000387?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6507000387?node=27331360) |
| Taos Dev | 允许失败时，流的通知发送改成异步进行 | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6503261141?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6503261141?node=27331360) |
| Taos Dev | [规划] 流计算 checkpoint 各类失败问题处理 | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6490739879?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6490739879?node=27331360) |
| Taos Dev | [规划] 流计算删除 snode 时的 checkpoint 同步与校验 | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6491292920?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6491292920?node=27331360) |
| Taos Dev | [规划] 流计算各个 task 的快速退出处理（长时间处理快速结束、kill task） | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6490638809?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6490638809?node=27331360) |
| Taos Dev | [规划] 流计算历史计算性能优化 | TSDB-20260630 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6490635370?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6490635370?node=27331360) |
| Taos Dev | 流计算虚拟表触发计算性能优化 | TSDB-20260630 | IDMP | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6490982243?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6490982243?node=27331360) |
| Taos Support | [交付] 禁止删除正在被订阅使用的子表的对应的超级表 | TSDB-20260630 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6491115004?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6491115004?node=27331360) |
| Taos Support | [交付][中国电建] 副本变更不影响数据订阅 | TSDB-20260630 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6490727766?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6490727766?node=27331360) |
| Taos Support | [交付] 支持 create stable as select * from stable 语法 | TSDB-20260630 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6490717238?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6490717238?node=27331360) |
| 「TSDB-20260930」共33个 |  |  |  |  |  |
| 「创新机动组」共4个 |  |  |  |  |  |
| Taos Dev | [产品] TDgpt 支持 PCA、PLS、聚类分析 | TSDB-20260930 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6568541194?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6568541194?node=27331360) |
| Taos Dev | TDgpt 提供针对时序基础模型的微调功能 | TSDB-20260930 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6568496620?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6568496620?node=27331360) |
| Taos Dev | TDgpt 优化训练自有的时序基础模型 | TSDB-20260930 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6568420388?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6568420388?node=27331360) |
| Taos Dev | TDgpt 支持时序数据分类模型 | TSDB-20260930 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6568689822?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6568689822?node=27331360) |
| 「查询组」共10个 |  |  |  |  |  |
| Taos Dev | [规划] 竞品及 TSBS 相关查询指标优化 | TSDB-20260930 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6660035139?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6660035139?node=27331360) |
| Taos Dev | [规划] 复杂查询性能优化 | TSDB-20260930 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6659904486?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6659904486?node=27331360) |
| Taos Dev | [规划] TSDB SQL 模糊测试 | TSDB-20260930 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6659190651?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6659190651?node=27331360) |
| Taos Dev | tbname tag is slower than the other tag created with index | TSDB-20260930 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6554504944?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6554504944?node=27331360) |
| Taos Dev | SQL 测试覆盖工具 | TSDB-20260930 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6542786686?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6542786686?node=27331360) |
| Taos Dev | [having]HAVING 可以独立于GROUP BY/PARTITION BY使用 | TSDB-20260930 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6534756930?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6534756930?node=27331360) |
| Taos Dev | [子查询]支持标量相关子查询 | TSDB-20260930 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6534632447?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6534632447?node=27331360) |
| Taos Dev | [partition]内存页个数不足导致频繁读写磁盘 | TSDB-20260930 | Plan | P2 | [https://project.feishu.cn/taosdata_td/feature/detail/6534804828?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6534804828?node=27331360) |
| Taos Dev | [join]支持多表JOIN和嵌套JOIN | TSDB-20260930 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6533192882?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6533192882?node=27331360) |
| Taos Dev | [state window]能够支持多group table scan并发读返回的数据 | TSDB-20260930 | Plan | P4 | [https://project.feishu.cn/taosdata_td/feature/detail/6495478321?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6495478321?node=27331360) |
| 「存储组」共7个 |  |  |  |  |  |
| Taos Dev | [IDMP] 元数据更新支持事务 | TSDB-20260930 | IDMP | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6661374272?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6661374272?node=27331360) |
| Taos Dev | [规划] 提高数据 Redistribute 的性能（不阻塞写入） | TSDB-20260930 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6659905696?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6659905696?node=27331360) |
| Taos Dev | [规划] 提高数据 SPLIT 的性能（不阻塞写入） | TSDB-20260930 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6659797100?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6659797100?node=27331360) |
| Taos Dev | [规划] 支持磁盘文件移动时中的限速（例如多级存储迁移） | TSDB-20260930 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6660077086?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6660077086?node=27331360) |
| Taos Dev | [社区版 OS] 社区版 OS 的支持检查与更新 | TSDB-20260930 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6551501198?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6551501198?node=27331360) |
| Taos Support | [新奥新智] Compact 时不影响数据写入及查询 | TSDB-20260930 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6511355434?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6511355434?node=27331360) |
| Taos Support | [规划] 全量与增量备份（详细设计） | TSDB-20260930 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6511905550?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6511905550?node=27331360) |
| 「分析组」共12个 |  |  |  |  |  |
| Taos Dev | [规划] 流结果表中增加一个选项，可以把流计算完成的时间写入 | TSDB-20260930 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6574669459?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6574669459?node=27331360) |
| Taos Support | [IDMP] 支持累计窗口 | TSDB-20260930 | IDMP | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6550405040?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6550405040?node=27331360) |
| Taos Support | [售前][社区] CSUM 函数优化 | TSDB-20260930 | FromTX | P4 | [https://project.feishu.cn/taosdata_td/feature/detail/6511923608?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6511923608?node=27331360) |
| Taos Support | [售前][社区] 支持在函数中使用 Distinct 关键字 | TSDB-20260930 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6511919698?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6511919698?node=27331360) |
| Taos Dev | [规划] 流计算处理有关函数返回值的 CI 问题 | TSDB-20260930 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6491216977?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6491216977?node=27331360) |
| Taos Dev | [规划] 流计算可维护性提升 | TSDB-20260930 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6490716382?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6490716382?node=27331360) |
| Taos Support | [售前][卡奥斯] 数据订阅支持连续查询（订阅虚拟表的时序数据） | TSDB-20260930 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6491169485?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6491169485?node=27331360) |
| Taos Support | [售前][江苏国信] 支持 9 个火电时序常用函数 | TSDB-20260930 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6490631285?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6490631285?node=27331360) |
| Taos Support | [售前][蓝卓] 支持积分、积分平均、连续方差函数 | TSDB-20260930 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6491141359?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6491141359?node=27331360) |
| Taos Support | [售前][东方电子CEP] 值变化次数函数 | TSDB-20260930 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6491186422?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6491186422?node=27331360) |
| Taos Support | [售前][东方电子CEP] 位变化次数函数 | TSDB-20260930 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6491070857?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6491070857?node=27331360) |
| Taos Support | [售前][东方电子CEP] 变化持续时长函数 | TSDB-20260930 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6490696232?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6490696232?node=27331360) |
| 「TSDB-20261231」共28个 |  |  |  |  |  |
| 「创新机动组」共2个 |  |  |  |  |  |
| Taos Dev | [规划] 引擎侧内存管理 | TSDB-20261231 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6659879819?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6659879819?node=27331360) |
| Taos Dev | [规划] 引擎侧 CPU 管理 | TSDB-20261231 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6659794715?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6659794715?node=27331360) |
| 「查询组」共8个 |  |  |  |  |  |
| Taos Dev | [规划] 查询并行化 | TSDB-20261231 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6660023965?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6660023965?node=27331360) |
| Taos Dev | [性能]支持多个非相关子查询并发调度执行 | TSDB-20261231 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6659831352?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6659831352?node=27331360) |
| Taos Support | [售前][广州电力调度]嵌套查询的外层查询性能优化 | TSDB-20261231 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6580190112?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6580190112?node=27331360) |
| Taos Dev | [产品] 通过 SQL 查看长查询的当前状态 | TSDB-20261231 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6544786148?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6544786148?node=27331360) |
| Taos Dev | 增加强制删除查询任务的方法 | TSDB-20261231 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6544715732?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6544715732?node=27331360) |
| Taos Dev | disctinct 主键列性能优化 | TSDB-20261231 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6541142068?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6541142068?node=27331360) |
| Taos Support | [售前] 提高子表的聚合查询性能 | TSDB-20261231 | FromTX | P4 | [https://project.feishu.cn/taosdata_td/feature/detail/6513159461?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6513159461?node=27331360) |
| Taos Dev | 超级表按主键时间戳排序性能差 | TSDB-20261231 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6492248250?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6492248250?node=27331360) |
| 「存储组」共10个 |  |  |  |  |  |
| Taos Dev | [规划] 全量与增量备份 | TSDB-20261231 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6661649140?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6661649140?node=27331360) |
| Taos Support | [规划] 新增数据类型 TEXT | TSDB-20261231 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6589163599?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6589163599?node=27331360) |
| Taos Support | [售前] display complete result of "show create table" | TSDB-20261231 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6589056603?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6589056603?node=27331360) |
| Taos Dev | [TagIndex] drop all tag index but disk space become large | TSDB-20261231 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6554432068?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6554432068?node=27331360) |
| Taos Dev | 配置文件行为优化 | TSDB-20261231 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6551407269?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6551407269?node=27331360) |
| Taos Support | [东方电子] 新增不定长字符串数据类型 | TSDB-20261231 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6509688788?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6509688788?node=27331360) |
| Taos Support | [交付][南网储能-拾贝云] 查询超级表占用的磁盘空间 | TSDB-20261231 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6511118472?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6511118472?node=27331360) |
| Taos Support | [交付][新奥新智] 支持数据列改名 | TSDB-20261231 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6509715486?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6509715486?node=27331360) |
| Taos Support | [售前][交付] 数据库改名 | TSDB-20261231 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6506119319?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6506119319?node=27331360) |
| Taos Support | [售前] 丰富磁盘监控指标：请求频率、繁忙度 | TSDB-20261231 | FromTX | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6507142667?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6507142667?node=27331360) |
| 「分析组」共8个 |  |  |  |  |  |
| Taos Dev | [规划] UDF 函数框架重构 | TSDB-20261231 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6659859947?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6659859947?node=27331360) |
| Taos Dev | [规划] like 查询支持 * 和? | TSDB-20261231 | Plan | P5 | [https://project.feishu.cn/taosdata_td/feature/detail/6554627841?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6554627841?node=27331360) |
| Taos Dev | [产品] 支持 MySQL 运算符（10 个） | TSDB-20261231 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6547876231?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6547876231?node=27331360) |
| Taos Dev | [产品] 支持 MySQL 的日期和时间函数（第二期 27 个） | TSDB-20261231 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6549916259?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6549916259?node=27331360) |
| Taos Dev | [产品] 支持 MySQL 的位函数和运算符（10个） | TSDB-20261231 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6549155721?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6549155721?node=27331360) |
| Taos Dev | [产品] 支持 MySQL 的字符串函数和运算符（28个） | TSDB-20261231 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6547651403?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6547651403?node=27331360) |
| Taos Dev | [产品] 支持 MySQL 的日期和时间函数（第一期 27 个） | TSDB-20261231 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6547961938?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6547961938?node=27331360) |
| Taos Support | [产品] 支持 MySQL 的数值函数和运算符（约20个） | TSDB-20261231 | Plan | P3 | [https://project.feishu.cn/taosdata_td/feature/detail/6491090816?node=27331360](https://project.feishu.cn/taosdata_td/feature/detail/6491090816?node=27331360) |
