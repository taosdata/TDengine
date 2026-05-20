# 20260520 JDBC ADAPTER HA 设计评审记录

## 1. 评审信息

1. 评审目的：评估 "JDBC Connector adapter HA FS" 设计的合理性、安全性、性能、兼容性及可维护性
2. 评审文档：[JDBC ADAPTER HA FS](../../../05-设计文档/JDBC%20ADAPTER%20HA%20FS.md)
3. 会议主持：关胜亮
4. 会议人员：关胜亮、霍琳贺、佘彦杰、杨志宇、肖波、谭雪峰
5. 会议时间：2026-05-20 09:30 - 09:40
6. 会议形式：线下
7. 会议地点：taosX
8. 会议记录人：关胜亮

## 2. 评审记录

评审团队对设计文档（JDBC Connector adapter HA FS）进行了全面审查，认为整体设计贴合 JDBC 客户端 adapter 集群高可用与负载均衡需求、逻辑严谨、可落地性强，具体评审意见如下：
1. 设计目标清晰精准，核心痛点定位明确，紧扣用户仅配置一个种子 adapter 地址时无法发现同集群内其他实例、种子节点下线后无法自动切换的痛点，明确核心目标为 JDBC WebSocket SQL 和 TMQ 通过 `adapterHA=true` 配置项启用 adapter HA 后利用 adapter 返回的 `list_instances` 实现动态实例发现、endpoint 列表扩展、已知 cluster 登记及后续连接负载均衡，并结合既有 `enableAutoReconnect` 能力实现种子节点下线后的故障切换，目标聚焦、指引明确。
2. 功能设计全面细致，可落地性强，覆盖核心业务场景：配置项设计简洁（`adapterHA` 布尔类型默认 false 适用 SQL 和 TMQ 两条路径）、SQL 连接行为完整（6 条处理规则覆盖禁用/adapter 调用失败/响应缺失或空/重复项/非法地址/新增 WSClient 并写回 ConnectionParam.endpoints）、TMQ 订阅行为复用 SQL 的 endpoint 合并逻辑且明确 `adapterHA` 不透传到 TMQ config、已知 cluster 扩展设计清晰（expandCluster 保守不 shrink/expandEndpointsIfKnown 使后续种子连接建连前获得完整列表/继续按最小连接数算法选择目标节点）、故障切换依赖既有自动重连能力且提供推荐配置参数组合、错误处理覆盖 6 类场景行为明确、使用场景覆盖 SQL 写入切换/TMQ 订阅切换/后续连接负载均衡/旧版本灰度四类典型场景、常见错误与排查覆盖 6 类症状含排查建议，设计闭环完整。
3. 设计文档结构规范，版本与修订记录清晰：文档包含一版修订记录（1.0 初稿）、背景（4 项优化目标）、定义（5 项术语）、行为说明（配置项/SQL 连接/TMQ 订阅/已知 cluster 扩展/故障切换/错误处理六大子节）、性能（5 项分析）、安全（4 项保障）、兼容性（5 项兼容声明）、运维（4 项建议）、使用场景（4 项）、约束和限制（5 项）、常见错误和排查（6 类场景）、可观测性（4 项）、安装和卸载、文档（3 项修改点）、参考文档（10 个源码文件）、附录（含关键流程文本图）共 16 大章节，层次分明、约束与限制界定清晰，逻辑清晰、无歧义，符合 TDengine 设计文档规范要求。
4. 安全性、兼容性与性能考虑周全，风险可控：安全方面不新增认证方式、不改变密码/Token/白名单/SSL 校验、`list_instances` 仅含实例地址不含用户数据或凭据、默认关闭仅显式启用时才请求、日志不记录敏感信息；兼容性方面 `adapterHA` 默认 false 旧应用不受影响、旧 adapter 忽略未知字段或不返回时连接仍成功、`adapterHA` 被识别为 JDBC 内部配置不进入 TMQ config、配置 `slaveClusterHost` 时不执行已知 cluster 扩展避免与主从机制冲突；性能方面默认路径无额外开销、启用后仅在连接/订阅成功路径做线性复杂度的 endpoint 合并、查询/写入/poll/fetch/commit 正常执行路径不调用 adapter 实例查询接口。

## 3. 评审结论

设计文档整体设计合理、逻辑清晰，功能覆盖全面，JDBC Connector adapter HA 通过 `adapterHA` 配置项启用后利用 adapter `list_instances` 响应实现动态实例发现和 endpoint 扩展，结合 `RebalanceManager` 的 expandCluster/expandEndpointsIfKnown 能力使后续连接自动获得完整集群列表并按最小连接数负载均衡，配合既有自动重连能力实现种子节点故障切换，性能、安全、兼容性设计符合系统规范，精准解决了用户仅配置种子 adapter 时无法感知集群拓扑和故障切换的核心痛点。

## 4. 后续行动项

无
