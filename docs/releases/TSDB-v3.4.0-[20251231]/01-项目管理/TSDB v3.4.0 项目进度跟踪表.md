# TSDB v3.4.0 项目进度跟踪表

## 1. 修订记录

| 更新日期 | 更新人 | 主要修改内容 |
| --- | --- | --- |
| 2025-9-24 | 关胜亮 | 编写工作分解结构和风险管理表 |
| 2025-9-26 | 关胜亮 | 按 “[评审记录](https://taosdata.feishu.cn/wiki/IqNKwb7fDiWj43kVZYScu5qSnMT)” 更新工作项 |
| 2025-10-20 | 关胜亮、霍琳贺、王旭 | 更新各任务的工作进度 |
| 2025-12-24 | 关胜亮、霍琳贺、肖波 | 更新各任务的工作进度 |
| 2026-01-09 | 关胜亮、霍琳贺、肖波 | 更新各任务的进行情况 |
| 2026-01-28 | 关胜亮、霍琳贺、肖波 | 更新各任务的进行情况 |

## 2. 项目进度概览

1. 整体进度：*（未开始/正常/有风险/严重滞后/已完成）*
   - 引擎：**正常**
   - 工具：**正常**
   - 平台：**正常**
2. 范围状态：
   - 引擎：新增任务  35  个，移出任务  20  个
   - 工具：新增任务  25  个，移出任务  9  个
   - 平台：新增任务  13  个，移出任务  0  个
3. 主要风险：
   - 引擎：无
   - 工具：
      - 安全可靠性提升需求经评审后，taosAdapter、连接器、Explorer 均需进行各自模块的功能实现；以及新增 Pulsar 数据源、OPC 多节点支持任务；主要影响 taosX 高可用的实现进度，有一定风险；
   - 平台：无

## 3. 工作分解结构与进度跟踪

### 3.1 亮点功能

#### 3.1.1 引擎

1. 安全可靠性提升：包括身份鉴别、访问控制、存储安全、传输安全、安全函数、安全审计、加密算法
2. 虚拟表：超级表、窗口查询等场景的性能提升
3. 流计算：支持按自然月触发、流计算支持子事件、流计算性能提升
4. TDgpt：支持相关性分析

#### 3.1.2 工具

1. 安全可靠性提升，包括各组件之间的数据完整性和加密认证、解决明文密码问题、OAUTH 2.0 API 客户端认证、Explorer OAuth 2.0 (OpenID Connect) 支持（SSO）、taosX 分布式高可用
2. taosgen 开源（完成写入 Kafka 支持并完成 SDK 文档，开放写入其他目标端）
3. 新增数据源，包括 KingHistorian、Pulsar 等

#### 3.1.3 平台

1. 内网环境安全防护，满足安可的最低要求
2. 缩短 PR 的运行时间至当前的三分之一
3. 云服务告警的治理，将常见的消除告警的操作自动化
4. 各产品安装包的优化，包括安装、卸载脚本的优化、完善非 root 用户安装和自定义路径安装、减少安装包的构建次数
5. 各产品 Docker 镜像的优化，包括优化 Dockerfile 以减少镜像大小、统一 base image，包括多架构的实现由 manifest 迁移至 buildx

### 3.2 中国业务

#### 3.2.1 引擎

| 序号 | 关键字 | 标题 | 需求 | 设计 | 开发 | 测试 | 变更 | 进度说明 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | [6487609391](https://project.feishu.cn/taosdata_td/feature/detail/6487609391) | 简版 Lag 函数 | - | - | - | - | 新增 | 已完成 |
| 2 | [6491072341](https://project.feishu.cn/taosdata_td/feature/detail/6491072341) | 支持流计算批量删除语句 | - | - | - | - | 新增 | 已完成 |
| 3 | [6506048792](https://project.feishu.cn/taosdata_td/feature/detail/6506048792) | [IDMP] 流计算的事件窗口仅在满足 true_for 条件时生成窗口开启通知 | - | - | - | - | 新增 | 已完成 |
| 4 | [TS-7714](https://jira.taosdata.com:18080/browse/TS-7714?src=confmacro) | [华润电力] 3.3.8 show streams 指令文档描述不对 | - | - | - | - | 新增 | 已完成 |
| 5 | [TS-7596](https://jira.taosdata.com:18080/browse/TS-7596?src=confmacro) | [交付][东航私有云] 授权服务可以按节点数或 CPU 数量限制授权总数 | - | - | - | - | 新增 | 已完成 |
| 6 | [TS-7591](https://jira.taosdata.com:18080/browse/TS-7591?src=confmacro) | [交付][卡奥斯] 使用 Last 查询虚拟表时走对应子表的缓存 | - | - | - | - | 新增 | 已完成 |
| 7 | [TS-7567](https://jira.taosdata.com:18080/browse/TS-7567?src=confmacro) | [交付][长庆油田] 消费端可控的 WAL 保留机制（订阅） | - | - | - | - | 新增 | 已完成 |
| 8 | [TS-7540](https://jira.taosdata.com:18080/browse/TS-7540?src=confmacro) | [售前] compact 命令支持 force 选项 | - | - | - | - | 新增 | 已完成 |
| 9 | [TS-7539](https://jira.taosdata.com:18080/browse/TS-7539?src=confmacro) | [交付][长庆油田] 消费端可控的 WAL 保留机制（存储） | - | - | - | - | 新增 | 已完成 |
| 10 | [TS-7399](https://jira.taosdata.com:18080/browse/TS-7399?src=confmacro) | [售前] 提升 event_window 按 tbname 分组查询的效率 | - | - | - | - | 新增 | 已完成 |
| 11 | [TS-7348](https://jira.taosdata.com:18080/browse/TS-7348) | [售前][卡奥斯] stmt 支持虚拟表查询 | - | - | - | - | - | 已完成 |
| 12 | [TS-7346](https://jira.taosdata.com:18080/browse/TS-7346) | [交付][中船九院] 节点时钟异常时，taosd 与 taosc 行为保持一致 | - | - | - | - | - | 已完成 |
| 13 | [TS-7325](https://jira.taosdata.com:18080/browse/TS-7325) | [交付][华润电力] 单条 SQL 的长度上限可配置 | - | - | - | - | - | 已完成 |
| 14 | [TS-7294](https://jira.taosdata.com:18080/browse/TS-7294) | [售前][恒运昌] stmt2 接口速度在某些场景低于 stmt 接口 | - | - | - | - | - | 已取消 |
| 15 | [TS-7207](https://jira.taosdata.com:18080/browse/TS-7207?src=confmacro) | [售前][工银瑞信] 提高 Interlace 模式下流式计算的性能 | - | - | - | - | - | 已完成 |
| 16 | [TS-7202](https://jira.taosdata.com:18080/browse/TS-7202?src=confmacro) | [交付][南网储能-拾贝云] 虚拟超级表查询慢 | - | - | - | - | - | 已完成 |
| 17 | [TS-7198](https://jira.taosdata.com:18080/browse/TS-7198?src=confmacro) | [售前][晶澳太阳能]支持多变量分析 | - | - | - | - | - | 已完成 |
| 18 | [TS-7150](https://jira.taosdata.com:18080/browse/TS-7150?src=confmacro) | [智共荟]虚拟表查询 last 值耗时长 | - | - | - | - | - | 已取消 |
| 19 | [TS-7018](https://jira.taosdata.com:18080/browse/TS-7018) | [IDMP]面板中配置“实时预测”，选择某些算法会报错 | - | - | - | - | 新增 | 已完成 |
| 20 | [TS-6919](https://jira.taosdata.com:18080/browse/TS-6919?src=confmacro) | [公共] show connections时，增加显示连接器/客户端的版本号字段 | - | - | - | - | - | 已完成 |
| 21 | [TS-6865](https://jira.taosdata.com:18080/browse/TS-6865?src=confmacro) | [交付][中国电建华东勘测] 支持登录失败策略 | - | - | - | - | - | 已完成 |
| 22 | [TS-6863](https://jira.taosdata.com:18080/browse/TS-6863?src=confmacro) | [售前][中国气象局] 支持在审计日志中记录查询、删除等操作 | - | - | - | - | - | 已完成 |
| 23 | [TS-6666](https://jira.taosdata.com:18080/browse/TS-6666?src=confmacro) | [交付][东航私有云] 独立的授权服务 | - | - | - | - | - | 已完成 |
| 24 | [TS-6665](https://jira.taosdata.com:18080/browse/TS-6665?src=confmacro) | [售前][赛力斯] 虚拟表列数量上限调整为 65535 | - | - | - | - | - | 已完成 |
| 25 | [TS-6562](https://jira.taosdata.com:18080/browse/TS-6562?src=confmacro) | [售前] 修改标签值后订阅及时生效 | - | - | - | - | - | 已完成 |
| 26 | [TS-6481](https://jira.taosdata.com:18080/browse/TS-6481?src=confmacro) | [交付][中石油加油站项目] 按等保要求修改密码安全策略 | - | - | - | - | - | 已完成 |
| 27 | [TS-6477](https://jira.taosdata.com:18080/browse/TS-6477) | [交付][山东能源] tmq info 级别日志调整 | - | - | - | - | 新增 | 已完成 |
| 28 | [TS-6412](https://jira.taosdata.com:18080/browse/TS-6412?src=confmacro) | [交付] 通过系统表 ins_tables 统计子表数量性能差 | - | - | - | - | - | 已完成 |
| 29 | [TS-6379](https://jira.taosdata.com:18080/browse/TS-6379?src=confmacro) | [交付][中石化] 修改表结构相关信息无需重建 topic | - | - | - | - | - | 已完成 |
| 30 | [TS-6146](https://jira.taosdata.com:18080/browse/TS-6146?src=confmacro) | [交付][电建华东院] 结果集带标签列时 last_row 查询性能优化 | - | - | - | - | - | 已完成 |
| 31 | [TS-5982](https://jira.taosdata.com:18080/browse/TS-5982?src=confmacro) | [售前] 执行计划无法显示索引 | - | - | - | - | - | 已完成 |
| 32 | [TS-5925](https://jira.taosdata.com:18080/browse/TS-5925?src=confmacro) | [售前][宁德新能源] 并发查询较多时写入受到影响 | - | - | - | - | - | 已完成 |
| 33 | [TS-5877](https://jira.taosdata.com:18080/browse/TS-5877?src=confmacro) | [社区][明阳集团] 优化 select tag,last_row(xxx) 的查询性能 | - | - | - | - | - | 已完成 |

#### 3.2.2 工具

| 序号 | 关键字 | 标题 | 需求 | 设计 | 开发 | 测试 | 变更 | 进度说明 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | [TS-7350](https://jira.taosdata.com:18080/browse/TS-7350) | [玉溪卷烟厂] explore面板中的监控界面优化 | - | - | - | - | - | 已完成 |
| 2 | [TS-7351](https://jira.taosdata.com:18080/browse/TS-7351) | [蒙西电网] taosExplorer 页面绘制曲线支持 decimal 数据类型 | - | - | - | - | - | 已完成 |
| 3 | [TS-7083](https://jira.taosdata.com:18080/browse/TS-7083) | taosX DataIn MQTT任务配置时支持多个broker |  |  |  |  |  | 转到 260331 周期 |
| 4 | [TS-7052](https://jira.taosdata.com:18080/browse/TS-7052) | taosX MQTT datain增加可配置参数 sub-offset |  |  |  |  |  | 转到 260331 周期 |
| 5 | [TS-6892](https://jira.taosdata.com:18080/browse/TS-6892) | [亿纬锂能] JSON 数据解析能力提升--嵌套数据 | - | - | - | - | - | 已完成 |
| 6 | [TS-6856](https://jira.taosdata.com:18080/browse/TS-6856) | taosAdapter支持websocket统一连接管理、鉴权管理 | - | - | - | - | - | 已完成 |
| 7 | [TS-6723](https://jira.taosdata.com:18080/browse/TS-6723) | [积成电子][共性需求] 连接器兼容性功能增强 | - | - | - | - | - | 已完成 |
| 8 | [TS-6713](https://jira.taosdata.com:18080/browse/TS-6713) | [积成电子]未配置 ssl 时出现明文密码传输，应改进 |  |  |  |  |  | 转到 260331 周期 |
| 9 | [TS-6706](https://jira.taosdata.com:18080/browse/TS-6706) | [公共] show connections时，增加显示连接器/客户端的版本号字段 | - | - | - | - | - | 已完成 |
| 10 | [TS-6455](https://jira.taosdata.com:18080/browse/TS-6455) | [内部] JDBC 绑定参数优化 | - | - | - | - |  | 已完成 |
| 11 | [TS-6299](https://jira.taosdata.com:18080/browse/TS-6299) | [沃太能源] taosX能够保证高可用，避免任务中断。 | - | - | - | - | - | 已完成 |
| 12 | [TS-5786](https://jira.taosdata.com:18080/browse/TS-5786) | [公共需求] 工具组 - 维护系统参数版本号及默认值清单 | - | - | - | - | - | 已完成 |
| 13 | [TS-5724](https://jira.taosdata.com:18080/browse/TS-5724) | [宁夏天地奔牛] TDengine产品的一些安全策略不达标 | - | - | - | - | - | 已完成 |
| 14 | [TS-7379](https://jira.taosdata.com:18080/browse/TS-7379) | [神东集团] KingHistorian数据迁移 | - | - | - | - | - | 已完成 |
| 15 | [TS-7477](https://jira.taosdata.com:18080/browse/TS-7477) | [海莱德自动化] taosX: 支持 OPC 冗余通信 | - | - | - | - | 新增 | 已完成，高优先级 |
| 16 | [~~TS-7476~~](https://jira.taosdata.com:18080/browse/TS-7476) | ~~[河北电力新一代调度项目] taosX 归档文件能够自定义文件占用的空间~~ |  |  |  |  | 移除 | 转到 260331 周期 |
| 17 | [TS-7470](https://jira.taosdata.com:18080/browse/TS-7470) | [taosX] CSV数据写入任务，配置CSV地址添加 “保留已处理文件”的选项 | - | - | - | - | 新增 | 已完成 |
| 18 | [TS-7467](https://jira.taosdata.com:18080/browse/TS-7467) | websocket连接方式支持failover特性backport至3.3.6.* | - | - | - | - | 新增 | 已完成 |
| 19 | [TS-7466](https://jira.taosdata.com:18080/browse/TS-7466) | [内部] python连接器补充执行sql超时配置参数 | - | - | - | - | 新增 | 已完成 |
| 20 | [TS-7446](https://jira.taosdata.com:18080/browse/TS-7446) | [内部测试]taosExplorer面板支持排序 | - | - | - | - | 新增 | 已完成 |
| 21 | [TS-7429](https://jira.taosdata.com:18080/browse/TS-7429) | [树根互联] taosx legacy_to_taos 支持 ns -> ms 的转存 | - | - | - | - | 新增 | 已完成，高优先级 |
| 22 | [TS-7422](https://jira.taosdata.com:18080/browse/TS-7422) | [公共] 在引擎执行前可以按预先制定规则拦截指定模式查询 | - | - | - | - | 新增 | 已完成 |
| ~~23~~ | [~~TS-7406~~](https://jira.taosdata.com:18080/browse/TS-7406) | ~~[售前] explorer 登录增加CAPTCHA功能~~ |  |  |  |  | ~~移除~~ | ~~转到 260331 周期~~ |
| ~~24~~ | [~~TS-7524~~](https://jira.taosdata.com:18080/browse/TS-7524) | ~~[河北电力新一代调度项目] taosX 归档的 archive 文件读取工具~~ |  |  |  |  | ~~移除~~ | ~~转到 260331 周期~~ |
| 25 | [TS-7570](https://jira.taosdata.com:18080/browse/TS-7570) | [KingHistorian] 支持查询 TagGroupProperties | - | - | - | - | 新增 | 已完成 |
| 26 | [TS-7536](https://jira.taosdata.com:18080/browse/TS-7536) | [南网广州电力] DataIn 数据写入任务列表需显示task-id | - | - | - | - | 新增 | 已完成 |
| 27 | [TS-7537](https://jira.taosdata.com:18080/browse/TS-7537) | [长江生态环保集团]OpenTSDB导入支持自定义列名标签名 | - | - | - | - | 新增 | 已完成 |
| 28 | [TS-7570](https://jira.taosdata.com:18080/browse/TS-7570) | [KingHistorian] 支持查询 TagGroupProperties | - | - | - | - | 新增 | 已完成 |
| 29 | [TS-7571](https://jira.taosdata.com:18080/browse/TS-7571) | [安徽智质]taosx去掉log中opcda任务的点位打印信息 | - | - | - | - | 新增 | 已完成 |
| 30 | [TS-7576](https://jira.taosdata.com:18080/browse/TS-7576) | [中冶京诚]explorer web页面点击返回慢 | - | - | - | - | 新增 | 已完成 |
| 31 | [TS-7583](https://jira.taosdata.com:18080/browse/TS-7583) | [长江生态环保集团] OpenTSDB导入支持自定义子表名 | - | - | - | - | 新增 | 已完成 |
| 32 | [TS-7585](https://jira.taosdata.com:18080/browse/TS-7585) | [内部测试]taosx opentsdb-id.log 文件过大 | - | - | - | - | 新增 | 已完成 |
| 33 | [TS-7586](https://jira.taosdata.com:18080/browse/TS-7586) | [长沙卷烟厂]OPC UA数据采集提示异常 | - | - | - | - | 新增 | 已完成 |
| 34 | [TS-7592](https://jira.taosdata.com:18080/browse/TS-7592) | [EPDC] taosx TMQ 同步时遇表不存在错误可跳过目标库表存在查询 | - | - | - | - | 新增 | 已完成 |
| 35 | [TS-7658](https://jira.taosdata.com:18080/browse/TS-7658) | [双活] taosX 双活同步实现支持 WAL 保留机制 | - | - | - | - | 新增 | 已完成 |
| 36 | [TS-7661](https://jira.taosdata.com:18080/browse/TS-7661) | [公共] taosAdapter 拦截规则可监听配置文件修改并生效 | - | - | - | - | 新增 | 已完成 |
| 37 | [TS-7667](https://jira.taosdata.com:18080/browse/TS-7667) | [上海麦糖] taosx 在 tmq 模式下支持高版本(3.3.6.x) 向低版本(3.1.1.x)实时同步 | - | - | - | - | 新增 | 已完成 |
| 38 | [TS-7684](https://jira.taosdata.com:18080/browse/TS-7684) | [树根科技] taosX 数据迁移支持额外的 WHERE 条件 | - | - | - | - | 新增 | 已完成 |
| 39 | [TS-7690](https://jira.taosdata.com:18080/browse/TS-7690) | [海澜智云科技有限公司] taosX 内存回收机制改进 | - | - | - | - | 新增 | 已完成 |
| 40 | [TS-7693](https://jira.taosdata.com:18080/browse/TS-7693) | [安徽智质]配置opc-da的数据写入任务，opc-da采集优化 | - | - | - | - | 新增 | 已完成 |

#### 3.2.3 平台

| 序号 | 关键字 | 标题 | 需求 | 设计 | 开发 | 测试 | 变更 | 进度说明 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | [TS-7297](https://jira.taosdata.com:18080/browse/TS-7297) | 验证双副本N-1断网、kill -9的切主性能 |  |  |  |  |  | ~~转到 260331 周期~~ |
| 2 | [TS-7196](https://jira.taosdata.com:18080/browse/TS-7196) | 【中保网盾】中兴新支点操作系统NewStart-Security-Server-OS-V6.06.07测试验证兼容性 |  |  |  |  | 新增 | ~~转到 260331 周期~~ |
| 3 | [TS-7430](https://jira.taosdata.com:18080/browse/TS-7430) | TDengine升级后/etc/systemd/system/taosd.service被重置 | - | - | - | - | 新增 | 已完成 |
| 4 | [TS-7631](https://jira.taosdata.com:18080/browse/TS-7631) | [天合富家] 3.3.6.12 - 20251028分支出具测试报告 | - | - | - | - | 新增 | 已完成 |
| 5 | [TS-7706](https://jira.taosdata.com:18080/browse/TS-7706) | [河北电力新一代调度项目]验证 restore dnode 过程中订阅的可用性 | - | - | - | - | 新增 | 已完成 |

### 3.3 海外业务

#### 3.3.1 引擎

| 序号 | 关键字 | 标题 | 需求 | 设计 | 开发 | 测试 | 变更 | 进度说明 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |

#### 3.3.2 工具

| 序号 | 关键字 | 标题 | 需求 | 设计 | 开发 | 测试 | 变更 | 进度说明 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | [TS-6852](https://jira.taosdata.com:18080/browse/TS-6852) | Better support for SQLAlchemy | - | - | - | - |  | 已完成 |
| 2 | [TS-7448](https://jira.taosdata.com:18080/browse/TS-7448) | [Ume Tea] taosX: Add data source for Tuya IOT via Pulsar MQ | - | - | - | - | 新增 | 已完成 |

#### 3.3.3 平台

| 序号 | 关键字 | 标题 | 需求 | 设计 | 开发 | 测试 | 变更 | 进度说明 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | [TS-7577](https://jira.taosdata.com:18080/browse/TS-7577) | [nevados] 流计算验证分支更新，需要更新云上的部署实例 | - | - | - | - |  | 已完成 |
| 2 | [TS-7607](https://jira.taosdata.com:18080/browse/TS-7607) | [nevados] 流计算验证分支更新，需要更新云上的部署nevados-stream-test实例 | - | - | - | - | 新增 | 已完成 |

### 3.4 产品规划

#### 3.4.1 引擎

| 序号 | 关键字 | 标题 | 需求 | 设计 | 开发 | 测试 | 变更 | 进度说明 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | [TS-7431](https://jira.taosdata.com:18080/browse/TS-7431?src=confmacro) | 外部实例注册（for taosAdapter） | - | - | - | - | 新增 | 已完成 |
| 2 | [TS-7276](https://jira.taosdata.com:18080/browse/TS-7276?src=confmacro) | 事件窗口支持子事件窗口 | - | - | - | - |  | 已完成 |
| 3 | [TS-7270](https://jira.taosdata.com:18080/browse/TS-7270?src=confmacro) | [产品] 安可：加密算法 | - | - | - | - |  | 已完成 |
| 4 | [TS-7241](https://jira.taosdata.com:18080/browse/TS-7241) | [产品] 流计算性能测试报告 | - | - | - | - |  | 已取消 |
| 5 | [TS-7236](https://jira.taosdata.com:18080/browse/TS-7236?src=confmacro) | [产品] 安可：编码安全 | - | - | - | - |  | 已取消 |
| 6 | [TS-7235](https://jira.taosdata.com:18080/browse/TS-7235?src=confmacro) | [产品] 安可：安全函数 | - | - | - | - |  | 已完成 |
| 7 | [TS-7234](https://jira.taosdata.com:18080/browse/TS-7234?src=confmacro) | [产品] 安可：入侵防范 | - | - | - | - |  | 已取消 |
| 8 | [TS-7233](https://jira.taosdata.com:18080/browse/TS-7233?src=confmacro) | [产品] 安可：安全审计 | - | - | - | - |  | 已完成 |
| 9 | [TS-7232](https://jira.taosdata.com:18080/browse/TS-7232?src=confmacro) | [产品] 安可：访问控制 | - | - | - | - |  | 已完成 |
| 10 | [TS-7231](https://jira.taosdata.com:18080/browse/TS-7231?src=confmacro) | [产品] 安可：身份鉴别 | - | - | - | - |  | 已完成 |
| 11 | [TS-7230](https://jira.taosdata.com:18080/browse/TS-7230?src=confmacro) | [产品] 安可：存储安全 | - | - | - | - |  | 已完成 |
| 12 | [TS-7229](https://jira.taosdata.com:18080/browse/TS-7229?src=confmacro) | [产品] 安可：传输安全 | - | - | - | - |  | 已完成 |
| 13 | [TS-7132](https://jira.taosdata.com:18080/browse/TS-7132?src=confmacro) | [产品] 虚拟表窗口计算性能优化 | - | - | - | - |  | 已完成 |
| 14 | [TS-6102](https://jira.taosdata.com:18080/browse/TS-6102?src=confmacro) | [产品] 窗口查询不需强制使用聚合函数 | - | - | - | - | 新增 | 已完成 |
| 15 | [TD-38729](https://jira.taosdata.com:18080/browse/TD-38729) | 优化：禁止创建虚拟表时源库精度不一致 | - | - | - | - | 新增 | 已完成 |
| 16 | [TD-38615](https://jira.taosdata.com:18080/browse/TD-38615) | 优化：新增 walDeleteOnCorruption 参数，启用后备份损坏的 WAL 文件并继续启动数据库 | - | - | - | - | 新增 | 已完成 |
| 17 | [TD-38577](https://jira.taosdata.com:18080/browse/TD-38577) | 优化：TDgpt 的数据补全算法支持任意采样间隔 | - | - | - | - | 新增 | 已完成 |
| 18 | [TD-38562](https://jira.taosdata.com:18080/browse/TD-38562) | 优化：在数据补全算法中明确白噪音检查失败的错误信息 | - | - | - | - | 新增 | 已完成 |
| 19 | [TD-38533](https://jira.taosdata.com:18080/browse/TD-38533) | 优化：改进 taosmqtt 的退出处理逻辑，实现更优雅的停机和资源释放 | - | - | - | - | 新增 | 已完成 |
| 20 | [TD-38501](https://jira.taosdata.com:18080/browse/TD-38501) | 优化：支持在流计算中缓存标签等值条件的过滤结果 | - | - | - | - | 新增 | 已完成 |
| 21 | [TD-38450](https://jira.taosdata.com:18080/browse/TD-38450) | 优化：不再重试已经超时的 RPC 消息 | - | - | - | - | 新增 | 已完成 |
| 22 | [TD-38256](https://jira.taosdata.com:18080/browse/TD-38256) | 优化：降低流计算在读取数据时的 CPU 使用比例 | - | - | - | - | 新增 | 已完成 |
| 23 | [TD-38255](https://jira.taosdata.com:18080/browse/TD-38255) | 优化：提升存在历史数据的流计算启动速度 | - | - | - | - | 新增 | 已完成 |
| 24 | [TD-38211](https://jira.taosdata.com:18080/browse/TD-38211) | 优化：支持修改 RSMA | - | - | - | - | 新增 | 已完成 |
| 25 | [TD-38163](https://jira.taosdata.com:18080/browse/TD-38163) | 优化：改进 mnode 启动时的日志记录和错误码显示 | - | - | - | - | 新增 | 已完成 |
| 26 | [TD-38148](https://jira.taosdata.com:18080/browse/TD-38148) | 优化：降低流计算触发数据与计算数据读取的资源消耗 | - | - | - | - | 新增 | 已完成 |
| 27 | [TD-38139](https://jira.taosdata.com:18080/browse/TD-38139) | 优化：流计算支持 interp 和 percentile 函数 | - | - | - | - | 新增 | 已完成 |
| 28 | [TD-38132](https://jira.taosdata.com:18080/browse/TD-38132) | 特性：增加皮尔逊相关系数计算函数 | - | - | - | - | 新增 | 已完成 |
| 29 | [TD-38004](https://jira.taosdata.com:18080/browse/TD-38004) | 优化：提升 last_row + composite key 的查询性能 | - | - | - | - | 新增 | 已完成 |
| 30 | [TD-37993](https://jira.taosdata.com:18080/browse/TD-37993) | event 窗口支持多个start condition | - | - | - | - |  | 已完成 |
| 31 | [TD-37942](https://jira.taosdata.com:18080/browse/TD-37942) | 优化：状态窗口支持通过 zeroth_state 指定“零状态”，处于该状态的窗口将跳过计算与输出 | - | - | - | - | 新增 | 已完成 |
| 32 | [TD-37767](https://jira.taosdata.com:18080/browse/TD-37767) | [产品] TDgpt 支持时序数据相关性分析 | - | - | - | - | 新增 | 已完成 |
| 33 | [TD-37309](https://jira.taosdata.com:18080/browse/TD-37309) | 流计算支持增删子表和修改子表标签值 | - | - | - | - |  | 已完成 |
| 34 | [TD-37063](https://jira.taosdata.com:18080/browse/TD-37063) | 优化：流计算的内存占用 | - | - | - | - | 新增 | 已完成 |
| 35 | [TD-37059](https://jira.taosdata.com:18080/browse/TD-37059) | partition by tag 支持字符串运算 | - | - | - | - | 新增 | 已完成 |
| 36 | [TD-31688](https://jira.taosdata.com:18080/browse/TD-31688) | taos-CLI 支持 3.3.5.0 ~ 3.3.8.0 版本中新增的命令词 TAB 补全 | - | - | - | - | 新增 | 已完成 |
| 37 | [6570501686](https://project.feishu.cn/taosdata_td/feature/detail/6570501686) | 优化: stmt2 写入时新增布尔类型校验 | - | - | - | - | 新增 | 已完成 |
| 38 | [6551611200](https://project.feishu.cn/taosdata_td/feature/detail/6551611200) | 优化：调整 stmt2 的日志，便于问题定位 | - | - | - | - | 新增 | 已完成 |
| 39 | [6497313576](https://project.feishu.cn/taosdata_td/feature/detail/6497313576) | 优化：优化 RPC 通信过程中读写锁的使用逻辑 | - | - | - | - | 新增 | 已完成 |

#### 3.4.2 工具

| 序号 | 关键字 | 标题 | 需求 | 设计 | 开发 | 测试 | 变更 | 进度说明 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | [TS-7257](https://jira.taosdata.com:18080/browse/TS-7257) | [产品] taosX 性能测试报告写入到 TDengine |  |  |  |  |  | 转到 260331 周期 |
| 2 | [TS-7059](https://jira.taosdata.com:18080/browse/TS-7059) | [产品] taosX: 写入方式优化 |  |  |  |  |  | 转到 260331 周期 |
| 3 | [TS-6188](https://jira.taosdata.com:18080/browse/TS-6188) | [产品] libtaos websocket 连接 CI 测试 | - | - | - | - |  | 已完成 |
| 4 | [TS-6143](https://jira.taosdata.com:18080/browse/TS-6143) | [测试] ]nodejs 连接器性能压测工具开发 | - | - | - | - |  | 已完成 |
| 5 | [TS-6141](https://jira.taosdata.com:18080/browse/TS-6141) | [产品] python 连接器性能压测工具开发 | - | - | - | - |  | 已完成 |
| 6 | [TS-6140](https://jira.taosdata.com:18080/browse/TS-6140) | [测试] C# 连接器性能压测工具开发 | - | - | - | - |  | 已完成 |
| 7 | [TS-6020](https://jira.taosdata.com:18080/browse/TS-6020) | [产品] 支持HTTP Post 的数据处理，数据格式是JSON | - | - | - | - |  | 已完成 |
| 8 | [TS-5665](https://jira.taosdata.com:18080/browse/TS-5665) | [产品] 客户端兼容性：taosAdapter 高可用 | - | - | - | - |  | 已完成 |
| 9 | [TS-5183](https://jira.taosdata.com:18080/browse/TS-5183) | [产品] taosBenchmark 支持导入 CSV 文件 | - | - | - | - |  | 已完成(taosgen 支持) |
| 10 | [TS-4922](https://jira.taosdata.com:18080/browse/TS-4922) | [产品] 支持 OAuth 2.0 | - | - | - | - |  | 已完成 |
| 11 | [TS-7354](https://jira.taosdata.com:18080/browse/TS-7354) | [产品] Explorer: 支持 OpenID Connect 第三方认证登录 | - | - | - | - |  | 已完成 |
| 12 | [TS-7353](https://jira.taosdata.com:18080/browse/TS-7353) | [产品] taosgen: 支持发布到 Kafka | - | - | - | - |  | 已完成 |
| 13 | [TS-7513](https://jira.taosdata.com:18080/browse/TS-7513) | [产品] taosAdapter 支持新版本身份鉴别策略 | - | - | - | - |  | 已完成 |

#### 3.4.3 平台

| 序号 | 关键字 | 主题 | 需求 | 设计 | 开发 | 测试 | 变更 | 进度说明 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | [TD-37719](https://jira.taosdata.com:18080/browse/TD-37719) | 检查 PR 变更代码的覆盖率 | - | - | - | - |  | 已完成 |
| 2 | [TD-38239](https://jira.taosdata.com:18080/browse/TD-38239) | 缩短 PR 的执行时间至当前的三分之一 | - | - | - | - |  | 已完成 |
| 3 | [TD-38241](https://jira.taosdata.com:18080/browse/TD-38241) | 全线产品 Docker Image 优化 | - | - | - | - |  | 已完成，包括： TSDB & IDMP |
| 4 | [TD-38240](https://jira.taosdata.com:18080/browse/TD-38240) | 云服务同步升级 | - | - | - | - |  | 已完成 |
| 5 | [TD-38247](https://jira.taosdata.com:18080/browse/TD-38247) | 云服务告警的治理 | - | - | - | - |  | 已完成 |
| 6 | [TD-38244](https://jira.taosdata.com:18080/browse/TD-38244) | 创建性能测试的统一看板 | - | - | - | - |  | 已完成 |
| 7 | [TD-37573](https://jira.taosdata.com:18080/browse/TD-37573) | 使用 Telegraf + TSDB + IDMP把公司所有IT资源管理起来 | - | - | - | - | 新增 | 已完成 |
| 8 | [TD-38119](https://jira.taosdata.com:18080/browse/TD-38119) | 内网服务器资源动态释放 | - | - | - | - | 新增 | 已完成 |
| 9 | [TD-38115](https://jira.taosdata.com:18080/browse/TD-38115) | 安可：内网环境安全防护 | - | - | - | - | 新增 | 已完成 |
| 10 | [TD-38245](https://jira.taosdata.com:18080/browse/TD-38245) | 为各产品主要文档中描述的步骤添加测试 | - | - | - | - | 新增 | 已完成 |
| 11 | [TD-38246](https://jira.taosdata.com:18080/browse/TD-38246) | 客户场景模拟的优化以及应用 | - | - | - | - | 新增 | 已完成 |
| 12 | [TD-38249](https://jira.taosdata.com:18080/browse/TD-38249) | 将 Terraform/OpenTofu 引入云服务的运维 | - | - | - | - | 新增 | 已完成 |
| 13 | [TD-38250](https://jira.taosdata.com:18080/browse/TD-38250) | 内网部署自动化平台 n8n 并应用 | - | - | - | - | 新增 | 已完成 |

## 4. 风险管理表

| 编号 | 风险分类 | 风险描述 | 提交人 | 提交日期 | 发生阶段 | 责任人 | 可能性 | 风险级别 | 管理策略 | 应对措施描述 | 风险状态 | 状态更新日 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 设计风险 | 安全功能开发缺少专业技术支撑 | 关胜亮 | 2025-9-24 | 需求与设计 | 关胜亮 | 高 | 中 | 风险减轻 | 咨询 AI 及相关书籍 | 已解决 | 2025-10-20 |
| 2 | 设计风险 | 安全功能可能引入性能衰退风险​ | 霍琳贺 | 2025-9-24 | 系统测试与验收 | 关胜亮 | 高 | 中 | 风险减轻 | 各功能引入性能测试 | 已识别 | 2025-12-14 |
| 3 | 管理风险 | 研发体系建设和过程改进无参考方案 | 关胜亮 | 2025-9-24 | 研发文档编制 | 关胜亮 | 高 | 中 | 风险减轻 | 参考 CMMI3 认证体系 | 已解决 | 2025-10-20 |
| 4 | 管理风险 | 质量体系建设无参考方案 | 王旭 | 2025-9-24 | 研发文档编制 | 王旭 | 高 | 中 | 风险减轻 | 引入外部专家 | 已解决 | 2025-11-21 |
| 5 | 管理风险 | 安全体系建设无参考方案 | 霍琳贺 | 2025-9-24 | 研发文档编制 | 霍琳贺 | 高 | 中 | 风险减轻 | 引入外部专家 | 已解决 | 2025-11-21 |

## 5. 月度总结

### 5.1 2025年10月总结

1. 项目进度总述
   - 本月项目整体进展正常，引擎、工具、平台三大模块均按计划推进，未识别出新的重大风险。
   - 本月工作的核心焦点在于安全可靠性提升、特定功能性能优化以及响应中国业务市场的客户需求。
2. 项目主要成果
   - 虚拟超级表性能提升
   - 优化 Last 查询性能
   - 流计算对比测试将基于 TSBS 开展
   - 安全可靠功能的所有需求文档已经编写且评审完成
3. 本月需求变更
   - 无影响项目进度的显著变更
4. 本月缺陷说明
   - 新发布的功能，未识别新的缺陷
5. 下月工作计划
   - 按照项目计划开展
   - 重点放在安全可靠性提升、流计算性能对比测试、虚拟表性能提升三个方面

### 5.2 2025年11月总结

1. 项目进度总述
   - 本月项目整体进展正常，引擎、工具、平台三大模块均按计划推进，未出现严重滞后情况。
   - 核心工作聚焦安全可靠性提升、关键功能优化（虚拟表、流计算等）及客户需求交付
   - 工具模块因新增安全相关任务及数据源适配需求，taosX 高可用实现进度存在一定风险。
2. 项目主要成果
   - 引擎侧：完成安全可靠性多模块的设计、开发仅中，虚拟表窗口查询性能提升进行中，流计算在 Nevados 场景下性能提升。
   - 工具侧：taosgen 开源并完成 Kafka 写入支持，新增 KingHistorian、Pulsar 等数据源适配，多个高优先级客户需求（如 OPC 冗余通信、CSV 任务保留文件配置）已完成交付。
   - 平台侧：PR 运行时间缩短至原来三分之一，云服务告警治理自动化推进，Docker 镜像优化及内网安全防护按计划落地，部分分支部署实例更新完成。
   - 业务交付：中国业务端长庆油田 WAL 保留机制、华润电力 SQL 长度配置等多项客户需求已完成，海外业务 Tuya IOT 数据源适配等任务落地。
3. 本月需求变更
   - 引擎模块新增任务 15 个、移出任务 11 个，工具模块新增任务 25 个、移出任务 6 个，平台模块无任务增减，整体范围调整未对核心进度造成重大影响。
4. 本月缺陷说明
   - 新增功能未识别出重大缺陷，工具模块部分任务（如 taosAdapter 鉴权管理）仍在推进中，需后续测试验证。
5. 下月工作计划
   - 持续推进未完成任务交付，重点攻克工具模块 taosX 高可用实现风险，确保安全可靠性相关功能全部落地。
   - 完成虚拟表、流计算等优化功能的测试验收，推进产品规划中未开始任务启动开发。
   - 跟进客户需求收尾工作，做好版本发布前的准备及风险排查。

### 5.3 2025年12月总结

1. 项目进度总述 
   - **引擎模块**：各项安全工作按计划推进，虚拟表与流计算性能优化成效显著。
   - **工具模块**：taosX 高可用开发等核心任务取得关键进展，但部分安可文档编写进度存在延迟。
   - **平台模块**：IDMP 支持与云服务运维是工作重点，多项发版和基础设施优化工作顺利完成。
2. 项目主要成果 
   - **引擎模块**
      - **安全可靠性提升**：安全函数、加密算法等模块开发已完成或进入测试阶段；身份鉴别、传输安全等模块进展顺利。
      - **虚拟表性能优化**：超级表查询性能瓶颈已优化，窗口查询优化开发完成，相关测试报告已撰写。
      - **流计算增强**：完成对 Nevados 等场景的性能优化并上线；支持`interp`和`percentile`函数等新功能。
      - **TDgpt**：已完成支持相关性分析的功能开发。
   - **工具模块**
      - **taosX高可用开发**：无状态任务调度器改造完成，mnode 端任务存储与分发接口开发取得进展。
      - **安可文档编写**：完成了部分管理制度文档及相关需求/设计文档的编写。
      - **连接器与质量**：JDBC 连接器优化了负载均衡逻辑；建立了 PR 报告单元测试覆盖率的流程。
   - **平台模块**
      - **IDMP支持**：完成了1.0.8.x 和1.0.9.0 等多个版本的发布，并支持了云服务实例的升级。
      - **云服务与基础设施**：完成了AWS EKS版本的升级计划第一阶段；初步完成了性能测试统一看板的方案设计。
      - **内部效率提升**：优化了飞书项目流程，新增了小组看板，并调研了与销售易、用友等第三方系统的集成方案。
3. 风险与挑战
   - **工具模块资源分配**：客户支持任务较多时，会影响taosX高可用等核心功能的开发进度
   - **平台模块依赖项**：IDMP IT Monitor的首次发版体验与预期功能还存在较大差距，需要持续改进
   - **安全可靠功能**：需要开始加班，才能确保顺利开发完成
4. 下月工作计划
   - **引擎模块**：继续跟进安可相关代码的开发与文档编写；进行 2026 年 Q1 的工作规划。
   - **工具模块**：重点推进 taosX 高可用功能的开发与联调；协助解决北美团队的技术问题。
   - **平台模块**：继续推进性能测试统一看板的建设；完成公司IT资产盘点与架构图更新；将 Terraform/OpenTofu 进一步引入云服务运维。

### 5.4 2026年01月总结

1. 项目进度总述 
项目全面达成初始设定目标。成功交付的 TDengine 3.4.0.0 版本包含 21 项新特性、26 项优化和 45 项修复[[1]](https%3A%2F%2Fdocs.taosdata.com%2Freleases%2Fnotes%2F3.4.0.0%2F)[[2]](https%3A%2F%2Fnewreleases.io%2Fproject%2Fgithub%2Ftaosdata%2FTDengine%2Frelease%2Fver-3.4.0.0)。产品能力在多个维度得到显著提升。
1. 项目主要成果 
   - 安全功能全面提升
   - 流计算的事件窗口触发支持子事件窗口
   - 流计算的资源消耗和计算延迟显著降低
   - 虚拟表的查询性能优化
   - 查询性能优化及语法增强
   - 连接器生态更加健壮
2. 后续工作计划
   - 完成项目总结过程
   - 完成项目各产出成果归档
该文档后续不再更新
