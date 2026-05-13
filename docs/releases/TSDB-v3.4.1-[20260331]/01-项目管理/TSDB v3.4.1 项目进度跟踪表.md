# TSDB v3.4.1 项目进度跟踪表

## 1. 修订记录

| 更新日期 | 更新人 | 主要修改内容 |
| --- | --- | --- |
| 2025-1-21 | 关胜亮 | 编写工作分解结构和风险管理表，更新各任务的进行情况 |
| 2026-2-25 | 关胜亮 | 更新各任务的工作进度 |
| 2026-3-11 | 关胜亮 | 更新各任务的工作进度 |
| 2026-3-19 | 霍琳贺、肖波 | 更新个任务工作进度 |
| 2026-4-14 | 关胜亮 | 更新各任务的工作进度，增加4月总结 |

## 2. 项目进度概览

1. 整体进度：*（未开始/正常/有风险/严重滞后/已完成）*
   - 业务：总计 64 项，已完成 64 项，未完成 0 项（0%）—— **已完成**
   - IDMP：总计 29 项，已完成 29 项，未完成 0 项（0%）—— **已完成**
   - 规划：总计 116 项，已完成 116 项，未完成 0 项（0%）—— **已完成**
2. 范围状态：
   - 业务：新增任务 33 个，移出任务 1 个
   - IDMP：新增任务 8 个，移出任务 0 个
   - 规划：新增任务 49 个，移出任务 0 个
3. 主要风险：
   - 业务：无
   - IDMP：无
   - 规划：
      - **LicenseCenter** 经评估，此特性对交付、产品稳定性影响较大，当前可作为 Demo 场景推动，测试、文档、稳定性不具备正式版本发版条件，决定不在 3.4.1 正式版推送，内部测试、交付验证一个月无问题后再正式上线。
      - **安全漏洞修复** 已取消（评估后调整策略，转为持续治理）

## 3. 工作分解结构与进度跟踪

### 3.1 亮点功能

1. 引擎
   - 安全：安全功能开发、安全漏洞修复
   - 存储：数据修复工具、批量标签修改、动态调整数据缓存的 LRU
   - 查询：子查询、外部窗口、ANY/SOME/ALL/EXISTS 运算符、窗口插值增强、Explain 和 ShowQueries 优化
   - 虚拟表：虚拟表超级表查询性能优化、订阅虚拟表的元数据变更、虚拟表和源表的引用校验
   - 流计算：按自然周/月/季/年触发、事件和状态窗口触发的 true_for 条件支持持续时间与条数、分组计算性能优化、虚拟超级表触发支持子表增删改
2. 工具
   - 授权服务：中心化授权服务，支持 TSDB、IDMP 独立授权
   - 认证：Explorer 支持 TOTP 认证，连接器、taosX 支持 TOKEN 认证
   - 安全加固：Explorer 明文密码、SQL 注入问题修复，taosX 安全加固，Adapter、连接器安全加固：明文密码、日志信息防信息泄漏，连接器安全开发用户指南等
   - 漏洞扫描和修复：adapter/连接器/taosx 第三方依赖漏洞扫描和修复，Web 端口漏洞扫描和修复，棱镜七彩工具接入 CI
   - taosX：适配 TSDB 权限管理，Windows 适配，扩展 Transform 解析功能，导出导入顺序一致性优化，力控实时库，KingHistorian 数据源优化，MQTT 支持多个 Broker 等
3. 平台
   - 飞书项目与销售易集成
   - IDMP Code Coverage 监控
   - IDMP SDK 发布
   - 建立 Github 分支清理工作办法

### 3.2 业务

#### 3.2.1 引擎

| 名称 | 优先级 | 报告人 | 状态 | 说明 | 链接 |
| --- | --- | --- | --- | --- | --- |
| [交付] 版本滚动升级检查保障机制 | P3 | Steven Zhang | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6861305319) |
| [scada.io] 支持多变量异常检测 | P3 | Leo Huo | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6793469667) |
| [交付][河北电力] 海量子表且存在订阅的场景下，频繁修改子表标签影响数据写入 | P3 | Zee Lv | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6765905149) |
| [交付] TSDB 等内部占用的点位，不计算在客户授权测点数内 | P3 | Richard Li | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6672169603?node=26742312) |
| [交付][卡奥斯] mybatis stmt查询支持的参数绑定优化 | P3 | Kian Wang | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6671312705?node=26742312) |
| [交付] Explain analyais 可读性增强，清晰看出语句执行过程 | P3 | Steven Zhang | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659962841?node=26742312) |
| [售前] TDlite 授权支持 taosX 部分连接器 | P3 | Derek Chen | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6628216389?node=26742312) |
| [交付] taosd 停服后 taosc 重连占用了太高的 cpu | P3 | Hui Li | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6598121270?node=26742312) |
| [售前][新奥数能] 实现 stmt 查询结果集和 stmt 解耦 | P3 | Zee Lv | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6597880825?node=26742312) |
| [售前][上海电气] 并发调用 python udf 函数资源占用较高 | P3 | Tyler Liu | Canceled |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6597787383?node=26742312) |
| [交付][河北电力] 一次性批量修改多个子表的多个 tag 值功能 | P3 | Zee Lv | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6594391614?node=26742312) |
| [交付][三峡云化集控] show queries 显示执行进度 | P3 | Yanqiong Dong | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6570714028?node=26742312) |
| [交付][天合富家] 动态调整 LRU 分片数量以提高 Last 查询性能 | P3 | Yanqiong Dong | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6568211421?node=26742312) |
| [交付][深开鸿] blob 类型支持 cast、substr 函数 | P3 | Raistlin Chen | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6567926427?node=26742312) |
| [北美][Nevados] Support subqueries "IN" clauses | P3 | Simon Guan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6539521758?node=26742312) |
| [交付][海澜智云] 自动清理无效 sql 信息 | P3 | Hui Li | Canceled |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6512028015?node=26742312) |
| [售前][三峡集团] 支持发生状态改变机组的原始数值查询 | P3 | Jack Dong | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6510828810?node=26742312) |
| [售前] join/window join 支持基于选择函数结果集进行运算 | P3 | Bo Xiao | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6510828917?node=26742312) |
| [售前][硕橙科技] In 支持嵌套查询 | P3 | Jack Dong | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6510267752?node=26742312) |
| [交付][东方电子] 支持配置多个监控目标地址 | P3 | Raistlin Chen | Canceled |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6507093771?node=26742312) |
| [售前][红河卷烟厂] 事件窗口功能增强 | P3 | ​Richard Li | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6507054803?node=26742312) |
| [交付][三峡]优化高负载情况下选主行为（技术方案） | P3 | Yanqiong Dong | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6507042141?node=26742312) |
| [售前] 同时支持 Ipv4 & Ipv6 协议栈 | P3 | Bo Xiao | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6507041840?node=26742312) |
| [交付][三峡新能源] fill prev 支持填充前一个非 null 值 | P3 | Yanqiong Dong | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6506970855?node=26742312) |
| [售前][社区] Interval 窗口支持插值时间范围 | P3 | ​Richard Li | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6506145499?node=26742312) |
| [交付] 调用订阅服务密码错误返回含义不明确的错误信息 | P3 | Steven Zhang | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6490634781?node=26742312) |
| [产品] taos_register_instance 接口使用 firstep 和 secondep | P3 | Xuefeng Tan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6487556383?node=26742312) |
| [社区] TDgpt restful 驱动支持 Gunicorn | P3 | Haojun Liao | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6484950091?node=26742312) |

#### 3.2.2 工具

| 名称 | 优先级 | 报告人 | 状态 | 说明 | 链接 |
| --- | --- | --- | --- | --- | --- |
| [冀南钢铁集团有限公司] 力控pSpace实时同步/历史迁移 | P1 | Jack Dong | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6653327869) |
| [瑞幸咖啡] 说明taoskeeper上传的promethues的metrics指标与grafana中默认报警规则使用的字段对应关系 | P1 | Jack Dong | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6622579928) |
| [中石化]6041/6060/6043/6050 扫描出漏洞，希望优化 | P3 | Zee Lv | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659862768) |
| [神东集团] KH迁移过程中结束时间为空时，应表示一直进行迁移 | P3 | Jack Dong | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6600045300) |
| [售前][上海电气中央研究院] 扩展 taosX 解析功能 | P3 | Tyler Liu | - | 移除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6622709348) |
| [河北电力新一代调度项目]taosx 增加对于建立数据写入任务权限、数量控制 | P3 | Zee Lv | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6622713900) |
| [一汽红旗] taosExplorer Kafka 写入任务配置页面中，json 解析规则输入框可以放大显示 | P3 | Tyler Liu | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6622581453) |
| [交付] Explorer SQL 注入问题修复 | P3 | Zee Lv | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6622823622) |
| [河北电力]taosX 导出导入任务保证顺序一致且子表对应关系正确 | P3 | Zee Lv | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6624113545) |
| [世窗信息] influxdb迁移到TDengine时需根据原有tag值定义表名 | P2 | Zach Wang | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6660065587) |
| [售前] explorer 登录增加CAPTCHA功能 | P3 | Zach Wang | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6663139939) |
| [大庆油田]非root用户对于explorer中taosx写入任务权限管理 | P2 | Jack Dong | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665149034) |
| [社区] taosdump支持decimal数据类型的导入导出 | P3 | Hui Li | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6622691504) |
| [交付] taosX支持导入Parquet格式 | P1 | Yanqiong Dong | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6689988491) |
| [NA] Explorer database info display compress ratio & disk usage | P3 | Arun | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6693108589) |
| [积成电子]未配置 ssl 时出现明文密码传输，应改进 | P3 | Daniel Clow | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6698916763) |
| [瑞幸咖啡]kafka数据采集支持通过过滤条件存储到不同超级表中 | P2 | Jack Dong | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6718901244) |
| 【神东集团】KH数据源同步中点位自动更新 | P1 | Jack Dong | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665149034) |
| [东航私有云] 3.3.6.25 taos-explorer 里面一些超链接 点击报page not found | P5 | Edward Cheng | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6735524796) |
| [西盟昌裕糖业]opcua 连通性检测增加 failover | P3 | Zee Lv | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6751321432) |
| [上海电气]explorer 数据写入页面加载缓慢 | P3 | Zee Lv | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6751436539) |
| [售前] TDlite 授权支持 taosX 部分连接器 | P3 | Bo Xiao | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6850123508) |
| [安克] go 支持多端点 failover | P3 | Raistlin Chen | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6858109965) |
| 【东方电子】支持在 TSDB 获取 offset 失败情况下正常生成备份点 | P3 | Perry Lv | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6865524240) |
| [常德卷烟厂]IDMP从OPC UA导入数据资产失败 | P2 | Richard Li | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665149034) |
| [南网储能] jdbc-driver 在 TAOSO-WS 下，varcharAsString 应为 true | P3 | Raistlin Chen | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6922990934) |
| [常德卷烟厂]建议IDMP、TDgpt提供离线安装包，应对工控网无法联网现状 | P3 | ​Richard Li | Done | Platform | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6481198091?node=27778027) |
| 【瑞幸咖啡】说明taoskeeper上传的promethues的metrics指标与grafana中默认报警规则使用的字段对应关系 | P1 | Jack Dong | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6622579928?node=27777982) |
| 【神东集团】KH迁移过程中结束时间为空时，应表示一直进行迁移 | P3 | Jack Dong | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6600045300?node=27777982) |
| [上海电气]taosx 任务支持查看功能 | P3 | Zee Lv | Canceled | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6751365085?node=27777982) |
| 【瑞幸咖啡】kafka数据采集支持通过过滤条件存储到不同超级表中 | P2 | Zee Lv | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6718901244?node=27777982) |
| 【大庆油田】非root用户对于explorer中taosx写入任务权限管理 | P2 | Jack Dong | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6684826126?node=27777982) |
| 【世窗信息】influxdb迁移到TDengine时需根据原有tag值定义表名 | P2 | Zach Wang | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6660065587?node=27777982) |

#### 3.2.3 平台

| 名称 | 优先级 | 报告人 | 状态 | 说明 | 链接 |
| --- | --- | --- | --- | --- | --- |
| 自动化巡检工具优化 | P3 | Xu Wang | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/job/detail/6659710381) |
| 中英文官网服务器升级和服务迁移 | P3 | Xu Wang | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/job/detail/6603629392) |
| 飞书项目“最终用户”从销售易中动态模糊检索 | P3 | Bo Xiao | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6590722532) |

### 3.3 IDMP

#### 3.3.1 引擎

| 名称 | 优先级 | 报告人 | 状态 | 说明 | 链接 |
| --- | --- | --- | --- | --- | --- |
| [售前][广汽] 流计算事件窗口，满足条件除时长外，还增加记录条数 | P1 | Jeff Tao | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6589462594?node=27777982) |
| [售前][信通院IDMP测试] 时序模型管理和时序数据预处理 测试项临时工作 | P3 | Zane Chen | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6772913657?node=26750033) |
| [上海电气]idmp 元素的属性页面刷新很慢 | P1 | Zee Lv | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6710694996?node=26750033) |
| [IDMP] 给定的 SQL 集合提供易于定位的明确错误信息 | P3 | Yaqiang Li | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659988199?node=26746283) |
| [IDMP] 支持 ANY/SOME/ALL/EXISTS 运算符 | P3 | Wei Pan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659773695?node=26746283) |
| [IDMP] 支持不带 FROM 的标量子查询 | P3 | Wei Pan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6641525627?node=26750033) |
| [售前][一汽红旗] 流计算中能够支持子查询过滤条件 | P1 | Tyler Liu | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6598056767?node=26746283) |
| [售前][瑞幸咖啡] 数据订阅支持虚拟表的元数据变更 | P3 | Kane Kuang | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6593807450?node=26746283) |
| [售前][广汽] 流计算和批查询的事件、状态窗口 true_for 判断支持持续时间与持续条数双条件 | P1 | Jeff Tao | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6589462594?node=26746283) |
| [IDMP] [IDMP] 虚拟表和源表的引用校验 | P1 | Yaqiang Li | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6589101088?node=26746283) |
| [IDMP] 流计算在源子表/虚拟子表长时间没有新数据写入时，也能提供发送通知的功能 | P1 | Yaqiang Li | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6572489317?node=26746283) |
| [售前][陕西中烟] 提升虚拟表按批次查询性能 | P3 | Joey Sima | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6548485194?node=26746283) |
| [规划] 外部窗口 | P3 | Simon Guan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6550634959?node=26746283) |
| [IDMP] 批量更新、增加和删除虚拟子表的标签和标签值 | P3 | Yaqiang Li | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6491345559?node=26746283) |
| 流计算虚拟超级表触发支持新增、删除子表、子表 tag 值修改、修改列映射关系 | P2 | Wei Pan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6491267649?node=26746283) |
| [售前][陕西中烟] 支持按自然周、月、季、年的定时计算 | P1 | Abraham Liu | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6490755304?node=26746283) |
| [售前][陕西中烟] 分析产生的新属性，可以作为输入继续进行分析 | P1 | Abraham Liu | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6490870739?node=26746283) |
| [规划] 虚拟表查询性能优化 | P3 | Joey Sima | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6483450778?node=26746283) |
| [IDMP] 虚拟表和源表的引用校验 | P1 | Yaqiang Li | Done | TSDB-FromIDMP | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6589101088?node=27777982) |
| [IDMP] 支持 ANY/SOME/ALL/EXISTS/NOT EXISTS 运算符 | P3 | Wei Pan | Done | TSDB-FromIDMP | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659773695?node=27777982) |

#### 3.3.2 工具

| 名称 | 优先级 | 报告人 | 状态 | 说明 | 链接 |
| --- | --- | --- | --- | --- | --- |

#### 3.3.3 平台

| 名称 | 优先级 | 报告人 | 状态 | 说明 | 链接 |
| --- | --- | --- | --- | --- | --- |
| IDMP Code Coverage 监控 | P3 | Xu Wang | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/job/detail/6660059913) |
| 提供 IDMP Staging 环境 | P3 | Bo Xiao | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/job/detail/6659811256) |
| 支持自动打包测试发布 IDMP SDK | P3 | Bo Ding | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/job/detail/6662825733) |
| IDMP CD 自动化 -  任何人可按需发版、按需运行指定测试项 |  | Bo Xiao | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/job/detail/6660036669) |
| IDMP 1.0.10.3 使用安装包安装时能够跳过联网下载步骤 | P3 | Tyler Liu | Done | Platform | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6697203036?node=27778027) |
| IDMP 启动脚本中增加内存检查 | P3 | Xu Wang | Done | Platform | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6487585751?node=27778027) |
| 生成TDgpt在Windows上的安装包，并测试运行 | P3 | Jeff Tao | Done | Platform | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6856808946?node=27778027) |
| IDMP CI 增加 AI comments 都已经解决的检查，不过不用进行后面的任务 | P2 | Yaqiang Li | Done | Platform | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6839382223?node=27778027) |
| 期望 idmp 在部署时自动创建 xnode | P2 | Xiang Gu | Done | Platform | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6713295921?node=27778027) |


### 3.4 规划

#### 3.4.1 引擎

| 名称 | 优先级 | 报告人 | 状态 | 说明 | 链接 |
| --- | --- | --- | --- | --- | --- |
| [规划] 添加独立参数控制权限控制行为 | P3 | Beryl Bao | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6891620198?node=27329741) |
| [规划] 优化虚拟表列多时的查询性能 | P3 | Joey Sima | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6862634689?node=27329741) |
| [规划] TDgpt 支持 Windows | P3 | Simon Guan | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6861933885?node=27329741) |
| [规划] 制定并实施 Windows 下 Coredump 文件生成策略 | P3 | Simon Guan | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6861895851?node=27329741) |
| [规划] 旧的流计算代码清理 | P3 | Mark Wang | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6857889519?node=27329741) |
| [规划] 流计算支持 Windows | P3 | Haoran Chen | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6857094454?node=27329741) |
| [规划] v3.4 授权语法兼容 v3.3 版本 | P3 | Leo Huo | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6841578151?node=27329741) |
| [规划] 支持关闭密码过期、强密码、密码轮换等策略 | P3 | Leo Huo | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6841566765?node=27329741) |
| [规划] 虚拟表引用物理表列数较多时的查询性能 | P3 | Joey Sima | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6755544717?node=27329741) |
| [安全可靠测评] 支持更新密钥过期时间和过期策略 | P3 | Beryl Bao | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6749222965?node=27329741) |
| [规划] 提升虚拟表查询含 tbname 列时的性能 | P3 | Joey Sima | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6747163479?node=27329741) |
| [规划] 优化虚拟超级表状态窗口的查询性能 | P3 | Joey Sima | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6694539984?node=27329741) |
| [规划] 流计算支持 IN 子查询 | P3 | Wei Pan | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6692120342?node=27329741) |
| [安全可靠测评] 防 SQL 注入：防火墙机制 | P2 | Kane Kuang | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6670404791?node=27329741) |
| [安全可靠测评] taosc/taosd 防拒绝服务攻击 | P1 | Wei Pan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6670390631?node=27329741) |
| [安全可靠测评] taosc/taosd 防溢出攻击 | P1 | Wei Pan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6670169846?node=27329741) |
| [规划] NULL 值比较结果修正 | P3 | Wei Pan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6668153717?node=27329741) |
| [规划] client monitor 上报 slow log 重构 | P3 | Mark Wang | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665488038?node=27329741) |
| [安全可靠测评] 整理仓库代码以提高自研率 | P3 | Simon Guan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659850619?node=27329741) |
| [安全可靠测评] 安全漏洞修复 | P3 | Simon Guan | Canceled |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659822076?node=27329741) |
| [安全可靠测评] 数据订阅支持的 token登录 | P3 | Xuefeng Tan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659792966?node=27329741) |
| [等保四级] root 用户使用默认密码登录后，强制其修改密码 | P3 | Beryl Bao | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6641469804?node=27329741) |
| [等保四级] 审计信息不经过 taoskeeper 记录 | P3 | Beryl Bao | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6641435300?node=27329741) |
| [等保四级] 支持敏感数据删除后的强制覆盖 | P3 | Beryl Bao | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6641346408?node=27329741) |
| [安全可靠测评] 列权限生效 | P3 | Beryl Bao | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6640315568?node=27329741) |
| [安全可靠测评] 完善存储加密功能 | P3 | Beryl Bao | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6640296081?node=27329741) |
| [安全可靠测评] 增加 token 相关的通知机制 | P3 | Beryl Bao | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6640223025?node=27329741) |
| [安全可靠测评] 支持用户修改权限控制 | P3 | Beryl Bao | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6640208544?node=27329741) |
| [安全可靠测评] 完善视图和审计相关的权限控制 | P3 | Beryl Bao | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6640186564?node=27329741) |
| [安全可靠测评] 支持从旧的加密集群升级到新的版本 | P3 | Beryl Bao | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6640162570?node=27329741) |
| [安全可靠测评] create totp 时返回结果集 | P3 | Beryl Bao | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6640162509?node=27329741) |
| [安全可靠测评] 权限控制的兼容性处理 | P3 | Beryl Bao | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6640076601?node=27329741) |
| [安全可靠测评] 禁止篡改配置文件 | P3 | Beryl Bao | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6640062620?node=27329741) |
| [规划] 子查询做主键过滤条件时的性能优化 | P3 | Wei Pan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6617004723?node=27329741) |
| [规划] explain analyze 算子显示的执行时间 | P4 | Wei Pan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6548173402?node=27329741) |
| [规划] 优化 explain 输出结果 | P3 | Wei Pan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6545510969?node=27329741) |
| [规划] 支持 MySQL 的非相关标量子查询 | P3 | Wei Pan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6545510969?node=27329741) |
| [规划] 流计算多分组批量计算 | P3 | Wei Pan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6491136498?node=27329741) |
| [规划] 数据修复工具 | P3 | Simon Guan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6469793274?node=27329741) |
| 流计算支持 IN 子查询 | P3 | Wei Pan | Done | TSDB-Plan | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6692120342?node=27777982) |
| taosc/taosd 防溢出攻击 | P1 | Wei Pan | Done | Security, TSDB-Plan | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6670169846?node=27777982) |
| taosc/taosd 防拒绝服务攻击 | P1 | Wei Pan | Done | Security, TSDB-Plan | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6670390631?node=27777982) |
| 制定并实施 Windows 下 Coredump 文件生成策略 | P3 | Simon Guan | Done | TSDB-Plan | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6861895851?node=27777982) |
| 添加独立参数控制权限控制行为 | P3 | Beryl Bao | Done | TSDB-Plan | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6891620198?node=27777982) |
| taosd: 添加参数，启用时 grant all on <>.* 行为与 3.3 保持一致 | P3 | Leo Huo | Done | TSDB-Plan | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6841578151?node=27777982) |
| [安全可靠测评] 完善权限控制 | P3 | Beryl Bao | Done | Security, TSDB-Plan | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6640186564?node=27777982) |
| NULL 值比较结果修正 | P3 | Wei Pan | Done | TSDB-Plan | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6668153717?node=27777982) |
| [产品] 支持 MySQL 的非相关标量子查询 | P3 | Wei Pan | Done | TSDB-Plan | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6542129231?node=27777982) |
| 旧的流计算代码清理 | P3 | Mark Wang | Done | TSDB-Plan | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6857889519?node=27777982) |
| client monitor 上报 slow log 重构优化 | P3 | Mark Wang | Done | TSDB-Plan | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665488038?node=27777982) |
| 虚拟超级表窗口查询优化 | P3 | Joey Sima | Done | TSDB-Plan | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6862634689?node=27777982) |
| 优化虚拟表列多时的查询性能 | P3 | Joey Sima | Done | TSDB-Plan | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6755544717?node=27777982) |
| 虚拟表 condition 下推 | P3 | Joey Sima | Done | TSDB-Plan | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6747163479?node=27777982) |
| 虚拟超级表 state_window 查询优化 | P3 | Joey Sima | Done | TSDB-Plan | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6694539984?node=27777982) |
| [产品] 流计算支持 Windows | P2 | Haoran Chen | Done | TSDB-Plan | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6857094454?node=27777982) |
| [安可] 防 SQL 注入：防火墙机制 | P2 | Kane Kuang | Done | Security, TSDB-Plan | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6670404791?node=27777982) |
| taosd 默认关闭安全功能 | P3 | Leo Huo | Done | TSDB-Plan | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6841566765?node=27777982) |
| [产品] 优化 explain 输出结果 | P3 | Wei Pan | Done | TSDB-Plan | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6545510969?node=27777982) |

#### 3.4.2 工具

| 名称 | 优先级 | 报告人 | 状态 | 说明 | 链接 |
| --- | --- | --- | --- | --- | --- |
| [规划] License Center | P1 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665336277) |
| [安全] Explorer 安全加固 | P2 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6658919461) |
| [安全] Explorer：TOTP 认证 | P2 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6506023136) |
| [安全] Explorer支持TOKEN认证 | P2 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6658975929) |
| [安全] 连接器安全加固 | P2 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659285650) |
| [安全] taosX 权限管理 | P2 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6658956251) |
| [安全] 修复 JDBC sonar 检查的错误和安全问题 | P3 | Yanjie She | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6482039483) |
| [安全] 连接器安全开发 - 指南文档 | P3 | Yanjie She | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6658900952) |
| [安全] taosKeeper 密码信息脱敏处理 | P3 | Ethan Guo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6600039687) |
| [产品] XNODE: CREATE TASK ... 添加 created_by, task_type 字段 | P3 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659009378) |
| [连接器] jdbc WebSocket 参数绑定支持 decimal 类型和 blob 类型 | P3 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6662931308) |
| [文档] jmeter 测试查询方案 | P2 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6660015604) |
| [连接器] nodejs 支持上报连接器类型和版本，方便交付排查版本兼容性 | P3 | Yanjie She | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6666017184) |
| [产品] taosx 高可用支持双活 | P3 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6646286429) |
| [产品] taosx 任务运行不受密码修改影响 | P3 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6658967000) |
| [产品] xnoded 支持 Windows | P3 | Leo Huo | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6646294817) |
| [产品] taosgen: 社区新增数据源简化修改范围 | P3 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659799103) |
| [产品] taosgen 参数管理/数据结构框架与业务分离 | P3 | Cris Pei | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6657217599) |
| [安全] C WebSocket 连接器密码信息脱敏处理 | P3 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6599885679) |
| [产品] taosx opcua 数据源优化 | P3 | Leo Huo | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6753325311) |
| [规划] taosdump支持 blob 数据类型的导入导出 | P3 | Yanjie She | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6835134117) |
| [规划] taosdump支持 stmt2 写入方式 | P3 | Yanjie She | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6834892123) |
| [安可] TSDB SQL Fuzzing Test | P3 | Leo Huo | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6839350638) |
| [规划] 日志默认保存时长变更 | P3 | Leo Huo | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6837004097) |
| [AI] Add Skill for taosgen config generation and run | P1 | Leo Huo | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6851421856) |
| [AI] 开发支持 IDMP 的 MCP server | P1 | Leo Huo | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6737104419) |
| taosx-opc points 返回 ClassNode=Object 的节点 | P2 | Zhiyu Yang | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6755546398) |
| [规划] taosX 超级表模板名和子表名支持表达式计算 | P2 | Leo Huo | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6757417051) |
| [安全] JDBC 连接器添加依赖库安全检查和 LICENSE 检查 | P3 | Yanjie She | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6764349382) |
| [安全] Flink 连接器添加依赖库安全检查和 LICENSE 检查 | P3 | Yanjie She | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6765990533) |
| [安全] 使用 Conan 管理 C 依赖并生成 SBOM | P2 | Leo Huo | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6769684934) |
| [安全] kafka connect 添加依赖库安全检查和 LICENSE 检查 | P3 | Yanjie She | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6785941906) |
| [安全] grafanaplugin 添加依赖库安全检查和 LICENSE 检查 | P3 | Yanjie She | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6786957442) |
| [安全] taoskeeper 依赖升级解决漏洞问题 | P3 | Yanjie She | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6790719104) |
| [安全] Rust/Python/Node.js 连接器 CI 添加 SBOM 生成 | P3 | Yanjie She | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6796130131) |
| [规划] Nodejs 连接器建立连接支持 token | P3 | Leo Huo | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6829477207) |
| [规划] python 连接器 sqlaichemy WebSocket 也需要依赖 libtaos，需要优化 | P3 | Yanjie She | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6893784094) |
| [规划] MQTT 数据源 Topic 支持任意字符 | P3 | Astro Yan | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6747091907) |
| nodejs 连接器订阅支持 token | P3 | Yanjie She | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6737140458) |
| C# 连接器 WebSocket 订阅支持 token | P3 | Yanjie She | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6735000446) |
| Go WebSocket 订阅支持 token | P3 | Yanjie She | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6735513765) |
| python 连接器订阅支持 token | P3 | Yanjie She | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6735486234) |
| rust 连接器订阅支持 token | P3 | Yanjie She | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6735261785) |
| taosadapter 订阅支持 token | P3 | Yanjie She | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6734823388) |
| JDBC WebSocket 订阅支持 token | P3 | Yanjie She | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6735523718) |
| taosdump支持decimal数据类型的导入导出 | P3 | Hui Li | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6622691504?node=27777982) |

#### 3.4.3 平台

| 名称 | 优先级 | 报告人 | 状态 | 说明 | 链接 |
| --- | --- | --- | --- | --- | --- |
| 建立 Github 例行维护清理工作办法 | P3 | Bo Xiao | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/job/detail/6659951656) |
| 迁移Jira中除TX项目外未关闭问题 | P3 | Bo Xiao | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/job/detail/6659859717) |
| 统一公司操作系统：基础镜像、公司官网 | P3 | Bo Xiao | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/job/detail/6659987156) |
| 清理 Github 仓库无用、重复代码及文件 | P3 | Bo Xiao | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/job/detail/6659711335) |
| 梳理 7*24 运行的测试并查漏补缺 | P3 | Xu Wang | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/job/detail/6659802448) |
| 建立云服务运维相关需求和缺陷的反馈机制 | P3 | Xu Wang | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/job/detail/6660054317) |
| 安装包需包含 taosk | P2 | Beryl Bao | Done | Platform | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6790431463?node=27778027) |
| 离线一键部署TSDB优化需求 | P3 | Kian Wang | Done | Platform | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6740841770?node=27778027) |
| 主线版本需要常态化集成测试与长稳测试 | P3 | Steven Zhang | Done | Platform | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6668127609?node=27778027) |
| TSDB自动化安装工具v1.0版本 | P3 | Kian Wang | Done | Platform | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6668135081?node=27778027) |
| TSDB Playbook 支持 taosadapter/taosx 等组件的自定义配置 | P3 | Jayden Jia | Done | Platform | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659186967?node=27778027) |
| 官网添加 AI 问答功能 | P3 | Jeff Tao | Done | Platform | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6487644306?node=27778027) |

## 4. 风险管理表

| 编号 | 风险分类 | 风险描述 | 提交人 | 提交日期 | 发生阶段 | 责任人 | 可能性 | 风险级别 | 管理策略 | 应对措施描述 | 风险状态 | 状态更新日 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 开发风险 | 扫描出的安全漏洞较多，影响进度 | 关胜亮 | 2026-01-21 | 开发阶段 | 关胜亮 | 高 | 中 | 风险减轻 | 按优先级分类 | 已解决 | 2026-04-13 |
| 2 | 开发风险 | 安可提测需要补充部分文档，小规模影响开发进度 | 关胜亮 | 2026-01-21 | 需求与设计 | 关胜亮 | 高 | 中 | 风险减轻 | 加班完成 | 已完成 | 2026-02-25 |
| 3 | 开发风险 | 春节期间请年假人数较多，影响进度 | 霍琳贺 | 2026-2-25 | 开发阶段 | 关胜亮 | 高 | 中 | 风险减轻 | 和业务部门沟通，移除优先级低的任务 | 已解决 | 2026-04-13 |

## 5. 月度总结

### 5.1 2026年1月总结

1. 项目进度总述
   - 本月项目整体进展正常，引擎、工具、平台三大模块均按计划推进，识别两个风险，考虑了应对措施。
   - 本月安全方面增加六项功能
2. 项目主要成果
   - 支持不带 FROM 字句的子查询
   - 提升子查询作为主键过滤条件时的性能
   - 优化 interp 的fill(prev/next/near/linear) 填充行为，支持填充前 / 后非 NULL 值
   - 其他功能在编写需求、设计文档
3. 本月需求变更
   - 无新增需求
4. 本月缺陷说明
   - 新发布的功能，未识别明显缺陷
5. 下月工作计划
   - 按照项目计划开展
   - 重点放在安全可靠性提升、查询能力提升两个方面

### 5.2 2026年2月总结

1. 项目进度总述
   - 本月项目整体进展正常，引擎、工具、平台三大模块均按计划推进
   - 本月识别一个风险，考虑了应对措施
2. 项目主要成果
   - 支持 IN 运算符中使用非相关子查询
   - 同时支持 IPv4 & IPv6 协议栈
   - 优化虚拟超级表的查询性能
   - 流计算和批查询的事件、状态窗口 true_for 判断支持持续时间与持续条数双条件
3. 本月需求变更
   - 引擎新增 11 项功能，工具新增 6 项功能
4. 本月缺陷说明
   - 新发布的功能，未识别明显缺陷
5. 下月工作计划
   - 按照项目计划开展
   - 部分功能尚未开展，需要调整优先级，和业务部门沟通移除优先级低的任务

### 5.3 2026年3月总结

1. 项目进度总述
   - 本月项目整体进展正常，引擎、工具、平台三大模块均按计划推进
   - 本月无新增大需求
2. 项目主要成果
   - 新增对 ANY/SOME/ALL/EXISTS/NOT EXISTS 运算符的支持
   - TDgpt 新增对多变量异常检测功能的支持
3. 本月需求变更
   - 本月新增 Windows 适配工作，见[工作计划](https://taosdata.feishu.cn/wiki/CDTzwRqlTi73tTkovQvcoFXqnmb)
4. 本月缺陷说明
   - 新发布的功能，未识别明显缺陷
5. 下月工作计划
   - 按照项目计划开展
   - 部分功能尚未开展，需要调整优先级，和业务部门沟通移除优先级低的任务

### 5.4 2026年4月总结

1. 项目进度总述
   - v3.4.1 已于 2026 年 3 月底发版，4 月份进入验证和收尾阶段
   - 大部分任务已完成或进入验证阶段，少量任务延续到 v3.4.2 处理
2. 项目主要成果
   - 安全漏洞修复工作取消（评估后调整策略，转为持续治理）
   - 外部窗口、事件窗口增强等核心功能进入验证阶段
   - 流计算多分组批量计算、虚拟表查询性能优化等已完成
   - explain analyze 算子执行时间显示已完成
   - 敏感数据删除后强制覆盖已完成
   - 多项 Releasing 状态任务推进到 Verifying
3. 本月需求变更
   - 无新增需求
4. 本月缺陷说明
   - 验证阶段发现的问题正在修复中
5. 下月工作计划
   - 完成所有 Verifying 状态任务的验收
   - 遗留任务转入 v3.4.2 跟踪
