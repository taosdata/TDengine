# TSDB v3.4.2 项目进度跟踪表

## 1. 修订记录

| 更新日期 | 更新人 | 主要修改内容 |
| --- | --- | --- |
| 2026-4-13 | 关胜亮 | 第一次进度跟踪 |
| 2026-5-26 | 关胜亮 | 第二次进度跟踪 |

## 2. 项目进度概览

1. 整体进度：正常
   - 业务：总计 49 项，已完成 6 项，未完成 43 项（88%）
   - IDMP：总计 24 项，已完成 3 项，未完成 21 项（88%）
   - 规划：总计 89 项，已完成 38 项，未完成 51 项（57%）
   - 海外：总计 8 项，已完成 1 项，未完成 7 项（88%）
   - 平台：总计 15 项，已完成 6 项，未完成 9 项（60%）
2. 范围状态：
   - 业务：新增任务  7  个，移出任务  3  个
   - IDMP：新增任务  10  个，移出任务  2  个
   - 规划：新增任务  19  个，移出任务  30  个
   - 海外：新增任务  6  个，移出任务  1  个
   - 平台：新增任务  4  个，移出任务  5  个
3. 主要风险：
   - 业务：无
   - IDMP：无
   - 规划：无
   - 海外：无
   - 平台：无

## 3. 工作分解结构与进度跟踪

### 3.1 亮点功能


- 虚拟表：引用其他虚拟表的列，引用标签列，虚拟表继承，虚拟表变更支持事务，支持不同精度数据库
- 流计算：叶子节点层级汇总，多级子事件，优化分组计算和历史计算等场- 景的性能
- 查询：联邦查询，自然周/月/季/年，时区改造，状态窗口多状态列
- 函数：窗口函数与 OVER 子句，FFT、SLEEP、REGEXP 等函数
- 集群：数据快速恢复能力（不停机和停机场景），CPU 亲和性绑定
- 工具：License Server，taosX 负载均衡、OPC AE 及其他优化，Explorer 补充 DataIn/DataOut/Xnode 配置
- 连接器能力补齐：借助 Adapter 的高可用，STMT2 ，BLOB 和 Decimal 支持，传输压缩，压测工具等
- 其他：构建 TSDB 统一仓库，实现开发、CI、打包的完整迁移

### 3.2 业务

| 工作项ID | 名称 | 优先级 | 报告人 | 状态 | 说明 | 链接 |
| --- | --- | --- | --- | --- | --- | --- |
| 6925549512 | [售前][中石油] 支持不限制国产操作系统和 CPU 的社区版 | P3 | Guang Li | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6925549512?node=27777982) |
| 6977843602 | [交付][杨凌美畅] asof join 查询优化 | P2 | Kian Wang | Verifying | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6977843602?node=27777982) |
| 6772914536 | [交付][博创联动] 子查询数据扫描时，取子查询与外层时间范围的交集进行扫描 | P3 | Kian Wang | Releasing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6772914536?node=27777982) |
| 6511294180 | [交付][拾贝云] Greatest/Least 与 MySQL 对齐，支持忽略 NULL | P3 | Raistlin Chen | Releasing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6511294180?node=27777982) |
| 6507136288 | [交付] 查询函数 Sleep(duration) 用于超时问题模拟 | P3 | Raistlin Chen | Releasing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6507136288?node=27777982) |
| 6507051705 | [交付][海澜智云] 社区版在执行企业版专有功能时有报错提醒 | P3 | Tyler Liu | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6507051705?node=27777982) |
| 6984169049 | [交付] 提升不停机场景下单 vnode 数据快速恢复能力 | P3 | Zachary Xiao | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6984169049?node=27777982) |
| 6671837225 | [交付][新奥新智] 大量查询不存在表导致 mnode CPU 高 | P3 | Zee Lv | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6671837225?node=27777982) |
| 6589436029 | [交付][河北电力] 优化频繁 use db 导致 mnode read 线程压力过大 | P3 | Zee Lv | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6589436029?node=27777982) |
| 6511301953 | [交付] Audit 库可以记录客户端 IP | P3 | Hui Li | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6511301953?node=27777982) |
| 6506113427 | [交付][南网储能-拾贝云] 节点启动过程中应用需要正常使用不报错 | P3 | Raistlin Chen | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6506113427?node=27777982) |
| 6507042141 | [交付][三峡]优化高负载情况下选主行为（可行性方案） | P3 | Yanqiong Dong | Verifying |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6507042141?node=27777982) |
| 6751085739 | [长飞光纤]AVEVA Historian数据源任务数据实时同步优化 | P3 | Kian Wang | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6751085739?node=27777982) |
| 6751395113 | [沃太能源]taosx支持指定表的备份恢复 | P3 | Yanqiong Dong | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6751395113?node=27777982) |
| 6923036499 | [交付][领储宇能] ServerPort 修改后订阅内部记录应同步更新 | P3 | Kian Wang | Releasing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6923036499?node=27777982) |
| 6491115004 | [交付]  query 类型的 topic，被订阅使用的表可以被删除，删除重建后需重新 reload topic 即可 | P3 | Simon Guan | Verifying |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6491115004?node=27777982) |
| 6490727766 | [交付][中国电建] 副本变更不影响数据订阅 | P3 | Tyler Liu | Verifying |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6490727766?node=27777982) |
| 6928677802 | [赛力斯] taosx 支持创建 400+ 个字段的数据写入任务 | P3 | Raistlin Chen | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6928677802?node=27777982) |
| 6921874285 | [售前][川威] TSDB Lite 的 Explorer 数据写入选项，仅保留支持的选项：OPC、MQTT 等 | P3 | Bo Xiao | Verifying |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6921874285?node=27777982) |
| 6662862362 | [河北电力新一代调度项目] taosX 归档的 archive 文件读取工具 | P3 | Tyler Liu | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6662862362?node=27777982) |
| 6622709348 | [售前][上海电气中央研究院] 扩展 taosX 解析功能 | P3 | Tyler Liu | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6622709348?node=27777982) |
| 6622596851 | [河北电力新一代调度项目]explorer 增加 taosx命令行方式的-T 参数 | P3 | Zee Lv | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6622596851?node=27777982) |
| 6617550569 | [售前][上海电气中央研究院] MQTT 数据源能够获取报文头信息 | P3 | Tyler Liu | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6617550569?node=27777982) |
| 6617536550 | [售前][上海电气中央研究院] 希望 taosX 能够主动控制资源占用 | P3 | Tyler Liu | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6617536550?node=27777982) |
| 6617617575 | [售前][上海电气中央研究院] 通过 taosX 上传大量 csv 文件并导入的行为改进 | P3 | Tyler Liu | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6617617575?node=27777982) |
| 6988894856 | [交付] 支持监控 restore snapshot 的进度 | P3 | Beryl Bao | Verifying | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6988894856?node=27777982) |
| 6977811661 | [售前][招商智科] 断电宕机后确保 WAL 可用性（自动修复 WAL 文件） | P3 | Jack Dong | Processing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6977811661?node=27777982) |
| 6930507762 | [交付][树根科技] restore 命令支持指定 vgroup id 恢复 | P3 | Zachary Xiao | Verifying |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6930507762?node=27777982) |
| 6914731989 | [社区] 订阅功能开源版本可以修改 topic 数量 | P3 | Steven Zhang | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6914731989?node=27777982) |
| 6914845502 | [交付] 全局参数修改专用工具 | P3 | Steven Zhang | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6914845502?node=27777982) |
| 6670886193 | [交付][河北电力] 希望增加日志里重要 ERROR 告警 | P3 | Zee Lv | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6670886193?node=27777982) |
| 6672111997 | [交付] 规范化数据库重要操作开始结束标志信息输出 | P3 | Steven Zhang | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6672111997?node=27777982) |
| 6599966995 | [售前][南网 CEP] show local/dnode variables增加一参数列：是否需要重启生效、当前参数未生效 | P3 | Bo Xiao | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6599966995?node=27777982) |
| 6574020760 | [交付][天合富家] 增加缓存强制刷新功能 | P3 | Yanqiong Dong | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6574020760?node=27777982) |
| 6491198599 | [交付] 支持指定列进行最新数据缓存 | P3 | Beryl Bao | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6491198599?node=27777982) |
| 6923487183 | [售前][冠德] 流计算 external window 和多分组优化支持 JOIN 语句 | P3 | Jack Dong | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6923487183?node=27777982) |
| 6670762934 | 支持以COUNT_WINDOW(N, 1) 为触发的流进行重算 | P3 | Kane Kuang | Processing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6670762934?node=27777982) |
| 6513771567 | [售前][三峡集团] 支持 ROW_NUMBER() OVER() 函数 | P2 | Jack Dong | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6513771567?node=27777982) |
| 6510119993 | [售前][上科信息] 分组查询 partition by 支持组内排序 | P3 | ​Richard Li | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6510119993?node=27777982) |
| 6507156244 | [售前][陕西中烟] 支持排名函数，如 rank() | P3 | Abraham Liu | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6507156244?node=27777982) |
| 6955602052 | [规划] 数据修复工具支持以 copy 文件的方式恢复数据 | P3 | Zachary Xiao | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6955602052?node=27777982) |
| 6952596147 | [交付][郑煤机] trim 操作和 ssmigrate 事务之间冲突 | P3 | Kian Wang | Verifying |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6952596147?node=27777982) |
| 6861626512 | [交付][领储宇能] k8s 部署时无法获得正常的容器资源信息 | P3 | Steven Zhang | Verifying |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6861626512?node=27777982) |
| 6849686611 | [交付][疆海] TO_ISO8601 支持夏令时的转化 | P3 | Raistlin Chen | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6849686611?node=27777982) |
| 6511323203 | [售前][神东集团] 单副本变三副本支持共享存储 | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6511323203?node=27777982) |
| 6506025858 | [交付] show table distribute 格式化显示，便于过滤 | P3 | Raistlin Chen | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6506025858?node=27777982) |
| 6491037879 | [交付][爱动] 支持分钟级别的时区 | P3 | Kian Wang | Verifying |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6491037879?node=27777982) |
| 6996017077 | [售前][一汽] state_window 支持逻辑运算符 | P3 | ​Richard Li | Reviewing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6996017077?node=27777982) |
| 6514083018 | [售前][南网数研院][南瑞电网] 提升 Interp 查询性能 | P4 | Bo Xiao | Blocked |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6514083018?node=27777982) |
| 6510958760 | [交付][中冶京诚] insert into file 错误信息优化提升 | P3 | Steven Zhang | Canceled | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6510958760?node=28816616) |
| 6671971734 | [售前][上海电气中央研究院] 虚拟表支持引用不同数据库精度的表 | P3 | Tyler Liu | New | 删除，移至IDMP | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6671971734?node=28816616) |
| 6918805473 | [交付][郑煤机] 共享存储支持配置华为 obs | P3 | Kian Wang | Canceled | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6918805473?node=28816616) |

### 3.3 IDMP

| 工作项ID | 名称 | 优先级 | 报告人 | 状态 | 说明 | 链接 |
| --- | --- | --- | --- | --- | --- | --- |
| 6968250338 | [IDMP] 支持 REGEXP_EXTRACT 函数（正则表达式提取） | P3 | Simon Guan | Releasing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6968250338?node=27777982) |
| 6927835648 | [IDMP] 支持 FFT | P3 | Jeff Tao | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6927835648?node=27777982) |
| 6511919698 | [售前][社区] 支持在函数中使用 Distinct 关键字 | P3 | Kane Kuang | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6511919698?node=27777982) |
| 6984175318 | [IDMP] 源子表的标签值修改后能够同步更新虚拟子表的标签值 | P2 | Yaqiang Li | Reviewing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6984175318?node=27777982) |
| 6671971734 | [售前][上海电气中央研究院] 虚拟表支持引用不同数据库精度的表 | P3 | Tyler Liu | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6671971734?node=27777982) |
| 6589380578 | [IDMP][北美] 虚拟表支持引用虚拟表 | P1 | Yaqiang Li | Reviewing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6589380578?node=27777982) |
| 6492554061 | [规划] 虚拟表继承 | P2 | Simon Guan | Processing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6492554061?node=27777982) |
| 6986555668 | taosdump 支持导出 stream 语句 | P3 | Simon Guan | Releasing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6986555668?node=27777982) |
| 6927058167 | [IDMP] TSDB 默认授权应不因 machine id 变化而 revoke 授权 | P2 | Bo Xiao | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6927058167?node=27777982) |
| 6659965197 | [IDMP] 元数据更新支持事务（虚拟表变更） | P3 | Yaqiang Li | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659965197?node=27777982) |
| 6572940279 | [IDMP] 删除数据库不加 force 应该告知客户真实原因 | P3 | Yaqiang Li | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6572940279?node=27777982) |
| 6570504710 | [IDMP] 支持修改虚拟超级表列名 | P4 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6570504710?node=27777982) |
| 6590611316 | [IDMP] 数据备份支持备份虚拟表和流计算 | P1 | Yaqiang Li | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6590611316?node=27777982) |
| 6993473771 | [性能优化] taosd 存在大量的超级表时，taosd 偶发卡死 | P2 | Beryl Bao | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6993473771?node=27777982) |
| 6986382331 | stream 支持tag-ref 和虚拟表引用 | P3 | Yihao Deng-8076 | Testing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6986382331?node=27777982) |
| 6659773700 | [IDMP] 放宽窗口查询限制（不仅是聚合） | P3 | Yaqiang Li | Releasing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659773700?node=27777982) |
| 6598098782 | [IDMP] CSUM 支持在窗口查询中使用 | P3 | Yaqiang Li | Releasing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6598098782?node=27777982) |
| 6987137653 | 目前的子事件的通知消息没有关联父事件和子事件的关系 | P2 | Yaqiang Li | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6987137653?node=27777982) |
| 6979200215 | [IDMP] 流计算支持基于超级表标签，实现叶子节点逐级汇总计算 | P3 | Simon Guan | Testing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6979200215?node=27777982) |
| 6952737120 | [IDMP] 流计算支持多级子事件 | P3 | Kane Kuang | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6952737120?node=27777982) |
| 6549502576 | [IDMP] 支持窗口函数和 OVER 子句 | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6549502576?node=27777982) |
| 6592836563 | [IDMP][一汽红旗] 事件窗口的结束条件也能够设置持续时间判断 | P3 | Tyler Liu | Reviewing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6592836563?node=27777982) |
| 6927171373 | [IDMP] 状态窗口需要支持多状态 | P3 | Jeff Tao | Releasing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6927171373?node=27777982) |
| 6661700117 | [IDMP] 查询中支持按自然周、月、季、年（时区与查询改造） | P3 | Simon Guan | Reviewing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6661700117?node=27777982) |
| 6661525203 | [IDMP] 元数据更新支持事务（演示版本） | P3 | Yaqiang Li | Canceled | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6661525203?node=28816616) |
| 6932631879 | [IDMP] 流计算的表达式需要支持标签列 | P3 | Jeff Tao | Canceled | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6932631879?node=28816616) |

### 3.4 规划

| 工作项ID | 名称 | 优先级 | 报告人 | 状态 | 说明 | 链接 |
| --- | --- | --- | --- | --- | --- | --- |
| 6994456141 | TSDB 用户手册重新组织 | P3 | Simon Guan | Processing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6994456141?node=27777982) |
| 6993060355 | Python UDF 插件内置编译 | P3 | Simon Guan | Releasing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6993060355?node=27777982) |
| 6991235591 | 减少可执行文件的大小 | P3 | Simon Guan | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6991235591?node=27777982) |
| 6862269465 | [Windows] 分析不支持的功能小项并适配 | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6862269465?node=27777982) |
| 6944899507 | （子）查询数据来自 CSV 文件或者字符串 | P3 | Xinsheng Ren | Releasing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6944899507?node=27777982) |
| 6936177611 | external window 和 STMT 一起使用 | P3 | Xinsheng Ren | Releasing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6936177611?node=27777982) |
| 6934772510 | ExternalWindow FILL 支持 | P3 | Xinsheng Ren | Releasing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6934772510?node=27777982) |
| 6581335366 | [规划] dataOrderLevel 使用及 table merge scan 有序传递 | P3 | Xinsheng Ren | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6581335366?node=27777982) |
| 6536374390 | [规划] 优化需要 TS 主键列函数的执行条件 | P3 | Wei Pan | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6536374390?node=27777982) |
| 6876989393 | [Windows] UDF 适配 | P1 | Simon Guan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6876989393?node=27777982) |
| 6862031345 | [Windows] MQTT 订阅适配 | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6862031345?node=27777982) |
| 6751417338 | [规划] 流计算支持 ANY/SOME/ALL/EXISTS/NOT EXISTS 运算符 | P3 | Wei Pan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6751417338?node=27777982) |
| 6619755141 | [规划] 流计算支持虚拟超级表聚合查询优化 | P3 | Joey Sima | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6619755141?node=27777982) |
| 6994736247 | [Windows] 增加 ASAN 编译选项以检查内存写坏问题 | P3 | Simon Guan | Reviewing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6994736247?node=27777982) |
| 6916090474 | [安全可靠测评] 列查询结果脱敏展示 | P3 | Cary Xu_8085 | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6916090474?node=27777982) |
| 6671585124 | [安全可靠测评] 强制访问控制，主体级别、客体级别（1-5） | P3 | Beryl Bao | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6671585124?node=27777982) |
| 6670071929 | [安全可靠测评] 引擎侧支持三员权限 | P1 | Beryl Bao | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6670071929?node=27777982) |
| 6659897268 | [规划] 缩短离线节点恢复的时间（不阻塞写入） | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659897268?node=27777982) |
| 6660003972 | [规划] 缩短多副本切主后集群恢复时间 | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6660003972?node=27777982) |
| 6659794715 | [规划] 引擎侧 CPU 管理 | P3 | Simon Guan | Releasing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659794715?node=27777982) |
| 6935889375 | jdbc 元数据订阅需求同步更新：新增修改表 19,20 类型，创建表虚拟子表信息 | P3 | Mark She | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6935889375?node=27777982) |
| 6860938390 | JDBC stmt2 序列化优化 | P3 | Mark She | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6860938390?node=27777982) |
| 6662868645 | JDBC 支持 Adapter 高可用 | P3 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6662868645?node=27777982) |
| 6662886210 | [生态-IOT] 添加 Ignition 文档 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6662886210?node=27777982) |
| 6662891539 | TDinsight增加统计指标 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6662891539?node=27777982) |
| 6658900952 | [安全] 连接器安全开发 - 指南文档 | P2 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6658900952?node=27777982) |
| 6482039483 | [安全] 修复 JDBC sonar 检查的错误和安全问题 | P2 | Mark She | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6482039483?node=27777982) |
| 6984382630 | [文档]修改 OPCUA 的用户手册 | P3 | Zhiyu Yang | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6984382630?node=27777982) |
| 6660036900 | [规划] 联邦查询（演示版本） | P3 | Simon Guan | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6660036900?node=27777982) |
| 6935517365 | 优化tq 在meta 变更时的处理逻辑，修复超级表订阅 drop table时，meta获取不到的问题，优化tq文件架构 | P3 | Mark Wang | Releasing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6935517365?node=27777982) |
| 6616784073 | [规划] 流计算 vnode 切主 reader tablelist 更新逻辑（虚拟表和非虚拟表） | P3 | Mark Wang | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6616784073?node=27777982) |
| 6490739879 | [规划] 流计算 checkpoint 各类失败问题处理 | P3 | Wei Pan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6490739879?node=27777982) |
| 6491292920 | [规划] 流计算删除 snode 时的 checkpoint 同步与校验 | P3 | Wei Pan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6491292920?node=27777982) |
| 6490635370 | [规划] 流计算历史计算性能优化 | P3 | Wei Pan | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6490635370?node=27777982) |
| 6995842552 | taosgen 工业场景模拟数据生成 SKILL | P3 | Cris Pei | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6995842552?node=27777982) |
| 6984257331 | taosgen 输出日志的参数 -f 语义模糊，替换成表达能力更强的 -o | P3 | Cris Pei | Releasing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6984257331?node=27777982) |
| 6977366618 | tsdb 仓库 ODBC 支持 Docker 中编译/本地跑测试用例 | P3 | Cris Pei | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6977366618?node=27777982) |
| 6755732199 | create xnode task 的 database 类型支持创建默认 token | P3 | Joe Zhang | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6755732199?node=27777982) |
| 6665254525 | taos shell支持以16进制显示查询结果 | P3 | Mark She | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665254525?node=27777982) |
| 6665149073 | TDengine CLI 中无法中断查询显示错误提示 | P3 | Mark She | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665149073?node=27777982) |
| 6665211727 | ODBC 统一 Native 和 WebSocket 接口调用，且需要支持 stmt2 | P3 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665211727?node=27777982) |
| 6665124593 | 命令行方便查看订阅数据的工具 | P3 | Leo Huo | Releasing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665124593?node=27777982) |
| 6664967551 | 支持压缩：ODBC 连接器 （WS) | P3 | Mark She | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6664967551?node=27777982) |
| 6663148353 | C# websocket 支持 blob 类型 | P3 | Mark She | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6663148353?node=27777982) |
| 6663137799 | C# 支持 Adapter 高可用 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6663137799?node=27777982) |
| 6662861855 | C# WebSocket 参数绑定支持 decimal 类型 | P3 | Mark She | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6662861855?node=27777982) |
| 6661523672 | [产品] taosgen: CSV 导入功能优化 | P3 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6661523672?node=27777982) |
| 6977133755 | xnoded 日志优化 | P3 | Astro Yan | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6977133755?node=27777982) |
| 6862666522 | taosx 支持导入导出配置文件 | P3 | Zhiyu Yang | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6862666522?node=27777982) |
| 6793571117 | Explorer 生成任务配置优化，global 字段默认不传或内部字段默认为空 | P3 | Astro Yan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6793571117?node=27777982) |
| 6778234983 | [XNODE] Explorer 创建任务收到没有可用 XNODE 时，引导用户到 XNODE 创建页面 | P2 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6778234983?node=27777982) |
| 6755452428 | [产品] Data Out 支持导出到 Parquet | P2 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6755452428?node=27777982) |
| 6755550481 | [产品] Data Out 支持导出到 Kafka | P1 | Leo Huo | Reviewing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6755550481?node=27777982) |
| 6755725378 | [产品] Data Out 支持导出到 MQTT | P2 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6755725378?node=27777982) |
| 6755509969 | [产品] TSDB taosX/Explorer 数据导出 | P2 | Mark She | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6755509969?node=27777982) |
| 6714936723 | XNODE: Explorer 支持添加删除 XNODE | P3 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6714936723?node=27777982) |
| 6662978465 | [公共] taosx支持进行数据transformer的导入及导出 | P3 | Steven Zhang | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6662978465?node=27777982) |
| 6663226836 | [explorer] 数据订阅的示例代码页面，步骤条显示错误 | P5 | Yuanpai Zhang | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6663226836?node=27777982) |
| 6622744595 | Explorer 可配置 Agent 服务地址 | P3 | Jim Fan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6622744595?node=27777982) |
| 6994658352 | [Windows] 修改 Database 的 wal 选项默认值为强制刷新 | P3 | Simon Guan | Processing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6994658352?node=27777982) |
| 6883786265 | [安全相关] 支持通过 taosk 命令修改加密的配置文件 | P3 | Beryl Bao | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6883786265?node=27777982) |
| 6661410964 | [规划] 完善数据修复工具 | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6661410964?node=27777982) |
| 6570698058 | [规划] lastrow 并发查询性能优化 | P3 | Bo Xiao | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6570698058?node=27777982) |
| 6490743340 | [规划] 提升开启 Last 缓存时多列场景的写入性能 | P3 | Beryl Bao | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6490743340?node=27777982) |
| 6973266590 | 【流计算】虚拟表meta 变更， trigger 逻辑处理 | P3 | Mark Wang | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6973266590?node=27777982) |
| 6984239433 | taosAdapter 支持 stmt2 查询获取 fields 信息 | P3 | Ethan Guo | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6984239433?node=27777982) |
| 6974553715 | C WebSocket 连接器支持 decimal 数据类型 | P3 | Ethan Guo | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6974553715?node=27777982) |
| 6935295207 | Rust 连接器支持新的 TMQ AlterType 19, 20 | P3 | Ethan Guo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6935295207?node=27777982) |
| 6920717643 | nodejs 支持app 名称和 ip设置 | P3 | Mark She | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6920717643?node=27777982) |
| 6665840988 | nodejs 连接器性能压测工具开发 | P3 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665840988?node=27777982) |
| 6666030663 | python 连接器性能压测工具开发 | P3 | Leo Huo | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6666030663?node=27777982) |
| 6665271129 | nodejs WebSocket 参数绑定支持 decimal 类型和 blob 类型 | P3 | Mark She | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665271129?node=27777982) |
| 6665209157 | python WebSocket 参数绑定支持 decimal 类型和 blob 类型 | P3 | Mark She | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665209157?node=27777982) |
| 6665220606 | rust 连接器参数绑定支持 decimal 类型和 blob 类型 | P3 | Mark She | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665220606?node=27777982) |
| 6665209146 | C 支持 Adapter 高可用 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665209146?node=27777982) |
| 6666030630 | rust 连接器支持 Adapter 高可用 | P3 | Leo Huo | Releasing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6666030630?node=27777982) |
| 6665221613 | python 连接器支持 Adapter 高可用 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665221613?node=27777982) |
| 6665098131 | nodejs 支持 blob 类型 | P3 | Mark She | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665098131?node=27777982) |
| 6665270968 | Nodejs 支持 Adapter 高可用 | P3 | Leo Huo | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665270968?node=27777982) |
| 6662936374 | Go 支持 Adapter 高可用 | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6662936374?node=27777982) |
| 6548007902 | c websocket 连接器增加三个函数 | P2 | Bomin Zhang | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6548007902?node=27777982) |
| 6488942152 | websocket 连接器增加两个函数 | P2 | Bomin Zhang | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6488942152?node=27777982) |
| 6994396984 | stmt2 查询根据时间戳位数，自动判断精度 | P3 | Simon Guan | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6994396984?node=27777982) |
| 6944442785 | 0x80000125	Retry needed 错误码让应用重试，不合理，建议优化 | P3 | Mark She | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6944442785?node=27777982) |
| 6579574893 | [规划] show streams 支持不指定 dbname | P3 | Bo Xiao | Releasing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6579574893?node=27777982) |
| 6503261141 | [规划] 允许失败时，流的通知发送改成异步进行 | P3 | Kane Kuang | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6503261141?node=27777982) |
| 6862220600 | [Windows] 共享存储适配 | P3 | Simon Guan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6862220600?node=27777982) |
| 6544826545 | [规划] 子查询涉及主键列排序场景的性能优化 | P3 | Wei Pan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6544826545?node=27777982) |
| 6474961364 | [规划] 支持季度时间单位 | P3 | Wei Pan | Releasing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6474961364?node=27777982) |
| 6490982243 | [规划] 流计算虚拟表触发计算性能优化 | P3 | Wei Pan | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6490982243?node=28816616) |
| 6492554061 | [规划] 虚拟表继承 | P2 | Simon Guan | New | 删除，移至IDMP | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6492554061?node=28816616) |
| 6551339451 | [规划] taosc API 在 stdout 不应有输出 | P3 | Leo Huo | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6551339451?node=28816616) |
| 6635149921 | taosX 高可用异常测试自动化 | P3 | Leo Huo | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6635149921?node=28816616) |
| 6646214948 | taosX：PostgreSQL 支持负载均衡 | P3 | Leo Huo | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6646214948?node=28816616) |
| 6646286429 | [产品] taosx 高可用支持双活 | P3 | Leo Huo | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6646286429?node=28816616) |
| 6646294822 | taosX: 数据迁移支持负载均衡 | P3 | Leo Huo | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6646294822?node=28816616) |
| 6646341320 | taosX: Oracle 支持负载均衡 | P3 | Leo Huo | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6646341320?node=28816616) |
| 6646475807 | taosx 高可用：支持同一任务下多个 agent 节点故障转移 | P3 | Leo Huo | Processing | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6646475807?node=28816616) |
| 6646545784 | taosx: MySQL 支持负载均衡 | P3 | Leo Huo | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6646545784?node=28816616) |
| 6646814636 | taosx：Agent 支持高可用 | P3 | Leo Huo | Processing | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6646814636?node=28816616) |
| 6646964092 | taosX: TDengine 数据订阅支持负载均衡 | P3 | Leo Huo | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6646964092?node=28816616) |
| 6647002003 | taosX: MSSQL 支持查询负载均衡 | P3 | Leo Huo | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6647002003?node=28816616) |
| 6658950467 | [测试] 连接器负载均衡测试 | P5 | Leo Huo | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6658950467?node=28816616) |
| 6659281143 | [测试] taosx xnode 稳定性测试 | P3 | Leo Huo | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659281143?node=28816616) |
| 6659287657 | [规划] taosx 写入性能优化 | P3 | Leo Huo | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659287657?node=28816616) |
| 6659306656 | [规划] taosx 性能指标可观测性优化 | P3 | Leo Huo | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659306656?node=28816616) |
| 6659796573 | [规划] 流计算进一步降低资源消耗 | P3 | Simon Guan | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659796573?node=28816616) |
| 6659810600 | [规划] 流计算多测点场景的性能优化 | P3 | Simon Guan | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659810600?node=28816616) |
| 6659972286 | taosx: 新增数据源 开发指南 | P3 | Leo Huo | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659972286?node=28816616) |
| 6660030137 | [规划] 流计算多个客户场景的性能提升 | P3 | Simon Guan | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6660030137?node=28816616) |
| 6662904830 | C# 连接器性能压测工具开发 | P3 | Leo Huo | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6662904830?node=28816616) |
| 6663246472 | 支持压缩：Node.JS 连接器 (WS) | P5 | Mark She | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6663246472?node=28816616) |
| 6665336277 | [规划] License Center | P3 | Leo Huo | Processing | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665336277?node=28816616) |
| 6666686250 | taosgen 优化CSV文件读入的方式 | P2 | Mark She | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6666686250?node=28816616) |
| 6666995190 | taosgen 支持查询 TDengine | P3 | Mark She | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6666995190?node=28816616) |
| 6669980852 | [安全] jwt token secret 变为动态发送给 xnoded | P3 | Joe Zhang | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6669980852?node=28816616) |
| 6793466899 | taosx 使用 taos-ui 请使用 submodule | P2 | Leo Huo | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6793466899?node=28816616) |
| 6832951901 | [测试] Explorer: UI 自动化测试 | P3 | Leo Huo | Processing | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6832951901?node=28816616) |
| 6922162175 | taosgen 所有命令行参数，支持环境变量 | P2 | Mark She | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6922162175?node=28816616) |

### 3.5 海外

| 工作项ID | 名称 | 优先级 | 报告人 | 状态 | 说明 | 链接 |
| --- | --- | --- | --- | --- | --- | --- |
| 6989789149 | [Viega] Feature Request: Add Store-and-Forward Support for taosx-agent OPC UA Ingestion | P2 | Jim Fan | Releasing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6989789149?node=27777982) |
| 6956269849 | [ADS] OPC UA 支持 Alarm & Events | P3 | Simon Guan | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6956269849?node=27777982) |
| 6923100607 | Data In建超级表，数据列移至标签列自动删除默认标签列 | P5 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6923100607?node=27777982) |
| 6993291249 | taosgen TDengine schemaless 方式写入支持提前建表 | P3 | Cris Pei | Releasing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6993291249?node=27777982) |
| 6990459028 | taosgen TDengine schemaless 写入支持可选的指定子表列名称 | P3 | Cris Pei | Releasing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6990459028?node=27777982) |
| 6988915671 | taosgen schemaless 行协议支持一定比例 NONE（数据值缺失） 语义 | P3 | Cris Pei | Releasing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6988915671?node=27777982) |
| 6986376191 | taosgen 变长数据类型 varchar/nchar 支持生成随机长度的值 | P3 | Cris Pei | Releasing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6986376191?node=27777982) |
| 6984939673 | taosgen需要能将生成的数据通过line protocol写入到tdengine或influxDB | P3 | Jeff Tao | Releasing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6984939673?node=27777982) |
| 6861580736 | [Shape Digital] Rolling full-backup | P3 | Leo Huo | New | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6861580736?node=28816616) |

### 3.6 平台

| 工作项ID | 名称 | 优先级 | 报告人 | 状态 | 说明 | 链接 |
| --- | --- | --- | --- | --- | --- | --- |
| 6983602662 | 自动收集日报并记录到 Gitlab 仓库中 | P2 | Simon Guan | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6983602662?node=27778027) |
| 6975432268 | [ 星网 ] 配合云平台下发集群的自动化部署脚本 | P3 | Bo Xiao | Verifying | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6975432268?node=27778027) |
| 6975319054 | cd：cd 流程迁移至 gitlab 仓库 | P3 | Haoran Chen | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6975319054?node=27778027) |
| 6953183654 | cd：Historian 安装包优化 | P3 | Haoran Chen | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6953183654?node=27778027) |
| 6929400303 | 前端 CI 可以支持加上参数支持一个用例失败就结束 | P2 | Yaqiang Li | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6929400303?node=27778027) |
| 6925376607 | IDMP 用户手册需要支持版本 | P3 | Jeff Tao | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6925376607?node=27778027) |
| 6918937030 | 失败 pr 和 定时任务  action，能否推送飞书 | P3 | Mark She | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6918937030?node=27778027) |
| 6869917412 | 检查并更新TDengine, TDinternal, TDasset, taosX等几个大的项目的README文件 | P3 | Jeff Tao | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6869917412?node=27778027) |
| 6869885321 | 我们几个主要的项目在GitHub对应的首页，README的上方，要呈现Release, Testing, Coverage等Badge，便于公司任何人查看当前状态 | P3 | Jeff Tao | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6869885321?node=27778027) |
| 6842917229 | 云服务前端项目 TDC-UI 的 CICD Github Action 流程包括测试和部署 | P2 | Yaqiang Li | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6842917229?node=27778027) |
| 6775003566 | [Platform] 内部软件仓库 | P2 | Leo Huo | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6775003566?node=27778027) |
| 6764403764 | [TSDB] CI 中添加预检测，避免使用内存不安全的函数 | P3 | Yihao Deng-8076 | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6764403764?node=27778027) |
| 6668158551 | [内部] License发放数据库备份及高可用 | P3 | Steven Zhang | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6668158551?node=27778027) |
| 6668116586 | [内部] 界面化License自助发码 | P3 | Steven Zhang | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6668116586?node=27778027) |
| 6666939755 | 改进 CI 用例执行时间 | P2 | Wei Pan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6666939755?node=27778027) |
| 6437586640 | 为文档站点生成 single page html | P3 | Xu Wang | Backlog | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6437586640?node=27778027) |
| 6668113757 | 【售前】统一非root用户和root用户安装后启动taos cli的行为 | P3 | Zach Wang | Backlog | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6668113757?node=27778027) |
| 6747206999 | [Sail ADV] TDengine Sync Process Reliability Inside Docker | P3 | Leo Huo | Backlog | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6747206999?node=27778027) |
| 6776375399 | [安全] 为发布版本生成 SBOM 文件 | P3 | Leo Huo | Backlog | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6776375399?node=27778027) |
| 6836311627 | [公共] 非root安装仅限企业版客户 | P3 | Bo Xiao | Backlog | 删除 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6836311627?node=27778027) |

## 4. 风险管理表

| 编号 | 风险分类 | 风险描述 | 提交人 | 提交日期 | 发生阶段 | 责任人 | 可能性 | 风险级别 | 管理策略 | 应对措施描述 | 风险状态 | 状态更新日 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 进度风险 | License Server 需求和设计尚未确认，工期有风险 | 关胜亮 | 2026-04-13 | 需求设计阶段 | Leo Huo | 高 | 高 | 风险减轻 | 尽快组织需求评审和设计评审，确认功能范围和技术方案 | 已解决 | 2026-05-26 |
| 2 | 进度风险 | 联邦查询（演示版本）需求和设计尚未确认，工期有风险 | 关胜亮 | 2026-04-13 | 需求设计阶段 | Simon Guan | 高 | 高 | 风险减轻 | 尽快确认联邦查询的需求范围和设计方案，明确演示版本的功能边界 | 已解决 | 2026-05-26 |

## 5. 月度总结

### 5.1 2026年4月总结

1. 项目进度总述
   - 本月完成项目立项和项目计划评审，开始进入需求与设计阶段。整体进度正常，业务已完成 11/45 项，IDMP 已完成 3/16 项，规划已完成 13/100 项，海外已完成 1/3 项，平台已完成 7/16 项。
2. 项目主要成果
   - 业务：完成社区版不限制国产操作系统和 CPU 的支持（Done）；trim 操作和 ssmigrate 事务冲突修复、TSDB Lite Explorer 写入选项优化、TO_ISO8601 夏令时支持、分钟级别时区支持已进入发布流程（Releasing）；restore 支持指定 vgroup id 恢复、高负载选主行为优化已进入验证阶段（Verifying）；show table distribute 格式化显示进入测试阶段（Testing）；AVEVA Historian 数据源实时同步优化进入评审阶段（Reviewing）
   - IDMP：完成 TSDB 默认授权不因 machine id 变化而 revoke 授权（Done）
   - 规划：流计算支持 ANY/SOME/ALL/EXISTS/NOT EXISTS 运算符、c websocket 连接器增加三个函数、websocket 连接器增加两个函数已完成（Done）；jdbc 元数据订阅更新、Rust 连接器 TMQ AlterType 19/20 支持、Explorer 任务配置优化、python/rust 连接器 decimal 和 blob 类型参数绑定、show streams 不指定 dbname、taosgen CSV 导入优化已进入发布流程（Releasing）；taosx 导入导出配置文件、nodejs blob 类型支持进入测试阶段（Testing）；taos shell 16进制显示进入评审阶段（Reviewing）
   - 海外：Data In 建超级表数据列移至标签列自动删除默认标签列已完成（Done）
   - 平台：IDMP 用户手册版本支持已完成（Done）；Historian 安装包优化已进入发布流程（Releasing）
3. 本月需求变更
   - 新增任务 56 个（业务 9 个、IDMP 6 个、规划 29 个、海外 2 个、平台 10 个），移出任务 0 个
4. 本月缺陷说明
   - 无
5. 下月工作计划
   - 按照项目计划推进各模块需求分析和功能设计
   - 重点跟进 License Server 和联邦查询的需求确认与设计评审，降低工期风险
   - 持续推进业务侧客户交付需求的开发与验证

### 5.2 2026年5月总结

1. 项目进度总述
   - 本月项目进入开发及功能测试阶段，整体进度正常。业务已完成 6/49 项，IDMP 已完成 3/24 项，规划已完成 38/89 项，海外已完成 1/8 项，平台已完成 6/15 项。规划模块完成率较高（43%），主要得益于连接器和工具侧多项功能快速交付。
2. 项目主要成果
   - 业务：数据修复工具支持 copy 文件方式恢复数据已完成（Done）；子查询与外层时间范围交集扫描、Greatest/Least 忽略 NULL、Sleep 函数模拟已进入发布流程（Releasing）；asof join 查询优化、监控 restore snapshot 进度、restore 指定 vgroup id 恢复等多项进入验证阶段（Verifying）；断电宕机 WAL 自动修复、COUNT_WINDOW 流重算进入开发阶段（Processing）
   - IDMP：taosd 大量超级表偶发卡死问题修复、子事件通知关联关系已完成（Done）；REGEXP_EXTRACT 函数、taosdump 导出 stream 语句等进入发布流程（Releasing）；stream 支持 tag-ref 和虚拟表引用、流计算逐级汇总计算进入测试阶段（Testing）；虚拟表继承、虚拟表支持引用虚拟表等核心功能持续推进（Processing）
   - 规划：减少可执行文件大小、列查询结果脱敏、taosk 修改加密配置文件、ODBC Docker 编译支持、xnoded 日志优化、taosAdapter 支持 stmt2 查询、C WebSocket decimal 支持、stmt2 自动判断精度、Retry 错误码优化、OPCUA 用户手册修改、taosgen 工业场景 SKILL 等大量功能已完成（Done）；Python UDF 内置编译、引擎侧 CPU 管理、tq meta 变更处理优化、taosgen -f 参数替换等进入发布流程（Releasing）
   - 海外：Viega Store-and-Forward 支持、taosgen schemaless 多项增强（提前建表、指定子表列名、NONE 语义、随机长度值）、taosgen line protocol 写入等 6 项进入发布流程（Releasing）
   - 平台：自动收集日报到 Gitlab、前端 CI 参数优化、Historian 安装包优化、IDMP 用户手册版本支持、内部软件仓库、失败 pr 推送飞书等已完成（Done）；星网自动化部署脚本进入验证阶段（Verifying）
3. 本月需求变更
   - 新增任务 46 个（业务 7 个、IDMP 10 个、规划 19 个、海外 6 个、平台 4 个），移出任务 41 个（业务 3 个、IDMP 2 个、规划 30 个、海外 1 个、平台 5 个）
4. 本月缺陷说明
   - 无
5. 下月工作计划
   - 继续推进开发及功能测试阶段的各项任务
   - 业务侧重点推进断电宕机 WAL 修复、缓存强制刷新等客户急需功能
   - IDMP 侧重点推进虚拟表继承、流计算 tag-ref 和逐级汇总功能的测试验证
   - 规划侧持续推进安全可靠测评（强制访问控制、三员权限）和连接器能力补齐
   - 准备进入系统测试阶段，确保各模块功能就绪

### 5.3 2026年6月总结

1. 项目进度总述
   - 待更新
2. 项目主要成果
   - 待更新
3. 本月需求变更
   - 待更新
4. 本月缺陷说明
   - 待更新
5. 下月工作计划
   - 待更新
