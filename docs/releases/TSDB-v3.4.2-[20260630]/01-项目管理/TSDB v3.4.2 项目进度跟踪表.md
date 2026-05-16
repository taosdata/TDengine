# TSDB v3.4.2 项目进度跟踪表

## 1. 修订记录

| 更新日期 | 更新人 | 主要修改内容 |
| --- | --- | --- |
| 2026-4-13 | 关胜亮 | 第一次进度跟踪 |

## 2. 项目进度概览

1. 整体进度：*（未开始/正常/有风险/严重滞后/已完成）*
   - 业务：总计 45 项，已完成 11 项，未完成 34 项（76%）
   - IDMP：总计 16 项，已完成 3 项，未完成 13 项（81%）
   - 规划：总计 100 项，已完成 13 项，未完成 87 项（87%）
   - 海外：总计 3 项，已完成 1 项，未完成 2 项（67%）
   - 平台：总计 16 项，已完成 7 项，未完成 9 项（56%）
2. 范围状态：
   - 业务：新增任务  9  个，移出任务  0  个
   - IDMP：新增任务  6  个，移出任务  0  个
   - 规划：新增任务  29  个，移出任务  0  个
   - 海外：新增任务  2  个，移出任务  0  个
   - 平台：新增任务  10  个，移出任务  0  个
3. 主要风险：
   - 业务：无
   - IDMP：无
   - 规划：License Server 和联邦查询（演示版本）需求和设计尚未确认，工期有风险，需尽快组织评审
   - 海外：无
   - 平台：无

## 3. 工作分解结构与进度跟踪

### 3.1 亮点功能

1. 引擎
   - 窗口函数和 OVER 子句
   - 虚拟表继承机制
   - 联邦查询演示版本
   - 元数据变更支持事务
   - 集群切主、同步等的稳定性提升
2. 工具
   - TSDB Explorer 支持 Data Out 到 Parquet/Kafka/MQTT 的导出能力
   - taosX 高可用与负载均衡完善和增强
   - 各个连接器的高可用与负载均衡
   - License Server 正式发布
3. 平台
   - 建立日志周期性检查机制
   - crash-gen 优化及增强
   - GitHub Internal/Private 仓库迁移至 GitLab
   - IDMP 稳定性测试框架

### 3.2 业务

| 工作项ID | 名称 | 优先级 | 报告人 | 状态 | 说明 | 链接 |
| --- | --- | --- | --- | --- | --- | --- |
| 6952596147 | [交付][郑煤机] trim 操作和 ssmigrate 事务之间冲突 | P3 | Kian Wang | Releasing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6952596147?node=28816616) |
| 6930507762 | [交付][树根科技] restore 命令支持指定 vgroup id 恢复 | P3 | Zachary Xiao | Verifying | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6930507762?node=28816616) |
| 6928677802 | [赛力斯] taosx 支持创建 400+ 个字段的数据写入任务 | P3 | Raistlin Chen | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6928677802?node=28816616) |
| 6925549512 | [售前][中石油] 支持不限制国产操作系统和 CPU 的社区版 | P3 | Guang Li | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6925549512?node=28816616) |
| 6923487183 | [售前][冠德] 流计算 external window 和多分组优化支持 JOIN 语句 | P3 | Jack Dong | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6923487183?node=28816616) |
| 6923036499 | [交付][领储宇能] ServerPort 修改后订阅内部记录应同步更新 | P3 | Kian Wang | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6923036499?node=28816616) |
| 6921874285 | [售前][川威] TSDB Lite 的 Explorer 数据写入选项，仅保留支持的选项：OPC、MQTT 等 | P3 | Bo Xiao | Releasing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6921874285?node=28816616) |
| 6918805473 | [交付][郑煤机] 共享存储支持配置华为 obs | P3 | Kian Wang | Canceled |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6918805473?node=28816616) |
| 6914731989 | [社区] 订阅功能开源版本可以修改 topic 数量 | P3 | Steven Zhang | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6914731989?node=28816616) |
| 6914845502 | [交付] 全局参数修改专用工具 | P3 | Steven Zhang | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6914845502?node=28816616) |
| 6861626512 | [交付][领储宇能] k8s 部署时无法获得正常的容器资源信息 | P3 | Steven Zhang | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6861626512?node=28816616) |
| 6849686611 | [交付][疆海] TO_ISO8601 支持夏令时的转化 | P3 | Raistlin Chen | Releasing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6849686611?node=28816616) |
| 6772914536 | [交付][博创联动] 子查询数据扫描时，取子查询与外层时间范围的交集进行扫描 | P3 | Kian Wang | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6772914536?node=28816616) |
| 6751085739 | [长飞光纤]AVEVA Historian数据源任务数据实时同步优化 | P3 | Kian Wang | Reviewing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6751085739?node=28816616) |
| 6751395113 | [沃太能源]taosx支持指定表的备份恢复 | P3 | Yanqiong Dong | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6751395113?node=28816616) |
| 6671837225 | [交付][新奥新智] 大量查询不存在表导致 mnode CPU 高 | P3 | Zee Lv | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6671837225?node=28816616) |
| 6671971734 | [售前][上海电气中央研究院] 虚拟表支持引用不同数据库精度的表 | P3 | Tyler Liu | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6671971734?node=28816616) |
| 6670886193 | [交付][河北电力] 希望增加日志里重要 ERROR 告警 | P3 | Zee Lv | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6670886193?node=28816616) |
| 6672111997 | [交付] 规范化数据库重要操作开始结束标志信息输出 | P3 | Steven Zhang | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6672111997?node=28816616) |
| 6662862362 | [河北电力新一代调度项目] taosX 归档的 archive 文件读取工具 | P3 | Tyler Liu | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6662862362?node=28816616) |
| 6622709348 | [售前][上海电气中央研究院] 扩展 taosX 解析功能 | P3 | Tyler Liu | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6622709348?node=28816616) |
| 6622596851 | [河北电力新一代调度项目]explorer 增加 taosx命令行方式的-T 参数 | P3 | Zee Lv | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6622596851?node=28816616) |
| 6617550569 | [售前][上海电气中央研究院] MQTT 数据源能够获取报文头信息 | P3 | Tyler Liu | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6617550569?node=28816616) |
| 6617536550 | [售前][上海电气中央研究院] 希望 taosX 能够主动控制资源占用 | P3 | Tyler Liu | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6617536550?node=28816616) |
| 6617617575 | [售前][上海电气中央研究院] 通过 taosX 上传大量 csv 文件并导入的行为改进 | P3 | Tyler Liu | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6617617575?node=28816616) |
| 6599966995 | [售前][南网 CEP] show local/dnode variables增加一参数列：是否需要重启生效、当前参数未生效 | P3 | Bo Xiao | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6599966995?node=28816616) |
| 6589436029 | [交付][河北电力] 优化频繁 use db 导致 mnode read 线程压力过大 | P3 | Zee Lv | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6589436029?node=28816616) |
| 6574020760 | [交付][天合富家] 增加缓存强制刷新功能 | P3 | Yanqiong Dong | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6574020760?node=28816616) |
| 6511301953 | [交付] Audit 库可以记录客户端 IP | P3 | Hui Li | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6511301953?node=28816616) |
| 6514083018 | [售前][南网数研院][南瑞电网] 提升 Interp 查询性能 | P4 | Bo Xiao | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6514083018?node=28816616) |
| 6510958760 | [交付][中冶京诚] insert into file 错误信息优化提升 | P3 | Steven Zhang | Canceled |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6510958760?node=28816616) |
| 6511323203 | [售前][神东集团] 单副本变三副本支持共享存储 | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6511323203?node=28816616) |
| 6513771567 | [售前][三峡集团] 需要支持ROW_NUMBER() OVER()函数 | P4 | Jack Dong | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6513771567?node=28816616) |
| 6511294180 | [交付][拾贝云] Greatest/Least 与 MySQL 对齐，支持忽略 NULL | P3 | Raistlin Chen | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6511294180?node=28816616) |
| 6510119993 | [售前][上科信息] 分组查询 partition by 支持组内排序 | P3 | ​Richard Li | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6510119993?node=28816616) |
| 6507136288 | [交付] 查询函数 Sleep(duration) 用于超时问题模拟 | P3 | Raistlin Chen | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6507136288?node=28816616) |
| 6506025858 | [交付] show table distribute 格式化显示，便于过滤 | P3 | Raistlin Chen | Testing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6506025858?node=28816616) |
| 6506113427 | [交付][南网储能-拾贝云] 节点启动过程中应用需要正常使用不报错 | P3 | Raistlin Chen | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6506113427?node=28816616) |
| 6507042141 | [交付][三峡]优化高负载情况下选主行为（可行性方案） | P3 | Yanqiong Dong | Verifying | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6507042141?node=28816616) |
| 6507051705 | [交付][海澜智云] 社区版在执行企业版专有功能时有报错提醒 | P3 | Tyler Liu | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6507051705?node=28816616) |
| 6507156244 | [售前][陕西中烟] 缺少排名函数，如rank() | P3 | Abraham Liu | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6507156244?node=28816616) |
| 6491198599 | [交付] 支持指定列进行最新数据缓存 | P3 | Beryl Bao | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6491198599?node=28816616) |
| 6491037879 | [交付][爱动] 支持分钟级别的时区 | P3 | Kian Wang | Releasing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6491037879?node=28816616) |
| 6491115004 | [交付] 禁止删除正在被订阅使用的子表的对应的超级表 | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6491115004?node=28816616) |
| 6490727766 | [交付][中国电建] 副本变更不影响数据订阅 | P3 | Tyler Liu | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6490727766?node=28816616) |

### 3.3 IDMP

| 工作项ID | 名称 | 优先级 | 报告人 | 状态 | 说明 | 链接 |
| --- | --- | --- | --- | --- | --- | --- |
| 6952737120 | [IDMP] 流计算支持多级子事件 | P3 | Kane Kuang | Processing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6952737120?node=28816616) |
| 6932631879 | [IDMP] 流计算的表达式需要支持标签列 | P3 | Jeff Tao | Canceled | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6932631879?node=28816616) |
| 6927835648 | [IDMP] 支持 FFT | P3 | Jeff Tao | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6927835648?node=28816616) |
| 6927171373 | [IDMP] 状态窗口需要支持多状态 | P3 | Jeff Tao | Processing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6927171373?node=28816616) |
| 6927058167 | [IDMP] TSDB 默认授权应不因 machine id 变化而 revoke 授权 | P2 | Bo Xiao | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6927058167?node=28816616) |
| 6661700117 | [IDMP] 查询中支持按自然周、月、季、年 | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6661700117?node=28816616) |
| 6661525203 | [IDMP] 元数据更新支持事务（演示版本） | P3 | Yaqiang Li | Canceled |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6661525203?node=28816616) |
| 6659965197 | [IDMP] 元数据更新支持事务（虚拟表变更） | P3 | Yaqiang Li | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659965197?node=28816616) |
| 6659773700 | [IDMP] 放宽窗口查询限制（不仅是聚合） | P3 | Yaqiang Li | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659773700?node=28816616) |
| 6598098782 | [IDMP] CSUM 支持在窗口查询中使用 | P3 | Yaqiang Li | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6598098782?node=28816616) |
| 6592836563 | [IDMP][一汽红旗] 事件窗口的结束条件也能够设置持续时间判断 | P3 | Tyler Liu | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6592836563?node=28816616) |
| 6590611316 | [IDMP] 数据备份支持备份虚拟表和流计算 | P2 | Yaqiang Li | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6590611316?node=28816616) |
| 6589380578 | [IDMP][北美] 虚拟表支持引用虚拟表 | P1 | Yaqiang Li | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6589380578?node=28816616) |
| 6572940279 | [IDMP] 删除数据库不加 force 应该告知客户真实原因 | P3 | Yaqiang Li | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6572940279?node=28816616) |
| 6570504710 | [IDMP] 支持修改虚拟超级表列名 | P4 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6570504710?node=28816616) |
| 6549502576 | [IDMP] 支持窗口函数和 OVER 子句 | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6549502576?node=28816616) |

### 3.4 规划

| 工作项ID | 名称 | 优先级 | 报告人 | 状态 | 说明 | 链接 |
| --- | --- | --- | --- | --- | --- | --- |
| 6944899507 | （子）查询数据来自 CSV 文件 | P3 | Xinsheng Ren | Processing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6944899507?node=28816616) |
| 6936177611 | external window 和 STMT 一起使用 | P3 | Xinsheng Ren | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6936177611?node=28816616) |
| 6935889375 | jdbc 元数据订阅需求同步更新：新增修改表 19,20 类型，创建表虚拟子表信息 | P3 | Mark She | Releasing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6935889375?node=28816616) |
| 6935295207 | Rust 连接器支持新的 TMQ AlterType 19, 20 | P3 | Ethan Guo | Releasing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6935295207?node=28816616) |
| 6934772510 | ExternalWindow FILL 支持 | P3 | Xinsheng Ren | Processing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6934772510?node=28816616) |
| 6922162175 | taosgen 所有命令行参数，支持环境变量 | P2 | Mark She | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6922162175?node=28816616) |
| 6920717643 | nodejs 支持app 名称和 ip设置 | P3 | Mark She | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6920717643?node=28816616) |
| 6876989393 | [Windows] UDF 适配 | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6876989393?node=28816616) |
| 6862666522 | taosx 支持导入导出配置文件 | P3 | Zhiyu Yang | Testing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6862666522?node=28816616) |
| 6862269465 | [Windows] 分析不支持的功能小项并适配 | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6862269465?node=28816616) |
| 6862031345 | [Windows] MQTT 订阅适配 | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6862031345?node=28816616) |
| 6862220600 | [Windows] 共享存储适配 | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6862220600?node=28816616) |
| 6860938390 | JDBC stmt2 序列化优化 | P3 | Mark She | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6860938390?node=28816616) |
| 6832951901 | [测试] Explorer: UI 自动化测试 | P3 | Leo Huo | Processing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6832951901?node=28816616) |
| 6793571117 | Explorer 生成任务配置优化，global 字段默认不传或内部字段默认为空 | P3 | Astro Yan | Releasing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6793571117?node=28816616) |
| 6793466899 | taosx 使用 taos-ui 请使用 submodule | P2 | Leo Huo | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6793466899?node=28816616) |
| 6778234983 | [XNODE] Explorer 创建任务收到没有可用 XNODE 时，引导用户到 XNODE 创建页面 | P2 | Leo Huo | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6778234983?node=28816616) |
| 6755452428 | [产品] Data Out 支持导出到 Parquet | P2 | Leo Huo | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6755452428?node=28816616) |
| 6755550481 | [产品] Data Out 支持导出到 Kafka | P2 | Leo Huo | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6755550481?node=28816616) |
| 6755725378 | [产品] Data Out 支持导出到 MQTT | P2 | Leo Huo | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6755725378?node=28816616) |
| 6755509969 | [产品] TSDB taosX/Explorer 数据导出 | P2 | Leo Huo | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6755509969?node=28816616) |
| 6755732199 | create xnode task 的 database 类型支持创建默认 token | P3 | Joe Zhang | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6755732199?node=28816616) |
| 6751417338 | [规划] 流计算支持 ANY/SOME/ALL/EXISTS/NOT EXISTS 运算符 | P3 | Wei Pan | Done |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6751417338?node=28816616) |
| 6714936723 | XNODE: Explorer 支持添加删除 XNODE | P3 | Leo Huo | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6714936723?node=28816616) |
| 6671585124 | [安全可靠测评] 强制访问控制，主体级别、客体级别（1-5） | P3 | Beryl Bao | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6671585124?node=28816616) |
| 6670071929 | [安全可靠测评] 引擎侧支持三员权限 | P1 | Beryl Bao | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6670071929?node=28816616) |
| 6669980852 | [安全] jwt token secret 变为动态发送给 xnoded | P3 | Joe Zhang | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6669980852?node=28816616) |
| 6666995190 | taosgen 支持查询 TDengine | P3 | Mark She | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6666995190?node=28816616) |
| 6666686250 | taosgen 优化CSV文件读入的方式 | P2 | Mark She | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6666686250?node=28816616) |
| 6665336277 | [规划] License Center | P3 | Leo Huo | Processing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665336277?node=28816616) |
| 6665840988 | nodejs 连接器性能压测工具开发 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665840988?node=28816616) |
| 6666030663 | python 连接器性能压测工具开发 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6666030663?node=28816616) |
| 6665254525 | taos shell支持以16进制显示查询结果 | P3 | Mark She | Reviewing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665254525?node=28816616) |
| 6665149073 | TDengine CLI 中无法中断查询显示错误提示 | P3 | Mark She | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665149073?node=28816616) |
| 6665271129 | nodejs WebSocket 参数绑定支持 decimal 类型和 blob 类型 | P3 | Mark She | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665271129?node=28816616) |
| 6665209157 | python WebSocket 参数绑定支持 decimal 类型和 blob 类型 | P3 | Mark She | Releasing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665209157?node=28816616) |
| 6665220606 | rust 连接器参数绑定支持 decimal 类型和 blob 类型 | P3 | Mark She | Releasing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665220606?node=28816616) |
| 6665211727 | ODBC Websocket 支持 stmt2 | P3 | Leo Huo | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665211727?node=28816616) |
| 6665124593 | 命令行方便查看订阅数据的工具 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665124593?node=28816616) |
| 6665209146 | C 支持 Adapter 高可用 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665209146?node=28816616) |
| 6666030630 | rust 连接器支持 Adapter 高可用 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6666030630?node=28816616) |
| 6665221613 | python 连接器支持 Adapter 高可用 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665221613?node=28816616) |
| 6665098131 | nodejs 支持 blob 类型 | P3 | Mark She | Testing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665098131?node=28816616) |
| 6665270968 | Nodejs 支持 Adapter 高可用 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6665270968?node=28816616) |
| 6662868645 | JDBC 支持 Adapter 高可用 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6662868645?node=28816616) |
| 6662886210 | [生态-IOT] 添加 Ignition 文档 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6662886210?node=28816616) |
| 6662891539 | TDinsight增加统计指标 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6662891539?node=28816616) |
| 6662978465 | [公共] taosx支持进行数据transformer的导入及导出 | P3 | Steven Zhang | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6662978465?node=28816616) |
| 6663226836 | [explorer] 数据订阅的示例代码页面，步骤条显示错误 | P5 | Yuanpai Zhang | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6663226836?node=28816616) |
| 6664967551 | 支持压缩：ODBC 连接器 （WS) | P3 | Mark She | Processing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6664967551?node=28816616) |
| 6663246472 | 支持压缩：Node.JS 连接器 (WS) | P5 | Mark She | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6663246472?node=28816616) |
| 6663148353 | C# websocket 支持 blob 类型 | P3 | Mark She | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6663148353?node=28816616) |
| 6662936374 | Go 支持 Adapter 高可用 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6662936374?node=28816616) |
| 6663137799 | C# 支持 Adapter 高可用 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6663137799?node=28816616) |
| 6662861855 | C# WebSocket 参数绑定支持 decimal 类型和 blob 类型 | P3 | Mark She | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6662861855?node=28816616) |
| 6662904830 | C# 连接器性能压测工具开发 | P3 | Leo Huo | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6662904830?node=28816616) |
| 6661523672 | [产品] taosgen: CSV 导入功能优化 | P3 | Leo Huo | Releasing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6661523672?node=28816616) |
| 6661410964 | [规划] 完善数据修复工具 | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6661410964?node=28816616) |
| 6659897268 | [规划] 缩短离线节点恢复的时间（不阻塞写入） | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659897268?node=28816616) |
| 6660003972 | [规划] 缩短多副本切主后集群恢复时间 | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6660003972?node=28816616) |
| 6659796573 | [规划] 流计算进一步降低资源消耗 | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659796573?node=28816616) |
| 6659810600 | [规划] 流计算多测点场景的性能优化 | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659810600?node=28816616) |
| 6660030137 | [规划] 流计算多个客户场景的性能提升 | P3 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6660030137?node=28816616) |
| 6660036900 | [规划] 联邦查询（演示版本） | P3 | Simon Guan | Processing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6660036900?node=28816616) |
| 6659972286 | taosx: 新增数据源 开发指南 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659972286?node=28816616) |
| 6658900952 | [安全] 连接器安全开发 - 指南文档 | P2 | Leo Huo | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6658900952?node=28816616) |
| 6658950467 | [测试] 连接器负载均衡测试 | P5 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6658950467?node=28816616) |
| 6659306656 | [规划] taosx 性能指标可观测性优化 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659306656?node=28816616) |
| 6659281143 | [测试] taosx xnode 稳定性测试 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659281143?node=28816616) |
| 6659287657 | [规划] taosx 写入性能优化 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6659287657?node=28816616) |
| 6646286429 | [产品] taosx 高可用支持双活 | P3 | Leo Huo | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6646286429?node=28816616) |
| 6646475807 | taosx 高可用：支持同一任务下多个 agent 节点故障转移 | P3 | Leo Huo | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6646475807?node=28816616) |
| 6646814636 | taosx：Agent 支持高可用 | P3 | Leo Huo | Processing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6646814636?node=28816616) |
| 6646294822 | taosX: 数据迁移支持负载均衡 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6646294822?node=28816616) |
| 6646964092 | taosX: TDengine 数据订阅支持负载均衡 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6646964092?node=28816616) |
| 6646341320 | taosX: Oracle 支持负载均衡 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6646341320?node=28816616) |
| 6647002003 | taosX: MSSQL 支持查询负载均衡 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6647002003?node=28816616) |
| 6646214948 | taosX：PostgreSQL 支持负载均衡 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6646214948?node=28816616) |
| 6646545784 | taosx: MySQL 支持负载均衡 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6646545784?node=28816616) |
| 6635149921 | taosX 高可用异常测试自动化 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6635149921?node=28816616) |
| 6622744595 | Explorer 可配置 Agent 服务地址 | P3 | Jim Fan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6622744595?node=28816616) |
| 6619755141 | [规划] 流计算支持虚拟超级表聚合查询优化 | P3 | Joey Sima | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6619755141?node=28816616) |
| 6616784073 | [规划] 流计算 vnode 切主 reader tablelist 更新逻辑（虚拟表和非虚拟表） | P3 | Mark Wang | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6616784073?node=28816616) |
| 6581335366 | [规划] dataOrderLevel 使用及 table merge scan 有序传递 | P3 | Xinsheng Ren | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6581335366?node=28816616) |
| 6579574893 | [规划] show streams 支持不指定 dbname | P3 | Bo Xiao | Releasing |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6579574893?node=28816616) |
| 6570698058 | [规划] lastrow 并发查询性能优化 | P3 | Bo Xiao | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6570698058?node=28816616) |
| 6551339451 | [规划] taosc API 在 stdout 不应有输出 | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6551339451?node=28816616) |
| 6548007902 | c websocket 连接器增加三个函数 | P2 | Bomin Zhang | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6548007902?node=28816616) |
| 6536374390 | [规划] 优化需要 TS 主键列函数的执行条件 | P3 | Wei Pan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6536374390?node=28816616) |
| 6544826545 | [规划] 子查询涉及主键列排序场景的性能优化 | P3 | Wei Pan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6544826545?node=28816616) |
| 6503261141 | [规划] 允许失败时，流的通知发送改成异步进行 | P3 | Kane Kuang | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6503261141?node=28816616) |
| 6492554061 | [规划] 虚拟表继承 | P2 | Simon Guan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6492554061?node=28816616) |
| 6490743340 | [规划] 提升开启 Last 缓存时多列场景的写入性能 | P3 | Beryl Bao | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6490743340?node=28816616) |
| 6490739879 | [规划] 流计算 checkpoint 各类失败问题处理 | P3 | Wei Pan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6490739879?node=28816616) |
| 6491292920 | [规划] 流计算删除 snode 时的 checkpoint 同步与校验 | P3 | Wei Pan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6491292920?node=28816616) |
| 6490635370 | [规划] 流计算历史计算性能优化 | P3 | Wei Pan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6490635370?node=28816616) |
| 6490982243 | [规划] 流计算虚拟表触发计算性能优化 | P3 | Wei Pan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6490982243?node=28816616) |
| 6488942152 | websocket 连接器增加两个函数 | P2 | Bomin Zhang | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6488942152?node=28816616) |
| 6482039483 | [安全] 修复 JDBC sonar 检查的错误和安全问题 | P2 | Mark She | Processing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6482039483?node=28816616) |
| 6474961364 | [规划] 支持季度时间单位 | P3 | Wei Pan | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6474961364?node=28816616) |

### 3.5 海外

| 工作项ID | 名称 | 优先级 | 报告人 | 状态 | 说明 | 链接 |
| --- | --- | --- | --- | --- | --- | --- |
| 6956269849 | OPC UA 支持 Alarm & Events | P2 | Simon Guan | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6956269849?node=28816616) |
| 6923100607 | Data In建超级表，数据列移至标签列自动删除默认标签列 | P5 | Leo Huo | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6923100607?node=28816616) |
| 6861580736 | [Shape Digital] Rolling full-backup | P3 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6861580736?node=28816616) |

### 3.6 平台

| 工作项ID | 名称 | 优先级 | 报告人 | 状态 | 说明 | 链接 |
| --- | --- | --- | --- | --- | --- | --- |
| 6953183654 | cd：Historian 安装包优化 | P3 | Haoran Chen | Releasing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6953183654?node=27778027) |
| 6925376607 | IDMP 用户手册需要支持版本 | P3 | Jeff Tao | Done | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6925376607?node=27778027) |
| 6918937030 | 失败 pr 和 定时任务  action，能否推送飞书 | P3 | Mark She | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6918937030?node=27778027) |
| 6869917412 | 检查并更新TDengine, TDinternal, TDasset, taosX等几个大的项目的README文件 | P3 | Jeff Tao | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6869917412?node=27778027) |
| 6869885321 | 我们几个主要的项目在GitHub对应的首页，README的上方，要呈现Release, Testing, Coverage等Badge，便于公司任何人查看当前状态 | P3 | Jeff Tao | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6869885321?node=27778027) |
| 6842917229 | 云服务前端项目 TDC-UI 的 CICD Github Action 流程包括测试和部署 | P2 | Yaqiang Li | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6842917229?node=27778027) |
| 6836311627 | [ 公共 ] 非root安装仅限企业版客户 | P3 | Bo Xiao | Backlog | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6836311627?node=27778027) |
| 6776375399 | [安全] 为发布版本生成 SBOM 文件 | P3 | Leo Huo | Backlog |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6776375399?node=27778027) |
| 6775003566 | [Platform] 内部软件仓库 | P2 | Leo Huo | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6775003566?node=27778027) |
| 6764403764 | [TSDB] CI 中添加预检测，避免使用内存不安全的函数 | P3 | Yihao Deng-8076 | Processing | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6764403764?node=27778027) |
| 6747206999 | [Sail ADV] TDengine Sync Process Reliability Inside Docker | P3 | Leo Huo | Backlog | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6747206999?node=27778027) |
| 6668113757 | 【售前】统一非root用户和root用户安装后启动taos cli的行为 | P3 | Zach Wang | Backlog |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6668113757?node=27778027) |
| 6668158551 | [内部] License发放数据库备份及高可用 | P3 | Steven Zhang | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6668158551?node=27778027) |
| 6668116586 | [内部] 界面化License自助发码 | P3 | Steven Zhang | New |  | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6668116586?node=27778027) |
| 6666939755 | 改进 CI 用例执行时间 | P2 | Wei Pan | New | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6666939755?node=27778027) |
| 6437586640 | 为文档站点生成 single page html | P3 | Xu Wang | Backlog | 新增 | [链接](https://project.feishu.cn/taosdata_td/feature/detail/6437586640?node=27778027) |

## 4. 风险管理表

| 编号 | 风险分类 | 风险描述 | 提交人 | 提交日期 | 发生阶段 | 责任人 | 可能性 | 风险级别 | 管理策略 | 应对措施描述 | 风险状态 | 状态更新日 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 进度风险 | License Server 需求和设计尚未确认，工期有风险 | 关胜亮 | 2026-04-13 | 需求设计阶段 | Leo Huo | 高 | 高 | 风险减轻 | 尽快组织需求评审和设计评审，确认功能范围和技术方案 | 已识别 | 2026-04-13 |
| 2 | 进度风险 | 联邦查询（演示版本）需求和设计尚未确认，工期有风险 | 关胜亮 | 2026-04-13 | 需求设计阶段 | Simon Guan | 高 | 高 | 风险减轻 | 尽快确认联邦查询的需求范围和设计方案，明确演示版本的功能边界 | 已识别 | 2026-04-13 |

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
   - 待更新
2. 项目主要成果
   - 待更新
3. 本月需求变更
   - 待更新
4. 本月缺陷说明
   - 待更新
5. 下月工作计划
   - 待更新

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
