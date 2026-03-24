# 流计算重构 TS

## 1. 测试目标

流计算重构后的可用性，包括：流计算节点、流计算管理、触发模式、控制选项、通知机制、计算任务、重算机制、结果保存、数据乱序/更新/删除、性能、权限、兼容性、用户场景、稳定性。

## 2. 相关资料

相关文档
1. [TS-6100](https://jira.taosdata.com:18080/browse/TS-6100)
2. [流计算需求 RS](https://taosdata.feishu.cn/wiki/N3GewVEPkiAMuQk9nkjcts5Pnnh)
3. [流计算重构 FS](https://taosdata.feishu.cn/wiki/HlKTwwnA2iaF3IkFfY5ctzqknne)
用新测试框架测试
1. 用户手册：TDinternal/community/test/README.md
2. 样例文件：
   - TDinternal/community/test/cases/13-StreamProcessing/99-Others/test_dev_basic1.py  
   - TDinternal/community/test/cases/13-StreamProcessing/07-SubQuery/test_subquery_basic.py
3. 运行方法：
  ```bash
  cd /root/TDinternal/community/test
  pytest --clean cases/13-StreamProcessing/99-Others/test_dev_basic1.py 
  pytest --clean cases/13-StreamProcessing/07-SubQuery/test_subquery_basic.py
  ```

1. [新框架使用技巧](https://taosdata.feishu.cn/wiki/LYZdw216pi3EInkRbrFcJb6DnEc)

## 3. 变更历史

| 日期 | 版本 | 作者 | 备忘 |
| --- | --- | --- | --- |
| 2025-5-14 | 0.1 | 关胜亮 | 建立 |
| 2025-7-8 | 0.2 | 关胜亮 | 人员重新分工 |

## 4. 测试结论

待补充

## 5. 测试环境

1. 功能测试: 开发机、Linux 系统
2. 稳定性测试：物理机（待定）

## 6. 功能测试

### 6.1 流计算节点（吕泽-完成）

#### 6.1.1 测试要点

1. 基础功能
   - 创建、删除`snode`基本操作
   - 语法校验、边界
   - `ins_snodes`系统表
   - `show snode``s`命令
   - 特殊情况
      - 无效、离线的`dnode`
      - 重复创建与删除`snode`
      - 删除最后一个`snode`
2. 计算任务
   - `snode`计算状态的持久化存储
   - 删除有计算任务（暂停、计算中、计算完成）的`snode`
   - 同一个节点上反复创建`snode`，检查计算任务残留
   - `snode`所在`dnode`启停对计算任务的影响
3. 副本`snode`
   - `snode`副本的创建、删除与同步
   - 同步状态中的双副本`snode`删除
   - 双副本snode启停对计算任务的影响
4. 权限与监控
   - `snode`的操作权限
   - `snode`的审计日志
   - `snode`的监控指标
5. 其他
   - 系统配置参数的查看、更新

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | snode_mgmt.py -N 8（通过） snode_mgmt_rep3.py -N 6 --replica 3（ 通过） | 1. 创建 snode 1. 删除 snode 1. 查询 information_schema.ins_snodes 1. 执行 show 命令 1. 在同一个 dnode 上重复创建和删除 1. 重启并测试持久化状态 1. 删除非法的 snode、最后一个 snode 1. 删除正在进行计算的 snode 1. 删除处于暂停状态的 snode 1. Dnode 离线时的创建、删除操作 1. 删除存在 snode 的 dnode | 通过 |
| 2 | snode_replicas.py -N 8（通过） | 1. 单副本 snode 启停对 stream 的影响 1. 多个 snode 之间互为副本 1. 删除双副本 snode 中的一个 snode 1. 增加一个 snode 使其够成双副本 1. 双副本增删时，未同步完成状态的 snode 不能被删除，计算状态的监控 1. 单、双副本时的 stream 和 snode 状态 | 通过 |
| 3 | snode_privileges.py（通过） snode_privileges_monitor_table.py（通过） snode_privileges_recalc.py（通过） snode_privileges_twodb.py（通过） snode_params_alter.py(通过) snode_params_alter_normaluser.py（通过） snode_params_check_default.py（通过） snode_params_check_maxValue.py（通过） snode_params_check.py（通过） ~~streambuffersize_verify.py（暂时不测）~~ | 1. 分普通用户、管理员、Sysinfo 身份 1. 增、删、暂停权限 1. 监控指标查看 1. 查看、修改关键的配置参数 1. 修改关键的配置参数 | 通过 |

#### 6.1.3 测试结论

1. 普通用户无法建立、删除 snode，只有 root 用户可以建立、删除 snode
2. 测试中发现如果普通用户没有 sysinfo 权限，也可以查看 ins_streams\tasks\recalculates系统表，与潘魏讨论，他说：除流计算外，其他系统表也有这样的情况，有的是可能部分表的部分列单独做了限制，流计算的这 3 个表都不限制用户查看
3. 发现问题[TD-35974](https://jira.taosdata.com:18080/browse/TD-35974) ，删除 dnode，但是对应 snode 没有删除掉，已经修复
4. 发现问题[TD-35979](https://jira.taosdata.com:18080/browse/TD-35979)，删除 snosde 出现 crash ，已经修复
5. 发现问题[TD-36016](https://jira.taosdata.com:18080/browse/TD-36016)，停止 dnode 出现 crash，已经修复
6. 发现流计算相关参数最大值存在问题、与潘魏沟通，已经修复
7. 发现好几个参数可以通过 sql 修改（但是重启生效），但与潘魏沟通结果：除了streamBufferSize参数可以使用 alter 修改，其他流计算参数都不支持 SQL 修改
8. 用例snode_mgmt_zlv.py -N 6 --replica 3 、snode_privileges.py、snode_privileges_twodb.py、stream_nosnode.py（ 非 asan 环境下通过，但asan 环境下可能出现内存泄露，目前 ci 中关闭了 asan）
9. 目前流计算没有一个总内存使用参数控制。streamBufferSize参数只有在使用了 trows 才起作用，构造了一个用例[TD-36635](https://jira.taosdata.com:18080/browse/TD-36635)发现可能此参数没起作用，但与潘魏讨论，目前内存泄露会比较多，没法判断是否真正生效，暂时不测试此项了。

### 6.2 流计算对象（吕泽-完成）

#### 6.2.1 测试要点

1. 基础功能
   - 创建、删除`stream`基本操作
   - 语法检查、命名边界、重名问题
   - `ins_streams`、`ins_stream_tasks`等系统表
   - `show streams`命令
   - 禁止在无`snode`或`snode`离线时创建
   - 重复创建与删除同名的`stream`（属性相同或者不同）
2. 状态存取
   - `stream`计算状态的持久化存储
   - `stream`在不同副本的计算状态同步
   - `stream`计算任务的动态分配
   - `stream`各状态下的删除操作，包括暂停、计算中、计算完成等
   - `stream`计算任务的资源控制和检查（如果有）
3. 权限与监控
   - `stream`的创建、删除、暂停权限
   - `stream`的触发表、输出表、查询表及它们所在数据库的读写权限
   - `stream`的审计日志
   - `stream`的监控指标

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | snode_mgmt_zlv.py -N 6 --replica 3 | 管理 1. 创建 stream 1. 删除 stream 1. 查询 information_schema.ins_streams 1. 执行 show 命令 1. 重启并测试元数据持久化状态 1. 暂停、恢复流计算 1. 暂停状态 stream 的删除 1. dnode/snode 离线时的创建、删除 1. dnode/snode 启停时的 stream 中间状态存取 1. Show create (指定 into 数据库与否） | 通过 |
| 2 | steram_nosnode.py stream_checkname.py stream_long_name.py stream_samename.py | 合法性检查 1. 语法合法性检查、语法错误 1. 命名、边界等 1. 无 snode 时的 stream创建 1. 资源控制及其检查 1. 触发表检查 | 通过 |

#### 6.2.3 测试结论

1. 可以跨数据库建立流，触发表和结果表分别在 2 个 DB 中，普通用户需要有目标库的写权限
2. 建流、删流、停止、启动流、手动重算都要有 write 权限
3. 流计算名称、结果表名长度最大 192 字节
4. 流计算名称长度很大情况下，ins_streams显示的 stream_name不完整[TD-36598](https://jira.taosdata.com:18080/browse/TD-36598)，已经修复
5. 重名 stream 可以建立[TD-36600](https://jira.taosdata.com:18080/browse/TD-36600)，已经修复
6. 几个限制：
   - EXPIRED_TIME must be greater than WATERMARK
   - %%trows 后不能使用 where

### 6.3 触发模式（廖浩均）

#### 6.3.1 测试要点

1. 测试不同的触发窗口
  触发的窗口只可以且必须指定一种类型。
   - session
      - 测试是否使用主键列
      - 测试 `session_val` 值是否合法
   - state 
      - 测试状态列是否合法
      - 测试可选项 `TRUE_FOR(duration_time)` 是否存在
         - 测试 `duration_time` 是否合法（如时间单位等）
   - sliding
      - 测试可选项 `[INTERVAL(interval_val[, interval_offset])]` 是否存在
         - 测试 `interval_val` 是否合法
         - 测试 `interval_offset` 是否存在以及是否合法
      - 测试必选项 `SLIDING(sliding_val[, offset_time])` 
         - 测试 `sliding_val` 是否合法
         - 测试 `offset_time` 是否存在以及是否合法
   - event
      - 测试必选项 `START WITH start_condition END WITH end_condition`
         - 测试 `start_condition` 和 `end_condition` 是否合法（如条件中的列是否来自于触发表）
         - 测试只指定 `start_condition` 或 `end_condition` 的场景
         - 测试 `start_condition` 和 `end_condition` 指定了常量条件的场景
      - 测试可选项 `TRUE_FOR(duration_time)` 是否存在
         - 测试 `duration_time` 是否合法
   - count
      - 测试 `count_val` 是否合法
      - 测试可选项 `sliding_val` 是否存在以及是否合法
      - 测试可选项 `[, col1[, ...]]` 是否存在以及是否合法（如条件中的列是否来自于触发表）
   - period
      - 测试 `period_time` 是否合法（支持的时间单位包括：毫秒(a)、秒(s)、分(m)、小时(h)、天(d)，支持的时间范围为[10a, 3650d]）
      - 测试 `offset_time` 是否存在以及是否合法（支持的时间单位包括：毫秒(a)、秒(s)、分(m)、小时(h)）
   - anomaly(不支持的触发类型)
  
1. 测试不同的触发表
   - 是否指定触发表
   - 触发表是否指定 `db_name`
   - 触发表类型
      - 触发表为超级表
      - 触发表为普通表/子表
      - 触发表为虚拟超级表
      - 触发表为虚拟普通表/虚拟子表
      - 触发表为视图/系统表等（非法场景）
2. 测试触发分组
   - 是否存在触发分组
   - 触发分组的列是否来自触发表
   - 触发分组为 tbname, tag, column 的组合。合法的场景为只包含 tbname 以及 tag

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | test_period_1 | 包含 period 触发计算模式 Case 1: 无触发表，超级表聚合结果，写入目标表 （通过） Case 2：高频（10a）无触发表，定时计算超级表结果写入目标表（通过） Case 3：无触发表，计算多行结果+ 常量结果 + 聚合超级表结果写入目标表 （通过） Case 4：无触发表，计算超级表窗口计算结果写入目标表（通过） Case 5：无触发表，计算超级表窗口结果，写入窗口结果中其中一条结果（通过） Case 6：超级表触发，分组聚合超级表结果，写入目标超级表（通过） Case 7：超级表触发，投影（标量）计算超级表分组结果，写入目标超级表（通过） Case 8：超级表分组触发，状态窗口分组聚合计算结果，写入目标超级表（通过） Case 9：超级表分组触发，会话窗口分组聚合计算，写入目标超级表（通过） Case 10：超级表分组触发，计数窗口分组聚合计算，写入目标超级表（回归中） Case 11：虚拟表分组触发，分组聚合结果，写入目标超级表（通过） Case 12： 虚拟超级表分组触发，分组聚合结果，写入目标超级表（通过） Case 13： 边界条件、错误场景、无触发表、非法 period 值检查（通过） |  |
| 2 | test_sliding_1 | 包含 sliding 触发计算模式 Case 1：超级表触发，no interval，定时聚合计算超级表，写入目标表（通过） Case 2：超级表触发，定时聚合窗口结果（interval!=sliding），写入目标表（通过） Case 3：超级表触发，聚合窗口结果（interval!=sliding），写入目标表（回归中） Case 4：超级表触发，10ms窗口聚合结果，写入目标表（通过） Case 5：超级表触发，定时聚合窗口结果，写入目标表（回归中） Case 6：超级表分组触发，定时聚合窗口结果，写入目标超级表（通过） Case 7：超级表分组触发，分组定时聚合窗口结果，写入目标超级表（通过） Case 8：超级表分组触发，分组投影查询，写入目标超级表（通过） Case 9：超级表分组触发，分组状态窗口聚合查询，写入目标超级表（回归中） Case 10：超级表分组触发，分组会话窗口聚合查询，写入目标超级表（回归中） Case 11：超级表分组触发，分组计数窗口聚合查询，写入目标超级表（回归中） Case 12：超级表分组触发，分组聚合查询+新加子表，写入目标超级表（回归中） Case 13：超级表分组触发，标签（非tbname）分组聚合查询，写入目标超级表（通过） Case 14：超级表分组触发，带Option，分组聚合查询，删除再创建同名子表，写入目标超级表（通过） Case 15：超级表分组触发，分组聚合查询+limit/offset/order，写入目标超级表（通过） Case 16：多流并行运行测试（通过） Case 17：虚拟表触发，带Option，子表join后窗口聚合查询，写入目标超级表（通过） Case 18：边界条件、非法参数测试（通过） |  |
| 3 | test_count_new | 包含 count 计算触发模式 Case 0/1/2/3/4/5/6: 列触发 + 偏移 ＋ 乱序写入 + 更新数据 + ( 预过滤+ 强制输出 + fill_history)（通过） Case 7：列触发 + 偏移 ＋ 乱序写入 + 更新数据 （通过） Case 8：列触发 + 偏移 ＋ 乱序写入 + 更新数据 （通过） Case 9：列触发，option + 分组聚合 + 结果表名称 （通过） Case 10：列触发，option + 单表聚合， 结果写入目标表（通过） Case 11： Case 12：虚拟表触发，虚拟表触发数据聚合，结果写入目标表（通过） Case 13：虚拟表分组触发，虚拟表窗口聚合，结果写入目标超级表（通过） Case 14：虚拟超级表分组触发，虚拟超级表窗口聚合，结果写入目标超级表（通过） Case 15：边界条件、错误参数测试（通过） Case 16：单表 tag 列触发，窗口范围聚合，写入目标表（回归中） |  |
| 4 | test_event_new | 包含 event 计算触发模式 Case 1/2/3/4/5/6/7：普通表/超级表+fill_history + 输出表名称 Case 8：普通/超级表分组触发 Case 9：虚拟表触发，顺序+乱序，写入目标表（通过） Case 10：虚拟表触发，虚拟表 join 后窗口聚合，写入目标表（通过） Case 11：多流并行运行测试（通过） Case 12：虚拟超级表触发，虚拟超级表分组窗口聚合，写入目标超级表（通过） Case 13：非法值检查、边界检测测试 |  |
| 5 | test_session_1 | 包含 session 计算触发模式 Case 1：超级表触发，窗口聚合，写入目标表（通过） Case 2：超级表触发，窗口聚合（占位符），写入目标表（通过） Case 3：超级表触发，interval 窗口聚合全部数据集，写入目标表（通过） Case 4：超级表触发，interval 窗口聚合全部数据 + 占位符时间戳，写入目标表（通过） Case 5： 超级表触发，interval 窗口聚合 + 占位符时间戳，写入目标表（通过） Case 6：超级表分组触发，分组数据投影查询，写入目标超级表（通过） Case 7：超级表分组触发，分组计算全部数据状态窗口聚合， 写入目标超级表（通过） Case 8：超级表分组触发，带Option+分组聚合全部数据，写入目标超级表（通过） Case 9：超级表分组触发，分组计算全部数据， 写入目标超级表（通过） Case 10：超级表分组触发，计算不规则时间戳单表数据， 写入目标表（通过） Case 11：超级表分组触发，乱序数据聚合， 写入目标表（通过） Case 12：超级表触发，虚拟表 join 后窗口聚合，写入目标表（通过） Case 13：非法值检查、边界检测测试（通过） |  |
| 6 | test_state_new | 包含 state 计算触发模式 Case 1/2：单表/超级表分组触发，max_delay/true_for，结果写入目标超级表（通过） Case 3/4：单表/超级表分组触发，预过滤/强制输出，结果写入目标超级表（通过） Case 5/6：单表/超级表分组触发，fill_history，结果写入目标超级表（通过） Case 7：单表/超级表分组触发，删除重算，结果写入目标超级表（通过） Case 8：单表/超级表分组触发，乱序/顺序写入，结果写入目标表（通过） Case 9：单表触发、max_delay+true_for，慢写入，结果写入目标表（回归中） Case 10：单表触发、窗口聚合+删除重算， 写入目标表（通过） Case 11：非法值检查、边界检测测试（通过） |  |

### 6.4 控制选项（李珲-完成）

#### 6.4.1 测试要点

1. 测试触发控制条件
  控制条件可以为以下条件的一种或多种的组合。
   - `WATERMARK(duration_time)`：测试 `duration_time` 是否合法以及未指定的场景
   - `EXPIRED_TIME(exp_time)`：测试 `exp_time` 是否合法以及未指定的场景
   - `IGNORE_DISORDER`
   - `DELETE_RECALC`
   - `DELETE_OUTPUT_TABLE`
   - `CALC_NOTIFY_ONLY`
   - `LOW_LATENCY_CALC`
   - `FORCE_OUTPUT`
   - `FILL_HISTORY(start_time)`/`FILL_HISTORY_FIRST(start_time)`：
      - 测试 `start_time` 是否合法以及未指定的场景
      - 测试这两种控制选项同时指定的场景（非法）
      - 测试定时触发（`PERIOD`）下指定这两种选项的场景（非法）
   - `PRE_FILTER(expr)`：
      - 测试 `expr` 是否合法（如过滤的列是否为触发表的列）
      - 测试未指定触发表时指定该选项的场景（非法）
   - `MAX_DELAY(delay_time)`
      - 测试 `delay_time` 是否合法以及未指定的场景
      - 测试触发窗口存在 TRUE_FOR 条件的场景
      - 测试存在 SLIDING 触发以及定时触发时的场景（此时应忽略该选项）
   - `EVENT_TYPE(event_types)`
      - 测试 `event_types` 的不同组合
      - 测试存在 SLIDING 触发以及定时触发时的场景（此时应忽略该选项）

#### 6.4.2 用例列表

- 采用子表 和 超级表 的用例

| # | 测试用例 | 测试描述 | 测试结果 | 备注 |  |
| --- | --- | --- | --- | --- | --- |
| 1 | test_options.py :: Basic0 | WATERMARK | 通过 |  |  |
| 2 | test_options.py :: Basic1 | EXPIRED_TIME | 否 | 错误： 1、子表：过期、未过期都会进行重算； 2、超级表分组：有的分组都重算，有的都不重算。 | [TD-36739](https://jira.taosdata.com:18080/browse/TD-36739) [流计算开发阶段] 流计算state窗口+expired_time(10s)对过期的乱序数据也进行了重算 |
| 3 | test_options.py :: Basic2 | IGNORE_DISORDER | 通过 |  |  |
| 4 | test_options.py :: Basic3 | DELETE_RECALC | 通过 |  |  |
| 5 | test_options.py :: Basic4 | DELETE_OUTPUT_TABLE | 否 | 功能还没有开发完成 | [TD-36305](https://jira.taosdata.com:18080/browse/TD-36305) [流计算开发阶段] 流计算state窗口+超级表%%rows+delete_output_table没有删除结果表 |
| 6 | test_options.py :: Basic5 | FILL_HISTORY | 通过 |  |  |
| 7 | test_options.py :: Basic6 | FILL_HISTORY_FIRST | 通过 | 需要在大量历史数据的情况下才能验证。 功能用例做个简单。 |  |
| 8 | test_options.py :: Basic7 | CALC_NOTIFY_ONLY | 通过 |  |  |
| 9 | test_options.py :: Basic8 | LOW_LATENCY_CALC | - | 目前缺省就是这个模式，批量计算的还没有实现。 先不测试。 |  |
| 10 | test_options.py :: Basic9 | PRE_FILTER | 通过 |  |  |
| 11 | test_options.py :: Basic10 | FORCE_OUTPUT | 通过 |  |  |
| 12 | test_options.py :: Basic11 test_options.py :: Basic11_1 | MAX_DELAY | 通过 | 还需要补充用例： 1、sliding(无interval) 和 period 忽略 max_delayl; 2、max_delay 与 true_for 同时配置时，且 true_for 大于 max_delay时，max_delay 仍然生效 | [TD-36325](https://jira.taosdata.com:18080/browse/TD-36325) [流计算开发阶段] 流计算event窗口+max_delay+分组的结果表中记录数少于预期 |
| 13 | test_options.py :: Basic12 | EVENT_TYPE | 通过 |  |  |
| 14 | test_options.py :: Basic13 | IGNORE_NODATA_TRIGGER | 通过 |  |  |


- 采用虚拟子表 和 虚拟超级表 的用例

| # | 测试用例 | 测试描述 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- |
| 1 | test_options_vtbl.py :: Basic0 | WATERMARK | 通过 |  |
| 2 | test_options_vtbl.py :: Basic1 | EXPIRED_TIME | 否 |  |
| 3 | test_options_vtbl.py :: Basic2 | IGNORE_DISORDER | 通过 |  |
| 4 | test_options_vtbl.py :: Basic3 | DELETE_RECALC | 通过 |  |
| 5 | test_options_vtbl.py :: Basic4 | DELETE_OUTPUT_TABLE | 否 | [TD-36305](https://jira.taosdata.com:18080/browse/TD-36305) [流计算开发阶段] 流计算state窗口+超级表%%rows+delete_output_table没有删除结果表 |
| 6 | test_options_vtbl.py :: Basic5 | FILL_HISTORY | 通过 |  |
| 7 | test_options_vtbl.py :: Basic6 | FILL_HISTORY_FIRST | 通过 |  |
| 8 | test_options_vtbl.py :: Basic7 | CALC_NOTIFY_ONLY | 通过 |  |
| 9 | test_options_vtbl.py :: Basic8 | LOW_LATENCY_CALC | - |  |
| 10 | test_options_vtbl.py :: Basic9 | PRE_FILTER | 通过 |  |
| 11 | test_options_vtbl.py :: Basic10 | FORCE_OUTPUT | 否 |  |
| 12 | test_options_vtbl.py :: Basic11 test_options_vtbl.py :: Basic11_1 | MAX_DELAY | 通过 |  |
| 13 | test_options_vtbl.py :: Basic12 | EVENT_TYPE | 通过 |  |
| 14 | test_options_vtbl.py :: Basic13 | IGNORE_NODATA_TRIGGER | 通过 |  |

- 异常用例

| # | 测试用例 | 测试描述 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- |
| 1 | test_options_abnormal .py :: Basic0 | watermark 应该小于 expired_time | 通过 |  |
| 2 |  | watermark 必须是整数 | 通过 |  |
| 3 |  | fill_history 非法时间 | 通过 |  |
| 4 |  | fill_history 与 fill_history_first 不能同时指定 | 通过 |  |
| 5 |  | period 不能指定 fill_history | 通过 |  |
| 6 |  | period 不能指定 fill_history_first | 通过 |  |
| 7 |  | pre_filter 不能使用非触发表的列 | 通过 |  |
| 8 |  | 一个 tag 列作为分组，这个列的值不允许修改 | 通过 |  |
| 9 | test_options_abnormal_vtbl.py :: Basic0 | watermark 应该小于 expired_time | 通过 |  |
| 10 |  | watermark 必须是整数 | 通过 |  |
| 11 |  | fill_history 非法时间 | 通过 |  |
| 12 |  | fill_history 与 fill_history_first 不能同时指定 | 通过 |  |
| 13 |  | period 不能指定 fill_history | 通过 |  |
| 14 |  | period 不能指定 fill_history_first | 通过 |  |
| 15 |  | pre_filter 不能使用非触发表的列 | 通过 |  |
| 16 |  | 一个 tag 列作为分组，这个列的值不允许修改 | 通过 |  |

#### 6.4.3 测试结论

1. 控制项 除了 LOW_LATENCY_CALC 外， 其他都进行了测试，又分别针对 子表、超级表、虚拟子表、虚拟超级表进行用例测试。
2. 每个文件中包含多个用例，目前单个用例能跑过，但同时打开并发运行时，还会出现问题。所以提交到github上的文件，虽然有些用例标注了 [ok]，但还是注释状态。需要后续单个用例全部都能跑过后，再全部打开一起运行进行验证。
3. 到2025-07-23 18:00:00 截止，遗留未解决的问题如下：
| 关键字 | 概要 | 状态 | 经办人 | Owner | 报告嗯 |
| --- | --- | --- | --- | --- | --- |
| [TD-36984](https://jira.taosdata.com:18080/browse/TD-36984) | [[流计算开发阶段]虚拟表触发多出一个没有的窗口](https://jira.taosdata.com:18080/browse/TD-36984) | NEW | [Mingming Wang](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=mmwang) | [Mingming Wang](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=mmwang) | [Hui Li](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=huili) |
| [TD-36979](https://jira.taosdata.com:18080/browse/TD-36979) | [[流计算开发阶段] nevados用户流计算场景模拟-1条sql中700条记录，流计算出结果非常慢](https://jira.taosdata.com:18080/browse/TD-36979) | NEW | [Xinsheng Ren](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=xsren) | [Xinsheng Ren](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=xsren) | [Hui Li](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=huili) |
| [TD-36976](https://jira.taosdata.com:18080/browse/TD-36976) | [[流计算开发阶段] nevados用户流计算场景模拟-state窗口触发与预期不符](https://jira.taosdata.com:18080/browse/TD-36976) | Processing | [Wei Pan](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=wpan) | [Wei Pan](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=wpan) | [Hui Li](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=huili) |
| [TD-36792](https://jira.taosdata.com:18080/browse/TD-36792) | [[流计算开发阶段] 虚拟超级表+新增和删除流计算输出结果超级表列，再写入新数据满足窗口输出，但结果表中没有新的记录](https://jira.taosdata.com:18080/browse/TD-36792) | Processing | [Xinsheng Ren](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=xsren) | [Xinsheng Ren](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=xsren) | [Hui Li](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=huili) |
| [TD-36750](https://jira.taosdata.com:18080/browse/TD-36750) | [[流计算开发阶段] 虚拟表+删除pre_filter(cbigint >=1)中cbigint列后，应该没有符合条件的数据了，不应该再触发计算窗口](https://jira.taosdata.com:18080/browse/TD-36750) | NEW | [Kane Kuang](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=Kane Kuang) | [Kane Kuang](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=Kane Kuang) | [Hui Li](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=huili) |
| [TD-36739](https://jira.taosdata.com:18080/browse/TD-36739) | [[流计算开发阶段] 流计算state窗口+expired_time(10s)对过期的乱序数据也进行了重算](https://jira.taosdata.com:18080/browse/TD-36739) | Processing | [Kane Kuang](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=Kane Kuang) | [Kane Kuang](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=Kane Kuang) | [Hui Li](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=huili) |
| [TD-36727](https://jira.taosdata.com:18080/browse/TD-36727) | [[流计算开发阶段] 创建流之后增加新的虚拟子表，没有预期触发生成结果表](https://jira.taosdata.com:18080/browse/TD-36727) | NEW | [Kane Kuang](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=Kane Kuang) | [Kane Kuang](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=Kane Kuang) | [Hui Li](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=huili) |
| [TD-36673](https://jira.taosdata.com:18080/browse/TD-36673) | [[流计算开发阶段] 10个单跑都成功的用例同时运行，随机出现各种异常](https://jira.taosdata.com:18080/browse/TD-36673) | Processing | [Wei Pan](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=wpan) | [Kane Kuang](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=Kane Kuang) | [Hui Li](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=huili) |
| [TD-36579](https://jira.taosdata.com:18080/browse/TD-36579) | [[流计算开发阶段] ignore_disorder控制乱序和更新数据，delete_recalc 控制删除数据](https://jira.taosdata.com:18080/browse/TD-36579) | Processing | [Kane Kuang](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=Kane Kuang) | [Kane Kuang](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=Kane Kuang) | [Hui Li](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=huili) |
| [TD-36573](https://jira.taosdata.com:18080/browse/TD-36573) | [[流计算开发阶段] expired_time(10d)未过期数据有乱序数据时，窗口计算结果不正确。](https://jira.taosdata.com:18080/browse/TD-36573) | Processing | [Kane Kuang](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=Kane Kuang) | [Kane Kuang](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=Kane Kuang) | [Hui Li](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=huili) |
| [TD-36528](https://jira.taosdata.com:18080/browse/TD-36528) | [[流计算开发阶段] 历史数据在建流前被删除，但建流后的结果表还对删除数据进行了计算](https://jira.taosdata.com:18080/browse/TD-36528) | Processing | [Kane Kuang](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=Kane Kuang) | [Kane Kuang](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=Kane Kuang) | [Hui Li](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=huili) |
| [TD-36525](https://jira.taosdata.com:18080/browse/TD-36525) | [[流计算开发阶段] 删除流结果表后继续触发了也没有重建，不符合预期](https://jira.taosdata.com:18080/browse/TD-36525) | Processing | [Xinsheng Ren](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=xsren) | [Xinsheng Ren](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=xsren) | [Hui Li](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=huili) |
| [TD-36305](https://jira.taosdata.com:18080/browse/TD-36305) | [[流计算开发阶段] 流计算state窗口+超级表%%rows+delete_output_table没有删除结果表](https://jira.taosdata.com:18080/browse/TD-36305) | NEW | [Joey Sima](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=Joey Sima) | [Joey Sima](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=Joey Sima) | [Hui Li](https://jira.taosdata.com:18080/secure/ViewProfile.jspa?name=huili) |


### 6.5 通知机制（廖浩均）

#### 6.5.1 测试要点

1. 测试不同触发模式下通知是否正常工作。
   - 不同的触发模式以及选项见 6.3 节
2. 测试 `url [, ...]`  （代码没有进行该部分检查）
   - 测试 `url` 是否合法 （必须包括协议、IP 或域名、端口号）
   - 测试 `url` 包含路径、参数的情况（合法）
   - 测试不同的协议（目前只支持 websocket）
3. 测试 `[ON (event_types)]`
   - 测试该选项是否存在
      - 测试 `SLIDING`和 `PERIOD` 触发模式的场景（可以不存在）
      - 测试其他触发模式的场景（必须存在）
   - 测试不同的事件类型
    可以为以下事件类型的一种或多种的组合
      - `WINDOW_OPEN`
      - `WINDOW_CLOSE`
1. 测试 `[WHERE condition]`
   - 测试该选项是否存在
   - 测试 `condition` 
      - 只指定含计算结果列和（或）常量的条件（合法）
      - 其他（非法）
2. 测试 `[NOTIFY_OPTIONS(notify_option[|notify_option)]]`
   - 测试该选项是否存在
   - 测试不同的 `notify_option`
    可以为以下通知选项的一种或多种的组合
      - `NOTIFY_HISTORY`
      - `ON_FAILURE_PAUSE`

#### 6.5.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | test_notify | Case 1: window_open|window_close 、notify_history|on_failure_pause 组合测试（通过） Case 2： 多种数据类型查询结果 notify （通过） Case 3：多流并行 notify 测试，部分开启 on_failure_pause， 关闭 notify server （通过） Case 4/5：开启 window_open|window_close，多种类型数据查询结果，测试（通过） Case 6：多流并行，不同触发类型流计算（通过） Case 7：虚拟表触发，推送结果测试（通过） Case 8：开启 CALC_NOTIFY_ONLY 测试 （通过） Case 9：开启notify_history， 搭配历史数据测试（通过） Case 10： 标量查询结果 notify 测试（回归中） Case 11：大批量写入 + count_window(1)， 推送测试（回归中） Case 12：边界条件、非法 SQL 测试 |  |

### 6.6 结果保存（鲍之骁-完成）

#### 6.6.1 测试要点

1. 测试 `[INTO [db_name.]table_name]`
   - 测试该选项是否存在
      - 只触发通知不计算场景、计算结果只通知不保存输出的场景可以不存在该选项
      - 其他场景必须存在该选项
   - 测试是否指定 `db_name`
   - 测试不同触发条件下输出表的类型
      - 存在触发分组：输出表为超级表
      - 不存在触发分组：输出表为普通表
   - 测试输出表已经存在的场景
      - 已存在的表和输出表类型相同
      - 已存在的表和输出表类型不同
2. 测试 `[OUTPUT_SUBTABLE(tbname_expr)]`
   - 测试该选项是否存在
      - 有触发分组且存在（合法）
      - 有触发分组且不存在（合法）
      - 没有触发分组且存在（非法）
      - 没有触发分组且不存在（合法）
   - 测试该选项的列是否来自触发表的分组列
   - 测试该选项的 `tbname_expr` 是否为输出为字符串的表达式
   - 测试输出长度大于表最大长度的场景（截断处理）
3. 测试`[(column_name1, column_name2 [PRIMARY KEY][, ...])]`
   - 测试该选项是否存在
      - 存在该选项
         - 测试输出表是否已存在
            - 输出表存在且列名与已存在的表一致（合法）
            - 输出表存在且列名与已存在的表不一致（不合法）
            - 输出表不存在（合法）
         - 测试是否指定 `[PRIMARY KEY]`
            - 指定 `[PRIMARY KEY]`
               - 第二列为整型或字符串类型（合法）
               - 第二列为其他类型（非法）
               - 指定第三列为 PRIMARY KEY(非法)
            - 不指定
      - 不存在该选项
         - 测试默认的输出表的列名与计算结果的列名是否相同
4. 测试`[TAGS (tag_definition [, ...])]`
   - 测试该选项是否存在
      - 存在该选项
         - 测试输出表是否已存在
            - 输出表存在且 tag 类型与名称都与已存在的表一致（合法）
            - 输出表存在且 tag 类型与名称与已存在的表不一致（非法）
            - 输出表不存在（合法）
      - 不存在该选项
         - 测试默认的 tag 列定义和值是否与触发分组列一一对应
         - 测试按表分组时产生的 tag 列的名字是否为 `tag_tbname`
   - 测试是否指定分组列
      - 指定分组列（合法）
      - 未指定分组列（非法）
   - 测试 tag 指定的 `expr` 是否为来自触发分组的列
   - 测试是否指定 `[COMMENT 'string_value']`
   - 测试生成的列名称在指定/不指定情况下的正确性

#### 6.6.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 仅触发通知，不触发计算场景 | 通过 |
| 2 | 是否支持指定 db_name | 通过 |
| 3 | 测试不同触发条件下的输出表类型 | 通过 |
| 4 | 测试输出表已经存在场景 | 通过 |
| 5 | 测试 `OUTPUT_SUBTABLE` 和触发分组的结合使用 | 通过 |
| 6 | 测试 `OUTPUT_SUBTABLE` 的列是否来自触发表分组列 | 通过 |
| 7 | 测试`OUTPUT_SUBTABLE`的 `tbname_expr` 是否为输出为字符串的表达式 | 通过 |
| 8 | 测试`OUTPUT_SUBTABLE`输出长度大于表最大长度的场景（截断处理） | 通过 |
| 9 | 测试指定 column_name 的基本场景 | 通过 |
| 10 | 测试指定 column_name 同时指定 `[PRIMARY KEY]` | 通过 |
| 11 | 测试指定 TAGS 的基本场景 | 通过 |
| 12 | 测试指定 TAGS 是否指定分组列 | 通过 |
| 13 | 测试指定 TAGS 同时指定 COMMENT | 通过 |

#### 6.6.3 测试结论

共创建流 24 个，依次验证结果正确性，共发现 bug 5 个, 目前均已经修复。

TD-36330


TD-36680


TD-36396


TD-36434


TD-36685

### 6.7 查询子句（关胜亮）

#### 6.7.1 测试要点

本节测试`AS subquery`子句的语法、结果、边界。选择如下维度进行合理数目的排列组合。

##### 6.7.1.1 基础测试

分成 6 个用例，每个用例约 创建 400 个不同的流
1. 测试各触发方式都可使用`SubQuery`，从各触发方式中选择一种典型的实例，合计 6 个维度
   - `sliding`
   - `session`
   - `state`
   - `event`
   - `count`
   - `period`
2. 测试各分组情况都可使用`SubQuery`，从各分组方式中选择一种典型的实例，合计 4 个维度
   - 无分组
   - 按 tbname 分组
   - 按 标签分组
   - 按 普通列分组
3. 测试任意类型的查询语句，按如下维度组合成 50 个 SQL 语句
   - 表：系统表、超级表、子表、普通表、虚拟超级表、虚拟子表
   - 查询：投影查询、嵌套查询、关联查询、窗口查询（时间、事件、计数、会话、状态）、Show 命令、GroupBy、PartitionBy, OrderBy, Limit, Slimit, Union
   - 函数：单行函数（数学函数、字符串函数、转换函数、时间函数）、聚合函数、选择函数、时序特有函数、地理信息函数、系统信息函数
   - 筛选：时间比较、普通列比较、标签列比较
   - 运算符：数学运算符、字符串、位、比较、逻辑运算符、JSON 运算符
   - 其他：与触发表相同/不同的库表查询、视图查询
4. 测试查询结果集，在第 3 步的查询语句中，包含如下结果集的组合
   - 使用所有数据类型，包括数值型、二进制型、字符串型、地理信息型等
   - 使用所有伪列，包括`_qstart`、`_qend`、`_wstart`、`_wend`、`_wduration`、`_c0`、`_rowts`、`irowts`、`_irowtsorigin`、`tbname`
   - 包含普通列、标签列、主键列
   - 结果集随机包含`None`和`NULL`
   - 结果集数目：`1`条、`n`条
   - 结果集包含重复时间戳
5. 测试占位符的使用，在第 3 步的查询语句中，包含如下占位符的使用
   - 在`FROM`、`SELECT`、`WHERE`等各种可能的位置使用占位符
   - 使用每个占位符：`_twstart`、`_twend`、`_twduration`、`_twrownum`、`_tcurrent_ts`、`_tgrpid`、`_tlocaltime`、`%%n`、`%%tbname`、`%%trows `
6. 检查项
   - 按`schema`自动创建输出表
   - 按照给定的查询语句读取数据并写入到输出表中，验证结果正确性
   - 关注占位符数据的准确性，例如`%%trows`

##### 6.7.1.2 限制条件

验证各占位符的使用限制，例如
1. 非窗口触发不能使用`_twstart`、`_twend`、`_twduration`、`_twrownum`
2. 非滑动触发不能使用 `_tcurrent_ts`、`_tprev_ts`、`_tnext_ts`
3. 仅定时触发可以使用 `_tprev_localtime`、`_tnext_localtime`
4. `%%trows`只能用于 `FROM`子句
5. 其他占位符只能用于 `SELECT` 和 `WHERE` 子句
6. `%%n` 中`n`的取值范围
7. 拼写错误的占位符
8. 不允许 insert 或其他不返回结果集的语句

#### 6.7.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | test_subquery_basic.py | 基本的测试用例，作为后续测试用例编写的基础 | 通过 |
| 2 | test_subquery_sliding.py | 通过 |
| 3 | test_subquery_session.py | 进行中 |
| 4 | test_subquery_state.py | 进行中 |
| 5 | test_subquery_event.py | 进行中 |
| 6 | test_subquery_count.py | 进行中 |
| 7 | test_subquery_period.py | 进行中 |
| 8 | test_subquery_limit.py | 参照 6.7.1.2 章节 | 通过 |

#### 6.7.3 测试结论

测试进行中，已确定有如下注意事项：
1. 以下函数不能在子查询中使用（未来不支持）
```bash {wrap}
 CLIENT_VERSION()
 CURRENT_USER()
 SERVER_STATUS()
 SERVER_VERSION()
 DATABASE() 
```

1. 以下函数不能在子查询中使用（后续支持）
```bash {wrap}
INTERP
PERCENTILE 
```

1. 以下数据类型不能在子查询中使用（后续支持）
```bash {wrap}
Geometry: 不影响使用，已创建 JIRA TD-35766，低优先级处理
```

### 6.8 重算机制（鲍之骁 完成）

#### 6.8.1 测试要点

1. 不同窗口下的重算机制验证
   - 乱序数据
      - 计数窗口触发 （忽略不处理）
      - 定时触发 滑动触发 （忽略不处理）
      - 其他窗口触发（默认：通过重算进行处理，可通过选项忽略）
   - 数据更新 （处理规则同乱序数据）
   - 数据删除
      - 计数窗口触发 （忽略不处理）
      - 定时触发 滑动触发 （忽略不处理）
      - 其他窗口触发（默认：忽略不处理，可选选项：当作乱序数据处理）
2. `EXPIRED_TIME(exp_time)`
   - 触发表写入未过期的乱序，更新数据 正确触发重算
   - 触发表写入过期的乱序，更新数据 不触发重新计算
   - 以上场景结合 part 1 (不同窗口下的重算机制验证) 做结果验证
3. `IGNORE_DISORDER` 
   - 触发表写入过期的乱序，更新数据 不触发重新计算
   - 以上场景结合 part 1 (不同窗口下的重算机制验证) 做结果验证
4. `DELETE_RECALC`
   - 删除触发表数据 触发重新计算
   - 以上场景结合 part 1 (不同窗口下的重算机制验证) 做结果验证
5. 测试选项 `WATERMARK(duration_time)`
   - 在容忍数据乱序的时间区间内写入乱序数据 不触发重算
   - 在容忍数据乱序的时间区间外写入乱序数据 触发重算
   - 更新最新的事件事件（写入新数据），测试触发表已关闭的窗口是否参与触发，并且数据进行了重算
   - 以上场景结合 part 1 (不同窗口下的重算机制验证) 做结果验证
6. 参数组合使用
   - 测试 `EXPIRED_TIME` 和 `WATERMARK` 组合使用。
   - 测试 `IGNORE_DISORDER` 和 `WATERMARK` 组合使用。
   - 测试 `DELETE_RECALC` 和 `EXPIRED_TIME` 组合使用。
   - 测试 `WATERMARK` ， `DELETE_RECALC` 和 `EXPIRED_TIME` 组合使用。
7. 手动重算
   - 测试手动重算的基础功能。
   - 测试手动重算与各种参数的组合使用。

#### 6.8.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | test_recalc_expired_time.py | 测试参数 `EXPIRED_TIME` 。 1. 未过期数据可以正确触发重算，并且计算结果正确。 2. 过期数据不触发重算。 | 通过 |
| 2 | test_recalc_ignore_disorder.py | 测试参数 `IGNORE_DISORDER` 所有的乱序数据都不触发重算。 | 通过 |
| 3 | test_recalc_delete_recalc.py | 测试参数 `DELETE_RECALC` 删除数据时触发重算。 | 通过 |
| 4 | test_recalc_watermark.py | 测试参数 `WATERMARK`。 1. 写入乱序容忍区间内的乱序数据。 2. 写入乱序容忍区间外的乱序数据。 3. 写入新数据，观察 watermark 是否正确推进。 | 通过 |
| 5 | test_recalc_combined_options.py | 测试使用和重算相关的几个参数组合。 | 通过 |
| 6 | test_recalc_manual.py | 等待研发修复问题 |
| 7 | test_recalc_manual_with_options.py | 等待研发修复问题 |

#### 6.8.3 测试结论

共发现问题 18 个，目前都已经解决。手动重算功能和一些参数组合使用时，可能会不生效，希望研发人员在空闲之余可以完善这部分的文档。
1. 
  TD-36471

1. 
  TD-36495

1. 
  TD-36556

1. 
  TD-36568

1. 
  TD-36602

1. 
  TD-36643

1. 
  TD-36651

1. 
  TD-36652

1. 
  TD-36658

1. 
  TD-36675

1. 
  TD-36747

1. 
  TD-36819

1. 
  TD-36691

1. 
  TD-37160

1. 
  TD-37154

1. 
  TD-37105

1. 
  TD-37084

1. 
  TD-37077

## 7. 特殊场景测试（李珲-完成）

### 7.1 数据库时间精度

#### 7.1.1 测试要点

#### 7.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 | 备注 | 对应虚拟表 |  |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | test_options_us.py :: Basic0 | WATERMARK | 进行中 |  |  |  |
| 2 | test_options_us.py :: Basic1 | EXPIRED_TIME | 进行中 |  |  |  |
| 3 | test_options_us.py :: Basic2 | IGNORE_DISORDER | 进行中 |  |  |  |
| 4 | test_options_us.py :: Basic3 | DELETE_RECALC | 进行中 |  |  |  |
| 5 | test_options_us.py :: Basic4 | DELETE_OUTPUT_TABLE | 进行中 |  |  |  |
| 6 | test_options_us.py :: Basic5 | FILL_HISTORY | 进行中 |  |  |  |
| 7 | test_options_us.py :: Basic6 | FILL_HISTORY_FIRST | 进行中 |  |  |  |
| 8 | test_options_us.py :: Basic7 | CALC_NOTIFY_ONLY | 进行中 |  |  |  |
| 9 | test_options_us.py :: Basic8 | LOW_LATENCY_CALC | - |  |  |  |
| 10 | test_options_us.py :: Basic9 | PRE_FILTER | 进行中 |  |  |  |
| 11 | test_options_us.py :: Basic10 | FORCE_OUTPUT | 进行中 |  |  |  |
| 12 | test_options_us.py :: Basic11 test_options_us.py :: Basic11_1 | MAX_DELAY | 进行中 |  |  |  |
| 13 | test_options_us.py :: Basic12 | EVENT_TYPE | 进行中 |  |  |  |
| 14 | test_options_us.py :: Basic13 | IGNORE_NODATA_TRIGGER | 进行中 |  |  |  |


| # | 测试用例 | 测试描述 | 测试结果 | 备注 | 对应虚拟表 |  |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | test_options_ns.py :: Basic0 | WATERMARK | 进行中 |  |  |  |
| 2 | test_options_ns.py :: Basic1 | EXPIRED_TIME | 进行中 |  |  |  |
| 3 | test_options_ns.py :: Basic2 | IGNORE_DISORDER | 进行中 |  |  |  |
| 4 | test_options_ns.py :: Basic3 | DELETE_RECALC | 进行中 |  |  |  |
| 5 | test_options_ns.py :: Basic4 | DELETE_OUTPUT_TABLE | 进行中 |  |  |  |
| 6 | test_options_ns.py :: Basic5 | FILL_HISTORY | 进行中 |  |  |  |
| 7 | test_options_ns.py :: Basic6 | FILL_HISTORY_FIRST | 进行中 |  |  |  |
| 8 | test_options_ns.py :: Basic7 | CALC_NOTIFY_ONLY | 进行中 |  |  |  |
| 9 | test_options_ns.py :: Basic8 | LOW_LATENCY_CALC | - |  |  |  |
| 10 | test_options_ns.py :: Basic9 | PRE_FILTER | 进行中 |  |  |  |
| 11 | test_options_ns.py :: Basic10 | FORCE_OUTPUT | 进行中 |  |  |  |
| 12 | test_options_ns.py :: Basic11 test_options_ns.py :: Basic11_1 | MAX_DELAY | 进行中 |  |  |  |
| 13 | test_options_ns.py :: Basic12 | EVENT_TYPE | 进行中 |  |  |  |
| 14 | test_options_ns.py :: Basic13 | IGNORE_NODATA_TRIGGER | 进行中 |  |  |  |

#### 7.1.3 测试结论

将 test_options.py 中的所有用例的数据库都分别创建为 us、ns 精度进行运行。解决了发现与精度相关的问题后，还不能全部通过的原因是共性的。等  test_options.py 能全部通过后，这两个文件中的用例，也同步全部打开运行验证。

### 7.2 数据乱序、更新、删除

#### 7.2.1 测试要点

#### 7.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- |
| 1 | test_disorderUpdateDelete.py::Basic0 | fill_history | delete_recalc: 历史数据中有乱序和更新、删除的情况 | 通过 |  |
|  |  | fill_history | delete_recalc: 实时数据中有乱序和更新、删除的情况 | 通过 |  |
| 2 | test_disorderUpdateDelete.py::Basic1 | fill_history: 历史数据中有乱序和更新、删除的情况 | 否 | [TD-36528](https://jira.taosdata.com:18080/browse/TD-36528) [流计算开发阶段] 历史数据在建流前被删除，但建流后的结果表还对删除数据进行了计算 |
| 3 | test_disorderUpdateDelete.py::Basic2 | delete_recalc | expire_time(10d): 实时数据 和 未过期数据中有乱序和更新、删除的情况 | 否 | [TD-36573](https://jira.taosdata.com:18080/browse/TD-36573) [流计算开发阶段] expired_time(10d)未过期数据有乱序数据时，窗口计算结果不正确。 |
| 4 | test_disorderUpdateDelete.py::Basic3 | delete_recalc ， 但有 ignore_disorder : 即不对删除数据重算，也不对乱序/更新数据重算 | 否 | 目前行为： ignore_disorder 和 delete_recalc 都没有指定时，只对乱序和更新数据进行重算，删除数据不重算； ignore_disorder 和 delete_recalc 都指定时，对乱序、更新、删除数据都不再进行重算； 潘魏最后确定： ignore_disorder控制乱序和更新数据重算，delete_recalc 控制删除数据的重算。 两个参数分别控制不同的数据变更。 |
| 5 | test_disorderUpdateDelete_vtbl.py::Basic0 | fill_history | delete_recalc: 历史数据中有乱序和更新、删除的情况 | 进行中 |  |
|  |  | fill_history | delete_recalc: 实时数据中有乱序和更新、删除的情况 | 进行中 |  |
| 6 | test_disorderUpdateDelete_vtbl.py::Basic1 | fill_history: 历史数据中有乱序和更新、删除的情况 | 进行中 |  |
| 7 | test_disorderUpdateDelete_vtbl.py::Basic2 | delete_recalc | expire_time(10d): 实时数据 和 未过期数据中有乱序和更新、删除的情况 | 进行中 |  |
| 8 | test_disorderUpdateDelete_vtbl.py::Basic3 | delete_recalc ， 但有 ignore_disorder : 即不对删除数据重算，也不对乱序/更新数据重算 | 进行中 |  |

#### 7.2.3 测试结论

目前还只跑通了一个用例，遗漏未解决的问题：
[TD-36528](https://jira.taosdata.com:18080/browse/TD-36528) [流计算开发阶段] 历史数据在建流前被删除，但建流后的结果表还对删除数据进行了计算
[TD-36573](https://jira.taosdata.com:18080/browse/TD-36573) [流计算开发阶段] expired_time(10d)未过期数据有乱序数据时，窗口计算结果不正确。
[TD-36579](https://jira.taosdata.com:18080/browse/TD-36579) [流计算开发阶段] ignore_disorder控制乱序和更新数据，delete_recalc 控制删除数据

### 7.3 数据库表等元数据的更新、删除

#### 7.3.1 测试要点

例如 分组发生变化时的结果生成，新增删除子表等

#### 7.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- |
| 1 | 在超级表下新建子表 | 通过 |  |
|  | 在超级表下删除子表 | 通过 |  |
|  | 删除普通触发表 | 通过 |  |
| 2 | test_meta.py::Basic1 | 触发表 和 数据源表 分离时，删除数据源表 | 通过 |  |
| 3 | tag过滤时，修改tag的值，从不满足流条件，到满足流条件 | 通过 |  |
|  | tag过滤时，修改tag的值，从满足流条件，到不满足流条件 | 通过 |  |
|  | tag过滤时，修改tag的值，从满足流条件A，到满足流条件B | 通过 |  |
| 4 | test_meta.py::Basic3 | 在触发表中新增一个数据列，删除一个过滤条件的数据列、删除一个过滤条件中没有的数据列 | 通过 | 删除过滤条件中的列后，仍然会生成结果，[TD-36509](https://jira.taosdata.com:18080/browse/TD-36509) |
| 5 | 触发表 和 数据源表 分离时，删除数据源表中在查询中使用到的数据列 和 tag 列 | 通过 |  |
|  | 触发表 和 数据源表 分离时，删除数据源表中在查询没有使用到的数据列 和 tag 列 | 通过 |  |
| 6 | 修改普通触发表使用到的列名 | 通过 | 修改列名的不受影响（因为内部是用 id 来处理的，修改列名，id不会变） |
|  | 修改普通数据源表中使用到的列名 | 通过 | 修改列名的不受影响（因为内部是用 id 来处理的，修改列名，id不会变） |
| 7 | 删除输出流结果表 | 否 | [TD-36525](https://jira.taosdata.com:18080/browse/TD-36525) |
|  | 修改输出流结果表 1、对输出的超级表结果表：增加列、删除列 2、对输出的普通表：增加列、删除列 | 否 | 潘魏确认：可以对流结果表进行 增加列、删除列 流结果继续保存，新增列和删除列都是 NULL |
| 8 | 流 在db1、流的触发表 在 db1、流的数据源表 在 db1 删除db1 | 通过 |  |
|  | 流 在db2、流的触发表 在 db3、流的数据源表 在 db4，流结果表在db5 删除db2、db3、db4、db5 | 通过 | 在没有删除db2时，不能删除db3、db4，可以删除db5 |
|  |  |  |  |  |
| 9 | 在超级表下新建子表 | 否 |  |
|  | 在超级表下删除子表 | 进行中 |  |
|  | 删除普通触发表 | 进行中 |  |
| 10 | test_meta_vtbl.py::Basic1 | 触发表 和 数据源表 分离时，删除数据源表 | 进行中 |  |
| 11 | tag过滤时，修改tag的值，从不满足流条件，到满足流条件 | 进行中 |  |
|  | tag过滤时，修改tag的值，从满足流条件，到不满足流条件 | 进行中 |  |
|  | tag过滤时，修改tag的值，从满足流条件A，到满足流条件B | 进行中 |  |
| 12 | test_meta_vtbl.py::Basic3 | 在触发表中新增一个数据列，删除一个过滤条件的数据列、删除一个过滤条件中没有的数据列 | 进行中 |  |
| 13 | 触发表 和 数据源表 分离时，删除数据源表中在查询中使用到的数据列 和 tag 列 | 通过 |  |
|  | 触发表 和 数据源表 分离时，删除数据源表中在查询没有使用到的数据列 和 tag 列 | 通过 |  |
| 14 | 修改普通触发表使用到的列名 | 进行中 |  |
|  | 修改普通数据源表中使用到的列名 | 进行中 |  |
| 15 | 删除输出流结果表 | 进行中 |  |
|  | 修改输出流结果表 1、对输出的超级表结果表：增加列、删除列 2、对输出的普通表：增加列、删除列、修改列名 | 进行中 |  |
| 16 | 流 在db1、流的触发表 在 db1、流的数据源表 在 db1 删除db1 | 通过 |  |
|  | 流 在db2、流的触发表 在 db3、流的数据源表 在 db4，流结果表在db5 删除db2、db3、db4、db5 | 通过 |  |

### 7.4 测试结论

1. 元数据变更，覆盖了数据库删除、触发表、数据源表的增删、列的增删改等的测试用例，包括子表、超级表、虚拟子表、虚拟超级表。遗留未解决的问题如下：
[TD-36525](https://jira.taosdata.com:18080/browse/TD-36525) [流计算开发阶段] 删除流结果表后继续触发了也没有重建，不符合预期
[TD-36727](https://jira.taosdata.com:18080/browse/TD-36727) [流计算开发阶段] 创建流之后增加新的虚拟子表，没有预期触发生成结果表
[TD-36984](https://jira.taosdata.com:18080/browse/TD-36984) [流计算开发阶段]虚拟表触发多出一个没有的窗口
[TD-36750](https://jira.taosdata.com:18080/browse/TD-36750) [流计算开发阶段] 虚拟表+删除pre_filter(cbigint >=1)中cbigint列后，应该没有符合条件的数据了，不应该再触发计算窗口
[TD-36525](https://jira.taosdata.com:18080/browse/TD-36525) [流计算开发阶段] 删除流结果表后继续触发了也没有重建，不符合预期

## 8. 用户场景测试

按照用户场景构建数据模型和写入模式，用较小规模的数据集验证功能可用性。

### 8.1 电表场景（段宽军-完成）

#### 8.1.1 测试要点

1. **完整业务流程测试**
  - AI 推荐分析功能正确性
  - 手工创建分析功能正确性
  - 与 AI 交互创建分析功能正确性
验证 生成流 SQL -> 发送引擎 -> 输入触发事件数据 -> 事件触发 -> TDasset 界面收到事件 -> 邮件收到事件的完整业务流程
1. **界面涵盖流计算功能测试**
  - 流触发类型
    - 滑动窗口
    - 会话窗口
    - 事件窗口
    - 计数窗口
  - 滑动时长
    - 毫秒
    - 秒
    - 分钟
    - 小时
  - 乱序数据重新计算选项
  - 计算
    - 开始时间 + 偏移
    - 结束时间 + 偏移
    - 输出时间戳 + 偏移
    - 输出属性  表达式 + 元素属性 + 事件属性

#### 8.1.2 测试用例

| **#** | **测试用例** | **测试描述** | **测试结果** |
| --- | --- | --- | --- |
|  | **test_idmp_meters.py** |  |  |
| 1 | stream1 | 智能电表:em-1的实时电压超过250伏持续超过10分钟时，常规告警，计算平均电压 (AI 推荐场景一) | 不通过，平均电压计算不正确。 [TD-36468](https://jira.taosdata.com:18080/browse/TD-36468) |
| 2 | stream1_sub1 | 只在窗口打开时触发计算 | 通过 |
| 3 | stream1_sub2 | 只在窗口关闭时触发计算 | 通过 |
| 4 | trigger_stream1 | 只写入一份数据，同时可触发三个流stream1 stream1_sub1 stream1_sub2 | 通过 |
| 5 | stream2 | 智能电表: em-2 每 5 分钟计算一次 1 小时内的最大功率，警告 (AI 推荐场景二) | 通过 |
| 6 | stream3 | 智能电表: em-3 的电流超过 100 安持续 5 分钟时，发出次要报警，计算平均电流 (AI 推荐场景三) | 不通过，平均电流计算不正确 |
| 7 | stream3_sub1 | 事件窗口 验证数据集 trows 与 where ts >=_twstart and ts <= _twstart 输出结果应相同 | 通过 |
| 8 | stream3_sub2 | 事件窗口 验证触发条件为复合条件（ and or ）触发行为的正确性 | 通过 |
| 9 | stream3_sub3 | 事件窗口 验证选项 PRE_FILTER 行为的正确性 | 不通过，[TD-37685](https://jira.taosdata.com:18080/browse/TD-37685) |
| 10 | stream3_sub4 | 事件窗口 验证基于超级表按子表分组触发行为的正确性 | 通过 |
| 11 | stream3_sub5 | 事件窗口 触发动作 验证只计算不通知正确性 | 通过 |
| 12 | stream3_sub6 | 事件窗口 触发动作 验证只通知不计算，在窗口打开和关闭时都通知 | 通过 |
| 13 | stream3_sub7 | 事件窗口 触发动作 验证只通知不计算，只在窗口打开时通知 | 通过 |
| 14 | stream3_sub8 | 事件窗口 触发动作 验证只通知不计算，只在窗口关闭时通知 | 通过 |
| 15 | stream4 | 智能电表: em-4 每隔 10 分钟实时计算 10 分钟内数据的平均电压和功率和 （手工创建分析场景一） | 通过 |
| 16 |  | 勾选对乱序数据重新计算选项，验证是否乱序数据在目标表中进行了重算 | 通过 |
| 17 | stream4_sub1 | 不勾选乱序数据重新计算，验证是否符合预期 | 不通过，不重启 TAOSD 符合预期，重启动 TAOSD 后当有数据写入时又会重算。[TD-36508](https://jira.taosdata.com:18080/browse/TD-36508) |
| 18 | stream4_sub2 | 触发相对时间毫秒输入：10a | 通过 |
| 19 | stream4_sub3 | 触发相对时间秒输入: 10s | 通过 |
| 20 | stream4_sub4 | 触发相对时间分输入: 10m | 通过 |
| 21 | stream4_sub5 | 触发相对时间小时输入: 10h | 通过 |
| 22 | stream4_sub6 | 触发相对时间毫秒输入: 10d | 通过 |
| 23 | stream4_sub7 | sliding > interval 场景 数据正确性 | 通过 |
| 24 | stream4_sub8 | sliding = interval 场景 数据正确性 | 不通过，[TD-36585](https://jira.taosdata.com:18080/browse/TD-36585) |
| 25 | stream4_sub9 | sliding < interval 场景 数据正确性 | 不通过，[TD-36575](https://jira.taosdata.com:18080/browse/TD-36575) |
| 26 | stream4_sub7+stream4_sub8+stream4_sub9 | 时间单位为毫秒、秒、分钟、小时、天的正确性 | 通过 |
| 27 | stream5 | 会话窗口 顺序写入分隔窗口正确性 | 通过 |
| 28 | stream5_sub1 | 会话窗口 有乱序写入后分隔窗口的正确性 | 不通过， [TD-36638](https://jira.taosdata.com:18080/browse/TD-36638) |
| 29 | stream5_sub2 | 会话窗口 带 PRE_FILTER 过滤后数据分隔窗口的正确性 | 通过 |
| 30 | stream5_sub3 | 会话窗口 stream5_sub2 换成 %%trows 数据集后生成数据结果应相同 | 不通过，[TD-37528](https://jira.taosdata.com:18080/browse/TD-37528) |
| 31 | stream5_sub4 | 会话窗口 超级表支持子表及TAG分组功能验证 | 通过 |
| 32 | stream5_sub5 | 会话窗口 超级表不分组情况下验证输出结果正确性 | 不通过，[TD-37704](https://jira.taosdata.com:18080/browse/TD-37704) , [TD-37698](https://jira.taosdata.com:18080/browse/TD-37698) |
| 33 | stream5_sub6 | 会话窗口 超级表不分组情况下验证流状态正确性 | 不通过，[TD-37749](https://jira.taosdata.com:18080/browse/TD-37749) |
| 34 | stream5_sub7 | 会话窗口 触发表为子表情况验证子表结果预期与超级表为触发表子表过滤后结果相同，超级表与子表相互验证结果。 | 不通过，[TD-37755](https://jira.taosdata.com:18080/browse/TD-37755) |
| 35 | stream6 | 计数窗口正序及默认值场景，加忽略乱序数据，预期忽略 | 通过 |
| 36 | stream6_sub1 | 计数窗口增加乱序重计算选项，不加忽略乱序数据，预期此选项无效，忽略 | 不通过，仍计算过期数据 [TD-36670](https://jira.taosdata.com:18080/browse/TD-36670) |
| 37 | stream6_sub2 | 计数窗口 以某一含 NULL 值普通列计数，验证计数划分窗口正确性 | 通过 |
| 38 | stream6_sub3 | 计数窗口 以某两列含 NULL值 普通列计数，验证计数划分窗口正确性 | 通过 |
| 39 | stream6_sub4 | 计数窗口 以超级表为触发表，验证计数划分窗口正确性 | 通过 |
| 40 | stream6_sub5 | 计数窗口 计算通过触发表而非 trows ，验证 wstart wend 正确输出 | 通过 |
| 41 | trigger_stream6_again | 验证计数窗口预期会忽略乱序、更新、删除数据 | 通过 |
| 42 |  |  |  |
| 43 | stream7 | 状态窗口正序及默认值场景测试，同时加入 NULL 值， 预期 NULL 值忽略 | 不通过，NULL 值会触发状态改变 [TD-36708](https://jira.taosdata.com:18080/browse/TD-36708) |
| 44 | stream7_sub1 | 状态窗口 不忽略过期数据测试 | 通过 |
| 45 | stream7_sub2 | 状态窗口 PRE_FILTER 过滤功能正确性 | 通过 |
| 46 | stream7_sub3 | 状态窗口 PRE_FILTER 过滤功能多个条件正确性 | 通过 |
| 47 | stream7_sub4 | 状态窗口 DELETE_RECALC 选项的正确性 | 不通过，[TD-37633](https://jira.taosdata.com:18080/browse/TD-37633) |
| 48 | stream7_sub5 | 状态窗口 PRE_FILTER + DELETE_RECALC 选项的正确性 | 不通过，[TD-37633](https://jira.taosdata.com:18080/browse/TD-37633) |
| 49 | stream8 | 定时触发窗口默认选项测试 | 不通过，[TD-36728](https://jira.taosdata.com:18080/browse/TD-36728) |
| 50 | stream8_1 | 定时任务中 IGNORE_NODATA_GRIGGER 是否生效 | 不通过，[TD-37422](https://jira.taosdata.com:18080/browse/TD-37422) |
| 51 | stream8_2 | 定时任务中占位符 tprev_localtime t_next_localtime 的正确性 | 通过 |
| 52 | stream8_3 | 定时任务中不需要触发表的触发及使用正确性验证 | 通过 |
| 53 | stream8_4 | 定时任务验证选项 FORCE_OUTPUT 预期结果 | 通过 |
| 54 | stream8_5 | 定时任务验证非 FORCE_OUTPUT 预期结果 | 通过 |
| 55 | stream9 | 滑动触发最小为 1 毫秒测试，预期生成的数据与触发表中的数据相同 | 通过 |
| 56 | stream10 | 滑动触发不加流选项间隔一个空周期写入数据测试，预期空周期在结果表中生成一条记录 | 通过 |
| 57 |  | 在结果表中输出 _tprev_ts , _tnext_ts 占位符，并验证其值的正确性 | 通过 |
| 58 | stream10_sub1 | 滑动触发 增加 IGNORE_NODATA_TRIGGER 选项，验证空周期不输出结果 | 通过 |
| 59 | stream10_sub2 | 滑动触发 支持虚拟超级表及支持多个分组列功能的正确性 | 通过 |
| 60 | stream10_sub3 | 滑动触发 支持写入数据过滤触发 | 不通过，[TD-37465](https://jira.taosdata.com:18080/browse/TD-37465) |
| 61 | stream10_sub4 | 滑动窗口触发 使用 PRE_FILTER 验证无效数据不应参与窗口关闭过程 | 不通过，[TD-37504](https://jira.taosdata.com:18080/browse/TD-37504) |
| 62 | stream10_sub5 | 滑动窗口触发 stream_sub4 对比数据，与 sub4 仅 PRE_FILTER 选项没有，其它一样，验证不使用过滤选项正常生成流数据的正确性 | 不通过，与 sub4 创建时相互影响 [TD-37505](https://jira.taosdata.com:18080/browse/TD-37505) |
| 63 | stream10_sub6 | 滑动触发 验证 offset 设置后生成数据的正确性 | 通过 |
| 64 | stream10_sub7 | 滑动窗口触发 从虚拟超级表创建的流与相同的超级表创建流输出结果相同 | 通过 |
| 65 | stream10_sub8 | 滑动窗口触发 使用 DELETE_RECALC 选项删除一个子表相应结果表中也会删除此子表的计算结果 | 不通过，[TD-37639](https://jira.taosdata.com:18080/browse/TD-37639) |
| 66 | stream11 | 滑动窗口触发 验证占位符 _twstart,_twend,_twduration, _twrownum 的正确性 | 不通过，[TD-37528](https://jira.taosdata.com:18080/browse/TD-37528?src=confmacro) |
| 67 | stream11_sub1 | 滑动窗口触发 INTERVAL 的 OFFSET 设置后输出数据的正确性 | 通过 |
| 68 | stream11_sub2 | 滑动窗口触发 INTERVAL 的 OFFSET 设置为与窗口同宽下数据的正确性 | 通过 |
| 69 | stream11_sub3 | 滑动窗口触发 INTERVAL 与 SLIDING 相同时再设置 OFFSET 的数据正确性 | 通过 |
| 70 | stream11_sub4 | 滑动窗口触发 触发表为超级表以 tbname 分表，不指定 OUTPUT_TABLE，生成表在本数据为内 | 通过 |
| 71 | stream11_sub5 | 滑动窗口触发 触发表为超级表以 tbname 分表，指定 OUTPUT_TABLE，生成表在非本数据库内 | 通过 |
| 72 | stream11_sub6 | 滑动窗口触发 触发表为超级表以 tbname 分表，指定输出表的列名，生成在本非数据库内 | 通过 |
| 73 | **test_period_long.py** | 定时任务中对 interval 小于 1 天及 大于 1 天的触发时间的正确性验证 | 不通过，[TD-37467](https://jira.taosdata.com:18080/browse/TD-37467) |
|  |  |  |  |

#### 8.1.3 测试结论

编写 CASE 23 个，共发现问题 11 个， 测试重点为数据正确性，很多都是基础功能问题，问题较多，结论为不易对外发布。

### 8.2 卷烟场景（杨志宇）

#### 8.2.1 测试要点

1. 测试 AI 推荐生成的分析，创建 Stream 及其计算结果的正确性
2. 测试手动创建分析，创建 Stream 及其计算结果的正确性
   - 触发类型：
      - 定时窗口：指定不同的窗口大小、窗口偏移
      - 状态窗口：指定状态的字段
      - 会话窗口：指定会话的时间间隔
   - 时间窗口聚合：
      - 窗口开始时间：_tprev_localtime/ _twstart/ _tprev_ts
      - 窗口结束时间：_tlocaltime/ _twend/ _tcurrent_ts
   - 输出属性：
      - AVG：平均值
      - LAST：最新值
      - SUM：求和
      - MAX：最大值
      - STDDEV：标准差
      - SPREAD：极差
      - SPREAD/FIRST：变化率

#### 8.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | AI 推荐：系统中振动输送机的振动幅度每5分钟计算一次过去半小时的平均值，常规告警 | 通过 |
| 2 | AI 推荐：振动输送机的电机信号,全部设备超过10分钟没有上报数据则发出严重报警,取出最后一条的电机信号 | 通过 |
| 3 | AI 推荐：振动输送机每15分钟计算一次过去1小时的电机信号最大值，次要告警 | 通过 |
| 4 | AI 推荐：振动输送机每10分钟检查一次工序的振动幅度总和，常规告警 | 通过 |
| 5 | AI 推荐：振动输送机超过5分钟没有数据上报时，记录最后一条电机信号，主要告警 | 通过 |
| 6 | AI 推荐：振动输送机每20分钟计算一次过去40分钟的振动幅度标准差，警告告警 | 通过 |
| 7 | AI 推荐：振动输送机每30分钟计算一次过去1小时的电机信号极差，次要告警 | 通过 |
| 8 | AI 推荐：振动输送机全部设备超过15分钟没有上报电机信号数据，严重告警 | 通过 |
| 9 | AI 推荐：振动输送机每5分钟计算一次过去15分钟的振动幅度变化率，常规告警 | 通过 |
| 10 | 手动分析：电子皮带秤，定时窗口，每秒计算之前所有数据的平均测量值 | 通过 |
| 11 | 手动分析：电子皮带秤，定时窗口，每1小时计算_tprev_localtime 到 _tlocaltime 的平均测量值 | 通过 |
| 12 | 手动分析：计量管，状态窗口，每个窗口计算 last 值 | 通过 |

#### 8.2.3 测试结论

所有用例均已通过，已加入 CI。
所有关联 JIRA 请查看：https://jira.taosdata.com:18080/issues/?filter=24846
剩一个性能相关的问题，待排查：
TD-36699

### 8.3 光伏场景（杨志宇）

#### 8.3.1 测试要点

1. 测试 AI 推荐生成的分析，创建 Stream，验证流的正确性
2. 测试不同的触发类型
   - 滑动窗口：每 n 分钟计算一次 m 小时内的聚合
   - 事件窗口：field 从 start_condition 开始，到 stop_condition 结束
   - 会话窗口：超过 n 分钟没有上报数据
   - 计数窗口：连续 n 次采集数据
3. 不同类型的聚合函数：
   - AVG：平均值
   - LAST：最新值
   - SUM：求和
   - MAX：最大值

#### 8.3.2 测试用例

| **#** | **测试用例** | **测试描述** | **测试结果** |
| --- | --- | --- | --- |
| 1 | AI 推荐：气象传感器01每5分钟计算一次1小时内的最大辐照 | 通过 |
| 2 | AI 推荐：气象传感器02的实时环境温度超过50度持续超过10分钟时，计算平均环境温度 | 通过 |
| 3 | AI 推荐：气象传感器03超过10分钟没有上报数据，则发出主要报警，取出最后一条的环境温度 | 通过 |
| 4 | AI 推荐：气象传感器03接收数据3次输出平均辐照 | 通过 |
| 5 | AI 推荐：光伏逆变器0101直流电量超过500kW持续超过10分钟时计算平均直流电量 | 通过 |
| 6 | AI 推荐：光伏逆变器0102累计发电量达到1MWh到2MWh时记录当前日发电量 | 通过 |
| 7 | AI 推荐：光伏逆变器0103直流电量连续3次采集输出当前值 | 通过 |
| 8 | AI 推荐：逆变器的光伏逆变器全部设备超过20分钟没有上报数据，则发出严重报警，取出最后一条的交流电量 | 通过 |
| 9 | AI 推荐：逆变器的光伏逆变器全部设备超过20分钟没有上报数据，则发出严重报警，取出最后一条的交流电量 | 通过 |

#### 8.3.3 测试结论

所有用例均已通过，已加入 CI。
产生的所有 JIRA 问题，请查看：https://jira.taosdata.com:18080/issues/?filter=24846

### 8.4 车辆场景（段宽军 完成）

#### 8.4.1 测试要点

1. 数据正确性

#### 8.4.2 测试用例

| **#** | **测试用例** | **测试描述** | **测试结果** |
| --- | --- | --- | --- |
|  | **test_idmp_vehicle.py** | 车辆场景 CASE |  |
| 1 | stream1 | 事件窗口-> NULL值写入顺序场景测试，预期： 1. NULL 值表示未收到新值，保持前值状态，会计算在事件窗口中 1. 中间插入 NULL 值不影响 TRUE_FOR 的条件判断 NULL 值写入在改乱序场景，预期： 1. 与顺序场景相同 1. NULL 值更新为有实际值，满足条件时预期触发事件 1. 实际值更新为 NULL 值， 触发为不满足条件时，原来的窗口删除 | 通过 |
| 2 | stream2 | 事件窗口 -> 乱序及更新 -> 原来满足条件窗口变为不满足，预期 1. 当设置 IGNORE_DISORDER ，增删改对已生成窗口无影响 1. 当不设置 IGNORE_DISORDER ， 增删改对已生成窗口有影响，若触发条件不再不满足，撤消原来窗口，若还满足，重新计算窗口数据 | [TD-36852](https://jira.taosdata.com:18080/browse/TD-36852) |
| 3 | stream3 | 事件窗口 -> 乱序及更新 -> 原来不满足条件窗口变为满足，预期 1. 当设置 IGNORE_DISORDER ，乱序及更新改对已生成窗口无影响 2. 当不设置 IGNORE_DISORDER ， 乱序及更新对已生成窗口有影响，若触发条件仍不满足，不触发，若变为满足，触发事件并启动计算，输出新触发窗口 | [TD-36936](https://jira.taosdata.com:18080/browse/TD-36936) |
| 4 | stream4 | 事件窗口->数据删除，数据删除是否触发重算由 DELETE_RECALC 决定，默认为不重算（在顺序写入场景下），预期： 1. 创建流不带 DELETE_RECALC , 顺序写入，预期不重算 1. 创建流不带 DELETE_RECALC , 顺序+乱序写入，乱序部分会重算 1. 创建流带 DELETE_RECALC , 不管顺序乱写都会重算 删除对触发影响场景： 1. 一个触发窗口数据全部删除 1. 一个触发窗口数据删除部分，达到不触发条件 1. 一个触发窗口数据删除部分，但仍能达到触发条件 | [TD-36908](https://jira.taosdata.com:18080/browse/TD-36908) |
| 5 | stream5 stream6 stream7 | 滑动窗口-> IGNORE_NODATA_TRIGGER 选项生成数据的正确性： 创建两个相同流，一个设置此选项，一个不设置，对比两流结果 两流都开启乱序重算功能 一、写入顺序 1. 顺序写入，中间留一段空白, 验证选项 1. 乱序写入，中间留一段空白，验证选项 1. 在顺序写入空白处写入一条数据，验证选项 1. 在乱序写入空白处写入一条数据，验证选项 二、窗口宽度与步长 1. 窗口宽度 = 步长场景，顺序写入中间留一段空白 1. 窗口宽度 > 步长场景，顺序写入中间留一段空白 1. 窗口宽度 < 步长场景，顺序写入中间留一段空白 1. 窗口宽度 = 步长场景，乱序写入中间留一段空白 1. 窗口宽度 > 步长场景，乱序写入中间留一段空白 1. 窗口宽度 < 步长场景，乱序写入中间留一段空白 三、空白与数据位置关系 1. 数据-> 空白 -> 数据 顺序写入 1. 空白 -> 数据-> 空白 顺序写入 1. 数据-> 空白 -> 数据 乱序写入 1. 空白 -> 数据-> 空白 乱序写入 1. 复杂模式 数据（乱） -> 空白 -> 数据（顺） -> 空白 ->数据（乱） | 通过 |
| 6 | stream8 | Watermark 机制窗口关闭时间正确计算 | [TD-37238](https://jira.taosdata.com:18080/browse/TD-37238) |
| 7 | stream9 | Watermark 机制与乱序数据重算选项组合验证 1. 在 watermark 范围内数据不应被标为乱序数据 1. 不启用乱序数据重算功能，在 watermark 范围内数据应被正确计算 | [TD-37196](https://jira.taosdata.com:18080/browse/TD-37196) |
| 8 | stream10 | 数据过期机制基本功能验证： 1. 过期数据的标识准确性验证 1. 写入过期数据在乱序重算及非乱序重算场景下都不应再计算 | [TD-37250](https://jira.taosdata.com:18080/browse/TD-37250) |
| 9 | stream_stb1 | 虚拟超级表上创建流，验证虚拟超级表通过 parition by 进行分割存储流数据的正确性。 1. 计算实时数据场景 1. 计算历史数据场景 | 通过 |
|  |  |  |  |

#### 8.4.3 测试结论

产生的所有 JIRA 问题，请查看：https://jira.taosdata.com:18080/issues/?filter=24848

### 8.5 三峡新能源场景（吕泽-完成）

#### 8.5.1 测试要点

针对三峡客户使用的旧版本流计算，进行改写为新版本流计算，因为部分流用法类似，挑选了有代表性的进行改写。
主要是以下流：
[第一步流计算20250307](https://taosdata.feishu.cn/wiki/ATWQwcOAviZfWikU69WcNAbTndc)
[第二步流计算20250310](https://taosdata.feishu.cn/wiki/R014wmTQyi1Omck6NZwcUT0Cn7c)
[第三步流计算20250314](https://taosdata.feishu.cn/wiki/XaqbweV96iZVRnkgHLJcx2ZCnQf)

#### 8.5.2 测试用例

| **#** | **测试用例** | **测试描述** | **测试结果** | **备注** |
| --- | --- | --- | --- | --- |
| 1 | ~~test_three_gorges_case1.py（暂不支持 state window里 cast）~~ |  |
| 2 | ~~test_three_gorges_case2.py（暂不支持 state window里 cast）~~ |  |
| 3 | ~~test_three_gorges_case3.py（暂不支持 state window里 cast）~~ |  |
| 4 | test_three_gorges_case4.py | 通过 |  |
| 5 | test_three_gorges_case4_bug1.py | 通过 |  |
| 6 | test_three_gorges_case5.py | 通过 |  |
| 7 | test_three_gorges_second_case1_bug1.py | 通过 |  |
| 8 | test_three_gorges_second_case1_twostream.py | 通过 |  |
| 9 | test_three_gorges_second_case3.py | 通过 |  |
| 10 | test_three_gorges_second_case4.py | 通过 |  |
| 11 | test_three_gorges_second_case6.py | 通过 |  |
| 12 | ~~test_three_gorges_second_case12.py（暂不支持 state window里 cast）~~ |  | 暂不支持 |
| 13 | test_three_gorges_second_case17.py | 通过 |  |
| 14 | test_three_gorges_second_case18.py | 通过 |  |
| 15 | test_three_gorges_second_case19.py | 通过 |  |
| 16 | test_three_gorges_second_case19_bug1.py | 通过 |  |
| 17 | test_three_gorges_second_case22.py | 通过 |  |
| 18 | ~~test_three_gorges_second_case26.py（暂不支持 state window里 cast）~~ |  | 暂不支持 |

#### 8.5.3 测试结论

1. 新版本流计算不支持 interval+fill(prev)的用法，目前不支持填充非 NULL 的类型，只能 force_output 一个 NULL
2. _wstart在计算语句里使用的时候必须要和窗口一起使用
3. 目前不支持触发条件里面使用state_window(cast(val as int))这种，后期可能支持，三峡里面有的用例就是这种，暂时用不了
4. 结果表名称中不能含点'.' 
5. BUG:大部分已经修复
  1. 
    TD-36985

  1. 
    TD-36945

  1. 
    TD-36915

  1. 
    TD-36871

   - ，后面又暂时不支持了
    TD-36866

  1. 
    TD-36846

  1. 
    TD-36817

  1. 
    TD-36787


### 8.6 Navados 场景（李珲-完成）

#### 8.6.1 测试要点

1. 从客户的云服务后台，获取流计算模拟需要的元数据信息：建流语句 和 对应的超级表 Schema。
```sql {wrap}
taos> select * from ins_streams \G;
*************************** 1.row ***************************
        stream_name: windspeeds_hourly
        create_time: 2025-05-09 12:46:37.843
          stream_id: 0x4ca4943aa57f351e
         history_id: NULL
                sql: create stream windspeeds_hourly fill_history 1 into windspeeds_hourly as select _wend as window_hourly, site, id, max(speed) as windspeed_hourly_maximum from windspeeds where _ts >= '2025-05-07' partition by site, id interval(1h);
             status: ready
          source_db: dev
          target_db: dev
       target_table: windspeeds_hourly
          watermark: 0
            trigger: window close
         sink_quota: 0
checkpoint_interval: 600 sec
  checkpoint_backup: none
  history_scan_idle: 100a
            message:  
*************************** 2.row ***************************
        stream_name: kpi_db_test
        create_time: 2025-05-09 12:47:24.202
          stream_id: 0x4ca1d0d902109246
         history_id: NULL
                sql: create stream if not exists kpi_db_test trigger window_close watermark 10m fill_history 1 ignore update 1 into kpi_db_test as select _wend as window_end, case when last(_ts) is not null then 1 else 0 end as db_online from trackers where _ts >= '2024-10-04T00:00:00.000Z' interval(1h) sliding(1h);
             status: ready
          source_db: dev
          target_db: dev
       target_table: kpi_db_test
          watermark: 600000
            trigger: window close
         sink_quota: 0
checkpoint_interval: 600 sec
  checkpoint_backup: none
  history_scan_idle: 100a
            message:  
*************************** 3.row ***************************
        stream_name: windspeeds_daily
        create_time: 2025-05-09 12:52:15.191
          stream_id: 0x4c9078b4d6cb15b5
         history_id: NULL
                sql: create stream windspeeds_daily fill_history 1 into windspeeds_daily as select _wend as window_daily, site, id, max(windspeed_hourly_maximum) as windspeed_daily_maximum from windspeeds_hourly partition by site, id interval(1d, 5h);
             status: ready
          source_db: dev
          target_db: dev
       target_table: windspeeds_daily
          watermark: 0
            trigger: window close
         sink_quota: 0
checkpoint_interval: 600 sec
  checkpoint_backup: none
  history_scan_idle: 100a
            message:  
*************************** 4.row ***************************
        stream_name: kpi_trackers_test
        create_time: 2025-05-09 12:53:06.032
          stream_id: 0x4c8d70f2308804c2
         history_id: NULL
                sql: create stream if not exists kpi_trackers_test trigger window_close watermark 10m fill_history 1 ignore update 1 into kpi_trackers_test as select _wend as window_end, site, zone, tracker, case when ((min(abs(reg_pitch - reg_move_pitch)) <= 2) or (min(reg_temp_therm2) < -10) or (max(reg_temp_therm2) > 60) or (last(reg_system_status14) = true)) then 1 else 0 end as tracker_on_target, case when last(reg_pitch) is not null then 1 else 0 end as tracker_online from trackers where _ts >= '2024-10-04T00:00:00.000Z' partition by tbname interval(1h) sliding(1h);
             status: ready
          source_db: dev
          target_db: dev
       target_table: kpi_trackers_test
          watermark: 600000
            trigger: window close
         sink_quota: 0
checkpoint_interval: 600 sec
  checkpoint_backup: none
  history_scan_idle: 100a
            message:  
*************************** 5.row ***************************
        stream_name: off_target_trackers
        create_time: 2025-05-09 12:44:30.938
          stream_id: 0x4cac24a68dd426a1
         history_id: NULL
                sql: create stream off_target_trackers ignore expired 0 ignore update 0 into off_target_trackers as select _wend as _ts, site, tracker, last(reg_pitch) as off_target_pitch, last(mode) as mode from trackers where _ts >= '2024-04-23' and _ts < now() + 1h and abs(reg_pitch-reg_move_pitch) > 2 partition by site, tracker interval(15m) sliding(5m);
             status: ready
          source_db: dev
          target_db: dev
       target_table: off_target_trackers
          watermark: 0
            trigger: window close
         sink_quota: 0
checkpoint_interval: 600 sec
  checkpoint_backup: none
  history_scan_idle: 100a
            message:  
*************************** 6.row ***************************
        stream_name: snowdepths_daily
        create_time: 2025-05-09 12:44:32.669
          stream_id: 0x4cac0a3c9f116d1e
         history_id: NULL
                sql:  create stream snowdepths_daily fill_history 1 into snowdepths_daily as select _wend as window_daily, site, id, max(snowdepth_hourly_maximum) as snowdepth_daily_maximum from snowdepths_hourly partition by site, id interval(1d, 5h);
             status: ready
          source_db: dev
          target_db: dev
       target_table: snowdepths_daily
          watermark: 0
            trigger: window close
         sink_quota: 0
checkpoint_interval: 600 sec
  checkpoint_backup: none
  history_scan_idle: 100a
            message:  
*************************** 7.row ***************************
        stream_name: kpi_zones_test
        create_time: 2025-05-09 12:45:28.581
          stream_id: 0x4ca8b516393a6504
         history_id: NULL
                sql: create stream if not exists kpi_zones_test trigger window_close watermark 10m fill_history 1 ignore update 1 into kpi_zones_test as select _wend as window_end, site, zone, case when last(_ts) is not null then 1 else 0 end as zone_online from trackers where _ts >= '2024-10-04T10:00:00.000Z' partition by site, zone interval(1h) sliding(1h);
             status: ready
          source_db: dev
          target_db: dev
       target_table: kpi_zones_test
          watermark: 600000
            trigger: window close
         sink_quota: 0
checkpoint_interval: 600 sec
  checkpoint_backup: none
  history_scan_idle: 100a
            message:  
*************************** 8.row ***************************
        stream_name: kpi_sites_test
        create_time: 2025-05-09 12:46:08.728
          stream_id: 0x4ca6507c6a9d4f0d
         history_id: NULL
                sql:  create stream if not exists kpi_sites_test trigger window_close watermark 10m fill_history 1 ignore update 1 into  kpi_sites_test as select _wend as window_end, site, case when last(_ts) is not null then 1 else 0 end as site_online from  trackers where _ts >= '2024-10-04T00:00:00.000Z' partition by site interval(1h) sliding(1h);
             status: ready
          source_db: dev
          target_db: dev
       target_table: kpi_sites_test
          watermark: 600000
            trigger: window close
         sink_quota: 0
checkpoint_interval: 600 sec
  checkpoint_backup: none
  history_scan_idle: 100a
            message:  
*************************** 9.row ***************************
        stream_name: trackers_motor_current_state_window
        create_time: 2025-05-09 12:46:09.328
          stream_id: 0x4ca64755594b63f8
         history_id: NULL
                sql: create stream trackers_motor_current_state_window into  trackers_motor_current_state_window as select _ts, site, tracker, max(`reg_motor_last_move_peak_mA` / 1000) as max_motor_current from  trackers where _ts >= '2024-09-22' and _ts < now() + 1h and `reg_motor_last_move_peak_mA` > 0 partition by tbname/*, site, tracker */ state_window(cast(reg_motor_last_move_count as int));
             status: ready
          source_db: dev
          target_db: dev
       target_table: trackers_motor_current_state_window
          watermark: 0
            trigger: window close
         sink_quota: 0
checkpoint_interval: 600 sec
  checkpoint_backup: none
  history_scan_idle: 100a
            message:  
*************************** 10.row ***************************
        stream_name: snowdepths_hourly
        create_time: 2025-05-09 12:46:14.234
          stream_id: 0x4ca5fc7ae4656c70
         history_id: NULL
                sql: create stream snowdepths_hourly fill_history 1 into  snowdepths_hourly as select _wend as window_hourly, site, id, max(depth) as snowdepth_hourly_maximum from  snowdepths where _ts >= '2024-01-01' partition by site, id interval(1h);
             status: ready
          source_db: dev
          target_db: dev
       target_table: snowdepths_hourly
          watermark: 0
            trigger: window close
         sink_quota: 0
checkpoint_interval: 600 sec
  checkpoint_backup: none
  history_scan_idle: 100a
            message:  
Query OK, 10 row(s) in set (0.003351s)
```

1. 针对客户的建流语句，按照新的流规则进行改写；
2. 针对流中的条件，构造合适的数据进行写入；

#### 8.6.2 测试用例

| **#** | **测试用例** | **测试描述** | **测试结果** | **备注** |
| --- | --- | --- | --- | --- |
| 1 | test_nevados.py::windspeeds_hourly | 流 windspeeds_hourly | 通过 |
| 2 | test_nevados.py::windspeeds_daily | 流 windspeeds_daily | 通过 |
| 3 | test_nevados.py::kpi_db_test | 流 kpi_db_test | 通过 |
| 4 | test_nevados.py::kpi_trackers_test | 流 kpi_trackers_test | 通过 |
| 5 | test_nevados.py::off_target_trackers | 流 off_target_trackers | 通过 |
| 6 | test_nevados.py::kpi_zones_test | 流 kpi_zones_test | 通过 |
| 7 | test_nevados.py::kpi_sites_test | 流 kpi_sites_test | 通过 |
| 8 | test_nevados.py::trackers_motor_current_state_window | 流 trackers_motor_current_state_window | 否 |

#### 8.6.3 测试结论

遗留问题：
1. [TD-36979](https://jira.taosdata.com:18080/browse/TD-36979) [流计算开发阶段] nevados用户流计算场景模拟-1条sql中700条记录，流计算出结果非常慢
2. [TD-36976](https://jira.taosdata.com:18080/browse/TD-36976) [流计算开发阶段] nevados用户流计算场景模拟-state窗口触发与预期不符
3. [TD-37013](https://jira.taosdata.com:18080/browse/TD-37013) [流计算开发阶段] nevados用户流计算场景模拟-interval窗口+pre_filter出现非预期的结果

### 8.7 运维场景（段宽军-进行中）

#### 8.7.1 测试要点

流计算在数据库管理员日常运维场景下的表现，运维场景包括:
1. 建改删库
2. 建改删表
3. 重启服务
4. 备份/还原
5. COMPACT 操作等。

#### 8.7.2 测试用例

| **#** | **测试用例** | **测试描述** | **测试结果** |
| --- | --- | --- | --- |
|  | **test_idmp_manager.py** | 数据库管理相关操作 |  |
| 1 | stream1 | 超级表为触发表 子表在建流前创建场景 | 通过 |
| 2 | stream1_sub1 | 超级表为触发表 建流完后再创建子表，预期新建子表加入流并按分组输出数据 | 通过 |
| 3 | stream1_sub2 | 虚拟超级表为触发表 建流完成后再创建虚拟子表，预期新建虚拟子表不会加入流数据输出 | 通过 |
| 4 | stream1_sub3 | 用户删除触发表，预期为忽略，程序不崩溃，输出表中内容不变 | 通过 |
| 5 | alter_table | 1. 流计算期间对触发表进行加列、删列，改变列长度的修改，预期不崩溃，流正常运行 1. 修改被流使用的 TAG 列的值，预期为不能修改 1. 修改被流使用的 TAG 列的值，预期为可以修改 1. 删除输出表，预期为程序不崩溃，有新数据写入会自动重建 | 不通过， [TD-37796](https://jira.taosdata.com:18080/browse/TD-37796) [TD-37797](https://jira.taosdata.com:18080/browse/TD-37797) |
| 6 | stream1_sub4 | 触发表使用分组，计算查询中同时也使用了分组，即又重分组，预期是先触发表分组完再在组中进行查询分组，同时程序不崩溃。 | 通过 |
| 7 | stream3 | 输出表为普通表时，删除流计算输出表，预期再次写入数据会自动生成输出表 | 通过 |
| 8 | stream4 | 流计算嵌套，在流计算结果表上再建流，预期是可以支持，功能正常 | 通过 |
| 9 | verify_config | 检查流计算新增配置项： 1. 流的 5 个可配置线程数选项，预期实际线程数 >= 配置线程数。 | 不通过，[TD-37846](https://jira.taosdata.com:18080/browse/TD-37846) |
| 10 | stream5 | 事件窗口验证带选项 FILL_HISTORY 的流处理历史数据与实时数据混合的正确性 | 不通过，[TD-37867](https://jira.taosdata.com:18080/browse/TD-37867) |
| 11 | stream5_sub1 | 事件窗口验证 通知条件过滤 WHERE 过滤通知的正确性 | 不通过，[TD-37878](https://jira.taosdata.com:18080/browse/TD-37878) |
| 12 | stream6 | 事件窗口验证选项 FILL_HISTORY 计算历史数据的正确性 | 不通过，[TD-37868](https://jira.taosdata.com:18080/browse/TD-37868) |
| 13 | verify_stream_status | 从系统表及 show streams 分别验证查看流状态等信息正确性 | 通过 |
| 14 | stream7 | 验证手工重算与原始计算结果相同： 滑动窗口带 DELETE_RECALC 选项，预期手工重算与自动算结果应一致： 1. 设计 5 个窗口数据，1 和 4 为顺序窗口， 2 为乱序窗口， 3 为被删除窗口，5 为手动重算后实时写入窗口 1. 创建完流后顺序写入 1、3、4 窗口 1. 写入乱序窗口 2，删除窗口 3 数据 1. 等待流计算完成，验证流计算结果 1. 删除 stream7 的计算结果表 result_stream7 1. 手工启动流重算，开始时间为窗口 1 开始时间，结束时间不填写 1. 等待流计算完成，验证重算结果应与原始计算结果完全相同 | 不通过，[TD-37917](https://jira.taosdata.com:18080/browse/TD-37917) |
| 15 | stream7_sub1 | 验证手工重算与原始计算结果不同： 1. 创建忽略乱序数据及删除数据的滑动窗口流 stream7_sub1 1. 数据写入同 stream7 1. 预期手工重算结果与原始计算结果不同，乱序及删除数据在输出结果中应有体现。 此用例应用场景：有严重乱序数据或删除操作，为了不影响流性能先忽略这两种数据，在每天12点服务器空闲时再重算校正结果。 | 不通过，[TD-37918](https://jira.taosdata.com:18080/browse/TD-37918) |
| 16 | stream7_sub4 | 定时窗口不允许进行重算，预期为命令执行失败 | 通过 |
| 17 | stream8 | 验证流开始/停止操作后状态正确性及查询状态是否为实际流真实状态 | 通过 |
| 18 | verify_stream8_again | 验证流删除操作及从系统表中检查流是否真正删除 | 通过 |
|  | **test_idmp_privilege.py** | **流用户及权限管理相关内容** |  |
| 19 | stream1 | 创建非超级管理员权限普通用户，赋予流所属库写权限及输出表所属库写权限，预期可以建流。 | 不通过， [TD-37954](https://jira.taosdata.com:18080/browse/TD-37954) |

#### 8.7.3 测试结论

## 9. 历史用例迁移（进行中）

从现有的`200`个`SIM`和`Python`用例中，筛选出不重复的内容，将其整合、优化为约`30`个精简、高效的用例。

## 10. 长稳测试（郭向阳）

1. 扩大第 8 章用户场景的数据集，进行至少 24 小时的长稳测试
2. 编写一个典型的测试场景，涵盖数据更新、数据删除、数据修改、过期数据、多副本切换、表结构修改、子表创建与删除等的混合情况

## 11. 性能测试（郭向阳）

使用同一台测试服务器，使得不同时间的测试结果都具备可比性。在测试过程中，要记录如下性能参数，性能指标采样频率为 1 秒。
1. 性能参数
   - taosd  CPU 负载
   - taosd 内存开销
   - 系统总体负载
   - 读取开销（次数、数据量）
   - 写入开销（次数、数据量）
   - 计算完成时间（历史数据计算场景）
2. 测试用例
   - 存放路径：13-StreamProcessing/22-Performance
   - 命名方式：由于不加入到 CI，不以 test_ 作为前缀
3. 部署说明
   - 可在同一台机器上测试，但考虑部署多个 dnode
   - 查询数据集、目标数据集、Snode 尽可能在不同 dnode 上，便于监控资源
   - 如果涉及触发数据集，触发数据集也尽可能在单独 dnode 上
   - 本次测试先局限在单副本场景
4. 测试报告
   - 测试报告编写单独文档，例如“历史数据流计算-性能测试”，每个测试报告都需要描述使用的建流语句
   - 测试结束后，绘制曲线图，以表格方式对比平均值和中位数，
   - 观察性能对比结果，发现异常情况
   - 出现异常情况时，交给研发分析，例如查看火焰图、分析是否存在明显的性能瓶颈、寻找可优化的性能关键点。

### 11.1 历史数据流计算

#### 11.1.1 数据集

1. 数据列：五列，分别为 timestamp、int、bigint、float、double
2. 标签列：两列，分别为 int、varchar（16）
3. 数据规模：10000 张子表，每个子表 10 万条记录，数据频率 1 秒，VGroups=4
4. 写入方式：interlace 方式写入
5. 触发数据集：和用于查询的数据集相同

#### 11.1.2 测试场景

按照 60 秒时间间隔，以 tbname 作为单一维度，计算各数据列的平均值、最大值、最小值，写入到结果表
1. 查询写入
   - 手写 python 程序，单线程循环执行计算
   - 单次计算：以 paritition by tbname 方式查询 300 秒内的聚合结果，拼批写入到结果表中
   - 循环“单次计算”，直到所有数据处理完毕
2. 旧版：AT_ONCE 
3. 旧版：WINDOW_CLOSE
4. 旧版：CONTINUOUS_WINDOW_CLOSE
5. 新版：INTERVAL(60s) SLIDING（60s） 触发
6. 新版：COUNT_WINDOW(60) 触发
其他
1. 考虑在写入完成后备份数据库文件，以便每次测试复用
2. 场景 1-4 的测试结果可提前保存好
3. 需对比测试结果，验证计算的准确性

#### 11.1.3 测试报告

草稿见 [历史数据流计算](https://taosdata.feishu.cn/wiki/NGy0wTygmihrOckXcwEclYevnQl)

### 11.2 实时数据流计算

#### 11.2.1 数据集

1. 数据列：五列，分别为 timestamp、int、bigint、float、double
2. 标签列：两列，分别为 int、varchar（32）
3. 数据规模：1000 张子表，每个子表 10800 条记录，数据频率 50 毫秒（运行时间约 9 分钟）
4. 写入方式：taosBenchmark interlace 方式写入，每轮写入完成后，taosBenchmark 等待
5. 触发数据集：对于场景 12 及之后，额外准备相同数量的子表（1000），精心设置触发条件

#### 11.2.2 测试场景

按照 15 秒时间间隔，以 tbname 作为单一维度，计算平均值、最大值、最小值，写入到结果表
1. 查询写入
   - 手写 python 程序，单线程循环执行计算
   - 单次计算：以 paritition by tbname 方式查询 15 秒内的聚合结果，拼批写入到结果表中
   - 每隔 15 秒执行“单次计算”，直到所有数据处理完毕
2. 旧版：AT_ONCE
3. 旧版：WINDOW_CLOSE
4. 旧版：MAX_DELAY 5 秒
5. 旧版：FORCE_WINDOW_CLOSE
6. 旧版：CONTINUOUS_WINDOW_CLOSE
7. 新版：SLIDING 触发 from %%tbname
8. 新版：SLIDING 触发 from %trows
9. 新版：SLIDING 触发 +MAX_DELAY  5 秒 from %%tbname
10. 新版：SLIDING 触发 +MAX_DELAY  5 秒 from %trows
11. 新版：PERIOD(15s） 触发 %%tbname
12. 新版：SESSION_WINDOW: from %%tbname
13. 新版：COUNT_WINDOW: from %%tbname
14. 新版：EVENT_WINDOW: from %%tbname
15. 新版：STATE_WINDOW: from %%tbname

#### 11.2.3 测试报告

草稿见 [实时数据流计算](https://taosdata.feishu.cn/wiki/EeDXw5qchi1GHLkMgrvcryNsnEe)

### 11.3 乱序数据流计算

#### 11.3.1 数据集

与“实时数据计算”的数据集大体相同，不同点在于 20% 比例乱序数据

#### 11.3.2 测试场景

与“实时数据计算”的测试场景的大体相同，对于场景 7-10，还增加如下选项的测试
1. IGNORE_DISORDER 
2. WATERMARK 4 秒

#### 11.3.3 测试报告

## 12. Crash_gen 测试（郭向阳）

1. 更新 crash_gen 中流计算相关的部分
2. 持续运行，寻找故障

## 13. 兼容性测试（鲍之骁 完成）

### 13.1 测试要点

#### 13.1.1 流计算兼容性测试

**测试历史版本到流计算开发分支的兼容性 ( 3.3.3.0 , 3.3.4.0 , 3.3.5.0 , 3.3.6.0) ：**
1. 历史版本创建 snode ， 流计算 ， tsma 。
2. 升级 taosd 版本，此时启动会失败。
3. 使用历史版本启动，删除不兼容内容( 旧的TSMA、流计算任务和 snode(snode路径需要手动删除))
4. 使用新版本启动，创建基本的流，并验证流计算结果。
**测试结论：**
可以通过。

### 13.2 整体兼容性测试

**主要是迁移旧框架的兼容性用例。**
**测试结论**
发现一个订阅兼容性问题，存在订阅时无法升级 taosd 版本，应该是 plan 的版本号改错了，目前已修复。
JIRA:
TD-36956
