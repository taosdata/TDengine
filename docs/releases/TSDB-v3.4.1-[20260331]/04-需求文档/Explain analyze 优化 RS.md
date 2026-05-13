# Explain analyze 优化 RS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-02-03 | 2026-02-06 | 1.0 | 关胜亮 | 新建 |

## 2. 引言

### 2.1 术语与缩写名词

| Explain analyze | 用于分析查询语句的执行计划及算子执行详情，优化后可提供更精准的性能指标和更清晰的输出格式，助力性能瓶颈定位 |
| --- | --- |
| 算子 | 查询执行过程中的基本计算单元（如Table Scan、Projection、Data Exchange等），是性能统计和优化的核心对象 |
| start_ts（算子启动时间） | 算子首次开始执行的时刻，优化后不再作为输出指标 |
| end_ts（算子结束时间） | 算子完成全部数据处理的时刻，优化后不再作为输出指标 |
| input_wait_elapsed | 算子等待下游算子输入数据的累计时间，属于算子等待时间的一种，verbose级别输出 |
| output_wait_elapsed | 算子等待上游算子消费数据的累计时间，属于算子等待时间的一种，verbose级别输出 |
| exec_elapsed | 算子的实际执行时间，计算公式为：end_ts - start_ts - (input_wait_elapsed + output_wait_elapsed)，普通级别输出 |
| verbose（详细级别） | 输出级别之一，用于展示更详细的性能指标和算子信息，未开启时仅输出核心指标最大值 |
| vgroups（虚拟组） | 集群中的虚拟分组，多vgroups场景下，verbose级别会输出各vgroups的指标并标明vgroup_id |
| Datablock | 数据存储的基本单元，Table Scan算子相关指标会涉及Datablock的统计信息 |
| Format（输出格式） | Explain analyze支持的输出格式，包括text（默认）、html、json、graphviz四种 |
| graphviz | 一种可视化输出格式，其输出可导入graphviz工具，生成算子执行计划的树状拓扑图 |
| DML语句 | 数据操纵语句（如增删改），因存在数据变更风险，不直接支持Explain analyze，需提取查询部分单独分析 |
| Ratio（采样率） | 原计划新增的采样率功能，目前暂不支持，优化后不再输出错误采样率值 |

### 2.2 相关文档资料



### 2.3 优先级要求

中

### 2.4 版本要求

企业版和社区版都支持

## 3. 需求目标

优化Explain analyze功能，解决当前算子执行时间统计不准确、输出指标不足、可读性差、官网文档缺失等问题；新增算子性能指标（通用+专属），规范输出级别和格式；支持多种输出格式，增强性能瓶颈定位能力；明确DML语句支持方案，暂不实现Ratio功能；补充官网文档说明，为Explain analyze优化功能的开发落地提供明确指导，满足交付人员和用户的性能分析需求。

## 4. 功能需求

为解决当前Explain analyze存在的算子时间统计混乱、指标缺失、可读性差、格式单一等问题，结合交付人员和用户的核心需求，明确优化方向和具体功能要求，覆盖指标优化、格式调整、功能支持等核心场景，具体如下：

### 4.1 现状及问题

当前Explain analyze功能存在以下核心问题，需通过优化逐一解决：

#### 4.1.1 算子执行时间统计问题

目前仅输出算子的“起始时间”和“结束时间”，计算逻辑混乱且不准确；仅通过起止时间无法得出算子实际执行时间（未扣除等待时间），对性能调优意义不大；相关时间指标的实际含义不清晰，无法满足瓶颈定位需求。

#### 4.1.2 算子其他参数问题

Ratio关键字的采样率功能未实现，但未设置时会输出错误值（如0.001000），误导用户；算子相关性能指标不足，缺乏数据处理效率、资源占用等关键信息，无法全面评估性能瓶颈。

#### 4.1.3 输出可读性问题

输出格式杂乱，包含“QUERY_PLAN:”前缀和“*”分隔行，层级不清晰；未区分格式类和性能类指标，用户关注的性能指标被冗余信息掩盖；多vgroups场景下，无法直观查看各分组的性能差异。

#### 4.1.4 功能支持不足问题

不支持多种输出格式，无法满足不同场景下的使用需求（如可视化、自动化分析）；官网文档缺少对Explain analyze语句的详细介绍，用户使用门槛高；未明确DML语句的支持方案，存在数据变更风险。

### 4.2 算子执行时间优化

优化算子执行时间相关指标，删除冗余指标，新增核心性能指标，明确各指标的定义、诊断价值、单位及输出级别，具体要求如下：

#### 4.2.1 指标调整规则

删除原有的“起始时间”“结束时间”指标，新增4个核心时间指标；时间类指标（exec_elapsed、input_wait_elapsed、output_wait_elapsed）精确到小数点后三位（等价于微秒级）；exec_elapsed为普通级别输出，其余3个为verbose级别输出。

#### 4.2.2 新增时间指标详情

| 指标名称 | 含义 | 诊断价值 | 单位（精度） | 输出级别 |
| --- | --- | --- | --- | --- |
| exec_elapsed | 算子的实际执行时间（扣除等待时间） | 评估算子本身的计算开销 | 毫秒（小数点后三位） | 普通 |
| exec_elapsed_ratio | 当前算子实际执行时间在所有算子中的占比 | 快速发现耗时最高的算子，定位性能瓶颈 | 百分数（整数） | verbose |
| input_wait_elapsed | 等待下游数据的累计时间 | 判断下游算子是否为性能瓶颈 | 毫秒（小数点后三位） | verbose |
| output_wait_elapsed | 等待上游消费数据的累计时间 | 判断上游算子是否为性能瓶颈 | 毫秒（小数点后三位） | verbose |

### 4.3 算子其他指标新增

除时间指标外，新增算子通用性能指标（所有算子均包含）和专属性能指标（仅Table Scan算子包含），补充数据处理效率、资源占用等关键信息，助力全面定位性能瓶颈，具体要求如下：

#### 4.3.1 通用指标（所有算子）

| 指标名称 | 含义 | 诊断价值 | 输出级别 | 备注 |
| --- | --- | --- | --- | --- |
| rows_out | 算子输出数据行数 | 反映算子数据处理效率，评估系统资源需求 | verbose | 目前已有rows_in，仅具备filter能力的算子输出rows_out |
| bytes_in / bytes_out | 算子输入/输出数据量 | 评估系统资源（带宽、存储）需求 | verbose | bytes_out仅具备filter能力的算子输出 |
| filter_efficiency | 算子过滤效率 | 比值越小，过滤效果越好，资源利用率越高 | verbose | 可嵌入现有filter相关输出部分 |
| peak_memory_usage | 算子内存使用峰值 | 判断内存使用是否接近上限，是否存在内存瓶颈 | verbose | 需采样实现，目前暂无法落地 |

#### 4.3.2 专属指标（Table Scan算子）

| 指标名称 | 含义 | 诊断价值 | 输出级别 | 备注 |
| --- | --- | --- | --- | --- |
| block_avg_rows | 读入的Datablock的平均记录条数 | 判断数据稀疏程度 | verbose | - |
| block_intersection_ratio | 存在时间交织的Datablock比例 | 判断数据写入乱序情况 | verbose | - |
| stt_files / stt_files_size | 读取stt文件的个数、大小 | 判断是否有大量数据存储在stt文件中 | verbose | - |
| data_files / data_files_size | 读取data文件的个数、大小 | 评估数据读取的规模和效率 | verbose | - |

### 4.4 Verbose级别与输出可读性优化

规范verbose输出级别规则，优化输出格式，区分格式类和性能类指标，提升可读性，适配不同用户场景（普通用户、开发/运维人员），具体要求如下：

#### 4.4.1 Verbose级别规则

- 指标输出级别严格遵循4.2.2、4.3.1、4.3.2中的规定，普通级别仅输出核心性能指标，verbose级别输出全部指标。
- 多vgroups场景：未开启verbose时，仅输出各指标的最大值；开启verbose时，并排输出各vgroups的指标结果，并明确标明vgroup_id，方便对比各分组性能差异。

#### 4.4.2 输出可读性优化

##### 4.4.2.1 指标分类管理

将输出指标分为两类：格式类（columns、width、order等，与数据格式相关，无关数据分布和性能）、性能类（exec_elapsed、rows_in、bytes_in等，与数据处理和性能相关）。
新增内部配置项，仅开发人员可配置：开发阶段可显示全部指标（格式类+性能类），用于验证数据正确性；用户正常使用时，仅输出算子信息和性能类指标，屏蔽冗余的格式类指标。

##### 4.4.2.2 输出格式调整

删除现有输出中的“QUERY_PLAN:”前缀和“*”分隔行，简化输出内容。
所有算子行均以“->”开头，按照树状层级以三字符为一层缩进，明确算子之间的层级关系。

##### 4.4.2.3 格式对比示例

现有格式：
taos> explain analyze verbose false select * from stb where c1 > 0 and ts > now\G;
*************************** 1.row ***************************
QUERY_PLAN: -> Data Exchange 3:1 (cost=0.223..0.000 rows=0 width=16)
*************************** 2.row ***************************
QUERY_PLAN:    -> Projection (cost=0.252..0.254 rows=0 columns=3 width=16 input_order=asc )
*************************** 3.row ***************************
QUERY_PLAN:       -> Table Scan on stb (cost=0.000..0.252 rows=0 columns=2 pseudo_columns=1 width=16 order=[asc|1 desc|0] mode=ts_order data_load=data)
*************************** 4.row ***************************
QUERY_PLAN:             I/O: total_blocks=0.0 load_blocks=0.0 load_block_SMAs=0.0 total_rows=0.0 check_rows=0.0
*************************** 5.row ***************************
QUERY_PLAN:              max_row_task=0, total_rows:0, ep:tbd (cost=0.000..0.093)
*************************** 6.row ***************************
QUERY_PLAN: Ratio: 0.001000
*************************** 7.row ***************************
QUERY_PLAN: Planning Time: 1.471 ms
*************************** 8.row ***************************
QUERY_PLAN: Execution Time: 5.824 ms
Query OK, 8 row(s) in set (0.007431s)
目标格式（verbose false）：
taos> explain analyze verbose false select * from stb where c1 > 0 and ts > now\G;
-> Data Exchange 3:1 (exec_elapsed=0.223 rows_in=0)
   -> Projection (exec_elapsed=0.002 rows=0)
 -> Table Scan on stb (exec_elapsed=0.252 rows=0)
Query OK, 3 row(s) in set (0.007431s)
目标格式（verbose true）：
taos> explain analyze verbose true select * from stb where c1 > 0 and ts > now\G;
-> Data Exchange 3:1 (exec_elapsed=0.223 exec_elapsed_ratio=3% rows_in=0 bytes_in=0)
   -> vgroup_id: 3
   -> Projection (exec_elapsed=0.002 exec_elapsed_ratio=3% rows_in=0 rows_out=0 bytes_in=0 bytes_out=0)
      -> Table Scan on stb (exec_elapsed=0.252 exec_elapsed_ratio=90% rows_in=0 rows_out=0 bytes_in=0 bytes_out=0)

### 4.5 Format选项支持

参考duckdb相关功能，为Explain analyze新增Format选项，支持多种输出格式，适配不同使用场景（可视化、自动化分析等），具体要求如下：

#### 4.5.1 SQL语法

EXPLAIN [(FORMAT fmt)] [ANALYZE [VERBOSE]] query;
其中，fmt取值为text、html、json、graphviz，默认格式为text。

#### 4.5.2 各格式说明

- text：默认格式，优化后按4.4.2的目标格式输出，简洁清晰，适配普通用户使用。
- html：以HTML格式输出，支持在浏览器中查看，适配网页端展示场景。
- json：以JSON格式输出，结构化程度高，适配自动化脚本分析、数据导入等场景。
- graphviz：特殊可视化格式，输出可导入graphviz工具，生成算子执行计划的树状拓扑图；节点为算子，有向边为数据流转路径，可直观对比各节点计算量，判断数据倾斜问题。

### 4.6 DML语句支持方案

因DML语句（增删改）存在不可逆的数据变更风险，且主流数据库通过事务包裹规避风险的方案目前无法实现，故不建议Explain analyze直接支持DML语句，具体方案如下：
提取DML语句中的数据查询部分，单独对查询部分执行Explain analyze分析，避免直接操作DML语句导致的生产环境数据错误；明确提示用户，Explain analyze仅支持对查询部分的分析，不执行DML语句的实际数据变更操作。

### 4.7 Ratio功能说明

Ratio关键字对应的采样率功能暂不支持，优化后需删除现有错误的采样率输出（如Ratio: 0.001000），避免误导用户；后续若需实现该功能，需单独补充需求文档，明确实现方案和指标定义。

### 4.8 约束和限制

- 指标约束：peak_memory_usage指标因需采样实现，目前暂无法落地；时间类指标精度固定为小数点后三位，不可配置。
- verbose级别约束：多vgroups场景下，verbose级别输出会增加一定性能开销，建议仅在性能排查时开启。
- Format格式约束：graphviz格式需依赖外部graphviz工具才能生成可视化拓扑图，系统仅输出适配该工具的文本内容，不直接生成图片。
- DML约束：不支持直接对DML语句执行Explain analyze，仅可提取查询部分单独分析，需明确用户提示。
- 兼容性约束：优化后的输出格式和指标不影响现有Explain语句的正常执行，仅对Explain analyze进行优化。

## 5. 性能需求

- 额外统计开销约束：优化后新增的指标统计、格式转换等操作，额外性能开销需极低，不影响原有查询语句的执行性能，确保用户使用体验不受影响。
- 多格式输出性能：text、html、json格式的输出性能需与原有输出性能基本一致；graphviz格式输出因需生成结构化文本，可允许轻微性能损耗，但需控制在合理范围（不超过原有性能的10%）。
- 多vgroups场景性能：verbose级别输出时，遍历各vgroups统计指标的操作需优化，避免因vgroups数量过多导致性能大幅下降；未开启verbose时，仅统计指标最大值，确保性能不受影响。

## 6. 安全需求

- 数据安全：明确DML语句的支持边界，禁止直接对DML语句执行Explain analyze，避免用户混淆功能边界，导致生产环境不可逆的数据错误；添加明确的用户提示，告知功能限制。
- 输出安全：html、json、graphviz格式的输出内容需进行安全校验，避免包含恶意代码、特殊字符等，防止注入攻击或展示异常。
- 配置安全：内部配置项（格式类指标显示）需进行权限管控，仅内部开发人员可配置，普通用户无法修改，避免误操作导致输出混乱。

## 7. 兼容性需求

完全兼容现有版本，具体兼容性要求如下：
- 语法兼容：新增的FORMAT选项、VERBOSE参数为可选配置，不影响现有Explain analyze语句的正常执行；原有语句执行后，输出优化后的text格式，保持用户使用习惯。
- 功能兼容：优化后的指标输出、格式调整不破坏现有Explain语句的功能，Explain与Explain analyze的功能边界保持不变。
- 版本兼容：企业版和社区版同步支持优化后的功能，不同版本间的输出格式、指标定义保持一致，确保用户跨版本使用无差异。

## 8. 其他需求

- 文档需求：修改官网文档，新增Explain analyze语句的详细介绍，包括优化后的语法、指标说明（通用+专属）、输出格式（text/json/html/graphviz）、verbose级别使用方法、DML语句支持方案等；补充示例（如不同格式输出示例、verbose级别对比示例），降低用户使用门槛。
- 常见错误和排查：梳理优化后可能出现的错误场景（如graphviz格式输出失败、verbose参数使用错误、DML语句直接执行报错等），明确错误码和错误信息，确保用户可快速排查问题；结合相关JIRA问题，完善错误处理逻辑。
- 运维需求：无特殊运维操作，新增的内部配置项需提供清晰的配置说明；graphviz格式需告知用户依赖外部工具，提供工具安装和使用的简要指引。
