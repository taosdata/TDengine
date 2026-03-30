# 三员权限+强制访问控制 FS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-02-06 | 2026-02-09 | 0.1 | 徐开礼 | 新建 |
| 2026-02-11 | 2026-02-12 | 0.2 | 徐开礼 | 根据线下评审修改。涉及章节： [4.1.2 命令行启用 ](https://taosdata.feishu.cn/wiki/EPrzw2OvGitfjIkbdfdcBq2znuI#ZwOtdseRRoJId6xcDQ8clHE4nAb)[`强制三权分立`](https://taosdata.feishu.cn/wiki/EPrzw2OvGitfjIkbdfdcBq2znuI#ZwOtdseRRoJId6xcDQ8clHE4nAb) [4.2 强制访问控制(MAC)](https://taosdata.feishu.cn/wiki/EPrzw2OvGitfjIkbdfdcBq2znuI#JaK7dnSC9oHpJSxtxXlcrKupnZf) 6. [安全](https://taosdata.feishu.cn/wiki/EPrzw2OvGitfjIkbdfdcBq2znuI#WDKvdugq2oVCb1xx486c8bO3nxh) 1. [兼容性](https://taosdata.feishu.cn/wiki/EPrzw2OvGitfjIkbdfdcBq2znuI#PVUWdcmoDoLR2Dx0UjQchW60nje) 1. [运维](https://taosdata.feishu.cn/wiki/EPrzw2OvGitfjIkbdfdcBq2znuI#FBaxdlll6o9dqjxoj6scH4fAnYd) 1. [使用场景](https://taosdata.feishu.cn/wiki/EPrzw2OvGitfjIkbdfdcBq2znuI#I4JgdOvxzoPJitxzW4sc2qD0nlb) |
| 2026-02-19 | 2026-02-19 | 0.3 | 徐开礼 | - 角色仅作为权限的载体，不拥有 security_level。 |

## 2. 背景

### 2.1 三员权限(DAC)

```plaintext {wrap}
允许数据初始化阶段开启三员，开启三员权限后禁用超级管理员，三员权限开启后不可关闭。
```

- [TDengine 3.4.0.0 版本](https://taosdata.feishu.cn/wiki/SxFfwi3p0iTPZxkO5pRco9BmnKd)，实现了基于 RBAC 的访问控制，其中，三员权限是可选的。root 作为超级用户，同时拥有 SYSDBA/SYSSEC/SYSAUDIT 三种系统管理角色，也拥有跳过任何权限检查的能力；即使已经将 SYSDBA/SYSSEC/SYSAUDIT 角色授予其他用户，系统也不强制要求禁用 root，并且禁用 root 后还可以激活。
- 由于 root 用户存在上述问题，不能`实现真正的权力制衡、消除单点安全风险、建立不可抵赖的审计闭环`。
- 为解决上述问题，自 `TDengine 3.4.1.0` 版本起，支持开启三员权限后，系统自动永久禁用 root 用户。

### 2.2 强制访问控制(MAC)

```plaintext {wrap}
强制访问控制，主体级别、客体级别（1-5）
定义一个明确的安全等级体系（例如：公开<内部<秘密<机密）和基于此的访问控制规则
读取规则 (No Read Up)：用户的安全级别不低于数据的安全级别时，才允许读取
写入规则 (No Write Down)：用户的安全级别不高于数据的安全级别时，才允许写入
主体：考虑为用户，在用户表增加字段，记录其安全等级（如 security_level）
客体：考虑数据库、超级表，不考虑子表/虚拟表/普通表
```

## 3. 定义

### 3.1 术语定义

- **三权分立**    [系统将最高管理权限拆分为三个互斥的角色](https://taosdata.feishu.cn/wiki/SxFfwi3p0iTPZxkO5pRco9BmnKd)，以实现权力制衡：
```plaintext {wrap}
SYSDBA (系统管理员)：负责数据库运维、资源分配及对象创建、用户和角色创建。
SYSSEC (安全管理员)：负责用户和角色权限管理与分配，以及安全等级的设置。
SYSAUDIT (审计管理员)：负责安全审计日志的管理与查看。
```

- **强制三权分立  **系统中存在三个独立的普通用户分别拥有 SYSDBA、SYSSEC、SYSAUDIT 角色，设置并持久化记录已经进入 `三权分立`状态且状态不可逆，并且永久禁用 root 用户且不可再激活。
- **超级用户**    拥有 superUser 标记的 root 用户，可以绕过所有权限检查。
- **主体**    发起访问请求的操作实体，在 TDengine 中指**用户 (User)**。
- **客体**    受保护的资源实体。包括数据库 (Database)、超级表 (STable)、普通表 (Table)、视图 (View)、主题 (Topic)、流 (Stream) 等；子表继承所属超级表的权限配置与安全等级。
- **安全等级（Security Level）**用于量化**主体信任许可度**与**客体敏感程度**的整数标签。在 TDengine 的安全模型中，安全等级采用 0 至 4 的固定标度，数值与安全重要性成正比。
- **禁止上读（No-Read-Up, NRU)：** 仅当 `主体等级` >= `客体等级` 时，才允许读取。
- **禁止下写（No-Write-Down, NWD)：**仅当 `主体等级` <= `客体等级` 时，才允许写入。
- [**自主访问控制 (Discretionary Access Control, DAC)**](https://taosdata.feishu.cn/wiki/SxFfwi3p0iTPZxkO5pRco9BmnKd)**：**基于“资源所有权”的访问控制机制。数据所有者（Owner）对其拥有的对象拥有绝对控制权，并可通过 GRANT/REVOKE 指令将其拥有的权限`自主`的分发给其他主体或角色。访问决策仅基于主体身份和对应的权限表。
- **强制访问控制 (Mandatory Access Control, MAC)**：基于 Bell-LaPadula 模型，通过对比`主体`与`客体`的安全等级，强制实施访问控制。该控制逻辑独立于 DAC 运行，通过“高不读、低不写”的准则，实现数据的跨等级安全隔离，防止越权读取或敏感数据降级泄露。

### 3.2 安全等级定义

| 等级 | 名称 | 说明 |
| --- | --- | --- |
| 0 | 公开（Public） | 公开数据，最低安全要求即可访问。 |
| 1 | 内部（Internal） | 仅限内部使用，不得对外传播。 |
| 2 | 秘密（Secret） | 敏感数据，需要受控访问。 |
| 3 | 机密（Confidential） | 高度敏感数据，严格限于知悉范围。 |
| 4 | 绝密（Top Secret） | 最高敏感数据，仅最高安全等级可访问。 |

- 排序关系： 公开(0) < 内部(1) < 秘密(2) < 机密(3) < 绝密(4)

## 4. 行为说明

### 4.1 三员权限

#### 4.1.1 需求事项

- 提供启用`强制三权分立`模式的机制，提供`命令行参数`和`SQL 命令`两种启用方式。
- 启用前，系统必须校验 SYSDBA、SYSSEC、SYSAUDIT 三个角色分别至少有一个未禁用的非 root 用户持有。 
- 启用后，超级用户（root）被禁用：清除 superUser 标志，root 无法登录。
- 启用后不可关闭。
- 启用状态跨重启持久化。
- 启用后，所有因 superUser == 1 而短路的权限检查路径不再生效。
- 启用后，任何创建 root 或重新启用 root 的操作均失败。
- `强制三权分立`状态可以查询。

#### 4.1.2 命令行启用

- 适用于集群升级或者初始部署时，需要启用 `强制三权分立` 的场景。

##### 4.1.2.1 启用`强制三权分立`模式

```shell
taosd --SoD=mandatory      # SoD(Separation of Duties)，不区分大小写，只支持取值为 mandatory。
```

- 通过 --SoD=mandatory 命令行参数启动 taosd，行为如下：
```shell {wrap}
如果不是 mnode leader，正常启动；如果是 mnode leader，行为如下：
1）如果系统已经进入强制三权分立模式，正常启动并提供服务。
2）如果系统未进入强制三权分立模式，则检查系统中是否已经存在 3 个独立的有效普通用户分别拥有 SYSDBA、SYSSEC、SYSAUDIT 角色。
2.1）如果存在，则系统进入强制三权分立模式，root 账户永远禁用且不可再被激活。此时，系统可以正常提供服务。
2.2）如果不存在，则系统进入 SoD mandatory(initial) 模式(可通过 show security_policies; 查看)，仅允许进行创建用户、删除用户、修改用户、授予角色、撤回角色、查看用户、查看安全策略等用户管理和查看操作，不能执行其他操作；当系统检测到存在 3 个独立的有效普通用户分别拥有 SYSDBA、SYSSEC、SYSAUDIT 角色时，自动进入强制三权分立模式(SoD mandatory)，此时 root 账户被永久禁用且不能再被激活，系统可正常提供服务。
3）执行 2.2 期间，如果 mnode 不再 leader，则启动失败并且退出 taosd 服务；此时，如果其他节点被选举为 mnode leader 且新的 mnode leader 启动时指定了 --SoD=mandatory，也会执行 1) 2) 的操作，如果未指定，则正常启动。
4）因此，如果期望在集群升级或部署后初次启动时就启用强制三权分立模式，建议在所有的 mnode 节点启动时，均使用 --SoD=mandatory。
```

#### 4.1.3 SQL 命令启用

- 适用于集群升级、初始部署以及后续启动时，未启用 `强制三权分立` ，启动后需要启用  `强制三权分立` 的场景。

##### 4.1.3.1 前置条件

- 当前用户必须是 root（超级用户）或持有 SYSSEC 角色。
- 至少有一个非 root 且未禁用的用户被分配了 SYSDBA。
- 至少有一个非 root 且未禁用的用户被分配了 SYSSEC。
- 至少有一个非 root 且未禁用的用户被分配了 SYSAUDIT。
- `强制三权分立`模式尚未启用，已启用则忽略，不报错。

##### 4.1.3.2 启用`强制三权分立`模式

```sql {wrap}
alter cluster 'SoD' 'mandatory'; -- 不区分大小写
或 alter cluster 'separation_of_duties' 'mandatory'; -- 不区分大小写
```

- 行为如下：
```sql {wrap}
1) 如果 4.1.3.1 中的前置条件满足，先进入 SoD mandatory(enforcing) 模式(可通过 show security_policies 查看)，此时，为保证事务执行期间 4.1.3.1 中的条件不被破坏，不允许执行 drop user/disable user/revoke role(SYSDBA、SYSSEC、SYSAUDIT) 操作，事务执行完成后，系统进入 SoD mandatory 模式。
2）如果 4.1.3.1 中的前置条件不满足，则报错，并返回具体的错误原因。例如，No enabled user with SYSDBA role found to satisfy SoD policy。
```

##### 4.1.3.3 执行效果

- 在 SClusterObj 中增加 uint8_t flags 字段，取一个 bit 标识 SoD 的状态，并记录 operator 用户名和操作时间。
```sql
不设置全局变量标记 SoD 状态。
因为权限判断均基于用户，所以，将 SoD 标记随用户一起下发。
```

- 将 root 用户的 enable 和 superUser 字段均置为 0。
- 记录审计日志。

#### 4.1.4 测试用例

| 测试用例 | 描述 |
| --- | --- |
| F1-T1 | 三个角色均已分配时启用三权分立 → 成功，root 被禁用。 |
| F1-T2 | 未分配 SYSDBA 时启用 → 报错。 |
| F1-T3 | 未分配 SYSSEC 时启用 → 报错。 |
| F1-T4 | 未分配 SYSAUDIT 时启用 → 报错。 |
| F1-T5 | 启用后 root 登录 → 拒绝。 |
| F1-T6 | 启用后创建超级用户 → 拒绝。 |
| F1-T7 | 启用后关闭三权分立 → 拒绝。 |
| F1-T8 | 启用后重启 mnode → root 保持禁用状态。 |
| F1-T9 | 启用后撤销最后一个 SYSDBA 或 SYSSEC 或 SYSAUDIT → 拒绝（每个角色至少保留一个有效持有者）。 |
| F1-T10 | 启用后 SYSDBA 仍可执行数据库操作。 |

### 4.2 强制访问控制(MAC)

- TDengine 3.4.0.y 权限系统基于 RBAC 的自主控制 DAC。拥有权限管理的用户可以自由的授予/撤销权限。
- TDengine 3.4.1.y 引入 MAC，独立于 DAC，确保系统中的操作执行时，主体级别 >= 客户级别。因此，操作须同时通过 DAC 和 MAC 检查才允许被执行。

#### 4.2.1 需求事项

##### 4.2.1.1 **基本规则**

- MAC 默认开启，且不支持关闭。在 SClusterObj 中，记录启动时间(操作者为 SYSTEM，不需要记录)。
- 仅 SYSSEC 可以设置/修改用户和对象的安全等级。
- MAC 在 DAC 之上叠加执行；两者都必须通过验证。先检查 MAC，再检查 DAC，以便在 Fast-Fail 路径快速返回，提升效率。如果检查不通过，在返回的错误码中能够区分是 MAC 还是 DAC 导致。
- 用户拥有的 security_level 是一个范围 [min_level, max_level]，`禁止上读` 取 max_level，满足 max_level >= object.security_level，`禁止下写`取 min_level，满足 min_level <= object.security_level<=max_level。
<callout emoji="bulb" background-color="light-orange" border-color="light-orange">
- 由于要同时满足 `禁止上读` 和 `禁止下写`，如果只有一个单独的值，会影响业务。示例如下：
场景 1）用户 u1 拥有的 security_level 为 3，对数据库 db1 (security_level 为 3)同时拥有读/写权限。如果要将 db1 提级为 4，则首先要为 u1 提级为 4，否则，`禁止上读`的规则会造成 db1 提级期间 `读取失败`，但是，先为 u1 提级又会造成 u1 违背 `禁止下写` 的规则，造成 `写入失败`。
场景 2）用户 u1 拥有的 security_level 为 3，对数据库 db1 (security_level 为 3)同时拥有读/写权限。如果要将 db1 降级为 2，则首先要为 u1 降级为 2，否则，`禁止下写`的规则会造成 db1 降级期间 `写入失败`，但是，先为 u1 降级又会造成 u1 违背 `禁止上读` 的规则，造成 `读取失败`。
- 基于上述考虑，并参考其他产品的实现，将 user 的 security_level 设置为一个范围，则上述 2 个问题均可解决。具体如下：
Solution 1）
1.1) 将 u1 的 max_level 提为 4，min_level 保持 3 不变；
1.2) 将 db1 的 security_level 提为 4；
1.3) 将 u1 的 min_level 提为 4；
Solution 2)
2.1) 将 u1 的 min_level 降为 2，max_level 保持 3 不变;
2.2) 将 db1 的 security_level 降为 2；
2.3) 将 u1 的 max_level 降为 2；
上述操作期间，均未破坏 `禁止上读` 和 `禁止下写`  的规则。

- 当然，上述场景也有其他解决方案。例如，为用户临时引入 WRITE_ANY_LEVEL 的写特权，使用完成后再收回。该方案，不如 security_level range 的方案灵活和明确，如果忘记取消 WRITE_ANY_LEVEL 的写特权，容易造成数据泄漏；而 security_level 的范围取值，在 show users 时，会明确展示给用户。
</callout>

- 在进行 CREATE DATABASE/TABLE 等创建对象操作，满足 `主体.security_level >= 客体.security_level`。
- 在进行 SELECT/SHOW/SHOW CREATE/DESCRIBE/SUBSCRIBE 对象操作，满足 `主体.security_level >= 客体.security_level`（禁止上读）。
- INSERT/UPDATE 操作，满足 `主体.min_level <= 客体.security_level`（禁止下写）。
- DELETE 操作，仅涉及查询删除数量及记录删除标记，不涉及写入业务数据。因此，不受`禁止下写`限制，仅需满足`主体等级 >= 客体等级（禁止上读）`即可。。
- GRANT/REVOKE/ALTER/DROP 操作，满足 `主体.security_level >= 客体.security_level`（等级压制即可管理）。
- 可以在 DB 中创建高于 DB security level 的对象。
- 不可以在 DB 中创建低于 DB security level 的对象。

##### 4.2.1.2 **元数据安全等级 (Security Level)**

<callout emoji="bulb" background-color="light-orange" border-color="light-orange">
在 create/alter 设置安全等级的语法中，取关键词为 security_level；在 show/select 语法中，对应的列名均设置为 sec_level。 
</callout>


| 元数据类型 | 创建时可指定 | 可修改 | 默认值 | 备注 |
| --- | --- | --- | --- | --- |
| user | 是 | 是 | [0,1] | security_level 是一个区间：[min_level, max_level] - 普通用户的 security_level 属性默认为 [0,1]。 <callout emoji="bulb" background-color="light-orange" border-color="light-orange"> 因为目前创建用户，默认授予了 SYSINFO_1 的角色，而 SYSINFO_1 的角色 security_level 为 内部(1)。同时，未指定 security_level 或 升级上来的老版本的客体，默认 security_level 为 0。因此，将普通用户默认权限设置为 [0,1]. </callout> - root 用户的 security_level 为 [0,4]，且不可修改。 - 升级上来的老版本，用户最低权限为 0，最高权限依据其拥有的[角色赋予对应的权限](https://taosdata.feishu.cn/wiki/EPrzw2OvGitfjIkbdfdcBq2znuI#AmUkdySlSoEcXFxwggzcNE16ncb)。 - 降级时，max_level 不能小于已赋予的角色等级。 |
| role | N/A | N/A | N/A | - 角色仅作为权限的载体，不拥有 security_level。但是，拥有 SYSDBA、SYSSEC、SYSAUDIT、SYSAUDIT_LOG 角色的用户的最高 level 为 4,4,2,4。 |
| db | 是 | 是 | user.min_level | - 审计库，默认为 4（绝密）。 - 如果指定 security_level，则 user.min_level <= db.security_level <= user.max_level。 - information_schema/performance_schema 的等级为 0。 - 提级或降级时，只修改 db 的等级，不修改 db 内元数据对象的等级。 |
| stable | 是 | 是 | max(user.min_level, db.level) | 核心表对象。 - 创建 stable 时，未指定 security_level，则 stable 的 security_level 默认为 max(user.min_level, db.security_level)。 - 创建 stable 时，指定了 security_level，满足 db.security_level <= specified_level <= user.max_level. - DB 等级降低，直接操作即可。等级降低后，stable 的 level 保持不变。 - DB 等级提升，需要确保 DB 中的超级表、view 等所有对象的 security_level >= 期望降至的 db security_level。否则，报错。提级期间，不允许在 DB 中创建可自定义 security_level 的对象(DB inside 事务)。 // 虚拟表本期不支持，后续可考虑支持。 |
| child table | 否(继承stable) | 否(继承stable) | 继承 stable | - 继承自超级表，不支持独立设置及修改。 - information_schema/performance_schema 库中的表，等级为 0。 |
| normal table | 否 | 否 | 继承 db | - 继承 db，不支持独立设置及修改。 <callout emoji="bulb" background-color="light-orange" border-color="light-orange"> 因为继承自 db，在 db 安全等级变化时，如何通知 catalog cache 中，普通表的 table meta 进行更新？ - 可以不更新，每次使用时，实时获取 db 的 security_level。 - 在 catalog 中增加全局标记，e.g. dbCfgVersion，当有 db schema 更新时，继承自 db security_level 的 normal table 等元数据对象，在返回 table meta 时，重新获取 db security_level。 </callout> |
| view | 是 | 是 | 继承 db/table | - 创建/修改时，需要检查 base db/table，满足 view(level) >= max(base db/table level)。 - 在执行查询时，满足： user(level) >= view(level)。 - 在 db/table 等级发生变化时，延迟检查以确保 view(level) >= max(base db/table level)；如果不满足，标记 view 为不可用状态。 // 本期不支持，后续由业务开发方支持。 |
| topic | 是 | 是 | 继承 db/table | - 创建时，需要检查 base db/table，满足 topic(level) >= max(base db/table level)。 - 在执行查询时，满足： user(level) >= topic(level)。 - 在 db/table 等级发生变化时，延迟检查以确保 topic(level) >= max(base db/table level)；如果不满足，标记 topic 为不可用状态。 // 本期不支持，后续由业务开发方支持。 |
| stream | 是 | 是 | max(源表/源库/结果表/结果库) | - 在 create时，满足： max(result table/db level) >= max(source table/db level) stream(level) >= max(result table/db level) user(level) >= stream(level) 如果不指定，dest db 继承 source db，dest table 继承 source table。 - 在 start/resume/recalculate 时，满足： max(result table/db level) >= max(source table/db level) stream(level) >= max(result table/db level) user(level) >= stream(level) - 在运行时，如果 source/dest db/table 的安全等级发生变化，满足： max(result table/db level) >= max(source table/db level) stream(level) >= max(result table/db level) // 本期不支持，后续由业务开发方支持。 |
| index | 否 | 否 | 继承 stb | 暂不支持独立设置及修改，均继承所属 stb 的 security level。 // 本期不支持，后续支持。 |
| tsma | 否 | 否 | 继承 stb | 暂不支持独立设置及修改，均继承所属 stb 的 security level。 // 本期不支持，后续支持。 |
| rsma | 否 | 否 | 继承 stb | 暂不支持独立设置及修改，均继承所属 stb 的 security level。 // 本期不支持，后续支持。 |
| mount | 否 | 否 | 继承 db | max(mounted_db level) // 本期不支持，后续支持。 |
| function | 否 | 否 | 0 | security_level 为 0，不限制。 // 本期不支持，后续支持。 |

#### 4.2.2 SQL 语法

##### 4.2.2.1 设置修改 security_level

- user/database/stable 等元数据对象支持创建及修改时指定 security_level。示例如下：
```sql {wrap}
CREATE DATABASE secret_db SECURITY_LEVEL 4;
CREATE STABLE db.public_stb (ts TIMESTAMP, val FLOAT) TAGS (id INT) SECURITY_LEVEL 0;
CREATE STABLE db.secret_stb (ts TIMESTAMP, val FLOAT) TAGS (id INT) SECURITY_LEVEL 4;
CREATE USER reader SECURITY_LEVEL 2,3;
ALTER USER reader SECURITY_LEVEL 2,3;
ALTER STABLE db.secret_stb SECURITY_LEVEL 3;
ALTER DATABASE db SECURITY_LEVEL 3;
```

- user/db/stable 等元数据查看输出结果增加 security_level 列。

#### 4.2.3 错误码

#### 4.2.4 测试用例

| 测试用例 | 描述 |
| --- | --- |
| F2-T1 | 用户等级 3 读取客体等级 2 → 允许（满足 NRU）。 |
| F2-T2 | 用户等级 2 读取客体等级 4 → 拒绝（违反 NRU）。 |
| F2-T3 | 用户等级 2 写入客体等级 4 → 允许（满足 NWD）。 |
| F2-T4 | 用户等级 3 写入客体等级 1 → 拒绝（违反 NWD）。 |
| F2-T5 | 用户等级 3 读取客体等级 3 → 允许（等级相同）。 |
| F2-T6 | 用户等级 3 写入客体等级 3 → 允许（等级相同）。 |
| F2-T7 | DAC 允许但 MAC 拒绝 → 最终拒绝。 |
| F2-T8 | DAC 拒绝但 MAC 允许 → 最终拒绝。 |
| F2-T9 | 设置 STB 等级 < DB 等级 → 报错。 |
| F2-T10 | SYSSEC 设置用户等级 > 自身等级 → 报错。 |
| F2-T11 | 子表继承父超级表等级 → 通过查询验证。 |
| F2-T12 | 跨等级超级表查询及显示正确过滤。例如，show stables 等。 |
| F2-T13 | 高等级用户对低等级客体执行 DELETE → 允许。 |
| F2-T14 | 低等级用户对高等级客体执行 ALTER → 拒绝。 |
| F2-T15 | `ins_users` 显示 `sec_levels` 列。 |
| F2-T16 | `ins_users_full` 显示 `sec_levels` 列。 |
| F2-T17 | `ins_databases` 显示 `sec_level` 列。 |
| F2-T18 | `ins_stables` 显示 `sec_level` 列。 |

### 4.3 用户或角色命名规则

- 在一些场景，需要区分操作是系统发起还是用户发起。为便于区分，非系统`用户、角色` 不能为如下命名：
```sql {wrap}
1) 核心身份 SYSTEM, ROOT, ANONYMOUS 等
2) 安全三员 SYSDBA, SYSSEC, SYSAUDIT 等
3) 保留关键词 PUBLIC, NONE, NULL, DEFAULT, ALL, ANY 等
4) 元数据相关 INFORMATION_SCHEMA, PERFORMANCE_SCHEMA, INS 等
5) 特殊字符：以 [ 开头, 禁止以 _ 开头, 包含空格等
```

## 5. 性能

- 访问过程增加了校验，耗时会增加。根据不同的操作类型，增加幅度不应该超过 (20%-100%]。

## 6. 安全

- 用户拥有的权限和角色、以及用户和对象拥有的 security_level 信息，是 mandatory SoD 和 MAC 功能正常运转的前提。因此，其取值的正确性至关重要。
```sql {wrap}
1）针对 mandatory SoD 状态被篡改的风险，不设置全局变量标记其状态，而是随用户对象下发。
2）针对 vnode.json 中的 security_level 被篡改的风险，需要结合配置文件加密使用。
```

## 7. 兼容性

低版本升级上来的各种行为：例如，是否开启 SoD 升级上来的场景。
- 支持从低版本停机后，自动升级至 3.4.1.0 及以上的版本；不支持滚动升级。升级后，无法再降级。
```sql {wrap}
升级后：
1）root 自动设置 security_level [0,4]；其他用户的 min security_level 为 0，根据拥有的角色(SYSSEC:4，SYSAUDIT:4, SYSDBA:3, SYSAUDIT_LOG:4,SYSINFO_1:1,SYSINFO_0:0) 赋予 max security_level；多个角色的 security_level 取并集，min_level 最低为 0， max_level 最低为 1。
2）系统角色的 security_level 取值参照 4.2.1.3，非系统角色的 security_level 取值 为 0.
3)针对 db, stb，根据 owner 或 creator 拥有的 security_level，自动设置 security_level。针对 db, stb，考虑到兼容性问题，将其 security_level 设置为 0，以防止根据 owner/creator 拥有的 security_level 进行设置而导致 security_level 过高，原来拥有读/写权限的普通用户无法正常读写。
```

## 8. 运维

### 8.1 最佳实践

- 针对数据安全性要求较高的场景，初始运行时开启 `强制三员权限` 模式，并为`主体(用户)`、`客体(数据库、超级表等对象)`以及`角色`设置合理的安全等级。具体参照 [使用场景](https://taosdata.feishu.cn/wiki/EPrzw2OvGitfjIkbdfdcBq2znuI#I4JgdOvxzoPJitxzW4sc2qD0nlb) 中的描述。

### 8.2 注意事项

<callout emoji="bulb" background-color="light-orange" border-color="light-orange">
自 3.4.1.0 版本开始，默认支持强制访问控制（MAC），MAC 的基本规则是 `禁止上读(NRU)`、`禁止下写(NWD)`。在设置或修改 User、DB 或 Stable 的 security_level 时，可参照[使用场景](https://taosdata.feishu.cn/wiki/EPrzw2OvGitfjIkbdfdcBq2znuI#I4JgdOvxzoPJitxzW4sc2qD0nlb)中列出的具体问题，仔细评估，以防止设置后影响正常的写入和查询，造成业务中断。
</callout>

## 9. 使用场景

- 针对数据库访问进行权限检查和访问控制的操作。

### 9.1 老版本 ( < 3.4.1.0 ) 升级

<callout emoji="bulb" background-color="light-orange" border-color="light-orange">
仅支持停机升级，不支持滚动升级，升级后不支持降级。
</callout>

#### 9.1.1 通过[命令行参数](https://taosdata.feishu.cn/wiki/EPrzw2OvGitfjIkbdfdcBq2znuI#ZwOtdseRRoJId6xcDQ8clHE4nAb)开启 `强制三权分立`

- 升级后：1）如果系统中包含 3 个独立的有效普通用户分别拥有 SYSDBA、SYSSEC、SYSAUDIT 角色，自动进入 `强制三权分立` 模式，永久禁用 root 且不允许再激活，此时，系统可正常提供服务。2）否则，仅允许 root 用户登录，且登录后仅允许执行`创建用户、查看用户和分配角色`操作。通过 show users 命令，查看哪个系统角色未被普通用户持有，则创建普通用户并授予相应的系统角色。当系统检测到满足 `3 个独立的有效普通用户分别拥有 SYSDBA、SYSSEC、SYSAUDIT 角色` 条件时，则进入 `强制三权分立` 模式，此时，root 用户被禁用，不再允许任何操作，系统可正常提供服务。
- 以非 root 账户登录。
- 查看 show users。root 的 sec_level 为 [0,4] 且被禁用，其他用户取值为拥有角色的最高 sec_level。
- 查看 select name,sec_level from information_schema.ins_databases，普通 db 的 sec_level 为 0，系统 db 的 sec_level 为 0。
- 查看 select stable_name,sec_level from information_schema.ins_stables，sec_level 为 0。
- 查看 select policy_name, mode from information_schema.ins_security_policies ，SoD 和 MAC 对应的 mode 均为 mandatory。
- 此时，如果想要提升 db 或 stable 的 security_level，请参考 [提升安全等级](https://taosdata.feishu.cn/wiki/EPrzw2OvGitfjIkbdfdcBq2znuI#Bqznd7TXko8DghxCIShcXjn5n7b)。 

#### 9.1.2 通过[ SQL 命令](https://taosdata.feishu.cn/wiki/EPrzw2OvGitfjIkbdfdcBq2znuI#A8LOdr7QOoDgPxxPhkEcSdM1nkm)开启 `强制三权分立`

- 升级后，root 和 普通用户均可正常登录。
- 升级后，查看 show users。root 的 security_level 为 [0,4]，其他用户取值为拥有角色的最高 secuirty_level。
<callout emoji="bulb" background-color="light-orange" border-color="light-orange">
3.4.0.0 升级至 3.4.1.0，即使某个普通用户没有任何角色，其 security_level 也不是 [0,0]，而是 [0,1]。
</callout>

- 升级后，查看 select name,sec_level from information_schema.ins_databases，普通 db 的 security_level 为 0，系统 db 的 security_level 为 0。
- 升级后，查看 select stable_name from information_schema.ins_stables，security_level 为 0。
- 查看 select policy_name, mode from information_schema.ins_security_policies ，SoD 对应的 mode 为 enabled，MAC 对应的 mode 均为 mandatory。
- 以 root 用户登录，通过下述命令启用 `强制三权分立`：
```sql {wrap}
alter cluster 'SoD mandatory'; -- 不区分大小写
或 alter cluster 'separation_of_duties mandatory'; -- 不区分大小写
```

如果系统中已经包含 3 个独立的有效普通用户分别拥有 SYSDBA、SYSSEC、SYSAUDIT 角色，系统进入 `强制三权分立` 模式，永久禁用 root 且不允许再激活；否则，提示缺少某个系统角色的用户，root 则根据提示创建用户并分配角色，当系统检测到满足 `3 个独立的有效普通用户分别拥有 SYSDBA、SYSSEC、SYSAUDIT 角色` 条件时，进入 `强制三权分立` 模式（此时，root 用户被禁用，不再允许任何操作）。操作期间，系统一直可正常提供服务。
- 以非 root 账户登录。
- 查看 select policy_name, mode from information_schema.ins_security_policies ，SoD 和 MAC 对应的 mode 均为 mandatory。

### 9.2 新版本 ( >= 3.4.1.0 ) 部署

#### 9.2.1 通过命令行参数开启 `强制三权分立`

- 启动后：1）仅允许 root 用户登录，且登录后仅允许执行创建用户、查看用户和分配角色操作。此时，创建 3 个普通用户且分别赋予 SYSDBA、SYSSEC、SYSAUDIT 角色。当系统检测到满足 `3 个独立的有效普通用户分别拥有 SYSDBA、SYSSEC、SYSAUDIT 角色` 条件时，进入 `强制三权分立` 模式，此时，root 用户被禁用，不再允许任何操作，系统可正常提供服务。
- 以非 root 账户登录。
- 查看 show users。root 的 security_level 为 [0,4] 且被禁用，其他用户取值为拥有角色的最高 secuirty_level。
- 查看 select name,sec_level from information_schema.ins_databases，系统 db 的 security_level 为 0。
- 查看 select policy_name, mode from information_schema.ins_security_policies ，SoD 和 MAC 对应的 mode 均为 mandatory。
- 此时，以非 root 账户登录，创建用户、DB、Stable 等对象，根据需求设置合理的 security_level，开始提供服务。

#### 9.2.2 通过SQL 命令开启 `强制三权分立`

- 启动后，以 root 用户登录。
- 查看 show users。root 的 security_level 为 [0,4]。
- 查看 select name,sec_level from information_schema.ins_databases，系统 db 的 security_level 为 0。
- 查看 select policy_name, mode from information_schema.ins_security_policies ，SoD 对应的 mode 为 enabled，MAC 对应的 mode 均为 mandatory。
- 创建 3 个普通用户并分别授予 SYSDBA、SYSSEC、SYSAUDIT 角色。
- 通过下述命令启用 `强制三权分立`：
```sql {wrap}
alter cluster 'SoD mandatory'; -- 不区分大小写
或 alter cluster 'separation_of_duties mandatory'; -- 不区分大小写
```

此时，系统进入 `强制三权分立` 模式，永久禁用 root 且不允许再激活。
- 以非 root 账户登录。
- 查看 select policy_name, mode from information_schema.ins_security_policies ，SoD 和 MAC 对应的 mode 均为 mandatory。
- 此时，可以创建用户、DB、Stable 等对象，根据需求设置合理的 security_level，开始提供服务。

### 9.3 社区版

- show users 查看：1）root 账户的 security_level 为 [0,4]，拥有 SYSAUDIT、SYSDBA、SYSSEC 角色；2）普通用户也可以设置 security_level，但是不会实际生效，默认值与企业版一致，为 [0,1]。
- 执行 grant/revoke 语句直接报错。
- DB, stable 等对象也可以设置 security_level，但是不会实际生效，[默认值与企业版一致](https://taosdata.feishu.cn/wiki/EPrzw2OvGitfjIkbdfdcBq2znuI#PhlWdnaCSo1urRxM7QBcLwPpnOd)。
- 不支持 --SoD=mandatory 参数，包含时启动报错。
- 不支持下述命令，执行报错。
```sql {wrap}
alter cluster 'SoD mandatory'; -- 不区分大小写
或 alter cluster 'separation_of_duties mandatory'; -- 不区分大小写
```

### 9.4 社区版变更为企业版

- 与企业版升级至 3.4.1.0+版本一致。具体参照 [9.1](https://taosdata.feishu.cn/wiki/EPrzw2OvGitfjIkbdfdcBq2znuI#UUbid7FJwoJ1gDxsdYpc2sOdnge) 和 [9.2](https://taosdata.feishu.cn/wiki/EPrzw2OvGitfjIkbdfdcBq2znuI#N2yHdKW03oIWTzxUwOVcih7Mn6e) 的描述。

### 9.5 提升安全等级

- 用户 u1 拥有的 security_level 为 [3,3]，对数据库 db1 (security_level 为 3)拥有 use 权限，对 db1 中的 stb1 (security_level 3) 拥有读写权限。此时，需要将 db1 及其内部的超级表提级为 4。操作步骤示例如下：
```sql {wrap}
1) alter user u1 security_level 3,4;    -- 将 u1 的 max_level 提为 4
2) alter table db1.stb1 security_level 4; -- 将 db1.stb1 的 security_level 提为 4；
3）alter database db1 security_level 4; -- 将 db1 的 security_level 提为 4
4) alter user u1 security_level 4,4;    -- 将 u1 的 min_level 提为 4
```

- 用户 u1 拥有的 security_level 为 [2,3]，对数据库 db1 (security_level 为 3)拥有 use 权限，对 db1 中的 stb1 (security_level 3) 拥有读写权限。此时，需要将 stb1 提级为 4。操作步骤示例如下：
```sql {wrap}
1) alter user u1 security_level 3,4; 
2) alter table db1.stb1 security_level 4;
```

- 用户 u1 拥有的 security_level 为 [3,3]，对数据库 db1 (security_level 为 3)拥有 use 权限，对 db1 中的 stb1 (security_level 3) 拥有读写权限。此时，需要将 u1 提级为 4，以能够查看更高等级的对象，同时保持对 db1 读写能力。操作步骤如下：
```sql {wrap}
1) alter user u1 security_level 3,4;      -- 将 u1 的 max_level 提为 4, min_level 保持不变
```

- 用户 u1 拥有的 security_level 为 [3,3]，对数据库 db1 (security_level 为 3)拥有 use 权限，对 db1 中的 stb1 (security_level 3) 拥有读写权限。此时，需要将 u1 提级为 4，以便查看更高等级的对象，并且，不再保持对 db1 写能力。操作步骤如下：
```sql {wrap}
1) alter user u1 security_level 4,4;      -- 将 u1 的 min_level, max_level 均提为 4
```

### 9.6 降低安全等级

- 用户 u1 拥有的 security_level 为 [3,3]，对数据库 db1 (security_level 为 3) 拥有 use 权限，对 db1 中的 stb1 (security_level 3) 拥有读写权限。此时，需要将 db1 和其内部的超级表降级为 2。操作步骤示例如下：
```sql {wrap}
1) alter user u1 security_level 2,3;    -- 将 u1 的 min_level 降为 2;
2) alter database db1 security_level 2; -- 将 db1 的 security_level 降为 2.
3) alter table db1.stb1 security_level 2; -- 将 db1 内超级表降级为 2(如果有多个超级表，需要依次执行)
4) alter user u1 security_level 2,2;    -- 将 u1 的 max_level 降为 2；
```

- 用户 u1 拥有的 security_level 为 [3,3]，对数据库 db1 (security_level 为 3) 拥有 use 权限，对 db1 中的 stb1 (security_level 3) 拥有读写权限。此时，需要将 stb1 降级为 2。操作步骤示例如下：
```sql {wrap}
1) alter user u1 security_level 2,3;      -- 将 u1 的 min_level 降为 2;
2) alter database db1 security_level 2;   -- 将 db1 的 security_level 降为 2；
3) alter table db1.stb1 security_level 2; -- 将 db1.stb1 的 security_level 降为 2；
-- 该场景独立操作的现实意义不大，一般作为 db1 降级的后续操作。
```

- 用户 u1 拥有的 security_level 为 [3,3]，对数据库 db1 (security_level 为 3)拥有 use 权限，对 db1 中的 stb1 (security_level 3) 拥有读写权限。此时，需要将 u1 降级为 2，以能够向低等级的对象写入，同时保持对 db1 读写能力。操作步骤如下：
```sql {wrap}
1) alter user u1 security_level 2,3;      -- 将 u1 的 min_level 降为 2, max_level 保持不变
```

- 用户 u1 拥有的 security_level 为 [3,3]，对数据库 db1 (security_level 为 3)拥有 use 权限，对 db1 中的 stb1 (security_level 3) 拥有读写权限。此时，需要将 u1 降级为 2，以能够向低等级的对象写入，并且，不再保持对 db1 读写能力。操作步骤如下：
```sql {wrap}
1) alter user u1 security_level 2,2;      -- 将 u1 的 min_level、max_level 均降为 2,
```

## 10. 约束和限制

- 仅企业版支持 `强制三员权限（Mandatory Sod)` 和 `强制访问控制（MAC）` 功能，社区版不支持。
- 社区版行为如下：
```sql {wrap}
1) root 的 security_level 为 [0,4]，包含 SYSDBA、SYSSEC、SYSAUDIT 角色，但不实际生效。
2）普通用户可设置 security_level，默认值与企业版一致，为 [0,1]，包含 SYSINFO_1 角色，但不实际生效。
3）db,stable 等对象可以设置 security_level，默认值与企业版一致，但不实际生效。
```

- 3.4.1.0 版本，仅 user, db, stable, child table, normal table 等核心对象支持 MAC 控制逻辑（child table 继承 stable 的 security_level，normal table 继承 db 的 security level），role 不支持 MAC。virtual table、view、topic、stream、index、rsma、tsma、mount、function 等对象，暂不支持 MAC 控制逻辑，后续版本会逐步支持。具体参照 [元数据安全等级](https://taosdata.feishu.cn/wiki/EPrzw2OvGitfjIkbdfdcBq2znuI#PhlWdnaCSo1urRxM7QBcLwPpnOd) 中的描述。

## 11. 常见错误和排查

- 用户操作失败，错误码对照表

| Error code | description | note |
| --- | --- | --- |
| TSDB_CODE_MND_ROLE_NO_VALID_SYSDBA | No enabled user with SYSDBA role found to satisfy SoD policy | `开启强制三员权限` 时报错 |
| TSDB_CODE_MND_ROLE_NO_VALID_SYSSEC | No enabled user with SYSSEC role found to satisfy SoD policy | `开启强制三员权限` 时报错 |
| TSDB_CODE_MND_ROLE_NO_VALID_SYSAUDIT | No enabled user with SYSAUDIT role found to satisfy SoD policy | `开启强制三员权限` 时报错 |
| TSDB_CODE_OPS_NOT_SUPPORT | Only SYSSEC can enable Separation of Duties (SoD) | `开启强制三员权限` 时报错 |
| TSDB_CODE_MAC_NO_READ_UP | Insufficient user security level to read (No-Read-Up) |  |
| TSDB_CODE_MND_MAC_NO_WRITE_DOWN | User security level is too high to write (No-Write-Down) |  |
| TSDB_CODE_MAC_INVALID_SECURITY_LEVEL | Invalid security level: must be between 0 and 4 |  |
| TSDB_CODE_MAC_LEVEL_ESCALATION | Cannot set security level higher than your own security level |  |
| TSDB_CODE_MND_MAC_OBJ_LEVEL_OVER_DB | Object security level exceeds DB security level |  |

## 12. 可观测性

### 12.1 集群安全策略

- 增加命令 `show security_policies;`  和  `select * from information_schema.ins_security_policies;`，用于展示集群安全策略：
```sql {wrap}
show security_policies;
select * from information_schema.ins_security_policies;

-- 未启用 sod mandatory 的状态
taos> show security_policies\G;
*************************** 1.row ***************************
       name: SoD
       mode: enabled
   operator: SYSTEM
last_update: 2026-02-22 11:31:42.728
       desc: non-mandatory, root not disabled
*************************** 2.row ***************************
       name: MAC
       mode: mandatory
   operator: SYSTEM
last_update: 2026-02-22 11:31:42.728
       desc: security levels 0-4, non-configurable
Query OK, 2 row(s) in set (0.015720s)

-- 通过 taosd --sod=mandatory 启动的中间状态。该状态下，仅允许执行 CREATE/DROP/ALTER USER, GRANT/REVOKE ROLE, SHOW USERS, SHOW SECURITY_POLICIES 等用户管理和查看、集群安全策略查看操作。
taos> show security_policies\G;
*************************** 1.row ***************************
       name: SoD
       mode: mandatory(initial)
   operator: 
last_update: 2026-02-22 11:07:51.751
       desc: Initial phase: mandatory roles missing, only account setup operations are allowed
*************************** 2.row ***************************
       name: MAC
       mode: mandatory
   operator: SYSTEM
last_update: 2026-02-22 11:07:51.751
       desc: security levels 0-4, non-configurable
Query OK, 2 row(s) in set (0.016527s)

-- 执行 alter cluster 'sod' 'mandatory' 中间状态。该状态下，为保证操作的原子性，不允许执行 DISABLE USER，DROP USER, REVOKE ROLE 操作，以防止在状态转换期间，原本满足三权分立的用户账号被非预期的变更。
taos> show security_policies\G;
*************************** 1.row ***************************
       name: SoD
       mode: mandatory(enforcing)
   operator: root
last_update: 2026-02-22 10:22:23.566
       desc: Enforce phase: transitioning mode, account destructive operations are blocked
*************************** 2.row ***************************
       name: MAC
       mode: mandatory
   operator: SYSTEM
last_update: 2026-02-22 10:21:41.236
       desc: security levels 0-4, non-configurable
Query OK, 2 row(s) in set (0.012882s)

-- sod mandatory 最终的稳定状态   
taos> show security_policies\G;
*************************** 1.row ***************************
       name: SoD
       mode: mandatory
   operator: root
last_update: 2026-02-22 10:36:03.204
       desc: system is operational, root disabled permanently
*************************** 2.row ***************************
       name: MAC
       mode: mandatory
   operator: SYSTEM
last_update: 2026-02-22 10:35:47.011
       desc: security levels 0-4, non-configurable
Query OK, 2 row(s) in set (0.017721s)

注：不再允许用户创建 SYSTEM 的用户名(不区分大小写)。
```

### 12.2 安全等级

- 用户和角色拥有的权限可通过下述命令查看其拥有的角色、安全等级和权限等信息。
```sql {wrap}
show users； // 查看用户拥有的角色和安全等级
taos> select name,sec_level` from information_schema.ins_users;
            name            |      sec_levels      |
====================================================
 root                       |      [0,4]           |
 u1                         |      [3,3]           |
 Query OK, 2 row(s) in set (0.017577s)
 
show user privileges; // 查看用户拥有的权限
show role privileges; // 查看角色拥有的权限
```

- 用户可通过下述命令查看对象拥有的安全等级。
```sql {wrap}
select * from information_schema.ins_databases; 
select * from information_schema.ins_stables; 
```

### 12.3 审计与日志

- SoD 通过命令行参数或 SQL 命令开启的动作，无论成功或失败，均会记录审计日志和日志文件。
- 设置和修改 security_level 的动作，无论成功或失败，均会记录审计日志和日志文件。
- 审计日志的详细行为参考： [安全审计 FS](https://taosdata.feishu.cn/wiki/XmSVwBepXiBHopkwu41cGW50n5f)

## 13. 安装和卸载

- 无特殊要求

## 14. 文档

- 需要修改官网文档

## 15. 参考

- [访问控制 RS](https://taosdata.feishu.cn/wiki/Y12Ywd797ieHBBkVZsqcpsRgnAg)
- [访问控制 FS](https://taosdata.feishu.cn/wiki/SxFfwi3p0iTPZxkO5pRco9BmnKd)
- [三员权限 + 强制访问控制 - 威胁建模报告](https://taosdata.feishu.cn/wiki/U6DXwBf2kiIxbPklVoTcI20GnVc)
- [安全审计 FS](https://taosdata.feishu.cn/wiki/XmSVwBepXiBHopkwu41cGW50n5f)

## 16. 附录
