# show variables 支持模糊匹配 RS

## 1. 引言

### 1.1 术语与缩写名词

无

### 1.2 相关文档资料

JIRA [TS-5650](https://jira.taosdata.com:18080/browse/TS-5650)

### 1.3 优先级要求

中

### 1.4 版本要求

企业版和社区版都支持

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2025/01/22 | 1.0 | 关胜亮 | 新建 |

## 3. 需求目标

1. 整理系统表 `ins_configs` 和 `ins_dnode_variables` 的字段。
2. `show Variables` 支持通过 `like` 关键字对 `name` 字段进行模糊查找。

## 4. 功能需求

### 4.1 功能现状

#### 4.1.1 系统表：ins_configs

系统配置参数表，各字段如下

| # | 列名 | 数据类型 | 说明 |
| --- | --- | --- | --- |
| 1 | name | VARCHAR(32) | 配置项名称 |
| 2 | value | VARCHAR(64) | 该配置项的值。需要注意，value 为 TDengine 关键字，作为列名使用时需要使用 ` 进行转义。 |

#### 4.1.2 系统表：ins_dnode_variables

系统中每个 dnode 的配置参数。SYSINFO 属性 为 0 的用户不能查看此表。

| # | 列名 | 数据类型 | 说明 |
| --- | --- | --- | --- |
| 1 | dnode_id | INT | dnode 的 ID |
| 2 | name | VARCHAR(32) | 配置项名称 |
| 3 | value | VARCHAR(64) | 该配置项的值。需要注意，value 为 TDengine 关键字，作为列名使用时需要使用 ` 进行转义。 |

#### 4.1.3 快捷命令系列：show variables

1. show variables
```sql {wrap}
taos> show variables \G;
*************************** 1.row ***************************
    name: shellActivityTimer
   value: 3
   scope: both
category: global
    info: 
```

1. show local variables
```sql {wrap}
taos> show local variables \G;
*************************** 1.row ***************************
    name: shellActivityTimer
   value: 3
   scope: both
category: global
    info: 
```

1. show cluster variables
```sql {wrap}
taos> show cluster Variables \G;
*************************** 1.row ***************************
    name: shellActivityTimer
   value: 3
   scope: both
category: global
    info: 
```

1. show dnode <dnode_id> variables
```sql {wrap}
taos> show dnode 1 variables \G;
……
```

### 4.2 需求明细

系统表与快捷命令之间的对应关系较为模糊，容易让用户产生疑惑。在不改变现有语句主要行为的前提下，可以适当增加一些快捷命令和系统表，以提升清晰度和易用性。

#### 4.2.1 增加 show configs 命令

增加 `show configs` 命令， 和 `select * from ins_configs` 命令相互对应，用于展示集群内各个 dnode 的配置参数，这些参数必须完全一致。

#### 4.2.2 增加 ins_variables 系统表

新增 `ins_variables` 系统表，其字段与 `ins_dnode_variables` 表保持一致，展示内容亦完全相同。

#### 4.2.3 在文档中弱化 ins_dnode_variables 系统表

在官网的用户手册中，将 `ins_dnode_variables` 的名称更新为 `ins_variables`，并明确说明 `ins_dnode_variables` 将在未来版本中被弃用。在代码实现方面，基于兼容性考虑，`ins_dnode_variables` 的现有逻辑将暂时保留，以确保平稳过渡。

#### 4.2.4 show variables 系列命令，支持 like 关键字 

与变量相关的命令包括 `SHOW CONFIGS`、`SHOW VARIABLES`、`SHOW CLUSTER VARIABLES`、`SHOW DNODE VARIABLES` 和 `SHOW LOCAL VARIABLES`。这些命令目前均不支持 `LIKE` 关键字。为了提升灵活性，参考 `SHOW TABLES LIKE ''` 命令的语法，使上述 `SHOW` 命令均支持通过 `LIKE` 关键字对 `name` 字段进行模糊查询。
`SHOW VARIABLES`、`SHOW CLUSTER VARIABLES`、`SHOW DNODE VARIABLES` 和 `SHOW LOCAL VARIABLES` 这四个变量的区别，在 FS 中进行说明。具体行为更新到官网用户文档中，以便用户更好地理解和使用。

## 5. 性能需求

无。

## 6. 其他需求

以上语法在测试用例中的覆盖不完整，请整理已有的用例（python、sim），形成一个新的完整用例。
