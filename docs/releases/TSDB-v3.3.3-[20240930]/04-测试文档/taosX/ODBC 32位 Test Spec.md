# ODBC 32位 Test Spec

<quote-container>
文档命令规则：Feature 或 优化名称  - Test Spec
</quote-container>

## 1. 测试目标

- 验证 ODBC 32位驱动程序可以被SCADA调用生效
- 验证ODBC 32位驱动程序读写性能满足设计预期

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 7.23 | 1.0 | 霍宏 |  |
| 8.2 | 1.1 | 霍宏 | review会后补充用例，修改性能测试场景 |

## 3. 测试范围

- ODBC 32位被SCADA调用基本增删改查操作
- ODBC 32位读写性能

## 4. 测试结论

- ODBC 32位接口满足基本功能使用
- ODBC 32位读写性能满足客户场景需求
- 未实现接口及SCADA场景下使用限制详见见第6小节及FS中对接口支持的描述

## 5. 开发质量报告

结论：本特性/优化的开发质量是 优

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 1 |
| 严重 Bug 总数 | 0 |

## 6. 已知问题和限制

- 在KingSCADA上不支持SQLInsert函数、不支持SQLFirst等游标函数
- 在KingSCADA上不支持记录体创建超表、子表
- 在KingSCADA上不支持报警事件配置及写入
- 不支持DSN参数配置 [TD-31925](https://jira.taosdata.com:18080/browse/TD-31925) 跟进
- 不支持kepware  [TD-32210](https://jira.taosdata.com:18080/browse/TD-32210) 跟进

## 7. 测试环境

- OS: Windows

## 8. 测试数据 (Optional)

这里用于描述性能、稳定性测试时的数据准备工作，包括但不局限于：
- 创建10000子表(覆盖各种数据类型单列)
<quote-container>
Create stable odbcstb (ts TIMESTAMP, qualitystamp INT, val1 FLOAT）
TAGS(factory VARCHAR(20), area VARCHAR(20), equipment VARCHAR(20), tagName VARCHAR(20), datasource VARCHAR(20), unit VARCHAR(20))
</quote-container>

- 插入数据
taosBenchmark -l 2 -b INT,FLOAT -A BINARY\(20\),BINARY\(20\),BINARY\(20\),BINARY\(20\),BINARY\(20\),BINARY\(20\)

> ⚠ 嵌入文件，需在飞书中查看 (token: NFGEby3fpoKnHTxSrjAcH3eQnrc)

## 9. 测试用例

### 9.1 功能

在提测时，开发应保证基础用例全部通过。
| 类型 | 测试目的 | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| SCADA记录体方式调用 | SQLConnect | 按钮点击事件调用函数 | 函数返回成功 |  | Pass |  |
|  | SQLExecute调用create table | 按钮点击事件调用函数 | 函数返回成功 |  | Pass | 使用SQLCreateTable函数不支持using、tags关键字，可以使用SQLExecute执行create语句 |
|  | SQLDropTable | 按钮点击事件调用函数 | 函数返回成功 |  | Pass |  |
|  | SQLInsert | 按钮点击事件调用函数 | 函数返回成功 | Y |  | 不兼容 |
|  | SQLDelete | 按钮点击事件调用函数 | 函数返回成功 |  | Pass |  |
|  | SQLSelect | 按钮点击事件调用函数 | 函数返回成功 | Y | Pass | [TD-31611](https://jira.taosdata.com:18080/browse/TD-31611) |
|  | SQLExecute调用select | 按钮点击事件调用函数 | 函数返回成功 |  | Pass |  |
|  | SQLExecute调用alter tag | 按钮点击事件调用函数 | 函数返回成功 |  | Pass |  |
|  | SQLExecute调用alter column | 按钮点击事件调用函数 | 函数返回成功 |  | Pass |  |
| SCADA数据集方式调用 | KDBGetDataset | 按钮点击事件调用函数 | 函数返回成功 | Y | Pass |  |
|  | KDBGetConnectID | 按钮点击事件调用函数 | 函数返回成功 |  | Pass |  |
|  | KDBGetDataset1 | 按钮点击事件调用函数 | 函数返回成功 |  | Pass |  |
|  | KDBExecuteStatement | 按钮点击事件调用函数 | 函数返回成功 |  | Pass |  |
|  | KDBExecuteStatement1 | 按钮点击事件调用函数 | 函数返回成功 |  | Pass |  |
| 事件数据写入 | 生成事件可写入数据库 | 用mysql选项配置TDengine的DSN连接 | 事件数据写入数据库 |  |  | 不支持 |
| DSN连接 | 连接数据库成功 | DSN配置连接6041端口 | 测试连接可成功 | Y | Pass |  |
|  | 连接云服务成功 | DSN配置连接云服务url | 测试连接可成功 |  | Pass |  |
| DSN参数配置 |  |  |  |  |  | 不支持 |
| PowerBI使用ODBC | 32位ODBC支持PowerBI | 在PowerBI获取数据，使用32位DSN | 连接成功
可浏览数据 |  | Pass |  |
| 日志 | 连接失败有日志追踪；检查TAOS_ODBC_LOG_LEVEL DEBUG级别 | 设置环境变量TAOS_ODBC_LOG_LEVEL=DEBUG,TAOS_ODBC_LOGGER=temp，DSN配置数据库不存在 | 日志显示数据库不存在 |  | Pass |  |
|  | SQL语法错误日志追踪 | 设置环境变量TAOS_ODBC_LOG_LEVEL=DEBUG,TAOS_ODBC_LOGGER=temp，使用非法SQL命令调用 | 日志显示SQL语法错误内容 |  | Pass |  |

### 9.2 可用性

无

### 9.3 可靠性

无

### 9.4 性能

这里用于描述性能测试相关的内容。

| 使用场景 | 类别 | 要求 | 场景说明 | 备注 | 测试结果 |
| --- | --- | --- | --- | --- | --- |
| 支持10个子表并发查询，每个查询5000条记录 | 查询 | 2秒内完成查询 | Websocket+SQL+单列结构+子表数10+单表数据量5000+Select * from meters; | Int单列表结构 | 0.231s |
| 支持10000个子表同时查询最新数据 | 查询 | 1秒内完成查询 | Websocket+SQL+单列结构+子表数10000+单表数据量10000+Select last_row(*) from meters group by tbname; | Int单列表结构 | 0.412s |
| 支持10000个子表同时写入最新数据 | 写入 | 1秒内完成写入 | Websocket+SQL(500)+interlace+单列结构+子表数10000+并发20 | ODBC SQL（10000/10000） 每表一条 并发 10/1 | 0.07s |
| ~~支持写入报警事件记录数据，30个字段左右~~ | ~~写入~~ | ~~每秒数据写入100条~~ |  | 不支持 |  |
| 支持5个客户端并发执行，查询10000个子表的最新数据和写入10000个子表的最新数据 秒级 每客户端2000条写入 | 稳定性 | 持续3*24小时压测，无异常 | Select last_row insert | 创建10000子表； 每个子表插入5000条记录 用压测工具测试 | 通过 |

备注：建立的模型是通常是vqt形式，就是变量值，质量戳、时间戳，标签设置为factory、area、equipment、tagName、datasource、unit等6-10个字段。

### 9.5 安全性

无

### 9.6 兼容性

在不同系统上验证可用
- [x] Windows 10
- [x] Windows Server 2019 

### 9.7 本地化

无

## 10. 待讨论(Optional)

- [x] 是否支持Windows 7： 不支持
- [x] 是否支持SQLUpdate： 只支持schema修改
- [x] 是否支持报警事件写入：  不支持

## 11. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: [odbc-32]

## 12. 风险评估

scada上报警事件记录无TDengine选项，尝试用mysql选项，有不兼容风险

## 13. 测试备忘 (Optional)

scada安装包
<view type="1">

  > ⚠ 嵌入文件，需在飞书中查看 (token: THlEbfLvJofPCpxMSxjcSRWUnNf)

</view>

scada画面文件

## 14. 参考文档 (Optional)

- [ODBC 32位驱动程序](https://taosdata.feishu.cn/wiki/LOHFwuQGJiLAMWkmWIkcYsXHn5c)
- [ODBC 32位产品需求](https://taosdata.feishu.cn/wiki/U8CFwRYYViItnSkgDK3cvHJinKb)
