# TDengine 行业版 Test Spec

## 1. 测试目标

这里用于描述本需求主要的测试目标
- TDengine 行业版可以按所选功能启用打包，并且在没启用的功能授权激活以后，授权激活的功能应该可以正确工作
- TDengine 行业版在所选功能打包以后，可以成功安装，同时使得所选功能正确工作，所有组件的版本信息能正确反映所打的包，针对没选的功能不要在explorer展示出来
Fun Spec：[TDengine 行业版](https://taosdata.feishu.cn/wiki/KIkbwAUJwif3Alkxz2Acn2e6nOf)

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024-06-11 | 0.1 | @宋正勤 | draft |
|  |  |  |  |

## 3. 测试范围

这里用于描述本需求的覆盖范围：
- TDengine 行业版 按需启用可选功能
- Explorer可以正确显示所打包的版本信息，以及正确显示已选的功能选项，或者显示那些没被选择打包，但是之后又授权激活的功能
- 所有taos组件的-V命令输出信息需要正确显示，所有taos组件的日志输出信息包含正确的版本信息

## 4. 测试结论

测试结论中包含结论和关键数据，但不需罗列过多细节，此处需要把把握信息的详细程度，原则上是外部 Reviewer 能够获得清晰的测试结论且尽量没有冗余信息为标准（这个标准是一句正确的废话，具体实行中需要大家 case by case 来处理）
1. TDengine 行业版可以正确打包，在打包的时候没有启用的功能，在explorer页面不会显示出来，但是后续又激活授权以后，又能在explorer显示出来并正确工作
2. 目前TDengine 行业版仅支持 Power作为关键字，也即  电力版
3. TDengine行业版安装以后，各组件的-V版本信息已按设计要求正确实现

## 5. 开发质量报告

结论：本特性/优化的开发质量是（优，良，一般，差，很差）

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 |  |
| 基础测试用例不通过 |  |
| Bug 总数 | 5 |
| 严重 Bug 总数 |  |

## 6. 已知问题和限制

这里用于记录产品使用上的一些限制，包括不支持的场景等，以及在发版时没有解决的minor issues.
- 
- 

## 7. 测试环境

- OS: Windows, Linux(x64, arm)

## 8. 测试数据 (Optional)

这里用于描述性能、稳定性测试时的数据准备工作，包括但不局限于：

## 9. 测试用例

### 9.1 功能

在提测时，开发应保证基础用例全部通过。
| 版本 | 分类 | 介绍 | 1. 逐个激活可选功能选项的测试场景  （或者）
1. 打包时已选中的功能选项测试场景 | 测试步骤 | 预期结果 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| TDengine 行业版 | Power | 使用 Power 作为 自定义产品名打包，不勾选任何功能选项, 最后逐个激活各个功能选项 | stream | 1. show grants/show grants full 打包未选中时不显示
1. show grants/show grants full 激活以后显示
2. explorer展示授权的地方直接使用 show grants 的输出
3. 通过explorer在创建 data in 任务/展示可选功能的地方，用户可创建的数据源列表/可展示的功能取决于 show grants 的输出 | 1. show grants/show grants full 打包未选中时不显示
4. show grants/show grants full 激活以后显示
5. explorer展示授权的地方直接使用 show grants 的输出
6. 通过explorer在创建 data in 任务/展示可选功能的地方，用户可创建的数据源列表/可展示的功能取决于 show grants 的输出 | Pass |  |
|  |  |  | subscription |  |  | Pass |  |
|  |  |  | audit |  |  | Pass |  |
|  |  |  | csv |  |  | Pass |  |
|  |  | 查看explorer页面展示的版本信息是否正确（包含各个显示版本号的地方） |  |  | 查看explorer页面展示的版本信息应该正确显示（包含各个显示版本号的地方） | Pass |  |
|  |  | explorer切换到中文页面，查看explorer页面展示是否正确（包含各个显示版本号的地方） |  |  | 查看explorer页面展示的版本信息应该正确显示（包含各个显示版本号的地方） | Pass |  |
|  |  | 各个组件版本号查看(-V) |  | taos -V | 1. 输出完整的产品名称
1. 日志中涉及产品名称的地方，要替换为正确的完整产品名称 | Pass |  |
|  |  |  |  | taosd -V | 1. 输出完整的产品名称
1. 日志中涉及产品名称的地方，要替换为正确的完整产品名称 | Pass |  |
|  |  |  |  | taosAdapter -V | 输出格式抽象如下，其中 <> 为占位符，无 <> 包括的为常量字符串，[] 表示可选占位符
<component_name> version: <version> [internal version] # internal version 可选，只有 taosx 需要
git: <full commit ID>
[git: <full commit ID>] # 如果代码来自多个仓库，如 taosd 这里放 TDinternal 仓库的 commit ID，其它组件应该没有这个需要
build: <platform> <date> <time> <timezone> | Pass |  |
|  |  |  |  | taosKeeper -V |  | Pass |  |
|  |  |  |  | taosX -V |  | Pass |  |
|  |  |  |  | taos-explorer -V |  | Pass |  |
|  |  |  |  | taosx-agent -V |  | Pass |  |
|  |  |  |  | udfd -V |  | Pass |  |
|  |  |  |  | taosBenchmark -V |  | Pass |  |
|  |  |  |  | taosdump -V |  | Pass |  |
|  |  | TDengine-Power-client-3.3.1.0-Linux-x64.tar.gz |  | taosBenchmark -V |  | Pass |  |
|  |  |  |  | taosdump -V |  | Pass |  |
|  |  |  |  | taos -V |  |  |  |

### 9.2 可用性

测试用例包括但不局限于：
- 

### 9.3 可靠性

这里用于描述稳定性测试相关的内容。

### 9.4 性能

这里用于描述性能测试相关的内容。

### 9.5 安全性

测试用例包括但不局限于：
- 

### 9.6 兼容性

测试用例包括但不局限于：

### 9.7 本地化

测试用例包括但不局限于：
- 

## 10. 待讨论(Optional)

这里用于记录在测试或用例编写过程中想到的需要讨论的问题：

## 11. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: 

TD-30562


TD-30560


TD-30623


TD-30565


TD-30641


## 12. 测试计划 (Optional)

这里用于计划此 feature 测试的开始和结束时间。

## 13. 测试备忘 (Optional)

这里用于记录测试过程中发现的，与产品行为相关的一些重要信息。

## 14. 参考文档

[TDengine 行业版](https://taosdata.feishu.cn/wiki/KIkbwAUJwif3Alkxz2Acn2e6nOf)
