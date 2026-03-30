# 副本变更 Test Spec 

## 一、测试目标

  测试的需求文档：[当前客户成功面临的挑战和举措](https://taosdata.feishu.cn/wiki/Eit4wdGLciwMzikhkoScvJXtnng) 的加强内部测试：副本变更测试，包括副本变更自身的性能、副本变更后的查询性能、副本变更的健壮性三项。

## 二、变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024-05-30 | 0.1 | guoxy | 初稿 |
| 2024-06-03 | 0.2 | guoxy | 增加测试结论，调整和补充第9部分：性能测试用例 |
| 2024-06-04 | 0.3 | guoxy | 修改测试结论，修改文档中模糊的介绍，修改测试用例内容写反的说明 |

## 三、测试结论

### 基于现在3.3.0.3版本的测试结论

原始测试报告可参考[[酒泉项目]2节点单副本升级双副本验证](https://taosdata.feishu.cn/wiki/N1s3wOtkhiTbnjkuvUWcbK5vnVe)，简要的测试结论见下：
测试数据：10个vgroups，两个超级表，每个表10亿数据，数据列是22列数据和2列tag，两台机器一共300G数据（包含wal文件）
1、在副本变更速度上：

| 副本变更 | 耗时 | 两者区别 |
| --- | --- | --- |
| 单副本--双副本 | 接近90分钟 | 单副本--》双副本：单副本（3.1.1.18版本）升级到 单副本（3.3.0.3版本）alter--》双副本 |
| 单副本--三副本 | 42分钟 | 单副本--》三副本：单副本（3.3.0.3版本）alter--》三副本 |

2、在副本变更后查询数据:
单副本->双副本【单副本（3.1.1.18版本）升级到 单副本（3.3.0.3版本）alter--》双副本】

| 测试sql | 升级前单副本 | 升级后双副本 | 下降幅度 | 升级后双副本compact | compact之后下降幅度 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| select count(*) from ins_tables where stable_name ='meters1'; | 0.113247s | 0.106208s | 持平 | 0.095724s | 持平略高 |  |
| select count(*) from meters1; | 0.087519s | 2.318983s | 下降29倍 | 0.063034s | 提升25% |  |
| select last(ts) from meters1; | 0.071613s | 8.167917s | 下降115倍 | 1.335016s | 下降19倍 |  |
| select sum(current) from meters1; | 0.099167s | 21.691253s | 下降219倍 | 0.172453s | 下降1.73倍 |  |
| select avg(phase) from meters1; | 0.093128s | 13.078685s | 下降140倍 | 0.094306s | 持平 |  |
| select first(ts) from meters1; | 0.129154s | 5.962807s | 下降46倍 | 1.657639s | 下降12.8倍 |  |
| select count(tt) from (select tbname tt,count(*) from meters1 group by tbname ); | 0.404867s | 39.365840s | 下降98倍 | 3.589550s | 下降8.86倍 |  |

单副本->三副本【单副本（3.3.0.3版本）alter--》三副本】

| 测试sql | 升级前单副本 | 升级后三副本 | 下降幅度 | 升级后三副本compact | compact之后下降幅度 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| select count(*) from ins_tables where stable_name ='meters1'; | 0.095321s | 0.102454s | 持平 | 0.095724s | 持平 |  |
| select count(*) from meters1; | 0.257317s | 0.474755s | 下降1.5倍 | 0.063034s | 提升4倍 |  |
| select last(ts) from meters1; | 0.476646s | 0.810133s | 下降1.8倍 | 1.335016s | 下降3倍 |  |
| select sum(current) from meters1; | 0.866787s | 2.381345s | 下降2.7倍 | 0.172453s | 提升5倍 |  |
| select avg(phase) from meters1; | 0.995028s | 2.655377s | 下降2.65倍 | 0.094306s | 提升10倍 |  |
| select first(ts) from meters1; | 0.783851s | 1.092280s | 下降1.3倍 | 1.657639s | 下降2.1倍 |  |
| select count(tt) from (select tbname tt,count(*) from meters1 group by tbname ); | 4.837791s | 6.332197s | 下降1.3倍 | 3.589550s | 提升1.35倍 |  |

### 基于优化后的版本 xxxxx 测试结论

测试后补充。

## 四、已知问题和限制

- https://jira.taosdata.com:18080/browse/TD-30173?filter=-2 单副本变更成双副本后查询性能下降明显
- https://jira.taosdata.com:18080/browse/TD-30196?filter=-2 单副本变双副本耗时比单副本变三副本耗时久

## 五、测试资源及环境

   测试平台：Linux x64
   测试资源：192.168.1.44

   对比前版本：升级版本3.1.1.18
   对比版本：优化前：3.3.0.3版本
                      优化后：3.0提测的开发分支（需补充taosd -V信息）或者3.3.1.0版本

## 六：测试范围及重点

 本测试主要对副本变更相关功能的重点测试，包含
  a：副本变更自身的性能
  b：副本变更后的查询性能
  c：副本变更的健壮性，在事务健壮性测试中进行，参考文档：[事务健壮性测试 Test Spec](https://taosdata.feishu.cn/wiki/FM3ZwJS49ibuVykwO0AcHWVxnqf)测试用例7

## 七、测试数据 

- a：升级场景数据集： 10vgroups（50vgroups，100vgroups）、10w字表，10亿数据，schema一共20列（一列ts，8列数据列，4列浮点列，4列字符串类型，1列bool，2列tag）。
- b：3.0版本副本变更 固定子表场景数据集 。固定10亿数量，10w子表，vgroup不同进行组合(2、10、20、50、100），schema一共20列（一列ts，8列数据列，4列浮点列，4列字符串类型，1列bool，2列tag）。
- c：3.0版本副本变更 固定vgroup场景数据集 。固定10亿数量，50个vgroup，子表数量不同进行组合（1000、1w、5w、10w、50w、100w），schema一共20列（一列ts，8列数据列，4列浮点列，4列字符串类型，1列bool，2列tag）。

## 八、测试用例

### 8.1 副本变更自身的性能测试用例

下面a、b、c场景对应的测试数据集见第七节中a、b、c数据集信息。

#### a：升级场景数据集测试场景（数据集a）

1、主要验证升级前后，数据库在新旧两个版本的在双副本和3副本变更时耗时的变化。
2、验证升级后，数据库变更成双副本和3副本两者之间的时间对比。

在对比前版本写入旧数据，备份，验证相同数据量，不同vgroups情况下单副本---双副本耗时（alter db replica 2）和单副本---三副本耗时（alter db replica 3）耗时。

| 对比前版本副本变更VS对比版本副本变更 | vgroups=10 | vgroups=50 | vgroups=100 | 耗时 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 单副本---双副本耗时（alter db replica 2） |  |  |  |  | 对比前版本不支持双副本，此行不测 |
| 对比前版本： 单副本---三副本耗时（alter db replica 3） |  |  |  |  |  |
| 对比版本： 单副本---三副本耗时（alter db replica 3） |  |  |  |  |  |

将备份数据升级到对比版本，分别进行双副本变更和三副本变更，记录耗时。会复用场景b中部分数据。

| 对比前版本--升级到-->对比版本 | vgroups=10 | vgroups=50 | vgroups=100 | 耗时 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 单副本---双副本耗时（alter db replica 2） |  |  |  |  |  |
| 单副本---三副本耗时（alter db replica 3） |  |  |  |  |  |


#### b：对比版本副本变更 固定子表场景数据集场景（数据集b）

在对比版本写入数据，然后进行副本变更
10亿数据，10w字表，分布在不同vgroups。

| 子表固定，vgroup变化 | 变更双副本总耗时 | 变更三副本总耗时 | 备注 |
| --- | --- | --- | --- |
| vgroup=2 |  |  |  |
| vgroup=10 |  |  |  |
| vgroup=20 |  |  |  |
| vgroup=50 |  |  |  |
| vgroup=100 |  |  |  |


#### c：对比版本副本变更 固定vgroup场景数据集场景（数据集c）

在对比版本写入数据，然后进行副本变更
10亿数据，1000子表～100w子表，分布在固定数量的vgroups中。

| vgroup=50，子表变化 | 变更双副本总耗时 | 变更三副本总耗时 | 备注 |
| --- | --- | --- | --- |
| 子表数量=1000 |  |  |  |
| 子表数量=1w |  |  |  |
| 子表数量=5w |  |  |  |
| 子表数量=10w |  |  |  |
| 子表数量=50w |  |  |  |
| 子表数量=100w |  |  |  |


### 8.2 副本变更查询性能测试用例

测试场景

#### a：升级场景数据集测试场景（数据集a）

在对比前版本写入旧数据，然后升级到对比版本变更副本后查询。

| 对比前版本--->对比版本 | 查询语句 | 升级前版本查询耗时 | 最新版本第一次查询耗时 | 最新版本第二次查询耗时 | 最新版本compact之后第一次查询耗时 | 最新版本compact之后第二次查询耗时 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| select count(*) from ins_tables where stable_name ='meters1'; |  |  |  |  |  |  |
| select count(*) from meters1; |  |  |  |  |  |  |
| select last(ts) from meters1; |  |  |  |  |  |  |
| select sum(current) from meters1; |  |  |  |  |  |  |
| select avg(phase) from meters1; |  |  |  |  |  |  |
| select count(tt) from (select tbname tt,count(*) from meters1 group by tbname ); |  |  |  |  |  |  |
| select first(ts) from meters1; |  |  |  |  |  |  |
| select count(*) from ins_tables where stable_name ='meters1'; |  |  |  |  |  |  |
| select count(*) from meters1; |  |  |  |  |  |  |
| select last(ts) from meters1; |  |  |  |  |  |  |
| select sum(current) from meters1; |  |  |  |  |  |  |
| select avg(phase) from meters1; |  |  |  |  |  |  |
| select count(tt) from (select tbname tt,count(*) from meters1 group by tbname ); |  |  |  |  |  |  |
| select first(ts) from meters1; |  |  |  |  |  |  |


#### b：对比版本副本变更 固定子表场景数据集场景（数据集b）

在对比版本写入数据，然后进行副本变更后查询。
10亿数据，10w字表，分布在不同vgroups（10、50、100三组）。


| 对比版本 | 查询语句 | 最新版本第一次查询耗时 | 最新版本第二次查询耗时 | 最新版本compact之后第一次查询耗时 | 最新版本compact之后第二次查询耗时 | 备注【每格三行，对应 vgroup=10、50、100三组】 |
| --- | --- | --- | --- | --- | --- | --- |
| select count(*) from ins_tables where stable_name ='meters1'; |  |  |  |  |  |
| select count(*) from meters1; |  |  |  |  |  |
| select last(ts) from meters1; |  |  |  |  |  |
| select sum(current) from meters1; |  |  |  |  |  |
| select avg(phase) from meters1; |  |  |  |  |  |
| select count(tt) from (select tbname tt,count(*) from meters1 group by tbname ); |  |  |  |  |  |
| select first(ts) from meters1; |  |  |  |  |  |
| select count(*) from ins_tables where stable_name ='meters1'; |  |  |  |  |  |
| select count(*) from meters1; |  |  |  |  |  |
| select last(ts) from meters1; |  |  |  |  |  |
| select sum(current) from meters1; |  |  |  |  |  |
| select avg(phase) from meters1; |  |  |  |  |  |
| select count(tt) from (select tbname tt,count(*) from meters1 group by tbname ); |  |  |  |  |  |
| select first(ts) from meters1; |  |  |  |  |  |


#### c：对比版本副本变更 固定vgroup场景数据集场景（数据集c）

在对比版本写入数据，然后进行副本变更。
10亿数据，1w子表、10w子表、100w子表，分布在固定数量的vgroups中。

| 对比版本 | 查询语句 | 最新版本第一次查询耗时 | 最新版本第二次查询耗时 | 最新版本compact之后第一次查询耗时 | 最新版本compact之后第二次查询耗时 | 备注【每格三行，对应 字表=1w、10w、100w三组】 |
| --- | --- | --- | --- | --- | --- | --- |
| select count(*) from ins_tables where stable_name ='meters1'; |  |  |  |  |  |
| select count(*) from meters1; |  |  |  |  |  |
| select last(ts) from meters1; |  |  |  |  |  |
| select sum(current) from meters1; |  |  |  |  |  |
| select avg(phase) from meters1; |  |  |  |  |  |
| select count(tt) from (select tbname tt,count(*) from meters1 group by tbname ); |  |  |  |  |  |
| select first(ts) from meters1; |  |  |  |  |  |
| select count(*) from ins_tables where stable_name ='meters1'; |  |  |  |  |  |
| select count(*) from meters1; |  |  |  |  |  |
| select last(ts) from meters1; |  |  |  |  |  |
| select sum(current) from meters1; |  |  |  |  |  |
| select avg(phase) from meters1; |  |  |  |  |  |
| select count(tt) from (select tbname tt,count(*) from meters1 group by tbname ); |  |  |  |  |  |
| select first(ts) from meters1; |  |  |  |  |  |


## 九、问题(Optional)

这里用于记录需要讨论的问题：
- 暂无

## 十、Jira

此feature相关的所有Jira, 标题中应包含统一的标签: replica

## 十一、测试计划 

2024.06 -- 2024.07。

## 十二、参考文档 

这里用于添加对该需求测试有帮助的文档链接：
[[酒泉项目]2节点单副本升级双副本验证](https://taosdata.feishu.cn/wiki/N1s3wOtkhiTbnjkuvUWcbK5vnVe)
