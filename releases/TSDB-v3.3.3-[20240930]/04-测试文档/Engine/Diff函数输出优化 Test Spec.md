# Diff函数输出优化 Test Spec

## 1. 测试目标

测试需求文档：[diff 函数输出优化](https://taosdata.feishu.cn/wiki/M8i4w1zfritngjkEzlqcWeeXnAc)
具体需求包含两个：
[TD-29154](https://jira.taosdata.com:18080/browse/TD-29154)：diff 函数在碰到 null  时，输出大量无效的null, 没有实现 null 值过滤的规则，因此需要做一些优化。
[TD-24514](https://jira.taosdata.com:18080/browse/TD-24514) ：当select 多个 diff 时，当前版本不允许 diff 设置为忽略负值，这个 sql 应当被允许。

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024-07-09 | 1.0 | guoxy |  |

## 3. 测试结论

功能测试通过。
性能测试在新版本中，diff（0、1、2、3）四种组合性能差别忽略不计。
但在新旧版本对比时：
对比版本，V3.0【date-0903】VS V3.3.2.0【date-0628】
性能略有下降，约14.6%～35.2%【数据量=1亿】，需要分析一下

| vgroup | 每个表行数 | 子表数 | diff(ts,0)-old | diff(ts,0)-new | diff(ts,1)-old | diff(ts,1)-new | 下降幅度（new-old）/new*100% |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 10 | 1000 | 10w | 21.591658s | 25.277641s | 18.142290s | 26.179074s | 14.6%-30.6% |
| 20 | 1000 | 10w | 19.311520s | 24.856891s | 17.292603s | 25.918921s | 22.3%-33.3% |
| 50 | 1000 | 10w | 14.306539s | 22.075898s | 16.416307s | 20.974270s | 21.7%-35.2% |
| 50 | 2000 | 5w | 14.013530s | 19.928115s | 13.053929s | 19.248273s | 29.7%-32.1% |
| 50 | 200 | 50w | 53.932999s | 75.112528s | 54.499512s | 76.535811s | 28.2%-28.8% |


## 4. 开发质量报告

结论：本特性/优化的开发质量是 优（优，良，一般，差，很差）

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 （测试阻塞，无法进行） | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 0 |
| 严重 Bug 总数 | 0 |

## 5. 已知问题和限制

无

## 6. 测试资源及环境

测试平台：Linux x64
测试资源：192.168.1.64
测试版本：V3.3.3.0

## 7. 测试范围和重点

本次测试的重点如下：
1. DIFF(expr [, ignore_option])ignore_option=0\1\2\3的diff结果正确性
2. ignore_option配置之后对null过滤是否生效。
3. ignore_option配置之后对负值的处理是否生效。
4. diff在大数据情况下的性能。

## 8. 测试数据

功能测试数据准备：
1、diff函数使用的的数据类型包括数值类型，时间戳和bool类型，因此创建有规律的数据，包含上述各类型和一列非上述类型。
2、创建普通表，超级表，具体细分如下：

| 普通表 | 带复合主键 |  | 数据组合中包含null和非null，下同 |
| --- | --- | --- | --- |
|  | 不带复合主键 |  |  |
| 超级表 | 带复合主键 | 超级表的子表中包含相同ts+相同pk |  |
|  | 带复合主键 | 超级表的子表中包含相同ts+不同pk |  |
|  | 带复合主键 | 超级表的子表中包含不同ts+相同pk |  |
|  | 带复合主键 | 超级表的子表中包含不同ts+不同pk |  |
|  | 不带复合主键 | 超级表的子表中包含相同ts |  |
|  | 不带复合主键 | 超级表的子表中包含不相同ts |  |

## 

性能测试数据准备：
旧版本：**TDengine-enterprise-3.3.2.0-Linux-x64.tar.gz**
新版本：[taosd -V
TDengine Enterprise Edition
taosd version: 3.3.3.0.alpha compatible_version: 3.0.0.0
git: 640311cc169a62c84dbbc75654cc77734a4beaa4
gitOfInternal: 1cef5c5bec7407ab5449d68a0e17f81615d92fe8
build: Linux-x64 2024-09-03 15:34:48 +0800]
5个线程并发5次。
1、由于taosBenchmark建的子表ts一致，因此性能只记录一下下面4个sql的执行耗时：
sql1：select * from (select diff(ts,0) dt from stb partition by tbname) where dt!=ts步长;
sql2：select * from (select diff(ts,1) dt from stb partition by tbname) where dt!=ts步长;
sql3：select * from (select diff(ts,2) dt from stb partition by tbname) where dt!=ts步长;
sql4：select * from (select diff(ts,3) dt from stb partition by tbname) where dt!=ts步长.
2、schema一共10列（1列ts，3列数据列，3列浮点列，1列字符串类型，1列bool，1列tag）。
- a：固定子表场景数据集 。固定1亿数量，10w子表，vgroup不同进行组合(10、20、50）。
- b：固定vgroup场景数据集 。固定1亿数量，50个vgroup，子表数量不同进行组合（5w、10w、50w）。

## 9. 功能测试用例

| No. | 用例名称 | 用例描述 | 期望结果 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | 官网的行为修改说明 | https://docs.taosdata.com/taos-sql/function/#diff 按照Func Spec第4节行为说明修改 | 下面用例的测试结果和官网说明的一致 | 通过 |  |
| 2 | ignore_option=0 | 1、准备测试数据 2、diff（ts,0）验证 3、diff（数值类型,0）验证 4、diff（bool,0）验证 5、diff（非上述类型,0）验证 | 1、测试数据完整 2、结果中存在负值，存在null值 3、结果中存在负值，存在null值 4、结果中存在负值（true按1计算，false按0计算），存在null值 5、语法报错 | 通过 | 用例2-6针对普通表和超级表中没有重复时间戳对超级表。 |
| 3 | ignore_option=1 | 1、准备测试数据 2、diff（ts,1）验证 3、diff（数值类型,1）验证 4、diff（bool,1）验证 5、diff（非上述类型,1）验证 | 1、测试数据完整 2、结果中不存在负值，存在null值，且负值转换成null值 3、结果中不存在负值，存在null值，且负值转换成null值 4、结果中不存在负值（true按1计算，false按0计算），存在null值，且负值转换成null值 5、语法报错 | 通过 |  |
| 4 | ignore_option=2 | 1、准备测试数据 2、diff（ts,2）验证 3、diff（数值类型,2）验证 4、diff（bool,2）验证 5、diff（非上述类型,2）验证 | 1、测试数据完整 2、结果中存在负值，但不存在null值 3、结果中存在负值，但不存在null值 4、结果中存在负值（true按1计算，false按0计算），但不存在null值 5、语法报错 | 通过 |  |
| 5 | ignore_option=3 | 1、准备测试数据 2、diff（ts,3）验证 3、diff（数值类型,3）验证 4、diff（bool,3）验证 5、diff（非上述类型,3）验证 | 1、测试数据完整 2、结果中不存在负值，且不存在null值 3、结果中不存在负值，且不存在null值 4、结果中不存在负值（true按1计算，false按0计算），且不存在null值 5、语法报错 | 通过 |  |
| 6 | 多个diff组合 | 1、准备测试数据 2、diff（ts,0）,diff（数值类型,0）,diff（bool,0）验证 3、diff（ts,1）,diff（数值类型,1）,diff（bool,1）验证 4、diff（ts,2）,diff（数值类型,2）,diff（bool,2）验证 5、diff（ts,3）,diff（数值类型,3）,diff（bool,3）验证 | 1、测试数据完整 2、结果中存在负值，存在null值 3、结果中不存在负值，存在null值，且负值转换成null值 4、结果中存在负值，但不存在null值；如果diff结果均为null 时，该行从结果集剔除；如果column 有值时，且diff结果均为null 时，该行也从结果集剔除 5、结果中不存在负值，且不存在null值；如果diff结果均为null 时，该行从结果集剔除；如果column 有值时，且diff结果均为null 时，该行也从结果集剔除 | 通过 | diff组合 测试时还会测试一下ignore_option=0，1，2，3在一起的组合，属于本次新增的组合 |
| 7 | 超级表带重复时间戳 | 1、准备测试数据 2、diff（ts,0-3）验证 3、diff（数值类型,0-3）验证 4、diff（bool,0-3）验证 5、diff（非上述类型,0-3）验证 | 1、测试数据完整 2、Duplicate timestamps not allowed 3、Duplicate timestamps not allowed 4、Duplicate timestamps not allowed 5、Duplicate timestamps not allowed | 通过 |  |
| 8 | 超级表带重复时间戳+partition by tbname 切分ignore_option=0 | 1、准备测试数据 2、diff（ts,0）partition by tbname验证 3、diff（数值类型,0）partition by tbname验证 4、diff（bool,0）partition by tbname验证 5、diff（非上述类型,0）partition by tbname验证 | 1、测试数据完整 2、结果中存在负值，存在null值 3、结果中存在负值，存在null值 4、结果中存在负值（true按1计算，false按0计算），存在null值 5、语法报错 | 通过 | 用例8-12针对超级表中带有重复时间戳，需要对tbname进行切分验证。 |
| 9 | ignore_option=1 | 1、准备测试数据 2、diff（ts,1）partition by tbname验证 3、diff（数值类型,1）partition by tbname验证 4、diff（bool,1）partition by tbname验证 5、diff（非上述类型,1）partition by tbname验证 | 1、测试数据完整 2、结果中不存在负值，存在null值，且负值转换成null值 3、结果中不存在负值，存在null值，且负值转换成null值 4、结果中不存在负值（true按1计算，false按0计算），存在null值，且负值转换成null值 5、语法报错 | 通过 |  |
| 10 | ignore_option=2 | 1、准备测试数据 2、diff（ts,2）partition by tbname验证 3、diff（数值类型,2）partition by tbname验证 4、diff（bool,2）partition by tbname验证 5、diff（非上述类型,2）partition by tbname验证 | 1、测试数据完整 2、结果中存在负值，但不存在null值 3、结果中存在负值，但不存在null值 4、结果中存在负值（true按1计算，false按0计算），但不存在null值 5、语法报错 | 通过 |  |
| 11 | ignore_option=3 | 1、准备测试数据 2、diff（ts,3）partition by tbname验证 3、diff（数值类型,3）partition by tbname验证 4、diff（bool,3）partition by tbname验证 5、diff（非上述类型,3）partition by tbname验证 | 1、测试数据完整 2、结果中不存在负值，且不存在null值 3、结果中不存在负值，且不存在null值 4、结果中不存在负值（true按1计算，false按0计算），且不存在null值 5、语法报错 | 通过 |  |
| 12 | 多个diff组合 | 1、准备测试数据 2、diff（ts,0）,diff（数值类型,0）,diff（bool,0）partition by tbname验证 3、diff（ts,1）,diff（数值类型,1）,diff（bool,1）partition by tbname验证 4、diff（ts,2）,diff（数值类型,2）,diff（bool,2）partition by tbname验证 5、diff（ts,3）,diff（数值类型,3）,diff（bool,3）partition by tbname验证 | 1、测试数据完整 2、结果中存在负值，存在null值 3、结果中不存在负值，存在null值，且负值转换成null值 4、结果中存在负值，但不存在null值；如果diff结果均为null 时，该行从结果集剔除；如果column 有值时，且diff结果均为null 时，该行也从结果集剔除 5、结果中不存在负值，且不存在null值；如果diff结果均为null 时，该行从结果集剔除；如果column 有值时，且diff结果均为null 时，该行也从结果集剔除 | 通过 | diff组合 |
|  |  |  |  |  |  |

## 10、性能测试用例

测试数据集见第八节性能测试数据集中a、b数据集信息。

#### 9.0.1 a： 固定子表场景数据集场景（数据集a）

1亿数据，10w字表，分布在不同vgroups。
因为旧版本并发时会crash，所以新、旧版本只记录运行一次的耗时作为对比。

| 子表固定，vgroup变化 | sql. 5个并发查询5次 | max(ms) | min(ms) | avg (ms) | p95 (ms) | p99 (ms) | 备注，单次shell执行耗时 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| vgroup=10 diff_vgroup_10 | select * from (select diff(ts,0) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 119.987445s | 115.315311s | 116.572702s | 116.551456s | 119.987445s | 25.277641s |
|  | select * from (select diff(ts,0) dt from stb partition by tbname) where dt!=ts步长;-旧版本 | 忽略 | 忽略 | 忽略 | 忽略 | 忽略 | 21.591658s |
|  | select * from (select diff(ts,1) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 116.366251s | 115.307264s | 116.017163s | 116.354395s | 116.366251s | 26.179074s |
|  | select * from (select diff(ts,1) dt from stb partition by tbname) where dt!=ts步长;-旧版本 | 忽略 | 忽略 | 忽略 | 忽略 | 忽略 | 18.142290s |
|  | select * from (select diff(ts,2) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 116.755702s | 115.413771s | 115.829484s | 116.005619s | 116.755702s | 24.823488s |
|  | select * from (select diff(ts,3) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 117.168855s | 115.500359s | 116.114157s | 116.011863s | 117.168855s | 24.894924s |
| vgroup=20 diff_vgroup_20 | select * from (select diff(ts,0) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 116.063593s | 114.004623s | 115.100033s | 115.776222s | 116.063593s | 24.856891s |
|  | select * from (select diff(ts,0) dt from stb partition by tbname) where dt!=ts步长;-旧版本 | 忽略 | 忽略 | 忽略 | 忽略 | 忽略 | 19.311520s |
|  | select * from (select diff(ts,1) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 116.847480s | 115.266793s | 115.555788s | 116.227089s | 116.847480s | 25.918921s |
|  | select * from (select diff(ts,1) dt from stb partition by tbname) where dt!=ts步长;-旧版本 | 忽略 | 忽略 | 忽略 | 忽略 | 忽略 | 17.292603s |
|  | select * from (select diff(ts,2) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 115.670036s | 114.192920s | 115.319301s | 115.629298s | 115.670036s | 25.225302s |
|  | select * from (select diff(ts,3) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 116.459213s | 115.672076s | 116.131472s | 116.042279s | 116.459213s | 25.054426s |
| vgroup=50 diff_vgroup_50 | select * from (select diff(ts,0) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 90.599630s | 88.539993s | 89.314623s | 89.348056s | 90.599630s | 22.075898s |
|  | select * from (select diff(ts,0) dt from stb partition by tbname) where dt!=ts步长;-旧版本 | 忽略 | 忽略 | 忽略 | 忽略 | 忽略 | 14.306539s |
|  | select * from (select diff(ts,1) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 90.329645s | 88.445570s | 89.779564s | 90.090874s | 90.329645s | 20.974270s |
|  | select * from (select diff(ts,1) dt from stb partition by tbname) where dt!=ts步长;-旧版本 | 忽略 | 忽略 | 忽略 | 忽略 | 忽略 | 16.416307s |
|  | select * from (select diff(ts,2) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 89.871507s | 89.175763s | 89.401994s | 89.116545s | 89.871507s | 21.264263s |
|  | select * from (select diff(ts,3) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 89.911485s | 89.001368s | 89.537883s | 89.506818s | 89.911485s | 22.054003s |

#### 9.0.2 b：固定vgroup场景数据集场景（数据集b）

1亿数据，1w子表～50w子表，分布在固定数量的vgroups中。

| vgroup=50， 子表变化 | sql | 单次耗时seconds | max(ms) | min(ms) | avg (ms) | p95 (ms) | p99 (ms) | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 子表数量=5w diff_vgroup_50_5wtable | select * from (select diff(ts,0) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 19.928115s[单次] | 70.426030s | 68.805771s | 69.381962s | 70.405832s | 70.426030s |
|  | select * from (select diff(ts,0) dt from stb partition by tbname) where dt!=ts步长;-旧版本 | 14.013530s[单次] | 忽略 | 忽略 | 忽略 | 忽略 | 忽略 |
|  | select * from (select diff(ts,1) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 19.617958s[单次] | 70.639644s | 69.638128s | 69.805040s | 69.924281s | 70.639644s |
|  | select * from (select diff(ts,1) dt from stb partition by tbname) where dt!=ts步长;-旧版本 | 13.053929s[单次] | 忽略 | 忽略 | 忽略 | 忽略 | 忽略 |
|  | select * from (select diff(ts,2) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 19.248273s[单次] | 70.935111s | 68.260235s | 69.761035s | 70.765497s | 70.935111s |
|  | select * from (select diff(ts,3) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 20.145031s[单次] | 70.255261s | 68.101507s | 69.726219s | 70.252827s | 70.255261s |
| 子表数量=10w diff_vgroup_50 | select * from (select diff(ts,0) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 22.075898s[单次] | 90.599630s | 88.539993s | 89.314623s | 89.348056s | 90.599630s |
|  | select * from (select diff(ts,0) dt from stb partition by tbname) where dt!=ts步长;-旧版本 | 14.306539s[单次] | 忽略 | 忽略 | 忽略 | 忽略 | 忽略 |
|  | select * from (select diff(ts,1) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 20.974270s[单次] | 90.329645s | 88.445570s | 89.779564s | 90.090874s | 90.329645s |
|  | select * from (select diff(ts,1) dt from stb partition by tbname) where dt!=ts步长;-旧版本 | 16.416307s[单次] | 忽略 | 忽略 | 忽略 | 忽略 | 忽略 |
|  | select * from (select diff(ts,2) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 21.264263s[单次] | 89.871507s | 89.175763s | 89.401994s | 89.116545s | 89.871507s |
|  | select * from (select diff(ts,3) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 22.054003s[单次] | 89.911485s | 89.001368s | 89.537883s | 89.506818s | 89.911485s |
| 子表数量=50w diff_vgroup_50_50wtable | select * from (select diff(ts,0) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 75.112528s[单次] | 381.156837s | 371.607656s | 377.608927s | 381.011513s | 381.156837s |
|  | select * from (select diff(ts,0) dt from stb partition by tbname) where dt!=ts步长;-旧版本 | 53.932999s[单次] | 忽略 | 忽略 | 忽略 | 忽略 | 忽略 |
|  | select * from (select diff(ts,1) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 76.535811s[单次] | 393.220052s | 381.551853s | 384.580982s | 386.773829s | 393.220052s |
|  | select * from (select diff(ts,1) dt from stb partition by tbname) where dt!=ts步长;-旧版本 | 54.499512s[单次] | 忽略 | 忽略 | 忽略 | 忽略 | 忽略 |
|  | select * from (select diff(ts,2) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 76.295682s[单次] | 391.254006s | 382.662114s | 386.452637s | 388.817142s | 391.254006s |
|  | select * from (select diff(ts,3) dt from stb partition by tbname) where dt!=ts步长;-新版本 | 75.712069s[单次] | 393.585646s | 381.164733s | 386.117778s | 388.834604s | 393.585646s |

## 10. 问题

| Id | Title | Commen |
| --- | --- | --- |
|  |  |  |
|  |  |  |

## 11. 测试计划 

2024-07、09

## 12. 测试备忘 

无

## 13. 参考文档

[diff 函数输出优化](https://taosdata.feishu.cn/wiki/M8i4w1zfritngjkEzlqcWeeXnAc)
