# TDengine 双副本 Test Spec 

## 一、测试目标

  测试的需求文档：[TDengine 双副本](https://taosdata.feishu.cn/wiki/CTSLwLgcLitcGlkAh21cnY1ln0g)
  主要目标是TDengine要支持双副本。

## 二、变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024-03-01 | 0.1 | guoxy |  |
| 2024-03-05 | 0.2 | guoxy | 修改测试用例，增加测试场景 |
| 2024-03-06 | 0.3 | guoxy | 合并测试用例，增加测试步骤，将以前的功能测试用中的7、8、9、10、11和可靠性8、9、10合并到功能用例7、8、9中，修改了功能测试用例12 |
| 2024-04-28 | 0.4 | guoxy | 补充性能测试结果 |
| 2024-04-29 | 0.5 | guoxy | 调整布局，补充缺失的测试结论等 |

## 三、测试结论

 1、功能正常。
 2、双副本的空间占用，是单副本空间占用的约2倍，是三副本空间占用的约2/3，基本达到节省空间约1/3的需求。
 3、通过配置mnode的vgroup=0，可以显著降低mnode的资源占用情况，而将读写集中到双副本所在的dnode上，满足减少机器性能配置的需求。
 4、大规格的性能上略有不足，具体实例为：创建的数据库有很多个vgroup，然后某个dnode出现故障等异常后，会发出大量的mnd-write请求，有可能造成队列的积压，具体见[TD-29713](https://jira.taosdata.com:18080/browse/TD-29713?filter=-2)，在封板前也进行了多轮的优化，有所改善，但还未彻底解决，所以在双副本的库上应该尽量进行减少vgroup的配置。由于在后面版本中还会进行后续优化，因此新开个jira进行跟踪，见[TD-29879](https://jira.taosdata.com:18080/browse/TD-29879?filter=-2)。
整理测试结论：通过

## 四、开发质量报告

结论：本特性/优化的开发质量是（**优，**良，一般，差，很差）

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 | 0 |
| 基础测试用例不通过 | 1 |
| Bug 总数 | 19 |
| 严重 Bug 总数 | 3 |

## 五、已知问题和限制

这里用于记录产品使用上的一些限制，包括不支持的场景等，以及在发版时没有解决的minor issues.
- 双副本的vgroups尽量配置少些，避免节点出问题后的请求过多
- 

## 六、测试资源及环境

   测试平台：Linux x64
   测试资源：192.168.1.43（普通版）、64（asan版本）、192.168.0.209（长稳和性能）

## 七：测试范围及重点

 本测试主要对新增的双副本功能，包含
  a：功能测试、基本功能测试
  b：可靠性测试、双副本的健壮性
  c：兼容性测试、1: 新增的功能不影响原有 TDengine 的功能
                               2:删除replica=2的数据库后，可以回退到上一个版本
此次测试的重点在功能和可靠性测试两部分。
各个状态的变化参考此状态机：
<grid cols="2">
  <column width="50">
    <add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"---\ntitle: Vgroup state diagram (follower crash)\n---\nstateDiagram-v2\n    init --\u003e leader,follower\n    leader,follower --\u003e leader,offline: follower crash\n    leader,offline --\u003e assigned,offline: arb assigne\n    assigned,offline --\u003e assigned,follower: vnode restart\n    assigned,follower --\u003e leader,follower: vgroup resync\n\n","theme":"default","view":"chart"}"/>

  </column>
  <column width="50">
    <add-ons component-id="" component-type-id="blk_631fefbbae02400430b8f9f4" record="{"data":"---\ntitle: Vgroup state diagram (leader crash)\n---\nstateDiagram-v2\n    init --\u003e leader,follower\n    leader,follower --\u003e follower,offline: leader crash\n    follower,offline --\u003e candicate,offline: vnode electe\n    candicate,offline --\u003e assigned,offline: arb assigne\n    assigned,offline --\u003e assigned,follower: vnode restart\n    assigned,follower --\u003e leader,follower: vgroup resync\n\n","theme":"default","view":"chart"}"/>

  </column>
</grid>


## 八、测试数据 (Optional)

这里用于描述性能、稳定性测试时的数据准备工作，包括但不局限于：
- field的数量、类型：默认taosBenchmark插入json的数量
- tag的数量、类型：默认taosBenchmark插入json的数量
- 存储空间占用数据量的大小：一亿、五亿和十亿数据量的空间占用，测试见第九节用例13.
- 性能测试数据规格：一万子表*1w数据，replica=2，vgroups=1，搭配不同的客户端线程，测试见第十节。

## 九、测试用例


| 分类 | 用例编号 | 测试场景 | 测试内容/步骤 | 预期 | 结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 【基础用例】 | 单 mnode 创建双副本数据库 | 1、Create db replica 2 2、除了replica 2外，搭配其他建库的参数，包括并不限于cachesize、cachemodel、maxrows、vgroups等 | 1、创建成功，检查项如下： a、系统表中replica=2 b、和show create database 信息一致 c、查看show vgroups状态，重点是各个节点的状态 d、查看show arbgroups状态，重点是is_sync【初始状态=0同步未成功，大约(2*arbCheckSyncIntervalSec)时状态=1是同步成功】、assigined_dnode状态。 e、taosd状态正常，无内存泄漏等。 2、创建成功，检查项同上。 | 通过 | 备注或报BUG 的JIRA号 [TD-29180](https://jira.taosdata.com:18080/browse/TD-29180?filter=-2) TD-29189 |
| 2【基础用例】 | 单 mnode 删除双副本数据库 | 1、Drop database db | 1、删除成功，检查项如下： a、系统表中无该库信息 b、show create database 无该库信息 c、查看show vgroups，无该库信息 d、查看show arbgroups，无该库信息 e、taosd状态正常，无内存泄漏等。 | 通过 |  |
| 3【基础用例】 | 单 mnode 创建和删除同名的单、三、双副本库 | 1、创建单副本同名库 2、删除单副本同名库 3、创建双副本同名库 4、删除双副本同名库 5、创建三副本同名库 6、删除三副本同名库 | 1、结果同功能用例1，区别是副本数的变化 2、结果同功能用例2 3、结果同功能用例1，区别是副本数的变化 4、结果同功能用例2 5、结果同功能用例1，区别是副本数的变化 6、结果同功能用例2 | 通过 | [TD-29187](https://jira.taosdata.com:18080/browse/TD-29187?filter=-2) |
| 4、【基础用例】 | 单 mnode 数据写入、查询 | 1、对双副本数据库进行数据写入 2、进行基本的数据查询 | 1、数据写入成功，数据写入量=count结果 2、进行max、min、top、last、first、last_row、sum、diff等函数的验证数据返回正确。 | 通过 |  |
| 5.1【基础用例】 | mnode=3，运行用例1 | 同用例1 | 同用例1 | 通过 |  |
| 5.2 | mnode=3，运行用例2 | 同用例2 | 同用例2 | 通过 |  |
| 5.3 | mnode=3，运行用例3 | 同用例3 | 同用例3 | 通过 |  |
| 5.4 | mnode=3，运行用例4 | 同用例4 | 同用例4 | 通过 |  |
| 6、【基础用例】 | 模拟vgroup中，vnode状态是leader+follower | 1、运行用例1【初始状态为 leader+follower】 2、运行用例4 3、运行用例2 | 1、预期结果同用例1 2、预期结果同用例4 3、预期结果同用例2 | 通过 |  |
| 7、【基础用例】 | 模拟follower节点故障，验证各节点的状态 | 1、运行用例1【初始状态为leader+follower，且isSync =1】 2、运行用例4 3、模拟follower节点down 4、运行用例4 5、在 arbSetAssignedTimeoutSec*2 内，arbitrator感知 6、运行用例2 7、模拟follower节点up 8、运行用例4 9、isSync 显示达成同步 10、运行用例4 11、运行用例2 | 1、预期结果同用例1 2、预期结果同用例4 3、状态变成【leader+offline，isSync =1】 4、预期结果参考用例4【数据不能写入，但查询正常】 5、状态变成【assigned+offline，isSync 最终状态为0】 6、无法删除 db 7、状态变成【assigned+follower，isSync =0】 8、预期结果同用例4 9、状态变成【leader+follower，isSync 最终状态为 1】 10、预期结果同用例4 11、预期结果同用例2 | 通过 |  |
| 8、【基础用例】 | 模拟leader节点故障，验证各节点的状态 | 1、运行用例1【初始状态为leader+follower，且isSync =1】 2、运行用例4 3、模拟leader节点down 4、运行用例4 5、follower 自动执行 elect 6、在 arbSetAssignedTimeoutSec*2 内，arbitrator感知 7、运行用例2 8、模拟leader节点up 9、运行用例4 10、isSync 显示达成同步 11、运行用例4 12、运行用例2 | 1、预期结果同用例1 2、预期结果同用例4 3、状态变成【follower+offline，isSync =1】 4、预期结果参考用例4【数据不能写入，不能查询】 5、状态变成【candicate+offline】 6、状态变成【assigned+offline，isSync 最终状态为0】 7、无法删除 db 8、状态变成【assigned+follower，isSync =0】 9、预期结果同用例4 10、状态变成【leader+follower，isSync 最终状态为 1】 11、预期结果同用例4 12、预期结果同用例2 | 通过 |  |
| 9、【基础用例】 | 模拟leader、follower节点不同时间点故障，验证各节点的状态 | 1、运行用例1【初始状态为leader+follower，且isSync =1】 2、运行用例4 3、模拟follower节点down 4、长时间运行用例4，确保leader的数据比follower数据多 5、模拟follower节点up 6、模拟leader节点down 7、运行用例4 8、模拟leader节点up 9、运行用例4 10、运行用例2 | 1、预期结果同用例1 2、预期结果同用例4 3、状态变成【leader+offline，isSync =1】，然后在变成【assigned+offline，isSync 最终状态为0】 4、预期结果同用例4 5、状态变成【assigned+follower，isSync =0】 6、状态变成【follower+offline，isSync =0】 7、预期结果参考用例4【数据不能写入，不能查询】 8、状态变成【assigned+follower，isSync =0】 9、预期结果同用例4 10、预期结果同用例2 | 通过 |  |
|  | 10 | compact db | 1、对各vgroups状态正常的数据库进行compact 2、对各vgroups状态不正常的数据库进行compact | 1、compact正常，可以看到show compacts信息，可以查看 show compact id的进度，compact完成后，show compacts消失。 2、compact命令可以下发，在vgroups状态未恢复正常时，可以一直看到show compacts信息不消失，等vgroups状态完全恢复后，状态异常的节点继续进行compact，完成后，show compacts消失。 | 未通过 | TD-29713 |
|  | 11 | Split vgroup | 1、进行split vgroup | 1、当前版本的db配置双副本时暂不支持，其余不受影响 | 通过 |  |
|  | 12 | Redistribute vgroup | 1、进行redistribute vgroup | 1、当前版本的db配置双副本时暂不支持，其余不受影响 | 通过 |  |
|  | 13、【基础用例】 | 存储空间验证 | 1、创建单副本、双副本、三副本数据库，写入同样的数据 2、对上述数据库进行compact | 1、存储空间占用。 3副本库/2副本库 约等于 3/2 3副本库/单副本库 约等于 3/1 2、compact后空间占用同上 | 通过 | 5亿数据量 单：3.4G 双：7.6G 三：11G |
|  |  |  |  |  |  |  |
| 可靠性测试 | 1、【基础用例】 【用例1-2基于单mnode】 | 多次创建+删除双副本数据库 | 多次重复执行功能用例1+2 | 结果同功能用例的 1、2、3、4 | 通过 | TD-29190 TD-29533 TD-29480 TD-29472 |
|  | 2、【基础用例】 | 创建和删除多个混合库，包括单副本、双副本、三副本 | 多次叠加执行功能用例1+2+3 | 结果同功能用例的 1、2、3、4 | 通过 | TD-29284 TD-29301 |
|  | 3、【基础用例】 【下面用例基于三mnode】 | 多次创建+删除双副本数据库 | 多次重复执行功能用例1+2 | 结果同功能用例的 1、2、3、4 | 通过 | TD-29273 |
|  | 4、【基础用例】 | 创建和删除多个混合库，包括单副本、双副本、三副本 | 多次叠加执行功能用例1+2+3 | 结果同功能用例的 1、2、3、4 | 通过 | TD-29253 TD-29330 |
|  | 5、【基础用例】 | 模拟mode切主 | 多次启停各个vnode，进行mnode的切主，验证验证功能测试用例4 | 结果同功能用例的4 | 通过 | TD-29206 TD-29272 |
|  | 6 | 多次增删mnode | 多次增加、删除mnode，验证功能测试用例4 | 结果同功能用例的4 | 通过 |  |
|  | 7、【基础用例】 | 多次重启整个集群 | 多次启停部分节点、所有节点 | 结果同功能用例的 1、2、3、4 | 通过 | TD-29223 TD-29603 |
|  |  |  |  |  |  |  |
| 兼容性测试 | 1、【基础用例】 | 新增的功能不影响原有 TDengine 的功能 | 1、验证CI用例 2、验证全量用例 | 1、CI用例正常 2、全量用例正常 | 通过 | TD-29556 |
|  | 2 | 删除replica=2的数据库 | 1、删除replica=2的数据库后，回退到上一个发版版本，旧数据库能打开 | 此用例在3230和3240数据结构不发生变化时可以验证，如果发生变化，降级后可能出现打不开现象，属于正常。 | 通过 | 大版本变更，不支持回退 |



## 十、性能测试及结果

**测试背景：**
1.部署三节点集群，第一个节点 supportVnode 设置为 0，保证该节点没有任何 vnode
2.不在其他节点部署 mnode
3.创建单 vnode 双副本的数据库 ，一万子表*1w数据，replica=2，vgroups=1
4.使用 taosBenchmark 写入数据，选择几组写入线程数目作为不同场景来验证mnode的资源占用情况
**测试结论：**
按照产品规划，符合产品要求，即mnode资源占用低，双副本的leader节点资源占用最高，follower节点资源占用比leader节点资源占用低，可以满足减少mode所在设备的配置略低的需求。

| 客户端线程 | mnode-cpu | mnode-men | mnode-io | leader-cpu | leader-men | leader-io | follower-cpu | follower-men | follower-io |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 1.3% | 0.2% | 6.1k/s | 33.5% | 0.3-0.4% | 1.59M/s | 21% | 0.3-0.4% | 1.43M/s |
| 2 | 1.0% | 0.2% | 6.1k/s | 60% | 0.4% | 1.66M/s | 36% | 0.4% | 1.49M/s |
| 5 | 1.0% | 0.2% | 8.37k/s | 188% | 0.4% | 6.84M/s | 143% | 0.4% | 6.08M/s |
| 10 | 1.0% | 0.2% | 10.9k/s | 298% | 0.4% | 13.2M/s | 234% | 0.4% | 11.7M/s |
| 20 | 1.0% | 0.2% | 32.6k/s | 377% | 0.4% | 19.2M/s | 295% | 0.4% | 17.1M/s |
| 50 | 1.0% | 0.2% | 33.2k/s | 367% | 0.4% | 16.7M/s | 299% | 0.4% | 14.8M/s |
| 100 | 1.3% | 0.3% | 60.3k/s | 352% | 0.4% | 16.1M/s | 289% | 0.4% | 14.3M/s |
| 200 | 2.0% | 0.3% | 107k/s | 352% | 0.4% | 15.7M/s | 273% | 0.4% | 13.9M/s |
| 500 | 2.6% | 0.3% | 247k/s | 314% | 0.4% | 14.3M/s | 241% | 0.4% | 12.8M/s |



## 十一、问题(Optional)

这里用于记录需要讨论的问题：
- 暂无

## 十二、Jira

此feature相关的所有Jira, 标题中应包含统一的标签: replica2
<!-- Unsupported block type: 999 -->

## 十三、测试计划 (Optional)

2024.03 -- 2024.04，其中3月以功能为主，4月以性能和稳定性为主。

## 十四、测试备忘 (Optional)

这里用于记录测试过程中发现的，与产品行为相关的一些重要信息。
2024.03月。目前测试告一段落，主题功能运行正常。主要有些问题是大并发场景下，会出现事务很卡的问题，部分修复的代码还在main分支，需要和入3.0。等和入后4月份会继续进行大并发的测试。

## 十五、参考文档 (Optional)

这里用于添加对该需求测试有帮助的文档链接：
- [TDengine 双副本](https://taosdata.feishu.cn/wiki/CTSLwLgcLitcGlkAh21cnY1ln0g)
- [需求说明：双副本](https://taosdata.feishu.cn/wiki/SZFwwRR36ib9oTkOnTccDLBxnvb)
