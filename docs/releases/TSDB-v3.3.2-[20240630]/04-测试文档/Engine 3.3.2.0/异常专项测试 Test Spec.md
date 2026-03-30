# 异常专项测试 Test Spec

## 一、测试目标

  测试的需求文档：[当前客户成功面临的挑战和举措](https://taosdata.feishu.cn/wiki/Eit4wdGLciwMzikhkoScvJXtnng) 的加强内部测试：异常专项测试。
和 https://jira.taosdata.com:18080/browse/TD-29927?filter=-1
【复盘问题  [TS-4688](https://jira.taosdata.com:18080/browse/TS-4688) 及 [TS-4737](https://jira.taosdata.com:18080/browse/TS-4737), 都是因为程序出现了异常，异常处理逻辑不正确导致的严重问题。还有 [TS-4730](https://jira.taosdata.com:18080/browse/TS-4730) , -[TS-4715](https://jira.taosdata.com:18080/browse/TS-4715)- , 也是异常情况出现的问题，专门针对异常还没有做过一次专项测试，所以这里需要开展一次比较深的全面的对各种的测试】

## 二、变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024-05-30 | 0.1 | guoxy | 初稿 |
| 2024-06-04 | 0.2 | guoxy | 优化文档结构，修改测试范围和重点，减少调研性东西，增加测试用例覆盖面，修改描述不清楚的地方。 |

## 三、测试资源及环境

   测试平台：Linux x64
   测试资源：192.168.1.44（普通版）、64（asan版本）
   测试版本：3.3.1.0

## 四：测试范围及重点

 本测试主要对相关功能的重点测试，包含操作系统部分和taosd两部分。
 故障模拟脚本和taosd脚本两者相互独立，独自运行，互不干涉。
  a：系统异常和资源受限异常：
原始需求：主要包括网络 和 磁盘IO 出现异常，读写连接打开失败类。
       这类可以通过一些第三方工具模拟，目前调研过结合TDengine可以叠加的主要是
        CPU：模拟CPU满负荷、部分CPU满负荷、全部CPU达到部分负荷。
        内存：模拟内存占满，模拟内存占比，模拟内存不停分配释放。
        IO：模拟磁盘高读、模拟磁盘高写。
        磁盘：模拟磁盘空间占用，/tmp空间占用。
        网络：模拟网络延迟、网络丢包。
        进程：模拟杀除进程、杀除端口号、暂停进程。
  b：软件异常：
模拟taosd、taosc、taosadepter等进程重启，进程杀除可以通过上面完成，重新启动需要在此完成。
但测试的taosd内部需要叠加一部分业务，具体业务包括并不局限于 数据写入、数据删除、数据查询、长查询、增删mnode、增删dnode、频繁建库建表删库删表、schema修改、系统表查询、副本变更、compact、split、flush、balance vgroup、redistribute vgroup、kill trans、kill query、kill connect、缓存更新和查询、订阅等。

## 五、测试运行平台

该测试叠加日常运行的测试环境
- 日常全量测试环境数据，因为全量里面比较容易并发而提高软件的高负载，且很多基础业务功能已经实现。
- 日常长稳测试环境数据。

## 六、测试思路和用例设计

taosd 内部进行常规业务的并发，具体的业务搭配合适的故障类型。
故障类型选择见第四章，分别是：CPU、内存、IO、磁盘、网络、进程。
        1、其中CPU和内存为基础测试项，所有业务都搭配。
        2、网络在多台机器中构建的集群中作为基础测试项使用。
        3、重点搭配故障类型列中根据业务选择其余3组故障叠加使用。
        4、目前黄色的部分是查询全量中未包含的，需要新增脚本覆盖，部分业务中缺失的在用例中单独补充。
5、同时全量拆分成两个用例列表，正常运行的测试用例列表，继续保证数据正确性。正常用例加上4中补充的异常测试列表，允许部分用例失败（比如connect、query、trans等被kill了而用例失败了），但不允许故障恢复后，taosd、taosc、taosadepeter等异常无法恢复，taos连接之后mnode、dnode、query、arbgroup、缓存错误等异常无法恢复，无crash和内存泄漏等。

| 业务 | 命令 | 重点搭配故障类型 | 脚本测试重点及需要补充测试点 | 备注 |
| --- | --- | --- | --- | --- |
| 增加库 | Create db | 进程 | 全量已包含并发创建20个库 |  |
| 删除库 | Drop db | 进程 | 全量已包含单个删除库 |  |
| 插入数据 | Insert data | IO、磁盘、进程 | 全量已包含并发对20个库数据写入，但要增加多个benchmark并发写入的场景 |  |
| 删除数据 | Delete data | IO、磁盘、进程 | 全量已包含单个库数据删除，需要增加多个大数据量删除的场景 |  |
| 创建超级表 | Create stable | IO、进程 | 全量已包含大批量schema建表 |  |
| 删除超级表 | Drop stable | IO、进程 | 全量已包含删除单个超级表，但缺少连续删除很大量超级表 |  |
| 创建子表 | Create table | 进程 | 全量已包含大批量schema建表 |  |
| 删除子表 | Drop table | 进程 | 全量已包含删除单个子表，但缺少连续删除很大量子表 |  |
| 修改schema | Alter add column | IO、进程 | 全量已包含批量add column |  |
|  | Alter drop column | IO、进程 | 全量已包含批量drop column |  |
|  | Alter add tag | IO、进程 | 全量已包含批量add tag |  |
|  | Alter drop tag | IO、进程 | 全量已包含批量drop tag |  |
| 系统表查询 | information_schema | IO、进程 | 全量已包含并发查询系统表 |  |
|  | performance_schema | IO、进程 | 全量已包含并发查询系统表 |  |
| 测试库query | Query sql in db | IO、磁盘、进程 | 全量已包含并发查询20个数据库 |  |
| dnode | Create dnode | IO、磁盘、进程 | 全量在初始化环境创建 |  |
|  | Drop dnode | IO、磁盘、进程 | 全量在结束后销毁 |  |
| mnode | Create mnode | IO、磁盘、进程 | 全量在初始化环境创建 |  |
|  | Drop mnode | IO、磁盘、进程 | 全量在结束后销毁 |  |
| replica | Alter db replica 1 | IO、磁盘、进程 | 全量支持并发创建20个单副本库 |  |
|  | Alter db replica 2 | IO、磁盘、进程 | 全量支持并发创建20个双副本库 |  |
|  | Alter db replica 3 | IO、磁盘、进程 | 全量支持并发创建20个三副本库 |  |
| compact | Compact db | IO、磁盘、进程 | 全量支持并发compact 20个库 |  |
| flush | Flush db | IO、磁盘、进程 | 全量支持并发flush 20个库 |  |
| split | Split vgroup | IO、磁盘、进程 | 全量缺失，正确性测试场景中包含，移植到全量中 |  |
| balance | Balance vgroup | IO、磁盘、进程 | 全量缺失，正确性测试场景中包含，移植到全量中 |  |
| redistribute | redistribute vgroup | IO、磁盘、进程 | 全量缺失，正确性测试场景中包含，移植到全量中 |  |
| transactions | Kill transaction id | IO、进程 | 全量缺失，需增加在系统表查询后连续kill transaction的场景 |  |
| query | Kill query id | 进程 | 全量缺失，需增加在系统表查询后连续kill query的场景 |  |
| connect | Kill connect | 进程 | 全量缺失，需增加在系统表查询后连续kill connect的场景 |  |
| cachemodel | Alter cachemodel | 进程 | 全量支持并发修改20个测试库 |  |
| 缓存写入、更新 | Insert data、delete data、updata data | IO、进程 | 全量支持并发20个测试库的缓存建立更新等，但缺少多数据量大数据量并发建立缓存的场景 |  |
| 缓存查询 | Select last(*)\last_row(*) from **** | IO、进程 | 全量支持并发8个测试库的缓存查询等，但缺少多数据量大数据量并发查询缓存的场景 |  |
| 订阅 | topic | IO、磁盘、进程 | 全量支持并发5个topic订阅 |  |
| index | Create index | IO、磁盘、进程 | 全量支持并发create 20个库index |  |
|  | Drop index | IO、磁盘、进程 | 全量支持删除单个库index |  |


## 七、Jira

此feature相关的所有Jira, 标题中应包含统一的标签: TD-29927
此项任务已经开始一段时间了，发现了一些问题，部分已修复。
<!-- Unsupported block type: 999 -->

## 八、测试计划 

作为长稳和全量的一部分，长期运行。

## 九、参考文档 

目前已调研了一些测试工具，分别是stress、stress-ng、ChaosBlade、ulimit等。具体使用方法，可以参考文档：[异常专项测试工具调研](https://taosdata.feishu.cn/wiki/QCbiwSTakiGTCakH4GActZeTnEh)
