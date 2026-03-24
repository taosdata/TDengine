# 授权机制优化（基础功能、taosd功能项）

### 1. 测试总结：

1. taosGrant工具功能正常，异常参数执行有清晰错误提示
2. 授权状态各状态切换正常，未授权默认参数、授权后参数正常，基础功能及可选功能正常
3. 更换节点场景授权状态切换到‘revoked’状态，重新授权后功能正常
4. 授权失效后，授权参数显示正常，各功能项正常
5. 停服务升级正常
综上，授权机制优化符合预期

### 2. 测试目标

Spec参考文档：[授权机制优化](https://taosdata.feishu.cn/wiki/OydKwSf1jidC04ki9V2c65NvnKe) 
需求说明：[需求说明：License 独立控制](https://taosdata.feishu.cn/wiki/SqccwbvkkibacFkvXi9c3TYOn6b)
[TD-27463](https://jira.taosdata.com:18080/browse/TD-27463?src=confmacro) [[grant] 独立控制可选产品功能的 license](https%3A%2F%2Fjira.taosdata.com%3A18080%2Fbrowse%2FTD-27463%3Fsrc%3Dconfmacro)
[TD-28247](https://jira.taosdata.com:18080/browse/TD-28247?src=confmacro) [[grant] 流计算和订阅功能支持授权过期行为](https%3A%2F%2Fjira.taosdata.com%3A18080%2Fbrowse%2FTD-28247%3Fsrc%3Dconfmacro)
当前授权机制存在的问题
1. taosd与taosx的授权码独立存在
2. 针对clusterid授权的集群，虽然减少了对集群中每台机器授权的重复性、复杂性，但存在复制集群的漏洞
授权机制新功能需求：
   - 采用单一授权码控制TDengine的基本功能和各项可选功能
本次优化增加了基础功能和可选功能，将taosd与taosx对应的功能项作为授权可选功能项，独立处理；针对复制集群的漏洞，增加了machine code运行检查机制，同时对机器cpu变更或节点变更的场景，增加了revoked状态，通过特殊授权码再次授权解除revoked状态，降低复制集群的概率；增加授权状态机，约束各个授权状态的转换条件，根据授权状态更灵活地应对多种授权场景。
本次测试的目标是保证新授权码生成工具、根据集群状态变化使用授权码的功能、兼容性、异常场景的容错性处理与Spec需求文档一致。

### 3. 变更历史

| 日期 | 版本 | 负责人 | 修改记录 |
| --- | --- | --- | --- |
| 2024-01-04 | 0.1 | Charles |  |
| 2024-01-05 | 0.2 | Charles | 修改报告格式，增加用例中关于授权状态（ungranted、 granted、revoked）的测试点描述 |
| 2024-01-15 | 0.3 | Charles | 更新测试范围及相关测试用例中的测试点，以json文件的方式生成授权码；授权状态的检查及授权失效后集群基础功能、可选功能的参数检查 |
| 2024-01-16 | 0.4 | Charles | 1. 修改兼容性升级用例，升级后集群状态变为未授权，时间为升级时间+7天，功能正常，需要生成授权码重新授权 1. 删除测试点“授权码的生成时间早于集群中上次激活的授权码的生成时间” |
| 2024-01-17 | 1.0 | Charles | 1. 更新测试点“Dnode machine code变更， 授权码回收状态” 1. 更新用例13、15 |

### 4. 测试范围

本次优化更新了taosGrant工具及授权检查机制，只涉及授权功能及交付团队使用流程的变更，对产品性能无影响，所以此次测试范围主要包括：
功能：
1. 授权码的生成与解析(taosGrant工具)：
  1. 
  - 生成授权码9、
    - 生成普通授权码
    - 生成特殊授权码
    - 指定基础功能项、可选功能项参数生成授权码
    - 以json文件的方式提供授权信息生成授权码
  - 查看解析结果
    - 授权码结果与指定参数值一致
    - 基础功能、可选功能参数默认值与Spec文档定义一致
    - 解析授权码失败
  - 生成授权码失败
    - 参数缺失导致生成授权码失败
    - 参数错误导致生成授权码失败
1. 授权码使用
  - 激活授权码 
    - 普通授权码、特殊授权码激活
    - 普通授权码、特殊授权码激活失败
    - 不检查硬件变更的授权码，支持集群复制
    - 授权码可用时间 3天
    - 维保服务时间
    - 激活授权码后，查看授权状态，基础功能项、可选功能项参数；授权项功能
    - 叠加授权场景，验证授权项合并规则
    - 授权码保存信息
    - 激活授权码失败
      - machne code不一致（防止集群复制）
      - 超出授权项指定值
      - 授权码未包括基础功能项
      - ~~授权码的生成时间早于集群中上次激活的授权码的生成时间~~
      - 可选功能项的到期时间大于基础功能的到期时间
  - 运行时检查
    - 基础功能检查（dnode数、测点数、cpu核数）
    - 授权项超出授权码中指定值（流的数、数据订阅数、视图的数量）
    - Dnode machine code变更， 授权码回收状态
      - 集群创建并生成cluster id时会记录首个dnode节点的机器码
      - 当有新的dnode加入集群时会保存机器码到历史列表中，保存数量与授权节点数量相同
      - 删除dnode不会删除历史列表中的已保存machine code
      - 当新增dnode数量超过授权指定数量时，会被识别为硬件变更（机器码不一致）进入revoked状态
      - 在定期检查中，如发现某个dnode的机器码不在集群中，会被识别为硬件变更，进入revoked状态
    - 集群授权状态检查（granted、ungranted、expired、revoked）
    - 集群扩容，增加mnode或dndoe节点
    - mnode副本变更
  - 授权码过期
    - 基础功能、可选功能的限制
    - 基础功能未到期，可选功能到期（流计算、数据订阅、多级存储、视图、审计日志）
    - 集群自动切换到授权码回收状态
    - 收回授权码
兼容性：
- 对已有授权集群的升级，包括taos.cfg方式授权和alter dnode 方式授权，集群升级后进入未授权状态，集群可用时间为升级时间+7天，基础功能、可选功能正常，需要生成授权码重新激活
- windows、arm平台的集群授权码激活基础流程
性能：N/A
稳定性：N/A
正确性：N/A

### 5. 已知问题

无

### 6. 测试环境

测试平台：Linux x64、ARM、Windows
测试资源：
- 192.168.1.35
- 192.168.1.61
- 192.168.1.44
Windows: 192.168.1.84
ARM64: 159.138.91.174

### 7. 测试用例

| 测试类型 | 测试场景 | 用例No. | 基础用例 | 测试用例名称 | 覆盖测试点 | 测试步骤 | 期望结果 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1 | 是 | 授权码功能项默认参数值 | 1. 生成普通授权码 1. 生成特殊授权码 1. 查看解析授权码 1. 授权功能项默认参数值 | 1. 使用taosGrant工具生成普通授权码，命令：./taosGrant_linux64 -k xxxxxx --basic un,un,un,un 1. 使用taosGrant工具生成特殊授权码，命令：./taosGrant_linux64 -k xxxxxx -m xxxx,xxxxxx --basic un,un,un,un 1. 指定维保时间通过taosGrant生成授权码 | 1. 授权码生成，验证可选功能项参数与Spec定义是否一致，授权码版本号信息 1. 授权码生成，验证可选功能项参数与Spec定义是否一致, 授权码版本号信息 1. 授权码生成，验证维保时间与授权码指定值一致 | Pass | 默认维保时间为集群创建时间 |
| 2 | 是 | 授权码功能项参数 | 1. 指定授权项参数生成普通、特殊授权码 1. 查看授权码中授权项参数与指定值一致（包括多级存储、流计算、数据订阅、视图、审计日志） 1. 通过json文件指定参数生成授权码 | 1. 使用taosGrant工具生成普通授权码，命令：./taosGrant_linux64 -k xxxxxx --basic 2024-01-01, 1000000，3， 40 1. 使用taosGrant工具生成普通授权码，命令：./taosGrant_linux64 -k xxxxxx --basic 2024-01-01, 1000000，3， 40 -stream 2024-01-01 100 1. 使用taosGrant工具生成普通授权码，命令：./taosGrant_linux64 -k xxxxxx --basic 2024-01-01, 1000000，3， 40 -stream 2024-01-01 100 -subscribe 2024-01-01 100 1. 使用taosGrant工具生成特殊授权码，命令：./taosGrant_linux64 -k xxxxxx -m xxxxx --basic 2024-01-01, 1000000，3， 40 -stream 2024-01-01 100 -subscribe 2024-01-01 100 -storage 2024-01-01 1. 使用taosGrant工具生成特殊授权码，命令：./taosGrant_linux64 -k xxxxxx -m xxxxxx --basic 2024-01-01, 1000000，3， 40 -stream 2024-01-01 100 -subscribe 2024-01-01 100 -storage 2024-01-01 -audit 2024-01-01 1. 使用taosGrant工具生成特殊授权码，命令：./taosGrant_linux64 -k xxxxxx -m xxxxxx --basic 2024-01-01, 1000000，3， 40 -service expire xxxxx -stream 2024-01-01 100 -subscribe 2024-01-01 100 -storage 2024-01-01 -audit 2024-01-01 1. 使用taosGrant工具生成特殊授权码，命令：./taosGrant_linux64 -k xxxxxx -m xxxxxx --basic 2024-01-01, 1000000，3， 40 -service expire xxxxx -stream 2024-01-01 100 -subscribe 2024-01-01 100 -storage 2024-01-01 -audit 2024-01-01 -view 2023-01-01 10 1. 通过json文件指定功能项参数生成以上授权码 1. 使用参数及json文件指定相同的授权项生成授权码 | 1. 授权码生成，解析结果中基础功能参数与指定参数值一致 1. 授权码生成，解析结果中流计算参数与指定参数值一致 1. 授权码生成，解析结果中数据订阅参数与指定参数值一致 1. 授权码生成，解析结果中多节存储参数与指定参数值一致 1. 授权码生成，解析结果中审计日志参数与指定参数值一致 1. 授权码生成，解析结果中维保服务时间参数与指定参数值一致 1. 授权码生成，解析结果中视图参数与指定参数值一致 1. 授权码生成，解析结果中参数值与指定参数值一致 1. 授权项的值与参数值一致，对于参数不存在的授权项取json文件指定授权项的值 | Pass |  |
| 解析授权码异常 | 3 | 否 | 解析历史授权码 | 1. clusterId与activeCode不一致 1. 不正确的activeCode | 1. 使用taosGrant工具解析历史授权码，clusterid与activeCode不一致 1. 使用taosGrant工具解析历史授权码，使用不正确的activeCode | 1. 解析授权码失败，提示错误“failed to parse uniq active code” 1. 解析授权码失败，提示错误“failed to parse uniq active code” | Pass |  |
| 4 | 否 | 功能项参数缺失 | 1. 授权码生成工具对授权项参数缺失容错性 | 1. 使用taosGrant工具生成普通授权码，命令：./taosGrant_linux64 -k xxxxxx --basic 2024-01-01, 1000000， 40 1. 使用taosGrant工具生成普通授权码，命令：./taosGrant_linux64 -k xxxxxx --stream 2024-01-01 8 1. 重复步骤2，subscribe，storage，audit, view、service expire进行测试 | 1. 生成授权码失败，提示参数不匹配错误“failed to parse param:--basic 2024-12-31,un,12 since invalid param num:3, should be: expireDay(e.g. 2023-12-30),timeseriesNum,dnodeNum,cpuCoreNum” 1. 生成授权码失败，提示参数不匹配错误“failed to parse param:--basic 2024-12-31,un,12 since invalid param num:3, should be: expireDay(e.g. 2023-12-30),timeseriesNum,dnodeNum,cpuCoreNum” 1. 生成授权码失败，提示参数不匹配错误“failed to parse param:--basic 2024-12-31,un,12 since invalid param num:3, should be: expireDay(e.g. 2023-12-30),timeseriesNum,dnodeNum,cpuCoreNum” | Pass | 基础功能默认参数 |
| 5 | 否 | 功能项参数异常 | 1. 授权码生成工具对授权项参数异常检查 | 1. 使用taosGrant工具生成普通授权码，命令：./taosGrant_linux64 -k xxxxxx --basic 2024-01-01, 1000000，3， 40 -stream 2024-01-02 10 8 1. 使用taosGrant工具生成普通授权码，指定可选功能时间大于基础功能时间 1. 重复步骤1，对subscribe、storage、audit、view、service expire进行异常参数测试 1. 指定维保服务时间大于基础功能时间 | 1. 生成授权码失败，提示参数异常错误“failed to parse param:--storage 2024-12-31,10 since invalid param num:2, should be: expireDay(e.g. 2023-12-30)，streamNum” 1. 生成授权码失败，提示错误“failed to generate activeCode since storageExpire 2025-12-31 larger than basicExpire” 1. 生成授权码失败，提示参数异常错误 1. 生成授权码失败，提示参数异常错误 | Fail | [TD-28570](https://jira.taosdata.com:18080/browse/TD-28570) |
| 6 | 是 | 普通授权码激活 | 1. 普通授权码激活单节点、多节点、单副本、多副本集群 1. 普通授权码激活基础功能、各授权项功能 1. 多次授权，授权信息合并 1. show grants、show grants full、show grants logs命令 1. 集群中有mnode或dnode状态为offline激活集群 | 1. 安装部署并启动单节点单副本集群 1. 获取集群id 1. 根据集群id，通过taosGrant工具生成普通授权码并激活集群 1. 验证集群授权信息与授权码中指定授权项的一致性，包括基础功能、流计算、数据订阅、多级存储、审计日志（sql：show cluster；show grants；show grants full；） 1. 安装部署并启动多节点单副本集群，获取集群id 1. 根据集群id，通过taosGrant工具生成只包括基础功能授权码 1. 多次生成普通授权码，修改基础功能、流计算、数据订阅、多级存储、审计日志的授权项的合理参数值，通过show grants logs查看历史信息 1. 安装部署多节点、多副本集群 1. 增加两个mnode节点（未超过基础功能dnode节点定义值），使用步骤普通授权码激活集群 1. 验证集群授权信息与授权码中指定授权项的一致性 1. 停掉集群中一个mnode或dnode节点 1. 使用普通授权码激活集群 1. 重新启动mnode或dnode节点 | 1. 集群启动成功，集群处于临时授权 1. 获取集群id正常 1. 集群被激活 1. 授权码发放时间，过期时间正确，集群授权信息与授权码中指定授权项的值一致 1. 集群启动成功，集群处于临时授权，获取集群id正常 1. 集群被激活，只有基础功能可用，其他授权项功能不可用 1. 授权码发放时间，过期时间正确，集群授权完成，授权信息更新且遵循授权信息合并规则，集群授权历史信息显示正常 1. 集群启动成功，集群处于临时授权 1. 集群被激活 1. 授权码发放时间，过期时间正确，集群授权信息与授权码中指定授权项的值一致 1. 一个mnode或dnode节点处于offline状态 1. 激活集群完成，集群状态为granted 1. 节点启动正常，集群功能正常 | Fail | [TD-28577](https://jira.taosdata.com:18080/browse/TD-28577) [TD-28578](https://jira.taosdata.com:18080/browse/TD-28578) [TD-28582](https://jira.taosdata.com:18080/browse/TD-28582) [TD-28583](https://jira.taosdata.com:18080/browse/TD-28583) |
| 7 | 否 | 普通授权码激活失败 | 1. cpu核数大于授权码指定cpu核数，激活集群失败 1. 测点数大于授权码指定测点数，激活集群失败 1. dnode数大于授权码指定dnode数，激活集群失败 1. 基础功能不可用，激活集群失败 1. 激活前集群复制 1. 激活码未在3天内使用，激活码失效 1. 使用普通授权码重复激活集群 | 1. 安装部署并启动单节点集群 1. 获取集群id 1. 根据集群id，通过taosGrant工具生成普通授权码, 指定cpu核数大于节点cpu核数，使用授权码激活集群 1. 恢复集群至临时授权状态，启动集群并写入部分数据，计算当前数据的测点数 1. 根据集群id，通过taosGrant工具生成普通授权码, 指定测点数小于已经存在的测点数，使用授权码激活集群 1. 安装部署并启动3节点集群 1. 获取集群id 1. 根据集群id，通过taosGrant工具生成普通授权码, 指定dnode数为2，使用授权码激活集群 1. 生成未包括基础功能的授权码并使用授权码激活集群 1. 集群失效后，通过授权码再次激活集群 1. 通过taosGrant工具生成普通授权码，指定维保服务时间大于基础功能时间，使用授权码激活集群 1. 将集群的所有数据复制到其他节点，使用相同的授权码激活两套集群 1. 使用不正确的授权码授权集群 1. 通过toasGrant生成三天前的授权码，使用授权码激活当前集群 1. 使用普通授权码对一套集群多次激活 | 1. 集群启动成功，集群处于临时授权 1. 获取集群id正常 1. 激活集群失败，提示错误"Number of CPU cores has reached the licensed upper limit" 1. 集群启动完成，写入数据正常 1. 激活集群失败，提示错误" Number of time series has reached the licensed upper limit" 1. 集群启动成功，集群处于临时授权 1. 获取集群id正常 1. 激活集群失败，提示错误"Number of dnodes has reached the licensed upper limit" 1. 激活集群失败，提示错误"Lack of basic functions in active code" 1. 激活集群失败，提示错误"`The active code can't be activated repeatedly`" 1. 生成授权码失败 1. 普通授权码可以同时激活两套集群；特殊授权码只有匹配machine code的集群被激活，复制集群提示错误“Cluster machines mismatch with active code” 1. 激活集群失败，提示错误“Invalid active code”或“Invalid active code length to parse active code” 1. 集群激活失败, 提示错误“License expired” 1. 集群第一次授权被激活，之后激活显示错误“`The active code can't be activated repeatedly`.”，集群状态及功能正常 | Pass | 普通授权码无法防止集群复制 |
| 8 | 是 | 特殊授权码激活 | 1. 单节点、多节点绑定mnode machine code信息的特殊授权码激活集群 1. 集群中有dnode处于offline状态激活集群 1. 集群中有mnode处于offline状态激活集群 | 1. 安装部署并启动单节点集群 1. 获取集群id及mnode的machine code 1. 根据集群id，mnode的machine code，通过taosGrant工具生成特殊授权码并激活集群 1. 验证集群授权信息与授权码中指定授权项的一致性，包括基础功能，流计算，数据订阅，审计日志 1. 重复以上步骤对多节点集群授权并检查各授权项信息 1. 停掉集群中一个dnode节点 1. 使用特殊授权码激活集群 1. 重新启动dnode节点 1. 停掉集群中一个mnode节点 1. 使用特殊授权码激活集群 1. 启动mnode节点 | 1. 集群启动成功，集群处于临时授权 1. 获取集群id及mnode 的machine code正常 1. 集群被激活 1. 集群授权信息与授权码中指定授权项的值一致 1. 集群被激活，授权信息与特殊授权码指定值一致 1. 集群中一个dnode节点处于offline状态 1. 激活集群完成，状态变为granted状态 1. 重新启动dnode节点完成 1. 集群中一个mnode节点处于offline状态 1. 激活集群完成 1. mnode节点启动完成 | Pass |  |
| 9 | 否 | 特殊授权码激活失败 | 1. 生成授权码中mnode machine code与集群节点machine code不一致 1. 使用特殊授权码多次激活集群 | 1. 安装部署并启动单节点集群 1. 获取集群id及mnode的machine code 1. 复制集群到另一节点 1. 根据集群id，mnode的machine code，通过taosGrant工具生成特殊授权码并激活集群 1. 验证集群授权信息与授权码中指定授权项的一致性，包括基础功能，流计算，数据订阅，审计日志 1. 多节点集群，更换其中一个mnode节点，重复以上步骤对多节点集群授权 1. 使用特殊授权码对一套集群多次激活 | 1. 集群启动成功，集群处于临时授权 1. 获取集群id及mnode 的machine code正常 1. 复制集群完成 1. 集群被激活，复制集群激活失败，提示错误“Illegal operation, the license is being used by an unlicensed cluster” 1. 集群授权信息与授权码中指定授权项的值一致 1. 激活集群失败，提示错误“Illegal operation, the license is being used by an unlicensed cluster” 1. 集群第一次授权被激活，之后激活显示错误“`The active code can't be activated repeatedly`.”，集群状态及功能正常 | Pass | 特殊授权码绑定所有机器的machine code可以防止集群复制（Note：更换dnode或mnode操作需要重新激活） |
|  | 10 | 是 | 集群状态异常激活 | 1. 多节点集群中，有异常dnode状态时，通过普通授权码授权 1. 多节点集群中，有异常mnode状态时，通过普通授权码授权 1. 多节点集群中，有异常dnode状态时，通过特殊授权码授权 1. 多节点集群中，有异常mnode状态时，通过特殊授权码授权 | 1. 通过taosGrant生成普通授权码 1. 停止多节点集群中一个dndoe 1. 使用授权码激活集群 1. 启动dnode节点 1. 通过taosGrant生成普通授权码 1. 停止多节点集群中一个mndoe 1. 使用授权码激活集群 1. 启动mnode节点 1. 重启mnode leader节点，检查集群状态 1. 通过taosGrant生成特殊授权码 1. 停止多节点集群中一个dndoe 1. 使用特殊授权码激活集群 1. 启动dnode节点 1. 通过taosGrant生成特殊授权码 1. 停止多节点集群中一个mndoe 1. 使用授权码激活集群 1. 启动mnode节点 1. 使用特殊授权码重新激活集群 1. 重启mnode leader节点，检查集群状态 | 1. 普通授权码生成 1. 集群中1个dnode停止服务 1. 激活集群成功 1. dnode节点启动完成，集群正常 1. 特殊授权码生成 1. 集群中1个mnode节点停止服务 1. 激活集群成功 1. mnode节点启动完成，集群正常 1. mnode leader节点重启完成，mnode切主，集群正常 1. 特殊授权码生成 1. 集群中1个dnode停止服务 1. 激活集群成功 1. dnode节点启动完成，集群正常 1. 特殊授权码生成 1. 集群中1个mnode节点停止服务 1. 激活集群失败 1. mnode节点启动完成，集群正常 1. 集群激活成功 1. mnode leader节点重启完成，mnode切主，集群正常 | Pass |  |
| 11 | 否 | 基础功能授权检查 | 1. dnode数检查 1. 测点数检查 1. cpu核数检查 | 1. 安装部署单节点集群并启动集群，获取cluster id 1. 通过taosGrant工具生成普通授权码，指定dnode数为1，测点数为100，并激活集群 1. 增加dnode节点 1. 写入数据超过100点 1. 重新生成授权码，指定dndoe数为2，测试数点200并更新授权码 1. 增加dnode节点 1. 写入数据 1. 恢复集群至初始状态，通过taosGrant生成普通授权码，指定dnode数为2，cpu小于两个节点cpu核数之和 1. 停止一台节点，使用授权码激活集群 1. 增加另一个dnode节点 1. 重新生成授权码，指定cpu核数超过2个节点的cpu核数并更新授权码 1. 增加另一个dndoe节点 1. 使用特殊授权码重复以上过程 | 1. 集群启动成功并获取cluster id 1. 激活集群完成 1. 增加dnode节点失败，提示错误 1. 写入数据失败，提示错误 1. 授权码更新 1. 增加dnode正常 1. 写入数据正常 1. 集群启动完成，普通授权码生成 1. 集群激活完成 1. 增加dnode节点失败，提示错误 1. 授权码更新 1. 增加dnode节点正常 1. 结果与普通授权码结果一致 | Pass |  |
| 12 | 否 | 授权功能项检查 | 1. 流计算数检查 1. 数据订阅topic数检查 1. 多级存储 1. 日志审计 1. 视图 | 1. 安装部署3节点集群并启动集群，获取cluster id 1. 通过taosGrant工具生成普通授权码，指定流计算数为3，数据订阅topic数为3，使用授权码激活集群 1. 创建4个流计算 1. 创建4个topic数据订阅 1. 重新生成授权码，指定流计算、数据订阅数为5，使用授权码激活集群 1. 创建流计算、topic数据订阅 1. 通过taosGrant工具生成特殊授权码，指定多级存储、日志审计、视图的功能有效时间为集群可用时间，视图数量为3，使用授权码激活集群 1. 创建4个视图 1. 等待功能失效，验证功能是否可用 1. 使用特殊授权码重复以上过程 | 1. 集群启动成功并获取cluster id 1. 激活集群完成 1. 3个流计算创建完成，第4个流计算创建失败，提示错误 1. 3个topic创建成功，第4个数据订阅创建失败，提示错误 1. 集群激活完成 1. 创建流计算、数据订阅完成 1. 集群被激活，功能项与授权码指定值一致 1. 创建3个视图成功，第4个视图失败 1. 时间到期后授权功能失效，功能不可用 1. 结果与普通授权码结果一致 | Fail | [TD-28647](https://jira.taosdata.com:18080/browse/TD-28647) [TD-28672](https://jira.taosdata.com:18080/browse/TD-28672) 视图、订阅、流增删更新时间为1分钟以内； 订阅限制topic数量 |
| 13 | 否 | CPU硬件或节点变更 | 1. cpu或节点变更导致的dnode machine code变更 | 1. 安装部署3节点集群并启动集群，获取cluster id 1. 通过taosGrant工具生成普通授权码，使用授权码激活集群 1. 停止mnode leader节点进行切主 1. 删除之前的leader mnode节点并更换mnode节点为另一台节点 1. 创建新节点为mnode 1. 使用特殊授权码激活集群，并重复步骤1-5 1. 检查集群状态及授权功能项状态 1. 根据集群cluster id重新生成普通授权码并激活集群 1. 停掉一个mnode节点 1. 根据集群cluster id和mnode machine code重新生成特殊授权码并激活集群 1. 启动mnode节点 1. 使用特殊授权码重新激活集群 | 1. 集群启动成功并获取cluster id 1. 激活集群完成 1. mnode leader节点停止，mndoe切主完成 1. 节点更换失败，集群进入revoked状态 1. 创建mnode完成，集群正常 1. 创建新节点mnode成功，集群自动进入“revoked”状态 1. 集群过期时间剩余7天，授权功能项与之前保持一致 1. 集群激活失败，提示错误“Illegal operation, the license is being used by an unlicensed cluster” 1. 一个mnode节点处于offline状态，集群状态依然为revoked，集群功能正常 1. 激活集群失败，提示错误 1. mnode节点启动完成，集群功能正常 1. 激活集群成功，授权功能项工作正常 | Pass |  |
| 14 | 否 | 授权变更限制 | 1. 授权码状态变更最多显示30个 1. 授权码更新最多显示10个 | 1. 安装部署集群 1. 重复执行回收授权码，激活集群操作 1. 激活集群11次 1. 重新安装部署集群 1. 激活集群并等待集群到期 1. 执行回收授权码命令 1. 重新激活集群 1. 重复步骤5-7，使集群状态变更30次 | 1. 安装部署集群完成，集群处于ungranted状态 1. 集群状态变更为revoked，granted状态 1. 仅显示近10次的授权码前30位 1. 安装部署集群完成，集群状态为ungranted 1. 激活集群完成，状态为granted，集群到期，状态为过期状态 1. 授权码回收完成，集群状态为revoked 1. 激活集群完成，集群状态为granted状态 1. 仅显示近30次的状态变更 | Pass |  |
| 15 | 否 | 集群扩容 | 1. 扩容dnode节点 1. 扩容mnode节点 | 1. 安装部署集群 1. 执行命令“show cluster machines;" 查看dnode的机器码 1. 生成特殊授权码并激活集群，指定dnode数量大于当前节点数量，如3 1. 连续创建两个dnode节点 1. 删除一个dnode，再添加另一个dnode 1. 重新生成特殊授权码激活集群 1. 创建两个mnode 1. 增加1个dnode 1. 重新生成特殊授权码，指定dnode数为4并激活集群 1. 检查集群基础、可选功能 1. 更换一个dnode节点 1. 重新生成特殊授权码激活集群 | 1. 安装部署集群完成 1. dnode机器码正常 1. 生成授权码并激活集群完成 1. 创建dnode节点完成，集群功能正常 1. 添加dnode失败，提示错误，集群进入revoked状态 1. 集群恢复正常，dnode 机器码更新 1. 创建mnode正常 1. 增加dnode失败，集群进入revoked状态 1. 集群激活完成 1. 集群基础、可选功能正常 1. 集群自动进入revoked状态 1. 激活集群完成 | Pass |  |
| 16 | 否 | 基础功能到期 | 1. 基础功能时间到期 1. 各授权功能项不可用 1. 基础功能过期集群节点异常 | 1. 安装部署3节点单副本集群，获取cluster id 1. 通过taosGrant工具生成授权码并指定基础功能和授权项到期时间（两者时间一致），并使用授权码激活集群 1. 检查集群写入、查询、流计算、数据订阅、多级存储，审计日志、视图功能 1. 等集群授权到期，再次检查以上功能 1. 停掉一个mnode或dnode节点 1. 生成新的授权码增加基础功能和可选功能时间，重新激活集群 1. 检查集群写入、查询、流计算、数据订阅、多级存储，审计日志、视图功能 | 1. 集群激动成功并获取cluster id 1. 激活集群完成 1. 集群各功能正常 1. 授权到期，基础功能只能写入，不能查询；流计算进入suspending状态；无法继续创建新的topic，同时订阅查询失败；审计日志无更新、不能创建新的视图，已存在的视图不能查看 1. 一个mnode或dnode节点处于offline状态，集群状态不变，集群功能与步骤四一致 1. 激活集群完成 1. 集群状态变为granted，各功能恢复正常 | Pass |  |
| 17 | 否 | 可选功能到期 | 1. 基础功能可用 1. 各授权功能项不可用 1. 可选功能过期集群节点异常 | 1. 安装部署3节点3副本集群，获取cluster id 1. 通过taosGrant工具生成授权码并指定基础功能和授权项到期时间（基础功能时间大于各功能项时间），并使用授权码激活集群 1. 检查集群写入、查询、流计算、数据订阅、多级存储，视图、审计日志功能 1. 等集群授权到期，再次检查授权功能项功能 1. 停掉一个mnode或dnode节点 1. 生成新的授权码增加可选功能时间，重新激活集群 1. 检查集群写入、查询、流计算、数据订阅、多级存储，审计日志、视图功能 | 1. 集群激动成功并获取cluster id 1. 激活集群完成 1. 集群各功能正常 1. 授权到期，基础功能正常；多级存储无法写入；流计算进入suspending状态；无法继续创建新的topic，同时订阅查询失败；无法创建新的视图、审计日志无更新、不能创建新的视图，已存在的视图不能查看 1. 一个mnode或dnode节点处于offline状态，集群状态不变，集群功能与步骤四一致 1. 激活集群完成 1. 集群各功能恢复正常 | Fail | [TD-28743](https://jira.taosdata.com:18080/browse/TD-28743) |
| 18 | 否 | 回收授权码 | 1. 授权码到期后清空授权码 1. 授权码未到期清空授权码 1. 集群有mnode或dnode offline状态下集群进入‘revoked’状态 1. 集群revoked状态下基础和可用功能 | 1. 安装部署3节点3副本集群，获取cluster id 1. 生成授权码并激活集群 1. 执行命令“alter cluster ‘activeCode’ ‘revoked’；”使集群进入revoked状态 1. 重新生成授权码并激活集群 1. 等集群到期后，查看集群状态 1. 待集群完全失效后，验证基础功能和可选功能 1. 使用特殊授权码激活集群 1. 停掉一个mnode或dnode节点使其成为offline状态 1. 执行命令“alter cluster ‘activeCode’ ‘revoked’；”使集群进入revoked状态 1. 生成授权码重新激活集群 1. 启动mnode或dnode 节点 | 1. 集群激动成功并获取cluster id 1. 激活集群完成 1. 集群状态为‘revoked’状态，基础功能和可选功能正常 1. 集群状态变为‘granted’状态，基础功能和可选功能正常 1. 集群状态变为‘revoked’状态，基础功能和可选功能正常，可用时间为失效时间+7天 1. 基础功能只能写入，不能查询；流计算进入suspending状态；无法继续创建新的topic，同时订阅查询失败；审计日志无更新、不能创建新的视图，已存在的视图不能查看 1. 集群恢复到授权状态，基础和可用功能正常 1. 集群中一个mnode或dnode节点处于offline状态 1. 集群进入revoked状态 1. 集群被激活，集群状态变为granted装填 1. mnode或dnode节点启动正常，集群基础功能和可选功能正常 | Pass |  |
| 19 | 否 | V3.0.5.0 V3.1.0.0 V3.1.1.7 taos.cfg激活，升级 | 1. 兼容通过taos.cfg激活的集群停服务升级 1. 升级完成后的授权项 1. 升级完成后的集群功能 | 1. 安装部署V3.0.5.0, V3.1.0.0, V3.1.1.7的3节点集群并通过taos.cfg的方式激活 1. 数据写入、查询功能正常 1. 停止业务及全部taosd服务，升级集群的每个节点 1. 重新生成授权码激活集群 | 1. 集群部署并激活正常 1. 集群功能正常 1. 集群升级完成，状态变更为未授权，集群功能正常，可用时间为当前时间+10天 1. 激活集群成功，基础功能及可选功能正常 | Pass | 滚动升级需保持前三位版本号一致，不支持跨多版本滚动升级 |
| 20 | 否 | V3.0.5.0 V3.1.0.0 V3.1.1.7 Alter dnode激活，升级 | 1. 兼容通过alter dnode激活的集群停服务升级 1. 升级完成后的授权项 1. 升级完成后的集群功能 | 1. 安装部署V3.1.0.0, V3.1.1.7的3节点集群并通过alter dnodes 的方式激活 1. 数据写入、查询功能正常 1. 停止业务及全部taosd服务，升级集群的每个节点 1. 重新生成授权码激活集群 | 1. 集群部署并激活正常 1. 集群功能正常 1. 集群升级完成，状态变更为未授权，集群功能正常，可用时间为当前时间+10天 1. 激活集群成功，基础功能及可选功能正常 | Pass | 滚动升级需保持前三位版本号一致，不支持跨多版本滚动升级 |
| 21 | 否 | 删除alter dnode命令 | 1. 不支持Alter all dnodes xxxx 1. 不支持Alter dnode {dnodeid}命令 | 1. 升级集群到3.2.2.3版本以上 1. 对升级后的集群执行命令”alter all dnodes xxxxx“ 和 ”alter dnode {dnodeid} xxxxxx“ | 1. 集群升级完成，集群各功能正常 1. 执行命令失败，提示错误“Invalid config option” | Pass |  |
| 兼容平台 | 22 | 否 | Windows、Arm 64平台 | 1. Windows平台激活集群 1. Arm 64平台激活集群 | 1. 在Windoes平台安装部署TDengine集群 1. 根据cluster id生成授权码并通过授权码激活集群 1. 重新安装部署集群 1. 根据cluster id及machine code生成特殊授权码并通过授权码激活集群 1. 在Arm 64 执行步骤1-4 | 1. 集群安装部署完成 1. 激活集群完成，基础功能、可选功能正常 1. 安装部署集群完成 1. 激活集群完成，基础功能、可选功能正常 1. Arm 64 执行步骤1-4 完成 | Fail | [TD-28746](https://jira.taosdata.com:18080/browse/TD-28746) |
| 补充用例 | 云平台 | 23 | 是 | 云服务版授权，集群复制 | 1. taosGrant工具参数 --check-machine为0的场景 1. 验证云服务版不检查机器码的复制场景 | 1. 编译部署云服务版TDengine 1. 检查云服务版未授权状态，基础功能及可选功能的默认参数 1. 使用taosGrant工具生成授权码，指定参数check-machine为0，并指定基础功能及taosd可选功能参数 1. 使用授权码授权集群 1. 将集群数据拷贝到另一节点并启动taosd服务 1. 检查基础功能、可选功能授权值 | 1. 部署云服务版TDengine正常 1. 基础功能及可选功能参数与Func spec一致 1. 生成授权码 1. 授权完成，基础功能和可选功能功能授权参数与授权码一致 1. 拷贝节点数据完成，节点能够正常启动 1. 基础功能和可选功能值与授权码一致 | Fail | [TD-28848](https://jira.taosdata.com:18080/browse/TD-28848) |

### 8. JIRA列表

| Id | Title | Comment |
| --- | --- | --- |
| [TD-28570](https://jira.taosdata.com:18080/browse/TD-28570) | taosGrant 3.2.3.0 生成授权码basic功能参数类型未检查 | Fixed |
| [TD-28577](https://jira.taosdata.com:18080/browse/TD-28577) | 三节点环境show grants logs命令返回的machine id只有一个 | Fixed |
| [TD-28578](https://jira.taosdata.com:18080/browse/TD-28578) | 3节点环境show cluster machines命令返回的machinde code重复 | Fixed |
| [TD-28582](https://jira.taosdata.com:18080/browse/TD-28582) | 指定service时间 小于当前时间生成授权码，激活授权码后service expire变成1970-01-01 | Fixed |
| [TD-28583](https://jira.taosdata.com:18080/browse/TD-28583) | 测点数、cpu core数显示值超过授权码定义值 | Canceled |
| [TD-28647](https://jira.taosdata.com:18080/browse/TD-28647) | 流计算数量达到授权最大值时，创建新的流计算触发coredump | Fixed |
| [TD-28672](https://jira.taosdata.com:18080/browse/TD-28672) | 数据订阅授权topic数量不生效（之前授权的是订阅数量） | Fixed |
| [TD-28691](https://jira.taosdata.com:18080/browse/TD-28691) | 版本更新重启节点，流计算任务空指针触发coredump | Fixed |
| [TD-28743](https://jira.taosdata.com:18080/browse/TD-28743) | 授权失效后，流任务进入paused状态超过1分钟 |  |
| [TD-28746](https://jira.taosdata.com:18080/browse/TD-28746) | 当taosGrant工具节点时间小于集群节点时间时, 授权（revoked->granted)失败 | Fixed |
| [TD-28848](https://jira.taosdata.com:18080/browse/TD-28848) | 云服务版授权后，拷贝数据到另一环境启动失败 | Fixed |

### 9. 开始结束时间

2024-01 -- 2024-02
