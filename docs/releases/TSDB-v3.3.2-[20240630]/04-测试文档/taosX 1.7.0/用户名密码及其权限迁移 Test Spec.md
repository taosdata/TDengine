# 用户名密码及其权限迁移 Test Spec

## 1. 测试目标

验证可以通过 Explorer 将 3.3.2.0 版本的 TDengine 用户名密码、权限及白名单信息导入；

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
|  |  |  |  |
|  |  |  |  |

## 3. 测试范围

通过 Explorer 操作用户名密码及权限迁移

## 4. 测试结论

1. 通过 Explorer 导入用户名密码及其权限、白名单信息，验证通过
2. 通过 Explorer 只导入用户名密码，验证通过
3. 通过 Explorer 只导入权限，若用户存在且对应资源存在，则导入成功，验证通过
4. 通过 Explorer 只导入白名单和用户名密码，验证通过

## 5. 开发质量报告

结论：本特性/优化的开发质量是良

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 |  |
| 基础测试用例不通过 | 1 |
| Bug 总数 | 1 |
| 严重 Bug 总数 |  |

## 6. 已知问题和限制

- 

## 7. 测试环境

- OS: Windows, Linux, macOS
- Browser: Chrome
3.3.1.0 的源数据库使用 192.168.2.19，目标数据库使用 192.168.2.10
3.3.0.0 的数据库使用 192.168.2.18

## 8. 测试数据 (Optional)

测试用户及权限信息：
注意：目标端的数据库（192.168.2.10）只有 test_privileges1 和 test_privileges2

| 用户名 | 权限信息 |
| --- | --- |
| test1/tbase1234!@#$ | ![](./images/img_Hjmtbpn1dorEvHxUXsHcwC5LnVc.png) |
| test2/tbase1234!@#$ | ![](./images/img_XpXqb88ado2lK8x8RTRcBS1TnBb.png) |
| test3/tBase1234!@#$ | ![](./images/img_UiRybiiWUoIJaDxtbphcnFMbnfe.png) |
| test4/tBase1234!@#$ | ![](./images/img_CoXpbFhZDoWPsex6JZJcyEoanwg.png) |

## 9. 测试用例

### 9.1 功能

在提测时，开发应保证基础用例全部通过。
| 类型 | 测试场景 | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| Explorer | 测试 UI 展示正确 | 点击 Management - Users - Import | 默认选中前两项
用户名密码
权限 |  | Pass | 建议在 Server 输入框，添加 placeholder 形式的说明 |
|  | 用户名、密码、权限、白名单可正常导入 |  | 导入完成后，能够正确展示导入结果 |  | Pass | [https://jira.taosdata.com:18080/browse/TD-30713](https://jira.taosdata.com:18080/browse/TD-30713)
密码在输入框不能明文直接展示出来 |
| Explorer 导入异常场景 | 错误的 taosadapter IP | 点击 Management - Users - Import 填写错误的 URL 或 taosadapter IP | 若是不符合规则的 URL 由前端限制无法正常提交请求，若 IP 错误则返回请求异常。 |  | Pass | 若 IP 异常返回：[0xE001] Internal error: `WebSocket internal error: IO error: No route to host (os error 113)`: Internal error: `WebSocket internal error: IO error: No route to host (os error 113)` |
|  | 错误的 taosadapter Port | 点击 Management - Users - Import 填写错误的端口信息 | 只要不是正常的 adapter 服务都应该能返回错误。 |  | Pass | 若端口无法连通则错误为：
[0xE001] Internal error: `WebSocket internal error: IO error: Connection refused (os error 111)`: Internal error: `WebSocket internal error: IO error: Connection refused (os error 111)`
若端口不是一个可用的 taosadapter 的端口则返回错误：
[0xE001] Internal error: `WebSocket internal error: HTTP error: 200 OK`: Internal error: `WebSocket internal error: HTTP error: 200 OK` |
|  | 错误的 root 用户密码 | 点击 Management - Users - Import 在密码一栏填写错误的密码 | 提示认证失败 |  | Pass | 密码错误返回：[0x0357] Internal error: `Authentication failure`: Internal error: `Authentication failure` |
|  | 所有 items 均为勾选 |  |  |  |  |  |
| 导入项 | 导入的用户在目标端已存在，且与源端密码相同 |  |  |  |  |  |
|  | 导入的用户在目标端已存在，且与源端密码不同 |  |  |  |  |  |
|  | 导入的用户在目标端已存在，且在源端拥有对 db1 的 read 权限，但目标端 db1 不存在 |  |  |  |  |  |
|  | 只指定用户名及密码导入 | 1.点击 Management - Users - Import
2.点击导入，填写正确的地址及密码
3.只勾选用户名及密码
4.点击导入 | 1.若用户不存在，导入成功。
2.若用户存在无论密码是否一致提示用户已存在，不会覆盖。
3.导入的用户可以通过 Explorer 和 taosShell 正常登录。 |  | Pass |  |
|  | 只指定权限导入 | 1.点击 Management - Users - Import
2.点击导入，填写正确的地址及密码
3.只勾选权限
4.点击导入 | 1.若用户不存在，则提示权限导入失败。
2.若用户存在但无该权限且对应资源存在无论密码是否更改权限都会导入成功。
3.若用户存在但不存在该资源则导入失败。
4.若用户存在且用户已拥有该权限则导入失败提示已经存在。 |  | Pass | 权限目前分为两种，对数据库的读权限和写权限，
对 topic 的订阅权限。

4.若用户已存在且用户已对资源有权限不会提示权限已经存在。 |
|  | 白名单及用户名和密码一起导入 | 1.点击 Management - Users - Import
2.点击导入，填写正确的地址及密码
3.只勾选白名单
4.点击导入 | 1.若用户存在则导入失败；
2.若用户不存在则导入成功； |  | Pass |  |
| 命令行模式 | 支持使用 native 连接在线迁移 | 1.对 3.3.2.0 执行 taosx privileges -f "taos://root:taosdata@192.168.2.19:6030" -t "taos://192.168.2.18:6030"

2.对 3.3.1.0 执行 taosx privileges -f "taos://root:taosdata@192.168.2.19:6030" -t "taos+ws://root:taosdata@192.168.2.10:6041"

1. 对 3.3.0.0 执行 taosx privileges -f "taos://192.168.2.18:6030" -t "taos+ws://192.168.2.18:6041"

4.使用 -u 仅对用户名和密码进行迁移
5.使用 -p 仅对权限进行迁移 | 1.执行成功
2.执行失败，提示目标端版本不支持
3.执行失败，提示源端版本不支持
4.所有迁移之后的用户能够正常登录 |  |  | 1.部分同步错误提示：
1.1 Partially failed: 4 users16 privileges imported successfully, 15 items failed:
1.2 数据库不存在，用户 User `test1` privilege `read on `kafka1`` import fails: Internal error: `Database not exist`,
1.3 Topic 不存在，User `test3` privilege `subscribe on `test_privileges1`` import fails: Internal error: `Topic not exist`,

2.目标端版本不兼容的错误提示：
Error: Version mismatch, expected 3.3.2.0.0621 compatible version, got 3.3.0.0
3.源端版本不匹配的话内容会返回：Error: [0x011E] Internal error: `Version not compatible`
4.现在版本应该校验的是 3.3.1.0 网上的，所以 3.3.1.0 会去执行查询用户的操作，但是 3.3.1.0 不包含 ins_grants_full 这个表； |
|  | 支持使用 websocket 连接在线迁移 | 1.对 3.3.1.0 执行 taosx privileges -f "taos+ws://root:taosdata@192.168.2.19:6041" -t "taos+ws://192.168.2.10:6041"

2.对 3.3.1.0 执行 taosx privileges -f "taos+ws://root:taosdata@192.168.2.19:6041" -t "taos+ws://root:taosdata@192.168.2.18:6041"

1. 对 3.3.0.0 执行 taosx privileges -f "taos+ws://192.168.2.18:6030" -t "taos+ws://192.168.2.10:6041" | 1.执行成功
2.执行失败，提示目标端版本不支持
3.执行失败，提示源端版本不支持 |  |  |  |
|  | 支持导出为文件 | 1.对 3.3.1.0 执行 taosx privileges -f "taos://root:taosdata@192.168.2.19:6030" -o /data/2.19.security

2.对 3.3.1.0 执行 taosx privileges -f "taos+ws://root:taosdata@192.168.2.19:6041" -o /data/2.19.security.2

1. 对 3.3.0.0 执行 taosx privileges -f "taos://192.168.2.18:6030" -o /data/2.19.security.3

2. 对 3.3.0.0 执行 taosx privileges -f "taos+ws://192.168.2.18:6041" -o /data/2.19.security.4 | 1.执行成功
2.执行成功
3.执行失败，提示源端版本不支持
4.执行失败，提示源端版本不支持 |  |  |  |
|  | 支持从文件导入 | 1.taosx privileges -i /data/2.19.security -t "taos://192.168.2.19:6030"

2.taosx privileges -i /data/2.19.security.2 -t "taos+ws://192.168.2.10:6041"

3.taosx privileges -i /data/2.19.security -t "taos+ws://192.168.2.18:6041" | 1.执行成功
2.执行成功
3.执行失败，提示目标端版本不支持 |  |  |  |
| 命令行模式异常场景 | 使用非 root 用户导出、导入 | 1.对 3.3.1.0 执行 taosx privileges -f "taos+ws://test1:tbase1234%21%40%23%24@192.168.2.19:6041" -o /data/2.19.security

2.对 3.3.1.0 执行 taosx privileges -f "taos+ws://test1:tbase1234%21%40%23%24@192.168.2.19:6041" -t "taos+ws://root:taosdata@192.168.2.18:6041"

3.taosx privileges -i /data/2.19.security -t "taos://test1:tbase1234%21%40%23%24@192.168.2.19:6030" | 1.执行失败，提示只有 root 用户才能操作
2.执行失败，提示只有 root 用户才能操作
3.执行失败，提示只有 root 用户才能操作 |  |  |  |
|  | 使用在线迁移方式时，源或者目标端的 IP 错误 |  |  |  |  |  |
|  | 使用在线迁移方式时，源或者目标端的 port 错误 |  |  |  |  |  |
|  | 导出为文件时，目标端目录无写入权限 |  |  |  |  |  |

### 9.2 可用性

测试用例包括但不局限于：
- UI是否美观？
- 交互是否合理？
- 字体、字号是否合适？
- 是否存在错别字？

### 9.3 可靠性

这里用于描述稳定性测试相关的内容。

### 9.4 性能

这里用于描述性能测试相关的内容。

### 9.5 安全性

测试用例包括但不局限于：
- 日志中是否包含敏感信息？

### 9.6 兼容性

测试用例包括但不局限于：
- 升级安装后，老版本（上一个版本）下创建的任务，能否继续执行？

### 9.7 本地化

测试用例包括但不局限于：
- 点击切换语言按钮后，UI上的所有元素是否按照选择的语言，正确展示？

## 10. 待讨论(Optional)

这里用于记录在测试或用例编写过程中想到的需要讨论的问题：
- aaa
- bbb

## 11. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: abc

## 12. 测试计划 (Optional)

这里用于计划此 feature 测试的开始和结束时间。

## 13. 测试备忘 (Optional)

这里用于记录测试过程中发现的，与产品行为相关的一些重要信息。

## 14. 参考文档

- [FS - 用户名、密码及其权限导入导出](https://taosdata.feishu.cn/wiki/MjWnwuGvniVod5kwxOEcPzMUnLf)
- [IP 白名单用户手册](https://taosdata.feishu.cn/wiki/TEQlwg19hizT7ukPcWscRRYunub)
- [[Test Report] - TD-25305 TDengine 白名单机制 （企业版功能）](https://taosdata.feishu.cn/wiki/YyZmw9vsDioPmeks5ZOc09tLn2e)
