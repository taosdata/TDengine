# TS-5111 [天合能源] 删除名称非法的表 Test Report

## 1. 测试结论

经测试，所有测试用例已通过

## 2. 测试目标

Jira：
TS-5111

Spec参考文档： [支持删除名称非法的表](https://taosdata.feishu.cn/wiki/JgeDwZkH3iTNv2ksVkWcHenKnTf)
TD2.x 和 TD3.x 对表名校验不严格，会导致创建包含可见及不可见的非法字符的表，本次修改提供一种语法对包含非法字符的超级表、子表及普通表进行删除（root用户）。本次测试目标主要验证root用户使用新语法删除包含非法字符的表、非root用户无权限使用新语法删除表的功能性。

## 3. 变更历史

| 日期 | 版本 | 负责人 | 修改记录 |
| --- | --- | --- | --- |
| 2024-07-10 | 0.1 | Charles |  |

## 4. 测试范围

本次测试功能范围如下：
1. root用户删除一个或多个包含非法字符的超级表、子表、普通表
2. 存在以uid命名的超级表、子表、普通表，只删除指定uid的表，以uid命名的表不受影响
3. 删除不存在uid的表，提示错误
4. root用户删除多个包含非法字符的混合超级表、子表、普通表，删除失败，提示错误
5. 系统表uid为空，无法被删除
6. 非root用户无权限删除包含非法字符的表并提示错误(用户有db、超级表及子表权限）

## 5. 开发质量报告

结论：本特性/优化的开发质量是（优，良，一般，差，很差）

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 1 |
| 严重 Bug 总数 | 0 |

## 6. 已知问题和限制

无

## 7. 测试环境

测试平台：Linux x64
测试资源：
- 192.168.1.35
测试版本：3.0分支最新代码（89c227800f22db951a786da381473895d844b586）

## 8. 测试数据

无

## 9. 测试用例

| 用例No. | 基础用例 | 测试用例名称 | 测试步骤 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- |
| 1 | 是 | root用户通过uid删除一个表 | 1. 安装部署TDengine 1. 创建db，sql： create database db； 1. 创建包含（不包含）特殊字符的超级表、子表、普通表 1. 查询系统表获取包含（不包含）特殊字符的超级表、子表、普通表的uid 1. 使用uid删除包含（不包含）特殊字符的超级表，sql：DROP STABLE WITH uid； 1. 使用uid删除包含（不包含）特殊字符的子表、普通表，sql：DROP TABLE WITH uid； | Pass |  |
| 2 | 是 | root用户通过uid删除多个表（100个） | 1. 安装部署TDengine 1. 创建db，sql： create database db； 1. 创建包含（不包含）特殊字符的多个超级表、子表、普通表 1. 查询系统表获取包含（不包含）特殊字符的多个超级表、子表、普通表的uid 1. 使用uid循环删除包含（不包含）特殊字符的超级表，sql：DROP STABLE WITH uid； 1. 使用uid删除包含（不包含）特殊字符的超级表、子表、普通表，sql：DROP TABLE WITH uid, uid, uid ...； | Pass |  |
| 3 | 否 | 以表uid命名的表不被删除 | 1. 安装部署TDengine 1. 创建db，sql： create database db； 1. 创建包含特殊字符的超级表 1. 查询系统表获取包含特殊字符超级表的uid 1. 创建名称为uid的超级表 1. 使用uid删除包含特殊字符的超级表， sql: drop stable with if exists uid; 1. 检查以uid命名的超级表及数据未被删除 1. 重复步骤3-7对子表、普通表进行验证 | Pass |  |
| 4 | 否 | 删除不存在的uid删除表 | 1. 安装部署TDengine 1. 创建db，sql： create database db； 1. 不存在的uid表，sql：drop （s)table with xxxxx； 1. 删除表提示错误“Table does not exists” | Pass |  |
| 5 | 否 | 删除超级表语句删除子表、普通表 | 1. 安装部署TDengine 1. 创建db，sql： create database db； 1. 创建包含特殊字符的超级表、子表、普通表 1. 查询系统表获取包含特殊字符子表、普通表的uid 1. 删除子表、普通表报错，提示错误“STable does not exists” | Pass | [TD-32302](https://jira.taosdata.com:18080/browse/TD-32302) |
| 6 | 否 | 使用混合uid删除表 | 1. 安装部署TDengine 1. 创建db，sql： create database db； 1. 创建包含特殊字符的超级表、子表、普通表 1. 查询系统表获取包含特殊字符超级表、子表、普通表的uid 1. 删除包含超级表、子表或普通表的uid，提示错误“Cannot drop super table in batch” | Pass |  |
| 7 | 否 | 删除系统表 | 1. 安装部署TDengine 1. 创建db，sql： create database db； 1. 查询系统表uid 1. 删除系统表，drop table with xxxx，提示错误“” | Pass |  |
| 8 | 否 | 非root用户通过uid删除一个表或多个表 | 1. 安装部署TDengine 1. 创建db，sql： create database db； 1. 创建包含特殊字符的多个超级表、子表、普通表 1. 创建用户test并使用test用户连接taosd 1. 将db所有权限赋给test用户 1. 使用test用户根据uid删除超级表、子表、普通表 1. 将test用户权限分别改为db写权限、超级表、子表、普通表读写/写权限，使用test用户根据uid删除超级表、子表、普通表 | Pass |  |

## 10. 待讨论(Optional)

无

## 11. JIRA列表

| Id | Title | Comment |
| --- | --- | --- |
| [TD-32302](https://jira.taosdata.com:18080/browse/TD-32302) | 通过uid删除超级表语句删除子表、普通表的错误提示不稳定 | Fixed |

## 12. 测试计划 (Optional)

2024-09-23 - 2024-09-26

## 13. 风险评估

无

## 14. 测试备忘 (Optional)

无

## 15. 参考文档 (Optional)

[支持删除名称非法的表](https://taosdata.feishu.cn/wiki/JgeDwZkH3iTNv2ksVkWcHenKnTf)
