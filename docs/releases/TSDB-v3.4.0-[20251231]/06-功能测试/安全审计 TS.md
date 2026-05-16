# 安全审计 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-1-20 | 2026-1-23 | 1.0 | 陈东明 | 初始化 |

## 2. 测试目标

1. 可以设置安全审计级别
2. 可以设置用来传输审计记录所有使用的网络协议
3. 记录查看审计记录

## 3. 参考文档

[安全审计 FS](https://taosdata.feishu.cn/wiki/XmSVwBepXiBHopkwu41cGW50n5f)。

## 4. 测试结论

功能符合预期

## 5. 测试环境

- OS: Linux

## 6. 功能测试

### 6.1 设置审计级别功能

#### 6.1.1 测试要点

通过参数auditLevel设置审计级别

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | auditLevel设置为1 | 根据[安全审计 FS](https://taosdata.feishu.cn/wiki/XmSVwBepXiBHopkwu41cGW50n5f)，4.2节，在该级别下，记录create dnode，drop dnode ，alter dnode，create mnod，create mnode，create qnode，drop qnode，restore dnode/mnode/vnode/qnode | 通过 |
| 2 | auditLevel设置为2 | 根据[安全审计 FS](https://taosdata.feishu.cn/wiki/XmSVwBepXiBHopkwu41cGW50n5f)，4.2节，在该级别下，记录Alter cluster，balance vgroup leader，REDISTRIBUTE VGROUP，BALANCE VGROUP，Assign Leader，GRANT privileges，REVOKE privileges，login，alter user，create user，import user，drop user，GrantPrivileges，RevokePrivileges，Create Mount，Drop Mount，kill Retention，auto TrimDB，createEncryptAlgr，dropEncryptAlgr以及auditLevel设置为1的内容 | 通过 |
| 3 | auditLevel设置为3 | 根据[安全审计 FS](https://taosdata.feishu.cn/wiki/XmSVwBepXiBHopkwu41cGW50n5f)，4.2节，在该级别下，记录create database，alter database，drop database，Kill compact，compact ，alterStb，create stable，dropStb，create stream，drop stream，recalcStream，create topic，drop topic，reload topic，drop Rsma，create Rsma ，alterRsma ，createView，dropView ，以及auditLevel设置为2和1的内容 | 通过 |
| 4 | auditLevel设置为4 | 根据[安全审计 FS](https://taosdata.feishu.cn/wiki/XmSVwBepXiBHopkwu41cGW50n5f)，4.2节，在该级别下，记录createTable，dropTable ，以及auditLevel设置为3, 2和1的内容 | 通过 |
| 5 | auditLevel设置为5 | 根据[安全审计 FS](https://taosdata.feishu.cn/wiki/XmSVwBepXiBHopkwu41cGW50n5f)，4.2节，在该级别下，记录delete，insert ， select，以及auditLevel设置为4, 3, 2和1的内容 | 通过 |
| 6 | auditLevel设置为5，enableAuditSelect为false, enableAuditInsert为false | 根据[安全审计 FS](https://taosdata.feishu.cn/wiki/XmSVwBepXiBHopkwu41cGW50n5f)，4.2节，在该级别下，记录delete，以及auditLevel设置为4, 3, 2和1的内容 | 通过 |
| 7 | auditLevel设置为5，enableAuditDelete为false, enableAuditInsert为false | 根据[安全审计 FS](https://taosdata.feishu.cn/wiki/XmSVwBepXiBHopkwu41cGW50n5f)，4.2节，在该级别下，记录select，以及auditLevel设置为4, 3, 2和1的内容 | 通过 |
| 8 | auditLevel设置为5，enableAuditDelete为false, enableAuditSelect为false | 根据[安全审计 FS](https://taosdata.feishu.cn/wiki/XmSVwBepXiBHopkwu41cGW50n5f)，4.2节，在该级别下，记录insert，以及auditLevel设置为4, 3, 2和1的内容 | 通过 |

### 6.2 传输协议设置功能

#### 6.2.1 测试要点

通过参数`AuditHttps`设置审计传输协议

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 将auditHttps设置为1 | taosd与taoskeeper的传输采用https协议，通过抓包确认无明文传输 | 通过 |

### 6.3 审计库保存时间

#### 6.3.1 测试要点

测试创建audit库时的keep参数

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | Create audit db 时参数keep小于1825d | 不符合要求，不能创建 | 通过 |

### 6.4 审计库落盘策略

#### 6.4.1 测试要点

测试创建audit库时的Wal_level参数

#### 6.4.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | Create audit db 时参数wal_level设置为1 | 不符合要求，不能创建 | 通过 |

### 6.5 审计库加密策略

#### 6.5.1 测试要点

测试创建audit库时的ENCRYPT_ALGORITHM参数

#### 6.5.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | Create audit db 时参数ENCRYPT_ALGORITHM设置为空 | 不符合要求，不能创建 | 通过 |

## 7. 易用性测试（可选）

## 8. 长期稳定性测试（可选）

## 9. 性能测试

#### 9.0.1 测试要点

各种审计级别时的数据库性能

#### 9.0.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | auditLevel设置为1 | 通过benchmarch写入100K数据库，记录完成时间 对比性能 与未打开时无明显下降 | 通过 |
| 2 | auditLevel设置为2 | 通过benchmarch写入100K数据库，记录完成时间 对比性能 与未打开时无明显下降 |  |
| 3 | auditLevel设置为3 | 通过benchmarch写入100K数据库，记录完成时间 对比性能 与未打开时无明显下降 |  |
| 4 | auditLevel设置为4 | 通过benchmarch写入100K数据库，记录完成时间 对比性能 与未打开时无明显下降 |  |

## 10. 安全测试

#### 10.0.1 测试要点

检查加密后是否能在数据文件中看到明文

#### 10.0.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | WAL文件 | 创建审计数据库，指定AES算法，通过benchmarch写入100条数据，数据中包含字符串 检查wal文件中是否包含字符串。 | 通过 |
| 2 | Tsdb文件 | 创建审计数据库，指定AES算法，通过benchmarch写入100条数据，数据中包含字符串 检查tsdb文件中是否包含字符串。 | 通过 |

## 11. 兼容性测试

旧审计库无法升级到新版本，需删除旧审计库，重新创建新审计库。

## 12. 已知问题和限制（可选）
