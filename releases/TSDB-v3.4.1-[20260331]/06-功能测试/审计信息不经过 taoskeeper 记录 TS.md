# 审计信息不经过 taoskeeper 记录 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-3-10 | 2026-3-30 | 0.1 | 陈东明 | 初始化 |

## 2. 测试目标

<quote-container>
测试将审计信息保存到自身集群，从而不经过taoskeeper
</quote-container>

## 3. 参考文档

<quote-container>
[审计信息不经过 taoskeeper 记录 FS](https://taosdata.feishu.cn/wiki/IE6dwg0BAiWeT7kBktycLLbYnGd)。
</quote-container>

## 4. 测试结论

<quote-container>
功能符合预期
</quote-container>

## 5. 测试环境

- OS: Linux

## 6. 功能测试

### 6.1 AuditSaveInSelf 开关功能

#### 6.1.1 测试要点

通过参数AuditSaveInSelf打开功能开关

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | AuditSaveInSelf设置为1 | 执行show variables，AuditSaveInSelf的值变为1 | 通过 |

### 6.2 创建审计库功能

#### 6.2.1 测试要点

执行创建审计库，会同时创建operations超级表

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | CREATE DATABASE IF NOT EXISTS audit IS_AUDIT 1 VGROUPS 1; | audit库被创建成功，并且超级表operations也被创建 | 通过 |

### 6.3 审计信息保存功能

#### 6.3.1 测试要点

审计信息被正确保存

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 审计信息保存 | 根据[安全审计 FS](https://taosdata.feishu.cn/wiki/XmSVwBepXiBHopkwu41cGW50n5f) 中的操作列表，执行相关操作，查询audit.operations表，确认该操作在表中存在 | 通过 |

## 7. 易用性测试（可选）

无

## 8. 长期稳定性测试（可选）

无。

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

#### 11.0.1 测试要点

检查新版本兼容旧版本

#### 11.0.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | 新版本兼容旧版本 | 安装3.4.1.0之前的版本，并且通过taoskeeper已经记录了审计信息，升级到3.4.1.0版本，打开AuditSaveInSelf开关，审计信息不通过taoskeeper也可以写入审计库。 | 通过 |

## 12. 已知问题和限制（可选）

无
