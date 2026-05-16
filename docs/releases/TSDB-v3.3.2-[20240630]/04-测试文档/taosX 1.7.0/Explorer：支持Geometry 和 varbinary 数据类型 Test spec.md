# Explorer：支持Geometry 和 varbinary 数据类型 Test spec

## 1. 测试目标

- 验证explorer上，支持Gemoetry 和 varbinary 数据类型

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.06.03 | 0.1 | 聂敏慧 | Initial Draft |

## 3. 测试范围

本需求的覆盖范围：
- 数据浏览器页面创建超级表/普通表时
- 数据浏览器页面查看超级表/普通表配置信息
- 数据浏览器页面编辑超级表/普通表配置
- 数据浏览器页面查看数据，能够正确显示 geometry 和 varbinary 数据类型的值
- Datain 支持 transformer 的数据源配置数据映射时创建超级表
- 本次测试均在 explorer上执行

## 4. 测试结论

- 使用 explorer ， 在数据浏览器页面创建超级表/普通表时，支持 geometry 和 varbinary 数据类型，测试通过。
- 使用 explorer ， Datain 支持 transformer 的数据源页面创建超级表/普通表时，支持 geometry 和 varbinary 数据类型，测试通过。
- 使用 explorer ， 在数据浏览器页面查看超级表/普通表时，支持 geometry 和 varbinary 数据类型，测试通过。
- 使用 explorer ， 在数据浏览器页面编辑超级表/普通表时，支持 geometry 和 varbinary 数据类型长度，编码类型，压缩类型和压缩级别的修改，测试通过。
- 使用 explorer， 在数据浏览器页面查看数据，能够正确显示 geometry 和 varbinary 数据类型的值（已知问题见第 6 节）

## 5. 开发质量报告

结论：本特性/优化的开发质量是优

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 1 |
| 严重 Bug 总数 | 0 |

## 6. 已知问题和限制

- GEOMETRY 显示的问题
1. 小数位后的0会被截掉。如POINT(100.00 100.00) 会显示为 POINT(100 100）
2. 浮点数精度的问题：数值太大的时候，显示有误差。如'POINT(1999999999999999999999999 31.00112283)') 显示成POINT (2000000000000000000000000 31.001123)

## 7. 测试环境

- OS: Windows, Linux
- Browser: Chrome

## 8. 测试用例

### 8.1 功能

| Description | Expected Results | Result | Jira | Automated | Memo |
| --- | --- | --- | --- | --- | --- |
| [basic]
数据浏览器页面创建超级表/普通表，普通列使用Geometry 和 varbinary 类型，encode，compress, level使用默认值 | 成功创建超级表/普通表 | Pass |  |  |  |
| [basic]数据浏览器页面查看数据 | 能够正确显示 geometry 和 varbinary 数据类型的值 | Pass |  |  |  |
| 数据浏览器页面创建超级表，普通列使用Geometry 类型 | 1. 不能选择为复合主键 | Pass |  |  |  |
|  | 2. 前端限制长度为1-65517 | Pass |  |  |  |
|  | 3. encode只能选择 disabled | Pass |  |  |  |
|  | 4. compress 分别选择 lz4, zlib, zstd, xz 成功创建表 | Pass |  |  |  |
|  | 5. 压缩等级分别选择medium, low, high 成功创建表 | Pass |  |  |  |
| 数据浏览器页面创建普通表，普通列使用Geometry 类型 | 同上4-8行 | Pass |  |  |  |
| 在transformer的映射配置创建超级表，普通列使用Geometry 类型 | 同上4-8行 | Pass |  |  |  |
| 数据浏览器页面创建超级表，标签列使用Geometry 类型 | 1. 前端限制长度为1-16382 | Pass |  |  |  |
|  | 2. 能成功创建表 | Pass |  |  |  |
| 在transformer的映射配置创建超级表，标签列使用Geometry 类型 | 同上11-12行 | Pass |  |  |  |
| 数据浏览器页面创建超级表，普通列使用varbinary类型 | 1. 不能选择为复合主键 | Pass |  |  |  |
|  | 2. 前端限制长度为1-65517 | Pass |  |  |  |
|  | 3. encode只能选择 disabled | Pass |  |  |  |
|  | 4. compress 分别选择 lz4, zlib, zstd, xz 成功创建表 | Pass |  |  |  |
|  | 5. 压缩级别分别选择medium, low, high 成功创建表 | Pass |  |  |  |
| 数据浏览器页面创建普通表，普通列使用varbinary 类型 | 同上14-18行 | Pass |  |  |  |
| 在transformer的映射配置创建超级表，普通列使用varbinary 类型 | 同上14-18行 | Pass |  |  |  |
| 数据浏览器页面创建超级表，标签列使用 varbinary 类型 | 1. 前端限制长度为1-16382 | Pass |  |  |  |
|  | 2. 能成功创建表 | Pass |  |  |  |
| 在transformer的映射配置创建超级表，标签列使用varbinary 类型 | 同上21-22行 | Pass |  |  |  |
| 数据浏览器页面查看超级表/普通表信息 | 1.geometry类型的列信息显示正确
2.varbinary类型的列信息显示正确 | Pass |  |  |  |
| 数据浏览器页面编辑超级表/普通表的Geometry 数据类型列 | 1. 可以增加长度 | Fail | [TD-30588](https://jira.taosdata.com:18080/browse/TD-30588) |  |  |
|  | 2. 成功修改压缩算法 | Fail | [TD-30588](https://jira.taosdata.com:18080/browse/TD-30588) |  |  |
|  | 3. 成功修改压缩等级 | Fail | [TD-30588](https://jira.taosdata.com:18080/browse/TD-30588) |  |  |
| 数据浏览器页面编辑超级表/普通表的varbinary 数据类型列 | 同上25-27 | Fail | [TD-30588](https://jira.taosdata.com:18080/browse/TD-30588) |  |  |
| 数据浏览器页面点击表查看数据 | 能够正确显示 geometry 和 varbinary 数据类型的值（结果同taos shell） | Pass |  |  |  |
| 数据浏览器页面输入sql查看数据 | 能够正确显示 geometry 和 varbinary 数据类型的值（结果同taos shell） | Pass |  |  |  |

### 8.2 可用性

- UI 是否美观
- 交互是否合理
- 是否存在错别字

### 8.3 可靠性

无

### 8.4 性能

无

### 8.5 安全性

无

### 8.6 兼容性

测试用例：
- TDengine 版本 3.1.0.0 以上版本, explorer 显示 Geometry 数据类型
测试3.1.0.3 版本通过
- TDengine 版本 3.1.1.0 以上版本, explorer 显示 Geometry 和 Varbinary 数据类型
测试3.1.1.16 版本通过
测试3.2.3.0 版本通过
- TDengine 版本 3.3.0.0 以上版本, explorer 显示 Geometry 和 VARBINARY 数据类型的编码算法，压缩算法和压缩级别
测试3.3.0.7 版本通过

### 8.7 本地化

测试用例：
- 点击切换语言按钮后，UI上的所有元素是否按照选择的语言，正确展示

## 9. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: [explorer Geometry/Varbinary], epic：taosx1.7.0
<!-- Unsupported block type: 999 -->

## 10. 参考文档 

[Explorer-支持 Geometry/Varbinary 数据类型](https://taosdata.feishu.cn/wiki/BtTQw7xQWi8SRIkc9QycuYabnUQ)
