# Explorer 建表：支持复合主键和压缩增强 Test spec 

## 1. 测试目标

- 验证explorer上的建表，支持复合主键和压缩增强

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.04.12 | 0.1 | 聂敏慧 | Initial Draft |

## 3. 测试范围

本需求的覆盖范围：
- 数据浏览器页面创建超级表/普通表
- 数据浏览器页面查看超级表/普通表配置信息
- 数据浏览器页面编辑超级表/普通表配置
- Datain 支持 transformer 的数据源配置数据映射时创建超级表

## 4. 测试结论

- 在数据浏览器页面创建超级表/普通表，测试通过
- 在数据浏览器页面查看超级表/普通表配置信息，测试通过
- 在数据浏览器页面编辑超级表/普通表配置，遗留问题见第6节
- 在Datain 页面 transformer 的数据源配置数据映射时创建超级表，测试通过。超级表的显示正常。

## 5. 开发质量报告

结论：本特性/优化的开发质量是优

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 4 |
| 严重 Bug 总数 | 0 |

## 6. 已知问题和限制

- 受jira()的影响, 在编辑超级表/普通表时，增加列时，编码方法，压缩算法和压缩级别需要置空。
  TD-30006


## 7. 测试环境

- OS: Windows, Linux
- Browser: Chrome

## 8. 测试数据 

涉及数据类型：int, int unsigned, bigint, bigint unsigned, samllint, smallint unsigned, tinyint, tinyint unsigned, float, double,bool, varchar, nchar 

## 9. 测试用例

### 9.1 功能

| Description | Expected Results | Result | Jira | Automated | Memo |
| --- | --- | --- | --- | --- | --- |
| [basic]数据浏览器页面创建超级表和普通表 | 对于普通列，encode，compress, level使用默认值，能创建超级表/普通表 | Pass |  |  |  |
| 数据浏览器页面创建超级表 | 1. 第二列是int，bigint, int unsigned, bigint unsigned, varchar时，可以选择为复合主键选择，成功创建表
1. encode 对于不同类型显示不同默认值和可选编码算法
2. 选择encode算法, 成功创建表
3. compress 对于不同类型显示不同默认值和可选压缩算法
4. 选择不同的compress算法，成功创建表
5. level，默认显示 medium, 使用默认值成功创建表
6. 选择不同的level， 成功创建表 | Pass |  |  |  |
| 数据浏览器页面创建普通表 | 同上 | Pass |  |  |  |
| Datain数据源配置数据映射时创建超级表 | 1. 同上
1. 复合主键有提示，非空 | Pass | [https://jira.taosdata.com:18080/browse/TD-30222?jql=text%20~%20%22explorer%20%E5%BB%BA%E8%A1%A8%22%20and%20creator%20%3D%20Mia%20](https://jira.taosdata.com:18080/browse/TD-30222?jql=text ~ "explorer 建表" and creator = Mia ) |  |  |
| 数据浏览器页面查看超级表信息 | 1.表结构每个数据列信息中有encode, compress,level信息
2.表结构信息展示复合主键列 | Pass |  |  |  |
| 数据浏览器页面查看普通表信息 | 同上 | Pass |  |  |  |
| 数据浏览器页面编辑超级表信息 | 1. 复合主键列只可以修改压缩算法
1. 其他列可以修改 | Pass |  |  |  |

### 9.2 可用性

- UI 是否美观
- 交互是否合理
- 是否存在错别字
- 格式化显示时间

### 9.3 可靠性

无。

### 9.4 性能

- 无

### 9.5 安全性

无。

### 9.6 兼容性

- TDengine 版本 < 3.3.0.0, explorer 保持原 UI

### 9.7 本地化

- 点击切换语言按钮后，UI上的所有元素是否按照选择的语言，正确展示

## 10. 问题

无

## 11. Jira

此feature相关的所有Jira, 标题中应包含统一的标签: [explorer 建表], epic：taosx1.7.0
<!-- Unsupported block type: 999 -->

## 12. 测试备忘 

1. int, int unsigned, bigint, bigint unsigned , varchar 类型可以作为复合主键
2. 各类型的可选编码算法，可选压缩算法见文档[可配置存储压缩-Function Spec](https://taosdata.feishu.cn/wiki/St4WwSX5Ei3VfMk3yMUcv2DMnMh)中3.3节

## 13. 参考文档 

[Explorer 建表：支持复合主键和压缩增强](https://taosdata.feishu.cn/wiki/GePgw73JliRI4qklV7Vc32EMnBf)
[可配置存储压缩-Function Spec](https://taosdata.feishu.cn/wiki/St4WwSX5Ei3VfMk3yMUcv2DMnMh)
