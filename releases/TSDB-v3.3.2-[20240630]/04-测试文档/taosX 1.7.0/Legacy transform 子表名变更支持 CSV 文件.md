# Legacy transform 子表名变更支持 CSV 文件

## 1. 测试目标

这里用于描述本需求主要的测试目标
- Legacy 数据迁移支持 -T 参数配置csv文件方式进行子表名变更
  TS-5040

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024-06-25 | 0.1 | @贾晨阳 |  |
|  |  |  |  |

## 3. 测试范围

这里用于描述本需求的覆盖范围：
- 通过配置csv文件，实现自定义子表重命名

## 4. 测试结论

本次测试验证并通过了以下内容：
- 命令行方式下， Legacy 数据迁移支持 -T 参数配置csv文件方式进行子表名变更

## 5. 开发质量报告

结论：本特性/优化的开发质量是优

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 |  |
| 基础测试用例不通过 |  |
| Bug 总数 |  |
| 严重 Bug 总数 |  |

## 6. 已知问题和限制

- 只在命令行模式下验证
- 由于2.6上show tables存在问题且无法修复，本次测试不进行2.6-3.0迁移的验证

## 7. 测试环境

- OS: Windows, Linux, macOS
- Browser: Chrome

## 8. 测试数据 (Optional)

在TDengine 3.x中创建数据库，子表名为d1～d10

## 9. 测试用例

### 9.1 功能

在提测时，开发应保证基础用例全部通过。

| 类型 | 测试目的 | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | JIRA | 备注 |
| --- | --- | --- | --- | --- | --- | --- | --- |
|  | 验证全部transform子表重命名生效 | 配置csv文件：
d0,t0
d1,t1
d2,t2
d3,t3
d4,t4
d5,t5
d6,t6
d7,t7
d8,t8
d9,t9
所有子表均进行重命名 | 目标库中所有子表均重命名 |  | Pass |  |  |
|  | 验证部分子表重命名生效 | 配置csv文件：
d0,t0
d1,t1
d2,t2
d3,t3
d4,t4
其余子表不配置transform | 所有子表都会被迁移，只有csv文件中配置的子表被重命名 |  | Pass |  |  |
|  |  | 配置csv文件：
d0,d0
d1,d1
d2,d2
d3,d3
d4,d4
d5,t5
d6,t6
d7,t7
d8,t8
d9,t9
部分子表名未改变 | 所有子表都会被迁移，满足csv文件中配置重命名规则 |  | Pass |  |  |
|  |  | 重命名后的子表包含大小写、数字、特殊字符 | 可正常迁移 |  | Pass |  |  |
|  | 验证csv文件异常时的处理 | 配置csv文件：
d0,t0
d1,t1
d2,t2
d3,t3
d4,t4
d5,t5
d6,t6
d7,t7
d8,t8
d9
格式不满足要求 | 程序报错退出 |  | Pass |  | error: invalid value 'rename-child-table:map:@/data/cyjia_root/cyjia/2.6/map_wrong.csv' for '--transform <TRANSFORM>': Rename parse error: Invalid csv content, expect `old,new` pair, but got `d9` |
|  |  | 配置不存在的csv文件 | 程序报错退出 |  | Pass |  | error: invalid value 'rename-child-table:map:@/data/cyjia_root/cyjia/2.6/map_wron.csv' for '--transform <TRANSFORM>': Rename parse error: Invalid csv input: No such file or directory (os error 2) |

### 9.2 可用性

测试用例包括但不局限于：
- UI是否美观？
- 交互是否合理？
- 字体、字号是否合适？
- 是否存在错别字？

### 9.3 可靠性

这里用于描述稳定性测试相关的内容。

### 9.4 性能

无

### 9.5 安全性

该项修改不涉及安全性。

### 9.6 兼容性

### 9.7 本地化

## 10. 待讨论(Optional)

## 11. Jira


## 12. 测试计划 (Optional)


## 13. 测试备忘 (Optional)

使用的命令行参数格式：
```shell {wrap}
taosx run -f "taos://192.168.1.40:6030/cyjia?libraryPath=/data/cyjia_root/cyjia/2.6/libtaos.so.2.6.0.27&configDir=/data/cyjia_root/cyjia/2.6" -t "taos:///cyjia?assert" -T rename-child-table:map:@/data/cyjia_root/cyjia/2.6/mapfull.csv
```


## 14. 参考文档 (Optional)

这里用于添加对该需求测试有帮助的文档链接：
- [数据迁移 Transform：子表名变更支持 CSV 文件](https://taosdata.feishu.cn/wiki/MMuSwBRkBiS0BjkE0QUcep8nnof)
