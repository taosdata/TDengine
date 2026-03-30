# taosdump 支持 decimal/blob/stmt2 - TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-02-28 | YYYY-MM-DD | 1.0 | 裴亚明 | 初始版本 |

## 2. 测试目标

本测试报告主要测试目标包括：
- **DECIMAL 数据类型支持**：验证 taosdump 能够正确导出和导入 DECIMAL类型数据
- **BLOB 数据类型支持**：验证 taosdump 能够正确导出和导入 BLOB 类型数据
- **Stmt2 数据导入**：验证 taosdump 使用 stmt2 接口进行数据导入的功能正确性

## 3. 参考文档

[STMT2支持decimal类型写入-FS](https://taosdata.feishu.cn/wiki/P2zFw4469ikfTvk6xRJcfAKinHd)
[BLOB FS](https://taosdata.feishu.cn/wiki/U2F7wkwjxizN85k73AQcne8PnQb)

## 4. 测试结论

实现的 taosdump 支持 DECIMAL/BLOB 数据类型和 stmt2 数据导入功能已按照预期工作。

## 5. 测试环境

- OS:  Ubuntu 24.04 LTS
- CPU: x86_64
- 内存: 8GB
- 磁盘: SSD 60 GB

## 6. 功能测试

### 6.1 **DECIMAL 数据类型支持**

#### 6.1.1 测试要点

- 支持 DECIMAL(30, 16) 高精度小数类型
- 支持 DECIMAL(16, 10) 标准小数类型
- 验证正数、负数、NULL 值的正确处理
- 验证小数精度在导出导入后不丢失
- WebSocket 模式跳过测试（已知限制）

#### 6.1.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | DECIMAL 正数测试 | 插入并验证高精度正小数 98765432109876.1234567890123456 | 通过 |
| 2 | DECIMAL 负数测试 | 插入并验证负小数 -56789.1234567890 | 通过 |
| 3 | DECIMAL NULL 测试 | 验证 NULL 值的正确导出导入 | 通过 |
| 4 | DECIMAL 精度保持 | 验证 DECIMAL(30,16) 精度不丢失 | 通过 |
| 5 | 数据一致性验证 | 对比导出前后数据一致性 | 通过 |

### 6.2 **BLOB 数据类型支持**

#### 6.2.1 测试要点

- 支持 BLOB 二进制大对象类型
- 验证普通字符串数据的正确处理
- 验证含空字节(\x00)的二进制数据处理
- 验证 NULL 值的处理
- WebSocket 模式跳过测试（已知限制）

#### 6.2.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | BLOB 字符串测试 | 验证普通字符串 'abc' 的导出导入 | 通过 |
| 2 | BLOB 二进制测试 | 验证含空字节数据 '\x61620063' 的处理 | 通过 |
| 3 | BLOB NULL 测试 | 验证 BLOB 字段 NULL 值处理 | 通过 |
| 4 | 数据一致性验证 | 对比导出前后数据一致性 | 通过 |

### 6.3 **Stmt2 数据导入**

#### 6.3.1 测试要点

- 验证 taosdump 使用 stmt2 接口进行数据导入
- 验证各种数据类型的正确绑定

#### 6.3.2 用例列表

| # | 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- | --- |
| 1 | stmt2 基础导入 | 验证基础数据类型数据导入 | 通过 |
| 2 | DECIMAL 绑定导入 | 验证 DECIMAL 类型 stmt2 绑定导入 | 通过 |
| 3 | BLOB 绑定导入 | 验证 BLOB 类型 stmt2 绑定导入 | 通过 |
| 4 | 多类型混合 | 验证多种数据类型混合导入 | 通过 |

## 7. 性能测试

使用 taosBenchmark 生成 1亿条 meters 表数据，并将数据库数据导出到文件夹 tmp 中，分别测试使用 stmt v1 和 v2 导入的效率。
1. stmt v1 导入数据耗时
```shell
time taosdump_stmtv1 -W "test=test1" -i ./tmp

real    1m12.119s
user    3m24.016s
sys     0m17.820s
```

1. stmt v2 导入数据耗时
```shell
time taosdump_stmtv2 -W "test=test2" -i ./tmp

real    1m12.110s
user    3m17.617s
sys     0m16.639s
```

stmt2 接口导入数据的性能与 stmt1 接口无显著差异。

## 8. 已知问题和限制

WebSocket 连接模式暂不支持 DECIMAL 和 BLOB 类型
