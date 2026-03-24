# Interval 窗口支持插值时间范围 TS

## 1. 修订记录

| 编写日期 | 发布日期 | 版本 | 修订人 | 主要修改内容 |
| --- | --- | --- | --- | --- |
| 2026-01-28 |  | 0.1 | 张天毅 | 创建测试文档 |
| 2026-02-03 | 2026-02-05 | 1.0 | 张天毅 | 更新测试结果 |

## 2. 测试目标

测试surround子句功能正确性。

## 3. 参考文档

[Interval 窗口支持插值时间范围 FS](https://taosdata.feishu.cn/wiki/FfdTwAjHHi7q3vkYRXGcjxbanzf)

## 4. 测试结论

测试通过，行为与设计文档一致

## 5. 功能测试

### 5.1 Interp fill surround

#### 5.1.1 测试要点

注意兼容旧的range interval语法

#### 5.1.2 用例列表

| 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- |
| test_interp_fill_surround | 测试interp+fill(prev/next/near) surround在不同数据源下的功能：普通表、超级表、超级表按tbname分组、超级表按tag分组 | 通过 |
| test_interp_fill_surround_stream | 测试流计算中interp+fill(prev/next/near) surround from不同数据源：%%trows、普通表、超级表 | 通过 |
| test_interp_fill_surround_abnormal | 测试异常语法： - fill_values与interp数目不一致 - 新旧语法混用 - Surrounding time value格式、单位、数值问题 - fill_values使用子查询、格式非法 - 其他fill模式与surround子句混用 | 通过 |

### 5.2 Interval fill surround

#### 5.2.1 用例列表

| 测试用例 | 测试描述 | 测试结果 |
| --- | --- | --- |
| check_surround_normal | 测试interval+fill(prev/next) surround在不同场景中的功能： - 普通表fill(prev/next)，interval输出正序、倒序数据 - 超级表按tbname分组，select list中包含tbname - 多列数据分别fill，各自判断使用表中数据还是fill_values进行fill - 主键列为fist(ts)而非_wstart伪列 - 查询较多数据时，使用下（上）一个datablock数据进行fill | 通过 |
| check_surround_stream | 测试流计算中interval+fill(prev/next) surround from 不同数据源：普通表、超级表、%%tbname | 通过 |
| check_surround_abnormal | 与interp中的异常语法类似，额外： - interval不支持near模式 | 通过 |

## 6. 性能测试

几乎没有性能下降

## 7. 兼容性测试

兼容

## 8. 已知问题和限制（可选）
