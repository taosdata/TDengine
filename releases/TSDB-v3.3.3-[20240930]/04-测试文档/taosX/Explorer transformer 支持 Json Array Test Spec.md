# Explorer transformer 支持 Json Array Test Spec

## 1. 测试目标

验证 Explorer transformer 中 Json array 的支持和格式限制。

## 2. 变更历史

| Date | Version | Owner | Memo |
| --- | --- | --- | --- |
| 2024.7.15 | v0.0 | @贾晨阳 |  |
| 2024.7.17 | v1.0 | @贾晨阳 | 依据营昭的意见修改测试用例的预期结果 |

## 3. 测试范围

这里用于描述本需求的覆盖范围：
1. 可以输入一个或多个 json array 或者一个或多个 json object，但不能出现 json object 和 json array 混杂的情况；
2. 多个 json object 结构可以是异构的，最终解析结果为各个 json object 属性的并集；
3. 同一个 json array 中的 json object 或者多个 json array 中的 json obejct 可以是异构的，最终解析结果为各个 json object 子元素属性的并集；
4. 多个 json item 间以不可见字符分割，不可以用逗号、分号等分割。

## 4. 测试结论

本次测试对第3章节中的需求进行的验证，验证全部通过。

## 5. 开发质量报告

结论：本优化的开发质量是优

| 统计指标 | 数量 |
| --- | --- |
| 提测被拒次数 | 0 |
| 基础测试用例不通过 | 0 |
| Bug 总数 | 0 |
| 严重 Bug 总数 | 0 |

## 6. 已知问题和限制

## 7. 测试环境

- OS: Linux
- Browser: Chrome

## 8. 测试数据 (Optional)

无

## 9. 测试用例

### 9.1 功能

在提测时，开发应保证基础用例全部通过。
| 类型 | 测试目的 | 测试步骤 | 预期结果 | 是否为基础用例 | 测试结果 | 备注 |
| --- | --- | --- | --- | --- | --- | --- |
| 正常用例 | 验证正确/常用格式的json字符串 | 输入示例数据：
{"a": 1, "b": "vb1"} | 正确解析示例数据 |  | Pass |  |
|  |  | 输入示例数据：
{"a": 1, "b": "vb1"}{"a": 2, "b": "vb2"} | 正确解析示例数据 |  | Pass |  |
|  |  | 输入示例数据：
{"a": 1, "b": "vb1", "c": "vc1"}
{"a": 2, "b": "vb2"}
{"a": 3, "b": "vb3", "d": "vd3"} | 正确解析示例数据，解析后内容为示例数据中所有object的并集 |  | Pass |  |
|  |  | 输入示例数据：
{"a": 1, "b": "vb1"}\空格\{"a": 2, "b": "vb2"} | 正确解析示例数据 |  | Pass |  |
|  |  | 输入示例数据：
{"a": 1, "b": "vb1"}\回车\
{"a": 2, "b": "vb2"} | 正确解析示例数据 |  | Pass |  |
|  |  | 输入示例数据：
{
    "a": 1, 
    "b": "vb1"
}
{
    "a": 2, 
    "b": "vb2"
}{
    "a": 3, 
    "b": "vb3"
} | 正确解析示例数据 |  | Pass |  |
|  |  | 输入示例数据：
{
  "a": 1,
  "b": [
    {
      "b1": 2
    },
    {
      "b2": 3
    }
  ]
} | 正确解析数据 |  | Pass |  |
|  | 验证正确/常用格式的json array | 输入示例数据：
[{"a": 1, "b": "vb1"}] | 正确解析示例数据 |  | Pass |  |
|  |  | 输入示例数据：
[{"a": 1, "b": "vb1"},{"a": 2, "b": "vb2"}] | 正确解析示例数据 |  | Pass |  |
|  |  | 输入示例数据：
[{"a": 1, "b": "vb1", "c": "vc1"},
{"a": 2, "b": "vb2"},
{"a": 3, "b": "vb3", "d": "vd3"}] | 正确解析示例数据，解析后内容为示例数据中所有object的并集 |  | Pass |  |
| 异常用例 | 异常示例数据测试：json array中每个object用空格分隔 | 输入示例数据：
[{"a": 1, "b": "vb1", "c": "vc1"}\空格\{"a": 2, "b": "vb2"}\空格\{"a": 3, "b": "vb3", "d": "vd3"}] | 解析时提示错误，错误信息正确 |  | Pass | 错误提示：第 1 条示例数据不是有效的 JSON 格式：SyntaxError: Expected ',' or ']' after array element in JSON at position 34 (line 2 column 1) |
|  | 异常示例数据测试：json array中每个object用回车分隔 | 输入示例数据：
[
  {"a": 1, "b": "vb1", "c": "vc1"}
  {"a": 2, "b": "vb2"}
  {"a": 3, "b": "vb3", "d": "vd3"}
  ] | 解析时提示错误，错误信息正确 |  | Pass | 错误提示：第 1 条示例数据不是有效的 JSON 格式：SyntaxError: Expected ',' or ']' after array element in JSON at position 34 (line 2 column 1) |
|  | 异常示例数据测试：输入json object和json object混合的数据 | 输入示例数据：
[{"a": 1, "b": "vb1"}]
{"a": 1, "b": "vb1"} | 解析时提示错误，错误信息正确 |  | Pass | 错误提示：请输入正确 JSON 格式 |
|  | 异常示例数据测试：json array的数据之间使用了逗号","或分号";"进行分隔 | 输入示例数据：
[{"a": 1, "b": "vb1", "c": "vc1"}],[
  {"a": 2, "b": "vb2"}]
 使用逗号分隔json array中的object | 解析时提示错误，错误信息正确 |  | Pass | 第 1 条示例数据不是有效的 JSON 格式：SyntaxError: Unexpected non-whitespace character after JSON at position 34 (line 1 column 35) |
|  |  | 输入示例数据：
[{"a": 1, "b": "vb1", "c": "vc1"}]；
[ {"a": 2, "b": "vb2"}]
 使用分号分隔json array中的object | 解析时提示错误，错误信息正确 |  | Pass | 错误提示：第 1 条示例数据不是有效的 JSON 格式：SyntaxError: Unexpected non-whitespace character after JSON at position 34 (line 1 column 35) |
|  | 异常示例数据测试：消息体结构不完整 | 使用非{}或[]结构的数据 | 解析时提示错误，错误信息正确 |  | Pass | 错误提示：请输入正确 JSON 格式 |

### 9.2 可用性

### 9.3 可靠性

无 

### 9.4 性能

无

### 9.5 安全性

无

### 9.6 兼容性

无

### 9.7 本地化

无

## 10. 待讨论(Optional)

## 11. Jira

无

## 12. 测试计划 (Optional)

无

## 13. 风险评估

无

## 14. 测试备忘 (Optional)

这里用于记录测试过程中发现的，与产品行为相关的一些重要信息。

## 15. 参考文档 (Optional)

这里用于添加对该需求测试有帮助的文档链接：
- [Explorer transformer 支持 Json Array](https://taosdata.feishu.cn/wiki/QED9wVBO0iJ47IklbAIcUfS2nIh)
