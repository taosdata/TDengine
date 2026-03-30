# Explorer transformer 支持 Json Array

## 1. 背景

为了增加 json 数据的适应性，Explorer 前端增加对 json array 的支持。

TD-30416

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/07/12 | 0.1 | 周营昭 | 初稿 |
| 2024/7/15 | 0.2 | 周营昭 | 基于 Wade Review 意见修改 |

## 3. 定义

无。

## 4. 行为说明

### 4.1 JSON 规则约束

![](./images/img_IEeQbBVvPoU9XOxdL1dccQNNneb.png)

如上图所示，在 json 输入数据时， 支持输入的 json 数据约束如下：
1. 可以输入一个或多个 json array 或者一个或多个 json object，但不能出现 json object 和 json array 混杂的情况；
2. 多个 json object 结构可以是异构的，最终解析结果为各个 json object 属性的并集；
3. 同一个 json array 中的 json object 或者多个 json array 中的 json obejct 可以是异构的，最终解析结果为各个 json object 子元素属性的并集；
4. 多个 json item 间以不可见字符分割，不可以用逗号、分号等分割。

### 4.2 非法输入示例

```json
// 错误1： json object 和 json array 混杂输入
{"a": 1, "b": "vb1"}
[{"a": 2, "b": "vb2"},{"a": 3, "b": "vb3"}]
```

```json
// 错误2： json array 格式不正确
[{"a": 1, "b": "vb1"}]
[{"a": 2, "b": "vb2"}{"a": 3, "b": "vb3"}]
```

```json
// 错误3： 多个 json object 之间用可见字符分割
{"a": 1, "b": "vb1"},
{"a": 2, "b": "vb2"},
{"a": 3, "b": "vb3"}
```

### 4.3 正确输入示例

```json
// 多个 json object 之间可以用换行符、空格分隔，也可以相连
{"a": 1, "b": "vb1"}
{"a": 2, "b": "vb2"}{"a": 3, "b": "vb3"}
```

```json
// 多个 json object 结构可以不一致，最终解析出的属性是所有属性的并集
{"a": 1, "b": "vb1", "c": "vc1"}
{"a": 2, "b": "vb2"}
{"a": 3, "b": "vb3", "d": "vd3"}
```

```json
// 每一个object, 可以是格式化的
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
}
```

```json
// json array
[{"a": 1, "b": "vb1"}]
[{"a": 2, "b": "vb2"}, {"a": 3, "b": "vb3"}]
```

### 4.4 常见错误和提示信息

1. 不是以`{`开头，以`}`结尾，或者不是以`[`开头，以`]`结尾
中文提示消息：请输入正确 JSON 格式；
英文提示消息：Please enter the correct JSON format
1. 按照规则分割为多个 json，尝试解析，如果解析失败则会提示对应的语法错误：
中文提示：
第 2 条示例数据不是有效的 JSON 格式：SyntaxError: Expected double-quoted property name in JSON at position 15 (line 1 column 16)
英文提示：
The json string at pos `2` is not a valid json string: SyntaxError: Expected double-quoted property name in JSON at position 15 (line 1 column 16)

## 5. 性能

无。

## 6. 兼容性

无。

## 7. 运维

无。

## 8. 使用场景

Mqtt, kafka 输入示例数据，预览 json 解析结果。

## 9. 约束和限制

无。

## 10. 常见错误和排查

无。

## 11. 可观测性

无。

## 12. 安装和卸载

无。

## 13. 文档

无。

## 14. 参考文档

无。

## 15. 附录

无。
