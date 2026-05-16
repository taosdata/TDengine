# Interval支持更多时间类型

@王加明请更新新的语法说明和提交官网文档说明 请更新新的语法说明和提交官网文档说明
- interval/sliding原有语法不变.增加了以下两个新语法.
- interval/sliding内可以不带单位, 使用库的默认单位, 如ms库, `interval(1000)`即1s一个窗口.
- interval/sliding内也可以用单引号或者双引号的字符串, 如`interval("1s")`, `interval('1s')`,引号内必须带单位, 如inteval('1')是错误的. 引号内除了单位以外, 其他`必须`是数字, 不能有其他空格. 如interval(' 1s ')是错误的.
