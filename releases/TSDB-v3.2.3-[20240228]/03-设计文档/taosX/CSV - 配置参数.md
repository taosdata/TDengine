# CSV - 配置参数

## 1. 背景

目前 csv data in task 的配置，没有 csv 相关的技术参数配置，默认将逗号`,`作为字段间的分隔符，把`"`作为quotaChar。
陶总提出需要对这些技术参数可配置，增强 csv 处理能力的通用性，详见文档[UI Design for Data In ](https://taosdata.feishu.cn/docx/FEPfdx4dbot5xRxHbFlcjYVZn9G) 。

Jira 列表
[TD-28569：CSV - 增加 csv 特有参数 skipRows、delimiter、 quoteChar、commentPrefix](https://jira.taosdata.com:18080/browse/TD-28569)

## 2. 定义

skipRows: number of rows to be skipped in the file.
Delimiter: Character used to delimit columns.
quoteChar: Character used to quote values containing the delimiter.
commentPrefix: String prefix to identify a comment. Always added to annotations.

## 3. 变更历史

| 日期 | 版本 | 撰写人 |
| --- | --- | --- |
| 2024-02-04 | 0.1 | 周营昭 |

## 4. 行为说明

### 4.1 Explorer UI

CSV选项部分修改后如下：
![](./images/img_LYDQbOceWovBWGxmJgEcHGrVnCh.png)

1. 将原 “上传 CSV 文件 / 配置 CSV 地址” 区域的 “包含 Header” 参数移动到 “CSV 选项” 区域
2. 删除 “自定义列” 参数，不包含Header情况下，列名根据数据首行的 split 列数量，默认命名为 c0, c1, ..., c(n-1) 。

![](./images/img_J2rrbVtOAoEzadxi0jxccpa9n2g.png)


增加文件地址的填写说明。
![](./images/img_X1aebG8kyomIxbxXLMhcXjScnYc.png)

“上传 CSV 文件 / 配置 CSV 地址” 区域的 “下一步” 按钮修改为“解析”按钮，调用接口 `api/x/filemeta` 时需要增加 Deliliter Char、Quote Char、Comment Prefix 三个参数。

### 4.2 参数行为说明

1. 包含 Header
解析文件中的第一行，将首行解析出的 value, 作为 column 字段名；默认不包含。
1. 忽略前 N 行
如果 “包含 Header”，则跳过 header 后，再跳过N行来解析数据；默认为0。
如果不 “包含 Header”，则直接跳过 N 行后再解析数据。
其中注释行不包括在 N 行范围内。
1. Delimiter
字段间的分割符，默认为`,`。
1. Quote Char
如果某一个列值中包含分隔符，则使用 quote char 来包括整列。
1. Comment prefix
注释行的前缀符号，以 CommentPrefix 开头的行，被认为是注释行，将被忽略。

### 4.3 taosx 修改

#### 4.3.1 修改接口参数

修改 filemeta 接口参数实体类 FileMetaRequest 的结构，增加 delimiter/quote/comment 三个参数。

#### 4.3.2 修改读 CSV 文件功能 

读 CSV 文件时需要显式设置 delimiter/quote/comment 三个属性，如果参数为空，则分别使用以下默认值：
1. delimiter: , (half width comma)
2. quote: " (half width double quote)
3. comment: # (hash symbol) 

## 5. 性能

无。

## 6. 兼容性

~~需要按照默认值，兼容没有配置 “csv选项” 的旧有配置。~~
taosX 和 Explorer 必须同时升级到相同版本，可兼容历史配置数据。

## 7. 运维

无。

## 8. 使用场景

csv 文件使用非默认值`,`作为分隔符的场景。

## 9. 常见错误和排查

无。
