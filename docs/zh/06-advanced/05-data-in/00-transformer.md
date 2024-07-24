---
title: "数据转换"
sidebar_label: "数据转换"
---

Transformer 是数据写入的核心，从数据源读取数据后，经过解析、提取拆分、数据过滤、映射，最终写入 TDengine 数据表中。taosX 将 Transformer 过程抽象为四个步骤：

1. **解析**：数据结构化的过程，就是将非结构化数据转成可以用统一 schema 描述的结构化数据。来自于 MQTT/Kafka 等数据源，其消息往往是普通的字符串，需要用一些格式化的方式来解析。但是数据源本身就是结构化数据，则无需解析过程。
2. **提取拆分**：部分字段进一步细化拆分的过程，比如数据源使用一个字段描述重量 “5千克”， 目标库使用两个字段来描述重量：weight、unit，则需要将源字段拆分。
3. **数据过滤**：设置过滤条件，满足条件的数据行才写入目标表。
4. **映射**：将经过上述过程的源字段映射到目标 TDengine 数据表字段。

## 数据转换过程详解

taosExplorer 中目前支持大部分数据源写入的 transformer 配置。接下来四个小节详细说明四个步骤**解析**、**提取拆分**、**数据过滤**、**映射**可视化配置方法。

### 1 解析

仅非结构化的数据源需要这个步骤，目前 MQTT 和 Kafka 数据源会使用这个步骤提供的规则来解析非结构化数据，以初步获取结构化数据，即可以以字段描述的行列数据。在 explorer 中您需要提供示例数据和解析规则，来预览解析出以表格呈现的结构化数据。

#### 1.1 示例数据

![示例数据](./pic/transform-01.png)

如图，textarea 输入框中就是示例数据，可以通过三种方式来获取示例数据：

1. 直接在 textarea 中输入示例数据；
2. 点击右侧按钮 “从服务器检索” 则从配置的服务器获取示例数据，并追加到示例数据 textarea 中；
3. 上传文件，将文件内容追加到示例数据 textarea 中。

#### 1.2 解析<a name="parse"></a>

解析就是通过解析规则，将非结构化字符串解析为结构化数据。消息体的解析规则目前支持 JSON、Regex 和 UDT。

##### 1. JSON 解析

如下 JSON 示例数据，可自动解析出字段：`groupid`、`voltage`、`current`、`ts`、`inuse`、`location`。

``` json
{"groupid": 170001, "voltage": "221V", "current": 12.3, "ts": "2023-12-18T22:12:00", "inuse": true, "location": "beijing.chaoyang.datun"}
{"groupid": 170001, "voltage": "220V", "current": 12.2, "ts": "2023-12-18T22:12:02", "inuse": true, "location": "beijing.chaoyang.datun"}
{"groupid": 170001, "voltage": "216V", "current": 12.5, "ts": "2023-12-18T22:12:04", "inuse": false, "location": "beijing.chaoyang.datun"}
```

如下嵌套结构的 JSON 数据，可自动解析出字段`groupid`、`data_voltage`、`data_current`、`ts`、`inuse`、`location_0_province`、`location_0_city`、`location_0_datun`，也可以选择要解析的字段，并设置解析的别名。

``` json
{"groupid": 170001, "data": { "voltage": "221V", "current": 12.3 }, "ts": "2023-12-18T22:12:00", "inuse": true, "location": [{"province": "beijing", "city":"chaoyang", "street": "datun"}]}
```

![JSON 解析](./pic/transform-02.png)

##### 2. Regex 正则表达式<a name="regex"></a>

可以使用正则表达式的**命名捕获组**从任何字符串（文本）字段中提取多个字段。如图所示，从 nginx 日志中提取访问ip、时间戳、访问的url等字段。

``` re
(?<ip>\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b)\s-\s-\s\[(?<ts>\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}\s\+\d{4})\]\s"(?<method>[A-Z]+)\s(?<url>[^\s"]+).*(?<status>\d{3})\s(?<length>\d+)
```

![Regex 解析](./pic/transform-03.png)

##### 3. UDT 自定义解析脚本

自定义 rhai 语法脚本解析输入数据（参考 `https://rhai.rs/book/` ），脚本目前仅支持 json 格式原始数据。

**输入**：脚本中可以使用参数 data, data 是原始数据 json 解析后的 Object Map；

**输出**：输出的数据必须是数组。

例如对于数据，一次上报三相电压值，分别入到三个子表中。则需要对这类数据做解析

``` json
{
    "ts": "2024-06-27 18:00:00", 
    "voltage": "220.1,220.3,221.1", 
    "dev_id": "8208891"
}
```

那么可以使用如下脚本来提取三个电压数据。

```
let v3 = data["voltage"].split(",");

[
#{"ts": data["ts"], "val": v3[0], "dev_id": data["dev_id"]},
#{"ts": data["ts"], "val": v3[1], "dev_id": data["dev_id"]},
#{"ts": data["ts"], "val": v3[2], "dev_id": data["dev_id"]}
]
```

最终解析结果如下所示：

![UDT](./pic/transform-udf.png)

### 2 提取或拆分

解析后的数据，可能还无法满足目标表的数据要求。比如智能表原始采集数据如下（ json 格式）：

``` json
{"groupid": 170001, "voltage": "221V", "current": 12.3, "ts": "2023-12-18T22:12:00", "inuse": true, "location": "beijing.chaoyang.datun"}
{"groupid": 170001, "voltage": "220V", "current": 12.2, "ts": "2023-12-18T22:12:02", "inuse": true, "location": "beijing.chaoyang.datun"}
{"groupid": 170001, "voltage": "216V", "current": 12.5, "ts": "2023-12-18T22:12:04", "inuse": false, "location": "beijing.chaoyang.datun"}
```

使用 json 规则解析出的电压是字符串表达的带单位形式，最终入库希望能使用 int 类型记录电压值和电流值，便于统计分析，此时就需要对电压进一步拆分；另外日期期望拆分为日期和时间入库。

如下图所示可以对源字段`ts`使用 split 规则拆分成日期和时间，对字段`voltage`使用 regex 提取出电压值和电压单位。split 规则需要设置**分隔符**和**拆分数量**，拆分后的字段命名规则为`{原字段名}_{顺序号}`，Regex 规则同解析过程中的一样，使用**命名捕获组**命名提取字段。

![拆分和提取](./pic/transform-04.png)

### 3 过滤<a name="filter"></a>

过滤功能可以设置过滤条件，满足条件的数据行 才会被写入目标表。过滤条件表达式的结果必须是 boolean 类型。在编写过滤条件前，必须确定 解析字段的类型，根据解析字段的类型，可以使用判断函数、比较操作符（`>`、`>=`、`<=`、`<`、`==`、`!=`）来判断。

#### 3.1 字段类型及转换

只有明确解析出的每个字段的类型，才能使用正确的语法做数据过滤。

使用 json 规则解析出的字段，按照属性值来自动设置类型：

1. bool 类型："inuse": true
2. int 类型："voltage": 220
3. float 类型："current" : 12.2
4. String 类型："location": "MX001"

使用 regex 规则解析的数据都是 string 类型。
使用 split 和 regex 提取或拆分的数据是 string 类型。

如果提取出的数据类型不是预期中的类型，可以做数据类型转换。常用的数据类型转换就是把字符串转换成为数值类型。支持的转换函数如下：

|Function|From type|To type|e.g.|
|:----|:----|:----|:----|
| parse_int  | string | int | parse_int("56")  // 结果为整数 56 |
| parse_float  | string | float | parse_float("12.3")  // 结果为浮点数 12.3 |

#### 3.2 判断表达式

不同的数据类型有各自判断表达式的写法。

##### 1. BOOL 类型

可以使用变量或者使用操作符`!`，比如对于字段 "inuse": true，可以编写以下表达式：

> 1. inuse
> 2. !inuse

##### 2. 数值类型（int/float）

数值类型支持使用比较操作符`==`、`!=`、`>`、`>=`、`<`、`<=`。

##### 3. 字符串类型

使用比较操作符，比较字符串。

字符串函数

|Function|Description|e.g.|
|:----|:----|:----|
| is_empty  | returns true if the string is empty | s.is_empty() |
| contains  | checks if a certain character or sub-string occurs in the string | s.contains("substring") |
| starts_with  | returns true if the string starts with a certain string | s.starts_with("prefix") |
| ends_with  | returns true if the string ends with a certain string | s.ends_with("suffix") |
| len  | returns the number of characters (not number of bytes) in the string，must be used with comparison operator | s.len == 5 判断字符串长度是否为5；len作为属性返回 int ，和前四个函数有区别，前四个直接返回 bool。 |

##### 4. 复合表达式

多个判断表达式，可以使用逻辑操作符(&&、||、!)来组合。
比如下面的表达式表示获取北京市安装的并且电压值大于 200 的智能表数据。

> location.starts_with("beijing") && voltage > 200

### 4 映射

映射是将解析、提取、拆分的**源字段**对应到**目标表字段**，可以直接对应，也可以通过一些规则计算后再映射到目标表。

#### 4.1 选择目标超级表

选择目标超级表后，会加载出超级表所有的 tags 和 columns。
源字段根据名称自动使用 mapping 规则映射到目标超级表的 tag 和 column。
例如有如下解析、提取、拆分后的预览数据：

#### 4.2 映射规则 <a name="expression"></a>

支持的映射规则如下表所示：

|rule|description|
|:----|:----|
| mapping | 直接映射，需要选择映射源字段。|
| value | 常量，可以输入字符串常量，也可以是数值常量，输入的常量值直接入库。|
| generator | 生成器，目前仅支持时间戳生成器 now，入库时会将当前时间入库。|
| join | 字符串连接器，可指定连接字符拼接选择的多个源字段。|
| format | **字符串格式化工具**，填写格式化字符串，比如有三个源字段 year, month, day 分别表示年月日，入库希望以yyyy-MM-dd的日期格式入库，则可以提供格式化字符串为 `${year}-${month}-${day}`。其中`${}`作为占位符，占位符中可以是一个源字段，也可以是 string 类型字段的函数处理|
| sum | 选择多个数值型字段做加法计算。|
| expr | **数值运算表达式**，可以对数值型字段做更加复杂的函数处理和数学运算。|

##### 1. format 中支持的字符串处理函数

|Function|description|e.g.|
|:----|:----|:----|
| pad(len, pad_chars) | pads the string with a character or a string to at least a specified length | "1.2".pad(5, '0') // 结果为"1.200" |
|trim|trims the string of whitespace at the beginning and end|"  abc ee ".trim() // 结果为"abc ee"|
|sub_string(start_pos, len)|extracts a sub-string，两个参数：<br />1. start position, counting from end if < 0<br />2. (optional) number of characters to extract, none if ≤ 0, to end if omitted|"012345678".sub_string(5)  // "5678"<br />"012345678".sub_string(5, 2)  // "56"<br />"012345678".sub_string(-2)  // "78"|
|replace(substring, replacement)|replaces a sub-string with another|"012345678".replace("012", "abc") // "abc345678"|

##### 2. expr 数学计算表达式

基本数学运算支持加`+`、减`-`、乘`*`、除`/`。

比如数据源采集数值以设置度为单位，目标库存储华氏度温度值。那么就需要对采集的温度数据做转换。

解析的源字段为`temperature`，则需要使用表达式 `temperature * 1.8 + 32`。

数值表达式中也支持使用数学函数，可用的数学函数如下表所示：

|Function|description|e.g.|
|:----|:----|:----|
|sin、cos、tan、sinh、cosh|Trigonometry|a.sin()   |
|asin、acos、atan、 asinh、acosh|arc-trigonometry|a.asin()|
|sqrt|Square root|a.sqrt()  // 4.sqrt() == 2|
|exp|Exponential|a.exp()|
|ln、log|Logarithmic|a.ln()   // e.ln()  == 1<br />a.log()  // 10.log() == 1|
|floor、ceiling、round、int、fraction|rounding|a.floor() // (4.2).floor() == 4<br />a.ceiling() // (4.2).ceiling() == 5<br />a.round() // (4.2).round() == 4<br />a.int() // (4.2).int() == 4<br />a.fraction() // (4.2).fraction() == 0.2|

#### 4.3 子表名映射

子表名类型为字符串，可以使用映射规则中的字符串格式化 format 表达式定义子表名。
