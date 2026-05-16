# [Test Report] TD-19291 Schemaless table name can be composed based on rule

### 1. 概述：

当前schemaless写入数据时，如不指定子表名，默认通过md5规则生成；或通过在taos.cfg配置smlChildTableName参数指定子表名的key，在schemaless写入数据中，每条数据需要指定子表名的值，
为了能够更友好地支持动态根据tag生成表名，建议新增一个配置项smlAutoChildTableNameDelimiter，
smlChildTableNameRule=$tag1-$tag2-$tag3 ，其中的 tag1, tag2, tag3在使用时替换为实际数据中的tag。tag与tag之间的分隔符可以hard-code为 "-"
优化之后的收益：用户不需要在每条记录中附带子表名的tag，只需要在 taosc.cfg 中配置即可

### 2. 测试环境：

192.168.1.35：
CPU: Intel(R) Xeon(R) CPU E5-2630 v2 @ 2.60GHz （2）24核
Mem: DDR3  32 GB * 2
Disk: 2792GB

### 3. 测试用例：

| 用例名称 | 用例描述 | 期望结果 | 测试结果 |
| --- | --- | --- | --- |
| 创建子表名带正常分隔符 | 1. 安装部署最新代码的TDengine 1. 配置taos.cfg，smlChildTableName为”dataModelName“，smlAutoChildTableNameDelimiter为”-“ 1. schemaless写入数据，数据标签字符串带引号，标签总长度不超过192 1. schemaless写入数据，数据标签字符串带引号，标签总长度不超过192，且数据中dataModelName值为“ttt” 1. schemaless写入数据，标签值中增加类型且带符号“.”， 如f.64 1. schemaless写入数据，标签值等于子表名最大长度192 1. schemaless写入数据，单个标签值长度超过192 1. schemaless写入数据，多个标签值长度超过192 1. 使用分割符如”_“、”:“, 重复以上步骤 1. 使用特殊分隔符“&”，重复以上步骤 | 1. TDengine安装部署正常 1. 配置文件参数设置正常 1. 子表名中对“进行转义处理 1. 子表名会将标签值及dataModelName值通过分隔符连接 1. 子表名中将"."转换成”_“ 1. 子表名按照分隔符规则生成 1. 子表名使用md5规则生成 1. 子表名使用md5规则生成 1. 测试结果与以上保持一致 1. 测试结果与以上保持一致 | pass |

### 4. 总结：

1. 新增加的“smlAutoChildTableNameDelimiter”配置项生成子表名符合预期
2. 当同时存在”smlChildTableName“和“smlAutoChildTableNameDelimiter”配置项时，子表名称会遵循连接符规则，同时将数据中“smlChildTableName”的值作为标签值，应用到子表名中
**Note：**
1. 如用户在使用之前的规则，就不要使用新规则配置，新规则优先级更高，会将数据写到新命名的子表中。
2. 对特殊字符“@”、“#”、空格，不能用做分隔符，需要在说明文档中提示用户
