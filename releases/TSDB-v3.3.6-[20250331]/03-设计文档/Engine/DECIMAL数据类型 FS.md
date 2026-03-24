# DECIMAL数据类型 FS

## 1. 背景

`DECIMAL`数据类型用来存储高精度数值数据, 在其他数据库也被称为`NUMERIC`. `DECIMAL`数据类型的基本运算返回的是精确结果, 不过`DECIMAL`类型的计算性能比整数浮点数要差.

## 2. 变更历史

| 日期 | 版本 | 负责人 | comment |
| --- | --- | --- | --- |
| 2024 年 4 月 29 日 | v0.1 | 王加明 |  |
| 2024 年 12 月 3 日 | v0.2 | 王加明 |  |

## 3. 定义

`DECIMAL`数据类型中有两个重要术语.
- Precision: 精度, 指整个数值中有效数字个数, 包括了小数点左右两侧的所有数字. `PRECISION`最大值为`MAX_PRECISION`.
- Scale: 小数位数, 指位于小数点右侧的小数个数, 必须为非负整数. SCALE必须小于等于指定的`PRECISION`值, 且`SCALE`最大值为`MAX_SCALE`.
如定义一个`DECIMAL`数据类型: `DECIMAL(4,2)`. 表示有效数字最多为4个, 小数点右侧2位, 其表示的值范围为: `[-99.99, 99.99]`.
不支持DECIMAL UNSIGNED.

## 4. 行为说明

### 4.1 定义`DECIMAL`数据类型

定义`DECIMAL`数据类型.
```sql
DECIMAL(precision, scale)
DECIMAL(precision) -- scale defaults to 0
DECIMAL??
```

其中`precision`必须为正整数, `scale`必须为非负整数. `precision`取值范围`[1, MAX_PRECISION]`, `scale`取值范围`[0, ``min(MAX_SCALE``, precision)]`不指定`scale`时, 则`scale`为0, 即只表示整数, 若precision等于scale, 则只能表示(-1,1)的值.
~~定义DECIMAL类型列时还可以不指定precision和scale, 则表示taosd中可表示的最大DECIMAL类型, 与PG类似.~~
不支持precision和scale都不指定的方式定义Decimal类型.
<callout emoji="bulb" background-color="light-orange" border-color="light-orange">
PG中还支持不指定precision和scale定义DECIMAL数据类型, 表示PG中可表示的最大DECIMAL数据类型. 称为`unconstrained numeric`
</callout>

### 4.2 插入`DECIMAL`类型数据

~~插入数据时, ~~`~~DECIMAL~~`~~数据类型需要~~~~以字符串形式传递~~~~. 若~~~~以数值格式~~~~传递~~~~, 则~~~~可能会被降低精度(这个行为目前看可以做到不丢失精度)~~~~. ~~~~可以考虑在不影响其他数据类型插入性能的前提下支持插入decimal类型列时以数值类型传递.~~
插入数据时, DECIMAL类型数据以数值传递, 不会降低精度.
支持科学计数法插入数据, 形如`1E-37`即 <equation>1 \times 10^{-37}
</equation>, `E`也可以使用小写`e`.
<callout emoji="bulb" background-color="light-orange" border-color="light-orange">
MySQL, PG支持以数值类型传入, 不会降低精度.  
</callout>

在插入数据时, 若插入数值的scale高于列定义的scale, 则会丢失大于scale部分的精度. 若插入的数值小数点左侧数字个数大于列定义的`precision`-`scale`, 则会报`Decimal field overflow`错.
例如定义数据类型`DECIMAL(3,1)`. 若插入`0.11`, 则实际插入0.1(四舍五入), 若插入`100.1`, 则会报`Decimal field overflow`错, 可插入值的范围为`[-99.9, 99.9]`.

### 4.3 查询中的数值常量

查询中数值常量使用整数或者double类型解释, 不会自动解释为DECIMAL类型, 若希望使用DECIMAL类型可以手动cast.
~~查询中的数值常量以DECIMAL类型解释, 如~~`~~select 3.141592653589793238462643383279 + c1 + 3.14 from t0~~`~~, 将两个常量都以DECIMAL类型解释, 可以得到精确的计算结果, 且不会丢失精度.~~
~~查询中的整数常量若使用任何整数类型可以存储, 则直接使用该整数类型, 若BIGINT都无法存储, 则使用DECIMAL进行解释, 此过程为内部自动控制.~~~~ 非整数类型时, 默认使用decimal解释(pg中为此行为).~~

### 4.4 `DECIMAL`类型表达式计算

以下行为参考MySQL.
~~DECIMAL类型数据在做除法时, 会增加精度, ~~`~~div_precision_increment~~`~~, 值为4(MySQL可配置, 默认值为4), 即增加4精度, 即decimal(2,2) / 2 的结果小数点右侧scale变成6, ~~~~precision也相应增加, 输出类型为decimal(6,6)~~~~.~~
注: 计算结果类型中的`PRECISION`和`SCALE`不会超过`MAX_PRECISION`和`MAX_SCALE`.
若计算结果值大于precision为MAX_PRECISION时可表示的最大值, 则报overflow错误.
若计算结果未大于precision为MAX_PRECISION时可表示的最大值, 但是小数点左侧位数大于`MAX_PRECISION` - `SCALE`, 则自动进行truncate(????), 减小`SCALE`.

| 输入类型 | 操作符 | 输出类型(参考MySQL行为 废弃) | 输出类型 | comment |
| --- | --- | --- | --- | --- |
| decimal(p1,s1) + decimal(p2,s2) | + | ~~decimal(1 + max(p1-s1,p2-s2) + max(s1,s2), max(s1,s2))~~ | decimal |  |
| decimal(p1,s1) - decimal(p2,s2) | - | ~~decimal(1 + max(p1-s1,p2-s2) + max(s1,s2), max(s1,s2))~~ | decimal |  |
| decimal(p1,s1) * decimal(p2,s2) | * | ~~decimal(p1+p2, s1+s2)~~ | decimal |  |
| decimal(p1,s1) / decimal(p2,s2) | / | ~~decimal(p1 + s2 + div_precision_increment, s1 + div_precision_increment)~~ | decimal |  |
| Decimal op float | +,-,*,/ | ~~double~~ | double |  |
| Float op decimal | +,-,*,/ | ~~double~~ | double |  |
| Decimal op double | +,-,*,/ | ~~double~~ | doublel |  |
| Double op decimal | +,-,*,/ | ~~double~~ | doublel |  |
| INTEGER op decimal | +,-,*,/ | ~~将INTEGER类型转化为decimal之后带入前四行~~~~.~~ | decimal |  |
| Decimal op INTEGER | +,-,*,/ | ~~同上~~ | decimal |  |
| Decimal op string | +,-,*,/ |  | ~~decimal ~~double |  |

DECIMAL类型作为数值类型, 在与DECIMAL或者其他数值类型比较时, 如`>,<,>=,<=,=`, 结果与其他所有数值类型比较一致, 如:
decimal类型`1.1000`与decimal类型`1.1`是相等的. 如decimal类型`123`与整数类型`123`是相等的. 如decimal类型`1.11000`与float`1.11`是相等的.

当两个操作数中存在float或者double或者varchar/nchar时, 输出类型为double.

Add, subtract时, 输出的Scale为max(S1, S2)
Multiply时, 输出Scale为MIN(MAX_SCALE, S1 + S2)
divide时, 输出Scale为S1

操作符两侧不同类型字段计算时遵循以下转换规则, 转换之后再进行计算.

|  | decimal |
| --- | --- |
| NULL | 转换为NULL |
| BOOL | 转换为decimal |
| TINYINT | 转换为decimal |
| SMALLINT | 转换为decimal |
| INT | 转换为decimal |
| BIGINT | 转换为decimal |
| FLOAT | 转换为double |
| DOUBLE | 转换为double |
| VARCHAR | 转换为decimal |
| TIMESTAMP | 转换为decimal |
| NCHAR | 转换为~~decimal ~~double |
| UTINYINT | 转换为decimal |
| USMALLINT | 转换为decimal |
| UINT | 转换为decimal |
| UBIGINT | 转换为decimal |
| JSON | 错误, 可能可以支持 |
| VARBINARY | 错误 |
| DECIMAL |  |
| BLOB | - |
| MEDIUMBLOB | - |
| BINARY | 转换为~~decimal ~~double |
| GEOMERTY | 错误 |
|  |  |


DECIMAL(P,S)类型的函数计算对应输出类型见下表:(~~可能需要做到所有类型计算输出都是decimal~~)

| 函数 | 输出类型 | comment |
| --- | --- | --- |
| abs | DECIMAL |  |
| acos,asin,atan,cos,log,pow,sin,sqrt,tan | DOUBLE |  |
| ceil | BIGINT |  |
| round | DECIMAL |  |
| floor | DECIMAL |  |
| apercentile | DECIMAL |  |
| sum | DECIMAL |  |
| avg | DECIMAL |  |
| leastsquares | VARCHAR |  |
| spread | DECIMAL |  |
| stddev | DOUBLE |  |
| hyperloglog | INTEGER |  |
| histogram | DOUBLE/BIGINT |  |
| percentile | DECIMAL |  |
| bottom | DECIMAL |  |
| first | DECIMAL |  |
| interp | DECIMAL |  |
| last | DECIMAL |  |
| max | DECIMAL |  |
| min | DECIMAL |  |
| mode | DECIMAL |  |
| sample | DECIMAL |  |
| tail | DECIMAL |  |
| top | DECIMAL |  |
| unique | DECIMAL |  |
| csum | DECIMAL |  |
| derivative | DECIMAL |  |
| diff | DECIMAL |  |
| irate | DOUBLE |  |
| mavg | DECIMAL |  |
| statecount | INTEGER |  |
| stateduration | INTEGER |  |
| twa | DOUBLE |  |

<callout emoji="bulb" background-color="light-orange" border-color="light-orange">
PG中DECIMAL类型表达式的返回类型没有这么复杂, PG中支持不带PRECISION和SCALE定义DECIMAL类型, 称为`unconstrained numeric`即表示PG中可表示的最大DECIMAL类型. 所有DECIMAL的 表达式的返回类型不需要计算具体PRECISION和SCALE是多少, 直接使用DECIMAL.
</callout>

支持cast.

### 4.5 其他

DECIMAL类型数据使用专门的存储格式, taos shell查询返回的结果格式受DECIMAL的PRECISION和SCALE控制.
如定义`DECIMAL(5,2)`, 当查询数值为`2.3`时, 客户端显示结果为`2.30`.
~~若定义列类型为DECIMAL, 不指定PRECISION和SCALE, 则小数部分在插入数据时会记录其scale信息, 查询该数据时会补0, 如decimal类型列, 插入~~`~~0.1000~~`~~, ~~~~则查询时也会得到~~`~~0.1000~~`.(pg为此行为, 因为pg中给每一行记录了`display scale`, 但是CH中必须指定Scale, 不指定precision时, precision为0, 因此不会自动补充0, 同时CH中不记录display scale, MySQL中也是此行为)
根据数据类型大小选择Decimal128 Decimal64进行计算和存储.
查看表结构时, DECIMAL类型列的展示与varchar等类似, 显示: `decimal(P,S)`.

### 4.6 功能进度划分

#### 4.6.1 近期可实现的功能

- Decimal基本功能, 包括:
  - 表的普通列支持该类型
  - 提供默认压缩算法
  - Desc, show create table显示类型信息
  - 该类型支持NULL/NONE取值
  - 写入, 修改, 删除该类型数据
  - 该类型的溢出检查
- 支持基本的运算符
  - +,-,*,/, >,>=,<,<=,==,!=,mod等
- 支持基本的类型转换
  - 其他数值类型与decimal类型的互转
- 支持基本的函数计算
  基本的算数运算和类型转换支持之后, 一部分函数支持会比较简单, 如
  - avg,max,sum,min~~,abs~~,cast,~~ceil,floor,round,unique,interp,mode,csum,diff,~~
- ~~支持在输入数值类型可能导致越界时, 默认使用decimal类型~~(暂不支持, 新版本在不创建Deicmal类型时不在计算结果中引入Decimal类型).
- decimal类型支持流计算,订阅.
- taosbenchmark支持decimal类型
- 连接器支持decimal

#### 4.6.2 可考虑延迟的功能

不支持的功能会明确报错.
- decimal类型支持tag列.
- 其他数学计算
  - ln,log,power,sqrt,exp,三角函数,stddev,spread,histogram,percentile,twa,mvg
- 在支持decimal数据类型之后的优化, 如
  - 支持在其他数值类型的计算时可能导致越界时使用decimal类型防止越界.
- Udf支持decimal类型
- schemaless写入decimal类型数据
- stmt

## 5. 性能

Decimal类型的表达式计算, 如+, -, *, /, 以及函数计算等, 相比于double, float, int等数据类型性能有所下降. 性能下降不超过50%(?).

## 6. 兼容性

若创建了 DECIMAL 数据类型, 不能回退到不支持 DECIMAL 的版本. 若没有建该类型, 则可以回退.
decimal类型的sma需要计算.
last函数, sma不受影响.

## 7. 运维

无特殊的运维要求

## 8. 使用场景

当需要存储精确的数值类型, 精确的数值计算, 或对精度要求较高时可以创建 DECIMAL 类型的列. 典型场景如金融数据, 经纬度等.

## 9. 约束与限制

### 9.1 定义`DECIMAL`类型数据

只在Linux,Mac,Windows支持.
定义`DECIMAL`类型数据时, `MAX_PRECISION`值为`38`, `MAX_SCALE`值也为`38`(使用clickhouse中Decimal128相同行为). 
<callout emoji="bulb" background-color="light-orange" border-color="light-orange">
MySQL中`PRECISION`最大值为65, `SCALE`最大值为30
PG中指定PRECISION时, 最大值为1000,不指定PRECISION时, MAX_PRECISION为131072+16383, MAX_SCALE为16383
</callout>

### 9.2 其他

不支持修改DECIMAL数据类型的`precision`和`scale`. 
~~实现相关: 是否有必要拆分类型, 如ClickHouse中的Decimal64, Decimal32, Decimal128, Decimal256~~.

### 9.3 Decimal类型的解析API

客户端返回DECIMAL数据列之前, 转换为字符串类型, 应用读取时类型已经为字符串类型, 此处逻辑与查询JSON类型的整个列类似.
上层应用使用DECIMAL类型数据时, 可使用自己支持的DECIMAL类型从字符串进行解析.

给出Raw block的解析格式
stmt2
schemaless

### 9.4 解决Decimal类型与其他组件的兼容问题

#### 9.4.1 正常查询结果

使用C API的`taos_fetch_row`, 查询Decimal类型的数据时, 在输出的行中, 该列数据会被转换为字符串类型, 可直接读取.

#### 9.4.2 订阅

数据订阅对于Decimal类型需要处理类型转换, 首先需要获取输入和输出Decimal类型的precision和scale信息. 然后调用`decimalScaleUp`接口进行类型转换.
如果需要解析出具体Decimal值, 参考下文`如何解析Decimal类型`.

#### 9.4.3 Schemaless

第一版暂不支持
考虑实现方案和其他数值类型类似, 如写入Decimal64/Decimal128, 则使用后缀d64, d128. 不指定时, 默认使用double.

#### 9.4.4 参数绑定 Stmt2

考虑暂不支持.
实现方案考虑传入字符串, 类型传递Decimal64或者Decimal128, 此时没有precision和scale信息, 在解析字符串时, 生成对应的precision和scale. 若超过该类型可表示的最大值, 则报错, 若小数部分溢出则四舍五入. 存储在`pPlaceholderValues`中.

#### 9.4.5 Raw Block中如何解析Decimal类型数据

若调用`taos_fetch_raw_block`接口. `raw_block`内结构无变化, Decimal类型分别对应DECIMAL64和DECIMAL128, 为定长字段, 长度分别为8和16字节. 要解释这8/16个字节需要`scale`信息. 目前raw_block内没有`scale`信息存储的位置, 考虑复用`raw block`中的第七个字段, 列的Schema: 每列的类型(1字节) + 所需大小(4字节). 这里将`所需大小`的4字节拆开, 其中第一个1字节存储所需大小(8/16), 后两个字节分别存储`precision和scale`.
|___bytes___|__empty__|___prec___|__scale___|. ### 如何解析Decimal类型 - `Decimal64` `Decimal64`占`8`字节, 最大`precision`为`18`, `scale` <= `precision`, 首先直接转换成`int64`, 如得到`12345678901234`, 若`scale`是`5`, 则Decimal64的实际值为`123456789.01234`, 若`scale`为`6`, 则实际值为`12345678.901234`. - `decimal128` `Deicmal128`占`16`个字节, 最大`precision`为`38`, `scale` <= `precision`, 这16个字节与`decimal64`相似, 其实就是一个`int128`, 采用小端序存储, 转换成整数之后, 利用scale确定小数点的位置. 如转换成整数之后是: `-123456789012345678901234567890000`, scale为`10`, 则decimal值为`-12345678901234567890123.4567890000`. ### taosDump dump时需要dump出对应的`precision`, `scale`信息. 写回时根据目标类型进行转换. ### taosBenchmark 支持创建decimal类型数据, 写入decimal类型数据. 查询decimal类型数据. ## 连接器的支持情况 ~~Jdbc, go, python必须支持, ~~所有连接器都支持. ~~Nodejs, c#之后支持~~ # 常见错误与排查 # 参考文档 MySQL 8.0用户手册(8.0.21). PostgreSQL 16.2用户手册. ClickHouse官方文档 24.11 [需求说明：Decimal](https://taosdata.feishu.cn/wiki/CI6KwADzEiaOrjkLL4Uc2yhUneb)
