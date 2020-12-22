# 超级表STable：多表聚合

TDengine要求每个数据采集点单独建表，这样能极大提高数据的插入/查询性能，但是导致系统中表的数量猛增，让应用对表的维护以及聚合、统计操作难度加大。为降低应用的开发难度，TDengine引入了超级表STable (Super Table)的概念。

## 什么是超级表

STable是同一类型数据采集点的抽象，是同类型采集实例的集合，包含多张数据结构一样的子表。每个STable为其子表定义了表结构和一组标签：表结构即表中记录的数据列及其数据类型；标签名和数据类型由STable定义，标签值记录着每个子表的静态信息，用以对子表进行分组过滤。子表本质上就是普通的表，由一个时间戳主键和若干个数据列组成，每行记录着具体的数据，数据查询操作与普通表完全相同；但子表与普通表的区别在于每个子表从属于一张超级表，并带有一组由STable定义的标签值。每种类型的采集设备可以定义一个STable。数据模型定义表的每列数据的类型，如温度、压力、电压、电流、GPS实时位置等，而标签信息属于Meta Data，如采集设备的序列号、型号、位置等，是静态的，是表的元数据。用户在创建表（数据采集点）时指定STable(采集类型)外，还可以指定标签的值，也可事后增加或修改。

TDengine扩展标准SQL语法用于定义STable，使用关键词tags指定标签信息。语法如下：

```mysql
CREATE TABLE <stable_name> (<field_name> TIMESTAMP, field_name1 field_type,…)   TAGS(tag_name tag_type, …) 
```

其中tag_name是标签名，tag_type是标签的数据类型。标签可以使用时间戳之外的其他TDengine支持的数据类型，标签的个数最多为6个，名字不能与系统关键词相同，也不能与其他列名相同。如：

```mysql
create table thermometer (ts timestamp, degree float) 
tags (location binary(20), type int)
```

上述SQL创建了一个名为thermometer的STable，带有标签location和标签type。

为某个采集点创建表时，可以指定其所属的STable以及标签的值，语法如下：

```mysql
CREATE TABLE <tb_name> USING <stb_name> TAGS (tag_value1,...)
```

沿用上面温度计的例子，使用超级表thermometer建立单个温度计数据表的语句如下：

```mysql
create table t1 using thermometer tags (‘beijing’, 10)
```

上述SQL以thermometer为模板，创建了名为t1的表，这张表的Schema就是thermometer的Schema，但标签location值为‘beijing’，标签type值为10。

用户可以使用一个STable创建数量无上限的具有不同标签的表，从这个意义上理解，STable就是若干具有相同数据模型，不同标签的表的集合。与普通表一样，用户可以创建、删除、查看超级表STable，大部分适用于普通表的查询操作都可运用到STable上，包括各种聚合和投影选择函数。除此之外，可以设置标签的过滤条件，仅对STbale中部分表进行聚合查询，大大简化应用的开发。

TDengine对表的主键（时间戳）建立索引，暂时不提供针对数据模型中其他采集量（比如温度、压力值）的索引。每个数据采集点会采集若干数据记录，但每个采集点的标签仅仅是一条记录，因此数据标签在存储上没有冗余，且整体数据规模有限。TDengine将标签数据与采集的动态数据完全分离存储，而且针对STable的标签建立了高性能内存索引结构，为标签提供全方位的快速操作支持。用户可按照需求对其进行增删改查（Create，Retrieve，Update，Delete，CRUD）操作。

STable从属于库，一个STable只属于一个库，但一个库可以有一到多个STable, 一个STable可有多个子表。

## 超级表管理

- 创建超级表

    ```mysql
    CREATE TABLE <stable_name> (<field_name> TIMESTAMP, field_name1 field_type,…) TAGS(tag_name tag_type, …)
    ```

    与创建表的SQL语法相似。但需指定TAGS字段的名称和类型。 

    说明：

    1. TAGS列总长度不能超过512 bytes；
    2. TAGS列的数据类型不能是timestamp和nchar类型；
    3. TAGS列名不能与其他列名相同;
    4. TAGS列名不能为预留关键字. 

- 显示已创建的超级表

    ```mysql
    show stables;
    ```

    查看数据库内全部STable，及其相关信息，包括STable的名称、创建时间、列数量、标签（TAG）数量、通过该STable建表的数量。 

- 删除超级表

    ```mysql
    DROP TABLE <stable_name>
    ```

    Note: 删除STable不会级联删除通过STable创建的表；相反删除STable时要求通过该STable创建的表都已经被删除。

- 查看属于某STable并满足查询条件的表

    ```mysql
    SELECT TBNAME,[TAG_NAME,…] FROM <stable_name> WHERE <tag_name> <[=|=<|>=|<>] values..> ([AND|OR] …)
    ```

    查看属于某STable并满足查询条件的表。说明：TBNAME为关键词，显示通过STable建立的子表表名，查询过程中可以使用针对标签的条件。

    ```mysql
    SELECT COUNT(TBNAME) FROM <stable_name> WHERE <tag_name> <[=|=<|>=|<>] values..> ([AND|OR] …)
    ```

    统计属于某个STable并满足查询条件的子表的数量

## 写数据时自动建子表

在某些特殊场景中，用户在写数据时并不确定某个设备的表是否存在，此时可使用自动建表语法来实现写入数据时里用超级表定义的表结构自动创建不存在的子表，若该表已存在则不会建立新表。注意：自动建表语句只能自动建立子表而不能建立超级表，这就要求超级表已经被事先定义好。自动建表语法跟insert/import语法非常相似，唯一区别是语句中增加了超级表和标签信息。具体语法如下：

```mysql
INSERT INTO <tb_name> USING <stb_name> TAGS (<tag1_value>, ...) VALUES (field_value, ...) (field_value, ...) ...;
```

向表tb_name中插入一条或多条记录，如果tb_name这张表不存在，则会用超级表stb_name定义的表结构以及用户指定的标签值(即tag1_value…)来创建名为tb_name新表，并将用户指定的值写入表中。如果tb_name已经存在，则建表过程会被忽略，系统也不会检查tb_name的标签是否与用户指定的标签值一致，也即不会更新已存在表的标签。

```mysql
INSERT INTO <tb1_name> USING <stb1_name> TAGS (<tag1_value1>, ...) VALUES (<field1_value1>, ...) (<field1_value2>, ...) ... <tb_name2> USING <stb_name2> TAGS(<tag1_value2>, ...) VALUES (<field1_value1>, ...) ...;
```

向多张表tb1_name，tb2_name等插入一条或多条记录，并分别指定各自的超级表进行自动建表。

## STable中TAG管理

除了更新标签的值的操作是针对子表进行，其他所有的标签操作（添加标签、删除标签等）均只能作用于STable，不能对单个子表操作。对STable添加标签以后，依托于该STable建立的所有表将自动增加了一个标签，对于数值型的标签，新增加的标签的默认值是0.

- 添加新的标签

    ```mysql
    ALTER TABLE <stable_name> ADD TAG <new_tag_name> <TYPE>
    ```

    为STable增加一个新的标签，并指定新标签的类型。标签总数不能超过6个。

- 删除标签

    ```mysql
    ALTER TABLE <stable_name> DROP TAG <tag_name>
    ```

    删除超级表的一个标签，从超级表删除某个标签后，该超级表下的所有子表也会自动删除该标签。

    	说明：第一列标签不能删除，至少需要为STable保留一个标签。

- 修改标签名

    ```mysql
    ALTER TABLE <stable_name> CHANGE TAG <old_tag_name> <new_tag_name>
    ```

    	修改超级表的标签名，从超级表修改某个标签名后，该超级表下的所有子表也会自动更新该标签名。

- 修改子表的标签值

    ```mysql
    ALTER TABLE <table_name> SET TAG <tag_name>=<new_tag_value>
    ```

## STable多表聚合

针对所有的通过STable创建的子表进行多表聚合查询，支持按照全部的TAG值进行条件过滤，并可将结果按照TAGS中的值进行聚合，暂不支持针对binary类型的模糊匹配过滤。语法如下：

```mysql
SELECT function<field_name>,… 
 FROM <stable_name> 
 WHERE <tag_name> <[=|<=|>=|<>] values..> ([AND|OR] …)
 INTERVAL (<interval> [, offset])
 GROUP BY <tag_name>, <tag_name>…
 ORDER BY <tag_name> <asc|desc>
 SLIMIT <group_limit>
 SOFFSET <group_offset>
 LIMIT <record_limit>
 OFFSET <record_offset>
```

**说明**：

超级表聚合查询，TDengine目前支持以下聚合\选择函数：sum、count、avg、first、last、min、max、top、bottom，以及针对全部或部分列的投影操作，使用方式与单表查询的计算过程相同。暂不支持其他类型的聚合计算和四则运算。当前所有的函数及计算过程均不支持嵌套的方式进行执行。

 不使用GROUP BY的查询将会对超级表下所有满足筛选条件的表按时间进行聚合，结果输出默认是按照时间戳单调递增输出，用户可以使用ORDER BY _c0 ASC|DESC选择查询结果时间戳的升降排序；使用GROUP BY <tag_name> 的聚合查询会按照tags进行分组，并对每个组内的数据分别进行聚合，输出结果为各个组的聚合结果，组间的排序可以由ORDER BY <tag_name> 语句指定，每个分组内部，时间序列是单调递增的。

使用SLIMIT/SOFFSET语句指定组间分页，即指定结果集中输出的最大组数以及对组起始的位置。使用LIMIT/OFFSET语句指定组内分页，即指定结果集中每个组内最多输出多少条记录以及记录起始的位置。

## STable使用示例

以温度传感器采集时序数据作为例，示范STable的使用。 在这个例子中，对每个温度计都会建立一张表，表名为温度计的ID，温度计读数的时刻记为ts，采集的值记为degree。通过tags给每个采集器打上不同的标签，其中记录温度计的地区和类型，以方便我们后面的查询。所有温度计的采集量都一样，因此我们用STable来定义表结构。

###定义STable表结构并使用它创建子表

创建STable语句如下：

```mysql
CREATE TABLE thermometer (ts timestamp, degree double) 
TAGS(location binary(20), type int)
```

假设有北京，天津和上海三个地区的采集器共4个，温度采集器有3种类型，我们就可以对每个采集器建表如下：

```mysql
CREATE TABLE therm1 USING thermometer TAGS (’beijing’, 1);
CREATE TABLE therm2 USING thermometer TAGS (’beijing’, 2);
CREATE TABLE therm3 USING thermometer TAGS (’tianjin’, 1);
CREATE TABLE therm4 USING thermometer TAGS (’shanghai’, 3);
```

其中therm1，therm2，therm3，therm4是超级表thermometer四个具体的子表，也即普通的Table。以therm1为例，它表示采集器therm1的数据，表结构完全由thermometer定义，标签location=”beijing”, type=1表示therm1的地区是北京，类型是第1类的温度计。

###写入数据

注意，写入数据时不能直接对STable操作，而是要对每张子表进行操作。我们分别向四张表therm1，therm2， therm3， therm4写入一条数据，写入语句如下：

```mysql
INSERT INTO therm1 VALUES (’2018-01-01 00:00:00.000’, 20);
INSERT INTO therm2 VALUES (’2018-01-01 00:00:00.000’, 21);
INSERT INTO therm3 VALUES (’2018-01-01 00:00:00.000’, 24);
INSERT INTO therm4 VALUES (’2018-01-01 00:00:00.000’, 23);
```

###按标签聚合查询

查询位于北京(beijing)和天津(tianjing)两个地区的温度传感器采样值的数量count(*)、平均温度avg(degree)、最高温度max(degree)、最低温度min(degree)，并将结果按所处地域(location)和传感器类型(type)进行聚合。

```mysql
SELECT COUNT(*), AVG(degree), MAX(degree), MIN(degree)
FROM thermometer
WHERE location=’beijing’ or location=’tianjing’
GROUP BY location, type 
```

###按时间周期聚合查询

查询仅位于北京以外地区的温度传感器最近24小时(24h)采样值的数量count(*)、平均温度avg(degree)、最高温度max(degree)和最低温度min(degree)，将采集结果按照10分钟为周期进行聚合，并将结果按所处地域(location)和传感器类型(type)再次进行聚合。

```mysql
SELECT COUNT(*), AVG(degree), MAX(degree), MIN(degree)
FROM thermometer
WHERE name<>’beijing’ and ts>=now-1d
INTERVAL(10M)
GROUP BY location, type
```