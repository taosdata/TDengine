

# TDengine数据建模

TDengine采用关系型数据模型，需要建库、建表。因此对于一个具体的应用场景，需要考虑库的设计，超级表和普通表的设计。本节不讨论细致的语法规则，只介绍概念。

关于数据建模请参考[视频教程](https://www.taosdata.com/blog/2020/11/11/1945.html)。

## <a class="anchor" id="create-db"></a>创建库

不同类型的数据采集点往往具有不同的数据特征，包括数据采集频率的高低，数据保留时间的长短，副本的数目，数据块的大小，是否允许更新数据等等。为了在各种场景下TDengine都能最大效率的工作，TDengine建议将不同数据特征的表创建在不同的库里，因为每个库可以配置不同的存储策略。创建一个库时，除SQL标准的选项外，应用还可以指定保留时长、副本数、内存块个数、时间精度、文件块里最大最小记录条数、是否压缩、一个数据文件覆盖的天数等多种参数。比如：

```mysql
CREATE DATABASE power KEEP 365 DAYS 10 BLOCKS 4 UPDATE 1;
```
上述语句将创建一个名为power的库，这个库的数据将保留365天（超过365天将被自动删除），每10天一个数据文件，内存块数为4，允许更新数据。详细的语法及参数请见 [TAOS SQL 的数据管理](https://www.taosdata.com/cn/documentation/taos-sql#management) 章节。

创建库之后，需要使用SQL命令USE将当前库切换过来，例如：

```mysql
USE power;	
```

将当前连接里操作的库换为power，否则对具体表操作前，需要使用“库名.表名”来指定库的名字。  

**注意：**

- 任何一张表或超级表是属于一个库的，在创建表之前，必须先创建库。
- 处于两个不同库的表是不能进行JOIN操作的。
- 创建并插入记录、查询历史记录的时候，均需要指定时间戳。

## <a class="anchor" id="create-stable"></a>创建超级表

一个物联网系统，往往存在多种类型的设备，比如对于电网，存在智能电表、变压器、母线、开关等等。为便于多表之间的聚合，使用TDengine, 需要对每个类型的数据采集点创建一个超级表。以[表1](https://www.taosdata.com/cn/documentation/architecture#model_table1)中的智能电表为例，可以使用如下的SQL命令创建超级表：

```mysql
CREATE STABLE meters (ts timestamp, current float, voltage int, phase float) TAGS (location binary(64), groupId int);
```

**注意：**这一指令中的 STABLE 关键字，在 2.0.15 之前的版本中需写作 TABLE 。

与创建普通表一样，创建表时，需要提供表名（示例中为meters），表结构Schema，即数据列的定义。第一列必须为时间戳（示例中为ts)，其他列为采集的物理量（示例中为current, voltage, phase)，数据类型可以为整型、浮点型、字符串等。除此之外，还需要提供标签的schema (示例中为location, groupId)，标签的数据类型可以为整型、浮点型、字符串等。采集点的静态属性往往可以作为标签，比如采集点的地理位置、设备型号、设备组ID、管理员ID等等。标签的schema可以事后增加、删除、修改。具体定义以及细节请见 [TAOS SQL 的超级表管理](https://www.taosdata.com/cn/documentation/taos-sql#super-table) 章节。

每一种类型的数据采集点需要建立一个超级表，因此一个物联网系统，往往会有多个超级表。对于电网，我们就需要对智能电表、变压器、母线、开关等都建立一个超级表。在物联网中，一个设备就可能有多个数据采集点（比如一台风力发电的风机，有的采集点采集电流、电压等电参数，有的采集点采集温度、湿度、风向等环境参数），这个时候，对这一类型的设备，需要建立多张超级表。一张超级表里包含的采集物理量必须是同时采集的（时间戳是一致的）。

一张超级表最多容许1024列，如果一个采集点采集的物理量个数超过1024，需要建多张超级表来处理。一个系统可以有多个DB，一个DB里可以有一到多个超级表。

## <a class="anchor" id="create-table"></a>创建表

TDengine对每个数据采集点需要独立建表。与标准的关系型数据库一样，一张表有表名，Schema，但除此之外，还可以带有一到多个标签。创建时，需要使用超级表做模板，同时指定标签的具体值。以[表1](https://www.taosdata.com/cn/documentation/architecture#model_table1)中的智能电表为例，可以使用如下的SQL命令建表：

```mysql
CREATE TABLE d1001 USING meters TAGS ("Beijing.Chaoyang", 2);
```

其中d1001是表名，meters是超级表的表名，后面紧跟标签Location的具体标签值”Beijing.Chaoyang"，标签groupId的具体标签值2。虽然在创建表时，需要指定标签值，但可以事后修改。详细细则请见 [TAOS SQL 的表管理](https://www.taosdata.com/cn/documentation/taos-sql#table) 章节。

**注意：**目前 TDengine 没有从技术层面限制使用一个 database （dbA）的超级表作为模板建立另一个 database （dbB）的子表，后续会禁止这种用法，不建议使用这种方法建表。

TDengine建议将数据采集点的全局唯一ID作为表名(比如设备序列号）。但对于有的场景，并没有唯一的ID，可以将多个ID组合成一个唯一的ID。不建议将具有唯一性的ID作为标签值。  

**自动建表**：在某些特殊场景中，用户在写数据时并不确定某个数据采集点的表是否存在，此时可在写入数据时使用自动建表语法来创建不存在的表，若该表已存在则不会建立新表。比如：

```mysql
INSERT INTO d1001 USING meters TAGS ("Beijng.Chaoyang", 2) VALUES (now, 10.2, 219, 0.32);
```

上述SQL语句将记录 (now, 10.2, 219, 0.32) 插入表d1001。如果表d1001还未创建，则使用超级表meters做模板自动创建，同时打上标签值“Beijing.Chaoyang", 2。  

关于自动建表的详细语法请参见 [插入记录时自动建表](https://www.taosdata.com/cn/documentation/taos-sql#auto_create_table) 章节。

## 多列模型 vs 单列模型

TDengine支持多列模型，只要物理量是一个数据采集点同时采集的（时间戳一致），这些量就可以作为不同列放在一张超级表里。但还有一种极限的设计，单列模型，每个采集的物理量都单独建表，因此每种类型的物理量都单独建立一超级表。比如电流、电压、相位，就建三张超级表。

TDengine建议尽可能采用多列模型，因为插入效率以及存储效率更高。但对于有些场景，一个采集点的采集量的种类经常变化，这个时候，如果采用多列模型，就需要频繁修改超级表的结构定义，让应用变的复杂，这个时候，采用单列模型会显得更简单。

