# 海迅数据库适配TDengine引擎侧需要支持的功能

根据《[南瑞电网北海CEP项目中使用的API列表](https://taosdata.feishu.cn/wiki/VkpCwW2jKi3R5qkEJzdcpTKOnpc)》中的API，梳理出TDengine引擎侧需要支持的功能：

## 1. 数据模型

以海迅数据库的浮点型测点为例，建立对应的超级表
```sql
CREATE STABLE `hxpoints`.`points` (
    `ts` TIMESTAMP, 
    `status` INT, 
    `value` FLOAT
) TAGS (
    `addr` VARCHAR(128),
    `desc` VARCHAR(65),
    `unit` VARCHAR(256),
    `deadband` FLOAT,
    `mirror` TINYINT UNSIGNED,
    `archive` TINYINT UNSIGNED,
    `precision` TINYINT UNSIGNED,
    `datatype` TINYINT UNSIGNED,
    `statistics` TINYINT UNSIGNED,
    `histdays` SMALLINT UNSIGNED,
    `cSource` VARCHAR(8),
    `cGroup` VARCHAR(8),
    `id` INT UNSIGNED,
    `groupId` SMALLINT UNSIGNED,
    `exproperty1` FLOAT,
    `exproperty2` FLOAT
);
```


## 2. 功能需求

### 2.1 FILL 子句支持类型扩展

来源海迅数据库函数：
```cpp {wrap}
DllExport int DB_RetrieveSnapByName(const char* connection, const char* name, DB_HistoryPosition* position, const DB_Time* period, DB_HistValueBlock* block, DB_SnapShotsRetrieveType snap_type = DB_SNAP);
```

其中最后一个参数的类型定义为：
```cpp
///snap shorts type
enum DB_SnapShotsRetrieveType
{
    DB_INTERPOLATION,       ///<插值查询                ==> 对应 TDengine FILL(LINEAR)
    DB_SNAP,                ///<阶梯查询                ==> 对应 TDengine FILL(PREV)
    DB_SNAP_BACKWARD,       ///<阶梯向后取值             ==> 对应 TDengine FILL(NEXT)
    DB_SNAP_NEAR,           ///<阶梯最近值
    DB_SNAP_FORCE,          ///<强制阶梯查询(超过最大值取前值)
    DB_SNAP_BACKWARD_FORCE, ///<强制阶梯向后取值(小于最小值取后值)
    DB_SNAP_NEAR_FORCE      ///<强制阶梯最近值
};
```

根据TDengine官网文档描述：[FILL 子句](https://docs.taosdata.com/reference/taos-sql/distinguished/#fill-%E5%AD%90%E5%8F%A5)
对上边类型进行了初步的映射
其中后四种类型：DB_SNAP_NEAR、DB_SNAP_FORCE、DB_SNAP_BACKWARD_FORCE、DB_SNAP_NEAR_FORCE TDengine 暂不支持。其中 DB_SNAP_FORCE、DB_SNAP_BACKWARD_FORCE、DB_SNAP_NEAR_FORCE 的准确语义需要与客户沟通后明晰【经与客户沟通，此三种类型客户暂时没有使用】。

#### 2.1.1 已经支持的功能

- 插值查询
```sql
taos> select _irowts, interp(status), interp(`value`), _isfilled
    > from hxpoints.d0
    > range('2024-08-01 11:00:00.000', '2024-08-01 13:00:00.000')
    > every(4m)
    > fill(linear);
         _irowts         | interp(status) |   interp(`value`)    | _isfilled |
==============================================================================
 2024-08-01 11:00:00.000 |             39 |          215.3170013 | false     |
 2024-08-01 11:04:00.000 |             68 |          226.5496063 | true      |
 2024-08-01 11:08:00.000 |             45 |          212.4766083 | true      |
 2024-08-01 11:12:00.000 |             64 |          213.6073914 | true      |
 2024-08-01 11:16:00.000 |             59 |          217.1829987 | true      |
 2024-08-01 11:20:00.000 |             90 |          224.2870026 | false     |
 2024-08-01 11:24:00.000 |             74 |          220.2474060 | true      |
```

- 阶梯查询
```sql
taos> select _irowts, interp(status), interp(`value`), _isfilled
    > from hxpoints.d0
    > range(1722481200000, 1722488400000)
    > every(4m)
    > fill(prev);
         _irowts         | interp(status) |   interp(`value`)    | _isfilled |
==============================================================================
 2024-08-01 11:00:00.000 |             39 |          215.3170013 | false     |
 2024-08-01 11:04:00.000 |             87 |          224.2559967 | true      |
 2024-08-01 11:08:00.000 |             53 |          202.7870026 | true      |
 2024-08-01 11:12:00.000 |             74 |          212.8849945 | true      |
 2024-08-01 11:16:00.000 |             41 |          212.0529938 | true      |
 2024-08-01 11:20:00.000 |             90 |          224.2870026 | false     |
 2024-08-01 11:24:00.000 |             81 |          214.6950073 | true      |
```

- 阶梯向后查询
```sql
taos> select _irowts, interp(status), interp(`value`), _isfilled
    > from hxpoints.d0
    > range(1722481200000, 1722488400000)
    > every(4m)
    > fill(next);
         _irowts         | interp(status) |   interp(`value`)    | _isfilled |
==============================================================================
 2024-08-01 11:00:00.000 |             39 |          215.3170013 | false     |
 2024-08-01 11:04:00.000 |             40 |          229.9900055 | true      |
 2024-08-01 11:08:00.000 |             44 |          214.8990021 | true      |
 2024-08-01 11:12:00.000 |             26 |          216.4969940 | true      |
 2024-08-01 11:16:00.000 |             71 |          220.6029968 | true      |
 2024-08-01 11:20:00.000 |             90 |          224.2870026 | false     |
 2024-08-01 11:24:00.000 |             65 |          228.5760040 | true      |
```

#### 2.1.2 待支持的功能

- 阶梯最近值查询
```sql
taos> select _irowts, interp(status), interp(`value`), _isfilled
    > from hxpoints.d0
    > range(1722481200000, 1722488400000)
    > every(4m)
    > fill(near);
```

near：取prev、next两者最近数据的值
- 强制阶梯查询
```sql
taos> select _irowts, interp(status), interp(`value`), _isfilled
    > from hxpoints.d0
    > range(1722481200000, 1722488400000)
    > every(4m)
    > fill(prev_force);
```

**通过头文件说明无法了解准确含义，需要与客户沟通后确认。【经与客户沟通，此种类型客户暂时没有使用】。**
- 强制阶梯向后查询
```sql
taos> select _irowts, interp(status), interp(`value`), _isfilled
    > from hxpoints.d0
    > range(1722481200000, 1722488400000)
    > every(4m)
    > fill(next_force);
```

**通过头文件说明无法了解准确含义，需要与客户沟通后确认。【经与客户沟通，此种类型客户暂时没有使用】。**
- 强制阶梯最近值查询
```sql
taos> select _irowts, interp(status), interp(`value`), _isfilled
    > from hxpoints.d0
    > range(1722481200000, 1722488400000)
    > every(4m)
    > fill(near_force);
```

**通过头文件说明无法了解准确含义，需要与客户沟通后确认。【经与客户沟通，此种类型客户暂时没有使用】。**

### 2.2 INTERP 函数支持新功能

#### 2.2.1 待支持的功能

- 获取插值来源数据记录的原始时间（线性插值除外）
- 增加参数，控制插值来源数据记录和插值点的间隔是否超过指定阈值

来源海迅数据库函数：
```cpp
//! 根据数值型测点ID获取测点历史断面值和数据类型
//! 注：Original_Time只对阶梯查询生效，如选为true，则查询出来的值为数据库保存的原始时间
//! @param[in] connection：海迅连接字符串
//! @param[in] start：历史断面时刻
//! @param[in] names：测点名称数组
//! @param[out] values：测点最新值数组和测点类型
//! @param[in] count：测点名称数组数目
//! @param[out] results：测点查询结果
//! @param[in] type：查询类型
//! @param[in] Original_Time: 阶梯查询是否返回原始时间，插值查询该参数不生效
//! @param[in] flag：断面无值时前后查询间隔是否超过24小时,0(超过),1(不超过)

DllExport int DB_RetrieveSnapshotsByNameWithType_Ex(const char* connection, const DB_Time* start, const DB_NAME* names, DB_Shots_Value * values, uint32_t count, int* results, DB_SnapShotsRetrieveType type = DB_INTERPOLATION, bool Original_Time = false, unsigned char flag = 0);
```

以上两个功能对应上边函数的最后两个参数：Original_Time、flag
结合TDengine的interp函数，初步设计如下：
```sql
taos> select tbname, _irowts, interp(status), interp(`value`), _isfilled
    > from hxpoints.points
    > where tbname in ('d1', 'd3', 'd4')
    > partition by tbname
    > range(1722481200001)
    > fill(prev, true, '24h');
             tbname             |         _irowts         | interp(status) |   interp(`value`)    | _isfilled |
===============================================================================================================
 d4                             | 2024-08-01 11:00:00.000 |             39 |          215.3170016 | true      |
 d3                             | 2024-08-01 11:00:00.000 |             39 |          213.5170072 | true      |
 d1                             | 2024-08-01 11:00:00.000 |             39 |          216.9170418 | true      |
```

fill 参数说明：
1. 第一个参数：填充类型
有效值至少包括：prev、next、near，分别表示：向前取值、向后取值、前后取最近时间点的值；
1. 第二个参数：查询结果是否返回数据的原始时间，默认为false，即返回断面点时间，当为true时，返回数据的原始时间；
2. 第三个参数：断面无值时前后查询的范围。如果该范围内有值, 则使用，如果无值，可以填充用户传入的默认值，包括空值。h表示小时，可酌情支持秒、分钟等；
