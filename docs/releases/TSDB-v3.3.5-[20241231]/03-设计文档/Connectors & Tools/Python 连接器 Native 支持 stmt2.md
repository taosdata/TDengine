# Python 连接器 Native 支持 stmt2

## 1. 背景

TDengine C/C++ 连接器 Native 方式已经支持了 stmt2 新接口，FS 文档 [stmt2 功能规格](https://taosdata.feishu.cn/wiki/OHn7wgE38i6LiCkKFQec3wyKnOf)
因此，Python 连接器 Native 方式也需要支持。

## 2. 变更历史

| 日期 | 版本 | 负责人 | 主要修改内容 |
| --- | --- | --- | --- |
| 2024/09/11 | 0.1 | 段宽军/裴亚明 | 初稿 |
|  |  |  |  |
|  |  |  |  |

## 3. 定义

stmt2 API: 预处理语句版本 2 API。

## 4. 行为说明

#### 4.0.1 stmt2 connection 类

```python
class TaosConnection(object):
    """TDengine connection object"""

    def statement2(self, sql=None, option=None):
```


1. statement2 方法说明
功能：connection 类增加方法 statement2，用于生成并返回 TaosStmt2 类对象，在方法 statement2 内实现了底层 C/C++ 接口 taos_stmt2_init / taos_stmt2_prepare 的调用。如果初始化失败，抛出异常
StatementError: [0xffff]: stmt2 init failed。
参数：
        @ sql 初始化时给定的 prepare 时的 sql 参数，如果指定，prepare 函数也将被调用
        @ option 初始化的参数，TaosStmt2Option 类对象

1. TaosStmt2Option 类定义
```python {wrap}
class TaosStmt2Option:
    def __init__(self, reqid: int=None, single_stb_insert: bool=False, single_table_bind_once: bool=False, **kwargs):

    def get_impl(self):
```

类 TaosStmt2Option 对外提供，初始化对象时可以设置3个可选参数：
- reqid：int 类型，表示请求编号，用于标记本次 stmt2 的编号，方便问题定位；
- single_stb_insert：bool 类型，表示本次 stmt2 是否仅是单个超级表插入；
- single_table_bind_once：bool 类型，表示本次 stmt2 是否仅绑定一次；
- 可变参数 **kwargs: 支持调用时传入 is_async: bool，显示指定内部调用 C 接口是否使用异步方式，此参数仅用于测试目的，因为 python 连接器对外表现是同步行为，对外提供无实质意义；
对内提供方法 get_impl，用于获取 TaosStmt2OptionImpl 类对象，对应 C/C++ 版本的结构体 TAOS_STMT2_OPTION。

#### 4.0.2 stmt2 statement 类

```python
class TaosStmt2(object):
    """TDengine STMT2 interface"""

    def __init__(self, stmt, decode_binary=True):

    def prepare(self, sql):

    def bind_param(self, tbnames, tags, datas):
    
    def bind_param_with_tables(self, tables):

    def execute(self) -> int:

    def result(self):
 0.843509s
    @property
    def affected_rows(self):

    def close(self):

    def is_insert(self):

    def get_fields(self):
   
```

1. __init__(self, stmt, decode_binary=True)
功能：初始化TaosStmt2类对象，该方法在 TaosConnection 类的 statement2 方法内被调用。
z参数：
   @stmt 底层C接口 taos_stmt2_init 返回的 stmt2 句柄
         @decode_binary 二进制类型字段的数据是否使用"utf-8"解码
1. prepare(self, sql)
功能：预处理的 SQL 语句
参数：
        @ sql stmt2 预处理 SQL 语句，给定表的 meta 信息
1. bind_param(self, tbnames, tags, datas):
功能：绑定参数的具体值，如果不需要指定 tbnames 或 tags，可将其值指定为 None。如果初始化后未调用 prepare 接口就直接绑定，抛出异常：StatementError: [0x022a]: Stmt API usage error，成功时返回 0。
参数：
        @tbnames 指定所有绑定子表的名称列表，如果子表名未绑定，可将其值设置为 None。tbnames为一维列表结构，其元素表示每个子表名称
        @tags 指定所有绑定子表的tags数据列表，如果tags未绑定，可将其值设置为 None。tags为二维列表结构，其元素表示的是每张子表的tag值数据
        @datas 指定所有绑定表的数据列表，datas为三维列表结构，其元素表示的是每张子表的数据信息。根据列排列方式组织数据，每列可包含多行数据。具体结构可参考示例程序。
1. def bind_param_with_tables(self, tables)
功能：绑定参数的具体值，功能和约束条件与bind_param 方法相同，仅参数中数据的组织形式不同。
参数：
        @tables 指定所有绑定 BindTable 对象列表，BindTable 类表示一张表的数据，包含表名称、tags列表和数据，定义如下：
```python
class BindTable(object):
    def __init__(self, name, tags):
        self.name  = name
        self.tags  = tags
        self.datas = []

    # add column data
    def add_col_data(self, data):
        self.datas.append(data)
```

表名称、tags列表在构造 BindTable 类对象时传入，每列的数据需要调用方法 add_col_data 依次传入。
1. execute(self) -> int:
功能：向服务端提交绑定的数据
参数：无
返回值：本次执行受影响的行数
1. is_insert(self):
功能：判断当前是否为写入操作
参数：无
返回值：
      True:   写入操作 
       False: 非写入操作
1. get_fields(self, field_type):
功能：返回当前待绑定参数的元数据信息
参数：@ field_type  field_type的类型为int，有效值为：TAOS_FIELD_COL, TAOS_FIELD_TAG, TAOS_FIELD_QUERY, TAOS_FIELD_TBNAME，其他值将抛出异常StatementError([0xffff]:"invalid field_type value: %d." % field_type)。目前引擎侧仅在传入TAOS_FIELD_COL, TAOS_FIELD_TAG才会返回 fields 信息
```python
TAOS_FIELD_COL    = 1 
TAOS_FIELD_TAG    = 2
TAOS_FIELD_QUERY  = 3
TAOS_FIELD_TBNAME = 4
```

返回值：
Tuple[int, List[TaosFieldEx]：返回值是元祖，第一个元素是count，第二个元素是tag/col的字段信息列表，类 TaosFieldEx 的定义如下：
```python
class TaosFieldEx(ctypes.Structure):
    # 返回字段名称
    @property
    def name(self):

    # 返回字段类型
    @property
    def type(self):

    # 返回字段精度
    @property
    def precision(self):

    # 返回字段级别
    @property
    def scale(self):

    # 返回字段长度
    @property
    def length(self):

    # 同 length 属性
    @property
    def bytes(self):
```



1. result(self):
功能：返回查询对象供查询使用
参数：无
返回值： 查询对象
1. error(self):
功能：返回最后一次错误的报错信息
参数：无
返回值：字符串类型，报错描述信息
1. close(self)
功能：关闭 statement
参数：无
1. affected_rows
功能：返回上次执行受影响的行数，注意：affected_rows 为属性，而不是方法
参数：无
返回值：上次执行受影响的行数

#### 4.0.3 stmt2 bind 相关类

```python

class TaosStmt2Bind(ctypes.Structure):
    _fields_ = [
        ("buffer_type", ctypes.c_int),
        ("buffer", ctypes.c_void_p),
        ("length", ctypes.POINTER(ctypes.c_int32)),
        ("is_null", ctypes.c_char_p),
        ("num", ctypes.c_int)
    ]


class TaosStmt2BindV(ctypes.Structure):
    _fields_ = [
        ("count", ctypes.c_int),
        ("tbnames", ctypes.POINTER(ctypes.c_char_p)),
        ("tags", ctypes.POINTER(ctypes.POINTER(TaosStmt2Bind))),
        ("bind_cols", ctypes.POINTER(ctypes.POINTER(TaosStmt2Bind)))
    ]

```

**TaosStmt2Bind、TaosStmt2BindV 为内部实现类。**
类 TaosStmt2Bind 对应 C/C++ 版本的结构体 TAOS_STMT2_BIND， 在引擎测的实现中，bind 类与 stmt v1 相比，消除了 buffer 中空洞，通过 length 数组中的长度数据表示每一项元素在 buffer 中占用的字节数量；其中 is_null 数组中，0 表示 Value，1 表示 NULL， 2 表示 NONE，不更新该列数据。
反映到python连接器的使用上，在构造绑定数据时，用户可以在绑定数据时，传入特殊类实例 IGNORE，实现设置不更新该列数据的功能。
新增类 TaosStmt2BindV，对应 C/C++ 版本的结构体 TAOS_STMT2_BINDV，可以一次绑定一批表，count 表示需要绑定的表数量，tags 与 bind_cols 分别表示待绑定的标签值与数据，与 tbnames 中的表名称一一对应。

#### 4.0.4 stmt2 bind 相关函数

```python {wrap}
def new_stmt2_binds(size: int) -> Array[TaosStmt2Bind]:
    # type: (int) -> Array[TaosStmt2Bind]
    return (TaosStmt2Bind * size)()


def new_bindv(
        count: int,
        tbnames: List[str],
        tags: Optional[List[Array[TaosStmt2Bind]]],
        bind_cols: Optional[List[Array[TaosStmt2Bind]]]
):
```

**函数 new_stmt2_binds、new_bindv 均为内部使用，不对外提供。**
新增函数 new_stmt2_binds，类似 stmt v1 中的函数 new_multi_binds(size) 。TaosStmt2Bind 类继承自 ctypes.Structure，并且 *fields* 属性定义了结构体的字段和类型。当使用 new_stmt2_binds 函数创建一个 TaosStmt2Bind 的数组时，ctypes 会按照 C 语言的规则分配内存。
新增函数 new_bindv，供 TaosStmt2 类对象调用，用于将用户输入的 bind 信息，构建 TaosS C语言风格内存布局的 stmt2BindV 类的对象。

#### 4.0.5 stmt2 支持绑定 sql 情况

支持以下语句：
1. 绑定普通表数据
insert into common_table values(?,?)
1. 自动建表：指定子表名称
insert into child_table using super_table tags (?,?) values (?,?) 
1. 自动建表：绑定超级表的子表数据
insert into ? using super_table tags(?,?) values(?,?)

注意：因为引擎测暂时不支持在prepare后获取meta信息，导致python连接器会报错：
taos.error.StatementError: [0xffff]: obtain schema failed, maybe this sql is not supported, sql: insert into ? using super_table tags(?,?) values(?,?)

1. 查询语句
select * from common_table/supper_table/child_table where ts = ? and v = ?

当前不支持以下语句：
1. 绑定普通表数据 需要绑定表名
**Python 连接器不支持此种语法，会抛出异常：**
**taos.error.StatementError: [0xffff]: obtain schema failed, maybe this sql is not supported, sql: insert into ? values(?,?)。**
**相关 jira TD-32120。**

insert into ? values(?,?)

1. 自动建表：不带tags关键字的超级表语法
insert into super_table(tbname, location, groupId, ts, current, voltage, phase) values(?,?,?,?,?,?,?)

### 4.1 示例程序

#### 4.1.1 bind 单表多行多列的数据结构

```python
        # prepare data
        tbanmes = ["d1"]
        tags    = [
            ["grade1", 1]
        ]
        datas   = [
            # class 1
            [
                # student
                [1601481600000,1601481600001,1601481600002,1601481600003,1601481600004],
                ["Mary",       "Tom",        "Jack",       "Jane",       "alex"       ],
                [0,            1,            1,            0,            1            ],
                [98,           80,           60,           100,          99           ]
            ]
        ]
```

#### 4.1.2 bind 单表单行多列的数据结构

```python
        # prepare data
        tbanmes = ["d1"]
        tags    = [
            ["grade1", 1]
        ]
        datas   = [
            # class 1
            [
                # student
                [1601481600000],
                ["Mary"],
                [0],
                [98]
            ]
        ]
```

#### 4.1.3 bind 单表多行单列的数据结构

```python
        # prepare data
        tbanmes = ["d1"]
        tags    = [
            ["grade1", 1]
        ]
        datas   = [
            # class 1
            [
                # student
                [1601481600000,1601481600001,1601481600002,1601481600003,1601481600004]
            ]
        ]
```


#### 4.1.4 示例程序：超级表子表插入

```python

## 5. encoding:UTF-8

from taos import *
from ctypes import *
from datetime import datetime
import pytest

@pytest.fixture
def conn():
    # type: () -> taos.TaosConnection
    return connect()

def checkResultCorrects(conn, tbnames, tags, datas):
    pass

def test_stmt_insert(conn):
    # type: (TaosConnection) -> None
    dbname  = "stmt2"
    stbname = "meters"
    try:
        conn.execute("drop database if exists %s" % dbname)
        conn.execute("create database if not exists %s" % dbname)
        conn.select_db(dbname)

        sql = f"create table if not exists {stbname}(ts timestamp, name binary(32), sex bool, score int) tags(grade binary(24), class int)"
        conn.execute(sql)
        # conn.load_table_info("log")

        # 
        #  table info , write 5 lines to 3 child tables d0, d1, d2 with super table
        #
        # 1601481600000

        # prepare data
        tbanmes = ["d1","d2","d3"]
        tags    = [
            ["grade1", 1],
            ["grade1", 2],
            ["grade1", 3]
        ]
        datas   = [
            # class 1
            [
                # student
                [1601481600000,1601481600001,1601481600002,1601481600003,1601481600004],
                ["Mary",       "Tom",        "Jack",       "Jane",       "alex"       ],
                [0,            1,            1,            0,            1            ],
                [98,           80,           60,           100,          99           ]
            ]
            # class 2
            [
                # student
                [1601481600000,1601481600001,1601481600002,1601481600003,1601481600004],
                ["Mary2",      "Tom2",       "Jack2",       "Jane2",     "alex2"       ],
                [0,            1,            1,             0,           1             ],
                [298,          280,          260,           2100,        299           ]
            ]
            # class 3
            [
                # student
                [1601481600000,1601481600001,1601481600002,1601481600003,1601481600004],
                ["Mary3",      "Tom3",       "Jack3",       "Jane3",     "alex3"       ],
                [0,            1,            1,             0,           1             ],
                [298,          380,          360,           3100,        399           ]

            ]
        ]

        option = TaosStmt2Option(reqid=12345, single_stb_insert=False, single_table_bind_once=False)
        stmt2 = conn.statement2(f"insert into ? using {stbname} tags(?,?) values(?,?,?,?)", option)
        stmt2.bind_param(tbanmes, tags, datas)
        stmt2.execute()

        # check correct
        checkResultCorrects(conn, tbanmes, tags, datas)

        conn.execute("drop database if exists %s" % dbname)
        print("pass test_stmt_insert")

    except Exception as err:
        conn.execute("drop database if exists %s" % dbname)
        raise err

if __name__ == "__main__":
    print("stmt3 test case\n")
    # connect db
    conn = taos.connect()

    # test stmt2
    test_stmt_insert(conn)

    # close
    conn.close()

```


示例中用到的是核心的4个 API：
conn.statement2()
stmt2.bind_param()
stmt2.execute()
stmt2.close()
运行后可使用 "select * from db.tb;" 语句查看写入的数据。

## 6. 性能

对启动性能无影响，stmt2 API 的写入、查询性能不低于之前版本。

## 7. 兼容性

新增 API，不影响原有 API 的兼容性。

## 8. 运维

无。

## 9. 使用场景

对本特性被用到的使用场景与之前版本相同，主要应用在避免重复解析 SQL 语句的场景。

## 10. 约束和限制

约束：无
限制：stmt2 API 放宽了一些之前版本的限制，详见参考文档《stmt2 规格说明》行为说明中 API 升级一节。

## 11. 常见错误和排查

stmt2 API 基本流程同之前的版本，在 API 数量上进行了简化。

## 12. 可观测性

对 taos shell, taos Explorer， TDinsight 等基于 UI 或用户交互界面的产品组件无影响。

## 13. 安装和卸载

对安装和卸载脚本无要求。

## 14. 文档

不需要修改企业版文档
暂不需要修改官网文档（需要等 API 稳定后再公开到官网文档）

## 15. 参考文档

[stmt2 功能规格](https://taosdata.feishu.cn/wiki/OHn7wgE38i6LiCkKFQec3wyKnOf)

## 16. 附录

主要基于 stmt 原有实现，简化 API 交互，提供异步操作方式。
