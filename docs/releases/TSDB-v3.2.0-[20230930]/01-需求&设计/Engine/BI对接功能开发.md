# BI对接功能开发

### 1. BI模式

#### 1.1 taosdapater和taosc模式的传递

taosc提供一个API, 完成连接上模式的设置. 当前只支持一种mode BI, value是0或者1. 0代表不启用BI, 1 为BI模式, 默认为0, 不启用BI模式. 只有企业版有效, 开源版忽略
```json
int taos_set_conn_mode(TAOS* taos, int mode, int value);
```

#### 1.2 Connector

根据BI模式, 调用SQL完成. Connector把模式传递给taosadapter

### 2. 语法

1. 显示表和数据库
```json
SHOW [NORMAL|CHILD] TABLES
```

REGULAR: 显示普通表
SUB: 显示子表
缺省显示所有表, 包含子表和普通表
```json
SHOW [SYSTEM|USER] DATABASES
```

SYSTEM: 显示系统数据库. 目前只有 information_schema和performance_schema
USER: 显示用户定义数据库
缺省显示所有数据库, 包含系统定义数据库和用户定义数据库

1. BI模式DESC 超级表
在BI模式下, 添加tbname列, 代表子表名称, 类型为varchar, 长度为表名最大长度

1. BI模式 星号的展开

| BI的查询 | 查询的改写 | 说明 |
| --- | --- | --- |
| last(*) | last(*), last(tbname) | last非选择函数, 不支持tbname, 填充成last(tbname) |
| first(*) | first(*), first(tbname) | first非选择函数, 不支持tbname, 填充成last(tbname) |
| * | *,tbname |  |
| last_row(*) | last_row(*),tbname |  |


1. 外查询中直接使用伪列名称
在外查询使用伪列名称, 直接引用子查询中的对应列
```json {wrap}
select _wstart, _wend from (select _wstart, _wend from table interval(1s))
```
