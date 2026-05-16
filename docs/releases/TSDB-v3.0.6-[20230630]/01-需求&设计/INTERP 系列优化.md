# INTERP 系列优化

@Gavin 
此版本增加对INTERP以下三个方面的优化和改进：

#### 1. [INTERP支持单点查询](https://jira.taosdata.com:18080/browse/TS-3048)

INTERP支持对单个时间点的插值，对于fill(null), fill(value)一定会产生插值，对于fill(prev), fill(next), fill(linear)是否产生插值点依赖于改点前后是否有数据点。该行为与插值时间段为窗口时保持一致。
```sql
SELECT INTERP(column) FROM table RANGE(ts1, ts2) EVERY(unit) FILL(fill_val)
SELECT INTERP(column) FROM table RANGE(ts), EVERY(unit), FILL(fill_val)
SELECT INTERP(column) FROM table RANGE(ts), FILL(fill_val)
```

在保持原有interp语法基础上，RANGE子句填入两个相同的时间戳可以实现单点插值的功能。此外，新增加RANGE子句只填写单个时间戳的语法实现此功能，在这种情况下，EVERY子句可以省略不写。

#### 2. [INTERP FILL(value)支持常量表达式](https://jira.taosdata.com:18080/browse/TD-24274)

INTERP支持FILL(VALUE, fill_val) value子句时，支持对于fill_val写入常量表达式，例如，FILL(value, 1+2), FILL(value, 1.5 + 2.5), FILL(value, 1+'123'), 对于数值与字符串类型的标量运算符合数据类型隐式转换(能够转换为数字的则转换，不能转换的当作0来处理)。
此外，对于多个INTERP进行FILL(value, val_1, val_2, ...）时，参数val_n的个数需要与之前INTERP函数个数保持一致，例如：
```sql {wrap}
SELECT INTERP(col1), INTERP(col2) FROM table RANGE(ts1, ts2) EVERY(unit) FILL(value, 1, 2)
```

如果INTERP函数与参数个数不匹配，则会报"fill value number missmatch"的错误，比如以下语句都会被判定为非法:
```sql
SELECT INTERP(col1), INTERP(col2) FROM table RANGE(ts1, ts2) EVERY(unit) FILL(value, 1)
SELECT INTERP(col1) FROM table RANGE(ts1, ts2) EVERY(unit) FILL(value, 1, 2)
```

该行为与timewindow插值时的FILL行为保持一致(value 参数个数需要与聚合函数个数匹配), 注意，_ISROWTS, _ISFILLED等伪列不需要与参数个数进行匹配。

#### 3. [INTERP内存可控](https://jira.taosdata.com:18080/browse/TD-22193)

此任务为内部优化，通过pipeline方式降低算子内存占用，对用户不可见， 无行为改变。
