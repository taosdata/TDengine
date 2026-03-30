# LEFT (SEMI) JOIN需求

### 1. 背景

目前多个客户场景(金融、工业)要求，TDengine支持LEFT JOIN查询

### 2. 客户功能需求

1. 从左表记录，查找右表符合JOIN条件的所有记录，示例如下
```sql {wrap}
select a.*,b.* from  quot_tick a
left join quot_order b
    on a.cmpl_cd=b.cmpl_cd and timetruncate(a.ts,1d)=timetruncate(b.ts,1d) and b.entr_ordr_no=a.buyr_ordr_no and b.deal_dir='1'
left join quot_tick c
    on a.cmpl_cd=b.cmpl_cd and timetruncate(a.ts,1d)=timetruncate(b.ts,1d) and b.entr_ordr_no=a.sler_ordr_no AND b.deal_dir='2'
```

1. LEFT JOIN允许不指定时间戳对齐，允许多个条件
2. LEFT JOIN允许附带筛选条件，左表、右表均可独立设定筛选条件：时间戳、普通列、标签列均可
3. 左表一条记录，对应右表一条或多条记录。如右表未匹配到结果，其结果列返回NULL

### 3. 引申需求

1. 企业版专属功能
2. LEFT SEMI JOIN 左半连接。左半连接仅返回右表匹配的第一条记录
3. 针对时序结果列，支持聚合类、选择类、时序特有函数，需正确返回对所有符合条件的记录的计算结果 （透明）
4. 支持UDF （透明）
5. 支持窗口子句：interval, session, state_window, event_window；支持fill子句  （透明）
6. ~~支持流计算。用户可通过该类LEFT JOIN查询结果来创建流计算~~
7. ~~支持数据订阅。用户可通过该类LEFT JOIN查询结果来创建topic~~

### 4. 性能需求

查询性能，与基准版本相同
