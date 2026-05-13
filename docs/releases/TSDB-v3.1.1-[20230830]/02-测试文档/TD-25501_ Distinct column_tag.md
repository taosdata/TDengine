# TD-25501: Distinct column\tag

## 1. 数据准备

500w子表，每个子表30条数据，在不同vgroup下的distinct 时间

## 2. 测试sql

```sql
select count(*) from (select * from information_schema.ins_tables where db_name ='db_500wtb_2vg');
select count(*) from (select * from information_schema.ins_tables where db_name ='db_500wtb_10vg');
select count(*) from (select * from information_schema.ins_tables where db_name ='db_500wtb_20vg');

select distinct ts from meters;
select distinct current from meters;
select distinct voltage from meters;
select distinct phase from meters;
select distinct groupid from meters;
select distinct location from meters;
select distinct tbname from meters;
```

## 3. 测试结论

最新3.0分支,时间为s
最后对location新建了索引【create index index_loc on meters(location);】

| vgroup | count（子表数量)时间(s) | distinct ts 时间(s)后同 | distinct current | distinct voltage | distinct phase | distinct groupid | distinct location | distinct location(建立索引) | distinct tbname |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| vgroup=2 | 17.78 | 115.27 | 121.43 | 108.47 | 120.44 | 62.37 | 65.78 | 163.66 | 15.48 |
| vgroup=10 | 3.48 | 23.71 | 24.42 | 23.59 | 24.25 | 11.45 | 12.13 | 21.12 | 2.89 |
| vgroup=20 | 2.29 | 13.21 | 14.01 | 11.52 | 12.75 | 6.82 | 6.77 | 11.22 | 2.01 |
