# 列级注释 (Column Comment) (Canceled)

#### 1. 创建

列注释可以加在普通表列上, 或者超级的列和tag上, 如建表语句:
```sql
create table normal_table (ts timestamp comment 'ts', c1 int comment 'c1 comment');
create stable stb(ts timestamp comment 'ts', c1 int comment 'c1 comment') tags(tg1 int comment 'tg1 comment');
```

Alter table add column时也可以添加列注释, 普通表和超级表相同, add tag也类似. 如sql:
```sql
alter table normal_table add column c2 varchar(255) comment 'c2 255 comment';
```

#### 2. 暂不支持修改

由于Alter table modify column目前只能改varchar类型字段的大小, 因此以下语句不支持执行.
```sql
alter table normal_table modify column c1 int comment 'c1 new comment';
```

但是以下语句可以执行, 正常修改varchar长度为256, 但是comment目前不会修改.
```sql
alter table normal_table modify column c2 varchar(256) comment 'c2 256 comment';
```

#### 3. 查看

通过以下语句可以查看列注释的信息.
```sql
show create table normal_table;
desc normal_table;
```
