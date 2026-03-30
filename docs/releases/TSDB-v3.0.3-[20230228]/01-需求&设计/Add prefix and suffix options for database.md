# Add prefix and suffix options for database

这是来自台网中心的需求，已经在开发进行中，这里简单的补充一下文档

为数据库增加两个参数，table_prefix 和 table_suffix，表示忽略表名的前几个字符和后几个字符，默认值为 0，表示无任何忽略

用如下示例来说明实现机制：
1. 数据库名称是 db , 表名是 t123name567 ，表名的全称是 0.db.t123name567
2. 假如 prefix = 4 suffix = 3 ，补充db的前缀长度后(0.db.->5)，实际前缀的长度就是4+5 = 9, 用于hash名称从 0.db.t123name567  变为 name
3. 如果表名的长度小于等于 suffix + prefix ，那么采用默认的 hash 方法，忽略 suffix + prefix 选项

具体语法如下:
```sql
create database d1 table_prefix 3 table_suffix 2 vgroups 2;
use d1;
create table st (ts timestamp, i int) tags (j int);
create table st_ct_1 using st tags(3) st_ct_2 using st tags(4) 
create table st_ct_3 using st tags(5) st_ct_4 using st tags(6) 
create table st_ct_5 using st tags(7)
```

st_ct_1 - st_ct_5 这几个子表，都会被存放到同一个 vgroup 中。
