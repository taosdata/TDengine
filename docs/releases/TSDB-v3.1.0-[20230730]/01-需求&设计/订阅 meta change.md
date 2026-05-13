# 订阅 meta change

@王明明
超级表订阅和数据库订阅增加只订阅meta数据的功能，
语法如下
CREATE TOPIC topic_name only meta AS STABLE stb_name [where_condition]
CREATE TOPIC topic_name only meta AS DATABASE db_name;

通过only meta来指定该topic只订阅meta数据，包括建表，改表等，主要用于taosx做表结构同步。
写数据和删除数据等数据相关的无法订阅到。
