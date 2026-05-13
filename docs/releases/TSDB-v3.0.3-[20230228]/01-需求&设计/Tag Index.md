# Tag Index

## 1. 背景 

当前只有超级表的第一列TAG上有默认有索引，不支持动态给其TAG他列添加索引，本功能主要是能够动态为非第一列的TAG列动态添加索引。如果没有索引，需要扫描整个tdb，并对这些值做条件过滤，加了索引之后，过滤条件直接下沉到tdb,  相对高效。

### 1.1 语法

`创建语法： CREATE INDEX ``index_name ``ON tbl_name (``tag``Col``Name``）`
    index_name为索引名称， tbl_name 为超级表名称, tagColName 为tag 列名称,  tagColName 不限制类型。 
`删除语法： DROP INDEX ``index_name`        
    index_name 代表索引名称
`查看已经存在INDEX``： ``SELECT * FROM information_schema.``INS_INDEXES` 
```plaintext
  taos> create index ta_3_db.t2i on ta_3_mt  (t2);
Create OK, 0 row(s) affected (0.080824s)

taos> select * from information_schema.ins_indexes\G;
***************************1.row **************************
 index_name: t2i
    db_name: ta_3_db
 table_name: ta_3_mt
  vgroup_id: -1
create_time: 2023-02-13 21:07:49.874
column_name: t2
 index_type: tag_index
Query OK, 1 row(s) in set (0.014298s)
taos>
```



### 1.2 实现

  客户端发起create/drop index发送到mnode上，mnode模块找到当前超级表所属于vg, 并发起事务，向各个vg上发送请求req, 各个vg收到消息后，根据消息遍历meta模块，生成索引信息，并发送resp到mnode模块。一旦mnode发现事务进行完毕，就更新的schema信息及其版本信息.  create/drop index 是一个同步命令
  
### 1.3 代价

   - tag 索引会导致meta存储的的内容更多, 具体值只不好估计，之后测试做一个估计。
  2.索引更新对创建新表带来的的性能影响，之前创建新表时候需要抽取第一列tag做索引，现在需要抽取多列tag（有索引的）信息存储在meta模块上，在实现是多了几次循环，影响比较小。 如果meta模块性能足够，这个不会是问题。 
   - 动态增加索引的时候，需要遍历当前已经存在的table的tag信息，并添加到索引中，这个锁力度比较大，可能会对当前正在进行的建表、写数据有一定的影响。 为了尽可能避免这种影响，推荐在创建超级表之后，就在相应tag上添加索引。
   - 动态删除索引的时候，需要遍历当前已经存在table的tag信息，并在对应模块做删除，这个锁的力度比较大，可能会对当前正在进行的建表、写数据有一定的影响、并且删除会导致meta做balance操作。 
   - tag索引会在mnode存储极少元信息，占用空间可以忽略。

### 1.4 限制  

   - 算子限制，目前主要支持的基础算子为=、>、>=、<、<= 算子，其他诸如IN等算子并没有下沉meta。针对这里暂不支持的算子，其算子性能和未加索引类似，之后需要添加更多基础算子
   - 针对某列tag, 只能添加一次索引, 多次添加会报错。
   - 目前语法上只支持一次添加针对一列的索引, 不能同时对多个tag列添加索引。
   - tag index和sma index同属于索引，在mnode上统一管理，带来的要求是： 针对同一个DB, sma index 和tag index不能重名。具体来说： 如果已经创建了一个名为indexName的sma索引，那么不能再创建一个名为indexName的tag index索引, 如果创建的话，会直接报错， 反之亦然。 一个DB内只能有一个名为indexName 的index.  
   - 目前对索引个数并没有做限制，但每增加一个索引，都会在meta存储tag值到uid的映射关系，会带来一定的空间占用, 如果因添加索引导致meta太大，从而导致B+tree太深而影响整个meta模块的性能，继而使其他依赖meta模块的模块性能受影响，会得不偿失
   - 在普通表和子表添加会直接报错。
   - 为了兼容之前的数据，当前第一列tag 的index_name 不能通过` SELECT * FROM information_schema.INS_INDEXES`查询得到, 如果在第一列tag 创建index, 会直接报错tag index already exists. 
   - 如果某列 Distinct tag 值特别的少的时候，而表又比较多的时候，不建议加索引。

### 1.5 简单性能测试（还在补充中，需要整理成对比列表）

1. 1 万子表，对某列INT类型添加索引后，点查询性能提高了近8～10倍。
