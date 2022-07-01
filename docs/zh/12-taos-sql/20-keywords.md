---
sidebar_label: 参数限制与保留关键字 
title: TDengine 参数限制与保留关键字
---

## 名称命名规则

1. 合法字符：英文字符、数字和下划线
2. 允许英文字符或下划线开头，不允许以数字开头
3. 不区分大小写
4. 转义后表（列）名规则：
   为了兼容支持更多形式的表（列）名，TDengine 引入新的转义符 "`"。可用让表名与关键词不冲突，同时不受限于上述表名称合法性约束检查。
   转义后的表（列）名同样受到长度限制要求，且长度计算的时候不计算转义符。使用转义字符以后，不再对转义字符中的内容进行大小写统一。

   例如：\`aBc\` 和 \`abc\` 是不同的表（列）名，但是 abc 和 aBc 是相同的表（列）名。
   需要注意的是转义字符中的内容必须是可打印字符。
   支持转义符的功能从 2.3.0.1 版本开始。

## 密码合法字符集

`[a-zA-Z0-9!?$%^&*()_–+={[}]:;@~#|<,>.?/]`

去掉了 `` ‘“`\ `` (单双引号、撇号、反斜杠、空格)

- 数据库名：不能包含“.”以及特殊字符，不能超过 32 个字符
- 表名：不能包含“.”以及特殊字符，与所属数据库名一起，不能超过 192 个字节 ，每行数据最大长度 48KB
- 表的列名：不能包含特殊字符，不能超过 64 个字节
- 数据库名、表名、列名，都不能以数字开头，合法的可用字符集是“英文字符、数字和下划线”
- 表的列数：不能超过 1024 列，最少需要 2 列，第一列必须是时间戳（从 2.1.7.0 版本开始，改为最多支持 4096 列）
- 记录的最大长度：包括时间戳 8 字节，不能超过 48KB（每个 BINARY/NCHAR 类型的列还会额外占用 2 个 字节 的存储位置）
- 单条 SQL 语句默认最大字符串长度：1048576 字节，但可通过系统配置参数 maxSQLLength 修改，取值范围 65480 ~ 1048576 字节
- 数据库副本数：不能超过 3
- 用户名：不能超过 23 个 字节
- 用户密码：不能超过 15 个 字节
- 标签(Tags)数量：不能超过 128 个，可以 0 个
- 标签的总长度：不能超过 16KB
- 记录条数：仅受存储空间限制
- 表的个数：仅受节点个数限制
- 库的个数：仅受节点个数限制
- 单个库上虚拟节点个数：不能超过 64 个
- 库的数目，超级表的数目、表的数目，系统不做限制，仅受系统资源限制
- SELECT 语句的查询结果，最多允许返回 1024 列（语句中的函数调用可能也会占用一些列空间），超限时需要显式指定较少的返回数据列，以避免语句执行报错。（从 2.1.7.0 版本开始，改为最多允许 4096 列）

## 保留关键字

目前 TDengine 有将近 200 个内部保留关键字，这些关键字无论大小写均不可以用作库名、表名、STable 名、数据列名及标签列名等。这些关键字列表如下：

### A

- ABORT
- ACCOUNT
- ACCOUNTS
- ADD
- AFTER
- ALL
- ALTER
- AND
- AS
- ASC
- ATTACH

### B

- BEFORE
- BEGIN
- BETWEEN
- BIGINT
- BINARY
- BITAND
- BITNOT
- BITOR
- BLOCKS
- BOOL
- BY

### C

- CACHE
- CACHELAST
- CASCADE
- CHANGE
- CLUSTER
- COLON
- COLUMN
- COMMA
- COMP
- COMPACT
- CONCAT
- CONFLICT
- CONNECTION
- CONNECTIONS
- CONNS
- COPY
- CREATE
- CTIME

### D

- DATABASE  
- DATABASES 
- DAYS      
- DBS       
- DEFERRED  
- DELETE
- DELIMITERS
- DESC      
- DESCRIBE  
- DETACH    
- DISTINCT  
- DIVIDE    
- DNODE     
- DNODES    
- DOT       
- DOUBLE    
- DROP  

### E

- END     
- EQ      
- EXISTS  
- EXPLAIN 

### F

- FAIL   
- FILE   
- FILL   
- FLOAT  
- FOR    
- FROM   
- FSYNC  

### G

- GE    
- GLOB  
- GRANTS
- GROUP 
- GT  

### H

- HAVING 

### I

- ID
- IF
- IGNORE 
- IMMEDIA
- IMPORT 
- IN     
- INITIAL
- INSERT 
- INSTEAD
- INT    
- INTEGER
- INTERVA
- INTO   
- IS     
- ISNULL 

### J

- JOIN

### K

- KEEP
- KEY 
- KILL

### L

- LE    
- LIKE  
- LIMIT 
- LINEAR
- LOCAL 
- LP    
- LSHIFT
- LT 

### M

- MATCH    
- MAXROWS  
- MINROWS  
- MINUS    
- MNODES   
- MODIFY   
- MODULES  

### N

- NE     
- NONE   
- NOT    
- NOTNULL
- NOW    
- NULL

### O

- OF    
- OFFSET
- OR    
- ORDER 

### P

- PARTITION
- PASS     
- PLUS     
- PPS      
- PRECISION
- PREV     
- PRIVILEGE

### Q

- QTIME 
- QUERIE
- QUERY 
- QUORUM

### R

- RAISE  
- REM    
- REPLACE
- REPLICA
- RESET  
- RESTRIC
- ROW    
- RP     
- RSHIFT

### S

- SCORES 
- SELECT 
- SEMI   
- SESSION
- SET    
- SHOW   
- SLASH  
- SLIDING
- SLIMIT 
- SMALLIN
- SOFFSET
- STable 
- STableS
- STAR    
- STATE   
- STATEMEN
- STATE_WI
- STORAGE 
- STREAM  
- STREAMS 
- STRING  
- SYNCDB  

### T

- TABLE     
- TABLES    
- TAG       
- TAGS      
- TBNAME    
- TIMES     
- TIMESTAMP 
- TINYINT   
- TOPIC     
- TOPICS    
- TRIGGER   
- TSERIES   

### U

- UMINUS   
- UNION    
- UNSIGNED 
- UPDATE   
- UPLUS    
- USE      
- USER     
- USERS    
- USING  

### V

- VALUES   
- VARIABLE 
- VARIABLES
- VGROUPS  
- VIEW     
- VNODES   

### W

- WAL
- WHERE

### _

- _C0
- _QSTART
- _QSTOP
- _QDURATION
- _WSTART
- _WSTOP
- _WDURATION


## 特殊说明
### TBNAME
`TBNAME` 可以视为超级表中一个特殊的标签，代表子表的表名。

获取一个超级表所有的子表名及相关的标签信息：
```mysql
SELECT TBNAME, location FROM meters;

统计超级表下辖子表数量：
```mysql
SELECT COUNT(TBNAME) FROM meters;
```

以上两个查询均只支持在WHERE条件子句中添加针对标签（TAGS）的过滤条件。例如：
```mysql
taos> SELECT TBNAME, location FROM meters;
             tbname             |            location            |
==================================================================
 d1004                          | California.SanFrancisco        |
 d1003                          | California.SanFrancisco        |
 d1002                          | California.LosAngeles          |
 d1001                          | California.LosAngeles          |
Query OK, 4 row(s) in set (0.000881s)

taos> SELECT COUNT(tbname) FROM meters WHERE groupId > 2;
     count(tbname)     |
========================
                     2 |
Query OK, 1 row(s) in set (0.001091s)
```
### _QSTART/_QSTOP/_QDURATION
表示查询过滤窗口的起始，结束以及持续时间 (从2.6.0.0版本开始支持)

### _WSTART/_WSTOP/_WDURATION
窗口切分聚合查询（例如 interval/session window/state window）中表示每个切分窗口的起始，结束以及持续时间（从 2.6.0.0 版本开始支持）

### _c0
表示表或超级表的第一列