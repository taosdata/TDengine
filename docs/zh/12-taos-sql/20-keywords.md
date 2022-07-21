---
sidebar_label: 保留关键字 
title: TDengine 保留关键字
---

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
```

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
表示查询过滤窗口的起始，结束以及持续时间。

### _WSTART/_WSTOP/_WDURATION
窗口切分聚合查询（例如 interval/session window/state window）中表示每个切分窗口的起始，结束以及持续时间。

### _c0/_ROWTS
_c0 _ROWTS 等价，表示表或超级表的第一列
