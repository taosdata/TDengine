---
title: Keywords
---

There are about 200 keywords reserved by TDengine, they can't be used as the name of database, STable or table with either upper case, lower case or mixed case.

## Keyword List

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

## Explanations
### TBNAME
`TBNAME` can be considered as a special tag, which represents the name of the subtable, in STable.

Get the table name and tag values of all subtables in a STable.
```mysql
SELECT TBNAME, location FROM meters;

Count the number of subtables in a STable.
```mysql
SELECT COUNT(TBNAME) FROM meters;
```

Only filter on TAGS can be used in WHERE clause in the above two query statements.
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
The start, stop and duration of a query time window (Since version 2.6.0.0).

### _WSTART/_WSTOP/_WDURATION
The start, stop and duration of aggegate query by time window, like interval, session window, state window (Since version 2.6.0.0).

### _c0
The first column of a table or STable.
