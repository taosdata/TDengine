---
title: Keywords
---

There are about 200 keywords reserved by TDengine, they can't be used as the name of database, STable or table with either upper case, lower case or mixed case.

**Keywords List**

|   |            |           |            |              |
| ----------- | ---------- | --------- | ---------- | ------------ |
| ABORT       | CREATE     | IGNORE    | NULL       | STAR         |
| ACCOUNT     | CTIME      | IMMEDIATE | OF         | STATE        |
| ACCOUNTS    | DATABASE   | IMPORT    | OFFSET     | STATEMENT    |
| ADD         | DATABASES  | IN        | OR         | STATE_WINDOW |
| AFTER       | DAYS       | INITIALLY | ORDER      | STORAGE      |
| ALL         | DBS        | INSERT    | PARTITIONS | STREAM       |
| ALTER       | DEFERRED   | INSTEAD   | PASS       | STREAMS      |
| AND         | DELIMITERS | INT       | PLUS       | STRING       |
| AS          | DESC       | INTEGER   | PPS        | SYNCDB       |
| ASC         | DESCRIBE   | INTERVAL  | PRECISION  | TABLE        |
| ATTACH      | DETACH     | INTO      | PREV       | TABLES       |
| BEFORE      | DISTINCT   | IS        | PRIVILEGE  | TAG          |
| BEGIN       | DIVIDE     | ISNULL    | QTIME      | TAGS         |
| BETWEEN     | DNODE      | JOIN      | QUERIES    | TBNAME       |
| BIGINT      | DNODES     | KEEP      | QUERY      | TIMES        |
| BINARY      | DOT        | KEY       | QUORUM     | TIMESTAMP    |
| BITAND      | DOUBLE     | KILL      | RAISE      | TINYINT      |
| BITNOT      | DROP       | LE        | REM        | TOPIC        |
| BITOR       | EACH       | LIKE      | REPLACE    | TOPICS       |
| BLOCKS      | END        | LIMIT     | REPLICA    | TRIGGER      |
| BOOL        | EQ         | LINEAR    | RESET      | TSERIES      |
| BY          | EXISTS     | LOCAL     | RESTRICT   | UMINUS       |
| CACHE       | EXPLAIN    | LP        | ROW        | UNION        |
| CACHELAST   | FAIL       | LSHIFT    | RP         | UNSIGNED     |
| CASCADE     | FILE       | LT        | RSHIFT     | UPDATE       |
| CHANGE      | FILL       | MATCH     | SCORES     | UPLUS        |
| CLUSTER     | FLOAT      | MAXROWS   | SELECT     | USE          |
| COLON       | FOR        | MINROWS   | SEMI       | USER         |
| COLUMN      | FROM       | MINUS     | SESSION    | USERS        |
| COMMA       | FSYNC      | MNODES    | SET        | USING        |
| COMP        | GE         | MODIFY    | SHOW       | VALUES       |
| COMPACT     | GLOB       | MODULES   | SLASH      | VARIABLE     |
| CONCAT      | GRANTS     | NCHAR     | SLIDING    | VARIABLES    |
| CONFLICT    | GROUP      | NE        | SLIMIT     | VGROUPS      |
| CONNECTION  | GT         | NONE      | SMALLINT   | VIEW         |
| CONNECTIONS | HAVING     | NOT       | SOFFSET    | VNODES       |
| CONNS       | ID         | NOTNULL   | STable     | WAL          |
| COPY        | IF         | NOW       | STableS    | WHERE        |
| _C0         | _QSTART    | _QSTOP    | _QDURATION | _WSTART      |
| _WSTOP      | _WDURATION |

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