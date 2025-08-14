---
sidebar_label: 保留关键字
title: 保留关键字
description: TDengine TSDB 保留关键字的详细列表
---

## 保留关键字

目前 TDengine TSDB 有 200 多个内部保留关键字，这些关键字如果需要用作库名、表名、超级表名、子表名、数据列名及标签列名等，无论大小写，需要使用符号 `` ` `` 将关键字括起来使用，例如 \`ADD\`。

关键字列表如下：

### A

|关键字 | 说明|
|----------------------|-|
| ABORT                | |
| ACCOUNT              | |
| ACCOUNTS             | |
| ADD                  | |
| AFTER                | |
| AGGREGATE            | |
| ALIAS                | |
| ALIVE                | |
| ALL                  | |
| ALTER                | |
| ANALYZE              | 3.3.4.3+ |
| AND                  | |
| ANODE                | 3.3.4.3+ |
| ANODES               | 3.3.4.3+ |
| ANOMALY_WINDOW       | 3.3.4.3+ |
| ANTI                 | |
| APPS                 | |
| ARBGROUPS            | |
| ARROW                | |
| AS                   | |
| ASC                  | |
| ASOF                 | |
| ASYNC                | 3.3.6.0+ |
| AT_ONCE              | |
| ATTACH               | |
| AUTO                 | 3.3.5.0+ |
| ASSIGN               | 3.3.6.0+ |

### B

|关键字 | 说明|
|----------------------|-|
| BALANCE              | |
| BEFORE               | |
| BEGIN                | |
| BETWEEN              | |
| BIGINT               | |
| BIN                  | |
| BINARY               | |
| BITAND               | |
| BITAND               | |
| BITNOT               | |
| BITOR                | |
| BLOB                 | |
| BLOCKS               | |
| BNODE                | |
| BNODES               | |
| BOOL                 | |
| BOTH                 | |
| BUFFER               | |
| BUFSIZE              | |
| BWLIMIT              | |
| BY                   | |

### C

|关键字 | 说明|
|----------------------|-|
| CACHE                | |
| CACHEMODEL           | |
| CACHESIZE            | |
| CASE                 | |
| CAST                 | |
| CHANGE               | |
| CHILD                | |
| CLIENT_VERSION       | |
| CLUSTER              | |
| COLON                | |
| COLS                 | 3.3.6.0+ |
| COLUMN               | |
| COMMA                | |
| COMMENT              | |
| COMP                 | |
| COMPACT              | |
| COMPACTS             | |
| COMPACT_INTERVAL     | 3.3.5.0+ |
| COMPACT_TIME_OFFSET  | 3.3.5.0+ |
| COMPACT_TIME_RANGE   | 3.3.5.0+ |
| CONCAT               | |
| CONFLICT             | |
| CONNECTION           | |
| CONNECTIONS          | |
| CONNS                | |
| CONSUMER             | |
| CONSUMERS            | |
| CONTAINS             | |
| CONTINUOUS_WINDOW_CLOSE | 3.3.6.0+ |
| COPY                 | |
| COUNT                | |
| COUNT_WINDOW         | |
| CREATE               | |
| CREATEDB             | |
| CURRENT_USER         | |

### D

|关键字 | 说明|
|----------------------|-|
| DATABASE             | |
| DATABASES            | |
| DBS                  | |
| DECIMAL              | 3.3.6.0+ |
| DEFERRED             | |
| DELETE               | |
| DELETE_MARK          | |
| DELIMITERS           | |
| DESC                 | |
| DESCRIBE             | |
| DETACH               | |
| DISK_INFO            | 3.3.5.0+ |
| DISTINCT             | |
| DISTRIBUTED          | |
| DIVIDE               | |
| DNODE                | |
| DNODES               | |
| DOT                  | |
| DOUBLE               | |
| DROP                 | |
| DURATION             | |

### E

|关键字 | 说明|
|----------------------|-|
| EACH                 | |
| ELSE                 | |
| ENABLE               | |
| ENCRYPT_ALGORITHM    | |
| ENCRYPT_KEY          | |
| ENCRYPTIONS          | |
| END                  | |
| EQ                   | |
| EVENT_WINDOW         | |
| EVERY                | |
| EXCEPT               | |
| EXISTS               | |
| EXPIRED              | |
| EXPLAIN              | |

### F

|关键字 | 说明|
|----------------------|-|
| FAIL                 | |
| FHIGH                | 3.3.4.3+ |
| FILE                 | |
| FILL                 | |
| FILL_HISTORY         | |
| FIRST                | |
| FLOAT                | |
| FLOW                 | 3.3.4.3+ |
| FLUSH                | |
| FOR                  | |
| FORCE                | |
| FORCE_WINDOW_CLOSE   | 3.3.4.3+ |
| FROM                 | |
| FROWTS               | 3.3.4.3+ |
| FULL                 | |
| FUNCTION             | |
| FUNCTIONS            | |

### G

|关键字 | 说明|
|----------------------|-|
| GE                   | |
| GEOMETRY             | |
| GLOB                 | |
| GRANT                | |
| GRANTS               | |
| GROUP                | |
| GT                   | |

### H

|关键字 | 说明|
|----------------------|-|
| HAVING               | |
| HEX                  | |
| HOST                 | |

### I

|关键字 | 说明|
|----------------------|-|
| ID                   | |
| IF                   | |
| IGNORE               | |
| ILLEGAL              | |
| IMMEDIATE            | |
| IMPORT               | |
| IN                   | |
| INDEX                | |
| INDEXES              | |
| INITIALLY            | |
| INNER                | |
| INSERT               | |
| INSTEAD              | |
| INT                  | |
| INTEGER              | |
| INTERSECT            | |
| INTERVAL             | |
| INTO                 | |
| IPTOKEN              | |
| IROWTS               | |
| IROWTS_ORIGIN        | 3.3.5.0+ |
| IS                   | |
| IS_IMPORT            | |
| ISFILLED             | |
| ISNULL               | |

### J

|关键字 | 说明|
|----------------------|-|
| JLIMIT               | |
| JOIN                 | |
| JSON                 | |

### K

|关键字 | 说明|
|----------------------|-|
| KEEP                 | |
| KEEP_TIME_OFFSET     | |
| KEY                  | |
| KILL                 | |

### L

|关键字 | 说明|
|----------------------|-|
| LANGUAGE             | |
| LAST                 | |
| LAST_ROW             | |
| LE                   | |
| LEADER               | |
| LEADING              | |
| LEFT                 | |
| LEVEL                | 3.3.0.0 - 3.3.2.11 |
| LICENCES             | |
| LIKE                 | |
| LIMIT                | |
| LINEAR               | |
| LOCAL                | |
| LOGS                 | |
| LP                   | |
| LSHIFT               | |
| LT                   | |

### M

|关键字 | 说明|
|----------------------|-|
| MACHINES             | |
| MATCH                | |
| MAX_DELAY            | |
| MAXROWS              | |
| MEDIUMBLOB           | |
| MERGE                | |
| META                 | |
| META_ONLY            | 3.3.6.0+ |
| MINROWS              | |
| MINUS                | |
| MNODE                | |
| MNODES               | |
| MODIFY               | |
| MODULES              | |

### N

|关键字 | 说明|
|----------------------|-|
| NCHAR                | |
| NE                   | |
| NEXT                 | |
| NMATCH               | |
| NONE                 | |
| NORMAL               | |
| NOT                  | |
| NOTIFY               | 3.3.6.0+ |
| NOTIFY_HISTORY       | 3.3.6.0+ |
| NOTNULL              | |
| NOW                  | |
| NULL                 | |
| NULL_F               | |
| NULLS                | |

### O

|关键字 | 说明|
|----------------------|-|
| OF                   | |
| OFFSET               | |
| ON                   | |
| ONLY                 | |
| ON_FAILURE           | 3.3.6.0+ |
| OR                   | |
| ORDER                | |
| OUTER                | |
| OUTPUTTYPE           | |

### P

|关键字 | 说明|
|----------------------|-|
| PAGES                | |
| PAGESIZE             | |
| PARTITION            | |
| PASS                 | |
| PAUSE                | |
| PI                   | |
| PLUS                 | |
| PORT                 | |
| POSITION             | |
| PPS                  | |
| PRECISION            | |
| PREV                 | |
| PRIMARY              | |
| PRIVILEGE            | |
| PRIVILEGES           | |

### Q

|关键字 | 说明|
|----------------------|-|
| QDURATION            | |
| QEND                 | |
| QNODE                | |
| QNODES               | |
| QSTART               | |
| QTAGS                | |
| QTIME                | |
| QUERIES              | |
| QUERY                | |
| QUESTION             | |

### R

|关键字 | 说明|
|----------------------|-|
| RAISE                | |
| RAND                 | |
| RANGE                | |
| RATIO                | |
| READ                 | |
| RECURSIVE            | |
| REGEXP               | 3.3.6.0+ |
| REDISTRIBUTE         | |
| REM                  | |
| REPLACE              | |
| REPLICA              | |
| RESET                | |
| RESTORE              | |
| RESTRICT             | |
| RESUME               | |
| RETENTIONS           | |
| REVOKE               | |
| RIGHT                | |
| ROLLUP               | |
| ROW                  | |
| ROWTS                | |
| RP                   | |
| RSHIFT               | |

### S

|关键字 | 说明|
|----------------------|-|
| SCHEMALESS           | |
| SCORES               | |
| SELECT               | |
| SEMI                 | |
| SERVER_STATUS        | |
| SERVER_VERSION       | |
| SESSION              | |
| SET                  | |
| SHOW                 | |
| SINGLE_STABLE        | |
| SLASH                | |
| SLIDING              | |
| SLIMIT               | |
| SMA                  | |
| SMALLINT             | |
| SMIGRATE             | |
| SNODE                | |
| SNODES               | |
| SOFFSET              | |
| SPLIT                | |
| SS_CHUNKPAGES        | |
| SS_COMPACT           | |
| SS_KEEPLOCAL         | |
| SSMIGRATE            | |
| STABLE               | |
| STABLES              | |
| STAR                 | |
| START                | |
| STATE                | |
| STATE_WINDOW         | |
| STATEMENT            | |
| STORAGE              | |
| STREAM               | |
| STREAMS              | |
| STRICT               | |
| STRING               | |
| STT_TRIGGER          | |
| SUBSCRIBE            | |
| SUBSCRIPTIONS        | |
| SUBSTR               | |
| SUBSTRING            | |
| SUBTABLE             | |
| SYSINFO              | |
| SYSTEM               | |

### T

|关键字 | 说明|
|----------------------|-|
| TABLE                | |
| TABLE_PREFIX         | |
| TABLE_SUFFIX         | |
| TABLES               | |
| TAG                  | |
| TAGS                 | |
| TBNAME               | |
| THEN                 | |
| TIMES                | |
| TIMESTAMP            | |
| TIMEZONE             | |
| TINYINT              | |
| TO                   | |
| TODAY                | |
| TOPIC                | |
| TOPICS               | |
| TRAILING             | |
| TRANSACTION          | |
| TRANSACTIONS         | |
| TRIGGER              | |
| TRIM                 | |
| TRUE_FOR             | 3.3.6.0+ |
| TSDB_PAGESIZE        | |
| TSERIES              | |
| TSMA                 | |
| TSMAS                | |
| TTL                  | |

### U

|关键字 | 说明|
|----------------------|-|
| UNION                | |
| UNSAFE               | |
| UNSIGNED             | |
| UNTREATED            | |
| UPDATE               | |
| USE                  | |
| USER                 | |
| USERS                | |
| USING                | |

### V

|关键字 | 说明|
|----------------------|-|
| VALUE                | |
| VALUE_F              | |
| VALUES               | |
| VARBINARY            | |
| VARCHAR              | |
| VARIABLE             | |
| VARIABLES            | |
| VERBOSE              | |
| VGROUP               | |
| VGROUPS              | |
| VIEW                 | |
| VIEWS                | |
| VNODE                | |
| VNODES               | |

### W

|关键字 | 说明|
|----------------------|-|
| WAL                  | |
| WAL_FSYNC_PERIOD     | |
| WAL_LEVEL            | |
| WAL_RETENTION_PERIOD | |
| WAL_RETENTION_SIZE   | |
| WAL_ROLL_PERIOD      | |
| WAL_SEGMENT_SIZE     | |
| WATERMARK            | |
| WDURATION            | |
| WEND                 | |
| WHEN                 | |
| WHERE                | |
| WINDOW               | |
| WINDOW_CLOSE         | |
| WINDOW_OFFSET        | |
| WITH                 | |
| WRITE                | |
| WSTART               | |

### \_

- \_C0
- \_IROWTS
- \_QDURATION
- \_QEND
- \_QSTART
- \_ROWTS
- \_WDURATION
- \_WEND
- \_WSTART
