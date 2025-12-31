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
| ASYNC                | 3.3.6.0 - 3.3.7.0 |
| AT_ONCE              | 3.0.0.0 - 3.3.7.0 |
| ATTACH               | |
| AUTO                 | 3.3.5.0+ |
| ASSIGN               | 3.3.6.0+ |
| ALGR_NAME            | 3.4.0.0+ |
| ALGR_TYPE            | 3.4.0.0+ |

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
| CALC_NOTIFY_ONLY     | 3.3.7.0+ |
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
| COMPRESS             | |
| CONCAT               | |
| CONFLICT             | |
| CONNECTION           | |
| CONNECTIONS          | |
| CONNS                | |
| CONSUMER             | |
| CONSUMERS            | |
| CONTAINS             | |
| CONTINUOUS_WINDOW_CLOSE | 3.3.6.0 - 3.3.7.0 |
| COPY                 | |
| COUNT                | |
| COUNT_WINDOW         | |
| CREATE               | |
| CREATEDB             | |
| CURRENT_USER         | |
| SCAN                 | |
| SCANS                | |

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
| DELETE_OUTPUT_TABLE  | 3.3.7.0+ |
| DELETE_RECALC        | 3.3.7.0+ |
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
| ENCODE               | |
| ENABLE               | |
| ENCRYPT_ALGORITHM    | |
| ENCRYPT_KEY          | |
| ENCRYPTIONS          | |
| END                  | |
| EQ                   | |
| EVENT_TYPE           | 3.3.7.0+ |
| EVENT_WINDOW         | |
| EVERY                | |
| EXCEPT               | |
| EXISTS               | |
| EXPIRED              | 3.0.0.0 - 3.3.7.0 |
| EXPIRED_TIME         | 3.3.7.0+ |
| EXPLAIN              | |
| ENCRYPT_ALGORITHMS   | 3.4.0.0+ |
| ENCRYPT_ALGR         | 3.4.0.0+ |

### F

|关键字 | 说明|
|----------------------|-|
| FAIL                 | |
| FHIGH                | 3.3.4.3+ |
| FILE                 | |
| FILL                 | |
| FILL_HISTORY         | |
| FILL_HISTORY_FIRST   | 3.3.7.0+ |
| FIRST                | |
| FLOAT                | |
| FLOW                 | 3.3.4.3+ |
| FLUSH                | |
| FOR                  | |
| FORCE                | |
| FORCE_OUTPUT         | 3.3.7.0+ |
| FORCE_WINDOW_CLOSE   | 3.3.4.3 - 3.3.7.0 |
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
| IGNORE_DISORDER      | 3.3.7.0+ |
| IGNORE_NODATA_TRIGGER| 3.3.7.0+ |
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
| IS_AUDIT             | 3.3.9.0+ |

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
| LOW_LATENCY_CALC     | 3.3.7.0+ |
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
| NEAR                 | |
| NCHAR                | |
| NE                   | |
| NEXT                 | |
| NMATCH               | |
| NONE                 | |
| NORMAL               | |
| NOT                  | |
| NOTIFY               | 3.3.6.0+ |
| NOTIFY_HISTORY       | 3.3.6.0+ |
| NOTIFY_OPTIONS       | 3.3.7.0+ |
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
| ON_FAILURE           | 3.3.6.0 - 3.3.7.0 |
| ON_FAILURE_PAUSE     | 3.3.7.0+ |
| OPTIONS              | 3.3.7.0+ |
| OR                   | |
| ORDER                | |
| OUTER                | |
| OUTPUT_SUBTABLE      | 3.3.7.0+ |
| OUTPUTTYPE           | |
| OSSL_ALGR_NAME       | 3.4.0.0+ |

### P

|关键字 | 说明|
|----------------------|-|
| PAGES                | |
| PAGESIZE             | |
| PARTITION            | |
| PASS                 | |
| PAUSE                | |
| PERIOD               | 3.3.7.0+ |
| PI                   | |
| PLUS                 | |
| PORT                 | |
| POSITION             | |
| PPS                  | |
| PRE_FILTER           | 3.3.7.0+ |
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
| RECALCULATE          | |
| RECURSIVE            | |
| REGEXP               | 3.3.6.0+ |
| REDISTRIBUTE         | |
| REM                  | |
| RENAME               | |
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
| S3MIGRATE            | |
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
| SUBTABLE             | 3.0.0.0 - 3.3.7.0 |
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
| TRIGGER              | 3.0.0.0 - 3.3.7.0 |
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
| VIRTUAL              | |
| VNODE                | |
| VNODES               | |
| VTABLE               | |
| VTABLES              | |

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
| WINDOW_OPEN          | 3.3.7.0+ |
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
