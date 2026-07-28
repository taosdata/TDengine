---
sidebar_label: 保留关键字
title: 保留关键字
description: 保留关键字列表
---

## 保留关键字

目前 TDengine 共列出 **584** 个保留关键字，其中 **498** 个已在当前版本词法分析中启用；其余 **86** 个为语法层仍保留定义、或历史文档中的关键字，即便暂时未启用也一并保留，便于对照与后续使用。

这些关键字若要用作库名、表名、超级表名、子表名、数据列名或标签列名等，无论大小写，均需使用反引号 `` ` `` 括起，例如 `` `ADD` ``。

说明列中的版本号表示该关键字自该版本起生效；若为版本区间，表示仅在该区间内为关键字（其后已移除或调整）。

关键字列表如下：

### A

| **关键字**         | **说明** |
| ----------------- | --- |
| `ABORT`           | |
| `ACCOUNT`         | |
| `ACCOUNTS`        | |
| `ADD`             | |
| `AES_DECRYPT`     | `v3.4.0.3`+ |
| `AES_ENCRYPT`     | `v3.4.0.3`+ |
| `AFTER`           | |
| `AGG`             | `v3.4.2.0`+ |
| `AGGREGATE`       | |
| `ALGR_NAME`       | `v3.4.0.0`+ |
| `ALGR_TYPE`       | `v3.4.0.0`+ |
| `ALIAS`           | |
| `ALIVE`           | |
| `ALL`             | |
| `ALLOW_DATETIME`  | `v3.4.0.0`+ |
| `ALLOW_DROP`      | |
| `ALLOW_TOKEN_NUM` | `v3.4.0.0`+ |
| `ALTER`           | |
| `ANALYZE`         | `v3.3.4.3`+ |
| `AND`             | |
| `ANODE`           | `v3.3.4.3`+ |
| `ANODES`          | `v3.3.4.3`+ |
| `ANOMALY_WINDOW`  | `v3.3.4.3`+ |
| `ANTI`            | |
| `ANY`             | `v3.4.1.0`+ |
| `API_TOKEN`       | `v3.4.2.0`+ |
| `APPS`            | |
| `ARBGROUPS`       | |
| `ARROW`           | |
| `AS`              | |
| `ASC`             | |
| `ASOF`            | |
| `ASSIGN`          | `v3.3.6.0`+ |
| `ASYNC`           | `v3.3.6.0` - `v3.3.7.0` |
| `ATTACH`          | |
| `AT_ONCE`         | `v3.0.0.0` - `v3.3.7.0` |
| `AUTO`            | `v3.3.5.0`+ |

### B

| **关键字**    | **说明** |
| ------------ | --- |
| `BALANCE`    | |
| `BASE`       | `v3.4.2.0`+ |
| `BATCH_SCAN` | |
| `BEFORE`     | |
| `BEGIN`      | |
| `BETWEEN`    | |
| `BIGINT`     | |
| `BIN`        | |
| `BINARY`     | |
| `BITAND`     | |
| `BITNOT`     | |
| `BITOR`      | |
| `BLOB`       | |
| `BLOCKS`     | |
| `BNODE`      | |
| `BNODES`     | |
| `BOOL`       | |
| `BOTH`       | |
| `BUFFER`     | |
| `BUFSIZE`    | |
| `BWLIMIT`    | |
| `BY`         | |

### C

| **关键字**                 | **说明** |
| ------------------------- | --- |
| `CACHE`                   | |
| `CACHEMODEL`              | |
| `CACHESHARDBITS`          | |
| `CACHESIZE`               | |
| `CALC_NOTIFY_ONLY`        | `v3.3.7.0`+ |
| `CALL_PER_SESSION`        | `v3.4.0.0`+ |
| `CASE`                    | |
| `CAST`                    | |
| `CHANGE`                  | |
| `CHANGEPASS`              | `v3.4.0.0`+ |
| `CHILD`                   | |
| `CLIENT_VERSION`          | |
| `CLOSE`                   | `v3.4.2.0`+ |
| `CLUSTER`                 | |
| `COALESCE`                | |
| `COLON`                   | |
| `COLS`                    | `v3.3.6.0`+ |
| `COLUMN`                  | |
| `COMMA`                   | |
| `COMMENT`                 | |
| `COMMIT`                  | `v3.4.2.0`+ |
| `COMP`                    | |
| `COMPACT`                 | |
| `COMPACTS`                | |
| `COMPACT_INTERVAL`        | `v3.3.5.0`+ |
| `COMPACT_TIME_OFFSET`     | `v3.3.5.0`+ |
| `COMPACT_TIME_RANGE`      | `v3.3.5.0`+ |
| `COMPOSITE`               | `v3.3.6.3`+ |
| `COMPRESS`                | |
| `CONCAT`                  | |
| `CONFLICT`                | |
| `CONNECTION`              | |
| `CONNECTIONS`             | |
| `CONNECT_IDLE_TIME`       | `v3.4.0.0`+ |
| `CONNECT_TIME`            | `v3.4.0.0`+ |
| `CONNS`                   | |
| `CONSUMER`                | |
| `CONSUMERS`               | |
| `CONTAINS`                | |
| `CONTINUOUS_WINDOW_CLOSE` | `v3.3.6.0` - `v3.3.7.0` |
| `COPY`                    | |
| `COUNT`                   | |
| `COUNT_WINDOW`            | |
| `CPU_ALLOCATION`          | `v3.4.2.0`+ |
| `CREATE`                  | |
| `CREATEDB`                | |
| `CURRENT`                 | `v3.4.2.0`+ |
| `CURRENT_USER`            | |

### D

| **关键字**             | **说明** |
| --------------------- | --- |
| `DATABASE`            | |
| `DATABASES`           | |
| `DAYS`                | |
| `DBS`                 | |
| `DB_KEY`              | |
| `DECIMAL`             | `v3.3.6.0`+ |
| `DEFAULT`             | `v3.4.0.0`+ |
| `DEFERRED`            | |
| `DELETE`              | |
| `DELETE_MARK`         | |
| `DELETE_OUTPUT_TABLE` | `v3.3.7.0`+ |
| `DELETE_RECALC`       | `v3.3.7.0`+ |
| `DELIMITERS`          | |
| `DESC`                | |
| `DESCRIBE`            | |
| `DETACH`              | |
| `DISK_INFO`           | `v3.3.5.0`+ |
| `DISTINCT`            | |
| `DISTRIBUTED`         | |
| `DIVIDE`              | |
| `DNODE`               | |
| `DNODES`              | |
| `DOT`                 | |
| `DOUBLE`              | |
| `DRAIN`               | `v3.3.7.0`+ |
| `DROP`                | |
| `DURATION`            | |

### E

| **关键字**            | **说明** |
| -------------------- | --- |
| `EACH`               | |
| `ELSE`               | |
| `ENABLE`             | |
| `ENCODE`             | |
| `ENCRYPTIONS`        | |
| `ENCRYPT_ALGORITHM`  | |
| `ENCRYPT_ALGORITHMS` | `v3.4.0.0`+ |
| `ENCRYPT_ALGR`       | `v3.4.0.0`+ |
| `ENCRYPT_KEY`        | |
| `ENCRYPT_STATUS`     | |
| `END`                | |
| `EQ`                 | |
| `EVENT_TYPE`         | `v3.3.7.0`+ |
| `EVENT_WINDOW`       | |
| `EVERY`              | |
| `EXCEPT`             | |
| `EXISTS`             | |
| `EXPIRED`            | `v3.0.0.0` - `v3.3.7.0` |
| `EXPIRED_TIME`       | `v3.3.7.0`+ |
| `EXPLAIN`            | |
| `EXTEND`             | `v3.4.2.0`+ |
| `EXTERNAL`           | `v3.4.2.0`+ |
| `EXTERNAL_WINDOW`    | `v3.3.7.0`+ |
| `EXTRA_INFO`         | `v3.4.0.0`+ |

### F

| **关键字**               | **说明** |
| ----------------------- | --- |
| `FAIL`                  | |
| `FAILED_LOGIN_ATTEMPTS` | `v3.4.0.0`+ |
| `FHIGH`                 | `v3.3.4.3`+ |
| `FILE`                  | |
| `FILL`                  | |
| `FILL_HISTORY`          | |
| `FILL_HISTORY_FIRST`    | `v3.3.7.0`+ |
| `FIRST`                 | |
| `FIRST_DAY_OF_WEEK`     | `v3.4.2.0`+ |
| `FLOAT`                 | |
| `FLOW`                  | `v3.3.4.3`+ |
| `FLUSH`                 | |
| `FOLLOWING`             | `v3.4.2.0`+ |
| `FOR`                   | |
| `FORCE`                 | |
| `FORCE_OUTPUT`          | `v3.3.7.0`+ |
| `FORCE_WINDOW_CLOSE`    | `v3.3.4.3` - `v3.3.7.0` |
| `FROM`                  | |
| `FROM_BASE64`           | `v3.4.0.3`+ |
| `FROWTS`                | `v3.3.4.3`+ |
| `FULL`                  | |
| `FUNCTION`              | |
| `FUNCTIONS`             | |

### G

| **关键字**  | **说明** |
| ---------- | --- |
| `GE`       | |
| `GEOMETRY` | |
| `GLOB`     | |
| `GRANT`    | |
| `GRANTS`   | |
| `GROUP`    | |
| `GT`       | |

### H

| **关键字**     | **说明** |
| ----------- | --- |
| `HASH_JOIN` | |
| `HAVING`    | |
| `HEX`       | |
| `HOST`      | |

### I

| **关键字**               | **说明** |
| ----------------------- | --- |
| `ID`                    | |
| `IDLE`                  | `v3.3.4.0`+ |
| `IDLE_TIMEOUT`          | `v3.3.4.0`+ |
| `IF`                    | |
| `IFNULL`                | |
| `IGNORE`                | |
| `IGNORE_DISORDER`       | `v3.3.7.0`+ |
| `IGNORE_NODATA_TRIGGER` | `v3.3.7.0`+ |
| `ILLEGAL`               | |
| `IMMEDIATE`             | |
| `IMPORT`                | |
| `IN`                    | |
| `INACTIVE_ACCOUNT_TIME` | `v3.4.0.0`+ |
| `INDEX`                 | |
| `INDEXES`               | |
| `INHERITS`              | `v3.4.2.0`+ |
| `INITIALLY`             | |
| `INNER`                 | |
| `INSERT`                | |
| `INSTANCES`             | |
| `INSTEAD`               | |
| `INT`                   | |
| `INTEGER`               | |
| `INTERSECT`             | |
| `INTERVAL`              | |
| `INTO`                  | |
| `IPTOKEN`               | |
| `IROWTS`                | |
| `IROWTS_ORIGIN`         | `v3.3.5.0`+ |
| `IS`                    | |
| `ISFILLED`              | |
| `ISNOTNULL`             | |
| `ISNULL`                | |
| `IS_AUDIT`              | `v3.3.9.0`+ |
| `IS_IMPORT`             | |

### J

| **关键字**  | **说明** |
| -------- | --- |
| `JLIMIT` | |
| `JOIN`   | |
| `JSON`   | |

### K

| **关键字**          | **说明** |
| ------------------ | --- |
| `KEEP`             | |
| `KEEP_TIME_OFFSET` | |
| `KEY`              | |
| `KEY_EXPIRATION`   | |
| `KILL`             | |

### L

| **关键字**          | **说明** |
| ------------------ | --- |
| `LANGUAGE`         | |
| `LAST`             | |
| `LAST_ROW`         | |
| `LE`               | |
| `LEADER`           | |
| `LEADING`          | |
| `LEFT`             | |
| `LEVEL`            | `v3.3.0.0` - `v3.3.2.11` |
| `LICENCES`         | |
| `LIKE`             | |
| `LIMIT`            | |
| `LINEAR`           | |
| `LOCAL`            | |
| `LOCK`             | |
| `LOGS`             | |
| `LOW_LATENCY_CALC` | `v3.3.7.0`+ |
| `LP`               | |
| `LSHIFT`           | |
| `LT`               | |

### M

| **关键字**    | **说明** |
| ------------ | --- |
| `MACHINES`   | |
| `MASK`       | |
| `MATCH`      | |
| `MAXROWS`    | |
| `MAX_DELAY`  | |
| `MD5`        | `v3.4.0.3`+ |
| `MEDIUMBLOB` | |
| `MERGE`      | |
| `META`       | |
| `META_ONLY`  | `v3.3.6.0`+ |
| `MINROWS`    | |
| `MINUS`      | |
| `MNODE`      | |
| `MNODES`     | |
| `MODIFY`     | |
| `MODULES`    | |
| `MOUNT`      | |
| `MOUNTS`     | |

### N

| **关键字**                 | **说明** |
| ------------------------- | --- |
| `NCHAR`                   | |
| `NE`                      | |
| `NEAR`                    | |
| `NEXT`                    | |
| `NMATCH`                  | |
| `NODE`                    | |
| `NODELAY_CREATE_SUBTABLE` | `v3.4.1.0`+ |
| `NODES`                   | |
| `NONE`                    | |
| `NORMAL`                  | |
| `NOT`                     | |
| `NOTIFY`                  | `v3.3.6.0`+ |
| `NOTIFY_HISTORY`          | `v3.3.6.0`+ |
| `NOTIFY_OPTIONS`          | `v3.3.7.0`+ |
| `NOTNULL`                 | |
| `NOT_ALLOW_DATETIME`      | `v3.4.0.0`+ |
| `NOT_ALLOW_HOST`          | `v3.4.0.0`+ |
| `NOW`                     | |
| `NO_BATCH_SCAN`           | |
| `NO_ZEROTH`               | `v3.4.2.0`+ |
| `NULL`                    | |
| `NULLIF`                  | |
| `NULLS`                   | |
| `NULL_F`                  | |
| `NVL`                     | |
| `NVL2`                    | |

### O

| **关键字**          | **说明** |
| ------------------ | --- |
| `OF`               | |
| `OFFSET`           | |
| `ON`               | |
| `ONLY`             | |
| `ON_FAILURE`       | `v3.3.6.0` - `v3.3.7.0` |
| `ON_FAILURE_PAUSE` | `v3.3.7.0`+ |
| `OPEN`             | `v3.4.2.0`+ |
| `OPTIONS`          | `v3.3.7.0`+ |
| `OR`               | |
| `ORDER`            | |
| `ORPHANS`          | `v3.4.2.0`+ |
| `OSSL_ALGR_NAME`   | `v3.4.0.0`+ |
| `OUTER`            | |
| `OUTPUTTYPE`       | |
| `OUTPUT_SUBTABLE`  | `v3.3.7.0`+ |
| `OVER`             | `v3.4.2.0`+ |

### P

| **关键字**             | **说明** |
| --------------------- | --- |
| `PAGES`               | |
| `PAGESIZE`            | |
| `PARALLEL`            | `v3.4.2.0`+ |
| `PARA_TABLES_SORT`    | |
| `PARTITION`           | |
| `PARTITION_FIRST`     | |
| `PASS`                | |
| `PASSWORD`            | `v3.4.2.0`+ |
| `PASSWORD_GRACE_TIME` | `v3.4.0.0`+ |
| `PASSWORD_LIFE_TIME`  | `v3.4.0.0`+ |
| `PASSWORD_LOCK_TIME`  | `v3.4.0.0`+ |
| `PASSWORD_REUSE_MAX`  | `v3.4.0.0`+ |
| `PASSWORD_REUSE_TIME` | `v3.4.0.0`+ |
| `PAUSE`               | |
| `PERIOD`              | `v3.3.7.0`+ |
| `PI`                  | |
| `PLUS`                | |
| `PORT`                | |
| `POSITION`            | |
| `PPS`                 | |
| `PRECEDING`           | `v3.4.2.0`+ |
| `PRECISION`           | |
| `PREV`                | |
| `PRE_FILTER`          | `v3.3.7.0`+ |
| `PRIMARY`             | |
| `PRIVILEGE`           | |
| `PRIVILEGES`          | |
| `PROVIDER`            | `v3.4.0.0`+ |

### Q

| **关键字**   | **说明** |
| ----------- | --- |
| `QDURATION` | |
| `QEND`      | |
| `QNODE`     | |
| `QNODES`    | |
| `QSTART`    | |
| `QTAGS`     | |
| `QTIME`     | |
| `QUERIES`   | |
| `QUERY`     | |
| `QUESTION`  | |

### R

| **关键字**        | **说明** |
| ---------------- | --- |
| `RAISE`          | |
| `RAND`           | |
| `RANGE`          | |
| `RATIO`          | |
| `READ`           | |
| `REBALANCE`      | `v3.3.7.0`+ |
| `RECALCULATE`    | |
| `RECURSIVE`      | |
| `REDISTRIBUTE`   | |
| `REFRESH`        | `v3.4.2.0`+ |
| `REGEXP`         | `v3.3.6.0`+ |
| `REGEXP_REPLACE` | |
| `RELOAD`         | |
| `REM`            | |
| `REMOVE`         | `v3.4.2.0`+ |
| `RENAME`         | |
| `REPLACE`        | |
| `REPLICA`        | |
| `REPLICAS`       | `v3.3.7.0`+ |
| `RESET`          | |
| `RESTORE`        | |
| `RESTRICT`       | |
| `RESUME`         | |
| `RETENTION`      | |
| `RETENTIONS`     | |
| `REVOKE`         | |
| `RIGHT`          | |
| `ROLE`           | |
| `ROLES`          | |
| `ROLLBACK`       | `v3.4.2.0`+ |
| `ROLLUP`         | |
| `ROW`            | `v3.4.2.0`+ |
| `ROWS`           | `v3.4.2.0`+ |
| `ROWTS`          | |
| `RP`             | |
| `RSHIFT`         | |
| `RSMA`           | |
| `RSMAS`          | |

### S

| **关键字**             | **说明** |
| --------------------- | --- |
| `S3MIGRATE`           | |
| `SCALAR`              | `v3.4.2.0`+ |
| `SCAN`                | |
| `SCANS`               | |
| `SCHEMA`              | `v3.4.2.0`+ |
| `SCHEMALESS`          | |
| `SCORES`              | |
| `SECURE_DELETE`       | |
| `SECURITY_LEVEL`      | `v3.4.1.6`+ |
| `SECURITY_POLICIES`   | `v3.4.1.6`+ |
| `SELECT`              | |
| `SEMI`                | |
| `SERIES`              | `v3.4.2.0`+ |
| `SERVER_STATUS`       | |
| `SERVER_VERSION`      | |
| `SESSION`             | |
| `SESSION_PER_USER`    | `v3.4.0.0`+ |
| `SET`                 | |
| `SHA`                 | `v3.4.0.3`+ |
| `SHA1`                | `v3.4.0.3`+ |
| `SHA2`                | `v3.4.0.3`+ |
| `SHOW`                | |
| `SINGLE_STABLE`       | |
| `SKIP_TSMA`           | |
| `SLASH`               | |
| `SLIDING`             | |
| `SLIMIT`              | |
| `SM4_DECRYPT`         | `v3.4.0.3`+ |
| `SM4_ENCRYPT`         | `v3.4.0.3`+ |
| `SMA`                 | |
| `SMALLDATA_SCAN_SORT` | `v3.4.2.0`+ |
| `SMALLDATA_TS_SORT`   | |
| `SMALLINT`            | |
| `SMIGRATE`            | |
| `SNODE`               | |
| `SNODES`              | |
| `SOFFSET`             | |
| `SOME`                | `v3.4.1.0`+ |
| `SORT_FOR_GROUP`      | |
| `SOURCE`              | `v3.4.2.0`+ |
| `SOURCES`             | `v3.4.2.0`+ |
| `SPLIT`               | |
| `SSMIGRATE`           | |
| `SSMIGRATES`          | |
| `SS_CHUNKPAGES`       | |
| `SS_COMPACT`          | |
| `SS_KEEPLOCAL`        | |
| `STABLE`              | |
| `STABLES`             | |
| `STAR`                | |
| `START`               | |
| `STATE`               | |
| `STATEMENT`           | |
| `STATE_WINDOW`        | |
| `STOP`                | |
| `STORAGE`             | |
| `STRATEGY`            | |
| `STREAM`              | |
| `STREAMS`             | |
| `STREAM_OPTIONS`      | `v3.3.7.0`+ |
| `STRICT`              | |
| `STRING`              | |
| `STT_TRIGGER`         | |
| `SUBSCRIBE`           | |
| `SUBSCRIPTIONS`       | |
| `SUBSTR`              | |
| `SUBSTRING`           | |
| `SUBTABLE`            | `v3.0.0.0` - `v3.3.7.0` |
| `SURROUND`            | |
| `SVR_KEY`             | |
| `SYSINFO`             | |
| `SYSTEM`              | |

### T

| **关键字**       | **说明** |
| --------------- | --- |
| `TABLE`         | |
| `TABLES`        | |
| `TABLE_PREFIX`  | |
| `TABLE_SUFFIX`  | |
| `TAG`           | |
| `TAGS`          | |
| `TBNAME`        | |
| `TEXT`          | `v3.4.2.0`+ |
| `THEN`          | |
| `TIMES`         | |
| `TIMESTAMP`     | |
| `TIMEZONE`      | |
| `TINYINT`       | |
| `TO`            | |
| `TODAY`         | |
| `TOKEN`         | `v3.4.0.0`+ |
| `TOKENS`        | `v3.4.0.0`+ |
| `TOPIC`         | |
| `TOPICS`        | |
| `TOTPSEED`      | |
| `TOTP_SECRET`   | `v3.4.0.0`+ |
| `TO_BASE64`     | `v3.4.0.3`+ |
| `TRAILING`      | |
| `TRANSACTION`   | |
| `TRANSACTIONS`  | |
| `TRIGGER`       | `v3.0.0.0` - `v3.3.7.0` |
| `TRIM`          | |
| `TROWS`         | |
| `TRUE_FOR`      | `v3.3.6.0`+ |
| `TSDB_PAGESIZE` | |
| `TSERIES`       | |
| `TSMA`          | |
| `TSMAS`         | |
| `TTL`           | |
| `TYPE`          | `v3.4.2.0`+ |

### U

| **关键字**   | **说明** |
| ----------- | --- |
| `UNBOUNDED` | `v3.4.2.0`+ |
| `UNION`     | |
| `UNLIMITED` | |
| `UNLOCK`    | |
| `UNSAFE`    | |
| `UNSIGNED`  | |
| `UNTREATED` | |
| `UPDATE`    | |
| `USE`       | |
| `USER`      | |
| `USERS`     | |
| `USING`     | |

### V

| **关键字**        | **说明** |
| ---------------- | --- |
| `VALIDATE`       | `v3.3.7.0`+ |
| `VALUE`          | |
| `VALUES`         | |
| `VALUE_F`        | |
| `VARBINARY`      | |
| `VARCHAR`        | |
| `VARIABLE`       | |
| `VARIABLES`      | |
| `VERBOSE`        | |
| `VGROUP`         | |
| `VGROUPS`        | |
| `VIEW`           | |
| `VIEWS`          | |
| `VIRTUAL`        | |
| `VNODE`          | |
| `VNODES`         | |
| `VNODE_PER_CALL` | `v3.4.0.0`+ |
| `VTABLE`         | |
| `VTABLES`        | |

### W

| **关键字**              | **说明** |
| ---------------------- | --- |
| `WAL`                  | |
| `WAL_FSYNC_PERIOD`     | |
| `WAL_LEVEL`            | |
| `WAL_RETENTION_PERIOD` | |
| `WAL_RETENTION_SIZE`   | |
| `WAL_ROLL_PERIOD`      | |
| `WAL_SEGMENT_SIZE`     | |
| `WATERMARK`            | |
| `WDURATION`            | |
| `WEND`                 | |
| `WHEN`                 | |
| `WHERE`                | |
| `WINDOW`               | |
| `WINDOW_CLOSE`         | |
| `WINDOW_OFFSET`        | |
| `WINDOW_OPEN`          | `v3.3.7.0`+ |
| `WIN_OPTIMIZE_BATCH`   | |
| `WIN_OPTIMIZE_SINGLE`  | |
| `WITH`                 | |
| `WRITE`                | |
| `WSTART`               | |

### X

| **关键字** | **说明** |
| -------- | --- |
| `XNODE`  | `v3.3.7.0`+ |
| `XNODES` | `v3.3.7.0`+ |

### Z

| **关键字**      | **说明** |
| -------------- | --- |
| `ZEROTH_STATE` | `v3.4.2.0`+ |

### `_`

| **关键字**          | **说明** |
| ------------------ | --- |
| `_ANOMALYMARK`     | `v3.3.4.3`+ |
| `_C0`              | |
| `_FHIGH`           | `v3.3.4.3`+ |
| `_FLOW`            | `v3.3.4.3`+ |
| `_FROWTS`          | `v3.3.4.3`+ |
| `_IMPMARK`         | |
| `_IMPROWTS`        | |
| `_IROWTS`          | |
| `_IROWTS_ORIGIN`   | `v3.3.5.0`+ |
| `_ISFILLED`        | |
| `_QDURATION`       | |
| `_QEND`            | |
| `_QSTART`          | |
| `_ROWTS`           | |
| `_TAGS`            | |
| `_TCURRENT_TS`     | |
| `_TGRPID`          | |
| `_TIDLEEND`        | |
| `_TIDLESTART`      | |
| `_TLOCALTIME`      | |
| `_TNEXT_LOCALTIME` | |
| `_TNEXT_TS`        | |
| `_TPREV_LOCALTIME` | |
| `_TPREV_TS`        | |
| `_TROLLUP_TBCOUNT` | `v3.4.2.0`+ |
| `_TWDURATION`      | |
| `_TWEND`           | |
| `_TWROWNUM`        | |
| `_TWSTART`         | |
| `_WDURATION`       | |
| `_WEND`            | |
| `_WSTART`          | |
