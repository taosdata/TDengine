# Change Log

## Transaction support: BEGIN / COMMIT / ROLLBACK

### Problem

After `BEGIN`, the system was either missing support entirely or (in earlier prototype iterations) was blocking operations like `USE db`, `SHOW d0.stables`, and `SELECT *` — which should be permitted inside a transaction according to industry-standard behavior.

### Changes

**`include/util/taoserror.h`**
- Added `TSDB_CODE_TXN_ONLY_TABLE_DDL_ALLOWED` (`0x8000331A`): error returned when a non-allowed statement is issued inside a `BEGIN` transaction.

**`source/util/src/terror.c`**
- Added the error message string for `TSDB_CODE_TXN_ONLY_TABLE_DDL_ALLOWED`.

**`source/client/inc/clientInt.h`**
- Added `bool inTransaction` field to `STscObj` to track per-connection transaction state.

**`source/client/src/clientImpl.c`**
- Added `handleTransactionControlCmd()`: intercepts `BEGIN`, `COMMIT`, and `ROLLBACK` SQL strings before parsing, sets/clears `pTscObj->inTransaction`, and completes the request with success.
- Modified `taosAsyncQueryImpl()` and `taosAsyncQueryImplWithReqid()` to call `handleTransactionControlCmd()` after building the request object.

**`source/client/src/clientMain.c`**
- Added `nodeIsShowStmt()`: identifies all `SHOW` statement node types (read-only, allowed in transactions).
- Added `checkStmtAllowedInTransaction()`: validates that a parsed statement is permitted inside an active transaction.  Allowed: table DDL (`CREATE/DROP/ALTER TABLE`), `SELECT`, `SHOW*`, `EXPLAIN`, `DESCRIBE`, `USE DATABASE`, `ALTER LOCAL`, `RESET QUERY CACHE`.  Blocked: database DDL, user/node management, DML (`INSERT`, `DELETE`), etc.
- Modified `doAsyncQuery()` to call `checkStmtAllowedInTransaction()` when `pTscObj->inTransaction` is `true`, returning `TSDB_CODE_TXN_ONLY_TABLE_DDL_ALLOWED` if the statement is not permitted.

### Behavior

```
taos> begin;
Query OK, 0 row(s) affected

taos> create table d0.t1 (ts timestamp, c0 int);   -- allowed (table DDL)
Create OK, 0 row(s) affected

taos> use d0;                                        -- allowed (context setting)
Database changed.

taos> show d0.stables;                               -- allowed (read-only)
...

taos> select * from d0.t1;                           -- allowed (read query)
...

taos> create database db2;                           -- blocked
DB error: Only table DDL (CREATE/DROP/ALTER TABLE) allowed in transaction [0x8000331A]

taos> insert into d0.t1 values(now, 1);              -- blocked
DB error: Only table DDL (CREATE/DROP/ALTER TABLE) allowed in transaction [0x8000331A]

taos> commit;
Query OK, 0 row(s) affected
```
