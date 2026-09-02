/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

/*
 * streamReaderExt.c  --  External-source stream reader (ETR) implementation.
 *
 * Handles all STRIGGER_PULL_*_EXT subtypes delivered via
 * TDMT_STREAM_TRIGGER_PULL_EXT.  See DS §6.1.5, §6.2.7, §6.4.5.
 *
 * Layer overview:
 *   1. Lifecycle  : streamReaderExtOpen / streamReaderExtClose
 *   2. Dispatcher : streamReaderExtHandlePull  (switch on pullType)
 *   3. Handlers   : handleLastTsPull / handleMetaPull / handleDataPull /
 *                   handleMetaDataPull / handleCalcDataPull
 *
 * InfluxDB special-case: the external source does not support arbitrarily
 * long IN-lists, so META / DATA / CALC_DATA pulls use an N=64 uid OR-group
 * loop.  See buildInfluxWhereClauseForUidBatch().
 */

#define _DEFAULT_SOURCE
#include "streamReaderExt.h"

#include <stdarg.h>
#include "executor.h"
#include "extConnector.h"
#include "osString.h"     // taosUcs4ToMbs / TdUcs4 (extColCellToStr NCHAR conversion)
#include "plannodes.h"    // SExtColTypeMapping full definition
#include "querynodes.h"   // SColumnNode / SExprNode
#include "scalar.h"       // scalarCalculate for vectorized partition expressions
#include "taoserror.h"
#include "tdatablock.h"
#include "thash.h"
#include "tlog.h"
#include "ttypes.h"       // Typed partition-expression values
#include "tmsg.h"
#include "tsimplehash.h"
#include "taos.h"
#include "tutil.h"

/* STREAM_RETURN_ROWS_NUM is defined in streamMsg.h (shared with trigger side).
 * The #ifndef guard below is kept only as a safety net for out-of-tree builds. */
#ifndef STREAM_RETURN_ROWS_NUM
#define STREAM_RETURN_ROWS_NUM 4096
#endif

#include "extConnectorInt.h"

/* Maximum number of uids per InfluxDB OR-group (DS §6.1.5). */
#define EXT_INFLUX_UID_BATCH_SIZE  64

/* SQL buffer sizes by statement shape.  Only batched UNION ALL statements
 * need the largest buffer; fixed-shape and single-uid queries have smaller
 * bounded inputs. */
#define EXT_SCHEMA_SQL_BUF_LEN  512
#define EXT_SIMPLE_SQL_BUF_LEN  (1024 * 2)
#define EXT_CALC_SQL_BUF_LEN    4096
#define EXT_DATA_SQL_BUF_LEN    (4096 * 2)
#define EXT_BATCH_SQL_BUF_LEN   (4096 * 4)

/* Max length for the tagset key string used as pTagsetIndex key
 * ("col1=val1|col2=val2|..."). */
#define EXT_TAGSET_KEY_MAX 1024

/* Scratch buffer size for one generated SQL clause fragment (a WHERE
 * prefilter condition or a SELECT column list) before it is spliced into a
 * full statement in one of the statement-shape buffers above. */
#define EXT_SQL_CLAUSE_BUF_LEN 1024

/* A tagset key can expand when separators and escaped quotes are rendered as
 * SQL.  Four times the key limit covers that expansion without using the full
 * batch-statement buffer. */
#define EXT_TAG_WHERE_BUF_LEN  (EXT_TAGSET_KEY_MAX * 4)

/* Bounds-safe SQL fragment append.
 *
 * Appends the formatted text at *pOff into buf[bufLen] and advances *pOff.
 * Returns true when the whole fragment fit; false when it did not (or *pOff
 * was already at the end).  On a short write, buf stays NUL-terminated within
 * bounds and *pOff is set to bufLen so every subsequent call is a safe no-op.
 *
 * This replaces the unchecked `off += snprintf(buf + off, bufLen - off, ...)`
 * idiom, where snprintf's return value (the would-be length) can push off past
 * bufLen, making the next `bufLen - off` negative — which, as a size_t, becomes
 * a huge value and turns the following snprintf into an out-of-bounds write. */
static bool extSqlCat(char *buf, int32_t bufLen, int32_t *pOff, const char *fmt, ...) {
  if (buf == NULL || bufLen <= 0 || *pOff < 0 || *pOff >= bufLen) return false;
  va_list ap;
  va_start(ap, fmt);
  int32_t n = vsnprintf(buf + *pOff, (size_t)(bufLen - *pOff), fmt, ap);
  va_end(ap);
  if (n < 0) {            /* encoding error */
    buf[*pOff] = '\0';
    return false;
  }
  if (n >= bufLen - *pOff) {  /* truncated: buf already NUL-terminated by vsnprintf */
    *pOff = bufLen;
    return false;
  }
  *pOff += n;
  return true;
}

/* metaBlock column indices (DS §6.2.4). */
#define META_COL_GROUP_ID  0
#define META_COL_SKEY      1
#define META_COL_EKEY      2
#define META_COL_UID       3
#define META_COL_ROWS      4
#define META_COL_COUNT     5

/* ============================================================
 * Forward declarations for private helpers
 * ============================================================ */

static int32_t handleLastTsPull(SStreamExtReaderInfo *pInfo,
                                const SSTriggerExtPullReq *pReq,
                                SSTriggerExtPullRsp *pRsp);
static int32_t handleMetaPull(SStreamExtReaderInfo *pInfo,
                              const SSTriggerExtPullReq *pReq,
                              SSTriggerExtPullRsp *pRsp);
static int32_t handleDataPull(SStreamExtReaderInfo *pInfo,
                              const SSTriggerExtPullReq *pReq,
                              SSTriggerExtPullRsp *pRsp);
static int32_t handleMetaDataPull(SStreamExtReaderInfo *pInfo,
                                  const SSTriggerExtPullReq *pReq,
                                  SSTriggerExtPullRsp *pRsp);
static int32_t handleCalcDataPull(SStreamExtReaderInfo *pInfo,
                                  const SSTriggerExtPullReq *pReq,
                                  SSTriggerExtPullRsp *pRsp);
static int32_t handleGroupColValuePull(SStreamExtReaderInfo *pInfo,
                                       const SSTriggerExtPullReq *pReq,
                                       SSTriggerExtPullRsp *pRsp);
static int32_t extCopyOptionalString(const char *src, char **ppDst, const char *fieldName);
static int32_t extCopyFixedStringArray(SArray *pSrc, int32_t elemSize, SArray **ppDst, const char *fieldName);
static int32_t extCopyPointerStringArray(SArray *pSrc, SArray **ppDst, const char *fieldName);
static int32_t extParsePartitionExprNodes(SStreamExtReaderInfo *pInfo);
static int32_t extCopyColTypeMappings(const SExtColTypeMapping *pSrc, int32_t nMappings,
                                      SExtColTypeMapping **ppDst, int32_t *pCopiedCount,
                                      const char *fieldName);
static void extBuildSourceCfg(const SStreamExtTriggerSpec *pExtSpec, const char *plainPwd,
                              SExtSourceCfg *pCfg);
static int32_t extInitReaderHashes(SStreamExtReaderInfo *pInfo);
static void extDestroyUidBlocks(SArray *pUidBlocks);
static int32_t extFetchUidBlocks(SStreamExtReaderInfo *pInfo,
                                 const char *phase,
                                 const char *colList,
                                 int32_t nCols,
                                 const char *prefilterBuf,
                                 const SExtColTypeMapping *pMappings,
                                 int32_t nMappings,
                                 SSHashObj *pUidWindow,
                                 SArray *pUidBlocks);
static int32_t extBuildUidWindowFromMetaBlock(SSDataBlock *pMetaBlock,
                                              SSHashObj **ppUidWindow);

/* ============================================================
 * Utility: free a response struct (all owned sub-objects).
 * ============================================================ */
void streamExtPullRspFree(SSTriggerExtPullRsp *pRsp) {
  if (pRsp == NULL) return;
  taosArrayDestroy(pRsp->pLastTsArr);
  pRsp->pLastTsArr = NULL;
  blockDataDestroy(pRsp->pMetaBlock);
  pRsp->pMetaBlock = NULL;
  blockDataDestroy(pRsp->pDataBlock);
  pRsp->pDataBlock = NULL;
  if (pRsp->pIndexHash) {
    tSimpleHashCleanup(pRsp->pIndexHash);
    pRsp->pIndexHash = NULL;
  }
  taosArrayDestroyEx(pRsp->pGroupColVals, tDestroySStreamGroupValue);
  pRsp->pGroupColVals = NULL;
  taosMemoryFree(pRsp);
}

/* ============================================================
 * Utility: pGroupIndex cleanup callback — frees the SArray* value.
 * ============================================================ */
static void freeGroupIndexValue(void *pVal) {
  SArray **ppArr = (SArray **)pVal;
  if (ppArr && *ppArr) {
    taosArrayDestroy(*ppArr);
    *ppArr = NULL;
  }
}

static void freeUidIndexValue(void *pVal) {
  SUidIndexEntry *pEntry = (SUidIndexEntry *)pVal;
  if (pEntry == NULL) return;
  taosArrayDestroyEx(pEntry->partitionValues, tDestroySStreamGroupValue);
  pEntry->partitionValues = NULL;
}

/* ============================================================
 * Utility: allocate + zero a response struct.
 * ============================================================ */
static SSTriggerExtPullRsp *allocPullRsp(ESTriggerPullType pullType) {
  SSTriggerExtPullRsp *pRsp = taosMemoryCalloc(1, sizeof(SSTriggerExtPullRsp));
  if (pRsp) pRsp->pullType = pullType;
  return pRsp;
}

/* ============================================================
 * Utility: determine if the source uses relational DATETIME columns
 * (MySQL / PostgreSQL) or raw integer epoch timestamps (InfluxDB).
 * This controls how WHERE-clause time bounds are formatted in SQL.
 *
 * NOTE: all timestamps passed from the reader to the trigger are
 * normalised to nanoseconds (TSDB_TIME_PRECISION_NANO) regardless
 * of source type, so that the trigger side has a single, uniform
 * precision to convert from.
 * ============================================================ */
static bool extSrcIsRelational(int8_t sourceType) {
  return (EExtSourceType)sourceType != EXT_SOURCE_INFLUXDB;
}

/* ============================================================
 * Utility: convert epoch timestamp (in the given precision) to a quoted
 * DATETIME literal string suitable for MySQL/PostgreSQL WHERE clauses.
 *
 * MySQL DATETIME(3/6) columns store wall-clock time in the server/client
 * timezone, not UTC epoch integers.  Passing a raw int64 timestamp in a
 * WHERE clause causes MySQL to interpret it as a YYYYMMDDHHMMSS integer.
 *
 * This function normalises the epoch value to microseconds then formats it
 * as 'YYYY-MM-DD HH:MM:SS.uuuuuu' so MySQL can compare DATETIME(6) columns.
 *
 * Special case: INT64_MIN / negative values are mapped to
 * '1970-01-01 00:00:00.000000' so that the initial "no watermark" state
 * fetches all rows that are more recent than the Unix epoch.
 *
 * Output format: 'YYYY-MM-DD HH:MM:SS.uuuuuu'  (including surrounding quotes)
 * buf must be at least 32 bytes.
 * Returns the number of characters written (excluding NUL terminator).
 * ============================================================ */
static int32_t epochToDatetimeStr(int64_t epoch, int32_t precision, char *buf, int32_t bufLen) {
  /* Clamp INT64_MIN / negative to epoch origin. */
  if (epoch == INT64_MIN || epoch < 0) {
    return snprintf(buf, bufLen, "'1970-01-01 00:00:00.000000'");
  }
  /* Normalize to microseconds for sub-second formatting. */
  int64_t epoch_us;
  if (precision == TSDB_TIME_PRECISION_NANO) {
    epoch_us = epoch / 1000LL;
  } else if (precision == TSDB_TIME_PRECISION_MILLI) {
    epoch_us = epoch * 1000LL;
  } else {
    /* MICRO — already in µs */
    epoch_us = epoch;
  }
  time_t     sec     = (time_t)(epoch_us / 1000000LL);
  int        us      = (int)(epoch_us % 1000000LL);
  struct tm  tm_info = {0};
  /* Use TDengine's taosLocalTime to honour the taosd system timezone. */
  taosLocalTime(&sec, &tm_info, NULL, 0, NULL);
  return snprintf(buf, bufLen,
                  "'%04d-%02d-%02d %02d:%02d:%02d.%06d'",
                  tm_info.tm_year + 1900, tm_info.tm_mon + 1, tm_info.tm_mday,
                  tm_info.tm_hour, tm_info.tm_min, tm_info.tm_sec, us);
}

/* ============================================================
 * Utility: return the correct table qualifier for manual SQL building.
 * For PostgreSQL, the qualifier is the schema name (extSchema, e.g. "public").
 * For MySQL/InfluxDB, the qualifier is the database name (extDb).
 * ============================================================ */
static const char *extTableQualifier(const SStreamExtTriggerSpec *pSpec) {
  if ((EExtSourceType)pSpec->sourceType == EXT_SOURCE_POSTGRESQL &&
      pSpec->extSchema[0] != '\0') {
    return pSpec->extSchema;
  }
  return pSpec->extDb;
}

/* ============================================================
 * Utility: write the full table reference into buf as used in FROM clauses.
 *
 * InfluxDB 3.x (Arrow Flight SQL / DataFusion):
 *   The connection already carries the default database; DataFusion treats a
 *   two-part "db.table" as "schema.table", and then prepends the catalog name
 *   producing "public.db.table" which is not found.  Use the bare table name.
 *
 * MySQL / PostgreSQL:
 *   Use the conventional "qualifier.table" form.
 *
 * Returns the number of bytes written (snprintf-style, excl. NUL).
 * ============================================================ */
static int32_t buildExtTableRef(const SStreamExtTriggerSpec *pSpec,
                                char *buf, int32_t bufLen) {
  int32_t n = 0;
  if ((EExtSourceType)pSpec->sourceType == EXT_SOURCE_INFLUXDB) {
    n = snprintf(buf, bufLen, "%s", pSpec->extTable);
  } else {
    n = snprintf(buf, bufLen, "%s.%s",
                 extTableQualifier(pSpec), pSpec->extTable);
  }
  if (n < 0 || n >= bufLen) {
    stError("ext: buildExtTableRef overflow need:%d have:%d table:%s", n, bufLen, pSpec->extTable);
    if (bufLen > 0) buf[0] = '\0';
    return TSDB_CODE_OUT_OF_RANGE;
  }
  return n;
}

/* ============================================================
 * Utility: build the CALC reader prefilter clause fragment.
 * Derived from the calc SELECT's WHERE clause; used in extFetchDataBuildSql
 * (calc data pull) and handleCalcDataPull paths.
 * Writes "(<prefilter>) AND " into buf when non-empty.
 * ============================================================ */
static int32_t buildPrefilterClause(const SStreamExtTriggerSpec *pSpec,
                                    char *buf, int32_t bufLen) {
  if (pSpec->prefilter == NULL || pSpec->prefilter[0] == '\0') {
    buf[0] = '\0';
    return 0;
  }
  int32_t n = snprintf(buf, bufLen, "(%s) AND ", pSpec->prefilter);
  if (n < 0 || n >= bufLen) {
    stError("ext: buildPrefilterClause overflow need:%d have:%d", n, bufLen);
    buf[0] = '\0';
    return TSDB_CODE_OUT_OF_RANGE;
  }
  return n;
}

/* ============================================================
 * Utility: build the TRIGGER reader prefilter clause fragment.
 * Derived from the PRE_FILTER option in CREATE STREAM ... TRIGGER ... PRE_FILTER;
 * used in handleLastTsPull, handleMetaPullRelational, handleMetaPullInflux,
 * and the trigger-side path of fetchDataForUid.
 * Writes "(<triggerPrefilter>) AND " into buf when non-empty.
 * ============================================================ */
static int32_t buildTriggerPrefilterClause(const SStreamExtTriggerSpec *pSpec,
                                           char *buf, int32_t bufLen) {
  if (pSpec->triggerPrefilter == NULL || pSpec->triggerPrefilter[0] == '\0') {
    buf[0] = '\0';
    return 0;
  }
  int32_t n = snprintf(buf, bufLen, "(%s) AND ", pSpec->triggerPrefilter);
  if (n < 0 || n >= bufLen) {
    stError("ext: buildTriggerPrefilterClause overflow need:%d have:%d", n, bufLen);
    buf[0] = '\0';
    return TSDB_CODE_OUT_OF_RANGE;
  }
  return n;
}

/* ============================================================
 * Utility: collect all uids from pUidIndex into a flat SArray<int64_t>.
 * Caller must taosArrayDestroy the result.
 * ============================================================ */
static SArray *collectAllUids(SSHashObj *pUidIndex) {
  SArray *pArr = taosArrayInit(tSimpleHashGetSize(pUidIndex), sizeof(uint64_t));
  if (pArr == NULL) return NULL;

  int32_t iter = 0;
  void   *pVal = NULL;
  while ((pVal = tSimpleHashIterate(pUidIndex, pVal, &iter)) != NULL) {
    size_t  keyLen = 0;
    uint64_t uid    = *(uint64_t *)tSimpleHashGetKey(pVal, &keyLen);
    taosArrayPush(pArr, &uid);
  }
  return pArr;
}

/* ============================================================
 * Utility: collect uids from pUidWindow hash into a flat SArray<int64_t>.
 * ============================================================ */
static SArray *collectUidsFromWindow(SSHashObj *pUidWindow) {
  if (pUidWindow == NULL) return NULL;
  SArray *pArr = taosArrayInit(tSimpleHashGetSize(pUidWindow), sizeof(uint64_t));
  if (pArr == NULL) return NULL;

  int32_t iter = 0;
  void   *pVal = NULL;
  while ((pVal = tSimpleHashIterate(pUidWindow, pVal, &iter)) != NULL) {
    size_t  kLen = 0;
    uint64_t uid  = *(uint64_t *)tSimpleHashGetKey(pVal, &kLen);
    taosArrayPush(pArr, &uid);
  }
  return pArr;
}

/* ============================================================
 * SQL builder: MySQL/PG — uid list using "uid IN (u1, u2, ...)"
 * Writes SQL into buf[0..bufLen-1].  Returns snprintf-style length.
 * ============================================================ */
/* Convert a tagset key string "col1=val1|col2=val2|..." into an InfluxDB
 * WHERE fragment "col1='val1' AND col2='val2' AND ...".
 * Returns the number of bytes written (excl. NUL), or -1 on truncation.
 * If tagsetKey is empty (MySQL/PG single-group), writes nothing. */
static int32_t buildInfluxTagWhereClause(const char *tagsetKey, char *buf,
                                         int32_t bufLen) {
  if (tagsetKey == NULL || tagsetKey[0] == '\0') {
    if (bufLen > 0) buf[0] = '\0';
    return 0;
  }

  int32_t off   = 0;
  bool    first = true;

  /* Duplicate the key so we can tokenise in-place. */
  char tmp[EXT_TAGSET_KEY_MAX] = {0};
  tstrncpy(tmp, tagsetKey, sizeof(tmp));

  char *saveptr = NULL;
  char *pair    = strtok_r(tmp, "|", &saveptr);
  while (pair != NULL) {
    /* Split "col=val" at the first '='. */
    char *eq = strchr(pair, '=');
    if (eq == NULL) {
      pair = strtok_r(NULL, "|", &saveptr);
      continue;
    }
    *eq = '\0';
    const char *col = pair;
    const char *val = eq + 1;

    /* Escape single quotes in the tag value so the SQL string literal stays
     * well-formed (and a value containing ' cannot break out of / inject into
     * the WHERE clause).  SQL standard: a single quote inside a literal is
     * doubled.  Tag values are NCHAR(EXT_INFLUX_TAG_NCHAR_CHARS), so the
     * doubled form fits escVal. */
    char    escVal[2 * EXT_INFLUX_TAG_NCHAR_CHARS + 1] = {0};
    int32_t ei                  = 0;
    for (const char *p = val; *p != '\0' && ei < (int32_t)sizeof(escVal) - 2; ++p) {
      if (*p == '\'') {
        escVal[ei++] = '\'';
        escVal[ei++] = '\'';
      } else {
        escVal[ei++] = *p;
      }
    }
    escVal[ei] = '\0';

    int32_t n = snprintf(buf + off, bufLen - off,
                         "%s%s='%s'", first ? "" : " AND ", col, escVal);
    if (n < 0 || off + n >= bufLen) return -1;
    off  += n;
    first = false;
    pair  = strtok_r(NULL, "|", &saveptr);
  }

  return off;
}

/* ============================================================
 * extQueryExecFetchOne — Execute SQL, fetch the first result block, close.
 *
 * pConn      : connector handle
 * sql        : SQL string to execute
 * pMappings  : column type mappings (may be NULL if caller does not need
 *              typed columns — in that case pDataBlock will be empty)
 * nMappings  : length of pMappings array (0 when pMappings is NULL)
 * ppOut      : receives the first SSDataBlock (caller must blockDataDestroy)
 *
 * Returns TSDB_CODE_SUCCESS; *ppOut is NULL when no rows were returned.
 * ============================================================ */
static int32_t extQueryExecFetchOne(SExtConnectorHandle  *pConn,
                                    const char           *sql,
                                    SExtColTypeMapping   *pMappings,
                                    int32_t               nMappings,
                                    SSDataBlock         **ppOut) {
  int32_t            code   = TSDB_CODE_SUCCESS;
  SExtQueryHandle   *pQHdl  = NULL;
  SExtConnectorError connErr = {0};

  stDebug("ext: extQueryExecFetchOne sql=\"%.120s\" nMappings=%d", sql, nMappings);

  code = extConnectorExecQuery(pConn, NULL, sql, &pQHdl, &connErr);
  if (code != TSDB_CODE_SUCCESS) {
    stError("ext: extQueryExecFetchOne exec failed code:%d msg:%s",
            code, connErr.remoteMessage);
    return code;
  }

  *ppOut = NULL;
  code = extConnectorFetchBlock(pQHdl, pMappings, nMappings, ppOut, &connErr);
  if (code != TSDB_CODE_SUCCESS) {
    stError("ext: extQueryExecFetchOne fetch failed code:%d msg:%s",
            code, connErr.remoteMessage);
  }
  extConnectorCloseQuery(pQHdl);
  stDebug("ext: extQueryExecFetchOne done rows=%" PRId64,
          *ppOut ? (int64_t)(*ppOut)->info.rows : 0);
  return code;
}

/* ============================================================
 * extQueryForEachBlock callback type.
 * Called once per fetched SSDataBlock (owned by callee — destroy when done).
 * Return TSDB_CODE_SUCCESS to continue fetching; any other code stops the loop
 * and is propagated as the return value of extQueryExecForEach.
 * ============================================================ */
typedef int32_t (*FExtBlockCallback)(SSDataBlock *pBlock, void *pCtx);

/* ============================================================
 * extQueryExecForEach — Execute SQL, call cb for every result block, close.
 *
 * Ownership: each pBlock passed to cb belongs to cb; cb must destroy it.
 * ============================================================ */
static int32_t extQueryExecForEach(SExtConnectorHandle  *pConn,
                                   const char           *sql,
                                   SExtColTypeMapping   *pMappings,
                                   int32_t               nMappings,
                                   FExtBlockCallback     cb,
                                   void                 *pCtx) {
  int32_t            code   = TSDB_CODE_SUCCESS;
  SExtQueryHandle   *pQHdl  = NULL;
  SExtConnectorError connErr = {0};

  stDebug("ext: extQueryExecForEach sql=\"%.120s\" nMappings=%d", sql, nMappings);

  code = extConnectorExecQuery(pConn, NULL, sql, &pQHdl, &connErr);
  if (code != TSDB_CODE_SUCCESS) {
    stError("ext: extQueryExecForEach exec failed code:%d msg:%s",
            code, connErr.remoteMessage);
    return code;
  }

  while (true) {
    SSDataBlock *pBlk = NULL;
    code = extConnectorFetchBlock(pQHdl, pMappings, nMappings, &pBlk, &connErr);
    if (code != TSDB_CODE_SUCCESS) {
      stError("ext: extQueryExecForEach fetch failed code:%d msg:%s",
              code, connErr.remoteMessage);
      break;
    }
    if (pBlk == NULL) break;  /* end of results */

    code = cb(pBlk, pCtx);
    if (code != TSDB_CODE_SUCCESS) break;
  }

  extConnectorCloseQuery(pQHdl);
  stDebug("ext: extQueryExecForEach done code=%d", code);
  return code;
}

/* ============================================================
 * extQueryExecFetchAll — Execute SQL, loop-fetch ALL result batches,
 * merge into a single SSDataBlock, then close.
 *
 * Unlike extQueryExecFetchOne (one batch only), this drains the entire
 * result set.  Arrow Flight / InfluxDB may split results across multiple
 * RecordBatches; using FetchOne would silently drop trailing batches.
 * ============================================================ */
static int32_t extQueryExecFetchAll(SExtConnectorHandle  *pConn,
                                    const char           *sql,
                                    SExtColTypeMapping   *pMappings,
                                    int32_t               nMappings,
                                    SSDataBlock         **ppOut) {
  int32_t            code   = TSDB_CODE_SUCCESS;
  SExtQueryHandle   *pQHdl  = NULL;
  SExtConnectorError connErr = {0};

  stDebug("ext: extQueryExecFetchAll sql=\"%.120s\" nMappings=%d", sql, nMappings);

  code = extConnectorExecQuery(pConn, NULL, sql, &pQHdl, &connErr);
  if (code != TSDB_CODE_SUCCESS) {
    stError("ext: extQueryExecFetchAll exec failed code:%d msg:%s",
            code, connErr.remoteMessage);
    return code;
  }

  *ppOut = NULL;
  while (true) {
    SSDataBlock *pBlk = NULL;
    code = extConnectorFetchBlock(pQHdl, pMappings, nMappings, &pBlk, &connErr);
    if (code != TSDB_CODE_SUCCESS) {
      stError("ext: extQueryExecFetchAll fetch failed code:%d msg:%s",
              code, connErr.remoteMessage);
      break;
    }
    if (pBlk == NULL) break;  // end of results

    if (*ppOut == NULL) {
      *ppOut = pBlk;  // first batch — take ownership directly
    } else {
      code = blockDataMerge(*ppOut, pBlk);
      blockDataDestroy(pBlk);
      if (code != TSDB_CODE_SUCCESS) {
        stError("ext: extQueryExecFetchAll merge failed code:%d", code);
        break;
      }
    }
  }

  extConnectorCloseQuery(pQHdl);
  stDebug("ext: extQueryExecFetchAll done rows=%" PRId64,
          *ppOut ? (int64_t)(*ppOut)->info.rows : 0);
  return code;
}

static int32_t extCopyOptionalString(const char *src, char **ppDst, const char *fieldName) {
  if (src == NULL || src[0] == '\0') {
    *ppDst = NULL;
    return TSDB_CODE_SUCCESS;
  }

  *ppDst = tstrdup(src);
  if (*ppDst == NULL) {
    stError("ext: OOM copying %s", fieldName);
    return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t extCopyFixedStringArray(SArray *pSrc, int32_t elemSize, SArray **ppDst,
                                       const char *fieldName) {
  *ppDst = NULL;
  if (pSrc == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t nItems = (int32_t)taosArrayGetSize(pSrc);
  SArray *pDst = taosArrayInit(nItems, elemSize);
  if (pDst == NULL) {
    stError("ext: OOM copying %s", fieldName);
    return terrno;
  }

  for (int32_t i = 0; i < nItems; ++i) {
    const void *pItem = taosArrayGet(pSrc, i);
    if (taosArrayPush(pDst, pItem) == NULL) {
      taosArrayDestroy(pDst);
      stError("ext: OOM pushing %s[%d]", fieldName, i);
      return terrno;
    }
  }

  *ppDst = pDst;
  return TSDB_CODE_SUCCESS;
}

static int32_t extCopyPointerStringArray(SArray *pSrc, SArray **ppDst, const char *fieldName) {
  *ppDst = NULL;
  if (pSrc == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t nItems = (int32_t)taosArrayGetSize(pSrc);
  SArray *pDst = taosArrayInit(nItems, POINTER_BYTES);
  if (pDst == NULL) {
    stError("ext: OOM copying %s", fieldName);
    return terrno;
  }

  for (int32_t i = 0; i < nItems; ++i) {
    const char *pItem = (const char *)taosArrayGetP(pSrc, i);
    char       *pDup = taosStrdup(pItem != NULL ? pItem : "");
    if (pDup == NULL) {
      taosArrayDestroyP(pDst, taosMemFree);
      stError("ext: OOM copying %s[%d]", fieldName, i);
      return terrno;
    }
    if (taosArrayPush(pDst, &pDup) == NULL) {
      taosMemoryFree(pDup);
      taosArrayDestroyP(pDst, taosMemFree);
      stError("ext: OOM pushing %s[%d]", fieldName, i);
      return terrno;
    }
  }

  *ppDst = pDst;
  return TSDB_CODE_SUCCESS;
}

static int32_t extParsePartitionExprNodes(SStreamExtReaderInfo *pInfo) {
  const SStreamTask *pTask = pInfo->pTask;
  pInfo->pPartitionColExprNodes = NULL;
  if (pInfo->spec.partitionTagExprs == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t nExpr = (int32_t)taosArrayGetSize(pInfo->spec.partitionTagExprs);
  pInfo->pPartitionColExprNodes = taosArrayInit(nExpr, POINTER_BYTES);
  if (pInfo->pPartitionColExprNodes == NULL) {
    ST_TASK_ELOG("%s", "ext: OOM allocating pPartitionColExprNodes");
    return terrno;
  }

  for (int32_t i = 0; i < nExpr; ++i) {
    const char *exprStr = (const char *)taosArrayGetP(pInfo->spec.partitionTagExprs, i);
    SNode      *pNode = NULL;

    if (exprStr != NULL && exprStr[0] != '\0') {
      int32_t code = nodesStringToNode(exprStr, &pNode);
      if (code != TSDB_CODE_SUCCESS) {
        ST_TASK_ELOG("ext: failed to parse partitionTagExprs[%d]=\"%s\" code:%d", i, exprStr, code);
        return code;
      }
    }

    if (taosArrayPush(pInfo->pPartitionColExprNodes, &pNode) == NULL) {
      nodesDestroyNode(pNode);
      ST_TASK_ELOG("ext: OOM pushing pPartitionColExprNodes[%d]", i);
      return terrno;
    }
  }

  ST_TASK_DLOG("ext: parsed %d partitionTagExprs templates", nExpr);
  return TSDB_CODE_SUCCESS;
}

static int32_t extCopyColTypeMappings(const SExtColTypeMapping *pSrc, int32_t nMappings,
                                      SExtColTypeMapping **ppDst, int32_t *pCopiedCount,
                                      const char *fieldName) {
  *ppDst = NULL;
  *pCopiedCount = 0;
  if (pSrc == NULL || nMappings <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SExtColTypeMapping *pDst = taosMemoryMalloc((size_t)nMappings * sizeof(SExtColTypeMapping));
  if (pDst == NULL) {
    stError("ext: OOM copying %s", fieldName);
    return terrno;
  }

  memcpy(pDst, pSrc, (size_t)nMappings * sizeof(SExtColTypeMapping));
  *ppDst = pDst;
  *pCopiedCount = nMappings;
  return TSDB_CODE_SUCCESS;
}

static void extBuildSourceCfg(const SStreamExtTriggerSpec *pExtSpec, const char *plainPwd,
                              SExtSourceCfg *pCfg) {
  memset(pCfg, 0, sizeof(*pCfg));
  pCfg->source_type = (EExtSourceType)pExtSpec->sourceType;
  tstrncpy(pCfg->source_name, pExtSpec->sourceName, sizeof(pCfg->source_name));
  tstrncpy(pCfg->host, pExtSpec->host, sizeof(pCfg->host));
  pCfg->port = (int32_t)pExtSpec->port;
  tstrncpy(pCfg->user, pExtSpec->user, sizeof(pCfg->user));
  tstrncpy(pCfg->password, plainPwd, sizeof(pCfg->password));
  tstrncpy(pCfg->default_database, pExtSpec->extDb, sizeof(pCfg->default_database));
  tstrncpy(pCfg->options, pExtSpec->options, sizeof(pCfg->options));
  pCfg->meta_version = (int64_t)pExtSpec->connCfgVersion;
}

static int32_t extInitReaderHashes(SStreamExtReaderInfo *pInfo) {
  const SStreamTask *pTask = pInfo->pTask;
  pInfo->pUidIndex = tSimpleHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (pInfo->pUidIndex == NULL) {
    ST_TASK_ELOG("%s", "ext: tSimpleHashInit(pUidIndex) failed");
    return terrno;
  }
  tSimpleHashSetFreeFp(pInfo->pUidIndex, freeUidIndexValue);

  pInfo->pGroupIndex = tSimpleHashInit(16, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (pInfo->pGroupIndex == NULL) {
    ST_TASK_ELOG("%s", "ext: tSimpleHashInit(pGroupIndex) failed");
    return terrno;
  }
  tSimpleHashSetFreeFp(pInfo->pGroupIndex, freeGroupIndexValue);

  pInfo->pTagsetIndex = tSimpleHashInit(64, MurmurHash3_32);
  if (pInfo->pTagsetIndex == NULL) {
    ST_TASK_ELOG("%s", "ext: tSimpleHashInit(pTagsetIndex) failed");
    return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t extCopyReaderSpec(SStreamExtReaderInfo *pInfo, const SStreamExtTriggerSpec *pExtSpec) {
  int32_t code = TSDB_CODE_SUCCESS;

  pInfo->spec = *pExtSpec;
  pInfo->spec.prefilter = NULL;
  pInfo->spec.triggerPrefilter = NULL;
  pInfo->spec.triggerColumns = NULL;
  pInfo->spec.partitionTagCols = NULL;
  pInfo->spec.partitionTagExprs = NULL;
  pInfo->spec.pColMappings = NULL;
  pInfo->spec.numColMappings = 0;
  pInfo->spec.calcColumns = NULL;
  pInfo->spec.pCalcMappings = NULL;
  pInfo->spec.numCalcMappings = 0;

  code = extCopyOptionalString(pExtSpec->prefilter, &pInfo->spec.prefilter, "prefilter");
  if (code != TSDB_CODE_SUCCESS) return code;

  code = extCopyOptionalString(pExtSpec->triggerPrefilter, &pInfo->spec.triggerPrefilter, "triggerPrefilter");
  if (code != TSDB_CODE_SUCCESS) return code;

  code = extCopyFixedStringArray(pExtSpec->triggerColumns, TSDB_COL_NAME_LEN,
                                 &pInfo->spec.triggerColumns, "triggerColumns");
  if (code != TSDB_CODE_SUCCESS) return code;

  code = extCopyFixedStringArray(pExtSpec->partitionTagCols, TSDB_COL_NAME_LEN,
                                 &pInfo->spec.partitionTagCols, "partitionTagCols");
  if (code != TSDB_CODE_SUCCESS) return code;

  code = extCopyPointerStringArray(pExtSpec->partitionTagExprs, &pInfo->spec.partitionTagExprs,
                                   "partitionTagExprs");
  if (code != TSDB_CODE_SUCCESS) return code;

  code = extParsePartitionExprNodes(pInfo);
  if (code != TSDB_CODE_SUCCESS) return code;

  code = extCopyColTypeMappings(pExtSpec->pColMappings, pExtSpec->numColMappings,
                                &pInfo->spec.pColMappings, &pInfo->spec.numColMappings,
                                "pColMappings");
  if (code != TSDB_CODE_SUCCESS) return code;

  code = extCopyFixedStringArray(pExtSpec->calcColumns, TSDB_COL_NAME_LEN,
                                 &pInfo->spec.calcColumns, "calcColumns");
  if (code != TSDB_CODE_SUCCESS) return code;

  return extCopyColTypeMappings(pExtSpec->pCalcMappings, pExtSpec->numCalcMappings,
                                &pInfo->spec.pCalcMappings, &pInfo->spec.numCalcMappings,
                                "pCalcMappings");
}

static int32_t extOpenReaderConnector(SStreamExtReaderInfo *pInfo, const SStreamExtTriggerSpec *pExtSpec) {
  const SStreamTask *pTask = pInfo->pTask;
  char          plainPwd[TSDB_EXT_SOURCE_PASSWORD_LEN] = {0};
  SExtSourceCfg cfg = {0};
  int32_t       code = TSDB_CODE_SUCCESS;

  decryptExtSourcePassword((const char *)pExtSpec->encryptedPassword, plainPwd);
  extBuildSourceCfg(pExtSpec, plainPwd, &cfg);
  memset(plainPwd, 0, sizeof(plainPwd));

  code = extConnectorOpen(&cfg, &pInfo->pConn);
  memset(cfg.password, 0, sizeof(cfg.password));
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: extConnectorOpen failed code:%d source=%s host=%s port=%d",
                 code, pExtSpec->sourceName, pExtSpec->host, (int)pExtSpec->port);
    return code;
  }

  ST_TASK_DLOG("ext: extConnectorOpen ok, source=%s", pExtSpec->sourceName);
  return TSDB_CODE_SUCCESS;
}

/* ============================================================
 * Lifecycle: open
 * ============================================================ */
int32_t streamReaderExtOpen(void *pSpec, const SStreamTask *pTask,
                            SStreamExtReaderInfo **ppReaderInfo) {
  int32_t code = TSDB_CODE_SUCCESS;

  stDebug("ext: streamReaderExtOpen enter, pSpec=%p", pSpec);

  if (pSpec == NULL || ppReaderInfo == NULL) {
    stError("ext: streamReaderExtOpen invalid args");
    return TSDB_CODE_INVALID_PARA;
  }

  const SStreamExtTriggerSpec *pExtSpec = (const SStreamExtTriggerSpec *)pSpec;
  stDebug("ext: open extReader source=%s host=%s port=%d user=%s db=%s tbl=%s srcType=%d",
          pExtSpec->sourceName, pExtSpec->host, (int)pExtSpec->port,
          pExtSpec->user, pExtSpec->extDb, pExtSpec->extTable,
          (int)pExtSpec->sourceType);

  SStreamExtReaderInfo *pInfo = taosMemoryCalloc(1, sizeof(SStreamExtReaderInfo));
  if (pInfo == NULL) {
    stError("ext: OOM allocating SStreamExtReaderInfo");
    return terrno;
  }

  pInfo->pTask = pTask;
  code = extCopyReaderSpec(pInfo, pExtSpec);
  if (code != TSDB_CODE_SUCCESS) goto _err;

  code = extOpenReaderConnector(pInfo, pExtSpec);
  if (code != TSDB_CODE_SUCCESS) goto _err;
  code = extInitReaderHashes(pInfo);
  if (code != TSDB_CODE_SUCCESS) goto _err;

  pInfo->influxBatchOffset = 0;
  *ppReaderInfo = pInfo;
  ST_TASK_DLOG("ext: streamReaderExtOpen ok source=%s", pExtSpec->sourceName);
  return TSDB_CODE_SUCCESS;

_err:
  streamReaderExtClose(pInfo);
  *ppReaderInfo = NULL;
  ST_TASK_ELOG("ext: streamReaderExtOpen failed code:%d", code);
  return code;
}

/* ============================================================
 * Lifecycle: close
 * ============================================================ */
void streamReaderExtClose(SStreamExtReaderInfo *pInfo) {
  if (pInfo == NULL) return;

  const SStreamTask *pTask = pInfo->pTask;
  ST_TASK_DLOG("ext: streamReaderExtClose enter source=%s", pInfo->spec.sourceName);

  if (pInfo->pConn) {
    extConnectorClose(pInfo->pConn);
    pInfo->pConn = NULL;
  }

  if (pInfo->pUidIndex) {
    tSimpleHashCleanup(pInfo->pUidIndex);
    pInfo->pUidIndex = NULL;
  }
  if (pInfo->pGroupIndex) {
    /* freeGroupIndexValue callback frees each SArray* value. */
    tSimpleHashCleanup(pInfo->pGroupIndex);
    pInfo->pGroupIndex = NULL;
  }
  if (pInfo->pTagsetIndex) {
    tSimpleHashCleanup(pInfo->pTagsetIndex);
    pInfo->pTagsetIndex = NULL;
  }
  if (pInfo->pInfluxTagCols) {
    taosArrayDestroy(pInfo->pInfluxTagCols);
    pInfo->pInfluxTagCols = NULL;
  }

  /* Free parsed pPartitionColExprNodes cache. */
  if (pInfo->pPartitionColExprNodes) {
    int32_t n = (int32_t)taosArrayGetSize(pInfo->pPartitionColExprNodes);
    for (int32_t i = 0; i < n; i++) {
      SNode *pNode = (SNode *)taosArrayGetP(pInfo->pPartitionColExprNodes, i);
      nodesDestroyNode(pNode);
    }
    taosArrayDestroy(pInfo->pPartitionColExprNodes);
    pInfo->pPartitionColExprNodes = NULL;
  }

  tCleanupSStreamExtTriggerSpec(&pInfo->spec);

  /* Zero the whole struct to prevent use-after-free leaking sensitive data. */
  memset(pInfo, 0, sizeof(*pInfo));
  taosMemoryFree(pInfo);
  ST_TASK_DLOG("%s", "ext: streamReaderExtClose done");
}

/* ============================================================
 * void* bridge wrappers for streamReader.c
 *
 * streamReader.c cannot include streamReaderExt.h directly because
 * streamReaderExt.h redefines SStreamExtReaderInfo (a different, smaller
 * struct than the one in streamReader.h).  These thin wrappers expose the
 * same functionality via void* so streamReader.c can forward-declare them
 * without depending on the incompatible struct definition.
 * ============================================================ */

/* Called by stReaderTaskDeploy in streamReader.c for ext-source reader tasks.
 * Delegates to streamReaderExtOpen and returns the opaque info pointer
 * via *ppInfo (void*). */
int32_t stExtReaderOpen(void *pSpec, const SStreamTask *pTask, void **ppInfo) {
  SStreamExtReaderInfo *pReaderInfo = NULL;
  int32_t code = streamReaderExtOpen(pSpec, pTask, &pReaderInfo);
  *ppInfo = (void *)pReaderInfo;
  return code;
}

/* Called by stReaderTaskUndeployImpl in streamReader.c for ext-source reader
 * tasks.  Delegates to streamReaderExtClose. */
void stExtReaderClose(void *pInfo) {
  streamReaderExtClose((SStreamExtReaderInfo *)pInfo);
}

/* ============================================================
 * streamExtReaderInitTableList
 *
 * One-shot initialisation of the three lookup hashes (pUidIndex,
 * pGroupIndex, pTagsetIndex).  Analogous to vnodeProcessStreamReaderMsg's
 * initTableList for non-ext sources.  Called the first time any PULL
 * request arrives and pUidIndex is still empty.
 *
 * Semantics per source type:
 *
 *   MySQL / PostgreSQL
 *     The external table maps to exactly one TDengine "sub-table".
 *     uid = groupId = MurmurHash3_64(sourceName + "." + extTable) | 1
 *     (OR-1 ensures the value is never 0.)
 *     pTagsetIndex is left empty (no tag concept).
 *
 *   InfluxDB (always sub-table granularity, regardless of PARTITION BY)
 *     Queries information_schema.columns to discover tag column names, then
 *     SELECT DISTINCT <tag cols> to enumerate live tag-value combinations.
 *     Each unique combination is one sub-table:
 *       uid = MurmurHash3_64(full tagset "col=val|...") | 1
 *     groupId is derived from the PARTITION BY spec, so multiple uids may
 *     share one groupId (pGroupIndex[groupId] → SArray<uid>):
 *       - no PARTITION BY        → groupId = MurmurHash3_64(measurement) | 1
 *                                  (all sub-tables in one group)
 *       - PARTITION BY tbname     → groupId = uid (each sub-table its own group)
 *       - PARTITION BY <tags>     → groupId = MurmurHash3_64(partition-tag subset) | 1
 *     pTagsetIndex maps the full tagset-string → uid.  A measurement with no
 *     tag columns falls back to a single measurement-level uid/group.
 *
 * On success all three hashes contain consistent entries.
 * maxTs is initialised to INT64_MIN (no watermark yet).
 * ============================================================ */

/* Build a stable tagset key string from ordered tag column names and their
 * values.  Format: "col1=val1|col2=val2|..."
 * Returns the number of bytes written (not including NUL), or -1 on truncation. */
static int32_t buildTagsetKey(char *buf, int32_t bufLen,
                              const char **colNames, const char **colVals,
                              int32_t nCols) {
  int32_t off = 0;
  for (int32_t i = 0; i < nCols; i++) {
    int32_t n = snprintf(buf + off, bufLen - off, "%s=%s%s",
                         colNames[i], colVals[i] ? colVals[i] : "",
                         (i + 1 < nCols) ? "|" : "");
    if (n < 0 || off + n >= bufLen) return -1;
    off += n;
  }
  return off;
}

typedef struct SBindPartitionExprCtx {
  SArray *pTagCols;
  int32_t code;
} SBindPartitionExprCtx;

static EDealRes bindPartitionExprColumn(SNode *pNode, void *pContext) {
  if (nodeType(pNode) != QUERY_NODE_COLUMN) return DEAL_RES_CONTINUE;

  SBindPartitionExprCtx *pCtx = (SBindPartitionExprCtx *)pContext;
  SColumnNode *pCol = (SColumnNode *)pNode;
  int32_t nTags = (int32_t)taosArrayGetSize(pCtx->pTagCols);
  for (int32_t i = 0; i < nTags; ++i) {
    const char *pTagCol = (const char *)taosArrayGet(pCtx->pTagCols, i);
    if (pTagCol != NULL && strcasecmp(pTagCol, pCol->colName) == 0) {
      pCol->dataBlockId = 0;
      pCol->slotId = i;
      return DEAL_RES_CONTINUE;
    }
  }

  pCtx->code = TSDB_CODE_MND_STREAM_TBNAME_CALC_FAILED;
  return DEAL_RES_ERROR;
}

static int32_t bindPartitionExprNodes(SStreamExtReaderInfo *pInfo, SArray *pTagCols) {
  const SStreamTask *pTask = pInfo->pTask;
  if (pInfo->pPartitionColExprNodes == NULL) return TSDB_CODE_SUCCESS;

  int32_t nExprs = (int32_t)taosArrayGetSize(pInfo->pPartitionColExprNodes);
  int32_t bound = 0;
  for (int32_t i = 0; i < nExprs; ++i) {
    SNode *pExpr = (SNode *)taosArrayGetP(pInfo->pPartitionColExprNodes, i);
    if (pExpr == NULL) continue;
    SBindPartitionExprCtx ctx = {.pTagCols = pTagCols, .code = TSDB_CODE_SUCCESS};
    nodesWalkExpr(pExpr, bindPartitionExprColumn, &ctx);
    if (ctx.code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("ext: failed to bind partition expression slot:%d code:%d", i, ctx.code);
      return ctx.code;
    }
    ++bound;
  }
  ST_TASK_DLOG("ext: bound %d partition expression(s) for vectorized evaluation", bound);
  return TSDB_CODE_SUCCESS;
}

static int32_t extColumnRowToGroupValue(SColumnInfoData *pCol, int32_t row, SStreamGroupValue *pOut) {
  SStreamGroupValue value = {0};
  value.data.type = pCol->info.type;
  value.isNull = colDataIsNull_s(pCol, row) || value.data.type == TSDB_DATA_TYPE_NULL;
  if (value.isNull) {
    *pOut = value;
    return TSDB_CODE_SUCCESS;
  }

  void *pRaw = colDataGetData(pCol, row);
  if (pRaw == NULL) return TSDB_CODE_MND_STREAM_TBNAME_CALC_FAILED;

  if (IS_VAR_DATA_TYPE(value.data.type)) {
    bool    isBlob = IS_STR_DATA_BLOB(value.data.type);
    int32_t len = isBlob ? blobDataLen(pRaw) : varDataLen(pRaw);
    const void *pPayload = isBlob ? (const void *)blobDataVal(pRaw) : (const void *)varDataVal(pRaw);
    value.data.pData = taosMemoryMalloc(len > 0 ? len : 1);
    if (value.data.pData == NULL) return terrno;
    if (len > 0) memcpy(value.data.pData, pPayload, len);
    value.data.nData = len;
  } else if (value.data.type == TSDB_DATA_TYPE_DECIMAL) {
    int32_t len = pCol->info.bytes;
    value.data.pData = taosMemoryMalloc(len > 0 ? len : 1);
    if (value.data.pData == NULL) return terrno;
    if (len > 0) memcpy(value.data.pData, pRaw, len);
    value.data.nData = len;
  } else {
    valueSetDatum(&value.data, value.data.type, pRaw, tDataTypes[value.data.type].bytes);
  }

  *pOut = value;
  return TSDB_CODE_SUCCESS;
}

static int32_t extCloneGroupValue(const SStreamGroupValue *pSrc, SStreamGroupValue *pDst) {
  *pDst = *pSrc;
  if (pSrc->isNull || (!IS_VAR_DATA_TYPE(pSrc->data.type) && pSrc->data.type != TSDB_DATA_TYPE_DECIMAL)) {
    return TSDB_CODE_SUCCESS;
  }

  pDst->data.pData = taosMemoryMalloc(pSrc->data.nData > 0 ? pSrc->data.nData : 1);
  if (pDst->data.pData == NULL) return terrno;
  if (pSrc->data.nData > 0) memcpy(pDst->data.pData, pSrc->data.pData, pSrc->data.nData);
  return TSDB_CODE_SUCCESS;
}

static int32_t appendPartitionGroupKey(char *buf, int32_t bufLen, int32_t *pOffset,
                                       const SStreamGroupValue *pValue) {
  int32_t dataLen = 0;
  const void *pData = NULL;
  if (!pValue->isNull) {
    if (IS_VAR_DATA_TYPE(pValue->data.type) || pValue->data.type == TSDB_DATA_TYPE_DECIMAL) {
      dataLen = pValue->data.nData;
      pData = pValue->data.pData;
    } else {
      dataLen = tDataTypes[pValue->data.type].bytes;
      pData = &pValue->data.val;
    }
  }

  int32_t headerLen = sizeof(pValue->data.type) + sizeof(pValue->isNull) + sizeof(dataLen);
  if (*pOffset > bufLen - headerLen || dataLen > bufLen - *pOffset - headerLen) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  memcpy(buf + *pOffset, &pValue->data.type, sizeof(pValue->data.type));
  *pOffset += sizeof(pValue->data.type);
  memcpy(buf + *pOffset, &pValue->isNull, sizeof(pValue->isNull));
  *pOffset += sizeof(pValue->isNull);
  memcpy(buf + *pOffset, &dataLen, sizeof(dataLen));
  *pOffset += sizeof(dataLen);
  if (dataLen > 0) {
    memcpy(buf + *pOffset, pData, dataLen);
    *pOffset += dataLen;
  }
  return TSDB_CODE_SUCCESS;
}

static void extRollbackGroupIndexInsert(SStreamExtReaderInfo *pInfo, uint64_t groupId, uint64_t uid,
                                        SArray *pArr, bool groupCreated) {
  const SStreamTask *pTask = pInfo->pTask;
  if (pArr != NULL) {
    if (taosArrayPop(pArr) == NULL) {
      ST_TASK_ELOG("ext: initTableList pGroupIndex rollback pop failed gid=%" PRIu64 " uid=%" PRIu64,
                   groupId, uid);
    }
  }

  if (groupCreated) {
    int32_t rollbackCode = tSimpleHashRemove(pInfo->pGroupIndex, &groupId, sizeof(groupId));
    if (rollbackCode != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("ext: initTableList pGroupIndex rollback failed gid=%" PRIu64 " code:%d",
                   groupId, rollbackCode);
    }
  }
}

static int32_t extEnsureGroupUidArray(SStreamExtReaderInfo *pInfo, uint64_t groupId,
                                      SArray ***pppArr, bool *pGroupCreated) {
  const SStreamTask *pTask = pInfo->pTask;
  *pppArr = (SArray **)tSimpleHashGet(pInfo->pGroupIndex, &groupId, sizeof(groupId));
  *pGroupCreated = false;
  if (*pppArr != NULL) {
    return TSDB_CODE_SUCCESS;
  }

  SArray *pArr = taosArrayInit(4, sizeof(uint64_t));
  if (pArr == NULL) {
    ST_TASK_ELOG("ext: initTableList pGroupIndex OOM gid=%" PRIu64, groupId);
    return terrno;
  }

  int32_t code = tSimpleHashPut(pInfo->pGroupIndex, &groupId, sizeof(groupId), &pArr, POINTER_BYTES);
  if (code != TSDB_CODE_SUCCESS) {
    taosArrayDestroy(pArr);
    ST_TASK_ELOG("ext: initTableList pGroupIndex put failed gid=%" PRIu64, groupId);
    return code;
  }

  *pppArr = (SArray **)tSimpleHashGet(pInfo->pGroupIndex, &groupId, sizeof(groupId));
  if (*pppArr == NULL) {
    ST_TASK_ELOG("ext: initTableList pGroupIndex lookup failed gid=%" PRIu64, groupId);
    extRollbackGroupIndexInsert(pInfo, groupId, 0, NULL, true);
    return TSDB_CODE_INTERNAL_ERROR;
  }

  *pGroupCreated = true;
  return TSDB_CODE_SUCCESS;
}

static int32_t extPublishTagsetUid(SStreamExtReaderInfo *pInfo, uint64_t uid,
                                   const char *tagsetKey, int32_t tagsetKeyLen) {
  const SStreamTask *pTask = pInfo->pTask;
  if (tagsetKey == NULL || tagsetKeyLen <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = tSimpleHashPut(pInfo->pTagsetIndex, tagsetKey, tagsetKeyLen, &uid, sizeof(uid));
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: initTableList pTagsetIndex put failed uid=%" PRIu64, uid);
  }
  return code;
}

/* Insert one uid entry into all three hashes atomically.
 * uid == groupId for now (no separate groupId derivation needed). */
static int32_t extTableListInsertEntry(SStreamExtReaderInfo *pInfo,
                                       uint64_t uid, uint64_t groupId,
                                       const char *tagsetKey, int32_t tagsetKeyLen,
                                       SArray *pPartitionValues) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t code = TSDB_CODE_SUCCESS;
  bool    groupCreated = false;
  bool    tagsetInserted = false;
  SArray **ppArr = NULL;

  code = extEnsureGroupUidArray(pInfo, groupId, &ppArr, &groupCreated);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  if (taosArrayPush(*ppArr, &uid) == NULL) {
    code = terrno;
    ST_TASK_ELOG("ext: initTableList pGroupIndex array push OOM gid=%" PRIu64, groupId);
    goto _exit;
  }

  code = extPublishTagsetUid(pInfo, uid, tagsetKey, tagsetKeyLen);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }
  tagsetInserted = (tagsetKey != NULL && tagsetKeyLen > 0);

  SUidIndexEntry entry = {.groupId = groupId, .partitionValues = pPartitionValues};
  if (tagsetKey != NULL && tagsetKeyLen > 0) {
    tstrncpy(entry.tagsetKey, tagsetKey,
             TMIN((int32_t)sizeof(entry.tagsetKey), tagsetKeyLen + 1));
  }
  code = tSimpleHashPut(pInfo->pUidIndex, &uid, sizeof(uid), &entry, sizeof(entry));
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: initTableList pUidIndex put failed uid=%" PRIu64 " code:%d", uid, code);
    goto _exit;
  }

  ST_TASK_DLOG("ext: initTableList inserted uid=%" PRIu64 " gid=%" PRIu64
               " tagset=\"%.*s\"", uid, groupId, tagsetKeyLen, tagsetKey ? tagsetKey : "");
  return TSDB_CODE_SUCCESS;

_exit:
  if (code != TSDB_CODE_SUCCESS) {
    if (tagsetInserted) {
      int32_t rollbackCode = tSimpleHashRemove(pInfo->pTagsetIndex, tagsetKey, tagsetKeyLen);
      if (rollbackCode != TSDB_CODE_SUCCESS) {
        ST_TASK_ELOG("ext: initTableList pTagsetIndex rollback failed uid=%" PRIu64 " code:%d",
                     uid, rollbackCode);
      }
    }
    extRollbackGroupIndexInsert(pInfo, groupId, uid, ppArr != NULL ? *ppArr : NULL, groupCreated);
    taosArrayDestroyEx(pPartitionValues, tDestroySStreamGroupValue);
  }
  return code;
}

/* ============================================================
 * InfluxDB tag-partition callbacks used by extInitInfluxTagPartition.
 * ============================================================ */

/* Read one cell of a var-length column (VARCHAR/NCHAR) into a narrow,
 * NUL-terminated output buffer.
 *
 * colDataGetData() for var-length types returns the raw VARSTR pointer (the
 * 2-byte length header followed by the data), not a pointer past the header;
 * and for NCHAR the data is UCS-4 (4 bytes per character), not narrow bytes.
 * Reading it directly with tstrncpy (as this code previously did at all 4
 * call sites below) corrupts short strings into control-character garbage:
 * the VARSTR length prefix itself gets misread as the string's first bytes
 * (e.g. a 4-char tag name stored as 16 bytes of UCS-4 reads back as the
 * single control byte 0x10, since tstrncpy stops at the header's second,
 * zero, byte). That garbled name then gets embedded verbatim into the SQL
 * sent to the external source, which the remote parser rejects.
 *
 * Returns the number of narrow bytes written (excl. NUL), or a negative error
 * code on failure; outBuf is always NUL-terminated. */
static int32_t extColCellToStr(SColumnInfoData *pCol, int32_t row, char *outBuf, int32_t outBufLen) {
  if (outBufLen <= 0) {
    stError("ext: convert column cell to string failed, invalid output buffer length row:%d bufLen:%d", row,
            outBufLen);
    return -1;
  }
  outBuf[0]          = '\0';
  char       *pRaw   = colDataGetVarData(pCol, row);
  int32_t     rawLen = varDataLen(pRaw);
  const char *pVal    = varDataVal(pRaw);
  if (pCol->info.type == TSDB_DATA_TYPE_NCHAR) {
    /* taosUcs4ToMbs requires the output buffer to hold at least rawLen bytes
     * (the UCS-4 byte length), mirroring streamTriggerTask.c's usage. */
    if (rawLen > outBufLen) {
      stError("ext: convert NCHAR cell to string failed, output buffer too small row:%d type:%d rawLen:%d bufLen:%d",
              row, pCol->info.type, rawLen, outBufLen);
      return TSDB_CODE_STREAM_EXT_TAG_INVALID;
    }
    int32_t len = taosUcs4ToMbs((TdUcs4 *)pVal, rawLen, outBuf, NULL);
    if (len < 0 || len >= outBufLen) {
      outBuf[0] = '\0';
      stError("ext: convert NCHAR cell from UCS-4 failed row:%d type:%d rawLen:%d bufLen:%d resultLen:%d", row,
              pCol->info.type, rawLen, outBufLen, len);
      return TSDB_CODE_STREAM_EXT_TAG_INVALID;
    }
    outBuf[len] = '\0';
    return len;
  }
  int32_t len = TMIN(rawLen, outBufLen - 1);
  memcpy(outBuf, pVal, len);
  outBuf[len] = '\0';
  return len;
}

/* Collect tag column names from information_schema result blocks. */
static int32_t influxSchemaBlockCb(SSDataBlock *pBlock, void *pCtx) {
  SArray *pTagCols = (SArray *)pCtx;
  for (int32_t r = 0; r < pBlock->info.rows; r++) {
    SColumnInfoData *pCol = taosArrayGet(pBlock->pDataBlock, 0);
    if (pCol == NULL || colDataIsNull_s(pCol, r)) continue;
    char *nameBuf = (char *)taosArrayReserve(pTagCols, 1);
    if (nameBuf == NULL) {
      int32_t code = terrno;
      stError("ext: reserve InfluxDB tag column name failed at row:%d code:%d", r, code);
      blockDataDestroy(pBlock);
      return code;
    }
    int32_t code = extColCellToStr(pCol, r, nameBuf, EXT_INFLUX_KEY_NCHAR_CHARS);
    if (code < 0) {
      taosArrayPop(pTagCols);
      stError("ext: convert InfluxDB tag column name failed at row:%d, code:%d", r, code);
      blockDataDestroy(pBlock);
      return code;
    }
  }
  blockDataDestroy(pBlock);
  return TSDB_CODE_SUCCESS;
}

/* Context for influxDistinctBlockCb. */
typedef struct {
  SStreamExtReaderInfo *pInfo;
  SArray                   *pTagCols;
  int32_t                   nTags;
  int32_t                   nGroups;
  /* groupId derivation (DS §6.2.7):
   *   0 = single group  : no PARTITION BY; groupId = measurementGroupId.
   *   1 = all tags       : PARTITION BY tbname; groupId = uid (per sub-table).
   *   2 = subset          : PARTITION BY <tags>; groupId = hash(partition-tag subset). */
  int8_t                    groupMode;
  uint64_t                  measurementGroupId;
  int32_t                  *partIdx; /* direct-column tag index per partition slot; -1 for expressions */
  int32_t                   nPart;   /* number of PARTITION BY slots */
  /* Parsed complete expression per slot, or NULL for a bare tag column. */
  SNode                   **partExprNodes;
} SInfluxDistinctCtx;

static int32_t extBuildNCharGroupValue(const SStreamTask *pTask, const char *colName, const char *colVal,
                                       SStreamGroupValue *pOut);

static void extDestroyPartitionResultCols(SColumnInfoData **ppCols, int32_t nPart) {
  if (ppCols == NULL) return;
  for (int32_t p = 0; p < nPart; ++p) {
    if (ppCols[p] == NULL) continue;
    colDataDestroy(ppCols[p]);
    taosMemoryFree(ppCols[p]);
  }
  taosMemoryFree(ppCols);
}

static int32_t extEvaluatePartitionExprBlock(SInfluxDistinctCtx *pCtx, SSDataBlock *pBlock,
                                             SColumnInfoData ***ppResultCols) {
  const SStreamTask *pTask = pCtx->pInfo->pTask;
  *ppResultCols = NULL;
  if (pCtx->nPart == 0) return TSDB_CODE_SUCCESS;

  SColumnInfoData **ppCols = taosMemoryCalloc(pCtx->nPart, POINTER_BYTES);
  if (ppCols == NULL) return terrno;
  SArray *pBlockList = taosArrayInit(1, POINTER_BYTES);
  if (pBlockList == NULL || taosArrayPush(pBlockList, &pBlock) == NULL) {
    taosArrayDestroy(pBlockList);
    taosMemoryFree(ppCols);
    return terrno;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  int32_t evaluated = 0;
  for (int32_t p = 0; p < pCtx->nPart; ++p) {
    SNode *pExpr = pCtx->partExprNodes[p];
    if (pExpr == NULL) continue;

    ppCols[p] = taosMemoryCalloc(1, sizeof(SColumnInfoData));
    if (ppCols[p] == NULL) {
      code = terrno;
      break;
    }
    SDataType *pType = &((SExprNode *)pExpr)->resType;
    ppCols[p]->info.type = pType->type;
    ppCols[p]->info.bytes = pType->bytes;
    ppCols[p]->info.scale = pType->scale;
    ppCols[p]->info.precision = pType->precision;

    SScalarParam output = {.columnData = ppCols[p]};
    code = scalarCalculate(pExpr, pBlockList, &output, NULL);
    if (code != TSDB_CODE_SUCCESS || output.numOfRows != pBlock->info.rows) {
      if (code == TSDB_CODE_SUCCESS) code = TSDB_CODE_INTERNAL_ERROR;
      ST_TASK_ELOG("ext: vectorized partition expression slot:%d failed rows:%d expected:%" PRId64 " code:%d",
                   p, output.numOfRows, pBlock->info.rows, code);
      break;
    }
    ++evaluated;
  }
  taosArrayDestroy(pBlockList);

  if (code != TSDB_CODE_SUCCESS) {
    extDestroyPartitionResultCols(ppCols, pCtx->nPart);
    return code;
  }
  *ppResultCols = ppCols;
  ST_TASK_DLOG("ext: vectorized %d partition expression(s) over %" PRId64 " distinct tag row(s)", evaluated,
               pBlock->info.rows);
  return TSDB_CODE_SUCCESS;
}

static int32_t extAllocInfluxTagValueBufs(int32_t nTags, char **ppValBufs) {
  *ppValBufs = NULL;
  if (nTags <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  *ppValBufs = taosMemoryCalloc(nTags, EXT_INFLUX_TAG_NCHAR_CHARS);
  if (*ppValBufs == NULL) {
    stError("ext: allocate distinct tag value buffers failed count:%d bytes:%zu code:%d",
            nTags, (size_t)EXT_INFLUX_TAG_NCHAR_CHARS, terrno);
    return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t extExtractInfluxTagRowValues(SInfluxDistinctCtx *pDCtx, SSDataBlock *pBlock, int32_t row,
                                            char *pValBufs, const char **colNames, const char **colVals) {
  const SStreamTask *pTask = pDCtx->pInfo->pTask;
  for (int32_t c = 0; c < pDCtx->nTags; ++c) {
    char            *pValBuf = pValBufs + (size_t)c * EXT_INFLUX_TAG_NCHAR_CHARS;
    SColumnInfoData *pCol = taosArrayGet(pBlock->pDataBlock, c);

    colNames[c] = (const char *)taosArrayGet(pDCtx->pTagCols, c);
    if (pCol != NULL && !colDataIsNull_s(pCol, row)) {
      int32_t code = extColCellToStr(pCol, row, pValBuf, EXT_INFLUX_TAG_NCHAR_CHARS - TSDB_NCHAR_SIZE);
      if (code < 0) {
        ST_TASK_ELOG("ext: distinct tag value conversion failed row:%d tag:%d code:%d", row, c, code);
        return code;
      }
      colVals[c] = pValBuf;
    } else {
      pValBuf[0] = '\0';
      colVals[c] = "";
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t extBuildInfluxPartitionValues(SInfluxDistinctCtx *pDCtx, SColumnInfoData **ppExprResults,
                                             int32_t row, const char **colNames, const char **colVals,
                                             SArray **ppPartitionValues) {
  const SStreamTask *pTask = pDCtx->pInfo->pTask;
  *ppPartitionValues = NULL;
  if (pDCtx->nPart <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SArray *pPartitionValues = taosArrayInit(pDCtx->nPart, sizeof(SStreamGroupValue));
  if (pPartitionValues == NULL) {
    return terrno;
  }

  for (int32_t p = 0; p < pDCtx->nPart; ++p) {
    SStreamGroupValue partValue = {0};
    const char       *pColName = (const char *)taosArrayGet(pDCtx->pInfo->spec.partitionTagCols, p);
    int32_t           code = TSDB_CODE_SUCCESS;

    if (pColName != NULL && strcmp(pColName, INFLUXDB_PARTITION_BY_TBNAME) == 0) {
      partValue.isNull = true;
    } else if (ppExprResults != NULL && ppExprResults[p] != NULL) {
      code = extColumnRowToGroupValue(ppExprResults[p], row, &partValue);
    } else {
      int32_t idx = pDCtx->partIdx[p];
      code = idx >= 0 ? extBuildNCharGroupValue(pTask, colNames[idx], colVals[idx], &partValue)
                      : TSDB_CODE_MND_STREAM_TBNAME_CALC_FAILED;
    }

    if (code != TSDB_CODE_SUCCESS || taosArrayPush(pPartitionValues, &partValue) == NULL) {
      if (code == TSDB_CODE_SUCCESS) {
        code = terrno;
      }
      tDestroySStreamGroupValue(&partValue);
      taosArrayDestroyEx(pPartitionValues, tDestroySStreamGroupValue);
      return code;
    }
  }

  *ppPartitionValues = pPartitionValues;
  return TSDB_CODE_SUCCESS;
}

static int32_t extBuildInfluxGroupId(const SInfluxDistinctCtx *pDCtx, uint64_t uid,
                                     SArray *pPartitionValues, uint64_t *pGroupId) {
  const SStreamTask *pTask = pDCtx->pInfo->pTask;
  if (pDCtx->groupMode == 1) {
    *pGroupId = uid;
    return TSDB_CODE_SUCCESS;
  }

  if (pDCtx->groupMode != 2) {
    *pGroupId = pDCtx->measurementGroupId;
    return TSDB_CODE_SUCCESS;
  }

  char    subKey[EXT_TAGSET_KEY_MAX * TSDB_NCHAR_SIZE] = {0};
  int32_t subLen = 0;
  for (int32_t p = 0; p < pDCtx->nPart; ++p) {
    SStreamGroupValue *pPartValue = (SStreamGroupValue *)taosArrayGet(pPartitionValues, p);
    int32_t code = appendPartitionGroupKey(subKey, sizeof(subKey), &subLen, pPartValue);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("ext: partition tuple key exceeds %d bytes at slot:%d code:%d",
                   (int32_t)sizeof(subKey), p, code);
      return code;
    }
  }

  *pGroupId = MurmurHash3_64(subKey, subLen) | 1ULL;
  return TSDB_CODE_SUCCESS;
}

/* Register each distinct tag combination as a uid entry. */
static int32_t influxDistinctBlockCb(SSDataBlock *pBlock, void *pCtx) {
  SInfluxDistinctCtx    *pDCtx = (SInfluxDistinctCtx *)pCtx;
  SStreamExtReaderInfo  *pInfo = pDCtx->pInfo;
  const SStreamTask     *pTask = pInfo->pTask;
  int32_t                nTags = pDCtx->nTags;
  int32_t                code = TSDB_CODE_SUCCESS;
  SColumnInfoData      **ppExprResults = NULL;
  char                  *pValBufs = NULL;

  if (nTags > TSDB_MAX_TAGS) {
    ST_TASK_ELOG("ext: distinct tag count exceeds limit count:%d limit:%d", nTags, TSDB_MAX_TAGS);
    blockDataDestroy(pBlock);
    return TSDB_CODE_OUT_OF_RANGE;
  }

  code = extEvaluatePartitionExprBlock(pDCtx, pBlock, &ppExprResults);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return code;
  }

  code = extAllocInfluxTagValueBufs(nTags, &pValBufs);
  if (code != TSDB_CODE_SUCCESS) {
    extDestroyPartitionResultCols(ppExprResults, pDCtx->nPart);
    blockDataDestroy(pBlock);
    return code;
  }

  for (int32_t r = 0; r < pBlock->info.rows; ++r) {
    char tagsetKey[EXT_TAGSET_KEY_MAX] = {0};
    const char *colNames[TSDB_MAX_TAGS] = {0};
    const char *colVals[TSDB_MAX_TAGS]  = {0};
    SArray     *pPartitionValues = NULL;
    uint64_t    groupId = 0;

    code = extExtractInfluxTagRowValues(pDCtx, pBlock, r, pValBufs, colNames, colVals);
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }

    int32_t keyLen = buildTagsetKey(tagsetKey, sizeof(tagsetKey), colNames, colVals, nTags);
    if (keyLen < 0) {
      ST_TASK_WLOG("%s", "ext: initInfluxTagPartition tagset key truncated, skipping row");
      continue;
    }

    uint64_t uid = MurmurHash3_64(tagsetKey, keyLen) | 1ULL;
    if (tSimpleHashGet(pInfo->pUidIndex, &uid, sizeof(uid)) != NULL) continue;

    code = extBuildInfluxPartitionValues(pDCtx, ppExprResults, r, colNames, colVals, &pPartitionValues);
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }

    code = extBuildInfluxGroupId(pDCtx, uid, pPartitionValues, &groupId);
    if (code != TSDB_CODE_SUCCESS) {
      taosArrayDestroyEx(pPartitionValues, tDestroySStreamGroupValue);
      break;
    }

    code = extTableListInsertEntry(pInfo, uid, groupId, tagsetKey, keyLen, pPartitionValues);
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }
    pDCtx->nGroups++;
  }

  taosMemoryFree(pValBufs);
  extDestroyPartitionResultCols(ppExprResults, pDCtx->nPart);
  blockDataDestroy(pBlock);
  return code;
}

typedef struct {
  uint64_t measurementGroupId;
  int8_t   groupMode;
  int32_t  nPart;
  int32_t *partIdx;
  SNode  **partExprNodes;
} SInfluxPartitionPlan;

static void extDestroyInfluxPartitionPlan(SInfluxPartitionPlan *pPlan) {
  if (pPlan == NULL) {
    return;
  }

  taosMemoryFree(pPlan->partIdx);
  taosMemoryFree(pPlan->partExprNodes);
  memset(pPlan, 0, sizeof(*pPlan));
}

static int32_t extDiscoverInfluxTagCols(SStreamExtReaderInfo *pInfo, SArray **ppTagCols) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t code = TSDB_CODE_SUCCESS;

  char schemaSQL[EXT_SCHEMA_SQL_BUF_LEN] = {0};
  snprintf(schemaSQL, sizeof(schemaSQL),
           "SELECT column_name FROM information_schema.columns "
           "WHERE table_name = '%s' AND data_type LIKE 'Dictionary%%' "
           "ORDER BY ordinal_position",
           pInfo->spec.extTable);
  ST_TASK_DLOG("ext: initInfluxTagPartition schema SQL=\"%s\"", schemaSQL);

  SExtColTypeMapping nameMapping = {0};
  nameMapping.tdType.type = TSDB_DATA_TYPE_NCHAR;
  nameMapping.tdType.bytes = EXT_INFLUX_KEY_NCHAR_CHARS_TOO_LONG + VARSTR_HEADER_SIZE;
  tstrncpy(nameMapping.colName, "column_name", sizeof(nameMapping.colName));

  SArray *pTagCols = taosArrayInit(8, EXT_INFLUX_KEY_NCHAR_CHARS);
  if (pTagCols == NULL) {
    return terrno;
  }

  code = extQueryExecForEach(pInfo->pConn, schemaSQL, &nameMapping, 1, influxSchemaBlockCb, pTagCols);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: initInfluxTagPartition schema query failed code:%d", code);
    taosArrayDestroy(pTagCols);
    return code;
  }

  ST_TASK_DLOG("ext: initInfluxTagPartition discovered %d tag columns",
               (int32_t)taosArrayGetSize(pTagCols));
  *ppTagCols = pTagCols;
  return TSDB_CODE_SUCCESS;
}

static int32_t extBuildInfluxDistinctSql(const SStreamExtReaderInfo *pInfo, SArray *pTagCols,
                                         char *distSQL, int32_t distSqlLen) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t nTags = (int32_t)taosArrayGetSize(pTagCols);
  int32_t off = 0;
  bool    ok = extSqlCat(distSQL, distSqlLen, &off, "SELECT DISTINCT ");

  for (int32_t i = 0; ok && i < nTags; ++i) {
    const char *pColName = (const char *)taosArrayGet(pTagCols, i);
    ok = extSqlCat(distSQL, distSqlLen, &off, "%s%s", pColName, (i + 1 < nTags) ? ", " : "");
  }
  if (ok) {
    ok = extSqlCat(distSQL, distSqlLen, &off, " FROM %s", pInfo->spec.extTable);
  }
  if (!ok) {
    ST_TASK_ELOG("ext: initInfluxTagPartition distinct SQL exceeds %d bytes for %d tag cols",
                 distSqlLen, nTags);
    return TSDB_CODE_OUT_OF_RANGE;
  }

  ST_TASK_DLOG("ext: initInfluxTagPartition distinct SQL=\"%s\"", distSQL);
  return TSDB_CODE_SUCCESS;
}

static int32_t extBuildInfluxTagMappings(SArray *pTagCols, SExtColTypeMapping **ppMappings) {
  int32_t nTags = (int32_t)taosArrayGetSize(pTagCols);
  SExtColTypeMapping *pMappings = taosMemoryCalloc(nTags, sizeof(SExtColTypeMapping));
  if (pMappings == NULL) {
    return terrno;
  }

  for (int32_t i = 0; i < nTags; ++i) {
    pMappings[i].tdType.type = TSDB_DATA_TYPE_NCHAR;
    pMappings[i].tdType.bytes = EXT_INFLUX_TAG_NCHAR_CHARS;
    tstrncpy(pMappings[i].colName, (const char *)taosArrayGet(pTagCols, i), sizeof(pMappings[i].colName));
  }

  *ppMappings = pMappings;
  return TSDB_CODE_SUCCESS;
}

static uint64_t extBuildInfluxMeasurementGroupId(const SStreamExtReaderInfo *pInfo) {
  char mkey[TSDB_TABLE_NAME_LEN * 2] = {0};
  int32_t mkeyLen = snprintf(mkey, sizeof(mkey), "%s.%s",
                             extTableQualifier(&pInfo->spec), pInfo->spec.extTable);
  return MurmurHash3_64(mkey, mkeyLen) | 1ULL;
}

static int32_t extResolveInfluxPartitionPlan(SStreamExtReaderInfo *pInfo, SArray *pTagCols,
                                             SInfluxPartitionPlan *pPlan) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t code = TSDB_CODE_SUCCESS;

  pPlan->measurementGroupId = extBuildInfluxMeasurementGroupId(pInfo);
  pPlan->groupMode = 0;
  pPlan->nPart = 0;
  pPlan->partIdx = NULL;
  pPlan->partExprNodes = NULL;

  if (!pInfo->spec.partitionByTag) {
    ST_TASK_DLOG("ext: initInfluxTagPartition groupMode=%d nPart=%d measurementGid=%" PRIu64,
                 (int)pPlan->groupMode, pPlan->nPart, pPlan->measurementGroupId);
    return TSDB_CODE_SUCCESS;
  }

  int32_t nPartCols = pInfo->spec.partitionTagCols
                          ? (int32_t)taosArrayGetSize(pInfo->spec.partitionTagCols)
                          : 0;
  if (nPartCols == 0) {
    ST_TASK_ELOG("%s", "ext: PARTITION BY has no positional tag or expression slots");
    return TSDB_CODE_MND_STREAM_TBNAME_CALC_FAILED;
  }

  pPlan->partIdx = taosMemoryCalloc(nPartCols, sizeof(int32_t));
  pPlan->partExprNodes = taosMemoryCalloc(nPartCols, sizeof(SNode *));
  if (pPlan->partIdx == NULL || pPlan->partExprNodes == NULL) {
    return terrno;
  }

  int32_t nTags = (int32_t)taosArrayGetSize(pTagCols);
  bool    allResolved = true;
  for (int32_t p = 0; p < nPartCols; ++p) {
    pPlan->partIdx[p] = -1;
    pPlan->partExprNodes[p] = pInfo->pPartitionColExprNodes
                                  ? (SNode *)taosArrayGetP(pInfo->pPartitionColExprNodes, p)
                                  : NULL;
    if (pPlan->partExprNodes[p] != NULL) {
      continue;
    }

    const char *pCol = (const char *)taosArrayGet(pInfo->spec.partitionTagCols, p);
    if (pCol != NULL && strcmp(pCol, INFLUXDB_PARTITION_BY_TBNAME) == 0) {
      continue;
    }
    if (pCol == NULL || pCol[0] == '\0') {
      ST_TASK_ELOG("ext: partition slot:%d has neither a tag column nor an expression", p);
      allResolved = false;
      break;
    }

    for (int32_t t = 0; t < nTags; ++t) {
      if (strcasecmp(pCol, (const char *)taosArrayGet(pTagCols, t)) == 0) {
        pPlan->partIdx[p] = t;
        break;
      }
    }
    if (pPlan->partIdx[p] < 0) {
      ST_TASK_ELOG("ext: partition tag column '%s' not found for slot:%d", pCol, p);
      allResolved = false;
      break;
    }
  }
  if (!allResolved) {
    return TSDB_CODE_MND_STREAM_TBNAME_CALC_FAILED;
  }

  pPlan->nPart = nPartCols;
  pPlan->groupMode = pInfo->spec.partitionByTbname ? 1 : 2;
  code = bindPartitionExprNodes(pInfo, pTagCols);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  ST_TASK_DLOG("ext: initInfluxTagPartition groupMode=%d nPart=%d measurementGid=%" PRIu64,
               (int)pPlan->groupMode, pPlan->nPart, pPlan->measurementGroupId);
  return TSDB_CODE_SUCCESS;
}

static int32_t extEnumerateInfluxDistinctGroups(SStreamExtReaderInfo *pInfo, SArray *pTagCols,
                                                SExtColTypeMapping *pMappings,
                                                const SInfluxPartitionPlan *pPlan,
                                                int32_t *pGroupCount) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t nTags = (int32_t)taosArrayGetSize(pTagCols);
  char    distSQL[EXT_BATCH_SQL_BUF_LEN] = {0};
  int32_t code = extBuildInfluxDistinctSql(pInfo, pTagCols, distSQL, sizeof(distSQL));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SInfluxDistinctCtx dCtx = {
    .pInfo = pInfo,
    .pTagCols = pTagCols,
    .nTags = nTags,
    .nGroups = 0,
    .groupMode = pPlan->groupMode,
    .measurementGroupId = pPlan->measurementGroupId,
    .partIdx = pPlan->partIdx,
    .nPart = pPlan->nPart,
    .partExprNodes = pPlan->partExprNodes,
  };

  code = extQueryExecForEach(pInfo->pConn, distSQL, pMappings, nTags, influxDistinctBlockCb, &dCtx);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: initInfluxTagPartition distinct query failed code:%d", code);
    return code;
  }

  *pGroupCount = dCtx.nGroups;
  return TSDB_CODE_SUCCESS;
}

static int32_t extInsertInfluxSingleGroup(SStreamExtReaderInfo *pInfo) {
  const SStreamTask *pTask = pInfo->pTask;
  char    key[TSDB_TABLE_NAME_LEN * 2] = {0};
  int32_t keyLen = snprintf(key, sizeof(key), "%s.%s",
                            extTableQualifier(&pInfo->spec), pInfo->spec.extTable);
  uint64_t uid = MurmurHash3_64(key, keyLen) | 1ULL;

  ST_TASK_DLOG("ext: initInfluxTagPartition no tag combos — single group uid=%" PRIu64, uid);
  return extTableListInsertEntry(pInfo, uid, uid, "", 0, NULL);
}

/* InfluxDB: query information_schema.columns for tag column names,
 * then SELECT DISTINCT <tags> to enumerate live tag combinations.
 * Each combination → one uid/group entry. */
static int32_t extInitInfluxTagPartition(SStreamExtReaderInfo *pInfo) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               nGroups = 0;
  SArray               *pTagCols = NULL;
  SExtColTypeMapping   *pMappings = NULL;
  SInfluxPartitionPlan  plan = {0};

  code = extDiscoverInfluxTagCols(pInfo, &pTagCols);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  if ((int32_t)taosArrayGetSize(pTagCols) == 0) {
    code = extInsertInfluxSingleGroup(pInfo);
    goto _exit;
  }

  code = extBuildInfluxTagMappings(pTagCols, &pMappings);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  code = extResolveInfluxPartitionPlan(pInfo, pTagCols, &plan);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  code = extEnumerateInfluxDistinctGroups(pInfo, pTagCols, pMappings, &plan, &nGroups);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  if (nGroups > 0) {
    if (pInfo->pInfluxTagCols != NULL) {
      taosArrayDestroy(pInfo->pInfluxTagCols);
    }
    pInfo->pInfluxTagCols = pTagCols;
    pTagCols = NULL;
    ST_TASK_DLOG("ext: initInfluxTagPartition inserted %d tag-combination groups", nGroups);
    goto _exit;
  }

  code = extInsertInfluxSingleGroup(pInfo);

_exit:
  extDestroyInfluxPartitionPlan(&plan);
  taosMemoryFree(pMappings);
  taosArrayDestroy(pTagCols);
  return code;
}

/* Build a stable uid key string for MySQL/PG (no tag concept). */
static int32_t extInitSingleGroup(SStreamExtReaderInfo *pInfo) {
  const SStreamTask *pTask = pInfo->pTask;
  char   key[TSDB_TABLE_NAME_LEN * 3] = {0};
  int32_t keyLen = snprintf(key, sizeof(key), "%s.%s.%s",
                             pInfo->spec.sourceName,
                             extTableQualifier(&pInfo->spec),
                             pInfo->spec.extTable);
  uint64_t uid = MurmurHash3_64(key, keyLen) | 1ULL;
  ST_TASK_DLOG("ext: initSingleGroup uid=%" PRIu64 " key=\"%s\"", uid, key);
  return extTableListInsertEntry(pInfo, uid, uid, NULL, 0, NULL);
}

static int32_t streamExtReaderInitTableList(SStreamExtReaderInfo *pInfo) {
  const SStreamTask *pTask = pInfo->pTask;
  ST_TASK_DLOG("ext: initTableList enter sourceType=%d source=%s db=%s schema=%s tbl=%s",
               (int)pInfo->spec.sourceType, pInfo->spec.sourceName,
               pInfo->spec.extDb, pInfo->spec.extSchema, pInfo->spec.extTable);

  int32_t code = TSDB_CODE_SUCCESS;
  bool isInflux = ((EExtSourceType)pInfo->spec.sourceType == EXT_SOURCE_INFLUXDB);

  if (isInflux) {
    /* InfluxDB always enumerates sub-tables (one uid per full tag-value
     * combination) via information_schema + SELECT DISTINCT, regardless of
     * PARTITION BY.  groupId is then derived from the partition-tag subset
     * inside extInitInfluxTagPartition (many uids may share one groupId). */
    code = extInitInfluxTagPartition(pInfo);
  } else {
    /* MySQL / PostgreSQL: single uid per external table. */
    code = extInitSingleGroup(pInfo);
  }

  if (code == TSDB_CODE_SUCCESS) {
    ST_TASK_DLOG("ext: initTableList done uidCount=%d groupCount=%d",
                 tSimpleHashGetSize(pInfo->pUidIndex),
                 tSimpleHashGetSize(pInfo->pGroupIndex));
  } else {
    ST_TASK_ELOG("ext: initTableList failed code:%d", code);
  }
  return code;
}

/* ============================================================
 * PULL dispatcher
 * ============================================================ */
static int32_t extDispatchPullRequest(SStreamExtReaderInfo *pInfo,
                                      ESTriggerPullType pullType,
                                      const SSTriggerExtPullReq *pReq,
                                      SSTriggerExtPullRsp *pRsp) {
  switch (pullType) {
    case STRIGGER_PULL_LAST_TS_EXT:
      return handleLastTsPull(pInfo, pReq, pRsp);
    case STRIGGER_PULL_META_EXT:
      return handleMetaPull(pInfo, pReq, pRsp);
    case STRIGGER_PULL_DATA_EXT:
      return handleDataPull(pInfo, pReq, pRsp);
    case STRIGGER_PULL_META_DATA_EXT:
      return handleMetaDataPull(pInfo, pReq, pRsp);
    case STRIGGER_PULL_CALC_DATA_EXT:
      return handleCalcDataPull(pInfo, pReq, pRsp);
    case STRIGGER_PULL_GROUP_COL_VALUE_EXT:
      return handleGroupColValuePull(pInfo, pReq, pRsp);
    default:
      return TSDB_CODE_INVALID_PARA;
  }
}

int32_t streamReaderExtHandlePull(SStreamExtReaderInfo *pInfo,
                                  int32_t pullType,
                                  const void *pReqVoid,
                                  void **ppRsp) {
  int32_t             code = TSDB_CODE_SUCCESS;
  SSTriggerExtPullRsp *pRsp = NULL;

  if (pInfo == NULL || ppRsp == NULL) {
    stError("ext: handlePull invalid args");
    return TSDB_CODE_INVALID_PARA;
  }

  const SStreamTask *pTask = pInfo->pTask;
  ST_TASK_DLOG("ext: streamReaderExtHandlePull enter pullType=%d pInfo=%p",
               pullType, pInfo);
  *ppRsp = NULL;

  const SSTriggerExtPullReq *pReq = (const SSTriggerExtPullReq *)pReqVoid;
  ESTriggerPullType          typedPullType = (ESTriggerPullType)pullType;

  /* ---- One-shot table-list initialization ----
   * For ext-source readers, pUidIndex is built by querying the external DB
   * (InfluxDB tag combinations, or a single synthetic uid for MySQL/PG).
   * Whenever pUidIndex is empty we re-initialize from the source before
   * dispatching — regardless of pull type. */
  if (tSimpleHashGetSize(pInfo->pUidIndex) == 0) {
    ST_TASK_DLOG("ext: handlePull: uid hash empty (pullType=%d), initializing table list",
                 pullType);
    code = streamExtReaderInitTableList(pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("ext: initTableList failed code:%d — aborting pull", code);
      goto _exit;
    }
  }

  pRsp = allocPullRsp(typedPullType);
  if (pRsp == NULL) {
    code = terrno;
    ST_TASK_ELOG("%s", "ext: handlePull OOM allocating rsp");
    goto _exit;
  }

  code = extDispatchPullRequest(pInfo, typedPullType, pReq, pRsp);
  if (code == TSDB_CODE_INVALID_PARA &&
      !(typedPullType == STRIGGER_PULL_LAST_TS_EXT ||
        typedPullType == STRIGGER_PULL_META_EXT ||
        typedPullType == STRIGGER_PULL_DATA_EXT ||
        typedPullType == STRIGGER_PULL_META_DATA_EXT ||
        typedPullType == STRIGGER_PULL_CALC_DATA_EXT ||
        typedPullType == STRIGGER_PULL_GROUP_COL_VALUE_EXT)) {
    ST_TASK_ELOG("ext: handlePull unknown pullType=%d", pullType);
  }

  if (code == TSDB_CODE_STREAM_NO_DATA) {
    /* No data is a normal signal, not an error.  Propagate the code to the
     * caller (snode.c) so it sets rspMsg.code = NO_DATA, enabling the trigger
     * side to distinguish "empty pull" from a real failure without having to
     * inspect row counts. */
    ST_TASK_DLOG("ext: handler for pullType=%d no data", pullType);
  } else if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: handler for pullType=%d returned code:%d", pullType, code);
    pRsp->code = code;
    /* Return the struct so the caller can inspect pRsp->code. */
  } else {
    ST_TASK_DLOG("ext: handler for pullType=%d ok", pullType);
  }

  *ppRsp = pRsp;
  pRsp = NULL;

_exit:
  if (pRsp != NULL) {
    streamExtPullRspFree(pRsp);
  }
  return code;
}

/* Forward declarations: influx batched implementation is defined later
 * (after buildInfluxBatchSql/SInfluxUidAgg/influxAccumBlockCb, which it
 * reuses), alongside handleMetaPullInflux. */
static int32_t handleLastTsPullInflux(SStreamExtReaderInfo *pInfo,
                                      SArray *pAllUids, int32_t totalUids,
                                      const char *tsCol, SArray *pLastTsArr);

/* ============================================================
 * Handler: STRIGGER_PULL_LAST_TS_EXT (relational: MySQL/PostgreSQL)
 *
 * Relational sources are single-group (uid == groupId, one uid per external
 * table — see extInitSingleGroup), so a single global MAX(ts) trivially
 * applies to the only uid.
 * SQL: SELECT MAX(<tsCol>) FROM <extDb>.<extTable> [WHERE prefilter]
 * ============================================================ */
static int32_t handleLastTsPullRelational(SStreamExtReaderInfo *pInfo,
                                          const char *tsCol,
                                          SArray *pLastTsArr) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t            code  = TSDB_CODE_SUCCESS;

  char sqlBuf[EXT_SIMPLE_SQL_BUF_LEN] = {0};
  char prefilterBuf[EXT_SQL_CLAUSE_BUF_LEN] = {0};
  /* Trigger reader uses PRE_FILTER (triggerPrefilter), not the calc WHERE. */
  code = buildTriggerPrefilterClause(&pInfo->spec, prefilterBuf, sizeof(prefilterBuf));
  if (code < 0) return code;

  /* Build SQL: SELECT MAX(ts) AS ts FROM <table> [WHERE triggerPrefilter]
   * The AS alias ensures the result column name matches tsMapping.colName
   * for databases (e.g. InfluxDB 3.x DataFusion) that name aggregates
   * as "MAX(ts)" rather than the bare column name. */
  char tblRef[TSDB_TABLE_NAME_LEN + TSDB_DB_NAME_LEN + 2] = {0};
  code = buildExtTableRef(&pInfo->spec, tblRef, sizeof(tblRef));
  if (code < 0) return code;
  int32_t off = 0;
  bool    ok  = extSqlCat(sqlBuf, sizeof(sqlBuf), &off,
                          "SELECT MAX(%s) AS %s FROM %s",
                          tsCol, tsCol, tblRef);
  if (prefilterBuf[0] != '\0') {
    ok = extSqlCat(sqlBuf, sizeof(sqlBuf), &off, " WHERE %s", prefilterBuf);
    /* Strip trailing " AND " added by buildPrefilterClause. */
    int32_t plen = strlen(prefilterBuf);
    if (plen >= 5 && strcmp(prefilterBuf + plen - 5, " AND ") == 0) {
      sqlBuf[off - 5] = '\0';
    }
  }
  if (!ok) {
    ST_TASK_ELOG("ext: lastTs relational SQL exceeds %d bytes", (int32_t)sizeof(sqlBuf));
    return TSDB_CODE_OUT_OF_RANGE;
  }
  ST_TASK_DLOG("ext: lastTs relational SQL=\"%s\"", sqlBuf);

  /* Execute query with a single-column TIMESTAMP mapping for MAX(ts).
   * All reader->trigger timestamps are normalised to nanoseconds so the
   * trigger side has a single uniform precision to convert from.
   * The connector will scale the source value (µs for MySQL/PG, ns for InfluxDB)
   * to ns when tsPrecision is set to TSDB_TIME_PRECISION_NANO. */
  SExtColTypeMapping tsMapping = {0};
  tsMapping.tdType.type   = TSDB_DATA_TYPE_TIMESTAMP;
  tsMapping.tdType.bytes  = sizeof(int64_t);
  tstrncpy(tsMapping.colName, tsCol, sizeof(tsMapping.colName));

  SSDataBlock *pBlock = NULL;
  code = extQueryExecFetchAll(pInfo->pConn, sqlBuf,
                              &tsMapping, 1, &pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: handleLastTsPullRelational query failed code:%d", code);
    return code;
  }

  /* Extract max ts value from the first row, first column. */
  int64_t globalMaxTs = INT64_MIN;
  if (pBlock != NULL && pBlock->info.rows > 0) {
    SColumnInfoData *pCol = taosArrayGet(pBlock->pDataBlock, 0);
    if (pCol && !colDataIsNull_s(pCol, 0)) {
      globalMaxTs = *(int64_t *)colDataGetData(pCol, 0);
      ST_TASK_DLOG("ext: lastTs globalMaxTs=%" PRId64, globalMaxTs);
    }
    printDataBlock(pBlock, __func__, "ext_last_ts_raw", pTask->streamId);
    blockDataDestroy(pBlock);
  }

  int32_t iter = 0;
  void   *pVal = NULL;
  while ((pVal = tSimpleHashIterate(pInfo->pUidIndex, pVal, &iter)) != NULL) {
    size_t         kLen  = 0;
    uint64_t        uid   = *(uint64_t *)tSimpleHashGetKey(pVal, &kLen);
    SUidIndexEntry *pEnt = (SUidIndexEntry *)pVal;
    SExtLastTsInfo info  = {
      .uid = (int64_t)uid,
      .gid = (int64_t)pEnt->groupId,
      .ts  = globalMaxTs,
    };
    taosArrayPush(pLastTsArr, &info);
  }
  return TSDB_CODE_SUCCESS;
}

/* ============================================================
 * Handler: STRIGGER_PULL_LAST_TS_EXT
 *
 * Returns SArray<SExtLastTsInfo{uid, gid, ts}> for every uid in pUidIndex,
 * used by the trigger as the initial per-uid watermark (subsequent META_EXT
 * pulls fetch rows with ts > this value).
 *
 * Relational sources (MySQL/PG) are single-group, so one global MAX(ts)
 * query suffices (handleLastTsPullRelational).  InfluxDB sources enumerate
 * one uid per sub-table (tag combination), so MAX(ts) must be computed PER
 * UID using that uid's own tagset as the WHERE filter — handled by the
 * OR-batched handleLastTsPullInflux (mirrors handleMetaPullInflux).
 * ============================================================ */
static int32_t handleLastTsPull(SStreamExtReaderInfo *pInfo,
                                const SSTriggerExtPullReq *pReq,
                                SSTriggerExtPullRsp *pRsp) {
  const SStreamTask *pTask = pInfo->pTask;
  ST_TASK_DLOG("ext: handleLastTsPull enter source=%s", pInfo->spec.sourceName);

  bool        isInflux = ((EExtSourceType)pInfo->spec.sourceType == EXT_SOURCE_INFLUXDB);
  const char *tsCol    = (pInfo->spec.tsColumn[0] != '\0')
                           ? pInfo->spec.tsColumn : "ts";

  int32_t uidCnt = tSimpleHashGetSize(pInfo->pUidIndex);
  pRsp->pLastTsArr = taosArrayInit(TMAX(uidCnt, 1), sizeof(SExtLastTsInfo));
  if (pRsp->pLastTsArr == NULL) {
    ST_TASK_ELOG("%s", "ext: handleLastTsPull OOM");
    return terrno;
  }

  int32_t code;
  if (isInflux) {
    SArray *pAllUids = collectAllUids(pInfo->pUidIndex);
    if (pAllUids == NULL) {
      ST_TASK_ELOG("%s", "ext: handleLastTsPull collectAllUids OOM");
      return terrno;
    }
    int32_t totalUids = (int32_t)taosArrayGetSize(pAllUids);
    code = handleLastTsPullInflux(pInfo, pAllUids, totalUids, tsCol, pRsp->pLastTsArr);
    taosArrayDestroy(pAllUids);
  } else {
    code = handleLastTsPullRelational(pInfo, tsCol, pRsp->pLastTsArr);
  }

  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: handleLastTsPull failed code:%d", code);
    return code;
  }

  ST_TASK_DLOG("ext: handleLastTsPull done, returned %d entries",
               (int32_t)taosArrayGetSize(pRsp->pLastTsArr));
  return TSDB_CODE_SUCCESS;
}

/* ============================================================
 * metaBlock allocator: 5 columns {groupId, skey, ekey, uid, rows}.
 * ============================================================ */
static SSDataBlock *createMetaBlock(void) {
  /* TODO(P3): use blockDataCreate / blockDataAppendColInfo properly.
   * For now, create an SSDataBlock with 5 BIGINT columns. */
  SSDataBlock *pBlock = taosMemoryCalloc(1, sizeof(SSDataBlock));
  if (pBlock == NULL) return NULL;

  pBlock->pDataBlock = taosArrayInit(META_COL_COUNT, sizeof(SColumnInfoData));
  if (pBlock->pDataBlock == NULL) {
    taosMemoryFree(pBlock);
    return NULL;
  }
  for (int32_t i = 0; i < META_COL_COUNT; i++) {
    SColumnInfoData col = {0};
    col.info.type  = TSDB_DATA_TYPE_BIGINT;
    col.info.bytes = sizeof(int64_t);
    col.info.colId = i + 1;
    taosArrayPush(pBlock->pDataBlock, &col);
  }
  return pBlock;
}

/* Append one row to a metaBlock.  All values are int64. */
static int32_t metaBlockAppendRow(SSDataBlock *pBlock,
                                  uint64_t groupId, int64_t skey, int64_t ekey,
                                  uint64_t uid, int64_t rows) {
  int32_t row = pBlock->info.rows;
  /* Ensure capacity. */
  int32_t code = blockDataEnsureCapacity(pBlock, row + 1);
  if (code != TSDB_CODE_SUCCESS) return code;

  int64_t vals[META_COL_COUNT] = {(int64_t)groupId, skey, ekey, (int64_t)uid, rows};
  for (int32_t c = 0; c < META_COL_COUNT; c++) {
    SColumnInfoData *pCol = taosArrayGet(pBlock->pDataBlock, c);
    code = colDataSetVal(pCol, row, (const char *)&vals[c], false);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  pBlock->info.rows = row + 1;
  return TSDB_CODE_SUCCESS;
}

static int64_t extLookupUidMaxTs(SSHashObj *pUidMaxTs, uint64_t uid) {
  int64_t maxTs = INT64_MIN;

  if (pUidMaxTs != NULL) {
    int64_t *pMaxTs = (int64_t *)tSimpleHashGet(pUidMaxTs, &uid, sizeof(uid));
    if (pMaxTs != NULL) {
      maxTs = *pMaxTs;
    }
  }

  return maxTs;
}

static void extInitRelationalMetaMappings(const char *tsCol, SExtColTypeMapping metaMappings[4]) {
  metaMappings[0].tdType.type = TSDB_DATA_TYPE_TIMESTAMP;
  metaMappings[0].tdType.bytes = sizeof(int64_t);
  tstrncpy(metaMappings[0].colName, tsCol, sizeof(metaMappings[0].colName));

  metaMappings[1].tdType.type = TSDB_DATA_TYPE_TIMESTAMP;
  metaMappings[1].tdType.bytes = sizeof(int64_t);
  tstrncpy(metaMappings[1].colName, tsCol, sizeof(metaMappings[1].colName));

  metaMappings[2].tdType.type = TSDB_DATA_TYPE_BIGINT;
  metaMappings[2].tdType.bytes = sizeof(int64_t);
  tstrncpy(metaMappings[2].colName, "uid", sizeof(metaMappings[2].colName));

  metaMappings[3].tdType.type = TSDB_DATA_TYPE_BIGINT;
  metaMappings[3].tdType.bytes = sizeof(int64_t);
  tstrncpy(metaMappings[3].colName, "cnt", sizeof(metaMappings[3].colName));
}

static int32_t extBuildRelationalMetaSql(const char *tblRef, const char *prefilterBuf,
                                         const char *tsCol, uint64_t uid, int64_t maxTs,
                                         char *sqlBuf, int32_t sqlBufLen) {
  char dtBuf[32] = {0};
  epochToDatetimeStr(maxTs, TSDB_TIME_PRECISION_MICRO, dtBuf, sizeof(dtBuf));

  int32_t sqlLen = snprintf(sqlBuf, sqlBufLen,
                            "SELECT MIN(%s),MAX(%s),%" PRIu64 ",COUNT(*) FROM %s "
                            "WHERE %s%s > %s",
                            tsCol, tsCol, uid,
                            tblRef,
                            prefilterBuf,
                            tsCol, dtBuf);
  if (sqlLen < 0 || sqlLen >= sqlBufLen) {
    stError("ext: meta relational SQL exceeds %d bytes uid=%" PRIu64,
            sqlBufLen, uid);
    return TSDB_CODE_OUT_OF_RANGE;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t extAppendRelationalMetaRow(const SStreamTask *pTask, SSDataBlock *pBlock,
                                          uint64_t groupId, uint64_t uid, SSDataBlock *pMetaBlock,
                                          int32_t *pTotalFetched) {
  if (pBlock == NULL || pBlock->info.rows == 0) {
    ST_TASK_DLOG("ext: meta relational uid=%" PRIu64 " no rows", uid);
    return TSDB_CODE_SUCCESS;
  }

  if (taosArrayGetSize(pBlock->pDataBlock) < 4) {
    ST_TASK_WLOG("ext: meta relational uid=%" PRIu64 " block cols=%d < 4, skip",
                 uid, (int32_t)taosArrayGetSize(pBlock->pDataBlock));
    return TSDB_CODE_SUCCESS;
  }

  SColumnInfoData *colMin = taosArrayGet(pBlock->pDataBlock, 0);
  SColumnInfoData *colMax = taosArrayGet(pBlock->pDataBlock, 1);
  SColumnInfoData *colCnt = taosArrayGet(pBlock->pDataBlock, 3);
  if (colMin == NULL || colMax == NULL ||
      colDataIsNull_s(colMin, 0) || colDataIsNull_s(colMax, 0)) {
    return TSDB_CODE_SUCCESS;
  }

  int64_t skey = *(int64_t *)colDataGetData(colMin, 0);
  int64_t ekey = *(int64_t *)colDataGetData(colMax, 0);
  int64_t cnt = colCnt ? *(int64_t *)colDataGetData(colCnt, 0) : 0;

  printDataBlock(pBlock, __func__, "ext_meta_relational_raw", pTask->streamId);

  int32_t code = metaBlockAppendRow(pMetaBlock, groupId, skey, ekey, uid, cnt);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  *pTotalFetched += (int32_t)cnt;
  if (*pTotalFetched >= STREAM_RETURN_ROWS_NUM) {
    ST_TASK_DLOG("%s", "ext: meta relational hit row threshold, returning");
  }

  return TSDB_CODE_SUCCESS;
}

/* ============================================================
 * handleMetaPullRelational — MySQL / PostgreSQL meta pull.
 *
 * Issues one SELECT MIN/MAX/COUNT per uid, accumulates rows into
 * pRsp->pMetaBlock, updates pUidIndex watermarks.
 * Returns TSDB_CODE_SUCCESS; caller checks pRsp->pMetaBlock->info.rows
 * against STREAM_RETURN_ROWS_NUM to determine whether to re-PULL.
 * ============================================================ */
static int32_t handleMetaPullRelational(SStreamExtReaderInfo *pInfo,
                                        SArray                   *pAllUids,
                                        int32_t                   totalUids,
                                        const char               *tsCol,
                                        SSHashObj                *pUidMaxTs,
                                        SSTriggerExtPullRsp      *pRsp) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t totalFetched = 0;
  SSDataBlock *pBlock = NULL;
  SExtColTypeMapping metaMappings[4] = {0};
  char prefilterBuf[EXT_SQL_CLAUSE_BUF_LEN] = {0};
  char tblRef[TSDB_TABLE_NAME_LEN + TSDB_DB_NAME_LEN + 2] = {0};

  code = buildTriggerPrefilterClause(&pInfo->spec, prefilterBuf, sizeof(prefilterBuf));
  if (code < 0) {
    goto _exit;
  }

  code = buildExtTableRef(&pInfo->spec, tblRef, sizeof(tblRef));
  if (code < 0) {
    goto _exit;
  }

  extInitRelationalMetaMappings(tsCol, metaMappings);

  for (int32_t i = 0; i < totalUids; ++i) {
    uint64_t uid = *(uint64_t *)taosArrayGet(pAllUids, i);
    SUidIndexEntry *pEntry = tSimpleHashGet(pInfo->pUidIndex, &uid, sizeof(uid));
    char sqlBuf[EXT_SIMPLE_SQL_BUF_LEN] = {0};

    if (pEntry == NULL) {
      continue;
    }

    code = extBuildRelationalMetaSql(tblRef, prefilterBuf, tsCol, uid,
                                     extLookupUidMaxTs(pUidMaxTs, uid),
                                     sqlBuf, sizeof(sqlBuf));
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }

    ST_TASK_DLOG("ext: meta relational uid=%" PRIu64 " sql=\"%.120s\"", uid, sqlBuf);

    code = extQueryExecFetchAll(pInfo->pConn, sqlBuf, metaMappings, 4, &pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("ext: meta relational query uid=%" PRIu64 " code:%d", uid, code);
      break;
    }

    code = extAppendRelationalMetaRow(pTask, pBlock, pEntry->groupId, uid,
                                      pRsp->pMetaBlock, &totalFetched);
    blockDataDestroy(pBlock);
    pBlock = NULL;
    if (code != TSDB_CODE_SUCCESS || totalFetched >= STREAM_RETURN_ROWS_NUM) {
      break;
    }
  }

_exit:
  if (pBlock != NULL) {
    blockDataDestroy(pBlock);
  }
  return code;
}

/* ============================================================
 * Influx batch helpers — internal to handleMetaPullInflux.
 * ============================================================ */

/* Per-uid accumulator for one InfluxDB batch. */
typedef struct {
  int64_t skey;
  int64_t ekey;
  int64_t cnt;
  bool    seen;
} SInfluxUidAgg;

/* Build the UNION ALL SQL for a batch of uids starting at batchStart.
 *
 * Returns the number of uids *consumed* (advanced past) from batchStart, and
 * sets *pAnyUid to whether at least one uid contributed a WHERE clause.  The
 * returned count may be smaller than batchSize when a uid's clause would
 * overflow sqlBuf: that uid (and the rest of the batch) is left for the caller
 * to re-batch, so no uid is silently dropped and no out-of-bounds write occurs.
 * A return of batchSize means the whole batch was processed. */
static int32_t buildInfluxBatchSql(SStreamExtReaderInfo *pInfo,
                                   SArray                   *pAllUids,
                                   int32_t                   batchStart,
                                   int32_t                   batchSize,
                                   const char               *tsCol,
                                   const char               *prefilterBuf,
                                   int32_t                   nTags,
                                   SSHashObj                *pUidMaxTs,
                                   char                     *sqlBuf,
                                   int32_t                   sqlBufLen,
                                   bool                     *pAnyUid) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t off = 0;
  *pAnyUid = false;

  /* One UNION ALL arm per uid, each a standalone SELECT with its own WHERE.
   * DataFusion (InfluxDB 3.x) cannot plan a single query whose top-level OR
   * combines branches with different time-column bounds ("unable to analyze
   * provided filters for a boundary on the time column"); UNION ALL sidesteps
   * this because every arm is planned/pruned independently, while still
   * issuing a single round trip for the whole uid batch. */
  bool    anyUid = false;
  int32_t k      = 0;
  for (; k < batchSize; k++) {
    uint64_t        uid = *(uint64_t *)taosArrayGet(pAllUids, batchStart + k);
    SUidIndexEntry *ent = tSimpleHashGet(pInfo->pUidIndex, &uid, sizeof(uid));
    if (ent == NULL) continue;  /* missing index entry: consume, emit no clause */

    char tagWhere[EXT_TAG_WHERE_BUF_LEN] = {0};
    if (nTags > 0) {
      int32_t twLen = buildInfluxTagWhereClause(ent->tagsetKey,
                                                tagWhere, sizeof(tagWhere));
      if (twLen < 0) {
        ST_TASK_ELOG("ext: influx batch tag WHERE exceeds %d bytes uid=%" PRIu64,
                     (int32_t)sizeof(tagWhere), uid);
        return TSDB_CODE_OUT_OF_RANGE;
      }
    }

    /* Look up watermark from trigger-provided map; INT64_MIN means fetch all. */
    int64_t maxTs = extLookupUidMaxTs(pUidMaxTs, uid);

    /* Tentatively append this uid's SELECT arm; revert atomically if it overflows. */
    int32_t saveOff  = off;
    bool    clauseOk = true;
    if (anyUid) clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off, " UNION ALL ");
    if (clauseOk) clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off, "SELECT %s", tsCol);
    for (int32_t c = 0; clauseOk && c < nTags; c++) {
      const char *cn = (const char *)taosArrayGet(pInfo->pInfluxTagCols, c);
      clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off, ", %s", cn);
    }
    if (clauseOk) {
      clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off, " FROM %s WHERE ", pInfo->spec.extTable);
    }
    if (clauseOk && prefilterBuf[0] != '\0') {
      /* prefilterBuf already ends with " AND " */
      clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off, "%s", prefilterBuf);
    }
    if (clauseOk && tagWhere[0] != '\0') {
      clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off, "%s AND ", tagWhere);
    }
    /* DataFusion (InfluxDB 3.x): CAST(ns AS TIMESTAMP) treats the value as
     * seconds, causing overflow.  Use to_timestamp_nanos() which accepts
     * nanosecond integers directly without unit conversion. */
    if (clauseOk) {
      clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off,
                           "%s > to_timestamp_nanos(%" PRId64 ")", tsCol, maxTs);
    }

    if (!clauseOk) {
      off = saveOff;
      sqlBuf[saveOff] = '\0';
      ST_TASK_ELOG("ext: influx batch SQL exceeds %d bytes uid=%" PRIu64 " batchStart=%d batchSize=%d",
                   sqlBufLen, uid, batchStart, batchSize);
      return TSDB_CODE_OUT_OF_RANGE;
    }
    anyUid = true;
  }

  *pAnyUid = anyUid;
  return (k > 0) ? k : batchSize;  /* uids consumed this round (>=1 when batchSize>=1) */
}

/* Context passed to influxAccumBlockCb. */
typedef struct {
  SStreamExtReaderInfo *pInfo;
  SArray                   *pAllUids;
  int32_t                   batchStart;
  int32_t                   batchSize;
  int32_t                   nTags;
  SInfluxUidAgg            *pAgg;
} SInfluxAccumCtx;

static int32_t influxAccumBlockCb(SSDataBlock *pBlk, void *pCtx);

static int32_t extBuildInfluxMetaMappings(SStreamExtReaderInfo *pInfo, const char *tsCol,
                                          int32_t nTags, SExtColTypeMapping **ppMappings,
                                          int32_t *pCols) {
  *pCols = 1 + nTags;
  *ppMappings = taosMemoryCalloc(*pCols, sizeof(SExtColTypeMapping));
  if (*ppMappings == NULL) {
    return terrno;
  }

  (*ppMappings)[0].tdType.type = TSDB_DATA_TYPE_TIMESTAMP;
  (*ppMappings)[0].tdType.bytes = sizeof(int64_t);
  tstrncpy((*ppMappings)[0].colName, tsCol, sizeof((*ppMappings)[0].colName));
  for (int32_t c = 0; c < nTags; ++c) {
    (*ppMappings)[1 + c].tdType.type = TSDB_DATA_TYPE_NCHAR;
    (*ppMappings)[1 + c].tdType.bytes = EXT_INFLUX_TAG_NCHAR_CHARS;
    tstrncpy((*ppMappings)[1 + c].colName,
             (const char *)taosArrayGet(pInfo->pInfluxTagCols, c),
             sizeof((*ppMappings)[1 + c].colName));
  }

  return TSDB_CODE_SUCCESS;
}

static SInfluxUidAgg *extAllocInfluxUidAgg(int32_t consumed) {
  SInfluxUidAgg *pAgg = taosMemoryCalloc(consumed, sizeof(SInfluxUidAgg));
  if (pAgg == NULL) {
    return NULL;
  }

  for (int32_t k = 0; k < consumed; ++k) {
    pAgg[k].skey = INT64_MAX;
    pAgg[k].ekey = INT64_MIN;
  }

  return pAgg;
}

static int32_t extFlushInfluxMetaAgg(SStreamExtReaderInfo *pInfo, SArray *pAllUids,
                                     int32_t batchStart, int32_t consumed,
                                     SInfluxUidAgg *pAgg, SSTriggerExtPullRsp *pRsp,
                                     int32_t *pTotalFetched, int32_t batchIdx) {
  const SStreamTask *pTask = pInfo->pTask;
  for (int32_t k = 0; k < consumed; ++k) {
    if (!pAgg[k].seen) {
      continue;
    }

    uint64_t        uid = *(uint64_t *)taosArrayGet(pAllUids, batchStart + k);
    SUidIndexEntry *pEntry = tSimpleHashGet(pInfo->pUidIndex, &uid, sizeof(uid));
    if (pEntry == NULL) {
      continue;
    }

    int32_t code = metaBlockAppendRow(pRsp->pMetaBlock, pEntry->groupId,
                                      pAgg[k].skey, pAgg[k].ekey, uid, pAgg[k].cnt);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }

    *pTotalFetched += (int32_t)pAgg[k].cnt;
    if (*pTotalFetched >= STREAM_RETURN_ROWS_NUM) {
      ST_TASK_DLOG("ext: influx meta batch %d hit row threshold uid=%" PRIu64, batchIdx, uid);
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t extRunInfluxMetaBatch(SStreamExtReaderInfo *pInfo, SArray *pAllUids,
                                     int32_t batchStart, int32_t batchSize, int32_t batchIdx,
                                     const char *tsCol, const char *prefilterBuf, int32_t nTags,
                                     SSHashObj *pUidMaxTs, SExtColTypeMapping *pMappings, int32_t nCols,
                                     SSTriggerExtPullRsp *pRsp, int32_t *pConsumed,
                                     int32_t *pTotalFetched) {
  const SStreamTask *pTask = pInfo->pTask;
  char    sqlBuf[EXT_BATCH_SQL_BUF_LEN] = {0};
  bool    anyUid = false;
  int32_t consumed = buildInfluxBatchSql(pInfo, pAllUids, batchStart, batchSize,
                                         tsCol, prefilterBuf, nTags, pUidMaxTs,
                                         sqlBuf, sizeof(sqlBuf), &anyUid);
  if (consumed < 0) {
    return consumed;
  }
  if (consumed == 0) {
    consumed = batchSize;
  }
  *pConsumed = consumed;

  if (!anyUid) {
    ST_TASK_DLOG("ext: influx meta batch %d: all uids missing, skip", batchIdx);
    return TSDB_CODE_SUCCESS;
  }

  ST_TASK_DLOG("ext: influx meta batch sql=\"%.200s\"", sqlBuf);
  SInfluxUidAgg *pAgg = extAllocInfluxUidAgg(consumed);
  if (pAgg == NULL) {
    return terrno;
  }

  SInfluxAccumCtx aCtx = {
    .pInfo = pInfo,
    .pAllUids = pAllUids,
    .batchStart = batchStart,
    .batchSize = consumed,
    .nTags = nTags,
    .pAgg = pAgg,
  };

  int32_t code = extQueryExecForEach(pInfo->pConn, sqlBuf, pMappings, nCols,
                                     influxAccumBlockCb, &aCtx);
  if (code == TSDB_CODE_SUCCESS) {
    code = extFlushInfluxMetaAgg(pInfo, pAllUids, batchStart, consumed, pAgg, pRsp,
                                 pTotalFetched, batchIdx);
  } else {
    ST_TASK_ELOG("ext: influx meta batch %d query failed code:%d", batchIdx, code);
  }

  taosMemoryFree(pAgg);
  return code;
}

static int32_t extExtractInfluxAccumTagValues(SInfluxAccumCtx *pACtx, SSDataBlock *pBlk,
                                              int32_t row, char *pValBufs,
                                              const char **colNames, const char **colVals) {
  SStreamExtReaderInfo *pInfo = pACtx->pInfo;
  const SStreamTask    *pTask = pInfo->pTask;

  for (int32_t c = 0; c < pACtx->nTags; ++c) {
    char *pValBuf = pValBufs + (size_t)c * EXT_INFLUX_TAG_NCHAR_CHARS;
    SColumnInfoData *pTagCol = taosArrayGet(pBlk->pDataBlock, 1 + c);

    colNames[c] = (const char *)taosArrayGet(pInfo->pInfluxTagCols, c);
    if (pTagCol != NULL && !colDataIsNull_s(pTagCol, row)) {
      int32_t code = extColCellToStr(pTagCol, row, pValBuf, EXT_INFLUX_TAG_NCHAR_CHARS);
      if (code < 0) {
        ST_TASK_ELOG("ext: accumulated tag value conversion failed row:%d tag:%d code:%d",
                     row, c, code);
        return code;
      }
      colVals[c] = pValBuf;
      continue;
    }

    pValBuf[0] = '\0';
    colVals[c] = "";
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t extResolveInfluxAccumSlot(SInfluxAccumCtx *pACtx, const char **colNames,
                                         const char **colVals, int32_t *pSlot) {
  SStreamExtReaderInfo *pInfo = pACtx->pInfo;

  *pSlot = -1;
  if (pACtx->nTags == 0) {
    *pSlot = 0;
    return TSDB_CODE_SUCCESS;
  }

  char tagsetKey[EXT_TAGSET_KEY_MAX] = {0};
  int32_t keyLen = buildTagsetKey(tagsetKey, sizeof(tagsetKey), colNames, colVals, pACtx->nTags);
  if (keyLen < 0) {
    return keyLen;
  }

  uint64_t *pUid = (uint64_t *)tSimpleHashGet(pInfo->pTagsetIndex, tagsetKey, keyLen);
  if (pUid == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  for (int32_t k = 0; k < pACtx->batchSize; ++k) {
    if (*(uint64_t *)taosArrayGet(pACtx->pAllUids, pACtx->batchStart + k) == *pUid) {
      *pSlot = k;
      break;
    }
  }

  return TSDB_CODE_SUCCESS;
}

/* Per-block callback: accumulate ts min/max/cnt into pAgg slots. */
static int32_t influxAccumBlockCb(SSDataBlock *pBlk, void *pCtx) {
  SInfluxAccumCtx *pACtx = (SInfluxAccumCtx *)pCtx;
  const SStreamTask *pTask = pACtx->pInfo->pTask;
  int32_t code = TSDB_CODE_SUCCESS;
  char *pValBufs = NULL;

  ST_TASK_DLOG("ext: influxAccumBlockCb rows=%" PRId64, pBlk != NULL ? pBlk->info.rows : (int64_t)-1);

  if (pACtx->nTags > TSDB_MAX_TAGS) {
    ST_TASK_ELOG("ext: accumulated tag count exceeds limit count:%d limit:%d",
                 pACtx->nTags, TSDB_MAX_TAGS);
    code = TSDB_CODE_OUT_OF_RANGE;
    goto _exit;
  }

  if (pACtx->nTags > 0) {
    pValBufs = taosMemoryCalloc(pACtx->nTags, EXT_INFLUX_TAG_NCHAR_CHARS);
    if (pValBufs == NULL) {
      code = terrno;
      ST_TASK_ELOG("ext: allocate accumulated tag value buffers failed count:%d bytes:%zu code:%d",
                   pACtx->nTags, (size_t)EXT_INFLUX_TAG_NCHAR_CHARS, code);
      goto _exit;
    }
  }

  for (int32_t r = 0; r < pBlk->info.rows; ++r) {
    SColumnInfoData *pTsCol = taosArrayGet(pBlk->pDataBlock, 0);
    const char *colNames[TSDB_MAX_TAGS] = {0};
    const char *colVals[TSDB_MAX_TAGS] = {0};
    int32_t slot = -1;

    if (pTsCol == NULL || colDataIsNull_s(pTsCol, r)) {
      continue;
    }

    code = extExtractInfluxAccumTagValues(pACtx, pBlk, r, pValBufs, colNames, colVals);
    if (code != TSDB_CODE_SUCCESS) {
      goto _exit;
    }

    code = extResolveInfluxAccumSlot(pACtx, colNames, colVals, &slot);
    if (code != TSDB_CODE_SUCCESS) {
      if (code < 0) {
        goto _exit;
      }
      continue;
    }
    if (slot < 0) {
      continue;
    }

    int64_t ts = *(int64_t *)colDataGetData(pTsCol, r);
    if (ts < pACtx->pAgg[slot].skey) {
      pACtx->pAgg[slot].skey = ts;
    }
    if (ts > pACtx->pAgg[slot].ekey) {
      pACtx->pAgg[slot].ekey = ts;
    }
    pACtx->pAgg[slot].cnt++;
    pACtx->pAgg[slot].seen = true;
  }

_exit:
  taosMemoryFree(pValBufs);
  blockDataDestroy(pBlk);
  return code;
}

/* ============================================================
 * handleMetaPullInflux — InfluxDB meta pull (N=64 uid batch loop).
 *
 * One query per batch of N uids using a UNION ALL of per-uid SELECTs
 * (DS §6.1.5); UNION ALL (rather than a top-level OR) is required because
 * DataFusion (InfluxDB 3.x) cannot plan an OR whose branches carry different
 * time-column bounds, see buildInfluxBatchSql.
 * Rows returned by the connector are client-aggregated into per-uid
 * min/max ts and row count, then appended to pRsp->pMetaBlock.
 * ============================================================ */
static int32_t handleMetaPullInflux(SStreamExtReaderInfo *pInfo,
                                    SArray                   *pAllUids,
                                    int32_t                   totalUids,
                                    const char               *tsCol,
                                    SSHashObj                *pUidMaxTs,
                                    SSTriggerExtPullRsp      *pRsp) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t code         = TSDB_CODE_SUCCESS;
  int32_t totalFetched = 0;
  int32_t nTags        = pInfo->pInfluxTagCols
                           ? (int32_t)taosArrayGetSize(pInfo->pInfluxTagCols) : 0;
  int32_t batchStart   = pInfo->influxBatchOffset;
  int32_t batchIdx     = 0;
  int32_t nCols        = 0;
  SExtColTypeMapping *pMappings = NULL;

  /* Trigger reader uses PRE_FILTER (triggerPrefilter), not the calc WHERE.
   * Build once; invariant across all uid batches. */
  char prefilterBuf[EXT_SQL_CLAUSE_BUF_LEN] = {0};
  code = buildTriggerPrefilterClause(&pInfo->spec, prefilterBuf, sizeof(prefilterBuf));
  if (code < 0) goto _done;

  code = extBuildInfluxMetaMappings(pInfo, tsCol, nTags, &pMappings, &nCols);
  if (code != TSDB_CODE_SUCCESS) goto _done;

  while (batchStart < totalUids) {
    int32_t batchEnd  = TMIN(batchStart + EXT_INFLUX_UID_BATCH_SIZE, totalUids);
    int32_t batchSize = batchEnd - batchStart;
    int32_t consumed = batchSize;
    ST_TASK_DLOG("ext: influx meta batch %d uids=%d [%d..%d)",
            batchIdx, batchSize, batchStart, batchEnd);

    code = extRunInfluxMetaBatch(pInfo, pAllUids, batchStart, batchSize, batchIdx, tsCol,
                                 prefilterBuf, nTags, pUidMaxTs, pMappings, nCols, pRsp,
                                 &consumed, &totalFetched);
    if (code != TSDB_CODE_SUCCESS || totalFetched >= STREAM_RETURN_ROWS_NUM) break;

    batchStart += consumed;
    batchIdx++;
  }

_done:
  taosMemoryFree(pMappings);

  /* Advance or reset the batch offset for the next PULL. */
  bool hitThreshold = (totalFetched >= STREAM_RETURN_ROWS_NUM);
  pInfo->influxBatchOffset = hitThreshold ? batchStart : 0;
  ST_TASK_DLOG("ext: influx meta done code=%d batchOffset=%d hitThreshold=%d",
          code, pInfo->influxBatchOffset, (int)hitThreshold);
  return code;
}

static int32_t extAppendInfluxLastTsBatch(SStreamExtReaderInfo *pInfo, SArray *pAllUids,
                                          int32_t batchStart, int32_t consumed,
                                          SInfluxUidAgg *pAgg, SArray *pLastTsArr) {
  for (int32_t k = 0; k < consumed; ++k) {
    uint64_t uid = *(uint64_t *)taosArrayGet(pAllUids, batchStart + k);
    SUidIndexEntry *pEntry = tSimpleHashGet(pInfo->pUidIndex, &uid, sizeof(uid));
    if (pEntry == NULL) {
      continue;
    }

    SExtLastTsInfo info = {
      .uid = (int64_t)uid,
      .gid = (int64_t)pEntry->groupId,
      .ts = pAgg[k].seen ? pAgg[k].ekey : INT64_MIN,
    };
    taosArrayPush(pLastTsArr, &info);
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t extRunInfluxLastTsBatch(SStreamExtReaderInfo *pInfo, SArray *pAllUids,
                                       int32_t batchStart, int32_t batchSize, int32_t batchIdx,
                                       const char *tsCol, const char *prefilterBuf, int32_t nTags,
                                       SExtColTypeMapping *pMappings, int32_t nCols,
                                       SArray *pLastTsArr, int32_t *pConsumed) {
  const SStreamTask *pTask = pInfo->pTask;
  char sqlBuf[EXT_BATCH_SQL_BUF_LEN] = {0};
  bool anyUid = false;
  SInfluxUidAgg *pAgg = NULL;
  int32_t code = TSDB_CODE_SUCCESS;

  *pConsumed = buildInfluxBatchSql(pInfo, pAllUids, batchStart, batchSize,
                                   tsCol, prefilterBuf, nTags, NULL,
                                   sqlBuf, sizeof(sqlBuf), &anyUid);
  if (*pConsumed < 0) {
    return *pConsumed;
  }
  if (*pConsumed == 0) {
    *pConsumed = batchSize;
  }

  pAgg = extAllocInfluxUidAgg(*pConsumed);
  if (pAgg == NULL) {
    return terrno;
  }

  if (anyUid) {
    ST_TASK_DLOG("ext: influx lastTs batch sql=\"%.200s\"", sqlBuf);
    SInfluxAccumCtx aCtx = {
      .pInfo = pInfo,
      .pAllUids = pAllUids,
      .batchStart = batchStart,
      .batchSize = *pConsumed,
      .nTags = nTags,
      .pAgg = pAgg,
    };
    code = extQueryExecForEach(pInfo->pConn, sqlBuf, pMappings, nCols,
                               influxAccumBlockCb, &aCtx);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("ext: influx lastTs batch %d query failed code:%d", batchIdx, code);
      goto _exit;
    }
  } else {
    ST_TASK_DLOG("ext: influx lastTs batch %d: all uids missing index entry", batchIdx);
  }

  code = extAppendInfluxLastTsBatch(pInfo, pAllUids, batchStart, *pConsumed, pAgg, pLastTsArr);

_exit:
  taosMemoryFree(pAgg);
  return code;
}

/* ============================================================
 * handleLastTsPullInflux — InfluxDB LAST_TS pull (N=64 uid batch loop).
 *
 * Computes MAX(ts) PER UID (not one global value applied to every uid),
 * using each uid's own tagset as the WHERE filter — mirrors
 * handleMetaPullInflux's OR-batched fetch (buildInfluxBatchSql /
 * influxAccumBlockCb), reusing the same per-uid MIN/MAX/COUNT accumulator
 * and keeping only the MAX (ekey).
 *
 * No watermark filtering (pUidMaxTs == NULL passed to buildInfluxBatchSql):
 * LAST_TS establishes the initial watermark, so it must see the true max
 * across all history for every uid, not rows-since-some-prior-watermark.
 * A uid with zero matching rows gets ts=INT64_MIN ("no watermark yet"),
 * mirroring handleLastTsPullRelational's no-rows fallback.
 * ============================================================ */
static int32_t handleLastTsPullInflux(SStreamExtReaderInfo *pInfo,
                                      SArray *pAllUids, int32_t totalUids,
                                      const char *tsCol, SArray *pLastTsArr) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t nTags = pInfo->pInfluxTagCols
                    ? (int32_t)taosArrayGetSize(pInfo->pInfluxTagCols) : 0;
  int32_t batchStart = 0;
  int32_t batchIdx = 0;
  int32_t nCols = 0;
  SExtColTypeMapping *pMappings = NULL;
  char prefilterBuf[EXT_SQL_CLAUSE_BUF_LEN] = {0};

  code = buildTriggerPrefilterClause(&pInfo->spec, prefilterBuf, sizeof(prefilterBuf));
  if (code < 0) {
    goto _exit;
  }

  code = extBuildInfluxMetaMappings(pInfo, tsCol, nTags, &pMappings, &nCols);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  while (batchStart < totalUids) {
    int32_t batchEnd = TMIN(batchStart + EXT_INFLUX_UID_BATCH_SIZE, totalUids);
    int32_t batchSize = batchEnd - batchStart;
    int32_t consumed = batchSize;

    ST_TASK_DLOG("ext: influx lastTs batch %d uids=%d [%d..%d)",
                 batchIdx, batchSize, batchStart, batchEnd);

    code = extRunInfluxLastTsBatch(pInfo, pAllUids, batchStart, batchSize, batchIdx,
                                   tsCol, prefilterBuf, nTags, pMappings, nCols,
                                   pLastTsArr, &consumed);
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }

    batchStart += consumed;
    batchIdx++;
  }

_exit:
  taosMemoryFree(pMappings);
  return code;
}

/* ============================================================
 * Handler: STRIGGER_PULL_META_EXT
 *
 * Dispatches to handleMetaPullRelational (MySQL/PG) or
 * handleMetaPullInflux (InfluxDB) based on source type.
 *
 * Response: pRsp->pMetaBlock (DS §6.2.4).
 * Also updates pUidIndex maxTs for each uid seen.
 * ============================================================ */
static int32_t handleMetaPull(SStreamExtReaderInfo *pInfo,
                              const SSTriggerExtPullReq *pReq,
                              SSTriggerExtPullRsp *pRsp) {
  const SStreamTask *pTask = pInfo->pTask;
  ST_TASK_DLOG("ext: handleMetaPull enter source=%s", pInfo->spec.sourceName);

  bool        isInflux = ((EExtSourceType)pInfo->spec.sourceType == EXT_SOURCE_INFLUXDB);
  const char *tsCol    = (pInfo->spec.tsColumn[0] != '\0')
                           ? pInfo->spec.tsColumn : "ts";

  SArray *pAllUids = collectAllUids(pInfo->pUidIndex);
  if (pAllUids == NULL) {
    ST_TASK_ELOG("%s", "ext: handleMetaPull collectAllUids OOM");
    return terrno;
  }
  int32_t totalUids = (int32_t)taosArrayGetSize(pAllUids);
  ST_TASK_DLOG("ext: handleMetaPull totalUids=%d isInflux=%d", totalUids, (int)isInflux);

  pRsp->pMetaBlock = createMetaBlock();
  if (pRsp->pMetaBlock == NULL) {
    taosArrayDestroy(pAllUids);
    return terrno;
  }

  /* Pass the trigger watermark map directly to sub-handlers; they look up
   * uid->maxTs on demand without copying into pUidIndex. */
  SSHashObj *pUidMaxTs = (pReq != NULL) ? pReq->pUidMaxTs : NULL;
  int32_t code = isInflux
      ? handleMetaPullInflux(pInfo, pAllUids, totalUids, tsCol, pUidMaxTs, pRsp)
      : handleMetaPullRelational(pInfo, pAllUids, totalUids, tsCol, pUidMaxTs, pRsp);

  taosArrayDestroy(pAllUids);
  int64_t metaRows = (pRsp->pMetaBlock != NULL) ? pRsp->pMetaBlock->info.rows : 0;
  ST_TASK_DLOG("ext: handleMetaPull done code=%d metaRows=%" PRId64, code, metaRows);
  printDataBlock(pRsp->pMetaBlock, __func__, "ext_meta", pTask->streamId);

  /* Mirror vnodeProcessStreamWalMetaDataNewReq: return NO_DATA when empty so
   * the trigger side can distinguish "nothing new" from a real error. */
  if (code == TSDB_CODE_SUCCESS && metaRows == 0) {
    ST_TASK_DLOG("%s", "ext: handleMetaPull no rows -> TSDB_CODE_STREAM_NO_DATA");
    code = TSDB_CODE_STREAM_NO_DATA;
  }
  return code;
}

/* ============================================================
 * Helper: build and execute a data-fetch SQL, returning an SSDataBlock.
 * Used by handleDataPull, handleMetaDataPull, handleCalcDataPull.
 *
 * colList: comma-separated column list string (e.g. "ts,col1,col2")
 * uid:     the uid whose tagsetKey (stored in pUidIndex) is used to
 *          build the tag filter for InfluxDB; ignored for MySQL/PG
 *          (single-group, no per-uid filter needed)
 * skey/ekey: time window [skey, ekey] inclusive
 * isInflux:  if true, apply tag-based WHERE filter from ent->tagsetKey
 * pMappings/nMappings: column type mappings to pass to extConnectorFetchBlock.
 *   When NULL/0, falls back to pInfo->spec.pColMappings (trigger col mappings).
 * ============================================================ */
static int32_t extBuildRelationalFetchSql(const char *colList, const char *tblRef,
                                          const char *prefilterBuf, const char *tsCol,
                                          uint64_t uid, int64_t skey, int64_t ekey,
                                          char *sqlBuf, int32_t sqlBufLen) {
  char skeyDt[32] = {0};
  char ekeyDt[32] = {0};
  epochToDatetimeStr(skey, TSDB_TIME_PRECISION_MICRO, skeyDt, sizeof(skeyDt));
  epochToDatetimeStr(ekey, TSDB_TIME_PRECISION_MICRO, ekeyDt, sizeof(ekeyDt));

  int32_t sqlLen = snprintf(sqlBuf, sqlBufLen,
                            "SELECT %s FROM %s WHERE %s%s >= %s AND %s <= %s",
                            colList, tblRef, prefilterBuf, tsCol, skeyDt, tsCol, ekeyDt);
  if (sqlLen < 0 || sqlLen >= sqlBufLen) {
    stError("ext: fetchDataForUid relational SQL exceeds %d bytes uid=%" PRIu64,
            sqlBufLen, uid);
    return TSDB_CODE_OUT_OF_RANGE;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t extBuildInfluxFetchSql(SStreamExtReaderInfo *pInfo, const char *colList,
                                      const char *tblRef, const char *prefilterBuf,
                                      const char *tsCol, uint64_t uid,
                                      int64_t skey, int64_t ekey,
                                      char *sqlBuf, int32_t sqlBufLen) {
  const SStreamTask *pTask = pInfo->pTask;
  SUidIndexEntry *pEntry = tSimpleHashGet(pInfo->pUidIndex, &uid, sizeof(uid));
  char tagWhere[EXT_TAG_WHERE_BUF_LEN] = {0};

  if (pEntry != NULL && pEntry->tagsetKey[0] != '\0') {
    int32_t tagCode = buildInfluxTagWhereClause(pEntry->tagsetKey, tagWhere, sizeof(tagWhere));
    if (tagCode < 0) {
      ST_TASK_ELOG("ext: fetchDataForUid tag WHERE exceeds %d bytes uid=%" PRIu64,
                   (int32_t)sizeof(tagWhere), uid);
      return TSDB_CODE_OUT_OF_RANGE;
    }
  }

  const char *wherePrefix = prefilterBuf;
  const char *tagClause = tagWhere;
  const char *timeSep = (tagClause[0] != '\0') ? " AND " : "";
  int32_t sqlLen = snprintf(sqlBuf, sqlBufLen,
                            "SELECT %s FROM %s WHERE %s%s%s%s >= to_timestamp_nanos(%" PRId64 ")"
                            " AND %s <= to_timestamp_nanos(%" PRId64 ")",
                            colList, tblRef,
                            wherePrefix, tagClause, timeSep,
                            tsCol, skey, tsCol, ekey);
  if (sqlLen < 0 || sqlLen >= sqlBufLen) {
    ST_TASK_ELOG("ext: fetchDataForUid influx SQL exceeds %d bytes uid=%" PRIu64,
                 sqlBufLen, uid);
    return TSDB_CODE_OUT_OF_RANGE;
  }

  return TSDB_CODE_SUCCESS;
}

static void extSelectFetchMappings(const SExtColTypeMapping *pMappings, int32_t nMappings,
                                   const SExtColTypeMapping **ppEffMappings,
                                   int32_t *pEffMappings) {
  *ppEffMappings = (pMappings != NULL && nMappings > 0) ? pMappings : NULL;
  *pEffMappings = (pMappings != NULL && nMappings > 0) ? nMappings : 0;
}

/* prefilterBuf must already be built by the caller:
 *   handleDataPull     → buildTriggerPrefilterClause (trigger data, PRE_FILTER)
 *   handleCalcDataPull → buildTriggerPrefilterClause (trigger-side data for calc, PRE_FILTER) */
static int32_t fetchDataForUid(SStreamExtReaderInfo *pInfo,
                               const char *colList,
                               uint64_t uid, int64_t skey, int64_t ekey,
                               bool isInflux,
                               const char *prefilterBuf,
                               const SExtColTypeMapping *pMappings,
                               int32_t nMappings,
                               SSDataBlock **ppBlock) {
  const SStreamTask *pTask = pInfo->pTask;
  char sqlBuf[EXT_DATA_SQL_BUF_LEN] = {0};
  char tblRef[TSDB_TABLE_NAME_LEN + TSDB_DB_NAME_LEN + 2] = {0};
  const char *tsCol = (pInfo->spec.tsColumn[0] != '\0')
                        ? pInfo->spec.tsColumn : "ts";
  const SExtColTypeMapping *pEffMappings = NULL;
  int32_t nEffMappings = 0;
  int32_t code = buildExtTableRef(&pInfo->spec, tblRef, sizeof(tblRef));
  if (code < 0) {
    return code;
  }

  code = isInflux
           ? extBuildInfluxFetchSql(pInfo, colList, tblRef, prefilterBuf, tsCol,
                                    uid, skey, ekey, sqlBuf, sizeof(sqlBuf))
           : extBuildRelationalFetchSql(colList, tblRef, prefilterBuf, tsCol,
                                        uid, skey, ekey, sqlBuf, sizeof(sqlBuf));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  ST_TASK_DLOG("ext: fetchDataForUid uid=%" PRIu64 " sql=\"%.180s\"", uid, sqlBuf);
  extSelectFetchMappings(pMappings, nMappings, &pEffMappings, &nEffMappings);
  return extQueryExecFetchAll(pInfo->pConn, sqlBuf,
                              (SExtColTypeMapping *)pEffMappings,
                              nEffMappings,
                              ppBlock);
}

/* ============================================================
 * Build trigger-column list string from the spec.
 * Returns the number of columns found (0 if triggerColumns is empty).
 * ============================================================ */
static int32_t buildTriggerColList(const SStreamExtTriggerSpec *pSpec,
                                   char *buf, int32_t bufLen) {
  if (pSpec->triggerColumns == NULL ||
      taosArrayGetSize(pSpec->triggerColumns) == 0) {
    /* Fallback: select all columns. */
    snprintf(buf, bufLen, "*");
    return 0;
  }
  int32_t off = 0;
  int32_t n   = (int32_t)taosArrayGetSize(pSpec->triggerColumns);
  for (int32_t i = 0; i < n; i++) {
    const char *colName = (const char *)taosArrayGet(pSpec->triggerColumns, i);
    if (!extSqlCat(buf, bufLen, &off, "%s%s", colName, (i < n - 1) ? "," : "")) {
      stError("ext: triggerColumns list exceeds %d bytes at col %d/%d", bufLen, i, n);
      return TSDB_CODE_OUT_OF_RANGE;
    }
  }
  return n;
}

/* ============================================================
 * Build calc-column list string from the spec.
 * calcColumns holds the aggregate-input columns (e.g. val for SUM(val)).
 * Falls back to SELECT * (NOT triggerColumns) when calcColumns is empty, because
 * triggerColumns only contains the trigger window column (ts) which is insufficient
 * for aggregate calculations that need measure columns (val, etc.).
 * Returns the number of columns found (0 when falling back to SELECT *).
 * ============================================================ */
static int32_t buildCalcColList(const SStreamExtTriggerSpec *pSpec,
                                char *buf, int32_t bufLen) {
  if (pSpec->calcColumns == NULL ||
      taosArrayGetSize(pSpec->calcColumns) == 0) {
    /* No calc columns available yet — fetch all columns so the aggregate
     * operator can find both ts and measure columns in the result block. */
    snprintf(buf, bufLen, "*");
    return 0;
  }
  int32_t off = 0;
  int32_t n   = (int32_t)taosArrayGetSize(pSpec->calcColumns);
  for (int32_t i = 0; i < n; i++) {
    const char *colName = (const char *)taosArrayGet(pSpec->calcColumns, i);
    if (!extSqlCat(buf, bufLen, &off, "%s%s", colName, (i < n - 1) ? "," : "")) {
      stError("ext: calcColumns list exceeds %d bytes at col %d/%d", bufLen, i, n);
      return TSDB_CODE_OUT_OF_RANGE;
    }
  }
  return n;
}

/* ============================================================
 * Build indexHash and dataBlock from a list of per-uid blocks.
 * pUidBlocks: SArray<struct{uint64_t uid; SSDataBlock* pBlock}>
 * ============================================================ */
typedef struct {
  uint64_t      uid;
  SSDataBlock *pBlock;
} SUidBlockPair;

static int32_t buildDataBlockAndIndex(SArray *pUidBlocks,
                                      SSDataBlock **ppDataBlock,
                                      SSHashObj **ppIndexHash) {
  int32_t code = TSDB_CODE_SUCCESS;

  *ppIndexHash = tSimpleHashInit(taosArrayGetSize(pUidBlocks),
                                 taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (*ppIndexHash == NULL) return terrno;

  /* Merge all per-uid blocks into a single dataBlock and keep uid -> row range
   * entries aligned with the merged block. */
  *ppDataBlock = NULL;
  int32_t globalRow = 0;

  for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pUidBlocks); i++) {
    SUidBlockPair *pair = taosArrayGet(pUidBlocks, i);
    if (pair->pBlock == NULL || pair->pBlock->info.rows == 0) continue;

    int32_t rowsForUid = pair->pBlock->info.rows;
    SExtIndexEntry entry = {
      .startRow = globalRow,
      .rowCount = rowsForUid,
    };
    code = tSimpleHashPut(*ppIndexHash, &pair->uid, sizeof(pair->uid),
                          &entry, sizeof(entry));
    if (code != TSDB_CODE_SUCCESS) return code;

    if (*ppDataBlock == NULL) {
      *ppDataBlock = pair->pBlock;
      pair->pBlock = NULL;  /* transfer ownership */
    } else {
      code = blockDataMerge(*ppDataBlock, pair->pBlock);
      if (code != TSDB_CODE_SUCCESS) return code;
      blockDataDestroy(pair->pBlock);
      pair->pBlock = NULL;
    }
    globalRow += rowsForUid;
  }

  stDebug("ext: buildDataBlockAndIndex done globalRow=%d indexEntries=%d",
          globalRow, tSimpleHashGetSize(*ppIndexHash));
  return TSDB_CODE_SUCCESS;
}

/* ============================================================
 * Batched InfluxDB data fetch (perf, DS §6.1.5).
 *
 * Instead of one external query per uid, issue one UNION ALL query per batch
 * of up to EXT_INFLUX_UID_BATCH_SIZE uids and demultiplex the returned rows back
 * to per-uid blocks by tagset — mirroring the META batch loop.  Used only for
 * InfluxDB sources that PARTITION BY tag (nTags > 0) and have an explicit column
 * list + matching type mappings, so per-uid result blocks can be built with the
 * correct schema.  All other cases keep the per-uid path (fetchDataForUid).
 *
 * The SELECT appends the nTags tag columns after the nData data columns so each
 * returned row maps to its uid; only the first nData columns are kept in the
 * per-uid output blocks.  On success, appends {uid, SSDataBlock*} pairs to
 * pUidBlocks (ownership moves to pUidBlocks).
 * ============================================================ */
typedef struct {
  SStreamExtReaderInfo *pInfo;
  int32_t               nData;   /* number of data (output) columns (== nMappings) */
  int32_t               nTags;   /* tag columns appended after the data columns */
  SSHashObj            *pUidBlk; /* hash<uint64_t uid, SSDataBlock*>; owns the blocks */
  int32_t               code;    /* sticky error captured inside the callback */
} SInfluxDataDemuxCtx;

/* Append data columns [0, nData) of source row r into the per-uid block,
 * creating the block (schema cloned from the source) on first use. */
static int32_t influxDemuxAppendRow(SInfluxDataDemuxCtx *pCtx, SSDataBlock *pSrc,
                                    int32_t r, uint64_t uid) {
  SSDataBlock **ppDst = (SSDataBlock **)tSimpleHashGet(pCtx->pUidBlk, &uid, sizeof(uid));
  SSDataBlock  *pDst  = (ppDst != NULL) ? *ppDst : NULL;

  if (pDst == NULL) {
    /* Bare block whose first nData columns mirror the source schema. */
    pDst = taosMemoryCalloc(1, sizeof(SSDataBlock));
    if (pDst == NULL) return terrno;
    pDst->pDataBlock = taosArrayInit(pCtx->nData, sizeof(SColumnInfoData));
    if (pDst->pDataBlock == NULL) { taosMemoryFree(pDst); return terrno; }
    for (int32_t c = 0; c < pCtx->nData; c++) {
      SColumnInfoData *pSrcCol = taosArrayGet(pSrc->pDataBlock, c);
      SColumnInfoData  col     = {0};
      col.info = pSrcCol->info; /* type/bytes/colId; data pointers stay NULL */
      if (taosArrayPush(pDst->pDataBlock, &col) == NULL) {
        taosArrayDestroy(pDst->pDataBlock);
        taosMemoryFree(pDst);
        return terrno;
      }
    }
    int32_t code = tSimpleHashPut(pCtx->pUidBlk, &uid, sizeof(uid), &pDst, sizeof(pDst));
    if (code != TSDB_CODE_SUCCESS) { blockDataDestroy(pDst); return code; }
  }

  int32_t row  = pDst->info.rows;
  int32_t code = blockDataEnsureCapacity(pDst, row + 1);
  if (code != TSDB_CODE_SUCCESS) return code;
  for (int32_t c = 0; c < pCtx->nData; c++) {
    SColumnInfoData *pSrcCol = taosArrayGet(pSrc->pDataBlock, c);
    SColumnInfoData *pDstCol = taosArrayGet(pDst->pDataBlock, c);
    bool        isNull = colDataIsNull_s(pSrcCol, r);
    const char *pData  = isNull ? NULL : colDataGetData(pSrcCol, r);
    code = colDataSetVal(pDstCol, row, pData, isNull);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  pDst->info.rows = row + 1;
  return TSDB_CODE_SUCCESS;
}

/* Per-result-block callback: resolve uid from the trailing tag columns and
 * append the row's data columns to that uid's block. */
static int32_t influxDataDemuxBlockCb(SSDataBlock *pBlk, void *pCtxArg) {
  SInfluxDataDemuxCtx  *pCtx  = (SInfluxDataDemuxCtx *)pCtxArg;
  SStreamExtReaderInfo *pInfo = pCtx->pInfo;
  const SStreamTask     *pTask = pInfo->pTask;

  if (pCtx->nTags > TSDB_MAX_TAGS) {
    pCtx->code = TSDB_CODE_OUT_OF_RANGE;
    ST_TASK_ELOG("ext: demux tag count exceeds limit count:%d limit:%d", pCtx->nTags, TSDB_MAX_TAGS);
    blockDataDestroy(pBlk);
    return pCtx->code;
  }

  int32_t nt = pCtx->nTags;
  char   *pValBufs = NULL;
  if (nt > 0) {
    pValBufs = taosMemoryCalloc(nt, EXT_INFLUX_TAG_NCHAR_CHARS);
    if (pValBufs == NULL) {
      pCtx->code = terrno;
      ST_TASK_ELOG("ext: allocate demux tag value buffers failed count:%d bytes:%zu code:%d",
                   nt, (size_t)EXT_INFLUX_TAG_NCHAR_CHARS, pCtx->code);
      blockDataDestroy(pBlk);
      return pCtx->code;
    }
  }

  for (int32_t r = 0; r < pBlk->info.rows && pCtx->code == TSDB_CODE_SUCCESS; r++) {
    char        tagsetKey[EXT_TAGSET_KEY_MAX] = {0};
    const char *colNames[TSDB_MAX_TAGS]                  = {0};
    const char *colVals[TSDB_MAX_TAGS]                   = {0};

    for (int32_t c = 0; c < nt; c++) {
      char *pValBuf = pValBufs + (size_t)c * EXT_INFLUX_TAG_NCHAR_CHARS;
      colNames[c] = (const char *)taosArrayGet(pInfo->pInfluxTagCols, c);
      SColumnInfoData *pTagCol = taosArrayGet(pBlk->pDataBlock, pCtx->nData + c);
      if (pTagCol && !colDataIsNull_s(pTagCol, r)) {
        int32_t tagCode = extColCellToStr(pTagCol, r, pValBuf, EXT_INFLUX_TAG_NCHAR_CHARS);
        if (tagCode < 0) {
          pCtx->code = tagCode;
          ST_TASK_ELOG("ext: demux tag value conversion failed row:%d tag:%d code:%d", r, c, pCtx->code);
          taosMemoryFree(pValBufs);
          blockDataDestroy(pBlk);
          return pCtx->code;
        }
        colVals[c] = pValBuf;
      } else {
        pValBuf[0] = '\0';
        colVals[c]    = "";
      }
    }

    int32_t keyLen = buildTagsetKey(tagsetKey, sizeof(tagsetKey), colNames, colVals, nt);
    if (keyLen < 0) continue;
    int64_t *pUid = (int64_t *)tSimpleHashGet(pInfo->pTagsetIndex, tagsetKey, keyLen);
    if (pUid == NULL) continue; /* row's tagset not in this stream's partition set */

    pCtx->code = influxDemuxAppendRow(pCtx, pBlk, r, *pUid);
  }

  taosMemoryFree(pValBufs);
  blockDataDestroy(pBlk);
  return pCtx->code;
}

static int32_t extSnapshotUidWindows(SSHashObj *pUidWindow, SArray **ppUids, SArray **ppWins) {
  int32_t nUids = tSimpleHashGetSize(pUidWindow);
  *ppUids = taosArrayInit(nUids, sizeof(uint64_t));
  *ppWins = taosArrayInit(nUids, sizeof(SExtUidWindow));
  if (*ppUids == NULL || *ppWins == NULL) {
    taosArrayDestroy(*ppUids);
    taosArrayDestroy(*ppWins);
    *ppUids = NULL;
    *ppWins = NULL;
    return terrno;
  }

  int32_t iter = 0;
  void   *pVal = NULL;
  while ((pVal = tSimpleHashIterate(pUidWindow, pVal, &iter)) != NULL) {
    uint64_t uid = *(uint64_t *)tSimpleHashGetKey(pVal, NULL);
    if (taosArrayPush(*ppUids, &uid) == NULL || taosArrayPush(*ppWins, (SExtUidWindow *)pVal) == NULL) {
      taosArrayDestroy(*ppUids);
      taosArrayDestroy(*ppWins);
      *ppUids = NULL;
      *ppWins = NULL;
      return terrno;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t extBuildInfluxFetchMappings(SStreamExtReaderInfo *pInfo,
                                           const SExtColTypeMapping *pMappings, int32_t nMappings,
                                           int32_t nTags, SExtColTypeMapping **ppMapAll,
                                           int32_t *pColsAll) {
  *ppMapAll = NULL;
  *pColsAll = nMappings + nTags;

  SExtColTypeMapping *pMapAll = taosMemoryCalloc(*pColsAll, sizeof(SExtColTypeMapping));
  if (pMapAll == NULL) {
    return terrno;
  }

  memcpy(pMapAll, pMappings, (size_t)nMappings * sizeof(SExtColTypeMapping));
  for (int32_t c = 0; c < nTags; ++c) {
    pMapAll[nMappings + c].tdType.type = TSDB_DATA_TYPE_NCHAR;
    pMapAll[nMappings + c].tdType.bytes = EXT_INFLUX_TAG_NCHAR_CHARS;
    tstrncpy(pMapAll[nMappings + c].colName,
             (const char *)taosArrayGet(pInfo->pInfluxTagCols, c),
             sizeof(pMapAll[nMappings + c].colName));
  }

  *ppMapAll = pMapAll;
  return TSDB_CODE_SUCCESS;
}

static int32_t extBuildInfluxFetchBatchSql(SStreamExtReaderInfo *pInfo, const char *colList,
                                           const char *tblRef, const char *tsCol,
                                           const char *prefilterBuf, int32_t nTags,
                                           SArray *pUids, SArray *pWins, int32_t batchStart,
                                           int32_t batchSize, char *sqlBuf, int32_t sqlBufLen,
                                           int32_t *pConsumed, bool *pAnyUid) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t off = 0;
  bool    anyUid = false;
  int32_t k = 0;

  for (; k < batchSize; ++k) {
    uint64_t        uid = *(uint64_t *)taosArrayGet(pUids, batchStart + k);
    SExtUidWindow  *pWin = (SExtUidWindow *)taosArrayGet(pWins, batchStart + k);
    SUidIndexEntry *pEntry = tSimpleHashGet(pInfo->pUidIndex, &uid, sizeof(uid));
    char            tagWhere[EXT_TAG_WHERE_BUF_LEN] = {0};

    if (pEntry != NULL && pEntry->tagsetKey[0] != '\0') {
      int32_t twLen = buildInfluxTagWhereClause(pEntry->tagsetKey, tagWhere, sizeof(tagWhere));
      if (twLen < 0) {
        ST_TASK_ELOG("ext: influx data batch tag WHERE exceeds %d bytes uid=%" PRIu64,
                     (int32_t)sizeof(tagWhere), uid);
        return TSDB_CODE_OUT_OF_RANGE;
      }
    }

    int32_t saveOff = off;
    bool    clauseOk = true;
    if (anyUid) clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off, " UNION ALL ");
    if (clauseOk) clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off, "SELECT %s", colList);
    for (int32_t c = 0; clauseOk && c < nTags; ++c) {
      clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off, ", %s",
                           (const char *)taosArrayGet(pInfo->pInfluxTagCols, c));
    }
    if (clauseOk) clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off, " FROM %s WHERE ", tblRef);
    if (clauseOk && prefilterBuf[0] != '\0') clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off, "%s", prefilterBuf);
    if (clauseOk && tagWhere[0] != '\0') clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off, "%s AND ", tagWhere);
    if (clauseOk) {
      clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off,
                           "%s >= to_timestamp_nanos(%" PRId64 ")"
                           " AND %s <= to_timestamp_nanos(%" PRId64 ")",
                           tsCol, pWin->skey, tsCol, pWin->ekey);
    }
    if (!clauseOk) {
      sqlBuf[saveOff] = '\0';
      ST_TASK_ELOG("ext: influx data batch SQL exceeds %d bytes uid=%" PRIu64 " batchStart=%d batchSize=%d",
                   sqlBufLen, uid, batchStart, batchSize);
      return TSDB_CODE_OUT_OF_RANGE;
    }

    anyUid = true;
  }

  *pConsumed = (k > 0) ? k : batchSize;
  *pAnyUid = anyUid;
  return TSDB_CODE_SUCCESS;
}

static int32_t extTransferUidBlocksFromHash(SSHashObj *pUidBlk, SArray *pUidBlocks) {
  int32_t iter = 0;
  void   *pVal = NULL;

  while ((pVal = tSimpleHashIterate(pUidBlk, pVal, &iter)) != NULL) {
    uint64_t      uid = *(uint64_t *)tSimpleHashGetKey(pVal, NULL);
    SSDataBlock  *pBlk = *(SSDataBlock **)pVal;
    SUidBlockPair pair = {.uid = uid, .pBlock = pBlk};

    if (taosArrayPush(pUidBlocks, &pair) == NULL) {
      return terrno;
    }
    *(SSDataBlock **)pVal = NULL;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t fetchDataBatchInflux(SStreamExtReaderInfo       *pInfo,
                                    const char                 *colList,
                                    const SExtColTypeMapping   *pMappings,
                                    int32_t                     nMappings,
                                    int32_t                     nTags,
                                    const char                 *prefilterBuf,
                                    SSHashObj                  *pUidWindow,
                                    SArray                     *pUidBlocks) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t             code = TSDB_CODE_SUCCESS;
  SExtColTypeMapping *pMapAll = NULL;
  SSHashObj          *pUidBlk = NULL;
  SArray             *pUids = NULL;
  SArray             *pWins = NULL;
  int32_t             nColsAll = 0;
  int32_t             batchStart = 0;
  SInfluxDataDemuxCtx ctx = {0};

  const char *tsCol = (pInfo->spec.tsColumn[0] != '\0') ? pInfo->spec.tsColumn : "ts";
  char tblRef[TSDB_TABLE_NAME_LEN + TSDB_DB_NAME_LEN + 2] = {0};
  code = buildExtTableRef(&pInfo->spec, tblRef, sizeof(tblRef));
  if (code < 0) goto _exit;

  int32_t nUids = tSimpleHashGetSize(pUidWindow);
  if (nUids == 0) return TSDB_CODE_SUCCESS;

  code = extSnapshotUidWindows(pUidWindow, &pUids, &pWins);
  if (code != TSDB_CODE_SUCCESS) goto _exit;

  code = extBuildInfluxFetchMappings(pInfo, pMappings, nMappings, nTags, &pMapAll, &nColsAll);
  if (code != TSDB_CODE_SUCCESS) goto _exit;

  pUidBlk = tSimpleHashInit(nUids, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (pUidBlk == NULL) { code = terrno; goto _exit; }

  ctx.pInfo   = pInfo;
  ctx.nData   = nMappings;
  ctx.nTags   = nTags;
  ctx.pUidBlk = pUidBlk;
  ctx.code    = TSDB_CODE_SUCCESS;

  batchStart = 0;
  while (batchStart < nUids && code == TSDB_CODE_SUCCESS) {
    int32_t batchEnd  = TMIN(batchStart + EXT_INFLUX_UID_BATCH_SIZE, nUids);
    int32_t batchSize = batchEnd - batchStart;
    char    sqlBuf[EXT_BATCH_SQL_BUF_LEN] = {0};
    bool    anyUid = false;
    int32_t consumed = batchSize;

    code = extBuildInfluxFetchBatchSql(pInfo, colList, tblRef, tsCol, prefilterBuf, nTags,
                                       pUids, pWins, batchStart, batchSize, sqlBuf, sizeof(sqlBuf),
                                       &consumed, &anyUid);
    if (code != TSDB_CODE_SUCCESS) break;

    if (anyUid) {
      ST_TASK_DLOG("ext: influx data batch [%d..%d) sql=\"%.180s\"",
                   batchStart, batchStart + consumed, sqlBuf);
      code = extQueryExecForEach(pInfo->pConn, sqlBuf, pMapAll, nColsAll,
                                 influxDataDemuxBlockCb, &ctx);
      if (code == TSDB_CODE_SUCCESS) code = ctx.code;
      if (code != TSDB_CODE_SUCCESS) {
        ST_TASK_ELOG("ext: influx data batch query failed code:%d", code);
        break;
      }
    }
    batchStart += consumed;
  }

  /* Move per-uid blocks into pUidBlocks (ownership transfer). */
  if (code == TSDB_CODE_SUCCESS) {
    code = extTransferUidBlocksFromHash(pUidBlk, pUidBlocks);
  }

_exit:
  /* Destroy any per-uid blocks still owned by the hash (error path / not moved). */
  if (pUidBlk != NULL) {
    int32_t it = 0;
    void   *pv = NULL;
    while ((pv = tSimpleHashIterate(pUidBlk, pv, &it)) != NULL) {
      SSDataBlock *pBlk = *(SSDataBlock **)pv;
      if (pBlk) blockDataDestroy(pBlk);
    }
    tSimpleHashCleanup(pUidBlk);
  }
  taosMemoryFree(pMapAll);
  taosArrayDestroy(pUids);
  taosArrayDestroy(pWins);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: fetchDataBatchInflux failed code:%d", code);
  }
  return code;
}

static void extDestroyUidBlocks(SArray *pUidBlocks) {
  if (pUidBlocks == NULL) {
    return;
  }

  for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pUidBlocks); ++i) {
    SUidBlockPair *pPair = taosArrayGet(pUidBlocks, i);
    if (pPair != NULL && pPair->pBlock != NULL) {
      blockDataDestroy(pPair->pBlock);
      pPair->pBlock = NULL;
    }
  }

  taosArrayDestroy(pUidBlocks);
}

static int32_t extFetchUidBlocks(SStreamExtReaderInfo *pInfo,
                                 const char *phase,
                                 const char *colList,
                                 int32_t nCols,
                                 const char *prefilterBuf,
                                 const SExtColTypeMapping *pMappings,
                                 int32_t nMappings,
                                 SSHashObj *pUidWindow,
                                 SArray *pUidBlocks) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t code = TSDB_CODE_SUCCESS;
  bool    isInflux = ((EExtSourceType)pInfo->spec.sourceType == EXT_SOURCE_INFLUXDB);
  int32_t nTags = pInfo->pInfluxTagCols ? (int32_t)taosArrayGetSize(pInfo->pInfluxTagCols) : 0;

  /* InfluxDB can batch uid windows only when the select list and mappings
   * align positionally; otherwise it must fall back to one query per uid. */
  if (isInflux && nTags > 0 && nCols > 0 && nMappings == nCols) {
    ST_TASK_DLOG("ext: %s using batched influx fetch uidCount=%d nCols=%d nTags=%d",
                 phase, tSimpleHashGetSize(pUidWindow), nCols, nTags);
    return fetchDataBatchInflux(pInfo, colList, pMappings, nMappings, nTags, prefilterBuf,
                                pUidWindow, pUidBlocks);
  }

  ST_TASK_DLOG("ext: %s using per-uid fetch uidCount=%d nCols=%d nMappings=%d",
               phase, tSimpleHashGetSize(pUidWindow), nCols, nMappings);

  int32_t iter = 0;
  void   *pVal = NULL;
  while ((pVal = tSimpleHashIterate(pUidWindow, pVal, &iter)) != NULL) {
    size_t         kLen = 0;
    uint64_t       uid = *(uint64_t *)tSimpleHashGetKey(pVal, &kLen);
    SExtUidWindow *pWin = (SExtUidWindow *)pVal;
    SSDataBlock   *pBlock = NULL;

    code = fetchDataForUid(pInfo, colList, uid, pWin->skey, pWin->ekey, isInflux, prefilterBuf,
                           pMappings, nMappings, &pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("ext: %s fetchDataForUid uid=%" PRIu64 " code:%d", phase, uid, code);
      return code;
    }

    SUidBlockPair pair = {.uid = uid, .pBlock = pBlock};
    if (taosArrayPush(pUidBlocks, &pair) == NULL) {
      code = terrno;
      ST_TASK_ELOG("ext: %s push uid block failed uid=%" PRIu64 " code:%d", phase, uid, code);
      blockDataDestroy(pBlock);
      return code;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t extBuildUidWindowFromMetaBlock(SSDataBlock *pMetaBlock, SSHashObj **ppUidWindow) {
  int32_t    code = TSDB_CODE_SUCCESS;
  SSHashObj *pUidWindow = NULL;

  if (pMetaBlock == NULL || ppUidWindow == NULL) {
    stError("ext: buildUidWindowFromMetaBlock invalid args metaBlock=%p ppUidWindow=%p",
            pMetaBlock, ppUidWindow);
    return TSDB_CODE_INVALID_PARA;
  }

  *ppUidWindow = NULL;
  pUidWindow = tSimpleHashInit(pMetaBlock->info.rows,
                               taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (pUidWindow == NULL) {
    code = terrno;
    stError("ext: buildUidWindowFromMetaBlock alloc hash failed code:%d", code);
    goto _exit;
  }

  SColumnInfoData *pColUid = taosArrayGet(pMetaBlock->pDataBlock, META_COL_UID);
  SColumnInfoData *pColSkey = taosArrayGet(pMetaBlock->pDataBlock, META_COL_SKEY);
  SColumnInfoData *pColEkey = taosArrayGet(pMetaBlock->pDataBlock, META_COL_EKEY);

  for (int32_t row = 0; row < pMetaBlock->info.rows; ++row) {
    uint64_t uid = *(uint64_t *)colDataGetData(pColUid, row);
    int64_t  skey = *(int64_t *)colDataGetData(pColSkey, row);
    int64_t  ekey = *(int64_t *)colDataGetData(pColEkey, row);
    SExtUidWindow win = {.skey = skey, .ekey = ekey};

    code = tSimpleHashPut(pUidWindow, &uid, sizeof(uid), &win, sizeof(win));
    if (code != TSDB_CODE_SUCCESS) {
      stError("ext: buildUidWindowFromMetaBlock put uid=%" PRIu64 " failed code:%d",
              uid, code);
      goto _exit;
    }
  }

  *ppUidWindow = pUidWindow;
  pUidWindow = NULL;

_exit:
  if (pUidWindow != NULL) {
    tSimpleHashCleanup(pUidWindow);
  }
  return code;
}

/* ============================================================
 * Handler: STRIGGER_PULL_DATA_EXT
 *
 * Request: hash<uid, {skey, ekey}>
 * SQL: SELECT <trigger cols> FROM db.table
 *      WHERE prefilter AND ts BETWEEN skey AND ekey [AND uid filter]
 * Response: pRsp->pDataBlock + pRsp->pIndexHash
 * ============================================================ */
static int32_t handleDataPull(SStreamExtReaderInfo *pInfo,
                              const SSTriggerExtPullReq *pReq,
                              SSTriggerExtPullRsp *pRsp) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t            code = TSDB_CODE_SUCCESS;
  int64_t            dataRows = 0;
  SArray            *pUidBlocks = NULL;

  ST_TASK_DLOG("ext: handleDataPull enter source=%s", pInfo->spec.sourceName);

  if (pReq == NULL || pReq->pUidWindow == NULL) {
    ST_TASK_ELOG("%s", "ext: handleDataPull: pUidWindow is NULL");
    return TSDB_CODE_INVALID_PARA;
  }

  char    colList[EXT_SQL_CLAUSE_BUF_LEN] = {0};
  int32_t nCols = buildTriggerColList(&pInfo->spec, colList, sizeof(colList));
  if (nCols < 0) {
    code = nCols;
    goto _exit;
  }

  /* Trigger data path: use PRE_FILTER (triggerPrefilter). */
  char prefilterBuf[EXT_SQL_CLAUSE_BUF_LEN] = {0};
  int32_t pfCode = buildTriggerPrefilterClause(&pInfo->spec, prefilterBuf, sizeof(prefilterBuf));
  if (pfCode < 0) {
    code = pfCode;
    goto _exit;
  }

  /* Mappings correspond to the explicit trigger column list; with the "*"
   * fallback (nCols == 0) the result columns won't match them positionally, so
   * drop them and let the connector return raw columns. */
  const SExtColTypeMapping *pColMaps = (nCols > 0) ? pInfo->spec.pColMappings : NULL;
  int32_t                   nColMaps = (nCols > 0) ? pInfo->spec.numColMappings : 0;

  pUidBlocks = taosArrayInit(tSimpleHashGetSize(pReq->pUidWindow), sizeof(SUidBlockPair));
  if (pUidBlocks == NULL) {
    code = terrno;
    ST_TASK_ELOG("ext: handleDataPull alloc uid blocks failed code:%d", code);
    goto _exit;
  }

  code = extFetchUidBlocks(pInfo, "handleDataPull", colList, nCols, prefilterBuf, pColMaps,
                           nColMaps, pReq->pUidWindow, pUidBlocks);
  if (code == TSDB_CODE_SUCCESS) {
    code = buildDataBlockAndIndex(pUidBlocks, &pRsp->pDataBlock, &pRsp->pIndexHash);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("ext: handleDataPull buildDataBlockAndIndex failed code:%d", code);
      goto _exit;
    }
  }

  dataRows = (pRsp->pDataBlock != NULL) ? pRsp->pDataBlock->info.rows : 0;
  ST_TASK_DLOG("ext: handleDataPull done code=%d dataRows=%" PRId64, code, dataRows);
  printDataBlock(pRsp->pDataBlock, __func__, "ext_data", pTask->streamId);

  /* Mirror vnodeProcessStreamWalMetaDataNewReq: signal NO_DATA when result is
   * empty so the trigger side treats this as a normal empty-poll. */
  if (code == TSDB_CODE_SUCCESS && dataRows == 0) {
    ST_TASK_DLOG("%s", "ext: handleDataPull no rows -> TSDB_CODE_STREAM_NO_DATA");
    code = TSDB_CODE_STREAM_NO_DATA;
  }

_exit:
  extDestroyUidBlocks(pUidBlocks);
  return code;
}

/* ============================================================
 * Handler: STRIGGER_PULL_META_DATA_EXT
 *
 * Combines META pull and DATA pull in one handler.
 * Response: pRsp->pMetaBlock + pRsp->pDataBlock + pRsp->pIndexHash
 * ============================================================ */
static int32_t handleMetaDataPull(SStreamExtReaderInfo *pInfo,
                                  const SSTriggerExtPullReq *pReq,
                                  SSTriggerExtPullRsp *pRsp) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t            code = TSDB_CODE_SUCCESS;
  SSHashObj         *pUidWindow = NULL;

  ST_TASK_DLOG("ext: handleMetaDataPull enter source=%s", pInfo->spec.sourceName);

  /* First, execute the META logic (same as handleMetaPull). */
  code = handleMetaPull(pInfo, pReq, pRsp);
  if (code != TSDB_CODE_SUCCESS && code != TSDB_CODE_STREAM_NO_DATA) {
    ST_TASK_ELOG("ext: handleMetaDataPull: meta phase failed code:%d", code);
    goto _exit;
  }

  /* Build a synthetic pUidWindow from the resulting metaBlock so we can reuse
   * handleDataPull logic. */
  if (code == TSDB_CODE_STREAM_NO_DATA || pRsp->pMetaBlock == NULL || pRsp->pMetaBlock->info.rows == 0) {
    ST_TASK_DLOG("%s", "ext: handleMetaDataPull: no meta rows, skipping data phase");
    code = TSDB_CODE_SUCCESS;
    goto _exit;
  }

  code = extBuildUidWindowFromMetaBlock(pRsp->pMetaBlock, &pUidWindow);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: handleMetaDataPull build uid window failed code:%d", code);
    goto _exit;
  }

  /* Temporarily set pUidWindow in a synthetic request. */
  SSTriggerExtPullReq syntheticReq = {0};
  if (pReq) syntheticReq = *pReq;
  syntheticReq.pUidWindow = pUidWindow;

  code = handleDataPull(pInfo, &syntheticReq, pRsp);
  if (code == TSDB_CODE_STREAM_NO_DATA) {
    code = TSDB_CODE_SUCCESS;
  }

  ST_TASK_DLOG("ext: handleMetaDataPull done code=%d metaRows=%" PRId64 " dataRows=%" PRId64,
          code,
          pRsp->pMetaBlock ? pRsp->pMetaBlock->info.rows : 0,
          pRsp->pDataBlock ? pRsp->pDataBlock->info.rows : 0);
  printDataBlock(pRsp->pMetaBlock, __func__, "ext_meta_data_meta", pTask->streamId);
  printDataBlock(pRsp->pDataBlock, __func__, "ext_meta_data_data", pTask->streamId);

_exit:
  if (pUidWindow != NULL) {
    tSimpleHashCleanup(pUidWindow);
  }
  return code;
}

static int32_t extPrepareCalcFetch(SStreamExtReaderInfo *pInfo,
                                   char *colList, int32_t colListLen,
                                   int32_t *pCols,
                                   const SExtColTypeMapping **ppMappings,
                                   int32_t *pMappings,
                                   char *prefilterBuf, int32_t prefilterBufLen) {
  const SStreamTask *pTask = pInfo->pTask;
  *pCols = buildCalcColList(&pInfo->spec, colList, colListLen);
  if (*pCols < 0) {
    return *pCols;
  }

  *ppMappings = (*pCols > 0) ? pInfo->spec.pCalcMappings : NULL;
  *pMappings = (*pCols > 0) ? pInfo->spec.numCalcMappings : 0;
  ST_TASK_DLOG("ext: handleCalcDataPull colList='%s'", colList);

  return buildTriggerPrefilterClause(&pInfo->spec, prefilterBuf, prefilterBufLen);
}

/* ============================================================
 * Handler: STRIGGER_PULL_CALC_DATA_EXT
 *
 * Fetches aggregate-input columns (calcColumns) for the given uid/window set.
 * Uses calcColumns / pCalcMappings (built from calcCacheScanPlan.pScanCols in
 * stReaderTaskDeploy) instead of triggerColumns, so the SQL selects the correct
 * columns for aggregation (e.g. val for SUM(val), not just the ts column).
 * Falls back to trigger columns when calcColumns is empty (not yet deployed).
 *
 * Returns TSDB_CODE_STREAM_NO_DATA when the result is empty (mirroring
 * vnodeProcessStreamWalCalcDataNewReq), allowing the trigger side to schedule
 * an idle wait instead of treating an empty result as an error.
 * ============================================================ */
static int32_t handleCalcDataPull(SStreamExtReaderInfo *pInfo,
                                  const SSTriggerExtPullReq *pReq,
                                  SSTriggerExtPullRsp *pRsp) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t code = TSDB_CODE_SUCCESS;
  int64_t calcRows = 0;
  int32_t nCols = 0;
  int32_t nMappings = 0;
  SArray *pUidBlocks = NULL;
  char colList[EXT_SQL_CLAUSE_BUF_LEN] = {0};
  char calcPrefilterBuf[EXT_SQL_CLAUSE_BUF_LEN] = {0};
  const SExtColTypeMapping *pMappings = NULL;

  ST_TASK_DLOG("ext: handleCalcDataPull enter source=%s calcCols=%d",
               pInfo->spec.sourceName,
               pInfo->spec.calcColumns
                   ? (int32_t)taosArrayGetSize(pInfo->spec.calcColumns)
                   : 0);

  if (pReq == NULL || pReq->pUidWindow == NULL) {
    ST_TASK_ELOG("%s", "ext: handleCalcDataPull: pUidWindow is NULL");
    return TSDB_CODE_INVALID_PARA;
  }

  code = extPrepareCalcFetch(pInfo, colList, sizeof(colList), &nCols, &pMappings,
                             &nMappings, calcPrefilterBuf, sizeof(calcPrefilterBuf));
  if (code < 0) {
    goto _exit;
  }

  pUidBlocks = taosArrayInit(tSimpleHashGetSize(pReq->pUidWindow), sizeof(SUidBlockPair));
  if (pUidBlocks == NULL) {
    code = terrno;
    ST_TASK_ELOG("ext: handleCalcDataPull alloc uid blocks failed code:%d", code);
    goto _exit;
  }

  code = extFetchUidBlocks(pInfo, "handleCalcDataPull", colList, nCols,
                           calcPrefilterBuf, pMappings, nMappings,
                           pReq->pUidWindow, pUidBlocks);
  if (code == TSDB_CODE_SUCCESS) {
    code = buildDataBlockAndIndex(pUidBlocks, &pRsp->pDataBlock, &pRsp->pIndexHash);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("ext: handleCalcDataPull buildDataBlockAndIndex failed code:%d", code);
      goto _exit;
    }
  }

  calcRows = (pRsp->pDataBlock != NULL) ? pRsp->pDataBlock->info.rows : 0;
  ST_TASK_DLOG("ext: handleCalcDataPull done code=%d calcRows=%" PRId64, code, calcRows);
  printDataBlock(pRsp->pDataBlock, __func__, "ext_calc_data", pTask->streamId);
  if (code == TSDB_CODE_SUCCESS && calcRows == 0) {
    ST_TASK_DLOG("%s", "ext: handleCalcDataPull no rows -> TSDB_CODE_STREAM_NO_DATA");
    code = TSDB_CODE_STREAM_NO_DATA;
  }

_exit:
  extDestroyUidBlocks(pUidBlocks);
  return code;
}

/* ============================================================
 * Helpers for handleGroupColValuePull (below): resolving one gid's
 * PARTITION BY tag/tbname values is broken into small steps so the overall
 * flow reads as an orchestrator rather than one long function.
 * ============================================================ */

/* Per-call state threaded through the extGroupColValue* helpers below. */
typedef struct SExtGroupColValueCtx {
  SStreamExtReaderInfo *pInfo;
  uint64_t              uid;
  int32_t               nPart;       /* size of pInfo->spec.partitionTagCols, or 0 */
  bool                  wantTbname;  /* tbname is in PARTITION BY and source is InfluxDB */
  int32_t               tbnameSlot;  /* position of the INFLUXDB_PARTITION_BY_TBNAME
                                      * sentinel in partitionTagCols, or -1 if
                                      * tbname isn't one of the explicit
                                      * PARTITION BY items */
  SStreamGroupValue    *pSlots;      /* nPart slots, indexed like partitionTagCols */
  bool                 *pFilled;     /* which of pSlots[] have a real value */
  /* Scratch for the "<extTable>_<tagVal>_<tagVal>..." subtable identity. */
  char                  nameBuf[TSDB_TABLE_NAME_LEN];
  int32_t               nameLen;
  bool                  nameOverflow;
} SExtGroupColValueCtx;

/* Find the position (0-based) within partitionTagCols marked with the
 * INFLUXDB_PARTITION_BY_TBNAME sentinel by buildExtSpecs (parTranslater.c) -- i.e. a bare tbname
 * reference mixed into PARTITION BY alongside explicit tag columns (e.g.
 * "PARTITION BY host, tbname, region" -> tbname is position 2, 0-based
 * index 1). Returns -1 when tbname isn't one of the explicit positions. */
static int32_t extGroupColValueFindTbnameSlot(SStreamExtReaderInfo *pInfo, int32_t nPart) {
  for (int32_t p = 0; p < nPart; p++) {
    const char *pcol = (const char *)taosArrayGet(pInfo->spec.partitionTagCols, p);
    if (pcol != NULL && strcmp(pcol, INFLUXDB_PARTITION_BY_TBNAME) == 0) {
      return p;
    }
  }
  return -1;
}

/* Build one UCS-4-encoded NCHAR SStreamGroupValue from a raw UTF-8 tag value.
 * InfluxDB tag columns are discovered/typed as NCHAR (see
 * extInitInfluxTagPartition's pMappings[i].tdType.type), so the parser
 * resolves a PARTITION BY tag reference (e.g. "host") to NCHAR too.
 * FUNCTION_TYPE_PLACEHOLDER_COLUMN (functionMgt.c) rejects a type mismatch
 * against the resolved value node's type, so this must be UCS-4-encoded
 * NCHAR, not raw UTF-8 VARCHAR bytes. */
static int32_t extBuildNCharGroupValue(const SStreamTask *pTask, const char *colName, const char *colVal,
                                       SStreamGroupValue *pOut) {
  SStreamGroupValue val = {0};
  val.isNull   = false;
  val.isTbname = false;
  val.data.type = TSDB_DATA_TYPE_NCHAR;
  int32_t srcLen  = (int32_t)strlen(colVal);
  int32_t maxUcs4 = srcLen * TSDB_NCHAR_SIZE;
  val.data.pData = taosMemoryMalloc(maxUcs4 > 0 ? maxUcs4 : 1);
  if (val.data.pData == NULL) return terrno;
  int32_t outLen = 0;
  if (srcLen > 0) {
    outLen = maxUcs4;
    bool ok = taosMbsToUcs4(colVal, srcLen, (TdUcs4 *)val.data.pData, maxUcs4, &outLen, NULL);
    if (!ok) {
      ST_TASK_ELOG("ext: handleGroupColValuePull UTF-8 to UCS-4 conversion failed for col='%s'", colName);
      taosMemoryFree(val.data.pData);
      return TSDB_CODE_INVALID_PARA;
    }
  }
  val.data.nData = outLen;
  *pOut = val;
  return TSDB_CODE_SUCCESS;
}

/* Single pass over pEntry->tagsetKey ("col=val|col=val|..."). With no
 * positional PARTITION BY tuple it emits the raw tag values. Otherwise the
 * typed tuple comes from pEntry->partitionValues and this scan only composes
 * the "<extTable>_<tagVal>_..." subtable identity when tbname is requested. */
static int32_t extGroupColValueScanTagsetKey(SExtGroupColValueCtx *pCtx, const SUidIndexEntry *pEntry,
                                             SArray *pGroupColVals) {
  int32_t            code  = TSDB_CODE_SUCCESS;
  const SStreamTask *pTask = pCtx->pInfo->pTask;

  char tagsetKey[EXT_TAGSET_KEY_MAX] = {0};
  tstrncpy(tagsetKey, pEntry->tagsetKey, sizeof(tagsetKey));

  const char *colNames[TSDB_MAX_TAGS] = {0};
  const char *colVals[TSDB_MAX_TAGS] = {0};
  int32_t     nCols = 0;
  char       *saveptr = NULL;
  char       *pair = strtok_r(tagsetKey, "|", &saveptr);
  while (pair != NULL) {
    char *eq = strchr(pair, '=');
    if (eq == NULL) {
      pair = strtok_r(NULL, "|", &saveptr);
      continue;
    }
    *eq = '\0';
    const char *colName = pair;
    const char *colVal  = eq + 1;
    if (nCols >= TSDB_MAX_TAGS) {
      ST_TASK_ELOG("ext: tagset contains more than %d columns", TSDB_MAX_TAGS);
      return TSDB_CODE_OUT_OF_RANGE;
    }
    colNames[nCols] = colName;
    colVals[nCols] = colVal;
    ++nCols;

    if (pCtx->wantTbname && !pCtx->nameOverflow) {
      int32_t n = snprintf(pCtx->nameBuf + pCtx->nameLen, sizeof(pCtx->nameBuf) - pCtx->nameLen, "_%s", colVal);
      if (n < 0 || n >= (int32_t)(sizeof(pCtx->nameBuf) - pCtx->nameLen)) {
        pCtx->nameOverflow = true;
      } else {
        pCtx->nameLen += n;
      }
    }

    pair = strtok_r(NULL, "|", &saveptr);
  }

  if (pCtx->nPart == 0) {
    for (int32_t i = 0; i < nCols; ++i) {
      SStreamGroupValue value = {0};
      code = extBuildNCharGroupValue(pTask, colNames[i], colVals[i], &value);
      if (code != TSDB_CODE_SUCCESS) return code;
      if (taosArrayPush(pGroupColVals, &value) == NULL) {
        tDestroySStreamGroupValue(&value);
        return terrno;
      }
    }
    return TSDB_CODE_SUCCESS;
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t extGroupColValueCopyCachedSlots(SExtGroupColValueCtx *pCtx, const SUidIndexEntry *pEntry) {
  const SStreamTask *pTask = pCtx->pInfo->pTask;
  if (pCtx->nPart == 0) return TSDB_CODE_SUCCESS;
  if (pEntry == NULL || pEntry->partitionValues == NULL ||
      taosArrayGetSize(pEntry->partitionValues) != pCtx->nPart) {
    ST_TASK_ELOG("ext: cached partition tuple missing or misaligned uid:%" PRIu64 " expected:%d actual:%d",
                 pCtx->uid, pCtx->nPart,
                 pEntry != NULL && pEntry->partitionValues != NULL
                     ? (int32_t)taosArrayGetSize(pEntry->partitionValues)
                     : 0);
    return TSDB_CODE_MND_STREAM_TBNAME_CALC_FAILED;
  }

  for (int32_t p = 0; p < pCtx->nPart; ++p) {
    if (p == pCtx->tbnameSlot) continue;
    SStreamGroupValue *pCached = (SStreamGroupValue *)taosArrayGet(pEntry->partitionValues, p);
    int32_t code = extCloneGroupValue(pCached, &pCtx->pSlots[p]);
    if (code != TSDB_CODE_SUCCESS) return code;
    pCtx->pFilled[p] = true;
  }
  ST_TASK_DLOG("ext: reused %d cached partition slot(s) for uid:%" PRIu64, pCtx->nPart, pCtx->uid);
  return TSDB_CODE_SUCCESS;
}

/* Resolve the sub-table's own tbname string into tbnameBuf: the composed
 * "<measurement>_<tagVal>..." identity when pCtx->nameBuf was built without
 * overflowing (and this uid actually had a tagsetKey to compose from), else
 * the "influxdb_<uid>" fallback. Feeds extGroupColValueFillTbnameSlot's
 * single call site in handleGroupColValuePull. */
static void extGroupColValueComposeTbname(const SExtGroupColValueCtx *pCtx, bool hadTagsetKey, uint64_t uid,
                                          uint64_t gid, char *tbnameBuf, int32_t tbnameBufSize,
                                          int32_t *pTbnameLen) {
  const SStreamTask *pTask    = pCtx->pInfo->pTask;
  bool                composed = hadTagsetKey && !pCtx->nameOverflow;
  if (composed) {
    *pTbnameLen = snprintf(tbnameBuf, tbnameBufSize, "%s", pCtx->nameBuf);
  } else {
    if (hadTagsetKey) {
      ST_TASK_DLOG("ext: handleGroupColValuePull gid=%" PRIu64 " uid=%" PRIu64
                   " measurement_tag name overflowed TSDB_TABLE_NAME_LEN, "
                   "falling back to influxdb_<uid>", gid, uid);
    }
    *pTbnameLen = snprintf(tbnameBuf, tbnameBufSize, "influxdb_%" PRIu64, uid);
  }
}

/* Fill pCtx->pSlots[pCtx->tbnameSlot] (if tbname is one of the explicit
 * PARTITION BY positions) as a regular VARCHAR positional value -- NOT
 * UCS-4 NCHAR like the tag-column slots in extGroupColValueScanTagsetKey,
 * since a positional reference to this slot (e.g. %%2) has its parse-time
 * resType inherited from the raw tbname() node itself (TSDB_DATA_TYPE_BINARY/
 * VARCHAR; see FUNCTION_TYPE_PLACEHOLDER_COLUMN's resType resolution in
 * parTranslater.c, which copies pExpr->resType from the PARTITION BY list
 * entry at that position). No-op when tbname isn't an explicit position.
 *
 * Also marked isTbname=true: since buildExtSpecs guarantees partitionTagCols
 * is non-NULL whenever partitionByTbname is set (a bare "PARTITION BY
 * tbname" always occupies its own slot too, see the anyPositionalItem check
 * in parTranslater.c), this slot always exists whenever handleGroupColValuePull
 * wants tbname at all -- so it doubles as the sole source FUNCTION_TYPE_
 * PLACEHOLDER_TBNAME (%%tbname, functionMgt.c) scans for, with no separate
 * trailing entry needed. */
static int32_t extGroupColValueFillTbnameSlot(SExtGroupColValueCtx *pCtx, const char *tbnameBuf, int32_t tbnameLen) {
  if (pCtx->tbnameSlot < 0) return TSDB_CODE_SUCCESS;

  SStreamGroupValue tbSlotVal = {0};
  tbSlotVal.isNull    = false;
  tbSlotVal.isTbname  = true;
  tbSlotVal.data.type = TSDB_DATA_TYPE_VARCHAR;
  tbSlotVal.data.pData = taosMemoryMalloc(tbnameLen > 0 ? tbnameLen : 1);
  if (tbSlotVal.data.pData == NULL) return terrno;
  if (tbnameLen > 0) memcpy(tbSlotVal.data.pData, tbnameBuf, tbnameLen);
  tbSlotVal.data.nData = tbnameLen;

  pCtx->pSlots[pCtx->tbnameSlot]  = tbSlotVal;
  pCtx->pFilled[pCtx->tbnameSlot] = true;
  return TSDB_CODE_SUCCESS;
}

/* Destroy any typed pCtx->pSlots[p] still marked pFilled[p] -- i.e. built by
 * extGroupColValueScanTagsetKey/extGroupColValueFillTbnameSlot but never
 * handed off to pGroupColVals (extGroupColValueFlushSlots clears pFilled[p]
 * the moment ownership transfers, see below). Safe to call unconditionally
 * from handleGroupColValuePull's _exit on both the success and every error
 * path: on success every slot has already been cleared by a successful
 * flush, so this is a no-op; on any early return (a tag conversion failure
 * mid tagsetKey scan, an OOM while filling the tbname slot, or a failed
 * taosArrayPush partway through the flush) it reclaims exactly the slots
 * that were filled but never flushed. */
static void extGroupColValueFreeFilledSlots(SExtGroupColValueCtx *pCtx) {
  if (pCtx->pSlots == NULL || pCtx->pFilled == NULL) return;
  for (int32_t p = 0; p < pCtx->nPart; p++) {
    if (pCtx->pFilled[p]) {
      tDestroySStreamGroupValue(&pCtx->pSlots[p]);
      pCtx->pFilled[p] = false;
    }
  }
}

/* Flush the ordered tag-column slots (subset, or mixed-with-tbname) into
 * pGroupColVals, in partitionTagCols order -- this is what keeps position p
 * aligned with _placeholder_column(p+1). A slot that was never filled is
 * emitted as isNull=true so later positions don't
 * shift. Clears pFilled[p] the instant a filled slot's value is copied into
 * pGroupColVals: ownership of slotVal.data.pData has now passed to the
 * array, so extGroupColValueFreeFilledSlots must not free it again. On a
 * push failure partway through, the slots already pushed are correctly
 * left untouched (pFilled already false) and every remaining
 * filled-but-not-yet-pushed slot (including the one that just failed) is
 * left with pFilled[p]==true so the caller's cleanup reclaims it -- no slot
 * is ever freed twice or leaked. */
static int32_t extGroupColValueFlushSlots(SExtGroupColValueCtx *pCtx, SArray *pGroupColVals) {
  for (int32_t p = 0; p < pCtx->nPart; p++) {
    SStreamGroupValue slotVal = {0};
    if (pCtx->pFilled[p]) {
      slotVal = pCtx->pSlots[p];
    } else {
      slotVal.isNull = true;
    }
    if (taosArrayPush(pGroupColVals, &slotVal) == NULL) {
      return terrno;
    }
    pCtx->pFilled[p] = false;
  }
  return TSDB_CODE_SUCCESS;
}

static int32_t extInitGroupColValueCtx(SStreamExtReaderInfo *pInfo, uint64_t gid,
                                       SExtGroupColValueCtx *pCtx) {
  pCtx->pInfo = pInfo;
  pCtx->uid = gid;

  SArray **ppUids = (SArray **)tSimpleHashGet(pInfo->pGroupIndex, &gid, sizeof(gid));
  if (ppUids != NULL && taosArrayGetSize(*ppUids) > 0) {
    pCtx->uid = *(uint64_t *)taosArrayGet(*ppUids, 0);
  }

  pCtx->nPart = pInfo->spec.partitionTagCols
                    ? (int32_t)taosArrayGetSize(pInfo->spec.partitionTagCols)
                    : 0;
  pCtx->wantTbname = (pInfo->spec.partitionByTbname &&
                      (EExtSourceType)pInfo->spec.sourceType == EXT_SOURCE_INFLUXDB);
  pCtx->tbnameSlot = (pCtx->wantTbname && pCtx->nPart > 0)
                         ? extGroupColValueFindTbnameSlot(pInfo, pCtx->nPart)
                         : -1;

  if (pCtx->wantTbname) {
    pCtx->nameLen = snprintf(pCtx->nameBuf, sizeof(pCtx->nameBuf), "%s", pInfo->spec.extTable);
    pCtx->nameOverflow = (pCtx->nameLen < 0 || pCtx->nameLen >= (int32_t)sizeof(pCtx->nameBuf));
  }

  if (pCtx->nPart == 0) {
    return TSDB_CODE_SUCCESS;
  }

  pCtx->pSlots = (SStreamGroupValue *)taosMemoryCalloc(pCtx->nPart, sizeof(SStreamGroupValue));
  pCtx->pFilled = (bool *)taosMemoryCalloc(pCtx->nPart, sizeof(bool));
  if (pCtx->pSlots == NULL || pCtx->pFilled == NULL) {
    return terrno;
  }

  return TSDB_CODE_SUCCESS;
}

/* ============================================================
 * Handler: STRIGGER_PULL_GROUP_COL_VALUE_EXT
 *
 * Resolves the PARTITION BY tag column value(s) for one gid, using the
 * tagsetKey ("col1=val1|col2=val2|...") recorded per-uid at table-list init
 * time (extInitInfluxTagPartition / influxDistinctBlockCb).  Mirrors the
 * non-EXT vnodeProcessStreamGroupColValueReq, which answers from a
 * pre-built groupIdMap instead of the reader's own pUidIndex/pGroupIndex.
 *
 * The trigger side currently sets gid == uid directly (see
 * stRealtimeContextAddExtDataSlices), so pGroupIndex[gid] is tried first and
 * gid itself is used as the uid fallback -- this keeps the lookup correct
 * whether or not gid is later changed to be the reader's own subset-partition
 * groupId.
 * ============================================================ */
static int32_t handleGroupColValuePull(SStreamExtReaderInfo *pInfo,
                                       const SSTriggerExtPullReq *pReq,
                                       SSTriggerExtPullRsp *pRsp) {
  const SStreamTask *pTask = pInfo->pTask;
  uint64_t           gid   = (uint64_t)pReq->gid;
  int32_t            code  = TSDB_CODE_SUCCESS;

  ST_TASK_DLOG("ext: handleGroupColValuePull enter gid=%" PRIu64, gid);

  pRsp->pGroupColVals = taosArrayInit(4, sizeof(SStreamGroupValue));
  if (pRsp->pGroupColVals == NULL) {
    code = terrno;
    goto _exit;
  }

  SExtGroupColValueCtx ctx = {0};
  code = extInitGroupColValueCtx(pInfo, gid, &ctx);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: handleGroupColValuePull init ctx failed gid=%" PRIu64 " code:%d",
                 gid, code);
    goto _exit;
  }

  SUidIndexEntry *pEntry = (SUidIndexEntry *)tSimpleHashGet(pInfo->pUidIndex, &ctx.uid, sizeof(ctx.uid));
  bool hadTagsetKey = (pEntry != NULL && pEntry->tagsetKey[0] != '\0');
  if (!hadTagsetKey) {
    /* MySQL/PG (no tag concept) or no tag columns: nothing to report from
     * the tagsetKey path; still fall through to the tbname branch below. */
    ST_TASK_DLOG("ext: handleGroupColValuePull gid=%" PRIu64 " uid=%" PRIu64 " has no tagsetKey",
                 gid, ctx.uid);
  } else {
    code = extGroupColValueScanTagsetKey(&ctx, pEntry, pRsp->pGroupColVals);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
  }
  code = extGroupColValueCopyCachedSlots(&ctx, pEntry);
  if (code != TSDB_CODE_SUCCESS) goto _exit;

  char    tbnameBuf[32 + TSDB_TABLE_NAME_LEN] = {0};
  int32_t tbnameLen = 0;
  if (ctx.wantTbname) {
    extGroupColValueComposeTbname(&ctx, hadTagsetKey, ctx.uid, gid, tbnameBuf, sizeof(tbnameBuf), &tbnameLen);
    code = extGroupColValueFillTbnameSlot(&ctx, tbnameBuf, tbnameLen);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
  }

  code = extGroupColValueFlushSlots(&ctx, pRsp->pGroupColVals);
  if (code != TSDB_CODE_SUCCESS) goto _exit;

  ST_TASK_DLOG("ext: handleGroupColValuePull gid=%" PRIu64 " resolved %d tag value(s)",
               gid, (int32_t)taosArrayGetSize(pRsp->pGroupColVals));

_exit:
  extGroupColValueFreeFilledSlots(&ctx);
  taosMemoryFree(ctx.pSlots);
  taosMemoryFree(ctx.pFilled);
  return code;
}

static bool extAppendFetchDataBound(char *sql, int32_t sqlLen, int32_t *pOff,
                                    const char *tsCol, const char *op,
                                    int64_t ts, bool isInflux) {
  if (isInflux) {
    return extSqlCat(sql, sqlLen, pOff, " AND %s %s to_timestamp_nanos(%" PRId64 ")",
                     tsCol, op, ts);
  }

  char dt[32] = {0};
  epochToDatetimeStr(ts, TSDB_TIME_PRECISION_MICRO, dt, sizeof(dt));
  return extSqlCat(sql, sqlLen, pOff, " AND %s %s %s", tsCol, op, dt);
}

/* ============================================================
 * P4-E1: EXT fetch — called by the snode runner-queue worker
 * when a TDMT_STREAM_FETCH_EXT message arrives from a runner
 * (calc task).  Executes a time-bounded SELECT against the
 * external source and returns a single data block.
 *
 * SQL template:
 *   SELECT * FROM <extDb>.<extTable>
 *     WHERE <tsCol> >= <skey> AND <tsCol> <= <ekey>
 * Both bounds are optional: pass TSDB_DATA_MIN_TIMESTAMP /
 * TSDB_DATA_MAX_TIMESTAMP to omit a bound.
 * ============================================================ */
int32_t streamReaderExtFetchData(SStreamExtReaderInfo *pReaderInfo,
                                 int64_t skey, int64_t ekey,
                                 SSDataBlock **ppOut) {
  int32_t code = TSDB_CODE_SUCCESS;
  bool    ok = true;

  if (pReaderInfo == NULL || ppOut == NULL) {
    stError("ext: streamReaderExtFetchData invalid args pInfo=%p ppOut=%p",
            pReaderInfo, ppOut);
    return TSDB_CODE_INVALID_PARA;
  }

  const SStreamTask *pTask = pReaderInfo->pTask;
  *ppOut = NULL;

  const char *tsCol    = (pReaderInfo->spec.tsColumn[0] != '\0')
                           ? pReaderInfo->spec.tsColumn : "ts";

  char tblRef[TSDB_TABLE_NAME_LEN + TSDB_DB_NAME_LEN + 2] = {0};
  code = buildExtTableRef(&pReaderInfo->spec, tblRef, sizeof(tblRef));
  if (code < 0) {
    goto _exit;
  }

  /* Build column list from calcColumns (aggregate-input columns).
   * Returns the explicit column count, or 0 when it falls back to "*". */
  char    colList[EXT_SQL_CLAUSE_BUF_LEN] = {0};
  int32_t nCols = buildCalcColList(&pReaderInfo->spec, colList, sizeof(colList));
  if (nCols < 0) {
    code = nCols;
    goto _exit;
  }

  /* Only enforce the calc column-type mappings when we built an explicit column
   * list (nCols > 0).  With the "*" fallback the result column set and order are
   * not guaranteed to match pCalcMappings positionally, so let the connector
   * return raw columns instead of mis-typing them. */
  const SExtColTypeMapping *pMappings = (nCols > 0) ? pReaderInfo->spec.pCalcMappings : NULL;
  int32_t                   nMappings = (nCols > 0) ? pReaderInfo->spec.numCalcMappings : 0;

  /* streamReaderExtFetchData fetches pure calc data for aggregation;
   * apply calc WHERE prefilter (prefilter) only — not PRE_FILTER. */
  char prefilterBuf[EXT_SQL_CLAUSE_BUF_LEN] = {0};
  code = buildPrefilterClause(&pReaderInfo->spec, prefilterBuf, sizeof(prefilterBuf));
  if (code < 0) {
    goto _exit;
  }

  /* Build the SELECT statement (bounds-safe appends; bail out on truncation so
   * we never execute a malformed query). */
  char    sql[EXT_CALC_SQL_BUF_LEN] = {0};
  int32_t off = 0;
  ok = extSqlCat(sql, sizeof(sql), &off, "SELECT %s FROM %s WHERE %s1=1", colList, tblRef,
                 prefilterBuf);

  bool isInflux = ((EExtSourceType)pReaderInfo->spec.sourceType == EXT_SOURCE_INFLUXDB);
  if (ok && skey != INT64_MIN) {
    ok = extAppendFetchDataBound(sql, sizeof(sql), &off, tsCol, ">=", skey, isInflux);
  }
  if (ok && ekey != INT64_MAX) {
    ok = extAppendFetchDataBound(sql, sizeof(sql), &off, tsCol, "<=", ekey, isInflux);
  }
  if (!ok) {
    ST_TASK_ELOG("ext: streamReaderExtFetchData SQL exceeds %d bytes, aborting fetch",
                 (int32_t)sizeof(sql));
    code = TSDB_CODE_OUT_OF_RANGE;
    goto _exit;
  }

  ST_TASK_DLOG("ext: streamReaderExtFetchData sql=\"%.200s\" nMappings=%d",
               sql, nMappings);

  code = extQueryExecFetchAll(pReaderInfo->pConn, sql,
                              (SExtColTypeMapping *)pMappings, nMappings, ppOut);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: streamReaderExtFetchData failed code:%d", code);
    goto _exit;
  }

  ST_TASK_DLOG("ext: streamReaderExtFetchData done rows=%" PRId64,
          *ppOut ? (*ppOut)->info.rows : 0);

_exit:
  return code;
}
