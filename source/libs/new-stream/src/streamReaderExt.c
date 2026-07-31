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
#include "taoserror.h"
#include "tdatablock.h"
#include "thash.h"
#include "tlog.h"
#include "tmsg.h"
#include "tsimplehash.h"
#include "taos.h"
#include "tutil.h"

/* STREAM_RETURN_ROWS_NUM is defined in streamMsg.h (shared with trigger side).
 * The #ifndef guard below is kept only as a safety net for out-of-tree builds. */
#ifndef STREAM_RETURN_ROWS_NUM
#define STREAM_RETURN_ROWS_NUM 4096
#endif

/* extConnectorInt.h declares extDecryptPassword (internal API shared between
 * community extConnector and the stream reader).                              */
#include "extConnectorInt.h"


/* Maximum number of uids per InfluxDB OR-group (DS §6.1.5). */
#define EXT_INFLUX_UID_BATCH_SIZE  64

/* SQL buffer size; large enough for any generated statement. */
#define EXT_SQL_BUF_SIZE  (4096 * 4)

/* Max length for the tagset key string used as pTagsetIndex key
 * ("col1=val1|col2=val2|..."). */
#define EXT_TAGSET_KEY_MAX 1024

/* Scratch buffer size for one generated SQL clause fragment (a WHERE
 * prefilter condition or a SELECT column list) before it is spliced into a
 * full statement in EXT_SQL_BUF_SIZE. */
#define EXT_SQL_CLAUSE_BUF_LEN 1024

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
  if ((EExtSourceType)pSpec->sourceType == EXT_SOURCE_INFLUXDB) {
    return snprintf(buf, bufLen, "%s", pSpec->extTable);
  }
  return snprintf(buf, bufLen, "%s.%s",
                  extTableQualifier(pSpec), pSpec->extTable);
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
    /* Truncation: safer to drop the prefilter than to send a malformed SQL. */
    stWarn("ext: buildPrefilterClause truncated (need %d have %d), dropping prefilter",
           n, bufLen);
    buf[0] = '\0';
    return 0;
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
    stWarn("ext: buildTriggerPrefilterClause truncated (need %d have %d), dropping triggerPrefilter",
           n, bufLen);
    buf[0] = '\0';
    return 0;
  }
  return n;
}

/* ============================================================
 * Utility: collect all uids from pUidIndex into a flat SArray<int64_t>.
 * Caller must taosArrayDestroy the result.
 * ============================================================ */
static SArray *collectAllUids(SSHashObj *pUidIndex) {
  SArray *pArr = taosArrayInit(tSimpleHashGetSize(pUidIndex), sizeof(int64_t));
  if (pArr == NULL) return NULL;

  int32_t iter = 0;
  void   *pVal = NULL;
  while ((pVal = tSimpleHashIterate(pUidIndex, pVal, &iter)) != NULL) {
    size_t  keyLen = 0;
    int64_t uid    = *(int64_t *)tSimpleHashGetKey(pVal, &keyLen);
    taosArrayPush(pArr, &uid);
  }
  return pArr;
}

/* ============================================================
 * Utility: collect uids from pUidWindow hash into a flat SArray<int64_t>.
 * ============================================================ */
static SArray *collectUidsFromWindow(SSHashObj *pUidWindow) {
  if (pUidWindow == NULL) return NULL;
  SArray *pArr = taosArrayInit(tSimpleHashGetSize(pUidWindow), sizeof(int64_t));
  if (pArr == NULL) return NULL;

  int32_t iter = 0;
  void   *pVal = NULL;
  while ((pVal = tSimpleHashIterate(pUidWindow, pVal, &iter)) != NULL) {
    size_t  kLen = 0;
    int64_t uid  = *(int64_t *)tSimpleHashGetKey(pVal, &kLen);
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
  /* NOTE: password is intentionally NOT logged */

  SStreamExtReaderInfo *pInfo =
    taosMemoryCalloc(1, sizeof(SStreamExtReaderInfo));
  if (pInfo == NULL) {
    stError("ext: OOM allocating SStreamExtReaderInfo");
    return terrno;
  }

  /* Store back-pointer to owning task for log context (not owned). */
  pInfo->pTask = pTask;

  /* Copy the spec into local state. */
  pInfo->spec = *pExtSpec;
  /* The struct copy above aliases every owned pointer to the caller's spec.
   * Null the ones deep-copied further below up-front so an early _err (before
   * their copy block runs) cannot make streamReaderExtClose double-free the
   * caller-owned array. */
  pInfo->spec.partitionTagCols = NULL;
  /* The prefilter pointer must be deep-copied since the spec is transient. */
  if (pExtSpec->prefilter != NULL && pExtSpec->prefilter[0] != '\0') {
    pInfo->spec.prefilter = tstrdup(pExtSpec->prefilter);
    if (pInfo->spec.prefilter == NULL) {
      code = terrno;
      ST_TASK_ELOG("%s", "ext: OOM copying prefilter");
      goto _err;
    }
  } else {
    pInfo->spec.prefilter = NULL;
  }

  /* Deep-copy triggerPrefilter (PRE_FILTER for trigger reader). */
  if (pExtSpec->triggerPrefilter != NULL && pExtSpec->triggerPrefilter[0] != '\0') {
    pInfo->spec.triggerPrefilter = tstrdup(pExtSpec->triggerPrefilter);
    if (pInfo->spec.triggerPrefilter == NULL) {
      code = terrno;
      ST_TASK_ELOG("%s", "ext: OOM copying triggerPrefilter");
      goto _err;
    }
  } else {
    pInfo->spec.triggerPrefilter = NULL;
  }

  /* Deep-copy triggerColumns (SArray<char[TSDB_COL_NAME_LEN]>) — the source
   * spec is transient (deploy msg lifetime), so the reader owns its own copy. */
  pInfo->spec.triggerColumns = NULL;
  if (pExtSpec->triggerColumns != NULL) {
    int32_t nCols = (int32_t)taosArrayGetSize(pExtSpec->triggerColumns);
    pInfo->spec.triggerColumns = taosArrayInit(nCols, TSDB_COL_NAME_LEN);
    if (pInfo->spec.triggerColumns == NULL) {
      code = terrno;
      ST_TASK_ELOG("%s", "ext: OOM copying triggerColumns");
      goto _err;
    }
    for (int32_t i = 0; i < nCols; i++) {
      const char *colName = (const char *)taosArrayGet(pExtSpec->triggerColumns, i);
      if (taosArrayPush(pInfo->spec.triggerColumns, colName) == NULL) {
        code = terrno;
        ST_TASK_ELOG("ext: OOM pushing triggerColumns[%d]='%s'", i, colName);
        goto _err;
      }
    }
    ST_TASK_DLOG("ext: copied %d triggerColumns into spec", nCols);
  }

  /* Deep-copy partitionTagCols (SArray<char[TSDB_COL_NAME_LEN]>) — PARTITION BY
   * tag column names used to derive groupId over the partition-tag subset. */
  pInfo->spec.partitionTagCols = NULL;
  if (pExtSpec->partitionTagCols != NULL) {
    int32_t nPart = (int32_t)taosArrayGetSize(pExtSpec->partitionTagCols);
    pInfo->spec.partitionTagCols = taosArrayInit(nPart, TSDB_COL_NAME_LEN);
    if (pInfo->spec.partitionTagCols == NULL) {
      code = terrno;
      ST_TASK_ELOG("%s", "ext: OOM copying partitionTagCols");
      goto _err;
    }
    for (int32_t i = 0; i < nPart; i++) {
      const char *colName = (const char *)taosArrayGet(pExtSpec->partitionTagCols, i);
      if (taosArrayPush(pInfo->spec.partitionTagCols, colName) == NULL) {
        code = terrno;
        ST_TASK_ELOG("ext: OOM pushing partitionTagCols[%d]='%s'", i, colName);
        goto _err;
      }
    }
    ST_TASK_DLOG("ext: copied %d partitionTagCols into spec", nPart);
  }

  /* Deep-copy pColMappings (SExtColTypeMapping[]) — same lifetime concern. */
  pInfo->spec.pColMappings   = NULL;
  pInfo->spec.numColMappings = 0;
  if (pExtSpec->numColMappings > 0 && pExtSpec->pColMappings != NULL) {
    pInfo->spec.pColMappings = (SExtColTypeMapping *)taosMemoryMalloc(
        pExtSpec->numColMappings * sizeof(SExtColTypeMapping));
    if (pInfo->spec.pColMappings == NULL) {
      code = terrno;
      ST_TASK_ELOG("%s", "ext: OOM copying pColMappings");
      goto _err;
    }
    memcpy(pInfo->spec.pColMappings, pExtSpec->pColMappings,
           pExtSpec->numColMappings * sizeof(SExtColTypeMapping));
    pInfo->spec.numColMappings = pExtSpec->numColMappings;
    ST_TASK_DLOG("ext: copied %d colMappings into spec", pExtSpec->numColMappings);
  }

  /* Deep-copy calcColumns (SArray<char[TSDB_COL_NAME_LEN]>) — aggregate-input columns. */
  pInfo->spec.calcColumns = NULL;
  if (pExtSpec->calcColumns != NULL) {
    int32_t nCalc = (int32_t)taosArrayGetSize(pExtSpec->calcColumns);
    pInfo->spec.calcColumns = taosArrayInit(nCalc, TSDB_COL_NAME_LEN);
    if (pInfo->spec.calcColumns == NULL) {
      code = terrno;
      ST_TASK_ELOG("%s", "ext: OOM copying calcColumns");
      goto _err;
    }
    for (int32_t i = 0; i < nCalc; i++) {
      const char *colName = (const char *)taosArrayGet(pExtSpec->calcColumns, i);
      if (taosArrayPush(pInfo->spec.calcColumns, colName) == NULL) {
        code = terrno;
        ST_TASK_ELOG("ext: OOM pushing calcColumns[%d]='%s'", i, colName);
        goto _err;
      }
    }
    ST_TASK_DLOG("ext: copied %d calcColumns into spec", nCalc);
  }

  /* Deep-copy pCalcMappings (SExtColTypeMapping[]) — aggregate column type info. */
  pInfo->spec.pCalcMappings   = NULL;
  pInfo->spec.numCalcMappings = 0;
  if (pExtSpec->numCalcMappings > 0 && pExtSpec->pCalcMappings != NULL) {
    pInfo->spec.pCalcMappings = (SExtColTypeMapping *)taosMemoryMalloc(
        pExtSpec->numCalcMappings * sizeof(SExtColTypeMapping));
    if (pInfo->spec.pCalcMappings == NULL) {
      code = terrno;
      ST_TASK_ELOG("%s", "ext: OOM copying pCalcMappings");
      goto _err;
    }
    memcpy(pInfo->spec.pCalcMappings, pExtSpec->pCalcMappings,
           pExtSpec->numCalcMappings * sizeof(SExtColTypeMapping));
    pInfo->spec.numCalcMappings = pExtSpec->numCalcMappings;
    ST_TASK_DLOG("ext: copied %d calcMappings into spec", pExtSpec->numCalcMappings);
  }

  /* Decrypt password — extDecryptPassword decrypts in-place into outPlain. */
  char plainPwd[TSDB_EXT_SOURCE_PASSWORD_LEN] = {0};
  extDecryptPassword((const char *)pExtSpec->encryptedPassword, plainPwd,
                     (int32_t)sizeof(plainPwd));

  /* Build connector config. */
  SExtSourceCfg cfg = {0};
  cfg.source_type = (EExtSourceType)pExtSpec->sourceType;
  tstrncpy(cfg.source_name, pExtSpec->sourceName, sizeof(cfg.source_name));
  tstrncpy(cfg.host, pExtSpec->host, sizeof(cfg.host));
  cfg.port = (int32_t)pExtSpec->port;
  tstrncpy(cfg.user, pExtSpec->user, sizeof(cfg.user));
  tstrncpy(cfg.password, plainPwd, sizeof(cfg.password));
  tstrncpy(cfg.default_database, pExtSpec->extDb, sizeof(cfg.default_database));
  tstrncpy(cfg.options, pExtSpec->options, sizeof(cfg.options));
  cfg.meta_version = (int64_t)pExtSpec->connCfgVersion;

  /* Wipe plaintext password from stack immediately after use. */
  memset(plainPwd, 0, sizeof(plainPwd));

  code = extConnectorOpen(&cfg, &pInfo->pConn);
  /* Wipe password copy in cfg as well. */
  memset(cfg.password, 0, sizeof(cfg.password));
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: extConnectorOpen failed code:%d source=%s host=%s port=%d",
             code, pExtSpec->sourceName, pExtSpec->host, (int)pExtSpec->port);
    goto _err;
  }
  ST_TASK_DLOG("ext: extConnectorOpen ok, source=%s", pExtSpec->sourceName);

  /* Init pUidIndex: hash<int64_t uid, SUidIndexEntry>. */
  pInfo->pUidIndex = tSimpleHashInit(64,
      taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (pInfo->pUidIndex == NULL) {
    code = terrno;
    ST_TASK_ELOG("%s", "ext: tSimpleHashInit(pUidIndex) failed");
    goto _err;
  }

  /* Init pGroupIndex: hash<int64_t groupId, SArray*>.  The value is a
   * pointer to an SArray; we register a free callback to handle cleanup. */
  pInfo->pGroupIndex = tSimpleHashInit(16,
      taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (pInfo->pGroupIndex == NULL) {
    code = terrno;
    ST_TASK_ELOG("%s", "ext: tSimpleHashInit(pGroupIndex) failed");
    goto _err;
  }
  /* NOTE: tSimpleHashSetFreeFp signature is void(*)(void*) where the void*
   * points to the stored VALUE bytes.  For pointer-valued hashes the callback
   * receives a pointer to the stored SArray*, so we must dereference inside. */
  tSimpleHashSetFreeFp(pInfo->pGroupIndex, freeGroupIndexValue);

  /* Init pTagsetIndex: variable-length key (tagset string), value int64_t uid.
   * InfluxDB only; still allocated for all source types to keep code uniform. */
  pInfo->pTagsetIndex = tSimpleHashInit(64,
      MurmurHash3_32);   /* variable-length string key → MurmurHash */
  if (pInfo->pTagsetIndex == NULL) {
    code = terrno;
    ST_TASK_ELOG("%s", "ext: tSimpleHashInit(pTagsetIndex) failed");
    goto _err;
  }

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

  /* Free prefilter deep copy (calc reader). */
  if (pInfo->spec.prefilter) {
    taosMemoryFree(pInfo->spec.prefilter);
    pInfo->spec.prefilter = NULL;
  }

  /* Free triggerPrefilter deep copy (trigger reader). */
  if (pInfo->spec.triggerPrefilter) {
    taosMemoryFree(pInfo->spec.triggerPrefilter);
    pInfo->spec.triggerPrefilter = NULL;
  }

  /* Free triggerColumns deep copy. */
  if (pInfo->spec.triggerColumns) {
    taosArrayDestroy(pInfo->spec.triggerColumns);
    pInfo->spec.triggerColumns = NULL;
  }

  /* Free partitionTagCols deep copy. */
  if (pInfo->spec.partitionTagCols) {
    taosArrayDestroy(pInfo->spec.partitionTagCols);
    pInfo->spec.partitionTagCols = NULL;
  }

  /* Free pColMappings deep copy. */
  if (pInfo->spec.pColMappings) {
    taosMemoryFree(pInfo->spec.pColMappings);
    pInfo->spec.pColMappings   = NULL;
    pInfo->spec.numColMappings = 0;
  }

  /* Free calcColumns deep copy. */
  if (pInfo->spec.calcColumns) {
    taosArrayDestroy(pInfo->spec.calcColumns);
    pInfo->spec.calcColumns = NULL;
  }

  /* Free pCalcMappings deep copy. */
  if (pInfo->spec.pCalcMappings) {
    taosMemoryFree(pInfo->spec.pCalcMappings);
    pInfo->spec.pCalcMappings   = NULL;
    pInfo->spec.numCalcMappings = 0;
  }

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

/* Insert one uid entry into all three hashes atomically.
 * uid == groupId for now (no separate groupId derivation needed). */
static int32_t extTableListInsertEntry(SStreamExtReaderInfo *pInfo,
                                       int64_t uid, int64_t groupId,
                                       const char *tagsetKey, int32_t tagsetKeyLen) {
  const SStreamTask *pTask = pInfo->pTask;

  /* 1. pUidIndex: uid → {groupId, tagsetKey} */
  SUidIndexEntry entry = {.groupId = groupId};
  if (tagsetKey != NULL && tagsetKeyLen > 0) {
    tstrncpy(entry.tagsetKey, tagsetKey,
             TMIN((int32_t)sizeof(entry.tagsetKey), tagsetKeyLen + 1));
  }
  int32_t code = tSimpleHashPut(pInfo->pUidIndex, &uid, sizeof(uid),
                                &entry, sizeof(entry));
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: initTableList pUidIndex put failed uid=%" PRId64 " code:%d",
                 uid, code);
    return code;
  }

  /* 2. pGroupIndex: groupId → SArray<uid> */
  SArray **ppArr = (SArray **)tSimpleHashGet(pInfo->pGroupIndex,
                                              &groupId, sizeof(groupId));
  if (ppArr == NULL) {
    SArray *pArr = taosArrayInit(4, sizeof(int64_t));
    if (pArr == NULL) {
      ST_TASK_ELOG("ext: initTableList pGroupIndex OOM gid=%" PRId64, groupId);
      return terrno;
    }
    code = tSimpleHashPut(pInfo->pGroupIndex, &groupId, sizeof(groupId),
                          &pArr, POINTER_BYTES);
    if (code != TSDB_CODE_SUCCESS) {
      taosArrayDestroy(pArr);
      ST_TASK_ELOG("ext: initTableList pGroupIndex put failed gid=%" PRId64, groupId);
      return code;
    }
    ppArr = (SArray **)tSimpleHashGet(pInfo->pGroupIndex, &groupId, sizeof(groupId));
  }
  if (taosArrayPush(*ppArr, &uid) == NULL) {
    ST_TASK_ELOG("ext: initTableList pGroupIndex array push OOM gid=%" PRId64, groupId);
    return terrno;
  }

  /* 3. pTagsetIndex: tagsetKey → uid  (skip for empty key on non-InfluxDB) */
  if (tagsetKey != NULL && tagsetKeyLen > 0) {
    code = tSimpleHashPut(pInfo->pTagsetIndex, tagsetKey, tagsetKeyLen,
                          &uid, sizeof(uid));
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("ext: initTableList pTagsetIndex put failed uid=%" PRId64, uid);
      return code;
    }
  }

  ST_TASK_DLOG("ext: initTableList inserted uid=%" PRId64 " gid=%" PRId64
               " tagset=\"%.*s\"", uid, groupId, tagsetKeyLen, tagsetKey ? tagsetKey : "");
  return TSDB_CODE_SUCCESS;
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
 * Returns the number of narrow bytes written (excl. NUL), or -1 on failure/
 * would-not-fit; outBuf is always NUL-terminated. */
static int32_t extColCellToStr(SColumnInfoData *pCol, int32_t row, char *outBuf, int32_t outBufLen) {
  if (outBufLen <= 0) return -1;
  outBuf[0]          = '\0';
  char       *pRaw   = colDataGetVarData(pCol, row);
  int32_t     rawLen = varDataLen(pRaw);
  const char *pVal    = varDataVal(pRaw);
  if (pCol->info.type == TSDB_DATA_TYPE_NCHAR) {
    /* taosUcs4ToMbs requires the output buffer to hold at least rawLen bytes
     * (the UCS-4 byte length), mirroring streamTriggerTask.c's usage. */
    if (rawLen >= outBufLen) return -1;
    int32_t len = taosUcs4ToMbs((TdUcs4 *)pVal, rawLen, outBuf, NULL);
    if (len < 0 || len >= outBufLen) {
      outBuf[0] = '\0';
      return -1;
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
    char nameBuf[TSDB_COL_NAME_LEN] = {0};
    if (extColCellToStr(pCol, r, nameBuf, sizeof(nameBuf)) < 0) continue;
    taosArrayPush(pTagCols, nameBuf);
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
  int64_t                   measurementGroupId;
  int32_t                  *partIdx; /* indices into pTagCols of the partition tags (mode 2) */
  int32_t                   nPart;   /* number of valid entries in partIdx */
} SInfluxDistinctCtx;

/* Register each distinct tag combination as a uid entry. */
static int32_t influxDistinctBlockCb(SSDataBlock *pBlock, void *pCtx) {
  SInfluxDistinctCtx       *pDCtx = (SInfluxDistinctCtx *)pCtx;
  SStreamExtReaderInfo *pInfo  = pDCtx->pInfo;
  const SStreamTask        *pTask  = pInfo->pTask;
  int32_t                   nTags  = pDCtx->nTags;
  int32_t                   code   = TSDB_CODE_SUCCESS;

  for (int32_t r = 0; r < pBlock->info.rows; r++) {
    char tagsetKey[EXT_TAGSET_KEY_MAX] = {0};
    const char *colNames[TSDB_MAX_TAGS] = {0};
    const char *colVals[TSDB_MAX_TAGS]  = {0};
    char        valBufs[TSDB_MAX_TAGS][EXT_INFLUX_TAG_NCHAR_CHARS];

    for (int32_t c = 0; c < nTags && c < TSDB_MAX_TAGS; c++) {
      colNames[c] = (const char *)taosArrayGet(pDCtx->pTagCols, c);
      SColumnInfoData *pCol = taosArrayGet(pBlock->pDataBlock, c);
      if (pCol && !colDataIsNull_s(pCol, r) &&
          extColCellToStr(pCol, r, valBufs[c], sizeof(valBufs[c])) >= 0) {
        colVals[c] = valBufs[c];
      } else {
        valBufs[c][0] = '\0';
        colVals[c]    = "";
      }
    }
    int32_t keyLen = buildTagsetKey(tagsetKey, sizeof(tagsetKey),
                                    colNames, colVals, nTags);
    if (keyLen < 0) {
      ST_TASK_WLOG("%s", "ext: initInfluxTagPartition tagset key truncated, skipping row");
      continue;
    }

    /* uid = MurmurHash3_64(full tagsetKey) | 1  (never 0); identifies the sub-table. */
    int64_t uid = (int64_t)(MurmurHash3_64(tagsetKey, keyLen) | 1ULL);

    /* groupId depends on the PARTITION BY spec (may be shared across uids). */
    int64_t groupId;
    if (pDCtx->groupMode == 1) {
      groupId = uid;                          /* PARTITION BY tbname: per sub-table */
    } else if (pDCtx->groupMode == 2) {
      /* PARTITION BY <tags>: hash only the partition-tag values, so sub-tables
       * that agree on those tags share one groupId. */
      const char *subNames[TSDB_MAX_TAGS] = {0};
      const char *subVals[TSDB_MAX_TAGS]  = {0};
      int32_t     ns           = 0;
      for (int32_t p = 0; p < pDCtx->nPart && ns < TSDB_MAX_TAGS; p++) {
        int32_t idx = pDCtx->partIdx[p];
        subNames[ns] = colNames[idx];
        subVals[ns]  = colVals[idx];
        ns++;
      }
      char    subKey[EXT_TAGSET_KEY_MAX] = {0};
      int32_t subLen = buildTagsetKey(subKey, sizeof(subKey), subNames, subVals, ns);
      if (subLen < 0) {
        ST_TASK_WLOG("%s", "ext: initInfluxTagPartition subset group key truncated, using measurement group");
        groupId = pDCtx->measurementGroupId;
      } else {
        groupId = (int64_t)(MurmurHash3_64(subKey, subLen) | 1ULL);
      }
    } else {
      groupId = pDCtx->measurementGroupId;    /* no PARTITION BY: one group */
    }

    /* Skip duplicates (hash collision or repeated rows). */
    if (tSimpleHashGet(pInfo->pUidIndex, &uid, sizeof(uid)) != NULL) continue;

    code = extTableListInsertEntry(pInfo, uid, groupId, tagsetKey, keyLen);
    if (code != TSDB_CODE_SUCCESS) return code;
    pDCtx->nGroups++;
  }
  blockDataDestroy(pBlock);
  return code;
}

/* InfluxDB: query information_schema.columns for tag column names,
 * then SELECT DISTINCT <tags> to enumerate live tag combinations.
 * Each combination → one uid/group entry. */
static int32_t extInitInfluxTagPartition(SStreamExtReaderInfo *pInfo) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t            code  = TSDB_CODE_SUCCESS;

  /* Step 1: discover tag columns from information_schema.
   * InfluxDB 3.x encodes tag columns as Arrow Dictionary type; the
   * data_type column reports 'Dictionary(Int32, Utf8)' for tag columns.
   * Use data_type LIKE 'Dictionary%' to identify them. */
  char schemaSQL[EXT_SQL_BUF_SIZE] = {0};
  snprintf(schemaSQL, sizeof(schemaSQL),
           "SELECT column_name FROM information_schema.columns "
           "WHERE table_name = '%s' AND data_type LIKE 'Dictionary%%' "
           "ORDER BY ordinal_position",
           pInfo->spec.extTable);
  ST_TASK_DLOG("ext: initInfluxTagPartition schema SQL=\"%s\"", schemaSQL);

  SExtColTypeMapping nameMapping = {0};
  /* column_name is a VARCHAR; map to NCHAR for safe string retrieval. */
  nameMapping.tdType.type  = TSDB_DATA_TYPE_NCHAR;
  nameMapping.tdType.bytes = TSDB_COL_NAME_LEN;
  tstrncpy(nameMapping.colName, "column_name", sizeof(nameMapping.colName));

  /* Collect tag column names via callback. */
  SArray *pTagCols = taosArrayInit(8, sizeof(char[TSDB_COL_NAME_LEN]));
  if (pTagCols == NULL) return terrno;

  code = extQueryExecForEach(pInfo->pConn, schemaSQL,
                             &nameMapping, 1,
                             influxSchemaBlockCb, pTagCols);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: initInfluxTagPartition schema query failed code:%d", code);
    taosArrayDestroy(pTagCols);
    return code;
  }

  int32_t nTags = (int32_t)taosArrayGetSize(pTagCols);
  ST_TASK_DLOG("ext: initInfluxTagPartition discovered %d tag columns", nTags);

  if (nTags == 0) {
    /* No tag columns — treat whole measurement as single group (same as
     * no-partition path). */
    taosArrayDestroy(pTagCols);
    goto single_group;
  }

  /* Step 2: SELECT DISTINCT <tag cols> to enumerate live combinations. */
  char    distSQL[EXT_SQL_BUF_SIZE] = {0};
  int32_t off = 0;
  bool    ok  = extSqlCat(distSQL, sizeof(distSQL), &off, "SELECT DISTINCT ");
  for (int32_t i = 0; ok && i < nTags; i++) {
    const char *cn = (const char *)taosArrayGet(pTagCols, i);
    ok = extSqlCat(distSQL, sizeof(distSQL), &off,
                   "%s%s", cn, (i + 1 < nTags) ? ", " : "");
  }
  if (ok) ok = extSqlCat(distSQL, sizeof(distSQL), &off, " FROM %s", pInfo->spec.extTable);
  if (!ok) {
    ST_TASK_ELOG("ext: initInfluxTagPartition distinct SQL exceeds %d bytes for %d tag cols",
                 (int32_t)sizeof(distSQL), nTags);
    taosArrayDestroy(pTagCols);
    return TSDB_CODE_OUT_OF_RANGE;
  }
  ST_TASK_DLOG("ext: initInfluxTagPartition distinct SQL=\"%s\"", distSQL);

  /* Build col-type mappings (all tag values treated as NCHAR strings). */
  SExtColTypeMapping *pMappings = taosMemoryCalloc(nTags, sizeof(SExtColTypeMapping));
  if (pMappings == NULL) {
    taosArrayDestroy(pTagCols);
    return terrno;
  }
  for (int32_t i = 0; i < nTags; i++) {
    pMappings[i].tdType.type  = TSDB_DATA_TYPE_NCHAR;
    pMappings[i].tdType.bytes = EXT_INFLUX_TAG_NCHAR_CHARS;
    tstrncpy(pMappings[i].colName,
             (const char *)taosArrayGet(pTagCols, i),
             sizeof(pMappings[i].colName));
  }

  /* Determine the groupId derivation mode and the measurement-level constant
   * group (used when there is no PARTITION BY, or as a fallback). */
  char    mkey[TSDB_TABLE_NAME_LEN * 2] = {0};
  int32_t mkeyLen = snprintf(mkey, sizeof(mkey), "%s.%s",
                             extTableQualifier(&pInfo->spec), pInfo->spec.extTable);
  int64_t measurementGroupId = (int64_t)(MurmurHash3_64(mkey, mkeyLen) | 1ULL);

  int8_t   groupMode = 0;    /* 0=single, 1=all-tags (tbname), 2=subset */
  int32_t  nPart     = 0;
  int32_t *partIdx   = NULL;
  if (pInfo->spec.partitionByTag) {
    int32_t nPartCols = pInfo->spec.partitionTagCols
                          ? (int32_t)taosArrayGetSize(pInfo->spec.partitionTagCols) : 0;
    if (nPartCols == 0) {
      groupMode = 1;         /* PARTITION BY tbname == group by all tags */
    } else {
      partIdx = taosMemoryCalloc(nPartCols, sizeof(int32_t));
      if (partIdx == NULL) {
        taosMemoryFree(pMappings);
        taosArrayDestroy(pTagCols);
        return terrno;
      }
      /* Map each partition tag column name to its index in the discovered tags. */
      for (int32_t p = 0; p < nPartCols; p++) {
        const char *pcol = (const char *)taosArrayGet(pInfo->spec.partitionTagCols, p);
        for (int32_t t = 0; t < nTags; t++) {
          if (strncmp(pcol, (const char *)taosArrayGet(pTagCols, t), TSDB_COL_NAME_LEN) == 0) {
            partIdx[nPart++] = t;
            break;
          }
        }
      }
      if (nPart == 0) {
        /* None of the partition tags matched a discovered tag: degrade to a
         * single measurement group rather than mis-derive groupId. */
        ST_TASK_WLOG("%s", "ext: initInfluxTagPartition partition tags not found among tags, single group");
        taosMemoryFree(partIdx);
        partIdx = NULL;
      } else {
        groupMode = 2;       /* PARTITION BY specific tags */
      }
    }
  }
  ST_TASK_DLOG("ext: initInfluxTagPartition groupMode=%d nPart=%d measurementGid=%" PRId64,
               (int)groupMode, nPart, measurementGroupId);

  /* Enumerate distinct tag combinations via callback. */
  SInfluxDistinctCtx dCtx = {
    .pInfo    = pInfo,
    .pTagCols = pTagCols,
    .nTags    = nTags,
    .nGroups  = 0,
    .groupMode          = groupMode,
    .measurementGroupId = measurementGroupId,
    .partIdx            = partIdx,
    .nPart              = nPart,
  };
  code = extQueryExecForEach(pInfo->pConn, distSQL,
                             pMappings, nTags,
                             influxDistinctBlockCb, &dCtx);
  taosMemoryFree(pMappings);
  taosMemoryFree(partIdx);

  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: initInfluxTagPartition distinct query failed code:%d", code);
    taosArrayDestroy(pTagCols);
    return code;
  }

  if (dCtx.nGroups > 0) {
    /* Save tag column names for use in batch PULL queries. */
    if (pInfo->pInfluxTagCols) taosArrayDestroy(pInfo->pInfluxTagCols);
    pInfo->pInfluxTagCols = pTagCols;   /* transfer ownership */
    ST_TASK_DLOG("ext: initInfluxTagPartition inserted %d tag-combination groups",
                 dCtx.nGroups);
    return code;
  }
  taosArrayDestroy(pTagCols);
  /* Fall through if no rows returned (empty measurement). */

single_group:
  {
    /* No tags or empty measurement: treat as single group. */
    char   key[TSDB_TABLE_NAME_LEN * 2] = {0};
    int32_t keyLen = snprintf(key, sizeof(key), "%s.%s",
                               extTableQualifier(&pInfo->spec), pInfo->spec.extTable);
    int64_t uid = (int64_t)(MurmurHash3_64(key, keyLen) | 1ULL);
    ST_TASK_DLOG("ext: initInfluxTagPartition no tag combos — single group uid=%" PRId64, uid);
    return extTableListInsertEntry(pInfo, uid, uid, "", 0);
  }
}

/* Build a stable uid key string for MySQL/PG (no tag concept). */
static int32_t extInitSingleGroup(SStreamExtReaderInfo *pInfo) {
  const SStreamTask *pTask = pInfo->pTask;
  char   key[TSDB_TABLE_NAME_LEN * 3] = {0};
  int32_t keyLen = snprintf(key, sizeof(key), "%s.%s.%s",
                             pInfo->spec.sourceName,
                             extTableQualifier(&pInfo->spec),
                             pInfo->spec.extTable);
  int64_t uid = (int64_t)(MurmurHash3_64(key, keyLen) | 1ULL);
  ST_TASK_DLOG("ext: initSingleGroup uid=%" PRId64 " key=\"%s\"", uid, key);
  return extTableListInsertEntry(pInfo, uid, uid, NULL, 0);
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
int32_t streamReaderExtHandlePull(SStreamExtReaderInfo *pInfo,
                                  int32_t pullType,
                                  const void *pReqVoid,
                                  void **ppRsp) {
  int32_t code = TSDB_CODE_SUCCESS;

  if (pInfo == NULL || ppRsp == NULL) {
    stError("ext: handlePull invalid args");
    return TSDB_CODE_INVALID_PARA;
  }

  const SStreamTask *pTask = pInfo->pTask;
  ST_TASK_DLOG("ext: streamReaderExtHandlePull enter pullType=%d pInfo=%p",
          pullType, pInfo);
  *ppRsp = NULL;

  const SSTriggerExtPullReq *pReq = (const SSTriggerExtPullReq *)pReqVoid;

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
      return code;
    }
  }

  /* Allocate response. */
  SSTriggerExtPullRsp *pRsp = allocPullRsp((ESTriggerPullType)pullType);
  if (pRsp == NULL) {
    ST_TASK_ELOG("%s", "ext: handlePull OOM allocating rsp");
    return terrno;
  }

  /* Dispatch to specific handler. */
  switch ((ESTriggerPullType)pullType) {
    case STRIGGER_PULL_LAST_TS_EXT:
      code = handleLastTsPull(pInfo, pReq, pRsp);
      break;
    case STRIGGER_PULL_META_EXT:
      code = handleMetaPull(pInfo, pReq, pRsp);
      break;
    case STRIGGER_PULL_DATA_EXT:
      code = handleDataPull(pInfo, pReq, pRsp);
      break;
    case STRIGGER_PULL_META_DATA_EXT:
      code = handleMetaDataPull(pInfo, pReq, pRsp);
      break;
    case STRIGGER_PULL_CALC_DATA_EXT:
      code = handleCalcDataPull(pInfo, pReq, pRsp);
      break;
    case STRIGGER_PULL_GROUP_COL_VALUE_EXT:
      code = handleGroupColValuePull(pInfo, pReq, pRsp);
      break;
    default:
      ST_TASK_ELOG("ext: handlePull unknown pullType=%d", pullType);
      code = TSDB_CODE_INVALID_PARA;
      break;
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

  char sqlBuf[EXT_SQL_BUF_SIZE] = {0};
  char prefilterBuf[EXT_SQL_CLAUSE_BUF_LEN] = {0};
  /* Trigger reader uses PRE_FILTER (triggerPrefilter), not the calc WHERE. */
  buildTriggerPrefilterClause(&pInfo->spec, prefilterBuf, sizeof(prefilterBuf));

  /* Build SQL: SELECT MAX(ts) AS ts FROM <table> [WHERE triggerPrefilter]
   * The AS alias ensures the result column name matches tsMapping.colName
   * for databases (e.g. InfluxDB 3.x DataFusion) that name aggregates
   * as "MAX(ts)" rather than the bare column name. */
  char tblRef[TSDB_TABLE_NAME_LEN + TSDB_DB_NAME_LEN + 2] = {0};
  buildExtTableRef(&pInfo->spec, tblRef, sizeof(tblRef));
  int32_t off = snprintf(sqlBuf, sizeof(sqlBuf),
                         "SELECT MAX(%s) AS %s FROM %s",
                         tsCol, tsCol, tblRef);
  if (prefilterBuf[0] != '\0') {
    off += snprintf(sqlBuf + off, sizeof(sqlBuf) - off,
                    " WHERE %s", prefilterBuf);
    /* Strip trailing " AND " added by buildPrefilterClause. */
    int32_t plen = strlen(prefilterBuf);
    if (plen >= 5 && strcmp(prefilterBuf + plen - 5, " AND ") == 0) {
      sqlBuf[off - 5] = '\0';
    }
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
  int32_t code = extQueryExecFetchAll(pInfo->pConn, sqlBuf,
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
    int64_t        uid   = *(int64_t *)tSimpleHashGetKey(pVal, &kLen);
    SUidIndexEntry *pEnt = (SUidIndexEntry *)pVal;
    SExtLastTsInfo info  = {
      .uid = uid,
      .gid = pEnt->groupId,
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
                                  int64_t groupId, int64_t skey, int64_t ekey,
                                  int64_t uid, int64_t rows) {
  int32_t row = pBlock->info.rows;
  /* Ensure capacity. */
  int32_t code = blockDataEnsureCapacity(pBlock, row + 1);
  if (code != TSDB_CODE_SUCCESS) return code;

  int64_t vals[META_COL_COUNT] = {groupId, skey, ekey, uid, rows};
  for (int32_t c = 0; c < META_COL_COUNT; c++) {
    SColumnInfoData *pCol = taosArrayGet(pBlock->pDataBlock, c);
    code = colDataSetVal(pCol, row, (const char *)&vals[c], false);
    if (code != TSDB_CODE_SUCCESS) return code;
  }
  pBlock->info.rows = row + 1;
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
  int32_t code         = TSDB_CODE_SUCCESS;
  int32_t totalFetched = 0;

  /* Trigger reader uses PRE_FILTER (triggerPrefilter), not the calc WHERE.
   * Build the prefilter clause once; invariant across all uids. */
  char prefilterBuf[EXT_SQL_CLAUSE_BUF_LEN] = {0};
  buildTriggerPrefilterClause(&pInfo->spec, prefilterBuf, sizeof(prefilterBuf));

  char tblRef[TSDB_TABLE_NAME_LEN + TSDB_DB_NAME_LEN + 2] = {0};
  buildExtTableRef(&pInfo->spec, tblRef, sizeof(tblRef));

  /* All reader->trigger timestamps are in nanoseconds. The watermark (maxTs)
   * from triggerSideUidMaxTs is therefore in ns; use NANO for datetime
   * conversion and for the tsPrecision hint to the connector. */

  for (int32_t i = 0; i < totalUids; i++) {
    int64_t        uid = *(int64_t *)taosArrayGet(pAllUids, i);
    SUidIndexEntry *ent = tSimpleHashGet(pInfo->pUidIndex, &uid, sizeof(uid));
    if (ent == NULL) continue;

    char sqlBuf[EXT_SQL_BUF_SIZE] = {0};
    /* Look up the watermark from the trigger-provided map; default to INT64_MIN
     * (fetch everything) when no watermark exists yet for this uid. */
    int64_t maxTs = INT64_MIN;
    if (pUidMaxTs != NULL) {
      int64_t *pMaxTs = (int64_t *)tSimpleHashGet(pUidMaxTs, &uid, sizeof(uid));
      if (pMaxTs != NULL) maxTs = *pMaxTs;
    }
    /* MySQL / PostgreSQL: WHERE ts > 'YYYY-MM-DD HH:MM:SS.uuuuuu'
     * maxTs is in ns (uniform reader->trigger precision). */
    char dtBuf[32] = {0};
    epochToDatetimeStr(maxTs, TSDB_TIME_PRECISION_MICRO, dtBuf, sizeof(dtBuf));
    snprintf(sqlBuf, sizeof(sqlBuf),
              "SELECT MIN(%s),MAX(%s),%" PRId64 ",COUNT(*) FROM %s "
              "WHERE %s%s > %s",
              tsCol, tsCol, uid,
              tblRef,
              prefilterBuf,
              tsCol, dtBuf);
    
    ST_TASK_DLOG("ext: meta relational uid=%" PRId64 " sql=\"%.120s\"", uid, sqlBuf);

    /* Provide explicit column type mappings so the connector populates
     * pDataBlock: col0=MIN(ts) BIGINT, col1=MAX(ts) BIGINT,
     *             col2=uid literal BIGINT, col3=COUNT(*) BIGINT. */
    SExtColTypeMapping metaMappings[4] = {0};
    metaMappings[0].tdType.type  = TSDB_DATA_TYPE_TIMESTAMP;
    metaMappings[0].tdType.bytes = sizeof(int64_t);
    tstrncpy(metaMappings[0].colName, tsCol, sizeof(metaMappings[0].colName));
    metaMappings[1].tdType.type  = TSDB_DATA_TYPE_TIMESTAMP;
    metaMappings[1].tdType.bytes = sizeof(int64_t);
    tstrncpy(metaMappings[1].colName, tsCol, sizeof(metaMappings[1].colName));
    metaMappings[2].tdType.type  = TSDB_DATA_TYPE_BIGINT;
    metaMappings[2].tdType.bytes = sizeof(int64_t);
    tstrncpy(metaMappings[2].colName, "uid", sizeof(metaMappings[2].colName));
    metaMappings[3].tdType.type  = TSDB_DATA_TYPE_BIGINT;
    metaMappings[3].tdType.bytes = sizeof(int64_t);
    tstrncpy(metaMappings[3].colName, "cnt", sizeof(metaMappings[3].colName));

    SSDataBlock *pBlock = NULL;
    code = extQueryExecFetchAll(pInfo->pConn, sqlBuf,
                                metaMappings, 4, &pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("ext: meta relational query uid=%" PRId64 " code:%d",
              uid, code);
      break;
    }
    if (pBlock == NULL || pBlock->info.rows == 0) {
      if (pBlock) blockDataDestroy(pBlock);
      ST_TASK_DLOG("ext: meta relational uid=%" PRId64 " no rows", uid);
      continue;
    }

    /* Extract MIN(ts), MAX(ts), cnt from first row. */
    if (taosArrayGetSize(pBlock->pDataBlock) < 4) {
      ST_TASK_WLOG("ext: meta relational uid=%" PRId64 " block cols=%d < 4, skip",
              uid, (int)taosArrayGetSize(pBlock->pDataBlock));
      blockDataDestroy(pBlock);
      continue;
    }
    SColumnInfoData *colMin = taosArrayGet(pBlock->pDataBlock, 0);
    SColumnInfoData *colMax = taosArrayGet(pBlock->pDataBlock, 1);
    SColumnInfoData *colCnt = taosArrayGet(pBlock->pDataBlock, 3);

    if (colMin == NULL || colMax == NULL ||
        colDataIsNull_s(colMin, 0) || colDataIsNull_s(colMax, 0)) {
      blockDataDestroy(pBlock);
      continue;
    }

    int64_t skey = *(int64_t *)colDataGetData(colMin, 0);
    int64_t ekey = *(int64_t *)colDataGetData(colMax, 0);
    int64_t cnt  = colCnt ? *(int64_t *)colDataGetData(colCnt, 0) : 0;

    printDataBlock(pBlock, __func__, "ext_meta_relational_raw", pTask->streamId);

    code = metaBlockAppendRow(pRsp->pMetaBlock, ent->groupId, skey, ekey, uid, cnt);
    blockDataDestroy(pBlock);
    if (code != TSDB_CODE_SUCCESS) break;

    totalFetched += (int32_t)cnt;
    if (totalFetched >= STREAM_RETURN_ROWS_NUM) {
      ST_TASK_DLOG("%s", "ext: meta relational hit row threshold, returning");
      break;
    }
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

/* Build the OR-compound SQL for one batch of uids.
 * Returns true if at least one uid was written into sqlBuf; false if all
 * uids in the batch had missing index entries and the batch should be skipped. */
/* Build the OR-compound SQL for a batch of uids starting at batchStart.
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
  int32_t off = 0;
  *pAnyUid = false;

  /* SELECT clause: ts, tag1, tag2, ... */
  bool ok = extSqlCat(sqlBuf, sqlBufLen, &off, "SELECT %s", tsCol);
  for (int32_t c = 0; ok && c < nTags; c++) {
    const char *cn = (const char *)taosArrayGet(pInfo->pInfluxTagCols, c);
    ok = extSqlCat(sqlBuf, sqlBufLen, &off, ", %s", cn);
  }
  if (ok) ok = extSqlCat(sqlBuf, sqlBufLen, &off, " FROM %s WHERE ", pInfo->spec.extTable);
  if (!ok) {
    /* The SELECT/FROM header alone overflows the buffer — no query is possible.
     * Consume the whole batch so the caller makes forward progress (it logs the
     * empty batch and moves on). */
    return batchSize;
  }

  /* WHERE clause: OR of per-uid tag conditions. */
  bool    anyUid = false;
  int32_t k      = 0;
  for (; k < batchSize; k++) {
    int64_t        uid = *(int64_t *)taosArrayGet(pAllUids, batchStart + k);
    SUidIndexEntry *ent = tSimpleHashGet(pInfo->pUidIndex, &uid, sizeof(uid));
    if (ent == NULL) continue;  /* missing index entry: consume, emit no clause */

    char tagWhere[EXT_SQL_BUF_SIZE / 4] = {0};
    if (nTags > 0) {
      int32_t twLen = buildInfluxTagWhereClause(ent->tagsetKey,
                                                tagWhere, sizeof(tagWhere));
      if (twLen < 0) continue; /* tagset key too long, skip uid */
    }

    /* Look up watermark from trigger-provided map; INT64_MIN means fetch all. */
    int64_t maxTs = INT64_MIN;
    if (pUidMaxTs != NULL) {
      int64_t *pMaxTs = (int64_t *)tSimpleHashGet(pUidMaxTs, &uid, sizeof(uid));
      if (pMaxTs != NULL) maxTs = *pMaxTs;
    }

    /* Tentatively append this uid's clause; revert atomically if it overflows. */
    int32_t saveOff   = off;
    bool    clauseOk  = true;
    if (anyUid) clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off, " OR ");
    if (clauseOk) clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off, "(");
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
                           "%s > to_timestamp_nanos(%" PRId64 "))", tsCol, maxTs);
    }

    if (!clauseOk) {
      /* This uid's clause does not fit. Revert to the end of the previous uid's
       * clause and stop here; the caller re-batches uid k and the remainder. */
      off = saveOff;
      sqlBuf[saveOff] = '\0';
      if (!anyUid) {
        /* A single uid clause exceeds the entire buffer (pathological). Consume
         * just this uid so the caller still makes progress; *pAnyUid stays false
         * and the batch is skipped. */
        k++;
      }
      break;
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

/* Per-block callback: accumulate ts min/max/cnt into pAgg slots. */
static int32_t influxAccumBlockCb(SSDataBlock *pBlk, void *pCtx) {
  SInfluxAccumCtx          *pACtx = (SInfluxAccumCtx *)pCtx;
  SStreamExtReaderInfo *pInfo  = pACtx->pInfo;
  const SStreamTask        *pTask  = pInfo->pTask;

  for (int32_t r = 0; r < pBlk->info.rows; r++) {
    /* Extract ts (col 0). */
    SColumnInfoData *pTsCol = taosArrayGet(pBlk->pDataBlock, 0);
    if (pTsCol == NULL || colDataIsNull_s(pTsCol, r)) continue;
    int64_t ts = *(int64_t *)colDataGetData(pTsCol, r);

    /* Rebuild tagset key from tag columns to look up uid. */
    char        tagsetKey[EXT_TAGSET_KEY_MAX] = {0};
    const char *colNames[TSDB_MAX_TAGS]                  = {0};
    const char *colVals[TSDB_MAX_TAGS]                   = {0};
    char        valBufs[TSDB_MAX_TAGS][EXT_INFLUX_TAG_NCHAR_CHARS];

    for (int32_t c = 0; c < pACtx->nTags && c < TSDB_MAX_TAGS; c++) {
      colNames[c] = (const char *)taosArrayGet(pInfo->pInfluxTagCols, c);
      SColumnInfoData *pTagCol = taosArrayGet(pBlk->pDataBlock, 1 + c);
      if (pTagCol && !colDataIsNull_s(pTagCol, r) &&
          extColCellToStr(pTagCol, r, valBufs[c], sizeof(valBufs[c])) >= 0) {
        colVals[c] = valBufs[c];
      } else {
        valBufs[c][0] = '\0';
        colVals[c]    = "";
      }
    }

    /* Resolve uid and slot index.
     * When nTags == 0 (single-group, no PARTITION BY), the tagset index has no
     * entry (empty key was not inserted), so resolve directly from pAllUids. */
    int32_t slot = -1;
    int64_t uid  = 0;
    if (pACtx->nTags == 0) {
      /* Single-group: only one uid in the batch; slot is always 0. */
      uid  = *(int64_t *)taosArrayGet(pACtx->pAllUids, pACtx->batchStart);
      slot = 0;
    } else {
      int32_t keyLen = buildTagsetKey(tagsetKey, sizeof(tagsetKey),
                                      colNames, colVals, pACtx->nTags);
      if (keyLen < 0) continue;

      /* Resolve uid via tagset index. */
      int64_t *pUid = (int64_t *)tSimpleHashGet(pInfo->pTagsetIndex,
                                                 tagsetKey, keyLen);
      if (pUid == NULL) continue;
      uid = *pUid;

      /* Find slot in this batch. */
      for (int32_t k = 0; k < pACtx->batchSize; k++) {
        if (*(int64_t *)taosArrayGet(pACtx->pAllUids, pACtx->batchStart + k) == uid) {
          slot = k;
          break;
        }
      }
      if (slot < 0) continue;
    }

    if (ts < pACtx->pAgg[slot].skey) pACtx->pAgg[slot].skey = ts;
    if (ts > pACtx->pAgg[slot].ekey) pACtx->pAgg[slot].ekey = ts;
    pACtx->pAgg[slot].cnt++;
    pACtx->pAgg[slot].seen = true;
  }
  blockDataDestroy(pBlk);
  return TSDB_CODE_SUCCESS;
}

/* ============================================================
 * handleMetaPullInflux — InfluxDB meta pull (N=64 uid batch loop).
 *
 * One query per batch of N uids using OR-compound WHERE clauses (DS §6.1.5).
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

  /* Trigger reader uses PRE_FILTER (triggerPrefilter), not the calc WHERE.
   * Build once; invariant across all uid batches. */
  char prefilterBuf[EXT_SQL_CLAUSE_BUF_LEN] = {0};
  buildTriggerPrefilterClause(&pInfo->spec, prefilterBuf, sizeof(prefilterBuf));

  /* Col-type mappings: ts (TIMESTAMP) + nTags × NCHAR(EXT_INFLUX_TAG_NCHAR_CHARS).
   * Use TIMESTAMP (not BIGINT) so the connector correctly converts the Arrow
   * Timestamp(Nanosecond) column to an int64 nanosecond value; BIGINT would
   * receive the raw Arrow integer representation which may be wrong. */
  int32_t nCols = 1 + nTags;
  SExtColTypeMapping *pMappings = taosMemoryCalloc(nCols, sizeof(SExtColTypeMapping));
  if (pMappings == NULL) {
    code = terrno;
    goto _done;
  }
  pMappings[0].tdType.type  = TSDB_DATA_TYPE_TIMESTAMP;
  pMappings[0].tdType.bytes = sizeof(int64_t);
  tstrncpy(pMappings[0].colName, tsCol, sizeof(pMappings[0].colName));
  for (int32_t c = 0; c < nTags; c++) {
    pMappings[1 + c].tdType.type  = TSDB_DATA_TYPE_NCHAR;
    pMappings[1 + c].tdType.bytes = EXT_INFLUX_TAG_NCHAR_CHARS;
    tstrncpy(pMappings[1 + c].colName,
             (const char *)taosArrayGet(pInfo->pInfluxTagCols, c),
             sizeof(pMappings[1 + c].colName));
  }

  while (batchStart < totalUids) {
    int32_t batchEnd  = TMIN(batchStart + EXT_INFLUX_UID_BATCH_SIZE, totalUids);
    int32_t batchSize = batchEnd - batchStart;
    ST_TASK_DLOG("ext: influx meta batch %d uids=%d [%d..%d)",
            batchIdx, batchSize, batchStart, batchEnd);

    /* Build OR-compound SQL for this batch. buildInfluxBatchSql returns how many
     * uids it actually consumed: fewer than batchSize if a uid's clause would
     * have overflowed sqlBuf, in which case the remainder is re-batched next
     * iteration (no uid is dropped, no out-of-bounds write). */
    char    sqlBuf[EXT_SQL_BUF_SIZE] = {0};
    bool    anyUid   = false;
    int32_t consumed = buildInfluxBatchSql(pInfo, pAllUids, batchStart, batchSize,
                                           tsCol, prefilterBuf, nTags, pUidMaxTs,
                                           sqlBuf, sizeof(sqlBuf), &anyUid);
    if (consumed <= 0) consumed = batchSize;  /* safety: always make progress */
    if (!anyUid) {
      ST_TASK_DLOG("ext: influx meta batch %d: all uids missing, skip", batchIdx);
      batchStart += consumed;
      batchIdx++;
      continue;
    }
    ST_TASK_DLOG("ext: influx meta batch sql=\"%.200s\"", sqlBuf);

    /* Allocate per-uid accumulators (one slot per consumed uid). */
    SInfluxUidAgg *pAgg = taosMemoryCalloc(consumed, sizeof(SInfluxUidAgg));
    if (pAgg == NULL) {
      code = terrno;
      break;
    }
    for (int32_t k = 0; k < consumed; k++) {
      pAgg[k].skey = INT64_MAX;
      pAgg[k].ekey = INT64_MIN;
    }

    /* Execute query and accumulate rows via callback. */
    SInfluxAccumCtx aCtx = {
      .pInfo      = pInfo,
      .pAllUids   = pAllUids,
      .batchStart = batchStart,
      .batchSize  = consumed,
      .nTags      = nTags,
      .pAgg       = pAgg,
    };
    code = extQueryExecForEach(pInfo->pConn, sqlBuf,
                               pMappings, nCols,
                               influxAccumBlockCb, &aCtx);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("ext: influx meta batch %d query failed code:%d",
              batchIdx, code);
      taosMemoryFree(pAgg);
      break;
    }

    /* Flush aggregates into metaBlock. */
    for (int32_t k = 0; k < consumed && code == TSDB_CODE_SUCCESS; k++) {
      if (!pAgg[k].seen) continue;

      int64_t        uid = *(int64_t *)taosArrayGet(pAllUids, batchStart + k);
      SUidIndexEntry *ent = tSimpleHashGet(pInfo->pUidIndex, &uid, sizeof(uid));
      if (ent == NULL) continue;

      code = metaBlockAppendRow(pRsp->pMetaBlock, ent->groupId,
                                pAgg[k].skey, pAgg[k].ekey,
                                uid, pAgg[k].cnt);
      if (code != TSDB_CODE_SUCCESS) break;

      totalFetched += (int32_t)pAgg[k].cnt;
      if (totalFetched >= STREAM_RETURN_ROWS_NUM) {
        ST_TASK_DLOG("ext: influx meta batch %d hit row threshold uid=%" PRId64,
                batchIdx, uid);
      }
    }
    taosMemoryFree(pAgg);

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
  int32_t code       = TSDB_CODE_SUCCESS;
  int32_t nTags      = pInfo->pInfluxTagCols
                         ? (int32_t)taosArrayGetSize(pInfo->pInfluxTagCols) : 0;
  int32_t batchStart = 0;
  int32_t batchIdx   = 0;

  /* Trigger reader uses PRE_FILTER (triggerPrefilter), not the calc WHERE.
   * Build once; invariant across all uid batches. */
  char prefilterBuf[EXT_SQL_CLAUSE_BUF_LEN] = {0};
  buildTriggerPrefilterClause(&pInfo->spec, prefilterBuf, sizeof(prefilterBuf));

  /* Col-type mappings: ts (TIMESTAMP) + nTags × NCHAR(EXT_INFLUX_TAG_NCHAR_CHARS). */
  int32_t nCols = 1 + nTags;
  SExtColTypeMapping *pMappings = taosMemoryCalloc(nCols, sizeof(SExtColTypeMapping));
  if (pMappings == NULL) return terrno;
  pMappings[0].tdType.type  = TSDB_DATA_TYPE_TIMESTAMP;
  pMappings[0].tdType.bytes = sizeof(int64_t);
  tstrncpy(pMappings[0].colName, tsCol, sizeof(pMappings[0].colName));
  for (int32_t c = 0; c < nTags; c++) {
    pMappings[1 + c].tdType.type  = TSDB_DATA_TYPE_NCHAR;
    pMappings[1 + c].tdType.bytes = EXT_INFLUX_TAG_NCHAR_CHARS;
    tstrncpy(pMappings[1 + c].colName,
             (const char *)taosArrayGet(pInfo->pInfluxTagCols, c),
             sizeof(pMappings[1 + c].colName));
  }

  while (batchStart < totalUids) {
    int32_t batchEnd  = TMIN(batchStart + EXT_INFLUX_UID_BATCH_SIZE, totalUids);
    int32_t batchSize = batchEnd - batchStart;
    ST_TASK_DLOG("ext: influx lastTs batch %d uids=%d [%d..%d)",
                 batchIdx, batchSize, batchStart, batchEnd);

    /* Build OR-compound SQL for this batch; pUidMaxTs=NULL means no watermark
     * (fetch all history) since LAST_TS wants the true per-uid max. */
    char    sqlBuf[EXT_SQL_BUF_SIZE] = {0};
    bool    anyUid   = false;
    int32_t consumed = buildInfluxBatchSql(pInfo, pAllUids, batchStart, batchSize,
                                           tsCol, prefilterBuf, nTags, NULL,
                                           sqlBuf, sizeof(sqlBuf), &anyUid);
    if (consumed <= 0) consumed = batchSize;  /* safety: always make progress */

    /* Per-uid accumulators; uids never matching a row keep seen=false and
     * are reported as ts=INT64_MIN below. */
    SInfluxUidAgg *pAgg = taosMemoryCalloc(consumed, sizeof(SInfluxUidAgg));
    if (pAgg == NULL) { code = terrno; break; }
    for (int32_t k = 0; k < consumed; k++) {
      pAgg[k].skey = INT64_MAX;
      pAgg[k].ekey = INT64_MIN;
    }

    if (anyUid) {
      ST_TASK_DLOG("ext: influx lastTs batch sql=\"%.200s\"", sqlBuf);
      SInfluxAccumCtx aCtx = {
        .pInfo      = pInfo,
        .pAllUids   = pAllUids,
        .batchStart = batchStart,
        .batchSize  = consumed,
        .nTags      = nTags,
        .pAgg       = pAgg,
      };
      code = extQueryExecForEach(pInfo->pConn, sqlBuf, pMappings, nCols,
                                 influxAccumBlockCb, &aCtx);
      if (code != TSDB_CODE_SUCCESS) {
        ST_TASK_ELOG("ext: influx lastTs batch %d query failed code:%d", batchIdx, code);
        taosMemoryFree(pAgg);
        break;
      }
    } else {
      ST_TASK_DLOG("ext: influx lastTs batch %d: all uids missing index entry", batchIdx);
    }

    /* Emit one SExtLastTsInfo per uid in this batch, seen or not. */
    for (int32_t k = 0; k < consumed; k++) {
      int64_t         uid = *(int64_t *)taosArrayGet(pAllUids, batchStart + k);
      SUidIndexEntry *ent = tSimpleHashGet(pInfo->pUidIndex, &uid, sizeof(uid));
      if (ent == NULL) continue;
      SExtLastTsInfo info = {
        .uid = uid,
        .gid = ent->groupId,
        .ts  = pAgg[k].seen ? pAgg[k].ekey : INT64_MIN,
      };
      taosArrayPush(pLastTsArr, &info);
    }
    taosMemoryFree(pAgg);

    if (code != TSDB_CODE_SUCCESS) break;
    batchStart += consumed;
    batchIdx++;
  }

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
/* prefilterBuf must already be built by the caller:
 *   handleDataPull     → buildTriggerPrefilterClause (trigger data, PRE_FILTER)
 *   handleCalcDataPull → buildTriggerPrefilterClause (trigger-side data for calc, PRE_FILTER) */
static int32_t fetchDataForUid(SStreamExtReaderInfo *pInfo,
                               const char *colList,
                               int64_t uid, int64_t skey, int64_t ekey,
                               bool isInflux,
                               const char *prefilterBuf,
                               const SExtColTypeMapping *pMappings,
                               int32_t nMappings,
                               SSDataBlock **ppBlock) {
  char sqlBuf[EXT_SQL_BUF_SIZE] = {0};

  const char *tsCol = (pInfo->spec.tsColumn[0] != '\0')
                        ? pInfo->spec.tsColumn : "ts";

  char tblRef[TSDB_TABLE_NAME_LEN + TSDB_DB_NAME_LEN + 2] = {0};
  buildExtTableRef(&pInfo->spec, tblRef, sizeof(tblRef));

  if (!isInflux) {
    /* MySQL / PostgreSQL: no per-uid column filter; time window only.
     * skey/ekey are in ns (uniform reader->trigger precision); convert to
     * quoted DATETIME literals for the MySQL/PG WHERE clause. */
    char skeyDt[32] = {0};
    char ekeyDt[32] = {0};
    epochToDatetimeStr(skey, TSDB_TIME_PRECISION_MICRO, skeyDt, sizeof(skeyDt));
    epochToDatetimeStr(ekey, TSDB_TIME_PRECISION_MICRO, ekeyDt, sizeof(ekeyDt));
    snprintf(sqlBuf, sizeof(sqlBuf),
             "SELECT %s FROM %s WHERE %s%s >= %s AND %s <= %s",
             colList,
             tblRef,
             prefilterBuf,
             tsCol, skeyDt, tsCol, ekeyDt);
  } else {
    /* InfluxDB: filter by tag values derived from ent->tagsetKey. */
    SUidIndexEntry *ent = tSimpleHashGet(pInfo->pUidIndex, &uid, sizeof(uid));
    char tagWhere[EXT_SQL_BUF_SIZE / 2] = {0};
    if (ent != NULL && ent->tagsetKey[0] != '\0') {
      int32_t twLen = buildInfluxTagWhereClause(ent->tagsetKey,
                                                tagWhere, sizeof(tagWhere));
      if (twLen < 0) tagWhere[0] = '\0'; /* truncation: skip tag filter */
    }

    /* DataFusion (InfluxDB 3.x): use to_timestamp_nanos() for ns integers;
     * CAST(ns AS TIMESTAMP) would treat them as seconds and overflow. */
    if (tagWhere[0] != '\0' && prefilterBuf[0] != '\0') {
      snprintf(sqlBuf, sizeof(sqlBuf),
               "SELECT %s FROM %s WHERE %s%s AND %s >= to_timestamp_nanos(%" PRId64 ")"
               " AND %s <= to_timestamp_nanos(%" PRId64 ")",
               colList,
               tblRef,
               prefilterBuf, tagWhere,
               tsCol, skey, tsCol, ekey);
    } else if (tagWhere[0] != '\0') {
      snprintf(sqlBuf, sizeof(sqlBuf),
               "SELECT %s FROM %s WHERE %s AND %s >= to_timestamp_nanos(%" PRId64 ")"
               " AND %s <= to_timestamp_nanos(%" PRId64 ")",
               colList,
               tblRef,
               tagWhere,
               tsCol, skey, tsCol, ekey);
    } else {
      /* No tag filter (single-group or tagsetKey empty): time window only. */
      snprintf(sqlBuf, sizeof(sqlBuf),
               "SELECT %s FROM %s WHERE %s%s >= to_timestamp_nanos(%" PRId64 ")"
               " AND %s <= to_timestamp_nanos(%" PRId64 ")",
               colList,
               tblRef,
               prefilterBuf,
               tsCol, skey, tsCol, ekey);
    }
  }
  const SStreamTask *pTask = pInfo->pTask;
  ST_TASK_DLOG("ext: fetchDataForUid uid=%" PRId64 " sql=\"%.180s\"", uid, sqlBuf);

  /* Use caller-supplied mappings when provided (non-NULL, non-zero).
   * Callers are responsible for passing the correct mapping set:
   *   handleDataPull      → pInfo->spec.pColMappings (trigger cols)
   *   handleCalcDataPull  → pInfo->spec.pCalcMappings (aggregate-input cols)
   * When NULL/0, extConnectorFetchBlock skips column-type enforcement and
   * returns the raw result columns as-is (used when no mapping is available). */
  const SExtColTypeMapping *pEffMappings = (pMappings != NULL && nMappings > 0)
                                               ? pMappings
                                               : NULL;
  int32_t nEffMappings = (pMappings != NULL && nMappings > 0)
                             ? nMappings
                             : 0;
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
      /* Bounds-safe append truncated (no OOB write); the resulting SELECT would
       * be malformed and fail cleanly rather than corrupting the stack. */
      stWarn("ext: triggerColumns list truncated at col %d/%d (bufLen=%d)", i, n, bufLen);
      break;
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
      /* Bounds-safe append truncated (no OOB write); the resulting SELECT would
       * be malformed and fail cleanly rather than corrupting the stack. */
      stWarn("ext: calcColumns list truncated at col %d/%d (bufLen=%d)", i, n, bufLen);
      break;
    }
  }
  return n;
}

/* ============================================================
 * Build indexHash and dataBlock from a list of per-uid blocks.
 * pUidBlocks: SArray<struct{int64_t uid; SSDataBlock* pBlock}>
 * ============================================================ */
typedef struct {
  int64_t      uid;
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
 * Instead of one external query per uid, issue one OR-compound query per batch
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
  SSHashObj            *pUidBlk; /* hash<int64_t uid, SSDataBlock*>; owns the blocks */
  int32_t               code;    /* sticky error captured inside the callback */
} SInfluxDataDemuxCtx;

/* Append data columns [0, nData) of source row r into the per-uid block,
 * creating the block (schema cloned from the source) on first use. */
static int32_t influxDemuxAppendRow(SInfluxDataDemuxCtx *pCtx, SSDataBlock *pSrc,
                                    int32_t r, int64_t uid) {
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

  int32_t nt = pCtx->nTags < TSDB_MAX_TAGS ? pCtx->nTags : TSDB_MAX_TAGS;
  for (int32_t r = 0; r < pBlk->info.rows && pCtx->code == TSDB_CODE_SUCCESS; r++) {
    char        tagsetKey[EXT_TAGSET_KEY_MAX] = {0};
    const char *colNames[TSDB_MAX_TAGS]                  = {0};
    const char *colVals[TSDB_MAX_TAGS]                   = {0};
    char        valBufs[TSDB_MAX_TAGS][EXT_INFLUX_TAG_NCHAR_CHARS];

    for (int32_t c = 0; c < nt; c++) {
      colNames[c] = (const char *)taosArrayGet(pInfo->pInfluxTagCols, c);
      SColumnInfoData *pTagCol = taosArrayGet(pBlk->pDataBlock, pCtx->nData + c);
      if (pTagCol && !colDataIsNull_s(pTagCol, r) &&
          extColCellToStr(pTagCol, r, valBufs[c], sizeof(valBufs[c])) >= 0) {
        colVals[c] = valBufs[c];
      } else {
        valBufs[c][0] = '\0';
        colVals[c]    = "";
      }
    }

    int32_t keyLen = buildTagsetKey(tagsetKey, sizeof(tagsetKey), colNames, colVals, nt);
    if (keyLen < 0) continue;
    int64_t *pUid = (int64_t *)tSimpleHashGet(pInfo->pTagsetIndex, tagsetKey, keyLen);
    if (pUid == NULL) continue; /* row's tagset not in this stream's partition set */

    pCtx->code = influxDemuxAppendRow(pCtx, pBlk, r, *pUid);
  }

  blockDataDestroy(pBlk);
  return pCtx->code;
}

static int32_t fetchDataBatchInflux(SStreamExtReaderInfo       *pInfo,
                                    const char                 *colList,
                                    const SExtColTypeMapping   *pMappings,
                                    int32_t                     nMappings,
                                    int32_t                     nTags,
                                    const char                 *prefilterBuf,
                                    SSHashObj                  *pUidWindow,
                                    SArray                     *pUidBlocks) {
  const SStreamTask  *pTask      = pInfo->pTask;
  int32_t             code       = TSDB_CODE_SUCCESS;
  SExtColTypeMapping *pMapAll    = NULL;
  SSHashObj          *pUidBlk    = NULL;
  SArray             *pUids      = NULL;
  SArray             *pWins      = NULL;
  int32_t             nColsAll   = 0;
  int32_t             batchStart = 0;
  SInfluxDataDemuxCtx ctx        = {0};

  const char *tsCol = (pInfo->spec.tsColumn[0] != '\0') ? pInfo->spec.tsColumn : "ts";
  char tblRef[TSDB_TABLE_NAME_LEN + TSDB_DB_NAME_LEN + 2] = {0};
  buildExtTableRef(&pInfo->spec, tblRef, sizeof(tblRef));

  int32_t nUids = tSimpleHashGetSize(pUidWindow);
  if (nUids == 0) return TSDB_CODE_SUCCESS;

  /* Snapshot the requested uids (+windows) for batch iteration. */
  pUids = taosArrayInit(nUids, sizeof(int64_t));
  pWins = taosArrayInit(nUids, sizeof(SExtUidWindow));
  if (pUids == NULL || pWins == NULL) { code = terrno; goto _exit; }
  {
    int32_t it = 0;
    void   *pv = NULL;
    while ((pv = tSimpleHashIterate(pUidWindow, pv, &it)) != NULL) {
      int64_t uid = *(int64_t *)tSimpleHashGetKey(pv, NULL);
      if (taosArrayPush(pUids, &uid) == NULL) { code = terrno; goto _exit; }
      if (taosArrayPush(pWins, (SExtUidWindow *)pv) == NULL) { code = terrno; goto _exit; }
    }
  }

  /* Combined column-type mappings: data columns + tag columns (NCHAR). */
  nColsAll = nMappings + nTags;
  pMapAll = taosMemoryCalloc(nColsAll, sizeof(SExtColTypeMapping));
  if (pMapAll == NULL) { code = terrno; goto _exit; }
  memcpy(pMapAll, pMappings, (size_t)nMappings * sizeof(SExtColTypeMapping));
  for (int32_t c = 0; c < nTags; c++) {
    pMapAll[nMappings + c].tdType.type  = TSDB_DATA_TYPE_NCHAR;
    pMapAll[nMappings + c].tdType.bytes = EXT_INFLUX_TAG_NCHAR_CHARS;
    tstrncpy(pMapAll[nMappings + c].colName,
             (const char *)taosArrayGet(pInfo->pInfluxTagCols, c),
             sizeof(pMapAll[nMappings + c].colName));
  }

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

    /* SELECT <data cols>, <tag cols> FROM tbl WHERE (uid clauses OR-joined). */
    char    sqlBuf[EXT_SQL_BUF_SIZE] = {0};
    int32_t off = 0;
    bool    ok  = extSqlCat(sqlBuf, sizeof(sqlBuf), &off, "SELECT %s", colList);
    for (int32_t c = 0; ok && c < nTags; c++) {
      ok = extSqlCat(sqlBuf, sizeof(sqlBuf), &off, ", %s",
                     (const char *)taosArrayGet(pInfo->pInfluxTagCols, c));
    }
    if (ok) ok = extSqlCat(sqlBuf, sizeof(sqlBuf), &off, " FROM %s WHERE ", tblRef);
    if (!ok) { code = TSDB_CODE_OUT_OF_RANGE; break; }

    bool    anyUid = false;
    int32_t k      = 0;
    for (; k < batchSize; k++) {
      int64_t         uid = *(int64_t *)taosArrayGet(pUids, batchStart + k);
      SExtUidWindow  *win = (SExtUidWindow *)taosArrayGet(pWins, batchStart + k);
      SUidIndexEntry *ent = tSimpleHashGet(pInfo->pUidIndex, &uid, sizeof(uid));

      char tagWhere[EXT_SQL_BUF_SIZE / 4] = {0};
      if (ent != NULL && ent->tagsetKey[0] != '\0') {
        int32_t twLen = buildInfluxTagWhereClause(ent->tagsetKey, tagWhere, sizeof(tagWhere));
        if (twLen < 0) tagWhere[0] = '\0';
      }

      /* Tentatively append this uid's clause; revert atomically on overflow. */
      int32_t saveOff  = off;
      bool    clauseOk = true;
      if (anyUid) clauseOk = extSqlCat(sqlBuf, sizeof(sqlBuf), &off, " OR ");
      if (clauseOk) clauseOk = extSqlCat(sqlBuf, sizeof(sqlBuf), &off, "(");
      if (clauseOk && prefilterBuf[0] != '\0')
        clauseOk = extSqlCat(sqlBuf, sizeof(sqlBuf), &off, "%s", prefilterBuf);
      if (clauseOk && tagWhere[0] != '\0')
        clauseOk = extSqlCat(sqlBuf, sizeof(sqlBuf), &off, "%s AND ", tagWhere);
      if (clauseOk)
        clauseOk = extSqlCat(sqlBuf, sizeof(sqlBuf), &off,
                             "%s >= to_timestamp_nanos(%" PRId64 ")"
                             " AND %s <= to_timestamp_nanos(%" PRId64 "))",
                             tsCol, win->skey, tsCol, win->ekey);
      if (!clauseOk) {
        off = saveOff;
        sqlBuf[saveOff] = '\0';
        if (!anyUid) k++; /* single clause exceeds buffer: skip to make progress */
        break;
      }
      anyUid = true;
    }
    int32_t consumed = (k > 0) ? k : batchSize;

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
    int32_t it = 0;
    void   *pv = NULL;
    while ((pv = tSimpleHashIterate(pUidBlk, pv, &it)) != NULL) {
      int64_t       uid  = *(int64_t *)tSimpleHashGetKey(pv, NULL);
      SSDataBlock  *pBlk = *(SSDataBlock **)pv;
      SUidBlockPair pair = {.uid = uid, .pBlock = pBlk};
      if (taosArrayPush(pUidBlocks, &pair) == NULL) { code = terrno; break; }
      *(SSDataBlock **)pv = NULL; /* ownership moved; skip in cleanup below */
    }
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
  ST_TASK_DLOG("ext: handleDataPull enter source=%s", pInfo->spec.sourceName);

  if (pReq == NULL || pReq->pUidWindow == NULL) {
    ST_TASK_ELOG("%s", "ext: handleDataPull: pUidWindow is NULL");
    return TSDB_CODE_INVALID_PARA;
  }

  bool isInflux = ((EExtSourceType)pInfo->spec.sourceType == EXT_SOURCE_INFLUXDB);

  char    colList[EXT_SQL_CLAUSE_BUF_LEN] = {0};
  int32_t nCols = buildTriggerColList(&pInfo->spec, colList, sizeof(colList));

  /* Trigger data path: use PRE_FILTER (triggerPrefilter). */
  char prefilterBuf[EXT_SQL_CLAUSE_BUF_LEN] = {0};
  buildTriggerPrefilterClause(&pInfo->spec, prefilterBuf, sizeof(prefilterBuf));

  int32_t nTags = pInfo->pInfluxTagCols
                    ? (int32_t)taosArrayGetSize(pInfo->pInfluxTagCols) : 0;

  /* Mappings correspond to the explicit trigger column list; with the "*"
   * fallback (nCols == 0) the result columns won't match them positionally, so
   * drop them and let the connector return raw columns. */
  const SExtColTypeMapping *pColMaps = (nCols > 0) ? pInfo->spec.pColMappings : NULL;
  int32_t                   nColMaps = (nCols > 0) ? pInfo->spec.numColMappings : 0;

  SArray *pUidBlocks = taosArrayInit(tSimpleHashGetSize(pReq->pUidWindow),
                                     sizeof(SUidBlockPair));
  if (pUidBlocks == NULL) return terrno;

  int32_t code = TSDB_CODE_SUCCESS;

  /* Batched path: InfluxDB PARTITION BY tag with an explicit column list whose
   * count matches the type mappings — one OR-compound query per 64 uids instead
   * of one query per uid.  Otherwise fall back to the per-uid path. */
  if (isInflux && nTags > 0 && nCols > 0 && nColMaps == nCols) {
    code = fetchDataBatchInflux(pInfo, colList, pColMaps,
                                nColMaps, nTags, prefilterBuf,
                                pReq->pUidWindow, pUidBlocks);
  } else {
    int32_t iter = 0;
    void   *pVal = NULL;
    while ((pVal = tSimpleHashIterate(pReq->pUidWindow, pVal, &iter)) != NULL) {
      size_t  kLen  = 0;
      int64_t uid   = *(int64_t *)tSimpleHashGetKey(pVal, &kLen);
      SExtUidWindow *win = (SExtUidWindow *)pVal;

      SSDataBlock *pBlock = NULL;
      code = fetchDataForUid(pInfo, colList, uid, win->skey, win->ekey,
                             isInflux,
                             prefilterBuf,
                             pColMaps,
                             nColMaps,
                             &pBlock);
      if (code != TSDB_CODE_SUCCESS) {
        ST_TASK_ELOG("ext: handleDataPull fetchDataForUid uid=%" PRId64 " code:%d",
                uid, code);
        break;
      }
      SUidBlockPair pair = {.uid = uid, .pBlock = pBlock};
      taosArrayPush(pUidBlocks, &pair);
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    code = buildDataBlockAndIndex(pUidBlocks, &pRsp->pDataBlock,
                                  &pRsp->pIndexHash);
  }

  /* Free any unconsumed blocks. */
  for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pUidBlocks); i++) {
    SUidBlockPair *pair = taosArrayGet(pUidBlocks, i);
    if (pair->pBlock) blockDataDestroy(pair->pBlock);
  }
  taosArrayDestroy(pUidBlocks);

  int64_t dataRows = (pRsp->pDataBlock != NULL) ? pRsp->pDataBlock->info.rows : 0;
  ST_TASK_DLOG("ext: handleDataPull done code=%d dataRows=%" PRId64, code, dataRows);
  printDataBlock(pRsp->pDataBlock, __func__, "ext_data", pTask->streamId);

  /* Mirror vnodeProcessStreamWalMetaDataNewReq: signal NO_DATA when result is
   * empty so the trigger side treats this as a normal empty-poll. */
  if (code == TSDB_CODE_SUCCESS && dataRows == 0) {
    ST_TASK_DLOG("%s", "ext: handleDataPull no rows -> TSDB_CODE_STREAM_NO_DATA");
    code = TSDB_CODE_STREAM_NO_DATA;
  }
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
  ST_TASK_DLOG("ext: handleMetaDataPull enter source=%s", pInfo->spec.sourceName);

  /* First, execute the META logic (same as handleMetaPull). */
  int32_t code = handleMetaPull(pInfo, pReq, pRsp);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: handleMetaDataPull: meta phase failed code:%d", code);
    return code;
  }

  /* Build a synthetic pUidWindow from the resulting metaBlock so we can reuse
   * handleDataPull logic. */
  if (pRsp->pMetaBlock == NULL || pRsp->pMetaBlock->info.rows == 0) {
    ST_TASK_DLOG("%s", "ext: handleMetaDataPull: no meta rows, skipping data phase");
    return TSDB_CODE_SUCCESS;
  }

  SSHashObj *pUidWindow = tSimpleHashInit(pRsp->pMetaBlock->info.rows,
                                          taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (pUidWindow == NULL) return terrno;

  SColumnInfoData *colUid  = taosArrayGet(pRsp->pMetaBlock->pDataBlock, META_COL_UID);
  SColumnInfoData *colSkey = taosArrayGet(pRsp->pMetaBlock->pDataBlock, META_COL_SKEY);
  SColumnInfoData *colEkey = taosArrayGet(pRsp->pMetaBlock->pDataBlock, META_COL_EKEY);

  for (int32_t r = 0; r < pRsp->pMetaBlock->info.rows; r++) {
    int64_t uid  = *(int64_t *)colDataGetData(colUid,  r);
    int64_t skey = *(int64_t *)colDataGetData(colSkey, r);
    int64_t ekey = *(int64_t *)colDataGetData(colEkey, r);
    SExtUidWindow win = {.skey = skey, .ekey = ekey};
    tSimpleHashPut(pUidWindow, &uid, sizeof(uid), &win, sizeof(win));
  }

  /* Temporarily set pUidWindow in a synthetic request. */
  SSTriggerExtPullReq syntheticReq = {0};
  if (pReq) syntheticReq = *pReq;
  syntheticReq.pUidWindow = pUidWindow;

  code = handleDataPull(pInfo, &syntheticReq, pRsp);
  if (code == TSDB_CODE_STREAM_NO_DATA) {
    code = TSDB_CODE_SUCCESS;
  }
  tSimpleHashCleanup(pUidWindow);

  ST_TASK_DLOG("ext: handleMetaDataPull done code=%d metaRows=%" PRId64 " dataRows=%" PRId64,
          code,
          pRsp->pMetaBlock ? pRsp->pMetaBlock->info.rows : 0,
          pRsp->pDataBlock ? pRsp->pDataBlock->info.rows : 0);
  printDataBlock(pRsp->pMetaBlock, __func__, "ext_meta_data_meta", pTask->streamId);
  printDataBlock(pRsp->pDataBlock, __func__, "ext_meta_data_data", pTask->streamId);
  return code;
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
  ST_TASK_DLOG("ext: handleCalcDataPull enter source=%s calcCols=%d",
               pInfo->spec.sourceName,
               pInfo->spec.calcColumns
                   ? (int32_t)taosArrayGetSize(pInfo->spec.calcColumns)
                   : 0);

  if (pReq == NULL || pReq->pUidWindow == NULL) {
    ST_TASK_ELOG("%s", "ext: handleCalcDataPull: pUidWindow is NULL");
    return TSDB_CODE_INVALID_PARA;
  }

  bool isInflux = ((EExtSourceType)pInfo->spec.sourceType == EXT_SOURCE_INFLUXDB);

  /* Use calc columns for the SELECT list (aggregate-input columns).
   * Falls back to trigger columns (then SELECT *) when calcColumns is empty. */
  char    colList[EXT_SQL_CLAUSE_BUF_LEN] = {0};
  int32_t nCols = buildCalcColList(&pInfo->spec, colList, sizeof(colList));
  ST_TASK_DLOG("ext: handleCalcDataPull colList='%s'", colList);

  /* Use pCalcMappings for type-correct column reads, but only when we built an
   * explicit column list: with the "*" fallback (nCols == 0) the result column
   * set/order won't match the mappings positionally, so drop them and let the
   * connector return raw columns. */
  const SExtColTypeMapping *pMappings   = (nCols > 0) ? pInfo->spec.pCalcMappings : NULL;
  int32_t                   nMappings   = (nCols > 0) ? pInfo->spec.numCalcMappings : 0;

  /* handleCalcDataPull fetches trigger-side data to feed the calc engine;
   * apply PRE_FILTER (triggerPrefilter) only — same filter as the trigger reader. */
  char calcPrefilterBuf[EXT_SQL_CLAUSE_BUF_LEN] = {0};
  buildTriggerPrefilterClause(&pInfo->spec, calcPrefilterBuf, sizeof(calcPrefilterBuf));

  int32_t nTags = pInfo->pInfluxTagCols
                    ? (int32_t)taosArrayGetSize(pInfo->pInfluxTagCols) : 0;

  SArray *pUidBlocks = taosArrayInit(tSimpleHashGetSize(pReq->pUidWindow),
                                     sizeof(SUidBlockPair));
  if (pUidBlocks == NULL) return terrno;

  int32_t code = TSDB_CODE_SUCCESS;

  /* Batched path: InfluxDB PARTITION BY tag with an explicit calc column list
   * whose count matches the calc type mappings — one OR-compound query per 64
   * uids instead of one query per uid.  Otherwise fall back to the per-uid path. */
  if (isInflux && nTags > 0 && nCols > 0 && nMappings == nCols) {
    code = fetchDataBatchInflux(pInfo, colList, pMappings, nMappings, nTags,
                                calcPrefilterBuf, pReq->pUidWindow, pUidBlocks);
  } else {
    int32_t iter = 0;
    void   *pVal = NULL;
    while ((pVal = tSimpleHashIterate(pReq->pUidWindow, pVal, &iter)) != NULL) {
      size_t  kLen  = 0;
      int64_t uid   = *(int64_t *)tSimpleHashGetKey(pVal, &kLen);
      SExtUidWindow *win = (SExtUidWindow *)pVal;

      SSDataBlock *pBlock = NULL;
      code = fetchDataForUid(pInfo, colList, uid, win->skey, win->ekey,
                             isInflux, calcPrefilterBuf, pMappings, nMappings, &pBlock);
      if (code != TSDB_CODE_SUCCESS) {
        ST_TASK_ELOG("ext: handleCalcDataPull fetchDataForUid uid=%" PRId64 " code:%d",
                uid, code);
        break;
      }
      SUidBlockPair pair = {.uid = uid, .pBlock = pBlock};
      taosArrayPush(pUidBlocks, &pair);
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    code = buildDataBlockAndIndex(pUidBlocks, &pRsp->pDataBlock,
                                  &pRsp->pIndexHash);
  }

  for (int32_t i = 0; i < (int32_t)taosArrayGetSize(pUidBlocks); i++) {
    SUidBlockPair *pair = taosArrayGet(pUidBlocks, i);
    if (pair->pBlock) blockDataDestroy(pair->pBlock);
  }
  taosArrayDestroy(pUidBlocks);

  int64_t calcRows = (pRsp->pDataBlock != NULL) ? pRsp->pDataBlock->info.rows : 0;
  ST_TASK_DLOG("ext: handleCalcDataPull done code=%d calcRows=%" PRId64, code, calcRows);
  printDataBlock(pRsp->pDataBlock, __func__, "ext_calc_data", pTask->streamId);

  /* Mirror vnodeProcessStreamWalCalcDataNewReq: return NO_DATA when empty so
   * the trigger side treats this as a normal empty-poll. */
  if (code == TSDB_CODE_SUCCESS && calcRows == 0) {
    ST_TASK_DLOG("%s", "ext: handleCalcDataPull no rows -> TSDB_CODE_STREAM_NO_DATA");
    code = TSDB_CODE_STREAM_NO_DATA;
  }
  return code;
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
  int64_t            gid   = pReq->gid;

  ST_TASK_DLOG("ext: handleGroupColValuePull enter gid=%" PRId64, gid);

  pRsp->pGroupColVals = taosArrayInit(4, sizeof(SStreamGroupValue));
  if (pRsp->pGroupColVals == NULL) return terrno;

  int64_t  uid    = gid;
  SArray **ppUids = (SArray **)tSimpleHashGet(pInfo->pGroupIndex, &gid, sizeof(int64_t));
  if (ppUids != NULL && taosArrayGetSize(*ppUids) > 0) {
    uid = *(int64_t *)taosArrayGet(*ppUids, 0);
  }

  SUidIndexEntry *pEntry = (SUidIndexEntry *)tSimpleHashGet(pInfo->pUidIndex, &uid, sizeof(int64_t));
  if (pEntry == NULL || pEntry->tagsetKey[0] == '\0') {
    /* MySQL/PG (no tag concept) or no tag columns: nothing to report. */
    ST_TASK_DLOG("ext: handleGroupColValuePull gid=%" PRId64 " uid=%" PRId64 " has no tagsetKey, empty result",
                 gid, uid);
    return TSDB_CODE_SUCCESS;
  }

  int32_t nPart = pInfo->spec.partitionTagCols ? (int32_t)taosArrayGetSize(pInfo->spec.partitionTagCols) : 0;

  char tagsetKey[EXT_TAGSET_KEY_MAX] = {0};
  tstrncpy(tagsetKey, pEntry->tagsetKey, sizeof(tagsetKey));

  char *saveptr = NULL;
  char *pair    = strtok_r(tagsetKey, "|", &saveptr);
  while (pair != NULL) {
    char *eq = strchr(pair, '=');
    if (eq == NULL) {
      pair = strtok_r(NULL, "|", &saveptr);
      continue;
    }
    *eq = '\0';
    const char *colName = pair;
    const char *colVal  = eq + 1;

    /* nPart == 0 means PARTITION BY tbname (all tags); otherwise only keep
     * columns that are actually part of the PARTITION BY subset. */
    bool keep = (nPart == 0);
    for (int32_t p = 0; !keep && p < nPart; p++) {
      const char *pcol = (const char *)taosArrayGet(pInfo->spec.partitionTagCols, p);
      if (pcol != NULL && strcmp(pcol, colName) == 0) {
        keep = true;
      }
    }

    if (keep) {
      // InfluxDB tag columns are discovered/typed as NCHAR (see
      // extInitInfluxTagPartition's pMappings[i].tdType.type), so the parser
      // resolves a PARTITION BY tag reference (e.g. "host") to NCHAR too.
      // FUNCTION_TYPE_PLACEHOLDER_COLUMN (functionMgt.c) rejects a type
      // mismatch against the resolved value node's type, so this must be
      // UCS-4-encoded NCHAR, not raw UTF-8 VARCHAR bytes.
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
      if (taosArrayPush(pRsp->pGroupColVals, &val) == NULL) {
        taosMemoryFree(val.data.pData);
        return terrno;
      }
    }

    pair = strtok_r(NULL, "|", &saveptr);
  }

  ST_TASK_DLOG("ext: handleGroupColValuePull gid=%" PRId64 " resolved %d tag value(s)",
               gid, (int32_t)taosArrayGetSize(pRsp->pGroupColVals));
  return TSDB_CODE_SUCCESS;
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
  buildExtTableRef(&pReaderInfo->spec, tblRef, sizeof(tblRef));

  /* Build column list from calcColumns (aggregate-input columns).
   * Returns the explicit column count, or 0 when it falls back to "*". */
  char    colList[EXT_SQL_CLAUSE_BUF_LEN] = {0};
  int32_t nCols = buildCalcColList(&pReaderInfo->spec, colList, sizeof(colList));

  /* Only enforce the calc column-type mappings when we built an explicit column
   * list (nCols > 0).  With the "*" fallback the result column set and order are
   * not guaranteed to match pCalcMappings positionally, so let the connector
   * return raw columns instead of mis-typing them. */
  const SExtColTypeMapping *pMappings = (nCols > 0) ? pReaderInfo->spec.pCalcMappings : NULL;
  int32_t                   nMappings = (nCols > 0) ? pReaderInfo->spec.numCalcMappings : 0;

  /* streamReaderExtFetchData fetches pure calc data for aggregation;
   * apply calc WHERE prefilter (prefilter) only — not PRE_FILTER. */
  char prefilterBuf[EXT_SQL_CLAUSE_BUF_LEN] = {0};
  buildPrefilterClause(&pReaderInfo->spec, prefilterBuf, sizeof(prefilterBuf));

  /* Build the SELECT statement (bounds-safe appends; bail out on truncation so
   * we never execute a malformed query). */
  char    sql[EXT_SQL_BUF_SIZE] = {0};
  int32_t off = 0;
  bool    ok  = extSqlCat(sql, sizeof(sql), &off,
                          "SELECT %s FROM %s WHERE %s1=1", colList, tblRef, prefilterBuf);

  bool isInflux = ((EExtSourceType)pReaderInfo->spec.sourceType == EXT_SOURCE_INFLUXDB);
  if (ok && skey != INT64_MIN) {
    if (isInflux) {
      ok = extSqlCat(sql, sizeof(sql), &off,
                     " AND %s >= to_timestamp_nanos(%" PRId64 ")", tsCol, skey);
    } else {
      char skeyDt[32] = {0};
      epochToDatetimeStr(skey, TSDB_TIME_PRECISION_MICRO, skeyDt, sizeof(skeyDt));
      ok = extSqlCat(sql, sizeof(sql), &off, " AND %s >= %s", tsCol, skeyDt);
    }
  }
  if (ok && ekey != INT64_MAX) {
    if (isInflux) {
      ok = extSqlCat(sql, sizeof(sql), &off,
                     " AND %s <= to_timestamp_nanos(%" PRId64 ")", tsCol, ekey);
    } else {
      char ekeyDt[32] = {0};
      epochToDatetimeStr(ekey, TSDB_TIME_PRECISION_MICRO, ekeyDt, sizeof(ekeyDt));
      ok = extSqlCat(sql, sizeof(sql), &off, " AND %s <= %s", tsCol, ekeyDt);
    }
  }
  if (!ok) {
    ST_TASK_ELOG("ext: streamReaderExtFetchData SQL exceeds %d bytes, aborting fetch",
                 (int32_t)sizeof(sql));
    return TSDB_CODE_OUT_OF_RANGE;
  }

  ST_TASK_DLOG("ext: streamReaderExtFetchData sql=\"%.200s\" nMappings=%d",
               sql, nMappings);

  code = extQueryExecFetchAll(pReaderInfo->pConn, sql,
                              (SExtColTypeMapping *)pMappings, nMappings, ppOut);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: streamReaderExtFetchData failed code:%d", code);
    return code;
  }

  ST_TASK_DLOG("ext: streamReaderExtFetchData done rows=%" PRId64,
          *ppOut ? (*ppOut)->info.rows : 0);
  return TSDB_CODE_SUCCESS;
}
