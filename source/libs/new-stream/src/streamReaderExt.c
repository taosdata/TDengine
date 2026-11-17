/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * SPDX-License-Identifier: AGPL-3.0-or-later
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
 * TDMT_STREAM_TRIGGER_PULL_EXT.  See DS section 6.1.5, section 6.2.7, section 6.4.5.
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
#include "extTypeMap.h"   // extTimestampTypeTzClass
#include "osString.h"     // taosUcs4ToMbs / TdUcs4 (extColCellToStr NCHAR conversion)
#include "plannodes.h"    // SExtColTypeMapping full definition
#include "querynodes.h"   // SColumnNode / SExprNode
#include "scalar.h"       // scalarCalculate for vectorized partition expressions
#include "taoserror.h"
#include "tdatablock.h"
#include "thash.h"
#include "tlog.h"
#include "ttime.h"
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

/* Maximum number of uids per InfluxDB OR-group (DS section 6.1.5). */
#define EXT_INFLUX_UID_BATCH_SIZE  64

/* SQL buffer sizes by statement shape.  Only batched UNION ALL statements
 * need the largest buffer; fixed-shape and single-uid queries have smaller
 * bounded inputs. */
#define EXT_SCHEMA_SQL_BUF_LEN  512
#define EXT_SIMPLE_SQL_BUF_LEN  (EXT_SQL_PREFILTER_BUF_LEN + 1024 * 2)
#define EXT_CALC_SQL_BUF_LEN    (EXT_SQL_PREFILTER_BUF_LEN + 4096)
#define EXT_DATA_SQL_BUF_LEN    (4096 * 2)
#define EXT_BATCH_SQL_BUF_LEN   (4096 * 4)
#define EXT_META_BLOCK_INITIAL_ROWS 64U

/* Length limits for the tagset key string used as pTagsetIndex key
 * ("col1=val1|col2=val2|...").  EXT_TAGSET_KEY_MAX is the escaped storage size and
 * EXT_TAGSET_KEY_RAW_MAX the unescaped budget; both live in streamReaderExt.h because the
 * persistent SUidIndexEntry copies share them. */

/* Bound the re-enumeration skip after an Influx SERIES condition matched no tag
 * combinations (see SStreamExtReaderInfo.influxEmptyEnumTsMs). */
#define EXT_INFLUX_EMPTY_ENUM_RECHECK_MS 60000

/* Defined next to handleMetaPullInflux18(); declared here because the reader
 * close and series-reset paths release the same 1.8 META enumeration cache. */
static void extInflux18DropMetaSnapshot(SStreamExtReaderInfo *pInfo);
static void extInflux18AbortMetaEnumeration(SStreamExtReaderInfo *pInfo);

/* Scratch buffer size for one generated SQL clause fragment (a WHERE
 * prefilter condition or a SELECT column list) before it is spliced into a
 * full statement in one of the statement-shape buffers above. */
#define EXT_SQL_CLAUSE_BUF_LEN 1024

/* Buffer for a prefilter clause, i.e. the stored prefilter wrapped as
 * "(<prefilter>) AND ".
 *
 * The parser renders a PRE_FILTER into a 4096-byte buffer and fails the
 * statement rather than truncating, so a stored prefilter can be up to 4095
 * bytes - well past EXT_SQL_CLAUSE_BUF_LEN.  The MySQL hexadecimal form of a
 * string holding a backslash is twice as long as the value, which is how a
 * prefilter that used to fit can grow past a clause-sized buffer; building it
 * there returns OUT_OF_RANGE and leaves the reader FAILED on its first
 * watermark read, so a stream that used to run would break at startup.  The
 * statement buffers above are sized off this one for the same reason. */
#define EXT_SQL_PREFILTER_BUF_LEN (4096 + 16)

/* A quoted identifier is at most quote + ident + escaped-quote pairs + quote + NUL; the largest
 * identifier spliced into generated SQL is a table name. */
#define EXT_IDENT_QUOTE_BUF_LEN (TSDB_TABLE_NAME_LEN * 2 + 2)
/* A discovered InfluxDB tag key is stored in EXT_INFLUX_KEY_NCHAR_CHARS bytes (up to 64 NCHARs);
 * every byte can double when the key contains the dialect quote character, so size the tag-key
 * quoting buffer off the tag-key limit rather than the table-name limit. */
#define EXT_TAG_KEY_QUOTE_BUF_LEN (EXT_INFLUX_KEY_NCHAR_CHARS * 2 + 4)
/* Fully quoted "qualifier"."table" with both parts at their maximum length. */
#define EXT_TBL_REF_BUF_LEN (TSDB_DB_NAME_LEN * 2 + TSDB_TABLE_NAME_LEN * 2 + 8)

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
 * bufLen, making the next `bufLen - off` negative  -  which, as a size_t, becomes
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

/* metaBlock column indices (DS section 6.2.4). */
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
                                 int8_t boundPrecision,
                                 SArray *pUidBlocks);
static int32_t extBuildUidWindowFromMetaBlock(SSDataBlock *pMetaBlock,
                                              SSHashObj **ppUidWindow);

static void freeGroupInfoWithId(void *ptr) {
  SStreamGroupInfoWithId *pInfo = ptr;
  taosArrayDestroyEx(pInfo->gInfo, tDestroySStreamGroupValue);
  pInfo->gInfo = NULL;
}

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
  taosArrayDestroyEx(pRsp->pGroupInfos, freeGroupInfoWithId);
  pRsp->pGroupInfos = NULL;
  taosMemoryFree(pRsp);
}

/* ============================================================
 * Utility: pGroupIndex cleanup callback  -  frees the SArray* value.
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
 * Timestamp payloads are normalized to the requested column mapping
 * precision; relational SQL bounds are formatted from the request precision.
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
 *
 * tzName is the client timezone of the owning stream.  The connector decodes
 * timezone-naive source columns (MySQL DATETIME, PG timestamp without time
 * zone, SQL Server datetime2) with that timezone, so the WHERE literals must
 * be rendered with it as well; otherwise the fetch/metadata ranges shift by
 * the offset between the stream and the server global timezone.  An empty or
 * invalid name keeps the previous behaviour (server global timezone).
 * ============================================================ */
static timezone_t extResolveSqlTimezone(const char *tzName) {
  if (tzName == NULL || tzName[0] == '\0') {
    return NULL;
  }

  timezone_t tz = NULL;
  int32_t    code = taosValidateTimezone(tzName, &tz);
  if (code != TSDB_CODE_SUCCESS) {
    stWarn("ext: invalid timezone name:%s, using server global timezone", tzName);
    if (tz != NULL) {
      tzfree(tz);
    }
    return NULL;
  }
  return tz;
}

/* Timezone name used to render remote SQL timestamp bounds.
 *
 * The connector decodes timezone-naive source columns (MySQL DATETIME, PG
 * timestamp without time zone, SQL Server datetime2) with the stream client
 * timezone and timezone-aware ones (MySQL TIMESTAMP, PG timestamptz) into an
 * absolute instant, independently of both the stream and the taosd timezone.
 * Timezone-aware bounds are therefore always rendered from a UTC basis; see
 * extSqlBoundForm() for how each dialect spells such a bound. */
static const char *extSqlBoundTimezone(const SStreamExtReaderInfo *pInfo) {
  if (pInfo->tsTzClass == EXT_TIMESTAMP_TZ_CLASS_AWARE) {
    return "UTC";
  }
  return pInfo->timezone;
}

/* How a timestamp bound literal is spelled for the remote dialect. */
typedef enum {
  /* Timezone-naive column (MySQL DATETIME, PG timestamp, SQL Server datetime2):
   * the source wall clock is decoded with the stream timezone, so the bound is
   * rendered in that same timezone. */
  EXT_SQL_BOUND_LOCAL = 0,
  /* Timezone-aware column: the bound is UTC wall clock plus an explicit
   * "+00:00" suffix, because neither MySQL nor PG can be assumed to run its
   * session in UTC and an offset-less literal would be read in the session
   * timezone. */
  EXT_SQL_BOUND_UTC_SUFFIX,
  /* Timezone-aware column whose dialect reads an offset-less literal as UTC. */
  EXT_SQL_BOUND_UTC_WALL,
  /* Timezone-aware MySQL column: FROM_UNIXTIME(<epoch>), which is
   * session-timezone independent without needing offset-literal support. */
  EXT_SQL_BOUND_UNIX_EPOCH,
} EExtSqlBoundForm;

/* Pick the bound spelling for this reader.
 *
 * A timezone-aware column holds an absolute instant, but the remote side still
 * has to know which instant a bare wall clock means:
 *   - MySQL interprets an offset-less literal in the *session* timezone, and
 *     the reader cannot observe that session.  An explicit "+00:00" suffix
 *     fixes that but is only understood from MySQL 8.0.19 on; MySQL 5.7 (still
 *     supported, FS section 12) treats the suffix as trailing garbage and keeps
 *     comparing in the session timezone, which shifts every bound by the
 *     session offset.  FROM_UNIXTIME() has no such version dependency and
 *     always yields the session-timezone image of the requested instant, so it
 *     compares correctly against a TIMESTAMP column in any session timezone
 *     (>= 5.6.4 keeps the fractional seconds).
 *   - PostgreSQL accepts an offset on a datetime literal in every supported
 *     version, so the bound spells "+00:00".
 *   - SQL Server needs no suffix: it reads an offset-less datetimeoffset
 *     literal as +00:00 (DS 5.3.7), which the SQL Server end-to-end case pins
 *     down.
 */
static EExtSqlBoundForm extSqlBoundForm(const SStreamExtReaderInfo *pInfo) {
  if (pInfo->tsTzClass != EXT_TIMESTAMP_TZ_CLASS_AWARE) {
    return EXT_SQL_BOUND_LOCAL;
  }

  switch ((EExtSourceType)pInfo->spec.sourceType) {
    case EXT_SOURCE_MYSQL:
      return EXT_SQL_BOUND_UNIX_EPOCH;
    case EXT_SOURCE_SQLSERVER:
      return EXT_SQL_BOUND_UTC_WALL;
    case EXT_SOURCE_ORACLE:
      // The connector pins the session time zone to UTC (DS 5.3.5), so an
      // offset-less ANSI literal is read as that absolute instant.
      return EXT_SQL_BOUND_UTC_WALL;
    default:
      return EXT_SQL_BOUND_UTC_SUFFIX;
  }
}

/* Longest timestamp literal a remote bound can render:
 * 'YYYY-MM-DD HH:MM:SS.uuuuuu+00:00' (34 bytes incl. quotes) + NUL, or
 * FROM_UNIXTIME(<13-digit seconds>.<6-digit fraction>) (36 bytes) + NUL.
 * Oracle additionally takes the ANSI prefix ("TIMESTAMP ", 10 bytes) and up to
 * nine fraction digits: 10 + 1 + 19 + 1 + 9 + 1 = 41 bytes + NUL.  Every buffer
 * below is sized for that prefixed form. */
#define EXT_SQL_TS_LITERAL_LEN 56

/* Oracle needs the ANSI timestamp literal prefix (DS 5.3.6 R4): an offset-less
 * quoted string compared against a TIMESTAMP column is converted through the
 * session NLS_TIMESTAMP_FORMAT, so the bound would depend on server settings
 * instead of being fixed by the statement.  Every other dialect takes the bare
 * ISO text. */
static const char *extSqlBoundPrefix(const SStreamExtReaderInfo *pInfo) {
  return ((EExtSourceType)pInfo->spec.sourceType == EXT_SOURCE_ORACLE) ? "TIMESTAMP " : "";
}

static int32_t epochToDatetimeStr(int64_t epoch, int32_t precision, const char *tzName,
                                  EExtSqlBoundForm form, const char *prefix,
                                  bool subMicroDigits, char *buf, int32_t bufLen) {
  if (prefix == NULL) {
    prefix = "";
  }
  const char *suffix = (form == EXT_SQL_BOUND_UTC_SUFFIX) ? "+00:00'" : "'";
  /* INT64_MIN is the "no bound" sentinel callers pass, so it still renders the
   * epoch origin.  A real pre-1970 instant must keep its own wall clock: clamping
   * every negative bound to 1970 would make the remote start each range at
   * 1970-01-01 and drop the history rows before it, which the static prefilter
   * already handles (renderExtPrefilterTimestamp in parTranslater.c). */
  bool unbounded = (epoch == INT64_MIN);
  if (unbounded) {
    epoch = 0;
  }
  /* FROM_UNIXTIME() has no negative argument and a MySQL TIMESTAMP column cannot
   * hold a pre-1970 value, so that form keeps the epoch origin. */
  if (epoch < 0 && form == EXT_SQL_BOUND_UNIX_EPOCH) {
    epoch = 0;
  }
  /* Normalize to microseconds for sub-second formatting. */
  int64_t epoch_us;
  int32_t sub_ns = 0;
  if (precision == TSDB_TIME_PRECISION_NANO) {
    epoch_us = epoch / 1000LL;
    /* Digits below the microsecond a six-fraction literal cannot carry.  The
     * caller only keeps them for a dialect that accepts nine digits.  Floor
     * semantics keep the fraction non-negative for a pre-1970 instant. */
    sub_ns = (int32_t)(epoch % 1000LL);
    if (sub_ns < 0) {
      sub_ns += 1000;
      --epoch_us;
    }
  } else if (precision == TSDB_TIME_PRECISION_MILLI) {
    if (epoch > INT64_MAX / 1000LL || epoch < INT64_MIN / 1000LL) {
      stError("ext: millisecond bound epoch:%" PRId64 " overflows microsecond conversion", epoch);
      return TSDB_CODE_STREAM_INTERNAL_ERROR;
    }
    epoch_us = epoch * 1000LL;
  } else {
    /* MICRO  -  already in micros */
    epoch_us = epoch;
  }
  if (form == EXT_SQL_BOUND_UNIX_EPOCH) {
    /* Absolute instant as a numeric epoch: the remote evaluates it in its own
     * session timezone, so the bound stays correct without an offset literal. */
    return snprintf(buf, bufLen, "FROM_UNIXTIME(%" PRId64 ".%06d)",
                    (int64_t)(epoch_us / 1000000LL), (int32_t)(epoch_us % 1000000LL));
  }
  if (unbounded) {
    return snprintf(buf, bufLen, "%s'1970-01-01 00:00:00.000000%s", prefix, suffix);
  }
  time_t sec = (time_t)(epoch_us / 1000000LL);
  int    us  = (int)(epoch_us % 1000000LL);
  if (us < 0) {
    /* Floor split: a negative instant belongs to the previous second. */
    us += 1000000;
    --sec;
  }
  struct tm  tm_info = {0};
  /* Render the bound in the same timezone the connector uses to decode the
   * source column; NULL falls back to the server global timezone. */
  timezone_t tz = extResolveSqlTimezone(tzName);
  struct tm *pLocal = taosLocalTime(&sec, &tm_info, NULL, 0, tz);
  if (tz != NULL) {
    tzfree(tz);
  }
  int32_t year = tm_info.tm_year + 1900;
  if (pLocal == NULL || year < 1 || year > 9999 || tm_info.tm_mon < 0 || tm_info.tm_mon > 11 ||
      tm_info.tm_mday < 1 || tm_info.tm_mday > 31) {
    /* The platform calendar cannot express this instant (Windows stops at
     * 1900-01-01).  Replacing it with the epoch origin would silently narrow or
     * widen the remote fetch, so fail the reader request instead. */
    stError("ext: cannot render tz:%s bound epoch:%" PRId64 " s as a calendar time",
            tzName != NULL ? tzName : "(default)", (int64_t)sec);
    return TSDB_CODE_STREAM_INTERNAL_ERROR;
  }
  /* A sliding window subtracts one tick from its half-open end and the fetch
   * keeps an inclusive "<=", so dropping the sub-microsecond digits would
   * shrink every window end by up to 999 ns and lose its tail rows.  Oracle
   * TIMESTAMP(7..9) accepts the nine-digit ANSI literal, so keep them there. */
  if (subMicroDigits && precision == TSDB_TIME_PRECISION_NANO) {
    return snprintf(buf, bufLen, "%s'%04d-%02d-%02d %02d:%02d:%02d.%06d%03d%s",
                    prefix,
                    tm_info.tm_year + 1900, tm_info.tm_mon + 1, tm_info.tm_mday,
                    tm_info.tm_hour, tm_info.tm_min, tm_info.tm_sec, us, sub_ns, suffix);
  }
  return snprintf(buf, bufLen,
                  "%s'%04d-%02d-%02d %02d:%02d:%02d.%06d%s",
                  prefix,
                  tm_info.tm_year + 1900, tm_info.tm_mon + 1, tm_info.tm_mday,
                  tm_info.tm_hour, tm_info.tm_min, tm_info.tm_sec, us, suffix);
}

/* Render the remote SQL literal for one time bound of this reader.
 *
 * The spelling depends on both the timestamp class and the source dialect (see
 * extSqlBoundForm); keeping that decision in one entry point lets the reader
 * and the unit tests exercise the same code path. */
int32_t streamReaderExtRenderTimeBound(const SStreamExtReaderInfo *pInfo, int64_t bound,
                                       int8_t precision, char *buf, int32_t bufLen) {
  if (pInfo == NULL || buf == NULL || bufLen <= 0) {
    return TSDB_CODE_INVALID_PARA;
  }
  return epochToDatetimeStr(bound, precision, extSqlBoundTimezone(pInfo),
                            extSqlBoundForm(pInfo), extSqlBoundPrefix(pInfo),
                            (EExtSourceType)pInfo->spec.sourceType == EXT_SOURCE_ORACLE,
                            buf, bufLen);
}

/* Last instant a MySQL TIMESTAMP column can hold: 2038-01-19 03:14:07.999999
 * UTC, which is also the largest argument FROM_UNIXTIME() accepts. */
#define EXT_MYSQL_TS_MAX_EPOCH_US 2147483647999999LL

/* Keep an inclusive upper bound inside the MySQL TIMESTAMP domain.
 *
 * A timezone-aware MySQL column is a TIMESTAMP, and FROM_UNIXTIME() returns
 * NULL once its seconds argument exceeds TIMESTAMP_MAX_VALUE (INT_MAX32), so
 * both 5.7 and 8.0 reject anything past 2038-01-19 03:14:07.  A finite upper
 * bound beyond that ceiling cannot exclude any TIMESTAMP row, but rendering
 * its raw epoch would turn the predicate into "ts <= NULL", which matches
 * nothing and empties the whole fetch window.
 *
 * Only upper bounds are clamped.  A lower bound past the ceiling must keep its
 * raw epoch: "ts >= NULL" already produces the empty result that bound calls
 * for, whereas clamping it to the ceiling would wrongly re-admit the newest
 * rows. */
static int64_t extClampMysqlUpperBound(const SStreamExtReaderInfo *pInfo, int64_t bound,
                                       int8_t precision) {
  if (extSqlBoundForm(pInfo) != EXT_SQL_BOUND_UNIX_EPOCH) {
    return bound;
  }
  /* Compare in the caller's precision so no conversion can overflow. */
  int64_t ceiling = EXT_MYSQL_TS_MAX_EPOCH_US;
  if (precision == TSDB_TIME_PRECISION_NANO) {
    ceiling = EXT_MYSQL_TS_MAX_EPOCH_US * 1000LL;
  } else if (precision == TSDB_TIME_PRECISION_MILLI) {
    ceiling = EXT_MYSQL_TS_MAX_EPOCH_US / 1000LL;
  }
  return (bound > ceiling) ? ceiling : bound;
}

/* ============================================================
 * Utility: quote one SQL identifier for the source dialect.
 *
 * PostgreSQL (and InfluxDB/DataFusion) use double quotes; MySQL uses backticks.
 * Quoting preserves case and punctuation that the dialect would otherwise fold
 * away: an unquoted PostgreSQL column resolves to its lower-case form, so a
 * remote column created as "MixedValue" can only be selected as "MixedValue".
 * The embedded quote character is escaped by doubling it.
 *
 * Returns the number of bytes written (excl. NUL), or a negative error code.
 * ============================================================ */
static int32_t extQuoteIdent(const SStreamExtTriggerSpec *pSpec, const char *ident,
                             char *buf, int32_t bufLen) {
  if (ident == NULL) ident = "";
  int8_t srcType = pSpec != NULL ? pSpec->sourceType : EXT_SOURCE_POSTGRESQL;
  char   quote = ((EExtSourceType)srcType == EXT_SOURCE_MYSQL) ? '`' : '"';
  int32_t n = 0;
  if (buf == NULL || bufLen < 3) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  buf[n++] = quote;
  for (const char *p = ident; *p != '\0'; ++p) {
    // Reserve room for the current char, its doubled escape (worst case), the closing quote and
    // the trailing NUL (up to 4 more bytes); checking only for the char + escape would let the
    // final quote/NUL write past a tight caller buffer.
    if (n + 3 >= bufLen) {
      return TSDB_CODE_OUT_OF_RANGE;
    }
    buf[n++] = *p;
    if (*p == quote) {
      buf[n++] = quote;
    }
  }
  buf[n++] = quote;
  buf[n] = '\0';
  return n;
}

/* ============================================================
 * Utility: return the correct table qualifier for manual SQL building.
 * For PostgreSQL and SQL Server, the qualifier is the schema name
 * (extSchema, e.g. "public" / "dbo"): a 3-segment path flags the middle
 * segment as schema (DS 5.3.3), and a bare "<db>.<table>" 2-segment name
 * would make T-SQL look for a schema named after the database
 * (42S02 "Invalid object name").  The database travels in the connection
 * defaults (SrcDatabase).
 * For MySQL/InfluxDB, the qualifier is the database name (extDb).
 * ============================================================ */
static const char *extTableQualifier(const SStreamExtTriggerSpec *pSpec) {
  // Oracle has one namespace level - the schema - and its "database" is the
  // service name, which is connection state and not a namespace at all, so a
  // "<service>.<table>" qualifier would name nothing (ORA-00942).
  if (((EExtSourceType)pSpec->sourceType == EXT_SOURCE_POSTGRESQL ||
       (EExtSourceType)pSpec->sourceType == EXT_SOURCE_SQLSERVER ||
       (EExtSourceType)pSpec->sourceType == EXT_SOURCE_ORACLE) &&
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
  char    tableQ[EXT_IDENT_QUOTE_BUF_LEN] = {0};
  int32_t code = extQuoteIdent(pSpec, pSpec->extTable, tableQ, sizeof(tableQ));
  if (code < 0) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  int32_t n = 0;
  if ((EExtSourceType)pSpec->sourceType == EXT_SOURCE_INFLUXDB) {
    n = snprintf(buf, bufLen, "%s", tableQ);
  } else {
    char    qualifierQ[EXT_IDENT_QUOTE_BUF_LEN] = {0};
    code = extQuoteIdent(pSpec, extTableQualifier(pSpec), qualifierQ, sizeof(qualifierQ));
    if (code < 0) {
      return TSDB_CODE_OUT_OF_RANGE;
    }
    n = snprintf(buf, bufLen, "%s.%s", qualifierQ, tableQ);
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

static char *findUnescapedTagsetChar(char *value, char delimiter) {
  bool escaped = false;
  for (char *p = value; *p != '\0'; ++p) {
    if (escaped) {
      escaped = false;
    } else if (*p == '\\') {
      escaped = true;
    } else if (*p == delimiter) {
      return p;
    }
  }
  return NULL;
}

static void unescapeTagsetComponent(char *value) {
  char *write = value;
  for (char *read = value; *read != '\0'; ++read) {
    if (*read == '\\' && read[1] != '\0' && (read[1] == '\\' || read[1] == '|' || read[1] == '=')) {
      ++read;
    }
    *write++ = *read;
  }
  *write = '\0';
}

/* ============================================================
 * SQL builder: MySQL/PG  -  uid list using "uid IN (u1, u2, ...)"
 * Writes SQL into buf[0..bufLen-1].  Returns snprintf-style length.
 * ============================================================ */
/* Convert a tagset key string "col1=val1|col2=val2|..." into an InfluxDB
 * WHERE fragment "col1='val1' AND col2='val2' AND ...".
 * Returns the number of bytes written (excl. NUL), or -1 on truncation.
 * If tagsetKey is empty (MySQL/PG single-group), writes nothing.
 * Tag keys are quoted with the dialect's identifier quoting so a mixed-case
 * key resolves consistently with the quoted SELECT column list. */
static int32_t buildInfluxTagWhereClause(const SStreamExtTriggerSpec *pSpec, const char *tagsetKey,
                                         char *buf, int32_t bufLen) {
  if (tagsetKey == NULL || tagsetKey[0] == '\0') {
    if (bufLen > 0) buf[0] = '\0';
    return 0;
  }

  int32_t off   = 0;
  bool    first = true;

  /* Duplicate the key so we can tokenise in-place. */
  char tmp[EXT_TAGSET_KEY_MAX] = {0};
  tstrncpy(tmp, tagsetKey, sizeof(tmp));

  char *pair = tmp;
  while (pair != NULL && *pair != '\0') {
    char *next = findUnescapedTagsetChar(pair, '|');
    if (next != NULL) *next = '\0';
    char *eq = findUnescapedTagsetChar(pair, '=');
    if (eq == NULL) {
      pair = next != NULL ? next + 1 : NULL;
      continue;
    }
    *eq = '\0';
    unescapeTagsetComponent(pair);
    unescapeTagsetComponent(eq + 1);
    const char *col = pair;
    const char *val = eq + 1;

    char colQ[EXT_TAG_KEY_QUOTE_BUF_LEN] = {0};
    if (extQuoteIdent(pSpec, col, colQ, sizeof(colQ)) < 0) {
      return -1;
    }

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
                         "%s%s='%s'", first ? "" : " AND ", colQ, escVal);
    if (n < 0 || off + n >= bufLen) return -1;
    off  += n;
    first = false;
    pair = next != NULL ? next + 1 : NULL;
  }

  return off;
}

/* ============================================================
 * extQueryExecFetchOne  -  Execute SQL, fetch the first result block, close.
 *
 * pConn      : connector handle
 * sql        : SQL string to execute
 * pMappings  : column type mappings (may be NULL if caller does not need
 *              typed columns  -  in that case pDataBlock will be empty)
 * nMappings  : length of pMappings array (0 when pMappings is NULL)
 * ppOut      : receives the first SSDataBlock (caller must blockDataDestroy)
 *
 * Returns TSDB_CODE_SUCCESS; *ppOut is NULL when no rows were returned.
 * ============================================================ */
static int32_t extQueryExecFetchOne(SExtConnectorHandle  *pConn,
                                    const char           *timezone,
                                    const char           *sql,
                                    SExtColTypeMapping   *pMappings,
                                    int32_t               nMappings,
                                    SSDataBlock         **ppOut) {
  int32_t            code   = TSDB_CODE_SUCCESS;
  SExtQueryHandle   *pQHdl  = NULL;
  SExtConnectorError connErr = {0};

  stDebug("ext: extQueryExecFetchOne sql=\"%.120s\" nMappings=%d", sql, nMappings);

  code = extConnectorExecQuery(pConn, NULL, timezone, sql, &pQHdl, &connErr);
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
 * Called once per fetched SSDataBlock (owned by callee  -  destroy when done).
 * Return TSDB_CODE_SUCCESS to continue fetching; any other code stops the loop
 * and is propagated as the return value of extQueryExecForEach.
 * ============================================================ */
typedef int32_t (*FExtBlockCallback)(SSDataBlock *pBlock, void *pCtx);

/* ============================================================
 * extQueryExecForEach  -  Execute SQL, call cb for every result block, close.
 *
 * Ownership: each pBlock passed to cb belongs to cb; cb must destroy it.
 * ============================================================ */
static int32_t extQueryExecForEach(SExtConnectorHandle  *pConn,
                                   const char           *timezone,
                                   const char           *sql,
                                   SExtColTypeMapping   *pMappings,
                                   int32_t               nMappings,
                                   FExtBlockCallback     cb,
                                   void                 *pCtx) {
  int32_t            code   = TSDB_CODE_SUCCESS;
  SExtQueryHandle   *pQHdl  = NULL;
  SExtConnectorError connErr = {0};

  stDebug("ext: extQueryExecForEach sql=\"%.120s\" nMappings=%d", sql, nMappings);

  code = extConnectorExecQuery(pConn, NULL, timezone, sql, &pQHdl, &connErr);
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
 * extQueryExecFetchAll  -  Execute SQL, loop-fetch ALL result batches,
 * merge into a single SSDataBlock, then close.
 *
 * Unlike extQueryExecFetchOne (one batch only), this drains the entire
 * result set.  Arrow Flight / InfluxDB may split results across multiple
 * RecordBatches; using FetchOne would silently drop trailing batches.
 * ============================================================ */
static int32_t extQueryExecFetchAll(SExtConnectorHandle  *pConn,
                                    const char           *timezone,
                                    const char           *sql,
                                    SExtColTypeMapping   *pMappings,
                                    int32_t               nMappings,
                                    SSDataBlock         **ppOut) {
  int32_t            code   = TSDB_CODE_SUCCESS;
  SExtQueryHandle   *pQHdl  = NULL;
  SExtConnectorError connErr = {0};

  stDebug("ext: extQueryExecFetchAll sql=\"%.120s\" nMappings=%d", sql, nMappings);

  code = extConnectorExecQuery(pConn, NULL, timezone, sql, &pQHdl, &connErr);
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
      *ppOut = pBlk;  // first batch  -  take ownership directly
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

static bool extIsValidTimePrecision(int8_t precision) {
  return precision == TSDB_TIME_PRECISION_MILLI || precision == TSDB_TIME_PRECISION_MICRO ||
         precision == TSDB_TIME_PRECISION_NANO;
}

/* Source transport precision of the reader's timestamp column (DS 5.3.7).
 *
 * The trigger column mappings carry it, but the synthetic LAST_TS / MIN / MAX
 * mappings built inside streamReaderExt.c do not, so it is resolved here and
 * copied onto them.  The Oracle connector decodes a timestamp column at
 * tsPrecision and treats every value that is not NANO as microseconds, so a
 * missing precision truncates a TIMESTAMP(7..9) column by its last three digits
 * and the tail row can never enter a later pull range.
 *
 * SExtColTypeMapping documents tsPrecision 0 as "not set", and 0 collides with
 * TSDB_TIME_PRECISION_MILLI, so a zero must not be read as a millisecond
 * column.  When the mapping carries nothing, the spec precision is used: mnode
 * derives it from the trigger table's schema, which is exactly the transport
 * precision of that table.  Everything else falls back to MICRO, the
 * connector's own default. */
static int8_t extSpecTsSourcePrecision(const SStreamExtTriggerSpec *pSpec, const char *tsCol) {
  if (pSpec != NULL) {
    if (pSpec->pColMappings != NULL && tsCol != NULL) {
      for (int32_t i = 0; i < pSpec->numColMappings; ++i) {
        if (strcmp(pSpec->pColMappings[i].colName, tsCol) == 0) {
          int8_t prec = pSpec->pColMappings[i].tsPrecision;
          if (prec != 0 && extIsValidTimePrecision(prec)) {
            return prec;
          }
          break;
        }
      }
    }
    if (extIsValidTimePrecision(pSpec->precision)) {
      return pSpec->precision;
    }
  }
  return TSDB_TIME_PRECISION_MICRO;
}

/* Keep unbounded sentinels intact and clamp both signs before scaling to nanoseconds. */
static int64_t extInfluxBoundToNanos(int64_t ts, int8_t precision, bool upperBound) {
  if (ts == INT64_MIN || ts == INT64_MAX || precision == TSDB_TIME_PRECISION_NANO) {
    return ts;
  }
  int64_t factor = precision == TSDB_TIME_PRECISION_MILLI ? 1000000 :
                   precision == TSDB_TIME_PRECISION_MICRO ? 1000 : 1;
  if (ts < INT64_MIN / factor) {
    stDebug("ext: clamp negative Influx bound ts:%" PRId64 " precision:%d", ts, precision);
    return INT64_MIN;
  }
  int64_t nanos = convertTimePrecision(ts, precision, TSDB_TIME_PRECISION_NANO);
  /* Integer precision conversion truncates toward zero, so negative ticks
   * extend below their multiple and tick zero spans both signs. */
  int64_t extra = factor - 1;
  if (upperBound && ts >= 0) {
    return nanos > INT64_MAX - extra ? INT64_MAX : nanos + extra;
  }
  if (!upperBound && ts <= 0) {
    return nanos < INT64_MIN + extra ? INT64_MIN : nanos - extra;
  }
  return nanos;
}

static int32_t extNormalizeTimestampBlockPrecision(int8_t fallbackPrecision, SSDataBlock *pBlock,
                                                   const SExtColTypeMapping *pMappings, int32_t nMappings,
                                                   const char *phase) {
  if (pBlock == NULL || pBlock->info.rows <= 0 || pMappings == NULL || nMappings <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t nCols = TMIN(nMappings, (int32_t)taosArrayGetSize(pBlock->pDataBlock));
  for (int32_t c = 0; c < nCols; ++c) {
    if (pMappings[c].tdType.type != TSDB_DATA_TYPE_TIMESTAMP) {
      continue;
    }

    SColumnInfoData *pCol = taosArrayGet(pBlock->pDataBlock, c);
    if (pCol == NULL) {
      return terrno;
    }

    int8_t srcPrecision = pCol->info.precision;
    if (!extIsValidTimePrecision(srcPrecision)) {
      srcPrecision = pMappings[c].tsPrecision;
    }
    if (!extIsValidTimePrecision(srcPrecision)) {
      srcPrecision = pMappings[c].tdType.precision;
    }
    int8_t targetPrecision = fallbackPrecision;
    if (!extIsValidTimePrecision(targetPrecision)) {
      targetPrecision = pMappings[c].tdType.precision;
    }
    if (!extIsValidTimePrecision(targetPrecision)) {
      stWarn("ext: skip timestamp precision normalization phase:%s col:%d invalid target precision:%d",
             phase, c, targetPrecision);
      continue;
    }
    if (!extIsValidTimePrecision(srcPrecision)) {
      stWarn("ext: skip timestamp precision normalization phase:%s col:%d invalid source precision:%d",
             phase, c, srcPrecision);
      continue;
    }

    if (srcPrecision != targetPrecision) {
      for (int32_t r = 0; r < pBlock->info.rows; ++r) {
        if (colDataIsNull_s(pCol, r)) {
          continue;
        }
        int64_t *pTs = (int64_t *)colDataGetNumData(pCol, r);
        *pTs = convertTimePrecision(*pTs, srcPrecision, targetPrecision);
      }
      stDebug("ext: normalized timestamp column phase:%s col:%d rows:%" PRId64
              " precision:%d->%d",
              phase, c, (int64_t)pBlock->info.rows, srcPrecision, targetPrecision);
    }
    pCol->info.precision = targetPrecision;
  }

  return TSDB_CODE_SUCCESS;
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
  stDebug("ext: reader connection namespace source:%s db:%s", pExtSpec->sourceName, pExtSpec->extDb);
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

  if (pInfo->influxV1) {
    pInfo->pInflux18MetaEkey =
        tSimpleHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
    if (pInfo->pInflux18MetaEkey == NULL) {
      ST_TASK_ELOG("%s", "ext: tSimpleHashInit(pInflux18MetaEkey) failed");
      return terrno;
    }
  }

  return TSDB_CODE_SUCCESS;
}

static int32_t extCopyReaderSpec(SStreamExtReaderInfo *pInfo, const SStreamExtTriggerSpec *pExtSpec) {
  int32_t code = TSDB_CODE_SUCCESS;

  pInfo->spec = *pExtSpec;
  pInfo->spec.prefilter = NULL;
  pInfo->spec.triggerPrefilter = NULL;
  pInfo->spec.triggerPrefilterAst = NULL;
  pInfo->spec.tagCondJson = NULL;
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

  code = extCopyOptionalString(pExtSpec->triggerPrefilterAst, &pInfo->spec.triggerPrefilterAst,
                               "triggerPrefilterAst");
  if (code != TSDB_CODE_SUCCESS) return code;

  code = extCopyOptionalString(pExtSpec->tagCondJson, &pInfo->spec.tagCondJson, "tagCondJson");
  if (code != TSDB_CODE_SUCCESS) return code;
  stDebug("ext: copied tag condition present=%d length=%d", pInfo->spec.tagCondJson != NULL,
          pInfo->spec.tagCondJson != NULL ? (int32_t)strlen(pInfo->spec.tagCondJson) : 0);

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

static int32_t extOpenReaderConnector(const SStreamTask *pTask, const SStreamExtTriggerSpec *pExtSpec,
                                      SExtConnectorHandle **ppConn) {
  char          plainPwd[TSDB_EXT_SOURCE_PASSWORD_LEN] = {0};
  SExtSourceCfg cfg = {0};
  int32_t       code = TSDB_CODE_SUCCESS;

  decryptExtSourcePassword((const char *)pExtSpec->encryptedPassword, plainPwd);
  extBuildSourceCfg(pExtSpec, plainPwd, &cfg);
  memset(plainPwd, 0, sizeof(plainPwd));

  code = extConnectorOpen(&cfg, ppConn);
  memset(cfg.password, 0, sizeof(cfg.password));
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: extConnectorOpen failed code:%d source=%s host=%s port=%d",
                 code, pExtSpec->sourceName, pExtSpec->host, (int)pExtSpec->port);
    return code;
  }

  ST_TASK_DLOG("ext: extConnectorOpen ok, source=%s", pExtSpec->sourceName);
  return TSDB_CODE_SUCCESS;
}

/* Timezone class of the reader's source timestamp column, resolved from the
 * remote type mnode recorded in the deploy spec.
 *
 * mnode always fills the mapping slot of the remote timestamp column, so the
 * remote type name (e.g. "datetimeoffset(6)") is available even when the deploy
 * plan carries no federated scan (virtual-table trigger) or when its timestamp
 * hints do not include the bound column.  Returns
 * EXT_TIMESTAMP_TZ_CLASS_UNKNOWN when the timestamp column is not mapped.
 *
 * Exported with a void* signature because streamReader.c cannot include this
 * header (it redefines SStreamTriggerReaderInfo in its own translation unit);
 * the deploy-time resolver calls this instead of keeping a second copy of the
 * matching rules. */
int8_t streamReaderExtSpecTsTzClass(const void *pSpecVoid) {
  const SStreamExtTriggerSpec *pSpec = (const SStreamExtTriggerSpec *)pSpecVoid;
  if (pSpec == NULL || pSpec->tsColumn[0] == '\0' || pSpec->pColMappings == NULL ||
      pSpec->numColMappings <= 0) {
    return EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;
  }

  EExtSourceType srcType = (EExtSourceType)pSpec->sourceType;
  for (int32_t i = 0; i < pSpec->numColMappings; ++i) {
    const SExtColTypeMapping *pMapping = &pSpec->pColMappings[i];
    if (!extColumnNameEqual(srcType, pMapping->colName, pSpec->tsColumn)) {
      continue;
    }
    int8_t tzClass = extTimestampTypeTzClass(srcType, pMapping->extTypeName);
    stDebug("ext: resolved timestamp class from spec sourceType:%d column:%s type:%s class:%d",
            (int)pSpec->sourceType, pSpec->tsColumn, pMapping->extTypeName, (int)tzClass);
    return tzClass;
  }
  return EXT_TIMESTAMP_TZ_CLASS_UNKNOWN;
}

/* ============================================================
 * Lifecycle: open
 * ============================================================ */
int32_t streamReaderExtOpen(void *pSpec, const SStreamTask *pTask, SNode *pCalcTimeRange,
                            const char *timezone, int8_t tsTzClass,
                            SStreamExtReaderInfo **ppReaderInfo) {
  int32_t code = TSDB_CODE_SUCCESS;

  stDebug("ext: streamReaderExtOpen enter, pSpec=%p", pSpec);

  if (pSpec == NULL || ppReaderInfo == NULL) {
    stError("ext: streamReaderExtOpen invalid args");
    nodesDestroyNode(pCalcTimeRange);
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
    nodesDestroyNode(pCalcTimeRange);
    return terrno;
  }

  pInfo->pTask = pTask;
  pInfo->pCalcTimeRange = pCalcTimeRange;
  pCalcTimeRange = NULL;
  if (timezone != NULL && timezone[0] != '\0') {
    tstrncpy(pInfo->timezone, timezone, sizeof(pInfo->timezone));
  }
  pInfo->tsTzClass = tsTzClass;
  if (pInfo->tsTzClass == EXT_TIMESTAMP_TZ_CLASS_UNKNOWN) {
    /* The deploy-time resolver reads the plan's timestamp hints; fall back to
     * the remote type recorded in the spec so a virtual-table reader without a
     * federated scan still classifies a timezone-aware column. */
    pInfo->tsTzClass = streamReaderExtSpecTsTzClass(pExtSpec);
  }
  if (pInfo->tsTzClass == EXT_TIMESTAMP_TZ_CLASS_UNKNOWN &&
      ((EExtSourceType)pExtSpec->sourceType == EXT_SOURCE_SQLSERVER ||
       (EExtSourceType)pExtSpec->sourceType == EXT_SOURCE_ORACLE)) {
    /* An unclassified timestamp would render timezone-aware bounds as a client
     * wall clock, which SQL Server (datetimeoffset) and Oracle (TIMESTAMP WITH
     * TIME ZONE, whose session is pinned to UTC) read as UTC; keep the shift
     * diagnosable instead of silently dropping rows. */
    stWarn("ext: cannot classify the timestamp column, source:%s type:%d column:%s; "
           "timezone-aware bounds will be rendered in the stream timezone",
           pExtSpec->sourceName, (int)pExtSpec->sourceType, pExtSpec->tsColumn);
  }
  stDebug("ext: reader timezone=%s tsTzClass=%d",
          pInfo->timezone[0] != '\0' ? pInfo->timezone : "(server global)",
          (int)pInfo->tsTzClass);
  code = taosThreadMutexInit(&pInfo->connLock, NULL);
  if (code != TSDB_CODE_SUCCESS) goto _err;
  pInfo->connLockInited = true;

  code = extCopyReaderSpec(pInfo, pExtSpec);
  if (code != TSDB_CODE_SUCCESS) goto _err;

  /* The 1.8 provider has no SQL endpoint and a different series model; resolve
   * the dialect now so every pull takes the right path. */
  if ((EExtSourceType)pExtSpec->sourceType == EXT_SOURCE_INFLUXDB) {
    EExtSQLDialect dialect = EXT_SQL_DIALECT_INFLUXQL;
    int32_t        dcode = extConnectorValidateInfluxOptions(pExtSpec->options, NULL, &dialect);
    pInfo->influxV1 = (dcode == TSDB_CODE_SUCCESS && dialect == EXT_SQL_DIALECT_INFLUXDB_1_8);
    stDebug("ext: reader influx dialect resolved code:%d v1:%d", dcode, (int)pInfo->influxV1);
    bool triggerReader =
        pTask != NULL && ((const SStreamReaderTask *)pTask)->triggerReader != 0;
    if (pInfo->influxV1 && triggerReader && pInfo->spec.tagCondJson != NULL &&
        pInfo->spec.tagCondJson[0] != '\0') {
      code = nodesStringToNode(pInfo->spec.tagCondJson, &pInfo->pInfluxSeriesFilter);
      if (code != TSDB_CODE_SUCCESS) {
        stError("ext: parse SERIES condition failed code:%d", code);
        goto _err;
      }
    }
    if (pInfo->influxV1 && triggerReader && pInfo->spec.triggerPrefilterAst != NULL &&
        pInfo->spec.triggerPrefilterAst[0] != '\0') {
      code = nodesStringToNode(pInfo->spec.triggerPrefilterAst, &pInfo->pTriggerPrefilter);
      if (code != TSDB_CODE_SUCCESS) {
        stError("ext: parse PRE_FILTER AST failed code:%d", code);
        goto _err;
      }
    }
    if (pInfo->influxV1 && triggerReader && pInfo->spec.triggerPrefilter != NULL &&
        pInfo->spec.triggerPrefilter[0] != '\0' && pInfo->pTriggerPrefilter == NULL) {
      stError("ext: influx18 PRE_FILTER has no serialized AST, source:%s",
              pInfo->spec.sourceName);
      code = TSDB_CODE_EXT_SYNTAX_UNSUPPORTED;
      goto _err;
    }
  }

  /* triggerReader is set by stReaderTaskDeploy before the reader is opened. */
  pInfo->borrowConn = (pTask != NULL && ((const SStreamReaderTask *)pTask)->triggerReader == 0);
  if (!pInfo->borrowConn) {
    code = extOpenReaderConnector(pTask, pExtSpec, &pInfo->pConn);
    if (code != TSDB_CODE_SUCCESS) goto _err;
  }
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

  /* Release the InfluxDB 1.8 schema cache and table descriptor. */
  if (pInfo->pTriggerPrefilter) {
    nodesDestroyNode(pInfo->pTriggerPrefilter);
    pInfo->pTriggerPrefilter = NULL;
  }
  if (pInfo->pInfluxSeriesFilter) {
    nodesDestroyNode(pInfo->pInfluxSeriesFilter);
    pInfo->pInfluxSeriesFilter = NULL;
  }
  if (pInfo->pInflux18MetaEkey) {
    tSimpleHashCleanup(pInfo->pInflux18MetaEkey);
    pInfo->pInflux18MetaEkey = NULL;
  }
  extInflux18DropMetaSnapshot(pInfo);
  if (pInfo->pInflux18Meta) {
    extConnectorFreeTableSchema(pInfo->pInflux18Meta);
    pInfo->pInflux18Meta = NULL;
  }
  taosMemoryFree(pInfo->pInflux18Table);
  pInfo->pInflux18Table = NULL;

  /* Free the calc scan time range node kept for fetch-bound evaluation. */
  if (pInfo->pCalcTimeRange) {
    nodesDestroyNode(pInfo->pCalcTimeRange);
    pInfo->pCalcTimeRange = NULL;
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

  if (pInfo->connLockInited) {
    (void)taosThreadMutexDestroy(&pInfo->connLock);
    pInfo->connLockInited = false;
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
int32_t stExtReaderOpen(void *pSpec, const SStreamTask *pTask, SNode *pCalcTimeRange,
                        const char *timezone, int8_t tsTzClass, void **ppInfo) {
  SStreamExtReaderInfo *pReaderInfo = NULL;
  int32_t code =
      streamReaderExtOpen(pSpec, pTask, pCalcTimeRange, timezone, tsTzClass, &pReaderInfo);
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
 *     For InfluxDB 1.8 the empty tagset (a series without any of the known tag
 *     keys) keeps the measurement-level uid instead, so adding the first tag key
 *     to a measurement cannot change the identity of its untagged series.
 *     groupId is derived from the PARTITION BY spec, so multiple uids may
 *     share one groupId (pGroupIndex[groupId] -> SArray<uid>):
 *       - no PARTITION BY         -> groupId = MurmurHash3_64(measurement) | 1
 *                                  (all sub-tables in one group)
 *       - PARTITION BY tbname      -> groupId = uid (each sub-table its own group)
 *       - PARTITION BY <tags>      -> groupId = MurmurHash3_64(partition-tag subset) | 1
 *     pTagsetIndex maps the full tagset-string -> uid. A measurement with no
 *     tag columns falls back to a single measurement-level uid/group.
 *
 * On success all three hashes contain consistent entries.
 * maxTs is initialised to INT64_MIN (no watermark yet).
 * ============================================================ */

static int32_t appendEscapedTagsetComponent(char *buf, int32_t bufLen, int32_t *pOff, const char *value) {
  int32_t off = *pOff;
  for (const char *p = value != NULL ? value : ""; *p != '\0'; ++p) {
    if (*p == '\\' || *p == '|' || *p == '=') {
      if (off >= bufLen - 1) return TSDB_CODE_OUT_OF_RANGE;
      buf[off++] = '\\';
    }
    if (off >= bufLen - 1) return TSDB_CODE_OUT_OF_RANGE;
    buf[off++] = *p;
  }
  buf[off] = '\0';
  *pOff = off;
  return TSDB_CODE_SUCCESS;
}

/* Build a stable tagset key string from ordered tag column names and their
 * values.  Format: "col1=val1|col2=val2|...", with backslash escaping for
 * backslash, '|' and '=' inside names and values.
 * Returns the number of bytes written (not including NUL), or -1 on truncation. */
static int32_t buildTagsetKey(char *buf, int32_t bufLen,
                              const char **colNames, const char **colVals,
                              int32_t nCols) {
  int32_t off = 0;
  for (int32_t i = 0; i < nCols; i++) {
    if (appendEscapedTagsetComponent(buf, bufLen, &off, colNames[i]) != TSDB_CODE_SUCCESS) {
      return -1;
    }
    if (off >= bufLen - 1) return -1;
    buf[off++] = '=';
    if (appendEscapedTagsetComponent(buf, bufLen, &off, colVals[i] ? colVals[i] : "") !=
        TSDB_CODE_SUCCESS) {
      return -1;
    }
    if (i + 1 < nCols) {
      if (off >= bufLen - 1) return -1;
      buf[off++] = '|';
      buf[off] = '\0';
    }
  }
  return off;
}

/* Tagset key in the encoding used before escaping was introduced (name=value|name=value...,
 * nothing escaped).  The escaped key above is what the reader parses and looks tagsets up with,
 * but the persisted UID of an InfluxDB 3.x sub-table is MurmurHash3_64 over THIS byte
 * sequence.  Checkpoint watermarks (pUidMaxTs) and the PARTITION BY tbname group ids are
 * keyed by that uid, so a tag value containing '=', '|' or '\' has to keep producing its
 * historical identity across an upgrade.  Returns the number of bytes written, or -1 on
 * truncation. */
static int32_t buildRawTagsetKey(char *buf, int32_t bufLen, const char **colNames, const char **colVals,
                                 int32_t nCols) {
  int32_t off = 0;
  for (int32_t i = 0; i < nCols; i++) {
    int32_t n = snprintf(buf + off, bufLen - off, "%s=%s%s", colNames[i], colVals[i] ? colVals[i] : "",
                         (i + 1 < nCols) ? "|" : "");
    if (n < 0 || off + n >= bufLen) return -1;
    off += n;
  }
  return off;
}

/* InfluxDB 1.8 omits absent tags from series identity, and it rejects empty tag
 * values on write, so an empty tag value in a snapshot means "this series does
 * not carry the tag".  Skipping both keeps the UID of a series stable when
 * another series introduces a new tag key: without it, the tagset key of the
 * empty tagset would grow a "tag=" component per added tag key. */
static int32_t buildInflux18TagsetKey(char *buf, int32_t bufLen, SSDataBlock *pBlock, int32_t row,
                                      const char **colNames, const char **colVals, int32_t nCols) {
  int32_t off = 0;
  bool    hasComponent = false;
  for (int32_t i = 0; i < nCols; ++i) {
    SColumnInfoData *pCol = pBlock != NULL ? taosArrayGet(pBlock->pDataBlock, i) : NULL;
    if (pCol != NULL && colDataIsNull_s(pCol, row)) {
      continue;
    }
    if (colVals[i] == NULL || colVals[i][0] == '\0') {
      continue;
    }
    if (hasComponent) {
      if (off >= bufLen - 1) return -1;
      buf[off++] = '|';
    }
    if (appendEscapedTagsetComponent(buf, bufLen, &off, colNames[i]) != TSDB_CODE_SUCCESS) {
      return -1;
    }
    if (off >= bufLen - 1) return -1;
    buf[off++] = '=';
    if (appendEscapedTagsetComponent(buf, bufLen, &off, colVals[i] ? colVals[i] : "") !=
        TSDB_CODE_SUCCESS) {
      return -1;
    }
    hasComponent = true;
  }
  buf[off] = '\0';
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
  /* groupId derivation (DS section 6.2.7):
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
      /* A tag value longer than EXT_INFLUX_TAG_NCHAR_CHARS - TSDB_NCHAR_SIZE
       * narrow characters (256) does not fit the tag column the trigger
       * partitions on, so it has to be reported as TSDB_CODE_STREAM_EXT_TAG_INVALID
       * instead of being accepted.  Only the InfluxDB 1.8 tagset-key path
       * (extInflux18RowTagsetKey) passes the full cell width: there the value is
       * a prefix of the series key and an oversized one must not fail the pull. */
      int32_t code = extColCellToStr(pCol, row, pValBuf,
                                     EXT_INFLUX_TAG_NCHAR_CHARS - TSDB_NCHAR_SIZE);
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

/* InfluxDB 1.8 keeps one uid for the empty tagset.  A measurement that has no
 * tag keys yet registers its only series under this measurement-level uid, and
 * the same uid must survive once another series introduces a tag key: the
 * schema reset rebuilds the UID index from scratch, so re-deriving the empty
 * tagset from the current tag columns would otherwise change that series'
 * identity and drop the trigger-side watermark persisted under the old uid. */
static uint64_t extInflux18MeasurementUid(const SStreamExtReaderInfo *pInfo) {
  char    key[TSDB_TABLE_NAME_LEN * 2] = {0};
  int32_t keyLen =
      snprintf(key, sizeof(key), "%s.%s", extTableQualifier(&pInfo->spec), pInfo->spec.extTable);
  return MurmurHash3_64(key, keyLen) | 1ULL;
}

/* uid of an InfluxDB 1.8 tagset key.  The empty key is the empty tagset, which
 * keeps the measurement-level uid (see extInflux18MeasurementUid). */
static uint64_t extInflux18UidForKey(const SStreamExtReaderInfo *pInfo, const char *key, int32_t keyLen) {
  if (key == NULL || keyLen <= 0) return extInflux18MeasurementUid(pInfo);
  return MurmurHash3_64(key, keyLen) | 1ULL;
}

/* Register each distinct tag combination as a uid entry.  pBlock remains
 * owned by the caller so snapshot data can also feed the UID/Ts map. */
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
    return TSDB_CODE_OUT_OF_RANGE;
  }

  code = extEvaluatePartitionExprBlock(pDCtx, pBlock, &ppExprResults);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  code = extAllocInfluxTagValueBufs(nTags, &pValBufs);
  if (code != TSDB_CODE_SUCCESS) {
    extDestroyPartitionResultCols(ppExprResults, pDCtx->nPart);
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

    int32_t keyLen = pInfo->influxV1
                       ? buildInflux18TagsetKey(tagsetKey, sizeof(tagsetKey), pBlock, r, colNames,
                                                colVals, nTags)
                       : buildTagsetKey(tagsetKey, sizeof(tagsetKey), colNames, colVals, nTags);
    if (keyLen < 0) {
      ST_TASK_WLOG("%s", "ext: initInfluxTagPartition tagset key truncated, skipping row");
      continue;
    }

    uint64_t uid = 0;
    if (pInfo->influxV1) {
      uid = extInflux18UidForKey(pInfo, tagsetKey, keyLen);
    } else {
      /* The 3.x identity is the hash of the unescaped tagset, not of the escaped lookup key:
       * escaping was added for unambiguous parsing and must not move an existing sub-table to a
       * new uid (and a new PARTITION BY tbname group). */
      char    rawTagsetKey[EXT_TAGSET_KEY_RAW_MAX] = {0};
      int32_t rawKeyLen = buildRawTagsetKey(rawTagsetKey, sizeof(rawTagsetKey), colNames, colVals, nTags);
      if (rawKeyLen < 0) {
        ST_TASK_WLOG("%s", "ext: initInfluxTagPartition raw tagset key truncated, skipping row");
        continue;
      }
      uid = MurmurHash3_64(rawTagsetKey, rawKeyLen) | 1ULL;
    }
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
  return code;
}

/* influxDistinctBlockCb leaves pBlock owned by the caller (snapshot callers
 * reuse it afterwards), but extQueryExecForEach requires its callback to
 * destroy every block.  Wrap it for the for-each path only. */
static int32_t influxDistinctBlockCbForEach(SSDataBlock *pBlock, void *pCtx) {
  int32_t code = influxDistinctBlockCb(pBlock, pCtx);
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

  code = extQueryExecForEach(pInfo->pConn, pInfo->timezone, schemaSQL, &nameMapping, 1, influxSchemaBlockCb, pTagCols);
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
    char        colQ[EXT_TAG_KEY_QUOTE_BUF_LEN] = {0};
    if (extQuoteIdent(&pInfo->spec, pColName, colQ, sizeof(colQ)) < 0) {
      return TSDB_CODE_OUT_OF_RANGE;
    }
    ok = extSqlCat(distSQL, distSqlLen, &off, "%s%s", colQ, (i + 1 < nTags) ? ", " : "");
  }
  if (ok) {
    char tblQ[EXT_IDENT_QUOTE_BUF_LEN] = {0};
    if (extQuoteIdent(&pInfo->spec, pInfo->spec.extTable, tblQ, sizeof(tblQ)) < 0) {
      return TSDB_CODE_OUT_OF_RANGE;
    }
    ok = extSqlCat(distSQL, distSqlLen, &off, " FROM %s", tblQ);
  }
  if (ok && pInfo->spec.tagCondJson != NULL && pInfo->spec.tagCondJson[0] != '\0') {
    SNode *pCond = NULL;
    char condSql[EXT_BATCH_SQL_BUF_LEN] = {0};
    int32_t condLen = 0;
    int32_t code = nodesStringToNode(pInfo->spec.tagCondJson, &pCond);
    if (code != TSDB_CODE_SUCCESS || pCond == NULL ||
        nodesNodeToSQL(pCond, condSql, sizeof(condSql), &condLen) != TSDB_CODE_SUCCESS || condLen <= 0) {
      nodesDestroyNode(pCond);
      ST_TASK_ELOG("%s", "ext: invalid SERIES condition for distinct query");
      return TSDB_CODE_INVALID_MSG;
    }
    ok = extSqlCat(distSQL, distSqlLen, &off, " WHERE %s", condSql);
    nodesDestroyNode(pCond);
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

  code = extQueryExecForEach(pInfo->pConn, pInfo->timezone, distSQL, pMappings, nTags, influxDistinctBlockCbForEach,
                             &dCtx);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: initInfluxTagPartition distinct query failed code:%d", code);
    return code;
  }

  *pGroupCount = dCtx.nGroups;
  return TSDB_CODE_SUCCESS;
}

static int32_t extInsertInfluxSingleGroup(SStreamExtReaderInfo *pInfo) {
  const SStreamTask *pTask = pInfo->pTask;
  uint64_t uid = extInflux18MeasurementUid(pInfo);
  SArray  *pPartitionValues = NULL;
  int32_t  nPart = pInfo->spec.partitionTagCols
                       ? (int32_t)taosArrayGetSize(pInfo->spec.partitionTagCols)
                       : 0;

  if (nPart > 0) {
    pPartitionValues = taosArrayInit(nPart, sizeof(SStreamGroupValue));
    if (pPartitionValues == NULL) return terrno;

    for (int32_t i = 0; i < nPart; ++i) {
      SStreamGroupValue partValue = {.isNull = true};
      if (taosArrayPush(pPartitionValues, &partValue) == NULL) {
        int32_t code = terrno;
        taosArrayDestroyEx(pPartitionValues, tDestroySStreamGroupValue);
        ST_TASK_ELOG("ext: initInfluxTagPartition single group partition tuple alloc failed slot:%d code:%d", i, code);
        return code;
      }
    }
  }

  ST_TASK_DLOG("ext: initInfluxTagPartition no tag combos - single group uid=%" PRIu64 " partitionSlots=%d",
               uid, nPart);
  return extTableListInsertEntry(pInfo, uid, uid, "", 0, pPartitionValues);
}

/* ============================================================
 * InfluxDB 1.8 support
 *
 * 1.8 exposes InfluxQL only and the connector rejects unprepared SQL, so the
 * reader cannot reuse the 3.x SQL templates.  Instead:
 *   - the tag column list comes from the connector schema (SHOW TAG KEYS),
 *   - series enumeration and per-series watermarks come from one raw snapshot
 *     (`SELECT * FROM "db"."rp"."m" GROUP BY * ORDER BY time DESC LIMIT 1`),
 *   - data fetches use the provider's prepared raw scan with a complete tag
 *     binding and an inclusive time range.
 * A PRE_FILTER is rendered to InfluxQL by the provider for trigger-side
 * reads.  Series enumeration uses only SERIES identity, and calc readers keep
 * their own source semantics so a trigger predicate never filters calc input.
 * ============================================================ */

/* Build the minimal SExtTableNode the connector APIs expect. */
static int32_t extInflux18EnsureTable(SStreamExtReaderInfo *pInfo) {
  if (pInfo->pInflux18Table != NULL) {
    return TSDB_CODE_SUCCESS;
  }
  SExtTableNode *pTable = taosMemoryCalloc(1, sizeof(*pTable));
  if (pTable == NULL) {
    return terrno;
  }
  pTable->table.node.type = QUERY_NODE_EXTERNAL_TABLE;
  pTable->sourceType = pInfo->spec.sourceType;
  tstrncpy(pTable->sourceName, pInfo->spec.sourceName, sizeof(pTable->sourceName));
  tstrncpy(pTable->table.dbName, pInfo->spec.extDb, sizeof(pTable->table.dbName));
  tstrncpy(pTable->table.tableName, pInfo->spec.extTable, sizeof(pTable->table.tableName));
  tstrncpy(pTable->remoteTableName, pInfo->spec.extTable, sizeof(pTable->remoteTableName));
  tstrncpy(pTable->srcDatabase, pInfo->spec.extDb, sizeof(pTable->srcDatabase));
  tstrncpy(pTable->srcSchema, pInfo->spec.extSchema, sizeof(pTable->srcSchema));
  tstrncpy(pTable->srcOptions, pInfo->spec.options, sizeof(pTable->srcOptions));
  tstrncpy(pTable->schemaName, pInfo->spec.extSchema, sizeof(pTable->schemaName));
  pTable->metaVersion = (int64_t)pInfo->spec.connCfgVersion;
  pInfo->pInflux18Table = pTable;
  return TSDB_CODE_SUCCESS;
}

/* Drop the cached remote schema.  The next 1.8 operation refetches it, which is
 * what a reported schema change requires. */
static void extInflux18ResetSchema(SStreamExtReaderInfo *pInfo) {
  if (pInfo->pInflux18Meta != NULL) {
    extConnectorFreeTableSchema(pInfo->pInflux18Meta);
    pInfo->pInflux18Meta = NULL;
  }
}

/* Rebuild every derived series-index structure after a remote tag schema
 * change.  Reusing the old tagset keys would either bind new tags to '' or
 * keep removed tags in every scan. */
static int32_t extInflux18ResetSeriesState(SStreamExtReaderInfo *pInfo) {
  const SStreamTask *pTask = pInfo->pTask;
  if (pInfo->pUidIndex != NULL) {
    tSimpleHashCleanup(pInfo->pUidIndex);
    pInfo->pUidIndex = NULL;
  }
  if (pInfo->pGroupIndex != NULL) {
    tSimpleHashCleanup(pInfo->pGroupIndex);
    pInfo->pGroupIndex = NULL;
  }
  if (pInfo->pTagsetIndex != NULL) {
    tSimpleHashCleanup(pInfo->pTagsetIndex);
    pInfo->pTagsetIndex = NULL;
  }
  if (pInfo->pInflux18MetaEkey != NULL) {
    tSimpleHashCleanup(pInfo->pInflux18MetaEkey);
    pInfo->pInflux18MetaEkey = NULL;
  }
  /* The series set changed under the cached enumeration: drop it (together with
   * the cursor and the derived uid maps) so the next META pull re-reads the
   * remote rather than replaying a stale snapshot. */
  extInflux18AbortMetaEnumeration(pInfo);
  if (pInfo->pInfluxTagCols != NULL) {
    taosArrayDestroy(pInfo->pInfluxTagCols);
    pInfo->pInfluxTagCols = NULL;
  }
  extInflux18ResetSchema(pInfo);
  pInfo->influxEmptyEnumTsMs = 0;
  pInfo->influxBatchOffset = 0;

  int32_t code = extInitReaderHashes(pInfo);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: influx18 reset series state failed code:%d", code);
    return code;
  }
  ST_TASK_DLOG("%s", "ext: influx18 series state reset after schema change");
  return TSDB_CODE_SUCCESS;
}

/* Fetch the remote schema once and keep it: the prepared raw scan validates
 * every mapping against it, and 1.8 has no catalog cache on the stream side. */
static int32_t extInflux18EnsureSchema(SStreamExtReaderInfo *pInfo, SExtConnectorHandle *pConn) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t            code = extInflux18EnsureTable(pInfo);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: influx18 build table descriptor failed code:%d", code);
    return code;
  }
  if (pInfo->pInflux18Meta != NULL) {
    return TSDB_CODE_SUCCESS;
  }
  SExtTableMeta *pMeta = NULL;
  code = extConnectorGetTableSchema(pConn, pInfo->pInflux18Table, &pMeta);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: influx18 getTableSchema failed code:%d source:%s table:%s",
                 code, pInfo->spec.sourceName, pInfo->spec.extTable);
    return code;
  }
  pInfo->pInflux18Meta = pMeta;
  ST_TASK_DLOG("ext: influx18 cached schema cols:%d rp:%s", pMeta->numOfCols, pMeta->retentionPolicy);
  return TSDB_CODE_SUCCESS;
}

/* Tag column names in schema order.  The element type matches the 3.x tag
 * array so every downstream helper (tagset keys, PARTITION BY, SQL quoting)
 * works unchanged.  Returns NULL when the measurement has no tag keys. */
static int32_t extInflux18BuildTagCols(SStreamExtReaderInfo *pInfo, SArray **ppTagCols) {
  const SStreamTask *pTask = pInfo->pTask;
  *ppTagCols = NULL;

  const SExtTableMeta *pMeta = pInfo->pInflux18Meta;
  if (pMeta == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  SArray *pTagCols = taosArrayInit(8, EXT_INFLUX_KEY_NCHAR_CHARS);
  if (pTagCols == NULL) {
    return terrno;
  }
  for (int32_t i = 0; i < pMeta->numOfCols; ++i) {
    const SExtColumnDef *pCol = &pMeta->pCols[i];
    if (!pCol->isTag || pCol->isPrimaryKey) {
      continue;
    }
    char *nameBuf = (char *)taosArrayReserve(pTagCols, 1);
    if (nameBuf == NULL) {
      int32_t code = terrno;
      taosArrayDestroy(pTagCols);
      return code;
    }
    tstrncpy(nameBuf, pCol->colName, EXT_INFLUX_KEY_NCHAR_CHARS);
  }

  if (taosArrayGetSize(pTagCols) == 0) {
    taosArrayDestroy(pTagCols);
    return TSDB_CODE_SUCCESS;
  }
  ST_TASK_DLOG("ext: influx18 discovered %d tag columns", (int32_t)taosArrayGetSize(pTagCols));
  *ppTagCols = pTagCols;
  return TSDB_CODE_SUCCESS;
}

/* Series snapshot timestamps are nanoseconds (the provider requests epoch=ns);
 * the reader tracks watermarks in the trigger table precision. */
static int64_t extInflux18TsToSpec(int64_t ts, int8_t precision) {
  if (ts == INT64_MIN || ts == INT64_MAX || precision == TSDB_TIME_PRECISION_NANO ||
      !extIsValidTimePrecision(precision)) {
    return ts;
  }
  return convertTimePrecision(ts, TSDB_TIME_PRECISION_NANO, precision);
}

/* Build "tag=value|tag=value" for one snapshot row (NULL tag => empty value). */
static int32_t extInflux18RowTagsetKey(SStreamExtReaderInfo *pInfo, SSDataBlock *pBlock, int32_t row,
                                       char *keyBuf, int32_t keyLen, int32_t *pOutLen) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t            nTags = pInfo->pInfluxTagCols
                                 ? (int32_t)taosArrayGetSize(pInfo->pInfluxTagCols)
                                 : 0;
  *pOutLen = 0;
  if (nTags == 0) {
    return TSDB_CODE_SUCCESS;
  }
  if (nTags > TSDB_MAX_TAGS) {
    ST_TASK_ELOG("ext: influx18 tag count exceeds limit count:%d limit:%d", nTags, TSDB_MAX_TAGS);
    return TSDB_CODE_OUT_OF_RANGE;
  }

  char       *pValBufs = taosMemoryCalloc(nTags, EXT_INFLUX_TAG_NCHAR_CHARS);
  const char *colNames[TSDB_MAX_TAGS] = {0};
  const char *colVals[TSDB_MAX_TAGS] = {0};
  if (pValBufs == NULL) {
    return terrno;
  }

  int32_t code = TSDB_CODE_SUCCESS;
  for (int32_t c = 0; c < nTags; ++c) {
    char            *pValBuf = pValBufs + (size_t)c * EXT_INFLUX_TAG_NCHAR_CHARS;
    SColumnInfoData *pCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, c);
    colNames[c] = (const char *)taosArrayGet(pInfo->pInfluxTagCols, c);
    if (pCol != NULL && !colDataIsNull_s(pCol, row)) {
      /* pValBuf is one full EXT_INFLUX_TAG_NCHAR_CHARS slot: pass the whole width, otherwise a
       * tag value that exactly fills the column (the widest value the schema can hold) is
       * rejected here even though the remote returned it. */
      int32_t len = extColCellToStr(pCol, row, pValBuf, EXT_INFLUX_TAG_NCHAR_CHARS);
      if (len < 0) {
        ST_TASK_ELOG("ext: influx18 tag value conversion failed row:%d tag:%d code:%d", row, c, len);
        code = len;
        break;
      }
      colVals[c] = pValBuf;
    } else {
      pValBuf[0] = '\0';
      colVals[c] = "";
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    int32_t keySize =
        buildInflux18TagsetKey(keyBuf, keyLen, pBlock, row, colNames, colVals, nTags);
    if (keySize < 0) {
      /* One series whose tags do not fit the key budget must not fail the whole pull: the
       * enumeration skips such a series too (see influxDistinctBlockCb). Report it as a
       * per-series tag error so extInflux18BuildUidTsMap drops that row and keeps every other
       * series of the measurement readable. */
      ST_TASK_WLOG("ext: influx18 tagset key exceeds %d bytes, series skipped", keyLen);
      code = TSDB_CODE_STREAM_EXT_TAG_INVALID;
    } else {
      *pOutLen = keySize;
    }
  }

  taosMemoryFree(pValBufs);
  return code;
}

/* One bounded snapshot describes every series of the measurement: its tag
 * tuple and either its first or latest point time (ns). */
static int32_t extInflux18SeriesSnapshotImpl(SStreamExtReaderInfo *pInfo,
                                             SExtConnectorHandle *pConn, SArray *pTagCols,
                                             const SNode *pRowFilter, bool first,
                                             bool useTimeLower, int64_t timeLower,
                                             SSDataBlock **ppOut) {
  const SStreamTask *pTask = pInfo->pTask;
  *ppOut = NULL;

  const char *tagNames[TSDB_MAX_TAGS] = {0};
  int32_t     nTags = pTagCols != NULL ? (int32_t)taosArrayGetSize(pTagCols) : 0;
  if (nTags > TSDB_MAX_TAGS) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  for (int32_t i = 0; i < nTags; ++i) {
    tagNames[i] = (const char *)taosArrayGet(pTagCols, i);
  }

  SSDataBlock *pBlock = NULL;
  int32_t code = extInflux18EnsureSchema(pInfo, pConn);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  if (useTimeLower) {
    code = extConnectorInfluxV1SeriesSnapshotAfter(
        pConn, pInfo->pInflux18Table, pInfo->pInflux18Meta, tagNames, nTags,
        pInfo->pInfluxSeriesFilter, pRowFilter, first, pInfo->spec.precision, timeLower, &pBlock);
  } else {
    code = extConnectorInfluxV1SeriesSnapshot(
        pConn, pInfo->pInflux18Table, pInfo->pInflux18Meta, tagNames, nTags,
        pInfo->pInfluxSeriesFilter, pRowFilter, first, pInfo->spec.precision, &pBlock);
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: influx18 series snapshot failed code:%d", code);
    blockDataDestroy(pBlock);
    return code;
  }
  ST_TASK_DLOG("ext: influx18 series snapshot rows:%" PRId64 " tags:%d",
               pBlock != NULL ? pBlock->info.rows : (int64_t)-1, nTags);
  *ppOut = pBlock;
  return TSDB_CODE_SUCCESS;
}

static int32_t extInflux18SeriesSnapshotAfter(SStreamExtReaderInfo *pInfo,
                                              SExtConnectorHandle *pConn, SArray *pTagCols,
                                              const SNode *pRowFilter, bool first,
                                              int64_t timeLower, SSDataBlock **ppOut) {
  return extInflux18SeriesSnapshotImpl(pInfo, pConn, pTagCols, pRowFilter, first, true, timeLower,
                                       ppOut);
}

static int32_t extInflux18SeriesSnapshot(SStreamExtReaderInfo *pInfo, SExtConnectorHandle *pConn,
                                         SArray *pTagCols, const SNode *pRowFilter, bool first,
                                         SSDataBlock **ppOut) {
  return extInflux18SeriesSnapshotImpl(pInfo, pConn, pTagCols, pRowFilter, first, false,
                                       INT64_MIN, ppOut);
}

/* uid of the single synthetic group used by a measurement without tag keys. */
static uint64_t extInflux18SingleUid(SStreamExtReaderInfo *pInfo) {
  int32_t iter = 0;
  void   *pVal = tSimpleHashIterate(pInfo->pUidIndex, NULL, &iter);
  if (pVal == NULL) {
    return 0;
  }
  size_t keyLen = 0;
  return *(uint64_t *)tSimpleHashGetKey(pVal, &keyLen);
}

/* One series of a snapshot in the order the response reported it; META paging walks
 * this stable list so a resumed trigger response covers the remaining series. */
typedef struct SExtInflux18UidTs {
  uint64_t uid;
  int64_t  ts;
} SExtInflux18UidTs;

/* Map uid -> point time (spec precision) from a series snapshot.  When
 * ppOrder is not NULL it also receives one SExtInflux18UidTs per snapshot row in
 * response order; the map value is authoritative for the last point of a series
 * that the response reported more than once.
 *
 * keepEarliest selects which point of a duplicated series wins.  A LAST_TS or
 * META snapshot is ordered by time descending and wants the latest point, while
 * the FIRST (history head) snapshot is ordered ascending and wants the earliest.
 * A series response can repeat a timestamp while chunks are merged, so the
 * selected extreme remains explicit instead of depending on response order.
 * Keeping the maximum for the first-point query would report a point later than
 * the real first point and open the first window after data that was not read. */
static int32_t extInflux18BuildUidTsMap(SStreamExtReaderInfo *pInfo, SSDataBlock *pBlock,
                                        SSHashObj **ppMap, SArray **ppOrder, bool keepEarliest) {
  const SStreamTask *pTask = pInfo->pTask;
  *ppMap = NULL;
  if (ppOrder != NULL) {
    *ppOrder = NULL;
  }
  if (pBlock == NULL || pBlock->info.rows <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t nTags = pInfo->pInfluxTagCols
                      ? (int32_t)taosArrayGetSize(pInfo->pInfluxTagCols)
                      : 0;
  int32_t nTimeSlot = (int32_t)taosArrayGetSize(pBlock->pDataBlock) - 1;
  if (nTimeSlot < nTags) {
    ST_TASK_ELOG("ext: influx18 snapshot has %d columns, expected %d tags + time",
                 (int32_t)taosArrayGetSize(pBlock->pDataBlock), nTags);
    return TSDB_CODE_EXT_SCHEMA_CHANGED;
  }
  SColumnInfoData *pTimeCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, nTimeSlot);
  if (pTimeCol == NULL) {
    return TSDB_CODE_INTERNAL_ERROR;
  }

  uint64_t singleUid = (nTags == 0) ? extInflux18SingleUid(pInfo) : 0;
  if (nTags == 0 && singleUid == 0) {
    return TSDB_CODE_SUCCESS;
  }

  SSHashObj *pMap = tSimpleHashInit((int32_t)pBlock->info.rows,
                                    taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT));
  if (pMap == NULL) {
    return terrno;
  }
  SArray *pOrder = NULL;
  if (ppOrder != NULL) {
    pOrder = taosArrayInit((size_t)pBlock->info.rows, sizeof(SExtInflux18UidTs));
    if (pOrder == NULL) {
      tSimpleHashCleanup(pMap);
      return terrno;
    }
  }

  int32_t code = TSDB_CODE_SUCCESS;
  for (int32_t r = 0; r < pBlock->info.rows; ++r) {
    if (colDataIsNull_s(pTimeCol, r)) {
      continue;
    }
    int64_t ts = extInflux18TsToSpec(*(int64_t *)colDataGetData(pTimeCol, r), pInfo->spec.precision);
    uint64_t uid = singleUid;
    if (nTags > 0) {
      char    key[EXT_TAGSET_KEY_MAX] = {0};
      int32_t keyLen = 0;
      code = extInflux18RowTagsetKey(pInfo, pBlock, r, key, sizeof(key), &keyLen);
      if (code == TSDB_CODE_STREAM_EXT_TAG_INVALID) {
        /* An unidentifiable series (tag value the key budget cannot hold) is a property of that
         * one series: skip it the way the enumeration does instead of failing the pull, which
         * the trigger would turn into a permanent recovery loop. */
        ST_TASK_WLOG("ext: influx18 snapshot row %d has no usable tagset key, series skipped", r);
        code = TSDB_CODE_SUCCESS;
        continue;
      }
      if (code != TSDB_CODE_SUCCESS) {
        break;
      }
      uid = extInflux18UidForKey(pInfo, key, keyLen);
    }

    SExtInflux18UidTs candidate = {.uid = uid, .ts = ts};
    if (pOrder != NULL && taosArrayPush(pOrder, &candidate) == NULL) {
      code = terrno;
      break;
    }

    int64_t *pExisting = (int64_t *)tSimpleHashGet(pMap, &uid, sizeof(uid));
    if (pExisting != NULL) {
      if (keepEarliest ? (ts < *pExisting) : (ts > *pExisting)) {
        *pExisting = ts;
      }
      continue;
    }
    code = tSimpleHashPut(pMap, &uid, sizeof(uid), &ts, sizeof(ts));
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }
  }

  if (code != TSDB_CODE_SUCCESS) {
    taosArrayDestroy(pOrder);
    tSimpleHashCleanup(pMap);
    return code;
  }
  *ppMap = pMap;
  if (ppOrder != NULL) {
    *ppOrder = pOrder;
  }
  return TSDB_CODE_SUCCESS;
}

static bool extInflux18HasUnregisteredUid(SStreamExtReaderInfo *pInfo, SSHashObj *pUidTs) {
  if (pInfo == NULL || pUidTs == NULL) {
    return false;
  }

  int32_t iter = 0;
  void   *pVal = NULL;
  while (pUidTs != NULL && (pVal = tSimpleHashIterate(pUidTs, pVal, &iter)) != NULL) {
    size_t   keyLen = 0;
    uint64_t uid = *(uint64_t *)tSimpleHashGetKey(pVal, &keyLen);
    if (tSimpleHashGet(pInfo->pUidIndex, &uid, sizeof(uid)) == NULL) {
      return true;
    }
  }
  return false;
}

/* Register series newly observed in a snapshot without replacing existing
 * UID/group/tagset entries. */
static int32_t extInflux18RegisterSnapshotSeries(SStreamExtReaderInfo *pInfo,
                                                 SSDataBlock *pBlock) {
  const SStreamTask *pTask = pInfo->pTask;
  if (pInfo->pInfluxTagCols == NULL || pBlock == NULL || pBlock->info.rows <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SInfluxPartitionPlan plan = {0};
  int32_t             code = extResolveInfluxPartitionPlan(pInfo, pInfo->pInfluxTagCols, &plan);
  if (code != TSDB_CODE_SUCCESS) {
    extDestroyInfluxPartitionPlan(&plan);
    return code;
  }

  SInfluxDistinctCtx dCtx = {
      .pInfo = pInfo,
      .pTagCols = pInfo->pInfluxTagCols,
      .nTags = (int32_t)taosArrayGetSize(pInfo->pInfluxTagCols),
      .nGroups = 0,
      .groupMode = plan.groupMode,
      .measurementGroupId = plan.measurementGroupId,
      .partIdx = plan.partIdx,
      .nPart = plan.nPart,
      .partExprNodes = plan.partExprNodes,
  };
  code = influxDistinctBlockCb(pBlock, &dCtx);
  extDestroyInfluxPartitionPlan(&plan);
  ST_TASK_DLOG("ext: influx18 register snapshot discovered %d new series", dCtx.nGroups);
  return code;
}

/* InfluxDB 1.8: tag columns come from the connector schema.  Series identity
 * enumeration uses bounded snapshots without inheriting the trigger
 * PRE_FILTER, so later data changes cannot remove an existing series from the
 * UID index. */
static int32_t extInitInflux18TagPartition(SStreamExtReaderInfo *pInfo) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t            code = TSDB_CODE_SUCCESS;
  SArray            *pTagCols = NULL;
  SSDataBlock       *pBlock = NULL;
  SInfluxPartitionPlan plan = {0};
  int32_t            nGroups = 0;
  bool               tagColsAdopted = false;

  int64_t nowMs = taosGetTimestampMs();
  if (pInfo->influxEmptyEnumTsMs != 0 &&
      nowMs - pInfo->influxEmptyEnumTsMs < EXT_INFLUX_EMPTY_ENUM_RECHECK_MS) {
    ST_TASK_DLOG("ext: skip repeated influx18 enumeration after empty SERIES match, ageMs:%" PRId64,
                 nowMs - pInfo->influxEmptyEnumTsMs);
    return TSDB_CODE_SUCCESS;
  }
  pInfo->influxEmptyEnumTsMs = 0;

  if (pInfo->pInfluxTagCols != NULL) {
    taosArrayDestroy(pInfo->pInfluxTagCols);
    pInfo->pInfluxTagCols = NULL;
  }

  code = extInflux18EnsureSchema(pInfo, pInfo->pConn);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }
  code = extInflux18BuildTagCols(pInfo, &pTagCols);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }
  if (pTagCols == NULL) {
    code = extInsertInfluxSingleGroup(pInfo);
    goto _exit;
  }
  code = extResolveInfluxPartitionPlan(pInfo, pTagCols, &plan);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  code = extInflux18SeriesSnapshot(pInfo, pInfo->pConn, pTagCols, NULL, false, &pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    goto _exit;
  }

  // Publish the tag column list only after the snapshot succeeds.  The UID
  // builder below uses this list, while an earlier failure must leave no
  // adopted state for the next retry to overwrite.
  pInfo->pInfluxTagCols = pTagCols;
  pTagCols = NULL;
  tagColsAdopted = true;

  int32_t nTags = (int32_t)taosArrayGetSize(pInfo->pInfluxTagCols);
  if (pBlock != NULL && pBlock->info.rows > 0) {
    SInfluxDistinctCtx dCtx = {
      .pInfo = pInfo,
      .pTagCols = pInfo->pInfluxTagCols,
      .nTags = nTags,
      .nGroups = 0,
      .groupMode = plan.groupMode,
      .measurementGroupId = plan.measurementGroupId,
      .partIdx = plan.partIdx,
      .nPart = plan.nPart,
      .partExprNodes = plan.partExprNodes,
    };
    code = influxDistinctBlockCb(pBlock, &dCtx);
    blockDataDestroy(pBlock);
    pBlock = NULL;
    nGroups = dCtx.nGroups;
    if (code != TSDB_CODE_SUCCESS) {
      goto _exit;
    }
  }

  if (nGroups > 0) {
    ST_TASK_DLOG("ext: influx18 initTableList inserted %d series groups", nGroups);
    goto _exit;
  }

  // Tagged measurements must not cache a synthetic measurement-wide UID when
  // no series exists yet: later series derive tagset-specific UIDs and would
  // never be registered in the one-shot UID index.  Leave it empty and let the
  // existing empty-enumeration recheck discover new series.
  pInfo->influxEmptyEnumTsMs = taosGetTimestampMs();
  ST_TASK_DLOG("%s", "ext: influx18 matched no series");

_exit:
  extDestroyInfluxPartitionPlan(&plan);
  if (pBlock != NULL) {
    blockDataDestroy(pBlock);
  }
  if (pTagCols != NULL) {
    taosArrayDestroy(pTagCols);
  }
  if (code != TSDB_CODE_SUCCESS && tagColsAdopted && pInfo->pInfluxTagCols != NULL &&
      tSimpleHashGetSize(pInfo->pUidIndex) == 0) {
    taosArrayDestroy(pInfo->pInfluxTagCols);
    pInfo->pInfluxTagCols = NULL;
  }
  return code;
}

/* InfluxDB 1.8 LAST_TS: every series' latest point (all history). */
static int32_t handleLastTsPullInflux18(SStreamExtReaderInfo *pInfo, SArray *pLastTsArr) {
  const SStreamTask *pTask = pInfo->pTask;
  SSDataBlock       *pBlock = NULL;
  SSHashObj         *pUidTs = NULL;
  // Series identity is enumerated separately without PRE_FILTER. Here the row
  // filter must participate in the watermark snapshot: otherwise a non-matching
  // latest point advances the metadata candidate while DATA_EXT stays empty,
  // causing the trigger to repeatedly poll the same UID and timestamp.
  int32_t code =
      extInflux18SeriesSnapshot(pInfo, pInfo->pConn, pInfo->pInfluxTagCols,
                                pInfo->pTriggerPrefilter, false, &pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  code = extInflux18BuildUidTsMap(pInfo, pBlock, &pUidTs, NULL, false);
  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pBlock);
    return code;
  }
  if (extInflux18HasUnregisteredUid(pInfo, pUidTs)) {
    code = extInflux18RegisterSnapshotSeries(pInfo, pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      tSimpleHashCleanup(pUidTs);
      blockDataDestroy(pBlock);
      return code;
    }
  }

  int32_t iter = 0;
  void   *pVal = NULL;
  while ((pVal = tSimpleHashIterate(pInfo->pUidIndex, pVal, &iter)) != NULL) {
    size_t          keyLen = 0;
    uint64_t        uid = *(uint64_t *)tSimpleHashGetKey(pVal, &keyLen);
    SUidIndexEntry *pEnt = (SUidIndexEntry *)pVal;
    int64_t        *pTs = (pUidTs != NULL) ? (int64_t *)tSimpleHashGet(pUidTs, &uid, sizeof(uid)) : NULL;
    SExtLastTsInfo  info = {
      .uid = (int64_t)uid,
      .gid = (int64_t)pEnt->groupId,
      .ts = pTs != NULL ? *pTs : INT64_MIN,
    };
    if (taosArrayPush(pLastTsArr, &info) == NULL) {
      code = terrno;
      break;
    }
  }

  ST_TASK_DLOG("ext: influx18 lastTs returned %d entries",
               (int32_t)taosArrayGetSize(pLastTsArr));
  tSimpleHashCleanup(pUidTs);
  blockDataDestroy(pBlock);
  return code;
}

/* Mapping set covering the whole schema in schema order, used when the plan did
 * not publish an explicit column list. */
static int32_t extInflux18FullMappings(SStreamExtReaderInfo *pInfo, SExtColTypeMapping **ppMappings,
                                       int32_t *pNumMappings) {
  const SStreamTask *pTask = pInfo->pTask;
  *ppMappings = NULL;
  *pNumMappings = 0;
  const SExtTableMeta *pMeta = pInfo->pInflux18Meta;
  if (pMeta == NULL || pMeta->numOfCols <= 0) {
    return TSDB_CODE_SUCCESS;
  }

  SExtColTypeMapping *pMappings = taosMemoryCalloc(pMeta->numOfCols, sizeof(*pMappings));
  if (pMappings == NULL) {
    return terrno;
  }
  int32_t n = 0;
  for (int32_t i = 0; i < pMeta->numOfCols; ++i) {
    const SExtColumnDef *pCol = &pMeta->pCols[i];
    SDataType            tdType = {0};
    int32_t              code = extTypeNameToTDengineType(EXT_SOURCE_INFLUXDB, pCol->extTypeName, "", &tdType);
    if (code != TSDB_CODE_SUCCESS) {
      taosMemoryFree(pMappings);
      ST_TASK_ELOG("ext: influx18 map column '%s' type '%s' failed code:%d",
                   pCol->colName, pCol->extTypeName, code);
      return code;
    }
    pMappings[n].tdType = tdType;
    tstrncpy(pMappings[n].colName, pCol->colName, sizeof(pMappings[n].colName));
    ++n;
  }
  *ppMappings = pMappings;
  *pNumMappings = n;
  return TSDB_CODE_SUCCESS;
}

/* Split a tagset key ("tag=value|tag=value") into borrowed name/value pairs.
 * The scratch buffer receives a private copy; the pairs point into it. */
static int32_t extInflux18ParseTagBindings(const char *tagsetKey, char *scratch, int32_t scratchLen,
                                           SExtInfluxTagValue *pBindings, int32_t maxBindings,
                                           int32_t *pNumBindings) {
  *pNumBindings = 0;
  if (tagsetKey == NULL || tagsetKey[0] == '\0') {
    return TSDB_CODE_SUCCESS;
  }
  if (scratchLen <= (int32_t)strlen(tagsetKey)) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  tstrncpy(scratch, tagsetKey, scratchLen);

  int32_t n = 0;
  char   *pair = scratch;
  while (pair != NULL && *pair != '\0') {
    char *next = findUnescapedTagsetChar(pair, '|');
    if (next != NULL) *next = '\0';
    char *eq = findUnescapedTagsetChar(pair, '=');
    if (eq == NULL) {
      pair = next != NULL ? next + 1 : NULL;
      continue;
    }
    *eq = '\0';
    unescapeTagsetComponent(pair);
    unescapeTagsetComponent(eq + 1);
    if (n >= maxBindings) {
      return TSDB_CODE_OUT_OF_RANGE;
    }
    pBindings[n].pName = pair;
    pBindings[n].pValue = eq + 1;
    ++n;
    pair = next != NULL ? next + 1 : NULL;
  }
  *pNumBindings = n;
  return TSDB_CODE_SUCCESS;
}

/* A tagged measurement can contain an untagged series. Its sparse identity key
 * is empty, but scans still need explicit empty bindings for every known tag
 * so the read targets that series instead of the whole measurement. */
static int32_t extInflux18FillEmptyTagBindings(SStreamExtReaderInfo *pInfo,
                                               SExtInfluxTagValue *pBindings, int32_t maxBindings,
                                               int32_t *pNumBindings) {
  if (*pNumBindings != 0 || pInfo->pInfluxTagCols == NULL) {
    return TSDB_CODE_SUCCESS;
  }
  int32_t nTags = (int32_t)taosArrayGetSize(pInfo->pInfluxTagCols);
  if (nTags > maxBindings) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  for (int32_t i = 0; i < nTags; ++i) {
    pBindings[i].pName = taosArrayGet(pInfo->pInfluxTagCols, i);
    pBindings[i].pValue = "";
  }
  *pNumBindings = nTags;
  return TSDB_CODE_SUCCESS;
}

/* Execute one prepared InfluxDB 1.8 raw scan and drain it into one block.
 * pTags describes the complete series binding (unlisted tags are bound to ''
 * by the provider); passing none scans every series of the measurement. */
static int32_t extInflux18ExecScan(SStreamExtReaderInfo *pInfo, SExtConnectorHandle *pConn,
                                   const SExtColTypeMapping *pMappings, int32_t nMappings,
                                   const SExtInfluxTagValue *pTags, int32_t nTags,
                                   const SNode *pFilter,
                                   int64_t skey, int64_t ekey, bool timeAscending,
                                   SSDataBlock **ppOut) {
  const SStreamTask *pTask = pInfo->pTask;
  *ppOut = NULL;

  int8_t precision = pInfo->spec.precision;
  if (!extIsValidTimePrecision(precision)) {
    precision = TSDB_TIME_PRECISION_NANO;
  }

  SExtInfluxScanSpec spec = {0};
  spec.pTable = pInfo->pInflux18Table;
  spec.pColMappings = pMappings;
  spec.numColMappings = nMappings;
  spec.pTagBindings = pTags;
  spec.numTagBindings = nTags;
  spec.pSeriesFilter = pInfo->pInfluxSeriesFilter;
  spec.pExtraFilter = pFilter;
  spec.virtualScan = true;
  spec.hasSeriesBinding = (nTags > 0);
  spec.timeAscending = timeAscending;
  spec.scanRange.skey = skey;
  spec.scanRange.ekey = ekey;
  spec.rangePrecision = precision;

  char *sql = NULL;
  int32_t code = TSDB_CODE_SUCCESS;
  for (int32_t attempt = 0; attempt < 2; ++attempt) {
    code = extInflux18EnsureSchema(pInfo, pConn);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    /* EnsureSchema materializes pInflux18Table on first use; the prepared scan
     * validates spec.pTable, so refresh it now (a calc reader's very first
     * fetch reaches here without any prior snapshot having built the table). */
    spec.pTable = pInfo->pInflux18Table;
    spec.pCurrentMeta = pInfo->pInflux18Meta;
    code = extConnectorPrepareInfluxV1Scan(pConn, &spec, &sql);
    if (code == TSDB_CODE_EXT_SCHEMA_CHANGED && attempt == 0 && pInfo->borrowConn) {
      extInflux18ResetSchema(pInfo);
      taosMemoryFree(sql);
      sql = NULL;
      continue;
    }
    break;
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: influx18 prepare scan failed code:%d tags:%d", code, nTags);
    taosMemoryFree(sql);
    return code;
  }

  SExtQueryHandle   *pQHdl = NULL;
  SExtConnectorError connErr = {0};
  code = extConnectorExecQuery(pConn, NULL, pInfo->timezone, sql, &pQHdl, &connErr);
  taosMemoryFree(sql);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: influx18 exec scan failed code:%d msg:%s", code, connErr.remoteMessage);
    return code;
  }

  SSDataBlock *pMerged = NULL;
  while (true) {
    SSDataBlock *pBlock = NULL;
    code = extConnectorFetchBlock(pQHdl, pMappings, nMappings, &pBlock, &connErr);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("ext: influx18 fetch block failed code:%d msg:%s", code, connErr.remoteMessage);
      break;
    }
    if (pBlock == NULL) {
      break;
    }
    if (pMerged == NULL) {
      pMerged = pBlock;
    } else {
      code = blockDataMerge(pMerged, pBlock);
      blockDataDestroy(pBlock);
      if (code != TSDB_CODE_SUCCESS) {
        ST_TASK_ELOG("ext: influx18 merge block failed code:%d", code);
        break;
      }
    }
  }
  extConnectorCloseQuery(pQHdl);

  if (code != TSDB_CODE_SUCCESS) {
    blockDataDestroy(pMerged);
    return code;
  }
  if (pMerged != NULL) {
    code = extNormalizeTimestampBlockPrecision(pInfo->spec.precision, pMerged, pMappings, nMappings,
                                               "extInflux18ExecScan");
    if (code != TSDB_CODE_SUCCESS) {
      blockDataDestroy(pMerged);
      return code;
    }
  }
  *ppOut = pMerged;
  return TSDB_CODE_SUCCESS;
}

/* First point of one series newer than wm, read from the head of the same
 * prepared raw scan.  META needs the real first new timestamp (not wm+1):
 * the trigger aligns its first window at the reported skey, so a synthesized
 * boundary would open an extra empty window before the data.  Only the first
 * block is consumed; the query is then cancelled instead of draining it. */
static int32_t extInflux18SeriesFirstTs(SStreamExtReaderInfo *pInfo, SExtConnectorHandle *pConn,
                                        const SExtInfluxTagValue *pTags, int32_t nTags,
                                        int64_t wm, bool *pFound, int64_t *pFirstTs) {
  const SStreamTask *pTask = pInfo->pTask;
  *pFound = false;
  *pFirstTs = INT64_MIN;
  if (wm == INT64_MAX) {
    return TSDB_CODE_SUCCESS;
  }

  int32_t code = extInflux18EnsureSchema(pInfo, pConn);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  const SExtColumnDef *pTsCol = NULL;
  for (int32_t i = 0; i < pInfo->pInflux18Meta->numOfCols; ++i) {
    if (pInfo->pInflux18Meta->pCols[i].isPrimaryKey) {
      pTsCol = &pInfo->pInflux18Meta->pCols[i];
      break;
    }
  }
  if (pTsCol == NULL) {
    ST_TASK_ELOG("%s", "ext: influx18 schema has no primary timestamp column");
    return TSDB_CODE_EXT_SCHEMA_CHANGED;
  }

  SExtColTypeMapping tsMapping = {0};
  tsMapping.tdType.type = TSDB_DATA_TYPE_TIMESTAMP;
  tsMapping.tdType.bytes = sizeof(int64_t);
  tsMapping.tdType.precision = TSDB_TIME_PRECISION_NANO;
  tstrncpy(tsMapping.colName, pTsCol->colName, sizeof(tsMapping.colName));

  int8_t precision = pInfo->spec.precision;
  if (!extIsValidTimePrecision(precision)) {
    precision = TSDB_TIME_PRECISION_NANO;
  }

  SExtInfluxScanSpec spec = {0};
  spec.pTable = pInfo->pInflux18Table;
  spec.pColMappings = &tsMapping;
  spec.numColMappings = 1;
  spec.pTagBindings = pTags;
  spec.numTagBindings = nTags;
  spec.pSeriesFilter = pInfo->pInfluxSeriesFilter;
  // PRE_FILTER defines which rows participate in trigger evaluation. The first
  // matching row, rather than the first row of the series, must align the first
  // window.
  spec.pExtraFilter = pInfo->pTriggerPrefilter;
  spec.virtualScan = true;
  spec.hasSeriesBinding = (nTags > 0);
  spec.timeAscending = true;
  /* Only the earliest matching row is needed, and a recovery round runs this probe once per
   * series: without a cap the whole history of the series is transferred for a single value. */
  spec.rowLimit = 1;
  spec.scanRange.skey = (wm == INT64_MIN) ? INT64_MIN : wm + 1;  // inclusive >= wm+1 == > wm
  spec.scanRange.ekey = INT64_MAX;
  spec.rangePrecision = precision;

  char *sql = NULL;
  for (int32_t attempt = 0; attempt < 2; ++attempt) {
    if (attempt > 0) {
      code = extInflux18EnsureSchema(pInfo, pConn);
      if (code != TSDB_CODE_SUCCESS) return code;
    }
    spec.pCurrentMeta = pInfo->pInflux18Meta;
    code = extConnectorPrepareInfluxV1Scan(pConn, &spec, &sql);
    if (code == TSDB_CODE_EXT_SCHEMA_CHANGED && attempt == 0 && pInfo->borrowConn) {
      extInflux18ResetSchema(pInfo);
      taosMemoryFree(sql);
      sql = NULL;
      continue;
    }
    break;
  }
  if (code != TSDB_CODE_SUCCESS) {
    taosMemoryFree(sql);
    return code;
  }

  SExtQueryHandle   *pQHdl = NULL;
  SExtConnectorError connErr = {0};
  code = extConnectorExecQuery(pConn, NULL, pInfo->timezone, sql, &pQHdl, &connErr);
  taosMemoryFree(sql);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: influx18 first-ts scan failed code:%d msg:%s", code, connErr.remoteMessage);
    return code;
  }

  SSDataBlock *pBlock = NULL;
  code = extConnectorFetchBlock(pQHdl, &tsMapping, 1, &pBlock, &connErr);
  if (code == TSDB_CODE_SUCCESS && pBlock != NULL && pBlock->info.rows > 0) {
    SColumnInfoData *pCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, 0);
    if (pCol != NULL && !colDataIsNull_s(pCol, 0)) {
      *pFirstTs = extInflux18TsToSpec(*(int64_t *)colDataGetData(pCol, 0), pInfo->spec.precision);
      *pFound = true;
    }
  }
  blockDataDestroy(pBlock);
  extConnectorCloseQuery(pQHdl);

  ST_TASK_DLOG("ext: influx18 first-ts wm:%" PRId64 " found:%d ts:%" PRId64 " code:%d",
               wm, (int)*pFound, *pFirstTs, code);
  return code;
}

/* InfluxDB: query information_schema.columns for tag column names,
 * then SELECT DISTINCT <tags> to enumerate live tag combinations.
 * Each combination  ->  one uid/group entry. */
static int32_t extInitInfluxTagPartition(SStreamExtReaderInfo *pInfo) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t               code = TSDB_CODE_SUCCESS;
  int32_t               nGroups = 0;
  SArray               *pTagCols = NULL;
  SExtColTypeMapping   *pMappings = NULL;
  SInfluxPartitionPlan  plan = {0};

  // A SERIES condition that matched no tag combinations leaves pUidIndex empty, which would
  // otherwise re-run the information_schema query + DISTINCT enumeration on every idle pull.
  // Re-enumerate only after the re-check window so data that appears later is still picked up.
  int64_t nowMs = taosGetTimestampMs();
  if (pInfo->influxEmptyEnumTsMs != 0 &&
      nowMs - pInfo->influxEmptyEnumTsMs < EXT_INFLUX_EMPTY_ENUM_RECHECK_MS) {
    ST_TASK_DLOG("ext: skip repeated Influx enumeration after empty SERIES match, ageMs:%" PRId64,
                 nowMs - pInfo->influxEmptyEnumTsMs);
    return TSDB_CODE_SUCCESS;
  }
  pInfo->influxEmptyEnumTsMs = 0;

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

  if (pInfo->spec.tagCondJson != NULL && pInfo->spec.tagCondJson[0] != '\0') {
    ST_TASK_DLOG("%s", "ext: replacing InfluxDB tag columns after empty SERIES match");
    taosArrayDestroy(pInfo->pInfluxTagCols);
    pInfo->pInfluxTagCols = pTagCols;
    pTagCols = NULL;
    pInfo->influxEmptyEnumTsMs = taosGetTimestampMs();
    ST_TASK_DLOG("%s", "ext: initInfluxTagPartition SERIES condition matched no tag combinations");
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

  if (pInfo->influxV1) {
    /* InfluxDB 1.8: schema tag keys + one LAST(*) snapshot enumerate the series
     * and their latest point; the 3.x information_schema/DISTINCT statements
     * do not exist there. */
    code = extInitInflux18TagPartition(pInfo);
  } else if (isInflux) {
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

static int32_t extHandlePullLocked(SStreamExtReaderInfo *pInfo,
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
  bool                       schemaRefreshed = false;
  bool retryAfterRefresh =
      typedPullType == STRIGGER_PULL_LAST_TS_EXT || typedPullType == STRIGGER_PULL_META_EXT;

  /* ---- Initial table-list initialization ----
   * For ext-source readers, pUidIndex is built by querying the external DB
   * (InfluxDB tag combinations, or a single synthetic uid for MySQL/PG).
   * Whenever pUidIndex is empty we re-initialize from the source before
   * dispatching  -  regardless of pull type.  InfluxDB 1.8 can additionally
   * register newly observed series from later snapshots. */
_ensure_table_list:
  if (tSimpleHashGetSize(pInfo->pUidIndex) == 0) {
    ST_TASK_DLOG("ext: handlePull: uid hash empty (pullType=%d), initializing table list",
                 pullType);
    code = streamExtReaderInitTableList(pInfo);
    if (code == TSDB_CODE_EXT_SCHEMA_CHANGED && pInfo->influxV1 &&
        !pInfo->borrowConn && !schemaRefreshed) {
      code = extInflux18ResetSeriesState(pInfo);
      if (code != TSDB_CODE_SUCCESS) {
        goto _exit;
      }
      if (!retryAfterRefresh) {
        code = TSDB_CODE_EXT_SCHEMA_CHANGED;
        goto _exit;
      }
      code = streamExtReaderInitTableList(pInfo);
      if (code != TSDB_CODE_SUCCESS) {
        ST_TASK_ELOG("ext: initTableList after schema refresh failed code:%d", code);
        goto _exit;
      }
      schemaRefreshed = true;
      goto _allocate_rsp;
    }
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("ext: initTableList failed code:%d - aborting pull", code);
      goto _exit;
    }
  }

_allocate_rsp:
  pRsp = allocPullRsp(typedPullType);
  if (pRsp == NULL) {
    code = terrno;
    ST_TASK_ELOG("%s", "ext: handlePull OOM allocating rsp");
    goto _exit;
  }
  pRsp->deploymentGeneration = pReq->deploymentGeneration;

  code = extDispatchPullRequest(pInfo, typedPullType, pReq, pRsp);
  if (code == TSDB_CODE_EXT_SCHEMA_CHANGED && pInfo->influxV1 &&
      !pInfo->borrowConn && !schemaRefreshed) {
    ST_TASK_DLOG("%s", "ext: influx18 schema changed, rebuilding series state");
    streamExtPullRspFree(pRsp);
    pRsp = NULL;
    code = extInflux18ResetSeriesState(pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      goto _exit;
    }
    if (!retryAfterRefresh) {
      code = TSDB_CODE_EXT_SCHEMA_CHANGED;
      goto _exit;
    }
    code = streamExtReaderInitTableList(pInfo);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("ext: initTableList after schema refresh failed code:%d", code);
      goto _exit;
    }
    schemaRefreshed = true;
    goto _allocate_rsp;
  }
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

int32_t streamReaderExtHandlePull(SStreamExtReaderInfo *pInfo,
                                  int32_t pullType,
                                  const void *pReqVoid,
                                  void **ppRsp) {
  if (pInfo == NULL || ppRsp == NULL) {
    stError("ext: handlePull invalid args");
    return TSDB_CODE_INVALID_PARA;
  }

  (void)taosThreadMutexLock(&pInfo->connLock);
  int32_t code = extHandlePullLocked(pInfo, pullType, pReqVoid, ppRsp);
  (void)taosThreadMutexUnlock(&pInfo->connLock);
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
 * table  -  see extInitSingleGroup), so a single global MAX(ts) trivially
 * applies to the only uid.
 * SQL: SELECT MAX(<tsCol>) FROM <extDb>.<extTable> [WHERE prefilter]
 * ============================================================ */
static int32_t handleLastTsPullRelational(SStreamExtReaderInfo *pInfo,
                                          const char *tsCol,
                                          SArray *pLastTsArr) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t            code  = TSDB_CODE_SUCCESS;

  char sqlBuf[EXT_SIMPLE_SQL_BUF_LEN] = {0};
  char prefilterBuf[EXT_SQL_PREFILTER_BUF_LEN] = {0};
  /* Trigger reader uses PRE_FILTER (triggerPrefilter), not the calc WHERE. */
  code = buildTriggerPrefilterClause(&pInfo->spec, prefilterBuf, sizeof(prefilterBuf));
  if (code < 0) return code;

  /* Build SQL: SELECT MAX("ts") AS "ts" FROM <table> [WHERE triggerPrefilter]
   * Both the argument and the AS alias are quoted so PostgreSQL keeps the exact column name
   * (e.g. a remote "Ts" column) instead of folding it to lower case; the alias then still
   * matches tsMapping.colName. */
  char tblRef[EXT_TBL_REF_BUF_LEN] = {0};
  code = buildExtTableRef(&pInfo->spec, tblRef, sizeof(tblRef));
  if (code < 0) return code;
  char tsColQ[EXT_IDENT_QUOTE_BUF_LEN] = {0};
  code = extQuoteIdent(&pInfo->spec, tsCol, tsColQ, sizeof(tsColQ));
  if (code < 0) return TSDB_CODE_OUT_OF_RANGE;
  int32_t off = 0;
  bool    ok  = extSqlCat(sqlBuf, sizeof(sqlBuf), &off,
                          "SELECT MAX(%s) AS %s FROM %s",
                          tsColQ, tsColQ, tblRef);
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
   * Decode the source timestamp at its own transport precision, then normalize
   * the result to spec.precision before exposing the watermark to the trigger.
   * The Oracle connector reads tsPrecision, so a zero here silently truncates
   * TIMESTAMP(7..9) to microseconds and the row whose timestamp only differs in
   * the sub-microsecond digits is excluded from every later fetch. */
  SExtColTypeMapping tsMapping = {0};
  int8_t            srcPrec   = extSpecTsSourcePrecision(&pInfo->spec, tsCol);
  tsMapping.tdType.type       = TSDB_DATA_TYPE_TIMESTAMP;
  tsMapping.tdType.bytes      = sizeof(int64_t);
  tsMapping.tdType.precision  = srcPrec;
  tsMapping.tsPrecision       = srcPrec;
  tstrncpy(tsMapping.colName, tsCol, sizeof(tsMapping.colName));

  SSDataBlock *pBlock = NULL;
  code = extQueryExecFetchAll(pInfo->pConn, pInfo->timezone, sqlBuf,
                              &tsMapping, 1, &pBlock);
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: handleLastTsPullRelational query failed code:%d", code);
    return code;
  }

  /* Extract max ts value from the first row, first column. */
  int64_t globalMaxTs = INT64_MIN;
  if (pBlock != NULL && pBlock->info.rows > 0) {
    code = extNormalizeTimestampBlockPrecision(pInfo->spec.precision, pBlock, &tsMapping, 1,
                                               "handleLastTsPullRelational");
    if (code != TSDB_CODE_SUCCESS) {
      blockDataDestroy(pBlock);
      return code;
    }
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
 * UID using that uid's own tagset as the WHERE filter  -  handled by the
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
  if (pInfo->influxV1) {
    code = handleLastTsPullInflux18(pInfo, pRsp->pLastTsArr);
  } else if (isInflux) {
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

static int32_t extEnsureBlockCapacityGeometric(SSDataBlock *pBlock, uint32_t needed,
                                               uint32_t initialCapacity) {
  if (needed <= pBlock->info.capacity) {
    return TSDB_CODE_SUCCESS;
  }

  uint32_t capacity = pBlock->info.capacity;
  if (capacity == 0) {
    capacity = needed > initialCapacity ? needed : initialCapacity;
  }
  while (capacity < needed) {
    if (capacity > UINT32_MAX / 2) {
      capacity = needed;
      break;
    }
    capacity *= 2;
  }
  return blockDataEnsureCapacity(pBlock, capacity);
}

/* Append one row to a metaBlock.  All values are int64. */
static int32_t metaBlockAppendRow(SSDataBlock *pBlock,
                                  uint64_t groupId, int64_t skey, int64_t ekey,
                                  uint64_t uid, int64_t rows) {
  int32_t row = pBlock->info.rows;
  /* Grow geometrically so appending a large META or demux block does not copy every
   * existing row whenever the capacity is exceeded. */
  int32_t code = extEnsureBlockCapacityGeometric(
      pBlock, (uint32_t)row + 1, EXT_META_BLOCK_INITIAL_ROWS);
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

/* ============================================================
 * handleMetaPullInflux18  -  InfluxDB 1.8 META pull.
 *
 * Two bounded snapshots yield the latest and, when a bootstrap UID needs it,
 * the first point of every matching series.  A series with a latest point
 * newer than its own watermark has new data.  For the first report of a uid the
 * META row carries the real first new timestamp so the trigger aligns its first
 * window at data rather than at watermark+1.
 *
 * The snapshot describes every series of the measurement at once, but a META
 * response must still page: the trigger infers "more pages" from
 * metaRows >= STREAM_RETURN_ROWS_NUM and re-pulls META after adopting every
 * reported watermark, so an unbounded response would be followed by a pull that
 * can no longer report a new row (every uid is already at its last point).  That
 * pull returns NO_DATA and the windows accumulated from the first response are
 * never fetched.  influxBatchOffset therefore remembers how many snapshot rows
 * this enumeration already emitted.
 * ============================================================ */
/* Release the series snapshot and the uid maps derived from it that are cached
 * for the running META enumeration (see pInflux18MetaSnap).  Called when the
 * enumeration completes or is abandoned, and when the reader resets its series
 * state, so the next enumeration re-reads the remote. */
static void extInflux18DropMetaSnapshot(SStreamExtReaderInfo *pInfo) {
  if (pInfo == NULL) return;
  if (pInfo->pInflux18MetaSnap != NULL) {
    blockDataDestroy(pInfo->pInflux18MetaSnap);
    pInfo->pInflux18MetaSnap = NULL;
  }
  if (pInfo->pInflux18MetaUidTs != NULL) {
    tSimpleHashCleanup(pInfo->pInflux18MetaUidTs);
    pInfo->pInflux18MetaUidTs = NULL;
  }
  if (pInfo->pInflux18MetaUidOrder != NULL) {
    taosArrayDestroy(pInfo->pInflux18MetaUidOrder);
    pInfo->pInflux18MetaUidOrder = NULL;
  }
  if (pInfo->pInflux18MetaFirstSnap != NULL) {
    blockDataDestroy(pInfo->pInflux18MetaFirstSnap);
    pInfo->pInflux18MetaFirstSnap = NULL;
  }
  if (pInfo->pInflux18MetaFirstUidTs != NULL) {
    tSimpleHashCleanup(pInfo->pInflux18MetaFirstUidTs);
    pInfo->pInflux18MetaFirstUidTs = NULL;
  }
  /* The next enumeration decides again whether it needs a history-head snapshot. */
  pInfo->influx18MetaFirstTried = false;
  pInfo->influx18MetaFirstLowerValid = false;
  pInfo->influx18MetaFirstLower = INT64_MAX;
}

/* Lower bound of the history-head snapshot for the running enumeration: the smallest
 * (watermark + 1) over the series that still need a FIRST alignment, or INT64_MIN when any
 * of them has no watermark yet.  Computed on demand and cached, because the scan is O(N) and
 * a steady-state enumeration - where every series was already reported at its watermark -
 * never reaches the FIRST branch at all.  Walking the map on every continuation page instead
 * turned one meta round into O(N * pages). */
static int64_t extInflux18FirstSnapshotLower(SStreamExtReaderInfo *pInfo, SArray *pUidOrder,
                                             SSHashObj *pUidTs, SSHashObj *pUidMaxTs) {
  if (pInfo->influx18MetaFirstLowerValid) {
    return pInfo->influx18MetaFirstLower;
  }
  pInfo->influx18MetaFirstLowerValid = true;
  int64_t lower = INT64_MAX;
  int32_t nCandidates = pUidOrder != NULL ? (int32_t)taosArrayGetSize(pUidOrder) : 0;
  for (int32_t candidateRow = 0; candidateRow < nCandidates; ++candidateRow) {
    SExtInflux18UidTs *pCandidate = (SExtInflux18UidTs *)taosArrayGet(pUidOrder, candidateRow);
    if (pCandidate == NULL) {
      continue;
    }
    int64_t *pLastTs = (int64_t *)tSimpleHashGet(pUidTs, &pCandidate->uid, sizeof(pCandidate->uid));
    if (pLastTs == NULL || *pLastTs != pCandidate->ts) {
      continue;
    }
    int64_t wm = extLookupUidMaxTs(pUidMaxTs, pCandidate->uid);
    if (wm != INT64_MIN && *pLastTs <= wm) {
      continue;
    }
    if (wm == INT64_MIN) {
      lower = INT64_MIN;
      break;
    }
    lower = TMIN(lower, wm + 1);
  }
  if (lower == INT64_MAX) {
    /* Defensive: the caller only asks after finding a row that needs FIRST. */
    lower = INT64_MIN;
  }
  pInfo->influx18MetaFirstLower = lower;
  return lower;
}

/* Abandon the running enumeration.  A cursor is only meaningful together with
 * the snapshot it was computed from, so both are dropped and the next META pull
 * starts a new enumeration from the remote's current series set. */
static void extInflux18AbortMetaEnumeration(SStreamExtReaderInfo *pInfo) {
  if (pInfo == NULL) return;
  pInfo->influxBatchOffset = 0;
  extInflux18DropMetaSnapshot(pInfo);
}

/* Fetch the history-head snapshot of the running enumeration on first use.  One
 * enumeration needs it only to align the first window of a series at its real
 * first timestamp, and most requests need it not at all, so it is fetched at
 * most once and only when a row actually needs it.  (Scanning the whole uid map
 * per page to decide that instead cost O(N) on every continuation page.) */
static int32_t extInflux18EnsureFirstSnapshot(SStreamExtReaderInfo *pInfo, int64_t timeLower) {
  if (pInfo->influx18MetaFirstTried) {
    return TSDB_CODE_SUCCESS;
  }
  pInfo->influx18MetaFirstTried = true;
  SSDataBlock *pBlock = NULL;
  int32_t      code = TSDB_CODE_SUCCESS;
  if (timeLower == INT64_MIN) {
    code = extInflux18SeriesSnapshot(pInfo, pInfo->pConn, pInfo->pInfluxTagCols,
                                     pInfo->pTriggerPrefilter, true, &pBlock);
  } else {
    code = extInflux18SeriesSnapshotAfter(pInfo, pInfo->pConn, pInfo->pInfluxTagCols,
                                          pInfo->pTriggerPrefilter, true, timeLower, &pBlock);
  }
  if (code != TSDB_CODE_SUCCESS) {
    /* Let the retry of this page fetch it again instead of degrading every
     * candidate to the per-uid first-point query. */
    pInfo->influx18MetaFirstTried = false;
    return code;
  }
  blockDataDestroy(pInfo->pInflux18MetaFirstSnap);
  pInfo->pInflux18MetaFirstSnap = pBlock;
  if (pInfo->pInflux18MetaFirstUidTs != NULL) {
    tSimpleHashCleanup(pInfo->pInflux18MetaFirstUidTs);
    pInfo->pInflux18MetaFirstUidTs = NULL;
  }
  /* The history head is the earliest point of the series, not the latest. */
  SSHashObj *pUidFirstTs = NULL;
  code = extInflux18BuildUidTsMap(pInfo, pBlock, &pUidFirstTs, NULL, true);
  if (code != TSDB_CODE_SUCCESS) {
    pInfo->influx18MetaFirstTried = false;
    return code;
  }
  pInfo->pInflux18MetaFirstUidTs = pUidFirstTs;
  return TSDB_CODE_SUCCESS;
}

static int32_t handleMetaPullInflux18(SStreamExtReaderInfo *pInfo, SSHashObj *pUidMaxTs,
                                      SSTriggerExtPullRsp *pRsp) {
  const SStreamTask *pTask = pInfo->pTask;
  SSDataBlock       *pBlock = NULL;
  SSHashObj         *pUidTs = NULL;
  SArray            *pUidOrder = NULL;
  /* A continuation page must neither re-read the measurement nor re-derive its
   * uid maps: one snapshot costs one HTTP request per INFLUX_V1_SERIES_PAGE_SIZE
   * series, and rebuilding the maps costs one tagset-key build per series, so
   * re-reading them for each page of an N/STREAM_RETURN_ROWS_NUM-page META
   * enumeration multiplies the whole round by the page count.  Both the
   * snapshot and the maps it yields are therefore cached on the reader for the
   * duration of one enumeration (see pInflux18MetaSnap). */
  int32_t code = TSDB_CODE_SUCCESS;
  if (pInfo->influxBatchOffset > 0 && pInfo->pInflux18MetaSnap != NULL &&
      pInfo->pInflux18MetaUidOrder != NULL) {
    pBlock = pInfo->pInflux18MetaSnap;
    pUidTs = pInfo->pInflux18MetaUidTs;
    pUidOrder = pInfo->pInflux18MetaUidOrder;
  } else {
    /* A new enumeration owns a fresh cache and starts from its first row: any
     * leftover cache or cursor belongs to an enumeration that is no longer
     * running and would index a snapshot we are about to replace. */
    extInflux18AbortMetaEnumeration(pInfo);
    // Series enumeration is independent from PRE_FILTER. Watermark snapshots must
    // apply it so metadata advances only for rows that can produce DATA_EXT rows.
    code = extInflux18SeriesSnapshot(pInfo, pInfo->pConn, pInfo->pInfluxTagCols,
                                     pInfo->pTriggerPrefilter, false, &pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pInfo->pInflux18MetaSnap = pBlock;
    code = extInflux18BuildUidTsMap(pInfo, pBlock, &pUidTs, &pUidOrder, false);
    if (code != TSDB_CODE_SUCCESS) {
      extInflux18AbortMetaEnumeration(pInfo);
      return code;
    }
    pInfo->pInflux18MetaUidTs = pUidTs;
    pInfo->pInflux18MetaUidOrder = pUidOrder;
    /* Registering the snapshot's series is a snapshot-level effect: the
     * continuation pages walk the same snapshot, whose uids are all registered
     * by now, so a per-page rescan of the whole map would only repeat work. */
    if (extInflux18HasUnregisteredUid(pInfo, pUidTs)) {
      code = extInflux18RegisterSnapshotSeries(pInfo, pBlock);
      if (code != TSDB_CODE_SUCCESS) {
        extInflux18AbortMetaEnumeration(pInfo);
        return code;
      }
    }
  }

  int32_t totalFetched = 0;
  int32_t nCandidates = pUidOrder != NULL ? (int32_t)taosArrayGetSize(pUidOrder) : 0;
  int32_t startRow = pInfo->influxBatchOffset;
  if (startRow < 0 || startRow > nCandidates) {
    startRow = 0;  // the snapshot shrank below the remembered cursor: start over
  }
  int32_t row = startRow;
  for (; code == TSDB_CODE_SUCCESS && row < nCandidates && totalFetched < STREAM_RETURN_ROWS_NUM;
       ++row) {
    SExtInflux18UidTs *pCandidate = (SExtInflux18UidTs *)taosArrayGet(pUidOrder, row);
    if (pCandidate == NULL) {
      continue;
    }
    uint64_t uid = pCandidate->uid;
    int64_t *pLastTs = (int64_t *)tSimpleHashGet(pUidTs, &uid, sizeof(uid));
    if (pLastTs == NULL || *pLastTs != pCandidate->ts) {
      continue;  // a later row of the same series carried its last point
    }
    SUidIndexEntry *pEntry = tSimpleHashGet(pInfo->pUidIndex, &uid, sizeof(uid));
    if (pEntry == NULL) {
      continue;
    }
    int64_t wm = extLookupUidMaxTs(pUidMaxTs, uid);
    if (wm != INT64_MIN && *pLastTs <= wm) {
      continue;  // no row newer than the watermark
    }

    int64_t *pReported = (pInfo->pInflux18MetaEkey != NULL)
                             ? (int64_t *)tSimpleHashGet(pInfo->pInflux18MetaEkey, &uid, sizeof(uid))
                             : NULL;
    int64_t  skey;
    if (wm != INT64_MIN && pReported != NULL && *pReported == wm) {
      /* The trigger consumed the previous META row, so its first window only
       * needs a boundary after the watermark (skey is then unused: the
       * trigger starts new windows from prevMaxTs - watermark). */
      skey = (wm == INT64_MAX) ? INT64_MAX : wm + 1;
    } else {
      /* First META row for this uid (bootstrap or an unconsumed report): the
       * trigger aligns the first window at skey, so report the real first
       * timestamp of the new rows. */
      SExtInfluxTagValue tags[TSDB_MAX_TAGS];
      char               scratch[EXT_TAGSET_KEY_MAX] = {0};
      int32_t            nTags = 0;
      code = extInflux18ParseTagBindings(pEntry->tagsetKey, scratch, sizeof(scratch), tags,
                                         TSDB_MAX_TAGS, &nTags);
      if (code != TSDB_CODE_SUCCESS) {
        ST_TASK_ELOG("ext: influx18 meta parse tagset uid:%" PRIu64 " code:%d", uid, code);
        break;
      }
      code = extInflux18FillEmptyTagBindings(pInfo, tags, TSDB_MAX_TAGS, &nTags);
      if (code != TSDB_CODE_SUCCESS) {
        ST_TASK_ELOG("ext: influx18 meta fill empty tagset uid:%" PRIu64 " code:%d", uid, code);
        break;
      }
      bool    found = false;
      int64_t firstTs = INT64_MIN;
      code = extInflux18EnsureFirstSnapshot(
          pInfo, extInflux18FirstSnapshotLower(pInfo, pUidOrder, pUidTs, pUidMaxTs));
      if (code != TSDB_CODE_SUCCESS) {
        ST_TASK_ELOG("ext: influx18 first snapshot uid:%" PRIu64 " code:%d", uid, code);
        break;
      }
      int64_t *pFirstTs = (pInfo->pInflux18MetaFirstUidTs != NULL)
                              ? (int64_t *)tSimpleHashGet(pInfo->pInflux18MetaFirstUidTs, &uid,
                                                         sizeof(uid))
                              : NULL;
      if (pFirstTs != NULL && *pFirstTs > wm) {
        /* The batched first snapshot reports each series' earliest point over
         * its whole history.  Only trust it when it is actually newer than the
         * watermark; otherwise fall back to the per-uid query below, which
         * returns the real first point newer than wm.  Taking the history head
         * here would fail the firstTs > wm guard and fall through to wm+1,
         * opening an extra empty window before the data. */
        firstTs = *pFirstTs;
        found = true;
      } else {
        code = extInflux18SeriesFirstTs(pInfo, pInfo->pConn, nTags > 0 ? tags : NULL, nTags,
                                        wm, &found, &firstTs);
        if (code != TSDB_CODE_SUCCESS) {
          break;
        }
      }
      skey = (found && firstTs > wm) ? firstTs : ((wm == INT64_MIN) ? INT64_MIN : wm + 1);
    }

    code = metaBlockAppendRow(pRsp->pMetaBlock, pEntry->groupId, skey, *pLastTs, uid, 1);
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }
    /* Remember what the trigger will adopt as its watermark. */
    if (pInfo->pInflux18MetaEkey != NULL) {
      code = tSimpleHashPut(pInfo->pInflux18MetaEkey, &uid, sizeof(uid), pLastTs, sizeof(*pLastTs));
      if (code != TSDB_CODE_SUCCESS) {
        break;
      }
    }
    ++totalFetched;
  }

  /* Resume the enumeration when this page stopped at the response row limit; a
   * finished enumeration starts over on the next META pull.  A failed page keeps
   * its cursor together with the snapshot it was computed from, so the retry
   * re-reads the same series in the same order.  When there is nothing to
   * resume from, the cache is released and the next pull starts a new
   * enumeration. */
  int32_t nextOffset;
  if (code != TSDB_CODE_SUCCESS) {
    nextOffset = startRow;
  } else if (row < nCandidates) {
    nextOffset = row;
  } else if (nCandidates > 0 && totalFetched >= STREAM_RETURN_ROWS_NUM) {
    /* This page was exactly a full response and it exhausted the snapshot.  The trigger infers
     * "more pages" from metaRows >= STREAM_RETURN_ROWS_NUM and pulls META again immediately, so
     * releasing the snapshot here would take a fresh one on the next pull; under continuous
     * writes that snapshot can again be exactly full, and the stream would never reach
     * DATA_EXT.  Keeping the cursor at the end (with the snapshot) makes the next pull return
     * the terminating empty page instead. */
    nextOffset = nCandidates;
  } else {
    nextOffset = 0;
  }
  ST_TASK_DLOG("ext: influx18 meta done code:%d uids:%d withNewData:%d rows:%" PRId64
               " candidates:%d nextOffset:%d",
               code, pUidTs != NULL ? tSimpleHashGetSize(pUidTs) : 0, totalFetched,
               pRsp->pMetaBlock ? pRsp->pMetaBlock->info.rows : (int64_t)0, nCandidates,
               nextOffset);
  pInfo->influxBatchOffset = nextOffset;
  if (nextOffset == 0) {
    extInflux18AbortMetaEnumeration(pInfo);
  }
  return code;
}

static void extInitRelationalMetaMappings(const char *tsCol, int8_t srcPrec,
                                          SExtColTypeMapping metaMappings[4]) {
  metaMappings[0].tdType.type = TSDB_DATA_TYPE_TIMESTAMP;
  metaMappings[0].tdType.bytes = sizeof(int64_t);
  metaMappings[0].tdType.precision = srcPrec;
  metaMappings[0].tsPrecision = srcPrec;
  tstrncpy(metaMappings[0].colName, tsCol, sizeof(metaMappings[0].colName));

  metaMappings[1].tdType.type = TSDB_DATA_TYPE_TIMESTAMP;
  metaMappings[1].tdType.bytes = sizeof(int64_t);
  metaMappings[1].tdType.precision = srcPrec;
  metaMappings[1].tsPrecision = srcPrec;
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
                                         int8_t boundPrecision,
                                         const SStreamExtReaderInfo *pInfo,
                                         char *sqlBuf, int32_t sqlBufLen) {
  char dtBuf[EXT_SQL_TS_LITERAL_LEN] = {0};
  /* Emit the uid as its signed 64-bit representation: group uids are
   * MurmurHash3 values with the full uint64 range, and roughly half of them
   * exceed INT64_MAX.  The connector parses this literal back into an
   * int64_t with a strict converter (ERANGE on > INT64_MAX) and the column
   * is stored as int64 anyway, so the signed bit pattern round-trips
   * exactly - an unsigned literal merely worked by luck when the hash
   * happened to stay under 2^63. */
  int32_t sqlLen;
  if (maxTs == INT64_MIN) {
    /* INT64_MIN is the "no watermark yet" sentinel (the first LAST_TS pull on an
     * empty table).  Rendering it as a timestamp would bound the history query
     * to ts > 1970-01-01 and hide every older row, which an Oracle TIMESTAMP
     * column can legally hold, so the lower bound is simply omitted and only
     * the trigger prefilter (if any) remains. */
    int32_t pfLen = (int32_t)strlen(prefilterBuf);
    if (pfLen >= 5 && strcmp(prefilterBuf + pfLen - 5, " AND ") == 0) {
      pfLen -= 5;
    }
    if (pfLen > 0) {
      sqlLen = snprintf(sqlBuf, sqlBufLen,
                        "SELECT MIN(%s),MAX(%s),%" PRId64 ",COUNT(*) FROM %s WHERE %.*s",
                        tsCol, tsCol, (int64_t)uid, tblRef, pfLen, prefilterBuf);
    } else {
      sqlLen = snprintf(sqlBuf, sqlBufLen,
                        "SELECT MIN(%s),MAX(%s),%" PRId64 ",COUNT(*) FROM %s",
                        tsCol, tsCol, (int64_t)uid, tblRef);
    }
  } else {
    int32_t boundCode =
        streamReaderExtRenderTimeBound(pInfo, maxTs, boundPrecision, dtBuf, sizeof(dtBuf));
    if (boundCode < 0) {
      return boundCode;
    }
    sqlLen = snprintf(sqlBuf, sqlBufLen,
                      "SELECT MIN(%s),MAX(%s),%" PRId64 ",COUNT(*) FROM %s "
                      "WHERE %s%s > %s",
                      tsCol, tsCol, (int64_t)uid,
                      tblRef,
                      prefilterBuf,
                      tsCol, dtBuf);
  }
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
 * handleMetaPullRelational  -  MySQL / PostgreSQL meta pull.
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
  char prefilterBuf[EXT_SQL_PREFILTER_BUF_LEN] = {0};
  char tblRef[EXT_TBL_REF_BUF_LEN] = {0};
  // Quote the timestamp column for the source dialect; the mapping keeps the raw name so the
  // connector can match result columns.
  char tsColQ[EXT_IDENT_QUOTE_BUF_LEN] = {0};
  if (extQuoteIdent(&pInfo->spec, tsCol, tsColQ, sizeof(tsColQ)) < 0) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  code = buildTriggerPrefilterClause(&pInfo->spec, prefilterBuf, sizeof(prefilterBuf));
  if (code < 0) {
    goto _exit;
  }

  code = buildExtTableRef(&pInfo->spec, tblRef, sizeof(tblRef));
  if (code < 0) {
    goto _exit;
  }

  extInitRelationalMetaMappings(tsCol, extSpecTsSourcePrecision(&pInfo->spec, tsCol), metaMappings);

  for (int32_t i = 0; i < totalUids; ++i) {
    uint64_t uid = *(uint64_t *)taosArrayGet(pAllUids, i);
    SUidIndexEntry *pEntry = tSimpleHashGet(pInfo->pUidIndex, &uid, sizeof(uid));
    char sqlBuf[EXT_SIMPLE_SQL_BUF_LEN] = {0};

    if (pEntry == NULL) {
      continue;
    }

    code = extBuildRelationalMetaSql(tblRef, prefilterBuf, tsColQ, uid,
                                     extLookupUidMaxTs(pUidMaxTs, uid),
                                     pInfo->spec.precision, pInfo,
                                     sqlBuf, sizeof(sqlBuf));
    if (code != TSDB_CODE_SUCCESS) {
      break;
    }

    ST_TASK_DLOG("ext: meta relational uid=%" PRIu64 " sql=\"%.120s\"", uid, sqlBuf);

    code = extQueryExecFetchAll(pInfo->pConn, pInfo->timezone, sqlBuf, metaMappings, 4, &pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("ext: meta relational query uid=%" PRIu64 " code:%d", uid, code);
      break;
    }
    code = extNormalizeTimestampBlockPrecision(pInfo->spec.precision, pBlock, metaMappings, 4,
                                               "handleMetaPullRelational");
    if (code != TSDB_CODE_SUCCESS) {
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
 * Influx batch helpers  -  internal to handleMetaPullInflux.
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

  // Quote the timestamp column and the table name for DataFusion so case-sensitive remote
  // identifiers survive identifier resolution; the tag columns are quoted per arm below.
  char tsColQ[EXT_IDENT_QUOTE_BUF_LEN] = {0};
  if (extQuoteIdent(&pInfo->spec, tsCol, tsColQ, sizeof(tsColQ)) < 0) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  char tblQ[EXT_IDENT_QUOTE_BUF_LEN] = {0};
  if (extQuoteIdent(&pInfo->spec, pInfo->spec.extTable, tblQ, sizeof(tblQ)) < 0) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

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
      int32_t twLen = buildInfluxTagWhereClause(&pInfo->spec, ent->tagsetKey,
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
    if (clauseOk) clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off, "SELECT %s", tsColQ);
    for (int32_t c = 0; clauseOk && c < nTags; c++) {
      const char *cn = (const char *)taosArrayGet(pInfo->pInfluxTagCols, c);
      char        cnQ[EXT_TAG_KEY_QUOTE_BUF_LEN] = {0};
      if (extQuoteIdent(&pInfo->spec, cn, cnQ, sizeof(cnQ)) < 0) {
        return TSDB_CODE_OUT_OF_RANGE;
      }
      clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off, ", %s", cnQ);
    }
    if (clauseOk) {
      clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off, " FROM %s WHERE ", tblQ);
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
                           "%s > to_timestamp_nanos(%" PRId64 ")", tsColQ,
                           extInfluxBoundToNanos(maxTs, pInfo->spec.precision, true));
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

  int32_t code = extQueryExecForEach(pInfo->pConn, pInfo->timezone, sqlBuf, pMappings, nCols,
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

  ST_TASK_DLOG("ext: influxAccumBlockCb rows=%" PRId64 " precision:%d",
               pBlk != NULL ? pBlk->info.rows : (int64_t)-1, pACtx->pInfo->spec.precision);

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

    int8_t sourcePrecision = extIsValidTimePrecision(pTsCol->info.precision)
                                 ? pTsCol->info.precision : TSDB_TIME_PRECISION_NANO;
    int64_t ts = convertTimePrecision(*(int64_t *)colDataGetData(pTsCol, r),
                                      sourcePrecision, pACtx->pInfo->spec.precision);
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
 * handleMetaPullInflux  -  InfluxDB meta pull (N=64 uid batch loop).
 *
 * One query per batch of N uids using a UNION ALL of per-uid SELECTs
 * (DS section 6.1.5); UNION ALL (rather than a top-level OR) is required because
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
  char prefilterBuf[EXT_SQL_PREFILTER_BUF_LEN] = {0};
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
    code = extQueryExecForEach(pInfo->pConn, pInfo->timezone, sqlBuf, pMappings, nCols,
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
 * handleLastTsPullInflux  -  InfluxDB LAST_TS pull (N=64 uid batch loop).
 *
 * Computes MAX(ts) PER UID (not one global value applied to every uid),
 * using each uid's own tagset as the WHERE filter  -  mirrors
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
  char prefilterBuf[EXT_SQL_PREFILTER_BUF_LEN] = {0};

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
 * Response: pRsp->pMetaBlock (DS section 6.2.4).
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

  pRsp->pMetaBlock = createMetaBlock();
  if (pRsp->pMetaBlock == NULL) {
    return terrno;
  }

  /* Pass the trigger watermark map directly to sub-handlers; they look up
   * uid->maxTs on demand without copying into pUidIndex. */
  SSHashObj *pUidMaxTs = (pReq != NULL) ? pReq->pUidMaxTs : NULL;
  int32_t    code = TSDB_CODE_SUCCESS;
  if (pInfo->influxV1) {
    /* 1.8 enumerates the remote series set itself and never looks at the local
     * uid map, so the per-pull uid array is neither built nor needed here. */
    code = handleMetaPullInflux18(pInfo, pUidMaxTs, pRsp);
  } else {
    SArray *pAllUids = collectAllUids(pInfo->pUidIndex);
    if (pAllUids == NULL) {
      ST_TASK_ELOG("%s", "ext: handleMetaPull collectAllUids OOM");
      return terrno;
    }
    int32_t totalUids = (int32_t)taosArrayGetSize(pAllUids);
    ST_TASK_DLOG("ext: handleMetaPull totalUids=%d isInflux=%d", totalUids, (int)isInflux);
    code = isInflux ? handleMetaPullInflux(pInfo, pAllUids, totalUids, tsCol, pUidMaxTs, pRsp)
                    : handleMetaPullRelational(pInfo, pAllUids, totalUids, tsCol, pUidMaxTs, pRsp);
    taosArrayDestroy(pAllUids);
  }
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
                                          int8_t boundPrecision,
                                          const SStreamExtReaderInfo *pInfo,
                                          char *sqlBuf, int32_t sqlBufLen) {
  char skeyDt[EXT_SQL_TS_LITERAL_LEN] = {0};
  char ekeyDt[EXT_SQL_TS_LITERAL_LEN] = {0};
  int32_t code = streamReaderExtRenderTimeBound(pInfo, skey, boundPrecision, skeyDt, sizeof(skeyDt));
  if (code < 0) {
    return code;
  }
  code = streamReaderExtRenderTimeBound(
      pInfo, extClampMysqlUpperBound(pInfo, ekey, boundPrecision), boundPrecision, ekeyDt,
      sizeof(ekeyDt));
  if (code < 0) {
    return code;
  }

  // The virtual-table merger and the external timestamp sorter both walk a slice as ascending
  // [skey, ekey] ranges and stop at the first row beyond the window, so an unordered result set
  // (plain heap scan) silently drops or mis-assigns rows. Order explicitly at the source.
  int32_t sqlLen = snprintf(sqlBuf, sqlBufLen,
                            "SELECT %s FROM %s WHERE %s%s >= %s AND %s <= %s ORDER BY %s ASC",
                            colList, tblRef, prefilterBuf, tsCol, skeyDt, tsCol, ekeyDt, tsCol);
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
    int32_t tagCode = buildInfluxTagWhereClause(&pInfo->spec, pEntry->tagsetKey, tagWhere, sizeof(tagWhere));
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
                            tsCol, extInfluxBoundToNanos(skey, pInfo->spec.precision, false),
                            tsCol, extInfluxBoundToNanos(ekey, pInfo->spec.precision, true));
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
 *   handleDataPull      ->  buildTriggerPrefilterClause (trigger data, PRE_FILTER)
 *   handleCalcDataPull  ->  buildTriggerPrefilterClause (trigger-side data for calc, PRE_FILTER) */
static int32_t fetchDataForUid(SStreamExtReaderInfo *pInfo,
                               const char *colList,
                               uint64_t uid, int64_t skey, int64_t ekey,
                               bool isInflux,
                               const char *prefilterBuf,
                               const SExtColTypeMapping *pMappings,
                               int32_t nMappings,
                               int8_t boundPrecision,
                               SSDataBlock **ppBlock) {
  const SStreamTask *pTask = pInfo->pTask;
  char sqlBuf[EXT_DATA_SQL_BUF_LEN] = {0};
  char tblRef[EXT_TBL_REF_BUF_LEN] = {0};
  const char *tsCol = (pInfo->spec.tsColumn[0] != '\0')
                        ? pInfo->spec.tsColumn : "ts";
  char tsColQ[EXT_IDENT_QUOTE_BUF_LEN] = {0};
  const SExtColTypeMapping *pEffMappings = NULL;
  int32_t nEffMappings = 0;
  int32_t code = extQuoteIdent(&pInfo->spec, tsCol, tsColQ, sizeof(tsColQ));
  if (code < 0) {
    return TSDB_CODE_OUT_OF_RANGE;
  }
  code = buildExtTableRef(&pInfo->spec, tblRef, sizeof(tblRef));
  if (code < 0) {
    return code;
  }

  code = isInflux
           ? extBuildInfluxFetchSql(pInfo, colList, tblRef, prefilterBuf, tsColQ,
                                    uid, skey, ekey, sqlBuf, sizeof(sqlBuf))
           : extBuildRelationalFetchSql(colList, tblRef, prefilterBuf, tsColQ,
                                        uid, skey, ekey, boundPrecision, pInfo,
                                        sqlBuf, sizeof(sqlBuf));
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  ST_TASK_DLOG("ext: fetchDataForUid uid=%" PRIu64 " sql=\"%.180s\"", uid, sqlBuf);
  extSelectFetchMappings(pMappings, nMappings, &pEffMappings, &nEffMappings);
  code = extQueryExecFetchAll(pInfo->pConn, pInfo->timezone, sqlBuf,
                              (SExtColTypeMapping *)pEffMappings,
                              nEffMappings,
                              ppBlock);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }
  return extNormalizeTimestampBlockPrecision(pInfo->spec.precision, *ppBlock, pEffMappings, nEffMappings,
                                             "fetchDataForUid");
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
    char        colQ[EXT_IDENT_QUOTE_BUF_LEN] = {0};
    if (extQuoteIdent(pSpec, colName, colQ, sizeof(colQ)) < 0 ||
        !extSqlCat(buf, bufLen, &off, "%s%s", colQ, (i < n - 1) ? "," : "")) {
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
                                char *buf, int32_t bufLen, bool aliasSelectItems) {
  if (pSpec->calcColumns == NULL ||
      taosArrayGetSize(pSpec->calcColumns) == 0) {
    /* No calc columns available yet  -  fetch all columns so the aggregate
     * operator can find both ts and measure columns in the result block. */
    snprintf(buf, bufLen, "*");
    return 0;
  }
  int32_t off = 0;
  int32_t n   = (int32_t)taosArrayGetSize(pSpec->calcColumns);
  for (int32_t i = 0; i < n; i++) {
    const char *colName = (const char *)taosArrayGet(pSpec->calcColumns, i);
    char        colQ[EXT_IDENT_QUOTE_BUF_LEN] = {0};
    if (extQuoteIdent(pSpec, colName, colQ, sizeof(colQ)) < 0) {
      stError("ext: calcColumns quote exceeds %d bytes at col %d/%d", bufLen, i, n);
      return TSDB_CODE_OUT_OF_RANGE;
    }
    bool ok = aliasSelectItems
                  ? extSqlCat(buf, bufLen, &off, "%s AS _td_ext_c%d%s", colQ, i, (i < n - 1) ? "," : "")
                  : extSqlCat(buf, bufLen, &off, "%s%s", colQ, (i < n - 1) ? "," : "");
    if (!ok) {
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
 * Batched InfluxDB data fetch (perf, DS section 6.1.5).
 *
 * Instead of one external query per uid, issue one UNION ALL query per batch
 * of up to EXT_INFLUX_UID_BATCH_SIZE uids and demultiplex the returned rows back
 * to per-uid blocks by tagset  -  mirroring the META batch loop.  Used only for
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
  int32_t               nData;       /* number of data (output) columns (== nMappings) */
  int32_t               nTags;       /* tag columns appended after the data columns */
  const SExtColTypeMapping *pMappings;
  int32_t               nMappings;
  int8_t                fallbackPrecision;
  SSHashObj            *pUidBlk;     /* hash<uint64_t uid, SSDataBlock*>; owns the blocks */
  int32_t               code;        /* sticky error captured inside the callback */
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
  /* Per-UID blocks persist across every batch query. Start from the row that is
   * actually present so a sparse UID does not reserve a full 64-row page. */
  int32_t code = extEnsureBlockCapacityGeometric(pDst, (uint32_t)row + 1, 1U);
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

  pCtx->code = extNormalizeTimestampBlockPrecision(pCtx->fallbackPrecision, pBlk, pCtx->pMappings,
                                                   pCtx->nMappings, "fetchDataBatchInflux");
  if (pCtx->code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: batched influx demux timestamp normalization failed code:%d", pCtx->code);
    blockDataDestroy(pBlk);
    return pCtx->code;
  }
  ST_TASK_DLOG("ext: batched influx demux normalized block rows:%" PRId64 " mappings:%d",
               (int64_t)pBlk->info.rows, pCtx->nMappings);

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

  char tsColQ[EXT_IDENT_QUOTE_BUF_LEN] = {0};
  if (extQuoteIdent(&pInfo->spec, tsCol, tsColQ, sizeof(tsColQ)) < 0) {
    return TSDB_CODE_OUT_OF_RANGE;
  }

  for (; k < batchSize; ++k) {
    uint64_t        uid = *(uint64_t *)taosArrayGet(pUids, batchStart + k);
    SExtUidWindow  *pWin = (SExtUidWindow *)taosArrayGet(pWins, batchStart + k);
    SUidIndexEntry *pEntry = tSimpleHashGet(pInfo->pUidIndex, &uid, sizeof(uid));
    char            tagWhere[EXT_TAG_WHERE_BUF_LEN] = {0};

    if (pEntry != NULL && pEntry->tagsetKey[0] != '\0') {
      int32_t twLen = buildInfluxTagWhereClause(&pInfo->spec, pEntry->tagsetKey, tagWhere, sizeof(tagWhere));
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
      char tagQ[EXT_TAG_KEY_QUOTE_BUF_LEN] = {0};
      if (extQuoteIdent(&pInfo->spec, (const char *)taosArrayGet(pInfo->pInfluxTagCols, c), tagQ,
                        sizeof(tagQ)) < 0) {
        return TSDB_CODE_OUT_OF_RANGE;
      }
      clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off, ", %s", tagQ);
    }
    if (clauseOk) clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off, " FROM %s WHERE ", tblRef);
    if (clauseOk && prefilterBuf[0] != '\0') clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off, "%s", prefilterBuf);
    if (clauseOk && tagWhere[0] != '\0') clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off, "%s AND ", tagWhere);
    if (clauseOk) {
      clauseOk = extSqlCat(sqlBuf, sqlBufLen, &off,
                           "%s >= to_timestamp_nanos(%" PRId64 ")"
                           " AND %s <= to_timestamp_nanos(%" PRId64 ")",
                           tsColQ, extInfluxBoundToNanos(pWin->skey, pInfo->spec.precision, false),
                           tsColQ, extInfluxBoundToNanos(pWin->ekey, pInfo->spec.precision, true));
    }
    if (!clauseOk) {
      sqlBuf[saveOff] = '\0';
      off = saveOff;
      if (k == 0) {
        ST_TASK_ELOG("ext: influx data batch SQL exceeds %d bytes uid=%" PRIu64
                     " batchStart=%d batchSize=%d",
                     sqlBufLen, uid, batchStart, batchSize);
        return TSDB_CODE_OUT_OF_RANGE;
      }
      *pConsumed = k;
      *pAnyUid = anyUid;
      ST_TASK_DLOG("ext: split influx data batch at SQL capacity consumed=%d requested=%d",
                   k, batchSize);
      return TSDB_CODE_SUCCESS;
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
  char tblRef[EXT_TBL_REF_BUF_LEN] = {0};
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
  ctx.pMappings = pMapAll;
  ctx.nMappings = nColsAll;
  ctx.fallbackPrecision = pInfo->spec.precision;
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
      code = extQueryExecForEach(pInfo->pConn, pInfo->timezone, sqlBuf, pMapAll, nColsAll,
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

/* The per-uid blocks produced by extFetchUidBlocks are consumed as *ascending* [skey, ekey]
 * ranges: stNewTimestampSorterSetData() and the virtual-table merger advance a single row cursor
 * and stop at the first row beyond the window, so a per-uid block whose rows are not in ascending
 * ts order silently drops rows or assigns them to the wrong window.
 *
 * Relational sources are ordered on the remote side (extBuildRelationalFetchSql emits ORDER BY),
 * but InfluxDB 3 (DataFusion) leaves row order unspecified without ORDER BY, and the batched
 * UNION ALL path cannot be ordered per uid remotely at all. The contract is therefore enforced
 * here, the single point every ext data fetch (trigger / calc / meta+data) passes through.
 * Already-ascending blocks (the common case) are detected with a linear scan and left untouched,
 * so only genuinely unordered results pay for the sort.
 *
 * The timestamp column is located through the type mappings: block column i mirrors mapping i for
 * an explicit column list. Reader block slot 0 is the documented ts slot (the trigger side matches
 * the virtual ts column with otbSlotId = 0), so slot 0 is the fallback for the SELECT * path where
 * no mappings are available. */
static int32_t extResolveBlockTsSlot(const SExtColTypeMapping *pMappings, int32_t nMappings,
                                     const SSDataBlock *pBlock) {
  int32_t nCols = (int32_t)taosArrayGetSize(pBlock->pDataBlock);
  for (int32_t i = 0; i < nMappings && i < nCols; ++i) {
    if (pMappings[i].tdType.type == TSDB_DATA_TYPE_TIMESTAMP) {
      return i;
    }
  }
  return 0;
}

static bool extUidBlockTsIsAscending(const SSDataBlock *pBlock, int32_t tsSlotId) {
  SColumnInfoData *pTsCol = (SColumnInfoData *)taosArrayGet(pBlock->pDataBlock, tsSlotId);
  if (pTsCol == NULL || pTsCol->pData == NULL) {
    return true;
  }
  int64_t *pTsData = (int64_t *)pTsCol->pData;
  for (int32_t r = 1; r < pBlock->info.rows; ++r) {
    if (pTsData[r] < pTsData[r - 1]) {
      return false;
    }
  }
  return true;
}

static int32_t extSortUidBlocksByTs(const SStreamTask *pTask, const char *phase,
                                    const SExtColTypeMapping *pMappings, int32_t nMappings,
                                    SArray *pUidBlocks) {
  int32_t code = TSDB_CODE_SUCCESS;
  int32_t nBlocks = (int32_t)taosArrayGetSize(pUidBlocks);

  for (int32_t i = 0; i < nBlocks; ++i) {
    SUidBlockPair *pPair = (SUidBlockPair *)taosArrayGet(pUidBlocks, i);
    if (pPair == NULL || pPair->pBlock == NULL || pPair->pBlock->info.rows <= 1) {
      continue;
    }

    int32_t          tsSlotId = extResolveBlockTsSlot(pMappings, nMappings, pPair->pBlock);
    SColumnInfoData *pTsCol = (SColumnInfoData *)taosArrayGet(pPair->pBlock->pDataBlock, tsSlotId);
    if (pTsCol == NULL || pTsCol->info.type != TSDB_DATA_TYPE_TIMESTAMP) {
      ST_TASK_WLOG("ext: %s cannot locate ts column for uid:%" PRId64 ", rows left unsorted",
                   phase, pPair->uid);
      continue;
    }
    if (extUidBlockTsIsAscending(pPair->pBlock, tsSlotId)) {
      continue;
    }

    SArray *pOrderInfo = taosArrayInit(1, sizeof(SBlockOrderInfo));
    if (pOrderInfo == NULL) {
      return terrno;
    }
    SBlockOrderInfo order = {.nullFirst = false,
                             .order = TSDB_ORDER_ASC,
                             .slotId = tsSlotId,
                             .compFn = NULL,
                             .pColData = NULL};
    if (taosArrayPush(pOrderInfo, &order) == NULL) {
      taosArrayDestroy(pOrderInfo);
      return terrno;
    }
    code = blockDataSort(pPair->pBlock, pOrderInfo);
    taosArrayDestroy(pOrderInfo);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("ext: %s sort uid:%" PRId64 " by ts failed code:%d", phase, pPair->uid, code);
      return code;
    }
    ST_TASK_DLOG("ext: %s sorted uid:%" PRId64 " by ts, rows:%" PRId64
                 " (remote result was not ascending)",
                 phase, pPair->uid, (int64_t)pPair->pBlock->info.rows);
  }

  return code;
}

/* InfluxDB 1.8 data fetch: one prepared raw scan per uid.  The uid's complete
 * tag binding (from its tagset key) reproduces the series it was enumerated
 * from, and the provider orders every scan ascending by time, so the local
 * sort below is only a guard. */
static int32_t extFetchUidBlocksInflux18(SStreamExtReaderInfo *pInfo, const char *phase,
                                         const SExtColTypeMapping *pMappings, int32_t nMappings,
                                         SSHashObj *pUidWindow, SArray *pUidBlocks) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t            code = extInflux18EnsureSchema(pInfo, pInfo->pConn);
  if (code != TSDB_CODE_SUCCESS) {
    return code;
  }

  SExtColTypeMapping       *pFullMappings = NULL;
  int32_t                   nFullMappings = 0;
  const SExtColTypeMapping *pEffMappings = pMappings;
  int32_t                   nEffMappings = nMappings;
  if (nEffMappings <= 0) {
    code = extInflux18FullMappings(pInfo, &pFullMappings, &nFullMappings);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    pEffMappings = pFullMappings;
    nEffMappings = nFullMappings;
  }

  int32_t iter = 0;
  void   *pVal = NULL;
  while (code == TSDB_CODE_SUCCESS &&
         (pVal = tSimpleHashIterate(pUidWindow, pVal, &iter)) != NULL) {
    size_t          keyLen = 0;
    uint64_t        uid = *(uint64_t *)tSimpleHashGetKey(pVal, &keyLen);
    SExtUidWindow  *pWin = (SExtUidWindow *)pVal;
    SUidIndexEntry *pEntry = tSimpleHashGet(pInfo->pUidIndex, &uid, sizeof(uid));
    if (pEntry == NULL) {
      ST_TASK_WLOG("ext: %s influx18 uid:%" PRIu64 " not in the series index, skipped",
                   phase, uid);
      continue;
    }

    SExtInfluxTagValue tags[TSDB_MAX_TAGS];
    char               scratch[EXT_TAGSET_KEY_MAX] = {0};
    int32_t            nTags = 0;
    code = extInflux18ParseTagBindings(pEntry->tagsetKey, scratch, sizeof(scratch), tags,
                                       TSDB_MAX_TAGS, &nTags);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("ext: %s influx18 parse tagset uid:%" PRIu64 " code:%d", phase, uid, code);
      break;
    }
    code = extInflux18FillEmptyTagBindings(pInfo, tags, TSDB_MAX_TAGS, &nTags);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("ext: %s influx18 fill empty tagset uid:%" PRIu64 " code:%d", phase, uid, code);
      break;
    }

    SSDataBlock *pBlock = NULL;
    code = extInflux18ExecScan(pInfo, pInfo->pConn, pEffMappings, nEffMappings,
                               nTags > 0 ? tags : NULL, nTags, pInfo->pTriggerPrefilter,
                               pWin->skey, pWin->ekey, true, &pBlock);
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("ext: %s influx18 scan uid:%" PRIu64 " code:%d", phase, uid, code);
      break;
    }

    SUidBlockPair pair = {.uid = uid, .pBlock = pBlock};
    if (taosArrayPush(pUidBlocks, &pair) == NULL) {
      code = terrno;
      blockDataDestroy(pBlock);
      break;
    }
  }

  if (code == TSDB_CODE_SUCCESS) {
    code = extSortUidBlocksByTs(pTask, phase, pEffMappings, nEffMappings, pUidBlocks);
  }
  taosMemoryFree(pFullMappings);
  return code;
}

static int32_t extFetchUidBlocks(SStreamExtReaderInfo *pInfo,
                                 const char *phase,
                                 const char *colList,
                                 int32_t nCols,
                                 const char *prefilterBuf,
                                 const SExtColTypeMapping *pMappings,
                                 int32_t nMappings,
                                 SSHashObj *pUidWindow,
                                 int8_t boundPrecision,
                                 SArray *pUidBlocks) {
  const SStreamTask *pTask = pInfo->pTask;
  int32_t code = TSDB_CODE_SUCCESS;
  bool    isInflux = ((EExtSourceType)pInfo->spec.sourceType == EXT_SOURCE_INFLUXDB);
  int32_t nTags = pInfo->pInfluxTagCols ? (int32_t)taosArrayGetSize(pInfo->pInfluxTagCols) : 0;

  /* 1.8 has no SQL endpoint and no server-side predicate pushdown: every uid is
   * fetched with its own prepared raw scan. */
  if (pInfo->influxV1) {
    return extFetchUidBlocksInflux18(pInfo, phase, pMappings, nMappings, pUidWindow, pUidBlocks);
  }

  /* InfluxDB can batch uid windows only when the select list and mappings
   * align positionally; otherwise it must fall back to one query per uid. */
  if (isInflux && nTags > 0 && nCols > 0 && nMappings == nCols) {
    ST_TASK_DLOG("ext: %s using batched influx fetch uidCount=%d nCols=%d nTags=%d",
                 phase, tSimpleHashGetSize(pUidWindow), nCols, nTags);
    code = fetchDataBatchInflux(pInfo, colList, pMappings, nMappings, nTags, prefilterBuf,
                                pUidWindow, pUidBlocks);
    if (code != TSDB_CODE_SUCCESS) {
      return code;
    }
    /* A UNION ALL result set has no per-uid remote order guarantee, so the demuxed per-uid
     * blocks are ordered locally before they reach the timestamp sorter / virtual merger. */
    return extSortUidBlocksByTs(pTask, phase, pMappings, nMappings, pUidBlocks);
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
                           pMappings, nMappings, boundPrecision, &pBlock);
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

  return extSortUidBlocksByTs(pTask, phase, pMappings, nMappings, pUidBlocks);
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
  char prefilterBuf[EXT_SQL_PREFILTER_BUF_LEN] = {0};
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

  /* PERIOD can have no trigger projection even when %%trows needs typed
   * columns from the cached calculation scan. Reuse that scan's projection
   * and mappings together: the InfluxDB JSON provider cannot infer types or
   * column order from an unmapped SELECT * response. */
  if (nCols == 0 && pInfo->spec.calcColumns != NULL && pInfo->spec.pCalcMappings != NULL &&
      pInfo->spec.numCalcMappings > 0 &&
      taosArrayGetSize(pInfo->spec.calcColumns) == (size_t)pInfo->spec.numCalcMappings) {
    nCols = buildCalcColList(&pInfo->spec, colList, sizeof(colList), false);
    if (nCols < 0) {
      code = nCols;
      goto _exit;
    }
    pColMaps = pInfo->spec.pCalcMappings;
    nColMaps = pInfo->spec.numCalcMappings;
    ST_TASK_DLOG("ext: handleDataPull using calc projection columns=%d", nCols);
  } else if (nCols == 0) {
    ST_TASK_DLOG("%s", "ext: handleDataPull no typed projection, using SELECT * without mappings");
  }

  pUidBlocks = taosArrayInit(tSimpleHashGetSize(pReq->pUidWindow), sizeof(SUidBlockPair));
  if (pUidBlocks == NULL) {
    code = terrno;
    ST_TASK_ELOG("ext: handleDataPull alloc uid blocks failed code:%d", code);
    goto _exit;
  }

  code = extFetchUidBlocks(pInfo, "handleDataPull", colList, nCols, prefilterBuf, pColMaps,
                           nColMaps, pReq->pUidWindow, pInfo->spec.precision, pUidBlocks);
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
  bool aliasSelectItems = ((EExtSourceType)pInfo->spec.sourceType == EXT_SOURCE_INFLUXDB);
  *pCols = buildCalcColList(&pInfo->spec, colList, colListLen, aliasSelectItems);
  if (*pCols < 0) {
    return *pCols;
  }

  *ppMappings = (*pCols > 0) ? pInfo->spec.pCalcMappings : NULL;
  *pMappings = (*pCols > 0) ? pInfo->spec.numCalcMappings : 0;
  ST_TASK_DLOG("ext: handleCalcDataPull colList='%s' aliasSelectItems:%d", colList, aliasSelectItems);

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
  char calcPrefilterBuf[EXT_SQL_PREFILTER_BUF_LEN] = {0};
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
                           pReq->pUidWindow, pInfo->spec.precision, pUidBlocks);
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
  /* Scratch for the "<extTable>_<tag>_<value>..." (1.8) or
   * "<extTable>_<value>..." (2.x) subtable identity. */
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

/* Append one 1.8 sub-table name component ("_<tag>_<value>") with '~' as the escape
 * character: '~' becomes "~~" and '_' becomes "~_".  Doubling '_' alone was not
 * enough, because the escaped form still ends with the separator character
 * ({a_="b"} -> "_a__" + "_b" and {a="_b"} -> "_a" + "___b" both ended as
 * "..._a___b").  With '_' always preceded by '~' inside a component and '~'
 * always starting a two-character escape, the composition is injective: two
 * different tag tuples can never compose the same name.  Only the tag tuple is
 * escaped -- the measurement prefix is fixed for the whole stream.  Sets
 * nameOverflow (and NUL-terminates at the last complete component) when the
 * result no longer fits, so the caller falls back to "influxdb_<uid>". */
static void extInflux18AppendTbnameComponent(SExtGroupColValueCtx *pCtx, const char *tagName,
                                             const char *tagValue) {
  const char *parts[2] = {tagName, tagValue};
  const int32_t capacity = (int32_t)sizeof(pCtx->nameBuf);

  for (int32_t p = 0; p < 2; ++p) {
    if (pCtx->nameLen + 1 >= capacity) {
      pCtx->nameOverflow = true;
      pCtx->nameBuf[pCtx->nameLen] = '\0';
      return;
    }
    pCtx->nameBuf[pCtx->nameLen++] = '_';
    for (const char *s = parts[p]; s != NULL && *s != '\0'; ++s) {
      int32_t need = (*s == '_' || *s == '~') ? 2 : 1;
      if (pCtx->nameLen + need >= capacity) {
        pCtx->nameOverflow = true;
        pCtx->nameBuf[pCtx->nameLen] = '\0';
        return;
      }
      if (need == 2) pCtx->nameBuf[pCtx->nameLen++] = '~';
      pCtx->nameBuf[pCtx->nameLen++] = *s;
    }
  }
  pCtx->nameBuf[pCtx->nameLen] = '\0';
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
  char *pair = tagsetKey;
  while (pair != NULL && *pair != '\0') {
    char *next = findUnescapedTagsetChar(pair, '|');
    if (next != NULL) *next = '\0';
    char *eq = findUnescapedTagsetChar(pair, '=');
    if (eq == NULL) {
      pair = next != NULL ? next + 1 : NULL;
      continue;
    }
    *eq = '\0';
    unescapeTagsetComponent(pair);
    unescapeTagsetComponent(eq + 1);
    const char *colName = pair;
    const char *colVal  = eq + 1;
    if (nCols >= TSDB_MAX_TAGS) {
      ST_TASK_ELOG("ext: tagset contains more than %d columns", TSDB_MAX_TAGS);
      return TSDB_CODE_OUT_OF_RANGE;
    }
    colNames[nCols] = colName;
    colVals[nCols] = colVal;
    ++nCols;

    pair = next != NULL ? next + 1 : NULL;
  }

  if (pCtx->wantTbname && !pCtx->nameOverflow) {
    /* InfluxDB 1.8 leaves a tag the series does not carry out of its identity, so
     * two series can hold the same value under different tag keys ("host=a" and
     * "region=a").  Qualifying only the sparse tagsets is not enough: with the
     * schema {site, zone}, series "site=a" composes "<m>_site_a" in the
     * name-qualified form while the complete series "site=site,zone=a" composes
     * the same "<m>_site_a" in the value-only form, so both groups would write
     * the same window into one table.  Switching form by sparsity also renames
     * existing series as soon as a new tag key appears.  Every 1.8 component is
     * therefore qualified with its tag name, and the separator/escape characters
     * inside a name or value are escaped ('_' -> "~_", '~' -> "~~") so the
     * component boundaries stay decidable and two different tag tuples can never
     * compose the same name.  Sources without sparse tagsets (2.x) keep the
     * compact value-only form. */
    bool qualify = pCtx->pInfo->influxV1;
    for (int32_t i = 0; i < nCols; ++i) {
      if (qualify) {
        extInflux18AppendTbnameComponent(pCtx, colNames[i], colVals[i]);
        if (pCtx->nameOverflow) break;
        continue;
      }
      int32_t n = snprintf(pCtx->nameBuf + pCtx->nameLen, sizeof(pCtx->nameBuf) - pCtx->nameLen,
                           "_%s", colVals[i]);
      if (n < 0 || n >= (int32_t)(sizeof(pCtx->nameBuf) - pCtx->nameLen)) {
        pCtx->nameOverflow = true;
        break;
      }
      pCtx->nameLen += n;
    }
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
 * "<measurement>_<tag>_<value>..." (1.8) or "<measurement>_<value>..." (2.x)
 * identity when pCtx->nameBuf was built without overflowing (and this uid
 * actually had a tagsetKey to compose from), else the "influxdb_<uid>"
 * fallback. Feeds extGroupColValueFillTbnameSlot's single call site in
 * handleGroupColValuePull. */
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

static int32_t extAppendFetchDataBound(char *sql, int32_t sqlLen, int32_t *pOff,
                                       const char *tsCol, const char *op,
                                       int64_t ts, int8_t boundPrecision,
                                       const SStreamExtReaderInfo *pInfo, bool isInflux) {
  if (isInflux) {
    return extSqlCat(sql, sqlLen, pOff, " AND %s %s to_timestamp_nanos(%" PRId64 ")",
                     tsCol, op, extInfluxBoundToNanos(ts, boundPrecision, strcmp(op, "<=") == 0))
               ? TSDB_CODE_SUCCESS
               : TSDB_CODE_OUT_OF_RANGE;
  }

  char dt[EXT_SQL_TS_LITERAL_LEN] = {0};
  /* Only the inclusive upper bound is clamped into the remote domain; see
   * extClampMysqlUpperBound(). */
  int64_t bound = (strcmp(op, "<=") == 0) ? extClampMysqlUpperBound(pInfo, ts, boundPrecision) : ts;
  int32_t code = streamReaderExtRenderTimeBound(pInfo, bound, boundPrecision, dt, sizeof(dt));
  if (code < 0) {
    return code;
  }
  return extSqlCat(sql, sqlLen, pOff, " AND %s %s %s", tsCol, op, dt)
             ? TSDB_CODE_SUCCESS
             : TSDB_CODE_OUT_OF_RANGE;
}

/* ============================================================
 * EXT fetch time-range evaluation  -  mirrors vnodeStream.c
 * processCalaTimeRange for the external-source reader.
 * ============================================================ */

/* Canonical empty fetch range: renders as "ts >= 1 AND ts <= 0" (no rows)
 * without pushing extreme timestamps into the remote SQL. */
#define EXT_FETCH_EMPTY_SKEY 1
#define EXT_FETCH_EMPTY_EKEY 0

static void extSetEmptyFetchRange(STimeWindow *pWin) {
  pWin->skey = EXT_FETCH_EMPTY_SKEY;
  pWin->ekey = EXT_FETCH_EMPTY_EKEY;
}

/* Evaluate the fetch range spanned by the window parameters in pParams[first..last]
 * (start bound from pParams[first], end bound from pParams[last]).  pRt supplies the
 * trigger type / precision; pParams, groupId and pPartColVals replace the request's
 * own values on a local shallow copy so the scalar placeholders read this parameter
 * set.  On success *pWin holds an inclusive [skey, ekey]; an empty range is
 * reported as the canonical empty window and *pEmpty = true. */
static int32_t extCalcParamsRange(STimeRangeNode *node, const SStreamRuntimeFuncInfo *pRt,
                                  SArray *pParams, int64_t groupId, SArray *pPartColVals,
                                  int32_t first, int32_t last, STimeWindow *pWin, bool *pEmpty) {
  SSTriggerCalcParam *pFirst = taosArrayGet(pParams, first);
  SSTriggerCalcParam *pLast  = taosArrayGet(pParams, last);
  if (pFirst == NULL || pLast == NULL) {
    return TSDB_CODE_INVALID_PARA;
  }

  STimeWindow win = {.skey = INT64_MIN, .ekey = INT64_MAX};
  if (!node->needCalc) {
    win.skey = pFirst->wstart;
    win.ekey = pLast->wend;
    /* Sliding windows are half-open; the others already carry an inclusive end. */
    if (pRt->triggerType == STREAM_TRIGGER_SLIDING && win.ekey != INT64_MIN) win.ekey--;
  } else {
    /* A single window needs the complete predicate, including SQL NULL bounds.
     * Only a batch uses separate bounds to form its fetch envelope. */
    SStreamRuntimeFuncInfo tmp = *pRt;  // shallow view; never destroy tmp
    tmp.isMultiGroupCalc      = false;
    tmp.pStreamPesudoFuncVals = pParams;
    tmp.pStreamPartColVals    = pPartColVals;
    tmp.groupId               = groupId;
    tmp.curIdx                = first;

    STimeWindow cur   = {0};
    bool        valid = false;
    int32_t     code  = streamCalcCurrWinTimeRange(node, &tmp, &cur, &valid, first == last ? 3 : 1);
    if (code != TSDB_CODE_SUCCESS) return code;
    win.skey   = cur.skey;
    if (first != last) {
      tmp.curIdx = last;
      code = streamCalcCurrWinTimeRange(node, &tmp, &cur, &valid, 2);
      if (code != TSDB_CODE_SUCCESS) return code;
    }
    /* streamCalcCurrWinTimeRange returns a half-open end; INT64_MAX = unbounded. */
    win.ekey = cur.ekey;
    if (win.ekey != INT64_MAX) {
      if (win.ekey == INT64_MIN) {
        /* The evaluated end bound collapsed to the sentinel: the window is
         * invalid.  Fail loudly instead of silently fetching an empty range,
         * mirroring vnodeCalculateNodeGroupReadRange's !currentValid ||
         * current.ekey == INT64_MIN check. */
        stError("ext: extCalcParamsRange evaluated end bound is INT64_MIN, invalid window");
        return TSDB_CODE_STREAM_INTERNAL_ERROR;
      }
      win.ekey--;
    }
  }

  *pEmpty = win.skey > win.ekey;
  if (*pEmpty) {
    extSetEmptyFetchRange(&win);
  }
  *pWin = win;
  return TSDB_CODE_SUCCESS;
}

int32_t streamReaderExtGetCalcInfo(const SStreamReaderTask *pTask, int32_t execId,
                                  SStreamExtReaderInfo **ppReaderInfo) {
  if (ppReaderInfo == NULL) {
    stError("ext: calc reader lookup has NULL output, execId:%d", execId);
    return TSDB_CODE_INVALID_PARA;
  }
  *ppReaderInfo = NULL;
  if (pTask == NULL || pTask->triggerReader != 0 ||
      !STREAM_IS_REF_EXT_SOURCE(pTask->task.flags)) {
    stError("ext: calc reader lookup has invalid task:%p execId:%d",
             pTask, execId);
    return TSDB_CODE_INVALID_MSG;
  }
  if (pTask->info == NULL) {
    stError("ext: calc reader has no local info, task:%" PRIx64,
             pTask->task.taskId);
    return TSDB_CODE_STREAM_EXT_READER_NO_LOCAL_INFO;
  }
  SArray *pReaders = pTask->info;
  if (execId < 0 || (size_t)execId >= taosArrayGetSize(pReaders)) {
    stError("ext: invalid calc execId:%d replicas:%zu task:%" PRIx64,
             execId, taosArrayGetSize(pReaders), pTask->task.taskId);
    return TSDB_CODE_INVALID_PARA;
  }
  *ppReaderInfo = taosArrayGetP(pReaders, execId);
  if (*ppReaderInfo == NULL) {
    stError("ext: missing calc replica, execId:%d task:%" PRIx64,
             execId, pTask->task.taskId);
    return TSDB_CODE_STREAM_EXT_READER_NO_LOCAL_INFO;
  }
  stDebug("ext: selected calc reader execId:%d task:%" PRIx64 " info:%p",
           execId, pTask->task.taskId, *ppReaderInfo);
  return TSDB_CODE_SUCCESS;
}

int32_t streamReaderExtCalcFetchRange(SStreamExtReaderInfo *pReaderInfo,
                                      const SStreamRuntimeFuncInfo *pRt,
                                      int64_t *pSkey, int64_t *pEkey) {
  if (pReaderInfo == NULL || pSkey == NULL || pEkey == NULL) {
    stError("ext: streamReaderExtCalcFetchRange invalid args pInfo=%p pRt=%p",
            pReaderInfo, pRt);
    return TSDB_CODE_INVALID_PARA;
  }

  /* Unbounded full scan: no time range, no window parameters, or an
   * out-of-range window index. */
  *pSkey = INT64_MIN;
  *pEkey = INT64_MAX;

  if (pRt == NULL || pReaderInfo->pCalcTimeRange == NULL) {
    return TSDB_CODE_SUCCESS;
  }

  STimeRangeNode *node  = (STimeRangeNode *)pReaderInfo->pCalcTimeRange;
  STimeWindow     win   = {.skey = INT64_MIN, .ekey = INT64_MAX};
  bool            empty = false;
  int32_t         code  = TSDB_CODE_SUCCESS;
  int32_t         nWin  = 0;

  if (pRt->isMultiGroupCalc) {
    /* Multi-group calc: the ext fetch carries every group's parameters in
     * pGroupCalcInfos (pStreamPesudoFuncVals is not populated).  Bound the fetch
     * with the union of the non-empty per-group [first, last] ranges - the ETR
     * reads the whole external table, so this is the snode-side counterpart of
     * vnodeCalculateNodeGroupReadRange without a per-vnode read scope. */
    STimeWindow             combined = {.skey = INT64_MAX, .ekey = INT64_MIN};
    bool                    any      = false;
    int32_t                 iter     = 0;
    SSTriggerGroupCalcInfo *pGrp     = tSimpleHashIterate(pRt->pGroupCalcInfos, NULL, &iter);
    while (pGrp != NULL) {
      const int32_t n = (int32_t)taosArrayGetSize(pGrp->pParams);
      if (n > 0) {
        const int64_t *pGid     = tSimpleHashGetKey(pGrp, NULL);
        STimeWindow    current  = {0};
        bool           curEmpty = false;
        code = extCalcParamsRange(node, pRt, pGrp->pParams, *pGid, pGrp->pGroupColVals, 0, n - 1,
                                  &current, &curEmpty);
        if (code != TSDB_CODE_SUCCESS) break;
        if (!curEmpty) {
          combined.skey = TMIN(combined.skey, current.skey);
          combined.ekey = TMAX(combined.ekey, current.ekey);
          any           = true;
        }
        nWin += n;
      }
      pGrp = tSimpleHashIterate(pRt->pGroupCalcInfos, pGrp, &iter);
    }
    if (any) {
      win = combined;
    } else {
      empty = true;
      extSetEmptyFetchRange(&win);
    }
  } else {
    SArray *vals = pRt->pStreamPesudoFuncVals;
    nWin = (vals == NULL) ? 0 : (int32_t)taosArrayGetSize(vals);
    if (nWin > 0) {
      /* One calc request may carry multiple external windows: bound the fetch
       * with the first window's start and the last window's end, exactly like
       * processCalaTimeRange's withExternalWindow single-group branch.
       * Otherwise only the current window is evaluated. */
      const int32_t first = pRt->withExternalWindow ? 0 : pRt->curIdx;
      const int32_t last  = pRt->withExternalWindow ? nWin - 1 : pRt->curIdx;
      if (first < 0 || last >= nWin) {
        stError("ext: streamReaderExtCalcFetchRange curIdx:%d out of range nWin:%d",
                pRt->curIdx, nWin);
        return TSDB_CODE_STREAM_INTERNAL_ERROR;
      }
      code = extCalcParamsRange(node, pRt, vals, pRt->groupId, pRt->pStreamPartColVals, first, last,
                                &win, &empty);
    }
  }

  if (code != TSDB_CODE_SUCCESS) {
    stError("ext: streamReaderExtCalcFetchRange failed code:%d since %s",
            code, tstrerror(code));
    return code;
  }
  if (nWin <= 0) {
    stDebug("ext: streamReaderExtCalcFetchRange no window params, full scan");
    return TSDB_CODE_SUCCESS;
  }

  *pSkey = win.skey;
  *pEkey = win.ekey;
  stDebug("ext: streamReaderExtCalcFetchRange needCalc:%d withExtWin:%d multiGroup:%d empty:%d skey:%" PRId64
          " ekey:%" PRId64 " curIdx:%d nWin:%d",
          node->needCalc, pRt->withExternalWindow, pRt->isMultiGroupCalc, empty, *pSkey, *pEkey, pRt->curIdx,
          nWin);
  return TSDB_CODE_SUCCESS;
}

/* ============================================================
 * P4-E1: EXT fetch  -  called by the snode runner-queue worker
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
  char tsColQ[EXT_IDENT_QUOTE_BUF_LEN] = {0};
  code = extQuoteIdent(&pReaderInfo->spec, tsCol, tsColQ, sizeof(tsColQ));
  if (code < 0) {
    goto _exit;
  }

  char tblRef[EXT_TBL_REF_BUF_LEN] = {0};
  code = buildExtTableRef(&pReaderInfo->spec, tblRef, sizeof(tblRef));
  if (code < 0) {
    goto _exit;
  }

  /* Build column list from calcColumns (aggregate-input columns).
   * Returns the explicit column count, or 0 when it falls back to "*". */
  char colList[EXT_SQL_CLAUSE_BUF_LEN] = {0};
  bool aliasSelectItems =
      ((EExtSourceType)pReaderInfo->spec.sourceType == EXT_SOURCE_INFLUXDB);
  int32_t nCols = buildCalcColList(&pReaderInfo->spec, colList, sizeof(colList),
                                   aliasSelectItems);
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

  /* InfluxDB 1.8: no SQL endpoint, so the calc window is served by the
   * provider's prepared raw scan (all series, inclusive time range, ascending).
   * The plan's calc WHERE is applied by the stream executor.  The trigger
   * PRE_FILTER belongs only to the trigger reader and must not affect calc
   * input, including when calc reads a different external table. */
  if (pReaderInfo->influxV1) {
    SExtColTypeMapping       *pFullMappings = NULL;
    int32_t                   nFullMappings = 0;
    const SExtColTypeMapping *pEffMappings = pMappings;
    int32_t                   nEffMappings = nMappings;
    bool                      ownConn = pReaderInfo->borrowConn;
    SExtConnectorHandle      *pConn = pReaderInfo->pConn;
    if (ownConn) {
      pConn = NULL;
      code = extOpenReaderConnector(pTask, &pReaderInfo->spec, &pConn);
      if (code != TSDB_CODE_SUCCESS) {
        return code;
      }
    }
    /* The 1.8 table descriptor and remote schema below are lazy reader state
     * (pInflux18Table/pInflux18Meta): a concurrent FETCH_EXT on this reader could build them twice,
     * or reset them while this thread is still using them, so the schema refresh and the scan
     * share the same lock in both branches. */
    (void)taosThreadMutexLock(&pReaderInfo->connLock);
    if (ownConn) {
      /* A calculation reader is created independently of the trigger reader and never runs the
       * trigger-side LAST_TS/META field probe, so a field created after the schema was cached
       * stays outside its projection and a point carrying only that field yields no row at all
       * (COUNT(*) then undercounts).  Drop the cached schema for every calculation fetch: one
       * metadata round trip per window buys the exact row set, and the output column mapping is
       * unaffected because the plan's mappings stay authoritative when they exist. */
      extInflux18ResetSchema(pReaderInfo);
    }
    code = extInflux18EnsureSchema(pReaderInfo, pConn);
    if (code == TSDB_CODE_SUCCESS && nEffMappings <= 0) {
      code = extInflux18FullMappings(pReaderInfo, &pFullMappings, &nFullMappings);
      if (code == TSDB_CODE_SUCCESS) {
        pEffMappings = pFullMappings;
        nEffMappings = nFullMappings;
      }
    }
    if (code == TSDB_CODE_SUCCESS) {
      code = extInflux18ExecScan(pReaderInfo, pConn, pEffMappings, nEffMappings, NULL, 0, NULL, skey,
                                 ekey, true, ppOut);
    }
    (void)taosThreadMutexUnlock(&pReaderInfo->connLock);
    taosMemoryFree(pFullMappings);
    if (ownConn) {
      extConnectorClose(pConn);
    }
    if (code != TSDB_CODE_SUCCESS) {
      ST_TASK_ELOG("ext: streamReaderExtFetchData influx18 failed code:%d", code);
    }
    return code;
  }

  /* streamReaderExtFetchData fetches pure calc data for aggregation;
   * apply calc WHERE prefilter (prefilter) only  -  not PRE_FILTER. */
  char prefilterBuf[EXT_SQL_PREFILTER_BUF_LEN] = {0};
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
  int8_t boundPrecision = pReaderInfo->spec.precision;
  if (!extIsValidTimePrecision(boundPrecision)) {
    // Calc-only sources carry no trigger precision in the spec; keep the per-source fallback in
    // line with the remote storage: Influx timestamps are nanoseconds, relational connectors
    // expose microseconds.
    boundPrecision = isInflux ? TSDB_TIME_PRECISION_NANO : TSDB_TIME_PRECISION_MICRO;
    ST_TASK_WLOG("ext: invalid calc fetch bound precision:%d, fallback to %d",
                 pReaderInfo->spec.precision, boundPrecision);
  }
  ST_TASK_DLOG("ext: streamReaderExtFetchData bounds skey:%" PRId64 " ekey:%" PRId64
               " precision:%d isInflux:%d aliasSelectItems:%d",
               skey, ekey, boundPrecision, isInflux, aliasSelectItems);
  if (ok && skey != INT64_MIN) {
    code = extAppendFetchDataBound(sql, sizeof(sql), &off, tsColQ, ">=", skey,
                                   boundPrecision, pReaderInfo, isInflux);
    if (code != TSDB_CODE_SUCCESS) {
      goto _exit;
    }
  }
  if (ok && ekey != INT64_MAX) {
    code = extAppendFetchDataBound(sql, sizeof(sql), &off, tsColQ, "<=", ekey,
                                   boundPrecision, pReaderInfo, isInflux);
    if (code != TSDB_CODE_SUCCESS) {
      goto _exit;
    }
  }
  // External sources do not guarantee row order, and the trigger consumes calc rows with an
  // ascending time cursor: order the fetch by the timestamp column like the relational uid
  // fetch path (extBuildRelationalFetchSql) so an unordered remote result cannot drop rows
  // or misplace them across windows.
  if (ok) {
    ok = extSqlCat(sql, sizeof(sql), &off, " ORDER BY %s ASC", tsColQ);
  }
  if (!ok) {
    ST_TASK_ELOG("ext: streamReaderExtFetchData SQL exceeds %d bytes, aborting fetch",
                 (int32_t)sizeof(sql));
    code = TSDB_CODE_OUT_OF_RANGE;
    goto _exit;
  }

  ST_TASK_DLOG("ext: streamReaderExtFetchData sql=\"%.200s\" nMappings=%d",
               sql, nMappings);

  if (pReaderInfo->borrowConn) {
    /* Borrow a pooled connection just for this fetch (see borrowConn). */
    SExtConnectorHandle *pConn = NULL;
    code = extOpenReaderConnector(pTask, &pReaderInfo->spec, &pConn);
    if (code != TSDB_CODE_SUCCESS) goto _exit;
    code = extQueryExecFetchAll(pConn, pReaderInfo->timezone, sql, (SExtColTypeMapping *)pMappings, nMappings, ppOut);
    extConnectorClose(pConn);
  } else {
    (void)taosThreadMutexLock(&pReaderInfo->connLock);
    code = extQueryExecFetchAll(pReaderInfo->pConn, pReaderInfo->timezone, sql,
                                (SExtColTypeMapping *)pMappings, nMappings, ppOut);
    (void)taosThreadMutexUnlock(&pReaderInfo->connLock);
  }
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: streamReaderExtFetchData failed code:%d", code);
    goto _exit;
  }
  code = extNormalizeTimestampBlockPrecision(pReaderInfo->spec.precision, *ppOut, pMappings, nMappings,
                                             "streamReaderExtFetchData");
  if (code != TSDB_CODE_SUCCESS) {
    ST_TASK_ELOG("ext: streamReaderExtFetchData normalize timestamp failed code:%d", code);
    goto _exit;
  }

  ST_TASK_DLOG("ext: streamReaderExtFetchData done rows=%" PRId64,
          *ppOut ? (*ppOut)->info.rows : 0);

_exit:
  return code;
}
