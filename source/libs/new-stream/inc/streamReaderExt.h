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
 * External source stream reader (ETR) -- internal header.
 *
 * P0 declared the SStreamExtReaderInfo struct and three stub entry points.
 * P2 fills in the struct fields and the full implementation in streamReaderExt.c.
 * See DS section 6.2.7.
 */
#ifndef _TD_STREAM_READER_EXT_H_
#define _TD_STREAM_READER_EXT_H_

#include "extConnector.h"
#include "stream.h"
#include "streamMsg.h"
#include "tcommon.h"
#include "thash.h"
#include "tmsg.h"
#include "tsimplehash.h"

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Value stored in pUidIndex: one entry per external-source "sub-table" (uid).
 * groupId is derived at init time from the PARTITION BY spec (DS section 6.2.7).
 * maxTs is the watermark: reader only fetches rows with ts > maxTs.
 */
/*
 * One entry in pUidIndex.  groupId and maxTs are always present.
 * tagsetKey is only populated for InfluxDB sources; it holds the
 * "col1=val1|col2=val2|..." string used to reconstruct the tag-based
 * WHERE clause in per-uid queries. partitionValues caches the typed
 * PARTITION BY tuple evaluated during DISTINCT block initialization so
 * GROUP_COL_VALUE_PULL does not evaluate the same expressions again.
 * Empty string/NULL respectively for MySQL/PG.
 *
 * The key is stored in its ESCAPED form, and escaping spends one extra byte for
 * every '=', '|' and backslash in the tagset, so the storage has to cover the
 * worst case (2x) of the historical unescaped budget.  Sizing it to the
 * unescaped budget would silently drop any series whose tags are rich in those
 * separators.
 */
#define EXT_TAGSET_KEY_RAW_MAX 1024
#define EXT_TAGSET_KEY_MAX     (EXT_TAGSET_KEY_RAW_MAX * 2 + 1)

typedef struct SUidIndexEntry {
  uint64_t groupId;
  char     tagsetKey[EXT_TAGSET_KEY_MAX]; /* escaped "col1=val1|col2=val2|..."; empty for MySQL/PG */
  SArray  *partitionValues; /* SArray<SStreamGroupValue>; owned */
} SUidIndexEntry;

/*
 * Per-ETR-task, in-memory-only reader state.
 * Built on first TDMT_STREAM_TRIGGER_PULL_EXT processing.
 * See DS section 6.2.7.
 *
 * pUidIndex   : SSHashObj<uint64_t uid, SUidIndexEntry{groupId, tagsetKey, partitionValues}>
 * pGroupIndex : SSHashObj<uint64_t groupId, SArray<uint64_t uid>*>
 * pTagsetIndex: SSHashObj<char[] tagset, uint64_t uid>  (InfluxDB only)
 */
typedef struct SStreamExtReaderInfo {
  /* Connection to the external source.  Opened in streamReaderExtOpen and
   * held for the reader's lifetime by the trigger reader only; NULL for calc
   * readers (see borrowConn). */
  SExtConnectorHandle *pConn;

  /* Calc readers borrow a pooled connection for each fetch and return it
   * right after, instead of pinning one per execId for the stream's lifetime
   * (execReplica instances per calc scan would otherwise exhaust the
   * per-source pool).  The pool also probes liveness on reuse, so a broken
   * connection is replaced instead of failing every later fetch. */
  bool borrowConn;

  /* Serializes use of pConn and the pull state of the trigger reader. */
  TdThreadMutex connLock;
  bool          connLockInited;

  /* Deploy-time spec snapshot (copy; owned by this struct). */
  SStreamExtTriggerSpec spec;

  /* Client timezone (IANA name) of the stream owning this reader.  The reader
   * executes its own SQL without a physi node, so it hands the name to the
   * connector explicitly; timezone-naive source timestamps are then converted
   * with the stream's timezone instead of the server global one.  Empty when
   * the scan plan carries none (connector falls back to server global TZ). */
  char timezone[TD_TIMEZONE_LEN];

  /* EExtTimestampTzClass of the reader's source timestamp column, taken from
   * SFederatedScanPhysiNode.pTimestampTypeHints.  The connector decodes
   * timezone-aware columns (MySQL TIMESTAMP, PG timestamptz) with the server
   * timezone and timezone-naive ones with the client timezone, so the remote
   * fetch/metadata bounds must be rendered with the matching timezone.
   * SQL Server datetimeoffset bounds use UTC, independently of either timezone. */
  int8_t tsTzClass;

  /* Back-pointer to the owning task for log context (not owned; lifetime >= pInfo). */
  const SStreamTask *pTask;

  /* InfluxDB N=64 batch loop: index of the next uid to pull.
   * Reset to 0 after all uids have been covered in a batch round. */
  int32_t influxBatchOffset;

  /* 1 = this reader's InfluxDB source is configured for 1.8 (OPTIONS.version).
   * 1.8 has no SQL endpoint the reader could query directly: series
   * enumeration, watermarks and data fetches all go through the provider's
   * restricted raw-scan / series-snapshot API instead. */
  bool influxV1;

  /* InfluxDB 1.8: minimal external-table descriptor handed to the connector
   * (owned; built on first use from the deploy spec) and the cached remote
   * schema (owned; freed and refetched when the remote reports a schema
   * change).  Ignored for every other source. */
  SExtTableNode *pInflux18Table;
  SExtTableMeta *pInflux18Meta;

  /* InfluxDB 1.8: last end-timestamp this reader reported to the trigger for a
   * uid (hash<uint64_t uid, int64_t ekey>).  When the trigger's watermark for a
   * uid still differs from it, the trigger has not consumed the previous META
   * row yet and the exact first timestamp of the new rows must be queried so
   * the first window starts at real data instead of at watermark+1. */
  SSHashObj *pInflux18MetaEkey;

  /* InfluxDB 1.8: series snapshot reused by the continuation pages of one META
   * enumeration (owned; NULL when no enumeration is in progress).  A snapshot
   * costs one HTTP request per INFLUX_V1_SERIES_PAGE_SIZE series, so a META
   * response that pages at STREAM_RETURN_ROWS_NUM must not re-read the whole
   * measurement for every page.  The latest-point snapshot and, when the round
   * has new series, the history-head snapshot are both kept while
   * influxBatchOffset > 0 and dropped when the enumeration completes or is
   * abandoned, so the next enumeration observes the remote's current series
   * set.  A failed page keeps both the snapshot and the cursor: the retry must
   * walk the same series in the same order. */
  SSDataBlock *pInflux18MetaSnap;
  SSDataBlock *pInflux18MetaFirstSnap;

  /* Derived from the two snapshots above (owned; always released together with
   * them).  Building the uid -> last/first point maps costs one tagset-key
   * build per series, so rebuilding them for every page of one enumeration is
   * O(N^2 / STREAM_RETURN_ROWS_NUM) for an N-series measurement.  They depend
   * only on the snapshot, so they are built once per enumeration. */
  SSHashObj *pInflux18MetaUidTs;       /* hash<uint64_t uid, int64_t ts> */
  SArray    *pInflux18MetaUidOrder;    /* SArray<SExtInflux18UidTs> */
  SSHashObj *pInflux18MetaFirstUidTs;  /* hash<uint64_t uid, int64_t ts> */
  /* Set once the running enumeration has fetched the history-head snapshot (or
   * tried to); reset together with the cache.  Keeps the per-page decision from
   * rescanning the whole uid map. */
  bool influx18MetaFirstTried;
  /* Common lower bound of the history-head snapshot, derived from the series
   * that still need a FIRST alignment.  Deriving it walks the whole uid map, and
   * a steady-state enumeration (every series already reported at its watermark)
   * never asks for the snapshot at all, so the walk is deferred until the first
   * row that actually needs it and then cached for the rest of the enumeration. */
  int64_t influx18MetaFirstLower;
  bool    influx18MetaFirstLowerValid;

  /* InfluxDB tag column names (SArray<char[TSDB_COL_NAME_LEN]>).
   * Populated by InfluxDB table-list enumeration and refreshed when an empty
   * series set is re-enumerated; NULL for MySQL/PG.
   * Used to build the tagset string from returned rows and to look up uid. */
  SArray *pInfluxTagCols;   /* SArray<char[TSDB_COL_NAME_LEN]>; InfluxDB only */

  /* Parsed/cached scalar expression templates for spec.partitionTagExprs, parallel
   * to spec.partitionTagCols (same length as spec.partitionTagCols when
   * non-NULL). A NULL entry means that slot's partitionTagExprs[i] is "" (no
   * expression to evaluate -- bare column or tbname sentinel). Parsed once
   * in streamReaderExtOpen, then bound once to the DISTINCT tag block slots
   * before vectorized evaluation. Freed in streamReaderExtClose. */
  SArray *pPartitionColExprNodes;  /* SArray<SNode*> */

  /* Three in-memory lookup tables (DS section 6.2.7). */
  SSHashObj *pUidIndex;    /* hash<uint64_t uid,    SUidIndexEntry> */
  SSHashObj *pGroupIndex;  /* hash<uint64_t groupId, SArray<uint64_t>*> */
  SSHashObj *pTagsetIndex; /* hash<char[] tagset,  uint64_t uid>; InfluxDB only */

  /* Calc scan time range (STimeRangeNode*, owned) extracted from the calc
   * reader's scan plan at deploy time.  Each execId owns a separate clone.
   * mnode deploys one calc reader task per calc scan plan, so each external
   * scan (including each side of a self-join) has its own replica array.
   * When the calc statement carries
   * time-returning expressions (e.g. _twstart/_twend or casted localtime
   * placeholders), the fetch bounds must be evaluated per request with the
   * window parameters - the same way vnodeStream.c processCalaTimeRange does
   * for table scans - instead of trusting the raw merged [wstart, wend]
   * range.  NULL when the plan has no time range (full scan). */
  SNode *pCalcTimeRange;

  /* PRE_FILTER AST (owned), parsed from spec.triggerPrefilterAst at open.
   * InfluxDB 1.8 cannot execute the rendered triggerPrefilter SQL; the 1.8
   * provider renders this AST to InfluxQL for every trigger-side fetch.
   * NULL when the stream has no PRE_FILTER or the source is not 1.8. */
  SNode *pTriggerPrefilter;

  /* SERIES identity condition (owned), parsed from spec.tagCondJson at open.
   * It constrains which series belong to a virtual-table source and is kept
   * separate from the trigger PRE_FILTER. */
  SNode *pInfluxSeriesFilter;

  /* InfluxDB empty-SERIES-match enumeration cache: timestamp (ms) of the last
   * DISTINCT enumeration that matched no tag combinations. Repeated idle pulls
   * skip the information_schema + DISTINCT round-trips within the re-check
   * window; new remote data that appears later still triggers a fresh
   * enumeration. Reset naturally when the reader is re-deployed for a patch. */
  int64_t influxEmptyEnumTsMs;
} SStreamExtReaderInfo;

/* ---------------------------------------------------------------------------
 * Lifecycle API
 *
 *   streamReaderExtOpen   - decrypt password, open extConnector, init 3 hashes
 *   streamReaderExtClose  - close extConnector, cleanup hashes, zero the struct
 * --------------------------------------------------------------------------- */
int32_t streamReaderExtOpen(void *pSpec, const SStreamTask *pTask, SNode *pCalcTimeRange,
                            const char *timezone, int8_t tsTzClass,
                            SStreamExtReaderInfo **ppReaderInfo);
void    streamReaderExtClose(SStreamExtReaderInfo *pReaderInfo);
int32_t streamReaderExtGetCalcInfo(const SStreamReaderTask *pTask, int32_t execId,
                                  SStreamExtReaderInfo **ppReaderInfo);

/* ---------------------------------------------------------------------------
 * PULL dispatcher
 *
 *   pReaderInfo : reader state (must be non-NULL)
 *   pullType    : ESTriggerPullType (STRIGGER_PULL_*_EXT)
 *   pReq        : in-memory SSTriggerExtPullReq* (caller owns)
 *   ppRsp       : output SSTriggerExtPullRsp* (callee allocates; caller frees
 *                 with streamExtPullRspFree)
 *
 * Returns TSDB_CODE_SUCCESS on success.
 * --------------------------------------------------------------------------- */
int32_t streamReaderExtHandlePull(SStreamExtReaderInfo *pReaderInfo,
                                  int32_t pullType, const void *pReq,
                                  void **ppRsp);

/* Free helper for SSTriggerExtPullRsp allocated inside streamReaderExtHandlePull. */
void streamExtPullRspFree(SSTriggerExtPullRsp *pRsp);

/* ---------------------------------------------------------------------------
 * EXT fetch: called by handleExtFetchReq in snode.c to execute an ad-hoc
 * SELECT against the external source and return a data block.
 *
 *   pReaderInfo : ETR state opened by streamReaderExtOpen (must be non-NULL)
 *   skey        : fetch time-range start (inclusive), INT64_MIN = no lower bound
 *   ekey        : fetch time-range end   (inclusive), INT64_MAX = no upper bound
 *   ppOut       : output SSDataBlock*; callee allocates; caller destroys via blockDataDestroy.
 *                 Set to NULL when no rows are available (no error).
 *
 * Returns TSDB_CODE_SUCCESS on success.
 * --------------------------------------------------------------------------- */
int32_t streamReaderExtFetchData(SStreamExtReaderInfo *pReaderInfo,
                                 int64_t skey, int64_t ekey,
                                 SSDataBlock **ppOut);

/* ---------------------------------------------------------------------------
 * EXT fetch time-range evaluation  -  mirrors vnodeStream.c processCalaTimeRange.
 *
 *   pReaderInfo : ETR state (pCalcTimeRange may be NULL -> full scan)
 *   pRt         : stream runtime func info of the fetch request (borrowed)
 *   pSkey/pEkey : output inclusive fetch bounds.  INT64_MIN/INT64_MAX means
 *                 unbounded (no range to apply / no windows).  An empty range
 *                 is reported as skey > ekey (the remote query returns no rows).
 *
 * A raw merged [wstart, wend] range is only correct for the standard external
 * window shape.  When the calc statement carries time-returning expressions
 * (STimeRangeNode.needCalc), the bounds are evaluated with the current window
 * parameters, exactly like the non-external reader path.  Multi-group calc
 * requests are bounded by the union of the per-group ranges.
 *
 * When pReaderInfo->pCalcTimeRange is set the result is authoritative: the
 * caller must not fall back to the runner-published curWindow.
 * --------------------------------------------------------------------------- */
int32_t streamReaderExtCalcFetchRange(SStreamExtReaderInfo *pReaderInfo,
                                      const SStreamRuntimeFuncInfo *pRt,
                                      int64_t *pSkey, int64_t *pEkey);

/* ---------------------------------------------------------------------------
 * Render the remote SQL literal for one time bound of this reader.
 *
 *   pInfo     : reader state; its tsTzClass and spec.sourceType select the
 *               spelling (timezone-naive wall clock in the stream timezone,
 *               UTC wall clock, UTC wall clock + "+00:00", or FROM_UNIXTIME()
 *               for a timezone-aware MySQL column - see extSqlBoundForm).
 *   bound     : epoch in `precision` units
 *   precision : TSDB_TIME_PRECISION_* of `bound`
 *   buf/bufLen: output buffer; EXT_SQL_TS_LITERAL_LEN is always enough
 *
 * Returns the number of bytes written (snprintf semantics), or a negative
 * TSDB error code.
 * --------------------------------------------------------------------------- */
int32_t streamReaderExtRenderTimeBound(const SStreamExtReaderInfo *pInfo, int64_t bound,
                                       int8_t precision, char *buf, int32_t bufLen);

#ifdef __cplusplus
}
#endif
#endif /* _TD_STREAM_READER_EXT_H_ */
