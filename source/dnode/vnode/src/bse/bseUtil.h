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

#ifndef BSE_UTIL_H_
#define BSE_UTIL_H_

#include "bse.h"
#include "os.h"
#include "tlog.h"
#include "tmsg.h"

#ifdef __cplusplus
extern "C" {
#endif
// clang-format off
#define bseFatal(...) do { if (bseDebugFlag & DEBUG_FATAL) { taosPrintLog("BSE FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }}     while(0)
#define bseError(...) do { if (bseDebugFlag & DEBUG_ERROR) { taosPrintLog("BSE ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }}     while(0)
#define bseWarn(...)  do { if (bseDebugFlag & DEBUG_WARN)  { taosPrintLog("BSE WARN ", DEBUG_WARN, 255, __VA_ARGS__); }}       while(0)
#define bseInfo(...)  do { if (bseDebugFlag & DEBUG_INFO)  { taosPrintLog("BSE ", DEBUG_INFO, 255, __VA_ARGS__); }}            while(0)
#define bseDebug(...) do { if (bseDebugFlag & DEBUG_DEBUG) { taosPrintLog("BSE ", DEBUG_DEBUG, bseDebugFlag, __VA_ARGS__); }}    while(0)
#define bseTrace(...) do { if (bseDebugFlag & DEBUG_TRACE) { taosPrintLog("BSE ", DEBUG_TRACE, bseDebugFlag, __VA_ARGS__); }}    while(0)

#define bseGTrace(param, ...) do { if (bseDebugFlag & DEBUG_TRACE) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseTrace(param ",QID:%s", __VA_ARGS__, buf);}} while(0)
#define bseGFatal(param, ...) do { if (bseDebugFlag & DEBUG_FATAL) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseFatal(param ",QID:%s", __VA_ARGS__, buf);}} while(0)
#define bseGError(param, ...) do { if (bseDebugFlag & DEBUG_ERROR) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseError(param ",QID:%s", __VA_ARGS__, buf);}} while(0)
#define bseGWarn(param, ...)  do { if (bseDebugFlag & DEBUG_WARN)  { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseWarn(param ",QID:%s", __VA_ARGS__, buf);}} while(0)
#define bseGInfo(param, ...)  do { if (bseDebugFlag & DEBUG_INFO)  { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseInfo(param ",QID:%s", __VA_ARGS__, buf);}} while(0)
#define bseGDebug(param, ...) do { if (bseDebugFlag & DEBUG_DEBUG) { char buf[40] = {0}; TRACE_TO_STR(trace, buf); bseDebug(param ",QID:%s", __VA_ARGS__, buf);}}    while(0)



#define BSE_DATA_SUFFIX  "data"
#define BSE_LOG_SUFFIX   "log"
#define BSE_INDEX_SUFFIX "idx"
#define BSE_META_SUFFIX  "meta"

#define BSE_FILE_FULL_LEN TSDB_FILENAME_LEN

void bseBuildDataFullName(SBse *pBse, char *name, char *buf);
void bseBuildIndexFullName(SBse *pBse, int64_t ver, char *name);
void bseBuildLogFullName(SBse *pBse, int64_t ver, char *buf);
void bseBuildCurrentName(SBse *pBse, char *name) ;
void bseBuildTempCurrentName(SBse *pBse, char *name);
void bseBuildFullMetaName(SBse *pBse,char *name, char *path);
void bseBuildFullTempMetaName(SBse *pBse, char *name, char *path);
void bseBuildFullName(SBse *pBse, char *name, char *fullname);
void bseBuildDataName(int64_t seq, char *name);

void bseBuildMetaName(int64_t ts, char *name); 
void bseBuildTempMetaName(int64_t ts, char *name); 

int32_t bseCompressData(int8_t type, void *src, int32_t srcSize, void *dst, int32_t *dstSize) ;
int32_t bseDecompressData(int8_t type, void *src, int32_t srcSize, void *dst, int32_t *dstSize);

int32_t bseGetTableIdBySeq(SBse *pBse, int64_t seq, int64_t *timestamp );

typedef void* bsequeue[2];
#define BSE_QUEUE_NEXT(q) (*(bsequeue**)&((*(q))[0]))
#define BSE_QUEUE_PREV(q) (*(bsequeue**)&((*(q))[1]))

#define BSE_QUEUE_PREV_NEXT(q) (BSE_QUEUE_NEXT(BSE_QUEUE_PREV(q)))
#define BSE_QUEUE_NEXT_PREV(q) (BSE_QUEUE_PREV(BSE_QUEUE_NEXT(q)))

#define BSE_QUEUE_INIT(q)    \
  {                      \
    BSE_QUEUE_NEXT(q) = (q); \
    BSE_QUEUE_PREV(q) = (q); \
  }

#define BSE_QUEUE_IS_EMPTY(q) ((const bsequeue*)(q) == (const bsequeue*)BSE_QUEUE_NEXT(q))

#define BSE_QUEUE_PUSH(q, e)           \
  {                                \
    BSE_QUEUE_NEXT(e) = (q);           \
    BSE_QUEUE_PREV(e) = BSE_QUEUE_PREV(q); \
    BSE_QUEUE_PREV_NEXT(e) = (e);      \
    BSE_QUEUE_PREV(q) = (e);           \
  }

#define BSE_QUEUE_REMOVE(e)                 \
  {                                     \
    BSE_QUEUE_PREV_NEXT(e) = BSE_QUEUE_NEXT(e); \
    BSE_QUEUE_NEXT_PREV(e) = BSE_QUEUE_PREV(e); \
  }
#define BSE_QUEUE_SPLIT(h, q, n)       \
  do {                             \
    BSE_QUEUE_PREV(n) = BSE_QUEUE_PREV(h); \
    BSE_QUEUE_PREV_NEXT(n) = (n);      \
    BSE_QUEUE_NEXT(n) = (q);           \
    BSE_QUEUE_PREV(h) = BSE_QUEUE_PREV(q); \
    BSE_QUEUE_PREV_NEXT(h) = (h);      \
    BSE_QUEUE_PREV(q) = (n);           \
  } while (0)

#define BSE_QUEUE_MOVE(h, n)        \
  do {                          \
    if (BSE_QUEUE_IS_EMPTY(h)) {    \
      BSE_QUEUE_INIT(n);            \
    } else {                    \
      bsequeue* q = BSE_QUEUE_HEAD(h); \
      BSE_QUEUE_SPLIT(h, q, n);     \
    }                           \
  } while (0)

#define BSE_QUEUE_HEAD(q) (BSE_QUEUE_NEXT(q))

#define BSE_QUEUE_TAIL(q) (BSE_QUEUE_PREV(q))

#define BSE_QUEUE_FOREACH(q, e) for ((q) = BSE_QUEUE_NEXT(e); (q) != (e); (q) = BSE_QUEUE_NEXT(q))

/* Return the structure holding the given element. */
#define BSE_QUEUE_DATA(e, type, field) ((type*)((void*)((char*)(e)-offsetof(type, field))))


#define BSE_DATA_VER 0x1
#define BSE_FMT_VER 0x1
#define BSE_META_VER 0x1

#define BSE_VGID(pBse) ((pBse)->cfg.vgId)
#define BSE_KEEY_DAYS(pBse) ((pBse)->cfg.keepDays)
#define BSE_RETENTION(pBse) ((pBse)->cfg.retention)
#define BSE_TIME_PRECISION(pBse) ((pBse)->cfg.precision)
#define BSE_BLOCK_SIZE(pBse) ((pBse)->cfg.blockSize)
#define BSE_COMPRESS_TYPE(pBse) ((pBse)->cfg.compressType)
#define BSE_TABLE_CACHE_SIZE(p) ((p)->cfg.tableCacheSize)
#define BSE_BLOCK_CACHE_SIZE(p) ((p)->cfg.blockCacheSize)

// clang-format on
#ifdef __cplusplus
}
#endif

#endif