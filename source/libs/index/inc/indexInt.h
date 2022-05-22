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

#ifndef _TD_INDEX_INT_H_
#define _TD_INDEX_INT_H_

#include "os.h"

#include "index.h"
#include "taos.h"
#include "tarray.h"
#include "tchecksum.h"
#include "thash.h"
#include "tlog.h"
#include "tutil.h"

#ifdef USE_LUCENE
#include <lucene++/Lucene_c.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

// clang-format off
#define indexFatal(...) do { if (idxDebugFlag & DEBUG_FATAL) {  taosPrintLog("INDEX FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }} while (0)
#define indexError(...) do { if (idxDebugFlag & DEBUG_ERROR) {  taosPrintLog("INDEX ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }} while (0)
#define indexWarn(...)  do { if (idxDebugFlag & DEBUG_WARN)  {  taosPrintLog("INDEX WARN ", DEBUG_WARN, 255, __VA_ARGS__); }} while (0)
#define indexInfo(...)  do { if (idxDebugFlag & DEBUG_INFO)  { taosPrintLog("INDEX ", DEBUG_INFO, 255, __VA_ARGS__); } } while (0)
#define indexDebug(...) do { if (idxDebugFlag & DEBUG_DEBUG) { taosPrintLog("INDEX ", DEBUG_DEBUG, sDebugFlag, __VA_ARGS__);} } while (0)
#define indexTrace(...) do { if (idxDebugFlag & DEBUG_TRACE) { taosPrintLog("INDEX ", DEBUG_TRACE, sDebugFlag, __VA_ARGS__);} } while (0)
// clang-format on

typedef enum { LT, LE, GT, GE } RangeType;
typedef enum { kTypeValue, kTypeDeletion } STermValueType;

typedef struct SIndexStat {
  int32_t totalAdded;    //
  int32_t totalDeled;    //
  int32_t totalUpdated;  //
  int32_t totalTerms;    //
  int32_t distinctCol;   // distinct column
} SIndexStat;

struct SIndex {
  int64_t   refId;
  void*     cache;
  void*     tindex;
  SHashObj* colObj;  // < field name, field id>

  int64_t suid;      // current super table id, -1 is normal table
  int32_t cVersion;  // current version allocated to cache

  char* path;

  SIndexStat    stat;
  TdThreadMutex mtx;
  tsem_t        sem;
  bool          quit;
};

struct SIndexOpts {
#ifdef USE_LUCENE
  void* opts;
#endif

#ifdef USE_INVERTED_INDEX
  int32_t cacheSize;  // MB
  // add cache module later
#endif
  int32_t cacheOpt;  // MB
};

struct SIndexMultiTermQuery {
  EIndexOperatorType opera;
  SArray*            query;
};

// field and key;
typedef struct SIndexTerm {
  int64_t            suid;
  SIndexOperOnColumn operType;  // oper type, add/del/update
  uint8_t            colType;   // term data type, str/interger/json
  char*              colName;
  int32_t            nColName;
  char*              colVal;
  int32_t            nColVal;
} SIndexTerm;

typedef struct SIndexTermQuery {
  SIndexTerm*     term;
  EIndexQueryType qType;
} SIndexTermQuery;

typedef struct Iterate Iterate;

typedef struct IterateValue {
  int8_t   type;  // opera type, ADD_VALUE/DELETE_VALUE
  uint64_t ver;   // data ver, tfile data version is 0
  char*    colVal;

  SArray* val;
} IterateValue;

typedef struct Iterate {
  void*        iter;
  IterateValue val;
  bool (*next)(Iterate* iter);
  IterateValue* (*getValue)(Iterate* iter);
} Iterate;

void iterateValueDestroy(IterateValue* iv, bool destroy);

extern void* indexQhandle;

typedef struct TFileCacheKey {
  uint64_t suid;
  uint8_t  colType;
  char*    colName;
  int32_t  nColName;
} ICacheKey;

int indexFlushCacheToTFile(SIndex* sIdx, void*);

int64_t indexAddRef(void* p);
int32_t indexRemoveRef(int64_t ref);
void    indexAcquireRef(int64_t ref);
void    indexReleaseRef(int64_t ref);

int32_t indexSerialCacheKey(ICacheKey* key, char* buf);
// int32_t indexSerialKey(ICacheKey* key, char* buf);
// int32_t indexSerialTermKey(SIndexTerm* itm, char* buf);

#define INDEX_TYPE_CONTAIN_EXTERN_TYPE(ty, exTy) (((ty >> 4) & (exTy)) != 0)

#define INDEX_TYPE_GET_TYPE(ty) (ty & 0x0F)

#define INDEX_TYPE_ADD_EXTERN_TYPE(ty, exTy) \
  do {                                       \
    uint8_t oldTy = ty;                      \
    ty = (ty >> 4) | exTy;                   \
    ty = (ty << 4) | oldTy;                  \
  } while (0)

#ifdef __cplusplus
}
#endif

#endif /*_TD_INDEX_INT_H_*/
