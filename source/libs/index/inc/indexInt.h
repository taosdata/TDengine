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
#include "tlrucache.h"
#include "tutil.h"

#ifdef __cplusplus
extern "C" {
#endif

// clang-format off
#define indexFatal(...) do { if (idxDebugFlag & DEBUG_FATAL) {  taosPrintLog("IDX FATAL ", DEBUG_FATAL, 255, __VA_ARGS__); }} while (0)
#define indexError(...) do { if (idxDebugFlag & DEBUG_ERROR) {  taosPrintLog("IDX ERROR ", DEBUG_ERROR, 255, __VA_ARGS__); }} while (0)
#define indexWarn(...)  do { if (idxDebugFlag & DEBUG_WARN)  {  taosPrintLog("IDX WARN ", DEBUG_WARN, 255, __VA_ARGS__); }} while (0)
#define indexInfo(...)  do { if (idxDebugFlag & DEBUG_INFO)  { taosPrintLog("IDX ", DEBUG_INFO, 255, __VA_ARGS__); } } while (0)
#define indexDebug(...) do { if (idxDebugFlag & DEBUG_DEBUG) { taosPrintLog("IDX ", DEBUG_DEBUG, idxDebugFlag, __VA_ARGS__);} } while (0)
#define indexTrace(...) do { if (idxDebugFlag & DEBUG_TRACE) { taosPrintLog("IDX", DEBUG_TRACE, idxDebugFlag, __VA_ARGS__);} } while (0)
// clang-format on

extern void* indexQhandle;

typedef enum { LT, LE, GT, GE, CONTAINS, EQ } RangeType;
typedef enum { kTypeValue, kTypeDeletion } STermValueType;
typedef enum { kRebuild, kFinished } SIdxStatus;

typedef struct SIndexStat {
  int32_t total;
  int32_t add;      //
  int32_t del;      //
  int32_t update;   //
  int32_t terms;    //
  int32_t distCol;  // distinct column
} SIndexStat;

struct SIndex {
  SIndexOpts opts;

  int64_t   refId;
  void*     cache;
  void*     tindex;
  SHashObj* colObj;  // < field name, field id>

  int64_t    suid;     // current super table id, -1 is normal table
  int32_t    version;  // current version allocated to cache
  SLRUCache* lru;
  char*      path;

  int8_t        status;
  SIndexStat    stat;
  TdThreadMutex mtx;
  tsem_t        sem;
  bool          quit;
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

typedef struct TFileCacheKey {
  uint64_t suid;
  uint8_t  colType;
  char*    colName;
  int32_t  nColName;
} ICacheKey;

int32_t idxSerialCacheKey(ICacheKey* key, char* buf);

int idxFlushCacheToTFile(SIndex* sIdx, void*, bool quit);

int64_t idxAddRef(void* p);
int32_t idxRemoveRef(int64_t ref);
void    idxAcquireRef(int64_t ref);
void    idxReleaseRef(int64_t ref);

#define IDX_TYPE_CONTAIN_EXTERN_TYPE(ty, exTy) (((ty >> 4) & (exTy)) != 0)

#define IDX_TYPE_GET_TYPE(ty) (ty & 0x0F)

#define IDX_TYPE_ADD_EXTERN_TYPE(ty, exTy) \
  do {                                     \
    uint8_t oldTy = ty;                    \
    ty = ((ty >> 4) & 0xFF) | exTy;        \
    ty = (ty << 4) | oldTy;                \
  } while (0)

#ifdef __cplusplus
}
#endif

#endif /*_TD_INDEX_INT_H_*/
