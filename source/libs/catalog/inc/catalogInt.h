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

#ifndef _TD_CATALOG_INT_H_
#define _TD_CATALOG_INT_H_

#ifdef __cplusplus
extern "C" {
#endif

#include "catalog.h"
#include "tcommon.h"
#include "query.h"

#define CTG_DEFAULT_CACHE_CLUSTER_NUMBER 6
#define CTG_DEFAULT_CACHE_VGROUP_NUMBER 100
#define CTG_DEFAULT_CACHE_DB_NUMBER 20
#define CTG_DEFAULT_CACHE_TBLMETA_NUMBER 1000
#define CTG_DEFAULT_RENT_SECOND 10
#define CTG_DEFAULT_RENT_SLOT_SIZE 10
#define CTG_DEFAULT_MAX_RETRY_TIMES 3

#define CTG_RENT_SLOT_SECOND 1.5

#define CTG_DEFAULT_INVALID_VERSION (-1)

#define CTG_ERR_CODE_TABLE_NOT_EXIST TSDB_CODE_TDB_INVALID_TABLE_ID

enum {
  CTG_READ = 1,
  CTG_WRITE,
};

enum {
  CTG_RENT_DB = 1,
  CTG_RENT_STABLE,
};

enum {
  CTG_ACT_UPDATE_VG = 0,
  CTG_ACT_UPDATE_TBL,
  CTG_ACT_REMOVE_DB,
  CTG_ACT_REMOVE_STB,
  CTG_ACT_REMOVE_TBL,
  CTG_ACT_MAX
};

typedef struct SCtgDebug {
  bool     lockEnable;
  bool     cacheEnable;
  bool     apiEnable;
  bool     metaEnable;
  uint32_t showCachePeriodSec;
} SCtgDebug;


typedef struct SCtgTbMetaCache {
  SRWLatch  stbLock;
  SRWLatch  metaLock;        // RC between cache destroy and all other operations
  SHashObj *metaCache;       //key:tbname, value:STableMeta
  SHashObj *stbCache;        //key:suid, value:STableMeta*
} SCtgTbMetaCache;

typedef struct SCtgDBCache {
  SRWLatch         vgLock;
  uint64_t         dbId;
  int8_t           deleted;
  SDBVgInfo       *vgInfo;  
  SCtgTbMetaCache  tbCache;
} SCtgDBCache;

typedef struct SCtgRentSlot {
  SRWLatch lock;
  bool     needSort;
  SArray  *meta;  // element is SDbVgVersion or SSTableMetaVersion
} SCtgRentSlot;

typedef struct SCtgRentMgmt {
  int8_t         type;
  uint16_t       slotNum;
  uint16_t       slotRIdx;
  int64_t        lastReadMsec;
  SCtgRentSlot  *slots;
} SCtgRentMgmt;

typedef struct SCatalog {
  uint64_t         clusterId;  
  SHashObj        *dbCache;      //key:dbname, value:SCtgDBCache
  SCtgRentMgmt     dbRent;
  SCtgRentMgmt     stbRent;
} SCatalog;

typedef struct SCtgApiStat {

} SCtgApiStat;

typedef struct SCtgRuntimeStat {
  uint64_t qNum;
  uint64_t qDoneNum;
} SCtgRuntimeStat;

typedef struct SCtgCacheStat {

} SCtgCacheStat;

typedef struct SCatalogStat {
  SCtgApiStat      api;
  SCtgRuntimeStat  runtime;
  SCtgCacheStat    cache;
} SCatalogStat;

typedef struct SCtgUpdateMsgHeader {
  SCatalog* pCtg;
} SCtgUpdateMsgHeader;

typedef struct SCtgUpdateVgMsg {
  SCatalog* pCtg;
  char  dbFName[TSDB_DB_FNAME_LEN];
  uint64_t dbId;
  SDBVgInfo* dbInfo;
} SCtgUpdateVgMsg;

typedef struct SCtgUpdateTblMsg {
  SCatalog* pCtg;
  STableMetaOutput* output;
} SCtgUpdateTblMsg;

typedef struct SCtgRemoveDBMsg {
  SCatalog* pCtg;
  char  dbFName[TSDB_DB_FNAME_LEN];
  uint64_t dbId;
} SCtgRemoveDBMsg;

typedef struct SCtgRemoveStbMsg {
  SCatalog* pCtg;
  char  dbFName[TSDB_DB_FNAME_LEN];
  char  stbName[TSDB_TABLE_NAME_LEN];
  uint64_t dbId;
  uint64_t suid;
} SCtgRemoveStbMsg;

typedef struct SCtgRemoveTblMsg {
  SCatalog* pCtg;
  char  dbFName[TSDB_DB_FNAME_LEN];
  char  tbName[TSDB_TABLE_NAME_LEN];
  uint64_t dbId;
} SCtgRemoveTblMsg;


typedef struct SCtgMetaAction {
  int32_t  act;
  void    *data;
  bool     syncReq;
  uint64_t seqId;
} SCtgMetaAction;

typedef struct SCtgQNode {
  SCtgMetaAction         action;
  struct SCtgQNode      *next;
} SCtgQNode;

typedef struct SCtgQueue {
  SRWLatch              qlock;
  uint64_t              seqId;
  uint64_t              seqDone;
  SCtgQNode            *head;
  SCtgQNode            *tail;
  tsem_t                reqSem;  
  tsem_t                rspSem;  
  uint64_t              qRemainNum;
} SCtgQueue;

typedef struct SCatalogMgmt {
  bool                  exit;
  SRWLatch              lock;
  SCtgQueue             queue;
  TdThread             updateThread;  
  SHashObj             *pCluster;     //key: clusterId, value: SCatalog*
  SCatalogStat          stat;
  SCatalogCfg           cfg;
} SCatalogMgmt;

typedef uint32_t (*tableNameHashFp)(const char *, uint32_t);
typedef int32_t (*ctgActFunc)(SCtgMetaAction *);

typedef struct SCtgAction {
  int32_t    actId;
  char       name[32];
  ctgActFunc func;
} SCtgAction;

#define CTG_QUEUE_ADD() atomic_add_fetch_64(&gCtgMgmt.queue.qRemainNum, 1)
#define CTG_QUEUE_SUB() atomic_sub_fetch_64(&gCtgMgmt.queue.qRemainNum, 1)

#define CTG_STAT_ADD(n) atomic_add_fetch_64(&(n), 1)
#define CTG_STAT_SUB(n) atomic_sub_fetch_64(&(n), 1)

#define CTG_IS_META_NULL(type) ((type) == META_TYPE_NULL_TABLE)
#define CTG_IS_META_CTABLE(type) ((type) == META_TYPE_CTABLE)
#define CTG_IS_META_TABLE(type) ((type) == META_TYPE_TABLE)
#define CTG_IS_META_BOTH(type) ((type) == META_TYPE_BOTH_TABLE)

#define CTG_FLAG_STB          0x1
#define CTG_FLAG_NOT_STB      0x2
#define CTG_FLAG_UNKNOWN_STB  0x4
#define CTG_FLAG_INF_DB       0x8
#define CTG_FLAG_FORCE_UPDATE 0x10

#define CTG_FLAG_IS_STB(_flag) ((_flag) & CTG_FLAG_STB)
#define CTG_FLAG_IS_NOT_STB(_flag) ((_flag) & CTG_FLAG_NOT_STB)
#define CTG_FLAG_IS_UNKNOWN_STB(_flag) ((_flag) & CTG_FLAG_UNKNOWN_STB)
#define CTG_FLAG_IS_INF_DB(_flag) ((_flag) & CTG_FLAG_INF_DB)
#define CTG_FLAG_IS_FORCE_UPDATE(_flag) ((_flag) & CTG_FLAG_FORCE_UPDATE)
#define CTG_FLAG_SET_INF_DB(_flag) ((_flag) |= CTG_FLAG_INF_DB)
#define CTG_FLAG_SET_STB(_flag, tbType) do { (_flag) |= ((tbType) == TSDB_SUPER_TABLE) ? CTG_FLAG_STB : ((tbType) > TSDB_SUPER_TABLE ? CTG_FLAG_NOT_STB : CTG_FLAG_UNKNOWN_STB); } while (0)
#define CTG_FLAG_MAKE_STB(_isStb) (((_isStb) == 1) ? CTG_FLAG_STB : ((_isStb) == 0 ? CTG_FLAG_NOT_STB : CTG_FLAG_UNKNOWN_STB))
#define CTG_FLAG_MATCH_STB(_flag, tbType) (CTG_FLAG_IS_UNKNOWN_STB(_flag) || (CTG_FLAG_IS_STB(_flag) && (tbType) == TSDB_SUPER_TABLE) || (CTG_FLAG_IS_NOT_STB(_flag) && (tbType) != TSDB_SUPER_TABLE))

#define CTG_IS_INF_DBNAME(_dbname) ((*(_dbname) == 'i') && (0 == strcmp(_dbname, TSDB_INFORMATION_SCHEMA_DB)))

#define CTG_META_SIZE(pMeta) (sizeof(STableMeta) + ((pMeta)->tableInfo.numOfTags + (pMeta)->tableInfo.numOfColumns) * sizeof(SSchema))

#define CTG_TABLE_NOT_EXIST(code) (code == CTG_ERR_CODE_TABLE_NOT_EXIST) 
#define CTG_DB_NOT_EXIST(code) (code == TSDB_CODE_MND_DB_NOT_EXIST) 

#define ctgFatal(param, ...)  qFatal("CTG:%p " param, pCtg, __VA_ARGS__)
#define ctgError(param, ...)  qError("CTG:%p " param, pCtg, __VA_ARGS__)
#define ctgWarn(param, ...)   qWarn("CTG:%p " param, pCtg, __VA_ARGS__)
#define ctgInfo(param, ...)   qInfo("CTG:%p " param, pCtg, __VA_ARGS__)
#define ctgDebug(param, ...)  qDebug("CTG:%p " param, pCtg, __VA_ARGS__)
#define ctgTrace(param, ...)  qTrace("CTG:%p " param, pCtg, __VA_ARGS__)

#define CTG_LOCK_DEBUG(...) do { if (gCTGDebug.lockEnable) { qDebug(__VA_ARGS__); } } while (0)
#define CTG_CACHE_DEBUG(...) do { if (gCTGDebug.cacheEnable) { qDebug(__VA_ARGS__); } } while (0)
#define CTG_API_DEBUG(...) do { if (gCTGDebug.apiEnable) { qDebug(__VA_ARGS__); } } while (0)

#define TD_RWLATCH_WRITE_FLAG_COPY 0x40000000

#define CTG_IS_LOCKED(_lock) atomic_load_32((_lock))

#define CTG_LOCK(type, _lock) do {   \
  if (CTG_READ == (type)) {          \
    assert(atomic_load_32((_lock)) >= 0);  \
    CTG_LOCK_DEBUG("CTG RLOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    taosRLockLatch(_lock);           \
    CTG_LOCK_DEBUG("CTG RLOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    assert(atomic_load_32((_lock)) > 0);  \
  } else {                                                \
    assert(atomic_load_32((_lock)) >= 0);  \
    CTG_LOCK_DEBUG("CTG WLOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);  \
    taosWLockLatch(_lock);                                \
    CTG_LOCK_DEBUG("CTG WLOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);  \
    assert(atomic_load_32((_lock)) == TD_RWLATCH_WRITE_FLAG_COPY);  \
  }                                                       \
} while (0)

#define CTG_UNLOCK(type, _lock) do {                       \
  if (CTG_READ == (type)) {                                \
    assert(atomic_load_32((_lock)) > 0);  \
    CTG_LOCK_DEBUG("CTG RULOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    taosRUnLockLatch(_lock);                              \
    CTG_LOCK_DEBUG("CTG RULOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    assert(atomic_load_32((_lock)) >= 0);  \
  } else {                                                \
    assert(atomic_load_32((_lock)) == TD_RWLATCH_WRITE_FLAG_COPY);  \
    CTG_LOCK_DEBUG("CTG WULOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    taosWUnLockLatch(_lock);                              \
    CTG_LOCK_DEBUG("CTG WULOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__); \
    assert(atomic_load_32((_lock)) >= 0);  \
  }                                                       \
} while (0)

  
#define CTG_ERR_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; return _code; } } while (0)
#define CTG_RET(c) do { int32_t _code = c; if (_code != TSDB_CODE_SUCCESS) { terrno = _code; } return _code; } while (0)
#define CTG_ERR_JRET(c) do { code = c; if (code != TSDB_CODE_SUCCESS) { terrno = code; goto _return; } } while (0)

#define CTG_API_LEAVE(c) do { int32_t __code = c; CTG_UNLOCK(CTG_READ, &gCtgMgmt.lock); CTG_API_DEBUG("CTG API leave %s", __FUNCTION__); CTG_RET(__code); } while (0)
#define CTG_API_ENTER() do { CTG_API_DEBUG("CTG API enter %s", __FUNCTION__); CTG_LOCK(CTG_READ, &gCtgMgmt.lock); if (atomic_load_8((int8_t*)&gCtgMgmt.exit)) { CTG_API_LEAVE(TSDB_CODE_CTG_OUT_OF_SERVICE); }  } while (0)



#ifdef __cplusplus
}
#endif

#endif /*_TD_CATALOG_INT_H_*/
