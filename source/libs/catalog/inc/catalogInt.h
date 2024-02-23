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
#include "query.h"
#include "tcommon.h"
#include "ttimer.h"
#include "tglobal.h"

#define CTG_DEFAULT_CACHE_CLUSTER_NUMBER 6
#define CTG_DEFAULT_CACHE_VGROUP_NUMBER  100
#define CTG_DEFAULT_CACHE_DB_NUMBER      20
#define CTG_DEFAULT_CACHE_TBLMETA_NUMBER 1000
#define CTG_DEFAULT_CACHE_VIEW_NUMBER    256
#define CTG_DEFAULT_RENT_SECOND          10
#define CTG_DEFAULT_RENT_SLOT_SIZE       10
#define CTG_DEFAULT_MAX_RETRY_TIMES      3
#define CTG_DEFAULT_BATCH_NUM            64
#define CTG_DEFAULT_FETCH_NUM            8
#define CTG_MAX_COMMAND_LEN              512
#define CTG_DEFAULT_CACHE_MON_MSEC       5000
#define CTG_CLEAR_CACHE_ROUND_TB_NUM     3000  

#define CTG_RENT_SLOT_SECOND 1.5

#define CTG_DEFAULT_INVALID_VERSION (-1)

#define CTG_ERR_CODE_TABLE_NOT_EXIST TSDB_CODE_PAR_TABLE_NOT_EXIST

#define CTG_BATCH_FETCH 1

typedef enum {
  CTG_CI_CLUSTER = 0,
  CTG_CI_DNODE,
  CTG_CI_QNODE,
  CTG_CI_DB,
  CTG_CI_DB_VGROUP,
  CTG_CI_DB_CFG,
  CTG_CI_DB_INFO,
  CTG_CI_STABLE_META,
  CTG_CI_NTABLE_META,
  CTG_CI_CTABLE_META,
  CTG_CI_SYSTABLE_META,
  CTG_CI_OTHERTABLE_META,
  CTG_CI_TBL_SMA,
  CTG_CI_TBL_CFG,
  CTG_CI_TBL_TAG,
  CTG_CI_INDEX_INFO,
  CTG_CI_USER,
  CTG_CI_UDF,
  CTG_CI_SVR_VER,
  CTG_CI_VIEW,
  CTG_CI_MAX_VALUE,
} CTG_CACHE_ITEM;

#define CTG_CI_FLAG_LEVEL_GLOBAL  (1)
#define CTG_CI_FLAG_LEVEL_CLUSTER (1 << 1)
#define CTG_CI_FLAG_LEVEL_DB      (1 << 2)

enum {
  CTG_READ = 1,
  CTG_WRITE,
};

enum {
  CTG_RENT_DB = 1,
  CTG_RENT_STABLE,
  CTG_RENT_VIEW,
};

enum {
  CTG_OP_UPDATE_VGROUP = 0,
  CTG_OP_UPDATE_DB_CFG,
  CTG_OP_UPDATE_TB_META,
  CTG_OP_DROP_DB_CACHE,
  CTG_OP_DROP_DB_VGROUP,
  CTG_OP_DROP_STB_META,
  CTG_OP_DROP_TB_META,
  CTG_OP_UPDATE_USER,
  CTG_OP_UPDATE_VG_EPSET,
  CTG_OP_UPDATE_TB_INDEX,
  CTG_OP_DROP_TB_INDEX,
  CTG_OP_UPDATE_VIEW_META,
  CTG_OP_DROP_VIEW_META,
  CTG_OP_CLEAR_CACHE,
  CTG_OP_MAX
};

typedef enum {
  CTG_TASK_GET_QNODE = 0,
  CTG_TASK_GET_DNODE,
  CTG_TASK_GET_DB_VGROUP,
  CTG_TASK_GET_DB_CFG,
  CTG_TASK_GET_DB_INFO,
  CTG_TASK_GET_TB_META,
  CTG_TASK_GET_TB_HASH,
  CTG_TASK_GET_TB_SMA_INDEX,
  CTG_TASK_GET_TB_CFG,
  CTG_TASK_GET_INDEX_INFO,
  CTG_TASK_GET_UDF,
  CTG_TASK_GET_USER,
  CTG_TASK_GET_SVR_VER,
  CTG_TASK_GET_TB_META_BATCH,
  CTG_TASK_GET_TB_HASH_BATCH,
  CTG_TASK_GET_TB_TAG,
  CTG_TASK_GET_VIEW,
} CTG_TASK_TYPE;

typedef enum {
  CTG_TASK_LAUNCHED = 1,
  CTG_TASK_DONE,
} CTG_TASK_STATUS;

typedef struct SCtgDebug {
  bool     lockEnable;
  bool     cacheEnable;
  bool     apiEnable;
  bool     metaEnable;
  bool     statEnable;
  uint32_t showCachePeriodSec;
} SCtgDebug;

typedef struct SCtgCacheStat {
  uint64_t cacheNum[CTG_CI_MAX_VALUE];
  uint64_t cacheSize[CTG_CI_MAX_VALUE];
  uint64_t cacheHit[CTG_CI_MAX_VALUE];
  uint64_t cacheNHit[CTG_CI_MAX_VALUE];
} SCtgCacheStat;

typedef struct SCtgAuthReq {
  SRequestConnInfo* pConn;
  SUserAuthInfo*    pRawReq;
  SGetUserAuthRsp   authInfo;
  AUTH_TYPE         singleType;
  bool              onlyCache;
  bool              tbNotExists;
} SCtgAuthReq;

typedef struct SCtgAuthRsp {
  SUserAuthRes* pRawRes;
  bool          metaNotExists;
} SCtgAuthRsp;

typedef struct SCtgTbCacheInfo {
  bool     inCache;
  uint64_t dbId;
  uint64_t suid;
  int32_t  tbType;
} SCtgTbCacheInfo;

typedef struct SCtgTbMetaParam {
  SName*  pName;
  int32_t flag;
} SCtgTbMetaParam;

typedef struct SCtgTbMetaCtx {
  SCtgTbCacheInfo tbInfo;
  int32_t         vgId;
  SName*          pName;
  int32_t         flag;
} SCtgTbMetaCtx;

typedef struct SCtgFetch {
  int32_t         dbIdx;
  int32_t         tbIdx;
  int32_t         fetchIdx;
  int32_t         resIdx;
  int32_t         flag;
  SCtgTbCacheInfo tbInfo;
  int32_t         vgId;
} SCtgFetch;

typedef struct SCtgTbMetasCtx {
  int32_t fetchNum;
  SArray* pNames;
  SArray* pResList;
  SArray* pFetchs;
} SCtgTbMetasCtx;

typedef struct SCtgTbIndexCtx {
  SName* pName;
} SCtgTbIndexCtx;

typedef struct SCtgTbCfgCtx {
  SName*       pName;
  int32_t      tbType;
  SVgroupInfo* pVgInfo;
} SCtgTbCfgCtx;

typedef struct SCtgTbTagCtx {
  SName*       pName;
  SVgroupInfo* pVgInfo;
} SCtgTbTagCtx;

typedef struct SCtgDbVgCtx {
  char dbFName[TSDB_DB_FNAME_LEN];
} SCtgDbVgCtx;

typedef struct SCtgDbCfgCtx {
  char dbFName[TSDB_DB_FNAME_LEN];
} SCtgDbCfgCtx;

typedef struct SCtgDbInfoCtx {
  char dbFName[TSDB_DB_FNAME_LEN];
} SCtgDbInfoCtx;

typedef struct SCtgTbHashCtx {
  char   dbFName[TSDB_DB_FNAME_LEN];
  SName* pName;
} SCtgTbHashCtx;

typedef struct SCtgTbHashsCtx {
  int32_t fetchNum;
  SArray* pNames;
  SArray* pResList;
  SArray* pFetchs;
} SCtgTbHashsCtx;

typedef struct SCtgIndexCtx {
  char indexFName[TSDB_INDEX_FNAME_LEN];
} SCtgIndexCtx;

typedef struct SCtgUdfCtx {
  char udfName[TSDB_FUNC_NAME_LEN];
} SCtgUdfCtx;

typedef struct SCtgUserCtx {
  SUserAuthInfo user;
  int32_t       subTaskCode;
} SCtgUserCtx;

typedef struct SCtgViewsCtx {
  int32_t fetchNum;
  SArray* pNames;
  SArray* pResList;
  SArray* pFetchs;
} SCtgViewsCtx;


typedef STableIndexRsp STableIndex;

typedef struct SCtgTbCache {
  SRWLatch     metaLock;
  SRWLatch     indexLock;
  STableMeta*  pMeta;
  STableIndex* pIndex;
} SCtgTbCache;

typedef struct SCtgVgCache {
  SRWLatch   vgLock;
  SDBVgInfo* vgInfo;
} SCtgVgCache;

typedef struct SCtgCfgCache {
  SRWLatch    cfgLock;
  SDbCfgInfo* cfgInfo;
} SCtgCfgCache;

typedef struct SCtgViewCache {
  SRWLatch    viewLock;
  SViewMeta*  pMeta;
} SCtgViewCache;


typedef struct SCtgDBCache {
  SRWLatch     dbLock;  // RC between destroy tbCache/stbCache and all reads
  uint64_t     dbId;
  int8_t       deleted;
  SCtgVgCache  vgCache;
  SCtgCfgCache cfgCache;
  SHashObj*    viewCache; // key:viewname, value:SCtgViewCache
  SHashObj*    tbCache;   // key:tbname, value:SCtgTbCache
  SHashObj*    stbCache;  // key:suid, value:char*
  uint64_t     dbCacheNum[CTG_CI_MAX_VALUE];
  uint64_t     dbCacheSize;
} SCtgDBCache;

typedef struct SCtgRentSlot {
  SRWLatch lock;
  bool     needSort;
  SArray*  meta;  // element is SDbCacheInfo or SSTableVersion
} SCtgRentSlot;

typedef struct SCtgRentMgmt {
  int8_t        type;
  uint16_t      slotNum;
  uint16_t      slotRIdx;
  int64_t       lastReadMsec;
  uint64_t      rentCacheSize;
  int32_t       metaSize;
  SCtgRentSlot* slots;
} SCtgRentMgmt;

typedef struct SCtgUserAuth {
  SRWLatch        lock;
  SGetUserAuthRsp userAuth;
  uint64_t        userCacheSize;
} SCtgUserAuth;

typedef struct SCatalog {
  uint64_t        clusterId;
  bool            stopUpdate;
  SDynViewVersion dynViewVer;
  SHashObj*       userCache;  // key:user, value:SCtgUserAuth
  SHashObj*       dbCache;    // key:dbname, value:SCtgDBCache
  SCtgRentMgmt    dbRent;
  SCtgRentMgmt    stbRent;
  SCtgRentMgmt    viewRent;
  SCtgCacheStat   cacheStat;
} SCatalog;

typedef struct SCtgBatch {
  int32_t          batchId;
  int32_t          msgType;
  SArray*          pMsgs;
  SRequestConnInfo conn;
  char             dbFName[TSDB_DB_FNAME_LEN];
  SArray*          pTaskIds;
  SArray*          pMsgIdxs;
} SCtgBatch;

typedef struct SCtgJob {
  int64_t   refId;
  int32_t   batchId;
  SHashObj* pBatchs;
  SArray*   pTasks;
  int32_t   subTaskNum;
  int32_t   taskDone;
  SMetaData jobRes;
  int32_t   jobResCode;
  int32_t   taskIdx;
  SRWLatch  taskLock;

  uint64_t         queryId;
  SCatalog*        pCtg;
  SRequestConnInfo conn;
  void*            userParam;
  catalogCallback  userFp;
  int32_t          tbMetaNum;
  int32_t          tbHashNum;
  int32_t          tbTagNum;
  int32_t          dbVgNum;
  int32_t          udfNum;
  int32_t          qnodeNum;
  int32_t          dnodeNum;
  int32_t          dbCfgNum;
  int32_t          indexNum;
  int32_t          userNum;
  int32_t          dbInfoNum;
  int32_t          tbIndexNum;
  int32_t          tbCfgNum;
  int32_t          svrVerNum;
  int32_t          viewNum;
} SCtgJob;

typedef struct SCtgMsgCtx {
  int32_t   reqType;
  void*     lastOut;
  void*     out;
  char*     target;
  SHashObj* pBatchs;
} SCtgMsgCtx;

typedef struct SCtgTaskCallbackParam {
  uint64_t queryId;
  int64_t  refId;
  SArray*  taskId;
  int32_t  reqType;
  int32_t  batchId;
  SArray*  msgIdx;
} SCtgTaskCallbackParam;

typedef struct SCtgTask SCtgTask;
typedef int32_t (*ctgSubTaskCbFp)(SCtgTask*);

typedef struct SCtgSubRes {
  CTG_TASK_TYPE  type;
  int32_t        code;
  void*          res;
  ctgSubTaskCbFp fp;
} SCtgSubRes;

struct SCtgTask {
  CTG_TASK_TYPE   type;
  bool            subTask;
  int32_t         taskId;
  SCtgJob*        pJob;
  void*           taskCtx;
  SArray*         msgCtxs;
  SCtgMsgCtx      msgCtx;
  int32_t         code;
  void*           res;
  CTG_TASK_STATUS status;
  SRWLatch        lock;
  SArray*         pParents;
  SCtgSubRes      subRes;
};

typedef struct SCtgTaskReq {
  SCtgTask* pTask;
  int32_t   msgIdx;
} SCtgTaskReq;

typedef int32_t (*ctgInitTaskFp)(SCtgJob*, int32_t, void*);
typedef int32_t (*ctgLanchTaskFp)(SCtgTask*);
typedef int32_t (*ctgHandleTaskMsgRspFp)(SCtgTaskReq*, int32_t, const SDataBuf*, int32_t);
typedef int32_t (*ctgDumpTaskResFp)(SCtgTask*);
typedef int32_t (*ctgCloneTaskResFp)(SCtgTask*, void**);
typedef int32_t (*ctgCompTaskFp)(SCtgTask*, void*, bool*);

typedef struct SCtgAsyncFps {
  ctgInitTaskFp         initFp;
  ctgLanchTaskFp        launchFp;
  ctgHandleTaskMsgRspFp handleRspFp;
  ctgDumpTaskResFp      dumpResFp;
  ctgCompTaskFp         compFp;
  ctgCloneTaskResFp     cloneFp;
} SCtgAsyncFps;

typedef struct SCtgApiStat {
#if defined(WINDOWS) || defined(_TD_DARWIN_64)
  size_t avoidCompilationErrors;
#endif

} SCtgApiStat;

typedef struct SCtgRuntimeStat {
  uint64_t numOfOpAbort;
  uint64_t numOfOpEnqueue;
  uint64_t numOfOpDequeue;
  uint64_t numOfOpClearMeta;
  uint64_t numOfOpClearCache;
} SCtgRuntimeStat;

typedef struct SCatalogStat {
  SCtgApiStat     api;
  SCtgRuntimeStat runtime;
  SCtgCacheStat   cache;
} SCatalogStat;

typedef struct SCtgUpdateMsgHeader {
  SCatalog* pCtg;
} SCtgUpdateMsgHeader;

typedef struct SCtgUpdateVgMsg {
  SCatalog*  pCtg;
  char       dbFName[TSDB_DB_FNAME_LEN];
  uint64_t   dbId;
  SDBVgInfo* dbInfo;
} SCtgUpdateVgMsg;

typedef struct SCtgUpdateDbCfgMsg {
  SCatalog*   pCtg;
  char        dbFName[TSDB_DB_FNAME_LEN];
  uint64_t    dbId;
  SDbCfgInfo* cfgInfo;
} SCtgUpdateDbCfgMsg;

typedef struct SCtgUpdateTbMetaMsg {
  SCatalog*         pCtg;
  STableMetaOutput* pMeta;
} SCtgUpdateTbMetaMsg;

typedef struct SCtgDropDBMsg {
  SCatalog* pCtg;
  char      dbFName[TSDB_DB_FNAME_LEN];
  uint64_t  dbId;
} SCtgDropDBMsg;

typedef struct SCtgDropDbVgroupMsg {
  SCatalog* pCtg;
  char      dbFName[TSDB_DB_FNAME_LEN];
} SCtgDropDbVgroupMsg;

typedef struct SCtgDropStbMetaMsg {
  SCatalog* pCtg;
  char      dbFName[TSDB_DB_FNAME_LEN];
  char      stbName[TSDB_TABLE_NAME_LEN];
  uint64_t  dbId;
  uint64_t  suid;
} SCtgDropStbMetaMsg;

typedef struct SCtgDropTblMetaMsg {
  SCatalog* pCtg;
  char      dbFName[TSDB_DB_FNAME_LEN];
  char      tbName[TSDB_TABLE_NAME_LEN];
  uint64_t  dbId;
} SCtgDropTblMetaMsg;

typedef struct SCtgUpdateUserMsg {
  SCatalog*       pCtg;
  SGetUserAuthRsp userAuth;
} SCtgUpdateUserMsg;

typedef struct SCtgUpdateTbIndexMsg {
  SCatalog*    pCtg;
  STableIndex* pIndex;
} SCtgUpdateTbIndexMsg;

typedef struct SCtgDropTbIndexMsg {
  SCatalog* pCtg;
  char      dbFName[TSDB_DB_FNAME_LEN];
  char      tbName[TSDB_TABLE_NAME_LEN];
} SCtgDropTbIndexMsg;

typedef struct SCtgClearCacheMsg {
  SCatalog* pCtg;
  bool      clearMeta;
  bool      freeCtg;
} SCtgClearCacheMsg;

typedef struct SCtgUpdateEpsetMsg {
  SCatalog* pCtg;
  char      dbFName[TSDB_DB_FNAME_LEN];
  int32_t   vgId;
  SEpSet    epSet;
} SCtgUpdateEpsetMsg;

typedef struct SCtgUpdateViewMetaMsg {
  SCatalog*         pCtg;
  SViewMetaRsp*     pRsp;
} SCtgUpdateViewMetaMsg;

typedef struct SCtgDropViewMetaMsg {
  SCatalog* pCtg;
  char      dbFName[TSDB_DB_FNAME_LEN];
  char      viewName[TSDB_VIEW_NAME_LEN];
  uint64_t  dbId;
  uint64_t  viewId;
} SCtgDropViewMetaMsg;


typedef struct SCtgCacheOperation {
  int32_t opId;
  void*   data;
  bool    syncOp;
  tsem_t  rspSem;
  bool    stopQueue;
  bool    unLocked;
} SCtgCacheOperation;

typedef struct SCtgQNode {
  SCtgCacheOperation* op;
  struct SCtgQNode*   next;
} SCtgQNode;

typedef struct SCtgQueue {
  SRWLatch   qlock;
  bool       stopQueue;
  SCtgQNode* head;
  SCtgQNode* tail;
  tsem_t     reqSem;
  uint64_t   qRemainNum;
} SCtgQueue;

typedef struct SCatalogMgmt {
  bool         exit;
  int32_t      jobPool;
  SRWLatch     lock;
  SCtgQueue    queue;
  void        *timer;
  tmr_h        cacheTimer;
  TdThread     updateThread;
  SHashObj*    pCluster;  // key: clusterId, value: SCatalog*
  SCatalogStat statInfo;
  SCatalogCfg  cfg;
} SCatalogMgmt;

typedef uint32_t (*tableNameHashFp)(const char*, uint32_t);
typedef int32_t (*ctgOpFunc)(SCtgCacheOperation*);

typedef struct SCtgOperation {
  int32_t   opId;
  char      name[32];
  ctgOpFunc func;
} SCtgOperation;

typedef struct SCtgCacheItemInfo {
  char*            name;
  int32_t          flag;
} SCtgCacheItemInfo;

#define CTG_AUTH_READ(_t)  ((_t) == AUTH_TYPE_READ || (_t) == AUTH_TYPE_READ_OR_WRITE)
#define CTG_AUTH_WRITE(_t) ((_t) == AUTH_TYPE_WRITE || (_t) == AUTH_TYPE_READ_OR_WRITE)

#define CTG_QUEUE_INC() atomic_add_fetch_64(&gCtgMgmt.queue.qRemainNum, 1)
#define CTG_QUEUE_DEC() atomic_sub_fetch_64(&gCtgMgmt.queue.qRemainNum, 1)

#define CTG_STAT_INC(_item, _n) atomic_add_fetch_64(&(_item), _n)
#define CTG_STAT_DEC(_item, _n) atomic_sub_fetch_64(&(_item), _n)
#define CTG_STAT_GET(_item)     atomic_load_64(&(_item))

#define CTG_STAT_API_INC(item, n)  (CTG_STAT_INC(gCtgMgmt.statInfo.api.item, n))
#define CTG_STAT_RT_INC(item, n)   (CTG_STAT_INC(gCtgMgmt.statInfo.runtime.item, n))
#define CTG_STAT_NUM_INC(item, n)  (CTG_STAT_INC(gCtgMgmt.statInfo.cache.cacheNum[item], n))
#define CTG_STAT_NUM_DEC(item, n)  (CTG_STAT_DEC(gCtgMgmt.statInfo.cache.cacheNum[item], n))
#define CTG_STAT_HIT_INC(item, n)  (CTG_STAT_INC(gCtgMgmt.statInfo.cache.cacheHit[item], n))
#define CTG_STAT_HIT_DEC(item, n)  (CTG_STAT_DEC(gCtgMgmt.statInfo.cache.cacheHit[item], n))
#define CTG_STAT_NHIT_INC(item, n) (CTG_STAT_INC(gCtgMgmt.statInfo.cache.cacheNHit[item], n))
#define CTG_STAT_NHIT_DEC(item, n) (CTG_STAT_DEC(gCtgMgmt.statInfo.cache.cacheNHit[item], n))

#define CTG_CACHE_NUM_INC(item, n)  (CTG_STAT_INC(pCtg->cacheStat.cacheNum[item], n))
#define CTG_CACHE_NUM_DEC(item, n)  (CTG_STAT_DEC(pCtg->cacheStat.cacheNum[item], n))
#define CTG_CACHE_HIT_INC(item, n)  (CTG_STAT_INC(pCtg->cacheStat.cacheHit[item], n))
#define CTG_CACHE_NHIT_INC(item, n) (CTG_STAT_INC(pCtg->cacheStat.cacheNHit[item], n))

#define CTG_DB_NUM_INC(_item)   dbCache->dbCacheNum[_item] += 1
#define CTG_DB_NUM_DEC(_item)   dbCache->dbCacheNum[_item] -= 1
#define CTG_DB_NUM_SET(_item)   dbCache->dbCacheNum[_item] = 1
#define CTG_DB_NUM_RESET(_item) dbCache->dbCacheNum[_item] = 0

#define CTG_META_NUM_INC(type)                  \
  do {                                          \
    switch (type) {                             \
      case TSDB_SUPER_TABLE:                    \
        CTG_DB_NUM_INC(CTG_CI_STABLE_META);     \
        break;                                  \
      case TSDB_CHILD_TABLE:                    \
        CTG_DB_NUM_INC(CTG_CI_CTABLE_META);     \
        break;                                  \
      case TSDB_NORMAL_TABLE:                   \
        CTG_DB_NUM_INC(CTG_CI_NTABLE_META);     \
        break;                                  \
      case TSDB_SYSTEM_TABLE:                   \
        CTG_DB_NUM_INC(CTG_CI_SYSTABLE_META);   \
        break;                                  \
      default:                                  \
        CTG_DB_NUM_INC(CTG_CI_OTHERTABLE_META); \
        break;                                  \
    }                                           \
  } while (0)

#define CTG_META_NUM_DEC(type)                  \
  do {                                          \
    switch (type) {                             \
      case TSDB_SUPER_TABLE:                    \
        CTG_DB_NUM_DEC(CTG_CI_STABLE_META);     \
        break;                                  \
      case TSDB_CHILD_TABLE:                    \
        CTG_DB_NUM_DEC(CTG_CI_CTABLE_META);     \
        break;                                  \
      case TSDB_NORMAL_TABLE:                   \
        CTG_DB_NUM_DEC(CTG_CI_NTABLE_META);     \
        break;                                  \
      case TSDB_SYSTEM_TABLE:                   \
        CTG_DB_NUM_DEC(CTG_CI_SYSTABLE_META);   \
        break;                                  \
      default:                                  \
        CTG_DB_NUM_DEC(CTG_CI_OTHERTABLE_META); \
        break;                                  \
    }                                           \
  } while (0)

#define CTG_META_HIT_INC(type)                        \
  do {                                                \
    switch (type) {                                   \
      case TSDB_SUPER_TABLE:                          \
        CTG_CACHE_HIT_INC(CTG_CI_STABLE_META, 1);     \
        break;                                        \
      case TSDB_CHILD_TABLE:                          \
        CTG_CACHE_HIT_INC(CTG_CI_CTABLE_META, 1);     \
        break;                                        \
      case TSDB_NORMAL_TABLE:                         \
        CTG_CACHE_HIT_INC(CTG_CI_NTABLE_META, 1);     \
        break;                                        \
      case TSDB_SYSTEM_TABLE:                         \
        CTG_CACHE_HIT_INC(CTG_CI_SYSTABLE_META, 1);   \
        break;                                        \
      default:                                        \
        CTG_CACHE_HIT_INC(CTG_CI_OTHERTABLE_META, 1); \
        break;                                        \
    }                                                 \
  } while (0)

#define CTG_META_NHIT_INC() CTG_CACHE_NHIT_INC(CTG_CI_OTHERTABLE_META, 1)

#define CTG_IS_META_NULL(type)   ((type) == META_TYPE_NULL_TABLE)
#define CTG_IS_META_CTABLE(type) ((type) == META_TYPE_CTABLE)
#define CTG_IS_META_TABLE(type)  ((type) == META_TYPE_TABLE)
#define CTG_IS_META_BOTH(type)   ((type) == META_TYPE_BOTH_TABLE)

#define CTG_FLAG_STB          0x1
#define CTG_FLAG_NOT_STB      0x2
#define CTG_FLAG_UNKNOWN_STB  0x4
#define CTG_FLAG_SYS_DB       0x8
#define CTG_FLAG_FORCE_UPDATE 0x10
#define CTG_FLAG_ONLY_CACHE   0x20
#define CTG_FLAG_SYNC_OP      0x40

#define CTG_FLAG_SET(_flag, _v) ((_flag) |= (_v))

#define CTG_FLAG_IS_STB(_flag)          ((_flag)&CTG_FLAG_STB)
#define CTG_FLAG_IS_NOT_STB(_flag)      ((_flag)&CTG_FLAG_NOT_STB)
#define CTG_FLAG_IS_UNKNOWN_STB(_flag)  ((_flag)&CTG_FLAG_UNKNOWN_STB)
#define CTG_FLAG_IS_SYS_DB(_flag)       ((_flag)&CTG_FLAG_SYS_DB)
#define CTG_FLAG_IS_FORCE_UPDATE(_flag) ((_flag)&CTG_FLAG_FORCE_UPDATE)
#define CTG_FLAG_SET_SYS_DB(_flag)      ((_flag) |= CTG_FLAG_SYS_DB)
#define CTG_FLAG_SET_STB(_flag, tbType)                                                       \
  do {                                                                                        \
    (_flag) |= ((tbType) == TSDB_SUPER_TABLE)                                                 \
                   ? CTG_FLAG_STB                                                             \
                   : ((tbType) > TSDB_SUPER_TABLE ? CTG_FLAG_NOT_STB : CTG_FLAG_UNKNOWN_STB); \
  } while (0)
#define CTG_FLAG_MAKE_STB(_isStb) \
  (((_isStb) == 1) ? CTG_FLAG_STB : ((_isStb) == 0 ? CTG_FLAG_NOT_STB : CTG_FLAG_UNKNOWN_STB))
#define CTG_FLAG_MATCH_STB(_flag, tbType)                                                        \
  (CTG_FLAG_IS_UNKNOWN_STB(_flag) || (CTG_FLAG_IS_STB(_flag) && (tbType) == TSDB_SUPER_TABLE) || \
   (CTG_FLAG_IS_NOT_STB(_flag) && (tbType) != TSDB_SUPER_TABLE))

#define CTG_IS_BATCH_TASK(_taskType) ((CTG_TASK_GET_TB_META_BATCH == (_taskType)) || (CTG_TASK_GET_TB_HASH_BATCH == (_taskType)) || (CTG_TASK_GET_VIEW == (_taskType)))

#define CTG_GET_TASK_MSGCTX(_task, _id)                                                             \
  (CTG_IS_BATCH_TASK((_task)->type)  ? taosArrayGet((_task)->msgCtxs, (_id)) : &(_task)->msgCtx)

#define CTG_META_SIZE(pMeta) \
  (sizeof(STableMeta) + ((pMeta)->tableInfo.numOfTags + (pMeta)->tableInfo.numOfColumns) * sizeof(SSchema))

#define CTG_TABLE_NOT_EXIST(code) (code == CTG_ERR_CODE_TABLE_NOT_EXIST)
#define CTG_DB_NOT_EXIST(code) \
  (code == TSDB_CODE_MND_DB_NOT_EXIST || code == TSDB_CODE_MND_DB_IN_CREATING || code == TSDB_CODE_MND_DB_IN_DROPPING)

#define CTG_CACHE_OVERFLOW(_csize, _maxsize) ((_maxsize >= 0) ? ((_csize) >= (_maxsize) * 1048576L * 0.9) : false)
#define CTG_CACHE_LOW(_csize, _maxsize) ((_maxsize >= 0) ? ((_csize) <= (_maxsize) * 1048576L * 0.75) : true)


#define ctgFatal(param, ...) qFatal("CTG:%p " param, pCtg, __VA_ARGS__)
#define ctgError(param, ...) qError("CTG:%p " param, pCtg, __VA_ARGS__)
#define ctgWarn(param, ...)  qWarn("CTG:%p " param, pCtg, __VA_ARGS__)
#define ctgInfo(param, ...)  qInfo("CTG:%p " param, pCtg, __VA_ARGS__)
#define ctgDebug(param, ...) qDebug("CTG:%p " param, pCtg, __VA_ARGS__)
#define ctgTrace(param, ...) qTrace("CTG:%p " param, pCtg, __VA_ARGS__)

#define ctgTaskFatal(param, ...) qFatal("QID:%" PRIx64 " CTG:%p " param, pTask->pJob->queryId, pCtg, __VA_ARGS__)
#define ctgTaskError(param, ...) qError("QID:%" PRIx64 " CTG:%p " param, pTask->pJob->queryId, pCtg, __VA_ARGS__)
#define ctgTaskWarn(param, ...)  qWarn("QID:%" PRIx64 " CTG:%p " param, pTask->pJob->queryId, pCtg, __VA_ARGS__)
#define ctgTaskInfo(param, ...)  qInfo("QID:%" PRIx64 " CTG:%p " param, pTask->pJob->queryId, pCtg, __VA_ARGS__)
#define ctgTaskDebug(param, ...) qDebug("QID:%" PRIx64 " CTG:%p " param, pTask->pJob->queryId, pCtg, __VA_ARGS__)
#define ctgTaskTrace(param, ...) qTrace("QID:%" PRIx64 " CTG:%p " param, pTask->pJob->queryId, pCtg, __VA_ARGS__)

#define CTG_LOCK_DEBUG(...)     \
  do {                          \
    if (gCTGDebug.lockEnable) { \
      qDebug(__VA_ARGS__);      \
    }                           \
  } while (0)
#define CTG_CACHE_DEBUG(...)     \
  do {                           \
    if (gCTGDebug.cacheEnable) { \
      qDebug(__VA_ARGS__);       \
    }                            \
  } while (0)
#define CTG_API_DEBUG(...)     \
  do {                         \
    if (gCTGDebug.apiEnable) { \
      qDebug(__VA_ARGS__);     \
    }                          \
  } while (0)

#define TD_RWLATCH_WRITE_FLAG_COPY 0x40000000

#define CTG_LOCK(type, _lock)                                                                                \
  do {                                                                                                       \
    if (CTG_READ == (type)) {                                                                                \
      ASSERTS(atomic_load_32((_lock)) >= 0, "invalid lock value before read lock");                          \
      CTG_LOCK_DEBUG("CTG RLOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);         \
      taosRLockLatch(_lock);                                                                                 \
      CTG_LOCK_DEBUG("CTG RLOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);         \
      ASSERTS(atomic_load_32((_lock)) > 0, "invalid lock value after read lock");                            \
    } else {                                                                                                 \
      ASSERTS(atomic_load_32((_lock)) >= 0, "invalid lock value before write lock");                         \
      CTG_LOCK_DEBUG("CTG WLOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);         \
      taosWLockLatch(_lock);                                                                                 \
      CTG_LOCK_DEBUG("CTG WLOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);         \
      ASSERTS(atomic_load_32((_lock)) == TD_RWLATCH_WRITE_FLAG_COPY, "invalid lock value after write lock"); \
    }                                                                                                        \
  } while (0)

#define CTG_UNLOCK(type, _lock)                                                                                 \
  do {                                                                                                          \
    if (CTG_READ == (type)) {                                                                                   \
      ASSERTS(atomic_load_32((_lock)) > 0, "invalid lock value before read unlock");                            \
      CTG_LOCK_DEBUG("CTG RULOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);           \
      taosRUnLockLatch(_lock);                                                                                  \
      CTG_LOCK_DEBUG("CTG RULOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);           \
      ASSERTS(atomic_load_32((_lock)) >= 0, "invalid lock value after read unlock");                            \
    } else {                                                                                                    \
      ASSERTS(atomic_load_32((_lock)) == TD_RWLATCH_WRITE_FLAG_COPY, "invalid lock value before write unlock"); \
      CTG_LOCK_DEBUG("CTG WULOCK%p:%d, %s:%d B", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);           \
      taosWUnLockLatch(_lock);                                                                                  \
      CTG_LOCK_DEBUG("CTG WULOCK%p:%d, %s:%d E", (_lock), atomic_load_32(_lock), __FILE__, __LINE__);           \
      ASSERTS(atomic_load_32((_lock)) >= 0, "invalid lock value after write unlock");                           \
    }                                                                                                           \
  } while (0)

#define CTG_ERR_RET(c)                \
  do {                                \
    int32_t _code = c;                \
    if (_code != TSDB_CODE_SUCCESS) { \
      terrno = _code;                 \
      return _code;                   \
    }                                 \
  } while (0)
#define CTG_RET(c)                    \
  do {                                \
    int32_t _code = c;                \
    if (_code != TSDB_CODE_SUCCESS) { \
      terrno = _code;                 \
    }                                 \
    return _code;                     \
  } while (0)
#define CTG_ERR_JRET(c)              \
  do {                               \
    code = c;                        \
    if (code != TSDB_CODE_SUCCESS) { \
      terrno = code;                 \
      goto _return;                  \
    }                                \
  } while (0)

#define CTG_API_LEAVE(c)                             \
  do {                                               \
    int32_t __code = c;                              \
    CTG_UNLOCK(CTG_READ, &gCtgMgmt.lock);            \
    CTG_API_DEBUG("CTG API leave %s", __FUNCTION__); \
    CTG_RET(__code);                                 \
  } while (0)

#define CTG_API_NLEAVE()                             \
  do {                                               \
    CTG_UNLOCK(CTG_READ, &gCtgMgmt.lock);            \
    CTG_API_DEBUG("CTG API leave %s", __FUNCTION__); \
    return;                                          \
  } while (0)  

#define CTG_API_ENTER()                              \
  do {                                               \
    CTG_API_DEBUG("CTG API enter %s", __FUNCTION__); \
    CTG_LOCK(CTG_READ, &gCtgMgmt.lock);              \
    if (atomic_load_8((int8_t*)&gCtgMgmt.exit)) {    \
      CTG_API_LEAVE(TSDB_CODE_CTG_OUT_OF_SERVICE);   \
    }                                                \
  } while (0)

#define CTG_API_NENTER()                             \
  do {                                               \
    CTG_API_DEBUG("CTG API enter %s", __FUNCTION__); \
    CTG_LOCK(CTG_READ, &gCtgMgmt.lock);              \
    if (atomic_load_8((int8_t*)&gCtgMgmt.exit)) {    \
      CTG_API_NLEAVE();                              \
    }                                                \
  } while (0)  

#define CTG_API_JENTER()                             \
  do {                                               \
    CTG_API_DEBUG("CTG API enter %s", __FUNCTION__); \
    CTG_LOCK(CTG_READ, &gCtgMgmt.lock);              \
    if (atomic_load_8((int8_t*)&gCtgMgmt.exit)) {    \
      CTG_ERR_JRET(TSDB_CODE_CTG_OUT_OF_SERVICE);    \
    }                                                \
  } while (0)

#define CTG_API_LEAVE_NOLOCK(c)                      \
  do {                                               \
    int32_t __code = c;                              \
    CTG_API_DEBUG("CTG API leave %s", __FUNCTION__); \
    CTG_RET(__code);                                 \
  } while (0)

#define CTG_API_ENTER_NOLOCK()                            \
  do {                                                    \
    CTG_API_DEBUG("CTG API enter %s", __FUNCTION__);      \
    if (atomic_load_8((int8_t*)&gCtgMgmt.exit)) {         \
      CTG_API_LEAVE_NOLOCK(TSDB_CODE_CTG_OUT_OF_SERVICE); \
    }                                                     \
  } while (0)

void    ctgdShowTableMeta(SCatalog* pCtg, const char* tbName, STableMeta* p);
void    ctgdShowClusterCache(SCatalog* pCtg);
int32_t ctgdShowCacheInfo(void);
int32_t ctgdShowStatInfo(void);

int32_t ctgRemoveTbMetaFromCache(SCatalog* pCtg, SName* pTableName, bool syncReq);
int32_t ctgGetTbMetaFromCache(SCatalog* pCtg, SCtgTbMetaCtx* ctx, STableMeta** pTableMeta);
int32_t ctgGetTbMetasFromCache(SCatalog* pCtg, SRequestConnInfo* pConn, SCtgTbMetasCtx* ctx, int32_t dbIdx,
                               int32_t* fetchIdx, int32_t baseResIdx, SArray* pList);
void*   ctgCloneDbCfgInfo(void* pSrc);

int32_t ctgOpUpdateVgroup(SCtgCacheOperation* action);
int32_t ctgOpUpdateDbCfg(SCtgCacheOperation *operation);
int32_t ctgOpUpdateTbMeta(SCtgCacheOperation* action);
int32_t ctgOpDropDbCache(SCtgCacheOperation* action);
int32_t ctgOpDropDbVgroup(SCtgCacheOperation* action);
int32_t ctgOpDropStbMeta(SCtgCacheOperation* action);
int32_t ctgOpDropTbMeta(SCtgCacheOperation* action);
int32_t ctgOpDropViewMeta(SCtgCacheOperation* action);
int32_t ctgOpUpdateUser(SCtgCacheOperation* action);
int32_t ctgOpUpdateEpset(SCtgCacheOperation* operation);
int32_t ctgAcquireVgInfoFromCache(SCatalog* pCtg, const char* dbFName, SCtgDBCache** pCache);
void    ctgReleaseDBCache(SCatalog* pCtg, SCtgDBCache* dbCache);
void    ctgRUnlockVgInfo(SCtgDBCache* dbCache);
int32_t ctgTbMetaExistInCache(SCatalog* pCtg, char* dbFName, char* tbName, int32_t* exist);
int32_t ctgReadTbMetaFromCache(SCatalog* pCtg, SCtgTbMetaCtx* ctx, STableMeta** pTableMeta);
int32_t ctgReadTbVerFromCache(SCatalog* pCtg, SName* pTableName, int32_t* sver, int32_t* tver, int32_t* tbType,
                              uint64_t* suid, char* stbName);
int32_t ctgChkAuthFromCache(SCatalog* pCtg, SUserAuthInfo* pReq, bool tbNotExists, bool* inCache, SCtgAuthRsp* pRes);
int32_t ctgDropDbCacheEnqueue(SCatalog* pCtg, const char* dbFName, int64_t dbId);
int32_t ctgDropDbVgroupEnqueue(SCatalog* pCtg, const char* dbFName, bool syncReq);
int32_t ctgDropStbMetaEnqueue(SCatalog* pCtg, const char* dbFName, int64_t dbId, const char* stbName, uint64_t suid,
                              bool syncReq);
int32_t ctgDropTbMetaEnqueue(SCatalog* pCtg, const char* dbFName, int64_t dbId, const char* tbName, bool syncReq);
int32_t ctgUpdateVgroupEnqueue(SCatalog* pCtg, const char* dbFName, int64_t dbId, SDBVgInfo* dbInfo, bool syncReq);
int32_t ctgUpdateDbCfgEnqueue(SCatalog *pCtg, const char *dbFName, int64_t dbId, SDbCfgInfo *cfgInfo, bool syncOp);
int32_t ctgUpdateTbMetaEnqueue(SCatalog* pCtg, STableMetaOutput* output, bool syncReq);
int32_t ctgUpdateUserEnqueue(SCatalog* pCtg, SGetUserAuthRsp* pAuth, bool syncReq);
int32_t ctgUpdateVgEpsetEnqueue(SCatalog* pCtg, char* dbFName, int32_t vgId, SEpSet* pEpSet);
int32_t ctgUpdateTbIndexEnqueue(SCatalog* pCtg, STableIndex** pIndex, bool syncOp);
int32_t ctgDropViewMetaEnqueue(SCatalog *pCtg, const char *dbFName, uint64_t dbId, const char *viewName, uint64_t viewId, bool syncOp);
int32_t ctgClearCacheEnqueue(SCatalog* pCtg, bool clearMeta, bool freeCtg, bool stopQueue, bool syncOp);
int32_t ctgMetaRentInit(SCtgRentMgmt* mgmt, uint32_t rentSec, int8_t type, int32_t size);
int32_t ctgMetaRentAdd(SCtgRentMgmt* mgmt, void* meta, int64_t id, int32_t size);
int32_t ctgMetaRentUpdate(SCtgRentMgmt *mgmt, void *meta, int64_t id, int32_t size, __compar_fn_t sortCompare,
                          __compar_fn_t searchCompare);
int32_t ctgMetaRentGet(SCtgRentMgmt* mgmt, void** res, uint32_t* num, int32_t size);
int32_t ctgMetaRentRemove(SCtgRentMgmt *mgmt, int64_t id, __compar_fn_t sortCompare, __compar_fn_t searchCompare);
void    ctgRemoveStbRent(SCatalog *pCtg, SCtgDBCache *dbCache);
void    ctgRemoveViewRent(SCatalog *pCtg, SCtgDBCache *dbCache);
int32_t ctgUpdateRentStbVersion(SCatalog *pCtg, char *dbFName, char *tbName, uint64_t dbId, uint64_t suid,
                                SCtgTbCache *pCache);
int32_t ctgUpdateRentViewVersion(SCatalog *pCtg, char *dbFName, char *viewName, uint64_t dbId, uint64_t viewId,
                                SCtgViewCache *pCache);                                
int32_t ctgUpdateTbMetaToCache(SCatalog* pCtg, STableMetaOutput* pOut, bool syncReq);
int32_t ctgUpdateViewMetaToCache(SCatalog *pCtg, SViewMetaRsp *pRsp, bool syncReq);
int32_t ctgStartUpdateThread();
int32_t ctgRelaunchGetTbMetaTask(SCtgTask* pTask);
void    ctgReleaseVgInfoToCache(SCatalog* pCtg, SCtgDBCache* dbCache);
int32_t ctgReadTbIndexFromCache(SCatalog* pCtg, SName* pTableName, SArray** pRes);
int32_t ctgDropTbIndexEnqueue(SCatalog* pCtg, SName* pName, bool syncOp);
int32_t ctgOpDropTbIndex(SCtgCacheOperation* operation);
int32_t ctgOpUpdateTbIndex(SCtgCacheOperation* operation);
int32_t ctgOpClearCache(SCtgCacheOperation* operation);
int32_t ctgOpUpdateViewMeta(SCtgCacheOperation *operation);
int32_t ctgReadTbTypeFromCache(SCatalog* pCtg, char* dbFName, char* tableName, int32_t* tbType);
int32_t ctgGetTbHashVgroupFromCache(SCatalog* pCtg, const SName* pTableName, SVgroupInfo** pVgroup);
int32_t ctgGetViewsFromCache(SCatalog *pCtg, SRequestConnInfo *pConn, SCtgViewsCtx *ctx, int32_t dbIdx,
                               int32_t *fetchIdx, int32_t baseResIdx, SArray *pList);
int32_t ctgProcessRspMsg(void* out, int32_t reqType, char* msg, int32_t msgSize, int32_t rspCode, char* target);
int32_t ctgGetDBVgInfoFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, SBuildUseDBInput* input, SUseDbOutput* out,
                                SCtgTaskReq* tReq);
int32_t ctgGetQnodeListFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, SArray* out, SCtgTask* pTask);
int32_t ctgGetDnodeListFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, SArray** out, SCtgTask* pTask);
int32_t ctgGetDBCfgFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, const char* dbFName, SDbCfgInfo* out,
                             SCtgTask* pTask);
int32_t ctgGetIndexInfoFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, const char* indexName, SIndexInfo* out,
                                 SCtgTask* pTask);
int32_t ctgGetTbIndexFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, SName* name, STableIndex* out, SCtgTask* pTask);
int32_t ctgGetUdfInfoFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, const char* funcName, SFuncInfo* out,
                               SCtgTask* pTask);
int32_t ctgGetUserDbAuthFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, const char* user, SGetUserAuthRsp* out,
                                  SCtgTask* pTask);
int32_t ctgGetTbMetaFromMnodeImpl(SCatalog* pCtg, SRequestConnInfo* pConn, char* dbFName, char* tbName,
                                  STableMetaOutput* out, SCtgTaskReq* tReq);
int32_t ctgGetTbMetaFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName, STableMetaOutput* out,
                              SCtgTaskReq* tReq);
int32_t ctgGetTbMetaFromVnode(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName, SVgroupInfo* vgroupInfo,
                              STableMetaOutput* out, SCtgTaskReq* tReq);
int32_t ctgGetTableCfgFromVnode(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName,
                                SVgroupInfo* vgroupInfo, STableCfg** out, SCtgTask* pTask);
int32_t ctgGetTableCfgFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName, STableCfg** out,
                                SCtgTask* pTask);
int32_t ctgGetSvrVerFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, char** out, SCtgTask* pTask);
int32_t ctgGetViewInfoFromMnode(SCatalog* pCtg, SRequestConnInfo* pConn, SName* pName, SViewMetaOutput* out,
                                SCtgTaskReq* tReq);
int32_t ctgLaunchBatchs(SCatalog* pCtg, SCtgJob* pJob, SHashObj* pBatchs);

int32_t ctgInitJob(SCatalog* pCtg, SRequestConnInfo* pConn, SCtgJob** job, const SCatalogReq* pReq, catalogCallback fp,
                   void* param);
int32_t ctgLaunchJob(SCtgJob* pJob);
int32_t ctgMakeAsyncRes(SCtgJob* pJob);
int32_t ctgLaunchSubTask(SCtgTask** ppTask, CTG_TASK_TYPE type, ctgSubTaskCbFp fp, void* param);
int32_t ctgGetTbCfgCb(SCtgTask* pTask);
void    ctgFreeHandle(SCatalog* pCatalog);

void    ctgFreeSViewMeta(SViewMeta* pMeta);
void    ctgFreeMsgSendParam(void* param);
void    ctgFreeBatch(SCtgBatch* pBatch);
void    ctgFreeBatchs(SHashObj* pBatchs);
int32_t ctgCloneVgInfo(SDBVgInfo* src, SDBVgInfo** dst);
int32_t ctgCloneMetaOutput(STableMetaOutput* output, STableMetaOutput** pOutput);
int32_t ctgGenerateVgList(SCatalog* pCtg, SHashObj* vgHash, SArray** pList);
void    ctgFreeJob(void* job);
void    ctgFreeHandleImpl(SCatalog* pCtg);
int32_t ctgGetVgInfoFromHashValue(SCatalog* pCtg, SEpSet* pMgmtEps, SDBVgInfo* dbInfo, const SName* pTableName, SVgroupInfo* pVgroup);
int32_t ctgGetVgInfosFromHashValue(SCatalog* pCtg, SEpSet* pMgmgEpSet, SCtgTaskReq* tReq, SDBVgInfo* dbInfo, SCtgTbHashsCtx* pCtx,
                                   char* dbFName, SArray* pNames, bool update);
int32_t ctgGetVgIdsFromHashValue(SCatalog* pCtg, SDBVgInfo* dbInfo, char* dbFName, const char* pTbs[], int32_t tbNum,
                                 int32_t* vgId);
void    ctgResetTbMetaTask(SCtgTask* pTask);
void    ctgFreeDbCache(SCtgDBCache* dbCache);
int32_t ctgStbVersionSortCompare(const void* key1, const void* key2);
int32_t ctgViewVersionSortCompare(const void* key1, const void* key2);
int32_t ctgDbCacheInfoSortCompare(const void* key1, const void* key2);
int32_t ctgStbVersionSearchCompare(const void* key1, const void* key2);
int32_t ctgDbCacheInfoSearchCompare(const void* key1, const void* key2);
int32_t ctgViewVersionSearchCompare(const void* key1, const void* key2);
void    ctgFreeSTableMetaOutput(STableMetaOutput* pOutput);
int32_t ctgUpdateMsgCtx(SCtgMsgCtx* pCtx, int32_t reqType, void* out, char* target);
int32_t ctgAddMsgCtx(SArray* pCtxs, int32_t reqType, void* out, char* target);
char*   ctgTaskTypeStr(CTG_TASK_TYPE type);
int32_t ctgUpdateSendTargetInfo(SMsgSendInfo* pMsgSendInfo, int32_t msgType, char* dbFName, int32_t vgId);
int32_t ctgGetTablesReqNum(SArray* pList);
int32_t ctgAddFetch(SArray** pFetchs, int32_t dbIdx, int32_t tbIdx, int32_t* fetchIdx, int32_t resIdx, int32_t flag);
int32_t ctgCloneTableIndex(SArray* pIndex, SArray** pRes);
void    ctgFreeSTableIndex(void* info);
void    ctgClearSubTaskRes(SCtgSubRes* pRes);
void    ctgFreeQNode(SCtgQNode* node);
void    ctgClearHandle(SCatalog* pCtg);
void    ctgFreeTbCacheImpl(SCtgTbCache* pCache, bool lock);
void    ctgFreeViewCacheImpl(SCtgViewCache* pCache, bool lock);
int32_t ctgRemoveTbMeta(SCatalog* pCtg, SName* pTableName);
int32_t ctgRemoveCacheUser(SCatalog* pCtg, SCtgUserAuth* pUser, const char* user);
int32_t ctgGetTbHashVgroup(SCatalog* pCtg, SRequestConnInfo* pConn, const SName* pTableName, SVgroupInfo* pVgroup,
                           bool* exists);
SName*  ctgGetFetchName(SArray* pNames, SCtgFetch* pFetch);
int32_t ctgdGetOneHandle(SCatalog** pHandle);
int     ctgVgInfoComp(const void* lp, const void* rp);
int32_t ctgMakeVgArray(SDBVgInfo* dbInfo);
int32_t ctgChkSetAuthRes(SCatalog *pCtg, SCtgAuthReq *req, SCtgAuthRsp* res);
int32_t ctgReadDBCfgFromCache(SCatalog *pCtg, const char* dbFName, SDbCfgInfo* pDbCfg);

int32_t ctgAcquireVgMetaFromCache(SCatalog* pCtg, const char* dbFName, const char* tbName, SCtgDBCache** pDb,
                                  SCtgTbCache** pTb);
int32_t ctgCopyTbMeta(SCatalog* pCtg, SCtgTbMetaCtx* ctx, SCtgDBCache** pDb, SCtgTbCache** pTb, STableMeta** pTableMeta,
                      char* dbFName);
void    ctgReleaseVgMetaToCache(SCatalog* pCtg, SCtgDBCache* dbCache, SCtgTbCache* pCache);
void    ctgReleaseTbMetaToCache(SCatalog* pCtg, SCtgDBCache* dbCache, SCtgTbCache* pCache);
void    ctgGetGlobalCacheStat(SCtgCacheStat* pStat);
int32_t ctgChkSetAuthRes(SCatalog* pCtg, SCtgAuthReq* req, SCtgAuthRsp* res);
int32_t ctgBuildViewNullRes(SCtgTask* pTask, SCtgViewsCtx* pCtx);
int32_t dupViewMetaFromRsp(SViewMetaRsp* pRsp, SViewMeta* pViewMeta);
void    ctgDestroySMetaData(SMetaData* pData);
void    ctgGetGlobalCacheSize(uint64_t *pSize);
uint64_t ctgGetTbIndexCacheSize(STableIndex *pIndex);
uint64_t ctgGetViewMetaCacheSize(SViewMeta *pMeta);
uint64_t ctgGetTbMetaCacheSize(STableMeta *pMeta);
uint64_t ctgGetDbVgroupCacheSize(SDBVgInfo *pVg);
uint64_t ctgGetUserCacheSize(SGetUserAuthRsp *pAuth);
uint64_t ctgGetClusterCacheSize(SCatalog *pCtg);
void     ctgClearHandleMeta(SCatalog* pCtg, int64_t *pClearedSize, int64_t *pCleardNum, bool *roundDone);
void     ctgClearAllHandleMeta(int64_t *clearedSize, int64_t *clearedNum, bool *roundDone);
void     ctgProcessTimerEvent(void *param, void *tmrId);

int32_t ctgGetTbMeta(SCatalog* pCtg, SRequestConnInfo* pConn, SCtgTbMetaCtx* ctx, STableMeta** pTableMeta);
int32_t ctgGetCachedStbNameFromSuid(SCatalog* pCtg, char* dbFName, uint64_t suid, char **stbName);
int32_t ctgGetTbTagCb(SCtgTask* pTask);
int32_t ctgGetUserCb(SCtgTask* pTask);

extern SCatalogMgmt      gCtgMgmt;
extern SCtgDebug         gCTGDebug;
extern SCtgAsyncFps      gCtgAsyncFps[];
extern SCtgCacheItemInfo gCtgStatItem[CTG_CI_MAX_VALUE];

#ifdef __cplusplus
}
#endif

#endif /*_TD_CATALOG_INT_H_*/
