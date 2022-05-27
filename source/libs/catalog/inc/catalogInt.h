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

#define CTG_ERR_CODE_TABLE_NOT_EXIST TSDB_CODE_PAR_TABLE_NOT_EXIST

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
  CTG_ACT_UPDATE_USER,
  CTG_ACT_MAX
};

typedef enum {
  CTG_TASK_GET_QNODE = 0,
  CTG_TASK_GET_DB_VGROUP,
  CTG_TASK_GET_DB_CFG,
  CTG_TASK_GET_TB_META,
  CTG_TASK_GET_TB_HASH,
  CTG_TASK_GET_INDEX,
  CTG_TASK_GET_UDF,
  CTG_TASK_GET_USER,
} CTG_TASK_TYPE;

typedef struct SCtgDebug {
  bool     lockEnable;
  bool     cacheEnable;
  bool     apiEnable;
  bool     metaEnable;
  uint32_t showCachePeriodSec;
} SCtgDebug;

typedef struct SCtgTbCacheInfo {
  bool     inCache;
  uint64_t dbId;
  uint64_t suid;
  int32_t  tbType;
} SCtgTbCacheInfo;

typedef struct SCtgTbMetaCtx {
  SCtgTbCacheInfo tbInfo;
  SName* pName;
  int32_t flag;
} SCtgTbMetaCtx;

typedef struct SCtgDbVgCtx {
  char dbFName[TSDB_DB_FNAME_LEN];
} SCtgDbVgCtx;

typedef struct SCtgDbCfgCtx {
  char dbFName[TSDB_DB_FNAME_LEN];
} SCtgDbCfgCtx;

typedef struct SCtgTbHashCtx {
  char dbFName[TSDB_DB_FNAME_LEN];
  SName* pName;
} SCtgTbHashCtx;

typedef struct SCtgIndexCtx {
  char indexFName[TSDB_INDEX_FNAME_LEN];
} SCtgIndexCtx;

typedef struct SCtgUdfCtx {
  char udfName[TSDB_FUNC_NAME_LEN];
} SCtgUdfCtx;

typedef struct SCtgUserCtx {
  SUserAuthInfo user;
} SCtgUserCtx;

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

typedef struct SCtgUserAuth {
  int32_t   version;
  SRWLatch  lock;
  bool      superUser;
  SHashObj *createdDbs;
  SHashObj *readDbs;
  SHashObj *writeDbs;
} SCtgUserAuth;

typedef struct SCatalog {
  uint64_t         clusterId;  
  SHashObj        *userCache;    //key:user, value:SCtgUserAuth
  SHashObj        *dbCache;      //key:dbname, value:SCtgDBCache
  SCtgRentMgmt     dbRent;
  SCtgRentMgmt     stbRent;
} SCatalog;

typedef struct SCtgJob {
  int64_t          refId;
  SArray*          pTasks;
  int32_t          taskDone;
  SMetaData        jobRes;
  int32_t          rspCode;

  uint64_t         queryId;
  SCatalog*        pCtg; 
  void*            pTrans; 
  SEpSet           pMgmtEps;
  void*            userParam;
  catalogCallback  userFp;
  int32_t          tbMetaNum;
  int32_t          tbHashNum;
  int32_t          dbVgNum;
  int32_t          udfNum;
  int32_t          qnodeNum;
  int32_t          dbCfgNum;
  int32_t          indexNum;
  int32_t          userNum;
} SCtgJob;

typedef struct SCtgMsgCtx {
  int32_t reqType;
  void* lastOut;
  void* out;
  char* target;
} SCtgMsgCtx;

typedef struct SCtgTask {
  CTG_TASK_TYPE type;
  int32_t  taskId;
  SCtgJob *pJob;
  void* taskCtx;
  SCtgMsgCtx msgCtx;
  void* res;
} SCtgTask;

typedef int32_t (*ctgLanchTaskFp)(SCtgTask*);
typedef int32_t (*ctgHandleTaskMsgRspFp)(SCtgTask*, int32_t, const SDataBuf *, int32_t);
typedef int32_t (*ctgDumpTaskResFp)(SCtgTask*);

typedef struct SCtgAsyncFps {
  ctgLanchTaskFp launchFp;
  ctgHandleTaskMsgRspFp handleRspFp;
  ctgDumpTaskResFp dumpResFp;
} SCtgAsyncFps;

typedef struct SCtgApiStat {

#ifdef WINDOWS
  size_t avoidCompilationErrors;
#endif

} SCtgApiStat;

typedef struct SCtgRuntimeStat {
  uint64_t qNum;
  uint64_t qDoneNum;
} SCtgRuntimeStat;

typedef struct SCtgCacheStat {
  uint64_t clusterNum;
  uint64_t dbNum;
  uint64_t tblNum;
  uint64_t stblNum;
  uint64_t userNum;
  uint64_t vgHitNum;
  uint64_t vgMissNum;
  uint64_t tblHitNum;
  uint64_t tblMissNum;
  uint64_t userHitNum;
  uint64_t userMissNum;
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

typedef struct SCtgUpdateUserMsg {
  SCatalog* pCtg;
  SGetUserAuthRsp userAuth;
} SCtgUpdateUserMsg;


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
  int32_t               jobPool;
  SRWLatch              lock;
  SCtgQueue             queue;
  TdThread              updateThread;  
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

#define CTG_STAT_ADD(_item, _n) atomic_add_fetch_64(&(_item), _n)
#define CTG_STAT_SUB(_item, _n) atomic_sub_fetch_64(&(_item), _n)
#define CTG_STAT_GET(_item) atomic_load_64(&(_item))

#define CTG_RUNTIME_STAT_ADD(item, n) (CTG_STAT_ADD(gCtgMgmt.stat.runtime.item, n))
#define CTG_CACHE_STAT_ADD(item, n) (CTG_STAT_ADD(gCtgMgmt.stat.cache.item, n))
#define CTG_CACHE_STAT_SUB(item, n) (CTG_STAT_SUB(gCtgMgmt.stat.cache.item, n))

#define CTG_IS_META_NULL(type) ((type) == META_TYPE_NULL_TABLE)
#define CTG_IS_META_CTABLE(type) ((type) == META_TYPE_CTABLE)
#define CTG_IS_META_TABLE(type) ((type) == META_TYPE_TABLE)
#define CTG_IS_META_BOTH(type) ((type) == META_TYPE_BOTH_TABLE)

#define CTG_FLAG_STB          0x1
#define CTG_FLAG_NOT_STB      0x2
#define CTG_FLAG_UNKNOWN_STB  0x4
#define CTG_FLAG_SYS_DB       0x8
#define CTG_FLAG_FORCE_UPDATE 0x10

#define CTG_FLAG_SET(_flag, _v) ((_flag) |= (_v))

#define CTG_FLAG_IS_STB(_flag) ((_flag) & CTG_FLAG_STB)
#define CTG_FLAG_IS_NOT_STB(_flag) ((_flag) & CTG_FLAG_NOT_STB)
#define CTG_FLAG_IS_UNKNOWN_STB(_flag) ((_flag) & CTG_FLAG_UNKNOWN_STB)
#define CTG_FLAG_IS_SYS_DB(_flag) ((_flag) & CTG_FLAG_SYS_DB)
#define CTG_FLAG_IS_FORCE_UPDATE(_flag) ((_flag) & CTG_FLAG_FORCE_UPDATE)
#define CTG_FLAG_SET_SYS_DB(_flag) ((_flag) |= CTG_FLAG_SYS_DB)
#define CTG_FLAG_SET_STB(_flag, tbType) do { (_flag) |= ((tbType) == TSDB_SUPER_TABLE) ? CTG_FLAG_STB : ((tbType) > TSDB_SUPER_TABLE ? CTG_FLAG_NOT_STB : CTG_FLAG_UNKNOWN_STB); } while (0)
#define CTG_FLAG_MAKE_STB(_isStb) (((_isStb) == 1) ? CTG_FLAG_STB : ((_isStb) == 0 ? CTG_FLAG_NOT_STB : CTG_FLAG_UNKNOWN_STB))
#define CTG_FLAG_MATCH_STB(_flag, tbType) (CTG_FLAG_IS_UNKNOWN_STB(_flag) || (CTG_FLAG_IS_STB(_flag) && (tbType) == TSDB_SUPER_TABLE) || (CTG_FLAG_IS_NOT_STB(_flag) && (tbType) != TSDB_SUPER_TABLE))

#define CTG_IS_SYS_DBNAME(_dbname) (((*(_dbname) == 'i') && (0 == strcmp(_dbname, TSDB_INFORMATION_SCHEMA_DB))) || ((*(_dbname) == 'p') && (0 == strcmp(_dbname, TSDB_PERFORMANCE_SCHEMA_DB))))

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

#define CTG_PARAMS SCatalog* pCtg, void *pTrans, const SEpSet* pMgmtEps
#define CTG_PARAMS_LIST() pCtg, pTrans, pMgmtEps

void ctgdShowTableMeta(SCatalog* pCtg, const char *tbName, STableMeta* p);
void ctgdShowClusterCache(SCatalog* pCtg);
int32_t ctgdShowCacheInfo(void);

int32_t ctgRemoveTbMetaFromCache(SCatalog* pCtg, SName* pTableName, bool syncReq);
int32_t ctgGetTbMetaFromCache(CTG_PARAMS, SCtgTbMetaCtx* ctx, STableMeta** pTableMeta);

int32_t ctgActUpdateVg(SCtgMetaAction *action);
int32_t ctgActUpdateTb(SCtgMetaAction *action);
int32_t ctgActRemoveDB(SCtgMetaAction *action);
int32_t ctgActRemoveStb(SCtgMetaAction *action);
int32_t ctgActRemoveTb(SCtgMetaAction *action);
int32_t ctgActUpdateUser(SCtgMetaAction *action);
int32_t ctgAcquireVgInfoFromCache(SCatalog* pCtg, const char *dbFName, SCtgDBCache **pCache);
void ctgReleaseDBCache(SCatalog *pCtg, SCtgDBCache *dbCache);
void ctgReleaseVgInfo(SCtgDBCache *dbCache);
int32_t ctgAcquireVgInfoFromCache(SCatalog* pCtg, const char *dbFName, SCtgDBCache **pCache);
int32_t ctgTbMetaExistInCache(SCatalog* pCtg, char *dbFName, char* tbName, int32_t *exist);
int32_t ctgReadTbMetaFromCache(SCatalog* pCtg, SCtgTbMetaCtx* ctx, STableMeta** pTableMeta);
int32_t ctgReadTbVerFromCache(SCatalog *pCtg, const SName *pTableName, int32_t *sver, int32_t *tver, int32_t *tbType, uint64_t *suid, char *stbName);
int32_t ctgChkAuthFromCache(SCatalog* pCtg, const char* user, const char* dbFName, AUTH_TYPE type, bool *inCache, bool *pass);
int32_t ctgPutRmDBToQueue(SCatalog* pCtg, const char *dbFName, int64_t dbId);
int32_t ctgPutRmStbToQueue(SCatalog* pCtg, const char *dbFName, int64_t dbId, const char *stbName, uint64_t suid, bool syncReq);
int32_t ctgPutRmTbToQueue(SCatalog* pCtg, const char *dbFName, int64_t dbId, const char *tbName, bool syncReq);
int32_t ctgPutUpdateVgToQueue(SCatalog* pCtg, const char *dbFName, int64_t dbId, SDBVgInfo* dbInfo, bool syncReq);
int32_t ctgPutUpdateTbToQueue(SCatalog* pCtg, STableMetaOutput *output, bool syncReq);
int32_t ctgPutUpdateUserToQueue(SCatalog* pCtg, SGetUserAuthRsp *pAuth, bool syncReq);
int32_t ctgMetaRentInit(SCtgRentMgmt *mgmt, uint32_t rentSec, int8_t type);
int32_t ctgMetaRentAdd(SCtgRentMgmt *mgmt, void *meta, int64_t id, int32_t size);
int32_t ctgMetaRentGet(SCtgRentMgmt *mgmt, void **res, uint32_t *num, int32_t size);
int32_t ctgUpdateTbMetaToCache(SCatalog* pCtg, STableMetaOutput* pOut, bool syncReq);
int32_t ctgStartUpdateThread();
int32_t ctgRelaunchGetTbMetaTask(SCtgTask *pTask);



int32_t ctgProcessRspMsg(void* out, int32_t reqType, char* msg, int32_t msgSize, int32_t rspCode, char* target);
int32_t ctgGetDBVgInfoFromMnode(CTG_PARAMS, SBuildUseDBInput *input, SUseDbOutput *out, SCtgTask* pTask);
int32_t ctgGetQnodeListFromMnode(CTG_PARAMS, SArray *out, SCtgTask* pTask);
int32_t ctgGetDBCfgFromMnode(CTG_PARAMS, const char *dbFName, SDbCfgInfo *out, SCtgTask* pTask);
int32_t ctgGetIndexInfoFromMnode(CTG_PARAMS, const char *indexName, SIndexInfo *out, SCtgTask* pTask);
int32_t ctgGetUdfInfoFromMnode(CTG_PARAMS, const char *funcName, SFuncInfo *out, SCtgTask* pTask);
int32_t ctgGetUserDbAuthFromMnode(CTG_PARAMS, const char *user, SGetUserAuthRsp *out, SCtgTask* pTask);
int32_t ctgGetTbMetaFromMnodeImpl(CTG_PARAMS, char *dbFName, char* tbName, STableMetaOutput* out, SCtgTask* pTask);
int32_t ctgGetTbMetaFromMnode(CTG_PARAMS, const SName* pTableName, STableMetaOutput* out, SCtgTask* pTask);
int32_t ctgGetTbMetaFromVnode(CTG_PARAMS, const SName* pTableName, SVgroupInfo *vgroupInfo, STableMetaOutput* out, SCtgTask* pTask);

int32_t ctgInitJob(CTG_PARAMS, SCtgJob** job, uint64_t reqId, const SCatalogReq* pReq, catalogCallback fp, void* param);
int32_t ctgLaunchJob(SCtgJob *pJob);
int32_t ctgMakeAsyncRes(SCtgJob *pJob);

int32_t ctgCloneVgInfo(SDBVgInfo *src, SDBVgInfo **dst);
int32_t ctgCloneMetaOutput(STableMetaOutput *output, STableMetaOutput **pOutput);
int32_t ctgGenerateVgList(SCatalog *pCtg, SHashObj *vgHash, SArray** pList);
void ctgFreeJob(void* job);
void ctgFreeHandle(SCatalog* pCtg);
void ctgFreeVgInfo(SDBVgInfo *vgInfo);
int32_t ctgGetVgInfoFromHashValue(SCatalog *pCtg, SDBVgInfo *dbInfo, const SName *pTableName, SVgroupInfo *pVgroup);
void ctgResetTbMetaTask(SCtgTask* pTask);
void ctgFreeDbCache(SCtgDBCache *dbCache);
int32_t ctgStbVersionSortCompare(const void* key1, const void* key2);
int32_t ctgDbVgVersionSortCompare(const void* key1, const void* key2);
int32_t ctgStbVersionSearchCompare(const void* key1, const void* key2);
int32_t ctgDbVgVersionSearchCompare(const void* key1, const void* key2);
void ctgFreeSTableMetaOutput(STableMetaOutput* pOutput);
int32_t ctgUpdateMsgCtx(SCtgMsgCtx* pCtx, int32_t reqType, void* out, char* target);


extern SCatalogMgmt gCtgMgmt;
extern SCtgDebug gCTGDebug;
extern SCtgAsyncFps gCtgAsyncFps[];

#ifdef __cplusplus
}
#endif

#endif /*_TD_CATALOG_INT_H_*/
