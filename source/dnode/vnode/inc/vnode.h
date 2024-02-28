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

#ifndef _TD_VNODE_H_
#define _TD_VNODE_H_

#include "os.h"
#include "tmsgcb.h"
#include "tqueue.h"
#include "trpc.h"

#include "sync.h"
#include "tarray.h"
#include "tfs.h"
#include "wal.h"

#include "filter.h"
#include "tcommon.h"
#include "tfs.h"
#include "tgrant.h"
#include "tmsg.h"
#include "trow.h"

#include "storageapi.h"
#include "tdb.h"

#ifdef __cplusplus
extern "C" {
#endif

// vnode
typedef struct SVnode       SVnode;
typedef struct STsdbCfg     STsdbCfg;  // todo: remove
typedef struct SVnodeCfg    SVnodeCfg;
typedef struct SVSnapReader SVSnapReader;
typedef struct SVSnapWriter SVSnapWriter;

extern const SVnodeCfg vnodeCfgDefault;

int32_t vnodeInit(int32_t nthreads);
void    vnodeCleanup();
int32_t vnodeCreate(const char *path, SVnodeCfg *pCfg, int32_t diskPrimary, STfs *pTfs);
int32_t vnodeAlterReplica(const char *path, SAlterVnodeReplicaReq *pReq, int32_t diskPrimary, STfs *pTfs);
int32_t vnodeAlterHashRange(const char *srcPath, const char *dstPath, SAlterVnodeHashRangeReq *pReq,
                            int32_t diskPrimary, STfs *pTfs);
int32_t vnodeRestoreVgroupId(const char *srcPath, const char *dstPath, int32_t srcVgId, int32_t dstVgId,
                             int32_t diskPrimary, STfs *pTfs);
void    vnodeDestroy(int32_t vgId, const char *path, STfs *pTfs);
SVnode *vnodeOpen(const char *path, int32_t diskPrimary, STfs *pTfs, SMsgCb msgCb, bool force);
void    vnodePreClose(SVnode *pVnode);
void    vnodePostClose(SVnode *pVnode);
void    vnodeSyncCheckTimeout(SVnode *pVnode);
void    vnodeClose(SVnode *pVnode);
int32_t vnodeSyncCommit(SVnode *pVnode);
int32_t vnodeBegin(SVnode *pVnode);

int32_t vnodeStart(SVnode *pVnode);
void    vnodeStop(SVnode *pVnode);
int64_t vnodeGetSyncHandle(SVnode *pVnode);
int32_t   vnodeGetSnapshot(SVnode *pVnode, SSnapshot *pSnapshot);
void vnodeGetInfo(void *pVnode, const char **dbname, int32_t *vgId, int64_t *numOfTables, int64_t *numOfNormalTables);
int32_t   vnodeProcessCreateTSma(SVnode *pVnode, void *pCont, uint32_t contLen);
int32_t   vnodeGetTableList(void *pVnode, int8_t type, SArray *pList);
int32_t   vnodeGetAllTableList(SVnode *pVnode, uint64_t uid, SArray *list);
int32_t   vnodeIsCatchUp(SVnode *pVnode);
ESyncRole vnodeGetRole(SVnode *pVnode);

int32_t vnodeGetCtbIdList(void *pVnode, int64_t suid, SArray *list);
int32_t vnodeGetCtbIdListByFilter(SVnode *pVnode, int64_t suid, SArray *list, bool (*filter)(void *arg), void *arg);
int32_t vnodeGetStbIdList(SVnode *pVnode, int64_t suid, SArray *list);
int32_t vnodeGetStbIdListByFilter(SVnode *pVnode, int64_t suid, SArray *list, bool (*filter)(void *arg, void *arg1),
                                  void *arg);
void   *vnodeGetIdx(void *pVnode);
void   *vnodeGetIvtIdx(void *pVnode);

int32_t vnodeGetCtbNum(SVnode *pVnode, int64_t suid, int64_t *num);
int32_t vnodeGetStbColumnNum(SVnode *pVnode, tb_uid_t suid, int *num);
int32_t vnodeGetTimeSeriesNum(SVnode *pVnode, int64_t *num);
int32_t vnodeGetAllCtbNum(SVnode *pVnode, int64_t *num);

int32_t vnodeGetTableSchema(void *pVnode, int64_t uid, STSchema **pSchema, int64_t *suid);

void    vnodeResetLoad(SVnode *pVnode, SVnodeLoad *pLoad);
int32_t vnodeGetLoad(SVnode *pVnode, SVnodeLoad *pLoad);
int32_t vnodeGetLoadLite(SVnode *pVnode, SVnodeLoadLite *pLoad);
int32_t vnodeValidateTableHash(SVnode *pVnode, char *tableFName);

int32_t vnodePreProcessWriteMsg(SVnode *pVnode, SRpcMsg *pMsg);
int32_t vnodePreprocessQueryMsg(SVnode *pVnode, SRpcMsg *pMsg);

int32_t vnodeProcessWriteMsg(SVnode *pVnode, SRpcMsg *pMsg, int64_t version, SRpcMsg *pRsp);
int32_t vnodeProcessSyncMsg(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp);
int32_t vnodeProcessQueryMsg(SVnode *pVnode, SRpcMsg *pMsg);
int32_t vnodeProcessFetchMsg(SVnode *pVnode, SRpcMsg *pMsg, SQueueInfo *pInfo);
int32_t vnodeProcessStreamMsg(SVnode *pVnode, SRpcMsg *pMsg, SQueueInfo *pInfo);
void    vnodeProposeWriteMsg(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs);
void    vnodeApplyWriteMsg(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs);
void    vnodeProposeCommitOnNeed(SVnode *pVnode, bool atExit);

// meta
void        _metaReaderInit(SMetaReader *pReader, void *pVnode, int32_t flags, SStoreMeta *pAPI);
void        metaReaderReleaseLock(SMetaReader *pReader);
void        metaReaderClear(SMetaReader *pReader);
int32_t     metaReaderGetTableEntryByUid(SMetaReader *pReader, tb_uid_t uid);
int32_t     metaReaderGetTableEntryByUidCache(SMetaReader *pReader, tb_uid_t uid);
int32_t     metaGetTableTags(void *pVnode, uint64_t suid, SArray *uidList);
int32_t     metaGetTableTagsByUids(void *pVnode, int64_t suid, SArray *uidList);
int32_t     metaReadNext(SMetaReader *pReader);
const void *metaGetTableTagVal(const void *tag, int16_t type, STagVal *tagVal);
int         metaGetTableNameByUid(void *meta, uint64_t uid, char *tbName);

int      metaGetTableSzNameByUid(void *meta, uint64_t uid, char *tbName);
int      metaGetTableUidByName(void *pVnode, char *tbName, uint64_t *uid);
int      metaGetTableTypeByName(void *meta, char *tbName, ETableType *tbType);
int      metaGetTableTtlByUid(void *meta, uint64_t uid, int64_t *ttlDays);
bool     metaIsTableExist(void *pVnode, tb_uid_t uid);
int32_t  metaGetCachedTableUidList(void *pVnode, tb_uid_t suid, const uint8_t *key, int32_t keyLen, SArray *pList,
                                   bool *acquired);
int32_t  metaUidFilterCachePut(void *pVnode, uint64_t suid, const void *pKey, int32_t keyLen, void *pPayload,
                               int32_t payloadLen, double selectivityRatio);
tb_uid_t metaGetTableEntryUidByName(SMeta *pMeta, const char *name);
int32_t  metaGetCachedTbGroup(void *pVnode, tb_uid_t suid, const uint8_t *pKey, int32_t keyLen, SArray **pList);
int32_t  metaPutTbGroupToCache(void *pVnode, uint64_t suid, const void *pKey, int32_t keyLen, void *pPayload,
                               int32_t payloadLen);
bool     metaTbInFilterCache(SMeta *pMeta, const void* key, int8_t type);
int32_t  metaPutTbToFilterCache(SMeta *pMeta, const void* key, int8_t type);
int32_t  metaSizeOfTbFilterCache(SMeta *pMeta, int8_t type);
int32_t  metaInitTbFilterCache(SMeta *pMeta);

int32_t metaGetStbStats(void *pVnode, int64_t uid, int64_t *numOfTables, int32_t *numOfCols);

// tsdb
typedef struct STsdbReader STsdbReader;

#define TSDB_DEFAULT_STT_FILE  8
#define TSDB_DEFAULT_PAGE_SIZE 4096

#define TIMEWINDOW_RANGE_CONTAINED 1
#define TIMEWINDOW_RANGE_EXTERNAL  2

#define CACHESCAN_RETRIEVE_TYPE_ALL    0x1
#define CACHESCAN_RETRIEVE_TYPE_SINGLE 0x2
#define CACHESCAN_RETRIEVE_LAST_ROW    0x4
#define CACHESCAN_RETRIEVE_LAST        0x8

int32_t      tsdbReaderOpen2(void *pVnode, SQueryTableDataCond *pCond, void *pTableList, int32_t numOfTables,
                             SSDataBlock *pResBlock, void **ppReader, const char *idstr, SHashObj **pIgnoreTables);
int32_t      tsdbSetTableList2(STsdbReader *pReader, const void *pTableList, int32_t num);
void         tsdbReaderSetId2(STsdbReader *pReader, const char *idstr);
void         tsdbReaderClose2(STsdbReader *pReader);
int32_t      tsdbNextDataBlock2(STsdbReader *pReader, bool *hasNext);
int32_t      tsdbRetrieveDatablockSMA2(STsdbReader *pReader, SSDataBlock *pDataBlock, bool *allHave, bool *hasNullSMA);
void         tsdbReleaseDataBlock2(STsdbReader *pReader);
SSDataBlock *tsdbRetrieveDataBlock2(STsdbReader *pTsdbReadHandle, SArray *pColumnIdList);
int32_t      tsdbReaderReset2(STsdbReader *pReader, SQueryTableDataCond *pCond);
int32_t      tsdbGetFileBlocksDistInfo2(STsdbReader *pReader, STableBlockDistInfo *pTableBlockInfo);
int64_t      tsdbGetNumOfRowsInMemTable2(STsdbReader *pHandle);
void        *tsdbGetIdx2(SMeta *pMeta);
void        *tsdbGetIvtIdx2(SMeta *pMeta);
uint64_t     tsdbGetReaderMaxVersion2(STsdbReader *pReader);
void         tsdbReaderSetCloseFlag(STsdbReader *pReader);
int64_t      tsdbGetLastTimestamp2(SVnode *pVnode, void *pTableList, int32_t numOfTables, const char *pIdStr);
void         tsdbSetFilesetDelimited(STsdbReader* pReader);
void         tsdbReaderSetNotifyCb(STsdbReader* pReader, TsdReaderNotifyCbFn notifyFn, void* param);

int32_t tsdbReuseCacherowsReader(void *pReader, void *pTableIdList, int32_t numOfTables);
int32_t tsdbCacherowsReaderOpen(void *pVnode, int32_t type, void *pTableIdList, int32_t numOfTables, int32_t numOfCols,
                                SArray *pCidList, int32_t *pSlotIds, uint64_t suid, void **pReader, const char *idstr,
                                SArray* pFuncTypeList);
int32_t tsdbRetrieveCacheRows(void *pReader, SSDataBlock *pResBlock, const int32_t *slotIds, const int32_t *dstSlotIds,
                              SArray *pTableUids);
void   *tsdbCacherowsReaderClose(void *pReader);

void    tsdbCacheSetCapacity(SVnode *pVnode, size_t capacity);
size_t  tsdbCacheGetCapacity(SVnode *pVnode);
size_t  tsdbCacheGetUsage(SVnode *pVnode);
int32_t tsdbCacheGetElems(SVnode *pVnode);

//// tq
typedef struct SIdInfo {
  int64_t version;
  int32_t index;
} SIdInfo;

typedef struct STqReader {
  SPackedData     msg;
  SSubmitReq2     submit;
  int32_t         nextBlk;
  int64_t         lastBlkUid;
  SWalReader     *pWalReader;
  SMeta          *pVnodeMeta;
  SHashObj       *tbIdHash;
  SArray         *pColIdList;  // SArray<int16_t>
  int32_t         cachedSchemaVer;
  int64_t         cachedSchemaSuid;
  int64_t         cachedSchemaUid;
  SSchemaWrapper *pSchemaWrapper;
  SSDataBlock    *pResBlock;
  int64_t         lastTs;
} STqReader;

STqReader *tqReaderOpen(SVnode *pVnode);
void       tqReaderClose(STqReader *);

void    tqReaderSetColIdList(STqReader *pReader, SArray *pColIdList);
int32_t tqReaderSetTbUidList(STqReader *pReader, const SArray *tbUidList, const char *id);
int32_t tqReaderAddTbUidList(STqReader *pReader, const SArray *pTableUidList);
int32_t tqReaderRemoveTbUidList(STqReader *pReader, const SArray *tbUidList);

bool tqReaderIsQueriedTable(STqReader *pReader, uint64_t uid);
bool tqCurrentBlockConsumed(const STqReader *pReader);

int32_t      tqReaderSeek(STqReader *pReader, int64_t ver, const char *id);
bool         tqNextBlockInWal(STqReader *pReader, const char *idstr, int sourceExcluded);
bool         tqNextBlockImpl(STqReader *pReader, const char *idstr);
SWalReader  *tqGetWalReader(STqReader *pReader);
SSDataBlock *tqGetResultBlock(STqReader *pReader);
int64_t      tqGetResultBlockTime(STqReader *pReader);

int32_t extractMsgFromWal(SWalReader *pReader, void **pItem, int64_t maxVer, const char *id);
int32_t tqReaderSetSubmitMsg(STqReader *pReader, void *msgStr, int32_t msgLen, int64_t ver);
bool    tqNextDataBlockFilterOut(STqReader *pReader, SHashObj *filterOutUids);
int32_t tqRetrieveDataBlock(STqReader *pReader, SSDataBlock **pRes, const char *idstr);
int32_t tqRetrieveTaosxBlock(STqReader *pReader, SArray *blocks, SArray *schemas, SSubmitTbData **pSubmitTbDataRet);
int32_t tqGetStreamExecInfo(SVnode* pVnode, int64_t streamId, int64_t* pDelay, bool* fhFinished);

// sma
int32_t smaGetTSmaDays(SVnodeCfg *pCfg, void *pCont, uint32_t contLen, int32_t *days);

// SVSnapReader
int32_t vnodeSnapReaderOpen(SVnode *pVnode, SSnapshotParam *pParam, SVSnapReader **ppReader);
void    vnodeSnapReaderClose(SVSnapReader *pReader);
int32_t vnodeSnapRead(SVSnapReader *pReader, uint8_t **ppData, uint32_t *nData);
// SVSnapWriter
int32_t vnodeSnapWriterOpen(SVnode *pVnode, SSnapshotParam *pParam, SVSnapWriter **ppWriter);
int32_t vnodeSnapWriterClose(SVSnapWriter *pWriter, int8_t rollback, SSnapshot *pSnapshot);
int32_t vnodeSnapWrite(SVSnapWriter *pWriter, uint8_t *pData, uint32_t nData);

int32_t        buildSnapContext(SVnode *pVnode, int64_t snapVersion, int64_t suid, int8_t subType, int8_t withMeta,
                                SSnapContext **ctxRet);
int32_t        getTableInfoFromSnapshot(SSnapContext *ctx, void **pBuf, int32_t *contLen, int16_t *type, int64_t *uid);
SMetaTableInfo getMetaTableInfoFromSnapshot(SSnapContext *ctx);
int32_t        setForSnapShot(SSnapContext *ctx, int64_t uid);
int32_t        destroySnapContext(SSnapContext *ctx);

// structs
struct STsdbCfg {
  int8_t  precision;
  int8_t  update;
  int8_t  compression;
  int8_t  slLevel;
  int32_t minRows;
  int32_t maxRows;
  int32_t days;   // just for save config, don't use in tsdbRead/tsdbCommit/..., and use STsdbKeepCfg in STsdb instead
  int32_t keep0;  // just for save config, don't use in tsdbRead/tsdbCommit/..., and use STsdbKeepCfg in STsdb instead
  int32_t keep1;  // just for save config, don't use in tsdbRead/tsdbCommit/..., and use STsdbKeepCfg in STsdb instead
  int32_t keep2;  // just for save config, don't use in tsdbRead/tsdbCommit/..., and use STsdbKeepCfg in STsdb instead
  int32_t keepTimeOffset;  // just for save config, use STsdbKeepCfg in STsdb instead
  SRetention retentions[TSDB_RETENTION_MAX];
};

typedef struct {
  int64_t numOfSTables;
  int64_t numOfCTables;
  int64_t numOfNTables;
  int64_t numOfReportedTimeSeries;
  int64_t numOfNTimeSeries;
  int64_t numOfTimeSeries;
  // int64_t itvTimeSeries;
  int64_t pointsWritten;
  int64_t totalStorage;
  int64_t compStorage;
} SVnodeStats;

struct SVnodeCfg {
  int32_t     vgId;
  char        dbname[TSDB_DB_FNAME_LEN];
  uint64_t    dbId;
  int32_t     cacheLastSize;
  int32_t     szPage;
  int32_t     szCache;
  uint64_t    szBuf;
  bool        isHeap;
  bool        isWeak;
  int8_t      cacheLast;
  int8_t      isTsma;
  int8_t      isRsma;
  int8_t      hashMethod;
  int8_t      standby;
  STsdbCfg    tsdbCfg;
  SWalCfg     walCfg;
  SSyncCfg    syncCfg;
  SVnodeStats vndStats;
  uint32_t    hashBegin;
  uint32_t    hashEnd;
  bool        hashChange;
  int16_t     sttTrigger;
  int16_t     hashPrefix;
  int16_t     hashSuffix;
  int32_t     tsdbPageSize;
};

#define TABLE_ROLLUP_ON       ((int8_t)0x1)
#define TABLE_IS_ROLLUP(FLG)  (((FLG) & (TABLE_ROLLUP_ON)) != 0)
#define TABLE_SET_ROLLUP(FLG) ((FLG) |= TABLE_ROLLUP_ON)

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_H_*/
