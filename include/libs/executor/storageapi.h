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

#ifndef TDENGINE_STORAGEAPI_H
#define TDENGINE_STORAGEAPI_H

#include "tsimplehash.h"
#include "tscalablebf.h"
//#include "tdb.h"
#include "taosdef.h"
#include "tmsg.h"
#include "tcommon.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TIMEWINDOW_RANGE_CONTAINED 1
#define TIMEWINDOW_RANGE_EXTERNAL  2

#define CACHESCAN_RETRIEVE_TYPE_ALL    0x1
#define CACHESCAN_RETRIEVE_TYPE_SINGLE 0x2
#define CACHESCAN_RETRIEVE_LAST_ROW    0x4
#define CACHESCAN_RETRIEVE_LAST        0x8

typedef struct SMeta SMeta;

typedef struct SMetaEntry {
  int64_t  version;
  int8_t   type;
  int8_t   flags;  // TODO: need refactor?
  tb_uid_t uid;
  char *   name;
  union {
    struct {
      SSchemaWrapper schemaRow;
      SSchemaWrapper schemaTag;
      SRSmaParam     rsmaParam;
    } stbEntry;
    struct {
      int64_t  ctime;
      int32_t  ttlDays;
      int32_t  commentLen;
      char *   comment;
      tb_uid_t suid;
      uint8_t *pTags;
    } ctbEntry;
    struct {
      int64_t        ctime;
      int32_t        ttlDays;
      int32_t        commentLen;
      char *         comment;
      int32_t        ncid;  // next column id
      SSchemaWrapper schemaRow;
    } ntbEntry;
    struct {
      STSma *tsma;
    } smaEntry;
  };

  uint8_t *pBuf;
} SMetaEntry;

// int32_t tsdbReuseCacherowsReader(void* pReader, void* pTableIdList, int32_t numOfTables);
// int32_t tsdbCacherowsReaderOpen(void *pVnode, int32_t type, void *pTableIdList, int32_t numOfTables, int32_t numOfCols,
//                                SArray *pCidList, int32_t *pSlotIds, uint64_t suid, void **pReader, const char *idstr);
// int32_t tsdbRetrieveCacheRows(void *pReader, SSDataBlock *pResBlock, const int32_t *slotIds, const int32_t *dstSlotIds,
//                              SArray *pTableUids);
// void   *tsdbCacherowsReaderClose(void *pReader);
// int32_t tsdbGetTableSchema(void *pVnode, int64_t uid, STSchema **pSchema, int64_t *suid);

// int32_t      tsdbReaderOpen(void *pVnode, SQueryTableDataCond *pCond, void *pTableList, int32_t numOfTables,
//                            SSDataBlock *pResBlock, STsdbReader **ppReader, const char *idstr, bool countOnly, SHashObj** pIgnoreTables);
// int32_t      tsdbSetTableList(STsdbReader *pReader, const void *pTableList, int32_t num);
// void         tsdbReaderSetId(STsdbReader *pReader, const char *idstr);
// void         tsdbReaderClose(STsdbReader *pReader);
// int32_t      tsdbNextDataBlock(STsdbReader *pReader, bool *hasNext);
// int32_t      tsdbRetrieveDatablockSMA(STsdbReader *pReader, SSDataBlock *pDataBlock, bool *allHave);
// void         tsdbReleaseDataBlock(STsdbReader *pReader);
// SSDataBlock *tsdbRetrieveDataBlock(STsdbReader *pTsdbReadHandle, SArray *pColumnIdList);
// int32_t      tsdbReaderReset(STsdbReader *pReader, SQueryTableDataCond *pCond);
// int32_t      tsdbGetFileBlocksDistInfo(STsdbReader *pReader, STableBlockDistInfo *pTableBlockInfo);
// int64_t      tsdbGetNumOfRowsInMemTable(STsdbReader *pHandle);
// void        *tsdbGetIdx(void *pMeta);
// void        *tsdbGetIvtIdx(void *pMeta);
// uint64_t     tsdbGetReaderMaxVersion(STsdbReader *pReader);
// void         tsdbReaderSetCloseFlag(STsdbReader *pReader);
// int64_t      tsdbGetLastTimestamp(void* pVnode, void* pTableList, int32_t numOfTables, const char* pIdStr);

// int32_t vnodeGetCtbIdList(void *pVnode, int64_t suid, SArray *list);
// int32_t vnodeGetCtbIdListByFilter(void *pVnode, int64_t suid, SArray *list, bool (*filter)(void *arg), void *arg);
// int32_t vnodeGetStbIdList(void *pVnode, int64_t suid, SArray *list);
// void   *vnodeGetIdx(void *pVnode);
// void   *vnodeGetIvtIdx(void *pVnode);

// void        metaReaderInit(SMetaReader *pReader, SMeta *pMeta, int32_t flags);
// void        metaReaderReleaseLock(SMetaReader *pReader);
// void        metaReaderClear(SMetaReader *pReader);
// int32_t     metaGetTableEntryByUid(SMetaReader *pReader, tb_uid_t uid);
// int32_t     metaGetTableEntryByUidCache(SMetaReader *pReader, tb_uid_t uid);
// int         metaGetTableEntryByName(SMetaReader *pReader, const char *name);
// int32_t     metaGetTableTags(SMeta *pMeta, uint64_t suid, SArray *uidList);
// int32_t     metaGetTableTagsByUids(SMeta *pMeta, int64_t suid, SArray *uidList);
// const void *metaGetTableTagVal(void *tag, int16_t type, STagVal *tagVal);
// int         metaGetTableNameByUid(void *meta, uint64_t uid, char *tbName);
//
// int      metaGetTableSzNameByUid(void *meta, uint64_t uid, char *tbName);
// int      metaGetTableUidByName(void *meta, char *tbName, uint64_t *uid);
// int      metaGetTableTypeByName(void *meta, char *tbName, ETableType *tbType);
// bool     metaIsTableExist(SMeta *pMeta, tb_uid_t uid);
// int32_t  metaGetCachedTableUidList(SMeta *pMeta, tb_uid_t suid, const uint8_t *key, int32_t keyLen, SArray *pList,
//                                   bool *acquired);
// int32_t  metaUidFilterCachePut(SMeta *pMeta, uint64_t suid, const void *pKey, int32_t keyLen, void *pPayload,
//                               int32_t payloadLen, double selectivityRatio);
// int32_t  metaUidCacheClear(SMeta *pMeta, uint64_t suid);
// tb_uid_t metaGetTableEntryUidByName(SMeta *pMeta, const char *name);
// int32_t  metaGetCachedTbGroup(SMeta* pMeta, tb_uid_t suid, const uint8_t* pKey, int32_t keyLen, SArray** pList);
// int32_t  metaPutTbGroupToCache(SMeta* pMeta, uint64_t suid, const void* pKey, int32_t keyLen, void* pPayload,
//                               int32_t payloadLen);

// tq
typedef struct SMetaTableInfo {
  int64_t         suid;
  int64_t         uid;
  SSchemaWrapper *schema;
  char            tbName[TSDB_TABLE_NAME_LEN];
} SMetaTableInfo;

typedef struct SSnapContext {
  SMeta *     pMeta;
  int64_t     snapVersion;
  struct TBC *pCur;
  int64_t     suid;
  int8_t      subType;
  SHashObj *  idVersion;
  SHashObj *  suidInfo;
  SArray *    idList;
  int32_t     index;
  bool        withMeta;
  bool        queryMeta;  // true-get meta, false-get data
} SSnapContext;

typedef struct {
  int64_t uid;
  int64_t ctbNum;
} SMetaStbStats;

// void    tqReaderSetColIdList(STqReader *pReader, SArray *pColIdList);
// int32_t tqReaderSetTbUidList(STqReader *pReader, const SArray *tbUidList);
// int32_t tqReaderAddTbUidList(STqReader *pReader, const SArray *pTableUidList);
// int32_t tqReaderRemoveTbUidList(STqReader *pReader, const SArray *tbUidList);
// bool    tqReaderIsQueriedTable(STqReader* pReader, uint64_t uid);
// bool    tqCurrentBlockConsumed(const STqReader* pReader);
//
// int32_t tqSeekVer(STqReader *pReader, int64_t ver, const char *id);
// bool    tqNextBlockInWal(STqReader* pReader, const char* idstr);
// bool    tqNextBlockImpl(STqReader *pReader, const char* idstr);

// int32_t        getMetafromSnapShot(SSnapContext *ctx, void **pBuf, int32_t *contLen, int16_t *type, int64_t *uid);
// SMetaTableInfo getUidfromSnapShot(SSnapContext *ctx);
// int32_t        setForSnapShot(SSnapContext *ctx, int64_t uid);
// int32_t        destroySnapContext(SSnapContext *ctx);

// SMTbCursor *metaOpenTbCursor(SMeta *pMeta);
// void        metaCloseTbCursor(SMTbCursor *pTbCur);
// int32_t     metaTbCursorNext(SMTbCursor *pTbCur, ETableType jumpTableType);
// int32_t     metaTbCursorPrev(SMTbCursor *pTbCur, ETableType jumpTableType);

#define META_READER_NOLOCK 0x1

/*-------------------------------------------------new api format---------------------------------------------------*/

//  typedef int32_t       (*__store_reader_(STsdbReader *pReader, const void *pTableList, int32_t num);
//  typedef void          (*tsdbReaderSetId(STsdbReader *pReader, const char *idstr);
//  typedef void          (*tsdbReaderClose(STsdbReader *pReader);
//  typedef int32_t       (*tsdbNextDataBlock(STsdbReader *pReader, bool *hasNext);
//  typedef int32_t       (*tsdbRetrieveDatablockSMA(STsdbReader *pReader, SSDataBlock *pDataBlock, bool *allHave);
//  typedef void          (*tsdbReleaseDataBlock(STsdbReader *pReader);
//  typedef SSDataBlock * (*tsdbRetrieveDataBlock(STsdbReader *pTsdbReadHandle, SArray *pColumnIdList);
//  typedef int32_t       (*tsdbReaderReset(STsdbReader *pReader, SQueryTableDataCond *pCond);
//  typedef int32_t       (*tsdbGetFileBlocksDistInfo(STsdbReader *pReader, STableBlockDistInfo *pTableBlockInfo);
//  typedef int64_t       (*tsdbGetNumOfRowsInMemTable(STsdbReader *pHandle);
//  typedef void        * (*tsdbGetIdx(void *pMeta);
//  typedef void        * (*tsdbGetIvtIdx(void *pMeta);
//  typedef uint64_t      (*tsdbGetReaderMaxVersion(STsdbReader *pReader);
//  typedef void          (*tsdbReaderSetCloseFlag(STsdbReader *pReader);
//  typedef int64_t       (*tsdbGetLastTimestamp(void* pVnode, void* pTableList, int32_t numOfTables, const char* pIdStr);

typedef int32_t (*__store_reader_open_fn_t)(void *pVnode, SQueryTableDataCond *pCond, void *pTableList,
                                            int32_t numOfTables, SSDataBlock *pResBlock, void **ppReader,
                                            const char *idstr, bool countOnly, SHashObj **pIgnoreTables);

typedef struct SStoreDataReaderFn {
  __store_reader_open_fn_t storeReaderOpen;
  void (*storeReaderClose)();
  void (*setReaderId)(void *pReader, const char *pId);
  void (*storeReaderSetTableList)();
  int32_t (*storeReaderNextDataBlock)();
  int32_t (*storeReaderRetrieveBlockSMA)();

  SSDataBlock *(*storeReaderRetrieveDataBlock)();
  void (*storeReaderReleaseDataBlock)();

  void (*storeReaderResetStatus)();
  void (*storeReaderGetDataBlockDistInfo)();
  void (*storeReaderGetNumOfInMemRows)();
  void (*storeReaderNotifyClosing)();
} SStoreDataReaderFn;

/**
 * int32_t tsdbReuseCacherowsReader(void* pReader, void* pTableIdList, int32_t numOfTables);
int32_t tsdbCacherowsReaderOpen(void *pVnode, int32_t type, void *pTableIdList, int32_t numOfTables, int32_t numOfCols,
                                SArray *pCidList, int32_t *pSlotIds, uint64_t suid, void **pReader, const char *idstr);
int32_t tsdbRetrieveCacheRows(void *pReader, SSDataBlock *pResBlock, const int32_t *slotIds, const int32_t *dstSlotIds,
                              SArray *pTableUids);
void   *tsdbCacherowsReaderClose(void *pReader);
 */
typedef struct SStoreCachedDataReaderFn {
  int32_t (*openReader)(void *pVnode, int32_t type, void *pTableIdList, int32_t numOfTables, int32_t numOfCols,
                        SArray *pCidList, int32_t *pSlotIds, uint64_t suid, void **pReader, const char *idstr);
  void *(*closeReader)(void *pReader);
  int32_t (*retrieveRows)(void *pReader, SSDataBlock *pResBlock, const int32_t *slotIds, const int32_t *dstSlotIds,
                          SArray *pTableUidList);
  void (*reuseReader)(void *pReader, void *pTableIdList, int32_t numOfTables);
} SStoreCachedDataReaderFn;

/*------------------------------------------------------------------------------------------------------------------*/
/*
 *
void    tqReaderSetColIdList(STqReader *pReader, SArray *pColIdList);
int32_t tqReaderSetTbUidList(STqReader *pReader, const SArray *tbUidList);
int32_t tqReaderAddTbUidList(STqReader *pReader, const SArray *pTableUidList);
int32_t tqReaderRemoveTbUidList(STqReader *pReader, const SArray *tbUidList);
bool    tqReaderIsQueriedTable(STqReader* pReader, uint64_t uid);
bool    tqCurrentBlockConsumed(const STqReader* pReader);

int32_t tqSeekVer(STqReader *pReader, int64_t ver, const char *id);
bool    tqNextBlockInWal(STqReader* pReader, const char* idstr);
bool    tqNextBlockImpl(STqReader *pReader, const char* idstr);

 int32_t    tqRetrieveDataBlock(STqReader *pReader, SSDataBlock **pRes, const char* idstr);
STqReader *tqReaderOpen(void *pVnode);
void       tqCloseReader(STqReader *);

int32_t tqReaderSetSubmitMsg(STqReader *pReader, void *msgStr, int32_t msgLen, int64_t ver);
bool    tqNextDataBlockFilterOut(STqReader *pReader, SHashObj *filterOutUids);
SWalReader* tqGetWalReader(STqReader* pReader);
int32_t tqRetrieveTaosxBlock(STqReader *pReader, SArray *blocks, SArray *schemas, SSubmitTbData **pSubmitTbDataRet);
*/
// todo rename
typedef struct SStoreTqReaderFn {
  void *(*tqReaderOpen)();
  void (*tqReaderClose)();

  int32_t (*tqReaderSeek)();
  int32_t (*tqRetrieveBlock)();
  bool (*tqReaderNextBlockInWal)();
  bool (*tqNextBlockImpl)();  // todo remove it

  void (*tqReaderSetColIdList)();
  int32_t (*tqReaderSetTargetTableList)();

  int32_t (*tqReaderAddTables)();
  int32_t (*tqReaderRemoveTables)();

  bool (*tqReaderIsQueriedTable)();
  bool (*tqReaderCurrentBlockConsumed)();

  struct SWalReader *(*tqReaderGetWalReader)();  // todo remove it
  void (*tqReaderRetrieveTaosXBlock)();          // todo remove it

  int32_t (*tqReaderSetSubmitMsg)();  // todo remove it
  void (*tqReaderNextBlockFilterOut)();
} SStoreTqReaderFn;

typedef struct SStoreSnapshotFn {
  /*
  int32_t        getMetafromSnapShot(SSnapContext *ctx, void **pBuf, int32_t *contLen, int16_t *type, int64_t *uid);
  SMetaTableInfo getUidfromSnapShot(SSnapContext *ctx);
  int32_t        setForSnapShot(SSnapContext *ctx, int64_t uid);
  int32_t        destroySnapContext(SSnapContext *ctx);
   */
  int32_t (*storeCreateSnapshot)();
  void (*storeDestroySnapshot)();
  SMetaTableInfo (*storeSSGetTableInfo)();
  int32_t (*storeSSGetMetaInfo)();
} SStoreSnapshotFn;

/**
void        metaReaderInit(SMetaReader *pReader, SMeta *pMeta, int32_t flags);
void        metaReaderReleaseLock(SMetaReader *pReader);
void        metaReaderClear(SMetaReader *pReader);
int32_t     metaGetTableEntryByUid(SMetaReader *pReader, tb_uid_t uid);
int32_t     metaGetTableEntryByUidCache(SMetaReader *pReader, tb_uid_t uid);
int         metaGetTableEntryByName(SMetaReader *pReader, const char *name);
int32_t     metaGetTableTags(SMeta *pMeta, uint64_t suid, SArray *uidList);
const void *metaGetTableTagVal(void *tag, int16_t type, STagVal *tagVal);
int         metaGetTableNameByUid(void *meta, uint64_t uid, char *tbName);

int      metaGetTableSzNameByUid(void *meta, uint64_t uid, char *tbName);
int      metaGetTableUidByName(void *meta, char *tbName, uint64_t *uid);
int      metaGetTableTypeByName(void *meta, char *tbName, ETableType *tbType);
bool     metaIsTableExist(SMeta *pMeta, tb_uid_t uid);
int32_t  metaGetCachedTableUidList(SMeta *pMeta, tb_uid_t suid, const uint8_t *key, int32_t keyLen, SArray *pList,
                                   bool *acquired);
int32_t  metaUidFilterCachePut(SMeta *pMeta, uint64_t suid, const void *pKey, int32_t keyLen, void *pPayload,
                               int32_t payloadLen, double selectivityRatio);
int32_t  metaUidCacheClear(SMeta *pMeta, uint64_t suid);
tb_uid_t metaGetTableEntryUidByName(SMeta *pMeta, const char *name);
int32_t  metaGetCachedTbGroup(SMeta* pMeta, tb_uid_t suid, const uint8_t* pKey, int32_t keyLen, SArray** pList);
int32_t  metaPutTbGroupToCache(SMeta* pMeta, uint64_t suid, const void* pKey, int32_t keyLen, void* pPayload,
                               int32_t payloadLen);
 */
typedef struct SMetaReader {
  int32_t             flags;
  void *              pMeta;
  SDecoder            coder;
  SMetaEntry          me;
  void *              pBuf;
  int32_t             szBuf;
  struct SStorageAPI *storageAPI;
} SMetaReader;

typedef struct SStoreMetaReaderFn {
  void (*initReader)(void *pReader, void *pMeta, int32_t flags);
  void *(*clearReader)();

  void (*readerReleaseLock)();

  int32_t (*getTableEntryByUid)();
  int32_t (*getTableEntryByName)();
  int32_t (*readerGetEntryGetUidCache)(SMetaReader *pReader, tb_uid_t uid);
} SStoreMetaReaderFn;

typedef struct SStoreMetaFn {
  /*
SMTbCursor *metaOpenTbCursor(SMeta *pMeta);
void        metaCloseTbCursor(SMTbCursor *pTbCur);
int32_t     metaTbCursorNext(SMTbCursor *pTbCur, ETableType jumpTableType);
int32_t     metaTbCursorPrev(SMTbCursor *pTbCur, ETableType jumpTableType);
 */
  void *(*openMetaCursor)();
  void (*closeMetaCursor)();
  int32_t (*cursorNext)();
  void (*cursorPrev)();

  int32_t (*getTableTags)(void *pVnode, uint64_t suid, SArray *uidList);
  int32_t (*getTableTagsByUid)();
  const char *(*extractTagVal)(const void *tag, int16_t type, STagVal *tagVal);  // todo remove it

  int32_t (*getTableUidByName)(void *pVnode, char *tbName, uint64_t *uid);
  int32_t (*getTableTypeByName)(void *pVnode, char *tbName, ETableType *tbType);
  int32_t (*getTableNameByUid)(void *pVnode, uint64_t uid, char *tbName);
  bool (*isTableExisted)(void *pVnode, uint64_t uid);

  /**
   * int32_t  metaUidFilterCachePut(SMeta *pMeta, uint64_t suid, const void *pKey, int32_t keyLen, void *pPayload,
                               int32_t payloadLen, double selectivityRatio);
int32_t  metaUidCacheClear(SMeta *pMeta, uint64_t suid);
tb_uid_t metaGetTableEntryUidByName(SMeta *pMeta, const char *name);
int32_t  metaGetCachedTbGroup(SMeta* pMeta, tb_uid_t suid, const uint8_t* pKey, int32_t keyLen, SArray** pList);
int32_t  metaPutTbGroupToCache(SMeta* pMeta, uint64_t suid, const void* pKey, int32_t keyLen, void* pPayload,
                               int32_t payloadLen);
   */
  void (*getCachedTableList)();
  void (*putTableListIntoCache)();

  /**
   *
   */
  void *(*storeGetIndexInfo)();
  void *(*storeGetInvertIndex)();
  void (*storeGetChildTableList)();  // support filter and non-filter cases. [vnodeGetCtbIdList & vnodeGetCtbIdListByFilter]
  int32_t (*storeGetTableList)();    // vnodeGetStbIdList  & vnodeGetAllTableList
  void *storeGetVersionRange;
  void *storeGetLastTimestamp;

  int32_t (*getTableSchema)(void *pVnode, int64_t uid, STSchema **pSchema, int64_t *suid);  // tsdbGetTableSchema

  // db name, vgId, numOfTables, numOfSTables
  void (*storeGetNumOfChildTables)();  // int32_t metaGetStbStats(SMeta *pMeta, int64_t uid, SMetaStbStats *pInfo);
  void (*storeGetBasicInfo)();  // vnodeGetInfo(void *pVnode, const char **dbname, int32_t *vgId) & metaGetTbNum(SMeta *pMeta) & metaGetNtbNum(SMeta *pMeta);

  int64_t (*getNumOfRowsInMem)();
  /**
int32_t vnodeGetCtbIdList(void *pVnode, int64_t suid, SArray *list);
int32_t vnodeGetCtbIdListByFilter(void *pVnode, int64_t suid, SArray *list, bool (*filter)(void *arg), void *arg);
int32_t vnodeGetStbIdList(void *pVnode, int64_t suid, SArray *list);
 */
} SStoreMetaFn;

typedef struct STdbState {
  void*               rocksdb;
  void**              pHandle;
  void*               writeOpts;
  void*               readOpts;
  void**              cfOpts;
  void*               dbOpt;
  struct SStreamTask* pOwner;
  void*               param;
  void*               env;
  SListNode*          pComparNode;
  void*               pBackendHandle;
  char                idstr[64];
  void*               compactFactory;

  void* db;
  void* pStateDb;
  void* pFuncStateDb;
  void* pFillStateDb;  // todo refactor
  void* pSessionStateDb;
  void* pParNameDb;
  void* pParTagDb;
  void* txn;
} STdbState;

// incremental state storage
typedef struct {
  STdbState*        pTdbState;
  struct SStreamFileState* pFileState;
  int32_t           number;
  SSHashObj*        parNameMap;
  int64_t           checkPointId;
  int32_t           taskId;
  int64_t           streamId;
} SStreamState;

typedef struct SUpdateInfo {
  SArray      *pTsBuckets;
  uint64_t     numBuckets;
  SArray      *pTsSBFs;
  uint64_t     numSBFs;
  int64_t      interval;
  int64_t      watermark;
  TSKEY        minTS;
  SScalableBf *pCloseWinSBF;
  SHashObj    *pMap;
  uint64_t     maxDataVersion;
} SUpdateInfo;

typedef struct {
  void*    iter;
  void*    snapshot;
  void* readOpt;
  void*             db;
//  rocksdb_iterator_t*    iter;
//  rocksdb_snapshot_t*    snapshot;
//  rocksdb_readoptions_t* readOpt;
//  rocksdb_t*             db;

  void*    pCur;
  int64_t number;
} SStreamStateCur;

typedef TSKEY (*GetTsFun)(void*);

typedef struct SStateStore {
  int32_t (*streamStatePutParName)(SStreamState* pState, int64_t groupId, const char* tbname);
  int32_t (*streamStateGetParName)(SStreamState* pState, int64_t groupId, void** pVal);

  int32_t (*streamStateAddIfNotExist)(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen);
  int32_t (*streamStateReleaseBuf)(SStreamState* pState, const SWinKey* key, void* pVal);
  void    (*streamStateFreeVal)(void* val);

  int32_t (*streamStatePut)(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen);
  int32_t (*streamStateGet)(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen);
  bool    (*streamStateCheck)(SStreamState* pState, const SWinKey* key);
  int32_t (*streamStateGetByPos)(SStreamState* pState, void* pos, void** pVal);
  int32_t (*streamStateDel)(SStreamState* pState, const SWinKey* key);
  int32_t (*streamStateClear)(SStreamState* pState);
  void    (*streamStateSetNumber)(SStreamState* pState, int32_t number);
  int32_t (*streamStateSaveInfo)(SStreamState* pState, void* pKey, int32_t keyLen, void* pVal, int32_t vLen);
  int32_t (*streamStateGetInfo)(SStreamState* pState, void* pKey, int32_t keyLen, void** pVal, int32_t* pLen);

  int32_t (*streamStateFillPut)(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen);
  int32_t (*streamStateFillGet)(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen);
  int32_t (*streamStateFillDel)(SStreamState* pState, const SWinKey* key);

  int32_t (*streamStateCurNext)(SStreamState* pState, void* pCur);
  int32_t (*streamStateCurPrev)(SStreamState* pState, void* pCur);

  void*  (*streamStateGetAndCheckCur)(SStreamState* pState, SWinKey* key);
  void*  (*streamStateSeekKeyNext)(SStreamState* pState, const SWinKey* key);
  void*  (*streamStateFillSeekKeyNext)(SStreamState* pState, const SWinKey* key);
  void*  (*streamStateFillSeekKeyPrev)(SStreamState* pState, const SWinKey* key);
  void   (*streamStateFreeCur)(void* pCur);

  int32_t (*streamStateGetGroupKVByCur)(void* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen);
  int32_t (*streamStateGetKVByCur)(void* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen);

  int32_t (*streamStateSessionAddIfNotExist)(SStreamState* pState, SSessionKey* key, TSKEY gap, void** pVal, int32_t* pVLen);
  int32_t (*streamStateSessionPut)(SStreamState* pState, const SSessionKey* key, const void* value, int32_t vLen);
  int32_t (*streamStateSessionGet)(SStreamState* pState, SSessionKey* key, void** pVal, int32_t* pVLen);
  int32_t (*streamStateSessionDel)(SStreamState* pState, const SSessionKey* key);
  int32_t (*streamStateSessionClear)(SStreamState* pState);
  int32_t (*streamStateSessionGetKVByCur)(void* pCur, SSessionKey* pKey, void** pVal, int32_t* pVLen);
  int32_t (*streamStateStateAddIfNotExist)(SStreamState* pState, SSessionKey* key, char* pKeyData, int32_t keyDataLen,
                                     state_key_cmpr_fn fn, void** pVal, int32_t* pVLen);
  int32_t (*streamStateSessionGetKeyByRange)(void* pState, const SSessionKey* range, SSessionKey* curKey);

  void*   (*updateInfoInit)(int64_t interval, int32_t precision, int64_t watermark);
  TSKEY   (*updateInfoFillBlockData)(void *pInfo, SSDataBlock *pBlock, int32_t primaryTsCol);
  bool    (*updateInfoIsUpdated)(void *pInfo, uint64_t tableId, TSKEY ts);
  bool    (*updateInfoIsTableInserted)(void *pInfo, int64_t tbUid);
  void    (*updateInfoDestroy)(void *pInfo);

  SUpdateInfo* (*updateInfoInitP)(SInterval *pInterval, int64_t watermark);
  void        (*updateInfoAddCloseWindowSBF)(void *pInfo);
  void        (*updateInfoDestoryColseWinSBF)(void *pInfo);
  int32_t     (*updateInfoSerialize)(void *buf, int32_t bufLen, const void *pInfo);
  int32_t     (*updateInfoDeserialize)(void *buf, int32_t bufLen, void *pInfo);

  void* (*streamStateSessionSeekKeyNext)(SStreamState* pState, const SSessionKey* key);
  void* (*streamStateSessionSeekKeyCurrentPrev)(SStreamState* pState, const SSessionKey* key);
  void* (*streamStateSessionSeekKeyCurrentNext)(SStreamState* pState, const SSessionKey* key);

  void* (*streamFileStateInit)(int64_t memSize, uint32_t keySize, uint32_t rowSize, uint32_t selectRowSize, GetTsFun fp,
                               void* pFile, TSKEY delMark);

  void (*streamFileStateDestroy)(void* pFileState);
  void (*streamFileStateClear)(void* pFileState);
  bool (*needClearDiskBuff)(void* pFileState);

  SStreamState* (*streamStateOpen)(char* path, void* pTask, bool specPath, int32_t szPage, int32_t pages);
  void          (*streamStateClose)(SStreamState* pState, bool remove);
  int32_t       (*streamStateBegin)(SStreamState* pState);
  int32_t       (*streamStateCommit)(SStreamState* pState);
  void          (*streamStateDestroy)(SStreamState* pState, bool remove);
  int32_t       (*streamStateDeleteCheckPoint)(SStreamState* pState, TSKEY mark);
} SStateStore;

typedef struct SStorageAPI {
  SStoreMetaFn             metaFn;  // todo: refactor
  SStoreDataReaderFn       storeReader;
  SStoreMetaReaderFn       metaReaderFn;
  SStoreCachedDataReaderFn cacheFn;
  SStoreSnapshotFn         snapshotFn;
  SStoreTqReaderFn         tqReaderFn;
  SStateStore              stateStore;
} SStorageAPI;

typedef struct SMTbCursor {
  struct TBC *pDbc;
  void *      pKey;
  void *      pVal;
  int32_t     kLen;
  int32_t     vLen;
  SMetaReader mr;
} SMTbCursor;

typedef struct SRowBuffPos {
  void* pRowBuff;
  void* pKey;
  bool  beFlushed;
  bool  beUsed;
} SRowBuffPos;



#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STORAGEAPI_H
