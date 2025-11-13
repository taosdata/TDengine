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

#include "function.h"
#include "index.h"
#include "osMemory.h"
#include "taosdef.h"
#include "tcommon.h"
#include "tmsg.h"
#include "tscalablebf.h"
#include "tsimplehash.h"

#ifdef __cplusplus
extern "C" {
#endif

#define TIMEWINDOW_RANGE_CONTAINED 1
#define TIMEWINDOW_RANGE_EXTERNAL  2

#define CACHESCAN_RETRIEVE_TYPE_ALL    0x1
#define CACHESCAN_RETRIEVE_TYPE_SINGLE 0x2
#define CACHESCAN_RETRIEVE_LAST_ROW    0x4
#define CACHESCAN_RETRIEVE_LAST        0x8

#define META_READER_LOCK   0x0
#define META_READER_NOLOCK 0x1

#define STREAM_STATE_BUFF_HASH        1
#define STREAM_STATE_BUFF_SORT        2
#define STREAM_STATE_BUFF_HASH_SORT   3
#define STREAM_STATE_BUFF_HASH_SEARCH 4

typedef struct SMeta SMeta;
typedef TSKEY (*GetTsFun)(void*);

typedef struct SMetaEntry {
  int64_t  version;
  int8_t   type;
  int8_t   flags;  // TODO: need refactor?
  tb_uid_t uid;
  char*    name;
  union {
    struct {
      SSchemaWrapper schemaRow;
      SSchemaWrapper schemaTag;
      SRSmaParam     rsmaParam;
      int64_t        keep;
    } stbEntry;
    struct {
      int64_t  btime;
      int32_t  ttlDays;
      int32_t  commentLen;  // not include '\0'
      char*    comment;
      tb_uid_t suid;
      uint8_t* pTags;
    } ctbEntry;
    struct {
      int64_t        btime;
      int32_t        ttlDays;
      int32_t        commentLen;
      char*          comment;
      int32_t        ncid;  // next column id
      SSchemaWrapper schemaRow;
    } ntbEntry;
    struct {
      STSma* tsma;
    } smaEntry;
  };

  uint8_t* pBuf;

  SColCmprWrapper colCmpr;  // col compress alg
  SExtSchema*     pExtSchemas;
  SColRefWrapper  colRef;   // col reference for virtual table
} SMetaEntry;

typedef struct SMetaReader {
  int32_t            flags;
  void*              pMeta;
  SDecoder           coder;
  SMetaEntry         me;
  void*              pBuf;
  int32_t            szBuf;
  struct SStoreMeta* pAPI;
} SMetaReader;

typedef struct SMTbCursor {
  void*       pMeta;
  void*       pDbc;
  void*       pKey;
  void*       pVal;
  int32_t     kLen;
  int32_t     vLen;
  SMetaReader mr;
  int8_t      paused;
} SMTbCursor;

typedef struct SMCtbCursor {
  struct SMeta* pMeta;
  void*         pCur;
  tb_uid_t      suid;
  void*         pKey;
  void*         pVal;
  int           kLen;
  int           vLen;
  int8_t        paused;
  int           lock;
} SMCtbCursor;

typedef struct SRowBuffPos {
  void* pRowBuff;
  void* pKey;
  bool  beFlushed;
  bool  beUsed;
  bool  needFree;
  bool  beUpdated;
  bool  invalid;
} SRowBuffPos;

// tq
typedef struct SMetaTableInfo {
  int64_t         suid;
  int64_t         uid;
  SSchemaWrapper* schema;
  SExtSchema*     pExtSchemas;
  char            tbName[TSDB_TABLE_NAME_LEN];
} SMetaTableInfo;

static FORCE_INLINE void destroyMetaTableInfo(SMetaTableInfo* mtInfo){
  if (mtInfo == NULL) return;
  tDeleteSchemaWrapper(mtInfo->schema);
  taosMemoryFreeClear(mtInfo->pExtSchemas);
}

typedef struct SSnapContext {
  struct SMeta* pMeta;
  int64_t       snapVersion;
  void*         pCur;
  int64_t       suid;
  int8_t        subType;
  SHashObj*     idVersion;
  SHashObj*     suidInfo;
  SArray*       idList;
  int32_t       index;
  int8_t        withMeta;
  int8_t        queryMeta;  // true-get meta, false-get data
  bool          hasPrimaryKey;
} SSnapContext;

typedef struct {
  int64_t uid;
  int64_t ctbNum;
  int32_t colNum;
  int8_t  flags;
  int64_t keep;
} SMetaStbStats;

// clang-format off
/*-------------------------------------------------new api format---------------------------------------------------*/
typedef enum {
  TSD_READER_NOTIFY_DURATION_START,
  TSD_READER_NOTIFY_NEXT_DURATION_BLOCK,
} ETsdReaderNotifyType;

typedef union {
  struct {
    int32_t filesetId;
  } duration;
} STsdReaderNotifyInfo;

typedef void (*TsdReaderNotifyCbFn)(ETsdReaderNotifyType type, STsdReaderNotifyInfo* info, void* param);

struct SFileSetReader;

typedef struct TsdReader {
  int32_t      (*tsdReaderOpen)(void* pVnode, SQueryTableDataCond* pCond, void* pTableList, int32_t numOfTables,
                           SSDataBlock* pResBlock, void** ppReader, const char* idstr, SHashObj** pIgnoreTables);
  void         (*tsdReaderClose)(void* pReader);
  int32_t      (*tsdSetReaderTaskId)(void* pReader, const char* pId);
  int32_t      (*tsdSetQueryTableList)(void* p, const void* pTableList, int32_t num);
  int32_t      (*tsdNextDataBlock)(void* pReader, bool* hasNext);

  int32_t      (*tsdReaderRetrieveBlockSMAInfo)();
  int32_t      (*tsdReaderRetrieveDataBlock)(void* p, SSDataBlock** pBlock);

  void         (*tsdReaderReleaseDataBlock)(void* pReader);

  int32_t      (*tsdReaderResetStatus)(void* p, SQueryTableDataCond* pCond);
  int32_t      (*tsdReaderGetDataBlockDistInfo)();
  void         (*tsdReaderGetDatablock)();
  void         (*tsdReaderSetDatablock)();
  int64_t      (*tsdReaderGetNumOfInMemRows)();
  void         (*tsdReaderNotifyClosing)();

  void         (*tsdSetFilesetDelimited)(void* pReader);
  void         (*tsdSetSetNotifyCb)(void* pReader, TsdReaderNotifyCbFn notifyFn, void* param);

  // for fileset query
  int32_t (*fileSetReaderOpen)(void *pVnode, struct SFileSetReader **ppReader);
  int32_t (*fileSetReadNext)(struct SFileSetReader *);
  int32_t (*fileSetGetEntryField)(struct SFileSetReader *, const char *, void *);
  void (*fileSetReaderClose)(struct SFileSetReader **);

  // retrieve first/last ts for each table
  int32_t  (*tsdCreateFirstLastTsIter)(void *pVnode, STimeWindow *pWindow, SVersionRange *pVerRange, uint64_t suid, void *pTableList,
                                   int32_t numOfTables, int32_t order, void **pIter, const char *idstr);
  int32_t  (*tsdNextFirstLastTsBlock)(void *pIter, SSDataBlock *pRes);
  void     (*tsdDestroyFirstLastTsIter)(void *pIter);

} TsdReader;

typedef struct SStoreCacheReader {
  int32_t  (*openReader)(void *pVnode, int32_t type, void *pTableIdList, int32_t numOfTables, int32_t numOfCols,
                         SArray *pCidList, int32_t *pSlotIds, uint64_t suid, void **pReader, const char *idstr,
                         SArray *pFuncTypeList, SColumnInfo* pPkCol, int32_t numOfPks);
  void     (*closeReader)(void *pReader);
  int32_t  (*retrieveRows)(void *pReader, SSDataBlock *pResBlock, const int32_t *slotIds, const int32_t *dstSlotIds,
                           SArray *pTableUidList, bool* pGotAllRows);
  int32_t  (*reuseReader)(void *pReader, void *pTableIdList, int32_t numOfTables);
} SStoreCacheReader;

// clang-format on

/*------------------------------------------------------------------------------------------------------------------*/
// todo rename
typedef struct SStoreTqReader {
  struct STqReader* (*tqReaderOpen)();
  void (*tqReaderClose)();

  int32_t (*tqReaderSeek)();
  bool (*tqReaderNextBlockInWal)();
  bool (*tqNextBlockImpl)();  // todo remove it
  SSDataBlock* (*tqGetResultBlock)();
  int64_t (*tqGetResultBlockTime)();

  int32_t (*tqReaderSetColIdList)();
  int32_t (*tqReaderSetQueryTableList)();

  void (*tqReaderAddTables)();
  void (*tqReaderRemoveTables)();

  void (*tqSetTablePrimaryKey)();
  bool (*tqGetTablePrimaryKey)();
  bool (*tqReaderIsQueriedTable)();
  bool (*tqReaderCurrentBlockConsumed)();

  struct SWalReader* (*tqReaderGetWalReader)();  // todo remove it
                                                 //  int32_t (*tqReaderRetrieveTaosXBlock)();       // todo remove it

  int32_t (*tqReaderSetSubmitMsg)();  // todo remove it
  //  bool (*tqReaderNextBlockFilterOut)();

  int32_t (*tqReaderSetVtableInfo)();
} SStoreTqReader;

typedef struct SStoreSnapshotFn {
  bool (*taosXGetTablePrimaryKey)(SSnapContext* ctx);
  void (*taosXSetTablePrimaryKey)(SSnapContext* ctx, int64_t uid);
  int32_t (*setForSnapShot)(SSnapContext* ctx, int64_t uid);
  void (*destroySnapshot)(SSnapContext* ctx);
  int32_t (*getMetaTableInfoFromSnapshot)(SSnapContext* ctx, SMetaTableInfo* info);
  int32_t (*getTableInfoFromSnapshot)(SSnapContext* ctx, void** pBuf, int32_t* contLen, int16_t* type, int64_t* uid);
} SStoreSnapshotFn;

typedef struct SStoreMeta {
  SMTbCursor* (*openTableMetaCursor)(void* pVnode);                                 // metaOpenTbCursor
  void (*closeTableMetaCursor)(SMTbCursor* pTbCur);                                 // metaCloseTbCursor
  void (*pauseTableMetaCursor)(SMTbCursor* pTbCur);                                 // metaPauseTbCursor
  int32_t (*resumeTableMetaCursor)(SMTbCursor* pTbCur, int8_t first, int8_t move);  // metaResumeTbCursor
  int32_t (*cursorNext)(SMTbCursor* pTbCur, ETableType jumpTableType);              // metaTbCursorNext
  int32_t (*cursorPrev)(SMTbCursor* pTbCur, ETableType jumpTableType);              // metaTbCursorPrev

  int32_t (*getTableTags)(void* pVnode, uint64_t suid, SArray* uidList);
  int32_t (*getTableTagsByUid)(void* pVnode, int64_t suid, SArray* uidList);
  const void* (*extractTagVal)(const void* tag, int16_t type, STagVal* tagVal);  // todo remove it

  int32_t (*getTableUidByName)(void* pVnode, char* tbName, uint64_t* uid);
  int32_t (*getTableTypeSuidByName)(void* pVnode, char* tbName, ETableType* tbType, uint64_t* suid);
  int32_t (*getTableNameByUid)(void* pVnode, uint64_t uid, char* tbName);
  bool (*isTableExisted)(void* pVnode, tb_uid_t uid);

  int32_t (*metaGetCachedTbGroup)(void* pVnode, tb_uid_t suid, const uint8_t* pKey, int32_t keyLen, SArray** pList);
  int32_t (*metaPutTbGroupToCache)(void* pVnode, uint64_t suid, const void* pKey, int32_t keyLen, void* pPayload,
                                   int32_t payloadLen);

  int32_t (*getCachedTableList)(void* pVnode, tb_uid_t suid, const uint8_t* pKey, int32_t keyLen, SArray* pList1,
                                bool* acquireRes);
  int32_t (*putCachedTableList)(void* pVnode, uint64_t suid, const void* pKey, int32_t keyLen, void* pPayload,
                                int32_t payloadLen, double selectivityRatio);
  int32_t (*getStableCachedTableList)(void* pVnode, tb_uid_t suid,
    const uint8_t* pTagCondKey, int32_t tagCondKeyLen,
    const uint8_t* pKey, int32_t keyLen, SArray* pList1, bool* acquireRes);
  int32_t (*putStableCachedTableList)(void* pVnode, uint64_t suid,
    const void* pTagCondKey, int32_t tagCondKeyLen,
    const void* pKey, int32_t keyLen, SArray* pUidList, SArray** pTagColIds);

  int32_t (*metaGetCachedRefDbs)(void* pVnode, tb_uid_t suid, SArray* pList);
  int32_t (*metaPutRefDbsToCache)(void* pVnode, tb_uid_t suid, SArray* pList);

  void* (*storeGetIndexInfo)(void* pVnode);
  void* (*getInvertIndex)(void* pVnode);
  // support filter and non-filter cases. [vnodeGetCtbIdList & vnodeGetCtbIdListByFilter]
  int32_t (*getChildTableList)(void* pVnode, int64_t suid, SArray* list);
  int32_t (*storeGetTableList)(void* pVnode, int8_t type, SArray* pList);
  int32_t (*getTableSchema)(void* pVnode, int64_t uid, STSchema** pSchema, int64_t* suid, SSchemaWrapper** pTagSchema);
  int32_t (*getNumOfChildTables)(void* pVnode, int64_t uid, int64_t* numOfTables, int32_t* numOfCols, int8_t* flags);
  void (*getBasicInfo)(void* pVnode, const char** dbname, int32_t* vgId, int64_t* numOfTables,
                       int64_t* numOfNormalTables);
  int32_t (*getDBSize)(void* pVnode, SDbSizeStatisInfo* pInfo);

  SMCtbCursor* (*openCtbCursor)(void* pVnode, tb_uid_t uid, int lock);
  int32_t (*resumeCtbCursor)(SMCtbCursor* pCtbCur, int8_t first);
  void (*pauseCtbCursor)(SMCtbCursor* pCtbCur);
  void (*closeCtbCursor)(SMCtbCursor* pCtbCur);
  tb_uid_t (*ctbCursorNext)(SMCtbCursor* pCur);
} SStoreMeta;

typedef struct SStoreMetaReader {
  void (*initReader)(SMetaReader* pReader, void* pVnode, int32_t flags, SStoreMeta* pAPI);
  void (*clearReader)(SMetaReader* pReader);
  void (*readerReleaseLock)(SMetaReader* pReader);
  int32_t (*getTableEntryByUid)(SMetaReader* pReader, tb_uid_t uid);
  int     (*getTableEntryByVersionUid)(SMetaReader *pReader, int64_t version, tb_uid_t uid);
  int32_t (*getTableEntryByName)(SMetaReader* pReader, const char* name);
  int32_t (*getEntryGetUidCache)(SMetaReader* pReader, tb_uid_t uid);
} SStoreMetaReader;

typedef struct SUpdateInfo {
  SArray*      pTsBuckets;
  uint64_t     numBuckets;
  SArray*      pTsSBFs;
  uint64_t     numSBFs;
  int64_t      interval;
  int64_t      watermark;
  TSKEY        minTS;
  SScalableBf* pCloseWinSBF;
  SHashObj*    pMap;
  int64_t      maxDataVersion;
  int8_t       pkColType;
  int32_t      pkColLen;
  char*        pKeyBuff;
  char*        pValueBuff;

  int (*comparePkRowFn)(void* pValue1, void* pTs, void* pPkVal, __compar_fn_t cmpPkFn);
  __compar_fn_t comparePkCol;
} SUpdateInfo;

typedef struct SRecDataInfo {
  STimeWindow calWin;
  uint64_t    tableUid;
  int64_t     dataVersion;
  EStreamType mode;
  char        pPkColData[];
} SRecDataInfo;

typedef struct SScanRange {
  STimeWindow win;
  STimeWindow calWin;
  SSHashObj*  pGroupIds;
  SSHashObj*  pUIds;
} SScanRange;

typedef struct SResultWindowInfo {
  SRowBuffPos* pStatePos;
  SSessionKey  sessionWin;
  bool         isOutput;
} SResultWindowInfo;

typedef struct {
  void*   iter;      //  rocksdb_iterator_t*    iter;
  void*   snapshot;  //  rocksdb_snapshot_t*    snapshot;
  void*   readOpt;   //  rocksdb_readoptions_t* readOpt;
  void*   db;        //  rocksdb_t*             db;
  void*   pCur;
  int64_t number;
  void*   pStreamFileState;
  int32_t buffIndex;
  int32_t hashIter;
  void*   pHashData;
  int64_t minGpId;
} SStreamStateCur;

typedef struct STableTsDataState {
  SSHashObj*       pTableTsDataMap;
  __compar_fn_t    comparePkColFn;
  void*            pPkValBuff;
  int32_t          pkValLen;
  SStreamState*    pState;
  int32_t          curRecId;
  void*            pStreamTaskState;
  SArray*          pScanRanges;
  SRecDataInfo*    pRecValueBuff;
  int32_t          recValueLen;
  SStreamStateCur* pRecCur;
  int32_t          cfgIndex;
  void*            pBatch;
  int32_t          batchBufflen;
  void*            pBatchBuff;
} STableTsDataState;

typedef struct SStateStore {
  int32_t (*streamStatePutParName)(SStreamState* pState, int64_t groupId, const char* tbname);
  int32_t (*streamStateGetParName)(SStreamState* pState, int64_t groupId, void** pVal, bool onlyCache,
                                   int32_t* pWinCode);
  int32_t (*streamStateDeleteParName)(SStreamState* pState, int64_t groupId);
  void (*streamStateSetParNameInvalid)(SStreamState* pState);

  int32_t (*streamStateAddIfNotExist)(SStreamState* pState, const SWinKey* pKey, void** pVal, int32_t* pVLen,
                                      int32_t* pWinCode);
  void (*streamStateReleaseBuf)(SStreamState* pState, void* pVal, bool used);
  void (*streamStateClearBuff)(SStreamState* pState, void* pVal);
  void (*streamStateFreeVal)(void* val);
  int32_t (*streamStateGetPrev)(SStreamState* pState, const SWinKey* pKey, SWinKey* pResKey, void** pVal,
                                int32_t* pVLen, int32_t* pWinCode);
  int32_t (*streamStateGetAllPrev)(SStreamState* pState, const SWinKey* pKey, SArray* pResArray, int32_t maxNum);

  int32_t (*streamStatePut)(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen);
  int32_t (*streamStateGet)(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen, int32_t* pWinCode);
  bool (*streamStateCheck)(SStreamState* pState, const SWinKey* key, bool hasLimit, bool* pIsLast);
  bool (*streamStateCheckSessionState)(SStreamState* pState, SSessionKey* pKey, TSKEY gap, bool* pIsLast);
  int32_t (*streamStateGetByPos)(SStreamState* pState, void* pos, void** pVal);
  void (*streamStateDel)(SStreamState* pState, const SWinKey* key);
  void (*streamStateDelByGroupId)(SStreamState* pState, uint64_t groupId);
  void (*streamStateClear)(SStreamState* pState);
  void (*streamStateSetNumber)(SStreamState* pState, int32_t number, int32_t tsIdex);
  void (*streamStateSaveInfo)(SStreamState* pState, void* pKey, int32_t keyLen, void* pVal, int32_t vLen);
  int32_t (*streamStateGetInfo)(SStreamState* pState, void* pKey, int32_t keyLen, void** pVal, int32_t* pLen);
  int32_t (*streamStateGetNumber)(SStreamState* pState);
  int32_t (*streamStateDeleteInfo)(SStreamState* pState, void* pKey, int32_t keyLen);

  int32_t (*streamStateFillPut)(SStreamState* pState, const SWinKey* key, const void* value, int32_t vLen);
  int32_t (*streamStateFillGet)(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen,
                                int32_t* pWinCode);
  int32_t (*streamStateFillAddIfNotExist)(SStreamState* pState, const SWinKey* key, void** pVal, int32_t* pVLen,
                                          int32_t* pWinCode);
  void (*streamStateFillDel)(SStreamState* pState, const SWinKey* key);
  int32_t (*streamStateFillGetNext)(SStreamState* pState, const SWinKey* pKey, SWinKey* pResKey, void** pVal,
                                    int32_t* pVLen, int32_t* pWinCode);
  int32_t (*streamStateFillGetPrev)(SStreamState* pState, const SWinKey* pKey, SWinKey* pResKey, void** pVal,
                                    int32_t* pVLen, int32_t* pWinCode);

  void (*streamStateCurNext)(SStreamState* pState, SStreamStateCur* pCur);
  void (*streamStateCurPrev)(SStreamState* pState, SStreamStateCur* pCur);

  SStreamStateCur* (*streamStateGetAndCheckCur)(SStreamState* pState, SWinKey* key);
  SStreamStateCur* (*streamStateSeekKeyNext)(SStreamState* pState, const SWinKey* key);
  SStreamStateCur* (*streamStateFillSeekKeyNext)(SStreamState* pState, const SWinKey* key);
  SStreamStateCur* (*streamStateFillSeekKeyPrev)(SStreamState* pState, const SWinKey* key);
  void (*streamStateFreeCur)(SStreamStateCur* pCur);

  int32_t (*streamStateFillGetGroupKVByCur)(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen);
  int32_t (*streamStateGetKVByCur)(SStreamStateCur* pCur, SWinKey* pKey, const void** pVal, int32_t* pVLen);

  void (*streamStateClearExpiredState)(SStreamState* pState, int32_t numOfKeep, TSKEY minTs);
  void (*streamStateClearExpiredSessionState)(SStreamState* pState, int32_t numOfKeep, TSKEY minTs, SSHashObj* pFlushGroup);
  int32_t (*streamStateSetRecFlag)(SStreamState* pState, const void* pKey, int32_t keyLen, int32_t mode);
  int32_t (*streamStateGetRecFlag)(SStreamState* pState, const void* pKey, int32_t keyLen, int32_t* pMode);

  int32_t (*streamStateSessionAddIfNotExist)(SStreamState* pState, SSessionKey* key, TSKEY gap, void** pVal,
                                             int32_t* pVLen, int32_t* pWinCode);
  int32_t (*streamStateSessionPut)(SStreamState* pState, const SSessionKey* key, void* value, int32_t vLen);
  int32_t (*streamStateSessionGet)(SStreamState* pState, SSessionKey* key, void** pVal, int32_t* pVLen,
                                   int32_t* pWinCode);
  void (*streamStateSessionDel)(SStreamState* pState, const SSessionKey* key);
  void (*streamStateSessionReset)(SStreamState* pState, void* pVal);
  void (*streamStateSessionClear)(SStreamState* pState);
  int32_t (*streamStateSessionGetKVByCur)(SStreamStateCur* pCur, SSessionKey* pKey, void** pVal, int32_t* pVLen);
  int32_t (*streamStateStateAddIfNotExist)(SStreamState* pState, SSessionKey* key, char* pKeyData, int32_t keyDataLen,
                                           state_key_cmpr_fn fn, void** pVal, int32_t* pVLen, int32_t* pWinCode);
  int32_t (*streamStateSessionGetKeyByRange)(SStreamState* pState, const SSessionKey* range, SSessionKey* curKey);
  int32_t (*streamStateCountGetKeyByRange)(SStreamState* pState, const SSessionKey* range, SSessionKey* curKey);
  int32_t (*streamStateSessionAllocWinBuffByNextPosition)(SStreamState* pState, SStreamStateCur* pCur,
                                                          const SSessionKey* pKey, void** pVal, int32_t* pVLen);
  int32_t (*streamStateSessionSaveToDisk)(STableTsDataState* pTblState, SSessionKey* pKey, SRecDataInfo* pVal, int32_t vLen);
  int32_t (*streamStateFlushReaminInfoToDisk)(STableTsDataState* pTblState);
  int32_t (*streamStateSessionDeleteAll)(SStreamState* pState);

  int32_t (*streamStateCountWinAddIfNotExist)(SStreamState* pState, SSessionKey* pKey, COUNT_TYPE winCount,
                                              void** ppVal, int32_t* pVLen, int32_t* pWinCode);
  int32_t (*streamStateCountWinAdd)(SStreamState* pState, SSessionKey* pKey, COUNT_TYPE winCount, void** pVal,
                                    int32_t* pVLen);

  int32_t (*updateInfoInit)(int64_t interval, int32_t precision, int64_t watermark, bool igUp, int8_t pkType,
                            int32_t pkLen, SUpdateInfo** ppInfo);
  int32_t (*updateInfoFillBlockData)(SUpdateInfo* pInfo, SSDataBlock* pBlock, int32_t primaryTsCol,
                                     int32_t primaryKeyCol, TSKEY* pMaxResTs);
  bool (*updateInfoIsUpdated)(SUpdateInfo* pInfo, uint64_t tableId, TSKEY ts, void* pPkVal, int32_t len);
  bool (*updateInfoIsTableInserted)(SUpdateInfo* pInfo, int64_t tbUid);
  bool (*isIncrementalTimeStamp)(SUpdateInfo* pInfo, uint64_t tableId, TSKEY ts, void* pPkVal, int32_t len);

  void (*updateInfoDestroy)(SUpdateInfo* pInfo);
  void (*windowSBfDelete)(SUpdateInfo* pInfo, uint64_t count);
  int32_t (*windowSBfAdd)(SUpdateInfo* pInfo, uint64_t count);

  int32_t (*updateInfoInitP)(SInterval* pInterval, int64_t watermark, bool igUp, int8_t pkType, int32_t pkLen,
                             SUpdateInfo** ppInfo);
  void (*updateInfoAddCloseWindowSBF)(SUpdateInfo* pInfo);
  void (*updateInfoDestoryColseWinSBF)(SUpdateInfo* pInfo);
  int32_t (*updateInfoSerialize)(SEncoder* pEncoder, const SUpdateInfo* pInfo);
  int32_t (*updateInfoDeserialize)(SDecoder* pDeCoder, SUpdateInfo* pInfo);

  SStreamStateCur* (*streamStateSessionSeekKeyPrev)(SStreamState* pState, const SSessionKey* key);
  SStreamStateCur* (*streamStateSessionSeekKeyNext)(SStreamState* pState, const SSessionKey* key);
  SStreamStateCur* (*streamStateCountSeekKeyPrev)(SStreamState* pState, const SSessionKey* pKey, COUNT_TYPE count);
  SStreamStateCur* (*streamStateSessionSeekKeyCurrentPrev)(SStreamState* pState, const SSessionKey* key);
  SStreamStateCur* (*streamStateSessionSeekKeyCurrentNext)(SStreamState* pState, const SSessionKey* key);

  int32_t (*streamFileStateInit)(int64_t memSize, uint32_t keySize, uint32_t rowSize, uint32_t selectRowSize,
                                 GetTsFun fp, void* pFile, TSKEY delMark, const char* id, int64_t ckId, int8_t type,
                                 struct SStreamFileState** ppFileState);

  int32_t (*streamStateGroupPut)(SStreamState* pState, int64_t groupId, void* value, int32_t vLen);
  SStreamStateCur* (*streamStateGroupGetCur)(SStreamState* pState);
  void (*streamStateGroupCurNext)(SStreamStateCur* pCur);
  int32_t (*streamStateGroupGetKVByCur)(SStreamStateCur* pCur, int64_t* pKey, void** pVal, int32_t* pVLen);

  void (*streamFileStateDestroy)(struct SStreamFileState* pFileState);
  void (*streamFileStateClear)(struct SStreamFileState* pFileState);
  bool (*needClearDiskBuff)(struct SStreamFileState* pFileState);

  SStreamState* (*streamStateOpen)(const char* path, void* pTask, int64_t streamId, int32_t taskId);
  SStreamState* (*streamStateRecalatedOpen)(const char* path, void* pTask, int64_t streamId, int32_t taskId);
  void (*streamStateClose)(SStreamState* pState, bool remove);
  int32_t (*streamStateBegin)(SStreamState* pState);
  void (*streamStateCommit)(SStreamState* pState);
  void (*streamStateDestroy)(SStreamState* pState, bool remove);
  void (*streamStateReloadInfo)(SStreamState* pState, TSKEY ts);
  void (*streamStateCopyBackend)(SStreamState* src, SStreamState* dst);

  int32_t (*streamStateGetAndSetTsData)(STableTsDataState* pState, uint64_t tableUid, TSKEY* pCurTs, void** ppCurPkVal,
                                        TSKEY lastTs, void* pLastPkVal, int32_t lastPkLen, int32_t* pWinCode);
  int32_t (*streamStateTsDataCommit)(STableTsDataState* pState);
  int32_t (*streamStateInitTsDataState)(STableTsDataState** ppTsDataState, int8_t pkType, int32_t pkLen, void* pState, void* pOtherState);
  void (*streamStateDestroyTsDataState)(STableTsDataState* pTsDataState);
  int32_t (*streamStateRecoverTsData)(STableTsDataState* pTsDataState);
  int32_t (*streamStateReloadTsDataState)(STableTsDataState* pTsDataState);
  int32_t (*streamStateMergeAndSaveScanRange)(STableTsDataState* pTsDataState, STimeWindow* pWin, uint64_t gpId,
                                              SRecDataInfo* pRecData, int32_t len);
  int32_t (*streamStateMergeAllScanRange)(STableTsDataState* pTsDataState);
  int32_t (*streamStatePopScanRange)(STableTsDataState* pTsDataState, SScanRange* pRange);

  SStreamStateCur* (*streamStateGetLastStateCur)(SStreamState* pState);
  void (*streamStateLastStateCurNext)(SStreamStateCur* pCur);
  int32_t (*streamStateNLastStateGetKVByCur)(SStreamStateCur* pCur, int32_t num, SArray* pRes);
  SStreamStateCur* (*streamStateGetLastSessionStateCur)(SStreamState* pState);
  void (*streamStateLastSessionStateCurNext)(SStreamStateCur* pCur);
  int32_t (*streamStateNLastSessionStateGetKVByCur)(SStreamStateCur* pCur, int32_t num, SArray* pRes);
} SStateStore;

typedef struct SStorageAPI {
  SStoreMeta          metaFn;  // todo: refactor
  TsdReader           tsdReader;
  SStoreMetaReader    metaReaderFn;
  SStoreCacheReader   cacheFn;
  SStoreSnapshotFn    snapshotFn;
  SStoreTqReader      tqReaderFn;
  SStateStore         stateStore;
  SMetaDataFilterAPI  metaFilter;
  SFunctionStateStore functionStore;
} SStorageAPI;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STORAGEAPI_H
