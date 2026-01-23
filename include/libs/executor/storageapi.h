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
      int64_t        ownerId;
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
      int64_t        ownerId;
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
  int32_t  (*tsdNextFirstLastTsBlock)(void *pIter, SSDataBlock *pRes, bool* hasNext);
  void     (*tsdDestroyFirstLastTsIter)(void *pIter);

  int32_t (*tsdReaderStepDone)(void *pReader, int64_t notifyTs);
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
  int32_t (*tqReaderNextBlockInWal)();
  int64_t (*tqGetResultBlockTime)();

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
  int32_t (*tqUpdateTableTagCache)(struct STqReader *, SExprInfo *, int32_t,  int64_t,  col_id_t);

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
  int32_t          recValueLen;
  SStreamStateCur* pRecCur;
  int32_t          cfgIndex;
  void*            pBatch;
  int32_t          batchBufflen;
  void*            pBatchBuff;
} STableTsDataState;

typedef struct SStorageAPI {
  SStoreMeta          metaFn;  // todo: refactor
  TsdReader           tsdReader;
  SStoreMetaReader    metaReaderFn;
  SStoreCacheReader   cacheFn;
  SStoreSnapshotFn    snapshotFn;
  SStoreTqReader      tqReaderFn;
  SMetaDataFilterAPI  metaFilter;
  SFunctionStateStore functionStore;
} SStorageAPI;

#ifdef __cplusplus
}
#endif

#endif  // TDENGINE_STORAGEAPI_H
