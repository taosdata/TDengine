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

#include "tarray.h"
#include "tfs.h"
#include "wal.h"

#include "tmallocator.h"
#include "tmsg.h"
#include "trow.h"
#include "tmallocator.h"
#include "tcommon.h"
#include "tfs.h"

#ifdef __cplusplus
extern "C" {
#endif

/* ------------------------ TYPES EXPOSED ------------------------ */
typedef struct SMgmtWrapper SMgmtWrapper;
typedef struct SVnode       SVnode;

#define META_SUPER_TABLE  TD_SUPER_TABLE
#define META_CHILD_TABLE  TD_CHILD_TABLE
#define META_NORMAL_TABLE TD_NORMAL_TABLE

// Types exported
typedef struct SMeta SMeta;

typedef struct SMetaCfg {
  /// LRU cache size
  uint64_t lruSize;
} SMetaCfg;

typedef struct SMTbCursor  SMTbCursor;
typedef struct SMCtbCursor SMCtbCursor;
typedef struct SMSmaCursor SMSmaCursor;

typedef SVCreateTbReq   STbCfg;
typedef SVCreateTSmaReq SSmaCfg;

typedef struct SDataStatis {
  int16_t colId;
  int16_t maxIndex;
  int16_t minIndex;
  int16_t numOfNull;
  int64_t sum;
  int64_t max;
  int64_t min;
} SDataStatis;

typedef struct STsdbQueryCond {
  STimeWindow  twindow;
  int32_t      order;             // desc|asc order to iterate the data block
  int32_t      numOfCols;
  SColumnInfo *colList;
  bool         loadExternalRows;  // load external rows or not
  int32_t      type;              // data block load type:
} STsdbQueryCond;

typedef struct {
  TSKEY    lastKey;
  uint64_t uid;
} STableKeyInfo;


typedef struct STable {
  uint64_t  tid;
  uint64_t  uid;
  STSchema *pSchema;
} STable;

#define BLOCK_LOAD_OFFSET_SEQ_ORDER   1
#define BLOCK_LOAD_TABLE_SEQ_ORDER    2
#define BLOCK_LOAD_TABLE_RR_ORDER     3

#define TABLE_TID(t) (t)->tid
#define TABLE_UID(t) (t)->uid

// TYPES EXPOSED
typedef struct STsdb STsdb;

typedef struct STsdbCfg {
  int8_t   precision;
  int8_t   update;
  int8_t   compression;
  int32_t  daysPerFile;
  int32_t  minRowsPerFileBlock;
  int32_t  maxRowsPerFileBlock;
  int32_t  keep;
  int32_t  keep1;
  int32_t  keep2;
  uint64_t lruCacheSize;
  SArray  *retentions;
} STsdbCfg;


typedef struct {
  // TODO
  int32_t reserved;
} STqCfg;

typedef struct {
  int32_t  vgId;
  uint64_t dbId;
  STfs    *pTfs;
  uint64_t wsize;
  uint64_t ssize;
  uint64_t lsize;
  bool     isHeapAllocator;
  uint32_t ttl;
  uint32_t keep;
  int8_t   streamMode;
  bool     isWeak;
  STsdbCfg tsdbCfg;
  SMetaCfg metaCfg;
  STqCfg   tqCfg;
  SWalCfg  walCfg;
  SMsgCb   msgCb;
  uint32_t hashBegin;
  uint32_t hashEnd;
  int8_t   hashMethod;
} SVnodeCfg;

typedef struct {
  int64_t           ver;
  int64_t           tbUid;
  SHashObj         *tbIdHash;
  const SSubmitReq *pMsg;
  SSubmitBlk       *pBlock;
  SSubmitMsgIter    msgIter;
  SSubmitBlkIter    blkIter;
  SMeta            *pVnodeMeta;
  SArray           *pColIdList;  // SArray<int32_t>
  int32_t           sver;
  SSchemaWrapper   *pSchemaWrapper;
  STSchema         *pSchema;
} STqReadHandle;

/* ------------------------ SVnode ------------------------ */
/**
 * @brief Initialize the vnode module
 *
 * @return int 0 for success and -1 for failure
 */
int vnodeInit();

/**
 * @brief Cleanup the vnode module
 *
 */
void vnodeCleanup();

/**
 * @brief Open a VNODE.
 *
 * @param path path of the vnode
 * @param pVnodeCfg options of the vnode
 * @return SVnode* The vnode object
 */
SVnode *vnodeOpen(const char *path, const SVnodeCfg *pVnodeCfg);

/**
 * @brief Close a VNODE
 *
 * @param pVnode The vnode object to close
 */
void vnodeClose(SVnode *pVnode);

/**
 * @brief Destroy a VNODE.
 *
 * @param path Path of the VNODE.
 */
void vnodeDestroy(const char *path);

/**
 * @brief Process an array of write messages.
 *
 * @param pVnode The vnode object.
 * @param pMsgs The array of SRpcMsg
 */
void vnodeProcessWMsgs(SVnode *pVnode, SArray *pMsgs);

/**
 * @brief Apply a write request message.
 *
 * @param pVnode The vnode object.
 * @param pMsg The request message
 * @param pRsp The response message
 * @return int 0 for success, -1 for failure
 */
int vnodeApplyWMsg(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp);

/**
 * @brief Process a consume message.
 *
 * @param pVnode The vnode object.
 * @param pMsg The request message
 * @param pRsp The response message
 * @return int 0 for success, -1 for failure
 */
int vnodeProcessCMsg(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp);

/**
 * @brief Process the sync request
 *
 * @param pVnode
 * @param pMsg
 * @param pRsp
 * @return int
 */
int vnodeProcessSyncReq(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp);

/**
 * @brief Process a query message.
 *
 * @param pVnode The vnode object.
 * @param pMsg The request message
 * @return int 0 for success, -1 for failure
 */
int vnodeProcessQueryMsg(SVnode *pVnode, SRpcMsg *pMsg);

/**
 * @brief Process a fetch message.
 *
 * @param pVnode The vnode object.
 * @param pMsg The request message
 * @return int 0 for success, -1 for failure
 */
int vnodeProcessFetchMsg(SVnode *pVnode, SRpcMsg *pMsg, SQueueInfo *pInfo);

/* ------------------------ SVnodeCfg ------------------------ */
/**
 * @brief Initialize VNODE options.
 *
 * @param pOptions The options object to be initialized. It should not be NULL.
 */
void vnodeOptionsInit(SVnodeCfg *pOptions);

/**
 * @brief Clear VNODE options.
 *
 * @param pOptions Options to clear.
 */
void vnodeOptionsClear(SVnodeCfg *pOptions);

int vnodeValidateTableHash(SVnodeCfg *pVnodeOptions, char *tableFName);

/* ------------------------ FOR COMPILE ------------------------ */

int32_t vnodeAlter(SVnode *pVnode, const SVnodeCfg *pCfg);
int32_t vnodeCompact(SVnode *pVnode);
int32_t vnodeSync(SVnode *pVnode);
int32_t vnodeGetLoad(SVnode *pVnode, SVnodeLoad *pLoad);

/* ------------------------- TQ READ --------------------------- */

enum {
  TQ_STREAM_TOKEN__DATA = 1,
  TQ_STREAM_TOKEN__WATERMARK,
  TQ_STREAM_TOKEN__CHECKPOINT,
};

typedef struct {
  int8_t type;
  int8_t reserved[7];
  union {
    void   *data;
    int64_t wmTs;
    int64_t checkpointId;
  };
} STqStreamToken;

STqReadHandle *tqInitSubmitMsgScanner(SMeta *pMeta);

static FORCE_INLINE void tqReadHandleSetColIdList(STqReadHandle *pReadHandle, SArray *pColIdList) {
  pReadHandle->pColIdList = pColIdList;
}

// static FORCE_INLINE void tqReadHandleSetTbUid(STqReadHandle* pHandle, int64_t tbUid) {
// pHandle->tbUid = tbUid;
//}

static FORCE_INLINE int tqReadHandleSetTbUidList(STqReadHandle *pHandle, const SArray *tbUidList) {
  if (pHandle->tbIdHash) {
    taosHashClear(pHandle->tbIdHash);
  }

  pHandle->tbIdHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
  if (pHandle->tbIdHash == NULL) {
    terrno = TSDB_CODE_OUT_OF_MEMORY;
    return -1;
  }

  for (int i = 0; i < taosArrayGetSize(tbUidList); i++) {
    int64_t *pKey = (int64_t *)taosArrayGet(tbUidList, i);
    taosHashPut(pHandle->tbIdHash, pKey, sizeof(int64_t), NULL, 0);
  }

  return 0;
}

static FORCE_INLINE int tqReadHandleAddTbUidList(STqReadHandle *pHandle, const SArray *tbUidList) {
  if (pHandle->tbIdHash == NULL) {
    pHandle->tbIdHash = taosHashInit(64, taosGetDefaultHashFunction(TSDB_DATA_TYPE_BIGINT), true, HASH_NO_LOCK);
    if (pHandle->tbIdHash == NULL) {
      terrno = TSDB_CODE_OUT_OF_MEMORY;
      return -1;
    }
  }

  for (int i = 0; i < taosArrayGetSize(tbUidList); i++) {
    int64_t *pKey = (int64_t *)taosArrayGet(tbUidList, i);
    taosHashPut(pHandle->tbIdHash, pKey, sizeof(int64_t), NULL, 0);
  }

  return 0;
}

int32_t tqReadHandleSetMsg(STqReadHandle *pHandle, SSubmitReq *pMsg, int64_t ver);
bool    tqNextDataBlock(STqReadHandle *pHandle);
int     tqRetrieveDataBlockInfo(STqReadHandle *pHandle, SDataBlockInfo *pBlockInfo);
// return SArray<SColumnInfoData>
SArray *tqRetrieveDataBlock(STqReadHandle *pHandle);

// meta.h
SMeta          *metaOpen(const char *path, const SMetaCfg *pMetaCfg, SMemAllocatorFactory *pMAF);
void            metaClose(SMeta *pMeta);
void            metaRemove(const char *path);
int             metaCreateTable(SMeta *pMeta, STbCfg *pTbCfg);
int             metaDropTable(SMeta *pMeta, tb_uid_t uid);
int             metaCommit(SMeta *pMeta);
int32_t         metaCreateTSma(SMeta *pMeta, SSmaCfg *pCfg);
int32_t         metaDropTSma(SMeta *pMeta, int64_t indexUid);
STbCfg         *metaGetTbInfoByUid(SMeta *pMeta, tb_uid_t uid);
STbCfg         *metaGetTbInfoByName(SMeta *pMeta, char *tbname, tb_uid_t *uid);
SSchemaWrapper *metaGetTableSchema(SMeta *pMeta, tb_uid_t uid, int32_t sver, bool isinline);
STSchema       *metaGetTbTSchema(SMeta *pMeta, tb_uid_t uid, int32_t sver);
void           *metaGetSmaInfoByIndex(SMeta *pMeta, int64_t indexUid, bool isDecode);
STSmaWrapper   *metaGetSmaInfoByTable(SMeta *pMeta, tb_uid_t uid);
SArray         *metaGetSmaTbUids(SMeta *pMeta, bool isDup);
int             metaGetTbNum(SMeta *pMeta);
SMTbCursor     *metaOpenTbCursor(SMeta *pMeta);
void            metaCloseTbCursor(SMTbCursor *pTbCur);
char           *metaTbCursorNext(SMTbCursor *pTbCur);
SMCtbCursor    *metaOpenCtbCursor(SMeta *pMeta, tb_uid_t uid);
void            metaCloseCtbCurosr(SMCtbCursor *pCtbCur);
tb_uid_t        metaCtbCursorNext(SMCtbCursor *pCtbCur);

SMSmaCursor *metaOpenSmaCursor(SMeta *pMeta, tb_uid_t uid);
void         metaCloseSmaCursor(SMSmaCursor *pSmaCur);
int64_t      metaSmaCursorNext(SMSmaCursor *pSmaCur);

// Options
void metaOptionsInit(SMetaCfg *pMetaCfg);
void metaOptionsClear(SMetaCfg *pMetaCfg);

// query condition to build multi-table data block iterator
// STsdb
STsdb *tsdbOpen(const char *path, int32_t vgId, const STsdbCfg *pTsdbCfg, SMemAllocatorFactory *pMAF, SMeta *pMeta, STfs *pTfs);
void   tsdbClose(STsdb *);
void   tsdbRemove(const char *path);
int    tsdbInsertData(STsdb *pTsdb, SSubmitReq *pMsg, SSubmitRsp *pRsp);
int    tsdbPrepareCommit(STsdb *pTsdb);
int    tsdbCommit(STsdb *pTsdb);


int32_t tsdbInitSma(STsdb *pTsdb);
int32_t tsdbCreateTSma(STsdb *pTsdb, char *pMsg);
int32_t tsdbDropTSma(STsdb *pTsdb, char *pMsg);
/**
 * @brief When submit msg received, update the relative expired window synchronously.
 *
 * @param pTsdb
 * @param msg
 * @return int32_t
 */
int32_t tsdbUpdateSmaWindow(STsdb *pTsdb, SSubmitReq *pMsg);

/**
 * @brief Insert tSma(Time-range-wise SMA) data from stream computing engine
 *
 * @param pTsdb
 * @param indexUid
 * @param msg
 * @return int32_t
 */
int32_t tsdbInsertTSmaData(STsdb *pTsdb, int64_t indexUid, const char *msg);

/**
 * @brief Drop tSma data and local cache.
 * 
 * @param pTsdb 
 * @param indexUid 
 * @return int32_t 
 */
int32_t tsdbDropTSmaData(STsdb *pTsdb, int64_t indexUid);

/**
 * @brief Insert RSma(Rollup SMA) data.
 *
 * @param pTsdb
 * @param msg
 * @return int32_t
 */
int32_t tsdbInsertRSmaData(STsdb *pTsdb, char *msg);

// TODO: This is the basic params, and should wrap the params to a queryHandle.
/**
 * @brief Get tSma(Time-range-wise SMA) data.
 * 
 * @param pTsdb 
 * @param pData 
 * @param indexUid 
 * @param querySKey 
 * @param nMaxResult 
 * @return int32_t 
 */
int32_t tsdbGetTSmaData(STsdb *pTsdb, char *pData, int64_t indexUid, TSKEY querySKey, int32_t nMaxResult);

// STsdbCfg
int  tsdbOptionsInit(STsdbCfg *);
void tsdbOptionsClear(STsdbCfg *);

typedef void* tsdbReaderT;

/**
 * Get the data block iterator, starting from position according to the query condition
 *
 * @param tsdb       tsdb handle
 * @param pCond      query condition, including time window, result set order, and basic required columns for each block
 * @param tableInfoGroup  table object list in the form of set, grouped into different sets according to the
 *                        group by condition
 * @param qinfo      query info handle from query processor
 * @return
 */
tsdbReaderT *tsdbQueryTables(STsdb *tsdb, STsdbQueryCond *pCond, STableGroupInfo *tableInfoGroup, uint64_t qId, uint64_t taskId);

/**
 * Get the last row of the given query time window for all the tables in STableGroupInfo object.
 * Note that only one data block with only row will be returned while invoking retrieve data block function for
 * all tables in this group.
 *
 * @param tsdb   tsdb handle
 * @param pCond  query condition, including time window, result set order, and basic required columns for each block
 * @param tableInfo  table list.
 * @return
 */
//tsdbReaderT tsdbQueryLastRow(STsdbRepo *tsdb, STsdbQueryCond *pCond, STableGroupInfo *tableInfo, uint64_t qId,
//                                  SMemRef *pRef);


tsdbReaderT tsdbQueryCacheLast(STsdb *tsdb, STsdbQueryCond *pCond, STableGroupInfo *groupList, uint64_t qId, void* pMemRef);

int32_t tsdbGetFileBlocksDistInfo(tsdbReaderT* pReader, STableBlockDistInfo* pTableBlockInfo);

bool isTsdbCacheLastRow(tsdbReaderT* pReader);

/**
 *
 * @param tsdb
 * @param uid
 * @param skey
 * @param pTagCond
 * @param len
 * @param tagNameRelType
 * @param tbnameCond
 * @param pGroupInfo
 * @param pColIndex
 * @param numOfCols
 * @param reqId
 * @return
 */
int32_t tsdbQuerySTableByTagCond(void* pMeta, uint64_t uid, TSKEY skey, const char* pTagCond, size_t len,
                                 int16_t tagNameRelType, const char* tbnameCond, STableGroupInfo* pGroupInfo,
                                 SColIndex* pColIndex, int32_t numOfCols, uint64_t reqId, uint64_t taskId);
/**
 * get num of rows in mem table
 *
 * @param pHandle
 * @return row size
 */

int64_t tsdbGetNumOfRowsInMemTable(tsdbReaderT* pHandle);

/**
 * move to next block if exists
 *
 * @param pTsdbReadHandle
 * @return
 */
bool tsdbNextDataBlock(tsdbReaderT pTsdbReadHandle);

/**
 * Get current data block information
 *
 * @param pTsdbReadHandle
 * @param pBlockInfo
 * @return
 */
void tsdbRetrieveDataBlockInfo(tsdbReaderT *pTsdbReadHandle, SDataBlockInfo *pBlockInfo);

/**
 *
 * Get the pre-calculated information w.r.t. current data block.
 *
 * In case of data block in cache, the pBlockStatis will always be NULL.
 * If a block is not completed loaded from disk, the pBlockStatis will be NULL.

 * @pBlockStatis the pre-calculated value for current data blocks. if the block is a cache block, always return 0
 * @return
 */
int32_t tsdbRetrieveDataBlockStatisInfo(tsdbReaderT *pTsdbReadHandle, SDataStatis **pBlockStatis);

/**
 *
 * The query condition with primary timestamp is passed to iterator during its constructor function,
 * the returned data block must be satisfied with the time window condition in any cases,
 * which means the SData data block is not actually the completed disk data blocks.
 *
 * @param pTsdbReadHandle      query handle
 * @param pColumnIdList     required data columns id list
 * @return
 */
SArray *tsdbRetrieveDataBlock(tsdbReaderT *pTsdbReadHandle, SArray *pColumnIdList);

/**
 * destroy the created table group list, which is generated by tag query
 * @param pGroupList
 */
void tsdbDestroyTableGroup(STableGroupInfo *pGroupList);

/**
 * create the table group result including only one table, used to handle the normal table query
 *
 * @param tsdb        tsdbHandle
 * @param uid         table uid
 * @param pGroupInfo  the generated result
 * @return
 */
int32_t tsdbGetOneTableGroup(void *pMeta, uint64_t uid, TSKEY startKey, STableGroupInfo *pGroupInfo);

/**
 *
 * @param tsdb
 * @param pTableIdList
 * @param pGroupInfo
 * @return
 */
int32_t tsdbGetTableGroupFromIdList(STsdb *tsdb, SArray *pTableIdList, STableGroupInfo *pGroupInfo);

/**
 * clean up the query handle
 * @param queryHandle
 */
void tsdbCleanupReadHandle(tsdbReaderT queryHandle);

int32_t tdScanAndConvertSubmitMsg(SSubmitReq *pMsg);


#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_H_*/
