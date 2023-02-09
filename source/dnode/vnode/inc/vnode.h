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

#include "tcommon.h"
#include "tfs.h"
#include "tgrant.h"
#include "tmsg.h"
#include "trow.h"

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
int32_t vnodeCreate(const char *path, SVnodeCfg *pCfg, STfs *pTfs);
int32_t vnodeAlter(const char *path, SAlterVnodeReplicaReq *pReq, STfs *pTfs);
void    vnodeDestroy(const char *path, STfs *pTfs);
SVnode *vnodeOpen(const char *path, STfs *pTfs, SMsgCb msgCb);
void    vnodePreClose(SVnode *pVnode);
void    vnodePostClose(SVnode *pVnode);
void    vnodeSyncCheckTimeout(SVnode *pVnode);
void    vnodeClose(SVnode *pVnode);

int32_t vnodeStart(SVnode *pVnode);
void    vnodeStop(SVnode *pVnode);
int64_t vnodeGetSyncHandle(SVnode *pVnode);
void    vnodeGetSnapshot(SVnode *pVnode, SSnapshot *pSnapshot);
void    vnodeGetInfo(SVnode *pVnode, const char **dbname, int32_t *vgId);
int32_t vnodeProcessCreateTSma(SVnode *pVnode, void *pCont, uint32_t contLen);
int32_t vnodeGetAllTableList(SVnode *pVnode, uint64_t uid, SArray *list);

int32_t vnodeGetCtbIdList(SVnode *pVnode, int64_t suid, SArray *list);
int32_t vnodeGetCtbIdListByFilter(SVnode *pVnode, int64_t suid, SArray *list, bool (*filter)(void *arg), void *arg);
int32_t vnodeGetStbIdList(SVnode *pVnode, int64_t suid, SArray *list);
void   *vnodeGetIdx(SVnode *pVnode);
void   *vnodeGetIvtIdx(SVnode *pVnode);

int32_t vnodeGetCtbNum(SVnode *pVnode, int64_t suid, int64_t *num);
int32_t vnodeGetTimeSeriesNum(SVnode *pVnode, int64_t *num);
int32_t vnodeGetAllCtbNum(SVnode *pVnode, int64_t *num);

void    vnodeResetLoad(SVnode *pVnode, SVnodeLoad *pLoad);
int32_t vnodeGetLoad(SVnode *pVnode, SVnodeLoad *pLoad);
int32_t vnodeValidateTableHash(SVnode *pVnode, char *tableFName);

int32_t vnodePreProcessWriteMsg(SVnode *pVnode, SRpcMsg *pMsg);
int32_t vnodePreprocessQueryMsg(SVnode *pVnode, SRpcMsg *pMsg);

int32_t vnodeProcessWriteMsg(SVnode *pVnode, SRpcMsg *pMsg, int64_t version, SRpcMsg *pRsp);
int32_t vnodeProcessSyncMsg(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp);
int32_t vnodeProcessQueryMsg(SVnode *pVnode, SRpcMsg *pMsg);
int32_t vnodeProcessFetchMsg(SVnode *pVnode, SRpcMsg *pMsg, SQueueInfo *pInfo);
void    vnodeProposeWriteMsg(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs);
void    vnodeApplyWriteMsg(SQueueInfo *pInfo, STaosQall *qall, int32_t numOfMsgs);
void    vnodeProposeCommitOnNeed(SVnode *pVnode);

// meta
typedef struct SMeta       SMeta;  // todo: remove
typedef struct SMetaReader SMetaReader;
typedef struct SMetaEntry  SMetaEntry;

#define META_READER_NOLOCK 0x1

void        metaReaderInit(SMetaReader *pReader, SMeta *pMeta, int32_t flags);
void        metaReaderReleaseLock(SMetaReader *pReader);
void        metaReaderClear(SMetaReader *pReader);
int32_t     metaGetTableEntryByUid(SMetaReader *pReader, tb_uid_t uid);
int32_t     metaGetTableEntryByUidCache(SMetaReader *pReader, tb_uid_t uid);
int         metaGetTableEntryByName(SMetaReader *pReader, const char *name);
int32_t     metaGetTableTags(SMeta *pMeta, uint64_t suid, SArray *uidList, SHashObj *tags);
int32_t     metaGetTableTagsByUids(SMeta *pMeta, int64_t suid, SArray *uidList, SHashObj *tags);
int32_t     metaReadNext(SMetaReader *pReader);
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
int64_t  metaGetTbNum(SMeta *pMeta);
int64_t  metaGetNtbNum(SMeta *pMeta);
typedef struct {
  int64_t uid;
  int64_t ctbNum;
} SMetaStbStats;
int32_t metaGetStbStats(SMeta *pMeta, int64_t uid, SMetaStbStats *pInfo);

typedef struct SMetaFltParam {
  tb_uid_t suid;
  int16_t  cid;
  int16_t  type;
  void    *val;
  bool     reverse;
  int (*filterFunc)(void *a, void *b, int16_t type);

} SMetaFltParam;

// TODO, refactor later
int32_t metaFilterTableIds(SMeta *pMeta, SMetaFltParam *param, SArray *results);
int32_t metaFilterCreateTime(SMeta *pMeta, SMetaFltParam *parm, SArray *pUids);
int32_t metaFilterTableName(SMeta *pMeta, SMetaFltParam *param, SArray *pUids);
int32_t metaFilterTtl(SMeta *pMeta, SMetaFltParam *param, SArray *pUids);

#if 1  // refact APIs below (TODO)
typedef SVCreateTbReq   STbCfg;
typedef SVCreateTSmaReq SSmaCfg;

typedef struct SMTbCursor SMTbCursor;

SMTbCursor *metaOpenTbCursor(SMeta *pMeta);
void        metaCloseTbCursor(SMTbCursor *pTbCur);
int32_t     metaTbCursorNext(SMTbCursor *pTbCur);
int32_t     metaTbCursorPrev(SMTbCursor *pTbCur);

#endif

// tsdb
// typedef struct STsdb STsdb;
typedef struct STsdbReader STsdbReader;

#define TSDB_DEFAULT_STT_FILE  8
#define TSDB_DEFAULT_PAGE_SIZE 4096

#define TIMEWINDOW_RANGE_CONTAINED 1
#define TIMEWINDOW_RANGE_EXTERNAL  2

#define CACHESCAN_RETRIEVE_TYPE_ALL    0x1
#define CACHESCAN_RETRIEVE_TYPE_SINGLE 0x2
#define CACHESCAN_RETRIEVE_LAST_ROW    0x4
#define CACHESCAN_RETRIEVE_LAST        0x8

int32_t tsdbSetTableList(STsdbReader *pReader, const void *pTableList, int32_t num);
int32_t tsdbReaderOpen(SVnode *pVnode, SQueryTableDataCond *pCond, void *pTableList, int32_t numOfTables,
                       SSDataBlock *pResBlock, STsdbReader **ppReader, const char *idstr);

void         tsdbReaderClose(STsdbReader *pReader);
bool         tsdbNextDataBlock(STsdbReader *pReader);
int32_t      tsdbRetrieveDatablockSMA(STsdbReader *pReader, SSDataBlock *pDataBlock, bool *allHave);
SSDataBlock *tsdbRetrieveDataBlock(STsdbReader *pTsdbReadHandle, SArray *pColumnIdList);
int32_t      tsdbReaderReset(STsdbReader *pReader, SQueryTableDataCond *pCond);
int32_t      tsdbGetFileBlocksDistInfo(STsdbReader *pReader, STableBlockDistInfo *pTableBlockInfo);
int64_t      tsdbGetNumOfRowsInMemTable(STsdbReader *pHandle);
void        *tsdbGetIdx(SMeta *pMeta);
void        *tsdbGetIvtIdx(SMeta *pMeta);
uint64_t     getReaderMaxVersion(STsdbReader *pReader);

int32_t tsdbCacherowsReaderOpen(void *pVnode, int32_t type, void *pTableIdList, int32_t numOfTables, int32_t numOfCols,
                                uint64_t suid, void **pReader, const char *idstr);
int32_t tsdbRetrieveCacheRows(void *pReader, SSDataBlock *pResBlock, const int32_t *slotIds, SArray *pTableUids);
void   *tsdbCacherowsReaderClose(void *pReader);
int32_t tsdbGetTableSchema(SVnode *pVnode, int64_t uid, STSchema **pSchema, int64_t *suid);

void   tsdbCacheSetCapacity(SVnode *pVnode, size_t capacity);
size_t tsdbCacheGetCapacity(SVnode *pVnode);
size_t tsdbCacheGetUsage(SVnode *pVnode);

// tq
typedef struct SMetaTableInfo {
  int64_t         suid;
  int64_t         uid;
  SSchemaWrapper *schema;
  char            tbName[TSDB_TABLE_NAME_LEN];
} SMetaTableInfo;

typedef struct SIdInfo {
  int64_t version;
  int32_t index;
} SIdInfo;

typedef struct SSnapContext {
  SMeta    *pMeta;
  int64_t   snapVersion;
  TBC      *pCur;
  int64_t   suid;
  int8_t    subType;
  SHashObj *idVersion;
  SHashObj *suidInfo;
  SArray   *idList;
  int32_t   index;
  bool      withMeta;
  bool      queryMetaOrData;  // true-get meta, false-get data
} SSnapContext;

typedef struct STqReader {
  int64_t           ver;
  const SSubmitReq *pMsg;
  SSubmitBlk       *pBlock;
  SSubmitMsgIter    msgIter;
  SSubmitBlkIter    blkIter;

  SWalReader *pWalReader;

  SMeta    *pVnodeMeta;
  SHashObj *tbIdHash;
  SArray   *pColIdList;  // SArray<int16_t>

  int32_t         cachedSchemaVer;
  int64_t         cachedSchemaSuid;
  SSchemaWrapper *pSchemaWrapper;
  STSchema       *pSchema;
} STqReader;

STqReader *tqOpenReader(SVnode *pVnode);
void       tqCloseReader(STqReader *);

void    tqReaderSetColIdList(STqReader *pReader, SArray *pColIdList);
int32_t tqReaderSetTbUidList(STqReader *pReader, const SArray *tbUidList);
int32_t tqReaderAddTbUidList(STqReader *pReader, const SArray *tbUidList);
int32_t tqReaderRemoveTbUidList(STqReader *pReader, const SArray *tbUidList);

int32_t tqSeekVer(STqReader *pReader, int64_t ver);
int32_t tqNextBlock(STqReader *pReader, SFetchRet *ret);

int32_t tqReaderSetDataMsg(STqReader *pReader, const SSubmitReq *pMsg, int64_t ver);
bool    tqNextDataBlock(STqReader *pReader);
bool    tqNextDataBlockFilterOut(STqReader *pReader, SHashObj *filterOutUids);
int32_t tqRetrieveDataBlock(SSDataBlock *pBlock, STqReader *pReader);
int32_t tqRetrieveTaosxBlock(STqReader *pReader, SArray *blocks, SArray *schemas);

int32_t vnodeEnqueueStreamMsg(SVnode *pVnode, SRpcMsg *pMsg);

// sma
int32_t smaGetTSmaDays(SVnodeCfg *pCfg, void *pCont, uint32_t contLen, int32_t *days);

// SVSnapReader
int32_t vnodeSnapReaderOpen(SVnode *pVnode, int64_t sver, int64_t ever, SVSnapReader **ppReader);
void    vnodeSnapReaderClose(SVSnapReader *pReader);
int32_t vnodeSnapRead(SVSnapReader *pReader, uint8_t **ppData, uint32_t *nData);
// SVSnapWriter
int32_t vnodeSnapWriterOpen(SVnode *pVnode, int64_t sver, int64_t ever, SVSnapWriter **ppWriter);
int32_t vnodeSnapWriterClose(SVSnapWriter *pWriter, int8_t rollback, SSnapshot *pSnapshot);
int32_t vnodeSnapWrite(SVSnapWriter *pWriter, uint8_t *pData, uint32_t nData);

int32_t        buildSnapContext(SMeta *pMeta, int64_t snapVersion, int64_t suid, int8_t subType, bool withMeta,
                                SSnapContext **ctxRet);
int32_t        getMetafromSnapShot(SSnapContext *ctx, void **pBuf, int32_t *contLen, int16_t *type, int64_t *uid);
SMetaTableInfo getUidfromSnapShot(SSnapContext *ctx);
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
  SRetention retentions[TSDB_RETENTION_MAX];
};

typedef struct {
  int64_t numOfSTables;
  int64_t numOfCTables;
  int64_t numOfNTables;
  int64_t numOfNTimeSeries;
  int64_t numOfTimeSeries;
  int64_t itvTimeSeries;
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
  int16_t     sttTrigger;
  int16_t     hashPrefix;
  int16_t     hashSuffix;
  int32_t     tsdbPageSize;
};

typedef struct {
  uint64_t uid;
  uint64_t groupId;
} STableKeyInfo;

#define TABLE_ROLLUP_ON       ((int8_t)0x1)
#define TABLE_IS_ROLLUP(FLG)  (((FLG) & (TABLE_ROLLUP_ON)) != 0)
#define TABLE_SET_ROLLUP(FLG) ((FLG) |= TABLE_ROLLUP_ON)
struct SMetaEntry {
  int64_t  version;
  int8_t   type;
  int8_t   flags;  // TODO: need refactor?
  tb_uid_t uid;
  char    *name;
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
      char    *comment;
      tb_uid_t suid;
      uint8_t *pTags;
    } ctbEntry;
    struct {
      int64_t        ctime;
      int32_t        ttlDays;
      int32_t        commentLen;
      char          *comment;
      int32_t        ncid;  // next column id
      SSchemaWrapper schemaRow;
    } ntbEntry;
    struct {
      STSma *tsma;
    } smaEntry;
  };

  uint8_t *pBuf;
};

struct SMetaReader {
  int32_t    flags;
  SMeta     *pMeta;
  SDecoder   coder;
  SMetaEntry me;
  void      *pBuf;
  int32_t    szBuf;
};

struct SMTbCursor {
  TBC        *pDbc;
  void       *pKey;
  void       *pVal;
  int32_t     kLen;
  int32_t     vLen;
  SMetaReader mr;
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_H_*/
