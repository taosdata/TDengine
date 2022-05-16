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
#include "tmallocator.h"
#include "tmsg.h"
#include "trow.h"

#include "tdb.h"

#ifdef __cplusplus
extern "C" {
#endif

// vnode
typedef struct SVnode    SVnode;
typedef struct STsdbCfg  STsdbCfg;  // todo: remove
typedef struct SVnodeCfg SVnodeCfg;

extern const SVnodeCfg vnodeCfgDefault;

int     vnodeInit(int nthreads);
void    vnodeCleanup();
int     vnodeCreate(const char *path, SVnodeCfg *pCfg, STfs *pTfs);
void    vnodeDestroy(const char *path, STfs *pTfs);
SVnode *vnodeOpen(const char *path, STfs *pTfs, SMsgCb msgCb);
void    vnodeClose(SVnode *pVnode);
int     vnodePreprocessWriteReqs(SVnode *pVnode, SArray *pMsgs, int64_t *version);
int     vnodeProcessWriteReq(SVnode *pVnode, SRpcMsg *pMsg, int64_t version, SRpcMsg *pRsp);
int     vnodeProcessCMsg(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp);
int     vnodeProcessSyncReq(SVnode *pVnode, SRpcMsg *pMsg, SRpcMsg **pRsp);
int     vnodeProcessQueryMsg(SVnode *pVnode, SRpcMsg *pMsg);
int     vnodeProcessFetchMsg(SVnode *pVnode, SRpcMsg *pMsg, SQueueInfo *pInfo);
int32_t vnodeGetLoad(SVnode *pVnode, SVnodeLoad *pLoad);
int     vnodeValidateTableHash(SVnode *pVnode, char *tableFName);

int32_t vnodeStart(SVnode *pVnode);
void    vnodeStop(SVnode *pVnode);

int64_t vnodeGetSyncHandle(SVnode *pVnode);
void    vnodeGetSnapshot(SVnode *pVnode, SSnapshot *pSnapshot);
void    vnodeGetInfo(SVnode *pVnode, const char **dbname, int32_t *vgId);

// meta
typedef struct SMeta       SMeta;  // todo: remove
typedef struct SMetaReader SMetaReader;
typedef struct SMetaEntry  SMetaEntry;

void        metaReaderInit(SMetaReader *pReader, SMeta *pMeta, int32_t flags);
void        metaReaderClear(SMetaReader *pReader);
int         metaGetTableEntryByUid(SMetaReader *pReader, tb_uid_t uid);
int         metaReadNext(SMetaReader *pReader);
const void *metaGetTableTagVal(SMetaEntry *pEntry, int16_t cid);

#if 1  // refact APIs below (TODO)
typedef SVCreateTbReq   STbCfg;
typedef SVCreateTSmaReq SSmaCfg;

typedef struct SMTbCursor SMTbCursor;

SMTbCursor *metaOpenTbCursor(SMeta *pMeta);
void        metaCloseTbCursor(SMTbCursor *pTbCur);
int         metaTbCursorNext(SMTbCursor *pTbCur);
#endif

// tsdb
// typedef struct STsdb STsdb;
typedef void *tsdbReaderT;

#define BLOCK_LOAD_OFFSET_SEQ_ORDER 1
#define BLOCK_LOAD_TABLE_SEQ_ORDER  2
#define BLOCK_LOAD_TABLE_RR_ORDER   3

tsdbReaderT *tsdbQueryTables(SVnode *pVnode, SQueryTableDataCond *pCond, STableGroupInfo *tableInfoGroup, uint64_t qId,
                             uint64_t taskId);
tsdbReaderT  tsdbQueryCacheLast(SVnode *pVnode, SQueryTableDataCond *pCond, STableGroupInfo *groupList, uint64_t qId,
                                void *pMemRef);
int32_t      tsdbGetFileBlocksDistInfo(tsdbReaderT *pReader, STableBlockDistInfo *pTableBlockInfo);
bool         isTsdbCacheLastRow(tsdbReaderT *pReader);
int32_t      tsdbQuerySTableByTagCond(void *pMeta, uint64_t uid, TSKEY skey, const char *pTagCond, size_t len,
                                      int16_t tagNameRelType, const char *tbnameCond, STableGroupInfo *pGroupInfo,
                                      SColIndex *pColIndex, int32_t numOfCols, uint64_t reqId, uint64_t taskId);
int64_t      tsdbGetNumOfRowsInMemTable(tsdbReaderT *pHandle);
bool         tsdbNextDataBlock(tsdbReaderT pTsdbReadHandle);
void         tsdbRetrieveDataBlockInfo(tsdbReaderT *pTsdbReadHandle, SDataBlockInfo *pBlockInfo);
int32_t tsdbRetrieveDataBlockStatisInfo(tsdbReaderT *pTsdbReadHandle, SColumnDataAgg ***pBlockStatis, bool *allHave);
SArray *tsdbRetrieveDataBlock(tsdbReaderT *pTsdbReadHandle, SArray *pColumnIdList);
void    tsdbResetReadHandle(tsdbReaderT queryHandle, SQueryTableDataCond *pCond);
void    tsdbDestroyTableGroup(STableGroupInfo *pGroupList);
int32_t tsdbGetOneTableGroup(void *pMeta, uint64_t uid, TSKEY startKey, STableGroupInfo *pGroupInfo);
int32_t tsdbGetTableGroupFromIdList(SVnode *pVnode, SArray *pTableIdList, STableGroupInfo *pGroupInfo);
void    tsdbCleanupReadHandle(tsdbReaderT queryHandle);

// tq

typedef struct STqReadHandle STqReadHandle;

STqReadHandle *tqInitSubmitMsgScanner(SMeta *pMeta);

void    tqReadHandleSetColIdList(STqReadHandle *pReadHandle, SArray *pColIdList);
int     tqReadHandleSetTbUidList(STqReadHandle *pHandle, const SArray *tbUidList);
int     tqReadHandleAddTbUidList(STqReadHandle *pHandle, const SArray *tbUidList);
int32_t tqReadHandleSetMsg(STqReadHandle *pHandle, SSubmitReq *pMsg, int64_t ver);
bool    tqNextDataBlock(STqReadHandle *pHandle);
int32_t tqRetrieveDataBlock(SArray **ppCols, STqReadHandle *pHandle, uint64_t *pGroupId, uint64_t *pUid,
                            int32_t *pNumOfRows, int16_t *pNumOfCols);

// need to reposition

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

struct SVnodeCfg {
  int32_t  vgId;
  char     dbname[TSDB_DB_FNAME_LEN];
  uint64_t dbId;
  int32_t  szPage;
  int32_t  szCache;
  uint64_t szBuf;
  bool     isHeap;
  bool     isWeak;
  STsdbCfg tsdbCfg;
  SWalCfg  walCfg;
  SSyncCfg syncCfg;
  uint32_t hashBegin;
  uint32_t hashEnd;
  int8_t   hashMethod;
};

typedef struct {
  TSKEY    lastKey;
  uint64_t uid;
} STableKeyInfo;

struct SMetaEntry {
  int64_t     version;
  int8_t      type;
  tb_uid_t    uid;
  const char *name;
  union {
    struct {
      SSchemaWrapper schema;
      SSchemaWrapper schemaTag;
    } stbEntry;
    struct {
      int64_t        ctime;
      int32_t        ttlDays;
      tb_uid_t       suid;
      const uint8_t *pTags;
    } ctbEntry;
    struct {
      int64_t        ctime;
      int32_t        ttlDays;
      int32_t        ncid;  // next column id
      SSchemaWrapper schema;
    } ntbEntry;
    struct {
      STSma *tsma;
    } smaEntry;
  };
};

struct SMetaReader {
  int32_t    flags;
  SMeta     *pMeta;
  SDecoder   coder;
  SMetaEntry me;
  void      *pBuf;
  int        szBuf;
};

struct SMTbCursor {
  TDBC       *pDbc;
  void       *pKey;
  void       *pVal;
  int         kLen;
  int         vLen;
  SMetaReader mr;
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_H_*/
