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

#include "tcommon.h"
#include "tfs.h"
#include "tmallocator.h"
#include "tmsg.h"
#include "trow.h"

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
int32_t vnodeAlter(SVnode *pVnode, const SVnodeCfg *pCfg);
int32_t vnodeCompact(SVnode *pVnode);
int32_t vnodeSync(SVnode *pVnode);
int32_t vnodeGetLoad(SVnode *pVnode, SVnodeLoad *pLoad);
int     vnodeValidateTableHash(SVnodeCfg *pVnodeOptions, char *tableFName);

// meta
typedef struct SMeta      SMeta;  // todo: remove
typedef struct SMTbCursor SMTbCursor;

typedef SVCreateTbReq   STbCfg;
typedef SVCreateTSmaReq SSmaCfg;

SMTbCursor *metaOpenTbCursor(SMeta *pMeta);
void        metaCloseTbCursor(SMTbCursor *pTbCur);
char       *metaTbCursorNext(SMTbCursor *pTbCur);

// tsdb
typedef struct STsdb          STsdb;
typedef struct STsdbQueryCond STsdbQueryCond;
typedef void                 *tsdbReaderT;

#define BLOCK_LOAD_OFFSET_SEQ_ORDER 1
#define BLOCK_LOAD_TABLE_SEQ_ORDER  2
#define BLOCK_LOAD_TABLE_RR_ORDER   3

tsdbReaderT *tsdbQueryTables(STsdb *tsdb, STsdbQueryCond *pCond, STableGroupInfo *tableInfoGroup, uint64_t qId,
                             uint64_t taskId);
tsdbReaderT  tsdbQueryCacheLast(STsdb *tsdb, STsdbQueryCond *pCond, STableGroupInfo *groupList, uint64_t qId,
                                void *pMemRef);
int32_t      tsdbGetFileBlocksDistInfo(tsdbReaderT *pReader, STableBlockDistInfo *pTableBlockInfo);
bool         isTsdbCacheLastRow(tsdbReaderT *pReader);
int32_t      tsdbQuerySTableByTagCond(void *pMeta, uint64_t uid, TSKEY skey, const char *pTagCond, size_t len,
                                      int16_t tagNameRelType, const char *tbnameCond, STableGroupInfo *pGroupInfo,
                                      SColIndex *pColIndex, int32_t numOfCols, uint64_t reqId, uint64_t taskId);
int64_t      tsdbGetNumOfRowsInMemTable(tsdbReaderT *pHandle);
bool         tsdbNextDataBlock(tsdbReaderT pTsdbReadHandle);
void         tsdbRetrieveDataBlockInfo(tsdbReaderT *pTsdbReadHandle, SDataBlockInfo *pBlockInfo);
int32_t      tsdbRetrieveDataBlockStatisInfo(tsdbReaderT *pTsdbReadHandle, SColumnDataAgg **pBlockStatis);
SArray      *tsdbRetrieveDataBlock(tsdbReaderT *pTsdbReadHandle, SArray *pColumnIdList);
void         tsdbDestroyTableGroup(STableGroupInfo *pGroupList);
int32_t      tsdbGetOneTableGroup(void *pMeta, uint64_t uid, TSKEY startKey, STableGroupInfo *pGroupInfo);
int32_t      tsdbGetTableGroupFromIdList(STsdb *tsdb, SArray *pTableIdList, STableGroupInfo *pGroupInfo);

// tq

typedef struct STqReadHandle STqReadHandle;

STqReadHandle *tqInitSubmitMsgScanner(SMeta *pMeta);

void    tqReadHandleSetColIdList(STqReadHandle *pReadHandle, SArray *pColIdList);
int     tqReadHandleSetTbUidList(STqReadHandle *pHandle, const SArray *tbUidList);
int     tqReadHandleAddTbUidList(STqReadHandle *pHandle, const SArray *tbUidList);
int32_t tqReadHandleSetMsg(STqReadHandle *pHandle, SSubmitReq *pMsg, int64_t ver);
bool    tqNextDataBlock(STqReadHandle *pHandle);
int     tqRetrieveDataBlockInfo(STqReadHandle *pHandle, SDataBlockInfo *pBlockInfo);
SArray *tqRetrieveDataBlock(STqReadHandle *pHandle);

// need to reposition

// structs
struct SMetaCfg {
  uint64_t lruSize;
};

struct STsdbCfg {
  int8_t   precision;
  int8_t   update;
  int8_t   compression;
  int8_t   slLevel;
  int32_t  days;
  int32_t  minRows;
  int32_t  maxRows;
  int32_t  keep2;
  int32_t  keep0;
  int32_t  keep1;
  uint64_t lruCacheSize;
  SArray  *retentions;
};

struct SVnodeCfg {
  int32_t  vgId;
  char     dbname[TSDB_DB_NAME_LEN];
  uint64_t dbId;
  int32_t  szPage;
  int32_t  szCache;
  uint64_t szBuf;
  bool     isHeap;
  uint32_t ttl;
  uint32_t keep;
  int8_t   streamMode;
  bool     isWeak;
  STsdbCfg tsdbCfg;
  SWalCfg  walCfg;
  uint32_t hashBegin;
  uint32_t hashEnd;
  int8_t   hashMethod;
};

struct STsdbQueryCond {
  STimeWindow  twindow;
  int32_t      order;  // desc|asc order to iterate the data block
  int32_t      numOfCols;
  SColumnInfo *colList;
  bool         loadExternalRows;  // load external rows or not
  int32_t      type;              // data block load type:
};

typedef struct {
  TSKEY    lastKey;
  uint64_t uid;
} STableKeyInfo;

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_H_*/
