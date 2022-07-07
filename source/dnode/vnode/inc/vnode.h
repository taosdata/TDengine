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
#include "tmsg.h"
#include "trow.h"

#include "tdb.h"

#ifdef __cplusplus
extern "C" {
#endif

// vnode
typedef struct SVnode           SVnode;
typedef struct STsdbCfg         STsdbCfg;  // todo: remove
typedef struct SVnodeCfg        SVnodeCfg;
typedef struct SVSnapshotReader SVSnapshotReader;

extern const SVnodeCfg vnodeCfgDefault;

int32_t vnodeInit(int32_t nthreads);
void    vnodeCleanup();
int32_t vnodeCreate(const char *path, SVnodeCfg *pCfg, STfs *pTfs);
void    vnodeDestroy(const char *path, STfs *pTfs);
SVnode *vnodeOpen(const char *path, STfs *pTfs, SMsgCb msgCb);
void    vnodeClose(SVnode *pVnode);

int32_t vnodeStart(SVnode *pVnode);
void    vnodeStop(SVnode *pVnode);
int64_t vnodeGetSyncHandle(SVnode *pVnode);
void    vnodeGetSnapshot(SVnode *pVnode, SSnapshot *pSnapshot);
void    vnodeGetInfo(SVnode *pVnode, const char **dbname, int32_t *vgId);
int32_t vnodeSnapshotReaderOpen(SVnode *pVnode, SVSnapshotReader **ppReader, int64_t sver, int64_t ever);
int32_t vnodeSnapshotReaderClose(SVSnapshotReader *pReader);
int32_t vnodeSnapshotRead(SVSnapshotReader *pReader, const void **ppData, uint32_t *nData);

int32_t vnodeProcessCreateTSma(SVnode *pVnode, void *pCont, uint32_t contLen);
int32_t vnodeGetAllTableList(SVnode *pVnode, uint64_t uid, SArray *list);
int32_t vnodeGetCtbIdList(SVnode *pVnode, int64_t suid, SArray *list);
void   *vnodeGetIdx(SVnode *pVnode);
void   *vnodeGetIvtIdx(SVnode *pVnode);

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

// meta
typedef struct SMeta       SMeta;  // todo: remove
typedef struct SMetaReader SMetaReader;
typedef struct SMetaEntry  SMetaEntry;

void        metaReaderInit(SMetaReader *pReader, SMeta *pMeta, int32_t flags);
void        metaReaderClear(SMetaReader *pReader);
int32_t     metaGetTableEntryByUid(SMetaReader *pReader, tb_uid_t uid);
int32_t     metaReadNext(SMetaReader *pReader);
const void *metaGetTableTagVal(SMetaEntry *pEntry, int16_t type, STagVal *tagVal);

typedef struct SMetaFltParam {
  tb_uid_t suid;
  int16_t  cid;
  int16_t  type;
  char    *val;
  bool     reverse;
  int (*filterFunc)(void *a, void *b, int16_t type);

} SMetaFltParam;

int32_t metaFilteTableIds(SMeta *pMeta, SMetaFltParam *param, SArray *results);

#if 1  // refact APIs below (TODO)
typedef SVCreateTbReq   STbCfg;
typedef SVCreateTSmaReq SSmaCfg;

typedef struct SMTbCursor SMTbCursor;

SMTbCursor *metaOpenTbCursor(SMeta *pMeta);
void        metaCloseTbCursor(SMTbCursor *pTbCur);
int32_t     metaTbCursorNext(SMTbCursor *pTbCur);
#endif

// tsdb
// typedef struct STsdb STsdb;
typedef struct STsdbReader STsdbReader;

#define BLOCK_LOAD_OFFSET_ORDER   1
#define BLOCK_LOAD_TABLESEQ_ORDER 2
#define BLOCK_LOAD_EXTERN_ORDER   3

#define LASTROW_RETRIEVE_TYPE_ALL    0x1
#define LASTROW_RETRIEVE_TYPE_SINGLE 0x2

int32_t tsdbSetTableId(STsdbReader *pReader, int64_t uid);
int32_t tsdbReaderOpen(SVnode *pVnode, SQueryTableDataCond *pCond, SArray *pTableList, STsdbReader **ppReader,
                       const char *idstr);
void    tsdbReaderClose(STsdbReader *pReader);
bool    tsdbNextDataBlock(STsdbReader *pReader);
void    tsdbRetrieveDataBlockInfo(STsdbReader *pReader, SDataBlockInfo *pDataBlockInfo);
int32_t tsdbRetrieveDatablockSMA(STsdbReader *pReader, SColumnDataAgg ***pBlockStatis, bool *allHave);
SArray *tsdbRetrieveDataBlock(STsdbReader *pTsdbReadHandle, SArray *pColumnIdList);
int32_t tsdbReaderReset(STsdbReader *pReader, SQueryTableDataCond *pCond, int32_t tWinIdx);
int32_t tsdbGetFileBlocksDistInfo(STsdbReader *pReader, STableBlockDistInfo *pTableBlockInfo);
int64_t tsdbGetNumOfRowsInMemTable(STsdbReader *pHandle);

int32_t tsdbLastRowReaderOpen(void *pVnode, int32_t type, SArray *pTableIdList, int32_t *colId, int32_t numOfCols,
                              void **pReader);
int32_t tsdbRetrieveLastRow(void *pReader, SSDataBlock *pResBlock, const int32_t *slotIds);
int32_t tsdbLastrowReaderClose(void *pReader);

// tq

typedef struct STqReadHandle SStreamReader;

SStreamReader *tqInitSubmitMsgScanner(SMeta *pMeta);

void    tqReadHandleSetColIdList(SStreamReader *pReadHandle, SArray *pColIdList);
int32_t tqReadHandleSetTbUidList(SStreamReader *pHandle, const SArray *tbUidList);
int32_t tqReadHandleAddTbUidList(SStreamReader *pHandle, const SArray *tbUidList);
int32_t tqReadHandleRemoveTbUidList(SStreamReader *pHandle, const SArray *tbUidList);

int32_t tqReadHandleSetMsg(SStreamReader *pHandle, SSubmitReq *pMsg, int64_t ver);
bool    tqNextDataBlock(SStreamReader *pHandle);
bool    tqNextDataBlockFilterOut(SStreamReader *pHandle, SHashObj *filterOutUids);
int32_t tqRetrieveDataBlock(SSDataBlock *pBlock, SStreamReader *pHandle);

// sma
int32_t smaGetTSmaDays(SVnodeCfg *pCfg, void *pCont, uint32_t contLen, int32_t *days);

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
  int8_t   isTsma;
  int8_t   isRsma;
  int8_t   hashMethod;
  int8_t   standby;
  STsdbCfg tsdbCfg;
  SWalCfg  walCfg;
  SSyncCfg syncCfg;
  uint32_t hashBegin;
  uint32_t hashEnd;
};

typedef struct {
  TSKEY    lastKey;
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
