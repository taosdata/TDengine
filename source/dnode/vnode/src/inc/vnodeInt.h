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

#ifndef _TD_VNODE_DEF_H_
#define _TD_VNODE_DEF_H_

#include "executor.h"
#include "filter.h"
#include "qworker.h"
#include "sync.h"
#include "tRealloc.h"
#include "tchecksum.h"
#include "tcoding.h"
#include "tcompare.h"
#include "tcompression.h"
#include "tdatablock.h"
#include "tdb.h"
#include "tencode.h"
#include "tfs.h"
#include "tglobal.h"
#include "tjson.h"
#include "tlist.h"
#include "tlockfree.h"
#include "tlosertree.h"
#include "tlrucache.h"
#include "tmsgcb.h"
#include "trbtree.h"
#include "tref.h"
#include "tskiplist.h"
#include "tstream.h"
#include "ttime.h"
#include "ttimer.h"
#include "wal.h"

#include "vnode.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SVnodeInfo         SVnodeInfo;
typedef struct SMeta              SMeta;
typedef struct SSma               SSma;
typedef struct STsdb              STsdb;
typedef struct STQ                STQ;
typedef struct SVState            SVState;
typedef struct SVStatis           SVStatis;
typedef struct SVBufPool          SVBufPool;
typedef struct SQWorker           SQHandle;
typedef struct STsdbKeepCfg       STsdbKeepCfg;
typedef struct SMetaSnapReader    SMetaSnapReader;
typedef struct SMetaSnapWriter    SMetaSnapWriter;
typedef struct STsdbSnapReader    STsdbSnapReader;
typedef struct STsdbSnapWriter    STsdbSnapWriter;
typedef struct STqSnapReader      STqSnapReader;
typedef struct STqSnapWriter      STqSnapWriter;
typedef struct STqOffsetReader    STqOffsetReader;
typedef struct STqOffsetWriter    STqOffsetWriter;
typedef struct SStreamTaskReader  SStreamTaskReader;
typedef struct SStreamTaskWriter  SStreamTaskWriter;
typedef struct SStreamStateReader SStreamStateReader;
typedef struct SStreamStateWriter SStreamStateWriter;
typedef struct SRSmaSnapReader    SRSmaSnapReader;
typedef struct SRSmaSnapWriter    SRSmaSnapWriter;
typedef struct SSnapDataHdr       SSnapDataHdr;
typedef struct SCommitInfo        SCommitInfo;

#define VNODE_META_DIR  "meta"
#define VNODE_TSDB_DIR  "tsdb"
#define VNODE_TQ_DIR    "tq"
#define VNODE_WAL_DIR   "wal"
#define VNODE_TSMA_DIR  "tsma"
#define VNODE_RSMA_DIR  "rsma"
#define VNODE_RSMA0_DIR "tsdb"
#define VNODE_RSMA1_DIR "rsma1"
#define VNODE_RSMA2_DIR "rsma2"

#define VND_INFO_FNAME "vnode.json"

// vnd.h

void* vnodeBufPoolMalloc(SVBufPool* pPool, int size);
void* vnodeBufPoolMallocAligned(SVBufPool* pPool, int size);
void  vnodeBufPoolFree(SVBufPool* pPool, void* p);
void  vnodeBufPoolRef(SVBufPool* pPool);
void  vnodeBufPoolUnRef(SVBufPool* pPool);
int   vnodeDecodeInfo(uint8_t* pData, SVnodeInfo* pInfo);

// meta
typedef struct SMCtbCursor SMCtbCursor;
typedef struct SMStbCursor SMStbCursor;
typedef struct STbUidStore STbUidStore;

#define META_BEGIN_HEAP_BUFFERPOOL 0
#define META_BEGIN_HEAP_OS         1
#define META_BEGIN_HEAP_NIL        2

int             metaOpen(SVnode* pVnode, SMeta** ppMeta, int8_t rollback);
int             metaClose(SMeta* pMeta);
int             metaBegin(SMeta* pMeta, int8_t fromSys);
TXN*            metaGetTxn(SMeta* pMeta);
int             metaCommit(SMeta* pMeta, TXN* txn);
int             metaFinishCommit(SMeta* pMeta, TXN* txn);
int             metaPrepareAsyncCommit(SMeta* pMeta);
int             metaAbort(SMeta* pMeta);
int             metaCreateSTable(SMeta* pMeta, int64_t version, SVCreateStbReq* pReq);
int             metaAlterSTable(SMeta* pMeta, int64_t version, SVCreateStbReq* pReq);
int             metaDropSTable(SMeta* pMeta, int64_t verison, SVDropStbReq* pReq, SArray* tbUidList);
int             metaCreateTable(SMeta* pMeta, int64_t version, SVCreateTbReq* pReq, STableMetaRsp** pMetaRsp);
int             metaDropTable(SMeta* pMeta, int64_t version, SVDropTbReq* pReq, SArray* tbUids, int64_t* tbUid);
int             metaTtlDropTable(SMeta* pMeta, int64_t ttl, SArray* tbUids);
int             metaAlterTable(SMeta* pMeta, int64_t version, SVAlterTbReq* pReq, STableMetaRsp* pMetaRsp);
SSchemaWrapper* metaGetTableSchema(SMeta* pMeta, tb_uid_t uid, int32_t sver, int lock);
STSchema*       metaGetTbTSchema(SMeta* pMeta, tb_uid_t uid, int32_t sver, int lock);
int32_t         metaGetTbTSchemaEx(SMeta* pMeta, tb_uid_t suid, tb_uid_t uid, int32_t sver, STSchema** ppTSchema);
int             metaGetTableEntryByName(SMetaReader* pReader, const char* name);
int             metaAlterCache(SMeta* pMeta, int32_t nPage);

int64_t       metaGetTimeSeriesNum(SMeta* pMeta);
SMCtbCursor*  metaOpenCtbCursor(SMeta* pMeta, tb_uid_t uid, int lock);
void          metaCloseCtbCursor(SMCtbCursor* pCtbCur, int lock);
tb_uid_t      metaCtbCursorNext(SMCtbCursor* pCtbCur);
SMStbCursor*  metaOpenStbCursor(SMeta* pMeta, tb_uid_t uid);
void          metaCloseStbCursor(SMStbCursor* pStbCur);
tb_uid_t      metaStbCursorNext(SMStbCursor* pStbCur);
STSma*        metaGetSmaInfoByIndex(SMeta* pMeta, int64_t indexUid);
STSmaWrapper* metaGetSmaInfoByTable(SMeta* pMeta, tb_uid_t uid, bool deepCopy);
SArray*       metaGetSmaIdsByTable(SMeta* pMeta, tb_uid_t uid);
SArray*       metaGetSmaTbUids(SMeta* pMeta);
void*         metaGetIdx(SMeta* pMeta);
void*         metaGetIvtIdx(SMeta* pMeta);
int           metaTtlSmaller(SMeta* pMeta, uint64_t time, SArray* uidList);

int32_t metaCreateTSma(SMeta* pMeta, int64_t version, SSmaCfg* pCfg);
int32_t metaDropTSma(SMeta* pMeta, int64_t indexUid);

typedef struct SMetaInfo {
  int64_t uid;
  int64_t suid;
  int64_t version;
  int32_t skmVer;
} SMetaInfo;
int32_t metaGetInfo(SMeta* pMeta, int64_t uid, SMetaInfo* pInfo, SMetaReader* pReader);

// tsdb
int     tsdbOpen(SVnode* pVnode, STsdb** ppTsdb, const char* dir, STsdbKeepCfg* pKeepCfg, int8_t rollback);
int     tsdbClose(STsdb** pTsdb);
int32_t tsdbBegin(STsdb* pTsdb);
int32_t tsdbPrepareCommit(STsdb* pTsdb);
int32_t tsdbCommit(STsdb* pTsdb, SCommitInfo* pInfo);
int32_t tsdbFinishCommit(STsdb* pTsdb);
int32_t tsdbRollbackCommit(STsdb* pTsdb);
int32_t tsdbDoRetention(STsdb* pTsdb, int64_t now);
int     tsdbScanAndConvertSubmitMsg(STsdb* pTsdb, SSubmitReq2* pMsg);
int     tsdbInsertData(STsdb* pTsdb, int64_t version, SSubmitReq2* pMsg, SSubmitRsp2* pRsp);
int32_t tsdbInsertTableData(STsdb* pTsdb, int64_t version, SSubmitTbData* pSubmitTbData, int32_t* affectedRows);
int32_t tsdbDeleteTableData(STsdb* pTsdb, int64_t version, tb_uid_t suid, tb_uid_t uid, TSKEY sKey, TSKEY eKey);
int32_t tsdbSetKeepCfg(STsdb* pTsdb, STsdbCfg* pCfg);

// tq
int     tqInit();
void    tqCleanUp();
STQ*    tqOpen(const char* path, SVnode* pVnode);
void    tqClose(STQ*);
int     tqPushMsg(STQ*, void* msg, int32_t msgLen, tmsg_t msgType, int64_t ver);
int     tqCommit(STQ*);
int32_t tqUpdateTbUidList(STQ* pTq, const SArray* tbUidList, bool isAdd);
int32_t tqCheckColModifiable(STQ* pTq, int64_t tbUid, int32_t colId);
// tq-mq
int32_t tqProcessAddCheckInfoReq(STQ* pTq, int64_t version, char* msg, int32_t msgLen);
int32_t tqProcessDelCheckInfoReq(STQ* pTq, int64_t version, char* msg, int32_t msgLen);
int32_t tqProcessSubscribeReq(STQ* pTq, int64_t version, char* msg, int32_t msgLen);
int32_t tqProcessDeleteSubReq(STQ* pTq, int64_t version, char* msg, int32_t msgLen);
int32_t tqProcessOffsetCommitReq(STQ* pTq, int64_t version, char* msg, int32_t msgLen);
int32_t tqProcessPollReq(STQ* pTq, SRpcMsg* pMsg);
// tq-stream
int32_t tqProcessTaskDeployReq(STQ* pTq, int64_t version, char* msg, int32_t msgLen);
int32_t tqProcessTaskDropReq(STQ* pTq, int64_t version, char* msg, int32_t msgLen);
int32_t tqProcessStreamTaskCheckReq(STQ* pTq, SRpcMsg* pMsg);
int32_t tqProcessStreamTaskCheckRsp(STQ* pTq, int64_t version, char* msg, int32_t msgLen);
int32_t tqProcessSubmitReq(STQ* pTq, SPackedData submit);
int32_t tqProcessDelReq(STQ* pTq, void* pReq, int32_t len, int64_t ver);
int32_t tqProcessTaskRunReq(STQ* pTq, SRpcMsg* pMsg);
int32_t tqProcessTaskDispatchReq(STQ* pTq, SRpcMsg* pMsg, bool exec);
int32_t tqProcessTaskDispatchRsp(STQ* pTq, SRpcMsg* pMsg);
int32_t tqProcessTaskRetrieveReq(STQ* pTq, SRpcMsg* pMsg);
int32_t tqProcessTaskRetrieveRsp(STQ* pTq, SRpcMsg* pMsg);
int32_t tqProcessTaskRecover1Req(STQ* pTq, SRpcMsg* pMsg);
int32_t tqProcessTaskRecover2Req(STQ* pTq, int64_t version, char* msg, int32_t msgLen);
int32_t tqProcessTaskRecoverFinishReq(STQ* pTq, SRpcMsg* pMsg);
int32_t tqProcessTaskRecoverFinishRsp(STQ* pTq, SRpcMsg* pMsg);
int32_t tqCheckLogInWal(STQ* pTq, int64_t version);

int32_t tqBlockToSubmit(SVnode* pVnode, const SArray* pBlocks, const STSchema* pSchema,
                        SSchemaWrapper* pTagSchemaWrapper, bool createTb, int64_t suid, const char* stbFullName,
                        SBatchDeleteReq* pDeleteReq, void** ppData, int32_t* pLen);

// sma
int32_t smaInit();
void    smaCleanUp();
int32_t smaOpen(SVnode* pVnode, int8_t rollback);
int32_t smaClose(SSma* pSma);
int32_t smaBegin(SSma* pSma);
int32_t smaPrepareAsyncCommit(SSma* pSma);
int32_t smaCommit(SSma* pSma, SCommitInfo* pInfo);
int32_t smaFinishCommit(SSma* pSma);
int32_t smaPostCommit(SSma* pSma);
int32_t smaDoRetention(SSma* pSma, int64_t now);

int32_t tdProcessTSmaCreate(SSma* pSma, int64_t version, const char* msg);
int32_t tdProcessTSmaInsert(SSma* pSma, int64_t indexUid, const char* msg);

int32_t tdProcessRSmaCreate(SSma* pSma, SVCreateStbReq* pReq);
int32_t tdProcessRSmaSubmit(SSma* pSma, int64_t version, void* pReq, void* pMsg, int32_t len, int32_t inputType);
int32_t tdProcessRSmaDrop(SSma* pSma, SVDropStbReq* pReq);
int32_t tdFetchTbUidList(SSma* pSma, STbUidStore** ppStore, tb_uid_t suid, tb_uid_t uid);
int32_t tdUpdateTbUidList(SSma* pSma, STbUidStore* pUidStore, bool isAdd);
void*   tdUidStoreFree(STbUidStore* pStore);

// SMetaSnapReader ========================================
int32_t metaSnapReaderOpen(SMeta* pMeta, int64_t sver, int64_t ever, SMetaSnapReader** ppReader);
int32_t metaSnapReaderClose(SMetaSnapReader** ppReader);
int32_t metaSnapRead(SMetaSnapReader* pReader, uint8_t** ppData);
// SMetaSnapWriter ========================================
int32_t metaSnapWriterOpen(SMeta* pMeta, int64_t sver, int64_t ever, SMetaSnapWriter** ppWriter);
int32_t metaSnapWrite(SMetaSnapWriter* pWriter, uint8_t* pData, uint32_t nData);
int32_t metaSnapWriterClose(SMetaSnapWriter** ppWriter, int8_t rollback);
// STsdbSnapReader ========================================
int32_t tsdbSnapReaderOpen(STsdb* pTsdb, int64_t sver, int64_t ever, int8_t type, STsdbSnapReader** ppReader);
int32_t tsdbSnapReaderClose(STsdbSnapReader** ppReader);
int32_t tsdbSnapRead(STsdbSnapReader* pReader, uint8_t** ppData);
// STsdbSnapWriter ========================================
int32_t tsdbSnapWriterOpen(STsdb* pTsdb, int64_t sver, int64_t ever, STsdbSnapWriter** ppWriter);
int32_t tsdbSnapWrite(STsdbSnapWriter* pWriter, uint8_t* pData, uint32_t nData);
int32_t tsdbSnapWriterPrepareClose(STsdbSnapWriter* pWriter);
int32_t tsdbSnapWriterClose(STsdbSnapWriter** ppWriter, int8_t rollback);
// STqSnapshotReader ==
int32_t tqSnapReaderOpen(STQ* pTq, int64_t sver, int64_t ever, STqSnapReader** ppReader);
int32_t tqSnapReaderClose(STqSnapReader** ppReader);
int32_t tqSnapRead(STqSnapReader* pReader, uint8_t** ppData);
// STqSnapshotWriter ======================================
int32_t tqSnapWriterOpen(STQ* pTq, int64_t sver, int64_t ever, STqSnapWriter** ppWriter);
int32_t tqSnapWriterClose(STqSnapWriter** ppWriter, int8_t rollback);
int32_t tqSnapWrite(STqSnapWriter* pWriter, uint8_t* pData, uint32_t nData);
// STqOffsetReader ========================================
int32_t tqOffsetReaderOpen(STQ* pTq, int64_t sver, int64_t ever, STqOffsetReader** ppReader);
int32_t tqOffsetReaderClose(STqOffsetReader** ppReader);
int32_t tqOffsetSnapRead(STqOffsetReader* pReader, uint8_t** ppData);
// STqOffsetWriter ========================================
int32_t tqOffsetWriterOpen(STQ* pTq, int64_t sver, int64_t ever, STqOffsetWriter** ppWriter);
int32_t tqOffsetWriterClose(STqOffsetWriter** ppWriter, int8_t rollback);
int32_t tqOffsetSnapWrite(STqOffsetWriter* pWriter, uint8_t* pData, uint32_t nData);
// SStreamTaskWriter ======================================
// SStreamTaskReader ======================================
// SStreamStateWriter =====================================
// SStreamStateReader =====================================
// SRSmaSnapReader ========================================
int32_t rsmaSnapReaderOpen(SSma* pSma, int64_t sver, int64_t ever, SRSmaSnapReader** ppReader);
int32_t rsmaSnapReaderClose(SRSmaSnapReader** ppReader);
int32_t rsmaSnapRead(SRSmaSnapReader* pReader, uint8_t** ppData);
// SRSmaSnapWriter ========================================
int32_t rsmaSnapWriterOpen(SSma* pSma, int64_t sver, int64_t ever, SRSmaSnapWriter** ppWriter);
int32_t rsmaSnapWrite(SRSmaSnapWriter* pWriter, uint8_t* pData, uint32_t nData);
int32_t rsmaSnapWriterPrepareClose(SRSmaSnapWriter* pWriter);
int32_t rsmaSnapWriterClose(SRSmaSnapWriter** ppWriter, int8_t rollback);

typedef struct {
  int8_t  streamType;  // sma or other
  int8_t  dstType;
  int16_t padding;
  int32_t smaId;
  int64_t tbUid;
  int64_t lastReceivedVer;
  int64_t lastCommittedVer;
} SStreamSinkInfo;

typedef struct {
  SVnode*   pVnode;
  SHashObj* pHash;  // streamId -> SStreamSinkInfo
} SSink;

// SVState
struct SVState {
  int64_t committed;
  int64_t applied;
  int64_t applyTerm;
  int64_t commitID;
  int64_t commitTerm;
};

struct SVStatis {
  int64_t nInsert;              // delta
  int64_t nInsertSuccess;       // delta
  int64_t nBatchInsert;         // delta
  int64_t nBatchInsertSuccess;  // delta
};

struct SVnodeInfo {
  SVnodeCfg config;
  SVState   state;
  SVStatis  statis;
};

typedef enum {
  TSDB_TYPE_TSDB = 0,     // TSDB
  TSDB_TYPE_TSMA = 1,     // TSMA
  TSDB_TYPE_RSMA_L0 = 2,  // RSMA Level 0
  TSDB_TYPE_RSMA_L1 = 3,  // RSMA Level 1
  TSDB_TYPE_RSMA_L2 = 4,  // RSMA Level 2
} ETsdbType;

struct STsdbKeepCfg {
  int8_t  precision;  // precision always be used with below keep cfgs
  int32_t days;
  int32_t keep0;
  int32_t keep1;
  int32_t keep2;
};

struct SVnode {
  char*         path;
  SVnodeCfg     config;
  SVState       state;
  SVStatis      statis;
  STfs*         pTfs;
  SMsgCb        msgCb;
  TdThreadMutex mutex;
  TdThreadCond  poolNotEmpty;
  SVBufPool*    pPool;
  SVBufPool*    inUse;
  SMeta*        pMeta;
  SSma*         pSma;
  STsdb*        pTsdb;
  SWal*         pWal;
  STQ*          pTq;
  SSink*        pSink;
  tsem_t        canCommit;
  int64_t       sync;
  TdThreadMutex lock;
  bool          blocked;
  bool          restored;
  tsem_t        syncSem;
  int32_t       blockSec;
  int64_t       blockSeq;
  SQHandle*     pQuery;
#if 0
  SRpcHandleInfo blockInfo;
#endif
};

#define TD_VID(PVNODE) ((PVNODE)->config.vgId)

#define VND_TSDB(vnd)       ((vnd)->pTsdb)
#define VND_RSMA0(vnd)      ((vnd)->pTsdb)
#define VND_RSMA1(vnd)      ((vnd)->pSma->pRSmaTsdb[TSDB_RETENTION_L0])
#define VND_RSMA2(vnd)      ((vnd)->pSma->pRSmaTsdb[TSDB_RETENTION_L1])
#define VND_RETENTIONS(vnd) (&(vnd)->config.tsdbCfg.retentions)
#define VND_IS_RSMA(v)      ((v)->config.isRsma == 1)
#define VND_IS_TSMA(v)      ((v)->config.isTsma == 1)

struct STbUidStore {
  tb_uid_t  suid;
  SArray*   tbUids;
  SHashObj* uidHash;
};

struct SSma {
  bool          locked;
  TdThreadMutex mutex;
  SVnode*       pVnode;
  STsdb*        pRSmaTsdb[TSDB_RETENTION_L2];
  void*         pTSmaEnv;
  void*         pRSmaEnv;
};

#define SMA_CFG(s)        (&(s)->pVnode->config)
#define SMA_TSDB_CFG(s)   (&(s)->pVnode->config.tsdbCfg)
#define SMA_RETENTION(s)  ((SRetention*)&(s)->pVnode->config.tsdbCfg.retentions)
#define SMA_LOCKED(s)     ((s)->locked)
#define SMA_META(s)       ((s)->pVnode->pMeta)
#define SMA_VID(s)        TD_VID((s)->pVnode)
#define SMA_TFS(s)        ((s)->pVnode->pTfs)
#define SMA_TSMA_ENV(s)   ((s)->pTSmaEnv)
#define SMA_RSMA_ENV(s)   ((s)->pRSmaEnv)
#define SMA_RSMA_TSDB0(s) ((s)->pVnode->pTsdb)
#define SMA_RSMA_TSDB1(s) ((s)->pRSmaTsdb[TSDB_RETENTION_L0])
#define SMA_RSMA_TSDB2(s) ((s)->pRSmaTsdb[TSDB_RETENTION_L1])

// sma
void smaHandleRes(void* pVnode, int64_t smaId, const SArray* data);

enum {
  SNAP_DATA_CFG = 0,
  SNAP_DATA_META = 1,
  SNAP_DATA_TSDB = 2,
  SNAP_DATA_DEL = 3,
  SNAP_DATA_RSMA1 = 4,
  SNAP_DATA_RSMA2 = 5,
  SNAP_DATA_QTASK = 6,
  SNAP_DATA_TQ_HANDLE = 7,
  SNAP_DATA_TQ_OFFSET = 8,
  SNAP_DATA_STREAM_TASK = 9,
  SNAP_DATA_STREAM_STATE = 10,
};

struct SSnapDataHdr {
  int8_t  type;
  int8_t  flag;
  int64_t index;
  int64_t size;
  uint8_t data[];
};

struct SCommitInfo {
  SVnodeInfo info;
  SVnode*    pVnode;
  TXN*       txn;
};

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_DEF_H_*/
