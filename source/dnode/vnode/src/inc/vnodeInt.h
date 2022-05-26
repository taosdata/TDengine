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
#include "tmallocator.h"
#include "tmsgcb.h"
#include "tskiplist.h"
#include "tstream.h"
#include "ttime.h"
#include "ttimer.h"
#include "wal.h"

#include "vnode.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SVnodeInfo          SVnodeInfo;
typedef struct SMeta               SMeta;
typedef struct SSma                SSma;
typedef struct STsdb               STsdb;
typedef struct STQ                 STQ;
typedef struct SVState             SVState;
typedef struct SVBufPool           SVBufPool;
typedef struct SQWorker            SQHandle;
typedef struct STsdbKeepCfg        STsdbKeepCfg;
typedef struct SMetaSnapshotReader SMetaSnapshotReader;
typedef struct STsdbSnapshotReader STsdbSnapshotReader;

#define VNODE_META_DIR  "meta"
#define VNODE_TSDB_DIR  "tsdb"
#define VNODE_TQ_DIR    "tq"
#define VNODE_WAL_DIR   "wal"
#define VNODE_TSMA_DIR  "tsma"
#define VNODE_RSMA0_DIR "tsdb"
#define VNODE_RSMA1_DIR "rsma1"
#define VNODE_RSMA2_DIR "rsma2"

// vnd.h
void*   vnodeBufPoolMalloc(SVBufPool* pPool, int size);
void    vnodeBufPoolFree(SVBufPool* pPool, void* p);
int32_t vnodeRealloc(void** pp, int32_t size);
void    vnodeFree(void* p);

// meta
typedef struct SMCtbCursor SMCtbCursor;
typedef struct STbUidStore STbUidStore;

int             metaOpen(SVnode* pVnode, SMeta** ppMeta);
int             metaClose(SMeta* pMeta);
int             metaBegin(SMeta* pMeta);
int             metaCommit(SMeta* pMeta);
int             metaCreateSTable(SMeta* pMeta, int64_t version, SVCreateStbReq* pReq);
int             metaAlterSTable(SMeta* pMeta, int64_t version, SVCreateStbReq* pReq);
int             metaDropSTable(SMeta* pMeta, int64_t verison, SVDropStbReq* pReq);
int             metaCreateTable(SMeta* pMeta, int64_t version, SVCreateTbReq* pReq);
int             metaDropTable(SMeta* pMeta, int64_t version, SVDropTbReq* pReq, SArray* tbUids);
int             metaAlterTable(SMeta* pMeta, int64_t version, SVAlterTbReq* pReq);
SSchemaWrapper* metaGetTableSchema(SMeta* pMeta, tb_uid_t uid, int32_t sver, bool isinline);
STSchema*       metaGetTbTSchema(SMeta* pMeta, tb_uid_t uid, int32_t sver);
int             metaGetTableEntryByName(SMetaReader* pReader, const char* name);
int             metaGetTbNum(SMeta* pMeta);
SMCtbCursor*    metaOpenCtbCursor(SMeta* pMeta, tb_uid_t uid);
void            metaCloseCtbCursor(SMCtbCursor* pCtbCur);
tb_uid_t        metaCtbCursorNext(SMCtbCursor* pCtbCur);
STSma*          metaGetSmaInfoByIndex(SMeta* pMeta, int64_t indexUid);
STSmaWrapper*   metaGetSmaInfoByTable(SMeta* pMeta, tb_uid_t uid, bool deepCopy);
SArray*         metaGetSmaIdsByTable(SMeta* pMeta, tb_uid_t uid);
SArray*         metaGetSmaTbUids(SMeta* pMeta);
int32_t         metaSnapshotReaderOpen(SMeta* pMeta, SMetaSnapshotReader** ppReader, int64_t sver, int64_t ever);
int32_t         metaSnapshotReaderClose(SMetaSnapshotReader* pReader);
int32_t         metaSnapshotRead(SMetaSnapshotReader* pReader, void** ppData, uint32_t* nData);

int32_t metaCreateTSma(SMeta* pMeta, int64_t version, SSmaCfg* pCfg);
int32_t metaDropTSma(SMeta* pMeta, int64_t indexUid);

// tsdb
int          tsdbOpen(SVnode* pVnode, STsdb** ppTsdb, const char* dir, STsdbKeepCfg* pKeepCfg);
int          tsdbClose(STsdb** pTsdb);
int          tsdbBegin(STsdb* pTsdb);
int          tsdbCommit(STsdb* pTsdb);
int          tsdbScanAndConvertSubmitMsg(STsdb* pTsdb, SSubmitReq* pMsg);
int          tsdbInsertData(STsdb* pTsdb, int64_t version, SSubmitReq* pMsg, SSubmitRsp* pRsp);
int          tsdbInsertTableData(STsdb* pTsdb, SSubmitMsgIter* pMsgIter, SSubmitBlk* pBlock, SSubmitBlkRsp* pRsp);
tsdbReaderT* tsdbQueryTables(SVnode* pVnode, SQueryTableDataCond* pCond, STableGroupInfo* groupList, uint64_t qId,
                             uint64_t taskId);
tsdbReaderT  tsdbQueryCacheLastT(STsdb* tsdb, SQueryTableDataCond* pCond, STableGroupInfo* groupList, uint64_t qId,
                                 void* pMemRef);
int32_t      tsdbGetTableGroupFromIdListT(STsdb* tsdb, SArray* pTableIdList, STableGroupInfo* pGroupInfo);
int32_t      tsdbSnapshotReaderOpen(STsdb* pTsdb, STsdbSnapshotReader** ppReader, int64_t sver, int64_t ever);
int32_t      tsdbSnapshotReaderClose(STsdbSnapshotReader* pReader);
int32_t      tsdbSnapshotRead(STsdbSnapshotReader* pReader, void** ppData, uint32_t* nData);

// tq
STQ*    tqOpen(const char* path, SVnode* pVnode, SWal* pWal);
void    tqClose(STQ*);
int     tqPushMsg(STQ*, void* msg, int32_t msgLen, tmsg_t msgType, int64_t ver);
int     tqCommit(STQ*);
int32_t tqUpdateTbUidList(STQ* pTq, const SArray* tbUidList, bool isAdd);
int32_t tqProcessVgChangeReq(STQ* pTq, char* msg, int32_t msgLen);
int32_t tqProcessVgDeleteReq(STQ* pTq, char* msg, int32_t msgLen);
int32_t tqProcessPollReq(STQ* pTq, SRpcMsg* pMsg, int32_t workerId);
int32_t tqProcessTaskDeploy(STQ* pTq, char* msg, int32_t msgLen);
int32_t tqProcessStreamTrigger(STQ* pTq, SSubmitReq* data);
int32_t tqProcessTaskRunReq(STQ* pTq, SRpcMsg* pMsg);
int32_t tqProcessTaskDispatchReq(STQ* pTq, SRpcMsg* pMsg);
int32_t tqProcessTaskRecoverReq(STQ* pTq, SRpcMsg* pMsg);
int32_t tqProcessTaskDispatchRsp(STQ* pTq, SRpcMsg* pMsg);
int32_t tqProcessTaskRecoverRsp(STQ* pTq, SRpcMsg* pMsg);

// sma
int32_t smaOpen(SVnode* pVnode);
int32_t smaClose(SSma* pSma);

int32_t tdUpdateExpireWindow(SSma* pSma, SSubmitReq* pMsg, int64_t version);
int32_t tdProcessTSmaCreate(SSma* pSma, int64_t version, const char* msg);
int32_t tdProcessTSmaInsert(SSma* pSma, int64_t indexUid, const char* msg);

int32_t tdProcessRSmaCreate(SSma* pSma, SMeta* pMeta, SVCreateStbReq* pReq, SMsgCb* pMsgCb);
int32_t tdProcessRSmaSubmit(SSma* pSma, void* pMsg, int32_t inputType);
int32_t tdFetchTbUidList(SSma* pSma, STbUidStore** ppStore, tb_uid_t suid, tb_uid_t uid);
int32_t tdUpdateTbUidList(SSma* pSma, STbUidStore* pUidStore);
void    tdUidStoreDestory(STbUidStore* pStore);
void*   tdUidStoreFree(STbUidStore* pStore);

#if 0
int32_t tsdbUpdateSmaWindow(STsdb* pTsdb, SSubmitReq* pMsg, int64_t version);
int32_t tsdbCreateTSma(STsdb* pTsdb, char* pMsg);
int32_t tsdbInsertTSmaData(STsdb* pTsdb, int64_t indexUid, const char* msg);
int32_t tsdbRegisterRSma(STsdb* pTsdb, SMeta* pMeta, SVCreateStbReq* pReq, SMsgCb* pMsgCb);
int32_t tsdbFetchTbUidList(STsdb* pTsdb, STbUidStore** ppStore, tb_uid_t suid, tb_uid_t uid);
int32_t tsdbUpdateTbUidList(STsdb* pTsdb, STbUidStore* pUidStore);
void    tsdbUidStoreDestory(STbUidStore* pStore);
void*   tsdbUidStoreFree(STbUidStore* pStore);
int32_t tsdbTriggerRSma(STsdb* pTsdb, void* pMsg, int32_t inputType);
#endif

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
  // int64_t processed;
  int64_t committed;
  int64_t applied;
};

struct SVnodeInfo {
  SVnodeCfg config;
  SVState   state;
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
  char*      path;
  SVnodeCfg  config;
  SVState    state;
  STfs*      pTfs;
  SMsgCb     msgCb;
  SVBufPool* pPool;
  SVBufPool* inUse;
  SVBufPool* onCommit;
  SVBufPool* onRecycle;
  SMeta*     pMeta;
  SSma*      pSma;
  STsdb*     pTsdb;
  SWal*      pWal;
  STQ*       pTq;
  SSink*     pSink;
  int64_t    sync;
  tsem_t     canCommit;
  SQHandle*  pQuery;
};

#define TD_VID(PVNODE) (PVNODE)->config.vgId

#define VND_TSDB(vnd)       ((vnd)->pTsdb)
#define VND_RSMA0(vnd)      ((vnd)->pTsdb)
#define VND_RSMA1(vnd)      ((vnd)->pSma->pRSmaTsdb1)
#define VND_RSMA2(vnd)      ((vnd)->pSma->pRSmaTsdb2)
#define VND_RETENTIONS(vnd) (&(vnd)->config.tsdbCfg.retentions)

struct STbUidStore {
  tb_uid_t  suid;
  tb_uid_t  uid;  // TODO: just for debugging, remove when uid provided in SSDataBlock
  SArray*   tbUids;
  SHashObj* uidHash;
};

struct SSma {
  int16_t       nTSma;
  bool          locked;
  TdThreadMutex mutex;
  SVnode*       pVnode;
  STsdb*        pRSmaTsdb1;
  STsdb*        pRSmaTsdb2;
  void*         pTSmaEnv;
  void*         pRSmaEnv;
};

#define SMA_CFG(s)        (&(s)->pVnode->config)
#define SMA_TSDB_CFG(s)   (&(s)->pVnode->config.tsdbCfg)
#define SMA_LOCKED(s)     ((s)->locked)
#define SMA_META(s)       ((s)->pVnode->pMeta)
#define SMA_VID(s)        TD_VID((s)->pVnode)
#define SMA_TFS(s)        ((s)->pVnode->pTfs)
#define SMA_TSMA_NUM(s)   ((s)->nTSma)
#define SMA_TSMA_ENV(s)   ((s)->pTSmaEnv)
#define SMA_RSMA_ENV(s)   ((s)->pRSmaEnv)
#define SMA_RSMA_TSDB0(s) ((s)->pVnode->pTsdb)
#define SMA_RSMA_TSDB1(s) ((s)->pRSmaTsdb1)
#define SMA_RSMA_TSDB2(s) ((s)->pRSmaTsdb2)

static FORCE_INLINE bool vnodeIsRollup(SVnode* pVnode) {
  SRetention* pRetention = &(pVnode->config.tsdbCfg.retentions[0]);
  return (pRetention->freq > 0 && pRetention->keep > 0);
}

// sma
void smaHandleRes(void* pVnode, int64_t smaId, const SArray* data);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_DEF_H_*/
