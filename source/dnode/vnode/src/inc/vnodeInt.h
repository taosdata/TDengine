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
#include "tdbInt.h"
#include "tencode.h"
#include "tfs.h"
#include "tglobal.h"
#include "tjson.h"
#include "tlist.h"
#include "tlockfree.h"
#include "tlosertree.h"
#include "tmallocator.h"
#include "tskiplist.h"
#include "tstream.h"
#include "ttime.h"
#include "ttimer.h"
#include "wal.h"

#include "vnode.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SVnodeInfo   SVnodeInfo;
typedef struct SMeta        SMeta;
typedef struct STsdb        STsdb;
typedef struct STQ          STQ;
typedef struct SVState      SVState;
typedef struct SVBufPool    SVBufPool;
typedef struct SQWorkerMgmt SQHandle;

#define VNODE_META_DIR "meta"
#define VNODE_TSDB_DIR "tsdb"
#define VNODE_TQ_DIR   "tq"
#define VNODE_WAL_DIR  "wal"

// vnd.h
void* vnodeBufPoolMalloc(SVBufPool* pPool, int size);
void  vnodeBufPoolFree(SVBufPool* pPool, void* p);

// meta
typedef struct SMCtbCursor SMCtbCursor;
typedef struct STbUidStore STbUidStore;

int             metaOpen(SVnode* pVnode, SMeta** ppMeta);
int             metaClose(SMeta* pMeta);
int             metaBegin(SMeta* pMeta);
int             metaCommit(SMeta* pMeta);
int             metaCreateSTable(SMeta* pMeta, int64_t version, SVCreateStbReq* pReq);
int             metaCreateTable(SMeta* pMeta, int64_t version, SVCreateTbReq* pReq);
SSchemaWrapper* metaGetTableSchema(SMeta* pMeta, tb_uid_t uid, int32_t sver, bool isinline);
STSchema*       metaGetTbTSchema(SMeta* pMeta, tb_uid_t uid, int32_t sver);
int             metaGetTableEntryByName(SMetaReader* pReader, const char* name);
int             metaGetTbNum(SMeta* pMeta);
SMCtbCursor*    metaOpenCtbCursor(SMeta* pMeta, tb_uid_t uid);
void            metaCloseCtbCurosr(SMCtbCursor* pCtbCur);
tb_uid_t        metaCtbCursorNext(SMCtbCursor* pCtbCur);
SArray*         metaGetSmaTbUids(SMeta* pMeta, bool isDup);
void*           metaGetSmaInfoByIndex(SMeta* pMeta, int64_t indexUid, bool isDecode);
STSmaWrapper*   metaGetSmaInfoByTable(SMeta* pMeta, tb_uid_t uid);
int32_t         metaCreateTSma(SMeta* pMeta, SSmaCfg* pCfg);
int32_t         metaDropTSma(SMeta* pMeta, int64_t indexUid);

// tsdb
int     tsdbOpen(SVnode* pVnode, STsdb** ppTsdb);
int     tsdbClose(STsdb* pTsdb);
int     tsdbBegin(STsdb* pTsdb);
int     tsdbCommit(STsdb* pTsdb);
int32_t tsdbUpdateSmaWindow(STsdb* pTsdb, SSubmitReq* pMsg, int64_t version);
int32_t tsdbCreateTSma(STsdb* pTsdb, char* pMsg);
int32_t tsdbInsertTSmaData(STsdb* pTsdb, int64_t indexUid, const char* msg);
int     tsdbInsertData(STsdb* pTsdb, int64_t version, SSubmitReq* pMsg, SSubmitRsp* pRsp);

// tq
STQ*    tqOpen(const char* path, SVnode* pVnode, SWal* pWal);
void    tqClose(STQ*);
int     tqPushMsg(STQ*, void* msg, int32_t msgLen, tmsg_t msgType, int64_t ver);
int     tqCommit(STQ*);
int32_t tqProcessVgChangeReq(STQ* pTq, char* msg, int32_t msgLen);
int32_t tqProcessTaskExec(STQ* pTq, char* msg, int32_t msgLen, int32_t workerId);
int32_t tqProcessTaskDeploy(STQ* pTq, char* msg, int32_t msgLen);
int32_t tqProcessStreamTrigger(STQ* pTq, void* data, int32_t dataLen, int32_t workerId);
int32_t tqProcessPollReq(STQ* pTq, SRpcMsg* pMsg, int32_t workerId);

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
  STsdb*     pTsdb;
  SWal*      pWal;
  STQ*       pTq;
  SSink*     pSink;
  int64_t    sync;
  tsem_t     canCommit;
  SQHandle*  pQuery;
};

struct STbUidStore {
  tb_uid_t  suid;
  SArray*   tbUids;
  SHashObj* uidHash;
};

#define TD_VID(PVNODE) (PVNODE)->config.vgId

typedef struct STbDdlH STbDdlH;

// sma
void smaHandleRes(void* pVnode, int64_t smaId, const SArray* data);

#ifdef __cplusplus
}
#endif

#endif /*_TD_VNODE_DEF_H_*/
